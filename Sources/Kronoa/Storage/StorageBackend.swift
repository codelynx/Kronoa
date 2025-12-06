import Foundation

/// Abstract interface for storage backends.
///
/// Implementations provide interchangeable storage for local filesystem, S3, GCS, Azure Blob, etc.
/// All paths are relative to the storage root (e.g., "contents/editions/10001/.origin").
public protocol StorageBackend: Sendable {
    /// Read file content.
    /// - Throws: `StorageError.notFound` if file doesn't exist
    func read(path: String) async throws -> Data

    /// Write file content. Creates parent directories as needed.
    /// Overwrites if file exists.
    func write(path: String, data: Data) async throws

    /// Write file only if it doesn't exist (atomic create-if-absent).
    /// - Returns: true if file was created, false if it already existed
    /// - Throws: `StorageError` on I/O failure
    func writeIfAbsent(path: String, data: Data) async throws -> Bool

    /// Delete file.
    /// - Throws: `StorageError.notFound` if file doesn't exist
    func delete(path: String) async throws

    /// Check if file exists.
    func exists(path: String) async throws -> Bool

    /// List entries with given prefix.
    ///
    /// For directory-like listing, use prefix with trailing slash and delimiter "/".
    /// Returns immediate children only (files and subdirectory prefixes).
    ///
    /// - Parameters:
    ///   - prefix: Path prefix to list (e.g., "contents/editions/10001/")
    ///   - delimiter: Optional delimiter for hierarchical listing (typically "/")
    /// - Returns: Array of keys (full paths) or common prefixes (for delimiter mode)
    func list(prefix: String, delimiter: String?) async throws -> [String]

    /// Atomically increment a counter file and return the new value.
    ///
    /// If file doesn't exist, creates it with initial value and returns that value.
    /// Used for `.head` edition counter.
    ///
    /// - Parameters:
    ///   - path: Path to counter file
    ///   - initialValue: Value to use if file doesn't exist (default: 10000)
    /// - Returns: The new value after increment
    func atomicIncrement(path: String, initialValue: Int) async throws -> Int

    /// Acquire an exclusive lock with lease-based expiration.
    ///
    /// - Parameters:
    ///   - path: Path to lock file
    ///   - timeout: Maximum time to wait for lock acquisition
    ///   - leaseDuration: How long the lock is valid (must be renewed for longer operations)
    /// - Returns: A handle to manage the lock
    /// - Throws: `StorageError.lockTimeout` if lock cannot be acquired within timeout
    func acquireLock(
        path: String,
        timeout: TimeInterval,
        leaseDuration: TimeInterval
    ) async throws -> LockHandle
}

/// Handle for managing an acquired lock.
public protocol LockHandle: Sendable {
    /// Unique identifier for the lock owner.
    var owner: String { get }

    /// When the lock lease expires.
    var expiresAt: Date { get async }

    /// Extend the lease duration.
    /// - Throws: `StorageError.lockExpired` if lease already expired
    func renew(duration: TimeInterval) async throws

    /// Release the lock.
    /// - Throws: `StorageError.lockExpired` if lease expired (lock may have been taken by another process)
    func release() async throws
}

/// Errors from storage operations.
public enum StorageError: Error, Equatable {
    /// File not found at path.
    case notFound(path: String)

    /// Could not acquire lock within timeout.
    case lockTimeout

    /// Lock lease expired (another process may have taken it).
    case lockExpired

    /// Concurrent modification detected (ETag mismatch).
    /// Caller should retry the operation.
    case concurrentModification(path: String)

    /// Generic I/O error with description.
    case ioError(String)

    /// Invalid path format.
    case invalidPath(String)
}
