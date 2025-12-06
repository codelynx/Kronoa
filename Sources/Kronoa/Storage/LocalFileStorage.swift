import Foundation

/// Local filesystem implementation of StorageBackend.
///
/// Uses a root directory as the storage base. All paths are relative to this root.
/// Suitable for development and testing.
public actor LocalFileStorage: StorageBackend {
    private let root: URL
    private let fileManager: FileManager

    /// Initialize with a root directory path.
    /// - Parameter root: Base directory for all storage operations
    public init(root: URL) {
        // Standardize to resolve symlinks (e.g., /var -> /private/var on macOS)
        self.root = root.standardizedFileURL
        self.fileManager = FileManager.default
    }

    /// Convenience initializer with string path.
    public init(rootPath: String) {
        self.init(root: URL(fileURLWithPath: rootPath))
    }

    public func read(path: String) async throws -> Data {
        let url = root.appendingPathComponent(path)
        guard fileManager.fileExists(atPath: url.path) else {
            throw StorageError.notFound(path: path)
        }
        do {
            return try Data(contentsOf: url)
        } catch {
            throw StorageError.ioError("Failed to read \(path): \(error.localizedDescription)")
        }
    }

    public func write(path: String, data: Data) async throws {
        let url = root.appendingPathComponent(path)
        let directory = url.deletingLastPathComponent()

        do {
            // Create parent directories if needed
            if !fileManager.fileExists(atPath: directory.path) {
                try fileManager.createDirectory(at: directory, withIntermediateDirectories: true)
            }
            try data.write(to: url)
        } catch {
            throw StorageError.ioError("Failed to write \(path): \(error.localizedDescription)")
        }
    }

    public func delete(path: String) async throws {
        let url = root.appendingPathComponent(path)
        guard fileManager.fileExists(atPath: url.path) else {
            throw StorageError.notFound(path: path)
        }
        do {
            try fileManager.removeItem(at: url)
        } catch {
            throw StorageError.ioError("Failed to delete \(path): \(error.localizedDescription)")
        }
    }

    public func exists(path: String) async throws -> Bool {
        let url = root.appendingPathComponent(path)
        return fileManager.fileExists(atPath: url.path)
    }

    public func list(prefix: String, delimiter: String?) async throws -> [String] {
        // Determine the directory to list
        let directoryPath: String

        if prefix.hasSuffix("/") {
            // Listing a directory - remove trailing slash for path construction
            directoryPath = String(prefix.dropLast())
        } else if prefix.isEmpty {
            directoryPath = ""
        } else {
            // Prefix is partial filename - list parent directory
            if let lastSlash = prefix.lastIndex(of: "/") {
                directoryPath = String(prefix[..<lastSlash])
            } else {
                directoryPath = ""
            }
        }

        let directoryURL = directoryPath.isEmpty
            ? root
            : root.appendingPathComponent(directoryPath)

        // Standardize to handle symlinks
        let standardizedDirURL = directoryURL.standardizedFileURL

        guard fileManager.fileExists(atPath: standardizedDirURL.path) else {
            return [] // Empty result for non-existent directory (S3 semantics)
        }

        var isDirectory: ObjCBool = false
        guard fileManager.fileExists(atPath: standardizedDirURL.path, isDirectory: &isDirectory),
              isDirectory.boolValue else {
            return []
        }

        do {
            let contents = try fileManager.contentsOfDirectory(
                at: standardizedDirURL,
                includingPropertiesForKeys: [.isDirectoryKey]
            )

            var results: [String] = []
            let standardizedRootPath = root.path

            for itemURL in contents {
                // Standardize item URL and build relative path from root
                let standardizedItemURL = itemURL.standardizedFileURL
                var relativePath = standardizedItemURL.path

                if relativePath.hasPrefix(standardizedRootPath) {
                    relativePath = String(relativePath.dropFirst(standardizedRootPath.count))
                    if relativePath.hasPrefix("/") {
                        relativePath = String(relativePath.dropFirst())
                    }
                }

                // Apply prefix filter
                guard relativePath.hasPrefix(prefix) || prefix.isEmpty else {
                    continue
                }

                let isDir = (try? standardizedItemURL.resourceValues(forKeys: [.isDirectoryKey]))?.isDirectory ?? false

                if delimiter == "/" {
                    // Hierarchical listing: return immediate children only
                    if isDir {
                        results.append(relativePath + "/")
                    } else {
                        results.append(relativePath)
                    }
                } else {
                    // Flat listing
                    results.append(relativePath)
                }
            }

            return results.sorted()
        } catch {
            throw StorageError.ioError("Failed to list \(prefix): \(error.localizedDescription)")
        }
    }

    public func atomicIncrement(path: String, initialValue: Int = 10000) async throws -> Int {
        let url = root.appendingPathComponent(path)
        let directory = url.deletingLastPathComponent()

        // Ensure directory exists
        if !fileManager.fileExists(atPath: directory.path) {
            try fileManager.createDirectory(at: directory, withIntermediateDirectories: true)
        }

        // Read current value or use initial
        let currentValue: Int
        if fileManager.fileExists(atPath: url.path) {
            let data = try Data(contentsOf: url)
            guard let content = String(data: data, encoding: .utf8),
                  let value = Int(content.trimmingCharacters(in: .whitespacesAndNewlines)) else {
                throw StorageError.ioError("Invalid counter format at \(path)")
            }
            currentValue = value
        } else {
            currentValue = initialValue - 1 // So first increment returns initialValue
        }

        let newValue = currentValue + 1
        let data = "\(newValue)".data(using: .utf8)!
        try data.write(to: url)

        return newValue
    }

    public func acquireLock(
        path: String,
        timeout: TimeInterval,
        leaseDuration: TimeInterval
    ) async throws -> LockHandle {
        try await LocalFileLock(
            storage: self,
            path: path,
            timeout: timeout,
            leaseDuration: leaseDuration
        )
    }

    // MARK: - Internal helpers for lock

    func readLockInfo(path: String) async throws -> LockInfo? {
        let url = root.appendingPathComponent(path)
        guard fileManager.fileExists(atPath: url.path) else {
            return nil
        }
        let data = try Data(contentsOf: url)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return try decoder.decode(LockInfo.self, from: data)
    }

    func writeLockInfo(_ info: LockInfo, path: String) async throws {
        let url = root.appendingPathComponent(path)
        let directory = url.deletingLastPathComponent()

        if !fileManager.fileExists(atPath: directory.path) {
            try fileManager.createDirectory(at: directory, withIntermediateDirectories: true)
        }

        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(info)
        try data.write(to: url)
    }

    func deleteLockFile(path: String) async throws {
        let url = root.appendingPathComponent(path)
        if fileManager.fileExists(atPath: url.path) {
            try fileManager.removeItem(at: url)
        }
    }
}

/// Lock information stored in lock file.
public struct LockInfo: Codable, Sendable {
    public let owner: String
    public let acquiredAt: Date
    public var expiresAt: Date

    public init(owner: String, acquiredAt: Date, expiresAt: Date) {
        self.owner = owner
        self.acquiredAt = acquiredAt
        self.expiresAt = expiresAt
    }
}

/// Local filesystem lock implementation.
public actor LocalFileLock: LockHandle {
    private let storage: LocalFileStorage
    private let path: String
    public nonisolated let owner: String
    private var _expiresAt: Date

    public var expiresAt: Date {
        _expiresAt
    }

    init(
        storage: LocalFileStorage,
        path: String,
        timeout: TimeInterval,
        leaseDuration: TimeInterval
    ) async throws {
        self.storage = storage
        self.path = path
        self.owner = UUID().uuidString
        self._expiresAt = Date()

        // Try to acquire lock
        let deadline = Date().addingTimeInterval(timeout)

        while Date() < deadline {
            // Check if lock exists
            if let existingLock = try await storage.readLockInfo(path: path) {
                if existingLock.expiresAt < Date() {
                    // Lock expired, delete it
                    try await storage.deleteLockFile(path: path)
                } else {
                    // Lock held by someone else, wait
                    try await Task.sleep(nanoseconds: 100_000_000) // 100ms
                    continue
                }
            }

            // Try to create lock
            let now = Date()
            let lockInfo = LockInfo(
                owner: owner,
                acquiredAt: now,
                expiresAt: now.addingTimeInterval(leaseDuration)
            )
            try await storage.writeLockInfo(lockInfo, path: path)

            // Verify we own the lock (basic check - not fully atomic on local FS)
            if let written = try await storage.readLockInfo(path: path),
               written.owner == owner {
                self._expiresAt = lockInfo.expiresAt
                return
            }
        }

        throw StorageError.lockTimeout
    }

    public func renew(duration: TimeInterval) async throws {
        guard let current = try await storage.readLockInfo(path: path) else {
            throw StorageError.lockExpired
        }

        guard current.owner == owner else {
            throw StorageError.lockExpired
        }

        guard current.expiresAt > Date() else {
            throw StorageError.lockExpired
        }

        let newInfo = LockInfo(
            owner: owner,
            acquiredAt: current.acquiredAt,
            expiresAt: Date().addingTimeInterval(duration)
        )
        try await storage.writeLockInfo(newInfo, path: path)
        _expiresAt = newInfo.expiresAt
    }

    public func release() async throws {
        guard let current = try await storage.readLockInfo(path: path) else {
            // Lock file gone - already released or expired
            return
        }

        guard current.owner == owner else {
            throw StorageError.lockExpired
        }

        try await storage.deleteLockFile(path: path)
    }

}
