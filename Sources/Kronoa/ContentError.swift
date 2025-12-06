import Foundation

/// Errors from content operations.
public enum ContentError: Error, Equatable {
    /// Path is malformed (contains .., starts with ., etc.)
    case invalidPath(String)

    /// File not found in ancestry (or tombstoned)
    case notFound(path: String)

    /// Attempted write/delete in read-only mode
    case readOnlyMode

    /// Working label already exists
    case labelInUse(String)

    /// Operation requires editing mode
    case notInEditingMode

    /// endEditing/rollback called without beginEditing
    case notInTransaction

    /// beginEditing called while already in transaction
    case alreadyInTransaction

    /// Edition directory does not exist
    case editionNotFound(edition: Int)

    /// No .pending/{edition}.json file found
    case pendingNotFound(edition: Int)

    /// .pending/{edition}.json exists but JSON is invalid
    case pendingCorrupt(edition: Int, reason: String)

    /// .rejected/{edition}.json exists but JSON is invalid
    case rejectedCorrupt(edition: Int, reason: String)

    /// Pending base doesn't match current staging/production
    case conflictDetected(base: Int, current: Int, source: CheckoutSource)

    /// Could not acquire lock within timeout
    case lockTimeout

    /// Lock lease expired while holding
    case lockExpired

    /// Storage backend error
    case storageError(underlying: StorageError)

    /// Hash mismatch when reading object
    case integrityError(expected: String, actual: String)
}
