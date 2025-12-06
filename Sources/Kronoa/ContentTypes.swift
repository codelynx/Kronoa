import Foundation

/// Session mode determines read/write capabilities.
public enum SessionMode: Equatable, Sendable {
    /// Read-only, serves .production.json edition
    case production
    /// Read-only, serves .staging.json edition
    case staging
    /// Read-write, serves .{label}.json edition
    case editing(label: String)
    /// Read-only, edition submitted for review
    case submitted
}

/// Source to branch from when checking out.
public enum CheckoutSource: String, Codable, Equatable, Sendable {
    /// Branch from current staging (default)
    case staging
    /// Branch from current production (for emergency fixes)
    case production
}

/// File metadata returned by stat().
public struct FileStat: Equatable, Sendable {
    public let path: String
    public let status: FileStatus
    /// Edition where status was determined
    public let resolvedFrom: Int
    /// Only present when status == .exists
    public let hash: String?
    /// Only present when status == .exists
    public let size: Int?

    public init(path: String, status: FileStatus, resolvedFrom: Int, hash: String? = nil, size: Int? = nil) {
        self.path = path
        self.status = status
        self.resolvedFrom = resolvedFrom
        self.hash = hash
        self.size = size
    }
}

/// File status in the edition chain.
public enum FileStatus: Equatable, Sendable {
    /// File has content
    case exists
    /// Tombstone marker found
    case deleted
    /// Never existed in ancestry
    case notFound
}

/// Working session state stored in .{label}.json
public struct SessionState: Codable, Equatable, Sendable {
    public let edition: Int
    public let base: Int
    public let source: CheckoutSource

    public init(edition: Int, base: Int, source: CheckoutSource) {
        self.edition = edition
        self.base = base
        self.source = source
    }
}

/// Edition pointer stored in .production.json and .staging.json
public struct EditionPointer: Codable, Equatable, Sendable {
    public let edition: Int

    public init(edition: Int) {
        self.edition = edition
    }
}

/// A buffered change in a transaction.
public struct PendingChange: Equatable, Sendable {
    public let path: String
    public let action: ChangeAction

    public init(path: String, action: ChangeAction) {
        self.path = path
        self.action = action
    }
}

/// Type of change action.
public enum ChangeAction: Equatable, Sendable {
    /// Write new content (hash computed, data buffered)
    case write(hash: String, size: Int)
    /// Delete (tombstone)
    case delete
}

/// Pending submission metadata stored in .pending/{edition}.json
public struct PendingSubmission: Codable, Equatable, Sendable {
    public let edition: Int
    public let base: Int
    public let source: CheckoutSource
    public let label: String
    public let message: String
    public let submittedAt: Date

    public init(edition: Int, base: Int, source: CheckoutSource, label: String, message: String, submittedAt: Date) {
        self.edition = edition
        self.base = base
        self.source = source
        self.label = label
        self.message = message
        self.submittedAt = submittedAt
    }
}

/// Rejected submission metadata stored in .rejected/{edition}.json
public struct RejectedSubmission: Codable, Equatable, Sendable {
    public let edition: Int
    public let reason: String
    public let rejectedAt: Date

    public init(edition: Int, reason: String, rejectedAt: Date) {
        self.edition = edition
        self.reason = reason
        self.rejectedAt = rejectedAt
    }
}

/// Result of garbage collection run.
public struct GCResult: Equatable, Sendable {
    /// Total objects scanned
    public let scannedObjects: Int
    /// Objects deleted (0 in dry-run mode)
    public let deletedObjects: Int
    /// Objects kept via .ref fast path (live edition found in ref)
    public let skippedByRef: Int
    /// Objects kept via fallback scan (found in live edition)
    public let skippedByScan: Int
    /// Orphaned objects (counted in dry-run, deleted otherwise)
    public let skippedByAge: Int
    /// Objects that failed to delete
    public let errors: Int

    public init(
        scannedObjects: Int,
        deletedObjects: Int,
        skippedByRef: Int,
        skippedByScan: Int,
        skippedByAge: Int,
        errors: Int
    ) {
        self.scannedObjects = scannedObjects
        self.deletedObjects = deletedObjects
        self.skippedByRef = skippedByRef
        self.skippedByScan = skippedByScan
        self.skippedByAge = skippedByAge
        self.errors = errors
    }
}
