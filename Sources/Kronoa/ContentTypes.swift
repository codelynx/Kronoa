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
