import Foundation
import Kronoa

/// Normalize storage URL - accepts s3://, file://, or bare local paths
func normalizeStorageUrl(_ url: String) -> String {
    if url.hasPrefix("s3://") || url.hasPrefix("file://") {
        return url
    }
    // Treat bare paths as local file storage
    let absolutePath: String
    if url.hasPrefix("/") {
        absolutePath = url
    } else if url.hasPrefix("~") {
        absolutePath = NSString(string: url).expandingTildeInPath
    } else {
        // Relative path - make absolute
        absolutePath = FileManager.default.currentDirectoryPath + "/" + url
    }
    return "file://\(absolutePath)"
}

/// Validate storage URL format
func validateStorageUrl(_ url: String) throws {
    let normalized = normalizeStorageUrl(url)
    if normalized.hasPrefix("s3://") {
        let path = String(normalized.dropFirst(5))
        let components = path.split(separator: "/", maxSplits: 1)
        guard !components.isEmpty, !components[0].isEmpty else {
            throw CLIError.invalidStorageUrl(url)
        }
    } else if normalized.hasPrefix("file://") {
        // File URLs are always valid after normalization
    } else {
        throw CLIError.invalidStorageUrl(url)
    }
}

/// Get session mode from config, rejecting submitted mode
func getSessionMode(from config: SessionConfig) throws -> SessionMode {
    guard config.mode != "submitted" else {
        throw CLIError.submittedMode
    }

    return switch config.mode {
    case "production": .production
    case "staging": .staging
    case "editing" where config.label != nil: .editing(label: config.label!)
    default: .staging
    }
}

/// URI path utilities for kr: and file: schemes
enum PathScheme {
    case remote(String)    // kr:path
    case local(String)     // file:path or plain path

    static func parse(_ path: String, relativeTo cwd: String? = nil) -> PathScheme {
        if path.hasPrefix("kr:") {
            var remotePath = String(path.dropFirst(3))
            // Handle relative paths
            if !remotePath.hasPrefix("/"), let cwd = cwd {
                remotePath = cwd + remotePath
            }
            return .remote(remotePath)
        } else if path.hasPrefix("file:") {
            return .local(String(path.dropFirst(5)))
        } else {
            return .local(path)
        }
    }
}

/// Create storage backend from session config
func createStorage(from config: SessionConfig) async throws -> StorageBackend {
    guard let storageUrl = config.storage else {
        throw CLIError.noStorageConfigured
    }

    let normalized = normalizeStorageUrl(storageUrl)

    if normalized.hasPrefix("s3://") {
        let path = String(normalized.dropFirst(5))
        let components = path.split(separator: "/", maxSplits: 1)
        guard !components.isEmpty else {
            throw CLIError.invalidStorageUrl(storageUrl)
        }
        let bucket = String(components[0])
        let prefix = components.count > 1 ? String(components[1]) : ""
        return try await S3Storage(bucket: bucket, prefix: prefix)
    } else if normalized.hasPrefix("file://") {
        let path = String(normalized.dropFirst(7))
        return LocalFileStorage(root: URL(fileURLWithPath: path))
    } else {
        throw CLIError.invalidStorageUrl(storageUrl)
    }
}

/// CLI-specific errors
enum CLIError: Error, CustomStringConvertible {
    case noStorageConfigured
    case invalidStorageUrl(String)
    case notInEditingMode
    case submittedMode
    case invalidPath(String)
    case globRequiresFlag

    var description: String {
        switch self {
        case .noStorageConfigured:
            return "No storage configured. Run: kronoa config set storage <url>"
        case .invalidStorageUrl(let url):
            return "Invalid storage URL: \(url)"
        case .notInEditingMode:
            return "Not in editing mode. Run: kronoa checkout <label>"
        case .submittedMode:
            return "Edition has been submitted. Run: kronoa done to clear session, or kronoa pending to check status"
        case .invalidPath(let path):
            return "Invalid path: \(path)"
        case .globRequiresFlag:
            return "Glob patterns require --glob flag for destructive operations"
        }
    }
}

/// Check if path contains glob characters
func isGlobPattern(_ path: String) -> Bool {
    path.contains("*") || path.contains("?") || path.contains("[")
}

/// Simple glob matching (supports * only for now)
func matchGlob(pattern: String, against paths: [String]) -> [String] {
    guard pattern.contains("*") else {
        return paths.filter { $0 == pattern }
    }

    let regex = pattern
        .replacingOccurrences(of: ".", with: "\\.")
        .replacingOccurrences(of: "*", with: ".*")

    guard let regex = try? NSRegularExpression(pattern: "^\(regex)$") else {
        return []
    }

    return paths.filter { path in
        let range = NSRange(path.startIndex..., in: path)
        return regex.firstMatch(in: path, range: range) != nil
    }
}
