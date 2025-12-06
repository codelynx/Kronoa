import Foundation

/// Path validation utilities shared across storage backends.
enum PathValidation {
    /// Validate a storage path (file operations).
    /// - Throws: `StorageError.invalidPath` for invalid paths
    static func validatePath(_ path: String) throws {
        guard !path.isEmpty else {
            throw StorageError.invalidPath("Path cannot be empty")
        }

        guard !path.hasPrefix("/") else {
            throw StorageError.invalidPath("Absolute paths not allowed: \(path)")
        }

        let components = path.split(separator: "/", omittingEmptySubsequences: false)
        for component in components {
            if component == ".." {
                throw StorageError.invalidPath("Path traversal not allowed: \(path)")
            }
            if component == "." {
                throw StorageError.invalidPath("Current directory reference not allowed: \(path)")
            }
        }
    }

    /// Validate a prefix path for listing (allows empty and trailing slash).
    static func validatePrefix(_ prefix: String) throws {
        guard !prefix.isEmpty else { return }

        guard !prefix.hasPrefix("/") else {
            throw StorageError.invalidPath("Absolute paths not allowed: \(prefix)")
        }

        let components = prefix.split(separator: "/", omittingEmptySubsequences: false)
        for component in components {
            if component == ".." {
                throw StorageError.invalidPath("Path traversal not allowed: \(prefix)")
            }
            if component == "." {
                throw StorageError.invalidPath("Current directory reference not allowed: \(prefix)")
            }
        }
    }
}
