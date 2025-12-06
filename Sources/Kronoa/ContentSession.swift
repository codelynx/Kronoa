import Foundation

/// Content session for reading and writing versioned content.
///
/// Sessions operate in different modes:
/// - `.production`: Read-only access to live content
/// - `.staging`: Read-only access to staged content
/// - `.editing(label)`: Read-write access for content creation
/// - `.submitted`: Read-only after submitting for review
public actor ContentSession {
    private let storage: StorageBackend
    private var _mode: SessionMode
    private var _editionId: Int
    private var _baseEditionId: Int?
    private var _checkoutSource: CheckoutSource?

    // Storage paths
    private nonisolated let contentsPrefix = "contents/"
    private nonisolated let editionsPrefix = "contents/editions/"
    private nonisolated let objectsPrefix = "contents/objects/"

    /// Current session mode.
    public nonisolated var mode: SessionMode {
        get async { await _mode }
    }

    /// Current edition ID being served.
    public nonisolated var editionId: Int {
        get async { await _editionId }
    }

    /// Base edition ID (only set in editing mode).
    public nonisolated var baseEditionId: Int? {
        get async { await _baseEditionId }
    }

    /// Checkout source (only set in editing mode).
    public nonisolated var checkoutSource: CheckoutSource? {
        get async { await _checkoutSource }
    }

    /// Create a new content session.
    ///
    /// - Parameters:
    ///   - storage: Storage backend to use
    ///   - mode: Initial session mode (.production or .staging)
    /// - Throws: `ContentError.storageError` if edition pointer cannot be read
    public init(storage: StorageBackend, mode: SessionMode) async throws {
        guard case .production = mode, true else {
            guard case .staging = mode else {
                fatalError("Initial mode must be .production or .staging")
            }
            self.storage = storage
            self._mode = mode
            self._editionId = try await Self.readEditionPointer(storage: storage, file: "contents/.staging.json")
            self._baseEditionId = nil
            self._checkoutSource = nil
            return
        }
        self.storage = storage
        self._mode = mode
        self._editionId = try await Self.readEditionPointer(storage: storage, file: "contents/.production.json")
        self._baseEditionId = nil
        self._checkoutSource = nil
    }

    /// Read edition pointer from a JSON file.
    private static func readEditionPointer(storage: StorageBackend, file: String) async throws -> Int {
        do {
            let data = try await storage.read(path: file)
            let pointer = try JSONDecoder().decode(EditionPointer.self, from: data)
            return pointer.edition
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        } catch {
            throw ContentError.storageError(underlying: .ioError(error.localizedDescription))
        }
    }

    // MARK: - Checkout

    /// Checkout: create a new edition for editing.
    ///
    /// Transitions from production/staging mode to editing mode.
    ///
    /// - Parameters:
    ///   - label: Unique label for this editing session
    ///   - from: Source to branch from (default: .staging)
    /// - Throws: `ContentError.labelInUse`, `ContentError.invalidPath`, `ContentError.notInEditingMode`
    public func checkout(label: String, from source: CheckoutSource = .staging) async throws {
        // Must be in production or staging mode to checkout
        switch _mode {
        case .production, .staging:
            break
        case .editing, .submitted:
            throw ContentError.notInEditingMode
        }

        // Validate label
        guard isValidLabel(label) else {
            throw ContentError.invalidPath(label)
        }

        // Reserve the label first (before allocating edition ID) to avoid orphaned editions
        // We write a placeholder and update it with the real edition ID after allocation
        let workingFile = "contents/.\(label).json"
        do {
            // Try to create placeholder - fails if label already in use
            let created = try await storage.writeIfAbsent(path: workingFile, data: Data())
            if !created {
                throw ContentError.labelInUse(label)
            }
        } catch let error as ContentError {
            throw error
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        // Get base edition from source
        let baseEdition: Int
        do {
            switch source {
            case .staging:
                baseEdition = try await Self.readEditionPointer(storage: storage, file: "contents/.staging.json")
            case .production:
                baseEdition = try await Self.readEditionPointer(storage: storage, file: "contents/.production.json")
            }
        } catch {
            // Clean up placeholder on failure
            try? await storage.delete(path: workingFile)
            throw error
        }

        // Allocate new edition ID
        let newEditionId: Int
        do {
            newEditionId = try await storage.atomicIncrement(path: "contents/editions/.head", initialValue: 10000)
        } catch let error as StorageError {
            // Clean up placeholder on failure
            try? await storage.delete(path: workingFile)
            throw ContentError.storageError(underlying: error)
        }

        // Create edition directory with .origin
        let originPath = "\(editionsPrefix)\(newEditionId)/.origin"
        do {
            try await storage.write(path: originPath, data: "\(baseEdition)".data(using: .utf8)!)
        } catch let error as StorageError {
            // Clean up placeholder on failure
            try? await storage.delete(path: workingFile)
            throw ContentError.storageError(underlying: error)
        }

        // Update working file with real state
        let state = SessionState(edition: newEditionId, base: baseEdition, source: source)
        do {
            let data = try JSONEncoder().encode(state)
            try await storage.write(path: workingFile, data: data)
        } catch let error as StorageError {
            // Clean up placeholder on failure
            try? await storage.delete(path: workingFile)
            throw ContentError.storageError(underlying: error)
        } catch {
            try? await storage.delete(path: workingFile)
            throw ContentError.storageError(underlying: .ioError(error.localizedDescription))
        }

        // Update session state
        _mode = .editing(label: label)
        _editionId = newEditionId
        _baseEditionId = baseEdition
        _checkoutSource = source
    }

    /// Validate label for working file name.
    private func isValidLabel(_ label: String) -> Bool {
        guard !label.isEmpty else { return false }
        guard !label.hasPrefix(".") else { return false }
        guard !label.contains("/") else { return false }
        guard !label.contains("..") else { return false }
        return true
    }

    // MARK: - Read Operations

    /// Read file content, resolving through ancestry chain.
    ///
    /// - Parameter path: Content path (e.g., "articles/hello.md")
    /// - Returns: File content
    /// - Throws: `ContentError.invalidPath`, `ContentError.notFound`
    public func read(path: String) async throws -> Data {
        try validateContentPath(path)

        let stat = try await statInternal(path: path)
        switch stat.status {
        case .exists:
            guard let hash = stat.hash else {
                throw ContentError.notFound(path: path)
            }
            return try await readObject(hash: hash)
        case .deleted, .notFound:
            throw ContentError.notFound(path: path)
        }
    }

    /// Check if file exists (false for tombstoned files).
    ///
    /// - Parameter path: Content path
    /// - Returns: true if file exists and is not tombstoned
    /// - Throws: `ContentError.invalidPath`
    public func exists(path: String) async throws -> Bool {
        try validateContentPath(path)
        let stat = try await statInternal(path: path)
        return stat.status == .exists
    }

    /// Get file metadata without fetching content.
    ///
    /// - Parameter path: Content path
    /// - Returns: File metadata including status
    /// - Throws: `ContentError.invalidPath`
    public func stat(path: String) async throws -> FileStat {
        try validateContentPath(path)
        return try await statInternal(path: path)
    }

    /// Internal stat implementation without path validation.
    private func statInternal(path: String) async throws -> FileStat {
        var currentEdition = _editionId

        while true {
            let pathFile = "\(editionsPrefix)\(currentEdition)/\(path)"

            do {
                let content = try await storage.read(path: pathFile)
                guard let text = String(data: content, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines) else {
                    throw ContentError.storageError(underlying: .ioError("Invalid path file encoding"))
                }

                if text == "deleted" {
                    return FileStat(path: path, status: .deleted, resolvedFrom: currentEdition)
                }

                if text.hasPrefix("sha256:") {
                    let hash = String(text.dropFirst(7))
                    // Verify object exists before reporting .exists
                    let size = try await verifyAndGetObjectSize(hash: hash, path: path)
                    return FileStat(path: path, status: .exists, resolvedFrom: currentEdition, hash: hash, size: size)
                }

                throw ContentError.storageError(underlying: .ioError("Invalid path file format: \(text)"))
            } catch StorageError.notFound {
                // File not in this edition, check parent
                guard let parent = try await parentEdition(of: currentEdition) else {
                    return FileStat(path: path, status: .notFound, resolvedFrom: currentEdition)
                }
                currentEdition = parent
            } catch let error as StorageError {
                throw ContentError.storageError(underlying: error)
            }
        }
    }

    /// Get parent edition from .origin file.
    /// - Throws: `ContentError.storageError` for I/O errors, `ContentError.integrityError` for corrupt .origin
    private func parentEdition(of edition: Int) async throws -> Int? {
        // Check for .flattened marker first
        let flattenedPath = "\(editionsPrefix)\(edition)/.flattened"
        do {
            if try await storage.exists(path: flattenedPath) {
                return nil  // Stop traversal at flattened editions
            }
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        let originPath = "\(editionsPrefix)\(edition)/.origin"
        do {
            let data = try await storage.read(path: originPath)
            guard let text = String(data: data, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines) else {
                throw ContentError.integrityError(expected: "valid UTF-8 in .origin", actual: "invalid encoding")
            }
            guard let parent = Int(text) else {
                throw ContentError.integrityError(expected: "integer edition ID in .origin", actual: text)
            }
            return parent
        } catch StorageError.notFound {
            return nil  // No .origin means this is a root edition
        } catch let error as ContentError {
            throw error
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }
    }

    /// Read object content by hash.
    private func readObject(hash: String) async throws -> Data {
        let shard = String(hash.prefix(2))
        let objectPath = "\(objectsPrefix)\(shard)/\(hash).dat"
        do {
            return try await storage.read(path: objectPath)
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }
    }

    /// Verify object exists and get its size.
    /// - Throws: `ContentError.integrityError` if object is missing
    private func verifyAndGetObjectSize(hash: String, path: String) async throws -> Int {
        let shard = String(hash.prefix(2))
        let objectPath = "\(objectsPrefix)\(shard)/\(hash).dat"
        do {
            let data = try await storage.read(path: objectPath)
            return data.count
        } catch StorageError.notFound {
            throw ContentError.integrityError(expected: hash, actual: "missing")
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }
    }

    /// Validate content path.
    private func validateContentPath(_ path: String) throws {
        guard !path.isEmpty else {
            throw ContentError.invalidPath(path)
        }
        guard !path.contains("..") else {
            throw ContentError.invalidPath(path)
        }
        guard !path.hasPrefix(".") else {
            throw ContentError.invalidPath(path)
        }
        guard !path.hasPrefix("/") else {
            throw ContentError.invalidPath(path)
        }
        // Check for components starting with .
        let components = path.split(separator: "/")
        for component in components {
            if component.hasPrefix(".") {
                throw ContentError.invalidPath(path)
            }
        }
    }

    // MARK: - List

    /// List immediate children of a directory.
    ///
    /// Results are merged from ancestry, tombstones excluded.
    /// Returns entries in lexicographic order.
    ///
    /// - Parameter directory: Directory path (e.g., "articles/") or "" for root
    /// - Returns: Array of entry names sorted lexicographically
    /// - Throws: `ContentError.invalidPath` for malformed paths
    public func list(directory: String) async throws -> [String] {
        let entries = try await listInternal(directory: directory)
        return entries.sorted()
    }

    /// Internal list implementation.
    private func listInternal(directory: String) async throws -> Set<String> {
        // Validate directory path (allow empty for root)
        if !directory.isEmpty {
            // Remove trailing slash for validation
            let pathToValidate = directory.hasSuffix("/") ? String(directory.dropLast()) : directory
            try validateContentPath(pathToValidate)
        }

        // Normalize directory path
        let normalizedDir = directory.isEmpty ? "" : (directory.hasSuffix("/") ? directory : directory + "/")

        var allEntries: [String: EntryInfo] = [:]  // name -> (edition, isDeleted)
        var currentEdition = _editionId

        while true {
            let prefix = "\(editionsPrefix)\(currentEdition)/\(normalizedDir)"

            do {
                let keys = try await storage.list(prefix: prefix, delimiter: "/")

                for key in keys {
                    // Extract entry name from full path
                    let entryName: String
                    if key.hasSuffix("/") {
                        // Subdirectory
                        let withoutSlash = String(key.dropLast())
                        entryName = String(withoutSlash.split(separator: "/").last ?? "") + "/"
                    } else {
                        // File
                        entryName = String(key.split(separator: "/").last ?? "")
                    }

                    guard !entryName.isEmpty, !entryName.hasPrefix(".") else { continue }

                    // Only record if not already seen (child overrides parent)
                    if allEntries[entryName] == nil {
                        // For files, check if tombstoned
                        if !entryName.hasSuffix("/") {
                            let pathFile = "\(editionsPrefix)\(currentEdition)/\(normalizedDir)\(entryName)"
                            do {
                                let content = try await storage.read(path: pathFile)
                                if let text = String(data: content, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines) {
                                    let isDeleted = text == "deleted"
                                    allEntries[entryName] = EntryInfo(edition: currentEdition, isDeleted: isDeleted)
                                }
                            } catch {
                                // Skip entries we can't read
                            }
                        } else {
                            // Directory - mark as existing (will filter later based on contents)
                            allEntries[entryName] = EntryInfo(edition: currentEdition, isDeleted: false)
                        }
                    }
                }
            } catch let error as StorageError {
                throw ContentError.storageError(underlying: error)
            }

            // Move to parent
            guard let parent = try await parentEdition(of: currentEdition) else {
                break
            }
            currentEdition = parent
        }

        // Filter out deleted entries
        return Set(allEntries.filter { !$0.value.isDeleted }.keys)
    }
}

/// Helper for tracking entry info during list merge.
private struct EntryInfo {
    let edition: Int
    let isDeleted: Bool
}
