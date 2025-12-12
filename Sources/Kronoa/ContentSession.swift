import Foundation
import CryptoKit

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

    // Transaction state
    private var _isInTransaction: Bool = false
    private var _pendingChanges: [String: BufferedChange] = [:]  // path -> change

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

    /// Whether currently in a transaction.
    public nonisolated var isInTransaction: Bool {
        get async { await _isInTransaction }
    }

    /// Get list of buffered changes (for preview before commit).
    public nonisolated var pendingChanges: [PendingChange] {
        get async {
            await _pendingChanges.map { path, change in
                PendingChange(path: path, action: change.action)
            }.sorted { $0.path < $1.path }
        }
    }

    /// Create a new content session.
    ///
    /// - Parameters:
    ///   - storage: Storage backend to use
    ///   - mode: Session mode (.production, .staging, .editing(label:) to resume, or .edition(id:) for direct access)
    /// - Throws: `ContentError.storageError` if edition pointer cannot be read,
    ///           `ContentError.notFound` if editing session doesn't exist,
    ///           `ContentError.editionNotFound` if edition doesn't exist (for .edition mode)
    public init(storage: StorageBackend, mode: SessionMode) async throws {
        self.storage = storage

        switch mode {
        case .production:
            self._mode = mode
            self._editionId = try await Self.readEditionPointer(storage: storage, file: "contents/.production.json")
            self._baseEditionId = nil
            self._checkoutSource = nil

        case .staging:
            self._mode = mode
            self._editionId = try await Self.readEditionPointer(storage: storage, file: "contents/.staging.json")
            self._baseEditionId = nil
            self._checkoutSource = nil

        case .editing(let label):
            // Resume existing editing session from .{label}.json
            let workingFile = "contents/.\(label).json"
            let state: SessionState
            do {
                let data = try await storage.read(path: workingFile)
                state = try JSONDecoder().decode(SessionState.self, from: data)
            } catch StorageError.notFound {
                throw ContentError.notFound(path: workingFile)
            } catch let error as StorageError {
                throw ContentError.storageError(underlying: error)
            } catch {
                throw ContentError.storageError(underlying: .ioError(error.localizedDescription))
            }
            self._mode = mode
            self._editionId = state.edition
            self._baseEditionId = state.base
            self._checkoutSource = state.source

        case .submitted:
            fatalError("Cannot initialize with .submitted mode")

        case .edition(let id):
            // Read-only access to a specific edition by ID
            // Useful for previewing pending editions or viewing history
            // Verify edition exists: check .origin (normal) or .flattened (genesis/flattened)
            let editionPath = "contents/editions/\(id)/"
            do {
                let hasOrigin = try await storage.exists(path: editionPath + ".origin")
                let hasFlattened = try await storage.exists(path: editionPath + ".flattened")
                if !hasOrigin && !hasFlattened {
                    throw ContentError.editionNotFound(edition: id)
                }
            } catch let error as StorageError {
                throw ContentError.storageError(underlying: error)
            }
            self._mode = mode
            self._editionId = id
            self._baseEditionId = nil
            self._checkoutSource = nil
        }
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
        case .editing, .submitted, .edition:
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
            case .edition(let editionId):
                // Verify the edition exists by checking for .origin or .flattened
                let originPath = "\(editionsPrefix)\(editionId)/.origin"
                let flattenedPath = "\(editionsPrefix)\(editionId)/.flattened"
                let originExists = try? await storage.read(path: originPath)
                let flattenedExists = try? await storage.read(path: flattenedPath)
                guard originExists != nil || flattenedExists != nil else {
                    throw ContentError.editionNotFound(edition: editionId)
                }
                baseEdition = editionId
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
    /// In a transaction, sees buffered changes.
    ///
    /// - Parameter path: Content path (e.g., "articles/hello.md")
    /// - Returns: File content
    /// - Throws: `ContentError.invalidPath`, `ContentError.notFound`
    public func read(path: String) async throws -> Data {
        try validateContentPath(path)

        // Check pending changes first (transaction-aware)
        if let pending = _pendingChanges[path] {
            switch pending.action {
            case .write(let hash, _):
                // Return buffered data if available, otherwise read from storage
                if let data = pending.data {
                    return data
                }
                return try await readObject(hash: hash)
            case .delete:
                throw ContentError.notFound(path: path)
            }
        }

        // Use statInternal for consistency with stat() (both see pending changes)
        let fileStat = try await statInternal(path: path)
        switch fileStat.status {
        case .exists:
            guard let hash = fileStat.hash else {
                throw ContentError.notFound(path: path)
            }
            return try await readObject(hash: hash)
        case .deleted, .notFound:
            throw ContentError.notFound(path: path)
        }
    }

    /// Check if file exists (false for tombstoned files).
    /// In a transaction, sees buffered changes.
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
    /// In a transaction, sees buffered changes.
    ///
    /// - Parameter path: Content path
    /// - Returns: File metadata including status
    /// - Throws: `ContentError.invalidPath`
    public func stat(path: String) async throws -> FileStat {
        try validateContentPath(path)
        return try await statInternal(path: path)
    }

    /// Internal stat implementation (transaction-aware).
    private func statInternal(path: String) async throws -> FileStat {
        // Check pending changes first
        if let pending = _pendingChanges[path] {
            switch pending.action {
            case .write(let hash, let size):
                return FileStat(path: path, status: .exists, resolvedFrom: _editionId, hash: hash, size: size)
            case .delete:
                return FileStat(path: path, status: .deleted, resolvedFrom: _editionId)
            }
        }
        return try await statFromStorage(path: path)
    }

    /// Stat from storage only (ignores pending changes).
    private func statFromStorage(path: String) async throws -> FileStat {
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
    /// In a transaction, sees buffered changes.
    ///
    /// Results are merged from ancestry, tombstones excluded.
    /// Returns entries in lexicographic order.
    ///
    /// - Parameter directory: Directory path (e.g., "articles/") or "" for root
    /// - Returns: Array of entry names sorted lexicographically
    /// - Throws: `ContentError.invalidPath` for malformed paths
    public func list(directory: String) async throws -> [String] {
        let entries = try await listInternalWithInfo(directory: directory)
        return entries.keys.sorted()
    }

    /// List directory contents with metadata (hash and source edition).
    ///
    /// Returns immediate children only (files and subdirectories).
    /// For files, includes the content hash. For directories, hash is nil.
    /// Merges results from ancestry chain, with child editions overriding parents.
    /// Excludes tombstoned (deleted) entries.
    ///
    /// - Parameter directory: Directory path (e.g., "articles/") or "" for root
    /// - Returns: Array of ListEntry sorted by name
    /// - Throws: `ContentError.invalidPath` for malformed paths
    public func listWithMetadata(directory: String) async throws -> [ListEntry] {
        let entries = try await listInternalWithInfo(directory: directory)
        return entries.map { name, info in
            ListEntry(
                name: name,
                isDirectory: name.hasSuffix("/"),
                hash: info.hash,
                resolvedFrom: info.edition
            )
        }.sorted { $0.name < $1.name }
    }

    /// Internal list implementation with full entry info (transaction-aware).
    private func listInternalWithInfo(directory: String) async throws -> [String: EntryInfo] {
        // Validate directory path (allow empty for root)
        if !directory.isEmpty {
            // Remove trailing slash for validation
            let pathToValidate = directory.hasSuffix("/") ? String(directory.dropLast()) : directory
            try validateContentPath(pathToValidate)
        }

        // Normalize directory path
        let normalizedDir = directory.isEmpty ? "" : (directory.hasSuffix("/") ? directory : directory + "/")

        // First, collect pending changes that affect this directory
        // Track subdirs with their write/delete counts to determine visibility
        var pendingEntries: [String: Bool] = [:]  // name -> isDeleted
        var subdirWrites: [String: Int] = [:]     // subdir -> count of non-delete changes
        var subdirDeletes: [String: Int] = [:]    // subdir -> count of delete changes

        for (path, change) in _pendingChanges {
            let isDeleted = change.action == .delete

            // Check if this path is in the target directory
            if normalizedDir.isEmpty {
                // Root directory: only include paths without /
                if !path.contains("/") {
                    pendingEntries[path] = isDeleted
                } else {
                    // It's in a subdirectory - track writes vs deletes
                    let subdir = String(path.split(separator: "/").first!) + "/"
                    if isDeleted {
                        subdirDeletes[subdir, default: 0] += 1
                    } else {
                        subdirWrites[subdir, default: 0] += 1
                    }
                }
            } else if path.hasPrefix(normalizedDir) {
                let remainder = String(path.dropFirst(normalizedDir.count))
                if !remainder.contains("/") {
                    // Direct child file
                    pendingEntries[remainder] = isDeleted
                } else {
                    // Nested - track writes vs deletes for subdirectory
                    let subdir = String(remainder.split(separator: "/").first!) + "/"
                    if isDeleted {
                        subdirDeletes[subdir, default: 0] += 1
                    } else {
                        subdirWrites[subdir, default: 0] += 1
                    }
                }
            }
        }

        // Determine subdir visibility based on pending changes
        // A subdir with any writes is visible; a subdir with only deletes should be hidden
        let allSubdirs = Set(subdirWrites.keys).union(subdirDeletes.keys)
        for subdir in allSubdirs {
            let writes = subdirWrites[subdir, default: 0]
            let deletes = subdirDeletes[subdir, default: 0]
            if writes > 0 {
                // Has at least one write - subdir should be visible
                pendingEntries[subdir] = false
            } else if deletes > 0 {
                // Only deletes - mark subdir as deleted to suppress it from storage results
                pendingEntries[subdir] = true
            }
        }

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
                        // For files, check if tombstoned and extract hash
                        if !entryName.hasSuffix("/") {
                            let pathFile = "\(editionsPrefix)\(currentEdition)/\(normalizedDir)\(entryName)"
                            do {
                                let content = try await storage.read(path: pathFile)
                                guard let text = String(data: content, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines) else {
                                    throw ContentError.integrityError(
                                        expected: "valid UTF-8 content",
                                        actual: "invalid UTF-8 in \(pathFile)"
                                    )
                                }
                                if text == "deleted" {
                                    allEntries[entryName] = EntryInfo(edition: currentEdition, isDeleted: true, hash: nil)
                                } else if text.hasPrefix("sha256:") {
                                    let hash = String(text.dropFirst(7))
                                    allEntries[entryName] = EntryInfo(edition: currentEdition, isDeleted: false, hash: hash)
                                } else {
                                    throw ContentError.integrityError(
                                        expected: "sha256:<hash> or deleted",
                                        actual: "'\(text)' in \(pathFile)"
                                    )
                                }
                            } catch let error as ContentError {
                                throw error
                            } catch let error as StorageError {
                                throw ContentError.storageError(underlying: error)
                            }
                        } else {
                            // Directory - mark as existing (will filter later based on contents)
                            allEntries[entryName] = EntryInfo(edition: currentEdition, isDeleted: false, hash: nil)
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

        // Merge pending entries (they take precedence)
        for (name, isDeleted) in pendingEntries {
            // Get hash from pending changes if it's a file
            var hash: String? = nil
            if !name.hasSuffix("/") && !isDeleted {
                let fullPath = normalizedDir + name
                if let change = _pendingChanges[fullPath], case .write(let h, _) = change.action {
                    hash = h
                }
            }
            allEntries[name] = EntryInfo(edition: _editionId, isDeleted: isDeleted, hash: hash)
        }

        // Filter out deleted entries and return
        return allEntries.filter { !$0.value.isDeleted }
    }

    // MARK: - Transactional Editing

    /// Begin a transaction (changes buffered in memory).
    /// - Throws: `ContentError.alreadyInTransaction`, `ContentError.readOnlyMode`
    public func beginEditing() async throws {
        guard case .editing = _mode else {
            throw ContentError.readOnlyMode
        }
        guard !_isInTransaction else {
            throw ContentError.alreadyInTransaction
        }
        _isInTransaction = true
    }

    /// Flush buffered changes to storage.
    /// Writes objects and path files. Does NOT update .ref files (that happens in stage()).
    /// - Throws: `ContentError.notInTransaction`, `ContentError.storageError`
    public func endEditing() async throws {
        guard _isInTransaction else {
            throw ContentError.notInTransaction
        }

        // Write all buffered changes
        for (path, change) in _pendingChanges {
            switch change.action {
            case .write(let hash, _):
                // Write object if we have data buffered
                if let data = change.data {
                    try await writeObject(hash: hash, data: data)
                }
                // Write path file
                let pathFile = "\(editionsPrefix)\(_editionId)/\(path)"
                do {
                    try await storage.write(path: pathFile, data: "sha256:\(hash)".data(using: .utf8)!)
                } catch let error as StorageError {
                    throw ContentError.storageError(underlying: error)
                }

            case .delete:
                // Write tombstone
                let pathFile = "\(editionsPrefix)\(_editionId)/\(path)"
                do {
                    try await storage.write(path: pathFile, data: "deleted".data(using: .utf8)!)
                } catch let error as StorageError {
                    throw ContentError.storageError(underlying: error)
                }
            }
        }

        // Clear transaction state
        _pendingChanges.removeAll()
        _isInTransaction = false
    }

    /// Discard buffered changes without writing.
    /// - Throws: `ContentError.notInTransaction`
    public func rollback() async throws {
        guard _isInTransaction else {
            throw ContentError.notInTransaction
        }
        _pendingChanges.removeAll()
        _isInTransaction = false
    }

    // MARK: - Write Operations

    /// Write file content (editing mode only).
    /// If in a transaction, buffers the change. Otherwise, auto-commits.
    /// - Throws: `ContentError.invalidPath`, `ContentError.readOnlyMode`
    public func write(path: String, data: Data) async throws {
        try validateContentPath(path)

        guard case .editing = _mode else {
            throw ContentError.readOnlyMode
        }

        // Compute hash
        let hash = computeSHA256(data)

        if _isInTransaction {
            // Buffer the change
            _pendingChanges[path] = BufferedChange(
                action: .write(hash: hash, size: data.count),
                data: data
            )
        } else {
            // Auto-commit: write object and path file directly
            try await writeObject(hash: hash, data: data)
            let pathFile = "\(editionsPrefix)\(_editionId)/\(path)"
            do {
                try await storage.write(path: pathFile, data: "sha256:\(hash)".data(using: .utf8)!)
            } catch let error as StorageError {
                throw ContentError.storageError(underlying: error)
            }
        }
    }

    /// Delete file - creates tombstone (editing mode only).
    /// If in a transaction, buffers the change. Otherwise, auto-commits.
    /// - Throws: `ContentError.invalidPath`, `ContentError.readOnlyMode`
    public func delete(path: String) async throws {
        try validateContentPath(path)

        guard case .editing = _mode else {
            throw ContentError.readOnlyMode
        }

        if _isInTransaction {
            // Buffer the deletion
            _pendingChanges[path] = BufferedChange(action: .delete, data: nil)
        } else {
            // Auto-commit: write tombstone directly
            let pathFile = "\(editionsPrefix)\(_editionId)/\(path)"
            do {
                try await storage.write(path: pathFile, data: "deleted".data(using: .utf8)!)
            } catch let error as StorageError {
                throw ContentError.storageError(underlying: error)
            }
        }
    }

    /// Copy file within the edition (no data transfer, just hash reference).
    /// - Throws: `ContentError.invalidPath`, `ContentError.readOnlyMode`, `ContentError.notFound`
    public func copy(from sourcePath: String, to destPath: String) async throws {
        try validateContentPath(sourcePath)
        try validateContentPath(destPath)

        guard case .editing = _mode else {
            throw ContentError.readOnlyMode
        }

        // Check pending changes first for source (transaction-aware)
        if let pendingSource = _pendingChanges[sourcePath] {
            switch pendingSource.action {
            case .write(let hash, let size):
                if _isInTransaction {
                    // Copy buffered data reference
                    _pendingChanges[destPath] = BufferedChange(
                        action: .write(hash: hash, size: size),
                        data: pendingSource.data  // Carry forward buffered data if present
                    )
                } else {
                    // Auto-commit: write path file pointing to same hash
                    let pathFile = "\(editionsPrefix)\(_editionId)/\(destPath)"
                    do {
                        try await storage.write(path: pathFile, data: "sha256:\(hash)".data(using: .utf8)!)
                    } catch let error as StorageError {
                        throw ContentError.storageError(underlying: error)
                    }
                }
                return
            case .delete:
                throw ContentError.notFound(path: sourcePath)
            }
        }

        // Get source file's hash from storage
        let stat = try await statFromStorage(path: sourcePath)
        guard stat.status == .exists, let hash = stat.hash, let size = stat.size else {
            throw ContentError.notFound(path: sourcePath)
        }

        if _isInTransaction {
            // Buffer the copy (no data needed, object already in storage)
            _pendingChanges[destPath] = BufferedChange(
                action: .write(hash: hash, size: size),
                data: nil
            )
        } else {
            // Auto-commit: write path file pointing to same hash
            let pathFile = "\(editionsPrefix)\(_editionId)/\(destPath)"
            do {
                try await storage.write(path: pathFile, data: "sha256:\(hash)".data(using: .utf8)!)
            } catch let error as StorageError {
                throw ContentError.storageError(underlying: error)
            }
        }
    }

    /// Discard local change to a path (editing mode only).
    /// Removes the path from this edition, so it resolves through ancestry.
    /// - Throws: `ContentError.invalidPath`, `ContentError.readOnlyMode`
    public func discard(path: String) async throws {
        try validateContentPath(path)

        guard case .editing = _mode else {
            throw ContentError.readOnlyMode
        }

        if _isInTransaction {
            // Remove from pending changes
            _pendingChanges.removeValue(forKey: path)
        }

        // Also remove from storage if already written
        let pathFile = "\(editionsPrefix)\(_editionId)/\(path)"
        do {
            try await storage.delete(path: pathFile)
        } catch StorageError.notFound {
            // Already gone, that's fine
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }
    }

    // MARK: - Publishing Workflow

    /// Submit edition for review.
    /// Creates `.pending/{edition}.json` with submission metadata.
    /// Transitions mode from `.editing` to `.submitted`.
    /// - Parameter message: Submission message describing the changes
    /// - Throws: `ContentError.notInEditingMode`, `ContentError.storageError`
    public func submit(message: String) async throws {
        guard case .editing(let label) = _mode else {
            throw ContentError.notInEditingMode
        }

        guard let baseEdition = _baseEditionId, let source = _checkoutSource else {
            throw ContentError.notInEditingMode
        }

        // Ensure no uncommitted transaction
        if _isInTransaction {
            try await endEditing()
        }

        // Create pending submission record
        let submission = PendingSubmission(
            edition: _editionId,
            base: baseEdition,
            source: source,
            label: label,
            message: message,
            submittedAt: Date()
        )

        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(submission)

        // Write to .pending/{edition}.json
        let pendingPath = "\(contentsPrefix).pending/\(_editionId).json"
        do {
            try await storage.write(path: pendingPath, data: data)
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        // Remove working file
        let workingFile = "\(contentsPrefix).\(label).json"
        do {
            try await storage.delete(path: workingFile)
        } catch StorageError.notFound {
            // Already gone, that's fine
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        // Transition to submitted mode
        _mode = .submitted
    }

    /// List pending submissions awaiting review.
    /// - Returns: Array of pending submissions sorted by edition ID
    /// - Throws: `ContentError.storageError`
    public func listPending() async throws -> [PendingSubmission] {
        let pendingPrefix = "\(contentsPrefix).pending/"

        let keys: [String]
        do {
            keys = try await storage.list(prefix: pendingPrefix, delimiter: nil)
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        var submissions: [PendingSubmission] = []
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601

        for key in keys {
            guard key.hasSuffix(".json") else { continue }

            do {
                let data = try await storage.read(path: key)
                let submission = try decoder.decode(PendingSubmission.self, from: data)
                submissions.append(submission)
            } catch {
                // Skip corrupt files - they'll be caught by stage()
                continue
            }
        }

        return submissions.sorted { $0.edition < $1.edition }
    }

    /// List all rejected submissions.
    /// - Returns: Array of rejected submissions, sorted by edition ID
    public func listRejected() async throws -> [RejectedSubmission] {
        let rejectedPrefix = "\(contentsPrefix).rejected/"

        let keys: [String]
        do {
            keys = try await storage.list(prefix: rejectedPrefix, delimiter: nil)
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        var rejections: [RejectedSubmission] = []
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601

        for key in keys {
            guard key.hasSuffix(".json") else { continue }

            do {
                let data = try await storage.read(path: key)
                let rejection = try decoder.decode(RejectedSubmission.self, from: data)
                rejections.append(rejection)
            } catch {
                // Skip corrupt files
                continue
            }
        }

        return rejections.sorted { $0.edition < $1.edition }
    }

    /// Get rejection record for a specific edition.
    /// - Parameter edition: Edition ID to look up
    /// - Returns: Rejection record, or nil if not found
    /// - Throws: `ContentError.rejectedCorrupt` if file exists but JSON is invalid
    public func getRejection(edition: Int) async throws -> RejectedSubmission? {
        let rejectedPath = "\(contentsPrefix).rejected/\(edition).json"

        let data: Data
        do {
            data = try await storage.read(path: rejectedPath)
        } catch StorageError.notFound {
            return nil
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        do {
            return try decoder.decode(RejectedSubmission.self, from: data)
        } catch {
            throw ContentError.rejectedCorrupt(edition: edition, reason: error.localizedDescription)
        }
    }

    /// Accept a submission into staging.
    /// Validates pending exists, checks for conflicts, updates .ref files, updates staging pointer.
    /// - Parameter edition: Edition ID to stage
    /// - Throws: `ContentError.pendingNotFound`, `ContentError.pendingCorrupt`,
    ///           `ContentError.conflictDetected`, `ContentError.lockTimeout`, `ContentError.lockExpired`
    public func stage(edition: Int) async throws {
        // Acquire lock
        let lockPath = "\(contentsPrefix).lock"
        let lock: LockHandle
        do {
            lock = try await storage.acquireLock(path: lockPath, timeout: 30, leaseDuration: 60)
        } catch StorageError.lockTimeout {
            throw ContentError.lockTimeout
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        // Ensure lock is always released
        var caughtError: Error?
        do {
            try await stageWithLock(edition: edition, lock: lock)
        } catch {
            caughtError = error
        }

        // Always release lock - propagate release failure if no prior error
        do {
            try await lock.release()
        } catch {
            if caughtError == nil {
                throw ContentError.lockExpired
            }
            // If there was already an error, prioritize that over release failure
        }

        // Re-throw if there was an error
        if let error = caughtError {
            throw error
        }
    }

    /// Internal stage implementation that assumes lock is held.
    private func stageWithLock(edition: Int, lock: LockHandle) async throws {
        let pendingPath = "\(contentsPrefix).pending/\(edition).json"

        // Read and validate pending submission
        let pendingData: Data
        do {
            pendingData = try await storage.read(path: pendingPath)
        } catch StorageError.notFound {
            throw ContentError.pendingNotFound(edition: edition)
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let submission: PendingSubmission
        do {
            submission = try decoder.decode(PendingSubmission.self, from: pendingData)
        } catch {
            throw ContentError.pendingCorrupt(edition: edition, reason: error.localizedDescription)
        }

        // Verify edition's .origin matches pending metadata (guard against tampered/corrupt pending file)
        let originPath = "\(editionsPrefix)\(edition)/.origin"
        let actualOrigin: Int
        do {
            let originData = try await storage.read(path: originPath)
            guard let originText = String(data: originData, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines),
                  let origin = Int(originText) else {
                throw ContentError.pendingCorrupt(edition: edition, reason: "Invalid .origin format")
            }
            actualOrigin = origin
        } catch StorageError.notFound {
            throw ContentError.pendingCorrupt(edition: edition, reason: "Edition .origin not found")
        } catch let error as ContentError {
            throw error
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        if actualOrigin != submission.base {
            throw ContentError.pendingCorrupt(
                edition: edition,
                reason: "Pending base (\(submission.base)) doesn't match edition .origin (\(actualOrigin))"
            )
        }

        // Conflict check based on source
        let currentPointer: Int
        switch submission.source {
        case .staging:
            currentPointer = try await Self.readEditionPointer(storage: storage, file: "\(contentsPrefix).staging.json")
        case .production:
            currentPointer = try await Self.readEditionPointer(storage: storage, file: "\(contentsPrefix).production.json")
        case .edition(let sourceEdition):
            // For edition-based checkout, base should match the source edition
            currentPointer = sourceEdition
        }

        if submission.base != currentPointer {
            throw ContentError.conflictDetected(base: submission.base, current: currentPointer, source: submission.source)
        }

        // Update .ref files for all objects in this edition
        try await updateRefFiles(forEdition: edition, lock: lock)

        // Update staging pointer
        let stagingPointer = EditionPointer(edition: edition)
        let encoder = JSONEncoder()
        let pointerData = try encoder.encode(stagingPointer)
        do {
            try await storage.write(path: "\(contentsPrefix).staging.json", data: pointerData)
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        // Remove pending file
        do {
            try await storage.delete(path: pendingPath)
        } catch StorageError.notFound {
            // Already gone
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }
    }

    /// Deploy staging to production.
    /// Copies staging pointer to production pointer.
    /// - Throws: `ContentError.lockTimeout`, `ContentError.lockExpired`, `ContentError.storageError`
    public func deploy() async throws {
        // Acquire lock
        let lockPath = "\(contentsPrefix).lock"
        let lock: LockHandle
        do {
            lock = try await storage.acquireLock(path: lockPath, timeout: 30, leaseDuration: 60)
        } catch StorageError.lockTimeout {
            throw ContentError.lockTimeout
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        // Ensure lock is always released
        var caughtError: Error?
        do {
            try await deployWithLock()
        } catch {
            caughtError = error
        }

        // Always release lock - propagate release failure if no prior error
        do {
            try await lock.release()
        } catch {
            if caughtError == nil {
                throw ContentError.lockExpired
            }
            // If there was already an error, prioritize that over release failure
        }

        // Re-throw if there was an error
        if let error = caughtError {
            throw error
        }
    }

    /// Internal deploy implementation that assumes lock is held.
    private func deployWithLock() async throws {
        // Read staging pointer
        let stagingData: Data
        do {
            stagingData = try await storage.read(path: "\(contentsPrefix).staging.json")
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        // Write to production
        do {
            try await storage.write(path: "\(contentsPrefix).production.json", data: stagingData)
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }
    }

    /// Update .ref files for all objects in an edition.
    /// - Parameters:
    ///   - edition: Edition ID
    ///   - lock: Lock handle for renewal during long operations
    private func updateRefFiles(forEdition edition: Int, lock: LockHandle) async throws {
        let editionPrefix = "\(editionsPrefix)\(edition)/"

        // List all path files in the edition
        let keys: [String]
        do {
            keys = try await storage.list(prefix: editionPrefix, delimiter: nil)
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        var processedCount = 0
        let renewInterval = 20  // Renew lock every 20 objects

        for key in keys {
            // Skip directories (they have trailing slash)
            if key.hasSuffix("/") { continue }

            // Skip metadata files
            let filename = key.replacingOccurrences(of: editionPrefix, with: "")
            if filename.hasPrefix(".") { continue }

            // Read path file to get hash
            do {
                let content = try await storage.read(path: key)
                guard let text = String(data: content, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines) else {
                    continue
                }

                // Skip tombstones
                if text == "deleted" { continue }

                // Extract hash from "sha256:{hash}"
                guard text.hasPrefix("sha256:") else { continue }
                let hash = String(text.dropFirst(7))

                // Update .ref file
                try await appendToRefFile(hash: hash, edition: edition)

                processedCount += 1
                if processedCount % renewInterval == 0 {
                    // Renew lock periodically
                    do {
                        try await lock.renew(duration: 60)
                    } catch {
                        throw ContentError.lockExpired
                    }
                }
            } catch StorageError.notFound {
                continue
            } catch let error as StorageError {
                throw ContentError.storageError(underlying: error)
            }
        }
    }

    /// Append edition ID to an object's .ref file.
    private func appendToRefFile(hash: String, edition: Int) async throws {
        let shard = String(hash.prefix(2))
        let refPath = "\(objectsPrefix)\(shard)/\(hash).ref"

        // Read existing .ref file (if any)
        var editions: Set<Int> = []
        do {
            let data = try await storage.read(path: refPath)
            if let content = String(data: data, encoding: .utf8) {
                for line in content.split(separator: "\n") {
                    if let id = Int(line.trimmingCharacters(in: .whitespaces)) {
                        editions.insert(id)
                    }
                }
            }
        } catch StorageError.notFound {
            // No existing .ref file
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        // Add new edition
        editions.insert(edition)

        // Write updated .ref file
        let content = editions.sorted().map { String($0) }.joined(separator: "\n")
        do {
            try await storage.write(path: refPath, data: content.data(using: .utf8)!)
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }
    }

    // MARK: - Edition Info

    /// Get the origin (parent) edition ID for a given edition.
    ///
    /// Useful for comparing a pending edition against its base.
    ///
    /// - Parameter edition: Edition ID to query
    /// - Returns: Parent edition ID, or nil for genesis/flattened editions
    /// - Throws: `ContentError.editionNotFound` if edition doesn't exist,
    ///           `ContentError.integrityError` for malformed .origin
    public func origin(of edition: Int) async throws -> Int? {
        // Verify edition exists
        let editionPath = "\(editionsPrefix)\(edition)/"
        do {
            let hasOrigin = try await storage.exists(path: editionPath + ".origin")
            let hasFlattened = try await storage.exists(path: editionPath + ".flattened")
            if !hasOrigin && !hasFlattened {
                throw ContentError.editionNotFound(edition: edition)
            }
            // Flattened editions have no traversable parent
            if hasFlattened {
                return nil
            }
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        return try await getParentEdition(edition)
    }

    /// List files changed locally in an edition (not inherited from ancestors).
    ///
    /// Returns only files explicitly stored in the edition's directory,
    /// showing what was set (added/modified) or deleted (tombstoned).
    ///
    /// This is O(n) where n = files in the edition, not total files in ancestry.
    ///
    /// - Parameter edition: Edition ID to inspect
    /// - Returns: List of local changes (set or deleted), sorted by path
    /// - Throws: `editionNotFound`, `integrityError`, `storageError`
    public func localChanges(in edition: Int) async throws -> [LocalChange] {
        // Verify edition exists
        let editionPath = "\(editionsPrefix)\(edition)/"
        do {
            let hasOrigin = try await storage.exists(path: editionPath + ".origin")
            let hasFlattened = try await storage.exists(path: editionPath + ".flattened")
            if !hasOrigin && !hasFlattened {
                throw ContentError.editionNotFound(edition: edition)
            }
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        // List all files in edition directory
        let entries: [String]
        do {
            entries = try await storage.list(prefix: editionPath, delimiter: nil)
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        var changes: [LocalChange] = []

        for entry in entries {
            // Skip metadata files
            let filename = String(entry.dropFirst(editionPath.count))
            if filename.hasPrefix(".") {
                continue
            }

            // Read path file content
            let content: Data
            do {
                content = try await storage.read(path: entry)
            } catch let error as StorageError {
                throw ContentError.storageError(underlying: error)
            }

            guard let text = String(data: content, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines) else {
                throw ContentError.integrityError(
                    expected: "valid UTF-8 content",
                    actual: "invalid UTF-8 in \(entry)"
                )
            }

            if text == "deleted" {
                changes.append(LocalChange(path: filename, change: .deleted, hash: nil))
            } else if text.hasPrefix("sha256:") {
                let hash = String(text.dropFirst(7))
                changes.append(LocalChange(path: filename, change: .set, hash: hash))
            } else {
                throw ContentError.integrityError(
                    expected: "sha256:<hash> or deleted",
                    actual: "'\(text)' in \(entry)"
                )
            }
        }

        return changes.sorted { $0.path < $1.path }
    }

    // MARK: - Maintenance Operations

    /// Flatten an edition by copying all ancestor mappings into it.
    /// Creates `.flattened` marker so reads stop traversing at this edition.
    /// - Parameter edition: Edition ID to flatten
    /// - Throws: `ContentError.editionNotFound`, `ContentError.lockTimeout`, `ContentError.lockExpired`
    public func flatten(edition: Int) async throws {
        // Acquire lock
        let lockPath = "\(contentsPrefix).lock"
        let lock: LockHandle
        do {
            lock = try await storage.acquireLock(path: lockPath, timeout: 30, leaseDuration: 60)
        } catch StorageError.lockTimeout {
            throw ContentError.lockTimeout
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        var caughtError: Error?
        do {
            try await flattenWithLock(edition: edition, lock: lock)
        } catch {
            caughtError = error
        }

        // Always release lock
        do {
            try await lock.release()
        } catch {
            if caughtError == nil {
                throw ContentError.lockExpired
            }
        }

        if let error = caughtError {
            throw error
        }
    }

    /// Internal flatten implementation.
    private func flattenWithLock(edition: Int, lock: LockHandle) async throws {
        let editionPath = "\(editionsPrefix)\(edition)/"
        let flattenedMarker = "\(editionPath).flattened"

        // Check if edition exists
        let originPath = "\(editionPath).origin"
        let originExists: Bool
        do {
            originExists = try await storage.exists(path: originPath)
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        if !originExists {
            throw ContentError.editionNotFound(edition: edition)
        }

        // Check if already flattened (idempotent)
        do {
            if try await storage.exists(path: flattenedMarker) {
                return  // Already flattened, no-op
            }
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        // Collect all paths from ancestry
        var allPaths: [String: (hash: String?, isDeleted: Bool, fromEdition: Int)] = [:]
        var currentEdition: Int? = edition
        var processedCount = 0
        let renewInterval = 20

        while let editionId = currentEdition {
            let prefix = "\(editionsPrefix)\(editionId)/"

            // List all files in this edition
            let keys: [String]
            do {
                keys = try await storage.list(prefix: prefix, delimiter: nil)
            } catch let error as StorageError {
                throw ContentError.storageError(underlying: error)
            }

            for key in keys {
                if key.hasSuffix("/") { continue }

                let relativePath = key.replacingOccurrences(of: prefix, with: "")
                if relativePath.hasPrefix(".") { continue }  // Skip metadata

                // Only record if not already seen (child takes precedence)
                if allPaths[relativePath] == nil {
                    do {
                        let content = try await storage.read(path: key)
                        guard let text = String(data: content, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines) else {
                            continue
                        }

                        if text == "deleted" {
                            allPaths[relativePath] = (hash: nil, isDeleted: true, fromEdition: editionId)
                        } else if text.hasPrefix("sha256:") {
                            let hash = String(text.dropFirst(7))
                            allPaths[relativePath] = (hash: hash, isDeleted: false, fromEdition: editionId)
                        }

                        processedCount += 1
                        if processedCount % renewInterval == 0 {
                            do {
                                try await lock.renew(duration: 60)
                            } catch {
                                throw ContentError.lockExpired
                            }
                        }
                    } catch StorageError.notFound {
                        continue
                    } catch let error as StorageError {
                        throw ContentError.storageError(underlying: error)
                    }
                }
            }

            // Check for .flattened marker to stop traversal
            let ancestorFlattenedPath = "\(prefix).flattened"
            do {
                if try await storage.exists(path: ancestorFlattenedPath) {
                    break  // Stop at flattened ancestor
                }
            } catch let error as StorageError {
                throw ContentError.storageError(underlying: error)
            }

            // Move to parent
            currentEdition = try await getParentEdition(editionId)
        }

        // Write all collected paths to the target edition (if not from this edition already)
        for (path, info) in allPaths {
            if info.fromEdition == edition { continue }  // Already in this edition

            let targetPath = "\(editionPath)\(path)"
            let content: String
            if info.isDeleted {
                content = "deleted"
            } else if let hash = info.hash {
                content = "sha256:\(hash)"
            } else {
                continue
            }

            do {
                try await storage.write(path: targetPath, data: content.data(using: .utf8)!)
            } catch let error as StorageError {
                throw ContentError.storageError(underlying: error)
            }

            processedCount += 1
            if processedCount % renewInterval == 0 {
                do {
                    try await lock.renew(duration: 60)
                } catch {
                    throw ContentError.lockExpired
                }
            }
        }

        // Create .flattened marker
        do {
            try await storage.write(path: flattenedMarker, data: Data())
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }
    }

    /// Get parent edition ID from .origin file.
    /// - Returns: Parent edition ID, or nil for genesis edition (no .origin file)
    /// - Throws: `ContentError.integrityError` for malformed .origin content
    private func getParentEdition(_ edition: Int) async throws -> Int? {
        let originPath = "\(editionsPrefix)\(edition)/.origin"
        do {
            let data = try await storage.read(path: originPath)
            guard let text = String(data: data, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines) else {
                throw ContentError.integrityError(
                    expected: "valid UTF-8 integer",
                    actual: "invalid UTF-8 data in \(originPath)"
                )
            }
            guard let parentId = Int(text) else {
                throw ContentError.integrityError(
                    expected: "integer",
                    actual: "'\(text)' in \(originPath)"
                )
            }
            return parentId
        } catch StorageError.notFound {
            return nil  // Genesis edition
        } catch let error as ContentError {
            throw error
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }
    }

    /// Set staging pointer to a previously-staged edition (for rollback).
    /// Unlike stage(), this does not require a pending record or update .ref files.
    /// - Parameter edition: Edition ID to set as staging
    /// - Throws: `ContentError.editionNotFound`, `ContentError.lockTimeout`, `ContentError.lockExpired`
    /// - Warning: Only use for editions that were previously staged. Their objects are already
    ///   tracked in .ref files from the original staging operation.
    public func setStagingPointer(to edition: Int) async throws {
        // Acquire lock
        let lockPath = "\(contentsPrefix).lock"
        let lock: LockHandle
        do {
            lock = try await storage.acquireLock(path: lockPath, timeout: 30, leaseDuration: 60)
        } catch StorageError.lockTimeout {
            throw ContentError.lockTimeout
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        var caughtError: Error?
        do {
            try await setStagingPointerWithLock(to: edition)
        } catch {
            caughtError = error
        }

        // Always release lock
        do {
            try await lock.release()
        } catch {
            if caughtError == nil {
                throw ContentError.lockExpired
            }
        }

        if let error = caughtError {
            throw error
        }
    }

    /// Internal setStagingPointer implementation.
    private func setStagingPointerWithLock(to edition: Int) async throws {
        // Validate edition exists
        let editionPath = "\(editionsPrefix)\(edition)/"
        let originPath = "\(editionPath).origin"

        let exists: Bool
        do {
            exists = try await storage.exists(path: originPath)
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        if !exists {
            throw ContentError.editionNotFound(edition: edition)
        }

        // Update staging pointer
        let pointer = EditionPointer(edition: edition)
        let encoder = JSONEncoder()
        let data = try encoder.encode(pointer)

        do {
            try await storage.write(path: "\(contentsPrefix).staging.json", data: data)
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }
    }

    /// Reject a pending submission.
    /// Removes from .pending/ and stores rejection record in .rejected/
    /// - Parameters:
    ///   - edition: Edition ID to reject
    ///   - reason: Reason for rejection
    /// - Throws: `ContentError.pendingNotFound`, `ContentError.lockTimeout`, `ContentError.lockExpired`
    public func reject(edition: Int, reason: String) async throws {
        // Acquire lock
        let lockPath = "\(contentsPrefix).lock"
        let lock: LockHandle
        do {
            lock = try await storage.acquireLock(path: lockPath, timeout: 30, leaseDuration: 60)
        } catch StorageError.lockTimeout {
            throw ContentError.lockTimeout
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        var caughtError: Error?
        do {
            try await rejectWithLock(edition: edition, reason: reason)
        } catch {
            caughtError = error
        }

        // Always release lock
        do {
            try await lock.release()
        } catch {
            if caughtError == nil {
                throw ContentError.lockExpired
            }
        }

        if let error = caughtError {
            throw error
        }
    }

    /// Internal reject implementation.
    private func rejectWithLock(edition: Int, reason: String) async throws {
        let pendingPath = "\(contentsPrefix).pending/\(edition).json"

        // Verify pending exists
        do {
            if try await !storage.exists(path: pendingPath) {
                throw ContentError.pendingNotFound(edition: edition)
            }
        } catch let error as ContentError {
            throw error
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        // Create rejection record
        let rejection = RejectedSubmission(
            edition: edition,
            reason: reason,
            rejectedAt: Date()
        )

        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let rejectionData = try encoder.encode(rejection)

        let rejectedPath = "\(contentsPrefix).rejected/\(edition).json"
        do {
            try await storage.write(path: rejectedPath, data: rejectionData)
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        // Remove pending
        do {
            try await storage.delete(path: pendingPath)
        } catch StorageError.notFound {
            // Already gone
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }
    }

    /// Run garbage collection.
    ///
    /// Scans all objects and classifies them as:
    /// - `skippedByRef`: Live (found in .ref file pointing to live edition)
    /// - `skippedByScan`: Live (found via fallback edition scan)
    /// - `skippedByAge`: Orphaned (deleted when `dryRun: false`, counted when `dryRun: true`)
    ///
    /// Uses .ref files for fast path, falls back to edition scan for objects without refs.
    ///
    /// - Parameter dryRun: When `true` (default), only report what would be deleted without
    ///   actually deleting. Set to `false` to perform actual deletion. **Note:** Actual deletion
    ///   requires mtime support in `StorageBackend` which is not yet implemented - passing
    ///   `dryRun: false` will trigger a `preconditionFailure` until then.
    /// - Returns: Statistics about the GC run
    /// - Throws: `ContentError.lockTimeout`, `ContentError.lockExpired`
    public func gc(dryRun: Bool = true) async throws -> GCResult {
        if !dryRun {
            preconditionFailure("gc(dryRun: false) requires mtime support in StorageBackend (not yet implemented)")
        }
        // Acquire lock
        let lockPath = "\(contentsPrefix).lock"
        let lock: LockHandle
        do {
            lock = try await storage.acquireLock(path: lockPath, timeout: 30, leaseDuration: 60)
        } catch StorageError.lockTimeout {
            throw ContentError.lockTimeout
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        var result: GCResult?
        var caughtError: Error?
        do {
            result = try await gcWithLock(lock: lock)
        } catch {
            caughtError = error
        }

        // Always release lock
        do {
            try await lock.release()
        } catch {
            if caughtError == nil {
                throw ContentError.lockExpired
            }
        }

        if let error = caughtError {
            throw error
        }

        return result!
    }

    /// Internal GC implementation (dry-run only until mtime support added).
    private func gcWithLock(lock: LockHandle) async throws -> GCResult {
        // 1. Collect live editions
        let liveEditions = try await collectLiveEditions()

        // 2. Scan all objects
        var scanned = 0
        var deleted = 0
        var skippedByRef = 0
        var skippedByScan = 0
        var skippedByAge = 0
        var errors = 0

        let renewInterval = 20
        var processedCount = 0

        // List all shards
        let shards: [String]
        do {
            shards = try await storage.list(prefix: objectsPrefix, delimiter: "/")
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        for shard in shards {
            guard shard.hasSuffix("/") else { continue }

            // List objects in shard
            let objects: [String]
            do {
                objects = try await storage.list(prefix: shard, delimiter: nil)
            } catch let error as StorageError {
                throw ContentError.storageError(underlying: error)
            }

            for objectPath in objects {
                guard objectPath.hasSuffix(".dat") else { continue }

                scanned += 1
                processedCount += 1

                if processedCount % renewInterval == 0 {
                    do {
                        try await lock.renew(duration: 60)
                    } catch {
                        throw ContentError.lockExpired
                    }
                }

                // Extract hash from path
                let filename = objectPath.split(separator: "/").last!
                let hash = String(filename.dropLast(4))  // Remove .dat

                // Check .ref file first (fast path)
                let refPath = shard + hash + ".ref"
                var isLive = false

                do {
                    let refData = try await storage.read(path: refPath)
                    if let content = String(data: refData, encoding: .utf8) {
                        for line in content.split(separator: "\n") {
                            if let editionId = Int(line.trimmingCharacters(in: .whitespaces)) {
                                if liveEditions.contains(editionId) {
                                    isLive = true
                                    skippedByRef += 1
                                    break
                                }
                            }
                        }
                    }
                } catch StorageError.notFound {
                    // No .ref file, will need fallback scan
                } catch {
                    // Corrupt ref, proceed to fallback scan
                }

                // Fallback scan if not found in ref
                if !isLive {
                    isLive = try await objectExistsInLiveEditions(hash: hash, liveEditions: liveEditions)
                    if isLive {
                        skippedByScan += 1
                    }
                }

                // If not live, check age and delete
                if !isLive {
                    // Check file age (use mtime)
                    // Note: This is a simplification - in production, we'd need storage-specific
                    // mtime retrieval. For now, we assume objects without refs are old enough.
                    // TODO: Add mtime support to StorageBackend

                    // For safety, we'll skip age check in this implementation
                    // and rely on the grace period being applied by the caller
                    skippedByAge += 1  // Placeholder - needs mtime support

                    // Delete object and ref
                    // do {
                    //     try await storage.delete(path: objectPath)
                    //     try? await storage.delete(path: refPath)
                    //     deleted += 1
                    // } catch {
                    //     errors += 1
                    // }
                }
            }
        }

        return GCResult(
            scannedObjects: scanned,
            deletedObjects: deleted,
            skippedByRef: skippedByRef,
            skippedByScan: skippedByScan,
            skippedByAge: skippedByAge,
            errors: errors
        )
    }

    /// Collect all live edition IDs (production, staging, pending, working + ancestry).
    private func collectLiveEditions() async throws -> Set<Int> {
        var liveEditions = Set<Int>()

        // Production edition + ancestry
        do {
            let productionId = try await Self.readEditionPointer(storage: storage, file: "\(contentsPrefix).production.json")
            try await collectAncestry(from: productionId, into: &liveEditions)
        } catch {
            // Production may not exist yet
        }

        // Staging edition + ancestry
        do {
            let stagingId = try await Self.readEditionPointer(storage: storage, file: "\(contentsPrefix).staging.json")
            try await collectAncestry(from: stagingId, into: &liveEditions)
        } catch {
            // Staging may not exist yet
        }

        // Pending editions + ancestry
        let pendingPrefix = "\(contentsPrefix).pending/"
        do {
            let pendingFiles = try await storage.list(prefix: pendingPrefix, delimiter: nil)
            for file in pendingFiles {
                guard file.hasSuffix(".json") else { continue }
                do {
                    let data = try await storage.read(path: file)
                    let decoder = JSONDecoder()
                    decoder.dateDecodingStrategy = .iso8601
                    let submission = try decoder.decode(PendingSubmission.self, from: data)
                    try await collectAncestry(from: submission.edition, into: &liveEditions)
                } catch {
                    // Skip corrupt pending files
                }
            }
        } catch {
            // No pending directory
        }

        // Working editions + ancestry
        do {
            let rootFiles = try await storage.list(prefix: contentsPrefix, delimiter: "/")
            for file in rootFiles {
                // Match .{label}.json but not .production.json, .staging.json, etc.
                guard file.hasSuffix(".json"),
                      !file.hasSuffix(".production.json"),
                      !file.hasSuffix(".staging.json") else { continue }

                let filename = file.split(separator: "/").last!
                guard filename.hasPrefix(".") else { continue }

                do {
                    let data = try await storage.read(path: file)
                    let state = try JSONDecoder().decode(SessionState.self, from: data)
                    try await collectAncestry(from: state.edition, into: &liveEditions)
                } catch {
                    // Skip corrupt working files
                }
            }
        } catch {
            // No working files
        }

        return liveEditions
    }

    /// Collect edition and all its ancestors into the set.
    private func collectAncestry(from edition: Int, into editions: inout Set<Int>) async throws {
        var current: Int? = edition

        while let editionId = current {
            if editions.contains(editionId) {
                break  // Already collected this branch
            }
            editions.insert(editionId)

            // Check for .flattened marker
            let flattenedPath = "\(editionsPrefix)\(editionId)/.flattened"
            do {
                if try await storage.exists(path: flattenedPath) {
                    break  // Stop at flattened edition
                }
            } catch {
                // Ignore errors, continue traversal
            }

            current = try await getParentEdition(editionId)
        }
    }

    /// Check if an object hash exists in any live edition.
    private func objectExistsInLiveEditions(hash: String, liveEditions: Set<Int>) async throws -> Bool {
        for editionId in liveEditions {
            let editionPrefix = "\(editionsPrefix)\(editionId)/"

            // List all path files in edition
            let keys: [String]
            do {
                keys = try await storage.list(prefix: editionPrefix, delimiter: nil)
            } catch {
                continue
            }

            for key in keys {
                if key.hasSuffix("/") { continue }
                let filename = key.replacingOccurrences(of: editionPrefix, with: "")
                if filename.hasPrefix(".") { continue }

                do {
                    let content = try await storage.read(path: key)
                    guard let text = String(data: content, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines) else {
                        continue
                    }

                    if text == "sha256:\(hash)" {
                        return true
                    }
                } catch {
                    continue
                }
            }
        }

        return false
    }

    // MARK: - Private Helpers

    /// Write object to content-addressable storage.
    private func writeObject(hash: String, data: Data) async throws {
        let shard = String(hash.prefix(2))
        let objectPath = "\(objectsPrefix)\(shard)/\(hash).dat"

        // Check if object already exists (deduplication)
        do {
            if try await storage.exists(path: objectPath) {
                return  // Already stored
            }
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }

        // Write new object
        do {
            try await storage.write(path: objectPath, data: data)
        } catch let error as StorageError {
            throw ContentError.storageError(underlying: error)
        }
    }

    /// Compute SHA256 hash of data.
    private func computeSHA256(_ data: Data) -> String {
        let hash = SHA256.hash(data: data)
        return hash.compactMap { String(format: "%02x", $0) }.joined()
    }
}

/// Helper for tracking entry info during list merge.
private struct EntryInfo {
    let edition: Int
    let isDeleted: Bool
    let hash: String?  // nil for directories or deleted files
}

/// Buffered change in a transaction.
private struct BufferedChange {
    let action: ChangeAction
    let data: Data?  // Only present for writes with new data
}
