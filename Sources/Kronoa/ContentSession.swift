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
        let entries = try await listInternal(directory: directory)
        return entries.sorted()
    }

    /// Internal list implementation (transaction-aware).
    private func listInternal(directory: String) async throws -> Set<String> {
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

        // Merge pending entries (they take precedence)
        for (name, isDeleted) in pendingEntries {
            allEntries[name] = EntryInfo(edition: _editionId, isDeleted: isDeleted)
        }

        // Filter out deleted entries
        return Set(allEntries.filter { !$0.value.isDeleted }.keys)
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
}

/// Buffered change in a transaction.
private struct BufferedChange {
    let action: ChangeAction
    let data: Data?  // Only present for writes with new data
}
