import Foundation
import Testing
@testable import Kronoa

/// Tests for ContentSession.
@Suite("ContentSession Tests")
struct ContentSessionTests {
    let tempDir: URL

    init() throws {
        tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kronoa-session-tests-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
    }

    /// Create a storage with initial setup (genesis edition).
    func setupStorage() async throws -> LocalFileStorage {
        let storage = try LocalFileStorage(root: tempDir)

        // Create genesis edition 10000
        try await storage.write(
            path: "contents/editions/.head",
            data: "10000".data(using: .utf8)!
        )
        try await storage.write(
            path: "contents/.production.json",
            data: "{\"edition\":10000}".data(using: .utf8)!
        )
        try await storage.write(
            path: "contents/.staging.json",
            data: "{\"edition\":10000}".data(using: .utf8)!
        )

        return storage
    }

    /// Clean up temp directory.
    func cleanup() {
        try? FileManager.default.removeItem(at: tempDir)
    }

    // MARK: - Initialization Tests

    @Test("Initialize session in production mode")
    func initProductionMode() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .production)

        #expect(await session.mode == .production)
        #expect(await session.editionId == 10000)
        #expect(await session.baseEditionId == nil)
        #expect(await session.checkoutSource == nil)
    }

    @Test("Initialize session in staging mode")
    func initStagingMode() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .staging)

        #expect(await session.mode == .staging)
        #expect(await session.editionId == 10000)
    }

    // MARK: - Checkout Tests

    @Test("Checkout creates new edition")
    func checkoutCreatesEdition() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "draft-1")

        #expect(await session.mode == .editing(label: "draft-1"))
        #expect(await session.editionId == 10001)
        #expect(await session.baseEditionId == 10000)
        #expect(await session.checkoutSource == .staging)

        // Verify .origin file was created
        let originData = try await storage.read(path: "contents/editions/10001/.origin")
        let origin = String(data: originData, encoding: .utf8)
        #expect(origin == "10000")

        // Verify working file was created
        let workingData = try await storage.read(path: "contents/.draft-1.json")
        let state = try JSONDecoder().decode(SessionState.self, from: workingData)
        #expect(state.edition == 10001)
        #expect(state.base == 10000)
        #expect(state.source == .staging)
    }

    @Test("Checkout from production")
    func checkoutFromProduction() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .production)
        try await session.checkout(label: "hotfix-1", from: .production)

        #expect(await session.mode == .editing(label: "hotfix-1"))
        #expect(await session.baseEditionId == 10000)
        #expect(await session.checkoutSource == .production)
    }

    @Test("Checkout rejects duplicate label")
    func checkoutRejectsDuplicateLabel() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create a working file for "draft-1"
        try await storage.write(
            path: "contents/.draft-1.json",
            data: "{\"edition\":10001,\"base\":10000,\"source\":\"staging\"}".data(using: .utf8)!
        )

        let session = try await ContentSession(storage: storage, mode: .staging)

        await #expect(throws: ContentError.labelInUse("draft-1")) {
            try await session.checkout(label: "draft-1")
        }
    }

    @Test("Checkout rejects invalid label")
    func checkoutRejectsInvalidLabel() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .staging)

        await #expect(throws: ContentError.invalidPath(".hidden")) {
            try await session.checkout(label: ".hidden")
        }

        await #expect(throws: ContentError.invalidPath("foo/bar")) {
            try await session.checkout(label: "foo/bar")
        }
    }

    // MARK: - Read Tests

    @Test("Read file from current edition")
    func readFromCurrentEdition() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Write an object
        let content = "Hello, World!"
        let hash = "315f5bdb76d078c43b8ac0064e4a0164612b1fce77c869345bfc94c75894edd3"
        try await storage.write(
            path: "contents/objects/31/\(hash).dat",
            data: content.data(using: .utf8)!
        )
        // Write path file in edition 10000
        try await storage.write(
            path: "contents/editions/10000/hello.txt",
            data: "sha256:\(hash)".data(using: .utf8)!
        )

        let session = try await ContentSession(storage: storage, mode: .production)
        let data = try await session.read(path: "hello.txt")

        #expect(String(data: data, encoding: .utf8) == content)
    }

    @Test("Read file from ancestor edition")
    func readFromAncestor() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Write an object in edition 10000
        let content = "From parent"
        let hash = "abc123def456"
        try await storage.write(
            path: "contents/objects/ab/\(hash).dat",
            data: content.data(using: .utf8)!
        )
        try await storage.write(
            path: "contents/editions/10000/inherited.txt",
            data: "sha256:\(hash)".data(using: .utf8)!
        )

        // Create edition 10001 with origin pointing to 10000
        try await storage.write(
            path: "contents/editions/10001/.origin",
            data: "10000".data(using: .utf8)!
        )
        try await storage.write(
            path: "contents/.staging.json",
            data: "{\"edition\":10001}".data(using: .utf8)!
        )

        let session = try await ContentSession(storage: storage, mode: .staging)
        let data = try await session.read(path: "inherited.txt")

        #expect(String(data: data, encoding: .utf8) == content)
    }

    @Test("Read returns notFound for missing file")
    func readNotFound() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .production)

        await #expect(throws: ContentError.notFound(path: "missing.txt")) {
            _ = try await session.read(path: "missing.txt")
        }
    }

    @Test("Read returns notFound for tombstoned file")
    func readTombstone() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Write tombstone in edition 10000
        try await storage.write(
            path: "contents/editions/10000/deleted.txt",
            data: "deleted".data(using: .utf8)!
        )

        let session = try await ContentSession(storage: storage, mode: .production)

        await #expect(throws: ContentError.notFound(path: "deleted.txt")) {
            _ = try await session.read(path: "deleted.txt")
        }
    }

    @Test("Read rejects invalid path")
    func readRejectsInvalidPath() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .production)

        await #expect(throws: ContentError.invalidPath("../escape")) {
            _ = try await session.read(path: "../escape")
        }

        await #expect(throws: ContentError.invalidPath(".hidden")) {
            _ = try await session.read(path: ".hidden")
        }
    }

    // MARK: - Exists Tests

    @Test("Exists returns true for existing file")
    func existsTrue() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let hash = "abc123"
        try await storage.write(
            path: "contents/objects/ab/\(hash).dat",
            data: "content".data(using: .utf8)!
        )
        try await storage.write(
            path: "contents/editions/10000/file.txt",
            data: "sha256:\(hash)".data(using: .utf8)!
        )

        let session = try await ContentSession(storage: storage, mode: .production)
        let exists = try await session.exists(path: "file.txt")

        #expect(exists == true)
    }

    @Test("Exists returns false for missing file")
    func existsFalse() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .production)
        let exists = try await session.exists(path: "missing.txt")

        #expect(exists == false)
    }

    @Test("Exists returns false for tombstoned file")
    func existsTombstone() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        try await storage.write(
            path: "contents/editions/10000/deleted.txt",
            data: "deleted".data(using: .utf8)!
        )

        let session = try await ContentSession(storage: storage, mode: .production)
        let exists = try await session.exists(path: "deleted.txt")

        #expect(exists == false)
    }

    // MARK: - Stat Tests

    @Test("Stat returns exists for file")
    func statExists() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let content = "Test content"
        let hash = "abc123"
        try await storage.write(
            path: "contents/objects/ab/\(hash).dat",
            data: content.data(using: .utf8)!
        )
        try await storage.write(
            path: "contents/editions/10000/file.txt",
            data: "sha256:\(hash)".data(using: .utf8)!
        )

        let session = try await ContentSession(storage: storage, mode: .production)
        let stat = try await session.stat(path: "file.txt")

        #expect(stat.status == .exists)
        #expect(stat.hash == hash)
        #expect(stat.size == content.count)
        #expect(stat.resolvedFrom == 10000)
    }

    @Test("Stat returns deleted for tombstone")
    func statDeleted() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        try await storage.write(
            path: "contents/editions/10000/deleted.txt",
            data: "deleted".data(using: .utf8)!
        )

        let session = try await ContentSession(storage: storage, mode: .production)
        let stat = try await session.stat(path: "deleted.txt")

        #expect(stat.status == .deleted)
        #expect(stat.hash == nil)
        #expect(stat.resolvedFrom == 10000)
    }

    @Test("Stat returns notFound for missing")
    func statNotFound() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .production)
        let stat = try await session.stat(path: "missing.txt")

        #expect(stat.status == .notFound)
    }

    // MARK: - List Tests

    @Test("List returns files in directory")
    func listDirectory() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create files in articles/
        try await storage.write(
            path: "contents/editions/10000/articles/post1.md",
            data: "sha256:abc".data(using: .utf8)!
        )
        try await storage.write(
            path: "contents/editions/10000/articles/post2.md",
            data: "sha256:def".data(using: .utf8)!
        )

        let session = try await ContentSession(storage: storage, mode: .production)

        let entries = try await session.list(directory: "articles/")

        #expect(entries == ["post1.md", "post2.md"])
    }

    @Test("List excludes tombstoned files")
    func listExcludesTombstones() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        try await storage.write(
            path: "contents/editions/10000/articles/visible.md",
            data: "sha256:abc".data(using: .utf8)!
        )
        try await storage.write(
            path: "contents/editions/10000/articles/deleted.md",
            data: "deleted".data(using: .utf8)!
        )

        let session = try await ContentSession(storage: storage, mode: .production)

        let entries = try await session.list(directory: "articles/")

        #expect(entries == ["visible.md"])
    }

    @Test("List merges from ancestry")
    func listMergesAncestry() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Parent edition has file1
        try await storage.write(
            path: "contents/editions/10000/articles/from-parent.md",
            data: "sha256:abc".data(using: .utf8)!
        )

        // Child edition has file2
        try await storage.write(
            path: "contents/editions/10001/.origin",
            data: "10000".data(using: .utf8)!
        )
        try await storage.write(
            path: "contents/editions/10001/articles/from-child.md",
            data: "sha256:def".data(using: .utf8)!
        )
        try await storage.write(
            path: "contents/.staging.json",
            data: "{\"edition\":10001}".data(using: .utf8)!
        )

        let session = try await ContentSession(storage: storage, mode: .staging)

        let entries = try await session.list(directory: "articles/")

        #expect(entries == ["from-child.md", "from-parent.md"])
    }

    @Test("List returns empty for non-existent directory")
    func listEmptyDirectory() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .production)

        let entries = try await session.list(directory: "nonexistent/")

        #expect(entries.isEmpty)
    }

    @Test("List root directory")
    func listRoot() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        try await storage.write(
            path: "contents/editions/10000/readme.md",
            data: "sha256:abc".data(using: .utf8)!
        )

        let session = try await ContentSession(storage: storage, mode: .production)

        let entries = try await session.list(directory: "")

        #expect(entries.contains("readme.md"))
    }

    // MARK: - Integrity Tests

    @Test("Stat throws integrityError for missing object")
    func statMissingObject() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Path file points to non-existent object
        try await storage.write(
            path: "contents/editions/10000/orphan.txt",
            data: "sha256:deadbeef123456".data(using: .utf8)!
        )

        let session = try await ContentSession(storage: storage, mode: .production)

        await #expect(throws: ContentError.integrityError(expected: "deadbeef123456", actual: "missing")) {
            _ = try await session.stat(path: "orphan.txt")
        }
    }

    @Test("Exists returns false for missing object")
    func existsMissingObject() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Path file points to non-existent object
        try await storage.write(
            path: "contents/editions/10000/orphan.txt",
            data: "sha256:deadbeef123456".data(using: .utf8)!
        )

        let session = try await ContentSession(storage: storage, mode: .production)

        // exists() should throw integrityError (not silently return true)
        await #expect(throws: ContentError.integrityError(expected: "deadbeef123456", actual: "missing")) {
            _ = try await session.exists(path: "orphan.txt")
        }
    }

    @Test("Read throws integrityError for missing object")
    func readMissingObject() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Path file points to non-existent object
        try await storage.write(
            path: "contents/editions/10000/orphan.txt",
            data: "sha256:deadbeef123456".data(using: .utf8)!
        )

        let session = try await ContentSession(storage: storage, mode: .production)

        await #expect(throws: ContentError.integrityError(expected: "deadbeef123456", actual: "missing")) {
            _ = try await session.read(path: "orphan.txt")
        }
    }

    @Test("Ancestry throws integrityError for malformed .origin")
    func malformedOrigin() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create edition 10001 with malformed .origin
        try await storage.write(
            path: "contents/editions/10001/.origin",
            data: "not-a-number".data(using: .utf8)!
        )
        try await storage.write(
            path: "contents/.staging.json",
            data: "{\"edition\":10001}".data(using: .utf8)!
        )

        let session = try await ContentSession(storage: storage, mode: .staging)

        // Reading a file that doesn't exist in 10001 triggers ancestry traversal
        await #expect(throws: ContentError.integrityError(expected: "integer edition ID in .origin", actual: "not-a-number")) {
            _ = try await session.read(path: "missing.txt")
        }
    }

    // MARK: - Mode Precondition Tests

    @Test("Checkout rejects when already editing")
    func checkoutRejectsFromEditing() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "first")

        // Already in editing mode
        await #expect(throws: ContentError.notInEditingMode) {
            try await session.checkout(label: "second")
        }
    }

    // MARK: - writeIfAbsent Tests

    @Test("writeIfAbsent returns true for new file")
    func writeIfAbsentNew() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let created = try await storage.writeIfAbsent(
            path: "test/new-file.txt",
            data: "content".data(using: .utf8)!
        )

        #expect(created == true)

        // Verify file exists
        let exists = try await storage.exists(path: "test/new-file.txt")
        #expect(exists == true)
    }

    @Test("writeIfAbsent returns false for existing file")
    func writeIfAbsentExisting() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create file first
        try await storage.write(
            path: "test/existing.txt",
            data: "original".data(using: .utf8)!
        )

        // Try to create again
        let created = try await storage.writeIfAbsent(
            path: "test/existing.txt",
            data: "new content".data(using: .utf8)!
        )

        #expect(created == false)

        // Verify original content preserved
        let data = try await storage.read(path: "test/existing.txt")
        #expect(String(data: data, encoding: .utf8) == "original")
    }

    // MARK: - Write Tests

    @Test("Write creates object and path file")
    func writeCreatesFiles() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "writer")

        let content = "Hello, World!"
        try await session.write(path: "greeting.txt", data: content.data(using: .utf8)!)

        // Verify we can read it back
        let readData = try await session.read(path: "greeting.txt")
        #expect(String(data: readData, encoding: .utf8) == content)

        // Verify object exists in storage
        let stat = try await session.stat(path: "greeting.txt")
        #expect(stat.status == .exists)
        #expect(stat.hash != nil)
    }

    @Test("Write rejects in read-only mode")
    func writeRejectsReadOnly() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .production)

        await #expect(throws: ContentError.readOnlyMode) {
            try await session.write(path: "file.txt", data: "data".data(using: .utf8)!)
        }
    }

    @Test("Write deduplicates identical content")
    func writeDeduplicates() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "dedup")

        let content = "Same content"
        try await session.write(path: "file1.txt", data: content.data(using: .utf8)!)
        try await session.write(path: "file2.txt", data: content.data(using: .utf8)!)

        // Both files should have same hash
        let stat1 = try await session.stat(path: "file1.txt")
        let stat2 = try await session.stat(path: "file2.txt")
        #expect(stat1.hash == stat2.hash)
    }

    // MARK: - Delete Tests

    @Test("Delete creates tombstone")
    func deleteTombstone() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create a file in the base edition
        let hash = "abc123"
        try await storage.write(
            path: "contents/objects/ab/\(hash).dat",
            data: "original".data(using: .utf8)!
        )
        try await storage.write(
            path: "contents/editions/10000/file.txt",
            data: "sha256:\(hash)".data(using: .utf8)!
        )

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "deleter")

        // Verify file exists before delete
        #expect(try await session.exists(path: "file.txt") == true)

        // Delete it
        try await session.delete(path: "file.txt")

        // Verify file no longer exists
        #expect(try await session.exists(path: "file.txt") == false)

        // Verify tombstone was created
        let stat = try await session.stat(path: "file.txt")
        #expect(stat.status == .deleted)
    }

    @Test("Delete rejects in read-only mode")
    func deleteRejectsReadOnly() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .production)

        await #expect(throws: ContentError.readOnlyMode) {
            try await session.delete(path: "file.txt")
        }
    }

    // MARK: - Copy Tests

    @Test("Copy creates reference to same hash")
    func copyCreatesReference() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "copier")

        // Write original file
        let content = "Original content"
        try await session.write(path: "original.txt", data: content.data(using: .utf8)!)

        // Copy it
        try await session.copy(from: "original.txt", to: "copy.txt")

        // Both should have same content and hash
        let original = try await session.read(path: "original.txt")
        let copy = try await session.read(path: "copy.txt")
        #expect(original == copy)

        let stat1 = try await session.stat(path: "original.txt")
        let stat2 = try await session.stat(path: "copy.txt")
        #expect(stat1.hash == stat2.hash)
    }

    @Test("Copy from non-existent file throws notFound")
    func copyNotFound() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "copier")

        await #expect(throws: ContentError.notFound(path: "missing.txt")) {
            try await session.copy(from: "missing.txt", to: "dest.txt")
        }
    }

    // MARK: - Discard Tests

    @Test("Discard removes local change")
    func discardRemovesChange() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create a file in the base edition
        let hash = "abc123"
        try await storage.write(
            path: "contents/objects/ab/\(hash).dat",
            data: "original".data(using: .utf8)!
        )
        try await storage.write(
            path: "contents/editions/10000/file.txt",
            data: "sha256:\(hash)".data(using: .utf8)!
        )

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "discarder")

        // Modify the file
        try await session.write(path: "file.txt", data: "modified".data(using: .utf8)!)

        // Verify modification
        let modified = try await session.read(path: "file.txt")
        #expect(String(data: modified, encoding: .utf8) == "modified")

        // Discard change
        try await session.discard(path: "file.txt")

        // Should see original content through ancestry
        let restored = try await session.read(path: "file.txt")
        #expect(String(data: restored, encoding: .utf8) == "original")
    }

    // MARK: - Transaction Tests

    @Test("beginEditing starts transaction")
    func beginEditingStartsTransaction() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "transactor")

        #expect(await session.isInTransaction == false)
        try await session.beginEditing()
        #expect(await session.isInTransaction == true)
    }

    @Test("beginEditing rejects in read-only mode")
    func beginEditingRejectsReadOnly() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .production)

        await #expect(throws: ContentError.readOnlyMode) {
            try await session.beginEditing()
        }
    }

    @Test("beginEditing rejects if already in transaction")
    func beginEditingRejectsNested() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "transactor")

        try await session.beginEditing()

        await #expect(throws: ContentError.alreadyInTransaction) {
            try await session.beginEditing()
        }
    }

    @Test("Transaction buffers changes until endEditing")
    func transactionBuffersChanges() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "transactor")
        let editionId = await session.editionId

        try await session.beginEditing()

        // Write in transaction
        try await session.write(path: "a.txt", data: "A".data(using: .utf8)!)
        try await session.write(path: "b.txt", data: "B".data(using: .utf8)!)

        // Check pending changes
        let pending = await session.pendingChanges
        #expect(pending.count == 2)

        // Path files should NOT exist yet
        let pathFileA = "contents/editions/\(editionId)/a.txt"
        let existsA = try await storage.exists(path: pathFileA)
        #expect(existsA == false)

        // End transaction
        try await session.endEditing()

        // Now path files should exist
        let existsAfter = try await storage.exists(path: pathFileA)
        #expect(existsAfter == true)

        #expect(await session.isInTransaction == false)
        #expect(await session.pendingChanges.isEmpty)
    }

    @Test("rollback discards buffered changes")
    func rollbackDiscardsChanges() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "transactor")

        try await session.beginEditing()
        try await session.write(path: "temp.txt", data: "temporary".data(using: .utf8)!)

        #expect(await session.pendingChanges.count == 1)

        try await session.rollback()

        #expect(await session.isInTransaction == false)
        #expect(await session.pendingChanges.isEmpty)

        // File should not exist
        #expect(try await session.exists(path: "temp.txt") == false)
    }

    @Test("rollback throws if not in transaction")
    func rollbackThrowsIfNotInTransaction() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "transactor")

        await #expect(throws: ContentError.notInTransaction) {
            try await session.rollback()
        }
    }

    @Test("endEditing throws if not in transaction")
    func endEditingThrowsIfNotInTransaction() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "transactor")

        await #expect(throws: ContentError.notInTransaction) {
            try await session.endEditing()
        }
    }

    // MARK: - Transaction Visibility Tests

    @Test("Read sees buffered write in transaction")
    func readSeesBufferedWrite() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "reader")

        try await session.beginEditing()

        // Write in transaction
        let content = "Buffered content"
        try await session.write(path: "new.txt", data: content.data(using: .utf8)!)

        // Read should see the buffered content (not notFound)
        let readData = try await session.read(path: "new.txt")
        #expect(String(data: readData, encoding: .utf8) == content)

        try await session.rollback()
    }

    @Test("Exists sees buffered write in transaction")
    func existsSeesBufferedWrite() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "checker")

        try await session.beginEditing()

        // Write in transaction
        try await session.write(path: "new.txt", data: "content".data(using: .utf8)!)

        // Exists should return true
        #expect(try await session.exists(path: "new.txt") == true)

        try await session.rollback()
    }

    @Test("Stat sees buffered write in transaction")
    func statSeesBufferedWrite() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "statter")

        try await session.beginEditing()

        let content = "content data"
        try await session.write(path: "new.txt", data: content.data(using: .utf8)!)

        let stat = try await session.stat(path: "new.txt")
        #expect(stat.status == .exists)
        #expect(stat.size == content.count)

        try await session.rollback()
    }

    @Test("Read sees buffered delete in transaction")
    func readSeesBufferedDelete() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create a file in base edition
        let hash = "abc123"
        try await storage.write(
            path: "contents/objects/ab/\(hash).dat",
            data: "original".data(using: .utf8)!
        )
        try await storage.write(
            path: "contents/editions/10000/existing.txt",
            data: "sha256:\(hash)".data(using: .utf8)!
        )

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "deleter")

        // File exists before transaction
        #expect(try await session.exists(path: "existing.txt") == true)

        try await session.beginEditing()

        // Delete in transaction
        try await session.delete(path: "existing.txt")

        // Read should throw notFound
        await #expect(throws: ContentError.notFound(path: "existing.txt")) {
            _ = try await session.read(path: "existing.txt")
        }

        // Exists should return false
        #expect(try await session.exists(path: "existing.txt") == false)

        try await session.rollback()
    }

    @Test("Copy sees buffered write in transaction")
    func copySeesBufferedWrite() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "copier")

        try await session.beginEditing()

        // Write in transaction
        let content = "source content"
        try await session.write(path: "source.txt", data: content.data(using: .utf8)!)

        // Copy the buffered file
        try await session.copy(from: "source.txt", to: "dest.txt")

        // Both should be readable
        let sourceData = try await session.read(path: "source.txt")
        let destData = try await session.read(path: "dest.txt")
        #expect(sourceData == destData)

        try await session.rollback()
    }

    @Test("Copy fails for buffered delete in transaction")
    func copyFailsForBufferedDelete() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create a file in base edition
        let hash = "abc123"
        try await storage.write(
            path: "contents/objects/ab/\(hash).dat",
            data: "original".data(using: .utf8)!
        )
        try await storage.write(
            path: "contents/editions/10000/existing.txt",
            data: "sha256:\(hash)".data(using: .utf8)!
        )

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "copier")

        try await session.beginEditing()

        // Delete in transaction
        try await session.delete(path: "existing.txt")

        // Copy should fail with notFound
        await #expect(throws: ContentError.notFound(path: "existing.txt")) {
            try await session.copy(from: "existing.txt", to: "dest.txt")
        }

        try await session.rollback()
    }

    @Test("List sees buffered writes in transaction")
    func listSeesBufferedWrites() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "lister")

        try await session.beginEditing()

        // Write files in transaction
        try await session.write(path: "a.txt", data: "A".data(using: .utf8)!)
        try await session.write(path: "b.txt", data: "B".data(using: .utf8)!)

        // List should see them
        let entries = try await session.list(directory: "")
        #expect(entries.contains("a.txt"))
        #expect(entries.contains("b.txt"))

        try await session.rollback()
    }

    @Test("List hides buffered deletes in transaction")
    func listHidesBufferedDeletes() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create a file in base edition
        let hash = "abc123"
        try await storage.write(
            path: "contents/objects/ab/\(hash).dat",
            data: "original".data(using: .utf8)!
        )
        try await storage.write(
            path: "contents/editions/10000/existing.txt",
            data: "sha256:\(hash)".data(using: .utf8)!
        )

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "lister")

        // File visible before transaction
        let beforeList = try await session.list(directory: "")
        #expect(beforeList.contains("existing.txt"))

        try await session.beginEditing()

        // Delete in transaction
        try await session.delete(path: "existing.txt")

        // List should not show deleted file
        let afterList = try await session.list(directory: "")
        #expect(!afterList.contains("existing.txt"))

        try await session.rollback()
    }

    @Test("Read and stat return consistent results when buffered write shadows ancestor")
    func readStatConsistentWithShadowing() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create a file with DIFFERENT content in the ancestor edition
        let ancestorHash = "abc123abc123abc123abc123abc123abc123abc123abc123abc123abc123abc1"
        try await storage.write(
            path: "contents/objects/ab/\(ancestorHash).dat",
            data: "ancestor content".data(using: .utf8)!
        )
        try await storage.write(
            path: "contents/editions/10000/shadowed.txt",
            data: "sha256:\(ancestorHash)".data(using: .utf8)!
        )

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "shadower")

        // Verify ancestor content is accessible
        let ancestorRead = try await session.read(path: "shadowed.txt")
        #expect(String(data: ancestorRead, encoding: .utf8) == "ancestor content")

        try await session.beginEditing()

        // Write DIFFERENT content in the transaction (shadows ancestor)
        let newContent = "new buffered content"
        try await session.write(path: "shadowed.txt", data: newContent.data(using: .utf8)!)

        // Both stat and read should see the buffered version, NOT the ancestor
        let stat = try await session.stat(path: "shadowed.txt")
        let readData = try await session.read(path: "shadowed.txt")

        #expect(stat.status == .exists)
        #expect(stat.size == newContent.count)  // Size of buffered content
        #expect(String(data: readData, encoding: .utf8) == newContent)  // Buffered content, not ancestor

        // Verify hash from stat matches the new content, not the ancestor
        #expect(stat.hash != ancestorHash)

        try await session.rollback()
    }

    @Test("List does not show subdirectory when all pending changes are deletes")
    func listHidesSubdirWithOnlyDeletes() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create files in a subdirectory in the ancestor
        let hash1 = "abc123abc123abc123abc123abc123abc123abc123abc123abc123abc123abc1"
        let hash2 = "def456def456def456def456def456def456def456def456def456def456def4"
        try await storage.write(
            path: "contents/objects/ab/\(hash1).dat",
            data: "file1".data(using: .utf8)!
        )
        try await storage.write(
            path: "contents/objects/de/\(hash2).dat",
            data: "file2".data(using: .utf8)!
        )
        try await storage.write(
            path: "contents/editions/10000/subdir/file1.txt",
            data: "sha256:\(hash1)".data(using: .utf8)!
        )
        try await storage.write(
            path: "contents/editions/10000/subdir/file2.txt",
            data: "sha256:\(hash2)".data(using: .utf8)!
        )

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "deleter")

        // Subdir should be visible initially
        let beforeList = try await session.list(directory: "")
        #expect(beforeList.contains("subdir/"))

        try await session.beginEditing()

        // Delete both files in the subdirectory
        try await session.delete(path: "subdir/file1.txt")
        try await session.delete(path: "subdir/file2.txt")

        // The subdir should NOT appear in list when all pending changes under it are deletes
        // Semantics: transaction reads see state as-if changes were committed
        let afterList = try await session.list(directory: "")
        #expect(!afterList.contains("subdir/"))

        try await session.rollback()
    }

    // MARK: - Publishing Workflow Tests

    @Test("Submit creates pending file and transitions to submitted mode")
    func submitCreatesPending() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "feature-1")
        let editionId = await session.editionId

        // Write some content
        try await session.write(path: "test.txt", data: "content".data(using: .utf8)!)

        // Submit
        try await session.submit(message: "Test submission")

        // Mode should be submitted
        #expect(await session.mode == .submitted)

        // Pending file should exist
        let pendingData = try await storage.read(path: "contents/.pending/\(editionId).json")
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let submission = try decoder.decode(PendingSubmission.self, from: pendingData)

        #expect(submission.edition == editionId)
        #expect(submission.base == 10000)
        #expect(submission.source == .staging)
        #expect(submission.label == "feature-1")
        #expect(submission.message == "Test submission")

        // Working file should be removed
        let workingExists = try await storage.exists(path: "contents/.feature-1.json")
        #expect(workingExists == false)
    }

    @Test("Submit rejects if not in editing mode")
    func submitRejectsReadOnly() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .production)

        await #expect(throws: ContentError.notInEditingMode) {
            try await session.submit(message: "Should fail")
        }
    }

    @Test("Submit auto-commits pending transaction")
    func submitAutoCommits() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "auto-commit")
        let editionId = await session.editionId

        // Start transaction but don't end it
        try await session.beginEditing()
        try await session.write(path: "buffered.txt", data: "data".data(using: .utf8)!)

        #expect(await session.isInTransaction == true)

        // Submit should auto-commit
        try await session.submit(message: "Auto-commit test")

        #expect(await session.isInTransaction == false)

        // File should be written to storage
        let pathFile = "contents/editions/\(editionId)/buffered.txt"
        let exists = try await storage.exists(path: pathFile)
        #expect(exists == true)
    }

    @Test("listPending returns pending submissions")
    func listPendingReturns() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create two submissions
        let session1 = try await ContentSession(storage: storage, mode: .staging)
        try await session1.checkout(label: "feature-1")
        try await session1.write(path: "a.txt", data: "A".data(using: .utf8)!)
        try await session1.submit(message: "First")

        let session2 = try await ContentSession(storage: storage, mode: .staging)
        try await session2.checkout(label: "feature-2")
        try await session2.write(path: "b.txt", data: "B".data(using: .utf8)!)
        try await session2.submit(message: "Second")

        // List pending
        let reader = try await ContentSession(storage: storage, mode: .staging)
        let pending = try await reader.listPending()

        #expect(pending.count == 2)
        #expect(pending[0].label == "feature-1")
        #expect(pending[1].label == "feature-2")
    }

    @Test("Stage promotes pending to staging")
    func stagePromotesPending() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create and submit
        let editor = try await ContentSession(storage: storage, mode: .staging)
        try await editor.checkout(label: "to-stage")
        let editionId = await editor.editionId
        try await editor.write(path: "new.txt", data: "new content".data(using: .utf8)!)
        try await editor.submit(message: "Ready to stage")

        // Stage it
        let admin = try await ContentSession(storage: storage, mode: .staging)
        try await admin.stage(edition: editionId)

        // Staging should now point to new edition
        let stagingData = try await storage.read(path: "contents/.staging.json")
        let pointer = try JSONDecoder().decode(EditionPointer.self, from: stagingData)
        #expect(pointer.edition == editionId)

        // Pending should be removed
        let pendingExists = try await storage.exists(path: "contents/.pending/\(editionId).json")
        #expect(pendingExists == false)

        // .ref file should exist for the object
        let newSession = try await ContentSession(storage: storage, mode: .staging)
        let stat = try await newSession.stat(path: "new.txt")
        #expect(stat.status == .exists)

        if let hash = stat.hash {
            let shard = String(hash.prefix(2))
            let refPath = "contents/objects/\(shard)/\(hash).ref"
            let refData = try await storage.read(path: refPath)
            let refContent = String(data: refData, encoding: .utf8)!
            #expect(refContent.contains("\(editionId)"))
        }
    }

    @Test("Stage rejects non-existent pending")
    func stageRejectsNonExistent() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let admin = try await ContentSession(storage: storage, mode: .staging)

        await #expect(throws: ContentError.pendingNotFound(edition: 99999)) {
            try await admin.stage(edition: 99999)
        }
    }

    @Test("Stage detects conflict when base doesn't match")
    func stageDetectsConflict() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create first submission based on 10000
        let editor1 = try await ContentSession(storage: storage, mode: .staging)
        try await editor1.checkout(label: "first")
        let edition1 = await editor1.editionId
        try await editor1.write(path: "a.txt", data: "A".data(using: .utf8)!)
        try await editor1.submit(message: "First")

        // Create second submission also based on 10000
        let editor2 = try await ContentSession(storage: storage, mode: .staging)
        try await editor2.checkout(label: "second")
        let edition2 = await editor2.editionId
        try await editor2.write(path: "b.txt", data: "B".data(using: .utf8)!)
        try await editor2.submit(message: "Second")

        // Stage first one - should succeed
        let admin = try await ContentSession(storage: storage, mode: .staging)
        try await admin.stage(edition: edition1)

        // Stage second one - should fail due to conflict
        await #expect(throws: ContentError.conflictDetected(base: 10000, current: edition1, source: .staging)) {
            try await admin.stage(edition: edition2)
        }
    }

    @Test("Deploy promotes staging to production")
    func deployPromotesToProduction() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create, submit, and stage
        let editor = try await ContentSession(storage: storage, mode: .staging)
        try await editor.checkout(label: "to-deploy")
        let editionId = await editor.editionId
        try await editor.write(path: "prod.txt", data: "production content".data(using: .utf8)!)
        try await editor.submit(message: "Ready to deploy")

        let admin = try await ContentSession(storage: storage, mode: .staging)
        try await admin.stage(edition: editionId)

        // Deploy
        try await admin.deploy()

        // Production should now point to new edition
        let prodData = try await storage.read(path: "contents/.production.json")
        let pointer = try JSONDecoder().decode(EditionPointer.self, from: prodData)
        #expect(pointer.edition == editionId)

        // New session in production mode should see the file
        let prodSession = try await ContentSession(storage: storage, mode: .production)
        #expect(await prodSession.editionId == editionId)
        let exists = try await prodSession.exists(path: "prod.txt")
        #expect(exists == true)
    }

    @Test("Full publishing workflow")
    func fullPublishingWorkflow() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // 1. Editor checkouts and creates content
        let editor = try await ContentSession(storage: storage, mode: .production)
        try await editor.checkout(label: "spring-issue", from: .staging)

        try await editor.beginEditing()
        try await editor.write(path: "articles/cover.md", data: "# Cover Story".data(using: .utf8)!)
        try await editor.write(path: "articles/feature.md", data: "# Feature Article".data(using: .utf8)!)
        try await editor.endEditing()

        // 2. Editor submits
        try await editor.submit(message: "Spring issue content")
        let submittedEdition = await editor.editionId

        // 3. Admin reviews pending
        let admin = try await ContentSession(storage: storage, mode: .staging)
        let pending = try await admin.listPending()
        #expect(pending.count == 1)
        #expect(pending[0].edition == submittedEdition)

        // 4. Admin stages
        try await admin.stage(edition: submittedEdition)

        // Verify staging
        let stagingSession = try await ContentSession(storage: storage, mode: .staging)
        #expect(await stagingSession.editionId == submittedEdition)
        #expect(try await stagingSession.exists(path: "articles/cover.md") == true)

        // 5. Admin deploys
        try await admin.deploy()

        // Verify production
        let prodSession = try await ContentSession(storage: storage, mode: .production)
        #expect(await prodSession.editionId == submittedEdition)
        let coverData = try await prodSession.read(path: "articles/cover.md")
        #expect(String(data: coverData, encoding: .utf8) == "# Cover Story")
    }

    // MARK: - Maintenance Operations Tests

    @Test("Flatten copies ancestor mappings and creates .flattened marker")
    func flattenCopiesAncestorMappings() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create file in genesis edition
        let hash1 = "abc123abc123abc123abc123abc123abc123abc123abc123abc123abc123abc1"
        try await storage.write(
            path: "contents/objects/ab/\(hash1).dat",
            data: "genesis content".data(using: .utf8)!
        )
        try await storage.write(
            path: "contents/editions/10000/ancestor.txt",
            data: "sha256:\(hash1)".data(using: .utf8)!
        )

        // Create child edition with its own file
        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "to-flatten")
        let editionId = await session.editionId
        try await session.write(path: "new.txt", data: "new content".data(using: .utf8)!)

        // Verify ancestor.txt resolves through ancestry
        let beforeFlatten = try await session.read(path: "ancestor.txt")
        #expect(String(data: beforeFlatten, encoding: .utf8) == "genesis content")

        // Flatten
        try await session.flatten(edition: editionId)

        // Verify .flattened marker exists
        let flattenedExists = try await storage.exists(path: "contents/editions/\(editionId)/.flattened")
        #expect(flattenedExists == true)

        // Verify ancestor.txt is now in the flattened edition
        let ancestorPathFile = "contents/editions/\(editionId)/ancestor.txt"
        let ancestorContent = try await storage.read(path: ancestorPathFile)
        #expect(String(data: ancestorContent, encoding: .utf8)?.contains(hash1) == true)
    }

    @Test("Flatten is idempotent")
    func flattenIsIdempotent() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "flatten-twice")
        let editionId = await session.editionId
        try await session.write(path: "test.txt", data: "data".data(using: .utf8)!)

        // Flatten twice - should not throw
        try await session.flatten(edition: editionId)
        try await session.flatten(edition: editionId)

        // Still works
        let flattenedExists = try await storage.exists(path: "contents/editions/\(editionId)/.flattened")
        #expect(flattenedExists == true)
    }

    @Test("Flatten rejects non-existent edition")
    func flattenRejectsNonExistent() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .staging)

        await #expect(throws: ContentError.editionNotFound(edition: 99999)) {
            try await session.flatten(edition: 99999)
        }
    }

    @Test("Flatten preserves tombstones")
    func flattenPreservesTombstones() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create file in genesis
        let hash = "abc123abc123abc123abc123abc123abc123abc123abc123abc123abc123abc1"
        try await storage.write(
            path: "contents/objects/ab/\(hash).dat",
            data: "will be deleted".data(using: .utf8)!
        )
        try await storage.write(
            path: "contents/editions/10000/deleted.txt",
            data: "sha256:\(hash)".data(using: .utf8)!
        )

        // Create child that deletes the file
        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "deleter")
        let editionId = await session.editionId
        try await session.delete(path: "deleted.txt")

        // Flatten
        try await session.flatten(edition: editionId)

        // Verify tombstone is in flattened edition (not just in ancestor)
        let tombstonePath = "contents/editions/\(editionId)/deleted.txt"
        let content = try await storage.read(path: tombstonePath)
        #expect(String(data: content, encoding: .utf8) == "deleted")
    }

    @Test("Reads stop at flattened edition")
    func readsStopAtFlattened() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create a file that will be "hidden" by flattening
        let oldHash = "old111old111old111old111old111old111old111old111old111old111old1"
        try await storage.write(
            path: "contents/objects/ol/\(oldHash).dat",
            data: "old content".data(using: .utf8)!
        )
        try await storage.write(
            path: "contents/editions/10000/old.txt",
            data: "sha256:\(oldHash)".data(using: .utf8)!
        )

        // Create and flatten an edition that doesn't have old.txt
        let session = try await ContentSession(storage: storage, mode: .staging)
        try await session.checkout(label: "flatten-test")
        let editionId = await session.editionId
        try await session.write(path: "new.txt", data: "new".data(using: .utf8)!)

        // Before flattening - old.txt should resolve
        let beforeRead = try await session.read(path: "old.txt")
        #expect(String(data: beforeRead, encoding: .utf8) == "old content")

        // Flatten (this will copy old.txt to the flattened edition)
        try await session.flatten(edition: editionId)

        // After flattening - old.txt should still be readable (it was copied)
        let afterRead = try await session.read(path: "old.txt")
        #expect(String(data: afterRead, encoding: .utf8) == "old content")
    }

    @Test("setStagingPointer sets staging to specified edition")
    func setStagingPointerWorks() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create and stage an edition
        let editor = try await ContentSession(storage: storage, mode: .staging)
        try await editor.checkout(label: "first")
        let edition1 = await editor.editionId
        try await editor.write(path: "v1.txt", data: "version 1".data(using: .utf8)!)
        try await editor.submit(message: "First")

        let admin = try await ContentSession(storage: storage, mode: .staging)
        try await admin.stage(edition: edition1)

        // Create another edition
        let editor2 = try await ContentSession(storage: storage, mode: .staging)
        try await editor2.checkout(label: "second")
        let edition2 = await editor2.editionId
        try await editor2.write(path: "v2.txt", data: "version 2".data(using: .utf8)!)
        try await editor2.submit(message: "Second")

        try await admin.stage(edition: edition2)

        // Verify staging is at edition2
        let staging1 = try await ContentSession(storage: storage, mode: .staging)
        #expect(await staging1.editionId == edition2)

        // Rollback to edition1
        try await admin.setStagingPointer(to: edition1)

        // Verify staging is now at edition1
        let staging2 = try await ContentSession(storage: storage, mode: .staging)
        #expect(await staging2.editionId == edition1)
    }

    @Test("setStagingPointer rejects non-existent edition")
    func setStagingPointerRejectsNonExistent() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .staging)

        await #expect(throws: ContentError.editionNotFound(edition: 99999)) {
            try await session.setStagingPointer(to: 99999)
        }
    }

    @Test("Reject removes pending and creates rejection record")
    func rejectRemovesPending() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create and submit
        let editor = try await ContentSession(storage: storage, mode: .staging)
        try await editor.checkout(label: "to-reject")
        let editionId = await editor.editionId
        try await editor.write(path: "test.txt", data: "test".data(using: .utf8)!)
        try await editor.submit(message: "Test")

        // Verify pending exists
        let pendingExists = try await storage.exists(path: "contents/.pending/\(editionId).json")
        #expect(pendingExists == true)

        // Reject
        let admin = try await ContentSession(storage: storage, mode: .staging)
        try await admin.reject(edition: editionId, reason: "Not ready")

        // Pending should be removed
        let pendingAfter = try await storage.exists(path: "contents/.pending/\(editionId).json")
        #expect(pendingAfter == false)

        // Rejection record should exist
        let rejectedPath = "contents/.rejected/\(editionId).json"
        let rejectedExists = try await storage.exists(path: rejectedPath)
        #expect(rejectedExists == true)

        // Verify rejection content
        let rejectedData = try await storage.read(path: rejectedPath)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let rejection = try decoder.decode(RejectedSubmission.self, from: rejectedData)
        #expect(rejection.edition == editionId)
        #expect(rejection.reason == "Not ready")
    }

    @Test("Reject throws for non-existent pending")
    func rejectThrowsForNonExistent() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        let session = try await ContentSession(storage: storage, mode: .staging)

        await #expect(throws: ContentError.pendingNotFound(edition: 99999)) {
            try await session.reject(edition: 99999, reason: "test")
        }
    }

    @Test("listRejected returns rejected submissions")
    func listRejectedReturnsRejections() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create and reject two submissions
        let editor1 = try await ContentSession(storage: storage, mode: .staging)
        try await editor1.checkout(label: "reject1")
        let edition1 = await editor1.editionId
        try await editor1.write(path: "test1.txt", data: "content1".data(using: .utf8)!)
        try await editor1.submit(message: "First")

        let editor2 = try await ContentSession(storage: storage, mode: .staging)
        try await editor2.checkout(label: "reject2")
        let edition2 = await editor2.editionId
        try await editor2.write(path: "test2.txt", data: "content2".data(using: .utf8)!)
        try await editor2.submit(message: "Second")

        let admin = try await ContentSession(storage: storage, mode: .staging)
        try await admin.reject(edition: edition1, reason: "Bad format")
        try await admin.reject(edition: edition2, reason: "Duplicate content")

        // List rejections
        let rejections = try await admin.listRejected()
        #expect(rejections.count == 2)
        #expect(rejections[0].edition == edition1)
        #expect(rejections[0].reason == "Bad format")
        #expect(rejections[1].edition == edition2)
        #expect(rejections[1].reason == "Duplicate content")
    }

    @Test("getRejection returns rejection for specific edition")
    func getRejectionReturnsRejection() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create and reject a submission
        let editor = try await ContentSession(storage: storage, mode: .staging)
        try await editor.checkout(label: "get-reject")
        let editionId = await editor.editionId
        try await editor.write(path: "test.txt", data: "content".data(using: .utf8)!)
        try await editor.submit(message: "Test")

        let admin = try await ContentSession(storage: storage, mode: .staging)
        try await admin.reject(edition: editionId, reason: "Test rejection")

        // Get specific rejection
        let rejection = try await admin.getRejection(edition: editionId)
        #expect(rejection != nil)
        #expect(rejection?.edition == editionId)
        #expect(rejection?.reason == "Test rejection")

        // Non-existent rejection returns nil
        let missing = try await admin.getRejection(edition: 99999)
        #expect(missing == nil)
    }

    @Test("getRejection throws for corrupt rejection file")
    func getRejectionThrowsForCorrupt() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Write corrupt JSON to .rejected/
        try await storage.write(
            path: "contents/.rejected/12345.json",
            data: "not valid json".data(using: .utf8)!
        )

        let session = try await ContentSession(storage: storage, mode: .staging)
        await #expect(throws: ContentError.self) {
            _ = try await session.getRejection(edition: 12345)
        }
    }

    @Test("GC collects live editions correctly")
    func gcCollectsLiveEditions() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create some content
        let editor = try await ContentSession(storage: storage, mode: .staging)
        try await editor.checkout(label: "gc-test")
        try await editor.write(path: "test.txt", data: "content".data(using: .utf8)!)
        try await editor.submit(message: "Test")

        // Run GC (dry-run is default)
        let admin = try await ContentSession(storage: storage, mode: .staging)
        let result = try await admin.gc()

        // Should have scanned objects
        #expect(result.scannedObjects >= 0)
        // No deletions in dry-run mode
        #expect(result.deletedObjects == 0)
    }

    @Test("GC uses ref file fast path")
    func gcUsesRefFastPath() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create and stage an edition (which creates .ref files)
        let editor = try await ContentSession(storage: storage, mode: .staging)
        try await editor.checkout(label: "ref-test")
        let editionId = await editor.editionId
        try await editor.write(path: "test.txt", data: "content".data(using: .utf8)!)
        try await editor.submit(message: "Test")

        let admin = try await ContentSession(storage: storage, mode: .staging)
        try await admin.stage(edition: editionId)

        // Run GC (dry-run)
        let result = try await admin.gc(dryRun: true)

        // Should have used ref fast path for staged objects
        #expect(result.skippedByRef >= 1)
    }

    // MARK: - Origin Integrity Tests

    @Test("Flatten throws integrityError for malformed .origin")
    func flattenThrowsForMalformedOrigin() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create an edition chain
        let editor = try await ContentSession(storage: storage, mode: .staging)
        try await editor.checkout(label: "malformed-origin")
        let editionId = await editor.editionId
        try await editor.write(path: "test.txt", data: "content".data(using: .utf8)!)
        try await editor.submit(message: "Test")

        // Corrupt the .origin file with non-integer content
        try await storage.write(
            path: "contents/editions/\(editionId)/.origin",
            data: "not-an-integer".data(using: .utf8)!
        )

        // Flatten should throw integrityError
        let admin = try await ContentSession(storage: storage, mode: .staging)
        await #expect(throws: ContentError.self) {
            try await admin.flatten(edition: editionId)
        }
    }

    @Test("collectAncestry throws integrityError for malformed .origin during flatten")
    func collectAncestryThrowsForMalformedOrigin() async throws {
        defer { cleanup() }
        let storage = try await setupStorage()

        // Create an edition chain: genesis -> first -> second
        let editor = try await ContentSession(storage: storage, mode: .staging)
        try await editor.checkout(label: "first")
        let firstId = await editor.editionId
        try await editor.write(path: "test.txt", data: "content".data(using: .utf8)!)
        try await editor.submit(message: "First")

        let admin = try await ContentSession(storage: storage, mode: .staging)
        try await admin.stage(edition: firstId)

        let editor2 = try await ContentSession(storage: storage, mode: .staging)
        try await editor2.checkout(label: "second")
        let secondId = await editor2.editionId
        try await editor2.write(path: "test2.txt", data: "content2".data(using: .utf8)!)
        try await editor2.submit(message: "Second")

        // Corrupt the first edition's .origin (which second points to via ancestry)
        try await storage.write(
            path: "contents/editions/\(firstId)/.origin",
            data: "garbage-data".data(using: .utf8)!
        )

        // Flatten on second should throw integrityError when traversing to first's corrupted origin
        let admin2 = try await ContentSession(storage: storage, mode: .staging)
        await #expect(throws: ContentError.self) {
            try await admin2.flatten(edition: secondId)
        }
    }
}
