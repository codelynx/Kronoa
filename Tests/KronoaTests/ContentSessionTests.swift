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
}
