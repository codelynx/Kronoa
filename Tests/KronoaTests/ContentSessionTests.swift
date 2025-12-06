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
}
