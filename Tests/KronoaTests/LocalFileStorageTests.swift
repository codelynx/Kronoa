import Testing
import Foundation
@testable import Kronoa

@Suite struct LocalFileStorageTests {
    var storage: LocalFileStorage
    var tempDir: URL

    init() throws {
        tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kronoa-tests-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        storage = LocalFileStorage(root: tempDir)
    }

    // Note: Swift Testing structs don't have deinit.
    // Temp directories are cleaned up by the OS or manual cleanup.

    // MARK: - Basic Operations

    @Test func writeAndRead() async throws {
        let data = "Hello, World!".data(using: .utf8)!
        try await storage.write(path: "test.txt", data: data)

        let read = try await storage.read(path: "test.txt")
        #expect(read == data)
    }

    @Test func writeCreatesDirectories() async throws {
        let data = "nested content".data(using: .utf8)!
        try await storage.write(path: "a/b/c/file.txt", data: data)

        let read = try await storage.read(path: "a/b/c/file.txt")
        #expect(read == data)
    }

    @Test func readNotFound() async throws {
        await #expect(throws: StorageError.notFound(path: "nonexistent.txt")) {
            _ = try await storage.read(path: "nonexistent.txt")
        }
    }

    @Test func exists() async throws {
        let existsBefore = try await storage.exists(path: "exists-test.txt")
        #expect(!existsBefore)

        try await storage.write(path: "exists-test.txt", data: Data())

        let existsAfter = try await storage.exists(path: "exists-test.txt")
        #expect(existsAfter)
    }

    @Test func delete() async throws {
        try await storage.write(path: "delete-test.txt", data: Data())

        let existsBefore = try await storage.exists(path: "delete-test.txt")
        #expect(existsBefore)

        try await storage.delete(path: "delete-test.txt")

        let existsAfter = try await storage.exists(path: "delete-test.txt")
        #expect(!existsAfter)
    }

    @Test func deleteNotFound() async throws {
        await #expect(throws: StorageError.notFound(path: "nonexistent-delete.txt")) {
            try await storage.delete(path: "nonexistent-delete.txt")
        }
    }

    // MARK: - List Operations

    @Test func listDirectory() async throws {
        try await storage.write(path: "list-dir/a.txt", data: Data())
        try await storage.write(path: "list-dir/b.txt", data: Data())
        try await storage.write(path: "list-dir/sub/c.txt", data: Data())

        let entries = try await storage.list(prefix: "list-dir/", delimiter: "/")
        #expect(entries.sorted() == ["list-dir/a.txt", "list-dir/b.txt", "list-dir/sub/"])
    }

    @Test func listEmptyDirectory() async throws {
        let entries = try await storage.list(prefix: "nonexistent-list/", delimiter: "/")
        #expect(entries == [])
    }

    // MARK: - Atomic Increment

    @Test func atomicIncrementNew() async throws {
        let value = try await storage.atomicIncrement(path: "counter-new", initialValue: 10000)
        #expect(value == 10000)
    }

    @Test func atomicIncrementExisting() async throws {
        _ = try await storage.atomicIncrement(path: "counter-existing", initialValue: 10000)
        let value = try await storage.atomicIncrement(path: "counter-existing", initialValue: 10000)
        #expect(value == 10001)
    }

    // MARK: - Lock Operations

    @Test func acquireAndReleaseLock() async throws {
        let lock = try await storage.acquireLock(
            path: ".lock-acquire",
            timeout: 5,
            leaseDuration: 30
        )

        #expect(!lock.owner.isEmpty)

        let expiry = await lock.expiresAt
        #expect(expiry > Date())

        try await lock.release()

        // Should be able to acquire again
        let lock2 = try await storage.acquireLock(
            path: ".lock-acquire",
            timeout: 5,
            leaseDuration: 30
        )
        try await lock2.release()
    }

    @Test func lockRenewal() async throws {
        let lock = try await storage.acquireLock(
            path: ".lock-renewal",
            timeout: 5,
            leaseDuration: 5
        )

        let originalExpiry = await lock.expiresAt

        try await Task.sleep(nanoseconds: 100_000_000) // 100ms
        try await lock.renew(duration: 60)

        let newExpiry = await lock.expiresAt
        #expect(newExpiry > originalExpiry)

        try await lock.release()
    }

    @Test func lockRenewalDoesNotShortenLease() async throws {
        let lock = try await storage.acquireLock(
            path: ".lock-no-shorten",
            timeout: 5,
            leaseDuration: 60
        )

        let originalExpiry = await lock.expiresAt

        // Renew with a shorter duration - should extend from current expiry, not now
        try await lock.renew(duration: 5)

        let newExpiry = await lock.expiresAt
        #expect(newExpiry > originalExpiry)

        try await lock.release()
    }

    // MARK: - Path Validation

    @Test func readRejectsAbsolutePath() async throws {
        await #expect {
            _ = try await storage.read(path: "/etc/passwd")
        } throws: { error in
            guard case StorageError.invalidPath(let msg) = error else { return false }
            return msg.contains("Absolute")
        }
    }

    @Test func writeRejectsPathTraversal() async throws {
        await #expect {
            try await storage.write(path: "../escape.txt", data: Data())
        } throws: { error in
            guard case StorageError.invalidPath(let msg) = error else { return false }
            return msg.contains("traversal")
        }
    }

    @Test func deleteRejectsPathTraversal() async throws {
        await #expect {
            try await storage.delete(path: "foo/../../escape.txt")
        } throws: { error in
            guard case StorageError.invalidPath(let msg) = error else { return false }
            return msg.contains("traversal")
        }
    }

    @Test func existsRejectsAbsolutePath() async throws {
        await #expect {
            _ = try await storage.exists(path: "/tmp/test")
        } throws: { error in
            guard case StorageError.invalidPath(let msg) = error else { return false }
            return msg.contains("Absolute")
        }
    }

    @Test func listRejectsPathTraversal() async throws {
        await #expect {
            _ = try await storage.list(prefix: "../", delimiter: "/")
        } throws: { error in
            guard case StorageError.invalidPath(let msg) = error else { return false }
            return msg.contains("traversal")
        }
    }

    @Test func atomicIncrementRejectsPathTraversal() async throws {
        await #expect {
            _ = try await storage.atomicIncrement(path: "../counter", initialValue: 10000)
        } throws: { error in
            guard case StorageError.invalidPath(let msg) = error else { return false }
            return msg.contains("traversal")
        }
    }

    @Test func acquireLockRejectsAbsolutePath() async throws {
        await #expect {
            _ = try await storage.acquireLock(path: "/tmp/.lock", timeout: 1, leaseDuration: 5)
        } throws: { error in
            guard case StorageError.invalidPath(let msg) = error else { return false }
            return msg.contains("Absolute")
        }
    }

    @Test func rejectsCurrentDirectoryReference() async throws {
        await #expect {
            _ = try await storage.read(path: "./file.txt")
        } throws: { error in
            guard case StorageError.invalidPath(let msg) = error else { return false }
            return msg.contains("Current directory")
        }
    }

    @Test func rejectsEmptyPath() async throws {
        await #expect {
            _ = try await storage.read(path: "")
        } throws: { error in
            guard case StorageError.invalidPath(let msg) = error else { return false }
            return msg.contains("empty")
        }
    }

    @Test func listAllowsEmptyPrefix() async throws {
        try await storage.write(path: "root-file.txt", data: Data())
        let entries = try await storage.list(prefix: "", delimiter: "/")
        #expect(entries.contains("root-file.txt"))
    }
}
