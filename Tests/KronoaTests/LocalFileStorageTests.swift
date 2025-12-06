import XCTest
@testable import Kronoa

final class LocalFileStorageTests: XCTestCase {
    var storage: LocalFileStorage!
    var tempDir: URL!

    override func setUp() async throws {
        tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("kronoa-tests-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        storage = LocalFileStorage(root: tempDir)
    }

    override func tearDown() async throws {
        try? FileManager.default.removeItem(at: tempDir)
    }

    // MARK: - Basic Operations

    func testWriteAndRead() async throws {
        let data = "Hello, World!".data(using: .utf8)!
        try await storage.write(path: "test.txt", data: data)

        let read = try await storage.read(path: "test.txt")
        XCTAssertEqual(read, data)
    }

    func testWriteCreatesDirectories() async throws {
        let data = "nested content".data(using: .utf8)!
        try await storage.write(path: "a/b/c/file.txt", data: data)

        let read = try await storage.read(path: "a/b/c/file.txt")
        XCTAssertEqual(read, data)
    }

    func testReadNotFound() async throws {
        do {
            _ = try await storage.read(path: "nonexistent.txt")
            XCTFail("Expected notFound error")
        } catch StorageError.notFound(let path) {
            XCTAssertEqual(path, "nonexistent.txt")
        }
    }

    func testExists() async throws {
        let existsBefore = try await storage.exists(path: "test.txt")
        XCTAssertFalse(existsBefore)

        try await storage.write(path: "test.txt", data: Data())

        let existsAfter = try await storage.exists(path: "test.txt")
        XCTAssertTrue(existsAfter)
    }

    func testDelete() async throws {
        try await storage.write(path: "test.txt", data: Data())

        let existsBefore = try await storage.exists(path: "test.txt")
        XCTAssertTrue(existsBefore)

        try await storage.delete(path: "test.txt")

        let existsAfter = try await storage.exists(path: "test.txt")
        XCTAssertFalse(existsAfter)
    }

    func testDeleteNotFound() async throws {
        do {
            try await storage.delete(path: "nonexistent.txt")
            XCTFail("Expected notFound error")
        } catch StorageError.notFound {
            // Expected
        }
    }

    // MARK: - List Operations

    func testListDirectory() async throws {
        try await storage.write(path: "dir/a.txt", data: Data())
        try await storage.write(path: "dir/b.txt", data: Data())
        try await storage.write(path: "dir/sub/c.txt", data: Data())

        let entries = try await storage.list(prefix: "dir/", delimiter: "/")
        XCTAssertEqual(entries.sorted(), ["dir/a.txt", "dir/b.txt", "dir/sub/"])
    }

    func testListEmptyDirectory() async throws {
        let entries = try await storage.list(prefix: "nonexistent/", delimiter: "/")
        XCTAssertEqual(entries, [])
    }

    // MARK: - Atomic Increment

    func testAtomicIncrementNew() async throws {
        let value = try await storage.atomicIncrement(path: "counter", initialValue: 10000)
        XCTAssertEqual(value, 10000)
    }

    func testAtomicIncrementExisting() async throws {
        _ = try await storage.atomicIncrement(path: "counter", initialValue: 10000)
        let value = try await storage.atomicIncrement(path: "counter", initialValue: 10000)
        XCTAssertEqual(value, 10001)
    }

    // MARK: - Lock Operations

    func testAcquireAndReleaseLock() async throws {
        let lock = try await storage.acquireLock(
            path: ".lock",
            timeout: 5,
            leaseDuration: 30
        )

        XCTAssertFalse(lock.owner.isEmpty)

        let expiry = await lock.expiresAt
        XCTAssertGreaterThan(expiry, Date())

        try await lock.release()

        // Should be able to acquire again
        let lock2 = try await storage.acquireLock(
            path: ".lock",
            timeout: 5,
            leaseDuration: 30
        )
        try await lock2.release()
    }

    func testLockRenewal() async throws {
        let lock = try await storage.acquireLock(
            path: ".lock",
            timeout: 5,
            leaseDuration: 5
        )

        let originalExpiry = await lock.expiresAt

        try await Task.sleep(nanoseconds: 100_000_000) // 100ms
        try await lock.renew(duration: 60)

        let newExpiry = await lock.expiresAt
        XCTAssertGreaterThan(newExpiry, originalExpiry)

        try await lock.release()
    }

    func testLockRenewalDoesNotShortenLease() async throws {
        // Acquire lock with 60 second lease
        let lock = try await storage.acquireLock(
            path: ".lock",
            timeout: 5,
            leaseDuration: 60
        )

        let originalExpiry = await lock.expiresAt

        // Renew with a shorter duration - should extend from current expiry, not now
        try await lock.renew(duration: 5)

        let newExpiry = await lock.expiresAt

        // New expiry should be greater than original (extended from original expiry)
        // Not shortened to now + 5 seconds
        XCTAssertGreaterThan(newExpiry, originalExpiry)

        try await lock.release()
    }

    // MARK: - Path Validation

    func testReadRejectsAbsolutePath() async throws {
        do {
            _ = try await storage.read(path: "/etc/passwd")
            XCTFail("Expected invalidPath error")
        } catch StorageError.invalidPath(let msg) {
            XCTAssertTrue(msg.contains("Absolute"))
        }
    }

    func testWriteRejectsPathTraversal() async throws {
        do {
            try await storage.write(path: "../escape.txt", data: Data())
            XCTFail("Expected invalidPath error")
        } catch StorageError.invalidPath(let msg) {
            XCTAssertTrue(msg.contains("traversal"))
        }
    }

    func testDeleteRejectsPathTraversal() async throws {
        do {
            try await storage.delete(path: "foo/../../escape.txt")
            XCTFail("Expected invalidPath error")
        } catch StorageError.invalidPath(let msg) {
            XCTAssertTrue(msg.contains("traversal"))
        }
    }

    func testExistsRejectsAbsolutePath() async throws {
        do {
            _ = try await storage.exists(path: "/tmp/test")
            XCTFail("Expected invalidPath error")
        } catch StorageError.invalidPath(let msg) {
            XCTAssertTrue(msg.contains("Absolute"))
        }
    }

    func testListRejectsPathTraversal() async throws {
        do {
            _ = try await storage.list(prefix: "../", delimiter: "/")
            XCTFail("Expected invalidPath error")
        } catch StorageError.invalidPath(let msg) {
            XCTAssertTrue(msg.contains("traversal"))
        }
    }

    func testAtomicIncrementRejectsPathTraversal() async throws {
        do {
            _ = try await storage.atomicIncrement(path: "../counter", initialValue: 10000)
            XCTFail("Expected invalidPath error")
        } catch StorageError.invalidPath(let msg) {
            XCTAssertTrue(msg.contains("traversal"))
        }
    }

    func testAcquireLockRejectsAbsolutePath() async throws {
        do {
            _ = try await storage.acquireLock(path: "/tmp/.lock", timeout: 1, leaseDuration: 5)
            XCTFail("Expected invalidPath error")
        } catch StorageError.invalidPath(let msg) {
            XCTAssertTrue(msg.contains("Absolute"))
        }
    }

    func testRejectsCurrentDirectoryReference() async throws {
        do {
            _ = try await storage.read(path: "./file.txt")
            XCTFail("Expected invalidPath error")
        } catch StorageError.invalidPath(let msg) {
            XCTAssertTrue(msg.contains("Current directory"))
        }
    }

    func testRejectsEmptyPath() async throws {
        do {
            _ = try await storage.read(path: "")
            XCTFail("Expected invalidPath error")
        } catch StorageError.invalidPath(let msg) {
            XCTAssertTrue(msg.contains("empty"))
        }
    }

    func testListAllowsEmptyPrefix() async throws {
        // Empty prefix is valid for list (means list root)
        try await storage.write(path: "root-file.txt", data: Data())
        let entries = try await storage.list(prefix: "", delimiter: "/")
        XCTAssertTrue(entries.contains("root-file.txt"))
    }
}
