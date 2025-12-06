import XCTest
@testable import Kronoa

/// Integration tests for S3Storage.
///
/// These tests require:
/// - AWS credentials configured (via ~/.aws/credentials or environment)
/// - S3 bucket "kronoa-dev-test" in us-east-1
///
/// Tests are skipped if AWS is not available.
final class S3StorageTests: XCTestCase {
    var storage: S3Storage!
    var testPrefix: String!

    static let isS3Available: Bool = {
        // Check if AWS credentials are likely available
        let credentialsFile = FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent(".aws/credentials")
        let hasCredentialsFile = FileManager.default.fileExists(atPath: credentialsFile.path)
        let hasEnvCredentials = ProcessInfo.processInfo.environment["AWS_ACCESS_KEY_ID"] != nil
        return hasCredentialsFile || hasEnvCredentials
    }()

    override func setUp() async throws {
        try XCTSkipUnless(Self.isS3Available, "AWS credentials not available")

        // Use unique prefix per test run to avoid conflicts
        testPrefix = "test-\(UUID().uuidString)/"

        do {
            storage = try await S3Storage(
                bucket: "kronoa-dev-test",
                prefix: testPrefix,
                region: "us-east-1"
            )
        } catch {
            throw XCTSkip("Could not connect to S3: \(error)")
        }
    }

    override func tearDown() async throws {
        guard let storage = storage else { return }

        // Clean up test objects
        do {
            let objects = try await storage.list(prefix: "", delimiter: nil)
            for obj in objects {
                try? await storage.delete(path: obj)
            }
        } catch {
            // Ignore cleanup errors
        }
    }

    // MARK: - Basic Operations

    func testWriteAndRead() async throws {
        let data = "Hello, S3!".data(using: .utf8)!
        try await storage.write(path: "test.txt", data: data)

        let read = try await storage.read(path: "test.txt")
        XCTAssertEqual(read, data)
    }

    func testWriteNestedPath() async throws {
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

    func testListWithDelimiter() async throws {
        try await storage.write(path: "dir/a.txt", data: Data())
        try await storage.write(path: "dir/b.txt", data: Data())
        try await storage.write(path: "dir/sub/c.txt", data: Data())

        let entries = try await storage.list(prefix: "dir/", delimiter: "/")
        XCTAssertTrue(entries.contains("dir/a.txt"))
        XCTAssertTrue(entries.contains("dir/b.txt"))
        XCTAssertTrue(entries.contains("dir/sub/"))
        XCTAssertEqual(entries.count, 3)
    }

    func testListWithoutDelimiter() async throws {
        try await storage.write(path: "dir/a.txt", data: Data())
        try await storage.write(path: "dir/sub/c.txt", data: Data())

        let entries = try await storage.list(prefix: "dir/", delimiter: nil)
        XCTAssertTrue(entries.contains("dir/a.txt"))
        XCTAssertTrue(entries.contains("dir/sub/c.txt"))
        XCTAssertEqual(entries.count, 2)
    }

    func testListEmpty() async throws {
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
            timeout: 10,
            leaseDuration: 30
        )

        XCTAssertFalse(lock.owner.isEmpty)

        let expiry = await lock.expiresAt
        XCTAssertGreaterThan(expiry, Date())

        try await lock.release()

        // Should be able to acquire again
        let lock2 = try await storage.acquireLock(
            path: ".lock",
            timeout: 10,
            leaseDuration: 30
        )
        try await lock2.release()
    }

    func testLockRenewal() async throws {
        let lock = try await storage.acquireLock(
            path: ".lock",
            timeout: 10,
            leaseDuration: 10
        )

        let originalExpiry = await lock.expiresAt

        try await Task.sleep(nanoseconds: 100_000_000) // 100ms
        try await lock.renew(duration: 60)

        let newExpiry = await lock.expiresAt
        XCTAssertGreaterThan(newExpiry, originalExpiry)

        try await lock.release()
    }

    // MARK: - Zero-byte Content

    func testWriteAndReadEmptyData() async throws {
        let emptyData = Data()
        try await storage.write(path: "empty.txt", data: emptyData)

        let read = try await storage.read(path: "empty.txt")
        XCTAssertEqual(read, emptyData)
    }

    // MARK: - Binary Content

    func testWriteAndReadBinaryData() async throws {
        var bytes: [UInt8] = []
        for i in 0..<256 {
            bytes.append(UInt8(i))
        }
        let binaryData = Data(bytes)

        try await storage.write(path: "binary.dat", data: binaryData)

        let read = try await storage.read(path: "binary.dat")
        XCTAssertEqual(read, binaryData)
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
        try await storage.write(path: "root-file.txt", data: Data())
        let entries = try await storage.list(prefix: "", delimiter: "/")
        XCTAssertTrue(entries.contains("root-file.txt"))
    }
}
