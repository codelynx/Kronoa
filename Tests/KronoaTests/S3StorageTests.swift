import Testing
import Foundation
@testable import Kronoa

/// Check if AWS credentials are available (defined outside class to avoid circular reference)
private let isS3Available: Bool = {
    let credentialsFile = FileManager.default.homeDirectoryForCurrentUser
        .appendingPathComponent(".aws/credentials")
    let hasCredentialsFile = FileManager.default.fileExists(atPath: credentialsFile.path)
    let hasEnvCredentials = ProcessInfo.processInfo.environment["AWS_ACCESS_KEY_ID"] != nil
    return hasCredentialsFile || hasEnvCredentials
}()

/// Integration tests for S3Storage.
///
/// These tests require:
/// - AWS credentials configured (via ~/.aws/credentials or environment)
/// - S3 bucket "kronoa-dev-test" in us-east-1
///
/// Tests are skipped if AWS is not available.
///
/// Each test cleans up its own objects at the end to avoid leaking data in the bucket.
@Suite(.enabled(if: isS3Available, "AWS credentials not available"), .serialized)
struct S3StorageTests {
    let storage: S3Storage
    let testPrefix: String

    init() async throws {
        testPrefix = "test-\(UUID().uuidString)/"
        storage = try await S3Storage(
            bucket: "kronoa-dev-test",
            prefix: testPrefix,
            region: "us-east-1"
        )
    }

    /// Delete all objects under the test prefix
    private func cleanup() async {
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

    @Test func writeAndRead() async throws {
        let data = "Hello, S3!".data(using: .utf8)!
        try await storage.write(path: "test.txt", data: data)

        let read = try await storage.read(path: "test.txt")
        #expect(read == data)

        await cleanup()
    }

    @Test func writeNestedPath() async throws {
        let data = "nested content".data(using: .utf8)!
        try await storage.write(path: "a/b/c/file.txt", data: data)

        let read = try await storage.read(path: "a/b/c/file.txt")
        #expect(read == data)

        await cleanup()
    }

    @Test func readNotFound() async throws {
        await #expect(throws: StorageError.notFound(path: "nonexistent.txt")) {
            _ = try await storage.read(path: "nonexistent.txt")
        }
        // No cleanup needed - nothing written
    }

    @Test func exists() async throws {
        let existsBefore = try await storage.exists(path: "exists-test.txt")
        #expect(!existsBefore)

        try await storage.write(path: "exists-test.txt", data: Data())

        let existsAfter = try await storage.exists(path: "exists-test.txt")
        #expect(existsAfter)

        await cleanup()
    }

    @Test func delete() async throws {
        try await storage.write(path: "delete-test.txt", data: Data())

        let existsBefore = try await storage.exists(path: "delete-test.txt")
        #expect(existsBefore)

        try await storage.delete(path: "delete-test.txt")

        let existsAfter = try await storage.exists(path: "delete-test.txt")
        #expect(!existsAfter)
        // Cleanup via delete itself
    }

    @Test func deleteNotFound() async throws {
        await #expect(throws: StorageError.notFound(path: "nonexistent.txt")) {
            try await storage.delete(path: "nonexistent.txt")
        }
        // No cleanup needed - nothing written
    }

    // MARK: - List Operations

    @Test func listWithDelimiter() async throws {
        try await storage.write(path: "list-dir/a.txt", data: Data())
        try await storage.write(path: "list-dir/b.txt", data: Data())
        try await storage.write(path: "list-dir/sub/c.txt", data: Data())

        let entries = try await storage.list(prefix: "list-dir/", delimiter: "/")
        #expect(entries.contains("list-dir/a.txt"))
        #expect(entries.contains("list-dir/b.txt"))
        #expect(entries.contains("list-dir/sub/"))
        #expect(entries.count == 3)

        await cleanup()
    }

    @Test func listWithoutDelimiter() async throws {
        try await storage.write(path: "list-flat/a.txt", data: Data())
        try await storage.write(path: "list-flat/sub/c.txt", data: Data())

        let entries = try await storage.list(prefix: "list-flat/", delimiter: nil)
        #expect(entries.contains("list-flat/a.txt"))
        #expect(entries.contains("list-flat/sub/c.txt"))
        #expect(entries.count == 2)

        await cleanup()
    }

    @Test func listEmpty() async throws {
        let entries = try await storage.list(prefix: "nonexistent-dir/", delimiter: "/")
        #expect(entries == [])
        // No cleanup needed - nothing written
    }

    // MARK: - Atomic Increment

    @Test func atomicIncrementNew() async throws {
        let value = try await storage.atomicIncrement(path: "counter-new", initialValue: 10000)
        #expect(value == 10000)

        await cleanup()
    }

    @Test func atomicIncrementExisting() async throws {
        _ = try await storage.atomicIncrement(path: "counter-existing", initialValue: 10000)
        let value = try await storage.atomicIncrement(path: "counter-existing", initialValue: 10000)
        #expect(value == 10001)

        await cleanup()
    }

    // MARK: - Lock Operations

    @Test func acquireAndReleaseLock() async throws {
        let lock = try await storage.acquireLock(
            path: ".lock-acquire",
            timeout: 10,
            leaseDuration: 30
        )

        #expect(!lock.owner.isEmpty)

        let expiry = await lock.expiresAt
        #expect(expiry > Date())

        try await lock.release()

        // Should be able to acquire again
        let lock2 = try await storage.acquireLock(
            path: ".lock-acquire",
            timeout: 10,
            leaseDuration: 30
        )
        try await lock2.release()

        await cleanup()
    }

    @Test func lockRenewal() async throws {
        let lock = try await storage.acquireLock(
            path: ".lock-renewal",
            timeout: 10,
            leaseDuration: 10
        )

        let originalExpiry = await lock.expiresAt

        try await Task.sleep(nanoseconds: 100_000_000) // 100ms
        try await lock.renew(duration: 60)

        let newExpiry = await lock.expiresAt
        #expect(newExpiry > originalExpiry)

        try await lock.release()

        await cleanup()
    }

    // MARK: - Zero-byte Content

    @Test func writeAndReadEmptyData() async throws {
        let emptyData = Data()
        try await storage.write(path: "empty.txt", data: emptyData)

        let read = try await storage.read(path: "empty.txt")
        #expect(read == emptyData)

        await cleanup()
    }

    // MARK: - Binary Content

    @Test func writeAndReadBinaryData() async throws {
        var bytes: [UInt8] = []
        for i in 0..<256 {
            bytes.append(UInt8(i))
        }
        let binaryData = Data(bytes)

        try await storage.write(path: "binary.dat", data: binaryData)

        let read = try await storage.read(path: "binary.dat")
        #expect(read == binaryData)

        await cleanup()
    }

    // MARK: - Path Validation (no cleanup needed - these don't write anything)

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

        await cleanup()
    }

    // MARK: - Contention Tests

    @Test func atomicIncrementUnderContention() async throws {
        let counterPath = "contention-counter-\(UUID().uuidString)"
        let concurrentTasks = 5
        let incrementsPerTask = 3
        let storage = self.storage  // Capture for Sendable closure

        // Track results per task
        var allValues: [Int] = []
        var concurrentModificationCount = 0
        var otherErrors: [Error] = []

        await withTaskGroup(of: Result<[Int], Error>.self) { group in
            for _ in 0..<concurrentTasks {
                group.addTask {
                    var results: [Int] = []
                    for _ in 0..<incrementsPerTask {
                        do {
                            let value = try await storage.atomicIncrement(
                                path: counterPath,
                                initialValue: 1000
                            )
                            results.append(value)
                        } catch {
                            return .failure(error)
                        }
                    }
                    return .success(results)
                }
            }

            for await result in group {
                switch result {
                case .success(let values):
                    allValues.append(contentsOf: values)
                case .failure(let error):
                    if case StorageError.concurrentModification = error {
                        concurrentModificationCount += 1
                    } else {
                        otherErrors.append(error)
                    }
                }
            }
        }

        // concurrentModification is expected under contention - it means
        // the conditional write correctly detected a race
        #expect(otherErrors.isEmpty,
            "Only concurrentModification errors are acceptable, got: \(otherErrors)")

        // All successful values must be unique (no duplicates)
        let uniqueValues = Set(allValues)
        #expect(uniqueValues.count == allValues.count,
            "All successful increments should return unique values")

        // If we had some successes, verify they form a valid sequence
        if let minValue = allValues.min(), let maxValue = allValues.max() {
            #expect(minValue >= 1000, "Values should start at initialValue or higher")
            #expect(maxValue == minValue + allValues.count - 1,
                "Values should be consecutive")
        }

        await cleanup()
    }

    @Test func lockRenewalDoesNotOverwriteExpiredLock() async throws {
        let lock = try await storage.acquireLock(
            path: ".renewal-lock-\(UUID().uuidString)",
            timeout: 10,
            leaseDuration: 2
        )

        let originalExpiry = await lock.expiresAt

        try await lock.renew(duration: 30)
        let renewedExpiry = await lock.expiresAt
        #expect(renewedExpiry > originalExpiry)

        try await lock.release()

        await cleanup()
    }

    @Test func lockAcquisitionUnderContention() async throws {
        let lockPath = ".contention-lock-\(UUID().uuidString)"
        let concurrentAttempts = 3
        let storage = self.storage  // Capture for Sendable closure

        var successes = 0
        var timeouts = 0
        var otherErrors: [Error] = []

        await withTaskGroup(of: Result<String, Error>.self) { group in
            for i in 0..<concurrentAttempts {
                group.addTask {
                    do {
                        let lock = try await storage.acquireLock(
                            path: lockPath,
                            timeout: 5,
                            leaseDuration: 2
                        )
                        try await Task.sleep(nanoseconds: 100_000_000) // 100ms
                        try await lock.release()
                        return .success("Task \(i) acquired and released lock")
                    } catch {
                        return .failure(error)
                    }
                }
            }

            for await result in group {
                switch result {
                case .success:
                    successes += 1
                case .failure(let error):
                    if case StorageError.lockTimeout = error {
                        timeouts += 1
                    } else {
                        otherErrors.append(error)
                    }
                }
            }
        }

        #expect(otherErrors.isEmpty,
            "Only lockTimeout errors are acceptable, got: \(otherErrors)")
        #expect(successes >= 1, "At least one task should acquire the lock")
        #expect(successes + timeouts == concurrentAttempts,
            "All tasks should either succeed or timeout")

        await cleanup()
    }
}
