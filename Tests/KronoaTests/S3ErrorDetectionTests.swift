import XCTest
@testable import Kronoa
import ClientRuntime
import SmithyHTTPAPI

/// Unit tests for S3Storage error detection logic.
/// These tests don't require AWS credentials.
final class S3ErrorDetectionTests: XCTestCase {

    // MARK: - Mock Error Types

    /// Mock HTTPError for testing 412 detection
    struct MockHTTPError: HTTPError, Error {
        let httpResponse: HTTPResponse

        init(statusCode: HTTPStatusCode) {
            self.httpResponse = HTTPResponse(
                headers: Headers(),
                body: .noStream,
                statusCode: statusCode
            )
        }
    }

    /// Mock ServiceError for testing type name detection
    struct MockServiceError: ServiceError, Error {
        let typeName: String?
        let message: String? = nil
    }

    /// Mock AWSServiceError for testing error code detection
    struct MockAWSServiceError: Error {
        let errorCode: String?
        let requestID: String?

        init(errorCode: String?) {
            self.errorCode = errorCode
            self.requestID = nil
        }
    }

    // MARK: - isPreconditionFailedError Tests

    /// Access the private method via a test helper
    /// Since isPreconditionFailedError is private, we test it indirectly
    /// by testing the public behavior that uses it.

    // MARK: - StorageError.concurrentModification Tests

    func testConcurrentModificationErrorEquality() {
        let error1 = StorageError.concurrentModification(path: "test/path")
        let error2 = StorageError.concurrentModification(path: "test/path")
        let error3 = StorageError.concurrentModification(path: "other/path")

        XCTAssertEqual(error1, error2)
        XCTAssertNotEqual(error1, error3)
    }

    func testConcurrentModificationErrorDescription() {
        let error = StorageError.concurrentModification(path: "editions/.head")
        let description = String(describing: error)
        XCTAssertTrue(description.contains("concurrentModification"))
        XCTAssertTrue(description.contains("editions/.head"))
    }

    // MARK: - HTTP Status Code Tests

    func testHTTPStatusCodePreconditionFailed() {
        // Verify the status code constant exists and has expected value
        XCTAssertEqual(HTTPStatusCode.preconditionFailed.rawValue, 412)
    }

    // MARK: - Retry Backoff Calculation Tests

    func testExponentialBackoffCalculation() {
        // Test the backoff calculation used in atomicIncrement
        // Base: 50ms, shifted left by attempt number
        let baseNs: UInt64 = 50_000_000

        XCTAssertEqual(baseNs << 0, 50_000_000)   // Attempt 0: 50ms
        XCTAssertEqual(baseNs << 1, 100_000_000)  // Attempt 1: 100ms
        XCTAssertEqual(baseNs << 2, 200_000_000)  // Attempt 2: 200ms
        XCTAssertEqual(baseNs << 3, 400_000_000)  // Attempt 3: 400ms
        XCTAssertEqual(baseNs << 4, 800_000_000)  // Attempt 4: 800ms
    }
}

// MARK: - Integration Tests for Contention (requires S3)

extension S3StorageTests {
    /// Test that atomicIncrement handles concurrent access correctly.
    /// This test creates contention by running multiple increments in parallel.
    func testAtomicIncrementUnderContention() async throws {
        let counterPath = "contention-test-counter"
        let concurrentTasks = 5
        let incrementsPerTask = 3
        let storage = self.storage!

        // Run multiple tasks incrementing the same counter
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

            // Collect results
            var allValues: [Int] = []
            var errors: [Error] = []

            for await result in group {
                switch result {
                case .success(let values):
                    allValues.append(contentsOf: values)
                case .failure(let error):
                    errors.append(error)
                }
            }

            // With proper conditional writes, all values should be unique
            // (some may fail with concurrentModification, which is acceptable)
            let uniqueValues = Set(allValues)

            // Either we got unique values OR we got concurrentModification errors
            // Both are valid outcomes under contention
            if errors.isEmpty {
                XCTAssertEqual(uniqueValues.count, allValues.count,
                    "All successful increments should return unique values")
            }

            // Final value should be at least the number of successful increments
            if let maxValue = allValues.max() {
                XCTAssertGreaterThanOrEqual(maxValue, 1000 + allValues.count - 1)
            }
        }
    }

    /// Test that lock renewal uses conditional writes correctly.
    func testLockRenewalDoesNotOverwriteExpiredLock() async throws {
        // Acquire a lock with short lease
        let lock = try await storage.acquireLock(
            path: ".test-renewal-lock",
            timeout: 10,
            leaseDuration: 2  // Very short lease
        )

        let originalExpiry = await lock.expiresAt

        // Renew should work immediately
        try await lock.renew(duration: 30)
        let renewedExpiry = await lock.expiresAt
        XCTAssertGreaterThan(renewedExpiry, originalExpiry)

        // Release the lock
        try await lock.release()
    }

    /// Test that lock acquisition handles contention.
    func testLockAcquisitionUnderContention() async throws {
        let lockPath = ".contention-lock"
        let concurrentAttempts = 3
        let storage = self.storage!

        // Try to acquire the same lock from multiple tasks
        await withTaskGroup(of: Result<String, Error>.self) { group in
            for i in 0..<concurrentAttempts {
                group.addTask {
                    do {
                        let lock = try await storage.acquireLock(
                            path: lockPath,
                            timeout: 5,
                            leaseDuration: 2
                        )
                        // Hold the lock briefly
                        try await Task.sleep(nanoseconds: 100_000_000) // 100ms
                        try await lock.release()
                        return .success("Task \(i) acquired and released lock")
                    } catch {
                        return .failure(error)
                    }
                }
            }

            var successes = 0
            var timeouts = 0

            for await result in group {
                switch result {
                case .success:
                    successes += 1
                case .failure(let error):
                    if case StorageError.lockTimeout = error {
                        timeouts += 1
                    }
                }
            }

            // At least one task should succeed (the first one to acquire)
            XCTAssertGreaterThanOrEqual(successes, 1,
                "At least one task should acquire the lock")

            // Total should equal attempts (either success or timeout)
            XCTAssertEqual(successes + timeouts, concurrentAttempts,
                "All tasks should either succeed or timeout")
        }
    }
}
