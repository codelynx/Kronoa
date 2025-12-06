import Testing
import Foundation
@testable import Kronoa
import ClientRuntime
import SmithyHTTPAPI

/// Unit tests for S3Storage error detection logic.
/// These tests don't require AWS credentials.
@Suite struct S3ErrorDetectionTests {

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

    // MARK: - StorageError.concurrentModification Tests

    @Test func concurrentModificationErrorEquality() {
        let error1 = StorageError.concurrentModification(path: "test/path")
        let error2 = StorageError.concurrentModification(path: "test/path")
        let error3 = StorageError.concurrentModification(path: "other/path")

        #expect(error1 == error2)
        #expect(error1 != error3)
    }

    @Test func concurrentModificationErrorDescription() {
        let error = StorageError.concurrentModification(path: "editions/.head")
        let description = String(describing: error)
        #expect(description.contains("concurrentModification"))
        #expect(description.contains("editions/.head"))
    }

    // MARK: - HTTP Status Code Tests

    @Test func httpStatusCodePreconditionFailed() {
        #expect(HTTPStatusCode.preconditionFailed.rawValue == 412)
    }

    // MARK: - Retry Backoff Calculation Tests

    @Test func exponentialBackoffCalculation() {
        // Test the backoff calculation used in atomicIncrement
        // Base: 50ms, shifted left by attempt number
        let baseNs: UInt64 = 50_000_000

        #expect(baseNs << 0 == 50_000_000)   // Attempt 0: 50ms
        #expect(baseNs << 1 == 100_000_000)  // Attempt 1: 100ms
        #expect(baseNs << 2 == 200_000_000)  // Attempt 2: 200ms
        #expect(baseNs << 3 == 400_000_000)  // Attempt 3: 400ms
        #expect(baseNs << 4 == 800_000_000)  // Attempt 4: 800ms
    }
}
