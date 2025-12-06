import Foundation
import AWSS3
import AWSClientRuntime
import ClientRuntime
import SmithyHTTPAPI

/// S3 implementation of StorageBackend.
///
/// Uses AWS SDK for Swift. All paths are relative to the configured prefix.
/// Suitable for production deployments.
public actor S3Storage: StorageBackend {
    // S3Client is thread-safe internally; nonisolated(unsafe) silences Swift 6 warnings.
    private nonisolated(unsafe) let client: S3Client
    private let bucket: String
    private let prefix: String

    /// Initialize with bucket name and optional prefix.
    /// - Parameters:
    ///   - bucket: S3 bucket name
    ///   - prefix: Optional prefix for all keys (e.g., "contents")
    ///   - region: AWS region (defaults to us-east-1)
    public init(bucket: String, prefix: String = "", region: String = "us-east-1") async throws {
        let config = try await S3Client.S3ClientConfiguration(region: region)
        self.client = S3Client(config: config)
        self.bucket = bucket
        self.prefix = prefix.isEmpty ? "" : (prefix.hasSuffix("/") ? prefix : prefix + "/")
    }

    /// Build full S3 key from relative path.
    private func fullKey(_ path: String) -> String {
        prefix + path
    }

    // MARK: - StorageBackend Implementation

    public func read(path: String) async throws -> Data {
        try PathValidation.validatePath(path)
        let key = fullKey(path)
        do {
            let input = GetObjectInput(bucket: bucket, key: key)
            let output = try await client.getObject(input: input)
            guard let body = output.body else {
                throw StorageError.notFound(path: path)
            }
            let data = try await body.readData()
            return data ?? Data()
        } catch is AWSS3.NoSuchKey {
            throw StorageError.notFound(path: path)
        } catch let storageError as StorageError {
            throw storageError
        } catch {
            // Check if it's a service error with NotFound type
            if let serviceError = error as? ServiceError, serviceError.typeName == "NotFound" {
                throw StorageError.notFound(path: path)
            }
            throw StorageError.ioError("S3 read failed for \(path): \(error)")
        }
    }

    public func write(path: String, data: Data) async throws {
        try PathValidation.validatePath(path)
        let key = fullKey(path)
        do {
            let input = PutObjectInput(
                body: .data(data),
                bucket: bucket,
                key: key
            )
            _ = try await client.putObject(input: input)
        } catch {
            throw StorageError.ioError("S3 write failed for \(path): \(error)")
        }
    }

    public func delete(path: String) async throws {
        try PathValidation.validatePath(path)
        let key = fullKey(path)

        // Check if object exists first
        let exists = try await self.exists(path: path)
        guard exists else {
            throw StorageError.notFound(path: path)
        }

        do {
            let input = DeleteObjectInput(bucket: bucket, key: key)
            _ = try await client.deleteObject(input: input)
        } catch {
            throw StorageError.ioError("S3 delete failed for \(path): \(error)")
        }
    }

    public func exists(path: String) async throws -> Bool {
        try PathValidation.validatePath(path)
        let key = fullKey(path)
        do {
            let input = HeadObjectInput(bucket: bucket, key: key)
            _ = try await client.headObject(input: input)
            return true
        } catch is AWSS3.NotFound {
            return false
        } catch {
            // Check if it's a service error with NotFound type
            if let serviceError = error as? ServiceError, serviceError.typeName == "NotFound" {
                return false
            }
            throw StorageError.ioError("S3 exists check failed for \(path): \(error)")
        }
    }

    public func list(prefix listPrefix: String, delimiter: String?) async throws -> [String] {
        try PathValidation.validatePrefix(listPrefix)
        let fullPrefix = fullKey(listPrefix)
        var results: [String] = []
        var continuationToken: String? = nil

        repeat {
            let input = ListObjectsV2Input(
                bucket: bucket,
                continuationToken: continuationToken,
                delimiter: delimiter,
                prefix: fullPrefix
            )

            let output = try await client.listObjectsV2(input: input)

            // Add objects (files)
            if let contents = output.contents {
                for object in contents {
                    if let key = object.key {
                        // Remove our prefix to get relative path
                        let relativePath = String(key.dropFirst(self.prefix.count))
                        results.append(relativePath)
                    }
                }
            }

            // Add common prefixes (directories) if delimiter is used
            if let commonPrefixes = output.commonPrefixes {
                for prefix in commonPrefixes {
                    if let prefixStr = prefix.prefix {
                        let relativePath = String(prefixStr.dropFirst(self.prefix.count))
                        results.append(relativePath)
                    }
                }
            }

            continuationToken = output.nextContinuationToken
        } while continuationToken != nil

        return results.sorted()
    }

    public func atomicIncrement(path: String, initialValue: Int = 10000) async throws -> Int {
        try PathValidation.validatePath(path)

        // S3 doesn't support atomic operations natively.
        // We use optimistic locking with ETag (If-Match) and bounded retry.
        let key = fullKey(path)
        let maxRetries = 5

        // Track whether object exists (separate from having an ETag)
        enum ObjectState {
            case notFound
            case existsWithETag(String, value: Int)
            case existsWithoutETag(value: Int)  // Some S3 configs don't return ETag
        }

        for attempt in 0..<maxRetries {
            // Fresh read each attempt
            var state: ObjectState = .notFound

            do {
                let getInput = GetObjectInput(bucket: bucket, key: key)
                let getOutput = try await client.getObject(input: getInput)

                var value = initialValue - 1
                if let body = getOutput.body {
                    let data = try await body.readData()
                    if let data = data,
                       let content = String(data: data, encoding: .utf8),
                       let parsed = Int(content.trimmingCharacters(in: .whitespacesAndNewlines)) {
                        value = parsed
                    }
                }

                if let etag = getOutput.eTag, !etag.isEmpty {
                    state = .existsWithETag(etag, value: value)
                } else {
                    // Object exists but no ETag - can't use conditional write safely
                    state = .existsWithoutETag(value: value)
                }
            } catch is AWSS3.NoSuchKey {
                state = .notFound
            } catch {
                if let serviceError = error as? ServiceError, serviceError.typeName == "NotFound" {
                    state = .notFound
                } else {
                    throw StorageError.ioError("S3 atomicIncrement read failed for \(path): \(error)")
                }
            }

            let currentValue: Int
            switch state {
            case .notFound:
                currentValue = initialValue - 1
            case .existsWithETag(_, let value), .existsWithoutETag(let value):
                currentValue = value
            }

            let newValue = currentValue + 1
            let newData = "\(newValue)".data(using: .utf8)!

            // Write new value with conditional check when possible
            do {
                switch state {
                case .existsWithETag(let etag, _):
                    // Object exists with ETag - use If-Match for safe conditional write
                    let putInput = PutObjectInput(
                        body: .data(newData),
                        bucket: bucket,
                        ifMatch: etag,
                        key: key
                    )
                    _ = try await client.putObject(input: putInput)

                case .notFound:
                    // New object - use If-None-Match to prevent race
                    let putInput = PutObjectInput(
                        body: .data(newData),
                        bucket: bucket,
                        ifNoneMatch: "*",
                        key: key
                    )
                    _ = try await client.putObject(input: putInput)

                case .existsWithoutETag:
                    // Object exists but no ETag available.
                    // Cannot use conditional write safely - do unconditional write.
                    // This is a limitation; concurrent writers may conflict.
                    // Consider using DynamoDB for true atomicity if this is common.
                    let putInput = PutObjectInput(
                        body: .data(newData),
                        bucket: bucket,
                        key: key
                    )
                    _ = try await client.putObject(input: putInput)
                }
                // Success
                return newValue
            } catch {
                let isPreconditionFailed = isPreconditionFailedError(error)
                if isPreconditionFailed && attempt < maxRetries - 1 {
                    // Exponential backoff: 50ms, 100ms, 200ms, 400ms
                    let backoffNs = UInt64(50_000_000) << attempt
                    try await Task.sleep(nanoseconds: backoffNs)
                    continue
                }
                if isPreconditionFailed {
                    throw StorageError.concurrentModification(path: path)
                }
                throw StorageError.ioError("S3 atomicIncrement failed for \(path): \(error)")
            }
        }

        throw StorageError.concurrentModification(path: path)
    }

    /// Check if an error is a 412 Precondition Failed response.
    private func isPreconditionFailedError(_ error: Error) -> Bool {
        // Primary check: HTTP status code via HTTPError protocol
        // Most S3 errors (including conditional write failures) conform to HTTPError
        if let httpError = error as? HTTPError {
            return httpError.httpResponse.statusCode == .preconditionFailed
        }

        // Secondary check: AWSServiceError/ServiceError error codes
        // Covers cases where SDK wraps the error differently
        if let awsError = error as? AWSServiceError {
            let code = awsError.errorCode ?? ""
            if code == "PreconditionFailed" || code == "412" {
                return true
            }
        }
        if let serviceError = error as? ServiceError {
            let typeName = serviceError.typeName ?? ""
            if typeName == "PreconditionFailed" || typeName == "412" {
                return true
            }
        }

        // Check error type name for any "PreconditionFailed" typed errors
        // This catches AWSS3.PreconditionFailed or similar SDK-specific types
        let typeName = String(describing: type(of: error))
        if typeName.contains("PreconditionFailed") {
            return true
        }

        // Fallback: Check if error description contains 412
        // This catches edge cases where the error is wrapped unexpectedly
        let desc = String(describing: error).lowercased()
        return desc.contains("412") || desc.contains("preconditionfailed")
    }

    public func acquireLock(
        path: String,
        timeout: TimeInterval,
        leaseDuration: TimeInterval
    ) async throws -> LockHandle {
        try PathValidation.validatePath(path)
        return try await S3FileLock(
            storage: self,
            path: path,
            timeout: timeout,
            leaseDuration: leaseDuration
        )
    }

    // MARK: - Internal helpers for lock

    func readLockInfo(path: String) async throws -> LockInfo? {
        do {
            let data = try await read(path: path)
            let decoder = JSONDecoder()
            decoder.dateDecodingStrategy = .iso8601
            return try decoder.decode(LockInfo.self, from: data)
        } catch StorageError.notFound {
            return nil
        }
    }

    /// Read lock info along with its ETag for conditional updates.
    func readLockInfoWithETag(path: String) async throws -> (info: LockInfo, etag: String)? {
        let key = fullKey(path)
        do {
            let getInput = GetObjectInput(bucket: bucket, key: key)
            let getOutput = try await client.getObject(input: getInput)

            guard let body = getOutput.body else {
                return nil
            }
            guard let data = try await body.readData() else {
                return nil
            }
            guard let etag = getOutput.eTag, !etag.isEmpty else {
                // Fall back to non-conditional path if no ETag
                let decoder = JSONDecoder()
                decoder.dateDecodingStrategy = .iso8601
                let info = try decoder.decode(LockInfo.self, from: data)
                return (info, "")
            }

            let decoder = JSONDecoder()
            decoder.dateDecodingStrategy = .iso8601
            let info = try decoder.decode(LockInfo.self, from: data)
            return (info, etag)
        } catch is AWSS3.NoSuchKey {
            return nil
        } catch {
            if let serviceError = error as? ServiceError, serviceError.typeName == "NotFound" {
                return nil
            }
            throw StorageError.ioError("S3 readLockInfoWithETag failed for \(path): \(error)")
        }
    }

    func writeLockInfo(_ info: LockInfo, path: String) async throws {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(info)
        try await write(path: path, data: data)
    }

    /// Conditionally update lock file only if ETag matches.
    /// Returns true if updated, false if ETag mismatch (lock was modified).
    func tryUpdateLockInfo(_ info: LockInfo, path: String, ifMatch etag: String) async throws -> Bool {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(info)

        let key = fullKey(path)
        do {
            let putInput = PutObjectInput(
                body: .data(data),
                bucket: bucket,
                ifMatch: etag,
                key: key
            )
            _ = try await client.putObject(input: putInput)
            return true
        } catch {
            if isPreconditionFailedError(error) {
                return false
            }
            throw StorageError.ioError("S3 lock update failed for \(path): \(error)")
        }
    }

    /// Conditionally create lock file only if it doesn't exist.
    /// Returns true if lock was created, false if it already exists.
    func tryCreateLockInfo(_ info: LockInfo, path: String) async throws -> Bool {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(info)

        // Skip path validation since this is an internal method called after validation
        let key = fullKey(path)
        do {
            let putInput = PutObjectInput(
                body: .data(data),
                bucket: bucket,
                ifNoneMatch: "*",
                key: key
            )
            _ = try await client.putObject(input: putInput)
            return true
        } catch {
            // Check if this is a precondition failure (object already exists)
            if isPreconditionFailedError(error) {
                return false
            }
            throw StorageError.ioError("S3 lock creation failed for \(path): \(error)")
        }
    }

    func deleteLockFile(path: String) async throws {
        do {
            try await delete(path: path)
        } catch StorageError.notFound {
            // Already deleted
        }
    }
}

/// S3 lock implementation.
///
/// Uses If-None-Match: * for atomic lock creation. However, note that:
/// - After deleting an expired lock, there's a race window before tryCreateLockInfo
///   where another client could also see the expired lock, delete it, and race to create.
/// - S3 provides strong read-after-write consistency for new objects (since Dec 2020),
///   but delete-then-create sequences may still race under high contention.
/// - For strict exclusivity under heavy contention, consider DynamoDB-based locking.
public actor S3FileLock: LockHandle {
    private let storage: S3Storage
    private let path: String
    public nonisolated let owner: String
    private var _expiresAt: Date

    public var expiresAt: Date {
        _expiresAt
    }

    init(
        storage: S3Storage,
        path: String,
        timeout: TimeInterval,
        leaseDuration: TimeInterval
    ) async throws {
        self.storage = storage
        self.path = path
        self.owner = UUID().uuidString
        self._expiresAt = Date()

        // Try to acquire lock
        let deadline = Date().addingTimeInterval(timeout)

        while Date() < deadline {
            // Check if lock exists
            if let existingLock = try await storage.readLockInfo(path: path) {
                if existingLock.owner == owner {
                    // We already own this lock (from a previous write that we missed verifying)
                    self._expiresAt = existingLock.expiresAt
                    return
                } else if existingLock.expiresAt < Date() {
                    // Lock expired, delete it and try to create
                    try await storage.deleteLockFile(path: path)
                    // Fall through to try creating
                } else {
                    // Lock held by someone else, wait
                    try await Task.sleep(nanoseconds: 100_000_000) // 100ms
                    continue
                }
            }

            // Try to create lock atomically (If-None-Match: *)
            let now = Date()
            let lockInfo = LockInfo(
                owner: owner,
                acquiredAt: now,
                expiresAt: now.addingTimeInterval(leaseDuration)
            )

            let created = try await storage.tryCreateLockInfo(lockInfo, path: path)
            if created {
                // We successfully created the lock
                self._expiresAt = lockInfo.expiresAt
                return
            }

            // Lock was created by someone else between our check and write.
            // Verify if it's actually ours (edge case: our earlier write succeeded but we didn't see it)
            try await Task.sleep(nanoseconds: 50_000_000) // 50ms for consistency
            if let written = try await storage.readLockInfo(path: path),
               written.owner == owner {
                self._expiresAt = written.expiresAt
                return
            }
            // Someone else got it, loop again to wait or check expiry
        }

        throw StorageError.lockTimeout
    }

    public func renew(duration: TimeInterval) async throws {
        guard let (current, etag) = try await storage.readLockInfoWithETag(path: path) else {
            throw StorageError.lockExpired
        }

        guard current.owner == owner else {
            throw StorageError.lockExpired
        }

        let now = Date()
        guard current.expiresAt > now else {
            throw StorageError.lockExpired
        }

        // Extend from the later of now or current expiry to ensure monotonic extension.
        let baseTime = max(current.expiresAt, now)
        let newExpiry = baseTime.addingTimeInterval(duration)

        let newInfo = LockInfo(
            owner: owner,
            acquiredAt: current.acquiredAt,
            expiresAt: newExpiry
        )

        // Use conditional write if ETag available, otherwise fall back to unconditional
        if !etag.isEmpty {
            let updated = try await storage.tryUpdateLockInfo(newInfo, path: path, ifMatch: etag)
            if !updated {
                // Lock was modified by another process - it may have expired and been taken
                throw StorageError.lockExpired
            }
        } else {
            // No ETag available - fall back to unconditional write (documented limitation)
            try await storage.writeLockInfo(newInfo, path: path)
        }
        _expiresAt = newInfo.expiresAt
    }

    public func release() async throws {
        guard let current = try await storage.readLockInfo(path: path) else {
            // Lock file gone - already released or expired
            return
        }

        guard current.owner == owner else {
            throw StorageError.lockExpired
        }

        try await storage.deleteLockFile(path: path)
    }
}
