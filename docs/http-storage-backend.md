# RFC: HTTP Storage Backend

**Status:** Proposed
**Date:** 2025-12-12

## Summary

Add HTTP-based storage components to Kronoa:
- **`HTTPStorageBackend`** (client) - Read-only `StorageBackend` implementation over HTTP
- **`DevStorageServer`** (server) - Lightweight HTTP server exposing any `StorageBackend`

This enables viewer/consumer apps to connect to publisher/editor dev servers using the same `StorageBackend` abstraction.

## Motivation

Apps consuming Kronoa content often need two access modes:
- **Production**: Direct storage access (local filesystem or S3)
- **Development**: Remote access to another machine's local storage

Without HTTP support, consumer apps must implement custom HTTP fetching logic separate from the `StorageBackend` abstraction, creating:
- Duplicated code paths for the same operations
- Inconsistent error handling
- Testing complexity

By providing both client and server HTTP components, any app can expose or consume Kronoa storage over HTTP with zero custom code.

## Design

### HTTPStorageBackend (Client)

```swift
#if DEBUG
public final class HTTPStorageBackend: StorageBackend, Sendable {
    private let baseURL: URL
    private let session: URLSession

    public init(baseURL: URL) {
        self.baseURL = baseURL
        let config = URLSessionConfiguration.default
        config.timeoutIntervalForRequest = 30
        config.timeoutIntervalForResource = 120
        self.session = URLSession(configuration: config)
    }

    // MARK: - StorageBackend (Read Operations)

    public func read(path: String) async throws -> Data
    public func exists(path: String) async throws -> Bool
    public func list(prefix: String, delimiter: String?) async throws -> [String]

    // MARK: - StorageBackend (Write Operations - Not Supported)

    public func write(path: String, data: Data) async throws {
        throw StorageError.ioError("HTTPStorageBackend is read-only")
    }

    public func writeIfAbsent(path: String, data: Data) async throws -> Bool {
        throw StorageError.ioError("HTTPStorageBackend is read-only")
    }

    public func delete(path: String) async throws {
        throw StorageError.ioError("HTTPStorageBackend is read-only")
    }

    public func atomicIncrement(path: String, initialValue: Int) async throws -> Int {
        throw StorageError.ioError("HTTPStorageBackend is read-only")
    }

    public func acquireLock(path: String, timeout: TimeInterval, leaseDuration: TimeInterval) async throws -> LockHandle {
        throw StorageError.ioError("HTTPStorageBackend is read-only")
    }
}
#endif
```

### DevStorageServer (Server)

```swift
#if DEBUG
/// Lightweight HTTP server exposing a StorageBackend over HTTP.
@MainActor
public final class DevStorageServer: ObservableObject {
    private let storage: StorageBackend

    @Published public private(set) var isRunning: Bool
    @Published public private(set) var boundURL: URL?
    @Published public private(set) var boundInterface: String?

    public init(storage: StorageBackend) {
        self.storage = storage
    }

    /// Start server on available port (8765-8775 fallback).
    /// Returns immediately after binding; does NOT block.
    @discardableResult
    public func start(port: UInt16 = 8765) async throws -> URL

    /// Stop the server gracefully.
    public func stop() async
}
#endif
```

**Implementation notes:**
- Uses `NWListener` (Network.framework) for minimal dependencies
- Non-blocking: `start()` returns URL immediately after binding
- Port fallback: tries 8765, then 8766...8775 if in use
- `@MainActor` for SwiftUI integration (`@Published` properties)

### Network Binding

**Limitation:** `NWListener` does not support binding to a specific interface. The server binds to all interfaces but advertises only a private LAN IP to clients.

**Interface selection:**
```swift
// Prefer en0/en1, filter to private ranges (192.168.x, 10.x, 172.16-31.x)
let validPrefixes = ["192.168.", "10.", "172.16.", ...]
```

**Security relies on:**
- DEBUG-only code (stripped from release builds)
- Path validation (no traversal, no hidden files except allowed dotfiles)
- Read-only operations
- Typical dev environments behind firewall

### Authentication (Optional)

Token auth code exists but is **disabled by default** for simplicity:
- Server has `requireToken: Bool` property (default `false`)
- If enabled, generates UUID token on start
- Client sends `Authorization: Bearer <token>` header
- Server returns 401 if token missing/invalid

Not exposed in UI since dev servers are local-network-only.

## HTTP Contract

### Endpoints

| Method | Path | Query Params | Response |
|--------|------|--------------|----------|
| `GET` | `/health` | - | `{"status": "ok", "storage": "local"}` |
| `GET` | `/storage/read` | `path` | Raw file data (MIME by extension) |
| `GET` | `/storage/exists` | `path` | `{"exists": true\|false}` |
| `GET` | `/storage/list` | `prefix`, `delimiter` | `{"files": ["path1", "path2", ...]}` |

**Note:** Field names match existing Libramo implementation (`files` not `keys`, health includes `storage` field).

### Request Format

- All query parameters MUST be percent-encoded (RFC 3986)
- `Content-Type: application/json` for JSON responses
- `Content-Type` for file data inferred from extension:
  - `.json` → `application/json`
  - `.pdf` → `application/pdf`
  - `.jpg`, `.jpeg` → `image/jpeg`
  - `.png` → `image/png`
  - `.tiff`, `.tif` → `image/tiff`
  - (default) → `application/octet-stream`

### Response Status Codes

| Status | Meaning | Response Body |
|--------|---------|---------------|
| 200 | Success | Data or JSON |
| 400 | Invalid path | `{"error": "invalid_path", "message": "..."}` |
| 404 | Not found | `{"error": "not_found", "message": "..."}` |
| 500 | Server error | `{"error": "internal", "message": "..."}` |

**Note:** Error codes use `snake_case` to match existing implementation.

### Error Mapping (Client)

| HTTP Status | StorageError |
|-------------|--------------|
| 200 | Success |
| 400 | `.invalidPath(path)` |
| 404 | `.notFound(path)` |
| 5xx | `.ioError("Server error: \(statusCode) at \(url)")` |

**Network errors**: Connection failures, timeouts, and `URLError` cases map to `.ioError` with description and URL:
```swift
// Example: .ioError("Request timeout: http://192.168.1.100:8765/storage/read?path=... (URLError -1001)")
```

### Delimiter Semantics

The `delimiter` parameter MUST be forwarded to the server. `ContentSession.listInternalWithInfo` relies on `delimiter="/"` for hierarchical listing to correctly handle tombstones and edition merges. Flat listing would surface deep descendants and break override logic.

## Usage

### Server (Publisher/Editor App)

```swift
#if DEBUG
// Expose local storage over HTTP for viewer testing
let localStorage = LocalFileStorage(root: contentDirectory)
let server = DevStorageServer(storage: localStorage)

let url = try await server.start(port: 8765)
print("Dev server running at \(url)")
#endif
```

### Client (Viewer/Consumer App)

```swift
#if DEBUG
let storage = HTTPStorageBackend(baseURL: URL(string: "http://192.168.1.100:8765")!)

// Health check
guard await storage.checkHealth() else {
    print("Dev server not reachable")
    return
}

// Use same ContentSession as production
let session = try await ContentSession(storage: storage, mode: .staging)
let data = try await session.read(path: "issues/101/manifest.json")
#endif
```

## Scope

### In Scope
- Client: Read operations (`read`, `exists`, `list`), health check
- Server: HTTP endpoints for read operations
- DEBUG-only compilation (`#if DEBUG`)
- Error mapping to `StorageError`

### Out of Scope
- Write operations (dev workflow is read-only for consumers)
- Authentication (local network dev only)
- TLS/HTTPS (dev only)
- `listWithMetadata` - This is a `ContentSession` API, not `StorageBackend`

### Path Validation

Both client and server MUST validate paths. Server validation is authoritative:

```swift
private func validatePath(_ path: String) -> String? {
    // Reject empty paths (read/exists need actual paths)
    guard !path.isEmpty else { return nil }

    // Reject absolute paths
    guard !path.hasPrefix("/") else { return nil }

    // Split and validate components
    let components = path.split(separator: "/", omittingEmptySubsequences: true)
    guard !components.isEmpty else { return nil }

    // Reject traversal
    guard !components.contains("..") else { return nil }

    // Check hidden files - allow only known Kronoa dotfiles
    let allowedDotFiles = [".production.json", ".staging.json", ".origin", ".flattened", ".head"]
    for component in components {
        if component.hasPrefix(".") {
            guard allowedDotFiles.contains(String(component)) else { return nil }
        }
    }

    return components.joined(separator: "/")
}
```

**Validation rules:**

| Check | read/exists | list prefix |
|-------|-------------|-------------|
| Empty path | Reject | Allow (lists all) |
| Absolute (`/etc`) | Reject | Reject |
| Traversal (`../`) | Reject | Reject |
| Hidden files (`.foo`) | Reject (except allowed) | Reject (except allowed) |

**Allowed dotfiles:** `.production.json`, `.staging.json`, `.origin`, `.flattened`, `.head`

**List endpoint:** Handles empty prefix separately, then validates non-empty prefixes. Results are filtered through validation to exclude hidden files.

Defense in depth: client validates before sending, server validates before accessing storage.

## File Location

```
Sources/Kronoa/
├── Storage/
│   ├── StorageBackend.swift
│   ├── LocalFileStorage.swift
│   ├── S3Storage.swift
│   └── HTTPStorageBackend.swift    # NEW: Client
└── Dev/
    └── DevStorageServer.swift      # NEW: Server
```

## Dependencies

- Foundation (URLSession, NWListener)
- No additional packages required

## Testing

```swift
#if DEBUG
final class HTTPStorageTests: XCTestCase {
    var server: DevStorageServer!
    var client: HTTPStorageBackend!

    override func setUp() async throws {
        let tempStorage = LocalFileStorage(root: tempDirectory)
        // Create test files...

        server = DevStorageServer(storage: tempStorage)
        let url = try await server.start(port: 0) // Random available port
        client = HTTPStorageBackend(baseURL: url)
    }

    override func tearDown() async throws {
        await server.stop()
    }

    func testRead() async throws {
        let data = try await client.read(path: "test/file.txt")
        XCTAssertFalse(data.isEmpty)
    }

    func testNotFound() async {
        do {
            _ = try await client.read(path: "nonexistent")
            XCTFail("Expected error")
        } catch StorageError.notFound {
            // Expected
        }
    }

    func testListWithDelimiter() async throws {
        let keys = try await client.list(prefix: "articles/", delimiter: "/")
        // Should return immediate children only
    }

    func testWriteThrows() async {
        do {
            try await client.write(path: "test", data: Data())
            XCTFail("Expected error")
        } catch StorageError.ioError {
            // Expected - read-only
        }
    }
}
#endif
```

## Design Decisions

### Base URL Normalization

`HTTPStorageBackend.init(baseURL:)` strips trailing slashes to avoid double separators:
```swift
// Both produce same internal URL
HTTPStorageBackend(baseURL: URL(string: "http://localhost:8765")!)
HTTPStorageBackend(baseURL: URL(string: "http://localhost:8765/")!)
```

### Why Network.framework over SwiftNIO?

- Zero external dependencies
- Sufficient for single-client dev use
- Available on all Apple platforms

### Why Read-Only?

The HTTP backend serves the consumer/viewer use case where content is produced elsewhere. Write operations would require:
- Authentication
- Conflict resolution
- More complex server implementation

These are out of scope for dev tooling.

## Migration from Libramo HTTPContentStorage

After implementing in Kronoa, migrate Libramo Viewer:

1. **Replace client**: `HTTPContentStorage` → `HTTPStorageBackend`
2. **Remove**: `Viewer/Services/HTTPContentStorage.swift`
3. **Update imports**: Use Kronoa instead of local type

**API compatibility:**

| HTTPContentStorage | HTTPStorageBackend | Notes |
|-------------------|-------------------|-------|
| `read(path:)` | `read(path:)` | Same |
| `exists(path:)` | `exists(path:)` | Same |
| `list()` | `list(prefix: "", delimiter: "/")` | Add params for hierarchical listing |
| `checkHealth()` | `checkHealth()` | Same |
| `write`/`delete` | Throws `.ioError` | Both read-only |

**Response field names are compatible:** Both use `{"files": [...]}` and `{"exists": bool}`.

## Related

- [Architecture](architecture.md)
- [API Design](api-design.md)
