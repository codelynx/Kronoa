# Kronoa API Design

## Storage Protocol

Abstract interface for S3/local filesystem interchangeability:

```swift
protocol StorageBackend {
    func read(path: String) async throws -> Data
    func write(path: String, data: Data) async throws
    func delete(path: String) async throws
    func exists(path: String) async throws -> Bool
    func list(prefix: String) async throws -> [String]

    // Atomic operations for coordination
    func atomicIncrement(path: String) async throws -> Int
    func acquireLock(path: String, timeout: TimeInterval, leaseDuration: TimeInterval) async throws -> LockHandle
}

protocol LockHandle {
    var owner: String { get }
    var expiresAt: Date { get }

    /// Extend the lease. Throws `lockExpired` if lease already expired.
    func renew(duration: TimeInterval) async throws

    /// Release the lock. Throws `lockExpired` if lease already expired (lock was stolen).
    func release() async throws
}
```

**Note:** No `append()` method needed. `.ref` files are written during `stage()` while holding the lock, using read-modify-write pattern (safe with single writer).

## Session API

### Initialization

```swift
let storage = LocalFileStorage(root: "/path/to/root")
// or: let storage = S3Storage(bucket: "my-bucket", prefix: "contents")

let session = try await ContentSession(storage: storage, mode: .production)
```

### Session Modes and Types

```swift
enum SessionMode {
    case production  // read-only, serves .production.json edition
    case staging     // read-only, serves .staging.json edition
    case editing(label: String)  // read-write, serves .{label}.json edition
    case submitted   // read-only, edition submitted for review
    case edition(id: Int)  // read-only, direct access to any edition by ID
}

enum CheckoutSource {
    case staging     // default: branch from current staging
    case production  // hotfix: branch from current production
}

session.mode // current mode
session.editionId // current edition number
session.baseEditionId // edition this was branched from (editing mode)
session.checkoutSource // .staging or .production (editing mode)
```

#### Direct Edition Access

Use `.edition(id:)` mode to access any edition by ID without knowing internal storage details:

```swift
// Preview a pending submission before approving
let preview = try await ContentSession(storage: storage, mode: .edition(id: 10004))
let data = try await preview.read(path: "articles/draft.md")
let files = try await preview.list(directory: "articles/")

// View historical edition
let history = try await ContentSession(storage: storage, mode: .edition(id: 10001))
```

This mode is read-only and throws `editionNotFound` if the edition doesn't exist.

### Editor Operations

```swift
/// Checkout: transition from production/staging to editing mode.
/// - Parameters:
///   - label: Unique label for this editing session
///   - from: Source to branch from (default: .staging, use .production for hotfixes)
/// - Throws: `invalidPath` if label is invalid, `labelInUse` if already exists
func checkout(label: String, from: CheckoutSource = .staging) async throws

/// Read file content, resolving through ancestry chain.
/// - Throws: `invalidPath`, `notFound` (includes tombstoned files)
func read(path: String) async throws -> Data

/// Check if file exists (false for tombstoned files).
/// - Throws: `invalidPath`
func exists(path: String) async throws -> Bool

/// Get file metadata without fetching content. Never returns nil for valid paths.
/// - Throws: `invalidPath`
func stat(path: String) async throws -> FileStat

/// List immediate children of a directory (merged from ancestry, excludes tombstones).
/// Returns AsyncThrowingStream - pagination handled internally.
/// - Parameter directory: Directory path (e.g., "articles/") or "" for root
/// - Returns: AsyncThrowingStream yielding entry names in lexicographic order
/// - Throws: `invalidPath` for malformed paths (thrown when iteration starts)
/// - Note: Returns empty sequence for non-existent or empty directories (no `notFound` error).
/// - Note: Only lists immediate children. To traverse subdirectories, call list() on each.
/// - Warning: First `await` may block until full ancestry merge completes. Items are not
///   streamed lazily from backend - all editions must be scanned before yielding begins.
func list(directory: String) -> AsyncThrowingStream<String, Error>

/// Write file content (editing mode only, buffered until endEditing).
/// - Throws: `invalidPath`, `readOnlyMode`
func write(path: String, data: Data) async throws

/// Delete file - creates tombstone (editing mode only, buffered).
/// - Throws: `invalidPath`, `readOnlyMode`
func delete(path: String) async throws

/// Copy file within the edition (server-side copy, no data transfer).
/// - Throws: `invalidPath`, `readOnlyMode`, `notFound`
func copy(from sourcePath: String, to destPath: String) async throws

/// Discard local change to a path (editing mode only).
/// - Throws: `invalidPath`, `readOnlyMode`
func discard(path: String) async throws

/// Submit edition for review (like creating a PR).
/// - Throws: `notInEditingMode`
func submit(message: String) async throws
```

### Unsupported Operations

The following operations are **not supported** by design:

| Operation | Reason | Alternative |
|-----------|--------|-------------|
| Rename file | S3 has no rename | `copy()` + `delete()` |
| Rename directory | S3 has no directories | Copy each file + delete each file |
| Move file | S3 has no move | `copy()` + `delete()` |
| Directory operations | S3 uses key prefixes, not real directories | Operate on individual files |

**Why no directory operations:**
- S3 stores objects by key (path), not in directories
- "Directories" are just common prefixes in keys
- Renaming `foo/` to `bar/` requires copying every `foo/*` object individually
- This is expensive and non-atomic; client should handle explicitly

### Transactional Editing

Writes are **deferred** - buffered in memory until `endEditing()`.

```swift
/// Begin a transaction (changes buffered in memory).
/// - Throws: `alreadyInTransaction`, `readOnlyMode`
func beginEditing() throws

/// Flush buffered changes to storage.
/// Writes objects and path files. Does NOT update .ref files (that happens in stage()).
/// - Throws: `notInTransaction`, `storageError`
func endEditing() async throws

/// Discard buffered changes without writing.
/// - Throws: `notInTransaction`
func rollback() throws

/// Check if currently in a transaction.
var isInTransaction: Bool { get }

/// Get list of buffered changes (for preview before commit).
var pendingChanges: [PendingChange] { get }
```

Single operations auto-wrap in transaction:
```swift
try await session.write("file.txt", data: data)  // implicit begin/end
```

Batch operations use explicit transaction:
```swift
try session.beginEditing()
try await session.write("a.txt", data: dataA)
try await session.write("b.txt", data: dataB)
try await session.delete("c.txt")
try await session.endEditing()  // writes objects and path files
```

**Failure handling:**
- If `endEditing()` fails mid-write, edition may have partial state
- Partial editions are never referenced by staging/production
- Editor can retry or abandon; orphan editions cleaned by GC

### Edition Info

```swift
/// Get the origin (parent) edition ID for a given edition.
///
/// Useful for comparing a pending edition against its base.
///
/// - Returns: Parent edition ID, or nil for genesis/flattened editions
/// - Throws: `editionNotFound`, `integrityError`, `storageError`
func origin(of edition: Int) async throws -> Int?
```

Example: Comparing pending edition against its base:
```swift
let pending = try await session.listPending().first!
guard let baseId = try await session.origin(of: pending.edition) else {
    // Edition is genesis or flattened - no base to compare
    return
}

let pendingSession = try await ContentSession(storage: s, mode: .edition(id: pending.edition))
let baseSession = try await ContentSession(storage: s, mode: .edition(id: baseId))

// Compare files using stat() hashes
let pendingStat = try await pendingSession.stat(path: "article.md")
let baseStat = try await baseSession.stat(path: "article.md")
if pendingStat.hash != baseStat.hash {
    // File was modified
}
```

```swift
/// List files changed locally in an edition (not inherited from ancestors).
///
/// Returns only files explicitly stored in the edition's directory,
/// showing what was set (added/modified) or deleted (tombstoned).
/// This is O(n) where n = files in the edition, not total files in ancestry.
///
/// - Returns: List of local changes sorted by path
/// - Throws: `editionNotFound`, `integrityError`, `storageError`
func localChanges(in edition: Int) async throws -> [LocalChange]

struct LocalChange {
    let path: String
    let change: LocalChangeType  // .set or .deleted
    let hash: String?            // nil for deleted
}
```

Example: Admin reviewing what a submission changed:
```swift
let pending = try await session.listPending().first!
let changes = try await session.localChanges(in: pending.edition)

for change in changes {
    switch change.change {
    case .set:
        print("SET \(change.path) → \(change.hash!)")
    case .deleted:
        print("DEL \(change.path)")
    }
}
```

### Admin Operations

```swift
/// List pending submissions awaiting review.
func listPending() async throws -> [PendingSubmission]

/// Accept a submission into staging.
/// Validates: pending exists, JSON valid, base matches source.
/// Updates .ref files for all objects in the edition.
/// - Throws: `pendingNotFound`, `pendingCorrupt`, `conflictDetected`, `lockTimeout`, `lockExpired`
func stage(edition: Int) async throws

/// Deploy staging to production.
/// - Throws: `lockTimeout`, `lockExpired`
func deploy() async throws

/// Set staging pointer to a previously-staged edition (for rollback).
/// Unlike stage(), this does not require a pending record or update .ref files.
/// Only valid for editions that were previously staged (their .ref files already exist).
///
/// Validation: Checks that the edition directory exists. Does NOT verify the edition
/// was previously staged (admin responsibility). Pointing to a never-staged edition
/// would leave its objects without .ref entries, making them GC candidates.
///
/// - Throws: `editionNotFound`, `lockTimeout`, `lockExpired`
func setStagingPointer(to edition: Int) async throws

/// Reject a submission with reason.
func reject(edition: Int, reason: String) async throws

/// List all rejected submissions.
func listRejected() async throws -> [RejectedSubmission]

/// Get rejection record for a specific edition.
/// - Returns: Rejection record, or nil if not found
/// - Throws: `rejectedCorrupt` if file exists but JSON is invalid
func getRejection(edition: Int) async throws -> RejectedSubmission?

/// Flatten an edition (copies all ancestor mappings, long operation with lock renewal).
/// - Throws: `lockTimeout`, `lockExpired`
func flatten(edition: Int) async throws

/// Run garbage collection using .ref files with fallback scan.
/// - Parameter dryRun: When true (default), only report what would be deleted.
///   Passing false requires mtime support (not yet implemented) and will crash.
/// - Throws: `lockTimeout`, `lockExpired`
func gc(dryRun: Bool = true) async throws -> GCResult
```

## Error Types

```swift
enum ContentError: Error {
    /// Path is malformed (contains .., starts with ., etc.)
    case invalidPath(String)

    /// File not found in ancestry (or tombstoned - same error for simplicity)
    case notFound(path: String)

    /// Attempted write/delete in read-only mode
    case readOnlyMode

    /// Working label already exists
    case labelInUse(String)

    /// Operation requires editing mode
    case notInEditingMode

    /// endEditing/rollback called without beginEditing
    case notInTransaction

    /// beginEditing called while already in transaction
    case alreadyInTransaction

    // Edition errors
    /// Edition directory does not exist
    case editionNotFound(edition: Int)

    // Staging errors
    /// No .pending/{edition}.json file found
    case pendingNotFound(edition: Int)

    /// .pending/{edition}.json exists but JSON is invalid
    case pendingCorrupt(edition: Int, reason: String)

    /// .rejected/{edition}.json exists but JSON is invalid
    case rejectedCorrupt(edition: Int, reason: String)

    /// Pending base doesn't match current staging/production (depending on source)
    case conflictDetected(base: Int, current: Int, source: CheckoutSource)

    // Lock errors
    /// Could not acquire lock within timeout
    case lockTimeout

    /// Lock lease expired while holding (another process may have taken it)
    case lockExpired

    /// Storage backend error
    case storageError(underlying: Error)

    /// Hash mismatch when reading object
    case integrityError(expected: String, actual: String)
}
```

## Data Types

```swift
struct EditionInfo {
    let id: Int
    let origin: Int?
    let isFlattened: Bool
}

/// File metadata returned by stat().
/// Always returned for valid paths - check status to determine file state.
struct FileStat {
    let path: String
    let status: FileStatus
    let resolvedFrom: Int  // edition-id where status was determined

    // Only present when status == .exists
    let hash: String?
    let size: Int?
}

enum FileStatus {
    case exists      // file has content
    case deleted     // tombstone marker found
    case notFound    // never existed in ancestry
}

struct PendingChange {
    let path: String
    let action: ChangeAction
}

enum ChangeAction {
    case write(hash: String, size: Int)
    case copy(fromHash: String, size: Int)  // server-side copy
    case delete
}

struct SessionState: Codable {
    let edition: Int
    let base: Int
    let source: String  // "staging" or "production"
}

struct PendingSubmission: Codable {
    let edition: Int
    let base: Int
    let source: String  // "staging" or "production"
    let label: String
    let message: String
    let submittedAt: Date
}

struct RejectedSubmission: Codable {
    let edition: Int
    let reason: String
    let rejectedAt: Date
}

/// A file change local to a specific edition (not inherited).
struct LocalChange {
    let path: String
    let change: LocalChangeType
    let hash: String?  // nil for deleted
}

enum LocalChangeType {
    case set      // file was added or modified
    case deleted  // file was deleted (tombstone)
}

struct LockInfo: Codable {
    let owner: String
    let acquiredAt: Date
    let expiresAt: Date
}

struct GCResult {
    let liveEditions: Int
    let scannedObjects: Int
    let refHits: Int       // objects kept due to .ref lookup
    let fallbackScans: Int // objects that needed fallback edition scan
    let deletedObjects: Int
    let freedBytes: Int
}
```

## API Contracts

### Path Validation

All path operations first validate the path:
- Invalid paths throw `invalidPath` immediately
- Valid paths proceed to operation

Invalid paths:
- Empty string
- Contains `..` component
- Starts with `.` (reserved for metadata)
- Contains consecutive slashes (after normalization)

### read() vs stat() vs exists()

| Method | Invalid Path | Tombstone | Not in Ancestry | Exists |
|--------|--------------|-----------|-----------------|--------|
| `read()` | throws `invalidPath` | throws `notFound` | throws `notFound` | returns `Data` |
| `stat()` | throws `invalidPath` | `.deleted` | `.notFound` | `.exists` |
| `exists()` | throws `invalidPath` | `false` | `false` | `true` |

Note: `read()` treats tombstones the same as non-existent files (both throw `notFound`).
Use `stat()` if you need to distinguish between "deleted" and "never existed".

### stage() Operation

The `stage()` operation is more complex than other operations:

1. **Acquire lock** (recommend 60s+ lease for S3)
2. **Validate pending** - read and parse `.pending/{edition}.json`
3. **Conflict check** based on source:
   - `source == "staging"`: check `base == current_staging`
   - `source == "production"`: check `base == current_production`
4. **Update .ref files** - scan edition's path files, append edition-id to each object's .ref
5. **Update staging pointer** - write `.staging.json`
6. **Cleanup** - remove `.pending/{edition}.json`
7. **Release lock**

Step 4 can be slow for large editions. The lock must be renewed periodically.

If lock expires during operation → `lockExpired` thrown, edition NOT staged (safe to retry).

### deploy() Operation

Simple but can still fail with `lockExpired` if S3 is slow:

1. **Acquire lock**
2. **Copy** `.staging.json` → `.production.json`
3. **Release lock**

### gc() Operation

**Note:** Currently implemented as dry-run only. Actual deletion requires mtime support
in `StorageBackend` which is not yet implemented. Orphaned objects are counted in
`GCResult.skippedByAge` but not deleted.

```
1. Build live edition set:
   - Production + ancestry
   - Staging + ancestry
   - All pending + ancestry
   - All active working editions

2. For each object:
   a. Read .ref file (if exists)
   b. If .ref has ANY live edition-id → KEEP (fast path, skip fallback)
   c. Otherwise, run fallback scan:
      - Scan all live editions for this hash
      - If found → KEEP
      - If not found → count as orphan (DELETE when dryRun: false supported)

3. Renew lock periodically throughout
```

**Important:** `.ref` can only confirm "keep" (fast path). It cannot confirm "delete" because:
- Working/pending editions reuse objects but don't update `.ref`
- Fallback scan is required for ALL deletion decisions

The fallback scan handles:
- Objects referenced by unstaged editions (working/pending)
- Migration from deployments without .ref files
- Stale .ref entries from old staged editions

## Usage Examples

### Complete Publishing Workflow

```swift
// === EDITOR WORKFLOW ===

// 1. Start session (defaults to production mode)
let editor = try await ContentSession(storage: storage, mode: .production)

// 2. Checkout for editing (branches from staging by default)
try await editor.checkout(label: "spring-issue")
// mode: .editing("spring-issue"), edition: 10001, base: 10000, source: .staging

// 3. Create content (transactional - buffered until endEditing)
try editor.beginEditing()
try await editor.write("articles/cover.md", data: coverData)
try await editor.write("articles/feature.md", data: featureData)
try await editor.write("images/header.jpg", data: imageData)
try await editor.endEditing()  // writes objects and path files (no .ref yet)

// 4. Submit for review
try await editor.submit(message: "Spring issue content")
// mode: .submitted

// === ADMIN WORKFLOW ===

// 5. Admin reviews pending submissions
let admin = try await ContentSession(storage: storage, mode: .staging)
let pending = try await admin.listPending()
// [{edition: 10001, base: 10000, source: "staging", label: "spring-issue", ...}]

// 6. Accept into staging (validates, updates .ref files, then staging pointer)
try await admin.stage(edition: 10001)
// .ref files updated, .staging.json now points to 10001

// 7. Deploy to production
try await admin.deploy()
// .production.json now points to 10001
```

### Hotfix Workflow

```swift
// Staging has unfinished work (edition 10005, based on 10003)
// Production is at edition 10003
// Need to fix bug in production

// 1. Checkout from production (not staging)
let hotfix = try await ContentSession(storage: storage, mode: .production)
try await hotfix.checkout(label: "hotfix-123", from: .production)
// edition: 10006, base: 10003, source: .production

// 2. Apply fix
try await hotfix.write("config/settings.json", data: fixedConfig)

// 3. Submit
try await hotfix.submit(message: "Emergency config fix")
// pending: {edition: 10006, base: 10003, source: "production", ...}

// 4. Admin stages
// Conflict check: source=production, so check base 10003 == production 10003 ✓
try await admin.stage(edition: 10006)
// Staging jumps from 10005 to 10006

// 5. Deploy immediately
try await admin.deploy()

// 6. Previous staging work (10005) now needs rebase
// Editor must re-checkout from 10006 and re-apply their changes
```

### Listing Directory Contents

```swift
// Simple iteration - pagination handled internally
for try await entry in session.list(directory: "articles/") {
    print(entry)  // "draft.md", "images/", "post.md"
}
// Note: directories have trailing /, files don't

// Collect all entries into array
var entries: [String] = []
for try await entry in session.list(directory: "articles/") {
    entries.append(entry)
}
// ["draft.md", "images/", "post.md"]  ← lexicographically sorted

// Non-existent or empty directory returns empty sequence (not an error)
var empty: [String] = []
for try await entry in session.list(directory: "nonexistent/") {
    empty.append(entry)
}
// []  ← matches S3 semantics

// Process entries (same API for local and S3)
for try await entry in session.list(directory: "logs/") {
    await processEntry(entry)
}

// WARNING: First await blocks until ancestry merge completes.
// All editions are scanned before any items yield.

// Results are merged from ancestry, de-duplicated, tombstones excluded
// Only immediate children - traverse subdirectories by calling list() on each
```

### Using stat() for Metadata

```swift
// stat() always returns FileStat for valid paths - check status field
let stat = try await session.stat("articles/draft.md")

switch stat.status {
case .exists:
    print("File exists in edition \(stat.resolvedFrom)")
    print("Hash: \(stat.hash!), Size: \(stat.size!) bytes")

case .deleted:
    print("File was deleted in edition \(stat.resolvedFrom)")
    // Use discard() to restore from ancestor

case .notFound:
    print("File never existed in ancestry")
}

// Contrast with read() which throws for both deleted and notFound:
do {
    let data = try await session.read("articles/draft.md")
} catch ContentError.notFound(let path) {
    // Could be deleted OR never existed - use stat() to distinguish
}
```

### Handling Lock Expiration

```swift
// stage() can throw lockExpired if it takes too long
do {
    try await admin.stage(edition: 10001)
} catch ContentError.lockExpired {
    // Lock was stolen - another process may have staged something
    // Safe to retry, but re-check for conflicts first
    let pending = try await admin.listPending()
    if pending.contains(where: { $0.edition == 10001 }) {
        // Still pending, retry
        try await admin.stage(edition: 10001)
    } else {
        // Already staged by someone else, or rejected
    }
} catch ContentError.lockTimeout {
    // Could not acquire lock - try again later
}
```

### Delete and Discard

```swift
// File exists from ancestor edition
let stat1 = try await session.stat("articles/old.md")
// stat1.status == .exists, resolvedFrom: 10000

// Delete creates tombstone (buffered)
try session.beginEditing()
try await session.delete("articles/old.md")

// Check pending changes before commit
print(session.pendingChanges)
// [PendingChange(path: "articles/old.md", action: .delete)]

// Can rollback before endEditing
try session.rollback()
// Delete discarded, file still visible

// Or commit the delete
try session.beginEditing()
try await session.delete("articles/old.md")
try await session.endEditing()

// Now stat shows deleted
let stat2 = try await session.stat("articles/old.md")
// stat2.status == .deleted, resolvedFrom: 10001 (current edition)

// read() throws notFound for tombstoned files
do {
    _ = try await session.read("articles/old.md")
} catch ContentError.notFound {
    // Expected - tombstoned files are not readable
}

// Discard removes tombstone from current edition
try await session.discard("articles/old.md")

// File visible again through ancestry
let stat3 = try await session.stat("articles/old.md")
// stat3.status == .exists, resolvedFrom: 10000
```

## Implementation Notes

### Hash Computation

```swift
import CryptoKit

func computeHash(_ data: Data) -> String {
    let digest = SHA256.hash(data: data)
    return digest.map { String(format: "%02x", $0) }.joined()
}

func shardPath(hash: String) -> String {
    let shard = String(hash.prefix(2))
    return "objects/\(shard)/\(hash).dat"
}

// Empty file hash (useful constant)
let emptyFileHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
```

### Path File Format

Edition path files contain plain text:
```
sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
```
or
```
deleted
```

### .ref File Format

Each `objects/{shard}/{hash}.ref` contains edition-ids, one per line:
```
10001
10003
10007
```

**Updated during `stage()` only** (not during `endEditing()`). This ensures:
- Single writer (admin with lock) → no race conditions
- Only staged editions tracked → cleaner semantics
- GC fallback handles objects without .ref

### Path Normalization

1. Trim leading/trailing whitespace
2. Remove leading slashes
3. Remove trailing slashes
4. Collapse consecutive slashes to single slash
5. Reject if empty, contains `..`, or any component starts with `.`

### Lock Implementation with Lease

Lock file `.lock` contains JSON:
```json
{
  "owner": "uuid-or-process-id",
  "acquiredAt": "2025-01-15T10:30:00Z",
  "expiresAt": "2025-01-15T10:31:00Z"
}
```

**Lease duration recommendations:**
- Local filesystem: 30s
- S3: 60s+ (account for network latency)
- Renew every 15-20s for long operations

Local filesystem:
```swift
class FileLock: LockHandle {
    let path: URL
    let owner: String
    var expiresAt: Date

    init(path: URL, timeout: TimeInterval, leaseDuration: TimeInterval = 60) async throws {
        // 1. Generate unique owner ID
        // 2. Loop until timeout:
        //    a. Try create file exclusively
        //    b. If exists, read and check expiresAt
        //       - If expired: delete stale lock, retry
        //       - If not expired: sleep, retry
        // 3. Write lock JSON with owner + expiresAt
    }

    func renew(duration: TimeInterval) async throws {
        // 1. Read current lock
        // 2. Verify owner matches
        // 3. If owner mismatch or file gone: throw lockExpired
        // 4. Update expiresAt, write back
    }

    func release() async throws {
        // 1. Read current lock
        // 2. Verify owner matches
        // 3. If owner mismatch: throw lockExpired (someone stole it)
        // 4. Delete file
    }
}
```

S3:
```swift
class S3Lock: LockHandle {
    // Use conditional PUT with If-None-Match: * for create
    // Use conditional PUT with ETag for renew
    // Use conditional DELETE with ETag for release
    // Or use DynamoDB with TTL for distributed locking
}
```

### Concurrency Summary

| Operation | Lock Required | Can Throw lockExpired |
|-----------|---------------|----------------------|
| checkout | No | No |
| read | No | No |
| stat | No | No |
| write | No | No |
| delete | No | No |
| discard | No | No |
| submit | No | No |
| stage | Yes | Yes (long operation) |
| deploy | Yes | Yes (if S3 slow) |
| setStagingPointer | Yes | Yes (if S3 slow) |
| reject | No | No |
| flatten | Yes | Yes (long operation) |
| gc | Yes | Yes (long operation) |
