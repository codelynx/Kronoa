# Kronoa Programming Guide

A practical guide for client app developers using the Kronoa CMS framework.

## Quick Start

```swift
import Kronoa

// Setup storage backend
let storage = LocalFileStorage(root: "/path/to/root")
// or: let storage = S3Storage(bucket: "my-bucket", prefix: "contents")

// Create a session
let session = try await ContentSession(storage: storage, mode: .production)
```

## Session Modes

| Mode | Can Read | Can Write | Use Case |
|------|----------|-----------|----------|
| `.production` | Yes | No | Serve live content to end users |
| `.staging` | Yes | No | Preview before publish |
| `.editing(label)` | Yes | Yes | Create/modify content |
| `.submitted` | Yes | No | After submit, awaiting review |

## Basic Workflow

### 1. Checkout (Start Editing)

```swift
// Branch from staging (default)
try await session.checkout(label: "my-draft")

// Branch from production (emergency fix)
try await session.checkout(label: "hotfix-123", from: .production)
```

**What happens:** A new edition is created branching from staging (or production if `from: .production`). Your session enters editing mode.

> **Note:** The `from:` parameter determines the base edition, not the session's initial mode. You can start with `.production` mode to read production content, then checkout with default `from: .staging`.

### 2. Read Files

```swift
let data = try await session.read(path: "articles/hello.md")
```

**What happens:** Kronoa looks in your edition first, then walks up the ancestry chain until the file is found.

**Errors:**
- `notFound` - file doesn't exist (or was deleted)
- `invalidPath` - path is malformed

### 3. Write Files

```swift
// Single file (auto-commits)
try await session.write(path: "articles/new.md", data: content)

// Multiple files (batch)
try session.beginEditing()
try await session.write(path: "a.md", data: dataA)
try await session.write(path: "b.md", data: dataB)
try await session.endEditing()  // commits all at once
```

**What happens:** Content is stored by hash (deduplication). Your edition records which hash each path points to.

### 4. Delete Files

```swift
try await session.delete(path: "articles/old.md")
```

**What happens:** A tombstone marker is written. The file becomes invisible but original content remains (for ancestry).

### 5. Copy Files

```swift
try await session.copy(from: "template.md", to: "new-post.md")
```

**What happens:** No data transfer - just points the new path to the same content hash.

### 6. Discard Changes

```swift
// Undo a change to a specific file
try await session.discard(path: "articles/old.md")
```

**What happens:** Removes your edition's entry for that path. File now resolves through ancestry (restoring previous state).

### 7. Submit for Review

```swift
try await session.submit(message: "Added new article")
// session.mode is now .submitted (read-only)
```

**What happens:** Edition moves to pending queue. Your editing session ends.

## Admin Operations

Admin operations are methods on `ContentSession`. They work in any session mode - the mode only restricts content write/delete, not admin calls. Role enforcement is application-level, not framework-level.

```swift
let session = try await ContentSession(storage: storage, mode: .staging)
```

> **Best practice:** Use `.staging` mode for admin sessions. This lets you preview staged content before deploying.

### List Pending Submissions

```swift
let pending = try await session.listPending()
for p in pending {
    print("\(p.edition): \(p.message) by \(p.label)")
}
```

### Stage (Accept into Staging)

```swift
try await session.stage(edition: 10001)
```

**What happens:**
1. Validates the edition's base matches current staging/production
2. Updates staging pointer
3. Tracks objects for garbage collection

**Possible errors:**
- `conflictDetected` - base is outdated, editor must rebase
- `lockTimeout` - another admin operation in progress

### Deploy (Staging to Production)

```swift
try await session.deploy()
```

**What happens:** Production pointer is updated to match staging. Instant, no data copying.

### Reject

```swift
try await session.reject(edition: 10001, reason: "Needs revision")
```

### List Rejected Submissions

```swift
let rejections = try await session.listRejected()
for r in rejections {
    print("\(r.edition): \(r.reason)")
}
```

### Get Specific Rejection

```swift
if let rejection = try await session.getRejection(edition: 10001) {
    print("Rejected: \(rejection.reason)")
}
```

## Rollback

When production has a bad edition:

```swift
let session = try await ContentSession(storage: storage, mode: .staging)

// 1. Point staging to a known good edition
try await session.setStagingPointer(to: 10003)

// 2. Deploy the rollback
try await session.deploy()
```

**Important:** Only use `setStagingPointer` with editions that were previously staged. Their object references are already tracked.

## Emergency Fix (Branch from Production)

When you need to fix production without including staged changes:

```swift
// 1. Branch from production (not staging)
let session = try await ContentSession(storage: storage, mode: .production)
try await session.checkout(label: "hotfix-urgent", from: .production)

// 2. Apply fix
try await session.write(path: "config.json", data: fixedConfig)

// 3. Submit
try await session.submit(message: "Critical fix")

// 4. Fast-track: stage and deploy
try await session.stage(edition: session.editionId)
try await session.deploy()
```

## Checking File Status

```swift
// Simple existence check
let exists = try await session.exists(path: "articles/draft.md")

// Detailed status (distinguishes deleted vs never existed)
let stat = try await session.stat(path: "articles/draft.md")
switch stat.status {
case .exists:
    print("Size: \(stat.size!) bytes")
case .deleted:
    print("Was deleted in edition \(stat.resolvedFrom)")
case .notFound:
    print("Never existed")
}
```

## Listing Directory Contents

```swift
for try await entry in session.list(directory: "articles/") {
    if entry.hasSuffix("/") {
        print("Directory: \(entry)")
    } else {
        print("File: \(entry)")
    }
}
// Results are sorted lexicographically
// Empty/non-existent directories return empty sequence (not an error)
```

## Error Handling

```swift
do {
    try await session.write(path: "test.md", data: data)
} catch ContentError.readOnlyMode {
    // Not in editing mode
} catch ContentError.invalidPath(let path) {
    // Bad path (contains .., starts with ., etc.)
} catch ContentError.notFound(let path) {
    // File doesn't exist
} catch ContentError.conflictDetected(let base, let current, let source) {
    // Edition is outdated, need to rebase
} catch ContentError.lockTimeout {
    // Another admin operation in progress, retry later
} catch ContentError.lockExpired {
    // Lock was lost during long operation, retry
}
```

## Things to Know

### Paths
- No leading slash: `"articles/post.md"` not `"/articles/post.md"`
- No `..` allowed
- No paths starting with `.` (reserved for system)
- Trailing slash for directories in list results: `"images/"`

### No Rename/Move
S3 doesn't support atomic rename. Use copy + delete:
```swift
try await session.copy(from: "old-name.md", to: "new-name.md")
try await session.delete(path: "old-name.md")
```

### No Directory Operations
Directories don't exist as entities - they're just path prefixes. To "delete a directory", delete each file individually.

### Conflicts
If your edition's base doesn't match current staging/production when you try to stage:
1. Checkout fresh from staging
2. Re-apply your changes
3. Submit again

There's no automatic merge - this is intentional for CMS workflows.

### Transactions
- Single writes auto-commit
- Use `beginEditing()`/`endEditing()` for atomic batches
- Use `rollback()` to discard uncommitted changes

### Ancestry
Reads traverse the edition chain until the file is found. This enables:
- Efficient branching (new editions start empty)
- Non-destructive history (old content preserved)
- Fast checkout (no copying)

### Object Deduplication
Same content = same hash = stored once. Uploading the same image twice costs no extra storage.
