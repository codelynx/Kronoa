# Kronoa: Content Management System Architecture

## Overview

Kronoa is a version-controlled content management system designed for newspaper/magazine-style publishing workflows. It provides Git-like semantics over a content-addressable storage backend, targeting AWS S3 (with local filesystem as development stub).

## Core Concepts

### Content-Addressable Storage

All file contents are stored by their SHA256 hash, enabling:
- Deduplication across editions
- Integrity verification
- Efficient storage for large binary assets

### Edition Model

Editions are immutable snapshots identified by numeric IDs (starting at 10000). Each edition:
- Contains path-to-hash mappings (not actual file contents)
- References a parent edition via `.origin` file
- Supports non-destructive branching

### Session Modes

| Mode | Read | Write | Use Case |
|------|------|-------|----------|
| Production | Yes | No | Live content serving |
| Staging | Yes | No | Review before publish |
| Editing | Yes | Yes | Active content creation |

## Storage Layout

```
root/
└── contents/
    ├── .production.json      # Points to production edition-id
    ├── .staging.json         # Points to staging edition-id
    ├── .pending/             # Submitted editions awaiting review
    │   └── {edition-id}.json # Pending submission metadata
    ├── .rejected/            # Rejected submissions
    │   └── {edition-id}.json # Rejection record (edition, reason, rejectedAt)
    ├── .lock                 # Lock file for stage/deploy (contains lease info)
    │
    ├── editions/
    │   ├── .head             # Latest edition-id (atomic counter)
    │   └── {edition-id}/
    │       ├── .origin       # Parent edition-id (if branched)
    │       ├── .flattened    # Optional: stop ancestry traversal
    │       └── {path/to/file}  # Contains: "sha256:{hash}" or "deleted"
    │
    └── objects/
        └── {2-hex-shard}/
            ├── {sha256}.dat  # Actual file content
            ├── {sha256}.ref  # Staged edition-ids referencing this object
            └── {sha256}.info # Metadata/attributes (future)
```

### Path File Format

Files in `editions/{id}/{path}` contain one of:
- `sha256:{64-char-hash}` - references content in objects
- `deleted` - tombstone marker (file was deleted)

This allows zero-byte files to be stored (hash of empty content).

## Lifecycle

### Initial State (Genesis Bootstrap)

```
editions/.head = 10000
editions/10000/.flattened = (empty file)
.production.json = { "edition": 10000 }
.staging.json = { "edition": 10000 }
```

The genesis edition (10000) must have a `.flattened` marker to indicate it has no ancestors. This is required for edition existence validation. No content exists initially; all reads return "not found".

### Checkout (Start Editing)

```swift
// Default: branch from staging
session.checkout(label: "draft-1")

// Hotfix: branch from production
session.checkout(label: "hotfix-1", from: .production)
```

1. Atomic increment `.head` → get 10001
2. Determine base edition and source type (staging or production, per `from` parameter)
3. Create `editions/10001/.origin` containing base edition
4. Create `.draft-1.json`:
   ```json
   {
     "edition": 10001,
     "base": 10000,
     "source": "staging"
   }
   ```
5. Session enters **Editing** mode

No lock required - each editor gets unique edition-id.

### Transactional Editing

Writes are **deferred** until commit. This ensures all-or-nothing semantics.

```swift
try session.beginEditing()
// Changes buffered in memory
try await session.write("a.txt", data: dataA)
try await session.write("b.txt", data: dataB)
try await session.endEditing()  // Writes to storage here
```

**What happens during transaction:**
- `write()`: Buffers `(path, data)` in memory, computes hash
- `delete()`: Buffers `(path, tombstone)` in memory
- `endEditing()`:
  1. Write all objects to `objects/{shard}/{hash}.dat` (if not exists)
  2. Write all path files to `editions/{id}/{path}`
  3. Clear buffer
- `rollback()`: Clear buffer, discard changes

**Note:** `.ref` files are NOT updated during `endEditing()`. They are updated later during `stage()` when the edition is committed to staging. This ensures:
- No race conditions on `.ref` writes (single writer with lock)
- Abandoned/partial editions don't pollute `.ref` files
- GC can safely ignore unstaged editions

**Failure handling:**
- If `endEditing()` fails mid-write: edition may have partial state
- Partial editions are harmless - not referenced by staging/production
- Editor can retry or abandon (edition becomes orphan, cleaned by GC)

For single-file operations, auto-transaction wraps each call:
```swift
try await session.write("file.txt", data: data)  // implicit begin/end
```

### Read Operation (with Ancestry)

```swift
let data = try session.read("hello/world.txt")
```

1. Validate path (throws `invalidPath` if malformed)
2. Check `editions/10001/hello/world.txt`
3. If contains `sha256:{hash}`: fetch `objects/{shard}/{hash}.dat`
4. If contains `deleted`: throw `notFound` error
5. If file not found: read `.origin` → 10000, recurse
6. Stop recursion if `.flattened` exists or no `.origin`
7. If exhausted ancestry: throw `notFound` error

### List Operation (Directory Listing)

```swift
// Simple iteration - client doesn't manage pagination
for try await entry in session.list(directory: "articles/") {
    print(entry)  // "draft.md", "images/", "post.md"
}

// Collect all entries into array
var entries: [String] = []
for try await entry in session.list(directory: "articles/") {
    entries.append(entry)
}
// ["draft.md", "images/", "post.md"]
```

Returns an `AsyncThrowingStream<String, Error>` that yields immediate children only. Pagination is handled internally - client code is identical for local filesystem and S3.

#### Algorithm

1. **For each edition in ancestry chain** (current → parent → ... → flattened/root):
   - List entries with prefix `editions/{id}/{directory}` using delimiter `/`
   - S3: `LIST ?prefix=editions/{id}/articles/&delimiter=/`
   - Local: `readdir(editions/{id}/articles/)`

2. **Merge results**:
   - Collect all paths across editions
   - Child edition entries override parent (most recent wins)
   - **Name-first shadowing**: once a name is seen, older entries are fully ignored (hash vs tombstone doesn't matter)
   - Exclude tombstones (`deleted` entries) from final result

3. **Sort and yield**:
   - **Lexicographic order** (deterministic)
   - Files: `"post.md"`, Subdirectories: `"images/"` (trailing slash)
   - Yield entries one at a time via AsyncSequence

#### Subdirectory Visibility

Subdirectories appear only if at least one descendant survives the merge:
- `images/` appears because `articles/images/a.jpg` exists (not tombstoned)
- To hide a subdirectory, every file under that prefix must be tombstoned
- There is no directory-level tombstone - directories are emergent from file prefixes

#### Empty and Non-Existent Directories

- `list("foo/")` where `foo/` has no entries → empty sequence
- `list("foo/")` where `foo/` never existed → empty sequence
- **No `notFound` error** for directories - matches S3 semantics (prefix with no keys = empty result)
- `invalidPath` only thrown for malformed paths (contains `..`, starts with `.`, etc.)

This differs from file operations: `read("foo/bar.txt")` throws `notFound` if file doesn't exist, but `list("foo/")` returns empty sequence if directory is empty/missing.

#### Example with Ancestry

```
editions/10002/articles/new.md       → sha256:aaa   (current)
editions/10002/articles/old.md       → deleted      (tombstone)
editions/10002/articles/images/a.jpg → sha256:eee   (current)
editions/10001/articles/post.md      → sha256:bbb   (parent)
editions/10001/articles/old.md       → sha256:ccc   (overridden by tombstone)
editions/10000/articles/archive.md   → sha256:ddd   (grandparent)

list("articles/")
  → yields: "archive.md", "images/", "new.md", "post.md"
     (old.md excluded - deleted in 10002)
     (images/ shown as subdirectory - traverse with list("articles/images/"))
```

#### Performance Considerations

**Cost:** `O(entries_in_dir × ancestry_depth)` per directory - all editions must be scanned and merged before yielding.

**Why full scan is required:**
- Ancestry merge needs all entries to determine overrides and tombstones
- Cannot yield entries until merge is complete (child edition may delete parent's entry)
- This is inherent to the ancestry model, not a pagination limitation

**Backend differences (hidden from client):**
- **Local FS**: `readdir()` returns all entries at once → sort → yield
- **S3**: Multiple LIST calls with continuation tokens → merge → sort → yield

**Recommendations:**
- Flatten editions periodically to reduce ancestry depth
- Keep directories reasonably sized (hundreds, not millions)
- Only immediate children returned - traverse subdirectories explicitly

### Delete Operation

```swift
try session.delete("hello/world.txt")
```

Buffers tombstone; on `endEditing()`, writes `deleted` to `editions/10001/hello/world.txt`

### Copy Operation

```swift
try session.copy(from: "articles/template.md", to: "articles/new-post.md")
```

1. Resolve source path to hash (through ancestry if needed)
2. Buffer copy operation: `(destPath, sourceHash)`
3. On `endEditing()`: write `sha256:{sourceHash}` to `editions/10001/articles/new-post.md`

**Key benefit:** No data transfer. Since objects are content-addressed, copying just creates a new path pointing to the same hash. Works efficiently for both local filesystem and S3.

### Discard (Undo Local Change)

```swift
try session.discard("hello/world.txt")
```

- If in transaction: remove from buffer
- If already committed to edition: remove `editions/{current}/hello/world.txt`
- File now resolves through ancestry again (restoring previous state)

### Unsupported Operations

| Operation | Why Not Supported |
|-----------|-------------------|
| Rename | S3 has no atomic rename; use `copy()` + `delete()` |
| Move | Same as rename; use `copy()` + `delete()` |
| Directory rename | S3 has no directories; must copy+delete each file |
| Directory delete | Must delete each file individually |
| Directory copy | Must copy each file individually |

**S3 directory model:** S3 stores objects by key (full path). "Directories" are just common prefixes - they don't exist as entities. Operations like `rename("foo/", "bar/")` would require listing all `foo/*` keys, copying each to `bar/*`, then deleting originals. This is expensive, non-atomic, and error-prone. Clients should handle such operations explicitly.

## Publishing Workflow (PR-like Model)

### Roles (Application-Level Concept)

The storage layer doesn't enforce roles, but client applications typically have:
- **Editor**: Can checkout, edit, submit
- **Admin**: Can stage (accept), deploy

### Submit (Editor Action)

```swift
try session.submit(message: "Added new article")
```

1. Validate session is in editing mode
2. Flush any pending transaction (`endEditing()` if needed)
3. Create `.pending/{edition-id}.json` with metadata:
   ```json
   {
     "edition": 10001,
     "base": 10000,
     "source": "staging",
     "label": "draft-1",
     "message": "Added new article",
     "submittedAt": "2025-01-15T10:30:00Z"
   }
   ```
4. Delete `.{working-label}.json`
5. Session becomes read-only (submitted)

Like a pull request - edition is proposed but not yet staged.

### Stage (Admin Action) - Requires Lock

```swift
try admin.stage(edition: 10001)
```

1. **Acquire lock** on `.lock` (with lease, recommend 60s+ for S3)
2. **Validate pending**: Read `.pending/10001.json`
   - If not found: release lock, throw `pendingNotFound`
   - If JSON invalid: release lock, throw `pendingCorrupt`
3. **Conflict check based on source type**:
   - If `source == "staging"`: check `pending.base == current_staging`
   - If `source == "production"`: check `pending.base == current_production`
   - If mismatch: release lock, throw `conflictDetected`
4. **Update .ref files** for all objects in this edition:
   - Scan `editions/10001/` for all path files
   - For each `sha256:{hash}`, append `10001` to `objects/{shard}/{hash}.ref`
   - Renew lock periodically during this step
5. Update `.staging.json` to `{ "edition": 10001 }`
6. Remove `.pending/10001.json`
7. **Release lock**

**Why .ref updates happen here:**
- Single writer (admin with lock) → no race conditions
- Only staged editions get .ref entries → cleaner GC
- If .ref write fails, can retry (lock still held)
- If lock expires mid-operation → throw `lockExpired`, edition not staged

If base doesn't match, editor must:
- Checkout fresh from the appropriate source
- Re-apply changes (manual merge)
- Submit again

### Deploy (Admin Action) - Requires Lock

```swift
try admin.deploy()
```

1. **Acquire lock** on `.lock` (with lease)
2. Copy `.staging.json` → `.production.json`
3. **Release lock**

**Note:** Deploy is a fast operation (single file copy), but can still throw `lockExpired` if S3 operations are slow or clock skew occurs.

### Rollback (Admin Action) - Requires Lock

Rollback reverts staging (and then production via deploy) to a previously-staged edition.

```swift
try admin.setStagingPointer(to: 10003)  // known good edition
try admin.deploy()
```

1. **Acquire lock** on `.lock` (with lease)
2. Validate edition exists (`editions/10003/` directory)
3. Update `.staging.json` to `{ "edition": 10003 }`
4. **Release lock**

**Key differences from `stage()`:**
- No `.pending/{edition}.json` required
- No base/source conflict check
- No `.ref` file updates (edition was already staged before, so `.ref` files exist)

**Use case:** Production has a buggy edition (e.g., 10005). Admin identifies last known good edition (10003), sets staging pointer to 10003, then deploys. See `docs/scenario-rollback.md` for detailed scenarios.

**Validation:** Only checks that `editions/{id}/` directory exists. Does NOT verify the edition was previously staged - this is admin responsibility. Pointing to a never-staged edition would leave its objects without `.ref` entries, making them GC candidates after grace period.

**Safety note:** Only use for editions that were previously staged. Their objects are already tracked in `.ref` files from the original staging operation.

### Lock Implementation with Lease

Lock file `.lock` contains JSON:
```json
{
  "owner": "process-id-or-uuid",
  "acquiredAt": "2025-01-15T10:30:00Z",
  "expiresAt": "2025-01-15T10:31:00Z"
}
```

**Lease duration recommendations:**
- Local filesystem: 30s (fast operations)
- S3: 60s+ (account for network latency, retries)
- Long operations (stage with many files, GC, flatten): renew every 15-20s

**Acquire lock:**
1. Try to create `.lock` (fail if exists)
2. If exists, read and check `expiresAt`
   - If expired: delete stale lock, retry create
   - If not expired: wait and retry (up to timeout)
3. Write lock with `expiresAt = now + leaseDuration`

**Renew lock:**
1. Verify we still own the lock (check `owner`)
2. Update `expiresAt = now + duration`
3. If verification fails: throw `lockExpired`

**Release lock:**
1. Verify we still own the lock (check `owner`)
2. Delete `.lock`
3. If verification fails: throw `lockExpired` (lock was stolen after expiry)

**Stale lock recovery:**
- Any process can delete expired locks
- No operator intervention needed for crashed processes
- Lease duration should exceed expected max operation time

For S3:
- Use conditional PUT with `If-None-Match: *` for create
- Use conditional PUT with ETag for renew
- Use conditional DELETE with ETag for release
- Or use DynamoDB with TTL for distributed locking

## Branching

### From Staging (Default)

```swift
session.checkout(label: "draft-2")
```

Edition's `.origin` points to current staging edition.
Pending metadata includes `"source": "staging"`.

### From Production (Hotfix)

```swift
session.checkout(label: "hotfix-1", from: .production)
```

Edition's `.origin` points to current production edition.
Pending metadata includes `"source": "production"`.

Use case: Emergency fix when staging has unfinished work.

## Flattening (Optimization)

For long ancestry chains, admin can flatten:

```swift
try admin.flatten(edition: 10001)
```

1. Acquire lock (long operation, must renew periodically)
2. Resolve all paths to their final state
3. Write all mappings to edition (copying from ancestors)
4. Create `.flattened` marker
5. Release lock

Reads stop traversing at flattened editions.

## Garbage Collection

Uses `.ref` files to track which staged editions reference each object.

### .ref File Format

Each `objects/{shard}/{hash}.ref` contains edition-ids, one per line:
```
10001
10003
10007
```

**Important:** `.ref` files are only updated during `stage()`, not during `endEditing()`. This means:
- Only staged (committed) editions appear in `.ref`
- Working/abandoned editions are not tracked in `.ref`
- GC must handle objects without `.ref` files (migration, edge cases)

### GC Algorithm

```
1. Identify live editions:
   - Production edition and its ancestry
   - Staging edition and its ancestry
   - All pending editions and their ancestry
   - All active working editions (.{label}.json)

2. For each object in objects/:
   a. Read .ref file (if exists)
   b. If .ref exists and ANY edition-id in .ref is live → KEEP (fast path)
   c. Otherwise, run fallback scan:
      - Scan all live editions for this hash
      - If found in any live edition → KEEP
      - If not found → candidate for deletion

3. Delete candidates:
   - Only delete objects older than grace period (default 24h)
   - Remove .dat, .ref, .info files together
   - Renew lock periodically
```

**Key insight:** `.ref` can only confirm an object is live (fast path). It cannot confirm deletion because:
- Working/pending editions reuse objects but don't update `.ref`
- `.ref` may have stale entries from old staged editions

The fallback scan is required for ALL deletion decisions.

### Migration Safety

For deployments created before `.ref` files existed:
- Objects have no `.ref` files
- GC falls back to edition scan (step 2c)
- No data loss, just slower GC
- As new editions are staged, `.ref` files are populated
- Over time, GC becomes faster as more objects have `.ref` files

**Optional:** Run a one-time migration to populate `.ref` files for existing staged editions.

### Cost Analysis

| Phase | Storage | Cost |
|-------|---------|------|
| .ref lookup | Local FS | Fast file read |
| .ref lookup | S3 | GET per object |
| Fallback scan | Local FS | Directory walk |
| Fallback scan | S3 | LIST + GET per edition path file |

**Performance notes:**
- `.ref` fast path avoids fallback scan only when a live ID is found
- Objects not in any live staged edition always require fallback scan
- This is intentional: correctness over performance

For large deployments:
- Run GC during low-traffic periods
- Use generous grace period to avoid racing with stage operations
- Consider batching fallback scans (scan edition once, check multiple objects)

### Safety

- Grace period prevents deleting recently-written objects
- Lock required to prevent race with stage/deploy
- Fallback scan ensures objects are never incorrectly deleted
- If any step fails, err on the side of keeping objects

## Conflict Scenarios

### Two Editors, Same File (Both from Staging)

1. Editor A checks out (10001, base=10000, source=staging)
2. Editor B checks out (10002, base=10000, source=staging)
3. Both edit `article.md`
4. Editor A submits → Admin stages (staging=10001)
5. Editor B submits → **Conflict**: base 10000 ≠ staging 10001
6. Editor B must re-checkout from 10001 and re-apply changes

### Hotfix While Staging Has Work

1. Staging has edition 10005 (unfinished feature)
2. Production has edition 10003
3. Bug found in production
4. Admin checks out from production: `checkout(label: "hotfix", from: .production)`
   - Gets edition 10006, base=10003, source=production
5. Fix applied, submitted
6. Admin stages 10006:
   - source=production, so check: base 10003 == production 10003 ✓
   - Staging moves from 10005 to 10006
7. Previous staging work (10005) needs rebase:
   - Editor re-checkouts from 10006, re-applies their changes

Note: Hotfix editions bypass the staging base check because they're based on production.

### Two Editors, Different Files

1. Editor A checks out (10001), edits `sports/news.md`
2. Editor B checks out (10002), edits `tech/review.md`
3. Editor A submits → staged
4. Editor B submits → **Conflict** (base mismatch, same source)
5. Editor B re-checkouts, their file doesn't exist in 10001
6. Editor B re-applies (copy from their 10002), submits again

Note: No automatic merge. Deliberate simplicity for CMS use case.

## Design Principles

1. **Non-destructive**: Writes never modify previous editions
2. **Content-addressable**: Same content = same storage
3. **Lazy resolution**: Ancestry traversal only when needed
4. **Explicit tombstones**: `deleted` marker, not empty file
5. **Backend-agnostic**: S3 and local filesystem share same semantics
6. **Transactional edits**: Deferred writes, all-or-nothing batches
7. **Explicit staging**: PR-like review before publish
8. **Conflict-aware**: Base + source tracking prevents silent overwrites
9. **Self-healing locks**: Lease-based with renewal, auto-expire on crash
10. **Safe GC**: .ref for fast lookup, fallback scan for correctness
