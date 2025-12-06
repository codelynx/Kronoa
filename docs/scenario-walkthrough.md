# Kronoa Scenario Walkthrough

This document walks through a complete publishing workflow, showing both the **client API** and **behind-the-scenes storage** at each step.

> **Note:** This walkthrough uses simplified API examples for clarity. See `api-design.md` for exact signatures.

## Initial State: Fresh Setup

### Client Code
```swift
let storage = S3Storage(bucket: "my-cms")
// Admin runs setup (one-time initialization)
```

### Behind the Scenes
```
contents/
├── .production.json     → {"edition": 10000}
├── .staging.json        → {"edition": 10000}
└── editions/
    ├── .head            → 10000   ← last allocated edition ID
    └── 10000/           → (empty, the "genesis" edition)
```

The genesis edition (10000) is created. Both production and staging point to it.

---

## Story 1: First Content Creation

### Step 1.1: Checkout for Editing

**Client Code**
```swift
let session = try await ContentSession(storage: storage, mode: .production)
try await session.checkout(label: "spring-issue", from: .staging)
// session.mode == .editing("spring-issue")
// session.editionId == 10001
// session.baseEditionId == 10000
// session.checkoutSource == .staging
```

**Behind the Scenes**
```
contents/
├── .production.json     → {"edition": 10000}
├── .staging.json        → {"edition": 10000}
├── .spring-issue.json   → {"edition": 10001, "base": 10000, "source": "staging"}
└── editions/
    ├── .head            → 10001   ← incremented
    ├── 10000/
    └── 10001/
        └── .origin      → "10000"   ← points to parent
```

A new edition 10001 is created with `.origin` pointing to 10000. The working file `.spring-issue.json` tracks this editing session.

### Step 1.2: Write Files (Batch Transaction)

**Client Code**
```swift
try session.beginEditing()
try await session.write(path: "articles/hello.md", data: "# Hello World".data)
try await session.write(path: "articles/intro.md", data: "# Introduction".data)
// Changes buffered in memory until endEditing()
```

**Behind the Scenes** (writes are buffered in memory)
```
session.buffer = [
    "articles/hello.md": Data("# Hello World"),
    "articles/intro.md": Data("# Introduction")
]
```

Nothing written to storage yet! All changes are held in the session's write buffer.

> **Note:** Single-file writes (without explicit `beginEditing()`) auto-wrap in a transaction, flushing immediately. Here we use an explicit transaction to batch multiple writes.

### Step 1.3: Flush Buffer to Storage

**Client Code**
```swift
try await session.endEditing()  // writes objects and path files
```

**Behind the Scenes** (buffer flushed to storage)
```
contents/
├── .production.json     → {"edition": 10000}
├── .staging.json        → {"edition": 10000}
├── .spring-issue.json   → {"edition": 10001, "base": 10000, "source": "staging"}
├── editions/
│   ├── .head            → 10001
│   ├── 10000/
│   └── 10001/
│       ├── .origin              → "10000"
│       ├── articles/hello.md    → "sha256:abc123..."
│       └── articles/intro.md    → "sha256:def456..."
└── objects/
    ├── ab/
    │   └── abc123...dat         → "# Hello World"
    └── de/
        └── def456...dat         → "# Introduction"
```

- Each file's content is hashed (SHA256)
- Content stored in `objects/{first-2-hex-chars}/{full-hash}.dat`
- Edition stores path → hash mapping

### Step 1.4: Read Files

**Client Code**
```swift
let hello = try await session.read(path: "articles/hello.md")
// Returns "# Hello World" from storage

let missing = try await session.read(path: "nonexistent.md")
// Throws: ContentError.notFound (checked 10001 → 10000, not found)
```

Read checks: **current edition** → **parent edition** → ... → **genesis**

### Step 1.5: Submit for Review

**Client Code**
```swift
try await session.submit(message: "Spring issue content")
// session.mode == .submitted
```

**Behind the Scenes**
```
contents/
├── .pending/
│   └── 10001.json       → {"edition": 10001, "base": 10000, "source": "staging",
│                           "label": "spring-issue", "message": "Spring issue content",
│                           "submittedAt": "2025-01-15T10:30:00Z"}
└── editions/
    └── 10001/
        └── ...          (no changes to edition itself)

(working file .spring-issue.json is deleted)
```

Edition 10001 is now in `.pending/10001.json`, awaiting review. The working file is removed, and the session becomes read-only (submitted mode).

---

## Story 2: Stage First Edition, Then Continue Editing

Before continuing with more edits, admin stages the first edition.

### Step 2.1: Stage Edition 10001

**Client Code**
```swift
let admin = try await ContentSession(storage: storage, mode: .staging)
try await admin.stage(edition: 10001)
```

**Behind the Scenes**

1. Acquire `.lock`
2. Read `.pending/10001.json` → verify `base` (10000) matches current staging (10000) ✓
3. Create `.ref` files for all objects used by 10001
4. Update `.staging.json` → 10001
5. Remove `.pending/10001.json`
6. Release `.lock`

```
contents/
├── .staging.json        → {"edition": 10001}   ← UPDATED
├── .pending/
│   └── (empty, 10001.json removed)
└── objects/
    ├── ab/abc123...ref  → "10001"              ← .ref files created
    └── de/def456...ref  → "10001"
```

Note: `.ref` files are written **before** flipping `.staging.json` to avoid a window where staging points to an edition whose refs aren't populated.

### Step 2.2: Checkout for More Edits

**Client Code**
```swift
let session2 = try await ContentSession(storage: storage, mode: .staging)
try await session2.checkout(label: "spring-issue-v2", from: .staging)
// session2.editionId == 10002
// session2.baseEditionId == 10001 (current staging, after step 2.1)
// session2.checkoutSource == .staging
```

**Behind the Scenes**
```
contents/
├── .staging.json        → {"edition": 10001}
├── .spring-issue-v2.json → {"edition": 10002, "base": 10001, "source": "staging"}
└── editions/
    ├── .head            → 10002   ← incremented
    ├── 10001/
    │   └── ...
    └── 10002/
        └── .origin      → "10001"   ← based on current staging
```

### Step 2.3: Read Existing File

**Client Code**
```swift
let intro = try await session2.read(path: "articles/intro.md")
// Returns "# Introduction"
// Lookup: 10002(miss) → 10001(hit!) → returns content from objects/
```

### Step 2.4: Update Existing File

**Client Code**
```swift
try await session2.write(path: "articles/intro.md", data: "# Introduction\n\nWelcome!".data)
```

**Behind the Scenes** (auto-flushed since single write)
```
editions/10002/articles/intro.md → "sha256:789xyz..."  (NEW hash)
objects/78/789xyz...dat          → "# Introduction\n\nWelcome!"
```

### Step 2.5: Delete a File

**Client Code**
```swift
try await session2.delete(path: "articles/hello.md")
```

**Behind the Scenes** (tombstone written)
```
editions/10002/articles/hello.md → "deleted"  (tombstone marker)
```

### Step 2.6: Add New File

**Client Code**
```swift
try await session2.write(path: "articles/guide.md", data: "# User Guide".data)
```

### Step 2.7: Discard Changes (Oops, wrong file!)

**Client Code**
```swift
// Actually, let's not delete hello.md
try await session2.discard(path: "articles/hello.md")

// Or to discard ALL buffered changes (if in transaction):
// try session2.rollback()
```

**Behind the Scenes**

Since the delete was already written to the edition (auto-flushed), discard removes `editions/10002/articles/hello.md`, so the file resolves through ancestry again (restoring previous state from 10001).

### Step 2.8: Submit

**Client Code**
```swift
try await session2.submit(message: "Added guide, updated intro")
```

**Behind the Scenes**
```
contents/
├── .staging.json        → {"edition": 10001}
├── .pending/
│   └── 10002.json       → {"edition": 10002, "base": 10001, "source": "staging", ...}
├── editions/
│   ├── 10001/
│   │   ├── articles/hello.md    → "sha256:abc123..."
│   │   └── articles/intro.md    → "sha256:def456..."
│   └── 10002/
│       ├── .origin              → "10001"
│       ├── articles/intro.md    → "sha256:789xyz..."   ← NEW hash (updated)
│       └── articles/guide.md    → "sha256:ghi012..."   ← new file
└── objects/
    ├── ab/abc123...dat          → "# Hello World"       (still exists)
    ├── ab/abc123...ref          → "10001"
    ├── de/def456...dat          → "# Introduction"      (still exists)
    ├── de/def456...ref          → "10001"
    ├── 78/789xyz...dat          → "# Introduction\n\nWelcome!"  (new, no .ref yet)
    └── gh/ghi012...dat          → "# User Guide"        (new, no .ref yet)

(working file .spring-issue-v2.json is deleted)
```

Note: `hello.md` not in 10002 means it inherits from 10001 (not deleted, since we discarded the delete).

---

## Story 3: Deploy to Production

### Step 3.1: Verify Staging (Read-Only Access)

**Client Code**
```swift
let stagingSession = try await ContentSession(storage: storage, mode: .staging)
// stagingSession.editionId == 10001

for try await entry in stagingSession.list(directory: "articles/") {
    print(entry)  // "hello.md", "intro.md"
}

// Cannot write in staging mode:
try await stagingSession.write(path: "test.md", data: data)
// Throws: ContentError.readOnlyMode
```

### Step 3.2: Deploy to Production

**Client Code**
```swift
try await admin.deploy()
```

**Behind the Scenes**

1. Acquire `.lock`
2. Copy `.staging.json` → `.production.json`
3. Release `.lock`

```
contents/
├── .production.json     → {"edition": 10001}   ← UPDATED (was 10000)
├── .staging.json        → {"edition": 10001}
├── .pending/
│   └── 10002.json       → {"edition": 10002, "base": 10001, ...}  ← still pending
```

Production now serves edition 10001. Edition 10002 is still pending review.

---

## Story 4: Hotfix (Emergency Fix to Production)

Production is live with 10001, but we found a typo!

### Step 4.1: Checkout from Production

**Client Code**
```swift
let hotfix = try await ContentSession(storage: storage, mode: .production)
try await hotfix.checkout(label: "hotfix-typo", from: .production)
// hotfix.editionId == 10003
// hotfix.baseEditionId == 10001 (current production)
// hotfix.checkoutSource == .production
```

**Behind the Scenes**
```
contents/
├── .hotfix-typo.json    → {"edition": 10003, "base": 10001, "source": "production"}
├── .pending/
│   └── 10002.json       → {"edition": 10002, "base": 10001, ...}  ← still pending
└── editions/
    ├── .head            → 10003
    ├── 10001/           → (production & staging)
    ├── 10002/           → (pending, based on 10001)
    └── 10003/
        └── .origin      → "10001"   ← branches from production
```

### Step 4.2: Apply Fix

**Client Code**
```swift
let content = try await hotfix.read(path: "articles/intro.md")
let fixed = content.replacingOccurrences(of: "Wolrd", with: "World")  // fix typo
try await hotfix.write(path: "articles/intro.md", data: fixed.data)
```

### Step 4.3: Submit, Stage, Deploy (Fast Track)

**Client Code**
```swift
try await hotfix.submit(message: "Fix typo in intro")
try await admin.stage(edition: 10003)
try await admin.deploy()
```

**Behind the Scenes**

After `submit()`:
```
contents/
├── .pending/
│   ├── 10002.json       → {"edition": 10002, "base": 10001, "source": "staging", ...}
│   └── 10003.json       → {"edition": 10003, "base": 10001, "source": "production", ...}
```

`stage(10003)` follows the same steps as any stage operation:

1. Acquire `.lock`
2. Read `.pending/10003.json` → since `source: "production"`, verify `base` (10001) matches current **production** (10001) ✓
3. Create `.ref` files for all objects used by 10003
4. Update `.staging.json` → 10003
5. Remove `.pending/10003.json`
6. Release `.lock`

After `stage()`:
```
contents/
├── .staging.json        → {"edition": 10003}   ← UPDATED
├── .pending/
│   └── 10002.json       → {"edition": 10002, "base": 10001, ...}  ← still pending
│   (10003.json removed)
└── objects/
    └── fi/fixed99...ref → "10003"              ← .ref file created
```

After `deploy()`:
```
contents/
├── .production.json     → {"edition": 10003}   ← hotfix is now live
├── .staging.json        → {"edition": 10003}
├── .pending/
│   └── 10002.json       → {"edition": 10002, "base": 10001, ...}  ← still pending
└── editions/
    ├── 10001/
    ├── 10002/
    │   └── .origin      → "10001"   ← outdated! staging moved past this
    └── 10003/
        └── .origin      → "10001"
```

### Step 4.4: The Orphaned Edition Problem

Edition 10002 is based on 10001, but staging is now at 10003.

**What happens if we try to stage 10002?**
```swift
try await admin.stage(edition: 10002)
// Throws: ContentError.conflictDetected(base: 10001, current: 10003, source: .staging)
//   - 10002's source is "staging"
//   - 10002's base is 10001
//   - Current staging is 10003
//   - base ≠ staging → conflict!
```

`stage()` **refuses** to stage 10002 because its `base` (from `.pending/10002.json`) doesn't match current staging. The `source` field determines which pointer to check: `"staging"` checks staging, `"production"` checks production. This prevents accidentally reverting the hotfix.

**Client must handle this:**
```swift
// Option A: Rebase (create new edition with changes re-applied)
let rebased = try await ContentSession(storage: storage, mode: .staging)
try await rebased.checkout(label: "spring-issue-v3", from: .staging)
// Manually re-apply 10002's changes to rebased session
// Then submit the rebased edition

// Option B: Reject and discard 10002
try await admin.reject(edition: 10002, reason: "Superseded by hotfix")
// Removes .pending/10002.json
```

---

## Summary: Storage State After All Operations

```
contents/
├── .production.json     → {"edition": 10003}
├── .staging.json        → {"edition": 10003}
├── .pending/
│   └── 10002.json       → {"edition": 10002, "base": 10001, ...}  (orphaned)
│
├── editions/
│   ├── .head            → 10003
│   ├── 10000/           → genesis (empty)
│   ├── 10001/
│   │   ├── .origin              → "10000"
│   │   ├── articles/hello.md    → "sha256:abc123..."
│   │   └── articles/intro.md    → "sha256:def456..."
│   ├── 10002/
│   │   ├── .origin              → "10001"
│   │   ├── articles/intro.md    → "sha256:789xyz..."
│   │   └── articles/guide.md    → "sha256:ghi012..."
│   └── 10003/
│       ├── .origin              → "10001"
│       └── articles/intro.md    → "sha256:fixed99..."
│
└── objects/
    ├── ab/
    │   ├── abc123...dat         → "# Hello World"
    │   └── abc123...ref         → "10001"
    ├── de/
    │   ├── def456...dat         → "# Introduction"
    │   └── def456...ref         → "10001"
    ├── 78/
    │   └── 789xyz...dat         → "# Introduction\n\nWelcome!"
    │   (no .ref - never staged)
    ├── gh/
    │   └── ghi012...dat         → "# User Guide"
    │   (no .ref - never staged)
    └── fi/
        ├── fixed99...dat        → "# Introduction\n\nWorld!"
        └── fixed99...ref        → "10003"
```

## Key Concepts Illustrated

| Concept | Where Shown |
|---------|-------------|
| Working files | Step 1.1 - `.{label}.json` tracks editing session |
| Buffered writes | Step 1.2 - writes held until endEditing() |
| Auto-transaction | Step 2.4 - single writes auto-wrap and flush immediately |
| Ancestry lookup | Step 2.3 - read traverses 10002 → 10001 |
| Tombstones | Step 2.5 - delete writes "deleted" marker |
| Discard | Step 2.7 - removes changes from edition, restores ancestry |
| Content deduplication | Same content = same hash = stored once |
| .ref for GC | Step 2.1, 4.3 - every stage() creates .ref files for that edition's objects |
| .pending with base check | Step 2.1, 4.3/4.4 - stage() validates base vs staging or production (per source) |
| Lock on stage/deploy | Step 2.1, 3.2, 4.3 - both operations acquire .lock |
| Conflict detection | Step 4.4 - stage() refuses outdated editions |
| Hotfix workflow | Story 4 - branch from production, fast-track deploy |
| Orphaned editions | Step 4.4 - edition based on outdated parent |
