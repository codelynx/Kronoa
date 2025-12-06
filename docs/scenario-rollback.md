# Rollback and Hotfix Scenarios

This document covers emergency recovery workflows: rolling back a bad deployment and applying hotfixes.

> **Implementation status:** `setStagingPointer(to:)` and `ContentError.editionNotFound` are **design-only** right now. They are documented for planning but not yet implemented in code.

## Scenario 1: Rollback After Bad Deploy

Production deployed edition 10005, but it has a critical bug. Need to revert to previous edition 10003.

### Current State

```
contents/
├── .production.json     → {"edition": 10005}   ← BAD
├── .staging.json        → {"edition": 10005}
├── editions/
│   ├── .head            → 10005
│   ├── 10003/           → last known good
│   ├── 10004/
│   └── 10005/           → buggy
└── .lock                → (not held)
```

### Step 1: Identify the Good Edition

**Client Code**
```swift
// Check production history or known good edition
let admin = try await ContentSession(storage: storage, mode: .production)

// Read a file from the bad edition to confirm the bug
let buggyContent = try await admin.read(path: "config/settings.json")
// Yep, it's broken
```

### Step 2: Roll Back Staging Pointer

Rollback is a direct pointer update - not a `stage()` call. The `stage()` API requires a `.pending/{edition}.json` file and is designed for new submissions. For rollback, we update the staging pointer directly to a previously-staged edition.

> **Safety warning:** `setStagingPointer` only checks that `editions/{id}/` exists. It does **not** verify the edition was previously staged. Pointing to a never-staged edition would leave its objects without `.ref` entries, making them GC candidates after the grace period. Use only with editions that were already staged.

**Client Code**
```swift
// Direct pointer update to previously-staged edition
try await admin.setStagingPointer(to: 10003)
```

**Behind the Scenes**

1. Acquire `.lock`
2. Validate edition 10003 exists
3. Update `.staging.json` → `{"edition": 10003}`
4. Release `.lock`

```
contents/
├── .production.json     → {"edition": 10005}   ← still bad
├── .staging.json        → {"edition": 10003}   ← rolled back
```

> **Note:** `.ref` files for edition 10003's objects already exist from when it was originally staged. No `.ref` updates needed for rollback.

### Step 3: Deploy the Rollback

**Client Code**
```swift
try await admin.deploy()
```

**Behind the Scenes**

```
contents/
├── .production.json     → {"edition": 10003}   ← restored!
├── .staging.json        → {"edition": 10003}
```

Production now serves the known good edition.

### Step 4: Investigate and Fix

The buggy editions (10004, 10005) remain in storage for investigation. They can be:
- Analyzed to find the bug
- Used as reference when creating a fix
- Eventually cleaned up by GC once no longer referenced

---

## Scenario 2: Hotfix on Top of Rollback

After rolling back to 10003, you need to apply a targeted fix without re-introducing bugs from 10004/10005.

### Current State (After Rollback)

```
contents/
├── .production.json     → {"edition": 10003}
├── .staging.json        → {"edition": 10003}
├── editions/
│   ├── .head            → 10005
│   ├── 10003/           → production (rolled back)
│   ├── 10004/           → buggy, orphaned
│   └── 10005/           → buggy, orphaned
└── .lock                → (not held)
```

### Step 1: Checkout from Production

**Client Code**
```swift
let hotfix = try await ContentSession(storage: storage, mode: .production)
try await hotfix.checkout(label: "hotfix-critical", from: .production)
// hotfix.editionId == 10006 (next from .head)
// hotfix.baseEditionId == 10003 (the rolled-back production)
```

**Behind the Scenes**
```
contents/
├── .hotfix-critical.json → {"edition": 10006, "base": 10003, "source": "production"}
├── editions/
│   ├── .head            → 10006
│   └── 10006/
│       └── .origin      → "10003"
└── .lock                → (not held)
```

### Step 2: Apply Minimal Fix

**Client Code**
```swift
// Read the problematic file
let config = try await hotfix.read(path: "config/settings.json")

// Apply targeted fix (NOT the changes from 10004/10005)
let fixed = applyMinimalFix(config)
try await hotfix.write(path: "config/settings.json", data: fixed)

// Submit
try await hotfix.submit(message: "Critical hotfix for config issue")
```

**Behind the Scenes** (after submit)
```
contents/
├── .pending/
│   └── 10006.json       → {"edition": 10006, "base": 10003, "source": "production", ...}
├── editions/
│   └── 10006/
│       ├── .origin              → "10003"
│       └── config/settings.json → "sha256:{hash}"
└── .hotfix-critical.json        → (deleted)
```

### Step 3: Fast-Track Deploy

**Client Code**
```swift
try await admin.stage(edition: 10006)
try await admin.deploy()
```

**Behind the Scenes**

`stage(10006)` validates:
- `source: "production"` → check base (10003) matches production (10003) ✓
- Updates `.ref` files for 10006's objects
- Removes `.pending/10006.json`

```
contents/
├── .production.json     → {"edition": 10006}   ← hotfix live
├── .staging.json        → {"edition": 10006}
├── .pending/            → (empty)
├── editions/
│   ├── .head            → 10006
│   ├── 10003/
│   ├── 10004/           → orphaned (was buggy)
│   ├── 10005/           → orphaned (was buggy)
│   └── 10006/
│       └── .origin      → "10003"
└── .lock                → (not held)
```

---

## Scenario 3: Recovering Pending Work After Rollback

This is an alternative scenario (not a continuation of Scenarios 1-2). Before rolling back, there was pending work based on the buggy staging. After rollback, this work is orphaned.

### The Problem

Suppose before the rollback:
- Production: 10003, Staging: 10005 (buggy)
- Editor submitted edition 10006 based on staging (10005)

After rollback:
```
contents/
├── .production.json     → {"edition": 10003}   ← rolled back
├── .staging.json        → {"edition": 10003}
├── .pending/
│   └── 10006.json       → {"edition": 10006, "base": 10005, "source": "staging"}
├── editions/
│   ├── .head            → 10006
│   ├── 10003/
│   ├── 10005/           → buggy, no longer staging
│   └── 10006/           → pending, based on buggy 10005
└── .lock                → (not held)
```

Edition 10006 was based on 10005 (buggy). Staging is now 10003. If we try to stage 10006:

```swift
try await admin.stage(edition: 10006)
// Throws: ContentError.conflictDetected(base: 10005, current: 10003, source: .staging)
```

### Recovery Options

**Option A: Rebase onto new base**

> **Note:** The current API doesn't provide direct read access to arbitrary editions.
> To recover content from an orphaned pending edition, the application would need to:
> 1. Read `.pending/10006.json` to get edition metadata
> 2. List files in `editions/10006/` via storage backend
> 3. Resolve each path file to its object hash
> 4. Read object content from `objects/{shard}/{hash}.dat`
>
> A future `ContentSession(storage:, editionId:)` initializer could simplify this.

The helpers below (`listEditionPaths`, `readFromEdition`) are **app-level async functions not provided by Kronoa** - they wrap direct `StorageBackend` calls to implement the steps above.

```swift
// Checkout from current staging (10003)
let rebased = try await ContentSession(storage: storage, mode: .staging)
try await rebased.checkout(label: "feature-rebased", from: .staging)
// rebased.editionId == 10007, based on 10003

// Application-level: recover content from orphaned edition 10006
// (requires direct storage access - see note above)
let pathsInOldEdition = try await listEditionPaths(storage, edition: 10006)
let pathsInBase = try await listEditionPaths(storage, edition: 10003)
let changedPaths = pathsInOldEdition.subtracting(pathsInBase)

for path in changedPaths {
    let data = try await readFromEdition(storage, edition: 10006, path: path)
    try await rebased.write(path: path, data: data)
}

try await rebased.submit(message: "Rebased feature work onto stable base")
```

**Option B: Discard and restart**
```swift
// If the pending work is tainted by buggy code, reject it
try await admin.reject(edition: 10006, reason: "Based on buggy edition, will restart")

// Start fresh from stable base
let fresh = try await ContentSession(storage: storage, mode: .staging)
try await fresh.checkout(label: "feature-v2", from: .staging)
// fresh.editionId == 10007, based on 10003
// Implement the feature cleanly
```

---

## Summary: Rollback Decision Tree

```
Production is broken!
        │
        ▼
┌─────────────────────────┐
│ Identify last good      │
│ edition (e.g., 10003)   │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ admin.setStagingPointer │
│ (to: good edition)      │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ admin.deploy()          │
│ Production restored     │
└───────────┬─────────────┘
            │
            ▼
    ┌───────┴───────┐
    │               │
    ▼               ▼
Fix needed?     Investigate
    │           orphaned
    ▼           editions
Hotfix from
production
(see Scenario 2)
```

## Key Points

| Concept | Behavior |
|---------|----------|
| Rollback | Use `setStagingPointer(to:)` with a previously-staged edition, then `deploy()` |
| Orphaned editions | Editions based on rolled-back versions remain for analysis |
| Pending work conflict | `stage()` rejects editions with outdated base |
| Recovery | Rebase good changes onto stable base, or discard and restart |
| Hotfix after rollback | Checkout from production, fix, fast-track deploy |
