# Kronoa

A version-controlled content management system (CMS) framework in Swift for publishing workflows.

## Overview

Kronoa provides Git-like semantics over content-addressable storage, designed for newspaper/magazine-style publishing. It supports:

- **Immutable editions** - snapshots with ancestry tracking
- **PR-like workflow** - checkout, edit, submit, stage, deploy
- **Content deduplication** - files stored by SHA256 hash
- **Hotfix support** - branch from production for emergency fixes
- **Rollback** - instant revert to any previous edition

## Storage Backends

- **Local filesystem** - for development
- **AWS S3** - for production

## Quick Example

```swift
import Kronoa

// Setup
let storage = LocalFileStorage(root: "/path/to/content")
let session = try await ContentSession(storage: storage, mode: .staging)

// Checkout for editing (branches from staging by default)
try await session.checkout(label: "spring-issue")

// Create content
try await session.write(path: "articles/cover.md", data: coverData)
try await session.write(path: "articles/feature.md", data: featureData)

// Submit for review
try await session.submit(message: "Spring issue content")

// Stage and deploy (same session, admin ops require lock internally)
try await session.stage(edition: session.editionId)
try await session.deploy()
```

> **Note:** Admin operations (`stage`, `deploy`, `setStagingPointer`) acquire a lock internally. Role-based access control (who can call these methods) is application-level, not enforced by the framework.

## Workflow

```
Production ◄─── deploy ◄─── Staging ◄─── stage ◄─── Pending ◄─── submit ◄─── Editing
     │                          │
     │                          └── checkout (default)
     └── checkout (hotfix)
```

1. **Checkout** - create a new edition branching from staging (default) or production (for hotfixes)
2. **Edit** - read, write, delete, copy files
3. **Submit** - propose edition for review
4. **Stage** - accept into staging (validates base matches current pointer)
5. **Deploy** - publish staging to production

## Documentation

- [Architecture](docs/architecture.md) - system design, storage layout, GC algorithm
- [API Design](docs/api-design.md) - Swift API signatures and data types
- [Programming Guide](docs/programming-guide.md) - practical guide for app developers
- [Scenario Walkthrough](docs/scenario-walkthrough.md) - step-by-step workflow examples
- [Rollback Scenarios](docs/scenario-rollback.md) - emergency recovery workflows

## Status

**Design phase** - architecture and API documented, implementation in progress.

## License

[TBD]
