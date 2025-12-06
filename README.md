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

## CLI

Kronoa includes a command-line interface for content management:

```bash
# Build and install
swift build -c release
cp .build/release/kronoa /usr/local/bin/

# Configure storage
kronoa config set storage s3://my-bucket/content
# or for local development:
kronoa config set storage ./local-storage

# Check status
kronoa status

# Start editing
kronoa checkout my-feature

# Make changes
echo "# Hello World" | kronoa write kr:articles/hello.md
kronoa ls kr:articles/
kronoa cat kr:articles/hello.md

# Submit for review
kronoa submit "Add hello world article"

# Admin: review and deploy
kronoa pending
kronoa stage 10001
kronoa deploy

# Clear session
kronoa done
```

### CLI Commands

| Category | Commands |
|----------|----------|
| Session | `status`, `done`, `config` |
| Navigation | `pwd`, `cd` |
| File Ops | `ls`, `cat`, `write`, `cp`, `rm`, `stat` |
| Editor | `checkout`, `begin`, `commit`, `rollback`, `discard`, `submit` |
| Admin | `pending`, `stage`, `reject`, `rejected`, `deploy`, `admin-rollback` |
| Maintenance | `flatten`, `gc` |

See [CLI User Guide](docs/cli-guide.md) for details.

## Documentation

- [Architecture](docs/architecture.md) - system design, storage layout, GC algorithm
- [API Design](docs/api-design.md) - Swift API signatures and data types
- [Programming Guide](docs/programming-guide.md) - practical guide for app developers
- [CLI Design](docs/cli-design.md) - CLI reference
- [CLI User Guide](docs/cli-guide.md) - user-focused CLI guide
- [Scenario Walkthrough](docs/scenario-walkthrough.md) - step-by-step workflow examples
- [Rollback Scenarios](docs/scenario-rollback.md) - emergency recovery workflows

## License

MIT License - see [LICENSE](LICENSE) for details.
