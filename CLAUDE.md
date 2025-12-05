# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Kronoa is a version-controlled content management system (CMS) framework in Swift designed for publishing workflows (newspaper/magazine style). It provides Git-like semantics over content-addressable storage, targeting AWS S3 with local filesystem as development stub.

**Status:** Design phase - architecture and API documented, implementation pending.

## Architecture

### Core Concepts

- **Content-Addressable Storage**: Files stored by SHA256 hash in `objects/{2-hex-shard}/{hash}.dat`
- **Edition Model**: Immutable snapshots with numeric IDs (starting 10000), ancestry via `.origin` files
- **Session Modes**: Production (read-only), Staging (read-only), Editing (read-write)
- **PR-like Workflow**: checkout → edit → submit → stage → deploy

### Storage Layout

```
contents/
├── .production.json, .staging.json  # Edition pointers
├── .pending/                        # Submitted editions awaiting review
├── .lock                            # Lease-based lock for stage/deploy
├── editions/{id}/                   # Path-to-hash mappings
│   ├── .origin                      # Parent edition
│   └── {path}                       # "sha256:{hash}" or "deleted"
└── objects/{shard}/
    ├── {hash}.dat                   # File content
    └── {hash}.ref                   # Edition IDs referencing this object
```

### Key Design Decisions

- **Tombstones**: Use explicit `deleted` marker (not empty files) to support zero-byte content
- **Deferred Writes**: Buffered until `endEditing()` for transactional semantics
- **.ref Updates**: Only during `stage()` (single writer with lock), not during editing
- **GC**: `.ref` can only confirm "keep" (fast path); fallback scan required for ALL deletions
- **No rename/move**: S3 limitation; use `copy()` + `delete()` pattern
- **No directory operations**: S3 uses key prefixes, not real directories

## Documentation

- `docs/architecture.md` - System design, storage layout, lifecycle, GC algorithm
- `docs/api-design.md` - Swift API signatures, data types, usage examples

## Development Guidelines

- When in design phase: focus on "what" not "how"
- When implementing: focus on "how"
- Simple first, no frills
- Think about how code should work, not just how to fix bugs
