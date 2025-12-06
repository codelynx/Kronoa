# Kronoa CLI User Guide

A practical guide to using the Kronoa command-line interface for content management.

## Getting Started

### Installation

Build and install the CLI:

```bash
swift build -c release
cp .build/release/kronoa /usr/local/bin/
```

### Initial Setup

Configure your storage backend:

```bash
# For local development
kronoa config set storage ./my-content

# For S3
kronoa config set storage s3://my-bucket/content-prefix
```

Verify your configuration:

```bash
kronoa status
```

## Basic Concepts

### Path Schemes

Kronoa uses `kr:` prefix to identify content paths:

| Path | Meaning |
|------|---------|
| `kr:articles/post.md` | Content in Kronoa |
| `file:./local.md` | Local file (explicit) |
| `./local.md` | Local file (implicit) |

### Session Modes

Your session operates in one of these modes:

| Mode | Description |
|------|-------------|
| `staging` | Read-only view of staged content (default) |
| `production` | Read-only view of live content |
| `editing` | Read-write mode for making changes |
| `submitted` | Read-only after submitting for review |

Check your current mode:

```bash
kronoa status
```

## Browsing Content

### Listing Files

```bash
# List current directory
kronoa ls

# List specific directory
kronoa ls kr:articles/

# Use glob patterns
kronoa ls kr:articles/*.md

# JSON output for scripting
kronoa ls --json
```

### Reading Files

```bash
# Print to terminal
kronoa cat kr:articles/welcome.md

# Save to local file
kronoa cat kr:articles/welcome.md > local-copy.md

# Read multiple files (glob)
kronoa cat kr:articles/chapter-*.md
```

### File Information

```bash
kronoa stat kr:articles/welcome.md
```

Output:
```
Path:     articles/welcome.md
Status:   exists
Edition:  10005
Hash:     a1b2c3...
Size:     1234 bytes
```

### Navigation

Set a working directory to simplify paths:

```bash
kronoa cd kr:articles/
kronoa pwd                    # Output: /articles/
kronoa ls                     # Lists articles/
kronoa cat kr:post.md         # Reads articles/post.md
```

## Making Changes

### Step 1: Checkout

Create a working edition before making changes:

```bash
kronoa checkout my-feature
```

This branches from staging. For hotfixes from production:

```bash
kronoa checkout hotfix-123 --from production
```

### Step 2: Edit Content

**Upload a file:**
```bash
kronoa cp file:./new-post.md kr:articles/new-post.md
```

**Write from stdin:**
```bash
echo "# Hello World" | kronoa write kr:articles/hello.md
cat draft.md | kronoa write kr:articles/post.md
```

**Create empty file:**
```bash
kronoa write kr:placeholder.md --empty
```

**Delete a file:**
```bash
kronoa rm kr:articles/old-post.md
```

**Copy within Kronoa:**
```bash
kronoa cp kr:templates/post.md kr:articles/new-post.md
```

### Step 3: Review Changes

List your changes:
```bash
kronoa ls kr:articles/
kronoa cat kr:articles/new-post.md
```

Undo a change before submitting:
```bash
kronoa discard kr:articles/new-post.md
```

### Step 4: Submit

Submit your changes for review:

```bash
kronoa submit "Added new blog post about feature X"
```

After submitting, your session enters "submitted" mode. Clear it when done:

```bash
kronoa done
```

## Transactions

For atomic multi-file changes, use transactions:

```bash
kronoa begin

# All changes are buffered
kronoa write kr:config.json < new-config.json
kronoa write kr:version.txt < version.txt
kronoa rm kr:old-config.json

# Commit all at once
kronoa commit
```

To discard buffered changes:
```bash
kronoa rollback
```

## Downloading Content

**Single file:**
```bash
kronoa cp kr:articles/post.md file:./local-post.md
```

**Multiple files with glob:**
```bash
kronoa cp kr:articles/*.md file:./backup/
```

## Scripting Examples

### Backup All Content

```bash
#!/bin/bash
mkdir -p backup
for file in $(kronoa ls kr:articles/); do
    kronoa cp "kr:articles/$file" "file:./backup/$file"
done
```

### Find Large Files

```bash
kronoa ls kr: --json | jq '.[] | select(.size > 100000)'
```

### Update JSON Config

```bash
kronoa cat kr:config.json | jq '.version = "2.0"' | kronoa write kr:config.json
```

### Batch Upload

```bash
#!/bin/bash
kronoa checkout batch-upload
for file in ./posts/*.md; do
    name=$(basename "$file")
    kronoa cp "file:$file" "kr:articles/$name"
done
kronoa submit "Batch uploaded $(ls ./posts/*.md | wc -l) posts"
```

## Admin Operations

These commands are typically used by reviewers/admins.

### Review Submissions

```bash
# List pending submissions
kronoa pending

# Accept a submission
kronoa stage 10005

# Reject with reason
kronoa reject 10005 "Please fix formatting in section 2"
```

### Check Rejected Submissions

```bash
# List all rejections
kronoa rejected

# Get specific rejection reason
kronoa rejected 10005
```

### Deploy to Production

```bash
kronoa deploy
```

### Emergency Rollback

Roll back to a previous edition:

```bash
# Rollback staging and deploy to production
kronoa admin-rollback 10003

# Rollback staging only (don't deploy yet)
kronoa admin-rollback 10003 --no-deploy
```

## Maintenance

### Flatten Edition

Collapse edition ancestry for performance:

```bash
kronoa flatten 10005
```

### Garbage Collection

Analyze orphaned objects:

```bash
kronoa gc --list
kronoa gc --list --json
```

## Troubleshooting

### "No storage configured"

Run `kronoa config set storage <url>` to configure storage.

### "Not in editing mode"

Run `kronoa checkout <label>` before making changes.

### "Edition has been submitted"

Your changes were submitted. Run `kronoa done` to clear the session, then `kronoa pending` to check status.

### "Label in use"

Another session is using that label. Choose a different label or clear the existing session.

## Quick Reference

| Task | Command |
|------|---------|
| Setup storage | `kronoa config set storage ./path` |
| Check status | `kronoa status` |
| List files | `kronoa ls kr:path/` |
| Read file | `kronoa cat kr:file.md` |
| Start editing | `kronoa checkout my-branch` |
| Upload file | `kronoa cp file:./local.md kr:remote.md` |
| Download file | `kronoa cp kr:remote.md file:./local.md` |
| Write from stdin | `echo "text" \| kronoa write kr:file.md` |
| Delete file | `kronoa rm kr:file.md` |
| Submit changes | `kronoa submit "message"` |
| End session | `kronoa done` |
| Review pending | `kronoa pending` |
| Accept submission | `kronoa stage 10005` |
| Deploy | `kronoa deploy` |
