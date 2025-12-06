# Kronoa CLI Design

Command-line interface for Kronoa content management.

## URI Scheme

Remote paths use `kr:` prefix to distinguish from local files:

```bash
kr:articles/hello.md      # Remote path
file:./local.md           # Local path (explicit)
./local.md                # Local path (implicit, no prefix)
```

## Session Configuration

Session state is stored in `.kronoa/session` (persistent across terminals).

### Environment Variables

```bash
KRONOA_STORAGE=s3://bucket/prefix    # Storage backend
KRONOA_STORAGE=file:///path/to/local # Local filesystem
```

### Config Commands

```bash
kronoa status                 # Show current session state
kronoa done                   # Clear session (end editing)
kronoa config show            # Same as status
kronoa config clear           # Same as done
kronoa config set KEY VALUE   # Set config value
```

Example output:
```
$ kronoa status
Storage: s3://mybucket/contents
Mode:    editing
Label:   my-feature
Edition: 10005
Cwd:     articles/
```

## Navigation

```bash
kronoa pwd                    # Show remote current directory
kronoa cd kr:articles/        # Set remote cwd
kronoa cd ..                  # Go up one level
kronoa cd /                   # Go to root
```

## File Operations

### List

```bash
kronoa ls                     # List remote cwd
kronoa ls kr:articles/        # List specific directory
kronoa ls kr:*.md             # Glob pattern
kronoa ls kr:articles/*.md    # Glob in directory
kronoa ls --json              # JSON output for scripting
```

### Read/Cat

```bash
kronoa cat kr:article.md              # Print to stdout
kronoa cat kr:article.md > local.md   # Redirect to file
kronoa cat kr:chapter-*.md            # Concat multiple files (glob)
```

### Write

```bash
echo "content" | kronoa write kr:article.md     # From stdin
cat local.md | kronoa write kr:article.md       # Pipe file content
kronoa write kr:empty.md --empty                # Create empty file
```

### Copy

```bash
kronoa cp file:./local.md kr:articles/hello.md  # Upload
kronoa cp kr:articles/hello.md file:./local.md  # Download
kronoa cp kr:*.md file:./backup/                # Download multiple (glob)
```

### Delete

```bash
kronoa rm kr:articles/old.md          # Delete single file
kronoa rm kr:draft-*.md --glob        # Delete with glob (requires flag)
```

### Stat

```bash
kronoa stat kr:article.md             # Show file metadata
kronoa stat kr:article.md --json      # JSON output
```

## Editor Workflow

### Checkout

```bash
kronoa checkout my-feature                    # Create working edition from staging
kronoa checkout my-feature --from production  # Branch from production (hotfix)
```

### Editing

```bash
kronoa write kr:articles/new.md < content.md  # Write file
kronoa rm kr:articles/old.md                  # Delete file
kronoa discard kr:articles/new.md             # Undo uncommitted change
```

### Transaction Control

```bash
kronoa begin                  # Start transaction
kronoa write kr:file1.md < a.md
kronoa write kr:file2.md < b.md
kronoa commit                 # End transaction (endEditing)
kronoa rollback               # Discard all buffered changes
```

### Submit

```bash
kronoa submit "Added new articles"    # Submit for review
```

## Admin Workflow

### Review Pending

```bash
kronoa pending                        # List pending submissions
kronoa pending --json                 # JSON output
```

### Stage/Reject

```bash
kronoa stage 10001                    # Accept into staging
kronoa reject 10001 "Needs revision"  # Reject with reason
```

### Rejected Submissions

```bash
kronoa rejected                       # List rejected submissions
kronoa rejected 10001                 # Get specific rejection
```

### Deploy

```bash
kronoa deploy                         # Promote staging to production
```

### Rollback

```bash
kronoa rollback 10003                 # Set staging pointer + deploy (single command)
kronoa rollback 10003 --no-deploy     # Set staging pointer only, skip deploy
```

**Note:** `kronoa rollback` performs both `setStagingPointer` and `deploy` in one command for emergency scenarios. Use `--no-deploy` to only update staging without promoting to production.

## Maintenance

### Flatten

```bash
kronoa flatten 10005                  # Collapse ancestry
```

### Garbage Collection

```bash
kronoa gc --list                      # List orphaned objects (analysis only)
kronoa gc --list --json               # JSON output for scripting
# kronoa gc --execute                 # Not yet implemented (requires mtime support)
```

**Note:** Only `--list` is currently available. Actual deletion (`--execute`) requires mtime support in the storage backend which is not yet implemented.

## Output Formats

Default is human-readable. Use `--json` for scripting:

```bash
kronoa ls --json
kronoa status --json
kronoa pending --json
```

## Glob Support

| Command | Glob | Notes |
|---------|------|-------|
| `ls` | Yes | Safe, read-only |
| `cat` | Yes | Concatenates output |
| `cp` (download) | Yes | Multiple files to directory |
| `cp` (upload) | No | Explicit paths only |
| `rm` | `--glob` | Requires flag for safety |
| `stat` | No | Single file only |

## Examples

### Complete Workflow

```bash
# Setup
export KRONOA_STORAGE=s3://mybucket/contents

# Start editing
kronoa checkout feature-update
kronoa cd kr:articles/

# Make changes
kronoa cp file:./new-post.md ./new-post.md
kronoa rm ./deprecated.md
kronoa ls

# Submit
kronoa submit "Added new post, removed deprecated"

# Admin review
kronoa pending
kronoa stage 10005
kronoa deploy

# Done
kronoa done
```

### Scripting

```bash
# List all markdown files as JSON
kronoa ls kr:articles/*.md --json | jq '.[] | .path'

# Backup all files
for f in $(kronoa ls kr:articles/); do
  kronoa cp "kr:articles/$f" "file:./backup/$f"
done

# Transform and update
kronoa cat kr:config.json | jq '.version = "2.0"' | kronoa write kr:config.json
```
