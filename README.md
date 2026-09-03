# nz-ingest

> **A smart photo ingest appliance for TrueNAS.** Drop files, review the plan, approve moves. Nothing is deleted without your consent — everything is journalled.

---

## What It Does

nz-ingest is a photo management daemon that:

- **Watches a drop folder** for incoming media
- **Inspects every file** for metadata, corruption, and duplicates
- **Proposes a plan** for how to sort it into your archive
- **Waits for your approval** before moving anything
- **Journals all moves** so you can undo if needed
- **Runs on TrueNAS** as a Custom App with a web UI

The core principle: **nothing moves without your permission**, and **nothing is ever deleted** — damaged files and duplicates are parked in separate folders for you to decide.

```
drop folder  →  scan  →  plan  →  YOU APPROVE  →  execute  →  manifest
```

## Quick Start

### Docker / TrueNAS Custom App

```bash
# Clone the repo
git clone https://github.com/YOUR_USER/nz-ingest.git

# Create a Custom App in TrueNAS with:
# - Drop folder: /mnt/tank/drop (or your path)
# - Archive: /mnt/tank/archive
# - Database: /mnt/tank/data
# - Volume map: /app → nz-ingest/app

# Open your browser to http://your-truenas-ip:8077
```

Then:
1. **Drop photos** into the drop folder
2. **View the scan** progress in the web UI
3. **Review the plan** — what gets filed, quarantined, or marked as duplicate
4. **Adjust if needed** — uncheck items you don't want to move
5. **Execute** — move all approved items
6. **Undo** — rollback a batch if something went wrong

See the [full setup guide](./app/README.md) for detailed TrueNAS and CLI instructions.

## Features

✓ **Metadata extraction** — reads EXIF, maker, model, capture date  
✓ **Corruption detection** — finds truncated files, bad headers, broken videos  
✓ **Content-based deduplication** — SHA256 hashing to find exact duplicates  
✓ **Identity-based conflict detection** — same photo from camera, different bytes  
✓ **Resumable baseline scans** — index a 50k-file archive without crashing  
✓ **Never deletes** — damaged/duplicate files are parked, not removed  
✓ **Journal & undo** — reverse any batch of moves if needed  
✓ **Real-time monitoring** — web UI shows CPU, memory, disk I/O, current file  
✓ **Pure stdlib** — no pip dependencies, just Python 3.10+  
✓ **Hot-reload friendly** — edit code and refresh browser during development  
✓ **Auto-update** — fetches new versions from GitHub and prompts to install  

## The Four Rules

These principles were earned by learning from real data loss:

1. **Verify before dedupe** — a truncated file has a unique hash. Always check integrity first.
2. **Identity is not filename** — camera counters cycle. Use serial+imagenumber+datetime + hash.
3. **Nothing is deleted** — damaged and duplicate files are parked. Deletion is a manual act.
4. **Nothing moves without approval** — every move is journalled; you can always undo.

## Web UI

![nz-ingest web UI](./docs/ui.png)

**Home Panel:**
- Archive stats (files indexed, with hashes, damaged, waiting)
- Real-time scan progress with CPU/memory/I/O metrics
- Accretion-disc animation showing scan speed

**Review & Approve:**
- Table of proposed actions (FILE, QUARANTINE, DUPLICATE, CONFLICT, UNDATED, etc.)
- Reason for each action
- Approve/reject individual items
- Execute all approved moves at once

**Firmware:**
- Check for updates from GitHub
- Imperial loading animation while checking
- Install & restart with one click

## Performance

| Task | Time |
|------|------|
| Inspect 1,000 photos (metadata only) | ~30s |
| Baseline scan 10,000 files | ~5 min |
| Baseline scan 50,000 files (with hashing) | 20–40 min |
| Deduplicate scan | 2–3 min per 10k files |

Hashing is I/O-bound. Use `--hash` only when you need exact-duplicate detection in an existing archive.

## Use Cases

**You have:**
- A camera that writes to an SD card
- A folder on your NAS where you dump photos to review later
- An archive organized by capture date
- Some duplicates and damaged files you want to identify

**You want to:**
- Move photos into the archive by date automatically
- Detect duplicates before they clutter your archive
- Know which files are corrupted so you can go back to the source
- Keep full control — no automatic deletion

**nz-ingest** is for you.

## Installation

### TrueNAS SCALE

1. Create datasets for drop, archive, and database
2. Clone this repo into your TrueNAS
3. Create a Custom App with the provided Docker Compose config
4. Set environment variables (paths, update source, etc.)
5. Open the web UI

[Detailed TrueNAS setup guide →](./app/README.md#installation)

### Standalone (Linux/macOS)

```bash
python3 ingestd.py --plan /path/to/drop          # Dry-run scan
python3 ingestd.py --baseline /archive [--hash]  # Index existing archive
python3 ingestd.py                               # Run daemon + web UI
```

[CLI reference →](./app/README.md#usage)

## Configuration

Set these environment variables to customize behavior:

| Variable | Default | Purpose |
|----------|---------|---------|
| `NZ_DROP` | `/drop` | Drop folder path |
| `NZ_ARCHIVE` | `/archive` | Archive root path |
| `NZ_DB` | `/data/ingest.db` | SQLite database path |
| `NZ_PORT` | `8077` | Web UI port |
| `NZ_SETTLE` | `20` | Seconds to wait before scan starts (file copy settle time) |
| `NZ_READONLY` | `''` | Set to `1` for dry-run mode (no moves) |
| `NZ_UPDATE_BASE` | `''` | GitHub raw URL for auto-updates |
| `NZ_UPDATE_TOKEN` | `''` | GitHub token for private repos |

[Full config reference →](./app/README.md#environment-variables)

## Troubleshooting

**Files aren't moving after I approve**
- Check `NZ_READONLY=1` (dry-run mode enabled)
- Check file permissions and archive path

**"File truncated" verdict**
- The file's metadata says it should be larger than it is
- Likely a partial transfer or storage corruption
- Inspect the source; if it's incomplete, wait for a full retry

**Baseline scan is slow**
- Indexing a large archive without hashing takes a few minutes per 10k files
- Use `--hash` only if you need exact-duplicate detection (much slower)

[More troubleshooting →](./app/README.md#troubleshooting)

## Development

**Structure:**
- `app/ingestd.py` — daemon, HTTP server, web UI (1,700 lines)
- `app/mediacheck.py` — file inspection (metadata, structure, hashing)
- `app/store.py` — SQLite database layer

**Local testing:**
```bash
export NZ_DROP=/tmp/drop NZ_ARCHIVE=/tmp/archive
python3 app/ingestd.py
# Visit http://localhost:8077
```

**Contributing:**
1. Test with a real archive first — this tool handles precious data
2. Follow the inline code style (single-file daemon, pure stdlib)
3. Keep the app dependency-free (no pip)
4. Test resumable scans and undo operations

See [CONTRIBUTING.md](./CONTRIBUTING.md) for guidelines.

## Architecture

nz-ingest is intentionally simple:

- **Single Python file** (~1,700 lines) containing daemon, HTTP server, and embedded web UI
- **Pure stdlib** — no external dependencies, just hashlib, json, sqlite3, threading, http.server
- **Resumable state** — saves progress to SQLite every 500 files
- **Hot-reloadable** — edit code and refresh browser during development
- **Docker-friendly** — designed to run in TrueNAS Custom Apps

Why? Because this tool handles your most precious data. Simplicity = fewer places for bugs to hide.

## License

[MIT License](./LICENSE)

## Support & Community

- **Issues:** Report bugs or request features on [GitHub Issues](https://github.com/YOUR_USER/nz-ingest/issues)
- **Discussions:** Ask questions on [GitHub Discussions](https://github.com/YOUR_USER/nz-ingest/discussions)
- **Wiki:** Tips, tricks, and user setups on [GitHub Wiki](https://github.com/YOUR_USER/nz-ingest/wiki)

---

**Built for TrueNAS SCALE. Tested with real archives of thousands of photos.**

*If you're storing irreplaceable photos, this tool respects that. Nothing is deleted without your say-so.*
