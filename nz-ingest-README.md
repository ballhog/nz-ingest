# nz-ingest

**A smart photo ingest appliance: drop files, review the plan, approve moves.**

nz-ingest watches a drop folder for incoming media, inspects every file, and proposes how to sort it into your archive. Nothing moves without your approval, and everything is journalled — you can always undo.

## The Pipeline

```
drop folder  →  scan  →  plan  →  YOU APPROVE  →  execute  →  manifest
```

1. **Drop** — place photos/videos in a watched folder
2. **Scan** — inspects metadata, detects corruption, compares against archive
3. **Plan** — proposes actions: file, quarantine, deduplicate, park conflicts
4. **Approve** — review and adjust the plan in the web UI
5. **Execute** — moves files; journals all operations for undo
6. **Manifest** — SQLite database of everything in the archive

## Key Principles

**The Four Rules** (earned the hard way on real data):

1. **Verify before dedupe** — a truncated file has a unique hash. Content-based deduplication can delete the intact original in its favor. Integrity is checked first, always.
2. **Identity is not the filename** — camera counters cycle; the same basename is several different photographs. Identity is `serial|imagenumber|datetime`, with hash authoritative for exact duplicates.
3. **Nothing is ever deleted** — damaged files, duplicates, and conflicts are moved into parked trees (`_quarantine`, `_duplicates`, `_conflicts`, `_undated`). Deletion is a separate human act.
4. **Nothing moves without a plan you approved** — every move is journalled so the whole batch can be put back.

## Installation

### TrueNAS Custom App (Docker Compose)

nz-ingest is designed to run in TrueNAS as a Custom App with hot-reload during development.

**Requirements:**
- TrueNAS SCALE
- A dataset for the drop folder (e.g., `/mnt/tank/drop`)
- A dataset for the archive (e.g., `/mnt/tank/archive`)
- A dataset for the database (e.g., `/mnt/tank/data`)

**Setup:**

1. Clone or download the repo into your TrueNAS system
2. In TrueNAS, create a Custom App with this Docker Compose config:

```yaml
version: '3.8'
services:
  ingestd:
    image: python:3.12-slim
    entrypoint: /app/ingestd.py
    environment:
      NZ_DROP: /drop
      NZ_ARCHIVE: /archive
      NZ_DB: /data/ingest.db
      NZ_PORT: "8077"
      NZ_SETTLE: "20"
      NZ_UPDATE_BASE: "https://raw.githubusercontent.com/YOUR_USER/nz-ingest/main/app"
      NZ_UPDATE_TOKEN: ""  # for private repos: a GitHub fine-grained token
    ports:
      - 8077:8077
    volumes:
      - /path/to/drop:/drop
      - /path/to/archive:/archive
      - /path/to/data:/data
      - /path/to/nz-ingest/app:/app:ro  # or remove :ro for hot-reload
```

Adjust paths to match your dataset layout. For development with hot-reload, remove `:ro` from the `/app` volume.

### Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `NZ_DROP` | `/drop` | Folder where photos are dropped |
| `NZ_ARCHIVE` | `/archive` | Root folder where photos are sorted by date |
| `NZ_DB` | `/data/ingest.db` | SQLite manifest database |
| `NZ_PORT` | `8077` | Web UI port |
| `NZ_SETTLE` | `20` | Seconds of no size change before a drop is considered finished copying |
| `NZ_READONLY` | `''` | Set to `1` to refuse all execution (dry-run mode) |
| `NZ_EXCLUDE` | (see code) | Top-level archive folders to skip during baseline scans |
| `NZ_UPDATE_BASE` | `''` | Raw-file base URL for auto-updates (GitHub `/app` folder URL) |
| `NZ_UPDATE_TOKEN` | `''` | GitHub fine-grained token for private repos |

## Usage

### Web UI

Open `http://your-truenas-ip:8077` in your browser.

**Home Panel:**
- **Files indexed** — size of the archive manifest
- **With hashes** — files you've hashed for exact-duplicate detection
- **Need review** — damaged/unreadable files requiring inspection
- **In drop folder** — files waiting to be scanned

**During a Scan:**
- Real-time progress with CPU%, memory, and disk I/O
- Current filename being inspected
- Accretion-disc visualization of scan speed

**Review & Approve:**
- Table of proposed actions (FILE, QUARANTINE, DUPLICATE, CONFLICT, UNDATED)
- Reason for each action
- Checkbox to approve/reject individual items
- "Execute" button to move files

### CLI

```bash
# Scan incoming drop folder and propose a plan (dry-run)
./ingestd.py --plan /path/to/drop

# Index existing archive into the manifest (baseline scan)
# Add --hash to read every byte and detect exact duplicates (slow)
./ingestd.py --baseline /path/to/archive [--hash]

# Load a photo-manifest.py TSV export
./ingestd.py --import-manifest /path/to/manifest.tsv

# Run the daemon + web UI
./ingestd.py
```

## How It Works

### Inspection

For each file, nz-ingest:
1. **Reads metadata** — EXIF DateTimeOriginal, maker, model, etc.
2. **Checks structure** — verifies JPEG, PNG, TIFF, or MP4/MOV headers
3. **Hashes content** — SHA256 to detect duplicates
4. **Compares against archive** — by hash (exact duplicate) and identity (same frame, different bytes)

### Actions

| Action | Meaning |
|--------|---------|
| **FILE** | New photo. Sorted into `/archive/YYYY/MM/DD/` by capture date. |
| **QUARANTINE** | Damaged: truncated, corrupted header, no moov box, zero bytes. Parked in `_quarantine/`. |
| **DUPLICATE** | Byte-identical to a file already in the archive. Parked in `_duplicates/`. |
| **CONFLICT** | Same camera frame (serial+imagenumber+datetime) as an archive file, but different bytes. Parked in `_conflicts/`. Inspect to keep the better version. |
| **UNDATED** | No DateTimeOriginal metadata; can't sort by date. Parked in `_undated/`. |
| **OTHER** | Sidecar (XMP, THM, JSON) with no matching media file in the batch; parked in `_undated/`. |
| **SKIP** | Unreadable (permissions, I/O error). Not moved; logged for manual inspection. |

### Resumable Scans

Large baseline scans can resume. State is saved to disk (`/data/ingest.db.state`) every 500 files. If the scan crashes or is halted, restart it — it picks up where it left off.

## Performance Notes

- **Hashing is I/O bound** — use `--hash` only if you need to detect exact duplicates in an existing archive
- **Metadata extraction is fast** — most files scanned in <10ms
- **Baseline scans are slow** — a 50,000-file archive takes 20–40 minutes depending on storage speed

## Troubleshooting

**"File truncated — expected X bytes"**
→ The file's metadata says it should be larger than it actually is. Damaged during transfer or storage corruption. Inspect manually; if it's a partial/incomplete transfer, wait for a full retry.

**"Header bad" or "No MOOV box"**
→ The file's header is corrupted or incomplete. Likely a partial write or I/O error. Check the source.

**"Unreadable (permissions?)"**
→ The app doesn't have read permission on the file, or there's an I/O error. Check file permissions and filesystem health.

**A file I approved isn't moving**
→ Check `NZ_READONLY=1` (dry-run mode enabled). Also check that the destination path is writable.

**Can't update the app**
→ Set `NZ_UPDATE_BASE` to the GitHub repo's `/app` folder URL. For private repos, also set `NZ_UPDATE_TOKEN` to a fine-grained GitHub token with read-only Contents access.

## Development

**Requirements:**
- Python 3.10+
- No external dependencies — pure stdlib (hashlib, json, sqlite3, http.server, threading)

**Files:**
- `ingestd.py` — daemon, Flask-like HTTP server, and embedded web UI (HTML/CSS/JS)
- `mediacheck.py` — file inspection (metadata, structure, hashing)
- `store.py` — SQLite database layer

**Local testing (non-TrueNAS):**

```bash
export NZ_DROP=/tmp/drop NZ_ARCHIVE=/tmp/archive NZ_DB=/tmp/ingest.db
python3 ingestd.py
```

Then open `http://localhost:8077` and drop test files into `/tmp/drop`.

## License

MIT (or specify your license here)

## Contributing

Contributions welcome. Please test with a real archive first — this tool handles precious data.

---

**Questions?** Check the inline code comments in `ingestd.py` for implementation details. The app is a single ~1700-line file: the whole story is there.
