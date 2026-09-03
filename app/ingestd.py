#!/usr/bin/env python3
"""
nz-ingest - drop files in a folder, review the plan, then it sorts them.

  drop folder  ->  scan  ->  plan  ->  YOU APPROVE  ->  execute  ->  manifest

Pure stdlib. No pip install, no framework. Serves a small web UI for the
approval step.

THE FOUR RULES, all earned the hard way on 2026-09-02
  1. Verify before dedupe. A truncated file has a UNIQUE hash, so
     content-based dedupe treats it as a keeper and can delete the intact
     original in its favour. Integrity is established first, always.
  2. Identity is not the filename. Camera counters cycle; the same basename
     is several different photographs. Identity is
     serial|imagenumber|datetime, with the hash authoritative for exact
     duplicates.
  3. Nothing is ever deleted. Damaged files, duplicates and conflicts are
     moved into parked trees. Deletion is a separate human act.
  4. Nothing moves without a plan you approved, and every move is
     journalled so the whole batch can be put back.

CONFIG (environment)
  NZ_DROP      drop folder                      default /drop
  NZ_ARCHIVE   archive root to sort into        default /archive
  NZ_DB        sqlite manifest                  default /data/ingest.db
  NZ_PORT      web UI port                      default 8077
  NZ_SETTLE    seconds of no size change before a drop is considered
               finished copying                 default 20
  NZ_READONLY  set to 1 to refuse all execution (dry-run appliance)

CLI
  ingestd.py                       run the daemon + web UI
  ingestd.py --baseline PATH...    scan existing archive into the manifest
                                   (add --hash for exact-duplicate matching;
                                   slow, reads every byte)
  ingestd.py --import-manifest F   load a photo-manifest.py TSV instead
  ingestd.py --plan PATH           scan a folder, print the plan, exit
"""

import hashlib
import json
import os
import shutil
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mediacheck                                            # noqa: E402
from store import Store, ident_of                            # noqa: E402

DROP = os.environ.get('NZ_DROP', '/drop')
ARCHIVE = os.environ.get('NZ_ARCHIVE', '/archive')
DB = os.environ.get('NZ_DB', '/data/ingest.db')
PORT = int(os.environ.get('NZ_PORT', '8077'))
SETTLE = int(os.environ.get('NZ_SETTLE', '20'))
READONLY = os.environ.get('NZ_READONLY', '') == '1'

VERSION = '1.6.9'
# Where the update button pulls from - a raw-file base URL, e.g.
#   https://raw.githubusercontent.com/<user>/nz-ingest/main/app
# Left empty the panel simply reports that no source is configured. Nothing
# is ever fetched unless you press the button.
UPDATE_BASE = os.environ.get('NZ_UPDATE_BASE', '').rstrip('/')
# Optional token for a PRIVATE repo. raw.githubusercontent.com will not serve
# private content at all, so when a token is present the raw URL is rewritten
# to GitHub's contents API, which does. A fine-grained token with read-only
# "Contents" on this one repo is all that is needed.
UPDATE_TOKEN = os.environ.get('NZ_UPDATE_TOKEN', '').strip()
CODE_DIR = os.path.dirname(os.path.abspath(__file__))
CODE_FILES = ('ingestd.py', 'mediacheck.py', 'store.py')

# Top-level names skipped when walking the ARCHIVE root: the app's own parked
# trees, its database, and any other application's storage that happens to
# live on the same dataset (Immich's library, for one). Never applied below
# the top level, and never to the drop folder.
EXCLUDE = set(x for x in os.environ.get(
    'NZ_EXCLUDE',
    'photos,_audit,_ingest,_quarantine,_duplicates,_conflicts,_undated,'
    '.ix-apps,ix-applications').split(',') if x)

DAMAGED = ('ZERO', 'TRUNCATED', 'HEADER_BAD', 'NO_MOOV')
MEDIA_FMTS = ('tiff', 'jpeg', 'png', 'bmff')

PARK = {
    'QUARANTINE': '_quarantine',
    'DUPLICATE': '_duplicates',
    'CONFLICT': '_conflicts',
    'UNDATED': '_undated',
    'OTHER': '_undated',
}

ACTION_HELP = {
    'FILE': 'Sorted into the archive by capture date.',
    'QUARANTINE': 'Damaged. Parked, never deleted — inspect before discarding.',
    'DUPLICATE': 'Byte-identical to a file already in the archive. Parked.',
    'CONFLICT': 'Same camera frame as an existing file but different bytes. '
                'One of them may be damaged or edited — needs your eyes.',
    'UNDATED': 'No capture date in the metadata, so it cannot be sorted. Parked.',
    'OTHER': 'Not a media format this tool checks, and no media file to '
             'accompany. Parked.',
    'SKIP': 'Could not be read at all — usually permissions. Nothing done.',
}

VERDICT_HELP = {
    'ZERO': 'Zero bytes — the file holds nothing. For a photo that means it '
            'never arrived, and no copy or replica can bring it back. For '
            'source code and placeholders an empty file is often correct, so '
            'check what kind of file it is before reading anything into it.',
    'TRUNCATED': 'Shorter than its own metadata says it should be — read from '
                 'the file\'s header, not guessed from its size. The embedded '
                 'preview often still displays, which is how this hides in a '
                 'photo library.',
    'HEADER_BAD': 'The structure did not parse. Either the file is damaged '
                  'further in than a truncation, or it is a format this '
                  'checker does not know — which is a gap in the checker, not '
                  'a fault in the file.',
    'NO_MOOV': 'A video container with no moov box, so it carries no index '
               'and most players will refuse it. Still images in the same '
               'family (HEIC, AVIF) legitimately have none and are not '
               'counted here.',
    'ERROR': 'NOT damage — these could not be opened at all, almost always '
             'permissions. Their real condition is unknown. Any non-zero '
             'count here means the scan did not cover the archive and should '
             'be re-run as a user that can read all of it.',
}

store = None
lock = threading.Lock()
progress = {'batch': None, 'done': 0, 'total': 0, 'phase': ''}

# The baseline is a long job on a machine that reboots. Its state lives on
# disk beside the database so an interrupted scan can pick up where it left
# off - and so the daemon can resume one by itself after a restart, which is
# what three lost scans on 2026-09-02 argued for.
JOB = {'running': False, 'phase': 'idle', 'root': '', 'hash': False,
       'done': 0, 'skipped': 0, 'total': 0, 'started': 0, 'rate': 0.0,
       'note': '', 'file': '', 'cpu': 0.0, 'mem_mb': 0, 'io_mb': 0.0}
JOB_STATE = os.path.join(os.path.dirname(os.path.abspath(DB)),
                         'baseline.state')
_stop = threading.Event()

# System stats sampling for the status panel.
_last_stat = None
def sample_stats():
    """CPU %, memory (MB), disk I/O (MB/s). Pure /proc parsing, no psutil."""
    global _last_stat
    stats = {'cpu': 0.0, 'mem_mb': 0, 'io_mb': 0.0}
    try:
        # CPU: /proc/self/stat gives utime+stime in ticks. To get %, we need
        # to track the delta and divide by elapsed wall time.
        with open('/proc/self/stat') as f:
            parts = f.read().split()
            utime, stime = int(parts[13]), int(parts[14])
            cpu_ticks = utime + stime
        now = time.time()
        if _last_stat and _last_stat.get('cpu_ticks') is not None:
            delta_ticks = cpu_ticks - _last_stat['cpu_ticks']
            elapsed = now - _last_stat['time']
            if elapsed > 0:
                # Assume 100 ticks/sec (typical on Linux); compare to 1 core
                # at 100%. Divide by elapsed to get utilization percentage.
                cpu_pct = (delta_ticks / 100.0) / elapsed * 100.0
                stats['cpu'] = round(min(100.0, cpu_pct), 1)
        if _last_stat is None:
            _last_stat = {}
        _last_stat['cpu_ticks'] = cpu_ticks
        _last_stat['time'] = now
    except:
        pass
    try:
        # Memory: /proc/self/status has VmRSS
        with open('/proc/self/status') as f:
            for line in f:
                if line.startswith('VmRSS:'):
                    kb = int(line.split()[1])
                    stats['mem_mb'] = kb // 1024
                    break
    except:
        pass
    try:
        # Disk I/O: sample /proc/diskstats for read+write bytes.
        # This is a simple cumulative counter; calculate delta over time.
        with open('/proc/diskstats') as f:
            io_bytes = 0
            for line in f:
                # Only count the main data partition (usually sda or similar).
                # Format: major minor name reads_completed reads_merged reads_sectors
                #         reads_time writes_completed writes_merged writes_sectors...
                parts = line.split()
                if len(parts) >= 10 and parts[2] in ('sda', 'sdb', 'nvme0n1'):
                    # Sectors are 512 bytes each; sum reads + writes
                    reads = int(parts[5])  # read sectors
                    writes = int(parts[9])  # write sectors
                    io_bytes += (reads + writes) * 512
            if _last_stat and _last_stat.get('io_bytes') is not None:
                delta = max(0, io_bytes - _last_stat['io_bytes'])
                elapsed = time.time() - _last_stat['time']
                if elapsed > 0:
                    stats['io_mb'] = round(delta / (1024*1024*elapsed), 1)
        # Update shared state (don't overwrite, merge)
        if _last_stat is None:
            _last_stat = {}
        _last_stat['io_bytes'] = io_bytes
    except:
        pass
    return stats


def job_save():
    try:
        with open(JOB_STATE, 'w') as f:
            json.dump({'running': JOB['running'], 'root': JOB['root'],
                       'hash': JOB['hash']}, f)
    except OSError:
        pass


def job_load():
    try:
        with open(JOB_STATE) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def run_baseline(root, want_hash=False, resume=True):
    """Inspect every file under root into the manifest.

    resume=True skips files already recorded with the same size and mtime.
    That makes an interrupted scan cheap to continue instead of a full
    re-walk, which matters because this job outlives neither reboots nor
    container restarts.
    """
    if JOB['running']:
        raise RuntimeError('a scan is already running')
    _stop.clear()
    JOB.update({'running': True, 'phase': 'walking', 'root': root,
                'hash': bool(want_hash), 'done': 0, 'skipped': 0,
                'total': 0, 'started': time.time(), 'rate': 0.0, 'note': ''})
    job_save()
    try:
        paths = walk_files(root)
        JOB['total'] = len(paths)

        known = {}
        if resume:
            JOB['phase'] = 'reading manifest'
            for r in store.db.execute('SELECT path, size, mtime FROM files'):
                known[r['path']] = (r['size'], str(r['mtime']))

        JOB['phase'] = 'inspecting'
        t0 = time.time()
        n = 0
        for p in paths:
            if _stop.is_set():
                JOB['note'] = 'stopped at %d of %d' % (JOB['done'], JOB['total'])
                break
            # Track current file for the status readout panel.
            JOB['file'] = os.path.basename(p)
            rel = rel_to_archive(p)
            if rel in known:
                try:
                    st = os.stat(p)
                    if (known[rel][0] == st.st_size
                            and known[rel][1] == str(int(st.st_mtime))):
                        JOB['skipped'] += 1
                        JOB['done'] += 1
                        continue
                except OSError:
                    pass
            r = mediacheck.inspect(p, want_hash)
            store.add_file(rel, r)
            n += 1
            JOB['done'] += 1
            # Sample system stats and commit every 500 files.
            if n % 500 == 0:
                store.commit()
                el = time.time() - t0
                JOB['rate'] = n / el if el else 0.0
                # Sample CPU, memory, disk I/O for the UI.
                s = sample_stats()
                JOB.update(s)
        store.commit()
        # Final stats.
        s = sample_stats()
        JOB.update(s)
        if not _stop.is_set():
            JOB['note'] = ('%d files, %d inspected, %d unchanged'
                           % (JOB['total'], n, JOB['skipped']))
    finally:
        JOB.update({'running': False, 'phase': 'idle', 'rate': 0.0})
        job_save()
    return JOB['note']


def job_start(root=None, want_hash=False, resume=True):
    root = root or ARCHIVE
    t = threading.Thread(target=run_baseline, args=(root, want_hash, resume),
                         daemon=True)
    t.start()
    return t


# ----------------------------------------------------------------- firmware

def fw_local():
    """Short hash and size of each running source file."""
    out = {}
    for f in CODE_FILES:
        try:
            b = open(os.path.join(CODE_DIR, f), 'rb').read()
            out[f] = {'bytes': len(b),
                      'sha': hashlib.sha256(b).hexdigest()[:12]}
        except OSError:
            out[f] = {'bytes': 0, 'sha': '-'}
    return out


def _peek(b, n=60):
    """First few bytes as printable text, for error messages. Knowing what
    actually came back is the difference between a five-second fix and an
    hour of guessing."""
    try:
        t = b[:n].decode('utf-8', 'replace')
    except Exception:                                       # noqa: BLE001
        t = repr(b[:n])
    return t.replace('\n', ' ').replace('\r', ' ').strip() or '(empty)'


def fw_url(fname):
    """Resolve one file's URL, switching to the API form for private repos.

    raw.githubusercontent.com/OWNER/REPO/BRANCH/app/x.py
      -> api.github.com/repos/OWNER/REPO/contents/app/x.py?ref=BRANCH
    """
    base = '%s/%s' % (UPDATE_BASE, fname)
    if not UPDATE_TOKEN or 'raw.githubusercontent.com' not in base:
        return base
    tail = base.split('raw.githubusercontent.com/', 1)[1]
    bits = tail.split('/')
    if len(bits) < 4:
        return base
    owner, repo, branch = bits[0], bits[1], bits[2]
    path = '/'.join(bits[3:])
    return ('https://api.github.com/repos/%s/%s/contents/%s?ref=%s'
            % (owner, repo, path, branch))


def fw_repo():
    """(owner, repo, branch, subpath) from a raw.githubusercontent base, or
    None when the source is not GitHub."""
    parts = UPDATE_BASE.split('raw.githubusercontent.com/', 1)
    if len(parts) < 2:
        return None
    tail = parts[1]
    bits = [b for b in tail.split('/') if b]
    if len(bits) < 3:
        return None
    return (bits[0], bits[1], bits[2], '/'.join(bits[3:]))


def fw_commits():
    """Last commit per file: short sha, subject, date.

    Decoration, not function. It must NEVER raise: an update that works is
    worth more than knowing which commit it came from, and an early version
    of this let a metadata failure block the install entirely.
    """
    try:
        return _fw_commits()
    except Exception:                                       # noqa: BLE001
        return {}


def _fw_commits():
    info = {}
    r = fw_repo()
    if not r:
        return info
    import urllib.request
    import urllib.error
    owner, repo, branch, sub = r
    for f in CODE_FILES:
        path = ('%s/%s' % (sub, f)) if sub else f
        url = ('https://api.github.com/repos/%s/%s/commits'
               '?path=%s&sha=%s&per_page=1' % (owner, repo, path, branch))
        try:
            req = urllib.request.Request(url, headers=fw_headers())
            with urllib.request.urlopen(req, timeout=20) as resp:
                data = json.loads(resp.read().decode('utf-8'))
            if not data:
                continue
            c = data[0]
            msg = (c.get('commit', {}).get('message') or '').split('\n')[0]
            info[f] = {
                'rev': (c.get('sha') or '')[:7],
                'msg': msg[:90],
                'when': (c.get('commit', {}).get('committer', {})
                         .get('date') or ''),
                'who': (c.get('commit', {}).get('author', {})
                        .get('name') or ''),
            }
        except Exception:                                   # noqa: BLE001
            continue
    return info


def fw_record(commits):
    """Remember what was installed, so the panel can show installed revision
    against latest without another round trip."""
    try:
        with open(os.path.join(os.path.dirname(os.path.abspath(DB)),
                               'firmware.json'), 'w') as fh:
            json.dump({'at': int(time.time()), 'version': VERSION,
                       'commits': commits}, fh)
    except OSError:
        pass


def fw_installed():
    try:
        with open(os.path.join(os.path.dirname(os.path.abspath(DB)),
                               'firmware.json')) as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {}


def fw_headers():
    h = {'User-Agent': 'nz-ingest'}
    if UPDATE_TOKEN:
        h['Authorization'] = 'Bearer ' + UPDATE_TOKEN
        h['Accept'] = 'application/vnd.github.raw'
        h['X-GitHub-Api-Version'] = '2022-11-28'
    return h


def fw_fetch():
    """Pull each source file and refuse anything that will not compile.

    The gate matters more than the fetch: a truncated download or a syntax
    error written over a running app is a brick, and nothing inside the
    container could then serve a page to fix it from.
    """
    if not UPDATE_BASE:
        raise RuntimeError('NZ_UPDATE_BASE is not set')
    import urllib.request
    import urllib.error
    got = {}
    for f in CODE_FILES:
        url = fw_url(f)
        req = urllib.request.Request(url, headers=fw_headers())
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                b = r.read()
        except urllib.error.HTTPError as exc:
            hint = ''
            if exc.code == 404:
                hint = (' — repo/branch/path wrong, or the repo is private and '
                        'NZ_UPDATE_TOKEN is not set' if not UPDATE_TOKEN
                        else ' — path wrong, or the token cannot read this repo')
            elif exc.code in (401, 403):
                hint = ' — token rejected or lacks Contents:read on this repo'
            raise RuntimeError('%s: HTTP %s%s' % (f, exc.code, hint))
        if len(b) < 600:
            raise RuntimeError('%s came back only %d bytes - refusing (%s)'
                               % (f, len(b), _peek(b)))
        # utf-8-sig strips a BOM. Anything that has passed through a Windows
        # editor on its way to the repo may carry three invisible bytes at the
        # front, and Python rejects those as a syntax error on line 1.
        try:
            text = b.decode('utf-8-sig')
        except UnicodeDecodeError as exc:
            raise RuntimeError('%s is not text: %s (%s)'
                               % (f, exc, _peek(b)))
        if text.lstrip().startswith(('<!DOCTYPE', '<html', '<?xml')):
            raise RuntimeError(
                '%s came back as a web page, not code - the base URL is '
                'pointing at a GitHub page rather than raw content. It must '
                'start https://raw.githubusercontent.com/ (got: %s)'
                % (f, _peek(b)))
        try:
            compile(text, f, 'exec')
        except SyntaxError as exc:
            raise RuntimeError('%s does not compile: %s (starts: %s)'
                               % (f, exc, _peek(b)))
        got[f] = text.encode('utf-8')      # normalised, BOM removed
    return got


def fw_compare(got):
    loc = fw_local()
    out = {}
    for f, b in got.items():
        sha = hashlib.sha256(b).hexdigest()[:12]
        out[f] = {'bytes': len(b), 'sha': sha,
                  'changed': sha != loc[f]['sha']}
    return out


def fw_install(got, commits=None):
    """Write the verified files, keeping .bak copies, then exit so the
    container's restart policy brings the new code up."""
    for f, b in got.items():
        path = os.path.join(CODE_DIR, f)
        if os.path.exists(path):
            shutil.copy2(path, path + '.bak')
        tmp = path + '.new'
        with open(tmp, 'wb') as fh:
            fh.write(b)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    fw_record(commits or {})
    threading.Timer(1.2, lambda: os._exit(3)).start()


# ------------------------------------------------------------------ helpers

def rel_to_archive(p):
    p = os.path.abspath(p)
    a = os.path.abspath(ARCHIVE)
    return os.path.relpath(p, a) if p.startswith(a + os.sep) else p


def date_dir(dt):
    """'2021:07:03 14:22:31' -> '2021/07/03'. Empty if unusable."""
    if not dt or len(dt) < 10:
        return ''
    d = dt[:10].replace(':', '/').replace('-', '/')
    parts = d.split('/')
    if len(parts) != 3 or not all(x.isdigit() for x in parts):
        return ''
    y, m, day = parts
    if not (1970 <= int(y) <= 2100):
        return ''
    return '%s/%s/%s' % (y, m, day)


def unique_dest(path):
    if not os.path.exists(path):
        return path
    stem, ext = os.path.splitext(path)
    n = 1
    while os.path.exists('%s_%d%s' % (stem, n, ext)):
        n += 1
        if n > 9999:
            raise IOError('cannot find a free name for %s' % path)
    return '%s_%d%s' % (stem, n, ext)


def walk_files(root, exclude_top=True):
    """Walk root, skipping junk dirs and (at the top level only) anything in
    NZ_EXCLUDE. The excludes matter: /archive holds Immich's own library and
    the parked trees, and neither belongs in a manifest of archive photos."""
    root = os.path.abspath(root)
    out = []
    for dirpath, dirnames, filenames in os.walk(root, onerror=None):
        dirnames[:] = sorted(d for d in dirnames
                             if d not in ('.Trashes', '.Spotlight-V100',
                                          '@Recycle', '.@__thumb'))
        if exclude_top and os.path.abspath(dirpath) == root:
            dirnames[:] = [d for d in dirnames if d not in EXCLUDE]
        for name in sorted(filenames):
            if name in ('.DS_Store', 'Thumbs.db') or name.startswith('._'):
                continue
            out.append(os.path.join(dirpath, name))
    return out


def safe_move(src, dst, expect_sha=''):
    """Move src->dst. Verifies before removing the source. Returns method."""
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    try:
        os.rename(src, dst)
        return 'rename'
    except OSError:
        pass
    tmp = dst + '.part'
    src_size = os.path.getsize(src)
    with open(src, 'rb') as i, open(tmp, 'wb') as o:
        shutil.copyfileobj(i, o, 1 << 20)
        o.flush()
        os.fsync(o.fileno())
    if os.path.getsize(tmp) != src_size:
        os.unlink(tmp)
        raise IOError('size mismatch after copy')
    if expect_sha:
        import hashlib
        h = hashlib.sha256()
        with open(tmp, 'rb') as f:
            while True:
                c = f.read(1 << 20)
                if not c:
                    break
                h.update(c)
        if h.hexdigest() != expect_sha:
            os.unlink(tmp)
            raise IOError('hash mismatch after copy')
    os.rename(tmp, dst)
    os.unlink(src)
    return 'copy'


# ------------------------------------------------------------------ planner

def plan_batch(bid, root, items_out=None):
    """Inspect every file under root and decide what should happen to it."""
    paths = walk_files(root, exclude_top=False)
    progress.update({'batch': bid, 'done': 0, 'total': len(paths),
                     'phase': 'inspecting'})

    rows = []
    for i, p in enumerate(paths):
        r = mediacheck.inspect(p, True)          # always hash incoming
        r['src'] = p
        r['rel'] = os.path.relpath(p, root)
        r['ident'] = ident_of(r)
        rows.append(r)
        progress['done'] = i + 1

    progress['phase'] = 'planning'

    # media first, so sidecars can follow a decided parent
    rows.sort(key=lambda r: (0 if r['fmt'] in MEDIA_FMTS else 1, r['rel']))

    seen_hash = {}         # hash -> chosen dest within this batch
    decided = {}           # (dir, stem) -> dest of the media file
    planned = []

    for r in rows:
        act = reason = dup_of = grp = ''
        dest = ''
        v = r['verdict']

        if v == 'ERROR':
            act, reason = 'SKIP', 'unreadable (permissions?) — nothing done'

        elif v in DAMAGED:
            act = 'QUARANTINE'
            reason = {'ZERO': 'zero bytes — no image data at all',
                      'TRUNCATED': 'file is shorter than its own metadata '
                                   'requires (expected %s bytes)'
                                   % r.get('expected'),
                      'HEADER_BAD': 'structure unreadable or inconsistent',
                      'NO_MOOV': 'video container has no moov box'}[v]
            grp = v

        else:
            existing = store.by_hash(r['sha256'])
            if existing:
                act, dup_of = 'DUPLICATE', existing
                reason = 'byte-identical to a file already in the archive'
                grp = 'archive'
            elif r['sha256'] in seen_hash:
                act, dup_of = 'DUPLICATE', seen_hash[r['sha256']]
                reason = 'byte-identical to another file in this same batch'
                grp = 'batch'
            else:
                same_frame = store.by_ident(r['ident'])
                if same_frame:
                    act, dup_of = 'CONFLICT', same_frame[0]
                    reason = ('same camera frame as an existing file '
                              '(%s) but different bytes' % same_frame[0])
                    grp = 'ident'
                elif r['fmt'] in MEDIA_FMTS:
                    dd = date_dir(r['dt'])
                    if dd:
                        act = 'FILE'
                        dest = os.path.join(ARCHIVE, dd,
                                            os.path.basename(r['src']))
                        reason = 'capture date %s' % r['dt'][:10]
                    else:
                        act = 'UNDATED'
                        reason = 'no DateTimeOriginal in metadata'
                else:
                    key = (os.path.dirname(r['src']),
                           os.path.splitext(os.path.basename(r['src']))[0])
                    if key in decided:
                        act = 'FILE'
                        dest = os.path.join(
                            os.path.dirname(decided[key]),
                            os.path.basename(r['src']))
                        reason = 'sidecar — follows %s' % os.path.basename(
                            decided[key])
                    else:
                        act = 'OTHER'
                        reason = 'not a checked media format'

        if act != 'FILE' and act != 'SKIP':
            dest = os.path.join(ARCHIVE, PARK[act], 'batch-%d' % bid, r['rel'])

        if dest:
            dest = unique_dest(dest)

        if act == 'FILE':
            seen_hash[r['sha256']] = dest
            if r['fmt'] in MEDIA_FMTS:
                decided[(os.path.dirname(r['src']),
                         os.path.splitext(os.path.basename(r['src']))[0])] = dest

        it = dict(r, action=act, dest=dest, reason=reason, dup_of=dup_of,
                  grp=grp, approved=(act != 'SKIP'))
        planned.append(it)
        if items_out is None:
            store.add_item(bid, it)

    if items_out is not None:
        items_out.extend(planned)
        return planned

    blocked = 1 if any(p['action'] == 'SKIP' for p in planned) else 0
    store.set_batch(bid, state='review', blocked=blocked)
    store.commit()
    progress.update({'phase': 'review', 'batch': bid})
    return planned


# ----------------------------------------------------------------- executor

def execute_batch(bid):
    if READONLY:
        raise RuntimeError('NZ_READONLY=1 — execution disabled')
    b = store.batch(bid)
    if not b or b['state'] not in ('review', 'failed'):
        raise RuntimeError('batch %s is not awaiting execution' % bid)

    items = [i for i in store.items(bid)
             if i['approved'] and i['action'] != 'SKIP' and i['dest']
             and i['result'] != 'moved']
    store.set_batch(bid, state='executing')
    progress.update({'batch': bid, 'done': 0, 'total': len(items),
                     'phase': 'moving'})

    errors = 0
    for n, it in enumerate(items):
        try:
            dst = unique_dest(it['dest'])
            safe_move(it['src'], dst, it['sha256'] or '')
            store.journal_add(bid, it['src'], dst)
            store.set_result(it['id'], 'moved', dst)
            if it['action'] == 'FILE':
                store.add_file(rel_to_archive(dst), dict(it))
        except Exception as exc:                        # noqa: BLE001
            errors += 1
            store.set_result(it['id'], 'error', str(exc))
        progress['done'] = n + 1
        if n % 25 == 0:
            store.commit()

    store.commit()
    prune_empty(DROP)
    store.set_batch(bid, state='failed' if errors else 'done',
                    decided_at=int(time.time()),
                    note='%d error(s)' % errors if errors else '')
    progress['phase'] = 'done'
    return errors


def undo_batch(bid):
    if READONLY:
        raise RuntimeError('NZ_READONLY=1 — execution disabled')
    moved = store.journal(bid)
    back = 0
    for j in moved:
        try:
            if os.path.exists(j['dst']) and not os.path.exists(j['src']):
                safe_move(j['dst'], j['src'])
                store.drop_file(rel_to_archive(j['dst']))
                back += 1
        except Exception:                               # noqa: BLE001
            pass
    store.journal_clear(bid)
    store.set_batch(bid, state='undone', note='%d file(s) put back' % back)
    store.commit()
    return back


def prune_empty(root):
    for dirpath, dirnames, filenames in os.walk(root, topdown=False):
        if os.path.abspath(dirpath) == os.path.abspath(root):
            continue
        try:
            if not os.listdir(dirpath):
                os.rmdir(dirpath)
        except OSError:
            pass


# ------------------------------------------------------------------ watcher

def drop_signature():
    sig = []
    for p in walk_files(DROP, exclude_top=False):
        try:
            st = os.stat(p)
            sig.append((p, st.st_size))
        except OSError:
            pass
    return sig


def watcher():
    last, stable_since = None, 0.0
    while True:
        time.sleep(5)
        try:
            open_batch = any(b['state'] in ('scanning', 'review', 'executing')
                             for b in store.batches(10))
            sig = drop_signature()
            if not sig:
                last, stable_since = None, 0.0
                continue
            if sig != last:
                last, stable_since = sig, time.time()
                continue
            if open_batch or not stable_since:
                continue
            if time.time() - stable_since < SETTLE:
                continue
            with lock:
                bid = store.new_batch(
                    time.strftime('%Y-%m-%d %H:%M') + ' (%d files)' % len(sig),
                    DROP)
                plan_batch(bid, DROP)
            last, stable_since = None, 0.0
        except Exception as exc:                        # noqa: BLE001
            sys.stderr.write('watcher: %s\n' % exc)


# ----------------------------------------------------------------- web UI

PAGE = r"""<!doctype html><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>NZ-INGEST</title>
<style>
/* ---------------------------------------------------------------------------
   Ferrix panel. Deliberately dark-only: the whole design is backlit acrylic
   on black, and a light variant would be a different object. Every colour is
   stated explicitly rather than inherited.
   --------------------------------------------------------------------------- */
:root{
  --void:#06060a; --plate:#0c0c11; --inset:#101017; --bezel:#191920;
  --edge:#262630; --edge2:#33333f;
  --ink:#e8ecf4; --dim:#7e8494; --faint:#4b5060;
  --cyan:#3ad0ff; --amber:#ffc63f; --orange:#ff7d24; --red:#ff3b3b;
  --white:#f4f7ff;
  --mono:ui-monospace,SFMono-Regular,"SF Mono",Menlo,Consolas,monospace;
  --disp:"Helvetica Neue",Inter,system-ui,-apple-system,sans-serif;
}
*{box-sizing:border-box}
html{background:var(--void)}
body{margin:0;background:
  radial-gradient(1200px 700px at 50% -10%,#12121a 0%,#06060a 70%);
  color:var(--ink);font:13px/1.55 var(--disp);
  -webkit-font-smoothing:antialiased;min-height:100vh}
a{color:inherit;text-decoration:none}

/* ---- chassis ---- */
.chassis{max-width:1120px;margin:0 auto;padding:22px 14px 70px}
.panel{position:relative;background:linear-gradient(#131319,#0b0b10);
  border:1px solid var(--edge);border-radius:16px;
  box-shadow:0 0 0 1px #000 inset,0 24px 70px rgba(0,0,0,.75),
             0 1px 0 rgba(255,255,255,.05) inset;
  padding:20px 20px 16px}
.screw{position:absolute;width:9px;height:9px;border-radius:50%;
  background:radial-gradient(circle at 35% 30%,#5c5f6b,#20222a 60%,#0a0a0e);
  box-shadow:0 1px 1px rgba(0,0,0,.8),0 0 0 1px #000}
.screw.tl{left:9px;top:9px}.screw.tr{right:9px;top:9px}
.screw.bl{left:9px;bottom:9px}.screw.br{right:9px;bottom:9px}

/* ---- indicator head ---- */
.head{display:flex;align-items:flex-start;gap:22px;flex-wrap:wrap;
  padding:2px 4px 16px;border-bottom:1px solid var(--edge)}
.tris{display:flex;gap:14px;align-items:flex-start}
.tri{width:0;height:0;border-left:26px solid transparent;
  border-right:26px solid transparent;position:relative;
  transition:filter .4s,opacity .4s}
.tri.down{border-top:34px solid currentColor;border-bottom:0}
.tri.up{border-bottom:34px solid currentColor;border-top:0}
.tri.off{opacity:.14;filter:none}
.tri.on{opacity:1;filter:drop-shadow(0 0 14px currentColor)}
.tri-w{color:var(--white)}.tri-a{color:var(--amber)}.tri-o{color:var(--orange)}
.trilab{font:9px/1 var(--mono);letter-spacing:.18em;color:var(--dim);
  text-align:center;margin-top:8px;width:52px}
.mark{display:flex;gap:5px;align-items:center;margin-left:auto}
.mark i{display:block;width:11px;height:46px;background:var(--cyan);
  border-radius:2px;box-shadow:0 0 16px rgba(58,208,255,.7),
  0 0 42px rgba(58,208,255,.28)}
.mark i.s{height:26px}
.wordmark{font:600 15px/1 var(--disp);letter-spacing:.34em;color:var(--ink);
  margin-left:12px}
.sub{font:10px/1.4 var(--mono);letter-spacing:.12em;color:var(--faint);
  margin-top:5px}

/* ---- indicator strip: hex lamps in a recessed channel ---- */
.leds{display:flex;gap:6px;align-items:center;flex-wrap:wrap;margin:14px 0 2px;
  padding:11px 14px;background:#07070c;border-radius:7px;
  border:1px solid #000;
  box-shadow:0 3px 9px rgba(0,0,0,.75) inset,
             0 1px 0 rgba(255,255,255,.045)}
.led{width:15px;height:17px;background:#191921;
  clip-path:polygon(25% 0,75% 0,100% 50%,75% 100%,25% 100%,0 50%);
  transition:background .3s,box-shadow .3s,filter .3s}
.led.r{background:var(--red);filter:drop-shadow(0 0 7px var(--red))}
.led.c{background:var(--cyan);filter:drop-shadow(0 0 7px var(--cyan))}
.led.a{background:var(--amber);filter:drop-shadow(0 0 7px var(--amber))}
.ledlab{font:9px/1 var(--mono);letter-spacing:.16em;color:var(--faint);
  margin-left:10px}
.keys{display:flex;gap:4px;margin-left:auto}
.keys i{display:block;width:15px;height:15px;border-radius:2px;
  background:linear-gradient(#2b2b36,#171720);
  box-shadow:0 1px 0 rgba(255,255,255,.08) inset,0 1px 2px rgba(0,0,0,.7)}
.keys i.y{background:linear-gradient(#ffd24a,#c99408);
  box-shadow:0 0 9px rgba(255,198,63,.5)}
.keys i.w{background:linear-gradient(#fbfdff,#b9c2d2)}
.dome{width:34px;height:34px;border-radius:50%;flex:none;
  background:radial-gradient(circle at 36% 28%,#f2f5fa 0%,#9aa2b2 34%,
    #454b58 62%,#171b22 100%);
  box-shadow:0 0 0 3px #14161c,0 0 0 4px #2b2f38,0 3px 7px rgba(0,0,0,.8);
  margin-left:14px}

/* ---- modules ---- */
.mods{display:grid;gap:12px;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));
  padding:16px 0 4px}
.mod{background:linear-gradient(#0e0e14,#0a0a10);border:1px solid #000;
  border-radius:8px;padding:13px 15px;position:relative;
  box-shadow:0 4px 12px rgba(0,0,0,.7) inset,
             0 1px 0 rgba(255,255,255,.055),
             0 0 0 1px #21212a}
.mod:before{content:"";position:absolute;inset:4px;border-radius:5px;
  border:1px solid rgba(255,255,255,.035);pointer-events:none}
.mod b{display:block;font:600 27px/1.05 var(--disp);letter-spacing:-.02em;
  color:var(--ink)}
.mod span{display:block;font:9px/1.4 var(--mono);letter-spacing:.15em;
  color:var(--dim);margin-top:6px;text-transform:uppercase}
.mod.hit{cursor:pointer}
.mod.hit:hover{border-color:var(--edge2)}
.mod.warn b{color:var(--amber);text-shadow:0 0 22px rgba(255,198,63,.45)}
.mod.bad b{color:var(--orange);text-shadow:0 0 22px rgba(255,125,36,.45)}
.mod.cy b{color:var(--cyan);text-shadow:0 0 22px rgba(58,208,255,.4)}

/* ---- viewport (scan) ---- */
.vp{position:relative;margin:14px 0 4px;border:1px solid #000;
  border-radius:10px;overflow:hidden;background:#05050a;
  box-shadow:0 6px 20px rgba(0,0,0,.85) inset,
             0 0 44px rgba(58,208,255,.07) inset,
             0 1px 0 rgba(255,255,255,.05),0 0 0 1px #21212a}
.vp:before{content:"";position:absolute;inset:6px;border-radius:6px;
  border:1px solid rgba(255,255,255,.045);pointer-events:none;z-index:2}
.vp canvas{display:block;width:100%;height:186px}
.vp .ov{position:absolute;inset:0;padding:0 22px;display:flex;
  flex-direction:column;justify-content:center;pointer-events:none;
  text-shadow:0 2px 18px #000}
.vp .ph{font:9px/1 var(--mono);letter-spacing:.22em;color:var(--cyan);
  text-transform:uppercase}
.vp .big{font:600 34px/1.1 var(--disp);letter-spacing:-.02em;margin:6px 0 3px;
  color:var(--white)}
.vp .meta{font:11px/1.5 var(--mono);color:#9aa2b4}
.vp .act{position:absolute;right:14px;bottom:13px}

/* ---- in-flight unified panel ---- */
.inflight{background:linear-gradient(#0e0e14,#0a0a10);border:1px solid #000;
  border-radius:8px;padding:14px 16px;margin:10px 0;
  box-shadow:0 4px 12px rgba(0,0,0,.7) inset,
             0 1px 0 rgba(255,255,255,.055),0 0 0 1px #21212a}
.inflight .prog{margin-bottom:10px;display:grid;gap:4px}
.inflight .prog .row{display:grid;grid-template-columns:80px 1fr;gap:12px;
  align-items:baseline;font:11px/1.5 var(--mono)}
.inflight .prog .label{color:#666;text-transform:uppercase;letter-spacing:.08em;
  font-size:9px}
.inflight .prog .val{color:var(--cyan);font-weight:600}
.inflight .file-box{background:rgba(0,0,0,.3);border:1px solid #333;
  border-radius:4px;padding:6px 8px;margin:8px 0;font:10px/1.4 var(--mono);
  color:var(--cyan);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.inflight .stats-head{font:9px/1 var(--mono);letter-spacing:.16em;color:var(--amber);
  text-transform:uppercase;margin:8px 0 6px}
/* ---- telemetry traces ----
   Four rolling channels over a graticule, sitting above the four static
   bars they are meant to replace. A bar says what is happening now; a
   trace says what changed, which is the question you actually have when
   throughput falls off a cliff and you need to know whether it went I/O
   bound or CPU bound. Cyan is the healthy palette - red is spent only on
   real damage, one mark per condemned file at the point it was found. */
.inflight .trace{position:relative;margin:2px 0 8px;border:1px solid #000;
  border-radius:6px;overflow:hidden;background:#05050a;
  box-shadow:0 3px 14px rgba(0,0,0,.8) inset,
             0 0 30px rgba(58,208,255,.05) inset,0 0 0 1px #1c1c24}
.inflight .trace:before{content:"";position:absolute;inset:4px;border-radius:3px;
  border:1px solid rgba(255,255,255,.035);pointer-events:none}
.inflight .trace canvas{display:block;width:100%;height:120px}
.inflight .trlegend{display:flex;gap:15px;flex-wrap:wrap;margin:0 0 10px;
  font:9px/1.4 var(--mono);letter-spacing:.12em;color:var(--faint);
  text-transform:uppercase}
.inflight .trlegend b{color:var(--dim);font-weight:400}
.inflight .trlegend s{width:8px;height:2px;display:inline-block;margin-right:5px;
  text-decoration:none;vertical-align:2px}

.inflight .stats{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}
.inflight .stat{display:flex;flex-direction:column;gap:3px}
.inflight .label{font:9px/1 var(--mono);color:#666;text-transform:uppercase;
  letter-spacing:.08em}
.inflight .val{font:600 13px/1 var(--mono);color:var(--cyan)}
.inflight .bar{height:4px;background:rgba(0,0,0,.5);border:1px solid #333;
  border-radius:2px;overflow:hidden;--w:0%}
.inflight .bar:before{content:"";display:block;height:100%;background:
  linear-gradient(90deg,#4dd0ff,#00d9ff);width:var(--w);transition:width .15s ease-out}
.inflight #speedbar:before{background:linear-gradient(90deg,#4dd0ff,#00d9ff)}
.inflight #cpubar:before{background:linear-gradient(90deg,#ffaa00,#ff8800)}
.inflight #membar:before{background:linear-gradient(90deg,#ff6b6b,#ff4444)}
.inflight #iobar:before{background:linear-gradient(90deg,#66ff99,#44ff77)}

/* ---- archive snapshot ----
   Same acrylic plate as .mod, but the cells are read as a set rather than
   headline numbers, so the type is one step down and the glow is dropped
   from all but the size figure. The carousel is a stack of absolutely
   positioned cards cross-fading in place: rotating by re-rendering innerHTML
   would reflow the rack on every tick, which is the flicker the firmware
   panel was rebuilt to avoid. */
.asnap{display:grid;gap:14px;grid-template-columns:minmax(0,1fr) minmax(0,1.15fr)}
@media (max-width:720px){.asnap{grid-template-columns:minmax(0,1fr)}}
/* Fixed 2x2 rather than auto-fit: a camera body like "ILCE-7RM3" needs the
   width, and auto-fit was collapsing it to four ~100px columns and clipping
   the name to "ILCE-1...". */
.acells{display:grid;gap:10px;grid-template-columns:repeat(2,minmax(0,1fr));
  align-content:start}
.acell{background:#0a0a10;border:1px solid #000;border-radius:6px;padding:10px 12px;
  box-shadow:0 2px 8px rgba(0,0,0,.6) inset,0 0 0 1px #1c1c24}
.acell b{display:block;font:600 17px/1.1 var(--disp);letter-spacing:-.01em;
  color:var(--ink);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.acell span{display:block;font:9px/1.4 var(--mono);letter-spacing:.15em;
  color:var(--faint);margin-top:5px;text-transform:uppercase}
.acell.size b{color:var(--cyan);text-shadow:0 0 20px rgba(58,208,255,.35)}
.acell.body b{font-size:14px;letter-spacing:0;font-family:var(--mono)}
.exif{position:relative;height:104px;background:#08080c;border:1px solid #000;
  border-radius:6px;overflow:hidden;
  box-shadow:0 2px 10px rgba(0,0,0,.7) inset,0 0 0 1px #1c1c24}
.exif:after{content:"";position:absolute;inset:0;pointer-events:none;
  background:linear-gradient(90deg,rgba(58,208,255,.05),transparent 45%)}
.exif-card{position:absolute;inset:0;padding:12px 14px;opacity:0;
  transition:opacity .5s ease-in-out}
.exif-card.on{opacity:1}
.exif-card .p{font:11px/1.45 var(--mono);color:var(--cyan);word-break:break-all}
.exif-card dl{display:grid;grid-template-columns:auto 1fr;gap:2px 10px;margin:8px 0 0}
.exif-card dt{font:9px/1.5 var(--mono);letter-spacing:.14em;color:var(--faint);
  text-transform:uppercase}
.exif-card dd{margin:0;font:10px/1.5 var(--mono);color:var(--dim)}
.exif-dots{display:flex;gap:4px;position:absolute;right:12px;bottom:10px}
.exif-dots i{width:4px;height:4px;border-radius:50%;background:#22222c;
  transition:background .4s}
.exif-dots i.on{background:var(--cyan);box-shadow:0 0 6px rgba(58,208,255,.7)}
.fmts{display:flex;height:5px;border-radius:3px;overflow:hidden;margin:12px 0 7px;
  background:#08080c;box-shadow:0 0 0 1px #1c1c24}
.fmts i{height:100%}
.fmtkey{display:flex;gap:14px;flex-wrap:wrap;font:9px/1.4 var(--mono);
  letter-spacing:.1em;color:var(--faint);text-transform:uppercase}
.fmtkey s{width:6px;height:6px;border-radius:2px;display:inline-block;
  margin-right:5px;text-decoration:none}

/* ---- controls ---- */
.rack{background:linear-gradient(#0e0e14,#0a0a10);border:1px solid #000;
  border-radius:8px;padding:15px 17px;margin:12px 0;position:relative;
  box-shadow:0 4px 12px rgba(0,0,0,.7) inset,
             0 1px 0 rgba(255,255,255,.055),0 0 0 1px #21212a}
.rack h3{margin:0 0 3px;font:600 12px/1.3 var(--disp);letter-spacing:.14em;
  text-transform:uppercase;color:var(--ink)}
.rack p{margin:0;font:11px/1.55 var(--mono);color:var(--dim);max-width:62ch}
.row{display:flex;gap:12px;align-items:center;flex-wrap:wrap}
.grow{flex:1 1 auto;min-width:0}
button{font:600 10px/1 var(--mono);letter-spacing:.14em;text-transform:uppercase;
  padding:10px 15px;border-radius:6px;color:var(--ink);cursor:pointer;
  background:linear-gradient(#22222b,#15151c);border:1px solid var(--edge2);
  box-shadow:0 1px 0 rgba(255,255,255,.06) inset,0 2px 5px rgba(0,0,0,.5)}
button:hover{border-color:#454556;color:#fff}
button:active{transform:translateY(1px);box-shadow:0 0 8px rgba(0,0,0,.7) inset}
button.go{background:linear-gradient(#0e4d63,#0a3546);border-color:#1d7fa3;
  color:#cdf3ff;box-shadow:0 0 20px rgba(58,208,255,.3),
  0 1px 0 rgba(255,255,255,.1) inset}
button.hot{background:linear-gradient(#5c3208,#3a2005);border-color:#a55c11;
  color:#ffd9a8}
button:disabled{opacity:.4;cursor:default}

/* ---- data ---- */
h2.sec{font:600 11px/1 var(--disp);letter-spacing:.2em;text-transform:uppercase;
  color:var(--dim);margin:26px 0 8px;display:flex;align-items:center;gap:10px}
h2.sec:after{content:"";flex:1;height:1px;background:var(--edge)}
h1{font:600 17px/1.2 var(--disp);letter-spacing:.05em;margin:0 0 3px}
.note{font:11px/1.6 var(--mono);color:var(--dim);margin:0 0 16px;max-width:76ch}
.card{background:linear-gradient(#0e0e14,#0a0a10);border:1px solid #000;
  border-radius:8px;padding:14px 16px;margin:0 0 11px;
  box-shadow:0 3px 10px rgba(0,0,0,.6) inset,
             0 1px 0 rgba(255,255,255,.05),0 0 0 1px #21212a}
.scroll{overflow-x:auto}
table{width:100%;border-collapse:collapse;font:11px/1.5 var(--mono)}
th{text-align:left;padding:7px 9px;border-bottom:1px solid var(--edge2);
  font:9px/1 var(--mono);letter-spacing:.16em;text-transform:uppercase;
  color:var(--faint);white-space:nowrap}
td{padding:7px 9px;border-bottom:1px solid #16161d;vertical-align:top;
  color:#c3cad8}
tr:hover td{background:rgba(58,208,255,.04)}
code{font-family:var(--mono);color:var(--cyan);word-break:break-all}
.d{color:var(--faint)}
.pill{display:inline-block;padding:2px 9px;border-radius:3px;
  font:600 9px/1.6 var(--mono);letter-spacing:.14em;border:1px solid currentColor}
.FILE{color:#4ade9a}.QUARANTINE{color:var(--orange)}.DUPLICATE{color:var(--faint)}
.CONFLICT{color:var(--amber)}.UNDATED{color:var(--amber)}.OTHER{color:var(--faint)}
.SKIP{color:var(--red)}
.ZERO{color:var(--orange)}.TRUNCATED{color:var(--amber)}
.HEADER_BAD{color:var(--amber)}.NO_MOOV{color:var(--amber)}.ERROR{color:var(--red)}
.alert{border-left:2px solid var(--red);padding-left:13px}
.caution{border-left:2px solid var(--amber);padding-left:13px}

/* ---- foot: slot + terminals ---- */
.foot{display:flex;align-items:center;gap:16px;margin-top:18px;padding-top:14px;
  border-top:1px solid var(--edge)}
.slot{width:120px;height:15px;border-radius:3px;background:#05050a;
  box-shadow:0 0 0 1px #000 inset,0 2px 4px rgba(0,0,0,.9) inset;
  position:relative;flex:none}
.slot:after{content:"";position:absolute;left:7px;top:4px;width:12px;height:7px;
  background:#2a2a34;border-radius:1px}
.term{width:17px;height:17px;border-radius:50%;flex:none;
  background:radial-gradient(circle at 34% 30%,#8b8f9c,#3a3d47 55%,#12131a);
  box-shadow:0 0 0 1px #000,0 1px 3px rgba(0,0,0,.8)}
.footlab{font:9px/1.4 var(--mono);letter-spacing:.14em;color:var(--faint);
  margin-left:auto;text-align:right}
@media (prefers-reduced-motion:reduce){.tri{transition:none}}
</style>

<div class=chassis>
  <div class=panel>
    <i class="screw tl"></i><i class="screw tr"></i>
    <i class="screw bl"></i><i class="screw br"></i>
    <div class=head>
      <div class=tris>
        <div>
          <div class="tri down tri-w off" id=t-ready></div>
          <div class=trilab>READY</div>
        </div>
        <div>
          <div class="tri up tri-a off" id=t-active></div>
          <div class=trilab>ACTIVE</div>
        </div>
        <div>
          <div class="tri up tri-o off" id=t-review></div>
          <div class=trilab>REVIEW</div>
        </div>
      </div>
      <div class=mark>
        <i class=s></i><i></i><i class=s></i>
      </div>
      <div class=dome></div>
      <div>
        <div class=wordmark>NZ&middot;INGEST</div>
        <div class=sub id=hostline>NODE ZERO / MAXIMILIAN</div>
      </div>
    </div>
    <div class=leds id=leds></div>
    <div id=app><div class=note>initialising&hellip;</div></div>
    <div class=foot>
      <div class=slot></div>
      <div class=term></div><div class=term></div>
      <div class=footlab id=footlab>&nbsp;</div>
    </div>
  </div>
</div>

<script>
const E=s=>String(s??'').replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
let view={kind:'home'};
// Cached so a periodic re-render never flashes the loading placeholder, and
// a signature of the state so an idle panel is not rebuilt for no reason.
let fwCache=null, asCache=null, homeSig='', homeDrawn=false;
async function api(u,o){const r=await fetch(u,o);return r.json()}

/* ---- panel head ------------------------------------------------------- */
function lamp(id,on){
  const e=document.getElementById(id); if(!e) return;
  e.classList.toggle('on',!!on); e.classList.toggle('off',!on);
}
function setHead(d,j){
  const scanning=j.running||(d.progress&&d.progress.phase&&
                             d.progress.phase!=='review'&&d.progress.total);
  const review=(d.batches||[]).some(b=>b.state==='review')||d.damaged>0;
  lamp('t-ready',!scanning);
  lamp('t-active',scanning);
  lamp('t-review',review);
  // 12 lamps: a scan's progress when one is running, otherwise the archive's
  // standing state - lit cyan for clean, red where something wants a look.
  const N=10; let lit=0, kind='c';
  if(j.running&&j.total){ lit=Math.round(N*j.done/j.total); kind='a'; }
  else if(d.manifest){ lit=N; kind=review?'r':'c'; }
  let h='';
  for(let i=0;i<N;i++) h+=`<i class="led ${i<lit?kind:''}"></i>`;
  h+=`<span class=ledlab>${j.running?E(j.phase).toUpperCase()
      :(d.manifest?d.manifest.toLocaleString()+' INDEXED':'NO MANIFEST')}</span>`;
  h+=`<span class=keys><i class="${j.running?'y':''}"></i>
      <i class="${review?'y':''}"></i><i class="${d.waiting?'w':''}"></i>
      <i></i></span>`;
  document.getElementById('leds').innerHTML=h;
  document.getElementById('footlab').innerHTML=
    (d.readonly?'READ-ONLY &middot; ':'')+'DROP '+E(d.drop);
}
function mod(n,l,cls,click){
  return `<div class="mod ${cls||''} ${click?'hit':''}"${click?` onclick="${click}"`:''}>
    <b>${n}</b><span>${E(l)}</span></div>`;
}

/* ---- telemetry traces -------------------------------------------------- */
/* Channel order is paint order: the one you care about most is drawn last so
   the others cannot bury it. Each channel autoscales against its own rolling
   peak with a floor, because a scan that never exceeds 4 MB/s should still
   show its shape rather than a flat line along the bottom. */
const TR_CH=[
  {k:'mem',  col:'#5d8fa8', w:1.2, floor:128, fmt:v=>Math.round(v)+' mb'},
  {k:'io',   col:'#8fd8f2', w:1.3, floor:10,  fmt:v=>v.toFixed(1)+' mb/s'},
  {k:'cpu',  col:'#e8f6ff', w:1.3, floor:100, fmt:v=>v.toFixed(1)+'%'},
  {k:'rate', col:'#3ad0ff', w:2.0, floor:10,  fmt:v=>Math.round(v)+'/s'},
];
const TR={cap:240, buf:[], on:false, dmg:null, found:0};
function traceSample(j,d){
  const tot=(d.damaged|0)+(d.unreadable|0);
  /* The first sample of a scan only establishes a baseline. The manifest
     already carries damage from previous runs, and counting all of it as
     found-now would stripe the panel red at t=0. */
  const hit=TR.dmg!==null&&tot>TR.dmg;
  if(hit) TR.found+=tot-TR.dmg;
  TR.dmg=tot;
  TR.buf.push({t:Date.now(), rate:j.rate||0, cpu:j.cpu||0,
               mem:j.mem_mb||0, io:j.io_mb||0, hit:hit});
  if(TR.buf.length>TR.cap) TR.buf.shift();
  traceLabels();
}
/* home() rebuilds the whole in-flight panel every 7s while a scan runs, which
   resets these back to their placeholder markup. Repainting them from the
   buffer whenever the canvas is (re)created closes that gap - otherwise the
   readouts blank for up to a second on every redraw. */
function traceLabels(){
  if(!TR.buf.length) return;
  const last=TR.buf[TR.buf.length-1];
  for(const c of TR_CH){
    const el=document.getElementById('tr-'+c.k);
    if(el) el.textContent=c.fmt(last[c.k]);
  }
  const dm=document.getElementById('tr-dmg');
  if(dm) dm.textContent=TR.found.toLocaleString();
}
function startTrace(cv){
  if(!cv) return;
  traceLabels();
  if(cv.__on) return; cv.__on=true;
  const ctx=cv.getContext('2d');
  const still=matchMedia('(prefers-reduced-motion: reduce)').matches;
  const dpr=Math.min(2,window.devicePixelRatio||1);
  let w,h;
  const size=()=>{w=cv.width=cv.clientWidth*dpr; h=cv.height=cv.clientHeight*dpr};
  size(); try{new ResizeObserver(size).observe(cv)}catch(e){}
  function frame(){
    if(!cv.isConnected){cv.__on=false;return}
    ctx.fillStyle='#05050a'; ctx.fillRect(0,0,w,h);
    const step=w/(TR.cap-1);
    /* The graticule stays put. Sliding it along with the traces looks right
       for a fraction of a second and then snaps back a whole division once a
       second, which reads as a stutter. */
    ctx.lineWidth=dpr; ctx.strokeStyle='rgba(58,208,255,.055)'; ctx.beginPath();
    for(let x=w;x>=0;x-=step*8){ ctx.moveTo(x,0); ctx.lineTo(x,h); }
    for(let i=1;i<6;i++){ const y=Math.round(h*i/6); ctx.moveTo(0,y); ctx.lineTo(w,y); }
    ctx.stroke();
    ctx.strokeStyle='rgba(58,208,255,.10)'; ctx.beginPath();
    ctx.moveTo(0,Math.round(h/2)); ctx.lineTo(w,Math.round(h/2)); ctx.stroke();

    const b=TR.buf, n=b.length;
    if(n<2){ requestAnimationFrame(frame); return; }
    /* Samples land at 1 Hz. Sliding by the fraction of a second elapsed since
       the last one turns a once-a-second jump into a continuous drift: when
       the next sample arrives n is unchanged and frac resets, so every point
       moves left by exactly one step. This only smooths where existing points
       are drawn - no values are invented between them. */
    const frac=still?0:Math.max(0,Math.min(1,(Date.now()-b[n-1].t)/1000));
    const xOf=i=>w-(n-1-i+frac)*step;

    for(let i=0;i<n;i++){
      if(!b[i].hit) continue;
      const x=xOf(i);
      const g=ctx.createLinearGradient(0,0,0,h);
      g.addColorStop(0,'rgba(255,59,59,.05)'); g.addColorStop(1,'rgba(255,59,59,.45)');
      ctx.strokeStyle=g; ctx.lineWidth=dpr*1.3;
      ctx.beginPath(); ctx.moveTo(x,0); ctx.lineTo(x,h); ctx.stroke();
    }

    const pad=h*0.10;
    for(const c of TR_CH){
      let mx=c.floor;
      for(let i=0;i<n;i++) if(b[i][c.k]>mx) mx=b[i][c.k];
      const pts=[];
      for(let i=0;i<n;i++)
        pts.push([xOf(i), h-pad-(h-2*pad)*Math.min(1,b[i][c.k]/mx)]);
      /* Three cheap strokes rather than shadowBlur, which is applied per
         stroke and is the one thing here that would actually cost frames. */
      const draw=(lw,al)=>{
        ctx.beginPath(); ctx.moveTo(pts[0][0],pts[0][1]);
        for(let i=1;i<n-1;i++){
          const mxp=(pts[i][0]+pts[i+1][0])/2, myp=(pts[i][1]+pts[i+1][1])/2;
          ctx.quadraticCurveTo(pts[i][0],pts[i][1],mxp,myp);
        }
        ctx.lineTo(pts[n-1][0],pts[n-1][1]);
        ctx.strokeStyle=c.col; ctx.globalAlpha=al;
        ctx.lineWidth=dpr*lw; ctx.lineJoin='round'; ctx.lineCap='round'; ctx.stroke();
      };
      draw(c.w*3.4,0.09); draw(c.w*1.9,0.18); draw(c.w,1);
      ctx.globalAlpha=1;
      const p=pts[n-1];
      ctx.fillStyle=c.col; ctx.beginPath();
      ctx.arc(p[0],p[1],dpr*c.w*1.15,0,6.283); ctx.fill();
    }
    requestAnimationFrame(frame);
  }
  requestAnimationFrame(frame);
}

/* ---- accretion disc --------------------------------------------------- */
let warpRate=0, warpV=0;
function startWarp(cv){
  if(!cv||cv.__on) return; cv.__on=true;
  const ctx=cv.getContext('2d');
  const still=matchMedia('(prefers-reduced-motion: reduce)').matches;
  const dpr=Math.min(2,window.devicePixelRatio||1);
  let w,h,cx,cy;
  const size=()=>{w=cv.width=cv.clientWidth*dpr;h=cv.height=cv.clientHeight*dpr;
                  cx=w/2;cy=h/2};
  size(); try{new ResizeObserver(size).observe(cv)}catch(e){}
  const st=[];
  for(let i=0;i<210;i++) st.push({a:Math.random()*6.283,r:Math.random(),
    z:0.25+Math.random()*0.75});
  function frame(){
    if(!cv.isConnected){cv.__on=false;return}
    const R=Math.hypot(cx,cy), v=warpV;
    ctx.fillStyle='#05050a'; ctx.fillRect(0,0,w,h);
    ctx.save(); ctx.translate(cx,cy); ctx.scale(1,0.24);
    for(let k=0;k<3;k++){
      ctx.beginPath(); ctx.arc(0,0,R*(0.30+k*0.11),0,6.283);
      ctx.strokeStyle='rgba(58,208,255,'+(0.20-k*0.05)+')';
      ctx.lineWidth=dpr*(2.4-k*0.6); ctx.stroke();
    }
    ctx.restore();
    for(const s of st){
      if(!still) s.r+=(0.0015+v*0.055)*s.z;
      if(s.r>1){s.r=Math.random()*0.07;s.a=Math.random()*6.283}
      const r0=s.r*R, len=R*v*0.11*s.z;
      const c=Math.cos(s.a), n=Math.sin(s.a);
      ctx.beginPath(); ctx.moveTo(cx+c*r0,cy+n*r0);
      ctx.lineTo(cx+c*(r0+len),cy+n*(r0+len));
      ctx.strokeStyle='rgba(228,244,255,'+(0.18+0.7*s.z*(0.25+s.r))+')';
      ctx.lineWidth=dpr*s.z*1.15; ctx.stroke();
    }
    const g=ctx.createRadialGradient(cx,cy,0,cx,cy,R*0.24);
    g.addColorStop(0,'rgba(255,255,255,0.92)');
    g.addColorStop(0.36,'rgba(58,208,255,0.22)');
    g.addColorStop(1,'rgba(0,0,0,0)');
    ctx.fillStyle=g; ctx.beginPath(); ctx.arc(cx,cy,R*0.24,0,6.283); ctx.fill();
    requestAnimationFrame(frame);
  }
  requestAnimationFrame(frame);
}
function ago(iso){
  if(!iso) return '';
  const t=Date.parse(iso); if(isNaN(t)) return '';
  const s=Math.max(0,(Date.now()-t)/1000|0);
  if(s<90) return s+'s ago';
  const m=s/60|0; if(m<90) return m+'m ago';
  const h=m/60|0; if(h<36) return h+'h ago';
  return (h/24|0)+'d ago';
}
function hms(s){s=Math.max(0,s|0);const h=s/3600|0,m=(s%3600)/60|0;
  return h?h+'h '+m+'m':(m?m+'m '+(s%60)+'s':s+'s')}
function vpanel(){
  return `<div class=vp><canvas id=warp></canvas>
    <div class=act><button class=hot onclick="act('scan_stop',0)">halt</button></div>
  </div>
  <div class=inflight>
    <div class=prog>
      <div class=row><div class=label>Phase</div><div class=val id=phase>—</div></div>
      <div class=row><div class=label>Total</div><div class=val id=prog-total>—</div></div>
      <div class=row><div class=label>Done</div><div class=val id=prog-done>—</div></div>
      <div class=row><div class=label>Speed</div><div class=val id=prog-speed>—</div></div>
      <div class=row><div class=label>ETA</div><div class=val id=prog-eta>—</div></div>
    </div>
    <div class=file-box id=curfile></div>
    <div class=stats-head>SYSTEM STATUS</div>
    <div class=trace><canvas id=trace></canvas></div>
    <div class=trlegend>
      <span><s style="background:#3ad0ff"></s>rate <b id=tr-rate>&mdash;</b></span>
      <span><s style="background:#e8f6ff"></s>cpu <b id=tr-cpu>&mdash;</b></span>
      <span><s style="background:#8fd8f2"></s>i/o <b id=tr-io>&mdash;</b></span>
      <span><s style="background:#5d8fa8"></s>mem <b id=tr-mem>&mdash;</b></span>
      <span><s style="background:#ff3b3b"></s>damage <b id=tr-dmg>0</b></span>
    </div>
    <div class=stats>
      <div class=stat><div class=label>Speed</div><div class=val id=speed>—</div><div class=bar id=speedbar></div></div>
      <div class=stat><div class=label>CPU</div><div class=val id=cpustat>—</div><div class=bar id=cpubar></div></div>
      <div class=stat><div class=label>Memory</div><div class=val id=memstat>—</div><div class=bar id=membar></div></div>
      <div class=stat><div class=label>I/O</div><div class=val id=iostat>—</div><div class=bar id=iobar></div></div>
    </div>
  </div>`;
}
async function jobTick(){
  let j,d; try{ j=await api('/api/job'); d=await api('/api/state'); }catch(e){return}
  warpRate=j.running?(j.rate||0):0;
  const walking=/walk|manifest/i.test(j.phase||'');
  warpV=!j.running?0:(walking?0:Math.max(0.14,Math.min(1,warpRate/600)));
  // A new scan starts a new trace. Carrying the previous run's history over
  // would splice two unrelated timelines into one curve.
  if(j.running&&!TR.on){ TR.on=true; TR.buf.length=0; TR.dmg=null; TR.found=0; }
  else if(!j.running){ TR.on=false; }
  setHead(d,j);
  // Check if in-flight panel exists (scan running)
  const inflight=document.getElementById('phase');
  if(j.running&&view.kind==='home'&&!inflight) return home();
  // A scan just finished: the manifest moved, so the snapshot's numbers are
  // stale by definition. Drop the cache and let home() refetch once.
  if(!j.running&&inflight){ asCache=null; return home(); }
  if(j.running&&inflight){
    // Update progress section rows
    const ph=document.getElementById('phase');
    if(ph) ph.textContent=E(j.phase||'').toUpperCase();
    const progTotal=document.getElementById('prog-total');
    if(progTotal) progTotal.textContent=j.total?j.total.toLocaleString():'—';
    const progDone=document.getElementById('prog-done');
    if(progDone){
      let dtext=j.done.toLocaleString();
      if(j.pct) dtext+=' ('+j.pct+'%)';
      progDone.textContent=dtext;
    }
    const progSpeed=document.getElementById('prog-speed');
    if(progSpeed) progSpeed.textContent=Math.round(j.rate||0)+' files/sec';
    const progEta=document.getElementById('prog-eta');
    if(progEta){
      if(j.eta) progEta.textContent=hms(j.eta)+' remaining';
      else progEta.textContent='—';
    }
    // Update current file and system stats
    const f=document.getElementById('curfile');
    if(f) f.textContent=j.file||'(scanning)';
    const speed=document.getElementById('speed');
    const speedVal=Math.round(j.rate||0);
    if(speed) speed.textContent=speedVal+' files/sec';
    const cpustat=document.getElementById('cpustat');
    const cpuVal=j.cpu||0;
    if(cpustat) cpustat.textContent=cpuVal.toFixed(1)+'%';
    const memstat=document.getElementById('memstat');
    const memVal=j.mem_mb||0;
    if(memstat) memstat.textContent=memVal+' MB';
    const iostat=document.getElementById('iostat');
    const ioVal=j.io_mb||0;
    if(iostat) iostat.textContent=ioVal.toFixed(1)+' MB/s';
    // Set bar widths (speed: 0-120 files/sec, cpu: 0-100%, mem: scale to reasonable max, io: 0-300 MB/s)
    setBarWidth('speedbar', Math.min(100, speedVal/120*100));
    setBarWidth('cpubar', cpuVal);
    setBarWidth('membar', Math.min(100, memVal/512*100));
    setBarWidth('iobar', Math.min(100, ioVal/300*100));
    traceSample(j,d);
    startTrace(document.getElementById('trace'));
    startWarp(document.getElementById('warp'));
  }
}
function setBarWidth(id, pct){
  const bar=document.getElementById(id);
  if(bar) bar.style.setProperty('--w',Math.max(0,Math.min(100,pct))+'%');
}

/* ---- views ------------------------------------------------------------ */
async function home(){
  const d=await api('/api/state'), j=await api('/api/job');
  view={kind:'home'};
  // Re-rendering an idle panel every few seconds made the firmware rack
  // flash. Only redraw when something a person would notice has changed.
  const sig=JSON.stringify([d.manifest,d.hashed,d.damaged,d.unreadable,
    d.waiting,d.readonly,j.running,j.note,
    (d.batches||[]).map(b=>[b.id,b.state,b.note])]);
  if(homeDrawn&&sig===homeSig&&(!j.running||document.getElementById('phase'))) return;
  homeSig=sig; homeDrawn=true;
  let h='';
  h+=`<div class=mods>
    ${mod(d.manifest.toLocaleString(),'files indexed','cy')}
    ${mod(d.hashed.toLocaleString(),'with hashes')}
    ${mod(d.damaged.toLocaleString(),'need review','bad','dmg()')}
    ${d.unreadable?mod(d.unreadable.toLocaleString(),'unreadable','warn','dmg()'):''}
    ${mod(d.waiting,'in drop folder',d.waiting?'warn':'')}
  </div>`;
  if(j.running){ h+=vpanel(); }
  else{
    h+=`<div class=rack><div class=row>
      <div class=grow><h3>Archive scan</h3>
      <p>Builds the manifest that incoming cards are checked against.
      Resume skips files that have not changed.</p></div>
      <button class=go onclick="act('scan_start',0)">scan</button>
      <button onclick="act('scan_start',0,'',1)">+ hashes</button>
      <button onclick="act('scan_fresh',0)">full rescan</button>
    </div></div>`;
    if(d.waiting) h+=`<div class=rack><div class=row>
      <div class=grow><h3>Drop folder</h3>
      <p>${d.waiting} file(s) waiting. A batch forms on its own once copying
      stops, or force it now.</p></div>
      <button class=go onclick="act('drop_now',0)">scan drop</button>
    </div></div>`;
  }
  // Same rule as the firmware rack below: resolve the fetch *before* building
  // the section so the panel enters the DOM once, at its final height.
  if(!asCache){ try{ asCache=await api('/api/archive-stats'); }catch(e){ asCache=null; } }
  if(asCache) h+=`<h2 class=sec>Archive snapshot</h2>
    <div class=rack>${asHtml(asCache)}</div>`;
  if(j.note) h+=`<div class="card d">LAST SCAN &nbsp;${E(j.note)}</div>`;
  h+=`<h2 class=sec>Batches</h2>`;
  if(!(d.batches||[]).length)
    h+=`<div class="card d">No batches. Copy a card into the drop folder.</div>`;
  for(const b of d.batches){
    const hot=b.state==='review';
    h+=`<div class=card><div class=row>
      <div class=grow><b>#${b.id}</b> &nbsp;${E(b.name)}
      <div class=d>${E(b.state)}${b.note?' &middot; '+E(b.note):''}</div></div>
      <button class="${hot?'go':''}" onclick="show(${b.id})">${hot?'review':'open'}</button>
    </div></div>`;
  }
  // Firmware used to be stitched in after the fact: write a "loading…"
  // placeholder, then swap it for the real table once the fetch lands. Two
  // DOM writes of different heights is exactly what "flickers and shrinks"
  // means, and it happened on every redraw, not just the first. Fetch (or
  // reuse the cache) *before* building this section so the whole panel goes
  // into the DOM once, at its final height. Auto-check for updates on first load.
  if(!fwCache){ try{ fwCache=await api('/api/firmware?check=1'); }catch(e){ fwCache=null; } }
  h+=`<h2 class=sec>Firmware</h2><div class=rack id=fwrack>${fwCache?fwHtml(fwCache):
    `<div class=row><div class=grow><h3>Version ${E(j.version||'')}</h3>
    <p>loading&hellip;</p></div></div>`}</div>`;
  document.getElementById('app').innerHTML=h;
  setHead(d,j);
  startExif();
  if(j.running){
    const walking=/walk|manifest/i.test(j.phase||'');
    warpV=walking?0:Math.max(0.14,Math.min(1,(j.rate||0)/600));
    const box=document.getElementById('jobtxt');
    if(box) box.innerHTML=jobText(j);
  }
  startTrace(document.getElementById('trace'));
  startWarp(document.getElementById('warp'));
}

/* ---- imperial spinner --------------------------------------------------
   This was pasted inline twice, and in both copies every arc had endpoints
   off its own declared radius - the worst claimed r=90 with endpoints at
   77.8 and 94.9. SVG answers an impossible arc by growing the radius until
   the chord fits and then re-centring it, so that path orbited a point
   nowhere near the core and read as a tilted blob rather than a ring. One
   orbit dot sat at r=75 among three at r=85 for the same reason: the
   coordinates were typed by hand instead of computed. They are computed
   here, from one centre, so concentricity is not something that can rot.

   The three orbits are spaced so the counter-rotating rings cannot cross:
   ring1 occupies 74-82, the dots 85-93, ring2 95-101, and the outermost
   glow lands at ~107 inside a 110 half-box. ring2 is the outer one - it
   used to share r=90 with ring1 and swept straight through it. */
const IMP={c:110, r1:78, rd:89, r2:98};
function impPt(r,deg){
  const t=deg*Math.PI/180;
  return [(IMP.c+r*Math.cos(t)).toFixed(2),(IMP.c+r*Math.sin(t)).toFixed(2)];
}
function impArc(r,a0,a1){
  const [x0,y0]=impPt(r,a0), [x1,y1]=impPt(r,a1);
  return `M${x0},${y0} A${r},${r} 0 ${Math.abs(a1-a0)>180?1:0},${a1>a0?1:0} ${x1},${y1}`;
}
function imperialSvg(){
  const arc=(r,a0,a1,w,col,op)=>`<path d="${impArc(r,a0,a1)}" stroke="${col}" stroke-width="${w}" fill="none" stroke-linecap="round"${op?` opacity="${op}"`:''} filter="url(#glow)"/>`;
  const dot=a=>{ const [x,y]=impPt(IMP.rd,a);
    return `<circle cx="${x}" cy="${y}" r="4" fill="#ff6666" filter="url(#glow)"/>`; };
  const tick=a=>{ const [x0,y0]=impPt(45,a), [x1,y1]=impPt(68,a);
    return `<line x1="${x0}" y1="${y0}" x2="${x1}" y2="${y1}" stroke="#ff3333" stroke-width="5" stroke-linecap="round" filter="url(#glow)"/>`; };
  return `<div class=row style="justify-content:center;padding:60px 0">
    <svg class=imperial viewBox="0 0 220 220" style="width:200px;height:200px">
      <defs>
        <!-- filterUnits is not decoration. The default is objectBoundingBox,
             and a horizontal or vertical line has a zero-area bbox, which
             makes the filter region empty and drops the element entirely.
             That is why four of the eight tick marks have never once been
             drawn. A user-space region is independent of the bbox. -->
        <filter id="glow" filterUnits="userSpaceOnUse"
                x="0" y="0" width="220" height="220">
          <feGaussianBlur stdDeviation="3" result="b"/>
          <feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge>
        </filter>
        <style>
          @keyframes spin1{from{transform:rotate(0)}to{transform:rotate(360deg)}}
          @keyframes spin2{from{transform:rotate(0)}to{transform:rotate(-360deg)}}
          @keyframes spin3{from{transform:rotate(0)}to{transform:rotate(360deg)}}
          @keyframes pulse{0%,100%{r:16px;opacity:.6}50%{r:22px;opacity:1}}
          .ring1{animation:spin1 6s linear infinite;transform-origin:110px 110px}
          .ring2{animation:spin2 10s linear infinite;transform-origin:110px 110px}
          .ring3{animation:spin3 8s linear infinite;transform-origin:110px 110px}
          .pulse{animation:pulse 2s ease-in-out infinite}
          @media (prefers-reduced-motion:reduce){
            .ring1,.ring2,.ring3,.pulse{animation:none}}
        </style>
      </defs>
      <circle cx="110" cy="110" r="50" fill="none" stroke="#ff5555" stroke-width="2" opacity="0.5" filter="url(#glow)"/>
      <circle cx="110" cy="110" r="40" fill="none" stroke="#ff3333" stroke-width="1.5" opacity="0.6" filter="url(#glow)"/>
      ${[0,45,90,135,180,225,270,315].map(tick).join('')}
      <g class="ring1">${arc(IMP.r1,-90,-35,8,'#ff3333')}${arc(IMP.r1,90,145,8,'#ff3333')}</g>
      <g class="ring3">${dot(0)}${dot(90)}${dot(180)}${dot(270)}</g>
      <g class="ring2">${arc(IMP.r2,0,54,6,'#ff4444','0.8')}${arc(IMP.r2,180,234,6,'#ff4444','0.8')}</g>
      <circle cx="110" cy="110" r="18" class="pulse" fill="#ff3333" opacity="0.5" filter="url(#glow)"/>
      <circle cx="110" cy="110" r="10" fill="#000"/>
    </svg>
  </div>`;
}

async function fw(check){
  const el=document.getElementById('fwrack'); if(!el) return;
  if(!check&&fwCache) return fwRender(fwCache);
  if(check){
    el.innerHTML=`${imperialSvg()}`;
  }
  const start=Date.now();
  let f; try{ f=await api('/api/firmware'+(check?'?check=1':'')); }
  catch(e){ return; }
  // Enforce 5-second minimum display
  const elapsed=Date.now()-start;
  if(check&&elapsed<5000) await new Promise(r=>setTimeout(r,5000-elapsed));
  fwCache=f; fwRender(f);
}
// Pure string builder - no DOM access - so home() can lay the firmware
// section into its own single innerHTML write instead of writing a
// placeholder now and the real table a moment later.
/* ---- archive snapshot ------------------------------------------------- */
/* Binary units: these are file sizes on a ZFS pool, and the pool reports
   TiB. Showing decimal TB here and TiB in TrueNAS for the same bytes is how
   you end up not trusting either number. */
function fmtBytes(b){
  b=Number(b)||0;
  const u=['B','KiB','MiB','GiB','TiB','PiB'];
  let i=0; while(b>=1024&&i<u.length-1){ b/=1024; i++; }
  return (i===0?b:b.toFixed(b<10?2:1))+' '+u[i];
}
const FMT_COLOR={tiff:'#3ad0ff',jpeg:'#ffc63f',bmff:'#ff7d24',png:'#7bd88f',
  other:'#4b5060'};
function fmtDT(s){
  // EXIF DateTimeOriginal is "YYYY:MM:DD hh:mm:ss" - only the date half is
  // colon-separated in a way Date() misreads, so rewrite rather than parse.
  const m=/^(\d{4}):(\d{2}):(\d{2})[ T](.+)$/.exec(String(s||''));
  return m?`${m[1]}-${m[2]}-${m[3]} ${m[4]}`:(s||'—');
}
function asHtml(a){
  const named=(a.models||[]).filter(m=>m.model);
  const top=named.length?named[0]:null;
  const cells=[
    [a.file_count.toLocaleString(),'files',''],
    [fmtBytes(a.total_size),'total size','size'],
    [(a.file_count?fmtBytes(a.total_size/a.file_count):'—'),'average',''],
    [top?E(top.model):'—','top body','body'],
  ].map(([n,l,c])=>`<div class="acell ${c}"><b>${n}</b><span>${E(l)}</span></div>`)
   .join('');

  const fmts=Object.entries(a.formats||{}).filter(([k,v])=>v>0);
  const ftot=fmts.reduce((s,[,v])=>s+v,0)||1;
  const bar=fmts.map(([k,v])=>`<i style="width:${(v/ftot*100).toFixed(2)}%;
    background:${FMT_COLOR[k]||FMT_COLOR.other}"></i>`).join('');
  const key=fmts.map(([k,v])=>`<span><s style="background:${
    FMT_COLOR[k]||FMT_COLOR.other}"></s>${E(k||'unknown')} ${
    (v/ftot*100).toFixed(0)}%</span>`).join('');

  const s=a.samples||[];
  const cards=s.map((x,i)=>`<div class="exif-card${i?'':' on'}">
    <div class=p>${E(x.path)}</div>
    <dl><dt>body</dt><dd>${E(x.model||'—')}</dd>
    <dt>shot</dt><dd>${E(fmtDT(x.dt))}</dd>
    <dt>size</dt><dd>${fmtBytes(x.size)}${x.fmt?' &middot; '+E(x.fmt):''}</dd></dl>
  </div>`).join('');
  const dots=s.map((_,i)=>`<i class="${i?'':'on'}"></i>`).join('');

  return `<div class=asnap>
    <div><div class=acells>${cells}</div>
      <div class=fmts>${bar}</div><div class=fmtkey>${key}</div></div>
    <div class=exif id=exif>${s.length?cards+`<div class=exif-dots>${dots}</div>`
      :`<div class="exif-card on"><div class=p>No dated frames with a camera
        body yet — run a scan to populate the manifest.</div></div>`}</div>
  </div>`;
}
/* One interval for the life of the page. It re-reads the DOM each tick, so a
   home() redraw that replaces the cards cannot leave a second timer behind
   driving elements that no longer exist. */
let exifTimer=null;
function startExif(){
  if(exifTimer||matchMedia('(prefers-reduced-motion: reduce)').matches) return;
  exifTimer=setInterval(()=>{
    const box=document.getElementById('exif'); if(!box) return;
    const cards=box.querySelectorAll('.exif-card');
    const dots=box.querySelectorAll('.exif-dots i');
    if(cards.length<2) return;
    let i=0; cards.forEach((c,n)=>{ if(c.classList.contains('on')) i=n; });
    const j=(i+1)%cards.length;
    cards[i].classList.remove('on'); cards[j].classList.add('on');
    if(dots.length===cards.length){
      dots[i].classList.remove('on'); dots[j].classList.add('on');
    }
  },4000);
}

function fwHtml(f){
  let h=`<div class=row><div class=grow>
    <h3>Version ${E(f.version)}</h3>
    <p>${f.base?'Source '+E(f.base):
      'No update source configured. Set <code>NZ_UPDATE_BASE</code> to a raw '+
      'file base URL — a GitHub repo <code>/app</code> folder, say.'}
    ${f.writable?'':' <b>Code directory is read-only</b> — remove the '+
      '<code>:ro</code> from the /app volume to allow updates.'}</p></div>
    ${f.base?`<button onclick="fw(true)">check</button>`:''}
    ${f.remote&&Object.values(f.remote).some(v=>v.changed)&&f.writable
      ?`<button class=go onclick="act('fw_install',0)">install &amp; restart</button>`:''}
  </div>`;
  if(f.error) h+=`<div class="card alert" style="margin-top:12px">
    ${E(f.error)}</div>`;
  const inst=(f.installed&&f.installed.commits)||{};
  h+=`<div class="scroll" style="margin-top:12px"><table>
    <tr><th>file</th><th>bytes</th><th>rev</th><th>last change</th>
    ${f.remote?'<th>state</th>':''}</tr>
    ${(f.remote?Object.entries(f.remote):Object.entries(f.local)).map(([k,v])=>{
      const c=(f.commits&&f.commits[k])||inst[k]||null;
      const stale=c&&inst[k]&&f.commits&&f.commits[k]
        &&inst[k].rev!==f.commits[k].rev;
      return `<tr><td><code>${E(k)}</code>
        <div class=d>${E(v.sha)}</div></td>
        <td>${v.bytes.toLocaleString()}</td>
        <td>${c?`<code>${E(c.rev)}</code>${stale
          ?`<div class=d>have ${E(inst[k].rev)}</div>`:''}`:'<span class=d>—</span>'}</td>
        <td>${c?`${E(c.msg||'')}<div class=d>${E(ago(c.when))}${
          c.who?' · '+E(c.who):''}</div>`:'<span class=d>press check</span>'}</td>
        ${f.remote?`<td>${v.changed
          ?'<span class=pill style="color:var(--amber)">NEW</span>'
          :'<span class=d>same</span>'}</td>`:''}</tr>`}).join('')}
    </table></div>`;
  if(f.installed&&f.installed.at) h+=`<p class="note d" style="margin:10px 0 0">
    Installed ${E(ago(new Date(f.installed.at*1000).toISOString()))} —
    version ${E(f.installed.version||'?')}</p>`;
  if(f.remote&&!Object.values(f.remote).some(v=>v.changed))
    h+=`<p class="note d" style="margin:12px 0 0">Already up to date.</p>`;
  return h;
}
function fwRender(f){
  const el=document.getElementById('fwrack'); if(!el) return;
  el.innerHTML=fwHtml(f);
}
async function dmg(){
  const d=await api('/api/damaged'); view={kind:'damaged'}; homeDrawn=false;
  let h=`<h1>Files that need a look</h1>
  <p class=note>Archive files that did not pass a structural check. A check is
  a judgement, not a fact &mdash; some of these will be fine, and the verdict
  tells you what to weigh.</p>
  <p><button onclick=home()>&larr; panel</button>
  <a href="/api/damaged.tsv"><button>export tsv</button></a></p>`;
  if(!d.groups.length) h+=`<div class="card d">Nothing flagged.</div>`;
  if(d.unreadable) h+=`<div class="card alert"><b>${d.unreadable.toLocaleString()}
  file(s) could not be read.</b> That is not damage &mdash; their condition is
  <i>unknown</i>. A scan that silently skips files and reports success is worse
  than no scan: re-run as a user that can read the whole archive.</div>`;
  if(d.folders&&d.folders.length){
    h+=`<h2 class=sec>Distribution</h2><div class="card scroll"><table>
    <tr><th>top-level folder</th><th>flagged</th></tr>`;
    for(const f of d.folders)
      h+=`<tr><td><code>${E(f[0])}</code></td><td>${f[1]}</td></tr>`;
    h+=`</table></div>`;
  }
  for(const g of d.groups){
    h+=`<h2 class=sec><span class="pill ${g.verdict}">${g.verdict}</span>
      ${g.count.toLocaleString()}</h2>
      <p class=note>${E(d.help[g.verdict]||'')}</p>`;
    if(!g.items.length){h+=`<div class="card d">not listed</div>`;continue}
    h+=`<div class="card scroll"><table><tr><th>path</th><th>bytes</th>
      <th>fmt</th><th>camera</th></tr>`;
    for(const it of g.items)
      h+=`<tr><td><code>${E(it.path)}</code></td>
      <td>${(it.size||0).toLocaleString()}</td><td>${E(it.fmt||'')}</td>
      <td>${E(it.model||'')}${it.dt?`<div class=d>${E(it.dt)}</div>`:''}</td></tr>`;
    h+=`</table></div>`;
    if(g.items.length<g.count) h+=`<p class="note d">showing ${g.items.length}
      of ${g.count.toLocaleString()} &mdash; export the TSV for all of them</p>`;
  }
  document.getElementById('app').innerHTML=h;
}

async function show(id){
  const d=await api('/api/batch/'+id); view={kind:'batch',id}; homeDrawn=false;
  let h=`<h1>Batch #${d.id}</h1><p class=note>${E(d.name)} &middot; ${E(d.state)}</p>
  <p><button onclick=home()>&larr; panel</button></p>`;
  if(d.blocked) h+=`<div class="card alert"><b>${d.counts.SKIP||0} file(s) could
  not be read.</b> Usually permissions. Fix that before trusting this batch.</div>`;
  h+=`<div class=mods>`;
  for(const k of ['FILE','QUARANTINE','CONFLICT','DUPLICATE','UNDATED','OTHER','SKIP'])
    if(d.counts[k]) h+=mod(d.counts[k],k.toLowerCase(),
      k==='FILE'?'cy':(k==='SKIP'?'bad':(k==='QUARANTINE'?'bad':'warn')));
  h+=`</div>`;
  if(d.state==='review') h+=`<div class=rack><div class=row>
    <div class=grow><h3>Authorise</h3><p>Damaged files, duplicates and conflicts
    are parked, never deleted. Every move is journalled and reversible.</p></div>
    <button onclick="act('approve_all',${d.id})">approve all</button>
    <button class=go onclick="act('execute',${d.id})">execute</button></div></div>`;
  if(d.state==='done'||d.state==='failed') h+=`<div class=rack><div class=row>
    <div class=grow><h3>Executed</h3><p>${d.journal} move(s) journalled.</p></div>
    <button class=hot onclick="act('undo',${d.id})">undo batch</button></div></div>`;
  for(const g of d.groups){
    h+=`<h2 class=sec><span class="pill ${g.action}">${g.action}</span>
      ${g.items.length}</h2><p class=note>${E(g.help)}</p>`;
    if(g.action!=='SKIP'&&d.state==='review') h+=`<p>
      <button onclick="act('approve',${d.id},'${g.action}',1)">approve group</button>
      <button onclick="act('approve',${d.id},'${g.action}',0)">skip group</button></p>`;
    h+=`<div class="card scroll"><table><tr><th>from</th><th>to</th>
      <th>why</th><th>on</th></tr>`;
    for(const it of g.items)
      h+=`<tr><td><code>${E(it.rel)}</code>
      <div class=d>${(it.size||0).toLocaleString()} B${it.model?' · '+E(it.model):''}${it.dt?' · '+E(it.dt):''}</div></td>
      <td><code>${E(it.dest_short)}</code></td>
      <td>${E(it.reason)}${it.dup_of?`<div class=d><code>${E(it.dup_of)}</code></div>`:''}</td>
      <td>${it.approved?'yes':'<span class=d>no</span>'}</td></tr>`;
    h+=`</table></div>`;
  }
  document.getElementById('app').innerHTML=h;
}

async function act(what,id,action,val){
  const body=new URLSearchParams({what,id,action:action||'',approved:val??'',
    hash:(what==='scan_start'||what==='scan_fresh')&&val?'1':''});
  if(what==='fw_install'){
    document.getElementById('app').innerHTML=`${imperialSvg()}<div class=row style="text-align:center;padding:20px"><p style="font:11px/1.5 var(--mono);color:#9aa2b4">Installing update and restarting...</p></div>`;
  }
  await api('/api/act',{method:'POST',body});
  if(what==='fw_install'){ fwCache=null; asCache=null; homeDrawn=false; return; }
  setTimeout(()=>view.kind==='batch'?show(view.id)
    :(view.kind==='damaged'?dmg():home()),350);
}

home();
setInterval(jobTick,1000);
setInterval(()=>{if(view.kind==='home'&&!document.getElementById('jobtxt'))home()},7000);
</script>
"""

class Handler(BaseHTTPRequestHandler):
    server_version = 'nz-ingest'

    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype='application/json'):
        raw = body.encode() if isinstance(body, str) else body
        self.send_response(code)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        u = urlparse(self.path)
        if u.path == '/':
            return self._send(200, PAGE, 'text/html; charset=utf-8')
        if u.path == '/api/state':
            return self._send(200, json.dumps({
                'drop': DROP, 'archive': ARCHIVE, 'readonly': READONLY,
                'manifest': store.file_count(),
                'hashed': store.hashed_count(),
                'damaged': store.damaged_count(),
                'unreadable': store.unreadable_count(),
                'waiting': len(walk_files(DROP, exclude_top=False)),
                'progress': progress,
                'batches': [dict(b) for b in store.batches(25)],
            }))
        if u.path == '/api/archive-stats':
            stats = store.archive_stats()
            return self._send(200, json.dumps(stats))
        if u.path == '/api/firmware':
            want = parse_qs(u.query).get('check')
            info = {'version': VERSION, 'base': UPDATE_BASE,
                    'token': bool(UPDATE_TOKEN),
                    'writable': os.access(CODE_DIR, os.W_OK),
                    'local': fw_local(), 'remote': None, 'error': None,
                    'commits': {}, 'installed': fw_installed()}
            if want:
                try:
                    info['remote'] = fw_compare(fw_fetch())
                except Exception as exc:                    # noqa: BLE001
                    info['error'] = str(exc)
                info['commits'] = fw_commits()
            return self._send(200, json.dumps(info))
        if u.path == '/api/job':
            el = time.time() - JOB['started'] if JOB['started'] else 0
            left = max(0, JOB['total'] - JOB['done'])
            eta = int(left / JOB['rate']) if JOB['rate'] > 0.01 else 0
            return self._send(200, json.dumps(dict(
                JOB, elapsed=int(el), eta=eta, version=VERSION,
                pct=round(100.0 * JOB['done'] / JOB['total'], 1)
                if JOB['total'] else 0.0)))
        if u.path == '/api/damaged':
            want = (parse_qs(u.query).get('verdict') or [''])[0]
            counts = store.verdict_counts()
            groups = []
            order = list(Store.DAMAGE) + ['ERROR']
            for v in order:
                if not counts.get(v):
                    continue
                if want and v != want:
                    groups.append({'verdict': v, 'count': counts[v],
                                   'items': []})
                    continue
                rows = [dict(r) for r in store.damaged_files(verdict=v,
                                                             limit=2000)]
                groups.append({'verdict': v, 'count': counts[v],
                               'items': rows})
            return self._send(200, json.dumps({
                'groups': groups,
                'damaged': store.damaged_count(),
                'unreadable': store.unreadable_count(),
                'folders': store.damage_by_folder(),
                'help': VERDICT_HELP,
            }))
        if u.path == '/api/damaged.tsv':
            lines = ['verdict\tfmt\tsize\tdt\tmodel\tpath']
            for v in list(Store.DAMAGE) + ['ERROR']:
                for r in store.damaged_files(verdict=v, limit=100000):
                    lines.append('\t'.join(str(r[c] if r[c] is not None else '')
                                           for c in ('verdict', 'fmt', 'size',
                                                     'dt', 'model', 'path')))
            body = '\n'.join(lines) + '\n'
            raw = body.encode()
            self.send_response(200)
            self.send_header('Content-Type', 'text/tab-separated-values')
            self.send_header('Content-Disposition',
                             'attachment; filename="damaged.tsv"')
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            return self.wfile.write(raw)
        if u.path.startswith('/api/batch/'):
            try:
                bid = int(u.path.rsplit('/', 1)[1])
            except ValueError:
                return self._send(400, '{"error":"bad batch id"}')
            b = store.batch(bid)
            if not b:
                return self._send(404, '{"error":"no such batch"}')
            groups = []
            items = [dict(i) for i in store.items(bid)]
            for it in items:
                it['dest_short'] = rel_to_archive(it['dest']) if it['dest'] else '—'
            for act in ('FILE', 'QUARANTINE', 'CONFLICT', 'DUPLICATE',
                        'UNDATED', 'OTHER', 'SKIP'):
                sel = [i for i in items if i['action'] == act]
                if sel:
                    groups.append({'action': act, 'help': ACTION_HELP[act],
                                   'items': sel[:400]})
            return self._send(200, json.dumps({
                'id': b['id'], 'name': b['name'], 'state': b['state'],
                'blocked': b['blocked'], 'note': b['note'] or '',
                'counts': store.counts(bid), 'groups': groups,
                'journal': len(store.journal(bid)),
            }))
        return self._send(404, '{"error":"not found"}')

    def do_POST(self):
        length = int(self.headers.get('Content-Length') or 0)
        form = parse_qs(self.rfile.read(length).decode())
        g = lambda k: (form.get(k) or [''])[0]                # noqa: E731
        what, bid = g('what'), g('id')
        try:
            bid = int(bid)
        except ValueError:
            return self._send(400, '{"error":"bad id"}')
        try:
            with lock:
                if what == 'approve_all':
                    store.set_approval(bid, approved=True)
                    store.db.execute(
                        "UPDATE items SET approved=0 WHERE batch_id=? AND"
                        " action='SKIP'", (bid,))
                    store.commit()
                elif what == 'approve':
                    store.set_approval(bid, action=g('action'),
                                       approved=g('approved') == '1')
                elif what == 'execute':
                    threading.Thread(target=execute_batch, args=(bid,),
                                     daemon=True).start()
                elif what == 'undo':
                    undo_batch(bid)
                elif what == 'scan_start':
                    job_start(ARCHIVE, g('hash') == '1', True)
                elif what == 'scan_fresh':
                    job_start(ARCHIVE, g('hash') == '1', False)
                elif what == 'scan_stop':
                    _stop.set()
                elif what == 'fw_install':
                    if not os.access(CODE_DIR, os.W_OK):
                        raise RuntimeError(
                            'the code directory is mounted read-only - drop '
                            'the ":ro" from the /app volume to allow updates')
                    fw_install(fw_fetch(), fw_commits())
                elif what == 'drop_now':
                    if walk_files(DROP, exclude_top=False):
                        nb = store.new_batch(
                            time.strftime('%Y-%m-%d %H:%M') + ' (manual)', DROP)
                        threading.Thread(target=plan_batch, args=(nb, DROP),
                                         daemon=True).start()
                else:
                    return self._send(400, '{"error":"unknown action"}')
            return self._send(200, '{"ok":true}')
        except Exception as exc:                            # noqa: BLE001
            return self._send(500, json.dumps({'error': str(exc)}))


# --------------------------------------------------------------------- CLI

def cmd_baseline(roots, want_hash, resume):
    for root in roots or [ARCHIVE]:
        def tick():
            last = -1
            while JOB['running']:
                if JOB['done'] != last and JOB['done'] % 500 == 0:
                    sys.stderr.write('  %s %d/%d (%d unchanged)\n' % (
                        JOB['phase'], JOB['done'], JOB['total'], JOB['skipped']))
                    last = JOB['done']
                time.sleep(1)
        threading.Thread(target=tick, daemon=True).start()
        run_baseline(root, want_hash, resume)
        sys.stderr.write('baseline: %s | manifest %d files, %d hashed, '
                         '%d need review, %d unreadable\n'
                         % (JOB['note'], store.file_count(),
                            store.hashed_count(), store.damaged_count(),
                            store.unreadable_count()))


def cmd_import(tsv):
    n = 0
    with open(tsv) as f:
        cols = f.readline().rstrip('\n').split('\t')
        for line in f:
            v = line.rstrip('\n').split('\t')
            if len(v) != len(cols):
                continue
            r = dict(zip(cols, v))
            r['size'] = int(r.get('size') or 0)
            store.add_file(rel_to_archive(r.get('path', '')), r)
            n += 1
            if n % 2000 == 0:
                store.commit()
    store.commit()
    sys.stderr.write('imported %d rows; manifest now %d files, %d hashed\n'
                     % (n, store.file_count(), store.hashed_count()))


def cmd_plan(root):
    out = []
    plan_batch(0, root, items_out=out)
    print('%-11s %-46s %s' % ('ACTION', 'DEST', 'SRC'))
    for it in out:
        print('%-11s %-46s %s  (%s)' % (
            it['action'], rel_to_archive(it['dest']) if it['dest'] else '-',
            it['rel'], it['reason']))
    tally = {}
    for it in out:
        tally[it['action']] = tally.get(it['action'], 0) + 1
    sys.stderr.write('\n' + '  '.join('%s=%d' % kv for kv in
                                      sorted(tally.items())) + '\n')


def main(argv):
    global store
    args = argv[1:]
    store = Store(DB)

    if '--baseline' in args:
        i = args.index('--baseline')
        roots = [a for a in args[i + 1:] if not a.startswith('--')]
        return cmd_baseline(roots, '--hash' in args, '--fresh' not in args)
    if '--import-manifest' in args:
        return cmd_import(args[args.index('--import-manifest') + 1])
    if '--plan' in args:
        return cmd_plan(args[args.index('--plan') + 1])

    for d in (DROP, ARCHIVE):
        os.makedirs(d, exist_ok=True)
    threading.Thread(target=watcher, daemon=True).start()

    # A scan that was running when this process last died resumes itself.
    prev = job_load()
    if prev.get('running'):
        sys.stderr.write('resuming interrupted scan of %s\n'
                         % prev.get('root', ARCHIVE))
        job_start(prev.get('root') or ARCHIVE, prev.get('hash', False), True)
    sys.stderr.write('nz-ingest on :%d  drop=%s archive=%s manifest=%d files%s\n'
                     % (PORT, DROP, ARCHIVE, store.file_count(),
                        '  [READ-ONLY]' if READONLY else ''))
    ThreadingHTTPServer(('0.0.0.0', PORT), Handler).serve_forever()


if __name__ == '__main__':
    sys.exit(main(sys.argv) or 0)
