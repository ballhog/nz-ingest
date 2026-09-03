"""
store.py - SQLite manifest and batch state for nz-ingest.

Two things live here:

  files    the BASELINE MANIFEST - one row per file known to be in the
           archive, with its integrity verdict, camera identity and hash.
           This is what lets an incoming card be checked against everything
           you already have instead of only against its own batch.

  batches  one per drop-folder ingest, with its items, decisions and a
           journal of every move actually performed so it can be undone.

Identity, not filename. Camera counters cycle - the same basename is
several different photographs in one archive - so the identity key is
serial|imagenumber|datetime_original. The content hash is secondary and
authoritative for exact duplicates; the identity tuple catches the same
frame stored in a different container.
"""

import os
import sqlite3
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    id       INTEGER PRIMARY KEY,
    path     TEXT UNIQUE NOT NULL,   -- archive-relative
    size     INTEGER,
    mtime    INTEGER,
    fmt      TEXT,
    verdict  TEXT,
    dt       TEXT,
    model    TEXT,
    serial   TEXT,
    imgnum   TEXT,
    sha256   TEXT,
    ident    TEXT,
    added_at INTEGER
);
CREATE INDEX IF NOT EXISTS ix_files_sha   ON files(sha256);
CREATE INDEX IF NOT EXISTS ix_files_ident ON files(ident);
CREATE INDEX IF NOT EXISTS ix_files_vd    ON files(verdict);

CREATE TABLE IF NOT EXISTS batches (
    id         INTEGER PRIMARY KEY,
    name       TEXT,
    src_root   TEXT,
    state      TEXT,          -- scanning|review|executing|done|failed|undone
    blocked    INTEGER DEFAULT 0,
    created_at INTEGER,
    decided_at INTEGER,
    note       TEXT
);

CREATE TABLE IF NOT EXISTS items (
    id       INTEGER PRIMARY KEY,
    batch_id INTEGER NOT NULL,
    src      TEXT NOT NULL,
    rel      TEXT,
    size     INTEGER,
    fmt      TEXT,
    verdict  TEXT,
    dt       TEXT,
    model    TEXT,
    serial   TEXT,
    imgnum   TEXT,
    sha256   TEXT,
    ident    TEXT,
    action   TEXT,            -- FILE|QUARANTINE|DUPLICATE|CONFLICT|UNDATED|OTHER|SKIP
    dest     TEXT,
    reason   TEXT,
    dup_of   TEXT,
    grp      TEXT,
    approved INTEGER DEFAULT 1,
    result   TEXT,
    result_msg TEXT
);
CREATE INDEX IF NOT EXISTS ix_items_batch ON items(batch_id);

CREATE TABLE IF NOT EXISTS journal (
    id       INTEGER PRIMARY KEY,
    batch_id INTEGER NOT NULL,
    src      TEXT,
    dst      TEXT,
    ts       INTEGER
);
CREATE INDEX IF NOT EXISTS ix_journal_batch ON journal(batch_id);
"""


def ident_of(row):
    """serial|imgnum|datetime - empty string when the camera didn't say."""
    s, n, d = row.get('serial', ''), row.get('imgnum', ''), row.get('dt', '')
    if s and n:
        return '%s|%s|%s' % (s, n, d)
    return ''


class Store(object):
    def __init__(self, path):
        first = not os.path.exists(path)
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False,
                                  timeout=30.0)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=NORMAL')
        self.db.executescript(SCHEMA)
        self.db.commit()
        self.fresh = first

    # -------------------------------------------------- baseline manifest

    def add_file(self, rel, row):
        self.db.execute(
            'INSERT OR REPLACE INTO files'
            ' (path,size,mtime,fmt,verdict,dt,model,serial,imgnum,sha256,'
            '  ident,added_at)'
            ' VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
            (rel, row.get('size'), row.get('mtime'), row.get('fmt'),
             row.get('verdict'), row.get('dt'), row.get('model'),
             row.get('serial'), row.get('imgnum'), row.get('sha256'),
             ident_of(row), int(time.time())))

    def file_count(self):
        return self.db.execute('SELECT COUNT(*) c FROM files').fetchone()['c']

    def hashed_count(self):
        return self.db.execute(
            "SELECT COUNT(*) c FROM files WHERE sha256 <> ''").fetchone()['c']

    def by_hash(self, sha):
        if not sha:
            return None
        r = self.db.execute('SELECT path FROM files WHERE sha256=? LIMIT 1',
                            (sha,)).fetchone()
        return r['path'] if r else None

    def by_ident(self, ident):
        if not ident:
            return []
        return [r['path'] for r in self.db.execute(
            'SELECT path FROM files WHERE ident=? LIMIT 8', (ident,))]

    # Damage and unreadability are different claims and are counted
    # separately. A file that could not be opened is UNKNOWN, not broken;
    # merging the two hides a permissions problem inside a damage figure.
    DAMAGE = ('ZERO', 'TRUNCATED', 'HEADER_BAD', 'NO_MOOV')

    def verdict_counts(self):
        return {r['verdict']: r['c'] for r in self.db.execute(
            'SELECT verdict, COUNT(*) c FROM files GROUP BY verdict')}

    def damaged_count(self):
        q = ','.join('?' * len(self.DAMAGE))
        return self.db.execute(
            'SELECT COUNT(*) c FROM files WHERE verdict IN (%s)' % q,
            self.DAMAGE).fetchone()['c']

    def unreadable_count(self):
        return self.db.execute(
            "SELECT COUNT(*) c FROM files WHERE verdict='ERROR'"
        ).fetchone()['c']

    def damaged_files(self, verdict=None, limit=5000):
        """Damaged rows, or one verdict's rows. ERROR is available by asking
        for it explicitly — it is never folded into the damage total."""
        if verdict:
            return self.db.execute(
                'SELECT * FROM files WHERE verdict=? ORDER BY path LIMIT ?',
                (verdict, limit)).fetchall()
        q = ','.join('?' * len(self.DAMAGE))
        return self.db.execute(
            'SELECT * FROM files WHERE verdict IN (%s)'
            ' ORDER BY verdict, path LIMIT ?' % q,
            self.DAMAGE + (limit,)).fetchall()

    def damage_by_folder(self, depth=1, limit=40):
        """Top-level folder distribution of damage — the question that took a
        day to answer by hand on 2026-09-02."""
        out = {}
        for r in self.damaged_files(limit=100000):
            top = r['path'].split('/')[0] if '/' in r['path'] else '(root)'
            out[top] = out.get(top, 0) + 1
        return sorted(out.items(), key=lambda kv: -kv[1])[:limit]



    def archive_stats(self, limit=12):
        """Total archive size, format distribution, camera models, and sample
        EXIF data for the UI display."""
        # Total size
        total_size = self.db.execute(
            'SELECT COALESCE(SUM(size), 0) s FROM files WHERE verdict NOT IN ("ERROR")').fetchone()['s']

        # Format distribution
        formats = {}
        for r in self.db.execute('SELECT fmt, COUNT(*) c FROM files WHERE fmt IS NOT NULL GROUP BY fmt ORDER BY c DESC'):
            formats[r['fmt']] = r['c']

        # Camera models (top 5)
        models = []
        for r in self.db.execute('SELECT model, COUNT(*) c FROM files WHERE model IS NOT NULL GROUP BY model ORDER BY c DESC LIMIT 5'):
            models.append({'model': r['model'], 'count': r['c']})

        # Sample EXIF data from recent files (for rotating display)
        samples = []
        for r in self.db.execute(
            'SELECT path, size, fmt, model, dt FROM files WHERE model IS NOT NULL AND dt IS NOT NULL '
            'ORDER BY added_at DESC LIMIT ?', (limit,)):
            samples.append({
                'path': r['path'],
                'size': r['size'],
                'fmt': r['fmt'],
                'model': r['model'],
                'dt': r['dt']
            })

        return {
            'total_size': total_size,
            'file_count': self.file_count(),
            'formats': formats,
            'models': models,
            'samples': samples
        }

    # -------------------------------------------------------- batches

    def new_batch(self, name, src_root):
        cur = self.db.execute(
            'INSERT INTO batches (name,src_root,state,created_at)'
            ' VALUES (?,?,?,?)', (name, src_root, 'scanning', int(time.time())))
        self.db.commit()
        return cur.lastrowid

    def set_batch(self, bid, **kw):
        if not kw:
            return
        cols = ','.join('%s=?' % k for k in kw)
        self.db.execute('UPDATE batches SET %s WHERE id=?' % cols,
                        tuple(kw.values()) + (bid,))
        self.db.commit()

    def batch(self, bid):
        return self.db.execute('SELECT * FROM batches WHERE id=?',
                               (bid,)).fetchone()

    def batches(self, limit=50):
        return self.db.execute(
            'SELECT * FROM batches ORDER BY id DESC LIMIT ?',
            (limit,)).fetchall()

    def add_item(self, bid, it):
        self.db.execute(
            'INSERT INTO items (batch_id,src,rel,size,fmt,verdict,dt,model,'
            ' serial,imgnum,sha256,ident,action,dest,reason,dup_of,grp,approved)'
            ' VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (bid, it['src'], it.get('rel'), it.get('size'), it.get('fmt'),
             it.get('verdict'), it.get('dt'), it.get('model'),
             it.get('serial'), it.get('imgnum'), it.get('sha256'),
             it.get('ident'), it.get('action'), it.get('dest'),
             it.get('reason'), it.get('dup_of'), it.get('grp'),
             1 if it.get('approved', True) else 0))

    def items(self, bid, action=None):
        if action:
            return self.db.execute(
                'SELECT * FROM items WHERE batch_id=? AND action=?'
                ' ORDER BY dest, src', (bid, action)).fetchall()
        return self.db.execute(
            'SELECT * FROM items WHERE batch_id=? ORDER BY action, dest, src',
            (bid,)).fetchall()

    def counts(self, bid):
        rows = self.db.execute(
            'SELECT action, COUNT(*) c FROM items WHERE batch_id=?'
            ' GROUP BY action', (bid,)).fetchall()
        return {r['action']: r['c'] for r in rows}

    def set_approval(self, bid, action=None, item_id=None, approved=True):
        v = 1 if approved else 0
        if item_id is not None:
            self.db.execute(
                'UPDATE items SET approved=? WHERE batch_id=? AND id=?',
                (v, bid, item_id))
        elif action is not None:
            self.db.execute(
                'UPDATE items SET approved=? WHERE batch_id=? AND action=?',
                (v, bid, action))
        else:
            self.db.execute('UPDATE items SET approved=? WHERE batch_id=?',
                            (v, bid))
        self.db.commit()

    def set_result(self, item_id, result, msg=''):
        self.db.execute('UPDATE items SET result=?, result_msg=? WHERE id=?',
                        (result, msg, item_id))

    def journal_add(self, bid, src, dst):
        self.db.execute(
            'INSERT INTO journal (batch_id,src,dst,ts) VALUES (?,?,?,?)',
            (bid, src, dst, int(time.time())))

    def journal(self, bid):
        return self.db.execute(
            'SELECT * FROM journal WHERE batch_id=? ORDER BY id DESC',
            (bid,)).fetchall()

    def journal_clear(self, bid):
        self.db.execute('DELETE FROM journal WHERE batch_id=?', (bid,))
        self.db.commit()

    def drop_file(self, rel):
        self.db.execute('DELETE FROM files WHERE path=?', (rel,))

    def commit(self):
        self.db.commit()
