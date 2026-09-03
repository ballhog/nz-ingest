"""
mediacheck.py - format detection, integrity verdict and EXIF identity for
photo/video files. Pure stdlib.

Extracted from photo-manifest.py after validation against fixtures in all
five supported families (TIFF/ARW, JPEG, PNG, ISO-BMFF, zero-byte).

The one rule this module exists to serve: a file is not damaged until
something opened it. Size and name are metadata; they generate candidates,
never conclusions.
"""

import hashlib
import os
import struct
import sys

TYPE_SIZE = {1:1, 2:1, 3:2, 4:4, 5:8, 6:1, 7:1, 8:2, 9:4, 10:8, 11:4, 12:8, 13:4}
TYPE_FMT = {1:'B', 3:'H', 4:'I', 6:'b', 8:'h', 9:'i', 13:'I'}

EXTENT_PAIRS = ((273, 279), (324, 325), (513, 514))
SUBIFDS, EXIFIFD = 330, 34665

T_DATETIME_ORIG = 36867
T_DATETIME = 306
T_MODEL = 272
T_SERIAL = 42033
T_IMAGENUM = 37393

MAX_IFDS = 64
MAX_COUNT = 1000000
HASH_CHUNK = 1 << 20

TIFF_EXT = {'.arw', '.dng', '.nef', '.cr2', '.cr3', '.tif', '.tiff', '.orf',
            '.rw2', '.raf', '.pef', '.srw'}


# ---------------------------------------------------------------- TIFF core

class BadHeader(Exception):
    pass


def _tag_value(f, e, entry, size, base):
    """Decode one IFD entry's value. Returns [] / b'' on anything unsafe."""
    typ, cnt, raw = entry
    ts = TYPE_SIZE.get(typ)
    if ts is None or cnt == 0 or cnt > MAX_COUNT:
        return None
    total = ts * cnt
    if total <= 4:
        data = raw[:total]
    else:
        off = struct.unpack(e + 'I', raw)[0]
        if off <= 0 or base + off + total > size:
            return None
        f.seek(base + off)
        data = f.read(total)
        if len(data) < total:
            return None
    if typ in (2, 7):                     # ASCII / UNDEFINED
        return data
    fmt = TYPE_FMT.get(typ)
    if fmt is None:
        return None
    try:
        return list(struct.unpack(e + fmt * cnt, data))
    except struct.error:
        return None


def _read_ifd(f, e, off, size, base):
    if off <= 0 or base + off + 2 > size:
        raise BadHeader('ifd past eof')
    f.seek(base + off)
    r = f.read(2)
    if len(r) < 2:
        raise BadHeader('short ifd')
    n = struct.unpack(e + 'H', r)[0]
    if n > 4096:
        raise BadHeader('implausible ifd')
    need = n * 12 + 4
    buf = f.read(need)
    if len(buf) < need:
        raise BadHeader('ifd body past eof')
    entries = {}
    for i in range(n):
        x = buf[i * 12:i * 12 + 12]
        tag, typ, cnt = struct.unpack(e + 'HHI', x[:8])
        entries[tag] = (typ, cnt, x[8:12])
    nxt = struct.unpack(e + 'I', buf[n * 12:n * 12 + 4])[0]
    return entries, nxt


def _ascii(v):
    if not v:
        return ''
    if isinstance(v, bytes):
        return v.split(b'\x00')[0].decode('ascii', 'replace').strip()
    return str(v[0]) if v else ''


def walk_tiff(f, size, base=0):
    """Return (verdict, expected_end, meta) for a TIFF structure at `base`."""
    meta = {}
    f.seek(base)
    hdr = f.read(8)
    if len(hdr) < 8:
        return ('HEADER_BAD', 0, meta)
    if hdr[:2] == b'II':
        e = '<'
    elif hdr[:2] == b'MM':
        e = '>'
    else:
        return ('HEADER_BAD', 0, meta)
    magic, first = struct.unpack(e + 'HI', hdr[2:8])
    if magic == 43:
        return ('NO_EXTENT', 0, meta)      # BigTIFF, different layout
    # 42 is standard TIFF. Several raw formats are TIFF-structured - same IFD
    # layout, same first-IFD offset at bytes 4-8 - but stamp their own magic:
    #   0x4F52 / 0x5352   Olympus ORF ("IIRO" / "IIRS")
    #   85                Panasonic RW2
    # Rejecting these as HEADER_BAD condemned 241 perfectly good Olympus files
    # on 2026-09-02. A format the checker cannot parse is not a damaged file.
    if magic not in (42, 85, 0x4F52, 0x5352):
        return ('HEADER_BAD', 0, meta)

    max_end = 0
    seen = set()
    queue = [first]
    walked = 0
    try:
        while queue and walked < MAX_IFDS:
            off = queue.pop(0)
            if off == 0 or off in seen:
                continue
            seen.add(off)
            walked += 1
            entries, nxt = _read_ifd(f, e, off, size, base)
            if nxt and nxt not in seen:
                queue.append(nxt)

            for tag in (SUBIFDS, EXIFIFD):
                if tag in entries:
                    v = _tag_value(f, e, entries[tag], size, base)
                    if v:
                        for s in v:
                            if isinstance(s, int) and s and s not in seen:
                                queue.append(s)

            if T_DATETIME_ORIG in entries and 'dt' not in meta:
                meta['dt'] = _ascii(_tag_value(f, e, entries[T_DATETIME_ORIG], size, base))
            elif T_DATETIME in entries and 'dt' not in meta:
                meta['dt'] = _ascii(_tag_value(f, e, entries[T_DATETIME], size, base))
            if T_MODEL in entries and 'model' not in meta:
                meta['model'] = _ascii(_tag_value(f, e, entries[T_MODEL], size, base))
            if T_SERIAL in entries and 'serial' not in meta:
                meta['serial'] = _ascii(_tag_value(f, e, entries[T_SERIAL], size, base))
            if T_IMAGENUM in entries and 'imgnum' not in meta:
                v = _tag_value(f, e, entries[T_IMAGENUM], size, base)
                if v and isinstance(v[0], int):
                    meta['imgnum'] = str(v[0])

            for a, b in EXTENT_PAIRS:
                if a not in entries or b not in entries:
                    continue
                offs = _tag_value(f, e, entries[a], size, base)
                lens = _tag_value(f, e, entries[b], size, base)
                if not offs or not lens:
                    continue
                if len(lens) == 1 and len(offs) > 1:
                    lens = lens * len(offs)
                for o, l in zip(offs, lens):
                    if isinstance(o, int) and isinstance(l, int) and o > 0 and l > 0:
                        if base + o + l > max_end:
                            max_end = base + o + l
    except BadHeader:
        return ('HEADER_BAD', 0, meta)

    if max_end == 0:
        return ('NO_EXTENT', 0, meta)
    if max_end > size:
        return ('TRUNCATED', max_end, meta)
    return ('OK', max_end, meta)


# ---------------------------------------------------------------- JPEG

def check_jpeg(f, size):
    meta = {}
    f.seek(0)
    if f.read(3) != b'\xff\xd8\xff':
        return ('HEADER_BAD', 0, meta)

    # EXIF lives in an APP1 segment holding a complete TIFF structure
    pos = 2
    while pos + 4 <= size:
        f.seek(pos)
        b = f.read(4)
        if len(b) < 4 or b[0] != 0xFF:
            break
        marker = b[1]
        if marker in (0xD8, 0xD9) or 0xD0 <= marker <= 0xD7:
            pos += 2
            continue
        seglen = struct.unpack('>H', b[2:4])[0]
        if seglen < 2 or pos + 2 + seglen > size:
            break
        if marker == 0xE1:
            f.seek(pos + 4)
            if f.read(6) == b'Exif\x00\x00':
                _, _, m = walk_tiff(f, size, base=pos + 10)
                meta.update(m)
        if marker == 0xDA:                 # start of scan; metadata is behind us
            break
        pos += 2 + seglen

    # Cameras and editors append thumbnails, maker notes and XMP after the
    # EOI marker. A 4 KB window called such files TRUNCATED; 64 KB covers
    # real-world trailers. Kept as a bounded search rather than a full scan
    # because FFD9 also occurs by chance inside entropy-coded scan data, and
    # a wider window trades false alarms for missed damage.
    tail = 65536 if size > 65536 else size
    f.seek(size - tail)
    buf = f.read(tail)
    if buf.endswith(b'\xff\xd9'):
        return ('OK', size, meta)
    if b'\xff\xd9' in buf:
        return ('TRAILING', size - (len(buf) - buf.rfind(b'\xff\xd9') - 2), meta)
    return ('TRUNCATED', 0, meta)


# ---------------------------------------------------------------- PNG

def check_png(f, size):
    if size < 20:
        return ('TRUNCATED', 0, {})
    f.seek(size - 12)
    if f.read(12)[4:8] == b'IEND':
        return ('OK', size, {})
    f.seek(max(0, size - 4096))
    if b'IEND' in f.read(4096):
        return ('TRAILING', size, {})
    return ('TRUNCATED', 0, {})


# ---------------------------------------------------------------- ISO-BMFF

BMFF_CONTAINERS = {b'moov', b'trak', b'mdia', b'minf', b'stbl', b'udta', b'edts'}


def _bmff_boxes(f, start, end, depth=0):
    """Yield (type, payload_start, payload_end, declared_end). Shallow walk."""
    pos = start
    guard = 0
    while pos + 8 <= end and guard < 4096:
        guard += 1
        f.seek(pos)
        hdr = f.read(8)
        if len(hdr) < 8:
            return
        bsize = struct.unpack('>I', hdr[:4])[0]
        btype = hdr[4:8]
        body = pos + 8
        if bsize == 1:
            ext = f.read(8)
            if len(ext) < 8:
                return
            bsize = struct.unpack('>Q', ext)[0]
            body = pos + 16
        elif bsize == 0:
            bsize = end - pos
        if bsize < 8:
            return
        yield (btype, body, min(pos + bsize, end), pos + bsize)
        if depth < 4 and btype in BMFF_CONTAINERS:
            for sub in _bmff_boxes(f, body, min(pos + bsize, end), depth + 1):
                yield sub
        pos += bsize


# ISO-BMFF covers both video (moov) and still images (meta). A HEIC photo has
# no moov box and never should - demanding one flagged 1,946 iPhone photos as
# damaged on 2026-09-02. The ftyp brand says which rule applies.
STILL_BRANDS = {b'heic', b'heix', b'heim', b'heis', b'hevc', b'hevx',
                b'mif1', b'msf1', b'avif', b'avis', b'mia1', b'miaf'}


def check_bmff(f, size):
    meta = {}
    saw_moov = False
    saw_meta = False
    still = False
    overrun = False
    for btype, body, _stop, declared_end in _bmff_boxes(f, 0, size):
        if declared_end > size:
            overrun = True
        if btype == b'moov':
            saw_moov = True
        if btype == b'meta':
            saw_meta = True
        if btype == b'ftyp':
            f.seek(body)
            brands = f.read(min(64, max(0, _stop - body)))
            if brands[:4] in STILL_BRANDS:
                still = True
            for i in range(8, len(brands) - 3, 4):
                if brands[i:i + 4] in STILL_BRANDS:
                    still = True
        if btype == b'mvhd' and 'dt' not in meta:
            f.seek(body)
            v = f.read(20)
            if len(v) >= 20:
                ver = v[0]
                try:
                    if ver == 1:
                        secs = struct.unpack('>Q', v[4:12])[0]
                    else:
                        secs = struct.unpack('>I', v[4:8])[0]
                    if secs:
                        import datetime
                        base = datetime.datetime(1904, 1, 1)
                        meta['dt'] = (base + datetime.timedelta(seconds=secs)) \
                            .strftime('%Y:%m:%d %H:%M:%S')
                except (struct.error, OverflowError, ValueError):
                    pass
    if overrun:
        return ('TRUNCATED', 0, meta)
    if still:
        # A still image: meta is the index, moov is irrelevant.
        return ('OK', size, meta) if saw_meta else ('HEADER_BAD', 0, meta)
    if not saw_moov:
        return ('NO_MOOV', 0, meta)
    return ('OK', size, meta)


# ---------------------------------------------------------------- dispatch

def detect(f, size, ext):
    f.seek(0)
    head = f.read(12)
    if len(head) >= 8 and head[:8] == b'\x89PNG\r\n\x1a\n':
        return 'png'
    if len(head) >= 3 and head[:3] == b'\xff\xd8\xff':
        return 'jpeg'
    if len(head) >= 4 and head[:4] in (b'II*\x00', b'MM\x00*'):
        return 'tiff'
    if len(head) >= 8 and head[4:8] == b'ftyp':
        return 'bmff'
    if ext in TIFF_EXT:
        return 'tiff'
    return None


def inspect(path, want_hash):
    row = {'verdict': 'ERROR', 'size': -1, 'expected': 0, 'mtime': '',
           'fmt': '', 'dt': '', 'model': '', 'serial': '', 'imgnum': '',
           'sha256': ''}
    try:
        st = os.stat(path)
    except OSError:
        return row
    row['size'] = st.st_size
    row['mtime'] = str(int(st.st_mtime))
    if st.st_size == 0:
        row['verdict'] = 'ZERO'
        return row

    ext = os.path.splitext(path)[1].lower()
    try:
        with open(path, 'rb') as f:
            fmt = detect(f, st.st_size, ext)
            row['fmt'] = fmt or 'other'
            if fmt == 'tiff':
                v, exp, meta = walk_tiff(f, st.st_size)
            elif fmt == 'jpeg':
                v, exp, meta = check_jpeg(f, st.st_size)
            elif fmt == 'png':
                v, exp, meta = check_png(f, st.st_size)
            elif fmt == 'bmff':
                v, exp, meta = check_bmff(f, st.st_size)
            else:
                v, exp, meta = ('UNKNOWN_FMT', 0, {})
            row['verdict'] = v
            row['expected'] = exp
            for k in ('dt', 'model', 'serial', 'imgnum'):
                if meta.get(k):
                    row[k] = meta[k]
            if want_hash:
                h = hashlib.sha256()
                f.seek(0)
                while True:
                    chunk = f.read(HASH_CHUNK)
                    if not chunk:
                        break
                    h.update(chunk)
                row['sha256'] = h.hexdigest()
    except (OSError, struct.error):
        row['verdict'] = 'ERROR'
    return row


