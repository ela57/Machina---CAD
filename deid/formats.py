"""Format-aware rewriting. Every handler returns bytes of a file that keeps its
original format; replacements are same-length wherever offsets matter."""
import gzip
import io
import logging
import re
import struct
import tarfile
import zipfile
import zlib

log = logging.getLogger("deid")

SW_MARK = re.compile(rb"\x14\x00\x06\x00\x08\x00")
SW_EXT = {".sldprt", ".sldasm", ".slddrw", ".prtdot", ".asmdot", ".drwdot", ".sldlfp", ".slddrt"}


class Ctx:
    def __init__(self, matcher, rename, stats):
        self.m = matcher
        self.rename = rename  # str -> str (path component renaming)
        self.stats = stats    # dict of counters / notes

    def note(self, key, n=1):
        self.stats[key] = self.stats.get(key, 0) + n


def _deflate_fit(raw, limit):
    best = None
    for level in (9, 8, 7, 6, 5, 4, 3, 2, 1):
        for strat in (zlib.Z_DEFAULT_STRATEGY, zlib.Z_FILTERED):
            for mem in (9, 8):
                c = zlib.compressobj(level, zlib.DEFLATED, -15, mem, strat)
                out = c.compress(raw) + c.flush()
                if len(out) <= limit and (best is None or len(out) < len(best)):
                    best = out
        if best is not None:
            return best
    return None


def sw_iter_records(data, max_usize=512 * 2**20):
    for mt in SW_MARK.finditer(data):
        i = mt.start()
        if i + 26 > len(data):
            break
        crc, csize, usize, nlen = struct.unpack_from("<IIII", data, i + 10)
        if nlen > 512 or csize == 0 or csize > len(data) or usize > max_usize:
            continue
        s = i + 26 + nlen
        o = zlib.decompressobj(-15)
        try:
            raw = o.decompress(data[s:s + csize], usize + 16)
        except zlib.error:
            continue
        if o.eof and len(raw) == usize:
            yield bytes(((b << 4) | (b >> 4)) & 0xFF for b in data[i + 26:s]), raw


def solidworks_records(data, ctx):
    """SolidWorks 2015+ container (and OLE-wrapped variants): each record is
    marker, crc, csize, usize, name-length, nibble-rotated name, raw-deflate data."""
    buf = bytearray(data)
    for mt in SW_MARK.finditer(data):
        i = mt.start()
        if i + 26 > len(data):
            break
        crc, csize, usize, nlen = struct.unpack_from("<IIII", data, i + 10)
        if nlen > 512 or csize == 0 or csize > len(data):
            continue
        s = i + 26 + nlen
        comp = data[s:s + csize]
        o = zlib.decompressobj(-15)
        try:
            raw = o.decompress(comp, usize + 16)
        except zlib.error:
            continue
        if not o.eof or len(raw) != usize:
            continue
        name = bytes(((b << 4) | (b >> 4)) & 0xFF for b in data[i + 26:s])
        nname, nn = ctx.m.replace(name)
        if nn:
            buf[i + 26:s] = bytes(((b << 4) | (b >> 4)) & 0xFF for b in nname)
        new, n = ctx.m.replace(raw)
        if not n:
            continue
        new = process_bytes(new, name.decode("latin-1", "replace").lower(), ctx, depth=1) if name.lower().endswith((b".xml", b".png", b".jpg", b".bmp")) else new
        comp2 = _deflate_fit(new, csize)
        if comp2 is None and name.lower().endswith(b".xml"):
            # whitespace between tags is insignificant: make it uniform so it compresses better
            new = re.sub(rb">\s+<", lambda w: b">" + b" " * (len(w.group(0)) - 2) + b"<", new)
            comp2 = _deflate_fit(new, csize)
        if comp2 is None:
            ctx.note("sw_record_unfit")
            continue
        comp2 = comp2 + b"\x00" * (csize - len(comp2))
        buf[s:s + csize] = comp2
        if crc == (zlib.crc32(raw) & 0xFFFFFFFF):
            struct.pack_into("<I", buf, i + 10, zlib.crc32(new) & 0xFFFFFFFF)
        ctx.note("sw_record_rewritten")
    return bytes(buf)


def rewrite_zip(data, ctx, depth):
    try:
        zin = zipfile.ZipFile(io.BytesIO(data))
        infos = zin.infolist()
    except Exception:
        ctx.note("zip_unreadable")
        return ctx.m.replace(data)[0]
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", allowZip64=True) as zout:
        for info in infos:
            name = "/".join(ctx.rename(p) for p in info.filename.split("/"))
            ni = zipfile.ZipInfo(name, info.date_time)
            ni.compress_type = info.compress_type if info.compress_type in (0, 8) else 8
            ni.external_attr = info.external_attr
            ni.comment = ctx.m.replace(info.comment)[0]
            if info.is_dir():
                zout.writestr(ni, b"")
                continue
            try:
                member = zin.read(info)
            except Exception:
                ctx.note("zip_member_unreadable")
                continue
            ext = "." + info.filename.rsplit(".", 1)[-1].lower() if "." in info.filename else ""
            zout.writestr(ni, process_bytes(member, ext, ctx, depth + 1))
        zout.comment = ctx.m.replace(zin.comment or b"")[0]
    return out.getvalue()


def rewrite_tar(data, ctx, depth, mode="r:"):
    try:
        tin = tarfile.open(fileobj=io.BytesIO(data), mode=mode)
        members = tin.getmembers()
    except Exception:
        ctx.note("tar_unreadable")
        return ctx.m.replace(data)[0]
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w", format=tarfile.PAX_FORMAT) as tout:
        for mem in members:
            nm = tarfile.TarInfo("/".join(ctx.rename(p) for p in mem.name.split("/")))
            nm.mode, nm.mtime, nm.type = mem.mode, mem.mtime, mem.type
            nm.uname = nm.gname = ""
            if mem.issym() or mem.islnk():
                nm.linkname = "/".join(ctx.rename(p) for p in mem.linkname.split("/"))
            if mem.isfile():
                body = tin.extractfile(mem).read()
                ext = "." + mem.name.rsplit(".", 1)[-1].lower() if "." in mem.name else ""
                body = process_bytes(body, ext, ctx, depth + 1)
                nm.size = len(body)
                tout.addfile(nm, io.BytesIO(body))
            else:
                tout.addfile(nm)
    return out.getvalue()


def rewrite_pdf(data, ctx, depth):
    import fitz
    try:
        doc = fitz.open(stream=data, filetype="pdf")
    except Exception:
        ctx.note("pdf_unreadable")
        return ctx.m.replace(data)[0]
    try:
        md = doc.metadata or {}
        newmd = {k: ctx.m.replace_str(v) if isinstance(v, str) else v for k, v in md.items() if k in
                 ("author", "title", "subject", "keywords", "creator", "producer")}
        doc.set_metadata(newmd)
        xmp = doc.get_xml_metadata()
        if xmp:
            doc.set_xml_metadata(ctx.m.replace_str(xmp))
        for name in list(doc.embfile_names()):
            body = doc.embfile_get(name)
            info = doc.embfile_info(name)
            fn = info.get("filename") or name
            ext = "." + fn.rsplit(".", 1)[-1].lower() if "." in fn else ""
            body = process_bytes(body, ext, ctx, depth + 1)
            doc.embfile_upd(name, body, filename=ctx.rename(fn), ufilename=ctx.rename(info.get("ufilename") or fn),
                            desc=ctx.m.replace_str(info.get("desc") or ""))
            ctx.note("pdf_embedded_rewritten")
        for page in doc:
            hits = 0
            for x0, y0, x1, y1, word, *_ in page.get_text("words"):
                rep = ctx.m.replace_str(word)
                if rep != word:
                    r = fitz.Rect(x0, y0, x1, y1)
                    page.add_redact_annot(r, text=rep, fontsize=max(3, min(r.height * 0.8, 24)), fill=False,
                                          align=fitz.TEXT_ALIGN_LEFT)
                    hits += 1
            for annot in page.annots() or []:
                info = annot.info
                if any(ctx.m.replace_str(info.get(k, "")) != info.get(k, "") for k in ("content", "title", "subject")):
                    annot.set_info(content=ctx.m.replace_str(info.get("content", "")),
                                   title=ctx.m.replace_str(info.get("title", "")),
                                   subject=ctx.m.replace_str(info.get("subject", "")))
                    annot.update()
            for link in page.get_links():
                uri = link.get("uri")
                if uri and ctx.m.replace_str(uri) != uri:
                    link["uri"] = ctx.m.replace_str(uri)
                    page.update_link(link)
            if hits:
                page.apply_redactions(images=fitz.PDF_REDACT_IMAGE_NONE, graphics=fitz.PDF_REDACT_LINE_ART_NONE)
                ctx.note("pdf_text_redacted", hits)
        out = doc.tobytes(garbage=1, deflate=True)
    except Exception as e:
        log.warning("pdf rewrite failed: %s", e)
        ctx.note("pdf_rewrite_failed")
        return ctx.m.replace(data)[0]
    finally:
        doc.close()
    return ctx.m.replace(out)[0]


def process_bytes(data, ext, ctx, depth=0):
    if depth > 6 or not data:
        return data
    head = data[:8]
    if head.startswith(b"%PDF") or (ext == ".pdf" and b"%PDF" in data[:1024]):
        return rewrite_pdf(data, ctx, depth)
    if head.startswith(b"PK\x03\x04") or head.startswith(b"PK\x05\x06"):
        return rewrite_zip(data, ctx, depth)
    if head.startswith(b"\x1f\x8b"):
        try:
            raw = gzip.decompress(data)
        except Exception:
            ctx.note("gzip_unreadable")
            return ctx.m.replace(data)[0]
        inner = ext[:-3] if ext.endswith(".gz") else ext
        if len(raw) > 262 and raw[257:262] == b"ustar":
            new = rewrite_tar(raw, ctx, depth)
        else:
            new = process_bytes(raw, inner, ctx, depth + 1)
        return gzip.compress(new, mtime=0)
    if len(data) > 262 and data[257:262] == b"ustar":
        return rewrite_tar(data, ctx, depth)
    if ext in SW_EXT or SW_MARK.search(data[:65536]):
        data = solidworks_records(data, ctx)
    return ctx.m.replace(data)[0]
