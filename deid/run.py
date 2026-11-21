"""Streaming driver: download one source group at a time, harvest identities
(pass 1) or rewrite files (pass 2), then delete the raw input."""
import argparse
import concurrent.futures as cf
import json
import logging
import os
import shutil
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request
from collections import OrderedDict
from pathlib import Path

from .formats import Ctx, generic, process_bytes
from .harvest import Harvest, harvest_file
from .matcher import Matcher, looks_text
from .pseudo import Registry

log = logging.getLogger("deid")
BASE = "https://machina-input.joinbuilders.org/"
CHUNK = 64 * 2**20
STREAM_LIMIT = 384 * 2**20
SEVENZIP_EXT = {".7z"}


def read_manifest(path):
    rows = []
    for line in open(path, encoding="utf-8"):
        if line.startswith("#") or not line.strip():
            continue
        p, size, md5, url = line.rstrip("\n").split("\t")
        rows.append({"path": p, "size": int(size), "md5": md5, "url": url})
    return rows


def group_of(path):
    parts = path.split("/")
    return "/".join(parts[:3]) if parts[0] == "sources" else parts[0]


def groups(rows):
    g = OrderedDict()
    for r in rows:
        g.setdefault(group_of(r["path"]), []).append(r)
    return g


def download(row, dest, base):
    dest.parent.mkdir(parents=True, exist_ok=True)
    url = base + urllib.parse.quote(row["url"], safe="/%")  if "%" not in row["url"] else base + row["url"]
    for attempt in range(6):
        try:
            with urllib.request.urlopen(url, timeout=120) as r, open(dest, "wb") as f:
                shutil.copyfileobj(r, f, 8 * 2**20)
            return True
        except Exception as e:  # retry transient errors
            if getattr(e, "code", None) == 404:
                dest.write_bytes(b"")
                return False
            time.sleep(2 * (attempt + 1))
    return False


def ext_of(name):
    low = name.lower()
    for multi in (".tar.gz", ".tar.bz2"):
        if low.endswith(multi):
            return multi
    return "." + low.rsplit(".", 1)[-1] if "." in os.path.basename(low) else ""


def is_html_placeholder(data, ext):
    if ext in (".html", ".htm", ".svg", ".xml"):
        return False
    head = data[:512].lstrip().lower()
    return head.startswith(b"<!doctype html") or head.startswith(b"<html")


# ------------------------------------------------------------------ workers
_W = {}

NOTE_DEFECTS = {
    "zip_unreadable": "archive could not be opened as ZIP; identifiers replaced byte-wise",
    "zip_member_unreadable": "some ZIP members could not be read (encrypted or corrupt) and are omitted from the output archive",
    "tar_unreadable": "archive could not be opened as TAR; identifiers replaced byte-wise",
    "gzip_unreadable": "gzip stream unreadable or larger than 4 GiB uncompressed; identifiers replaced byte-wise",
    "7z_unreadable": "7z archive could not be extracted; identifiers replaced byte-wise",
    "pdf_unreadable": "file could not be parsed as PDF; identifiers replaced byte-wise",
    "pdf_rewrite_failed": "PDF could not be re-saved after redaction; identifiers replaced byte-wise",
    "sw_record_masked": "some identifiers inside fixed-size compressed SolidWorks records were masked with 'x' instead of a pseudonym",
    "sw_record_unfit": "some compressed SolidWorks records could not be rewritten in place",
}


def safe_rel(path):
    p = Path(path)
    return path and not p.is_absolute() and ".." not in p.parts


NOT_IDENTITIES = set("""
monday tuesday wednesday thursday friday saturday sunday january february march april may june
july august september october november december red green blue black white grey gray yellow orange
purple pink brown silver gold china england britain france germany italy spain japan korea india
canada mexico brazil russia europe asia africa america australia taiwan poland sweden norway denmark
finland netherlands holland belgium switzerland austria portugal greece turkey israel egypt
ukraine vietnam thailand singapore malaysia indonesia iran pakistan argentina chile colombia
peru york london paris berlin tokyo beijing shanghai california texas florida kentucky ohio
washington oregon illinois virginia georgia carolina boston chicago seattle jesus christ
god user-images git-lfs raw.githubusercontent github gitlab gitee nature hope honor bright judges
mars thai python centre center miles true-nature research-collection google-analytics a-pdf
pdf-tools swing-layout
""".split())


def drop_token(t, pinned=()):
    if t in pinned:  # named by the manifest path itself (repo owner, project, site)
        return len(set(t)) <= 2
    return t in NOT_IDENTITIES or len(set(t)) <= 2


def content_tokens(ids, pinned=()):
    """(tokens safe to replace inside file contents, tokens that are also ordinary words and
    are replaced only when Capitalized).  Paths use every token except NOT_IDENTITIES."""
    from wordfreq import zipf_frequency
    from .harvest import _dictionary
    lower_words, proper_words = _dictionary()
    keep, proper_only = {}, set()
    for t, k in ids.items():
        if len(t) < 4 or drop_token(t, pinned):
            continue
        if t in lower_words:
            name = t in proper_words or k in ("person", "first", "last")
            if t not in pinned and t not in proper_words and zipf_frequency(t, "en") >= 4.0:
                continue
            if not name and len(t) == 4:
                continue
            proper_only.add(t)
        keep[t] = k
    return keep, proper_only


def _init_worker(token_map, exact, path_map, proper_only=(), whole=()):
    _W["m"] = Matcher(token_map, exact, proper_only, whole)
    _W["p"] = Matcher(path_map, exact)


def rename_component(m, comp):
    return m.replace_str(comp)


def rename_path(m, path):
    return "/".join(rename_component(m, c) for c in path.split("/"))


def sevenzip_rewrite(data, ctx):
    with tempfile.TemporaryDirectory() as td:
        src = Path(td, "in.7z")
        src.write_bytes(data)
        x = Path(td, "x")
        if subprocess.run(["7z", "x", "-y", f"-o{x}", str(src)], capture_output=True).returncode:
            ctx.note("7z_unreadable")
            return generic(data, ctx)
        y = Path(td, "y")
        for root, _d, files in os.walk(x):
            for fn in files:
                p = Path(root, fn)
                rel = p.relative_to(x).as_posix()
                q = y / "/".join(ctx.rename(c) for c in rel.split("/"))
                q.parent.mkdir(parents=True, exist_ok=True)
                q.write_bytes(process_bytes(p.read_bytes(), ext_of(fn), ctx, 1))
        out = Path(td, "out.7z")
        subprocess.run(["7z", "a", "-y", str(out), "."], cwd=y, capture_output=True, check=True)
        return out.read_bytes()


def transform_one(src, dst, ext):
    m = _W["m"]
    stats = {}
    ctx = Ctx(m, lambda c: rename_component(_W["p"], c), stats)
    size = os.path.getsize(src)
    dst.parent.mkdir(parents=True, exist_ok=True)
    with open(src, "rb") as f:
        head = f.read(8)
    if size > STREAM_LIMIT and not head.startswith((b"%PDF", b"PK\x03\x04", b"\x1f\x8b")):
        # huge STEP/IGES/mesh files: chunked same-length replacement with overlap
        ov = 512
        binary = not looks_text(head + open(src, "rb").read(8192))
        with open(src, "rb") as fi, open(dst, "wb") as fo:
            carry = b""
            while True:
                buf = fi.read(CHUNK)
                if not buf:
                    fo.write(m.replace(carry, binary=binary)[0])
                    break
                data = carry + buf
                new, _ = m.replace(data, binary=binary)
                fo.write(new[:-ov])
                carry = new[-ov:]
        stats["streamed"] = 1
        return stats
    data = Path(src).read_bytes()
    if ext in SEVENZIP_EXT and head.startswith(b"7z\xbc\xaf"):
        out = sevenzip_rewrite(data, ctx)
    else:
        out = process_bytes(data, ext, ctx)
    with open(dst, "wb") as fo:
        fo.write(out)
    return stats


def harvest_one(src, ext):
    h = Harvest()
    data = Path(src).read_bytes() if os.path.getsize(src) <= STREAM_LIMIT else None
    if data is None:
        with open(src, "rb") as f:
            data = f.read(8 * 2**20)
    try:
        harvest_file(h, data, ext)
    except Exception as e:
        log.warning("harvest failed %s: %s", src, e)
    return h.ids


# --------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("phase", choices=["harvest", "transform", "report"])
    ap.add_argument("--readme", help="README.txt to write (report)")
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--state", required=True, help="private state dir (identity map, progress)")
    ap.add_argument("--work", required=True, help="scratch dir for raw downloads")
    ap.add_argument("--out", help="output corpus dir (transform)")
    ap.add_argument("--input", help="use an already-downloaded input tree instead of the mirror")
    ap.add_argument("--base", default=BASE)
    ap.add_argument("--jobs", type=int, default=os.cpu_count())
    ap.add_argument("--dl-jobs", type=int, default=24)
    ap.add_argument("--only", help="only groups containing this substring")
    ap.add_argument("--stop-out-gb", type=float, default=0, help="stop when --out grows beyond this")
    ap.add_argument("--batch-gb", type=float, default=3.0, help="raw bytes per download/process batch")
    ap.add_argument("--salt", default=os.environ.get("DEID_SALT"),
                    help="secret pseudonym key; default: random key kept in --state/salt")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    state = Path(a.state)
    state.mkdir(parents=True, exist_ok=True)
    os.chmod(state, 0o700)  # holds the identity map
    if a.salt is None:
        salt_file = state / "salt"
        if not salt_file.exists():
            salt_file.write_bytes(os.urandom(32).hex().encode())
        a.salt = salt_file.read_text().strip()
    rows = read_manifest(a.manifest)
    unsafe = [r for r in rows if not safe_rel(r["path"]) or not safe_rel(r["url"])]
    rows = [r for r in rows if safe_rel(r["path"]) and safe_rel(r["url"])]
    if a.input:
        have = set()
        for root, _d, files in os.walk(a.input):
            for fn in files:
                have.add(Path(root, fn).relative_to(a.input).as_posix())
        rows = [r for r in rows if r["path"] in have]
    grp = groups(rows)
    if a.only:
        grp = OrderedDict((k, v) for k, v in grp.items() if a.only in k)
    done_file = state / f"{a.phase}.done"
    done = set(done_file.read_text().split("\n")) if done_file.exists() else set()
    ids_file = state / "ids.json"
    ids = json.loads(ids_file.read_text()) if ids_file.exists() else {}
    seeds = state / "seeds.txt"  # extra identities found by manual review; private, like ids.json

    if a.phase == "harvest":
        h = Harvest()
        for r in rows:
            h.from_path(r["path"])
        for tok, kind in h.ids.items():
            ids.setdefault(tok, kind)
    reg = Registry.load(state / "pseudonyms.json", a.salt.encode())
    reg.reserved = set(ids)
    for k, v in list(reg.fwd.items()):
        if v in reg.reserved:
            del reg.fwd[k], reg.rev[v]
    work = Path(a.work)
    defects = json.loads((state / "defects.json").read_text()) if (state / "defects.json").exists() else {}
    for r in unsafe:
        defects[r["path"]] = "manifest path is absolute or contains '..'; not processed"
    dl = cf.ThreadPoolExecutor(a.dl_jobs)

    def fetch(r, sub):
        if a.input:
            return Path(a.input, r["path"])
        p = work / sub / r["path"]
        download(r, p, a.base)
        return p

    # merge small source groups into batches so the worker pool stays busy
    batches, cur, cur_bytes, cur_n = [], [], 0, 0
    for gname, grows in grp.items():
        if gname in done:
            continue
        for k, r in enumerate(grows):
            if not cur or cur[-1][0] != gname:
                cur.append((gname, [], False))
            cur[-1][1].append(r)
            cur_bytes += r["size"]
            cur_n += 1
            if k == len(grows) - 1:
                cur[-1] = (gname, cur[-1][1], True)
            if cur_bytes > a.batch_gb * 1e9 or cur_n >= 4000:
                batches.append(cur)
                cur, cur_bytes, cur_n = [], 0, 0
    if cur:
        batches.append(cur)

    def fetch_batch(i):
        rows_ = [r for _g, gr, _l in batches[i] for r in gr]
        return list(dl.map(lambda r: fetch(r, f"b{i}"), rows_))

    def build_matcher():
        if seeds.exists():
            for line in seeds.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    tok, _, kind = line.partition("\t")
                    ids.setdefault(tok.lower(), kind or "org")
        ph = Harvest()
        for r in read_manifest(a.manifest):
            ph.from_path(r["path"])
        pinned = set(ph.ids) & set(ids)
        (state / "pinned.json").write_text(json.dumps(sorted(pinned)))
        reg.reserved = set(ids)
        pm = {t: reg.get(t, k) for t, k in ids.items() if not drop_token(t, pinned)}
        reg.save(state / "pseudonyms.json")
        keep, proper_only = content_tokens(ids, pinned)
        tm = {t: pm[t] for t in keep}
        exact = [t for t in pm if len(t) <= 4]
        return tm, exact, pm, proper_only

    if a.phase in ("transform", "report"):
        tm, exact, pm, proper_only = build_matcher()
        path_m = Matcher(pm, exact)
        # output paths for the whole manifest up front, so renames never collide
        newpaths, claimed = {}, set()
        for r in read_manifest(a.manifest):
            if not safe_rel(r["path"]):
                continue
            np_ = rename_path(path_m, r["path"])
            if np_ in claimed:
                stem, dot, ext_ = np_.rpartition(".") if "." in np_.rsplit("/", 1)[-1] else (np_, "", "")
                k = 2
                while f"{stem}~{k}{dot}{ext_}" in claimed:
                    k += 1
                np_ = f"{stem}~{k}{dot}{ext_}"
            claimed.add(np_)
            newpaths[r["path"]] = np_
        # references to renamed files must follow the rename even where the identity in the
        # name is not replaced in free text (short or ordinary-word identities)
        content_m = Matcher(tm, exact, proper_only)
        aliases = set()
        for old, new in newpaths.items():
            ob, nb = old.rsplit("/", 1)[-1], new.rsplit("/", 1)[-1]
            if ob != nb and len(ob) >= 6 and len(ob.encode()) == len(nb.encode()) and content_m.replace_str(ob) != nb:
                if ob.lower() not in tm:
                    tm[ob.lower()] = nb.lower()
                    aliases.add(ob.lower())
        pool = cf.ProcessPoolExecutor(a.jobs, initializer=_init_worker, initargs=(tm, exact, pm, proper_only, aliases))
        _init_worker(tm, exact, pm, proper_only, aliases)
        orig_of = {v: k for k, v in newpaths.items()}
        if a.phase == "report":
            from .report import write_readme
            write_readme(a.readme, read_manifest(a.manifest), newpaths, defects, a.out, state / "written.tsv")
            return
        out = Path(a.out)
        ledger = open(state / "written.tsv", "a", encoding="utf-8", buffering=1)  # every output written, across resumed/emptied runs
        seen = {}
        seen_file = state / "md5_out.json"
        if seen_file.exists():
            seen = json.loads(seen_file.read_text())
    else:
        pool = cf.ProcessPoolExecutor(a.jobs)

    prefetch = cf.ThreadPoolExecutor(1)
    nxt = prefetch.submit(fetch_batch, 0) if batches else None
    harvested_md5 = set()
    for bi, batch in enumerate(batches):
        t0 = time.time()
        grows = [r for _g, gr, _l in batch for r in gr]
        srcs = nxt.result()
        nxt = prefetch.submit(fetch_batch, bi + 1) if bi + 1 < len(batches) else None
        futs = {}
        for r, src in zip(grows, srcs):
            ext = ext_of(r["path"])
            if not src.exists():
                defects[r["path"]] = "missing from mirror"
                continue
            sz = src.stat().st_size
            complete = sz == r["size"] and sz > 0
            if r["size"] == 0 or sz == 0:
                defects[r["path"]] = "zero-byte file in source" if r["size"] == 0 else "not downloadable from mirror"
            if a.phase == "harvest":
                if r["md5"] and r["md5"] in harvested_md5:
                    continue
                if complete and r["md5"]:
                    harvested_md5.add(r["md5"])
                futs[pool.submit(harvest_one, str(src), ext)] = r
            else:
                with open(src, "rb") as f:
                    head = f.read(512)
                if sz and is_html_placeholder(head, ext):
                    defects[r["path"]] = "source file is an HTML 'Not Found' page, not a %s file" % (ext or "data")
                newp = newpaths[r["path"]]
                dst = out / newp
                key = r["md5"] + ext if complete and r["md5"] else ""
                if key and key in seen and Path(out, seen[key]).exists():
                    if seen[key] != newp:
                        dst.parent.mkdir(parents=True, exist_ok=True)
                        if dst.exists():
                            dst.unlink()
                        os.link(Path(out, seen[key]), dst)
                        ledger.write(f"{newp}\t{dst.stat().st_size}\t1\n")
                        src_defect = defects.get(orig_of.get(seen[key]))
                        if src_defect and r["path"] not in defects:
                            defects[r["path"]] = src_defect
                    continue
                futs[pool.submit(transform_one, str(src), dst, ext)] = (r, newp, complete)
        for fu in cf.as_completed(futs):
            info = futs[fu]
            try:
                res = fu.result()
            except Exception as e:
                r = info if a.phase == "harvest" else info[0]
                log.warning("failed %s: %s", r["path"], e)
                if a.phase == "transform":
                    # never drop a file: fall back to plain same-length replacement
                    newp = info[1]
                    src = Path(a.input, r["path"]) if a.input else work / f"b{bi}" / r["path"]
                    data = src.read_bytes()
                    (out / newp).parent.mkdir(parents=True, exist_ok=True)
                    (out / newp).write_bytes(_W["m"].replace(data, binary=not looks_text(data))[0])
                    ledger.write(f"{newp}\t{(out / newp).stat().st_size}\t0\n")
                    defects.setdefault(r["path"], "format rewrite failed (%s); identifiers replaced byte-wise" % type(e).__name__)
                continue
            if a.phase == "harvest":
                for tok, kind in res.items():
                    ids.setdefault(tok, kind)
            else:
                r, newp, complete = info
                notes = [NOTE_DEFECTS[k] for k in NOTE_DEFECTS if res.get(k)]
                if notes and r["path"] not in defects:
                    defects[r["path"]] = "; ".join(notes)
                key = r["md5"] + ext_of(r["path"]) if r["md5"] and complete else ""
                first = seen.get(key) if key else None
                if first and first != newp and Path(out, first).exists():
                    (out / newp).unlink()
                    os.link(Path(out, first), out / newp)
                    ledger.write(f"{newp}\t{(out / newp).stat().st_size}\t1\n")
                    continue
                if key:
                    seen[key] = newp
                ledger.write(f"{newp}\t{(out / newp).stat().st_size}\t0\n")
        if not a.input:
            shutil.rmtree(work / f"b{bi}", ignore_errors=True)
        done.update(g for g, _gr, last in batch if last)
        done_file.write_text("\n".join(sorted(done)))
        ids_file.write_text(json.dumps(ids, ensure_ascii=False, sort_keys=True, indent=0))
        (state / "defects.json").write_text(json.dumps(defects, ensure_ascii=False, indent=0, sort_keys=True))
        if a.phase == "transform":
            seen_file.write_text(json.dumps(seen))
        log.info("%s batch %d/%d (%s ...): %d files in %.0fs (ids=%d)", a.phase, bi + 1, len(batches), batch[0][0], len(grows), time.time() - t0, len(ids))
        if a.phase == "transform" and a.stop_out_gb:
            used = int(subprocess.run(["du", "-sb", str(out)], capture_output=True, text=True).stdout.split()[0])
            if used > a.stop_out_gb * 1e9:
                log.info("output reached %.1f GB; stopping for upload", used / 1e9)
                break
    pool.shutdown()


if __name__ == "__main__":
    main()
