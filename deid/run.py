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

from .formats import Ctx, process_bytes
from .harvest import Harvest, harvest_file
from .matcher import Matcher
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


def _init_worker(token_map, exact):
    m = Matcher(token_map, exact)
    _W["m"] = m


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
            return ctx.m.replace(data)[0]
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
    ctx = Ctx(m, lambda c: rename_component(m, c), stats)
    size = os.path.getsize(src)
    dst.parent.mkdir(parents=True, exist_ok=True)
    with open(src, "rb") as f:
        head = f.read(8)
    if size > STREAM_LIMIT and not head.startswith((b"%PDF", b"PK\x03\x04", b"\x1f\x8b")):
        # huge STEP/IGES/mesh files: chunked same-length replacement with overlap
        ov = 512
        with open(src, "rb") as fi, open(dst, "wb") as fo:
            carry = b""
            while True:
                buf = fi.read(CHUNK)
                if not buf:
                    fo.write(m.replace(carry)[0])
                    break
                data = carry + buf
                new, _ = m.replace(data)
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
    ap.add_argument("phase", choices=["harvest", "transform"])
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
    ap.add_argument("--salt", default=os.environ.get("DEID_SALT", "machina-deid"))
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    state = Path(a.state)
    state.mkdir(parents=True, exist_ok=True)
    rows = read_manifest(a.manifest)
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
    seeds = Path(__file__).with_name("seeds.txt")

    if a.phase == "harvest":
        h = Harvest()
        for r in rows:
            h.from_path(r["path"])
        for tok, kind in h.ids.items():
            ids.setdefault(tok, kind)
    reg = Registry.load(state / "pseudonyms.json", a.salt.encode())
    work = Path(a.work)
    defects = json.loads((state / "defects.json").read_text()) if (state / "defects.json").exists() else {}
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
        tm = {t: reg.get(t, k) for t, k in ids.items()}
        reg.save(state / "pseudonyms.json")
        exact = [t for t in tm if len(t) <= 4]
        return tm, exact

    if a.phase == "transform":
        tm, exact = build_matcher()
        pool = cf.ProcessPoolExecutor(a.jobs, initializer=_init_worker, initargs=(tm, exact))
        _init_worker(tm, exact)
        out = Path(a.out)
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
            if r["size"] == 0 or sz == 0:
                defects[r["path"]] = "zero-byte file in source" if r["size"] == 0 else "not downloadable from mirror"
            if a.phase == "harvest":
                if r["md5"] and r["md5"] in harvested_md5:
                    continue
                harvested_md5.add(r["md5"])
                futs[pool.submit(harvest_one, str(src), ext)] = r
            else:
                with open(src, "rb") as f:
                    head = f.read(512)
                if sz and is_html_placeholder(head, ext):
                    defects[r["path"]] = "source file is an HTML 'Not Found' page, not a %s file" % (ext or "data")
                newp = rename_path(_W["m"], r["path"])
                dst = out / newp
                key = r["md5"] or ""
                if key and key in seen and Path(out, seen[key]).exists():
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    if dst.exists():
                        dst.unlink()
                    os.link(Path(out, seen[key]), dst)
                    continue
                futs[pool.submit(transform_one, str(src), dst, ext)] = (r, newp)
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
                    (out / newp).write_bytes(_W["m"].replace(data)[0])
                    defects.setdefault(r["path"], "format rewrite failed (%s); identifiers replaced byte-wise" % type(e).__name__)
                continue
            if a.phase == "harvest":
                for tok, kind in res.items():
                    ids.setdefault(tok, kind)
            else:
                r, newp = info
                if r["md5"]:
                    seen[r["md5"]] = newp
                if res.get("sw_record_unfit"):
                    defects.setdefault(r["path"], "some compressed SolidWorks records could not be rewritten in place")
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
