"""Scan a de-identified tree for any remaining original identity tokens (private report)."""
import argparse
import collections
import concurrent.futures as cf
import json
import os
from pathlib import Path

from .formats import SW_MARK, sw_iter_records
from .matcher import Matcher, looks_text
from .run import content_tokens

_M = {}


def _init(tm, proper_only):
    _M["m"] = Matcher(tm, (), proper_only)


def scan(path):
    m = _M["m"]
    hits = collections.Counter()
    with open(path, "rb") as f:
        while True:
            buf = f.read(64 * 2**20)
            if not buf:
                break
            if m.replace(buf, binary=not looks_text(buf))[1]:
                for t, c in m.matched(buf, binary=not looks_text(buf)).items():
                    hits[t] += c
    if path.suffix.lower() in (".sldprt", ".sldasm", ".slddrw") and path.stat().st_size < 2**30:
        data = path.read_bytes()
        if SW_MARK.search(data):
            for _name, raw in sw_iter_records(data):  # compressed records: scan their contents
                for t, c in m.matched(raw, binary=not looks_text(raw)).items():
                    hits[t] += c
    return str(path), hits


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--jobs", type=int, default=os.cpu_count())
    a = ap.parse_args()
    ids = json.loads(Path(a.state, "ids.json").read_text())
    pm = json.loads(Path(a.state, "pseudonyms.json").read_text())
    keep, proper_only = content_tokens(ids)
    tm = {t: pm[t] for t in keep if t in pm}
    files = [Path(r, f) for r, _d, fs in os.walk(a.out) for f in fs]
    total, per_tok, flagged = collections.Counter(), collections.Counter(), 0
    with cf.ProcessPoolExecutor(a.jobs, initializer=_init, initargs=(tm, proper_only)) as ex:
        for p, hits in ex.map(scan, files, chunksize=16):
            if hits:
                flagged += 1
                per_tok.update(hits)
    print(f"files scanned {len(files)}, files with residual tokens {flagged}")
    for t, c in per_tok.most_common(40):
        print(f"{c:6d} {t}")
    return total


if __name__ == "__main__":
    main()
