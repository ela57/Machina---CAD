"""Write the submission README.txt: how to run, counts, and every defect, using
de-identified output paths only."""
import collections
import os
from pathlib import Path

RUN = """\
How to run (Python 3.12; Debian/Ubuntu)
  sudo apt-get install -y p7zip-full wamerican-large
  pip install -r code/requirements.txt
  cd code
  curl -O https://machina-input.joinbuilders.org/manifest.tsv
  # 1. identity discovery over the whole corpus (paths + contents), streamed from the mirror
  python3 -m deid.run harvest   --manifest manifest.tsv --state STATE --work WORK
  # 2. de-identify every file into OUT (same streaming; MD5 duplicates hardlinked)
  python3 -m deid.run transform --manifest manifest.tsv --state STATE --work WORK --out OUT
  # 3. this README
  python3 -m deid.run report    --manifest manifest.tsv --state STATE --work WORK --out OUT --readme README.txt
STATE is private (identity map + random pseudonym key) and is never uploaded.  Optional
STATE/seeds.txt (token<TAB>kind per line) adds identities found by manual review of folder names.
--stop-out-gb N stops transform when OUT reaches N GB so it can be uploaded and emptied; rerunning resumes.
"""

METHOD = """\
Method
- Identities: repository owners/slugs and path user dirs from the manifest; emails, URLs/domains,
  OLE/SolidWorks/Inventor summary + custom properties, docProps XML, PDF metadata, STEP FILE_NAME
  author/organization, "Drawn by"/"Checked by"/copyright labels, Windows/Unix user-folder names.
  Common English and engineering vocabulary is excluded (wordfreq + system word list).
- Pseudonyms: deterministic HMAC of the identity, same length and letter case as the original, so
  one identity always maps to one pseudonym everywhere (paths, contents, archives) and distinct
  identities stay distinct.  Same byte length keeps offsets in native CAD, IGES, STEP, STL, DXF etc.
- Replacement is done in UTF-8, Latin-1 and UTF-16LE strings.  In binary files only matches that
  sit inside real text strings are rewritten, so geometry/compressed payloads are untouched.
- PDF: metadata, XMP, embedded files (recursively), annotations, link URIs, and visible text
  (redacted and re-typeset with the pseudonym).  ZIP/3MF/FCStd/TAR/GZ/7z: member names and
  contents, recursively.  SolidWorks 2015+ records are inflated, rewritten and re-deflated into
  their original compressed size (if it cannot fit, identifiers in that record are masked with x).
- File and folder names use the same mapping; references inside files change with them.
- Geometry, dimensions, tolerances, units, materials, configurations and BOM structure are not
  modified; only identifier strings are.
"""

LIMITS = """\
Known limitations
- Identifiers drawn only as raster pixels (logos, scanned title blocks, preview bitmaps) are not
  detected; PDF vector/text logos are redacted only where they are text.
- Identity discovery is heuristic; uncommon-word-looking engineering terms can be renamed and rare
  one-off names can be missed.
- Short (<4 letter) identities are renamed in paths only.
"""


def write_readme(path, rows, newpaths, defects, out, ledger=None):
    written = {}
    if ledger and Path(ledger).exists():
        for line in open(ledger, encoding="utf-8"):
            p, size, dup = line.rstrip("\n").split("\t")
            written[p] = (int(size), dup == "1")
    else:
        inodes = set()
        for root, _d, files in os.walk(out):
            for fn in files:
                q = Path(root, fn)
                st = os.stat(q)
                written[q.relative_to(out).as_posix()] = (st.st_size, st.st_ino in inodes)
                inodes.add(st.st_ino)
    n_out = len(written)
    n_links = sum(1 for _s, d in written.values() if d)
    n_bytes = sum(s for s, d in written.values() if not d)
    kinds = collections.Counter(defects.values())
    with open(path, "w", encoding="utf-8") as f:
        f.write("Machina CAD de-identification: submission\n\n")
        f.write("Layout\n  corpus/  de-identified files, same tree as the manifest with renamed components\n")
        f.write("  code/    the pipeline (deid/), requirements.txt\n  README.txt\n\n")
        f.write(RUN + "\n" + METHOD + "\n")
        f.write("Counts\n")
        f.write(f"  manifest entries: {len(rows)}\n  files in corpus/: {n_out} "
                f"({n_links} are byte-identical duplicates of another output)\n")
        f.write(f"  unique output bytes: {n_bytes}\n  files with a recorded defect: {len(defects)}\n")
        missing = [r for r in rows if newpaths.get(r["path"]) not in written]
        f.write(f"  manifest entries with no output file: {len(missing)}\n\n")
        f.write("Defects by kind\n")
        for k, c in kinds.most_common():
            f.write(f"  {c:7d}  {k}\n")
        f.write("\n" + LIMITS + "\n")
        f.write("Every defect (output path <TAB> defect); the file is still present in corpus/ unless stated\n")
        for p in sorted(defects, key=lambda p: newpaths.get(p, p)):
            f.write(f"{newpaths.get(p, '(unsafe manifest path omitted)')}\t{defects[p]}\n")
        if missing:
            f.write("\nManifest entries with no output file\n")
            for r in missing:
                f.write(newpaths.get(r["path"], "(unsafe manifest path)") + "\n")
