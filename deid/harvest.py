"""Identity discovery: source/owner/project names from corpus paths, and
people, organisations, accounts, e-mails and domains from file contents."""
import re
from functools import lru_cache

from wordfreq import zipf_frequency

KEEP = set("""
cad cam cae step stp iges igs stl obj dxf dwg pdf svg png jpg sldprt sldasm slddrw ipt iam idw fcstd
asm prt drw par psm dft catpart catproduct catdrawing jt x_t x_b sat brep 3mf vrml wrl ply amf
iso din ansi astm asme jis gb bs en nist ise iec ieee sae api usb pcb led lcd cnc pla abs petg tpu
nema gt2 m3 m4 m5 m6 m8 rev ver v1 v2 v3 assy assembly part parts drawing drawings model models
github gitlab www http https com org net edu gov io de uk fr cn jp main master readme license src
freecad solidworks inventor fusion onshape catia creo autocad rhino sketchup openscad blender kicad
eagle altium siemens nx parasolid acis opencascade occt ezdxf windows linux macos users user home
desktop documents temp appdata local admin administrator owner unknown default template templates
sample samples test tests example examples demo library libraries hardware mechanical mechanics
electronics design designs project projects final old new copy backup archive files file data docs
doc images image media assets resources export exports output input original native print printed
""".split())

ENG = set("""antibacklash bowdentube breadboard conic crosshead cryostat cycloidal cycloid cycloidgen dimensioning
endstop extruder gearcutting heatbreak heatsink hotend idler inputpulley outputpulley kettenspanner lagerblock
loadcell lubricator middleplate networkcable preamp quadrotor reductor ritzel softjaws spoolholder stepper
tensioner toolchanger turbomachine weldment zahnrad uncased sorter mouse metro city base duo public staff
archives courses journals website-files xyz seas typeb arc line circle point text solid face edge shell axis
plane spline ellipse polyline lwpolyline insert block layer dimension hatch vertex entity entities section
header tables viewport style ltype ucs appid units wire mesh brep nurbs surface curve product shape context
cubesat electromagnetics protoplate starterkit technicaldrawing technicaldrawingparts uppermass weldment
pmtlensholder pmtmountplateshutter upperdetarmbaseshutter portalmillingmachine ramanspectrometer houseplan
coldstagedesign gaseouspiston diyharmonicastronomymount teslaturbine collider testbeam bloodflow airquality
implantables crayford torsen softjaws solidwork cadmodels dmodels fabricationfiles mechanicaldesign cnccmv
cncbasics cncrebuild crebuild sourcecad freestep benchtop rapidtool visebase origami case06 cust ord ctc lod
uni peer global ups isr cif cplus ftc d638 dnp hdx rm2 xpix sduino frosti rebot meso spim nims ppm4eu phy esr pmp trc ami
""".split())

GENERIC_SECOND = set("""cdn files static assets media www api docs blob core windows prod dev ftp
images img download downloads storage s3 amazonaws cloudfront github io wordpress blogspot gitlab
""".split())
TLDS = set("""com org net edu gov io de uk fr cn jp it es nl be ch at au ca us ru in br mx ar pt pl se
no fi dk cz hu tr ir iq ec lu ac co info biz eu global studio science engineering tech""".split())


@lru_cache(maxsize=200000)
def common(word):
    w = word.lower()
    if w in KEEP:
        return True
    if any(c.isdigit() for c in w):
        return True
    return max(zipf_frequency(w, "en"), zipf_frequency(w, "de"), zipf_frequency(w, "fr"),
               zipf_frequency(w, "es"), zipf_frequency(w, "it")) >= 3.0


SPLIT = re.compile(r"[^0-9A-Za-zÀ-ÖØ-öø-ÿĀ-ž]+")
CAMEL = re.compile(r"[A-Z]+(?![a-z])|[A-Z]?[a-z]+|[0-9]+")


def word_tokens(s):
    for chunk in SPLIT.split(s):
        if chunk:
            yield chunk
            parts = CAMEL.findall(chunk)
            if len(parts) > 1:
                yield from parts


ROLE = set("""author authors admin administrator administrador user users owner engineer engineering company
corp corporation inc ltd llc gmbh dept department design designer drafter draftsman checker approver
student teacher guest unknown none default anonymous editor contributor manual info mail date created
modified lab laptop desktop first free and the of for by pc workstation office home student staff
team group project projects system systems service services support sales contact customer client
""".split())
TOKEN_OK = re.compile(r"^[0-9A-Za-zÀ-ÖØ-öø-žЀ-ӿ一-鿿][A-Za-zÀ-ÖØ-öø-žЀ-ӿ一-鿿0-9'.\\_-]*$")
GOOD_TLD = TLDS | set("""org com net edu gov mil int info biz name pro aero coop museum io co ai app dev xyz
online site tech store cloud email""".split())


@lru_cache(maxsize=1)
def _dictionary():
    """(lowercase common words, lowercased proper nouns) from the system word list."""
    lower, proper = set(), set()
    try:
        for w in open("/usr/share/dict/words", encoding="utf-8", errors="ignore"):
            w = w.strip()
            if not w or "'" in w:
                continue
            (proper if w[0].isupper() else lower).add(w.lower())
    except OSError:
        pass
    return lower, proper


class Harvest:
    def __init__(self):
        self.ids = {}  # token -> kind

    def add(self, tok, kind="org", force=False):
        tok = tok.strip().strip(".-_'\"")
        if len(tok) < 3 or len(tok) > 64:
            return
        low = tok.lower()
        if low in KEEP or low in ENG or low.isdigit():
            return
        if not force and common(low):
            return
        if not force and re.fullmatch(r"[a-z]{3}", low):
            return
        if not TOKEN_OK.match(tok) or low in ROLE:
            return
        letters = sum(c.isalpha() for c in low)
        if letters < 3 or letters < 0.6 * len(low):
            return
        if low not in self.ids:
            self.ids[low] = kind

    def add_name(self, full, kind="person"):
        full = full.strip()
        if not full or len(full) > 80 or "@" in full:
            return
        words = [w for w in re.split(r"[\s,;/\\]+", full) if w]
        if not words or len(words) > 5:
            return
        if kind == "person" and len(words) >= 2:
            for i, w in enumerate(words):
                if len(w) >= 3 and w[0].isalpha():
                    self.add(w, "first" if i == 0 else "last", force=self.namelike(w))
        else:
            for w in words:
                for t in word_tokens(w):
                    self.add(t, "first" if kind == "person" else "org", force=(kind == "person" and self.namelike(t)))
            if len(words) > 1:
                pass

    @staticmethod
    def namelike(w):
        """Person-context token: reject plain dictionary words that are not also proper nouns."""
        low = w.lower()
        if low in ROLE or low in KEEP or low in ENG or len(low) < 3:
            return False
        lower_words, proper_words = _dictionary()
        if low in lower_words and low not in proper_words:
            return False
        return zipf_frequency(low, "en") < 5.5

    def add_slug(self, slug, force_whole=True):
        """GitHub/GitLab owner names: the whole account plus its rare parts."""
        if force_whole:
            self.add(slug, "org", force=True)
        for t in word_tokens(slug):
            self.add(t, "org")

    def add_project(self, name):
        """Repository/project names: keep only distinctive (rare) words."""
        for t in word_tokens(name):
            if zipf_frequency(t.lower(), "en") < 2.0 and len(t) >= 4:
                self.add(t, "org")

    def add_domain(self, host):
        host = host.lower().strip(".")
        labels = [l for l in host.split(".") if l]
        for l in labels:
            if l in TLDS or l in GENERIC_SECOND or len(l) < 3:
                continue
            self.add(l, "org", force=not common(l.replace("-", "")))

    # ---------------------------------------------------------------- paths
    def from_path(self, path):
        parts = path.split("/")
        if parts[0] == "archives" and len(parts) > 1:
            name = parts[1].rsplit(".", 1)[0]
            bits = name.split("__")
            self.add_slug(bits[0])
            for b in bits[1:]:
                if not re.fullmatch(r"[0-9a-f]{12}", b):
                    self.add_slug(b, force_whole=False)
            return
        if len(parts) < 3 or parts[0] != "sources":
            return
        group, top = parts[1], parts[2]
        if group == "github":
            bits = top.split("__")
            self.add_slug(bits[0])
            for b in bits[1:]:
                if not re.fullmatch(r"[0-9a-f]{12}", b):
                    self.add_project(b)
        elif group == "gitlab":
            bits = [b for b in top.split("__") if b and b != "gitlab.com"]
            if bits:
                self.add_slug(bits[0])
            for b in bits[1:]:
                self.add_project(b)
        elif group == "web":
            self.add_domain(top)
        elif group == "catalog":
            for t in word_tokens(top):
                if t.isupper() and 3 <= len(t) <= 8 and zipf_frequency(t.lower(), "en") < 3.5:
                    self.add(t, "org", force=True)
                elif t[:1].isupper() and zipf_frequency(t.lower(), "en") < 2.0:
                    self.add(t, "org")
        elif group == "existing":
            for comp in parts[3:6]:
                for t in word_tokens(comp):
                    if t[:1].isupper() and zipf_frequency(t.lower(), "en") < 2.5:
                        self.add(t, "org")

    # -------------------------------------------------------------- content
    EMAIL = re.compile(rb"[A-Za-z0-9._%+-]{2,64}@([A-Za-z0-9-]{2,63}\.)+[A-Za-z]{2,12}")
    URL = re.compile(rb"(?:https?://|www\.)([A-Za-z0-9-]{2,63}(?:\.[A-Za-z0-9-]{2,63})+)")
    WINUSER = re.compile(rb"(?i)[A-Z]:[\\/]+(?:Users|Documents and Settings|home)[\\/]+([^\\/\x00-\x1f\"<>|:*?]{2,40})[\\/]")
    UNIXUSER = re.compile(rb"/(?:home|Users)/([A-Za-z0-9._-]{2,32})/")
    STEP_FN = re.compile(rb"FILE_NAME\s*\(\s*'(?:[^']|'')*'\s*,\s*'[^']*'\s*,\s*\(([^)]*)\)\s*,\s*\(([^)]*)\)", re.S)
    XMLPROP = re.compile(rb"<(dc:creator|cp:lastModifiedBy|dc:lastModifiedBy|Author|Company|Manager|meta:initial-creator|dc:publisher)>([^<]{2,80})</")
    SWPROP = re.compile(rb"<property[^>]*name=\"(Author|DrawnBy|Drawn ?By|CheckedBy|Checked ?By|Engineer|ApprovedBy|Approved ?By|Designer|Designed ?By|Company|CompanyName|Owner|Customer|Client|Supplier|Vendor|Manufacturer|Contact|Email|Project|Projekt)\"[^>]*>\s*<vt:lpwstr>([^<]{2,80})</", re.I)
    COPY = re.compile(rb"(?:Copyright|\(c\)|\xc2\xa9)\s*(?:\(c\)\s*)?(?:[0-9]{4}(?:\s*[-,]\s*[0-9]{4})?\s*)?(?:by\s+)?([A-Z][A-Za-z&.'-]+(?:\s+[A-Z][A-Za-z&.'-]+){0,3})")
    LABEL = re.compile(rb"(?i)\b(?:drawn|checked|approved|designed|author|engineer|created|modified)\s*(?:by)?\s*[:=]\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})")

    def from_bytes(self, data, ext=""):
        u16 = None
        if b"\x00" in data[:4096] and len(data) < 64 * 2**20:
            try:
                u16 = data.decode("utf-16-le", "ignore").encode("utf-8", "ignore")
            except Exception:
                u16 = None
        for blob in (data, u16) if u16 else (data,):
            for m in self.EMAIL.finditer(blob):
                local, _, host = m.group(0).decode("latin-1").partition("@")
                labels = host.lower().split(".")
                if labels[-1] not in GOOD_TLD or any(len(l) < 2 for l in labels) or not any(len(l) >= 3 for l in labels[:-1]):
                    continue
                if m.start() > 0 and (chr(blob[m.start() - 1]).isalnum()):
                    continue
                self.add(local, "org", force=True)
                for t in word_tokens(local):
                    self.add(t, "first", force=self.namelike(t))
                self.add_domain(host)
            for m in self.URL.finditer(blob):
                host = m.group(1).decode("latin-1")
                if host.lower().rsplit(".", 1)[-1] in GOOD_TLD:
                    self.add_domain(host)
            for m in self.WINUSER.finditer(blob):
                self.add_name(m.group(1).decode("latin-1"), "person")
            for m in self.UNIXUSER.finditer(blob):
                self.add(m.group(1).decode("latin-1"), "first", force=True)
            for m in self.XMLPROP.finditer(blob):
                self.add_name(m.group(2).decode("utf-8", "ignore"), "org" if m.group(1) in (b"Company", b"dc:publisher") else "person")
            for m in self.SWPROP.finditer(blob):
                k = m.group(1).lower()
                self.add_name(m.group(2).decode("utf-8", "ignore"), "person" if b"by" in k or k in (b"author", b"engineer", b"designer", b"contact") else "org")
            for m in self.COPY.finditer(blob):
                self.add_name(m.group(1).decode("latin-1"), "org")
            for m in self.LABEL.finditer(blob):
                self.add_name(m.group(1).decode("latin-1"), "person")
        for m in self.STEP_FN.finditer(data[:20000]):
            for grp, kind in ((m.group(1), "person"), (m.group(2), "org")):
                for v in re.findall(rb"'((?:[^']|'')*)'", grp):
                    v = v.decode("latin-1").strip()
                    if v and v.lower() not in ("unknown", "none", "author", "organization", "-", "user"):
                        self.add_name(v, kind)

    def from_ole(self, path_or_bytes):
        import olefile
        try:
            ole = olefile.OleFileIO(path_or_bytes)
            meta = ole.get_metadata()
        except Exception:
            return
        for attr, kind in (("author", "person"), ("last_saved_by", "person"), ("company", "org"),
                           ("manager", "person")):
            v = getattr(meta, attr, None)
            if v:
                self.add_name(v.decode("latin-1", "ignore") if isinstance(v, bytes) else str(v), kind)
        ole.close()

    def from_pdf_meta(self, data):
        import fitz
        try:
            doc = fitz.open(stream=data, filetype="pdf")
            md = doc.metadata or {}
            doc.close()
        except Exception:
            return
        if md.get("author"):
            self.add_name(md["author"], "person")


def harvest_file(h, data, ext):
    """Run every content extractor that applies to one file's bytes."""
    from .formats import SW_EXT, sw_iter_records
    big = len(data) > 16 * 2**20
    if ext in (".stl", ".obj", ".ply", ".off", ".wrl", ".vrml", ".3ds", ".glb", ".gltf") and len(data) > 2**20:
        data = data[:2**20]
        big = False
    sample = data[:2 * 2**20] + data[-2**20:] if len(data) > 4 * 2**20 else data
    h.from_bytes(sample, ext)
    if data[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1":
        h.from_ole(data)
    if data[:4] == b"%PDF" and not big:
        h.from_pdf_meta(data)
    if ext in SW_EXT and not big:
        for _name, raw in sw_iter_records(data, max_usize=2 * 2**20):
            h.from_bytes(raw, ext)
    if data[:4] == b"PK\x03\x04" and not big:
        import io
        import zipfile
        try:
            z = zipfile.ZipFile(io.BytesIO(data))
            for info in z.infolist()[:2000]:
                low = info.filename.lower()
                if info.file_size < 32 * 2**20 and low.endswith((".xml", ".txt", ".json", ".rels", ".step", ".stp", ".fcstd", ".pdf", ".md", ".csv", ".dxf", ".svg", ".html", ".model", ".config")):
                    harvest_file(h, z.read(info), "." + low.rsplit(".", 1)[-1])
        except Exception:
            pass
