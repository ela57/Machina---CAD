"""Find identity tokens in raw bytes (Latin-1/UTF-8 and UTF-16LE) and replace
them in place with same-length pseudonyms, keeping the original letter case."""
import ahocorasick

from .pseudo import apply_case

ENCODINGS = ("utf-8", "utf-16-le", "latin-1")


def _isalnum_ch(c):
    return c.isalnum()


_PRINTABLE = frozenset(range(0x20, 0x7F)) | {0x09, 0x0A, 0x0D}


def looks_text(data, n=8192):
    """True when the sample is almost entirely printable ASCII / UTF-8 text."""
    sample = data[:n]
    if not sample:
        return True
    bad = sum(1 for b in sample if b not in _PRINTABLE and b < 0x80)
    return bad <= len(sample) // 100


def _printable_run(data, start, end, limit=16):
    """Number of printable single-byte characters around data[start:end] (inclusive)."""
    i, j = start, end
    while i > 0 and start - i < limit and data[i - 1] in _PRINTABLE:
        i -= 1
    while j < len(data) and j - end < limit and data[j] in _PRINTABLE:
        j += 1
    return j - i


def _counted_string(data, start, end):
    """data[start:end] is a whole (or leading part of a) length-prefixed string, e.g. an OLE
    VT_LPSTR property: 4-byte little-endian length (incl. NUL) right before it."""
    if start < 4:
        return False
    n = int.from_bytes(data[start - 4:start], "little")
    return end - start < n <= 4096 and data[start + n - 1:start + n] == b"\0"


class Matcher:
    def __init__(self, token_map, exact_case=(), proper_only=()):
        """token_map: lowercase token -> lowercase pseudonym (same length)."""
        self.token_map = dict(token_map)
        self.exact = set(exact_case)
        self.proper_only = set(proper_only)  # also ordinary words: replace only when Capitalized
        self.auto = ahocorasick.Automaton()
        n = 0
        for tok, ps in self.token_map.items():
            for enc in ENCODINGS:
                try:
                    kb = tok.encode(enc)
                except UnicodeEncodeError:
                    continue
                key = kb.decode("latin-1").lower()
                if enc == "latin-1" and tok.isascii():
                    continue  # same as utf-8
                if enc == "utf-16-le" and len(tok) < 4:
                    continue
                self.auto.add_word(key + "\x00" + enc, None) if False else None
                lst = self.auto.get(key, None)
                if lst is None:
                    lst = []
                    self.auto.add_word(key, lst)
                lst.append((tok, ps, enc))
                n += 1
        self.empty = n == 0
        if not self.empty:
            self.auto.make_automaton()

    # ---- boundary checks on decoded text around the match -----------------
    @staticmethod
    def _char_before(data, start, enc):
        if start <= 0:
            return None
        if enc == "utf-16-le":
            if start < 2:
                return None
            try:
                return data[start - 2:start].decode("utf-16-le")
            except UnicodeDecodeError:
                return None
        b = data[start - 1]
        if b < 0x80:
            return chr(b)
        if enc == "latin-1":
            return chr(b)
        # utf-8 continuation: walk back
        i = start - 1
        while i > 0 and start - i < 4 and (data[i] & 0xC0) == 0x80:
            i -= 1
        try:
            return data[i:start].decode("utf-8")
        except UnicodeDecodeError:
            return chr(b)

    @staticmethod
    def _char_after(data, end, enc):
        if end >= len(data):
            return None
        if enc == "utf-16-le":
            try:
                return data[end:end + 2].decode("utf-16-le")
            except UnicodeDecodeError:
                return None
        b = data[end]
        if b < 0x80 or enc == "latin-1":
            return chr(b)
        for k in (2, 3, 4):
            try:
                return data[end:end + k].decode("utf-8")
            except UnicodeDecodeError:
                continue
        return chr(b)

    def _ok(self, data, start, end, orig, tok, enc, binary=False):
        if enc == "utf-16-le" and start % 2 == 1:
            # allow odd alignment only if surrounded by sane chars; most strings are aligned
            pass
        if binary and enc != "utf-16-le" and len(tok) < 8:
            # inside binary data only rewrite tokens that sit in a real text string, so
            # compressed or numeric payloads are never altered by a chance byte match
            if _printable_run(data, start, end) < max(8, end - start + 3) and not _counted_string(data, start, end):
                return False
        f, l = orig[0], orig[-1]
        if tok in self.proper_only and not f.isupper():
            return False
        p = self._char_before(data, start, enc)
        n = self._char_after(data, end, enc)
        if tok in self.exact and orig != tok and orig != tok.upper() and orig != tok.capitalize():
            return False
        long_tok = len(tok) >= 8
        if p is not None and p.isalnum():
            if not (long_tok or (p.isdigit() and f.isalpha()) or ((p.islower() or p.isdigit()) and f.isupper())):
                return False
        if n is not None and n.isalnum():
            if not (long_tok or n.isdigit() or (l.islower() and n.isupper()) or (l.isdigit() and n.isalpha())):
                return False
        return True

    def matched(self, data, binary=False):
        """Counter of tokens that replace() would rewrite in data."""
        import collections
        got = []
        self.replace(data, binary=binary, collect=got)
        return collections.Counter(got)

    def replace(self, data: bytes, filler=False, binary=False, collect=None):
        """Return (new_bytes, count). filler=True writes a low-entropy 'x' run instead of the
        pseudonym (last resort where a pseudonym would not fit a fixed compressed size)."""
        if self.empty or not data:
            return data, 0
        hay = data.decode("latin-1").lower()
        found = []
        for endi, lst in self.auto.iter(hay):
            for tok, ps, enc in lst:
                kl = len(tok.encode(enc))
                start = endi - kl + 1
                found.append((start, -kl, tok, ps, enc))
        if not found:
            return data, 0
        found.sort()
        out = bytearray(data)
        last = -1
        cnt = 0
        for start, nkl, tok, ps, enc in found:
            end = start - nkl
            if start < last:
                continue
            try:
                orig = bytes(data[start:end]).decode(enc)
            except UnicodeDecodeError:
                continue
            if orig.lower() != tok and orig.casefold() != tok.casefold():
                continue
            if not self._ok(data, start, end, orig, tok, enc, binary):
                continue
            if filler:
                ps = "".join("x" if c.isascii() else c for c in ps)
            rep = apply_case(ps, orig).encode(enc)
            if len(rep) != end - start:
                rep = ps.encode(enc)
                if len(rep) != end - start:
                    continue
            out[start:end] = rep
            last = end
            cnt += 1
            if collect is not None:
                collect.append(tok)
        return bytes(out), cnt

    def replace_str(self, s: str):
        b, n = self.replace(s.encode("utf-8"))
        return b.decode("utf-8", "replace") if n else s
