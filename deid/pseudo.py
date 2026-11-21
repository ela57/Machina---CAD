"""Same-length, case-preserving, deterministic pseudonyms.

Every identifying token maps to one pseudonym with the same number of
characters and the same UTF-8 / UTF-16 / Latin-1 byte length per character,
so replacements never move offsets inside binary CAD files.
"""
import hashlib
import hmac
import json
import os
import unicodedata

CONS = "bcdfghjklmnprstvz"
VOWS = "aeiou"
FIRST = ("Al Bo Cy Ed Jo Li Max Ana Ben Eva Ian Kai Leo Mia Noa Sam Tom Zoe Adam Alex Anna Cara Dana Eric "
         "Ella Finn Gwen Hugo Ivan Jack Kate Luke Mara Nina Omar Paul Rosa Sara Theo Vera Wade Yara Aaron "
         "Bella Caleb Diana Elena Felix Grace Henry Irene Jonas Karen Lucas Maria Nolan Olive Peter Quinn "
         "Rhett Sofia Tessa Vince Wendy Andrew Bianca Carmen Daniel Esther Fabian Gloria Harold Isabel "
         "Jasper Kelsey Lauren Martin Nathan Oliver Philip Rachel Samuel Teresa Victor Walter Abigail "
         "Barbara Charles Dominic Eleanor Frances Gabriel Harriet Ignacio Jessica Kenneth Leonard Matthew "
         "Natalie Patrick Rebecca Stephen Timothy Veronica Benjamin Caroline Clarissa Florence Jonathan "
         "Margaret Nicholas Penelope Theodore Valentina Christopher Alexandra").split()
LAST = ("Ng Li Wu Cox Fox Kim Lee Day Ash Orr Bell Cole Diaz Ford Gray Hart Hill Kerr Lowe Moss Nash Ortiz "
        "Pope Reed Shaw Vale Webb York Adams Baker Clark Davis Evans Foster Grant Hayes Irwin Jones Klein "
        "Lopez Moore Nolan Owens Perry Quinn Reyes Stone Tyler Walsh Young Barnes Carter Dawson Elliot "
        "Fisher Graham Harper Jensen Keller Lawson Morgan Norris Parker Ramsey Sutton Turner Warren "
        "Abbott Bennett Coleman Douglas Edwards Fleming Gardner Hawkins Jenkins Kennedy Lambert Mitchell "
        "Newton Osborne Palmer Russell Simmons Thornton Underwood Whitaker Armstrong Blackwell Caldwell "
        "Davenport Ellington Fairbanks Gallagher Hendricks Kingsley Livingston Montgomery Pemberton "
        "Richardson Stevenson Thompson Wellington Worthington Fitzgerald Huntington").split()
CJK = "王李张刘陈杨黄赵吴周徐孙马朱胡郭何高林罗郑梁谢宋唐许韩冯邓曹彭曾萧田董袁潘蒋蔡余杜叶程苏魏吕丁任沈姚卢姜崔钟谭陆汪范金石廖贾夏韦付方白邹孟熊秦邱江尹薛闫段雷侯龙史陶黎贺顾毛郝龚邵万钱严覃武戴莫孔向汤明华伟芳娜敏静丽强磊军洋勇艳杰涛超秀霞平刚桂英"
LAT1_LOWER = [c for c in "àáâãäåæçèéêëìíîïñòóôõöøùúûüýÿ"]


def _rng(key, salt, n):
    return hmac.new(salt, (key + "#" + str(n)).encode("utf-8"), hashlib.sha256).digest()


def _word(length, h, kind):
    pool = {"first": FIRST, "last": LAST}.get(kind)
    if pool:
        cands = [w for w in pool if len(w) == length]
        if cands:
            return cands[int.from_bytes(h[:4], "big") % len(cands)].lower()
    out = []
    start_vowel = h[4] & 1
    for i in range(length):
        b = h[(5 + i) % len(h)] ^ (i * 31 & 0xFF)
        if (i + start_vowel) % 2 == 0:
            out.append(CONS[b % len(CONS)])
        else:
            out.append(VOWS[b % len(VOWS)])
    return "".join(out)


def _char_like(c, b):
    """A replacement character with the same encoded size in every encoding."""
    o = ord(c)
    if c.isdigit() and o < 128:
        return c
    if o < 128:
        return c  # handled by the alphabetic generator
    if 0xC0 <= o <= 0xFF and c.isalpha():
        r = LAT1_LOWER[b % len(LAT1_LOWER)]
        return r.upper() if c.isupper() and r.upper() != r and len(r.upper()) == 1 and ord(r.upper()) <= 0xFF else r
    if 0x4E00 <= o <= 0x9FFF:
        return CJK[b % len(CJK)]
    if 0x0100 <= o <= 0x07FF and c.isalpha():
        # stay inside the same 2-byte block, pick another letter
        base = o & ~0x3F
        for k in range(64):
            cand = chr(base + ((b + k) % 64))
            if cand.isalpha() and cand != c:
                return cand
        return c
    if 0xAC00 <= o <= 0xD7A3:
        return chr(0xAC00 + b % (0xD7A3 - 0xAC00))
    if 0x3040 <= o <= 0x30FF:
        base = 0x3041 if o < 0x30A0 else 0x30A1
        return chr(base + b % 0x56)
    return c


def apply_case(template, original):
    out = []
    for t, o in zip(template, original):
        if o.isupper():
            out.append(t.upper() if len(t.upper()) == 1 else t)
        else:
            out.append(t)
    return "".join(out)


class Registry:
    """Token (casefolded) -> lowercase pseudonym of identical shape."""

    def __init__(self, salt: bytes):
        self.salt = salt
        self.fwd = {}
        self.rev = {}

    def key(self, token):
        return token.lower()

    def get(self, token, kind="org"):
        k = self.key(token)
        if k in self.fwd:
            return self.fwd[k]
        n = 0
        while True:
            h = _rng(k, self.salt, n)
            alpha = [c for c in k if c.isascii() and c.isalpha()]
            w = _word(len(alpha), h, kind if n < 3 else "org")
            wi = iter(w)
            out = []
            for i, c in enumerate(k):
                if c.isascii() and c.isalpha():
                    out.append(next(wi))
                else:
                    out.append(_char_like(c, h[(i * 7 + n) % len(h)]))
            p = "".join(out)
            if p != k and p not in self.rev and p not in self.fwd:
                break
            n += 1
            if n > 50:
                break
        self.fwd[k] = p
        self.rev[p] = k
        return p

    def save(self, path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.fwd, f, ensure_ascii=False, indent=0, sort_keys=True)

    @classmethod
    def load(cls, path, salt):
        r = cls(salt)
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                r.fwd = json.load(f)
            r.rev = {v: k for k, v in r.fwd.items()}
        return r
