# src/parse/html_parser.py
from __future__ import annotations

import re
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin


_SKIP_IMG_HINTS = (
    "logo",
    "icon",
    "sprite",
    "avatar",
    "favicon",
    "brand",
    "badge",
    "spinner",
    "loading",
)


def _norm_space(s: str) -> str:
    s = (s or "").replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _abs_url(base_url: str, maybe_url: str) -> str:
    u = (maybe_url or "").strip()
    if not u:
        return ""
    if u.startswith("//"):
        # protocol-relative
        return "https:" + u
    if u.startswith("http://") or u.startswith("https://"):
        return u
    return urljoin(base_url, u)


def _looks_like_image_url(u: str) -> bool:
    ul = (u or "").lower()
    if not ul.startswith("http"):
        return False
    # acepta sin extensión también (muchos CMS sirven imágenes sin .jpg)
    if any(ext in ul for ext in [".jpg", ".jpeg", ".png", ".webp", ".gif"]):
        return True
    # algunos usan parámetros, ejemplo: .../image?format=jpg
    if any(k in ul for k in ["format=jpg", "format=png", "format=webp", "image", "img", "media"]):
        return True
    return True


def _score_img(u: str) -> int:
    """
    Heurística simple para escoger una imagen “buena”.
    """
    ul = (u or "").lower()
    score = 0

    # penaliza cosas típicas de logos/icons
    if any(h in ul for h in _SKIP_IMG_HINTS):
        score -= 10

    # bonifica cosas típicas de “hero”
    if any(h in ul for h in ("hero", "header", "featured", "cover", "banner", "og", "social")):
        score += 5

    # bonifica extensiones “normales”
    if any(ext in ul for ext in (".jpg", ".jpeg", ".png", ".webp")):
        score += 2

    # bonifica si parece grande (muy común: 1200x630 etc.)
    if re.search(r"(1200x630|1080|1920|1600|1280|1024|800)", ul):
        score += 2

    return score


class _Parser(HTMLParser):
    def __init__(self, base_url: str):
        super().__init__(convert_charrefs=True)
        self.base_url = base_url

        self.in_title = False
        self.title_parts: list[str] = []

        self.text_parts: list[str] = []
        self.meta: dict[str, str] = {}

        self.images: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]):
        tag = (tag or "").lower()
        a = {k.lower(): (v or "") for k, v in (attrs or [])}

        if tag == "title":
            self.in_title = True
            return

        if tag == "meta":
            # og:image / twitter:image / etc
            prop = (a.get("property") or a.get("name") or "").strip().lower()
            content = (a.get("content") or "").strip()
            if prop and content:
                self.meta[prop] = content
            return

        if tag == "img":
            src = (a.get("src") or a.get("data-src") or a.get("data-lazy-src") or "").strip()
            if src:
                u = _abs_url(self.base_url, src)
                if u and _looks_like_image_url(u):
                    self.images.append(u)
            return

        # nada más: el texto lo capturamos en handle_data

    def handle_endtag(self, tag: str):
        tag = (tag or "").lower()
        if tag == "title":
            self.in_title = False

    def handle_data(self, data: str):
        if not data:
            return
        if self.in_title:
            self.title_parts.append(data)
        else:
            t = _norm_space(data)
            # evita meter basura ultra corta
            if len(t) >= 2:
                self.text_parts.append(t)


def parse_page(url: str, html: str) -> dict[str, Any]:
    """
    Parse HTML en un dict homogéneo para el extractor.
    Incluye:
      - title
      - text (plano)
      - meta (incluye og:image / twitter:image)
      - og_image (string)
      - images (lista)
    """
    url = (url or "").strip()
    html = html or ""
    if not url or not html:
        return {}

    p = _Parser(base_url=url)
    try:
        p.feed(html)
    except Exception:
        # HTML roto: igual devolvemos lo que tengamos
        pass

    title = _norm_space(" ".join(p.title_parts))
    text = _norm_space(" ".join(p.text_parts))

    # og:image / twitter:image como prioridad
    og = (p.meta.get("og:image") or "").strip()
    tw = (p.meta.get("twitter:image") or "").strip()
    og_abs = _abs_url(url, og) if og else ""
    tw_abs = _abs_url(url, tw) if tw else ""

    # dedupe imágenes preservando orden
    seen = set()
    imgs: list[str] = []
    for u in [og_abs, tw_abs] + (p.images or []):
        u = (u or "").strip()
        if not u:
            continue
        if u in seen:
            continue
        seen.add(u)
        imgs.append(u)

    # escoge “mejor” imagen
    best = ""
    if og_abs:
        best = og_abs
    elif tw_abs:
        best = tw_abs
    elif imgs:
        best = sorted(imgs, key=_score_img, reverse=True)[0]

    return {
        "url": url,
        "title": title,
        "text": text,
        "meta": dict(p.meta),
        "og_image": best,
        "images": imgs,
        "html": html,  # por si el extractor usa HTML
    }
