# src/media/image_processor.py
from __future__ import annotations

import hashlib
import os
from urllib.parse import urlparse

import requests


def _safe_join_url(*parts: str) -> str:
    """
    Join estilo URL, evitando '//' dobles.
    """
    cleaned = []
    for p in parts:
        p = (p or "").strip()
        if not p:
            continue
        cleaned.append(p.strip("/"))
    return "/".join(cleaned)


def _ext_from_url(url: str) -> str:
    try:
        path = urlparse(url).path or ""
        _, ext = os.path.splitext(path)
        ext = (ext or "").lower()
        if ext in (".jpg", ".jpeg", ".png", ".webp"):
            return ext
    except Exception:
        pass
    return ".jpg"


def _download_bytes(url: str, timeout: int) -> bytes | None:
    """
    Descarga bytes de imagen con requests (sin depender de web_fetch.fetch_url),
    para evitar incompatibilidades de firma.
    """
    headers = {
        "User-Agent": os.environ.get(
            "USER_AGENT",
            "geochicas-8m-global-mapper/1.0 (+https://github.com/geochicas/8m-global-mapper)",
        )
    }
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        r.raise_for_status()
        return r.content
    except Exception:
        return None


def download_and_process_image(source_url: str, out_dir: str = "data/images") -> dict:
    """
    Descarga imagen y devuelve:
      - local_path: path local en repo
      - public_url: path relativo para GitHub Pages, ej: 'images/xxx.jpg'
      - source_url

    Nota:
    - Aquí NO hacemos “procesamiento” pesado (resize/antologo). Es un downloader
      robusto y compatible para Actions/local.
    """
    source_url = (source_url or "").strip()
    if not source_url.startswith("http"):
        return {"public_url": "", "local_path": "", "source_url": source_url}

    os.makedirs(out_dir, exist_ok=True)

    ext = _ext_from_url(source_url)
    h = hashlib.sha1(source_url.encode("utf-8")).hexdigest()
    fname = f"{h}{ext}"
    local_path = os.path.join(out_dir, fname)

    if not os.path.exists(local_path):
        timeout = int(os.environ.get("REQUEST_TIMEOUT", "20"))
        content = _download_bytes(source_url, timeout=timeout)
        if content:
            with open(local_path, "wb") as f:
                f.write(content)

    # public_url para Pages: site/images/... se publica como /images/...
    public_url = _safe_join_url("images", fname)

    return {
        "public_url": public_url if os.path.exists(local_path) else "",
        "local_path": local_path if os.path.exists(local_path) else "",
        "source_url": source_url,
    }
