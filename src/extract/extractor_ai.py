# src/extract/extractor_ai.py
from __future__ import annotations

import os
import re
from typing import Any, Dict, Optional

TRIGGERS = [
    "8m",
    "8 m",
    "8-marzo",
    "8marzo",
    "dia internacional de la mujer",
    "international women's day",
    "international women s day",
    "journee internationale des femmes",
    "giornata internazionale della donna",
    "huelga feminista",
    "paro feminista",
    "feminist strike",
    "greve feministe",
    "frauenstreik",
    "sciopero femminista",
    "marcha feminista",
    "feminist march",
    "women's march",
    "womens march",
    "marche feministe",
]

_EVENT_TERMS = [
    "marcha", "manifestacion", "concentracion",
    "asamblea", "jornada", "encuentro", "convocatoria", "movilizacion",
    "acto", "accion directa",
    "march", "rally", "demonstration", "gathering", "mobilization",
    "marche", "rassemblement",
    "manifestazione", "corteo", "presidio",
    "kundgebung", "streik",
]

_RE_DATE_ISO   = re.compile(r"\b(20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\b")
_RE_DATE_SLASH = re.compile(r"\b(0?[1-9]|[12]\d|3[01])/(0[1-9]|1[0-2])/(20\d{2})\b")
_RE_DATE_DASH  = re.compile(r"\b(0?[1-9]|[12]\d|3[01])-(0[1-9]|1[0-2])-(20\d{2})\b")
_RE_DATE_ES    = re.compile(
    r"\b(0?[1-9]|[12]\d|3[01])\s*(?:de\s*)?"
    r"(enero|febrero|marzo|abril|mayo|junio|julio|agosto|"
    r"septiembre|setiembre|octubre|noviembre|diciembre)"
    r"(?:\s+(?:de\s+)?(20\d{2}))?",
    re.IGNORECASE,
)
_RE_TIME = re.compile(r"\b([01]?\d|2[0-3])[:h]([0-5]\d)\b")

_MONTH_ES = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
    "septiembre": "09", "setiembre": "09", "octubre": "10",
    "noviembre": "11", "diciembre": "12",
}


def _normalize_text(s):
    s = s or ""
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _extract_date(text):
    m = _RE_DATE_ISO.search(text)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = _RE_DATE_SLASH.search(text)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{int(m.group(1)):02d}"
    m = _RE_DATE_DASH.search(text)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{int(m.group(1)):02d}"
    m = _RE_DATE_ES.search(text)
    if m:
        day   = int(m.group(1))
        month = _MONTH_ES.get(m.group(2).lower(), "03")
        year  = m.group(3) or "2025"
        return f"{year}-{month}-{day:02d}"
    return ""


def _extract_time(text):
    m = _RE_TIME.search(text)
    if m:
        return f"{int(m.group(1)):02d}:{m.group(2)}"
    return ""


def _basic_score(text):
    t = (text or "").lower()
    score = 0

    trigger_hits = sum(1 for trig in TRIGGERS if trig in t)
    score += min(trigger_hits * 3, 9)

    if (_RE_DATE_ISO.search(text) or _RE_DATE_SLASH.search(text) or
            _RE_DATE_DASH.search(text) or _RE_DATE_ES.search(text)):
        score += 2
    if _RE_TIME.search(text):
        score += 2

    term_hits = sum(1 for term in _EVENT_TERMS if term in t)
    score += min(term_hits, 4)

    return min(score, 20)


def extract_event_fields(parsed):
    if not isinstance(parsed, dict):
        return None

    url   = parsed.get("url") or ""
    title = _normalize_text(str(parsed.get("title") or ""))
    text  = _normalize_text(str(parsed.get("text") or ""))
    blob  = f"{title}\n{text}".strip()

    if not blob:
        return None

    score = _basic_score(blob)

    min_score = int(os.environ.get("EXTRACTOR_MIN_SCORE", "1"))
    if score < min_score:
        return None

    fecha  = _extract_date(blob)
    hora   = _extract_time(blob)

    imagen = (parsed.get("og_image") or "").strip()
    if not imagen:
        imgs   = parsed.get("images") or []
        imagen = imgs[0] if imgs else ""

    ev = {
        "colectiva":            parsed.get("site_name") or "",
        "convocatoria":         title or url,
        "descripcion":          text[:1200] if text else "",
        "fecha":                fecha,
        "hora":                 hora,
        "pais":                 parsed.get("country") or "",
        "ciudad":               parsed.get("city")    or "",
        "localizacion_exacta":  parsed.get("place_name") or "",
        "direccion":            parsed.get("address")    or "",
        "lat":                  parsed.get("lat") or "",
        "lon":                  parsed.get("lon") or "",
        "imagen":               imagen,
        "cta_url":              parsed.get("cta_url") or url,
        "fuente_url":           url,
        "sitio_web_colectiva":  parsed.get("site_url") or "",
        "trans_incluyente":     parsed.get("trans_incluyente") or "",
        "confianza_extraccion": parsed.get("confianza_extraccion") or "media",
        "precision_ubicacion":  parsed.get("precision_ubicacion") or "",
        "score_relevancia":     score,
    }
    return ev
