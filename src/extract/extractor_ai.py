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
    "día internacional de la mujer",
    "dia internacional de la mujer",
    "huelga feminista",
    "paro feminista",
    "marcha",
    "manifestación",
    "manifestacion",
    "concentración",
    "concentracion",
    "asamblea",
    "jornada",
    "encuentro",
    "agenda",
    "programa",
    "programación",
    "programacion",
    "evento",
    "actividades",
    "inscripción",
    "inscripcion",
    "registro",
    "entradas",
    "ticket",
    "tickets",
]

_RE_DATE_ISO = re.compile(r"\b(20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\b")
_RE_DATE_ES = re.compile(
    r"\b(0?[1-9]|[12]\d|3[01])\s*(de)?\s*(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre)\b",
    re.IGNORECASE,
)
_RE_TIME = re.compile(r"\b([01]?\d|2[0-3])[:h]([0-5]\d)\b")
_RE_EVENT_WORDS = re.compile(
    r"\b(agenda|programa|programación|programacion|evento|actividades|inscripción|inscripcion|registro|entradas|conferencia|taller|conversatorio|charla|marcha|manifestación|manifestacion|concentración|concentracion)\b",
    re.IGNORECASE,
)


def _normalize_text(s: str) -> str:
    s = s or ""
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _basic_score(text: str) -> int:
    """
    Score híbrido:
    - TRIGGERS (8M / feminismo)
    - señales de “página de evento”: fecha, hora, palabras de agenda
    La idea es NO depender de que diga “8M” explícito.
    """
    t = (text or "").lower()
    score = 0

    # señales temáticas
    for trig in TRIGGERS:
        if trig in t:
            score += 1

    # señales de evento (bonos fuertes)
    if _RE_DATE_ISO.search(text) or _RE_DATE_ES.search(text):
        score += 3
    if _RE_TIME.search(text):
        score += 2
    if _RE_EVENT_WORDS.search(text):
        score += 2

    # cap para evitar explosión
    return min(score, 20)


def extract_event_fields(parsed: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(parsed, dict):
        return None

    url = parsed.get("url") or ""
    title = _normalize_text(str(parsed.get("title") or ""))
    text = _normalize_text(str(parsed.get("text") or ""))
    blob = f"{title}\n{text}".strip()

    if not blob:
        return None

    score = _basic_score(blob)

    # Deja pasar cosas con señales de evento, aunque no sean 8M explícito
    min_score = int(os.environ.get("EXTRACTOR_MIN_SCORE", "1"))
    if score < min_score:
        return None

    ev: Dict[str, Any] = {
        "colectiva": parsed.get("site_name") or "",
        "convocatoria": title or url,
        "descripcion": text[:1200] if text else "",
        "fecha": parsed.get("date") or "",
        "hora": parsed.get("time") or "",
        "pais": parsed.get("country") or "",
        "ciudad": parsed.get("city") or "",
        "localizacion_exacta": parsed.get("place_name") or "",
        "direccion": parsed.get("address") or "",
        "lat": parsed.get("lat") or "",
        "lon": parsed.get("lon") or "",
        "imagen": parsed.get("image") or "",
        "cta_url": parsed.get("cta_url") or url,
        "fuente_url": url,
        "sitio_web_colectiva": parsed.get("site_url") or "",
        "trans_incluyente": parsed.get("trans_incluyente") or "",
        "confianza_extraccion": parsed.get("confianza_extraccion") or "media",
        "precision_ubicacion": parsed.get("precision_ubicacion") or "",
        "score_relevancia": score,
    }
    return ev
