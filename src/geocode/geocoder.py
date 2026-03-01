# src/geocode/geocoder.py
from __future__ import annotations

import os
import re
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional, Dict, Any

import requests

DEFAULT_DB_PATH = "data/processed/geocode_cache.sqlite"
NOMINATIM_URL   = "https://nominatim.openstreetmap.org/search"

# Mapeo nombre de país → código ISO2 para restringir búsquedas en Nominatim.
# Evita que "Santiago de Chile" geocodifique un evento boliviano en Chile.
_COUNTRY_TO_ISO2 = {
    "argentina": "ar", "bolivia": "bo", "brasil": "br", "brazil": "br",
    "chile": "cl", "colombia": "co", "costa rica": "cr", "cuba": "cu",
    "ecuador": "ec", "el salvador": "sv", "guatemala": "gt", "haiti": "ht",
    "honduras": "hn", "mexico": "mx", "méxico": "mx", "nicaragua": "ni",
    "panama": "pa", "panamá": "pa", "paraguay": "py", "peru": "pe",
    "perú": "pe", "puerto rico": "pr", "republica dominicana": "do",
    "república dominicana": "do", "uruguay": "uy", "venezuela": "ve",
    "españa": "es", "spain": "es", "france": "fr", "francia": "fr",
    "germany": "de", "alemania": "de", "italy": "it", "italia": "it",
    "portugal": "pt", "united kingdom": "gb", "reino unido": "gb",
    "uk": "gb", "netherlands": "nl", "países bajos": "nl",
    "belgium": "be", "bélgica": "be", "switzerland": "ch", "suiza": "ch",
    "austria": "at", "sweden": "se", "suecia": "se", "norway": "no",
    "noruega": "no", "denmark": "dk", "dinamarca": "dk",
    "finland": "fi", "finlandia": "fi", "poland": "pl", "polonia": "pl",
    "czech republic": "cz", "república checa": "cz", "hungary": "hu",
    "hungría": "hu", "romania": "ro", "rumania": "ro", "bulgaria": "bg",
    "croatia": "hr", "croacia": "hr", "greece": "gr", "grecia": "gr",
    "turkey": "tr", "turquía": "tr",
    "united states": "us", "estados unidos": "us", "usa": "us",
    "canada": "ca", "canadá": "ca",
    "kenya": "ke", "nigeria": "ng", "south africa": "za",
    "sudáfrica": "za", "ethiopia": "et", "ghana": "gh", "senegal": "sn",
    "morocco": "ma", "marruecos": "ma", "egypt": "eg", "egipto": "eg",
    "tanzania": "tz", "uganda": "ug", "ivory coast": "ci",
    "india": "in", "pakistan": "pk", "bangladesh": "bd",
    "indonesia": "id", "philippines": "ph", "filipinas": "ph",
    "vietnam": "vn", "thailand": "th", "tailandia": "th",
    "malaysia": "my", "malasia": "my", "myanmar": "mm",
    "nepal": "np", "sri lanka": "lk",
    "china": "cn", "japan": "jp", "japón": "jp",
    "south korea": "kr", "corea del sur": "kr",
    "australia": "au", "new zealand": "nz", "nueva zelanda": "nz",
}


def _country_to_iso2(pais: str) -> Optional[str]:
    """Convierte nombre de país en español/inglés a código ISO2."""
    if not pais:
        return None
    return _COUNTRY_TO_ISO2.get(pais.strip().lower())


@dataclass
class GeocodeResult:
    lat:          str
    lon:          str
    display_name: str
    confidence:   str
    precision:    str


class Geocoder:

    def __init__(
        self,
        db_path:           str   = DEFAULT_DB_PATH,
        user_agent:        str   = "geochicas-8m-global-mapper/1.0",
        min_delay_seconds: float = 1.1,
        timeout_seconds:   int   = 20,
    ):
        self.db_path           = db_path
        self.user_agent        = user_agent
        self.min_delay_seconds = min_delay_seconds
        self.timeout_seconds   = timeout_seconds
        self._last_call_ts     = 0.0

        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self._init_db()

    def _init_db(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS geocode_cache (
                query        TEXT PRIMARY KEY,
                lat          TEXT,
                lon          TEXT,
                display_name TEXT,
                confidence   TEXT,
                precision    TEXT
            )
        """)
        self.conn.commit()

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def _norm_query(self, q: str) -> str:
        return re.sub(r"\s+", " ", (q or "").strip()).lower()

    def _get_cached(self, q_norm: str) -> Optional[GeocodeResult]:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT lat, lon, display_name, confidence, precision "
            "FROM geocode_cache WHERE query = ?",
            (q_norm,),
        )
        row = cur.fetchone()
        return GeocodeResult(*row) if row else None

    def _set_cache(self, q_norm: str, res: GeocodeResult):
        cur = self.conn.cursor()
        cur.execute("""
            INSERT OR REPLACE INTO geocode_cache
              (query, lat, lon, display_name, confidence, precision)
            VALUES (?,?,?,?,?,?)
        """, (q_norm, res.lat, res.lon, res.display_name, res.confidence, res.precision))
        self.conn.commit()

    def _rate_limit(self):
        now     = time.time()
        elapsed = now - self._last_call_ts
        if elapsed < self.min_delay_seconds:
            time.sleep(self.min_delay_seconds - elapsed)
        self._last_call_ts = time.time()

    def geocode(
        self,
        query:        str,
        countrycodes: Optional[str] = None,
    ) -> Optional[GeocodeResult]:
        """
        Geocodifica una query con Nominatim.

        countrycodes: código ISO2 del país para restringir la búsqueda (ej: "ar", "es").
        Si se pasa y no hay resultados, reintenta sin la restricción.
        La clave de caché incluye el countrycode para no mezclar resultados.
        """
        cache_key = self._norm_query(
            f"{query}|cc={countrycodes}" if countrycodes else query
        )

        cached = self._get_cached(cache_key)
        if cached:
            return cached

        self._rate_limit()

        params: Dict[str, Any] = {
            "q":      query,
            "format": "jsonv2",
            "limit":  1,
        }
        if countrycodes:
            params["countrycodes"] = countrycodes

        try:
            r = requests.get(
                NOMINATIM_URL,
                params=params,
                timeout=self.timeout_seconds,
                headers={"User-Agent": self.user_agent},
            )
            r.raise_for_status()
            data = r.json()
        except Exception:
            return None

        # Si con countrycode no hay resultados, reintentar sin él
        if not data and countrycodes:
            self._rate_limit()
            try:
                params_fb = {k: v for k, v in params.items() if k != "countrycodes"}
                r2 = requests.get(
                    NOMINATIM_URL,
                    params=params_fb,
                    timeout=self.timeout_seconds,
                    headers={"User-Agent": self.user_agent},
                )
                r2.raise_for_status()
                data = r2.json()
            except Exception:
                return None

        if not data:
            return None

        hit = data[0]
        res = GeocodeResult(
            lat=str(hit.get("lat", "")),
            lon=str(hit.get("lon", "")),
            display_name=str(hit.get("display_name", "")),
            confidence="media",
            precision="exacta",
        )

        self._set_cache(cache_key, res)
        return res


# =========================
# Compat layer para main.py
# =========================

_GEOCODER: Optional[Geocoder] = None


def _get_geocoder() -> Geocoder:
    global _GEOCODER
    if _GEOCODER is None:
        _GEOCODER = Geocoder()
    return _GEOCODER


def load_geocode_cache(path: str) -> Dict[str, Any]:
    """Stub de compatibilidad — la caché real vive en SQLite dentro de Geocoder."""
    return {}


def save_geocode_cache(path: str, cache: Dict[str, Any]) -> None:
    """Stub de compatibilidad."""
    return None


def geocode_event(
    ev: Dict[str, Any],
    geocode_cache=None,
) -> Optional[Dict[str, str]]:
    """
    Geocodifica un evento a partir de sus campos ciudad/pais.

    Cambios respecto a la versión anterior:
    - Valida que lat/lon sean números reales antes de devolverlos sin geocodificar.
    - Pasa countrycodes a Nominatim cuando el país está disponible.
    - Si la búsqueda restringida no da resultados, reintenta sin restricción.
    """
    # Si ya tiene coordenadas válidas, no llamar a Nominatim
    try:
        lat = float(ev.get("lat") or "")
        lon = float(ev.get("lon") or "")
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            return {
                "lat": str(lat),
                "lon": str(lon),
                "display_name": "",
                "confidence": "alta",
                "precision": ev.get("precision_ubicacion", ""),
            }
    except (TypeError, ValueError):
        pass

    ciudad = (ev.get("ciudad") or "").strip()
    pais   = (ev.get("pais")   or "").strip()

    if not ciudad and not pais:
        return None

    query        = ", ".join(x for x in [ciudad, pais] if x)
    countrycodes = _country_to_iso2(pais) if pais else None

    g   = _get_geocoder()
    res = g.geocode(query, countrycodes=countrycodes)
    if not res:
        return None

    return {
        "lat":          res.lat,
        "lon":          res.lon,
        "display_name": res.display_name,
        "confidence":   res.confidence,
        "precision":    res.precision,
    }
