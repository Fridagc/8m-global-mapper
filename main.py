# main.py — 8m-global-mapper

from __future__ import annotations

import csv
import os
import re
import time
from collections import deque
from datetime import date
from urllib.parse import urlparse, urljoin

import yaml

from src.collect.web_fetch import make_session, fetch_url
from src.collect.discover_links import extract_links, same_domain
from src.parse.html_parser import parse_page
from src.extract.extractor_ai import extract_event_fields
from src.geocode.geocoder import geocode_event, load_geocode_cache, save_geocode_cache
from src.media.image_processor import download_and_process_image
from src.export.to_csv import export_master_csv, export_umap_csv, export_sin_coord_csv
from src.collect.sources_loader import load_sources, should_include_social_seeds


# =========================
# Paths
# =========================
BASE_SOURCES_YML      = "config/sources.yml"
GENERATED_SOURCES_YML = "config/sources.generated.yml"
FEMINIST_SOURCES_YML  = "config/sources.feminist.yml"

KEYWORDS_YML     = "config/keywords.yml"
CITIES_TXT       = "config/cities.txt"
DOMAIN_RULES_YML = "config/domain_rules.yml"

EXPORT_MASTER    = "data/exports/mapa_8m_global_master.csv"
EXPORT_UMAP      = "data/exports/mapa_8m_global_umap.csv"
EXPORT_SIN_COORD = "data/exports/mapa_8m_global_sin_coord.csv"

IMAGES_DIR         = "data/images"
GEOCODE_CACHE_PATH = "data/processed/geocode_cache.json"


# =========================
# Tunables
# =========================
FAST_MODE = os.environ.get("FAST_MODE", "true").lower() in ("1", "true", "yes", "y", "on")

MAX_SEEDS            = int(os.environ.get("MAX_SEEDS",            "220"))
MAX_PRIORITY         = int(os.environ.get("MAX_PRIORITY",         "750"))
MAX_TOTAL_CANDIDATES = int(os.environ.get("MAX_TOTAL_CANDIDATES", "3000"))

CRAWL_DEPTH        = int(os.environ.get("CRAWL_DEPTH",       "2"))
MAX_PAGES_PER_SEED = int(os.environ.get("MAX_PAGES_PER_SEED", "30" if FAST_MODE else "60"))

REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "20"))

THRESHOLD_EXTRACT     = int(os.environ.get("THRESHOLD_EXTRACT",     "6"))
THRESHOLD_EXPORT_UMAP = int(os.environ.get("THRESHOLD_EXPORT_UMAP", "10"))

MIN_EVENT_DATE = date.fromisoformat(os.environ.get("MIN_EVENT_DATE", "2025-01-01"))


# =========================
# Mapeo región → país por defecto
# Cuando la seed no tiene país explícito, usamos la región para inferirlo.
# Se usa solo como fallback para geocodificación — no se muestra al usuario.
# =========================
_REGION_DEFAULT_COUNTRY = {
    "Europa":          "España",
    "America Sur":     "",
    "America Centro":  "",
    "America Norte":   "",
    "Africa":          "",
    "Asia":            "",
    "Oceania":         "",
}

# Mapeo dominio TLD → país ISO2 para inferir país desde la URL
_TLD_TO_COUNTRY = {
    ".ar": "Argentina",  ".bo": "Bolivia",   ".br": "Brasil",
    ".cl": "Chile",      ".co": "Colombia",  ".cr": "Costa Rica",
    ".cu": "Cuba",       ".do": "República Dominicana",
    ".ec": "Ecuador",    ".sv": "El Salvador", ".gt": "Guatemala",
    ".hn": "Honduras",   ".mx": "México",    ".ni": "Nicaragua",
    ".pa": "Panamá",     ".py": "Paraguay",  ".pe": "Perú",
    ".uy": "Uruguay",    ".ve": "Venezuela",
    ".es": "España",     ".fr": "Francia",   ".de": "Alemania",
    ".it": "Italia",     ".pt": "Portugal",  ".be": "Bélgica",
    ".nl": "Países Bajos", ".ch": "Suiza",   ".at": "Austria",
    ".uk": "Reino Unido", ".gb": "Reino Unido",
    ".se": "Suecia",     ".no": "Noruega",   ".dk": "Dinamarca",
    ".fi": "Finlandia",  ".pl": "Polonia",   ".cz": "República Checa",
    ".hu": "Hungría",    ".ro": "Rumania",   ".bg": "Bulgaria",
    ".hr": "Croacia",    ".gr": "Grecia",    ".tr": "Turquía",
    ".us": "Estados Unidos", ".ca": "Canadá",
    ".ke": "Kenya",      ".ng": "Nigeria",   ".za": "Sudáfrica",
    ".et": "Etiopía",    ".gh": "Ghana",     ".sn": "Senegal",
    ".ma": "Marruecos",  ".eg": "Egipto",    ".tz": "Tanzania",
    ".ug": "Uganda",
    ".in": "India",      ".pk": "Pakistán",  ".bd": "Bangladesh",
    ".ph": "Filipinas",  ".vn": "Vietnam",   ".th": "Tailandia",
    ".au": "Australia",  ".nz": "Nueva Zelanda",
}


def _infer_country_from_url(url: str) -> str:
    """
    Intenta inferir el país desde el TLD de la URL.
    .es → España, .ar → Argentina, etc.
    Devuelve "" si no puede inferir.
    """
    try:
        host = urlparse(url).netloc.lower()
        for tld, country in _TLD_TO_COUNTRY.items():
            if host.endswith(tld) or ("." + host).endswith(tld):
                return country
    except Exception:
        pass
    return ""


# =========================
# Utils
# =========================
def ensure_dirs():
    os.makedirs("data/exports",   exist_ok=True)
    os.makedirs("data/images",    exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)


def strip_fragment(u: str) -> str:
    return (u or "").split("#")[0].strip()


def dedupe(items: list) -> list:
    seen = set()
    out  = []
    for x in items:
        x = (x or "").strip()
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def normalize(s: str) -> str:
    if not s:
        return ""
    s = str(s).replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


# =========================
# Cities
# =========================
def load_cities(path: str) -> list:
    if not os.path.exists(path):
        return []
    cities = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                cities.append(line)
    return sorted(cities, key=lambda x: -len(x))


def detect_city(text: str, cities: list) -> str:
    if not text or not cities:
        return ""
    t = text.lower()
    for city in cities:
        if city.lower() in t:
            return city
    return ""


# =========================
# Domain rules
# =========================
def load_domain_rules() -> dict:
    if not os.path.exists(DOMAIN_RULES_YML):
        return {}
    with open(DOMAIN_RULES_YML, "r", encoding="utf-8") as f:
        y = yaml.safe_load(f)
        return y if isinstance(y, dict) else {}


def url_allowed_by_rules(rules: dict, url: str) -> bool:
    if not rules:
        return True

    u = (url or "").lower()

    # Reglas globales
    global_rules = rules.get("global") or {}
    if isinstance(global_rules, dict):
        for pat in (global_rules.get("deny_url_contains") or []):
            if isinstance(pat, str) and pat.lower() in u:
                return False

    # Reglas por dominio
    parsed = urlparse(url)
    host   = (parsed.netloc or "").lower().lstrip("www.")

    domain_rules = rules.get("domains") or {}
    if not isinstance(domain_rules, dict):
        return True

    matched = None
    for d in domain_rules:
        d_clean = d.lower().lstrip("www.")
        if host == d_clean or host.endswith("." + d_clean):
            if matched is None or len(d_clean) > len(matched):
                matched = d_clean

    if matched is None:
        return True

    drules = domain_rules.get(matched) or {}
    if not isinstance(drules, dict):
        return True

    if drules.get("hard_deny"):
        return False

    allow = drules.get("allow_url_contains") or []
    if allow:
        for pat in allow:
            if isinstance(pat, str) and pat.lower() in u:
                return True
        return False

    for pat in (drules.get("deny_url_contains") or []):
        if isinstance(pat, str) and pat.lower() in u:
            return False

    return True


# =========================
# Sources merge
# =========================
def read_sources_merged():
    seeds_all    = []
    priority_all = []
    hashtags_all = []
    seed_meta    = {}

    paths = [BASE_SOURCES_YML, GENERATED_SOURCES_YML, FEMINIST_SOURCES_YML]

    for p in paths:
        if not os.path.exists(p):
            continue

        bundle = load_sources(p)
        seeds_all.extend(bundle.seeds_urls)
        priority_all.extend(bundle.priority_urls)
        hashtags_all.extend(bundle.hashtags)

        if should_include_social_seeds():
            seeds_all.extend(bundle.social_urls)

        y = yaml.safe_load(open(p, "r", encoding="utf-8"))
        if isinstance(y, dict) and isinstance(y.get("seeds"), dict):
            for region, topics in y["seeds"].items():
                if not isinstance(topics, dict):
                    continue
                for tema, node in topics.items():
                    if not isinstance(node, dict):
                        continue
                    for u in (node.get("urls") or []):
                        if isinstance(u, str) and u.strip().startswith("http"):
                            seed_meta[u.strip()] = {"region": region, "temas": [tema]}

    return dedupe(seeds_all), dedupe(priority_all), dedupe(hashtags_all), seed_meta


# =========================
# Crawl BFS
# =========================
def crawl_seed_bfs(session, seed, rules, depth, max_pages, global_seen, global_out, global_cap):
    q          = deque([(seed, depth)])
    local_seen = set()
    added      = 0
    while q and added < max_pages and len(global_out) < global_cap:
        u, dleft = q.popleft()
        u = strip_fragment(u)
        if not u or u in local_seen:
            continue
        local_seen.add(u)
        if not same_domain(seed, u):
            continue
        if not url_allowed_by_rules(rules, u):
            continue
        if u not in global_seen:
            global_seen.add(u)
            global_out.append(u)
            added += 1
        if dleft <= 1:
            continue
        html = fetch_url(session, u, use_cache=True)
        if not html:
            continue
        for link in extract_links(u, html):
            link = strip_fragment(link)
            if link and same_domain(seed, link):
                q.append((link, dleft - 1))
    return added


# =========================
# Popup builder
# =========================
def build_umap_popup(ev: dict) -> str:
    titulo = normalize(ev.get("colectiva") or ev.get("convocatoria") or "")
    fecha  = normalize(ev.get("fecha") or "")
    hora   = normalize(ev.get("hora")  or "")

    when = ""
    if fecha and hora:
        when = f"{fecha} - {hora}"
    elif fecha:
        when = fecha
    elif hora:
        when = hora

    lines = []
    if titulo:
        lines.append(f"## {titulo}")
    if when:
        lines.append(when)

    img = normalize(ev.get("imagen") or "")
    if img:
        if img.startswith("images/"):
            img = f"https://geochicas.github.io/8m-global-mapper/{img}"
        lines.append(f"{{{{{img}}}}}")

    cta = normalize(ev.get("cta_url") or "")
    if cta.startswith("http"):
        lines.append(f"[[{cta}|Accede a la convocatoria]]")

    return "\n".join(lines).strip()


# =========================
# Inferir a qué seed pertenece una URL candidata
# =========================
def _find_seed_for_url(url: str, seed_meta: dict) -> str:
    """
    Devuelve la seed URL que corresponde al mismo dominio que url.
    Se usa para heredar región y país de la seed.
    """
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return ""
    for seed_url in seed_meta:
        try:
            seed_host = urlparse(seed_url).netloc.lower()
            if host == seed_host or host.endswith("." + seed_host) or seed_host.endswith("." + host):
                return seed_url
        except Exception:
            continue
    return ""


# =========================
# MAIN
# =========================
def main():
    ensure_dirs()
    session = make_session(timeout=REQUEST_TIMEOUT)
    rules   = load_domain_rules()
    cities  = load_cities(CITIES_TXT)

    seeds, priority, hashtags, seed_meta = read_sources_merged()

    print(f"🌐 Seeds: {min(len(seeds), MAX_SEEDS)}")
    print(f"🎯 Priority URLs: {min(len(priority), MAX_PRIORITY)}")
    print(f"🧭 Crawl: depth={CRAWL_DEPTH} max_pages_per_seed={MAX_PAGES_PER_SEED}")
    print(f"🏙️ Ciudades cargadas: {len(cities)}")

    candidates = []
    seen       = set()

    # Priority URLs primero
    for u in priority[:MAX_PRIORITY]:
        u = strip_fragment(u)
        if u and u not in seen and url_allowed_by_rules(rules, u):
            seen.add(u)
            candidates.append(u)

    # Crawl BFS por seed
    for seed in seeds[:MAX_SEEDS]:
        if len(candidates) >= MAX_TOTAL_CANDIDATES:
            break
        picked = crawl_seed_bfs(
            session, seed, rules, CRAWL_DEPTH,
            MAX_PAGES_PER_SEED,
            seen, candidates, MAX_TOTAL_CANDIDATES
        )
        if picked:
            print(f"🔗 {seed} -> candidatos: {picked}")

    print(f"🔎 Candidates total: {len(candidates)}")

    records       = []
    geocode_cache = load_geocode_cache(GEOCODE_CACHE_PATH)

    n_imgs      = 0
    n_geocoded  = 0
    n_low_score = 0
    n_old_skip  = 0

    for url in candidates:
        html = fetch_url(session, url, use_cache=True)
        if not html:
            continue

        parsed = parse_page(url, html)
        ev     = extract_event_fields(parsed)
        if not ev:
            continue

        score = int(ev.get("score_relevancia") or 0)
        if score < THRESHOLD_EXTRACT:
            n_low_score += 1
            continue

        # fecha mínima + anio
        f = (ev.get("fecha") or "").strip()
        if f:
            try:
                dd = date.fromisoformat(f)
                if dd < MIN_EVENT_DATE:
                    n_old_skip += 1
                    continue
                ev["anio"] = str(dd.year)
            except Exception:
                pass

        # ── Enriquecer con metadata de la seed ──────────────────────────────
        seed_url = _find_seed_for_url(url, seed_meta)
        meta     = seed_meta.get(seed_url, {})

        # Región
        if not ev.get("region") and meta.get("region"):
            ev["region"] = meta["region"]

        # Temas
        if not ev.get("temas") and meta.get("temas"):
            ev["temas"] = ", ".join(meta["temas"])

        # País — cascada: extractor → TLD de URL → región default
        if not ev.get("pais"):
            pais_tld = _infer_country_from_url(url)
            if pais_tld:
                ev["pais"] = pais_tld
            elif meta.get("region"):
                ev["pais"] = _REGION_DEFAULT_COUNTRY.get(meta["region"], "")

        # ── Ciudad fallback — SOLO en título + 300 chars de descripción ─────
        # NO buscar en texto completo: evita que "Verónica Sanz (moderadora)"
        # se convierta en ciudad del evento
        if not ev.get("ciudad"):
            short_blob = normalize(
                str(ev.get("convocatoria") or "") + " " +
                str(ev.get("descripcion")  or "")[:300]
            )
            found = detect_city(short_blob, cities)
            if found:
                ev["ciudad"] = found

        # ── Geocode ──────────────────────────────────────────────────────────
        geo = geocode_event(ev, geocode_cache=geocode_cache)
        if geo and geo.get("lat") and geo.get("lon"):
            ev["lat"] = geo["lat"]
            ev["lon"] = geo["lon"]
            n_geocoded += 1

        # ── Imagen fallback desde og:image en HTML crudo ─────────────────────
        img_url = (ev.get("imagen") or "").strip()
        if not img_url:
            m = re.search(
                r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
                html, re.I
            )
            if not m:
                m = re.search(
                    r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']',
                    html, re.I
                )
            if m:
                img_url = m.group(1).strip()

        if img_url:
            img_abs = urljoin(url, img_url)
            if img_abs.startswith("//"):
                img_abs = "https:" + img_abs
            if img_abs.startswith("http"):
                out = download_and_process_image(img_abs, out_dir=IMAGES_DIR)
                if out and out.get("public_url"):
                    ev["imagen"] = out["public_url"]
                    n_imgs += 1

        records.append(ev)

    save_geocode_cache(GEOCODE_CACHE_PATH, geocode_cache)

    for r in records:
        r["popup"] = build_umap_popup(r)

    export_master_csv(EXPORT_MASTER, records)
    export_umap_csv(EXPORT_UMAP, records, min_score=THRESHOLD_EXPORT_UMAP)
    export_sin_coord_csv(EXPORT_SIN_COORD, records, min_score=THRESHOLD_EXPORT_UMAP)

    print("")
    print(f"🧾 Eventos master:       {len(records)}")
    print(f"🧠 Skipped low score:    {n_low_score}")
    print(f"🗑️  Filtrados por fecha:  {n_old_skip}")
    print(f"📍 Geocoded:             {n_geocoded}")
    print(f"🖼️  Imagenes descargadas: {n_imgs}")


if __name__ == "__main__":
    main()
