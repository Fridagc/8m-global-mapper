# main.py — 8m-global-mapper (estable y coherente)

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
BASE_SOURCES_YML = "config/sources.yml"
GENERATED_SOURCES_YML = "config/sources.generated.yml"
FEMINIST_SOURCES_YML = "config/sources.feminist.yml"

KEYWORDS_YML = "config/keywords.yml"
CITIES_TXT = "config/cities.txt"
DOMAIN_RULES_YML = "config/domain_rules.yml"

MASTER_CSV_PATH = "data/exports/mapa_8m_global_master.csv"

EXPORT_MASTER = "data/exports/mapa_8m_global_master.csv"
EXPORT_UMAP = "data/exports/mapa_8m_global_umap.csv"
EXPORT_SIN_COORD = "data/exports/mapa_8m_global_sin_coord.csv"

IMAGES_DIR = "data/images"
GEOCODE_CACHE_PATH = "data/processed/geocode_cache.json"


# =========================
# Tunables
# =========================
FAST_MODE = os.environ.get("FAST_MODE", "true").lower() in ("1","true","yes","y","on")

MAX_SEEDS = int(os.environ.get("MAX_SEEDS", "220"))
MAX_PRIORITY = int(os.environ.get("MAX_PRIORITY", "750"))
MAX_TOTAL_CANDIDATES = int(os.environ.get("MAX_TOTAL_CANDIDATES", "3000"))

CRAWL_DEPTH = int(os.environ.get("CRAWL_DEPTH", "2"))
MAX_PAGES_PER_SEED = int(os.environ.get("MAX_PAGES_PER_SEED", "30" if FAST_MODE else "60"))

REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "20"))

THRESHOLD_EXTRACT = int(os.environ.get("THRESHOLD_EXTRACT", "6"))
THRESHOLD_EXPORT_UMAP = int(os.environ.get("THRESHOLD_EXPORT_UMAP", "7"))

MIN_EVENT_DATE = date.fromisoformat(os.environ.get("MIN_EVENT_DATE", "2025-01-01"))


# =========================
# Utils
# =========================
def ensure_dirs():
    os.makedirs("data/exports", exist_ok=True)
    os.makedirs("data/images", exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)


def strip_fragment(u: str) -> str:
    return (u or "").split("#")[0].strip()


def dedupe(items: list[str]) -> list[str]:
    seen=set()
    out=[]
    for x in items:
        x=(x or "").strip()
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def normalize(s: str) -> str:
    if not s:
        return ""
    s=str(s).replace("\u00a0"," ")
    s=re.sub(r"\s+"," ",s).strip()
    return s


# =========================
# Domain rules FIXED
# =========================
def load_domain_rules() -> dict:
    if not os.path.exists(DOMAIN_RULES_YML):
        return {}
    with open(DOMAIN_RULES_YML,"r",encoding="utf-8") as f:
        y=yaml.safe_load(f)
        return y if isinstance(y,dict) else {}


def url_allowed_by_rules(rules: dict, url: str) -> bool:
    if not rules:
        return True

    u=(url or "").lower()

    global_rules = rules.get("global", {}) if isinstance(rules.get("global"),dict) else {}

    deny = global_rules.get("deny_url_contains", [])
    for pat in deny:
        if isinstance(pat,str) and pat.lower() in u:
            return False

    allow = global_rules.get("allow_url_contains", [])
    if allow:
        for pat in allow:
            if isinstance(pat,str) and pat.lower() in u:
                return True
        return False

    return True


# =========================
# Sources merge
# =========================
def read_sources_merged():
    seeds_all=[]
    priority_all=[]
    hashtags_all=[]
    seed_meta={}

    paths=[BASE_SOURCES_YML, GENERATED_SOURCES_YML, FEMINIST_SOURCES_YML]

    for p in paths:
        if not os.path.exists(p):
            continue

        bundle=load_sources(p)
        seeds_all.extend(bundle.seeds_urls)
        priority_all.extend(bundle.priority_urls)
        hashtags_all.extend(bundle.hashtags)

        if should_include_social_seeds():
            seeds_all.extend(bundle.social_urls)

        y=yaml.safe_load(open(p,"r",encoding="utf-8"))
        if isinstance(y,dict) and isinstance(y.get("seeds"),dict):
            for region,topics in y["seeds"].items():
                for tema,node in topics.items():
                    for u in node.get("urls",[]):
                        seed_meta[u]={
                            "region": region,
                            "temas": [tema],
                            "ciudad_default": node.get("ciudad_default",""),
                            "pais_default": node.get("pais_default",""),
                        }

    return dedupe(seeds_all), dedupe(priority_all), dedupe(hashtags_all), seed_meta


# =========================
# Crawl BFS
# =========================
def crawl_seed_bfs(session, seed, rules, depth, max_pages, global_seen, global_out, global_cap):
    q=deque([(seed,depth)])
    local_seen=set()
    added=0
    while q and added<max_pages and len(global_out)<global_cap:
        u,dleft=q.popleft()
        u=strip_fragment(u)
        if not u or u in local_seen:
            continue
        local_seen.add(u)
        if not same_domain(seed,u):
            continue
        if not url_allowed_by_rules(rules,u):
            continue
        if u not in global_seen:
            global_seen.add(u)
            global_out.append(u)
            added+=1
        if dleft<=1:
            continue
        html=fetch_url(session,u,use_cache=True)
        if not html:
            continue
        for link in extract_links(u,html):
            link=strip_fragment(link)
            if link and same_domain(seed,link):
                q.append((link,dleft-1))
    return added


# =========================
# Popup builder
# =========================
def build_umap_popup(ev: dict) -> str:
    titulo = normalize(ev.get("convocatoria") or ev.get("colectiva") or "")
    fecha = normalize(ev.get("fecha") or "")
    hora = normalize(ev.get("hora") or "")

    when=""
    if fecha and hora:
        when=f"{fecha} - {hora}"
    elif fecha:
        when=fecha
    elif hora:
        when=hora

    lines=[]
    if titulo:
        lines.append(f"## {titulo}")
    if when:
        lines.append(when)

    img=normalize(ev.get("imagen") or "")
    if img:
        if img.startswith("images/"):
            img=f"https://geochicas.github.io/8m-global-mapper/{img}"
        lines.append(f"{{{{{img}}}}}")

    cta=normalize(ev.get("cta_url") or "")
    if cta.startswith("http"):
        lines.append(f"[[{cta}|Accede a la convocatoria]]")

    return "\n".join(lines).strip()


# =========================
# MAIN
# =========================
# =========================
# Helpers: seed meta + país desde TLD
# =========================

_TLD_TO_COUNTRY = {
    "es": "España", "ar": "Argentina", "mx": "México", "cl": "Chile",
    "co": "Colombia", "pe": "Perú", "bo": "Bolivia", "uy": "Uruguay",
    "py": "Paraguay", "ec": "Ecuador", "ve": "Venezuela", "br": "Brasil",
    "pt": "Portugal", "fr": "Francia", "it": "Italia", "de": "Alemania",
    "at": "Austria", "ch": "Suiza", "be": "Bélgica", "nl": "Países Bajos",
    "uk": "Reino Unido", "ie": "Irlanda", "se": "Suecia", "no": "Noruega",
    "dk": "Dinamarca", "fi": "Finlandia", "pl": "Polonia", "cz": "República Checa",
    "sk": "Eslovaquia", "hu": "Hungría", "ro": "Rumania", "bg": "Bulgaria",
    "gr": "Grecia", "hr": "Croacia", "si": "Eslovenia", "rs": "Serbia",
    "tr": "Turquía", "ua": "Ucrania", "ru": "Rusia",
    "au": "Australia", "nz": "Nueva Zelanda", "ph": "Filipinas",
    "in": "India", "jp": "Japón", "kr": "Corea del Sur", "cn": "China",
    "id": "Indonesia", "my": "Malasia", "sg": "Singapur", "th": "Tailandia",
    "vn": "Vietnam", "pk": "Pakistán", "bd": "Bangladesh", "np": "Nepal",
    "za": "Sudáfrica", "ng": "Nigeria", "ke": "Kenia", "gh": "Ghana",
    "tz": "Tanzania", "ug": "Uganda", "et": "Etiopía", "sn": "Senegal",
    "cm": "Camerún", "ci": "Costa de Marfil", "ma": "Marruecos",
    "eg": "Egipto", "tn": "Túnez", "dz": "Argelia",
    "ca": "Canadá", "us": "Estados Unidos", "cr": "Costa Rica",
    "gt": "Guatemala", "hn": "Honduras", "sv": "El Salvador",
    "ni": "Nicaragua", "pa": "Panamá", "do": "República Dominicana",
    "cu": "Cuba", "ht": "Haití", "jm": "Jamaica",
    "cat": "España",
}

def _infer_country_from_url(url: str) -> str:
    """Infiere el país desde el TLD del dominio."""
    try:
        from urllib.parse import urlparse
        host = urlparse(url).netloc.lower().split(":")[0]
        if host.startswith("www."):
            host = host[4:]
        parts = host.rsplit(".", 1)
        if len(parts) == 2:
            tld = parts[1]
            return _TLD_TO_COUNTRY.get(tld, "")
    except Exception:
        pass
    return ""


def _find_seed_meta_for_url(url: str, seed_meta: dict) -> dict:
    """Encuentra el seed_meta que corresponde a una URL de candidato."""
    try:
        from urllib.parse import urlparse
        host = urlparse(url).netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        for seed_url, meta in seed_meta.items():
            seed_host = urlparse(seed_url).netloc.lower()
            if seed_host.startswith("www."):
                seed_host = seed_host[4:]
            if host == seed_host or host.endswith("." + seed_host):
                return meta
    except Exception:
        pass
    return {}


def main():
    ensure_dirs()
    session=make_session(timeout=REQUEST_TIMEOUT)
    rules=load_domain_rules()

    seeds,priority,hashtags,seed_meta=read_sources_merged()

    print(f"🌐 Seeds: {min(len(seeds),MAX_SEEDS)}")
    print(f"🎯 Priority URLs: {min(len(priority),MAX_PRIORITY)}")
    print(f"🧭 Crawl: depth={CRAWL_DEPTH} max_pages_per_seed={MAX_PAGES_PER_SEED}")

    candidates=[]
    seen=set()

    for u in priority[:MAX_PRIORITY]:
        u=strip_fragment(u)
        if u and u not in seen and url_allowed_by_rules(rules,u):
            seen.add(u)
            candidates.append(u)

    for seed in seeds[:MAX_SEEDS]:
        if len(candidates)>=MAX_TOTAL_CANDIDATES:
            break
        picked=crawl_seed_bfs(
            session,seed,rules,CRAWL_DEPTH,
            MAX_PAGES_PER_SEED,
            seen,candidates,MAX_TOTAL_CANDIDATES
        )
        if picked:
            print(f"🔗 {seed} -> candidatos: {picked}")

    print(f"🔎 Candidates total: {len(candidates)}")

    records=[]
    geocode_cache=load_geocode_cache(GEOCODE_CACHE_PATH)

    n_imgs=0
    n_geocoded=0
    n_low_score=0
    n_old_skip=0

    for url in candidates:
        html=fetch_url(session,url,use_cache=True)
        if not html:
            continue

        parsed=parse_page(url,html)
        ev=extract_event_fields(parsed)
        if not ev:
            continue

        score=int(ev.get("score_relevancia") or 0)
        if score<THRESHOLD_EXTRACT:
            n_low_score+=1
            continue

        # fecha mínima
        f=(ev.get("fecha") or "").strip()
        if f:
            try:
                dd=date.fromisoformat(f)
                if dd<MIN_EVENT_DATE:
                    n_old_skip+=1
                    continue
                ev["anio"]=str(dd.year)
            except:
                pass

        # Aplicar ciudad_default y pais_default desde seed_meta
        seed_info = _find_seed_meta_for_url(url, seed_meta)
        if seed_info:
            if not (ev.get("ciudad") or "").strip() and seed_info.get("ciudad_default"):
                ev["ciudad"] = seed_info["ciudad_default"]
                # Limpiar coordenadas viejas para forzar re-geocodificación con ciudad correcta
                ev.pop("lat", None)
                ev.pop("lon", None)
            if not (ev.get("pais") or "").strip() and seed_info.get("pais_default"):
                ev["pais"] = seed_info["pais_default"]

        # Inferir país desde TLD si aún no tiene
        if not (ev.get("pais") or "").strip():
            pais_tld = _infer_country_from_url(url)
            if pais_tld:
                ev["pais"] = pais_tld

        # geocode RESTAURADO
        geo=geocode_event(ev,geocode_cache=geocode_cache)
        if geo and geo.get("lat") and geo.get("lon"):
            ev["lat"]=geo["lat"]
            ev["lon"]=geo["lon"]
            n_geocoded+=1

        # imagen fallback
        img_url=(ev.get("imagen") or "").strip()
        if not img_url:
            m=re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',html,re.I)
            if m:
                img_url=m.group(1).strip()

        if img_url:
            img_abs=urljoin(url,img_url)
            if img_abs.startswith("//"):
                img_abs="https:"+img_abs
            if img_abs.startswith("http"):
                out=download_and_process_image(img_abs,out_dir=IMAGES_DIR)
                if out and out.get("public_url"):
                    ev["imagen"]=out["public_url"]
                    n_imgs+=1

        records.append(ev)

    save_geocode_cache(GEOCODE_CACHE_PATH,geocode_cache)

    for r in records:
        r["popup"]=build_umap_popup(r)

    export_master_csv(EXPORT_MASTER,records)
    export_umap_csv(EXPORT_UMAP,records,min_score=THRESHOLD_EXPORT_UMAP)
    export_sin_coord_csv(EXPORT_SIN_COORD,records,min_score=THRESHOLD_EXPORT_UMAP)

    print("")
    print(f"🧾 Eventos master: {len(records)}")
    print(f"🧠 Skipped low score: {n_low_score}")
    print(f"🗑️ Filtrados por fecha: {n_old_skip}")
    print(f"📍 Geocoded: {n_geocoded}")
    print(f"🖼️ imágenes descargadas: {n_imgs}")


if __name__=="__main__":
    main()
