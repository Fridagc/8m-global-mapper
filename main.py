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

# --- módulos del repo ---
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
THRESHOLD_EXPORT_UMAP = int(os.environ.get("THRESHOLD_EXPORT_UMAP", "10"))

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


def _canon_seed_prefix(u: str) -> str:
    u=strip_fragment(u)
    try:
        p=urlparse(u)
        host=(p.netloc or "").lower()
        if host.startswith("www."):
            host=host[4:]
        path=(p.path or "/")
        if not path.startswith("/"):
            path="/"+path
        return host+path
    except:
        return ""


def _canon_url(u:str)->str:
    return _canon_seed_prefix(u)


def _assign_meta_by_seed_prefix(url:str, seed_meta:dict[str,dict])->tuple[str,list[str]]:
    ukey=_canon_url(url)
    matches=[]
    for s,meta in (seed_meta or {}).items():
        skey=_canon_seed_prefix(s)
        if ukey.startswith(skey):
            matches.append((len(skey),meta or {}))
    if not matches:
        return ("",[])
    matches.sort(key=lambda x:x[0], reverse=True)
    region=""
    temas=[]
    seen=set()
    for _,meta in matches:
        r=meta.get("region","")
        if r and not region:
            region=r
        for t in meta.get("temas",[]):
            if t and t not in seen:
                seen.add(t)
                temas.append(t)
    return (region,temas)


def _extract_image_from_html(html:str)->str:
    if not html:
        return ""
    m=re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',html,re.I)
    if m:
        return m.group(1).strip()
    m=re.search(r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)["\']',html,re.I)
    if m:
        return m.group(1).strip()
    for m in re.finditer(r'<img[^>]+src=["\']([^"\']+)["\']',html,re.I):
        src=m.group(1).strip()
        if any(x in src.lower() for x in ["favicon","logo","icon","sprite","data:image"]):
            continue
        return src
    return ""


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
                        seed_meta[u]={"region":region,"temas":[tema]}

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
# MAIN
# =========================
def main():
    ensure_dirs()
    session=make_session(timeout=REQUEST_TIMEOUT)

    seeds,priority,hashtags,seed_meta=read_sources_merged()

    print(f"🌐 Seeds: {min(len(seeds),MAX_SEEDS)}")
    print(f"🎯 Priority URLs: {min(len(priority),MAX_PRIORITY)}")
    print(f"🧭 Crawl: depth={CRAWL_DEPTH} max_pages_per_seed={MAX_PAGES_PER_SEED}")

    candidates=[]
    seen=set()

    for u in priority[:MAX_PRIORITY]:
        u=strip_fragment(u)
        if u and u not in seen:
            seen.add(u)
            candidates.append(u)

    for seed in seeds[:MAX_SEEDS]:
        if len(candidates)>=MAX_TOTAL_CANDIDATES:
            break
        picked=crawl_seed_bfs(
            session,seed,{},CRAWL_DEPTH,
            MAX_PAGES_PER_SEED,
            seen,candidates,MAX_TOTAL_CANDIDATES
        )
        if picked:
            print(f"🔗 {seed} -> candidatos: {picked}")

    print(f"🔎 Candidates total: {len(candidates)}")

    records=[]
    geocode_cache=load_geocode_cache(GEOCODE_CACHE_PATH)

    n_imgs=0
    n_old_skip=0
    n_low_score=0

    for url in candidates:
        html=fetch_url(session,url,use_cache=True)
        if not html:
            continue

        parsed=parse_page(url,html)
        ev=extract_event_fields(parsed)
        if not ev:
            continue

        score=int(ev.get("score_relevancia") or 0)

        f=(ev.get("fecha") or "").strip()
        if f:
            try:
                dd=date.fromisoformat(f)
                ev["anio"]=str(dd.year)
                if dd<MIN_EVENT_DATE:
                    n_old_skip+=1
                    continue
            except:
                ev["anio"]=""

        if score<THRESHOLD_EXTRACT:
            n_low_score+=1
            continue

        region,temas=_assign_meta_by_seed_prefix(url,seed_meta)
        ev["region"]=region
        ev["temas"]="; ".join(temas) if temas else "Sin tema detectado"

        img_url=ev.get("imagen") or ""
        if not img_url:
            img_url=_extract_image_from_html(html)

        if img_url:
            img_abs=urljoin(url,img_url)
            if img_abs.startswith("http"):
                out=download_and_process_image(img_abs,out_dir=IMAGES_DIR)
                if out and out.get("public_url"):
                    ev["imagen"]=out["public_url"]
                    n_imgs+=1

        records.append(ev)

    save_geocode_cache(GEOCODE_CACHE_PATH,geocode_cache)

    export_master_csv(EXPORT_MASTER,records)
    export_umap_csv(EXPORT_UMAP,records,min_score=THRESHOLD_EXPORT_UMAP)
    export_sin_coord_csv(EXPORT_SIN_COORD,records,min_score=THRESHOLD_EXPORT_UMAP)

    print("")
    print(f"🧾 Eventos master: {len(records)}")
    print(f"🗑️  Filtrados por fecha: {n_old_skip}")
    print(f"🧠 Skipped low score: {n_low_score}")
    print(f"🖼️ imágenes descargadas: {n_imgs}")


if __name__=="__main__":
    main()
