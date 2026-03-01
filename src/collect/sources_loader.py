# src/collect/sources_loader.py
# Lee sources.yml (anidado) y devuelve:
# - seeds_urls (scrapeables)
# - social_urls (guardadas para futuro; bloqueadas sin APIs)
# - hashtags (para scoring/matching)
# - priority_urls (si existen)
# - url_meta: {seed_url: {"region": "...", "temas": ["..."]}}
# - domain_meta: {domain: {"region": "...", "temas": ["..."]}}
#
# Regla: por defecto NO usamos social_urls como seeds (ENABLE_SOCIAL_SEEDS=false).

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple
import os
from urllib.parse import urlparse

import yaml


SOCIAL_DOMAINS = ("instagram.com", "twitter.com", "x.com", "facebook.com", "fb.me", "t.co")


@dataclass
class SourcesBundle:
    seeds_urls: list[str]
    social_urls: list[str]
    hashtags: list[str]
    priority_urls: list[str]


def _dedupe(items: list[str]) -> list[str]:
    seen = set()
    out = []
    for x in items:
        x = (x or "").strip()
        if not x:
            continue
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _is_url(s: str) -> bool:
    s = (s or "").strip().lower()
    return s.startswith("http://") or s.startswith("https://")


def _domain_of(url: str) -> str:
    try:
        return (urlparse(url).netloc or "").lower()
    except Exception:
        return ""


def _looks_social(url: str) -> bool:
    u = (url or "").lower()
    return any(d in u for d in SOCIAL_DOMAINS)


def _ensure_hash_tag(t: str) -> str:
    t = (t or "").strip()
    if not t:
        return ""
    return t if t.startswith("#") else f"#{t}"


def _collect_from_node(
    node: Any,
    seeds: list[str],
    social: list[str],
    hashtags: list[str],
    priority: list[str],
) -> None:
    """
    Compat: colecta URLs/hashtags/prioridad desde estructuras libres.
    (Mantiene tu comportamiento previo para no romper nada)
    """
    if node is None:
        return

    if isinstance(node, list):
        for item in node:
            _collect_from_node(item, seeds, social, hashtags, priority)
        return

    if isinstance(node, str):
        s = node.strip()
        if not s:
            return
        if _is_url(s):
            seeds.append(s)
        elif s.startswith("#"):
            hashtags.append(s)
        return

    if isinstance(node, dict):
        if "priority_urls" in node and isinstance(node["priority_urls"], list):
            for u in node["priority_urls"]:
                if isinstance(u, str) and _is_url(u):
                    priority.append(u.strip())

        if "urls" in node and isinstance(node["urls"], list):
            for u in node["urls"]:
                if isinstance(u, str) and _is_url(u):
                    seeds.append(u.strip())

        if "social" in node and isinstance(node["social"], list):
            for u in node["social"]:
                if isinstance(u, str) and _is_url(u):
                    social.append(u.strip())

        if "hashtags" in node:
            h = node["hashtags"]
            if isinstance(h, list):
                for t in h:
                    if isinstance(t, str):
                        ht = _ensure_hash_tag(t)
                        if ht:
                            hashtags.append(ht)
            elif isinstance(h, str):
                ht = _ensure_hash_tag(h)
                if ht:
                    hashtags.append(ht)

        for _, v in node.items():
            _collect_from_node(v, seeds, social, hashtags, priority)
        return


def _collect_with_meta_from_seeds_tree(
    y: Any,
) -> Tuple[List[str], List[str], Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """
    Lee específicamente la estructura:
      seeds:
        <Region>:
          <Tema>:
            urls: [...]
            hashtags: [...]
            social: [...]

    Devuelve:
      seeds_urls, hashtags, url_meta, domain_meta
    """
    seeds_urls: List[str] = []
    hashtags: List[str] = []
    url_meta: Dict[str, Dict[str, Any]] = {}
    domain_meta: Dict[str, Dict[str, Any]] = {}

    if not isinstance(y, dict):
        return seeds_urls, hashtags, url_meta, domain_meta

    seeds_root = y.get("seeds")
    if not isinstance(seeds_root, dict):
        return seeds_urls, hashtags, url_meta, domain_meta

    for region, temas_node in seeds_root.items():
        if not isinstance(temas_node, dict):
            continue

        for tema, leaf in temas_node.items():
            if not isinstance(leaf, dict):
                continue

            urls = leaf.get("urls", [])
            hs = leaf.get("hashtags", [])
            soc = leaf.get("social", [])

            # hashtags
            if isinstance(hs, list):
                for t in hs:
                    if isinstance(t, str):
                        ht = _ensure_hash_tag(t)
                        if ht:
                            hashtags.append(ht)

            # urls (seeds)
            if isinstance(urls, list):
                for u in urls:
                    if not (isinstance(u, str) and _is_url(u)):
                        continue
                    uu = u.strip()
                    seeds_urls.append(uu)

                    m = {"region": str(region), "temas": [str(tema)]}
                    url_meta[uu] = m

                    d = _domain_of(uu)
                    if d:
                        # merge: acumula temas por dominio
                        dm = domain_meta.get(d) or {"region": str(region), "temas": []}
                        # si cambia región para el mismo dominio, nos quedamos con la primera (señal de conflicto)
                        if not dm.get("region"):
                            dm["region"] = str(region)

                        tlist = dm.get("temas") or []
                        if str(tema) not in tlist:
                            tlist.append(str(tema))
                        dm["temas"] = tlist
                        domain_meta[d] = dm

            # social: lo guardamos pero NO lo tratamos como seed scrapeable
            if isinstance(soc, list):
                pass

    return seeds_urls, hashtags, url_meta, domain_meta


def load_sources(path: str) -> SourcesBundle:
    """
    Mantiene tu API anterior para no romper imports.
    """
    if not os.path.exists(path):
        return SourcesBundle([], [], [], [])

    with open(path, "r", encoding="utf-8") as f:
        y = yaml.safe_load(f)

    seeds: list[str] = []
    social: list[str] = []
    hashtags: list[str] = []
    priority: list[str] = []

    _collect_from_node(y, seeds, social, hashtags, priority)

    social_set = set([s.lower() for s in social])
    seeds_filtered = []
    for u in seeds:
        if u.lower() in social_set:
            continue
        if _looks_social(u):
            social.append(u)
            continue
        seeds_filtered.append(u)

    return SourcesBundle(
        seeds_urls=_dedupe(seeds_filtered),
        social_urls=_dedupe(social),
        hashtags=_dedupe(hashtags),
        priority_urls=_dedupe(priority),
    )


def load_sources_with_meta(path: str) -> Tuple[List[str], List[str], List[str], Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """
    NUEVO: devuelve (seeds_urls, social_urls, hashtags, url_meta, domain_meta)
    - seeds_urls: scrapeables
    - social_urls: guardadas (no scrape)
    - hashtags: para scoring
    - url_meta: meta por seed exacto
    - domain_meta: meta por dominio (para herencia)
    """
    if not os.path.exists(path):
        return [], [], [], {}, {}

    with open(path, "r", encoding="utf-8") as f:
        y = yaml.safe_load(f)

    # 1) meta desde estructura seeds/<region>/<tema>/urls
    seeds_meta, hashtags_meta, url_meta, domain_meta = _collect_with_meta_from_seeds_tree(y)

    # 2) compat: recolecta todo (priority_urls, social, etc.)
    seeds: list[str] = []
    social: list[str] = []
    hashtags: list[str] = []
    priority: list[str] = []
    _collect_from_node(y, seeds, social, hashtags, priority)

    # mezcla seeds (preferimos los del árbol meta si existen)
    seeds_all = seeds_meta[:] if seeds_meta else seeds[:]

    # hashtags: union
    hashtags_all = hashtags_meta + hashtags

    # filtra social
    social_set = set([s.lower() for s in social])
    seeds_filtered = []
    for u in seeds_all:
        if u.lower() in social_set:
            continue
        if _looks_social(u):
            social.append(u)
            continue
        seeds_filtered.append(u)

    return _dedupe(seeds_filtered), _dedupe(social), _dedupe(hashtags_all), url_meta, domain_meta


def should_include_social_seeds() -> bool:
    v = os.environ.get("ENABLE_SOCIAL_SEEDS", "").strip().lower()
    return v in ("1", "true", "yes", "y", "on")
