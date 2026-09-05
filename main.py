import asyncio
import hashlib
import json
import os
import re
import time
import uuid
from collections import OrderedDict
from contextlib import asynccontextmanager
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse
from playwright.async_api import async_playwright

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse

load_dotenv()

SPOTIFY_PATHFINDER_URL = "https://api-partner.spotify.com/pathfinder/v2/query"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36")
SPOTIFY_HEADERS = {
    "accept": "application/json",
    "accept-language": "en-US,en;q=0.9",
    "app-platform": "WebPlayer",
    "origin": "https://open.spotify.com",
    "referer": "https://open.spotify.com/",
    "user-agent": UA,
}

PLAYWRIGHT_CONCURRENCY = int(os.getenv("PLAYWRIGHT_CONCURRENCY", "4"))
SPOTIFY_HTTP_CONCURRENCY = int(os.getenv("SPOTIFY_HTTP_CONCURRENCY", "20"))
EMBED_CONCURRENCY = int(os.getenv("EMBED_CONCURRENCY", "12"))

HTTP_CONNECT_TIMEOUT = float(os.getenv("HTTP_CONNECT_TIMEOUT", "5"))
HTTP_READ_TIMEOUT = float(os.getenv("HTTP_READ_TIMEOUT", "15"))
HTTP_WRITE_TIMEOUT = float(os.getenv("HTTP_WRITE_TIMEOUT", "15"))
HTTP_POOL_TIMEOUT = float(os.getenv("HTTP_POOL_TIMEOUT", "5"))

SEARCH_CACHE_TTL = int(os.getenv("SEARCH_CACHE_TTL", "300"))
TRACK_META_CACHE_TTL = int(os.getenv("TRACK_META_CACHE_TTL", "600"))
EMBED_CACHE_TTL = int(os.getenv("EMBED_CACHE_TTL", "600"))
LYRICS_CACHE_TTL = int(os.getenv("LYRICS_CACHE_TTL", "600"))

MAX_SEARCH_CACHE = int(os.getenv("MAX_SEARCH_CACHE", "500"))
MAX_TRACK_CACHE = int(os.getenv("MAX_TRACK_CACHE", "2000"))
MAX_EMBED_CACHE = int(os.getenv("MAX_EMBED_CACHE", "500"))
MAX_LYRICS_CACHE = int(os.getenv("MAX_LYRICS_CACHE", "1000"))
MAX_LIMIT = 50

SEARCH_CACHE: OrderedDict[str, dict] = OrderedDict()
TRACK_META_CACHE: OrderedDict[str, dict] = OrderedDict()
EMBED_CACHE: OrderedDict[str, dict] = OrderedDict()
LYRICS_CACHE: OrderedDict[str, dict] = OrderedDict()

SEARCH_LOCKS: dict[str, asyncio.Lock] = {}
SEARCH_LOCKS_GUARD = asyncio.Lock()
TOKEN_LOCKS: dict[str, asyncio.Lock] = {}
CLIENT_TOKEN_LOCKS: dict[str, asyncio.Lock] = {}
TOKEN_LOCKS_GUARD = asyncio.Lock()
CLIENT_TOKEN_LOCKS_GUARD = asyncio.Lock()

PLAYWRIGHT_SEMAPHORE: Optional[asyncio.Semaphore] = None
SPOTIFY_HTTP_SEMAPHORE: Optional[asyncio.Semaphore] = None
EMBED_SEMAPHORE: Optional[asyncio.Semaphore] = None


def _cred_key(value: str) -> str:
    if not value:
        return "anon"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _now() -> float:
    return time.time()


def _safe_log(message: str):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def _extract_sp_dc(request: Request, sp_dc_q: Optional[str]) -> str:
    sp_dc = (sp_dc_q or "").strip()
    if not sp_dc:
        sp_dc = (
            request.headers.get("x-sp-dc")
            or request.headers.get("x-sp_dc")
            or request.headers.get("sp_dc")
            or request.headers.get("SP_DC")
            or request.cookies.get("sp_dc")
            or ""
        ).strip()
    if not sp_dc:
        for key, value in request.query_params.items():
            if key.lower() in ("sp_dc", "spdc", "sp-dc"):
                sp_dc = value.strip()
                break
    return sp_dc


def _require_sp_dc(request: Request, sp_dc_q: Optional[str]) -> str:
    sp_dc = _extract_sp_dc(request, sp_dc_q)
    if not sp_dc or len(sp_dc) < 20:
        raise HTTPException(status_code=401, detail="sp_dc required per room")
    return sp_dc


async def get_search_lock(query: str) -> asyncio.Lock:
    async with SEARCH_LOCKS_GUARD:
        lock = SEARCH_LOCKS.get(query)
        if lock is None:
            lock = asyncio.Lock()
            SEARCH_LOCKS[query] = lock
        return lock


async def get_token_lock(credential_key: str) -> asyncio.Lock:
    async with TOKEN_LOCKS_GUARD:
        lock = TOKEN_LOCKS.get(credential_key)
        if lock is None:
            lock = asyncio.Lock()
            TOKEN_LOCKS[credential_key] = lock
        return lock


async def get_client_token_lock(credential_key: str) -> asyncio.Lock:
    async with CLIENT_TOKEN_LOCKS_GUARD:
        lock = CLIENT_TOKEN_LOCKS.get(credential_key)
        if lock is None:
            lock = asyncio.Lock()
            CLIENT_TOKEN_LOCKS[credential_key] = lock
        return lock


def _cache_put(cache: OrderedDict, key: str, value: dict, max_items: int):
    cache[key] = value
    cache.move_to_end(key)
    while len(cache) > max_items:
        cache.popitem(last=False)


def sanitize_track_id(raw: str) -> str:
    tid = raw.strip()
    if tid.startswith("spotify:track:"):
        tid = tid.split(":")[-1]
    elif "open.spotify.com/track/" in tid:
        tid = tid.split("/track/")[-1].split("?")[0].split("/")[0]
    return re.sub(r"[^a-zA-Z0-9]+", "", tid)


def format_duration(ms: int | None) -> str:
    if not ms:
        return "0:00"
    total_sec = ms // 1000
    h, rem = divmod(total_sec, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


def best_image(sources):
    if not sources:
        return None
    return max(sources, key=lambda x: x.get("width") or 0).get("url")


def extract_artists(value):
    if isinstance(value, dict):
        items = value.get("items") or []
    elif isinstance(value, list):
        items = value
    else:
        items = []
    result = []
    for artist in items:
        if not isinstance(artist, dict):
            continue
        name = ((artist.get("profile") or {}).get("name") or artist.get("name"))
        if name:
            result.append(name)
    return result


def extract_thumbnail(album):
    album = album or {}
    sources = album.get("coverArt", {}).get("sources", [])
    return best_image(sources)


def map_track(d: dict[str, Any]) -> dict[str, Any]:
    track_id = d.get("id")
    artists = extract_artists(d.get("artists"))
    album = d.get("albumOfTrack") or d.get("album") or {}
    album_uri = album.get("uri")
    duration_ms = (
        (d.get("duration") or {}).get("totalMilliseconds")
        or d.get("duration_ms") or 0
    )
    explicit = (d.get("contentRating") or {}).get("label") == "EXPLICIT"
    album_id = album_uri.split(":")[-1] if album_uri and ":" in album_uri else None
    return {
        "title": d.get("name"),
        "trackId": track_id,
        "link": f"https://open.spotify.com/track/{track_id}" if track_id else None,
        "thumbnail": extract_thumbnail(album),
        "artist": ", ".join(artists),
        "artistList": artists,
        "album": album.get("name"),
        "albumUrl": f"https://open.spotify.com/album/{album_id}" if album_id else None,
        "duration": format_duration(duration_ms),
        "durationMs": duration_ms,
        "explicit": explicit,
        "type": "track",
    }


async def new_page_with_cookies(browser, sp_dc: Optional[str] = None):
    ctx = await browser.new_context(user_agent=UA, locale="en-US")
    if sp_dc and len(sp_dc) > 20:
        try:
            await ctx.add_cookies([
                {"name": "sp_dc", "value": sp_dc, "domain": ".spotify.com",
                 "path": "/", "httpOnly": False, "secure": True, "sameSite": "Lax"},
                {"name": "sp_dc", "value": sp_dc, "domain": "open.spotify.com",
                 "path": "/", "httpOnly": False, "secure": True, "sameSite": "Lax"},
            ])
        except Exception as exc:
            _safe_log(f"[COOKIE] failed: {exc}")
    page = await ctx.new_page()
    return page, ctx


async def _get_access_token_uncached(app: FastAPI, sp_dc: str):
    browser = app.state.browser
    if not browser:
        raise HTTPException(status_code=503, detail="Browser not ready")
    credential_key = _cred_key(sp_dc)
    token = None
    expire = 0

    def capture_request(req):
        if "api-partner.spotify.com/pathfinder" not in req.url:
            return
        try:
            parsed = parse_qs(urlparse(req.url).query)
            operation_name = (parsed.get("operationName") or [None])[0]
            ext_value = (parsed.get("extensions") or [None])[0]
            if not operation_name or not ext_value:
                body = json.loads(req.post_data or "")
                if not isinstance(body, dict):
                    return
                operation_name = body.get("operationName")
                extensions = body.get("extensions") or {}
            else:
                extensions = json.loads(ext_value)
            if not operation_name:
                return
            sha = (extensions.get("persistedQuery") or {}).get("sha256Hash")
            if sha:
                app.state.persisted_hashes[operation_name] = sha
        except Exception:
            return

    async def on_response(resp):
        nonlocal token, expire
        capture_request(resp.request)
        if "open.spotify.com/api/token" not in resp.url:
            return
        try:
            data = await resp.json()
        except Exception:
            return
        access_token = data.get("accessToken")
        if access_token:
            token = access_token
            expire = int(data.get("accessTokenExpirationTimestampMs", 0)) / 1000

    async with PLAYWRIGHT_SEMAPHORE:
        page = None
        ctx = None
        try:
            page, ctx = await new_page_with_cookies(browser, sp_dc)
            page.on("response", on_response)
            page.on("request", capture_request)
            await page.goto("https://open.spotify.com/search/hello",
                            wait_until="domcontentloaded", timeout=30000)
            for _ in range(60):
                if token:
                    break
                await asyncio.sleep(0.25)
        except Exception as exc:
            _safe_log(f"[TOKEN] goto error [{credential_key}]: {exc}")
        finally:
            if page:
                try: await page.close()
                except Exception: pass
            if ctx:
                try: await ctx.close()
                except Exception: pass

    if not token:
        raise HTTPException(status_code=502,
                            detail=f"Spotify access token unavailable for room {credential_key}")
    return token, expire or (_now() + 3600)


async def get_access_token(app: FastAPI, sp_dc: str) -> str:
    credential_key = _cred_key(sp_dc)
    cached = app.state.token_cache.get(credential_key)
    if cached and _now() < cached[1] - 30:
        return cached[0]
    lock = await get_token_lock(credential_key)
    async with lock:
        cached = app.state.token_cache.get(credential_key)
        if cached and _now() < cached[1] - 30:
            return cached[0]
        token, expires_at = await _get_access_token_uncached(app, sp_dc)
        app.state.token_cache[credential_key] = (token, expires_at)
        _safe_log(f"[TOKEN] refreshed room={credential_key}")
        return token


async def _get_client_token_uncached(app: FastAPI, sp_dc: str):
    browser = app.state.browser
    if not browser:
        raise HTTPException(status_code=503, detail="Browser not ready")
    credential_key = _cred_key(sp_dc)
    client_token = None
    expires_at = 0

    async def on_response(resp):
        nonlocal client_token, expires_at
        if "clienttoken.spotify.com/v1/clienttoken" not in resp.url:
            return
        try:
            data = await resp.json()
            granted = data.get("granted_token") or {}
            token = granted.get("token")
            if token:
                client_token = token
                expires_at = _now() + int(granted.get("expires_after_seconds", 3600))
        except Exception:
            pass

    async with PLAYWRIGHT_SEMAPHORE:
        page = None
        ctx = None
        try:
            page, ctx = await new_page_with_cookies(browser, sp_dc)
            page.on("response", on_response)
            await page.goto("https://open.spotify.com/search/hello",
                            wait_until="domcontentloaded", timeout=30000)
            for _ in range(60):
                if client_token:
                    break
                await asyncio.sleep(0.25)
        except Exception as exc:
            _safe_log(f"[CLIENT-TOKEN] error [{credential_key}]: {exc}")
        finally:
            if page:
                try: await page.close()
                except Exception: pass
            if ctx:
                try: await ctx.close()
                except Exception: pass

    if not client_token:
        raise HTTPException(status_code=502,
                            detail=f"Spotify client token unavailable for room {credential_key}")
    return client_token, expires_at or (_now() + 3600)


async def get_client_token(app: FastAPI, sp_dc: str) -> str:
    credential_key = _cred_key(sp_dc)
    cached = app.state.client_token_cache.get(credential_key)
    if cached and _now() < cached[1] - 60:
        return cached[0]
    lock = await get_client_token_lock(credential_key)
    async with lock:
        cached = app.state.client_token_cache.get(credential_key)
        if cached and _now() < cached[1] - 60:
            return cached[0]
        token, expires_at = await _get_client_token_uncached(app, sp_dc)
        app.state.client_token_cache[credential_key] = (token, expires_at)
        return token


async def discover_persisted_hash(app: FastAPI, operation_name: str, sp_dc: Optional[str] = None):
    existing = app.state.persisted_hashes.get(operation_name)
    if existing:
        return existing
    browser = app.state.browser
    if not browser:
        raise HTTPException(status_code=503, detail="Browser not ready")
    discovered = None

    def capture_request(req):
        nonlocal discovered
        if discovered or "api-partner.spotify.com/pathfinder" not in req.url:
            return
        try:
            body = json.loads(req.post_data or "")
        except Exception:
            return
        if body.get("operationName") != operation_name:
            return
        discovered = ((body.get("extensions") or {}).get("persistedQuery") or {}).get("sha256Hash")

    async with PLAYWRIGHT_SEMAPHORE:
        page = None
        ctx = None
        try:
            page, ctx = await new_page_with_cookies(browser, sp_dc)
            page.on("request", capture_request)
            await page.goto("https://open.spotify.com/search/hello",
                            wait_until="domcontentloaded", timeout=30000)
            for _ in range(60):
                if discovered:
                    break
                await asyncio.sleep(0.25)
        except Exception:
            pass
        finally:
            if page:
                try: await page.close()
                except Exception: pass
            if ctx:
                try: await ctx.close()
                except Exception: pass

    if not discovered:
        raise HTTPException(status_code=502, detail=f"Hash for {operation_name} not found")
    app.state.persisted_hashes[operation_name] = discovered
    return discovered


async def spotify_post(app: FastAPI, payload: dict, headers: dict):
    async with SPOTIFY_HTTP_SEMAPHORE:
        try:
            return await app.state.http.post(SPOTIFY_PATHFINDER_URL,
                                             json=payload, headers=headers)
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout,
                httpx.WriteTimeout, httpx.PoolTimeout) as exc:
            raise HTTPException(status_code=504,
                                detail=f"Spotify upstream timeout: {type(exc).__name__}")


async def spotify_query(app: FastAPI, operation_name: str, variables: dict,
                        sp_dc: Optional[str] = None, sha256_hash: Optional[str] = None):
    token = await get_access_token(app, sp_dc or "")
    sha256_hash = sha256_hash or app.state.persisted_hashes.get(operation_name)
    if not sha256_hash:
        sha256_hash = await discover_persisted_hash(app, operation_name, sp_dc)

    payload = {
        "operationName": operation_name,
        "variables": variables,
        "extensions": {"persistedQuery": {"version": 1, "sha256Hash": sha256_hash}},
    }
    headers = {
        "authorization": f"Bearer {token}",
        "accept": "application/json",
        "app-platform": "WebPlayer",
        "origin": "https://open.spotify.com",
        "referer": "https://open.spotify.com/",
        "user-agent": UA,
    }

    client_token = await get_client_token(app, sp_dc or "")
    if client_token:
        headers["client-token"] = client_token

    response = await spotify_post(app, payload, headers)
    credential_key = _cred_key(sp_dc or "")

    if response.status_code == 401:
        app.state.token_cache.pop(credential_key, None)
        token = await get_access_token(app, sp_dc or "")
        headers["authorization"] = f"Bearer {token}"
        response = await spotify_post(app, payload, headers)

    if response.status_code in (400, 404):
        app.state.token_cache.pop(credential_key, None)
        app.state.persisted_hashes.pop(operation_name, None)
        token = await get_access_token(app, sp_dc or "")
        new_hash = await discover_persisted_hash(app, operation_name, sp_dc)
        payload["extensions"] = {"persistedQuery": {"version": 1, "sha256Hash": new_hash}}
        headers["authorization"] = f"Bearer {token}"
        response = await spotify_post(app, payload, headers)

    if response.status_code != 200:
        raise HTTPException(status_code=502,
                            detail=f"Pathfinder error {response.status_code}: {response.text[:300]}")
    return response.json()


def extract_search(data):
    root = data.get("data") or {}
    search = root.get("searchV2") or root.get("search")
    if not search:
        return [], 0
    tracks = search.get("tracksV2") or search.get("tracks") or {}
    items = tracks.get("items") or []
    total = tracks.get("totalCount") or 0
    result = []
    for item in items:
        if not isinstance(item, dict):
            continue
        track = ((item.get("item") or {}).get("data")
                 or item.get("track") or item.get("data") or item)
        if isinstance(track, dict) and str(track.get("uri", "")).startswith("spotify:track:"):
            result.append(track)
    return result, total


def get_search_cache(query: str):
    cache = SEARCH_CACHE.get(query)
    if not cache:
        return None
    if _now() - cache["updated_at"] > SEARCH_CACHE_TTL:
        SEARCH_CACHE.pop(query, None)
        return None
    SEARCH_CACHE.move_to_end(query)
    return cache


def create_search_cache(query: str):
    cache = {"tracks": [], "seen": set(), "continuation_offset": 0,
             "total_results": 0, "has_more": True, "updated_at": _now()}
    _cache_put(SEARCH_CACHE, query, cache, MAX_SEARCH_CACHE)
    return cache


async def fetch_next_spotify_page(app, query, cache, fetch_limit=50):
    if not cache["has_more"]:
        return False
    data = await spotify_query(
        app, "searchDesktop",
        {"searchTerm": query, "offset": cache["continuation_offset"],
         "limit": fetch_limit, "numberOfTopResults": fetch_limit,
         "includeAudiobooks": False, "includeAuthors": False,
         "includePreReleases": False},
        sp_dc=None,
    )
    items, total = extract_search(data)
    cache["total_results"] = total
    for track in items:
        track_id = track.get("id")
        if not track_id or track_id in cache["seen"]:
            continue
        cache["seen"].add(track_id)
        cache["tracks"].append(map_track(track))
    cache["continuation_offset"] += len(items)
    cache["updated_at"] = _now()
    if len(items) < fetch_limit or (total and cache["continuation_offset"] >= total):
        cache["has_more"] = False
    return bool(items)


async def fetch_search_page(app, query, page, limit):
    query = query.strip()
    if not query:
        return {"data": [], "page": page, "limit": limit, "total": 0, "hasNext": False}

    lock = await get_search_lock(query)
    async with lock:
        cache = get_search_cache(query) or create_search_cache(query)
        start = (page - 1) * limit
        end = start + limit
        fetched = 0
        while len(cache["tracks"]) < end and cache["has_more"] and fetched < 25:
            fetched += 1
            if not await fetch_next_spotify_page(app, query, cache):
                break
        data = cache["tracks"][start:end]
        return {"data": data, "page": page, "limit": limit,
                "total": len(cache["tracks"]),
                "totalResults": cache.get("total_results", 0),
                "hasNext": len(cache["tracks"]) > end or cache["has_more"]}


def find_track_objs(obj, depth=0, out=None):
    if out is None:
        out = []
    if depth > 9 or not isinstance(obj, (dict, list)):
        return out
    if isinstance(obj, dict):
        if (str(obj.get("uri", "")).startswith("spotify:track:")
                and "albumOfTrack" in obj and "duration" in obj):
            out.append(obj)
        for value in obj.values():
            find_track_objs(value, depth + 1, out)
    else:
        for item in obj:
            find_track_objs(item, depth + 1, out)
    return out


async def get_track_metadata(app, track_id: str, sp_dc: str):
    tid = sanitize_track_id(track_id)
    now = _now()
    cached = TRACK_META_CACHE.get(tid)
    if cached and now < cached["expiresAt"]:
        TRACK_META_CACHE.move_to_end(tid)
        return cached["data"]
    if cached:
        TRACK_META_CACHE.pop(tid, None)

    for cache in SEARCH_CACHE.values():
        for track in cache.get("tracks", []):
            if track.get("trackId") == tid:
                _cache_put(TRACK_META_CACHE, tid,
                           {"data": track, "expiresAt": now + TRACK_META_CACHE_TTL},
                           MAX_TRACK_CACHE)
                return track

    oembed_thumb = None
    oembed_title = "Unknown"
    oembed_artist = ""

    try:
        response = await app.state.http.get(
            f"https://open.spotify.com/oembed?url=https://open.spotify.com/track/{tid}",
            headers={"User-Agent": UA}, timeout=10.0)
        if response.status_code == 200:
            data = response.json()
            oembed_thumb = data.get("thumbnail_url")
            title = data.get("title") or "Unknown"
            if " - " in title:
                oembed_title, oembed_artist = title.split(" - ", 1)
            elif data.get("author_name"):
                oembed_title, oembed_artist = title, data["author_name"]
            else:
                oembed_title = title
    except Exception:
        pass

    try:
        data = await asyncio.wait_for(
            spotify_query(app, "getTrack",
                          {"uri": f"spotify:track:{tid}",
                           "includeVideoAssociationItems": False},
                          sp_dc=sp_dc),
            timeout=15.0)
        objects = find_track_objs(data.get("data"))
        found = next((item for item in objects if item.get("id") == tid), None)
        root = data.get("data") or {}
        if not found:
            track_union = root.get("trackUnion") or {}
            if track_union.get("id") == tid:
                found = track_union
        if found:
            if not found.get("artists"):
                first_artist = ((root.get("trackUnion") or {}).get("firstArtist")
                                or found.get("firstArtist") or {})
                if isinstance(first_artist, dict) and first_artist.get("items"):
                    found = dict(found)
                    found["artists"] = {"items": first_artist["items"]}
            metadata = map_track(found)
            if not metadata.get("thumbnail") and oembed_thumb:
                metadata["thumbnail"] = oembed_thumb
            _cache_put(TRACK_META_CACHE, tid,
                       {"data": metadata, "expiresAt": _now() + TRACK_META_CACHE_TTL},
                       MAX_TRACK_CACHE)
            return metadata
    except Exception:
        pass

    if oembed_thumb:
        metadata = {
            "title": oembed_title, "trackId": tid,
            "link": f"https://open.spotify.com/track/{tid}",
            "thumbnail": oembed_thumb, "artist": oembed_artist,
            "artistList": [oembed_artist] if oembed_artist else [],
            "album": None, "albumUrl": None, "duration": "0:00",
            "durationMs": 0, "explicit": False, "type": "track",
        }
        _cache_put(TRACK_META_CACHE, tid,
                   {"data": metadata, "expiresAt": _now() + 30},
                   MAX_TRACK_CACHE)
        return metadata
    return None


SPCLIENT_LYRICS_URL = "https://spclient.wg.spotify.com/color-lyrics/v2/track/{track_id}?format=json&vocalRemoval=false&market=from_token"


def _normalize_lyrics_payload(track_id: str, data: dict) -> dict:
    node = (data.get("lyrics") or {}) if isinstance(data, dict) else {}
    sync_type = node.get("syncType") or "UNSYNCED"
    raw_lines = node.get("lines") or []
    lines: list[dict[str, Any]] = []
    for ln in raw_lines:
        if not isinstance(ln, dict):
            continue
        text = (ln.get("words") or "").strip()
        # Keep empty lines as instrumental breaks so timing gaps stay visible.
        # Frontend renders them as spacer glyph.
        try:
            start_ms = int(ln.get("startTimeMs") or 0)
        except (ValueError, TypeError):
            start_ms = 0
        try:
            dur_ms = int(ln.get("durationMs") or 0)
        except (ValueError, TypeError):
            dur_ms = 0
        lines.append({
            "startMs": max(0, start_ms),
            "durationMs": max(0, dur_ms),
            "endMs": max(0, start_ms) + max(0, dur_ms),
            "text": text,
        })
    # Ensure chronological order for binary-search sync on frontend.
    lines.sort(key=lambda x: x["startMs"])
    has_sync = sync_type == "LINE_SYNCED" and any(
        ln["startMs"] > 0 or ln["text"] for ln in lines
    )
    return {
        "trackId": track_id,
        "syncType": sync_type,
        "hasSync": bool(has_sync),
        "lines": lines,
        "provider": node.get("provider") or data.get("provider") if isinstance(data, dict) else None,
        "colors": data.get("colors") if isinstance(data, dict) else None,
    }


async def fetch_spotify_lyrics(app: FastAPI, track_id: str, sp_dc: str) -> dict | None:
    """Fetch synced lyrics via spclient color-lyrics API. Returns None if unavailable."""
    tid = sanitize_track_id(track_id)
    if not tid:
        return None
    token = await get_access_token(app, sp_dc)
    try:
        client_token: Optional[str] = await get_client_token(app, sp_dc)
    except Exception:
        client_token = None

    url = SPCLIENT_LYRICS_URL.format(track_id=tid)
    headers = {
        "authorization": f"Bearer {token}",
        "app-platform": "WebPlayer",
        "origin": "https://open.spotify.com",
        "referer": "https://open.spotify.com/",
        "user-agent": UA,
        "accept": "application/json",
    }
    if client_token:
        headers["client-token"] = client_token

    async def _do_get(hdrs: dict):
        async with SPOTIFY_HTTP_SEMAPHORE:
            return await app.state.http.get(url, headers=hdrs, timeout=10.0)

    try:
        resp = await _do_get(headers)
    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout,
            httpx.WriteTimeout, httpx.PoolTimeout) as exc:
        raise HTTPException(status_code=504,
                            detail=f"Lyrics upstream timeout: {type(exc).__name__}")

    if resp.status_code == 401:
        # Token expired — refresh once and retry.
        app.state.token_cache.pop(_cred_key(sp_dc), None)
        token = await get_access_token(app, sp_dc)
        headers["authorization"] = f"Bearer {token}"
        try:
            resp = await _do_get(headers)
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout,
                httpx.WriteTimeout, httpx.PoolTimeout) as exc:
            raise HTTPException(status_code=504,
                                detail=f"Lyrics upstream timeout: {type(exc).__name__}")

    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        raise HTTPException(status_code=502,
                            detail=f"Lyrics error {resp.status_code}: {resp.text[:300]}")
    try:
        data = resp.json()
    except Exception:
        raise HTTPException(status_code=502, detail="Lyrics invalid JSON")
    if not data or not (data.get("lyrics") or {}).get("lines"):
        return None
    return _normalize_lyrics_payload(tid, data)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global PLAYWRIGHT_SEMAPHORE, SPOTIFY_HTTP_SEMAPHORE, EMBED_SEMAPHORE
    PLAYWRIGHT_SEMAPHORE = asyncio.Semaphore(PLAYWRIGHT_CONCURRENCY)
    SPOTIFY_HTTP_SEMAPHORE = asyncio.Semaphore(SPOTIFY_HTTP_CONCURRENCY)
    EMBED_SEMAPHORE = asyncio.Semaphore(EMBED_CONCURRENCY)

    timeout = httpx.Timeout(connect=HTTP_CONNECT_TIMEOUT, read=HTTP_READ_TIMEOUT,
                            write=HTTP_WRITE_TIMEOUT, pool=HTTP_POOL_TIMEOUT)
    limits = httpx.Limits(max_connections=200, max_keepalive_connections=50, keepalive_expiry=60)
    app.state.http = httpx.AsyncClient(timeout=timeout, limits=limits,
                                       follow_redirects=True, http2=True,
                                       headers=SPOTIFY_HEADERS)
    app.state.token_cache = {}
    app.state.client_token_cache = {}
    app.state.persisted_hashes = {}

    _safe_log("Spotify API starting")
    _safe_log(f"PLAYWRIGHT_CONCURRENCY={PLAYWRIGHT_CONCURRENCY}")
    _safe_log(f"SPOTIFY_HTTP_CONCURRENCY={SPOTIFY_HTTP_CONCURRENCY}")
    _safe_log(f"EMBED_CONCURRENCY={EMBED_CONCURRENCY}")
    _safe_log("strict per-room sp_dc mode")

    async with async_playwright() as pw:
        app.state.browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu",
                  "--disable-background-networking",
                  "--disable-background-timer-throttling",
                  "--disable-renderer-backgrounding",
                  "--disable-features=Translate,BackForwardCache"],
        )
        yield
        try: await app.state.http.aclose()
        except Exception: pass
        try: await app.state.browser.close()
        except Exception: pass


app = FastAPI(title="Spotify Multi-Room API",
              description="Spotify API with isolated per-room sp_dc credentials and bounded concurrency",
              lifespan=lifespan)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])


@app.middleware("http")
async def request_logger(request: Request, call_next):
    request_id = uuid.uuid4().hex[:8]
    started = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception as exc:
        elapsed = time.perf_counter() - started
        _safe_log(f"[REQ {request_id}] {request.method} {request.url.path} ERROR {elapsed:.3f}s {type(exc).__name__}")
        raise
    elapsed = time.perf_counter() - started
    response.headers["X-Request-ID"] = request_id
    _safe_log(f"[REQ {request_id}] {request.method} {request.url.path} {response.status_code} {elapsed:.3f}s")
    return response


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "browser": getattr(app.state, "browser", None) is not None,
        "playwrightConcurrency": PLAYWRIGHT_CONCURRENCY,
        "spotifyHttpConcurrency": SPOTIFY_HTTP_CONCURRENCY,
        "embedConcurrency": EMBED_CONCURRENCY,
        "time": int(_now()),
    }


@app.get("/")
async def root():
    return {
        "message": "Spotify Multi-Room API",
        "mode": "strict per-room sp_dc",
        "search": "/search?q=QUERY&page=1&limit=20",
        "track": "/track?trackId=TRACK_ID&sp_dc=...",
        "lyrics": "/lyrics?trackId=TRACK_ID&sp_dc=...",
        "player": "/player?trackId=TRACK_ID&sp_dc=...",
        "embed-proxy": "/embed-proxy?trackId=TRACK_ID&sp_dc=...",
        "health": "/health",
        "concurrency": {
            "playwright": PLAYWRIGHT_CONCURRENCY,
            "spotifyHttp": SPOTIFY_HTTP_CONCURRENCY,
            "embed": EMBED_CONCURRENCY,
        },
    }


@app.get("/search")
async def search(q: str = Query(...), page: int = Query(1, ge=1),
                 limit: int = Query(20, ge=1, le=MAX_LIMIT),
                 offset: Optional[int] = Query(None, ge=0)):
    if offset is not None:
        page = (offset // limit) + 1
    return await fetch_search_page(app, q, page, limit)


@app.get("/track")
async def track_ep(request: Request, trackId: str = Query(...),
                   sp_dc: Optional[str] = Query(None)):
    tid = sanitize_track_id(trackId)
    if not tid:
        raise HTTPException(status_code=400, detail="Invalid trackId")
    room_sp_dc = _require_sp_dc(request, sp_dc)
    metadata = await get_track_metadata(app, tid, room_sp_dc)
    if not metadata:
        raise HTTPException(status_code=404, detail="Track not found")
    return metadata


@app.get("/lyrics")
async def lyrics_ep(request: Request, trackId: str = Query(...),
                    sp_dc: Optional[str] = Query(None)):
    tid = sanitize_track_id(trackId)
    if not tid:
        raise HTTPException(status_code=400, detail="Invalid trackId")
    room_sp_dc = _require_sp_dc(request, sp_dc)

    cached = LYRICS_CACHE.get(tid)
    if cached and _now() < cached["expiresAt"]:
        LYRICS_CACHE.move_to_end(tid)
        return cached["data"]
    if cached:
        LYRICS_CACHE.pop(tid, None)

    payload = await fetch_spotify_lyrics(app, tid, room_sp_dc)
    if not payload or not payload.get("lines"):
        # Cache negative result briefly to avoid hammering upstream.
        _cache_put(LYRICS_CACHE, tid,
                   {"data": {"trackId": tid, "syncType": "NONE",
                             "hasSync": False, "lines": [], "provider": None},
                    "expiresAt": _now() + 60},
                   MAX_LYRICS_CACHE)
        raise HTTPException(status_code=404, detail="Lyrics not available for this track")
    _cache_put(LYRICS_CACHE, tid,
               {"data": payload, "expiresAt": _now() + LYRICS_CACHE_TTL},
               MAX_LYRICS_CACHE)
    return payload


@app.get("/embed-proxy")
async def embed_proxy(request: Request, trackId: str = Query(...),
                      sp_dc: Optional[str] = Query(None)):
    room_sp_dc = _require_sp_dc(request, sp_dc)
    tid = sanitize_track_id(trackId)
    if not tid:
        raise HTTPException(status_code=400, detail="Invalid trackId")

    credential_key = _cred_key(room_sp_dc)
    cache_key = f"{tid}:{credential_key}"
    cached = EMBED_CACHE.get(cache_key)
    if cached and _now() < cached["expiresAt"]:
        EMBED_CACHE.move_to_end(cache_key)
        return HTMLResponse(content=cached["html"], headers=cached["headers"])
    if cached:
        EMBED_CACHE.pop(cache_key, None)

    target = f"https://open.spotify.com/embed/track/{tid}?utm_source=generator&theme=0"
    headers = {"User-Agent": UA, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
               "Accept-Language": "en-US,en;q=0.9", "Referer": "https://open.spotify.com/"}
    try:
        async with EMBED_SEMAPHORE:
            response = await app.state.http.get(target, headers=headers,
                                                cookies={"sp_dc": room_sp_dc}, timeout=25.0)
    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout, httpx.PoolTimeout) as exc:
        raise HTTPException(status_code=504, detail=f"Spotify embed timeout: {type(exc).__name__}")

    if response.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Embed fetch {response.status_code}")

    html = response.text
    # if "</head>" in html:
    #     html = html.replace("</head>",
    #         '<style>[data-testid="embed-widget-container"]{opacity:1 !important}'
    #         '[data-testid="embed-widget-skeleton"],[data-testid="skeleton"]{display:none !important}'
    #         "</style></head>", 1)
        
    if "</head>" in html:
        html = html.replace(
            "</head>",
            '<style>'
            '[data-testid="embed-widget-container"]{opacity:1 !important}'
            '[data-testid="embed-widget-skeleton"],'
            '[data-testid="skeleton"],'
            '[data-testid="save-on-spotify"]{display:none !important}'
            '</style></head>',
            1
        )

    response_headers = {
        "X-Frame-Options": "ALLOWALL",
        "Content-Security-Policy": "frame-ancestors *",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "public, max-age=300",
    }
    _cache_put(EMBED_CACHE, cache_key,
               {"html": html, "headers": response_headers,
                "expiresAt": _now() + EMBED_CACHE_TTL},
               MAX_EMBED_CACHE)
    return HTMLResponse(content=html, headers=response_headers)


@app.get("/player", response_class=FileResponse)
async def player(request: Request, trackId: str = Query(...),
                 sp_dc: Optional[str] = Query(None)):
    _require_sp_dc(request, sp_dc)
    return FileResponse("player.html", media_type="text/html")


@app.delete("/cache")
async def clear_cache():
    SEARCH_CACHE.clear()
    TRACK_META_CACHE.clear()
    EMBED_CACHE.clear()
    LYRICS_CACHE.clear()
    return {"success": True, "message": "All caches cleared"}


@app.delete("/cache/{query}")
async def clear_query_cache(query: str):
    existed = query in SEARCH_CACHE
    SEARCH_CACHE.pop(query, None)
    return {"success": True, "query": query, "removed": existed}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=1404, workers=1)
