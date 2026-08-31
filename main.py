import asyncio
import json
import os
import re
import time
from contextlib import asynccontextmanager
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from playwright.async_api import async_playwright

load_dotenv()

SP_DC = os.getenv("sp_dc") or os.getenv("SP_DC") or ""
SP_KEY = os.getenv("sp_key") or os.getenv("SP_KEY") or ""

SPOTIFY_PATHFINDER_URL = "https://api-partner.spotify.com/pathfinder/v2/query"

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
_headers = {
    "accept": "application/json",
    "accept-language": "en-US,en;q=0.9",
    "app-platform": "WebPlayer",
    "origin": "https://open.spotify.com",
    "referer": "https://open.spotify.com/",
    "user-agent": UA,
}

# ---------------------------------------------------------------------------
# Caches — mirip api-soundcloud
# ---------------------------------------------------------------------------
SEARCH_CACHE: dict = {}
CACHE_TTL = 300
CACHE_LOCK = asyncio.Lock()

TRACK_META_CACHE: dict = {}
TRACK_META_TTL = 600

EMBED_CACHE: dict = {}
EMBED_CACHE_TTL = 600
EMBED_LOCK = asyncio.Lock()

MAX_LIMIT = 50


# ---------------------------------------------------------------------------
# Lifespan — httpx + playwright (untuk token Spotify)
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.http = httpx.AsyncClient(
        timeout=httpx.Timeout(connect=5.0, read=10.0, write=10.0, pool=5.0),
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=30),
        http2=True,
        headers=_headers,
    )
    app.state.sp_dc = SP_DC
    app.state.sp_key = SP_KEY
    if SP_DC:
        print(f"[INIT] sp_dc loaded: {SP_DC[:12]}... ({len(SP_DC)} chars)")
    else:
        print("[INIT] WARNING: sp_dc not found – anonymous mode")

    async with async_playwright() as pw:
        app.state.browser = await pw.chromium.launch(
            headless=True, args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        )
        app.state.token = None
        app.state.token_expire = 0
        app.state.client_token = None
        app.state.client_expire = 0
        app.state.persisted_hashes = {}
        yield
        await app.state.http.aclose()
        await app.state.browser.close()


app = FastAPI(
    title="Spotify Search API",
    description="Scrape Spotify search results with pagination and caching — konsep sama seperti api-soundcloud",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Helpers — format mirip api-soundcloud
# ---------------------------------------------------------------------------
def sanitize_track_id(raw: str) -> str:
    tid = raw.strip()
    if tid.startswith("spotify:track:"):
        tid = tid.split(":")[-1]
    elif "open.spotify.com/track/" in tid:
        tid = tid.split("/track/")[-1].split("?")[0].split("/")[0]
    tid = re.sub(r"[^a-zA-Z0-9]+", "", tid)
    return tid


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
    res = []
    for a in items:
        if not isinstance(a, dict):
            continue
        name = (a.get("profile") or {}).get("name") or a.get("name")
        if name:
            res.append(name)
    return res


def extract_thumbnail(album):
    album = album or {}
    sources = album.get("coverArt", {}).get("sources", [])
    return best_image(sources)


def map_track(d: dict[str, Any]) -> dict[str, Any]:
    """parse_track versi Spotify — output disamakan dengan api-soundcloud"""
    track_id = d.get("id")
    artists = extract_artists(d.get("artists"))
    album = d.get("albumOfTrack") or d.get("album") or {}
    album_uri = album.get("uri")
    duration_ms = (d.get("duration") or {}).get("totalMilliseconds") or d.get("duration_ms") or 0
    explicit = (d.get("contentRating") or {}).get("label") == "EXPLICIT"
    album_id = None
    if album_uri and ":" in album_uri:
        album_id = album_uri.split(":")[-1]
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


# ---------------------------------------------------------------------------
# Token & Pathfinder — satu-satunya bagian yang beda dari SoundCloud
# (SoundCloud pakai client_id scraped, Spotify pakai accessToken via playwright)
# ---------------------------------------------------------------------------
async def new_page_with_cookies(browser, use_cookie: bool = True):
    if SP_DC and use_cookie:
        ctx = await browser.new_context(user_agent=UA, locale="en-US")
        try:
            await ctx.add_cookies(
                [
                    {"name": "sp_dc", "value": SP_DC, "domain": ".spotify.com", "path": "/", "httpOnly": False, "secure": True, "sameSite": "Lax"},
                    {"name": "sp_dc", "value": SP_DC, "domain": "open.spotify.com", "path": "/", "httpOnly": False, "secure": True, "sameSite": "Lax"},
                ]
            )
        except Exception as e:
            print(f"[COOKIE] add failed: {e}")
        page = await ctx.new_page()
        return page, ctx
    else:
        ctx = await browser.new_context(user_agent=UA, locale="en-US")
        page = await ctx.new_page()
        return page, ctx


async def get_access_token(app: FastAPI, force_anonymous: bool = False) -> str:
    now = time.time()
    if not force_anonymous and app.state.token and now < app.state.token_expire - 30:
        return app.state.token
    browser = app.state.browser
    if not browser:
        raise HTTPException(status_code=503, detail="Browser not ready")

    token = None
    expire = 0

    def capture_request(req):
        if "api-partner.spotify.com/pathfinder" not in req.url:
            return
        try:
            parsed = parse_qs(urlparse(req.url).query)
            op = (parsed.get("operationName") or [None])[0]
            ext_val = (parsed.get("extensions") or [None])[0]
            if not op or not ext_val:
                body = json.loads(req.post_data or "")
                if not isinstance(body, dict):
                    return
                op = body.get("operationName")
                extensions = body.get("extensions") or {}
            else:
                extensions = json.loads(ext_val)
        except Exception:
            return
        if not op or not isinstance(extensions, dict):
            return
        h = extensions.get("persistedQuery") or {}
        sha = h.get("sha256Hash")
        if sha:
            app.state.persisted_hashes[op] = sha

    async def on_response(resp):
        nonlocal token, expire
        capture_request(resp.request)
        if "open.spotify.com/api/token" not in resp.url:
            return
        try:
            j = await resp.json()
        except Exception:
            return
        t = j.get("accessToken")
        if t:
            token = t
            expire = int(j.get("accessTokenExpirationTimestampMs", 0)) / 1000

    use_cookie = bool(SP_DC) and not force_anonymous
    page, ctx = await new_page_with_cookies(browser, use_cookie=use_cookie)
    page.on("response", on_response)
    page.on("request", capture_request)
    try:
        await page.goto("https://open.spotify.com/search/hello", wait_until="domcontentloaded", timeout=30000)
        for _ in range(60):
            if token:
                break
            await asyncio.sleep(0.25)
    except Exception as e:
        print(f"[TOKEN] goto err: {e}")
    finally:
        try:
            await page.close()
        except Exception:
            pass
        if ctx:
            try:
                await ctx.close()
            except Exception:
                pass

    if not token:
        if not force_anonymous and SP_DC:
            print("[TOKEN] retry anonymous")
            return await get_access_token(app, force_anonymous=True)
        raise HTTPException(status_code=502, detail="Spotify access token unavailable – check sp_dc")

    if not force_anonymous:
        app.state.token = token
        app.state.token_expire = expire or (now + 3600)
    return token


async def get_client_token(app: FastAPI) -> str:
    now = time.time()
    if app.state.client_token and now < app.state.client_expire - 60:
        return app.state.client_token
    browser = app.state.browser
    if not browser:
        raise HTTPException(status_code=503, detail="Browser not ready")

    ctoken = None
    exp = 0

    async def on_resp(resp):
        nonlocal ctoken, exp
        if "clienttoken.spotify.com/v1/clienttoken" not in resp.url:
            return
        try:
            j = await resp.json()
            gt = j.get("granted_token") or {}
            t = gt.get("token")
            if t:
                ctoken = t
                exp = now + int(gt.get("expires_after_seconds", 3600))
        except Exception:
            pass

    page, ctx = await new_page_with_cookies(browser, use_cookie=True)
    page.on("response", on_resp)
    try:
        await page.goto("https://open.spotify.com/search/hello", wait_until="domcontentloaded", timeout=30000)
        for _ in range(60):
            if ctoken:
                break
            await asyncio.sleep(0.25)
    finally:
        try:
            await page.close()
        except Exception:
            pass
        if ctx:
            try:
                await ctx.close()
            except Exception:
                pass

    if not ctoken:
        if SP_KEY and len(SP_KEY) > 30:
            ctoken = SP_KEY
            exp = now + 3600
        else:
            raise HTTPException(status_code=502, detail="Client token unavailable")

    app.state.client_token = ctoken
    app.state.client_expire = exp or (now + 3600)
    return ctoken


async def discover_persisted_hash(app: FastAPI, operation_name: str) -> str:
    if operation_name in app.state.persisted_hashes:
        return app.state.persisted_hashes[operation_name]
    browser = app.state.browser
    discovered = None

    def cap(req):
        nonlocal discovered
        if discovered or "api-partner.spotify.com/pathfinder" not in req.url:
            return
        try:
            body = json.loads(req.post_data or "")
        except Exception:
            return
        if body.get("operationName") != operation_name:
            return
        discovered = body.get("extensions", {}).get("persistedQuery", {}).get("sha256Hash")

    for use_cookie in ([False, True] if operation_name == "searchDesktop" and SP_DC else [True, False]):
        page, ctx = await new_page_with_cookies(browser, use_cookie=use_cookie)
        page.on("request", cap)
        try:
            await page.goto("https://open.spotify.com/search/hello", wait_until="domcontentloaded", timeout=30000)
            for _ in range(60):
                if discovered:
                    break
                await asyncio.sleep(0.25)
        except Exception:
            pass
        finally:
            try:
                await page.close()
            except Exception:
                pass
            if ctx:
                try:
                    await ctx.close()
                except Exception:
                    pass
        if discovered:
            break

    if not discovered:
        raise HTTPException(status_code=502, detail=f"Hash for {operation_name} not found")
    app.state.persisted_hashes[operation_name] = discovered
    return discovered


async def spotify_query(app: FastAPI, operation_name: str, variables: dict, sha256_hash: Optional[str] = None):
    http: httpx.AsyncClient = app.state.http
    force_anon = operation_name == "searchDesktop"
    token = await get_access_token(app, force_anonymous=force_anon)
    sha256_hash = sha256_hash or app.state.persisted_hashes.get(operation_name) or await discover_persisted_hash(app, operation_name)
    if not sha256_hash:
        raise HTTPException(status_code=502, detail=f"Hash not found for {operation_name}")

    payload = {"operationName": operation_name, "variables": variables, "extensions": {"persistedQuery": {"version": 1, "sha256Hash": sha256_hash}}}
    headers = {"authorization": f"Bearer {token}", "accept": "application/json", "app-platform": "WebPlayer"}
    try:
        ct = await get_client_token(app)
        if ct:
            headers["client-token"] = ct
    except Exception:
        pass

    resp = await http.post(SPOTIFY_PATHFINDER_URL, json=payload, headers=headers)
    if resp.status_code == 401:
        app.state.token = None
        token = await get_access_token(app, force_anonymous=force_anon)
        headers["authorization"] = f"Bearer {token}"
        resp = await http.post(SPOTIFY_PATHFINDER_URL, json=payload, headers=headers)
    if resp.status_code in (400, 404):
        app.state.token = None
        app.state.persisted_hashes.pop(operation_name, None)
        token = await get_access_token(app, force_anonymous=force_anon)
        new_hash = await discover_persisted_hash(app, operation_name)
        payload["extensions"] = {"persistedQuery": {"version": 1, "sha256Hash": new_hash}}
        headers["authorization"] = f"Bearer {token}"
        resp = await http.post(SPOTIFY_PATHFINDER_URL, json=payload, headers=headers)
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Pathfinder error {resp.status_code}: {resp.text[:500]}")
    return resp.json()


# ---------------------------------------------------------------------------
# Search — sama persis konsepnya dengan api-soundcloud
# ---------------------------------------------------------------------------
def extract_search(data):
    root = data.get("data") or {}
    search = root.get("searchV2") or root.get("search")
    if not search:
        return ([], 0)
    tracks = search.get("tracksV2") or search.get("tracks") or {}
    items = tracks.get("items") or []
    total = tracks.get("totalCount") or 0
    res = []
    for it in items:
        if not isinstance(it, dict):
            continue
        track = (it.get("item") or {}).get("data") or it.get("track") or it.get("data") or it
        if isinstance(track, dict) and str(track.get("uri", "")).startswith("spotify:track:"):
            res.append(track)
    return (res, total)


def find_track_objs(o, depth=0, out=None):
    if out is None:
        out = []
    if depth > 9 or not isinstance(o, (dict, list)):
        return out
    if isinstance(o, dict):
        if o.get("uri", "").startswith("spotify:track:") and "albumOfTrack" in o and "duration" in o:
            out.append(o)
        for v in o.values():
            find_track_objs(v, depth + 1, out)
    else:
        for x in o:
            find_track_objs(x, depth + 1, out)
    return out


def get_cache(query: str):
    cache = SEARCH_CACHE.get(query)
    if not cache:
        return None
    if time.time() - cache["updated_at"] > CACHE_TTL:
        SEARCH_CACHE.pop(query, None)
        return None
    return cache


def create_cache(query: str):
    cache = {"tracks": [], "seen": set(), "continuation_offset": 0, "total_results": 0, "has_more": True, "updated_at": time.time()}
    SEARCH_CACHE[query] = cache
    return cache


async def fetch_next_spotify_page(app, query, cache, fetch_limit=50) -> bool:
    if not cache["has_more"]:
        return False
    data = await spotify_query(app, operation_name="searchDesktop", variables={"searchTerm": query, "offset": cache["continuation_offset"], "limit": fetch_limit, "numberOfTopResults": fetch_limit, "includeAudiobooks": False, "includeAuthors": False, "includePreReleases": False})
    items, total = extract_search(data)
    cache["total_results"] = total
    added = 0
    for t in items:
        tid = t.get("id")
        if not tid or tid in cache["seen"]:
            continue
        cache["seen"].add(tid)
        cache["tracks"].append(map_track(t))
        added += 1
    cache["continuation_offset"] += len(items)
    cache["updated_at"] = time.time()
    if len(items) < fetch_limit or (total and cache["continuation_offset"] >= total):
        cache["has_more"] = False
    return added > 0


async def fetch_search_page(app, query, page, limit):
    query = query.strip()
    if not query:
        return {"data": [], "page": page, "limit": limit, "total": 0, "hasNext": False}
    async with CACHE_LOCK:
        cache = get_cache(query) or create_cache(query)
        start = (page - 1) * limit
        end = start + limit
        fetched = 0
        while len(cache["tracks"]) < end and cache["has_more"] and fetched < 25:
            fetched += 1
            if not await fetch_next_spotify_page(app, query, cache):
                break
        data = cache["tracks"][start:end]
        has_next = len(cache["tracks"]) > end or cache["has_more"]
        return {"data": data, "page": page, "limit": limit, "total": len(cache["tracks"]), "totalResults": cache.get("total_results", 0), "hasNext": has_next}


async def get_track_metadata(app, track_id: str):
    """Ambil metadata track — 3 langkah: SEARCH_CACHE → oEmbed → pathfinder. Simple & cepat."""
    tid = sanitize_track_id(track_id)
    now = time.time()
    cached = TRACK_META_CACHE.get(tid)
    if cached and now < cached["expiresAt"]:
        return cached["data"]

    # 1. Cek SEARCH_CACHE dulu (paling cepat & akurat, karena dari search yang sama)
    try:
        for c in list(SEARCH_CACHE.values()):
            for t in c.get("tracks", []):
                if t.get("trackId") == tid:
                    TRACK_META_CACHE[tid] = {"data": t, "expiresAt": now + TRACK_META_TTL}
                    return t
    except Exception:
        pass

    # 2. oEmbed (150ms) + pathfinder cepat (3s) — kalau thumbnail ada, coba enrich dulu
    oembed_thumb = None
    oembed_title = "Unknown"
    oembed_artist = ""
    try:
        async with httpx.AsyncClient(timeout=4.0) as tmp:
            ro = await tmp.get(f"https://open.spotify.com/oembed?url=https://open.spotify.com/track/{tid}", headers={"User-Agent": UA})
            if ro.status_code == 200:
                oj = ro.json()
                oembed_thumb = oj.get("thumbnail_url")
                title = oj.get("title") or "Unknown"
                if " - " in title:
                    parts = title.split(" - ")
                    title = parts[0]
                    oembed_artist = " - ".join(parts[1:])
                elif oj.get("author_name"):
                    oembed_artist = oj.get("author_name")
                oembed_title = title
    except Exception:
        pass

    if oembed_thumb:
        # coba pathfinder cepat untuk duration akurat
        try:
            data_short = await asyncio.wait_for(spotify_query(app, operation_name="getTrack", variables={"uri": f"spotify:track:{tid}", "includeVideoAssociationItems": False}), timeout=3.0)
            objs_short = find_track_objs(data_short.get("data"))
            found_short = None
            root_short = data_short.get("data")
            for d in objs_short:
                if d.get("id") == tid:
                    found_short = d
                    break
            if not found_short:
                tu = (root_short or {}).get("trackUnion") or {}
                if tu.get("id") == tid:
                    found_short = tu
            if found_short:
                if not found_short.get("artists"):
                    fa = (root_short.get("trackUnion") or {}).get("firstArtist") or (found_short.get("firstArtist") or {})
                    if isinstance(fa, dict) and fa.get("items"):
                        found_short = dict(found_short)
                        found_short["artists"] = {"items": fa.get("items")}
                meta_short = map_track(found_short)
                if not meta_short.get("thumbnail") and oembed_thumb:
                    meta_short["thumbnail"] = oembed_thumb
                TRACK_META_CACHE[tid] = {"data": meta_short, "expiresAt": time.time() + 600}
                return meta_short
        except Exception:
            pass

        # fallback oEmbed (duration 0, akan di-enrich background)
        oembed_meta = {
            "title": oembed_title,
            "trackId": tid,
            "link": f"https://open.spotify.com/track/{tid}",
            "thumbnail": oembed_thumb,
            "artist": oembed_artist,
            "artistList": [oembed_artist] if oembed_artist else [],
            "album": None,
            "albumUrl": None,
            "duration": "0:00",
            "durationMs": 0,
            "explicit": False,
            "type": "track",
        }
        TRACK_META_CACHE[tid] = {"data": oembed_meta, "expiresAt": now + 30}

        async def bg_enrich():
            try:
                data = await asyncio.wait_for(spotify_query(app, operation_name="getTrack", variables={"uri": f"spotify:track:{tid}", "includeVideoAssociationItems": False}), timeout=8.0)
                objs = find_track_objs(data.get("data"))
                found = None
                root = data.get("data")
                for d in objs:
                    if d.get("id") == tid:
                        found = d
                        break
                if not found:
                    tu = (root or {}).get("trackUnion") or {}
                    if tu.get("id") == tid:
                        found = tu
                if found:
                    if not found.get("artists"):
                        fa = (root.get("trackUnion") or {}).get("firstArtist") or (found.get("firstArtist") or {})
                        if isinstance(fa, dict) and fa.get("items"):
                            found = dict(found)
                            found["artists"] = {"items": fa.get("items")}
                    meta = map_track(found)
                    if not meta.get("thumbnail") and oembed_thumb:
                        meta["thumbnail"] = oembed_thumb
                    TRACK_META_CACHE[tid] = {"data": meta, "expiresAt": time.time() + 600}
            except Exception:
                pass

        try:
            asyncio.create_task(bg_enrich())
        except Exception:
            pass
        return oembed_meta

    # 3. Fallback pathfinder langsung (kalau oEmbed gagal)
    try:
        data = await asyncio.wait_for(spotify_query(app, operation_name="getTrack", variables={"uri": f"spotify:track:{tid}", "includeVideoAssociationItems": False}), timeout=6.0)
        objs = find_track_objs(data.get("data"))
        found = None
        root = data.get("data")
        for d in objs:
            if d.get("id") == tid:
                found = d
                break
        if not found:
            tu = (root or {}).get("trackUnion") or {}
            if tu.get("id") == tid:
                found = tu
        if found:
            if not found.get("artists"):
                fa = (root.get("trackUnion") or {}).get("firstArtist") or (found.get("firstArtist") or {})
                if isinstance(fa, dict) and fa.get("items"):
                    found = dict(found)
                    found["artists"] = {"items": fa.get("items")}
            meta = map_track(found)
            TRACK_META_CACHE[tid] = {"data": meta, "expiresAt": time.time() + 600}
            return meta
    except Exception:
        pass

    return None


# ---------------------------------------------------------------------------
# Routes — disamakan dengan api-soundcloud: /search, /track, /player, /cache, /
# + streaming: /embed-proxy (cara streaming play dipertahankan)
# ---------------------------------------------------------------------------
@app.get("/search")
async def search(q: str = Query(..., description="Search query"), page: int = Query(1, ge=1), limit: int = Query(20, ge=1, le=MAX_LIMIT), offset: Optional[int] = Query(None, ge=0)):
    if offset is not None:
        page = (offset // limit) + 1
    return await fetch_search_page(app, q, page, limit)


@app.get("/track")
async def track_ep(trackId: str = Query(..., description="Spotify track ID")):
    tid = sanitize_track_id(trackId)
    if not tid:
        raise HTTPException(status_code=400, detail="Invalid trackId")
    meta = await get_track_metadata(app, tid)
    if not meta:
        raise HTTPException(status_code=404, detail="Track not found")
    return meta


@app.get("/embed-proxy")
async def embed_proxy(trackId: str = Query(..., description="Spotify track ID")):
    """Streaming play — dipertahankan, sama seperti sebelumnya. Proxy embed Spotify agar bisa full duration."""
    tid = sanitize_track_id(trackId)
    async with EMBED_LOCK:
        cached = EMBED_CACHE.get(tid)
        if cached and time.time() < cached["expiresAt"]:
            return HTMLResponse(content=cached["html"], headers=cached["headers"])

    target = f"https://open.spotify.com/embed/track/{tid}?utm_source=generator&theme=0"
    headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://open.spotify.com/",
    }
    cookies = {"sp_dc": SP_DC} if SP_DC else {}
    async with httpx.AsyncClient(follow_redirects=True, timeout=10) as c:
        r = await c.get(target, headers=headers, cookies=cookies)
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Embed fetch {r.status_code}")
        html = r.text
        if "</head>" in html:
            html = html.replace("</head>", '<style>[data-testid="embed-widget-container"]{opacity:1 !important} [data-testid="embed-widget-skeleton"],[data-testid="skeleton"]{display:none !important}</style></head>', 1)
        hdrs = {"X-Frame-Options": "ALLOWALL", "Content-Security-Policy": "frame-ancestors *", "Access-Control-Allow-Origin": "*", "Cache-Control": "public, max-age=300"}
        async with EMBED_LOCK:
            EMBED_CACHE[tid] = {"html": html, "headers": hdrs, "expiresAt": time.time() + EMBED_CACHE_TTL}
        return HTMLResponse(content=html, headers=hdrs)


@app.get("/player", response_class=FileResponse)
async def player(trackId: str = Query(..., description="Spotify track ID")):
    return FileResponse("player.html", media_type="text/html")


@app.delete("/cache")
async def clear_cache():
    async with CACHE_LOCK:
        SEARCH_CACHE.clear()
    async with EMBED_LOCK:
        EMBED_CACHE.clear()
    TRACK_META_CACHE.clear()
    return {"success": True, "message": "All caches cleared"}


@app.delete("/cache/{query}")
async def clear_query_cache(query: str):
    async with CACHE_LOCK:
        existed = query in SEARCH_CACHE
        SEARCH_CACHE.pop(query, None)
    return {"success": True, "query": query, "removed": existed}


@app.get("/")
async def root():
    return {
        "message": "Spotify Search API with Pagination",
        "usage": {
            "search": "/search?q=QUERY&page=1&limit=20",
            "track": "/track?trackId=TRACK_ID",
            "player": "/player?trackId=TRACK_ID",
            "embed-proxy": "/embed-proxy?trackId=TRACK_ID  (streaming play)",
        },
        "pagination": {"page": 1, "limit": 20, "max_limit": MAX_LIMIT},
        "cache": {"enabled": True, "ttl_seconds": CACHE_TTL},
        "streaming": "embed-proxy untuk full-duration playback (sp_dc)",
        "auth": {"spDcConfigured": bool(SP_DC)},
    }
