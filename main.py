import asyncio
import json
import time
from contextlib import asynccontextmanager
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

import httpx
from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from playwright.async_api import async_playwright


SPOTIFY_TOKEN_URL = "https://open.spotify.com/get_access_token"
SPOTIFY_PATHFINDER_URL = "https://api-partner.spotify.com/pathfinder/v2/query"

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.http = httpx.AsyncClient(
        timeout=httpx.Timeout(
            connect=5.0,
            read=10.0,
            write=10.0,
            pool=5.0,
        ),
        limits=httpx.Limits(
            max_connections=100,
            max_keepalive_connections=30,
        ),
        http2=True,
        headers={
            "accept": "application/json",
            "accept-language": "en-US,en;q=0.9",
            "app-platform": "WebPlayer",
            "origin": "https://open.spotify.com",
            "referer": "https://open.spotify.com/",
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/139.0.0.0 Safari/537.36"
            ),
        },
    )

    # Spotify memblokir pengambilan token dari server (403 URL Blocked),
    # jadi token di-mint lewat browser sekali saja lalu di-cache.
    async with async_playwright() as pw:
        app.state.browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        app.state.token = None
        app.state.token_expire = 0
        app.state.persisted_hashes = {}
        yield
        await app.state.http.aclose()
        await app.state.browser.close()


app = FastAPI(
    title="Spotify Search API",
    description="Fast Spotify search with pagination and caching",
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
# Pagination + Cache (sesuai konsep api-soundcloud)
# ---------------------------------------------------------------------------
SEARCH_CACHE: dict = {}
CACHE_TTL = 300
CACHE_LOCK = asyncio.Lock()
MAX_LIMIT = 50
FETCH_LIMIT = 50
MAX_FETCH_PAGES = 25


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

    return max(
        sources,
        key=lambda x: x.get("width") or 0
    ).get("url")


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

        name = (
            (artist.get("profile") or {}).get("name")
            or artist.get("name")
        )

        if name:
            result.append(name)

    return result


def extract_thumbnail(album):
    album = album or {}

    sources = (
        album.get("coverArt", {})
        .get("sources", [])
    )

    return best_image(sources)


def map_track(d: dict[str, Any]) -> dict[str, Any]:
    track_id = d.get("id")

    artists = extract_artists(
        d.get("artists")
    )

    album = (
        d.get("albumOfTrack")
        or d.get("album")
        or {}
    )

    album_uri = album.get("uri")

    duration_ms = (
        (d.get("duration") or {}).get("totalMilliseconds")
        or d.get("duration_ms")
        or 0
    )

    explicit = (
        (d.get("contentRating") or {}).get("label")
        == "EXPLICIT"
    )

    album_id = None

    if album_uri and ":" in album_uri:
        album_id = album_uri.split(":")[-1]

    return {
        "title": d.get("name"),
        "trackId": track_id,
        "uri": d.get("uri"),
        "link": (
            f"https://open.spotify.com/track/{track_id}"
            if track_id
            else None
        ),
        "thumbnail": extract_thumbnail(album),
        "artist": ", ".join(artists),
        "artistList": artists,
        "album": album.get("name"),
        "albumUrl": (
            f"https://open.spotify.com/album/{album_id}"
            if album_id
            else None
        ),
        "duration": format_duration(duration_ms),
        "durationMs": duration_ms,
        "explicit": explicit,
        "popularity": None,
        "previewUrl": None,
        "releaseDate": None,
        "type": "track",
    }


async def get_access_token(app: FastAPI) -> str:
    now = time.time()

    # Reuse token selama masih valid
    if (
        app.state.token
        and now < app.state.token_expire - 30
    ):
        return app.state.token

    browser = app.state.browser
    if not browser:
        raise HTTPException(
            status_code=503,
            detail="Browser tidak siap",
        )

    token = None
    expire = 0

    async def on_response(response):
        nonlocal token, expire
        capture_request(response.request)
        if "open.spotify.com/api/token" not in response.url:
            return
        try:
            j = await response.json()
        except Exception:
            return
        token = j.get("accessToken")
        expire = int(j.get("accessTokenExpirationTimestampMs", 0)) / 1000

    def capture_request(request):
        if "api-partner.spotify.com/pathfinder" not in request.url:
            return

        try:
            parsed = parse_qs(urlparse(request.url).query)
            operation_name = (parsed.get("operationName") or [None])[0]
            extensions_value = (parsed.get("extensions") or [None])[0]

            if not operation_name or not extensions_value:
                body = json.loads(request.post_data or "")
                if not isinstance(body, dict):
                    return
                operation_name = body.get("operationName")
                extensions = body.get("extensions") or {}
            else:
                extensions = json.loads(extensions_value)
        except Exception:
            return

        if not operation_name or not isinstance(extensions, dict):
            return

        persisted_query = extensions.get("persistedQuery") or {}
        sha256_hash = persisted_query.get("sha256Hash")

        if sha256_hash:
            app.state.persisted_hashes[operation_name] = sha256_hash

    for url in ("https://open.spotify.com/search/hello",):
        page = await browser.new_page()
        page.on("response", on_response)
        page.on("request", capture_request)
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            for _ in range(60):
                if token:
                    break
                await asyncio.sleep(0.25)
        except Exception:
            pass
        finally:
            await page.close()
        if token:
            break

    if not token:
        raise HTTPException(
            status_code=502,
            detail="Spotify access token tidak tersedia (browser)",
        )

    app.state.token = token
    app.state.token_expire = expire or (now + 300)
    return token


async def discover_persisted_hash(
    app: FastAPI,
    operation_name: str,
) -> str:
    browser = app.state.browser
    discovered_hash = None

    def capture_request(request):
        nonlocal discovered_hash

        if (
            discovered_hash
            or "api-partner.spotify.com/pathfinder" not in request.url
        ):
            return

        try:
            body = json.loads(request.post_data or "")
        except Exception:
            return

        if body.get("operationName") != operation_name:
            return

        discovered_hash = (
            body.get("extensions", {})
            .get("persistedQuery", {})
            .get("sha256Hash")
        )

    page = await browser.new_page()
    page.on("request", capture_request)
    try:
        await page.goto(
            "https://open.spotify.com/search/hello",
            wait_until="domcontentloaded",
            timeout=30000,
        )
        for _ in range(60):
            if discovered_hash:
                break
            await asyncio.sleep(0.25)
    except Exception:
        pass
    finally:
        await page.close()

    if not discovered_hash:
        raise HTTPException(
            status_code=502,
            detail=f"Hash Spotify untuk operasi {operation_name} tidak ditemukan",
        )

    app.state.persisted_hashes[operation_name] = discovered_hash
    return discovered_hash


async def spotify_query(
    app: FastAPI,
    operation_name: str,
    variables: dict,
    sha256_hash: Optional[str] = None,
):
    http: httpx.AsyncClient = app.state.http

    token = await get_access_token(app)

    sha256_hash = (
        sha256_hash
        or app.state.persisted_hashes.get(operation_name)
        or await discover_persisted_hash(app, operation_name)
    )
    if not sha256_hash:
        raise HTTPException(
            status_code=502,
            detail=f"Hash Spotify untuk operasi {operation_name} tidak ditemukan",
        )

    payload = {
        "operationName": operation_name,
        "variables": variables,
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": sha256_hash,
            }
        },
    }

    headers = {
        "authorization": f"Bearer {token}",
        "accept": "application/json",
        "app-platform": "WebPlayer",
    }

    response = await http.post(
        SPOTIFY_PATHFINDER_URL,
        json=payload,
        headers=headers,
    )

    if response.status_code == 401:
        # Token mungkin expired lebih cepat
        app.state.token = None

        token = await get_access_token(app)

        headers["authorization"] = (
            f"Bearer {token}"
        )

        response = await http.post(
            SPOTIFY_PATHFINDER_URL,
            json=payload,
            headers=headers,
        )

    if response.status_code in (400, 404):
        # Persisted query hash dapat berubah tanpa pemberitahuan.
        app.state.token = None
        app.state.persisted_hashes.pop(operation_name, None)
        token = await get_access_token(app)
        new_hash = await discover_persisted_hash(app, operation_name)

        payload["extensions"] = {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": new_hash,
            }
        }
        headers["authorization"] = f"Bearer {token}"
        response = await http.post(
            SPOTIFY_PATHFINDER_URL,
            json=payload,
            headers=headers,
        )

    if response.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=(
                f"Spotify Pathfinder error "
                f"{response.status_code}: "
                f"{response.text[:500]}"
            ),
        )

    return response.json()


def extract_search(data):
    """Mengembalikan (list_track_dict, total_count)."""
    root = data.get("data") or {}

    # Mendukung searchV2 maupun search (hash lama)
    search = root.get("searchV2") or root.get("search")

    if not search:
        return ([], 0)

    tracks = search.get("tracksV2") or search.get("tracks") or {}
    items = tracks.get("items") or []
    total = tracks.get("totalCount") or 0

    result = []

    for item in items:
        if not isinstance(item, dict):
            continue

        track = (
            (item.get("item") or {}).get("data")
            or item.get("track")
            or item.get("data")
            or item
        )

        if isinstance(track, dict) and str(
            track.get("uri", "")
        ).startswith("spotify:track:"):
            result.append(track)

    return (result, total)


def find_track_objs(o, depth=0, out=None):
    if out is None:
        out = []
    if depth > 9 or not isinstance(o, (dict, list)):
        return out
    if isinstance(o, dict):
        if (
            o.get("uri", "").startswith("spotify:track:")
            and "albumOfTrack" in o
            and "duration" in o
        ):
            out.append(o)
        for v in o.values():
            find_track_objs(v, depth + 1, out)
    else:
        for x in o:
            find_track_objs(x, depth + 1, out)
    return out


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------
def get_cache(query: str):
    cache = SEARCH_CACHE.get(query)
    if not cache:
        return None

    now = time.time()
    if now - cache["updated_at"] > CACHE_TTL:
        SEARCH_CACHE.pop(query, None)
        return None

    return cache


def create_cache(query: str):
    cache = {
        "tracks": [],
        "seen": set(),
        "continuation_offset": 0,
        "total_results": 0,
        "has_more": True,
        "updated_at": time.time(),
    }
    SEARCH_CACHE[query] = cache
    return cache


async def fetch_next_spotify_page(
    app: FastAPI,
    query: str,
    cache: dict,
    fetch_limit: int = FETCH_LIMIT,
) -> bool:
    if not cache["has_more"]:
        return False

    data = await spotify_query(
        app,
        operation_name="searchDesktop",
        variables={
            "searchTerm": query,
            "offset": cache["continuation_offset"],
            "limit": fetch_limit,
            "numberOfTopResults": fetch_limit,
            "includeAudiobooks": False,
            "includeAuthors": False,
            "includePreReleases": False,
        },
    )

    items, total = extract_search(data)

    cache["total_results"] = total

    added = 0
    for t in items:
        track_id = t.get("id")
        if not track_id or track_id in cache["seen"]:
            continue
        cache["seen"].add(track_id)
        cache["tracks"].append(map_track(t))
        added += 1

    cache["continuation_offset"] = cache["continuation_offset"] + len(items)
    cache["updated_at"] = time.time()

    if len(items) < fetch_limit or (
        total and cache["continuation_offset"] >= total
    ):
        cache["has_more"] = False

    return added > 0


async def fetch_search_page(
    app: FastAPI,
    query: str,
    page: int,
    limit: int,
) -> dict:
    query = query.strip()

    if not query:
        return {
            "data": [],
            "page": page,
            "limit": limit,
            "total": 0,
            "hasNext": False,
        }

    async with CACHE_LOCK:
        cache = get_cache(query)
        if not cache:
            cache = create_cache(query)

        start_index = (page - 1) * limit
        end_index = start_index + limit

        fetched_pages = 0

        while (
            len(cache["tracks"]) < end_index
            and cache["has_more"]
            and fetched_pages < MAX_FETCH_PAGES
        ):
            fetched_pages += 1
            success = await fetch_next_spotify_page(app, query, cache)
            if not success:
                break

        data = cache["tracks"][start_index:end_index]

        has_next = (
            len(cache["tracks"]) > end_index
            or cache["has_more"]
        )

        return {
            "data": data,
            "page": page,
            "limit": limit,
            "total": len(cache["tracks"]),
            "totalResults": cache.get("total_results", 0),
            "hasNext": has_next,
        }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/search")
async def search(
    request: Request,
    q: str = Query(..., description="Search query"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=MAX_LIMIT, description="Items per page"),
    offset: Optional[int] = Query(None, ge=0, description="Legacy offset pagination"),
):
    if offset is not None:
        page = (offset // limit) + 1

    return await fetch_search_page(request.app, q, page, limit)


@app.get("/track")
async def track(
    request: Request,
    trackId: str = Query(..., description="Spotify track ID or URI"),
):
    tid = trackId
    if tid.startswith("spotify:track:"):
        tid = tid.split(":")[-1]
    elif "open.spotify.com/track/" in tid:
        tid = tid.split("/track/")[-1].split("?")[0]

    browser = request.app.state.browser
    if not browser:
        raise HTTPException(status_code=503, detail="Browser tidak siap")

    page = await browser.new_page()
    found = {}

    async def on_response(response):
        if "api-partner.spotify.com/pathfinder" not in response.url:
            return
        try:
            j = await response.json()
        except Exception:
            return
        for d in find_track_objs(j.get("data")):
            if d.get("id") == tid:
                found["d"] = d
                found["root"] = j.get("data")
                return

    page.on("response", on_response)
    try:
        await page.goto(
            f"https://open.spotify.com/track/{tid}",
            wait_until="commit",
            timeout=30000,
        )
        for _ in range(20):
            if "d" in found:
                break
            await asyncio.sleep(0.5)
    except Exception as e:
        await page.close()
        raise HTTPException(status_code=502, detail=f"Scrape error: {e}")
    finally:
        await page.close()

    if "d" not in found:
        raise HTTPException(status_code=404, detail="Track not found")

    d = found["d"]
    if not d.get("artists"):
        fa = ((found.get("root") or {}).get("trackUnion") or {}).get("firstArtist") or {}
        d = dict(d)
        d["artists"] = {"items": (fa.get("items") or [])}
    return map_track(d)


@app.delete("/cache")
async def clear_cache():
    async with CACHE_LOCK:
        SEARCH_CACHE.clear()
    return {"success": True, "message": "Search cache cleared"}


@app.delete("/cache/{query}")
async def clear_query_cache(query: str):
    async with CACHE_LOCK:
        existed = query in SEARCH_CACHE
        SEARCH_CACHE.pop(query, None)
    return {"success": True, "query": query, "removed": existed}


@app.get(
    "/player",
    response_class=FileResponse,
)
async def player(
    trackId: str = Query(...),
):
    return FileResponse(
        "player.html",
        media_type="text/html",
    )


@app.get("/")
async def root():
    return {
        "message": "Spotify Search API with Pagination",
        "usage": {
            "search": "/search?q=QUERY&page=1&limit=20",
            "track": "/track?trackId=TRACK_ID",
            "player": "/player?trackId=TRACK_ID",
        },
        "pagination": {
            "page": 1,
            "limit": 20,
            "max_limit": MAX_LIMIT,
        },
        "cache": {
            "enabled": True,
            "ttl_seconds": CACHE_TTL,
        },
    }
