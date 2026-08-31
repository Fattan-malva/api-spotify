import asyncio
import json
import time
import os
import re
from contextlib import asynccontextmanager
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

import httpx
from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse, RedirectResponse, HTMLResponse
from playwright.async_api import async_playwright

from dotenv import load_dotenv

load_dotenv()

SP_DC = os.getenv("sp_dc") or os.getenv("SP_DC") or ""
SP_KEY = os.getenv("sp_key") or os.getenv("SP_KEY") or ""

SPOTIFY_PATHFINDER_URL = "https://api-partner.spotify.com/pathfinder/v2/query"

# ---------------------------------------------------------------------------
# Caches
# ---------------------------------------------------------------------------
SEARCH_CACHE: dict = {}
CACHE_TTL = 300
CACHE_LOCK = asyncio.Lock()
MAX_LIMIT = 50
FETCH_LIMIT = 50
MAX_FETCH_PAGES = 25

TRACK_META_CACHE: dict = {}
TRACK_META_TTL = 600

CDN_CACHE: dict = {}
CDN_CACHE_TTL = 3600 * 5
CDN_LOCK = asyncio.Lock()

EMBED_CACHE: dict = {}
EMBED_CACHE_TTL = 600
EMBED_LOCK = asyncio.Lock()


def sanitize_track_id(raw: str) -> str:
    tid = raw.strip()
    if tid.startswith("spotify:track:"):
        tid = tid.split(":")[-1]
    elif "open.spotify.com/track/" in tid:
        tid = tid.split("/track/")[-1].split("?")[0].split("/")[0]
    tid = re.sub(r"[^a-zA-Z0-9]+", "", tid)
    return tid


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.http = httpx.AsyncClient(
        timeout=httpx.Timeout(connect=5.0, read=10.0, write=10.0, pool=5.0),
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=30),
        http2=True,
        headers={
            "accept": "application/json",
            "accept-language": "en-US,en;q=0.9",
            "app-platform": "WebPlayer",
            "origin": "https://open.spotify.com",
            "referer": "https://open.spotify.com/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        },
    )
    app.state.sp_dc = SP_DC
    app.state.sp_key = SP_KEY
    if SP_DC:
        print(f"[INIT] sp_dc loaded: {SP_DC[:12]}... ({len(SP_DC)} chars)")
    else:
        print("[INIT] WARNING: sp_dc not found – anonymous mode")
    if SP_KEY:
        print(f"[INIT] sp_key loaded: {SP_KEY[:8]}...")
    else:
        print("[INIT] sp_key not set – CDN resolve may need client token from browser")

    async with async_playwright() as pw:
        app.state.browser = await pw.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage","--disable-gpu"])
        app.state.token = None
        app.state.token_expire = 0
        app.state.client_token = None
        app.state.client_expire = 0
        app.state.persisted_hashes = {}
        yield
        await app.state.http.aclose()
        await app.state.browser.close()


app = FastAPI(title="Spotify Full-Duration API (sp_dc + sp_key)", description="Play full duration without Spotify Developer API key. Uses sp_dc & sp_key from .env, no youtube fallback.", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------
def format_duration(ms: int | None) -> str:
    if not ms: return "0:00"
    total_sec = ms // 1000
    h, rem = divmod(total_sec, 3600)
    m, s = divmod(rem, 60)
    if h: return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"

def best_image(sources):
    if not sources: return None
    return max(sources, key=lambda x: x.get("width") or 0).get("url")

def extract_artists(value):
    if isinstance(value, dict): items = value.get("items") or []
    elif isinstance(value, list): items = value
    else: items = []
    res=[]
    for a in items:
        if not isinstance(a, dict): continue
        name = (a.get("profile") or {}).get("name") or a.get("name")
        if name: res.append(name)
    return res

def extract_thumbnail(album):
    album = album or {}
    sources = album.get("coverArt", {}).get("sources", [])
    return best_image(sources)

def map_track(d: dict[str, Any]) -> dict[str, Any]:
    track_id = d.get("id")
    artists = extract_artists(d.get("artists"))
    album = d.get("albumOfTrack") or d.get("album") or {}
    album_uri = album.get("uri")
    duration_ms = (d.get("duration") or {}).get("totalMilliseconds") or d.get("duration_ms") or 0
    explicit = (d.get("contentRating") or {}).get("label") == "EXPLICIT"
    album_id = None
    if album_uri and ":" in album_uri: album_id = album_uri.split(":")[-1]
    return {
        "title": d.get("name"),
        "trackId": track_id,
        "uri": d.get("uri"),
        "link": f"https://open.spotify.com/track/{track_id}" if track_id else None,
        "thumbnail": extract_thumbnail(album),
        "artist": ", ".join(artists),
        "artistList": artists,
        "album": album.get("name"),
        "albumUrl": f"https://open.spotify.com/album/{album_id}" if album_id else None,
        "duration": format_duration(duration_ms),
        "durationMs": duration_ms,
        "explicit": explicit,
        "popularity": None,
        "previewUrl": None,
        "releaseDate": None,
        "type": "track",
    }

async def new_page_with_cookies(browser, use_cookie: bool = True):
    if SP_DC and use_cookie:
        ctx = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36", locale="en-US")
        try:
            await ctx.add_cookies([
                {"name":"sp_dc","value":SP_DC,"domain":".spotify.com","path":"/","httpOnly":False,"secure":True,"sameSite":"Lax"},
                {"name":"sp_dc","value":SP_DC,"domain":"open.spotify.com","path":"/","httpOnly":False,"secure":True,"sameSite":"Lax"},
            ])
        except Exception as e:
            print(f"[COOKIE] add failed: {e}")
        page = await ctx.new_page()
        return page, ctx
    else:
        ctx = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36", locale="en-US")
        page = await ctx.new_page()
        return page, ctx

async def get_access_token(app: FastAPI, force_anonymous: bool = False) -> str:
    now=time.time()
    # reuse if not forced anonymous and still valid
    if not force_anonymous and app.state.token and now < app.state.token_expire - 30:
        return app.state.token
    browser=app.state.browser
    if not browser: raise HTTPException(status_code=503, detail="Browser not ready")
    token=None; expire=0
    async def on_response(resp):
        nonlocal token, expire
        capture_request(resp.request)
        if "open.spotify.com/api/token" not in resp.url: return
        try:
            j=await resp.json()
        except: return
        t=j.get("accessToken")
        if t:
            token=t
            expire=int(j.get("accessTokenExpirationTimestampMs",0))/1000
    def capture_request(req):
        if "api-partner.spotify.com/pathfinder" not in req.url: return
        try:
            parsed=parse_qs(urlparse(req.url).query)
            op=(parsed.get("operationName") or [None])[0]
            ext_val=(parsed.get("extensions") or [None])[0]
            if not op or not ext_val:
                body=json.loads(req.post_data or "")
                if not isinstance(body, dict): return
                op=body.get("operationName"); extensions=body.get("extensions") or {}
            else:
                extensions=json.loads(ext_val)
        except: return
        if not op or not isinstance(extensions, dict): return
        h=extensions.get("persistedQuery") or {}
        sha=h.get("sha256Hash")
        if sha: app.state.persisted_hashes[op]=sha
    use_cookie = bool(SP_DC) and not force_anonymous
    for url in ("https://open.spotify.com/search/hello",):
        page, ctx = await new_page_with_cookies(browser, use_cookie=use_cookie)
        page.on("response", on_response)
        page.on("request", capture_request)
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            for _ in range(60):
                if token: break
                await asyncio.sleep(0.25)
        except Exception as e:
            print(f"[TOKEN] goto err: {e}")
        finally:
            try: await page.close()
            except: pass
            if ctx: 
                try: await ctx.close()
                except: pass
        if token: break
    if not token:
        if not force_anonymous and SP_DC:
            # retry anonymous
            print("[TOKEN] retry anonymous after sp_dc failed")
            return await get_access_token(app, force_anonymous=True)
        raise HTTPException(status_code=502, detail="Spotify access token unavailable – check sp_dc")
    if not force_anonymous:
        app.state.token=token
        app.state.token_expire=expire or (now+3600)
    return token

async def get_client_token(app: FastAPI) -> str:
    now=time.time()
    if app.state.client_token and now < app.state.client_expire - 60:
        return app.state.client_token
    browser=app.state.browser
    if not browser: raise HTTPException(status_code=503, detail="Browser not ready")
    ctoken=None; exp=0
    async def on_resp(resp):
        nonlocal ctoken, exp
        if "clienttoken.spotify.com/v1/clienttoken" not in resp.url: return
        try:
            j=await resp.json()
            gt=j.get("granted_token") or {}
            t=gt.get("token")
            if t:
                ctoken=t
                exp=now + int(gt.get("expires_after_seconds", 3600))
        except: pass
    page, ctx = await new_page_with_cookies(browser, use_cookie=True)
    page.on("response", on_resp)
    try:
        await page.goto("https://open.spotify.com/search/hello", wait_until="domcontentloaded", timeout=30000)
        for _ in range(60):
            if ctoken: break
            await asyncio.sleep(0.25)
    finally:
        try: await page.close()
        except: pass
        if ctx:
            try: await ctx.close()
            except: pass
    if not ctoken:
        # fallback to SP_KEY if provided (may be raw token)
        if SP_KEY and len(SP_KEY) > 30:
            ctoken=SP_KEY
            exp=now+3600
        else:
            raise HTTPException(status_code=502, detail="Client token unavailable")
    app.state.client_token=ctoken
    app.state.client_expire=exp or (now+3600)
    return ctoken

async def discover_persisted_hash(app: FastAPI, operation_name: str) -> str:
    browser=app.state.browser
    # if hash already known, return
    if operation_name in app.state.persisted_hashes:
        return app.state.persisted_hashes[operation_name]
    discovered=None
    def cap(req):
        nonlocal discovered
        if discovered or "api-partner.spotify.com/pathfinder" not in req.url: return
        try:
            body=json.loads(req.post_data or "")
        except: return
        if body.get("operationName")!=operation_name: return
        discovered=body.get("extensions",{}).get("persistedQuery",{}).get("sha256Hash")
    # try anonymous first for searchDesktop (since authenticated gives different ops)
    for use_cookie in ([False, True] if operation_name=="searchDesktop" and SP_DC else [True, False]):
        page, ctx = await new_page_with_cookies(browser, use_cookie=use_cookie)
        page.on("request", cap)
        try:
            await page.goto("https://open.spotify.com/search/hello", wait_until="domcontentloaded", timeout=30000)
            for _ in range(60):
                if discovered: break
                await asyncio.sleep(0.25)
        except: pass
        finally:
            try: await page.close()
            except: pass
            if ctx:
                try: await ctx.close()
                except: pass
        if discovered: break
    if not discovered:
        raise HTTPException(status_code=502, detail=f"Hash for {operation_name} not found")
    app.state.persisted_hashes[operation_name]=discovered
    return discovered

async def spotify_query(app: FastAPI, operation_name: str, variables: dict, sha256_hash: Optional[str]=None):
    http: httpx.AsyncClient = app.state.http
    # searchDesktop must use anonymous token to get correct hash
    force_anon = (operation_name=="searchDesktop")
    token = await get_access_token(app, force_anonymous=force_anon)
    sha256_hash = sha256_hash or app.state.persisted_hashes.get(operation_name) or await discover_persisted_hash(app, operation_name)
    if not sha256_hash: raise HTTPException(status_code=502, detail=f"Hash not found for {operation_name}")
    payload={"operationName":operation_name,"variables":variables,"extensions":{"persistedQuery":{"version":1,"sha256Hash":sha256_hash}}}
    headers={"authorization":f"Bearer {token}","accept":"application/json","app-platform":"WebPlayer"}
    # try with client token if available
    try:
        ct=await get_client_token(app)
        if ct: headers["client-token"]=ct
    except: pass
    resp=await http.post(SPOTIFY_PATHFINDER_URL, json=payload, headers=headers)
    if resp.status_code==401:
        app.state.token=None
        token=await get_access_token(app, force_anonymous=force_anon)
        headers["authorization"]=f"Bearer {token}"
        resp=await http.post(SPOTIFY_PATHFINDER_URL, json=payload, headers=headers)
    if resp.status_code in (400,404):
        app.state.token=None
        app.state.persisted_hashes.pop(operation_name,None)
        token=await get_access_token(app, force_anonymous=force_anon)
        new_hash=await discover_persisted_hash(app, operation_name)
        payload["extensions"]={"persistedQuery":{"version":1,"sha256Hash":new_hash}}
        headers["authorization"]=f"Bearer {token}"
        resp=await http.post(SPOTIFY_PATHFINDER_URL, json=payload, headers=headers)
    if resp.status_code!=200:
        raise HTTPException(status_code=502, detail=f"Pathfinder error {resp.status_code}: {resp.text[:500]}")
    return resp.json()

def extract_search(data):
    root=data.get("data") or {}
    search=root.get("searchV2") or root.get("search")
    if not search: return ([],0)
    tracks=search.get("tracksV2") or search.get("tracks") or {}
    items=tracks.get("items") or []
    total=tracks.get("totalCount") or 0
    res=[]
    for it in items:
        if not isinstance(it, dict): continue
        track=(it.get("item") or {}).get("data") or it.get("track") or it.get("data") or it
        if isinstance(track, dict) and str(track.get("uri","")).startswith("spotify:track:"): res.append(track)
    return (res,total)

def find_track_objs(o, depth=0, out=None):
    if out is None: out=[]
    if depth>9 or not isinstance(o,(dict,list)): return out
    if isinstance(o, dict):
        if o.get("uri","").startswith("spotify:track:") and "albumOfTrack" in o and "duration" in o: out.append(o)
        for v in o.values(): find_track_objs(v, depth+1, out)
    else:
        for x in o: find_track_objs(x, depth+1, out)
    return out

def get_cache(q):
    c=SEARCH_CACHE.get(q)
    if not c: return None
    if time.time()-c["updated_at"]>CACHE_TTL: SEARCH_CACHE.pop(q,None); return None
    return c

def create_cache(q):
    c={"tracks":[],"seen":set(),"continuation_offset":0,"total_results":0,"has_more":True,"updated_at":time.time()}
    SEARCH_CACHE[q]=c; return c

async def fetch_next_spotify_page(app, query, cache, fetch_limit=FETCH_LIMIT):
    if not cache["has_more"]: return False
    data=await spotify_query(app, operation_name="searchDesktop", variables={"searchTerm":query,"offset":cache["continuation_offset"],"limit":fetch_limit,"numberOfTopResults":fetch_limit,"includeAudiobooks":False,"includeAuthors":False,"includePreReleases":False})
    items,total=extract_search(data)
    cache["total_results"]=total
    added=0
    for t in items:
        tid=t.get("id")
        if not tid or tid in cache["seen"]: continue
        cache["seen"].add(tid); cache["tracks"].append(map_track(t)); added+=1
    cache["continuation_offset"]+=len(items)
    cache["updated_at"]=time.time()
    if len(items)<fetch_limit or (total and cache["continuation_offset"]>=total): cache["has_more"]=False
    return added>0

async def fetch_search_page(app, query, page, limit):
    query=query.strip()
    if not query: return {"data":[],"page":page,"limit":limit,"total":0,"hasNext":False}
    async with CACHE_LOCK:
        cache=get_cache(query) or create_cache(query)
        start=(page-1)*limit; end=start+limit; fetched=0
        while len(cache["tracks"])<end and cache["has_more"] and fetched<MAX_FETCH_PAGES:
            fetched+=1
            if not await fetch_next_spotify_page(app, query, cache): break
        data=cache["tracks"][start:end]
        has_next=len(cache["tracks"])>end or cache["has_more"]
        return {"data":data,"page":page,"limit":limit,"total":len(cache["tracks"]),"totalResults":cache.get("total_results",0),"hasNext":has_next}

async def get_track_metadata(app, track_id: str):
    tid=sanitize_track_id(track_id)
    now=time.time()
    cached=TRACK_META_CACHE.get(tid)
    if cached and now < cached["expiresAt"]: return cached["data"]
    # Fastest: oEmbed only for thumbnail, no blocking pathfinder
    # Returns in ~150ms, background enrich will fill duration later
    oembed_thumb=None
    oembed_title="Unknown"
    oembed_artist=""
    try:
        async with httpx.AsyncClient(timeout=4.0) as tmp:
            ro=await tmp.get(f"https://open.spotify.com/oembed?url=https://open.spotify.com/track/{tid}", headers={"User-Agent":"Mozilla/5.0"})
            if ro.status_code==200:
                oj=ro.json()
                oembed_thumb=oj.get("thumbnail_url")
                title=oj.get("title") or "Unknown"
                artist=""
                if " - " in title:
                    parts=title.split(" - ")
                    title=parts[0]
                    artist=" - ".join(parts[1:])
                elif oj.get("author_name"):
                    artist=oj.get("author_name")
                oembed_title=title
                oembed_artist=artist
    except Exception as e:
        print(f"[META] oEmbed err {e}")
    # If we have thumbnail, return immediately and enrich duration in background
    if oembed_thumb:
        oembed_meta={
            "title": oembed_title,
            "trackId": tid,
            "uri": f"spotify:track:{tid}",
            "link": f"https://open.spotify.com/track/{tid}",
            "thumbnail": oembed_thumb,
            "artist": oembed_artist,
            "artistList": [oembed_artist] if oembed_artist else [],
            "album": None,
            "albumUrl": None,
            "duration": "0:00",
            "durationMs": 0,
            "explicit": False,
            "popularity": None,
            "previewUrl": None,
            "releaseDate": None,
            "type": "track",
        }
        TRACK_META_CACHE[tid]={"data":oembed_meta,"expiresAt":now+30}
        # Background enrich with accurate duration via pathfinder (don't block response)
        async def bg_enrich():
            try:
                import asyncio as _aio
                data=await _aio.wait_for(spotify_query(app, operation_name="getTrack", variables={"uri":f"spotify:track:{tid}","includeVideoAssociationItems":False}), timeout=8.0)
                objs=find_track_objs(data.get("data"))
                found=None
                root=data.get("data")
                for d in objs:
                    if d.get("id")==tid:
                        found=d; break
                if not found:
                    tu=(root or {}).get("trackUnion") or {}
                    if tu.get("id")==tid: found=tu
                if found:
                    if not found.get("artists"):
                        fa=(root.get("trackUnion") or {}).get("firstArtist") or (found.get("firstArtist") or {})
                        if isinstance(fa, dict) and fa.get("items"):
                            found=dict(found)
                            found["artists"]={"items":fa.get("items")}
                    meta=map_track(found)
                    if not meta.get("thumbnail") and oembed_thumb:
                        meta["thumbnail"]=oembed_thumb
                    TRACK_META_CACHE[tid]={"data":meta,"expiresAt":time.time()+600}
                    print(f"[META] bg enrich done for {tid} duration {meta.get('duration')}")
            except Exception as e:
                print(f"[META] bg enrich err {e} for {tid}")
        try:
            import asyncio as _asyncio
            _asyncio.create_task(bg_enrich())
        except: pass
        return oembed_meta
    # Fallback if oEmbed failed: try pathfinder directly
    try:
        import asyncio as _aio2
        data=await _aio2.wait_for(spotify_query(app, operation_name="getTrack", variables={"uri":f"spotify:track:{tid}","includeVideoAssociationItems":False}), timeout=6.0)
        objs=find_track_objs(data.get("data"))
        found=None
        root=data.get("data")
        for d in objs:
            if d.get("id")==tid:
                found=d; break
        if not found:
            tu=(root or {}).get("trackUnion") or {}
            if tu.get("id")==tid: found=tu
        if found:
            if not found.get("artists"):
                fa=(root.get("trackUnion") or {}).get("firstArtist") or (found.get("firstArtist") or {})
                if isinstance(fa, dict) and fa.get("items"):
                    found=dict(found)
                    found["artists"]={"items":fa.get("items")}
            meta=map_track(found)
            TRACK_META_CACHE[tid]={"data":meta,"expiresAt":time.time()+600}
            return meta
    except Exception as e:
        print(f"[META] pathfinder fallback err {e} for {tid}")
    # Last resort browser
    browser=app.state.browser
    if browser:
        try:
            page, ctx = await new_page_with_cookies(browser, use_cookie=True)
            found={}
            async def on_resp(resp):
                if "api-partner.spotify.com/pathfinder" not in resp.url: return
                try: j=await resp.json()
                except: return
                for d in find_track_objs(j.get("data")):
                    if d.get("id")==tid: found["d"]=d; found["root"]=j.get("data"); return
            page.on("response", on_resp)
            try:
                await page.goto(f"https://open.spotify.com/track/{tid}", wait_until="commit", timeout=12000)
                for _ in range(8):
                    if "d" in found: break
                    await asyncio.sleep(0.3)
            except Exception as e:
                print(f"[META] browser err {e}")
            finally:
                try: await page.close()
                except: pass
                if ctx:
                    try: await ctx.close()
                    except: pass
            if "d" in found:
                d=found["d"]
                if not d.get("artists"):
                    fa=((found.get("root") or {}).get("trackUnion") or {}).get("firstArtist") or {}
                    d=dict(d); d["artists"]={"items":(fa.get("items") or [])}
                meta=map_track(d)
                TRACK_META_CACHE[tid]={"data":meta,"expiresAt":time.time()+600}
                return meta
        except Exception as e:
            print(f"[META] browser exception {e}")
    return None


async def try_spotify_cdn(app, track_id: str):
    tid=sanitize_track_id(track_id)
    async with CDN_LOCK:
        cached=CDN_CACHE.get(tid)
        if cached and time.time() < cached["expiresAt"]:
            return cached["cdnurl"]
    # need tokens
    try:
        access=await get_access_token(app, force_anonymous=False)
        client=await get_client_token(app)
    except Exception as e:
        print(f"[CDN] token err {e}")
        return None
    headers={"Authorization":f"Bearer {access}","Client-Token":client,"client-token":client,"App-Platform":"WebPlayer","Accept":"application/json"}
    http=app.state.http
    # This endpoint is historically 404 for Web API tokens – we try pathfinder getTrack fileIds via alternative
    # First try to get fileId via pathfinder getTrack with extensions
    # The getTrack response already contains file info? Let's inspect getTrack via spotify_query
    try:
        data=await spotify_query(app, operation_name="getTrack", variables={"uri":f"spotify:track:{tid}","includeVideoAssociationItems":False})
        # search for fileId in response
        import re
        txt=json.dumps(data)
        # look for file id patterns like "fileId": or "file_id"
        m=re.search(r'"fileId"\s*:\s*"([^"]+)"', txt)
        if not m: m=re.search(r'"file_id"\s*:\s*"([^"]+)"', txt)
        if m:
            file_id=m.group(1)
            print(f"[CDN] found fileId via getTrack: {file_id}")
            # try storage-resolve
            for host in ["https://gew-spclient.spotify.com","https://spclient.wg.spotify.com"]:
                try:
                    url=f"{host}/storage-resolve/files/audio/interactive/{file_id}?alt=json"
                    r=await http.get(url, headers=headers, timeout=10)
                    if r.status_code==200:
                        j=r.json()
                        cdn=j.get("cdnurl") or j.get("cdn_url")
                        if cdn:
                            cdnurl=cdn[0] if isinstance(cdn, list) else cdn
                            print(f"[CDN] resolved {cdnurl[:80]}")
                            async with CDN_LOCK:
                                CDN_CACHE[tid]={"cdnurl":cdnurl,"expiresAt":time.time()+CDN_CACHE_TTL}
                            return cdnurl
                except Exception as e:
                    print(f"[CDN] storage {host} err {e}")
    except Exception as e:
        print(f"[CDN] getTrack err {e}")
    return None

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/search")
async def search(request: Request, q: str = Query(...), page: int = Query(1, ge=1), limit: int = Query(20, ge=1, le=MAX_LIMIT), offset: Optional[int]=Query(None, ge=0)):
    if offset is not None: page=(offset//limit)+1
    return await fetch_search_page(request.app, q, page, limit)

@app.get("/track")
async def track_ep(request: Request, trackId: str = Query(...)):
    tid=sanitize_track_id(trackId)
    if not tid: raise HTTPException(status_code=400, detail="Invalid trackId")
    meta=await get_track_metadata(request.app, tid)
    if not meta: raise HTTPException(status_code=404, detail="Track not found")
    base=str(request.base_url).rstrip("/")
    meta["streamUrl"]=f"{base}/stream?trackId={tid}"
    meta["proxyUrl"]=f"{base}/proxy?trackId={tid}"
    meta["cdnUrl"]=f"{base}/cdn?trackId={tid}"
    meta["embedProxyUrl"]=f"{base}/embed-proxy?trackId={tid}"
    meta["audioUrl"]=meta["proxyUrl"]
    meta["_spDcConfigured"]=bool(SP_DC)
    return meta

@app.get("/token")
async def token_ep(request: Request):
    try:
        access=await get_access_token(request.app, force_anonymous=False)
        client=await get_client_token(request.app)
        return {"accessToken":access,"clientToken":client,"expiresIn":3600,"spDcConfigured":bool(SP_DC)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

@app.get("/cdn")
async def cdn_ep(request: Request, trackId: str = Query(...)):
    tid=sanitize_track_id(trackId)
    cdn=await try_spotify_cdn(request.app, tid)
    if not cdn:
        raise HTTPException(status_code=404, detail="CDN URL not available – track may be DRM protected, use embed-proxy for full playback")
    return {"trackId":tid,"cdnUrl":cdn,"source":"spotify_cdn","note":"CDN file is Widevine encrypted – browser needs EME. Use /proxy to attempt streaming or /embed-proxy for guaranteed full playback via embed."}

@app.get("/stream")
async def stream_ep(request: Request, trackId: str = Query(...)):
    tid=sanitize_track_id(trackId)
    meta=await get_track_metadata(request.app, tid)
    base=str(request.base_url).rstrip("/")
    # Try CDN first
    cdn=await try_spotify_cdn(request.app, tid)
    if cdn:
        return {
            "trackId":tid,
            "title":meta.get("title") if meta else None,
            "artist":meta.get("artist") if meta else None,
            "thumbnail":meta.get("thumbnail") if meta else None,
            "durationMs":meta.get("durationMs") if meta else None,
            "duration":meta.get("duration") if meta else None,
            "source":"spotify_cdn",
            "audioUrl":f"{base}/proxy?trackId={tid}",
            "proxyUrl":f"{base}/proxy?trackId={tid}",
            "directCdnUrl":cdn,
            "embedProxyUrl":f"{base}/embed-proxy?trackId={tid}",
            "cdnUrl":f"{base}/cdn?trackId={tid}",
            "expiresAt": time.time()+CDN_CACHE_TTL,
            "note":"CDN is DRM-encrypted. For guaranteed full duration without decryption, use embedProxyUrl in iframe."
        }
    # Fallback to embed-proxy as full duration solution (no youtube, no api key, sp_dc authenticated)
    return {
        "trackId":tid,
        "title":meta.get("title") if meta else None,
        "artist":meta.get("artist") if meta else None,
        "thumbnail":meta.get("thumbnail") if meta else None,
        "durationMs":meta.get("durationMs") if meta else None,
        "duration":meta.get("duration") if meta else None,
        "source":"spotify_embed_proxy",
        "audioUrl":f"{base}/embed-proxy?trackId={tid}",
        "embedProxyUrl":f"{base}/embed-proxy?trackId={tid}",
        "proxyUrl":f"{base}/proxy?trackId={tid}",
        "cdnUrl":None,
        "note":"Full duration via authenticated embed proxy (sp_dc). Load this URL in iframe or use player.html."
    }

@app.get("/proxy")
async def proxy_ep(request: Request, trackId: str = Query(...)):
    tid=sanitize_track_id(trackId)
    cdn=await try_spotify_cdn(request.app, tid)
    if not cdn:
        raise HTTPException(status_code=404, detail="CDN not available – use /embed-proxy for full playback")
    headers={"User-Agent":"Mozilla/5.0","Accept":"*/*","Range":request.headers.get("range","")}
    if not headers["Range"]: headers.pop("Range",None)
    # Add auth for CDN?
    # CDN urls are signed and may not need auth, but we forward range
    # We try to proxy
    client=httpx.AsyncClient(follow_redirects=True, timeout=httpx.Timeout(10, read=30))
    try:
        req=client.build_request("GET", cdn, headers=headers)
        resp=await client.send(req, stream=True)
        if resp.status_code not in (200,206):
            await resp.aclose(); await client.aclose()
            raise HTTPException(status_code=502, detail=f"CDN upstream {resp.status_code}")
        resp_headers={k:v for k,v in resp.headers.items() if k.lower() in ["content-type","content-length","content-range","accept-ranges","cache-control"]}
        resp_headers["Access-Control-Allow-Origin"]="*"
        media_type=resp.headers.get("content-type","audio/ogg")
        async def iter_bytes():
            try:
                async for chunk in resp.aiter_bytes(64*1024):
                    yield chunk
            finally:
                await resp.aclose(); await client.aclose()
        return StreamingResponse(iter_bytes(), status_code=resp.status_code, headers=resp_headers, media_type=media_type)
    except HTTPException:
        try: await client.aclose()
        except: pass
        raise
    except Exception as e:
        try: await client.aclose()
        except: pass
        raise HTTPException(status_code=502, detail=f"Proxy err {e}")

@app.get("/embed-proxy")
async def embed_proxy(request: Request, trackId: str = Query(...)):
    tid=sanitize_track_id(trackId)
    # Fast cache — embed HTML rarely changes, and sp_dc auth is stable
    async with EMBED_LOCK:
        cached=EMBED_CACHE.get(tid)
        if cached and time.time() < cached["expiresAt"]:
            return HTMLResponse(content=cached["html"], headers=cached["headers"])
    target=f"https://open.spotify.com/embed/track/{tid}?utm_source=generator&theme=0"
    headers={
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/139.0.0.0 Safari/537.36",
        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language":"en-US,en;q=0.9",
        "Referer":"https://open.spotify.com/",
    }
    cookies={"sp_dc":SP_DC} if SP_DC else {}
    async with httpx.AsyncClient(follow_redirects=True, timeout=10) as c:
        r=await c.get(target, headers=headers, cookies=cookies)
        if r.status_code!=200:
            raise HTTPException(status_code=502, detail=f"Embed fetch {r.status_code}")
        html=r.text
        # Hide Spotify's internal skeleton/loading inside embed and force eager load
        # Inject style to ensure data-testid embed containers render instantly
        if "</head>" in html:
            html=html.replace("</head>", "<style>[data-testid=\"embed-widget-container\"]{opacity:1 !important} [data-testid=\"embed-widget-skeleton\"], [data-testid=\"skeleton\"]{display:none !important}</style></head>",1)
        hdrs={"X-Frame-Options":"ALLOWALL","Content-Security-Policy":"frame-ancestors *","Access-Control-Allow-Origin":"*","Cache-Control":"public, max-age=300"}
        async with EMBED_LOCK:
            EMBED_CACHE[tid]={"html":html,"headers":hdrs,"expiresAt":time.time()+EMBED_CACHE_TTL}
        return HTMLResponse(content=html, headers=hdrs)

@app.get("/audio")
async def audio_alias(request: Request, trackId: str = Query(...)):
    return RedirectResponse(url=f"/proxy?trackId={trackId}", status_code=302)

@app.delete("/cache")
async def clear_cache():
    async with CACHE_LOCK: SEARCH_CACHE.clear()
    async with CDN_LOCK: CDN_CACHE.clear()
    async with EMBED_LOCK: EMBED_CACHE.clear()
    TRACK_META_CACHE.clear()
    return {"success":True,"message":"All caches cleared"}

@app.delete("/cache/{query}")
async def clear_qc(query: str):
    async with CACHE_LOCK:
        existed=query in SEARCH_CACHE
        SEARCH_CACHE.pop(query,None)
    return {"success":True,"query":query,"removed":existed}

@app.get("/player", response_class=FileResponse)
async def player(trackId: str = Query(...)):
    return FileResponse("player.html", media_type="text/html")

@app.get("/")
async def root():
    return {
        "message":"Spotify Full-Duration API (sp_dc + sp_key, no Spotify Developer API key, no youtube)",
        "auth":{"spDcConfigured":bool(SP_DC),"spKeyConfigured":bool(SP_KEY)},
        "usage":{
            "search":"/search?q=QUERY&page=1&limit=20",
            "track":"/track?trackId=TRACK_ID",
            "token":"/token  -> accessToken + clientToken (sp_dc)",
            "cdn":"/cdn?trackId=TRACK_ID  -> direct CDN url (if available, DRM)",
            "proxy":"/proxy?trackId=TRACK_ID  -> stream CDN (DRM)",
            "embed-proxy":"/embed-proxy?trackId=TRACK_ID  -> authenticated embed HTML (full duration, recommended)",
            "stream":"/stream?trackId=TRACK_ID  -> JSON with embedProxyUrl for player",
            "player":"/player?trackId=TRACK_ID  -> HTML player (full duration via embed-proxy)"
        },
        "player":"Open /player?trackId=4iV5W9uYEdYUVa79Axb7Rh",
        "notes":"For music tracks, CDN is Widevine-encrypted. Use embed-proxy for guaranteed full playback. No youtube fallback, no API key needed.",
    }
