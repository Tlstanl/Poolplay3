"""
Pool Play Seed Projections — self-hosted server
================================================
Fetches USSSA pool standings/games and GameChanger live scores,
merges them, and serves the original frontend via SSE push.

Quick start
-----------
    pip install -r requirements.txt
    python server.py
    open http://localhost:8000

Add GC team URLs to GC_TEAMS as you collect them.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import time
from dataclasses import dataclass, field
from typing import Any

import aiohttp
from aiohttp import web

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION  ← only section you need to edit
# ─────────────────────────────────────────────────────────────────────────────

EVENT_ID    = 409034
DIVISION_ID = 2691056
AGE         = 11
AGE_CLASS   = "Open"
SPORT       = "baseball"

EVENT_NAME      = "Arkansas Premier East / West Shootout"
DIVISION_NAME   = f"{AGE}U {AGE_CLASS}"
EVENT_LOCATION  = "Springdale/Bentonville/Fayetteville, AR"
EVENT_DATES     = ""   # e.g. "May 10–11, 2026"

SOURCE_URL = (
    f"https://www.usssa.com/{SPORT}/event_gameCenter/"
    f"?eventID={EVENT_ID}&age={AGE}&ageClass={AGE_CLASS}"
    f"&option=101&divisionID={DIVISION_ID}&bnp=0&bf=1&isWinner=1"
)

# GameChanger team map: case-insensitive substring of USSSA team name → GC public_id
# public_id is the slug in: https://web.gc.com/teams/<public_id>/...
GC_TEAMS: dict[str, str] = {
    "patriots white":  "z415Y79XAsjY",
    "legacy beau":     "VrnhIDGE1Kbd",
    "texas majors":    "YYGumz2hZNCe",
    "patriots navy":   "y5jnLc1Ar98j",
    "fuel":            "r2y8C0KrPPNE",
    "oklahoma eleven": "Z9ZvOYlurKxl",
    # Add more teams here as you collect their GC URLs:
    # "rebels":     "aBcDeFgHiJkL",
}

# Polling cadence (seconds)
POLL_LIVE     = 20    # games actively in progress
POLL_WAITING  = 120   # between game windows
POLL_IDLE     = 300   # nothing going on

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 8000))

# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("poolplay")

USSSA_STANDINGS = (
    f"https://www.usssa.com/api2/{SPORT}/poolstandings"
    f"?eventID={EVENT_ID}&divisionID={DIVISION_ID}"
)
USSSA_GAMES = (
    f"https://www.usssa.com/api2/{SPORT}/poolGames"
    f"?eventID={EVENT_ID}&divisionID={DIVISION_ID}"
)

# ─────────────────────────────────────────────────────────────────────────────
# Shared state
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class AppState:
    payload: dict       = field(default_factory=dict)
    revision: int       = 0
    content_hash: str   = ""
    last_fetch: float   = 0.0
    fetch_error: str    = ""
    poll_reason: str    = "idle"
    next_poll_at: float = 0.0
    next_interval: int  = POLL_IDLE
    sse_queues: list    = field(default_factory=list)

state = AppState()

# ─────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.usssa.com/",
    "Origin": "https://www.usssa.com",
}

async def get_json(session: aiohttp.ClientSession, url: str) -> Any:
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
        r.raise_for_status()
        return await r.json(content_type=None)

# ─────────────────────────────────────────────────────────────────────────────
# USSSA data — fetched via Anthropic API + USSSA MCP connector
# ─────────────────────────────────────────────────────────────────────────────

def _as_list(raw: Any, *keys) -> list:
    """Pull a list from raw, trying each key in order."""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for k in keys:
            v = raw.get(k)
            if isinstance(v, list):
                return v
    return []

async def fetch_usssa_via_mcp(session: aiohttp.ClientSession) -> tuple[list, dict]:
    """Use Anthropic API + USSSA MCP to fetch pool standings and games."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY environment variable not set")

    prompt = (
        f"Using the USSSA MCP tool, fetch the pool standings and pool games for "
        f"eventID={EVENT_ID} and divisionID={DIVISION_ID}. "
        f"Return ONLY a JSON object with two keys: 'standings' (array of pool objects with results) "
        f"and 'games' (array of pool objects with games). No explanation, just JSON."
    )

    payload = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 4000,
        "messages": [{"role": "user", "content": prompt}],
        "mcp_servers": [
            {"type": "url", "url": "https://xelite.poolplaytool.com/", "name": "usssa-mcp"}
        ],
    }

    async with session.post(
        "https://api.anthropic.com/v1/messages",
        json=payload,
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "anthropic-beta": "mcp-client-2025-04-04",
            "Content-Type": "application/json",
        },
        timeout=aiohttp.ClientTimeout(total=60),
    ) as r:
        r.raise_for_status()
        resp = await r.json(content_type=None)

    # Extract JSON from the response text
    text = ""
    for block in resp.get("content", []):
        if block.get("type") == "text":
            text += block.get("text", "")

    # Find JSON in the response
    import re
    match = re.search(r'\{.*\}', text, re.DOTALL)
    if not match:
        raise RuntimeError(f"No JSON found in MCP response: {text[:200]}")
    data = json.loads(match.group())
    standings = _as_list(data.get("standings", []), "pools", "data", "results")
    games_raw = data.get("games", [])
    games_pools = _as_list(games_raw, "pools", "data") if not isinstance(games_raw, list) else games_raw
    games_by_pool: dict[str, list] = {}
    for p in games_pools:
        letter = p.get("pool") or p.get("letter") or "A"
        games_by_pool[letter] = p.get("games") or p.get("poolGames") or []
    return standings, games_by_pool

async def fetch_usssa_direct(session: aiohttp.ClientSession) -> tuple[list, dict]:
    """Direct USSSA API fetch — works when called from a server with proper headers."""
    standings_raw, games_raw = await asyncio.gather(
        get_json(session, USSSA_STANDINGS),
        get_json(session, USSSA_GAMES),
    )
    pools = _as_list(standings_raw, "pools", "data", "results")
    games_pools = _as_list(games_raw, "pools", "data")
    if not games_pools and isinstance(games_raw, dict) and games_raw.get("games"):
        games_pools = [{"pool": "A", "games": games_raw["games"]}]
    games_by_pool: dict[str, list] = {}
    for p in games_pools:
        letter = p.get("pool") or p.get("letter") or "A"
        games_by_pool[letter] = p.get("games") or p.get("poolGames") or []
    return pools, games_by_pool

async def fetch_usssa(session: aiohttp.ClientSession) -> tuple[list, dict]:
    """Try direct API first, fall back to MCP if blocked."""
    try:
        return await fetch_usssa_direct(session)
    except Exception as e:
        log.warning("Direct USSSA fetch failed (%s), trying MCP fallback", e)
        return await fetch_usssa_via_mcp(session)

# ─────────────────────────────────────────────────────────────────────────────
# GameChanger data
# ─────────────────────────────────────────────────────────────────────────────

def _gc_id(team_name: str) -> str | None:
    name_lower = (team_name or "").lower()
    for fragment, pid in GC_TEAMS.items():
        if fragment.lower() in name_lower:
            return pid
    return None

async def fetch_gc_schedule(session: aiohttp.ClientSession, public_id: str) -> list:
    url = f"https://api.playmetrics.com/v4/teams/{public_id}/schedule?includeGames=true&limit=30"
    try:
        raw = await get_json(session, url)
        return _as_list(raw, "data", "events", "games")
    except Exception as exc:
        log.warning("GC schedule %s: %s", public_id, exc)
        return []

def _normalise_gc_live(live: dict, _game: dict) -> dict:
    status = (live.get("status") or "").lower()
    source = "gc_completed" if ("final" in status or "complet" in status) else "gc_live"

    # Per-inning line score (GC calls them "innings")
    def _line(side: str):
        innings = live.get(f"{side}Innings") or live.get(f"{side}_innings") or []
        scores  = [i.get("runs") for i in innings]
        totals  = [
            live.get(f"{side}Runs") or live.get(f"{side}_runs"),
            live.get(f"{side}Hits") or live.get(f"{side}_hits"),
            live.get(f"{side}Errors") or live.get(f"{side}_errors"),
        ]
        return {"scores": scores, "totals": totals} if scores else None

    return {
        "source":        source,
        "inning":        live.get("inning") or live.get("currentInning"),
        "half":          live.get("half") or live.get("inningHalf"),
        "inning_label":  live.get("inningLabel") or live.get("inning_label"),
        "balls":         live.get("balls"),
        "strikes":       live.get("strikes"),
        "outs":          live.get("outs"),
        "team_a_score":  live.get("homeScore") or live.get("home_score") or 0,
        "team_b_score":  live.get("awayScore") or live.get("away_score") or 0,
        "team_a_line":   _line("home"),
        "team_b_line":   _line("away"),
        "has_live_stream": live.get("hasLiveStream") or False,
        "event_id":      live.get("eventId") or live.get("event_id"),
    }

def _find_gc_live(usssa_game: dict, gc_schedules: dict[str, list]) -> dict | None:
    """Match a USSSA game to a GC game by team name and return live data."""
    a_name = (
        usssa_game.get("WinningTeamName") or
        usssa_game.get("HomeTeamName") or ""
    ).lower()
    b_name = (
        usssa_game.get("LosingTeamName") or
        usssa_game.get("AwayTeamName") or ""
    ).lower()

    for _pid, schedule in gc_schedules.items():
        for ev in schedule:
            game = ev.get("game") or ev
            home = (game.get("homeTeamName") or game.get("home_team_name") or "").lower()
            away = (game.get("awayTeamName") or game.get("away_team_name") or "").lower()
            match = (
                (a_name and (a_name in home or home in a_name)) or
                (b_name and (b_name in away or away in b_name)) or
                (a_name and (a_name in away or away in a_name))
            )
            if match:
                live_data = game.get("liveData") or game.get("live") or game.get("scoring")
                if live_data:
                    return _normalise_gc_live(live_data, game)
    return None

# ─────────────────────────────────────────────────────────────────────────────
# Monte Carlo seed simulation
# ─────────────────────────────────────────────────────────────────────────────

MAX_RD   = 10
N_SIMS   = 5_000

def _simulate(teams: list[dict], games: list[dict]) -> dict[Any, tuple[int, int]]:
    """Return {team_id: (best_place, worst_place)} via Monte Carlo."""
    tids = [
        t.get("teamID") or t.get("team_id") or t.get("teamId")
        for t in teams
    ]

    # Seed from played games
    wins = {tid: 0   for tid in tids}
    loss = {tid: 0   for tid in tids}
    rf   = {tid: 0.0 for tid in tids}
    ra   = {tid: 0.0 for tid in tids}
    gp   = {tid: 0   for tid in tids}

    played, unplayed = [], []
    for g in games:
        if g.get("hasScore") and str(g.get("hasScore")) != "0":
            played.append(g)
        else:
            unplayed.append(g)

    for g in played:
        wid = g.get("WinningTeamID")
        lid = g.get("LosingTeamID")
        ws  = int(g.get("WinnersScore") or 0)
        ls  = int(g.get("LosersScore")  or 0)
        if wid in wins: wins[wid] += 1; rf[wid] += ws; ra[wid] += ls; gp[wid] += 1
        if lid in wins: loss[lid] += 1; rf[lid] += ls; ra[lid] += ws; gp[lid] += 1

    best  = {tid: len(tids) for tid in tids}
    worst = {tid: 1          for tid in tids}

    for _ in range(N_SIMS):
        sw, sl, srf, sra, sgp = dict(wins), dict(loss), dict(rf), dict(ra), dict(gp)
        for g in unplayed:
            a = g.get("HomeTeamID") or g.get("teamAID")
            b = g.get("AwayTeamID") or g.get("teamBID")
            if a not in sw or b not in sw:
                continue
            as_, bs_ = random.randint(0, 10), random.randint(0, 10)
            if as_ == bs_: bs_ += 1
            if as_ > bs_:
                sw[a] += 1; sl[b] += 1
                srf[a] += as_; sra[a] += bs_; srf[b] += bs_; sra[b] += as_
            else:
                sw[b] += 1; sl[a] += 1
                srf[b] += bs_; sra[b] += as_; srf[a] += as_; sra[a] += bs_
            sgp[a] += 1; sgp[b] += 1

        def key(tid):
            g_ = sgp[tid] or 1
            return (
                -(sw[tid] / g_),           # win pct desc
                -sw[tid],                  # wins desc
                sl[tid],                   # losses asc
                sra[tid] / g_,             # avg PA asc
                -min((srf[tid] - sra[tid]) / g_, MAX_RD),  # avg diff desc
            )

        ranked = sorted(tids, key=key)
        for place, tid in enumerate(ranked, 1):
            if place < best[tid]:  best[tid]  = place
            if place > worst[tid]: worst[tid] = place

    return {tid: (best[tid], worst[tid]) for tid in tids}

# ─────────────────────────────────────────────────────────────────────────────
# Payload builder
# ─────────────────────────────────────────────────────────────────────────────

def _usssa_game_out(g: dict) -> dict:
    has_score = g.get("hasScore") and str(g.get("hasScore")) != "0"
    is_tie    = bool(g.get("is_tie") or g.get("isTie"))

    if has_score and not is_tie:
        a_id, a_name, a_reg = g.get("WinningTeamID"), g.get("WinningTeamName",""), g.get("winningTeamRegionABR","")
        b_id, b_name, b_reg = g.get("LosingTeamID"),  g.get("LosingTeamName",""),  g.get("losingTeamRegionABR","")
    else:
        a_id, a_name, a_reg = g.get("HomeTeamID") or g.get("WinningTeamID"), g.get("HomeTeamName","") or g.get("WinningTeamName",""), g.get("winningTeamRegionABR","")
        b_id, b_name, b_reg = g.get("AwayTeamID") or g.get("LosingTeamID"),  g.get("AwayTeamName","")  or g.get("LosingTeamName",""),  g.get("losingTeamRegionABR","")

    return {
        "game_number":        g.get("GameNumber"),
        "pool_letter":        g.get("pool") or g.get("poolLetter") or "",
        "scheduled_time":     g.get("the_date") or g.get("gameDay") or g.get("scheduledTime"),
        "field_name":         g.get("field_name") or g.get("fieldName") or "",
        "played":             bool(has_score),
        "is_tie":             is_tie,
        "final_source":       "usssa",
        "team_a_id":          a_id,
        "team_a_name":        a_name,
        "team_a_region":      a_reg,
        "team_a_gc_public_id": _gc_id(a_name),
        "team_b_id":          b_id,
        "team_b_name":        b_name,
        "team_b_region":      b_reg,
        "team_b_gc_public_id": _gc_id(b_name),
        "winner_id":          g.get("WinningTeamID") if has_score and not is_tie else None,
        "winner_score":       int(g.get("WinnersScore") or 0),
        "loser_score":        int(g.get("LosersScore")  or 0),
    }

def _poll_cadence(pools: list[dict]) -> tuple[str, int]:
    live = sum(1 for p in pools for g in p.get("games", []) if g.get("live"))
    if live:
        return "in_progress", POLL_LIVE
    unplayed = sum(1 for p in pools for g in p.get("games", []) if not g.get("played"))
    if not unplayed:
        return "idle", POLL_IDLE
    return "waiting", POLL_WAITING

async def build_payload(session: aiohttp.ClientSession) -> dict:
    pools_raw, games_by_pool = await fetch_usssa(session)

    # Fetch GC schedules for all known teams
    gc_ids = list(set(GC_TEAMS.values()))
    gc_schedules: dict[str, list] = {}
    for pid in gc_ids:
        gc_schedules[pid] = await fetch_gc_schedule(session, pid)

    pools_out = []
    for idx, pool in enumerate(pools_raw):
        letter    = pool.get("pool") or pool.get("letter") or chr(65 + idx)
        teams_raw = pool.get("results") or pool.get("standings") or pool.get("teams") or []
        games_raw = games_by_pool.get(letter) or pool.get("games") or []
        pool_size = len(teams_raw)

        # Run simulation
        ranges = _simulate(teams_raw, games_raw) if pool_size > 1 else {}

        games_played = sum(
            1 for g in games_raw
            if g.get("hasScore") and str(g.get("hasScore")) != "0"
        )

        # Build teams
        teams_out = []
        for t in teams_raw:
            tid   = t.get("teamID") or t.get("team_id") or t.get("teamId")
            tname = t.get("teamName") or t.get("team_name") or t.get("name") or ""
            place = int(t.get("poolPlace") or t.get("pool_place") or 0)
            wins  = int(t.get("pool_wins")  or t.get("wins")   or 0)
            losss = int(t.get("pool_loses") or t.get("losses") or t.get("pool_losses") or 0)
            ties  = int(t.get("pool_ties")  or t.get("ties")   or 0)
            gp_n  = wins + losss + ties or 1
            avgpa   = float(t.get("pool_avg_points_allowed") or 0)
            avgdiff = float(t.get("pool_avg_point_diff_with_maximum") or 0)
            wp      = int(round(((wins + ties * 0.5) / gp_n) * 1000))
            b, w    = ranges.get(tid, (place, place))
            games_per_team = len(games_raw) // pool_size if pool_size else 0
            rem = max(0, games_per_team - gp_n)

            teams_out.append({
                "team_id":            tid,
                "team_name":          tname,
                "team_region":        t.get("teamRegion") or t.get("region") or "",
                "avatar_url":         t.get("avatar_url") or t.get("avatarUrl") or "",
                "gc_public_id":       _gc_id(tname),
                "current_place":      place,
                "best_place":         b,
                "worst_place":        w,
                "clinched":           b == w,
                "is_playing_now":     False,  # updated below
                "wins":               wins,
                "losses":             losss,
                "ties":               ties,
                "remaining_games":    rem,
                "win_pct_x1000":      wp,
                "avg_points_allowed": avgpa,
                "avg_capped_diff":    avgdiff,
                "pool_size":          pool_size,
                "bracket_projection": [],  # populated if bracket data available
            })

        # Build games and attach GC live data
        games_out = []
        live_team_ids: set = set()
        for g in games_raw:
            go = _usssa_game_out(g)
            gc_live = _find_gc_live(g, gc_schedules)
            if gc_live:
                go["live"] = gc_live
                live_team_ids.add(go["team_a_id"])
                live_team_ids.add(go["team_b_id"])
            games_out.append(go)

        # Mark playing-now teams
        for t in teams_out:
            t["is_playing_now"] = t["team_id"] in live_team_ids

        pools_out.append({
            "letter":               letter,
            "games_played":         games_played,
            "games_total":          len(games_raw),
            "is_final":             games_played == len(games_raw) and len(games_raw) > 0,
            "scenarios_considered": N_SIMS,
            "teams":                teams_out,
            "games":                games_out,
        })

    reason, interval = _poll_cadence(pools_out)
    now = time.time()
    return {
        "event_name":           EVENT_NAME,
        "division_name":        DIVISION_NAME,
        "event_dates":          EVENT_DATES,
        "event_location":       EVENT_LOCATION,
        "source_url":           SOURCE_URL,
        "fetched_at":           now,
        "next_poll_at":         now + interval,
        "next_interval":        interval,
        "next_interval_reason": reason,
        "fetch_error":          "",
        "pools":                pools_out,
    }

# ─────────────────────────────────────────────────────────────────────────────
# Background poller
# ─────────────────────────────────────────────────────────────────────────────

def _broadcast():
    msg = json.dumps({
        "revision":             state.revision,
        "fetched_at":           state.last_fetch,
        "next_poll_at":         state.next_poll_at,
        "next_interval":        state.next_interval,
        "next_interval_reason": state.poll_reason,
    })
    dead = []
    for q in state.sse_queues:
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        state.sse_queues.remove(q)

async def poller():
    connector = aiohttp.TCPConnector(limit=10, ssl=False)
    async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
        while True:
            try:
                payload = await build_payload(session)
                raw     = json.dumps(payload, default=str).encode()
                h       = hashlib.md5(raw).hexdigest()
                if h != state.content_hash:
                    state.content_hash = h
                    state.revision    += 1
                    log.info("Revision → %d", state.revision)
                state.payload    = payload
                state.last_fetch = time.time()
                state.fetch_error = ""
            except Exception as exc:
                log.error("Poll error: %s", exc, exc_info=True)
                state.fetch_error = str(exc)

            reason, interval = _poll_cadence(state.payload.get("pools", []))
            state.poll_reason   = reason
            state.next_interval = interval
            state.next_poll_at  = time.time() + interval
            _broadcast()
            log.info("Next poll in %ds (%s)", interval, reason)
            await asyncio.sleep(interval)

# ─────────────────────────────────────────────────────────────────────────────
# HTTP handlers
# ─────────────────────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

async def handle_index(req: web.Request) -> web.Response:
    path = os.path.join(BASE_DIR, "templates", "index.html")
    with open(path, encoding="utf-8") as f:
        html = f.read()
    return web.Response(text=html, content_type="text/html")

async def handle_state(req: web.Request) -> web.Response:
    try:
        payload = {
            **state.payload,
            "revision":             state.revision,
            "fetch_error":          state.fetch_error,
            "next_poll_at":         state.next_poll_at,
            "next_interval":        state.next_interval,
            "next_interval_reason": state.poll_reason,
        }
        # If payload is still empty (cold start), return a loading state
        if not payload.get("pools"):
            payload = {
                "event_name":           EVENT_NAME,
                "division_name":        DIVISION_NAME,
                "event_dates":          EVENT_DATES,
                "event_location":       EVENT_LOCATION,
                "source_url":           SOURCE_URL,
                "fetched_at":           state.last_fetch,
                "next_poll_at":         state.next_poll_at,
                "next_interval":        state.next_interval,
                "next_interval_reason": state.poll_reason,
                "fetch_error":          state.fetch_error or "Loading data, please wait…",
                "pools":                [],
                "revision":             state.revision,
            }
        return web.Response(
            text=json.dumps(payload, default=str),
            content_type="application/json",
            headers={"Access-Control-Allow-Origin": "*"},
        )
    except Exception as exc:
        log.error("handle_state error: %s", exc, exc_info=True)
        return web.Response(
            text=json.dumps({"fetch_error": str(exc), "pools": [], "revision": 0,
                             "next_interval_reason": "idle", "next_poll_at": 0, "next_interval": 30}),
            content_type="application/json",
            status=200,
            headers={"Access-Control-Allow-Origin": "*"},
        )

async def handle_events(req: web.Request) -> web.StreamResponse:
    resp = web.StreamResponse(headers={
        "Content-Type":                "text/event-stream",
        "Cache-Control":               "no-cache",
        "X-Accel-Buffering":           "no",
        "Access-Control-Allow-Origin": "*",
    })
    await resp.prepare(req)

    q: asyncio.Queue = asyncio.Queue(maxsize=10)
    state.sse_queues.append(q)

    # Send current state immediately so the client doesn't wait for next poll
    init = json.dumps({
        "revision":             state.revision,
        "fetched_at":           state.last_fetch,
        "next_poll_at":         state.next_poll_at,
        "next_interval":        state.next_interval,
        "next_interval_reason": state.poll_reason,
    })
    await resp.write(f"event: state\ndata: {init}\n\n".encode())

    try:
        while True:
            try:
                msg = await asyncio.wait_for(q.get(), timeout=15)
                await resp.write(f"event: state\ndata: {msg}\n\n".encode())
            except asyncio.TimeoutError:
                await resp.write(b": heartbeat\n\n")
    except (ConnectionResetError, asyncio.CancelledError):
        pass
    finally:
        if q in state.sse_queues:
            state.sse_queues.remove(q)
    return resp

# ─────────────────────────────────────────────────────────────────────────────
# App
# ─────────────────────────────────────────────────────────────────────────────

async def on_startup(app: web.Application):
    asyncio.create_task(poller())

def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/",           handle_index)
    app.router.add_get("/api/state",  handle_state)
    app.router.add_get("/api/events", handle_events)
    app.on_startup.append(on_startup)
    return app

if __name__ == "__main__":
    log.info("Pool Play server → http://%s:%d", HOST, PORT)
    log.info("BASE_DIR: %s", BASE_DIR)
    log.info("Template: %s", os.path.join(BASE_DIR, "templates", "index.html"))
    log.info("Template exists: %s", os.path.exists(os.path.join(BASE_DIR, "templates", "index.html")))
    web.run_app(create_app(), host=HOST, port=PORT, print=None)
