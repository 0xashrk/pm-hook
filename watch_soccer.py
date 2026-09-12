#!/usr/bin/env python3
"""Polymarket soccer moneyline price hook — CLOB WS → webhook.

Watches EPL / La Liga / MLS match moneylines (3-way Yes tokens).
Alert when Yes ≥ THRESHOLD, on DELTA_ALERT_CENTS moves, or for
POSITION_SLUGS / WATCH_MARKET_IDS always-watched markets.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import websockets
except ImportError:
    sys.exit("websockets missing — pip install -r requirements.txt (use .venv)")

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "out"
OUT.mkdir(exist_ok=True)
QUEUE = OUT / "price-hook-queue.jsonl"
ALERTED = OUT / "alerted_soccer.json"
LAST_MARK = OUT / "last_mark_soccer.json"
PID = ROOT / "watch_soccer.pid"

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA = "https://gamma-api.polymarket.com"
UA = "Mozilla/5.0 (compatible; polymarket-price-hook/1.0)"

# Verified via GET /sports (sport → series id)
LEAGUES = {
    "epl": {"series_id": "10188", "slug_prefix": "epl", "name": "Premier League"},
    "lal": {"series_id": "10193", "slug_prefix": "lal", "name": "La Liga"},
    "mls": {"series_id": "10189", "slug_prefix": "mls", "name": "MLS"},
}

# Matchday slug: epl-sun-ars-2026-09-12 (not futures / cup winners)
MATCH_SLUG_RE = re.compile(
    r"^(epl|lal|mls)-[a-z0-9]+-[a-z0-9]+-\d{4}-\d{2}-\d{2}$",
    re.I,
)


def load_env() -> None:
    path = ROOT / ".env"
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip())


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}] {msg}"
    print(line, flush=True)


def http_json(url: str) -> object:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=45) as resp:
        return json.load(resp)


def post_webhook(url: str | None, key: str | None, payload: dict) -> str:
    if not url:
        return "skip"
    headers = {"Content-Type": "application/json", "User-Agent": UA}
    if key:
        headers["Authorization"] = f"Bearer {key}"
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers=headers,
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=15).read()
        return "ok"
    except Exception as exc:  # noqa: BLE001
        return type(exc).__name__


def parse_csv_env(name: str) -> set[str]:
    raw = os.environ.get(name, "")
    return {x.strip() for x in raw.split(",") if x.strip()}


def load_json_set(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text())
        return set(data if isinstance(data, list) else [])
    except Exception:
        return set()


def save_json_set(path: Path, items: set[str], keep: int = 2000) -> None:
    ordered = sorted(items)
    if len(ordered) > keep:
        ordered = ordered[-keep:]
    path.write_text(json.dumps(ordered))


def load_json_dict(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_json_dict(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data))


def parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def parse_json_field(raw: object) -> object:
    if isinstance(raw, (list, dict)):
        return raw
    if not raw:
        return []
    try:
        return json.loads(raw)  # type: ignore[arg-type]
    except (json.JSONDecodeError, TypeError):
        return []


def within_horizon(event: dict, horizon_hours: float) -> bool:
    """Keep live + upcoming games inside the horizon window.

    Note: Gamma soccer `endDate` is usually kickoff (not full-time).
    Treat kickoff ± MATCH_DURATION_HOURS as the live window.
    """
    now = datetime.now(timezone.utc)
    duration_h = float(os.environ.get("MATCH_DURATION_HOURS", "3"))
    kick = parse_iso(
        (event.get("markets") or [{}])[0].get("gameStartTime")
        or event.get("startTime")
        or event.get("gameStartTime")
        or event.get("endDate")  # often kickoff on sports
        or event.get("startDate")
    )
    if not kick:
        return True
    live_until = kick + timedelta(hours=duration_h)
    if kick <= now <= live_until:
        return True  # in play / just finished buffer
    if now < kick <= now + timedelta(hours=horizon_hours):
        return True
    return False


def moneyline_rows(event: dict) -> list[dict]:
    rows: list[dict] = []
    for m in event.get("markets") or []:
        smt = (m.get("sportsMarketType") or "").lower()
        if smt and smt != "moneyline":
            continue
        outcomes = parse_json_field(m.get("outcomes"))
        prices = parse_json_field(m.get("outcomePrices"))
        tokens = parse_json_field(m.get("clobTokenIds"))
        if not isinstance(outcomes, list) or not isinstance(tokens, list):
            continue
        if len(outcomes) < 2 or len(tokens) < 2:
            continue
        if not m.get("enableOrderBook", True) and m.get("enableOrderBook") is False:
            continue
        if m.get("acceptingOrders") is False:
            continue
        # Yes token is always index 0 on neg-risk soccer moneylines
        yes_i = 0
        if "Yes" in outcomes:
            yes_i = outcomes.index("Yes")
        yes_tok = str(tokens[yes_i])
        yes_px = None
        if isinstance(prices, list) and len(prices) > yes_i:
            try:
                yes_px = float(prices[yes_i])
            except (TypeError, ValueError):
                yes_px = None
        label = (
            m.get("groupItemTitle")
            or (m.get("question") or "").replace("Will ", "").replace("?", "")
            or m.get("slug")
        )
        rows.append(
            {
                "market_id": m.get("id"),
                "market_slug": m.get("slug"),
                "condition_id": m.get("conditionId"),
                "question": m.get("question"),
                "outcome_label": label,
                "yes_token": yes_tok,
                "yes_price": yes_px,
                "sports_market_type": smt or "moneyline",
            }
        )
    return rows


def resolve_league(league_key: str, cfg: dict, horizon_hours: float) -> list[dict]:
    series_id = cfg["series_id"]
    url = (
        f"{GAMMA}/events?series_id={urllib.parse.quote(series_id)}"
        f"&active=true&closed=false&limit=100"
    )
    try:
        data = http_json(url)
    except Exception as exc:  # noqa: BLE001
        log(f"resolve_err {league_key} {type(exc).__name__}")
        return []
    if not isinstance(data, list):
        return []

    out: list[dict] = []
    for event in data:
        slug = event.get("slug") or ""
        if not MATCH_SLUG_RE.match(slug):
            continue
        if not within_horizon(event, horizon_hours):
            continue
        rows = moneyline_rows(event)
        if not rows:
            continue
        out.append(
            {
                "league": league_key,
                "league_name": cfg["name"],
                "series_id": series_id,
                "event_id": event.get("id"),
                "event_slug": slug,
                "title": event.get("title"),
                "end": event.get("endDate"),
                "start": event.get("startTime") or event.get("startDate"),
                "markets": rows,
            }
        )
    return out


class WatchState:
    def __init__(self) -> None:
        self.by_token: dict[str, dict] = {}  # yes_token -> market meta
        self.by_event: dict[str, dict] = {}  # event_slug -> event meta
        self.alerted: set[str] = load_json_set(ALERTED)
        self.last_mark: dict[str, float] = {
            k: float(v) for k, v in load_json_dict(LAST_MARK).items()
        }
        self.last_price: dict[str, float] = {}
        self.last_market_ts = time.time()
        self.threshold = float(os.environ.get("THRESHOLD", "0.90"))
        self.stale_secs = float(os.environ.get("STALE_SECS", "5"))
        self.refresh_secs = float(os.environ.get("REFRESH_SECS", "90"))
        self.horizon_hours = float(os.environ.get("HORIZON_HOURS", "48"))
        self.delta_cents = float(os.environ.get("DELTA_ALERT_CENTS", "0") or 0)
        self.min_yes = float(os.environ.get("MIN_YES", "0.02"))
        self.max_yes = float(os.environ.get("MAX_YES", "0.98"))
        self.webhook_url = os.environ.get("WEBHOOK_URL")
        self.webhook_key = os.environ.get("WEBHOOK_KEY")
        self.position_slugs = parse_csv_env("POSITION_SLUGS")
        self.watch_market_ids = parse_csv_env("WATCH_MARKET_IDS")
        # league filter: epl,lal,mls
        raw = os.environ.get("LEAGUES", "epl,lal,mls")
        self.leagues = {
            k.strip().lower()
            for k in raw.split(",")
            if k.strip().lower() in LEAGUES
        } or set(LEAGUES)

    def always_watch(self, meta: dict) -> bool:
        if meta.get("event_slug") in self.position_slugs:
            return True
        if meta.get("market_slug") in self.position_slugs:
            return True
        if str(meta.get("market_id") or "") in self.watch_market_ids:
            return True
        return False

    def actionable(self, yes_px: float | None, meta: dict) -> bool:
        if self.always_watch(meta):
            return True
        if yes_px is None:
            return True  # subscribe; WS will price it
        if yes_px <= self.min_yes or yes_px >= self.max_yes:
            return False
        return True

    def dedupe_key(self, meta: dict, reason: str) -> str:
        return f"soccer:{meta['market_id']}:Yes:{reason}"

    async def emit(
        self,
        meta: dict,
        price: float,
        source: str,
        reason: str,
        *,
        best_bid: float | None = None,
        best_ask: float | None = None,
        force: bool = False,
    ) -> None:
        key = self.dedupe_key(meta, reason)
        if not force and key in self.alerted and reason.startswith("threshold"):
            return
        if reason.startswith("threshold"):
            self.alerted.add(key)
            save_json_set(ALERTED, self.alerted)
        elif reason.startswith("delta"):
            # allow repeated deltas; gate by last_mark distance instead
            pass

        mid = meta["market_id"]
        payload = {
            "type": "soccer_moneyline_price_hook",
            "league": meta["league"],
            "league_name": meta["league_name"],
            "event_slug": meta["event_slug"],
            "market_slug": meta["market_slug"],
            "title": meta["title"],
            "outcome": meta["outcome_label"],
            "side": "Yes",
            "price": price,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "threshold": self.threshold,
            "reason": reason,
            "source": source,
            "market_id": mid,
            "event_id": meta["event_id"],
            "condition_id": meta.get("condition_id"),
            "start": meta.get("start"),
            "end": meta.get("end"),
            "url": f"https://askgina.ai/prediction-markets/{mid}",
            "polymarket_url": f"https://polymarket.com/event/{meta['event_slug']}",
            "ts": datetime.now(timezone.utc).isoformat(),
            "dedupe_key": key,
            "position_watch": self.always_watch(meta),
        }
        with QUEUE.open("a") as fh:
            fh.write(json.dumps(payload) + "\n")
        status = await asyncio.to_thread(
            post_webhook, self.webhook_url, self.webhook_key, payload
        )
        log(
            f"hook_{status} {meta['league'].upper()} {meta['outcome_label']}="
            f"{price:.4f} reason={reason} {meta['event_slug']}"
        )

    async def maybe_alert(
        self,
        token_id: str,
        price: float,
        source: str,
        *,
        best_bid: float | None = None,
        best_ask: float | None = None,
    ) -> None:
        token_id = str(token_id)
        meta = self.by_token.get(token_id)
        if not meta:
            return
        self.last_market_ts = time.time()
        self.last_price[token_id] = price

        # threshold alert
        if price >= self.threshold:
            await self.emit(
                meta,
                price,
                source,
                f"threshold>={self.threshold}",
                best_bid=best_bid,
                best_ask=best_ask,
            )

        # delta alert (cents) — mainly for POSITION_SLUGS / watched markets
        if self.delta_cents > 0 and (
            self.always_watch(meta) or price >= self.threshold * 0.5
        ):
            prev = self.last_mark.get(str(meta["market_id"]))
            if prev is None:
                self.last_mark[str(meta["market_id"])] = price
                save_json_dict(LAST_MARK, self.last_mark)
            else:
                move_cents = abs(price - prev) * 100.0
                if move_cents >= self.delta_cents:
                    await self.emit(
                        meta,
                        price,
                        source,
                        f"delta>={self.delta_cents}c",
                        best_bid=best_bid,
                        best_ask=best_ask,
                        force=True,
                    )
                    self.last_mark[str(meta["market_id"])] = price
                    save_json_dict(LAST_MARK, self.last_mark)


async def subscribe(ws, tokens: list[str]) -> None:
    if not tokens:
        return
    await ws.send(
        json.dumps(
            {
                "assets_ids": tokens,
                "type": "market",
                "custom_feature_enabled": True,
            }
        )
    )


async def unsubscribe(ws, tokens: list[str]) -> None:
    if not tokens:
        return
    await ws.send(json.dumps({"assets_ids": tokens, "operation": "unsubscribe"}))


async def ensure_slate(ws, state: WatchState) -> None:
    old_tokens = set(state.by_token.keys())
    new_by_token: dict[str, dict] = {}
    new_by_event: dict[str, dict] = {}

    for league_key in sorted(state.leagues):
        cfg = LEAGUES[league_key]
        events = await asyncio.to_thread(
            resolve_league, league_key, cfg, state.horizon_hours
        )
        log(f"resolved {league_key} events={len(events)}")
        for ev in events:
            new_by_event[ev["event_slug"]] = ev
            for m in ev["markets"]:
                meta = {
                    **m,
                    "league": ev["league"],
                    "league_name": ev["league_name"],
                    "series_id": ev["series_id"],
                    "event_id": ev["event_id"],
                    "event_slug": ev["event_slug"],
                    "title": ev["title"],
                    "start": ev.get("start"),
                    "end": ev.get("end"),
                }
                if not state.actionable(m.get("yes_price"), meta):
                    continue
                tok = m["yes_token"]
                new_by_token[tok] = meta
                if m.get("yes_price") is not None:
                    await state.maybe_alert(
                        tok, float(m["yes_price"]), "gamma_seed"
                    )

    state.by_token = new_by_token
    state.by_event = new_by_event
    new_tokens = set(new_by_token.keys())
    to_drop = list(old_tokens - new_tokens)
    to_add = list(new_tokens - old_tokens)
    if to_drop:
        await unsubscribe(ws, to_drop)
    if to_add:
        await subscribe(ws, to_add)
    elif new_tokens and not old_tokens:
        await subscribe(ws, list(new_tokens))
    state.last_market_ts = time.time()
    log(
        f"watching tokens={len(new_tokens)} events={len(new_by_event)} "
        f"leagues={sorted(state.leagues)}"
    )


async def handle_msg(state: WatchState, raw: str) -> None:
    if raw in ("PONG", "pong"):
        return
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        return
    if isinstance(msg, list):
        for item in msg:
            await handle_msg(state, json.dumps(item))
        return
    if not isinstance(msg, dict):
        return
    et = msg.get("event_type") or msg.get("type")
    if et == "best_bid_ask":
        token = msg.get("asset_id") or (msg.get("payload") or {}).get("tokenId")
        bb = msg.get("best_bid")
        ba = msg.get("best_ask")
        payload = msg.get("payload") or {}
        if bb is None:
            bb = payload.get("bestBid") or payload.get("best_bid")
        if ba is None:
            ba = payload.get("bestAsk") or payload.get("best_ask")
        if token is None:
            token = payload.get("tokenId") or payload.get("token_id")
        if token is not None:
            px = float(ba) if ba is not None else (float(bb) if bb is not None else None)
            if px is not None:
                await state.maybe_alert(
                    str(token),
                    px,
                    "best_bid_ask",
                    best_bid=float(bb) if bb is not None else None,
                    best_ask=float(ba) if ba is not None else None,
                )
        return
    if et == "last_trade_price":
        token = msg.get("asset_id") or (msg.get("payload") or {}).get("tokenId")
        price = msg.get("price")
        payload = msg.get("payload") or {}
        if token is None:
            token = payload.get("tokenId") or payload.get("token_id")
        if price is None:
            price = payload.get("price")
        if token is not None and price is not None:
            await state.maybe_alert(str(token), float(price), "last_trade_price")
        return
    if et == "price_change":
        changes = msg.get("price_changes") or (msg.get("payload") or {}).get(
            "priceChanges"
        ) or []
        for ch in changes:
            token = ch.get("asset_id") or ch.get("tokenId") or ch.get("token_id")
            ba = ch.get("best_ask") or ch.get("bestAsk")
            bb = ch.get("best_bid") or ch.get("bestBid")
            px = ba or bb or ch.get("price")
            if token is not None and px is not None:
                await state.maybe_alert(
                    str(token),
                    float(px),
                    "price_change",
                    best_bid=float(bb) if bb is not None else None,
                    best_ask=float(ba) if ba is not None else None,
                )
        return


async def run() -> None:
    load_env()
    PID.write_text(str(os.getpid()))
    state = WatchState()
    if not state.webhook_url:
        log("WARN WEBHOOK_URL unset — will queue only")
    log(
        f"leagues={sorted(state.leagues)} threshold>={state.threshold} "
        f"delta_cents={state.delta_cents} horizon_h={state.horizon_hours} "
        f"refresh={state.refresh_secs}s stale={state.stale_secs}s "
        f"position_slugs={sorted(state.position_slugs) or '-'}"
    )
    backoff = 1
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=None) as ws:
                log("ws_connected")
                backoff = 1
                state.by_token.clear()
                state.by_event.clear()
                await ensure_slate(ws, state)

                async def heartbeat():
                    while True:
                        await asyncio.sleep(10)
                        await ws.send("PING")

                async def refresh():
                    while True:
                        await asyncio.sleep(state.refresh_secs)
                        await ensure_slate(ws, state)

                async def liveness():
                    while True:
                        await asyncio.sleep(5)
                        age = time.time() - state.last_market_ts
                        if age > state.stale_secs and state.by_token:
                            log(f"stale_market {age:.0f}s — forcing reconnect")
                            await ws.close()
                            return

                hb = asyncio.create_task(heartbeat())
                rf = asyncio.create_task(refresh())
                live = asyncio.create_task(liveness())
                try:
                    async for raw in ws:
                        if isinstance(raw, bytes):
                            raw = raw.decode()
                        await handle_msg(state, raw)
                finally:
                    hb.cancel()
                    rf.cancel()
                    live.cancel()
        except Exception as exc:  # noqa: BLE001
            log(f"ws_err {type(exc).__name__}: {exc}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


def main() -> None:
    load_env()
    log("starting watch_soccer")
    try:
        asyncio.run(run())
    finally:
        if PID.exists():
            PID.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
