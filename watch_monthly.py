#!/usr/bin/env python3
"""Polymarket monthly crypto ladder (LFT) price hook — CLOB WS → webhook.

Resolves Gamma events tagged monthly + crypto, drops yearly / equities,
subscribes to Yes token ids for actionable rungs, alerts at THRESHOLD.
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
from datetime import datetime, timezone
from pathlib import Path

try:
    import websockets
except ImportError:
    sys.exit("websockets missing — pip install -r requirements.txt (use .venv)")

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "out"
OUT.mkdir(exist_ok=True)
QUEUE = OUT / "price-hook-queue.jsonl"
ALERTED = OUT / "alerted_monthly.json"
PID = ROOT / "watch_monthly.pid"

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA = "https://gamma-api.polymarket.com"
UA = "Mozilla/5.0 (compatible; polymarket-price-hook/1.0)"

# Gamma tag ids (verified): monthly=102144, crypto=21
TAG_MONTHLY = "102144"
TAG_CRYPTO = "21"

CRYPTO_TAGS = frozenset({"crypto", "crypto-prices"})
EXCLUDE_TAGS = frozenset(
    {
        "yearly",
        "equities",
        "stocks",
        "finance",
        "finance-updown",
        "pyth-finance",
        "indicies",
        "spx",
        "market-cap",
        "market-capitalization",
    }
)
# Non-crypto tickers that sometimes leak onto monthly lists
EXCLUDE_SLUG_RE = re.compile(
    r"(?:^|[-_])(?:abnb|aapl|msft|amzn|googl|meta|tsla|nvda|nflx|spx|spacex)(?:$|[-_])",
    re.I,
)
# Yearly heuristics on title/slug (monthly dated "september-2026" is OK)
YEARLY_SLUG_RE = re.compile(
    r"(?:^|[-_])(?:before[-_]?20\d{2}|in[-_]20\d{2}|by[-_](?:end[-_]of[-_])?20\d{2})(?:$|[-_])",
    re.I,
)
YEARLY_TITLE_RE = re.compile(
    r"\b(?:before\s+20\d{2}|in\s+20\d{2}|by\s+(?:the\s+)?end\s+of\s+20\d{2})\b",
    re.I,
)
MONTH_WORD_RE = re.compile(
    r"\b(?:january|february|march|april|may|june|july|august|september|"
    r"october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)\b",
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
    with urllib.request.urlopen(req, timeout=30) as resp:
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


def load_alerted() -> set[str]:
    if not ALERTED.exists():
        return set()
    try:
        data = json.loads(ALERTED.read_text())
        return set(data if isinstance(data, list) else [])
    except Exception:
        return set()


def save_alerted(alerted: set[str]) -> None:
    items = sorted(alerted)
    if len(items) > 2000:
        items = items[-2000:]
    ALERTED.write_text(json.dumps(items))


def _parse_json_list(raw) -> list:
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return []


def is_yearly_heuristic(title: str, slug: str) -> bool:
    """Drop calendar-year / before-YYYY markets; keep month-named monthly LFTs."""
    blob_slug = slug or ""
    blob_title = title or ""
    if YEARLY_SLUG_RE.search(blob_slug) and not MONTH_WORD_RE.search(blob_slug):
        return True
    if YEARLY_TITLE_RE.search(blob_title) and not MONTH_WORD_RE.search(blob_title):
        return True
    # Explicit before-YYYY always yearly even if a month word appears elsewhere
    if re.search(r"before[-_ ]?20\d{2}", blob_slug, re.I) or re.search(
        r"\bbefore\s+20\d{2}\b", blob_title, re.I
    ):
        return True
    return False


def event_is_monthly_crypto(event: dict) -> bool:
    tags = event.get("tags") or []
    slugs = {str(t.get("slug") or "").lower() for t in tags if isinstance(t, dict)}
    title = event.get("title") or ""
    slug = event.get("slug") or ""

    if "monthly" not in slugs:
        return False
    if "yearly" in slugs:
        return False
    if not (slugs & CRYPTO_TAGS):
        return False
    if slugs & EXCLUDE_TAGS:
        return False
    if EXCLUDE_SLUG_RE.search(slug) or EXCLUDE_SLUG_RE.search(title.replace(" ", "-")):
        return False
    if is_yearly_heuristic(title, slug):
        return False
    if event.get("closed"):
        return False
    return True


def yes_token_and_price(market: dict) -> tuple[str | None, float | None, list, dict]:
    outcomes = [str(x) for x in _parse_json_list(market.get("outcomes"))]
    prices_raw = _parse_json_list(market.get("outcomePrices"))
    tokens = [str(t) for t in _parse_json_list(market.get("clobTokenIds"))]
    prices: dict[str, float] = {}
    for i, o in enumerate(outcomes):
        if i < len(prices_raw):
            try:
                prices[o] = float(prices_raw[i])
            except (TypeError, ValueError):
                pass
    yes_idx = next((i for i, o in enumerate(outcomes) if o.lower() == "yes"), 0)
    yes_tok = tokens[yes_idx] if yes_idx < len(tokens) else None
    yes_px = None
    if outcomes and yes_idx < len(outcomes):
        yes_px = prices.get(outcomes[yes_idx])
    return yes_tok, yes_px, outcomes, prices


def rung_spread(market: dict) -> float | None:
    bb, ba = market.get("bestBid"), market.get("bestAsk")
    if bb is not None and ba is not None:
        try:
            return float(ba) - float(bb)
        except (TypeError, ValueError):
            pass
    sp = market.get("spread")
    if sp is not None:
        try:
            return float(sp)
        except (TypeError, ValueError):
            pass
    return None


def is_actionable(
    yes: float | None,
    market: dict,
    *,
    max_spread: float,
    threshold: float,
) -> bool:
    """Guardrails for WS subscribe: book + non-extreme; prefer mid-range / tight spread."""
    if yes is None:
        return False
    if yes <= 0.02 or yes >= 0.98:
        return False
    has_book = bool(market.get("enableOrderBook")) and bool(
        market.get("acceptingOrders", True)
    )
    if not has_book:
        return False
    spread = rung_spread(market)
    # Prefer tight spread; still allow threshold-zone rungs so we can alert.
    if spread is not None and spread > max_spread and yes < threshold:
        return False
    # Prefer 0.15–0.85 for watching; always keep >= THRESHOLD.
    if yes >= threshold:
        return True
    if 0.15 <= yes <= 0.85:
        return True
    # Approaching band (0.85 .. threshold): keep watching
    if yes > 0.85:
        return True
    return False


def parse_rung(market: dict, event: dict, *, max_spread: float, threshold: float) -> dict | None:
    if market.get("closed") or not market.get("active", True):
        return None
    yes_tok, yes_px, outcomes, prices = yes_token_and_price(market)
    if not yes_tok:
        return None
    mid = market.get("id")
    rung_label = market.get("groupItemTitle") or market.get("question") or str(mid)
    actionable = is_actionable(yes_px, market, max_spread=max_spread, threshold=threshold)
    return {
        "market_id": str(mid),
        "event_id": str(event.get("id")),
        "event_slug": event.get("slug"),
        "event_title": event.get("title"),
        "rung": rung_label,
        "question": market.get("question"),
        "yes_token": str(yes_tok),
        "yes": yes_px,
        "outcomes": outcomes,
        "prices": prices,
        "best_bid": market.get("bestBid"),
        "best_ask": market.get("bestAsk"),
        "spread": rung_spread(market),
        "enable_order_book": bool(market.get("enableOrderBook")),
        "accepting_orders": bool(market.get("acceptingOrders")),
        "actionable": actionable,
        "url": f"https://askgina.ai/prediction-markets/{mid}",
        "polymarket_url": f"https://polymarket.com/event/{event.get('slug')}",
        "condition_id": market.get("conditionId"),
    }


def fetch_monthly_crypto_events() -> list[dict]:
    """Page Gamma for monthly-tagged events; keep crypto LFT only."""
    events: list[dict] = []
    seen: set[str] = set()
    limit = 100
    for offset in range(0, 500, limit):
        qs = urllib.parse.urlencode(
            {
                "tag_id": TAG_MONTHLY,
                "active": "true",
                "closed": "false",
                "limit": str(limit),
                "offset": str(offset),
            }
        )
        try:
            data = http_json(f"{GAMMA}/events?{qs}")
        except Exception as exc:  # noqa: BLE001
            log(f"gamma_list_err offset={offset} {type(exc).__name__}")
            break
        if not isinstance(data, list) or not data:
            break
        for event in data:
            eid = str(event.get("id") or event.get("slug") or "")
            if not eid or eid in seen:
                continue
            if not event_is_monthly_crypto(event):
                continue
            seen.add(eid)
            events.append(event)
        if len(data) < limit:
            break
    return events


def resolve_ladder(*, max_spread: float, threshold: float) -> list[dict]:
    events = fetch_monthly_crypto_events()
    rungs: list[dict] = []
    for event in events:
        markets = event.get("markets") or []
        n_act = 0
        for m in markets:
            if not isinstance(m, dict):
                continue
            rung = parse_rung(m, event, max_spread=max_spread, threshold=threshold)
            if not rung:
                continue
            rungs.append(rung)
            if rung["actionable"]:
                n_act += 1
        log(
            f"event {event.get('slug')} markets={len(markets)} "
            f"actionable={n_act}"
        )
    return rungs


class WatchState:
    def __init__(self) -> None:
        self.by_token: dict[str, dict] = {}  # yes_token -> rung meta
        self.alerted: set[str] = load_alerted()
        self.last_price: dict[str, float] = {}
        self.last_market_ts = time.time()
        self.threshold = float(os.environ.get("THRESHOLD", "0.90"))
        self.stale_secs = float(os.environ.get("STALE_SECS", "5"))
        self.max_spread = float(os.environ.get("MAX_SPREAD", "0.05"))
        self.refresh_secs = float(os.environ.get("REFRESH_SECS", "90"))
        self.webhook_url = os.environ.get("WEBHOOK_URL")
        self.webhook_key = os.environ.get("WEBHOOK_KEY")

    def dedupe_key(self, meta: dict, side: str = "Yes") -> str:
        # Prefer market_id; also stable event:rung:side form
        mid = meta.get("market_id")
        if mid:
            return f"market:{mid}:{side}"
        return f"{meta.get('event_id')}:{meta.get('rung')}:{side}"

    async def emit(self, meta: dict, price: float, source: str) -> None:
        key = self.dedupe_key(meta, "Yes")
        if key in self.alerted:
            return
        if price < self.threshold:
            return
        self.alerted.add(key)
        save_alerted(self.alerted)
        payload = {
            "type": "crypto_monthly_price_hook",
            "slug": meta.get("event_slug"),
            "title": meta.get("event_title"),
            "rung": meta.get("rung"),
            "question": meta.get("question"),
            "url": meta.get("url"),
            "side": "Yes",
            "price": price,
            "spread": meta.get("spread"),
            "best_bid": meta.get("best_bid"),
            "best_ask": meta.get("best_ask"),
            "threshold": self.threshold,
            "max_spread": self.max_spread,
            "source": source,
            "market_id": meta.get("market_id"),
            "event_id": meta.get("event_id"),
            "condition_id": meta.get("condition_id"),
            "polymarket_url": meta.get("polymarket_url"),
            "ts": datetime.now(timezone.utc).isoformat(),
            "dedupe_key": key,
        }
        with QUEUE.open("a") as fh:
            fh.write(json.dumps(payload) + "\n")
        status = await asyncio.to_thread(
            post_webhook, self.webhook_url, self.webhook_key, payload
        )
        log(
            f"hook_{status} Yes={price:.4f} {meta.get('event_slug')} "
            f"rung={meta.get('rung')} mid={meta.get('market_id')}"
        )

    async def maybe_alert(self, token_id: str, price: float, source: str) -> None:
        token_id = str(token_id)
        meta = self.by_token.get(token_id)
        if not meta:
            return
        self.last_market_ts = time.time()
        self.last_price[token_id] = price
        # Track live ask-ish price on meta for logging
        meta = {**meta, "yes": price}
        self.by_token[token_id] = meta
        await self.emit(meta, price, source)


async def subscribe(ws, tokens: list[str]) -> None:
    if not tokens:
        return
    # CLOB accepts batches; chunk to stay polite
    for i in range(0, len(tokens), 80):
        chunk = tokens[i : i + 80]
        await ws.send(
            json.dumps(
                {
                    "assets_ids": chunk,
                    "type": "market",
                    "custom_feature_enabled": True,
                }
            )
        )


async def unsubscribe(ws, tokens: list[str]) -> None:
    if not tokens:
        return
    for i in range(0, len(tokens), 80):
        chunk = tokens[i : i + 80]
        await ws.send(json.dumps({"assets_ids": chunk, "operation": "unsubscribe"}))


async def ensure_ladder(ws, state: WatchState) -> None:
    old_tokens = set(state.by_token.keys())
    rungs = await asyncio.to_thread(
        resolve_ladder, max_spread=state.max_spread, threshold=state.threshold
    )
    new_map: dict[str, dict] = {}
    for rung in rungs:
        if not rung.get("actionable"):
            continue
        tok = str(rung["yes_token"])
        new_map[tok] = rung
        # Gamma seed / recheck alert path
        yes = rung.get("yes")
        if yes is not None:
            state.by_token[tok] = rung
            await state.maybe_alert(tok, float(yes), "gamma_seed")

    state.by_token = new_map
    new_tokens = set(new_map.keys())
    to_drop = list(old_tokens - new_tokens)
    to_add = list(new_tokens - old_tokens)
    if to_drop:
        await unsubscribe(ws, to_drop)
    if to_add:
        await subscribe(ws, to_add)
    elif not old_tokens and new_tokens:
        await subscribe(ws, list(new_tokens))
    elif old_tokens != new_tokens and new_tokens and not to_add:
        await subscribe(ws, list(new_tokens))

    state.last_market_ts = time.time()
    events = sorted({r.get("event_slug") for r in new_map.values()})
    log(
        f"watching yes_tokens={len(new_tokens)} events={len(events)} "
        f"max_spread={state.max_spread} threshold>={state.threshold}"
    )
    for slug in events:
        xs = [r for r in new_map.values() if r.get("event_slug") == slug]
        sample = ", ".join(
            f"{r.get('rung')}={float(r['yes']):.3f}"
            for r in sorted(xs, key=lambda z: float(z.get("yes") or 0), reverse=True)[:4]
            if r.get("yes") is not None
        )
        log(f"  {slug} n={len(xs)} top=[{sample}]")


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
            # Prefer ask for Yes buy-side signal; fall back to bid
            px = float(ba) if ba is not None else (float(bb) if bb is not None else None)
            if px is not None:
                meta = state.by_token.get(str(token))
                if meta is not None and bb is not None and ba is not None:
                    try:
                        meta["best_bid"] = float(bb)
                        meta["best_ask"] = float(ba)
                        meta["spread"] = float(ba) - float(bb)
                    except (TypeError, ValueError):
                        pass
                await state.maybe_alert(str(token), px, "best_bid_ask")
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
        changes = (
            msg.get("price_changes") or (msg.get("payload") or {}).get("priceChanges") or []
        )
        for ch in changes:
            token = ch.get("asset_id") or ch.get("tokenId") or ch.get("token_id")
            ba = ch.get("best_ask") or ch.get("bestAsk")
            bb = ch.get("best_bid") or ch.get("bestBid")
            px = ba or bb or ch.get("price")
            if token is not None and px is not None:
                await state.maybe_alert(str(token), float(px), "price_change")
        return


async def run() -> None:
    load_env()
    PID.write_text(str(os.getpid()))
    state = WatchState()
    if not state.webhook_url:
        log("WARN WEBHOOK_URL unset — will queue only")
    log(
        f"monthly crypto LFT threshold>={state.threshold} "
        f"max_spread={state.max_spread} refresh={state.refresh_secs}s "
        f"stale={state.stale_secs}s notify=immediate"
    )
    backoff = 1
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=None) as ws:
                log("ws_connected")
                backoff = 1
                state.by_token.clear()
                await ensure_ladder(ws, state)

                async def heartbeat():
                    while True:
                        await asyncio.sleep(10)
                        await ws.send("PING")

                async def refresh():
                    while True:
                        await asyncio.sleep(state.refresh_secs)
                        await ensure_ladder(ws, state)

                async def liveness():
                    while True:
                        await asyncio.sleep(5)
                        age = time.time() - state.last_market_ts
                        # Mute reconnect storms when nothing is actionable yet
                        if not state.by_token:
                            state.last_market_ts = time.time()
                            continue
                        if age > state.stale_secs:
                            log(f"stale_market {age:.0f}s — forcing reconnect")
                            await ws.close()
                            return

                hb = asyncio.create_task(heartbeat())
                ref = asyncio.create_task(refresh())
                live = asyncio.create_task(liveness())
                try:
                    async for raw in ws:
                        if isinstance(raw, bytes):
                            raw = raw.decode()
                        await handle_msg(state, raw)
                finally:
                    hb.cancel()
                    ref.cancel()
                    live.cancel()
        except Exception as exc:  # noqa: BLE001
            log(f"ws_err {type(exc).__name__}: {exc}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


def main() -> None:
    load_env()
    log("starting watch_monthly")
    try:
        asyncio.run(run())
    finally:
        if PID.exists():
            PID.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
