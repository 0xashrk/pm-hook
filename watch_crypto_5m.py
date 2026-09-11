#!/usr/bin/env python3
"""Polymarket crypto 5m up/down price hook — CLOB WS → webhook.

Assets: BTC / ETH / SOL / XRP. Alert when a side price >= THRESHOLD.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
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
ALERTED = OUT / "alerted_5m.json"
PID = ROOT / "watch_crypto_5m.pid"

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA = "https://gamma-api.polymarket.com"
UA = "Mozilla/5.0 (compatible; polymarket-price-hook/1.0)"

# series_id is informational; live resolve is via slug formula
ASSETS = {
    "btc": {"series_id": "10684", "slug_prefix": "btc-updown-5m"},
    "eth": {"series_id": "10683", "slug_prefix": "eth-updown-5m"},
    "sol": {"series_id": "10686", "slug_prefix": "sol-updown-5m"},
    "xrp": {"series_id": "10685", "slug_prefix": "xrp-updown-5m"},
}


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


def floor_5m(ts: int | None = None) -> int:
    ts = int(ts if ts is not None else time.time())
    return ts - (ts % 300)


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
    if len(items) > 500:
        items = items[-500:]
    ALERTED.write_text(json.dumps(items))


def resolve_asset_5m(asset: str, cfg: dict) -> dict | None:
    start = floor_5m()
    for candidate in (start, start + 300, start - 300):
        slug = f"{cfg['slug_prefix']}-{candidate}"
        try:
            data = http_json(f"{GAMMA}/events?slug={slug}")
        except Exception as exc:  # noqa: BLE001
            log(f"resolve_err {slug} {type(exc).__name__}")
            continue
        if not isinstance(data, list) or not data:
            continue
        event = data[0]
        markets = event.get("markets") or []
        if not markets:
            continue
        m = markets[0]
        try:
            outcomes = json.loads(m.get("outcomes") or "[]")
            prices = [float(x) for x in json.loads(m.get("outcomePrices") or "[]")]
            tokens = json.loads(m.get("clobTokenIds") or "[]")
        except (json.JSONDecodeError, TypeError, ValueError):
            continue
        if len(outcomes) != 2 or len(tokens) != 2:
            continue
        end = event.get("endDate")
        start_t = event.get("startTime") or event.get("startDate")
        now = datetime.now(timezone.utc)
        try:
            end_dt = datetime.fromisoformat(end.replace("Z", "+00:00")) if end else None
            start_dt = (
                datetime.fromisoformat(start_t.replace("Z", "+00:00")) if start_t else None
            )
        except Exception:
            end_dt = start_dt = None
        live = bool(
            start_dt and end_dt and start_dt <= now < end_dt and not event.get("closed")
        )
        if candidate == start and not live and end_dt and now >= end_dt:
            continue
        token_side = {str(tokens[i]): outcomes[i] for i in range(2)}
        mid = m.get("id")
        return {
            "asset": asset,
            "series_id": cfg["series_id"],
            "slug": event.get("slug") or slug,
            "title": event.get("title"),
            "event_id": event.get("id"),
            "market_id": mid,
            "condition_id": m.get("conditionId"),
            "start": start_t,
            "end": end,
            "outcomes": outcomes,
            "prices": dict(zip(outcomes, prices)) if len(prices) == 2 else {},
            "tokens": [str(t) for t in tokens],
            "token_side": token_side,
            "live": live,
            "url": f"https://askgina.ai/prediction-markets/{mid}",
            "polymarket_url": f"https://polymarket.com/event/{event.get('slug') or slug}",
        }
    return None


class WatchState:
    def __init__(self) -> None:
        self.by_asset: dict[str, dict] = {}
        self.token_index: dict[str, tuple[str, str]] = {}  # token -> (asset, side)
        self.alerted: set[str] = load_alerted()
        self.last_price: dict[str, float] = {}
        self.last_market_ts = time.time()
        self.threshold = float(os.environ.get("THRESHOLD", "0.90"))
        self.stale_secs = float(os.environ.get("STALE_SECS", "5"))
        self.webhook_url = os.environ.get("WEBHOOK_URL")
        self.webhook_key = os.environ.get("WEBHOOK_KEY")

    def window_key(self, asset: str, slug: str, side: str) -> str:
        return f"{asset}:{slug}:{side}"

    def rebuild_index(self) -> None:
        idx: dict[str, tuple[str, str]] = {}
        for asset, meta in self.by_asset.items():
            for tok, side in meta["token_side"].items():
                idx[str(tok)] = (asset, side)
        self.token_index = idx

    async def emit(self, asset: str, meta: dict, side: str, price: float, source: str) -> None:
        key = self.window_key(asset, meta["slug"], side)
        if key in self.alerted:
            return
        if price < self.threshold:
            return
        self.alerted.add(key)
        save_alerted(self.alerted)
        other = [o for o in meta["outcomes"] if o != side]
        other_price = None
        if other:
            for tok, s in meta["token_side"].items():
                if s == other[0] and tok in self.last_price:
                    other_price = self.last_price[tok]
                    break
            if other_price is None and meta.get("prices"):
                other_price = meta["prices"].get(other[0])
        payload = {
            "type": "crypto_5m_price_hook",
            "asset": asset.upper(),
            "series_id": meta["series_id"],
            "slug": meta["slug"],
            "title": meta["title"],
            "url": meta["url"],
            "side": side,
            "price": price,
            "other_side": other[0] if other else None,
            "other_price": other_price,
            "threshold": self.threshold,
            "source": source,
            "market_id": meta["market_id"],
            "event_id": meta["event_id"],
            "start": meta["start"],
            "end": meta["end"],
            "ts": datetime.now(timezone.utc).isoformat(),
            "dedupe_key": key,
        }
        with QUEUE.open("a") as fh:
            fh.write(json.dumps(payload) + "\n")
        status = await asyncio.to_thread(
            post_webhook, self.webhook_url, self.webhook_key, payload
        )
        log(f"hook_{status} {asset.upper()} {side}={price:.4f} {meta['slug']}")

    async def maybe_alert(self, token_id: str, price: float, source: str) -> None:
        token_id = str(token_id)
        hit = self.token_index.get(token_id)
        if not hit:
            return
        asset, side = hit
        meta = self.by_asset.get(asset)
        if not meta:
            return
        self.last_market_ts = time.time()
        self.last_price[token_id] = price
        await self.emit(asset, meta, side, price, source)


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


async def ensure_windows(ws, state: WatchState) -> None:
    old_tokens = set(state.token_index.keys())
    changed = False
    for asset, cfg in ASSETS.items():
        meta = await asyncio.to_thread(resolve_asset_5m, asset, cfg)
        if not meta:
            log(f"no_active_5m {asset}")
            continue
        old = state.by_asset.get(asset)
        same = bool(old and old["slug"] == meta["slug"] and old["tokens"] == meta["tokens"])
        if not same:
            if old:
                log(f"roll {asset} {old['slug']} → {meta['slug']}")
            else:
                log(f"sub {asset} {meta['slug']} live={meta['live']}")
            state.by_asset[asset] = meta
            changed = True
        else:
            state.by_asset[asset] = meta
        prices = meta.get("prices") or {}
        if prices:
            log(
                f"gamma {asset.upper()} {meta['slug']} "
                + " ".join(f"{s}={float(px):.4f}" for s, px in prices.items())
            )
        for side, px in prices.items():
            tok = next((t for t, s in meta["token_side"].items() if s == side), None)
            if tok:
                state.token_index[str(tok)] = (asset, side)
                await state.maybe_alert(
                    tok, float(px), "gamma_seed" if not same else "gamma_recheck"
                )

    state.rebuild_index()
    new_tokens = set(state.token_index.keys())
    if changed or old_tokens != new_tokens:
        to_drop = list(old_tokens - new_tokens)
        to_add = list(new_tokens - old_tokens)
        if to_drop:
            await unsubscribe(ws, to_drop)
        if to_add:
            await subscribe(ws, to_add)
        elif changed and new_tokens:
            await subscribe(ws, list(new_tokens))
        state.last_market_ts = time.time()
        log(f"watching tokens={len(new_tokens)} assets={sorted(state.by_asset)}")


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
        changes = msg.get("price_changes") or (msg.get("payload") or {}).get("priceChanges") or []
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
        f"assets={list(ASSETS)} threshold>={state.threshold} "
        f"stale={state.stale_secs}s notify=immediate"
    )
    backoff = 1
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=None) as ws:
                log("ws_connected")
                backoff = 1
                state.by_asset.clear()
                state.token_index.clear()
                await ensure_windows(ws, state)

                async def heartbeat():
                    while True:
                        await asyncio.sleep(10)
                        await ws.send("PING")

                async def rollover():
                    while True:
                        await asyncio.sleep(15)
                        await ensure_windows(ws, state)

                async def liveness():
                    while True:
                        await asyncio.sleep(5)
                        age = time.time() - state.last_market_ts
                        if age > state.stale_secs:
                            log(f"stale_market {age:.0f}s — forcing reconnect")
                            await ws.close()
                            return

                hb = asyncio.create_task(heartbeat())
                roll = asyncio.create_task(rollover())
                live = asyncio.create_task(liveness())
                try:
                    async for raw in ws:
                        if isinstance(raw, bytes):
                            raw = raw.decode()
                        await handle_msg(state, raw)
                finally:
                    hb.cancel()
                    roll.cancel()
                    live.cancel()
        except Exception as exc:  # noqa: BLE001
            log(f"ws_err {type(exc).__name__}: {exc}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


def main() -> None:
    load_env()
    log("starting watch_crypto_5m")
    try:
        asyncio.run(run())
    finally:
        if PID.exists():
            PID.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
