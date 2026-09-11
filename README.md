# polymarket-price-hook

Polymarket CLOB market WebSocket → webhook price hooks.

Two watchers:

| Script | What it watches |
|--------|-----------------|
| `watch_monthly.py` | Monthly crypto ladder (LFT) events — multi-rung Yes tokens |
| `watch_crypto_5m.py` | BTC / ETH / SOL / XRP 5-minute up/down windows |

Both connect to `wss://ws-subscriptions-clob.polymarket.com/ws/market`, send `PING` ~every 10s, force reconnect when market traffic goes stale (`STALE_SECS`), append alerts to `out/price-hook-queue.jsonl`, and **immediately** `POST` JSON to `WEBHOOK_URL` with `Authorization: Bearer <WEBHOOK_KEY>`.

Gina deep link in every payload: `https://askgina.ai/prediction-markets/<market_id>`.

## Setup

```bash
cd polymarket-price-hook
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# edit .env — set WEBHOOK_URL and WEBHOOK_KEY
```

Dry-run (queue only, no webhook):

```bash
# leave WEBHOOK_URL empty
.venv/bin/python watch_monthly.py
# or
.venv/bin/python watch_crypto_5m.py
```

Keep alive (requires `.env`):

```bash
# monthly (default)
./scripts/supervise.sh

# 5m instead
WATCH_SCRIPT=watch_crypto_5m.py ./scripts/supervise.sh
```

Do **not** commit `.env`. Copy `.env.example` only.

## Environment

| Variable | Default | Meaning |
|----------|---------|---------|
| `WEBHOOK_URL` | _(empty)_ | If unset, alerts are queued only |
| `WEBHOOK_KEY` | _(empty)_ | Bearer token for webhook |
| `THRESHOLD` | `0.90` | Alert when watched price ≥ this |
| `STALE_SECS` | `5` | Reconnect if no market msgs for N seconds |
| `MAX_SPREAD` | `0.05` | Monthly: prefer / require `best_ask − best_bid` ≤ this for subscribe |
| `REFRESH_SECS` | `90` | Monthly: Gamma re-resolve interval (use 60–120) |
| `WATCH_SCRIPT` | `watch_monthly.py` | For `scripts/supervise.sh` only |

## Monthly vs 5m

### `watch_crypto_5m.py`

- Resolves the live window via slug formula: `{btc,eth,sol,xrp}-updown-5m-<floor_5m_unix>` (tries current / +300 / −300).
- Subscribes to both outcome token ids (Up/Down or Yes/No as published).
- Rolls every ~15s; alerts once per `asset:slug:side` (persisted in `out/alerted_5m.json`).

### `watch_monthly.py`

**Resolve**

1. Page Gamma `GET /events?tag_id=102144&active=true&closed=false` (monthly tag).
2. Keep events that also have `crypto` or `crypto-prices` tags.
3. **Always drop** tag `yearly`, equities/stocks/finance tags, non-crypto tickers (e.g. ABNB), and yearly title/slug heuristics (`before-2027`, bare `in-2026` without a month name).
4. Each child market is a **rung** (`groupItemTitle` like `↑ 80000`).
5. Subscribe to **Yes** `clobTokenIds` for **actionable** rungs only.

**Actionable guardrails**

- Skip Yes ≤ `0.02` or ≥ `0.98`.
- Require CLOB book (`enableOrderBook` + accepting orders).
- Prefer spread ≤ `MAX_SPREAD` (still watch threshold-zone rungs so alerts can fire).
- Prefer Yes in `0.15–0.85` for watching; **still alert** if Yes ≥ `THRESHOLD`.

Re-resolves every `REFRESH_SECS` (default 90). Dedupe key: `market:<market_id>:Yes` (persisted in `out/alerted_monthly.json`).

## Outputs

```
out/
  price-hook-queue.jsonl   # every alert payload
  alerted_5m.json          # 5m dedupe set
  alerted_monthly.json     # monthly dedupe set
  watch_monthly.log        # when supervised
  watch_crypto_5m.log
  supervise.log
```

## WebSocket notes

- Subscribe payload: `{"assets_ids":[...],"type":"market","custom_feature_enabled":true}`
- App-level `PING` every ~10s (`ping_interval=None` on the client)
- Handled event types: `best_bid_ask`, `last_trade_price`, `price_change`
- HTTP calls send a descriptive `User-Agent`

## License

Internal / Team Gina — no secrets in this tree.
