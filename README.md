# polymarket-price-hook

Polymarket CLOB market WebSocket → webhook price hooks.

Three watchers:

| Script | What it watches |
|--------|-----------------|
| `watch_monthly.py` | Monthly crypto ladder (LFT) events — multi-rung Yes tokens |
| `watch_crypto_5m.py` | BTC / ETH / SOL / XRP 5-minute up/down windows |
| `watch_soccer.py` | EPL / La Liga / MLS match moneylines (3-way Yes) |

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
# or soccer
.venv/bin/python watch_soccer.py
```

Keep alive (requires `.env`):

```bash
# monthly (default)
./scripts/supervise.sh

# 5m instead
WATCH_SCRIPT=watch_crypto_5m.py ./scripts/supervise.sh

# soccer (EPL + La Liga + MLS)
WATCH_SCRIPT=watch_soccer.py ./scripts/supervise.sh
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
| `LEAGUES` | `epl,lal,mls` | Soccer: which leagues to resolve |
| `HORIZON_HOURS` | `48` | Soccer: upcoming kickoffs to include |
| `MATCH_DURATION_HOURS` | `3` | Soccer: live window after kickoff (Gamma `endDate` ≈ KO) |
| `DELTA_ALERT_CENTS` | `0` | Soccer: alert when mark moves ≥ N¢ (0 = off) |
| `POSITION_SLUGS` | _(empty)_ | Soccer: comma event/market slugs always watched |
| `WATCH_MARKET_IDS` | _(empty)_ | Soccer: comma Gamma market ids always watched |
| `MIN_YES` / `MAX_YES` | `0.02` / `0.98` | Soccer: skip extreme Yes for non-position markets |

## Monthly vs 5m vs soccer

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


### `watch_soccer.py`

**Resolve**

1. Gamma `GET /events?series_id={10188|10193|10189}&active=true&closed=false` (EPL / La Liga / MLS — series ids from `GET /sports`).
2. Keep **matchday** slugs only: `epl|lal|mls-<home>-<away>-YYYY-MM-DD` (drops futures / awards).
3. Keep kickoffs inside `HORIZON_HOURS`, plus in-play through `MATCH_DURATION_HOURS` after KO.
4. For each event, subscribe to **Yes** CLOB tokens for all `sportsMarketType=moneyline` legs (home / draw / away).

**Alerts**

- Fire when Yes ≥ `THRESHOLD` (deduped per market).
- Optional `DELTA_ALERT_CENTS` for mark moves (useful for open positions).
- `POSITION_SLUGS` / `WATCH_MARKET_IDS` force-watch those markets even outside the extreme-price skip band.

Payload `type`: `soccer_moneyline_price_hook` (includes `league`, `event_slug`, `market_slug`, Gina `url`).

Match Edge example:

```bash
POSITION_SLUGS=epl-sun-ars-2026-09-12
DELTA_ALERT_CENTS=3
THRESHOLD=0.90
WATCH_SCRIPT=watch_soccer.py ./scripts/supervise.sh
```

## Outputs

```
out/
  price-hook-queue.jsonl   # every alert payload
  alerted_5m.json          # 5m dedupe set
  alerted_monthly.json     # monthly dedupe set
  alerted_soccer.json      # soccer threshold dedupe
  last_mark_soccer.json    # soccer delta baseline
  watch_monthly.log        # when supervised
  watch_crypto_5m.log
  watch_soccer.log
  supervise.log
```

## WebSocket notes

- Subscribe payload: `{"assets_ids":[...],"type":"market","custom_feature_enabled":true}`
- App-level `PING` every ~10s (`ping_interval=None` on the client)
- Handled event types: `best_bid_ask`, `last_trade_price`, `price_change`
- HTTP calls send a descriptive `User-Agent`

## License

Internal / Team Gina — no secrets in this tree.
