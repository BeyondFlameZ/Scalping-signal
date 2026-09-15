# Bybit Micro Scalper v11

Paper-only event-driven research bot.

## Strategy (unchanged)
Three concrete events on Bybit USDT perps; score is a filter, not a trigger:
- liquidity sweep + reversal
- absorption + reversal
- impulse + pullback + continuation

No private API and no real orders.

## v11 fixes
- **Order book**: maintained from snapshot + delta (size 0 = remove).
  Best bid/ask, spread and top-10 imbalance are now correct instead of
  being read off arbitrary delta levels.
- **Threading**: signal evaluation and position management run on a
  dedicated worker thread. The WS callback only ingests data, so it never
  falls behind under load.
- **Clock**: one exchange-aligned clock (`ex_now`) for trade windows,
  cooldown and position age — no local/exchange skew.
- **Time stop**: `TIME_STOP_SEC` is a hard hold cap regardless of PnL.
- **Leverage**: `LEVERAGE` now caps total gross notional across open
  positions (previously dead config).
- **Session loss**: `MAX_SESSION_LOSS_PCT` sets an explicit, logged,
  dashboard-visible HALTED state instead of silently blocking entries.
- **Thread safety**: shared state behind a single lock; eval snapshots
  data under the lock and computes outside it.
- **Universe**: subscribes only to the symbols it monitors (`MONITOR_N`),
  no wasted streams.
- Fresh book snapshot forced after any WS reconnect; stale book (older
  than `MAX_BOOK_AGE`) blocks entries.

Earlier v11 tuning kept: reversal exit needs >=20s hold, >=0.10% adverse
move and strong opposite CVD; cooldown 120s; event score default 76.
