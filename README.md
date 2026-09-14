# Bybit Micro Scalper v9

Paper-only research bot for Railway.

Key changes from v8:
- 0.5% risk per trade instead of 2%
- 0.60% TP / 0.25% SL
- 10% aggregate open-risk cap
- fee + slippage model
- 3-of-4 microstructure confluence: CVD, order book, 5s momentum, flow impulse
- spread and expected-move filters
- anti-late-chase filter
- reversal exit
- 180s dead-trade timeout
- separate LONG/SHORT and exit-reason statistics
- session loss guard at 10%

No real orders are sent.
