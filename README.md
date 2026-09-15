# Bybit Micro Scalper v11

Paper-only event-driven research bot.

v11 fixes a critical v10 bug where event name and score were swapped.
It also removes the overly sensitive reversal churn:
- reversal exit requires at least 20 seconds in the trade;
- at least 0.10% adverse price movement;
- strong opposite CVD;
- stronger opposite-flow gap;
- cooldown increased to 120 seconds;
- event score default raised to 76;
- dashboard event statistics use stable string keys.

No private API and no real orders.
