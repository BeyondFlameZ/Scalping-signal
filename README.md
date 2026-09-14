# Bybit Micro Scalper v10

Paper-only event-driven scalper.

Unlike v9, score alone cannot open a trade. A concrete market event is required:
- SWEEP_REVERSAL
- ABSORPTION
- PULLBACK_CONT

The dashboard reports performance separately by event type so the strategy can be rejected or refined based on evidence rather than arbitrary score tuning.

No private Bybit API or real order endpoint is used.
