# Bybit Paper Bot Signal V2

30 simultaneous positions are intentional for the stress test.

The signal architecture is fixed:
publicTrade -> aggregate signed flow -> completed 15-minute bar -> CVD score -> one decision.

The old prototype made a decision on every trade tick, creating signal spam. This build makes decisions only after a 15-minute microstructure bar closes.

Railway variables:
START_BALANCE=10000
RISK_PCT=0.02
MAX_POSITIONS=30
TOP_N=300
DEEP_N=50
PAPER_ONLY=true

No API keys. No real orders.

Run command is supplied by Dockerfile.
