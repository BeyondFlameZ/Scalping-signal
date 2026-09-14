# BYBIT PAPER BOT — ONE-STEP DEPLOY

This is the runnable phone-friendly paper version.

It uses only Bybit public market data. Bybit's public linear WebSocket endpoint is `wss://stream.bybit.com/v5/public/linear`, and public topics do not require authentication. The trade stream is real-time. Official docs: https://bybit-exchange.github.io/docs/v5/ws/connect

IMPORTANT:
- No real orders.
- No API keys.
- Virtual starting balance: $10,000.
- 2% risk proxy.
- Max 5 positions.
- Top 300 ranking, Top 50 live deep scan.
- This is a research/paper engine, NOT a claim of profitability.

REPLIT:
1. Create a Python app.
2. Upload these files.
3. Install requirements.txt.
4. Run `uvicorn main:app --host 0.0.0.0 --port 8080`
5. Open the generated web preview URL.
6. For 24/7 hosting use a Replit Deployment rather than relying on the editor session.

The dashboard should show WebSocket CONNECTED and live signals. If it says OFFLINE, inspect the error in `/api/status`.
