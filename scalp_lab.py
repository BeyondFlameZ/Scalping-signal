#!/usr/bin/env python3
"""
scalp_lab.py — исследовательский стенд для скальп-стратегий на Bybit USDT-перпах.

Python 3.9+, ТОЛЬКО стандартная библиотека. Ни pip install, ни API-ключей.
Данные — публичный эндпоинт Bybit v5 /market/kline, кэшируются в ./data/*.csv.

Что делает:
  1. Тянет 1m (или 3m/5m) свечи по списку пар и кэширует их на диск.
  2. Печатает "пол издержек": сколько вообще двигается пара за свечу в bps,
     против того, сколько стоит круг по комиссии. Если движение меньше
     издержек — дальше можно не смотреть.
  3. Гоняет 10 скальп-стратегий + случайный бейзлайн одним движком с
     одинаковой моделью исполнения и издержек.
  4. Делит выборку 70/30 на in-sample / out-of-sample и печатает обе части
     отдельно. Стратегия, которая живёт только в IS, — это подгонка.
  5. Считает на сделку: gross bps, cost bps, net bps, t-статистику.
     Ключевая цифра — net bps и t-stat, а не win rate.

Модель исполнения (осознанно пессимистичная):
  - Сигнал считается на ЗАКРЫТОЙ свече i, вход по открытию свечи i+1.
    Заглядывания в будущее нет.
  - Стоп и цель проверяются внутри свечи. Если в одной свече задеты оба —
    считается, что сработал СТОП.
  - Тайм-стоп: выход по закрытию через N свечей.
  - Одна позиция на символ за раз, плюс кулдаун после выхода.
  - Издержки: комиссия на вход + на выход + проскальзывание, всё в bps.

Чего этот стенд НЕ умеет и умеет не будет на свечах:
  - order book imbalance / order flow imbalance — нужен L2-стакан и тиковые
    сделки, свечи такого не содержат. Это отдельный коллектор.
  - реальную вероятность исполнения лимитника. Режим --entry maker считает
    комиссию мейкера, но исполнение допускает всегда. Это ОПТИМИСТИЧНО,
    трактуй такие цифры как верхнюю границу.

Запуск:
    python3 scalp_lab.py --days 60
    python3 scalp_lab.py --symbols BTCUSDT,ETHUSDT,SOLUSDT --days 90 --interval 1
    python3 scalp_lab.py --costs-only          # только "пол издержек"
    python3 scalp_lab.py --strategy vwap_revert --verbose
    python3 scalp_lab.py --synthetic           # прогон на сгенерированных
                                               # данных, проверка самого движка
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import namedtuple
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Конфиг издержек — правь под свой VIP-уровень
# ---------------------------------------------------------------------------

TAKER_FEE_BPS = 5.5      # Bybit USDT-перп, обычный уровень: 0.055%
MAKER_FEE_BPS = 2.0      # 0.02%
SLIPPAGE_BPS = 1.5       # на сторону, рыночным ордером на ликвидной паре
COOLDOWN_BARS = 2        # сколько свечей не входим после выхода

DEFAULT_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT",
    "DOGEUSDT", "BNBUSDT", "AVAXUSDT", "LINKUSDT",
]

BASE_URL = "https://api.bybit.com/v5/market/kline"
DATA_DIR = "data"

Bar = namedtuple("Bar", "ts o h l c v")
Setup = namedtuple("Setup", "direction stop_price tp_r max_bars tag")
Trade = namedtuple("Trade", "symbol strat direction entry_i exit_i entry sl tp "
                            "exit_price bars_held gross_bps net_bps r_net reason")


# ---------------------------------------------------------------------------
# Загрузка данных
# ---------------------------------------------------------------------------

def http_get_json(url: str, retries: int = 4) -> dict:
    last = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "scalp-lab/1.0"})
            with urllib.request.urlopen(req, timeout=20) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:          # noqa: BLE001 — сеть, ловим всё
            last = e
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"запрос не прошёл после {retries} попыток: {last}")


# ---------------------------------------------------------------------------
# Доставка в телеграм
# ---------------------------------------------------------------------------

TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "")
TG_LIMIT = 3800          # запас под лимит телеграма в 4096 символов


def tg_send(text: str) -> bool:
    """Отправка в телеграм, только stdlib. Длинное режется по строкам."""
    if not (TG_TOKEN and TG_CHAT):
        return False
    chunks, cur = [], ""
    for line in text.split("\n"):
        if len(cur) + len(line) + 1 > TG_LIMIT:
            chunks.append(cur)
            cur = ""
        cur += line + "\n"
    if cur.strip():
        chunks.append(cur)

    ok = True
    for chunk in chunks:
        data = urllib.parse.urlencode({
            "chat_id": TG_CHAT,
            "text": f"<pre>{chunk}</pre>",
            "parse_mode": "HTML",
            "disable_web_page_preview": "true",
        }).encode()
        try:
            req = urllib.request.Request(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data=data)
            with urllib.request.urlopen(req, timeout=15) as r:
                if not json.loads(r.read().decode()).get("ok"):
                    ok = False
        except Exception as e:                       # noqa: BLE001
            print(f"[tg] не отправилось: {e}", file=sys.stderr)
            ok = False
        time.sleep(0.4)
    return ok


def emit(text: str, buf: list) -> None:
    """Печатает в лог и копит для отправки одним куском."""
    print(text)
    buf.append(text)


def fetch_klines(symbol: str, interval: str, days: int) -> list:
    """Тянет свечи назад от текущего момента. Bybit отдаёт максимум 1000 за раз."""
    per_bar_ms = int(interval) * 60_000
    need = int(days * 24 * 60 / int(interval))
    end_ms = int(time.time() * 1000)
    out = {}

    while len(out) < need:
        params = urllib.parse.urlencode({
            "category": "linear",
            "symbol": symbol,
            "interval": interval,
            "end": end_ms,
            "limit": 1000,
        })
        data = http_get_json(f"{BASE_URL}?{params}")
        if data.get("retCode") != 0:
            raise RuntimeError(f"{symbol}: Bybit вернул {data.get('retMsg')}")
        rows = data.get("result", {}).get("list", [])
        if not rows:
            break
        for r in rows:
            ts = int(r[0])
            out[ts] = Bar(ts, float(r[1]), float(r[2]), float(r[3]),
                          float(r[4]), float(r[5]))
        oldest = min(int(r[0]) for r in rows)
        if oldest >= end_ms:
            break
        end_ms = oldest - per_bar_ms
        time.sleep(0.12)                # вежливо к рейт-лимиту
        print(f"\r  {symbol}: {len(out)}/{need} свечей", end="", file=sys.stderr)

    print(f"\r  {symbol}: {len(out)} свечей загружено          ", file=sys.stderr)
    return [out[k] for k in sorted(out)]


def cache_path(symbol: str, interval: str) -> str:
    return os.path.join(DATA_DIR, f"{symbol}_{interval}m.csv")


def save_bars(path: str, bars: list) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts", "open", "high", "low", "close", "volume"])
        for b in bars:
            w.writerow([b.ts, b.o, b.h, b.l, b.c, b.v])


def load_bars(path: str) -> list:
    bars = []
    with open(path) as f:
        for row in csv.DictReader(f):
            bars.append(Bar(int(row["ts"]), float(row["open"]), float(row["high"]),
                            float(row["low"]), float(row["close"]), float(row["volume"])))
    return bars


def get_bars(symbol: str, interval: str, days: int, refresh: bool) -> list:
    path = cache_path(symbol, interval)
    if os.path.exists(path) and not refresh:
        bars = load_bars(path)
        need = int(days * 24 * 60 / int(interval))
        stale_min = (time.time() * 1000 - bars[-1].ts) / 60_000 if bars else 1e9
        if len(bars) >= need * 0.9 and stale_min < 120:
            print(f"  {symbol}: {len(bars)} свечей из кэша", file=sys.stderr)
            return bars[-need:]
        if bars:
            print(f"  {symbol}: кэш протух на {stale_min:.0f} мин, качаю заново",
                  file=sys.stderr)
    bars = fetch_klines(symbol, interval, days)
    save_bars(path, bars)
    return bars


def synthetic_bars(n: int = 40000, seed: int = 7) -> list:
    """Данные для проверки самого движка: GBM с меняющимися режимами волатильности."""
    rng = random.Random(seed)
    price, vol, bars, ts = 100.0, 0.0008, [], 1_700_000_000_000
    for i in range(n):
        if i % 500 == 0:
            vol = rng.uniform(0.0003, 0.0025)
        drift = rng.gauss(0, vol)
        o = price
        c = o * (1 + drift)
        wick = abs(rng.gauss(0, vol)) * o
        h, l = max(o, c) + wick, min(o, c) - wick
        bars.append(Bar(ts + i * 60_000, o, h, max(l, 0.01), c, rng.uniform(50, 500)))
        price = c
    return bars


# ---------------------------------------------------------------------------
# Индикаторы (списки той же длины, None пока не прогрелись)
# ---------------------------------------------------------------------------

def ema(vals: list, period: int) -> list:
    out, k, prev = [None] * len(vals), 2.0 / (period + 1), None
    for i, v in enumerate(vals):
        if i < period - 1:
            continue
        if prev is None:
            prev = sum(vals[i - period + 1:i + 1]) / period
        else:
            prev = v * k + prev * (1 - k)
        out[i] = prev
    return out


def sma(vals: list, period: int) -> list:
    out, run = [None] * len(vals), 0.0
    for i, v in enumerate(vals):
        run += v
        if i >= period:
            run -= vals[i - period]
        if i >= period - 1:
            out[i] = run / period
    return out


def rolling_std(vals: list, period: int) -> list:
    out = [None] * len(vals)
    for i in range(period - 1, len(vals)):
        win = vals[i - period + 1:i + 1]
        m = sum(win) / period
        out[i] = math.sqrt(sum((x - m) ** 2 for x in win) / period)
    return out


def atr(bars: list, period: int = 14) -> list:
    trs = [None] * len(bars)
    for i in range(1, len(bars)):
        b, p = bars[i], bars[i - 1]
        trs[i] = max(b.h - b.l, abs(b.h - p.c), abs(b.l - p.c))
    out, prev = [None] * len(bars), None
    for i in range(period, len(bars)):
        if prev is None:
            prev = sum(trs[1:period + 1]) / period
        else:
            prev = (prev * (period - 1) + trs[i]) / period
        out[i] = prev
    return out


def rsi(vals: list, period: int = 7) -> list:
    out = [None] * len(vals)
    gains = losses = 0.0
    for i in range(1, len(vals)):
        d = vals[i] - vals[i - 1]
        g, l = max(d, 0.0), max(-d, 0.0)
        if i <= period:
            gains += g
            losses += l
            if i == period:
                ag, al = gains / period, losses / period
                out[i] = 100.0 if al == 0 else 100 - 100 / (1 + ag / al)
        else:
            ag = (ag * (period - 1) + g) / period
            al = (al * (period - 1) + l) / period
            out[i] = 100.0 if al == 0 else 100 - 100 / (1 + ag / al)
    return out


def daily_vwap(bars: list) -> list:
    """VWAP с якорем на UTC-сутки — так им пользуются на практике."""
    out, pv, vv, day = [None] * len(bars), 0.0, 0.0, None
    for i, b in enumerate(bars):
        d = b.ts // 86_400_000
        if d != day:
            day, pv, vv = d, 0.0, 0.0
        typ = (b.h + b.l + b.c) / 3
        pv += typ * b.v
        vv += b.v
        out[i] = pv / vv if vv > 0 else b.c
    return out


class Ctx:
    """Свечи + все предрассчитанные индикаторы. Один объект на символ."""

    def __init__(self, symbol: str, bars: list):
        self.symbol = symbol
        self.bars = bars
        closes = [b.c for b in bars]
        vols = [b.v for b in bars]
        self.close = closes
        self.ema9 = ema(closes, 9)
        self.ema21 = ema(closes, 21)
        self.ema50 = ema(closes, 50)
        self.sma20 = sma(closes, 20)
        self.std20 = rolling_std(closes, 20)
        self.atr14 = atr(bars, 14)
        self.rsi7 = rsi(closes, 7)
        self.vwap = daily_vwap(bars)
        self.vol_sma20 = sma(vols, 20)
        self.warmup = 60

    def ready(self, i: int) -> bool:
        return (i >= self.warmup
                and self.atr14[i] is not None and self.atr14[i] > 0
                and self.std20[i] is not None
                and self.vol_sma20[i] is not None and self.vol_sma20[i] > 0)


# ---------------------------------------------------------------------------
# Стратегии. Каждая: (ctx, i) -> Setup | None. Сигнал на закрытой свече i.
# ---------------------------------------------------------------------------

def s_ema_cross(ctx, i):
    """Классика: пересечение EMA9/EMA21. Базовый трендовый скальп."""
    if ctx.ema9[i - 1] is None or ctx.ema21[i - 1] is None:
        return None
    a, b = ctx.ema9[i], ctx.ema21[i]
    pa, pb = ctx.ema9[i - 1], ctx.ema21[i - 1]
    bar, a14 = ctx.bars[i], ctx.atr14[i]
    if pa <= pb and a > b:
        return Setup(1, bar.c - 1.2 * a14, 1.5, 30, "ema_cross_long")
    if pa >= pb and a < b:
        return Setup(-1, bar.c + 1.2 * a14, 1.5, 30, "ema_cross_short")
    return None


def s_vwap_revert(ctx, i):
    """Отклонение от дневного VWAP на k*ATR — вход на возврат к нему."""
    bar, a14, vw = ctx.bars[i], ctx.atr14[i], ctx.vwap[i]
    dev = (bar.c - vw) / a14
    if dev < -2.0 and bar.c > ctx.bars[i - 1].c:      # ушли вниз и уже отскочили
        return Setup(1, bar.c - 1.0 * a14, 1.8, 45, "vwap_revert_long")
    if dev > 2.0 and bar.c < ctx.bars[i - 1].c:
        return Setup(-1, bar.c + 1.0 * a14, 1.8, 45, "vwap_revert_short")
    return None


def s_bb_fade(ctx, i):
    """Закрытие за 2-сигма полосой Боллинджера — фейдим обратно к середине."""
    bar, m, sd, a14 = ctx.bars[i], ctx.sma20[i], ctx.std20[i], ctx.atr14[i]
    if sd <= 0:
        return None
    if bar.c < m - 2.0 * sd:
        return Setup(1, bar.c - 1.0 * a14, 1.5, 30, "bb_fade_long")
    if bar.c > m + 2.0 * sd:
        return Setup(-1, bar.c + 1.0 * a14, 1.5, 30, "bb_fade_short")
    return None


def s_rsi_fade(ctx, i):
    """RSI(7) в экстремуме. На минутках RSI-14 слишком медленный."""
    r, bar, a14 = ctx.rsi7[i], ctx.bars[i], ctx.atr14[i]
    if r is None:
        return None
    if r < 12:
        return Setup(1, bar.c - 1.0 * a14, 1.5, 25, "rsi_fade_long")
    if r > 88:
        return Setup(-1, bar.c + 1.0 * a14, 1.5, 25, "rsi_fade_short")
    return None


def s_spike_fade(ctx, i):
    """Свеча размахом > 3*ATR с телом в одну сторону — фейдим импульс."""
    bar, a14 = ctx.bars[i], ctx.atr14[i]
    rng_ = bar.h - bar.l
    if rng_ < 3.0 * a14:
        return None
    body = bar.c - bar.o
    if abs(body) < 0.5 * rng_:
        return None
    if body < 0:
        return Setup(1, bar.l - 0.3 * a14, 1.5, 20, "spike_fade_long")
    return Setup(-1, bar.h + 0.3 * a14, 1.5, 20, "spike_fade_short")


def s_spike_follow(ctx, i):
    """Тот же спайк, но в направлении импульса. Проверка: фейдить или ехать."""
    bar, a14 = ctx.bars[i], ctx.atr14[i]
    rng_ = bar.h - bar.l
    if rng_ < 3.0 * a14 or bar.v < 3.0 * ctx.vol_sma20[i]:
        return None
    body = bar.c - bar.o
    if abs(body) < 0.5 * rng_:
        return None
    if body > 0:
        return Setup(1, bar.c - 1.2 * a14, 1.5, 25, "spike_follow_long")
    return Setup(-1, bar.c + 1.2 * a14, 1.5, 25, "spike_follow_short")


def s_range_break(ctx, i):
    """Пробой экстремума последних 20 свечей с подтверждением объёмом."""
    bar, a14 = ctx.bars[i], ctx.atr14[i]
    win = ctx.bars[i - 20:i]
    hi, lo = max(b.h for b in win), min(b.l for b in win)
    if bar.v < 2.0 * ctx.vol_sma20[i]:
        return None
    if bar.c > hi:
        return Setup(1, hi - 0.3 * a14, 1.5, 40, "range_break_long")
    if bar.c < lo:
        return Setup(-1, lo + 0.3 * a14, 1.5, 40, "range_break_short")
    return None


def s_momentum_run(ctx, i):
    """Три подряд свечи в одну сторону с растущим объёмом — продолжение."""
    b = ctx.bars
    a14 = ctx.atr14[i]
    ups = all(b[j].c > b[j].o for j in (i - 2, i - 1, i))
    dns = all(b[j].c < b[j].o for j in (i - 2, i - 1, i))
    growing = b[i].v > b[i - 1].v > b[i - 2].v
    if not growing:
        return None
    if ups:
        return Setup(1, b[i - 2].l, 1.5, 25, "momentum_run_long")
    if dns:
        return Setup(-1, b[i - 2].h, 1.5, 25, "momentum_run_short")
    return None


def s_squeeze_break(ctx, i):
    """Сжатие волатильности (узкие полосы) и выход из него."""
    m, sd, a14, bar = ctx.sma20[i], ctx.std20[i], ctx.atr14[i], ctx.bars[i]
    width = 4 * sd / m if m else 0
    prev_widths = []
    for j in range(i - 30, i):
        if ctx.std20[j] is not None and ctx.sma20[j]:
            prev_widths.append(4 * ctx.std20[j] / ctx.sma20[j])
    if len(prev_widths) < 20:
        return None
    if width > sorted(prev_widths)[int(len(prev_widths) * 0.25)]:
        return None                                   # сжатия нет
    if bar.c > m + 1.0 * sd:
        return Setup(1, bar.c - 1.0 * a14, 2.0, 45, "squeeze_break_long")
    if bar.c < m - 1.0 * sd:
        return Setup(-1, bar.c + 1.0 * a14, 2.0, 45, "squeeze_break_short")
    return None


def s_vwap_pullback(ctx, i):
    """Тренд по VWAP + откат к EMA21. Не фейд, а вход по тренду на откате."""
    bar, a14, vw = ctx.bars[i], ctx.atr14[i], ctx.vwap[i]
    e21, e50 = ctx.ema21[i], ctx.ema50[i]
    if e21 is None or e50 is None:
        return None
    if bar.c > vw and e21 > e50 and bar.l <= e21 and bar.c > e21:
        return Setup(1, bar.l - 0.3 * a14, 1.8, 40, "vwap_pullback_long")
    if bar.c < vw and e21 < e50 and bar.h >= e21 and bar.c < e21:
        return Setup(-1, bar.h + 0.3 * a14, 1.8, 40, "vwap_pullback_short")
    return None


def s_quarter_hour(ctx, i):
    """Эффект четверти часа: алгоритмы работают по круглым отметкам.
    Вход на открытии :00/:15/:30/:45 в сторону предыдущей свечи."""
    minute = (ctx.bars[i + 1].ts // 60_000) % 60 if i + 1 < len(ctx.bars) else -1
    if minute not in (0, 15, 30, 45):
        return None
    bar, a14 = ctx.bars[i], ctx.atr14[i]
    body = bar.c - bar.o
    if abs(body) < 0.5 * a14:
        return None
    d = 1 if body > 0 else -1
    return Setup(d, bar.c - d * 1.0 * a14, 1.5, 15, "quarter_hour")


STRATEGIES = {
    "ema_cross": s_ema_cross,
    "vwap_revert": s_vwap_revert,
    "bb_fade": s_bb_fade,
    "rsi_fade": s_rsi_fade,
    "spike_fade": s_spike_fade,
    "spike_follow": s_spike_follow,
    "range_break": s_range_break,
    "momentum_run": s_momentum_run,
    "squeeze_break": s_squeeze_break,
    "vwap_pullback": s_vwap_pullback,
    "quarter_hour": s_quarter_hour,
}


def make_random_baseline(rate: float, seed: int = 42):
    """Контрольная группа: входы наугад, те же стопы и цели.
    Если стратегия не бьёт этот бейзлайн — у неё нет края, есть только режим."""
    rng = random.Random(seed)

    def strat(ctx, i):
        if rng.random() > rate:
            return None
        a14, bar = ctx.atr14[i], ctx.bars[i]
        d = 1 if rng.random() < 0.5 else -1
        return Setup(d, bar.c - d * 1.2 * a14, 1.5, 30, "random")

    return strat


# ---------------------------------------------------------------------------
# Движок
# ---------------------------------------------------------------------------

def run_strategy(ctx: Ctx, strat, entry_mode: str) -> list:
    entry_fee = MAKER_FEE_BPS if entry_mode == "maker" else TAKER_FEE_BPS
    exit_fee = TAKER_FEE_BPS                       # выход по стопу — всегда тейкер
    slip = 0.0 if entry_mode == "maker" else SLIPPAGE_BPS
    cost_bps = entry_fee + exit_fee + slip + SLIPPAGE_BPS

    bars, trades = ctx.bars, []
    i = ctx.warmup
    n = len(bars)

    while i < n - 2:
        if not ctx.ready(i):
            i += 1
            continue
        setup = strat(ctx, i)
        if setup is None:
            i += 1
            continue

        d = setup.direction
        entry = bars[i + 1].o
        risk = abs(entry - setup.stop_price)
        if risk <= 0 or risk / entry < 0.0005:      # стоп ближе 5 bps — мусор
            i += 1
            continue
        sl = entry - d * risk
        tp = entry + d * risk * setup.tp_r

        exit_price, exit_i, reason = None, None, None
        for j in range(i + 1, min(i + 1 + setup.max_bars, n)):
            b = bars[j]
            hit_sl = (b.l <= sl) if d > 0 else (b.h >= sl)
            hit_tp = (b.h >= tp) if d > 0 else (b.l <= tp)
            if hit_sl:                              # оба в одной свече -> стоп
                exit_price, exit_i, reason = sl, j, "sl"
                break
            if hit_tp:
                exit_price, exit_i, reason = tp, j, "tp"
                break
        if exit_price is None:
            exit_i = min(i + setup.max_bars, n - 1)
            exit_price, reason = bars[exit_i].c, "time"

        gross_bps = d * (exit_price - entry) / entry * 10_000
        net_bps = gross_bps - cost_bps
        risk_bps = risk / entry * 10_000
        trades.append(Trade(ctx.symbol, setup.tag.rsplit("_", 1)[0], d, i + 1, exit_i,
                            entry, sl, tp, exit_price, exit_i - i,
                            gross_bps, net_bps, net_bps / risk_bps, reason))
        i = exit_i + COOLDOWN_BARS

    return trades


# ---------------------------------------------------------------------------
# Метрики
# ---------------------------------------------------------------------------

def metrics(trades: list) -> dict:
    n = len(trades)
    if n == 0:
        return {"n": 0}
    nets = [t.net_bps for t in trades]
    gross = [t.gross_bps for t in trades]
    rs = [t.r_net for t in trades]
    wins = [x for x in nets if x > 0]
    losses = [x for x in nets if x <= 0]
    mean = sum(nets) / n
    var = sum((x - mean) ** 2 for x in nets) / (n - 1) if n > 1 else 0.0
    sd = math.sqrt(var)
    t_stat = mean / (sd / math.sqrt(n)) if sd > 0 else 0.0

    eq, peak, dd = 0.0, 0.0, 0.0
    for x in nets:
        eq += x
        peak = max(peak, eq)
        dd = max(dd, peak - eq)

    gp = sum(wins)
    gl = -sum(losses)
    return {
        "n": n,
        "wr": len(wins) / n * 100,
        "gross_bps": sum(gross) / n,
        "net_bps": mean,
        "avg_r": sum(rs) / n,
        "t": t_stat,
        "pf": (gp / gl) if gl > 0 else float("inf"),
        "total_bps": eq,
        "maxdd_bps": dd,
        "avg_bars": sum(t.bars_held for t in trades) / n,
    }


# ---------------------------------------------------------------------------
# Формат под телефон: узкие строки, не таблица на 11 колонок
# ---------------------------------------------------------------------------

def verdict(m_is: dict, m_oos: dict, base_oos: dict) -> str:
    """Четыре условия. Проходит только всё сразу."""
    if m_oos.get("n", 0) < 30:
        return "нет данных"
    checks = [
        m_is.get("net_bps", -99) > 0,
        m_oos.get("net_bps", -99) > 0,
        m_oos.get("t", 0) > 2.0,
        m_oos.get("net_bps", -99) > base_oos.get("net_bps", 0),
    ]
    return "ГОДНО" if all(checks) else f"нет ({sum(checks)}/4)"


def strat_block(name: str, m_is: dict, m_oos: dict, base_oos: dict) -> str:
    if m_oos.get("n", 0) == 0 and m_is.get("n", 0) == 0:
        return f"{name}\n  сделок нет"
    o = m_oos if m_oos.get("n") else m_is
    return (f"{name}  [{verdict(m_is, m_oos, base_oos)}]\n"
            f"  IS  n{m_is.get('n', 0):<5} net {m_is.get('net_bps', 0):>7.2f}\n"
            f"  OOS n{o.get('n', 0):<5} net {o.get('net_bps', 0):>7.2f}"
            f"  t {o.get('t', 0):>6.2f}\n"
            f"  WR {o.get('wr', 0):.0f}%  R {o.get('avg_r', 0):+.2f}"
            f"  PF {o.get('pf', 0):.2f}  {o.get('avg_bars', 0):.0f}св")


def cost_row(ctx: Ctx, cost: float) -> str:
    rngs = sorted((b.h - b.l) / b.c * 10_000 for b in ctx.bars)
    med = rngs[len(rngs) // 2]
    p75 = rngs[int(len(rngs) * 0.75)]
    v = "ok" if p75 > cost * 2 else ("тонко" if p75 > cost else "МЁРТВО")
    return f"{ctx.symbol:<10} med {med:>5.1f}  p75 {p75:>5.1f}  {v}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser(description="Стенд для скальп-стратегий на Bybit")
    p.add_argument("--symbols", default=os.environ.get("SYMBOLS", ",".join(DEFAULT_SYMBOLS)))
    p.add_argument("--interval", default=os.environ.get("INTERVAL", "1"),
                   choices=["1", "3", "5", "15"])
    p.add_argument("--days", type=int, default=int(os.environ.get("DAYS", "60")))
    p.add_argument("--entry", default=os.environ.get("ENTRY", "taker"),
                   choices=["taker", "maker"])
    p.add_argument("--strategy", default=None, help="прогнать только одну")
    p.add_argument("--refresh", action="store_true")
    p.add_argument("--costs-only", action="store_true",
                   default=os.environ.get("COSTS_ONLY", "") == "1")
    p.add_argument("--synthetic", action="store_true")
    p.add_argument("--split", type=float, default=0.7)
    p.add_argument("--no-telegram", action="store_true")
    args = p.parse_args()

    entry_fee = MAKER_FEE_BPS if args.entry == "maker" else TAKER_FEE_BPS
    slip = 0.0 if args.entry == "maker" else SLIPPAGE_BPS
    cost = entry_fee + TAKER_FEE_BPS + slip + SLIPPAGE_BPS

    buf = []
    emit(f"SCALP LAB  {args.interval}m / {args.days}д / {args.entry}", buf)
    emit(f"круг = {cost:.1f} bps", buf)
    if args.entry == "maker":
        emit("maker: исполнение допущено всегда -> верхняя граница", buf)
    emit("", buf)

    if args.synthetic:
        symbols = ["SYNTH"]
    else:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    names = [args.strategy] if args.strategy else list(STRATEGIES)
    strats = {k: STRATEGIES[k] for k in names}
    strats["RANDOM_BASE"] = make_random_baseline(0.004)

    is_tr = {k: [] for k in strats}
    oos_tr = {k: [] for k in strats}
    cost_lines = []

    # По одному символу за раз: 60 дней минуток на 8 пар сразу в память не влезут
    for sym in symbols:
        try:
            bars = (synthetic_bars() if args.synthetic
                    else get_bars(sym, args.interval, args.days, args.refresh))
            ctx = Ctx(sym, bars)
        except Exception as e:                       # noqa: BLE001
            print(f"  {sym}: пропуск — {e}", file=sys.stderr)
            continue

        cost_lines.append(cost_row(ctx, cost))
        if not args.costs_only:
            cut = int(len(ctx.bars) * args.split)
            for name, fn in strats.items():
                for t in run_strategy(ctx, fn, args.entry):
                    (is_tr if t.entry_i < cut else oos_tr)[name].append(t)
        del ctx, bars

    emit("== ПОЛ ИЗДЕРЖЕК (bps) ==", buf)
    for line in cost_lines:
        emit(line, buf)
    emit("", buf)

    if not args.costs_only:
        m_is = {k: metrics(v) for k, v in is_tr.items()}
        m_oos = {k: metrics(v) for k, v in oos_tr.items()}
        base = m_oos.get("RANDOM_BASE", {})

        emit("== СТРАТЕГИИ ==", buf)
        emit("(сорт по OOS net bps)", buf)
        emit("", buf)
        order = sorted(strats, key=lambda k: m_oos[k].get("net_bps", -999), reverse=True)
        for name in order:
            emit(strat_block(name, m_is[name], m_oos[name], base), buf)
            emit("", buf)

        emit("Годно = net>0 в IS и OOS, t>2,", buf)
        emit("и бьёт RANDOM_BASE. Всё сразу.", buf)

    if not args.no_telegram:
        if tg_send("\n".join(buf)):
            print("[tg] отправлено", file=sys.stderr)
        else:
            print("[tg] не настроено (нет TELEGRAM_BOT_TOKEN/CHAT_ID)", file=sys.stderr)


if __name__ == "__main__":
    main()
