"""Telegram Martingale signal bot.

فقط سیگنال می‌دهد (معامله‌ی واقعی انجام نمی‌دهد).
استراتژی: RSI + فیلتر روند EMA، با مدیریت حجم به روش مارتینگل.
این کد توصیه‌ی مالی نیست.
"""
import asyncio
import json
import logging
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bot")

# ---------------- تنظیمات (از Environment در Render) ----------------
TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID = int(os.getenv("CHAT_ID", "0") or 0)
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()]
TF = os.getenv("TIMEFRAME", "15m")
BASE_STAKE = float(os.getenv("BASE_STAKE", "10"))      # حجم پایه (USDT)
MULT = float(os.getenv("MULTIPLIER", "2"))             # ضریب مارتینگل
MAX_STEPS = int(os.getenv("MAX_STEPS", "4"))           # حداکثر تعداد ضرر پشت‌سرهم قبل از توقف سری
TP_PCT = float(os.getenv("TP_PCT", "1.0")) / 100       # حد سود (درصد)
SL_PCT = float(os.getenv("SL_PCT", "1.0")) / 100       # حد ضرر (درصد)
EMA_P = int(os.getenv("EMA_PERIOD", "100"))
RSI_LOW = float(os.getenv("RSI_LOW", "35"))
RSI_HIGH = float(os.getenv("RSI_HIGH", "65"))
MAX_CANDLES = int(os.getenv("MAX_CANDLES", "48"))      # بستن سیگنال بعد از این تعداد کندل
CHECK_SECONDS = int(os.getenv("CHECK_SECONDS", "60"))
STATE_FILE = os.getenv("STATE_FILE", "state.json")

TF_MS = {"1m": 60_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000, "1h": 3_600_000, "4h": 14_400_000}
if TF not in TF_MS:
    raise SystemExit(f"TIMEFRAME باید یکی از {list(TF_MS)} باشد")

# data-api.binance.vision فقط دیتای بازار می‌دهد و معمولاً محدودیت منطقه‌ای ندارد
BASES = ["https://data-api.binance.vision", "https://api.binance.com"]

# ---------------- وضعیت ----------------
LOCK = threading.Lock()
state = {"step": 0, "open": None, "wins": 0, "losses": 0, "stopouts": 0,
         "pnl": 0.0, "series_loss": 0.0, "seen": {}}


def load_state():
    try:
        with open(STATE_FILE) as f:
            state.update(json.load(f))
    except (FileNotFoundError, ValueError):
        pass


def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except OSError as e:
        log.warning("save failed: %s", e)


def fmt(x):
    return f"{x:.2f}" if x >= 100 else f"{x:.6g}"


def stake():
    return round(BASE_STAKE * MULT ** state["step"], 2)


# ---------------- دیتا و اندیکاتور ----------------
def get_klines(symbol, interval, limit=300, start_ms=None):
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_ms:
        params["startTime"] = start_ms
    last_err = None
    for base in BASES:
        try:
            r = requests.get(f"{base}/api/v3/klines", params=params, timeout=10)
            if r.status_code == 200:
                return [{"t": int(k[0]), "o": float(k[1]), "h": float(k[2]),
                         "l": float(k[3]), "c": float(k[4])} for k in r.json()]
            last_err = f"{base} -> HTTP {r.status_code}"
        except requests.RequestException as e:
            last_err = f"{base} -> {e}"
    raise RuntimeError(f"دریافت دیتا ناموفق: {last_err}")


def ema(values, period):
    k = 2 / (period + 1)
    e = values[0]
    out = [e]
    for v in values[1:]:
        e = v * k + e * (1 - k)
        out.append(e)
    return out


def rsi(closes, period=14):
    gains = losses = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        gains += max(d, 0)
        losses += max(-d, 0)
    ag, al = gains / period, losses / period

    def val():
        return 100.0 if al == 0 else 100 - 100 / (1 + ag / al)

    out = [None] * period + [val()]
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        ag = (ag * (period - 1) + max(d, 0)) / period
        al = (al * (period - 1) + max(-d, 0)) / period
        out.append(val())
    return out


def analyze(sym):
    k = get_klines(sym, TF, 300)[:-1]  # کندل در حال تشکیل حذف می‌شود
    closes = [c["c"] for c in k]
    if len(closes) < EMA_P + 20:
        raise RuntimeError("کندل کافی نیست")
    r = rsi(closes)
    e = ema(closes, EMA_P)[-1]
    price = closes[-1]
    side = None
    if price > e and r[-2] < RSI_LOW <= r[-1]:      # برگشت از اشباع فروش در روند صعودی
        side = "LONG"
    elif price < e and r[-2] > RSI_HIGH >= r[-1]:   # برگشت از اشباع خرید در روند نزولی
        side = "SHORT"
    return {"symbol": sym, "t": k[-1]["t"], "price": price, "ema": e, "rsi": r[-1], "side": side}


# ---------------- منطق سیگنال و مارتینگل ----------------
def open_trade(info):
    p, side = info["price"], info["side"]
    tp = p * (1 + TP_PCT) if side == "LONG" else p * (1 - TP_PCT)
    sl = p * (1 - SL_PCT) if side == "LONG" else p * (1 + SL_PCT)
    state["open"] = {"symbol": info["symbol"], "side": side, "entry": p, "tp": tp, "sl": sl,
                     "stake": stake(), "t0": info["t"] + TF_MS[TF]}
    return signal_msg(state["open"], info)


def signal_msg(t, info):
    arrow = "🟢 خرید (LONG)" if t["side"] == "LONG" else "🔴 فروش (SHORT)"
    return (f"🚨 سیگنال جدید\n{arrow} {t['symbol']} | {TF}\n"
            f"ورود: {fmt(t['entry'])}\n"
            f"حد سود (TP): {fmt(t['tp'])}\n"
            f"حد ضرر (SL): {fmt(t['sl'])}\n"
            f"RSI: {info['rsi']:.1f}\n"
            f"💰 حجم مارتینگل: {t['stake']} USDT (مرحله {state['step'] + 1} از {MAX_STEPS + 1})")


def close_trade(t, exit_price, reason):
    pct = (exit_price - t["entry"]) / t["entry"] * (1 if t["side"] == "LONG" else -1)
    pnl = t["stake"] * pct
    state["pnl"] += pnl
    state["open"] = None
    head = {"TP": "✅ حد سود خورد", "SL": "❌ حد ضرر خورد", "TIME": "⏱ بسته شد (اتمام زمان)"}[reason]
    lines = [f"{head}\n{t['symbol']} {t['side']} | نتیجه: {pnl:+.2f} USDT"]
    if pnl > 0:
        state["wins"] += 1
        state["step"] = 0
        state["series_loss"] = 0.0
        lines.append("مارتینگل ریست شد → مرحله 1")
    else:
        state["losses"] += 1
        state["series_loss"] += -pnl
        state["step"] += 1
        if state["step"] > MAX_STEPS:
            state["stopouts"] += 1
            lines.append(f"🛑 سری مارتینگل شکست خورد (ضرر سری: {state['series_loss']:.2f} USDT). "
                         f"ریست شد → مرحله 1")
            state["step"] = 0
            state["series_loss"] = 0.0
        else:
            lines.append(f"مرحله بعد: {state['step'] + 1} از {MAX_STEPS + 1} | حجم بعدی: {stake()} USDT")
    return ["\n".join(lines)]


def check_open(t):
    candles = get_klines(t["symbol"], TF, 500, start_ms=t["t0"])
    for c in candles:
        if t["side"] == "LONG":
            hit_sl, hit_tp = c["l"] <= t["sl"], c["h"] >= t["tp"]
        else:
            hit_sl, hit_tp = c["h"] >= t["sl"], c["l"] <= t["tp"]
        if hit_sl:  # اگر هر دو در یک کندل خورد، محافظه‌کارانه SL حساب می‌شود
            return close_trade(t, t["sl"], "SL")
        if hit_tp:
            return close_trade(t, t["tp"], "TP")
    if len(candles) > MAX_CANDLES:
        return close_trade(t, candles[-1]["c"], "TIME")
    return []


def scan():
    pick = None
    for sym in SYMBOLS:
        try:
            info = analyze(sym)
        except Exception as e:
            log.warning("%s: %s", sym, e)
            continue
        if state["seen"].get(sym) == info["t"]:
            continue
        state["seen"][sym] = info["t"]
        if info["side"] and not pick:
            pick = info
    return [open_trade(pick)] if pick else []


def status_text():
    t = state["open"]
    total = state["wins"] + state["losses"]
    wr = f"{state['wins'] / total * 100:.0f}%" if total else "-"
    s = (f"📊 وضعیت\n"
         f"مرحله مارتینگل: {state['step'] + 1} از {MAX_STEPS + 1}\n"
         f"حجم بعدی: {stake()} USDT\n"
         f"برد/باخت: {state['wins']}/{state['losses']} (وین‌ریت {wr})\n"
         f"سری‌های شکست‌خورده: {state['stopouts']}\n"
         f"مجموع نتیجه: {state['pnl']:+.2f} USDT")
    if t:
        s += f"\n\n⏳ سیگنال باز: {t['side']} {t['symbol']} @ {fmt(t['entry'])}"
    return s


# ---------------- دستورات تلگرام ----------------
def owner_only(fn):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if CHAT_ID and update.effective_chat.id != CHAT_ID:
            return
        return await fn(update, context)
    return wrapper


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    if not CHAT_ID:
        await update.message.reply_text(
            f"Chat ID شما: {cid}\n"
            "این عدد را در Render داخل Environment با نام CHAT_ID بگذار و سرویس را Restart کن.")
        return
    if cid != CHAT_ID:
        return
    await update.message.reply_text(
        "ربات فعال است ✅\n/status وضعیت\n/scan تحلیل لحظه‌ای\n/reset ریست مارتینگل")


@owner_only
async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(status_text())


@owner_only
async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    def work():
        out = []
        for sym in SYMBOLS:
            try:
                i = analyze(sym)
                trend = "صعودی" if i["price"] > i["ema"] else "نزولی"
                out.append(f"{sym}: قیمت {fmt(i['price'])} | RSI {i['rsi']:.1f} | روند {trend}")
            except Exception as e:
                out.append(f"{sym}: خطا ({e})")
        return "\n".join(out)
    await update.message.reply_text(await asyncio.to_thread(work))


@owner_only
async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with LOCK:
        state.update(step=0, open=None, series_loss=0.0)
        save_state()
    await update.message.reply_text("مارتینگل ریست شد.")


async def tick(context: ContextTypes.DEFAULT_TYPE):
    if not CHAT_ID:
        return

    def work():
        with LOCK:
            msgs = []
            if state["open"]:
                msgs += check_open(state["open"])
            if not state["open"]:
                msgs += scan()
            save_state()
            return msgs

    try:
        msgs = await asyncio.to_thread(work)
    except Exception:
        log.exception("tick failed")
        return
    for m in msgs:
        await context.bot.send_message(CHAT_ID, m)


# ---------------- سرور سلامت (برای Render) ----------------
class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    do_HEAD = do_GET

    def log_message(self, *args):
        pass


def start_health():
    port = int(os.getenv("PORT", "10000"))
    srv = ThreadingHTTPServer(("0.0.0.0", port), Health)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    log.info("health server on :%s", port)


def main():
    if not TOKEN:
        raise SystemExit("BOT_TOKEN تنظیم نشده")
    load_state()
    start_health()
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("scan", cmd_scan))
    app.add_handler(CommandHandler("reset", cmd_reset))
    app.job_queue.run_repeating(tick, interval=CHECK_SECONDS, first=5)
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
