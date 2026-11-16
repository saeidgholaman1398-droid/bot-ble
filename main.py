"""
ربات تحلیل صندوق طلای عیار (تایم ۵ و ۱۵ دقیقه و ۱ و ۴ ساعت) برای تلگرام و بله، در یک سرویس
- هر پلتفرمی که توکنش ست شده باشد روشن می‌شود (TELEGRAM_TOKEN و/یا BALE_TOKEN)
- داده: API بازار سرمایه ایران TSETMC (بدون API Key؛ شناسه نماد به‌صورت خودکار پیدا می‌شود)
- تحلیل: موتور امتیازدهی اندیکاتوری (EMA, RSI, MACD, Bollinger, ATR, حجم)
- تحلیل یک بار ساخته می‌شود و برای مشترک‌های هر دو پلتفرم ارسال می‌شود
- هوش مصنوعی اختیاری: اگر GEMINI_API_KEY ست باشد، یک جمع‌بندی هم اضافه می‌شود
"""
import os
import re
import time
import html
import logging
import threading
import sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ayar-bot")

INTERVAL_MIN = float(os.getenv("INTERVAL_MIN", "5"))  # هر چند دقیقه ارسال شود (2 یا 5)
GEMINI_KEY = os.getenv("GEMINI_API_KEY", "").strip()  # اختیاری
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
PORT = int(os.getenv("PORT", "10000"))
SELF_URL = os.getenv("RENDER_EXTERNAL_URL", "").strip()  # رندر خودش ست می‌کند
DATABASE_PATH = os.getenv("DATABASE_PATH", "subscribers.db").strip()
SYMBOL = os.getenv("TSETMC_SYMBOL", "عیار").strip() or "عیار"
INS_CODE = os.getenv("TSETMC_INS_CODE", "").strip()  # اختیاری؛ در حالت عادی خودکار پیدا می‌شود
HISTORY_SESSIONS = max(20, min(180, int(os.getenv("HISTORY_SESSIONS", "90"))))
TSETMC_BASES = [x.strip().rstrip("/") for x in os.getenv(
    "TSETMC_BASE_URLS", "https://cdn.tsetmc.com,https://cdn10.tsetmc.com"
).split(",") if x.strip()]
MARKET_START = os.getenv("MARKET_START", "09:00")
MARKET_END = os.getenv("MARKET_END", "12:30")
MIN_CANDLES = max(30, int(os.getenv("MIN_CANDLES", "55")))

TIMEFRAMES = [5, 15, 60, 240]
TZ = timezone(timedelta(hours=3, minutes=30))  # وقت ایران
HTTP = requests.Session()
HTTP.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.tsetmc.com/",
})
INSTRUMENT_NAME = "صندوق طلای عیار مفید"
_DATA_LOCK = threading.RLock()
_DATA_CACHE = {"at": 0.0, "bundle": None}



# ---------------------------------------------------------------- داده‌های صندوق عیار (TSETMC)
# قالب کندل در ادامه همان ساختار قبلی است: (open, high, low, close, volume)
# قیمت‌های TSETMC ریال هستند؛ در پیام‌ها برای خوانایی به تومان تبدیل می‌شوند.
def api_get(path):
    errors = []
    for base in TSETMC_BASES:
        try:
            url = f"{base}/api{path}"
            r = HTTP.get(url, timeout=20)
            if r.status_code in (403, 429, 500, 502, 503, 504):
                errors.append(f"{base}: HTTP {r.status_code}")
                continue
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and any(k in str(data) for k in ("General Error Detected", "دسترسی شما مسدود")):
                errors.append(f"{base}: blocked response")
                continue
            return data
        except Exception as e:
            errors.append(f"{base}: {type(e).__name__}")
    raise RuntimeError("TSETMC در دسترس نیست: " + "; ".join(errors[-3:]))


def _rows(payload, key):
    if isinstance(payload, dict):
        value = payload.get(key, [])
        if isinstance(value, dict):
            return [value]
        return value if isinstance(value, list) else []
    return payload if isinstance(payload, list) else []


def resolve_ins_code():
    global INS_CODE, INSTRUMENT_NAME
    if INS_CODE:
        return INS_CODE
    q = requests.utils.quote(SYMBOL, safe="")
    payload = api_get(f"/Instrument/GetInstrumentSearch/{q}")
    candidates = _rows(payload, "instrumentSearch")
    exact = [x for x in candidates if str(x.get("lVal18AFC", "")).strip() == SYMBOL]
    if not exact:
        exact = [x for x in candidates if SYMBOL in str(x.get("lVal18AFC", ""))]
    if not exact:
        raise RuntimeError(f"نماد «{SYMBOL}» در TSETMC پیدا نشد")
    row = exact[0]
    code = str(row.get("insCode", "")).strip()
    if not code:
        raise RuntimeError(f"شناسه معاملاتی نماد «{SYMBOL}» خالی است")
    INS_CODE = code
    INSTRUMENT_NAME = str(row.get("lVal30") or INSTRUMENT_NAME)
    log.info("TSETMC instrument resolved: %s / %s", SYMBOL, INS_CODE)
    return INS_CODE


def init_market_tables():
    with db_connect() as db:
        db.execute("""CREATE TABLE IF NOT EXISTS intraday_quotes (
            d_even INTEGER NOT NULL, h_even INTEGER NOT NULL,
            price REAL NOT NULL, volume REAL NOT NULL DEFAULT 0,
            PRIMARY KEY (d_even, h_even)
        )""")
        db.execute("""CREATE TABLE IF NOT EXISTS intraday_days (
            d_even INTEGER PRIMARY KEY, fetched_at REAL NOT NULL,
            row_count INTEGER NOT NULL DEFAULT 0, source TEXT NOT NULL DEFAULT ''
        )""")
        db.commit()


def fetch_daily_rows(ins_code):
    payload = api_get(f"/ClosingPrice/GetClosingPriceDailyList/{ins_code}/{HISTORY_SESSIONS}")
    rows = _rows(payload, "closingPriceDaily")
    cleaned = []
    for row in rows:
        try:
            d = int(row.get("dEven", 0))
            if d:
                cleaned.append(row)
        except (TypeError, ValueError):
            continue
    return sorted(cleaned, key=lambda x: int(x.get("dEven", 0)))


def _as_int(value):
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def fetch_intraday_rows(ins_code, d_even):
    # تاریخچه درون‌روزی قیمت پایانی/آخرین معامله برای یک روز معاملاتی
    try:
        payload = api_get(f"/ClosingPrice/GetClosingPriceHistory/{ins_code}/{int(d_even)}")
        rows = _rows(payload, "closingPriceHistory")
    except Exception as e:
        log.info("closingPriceHistory unavailable for %s: %s", d_even, type(e).__name__)
        rows = []
    if rows:
        rows = sorted(rows, key=lambda x: _as_int(x.get("hEven")))
        result, previous_volume = [], 0.0
        for row in rows:
            h = _as_int(row.get("hEven"))
            price = float(row.get("pDrCotVal") or row.get("pClosing") or 0)
            cumulative = float(row.get("qTotTran5J") or 0)
            delta = max(0.0, cumulative - previous_volume)
            previous_volume = max(previous_volume, cumulative)
            if h and price > 0:
                result.append((h, price, delta))
        if result:
            return result, "TSETMC closingPriceHistory"

    # پشتیبان: معاملات ریز همان روز. مستندات عمومی TSETMC کسری احتمالی این فید را ذکر می‌کنند؛
    # بنابراین فقط در نبود تاریخچه قیمت از آن برای ساخت تقریبی کندل استفاده می‌شود.
    payload = api_get(f"/Trade/GetTradeHistory/{ins_code}/{int(d_even)}/false")
    trades = _rows(payload, "tradeHistory")
    result = []
    for row in trades:
        h = _as_int(row.get("hEven"))
        price = float(row.get("pTran") or 0)
        volume = float(row.get("qTitTran") or 0)
        if h and price > 0:
            result.append((h, price, volume))
    result.sort(key=lambda x: x[0])
    if result:
        by_time = {}
        for h, price, volume in result:
            if h not in by_time:
                by_time[h] = [price, volume]
            else:
                by_time[h][0] = price
                by_time[h][1] += volume
        return [(h, v[0], v[1]) for h, v in sorted(by_time.items())], "TSETMC tradeHistory (fallback)"
    return [], "empty"


def save_intraday_day(d_even, points, source):
    with db_connect() as db:
        if points:
            db.executemany(
                "INSERT INTO intraday_quotes(d_even,h_even,price,volume) VALUES(?,?,?,?) "
                "ON CONFLICT(d_even,h_even) DO UPDATE SET price=excluded.price, volume=excluded.volume",
                [(int(d_even), int(h), float(price), float(volume)) for h, price, volume in points]
            )
        db.execute(
            "INSERT INTO intraday_days(d_even,fetched_at,row_count,source) VALUES(?,?,?,?) "
            "ON CONFLICT(d_even) DO UPDATE SET fetched_at=excluded.fetched_at, "
            "row_count=excluded.row_count, source=excluded.source",
            (int(d_even), time.time(), len(points), source)
        )
        db.commit()


def _day_cache_info(d_even):
    with db_connect() as db:
        row = db.execute(
            "SELECT fetched_at,row_count,source FROM intraday_days WHERE d_even=?", (int(d_even),)
        ).fetchone()
    return row


def refresh_intraday_history(force_latest=False):
    """Backfill missing sessions into SQLite. Intended for the warm-up thread."""
    ins_code = resolve_ins_code()
    daily = fetch_daily_rows(ins_code)
    if not daily:
        raise RuntimeError("TSETMC تاریخچه روزانه‌ای برای نماد برنگرداند")
    dates = [int(x["dEven"]) for x in daily[-HISTORY_SESSIONS:]]
    latest_date = dates[-1]
    # تاریخچه قدیمی‌تر ابتدا دریافت می‌شود تا دریافت آخرین قیمت در زمان استفاده از ربات معطل نماند.
    for d in dates[:-1]:
        info = _day_cache_info(d)
        if info and int(info[1]) > 0:
            continue
        if info and int(info[1]) == 0 and time.time() - float(info[0]) < 3600:
            continue
        try:
            points, source = fetch_intraday_rows(ins_code, d)
            save_intraday_day(d, points, source)
            log.info("historical intraday %s: %s points (%s)", d, len(points), source)
        except Exception as e:
            log.warning("historical intraday failed for %s: %s", d, type(e).__name__)
        time.sleep(0.55)
    info = _day_cache_info(latest_date)
    fresh_enough = info and (time.time() - float(info[0]) < 45)
    if force_latest or not fresh_enough:
        try:
            points, source = fetch_intraday_rows(ins_code, latest_date)
            save_intraday_day(latest_date, points, source)
            log.info("latest intraday %s: %s points (%s)", latest_date, len(points), source)
        except Exception as e:
            log.warning("latest intraday refresh failed: %s", type(e).__name__)
    return daily


def refresh_latest_session(daily, force=False):
    if not daily:
        return
    ins_code = resolve_ins_code()
    latest_date = int(daily[-1]["dEven"])
    info = _day_cache_info(latest_date)
    fresh_enough = info and (time.time() - float(info[0]) < 35)
    if force or not fresh_enough:
        points, source = fetch_intraday_rows(ins_code, latest_date)
        save_intraday_day(latest_date, points, source)


def _time_parts(h_even):
    # TSETMC زمان HHMMSS را معمولاً به صورت عدد صحیح می‌دهد.
    raw = str(int(h_even)).zfill(6)
    return int(raw[:2]), int(raw[2:4]), int(raw[4:6])


def read_intraday_points(dates):
    if not dates:
        return []
    marks = ",".join("?" for _ in dates)
    with db_connect() as db:
        rows = db.execute(
            f"SELECT d_even,h_even,price,volume FROM intraday_quotes WHERE d_even IN ({marks}) ORDER BY d_even,h_even",
            [int(x) for x in dates]
        ).fetchall()
    return rows


def aggregate_candles(rows, timeframe):
    # بازه‌ها از ساعت شروع بازار تنظیم می‌شوند؛ در نتیجه کندل ۴ ساعته هر جلسه یک کندل است.
    start_h, start_m = (int(x) for x in MARKET_START.split(":"))
    start_minute = start_h * 60 + start_m
    buckets = {}
    for d_even, h_even, price, volume in rows:
        try:
            hh, mm, _ = _time_parts(h_even)
            minute = hh * 60 + mm
            if minute < start_minute:
                continue
            bucket = (minute - start_minute) // timeframe
            key = (int(d_even), int(bucket))
            if key not in buckets:
                buckets[key] = [float(price), float(price), float(price), float(price), max(0.0, float(volume))]
            else:
                bar = buckets[key]
                bar[1] = max(bar[1], float(price))
                bar[2] = min(bar[2], float(price))
                bar[3] = float(price)
                bar[4] += max(0.0, float(volume))
        except (TypeError, ValueError, IndexError):
            continue
    return [tuple(buckets[k]) for k in sorted(buckets)]


def _latest_daily_snapshot(daily):
    row = daily[-1] if daily else {}
    last = float(row.get("pDrCotVal") or row.get("pClosing") or 0)
    close = float(row.get("pClosing") or last or 0)
    yesterday = float(row.get("priceYesterday") or 0)
    change = (last / yesterday - 1) * 100 if yesterday > 0 and last > 0 else None
    return {
        "date": int(row.get("dEven") or 0),
        "last": last,
        "close": close,
        "open": float(row.get("priceFirst") or 0),
        "high": float(row.get("priceMax") or 0),
        "low": float(row.get("priceMin") or 0),
        "volume": float(row.get("qTotTran5J") or 0),
        "change_pct": change,
    }


def fetch_etf_nav(ins_code):
    try:
        payload = api_get(f"/Fund/GetETFByInsCode/{ins_code}")
        item = payload.get("etf") if isinstance(payload, dict) else None
        if isinstance(item, list):
            item = item[-1] if item else None
        if not isinstance(item, dict):
            return None
        red = float(item.get("pRedTran") or 0)
        sub = float(item.get("pSubTran") or 0)
        if red <= 0 and sub <= 0:
            return None
        return {"redemption": red or None, "subscription": sub or None,
                "date": _as_int(item.get("deven")), "time": _as_int(item.get("hEven"))}
    except Exception as e:
        log.info("ETF NAV unavailable: %s", type(e).__name__)
        return None


def get_market_bundle(force=False):
    with _DATA_LOCK:
        if not force and _DATA_CACHE["bundle"] and time.time() - _DATA_CACHE["at"] < 45:
            return _DATA_CACHE["bundle"]
        ins_code = resolve_ins_code()
        daily = fetch_daily_rows(ins_code)
        if not daily:
            raise RuntimeError("تاریخچه روزانه نماد عیار در TSETMC موجود نیست")
        try:
            refresh_latest_session(daily, force=force)
        except Exception as e:
            log.warning("latest session refresh failed: %s", type(e).__name__)
        dates = [int(x["dEven"]) for x in daily[-HISTORY_SESSIONS:]]
        raw = read_intraday_points(dates)
        candles = {tf: aggregate_candles(raw, tf) for tf in TIMEFRAMES}
        snapshot = _latest_daily_snapshot(daily)
        nav = fetch_etf_nav(ins_code)
        bundle = {
            "candles": candles, "snapshot": snapshot, "nav": nav,
            "source": "TSETMC", "dates_loaded": len(dates),
        }
        _DATA_CACHE.update({"at": time.time(), "bundle": bundle})
        return bundle


def warmup_history_loop():
    while True:
        try:
            refresh_intraday_history(force_latest=True)
            with _DATA_LOCK:
                _DATA_CACHE.update({"at": 0.0, "bundle": None})
            log.info("historical intraday cache ready")
            return
        except Exception as e:
            log.warning("history warmup failed: %s", e)
            time.sleep(60)


def is_market_clock_open():
    now = datetime.now(TZ)
    # هفته معاملاتی ایران: شنبه تا چهارشنبه؛ پنجشنبه و جمعه تعطیل
    if now.weekday() not in (5, 6, 0, 1, 2):
        return False
    try:
        sh, sm = (int(x) for x in MARKET_START.split(":"))
        eh, em = (int(x) for x in MARKET_END.split(":"))
        current = now.hour * 60 + now.minute
        return sh * 60 + sm <= current <= eh * 60 + em
    except Exception:
        return False


# ---------------------------------------------------------------- اندیکاتورها
def ema(vals, n):
    k = 2 / (n + 1)
    out, e = [], vals[0]
    for v in vals:
        e = v * k + e * (1 - k)
        out.append(e)
    return out


def rsi(closes, n=14):
    g = l = 0.0
    for i in range(1, n + 1):
        d = closes[i] - closes[i - 1]
        g += max(d, 0)
        l += max(-d, 0)
    ag, al = g / n, l / n
    for i in range(n + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        ag = (ag * (n - 1) + max(d, 0)) / n
        al = (al * (n - 1) + max(-d, 0)) / n
    return 100.0 if al == 0 else 100 - 100 / (1 + ag / al)


def atr(c, n=14):
    trs = []
    for i in range(1, len(c)):
        h, lo, pc = c[i][1], c[i][2], c[i - 1][3]
        trs.append(max(h - lo, abs(h - pc), abs(lo - pc)))
    a = sum(trs[:n]) / n
    for t in trs[n:]:
        a = (a * (n - 1) + t) / n
    return a


def bollinger(closes, n=20, k=2):
    w = closes[-n:]
    m = sum(w) / n
    sd = (sum((x - m) ** 2 for x in w) / n) ** 0.5
    return m - k * sd, m, m + k * sd


# ---------------------------------------------------------------- تحلیل
def analyze(c):
    closes = [x[3] for x in c]
    price = closes[-1]
    e9, e21, e50 = ema(closes, 9)[-1], ema(closes, 21)[-1], ema(closes, 50)[-1]
    macd_line = [a - b for a, b in zip(ema(closes, 12), ema(closes, 26))]
    hist = [m - s for m, s in zip(macd_line, ema(macd_line, 9))]
    h_now, h_prev = hist[-1], hist[-2]
    r = rsi(closes)
    lo, _, up = bollinger(closes)
    a = atr(c)
    vols = [x[4] for x in c]
    vol_ratio = vols[-1] / ((sum(vols[-21:-1]) / 20) or 1)

    score, why = 0.0, []
    strong_up = price > e9 > e21 > e50
    strong_down = price < e9 < e21 < e50

    if strong_up:
        score += 2; why.append("روند صعودی قوی (EMA 9>21>50)")
    elif strong_down:
        score -= 2; why.append("روند نزولی قوی (EMA 9<21<50)")
    elif e9 > e21:
        score += 0.75; why.append("EMA9 بالای EMA21")
    else:
        score -= 0.75; why.append("EMA9 پایین EMA21")

    if r < 30:
        if strong_down:
            why.append(f"RSI اشباع فروش ({r:.0f}) - احتمال برگشت، احتیاط")
        else:
            score += 1.5; why.append(f"RSI اشباع فروش ({r:.0f})")
    elif r > 70:
        if strong_up:
            why.append(f"RSI اشباع خرید ({r:.0f}) - احتمال اصلاح، احتیاط")
        else:
            score -= 1.5; why.append(f"RSI اشباع خرید ({r:.0f})")
    elif r >= 50:
        score += 0.5; why.append(f"RSI مثبت ({r:.0f})")
    else:
        score -= 0.5; why.append(f"RSI منفی ({r:.0f})")

    if h_now > 0 and h_now > h_prev:
        score += 1; why.append("MACD مثبت و در حال تقویت")
    elif h_now > 0:
        score += 0.4; why.append("MACD مثبت ولی ضعیف‌شونده")
    elif h_now < 0 and h_now < h_prev:
        score -= 1; why.append("MACD منفی و در حال تقویت")
    else:
        score -= 0.4; why.append("MACD منفی ولی ضعیف‌شونده")

    if price < lo:
        if strong_down:
            score -= 0.5; why.append("شکست باند پایین بولینگر در روند نزولی")
        else:
            score += 1; why.append("قیمت زیر باند پایین بولینگر")
    elif price > up:
        if strong_up:
            score += 0.5; why.append("شکست باند بالای بولینگر در روند صعودی")
        else:
            score -= 1; why.append("قیمت بالای باند بالای بولینگر")

    if vol_ratio > 1.5:
        d = 1 if c[-1][3] > c[-1][0] else -1
        score += 0.5 * d
        why.append(f"حجم {vol_ratio:.1f}× میانگین ({'خریدار' if d > 0 else 'فروشنده'})")

    if score >= 2.5:
        side = "bull"
    elif score <= -2.5:
        side = "bear"
    else:
        side = "flat"

    plan = None
    if side == "bull":
        plan = (price, price - 1.5 * a, price + 1.5 * a, price + 3 * a)
    elif side == "bear":
        plan = (price, price + 1.5 * a, price - 1.5 * a, price - 3 * a)

    return {
        "price": price, "score": score, "side": side,
        "conf": min(95, int(abs(score) / 5 * 100)),
        "rsi": r, "atr": a, "macd": h_now,
        "support": min(x[2] for x in c[-30:]),
        "resistance": max(x[1] for x in c[-30:]),
        "plan": plan, "why": why,
    }


SIDE_TXT = {"bull": "🟢 صعودی", "bear": "🔴 نزولی", "flat": "⚪️ خنثی"}


def fmt(x):
    return f"{x / 10:,.1f}"  # ریال به تومان


def tf_label(tf):
    if tf == 240:
        return "۴ ساعت (کل جلسه معاملاتی)"
    return f"{tf // 60} ساعت" if tf % 60 == 0 else f"{tf} دقیقه"


def tf_block(tf, a):
    s = f"⏱ <b>تایم {tf_label(tf)}</b>\n"
    s += f"{SIDE_TXT[a['side']]} | قدرت سیگنال الگوریتم: {a['conf']}%\n"
    s += f"RSI: {a['rsi']:.0f} | ATR: {fmt(a['atr'])} تومان\n"
    s += f"حمایت: {fmt(a['support'])} | مقاومت: {fmt(a['resistance'])} تومان\n"
    if a["plan"]:
        e, sl, t1, t2 = a["plan"]
        s += f"🎯 قیمت مرجع: {fmt(e)} تومان\n❌ حد ابطال سناریو: {fmt(sl)} تومان\n✅ هدف۱: {fmt(t1)} | هدف۲: {fmt(t2)} تومان\n"
    s += "• " + "\n• ".join(html.escape(w) for w in a["why"][:4])
    return s


def verdict(a5, a15):
    if a5["side"] == a15["side"] and a5["side"] != "flat":
        return "🔥 هر دو تایم هم‌جهت‌اند: " + SIDE_TXT[a5["side"]]
    if "bull" in (a5["side"], a15["side"]) and "bear" in (a5["side"], a15["side"]):
        return "⚠️ تضاد بین تایم‌ها؛ بهتره وارد نشی"
    if a15["side"] != "flat":
        return f"📌 جهت اصلی (۱۵ دقیقه): {SIDE_TXT[a15['side']]}؛ ۵ دقیقه هنوز تایید نکرده"
    return "😴 سیگنال واضحی نیست؛ صبر کن"


def ai_comment(a5, a15):
    if not GEMINI_KEY:
        return ""
    prompt = (
        "تو تحلیلگر تکنیکال صندوق طلای عیار در بورس ایران هستی. بر اساس داده‌های زیر یک جمع‌بندی خیلی کوتاه "
        "(حداکثر ۳ جمله) به فارسی بنویس. درباره حباب NAV یا قیمت جهانی طلا ادعایی نکن مگر داده‌اش داده شده باشد؛ "
        "توصیه مالی قطعی نده.\n"
        f"تایم ۵ دقیقه: جهت={a5['side']} امتیاز={a5['score']:.1f} RSI={a5['rsi']:.0f} MACD={a5['macd']:.2f}\n"
        f"تایم ۱۵ دقیقه: جهت={a15['side']} امتیاز={a15['score']:.1f} RSI={a15['rsi']:.0f} MACD={a15['macd']:.2f}\n"
        f"قیمت={a5['price'] / 10:.1f} تومان حمایت={a15['support'] / 10:.1f} مقاومت={a15['resistance'] / 10:.1f}"
    )
    try:
        r = HTTP.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent",
            headers={"x-goog-api-key": GEMINI_KEY},
            json={"contents": [{"parts": [{"text": prompt}]}]}, timeout=30)
        r.raise_for_status()
        txt = r.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
        return re.sub(r"[*_`]", "", txt)
    except Exception as e:
        log.warning("Gemini failed: %s", type(e).__name__)
        return ""


def build_message():
    bundle = get_market_bundle()
    snap = bundle["snapshot"]
    if snap["last"] <= 0:
        raise RuntimeError("قیمت معتبر نماد عیار از TSETMC دریافت نشد")
    now = datetime.now(TZ).strftime("%H:%M:%S")
    price = fmt(snap["last"])
    msg = (f"🟡 <b>تحلیل صندوق طلای عیار (عیار)</b>\n🕒 {now} | منبع: {bundle['source']}\n"
           f"💰 آخرین قیمت: <b>{price} تومان</b>")
    if snap["change_pct"] is not None:
        msg += f" | تغییر روز: {snap['change_pct']:+.2f}%"
    msg += "\n"
    if snap["close"] > 0:
        msg += f"📌 قیمت پایانی/میانگین تابلویی: {fmt(snap['close'])} تومان\n"
    nav = bundle.get("nav")
    if nav:
        red = nav.get("redemption")
        sub = nav.get("subscription")
        if red:
            msg += f"📊 NAV ابطال: {fmt(red)} تومان"
            premium = (snap["last"] / red - 1) * 100 if snap["last"] > 0 else None
            if premium is not None:
                msg += f" | فاصله قیمت تا NAV ابطال: {premium:+.2f}%"
            msg += "\n"
        if sub:
            msg += f"NAV صدور: {fmt(sub)} تومان\n"
    else:
        msg += "📊 NAV صندوق: فعلاً از منبع داده در دسترس نیست\n"
    msg += "\n"

    analyses = {}
    for tf in TIMEFRAMES:
        candles = bundle["candles"].get(tf, [])
        if len(candles) >= MIN_CANDLES:
            analyses[tf] = analyze(candles)
            msg += tf_block(tf, analyses[tf]) + "\n\n"
        else:
            msg += (f"⏱ <b>تایم {tf_label(tf)}</b>\n"
                    f"⚠️ تاریخچه کافی نیست: {len(candles)} کندل از حداقل {MIN_CANDLES} کندل موجود است.\n\n")

    if 5 in analyses and 15 in analyses:
        msg += f"<b>{verdict(analyses[5], analyses[15])}</b>"
        ai = ai_comment(analyses[5], analyses[15])
        if ai:
            msg += f"\n\n🤖 {html.escape(ai)}"
    else:
        msg += "⚠️ جمع‌بندی چندتایمی تا تکمیل تاریخچه ۵ و ۱۵ دقیقه فعال نمی‌شود."
    msg += "\n\n<i>کندل‌ها از نقاط درون‌روزی TSETMC تجمیع شده‌اند و سقف/کف هر بازه ممکن است تقریبی باشد. این تحلیل توصیه مالی نیست.</i>"
    return msg


# ---------------------------------------------------------------- پلتفرم‌ها (تلگرام و بله)
# متن‌ها داخل کد با تگ <b> و <i> نوشته شده‌اند. تلگرام همان HTML را می‌گیرد
# و برای بله هنگام ارسال به Markdown تبدیل می‌شود.
def plain(text):
    return html.unescape(re.sub(r"<[^>]+>", "", text))


def to_bale(text):
    text = re.sub(r"</?b>", "*", text)
    text = re.sub(r"</?i>", "_", text)
    return html.unescape(text)


def strip_markup(text):
    return text.replace("*", "").replace("_", "")


class Platform:
    def __init__(self, name, title, token, admin_id, base, parse_mode,
                 convert, fallback, parse_hint, poll_params):
        self.name = name              # telegram / bale (کلید در دیتابیس)
        self.title = title            # نام نمایشی
        self.admin_id = admin_id      # آیدی مدیر در همین پلتفرم
        self.api = f"{base}/bot{token}"
        self.parse_mode = parse_mode
        self.convert = convert        # تبدیل متن داخلی به قالب پلتفرم
        self.fallback = fallback      # تبدیل به متن ساده اگر قالب‌بندی خطا داد
        self.parse_hint = parse_hint  # نشانه‌ی خطای قالب‌بندی در پاسخ API
        self.poll_params = poll_params

    def is_admin(self, chat_id):
        return bool(self.admin_id) and str(chat_id) == self.admin_id

    def call(self, method, **payload):
        try:
            return HTTP.post(f"{self.api}/{method}", json=payload, timeout=20)
        except Exception as e:
            log.error("[%s] %s failed: %s", self.name, method, type(e).__name__)
            return None

    def deliver(self, method, payload):
        r = self.call(method, **payload)
        if (r is not None and r.status_code == 400 and "parse_mode" in payload
                and self.parse_hint in r.text and "not modified" not in r.text):
            # قالب‌بندی مشکل داشت؛ یک بار بدون قالب‌بندی دوباره بفرست
            payload = dict(payload)
            payload.pop("parse_mode", None)
            payload["text"] = self.fallback(payload["text"])
            r = self.call(method, **payload)
        if r is not None and not r.ok and "not modified" not in r.text:
            log.error("[%s] API error: %s", self.name, r.text[:200])
        return r

    def send(self, text, markup=None, chat_id=None):
        p = {"chat_id": chat_id or self.admin_id, "text": self.convert(text),
             "disable_web_page_preview": True}
        if self.parse_mode:
            p["parse_mode"] = self.parse_mode
        if markup:
            p["reply_markup"] = markup
        return self.deliver("sendMessage", p)

    def edit(self, chat_id, message_id, text, markup=None):
        p = {"chat_id": chat_id, "message_id": message_id, "text": self.convert(text),
             "disable_web_page_preview": True}
        if self.parse_mode:
            p["parse_mode"] = self.parse_mode
        if markup:
            p["reply_markup"] = markup
        self.deliver("editMessageText", p)

    def answer(self, cb_id, text=""):
        self.call("answerCallbackQuery", callback_query_id=cb_id, text=text)


def build_platforms():
    items = []
    tg_token = os.getenv("TELEGRAM_TOKEN", "").strip()
    if tg_token:
        items.append(Platform(
            name="telegram", title="تلگرام", token=tg_token,
            admin_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
            base=os.getenv("TELEGRAM_API_BASE", "https://api.telegram.org").strip().rstrip("/"),
            parse_mode="HTML", convert=lambda t: t, fallback=plain,
            parse_hint="parse entities",
            poll_params={"allowed_updates": '["message","callback_query"]'}))
    bale_token = os.getenv("BALE_TOKEN", "").strip()
    if bale_token:
        items.append(Platform(
            name="bale", title="بله", token=bale_token,
            admin_id=os.getenv("BALE_CHAT_ID", "").strip(),
            base=os.getenv("BALE_API_BASE", "https://tapi.bale.ai").strip().rstrip("/"),
            # بله پیام‌ها را Markdown نمایش می‌دهد؛ اگر BALE_PARSE_MODE خالی باشد، ارسال نمی‌شود
            parse_mode=os.getenv("BALE_PARSE_MODE", "Markdown").strip(),
            convert=to_bale, fallback=strip_markup, parse_hint="",
            poll_params={}))
    return items


PLATFORMS = build_platforms()

state = {"auto": True, "interval": INTERVAL_MIN}
INTERVAL_CHOICES = [2, 5, 10]
_FA = str.maketrans("0123456789", "۰۱۲۳۴۵۶۷۸۹")

HELP_TEXT = (
    "ℹ️ <b>راهنما</b>\n\n"
    "📊 تحلیل کامل صندوق عیار: بررسی هم‌زمان تایم ۵ و ۱۵ دقیقه و ۱ و ۴ ساعت\n"
    "⏱ فقط ۵ دقیقه، ۱۵ دقیقه، ۱ ساعت یا ۴ ساعت: تحلیل یک تایم\n"
    "💰 آخرین قیمت معامله صندوق عیار و NAV در صورت دسترس‌بودن\n"
    "⏸/▶️ توقف یا شروع ارسال خودکار\n"
    "🕒 تغییر فاصله ارسال خودکار (فقط در ساعت معامله)\n"
    "🔄 زیر هر تحلیل می‌تونی همون پیام رو تازه‌سازی کنی\n\n"
    "دستورها: /menu  /analyze  /price  /help\n\n"
    "<i>تحلیل الگوریتمی و آموزشی است، توصیه مالی نیست.</i>"
)

COMMANDS = [
    {"command": "menu", "description": "📋 نمایش منو"},
    {"command": "analyze", "description": "📊 تحلیل همین الان"},
    {"command": "price", "description": "💰 قیمت لحظه‌ای"},
    {"command": "help", "description": "ℹ️ راهنما"},
    {"command": "stop", "description": "🔕 لغو دریافت سیگنال خودکار"},
]


def fa(x):
    return f"{x:g}".translate(_FA)


def now_str():
    return datetime.now(TZ).strftime("%H:%M:%S")


# ---------------------------------------------------------------- عضویت و ارسال گروهی
# جدول subs برای هر پلتفرم جدا نگه می‌دارد (کلید: platform + chat_id)
def db_connect():
    return sqlite3.connect(DATABASE_PATH, timeout=20)


def init_subscribers_db():
    parent = Path(DATABASE_PATH).expanduser().resolve().parent
    parent.mkdir(parents=True, exist_ok=True)
    with db_connect() as db:
        db.execute("""CREATE TABLE IF NOT EXISTS subs (
            platform TEXT NOT NULL, chat_id TEXT NOT NULL, username TEXT,
            first_name TEXT, active INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL,
            PRIMARY KEY (platform, chat_id)
        )""")
        db.commit()


def subscribe(platform, chat_id, user=None):
    user = user or {}
    with db_connect() as db:
        db.execute(
            "INSERT INTO subs(platform, chat_id, username, first_name, active, created_at) "
            "VALUES (?, ?, ?, ?, 1, ?) ON CONFLICT(platform, chat_id) DO UPDATE SET "
            "username=excluded.username, first_name=excluded.first_name, active=1",
            (platform, str(chat_id), user.get("username"), user.get("first_name"),
             datetime.now(timezone.utc).isoformat())
        )
        db.commit()


def unsubscribe(platform, chat_id):
    with db_connect() as db:
        db.execute("UPDATE subs SET active=0 WHERE platform=? AND chat_id=?",
                   (platform, str(chat_id)))
        db.commit()


def active_subscribers(platform):
    with db_connect() as db:
        return [row[0] for row in db.execute(
            "SELECT chat_id FROM subs WHERE platform=? AND active=1", (platform,)).fetchall()]


def subscriber_counts(platform):
    with db_connect() as db:
        total, active = db.execute(
            "SELECT COUNT(*), COALESCE(SUM(active), 0) FROM subs WHERE platform=?",
            (platform,)).fetchone()
    return int(total), int(active)


def broadcast(p, text, markup=None):
    recipients = active_subscribers(p.name)
    sent = 0
    for recipient in recipients:
        response = p.send(text, markup, chat_id=recipient)
        if response is not None and response.ok:
            sent += 1
        elif response is not None and response.status_code == 403:
            # کاربر ربات را مسدود کرده یا دسترسی چت لغو شده است
            unsubscribe(p.name, recipient)
        time.sleep(0.05)  # جلوگیری از ارسال فشرده به API
    log.info("[%s] broadcast completed: %s/%s", p.name, sent, len(recipients))
    return sent, len(recipients)


def broadcast_all(text, markup=None):
    # هر پلتفرم در ترد جدا، تا کندی یکی روی دیگری اثر نگذارد
    threads = [threading.Thread(target=broadcast, args=(p, text, markup), daemon=True)
               for p in PLATFORMS]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


# ---------------------------------------------------------------- منوی شیشه‌ای
def menu_text():
    on = "✅ فعال" if state["auto"] else "⏸ متوقف"
    return ("📋 <b>منوی ربات تحلیل صندوق عیار</b>\n\n"
            f"ارسال خودکار: {on}\n"
            f"فاصله ارسال: هر {fa(state['interval'])} دقیقه\n\n"
            "یکی از گزینه‌ها رو انتخاب کن 👇")


def menu_markup(p, chat_id=None):
    keyboard = [
        [{"text": "📊 تحلیل کامل (همه تایم‌ها)", "callback_data": "full"}],
        [{"text": "⏱ فقط ۵ دقیقه", "callback_data": "tf5"},
         {"text": "⏱ فقط ۱۵ دقیقه", "callback_data": "tf15"}],
        [{"text": "⏱ فقط ۱ ساعت", "callback_data": "tf60"},
         {"text": "⏱ فقط ۴ ساعت", "callback_data": "tf240"}],
        [{"text": "💰 قیمت لحظه‌ای", "callback_data": "price"}],
        [{"text": "ℹ️ راهنما", "callback_data": "help"}],
    ]
    # کنترل ارسال خودکار فقط برای مدیر همان پلتفرم نمایش داده می‌شود.
    if p.is_admin(chat_id):
        auto_btn = "⏸ توقف ارسال خودکار" if state["auto"] else "▶️ شروع ارسال خودکار"
        row = []
        for m in INTERVAL_CHOICES:
            mark = " ✅" if float(state["interval"]) == m else ""
            row.append({"text": f"🕒 {fa(m)} دقیقه{mark}", "callback_data": f"int{m}"})
        keyboard.insert(4, [{"text": auto_btn, "callback_data": "toggle"}])
        keyboard.insert(5, row)
        keyboard.insert(6, [{"text": "👥 تعداد اعضای ربات", "callback_data": "members"}])
    return {"inline_keyboard": keyboard}


def result_markup(kind):
    return {"inline_keyboard": [[
        {"text": "🔄 تازه‌سازی", "callback_data": f"re_{kind}"},
        {"text": "📋 منو", "callback_data": "menu"},
    ]]}


# ---------------------------------------------------------------- محتوا
def get_price():
    bundle = get_market_bundle()
    snap = bundle["snapshot"]
    if snap["last"] <= 0:
        raise RuntimeError("قیمت عیار در دسترس نیست")
    return snap["last"], bundle["source"]


def build_price_message():
    bundle = get_market_bundle()
    snap = bundle["snapshot"]
    if snap["last"] <= 0:
        raise RuntimeError("قیمت عیار در دسترس نیست")
    msg = (f"🟡 <b>قیمت صندوق طلای عیار (عیار)</b>\n\n"
           f"💰 آخرین معامله: <b>{fmt(snap['last'])} تومان</b>\n")
    if snap["close"] > 0:
        msg += f"📌 قیمت پایانی: {fmt(snap['close'])} تومان\n"
    if snap["change_pct"] is not None:
        msg += f"📈 تغییر نسبت به پایانی روز قبل: {snap['change_pct']:+.2f}%\n"
    if snap["high"] > 0 and snap["low"] > 0:
        msg += f"دامنه روز: {fmt(snap['low'])} تا {fmt(snap['high'])} تومان\n"
    nav = bundle.get("nav")
    if nav and nav.get("redemption"):
        premium = (snap["last"] / nav["redemption"] - 1) * 100
        msg += f"📊 NAV ابطال: {fmt(nav['redemption'])} تومان | فاصله قیمت تا NAV: {premium:+.2f}%\n"
    msg += f"🕒 {now_str()} | منبع: {bundle['source']}"
    if not is_market_clock_open():
        msg += "\n<i>بازار در حال حاضر خارج از ساعت معمول معامله است؛ قیمت، آخرین داده ثبت‌شده است.</i>"
    return msg


def build_tf_message(tf):
    bundle = get_market_bundle()
    candles = bundle["candles"].get(tf, [])
    if len(candles) < MIN_CANDLES:
        return (f"🟡 <b>تحلیل صندوق عیار - تایم {tf_label(tf)}</b>\n"
                f"⚠️ داده کافی برای تحلیل موجود نیست ({len(candles)} از {MIN_CANDLES} کندل). "
                "با تکمیل تاریخچه دوباره تلاش کن.")
    a = analyze(candles)
    snap = bundle["snapshot"]
    return (f"🟡 <b>تحلیل صندوق طلای عیار (عیار)</b>\n🕒 {now_str()} | منبع: TSETMC\n"
            f"💰 آخرین قیمت: <b>{fmt(snap['last'])} تومان</b>\n\n{tf_block(tf, a)}\n\n"
            "<i>تحلیل الگوریتمی و آموزشی است؛ توصیه مالی نیست.</i>")


def build_members_message():
    n = lambda x: f"{x:,}".translate(_FA)
    lines = ["👥 <b>آمار اعضای ربات</b>\n"]
    all_total = all_active = 0
    for pl in PLATFORMS:
        total, active = subscriber_counts(pl.name)
        all_total += total
        all_active += active
        lines.append(f"{pl.title}: ✅ {n(active)} فعال | 🔕 {n(total - active)} لغو‌شده")
    lines.append(f"\n✅ مجموع فعال: <b>{n(all_active)}</b>\n"
                 f"📦 مجموع کل: {n(all_total)}\n\n🕒 {now_str()}")
    return "\n".join(lines)


def safe_content(kind):
    try:
        if kind == "price":
            return build_price_message()
        if kind == "members":
            return build_members_message()
        if kind == "full":
            return build_message()
        return build_tf_message(int(kind))
    except Exception as e:
        log.error("content failed: %s", e)
        return "⚠️ دریافت داده با مشکل روبه‌رو شد؛ چند ثانیه بعد دوباره تلاش کن."


# ---------------------------------------------------------------- مدیریت دکمه‌ها و دستورها
def handle_callback(p, cq):
    cid, data = cq["id"], cq.get("data", "")
    msg = cq.get("message") or {}
    chat_id = msg.get("chat", {}).get("id")
    if chat_id is None:
        chat_id = (cq.get("from") or {}).get("id")  # اگر پیام همراه کلیک نیامد
    mid = msg.get("message_id")
    if chat_id is None:
        p.answer(cid, "خطا در تشخیص چت")
        return

    is_admin = p.is_admin(chat_id)

    if data in ("members", "re_members") and not is_admin:
        p.answer(cid, "این بخش فقط برای مدیر ربات فعال است")
        return

    if data in ("full", "tf5", "tf15", "tf60", "tf240", "price", "members"):
        p.answer(cid, "⏳ در حال دریافت...")
        kind = {"tf5": "5", "tf15": "15", "tf60": "60", "tf240": "240"}.get(data, data)
        p.send(safe_content(kind), result_markup(kind), chat_id=chat_id)
    elif data.startswith("re_"):
        p.answer(cid, "🔄 در حال تازه‌سازی...")
        kind = data[3:]
        if mid is None:
            p.send(safe_content(kind), result_markup(kind), chat_id=chat_id)
        else:
            p.edit(chat_id, mid, safe_content(kind), result_markup(kind))
    elif data == "menu":
        p.answer(cid)
        p.send(menu_text(), menu_markup(p, chat_id), chat_id=chat_id)
    elif data == "toggle":
        if not is_admin:
            p.answer(cid, "این تنظیم فقط برای مدیر ربات فعال است")
            return
        state["auto"] = not state["auto"]
        p.answer(cid, "✅ ارسال خودکار فعال شد" if state["auto"] else "⏸ ارسال خودکار متوقف شد")
        p.edit(chat_id, mid, menu_text(), menu_markup(p, chat_id))
    elif data.startswith("int"):
        if not is_admin:
            p.answer(cid, "این تنظیم فقط برای مدیر ربات فعال است")
            return
        state["interval"] = float(data[3:])
        p.answer(cid, f"🕒 از این به بعد هر {fa(state['interval'])} دقیقه")
        p.edit(chat_id, mid, menu_text(), menu_markup(p, chat_id))
    elif data == "help":
        p.answer(cid)
        p.send(HELP_TEXT, chat_id=chat_id)
    else:
        p.answer(cid)


def handle_message(p, m):
    chat_id = m.get("chat", {}).get("id")
    if chat_id is None:
        return
    t = (m.get("text") or "").strip().lower()
    chat_type = m.get("chat", {}).get("type", "")
    if t.startswith("/start"):
        if chat_type in ("private", ""):
            subscribe(p.name, chat_id, m.get("from", {}))
            p.send("✅ عضویت شما فعال شد. از این به بعد سیگنال‌های جدید برایتان ارسال می‌شود.\nبرای لغو عضویت: /stop", chat_id=chat_id)
        p.send(menu_text(), menu_markup(p, chat_id), chat_id=chat_id)
    elif t.startswith("/stop"):
        unsubscribe(p.name, chat_id)
        p.send("🔕 عضویت شما لغو شد و دیگر سیگنال خودکار دریافت نمی‌کنید. هر زمان خواستید با /start دوباره عضو شوید.", chat_id=chat_id)
    elif t.startswith("/menu"):
        p.send(menu_text(), menu_markup(p, chat_id), chat_id=chat_id)
    elif t.startswith("/analyze"):
        p.send(safe_content("full"), result_markup("full"), chat_id=chat_id)
    elif t.startswith("/price"):
        p.send(safe_content("price"), result_markup("price"), chat_id=chat_id)
    elif t.startswith("/help"):
        p.send(HELP_TEXT, chat_id=chat_id)
    elif t.startswith("/members"):
        if p.is_admin(chat_id):
            p.send(safe_content("members"), result_markup("members"), chat_id=chat_id)
        else:
            p.send("این بخش فقط برای مدیر ربات فعال است.", chat_id=chat_id)


def poll_loop(p):
    offset = None
    while True:
        try:
            started = time.time()
            params = {"timeout": 30, "offset": offset, **p.poll_params}
            r = HTTP.get(f"{p.api}/getUpdates", timeout=45, params=params)
            if r.status_code == 409:  # دو نسخه هم‌زمان با یک توکن؛ کمی صبر کن
                log.warning("[%s] 409 conflict: نسخه‌ی دیگری با همین توکن در حال اجراست", p.name)
                time.sleep(10)
                continue
            if not r.ok:
                log.warning("[%s] getUpdates HTTP %s: %s", p.name, r.status_code, r.text[:150])
                time.sleep(5)
                continue
            updates = r.json().get("result", [])
            if not updates and time.time() - started < 1:
                time.sleep(1)  # اگر long-polling پشتیبانی نشد، API را شلوغ نکن
            for u in updates:
                offset = u["update_id"] + 1
                try:
                    if "callback_query" in u:
                        handle_callback(p, u["callback_query"])
                    elif "message" in u:
                        handle_message(p, u["message"])
                except Exception as e:
                    log.error("[%s] update failed: %s", p.name, e)
        except Exception as e:
            log.warning("[%s] polling error: %s", p.name, type(e).__name__)
            time.sleep(5)


def bot_loop():
    for p in PLATFORMS:
        p.call("setMyCommands", commands=COMMANDS)
        if p.admin_id:
            p.send(menu_text(), menu_markup(p, p.admin_id), chat_id=p.admin_id)
    last = 0.0
    while True:
        if state["auto"] and is_market_clock_open() and time.time() - last >= float(state["interval"]) * 60:
            last = time.time()
            try:
                bundle = get_market_bundle()
                today = int(datetime.now(TZ).strftime("%Y%m%d"))
                if int(bundle["snapshot"].get("date") or 0) == today:
                    message = build_message()
                    broadcast_all(message, result_markup("full"))
                else:
                    log.info("auto broadcast skipped: no fresh TSETMC session data")
            except Exception as e:
                log.error("cycle failed: %s", e)
        time.sleep(3)


# ---------------------------------------------------------------- وب‌سرور (برای رندر)
class Handler(BaseHTTPRequestHandler):
    def _ok(self, body=True):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        if body:
            self.wfile.write(b"Ayar bot is running")

    def do_GET(self):
        self._ok()

    def do_HEAD(self):
        self._ok(False)

    def log_message(self, *args):
        pass


def keep_alive():
    # رندر رایگان بعد از ۱۵ دقیقه بدون ترافیک ورودی می‌خوابد؛ این پینگ جلوی آن را می‌گیرد
    if not SELF_URL:
        return
    while True:
        time.sleep(8 * 60)
        try:
            HTTP.get(SELF_URL, timeout=15)
        except Exception:
            pass


if __name__ == "__main__":
    if not PLATFORMS:
        raise SystemExit("حداقل یکی از TELEGRAM_TOKEN یا BALE_TOKEN باید در Environment ست شود")
    init_subscribers_db()
    init_market_tables()
    threading.Thread(target=warmup_history_loop, daemon=True).start()
    for pl in PLATFORMS:
        log.info("platform enabled: %s", pl.name)
        if not pl.admin_id:
            log.warning("[%s] آیدی مدیر تنظیم نشده؛ منوی مدیر غیرفعال است", pl.name)
    threading.Thread(target=bot_loop, daemon=True).start()
    for pl in PLATFORMS:
        threading.Thread(target=poll_loop, args=(pl,), daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    log.info("listening on %s", PORT)
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
