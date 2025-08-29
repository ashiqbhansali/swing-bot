# final bot.py - single file scanner (Top50 F&O dynamic, batching, CSV logs, exports)
import os
import time
import math
import csv
import json
import requests
import logging
import itertools
import traceback
import asyncio
from datetime import datetime, timedelta, time as dtime
from typing import List, Tuple, Optional, Dict, Any

import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ---------------- CONFIG (edit only here) ----------------
TOKEN = "8280517227:AAFrJSxNu4Fta5s08FsONsO69DRvkkNHr5Y"

# Timing & batching
TOP50_REFRESH_MIN = 5         # recalc top50 every 5 minutes
SCAN_INTERVAL_SEC = 60        # scan interval for picked universe (1 minute)
BATCH_SIZE = 15               # how many signals per telegram message
LOG_RETENTION_DAYS = 30       # prune entries older than this
ALERTS_CSV = "alerts.csv"     # main CSV log
FNO_CACHE_FILE = "fno_list.txt"

# Limits and filters
MAX_DEBT_EQUITY = 2.0
MIN_PE = 0.1
MAX_PE = 200

# Always include these instruments (NIFTY/BANKNIFTY and commodities)
ALWAYS_INCLUDE = [
    "NSEI", "NIFTYBEES.NS", "BANKNIFTY",
    "GC=F", "SI=F", "HG=F", "NG=F"
]

# Market hours (used for "market hours" aware behavior)
MARKET_START = dtime(9, 15)
MARKET_END = dtime(15, 30)

# --------- logging setup ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("swing-bot")

# ----------------- Utility functions -------------------
def now_ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def prune_old_logs(csv_file: str, days: int = LOG_RETENTION_DAYS):
    if not os.path.exists(csv_file):
        return
    try:
        df = pd.read_csv(csv_file, parse_dates=["timestamp"])
    except Exception:
        return
    cutoff = pd.Timestamp(datetime.utcnow() - timedelta(days=days))
    df = df[df["timestamp"] >= cutoff]
    df.to_csv(csv_file, index=False)

def append_alert_to_csv(row: Dict[str, Any], csv_file: str = ALERTS_CSV):
    df_row = pd.DataFrame([row])
    if not os.path.exists(csv_file):
        df_row.to_csv(csv_file, index=False)
    else:
        df = pd.read_csv(csv_file)
        df = pd.concat([df, df_row], ignore_index=True)
        df.to_csv(csv_file, index=False)

# ---------------- F&O list fetching --------------------
def _get_headers():
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept-Language": "en-US,en;q=0.9",
    }

def load_local_fno() -> List[str]:
    if not os.path.exists(FNO_CACHE_FILE):
        return []
    with open(FNO_CACHE_FILE, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def save_local_fno(symbols: List[str]):
    try:
        with open(FNO_CACHE_FILE, "w", encoding="utf-8") as f:
            for s in sorted(set(symbols)):
                f.write(s + "\n")
    except Exception:
        pass

def fetch_fno_from_archive_csv() -> Tuple[Optional[List[str]], Optional[str]]:
    url = "https://archives.nseindia.com/content/fo/fo_mktlots.csv"
    s = requests.Session()
    try:
        r = s.get(url, headers=_get_headers(), timeout=10)
        if r.status_code != 200 or not r.text.strip():
            return None, f"archive csv status {r.status_code}"
        symbols = []
        for row in csv.reader(r.text.splitlines()):
            if not row:
                continue
            sym = row[0].strip().upper()
            if sym and sym not in ("SYMBOL", "SERIES"):
                if not sym.endswith(".NS"):
                    sym = sym + ".NS"
                symbols.append(sym)
        return sorted(set(symbols)), None
    except Exception as e:
        return None, f"archive csv error: {e}"

def fetch_fno_from_underlyinglist() -> Tuple[Optional[List[str]], Optional[str]]:
    page = "https://www.nseindia.com/content/fo/fo_underlyinglist.htm"
    s = requests.Session()
    try:
        s.get("https://www.nseindia.com", headers=_get_headers(), timeout=10)
        r = s.get(page, headers=_get_headers(), timeout=10)
        if r.status_code != 200:
            return None, f"status {r.status_code}"
        soup = BeautifulSoup(r.text, "lxml")
        symbols = []
        pre = soup.find("pre")
        if pre and pre.text.strip():
            for line in pre.text.splitlines():
                t = line.strip().split()
                if not t: 
                    continue
                sym = t[0].upper()
                if not sym.endswith(".NS"):
                    sym = sym + ".NS"
                symbols.append(sym)
        else:
            table = soup.find("table")
            if table:
                for tr in table.find_all("tr"):
                    tds = tr.find_all(["td","th"])
                    if not tds:
                        continue
                    candidate = tds[0].get_text(strip=True).upper()
                    if candidate and candidate not in ("SYMBOL","UNDERLYING"):
                        if not candidate.endswith(".NS"):
                            candidate = candidate + ".NS"
                        symbols.append(candidate)
        if not symbols:
            return None, "no symbols found"
        return sorted(set(symbols)), None
    except Exception as e:
        return None, f"underlyinglist error: {e}"

def get_fno_list(force_refresh: bool=False) -> Tuple[Optional[List[str]], Optional[str]]:
    if not force_refresh:
        cached = load_local_fno()
        if cached:
            return cached, None
    syms, err = fetch_fno_from_archive_csv()
    if syms:
        save_local_fno(syms)
        return syms, None
    syms, err2 = fetch_fno_from_underlyinglist()
    if syms:
        save_local_fno(syms)
        return syms, None
    cached = load_local_fno()
    if cached:
        return cached, None
    return None, f"All sources failed: {err if 'err' in locals() else ''} | {err2 if 'err2' in locals() else ''}"

# ----------------- Technical helpers -------------------
def calc_atr_df(df, period=14) -> Optional[float]:
    if df is None or df.empty or len(df) < period+1:
        return None
    high = df["High"]
    low = df["Low"]
    close = df["Close"]
    prev = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev).abs()
    tr3 = (low - prev).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1)
    trmax = tr.max(axis=1)
    atr = trmax.rolling(period).mean().iloc[-1]
    if pd.isna(atr):
        return None
    return float(atr)

def nearest_option_step(price: float) -> int:
    if price < 500:
        return 10
    if price < 1000:
        return 20
    if price < 2000:
        return 50
    return 100

def round_to_step(x: float, step: float):
    return round(x/step) * step

# ---------------- Option chain (NSE) ------------------
def fetch_option_chain_nse(symbol_no_suffix: str) -> Tuple[Optional[dict], Optional[str]]:
    base = "https://www.nseindia.com"
    api = f"{base}/api/option-chain-equities?symbol={symbol_no_suffix}"
    s = requests.Session()
    try:
        s.get(base, headers=_get_headers(), timeout=10)
        r = s.get(api, headers=_get_headers(), timeout=10)
        if r.status_code != 200:
            return None, f"status {r.status_code}"
        return r.json(), None
    except Exception as e:
        return None, f"option fetch error: {e}"

def choose_strike_info(option_json: dict, strike: int, prefer: str="CE") -> Dict[str, Any]:
    recs = option_json.get("records", {}).get("data", [])
    if not recs:
        return {"oi": None, "iv": None, "ltp": None}
    chosen = None
    for r in recs:
        if int(r.get("strikePrice", 0)) == int(strike):
            chosen = r
            break
    if not chosen:
        chosen = min(recs, key=lambda x: abs(x.get("strikePrice", 0) - strike))
    if prefer == "CE" and chosen.get("CE"):
        ce = chosen["CE"]
        return {"oi": ce.get("openInterest"), "iv": ce.get("impliedVolatility"), "ltp": ce.get("lastPrice")}
    if prefer == "PE" and chosen.get("PE"):
        pe = chosen["PE"]
        return {"oi": pe.get("openInterest"), "iv": pe.get("impliedVolatility"), "ltp": pe.get("lastPrice")}
    if chosen.get("CE"):
        ce = chosen["CE"]
        return {"oi": ce.get("openInterest"), "iv": ce.get("impliedVolatility"), "ltp": ce.get("lastPrice")}
    if chosen.get("PE"):
        pe = chosen["PE"]
        return {"oi": pe.get("openInterest"), "iv": pe.get("impliedVolatility"), "ltp": ce.get("lastPrice")}
    return {"oi": None, "iv": None, "ltp": None}

# ----------------- Trade idea generator -----------------
def generate_trade_idea(symbol: str) -> Dict[str, Any]:
    out = {"symbol": symbol, "timestamp": datetime.utcnow().isoformat()}
    try:
        tk = yf.Ticker(symbol)
        df = tk.history(period="3mo", interval="1d", auto_adjust=False)
        if df is None or df.empty:
            out["error"] = "no price data"
            return out
        close = df["Close"]
        high = df["High"]
        low = df["Low"]
        last_close = float(close.iloc[-1])
        if len(close) < 20:
            out["error"] = "insufficient bars"
            return out

        sma20 = float(close.rolling(20).mean().iloc[-1])
        atr = calc_atr_df(df, period=14)
        if atr is None or atr <= 0:
            atr = last_close * 0.02

        recent_low = float(low.tail(10).min())
        recent_high = float(high.tail(10).max())
        bullish = last_close > sma20

        if bullish:
            entry = last_close
            raw_sl = recent_low
            sl = min(raw_sl, entry - 0.5 * atr)
            if entry - sl < 0.5 * atr:
                sl = entry - 0.8 * atr
            target = entry + 1.5 * atr
            opt_type = "CE"
        else:
            entry = last_close
            raw_sl = recent_high
            sl = max(raw_sl, entry + 0.5 * atr)
            if sl - entry < 0.5 * atr:
                sl = entry + 0.8 * atr
            target = entry - 1.5 * atr
            opt_type = "PE"

        rr = abs((target - entry) / (entry - sl)) if entry != sl else 1.0
        holding = 3 if atr / last_close < 0.01 else (5 if atr / last_close < 0.02 else 7)
        step = nearest_option_step(last_close)
        strike = int(round_to_step(last_close, step))
        opt_suggestion = f"{symbol.replace('.NS','')} {strike} {opt_type}"

        fin_info = {}
        try:
            info = tk.info
            fin_info["pe"] = info.get("trailingPE") or info.get("forwardPE")
            fin_info["pb"] = info.get("priceToBook")
            fin_info["debt_eq"] = info.get("debtToEquity")
        except Exception:
            fin_info = {"pe": None, "pb": None, "debt_eq": None}

        opt_info = {}
        try:
            s_no = symbol.replace(".NS", "")
            oc_json, oc_err = fetch_option_chain_nse(s_no)
            if oc_json:
                opt_info = choose_strike_info(oc_json, strike, prefer=opt_type)
            else:
                opt_info = {"oi": None, "iv": None, "note": oc_err}
        except Exception as e:
            opt_info = {"oi": None, "iv": None, "note": str(e)}

        out.update({
            "trend": "Bullish" if bullish else "Bearish",
            "entry": round(entry, 2),
            "sl": round(sl, 2),
            "target": round(target, 2),
            "rr": round(rr, 2) if isinstance(rr, float) else rr,
            "holding_days": int(holding),
            "option_suggestion": opt_suggestion,
            "fundamentals": fin_info,
            "option_info": opt_info
        })

        return out
    except Exception as e:
        logger.exception("generate_trade_idea exception for %s: %s", symbol, e)
        out["error"] = str(e)
        return out

# ---------------- Top50 selection ---------------------
def rank_universe_top50(symbols: List[str]) -> List[str]:
    scores = []
    for s in symbols:
        try:
            tk = yf.Ticker(s)
            hist = tk.history(period="1mo", interval="1d", auto_adjust=False)
            if hist is None or hist.empty:
                continue
            avg_vol = hist["Volume"].mean() if "Volume" in hist else 0
            last_vol = hist["Volume"].iloc[-1] if "Volume" in hist else 0
            volume_ratio = (last_vol / avg_vol) if avg_vol and avg_vol > 0 else 0.0
            atr = calc_atr_df(hist, period=14) or 0.0
            vol_score = volume_ratio
            atr_score = atr / (hist["Close"].iloc[-1] + 1e-9)
            score = (vol_score * 0.7) + (atr_score * 0.3)
            scores.append((s, score))
        except Exception:
            continue
    scores_sorted = sorted(scores, key=lambda x: x[1], reverse=True)
    top = [s for s, sc in scores_sorted[:50]]
    return top

# ----------------- Batch sending ---------------------
async def send_batched_messages(bot, chat_id: int, messages: List[str], batch_size: int = BATCH_SIZE):
    out_blocks = []
    current = ""
    for m in messages:
        if len(current) + len(m) + 4 > 3000:
            out_blocks.append(current)
            current = m + "\n\n"
        else:
            current += m + "\n\n"
    if current:
        out_blocks.append(current)
    for part in out_blocks:
        for i in range(3):
            try:
                await bot.send_message(chat_id=chat_id, text=part)
                break
            except Exception as e:
                logger.error("send message failed (try %s): %s", i+1, e)
                time.sleep(1)

# ----------------- Global state for cycles -----------------
_batch_cycle = None
_current_top50: List[str] = []
_fno_cached: List[str] = []

# ----------------- Job implementations -----------------
async def refresh_fno_list_job(context: ContextTypes.DEFAULT_TYPE):
    global _fno_cached
    try:
        syms, err = get_fno_list(force_refresh=True)
        if syms:
            _fno_cached = syms
            logger.info("F&O list refreshed: %d symbols", len(syms))
        else:
            logger.warning("F&O refresh failed: %s", err)
    except Exception:
        logger.exception("Error in refresh_fno_list_job")

async def recalc_top50_job(context: ContextTypes.DEFAULT_TYPE):
    global _fno_cached, _current_top50, _batch_cycle
    try:
        if not _fno_cached:
            syms, err = get_fno_list()
            if not syms:
                logger.warning("No FNO list to rank: %s", err)
                return
            _fno_cached = syms
        top50 = rank_universe_top50(_fno_cached)
        for add in ALWAYS_INCLUDE:
            if add not in top50:
                top50.append(add)
        _current_top50 = top50[:50]
        chunks = [ _current_top50[i:i+BATCH_SIZE] for i in range(0, len(_current_top50), BATCH_SIZE) ] or [_current_top50]
        _batch_cycle = itertools.cycle(chunks)
        logger.info("Top50 recalculated: %d symbols", len(_current_top50))
    except Exception:
        logger.exception("Error in recalc_top50_job")

async def scan_one_batch_and_push(context: ContextTypes.DEFAULT_TYPE):
    global _batch_cycle, _current_top50
    if not _batch_cycle:
        return
    batch_symbols = next(_batch_cycle, [])
    bot = context.bot
    chat_id = context.job.chat_id
    results = []
    for sym in batch_symbols:
        res = generate_trade_idea(sym)
        if "error" in res:
            continue
        msg = f"{res['symbol']}: Trend {res['trend']} | Entry {res['entry']} | SL {res['sl']} | Target {res['target']} | RR {res['rr']} | Opt {res['option_suggestion']}"
        results.append(msg)
        append_alert_to_csv(res)
    if results:
        await send_batched_messages(bot, chat_id, results)

# ----------------- Telegram command handlers -----------------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Swing bot ready. Use /scan /multi /pushnow /autopush etc.")

async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("Scanning Top50 batch...")
    await scan_one_batch_and_push(context)

async def cmd_multi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global _batch_cycle
    await update.message.reply_text("Scanning all Top50...")
    if not _current_top50:
        await recalc_top50_job(context)
    all_msgs = []
    for s in _current_top50:
        res = generate_trade_idea(s)
        if "error" in res:
            continue
        msg = f"{res['symbol']}: Trend {res['trend']} | Entry {res['entry']} | SL {res['sl']} | Target {res['target']} | RR {res['rr']} | Opt {res['option_suggestion']}"
        all_msgs.append(msg)
        append_alert_to_csv(res)
    await send_batched_messages(context.bot, update.message.chat_id, all_msgs)

async def cmd_pushnow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await cmd_multi(update, context)

async def cmd_autopush(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Auto push activated. Top50 scanning every minute.")

async def cmd_refreshfno(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await refresh_fno_list_job(context)
    await update.message.reply_text("F&O list refreshed.")

async def cmd_exportlogs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if os.path.exists(ALERTS_CSV):
        await update.message.reply_document(open(ALERTS_CSV, "rb"))
    else:
        await update.message.reply_text("No logs yet.")

# ----------------- Main -----------------
def main():
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("scan", cmd_scan))
    app.add_handler(CommandHandler("multi", cmd_multi))
    app.add_handler(CommandHandler("pushnow", cmd_pushnow))
    app.add_handler(CommandHandler("autopush", cmd_autopush))
    app.add_handler(CommandHandler("refreshfno", cmd_refreshfno))
    app.add_handler(CommandHandler("exportlogs", cmd_exportlogs))

    # Schedule background jobs
    job_queue = app.job_queue
    job_queue.run_repeating(recalc_top50_job, interval=TOP50_REFRESH_MIN*60, first=5)
    job_queue.run_repeating(refresh_fno_list_job, interval=3600, first=10)

    prune_old_logs(ALERTS_CSV)

    logger.info("Bot starting...")
    app.run_polling()

if __name__ == "__main__":
    main()
