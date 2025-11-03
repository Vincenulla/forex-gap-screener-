#!/usr/bin/env python3
# gap_screener.py
import os
import yaml
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import pytz
import smtplib
from email.message import EmailMessage
import argparse
import sys
import logging

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------- Config ----------
HERE = os.path.dirname(__file__)
CONFIG_PATH = os.path.join(HERE, "config.yaml")

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

PAIRS = cfg.get("pairs", [])
TZ = pytz.timezone("Europe/Paris")

# SMTP envs (used only if not dry-run)
SMTP_HOST = os.environ.get("SMTP_HOST")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587")) if os.environ.get("SMTP_PORT") else None
SMTP_USER = os.environ.get("SMTP_USER")
SMTP_PASS = os.environ.get("SMTP_PASS")
EMAIL_FROM = os.environ.get("EMAIL_FROM") or os.environ.get("SMTP_USER")
EMAIL_TO = os.environ.get("EMAIL_TO")  # comma separated

def yf_download_safe(ticker, period="10d", interval="1h"):
    """wrapper to call yfinance with minimal exceptions handled"""
    try:
        df = yf.download(ticker, period=period, interval=interval, progress=False, threads=False)
        return df
    except Exception as e:
        logging.warning(f"yfinance error for {ticker}: {e}")
        return pd.DataFrame()

def get_friday_close_and_sunday_open(ticker):
    """
    Retourne (friday_time, friday_close, sunday_time, sunday_open)
    Les timestamps sont timezone-aware en Europe/Paris.
    - friday_close: dernière bougie horaire du vendredi (Close)
    - sunday_open: première bougie horaire du dimanche avec index.hour >= 22 (Open)
    """
    df = yf_download_safe(ticker, period="12d", interval="1h")
    if df.empty:
        return None, None, None, None

    # yfinance dataframe usually naive (UTC) -> localize + convert
    if df.index.tz is None:
        df = df.tz_localize("UTC").tz_convert(TZ)
    else:
        df = df.tz_convert(TZ)
    df = df.sort_index()

    # friday rows
    friday_rows = df[df.index.weekday == 4]
    if friday_rows.empty:
        return None, None, None, None
    friday_time = friday_rows.index.max()
    friday_close = float(friday_rows.loc[friday_time, "Close"])

    # sunday rows at/after 22:00 (we pick first)
    sunday_candidates = df[(df.index.weekday == 6) & (df.index.hour >= 22)]
    if sunday_candidates.empty:
        # fallback: any sunday row
        sunday_rows = df[df.index.weekday == 6]
        if sunday_rows.empty:
            return friday_time, friday_close, None, None
        sunday_time = sunday_rows.index.min()
        sunday_open = float(df.loc[sunday_time, "Open"] if "Open" in df.columns else df.loc[sunday_time, "Close"])
        return friday_time, friday_close, sunday_time, sunday_open

    sunday_time = sunday_candidates.index.min()
    sunday_open = float(df.loc[sunday_time, "Open"] if "Open" in df.columns else df.loc[sunday_time, "Close"])
    return friday_time, friday_close, sunday_time, sunday_open

def build_report(rows):
    df = pd.DataFrame(rows, columns=["pair", "friday_time", "friday_close", "sunday_time", "sunday_open", "gap_pct", "note"])
    df["abs_gap"] = df["gap_pct"].abs().fillna(0)
    df = df.sort_values("abs_gap", ascending=False).drop(columns=["abs_gap"])
    return df

def send_email(subject, body, csv_content, smtp_host, smtp_port, smtp_user, smtp_pass, email_from, email_to_list):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = email_from
    msg["To"] = ", ".join(email_to_list)
    msg.set_content(body)
    msg.add_attachment(csv_content, filename=f"forex_gaps_{datetime.now().strftime('%Y%m%d_%H%M')}.csv", subtype="csv")
    with smtplib.SMTP(smtp_host, smtp_port) as s:
        s.starttls()
        s.login(smtp_user, smtp_pass)
        s.send_message(msg)

def main():
    parser = argparse.ArgumentParser(description="Forex gap screener (Friday close -> Sunday Asian open)")
    parser.add_argument("--dry-run", action="store_true", help="Ne pas envoyer d'email; écrire CSV localement et afficher le rapport")
    parser.add_argument("--pairs", type=str, help="(optional) comma-separated list of tickers to override config.yaml")
    args = parser.parse_args()

    pairs = PAIRS
    if args.pairs:
        pairs = [p.strip() for p in args.pairs.split(",") if p.strip()]

    logging.info(f"Pairs to analyze: {len(pairs)}")

    rows = []
    for ticker in pairs:
        try:
            f_time, f_close, s_time, s_open = get_friday_close_and_sunday_open(ticker)
            note = ""
            if f_close is None:
                note = "no friday data"
            if s_open is None:
                note = "no sunday open"
            gap_pct = None
            if isinstance(f_close, float) and isinstance(s_open, float) and f_close != 0:
                gap_pct = (s_open - f_close) / f_close * 100.0
            rows.append((ticker, str(f_time) if f_time is not None else None, f_close, str(s_time) if s_time is not None else None, s_open, gap_pct, note or None))
        except Exception as e:
            logging.exception(f"Error processing {ticker}: {e}")
            rows.append((ticker, None, None, None, None, None, f"error: {e}"))

    report_df = build_report(rows)

    executed_at = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
    total_pairs = len(report_df)

    lines = [f"Gaps Forex - Exécution : {executed_at}",
             f"Paires analysées : {total_pairs}",
             "",
             "Liste (pair | friday_close_time | friday_close | sunday_open_time | sunday_open | gap % | note):",
             ""]
    for _, r in report_df.iterrows():
        pair = r["pair"]
        if isinstance(r["gap_pct"], float):
            lines.append(f"{pair} | {r['friday_time']} | {r['friday_close']:.6f} | {r['sunday_time']} | {r['sunday_open']:.6f} | {r['gap_pct']:+.4f}% | {r['note'] or ''}")
        else:
            lines.append(f"{pair} | {r['friday_time']} | {r['friday_close']} | {r['sunday_time']} | {r['sunday_open']} | {r['gap_pct']} | {r['note'] or ''}")

    body = "\n".join(lines)
    csv_buf = report_df.to_csv(index=False)

    if args.dry_run:
        # write csv locally and print summary
        outname = os.path.join(HERE, f"forex_gaps_dryrun_{datetime.now().strftime('%Y%m%d_%H%M')}.csv")
        with open(outname, "w", encoding="utf-8") as f:
            f.write(csv_buf)
        logging.info(f"DRY-RUN: CSV written to {outname}")
        print(body)
        return 0

    # Check SMTP envs before sending
    if not all([SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, EMAIL_FROM, EMAIL_TO]):
        logging.error("SMTP variables manquantes. Définit SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, EMAIL_FROM, EMAIL_TO.")
        print(body)
        return 2

    email_to_list = [e.strip() for e in EMAIL_TO.split(",")]
    subject = f"Gaps Forex - {executed_at}"
    send_email(subject, body, csv_buf, SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, EMAIL_FROM, email_to_list)
    logging.info("Email envoyé.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
