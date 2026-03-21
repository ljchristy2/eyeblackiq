"""
EyeBlackIQ — alert_handler.py
Sends alerts via Telegram and Gmail.
Used for kill switches, daily slips, and graded results.
"""
import os
import smtplib
import logging
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import Optional

load_dotenv()
logger = logging.getLogger(__name__)

TG_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT   = os.getenv("TELEGRAM_CHAT_ID", "")
EMAIL_TO  = os.getenv("EMAIL_TO",   "ljchristy2@gmail.com")
EMAIL_FROM = os.getenv("EMAIL_FROM", EMAIL_TO)
EMAIL_PASS = os.getenv("GOOGLE_APP_PASSWORD", "")

TG_MAX_CHARS = 4000


def _sanitize_tg(text: str) -> str:
    """Remove HTML/markdown chars that break plain Telegram messages."""
    return text.replace("<", "[").replace(">", "]").replace("&", "+")


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def send_telegram(text: str, chat_id: Optional[str] = None) -> bool:
    """
    Send plain text message to Telegram. Splits into chunks if > 4000 chars.

    Returns True if all chunks sent successfully.
    """
    if not TG_TOKEN or not (chat_id or TG_CHAT):
        logger.warning("Telegram credentials not set — skipping")
        return False

    cid = chat_id or TG_CHAT
    clean = _sanitize_tg(text)
    chunks = [clean[i:i+TG_MAX_CHARS] for i in range(0, len(clean), TG_MAX_CHARS)]

    for i, chunk in enumerate(chunks):
        payload = {"chat_id": cid, "text": chunk}
        r = requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json=payload, timeout=15
        )
        if not r.ok:
            logger.error(f"Telegram chunk {i+1}/{len(chunks)} failed: {r.text[:200]}")
            return False
        logger.info(f"Telegram chunk {i+1}/{len(chunks)} sent OK")

    return True


def send_email(subject: str, body_html: str, body_text: Optional[str] = None) -> bool:
    """
    Send HTML email via Gmail SMTP.

    Returns True if sent successfully.
    """
    if not EMAIL_PASS or not EMAIL_FROM:
        logger.warning("Email credentials not set — skipping")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = EMAIL_FROM
    msg["To"]      = EMAIL_TO

    if body_text:
        msg.attach(MIMEText(body_text, "plain"))
    msg.attach(MIMEText(body_html, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
            server.login(EMAIL_FROM, EMAIL_PASS)
            server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
        logger.info(f"Email sent to {EMAIL_TO}")
        return True
    except Exception as e:
        logger.error(f"Email failed: {e}")
        return False


def send_kill_switch_alert(reason: str) -> None:
    """Send urgent kill switch notification via both channels."""
    subject = "KILL SWITCH TRIGGERED — EyeBlackIQ"
    body = f"<h2>KILL SWITCH</h2><p>{reason}</p><p>All new bets paused. Human review required.</p>"
    send_telegram(f"KILL SWITCH TRIGGERED\n\n{reason}\n\nAll new bets paused. Human review required.")
    send_email(subject, body)


def send_daily_slip(slip_text: str, slip_html: Optional[str] = None) -> None:
    """Send daily bet slip."""
    from datetime import datetime
    today = datetime.now().strftime("%A, %B %d %Y")
    subject = f"EyeBlackIQ — Daily Slip {today}"
    send_telegram(slip_text)
    if slip_html:
        send_email(subject, slip_html, slip_text)


def send_graded_results(results_text: str) -> None:
    """Send graded results summary."""
    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")
    subject = f"EyeBlackIQ — Results {today}"
    send_telegram(results_text)
    send_email(subject, f"<pre>{results_text}</pre>", results_text)
