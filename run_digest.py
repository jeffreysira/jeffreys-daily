# =============================
# Jeffrey’s Daily – run_digest.py
# =============================

import json
import os
import re
import html
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import feedparser
import requests
from dateutil import tz

# -------------------------
# SECTION MAPPING
# -------------------------

SECTION_ALIASES = {
    "critical_signals": "big_signals",
    "work_intelligence": "supply_chain",
    "macro_markets": "markets",
    "tech_tools": "tech_ai_apple",
    "blogs_essays": "deep_dive",
    "gaming": "gaming_major",
    "podcasts": "podcasts_new"
}

# -------------------------
# DATA CLASS
# -------------------------

@dataclass
class Item:
    section: str
    source: str
    title: str
    link: str
    published_utc: Optional[datetime]
    summary: str
    score: float


# -------------------------
# CONFIG
# -------------------------

def load_config(path: str = "sources.json") -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


# -------------------------
# HELPERS
# -------------------------

def parse_entry_datetime(entry) -> Optional[datetime]:
    t = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
    if not t:
        return None
    return datetime.fromtimestamp(time.mktime(t), tz=timezone.utc)


def strip_html(s: str) -> str:
    return re.sub(r"<[^>]+>", "", s or "")


def clean_text(s: str, max_len: int) -> str:
    s = strip_html(s)
    s = re.sub(r"\s+", " ", s).strip()
    if len(s) > max_len:
        s = s[: max_len - 1].rstrip() + "…"
    return s


def norm_title(t: str) -> str:
    t = re.sub(r"\s+", " ", (t or "").lower()).strip()
    t = re.sub(r"[^a-z0-9 ]+", "", t)
    return t[:120]


def format_date_local(dt_utc: datetime, tz_name: str = "Europe/Amsterdam") -> str:
    local_tz = tz.gettz(tz_name)
    return dt_utc.astimezone(local_tz).strftime("%A %d %B %Y")


def esc(s: str) -> str:
    return html.escape(s or "")


# -------------------------
# SCORING
# -------------------------

def basic_score(published_utc: Optional[datetime], summary_len: int) -> float:
    score = 0.0
    if published_utc:
        age_h = (now_utc() - published_utc).total_seconds() / 3600.0
        score += max(0.0, 1.0 - min(age_h / 24.0, 1.0)) * 0.8
    score += min(summary_len, 220) / 220.0 * 0.4
    return score


# -------------------------
# DEDUPE / PICK
# -------------------------

def dedupe(items: List[Item]) -> List[Item]:
    seen_links = set()
    seen_titles = set()
    out: List[Item] = []

    for it in sorted(items, key=lambda x: x.score, reverse=True):
        lk = it.link.lower()
        tk = norm_title(it.title)

        if lk in seen_links or tk in seen_titles:
            continue

        seen_links.add(lk)
        seen_titles.add(tk)
        out.append(it)

    return out


def pick(items: List[Item], n: int) -> List[Item]:
    return sorted(items, key=lambda x: x.score, reverse=True)[:n]


# -------------------------
# RSS FETCH
# -------------------------

def fetch_rss_section(section: str, urls: List[str], since_utc: datetime) -> List[Item]:

    mapped_section = SECTION_ALIASES.get(section, section)

    out: List[Item] = []

    for url in urls:
        feed = feedparser.parse(url)
        feed_title = getattr(feed.feed, "title", url)

        for e in feed.entries:

            dt = parse_entry_datetime(e)
            if dt and dt < since_utc:
                continue

            title = clean_text(getattr(e, "title", ""), 140)
            link = getattr(e, "link", "").strip()
            summary = clean_text(getattr(e, "summary", "") or "", 220)

            if not title or not link:
                continue

            item = Item(
                section=mapped_section,
                source=f"RSS: {feed_title}",
                title=title,
                link=link,
                published_utc=dt,
                summary=summary,
                score=0.0
            )

            item.score = basic_score(dt, len(summary))
            out.append(item)

    return out


# -------------------------
# EMAIL RENDER
# -------------------------

def section_block(title: str, items: List[Item]) -> str:

    if not items:
        return ""

    lis = []

    for it in items:
        lis.append(f"""
        <li>
        <b>{esc(it.title)}</b><br>
        {esc(it.summary)}<br>
        <small><a href="{esc(it.link)}">Open source</a> · {esc(it.source)}</small>
        </li>
        """)

    return f"<h2>{title}</h2><ul>{''.join(lis)}</ul>"


def render_email_html(cfg: Dict[str, Any], sections: Dict[str, List[Item]]) -> str:

    date_str = format_date_local(now_utc())

    return f"""
    <h1>{cfg.get("meta", {}).get("digest_name", "Jeffrey’s Daily")} — 19:00</h1>
    <div>{date_str}</div>

    {section_block("🔝 Big Signals Today", sections.get("big_signals", []))}
    {section_block("📦 Supply Chain & Business", sections.get("supply_chain", []))}
    {section_block("🔎 Deep dive", sections.get("deep_dive", []))}
    {section_block("📈 Markets & Investing", sections.get("markets", []))}
    {section_block("🤖 Tech / AI / Apple", sections.get("tech_ai_apple", []))}
    {section_block("🎮 Gaming", sections.get("gaming_major", []))}
    {section_block("🎧 Podcasts", sections.get("podcasts_new", []))}
    """


# -------------------------
# SEND MAIL
# -------------------------

def send_via_resend(subject: str, html_body: str):

    r = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {os.environ['RESEND_API_KEY']}",
            "Content-Type": "application/json"
        },
        json={
            "from": os.environ["DIGEST_FROM_EMAIL"],
            "to": [os.environ["DIGEST_TO_EMAIL"]],
            "subject": subject,
            "html": html_body
        }
    )

    if r.status_code >= 300:
        raise RuntimeError(r.text)


# -------------------------
# MAIN
# -------------------------

def main():

    cfg = load_config("sources.json")
    since = now_utc() - timedelta(hours=24)

    all_items: List[Item] = []

    rss_cfg = cfg.get("rss", {})

    for section, urls in rss_cfg.items():
        all_items.extend(fetch_rss_section(section, urls, since))

    all_items = dedupe(all_items)

    by_section: Dict[str, List[Item]] = {}

    for it in all_items:
        by_section.setdefault(it.section, []).append(it)

    sec_limits_raw = cfg.get("sections", [])

    if isinstance(sec_limits_raw, list):
        sec_limits = {}
        for s in sec_limits_raw:
            sid = s.get("id")
            if sid:
                sid = SECTION_ALIASES.get(sid, sid)
                sec_limits[sid] = s
    else:
        sec_limits = sec_limits_raw

    sections: Dict[str, List[Item]] = {}

    for sec, items in by_section.items():
        max_items = sec_limits.get(sec, {}).get("max_items", 5)
        sections[sec] = pick(items, max_items)

    subject = f"Jeffrey’s Daily — {datetime.now().strftime('%Y-%m-%d')}"
    html_body = render_email_html(cfg, sections)

    send_via_resend(subject, html_body)


if __name__ == "__main__":
    main()
