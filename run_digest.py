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


# Map your new JSON section IDs -> existing email layout keys
SECTION_ALIASES = {
    "critical_signals": "big_signals",
    "work_intelligence": "supply_chain",
    "macro_markets": "markets",
    "tech_tools": "tech_ai_apple",
    "blogs_essays": "deep_dive",      # essays/longreads as deep dive bucket for now
    "gaming": "gaming_major",
    "podcasts": "podcasts_new",
    "books": "deep_dive"             # books can also live under deep dive for now
}


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
# Config / helpers
# -------------------------
def load_config(path: str = "sources.json") -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


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
# Scoring / selection
# -------------------------
def basic_score(published_utc: Optional[datetime], summary_len: int) -> float:
    score = 0.0
    if published_utc:
        age_h = (now_utc() - published_utc).total_seconds() / 3600.0
        score += max(0.0, 1.0 - min(age_h / 24.0, 1.0)) * 0.8
    score += min(summary_len, 220) / 220.0 * 0.4
    return score


def dedupe(items: List[Item]) -> List[Item]:
    seen_links = set()
    seen_titles = set()
    out: List[Item] = []
    for it in sorted(items, key=lambda x: x.score, reverse=True):
        lk = (it.link or "").strip().lower()
        tk = norm_title(it.title)
        if lk in seen_links or tk in seen_titles:
            continue
        seen_links.add(lk)
        seen_titles.add(tk)
        out.append(it)
    return out


def pick(items: List[Item], n: int) -> List[Item]:
    return sorted(items, key=lambda x: x.score, reverse=True)[:n]


def cap_per_source(items: List[Item], max_per_source: int) -> List[Item]:
    if max_per_source <= 0:
        return items
    out: List[Item] = []
    counts: Dict[str, int] = {}
    for it in sorted(items, key=lambda x: x.score, reverse=True):
        counts[it.source] = counts.get(it.source, 0) + 1
        if counts[it.source] <= max_per_source:
            out.append(it)
    return out


# -------------------------
# Fetch RSS for a JSON section
# -------------------------
def fetch_section_from_json(section_id: str, sources: List[Dict[str, Any]], since_utc: datetime) -> List[Item]:
    out: List[Item] = []
    mapped_section = SECTION_ALIASES.get(section_id, section_id)

    for src in sources:
        if (src.get("type") or "").lower() != "rss":
            continue
        url = (src.get("url") or "").strip()
        if not url or url.startswith("TODO_"):
            continue

        feed = feedparser.parse(url)
        feed_title = getattr(feed.feed, "title", src.get("name") or url)

        for e in feed.entries:
            dt = parse_entry_datetime(e)
            if dt and dt < since_utc:
                continue

            title = clean_text(getattr(e, "title", ""), 140)
            link = (getattr(e, "link", "") or "").strip()

            summary_raw = getattr(e, "summary", "") or getattr(e, "description", "") or ""
            summary = clean_text(summary_raw, 220)

            if not title or not link:
                continue

            it = Item(
                section=mapped_section,
                source=f"RSS: {feed_title}",
                title=title,
                link=link,
                published_utc=dt,
                summary=summary,
                score=0.0,
            )
            it.score = basic_score(dt, len(summary))
            out.append(it)

    return out


# -------------------------
# Email rendering
# -------------------------
def section_block(emoji: str, heading: str, items: List[Item]) -> str:
    if not items:
        return ""

    lis = []
    for it in items:
        lis.append(
            f"""
            <li style="margin:0 0 12px 0;">
              <div style="font-weight:700;">{esc(it.title)}</div>
              <div style="margin-top:4px;">{esc(it.summary)}</div>
              <div style="margin-top:4px;font-size:12px;color:#555;">
                <a href="{esc(it.link)}">Open source</a> · {esc(it.source)}
              </div>
            </li>
            """
        )

    return f"""
    <h2 style="margin:22px 0 8px 0;">{emoji} {esc(heading)}</h2>
    <ul style="padding-left:18px;margin:0;">
      {''.join(lis)}
    </ul>
    """


def render_email_html(cfg: Dict[str, Any], sections: Dict[str, List[Item]]) -> str:
    meta = cfg.get("meta", {}) or {}
    title = meta.get("digest_name", "Jeffrey’s Daily")
    date_str = format_date_local(now_utc(), meta.get("timezone", "Europe/Amsterdam"))

    html_out = f"""
    <div style="font-family:-apple-system,Segoe UI,Roboto,Arial; line-height:1.35;">
      <h1 style="margin:0 0 6px 0;">{esc(title)} — 19:00</h1>
      <div style="color:#666;margin:0 0 18px 0;">{esc(date_str)} · Leestijd: ±10–20 min</div>

      {section_block("🔝", "Big Signals Today", sections.get("big_signals", []))}
      {section_block("📦", "Supply Chain & Business", sections.get("supply_chain", []))}
      {section_block("🔎", "Deep dive", sections.get("deep_dive", []))}
      {section_block("📈", "Markets & Investing", sections.get("markets", []))}
      {section_block("🤖", "Tech / AI / Apple", sections.get("tech_ai_apple", []))}
      {section_block("🎮", "Gaming (major only)", sections.get("gaming_major", []))}
      {section_block("🎧", "Nieuwe podcasts", sections.get("podcasts_new", []))}

      <div style="margin-top:26px;color:#777;font-size:12px;">
        Tip: scan, klik alleen wat je aanspreekt — geen doomscroll nodig 🙂
      </div>
    </div>
    """
    return html_out


def send_via_resend(subject: str, html_body: str) -> None:
    api_key = os.environ["RESEND_API_KEY"]
    to_email = os.environ["DIGEST_TO_EMAIL"]
    from_email = os.environ["DIGEST_FROM_EMAIL"]

    payload = {"from": from_email, "to": [to_email], "subject": subject, "html": html_body}

    r = requests.post(
        "https://api.resend.com/emails",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )
    if r.status_code >= 300:
        raise RuntimeError(f"Resend error {r.status_code}: {r.text}")


# -------------------------
# Main
# -------------------------
def main() -> None:
    cfg = load_config("sources.json")
    lookback_hours = int(cfg.get("lookback_hours", 24))
    since = now_utc() - timedelta(hours=lookback_hours)

    # Build section limits dict from JSON sections list
    sec_limits: Dict[str, int] = {}
    for s in (cfg.get("sections") or []):
        sid = s.get("id")
        if not sid:
            continue
        mapped = SECTION_ALIASES.get(sid, sid)
        sec_limits[mapped] = int(s.get("max_items", 5))

    # Fetch
    all_items: List[Item] = []
    for s in (cfg.get("sections") or []):
        sid = s.get("id")
        if not sid:
            continue
        sources = s.get("sources") or []
        all_items.extend(fetch_section_from_json(sid, sources, since))

    # Dedupe globally
    all_items = dedupe(all_items)

    # Group by section (mapped keys)
    by_section: Dict[str, List[Item]] = {}
    for it in all_items:
        by_section.setdefault(it.section, []).append(it)

    # Apply caps (simple default to avoid 10x same publisher)
    sections_out: Dict[str, List[Item]] = {}
    for sec, items in by_section.items():
        items = cap_per_source(items, 3)
        sections_out[sec] = pick(items, sec_limits.get(sec, 5))

    subject = f"{cfg.get('meta', {}).get('digest_name', 'Jeffrey’s Daily')} — {datetime.now().strftime('%Y-%m-%d')}"
    html_body = render_email_html(cfg, sections_out)
    send_via_resend(subject, html_body)


if __name__ == "__main__":
    main()
