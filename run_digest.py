import json
import os
import re
import html
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import feedparser
import requests
from dateutil import tz

# Optional Reddit
try:
    import praw
except Exception:
    praw = None


@dataclass
class Item:
    section: str
    source: str
    title: str
    link: str
    published_utc: Optional[datetime]
    summary: str
    score: float


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


def clean_text(s: str, max_len: int = 240) -> str:
    s = re.sub(r"\s+", " ", s or "").strip()
    s = re.sub(r"<[^>]+>", "", s)  # strip HTML tags
    if len(s) > max_len:
        s = s[: max_len - 1].rstrip() + "…"
    return s


def is_major_gaming(title: str, summary: str) -> bool:
    t = (title or "").lower()
    s = (summary or "").lower()
    keywords = [
        "release date", "launch", "review", "metacritic", "trailer", "announcement",
        "gta", "elder scrolls", "witcher", "nintendo", "playstation", "xbox",
        "rockstar", "bethesda"
    ]
    return any(k in t or k in s for k in keywords)


def basic_score(item: Item) -> float:
    base = 0.0
    base += min(len(item.summary), 240) / 240.0  # more content -> higher score
    if item.published_utc:
        age_h = (now_utc() - item.published_utc).total_seconds() / 3600.0
        base += max(0.0, 1.0 - min(age_h / 24.0, 1.0)) * 0.6  # freshness bonus
    return base


def fetch_rss_section(section: str, urls: List[str], since_utc: datetime) -> List[Item]:
    out: List[Item] = []
    for url in urls:
        feed = feedparser.parse(url)
        feed_title = getattr(feed.feed, "title", url)
        for e in feed.entries:
            dt = parse_entry_datetime(e)
            if dt and dt < since_utc:
                continue
            title = clean_text(getattr(e, "title", ""))
            link = clean_text(getattr(e, "link", ""), 500)
            summary_raw = getattr(e, "summary", "") or getattr(e, "description", "")
            summary = clean_text(summary_raw, 260)
            if not title or not link:
                continue

            # Gaming "major only"
            if section == "gaming_major" and not is_major_gaming(title, summary):
                continue

            item = Item(
                section=section,
                source=f"RSS: {feed_title}",
                title=title,
                link=link,
                published_utc=dt,
                summary=summary,
                score=0.0,
            )
            item.score = basic_score(item)
            out.append(item)
    return out


def fetch_reddit(cfg: Dict[str, Any], since_utc: datetime) -> List[Item]:
    if not cfg.get("enabled"):
        return []
    if praw is None:
        return []  # keep MVP running even without praw installed

    client_id = os.environ.get("REDDIT_CLIENT_ID")
    client_secret = os.environ.get("REDDIT_CLIENT_SECRET")
    user_agent = os.environ.get("REDDIT_USER_AGENT", "jeffreys-daily/0.1")

    if not client_id or not client_secret:
        # If creds are missing, skip Reddit quietly for MVP
        return []

    r = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)
    min_upvotes = int(cfg.get("min_upvotes", 0))
    limit = int(cfg.get("limit_per_subreddit", 15))

    items: List[Item] = []
    for sub in cfg.get("subreddits", []):
        subreddit = r.subreddit(sub)
        for post in subreddit.hot(limit=limit):
            created = datetime.fromtimestamp(post.created_utc, tz=timezone.utc)
            if created < since_utc:
                continue
            ups = int(getattr(post, "ups", 0))
            if ups < min_upvotes:
                continue

            title = clean_text(post.title, 160)
            link = f"https://www.reddit.com{post.permalink}"
            summary = clean_text(getattr(post, "selftext", "") or "", 240)
            if not summary:
                summary = f"Reddit highlight (▲{ups})"
            it = Item(
                section="reddit",
                source=f"Reddit: r/{sub} (▲{ups})",
                title=title,
                link=link,
                published_utc=created,
                summary=summary,
                score=0.0,
            )
            it.score = basic_score(it) + min(0.4, ups / 1000.0)  # small popularity boost
            items.append(it)
    return items


def dedupe(items: List[Item]) -> List[Item]:
    seen = set()
    out = []
    for it in sorted(items, key=lambda x: x.score, reverse=True):
        key = it.link.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out


def pick(items: List[Item], n: int) -> List[Item]:
    return sorted(items, key=lambda x: x.score, reverse=True)[:n]


def format_date_local(dt_utc: datetime, tz_name: str = "Europe/Amsterdam") -> str:
    local_tz = tz.gettz(tz_name)
    return dt_utc.astimezone(local_tz).strftime("%A %d %B %Y")


def esc(s: str) -> str:
    return html.escape(s or "")


def render_email_html(cfg: Dict[str, Any], sections: Dict[str, List[Item]]) -> str:
    title = cfg.get("title", "Jeffrey’s Daily")
    date_str = format_date_local(now_utc())

    def section_block(emoji: str, heading: str, items: List[Item]) -> str:
        if not items:
            return ""
        lis = []
        for it in items:
            lis.append(
                f"""
                <li style="margin: 0 0 12px 0;">
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

    # Build blocks in your preferred order
    html_out = f"""
    <div style="font-family: -apple-system, Segoe UI, Roboto, Arial; line-height:1.35;">
      <h1 style="margin:0 0 6px 0;">{esc(title)} — 19:00</h1>
      <div style="color:#666;margin:0 0 18px 0;">{esc(date_str)} · Leestijd: ±10 min</div>

      {section_block("🔝", "Big Signals Today", sections.get("big_signals", []))}
      {section_block("📦", "Supply Chain & Business", sections.get("supply_chain", []))}
      {section_block("🔎", "Deep dive", sections.get("deep_dive", []))}
      {section_block("📈", "Markets & Investing", sections.get("markets", []))}
      {section_block("🤖", "Tech / AI / Apple", sections.get("tech_ai_apple", []))}
      {section_block("🌍", "Europa / NL", sections.get("europe_nl", []))}
      {section_block("🧩", "High-signal Reddit", sections.get("reddit", []))}
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

    payload = {
        "from": from_email,
        "to": [to_email],
        "subject": subject,
        "html": html_body,
    }

    r = requests.post(
        "https://api.resend.com/emails",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )
    if r.status_code >= 300:
        raise RuntimeError(f"Resend error {r.status_code}: {r.text}")


def main():
    cfg = load_config("sources.json")
    since = now_utc() - timedelta(hours=int(cfg.get("lookback_hours", 24)))

    all_items: List[Item] = []

    rss_cfg = cfg.get("rss", {})
    for section, urls in rss_cfg.items():
        all_items.extend(fetch_rss_section(section, urls, since))

    all_items.extend(fetch_reddit(cfg.get("reddit", {}), since))
    all_items = dedupe(all_items)

    # Split by section
    by_section: Dict[str, List[Item]] = {}
    for it in all_items:
        by_section.setdefault(it.section, []).append(it)

    sec_limits = cfg.get("sections", {})

    # Build sections with limits
    sections: Dict[str, List[Item]] = {}

    # Big signals: pick top from across several sections (except podcasts)
    candidates = []
    for k in ["supply_chain", "markets", "tech_ai_apple", "europe_nl"]:
        candidates.extend(by_section.get(k, []))
    sections["big_signals"] = pick(candidates, int(sec_limits.get("big_signals", {}).get("max_items", 5)))

    # Supply chain main block (exclude the one we use for deep dive to avoid repetition)
    supply = pick(by_section.get("supply_chain", []), int(sec_limits.get("supply_chain", {}).get("max_items", 8)))

    # Deep dive: pick top 1 supply chain/business item that has a bit more text (heuristic)
    deep_candidates = sorted(by_section.get("supply_chain", []), key=lambda x: (len(x.summary), x.score), reverse=True)
    deep = deep_candidates[: int(sec_limits.get("deep_dive", {}).get("max_items", 1))]

    # Remove deep dive item from supply list if same link
    deep_links = {d.link for d in deep}
    supply = [s for s in supply if s.link not in deep_links]

    sections["supply_chain"] = supply
    sections["deep_dive"] = deep

    # Regular sections
    for k in ["markets", "tech_ai_apple", "europe_nl", "reddit", "gaming_major", "podcasts_new"]:
        sections[k] = pick(by_section.get(k, []), int(sec_limits.get(k, {}).get("max_items", 5)))

    subject = f"{cfg.get('title', 'Jeffrey’s Daily')} — {datetime.now().strftime('%Y-%m-%d')}"
    html_body = render_email_html(cfg, sections)
    send_via_resend(subject, html_body)


if __name__ == "__main__":
    main()
