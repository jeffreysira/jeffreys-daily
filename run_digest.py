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

# Optional Reddit (API)
try:
    import praw  # type: ignore
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


def contains_any(text: str, keywords: List[str]) -> bool:
    t = text.lower()
    return any(k.lower() in t for k in keywords)


def keyword_blocked(section: str, title: str, summary: str, cfg: Dict[str, Any]) -> bool:
    filters = (cfg.get("filters") or {}).get(section) or {}
    inc = [k.lower() for k in filters.get("include_keywords", [])]
    exc = [k.lower() for k in filters.get("exclude_keywords", [])]
    text = f"{title} {summary}".lower()

    # If include list exists, require at least one match
    if inc and not any(k in text for k in inc):
        return True
    # If exclude list matches, block
    if exc and any(k in text for k in exc):
        return True
    return False


def is_major_gaming(title: str, summary: str, cfg: Dict[str, Any]) -> bool:
    rules = (cfg.get("gaming_rules") or {})
    must = rules.get("major_keywords", [])
    if must:
        return contains_any(f"{title} {summary}", must)
    # fallback
    fallback = [
        "release date", "launch", "review", "metacritic", "trailer", "announcement",
        "gta", "elder scrolls", "witcher", "nintendo", "playstation", "xbox",
        "rockstar", "bethesda", "insomniac"
    ]
    return contains_any(f"{title} {summary}", fallback)


# -------------------------
# Scoring / selection
# -------------------------
def basic_score(published_utc: Optional[datetime], summary_len: int) -> float:
    score = 0.0
    # prefer newer items
    if published_utc:
        age_h = (now_utc() - published_utc).total_seconds() / 3600.0
        score += max(0.0, 1.0 - min(age_h / 24.0, 1.0)) * 0.8
    # prefer items that actually have content
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


def pick(items: List[Item], n: int) -> List[Item]:
    return sorted(items, key=lambda x: x.score, reverse=True)[:n]


# -------------------------
# Fetchers
# -------------------------
def fetch_rss_section(section: str, urls: List[str], since_utc: datetime, cfg: Dict[str, Any]) -> List[Item]:
    out: List[Item] = []

    # summary length per section
    sum_len = int((cfg.get("summary_maxlen") or {}).get(section, cfg.get("summary_default_maxlen", 180)))

    for url in urls:
        feed = feedparser.parse(url)
        feed_title = getattr(feed.feed, "title", url)

        for e in feed.entries:
            dt = parse_entry_datetime(e)
            if dt and dt < since_utc:
                continue

            title = clean_text(getattr(e, "title", ""), 140)
            link = (getattr(e, "link", "") or "").strip()
            summary_raw = getattr(e, "summary", "") or getattr(e, "description", "") or ""
            summary = clean_text(summary_raw, sum_len)

            if not title or not link:
                continue

            if keyword_blocked(section, title, summary, cfg):
                continue

            # Gaming section: only major
            if section == "gaming_major" and not is_major_gaming(title, summary, cfg):
                continue

            src = f"RSS: {feed_title}"
            it = Item(
                section=section,
                source=src,
                title=title,
                link=link,
                published_utc=dt,
                summary=summary,
                score=0.0,
            )
            it.score = basic_score(dt, len(summary))

            out.append(it)

    return out


def fetch_reddit(cfg: Dict[str, Any], since_utc: datetime) -> List[Item]:
    reddit_cfg = cfg.get("reddit", {}) or {}
    if not reddit_cfg.get("enabled"):
        return []

    # If praw not installed or no creds, quietly skip for MVP
    if praw is None:
        return []

    client_id = os.environ.get("REDDIT_CLIENT_ID")
    client_secret = os.environ.get("REDDIT_CLIENT_SECRET")
    user_agent = os.environ.get("REDDIT_USER_AGENT", "jeffreys-daily/0.2")

    if not client_id or not client_secret:
        return []

    r = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)

    limit = int(reddit_cfg.get("limit_per_subreddit", 15))
    min_upvotes = int(reddit_cfg.get("min_upvotes", 150))

    sum_len = int((cfg.get("summary_maxlen") or {}).get("reddit", cfg.get("summary_default_maxlen", 180)))

    items: List[Item] = []
    for sub in reddit_cfg.get("subreddits", []):
        subreddit = r.subreddit(sub)

        # Use "hot" for good signal; swap to .top("day") if you want later
        for post in subreddit.hot(limit=limit):
            created = datetime.fromtimestamp(post.created_utc, tz=timezone.utc)
            if created < since_utc:
                continue

            ups = int(getattr(post, "ups", 0))
            if ups < min_upvotes:
                continue

            title = clean_text(getattr(post, "title", "") or "", 140)
            link = f"https://www.reddit.com{post.permalink}"

            selftext = getattr(post, "selftext", "") or ""
            summary = clean_text(selftext, sum_len)
            if not summary:
                summary = clean_text(f"Reddit highlight (▲{ups})", sum_len)

            # Optional keyword filter for reddit section too
            if keyword_blocked("reddit", title, summary, cfg):
                continue

            src = f"Reddit: r/{sub} (▲{ups})"
            it = Item(
                section="reddit",
                source=src,
                title=title,
                link=link,
                published_utc=created,
                summary=summary,
                score=0.0,
            )
            it.score = basic_score(created, len(summary)) + min(0.5, ups / 1200.0)

            items.append(it)

    return items


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
    title = cfg.get("title", "Jeffrey’s Daily")
    date_str = format_date_local(now_utc())

    html_out = f"""
    <div style="font-family:-apple-system,Segoe UI,Roboto,Arial; line-height:1.35;">
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
# Main logic
# -------------------------
def main() -> None:
    cfg = load_config("sources.json")
    since = now_utc() - timedelta(hours=int(cfg.get("lookback_hours", 24)))

    all_items: List[Item] = []

    # RSS sections
    rss_cfg = cfg.get("rss", {}) or {}
    for section, urls in rss_cfg.items():
        all_items.extend(fetch_rss_section(section, urls, since, cfg))

    # Reddit (API) section
    all_items.extend(fetch_reddit(cfg, since))

    # Dedupe globally
    all_items = dedupe(all_items)

    # Group by section
    by_section: Dict[str, List[Item]] = {}
    for it in all_items:
        by_section.setdefault(it.section, []).append(it)

    sec_limits = cfg.get("sections", {}) or {}
    max_per_source_cfg = cfg.get("max_per_source", {}) or {}

    # --- Big signals: choose across sections but keep it clean
    big_sources = []
    for k in ["supply_chain", "markets", "tech_ai_apple", "europe_nl"]:
        big_sources.extend(by_section.get(k, []))

    # Apply big_signals keyword filters too
    big_filtered = []
    for it in big_sources:
        if keyword_blocked("big_signals", it.title, it.summary, cfg):
            continue
        big_filtered.append(it)

    # cap per source to avoid 5x Yahoo in big signals
    big_mps = int((max_per_source_cfg.get("big_signals", 2)))
    big_filtered = cap_per_source(big_filtered, big_mps)

    sections: Dict[str, List[Item]] = {}
    sections["big_signals"] = pick(big_filtered, int(sec_limits.get("big_signals", {}).get("max_items", 5)))

    # --- Supply chain + deep dive
    supply_items = by_section.get("supply_chain", [])
    supply_items = cap_per_source(supply_items, int(max_per_source_cfg.get("supply_chain", 3)))

    # Deep dive: only from preferred sources
    deep_cfg = cfg.get("deep_dive", {}) or {}
    allowed_contains = [x.lower() for x in deep_cfg.get("allowed_sources_contains", [])]
    deep_candidates = supply_items[:]
    if allowed_contains:
        deep_candidates = [
            x for x in deep_candidates
            if any(a in x.source.lower() for a in allowed_contains)
        ]
    # also avoid "trial", "free", "promotion" type promo deep dives
    promo_words = deep_cfg.get("exclude_keywords", ["free trial", "promotion", "limited-time", "trial"])
    deep_candidates = [x for x in deep_candidates if not contains_any(f"{x.title} {x.summary}", promo_words)]

    deep_candidates = sorted(deep_candidates, key=lambda x: (x.score, len(x.summary)), reverse=True)
    deep = deep_candidates[: int(sec_limits.get("deep_dive", {}).get("max_items", 1))]
    deep_links = {d.link for d in deep}

    supply_main = [s for s in supply_items if s.link not in deep_links]
    sections["supply_chain"] = pick(supply_main, int(sec_limits.get("supply_chain", {}).get("max_items", 8)))
    sections["deep_dive"] = deep

    # --- Other sections with per-source caps
    for k in ["markets", "tech_ai_apple", "europe_nl", "reddit", "gaming_major", "podcasts_new"]:
        items_k = by_section.get(k, [])
        items_k = cap_per_source(items_k, int(max_per_source_cfg.get(k, 2 if k == "markets" else 3)))
        sections[k] = pick(items_k, int(sec_limits.get(k, {}).get("max_items", 5)))

    subject = f"{cfg.get('title', 'Jeffrey’s Daily')} — {datetime.now().strftime('%Y-%m-%d')}"
    html_body = render_email_html(cfg, sections)
    send_via_resend(subject, html_body)


if __name__ == "__main__":
    main()
