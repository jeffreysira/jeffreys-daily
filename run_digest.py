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


# -------------------------
# Data model
# -------------------------
@dataclass
class Item:
    section_id: str          # your JSON section id (e.g. "work_intelligence")
    section_title: str       # human title
    source_name: str         # source display name
    source_priority: str     # HIGH/MEDIUM/DISCOVERY
    title: str
    link: str
    published_utc: Optional[datetime]
    summary: str
    score: float


# -------------------------
# Helpers
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


def esc(s: str) -> str:
    return html.escape(s or "")


def norm_title(t: str) -> str:
    t = re.sub(r"\s+", " ", (t or "").lower()).strip()
    t = re.sub(r"[^a-z0-9 ]+", "", t)
    return t[:140]


def format_date_local(dt_utc: datetime, tz_name: str) -> str:
    local_tz = tz.gettz(tz_name)
    return dt_utc.astimezone(local_tz).strftime("%A %d %B %Y")


def contains_any(text: str, keywords: List[str]) -> bool:
    t = text.lower()
    return any((k or "").lower() in t for k in keywords if k)


def priority_weight(p: str) -> float:
    p = (p or "").upper().strip()
    if p == "HIGH":
        return 1.00
    if p == "MEDIUM":
        return 0.75
    if p == "DISCOVERY":
        return 0.55
    return 0.70


def basic_score(published_utc: Optional[datetime], summary_len: int) -> float:
    # newer + has real content
    score = 0.0
    if published_utc:
        age_h = (now_utc() - published_utc).total_seconds() / 3600.0
        score += max(0.0, 1.0 - min(age_h / 30.0, 1.0)) * 0.9
    score += min(summary_len, 220) / 220.0 * 0.35
    return score


def section_keyword_blocked(section_cfg: Dict[str, Any], title: str, summary: str) -> bool:
    inc = [k.lower() for k in (section_cfg.get("include_keywords") or [])]
    exc = [k.lower() for k in (section_cfg.get("exclude_keywords") or [])]
    text = f"{title} {summary}".lower()

    if inc and not any(k in text for k in inc):
        return True
    if exc and any(k in text for k in exc):
        return True
    return False


def dedupe(items: List[Item]) -> List[Item]:
    seen_links = set()
    seen_titles = set()
    out: List[Item] = []
    for it in sorted(items, key=lambda x: x.score, reverse=True):
        lk = (it.link or "").strip().lower()
        tk = norm_title(it.title)
        if not lk or lk in seen_links or tk in seen_titles:
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
        counts[it.source_name] = counts.get(it.source_name, 0) + 1
        if counts[it.source_name] <= max_per_source:
            out.append(it)
    return out


def pick(items: List[Item], n: int) -> List[Item]:
    return sorted(items, key=lambda x: x.score, reverse=True)[: max(0, n)]


# -------------------------
# Why-this-matters (simple, reliable heuristic)
# -------------------------
IMPACT_RULES: List[Tuple[List[str], str]] = [
    (["tariff", "sanction", "customs", "export", "import", "trade"], "Trade & compliance risk: potential impact on duties, lead times and documentation."),
    (["shipping", "freight", "ocean", "air freight", "road freight", "capacity", "rates", "port", "container"], "Logistics cost/lead-time signal: could move capacity, rates or reliability."),
    (["energy", "oil", "gas"], "Cost driver signal: energy prices can flow into transport and supplier costs."),
    (["inventory", "warehouse", "fulfillment", "automation", "3pl", "network"], "Operational leverage: relevant for network design, warehousing and execution."),
    (["retail", "sephora", "walmart", "ahold", "ikea", "consumer"], "Demand & retail signal: can affect volumes, promos, and planning assumptions."),
    (["interest rate", "inflation", "central bank", "bond", "yield", "liquidity"], "Macro driver: affects demand, financing costs and overall market sentiment."),
    (["ai", "data center", "capex", "semiconductor"], "Investment cycle signal: AI capex can reshape tech supply chains and demand pockets."),
    (["regulation", "epr", "csrd", "labeling", "compliance", "packaging"], "Regulatory signal: potential changes to product compliance and market access."),
]


def why_this_matters(title: str, summary: str, fallback: str = "High-signal update: worth a quick scan for downstream impact.") -> str:
    text = f"{title} {summary}".lower()
    for keys, msg in IMPACT_RULES:
        if any(k in text for k in keys):
            return msg
    return fallback


# -------------------------
# Fetching
# -------------------------
def fetch_rss_sources_for_section(section_cfg: Dict[str, Any], since_utc: datetime) -> List[Item]:
    out: List[Item] = []
    section_id = section_cfg.get("id", "unknown")
    section_title = section_cfg.get("title", section_id)

    sources = section_cfg.get("sources") or []
    for src in sources:
        if (src.get("type") or "").lower() != "rss":
            continue
        url = (src.get("url") or "").strip()
        if not url or url.startswith("TODO_"):
            continue

        src_name = src.get("name") or "RSS"
        src_priority = (src.get("priority") or "MEDIUM").upper().strip()

        feed = feedparser.parse(url)
        feed_title = getattr(feed.feed, "title", None) or src_name

        for e in feed.entries:
            dt = parse_entry_datetime(e)
            if dt and dt < since_utc:
                continue

            title = clean_text(getattr(e, "title", "") or "", 140)
            link = (getattr(e, "link", "") or "").strip()
            summary_raw = getattr(e, "summary", "") or getattr(e, "description", "") or ""
            summary = clean_text(summary_raw, 220)

            if not title or not link:
                continue

            if section_keyword_blocked(section_cfg, title, summary):
                continue

            it = Item(
                section_id=section_id,
                section_title=section_title,
                source_name=f"RSS: {feed_title}",
                source_priority=src_priority,
                title=title,
                link=link,
                published_utc=dt,
                summary=summary,
                score=0.0
            )

            it.score = basic_score(dt, len(summary)) * priority_weight(src_priority)
            out.append(it)

    return out


# -------------------------
# Rendering
# -------------------------
def section_block(heading: str, items: List[Item], show_why: bool = True) -> str:
    if not items:
        return ""

    lis = []
    for it in items:
        why = why_this_matters(it.title, it.summary)
        why_html = f'<div style="margin-top:6px;color:#333;"><i>Why this matters for you:</i> {esc(why)}</div>' if show_why else ""
        lis.append(
            f"""
            <li style="margin:0 0 14px 0;">
              <div style="font-weight:700;">{esc(it.title)}</div>
              <div style="margin-top:4px;">{esc(it.summary)}</div>
              {why_html}
              <div style="margin-top:6px;font-size:12px;color:#555;">
                <a href="{esc(it.link)}">Open source</a> · {esc(it.source_name)}
              </div>
            </li>
            """
        )

    return f"""
    <h2 style="margin:22px 0 8px 0;">{esc(heading)}</h2>
    <ul style="padding-left:18px;margin:0;">
      {''.join(lis)}
    </ul>
    """


def render_email_html(cfg: Dict[str, Any], out_sections: Dict[str, List[Item]], todays_focus: List[Item]) -> str:
    meta = cfg.get("meta", {}) or {}
    title = meta.get("digest_name", "Jeffrey’s Daily")
    tz_name = meta.get("timezone", "Europe/Amsterdam")
    date_str = format_date_local(now_utc(), tz_name)

    # stable layout (as agreed)
    html_out = f"""
    <div style="font-family:-apple-system,Segoe UI,Roboto,Arial; line-height:1.35; max-width:760px;">
      <h1 style="margin:0 0 6px 0;">{esc(title)} — 19:00</h1>
      <div style="color:#666;margin:0 0 18px 0;">{esc(date_str)} · Leestijd: ±10–20 min</div>

      {section_block("🎯 Today’s Focus", todays_focus, show_why=True)}
      {section_block("🔝 Critical Signals", out_sections.get("critical_signals", []), show_why=True)}
      {section_block("📦 Work Intelligence", out_sections.get("work_intelligence", []), show_why=True)}
      {section_block("📈 Macro & Markets", out_sections.get("markets", []), show_why=True)}
      {section_block("🧰 Tech / Tools / Productivity", out_sections.get("tech_tools", []), show_why=True)}
      {section_block("🌐 Blogs & Essays", out_sections.get("blogs", []), show_why=False)}
      {section_block("📚 Books & Reading", out_sections.get("books", []), show_why=False)}
      {section_block("🎮 Gaming (major only)", out_sections.get("gaming", []), show_why=False)}
      {section_block("🎧 Podcasts (new episodes)", out_sections.get("podcasts", []), show_why=False)}

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

    meta = cfg.get("meta", {}) or {}
    sel = cfg.get("selection", {}) or {}

    lookback_hours = int(cfg.get("lookback_hours", 24))
    since = now_utc() - timedelta(hours=lookback_hours)

    sections_cfg: List[Dict[str, Any]] = cfg.get("sections") or []
    if not isinstance(sections_cfg, list) or not sections_cfg:
        raise RuntimeError("sources.json: expected 'sections' to be a non-empty list")

    # Fetch items
    all_items: List[Item] = []
    for s in sections_cfg:
        all_items.extend(fetch_rss_sources_for_section(s, since))

    # Global dedupe
    all_items = dedupe(all_items)

    # Group by JSON section id
    by_section: Dict[str, List[Item]] = {}
    for it in all_items:
        by_section.setdefault(it.section_id, []).append(it)

    # Build output buckets (the ones we render)
    out_sections: Dict[str, List[Item]] = {
        "critical_signals": [],
        "work_intelligence": [],
        "markets": [],
        "tech_tools": [],
        "blogs": [],
        "books": [],
        "gaming": [],
        "podcasts": [],
    }

    # Pull max_items from config per section
    max_items_map: Dict[str, int] = {}
    for s in sections_cfg:
        sid = s.get("id")
        if not sid:
            continue
        max_items_map[sid] = int(s.get("max_items", 5))

    # Per-source caps (keep the digest clean)
    # These defaults are intentionally strict to stop single-source spam.
    per_source_caps = {
        "critical_signals": 2,
        "work_intelligence": 2,
        "markets": 2,
        "life_tools": 2,
        "tech_tools": 2,
        "blogs": 2,
        "books": 2,
        "gaming": 2,
        "podcasts": 2,
    }

    # Fill each known section if present
    def fill(section_id: str, out_key: str) -> None:
        items = by_section.get(section_id, [])
        items = cap_per_source(items, int(per_source_caps.get(section_id, 2)))
        out_sections[out_key] = pick(items, int(max_items_map.get(section_id, 5)))

    fill("critical_signals", "critical_signals")
    fill("work_intelligence", "work_intelligence")
    fill("markets", "markets")
    fill("macro_markets", "markets")     # if you named it macro_markets
    fill("life_tools", "tech_tools")     # if you named it life_tools
    fill("tech_tools", "tech_tools")
    fill("blogs", "blogs")
    fill("blogs_essays", "blogs")
    fill("books", "books")
    fill("gaming", "gaming")
    fill("podcasts", "podcasts")

    # Today’s Focus (top 3) from agreed sections
    tf = sel.get("todays_focus", {}) or {}
    tf_enabled = bool(tf.get("enabled", True))
    tf_max = int(tf.get("max_items", 3))
    tf_sections = tf.get("sources_from_sections", ["critical_signals", "work_intelligence"])
    tf_boost = [k.lower() for k in (tf.get("boost_keywords") or [])]

    todays_focus: List[Item] = []
    if tf_enabled:
        candidates: List[Item] = []
        for sid in tf_sections:
            candidates.extend(by_section.get(sid, []))

        # boost by keywords
        boosted: List[Item] = []
        for it in candidates:
            text = f"{it.title} {it.summary}".lower()
            boost_hits = sum(1 for k in tf_boost if k and k in text)
            it2 = Item(**{**it.__dict__})  # shallow copy
            it2.score = it2.score + min(0.9, boost_hits * 0.12)
            boosted.append(it2)

        boosted = dedupe(boosted)
        boosted = cap_per_source(boosted, 2)
        todays_focus = pick(boosted, tf_max)

        # Remove focus items from their original sections (avoid duplicates)
        focus_links = {x.link for x in todays_focus}
        for key in ["critical_signals", "work_intelligence", "markets", "tech_tools", "blogs", "books", "gaming", "podcasts"]:
            out_sections[key] = [x for x in out_sections[key] if x.link not in focus_links]

    subject = f"{meta.get('digest_name', 'Jeffrey’s Daily')} — {datetime.now().strftime('%Y-%m-%d')}"
    html_body = render_email_html(cfg, out_sections, todays_focus)
    send_via_resend(subject, html_body)


if __name__ == "__main__":
    main()
