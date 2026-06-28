"""Query-parsing layer: Claude (Haiku) converts a natural-language security
question into structured retrieval filters. Relative times resolved in Python."""
import json, re
from datetime import datetime, timedelta, timezone

from providers import completion_provider

PARSE_SYSTEM = """You convert security-event questions into a JSON filter object.
Output ONLY raw JSON, no markdown, no preamble. Schema:
{
  "semantic_query": str,
  "detection_type": str|null,
  "camera_id": str|null,
  "zone": str|null,
  "relative_time": str|null,
  "start": str|null,
  "end": str|null
}
detection_type is one of: loitering, intrusion, package_theft, tailgating,
vehicle, crowd, abandoned_object, or null.
relative_time is one of: today, yesterday, last_24h, last_Nd (N = number of
days, e.g. last_15d for "last 15 days"), last_Nw (N = number of weeks, e.g.
last_2w for "past 2 weeks"), last_weekend, this_week, last_week, this_month,
last_month, or null.
Use absolute ISO8601 in start/end only if the user gives explicit dates.
If a field is not mentioned, use null. Never invent values."""


def _first_of_month(d: datetime) -> datetime:
    return d.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _resolve_relative_time(rel: str, now: datetime):
    if rel == "today":
        s = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return s, now
    if rel == "yesterday":
        s = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        return s, s + timedelta(days=1)
    if rel == "last_24h":
        return now - timedelta(hours=24), now
    if rel == "this_week":
        s = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        return s, now
    if rel == "last_week":
        this_mon = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        return this_mon - timedelta(days=7), this_mon
    if rel == "last_weekend":
        days_since_sat = (now.weekday() - 5) % 7
        if days_since_sat < 2:  # today is Sat/Sun itself -> that's this weekend, not last
            days_since_sat += 7
        sat = (now - timedelta(days=days_since_sat)).replace(hour=0, minute=0, second=0, microsecond=0)
        return sat, sat + timedelta(days=2)
    if rel == "this_month":
        return _first_of_month(now), now
    if rel == "last_month":
        first_this = _first_of_month(now)
        first_prev = _first_of_month(first_this - timedelta(days=1))
        return first_prev, first_this
    m = re.fullmatch(r"last_(\d+)d", rel)
    if m:
        return now - timedelta(days=int(m.group(1))), now
    m = re.fullmatch(r"last_(\d+)w", rel)
    if m:
        return now - timedelta(weeks=int(m.group(1))), now
    return None, None


def parse_query(question: str) -> dict:
    cp = completion_provider()
    raw = cp.complete(system=PARSE_SYSTEM, prompt=question,
                      max_tokens=512, model=cp.parse_model)
    raw = re.sub(r"^```(?:json)?|```$", "", raw.strip()).strip()
    try:
        f = json.loads(raw)
    except json.JSONDecodeError:
        f = {"semantic_query": question}

    now = datetime.now(timezone.utc)
    if f.get("relative_time") and not f.get("start"):
        s, e = _resolve_relative_time(f["relative_time"], now)
        if s:
            f["start"], f["end"] = s.isoformat(), e.isoformat()
    return f
