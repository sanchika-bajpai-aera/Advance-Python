"""
LLM Analytics Report — sw-cdev-gsk
=====================================
Shows:
  1. MIN / MAX timestamp across all records
  2. Total unique event IDs
  3. Grouped table: TENANT_ID | ENVIRONMENT | PROJECT_ID | SERVICE_NAME | TOTAL_COUNT | TOTAL_TOKENS

Requires:  pip install requests

Usage:
  python llm_report_gsk.py
  python llm_report_gsk.py --export
  python llm_report_gsk.py --debug
"""

import argparse
import csv
import json
import sys
from datetime import date, timedelta, datetime

try:
    import requests
except ImportError:
    print("[ERROR] Run:  pip install requests")
    sys.exit(1)

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
URL     = "https://sw-uatirl001.aeratechnology.com/aera/cortex/agent-memory/api/v1/llm-analytics/snapshot-raw"
TOKEN   = "168671ea91cd3fdab05fdee1cc625316"
TENANTS = ["cb565o720"]

PROJECT_IDS = [
    "6EA48753_57DE_4B4E_A749_03C03908C860",
]

START_DATE = "2026-06-11"
END_DATE   = date.today().isoformat()
CHUNK_DAYS = 30
PAGE_SIZE  = 5000

TIMESTAMP_ALIASES = [
    "timestamp",           # confirmed field name from API
    "dd_event_timestamp", "event_timestamp", "eventTimestamp",
    "created_at", "createdAt", "created",
    "updated_at", "updatedAt", "event_time", "eventTime",
    "request_time", "requestTime", "time", "date",
]

EVENT_ID_ALIASES = [
    "event_id",            # confirmed field name from API
    "eventId", "id", "record_id", "recordId",
    "llm_event_id", "llmEventId", "dd_event_id",
    "uuid", "request_id", "requestId", "trace_id", "traceId",
]


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def date_windows():
    cur = date.fromisoformat(START_DATE)
    end = date.fromisoformat(END_DATE)
    while cur <= end:
        yield cur.isoformat(), min(cur + timedelta(days=CHUNK_DAYS - 1), end).isoformat()
        cur += timedelta(days=CHUNK_DAYS)


def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent":      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/125.0.0.0 Safari/537.36",
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection":      "keep-alive",
    })
    return s


def detect_field(record: dict, aliases: list):
    """Return the first alias key found in record (case-insensitive)."""
    low = {k.lower(): k for k in record}
    for alias in aliases:
        if alias in record:
            return alias
        if alias.lower() in low:
            return low[alias.lower()]
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  API
# ══════════════════════════════════════════════════════════════════════════════
def _post(session, project_id, from_date, to_date, cursor=""):
    resp = session.post(
        URL,
        json={
            "tenants":    TENANTS,
            "project_id": project_id,
            "from_date":  from_date,
            "to_date":    to_date,
        },
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type":  "application/json",
            "X-Limit":       str(PAGE_SIZE),
            "X-Cursor":      cursor,
        },
        timeout=60,
    )
    if resp.status_code == 403:
        print("\n[ERROR] HTTP 403 – run from the same network/VPN as Postman.", file=sys.stderr)
        sys.exit(1)
    if not resp.ok:
        print(f"\n[ERROR] HTTP {resp.status_code}: {resp.text[:400]}", file=sys.stderr)
        sys.exit(1)

    body = resp.json()
    if isinstance(body, list):
        return body, None

    records = (
        body.get("data")    or body.get("records") or
        body.get("events")  or body.get("items")   or
        body.get("results") or []
    )
    next_cursor = (
        body.get("next_cursor") or body.get("nextCursor") or
        body.get("cursor")      or body.get("next")       or
        resp.headers.get("X-Next-Cursor") or
        resp.headers.get("X-Cursor-Next") or
        None
    )
    return records, next_cursor


def fetch_window(session, project_id, from_date, to_date):
    all_recs, cursor = [], ""
    while True:
        recs, next_cursor = _post(session, project_id, from_date, to_date, cursor)
        all_recs.extend(recs)
        if not next_cursor or len(recs) == 0 or len(recs) < PAGE_SIZE:
            break
        cursor = next_cursor
    return all_recs


def fetch_all():
    session = make_session()
    windows = list(date_windows())

    print(f"  Environment : sw-cdev-gsk")
    print(f"  Tenant      : {TENANTS}")
    print(f"  Projects    : {len(PROJECT_IDS)}")
    print(f"  Date range  : {START_DATE}  ->  {END_DATE}")
    print(f"  Chunks/proj : {len(windows)} x {CHUNK_DAYS}-day windows\n")

    all_records = []
    for p_idx, project_id in enumerate(PROJECT_IDS, 1):
        proj_total = 0
        print(f"  -- Project {p_idx}/{len(PROJECT_IDS)}: {project_id}")
        for i, (w_from, w_to) in enumerate(windows, 1):
            print(f"     [{i:>3}/{len(windows)}] {w_from} -> {w_to} ...", end=" ", flush=True)
            chunk = fetch_window(session, project_id, w_from, w_to)
            print(f"{len(chunk):,}")
            all_records.extend(chunk)
            proj_total += len(chunk)
        print(f"     subtotal: {proj_total:,} records\n")

    return all_records


# ══════════════════════════════════════════════════════════════════════════════
#  TIMESTAMP ANALYSIS — MIN and MAX
# ══════════════════════════════════════════════════════════════════════════════
def print_timestamp_summary(records: list):
    B1 = "=" * 60
    print(f"\n{B1}")
    print(f"  TIMESTAMP SUMMARY")
    print(f"{B1}")

    if not records:
        print("  No records to analyse.")
        print(B1)
        return

    ts_field = detect_field(records[0], TIMESTAMP_ALIASES)

    if not ts_field:
        print("  [WARN] No timestamp field detected.")
        print("  Run --debug to see all available API field names.")
        print(B1)
        return

    values = [str(rec[ts_field]) for rec in records if rec.get(ts_field) is not None]

    if not values:
        print(f"  Field '{ts_field}' found but all values are null.")
        print(B1)
        return

    # parse ISO-8601 timestamps for correct ordering
    def parse_ts(s):
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ",
                    "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                continue
        return s   # fallback: raw string

    parsed = [(parse_ts(v), v) for v in values]
    parsed.sort(key=lambda x: (str(x[0]),))
    min_ts = parsed[0][1]
    max_ts = parsed[-1][1]

    print(f"  Field detected  : '{ts_field}'")
    print(f"  Records with ts : {len(values):,}")
    print(f"  MIN timestamp   : {min_ts}")
    print(f"  MAX timestamp   : {max_ts}")
    print(f"{B1}\n")


# ══════════════════════════════════════════════════════════════════════════════
#  EVENT ID SUMMARY — total unique IDs
# ══════════════════════════════════════════════════════════════════════════════
def print_event_id_summary(records: list):
    B1 = "=" * 60
    print(f"\n{B1}")
    print(f"  EVENT ID SUMMARY")
    print(f"{B1}")

    if not records:
        print("  No records.")
        print(B1)
        return

    id_field = detect_field(records[0], EVENT_ID_ALIASES)

    if not id_field:
        print("  [WARN] No event ID field detected.")
        print("  Run --debug to see all available API field names.")
        print(f"  Total raw records : {len(records):,}")
        print(B1)
        return

    all_ids    = [rec[id_field] for rec in records if rec.get(id_field) is not None]
    unique_ids = len(set(all_ids))
    dupes      = len(all_ids) - unique_ids

    print(f"  Field detected    : '{id_field}'")
    print(f"  Total raw records : {len(records):,}")
    print(f"  Total unique IDs  : {unique_ids:,}")
    if dupes > 0:
        print(f"  Duplicate IDs     : {dupes:,}  (same event across date chunks)")
    else:
        print(f"  Duplicates        : 0  (all IDs are unique)")
    print(f"{B1}\n")


# ══════════════════════════════════════════════════════════════════════════════
#  FIELD RESOLVER (for aggregate)
# ══════════════════════════════════════════════════════════════════════════════
FIELD_MAP = {
    "TENANT_ID":    ["tenant_id",       "dd_tenant_id",    "tenantId",    "tenant"],
    "ENVIRONMENT":  ["environment",     "dd_environment",  "env",         "dd_env"],
    "PROJECT_ID":   ["project_id",      "dd_project_id",   "projectId",   "project"],
    "SERVICE_NAME": ["service_name",    "dd_service_name", "serviceName", "service",
                     "dd_service",      "service_type"],
    "TOKENS":       ["total_tokens",    "ct_total_tokens", "totalTokens", "token_count",
                     "tokens",          "tokenCount",      "total_token", "ct_tokens"],
}
_key_cache = {}

def get_field(record: dict, col: str):
    if col in _key_cache:
        return record.get(_key_cache[col])
    low = {k.lower(): k for k in record}
    for alias in FIELD_MAP[col]:
        if alias in record:
            _key_cache[col] = alias
            return record[alias]
        if alias.lower() in low:
            real = low[alias.lower()]
            _key_cache[col] = real
            return record[real]
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  AGGREGATE — GROUP BY + COUNT(1) + SUM(tokens)
# ══════════════════════════════════════════════════════════════════════════════
def aggregate(records: list) -> list:
    groups = {}
    for rec in records:
        tenant  = get_field(rec, "TENANT_ID")    or "---"
        env     = get_field(rec, "ENVIRONMENT")  or "sw-cdev-gsk"
        project = get_field(rec, "PROJECT_ID")   or "---"
        service = get_field(rec, "SERVICE_NAME") or "---"
        tokens  = get_field(rec, "TOKENS")       or 0

        key = (tenant, env, project, service)
        if key not in groups:
            groups[key] = {
                "TENANT_ID":    tenant,
                "ENVIRONMENT":  env,
                "PROJECT_ID":   project,
                "SERVICE_NAME": service,
                "TOTAL_COUNT":  0,
                "TOTAL_TOKENS": 0,
            }
        groups[key]["TOTAL_COUNT"]  += 1
        groups[key]["TOTAL_TOKENS"] += int(tokens) if tokens else 0

    return sorted(groups.values(), key=lambda r: (
        r["TENANT_ID"], r["ENVIRONMENT"], r["PROJECT_ID"], r["SERVICE_NAME"]
    ))


# ══════════════════════════════════════════════════════════════════════════════
#  DISPLAY TABLE
# ══════════════════════════════════════════════════════════════════════════════
def print_table(rows: list):
    C  = {"t": 12, "e": 20, "p": 38, "s": 22, "c": 13, "k": 15}
    W  = sum(C.values()) + 14
    B1 = "=" * W
    B2 = "-" * W
    tr = lambda s, w: (s[:w-3] + "...") if len(s) > w else s

    print(f"\n{B1}")
    print(f"  {'TENANT_ID':<{C['t']}}  {'ENVIRONMENT':<{C['e']}}  "
          f"{'PROJECT_ID':<{C['p']}}  {'SERVICE_NAME':<{C['s']}}  "
          f"{'TOTAL_COUNT':>{C['c']}}  {'TOTAL_TOKENS':>{C['k']}}")
    print(f"  {B2}")

    tc = tt = 0
    for r in rows:
        tc += r["TOTAL_COUNT"]
        tt += r["TOTAL_TOKENS"]
        print(f"  {tr(r['TENANT_ID'],   C['t']):<{C['t']}}  "
              f"{tr(r['ENVIRONMENT'], C['e']):<{C['e']}}  "
              f"{tr(r['PROJECT_ID'],  C['p']):<{C['p']}}  "
              f"{tr(r['SERVICE_NAME'],C['s']):<{C['s']}}  "
              f"{r['TOTAL_COUNT']:>{C['c']},}  "
              f"{r['TOTAL_TOKENS']:>{C['k']},}")

    print(f"  {B2}")
    print(f"  {'TOTAL':<{C['t']}}  {'':<{C['e']}}  {'':<{C['p']}}  "
          f"{str(len(rows))+' service(s)':<{C['s']}}  "
          f"{tc:>{C['c']},}  {tt:>{C['k']},}")
    print(f"{B1}\n")


# ══════════════════════════════════════════════════════════════════════════════
#  DEBUG
# ══════════════════════════════════════════════════════════════════════════════
def print_debug(records: list):
    print("\n-- First raw API record (all fields) ----------------------------")
    if records:
        print(json.dumps(records[0], indent=2, default=str))
    print("\n-- Column -> matched API key ------------------------------------")
    for col, key in _key_cache.items():
        sample = records[0].get(key, "---") if records else "---"
        print(f"  {col:<14} -> {key!r:<30}  value={str(sample)[:50]!r}")
    missing = [c for c in FIELD_MAP if c not in _key_cache]
    if missing:
        print(f"\n  [WARN] No match for: {missing}")
        print("  Add the correct key name to FIELD_MAP in the script.")
    print("-----------------------------------------------------------------\n")


# ══════════════════════════════════════════════════════════════════════════════
#  EXPORT CSV
# ══════════════════════════════════════════════════════════════════════════════
def export_csv(rows: list):
    fn = f"llm_gsk_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(fn, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["TENANT_ID", "ENVIRONMENT", "PROJECT_ID",
                                           "SERVICE_NAME", "TOTAL_COUNT", "TOTAL_TOKENS"])
        w.writeheader()
        w.writerows(rows)
    print(f"  [CSV saved] -> {fn}\n")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--export", action="store_true", help="Save result to CSV")
    p.add_argument("--debug",  action="store_true", help="Show raw API fields + mapping")
    args = p.parse_args()

    print(f"\n{'─'*60}")
    print(f"  LLM Analytics  --  sw-cdev-gsk")
    print(f"{'─'*60}\n")

    # Step 1 — fetch all records
    raw = fetch_all()
    print(f"  Grand total raw records: {len(raw):,}\n")

    if not raw:
        print("  [WARN] No records returned.\n")
        sys.exit(0)

    # Step 2 — min / max timestamp
    print_timestamp_summary(raw)

    # Step 3 — unique event ID count
    print_event_id_summary(raw)

    # Step 4 — grouped table
    rows = aggregate(raw)

    if args.debug:
        print_debug(raw)

    print_table(rows)

    # Step 5 — export
    if args.export:
        export_csv(rows)


if __name__ == "__main__":
    main()