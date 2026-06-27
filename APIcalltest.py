"""
LLM Analytics Report — sw-cdev-gsk
=====================================
Replicates:
  SELECT DD_TENANT_ID, DD_ENVIRONMENT, DD_PROJECT_ID, DD_SERVICE_NAME,
         count(1), sum(CT_TOTAL_TOKENS)
  FROM   FACT_LLM_EVENTS
  WHERE  DD_ENVIRONMENT = 'sw-cdev-gsk'
  GROUP  BY DD_TENANT_ID, DD_ENVIRONMENT, DD_PROJECT_ID, DD_SERVICE_NAME
  ORDER  BY 1,2,3,4;

Requires:  pip install requests

Usage:
  python llm_report_gsk.py               # show summary table + event ID count
  python llm_report_gsk.py --export      # save to CSV
  python llm_report_gsk.py --debug       # show raw API record + field mapping
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
TOKEN   = "d0fada6a9c3dfec40ae2efc52a320f2c"
TENANTS = ["cb565o720"]

PROJECT_IDS = [
    "1D846964_4ED2_4CD3_A5E7_E3735E55E30F",
]

START_DATE = "2026-06-11"
END_DATE   = "2026-06-25"
CHUNK_DAYS = 30
PAGE_SIZE  = 5000

# All known aliases for the event/record ID field
EVENT_ID_ALIASES = [
    "event_id", "eventId", "id", "record_id", "recordId",
    "llm_event_id", "llmEventId", "dd_event_id", "dd_event_timestamp",
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


def make_session() -> requests.Session:
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


# ══════════════════════════════════════════════════════════════════════════════
#  API — single POST, returns (records, next_cursor)
# ══════════════════════════════════════════════════════════════════════════════
def _post(session, project_id, from_date, to_date, cursor="", offset=0):
    resp = session.post(
        URL,
        json={
            "tenants":    TENANTS,
            "project_id": project_id,
            "from_date":  from_date,
            "to_date":    to_date,
            "limit":      PAGE_SIZE,
            "offset":     offset,
            "page":       offset // PAGE_SIZE,
            "page_size":  PAGE_SIZE,
        },
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type":  "application/json",
            "X-Limit":       str(PAGE_SIZE),
            "X-Offset":      str(offset),
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
        records = body
        next_cursor = (
            resp.headers.get("X-Next-Cursor") or
            resp.headers.get("X-Cursor-Next") or
            None
        )
    else:
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


# ══════════════════════════════════════════════════════════════════════════════
#  FETCH — one project × one date window, all pages
# ══════════════════════════════════════════════════════════════════════════════
def fetch_window(session, project_id, from_date, to_date):
    all_recs, cursor, offset = [], "", 0
    seen_ids = set()
    while True:
        recs, next_cursor = _post(session, project_id, from_date, to_date, cursor, offset)

        # de-duplicate by event_id in case offset + cursor overlap
        new_recs = []
        for r in recs:
            rid = r.get("event_id") or r.get("id") or r.get("request_id")
            if rid not in seen_ids:
                seen_ids.add(rid)
                new_recs.append(r)
        all_recs.extend(new_recs)

        if len(recs) == 0:
            break
        if next_cursor and next_cursor != cursor:
            cursor = next_cursor
            offset += len(recs)
        elif len(recs) >= PAGE_SIZE:
            # no cursor returned but got a full page — try offset pagination
            offset += PAGE_SIZE
            cursor = ""
        else:
            break
    return all_recs


# ══════════════════════════════════════════════════════════════════════════════
#  FETCH ALL — every project × every 30-day window
# ══════════════════════════════════════════════════════════════════════════════
def fetch_all():
    session = make_session()
    windows = list(date_windows())

    print(f"  Environment : sw-uatirl001")
    print(f"  Tenant      : {TENANTS}")
    print(f"  Projects    : {len(PROJECT_IDS)}")
    print(f"  Date range  : {START_DATE}  →  {END_DATE}")
    print(f"  Chunks/proj : {len(windows)} × {CHUNK_DAYS}-day windows\n")

    all_records = []
    for p_idx, project_id in enumerate(PROJECT_IDS, 1):
        proj_total = 0
        print(f"  ── Project {p_idx}/{len(PROJECT_IDS)}: {project_id}")
        for i, (w_from, w_to) in enumerate(windows, 1):
            print(f"     [{i:>3}/{len(windows)}] {w_from} → {w_to} ...", end=" ", flush=True)
            chunk = fetch_window(session, project_id, w_from, w_to)
            print(f"{len(chunk):,}")
            all_records.extend(chunk)
            proj_total += len(chunk)
        print(f"     subtotal: {proj_total:,} records\n")

    return all_records


# ══════════════════════════════════════════════════════════════════════════════
#  FIELD RESOLVER
# ══════════════════════════════════════════════════════════════════════════════
FIELD_MAP = {
    "TENANT_ID":    ["dd_tenant_id",    "tenant_id",    "tenantId",    "tenant"],
    "ENVIRONMENT":  ["dd_environment",  "environment",  "env",         "dd_env"],
    "PROJECT_ID":   ["dd_project_id",   "project_id",   "projectId",   "project"],
    "SERVICE_NAME": ["dd_service_name", "service_name", "serviceName", "service",
                     "dd_service",      "service_type"],
    "TOKENS":       ["ct_total_tokens", "total_tokens", "totalTokens", "token_count",
                     "tokens",          "tokenCount",   "total_token", "ct_tokens"],
}
_key_cache = {}

def get_field(record: dict, col: str):
    if col in _key_cache:
        return record.get(_key_cache[col])
    low = {k.lower(): k for k in record}
    for alias in FIELD_MAP[col]:
        if alias in record:
            _key_cache[col] = alias; return record[alias]
        if alias.lower() in low:
            real = low[alias.lower()]; _key_cache[col] = real; return record[real]
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  EVENT ID COUNTER
#  Finds the event ID field automatically and counts total + unique IDs
# ══════════════════════════════════════════════════════════════════════════════
def count_event_ids(records: list) -> dict:
    if not records:
        return {"id_field": None, "total_records": 0,
                "unique_ids": 0, "duplicate_ids": 0, "all_ids": set()}

    # detect which key holds the event ID
    id_field = None
    low_keys  = {k.lower(): k for k in records[0]}
    for alias in EVENT_ID_ALIASES:
        if alias in records[0]:
            id_field = alias; break
        if alias.lower() in low_keys:
            id_field = low_keys[alias.lower()]; break

    if not id_field:
        return {"id_field": None, "total_records": len(records),
                "unique_ids": 0, "duplicate_ids": 0, "all_ids": set()}

    all_ids    = [rec.get(id_field) for rec in records if rec.get(id_field) is not None]
    unique_ids = set(all_ids)

    return {
        "id_field":      id_field,
        "total_records": len(records),
        "unique_ids":    len(unique_ids),
        "duplicate_ids": len(all_ids) - len(unique_ids),
        "all_ids":       unique_ids,
    }


def print_event_id_summary(records: list):
    result = count_event_ids(records)
    B1, B2 = "═" * 60, "─" * 60

    print(f"\n{B1}")
    print(f"  EVENT ID SUMMARY")
    print(f"{B1}")

    if not result["id_field"]:
        print(f"  [WARN] No event ID field detected.")
        print(f"  Run --debug to see all available API field names.")
        print(f"  Total raw records : {result['total_records']:,}")
    else:
        print(f"  Event ID field    : '{result['id_field']}'")
        print(f"  Total raw records : {result['total_records']:,}")
        print(f"  Total unique IDs  : {result['unique_ids']:,}")
        if result["duplicate_ids"] > 0:
            print(f"  Duplicate IDs     : {result['duplicate_ids']:,}  "
                  f"(same event in multiple date chunks)")
        else:
            print(f"  Duplicates        : 0  ✓")

    print(f"{B1}\n")
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  AGGREGATE — GROUP BY + COUNT(1) + SUM(tokens)
# ══════════════════════════════════════════════════════════════════════════════
def aggregate(records: list) -> list:
    groups = {}
    for rec in records:
        tenant  = get_field(rec, "TENANT_ID")    or "—"
        env     = get_field(rec, "ENVIRONMENT")  or "—"
        project = get_field(rec, "PROJECT_ID")   or "—"
        service = get_field(rec, "SERVICE_NAME") or "—"
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
    B1 = "═" * W
    B2 = "─" * W
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
    print("\n── First raw API record (all fields) ────────────────────────────────")
    if records:
        print(json.dumps(records[0], indent=2, default=str))
    print("\n── Column → matched API key ─────────────────────────────────────────")
    for col, key in _key_cache.items():
        sample = records[0].get(key, "—") if records else "—"
        print(f"  {col:<14} → {key!r:<30}  value={str(sample)[:50]!r}")
    missing = [c for c in FIELD_MAP if c not in _key_cache]
    if missing:
        print(f"\n  [WARN] No match for: {missing}")
        print("  Add the correct key name to FIELD_MAP in the script.")
    print("─────────────────────────────────────────────────────────────────────\n")


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
    print(f"  [CSV saved] → {fn}\n")


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
    print(f"  LLM Analytics  —  sw-stg-kraftheinz")
    print(f"{'─'*60}\n")

    # ── Step 1: fetch all raw records ────────────────────────────────────────
    raw = fetch_all()
    print(f"  ══ Grand total raw records: {len(raw):,} ══\n")

    if not raw:
        print("  [WARN] No records returned.\n"); sys.exit(0)

    # ── Step 2: count total unique event IDs ─────────────────────────────────
    event_result = print_event_id_summary(raw)

    # ── Step 3: aggregate + display table ────────────────────────────────────
    rows = aggregate(raw)

    if args.debug:
        print_debug(raw)

    print_table(rows)

    # ── Step 4: export ───────────────────────────────────────────────────────
    if args.export:
        export_csv(rows)


if __name__ == "__main__":
    main()