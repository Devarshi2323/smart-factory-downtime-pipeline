import os
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
from psycopg.rows import dict_row

load_dotenv()

STATIC = Path(__file__).with_name("static")
STATIONS = ["OP10", "OP20", "OP30", "OP40", "OP50"]
SHIFTS = ["Day", "Afternoon", "Night"]

app = FastAPI(title="Line 3 Live Dashboard")


def connection_string() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    return (
        f"host={os.getenv('DB_HOST', 'localhost')} "
        f"port={os.getenv('DB_PORT', '5432')} "
        f"dbname={os.getenv('DB_NAME', 'factory')} "
        f"user={os.getenv('DB_USER', 'factory')} "
        f"password={os.getenv('DB_PASSWORD', '')}"
    )


def clean(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, dict):
        return {k: clean(v) for k, v in value.items()}
    if isinstance(value, list):
        return [clean(v) for v in value]
    return value


DOWNTIME_FILTER = """
    start_ts >= now() - make_interval(days => %(days)s)
    AND (%(station)s::text IS NULL OR station_id = %(station)s::text)
    AND (%(shift)s::text IS NULL OR shift = %(shift)s::text)
"""

OEE_FILTER = """
    production_date > ((now() AT TIME ZONE 'America/Toronto') - INTERVAL '7 hours')::date - %(days)s
    AND (%(station)s::text IS NULL OR station_id = %(station)s::text)
"""

QUERIES = {
    "oee": f"""
        SELECT SUM(run_s) AS run_s, SUM(planned_s) AS planned_s, SUM(ideal_output_s) AS ideal_output_s,
               SUM(good_parts) AS good_parts, SUM(total_parts) AS total_parts
        FROM v_oee_daily WHERE {OEE_FILTER}
    """,
    "engines": """
        SELECT COALESCE(SUM(good_parts), 0) AS engines FROM v_oee_daily
        WHERE station_id = 'OP50'
          AND production_date > ((now() AT TIME ZONE 'America/Toronto') - INTERVAL '7 hours')::date - %(days)s
    """,
    "downtime": f"""
        SELECT COALESCE(SUM(duration_min), 0) AS minutes,
               COUNT(*) FILTER (WHERE category <> 'Line flow') AS stops
        FROM v_downtime WHERE NOT is_planned AND {DOWNTIME_FILTER}
    """,
    "by_station": f"""
        SELECT station_id, station_name, ROUND(SUM(duration_min)) AS minutes
        FROM v_downtime WHERE NOT is_planned AND {DOWNTIME_FILTER}
        GROUP BY station_id, station_name ORDER BY minutes DESC
    """,
    "pareto": f"""
        SELECT reason, category, ROUND(SUM(duration_min), 1) AS minutes, COUNT(*) AS stops
        FROM v_downtime WHERE NOT is_planned AND category <> 'Line flow' AND {DOWNTIME_FILTER}
        GROUP BY reason, category ORDER BY minutes DESC
    """,
    "shift": f"""
        SELECT shift, ROUND(SUM(duration_min)) AS minutes
        FROM v_downtime WHERE NOT is_planned AND {DOWNTIME_FILTER}
        GROUP BY shift
    """,
    "trend": f"""
        SELECT production_date,
               SUM(run_s) / NULLIF(SUM(planned_s), 0)
                 * SUM(ideal_output_s) / NULLIF(SUM(run_s), 0)
                 * SUM(good_parts)::numeric / NULLIF(SUM(total_parts), 0) AS oee
        FROM v_oee_daily WHERE {OEE_FILTER}
        GROUP BY production_date ORDER BY production_date
    """,
    "status": """
        SELECT station_id, station_name, state, fault_code, fault_description, state_since, minutes_in_state
        FROM v_station_status ORDER BY sequence_no
    """,
    "events": """
        SELECT e.event_ts, e.station_id, e.event_type, e.payload, fc.description AS fault_description
        FROM machine_events e
        LEFT JOIN fault_codes fc ON fc.fault_code = e.payload->>'fault_code'
        ORDER BY e.event_id DESC LIMIT 12
    """,
    "last_event": "SELECT MAX(event_ts) AS ts FROM machine_events",
}


def ratio(a, b):
    return float(a) / float(b) if a is not None and b else None


@app.get("/api/dashboard")
def dashboard(
    days: int = Query(7, ge=1, le=90),
    station: str | None = Query(None),
    shift: str | None = Query(None),
):
    params = {
        "days": days,
        "station": station if station in STATIONS else None,
        "shift": shift if shift in SHIFTS else None,
    }
    with psycopg.connect(connection_string(), row_factory=dict_row) as conn:
        data = {name: conn.execute(sql, params).fetchall() for name, sql in QUERIES.items()}

    o = data["oee"][0]
    availability = ratio(o["run_s"], o["planned_s"])
    performance = ratio(o["ideal_output_s"], o["run_s"])
    quality = ratio(o["good_parts"], o["total_parts"])
    oee = availability * performance * quality if None not in (availability, performance, quality) else None

    total_pareto = sum(float(r["minutes"]) for r in data["pareto"]) or 1
    running = 0.0
    pareto = []
    for row in data["pareto"]:
        share = float(row["minutes"]) / total_pareto
        running += share
        pareto.append({**row, "share": share, "cumulative": min(running, 1.0)})

    shift_lookup = {r["shift"]: r["minutes"] for r in data["shift"]}
    last_ts = data["last_event"][0]["ts"]
    live = last_ts is not None and (datetime.now(timezone.utc) - last_ts).total_seconds() < 180

    return JSONResponse(clean({
        "filters": params,
        "generated_at": datetime.now(timezone.utc),
        "last_event_at": last_ts,
        "live": live,
        "kpi": {
            "oee": oee,
            "availability": availability,
            "performance": performance,
            "quality": quality,
            "downtime_min": data["downtime"][0]["minutes"],
            "stop_events": data["downtime"][0]["stops"],
            "engines": data["engines"][0]["engines"],
            "scrap": (o["total_parts"] or 0) - (o["good_parts"] or 0),
        },
        "by_station": data["by_station"],
        "pareto": pareto,
        "shift": [{"shift": s, "minutes": shift_lookup.get(s, 0)} for s in SHIFTS],
        "trend": [{"date": r["production_date"].isoformat(), "oee": r["oee"]} for r in data["trend"]],
        "status": data["status"],
        "events": data["events"],
    }))


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")