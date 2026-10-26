import os

import psycopg
from dotenv import load_dotenv

load_dotenv()

QUERIES = {
    "Row counts": """
        SELECT (SELECT COUNT(*) FROM machine_events)  AS events,
               (SELECT COUNT(*) FROM part_cycles)     AS cycles,
               (SELECT COUNT(*) FROM state_intervals) AS intervals,
               (SELECT MIN(event_ts)::date FROM machine_events) AS first_day,
               (SELECT MAX(event_ts)::date FROM machine_events) AS last_day
    """,
    "OEE by station": """
        SELECT station_id,
               ROUND(100 * SUM(run_s) / NULLIF(SUM(planned_s), 0), 1)                AS availability,
               ROUND(100 * SUM(ideal_output_s) / NULLIF(SUM(run_s), 0), 1)           AS performance,
               ROUND(100.0 * SUM(good_parts) / NULLIF(SUM(total_parts), 0), 1)       AS quality,
               ROUND(100 * (SUM(run_s) / NULLIF(SUM(planned_s), 0))
                         * (SUM(ideal_output_s) / NULLIF(SUM(run_s), 0))
                         * (SUM(good_parts)::numeric / NULLIF(SUM(total_parts), 0)), 1) AS oee
        FROM v_oee_daily GROUP BY station_id ORDER BY station_id
    """,
    "Top downtime reasons (minutes)": """
        SELECT reason, category, ROUND(SUM(duration_min)) AS minutes, COUNT(*) AS stops
        FROM v_downtime GROUP BY reason, category ORDER BY minutes DESC LIMIT 8
    """,
    "Downtime by shift (minutes)": """
        SELECT shift, ROUND(SUM(duration_min)) AS minutes FROM v_downtime GROUP BY shift ORDER BY minutes DESC
    """,
    "Live station status": """
        SELECT station_id, station_name, state, COALESCE(fault_description, '') AS fault, minutes_in_state
        FROM v_station_status ORDER BY station_id
    """,
}


def print_table(cur):
    headers = [d.name for d in cur.description]
    rows = [[("" if v is None else str(v)) for v in row] for row in cur.fetchall()]
    widths = [max(len(h), *(len(r[i]) for r in rows)) if rows else len(h) for i, h in enumerate(headers)]
    print("  " + "  ".join(h.ljust(w) for h, w in zip(headers, widths)))
    print("  " + "  ".join("-" * w for w in widths))
    for row in rows:
        print("  " + "  ".join(v.ljust(w) for v, w in zip(row, widths)))


def main() -> None:
    conn_str = (
        f"host={os.getenv('DB_HOST', 'localhost')} "
        f"port={os.getenv('DB_PORT', '5432')} "
        f"dbname={os.getenv('DB_NAME', 'factory')} "
        f"user={os.getenv('DB_USER', 'factory')} "
        f"password={os.getenv('DB_PASSWORD', '')}"
    )
    with psycopg.connect(conn_str) as conn, conn.cursor() as cur:
        for title, sql in QUERIES.items():
            print(f"\n{title}")
            cur.execute(sql)
            print_table(cur)
    print()


if __name__ == "__main__":
    main()