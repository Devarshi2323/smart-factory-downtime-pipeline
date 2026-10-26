import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
import pytest
from psycopg.conninfo import make_conninfo

from collector import Collector, InvalidMessage, connection_string, parse_message

STATIONS = {"OP10", "OP20", "OP30", "OP40", "OP50"}
FAULTS = {"F101", "F401", "P001"}
T0 = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
SCHEMA = Path(__file__).resolve().parent.parent / "db" / "schema.sql"


def state(station, value, ts, fault=None):
    return json.dumps({"line": "LINE3", "station": station, "ts": ts.isoformat(), "type": "STATE",
                       "state": value, "fault_code": fault}).encode()


def cycle(station, ts, seconds=58.0, result="GOOD"):
    return json.dumps({"line": "LINE3", "station": station, "ts": ts.isoformat(), "type": "CYCLE",
                       "cycle_time_s": seconds, "result": result}).encode()


def heartbeat(ts):
    return json.dumps({"line": "LINE3", "ts": ts.isoformat(), "type": "HEARTBEAT"}).encode()


def heartbeats(collector, start, minutes):
    for minute in range(1, minutes + 1):
        collector.handle(heartbeat(start + timedelta(minutes=minute)))


def test_valid_state_message_is_parsed():
    msg = parse_message(state("OP40", "FAULTED", T0, "F401"), STATIONS, FAULTS)
    assert msg["state"] == "FAULTED"
    assert msg["ts"] == T0


@pytest.mark.parametrize("raw, reason", [
    (b"not json", "not valid JSON"),
    (json.dumps({"line": "LINE3", "ts": "2026-10-01T12:00:00Z", "type": "CYCLE"}).encode(), 'missing "station"'),
    (cycle("OP99", T0), 'unknown station "OP99"'),
    (state("OP10", "SLEEPING", T0), 'invalid state "SLEEPING"'),
    (state("OP10", "FAULTED", T0, "X999"), 'unknown fault code "X999"'),
    (cycle("OP10", T0, seconds=-5), "invalid cycle_time_s"),
    (cycle("OP10", T0, result="MAYBE"), 'invalid result "MAYBE"'),
    (json.dumps({"line": "LINE3", "station": "OP10", "ts": "2026-10-01T12:00:00", "type": "CYCLE",
                 "cycle_time_s": 50, "result": "GOOD"}).encode(), "no time zone"),
])
def test_bad_messages_are_rejected(raw, reason):
    with pytest.raises(InvalidMessage, match=reason):
        parse_message(raw, STATIONS, FAULTS)


def test_heartbeat_does_not_need_a_station():
    assert parse_message(heartbeat(T0), STATIONS, FAULTS)["type"] == "HEARTBEAT"


@pytest.fixture(scope="session")
def test_conninfo():
    try:
        admin = psycopg.connect(connection_string(), autocommit=True, connect_timeout=3)
    except psycopg.OperationalError:
        pytest.skip("PostgreSQL is not running (docker compose up -d)")
    if not admin.execute("SELECT 1 FROM pg_database WHERE datname = 'factory_test'").fetchone():
        admin.execute("CREATE DATABASE factory_test")
    admin.close()
    conninfo = make_conninfo(connection_string(), dbname="factory_test")
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute(SCHEMA.read_text(encoding="utf-8"))
    return conninfo


@pytest.fixture
def collector(test_conninfo):
    conn = psycopg.connect(test_conninfo, autocommit=True)
    conn.execute("TRUNCATE machine_events, state_intervals, part_cycles RESTART IDENTITY")
    yield Collector(conn)
    conn.close()


def intervals(collector):
    return collector.conn.execute(
        "SELECT station_id, state, fault_code, duration_s FROM state_intervals ORDER BY interval_id").fetchall()


def test_state_change_closes_interval_with_correct_duration(collector):
    collector.handle(state("OP40", "RUNNING", T0))
    heartbeats(collector, T0, 30)
    collector.handle(state("OP40", "FAULTED", T0 + timedelta(minutes=30), "F401"))
    heartbeats(collector, T0 + timedelta(minutes=30), 6)
    collector.handle(state("OP40", "RUNNING", T0 + timedelta(minutes=36)))
    rows = intervals(collector)
    assert rows[0][:2] == ("OP40", "RUNNING") and float(rows[0][3]) == 1800
    assert rows[1][:3] == ("OP40", "FAULTED", "F401") and float(rows[1][3]) == 360
    assert rows[2][1] == "RUNNING" and rows[2][3] is None


def test_repeated_state_does_not_create_new_interval(collector):
    collector.handle(state("OP10", "RUNNING", T0))
    collector.handle(state("OP10", "RUNNING", T0 + timedelta(minutes=1)))
    assert len(intervals(collector)) == 1


def test_out_of_order_state_is_ignored(collector):
    collector.handle(state("OP10", "RUNNING", T0))
    collector.handle(state("OP10", "FAULTED", T0 - timedelta(minutes=5), "F101"))
    assert [r[1] for r in intervals(collector)] == ["RUNNING"]


def test_cycles_are_saved(collector):
    collector.handle(state("OP50", "RUNNING", T0))
    collector.handle(cycle("OP50", T0 + timedelta(seconds=60), 60.4, "SCRAP"))
    row = collector.conn.execute("SELECT station_id, cycle_time_s, result FROM part_cycles").fetchone()
    assert row[0] == "OP50" and float(row[1]) == 60.4 and row[2] == "SCRAP"


def test_data_gap_closes_open_intervals_at_last_known_time(collector):
    collector.handle(state("OP10", "RUNNING", T0))
    collector.handle(heartbeat(T0 + timedelta(minutes=1)))
    collector.handle(state("OP10", "RUNNING", T0 + timedelta(hours=2)))
    rows = intervals(collector)
    assert float(rows[0][3]) == 60
    assert len(rows) == 2


def test_rejected_messages_are_counted_not_saved(collector):
    collector.handle(b"garbage")
    assert collector.counts["rejected"] == 1
    assert collector.conn.execute("SELECT COUNT(*) FROM machine_events").fetchone()[0] == 0


def test_downtime_view_excludes_running_and_open_intervals(collector):
    collector.handle(state("OP40", "RUNNING", T0))
    heartbeats(collector, T0, 10)
    collector.handle(state("OP40", "FAULTED", T0 + timedelta(minutes=10), "F401"))
    heartbeats(collector, T0 + timedelta(minutes=10), 6)
    collector.handle(state("OP40", "RUNNING", T0 + timedelta(minutes=16)))
    rows = collector.conn.execute("SELECT reason, duration_min FROM v_downtime").fetchall()
    assert rows == [("Torque out of spec", 6)]