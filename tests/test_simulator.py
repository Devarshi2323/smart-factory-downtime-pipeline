import random
from datetime import datetime, timedelta, timezone

import pytest

import simulator
from simulator import LineSimulator

START = datetime(2026, 10, 1, 7, 0, tzinfo=timezone.utc)


def make_sim(seed=1):
    messages = []
    sim = LineSimulator(lambda topic, msg: messages.append((topic, msg)), START, random.Random(seed), verbose=False)
    sim.start()
    return sim, messages


def run_until(sim, end):
    while sim.next_event_time() <= end:
        sim.step()


def test_start_reports_every_station_running():
    _, messages = make_sim()
    states = [m for _, m in messages if m["type"] == "STATE"]
    assert [m["station"] for m in states] == ["OP10", "OP20", "OP30", "OP40", "OP50"]
    assert all(m["state"] == "RUNNING" and m["fault_code"] is None for m in states)


def test_messages_use_station_topics_and_utc_timestamps():
    sim, messages = make_sim()
    run_until(sim, START + timedelta(minutes=5))
    for topic, msg in messages:
        if msg["type"] == "HEARTBEAT":
            assert topic == "factory/line3/heartbeat"
        else:
            assert topic == f"factory/line3/{msg['station']}/event"
        assert msg["ts"].endswith("Z")


def test_no_parts_are_produced_while_a_station_is_down():
    sim, messages = make_sim(seed=7)
    run_until(sim, START + timedelta(days=2))
    current = {}
    for _, msg in messages:
        if msg["type"] == "STATE":
            current[msg["station"]] = msg["state"]
        elif msg["type"] == "CYCLE":
            assert current[msg["station"]] == "RUNNING"


def test_fault_starves_downstream_and_blocks_upstream(monkeypatch):
    monkeypatch.setattr(simulator, "FAULT_PROBABILITY", 0)
    sim, _ = make_sim()
    run_until(sim, START + timedelta(minutes=2))
    sim.pick_fault = lambda station: ("F301", "FAULTED", 10)
    sim.start_fault(sim.stations["OP30"])

    run_until(sim, sim.clock + timedelta(minutes=3))
    assert sim.stations["OP30"].state == "FAULTED"
    assert sim.stations["OP20"].state == "BLOCKED"
    assert sim.stations["OP40"].state == "STARVED"
    assert sim.stations["OP10"].state == "RUNNING"

    run_until(sim, sim.clock + timedelta(minutes=10))
    assert all(s.state == "RUNNING" for s in sim.stations.values())


def test_heartbeat_every_minute():
    sim, messages = make_sim()
    run_until(sim, START + timedelta(minutes=10, seconds=1))
    heartbeats = [m for _, m in messages if m["type"] == "HEARTBEAT"]
    assert len(heartbeats) == 10


def test_same_seed_gives_same_data():
    sim_a, messages_a = make_sim(seed=42)
    sim_b, messages_b = make_sim(seed=42)
    run_until(sim_a, START + timedelta(hours=6))
    run_until(sim_b, START + timedelta(hours=6))
    assert messages_a == messages_b


def test_week_of_data_has_realistic_availability():
    sim, messages = make_sim(seed=3)
    end = START + timedelta(days=7)
    run_until(sim, end)
    running = {sid: 0.0 for sid in sim.order}
    last = {}
    for _, msg in messages:
        if msg["type"] != "STATE":
            continue
        ts = datetime.fromisoformat(msg["ts"].replace("Z", "+00:00"))
        sid = msg["station"]
        if sid in last and last[sid][0] == "RUNNING":
            running[sid] += (ts - last[sid][1]).total_seconds()
        last[sid] = (msg["state"], ts)
    total = (end - START).total_seconds()
    for sid in sim.order:
        if last[sid][0] == "RUNNING":
            running[sid] += (end - last[sid][1]).total_seconds()
        assert 0.75 < running[sid] / total < 0.97, sid