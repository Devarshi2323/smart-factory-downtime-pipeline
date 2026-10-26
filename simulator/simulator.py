import argparse
import heapq
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

load_dotenv()

LINE_ID = os.getenv("LINE_ID", "LINE3")
TOPIC_PREFIX = f"factory/{LINE_ID.lower()}"

STATIONS = [
    ("OP10", "Block Load", 55.0, 0.006),
    ("OP20", "Crank Install", 57.0, 0.008),
    ("OP30", "Piston Insert", 56.0, 0.007),
    ("OP40", "Head Torque", 58.0, 0.015),
    ("OP50", "Leak Test", 60.0, 0.030),
]

STATION_FAULTS = {
    "OP10": [("F101", "FAULTED", 6, 4, 14), ("F102", "FAULTED", 3, 2, 6), ("M001", "STARVED", 2, 5, 18)],
    "OP20": [("F201", "FAULTED", 4, 3, 9), ("F102", "FAULTED", 2, 2, 6)],
    "OP30": [("F301", "FAULTED", 4, 5, 16), ("M001", "STARVED", 2, 5, 15)],
    "OP40": [("F401", "FAULTED", 5, 3, 10), ("F402", "FAULTED", 2, 8, 22)],
    "OP50": [("F501", "FAULTED", 5, 2, 7)],
}

COMMON_FAULTS = [
    ("F901", "FAULTED", 1, 3, 10),
    ("F902", "FAULTED", 1, 1, 3),
    ("P001", "STOPPED", 1, 5, 10),
]

FAULT_PROBABILITY = 1 / 90
HEARTBEAT_S = 60
NEIGHBOR_IMPACT_DELAY_S = (30, 120)

log = logging.getLogger("simulator")


@dataclass
class Station:
    station_id: str
    name: str
    ideal_cycle_s: float
    scrap_rate: float
    state: str = "RUNNING"
    fault_code: str | None = None
    waiting_on: str | None = None
    state_since: datetime | None = None
    token: int = 0


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class LineSimulator:
    def __init__(self, publish, start_time: datetime, rng: random.Random, verbose: bool = True):
        self.publish = publish
        self.clock = start_time
        self.rng = rng
        self.verbose = verbose
        self.order = [s[0] for s in STATIONS]
        self.stations = {sid: Station(sid, name, ideal, scrap) for sid, name, ideal, scrap in STATIONS}
        self.queue = []
        self.seq = 0
        self.messages = 0

    def start(self):
        for sid in self.order:
            station = self.stations[sid]
            station.state_since = self.clock
            self.emit_state(station)
            self.schedule_next_cycle(station)
        self.schedule_heartbeat()

    def next_event_time(self) -> datetime | None:
        return self.queue[0][0] if self.queue else None

    def schedule(self, at: datetime, kind: str, station: Station, extra=None):
        self.seq += 1
        heapq.heappush(self.queue, (at, self.seq, kind, station.station_id, station.token, extra))

    def schedule_heartbeat(self):
        self.seq += 1
        heapq.heappush(self.queue, (self.clock + timedelta(seconds=HEARTBEAT_S), self.seq, "HEARTBEAT", None, 0, None))

    def step(self):
        at, _, kind, sid, token, extra = heapq.heappop(self.queue)
        self.clock = at
        if kind == "HEARTBEAT":
            self.publish(f"{TOPIC_PREFIX}/heartbeat", {"line": LINE_ID, "type": "HEARTBEAT", "ts": iso(self.clock)})
            self.schedule_heartbeat()
            return
        station = self.stations[sid]
        if kind == "IMPACT":
            self.apply_impact(station, extra)
        elif token != station.token:
            return
        elif kind == "CYCLE":
            self.complete_cycle(station, extra)
        elif kind == "RECOVER":
            self.recover(station)

    def schedule_next_cycle(self, station: Station):
        cycle_time = station.ideal_cycle_s * self.rng.uniform(1.0, 1.12)
        self.schedule(self.clock + timedelta(seconds=cycle_time), "CYCLE", station, cycle_time)

    def complete_cycle(self, station: Station, cycle_time: float):
        result = "SCRAP" if self.rng.random() < station.scrap_rate else "GOOD"
        self.send(station, {"type": "CYCLE", "cycle_time_s": round(cycle_time, 1), "result": result})
        if self.verbose:
            log.info("CYCLE %-4s %-14s %5.1fs  %s", station.station_id, station.name, cycle_time, result)
        if self.rng.random() < FAULT_PROBABILITY:
            self.start_fault(station)
        else:
            self.schedule_next_cycle(station)

    def pick_fault(self, station: Station):
        options = STATION_FAULTS[station.station_id] + COMMON_FAULTS
        code, state, _, low, high = self.rng.choices(options, weights=[o[2] for o in options], k=1)[0]
        minutes = self.rng.triangular(low, high, low + (high - low) * 0.3)
        return code, state, minutes

    def start_fault(self, station: Station):
        code, state, minutes = self.pick_fault(station)
        self.change_state(station, state, code)
        if self.verbose:
            log.info("FAULT %-4s %-14s %s -> %s", station.station_id, station.name, code, state)
        self.schedule(self.clock + timedelta(minutes=minutes), "RECOVER", station)
        for neighbor_id, impact_state in self.neighbors(station.station_id):
            delay = self.rng.uniform(*NEIGHBOR_IMPACT_DELAY_S)
            self.schedule(self.clock + timedelta(seconds=delay), "IMPACT", self.stations[neighbor_id],
                          (station.station_id, station.token, impact_state))

    def apply_impact(self, neighbor: Station, extra):
        source_id, source_token, impact_state = extra
        source = self.stations[source_id]
        if source.token != source_token or neighbor.state != "RUNNING":
            return
        neighbor.waiting_on = source_id
        self.change_state(neighbor, impact_state, None)
        if self.verbose:
            log.info("STATE %-4s %-14s RUNNING -> %s (waiting on %s)", neighbor.station_id, neighbor.name,
                     impact_state, source_id)

    def recover(self, station: Station):
        down_min = (self.clock - station.state_since).total_seconds() / 60
        self.change_state(station, "RUNNING", None)
        if self.verbose:
            log.info("STATE %-4s %-14s back to RUNNING (down %.1f min)", station.station_id, station.name, down_min)
        self.schedule_next_cycle(station)
        for neighbor_id, _ in self.neighbors(station.station_id):
            neighbor = self.stations[neighbor_id]
            if neighbor.waiting_on == station.station_id:
                self.release(neighbor)

    def release(self, station: Station):
        station.waiting_on = None
        for other_id, impact_state in self.neighbors(station.station_id):
            if self.stations[other_id].fault_code is not None:
                station.waiting_on = other_id
                self.change_state(station, impact_state, None)
                return
        self.change_state(station, "RUNNING", None)
        if self.verbose:
            log.info("STATE %-4s %-14s back to RUNNING", station.station_id, station.name)
        self.schedule_next_cycle(station)

    def neighbors(self, station_id: str):
        index = self.order.index(station_id)
        result = []
        if index + 1 < len(self.order):
            result.append((self.order[index + 1], "STARVED"))
        if index > 0:
            result.append((self.order[index - 1], "BLOCKED"))
        return result

    def change_state(self, station: Station, state: str, fault_code: str | None):
        station.state = state
        station.fault_code = fault_code
        station.state_since = self.clock
        station.token += 1
        self.emit_state(station)

    def emit_state(self, station: Station):
        self.send(station, {"type": "STATE", "state": station.state, "fault_code": station.fault_code})

    def send(self, station: Station, body: dict):
        message = {"line": LINE_ID, "station": station.station_id, "ts": iso(self.clock), **body}
        self.publish(f"{TOPIC_PREFIX}/{station.station_id}/event", message)
        self.messages += 1


class MqttPublisher:
    def __init__(self, host: str, port: int):
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="line3-simulator")
        for attempt in range(1, 11):
            try:
                self.client.connect(host, port, keepalive=60)
                break
            except OSError:
                log.warning("MQTT broker %s:%s not reachable (attempt %d/10). Is Docker running?", host, port, attempt)
                time.sleep(3)
        else:
            raise SystemExit("Could not connect to the MQTT broker. Run: docker compose up -d")
        self.client.loop_start()
        self.count = 0
        log.info("Connected to MQTT broker %s:%s", host, port)

    def __call__(self, topic: str, message: dict):
        info = self.client.publish(topic, json.dumps(message), qos=1)
        self.count += 1
        if self.count % 200 == 0:
            info.wait_for_publish()

    def close(self):
        self.client.loop_stop()
        self.client.disconnect()


def run(backfill_days: float, live: bool, seed: int | None):
    publisher = MqttPublisher(os.getenv("MQTT_HOST", "localhost"), int(os.getenv("MQTT_PORT", "1883")))
    rng = random.Random(seed)
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=backfill_days)
    sim = LineSimulator(publisher, start, rng, verbose=backfill_days == 0)
    sim.start()

    try:
        if backfill_days > 0:
            log.info("Backfilling %.1f days of history (%s -> now)...", backfill_days, iso(start))
            while sim.next_event_time() <= datetime.now(timezone.utc):
                sim.step()
                if sim.messages % 5000 == 0:
                    log.info("  %s simulated, %d messages published", iso(sim.clock), sim.messages)
            log.info("Backfill complete: %d messages published", sim.messages)

        if live:
            sim.verbose = True
            log.info("Live mode: %s simulation running in real time. Press Ctrl+C to stop.", LINE_ID)
            while True:
                wait = (sim.next_event_time() - datetime.now(timezone.utc)).total_seconds()
                if wait > 0:
                    time.sleep(min(wait, 1.0))
                    continue
                sim.step()
    except KeyboardInterrupt:
        log.info("Stopping simulator")
    finally:
        log.info("Published %d messages", sim.messages)
        publisher.close()


def main():
    parser = argparse.ArgumentParser(description="Line 3 engine assembly simulator")
    parser.add_argument("--backfill-days", type=float, default=0, help="generate this many days of history first")
    parser.add_argument("--no-live", action="store_true", help="stop after the backfill")
    parser.add_argument("--seed", type=int, default=None, help="random seed for repeatable data")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-5s %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    run(args.backfill_days, not args.no_live, args.seed)


if __name__ == "__main__":
    main()