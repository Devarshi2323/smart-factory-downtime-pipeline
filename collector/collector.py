import json
import logging
import os
from datetime import datetime, timedelta, timezone

import paho.mqtt.client as mqtt
import psycopg
from dotenv import load_dotenv

load_dotenv()

LINE_ID = os.getenv("LINE_ID", "LINE3")
TOPICS = [(f"factory/{LINE_ID.lower()}/+/event", 1), (f"factory/{LINE_ID.lower()}/heartbeat", 1)]
VALID_STATES = {"RUNNING", "STOPPED", "FAULTED", "STARVED", "BLOCKED"}
VALID_RESULTS = {"GOOD", "SCRAP"}
DATA_GAP = timedelta(minutes=3)
LIVE_WINDOW = timedelta(minutes=5)

log = logging.getLogger("collector")


class InvalidMessage(ValueError):
    pass


def connection_string() -> str:
    return (
        f"host={os.getenv('DB_HOST', 'localhost')} "
        f"port={os.getenv('DB_PORT', '5432')} "
        f"dbname={os.getenv('DB_NAME', 'factory')} "
        f"user={os.getenv('DB_USER', 'factory')} "
        f"password={os.getenv('DB_PASSWORD', '')}"
    )


def parse_message(raw: bytes, known_stations: set[str], known_faults: set[str]) -> dict:
    try:
        msg = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise InvalidMessage(f"not valid JSON ({exc})")
    if not isinstance(msg, dict):
        raise InvalidMessage("message is not a JSON object")

    required = ("line", "ts", "type") if msg.get("type") == "HEARTBEAT" else ("line", "station", "ts", "type")
    for field in required:
        if field not in msg:
            raise InvalidMessage(f'missing "{field}" field')
    if "station" in required and msg["station"] not in known_stations:
        raise InvalidMessage(f'unknown station "{msg["station"]}"')
    try:
        msg["ts"] = datetime.fromisoformat(str(msg["ts"]).replace("Z", "+00:00"))
    except ValueError:
        raise InvalidMessage(f'bad timestamp "{msg["ts"]}"')
    if msg["ts"].tzinfo is None:
        raise InvalidMessage("timestamp has no time zone")

    if msg["type"] == "HEARTBEAT":
        pass
    elif msg["type"] == "STATE":
        if msg.get("state") not in VALID_STATES:
            raise InvalidMessage(f'invalid state "{msg.get("state")}"')
        fault = msg.get("fault_code")
        if fault is not None and fault not in known_faults:
            raise InvalidMessage(f'unknown fault code "{fault}"')
    elif msg["type"] == "CYCLE":
        cycle_time = msg.get("cycle_time_s")
        if not isinstance(cycle_time, (int, float)) or cycle_time <= 0:
            raise InvalidMessage(f'invalid cycle_time_s "{cycle_time}"')
        if msg.get("result") not in VALID_RESULTS:
            raise InvalidMessage(f'invalid result "{msg.get("result")}"')
    else:
        raise InvalidMessage(f'unknown type "{msg["type"]}"')
    return msg


class Collector:
    def __init__(self, conn: psycopg.Connection):
        self.conn = conn
        self.stations = {r[0] for r in conn.execute("SELECT station_id FROM stations")}
        self.faults = {r[0] for r in conn.execute("SELECT fault_code FROM fault_codes")}
        self.last_line_ts = None
        self.counts = {"events": 0, "cycles": 0, "intervals": 0, "rejected": 0}

    def handle(self, raw: bytes) -> str | None:
        try:
            msg = parse_message(raw, self.stations, self.faults)
        except InvalidMessage as exc:
            self.counts["rejected"] += 1
            log.warning("Rejected message: %s", exc)
            return None

        with self.conn.transaction():
            self.close_gap_if_needed(msg["ts"])
            if msg["type"] == "HEARTBEAT":
                self.advance(msg["ts"])
                return None
            raw_payload = {**msg, "ts": msg["ts"].isoformat()}
            self.conn.execute(
                "INSERT INTO machine_events (event_ts, line_id, station_id, event_type, payload) "
                "VALUES (%s, %s, %s, %s, %s)",
                (msg["ts"], msg["line"], msg["station"], msg["type"], json.dumps(raw_payload)),
            )
            self.counts["events"] += 1
            if msg["type"] == "CYCLE":
                result = self.save_cycle(msg)
            else:
                result = self.save_state(msg)
        self.advance(msg["ts"])
        return result

    def advance(self, ts: datetime):
        if self.last_line_ts is None or ts > self.last_line_ts:
            self.last_line_ts = ts

    def close_gap_if_needed(self, ts: datetime):
        if self.last_line_ts is None or ts - self.last_line_ts <= DATA_GAP:
            return
        closed = self.conn.execute(
            "UPDATE state_intervals SET end_ts = %s WHERE end_ts IS NULL AND start_ts <= %s",
            (self.last_line_ts, self.last_line_ts),
        ).rowcount
        if closed:
            log.warning("Data gap of %s detected. Closed %d open intervals at last known time %s",
                        ts - self.last_line_ts, closed, self.last_line_ts.isoformat())

    def save_cycle(self, msg: dict) -> str:
        self.conn.execute(
            "INSERT INTO part_cycles (station_id, completed_ts, cycle_time_s, result) VALUES (%s, %s, %s, %s)",
            (msg["station"], msg["ts"], msg["cycle_time_s"], msg["result"]),
        )
        self.counts["cycles"] += 1
        return f"cycle      {msg['station']}  {msg['cycle_time_s']:.1f}s {msg['result'].lower()}"

    def save_state(self, msg: dict) -> str:
        station, ts, state, fault = msg["station"], msg["ts"], msg["state"], msg.get("fault_code")
        open_row = self.conn.execute(
            "SELECT interval_id, state, fault_code, start_ts FROM state_intervals "
            "WHERE station_id = %s AND end_ts IS NULL FOR UPDATE",
            (station,),
        ).fetchone()

        if open_row:
            interval_id, open_state, open_fault, open_start = open_row
            if ts < open_start:
                log.warning("Out-of-order event for %s at %s ignored (open interval started %s)",
                            station, ts.isoformat(), open_start.isoformat())
                return f"skipped    {station}  out-of-order"
            if open_state == state and open_fault == fault:
                return f"unchanged  {station}  {state}"
            self.conn.execute("UPDATE state_intervals SET end_ts = %s WHERE interval_id = %s", (ts, interval_id))

        self.conn.execute(
            "INSERT INTO state_intervals (station_id, state, fault_code, start_ts) VALUES (%s, %s, %s, %s)",
            (station, state, fault, ts),
        )
        self.counts["intervals"] += 1
        if open_row:
            minutes, seconds = divmod(int((ts - open_row[3]).total_seconds()), 60)
            return f"interval   {station}  {open_row[1]:<8} {minutes}m {seconds:02d}s closed -> now {state} {fault or ''}"
        return f"interval   {station}  now {state} {fault or ''}"


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-5s %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    conn = psycopg.connect(connection_string(), autocommit=True)
    conn.execute("SET synchronous_commit TO OFF")
    collector = Collector(conn)
    log.info("Database ready (%s stations, %s fault codes)", len(collector.stations), len(collector.faults))

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            log.error("MQTT connection failed: %s", reason_code)
            return
        client.subscribe(TOPICS)
        log.info("Subscribed to %s", ", ".join(t for t, _ in TOPICS))

    def on_message(client, userdata, message):
        try:
            result = collector.handle(message.payload)
        except psycopg.Error as exc:
            log.error("Database error, message skipped: %s", exc)
            if conn.closed:
                raise SystemExit("Lost database connection. Check: docker compose ps")
            return
        total = collector.counts["events"]
        if result and collector.last_line_ts and datetime.now(timezone.utc) - collector.last_line_ts < LIVE_WINDOW:
            log.info("SAVED %s", result)
        if result and total % 2000 == 0:
            log.info("Rows saved: events %d, cycles %d, intervals %d, rejected %d", total,
                     collector.counts["cycles"], collector.counts["intervals"], collector.counts["rejected"])

    def on_disconnect(client, userdata, flags, reason_code, properties):
        if reason_code != 0:
            log.warning("Disconnected from MQTT broker (%s). Reconnecting...", reason_code)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="line3-collector", clean_session=False)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    client.connect(os.getenv("MQTT_HOST", "localhost"), int(os.getenv("MQTT_PORT", "1883")), keepalive=60)

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        log.info("Stopping collector")
    finally:
        log.info("Rows saved: events %d, cycles %d, intervals %d, rejected %d", collector.counts["events"],
                 collector.counts["cycles"], collector.counts["intervals"], collector.counts["rejected"])
        client.disconnect()
        conn.close()


if __name__ == "__main__":
    main()