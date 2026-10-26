import os
import subprocess
import sys
import time

import psycopg
from dotenv import load_dotenv

load_dotenv()


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


def main() -> None:
    subprocess.run([sys.executable, "db/init_db.py"], check=True)
    with psycopg.connect(connection_string()) as conn:
        has_data = conn.execute("SELECT EXISTS (SELECT 1 FROM machine_events)").fetchone()[0]

    collector = subprocess.Popen([sys.executable, "collector/collector.py"])
    time.sleep(8)

    simulator_cmd = [sys.executable, "simulator/simulator.py"]
    if not has_data:
        simulator_cmd += ["--backfill-days", os.getenv("BACKFILL_DAYS", "7"), "--seed", "42"]
        print("Empty database: loading history before going live", flush=True)
    simulator = subprocess.Popen(simulator_cmd)

    processes = {"collector": collector, "simulator": simulator}
    while True:
        for name, proc in processes.items():
            if proc.poll() is not None:
                print(f"{name} stopped (exit code {proc.returncode}). Restarting the pipeline.", flush=True)
                for other in processes.values():
                    if other.poll() is None:
                        other.terminate()
                sys.exit(1)
        time.sleep(5)


if __name__ == "__main__":
    main()