import os

import psycopg
from dotenv import load_dotenv

load_dotenv()


def main() -> None:
    conn_str = (
        f"host={os.getenv('DB_HOST', 'localhost')} "
        f"port={os.getenv('DB_PORT', '5432')} "
        f"dbname={os.getenv('DB_NAME', 'factory')} "
        f"user={os.getenv('DB_USER', 'factory')} "
        f"password={os.getenv('DB_PASSWORD', '')}"
    )
    with psycopg.connect(conn_str) as conn:
        conn.execute("TRUNCATE machine_events, state_intervals, part_cycles RESTART IDENTITY")
        conn.commit()
    print("All machine data deleted. Stations and fault codes kept.")


if __name__ == "__main__":
    main()