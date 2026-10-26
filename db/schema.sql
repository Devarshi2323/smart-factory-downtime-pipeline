CREATE TABLE IF NOT EXISTS stations (
    station_id     TEXT PRIMARY KEY,
    station_name   TEXT NOT NULL,
    sequence_no    INT  NOT NULL,
    ideal_cycle_s  NUMERIC(6,1) NOT NULL
);

CREATE TABLE IF NOT EXISTS fault_codes (
    fault_code   TEXT PRIMARY KEY,
    description  TEXT NOT NULL,
    category     TEXT NOT NULL,
    is_planned   BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS machine_events (
    event_id      BIGSERIAL PRIMARY KEY,
    event_ts      TIMESTAMPTZ NOT NULL,
    received_ts   TIMESTAMPTZ NOT NULL DEFAULT now(),
    line_id       TEXT NOT NULL,
    station_id    TEXT NOT NULL REFERENCES stations(station_id),
    event_type    TEXT NOT NULL CHECK (event_type IN ('STATE', 'CYCLE')),
    payload       JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS state_intervals (
    interval_id   BIGSERIAL PRIMARY KEY,
    station_id    TEXT NOT NULL REFERENCES stations(station_id),
    state         TEXT NOT NULL CHECK (state IN ('RUNNING', 'STOPPED', 'FAULTED', 'STARVED', 'BLOCKED')),
    fault_code    TEXT REFERENCES fault_codes(fault_code),
    start_ts      TIMESTAMPTZ NOT NULL,
    end_ts        TIMESTAMPTZ,
    duration_s    NUMERIC GENERATED ALWAYS AS (EXTRACT(EPOCH FROM (end_ts - start_ts))) STORED
);

CREATE TABLE IF NOT EXISTS part_cycles (
    cycle_id      BIGSERIAL PRIMARY KEY,
    station_id    TEXT NOT NULL REFERENCES stations(station_id),
    completed_ts  TIMESTAMPTZ NOT NULL,
    cycle_time_s  NUMERIC(6,1) NOT NULL CHECK (cycle_time_s > 0),
    result        TEXT NOT NULL CHECK (result IN ('GOOD', 'SCRAP'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_open_interval_per_station
    ON state_intervals (station_id) WHERE end_ts IS NULL;

CREATE INDEX IF NOT EXISTS ix_intervals_station_start ON state_intervals (station_id, start_ts);
CREATE INDEX IF NOT EXISTS ix_cycles_station_ts       ON part_cycles (station_id, completed_ts);
CREATE INDEX IF NOT EXISTS ix_events_ts               ON machine_events (event_ts);

INSERT INTO stations (station_id, station_name, sequence_no, ideal_cycle_s) VALUES
    ('OP10', 'Block Load',     1, 55.0),
    ('OP20', 'Crank Install',  2, 57.0),
    ('OP30', 'Piston Insert',  3, 56.0),
    ('OP40', 'Head Torque',    4, 58.0),
    ('OP50', 'Leak Test',      5, 60.0)
ON CONFLICT (station_id) DO NOTHING;

INSERT INTO fault_codes (fault_code, description, category, is_planned) VALUES
    ('F101', 'Conveyor jam',               'Mechanical', FALSE),
    ('F102', 'Part not seated',            'Mechanical', FALSE),
    ('F201', 'Press-fit force low',        'Quality',    FALSE),
    ('F301', 'Robot gripper fault',        'Electrical', FALSE),
    ('F401', 'Torque out of spec',         'Quality',    FALSE),
    ('F402', 'Nutrunner fault',            'Electrical', FALSE),
    ('F501', 'Leak test fail',             'Quality',    FALSE),
    ('F901', 'Sensor fault',               'Electrical', FALSE),
    ('F902', 'Light curtain trip',         'Safety',     FALSE),
    ('M001', 'Material shortage',          'Material',   FALSE),
    ('P001', 'Tool change',                'Planned',    TRUE)
ON CONFLICT (fault_code) DO NOTHING;

CREATE OR REPLACE VIEW v_downtime AS
SELECT
    si.interval_id,
    si.station_id,
    s.station_name,
    s.sequence_no,
    si.state,
    si.fault_code,
    COALESCE(fc.description,
             CASE si.state WHEN 'BLOCKED' THEN 'Blocked by next station'
                           WHEN 'STARVED' THEN 'Starved by previous station'
                           ELSE 'Unspecified stop' END)            AS reason,
    COALESCE(fc.category,
             CASE WHEN si.state IN ('BLOCKED', 'STARVED') THEN 'Line flow' ELSE 'Other' END) AS category,
    COALESCE(fc.is_planned, FALSE)                                AS is_planned,
    si.start_ts,
    si.end_ts,
    ROUND(si.duration_s / 60.0, 2)                                AS duration_min,
    ((si.start_ts AT TIME ZONE 'America/Toronto') - INTERVAL '7 hours')::date AS production_date,
    CASE
        WHEN EXTRACT(HOUR FROM si.start_ts AT TIME ZONE 'America/Toronto') BETWEEN 7 AND 14  THEN 'Day'
        WHEN EXTRACT(HOUR FROM si.start_ts AT TIME ZONE 'America/Toronto') BETWEEN 15 AND 22 THEN 'Afternoon'
        ELSE 'Night'
    END                                                           AS shift
FROM state_intervals si
JOIN stations s          ON s.station_id = si.station_id
LEFT JOIN fault_codes fc ON fc.fault_code = si.fault_code
WHERE si.state <> 'RUNNING'
  AND si.end_ts IS NOT NULL;

CREATE OR REPLACE VIEW v_station_status AS
SELECT DISTINCT ON (s.station_id)
    s.station_id,
    s.station_name,
    s.sequence_no,
    si.state,
    si.fault_code,
    fc.description                                         AS fault_description,
    si.start_ts                                            AS state_since,
    ROUND(EXTRACT(EPOCH FROM (now() - si.start_ts)) / 60.0, 1) AS minutes_in_state
FROM stations s
LEFT JOIN state_intervals si ON si.station_id = s.station_id AND si.end_ts IS NULL
LEFT JOIN fault_codes fc     ON fc.fault_code = si.fault_code
ORDER BY s.station_id, si.start_ts DESC;

CREATE OR REPLACE VIEW v_oee_daily AS
WITH time_by_day AS (
    SELECT
        si.station_id,
        ((si.start_ts AT TIME ZONE 'America/Toronto') - INTERVAL '7 hours')::date AS production_date,
        SUM(si.duration_s) FILTER (WHERE NOT COALESCE(fc.is_planned, FALSE)) AS planned_s,
        SUM(si.duration_s) FILTER (WHERE si.state = 'RUNNING')               AS run_s
    FROM state_intervals si
    LEFT JOIN fault_codes fc ON fc.fault_code = si.fault_code
    WHERE si.end_ts IS NOT NULL
    GROUP BY 1, 2
),
parts_by_day AS (
    SELECT
        pc.station_id,
        ((pc.completed_ts AT TIME ZONE 'America/Toronto') - INTERVAL '7 hours')::date AS production_date,
        COUNT(*)                                   AS total_parts,
        COUNT(*) FILTER (WHERE pc.result = 'GOOD') AS good_parts
    FROM part_cycles pc
    GROUP BY 1, 2
)
SELECT
    t.station_id,
    s.station_name,
    t.production_date,
    ROUND(COALESCE(t.planned_s, 0), 1)                         AS planned_s,
    ROUND(COALESCE(t.run_s, 0), 1)                             AS run_s,
    COALESCE(p.total_parts, 0)                                 AS total_parts,
    COALESCE(p.good_parts, 0)                                  AS good_parts,
    ROUND(COALESCE(p.total_parts, 0) * s.ideal_cycle_s, 1)     AS ideal_output_s
FROM time_by_day t
JOIN stations s ON s.station_id = t.station_id
LEFT JOIN parts_by_day p
       ON p.station_id = t.station_id AND p.production_date = t.production_date;