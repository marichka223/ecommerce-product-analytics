CREATE OR REPLACE VIEW landing.v_clickstream_events_raw AS
SELECT 
    json_extract_string(metadata, '$.event_id') as event_id,
    metadata::JSON       AS metadata_json,
    keys::JSON           AS keys_json,
    raw_payload::JSON    AS raw_payload_json,
    DATE(regexp_extract(filename, 'dt=([^/]+)', 1)) AS dt,
    filename
FROM read_json_auto('../data/landing/clickstream/dt=*/part-*.jsonl', filename=true);

CREATE OR REPLACE VIEW landing.v_stripe_events_raw AS
SELECT 
    json_extract_string(raw_payload, '$.id') as event_id,
    metadata::JSON       AS metadata_json,
    keys::JSON           AS keys_json,
    raw_payload::JSON    AS raw_payload_json,
    DATE(regexp_extract(filename, 'dt=([^/]+)', 1)) AS dt,
    filename
FROM read_json_auto(
    '../data/landing/stripe_like/dt=*/part-*.jsonl',
    filename=true,
    union_by_name=true,
    sample_size=-1
);

UNION ALL

SELECT 
    json_extract_string(raw_payload, '$.id') as event_id,
    metadata::JSON       AS metadata_json,
    keys::JSON           AS keys_json,
    raw_payload::JSON    AS raw_payload_json,
    DATE(regexp_extract(filename, 'dt=([^/]+)', 1)) AS dt,
    filename
FROM read_json_auto(
    '../data/landing/stripe_like/refunds/dt=*/part-*.jsonl',
    filename=true,
    union_by_name=true,
    sample_size=-1
);


CREATE TABLE IF NOT EXISTS landing.stripe_events_raw(
    event_id VARCHAR,
    metadata_json JSON,
    keys_json JSON,
    raw_payload_json JSON,
    dt DATE,
    filename VARCHAR,
    load_id UUID,
    ingested_at TIMESTAMP,
    payload_hash VARCHAR
);

CREATE TABLE IF NOT EXISTS landing.clickstream_events_raw(
    event_id VARCHAR,
    metadata_json JSON,
    keys_json JSON,
    raw_payload_json JSON,
    dt DATE,
    filename VARCHAR,
    load_id UUID,
    ingested_at TIMESTAMP,
    payload_hash VARCHAR
);

-- inserting data into landing.stripe_events
with run AS(
    SELECT uuid() AS load_id, now() AS ingested_at
),

temp AS (
    SELECT 
        r.event_id,
        r.metadata_json, 
        r.keys_json,
        r.raw_payload_json,
        r.dt,
        r.filename,
        run.load_id,
        run.ingested_at,
        hex(sha256(r.raw_payload_json::VARCHAR)) AS payload_hash
    FROM landing.v_stripe_events_raw r
    CROSS JOIN run
)

INSERT INTO landing.stripe_events_raw
SELECT 
    t.event_id,
    t.metadata_json, 
    t.keys_json,
    t.raw_payload_json,
    t.dt,
    t.filename,
    t.load_id,
    t.ingested_at,
    t.payload_hash
FROM temp t
LEFT JOIN landing.stripe_events_raw s ON t.payload_hash = s.payload_hash
WHERE s.payload_hash IS NULL;


with run AS(
    SELECT uuid() AS load_id, now() AS ingested_at
),

temp AS (
    SELECT 
        r.event_id,
        r.metadata_json, 
        r.keys_json,
        r.raw_payload_json,
        r.dt,
        r.filename,
        run.load_id,
        run.ingested_at,
        hex(sha256(r.raw_payload_json::VARCHAR)) AS payload_hash
    FROM landing.v_clickstream_events_raw r
    CROSS JOIN run
)

INSERT INTO landing.clickstream_events_raw
SELECT 
    t.event_id,
    t.metadata_json, 
    t.keys_json,
    t.raw_payload_json,
    t.dt,
    t.filename,
    t.load_id,
    t.ingested_at,
    t.payload_hash
FROM temp t
LEFT JOIN landing.clickstream_events_raw s ON t.payload_hash = s.payload_hash
WHERE s.payload_hash IS NULL;




