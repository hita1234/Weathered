-- Weathered — Apache Cassandra schema
-- Run with: cqlsh -f schema.cql
-- Replication is set for a 3-node single-DC cluster; raise RF and add
-- NetworkTopologyStrategy datacenters for a multi-region deployment.

CREATE KEYSPACE IF NOT EXISTS weathered
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

USE weathered;

-- 1. Validated weather/disaster events (written by the Spark ingest job
--    after the SparkNLP-style hoax/fabrication filter passes a crawled post).
--    Partitioned by state so "events in Tamil Nadu, most recent first" is a
--    single-partition read; clustered by event_time descending for that order.
CREATE TABLE IF NOT EXISTS weather_events (
  state           text,
  event_time      timestamp,
  event_id        uuid,
  event_type      text,       -- e.g. 'Heavy Rainfall', 'Cyclone Alert'
  category        text,       -- derived: Weather | Flood | Cyclone | Heatwave | Landslide | Other
  city            text,
  severity        text,       -- high | med | low
  status          text,       -- ongoing | resolved
  source          text,       -- IMD | State Agency | Citizen + IMD
  latitude        double,
  longitude       double,
  validation_score double,    -- 0..1 confidence from the AI/ML validation layer
  raw_post_id     text,       -- reference back to the Scrapy-crawled source post
  PRIMARY KEY ((state), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC);

-- Query pattern: "all ongoing high-severity events" needs a second table
-- (Cassandra denormalizes rather than joins) since it isn't a state lookup.
CREATE TABLE IF NOT EXISTS events_by_severity (
  severity        text,
  event_time      timestamp,
  event_id        uuid,
  state           text,
  city            text,
  event_type      text,
  status          text,
  PRIMARY KEY ((severity), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC);

-- 2. Citizen disaster reports filed from the Report page.
--    Partitioned by state+district so a District Disaster Management
--    Authority can page through its own incoming reports.
CREATE TABLE IF NOT EXISTS disaster_reports (
  state           text,
  district        text,
  report_id       text,        -- e.g. 'DR-1000'
  created_at      timestamp,
  disaster_type   text,
  description     text,
  reporter_device_id text,
  gps_lat         double,
  gps_lon         double,
  capture_time    timestamp,
  media_refs      list<text>,  -- object storage keys/URLs for attached media
  validation_status text,      -- pending | validated | rejected
  sync_status     text,        -- waiting | synced
  PRIMARY KEY ((state, district), created_at, report_id)
) WITH CLUSTERING ORDER BY (created_at DESC);

-- Lookup a report by its public-facing ID regardless of partition.
CREATE TABLE IF NOT EXISTS reports_by_id (
  report_id       text PRIMARY KEY,
  state           text,
  district        text,
  created_at      timestamp
);

-- 3. Per-city forecast cache, refreshed by a scheduled Spark/Django job so the
--    dashboard reads pre-computed data instead of calling a model on every request.
CREATE TABLE IF NOT EXISTS forecast_cache (
  city            text,
  forecast_date   date,
  high_c          int,
  low_c           int,
  condition       text,
  rain_pct        int,
  humidity_pct    int,
  wind_kmh        int,
  uv_index        int,
  generated_at    timestamp,
  PRIMARY KEY ((city), forecast_date)
);

-- 4. Device/subscription registry for push notifications
--    (Severe Weather Alerts / Disaster Alerts toggles in Settings).
CREATE TABLE IF NOT EXISTS alert_subscriptions (
  device_id       text PRIMARY KEY,
  state           text,
  district        text,
  severe_weather  boolean,
  disaster_alerts boolean,
  event_updates   boolean,
  system_notifs   boolean,
  language        text,
  updated_at      timestamp
);

-- 5. Raw crawl audit trail: every Scrapy-sourced post, before and after
--    validation — kept so a rejected/hoax post can be reviewed later.
CREATE TABLE IF NOT EXISTS crawl_audit (
  crawl_date      date,
  crawled_at      timestamp,
  post_id         text,
  source_platform text,
  raw_text        text,
  hashtag         text,
  validation_result text,   -- accepted | rejected_hoax | rejected_duplicate
  validation_score double,
  linked_event_id uuid,
  PRIMARY KEY ((crawl_date), crawled_at, post_id)
) WITH CLUSTERING ORDER BY (crawled_at DESC);
