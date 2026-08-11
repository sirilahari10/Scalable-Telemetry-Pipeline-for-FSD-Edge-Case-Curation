-- 1. Base Table for raw disengagement events
CREATE TABLE fsd_metrics.raw_edge_cases (
    vehicle_id String,
    timestamp DateTime,
    event_type String,
    speed_mph Float32
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (event_type, vehicle_id, timestamp);

-- 2. Materialized View to instantly serve 'Miles per Critical Intervention' metrics 
CREATE MATERIALIZED VIEW fsd_metrics.mv_intervention_stats
ENGINE = AggregatingMergeTree()
ORDER BY (toStartOfDay(timestamp), event_type)
AS SELECT
    toStartOfDay(timestamp) AS day,
    event_type,
    countState() AS total_events,
    uniqState(vehicle_id) AS unique_vehicles_impacted
FROM fsd_metrics.raw_edge_cases
GROUP BY day, event_type;
