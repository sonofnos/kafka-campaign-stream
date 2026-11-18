-- Kafka Campaign Stream — PostgreSQL sink tables

-- ── Streaming aggregation sinks ───────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS stream_campaign_counts (
    id              BIGSERIAL PRIMARY KEY,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    campaign_id     VARCHAR(10) NOT NULL,
    event_type      VARCHAR(20) NOT NULL,
    event_count     INT DEFAULT 0,
    total_revenue   NUMERIC(12,2) DEFAULT 0,
    unique_customers INT DEFAULT 0,
    written_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_stream_counts_campaign_window
    ON stream_campaign_counts(campaign_id, window_start);

CREATE TABLE IF NOT EXISTS stream_conversion_alerts (
    id              BIGSERIAL PRIMARY KEY,
    window_start    TIMESTAMPTZ NOT NULL,
    campaign_id     VARCHAR(10) NOT NULL,
    conversion_count INT NOT NULL,
    total_revenue   NUMERIC(12,2) DEFAULT 0,
    alert_type      VARCHAR(50) NOT NULL,
    alerted_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stream_channel_performance (
    id              BIGSERIAL PRIMARY KEY,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    channel         VARCHAR(20) NOT NULL,
    total_events    INT DEFAULT 0,
    impressions     INT DEFAULT 0,
    clicks          INT DEFAULT 0,
    conversions     INT DEFAULT 0,
    ctr_pct         NUMERIC(6,2),
    cvr_pct         NUMERIC(6,2),
    written_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ── Reporting views ───────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW live_campaign_performance_vw AS
SELECT
    campaign_id,
    SUM(event_count) FILTER (WHERE event_type = 'impression')   AS total_impressions,
    SUM(event_count) FILTER (WHERE event_type = 'click')        AS total_clicks,
    SUM(event_count) FILTER (WHERE event_type = 'conversion')   AS total_conversions,
    SUM(total_revenue)                                           AS total_revenue,
    MAX(unique_customers)                                        AS peak_unique_customers,
    MAX(window_end)                                              AS last_updated
FROM stream_campaign_counts
WHERE window_start >= NOW() - INTERVAL '24 hours'
GROUP BY campaign_id
ORDER BY total_conversions DESC;


CREATE OR REPLACE VIEW recent_alerts_vw AS
SELECT
    campaign_id,
    alert_type,
    conversion_count,
    total_revenue,
    alerted_at
FROM stream_conversion_alerts
WHERE alerted_at >= NOW() - INTERVAL '1 hour'
ORDER BY alerted_at DESC;


CREATE OR REPLACE VIEW channel_ctr_trend_vw AS
SELECT
    channel,
    DATE_TRUNC('hour', window_start)                AS hour,
    ROUND(AVG(ctr_pct), 2)                          AS avg_ctr,
    ROUND(AVG(cvr_pct), 2)                          AS avg_cvr,
    SUM(impressions)                                AS total_impressions
FROM stream_channel_performance
WHERE window_start >= NOW() - INTERVAL '24 hours'
GROUP BY channel, DATE_TRUNC('hour', window_start)
ORDER BY hour DESC, avg_cvr DESC;
