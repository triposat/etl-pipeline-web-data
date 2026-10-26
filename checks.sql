-- Reject rate per source and UTC day: compare today with the days before it
SELECT source,
       timezone('UTC', captured_at)::DATE AS day,
       count(*) AS attempts,
       count(reason) AS rejected,
       round(100.0 * count(reason) / count(*), 1) AS reject_pct
FROM extract_log
GROUP BY ALL
ORDER BY day DESC, reject_pct DESC;

-- Freshness per item: the newest successful extract or stored row.
-- An item that never loaded shows NULL, so it cannot hide.
WITH tried AS (
    SELECT source, url, country,
           max(captured_at) FILTER (WHERE reason IS NULL) AS extracted
    FROM extract_log
    GROUP BY ALL
),
stored AS (
    SELECT url, country, max(captured_at) AS stored
    FROM prices
    GROUP BY ALL
)
SELECT t.source,
       right(url, 24) AS item,
       country,
       greatest(t.extracted, s.stored) AS last_loaded,
       date_diff('minute', last_loaded, now()) AS age_minutes
FROM tried AS t
LEFT JOIN stored AS s USING (url, country)
ORDER BY age_minutes DESC NULLS FIRST;
