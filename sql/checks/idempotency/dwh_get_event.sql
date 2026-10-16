SELECT
    id AS raw_id,
    kafka_topic AS topic,
    kafka_partition AS partition,
    kafka_offset AS offset
FROM raw.cdc_events
WHERE (event_key ->> 'id')::bigint = :'product_id'::bigint
  AND op = 'c'
  AND kafka_topic = :'kafka_topic'
\gset event_

SELECT
    :'event_raw_id',
    :'event_topic',
    :'event_partition',
    :'event_offset';
