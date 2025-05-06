CREATE TABLE clean_sensor_data (
  sensorId STRING,
  `timestamp` BIGINT,
  temperature FLOAT,
  humidity FLOAT,
  battery FLOAT,
  airQuality FLOAT
) WITH ('changelog.mode' = 'retract')


-- duplicates by sensorId and timestamp + cleanup without sensorid
INSERT INTO clean_sensor_data
SELECT sensorId,
       `timestamp`,
       temperature,
       humidity,
       battery,
       airQuality
FROM (
  SELECT
    sensorId,
    `timestamp`,
    temperature,
    humidity,
    battery,
    airQuality,
    ROW_NUMBER() OVER (
      PARTITION BY sensorId, `timestamp`
      ORDER BY `timestamp` DESC
    ) AS rn
  FROM `sensor-data`
  WHERE sensorId IS NOT NULL AND sensorId <> ''
)
WHERE rn = 1;

----
CREATE TABLE sensor_data_timestamped (
  sensorId STRING,
  ts TIMESTAMP(3),
  temperature FLOAT,
  humidity FLOAT,
  battery FLOAT,
  airQuality FLOAT,
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH ('changelog.mode' = 'retract')

INSERT INTO sensor_data_timestamped
SELECT 
  sensorId,
  CAST(TO_TIMESTAMP_LTZ(`timestamp`, 3) AS TIMESTAMP(3)) AS ts,
  temperature,
  humidity,
  battery,
  airQuality
FROM clean_sensor_data;


CREATE TABLE sensor_data_per_minute (
  sensorId STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  avg_temperature FLOAT,
  std_temperature FLOAT,
  avg_humidity FLOAT,
  std_humidity FLOAT,
  avg_battery FLOAT,
  std_battery FLOAT,
  avg_airQuality FLOAT,
  std_airQuality FLOAT
) 

INSERT INTO sensor_data_per_minute
SELECT
  sensorId,
  window_start,
  window_end,
  AVG(temperature) AS avg_temperature,
  STDDEV_POP(temperature) AS std_temperature,
  AVG(humidity) AS avg_humidity,
  STDDEV_POP(humidity) AS std_humidity,
  AVG(battery) AS avg_battery,
  STDDEV_POP(battery) AS std_battery,
  AVG(airQuality) AS avg_airQuality,
  STDDEV_POP(airQuality) AS std_airQuality
FROM TABLE(
  TUMBLE(
    TABLE sensor_data_timestamped,
    DESCRIPTOR(ts),
    INTERVAL '1' MINUTE
  )
)
GROUP BY sensorId, window_start, window_end;
