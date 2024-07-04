DECLARE Base_Fare DEFAULT 2;
DECLARE Cost_per_Mile DEFAULT 1.15;
DECLARE Cost_per_Minute DEFAULT 0.22;
DECLARE Service_Fee DEFAULT 1.75;

-- Create a master data table that joins all three tables: driver_ids, ride_ids, ride_timestamps
CREATE TABLE driver_data.master_data
AS
WITH requested_at AS (
SELECT ride_id,
       timestamp AS requested_at
FROM driver_data.ride_timestamps
WHERE event = 'requested_at'
)

, accepted_at AS (
SELECT ride_id,
       timestamp AS accepted_at
FROM driver_data.ride_timestamps
WHERE event = 'accepted_at'
)

, arrived_at AS (
SELECT ride_id,
       timestamp AS arrived_at
FROM driver_data.ride_timestamps
WHERE event = 'arrived_at'
)

, picked_up_at AS (
SELECT ride_id,
       timestamp AS picked_up_at
FROM driver_data.ride_timestamps
WHERE event = 'picked_up_at'
)

, dropped_off_at AS (
SELECT ride_id,
       timestamp AS dropped_off_at
FROM driver_data.ride_timestamps
WHERE event = 'dropped_off_at'
)

, event_flatten AS (
SELECT requested_at.ride_id,
       requested_at.requested_at,
       accepted_at.accepted_at,
       arrived_at.arrived_at,
       picked_up_at.picked_up_at,
       dropped_off_at.dropped_off_at
FROM requested_at
LEFT JOIN accepted_at USING (ride_id)
LEFT JOIN arrived_at USING (ride_id)
LEFT JOIN picked_up_at USING (ride_id)
LEFT JOIN dropped_off_at USING (ride_id)
)

, tbl AS (
SELECT 
  COALESCE(d.driver_id, r.driver_id) AS driver_id,
	-- d.driver_id,
	d.driver_onboard_date,
	r.ride_id,
	r.ride_distance,
	r.ride_duration,
	r.ride_prime_time,
	e.requested_at,
  e.accepted_at,
  e.arrived_at,
  e.picked_up_at,
  e.dropped_off_at
FROM driver_data.driver_ids d
FULL OUTER JOIN driver_data.ride_ids r ON r.driver_id = d.driver_id
LEFT JOIN event_flatten e ON e.ride_id = r.ride_id
-- LEFT JOIN driver_data.ride_timestamps ts ON r.ride_id = ts.ride_id
)

, fare_calculation AS (
SELECT
driver_id,
driver_onboard_date,
ride_id,
ride_distance,
ride_duration,
ride_prime_time,
CASE WHEN (Base_Fare + Cost_per_Mile * ride_distance * 0.000621371 + Cost_per_Minute * (ride_duration/60)) * (1 + (ride_prime_time/100)) + Service_Fee < 5 THEN 5
     WHEN (Base_Fare + Cost_per_Mile * ride_distance * 0.000621371 + Cost_per_Minute * (ride_duration/60)) * (1 + (ride_prime_time/100)) + Service_Fee > 400 THEN 400
     ELSE (Base_Fare + Cost_per_Mile * ride_distance * 0.000621371 + Cost_per_Minute * (ride_duration/60)) * (1 + (ride_prime_time/100)) + Service_Fee
     END AS ride_fare,
DATE(requested_at) AS ride_date,
requested_at,
accepted_at,
case when arrived_at > picked_up_at then picked_up_at else arrived_at end as arrived_at,
picked_up_at,
dropped_off_at
FROM tbl
)

SELECT *
FROM fare_calculation;

-- RFM Segmentation
WITH temp AS (
SELECT driver_id,
       CASE WHEN DATE_DIFF(DATE('2016-06-26'), MAX(ride_date), DAY) IS NULL THEN 91
            ELSE DATE_DIFF(DATE('2016-06-26'), MAX(ride_date), DAY)
       END AS days_since_last_drive,
       CASE WHEN SAFE_DIVIDE(COUNT(DISTINCT ride_id), COUNT(DISTINCT ride_date)) IS NULL THEN 0
            ELSE ROUND(SAFE_DIVIDE(COUNT(DISTINCT ride_id),COUNT(DISTINCT ride_date)),2) 
       END AS ride_freq_during_active,
       CASE WHEN SAFE_DIVIDE(SUM(ride_fare), COUNT(DISTINCT ride_date)) IS NULL THEN 0
            ELSE ROUND(SAFE_DIVIDE(SUM(ride_fare), COUNT(DISTINCT ride_date)),2)
       END AS earnings_per_active_day,
FROM `driver_data.master_data`
GROUP BY 1
)

, temp2 AS (
SELECT driver_id,
       days_since_last_drive,
       CASE WHEN days_since_last_drive <= 7 THEN '3'
            WHEN days_since_last_drive <= 30 THEN '2'
            ELSE '1'
       END AS r_group,
       ride_freq_during_active,
       CASE WHEN ride_freq_during_active <= 4 THEN '1'
            WHEN ride_freq_during_active <= 8 THEN '2'
            ELSE '3'
       END AS f_group,
       earnings_per_active_day,
       CASE WHEN earnings_per_active_day <= 60 THEN '1'
            WHEN earnings_per_active_day <= 110 THEN '2'
            ELSE '3'
       END AS m_group,
FROM temp
)

, temp3 AS (
SELECT driver_id, CONCAT(r_group, f_group, m_group) as rfm_group
FROM temp2
)

,final AS (
SELECT driver_id,
       CASE WHEN rfm_group IN ('111', '112', '121', '122', '132', '133', '123') THEN 'lost/churn'
            WHEN rfm_group IN ('321', '312', '311', '221', '212', '211') THEN 'at_risk'
            WHEN rfm_group IN ('322', '233', '232', '223', '222') THEN 'loyal'
            WHEN rfm_group IN ('333', '332', '323') THEN 'high_performer'
            ELSE rfm_group
       END AS segment
FROM temp3
)

SELECT segment,
       COUNT(DISTINCT driver_id) AS num_driver
FROM final
GROUP BY 1;

--- Feature Engineering, including features for Hypothesis Testing
WITH tbl AS (
SELECT *,
       TIMESTAMP_DIFF(accepted_at, requested_at, SECOND) AS acceptance_duration,
       TIMESTAMP_DIFF(arrived_at, accepted_at, SECOND) AS arrival_duration,
       TIMESTAMP_DIFF(picked_up_at, arrived_at, SECOND) AS pickup_duration,
FROM driver_data.master_data
WHERE ride_id IS NOT NULL
)

, feature1 AS (
SELECT driver_id,
       driver_onboard_date,
       MIN(ride_date) AS first_drive,
       MAX(ride_date) AS last_drive,
       COUNT(DISTINCT ride_id) AS num_drive,
       COUNT(ride_date) AS day_drive,
       MIN(ride_distance) AS min_ride_distance,
       MAX(ride_distance) AS max_ride_distance,
       SUM(ride_distance) AS total_ride_distance,
       MIN(ride_duration) AS min_ride_duration,
       MAX(ride_duration) AS max_ride_duration,
       SUM(ride_duration) AS total_ride_duration,
       SAFE_DIVIDE(SUM(ride_distance),COUNT(DISTINCT ride_date)) AS avg_distance_per_day,
       SAFE_DIVIDE(SUM(ride_duration),COUNT(DISTINCT ride_date)) AS avg_duration_per_day,
       SAFE_DIVIDE(SUM(ride_distance),COUNT(DISTINCT ride_id)) AS avg_distance_per_drive,
       SAFE_DIVIDE(SUM(ride_duration),COUNT(DISTINCT ride_id)) AS avg_duration_per_drive,
       MIN(acceptance_duration) AS min_acceptance_duration,
       MAX(acceptance_duration) AS max_acceptance_duration,
       AVG(acceptance_duration) AS avg_acceptance_duration,
       MIN(arrival_duration) AS min_arrival_duration,
       MAX(arrival_duration) AS max_arrival_duration,
       AVG(arrival_duration) AS avg_arrival_duration,
       MIN(pickup_duration) AS min_wait_duration,
       MAX(pickup_duration) AS max_wait_duration,
       AVG(pickup_duration) AS avg_wait_duration,
       AVG(ride_fare)*0.8 AS avg_earning_per_ride,
       MIN(ride_fare)*0.8 AS min_earning_ride,
       MAX(ride_fare)*0.8 AS max_earning_ride,
       SAFE_DIVIDE(SUM(ride_fare)*0.8, COUNT(DISTINCT ride_date)) AS earnings_per_active_day,
       CASE WHEN SAFE_DIVIDE(COUNT(DISTINCT ride_id), COUNT(DISTINCT ride_date)) IS NULL THEN 0
            ELSE SAFE_DIVIDE(COUNT(DISTINCT ride_id), COUNT(DISTINCT ride_date)) 
       END AS ride_freq_during_active,
       AVG(ride_prime_time) as avg_primetime_multiplier,
FROM tbl
GROUP BY 1, 2
)

, feature2 AS (
SELECT driver_id,
      --  PERCENTILE_CONT(ride_duration, 0.25) OVER (PARTITION BY driver_id) AS per25_ride_duration,
       PERCENTILE_CONT(ride_duration, 0.5) OVER (PARTITION BY driver_id) AS median_ride_duration,
      --  PERCENTILE_CONT(ride_duration, 0.75) OVER (PARTITION BY driver_id) AS per75_ride_duration,
FROM tbl
GROUP BY driver_id, ride_duration
)

, feature3 AS (
SELECT driver_id,
      --  PERCENTILE_CONT(ride_distance, 0.25) OVER (PARTITION BY driver_id) AS per25_ride_distance,
       PERCENTILE_CONT(ride_distance, 0.5) OVER (PARTITION BY driver_id) AS median_ride_distance,
      --  PERCENTILE_CONT(ride_distance, 0.75) OVER (PARTITION BY driver_id) AS per75_ride_distance,
FROM tbl
GROUP BY driver_id, ride_distance
)

, feature4 AS (
SELECT driver_id,
      --  PERCENTILE_CONT(acceptance_duration, 0.25) OVER (PARTITION BY driver_id) AS per25_acceptance_duration,
       PERCENTILE_CONT(acceptance_duration, 0.5) OVER (PARTITION BY driver_id) AS median_acceptance_duration,
      --  PERCENTILE_CONT(acceptance_duration, 0.75) OVER (PARTITION BY driver_id) AS per75_acceptance_duration,
FROM tbl
GROUP BY driver_id, acceptance_duration
)

, feature5 AS (
SELECT driver_id,
      --  PERCENTILE_CONT(arrival_duration, 0.25) OVER (PARTITION BY driver_id) AS per25_arrival_duration,
       PERCENTILE_CONT(arrival_duration, 0.5) OVER (PARTITION BY driver_id) AS median_arrival_duration,
      --  PERCENTILE_CONT(arrival_duration, 0.75) OVER (PARTITION BY driver_id) AS per75_arrival_duration,
FROM tbl
GROUP BY driver_id, arrival_duration
)

, feature6 AS (
SELECT driver_id,
      --  PERCENTILE_CONT(pickup_duration, 0.25) OVER (PARTITION BY driver_id) AS per25_pickup_duration,
       PERCENTILE_CONT(pickup_duration, 0.5) OVER (PARTITION BY driver_id) AS median_pickup_duration,
      --  PERCENTILE_CONT(pickup_duration, 0.75) OVER (PARTITION BY driver_id) AS per75_pickup_duration,
FROM tbl
GROUP BY driver_id, pickup_duration
)

, num_rides_in_30days AS (
SELECT driver_id,
       COUNT(DISTINCT CASE WHEN days_since_onboarding <= 30 THEN ride_id END) AS num_rides_in_1st_30days
FROM 
    (
    SELECT driver_id,
          ride_id,
          DATE(driver_onboard_date) AS driver_onboard_date,
          ride_date,
          DATE_DIFF(ride_date, DATE(driver_onboard_date), DAY) AS days_since_onboarding
    FROM tbl
    )
GROUP BY 1
)

SELECT *,
DATE_DIFF(DATE('2016-06-26'), DATE(driver_onboard_date), DAY) AS n_days_after_onboarding,
DATE_DIFF(DATE('2016-06-26'), last_drive, DAY) AS n_days_after_last_drive,
DATE_DIFF(first_drive, DATE(driver_onboard_date), DAY) AS n_days_to_first_drive,
CASE WHEN first_drive IS NULL OR DATE_DIFF(DATE('2016-06-26'), last_drive, DAY) >= 21 THEN 1 ELSE 0 END AS churn,
FROM feature1
LEFT JOIN num_rides_in_30days USING (driver_id)
-- -- LEFT JOIN feature2 USING (driver_id)
-- -- LEFT JOIN feature3 USING (driver_id)
-- -- LEFT JOIN feature4 USING (driver_id)
-- -- LEFT JOIN feature5 USING (driver_id)
-- -- LEFT JOIN feature6 USING (driver_id) 
-- ORDER BY 1,2
;

-- process data for churn identification
WITH data AS (
SELECT driver_id,
       driver_onboard_date,
       requested_at
FROM driver_data.ride_timestamps
WHERE driver_onboard_date IS NOT NULL AND requested_at IS NOT NULL
)

, max_date_all AS (
SELECT MAX(requested_at) AS max_date
FROM data
)

, all_data AS (
SELECT driver_id,
       driver_onboard_date,
       requested_at,
       max_date
FROM data d
LEFT JOIN max_date_all m ON 1=1
)

, check_date AS (
SELECT driver_id,
       DATE_DIFF(DATE(requested_at), DATE(driver_onboard_date), DAY) AS active_date_index,
       DATE_DIFF(DATE(max_date), DATE(driver_onboard_date), DAY) + 1 AS num_day
FROM all_data
)

, check_date_grouped AS (
SELECT DISTINCT *
FROM check_date
)

, check_date_agg AS (
SELECT driver_id,
       num_day,
       '[' || STRING_AGG(CAST(active_date_index AS STRING), ', ' ORDER BY active_date_index) || ']' AS date_index
FROM check_date_grouped
GROUP BY 1, 2
)

SELECT *
FROM check_date_agg;
