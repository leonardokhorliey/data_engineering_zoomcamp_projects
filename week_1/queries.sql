--Q3.
SELECT COUNT(*) FROM green_trip_data WHERE DATE(lpep_pickup_datetime) = '2019-01-15'
AND DATE(lpep_dropoff_datetime) = '2019-01-15';
-- 20530

--Q4.
SELECT DATE(lpep_pickup_datetime) Date, trip_distance
FROM green_trip_data
ORDER BY trip_distance DESC LIMIT 1;
-- 2019-01-15

--Q5.
SELECT passenger_count, COUNT(*) AS trip_count
FROM green_trip_data
WHERE DATE(lpep_pickup_datetime) = '2019-01-01' AND passenger_count IN (2, 3)
GROUP BY passenger_count;
-- 2 1,282
-- 3 254

--Q6.
SELECT pickup_zones."Zone" Pickup, dropoff_zones."Zone" Dropoff, 
SUM(tip_amount) Total_Tip
FROM green_trip_data
LEFT JOIN taxi_zones AS pickup_zones ON green_trip_data."PULocationID" = pickup_zones."LocationID" 
LEFT JOIN taxi_zones AS dropoff_zones ON green_trip_data."DOLocationID" = dropoff_zones."LocationID" 
WHERE pickup_zones."Zone" = 'Astoria' AND dropoff_zones."Zone" IN ('Central Park', 'Jamaica','South Ozone Park', 'Long Island City/Queens Plaza')
GROUP BY pickup_zones."Zone", dropoff_zones."Zone"
ORDER BY Total_Tip DESC LIMIT 1;
-- Long Island City/Queens Plaza 800.32