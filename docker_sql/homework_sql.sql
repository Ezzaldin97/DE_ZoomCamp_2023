--q3
--20689
select count(*) from public."green_tripdata_2019-01"
where cast(lpep_pickup_datetime as date) = '2019-01-15';
--q4
--2019-01-15
select cast(lpep_pickup_datetime as date),
       trip_distance
from public."green_tripdata_2019-01"
order by 2 desc limit 1;
--q5
--2: 1282 ; 3: 254
select count(case when passenger_count = 2 then index end) as trip_2,
       count(case when passenger_count = 3 then index end) as trip_3
from public."green_tripdata_2019-01"
where cast(lpep_pickup_datetime as date) = '2019-01-01' and
      passenger_count in (2, 3);
--q6
--Long Island City/Queens Plaza
select t3."Zone", t1.tip_amount as largest_tip
from public."green_tripdata_2019-01" as t1
inner join public.taxi_zone_lookup as t2 on t2."LocationID" = t1."PULocationID"
inner join public.taxi_zone_lookup as t3 on t3."LocationID" = t1."DOLocationID"
where t2."Zone" = 'Astoria'
order by 2 desc limit 1;