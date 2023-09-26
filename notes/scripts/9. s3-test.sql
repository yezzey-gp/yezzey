-- 1. Выборка агрегата (без редистрибуции) - select count(1) from <table>
select count(1) from ny_taxi_src_gp;
-- 1 rows - 18.758s, 2023-08-04 в 12:57:05
-- 1 rows - 18.456s, 2023-08-04 в 12:57:56
-- 1 rows - 17.960s, 2023-08-04 в 12:58:23

select count(1) from ny_taxi_s3_2013_2022;
-- 1 rows - 11m 24s (1ms получ.), 2023-08-04 в 13:10:17
-- 1 rows - 13m 30s (1ms получ.), 2023-08-04 в 13:29:03
-- 1 rows - 9m 21s, 2023-08-04 в 17:36:09


-- 2. Выборка агрегата с одним разрезом select vendorid, count(1) from <table> group by vendorid;
select vendorid, count(1) from ny_taxi_src_gp group by vendorid;
-- 6 rows - 25.497s, 2023-08-09 в 14:20:24
-- 6 rows - 25.212s, 2023-08-09 в 14:21:21
-- 6 rows - 25.285s, 2023-08-09 в 14:22:52

select vendorid, count(1) from ny_taxi_s3_2013_2022	 group by vendorid;
-- 6 rows - 10m 3s, 2023-08-09 в 14:40:12
-- 6 rows - 10m 0s, 2023-08-09 в 14:52:09
-- 6 rows - 8m 59s, 2023-08-13 в 12:03:07


-- 3. Выборка агрегата с 4 разрезами (для оценки снижения производительности при добавлении колонок в выборку)
-- select vendorid, pickup_date, passenger_count, payment_type, count(1) from <table> group by 1,2,3,4
select vendorid, pickup_date, passenger_count, payment_type, count(1)
from ny_taxi_src_gp
group by 1,2,3,4
-- 200 rows - 38.943s (7ms получ.), 2023-08-13 в 12:08:05
-- 200 rows - 38.370s (1ms получ.), 2023-08-13 в 12:09:16
-- 200 rows - 39.515s (1ms получ.), 2023-08-13 в 12:10:44

select vendorid, pickup_date, passenger_count, payment_type, count(1)
from ny_taxi_s3_2013_2022
group by 1,2,3,4
-- 200 rows - 10m 14s (2ms получ.), 2023-08-13 в 12:21:55
-- 200 rows - 11m 41s, 2023-08-13 в 12:35:09
-- 200 rows - 11m 15s (1ms получ.), 2023-08-13 в 12:47:05


-- 4. Выборка с аналитической функцией на 1й партиции 
select distinct dolocationid, max(total_amount) over (partition by dolocationid) from ny_taxi_src_gp
where pickup_date between '2014-01-01'::date and '2014-12-31'::date
-- 200 rows - 39.543s (1ms получ.), 2023-08-13 в 12:48:11
-- 200 rows - 37.803s (1ms получ.), 2023-08-13 в 12:53:55
-- 200 rows - 37.928s (2ms получ.), 2023-08-13 в 12:54:42

select distinct dolocationid, max(total_amount) over (partition by dolocationid) from ny_taxi_s3_2013_2022
where pickup_date between '2014-01-01'::date and '2014-12-31'::date
-- 200 rows - 1m 38s (1ms получ.), 2023-08-13 в 13:14:01
-- 200 rows - 1m 34s, 2023-08-13 в 13:16:01
-- 200 rows - 1m 34s (1ms получ.), 2023-08-13 в 13:19:49


-- 5. Выборка с аналитической функцией на 3х партициях
select distinct dolocationid, max(total_amount) over (partition by dolocationid) from ny_taxi_src_gp
where pickup_date between '2014-01-01'::date and '2016-12-31'::date
-- 200 rows - 1m 45s (1ms получ.), 2023-08-13 в 13:21:53
-- 200 rows - 1m 44s (1ms получ.), 2023-08-13 в 13:23:47
-- 200 rows - 1m 46s (1ms получ.), 2023-08-13 в 13:26:06

select distinct dolocationid, max(total_amount) over (partition by dolocationid) from ny_taxi_s3_2013_2022
where pickup_date between '2014-01-01'::date and '2016-12-31'::date
-- 200 rows - 4m 30s (4ms получ.), 2023-08-13 в 13:58:48
-- 200 rows - 4m 30s (2ms получ.), 2023-08-13 в 14:04:26
-- 200 rows - 4m 27s (2ms получ.), 2023-08-13 в 14:29:30


-- 6. Выборка с соединением со справочником и агрегацией по 3м партициям
select z.locationid, count(r.vendorid), sum(r.total_amount)
from ny_taxi_src_gp r
left join ny_taxi_zones_src_gp z
on r.pulocationid=z.locationid
where r.pickup_date between '2014-01-01'::date and '2016-12-31'::date
group by 1
-- 200 rows - 27.930s (1ms получ.), 2023-08-13 в 14:33:32
-- 200 rows - 27.963s (1ms получ.), 2023-08-13 в 14:34:50
-- 200 rows - 27.405s, 2023-08-13 в 14:39:56

select z.locationid, count(r.vendorid), sum(r.total_amount)
from ny_taxi_s3_2013_2022 r
left join ny_taxi_zones_src_gp z
on r.pulocationid=z.locationid
where r.pickup_date between '2014-01-01'::date and '2016-12-31'::date
group by 1
-- 200 rows - 5m 24s (1ms получ.), 2023-08-13 в 15:00:49
-- 200 rows - 5m 24s (1ms получ.), 2023-08-13 в 15:06:51
-- 200 rows - 3m 52s (1ms получ.), 2023-08-13 в 15:11:00
-- 200 rows - 3m 49s (2ms получ.), 2023-08-13 в 15:15:02
-- 200 rows - 3m 55s (1ms получ.), 2023-08-13 в 15:19:13

-- 7. Выборка с соединением со справочником и агрегацией по всей таблице
select z.locationid, count(r.vendorid), sum(r.total_amount)
from ny_taxi_src_gp r
left join ny_taxi_zones_src_gp z
on r.pulocationid=z.locationid
group by 1
-- 200 rows - 1m 0s, 2023-08-13 в 15:23:16
-- 200 rows - 59.799s (1ms получ.), 2023-08-13 в 15:24:25
-- 200 rows - 59.639s, 2023-08-13 в 15:26:36

select z.locationid, count(r.vendorid), sum(r.total_amount)
from ny_taxi_s3_2013_2022 r
left join ny_taxi_zones_src_gp z
on r.pulocationid=z.locationid
group by 1
-- 200 rows - 13m 16s, 2023-08-13 в 15:40:10
-- 200 rows - 9m 49s, 2023-08-13 в 17:07:54
-- 200 rows - 9m 45s (1ms получ.), 2023-08-13 в 17:22:51
-- 200 rows - 11m 48s (1ms получ.), 2023-08-13 в 17:50:19