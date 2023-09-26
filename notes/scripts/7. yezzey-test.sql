-- 1. Выборка агрегата (без редистрибуции) - select count(1) from <table>
select count(1) from ny_taxi_src_gp;
-- 1 rows - 14.614s, 2023-07-27 в 16:50:19 - 1 warning(s)
-- 1 rows - 15.973s, 2023-07-27 в 16:50:53 - 1 warning(s)
-- 1 rows - 14.971s, 2023-07-27 в 16:51:21 - 1 warning(s)

select count(1) from ny_taxi_yezzey;
-- 1 rows - 20.938s, 2023-07-27 в 17:02:11 - 1 warning(s)
-- 1 rows - 21.788s, 2023-07-27 в 17:03:47 - 1 warning(s)
-- 1 rows - 19.429s, 2023-07-27 в 17:04:16 - 1 warning(s)

select count(1) from ny_taxi_pxf_2013_2022;
-- 1 rows - 6m 6s, 2023-07-27 в 17:10:32 - 1 warning(s) 
-- 1 rows - 6m 6s, 2023-07-27 в 17:26:25 - 1 warning(s)
-- 1 rows - 6m 4s, 2023-07-27 в 17:33:16 - 1 warning(s)


-- 2. Выборка агрегата с одним разрезом select vendorid, count(1) from <table> group by vendorid;
select vendorid, count(1) from ny_taxi_src_gp group by vendorid;
-- 6 rows - 22.480s, 2023-07-27 в 17:43:51 - 1 warning(s)
-- 6 rows - 20.424s, 2023-07-27 в 17:44:26 - 1 warning(s)
-- 6 rows - 21.179s, 2023-07-27 в 17:45:05 - 1 warning(s)

select vendorid, count(1) from ny_taxi_yezzey group by vendorid;
-- 6 rows - 25.671s, 2023-07-27 в 17:46:53 - 1 warning(s)
-- 6 rows - 26.687s, 2023-07-27 в 17:47:31 - 1 warning(s)
-- 6 rows - 27.224s, 2023-07-27 в 17:48:17 - 1 warning(s)

select vendorid, count(1) from ny_taxi_pxf_2013_2022 group by vendorid;
-- 6 rows - 6m 34s, 2023-07-27 в 17:56:45
-- 6 rows - 6m 40s, 2023-07-27 в 18:19:48
-- 6 rows - 6m 35s, 2023-07-27 в 18:34:17


-- 3. Выборка агрегата с 4 разрезами (для оценки снижения производительности при добавлении колонок в выборку)
-- select vendorid, pickup_date, passenger_count, payment_type, count(1) from <table> group by 1,2,3,4
select vendorid, pickup_date, passenger_count, payment_type, count(1)
from ny_taxi_src_gp
group by 1,2,3,4
-- 200 rows - 36.420s (3ms получ.), 2023-07-27 в 18:38:32 - 1 warning(s)
-- 200 rows - 36.126s (2ms получ.), 2023-07-28 в 11:01:19 - 1 warning(s)
-- 200 rows - 36.206s (1ms получ.), 2023-07-28 в 11:02:19 - 1 warning(s)

select vendorid, pickup_date, passenger_count, payment_type, count(1)
from ny_taxi_yezzey
group by 1,2,3,4
-- 200 rows - 51.606s (4ms получ.), 2023-07-27 в 18:40:06
-- 200 rows - 51.83s (1ms получ.), 2023-07-28 в 11:03:34 - 1 warning(s)
-- 200 rows - 50.88s (1ms получ.), 2023-07-28 в 11:05:21 - 1 warning(s)

select vendorid, pickup_date, passenger_count, payment_type, count(1)
from ny_taxi_pxf_2013_2022
group by 1,2,3,4
-- 200 rows - 8m 9s (3ms получ.), 2023-07-28 в 11:23:40
-- 200 rows - 8m 30s (2ms получ.), 2023-07-28 в 11:33:51
-- 200 rows - 8m 30s (1ms получ.), 2023-07-28 в 11:53:37


-- 4. Выборка с аналитической функцией на 1й партиции 
select distinct dolocationid, max(total_amount) over (partition by dolocationid) from ny_taxi_src_gp
where pickup_date between '2014-01-01'::date and '2014-12-31'::date
-- 200 rows - 38.729s (1ms получ.), 2023-07-28 в 12:52:16 - 1 warning(s)
-- 200 rows - 40.822s (1ms получ.), 2023-07-28 в 12:53:33 - 1 warning(s)
-- 200 rows - 37.654s (1ms получ.), 2023-07-28 в 12:54:20 - 1 warning(s)

select distinct dolocationid, max(total_amount) over (partition by dolocationid) from ny_taxi_yezzey
where pickup_date between '2014-01-01'::date and '2014-12-31'::date
-- 200 rows - 40.494s (1ms получ.), 2023-07-28 в 12:56:31 - 1 warning(s)
-- 200 rows - 40.197s, 2023-07-28 в 12:57:40 - 1 warning(s)
-- 200 rows - 39.647s (1ms получ.), 2023-07-28 в 12:58:31 - 1 warning(s)

select distinct dolocationid, max(total_amount) over (partition by dolocationid) from ny_taxi_pxf_2013_2022
where pickup_date between '2014-01-01'::date and '2014-12-31'::date
-- 200 rows - 1m 38s (2ms получ.), 2023-07-28 в 13:01:32 - 1 warning(s)
-- 200 rows - 1m 36s (1ms получ.), 2023-07-28 в 13:03:54 - 1 warning(s)
-- 200 rows - 1m 44s, 2023-07-28 в 13:05:48 - 1 warning(s)


-- 5. Выборка с аналитической функцией на 3х партициях
select distinct dolocationid, max(total_amount) over (partition by dolocationid) from ny_taxi_src_gp
where pickup_date between '2014-01-01'::date and '2016-12-31'::date
-- 200 rows - 1m 44s, 2023-07-28 в 13:12:03 - 1 warning(s)
-- 200 rows - 1m 48s, 2023-07-28 в 13:14:46 - 1 warning(s)
-- 200 rows - 1m 40s, 2023-07-28 в 14:44:06 - 1 warning(s)

select distinct dolocationid, max(total_amount) over (partition by dolocationid) from ny_taxi_yezzey
where pickup_date between '2014-01-01'::date and '2016-12-31'::date
-- 200 rows - 1m 47s, 2023-07-28 в 14:50:07 - 1 warning(s)
-- 200 rows - 1m 44s, 2023-07-28 в 14:53:25 - 1 warning(s)
-- 200 rows - 1m 46s, 2023-07-28 в 14:55:21 - 1 warning(s)

select distinct dolocationid, max(total_amount) over (partition by dolocationid) from ny_taxi_pxf_2013_2022
where pickup_date between '2014-01-01'::date and '2016-12-31'::date
-- 200 rows - 4m 16s (1ms получ.), 2023-07-28 в 15:11:09 - 1 warning(s)
-- 200 rows - 4m 18s (1ms получ.), 2023-07-28 в 16:20:33 - 1 warning(s)
-- 200 rows - 4m 12s, 2023-07-28 в 16:26:42 - 1 warning(s)


-- 6. Выборка с соединением со справочником и агрегацией по 3м партициям
select z.locationid, count(r.vendorid), sum(r.total_amount)
from ny_taxi_src_gp r
left join ny_taxi_zones_src_gp z
on r.pulocationid=z.locationid
where r.pickup_date between '2014-01-01'::date and '2016-12-31'::date
group by 1
-- 200 rows - 28.818s (3ms получ.), 2023-07-28 в 16:37:30 - 1 warning(s)
-- 200 rows - 27.413s (2ms получ.), 2023-07-28 в 16:38:12 - 1 warning(s)
-- 200 rows - 28.622s (1ms получ.), 2023-07-28 в 16:41:14 - 1 warning(s)

select z.locationid, count(r.vendorid), sum(r.total_amount)
from ny_taxi_yezzey r
left join ny_taxi_zones_src_gp z
on r.pulocationid=z.locationid
where r.pickup_date between '2014-01-01'::date and '2016-12-31'::date
group by 1
-- 200 rows - 37.807s (1ms получ.), 2023-07-28 в 16:42:28 - 1 warning(s)
-- 200 rows - 36.25s, 2023-07-28 в 16:43:31 - 1 warning(s)
-- 200 rows - 36.117s (1ms получ.), 2023-07-28 в 16:44:22 - 1 warning(s)

select z.locationid, count(r.vendorid), sum(r.total_amount)
from ny_taxi_pxf_2013_2022 r
left join ny_taxi_zones_src_gp z
on r.pulocationid=z.locationid
where r.pickup_date between '2014-01-01'::date and '2016-12-31'::date
group by 1
-- 200 rows - 3m 45s, 2023-07-28 в 16:48:24 - 1 warning(s)
-- 200 rows - 3m 37s (1ms получ.), 2023-07-28 в 16:56:50 - 1 warning(s)
-- 200 rows - 3m 35s, 2023-07-28 в 17:02:15 - 1 warning(s)


-- 7. Выборка с соединением со справочником и агрегацией по всей таблице
select z.locationid, count(r.vendorid), sum(r.total_amount)
from ny_taxi_src_gp r
left join ny_taxi_zones_src_gp z
on r.pulocationid=z.locationid
group by 1
-- 200 rows - 56.936s, 2023-07-28 в 17:11:02 - 1 warning(s)
-- 200 rows - 55.202s, 2023-07-28 в 17:12:14 - 1 warning(s)
-- 200 rows - 55.626s, 2023-07-28 в 17:13:27 - 1 warning(s)

select z.locationid, count(r.vendorid), sum(r.total_amount)
from ny_taxi_yezzey r
left join ny_taxi_zones_src_gp z
on r.pulocationid=z.locationid
group by 1
-- 200 rows - 1m 18s, 2023-07-28 в 17:14:52 - 1 warning(s)
-- 200 rows - 1m 13s, 2023-07-28 в 17:16:18 - 1 warning(s)
-- 200 rows - 1m 10s, 2023-07-28 в 17:19:12 - 1 warning(s)

select z.locationid, count(r.vendorid), sum(r.total_amount)
from ny_taxi_pxf_2013_2022 r
left join ny_taxi_zones_src_gp z
on r.pulocationid=z.locationid
group by 1
-- 200 rows - 8m 13s, 2023-07-28 в 17:30:28 - 1 warning(s)
-- 200 rows - 7m 59s, 2023-07-28 в 17:54:32 - 1 warning(s)
-- 200 rows - 8m 51s, 2023-07-28 в 18:05:22 - 1 warning(s)