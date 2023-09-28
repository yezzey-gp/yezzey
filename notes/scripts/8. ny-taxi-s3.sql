-- drop external table ny_taxi_s3_2013_rw
create writable external table ny_taxi_s3_2013_rw
(like ny_taxi_src_gp)
LOCATION('s3://storage.yandexcloud.net/alexey-luzan-prod/s3-test/2013/ config_server=http://10.129.0.13:8553/s3.config region=ru-central1-a')
FORMAT 'csv'
distributed RANDOMLY;


-- drop external table ny_taxi_s3_2013_ro
create readable external table ny_taxi_s3_2013_ro
(like ny_taxi_s3_2013_rw)
LOCATION('s3://storage.yandexcloud.net/alexey-luzan-prod/s3-test/2013/ config_server=http://10.129.0.13:8553/s3.config region=ru-central1-a')
FORMAT 'csv';

insert into ny_taxi_s3_2013_rw
select * from ny_taxi_src_gp
where pickup_date between '2013-01-01'::date and '2013-12-31'::date;
/*
Updated Rows 171690930
Query	
	insert into ny_taxi_s3_2013_rw
	select * from ny_taxi_src_gp
	where pickup_date between '2013-01-01'::date and '2013-12-31'::date
Start time	Fri Aug 04 00:15:57 MSK 2023
Finish time	Fri Aug 04 00:20:18 MSK 2023
*/


-- drop external table ny_taxi_s3_2014_rw
create writable external table ny_taxi_s3_2014_rw
(like ny_taxi_src_gp)
LOCATION('s3://storage.yandexcloud.net/alexey-luzan-prod/s3-test/2014/ config_server=http://10.129.0.13:8553/s3.config region=ru-central1-a')
FORMAT 'csv'
distributed RANDOMLY;

-- drop external table ny_taxi_s3_2014_ro
create readable external table ny_taxi_s3_2014_ro
(like ny_taxi_s3_2014_rw)
LOCATION('s3://storage.yandexcloud.net/alexey-luzan-prod/s3-test/2014/ config_server=http://10.129.0.13:8553/s3.config region=ru-central1-a')
FORMAT 'csv';

insert into ny_taxi_s3_2014_rw
select * from ny_taxi_src_gp
where pickup_date between '2014-01-01'::date and '2014-12-31'::date;
/*
Updated Rows 	165496715
Query	
	insert into ny_taxi_s3_2014_rw
	select * from ny_taxi_src_gp
	where pickup_date between '2014-01-01'::date and '2014-12-31'::date
Start time	Fri Aug 04 00:21:02 MSK 2023
Finish time	Fri Aug 04 00:25:12 MSK 2023
*/


-- drop external table ny_taxi_s3_2015_rw
create writable external table ny_taxi_s3_2015_rw
(like ny_taxi_src_gp)
LOCATION('s3://storage.yandexcloud.net/alexey-luzan-prod/s3-test/2015/ config_server=http://10.129.0.13:8553/s3.config region=ru-central1-a')
FORMAT 'csv'
distributed RANDOMLY;

-- drop external table ny_taxi_s3_2015_ro
create readable external table ny_taxi_s3_2015_ro
(like ny_taxi_s3_2015_rw)
LOCATION('s3://storage.yandexcloud.net/alexey-luzan-prod/s3-test/2015/ config_server=http://10.129.0.13:8553/s3.config region=ru-central1-a')
FORMAT 'csv';

insert into ny_taxi_s3_2015_rw
select * from ny_taxi_src_gp
where pickup_date between '2015-01-01'::date and '2015-12-31'::date;
/*
Updated Rows	 146052940
Query
	insert into ny_taxi_s3_2015_rw
	select * from ny_taxi_src_gp
	where pickup_date between '2015-01-01'::date and '2015-12-31'::date
Start time	Fri Aug 04 00:29:52 MSK 2023
Finish time	Fri Aug 04 00:33:56 MSK 2023
*/


-- drop external table ny_taxi_s3_2016_rw
create writable external table ny_taxi_s3_2016_rw
(like ny_taxi_src_gp)
LOCATION('s3://storage.yandexcloud.net/alexey-luzan-prod/s3-test/2016/ config_server=http://10.129.0.13:8553/s3.config region=ru-central1-a')
FORMAT 'csv'
distributed RANDOMLY;

-- drop external table ny_taxi_s3_2016_ro
create readable external table ny_taxi_s3_2016_ro
(like ny_taxi_s3_2016_rw)
LOCATION('s3://storage.yandexcloud.net/alexey-luzan-prod/s3-test/2016/ config_server=http://10.129.0.13:8553/s3.config region=ru-central1-a')
FORMAT 'csv';

insert into ny_taxi_s3_2016_rw
select * from ny_taxi_src_gp
where pickup_date between '2016-01-01'::date and '2016-12-31'::date;
/*

Updated Rows	 131145797
Query
	insert into ny_taxi_s3_2016_rw
	select * from ny_taxi_src_gp
	where pickup_date between '2016-01-01'::date and '2016-12-31'::date
Start time	Fri Aug 04 00:34:55 MSK 2023
Finish time	Fri Aug 04 00:38:37 MSK 2023
*/


-- drop external table ny_taxi_s3_2016_rw
create writable external table ny_taxi_s3_2016_rw
(like ny_taxi_src_gp)
LOCATION('s3://storage.yandexcloud.net/alexey-luzan-prod/s3-test/2016/ config_server=http://10.129.0.13:8553/s3.config region=ru-central1-a')
FORMAT 'csv'
distributed RANDOMLY;

-- drop external table ny_taxi_s3_2016_ro
create readable external table ny_taxi_s3_2016_ro
(like ny_taxi_s3_2016_rw)
LOCATION('s3://storage.yandexcloud.net/alexey-luzan-prod/s3-test/2016/ config_server=http://10.129.0.13:8553/s3.config region=ru-central1-a')
FORMAT 'csv';

insert into ny_taxi_s3_2016_rw
select * from ny_taxi_src_gp
where pickup_date between '2016-01-01'::date and '2016-12-31'::date;
/*
Updated Rows	 131145797
Query
	insert into ny_taxi_s3_2016_rw
	select * from ny_taxi_src_gp
	where pickup_date between '2016-01-01'::date and '2016-12-31'::date
Start time	Fri Aug 04 00:34:55 MSK 2023
Finish time	Fri Aug 04 00:38:37 MSK 2023
*/


-- drop external table ny_taxi_s3_2017_rw
create writable external table ny_taxi_s3_2017_rw
(like ny_taxi_src_gp)
LOCATION('s3://storage.yandexcloud.net/alexey-luzan-prod/s3-test/2017/ config_server=http://10.129.0.13:8553/s3.config region=ru-central1-a')
FORMAT 'csv'
distributed RANDOMLY;

-- drop external table ny_taxi_s3_2017_ro
create readable external table ny_taxi_s3_2017_ro
(like ny_taxi_s3_2017_rw)
LOCATION('s3://storage.yandexcloud.net/alexey-luzan-prod/s3-test/2017/ config_server=http://10.129.0.13:8553/s3.config region=ru-central1-a')
FORMAT 'csv';

insert into ny_taxi_s3_2017_rw
select * from ny_taxi_src_gp
where pickup_date between '2017-01-01'::date and '2017-12-31'::date;
/*

Updated Rows 113510112
Query
	insert into ny_taxi_s3_2017_rw
	select * from ny_taxi_src_gp
	where pickup_date between '2017-01-01'::date and '2017-12-31'::date
Start time	Fri Aug 04 00:47:03 MSK 2023
Finish time	Fri Aug 04 00:50:12 MSK 2023
*/


-- drop external table ny_taxi_s3_2018_rw
create writable external table ny_taxi_s3_2018_rw
(like ny_taxi_src_gp)
LOCATION('s3://storage.yandexcloud.net/alexey-luzan-prod/s3-test/2018/ config_server=http://10.129.0.13:8553/s3.config region=ru-central1-a')
FORMAT 'csv'
distributed RANDOMLY;

-- drop external table ny_taxi_s3_2018_ro
create readable external table ny_taxi_s3_2018_ro
(like ny_taxi_s3_2018_rw)
LOCATION('s3://storage.yandexcloud.net/alexey-luzan-prod/s3-test/2018/ config_server=http://10.129.0.13:8553/s3.config region=ru-central1-a')
FORMAT 'csv';

insert into ny_taxi_s3_2018_rw
select * from ny_taxi_src_gp
where pickup_date between '2018-01-01'::date and '2018-12-31'::date;
/*
Updated Rows 102876343
Query
	insert into ny_taxi_s3_2018_rw
	select * from ny_taxi_src_gp
	where pickup_date between '2018-01-01'::date and '2018-12-31'::date
Start time	Fri Aug 04 10:45:26 MSK 2023
Finish time	Fri Aug 04 10:47:38 MSK 2023
*/


-- drop external table ny_taxi_s3_2019_rw
create writable external table ny_taxi_s3_2019_rw
(like ny_taxi_src_gp)
LOCATION('s3://storage.yandexcloud.net/alexey-luzan-prod/s3-test/2019/ config_server=http://10.129.0.13:8553/s3.config region=ru-central1-a')
FORMAT 'csv'
distributed RANDOMLY;

-- drop external table ny_taxi_s3_2019_ro
create readable external table ny_taxi_s3_2019_ro
(like ny_taxi_s3_2019_rw)
LOCATION('s3://storage.yandexcloud.net/alexey-luzan-prod/s3-test/2019/ config_server=http://10.129.0.13:8553/s3.config region=ru-central1-a')
FORMAT 'csv';

insert into ny_taxi_s3_2019_rw
select * from ny_taxi_src_gp
where pickup_date between '2019-01-01'::date and '2019-12-31'::date;
/*
Updated Rows 169173050
Query
	insert into ny_taxi_s3_2019_rw
	select * from ny_taxi_src_gp
	where pickup_date between '2019-01-01'::date and '2019-12-31'::date
Start time	Fri Aug 04 11:42:24 MSK 2023
Finish time	Fri Aug 04 11:47:04 MSK 2023
*/


-- drop external table ny_taxi_s3_2020_rw
create writable external table ny_taxi_s3_2020_rw
(like ny_taxi_src_gp)
LOCATION('s3://storage.yandexcloud.net/alexey-luzan-prod/s3-test/2020/ config_server=http://10.129.0.13:8553/s3.config region=ru-central1-a')
FORMAT 'csv'
distributed RANDOMLY;

-- drop external table ny_taxi_s3_2020_ro
create readable external table ny_taxi_s3_2020_ro
(like ny_taxi_s3_2020_rw)
LOCATION('s3://storage.yandexcloud.net/alexey-luzan-prod/s3-test/2020/ config_server=http://10.129.0.13:8553/s3.config region=ru-central1-a')
FORMAT 'csv';

insert into ny_taxi_s3_2020_rw
select * from ny_taxi_src_gp
where pickup_date between '2020-01-01'::date and '2020-12-31'::date;
/*
Updated Rows	 24700472
Query
	insert into ny_taxi_s3_2020_rw
	select * from ny_taxi_src_gp
	where pickup_date between '2020-01-01'::date and '2020-12-31'::date
Start time	Fri Aug 04 11:53:59 MSK 2023
Finish time	Fri Aug 04 11:54:40 MSK 2023
*/


-- drop external table ny_taxi_s3_2021_rw
create writable external table ny_taxi_s3_2021_rw
(like ny_taxi_src_gp)
LOCATION('s3://storage.yandexcloud.net/alexey-luzan-prod/s3-test/2021/ config_server=http://10.129.0.13:8553/s3.config region=ru-central1-a')
FORMAT 'csv'
distributed RANDOMLY;

-- drop external table ny_taxi_s3_2021_ro
create readable external table ny_taxi_s3_2021_ro
(like ny_taxi_s3_2021_rw)
LOCATION('s3://storage.yandexcloud.net/alexey-luzan-prod/s3-test/2021/ config_server=http://10.129.0.13:8553/s3.config region=ru-central1-a')
FORMAT 'csv';

insert into ny_taxi_s3_2021_rw
select * from ny_taxi_src_gp
where pickup_date between '2021-01-01'::date and '2021-12-31'::date;
/*
Updated Rows	 30897779
Query
	insert into ny_taxi_s3_2021_rw
	select * from ny_taxi_src_gp
	where pickup_date between '2021-01-01'::date and '2021-12-31'::date
Start time	Fri Aug 04 11:55:53 MSK 2023
Finish time	Fri Aug 04 11:56:47 MSK 2023
*/

-- drop external table ny_taxi_s3_2022_rw
create writable external table ny_taxi_s3_2022_rw
(like ny_taxi_src_gp)
LOCATION('s3://storage.yandexcloud.net/alexey-luzan-prod/s3-test/2022/ config_server=http://10.129.0.13:8553/s3.config region=ru-central1-a')
FORMAT 'csv'
distributed RANDOMLY;

-- drop external table ny_taxi_s3_2022_ro
create readable external table ny_taxi_s3_2022_ro
(like ny_taxi_s3_2022_rw)
LOCATION('s3://storage.yandexcloud.net/alexey-luzan-prod/s3-test/2022/ config_server=http://10.129.0.13:8553/s3.config region=ru-central1-a')
FORMAT 'csv';

insert into ny_taxi_s3_2022_rw
select * from ny_taxi_src_gp
where pickup_date between '2022-01-01'::date and '2022-12-31'::date;
/*
Updated Rows	 39652084
Query
	insert into ny_taxi_s3_2022_rw
	select * from ny_taxi_src_gp
	where pickup_date between '2022-01-01'::date and '2022-12-31'::date
Start time	Fri Aug 04 12:09:03 MSK 2023
Finish time	Fri Aug 04 12:10:13 MSK 2023
*/

create view ny_taxi_s3_2013_2022 as
select * from ny_taxi_s3_2013_ro
where pickup_date between '2013-01-01'::date and '2013-12-31'::date
union all
select * from ny_taxi_s3_2014_ro
where pickup_date between '2014-01-01'::date and '2014-12-31'::date
union all
select * from ny_taxi_s3_2015_ro
where pickup_date between '2015-01-01'::date and '2015-12-31'::date
union all
select * from ny_taxi_s3_2016_ro
where pickup_date between '2016-01-01'::date and '2016-12-31'::date
union all
select * from ny_taxi_s3_2017_ro
where pickup_date between '2017-01-01'::date and '2017-12-31'::date
union all
select * from ny_taxi_s3_2018_ro
where pickup_date between '2018-01-01'::date and '2018-12-31'::date
union all
select * from ny_taxi_s3_2019_ro
where pickup_date between '2019-01-01'::date and '2019-12-31'::date
union all
select * from ny_taxi_s3_2020_ro
where pickup_date between '2020-01-01'::date and '2020-12-31'::date
union all
select * from ny_taxi_s3_2021_ro
where pickup_date between '2021-01-01'::date and '2021-12-31'::date
union all
select * from ny_taxi_s3_2022_ro
where pickup_date between '2022-01-01'::date and '2022-12-31'::date
;