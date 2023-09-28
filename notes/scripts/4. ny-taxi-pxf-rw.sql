-- 2013
-- drop external table ny_taxi_pxf_2013_rw
create writable external table ny_taxi_pxf_2013_rw
(like ny_taxi_src_gp)
LOCATION (
	'pxf://alexey-luzan-prod/ny-taxi-pxf-rw/2013?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net&PARQUET_VERSION=v2&COMPRESSION_CODEC=snappy')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export')
distributed RANDOMLY;

-- drop external table ny_taxi_pxf_2013_ro
create readable external table ny_taxi_pxf_2013_ro
(like ny_taxi_pxf_2013_rw)
LOCATION (
	'pxf://alexey-luzan-prod/ny-taxi-pxf-rw/2013?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net&PARQUET_VERSION=v2&COMPRESSION_CODEC=snappy')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

insert into ny_taxi_pxf_2013_rw
select * from ny_taxi_src_gp
where pickup_date between '2013-01-01'::date and '2013-12-31'::date;
-- Updated Rows	 171690930

-- 2014
-- drop external table ny_taxi_pxf_2014_rw
create writable external table ny_taxi_pxf_2014_rw
(like ny_taxi_src_gp)
LOCATION (
	'pxf://alexey-luzan-prod/ny-taxi-pxf-rw/2014?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net&PARQUET_VERSION=v2&COMPRESSION_CODEC=snappy')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export')
distributed RANDOMLY;

-- drop external table ny_taxi_pxf_2014_ro
create readable external table ny_taxi_pxf_2014_ro
(like ny_taxi_pxf_2014_rw)
LOCATION (
	'pxf://alexey-luzan-prod/ny-taxi-pxf-rw/2014?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net&PARQUET_VERSION=v2&COMPRESSION_CODEC=snappy')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

insert into ny_taxi_pxf_2014_rw
select * from ny_taxi_src_gp
where pickup_date between '2014-01-01'::date and '2014-12-31'::date;
-- Updated Rows	165496715

-- 2015
-- drop external table ny_taxi_pxf_2015_rw
create writable external table ny_taxi_pxf_2015_rw
(like ny_taxi_src_gp)
LOCATION (
	'pxf://alexey-luzan-prod/ny-taxi-pxf-rw/2015?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net&PARQUET_VERSION=v2&COMPRESSION_CODEC=snappy')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export')
distributed RANDOMLY;

-- drop external table ny_taxi_pxf_2015_ro
create readable external table ny_taxi_pxf_2015_ro
(like ny_taxi_pxf_2015_rw)
LOCATION (
	'pxf://alexey-luzan-prod/ny-taxi-pxf-rw/2015?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net&PARQUET_VERSION=v2&COMPRESSION_CODEC=snappy')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

insert into ny_taxi_pxf_2015_rw
select * from ny_taxi_src_gp
where pickup_date between '2015-01-01'::date and '2015-12-31'::date;
-- Updated Rows	146052939

-- 2016
-- drop external table ny_taxi_pxf_2016_rw
create writable external table ny_taxi_pxf_2016_rw
(like ny_taxi_src_gp)
LOCATION (
	'pxf://alexey-luzan-prod/ny-taxi-pxf-rw/2016?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net&PARQUET_VERSION=v2&COMPRESSION_CODEC=snappy')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export')
distributed RANDOMLY;

-- drop external table ny_taxi_pxf_2016_ro
create readable external table ny_taxi_pxf_2016_ro
(like ny_taxi_pxf_2016_rw)
LOCATION (
	'pxf://alexey-luzan-prod/ny-taxi-pxf-rw/2016?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net&PARQUET_VERSION=v2&COMPRESSION_CODEC=snappy')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

insert into ny_taxi_pxf_2016_rw
select * from ny_taxi_src_gp
where pickup_date between '2016-01-01'::date and '2016-12-31'::date;
-- Updated Rows	131145797

-- 2017
-- drop external table ny_taxi_pxf_2017_rw
create writable external table ny_taxi_pxf_2017_rw
(like ny_taxi_src_gp)
LOCATION (
	'pxf://alexey-luzan-prod/ny-taxi-pxf-rw/2017?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net&PARQUET_VERSION=v2&COMPRESSION_CODEC=snappy')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export')
distributed RANDOMLY;

-- drop external table ny_taxi_pxf_2017_ro
create readable external table ny_taxi_pxf_2017_ro
(like ny_taxi_pxf_2017_rw)
LOCATION (
	'pxf://alexey-luzan-prod/ny-taxi-pxf-rw/2017?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net&PARQUET_VERSION=v2&COMPRESSION_CODEC=snappy')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

insert into ny_taxi_pxf_2017_rw
select * from ny_taxi_src_gp
where pickup_date between '2017-01-01'::date and '2017-12-31'::date;
-- Updated Rows	113510112

-- 2018
-- drop external table ny_taxi_pxf_2018_rw
create writable external table ny_taxi_pxf_2018_rw
(like ny_taxi_src_gp)
LOCATION (
	'pxf://alexey-luzan-prod/ny-taxi-pxf-rw/2018?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net&PARQUET_VERSION=v2&COMPRESSION_CODEC=snappy')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export')
distributed RANDOMLY;

-- drop external table ny_taxi_pxf_2018_ro
create readable external table ny_taxi_pxf_2018_ro
(like ny_taxi_pxf_2018_rw)
LOCATION (
	'pxf://alexey-luzan-prod/ny-taxi-pxf-rw/2018?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net&PARQUET_VERSION=v2&COMPRESSION_CODEC=snappy')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

insert into ny_taxi_pxf_2018_rw
select * from ny_taxi_src_gp
where pickup_date between '2018-01-01'::date and '2018-12-31'::date;
-- Updated Rows	102876119

-- 2019
-- drop external table ny_taxi_pxf_2019_rw
create writable external table ny_taxi_pxf_2019_rw
(like ny_taxi_src_gp)
LOCATION (
	'pxf://alexey-luzan-prod/ny-taxi-pxf-rw/2019?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net&PARQUET_VERSION=v2&COMPRESSION_CODEC=snappy')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export')
distributed RANDOMLY;

-- drop external table ny_taxi_pxf_2019_ro
create readable external table ny_taxi_pxf_2019_ro
(like ny_taxi_pxf_2019_rw)
LOCATION (
	'pxf://alexey-luzan-prod/ny-taxi-pxf-rw/2019?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net&PARQUET_VERSION=v2&COMPRESSION_CODEC=snappy')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

insert into ny_taxi_pxf_2019_rw
select * from ny_taxi_src_gp
where pickup_date between '2019-01-01'::date and '2019-12-31'::date;
-- Updated Rows 84603197

-- 2020
-- drop external table ny_taxi_pxf_2020_rw
create writable external table ny_taxi_pxf_2020_rw
(like ny_taxi_src_gp)
LOCATION (
	'pxf://alexey-luzan-prod/ny-taxi-pxf-rw/2020?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net&PARQUET_VERSION=v2&COMPRESSION_CODEC=snappy')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export')
distributed RANDOMLY;

-- drop external table ny_taxi_pxf_2020_ro
create readable external table ny_taxi_pxf_2020_ro
(like ny_taxi_pxf_2020_rw)
LOCATION (
	'pxf://alexey-luzan-prod/ny-taxi-pxf-rw/2020?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net&PARQUET_VERSION=v2&COMPRESSION_CODEC=snappy')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

insert into ny_taxi_pxf_2020_rw
select * from ny_taxi_src_gp
where pickup_date between '2020-01-01'::date and '2020-12-31'::date;
-- Updated Rows 24672753

-- 2021
-- drop external table ny_taxi_pxf_2021_rw
create writable external table ny_taxi_pxf_2021_rw
(like ny_taxi_src_gp)
LOCATION (
	'pxf://alexey-luzan-prod/ny-taxi-pxf-rw/2021?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net&PARQUET_VERSION=v2&COMPRESSION_CODEC=snappy')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export')
distributed RANDOMLY;

-- drop external table ny_taxi_pxf_2021_ro
create readable external table ny_taxi_pxf_2021_ro
(like ny_taxi_pxf_2021_rw)
LOCATION (
	'pxf://alexey-luzan-prod/ny-taxi-pxf-rw/2021?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net&PARQUET_VERSION=v2&COMPRESSION_CODEC=snappy')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

insert into ny_taxi_pxf_2021_rw
select * from ny_taxi_src_gp
where pickup_date between '2021-01-01'::date and '2021-12-31'::date;
-- Updated Rows 30897779

-- 2022
-- drop external table ny_taxi_pxf_2022_rw
create writable external table ny_taxi_pxf_2022_rw
(like ny_taxi_src_gp)
LOCATION (
	'pxf://alexey-luzan-prod/ny-taxi-pxf-rw/2022?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net&PARQUET_VERSION=v2&COMPRESSION_CODEC=snappy')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export')
distributed RANDOMLY;

-- drop external table ny_taxi_pxf_2022_ro
create readable external table ny_taxi_pxf_2022_ro
(like ny_taxi_pxf_2022_rw)
LOCATION (
	'pxf://alexey-luzan-prod/ny-taxi-pxf-rw/2022?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net&PARQUET_VERSION=v2&COMPRESSION_CODEC=snappy')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

insert into ny_taxi_pxf_2022_rw
select * from ny_taxi_src_gp
where pickup_date between '2022-01-01'::date and '2022-12-31'::date;
-- Updated Rows 39652084

create view ny_taxi_pxf_2013_2022 as
select * from ny_taxi_pxf_2013_ro
where pickup_date between '2013-01-01'::date and '2013-12-31'::date
union all
select * from ny_taxi_pxf_2014_ro
where pickup_date between '2014-01-01'::date and '2014-12-31'::date
union all
select * from ny_taxi_pxf_2015_ro
where pickup_date between '2015-01-01'::date and '2015-12-31'::date
union all
select * from ny_taxi_pxf_2016_ro
where pickup_date between '2016-01-01'::date and '2016-12-31'::date
union all
select * from ny_taxi_pxf_2017_ro
where pickup_date between '2017-01-01'::date and '2017-12-31'::date
union all
select * from ny_taxi_pxf_2018_ro
where pickup_date between '2018-01-01'::date and '2018-12-31'::date
union all
select * from ny_taxi_pxf_2019_ro
where pickup_date between '2019-01-01'::date and '2019-12-31'::date
union all
select * from ny_taxi_pxf_2020_ro
where pickup_date between '2020-01-01'::date and '2020-12-31'::date
union all
select * from ny_taxi_pxf_2021_ro
where pickup_date between '2021-01-01'::date and '2021-12-31'::date
union all
select * from ny_taxi_pxf_2022_ro
where pickup_date between '2022-01-01'::date and '2022-12-31'::date
;