-- drop TABLE ny_taxi_src_gp

CREATE TABLE ny_taxi_src_gp
(
vendorid int8 NULL,
pickup_date date NULL,
tpep_pickup_dttm timestamp(0) NULL,
tpep_dropoff_dttm timestamp(0) NULL,
passenger_count bigint NULL,
trip_distance float8 NULL,
ratecodeid float8 NULL,
store_and_fwd_flag bpchar(1) NULL,
pulocationid int8 NULL,
dolocationid int8 NULL,
payment_type int8 NULL,
fare_amount float8 NULL,
extra float8 NULL,
mta_tax float8 NULL,
tip_amount float8 NULL,
tolls_amount float8 NULL,
improvement_surcharge float8 NULL,
total_amount float8 NULL,
congestion_surcharge float8 NULL,
airport_fee float8 NULL
)
with
(
appendoptimized=true,
orientation=column,
compresstype=ZLIB,
compresslevel=6
)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE(pickup_date) 
(
	START (date '2013-01-01') INCLUSIVE
	END (date '2025-01-01') EXCLUSIVE
	EVERY (INTERVAL '1 year'),
	default partition other
);

insert into ny_taxi_src_gp
(
vendorid, pickup_date, tpep_pickup_dttm, tpep_dropoff_dttm, passenger_count, trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount --, congestion_surcharge, airport_fee
)
select
vendorid,
to_timestamp(tpep_pickup_datetime/1000000)::date as pickup_date,
to_timestamp(tpep_pickup_datetime/1000000) as tpep_pickup_dttm,
to_timestamp(tpep_dropoff_datetime/1000000) as tpep_dropoff_dttm,
passenger_count, trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount
--, cast(coalesce(congestion_surcharge,0) as float8)
--, cast(airport_fee as float8)
FROM public.ny_taxi_src_pxf_2013;


insert into ny_taxi_src_gp
(
vendorid, pickup_date, tpep_pickup_dttm, tpep_dropoff_dttm, passenger_count, trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount
--, improvement_surcharge
, total_amount
--, congestion_surcharge, airport_fee
)
select
vendorid,
to_timestamp(tpep_pickup_datetime/1000000)::date as pickup_date,
to_timestamp(tpep_pickup_datetime/1000000) as tpep_pickup_dttm,
to_timestamp(tpep_dropoff_datetime/1000000) as tpep_dropoff_dttm,
passenger_count, trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount
-- , improvement_surcharge
, total_amount
--, cast(coalesce(congestion_surcharge,0) as float8)
--, cast(airport_fee as float8)
FROM public.ny_taxi_src_pxf_2014;


insert into ny_taxi_src_gp
(
vendorid, pickup_date, tpep_pickup_dttm, tpep_dropoff_dttm, passenger_count, trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount --, congestion_surcharge, airport_fee
)
select
vendorid,
to_timestamp(tpep_pickup_datetime/1000000)::date as pickup_date,
to_timestamp(tpep_pickup_datetime/1000000) as tpep_pickup_dttm,
to_timestamp(tpep_dropoff_datetime/1000000) as tpep_dropoff_dttm,
passenger_count, trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount
--, cast(coalesce(congestion_surcharge,0) as float8)
--, cast(airport_fee as float8)
FROM public.ny_taxi_src_pxf_2015;


insert into ny_taxi_src_gp
(
vendorid, pickup_date, tpep_pickup_dttm, tpep_dropoff_dttm, passenger_count, trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount --, congestion_surcharge, airport_fee
)
select
vendorid,
to_timestamp(tpep_pickup_datetime/1000000)::date as pickup_date,
to_timestamp(tpep_pickup_datetime/1000000) as tpep_pickup_dttm,
to_timestamp(tpep_dropoff_datetime/1000000) as tpep_dropoff_dttm,
passenger_count, trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount
--, cast(coalesce(congestion_surcharge,0) as float8)
--, cast(airport_fee as float8)
FROM public.ny_taxi_src_pxf_2016;

insert into ny_taxi_src_gp
(
vendorid, pickup_date, tpep_pickup_dttm, tpep_dropoff_dttm, passenger_count, trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount --, congestion_surcharge, airport_fee
)
select
vendorid,
to_timestamp(tpep_pickup_datetime/1000000)::date as pickup_date,
to_timestamp(tpep_pickup_datetime/1000000) as tpep_pickup_dttm,
to_timestamp(tpep_dropoff_datetime/1000000) as tpep_dropoff_dttm,
passenger_count, trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount
--, cast(coalesce(congestion_surcharge,0) as float8)
--, cast(airport_fee as float8)
FROM public.ny_taxi_src_pxf_2017;

insert into ny_taxi_src_gp
(
vendorid, pickup_date, tpep_pickup_dttm, tpep_dropoff_dttm
--, passenger_count
, trip_distance
--, ratecodeid
, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount
--, improvement_surcharge
, total_amount --, congestion_surcharge, airport_fee
)
select
vendorid,
to_timestamp(tpep_pickup_datetime/1000000)::date as pickup_date,
to_timestamp(tpep_pickup_datetime/1000000) as tpep_pickup_dttm,
to_timestamp(tpep_dropoff_datetime/1000000) as tpep_dropoff_dttm
-- , passenger_count
, trip_distance
--, ratecodeid
, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount
--, improvement_surcharge
, total_amount
--, cast(coalesce(congestion_surcharge,0) as float8)
--, cast(airport_fee as float8)
FROM public.ny_taxi_src_pxf_2018;

insert into ny_taxi_src_gp
(
vendorid, pickup_date, tpep_pickup_dttm, tpep_dropoff_dttm, passenger_count, trip_distance
, ratecodeid
, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount --, congestion_surcharge, airport_fee
)
select
vendorid,
to_timestamp(tpep_pickup_datetime/1000000)::date as pickup_date,
to_timestamp(tpep_pickup_datetime/1000000) as tpep_pickup_dttm,
to_timestamp(tpep_dropoff_datetime/1000000) as tpep_dropoff_dttm,
passenger_count::bigint, trip_distance
, ratecodeid::float8
, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount
--, cast(coalesce(congestion_surcharge,0) as float8)
--, cast(airport_fee as float8)
FROM public.ny_taxi_src_pxf_2019;

insert into ny_taxi_src_gp
(
vendorid, pickup_date, tpep_pickup_dttm, tpep_dropoff_dttm, passenger_count, trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount --, congestion_surcharge, airport_fee
)
select
vendorid,
to_timestamp(tpep_pickup_datetime/1000000)::date as pickup_date,
to_timestamp(tpep_pickup_datetime/1000000) as tpep_pickup_dttm,
to_timestamp(tpep_dropoff_datetime/1000000) as tpep_dropoff_dttm,
passenger_count::bigint, trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount
--, cast(coalesce(congestion_surcharge,0) as float8)
--, cast(airport_fee as float8)
FROM public.ny_taxi_src_pxf_2020;



insert into ny_taxi_src_gp
(
vendorid, pickup_date, tpep_pickup_dttm, tpep_dropoff_dttm, passenger_count, trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount --, congestion_surcharge, airport_fee
)
select
vendorid,
to_timestamp(tpep_pickup_datetime/1000000)::date as pickup_date,
to_timestamp(tpep_pickup_datetime/1000000) as tpep_pickup_dttm,
to_timestamp(tpep_dropoff_datetime/1000000) as tpep_dropoff_dttm,
passenger_count, trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount
--, cast(coalesce(congestion_surcharge,0) as float8)
--, cast(airport_fee as float8)
FROM public.ny_taxi_src_pxf_2021;


insert into ny_taxi_src_gp
(
vendorid, pickup_date, tpep_pickup_dttm, tpep_dropoff_dttm, passenger_count, trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount --, congestion_surcharge, airport_fee
)
select
vendorid,
to_timestamp(tpep_pickup_datetime/1000000)::date as pickup_date,
to_timestamp(tpep_pickup_datetime/1000000) as tpep_pickup_dttm,
to_timestamp(tpep_dropoff_datetime/1000000) as tpep_dropoff_dttm,
passenger_count, trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount
--, cast(coalesce(congestion_surcharge,0) as float8)
--, cast(airport_fee as float8)
FROM public.ny_taxi_src_pxf_2022;


insert into ny_taxi_src_gp
(
vendorid, pickup_date, tpep_pickup_dttm, tpep_dropoff_dttm, passenger_count, trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount --, congestion_surcharge, airport_fee
)
select
vendorid,
to_timestamp(tpep_pickup_datetime/1000000)::date as pickup_date,
to_timestamp(tpep_pickup_datetime/1000000) as tpep_pickup_dttm,
to_timestamp(tpep_dropoff_datetime/1000000) as tpep_dropoff_dttm,
passenger_count::bigint, trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount
--, cast(coalesce(congestion_surcharge,0) as float8)
--, cast(airport_fee as float8)
FROM public.ny_taxi_src_pxf_2023;


vacuum 
analyze ny_taxi_src_gp;


select count(1) from ny_taxi_src_gp ntsg 