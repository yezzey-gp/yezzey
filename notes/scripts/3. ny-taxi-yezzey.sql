-- drop TABLE ny_taxi_yezzey

CREATE TABLE ny_taxi_yezzey
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

insert into ny_taxi_yezzey select * from ny_taxi_src_gp;

vacuum analyze ny_taxi_yezzey;

-- select * from yezzey_offload_relation_status_internal('ny_taxi_yezzey_1_prt_other'::regclass::oid);

-- select 'ny_taxi_yezzey_1_prt_10'::regclass::oid


select yezzey_define_offload_policy('ny_taxi_yezzey_1_prt_10');
select yezzey_define_offload_policy('ny_taxi_yezzey_1_prt_11');
select yezzey_define_offload_policy('ny_taxi_yezzey_1_prt_12');
select yezzey_define_offload_policy('ny_taxi_yezzey_1_prt_13');
select yezzey_define_offload_policy('ny_taxi_yezzey_1_prt_2');
select yezzey_define_offload_policy('ny_taxi_yezzey_1_prt_3');
select yezzey_define_offload_policy('ny_taxi_yezzey_1_prt_4');
select yezzey_define_offload_policy('ny_taxi_yezzey_1_prt_5');
select yezzey_define_offload_policy('ny_taxi_yezzey_1_prt_6');
select yezzey_define_offload_policy('ny_taxi_yezzey_1_prt_7');
select yezzey_define_offload_policy('ny_taxi_yezzey_1_prt_8');
select yezzey_define_offload_policy('ny_taxi_yezzey_1_prt_9');
select yezzey_define_offload_policy('ny_taxi_yezzey_1_prt_other');