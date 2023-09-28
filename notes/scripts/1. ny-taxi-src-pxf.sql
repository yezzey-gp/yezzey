

select RatecodeID, count(1) from ny_taxi_src_pxf_2018
group by 1
order by 1



-- drop external table ny_taxi_src_pxf_2013

create external table ny_taxi_src_pxf_2013
(
VendorID bigint,
tpep_pickup_datetime bigint,
tpep_dropoff_datetime bigint,
passenger_count bigint,
trip_distance FLOAT8,
RatecodeID BIGINT,
store_and_fwd_flag char(1),
PULocationID BIGINT,
DOLocationID BIGINT,
payment_type BIGINT,
fare_amount float(32),
extra float(32),
mta_tax float(32),
tip_amount float(32),
tolls_amount float(32),
improvement_surcharge float(32),
total_amount float(32),
congestion_surcharge FLOAT8
, airport_fee FLOAT8
)
location ('pxf://alexey-luzan-prod/ny-taxi/2013/*/?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net')
ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' );

-- drop external table ny_taxi_src_pxf_2014

create external table ny_taxi_src_pxf_2014
(
VendorID bigint,
tpep_pickup_datetime bigint,
tpep_dropoff_datetime bigint,
passenger_count bigint,
trip_distance FLOAT8,
RatecodeID BIGINT,
store_and_fwd_flag char(1),
PULocationID BIGINT,
DOLocationID BIGINT,
payment_type BIGINT,
fare_amount float(32),
extra float(32),
mta_tax float(32),
tip_amount float(32),
tolls_amount float(32),
improvement_surcharge float(32),
total_amount float(32),
congestion_surcharge FLOAT8
, airport_fee FLOAT8
)
location ('pxf://alexey-luzan-prod/ny-taxi/2014/*/?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net')
ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' );


-- drop external table ny_taxi_src_pxf_2015

create external table ny_taxi_src_pxf_2015
(
VendorID bigint,
tpep_pickup_datetime bigint,
tpep_dropoff_datetime bigint,
passenger_count bigint,
trip_distance FLOAT8,
RatecodeID BIGINT,
store_and_fwd_flag char(1),
PULocationID BIGINT,
DOLocationID BIGINT,
payment_type BIGINT,
fare_amount float(32),
extra float(32),
mta_tax float(32),
tip_amount float(32),
tolls_amount float(32),
improvement_surcharge float(32),
total_amount float(32),
congestion_surcharge FLOAT8
, airport_fee FLOAT8
)
location ('pxf://alexey-luzan-prod/ny-taxi/2015/*/?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net')
ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' );


-- drop external table ny_taxi_src_pxf_2016

create external table ny_taxi_src_pxf_2016
(
VendorID bigint,
tpep_pickup_datetime bigint,
tpep_dropoff_datetime bigint,
passenger_count bigint,
trip_distance FLOAT8,
RatecodeID BIGINT,
store_and_fwd_flag char(1),
PULocationID BIGINT,
DOLocationID BIGINT,
payment_type BIGINT,
fare_amount float(32),
extra float(32),
mta_tax float(32),
tip_amount float(32),
tolls_amount float(32),
improvement_surcharge float(32),
total_amount float(32),
congestion_surcharge FLOAT8
, airport_fee FLOAT8
)
location ('pxf://alexey-luzan-prod/ny-taxi/2016/*/?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net')
ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' );


-- drop external table ny_taxi_src_pxf_2017

create external table ny_taxi_src_pxf_2017
(
VendorID bigint,
tpep_pickup_datetime bigint,
tpep_dropoff_datetime bigint,
passenger_count bigint,
trip_distance FLOAT8,
RatecodeID BIGINT,
store_and_fwd_flag char(1),
PULocationID BIGINT,
DOLocationID BIGINT,
payment_type BIGINT,
fare_amount float(32),
extra float(32),
mta_tax float(32),
tip_amount float(32),
tolls_amount float(32),
improvement_surcharge float(32),
total_amount float(32),
congestion_surcharge FLOAT8
, airport_fee FLOAT8
)
location ('pxf://alexey-luzan-prod/ny-taxi/2017/*/?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net')
ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' );

-- drop external table ny_taxi_src_pxf_2018

create external table ny_taxi_src_pxf_2018
(
VendorID bigint,
tpep_pickup_datetime bigint,
tpep_dropoff_datetime bigint,
passenger_count bigint,
trip_distance FLOAT8,
RatecodeID FLOAT8,
store_and_fwd_flag char(1),
PULocationID BIGINT,
DOLocationID BIGINT,
payment_type BIGINT,
fare_amount float(32),
extra float(32),
mta_tax float(32),
tip_amount float(32),
tolls_amount float(32),
improvement_surcharge float(32),
total_amount float(32),
congestion_surcharge FLOAT8
, airport_fee FLOAT8
)
location ('pxf://alexey-luzan-prod/ny-taxi/2018/*/?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net')
ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' );

-- drop external table ny_taxi_src_pxf_2019

create external table ny_taxi_src_pxf_2019
(
VendorID bigint,
tpep_pickup_datetime bigint,
tpep_dropoff_datetime bigint,
passenger_count FLOAT8,
trip_distance FLOAT8,
RatecodeID FLOAT8,
store_and_fwd_flag char(1),
PULocationID BIGINT,
DOLocationID BIGINT,
payment_type BIGINT,
fare_amount float(32),
extra float(32),
mta_tax float(32),
tip_amount float(32),
tolls_amount float(32),
improvement_surcharge float(32),
total_amount float(32),
congestion_surcharge FLOAT8
, airport_fee FLOAT8
)
location ('pxf://alexey-luzan-prod/ny-taxi/2019/*/?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net')
ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' );


-- drop external table ny_taxi_src_pxf_2020

create external table ny_taxi_src_pxf_2020
(
VendorID bigint,
tpep_pickup_datetime bigint,
tpep_dropoff_datetime bigint,
passenger_count FLOAT8,
trip_distance FLOAT8,
RatecodeID FLOAT8,
store_and_fwd_flag char(1),
PULocationID BIGINT,
DOLocationID BIGINT,
payment_type BIGINT,
fare_amount float(32),
extra float(32),
mta_tax float(32),
tip_amount float(32),
tolls_amount float(32),
improvement_surcharge float(32),
total_amount float(32),
congestion_surcharge FLOAT8
, airport_fee FLOAT8
)
location ('pxf://alexey-luzan-prod/ny-taxi/2020/*/?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net')
ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' );


-- drop external table ny_taxi_src_pxf_2021

create external table ny_taxi_src_pxf_2021
(
VendorID bigint,
tpep_pickup_datetime bigint,
tpep_dropoff_datetime bigint,
passenger_count FLOAT8,
trip_distance FLOAT8,
RatecodeID FLOAT8,
store_and_fwd_flag char(1),
PULocationID BIGINT,
DOLocationID BIGINT,
payment_type BIGINT,
fare_amount float(32),
extra float(32),
mta_tax float(32),
tip_amount float(32),
tolls_amount float(32),
improvement_surcharge float(32),
total_amount float(32),
congestion_surcharge FLOAT8
, airport_fee FLOAT8
)
location ('pxf://alexey-luzan-prod/ny-taxi/2021/*/?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net')
ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' );

-- drop external table ny_taxi_src_pxf_2022

create external table ny_taxi_src_pxf_2022
(
VendorID bigint,
tpep_pickup_datetime bigint,
tpep_dropoff_datetime bigint,
passenger_count FLOAT8,
trip_distance FLOAT8,
RatecodeID FLOAT8,
store_and_fwd_flag char(1),
PULocationID BIGINT,
DOLocationID BIGINT,
payment_type BIGINT,
fare_amount float(32),
extra float(32),
mta_tax float(32),
tip_amount float(32),
tolls_amount float(32),
improvement_surcharge float(32),
total_amount float(32),
congestion_surcharge FLOAT8
, airport_fee FLOAT8
)
location ('pxf://alexey-luzan-prod/ny-taxi/2022/*/?PROFILE=s3:parquet&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net')
ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' );
