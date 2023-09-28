/* 

select * from ny_taxi_zones_src_pxf 

*/ 

-- drop external table ny_taxi_zones_src_pxf

create external table ny_taxi_zones_src_pxf
(
LocationID SMALLINT,
Borough varchar(20),
Zone varchar(50),
service_zone varchar(50)
)
location ('pxf://alexey-luzan-prod/ny-taxi-zones/?PROFILE=s3:text&SKIP_HEADER_COUNT=1&accesskey=<s3-access-key>&secretkey=<s3-secret-key>&endpoint=storage.yandexcloud.net')
ON ALL
FORMAT 'CSV' (delimiter ',');