/* 

    To reproduce please change the next variables:

        <bucket-name> - your S3 bucket
        <s3-access-key> - your S3 access key
        <s3-secret-key> - your S3 secret key

    Note that script is relevant for Yandex Object Storage (storage.yandexcloud.net). To use with another S3 storage please change the endpoint name

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