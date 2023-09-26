-- drop TABLE ny_taxi_zones_src_gp

CREATE TABLE ny_taxi_zones_src_gp
(
LocationID SMALLINT,
Borough varchar(20),
Zone varchar(50),
service_zone varchar(50)
)
with
(
appendoptimized=true,
orientation=column,
compresstype=ZLIB,
compresslevel=6
)
DISTRIBUTED BY (LocationID)
;

insert into ny_taxi_zones_src_gp
(
LocationID, Borough, Zone, service_zone
)
select
LocationID, Borough, Zone, service_zone
FROM public.ny_taxi_zones_src_pxf;


vacuum analyze ny_taxi_zones_src_gp;
