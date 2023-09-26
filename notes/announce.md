# Yezzey: GPDB extension for compute-storage disaggregation

Here's a simple walk-through usage guide and some benchmark statistics base on [NY Yellow Taxi](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) 2013-2022 dataset.

# Offloading data into cloud storage
Let's create a table and populate it with data.
```
CREATE TABLE ny_taxi_yezzey
(
-- Column information skipped
)
with
(
-- Doing GP business as usual
)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE(pickup_date) 
(
-- All the stuff you would typically do with Greenplum Append-Optimized table
);

insert into ny_taxi_yezzey select * from ny_taxi_src_gp;
```
For details see [scripts](https://github.com/x4m/yezzey/edit/benchmark_post/notes). Now we can instruct Greenplum to put this data into cold storage.

```
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
```
