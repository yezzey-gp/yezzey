# Yezzey: GPDB extension for compute-storage disaggregation

Here's a simple walk-through usage guide and some benchmark statistics base on [NY Yellow Taxi](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) 2013-2022 dataset.

## Offloading data into cloud storage
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
See [scripts](https://github.com/x4m/yezzey/edit/benchmark_post/notes/scripts) for details. Now we can instruct Greenplum to put this data into cold storage.

```
select yezzey_define_offload_policy('ny_taxi_yezzey_1_prt_10');
select yezzey_define_offload_policy('ny_taxi_yezzey_1_prt_11');
select yezzey_define_offload_policy('ny_taxi_yezzey_1_prt_12');
select yezzey_define_offload_policy('ny_taxi_yezzey_1_prt_13');
...
```

## Benchmarking
Fact tables with trips for all engines were intentionally distributed randomly into segments (DISTRIBUTED RANDOMLY) in order to evenly distribute data and guarantee the call of a redistricting operation.

Test queries:
1. Aggregate without redistribution - ```select count(1) from <table>```;

2. Aggregate by one column ```select vendorid, count(1) from <table> group by vendorid```;

3. Aggregate by 4 columns ```select vendorid, pickup_date, passenger_count, payment_type, count(1) from <table> group by 1,2,3,4;```

4. Analytics over 1 partition ```select distinct dolocationid, max(total_amount) over (partition by dolocationid)from <table> where pickup_date between '2014-01-01'::date and '2014-12-31'::date;```

5. Analytics over 3 partitions ```select distinct dolocationid, max(total_amount) over (partition by dolocationid)from <table> where pickup_date between '2014-01-01'::date and '2016-12-31'::date;```

6. Aggregation over 3 partitions and join ```select z.locationid, count(r.vendorid), sum(r.total_amount) from ny_taxi_src_gp r left join ny_taxi_zones_src_gp z on r.pulocationid=z.locationid where r.pickup_date between '2014-01-01'::date and '2016-12-31'::date group by 1```

7. Join with reference table and aggregation over whole dataset ```select z.locationid, count(r.vendorid), sum(r.total_amount) from ny_taxi_src_gp r left join ny_taxi_zones_src_gp z on r.pulocationid=z.locationid group by 1```
