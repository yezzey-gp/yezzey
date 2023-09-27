# Yezzey: GPDB extension for compute-storage disaggregation

Here's a minimalist usage guide and some benchmark statistics based on the [NY Yellow Taxi](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) 2013-2022 dataset.

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
-- All the stuff you would normally do with the Greenplum Append-Optimised table
);

insert into ny_taxi_yezzey select * from ny_taxi_src_gp;
```
See [scripts](https://github.com/x4m/yezzey/edit/benchmark_post/notes/scripts) for details. Now we can tell Greenplum to put this data into cold storage.

TODO: Configure S3 bucket and GPG encryption keys.

```
select yezzey_define_offload_policy('ny_taxi_yezzey');
```

## Benchmarking
Fact tables containing trips for all engines were intentionally DISTRIBUTED RANDOMLY to ensure an even distribution of data and to guarantee the call of a redistribution operation.

Test queries:
1. Aggregate without redistribution - ```select count(1) from <table>```;

2. Aggregate by one column ```select vendorid, count(1) from <table> group by vendorid```;

3. Aggregate by 4 columns ```select vendorid, pickup_date, passenger_count, payment_type, count(1) from <table> group by 1,2,3,4;```

4. Analytics over 1 partition ```select distinct dolocationid, max(total_amount) over (partition by dolocationid)from <table> where pickup_date between '2014-01-01'::date and '2014-12-31'::date;```

5. Analytics over 3 partitions ```select distinct dolocationid, max(total_amount) over (partition by dolocationid)from <table> where pickup_date between '2014-01-01'::date and '2016-12-31'::date;```

6. Aggregation over 3 partitions and join ```select z.locationid, count(r.vendorid), sum(r.total_amount) from ny_taxi_src_gp r left join ny_taxi_zones_src_gp z on r.pulocationid=z.locationid where r.pickup_date between '2014-01-01'::date and '2016-12-31'::date group by 1```

7. Join with reference table and aggregation over whole dataset ```select z.locationid, count(r.vendorid), sum(r.total_amount) from ny_taxi_src_gp r left join ny_taxi_zones_src_gp z on r.pulocationid=z.locationid group by 1```

## Benchmark results

Benchmark hardware:
* Master x2 s3-c2-m8 (2 vCPU, 100% vCPU rate, 8 ГБ RAM) 20 ГБ network-ssd
* Segment hosts x2 i3-c24-m192 (24 vCPU, 100% vCPU rate, 192 ГБ RAM) 368 ГБ local-ssd
* 4 segments per host

Here's the table showing the results of running benchmark queries against tables with different storage engines. GP 1, 2, 3 columns show runs over regular Greenplum append-optimised column storage. Yezzey 1, 2, 3 show results for tables in S3-compatible Yandex Object storage. PXF columns show results for Platform Extension Framework.
| Query | GP 1 | GP 2 | GP 3 | Yezzey 1 | Yezzey 2 | Yezzey 3 | PXF 1 | PXF 2 | PXF 3 |
| ------------- | ------------- | ------------- | ------------- | ------------- | ------------- | ------------- | ------------- | ------------- | ------------- |
| 1. ```select count(1)from <table>;```|14.614s|15.973s|14.971s|20.938s|21.788s|19.429s|6m 6s|6m 6s|6m 4s|
| 2. ```select vendorid, count(1) from <table>group by vendorid;```|22.480s|20.424s|21.179s|25.671s|26.687s|27.224s|6m 34s|6m 40s|6m 35s|
| 3. ```select vendorid, pickup_date, passenger_count, payment_type,count(1) from <table>group by 1,2,3,4```|36.420s|36.126s|36.206s|51.606s|51.83s|50.88s|8m 9s|8m 30s|8m 30s|
| 4. ```select distinct dolocationid, max(total_amount) over (partition by dolocationid) from <table> where pickup_date between '2014-01-01'::date and '2014-12-31'::date```|38.729s|40.822s|37.654s|40.494s|40.197s|39.647s|1m 38s|1m 36s|1m 44s|
| 5. ```select distinct dolocationid, max(total_amount) over (partition by dolocationid) from <table> where pickup_date between '2014-01-01'::date and '2016-12-31'::date```|1m 44s|1m 48s|1m 40s|1m 47s|1m 44|1m 46s|4m 16s|4m 18s|4m 12s|
| 6. ```select z.locationid, count(r.vendorid), sum(r.total_amount) from <table> r left join ny_taxi_zones_src_gp z on r.pulocationid=z.locationid where r.pickup_date between '2014-01-01'::date and '2016-12-31'::date group by 1``` |28.818s|27.413s|28.622s|37.807s|36.25s|36.117s|3m 45s|3m 37s|3m 35s|
| 7. ```select z.locationid, count(r.vendorid), sum(r.total_amount) from <table> r left join ny_taxi_zones_src_gp z on r.pulocationid=z.locationid group by 1``` | 56.936s|55.202s|55.626s|1m 18s|1m 13s|1m 10s|8m 13s|7m 59s|8m 51s|

As you can see, the query performance of Yezzey is comparable to file system tables, while the storage price is comparable to PXF.
We also tried [external tables using S3 protocol](https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/admin_guide-external-g-s3-protocol.html) and got results similar to PXF.

## Limitations and Roadmap
1. Yezzey is compatible with [WAL-G](https://github.com/wal-g/wal-g). We want to make instant S3 offload when the table did not change much since recent backups.
2. Local caching is not implemented yet.
3. When you drop a table - it might be a part of WAL-G backup. Thus, S3 space is not immidiately released, but rather occupied until backup deletion. Probably one day this trait will be transformed into some form of time travel mechanism.
4. We want to make table sharing between clusters possible. Some data catalog service would make possible to share same table between many clusters.
5. Transforming Yezzey into access method would allow instant expand: ability to add new compute nodes without doing heavy redistribution operations(aka expand). We reffer to this idea as [Yeneid](https://github.com/yezzey-gp/yeneid).

## Conclusion
Yezzey is an open source Greenplum extension that provides data analytics with the performance of local drives and the price of S3-compatible storage.
Easies way to test out this technology is to create a cluster in [Yandex Cloud Managed Greenplum](https://cloud.yandex.com/en/services/managed-greenplum). The technology is fully open source, Yandex Cloud engineers are committed to develop Yezzey together with community.
Yezzey is based on [Postgres Extensible Storage Manager API](https://commitfest.postgresql.org/35/3216/) which is [ported](https://github.com/greenplum-db/gpdb/pull/13601) into Greenplum.
