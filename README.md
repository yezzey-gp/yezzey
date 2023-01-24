## Yezzey

Yezzey is greenplum extension, which makes data offloading in Greenplum easy.

Yezzey extension defines API for creating data offloading policies and attach those policies to tables.

Data offloading means physical move of relation data to external storage, namely S3.

--- some more info here (TBD)

Conditions:

- no generic wal availabale in gp6 (pg 9.4)
- no custom access method in pg9.4
- no custom wal redo routines

This means we need to

# Design goals:

Greenplum compatibility

Avoid binary incompatibily, meaning to avoid custom WAL record, which will be unredoable for vanil gp

Try to avoid custom relation forks, use of which need to be somehow WAL-logged or handled separately, meaning additional backup/restore complexity and corner cases to handle.

# API

Yezzey defines custom smgr for AO/AOCS related storage operations

### Read-write operations

Table reading logic is the following:

Case: AO/AOCS file segments being read throught yezzey SMGR. Filename to be accessed is in form

base/\<dboid\>/\<tableoid\>.\<segnum\>


Read/write logic in GP with AO/AOCS tables works in following way:

* In case of read operation,
* Open AO/AOCS segment file.
*
* Set read/write offset to either 0 or $logicalEof
* Read/Write X bytes
* Close file


So, our read logic will be following:

1) Check if base/\<dboid\>/\<tableoid\>.<segnum> present locally. If yes, this means table (and this segment file) was not (yet) offloaded to external storage. So, process normally.
2) If not, try to search for file with prefix segment<gpsegment>/base/<dboid>/<tableoid>.<segnum>.<current_read_offset>* with highest epoch number is external storage (S3). Is there is,
   read them in lexicographically ascending order. Sum of sizes of external files should be >= than logical EOF (which can be found in pg_aoseg.pg_aoseg_<tableoid> table)
3) Read this file, while not exhausted
4) If any failure, there is probably a corruption by to some unknown bugs in implementation.

### Offloading API and implementation

Algo of AO/AOCS table offloading:

1) Lock AO/AOCS relation in pg_class in exclusive mode. This prevents other concurrent sessions to read or write anything from this table.
2) Write all table segments files to s3, one by one. Write them with name segment<gpsegment>/base/<dboid>/<tableoid>.<segnum>.0 (last number means that zero is this file logical eof start)
3) After that add relation file nodes to current transaction on commit pending deletion list.

Write to already offloaeded AO/AOCS segment logic is following:

1) For each AO/AOCS segment we firstly resolve resolve highest epoch in which this changes are made. This is last number from lexicographically largest segment<gpsegment>/base/\<dboid\>/\<tableoid\>.\<segnum\>.0.* file. Let in be Y
2) Write new file with name  segment<gpsegment>/base/\<dboid\>/\<tableoid\>.\<segnum\>.\<current_write_offset=logical_end_of_file\>.Y
3) success

```


      /------\                   Cloud (external storage) e.g. S3
     |\______/|                /-------\
     |  GP    |               (         )
     | segment|     ----->     \vvvvvvv/
     |\______/|
     |        |
     |        |
      \______/
```



## end



do not read this, this trash will be moved to separate doc/test files and explaned fully later

problems:

pg_aoseg.pg_aoseg_\<tableoid\> cannot be locked:

src/include/catalog/pg_class.h:172:#define		  RELKIND_AOSEGMENTS	  'o'		/* AO segment files and eof's */

LockRelationAppendOnlySegmentFile -> LOCKACQUIRE_ALREADY_HELD

SELECT seg.aooid, md5(seg.aotablefqn), 'pg_aoseg.' || quote_ident(aoseg_c.relname) AS aosegtablefqn,
seg.relfilenode, seg.reltablespace, seg.relstorage, seg.relnatts
FROM pg_class aoseg_c
JOIN (
SELECT pg_ao.relid AS aooid, pg_ao.segrelid,
aotables.aotablefqn, aotables.relstorage,
aotables.relnatts, aotables.relfilenode, aotables.reltablespace
FROM pg_appendonly pg_ao
JOIN (
SELECT c.oid, quote_ident(n.nspname)|| '.' || quote_ident(c.relname) AS aotablefqn,
c.relstorage, c.relnatts, c.relfilenode, c.reltablespace
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE relstorage IN ( 'ao', 'co' ) AND relpersistence='p'
) aotables ON pg_ao.relid = aotables.oid
) seg ON aoseg_c.oid = seg.segrelid;

to install this:

include yezzey into shared_preload_libraries
modify your config:

yezzey.storage_prefix = 'segment{{ content_id }}/'

restart cluster

source gpAux/gpdemo/gpdemo-env.sh
source /usr/local/gpdb/greenplum_path.sh
export GPHOME=/usr/local/gpdb/
export PATH=$PATH:/usr/local/gpdb/bin

make destroy-demo-cluster && make create-demo-cluster
gpconfig -c shared_preload_libraries -v yezzey

gpstop -a -i && gpstart -a

create extension yezzey;
create table aocst(i int, j int, k int) with (appendonly=true, orientation=column);
insert into aocst(k, j) select * from (select * from generate_series(1, 100)) a join (select * from generate_series(1, 100)) b on true;

insert into aocst(k, i) select * from (select * from generate_series(1, 100)) a join (select * from generate_series(1, 100)) b on true;
insert into aocst(k, i) select * from (select * from generate_series(1, 100)) a join (select * from generate_series(1, 100)) b on true;
insert into aocst(i, j) select * from (select * from generate_series(1, 100)) a join (select * from generate_series(1, 100)) b on true;

select count(1) from aocst;
select yezzey.offload_relation('aocst');
insert into aocst(k, j) select * from (select * from generate_series(1, 100)) a join (select * from generate_series(1, 100)) b on true;

insert into aocst(k, i) select * from (select * from generate_series(1, 100)) a join (select * from generate_series(1, 100)) b on true;
insert into aocst(k, i) select * from (select * from generate_series(1, 100)) a join (select * from generate_series(1, 100)) b on true;
insert into aocst(i, j) select * from (select * from generate_series(1, 100)) a join (select * from generate_series(1, 100)) b on true;
select count(1) from aocst;
select yezzey.offload_relation('aocst');
select count(1) from aocst;

insert into aocst(k, j) select * from (select * from generate_series(1, 100)) a join (select * from generate_series(1, 100)) b on true;

insert into aocst(k, i) select * from (select * from generate_series(1, 100)) a join (select * from generate_series(1, 100)) b on true;
insert into aocst(k, i) select * from (select * from generate_series(1, 100)) a join (select * from generate_series(1, 100)) b on true;
insert into aocst(i, j) select * from (select * from generate_series(1, 100)) a join (select * from generate_series(1, 100)) b on true;
select count(1) from aocst;
select yezzey.offload_relation('aocst');
select count(1) from aocst;

gpstop -a -i && gpstart -a

make destroy-demo-cluster && make create-demo-cluster && gpconfig -s shared_preload_libraries -v yezzey && gpstop -a -i && gpstart -a


make destroy-demo-cluster && make create-demo-cluster
gpconfig -c yezzey.storage_prefix -v 'wal-e/mdbrhqjnl6k5duk7loi2/6/segments_005'
gpconfig -c yezzey.storage_bucket -v 'loh228'
gpconfig -c yezzey.storage_config -v /home/reshke/s3test.conf
gpconfig -c shared_preload_libraries -v yezzey