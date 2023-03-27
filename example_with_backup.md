

## Yezzey min





# backup file reuse

```
-- create some table
create table local_table_backuped(i int, j int, k int, kk int) with(appendonly=true, orientation=column);

-- insert some data in this relation

db2=# insert into local_table_backuped select * from generate_series(1, 100) a join generate_series(1, 100) b on true join generate_series(1, 100) c on true join generate_series(1, 100) d on true;
INSERT 0 100000000
Time: 19720.754 ms

-- Use yezzey_relation_describe_external_storage_structure to list external storage files. There is no files yet.
-- we also get WARNINGs about fact, that public.local_table_backuped relation offload policy is not defines (yet)

db2=# select * from yezzey_relation_describe_external_storage_structure('local_table_backuped');
WARNING:  relation public.local_table_backuped is not in offload metadata table  (seg0 slice1 2a02:6b8:c25:1c55:0:1589:cb6d:a5e8:6000 pid=1140645)
CONTEXT:  PL/pgSQL function yezzey_relation_describe_external_storage_structure(text) line 3 at RETURN QUERY
WARNING:  relation public.local_table_backuped is not in offload metadata table  (seg2 slice1 2a02:6b8:c25:109d:0:1589:d69f:58c1:6000 pid=1117914)
CONTEXT:  PL/pgSQL function yezzey_relation_describe_external_storage_structure(text) line 3 at RETURN QUERY
WARNING:  relation public.local_table_backuped is not in offload metadata table  (seg3 slice1 2a02:6b8:c25:109d:0:1589:d69f:58c1:6001 pid=1117915)
CONTEXT:  PL/pgSQL function yezzey_relation_describe_external_storage_structure(text) line 3 at RETURN QUERY
WARNING:  relation public.local_table_backuped is not in offload metadata table  (seg1 slice1 2a02:6b8:c25:1c55:0:1589:cb6d:a5e8:6001 pid=1140644)
CONTEXT:  PL/pgSQL function yezzey_relation_describe_external_storage_structure(text) line 3 at RETURN QUERY
WARNING:  relation public.local_table_backuped is not in offload metadata table  (seg4 slice1 2a02:6b8:c25:124c:0:1589:bf3b:4e51:6000 pid=1093083)
CONTEXT:  PL/pgSQL function yezzey_relation_describe_external_storage_structure(text) line 3 at RETURN QUERY
WARNING:  relation public.local_table_backuped is not in offload metadata table  (seg5 slice1 2a02:6b8:c25:124c:0:1589:bf3b:4e51:6001 pid=1093084)
CONTEXT:  PL/pgSQL function yezzey_relation_describe_external_storage_structure(text) line 3 at RETURN QUERY
 offload_reloid | segindex | segfileindex | external_storage_filepath | local_bytes | local_commited_bytes | external_bytes
----------------+----------+--------------+---------------------------+-------------+----------------------+----------------
(0 rows)

Time: 380.055 ms

-- await for backup to run 


gpadmin@man-glhovkp9le8806ce:~$ psql db2  -c "select * from yezzey_relation_describe_external_storage_structure('local_table_backuped');"
WARNING:  relation public.local_table_backuped is not in offload metadata table  (seg0 slice1 2a02:6b8:c25:1c55:0:1589:cb6d:a5e8:6000 pid=1145473)
CONTEXT:  PL/pgSQL function yezzey_relation_describe_external_storage_structure(text) line 3 at RETURN QUERY
WARNING:  relation public.local_table_backuped is not in offload metadata table  (seg1 slice1 2a02:6b8:c25:1c55:0:1589:cb6d:a5e8:6001 pid=1145474)
CONTEXT:  PL/pgSQL function yezzey_relation_describe_external_storage_structure(text) line 3 at RETURN QUERY
WARNING:  relation public.local_table_backuped is not in offload metadata table  (seg2 slice1 2a02:6b8:c25:109d:0:1589:d69f:58c1:6000 pid=1122360)
CONTEXT:  PL/pgSQL function yezzey_relation_describe_external_storage_structure(text) line 3 at RETURN QUERY
WARNING:  relation public.local_table_backuped is not in offload metadata table  (seg3 slice1 2a02:6b8:c25:109d:0:1589:d69f:58c1:6001 pid=1122361)
CONTEXT:  PL/pgSQL function yezzey_relation_describe_external_storage_structure(text) line 3 at RETURN QUERY
WARNING:  relation public.local_table_backuped is not in offload metadata table  (seg4 slice1 2a02:6b8:c25:124c:0:1589:bf3b:4e51:6000 pid=1097803)
CONTEXT:  PL/pgSQL function yezzey_relation_describe_external_storage_structure(text) line 3 at RETURN QUERY
WARNING:  relation public.local_table_backuped is not in offload metadata table  (seg5 slice1 2a02:6b8:c25:124c:0:1589:bf3b:4e51:6001 pid=1097802)
CONTEXT:  PL/pgSQL function yezzey_relation_describe_external_storage_structure(text) line 3 at RETURN QUERY
 offload_reloid | segindex | segfileindex |                                                        external_storage_filepath                                                         | local_bytes | local_c
ommited_bytes | external_bytes
----------------+----------+--------------+------------------------------------------------------------------------------------------------------------------------------------------+-------------+--------
--------------+----------------
          90380 |        2 |            0 | wal-e/mdb8i7f8cr8ker9ec6a8/6/segments_005/seg2/basebackups_005/aosegments/1663_82385_bdd51f8ff6b3fbf3f122f263ed1ec0ae_131168_1_2_aoseg   |           0 |
            0 |       96129757
          90380 |        2 |            0 | wal-e/mdb8i7f8cr8ker9ec6a8/6/segments_005/seg2/basebackups_005/aosegments/1663_82385_bdd51f8ff6b3fbf3f122f263ed1ec0ae_131168_129_2_aoseg |           0 |
            0 |       96129757
          90380 |        2 |            0 | wal-e/mdb8i7f8cr8ker9ec6a8/6/segments_005/seg2/basebackups_005/aosegments/1663_82385_bdd51f8ff6b3fbf3f122f263ed1ec0ae_131168_257_2_aoseg |           0 |
            0 |       96129757
          90380 |        2 |            0 | wal-e/mdb8i7f8cr8ker9ec6a8/6/segments_005/seg2/basebackups_005/aosegments/1663_82385_bdd51f8ff6b3fbf3f122f263ed1ec0ae_131168_385_2_aoseg |           0 |
            0 |       96129757
          90380 |        5 |            0 | wal-e/mdb8i7f8cr8ker9ec6a8/6/segments_005/seg5/basebackups_005/aosegments/1663_82385_bdd51f8ff6b3fbf3f122f263ed1ec0ae_139360_1_2_aoseg   |           0 |
            0 |      128172803
          90380 |        5 |            0 | wal-e/mdb8i7f8cr8ker9ec6a8/6/segments_005/seg5/basebackups_005/aosegments/1663_82385_bdd51f8ff6b3fbf3f122f263ed1ec0ae_139360_129_2_aoseg |           0 |
            0 |      128172803
          90380 |        5 |            0 | wal-e/mdb8i7f8cr8ker9ec6a8/6/segments_005/seg5/basebackups_005/aosegments/1663_82385_bdd51f8ff6b3fbf3f122f263ed1ec0ae_139360_257_2_aoseg |           0 |
            0 |      128172803
          90380 |        5 |            0 | wal-e/mdb8i7f8cr8ker9ec6a8/6/segments_005/seg5/basebackups_005/aosegments/1663_82385_bdd51f8ff6b3fbf3f122f263ed1ec0ae_139360_385_2_aoseg |           0 |
            0 |      128172803
          90380 |        4 |            0 | wal-e/mdb8i7f8cr8ker9ec6a8/6/segments_005/seg4/basebackups_005/aosegments/1663_82385_bdd51f8ff6b3fbf3f122f263ed1ec0ae_139360_1_2_aoseg   |           0 |
            0 |      144194358
          90380 |        4 |            0 | wal-e/mdb8i7f8cr8ker9ec6a8/6/segments_005/seg4/basebackups_005/aosegments/1663_82385_bdd51f8ff6b3fbf3f122f263ed1ec0ae_139360_129_2_aoseg |           0 |
            0 |      144194358
          90380 |        4 |            0 | wal-e/mdb8i7f8cr8ker9ec6a8/6/segments_005/seg4/basebackups_005/aosegments/1663_82385_bdd51f8ff6b3fbf3f122f263ed1ec0ae_139360_257_2_aoseg |           0 |
            0 |      144194358
          90380 |        4 |            0 | wal-e/mdb8i7f8cr8ker9ec6a8/6/segments_005/seg4/basebackups_005/aosegments/1663_82385_bdd51f8ff6b3fbf3f122f263ed1ec0ae_139360_385_2_aoseg |           0 |
            0 |      144194358
          90380 |        3 |            0 | wal-e/mdb8i7f8cr8ker9ec6a8/6/segments_005/seg3/basebackups_005/aosegments/1663_82385_bdd51f8ff6b3fbf3f122f263ed1ec0ae_131168_1_2_aoseg   |           0 |
            0 |      120162070
          90380 |        3 |            0 | wal-e/mdb8i7f8cr8ker9ec6a8/6/segments_005/seg3/basebackups_005/aosegments/1663_82385_bdd51f8ff6b3fbf3f122f263ed1ec0ae_131168_129_2_aoseg |           0 |
            0 |      120162070
          90380 |        3 |            0 | wal-e/mdb8i7f8cr8ker9ec6a8/6/segments_005/seg3/basebackups_005/aosegments/1663_82385_bdd51f8ff6b3fbf3f122f263ed1ec0ae_131168_257_2_aoseg |           0 |
            0 |      120162070
          90380 |        3 |            0 | wal-e/mdb8i7f8cr8ker9ec6a8/6/segments_005/seg3/basebackups_005/aosegments/1663_82385_bdd51f8ff6b3fbf3f122f263ed1ec0ae_131168_385_2_aoseg |           0 |
            0 |      120162070
          90380 |        0 |            0 | wal-e/mdb8i7f8cr8ker9ec6a8/6/segments_005/seg0/basebackups_005/aosegments/1663_82385_bdd51f8ff6b3fbf3f122f263ed1ec0ae_131194_1_2_aoseg   |           0 |
            0 |      168226581
          90380 |        0 |            0 | wal-e/mdb8i7f8cr8ker9ec6a8/6/segments_005/seg0/basebackups_005/aosegments/1663_82385_bdd51f8ff6b3fbf3f122f263ed1ec0ae_131194_129_2_aoseg |           0 |
            0 |      168226581
          90380 |        0 |            0 | wal-e/mdb8i7f8cr8ker9ec6a8/6/segments_005/seg0/basebackups_005/aosegments/1663_82385_bdd51f8ff6b3fbf3f122f263ed1ec0ae_131194_257_2_aoseg |           0 |
            0 |      168226581
          90380 |        0 |            0 | wal-e/mdb8i7f8cr8ker9ec6a8/6/segments_005/seg0/basebackups_005/aosegments/1663_82385_bdd51f8ff6b3fbf3f122f263ed1ec0ae_131194_385_2_aoseg |           0 |
            0 |      168226581
          90380 |        1 |            0 | wal-e/mdb8i7f8cr8ker9ec6a8/6/segments_005/seg1/basebackups_005/aosegments/1663_82385_bdd51f8ff6b3fbf3f122f263ed1ec0ae_123002_1_2_aoseg   |           0 |
            0 |      144194358
          90380 |        1 |            0 | wal-e/mdb8i7f8cr8ker9ec6a8/6/segments_005/seg1/basebackups_005/aosegments/1663_82385_bdd51f8ff6b3fbf3f122f263ed1ec0ae_123002_129_2_aoseg |           0 |
            0 |      144194358
          90380 |        1 |            0 | wal-e/mdb8i7f8cr8ker9ec6a8/6/segments_005/seg1/basebackups_005/aosegments/1663_82385_bdd51f8ff6b3fbf3f122f263ed1ec0ae_123002_257_2_aoseg |           0 |
            0 |      144194358
          90380 |        1 |            0 | wal-e/mdb8i7f8cr8ker9ec6a8/6/segments_005/seg1/basebackups_005/aosegments/1663_82385_bdd51f8ff6b3fbf3f122f263ed1ec0ae_123002_385_2_aoseg |           0 |
            0 |      144194358
(24 rows)


-- now we have some files in external storage.
-- try offload our relation


db2=# select count(1) from public.local_table_backuped ;
selec   count
-----------
 200000000
(1 row)

Time: 3637.040 ms
db2=# select yezzey_define_offload_policy('public', 'local_table_backuped');
WARNING:  yezzey: relation virtual size calculated: 96129088  (seg2 slice1 2a02:6b8:c25:109d:0:1589:d69f:58c1:6000 pid=1122837)
WARNING:  yezzey: relation virtual size calculated: 120161392  (seg3 slice1 2a02:6b8:c25:109d:0:1589:d69f:58c1:6001 pid=1122836)
WARNING:  yezzey: relation virtual size calculated: 128172128  (seg5 slice1 2a02:6b8:c25:124c:0:1589:bf3b:4e51:6001 pid=1097971)
WARNING:  yezzey: relation virtual size calculated: 168225888  (seg0 slice1 2a02:6b8:c25:1c55:0:1589:cb6d:a5e8:6000 pid=1145638)
WARNING:  yezzey: relation virtual size calculated: 144193680  (seg1 slice1 2a02:6b8:c25:1c55:0:1589:cb6d:a5e8:6001 pid=1145639)
WARNING:  yezzey: relation virtual size calculated: 144193680  (seg4 slice1 2a02:6b8:c25:124c:0:1589:bf3b:4e51:6000 pid=1097970)
WARNING:  yezzey: relation virtual size calculated: 96129088  (seg2 slice1 2a02:6b8:c25:109d:0:1589:d69f:58c1:6000 pid=1122837)
WARNING:  yezzey: relation virtual size calculated: 128172128  (seg5 slice1 2a02:6b8:c25:124c:0:1589:bf3b:4e51:6001 pid=1097971)
WARNING:  yezzey: relation virtual size calculated: 120161392  (seg3 slice1 2a02:6b8:c25:109d:0:1589:d69f:58c1:6001 pid=1122836)
WARNING:  yezzey: relation virtual size calculated: 144193680  (seg1 slice1 2a02:6b8:c25:1c55:0:1589:cb6d:a5e8:6001 pid=1145639)
WARNING:  yezzey: relation virtual size calculated: 144193680  (seg4 slice1 2a02:6b8:c25:124c:0:1589:bf3b:4e51:6000 pid=1097970)
WARNING:  yezzey: relation virtual size calculated: 168225888  (seg0 slice1 2a02:6b8:c25:1c55:0:1589:cb6d:a5e8:6000 pid=1145638)
WARNING:  yezzey: relation virtual size calculated: 96129088  (seg2 slice1 2a02:6b8:c25:109d:0:1589:d69f:58c1:6000 pid=1122837)
WARNING:  yezzey: relation virtual size calculated: 128172128  (seg5 slice1 2a02:6b8:c25:124c:0:1589:bf3b:4e51:6001 pid=1097971)
WARNING:  yezzey: relation virtual size calculated: 120161392  (seg3 slice1 2a02:6b8:c25:109d:0:1589:d69f:58c1:6001 pid=1122836)
WARNING:  yezzey: relation virtual size calculated: 96129088  (seg2 slice1 2a02:6b8:c25:109d:0:1589:d69f:58c1:6000 pid=1122837)
WARNING:  yezzey: relation virtual size calculated: 144193680  (seg1 slice1 2a02:6b8:c25:1c55:0:1589:cb6d:a5e8:6001 pid=1145639)
WARNING:  yezzey: relation virtual size calculated: 144193680  (seg4 slice1 2a02:6b8:c25:124c:0:1589:bf3b:4e51:6000 pid=1097970)
WARNING:  yezzey: relation virtual size calculated: 168225888  (seg0 slice1 2a02:6b8:c25:1c55:0:1589:cb6d:a5e8:6000 pid=1145638)
WARNING:  yezzey: relation virtual size calculated: 120161392  (seg3 slice1 2a02:6b8:c25:109d:0:1589:d69f:58c1:6001 pid=1122836)
WARNING:  yezzey: relation virtual size calculated: 128172128  (seg5 slice1 2a02:6b8:c25:124c:0:1589:bf3b:4e51:6001 pid=1097971)
WARNING:  yezzey: relation virtual size calculated: 144193680  (seg1 slice1 2a02:6b8:c25:1c55:0:1589:cb6d:a5e8:6001 pid=1145639)
WARNING:  yezzey: relation virtual size calculated: 168225888  (seg0 slice1 2a02:6b8:c25:1c55:0:1589:cb6d:a5e8:6000 pid=1145638)
WARNING:  yezzey: relation virtual size calculated: 144193680  (seg4 slice1 2a02:6b8:c25:124c:0:1589:bf3b:4e51:6000 pid=1097970)
 yezzey_define_offload_policy
------------------------------

(1 row)

Time: 18376.182 ms
db2=#

db2=# select count(1) from public.local_table_backuped ;
   count
-----------
 200000000
(1 row)

Time: 6931.558 ms
-- as you see reading is twice slower. This is bacause relation is now not local.


-- we can make sure fo this by calling yezzey stat API function 

db2=# select * from yezzey_offload_relation_status('public', 'local_table_backuped');
 offload_reloid | segindex | local_bytes | local_commited_bytes | external_bytes
----------------+----------+-------------+----------------------+----------------
          90380 |        3 |           0 |                    0 |      480648280
          90380 |        0 |           0 |                    0 |      672906324
          90380 |        4 |           0 |                    0 |      576777432
          90380 |        5 |           0 |                    0 |      512691212
          90380 |        2 |           0 |                    0 |      384519028
          90380 |        1 |           0 |                    0 |      576777432
(6 rows)

Time: 831.954 ms

```