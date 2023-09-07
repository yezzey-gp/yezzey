DROP DATABASE testreg;
CREATE DATABASE testreg;
\c testreg

CREATE EXTENSION yezzey;

-- AO

CREATE TABLE regaoty(i INT) WITH (appendonly=true);
set allow_system_table_mods TO on;

update gp_distribution_policy set numsegments = 2 where localoid = 'regaoty'::regclass::oid;

SELECT * FROM yezzey_define_offload_policy('regaoty');

INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
SELECT COUNT(1) FROM regaoty;
SELECT * FROM yezzey_dump_virtual_index('regaoty');

ALTER TABLE regaoty EXPAND TABLE;

INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
SELECT COUNT(1) FROM regaoty;
SELECT * FROM yezzey_dump_virtual_index('regaoty');

