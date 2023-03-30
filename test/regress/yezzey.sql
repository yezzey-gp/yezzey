DROP DATABASE testreg;
CREATE DATABASE testreg;
\c testreg

CREATE EXTENSION yezzey;

-- AO

CREATE TABLE regaoty(i INT) WITH (appendonly=true);
INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
SELECT * FROM yezzey_define_offload_policy('regaoty');

SELECT * FROM yezzey_offload_relation_status('regaoty');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('regaoty');
SELECT * FROM yezzey_relation_describe_external_storage_structure('regaoty');

SELECT count(1) FROM regaoty;
SELECT * FROM regaoty LIMIT 5;
INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaoty;
SELECT * FROM regaoty LIMIT 5;

SELECT * FROM yezzey_offload_relation_status('regaoty');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('regaoty');
SELECT * FROM yezzey_relation_describe_external_storage_structure('regaoty');

ALTER TABLE regaoty ADD COLUMN j INT;
SELECT count(1) FROM regaoty;
SELECT * FROM regaoty LIMIT 5;
INSERT INTO regaoty SELECT * FROM generate_series(1, 100) a JOIN generate_series(1, 100) b ON true;
SELECT count(1) FROM regaoty;
SELECT * FROM regaoty LIMIT 5;

DELETE FROM regaoty;
INSERT INTO regaoty SELECT * FROM generate_series(1, 100) a JOIN generate_series(1, 100) b ON true;
VACUUM regaoty;
SELECT count(1) FROM regaoty;
SELECT * FROM regaoty LIMIT 5;

SELECT * FROM yezzey_offload_relation_status('regaoty');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('regaoty');
SELECT * FROM yezzey_relation_describe_external_storage_structure('regaoty');

SELECT yezzey_load_relation('regaoty');
SELECT count(1) FROM regaoty;
SELECT * FROM regaoty LIMIT 5;
VACUUM regaoty;

\! echo AO simple test OK

-- AOCS

CREATE TABLE regaocsty(i INT) WITH (appendonly=true, orientation=column);
INSERT INTO regaocsty SELECT * FROM generate_series(1, 100000);
SELECT * FROM yezzey_define_offload_policy('regaocsty');

SELECT * FROM yezzey_offload_relation_status('regaocsty');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('regaocsty');
SELECT * FROM yezzey_relation_describe_external_storage_structure('regaocsty');

SELECT count(1) FROM regaocsty;
SELECT * FROM regaocsty LIMIT 5;
INSERT INTO regaocsty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaocsty;
SELECT * FROM regaocsty LIMIT 5;

SELECT * FROM yezzey_offload_relation_status('regaocsty');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('regaocsty');
SELECT * FROM yezzey_relation_describe_external_storage_structure('regaocsty');

ALTER TABLE regaocsty ADD COLUMN j INT;
SELECT count(1) FROM regaocsty;
SELECT * FROM regaocsty LIMIT 5;
INSERT INTO regaocsty SELECT * FROM generate_series(1, 100) a JOIN generate_series(1, 100) b ON true;
SELECT count(1) FROM regaocsty;
SELECT * FROM regaocsty LIMIT 5;

DELETE FROM regaocsty;
INSERT INTO regaocsty SELECT * FROM generate_series(1, 100) a JOIN generate_series(1, 100) b ON true;
VACUUM regaocsty;
SELECT count(1) FROM regaocsty;
SELECT * FROM regaocsty LIMIT 5;

SELECT * FROM yezzey_offload_relation_status('regaocsty');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('regaocsty');
SELECT * FROM yezzey_relation_describe_external_storage_structure('regaocsty');

SELECT yezzey_load_relation('regaocsty');
SELECT count(1) FROM regaocsty;
SELECT * FROM regaocsty LIMIT 5;

DELETE FROM regaocsty;
INSERT INTO regaocsty SELECT * FROM generate_series(1, 100) a JOIN generate_series(1, 100) b ON true;
VACUUM regaocsty;
SELECT count(1) FROM regaocsty;
SELECT * FROM regaocsty LIMIT 5;
\! echo AOCS simple test OK

-- compressed

CREATE TABLE regaotcy(i INT) WITH (appendonly=true);
INSERT INTO regaotcy SELECT * FROM generate_series(1, 100000);
SELECT * FROM yezzey_define_offload_policy('regaotcy');

SELECT * FROM yezzey_offload_relation_status('regaotcy');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('regaotcy');
SELECT * FROM yezzey_relation_describe_external_storage_structure('regaotcy');

SELECT count(1) FROM regaotcy;
SELECT * FROM regaotcy LIMIT 5;
INSERT INTO regaotcy SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaotcy;
SELECT * FROM regaotcy LIMIT 5;

SELECT * FROM yezzey_offload_relation_status('regaotcy');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('regaotcy');
SELECT * FROM yezzey_relation_describe_external_storage_structure('regaotcy');

ALTER TABLE regaotcy ADD COLUMN j INT;
SELECT count(1) FROM regaotcy;
SELECT * FROM regaotcy LIMIT 5;
INSERT INTO regaotcy SELECT * FROM generate_series(1, 100) a JOIN generate_series(1, 100) b ON true;
SELECT count(1) FROM regaotcy;
SELECT * FROM regaotcy LIMIT 5;

DELETE FROM regaotcy;
INSERT INTO regaotcy SELECT * FROM generate_series(1, 100) a JOIN generate_series(1, 100) b ON true;
VACUUM regaotcy;
SELECT count(1) FROM regaotcy;
SELECT * FROM regaotcy LIMIT 5;

SELECT * FROM yezzey_offload_relation_status('regaotcy');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('regaotcy');
SELECT * FROM yezzey_relation_describe_external_storage_structure('regaotcy');

SELECT yezzey_load_relation('regaotcy');
SELECT count(1) FROM regaotcy;
SELECT * FROM regaotcy LIMIT 5;

DELETE FROM regaotcy;
INSERT INTO regaotcy SELECT * FROM generate_series(1, 100) a JOIN generate_series(1, 100) b ON true;
VACUUM regaotcy;
SELECT count(1) FROM regaotcy;
SELECT * FROM regaotcy LIMIT 5;

\! echo AO compressed simple test OK

-- offload, load, offload again

CREATE TABLE regaotylol(i INT) WITH (appendonly=true);
INSERT INTO regaotylol SELECT * FROM generate_series(1, 100000);
SELECT * FROM yezzey_define_offload_policy('regaotylol');

SELECT count(1) FROM regaotylol;
INSERT INTO regaotylol SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaotylol;

DELETE FROM regaotylol;
INSERT INTO regaotylol SELECT * FROM generate_series(1, 100000);
VACUUM regaotylol;
SELECT count(1) FROM regaotylol;

SELECT yezzey_load_relation('regaotylol');
SELECT count(1) FROM regaotylol;
INSERT INTO regaotylol SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaotylol;

SELECT * FROM yezzey_define_offload_policy('regaotylol');

SELECT count(1) FROM regaotylol;
INSERT INTO regaotylol SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaotylol;

DELETE FROM regaotylol;
INSERT INTO regaotylol SELECT * FROM generate_series(1, 100000);
VACUUM regaotylol;
SELECT count(1) FROM regaotylol;

\! echo AO offload-load-offlaod test OK

-- not default schema, (same relname) 
CREATE SCHEMA sh1;
CREATE TABLE sh1.regaoty(i INT) WITH (appendonly=true);
INSERT INTO sh1.regaoty SELECT * FROM generate_series(1, 100000);
SELECT * FROM yezzey_define_offload_policy('sh1', 'regaoty');

SELECT * FROM yezzey_offload_relation_status('sh1', 'regaoty');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('sh1', 'regaoty');
SELECT * FROM yezzey_relation_describe_external_storage_structure('sh1', 'regaoty');

SELECT count(1) FROM sh1.regaoty;
INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM sh1.regaoty;

SELECT * FROM yezzey_offload_relation_status('sh1', 'regaoty');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('sh1', 'regaoty');
SELECT * FROM yezzey_relation_describe_external_storage_structure('sh1', 'regaoty');

ALTER TABLE sh1.regaoty ADD COLUMN j INT;
SELECT count(1) FROM sh1.regaoty;
INSERT INTO sh1.regaoty SELECT * FROM generate_series(1, 100) a JOIN generate_series(1, 100) b ON true;
SELECT count(1) FROM sh1.regaoty;

SELECT * FROM yezzey_offload_relation_status('sh1', 'regaoty');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('sh1', 'regaoty');
SELECT * FROM yezzey_relation_describe_external_storage_structure('sh1', 'regaoty');

SELECT yezzey_load_relation('sh1', 'regaoty');
SELECT count(1) FROM sh1.regaoty;


\! echo AO non-default schema test OK
\c postgres

DROP DATABASE testreg;
