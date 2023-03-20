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
INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaoty;

SELECT * FROM yezzey_offload_relation_status('regaoty');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('regaoty');
SELECT * FROM yezzey_relation_describe_external_storage_structure('regaoty');

ALTER TABLE regaoty ADD COLUMN j INT;
SELECT count(1) FROM regaoty;
INSERT INTO regaoty SELECT * FROM generate_series(1, 100) a JOIN generate_series(1, 100) b ON true;
SELECT count(1) FROM regaoty;

SELECT * FROM yezzey_offload_relation_status('regaoty');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('regaoty');
SELECT * FROM yezzey_relation_describe_external_storage_structure('regaoty');

SELECT yezzey_load_relation('regaoty');
SELECT count(1) FROM regaoty;

-- AOCS

CREATE TABLE regaocsty(i INT) WITH (appendonly=true, orientation=column);
INSERT INTO regaocsty SELECT * FROM generate_series(1, 100000);
SELECT * FROM yezzey_define_offload_policy('regaocsty');

SELECT * FROM yezzey_offload_relation_status('regaocsty');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('regaocsty');
SELECT * FROM yezzey_relation_describe_external_storage_structure('regaocsty');

SELECT count(1) FROM regaocsty;
INSERT INTO regaocsty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaocsty;

SELECT * FROM yezzey_offload_relation_status('regaocsty');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('regaocsty');
SELECT * FROM yezzey_relation_describe_external_storage_structure('regaocsty');

ALTER TABLE regaocsty ADD COLUMN j INT;
SELECT count(1) FROM regaocsty;
INSERT INTO regaocsty SELECT * FROM generate_series(1, 100) a JOIN generate_series(1, 100) b ON true;
SELECT count(1) FROM regaocsty;

SELECT * FROM yezzey_offload_relation_status('regaoty');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('regaoty');
SELECT * FROM yezzey_relation_describe_external_storage_structure('regaoty');

SELECT yezzey_load_relation('regaocsty');
SELECT count(1) FROM regaocsty;

-- compressed

CREATE TABLE regaotcy(i INT) WITH (appendonly=true);
INSERT INTO regaotcy SELECT * FROM generate_series(1, 100000);
SELECT * FROM yezzey_define_offload_policy('regaotcy');

SELECT * FROM yezzey_offload_relation_status('regaotcy');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('regaotcy');
SELECT * FROM yezzey_relation_describe_external_storage_structure('regaotcy');

SELECT count(1) FROM regaotcy;
INSERT INTO regaotcy SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaotcy;

SELECT * FROM yezzey_offload_relation_status('regaotcy');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('regaotcy');
SELECT * FROM yezzey_relation_describe_external_storage_structure('regaotcy');

ALTER TABLE regaotcy ADD COLUMN j INT;
SELECT count(1) FROM regaotcy;
INSERT INTO regaotcy SELECT * FROM generate_series(1, 100) a JOIN generate_series(1, 100) b ON true;
SELECT count(1) FROM regaotcy;

SELECT * FROM yezzey_offload_relation_status('regaotcy');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('regaotcy');
SELECT * FROM yezzey_relation_describe_external_storage_structure('regaotcy');

SELECT yezzey_load_relation('regaotcy');
SELECT count(1) FROM regaotcy;

\c postgres

DROP DATABASE testreg;
