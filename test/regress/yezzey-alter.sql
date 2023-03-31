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

DELETE FROM regaoty;
INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaoty;

ALTER TABLE regaoty SET DISTRIBUTED RANDOMLY;
SELECT count(1) FROM regaoty;

SELECT * FROM yezzey_offload_relation_status('regaoty');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('regaoty');
SELECT * FROM yezzey_relation_describe_external_storage_structure('regaoty');

\! echo AO simple test OK

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

DELETE FROM regaocsty;
INSERT INTO regaocsty SELECT * FROM generate_series(1, 100000);
INSERT INTO regaocsty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaocsty;

ALTER TABLE regaoty SET DISTRIBUTED RANDOMLY;
SELECT count(1) FROM regaocsty;

SELECT * FROM yezzey_offload_relation_status('regaocsty');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('regaocsty');
SELECT * FROM yezzey_relation_describe_external_storage_structure('regaocsty');


\! echo AOCS simple test OK
\c postgres

DROP DATABASE testreg;
