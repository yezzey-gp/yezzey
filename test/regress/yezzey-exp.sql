
DROP DATABASE testreg;
CREATE DATABASE testreg;
\c testreg

SET allow_system_table_mods = true;

CREATE EXTENSION yezzey;

-- AO

CREATE TABLE regaoty(i INT) WITH (appendonly=true);

UPDATE gp_distribution_policy SET numsegments = 2 WHERE localoid = 'regaoty'::regclass;

INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
SELECT * FROM yezzey_define_offload_policy('regaoty');

SELECT * FROM yezzey_offload_relation_status('regaoty');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('regaoty');
SELECT * FROM yezzey_relation_describe_external_storage_structure('regaoty');

SELECT count(1) FROM regaoty;
INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaoty;

ALTER TABLE regaoty EXPAND TABLE;


SELECT count(1) FROM regaoty;
INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaoty;


SELECT * FROM yezzey_offload_relation_status('regaoty');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('regaoty');
SELECT * FROM yezzey_relation_describe_external_storage_structure('regaoty');

\! echo AO simple test OK

-- AOCS

CREATE TABLE regaocsty(i INT) WITH (appendonly=true, orientation=column);

UPDATE gp_distribution_policy SET numsegments = 2 WHERE localoid = 'regaocsty'::regclass;

INSERT INTO regaocsty SELECT * FROM generate_series(1, 100000);
SELECT * FROM yezzey_define_offload_policy('regaocsty');

SELECT * FROM yezzey_offload_relation_status('regaocsty');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('regaocsty');
SELECT * FROM yezzey_relation_describe_external_storage_structure('regaocsty');

SELECT count(1) FROM regaocsty;
INSERT INTO regaocsty SELECT * FROM generate_series(1, 100000);

ALTER TABLE regaocsty EXPAND TABLE;

SELECT count(1) FROM regaocsty;
INSERT INTO regaocsty SELECT * FROM generate_series(1, 100000);

SELECT * FROM yezzey_offload_relation_status('regaocsty');
SELECT * FROM yezzey_offload_relation_status_per_filesegment('regaocsty');
SELECT * FROM yezzey_relation_describe_external_storage_structure('regaocsty');


SET allow_system_table_mods = false;

\! echo AOCS simple test OK
\c postgres
DROP DATABASE testreg;
