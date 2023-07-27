DROP DATABASE testreg;
CREATE DATABASE testreg;
\c testreg

CREATE EXTENSION yezzey;

-- AO

CREATE TABLE regaoty(i INT) WITH (appendonly=true);
INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
SELECT * FROM yezzey_define_offload_policy('regaoty');

SELECT count(1) FROM regaoty;

SELECT * FROM yezzey_dump_virtual_index('regaoty');

INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);

ALTER TABLE regaoty ADD COLUMN j INT;

INSERT INTO regaoty SELECT * FROM generate_series(1, 100) a JOIN generate_series(1, 100) b ON true;
DELETE FROM regaoty;
INSERT INTO regaoty SELECT * FROM generate_series(1, 100) a JOIN generate_series(1, 100) b ON true;
VACUUM regaoty;

SELECT count(1) FROM regaoty;
