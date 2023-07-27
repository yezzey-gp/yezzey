DROP DATABASE testreg;
CREATE DATABASE testreg;
\c testreg

CREATE EXTENSION yezzey;

-- AO

CREATE TABLE regaoty(i INT) WITH (appendonly=true);
INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);
SELECT * FROM yezzey_define_offload_policy('regaoty');

INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);

SELECT count(1) FROM regaoty;
SELECT * FROM regaoty LIMIT 5 OFFSET 7823;

SELECT * FROM yezzey_dump_virtual_index('regaoty');

DELETE FROM regaoty WHERE random() <= 0.33;

INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);

VACUUM regaoty;

SELECT count(1) FROM regaoty;
SELECT * FROM regaoty LIMIT 5 OFFSET 7823;

INSERT INTO regaoty SELECT * FROM generate_series(1, 100000);

SELECT * FROM yezzey_dump_virtual_index('regaoty');
SELECT count(1) FROM regaoty;

\c postgres

DROP DATABASE testreg;
