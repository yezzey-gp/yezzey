DROP DATABASE testreg;
CREATE DATABASE testreg;
\c testreg

CREATE EXTENSION yezzey;

-- AO

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

\c postgres

DROP DATABASE testreg;
