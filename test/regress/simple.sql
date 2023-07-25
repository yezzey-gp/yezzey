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

DROP DATABASE testreg;
