DROP DATABASE testreg;
CREATE DATABASE testreg;
\c testreg

CREATE EXTENSION yezzey;

-- AO

CREATE TABLE regaoty(i INT) WITH (appendonly=true);
INSERT INTO regaoty SELECT * FROM generate_series(1, 1000000);

SELECT yezzey_set_relation_expirity('public', 'regaoty', (NOW() + '10 min'::INTERVAL)::TIMESTAMP);

\! echo AO simple test OK

-- AOCS

--\! echo AOCS simple test OK
--\c postgres

--DROP DATABASE testreg;
