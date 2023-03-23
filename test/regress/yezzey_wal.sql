DROP DATABASE testwal;
CREATE DATABASE testwal;
\c testwal

CREATE EXTENSION yezzey;


CREATE TABLE regaotywal(i INT) WITH (appendonly=true);
INSERT INTO regaotywal SELECT * FROM generate_series(1, 100000);
SELECT * FROM yezzey_define_offload_policy('regaotywal');

SELECT count(1) FROM regaotywal;
INSERT INTO regaotywal SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaotywal;

SELECT yezzey_load_relation('regaotywal');
SELECT count(1) FROM regaotywal;
INSERT INTO regaotywal SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaotywal;


\c postgres
DROP DATABASE testwal;
