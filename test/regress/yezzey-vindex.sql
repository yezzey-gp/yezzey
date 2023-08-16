DROP DATABASE testwal;
CREATE DATABASE testwal;
\c testwal

CREATE EXTENSION yezzey;

-- how to do it?
CREATE OR REPLACE FUNCTION 
yvindex_get()
RETURNS SET OF ROWS
AS $$
SELECT * FROM yezzey.yezzey_virtual_index_

CREATE TABLE regaotywal(i INT) WITH (appendonly=true);
INSERT INTO regaotywal SELECT * FROM generate_series(1, 100000);
SELECT * FROM yezzey_define_offload_policy('regaotywal');

SELECT count(1) FROM regaotywal;
INSERT INTO regaotywal SELECT * FROM generate_series(1, 100000);
SELECT count(1) FROM regaotywal;



\c postgres
DROP DATABASE testwal;
