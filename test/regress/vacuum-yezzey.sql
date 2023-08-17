DROP DATABASE testreg;
CREATE DATABASE testreg;
\c testreg

CREATE EXTENSION yezzey;
CREATE TABLE aot(i INT) WITH (appendonly=true);
select yezzey_define_offload_policy('aot');
insert into aot select * from generate_series(1, 10000);
insert into aot select * from generate_series(1, 10000);
delete from aot;
insert into aot select * from generate_series(10000, 20000);
VACUUM aot;
select count(1) from aot; -- works ok, use second block(segment) of relation ;
insert into aot select * from generate_series(10000, 20000); -- insert goes in first block(segment) with modcount 4
select count(1) from aot; -- works

\c postgres
DROP DATABASE testreg;
