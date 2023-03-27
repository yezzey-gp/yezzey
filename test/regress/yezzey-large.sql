DROP DATABASE largedb;
CREATE DATABASE largedb;
\c largedb
CREATE EXTENSION yezzey;
create table largeyt(i int, j int, k int, kk int) with (appendonly=true, orientation=column);

select yezzey_define_offload_policy('largeyt');

insert into largeyt select * from generate_series(1, 10)  a join  generate_series(1, 10) b on true join  generate_series(1, 10) c on true join  generate_series(1, 100) d on true;
insert into largeyt select * from generate_series(1, 100)  a join  generate_series(1, 10) b on true join  generate_series(1, 10) c on true join  generate_series(1, 100) d on true;
insert into largeyt select * from generate_series(1, 100)  a join  generate_series(1, 100) b on true join  generate_series(1, 10) c on true join  generate_series(1, 100) d on true;
