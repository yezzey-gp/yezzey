


problems:

pg_aoseg.pg_aoseg_<oid> cannot be locked:

src/include/catalog/pg_class.h:172:#define		  RELKIND_AOSEGMENTS	  'o'		/* AO segment files and eof's */

LockRelationAppendOnlySegmentFile -> LOCKACQUIRE_ALREADY_HELD



SELECT seg.aooid, md5(seg.aotablefqn), 'pg_aoseg.' || quote_ident(aoseg_c.relname) AS aosegtablefqn,
	seg.relfilenode, seg.reltablespace, seg.relstorage, seg.relnatts 
FROM pg_class aoseg_c
JOIN (
	SELECT pg_ao.relid AS aooid, pg_ao.segrelid, 
			aotables.aotablefqn, aotables.relstorage, 
			aotables.relnatts, aotables.relfilenode, aotables.reltablespace
	FROM pg_appendonly pg_ao
	JOIN (
		SELECT c.oid, quote_ident(n.nspname)|| '.' || quote_ident(c.relname) AS aotablefqn, 
				c.relstorage, c.relnatts, c.relfilenode, c.reltablespace 
		FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE relstorage IN ( 'ao', 'co' ) AND relpersistence='p'
		) aotables ON pg_ao.relid = aotables.oid
	) seg ON aoseg_c.oid = seg.segrelid;




to install this:

include yezzey into shared_preload_libraries
modify your config:

yezzey.S3_prefix = 'segment{{ content_id }}/'
yezzey.S3_putter = '/home/reshke/wal-g/main/pg/wal-g --config=/home/reshke/wal-g/conf.walg.yaml st put -f %f %s'
yezzey.S3_getter = '/home/reshke/wal-g/main/pg/wal-g --config=/home/reshke/wal-g/conf.walg.yaml st get %s %f'

restart cluster

create extension yezzey



create table aocst(i int, j int, k int) with (appendonly=true, orientation=column);
insert into aocst(k, j) select * from (select * from generate_series(1, 10)) a join (select * from generate_series(1, 10)) b on true;
select count(1) from aocst;
make destroy-demo-cluster && make create-demo-cluster && gpconfig -s shared_preload_libraries -v yezzey && gpstop -a -i && gpstart -a