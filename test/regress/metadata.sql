SELECT c.oid,c.*,d.description,
p.partitiontablename,p.partitionboundary as partition_expr,CASE WHEN x.urilocation IS NOT NULL THEN array_to_string(x.urilocation, ',') ELSE '' END AS urilocation,
CASE WHEN x.command IS NOT NULL THEN x.command ELSE '' END AS command,
x.fmttype, x.fmtopts,
coalesce(x.rejectlimit, 0) AS rejectlimit,
coalesce(x.rejectlimittype, '') AS rejectlimittype,
array_to_string(x.execlocation, ',') AS execlocation,
pg_encoding_to_char(x.encoding) AS encoding,
x.writable,
case when c.relstorage = 'x' then true else false end as "is_ext_table",
case when (ns.nspname !~ '^pg_toast' and ns.nspname like 'pg_temp%') then true else false end as "is_temp_table"
FROM pg_catalog.pg_class c
INNER JOIN pg_catalog.pg_namespace ns
on ns.oid = c.relnamespace
LEFT OUTER JOIN pg_catalog.pg_description d
ON d.objoid=c.oid AND d.objsubid=0
LEFT OUTER JOIN pg_catalog.pg_exttable x
on x.reloid = c.oid
LEFT OUTER JOIN pg_catalog.pg_partitions p
on c.relname = p.partitiontablename and ns.nspname = p.schemaname;
