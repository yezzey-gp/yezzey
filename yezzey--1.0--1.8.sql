
CREATE TABLE yezzey.offload_tablespace_map(
    reloid                 OID PRIMARY KEY,
    origin_tablespace_name TEXT
) DISTRIBUTED REPLICATED;

INSERT INTO 
    yezzey.offload_tablespace_map
SELECT 
    reloid, 'pg_default'
FROM 
    yezzey.offload_metadata
WHERE 
    offload_policy = 1
;

