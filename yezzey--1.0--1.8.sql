
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
    relpolicy = 1
;

CREATE OR REPLACE FUNCTION yezzey_define_relation_offload_policy_internal_prepare(reloid OID) RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON MASTER
LANGUAGE C STRICT;


CREATE OR REPLACE FUNCTION
yezzey_define_offload_policy(i_offload_nspname TEXT, i_offload_relname TEXT, i_policy offload_policy DEFAULT 'remote_always')
RETURNS VOID
AS $$
DECLARE
    v_tmprow OID;
    v_reloid OID;
    v_par_reloid OID;
BEGIN
    SELECT 
        oid
    FROM 
        pg_catalog.pg_class
    INTO v_reloid 
    WHERE 
        relname = i_offload_relname AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = i_offload_nspname);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'relation % is not found in pg_class', i_offload_relname;
    END IF;
    
    SELECT 
        reloid
    FROM
        yezzey.offload_metadata
    INTO v_tmprow 
    WHERE 
        reloid = v_reloid AND relpolicy = 1;

    PERFORM yezzey_define_relation_offload_policy_internal_prepare(
        v_reloid
    );

    IF FOUND THEN
        RAISE NOTICE 'relation % already offloaded', i_offload_relname;
	RETURN;
    END IF;

    SELECT parrelid 
         FROM pg_partition
    INTO v_par_reloid 
    WHERE parrelid = v_reloid;

    IF NOT FOUND THEN
        -- non-partitioned relation
        PERFORM yezzey_define_relation_offload_policy_internal_seg(
            v_reloid
        );
        PERFORM yezzey_define_relation_offload_policy_internal(
            v_reloid
        );
    ELSE 

         FOR v_tmprow IN 
             SELECT (i_offload_nspname||'.'||partitiontablename)::regclass::oid FROM pg_partitions WHERE schemaname = i_offload_nspname AND tablename = i_offload_relname
         LOOP

             RAISE NOTICE 'offloading partition oid %', v_tmprow;
             -- offload each part
             PERFORM yezzey_define_relation_offload_policy_internal_seg(
                 v_tmprow
             );
             PERFORM yezzey_define_relation_offload_policy_internal(
                 v_tmprow
             );
         END LOOP;

    END IF;
END;
$$
LANGUAGE PLPGSQL;
