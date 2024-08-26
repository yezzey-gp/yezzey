
CREATE TABLE yezzey.offload_tablespace_map(
    reloid                 OID PRIMARY KEY,
    origin_tablespace_name NAME
) DISTRIBUTED REPLICATED;

SET allow_segment_DML to on;

CREATE OR REPLACE FUNCTION
yezzey_upgrade_function() RETURNS VOID
AS $$ 
BEGIN

    -- SET gp_session_role to 'utility';
    INSERT INTO 
        yezzey.offload_tablespace_map
    SELECT 
        reloid, 'pg_default'
    FROM 
        yezzey.offload_metadata
    WHERE 
        relpolicy = 1
    ;

    -- RESET gp_session_role;
END;
$$ 
EXECUTE ON ALL SEGMENTS
LANGUAGE PLPGSQL;

SELECT yezzey_upgrade_function();

RESET allow_segment_DML;

CREATE OR REPLACE FUNCTION yezzey_define_relation_offload_policy_internal_prepare(reloid OID) RETURNS void
AS 'MODULE_PATHNAME'
VOLATILE
EXECUTE ON ALL SEGMENTS
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

    IF FOUND THEN
        RAISE NOTICE 'relation % already offloaded', i_offload_relname;
	RETURN;
    END IF;

    PERFORM yezzey_define_relation_offload_policy_internal_prepare(
        v_reloid
    );

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

DROP FUNCTION yezzey_upgrade_function();
