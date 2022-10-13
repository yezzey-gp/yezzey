#include "postgres.h"
#include "fmgr.h"

#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>

#include "utils/builtins.h"
#include "executor/spi.h"
#include "pgstat.h"
#include "yezzey.h"

#include "yezzey.h"
#include "storage/lmgr.h"
#include "access/aosegfiles.h"
#include "utils/tqual.h"

#include "external_storage.h"

char *s3_getter = NULL;
char *s3_putter = NULL;
char *s3_prefix = NULL;

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(offload_relation);
PG_FUNCTION_INFO_V1(load_relation);
PG_FUNCTION_INFO_V1(force_segment_offload);


int offload_relation_internal(Oid reloid) {
 	Relation aorel;
	int i;
	int segno;
	int total_segfiles;
	FileSegInfo **segfile_array;
	Snapshot appendOnlyMetaDataSnapshot;
	int rc;

	/*  This mode guarantees that the holder is the only transaction accessing the table in any way. 
	* we need to be sure, thar no other transaction either reads or write to given relation
	* because we are going to delete relation from local storage
	*/
	aorel = relation_open(reloid, AccessExclusiveLock);

	/*
	* Relation segments named base/DBOID/aorel->rd_node.*
	*/

	elog(yezzey_log_level, "offloading relnode %d", aorel->rd_node.relNode);

	/* for now, we locked relation */

	/* GetAllFileSegInfo_pg_aoseg_rel */

	/* acquire snapshot for aoseg table lookup */
	appendOnlyMetaDataSnapshot = SnapshotSelf;

	/* Get information about all the file segments we need to scan */
	segfile_array = GetAllFileSegInfo(aorel, appendOnlyMetaDataSnapshot, &total_segfiles);


	for (i = 0; i < total_segfiles; i++)
	{
		segno = segfile_array[i]->segno;
		elog(yezzey_log_level, "offloading segment no %d", segno);

		rc = offloadRelationSegment(aorel->rd_node, segno);
		if (rc < 0) {
			elog(ERROR, "failed to offload segment number %d", segno);
		}
		/* segment if offloaded */
	}

	if (segfile_array)
	{
		FreeAllSegFileInfo(segfile_array, total_segfiles);
		pfree(segfile_array);
	}


	/* insert entry in relocate table, is no any */


	/* cleanup */

	relation_close(aorel, AccessExclusiveLock);

	return 0;
}

Datum
load_relation(PG_FUNCTION_ARGS) {
	/*
	* Force table offloading to external storage
	* In order:
	* 1) lock table in IN EXCLUSIVE MODE (is that needed?) 
	* 2) check pg_aoseg.pg_aoseg_XXX table for all segments
	* 3) go and load each segment (XXX: enhancement: do loading in parallel)
 	*/
	Oid reloid;
	int rc;

	reloid = PG_GETARG_OID(0);

	// rc = load_relation_internal(reloid);

	PG_RETURN_VOID();
}


Datum
offload_relation(PG_FUNCTION_ARGS)
{
	/*
	* Force table offloading to external storage
	* In order:
	* 1) lock table in IN EXCLUSIVE MODE
	* 2) check pg_aoseg.pg_aoseg_XXX table for all segments
	* 3) go and offload each segment (XXX: enhancement: do offloading in parallel)
 	*/
	Oid reloid;
	int rc;

	reloid = PG_GETARG_OID(0);

	rc = offload_relation_internal(reloid);

	PG_RETURN_VOID();
}

Datum
force_segment_offload(PG_FUNCTION_ARGS) {
	PG_RETURN_VOID();
}