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
#include "access/aocssegfiles.h"
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
	AOCSFileSegInfo **segfile_array_cs;
	Snapshot appendOnlyMetaDataSnapshot;
	int rc;
	int nvp;
	int pseudosegno;
	int inat;

	/*  This mode guarantees that the holder is the only transaction accessing the table in any way. 
	* we need to be sure, thar no other transaction either reads or write to given relation
	* because we are going to delete relation from local storage
	*/
	aorel = relation_open(reloid, AccessExclusiveLock);
	nvp = aorel->rd_att->natts;

	/*
	* Relation segments named base/DBOID/aorel->rd_node.*
	*/

	elog(yezzey_log_level, "offloading relnode %d", aorel->rd_node.relNode);

	/* for now, we locked relation */

	/* GetAllFileSegInfo_pg_aoseg_rel */

	/* acquire snapshot for aoseg table lookup */
	appendOnlyMetaDataSnapshot = SnapshotSelf;

	if (aorel->rd_rel->relstorage == 'a') {

		/* Get information about all the file segments we need to scan */
		segfile_array = GetAllFileSegInfo(aorel, appendOnlyMetaDataSnapshot, &total_segfiles);

		rc = offloadRelationSegment(aorel->rd_node, 0);
		if (rc < 0) {
			elog(ERROR, "failed to offload segment number %d", 0);
		}
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
	} else {
				/* ao columns, relstorage == 'c' */
		segfile_array_cs = GetAllAOCSFileSegInfo(aorel,
										  appendOnlyMetaDataSnapshot, &total_segfiles);

		rc = offloadRelationSegment(aorel->rd_node, 0);
		if (rc < 0) {
			elog(ERROR, "failed to offload segment number %d", 0);
		}

		for (inat = 0; inat < nvp; ++ inat) {
			for (i = 0; i < total_segfiles; i++)
			{
				segno = segfile_array_cs[i]->segno;
				pseudosegno = (inat * AOTupleId_MultiplierSegmentFileNum) + segno;
				elog(WARNING, "offloading segment no %d, pseudosegno %d", segno, pseudosegno);

				rc = offloadRelationSegment(aorel->rd_node, pseudosegno);
				if (rc < 0) {
					elog(ERROR, "failed to offload segment number %d, pseudosegno %d", segno, pseudosegno);
				}
				/* segment if offloaded */
			}
		}

		if (segfile_array_cs)
		{
			FreeAllAOCSSegFileInfo(segfile_array_cs, total_segfiles);
			pfree(segfile_array_cs);
		}
	}

	/* insert entry in relocate table, is no any */

	/* cleanup */

	relation_close(aorel, AccessExclusiveLock);

	return 0;
}

int
load_relation_internal(Oid reloid) {
	Relation aorel;
	int i;
	int segno;
	int pseudosegno;
	int nvp;
	int inat;
	int total_segfiles;
	FileSegInfo **segfile_array;
	AOCSFileSegInfo **segfile_array_cs;
	Snapshot appendOnlyMetaDataSnapshot;
	int rc;	
	

	/*  XXX: maybe fix lock here to take more granular lock
	*/
	aorel = relation_open(reloid, AccessExclusiveLock);
	nvp = aorel->rd_att->natts;

	/*
	* Relation segments named base/DBOID/aorel->rd_node.*
	*/

	elog(yezzey_log_level, "loading relnode %d", aorel->rd_node.relNode);

	/* for now, we locked relation */

	/* GetAllFileSegInfo_pg_aoseg_rel */

	/* acquire snapshot for aoseg table lookup */
	appendOnlyMetaDataSnapshot = SnapshotSelf;

	/* Get information about all the file segments we need to scan */
	if (aorel->rd_rel->relstorage == 'a') {
		/* ao rows relation */
		segfile_array = GetAllFileSegInfo(aorel, appendOnlyMetaDataSnapshot, &total_segfiles);

		for (i = 0; i < total_segfiles; i++)
		{
			segno = segfile_array[i]->segno;
			elog(yezzey_log_level, "loading segment no %d", segno);

			rc = loadRelationSegment(aorel->rd_node, segno);
			if (rc < 0) {
				elog(ERROR, "failed to offload segment number %d", segno);
			}
			/* segment if loaded */
		}

		if (segfile_array)
		{
			FreeAllSegFileInfo(segfile_array, total_segfiles);
			pfree(segfile_array);
		}
	} else {
		/* ao columns, relstorage == 'c' */
		segfile_array_cs = GetAllAOCSFileSegInfo(aorel,
										  appendOnlyMetaDataSnapshot, &total_segfiles);

		for (inat = 0; inat < nvp; ++inat) {
			for (i = 0; i < total_segfiles; i++)
			{
				segno = segfile_array_cs[i]->segno;
				pseudosegno = (inat * AOTupleId_MultiplierSegmentFileNum) + segno;
				elog(yezzey_log_level, "loading cs segment no %d pseudosegno %d", segno, pseudosegno);

				rc = loadRelationSegment(aorel->rd_node, pseudosegno);
				if (rc < 0) {
					elog(ERROR, "failed to load cs segment number %d pseudosegno %d", segno, pseudosegno);
				}
				/* segment if loaded */
			}
		}

		if (segfile_array_cs)
		{
			FreeAllAOCSSegFileInfo(segfile_array_cs, total_segfiles);
			pfree(segfile_array_cs);
		}
	}

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

	rc = load_relation_internal(reloid);

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