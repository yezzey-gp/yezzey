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

#include "access/xlog.h"
#include "catalog/storage_xlog.h"
#include "funcapi.h"
#include "common/relpath.h"

#include "storage/lmgr.h"
#include "access/aosegfiles.h"
#include "access/aocssegfiles.h"
#include "utils/tqual.h"

#include "external_storage.h"

// For GpIdentity
#include "c.h"
#include "cdb/cdbvars.h"


char *s3_getter = NULL;
char *s3_putter = NULL;
char *s3_prefix = NULL;

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(offload_relation);
PG_FUNCTION_INFO_V1(load_relation);
PG_FUNCTION_INFO_V1(force_segment_offload);
PG_FUNCTION_INFO_V1(yezzey_get_relation_offload_status);
PG_FUNCTION_INFO_V1(yezzey_get_relation_offload_per_filesegment_status);

void
yezzey_log_smgroffload(RelFileNode *rnode);

/*
 * Perform XLogInsert of a XLOG_SMGR_CREATE record to WAL.
 */
void
yezzey_log_smgroffload(RelFileNode *rnode)
{
	xl_smgr_create xlrec;
	XLogRecData rdata;

	/*
	 * Make an XLOG entry reporting the file creation.
	 */
	xlrec.rnode = *rnode;
	xlrec.forkNum = YEZZEY_FORKNUM;

	rdata.data = (char *) &xlrec;
	rdata.len = sizeof(xlrec);
	rdata.buffer = InvalidBuffer;
	rdata.next = NULL;

	XLogInsert(RM_SMGR_ID, XLOG_SMGR_CREATE, &rdata);
}



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
	int64 modcount;
	int64 logicalEof;
	int pseudosegno;
	int inat;

	/*  This mode guarantees that the holder is the only transaction accessing the table in any way. 
	* we need to be sure, thar no other transaction either reads or write to given relation
	* because we are going to delete relation from local storage
	*/
	aorel = relation_open(reloid, AccessExclusiveLock);
	aorel->rd_smgr = smgropen(aorel->rd_node, InvalidBackendId);
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

		// rc = offloadRelationSegment(aorel->rd_node, 0, 0);
		// if (rc < 0) {
		// 	elog(ERROR, "failed to offload segment number %d", 0);
		// }
		for (i = 0; i < total_segfiles; i++)
		{
			segno = segfile_array[i]->segno;
			modcount = segfile_array[i]->modcount;
			logicalEof = segfile_array[i]->eof;
			
			elog(yezzey_log_level, "offloading segment no %d, modcount %ld up to logial eof %ld", segno, modcount, logicalEof);

			rc = offloadRelationSegment(aorel, aorel->rd_node, segno, modcount, logicalEof);
			if (rc < 0) {
				elog(ERROR, "failed to offload segment number %d, modcount %ld, up to %ld", segno, modcount, logicalEof);
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

		// rc = offloadRelationSegment(aorel->rd_node, 0, 0);
		// if (rc < 0) {
		// 	elog(ERROR, "failed to offload segment number %d", 0);
		// }

		for (inat = 0; inat < nvp; ++ inat) {
			for (i = 0; i < total_segfiles; i++)
			{
				segno = segfile_array_cs[i]->segno;
				/* in AOCS case actual *segno* differs from segfile_array_cs[i]->segno
				* whis is logical number of segment. On physical level, each logical
				* segno (segfile_array_cs[i]->segno) is represented by AOTupleId_MultiplierSegmentFileNum
				* in storage (1 file per attribute)  */
				pseudosegno = (inat * AOTupleId_MultiplierSegmentFileNum) + segno;
				modcount = segfile_array_cs[i]->modcount;
				logicalEof = segfile_array_cs[i]->vpinfo.entry[inat].eof;
				elog(yezzey_ao_log_level, "offloading cs segment no %d, pseudosegno %d, modcount %ld, up to eof %ld", segno, pseudosegno, modcount, logicalEof);

				rc = offloadRelationSegment(aorel, aorel->rd_node, pseudosegno, modcount, logicalEof);
				if (rc < 0) {
					elog(ERROR, "failed to offload cs segment number %d, pseudosegno %d, up to %ld", segno, pseudosegno, logicalEof);
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
	//yezzey_log_smgroffload(&aorel->rd_node);
	//smgrcreate(aorel->rd_smgr, YEZZEY_FORKNUM, false);

	smgrclose(aorel->rd_smgr);
	aorel->rd_smgr = NULL;

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


Datum
yezzey_get_relation_offload_per_filesegment_status(PG_FUNCTION_ARGS)
{
	Oid reloid;
	Relation aorel;
	int i;
	int segno;
	int total_segfiles;
	int64 modcount;
	int64 logicalEof;
	FileSegInfo **segfile_array;
	Snapshot appendOnlyMetaDataSnapshot;
	FuncCallContext *funcctx;
	MemoryContext oldcontext;
	AttInMetadata *attinmeta;
	int32		call_cntr;

	reloid = PG_GETARG_OID(0);

	/*  This mode guarantees that the holder is the only transaction accessing the table in any way. 
	* we need to be sure, thar no other transaction either reads or write to given relation
	* because we are going to delete relation from local storage
	*/
	aorel = relation_open(reloid, AccessShareLock);

	/* GetAllFileSegInfo_pg_aoseg_rel */

	/* acquire snapshot for aoseg table lookup */
	appendOnlyMetaDataSnapshot = SnapshotSelf;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL()) {
		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/*
		 * switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);


		if (aorel->rd_rel->relstorage == 'a') {
			/* ao rows relation */
			segfile_array = GetAllFileSegInfo(aorel, appendOnlyMetaDataSnapshot, &total_segfiles);

			/*
			* Build a tuple descriptor for our result type
			* The number and type of attributes have to match the definition of the
			* view yezzey_get_relation_offload_status
			*/
#define NUM_USED_OFFLOAD_PER_SEGMENT_STATUS 6
			funcctx->tuple_desc = CreateTemplateTupleDesc(NUM_USED_OFFLOAD_PER_SEGMENT_STATUS, false);

			TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber) 1, "reloid",
					OIDOID, -1 /* typmod */, 0 /* attdim */);
			TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber) 2, "segindex",
					INT4OID, -1 /* typmod */, 0 /* attdim */);
			TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber) 3, "segfileindex",
					INT4OID, -1 /* typmod */, 0 /* attdim */);
			TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber) 4, "local_bytes",
					INT8OID, -1 /* typmod */, 0 /* attdim */);
			TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber) 5, "local_commited_bytes",
					INT8OID, -1 /* typmod */, 0 /* attdim */);
			TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber) 6, "external_bytes",
					INT8OID, -1 /* typmod */, 0 /* attdim */);

			funcctx->tuple_desc =  BlessTupleDesc(funcctx->tuple_desc);
		} else {
			elog(ERROR, "wrong rel");
		}

		/*
		 * Generate attribute metadata needed later to produce tuples from raw
		 * C strings
		 */
		attinmeta = TupleDescGetAttInMetadata(funcctx->tuple_desc);
		funcctx->attinmeta = attinmeta;

		if (total_segfiles > 0)
		{
			funcctx->max_calls = total_segfiles;
			funcctx->user_fctx = segfile_array;
			/* funcctx->user_fctx */
		}
		else
		{
			/* fast track when no results */
			MemoryContextSwitchTo(oldcontext);
			relation_close(aorel, AccessShareLock);
			SRF_RETURN_DONE(funcctx);
		}

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;

	attinmeta = funcctx->attinmeta;
	
	if (call_cntr == funcctx->max_calls) {
		/* no pfree on segfile_array because context will be destroyed */
		relation_close(aorel, AccessShareLock);
		/* do when there is no more left */
		SRF_RETURN_DONE(funcctx);
	}

	segfile_array = funcctx->user_fctx;

	if (aorel->rd_rel->relstorage == 'a') {
		/* ao rows relation */

		size_t local_bytes = 0;
		size_t external_bytes = 0;
		size_t local_commited_bytes = 0;

		i = call_cntr;

		segno = segfile_array[i]->segno;
		modcount = segfile_array[i]->modcount;
		logicalEof = segfile_array[i]->eof;
		
		elog(yezzey_log_level, "stat segment no %d, modcount %ld with to logial eof %ld", segno, modcount, logicalEof);
		size_t curr_local_bytes = 0;
		size_t curr_external_bytes = 0;
		size_t curr_local_commited_bytes = 0;

		if (statRelationSpaceUsage(aorel->rd_node, segno, modcount, logicalEof, &curr_local_bytes, &curr_local_commited_bytes, &curr_external_bytes) < 0) {
			elog(ERROR, "failed to stat segment %d usage", segno);
		}

		local_bytes = curr_local_bytes;
		external_bytes = curr_external_bytes;
		local_commited_bytes = curr_local_commited_bytes;
		/* segment if loaded */
	

		Datum		values[NUM_USED_OFFLOAD_PER_SEGMENT_STATUS];
		bool		nulls[NUM_USED_OFFLOAD_PER_SEGMENT_STATUS];
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = ObjectIdGetDatum(reloid);
		values[1] = Int32GetDatum(GpIdentity.segindex);
		values[2] = Int32GetDatum(i);
		values[3] = Int64GetDatum(local_bytes);
		values[4] = Int64GetDatum(local_commited_bytes);
		values[5] = Int64GetDatum(external_bytes);

		HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		Datum result = HeapTupleGetDatum(tuple);

		relation_close(aorel, AccessShareLock);
		
		SRF_RETURN_NEXT(funcctx, result);
	} else {
		elog(ERROR, "wrong rel");
	}
	PG_RETURN_VOID();
}


Datum
yezzey_get_relation_offload_status(PG_FUNCTION_ARGS)
{
	Oid reloid;
	Relation aorel;
	int i;
	int segno;
	int total_segfiles;
	int64 modcount;
	int64 logicalEof;
	FileSegInfo **segfile_array;
	Snapshot appendOnlyMetaDataSnapshot;

	reloid = PG_GETARG_OID(0);

	/*  This mode guarantees that the holder is the only transaction accessing the table in any way. 
	* we need to be sure, thar no other transaction either reads or write to given relation
	* because we are going to delete relation from local storage
	*/
	aorel = relation_open(reloid, AccessShareLock);

	/* GetAllFileSegInfo_pg_aoseg_rel */

	/* acquire snapshot for aoseg table lookup */
	appendOnlyMetaDataSnapshot = SnapshotSelf;

	if (aorel->rd_rel->relstorage == 'a') {
		/* ao rows relation */
		segfile_array = GetAllFileSegInfo(aorel, appendOnlyMetaDataSnapshot, &total_segfiles);

		size_t local_bytes = 0;
		size_t external_bytes = 0;
		size_t local_commited_bytes = 0;

		for (i = 0; i < total_segfiles; i++)
		{
			segno = segfile_array[i]->segno;
			modcount = segfile_array[i]->modcount;
			logicalEof = segfile_array[i]->eof;
			
			elog(yezzey_log_level, "stat segment no %d, modcount %ld with to logial eof %ld", segno, modcount, logicalEof);
			size_t curr_local_bytes = 0;
			size_t curr_external_bytes = 0;
			size_t curr_local_commited_bytes = 0;

			if (statRelationSpaceUsage(aorel->rd_node, segno, modcount, logicalEof, &curr_local_bytes, &curr_local_commited_bytes, &curr_external_bytes) < 0) {
				elog(ERROR, "failed to stat segment %d usage", segno);
			}

			local_bytes += curr_local_bytes;
			external_bytes += curr_external_bytes;
			local_commited_bytes += curr_local_commited_bytes;
			/* segment if loaded */
		}


		/*
		* Build a tuple descriptor for our result type
		* The number and type of attributes have to match the definition of the
		* view yezzey_get_relation_offload_status
		*/
#define NUM_USED_OFFLOAD_STATUS 5
		TupleDesc tupdesc = CreateTemplateTupleDesc(NUM_USED_OFFLOAD_STATUS, false);

		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "reloid",
				OIDOID, -1 /* typmod */, 0 /* attdim */);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "segindex",
				INT4OID, -1 /* typmod */, 0 /* attdim */);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "local_bytes",
				INT8OID, -1 /* typmod */, 0 /* attdim */);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "local_commited_bytes",
				INT8OID, -1 /* typmod */, 0 /* attdim */);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "external_bytes",
				INT8OID, -1 /* typmod */, 0 /* attdim */);

		tupdesc =  BlessTupleDesc(tupdesc);

		Datum		values[NUM_USED_OFFLOAD_STATUS];
		bool		nulls[NUM_USED_OFFLOAD_STATUS];
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = ObjectIdGetDatum(reloid);
		values[1] = Int32GetDatum(GpIdentity.segindex);
		values[2] = Int64GetDatum(local_bytes);
		values[3] = Int64GetDatum(local_commited_bytes);
		values[4] = Int64GetDatum(external_bytes);

		HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
		Datum result = HeapTupleGetDatum(tuple);

		if (segfile_array)
		{
			FreeAllSegFileInfo(segfile_array, total_segfiles);
			pfree(segfile_array);
		}

		relation_close(aorel, AccessShareLock);
		PG_RETURN_DATUM(result);
	} else {
		elog(ERROR, "wrong rel");
	}
	PG_RETURN_VOID();
}