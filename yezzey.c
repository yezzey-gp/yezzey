
#include "postgres.h"
#include "fmgr.h"

#include "catalog/dependency.h"
#include "catalog/pg_extension.h"
#include "commands/extension.h"

#include "executor/spi.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "access/xlog.h"
#include "catalog/storage_xlog.h"
#include "common/relpath.h"
#include "funcapi.h"

#include "access/aocssegfiles.h"
#include "access/aosegfiles.h"
#include "storage/lmgr.h"
#include "utils/tqual.h"

#include "utils/guc.h"

// #include "smgr_s3_frontend.h"

// For GpIdentity
#include "c.h"
#include "catalog/pg_namespace.h"
#include "cdb/cdbvars.h"
#include "utils/catcache.h"
#include "utils/syscache.h"


#include "yezzey.h"

#include "storage.h"
#include "util.h"


#define GET_STR(textp)                                                         \
  DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(textp)))

/* STORAGE */
char *storage_prefix = NULL;
char *storage_bucket = NULL;
char *storage_config = NULL;
char *storage_host = NULL;

/* GPG */
char *gpg_engine_path = NULL;
char *gpg_key_id = NULL;

bool use_gpg_crypto = false;

/* WAL-G */
char *walg_bin_path;


PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(yezzey_offload_relation);
PG_FUNCTION_INFO_V1(yezzey_load_relation);
PG_FUNCTION_INFO_V1(yezzey_force_segment_offload);
PG_FUNCTION_INFO_V1(yezzey_offload_relation_status_internal);
PG_FUNCTION_INFO_V1(yezzey_offload_relation_status_per_filesegment);
PG_FUNCTION_INFO_V1(
    yezzey_relation_describe_external_storage_structure_internal);
PG_FUNCTION_INFO_V1(yezzey_define_relation_offload_policy_internal);
PG_FUNCTION_INFO_V1(yezzey_offload_relation_to_external_path);
PG_FUNCTION_INFO_V1(yezzey_show_relation_external_path);

void yezzey_log_smgroffload(RelFileNode *rnode);

/*
 * Perform XLogInsert of a XLOG_SMGR_CREATE record to WAL.
 */
void yezzey_log_smgroffload(RelFileNode *rnode) {
  xl_smgr_create xlrec;
  XLogRecData rdata;

  /*
   * Make an XLOG entry reporting the file creation.
   */
  xlrec.rnode = *rnode;
  xlrec.forkNum = YEZZEY_FORKNUM;

  rdata.data = (char *)&xlrec;
  rdata.len = sizeof(xlrec);
  rdata.buffer = InvalidBuffer;
  rdata.next = NULL;

  XLogInsert(RM_SMGR_ID, XLOG_SMGR_CREATE, &rdata);
}

Datum yezzey_define_relation_offload_policy_internal(PG_FUNCTION_ARGS) {
  Oid reloid;
  Oid yezzey_ext_oid;
  ObjectAddress relationAddr, extensionAddr;

  reloid = PG_GETARG_OID(0);

  relationAddr.classId = RelationRelationId;
  relationAddr.objectId = reloid;
  relationAddr.objectSubId = 0;

  yezzey_ext_oid = get_extension_oid("yezzey", false);

  if (!yezzey_ext_oid) {
    elog(ERROR, "failed to get yezzey extnsion oid");
  }

  extensionAddr.classId = ExtensionRelationId;
  extensionAddr.objectId = yezzey_ext_oid;
  extensionAddr.objectSubId = 0;

  elog(yezzey_log_level, "recording dependency on yezzey for relation %d",
       reloid);

  /*
   * OK, add the dependency.
   */
  // recordDependencyOn(&relationAddr, &extensionAddr, DEPENDENCY_EXTENSION);
  recordDependencyOn(&extensionAddr, &relationAddr, DEPENDENCY_NORMAL);
  // recordDependencyOn(&extensionAddr, &relationAddr, DEPENDENCY_INTERNAL);

  PG_RETURN_VOID();
}

int yezzey_offload_relation_internal(Oid reloid, bool remove_locally,
                                     const char *external_storage_path) {
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

  /*  This mode guarantees that the holder is the only transaction accessing the
   * table in any way. we need to be sure, thar no other transaction either
   * reads or write to given relation because we are going to delete relation
   * from local storage
   */
  aorel = relation_open(reloid, AccessExclusiveLock);
  RelationOpenSmgr(aorel);
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
    segfile_array =
        GetAllFileSegInfo(aorel, appendOnlyMetaDataSnapshot, &total_segfiles);

    for (i = 0; i < total_segfiles; i++) {
      segno = segfile_array[i]->segno;
      modcount = segfile_array[i]->modcount;
      logicalEof = segfile_array[i]->eof;

      elog(yezzey_log_level,
           "offloading segment no %d, modcount %ld up to logial eof %ld", segno,
           modcount, logicalEof);

      rc = offloadRelationSegment(aorel, segno, modcount,
                                  logicalEof, remove_locally,
                                  external_storage_path);

      if (rc < 0) {
        elog(ERROR,
             "failed to offload segment number %d, modcount %ld, up to %ld",
             segno, modcount, logicalEof);
      }
      /* segment if offloaded */
    }

    if (segfile_array) {
      FreeAllSegFileInfo(segfile_array, total_segfiles);
      pfree(segfile_array);
    }
  } else {
    /* ao columns, relstorage == 'c' */
    segfile_array_cs = GetAllAOCSFileSegInfo(aorel, appendOnlyMetaDataSnapshot,
                                             &total_segfiles);

    for (inat = 0; inat < nvp; ++inat) {
      for (i = 0; i < total_segfiles; i++) {
        segno = segfile_array_cs[i]->segno;
        /* in AOCS case actual *segno* differs from segfile_array_cs[i]->segno
         * whis is logical number of segment. On physical level, each logical
         * segno (segfile_array_cs[i]->segno) is represented by
         * AOTupleId_MultiplierSegmentFileNum in storage (1 file per attribute)
         */
        pseudosegno = (inat * AOTupleId_MultiplierSegmentFileNum) + segno;
        modcount = segfile_array_cs[i]->modcount;
        logicalEof = segfile_array_cs[i]->vpinfo.entry[inat].eof;
        elog(yezzey_ao_log_level,
             "offloading cs segment no %d, pseudosegno %d, modcount %ld, up to "
             "eof %ld",
             segno, pseudosegno, modcount, logicalEof);

        rc = offloadRelationSegment(aorel, pseudosegno,
                                    modcount, logicalEof, remove_locally,
                                    external_storage_path);

        if (rc < 0) {
          elog(ERROR,
               "failed to offload cs segment number %d, pseudosegno %d, up to "
               "%ld",
               segno, pseudosegno, logicalEof);
        }
        /* segment if offloaded */
      }
    }

    if (segfile_array_cs) {
      FreeAllAOCSSegFileInfo(segfile_array_cs, total_segfiles);
      pfree(segfile_array_cs);
    }
  }

  /* insert entry in relocate table, is no any */

  /* cleanup */
  // yezzey_log_smgroffload(&aorel->rd_node);
  // smgrcreate(aorel->rd_smgr, YEZZEY_FORKNUM, false);

  smgrclose(aorel->rd_smgr);
  aorel->rd_smgr = NULL;

  relation_close(aorel, AccessExclusiveLock);

  return 0;
}

int yezzey_load_relation_internal(Oid reloid, const char *dest_path) {
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
    segfile_array =
        GetAllFileSegInfo(aorel, appendOnlyMetaDataSnapshot, &total_segfiles);

    for (i = 0; i < total_segfiles; i++) {
      segno = segfile_array[i]->segno;
      elog(yezzey_log_level, "loading segment no %d", segno);

      rc = loadRelationSegment(aorel, segno, dest_path);
      if (rc < 0) {
        elog(ERROR, "failed to offload segment number %d", segno);
      }
      /* segment if loaded */
    }

    if (segfile_array) {
      FreeAllSegFileInfo(segfile_array, total_segfiles);
      pfree(segfile_array);
    }
  } else {
    /* ao columns, relstorage == 'c' */
    segfile_array_cs = GetAllAOCSFileSegInfo(aorel, appendOnlyMetaDataSnapshot,
                                             &total_segfiles);

    for (inat = 0; inat < nvp; ++inat) {
      for (i = 0; i < total_segfiles; i++) {
        segno = segfile_array_cs[i]->segno;
        pseudosegno = (inat * AOTupleId_MultiplierSegmentFileNum) + segno;
        elog(yezzey_log_level, "loading cs segment no %d pseudosegno %d", segno,
             pseudosegno);

        rc = loadRelationSegment(aorel, pseudosegno, dest_path);
        if (rc < 0) {
          elog(ERROR, "failed to load cs segment number %d pseudosegno %d",
               segno, pseudosegno);
        }
        /* segment if loaded */
      }
    }

    if (segfile_array_cs) {
      FreeAllAOCSSegFileInfo(segfile_array_cs, total_segfiles);
      pfree(segfile_array_cs);
    }
  }

  /* cleanup */

  relation_close(aorel, AccessExclusiveLock);

  return 0;
}

Datum yezzey_load_relation(PG_FUNCTION_ARGS) {
  /*
   * Force table offloading to external storage
   * In order:
   * 1) lock table in IN EXCLUSIVE MODE (is that needed?)
   * 2) check pg_aoseg.pg_aoseg_XXX table for all segments
   * 3) go and load each segment (XXX: enhancement: do loading in parallel)
   */
  Oid reloid;
  int rc;
  char *dest_path;

  reloid = PG_GETARG_OID(0);
  dest_path = GET_STR(PG_GETARG_TEXT_P(2));

  rc = yezzey_load_relation_internal(reloid, dest_path);
  if (rc) {
    elog(ERROR, "failed to load relation (oid=%d) files to path %s", reloid, dest_path);
  }

  PG_RETURN_VOID();
}

Datum yezzey_offload_relation(PG_FUNCTION_ARGS) {
  /*
   * Force table offloading to external storage
   * In order:
   * 1) lock table in IN EXCLUSIVE MODE
   * 2) check pg_aoseg.pg_aoseg_XXX table for all segments
   * 3) go and offload each segment (XXX: enhancement: do offloading in
   * parallel)
   */
  Oid reloid;
  bool remove_locally;
  int rc;

  reloid = PG_GETARG_OID(0);
  remove_locally = PG_GETARG_BOOL(1);

  rc = yezzey_offload_relation_internal(reloid, remove_locally, NULL);

  PG_RETURN_VOID();
}

Datum yezzey_force_segment_offload(PG_FUNCTION_ARGS) { PG_RETURN_VOID(); }

Datum yezzey_offload_relation_to_external_path(PG_FUNCTION_ARGS) {
  /*
   * Force table offloading to external storage
   * In order:
   * 1) lock table in IN EXCLUSIVE MODE
   * 2) check pg_aoseg.pg_aoseg_XXX table for all segments
   * 3) go and offload each segment (XXX: enhancement: do offloading in
   * parallel)
   */
  Oid reloid;
  bool remove_locally;
  const char *external_path;
  int rc;

  reloid = PG_GETARG_OID(0);
  remove_locally = PG_GETARG_BOOL(1);
  external_path = GET_STR(PG_GETARG_TEXT_P(2));

  rc = yezzey_offload_relation_internal(reloid, remove_locally, external_path);

  PG_RETURN_VOID();
}

Datum yezzey_show_relation_external_path(PG_FUNCTION_ARGS) {
  Oid reloid;
  int rc;
  Relation aorel;
  StringInfoData local_path;
  RelFileNode rnode;
  int32 segno;
  char *pgptr;
  char *ptr;
  HeapTuple tp;
  char *nspname;

  reloid = PG_GETARG_OID(0);
  segno = PG_GETARG_OID(1);

  aorel = relation_open(reloid, AccessShareLock);

  rnode = aorel->rd_node;

  initStringInfo(&local_path);
  appendStringInfo(&local_path, "base/%d/%d.%d", rnode.dbNode, rnode.relNode,
                   segno);

  tp = SearchSysCache1(NAMESPACEOID,
                       ObjectIdGetDatum(aorel->rd_rel->relnamespace));

  if (HeapTupleIsValid(tp)) {
    Form_pg_namespace nsptup = (Form_pg_namespace)GETSTRUCT(tp);
    nspname = pstrdup(NameStr(nsptup->nspname));
    ReleaseSysCache(tp);
  } else {
    elog(ERROR, "yezzey: failed to get namescape name of relation %s",
         aorel->rd_rel->relname.data);
  }

  getYezzeyExternalStoragePath(nspname, aorel->rd_rel->relname.data,
                               storage_host /*host*/, storage_bucket /*bucket*/,
                               storage_prefix /*prefix*/, local_path.data,
                               GpIdentity.segindex, &ptr);

  pgptr = pstrdup(ptr);
  free(ptr);
  pfree(nspname);

  relation_close(aorel, AccessShareLock);

  PG_RETURN_TEXT_P(cstring_to_text(pgptr));
}

Datum yezzey_offload_relation_status_per_filesegment(PG_FUNCTION_ARGS) {
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
  int32 call_cntr;

  reloid = PG_GETARG_OID(0);

  /*  This mode guarantees that the holder is the only transaction accessing the
   * table in any way. we need to be sure, thar no other transaction either
   * reads or write to given relation because we are going to delete relation
   * from local storage
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
      segfile_array =
          GetAllFileSegInfo(aorel, appendOnlyMetaDataSnapshot, &total_segfiles);

      /*
       * Build a tuple descriptor for our result type
       * The number and type of attributes have to match the definition of the
       * view yezzey_offload_relation_status_internal
       */
#define NUM_USED_OFFLOAD_PER_SEGMENT_STATUS 6
      funcctx->tuple_desc =
          CreateTemplateTupleDesc(NUM_USED_OFFLOAD_PER_SEGMENT_STATUS, false);

      TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)1, "reloid", OIDOID,
                         -1 /* typmod */, 0 /* attdim */);
      TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)2, "segindex",
                         INT4OID, -1 /* typmod */, 0 /* attdim */);
      TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)3, "segfileindex",
                         INT4OID, -1 /* typmod */, 0 /* attdim */);
      TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)4, "local_bytes",
                         INT8OID, -1 /* typmod */, 0 /* attdim */);
      TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)5,
                         "local_commited_bytes", INT8OID, -1 /* typmod */,
                         0 /* attdim */);
      TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)6, "external_bytes",
                         INT8OID, -1 /* typmod */, 0 /* attdim */);

      funcctx->tuple_desc = BlessTupleDesc(funcctx->tuple_desc);
    } else {
      elog(ERROR, "wrong rel");
    }

    /*
     * Generate attribute metadata needed later to produce tuples from raw
     * C strings
     */
    attinmeta = TupleDescGetAttInMetadata(funcctx->tuple_desc);
    funcctx->attinmeta = attinmeta;

    if (total_segfiles > 0) {
      funcctx->max_calls = total_segfiles;
      funcctx->user_fctx = segfile_array;
      /* funcctx->user_fctx */
    } else {
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

    elog(yezzey_log_level,
         "stat segment no %d, modcount %ld with to logial eof %ld", segno,
         modcount, logicalEof);
    size_t curr_local_bytes = 0;
    size_t curr_external_bytes = 0;
    size_t curr_local_commited_bytes = 0;

    if (statRelationSpaceUsage(aorel, segno, modcount, logicalEof,
                               &curr_local_bytes, &curr_local_commited_bytes,
                               &curr_external_bytes) < 0) {
      elog(ERROR, "failed to stat segment %d usage", segno);
    }

    local_bytes = curr_local_bytes;
    external_bytes = curr_external_bytes;
    local_commited_bytes = curr_local_commited_bytes;
    /* segment if loaded */

    Datum values[NUM_USED_OFFLOAD_PER_SEGMENT_STATUS];
    bool nulls[NUM_USED_OFFLOAD_PER_SEGMENT_STATUS];
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

typedef struct yezzeyChunkMetaInfo {
  Oid reloid;                            // "reloid" column
  int32_t segindex;                      // "segindex" column
  int32_t segfileindex;                  // "segfileindex" column
  const char *external_storage_filepath; // "external_storage_filepath" column
  int64_t local_bytes;                   // "local_bytes" column
  int64_t local_commited_bytes;          // "local_commited_bytes" column
  int64_t external_bytes;                // "external_bytes" column
} yezzeyChunkMetaInfo;

Datum yezzey_relation_describe_external_storage_structure_internal(
    PG_FUNCTION_ARGS) {
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
  int32 call_cntr;
  yezzeyChunkMetaInfo *chunkInfo;
  chunkInfo = palloc(sizeof(yezzeyChunkMetaInfo));

  reloid = PG_GETARG_OID(0);

  /*  This mode guarantees that the holder is the only transaction accessing the
   * table in any way. we need to be sure, thar no other transaction either
   * reads or write to given relation because we are going to delete relation
   * from local storage
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

    size_t total_row = 0;

    if (aorel->rd_rel->relstorage == 'a') {
      /* ao rows relation */
      segfile_array =
          GetAllFileSegInfo(aorel, appendOnlyMetaDataSnapshot, &total_segfiles);
      size_t local_bytes = 0;
      size_t external_bytes = 0;
      size_t local_commited_bytes = 0;

      for (i = 0; i < total_segfiles; ++i) {

        segno = segfile_array[i]->segno;
        modcount = segfile_array[i]->modcount;
        logicalEof = segfile_array[i]->eof;

        elog(yezzey_log_level,
             "stat segment no %d, modcount %ld with to logial eof %ld", segno,
             modcount, logicalEof);
        size_t curr_local_bytes = 0;
        size_t curr_external_bytes = 0;
        size_t curr_local_commited_bytes = 0;
        yezzeyChunkMeta *list;
        size_t cnt_chunks;

        if (statRelationSpaceUsagePerExternalChunk(
                aorel, segno, modcount, logicalEof, &curr_local_bytes,
                &curr_local_commited_bytes, &list, &cnt_chunks) < 0) {
          elog(ERROR, "failed to stat segment %d usage", segno);
        }

        local_bytes = curr_local_bytes;
        external_bytes = curr_external_bytes;
        local_commited_bytes = curr_local_commited_bytes;

        chunkInfo = repalloc(chunkInfo, sizeof(yezzeyChunkMetaInfo) *
                                            (total_row + cnt_chunks));

        for (size_t chunk_index = 0; chunk_index < cnt_chunks; ++chunk_index) {
          chunkInfo[total_row + chunk_index].reloid = reloid;
          chunkInfo[total_row + chunk_index].segindex = GpIdentity.segindex;
          chunkInfo[total_row + chunk_index].segfileindex = i;
          chunkInfo[total_row + chunk_index].external_storage_filepath =
              list[chunk_index].chunkName;
          chunkInfo[total_row + chunk_index].local_bytes = local_bytes;
          chunkInfo[total_row + chunk_index].local_commited_bytes =
              local_commited_bytes;
          chunkInfo[total_row + chunk_index].external_bytes =
              list[chunk_index].chunkSize;
        }
        total_row += cnt_chunks;
      }

      /*
       * Build a tuple descriptor for our result type
       * The number and type of attributes have to match the definition of the
       * view yezzey_offload_relation_status_internal
       */
#define NUM_USED_OFFLOAD_PER_SEGMENT_STATUS 7
      funcctx->tuple_desc =
          CreateTemplateTupleDesc(NUM_USED_OFFLOAD_PER_SEGMENT_STATUS, false);

      TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)1, "reloid", OIDOID,
                         -1 /* typmod */, 0 /* attdim */);
      TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)2, "segindex",
                         INT4OID, -1 /* typmod */, 0 /* attdim */);
      TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)3, "segfileindex",
                         INT4OID, -1 /* typmod */, 0 /* attdim */);
      TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)4,
                         "external_storage_filepath", TEXTOID, -1 /* typmod */,
                         0 /* attdim */);
      TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)5, "local_bytes",
                         INT8OID, -1 /* typmod */, 0 /* attdim */);
      TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)6,
                         "local_commited_bytes", INT8OID, -1 /* typmod */,
                         0 /* attdim */);
      TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)7, "external_bytes",
                         INT8OID, -1 /* typmod */, 0 /* attdim */);

      funcctx->tuple_desc = BlessTupleDesc(funcctx->tuple_desc);
    } else {
      elog(ERROR, "yezzey: wrong rel");
    }

    /*
     * Generate attribute metadata needed later to produce tuples from raw
     * C strings
     */
    attinmeta = TupleDescGetAttInMetadata(funcctx->tuple_desc);
    funcctx->attinmeta = attinmeta;

    if (total_row > 0) {
      funcctx->max_calls = total_row;
      funcctx->user_fctx = chunkInfo;
      /* funcctx->user_fctx */
    } else {
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

  chunkInfo = funcctx->user_fctx;

  if (aorel->rd_rel->relstorage == 'a') {
    /* ao rows relation */

    i = call_cntr;

    /* segment if loaded */
    Datum values[NUM_USED_OFFLOAD_PER_SEGMENT_STATUS];
    bool nulls[NUM_USED_OFFLOAD_PER_SEGMENT_STATUS];
    MemSet(nulls, 0, sizeof(nulls));

    values[0] = ObjectIdGetDatum(chunkInfo[i].reloid);
    values[1] = Int32GetDatum(GpIdentity.segindex);
    values[2] = Int32GetDatum(chunkInfo[i].segfileindex);
    values[3] = CStringGetTextDatum(chunkInfo[i].external_storage_filepath);
    values[4] = Int64GetDatum(chunkInfo[i].local_bytes);
    values[5] = Int64GetDatum(chunkInfo[i].local_commited_bytes);
    values[6] = Int64GetDatum(chunkInfo[i].external_bytes);

    HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
    Datum result = HeapTupleGetDatum(tuple);
    relation_close(aorel, AccessShareLock);

    SRF_RETURN_NEXT(funcctx, result);

  } else {
    elog(ERROR, "yezzey: wrong rel");
  }
  PG_RETURN_VOID();
}

Datum yezzey_offload_relation_status_internal(PG_FUNCTION_ARGS) {
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

  /*  This mode guarantees that the holder is the only transaction accessing the
   * table in any way. we need to be sure, thar no other transaction either
   * reads or write to given relation because we are going to delete relation
   * from local storage
   */
  aorel = relation_open(reloid, AccessShareLock);

  /* GetAllFileSegInfo_pg_aoseg_rel */

  /* acquire snapshot for aoseg table lookup */
  appendOnlyMetaDataSnapshot = SnapshotSelf;

  if (aorel->rd_rel->relstorage == 'a') {
    /* ao rows relation */
    segfile_array =
        GetAllFileSegInfo(aorel, appendOnlyMetaDataSnapshot, &total_segfiles);

    size_t local_bytes = 0;
    size_t external_bytes = 0;
    size_t local_commited_bytes = 0;

    for (i = 0; i < total_segfiles; i++) {
      segno = segfile_array[i]->segno;
      modcount = segfile_array[i]->modcount;
      logicalEof = segfile_array[i]->eof;

      elog(yezzey_log_level,
           "yezzey: stat segment no %d, modcount %ld with to logial eof %ld",
           segno, modcount, logicalEof);
      size_t curr_local_bytes = 0;
      size_t curr_external_bytes = 0;
      size_t curr_local_commited_bytes = 0;

      if (statRelationSpaceUsage(aorel, segno, modcount, logicalEof,
                                 &curr_local_bytes, &curr_local_commited_bytes,
                                 &curr_external_bytes) < 0) {
        elog(ERROR, "yezzey: failed to stat segment %d usage", segno);
      }

      local_bytes += curr_local_bytes;
      external_bytes += curr_external_bytes;
      local_commited_bytes += curr_local_commited_bytes;
      /* segment if loaded */
    }

    /*
     * Build a tuple descriptor for our result type
     * The number and type of attributes have to match the definition of the
     * view yezzey_offload_relation_status_internal
     */
#define NUM_USED_OFFLOAD_STATUS 5
    TupleDesc tupdesc = CreateTemplateTupleDesc(NUM_USED_OFFLOAD_STATUS, false);

    TupleDescInitEntry(tupdesc, (AttrNumber)1, "reloid", OIDOID,
                       -1 /* typmod */, 0 /* attdim */);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "segindex", INT4OID,
                       -1 /* typmod */, 0 /* attdim */);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "local_bytes", INT8OID,
                       -1 /* typmod */, 0 /* attdim */);
    TupleDescInitEntry(tupdesc, (AttrNumber)4, "local_commited_bytes", INT8OID,
                       -1 /* typmod */, 0 /* attdim */);
    TupleDescInitEntry(tupdesc, (AttrNumber)5, "external_bytes", INT8OID,
                       -1 /* typmod */, 0 /* attdim */);

    tupdesc = BlessTupleDesc(tupdesc);

    Datum values[NUM_USED_OFFLOAD_STATUS];
    bool nulls[NUM_USED_OFFLOAD_STATUS];
    MemSet(nulls, 0, sizeof(nulls));

    values[0] = ObjectIdGetDatum(reloid);
    values[1] = Int32GetDatum(GpIdentity.segindex);
    values[2] = Int64GetDatum(local_bytes);
    values[3] = Int64GetDatum(local_commited_bytes);
    values[4] = Int64GetDatum(external_bytes);

    HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
    Datum result = HeapTupleGetDatum(tuple);

    if (segfile_array) {
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