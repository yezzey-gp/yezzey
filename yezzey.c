
#include "postgres.h"

#include "catalog/dependency.h"
#include "catalog/pg_extension.h"
#include "commands/extension.h"

#include "access/xlog.h"
#include "catalog/storage_xlog.h"
#include "common/relpath.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "pgstat.h"
#include "utils/builtins.h"

#include "access/aocssegfiles.h"
#include "access/aosegfiles.h"
#include "storage/lmgr.h"
#include "utils/tqual.h"

#include "utils/fmgroids.h"

#include "catalog/catalog.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_tablespace.h"
#include "catalog/storage.h"

#include "access/xact.h"
#include "catalog/indexing.h"

#include "utils/guc.h"

#include "fmgr.h"

// #include "smgr_s3_frontend.h"

// For GpIdentity
#include "c.h"
#include "catalog/pg_namespace.h"
#include "cdb/cdbvars.h"
#include "utils/catcache.h"
#include "utils/syscache.h"

#include "catalog/heap.h"
#include "catalog/pg_namespace.h"

#include "nodes/primnodes.h"

#include "yezzey.h"

#include "storage.h"
#include "util.h"
#include "virtual_index.h"

#include "catalog/oid_dispatch.h"

#include "offload.h"
#include "offload_policy.h"

#include "virtual_tablespace.h"

#include "partition.h"

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
char *walg_bin_path = NULL;
char *walg_config_path = NULL;

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(yezzey_offload_relation);
PG_FUNCTION_INFO_V1(yezzey_load_relation);
PG_FUNCTION_INFO_V1(yezzey_load_relation_seg);
PG_FUNCTION_INFO_V1(yezzey_force_segment_offload);
PG_FUNCTION_INFO_V1(yezzey_offload_relation_status_internal);
PG_FUNCTION_INFO_V1(yezzey_offload_relation_status_per_filesegment);
PG_FUNCTION_INFO_V1(
    yezzey_relation_describe_external_storage_structure_internal);
PG_FUNCTION_INFO_V1(yezzey_define_relation_offload_policy_internal);
PG_FUNCTION_INFO_V1(yezzey_define_relation_offload_policy_internal_seg);
PG_FUNCTION_INFO_V1(yezzey_offload_relation_to_external_path);
PG_FUNCTION_INFO_V1(yezzey_show_relation_external_path);
PG_FUNCTION_INFO_V1(yezzey_init_metadata_seg);
PG_FUNCTION_INFO_V1(yezzey_init_metadata);
PG_FUNCTION_INFO_V1(yezzey_set_relation_expirity_seg);
PG_FUNCTION_INFO_V1(yezzey_check_part_exr);

/*
 * Perform XLogInsert of a XLOG_SMGR_CREATE record to WAL.
 */
void yezzey_log_smgroffload(RelFileNode *rnode);

Datum yezzey_init_metadata(PG_FUNCTION_ARGS) {
  (void)YezzeyOffloadPolicyRelation();
  PG_RETURN_VOID();
}

Datum yezzey_init_metadata_seg(PG_FUNCTION_ARGS) {
  return yezzey_init_metadata(fcinfo);
}

int yezzey_offload_relation_internal(Oid reloid, bool remove_locally,
                                     const char *external_storage_path);

/*
 * yezzey_define_relation_offload_policy_internal:
 * do all the work with initial relation offloading
 */
Datum yezzey_define_relation_offload_policy_internal(PG_FUNCTION_ARGS) {
  (void)YezzeyDefineOffloadPolicy(PG_GETARG_OID(0));
  PG_RETURN_VOID();
}

/*
 * yezzey_define_relation_offload_policy_internal_seg:
 * Follow the query dispatcher logic
 */
Datum yezzey_define_relation_offload_policy_internal_seg(PG_FUNCTION_ARGS) {
  return yezzey_define_relation_offload_policy_internal(fcinfo);
}

/*
 * yezzey_load_relation_internal:
 * TBD: doc the logic
 */

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
  Oid origrelfilenode;
  int rc;

  /*  XXX: maybe fix lock here to take more granular lock
   */
  aorel = relation_open(reloid, AccessExclusiveLock);
  nvp = aorel->rd_att->natts;
  origrelfilenode = aorel->rd_rel->relfilenode;

  /*
   * Relation segments named base/DBOID/aorel->rd_node.*
   */

  elog(yezzey_log_level, "loading relnode %d", aorel->rd_node.relNode);

  /* for now, we locked relation */

  /* GetAllFileSegInfo_pg_aoseg_rel */

  /* acquire snapshot for aoseg table lookup */
  appendOnlyMetaDataSnapshot = SnapshotSelf;
  /*sanity check */
  if (aorel->rd_node.spcNode != YEZZEYTABLESPACE_OID) {
    /* shoulde never happen*/
    elog(ERROR, "attempted to load non-offloaded relation");
  }

  /* Perform actual deletion of yezzey virtual index and metadata changes */
  (void)YezzeyATExecSetTableSpace(aorel, reloid, DEFAULTTABLESPACE_OID);

  /* Get information about all the file segments we need to scan */
  if (aorel->rd_rel->relstorage == 'a') {
    /* ao rows relation */
    segfile_array =
        GetAllFileSegInfo(aorel, appendOnlyMetaDataSnapshot, &total_segfiles);

    for (i = 0; i < total_segfiles; i++) {
      segno = segfile_array[i]->segno;
      elog(yezzey_log_level, "loading segment no %d", segno);

      rc = loadRelationSegment(aorel, origrelfilenode, segno, dest_path);
      if (rc < 0) {
        elog(ERROR, "failed to offload segment number %d", segno);
      }
      /* segment if loaded */
    }

    if (segfile_array) {
      FreeAllSegFileInfo(segfile_array, total_segfiles);
      pfree(segfile_array);
    }
  } else if (aorel->rd_rel->relstorage == 'c') {
    /* ao columns, relstorage == 'c' */
    segfile_array_cs = GetAllAOCSFileSegInfo(aorel, appendOnlyMetaDataSnapshot,
                                             &total_segfiles);

    for (inat = 0; inat < nvp; ++inat) {
      for (i = 0; i < total_segfiles; i++) {
        segno = segfile_array_cs[i]->segno;
        pseudosegno = (inat * AOTupleId_MultiplierSegmentFileNum) + segno;
        elog(yezzey_log_level, "loading cs segment no %d pseudosegno %d", segno,
             pseudosegno);

        rc =
            loadRelationSegment(aorel, origrelfilenode, pseudosegno, dest_path);
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
  } else {
    elog(ERROR, "not an AO/AOCS relation, no action performed");
  }

  /*
  Do not drop, just empty
    object.classId = RelationRelationId;
    object.objectId = YezzeyFindAuxIndex(reloid);
    object.objectSubId = 0;
    performDeletion(&object, DROP_CASCADE, PERFORM_DELETION_INTERNAL);
  */

  /* empty all track info */
  (void)emptyYezzeyIndex(YezzeyFindAuxIndex(reloid));

  /* cleanup */

  relation_close(aorel, AccessExclusiveLock);

  return 0;
}

Datum yezzey_load_relation(PG_FUNCTION_ARGS) {
  /*
   * Force table loading from external storage
   * In order:
   * 1) lock table in IN EXCLUSIVE MODE (is that needed?)
   * 2) check pg_aoseg.pg_aoseg_XXX table for all segments
   * 3) go and load each segment (XXX: enhancement: do loading in parallel)
   */
  Oid reloid;
  int rc;
  char *dest_path = NULL;

  reloid = PG_GETARG_OID(0);
  dest_path = GET_STR(PG_GETARG_TEXT_P(1));

  rc = yezzey_load_relation_internal(reloid, NULL);
  if (rc) {
    elog(ERROR, "failed to load relation (oid=%d) files to path %s", reloid,
         dest_path);
  }

  PG_RETURN_VOID();
}

Datum yezzey_load_relation_seg(PG_FUNCTION_ARGS) {
  return yezzey_load_relation(fcinfo);
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
  Relation aorel;
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

  (void)getYezzeyExternalStoragePathByCoords(
      nspname, aorel->rd_rel->relname.data, storage_host /*host*/,
      storage_bucket /*bucket*/, storage_prefix /*prefix*/, rnode.dbNode,
      rnode.relNode, segno, GpIdentity.segindex, &ptr);

  pgptr = pstrdup(ptr);
  free(ptr);
  pfree(nspname);

  relation_close(aorel, AccessShareLock);

  PG_RETURN_TEXT_P(cstring_to_text(pgptr));
}

/**
 * @brief yezzey_offload_relation_status_per_filesegment:
 * List relation external storage usage per filesegment(block)
 */
Datum yezzey_offload_relation_status_per_filesegment(PG_FUNCTION_ARGS) {
  Oid reloid;
  Relation aorel;
  int i;
  int segno;
  int total_segfiles;
  int64 modcount;
  int64 logicalEof;
  int pseudosegno;
  int inat;
  int nvp;
  FileSegInfo **segfile_array;
  AOCSFileSegInfo **segfile_array_cs;
  Snapshot appendOnlyMetaDataSnapshot;
  FuncCallContext *funcctx;
  MemoryContext oldcontext;
  AttInMetadata *attinmeta;
  int32 call_cntr;

  reloid = PG_GETARG_OID(0);
  segfile_array_cs = NULL;
  segfile_array = NULL;

  /*  This mode guarantees that the holder is the only transaction accessing the
   * table in any way. we need to be sure, thar no other transaction either
   * reads or write to given relation because we are going to delete relation
   * from local storage
   */
  aorel = relation_open(reloid, AccessShareLock);
  nvp = aorel->rd_att->natts;

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
    } else if (aorel->rd_rel->relstorage == 'c') {
      segfile_array_cs = GetAllAOCSFileSegInfo(
          aorel, appendOnlyMetaDataSnapshot, &total_segfiles);
    } else {
      elog(ERROR, "wrong relation storage type, not AO/AOCS relation");
    }

    /*
     * Build a tuple descriptor for our result type
     * The number and type of attributes have to match the definition of the
     * view yezzey_offload_relation_status_internal
     */
    funcctx->tuple_desc =
        CreateTemplateTupleDesc(NUM_USED_OFFLOAD_PER_SEGMENT_STATUS, false);

    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)1, "reloid", OIDOID,
                       -1 /* typmod */, 0 /* attdim */);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)2, "segindex", INT4OID,
                       -1 /* typmod */, 0 /* attdim */);
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

    /*
     * Generate attribute metadata needed later to produce tuples from raw
     * C strings
     */
    attinmeta = TupleDescGetAttInMetadata(funcctx->tuple_desc);
    funcctx->attinmeta = attinmeta;

    if (total_segfiles > 0) {

      if (aorel->rd_rel->relstorage == 'a') {
        funcctx->max_calls = total_segfiles;
        funcctx->user_fctx = segfile_array;
      } else if (aorel->rd_rel->relstorage == 'c') {
        funcctx->max_calls = total_segfiles * nvp;
        funcctx->user_fctx = segfile_array_cs;
      } else {
        Assert(false);
      }
      /* funcctx->user_fctx */
    } else {
      /* fast path when no results */
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

  size_t local_bytes = 0;
  size_t external_bytes = 0;
  size_t local_commited_bytes = 0;
  if (aorel->rd_rel->relstorage == 'a') {
    /* ao rows relation */

    segfile_array = funcctx->user_fctx;

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

  } else if (aorel->rd_rel->relstorage == 'c') {

    segfile_array_cs = funcctx->user_fctx;

    i = call_cntr / nvp;
    inat = call_cntr % nvp;

    segno = segfile_array_cs[i]->segno;
    /* in AOCS case actual *segno* differs from segfile_array_cs[i]->segno
     * whis is logical number of segment. On physical level, each logical
     * segno (segfile_array_cs[i]->segno) is represented by
     * AOTupleId_MultiplierSegmentFileNum in storage (1 file per attribute)
     */
    pseudosegno = (inat * AOTupleId_MultiplierSegmentFileNum) + segno;
    modcount = segfile_array_cs[i]->modcount;
    logicalEof = segfile_array_cs[i]->vpinfo.entry[inat].eof;

    elog(yezzey_log_level,
         "stat segment no %d, modcount %ld with to logial eof %ld", segno,
         modcount, logicalEof);
    size_t curr_local_bytes = 0;
    size_t curr_external_bytes = 0;
    size_t curr_local_commited_bytes = 0;

    if (statRelationSpaceUsage(aorel, pseudosegno, modcount, logicalEof,
                               &curr_local_bytes, &curr_local_commited_bytes,
                               &curr_external_bytes) < 0) {
      elog(ERROR, "failed to stat segment block %d usage", pseudosegno);
    }

    local_bytes = curr_local_bytes;
    external_bytes = curr_external_bytes;
    local_commited_bytes = curr_local_commited_bytes;
  } else {
    elog(ERROR, "wrong relation storage type, not AO/AOCS");
  }

  /* segment if loaded */
  Datum values[NUM_USED_OFFLOAD_PER_SEGMENT_STATUS];
  bool nulls[NUM_USED_OFFLOAD_PER_SEGMENT_STATUS];

  MemSet(nulls, 0, sizeof(nulls));

  values[0] = ObjectIdGetDatum(reloid);
  values[1] = Int32GetDatum(GpIdentity.segindex);

  if (aorel->rd_rel->relstorage == 'a') {
    values[2] = Int32GetDatum(segno);
  } else if (aorel->rd_rel->relstorage == 'c') {
    values[2] = Int32GetDatum(pseudosegno);
  }
  values[3] = Int64GetDatum(local_bytes);
  values[4] = Int64GetDatum(local_commited_bytes);
  values[5] = Int64GetDatum(external_bytes);

  HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
  Datum result = HeapTupleGetDatum(tuple);

  relation_close(aorel, AccessShareLock);

  SRF_RETURN_NEXT(funcctx, result);
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

/*
 * yezzey_relation_describe_external_storage_structure_internal:
 * List yezzey external storage detailed infomation, includeing
 * names of external storage relation chunks
 */
Datum yezzey_relation_describe_external_storage_structure_internal(
    PG_FUNCTION_ARGS) {
  Oid reloid;
  Relation aorel;
  int i;
  int segno;
  int pseudosegno;
  int total_segfiles;
  int nvp;
  int inat;
  int64 modcount;
  int64 logicalEof;
  FileSegInfo **segfile_array;
  AOCSFileSegInfo **segfile_array_cs;
  Snapshot appendOnlyMetaDataSnapshot;
  FuncCallContext *funcctx;
  MemoryContext oldcontext;
  AttInMetadata *attinmeta;
  int32 call_cntr;
  yezzeyChunkMetaInfo *chunkInfo;
  chunkInfo = palloc(sizeof(yezzeyChunkMetaInfo));

  reloid = PG_GETARG_OID(0);

  segfile_array_cs = NULL;
  segfile_array = NULL;

  /*  This mode guarantees that the holder is the only transaction accessing the
   * table in any way. we need to be sure, thar no other transaction either
   * reads or write to given relation because we are going to delete relation
   * from local storage
   */
  aorel = relation_open(reloid, AccessShareLock);

  nvp = aorel->rd_att->natts;

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
    size_t local_bytes = 0;
    size_t external_bytes = 0;
    size_t local_commited_bytes = 0;

    if (aorel->rd_rel->relstorage == 'a') {
      /* ao rows relation */
      segfile_array =
          GetAllFileSegInfo(aorel, appendOnlyMetaDataSnapshot, &total_segfiles);

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

    } else if (aorel->rd_rel->relstorage == 'c') {

      segfile_array_cs = GetAllAOCSFileSegInfo(
          aorel, appendOnlyMetaDataSnapshot, &total_segfiles);

      for (inat = 0; inat < nvp; ++inat) {
        for (i = 0; i < total_segfiles; i++) {
          segno = segfile_array_cs[i]->segno;
          /* in AOCS case actual *segno* differs from segfile_array_cs[i]->segno
           * whis is logical number of segment. On physical level, each logical
           * segno (segfile_array_cs[i]->segno) is represented by
           * AOTupleId_MultiplierSegmentFileNum in storage (1 file per
           * attribute)
           */
          pseudosegno = (inat * AOTupleId_MultiplierSegmentFileNum) + segno;
          modcount = segfile_array_cs[i]->modcount;
          logicalEof = segfile_array_cs[i]->vpinfo.entry[inat].eof;

          size_t curr_local_bytes = 0;
          size_t curr_external_bytes = 0;
          size_t curr_local_commited_bytes = 0;
          yezzeyChunkMeta *list;
          size_t cnt_chunks;

          if (statRelationSpaceUsagePerExternalChunk(
                  aorel, pseudosegno, modcount, logicalEof, &curr_local_bytes,
                  &curr_local_commited_bytes, &list, &cnt_chunks) < 0) {
            elog(ERROR, "failed to stat segment %d usage", segno);
          }

          local_bytes = curr_local_bytes;
          external_bytes = curr_external_bytes;
          local_commited_bytes = curr_local_commited_bytes;

          chunkInfo = repalloc(chunkInfo, sizeof(yezzeyChunkMetaInfo) *
                                              (total_row + cnt_chunks));

          for (size_t chunk_index = 0; chunk_index < cnt_chunks;
               ++chunk_index) {
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
      }
    } else {

      elog(ERROR, "yezzey: wrong relation storage type: not AO/AOCS relation");
    }

    funcctx->tuple_desc = CreateTemplateTupleDesc(
        NUM_USED_OFFLOAD_PER_SEGMENT_STATUS_STRUCT, false);

    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)1, "reloid", OIDOID,
                       -1 /* typmod */, 0 /* attdim */);
    TupleDescInitEntry(funcctx->tuple_desc, (AttrNumber)2, "segindex", INT4OID,
                       -1 /* typmod */, 0 /* attdim */);
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

  i = call_cntr;

  /* segment if loaded */
  Datum values[NUM_USED_OFFLOAD_PER_SEGMENT_STATUS_STRUCT];
  bool nulls[NUM_USED_OFFLOAD_PER_SEGMENT_STATUS_STRUCT];
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
}

/*
 * yezzey_offload_relation_status_internal:
 * List total segment(block) files statistic for relation
 * Includes info abount segment(block) external storage usage
 * (which may differ from logical offset (sic!)) and local stogare
 * usage. Urgent: for now, local stogare usage should be 0 since no
 * cache-logic implemented.
 */
Datum yezzey_offload_relation_status_internal(PG_FUNCTION_ARGS) {
  Oid reloid;
  Relation aorel;
  int i;
  int segno;
  int inat;
  int nvp;
  int pseudosegno;
  int total_segfiles;
  int64 modcount;
  int64 logicalEof;
  FileSegInfo **segfile_array;
  AOCSFileSegInfo **segfile_array_cs;
  Snapshot appendOnlyMetaDataSnapshot;
  TupleDesc tupdesc;

  segfile_array = NULL;
  segfile_array_cs = NULL;

  reloid = PG_GETARG_OID(0);

  /*
   * Lock table in share mode
   */
  aorel = relation_open(reloid, AccessShareLock);

  nvp = aorel->rd_att->natts;

  /* GetAllFileSegInfo_pg_aoseg_rel */

  /* acquire snapshot for aoseg table lookup */
  appendOnlyMetaDataSnapshot = SnapshotSelf;

  size_t local_bytes = 0;
  size_t external_bytes = 0;
  size_t local_commited_bytes = 0;

  if (aorel->rd_rel->relstorage == 'a') {
    /* ao rows relation */
    segfile_array =
        GetAllFileSegInfo(aorel, appendOnlyMetaDataSnapshot, &total_segfiles);

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
  } else if (aorel->rd_rel->relstorage == 'c') {
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

        elog(yezzey_log_level,
             "yezzey: stat segment no %d, pseudosegno %d, modcount %ld with to "
             "logial eof %ld",
             segno, pseudosegno, modcount, logicalEof);
        size_t curr_local_bytes = 0;
        size_t curr_external_bytes = 0;
        size_t curr_local_commited_bytes = 0;

        if (statRelationSpaceUsage(
                aorel, pseudosegno, modcount, logicalEof, &curr_local_bytes,
                &curr_local_commited_bytes, &curr_external_bytes) < 0) {
          elog(ERROR, "yezzey: failed to stat segment %d usage", segno);
        }

        local_bytes += curr_local_bytes;
        external_bytes += curr_external_bytes;
        local_commited_bytes += curr_local_commited_bytes;
        /* segment if offloaded */
      }
    }
  } else {
    elog(ERROR, "wrong relation type (not AO/AOCS) storage");
  }

  /*
   * Build a tuple descriptor for our result type
   * The number and type of attributes have to match the definition of the
   * view yezzey_offload_relation_status_internal
   */
  tupdesc = CreateTemplateTupleDesc(NUM_YEZZEY_OFFLOAD_STATE_COLS, false);

  TupleDescInitEntry(tupdesc, (AttrNumber)1, "reloid", OIDOID, -1 /* typmod */,
                     0 /* attdim */);
  TupleDescInitEntry(tupdesc, (AttrNumber)2, "segindex", INT4OID,
                     -1 /* typmod */, 0 /* attdim */);
  TupleDescInitEntry(tupdesc, (AttrNumber)3, "local_bytes", INT8OID,
                     -1 /* typmod */, 0 /* attdim */);
  TupleDescInitEntry(tupdesc, (AttrNumber)4, "local_commited_bytes", INT8OID,
                     -1 /* typmod */, 0 /* attdim */);
  TupleDescInitEntry(tupdesc, (AttrNumber)5, "external_bytes", INT8OID,
                     -1 /* typmod */, 0 /* attdim */);

  tupdesc = BlessTupleDesc(tupdesc);

  Datum values[NUM_YEZZEY_OFFLOAD_STATE_COLS];
  bool nulls[NUM_YEZZEY_OFFLOAD_STATE_COLS];

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

  if (segfile_array_cs) {
    FreeAllAOCSSegFileInfo(segfile_array_cs, total_segfiles);
    pfree(segfile_array_cs);
  }

  relation_close(aorel, AccessShareLock);
  PG_RETURN_DATUM(result);
}

Datum yezzey_set_relation_expirity_seg(PG_FUNCTION_ARGS) {
  /* INSERT INFO yezzey.offload_metadata */
  /*
    0: i_reloid OID,
    1: i_relpolicy INT,
    2: i_relexp TIMESTAMP
  */
  Oid i_reloid;
  int i_relpolicy;
  Timestamp i_relexp;

  i_reloid = PG_GETARG_OID(0);
  i_relpolicy = PG_GETARG_INT32(1);
  i_relexp = PG_GETARG_TIMESTAMP(2);

  (void)YezzeySetRelationExpiritySeg(
      i_reloid, i_relpolicy /* always external */, i_relexp);

  PG_RETURN_VOID();
}

/* partition - related worker routines */

/*
 * yezzey_check_part_exr
 */
Datum yezzey_check_part_exr(PG_FUNCTION_ARGS) {
  /*
    0: i_expr pg_node_tree,
  */
  text *expr = PG_GETARG_TEXT_P(0);
  PG_RETURN_TEXT_P(yezzey_get_expr_worker(expr));
}
