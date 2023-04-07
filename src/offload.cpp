
#include "offload.h"
#include "gucs.h"

#include "storage.h"

/*
 * yezzey_offload_relation_internal_rel: do the offloading job
 * aorel should be locked in AccessExclusiveLock
 */
int yezzey_offload_relation_internal_rel(Relation aorel, bool remove_locally,
                                         const char *external_storage_path) {
  int total_segfiles;
  FileSegInfo** segfile_array;
  AOCSFileSegInfo** segfile_array_cs;

  auto nvp = aorel->rd_att->natts;

  /*
   * Relation segments named base/DBOID/aorel->rd_node.*
   */

  elog(yezzey_log_level, "offloading relation %s, relnode %d", aorel->rd_rel->relname.data, aorel->rd_node.relNode);

  /* for now, we locked relation */

  /* GetAllFileSegInfo_pg_aoseg_rel */  

  /* acquire snapshot for aoseg table lookup */
  auto appendOnlyMetaDataSnapshot = SnapshotSelf;

  switch (aorel->rd_rel->relstorage) {
  case RELSTORAGE_AOROWS:
    /* Get information about all the file segments we need to scan */
      segfile_array =
        GetAllFileSegInfo(aorel, appendOnlyMetaDataSnapshot, &total_segfiles);

    for (int i = 0; i < total_segfiles; i++) {
      auto segno = segfile_array[i]->segno;
      auto modcount = segfile_array[i]->modcount;
      auto logicalEof = segfile_array[i]->eof;

      elog(yezzey_log_level,
           "offloading segment no %d, modcount %ld up to logial eof %ld", segno,
           modcount, logicalEof);

      auto rc = offloadRelationSegment(aorel, segno, modcount, logicalEof,
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
    break;
  case RELSTORAGE_AOCOLS:
    /* ao columns, relstorage == 'c' */
    segfile_array_cs = GetAllAOCSFileSegInfo(
        aorel, appendOnlyMetaDataSnapshot, &total_segfiles);

    for (int inat = 0; inat < nvp; ++inat) {
      for (int i = 0; i < total_segfiles; i++) {
        auto segno = segfile_array_cs[i]->segno;
        /* in AOCS case actual *segno* differs from segfile_array_cs[i]->segno
         * whis is logical number of segment. On physical level, each logical
         * segno (segfile_array_cs[i]->segno) is represented by
         * AOTupleId_MultiplierSegmentFileNum in storage (1 file per attribute)
         */
        auto pseudosegno = (inat * AOTupleId_MultiplierSegmentFileNum) + segno;
        auto modcount = segfile_array_cs[i]->modcount;
        auto logicalEof = segfile_array_cs[i]->vpinfo.entry[inat].eof;
        elog(yezzey_ao_log_level,
             "offloading cs segment no %d, pseudosegno %d, modcount %ld, up to "
             "eof %ld",
             segno, pseudosegno, modcount, logicalEof);

        auto rc = offloadRelationSegment(aorel, pseudosegno, modcount, logicalEof,
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
    break;
  default:
    elog(ERROR, "wrong relation storage type, not AO/AOCS");
  }

  /* insert entry in relocate table, is no any */

  /* cleanup */

  return 0;
}

/*
 * yezzey_offload_relation_internal:
 * offloads relation segments data to external storage.
 * if remove_locally is true,
 * issues ATExecSetTableSpace(tablespace shange to virtual (yezzey) tablespace)
 * which will result in local-storage files drops (on both primary and mirror
 * segments)
 */
int yezzey_offload_relation_internal(Oid reloid, bool remove_locally,
                                     const char *external_storage_path) {
  Relation aorel;
  int rc;
  /* need sanity checks */

  /*  This mode guarantees that the holder is the only transaction accessing the
   * table in any way. we need to be sure, thar no other transaction either
   * reads or write to given relation because we are going to delete relation
   * from local storage
   */
  aorel = relation_open(reloid, AccessExclusiveLock);
  RelationOpenSmgr(aorel);

  rc = yezzey_offload_relation_internal_rel(aorel, remove_locally,
                                            external_storage_path);

  relation_close(aorel, AccessExclusiveLock);
  return rc;
}