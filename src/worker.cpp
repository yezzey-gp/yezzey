
#include "worker.h"
#include "gucs.h"
#include "offload.h"
#include "offload_policy.h"


/*
* processOffloadedRelations:
* check all expired relations and move then to 
* external storage
*/
void processOffloadedRelations(Oid dboid) {
  HeapTuple tup;
  int rc;

  HeapScanDesc desc;
  ScanKeyData skey[1];

  auto offrel = heap_open(YEZZEY_OFFLOAD_POLICY_RELATION, AccessShareLock);

  /*
  * We cannot open yezzey offloaded relations metatable
  * in scheme 'yezzey'. This is possible when
  * this code executes on bgworker, which
  * executes on shared library loading, BEFORE
  * CREATE EXTENSION command was done.
  */
  if (!RelationIsValid(offrel)) {
    return;
  }
  auto snap = RegisterSnapshot(GetTransactionSnapshot());

  {
    ScanKeyInit(&skey[0], Anum_offload_metadata_relext_time,
                BTGreaterEqualStrategyNumber, F_TIMESTAMP_GT,
                TimestampTzGetDatum(GetCurrentTimestamp()));

    desc = heap_beginscan(offrel, snap, 0, NULL);

    while (HeapTupleIsValid(tup = heap_getnext(desc, ForwardScanDirection))) {
      auto meta = (Form_yezzey_offload_metadata)GETSTRUCT(tup);

      /**
       * @brief traverse pg_aoseg.pg_aoseg_<oid> relation
       * and try to lock each segment. If there is any transaction
       * in progress accessing some segment, lock attemt will fail,
       * because transactions holds locks on pg_aoseg.pg_aoseg_<oid> rows
       * and we are goind to lock row in X mode
       */
      if ((rc = yezzey_offload_relation_internal(meta->reloid, false, NULL)) <
          0) {
        elog(yezzey_log_level,
             "[YEZZEY_SMGR_BG] offloading relation (oid=%d) result: %d",
             meta->reloid, rc);
      }
    }

    heap_endscan(desc);
  }

  UnregisterSnapshot(snap);
  heap_close(offrel, AccessShareLock);
}