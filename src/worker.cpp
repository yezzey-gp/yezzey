
#include "worker.h"
#include "gucs.h"
#include "offload.h"
#include "offload_policy.h"

#include "pg.h"

void processPartitionOffload() {
  SetCurrentStatementStartTimestamp();
  StartTransactionCommand();
  SPI_connect();
  PushActiveSnapshot(GetTransactionSnapshot());
  pgstat_report_activity(STATE_RUNNING, "proccess relations to offload to yezzey");
  SetCurrentStatementStartTimestamp();
  
  auto query =  
  "select yezzey_set_relation_expirity_seg("
  "     parchildrelid,"
  "     1,"
  "      yezzey_part_date_eval('select '::text || pg_get_expr(parrangeend, 0))::timestamp"
  " ) from pg_partition_rule WHERE yezzey_check_part_exr(parrangeend);";

  auto ret = SPI_execute(query, true, 1);
  if (ret != SPI_OK_SELECT) {
    elog(ERROR, "Error while processing partitions");
  }

  auto tupdesc = SPI_tuptable->tupdesc;
  auto tuptable = SPI_tuptable;
  auto cnt = SPI_processed;


  SPI_finish();
  PopActiveSnapshot();
  CommitTransactionCommand();
  pgstat_report_activity(STATE_IDLE, NULL);
  pgstat_report_stat(true);
}

/*
 * processOffloadedRelations:
 * check all expired relations and move then to
 * external storage
 */
void processOffloadedRelations() {
  /*
   * Connect to the selected database
   *
   * Note: if we have selected a just-deleted database (due to using
   * stale stats info), we'll fail and exit here.
   */

  /* And do an appropriate amount of work */
  StartTransactionCommand();
  (void)GetTransactionSnapshot();

  HeapTuple tup;

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
                BTLessEqualStrategyNumber, F_TIMESTAMP_LT,
                TimestampGetDatum(GetCurrentTimestamp()));

    auto desc = heap_beginscan(offrel, snap, 1, skey);

    while (HeapTupleIsValid(tup = heap_getnext(desc, ForwardScanDirection))) {
      auto meta = (Form_yezzey_offload_metadata)GETSTRUCT(tup);

      /**
       * @brief traverse pg_aoseg.pg_aoseg_<oid> relation
       * and try to lock each segment. If there is any transaction
       * in progress accessing some segment, lock attemt will fail,
       * because transactions holds locks on pg_aoseg.pg_aoseg_<oid> rows
       * and we are goind to lock row in X mode
       */
      elog(INFO, "process expired relation (oid=%d)", meta->reloid);
      YezzeyDefineOffloadPolicy(meta->reloid);
    }

    heap_endscan(desc);
  }

  UnregisterSnapshot(snap);
  heap_close(offrel, AccessShareLock);

  CommitTransactionCommand();
}