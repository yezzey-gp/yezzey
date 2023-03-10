

#include "postgres.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "miscadmin.h"
#include "utils/snapmgr.h"

#if PG_VERSION_NUM >= 130000
#include "postmaster/interrupt.h"
#endif

#include "catalog/catalog.h"
#include "common/relpath.h"
#include "executor/spi.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/md.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "utils/builtins.h"

#include "postmaster/bgworker.h"
#include "storage/dsm.h"

#if PG_VERSION_NUM >= 100000
#include "common/file_perm.h"
#else
#include "access/xact.h"
#endif

#include "lib/stringinfo.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/guc.h"

#include "utils/elog.h"

#ifdef GPBUILD
#include "cdb/cdbvars.h"
#endif


#include "storage.h"
#include "yezzey.h"


#if PG_VERSION_NUM >= 100000
void yezzey_main(Datum main_arg);
#else
static void yezzey_main(Datum arg);
#endif

static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup = false;

static int yezzey_naptime = 10000;
static char *worker_name = "yezzey";

/* Shared state information for yezzey bgworker. */
typedef struct YezzeySharedState {
  LWLock lock;              /* mutual exclusion */
  pid_t bgworker_pid;       /* for main bgworker */
  pid_t pid_using_dumpfile; /* for autoprewarm or block dump */

  /* Following items are for communication with per-database worker */
  dsm_handle block_info_handle;
  Oid database;
  int yezzeystart_idx;
  int yezzey_stop_idx;
} YezzeySharedState;

static bool yezzey_init_shmem(void);
void yezzey_offload_databases(void);
void yezzey_ProcessConfigFile(void);
void yezzey_process_database(Datum main_arg);
void processColdTables(void);

/* Pointer to shared-memory state. */
static YezzeySharedState *yezzey_state = NULL;

/*
 * Allocate and initialize yezzey-related shared memory, if not already
 * done, and set up backend-local pointer to that state.  Returns true if an
 * existing shared memory segment was found.
 */
static bool yezzey_init_shmem(void) {
#if PG_VERSION_NUM < 100000
  static LWLockTranche tranche;
#endif
  bool found;

  LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
  yezzey_state = ShmemInitStruct("yezzey", sizeof(YezzeySharedState), &found);
  if (!found) {
    LWLockInitialize(&yezzey_state->lock, LWLockNewTrancheId());
    yezzey_state->bgworker_pid = InvalidPid;
  }
  LWLockRelease(AddinShmemInitLock);

#if PG_VERSION_NUM >= 100000
  LWLockRegisterTranche(yezzey_state->lock.tranche, "yezzey");
#else
  tranche.name = "yezzey";
  tranche.array_base = &yezzey_state->lock;
  tranche.array_stride = sizeof(LWLock);
  LWLockRegisterTranche(yezzey_state->lock.tranche, &tranche);

#endif

  return found;
}

// options for yezzey logging
static const struct config_enum_entry loglevel_options[] = {
    {"debug5", DEBUG5, false},   {"debug4", DEBUG4, false},
    {"debug3", DEBUG3, false},   {"debug2", DEBUG2, false},
    {"debug1", DEBUG1, false},   {"debug", DEBUG2, true},
    {"info", INFO, false},       {"notice", NOTICE, false},
    {"warning", WARNING, false}, {"error", ERROR, false},
    {"log", LOG, false},         {"fatal", FATAL, false},
    {"panic", PANIC, false},     {NULL, 0, false}};

void offloadExpiredRelations(void);

void yezzey_prepare(void) {
  SetCurrentStatementStartTimestamp();
  StartTransactionCommand();
  SPI_connect();
  PushActiveSnapshot(GetTransactionSnapshot());
  pgstat_report_activity(STATE_RUNNING, "initializing yezzey schema");
  SetCurrentStatementStartTimestamp();
}

void yezzey_finish(void) {
  SPI_finish();
  PopActiveSnapshot();
  CommitTransactionCommand();
  pgstat_report_activity(STATE_IDLE, NULL);
  pgstat_report_stat(true);
}

static void yezzey_sigterm(SIGNAL_ARGS) {
  int save_errno = errno;
  got_sigterm = true;
  if (MyProc)
    SetLatch(&MyProc->procLatch);
  errno = save_errno;
}

static void yezzey_sighup(SIGNAL_ARGS) {
  int save_errno = errno;
  got_sighup = true;
  if (MyProc)
    SetLatch(&MyProc->procLatch);
  errno = save_errno;
}

/*
 * We cannot crate table in scheme 'yezzey', because
 * this boostrap executes on bgworker startup, which
 * executes on shared library loading, BEFORE
 * CREATE EXTENSION command will be executed
 */
const char *yezzeyBoostrapSchemaName = "yezzey_boostrap";
const char *yezzeyRelocateTableName = "relocate_table";

const char *yezzeySchemaName = "yezzey";
const char *yezzeyOffloadMetadataTableName = "offload_metadata";
/* TODO: make this guc*/
const int processTableLimit = 10;

void processTables(void) {
  int ret, i;
  long unsigned int cnt;
  TupleDesc tupdesc;
  SPITupleTable *tuptable;
  HeapTuple tuple;
  char *tableOidRaw;
  Oid tableOid;
  StringInfoData buf;
  int rc;

  elog(yezzey_log_level,
       "[YEZZEY_SMGR_BG] putting unprocessed tables in relocate table");

  yezzey_prepare();

  /*construct query string */

  initStringInfo(&buf);

  appendStringInfo(&buf, "SELECT * FROM %s.%s WHERE expiration_date <= NOW()",
                   yezzeyBoostrapSchemaName, yezzeyRelocateTableName);

  ret = SPI_execute(buf.data, true, processTableLimit);
  if (ret != SPI_OK_SELECT) {
    elog(FATAL,
         "[YEZZEY_SMGR_BG] Error while trying to get unprocessed tables");
  }

  tupdesc = SPI_tuptable->tupdesc;
  tuptable = SPI_tuptable;
  cnt = SPI_processed;

  elog(yezzey_log_level, "[YEZZEY_SMGR_BG] got %lu results", cnt);

  for (i = 0; i < cnt; i++) {
    /**
     * @brief traverse pg_aoseg.pg_aoseg_<oid> relation
     * and try to lock each segment. If there is any transaction
     * in progress accessing some segment, lock attemt will fail,
     * because transactions holds locks on pg_aoseg.pg_aoseg_<oid> rows
     * and we are goind to lock row in X mode
     */
    tuple = tuptable->vals[i];
    tableOidRaw = SPI_getvalue(tuple, tupdesc, 1);

    /*
     * Convert to desired data type
     */
    tableOid = strtoll(tableOidRaw, NULL, 10);

    if ((rc = yezzey_offload_relation_internal(tableOid, false, NULL)) < 0) {
      elog(yezzey_log_level, "[YEZZEY_SMGR_BG] got %u for relation %d", rc,
           tableOid);
    }
  }

  yezzey_finish();
}

void processColdTables(void) {
  int ret, i;
  long unsigned int cnt;
  TupleDesc tupdesc;
  SPITupleTable *tuptable;
  HeapTuple tuple;
  char *tableOidRaw;
  Oid tableOid;
  StringInfoData buf;
  int rc;

  elog(yezzey_log_level,
       "[YEZZEY_SMGR_BG] putting unprocessed tables in relocate table");

  yezzey_prepare();

  /*construct query string */

  initStringInfo(&buf);

  appendStringInfo(&buf,
                   "SELECT * FROM %s.%s WHERE relpolicy = 'remote_always'",
                   yezzeySchemaName, yezzeyOffloadMetadataTableName);

  ret = SPI_execute(buf.data, true, processTableLimit);
  if (ret != SPI_OK_SELECT) {
    elog(FATAL,
         "[YEZZEY_SMGR_BG] Error while trying to get unprocessed tables");
  }

  tupdesc = SPI_tuptable->tupdesc;
  tuptable = SPI_tuptable;
  cnt = SPI_processed;

  elog(yezzey_log_level, "[YEZZEY_SMGR_BG] got %lu results", cnt);

  for (i = 0; i < cnt; i++) {
    /**
     * @brief traverse pg_aoseg.pg_aoseg_<oid> relation
     * and try to lock each segment. If there is any transaction
     * in progress accessing some segment, lock attemt will fail,
     * because transactions holds locks on pg_aoseg.pg_aoseg_<oid> rows
     * and we are goind to lock row in X mode
     */
    tuple = tuptable->vals[i];
    tableOidRaw = SPI_getvalue(tuple, tupdesc, 1);

    /*
     * Convert to desired data type
     */
    tableOid = strtoll(tableOidRaw, NULL, 10);

    if ((rc = yezzey_offload_relation_internal(tableOid, false, NULL)) < 0) {
      elog(yezzey_log_level, "[YEZZEY_SMGR_BG] got %u for relation %d", rc,
           tableOid);
    }
  }

  yezzey_finish();
}

void offloadExpiredRelations(void) {
  int ret, i;
  long unsigned int cnt;
  TupleDesc tupdesc;
  SPITupleTable *tuptable;
  HeapTuple tuple;
  char *tableOidRaw;
  Oid tableOid;
  int rc;

  elog(yezzey_log_level,
       "[YEZZEY_SMGR_BG] putting unprocessed tables in relocate table");

  yezzey_prepare();

  ret = SPI_execute(
      "SELECT * FROM yezzey.auto_offload_relations WHERE expire_date <= NOW()",
      true, processTableLimit);
  if (ret != SPI_OK_SELECT) {
    elog(FATAL,
         "[YEZZEY_SMGR_BG] Error while trying to get unprocessed tables");
  }

  tupdesc = SPI_tuptable->tupdesc;
  tuptable = SPI_tuptable;
  cnt = SPI_processed;

  elog(yezzey_log_level, "[YEZZEY_SMGR_BG] got %lu results", cnt);

  for (i = 0; i < cnt; i++) {
    /**
     * @brief traverse pg_aoseg.pg_aoseg_<oid> relation
     * and try to lock each segment. If there is any transaction
     * in progress accessing some segment, lock attemt will fail,
     * because transactions holds locks on pg_aoseg.pg_aoseg_<oid> rows
     * and we are goind to lock row in X mode
     */
    tuple = tuptable->vals[i];
    tableOidRaw = SPI_getvalue(tuple, tupdesc, 1);

    /*
     * Convert to desired data type
     */
    tableOid = strtoll(tableOidRaw, NULL, 10);

    if ((rc = yezzey_offload_relation_internal(tableOid, false, NULL)) < 0) {
      elog(yezzey_log_level, "[YEZZEY_SMGR_BG] got %u for relation %d", rc,
           tableOid);
    } else {
      elog(yezzey_log_level,
           "[YEZZEY_SMGR_BG] %s offloaded to external storage due expiration",
           tableOidRaw);
    }
  }

  yezzey_finish();
}

void yezzey_process_database(Datum main_arg) {
  Oid dboid;

  dboid = DatumGetObjectId(main_arg);

  /* Establish signal handlers; once that's done, unblock signals. */
  // pqsignal(SIGTERM, die);
  BackgroundWorkerUnblockSignals();

  /*	 BackgroundWorkerInitializeConnectionByOid(dboid, InvalidOid, 0); */
  BackgroundWorkerInitializeConnectionByOid(dboid, InvalidOid);

  processColdTables();
}

/*
 * Start yezzey per-database worker process.
 */
static void yezzey_start_database_worker(Oid dboid) {
  BackgroundWorker worker;
  elog(yezzey_log_level, "[YEZZEY_SMGR] setting up bgworker");

  memset(&worker, 0, sizeof(worker));
  worker.bgw_flags =
      BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
  worker.bgw_start_time = BgWorkerStart_ConsistentState;
  worker.bgw_restart_time = BGW_NEVER_RESTART;

#if PG_VERSION_NUM < 100000
  worker.bgw_main = yezzey_process_database;
  worker.bgw_main_arg = ObjectIdGetDatum(dboid);
#endif

  snprintf(worker.bgw_name, BGW_MAXLEN, "%s-oflload worker for %d", worker_name,
           dboid);
#if PG_VERSION_NUM >= 110000
  snprintf(worker.bgw_type, BGW_MAXLEN, "yezzey");
#endif

#if PG_VERSION_NUM >= 100000
  sprintf(worker.bgw_library_name, "yezzey");
  sprintf(worker.bgw_function_name, "yezzey_process_database");
#endif

#if PG_VERSION_NUM >= 90400
  worker.bgw_notify_pid = 0;
#endif

  RegisterBackgroundWorker(&worker);

  elog(yezzey_log_level, "[YEZZEY_SMGR] started datbase worker");
}

void yezzey_offload_databases() {
  SPITupleTable *tuptable;
  HeapTuple tuple;
  char *dbOidRaw;
  Oid dbOid;
  TupleDesc tupdesc;
  long unsigned int cnt;
  int ret;

  StringInfoData buf;

  /* start transaction */
  yezzey_prepare();
  /*
   * CREATE relocate table if not yet
   */
  initStringInfo(&buf);
  appendStringInfo(&buf, ""
                         "SELECT oid FROM pg_database "
                         "ORDER BY random() LIMIT 1;");

  pgstat_report_activity(STATE_RUNNING, buf.data);
  ret = SPI_execute(buf.data, true, 1);
  if (ret != SPI_OK_SELECT) {
    elog(FATAL, "Error while yezzey relocate table lookup");
  }

  tupdesc = SPI_tuptable->tupdesc;
  tuptable = SPI_tuptable;
  cnt = SPI_processed;

  /**
   * @brief traverse pg_aoseg.pg_aoseg_<oid> relation
   * and try to lock each segment. If there is any transaction
   * in progress accessing some segment, lock attemt will fail,
   * because transactions holds locks on pg_aoseg.pg_aoseg_<oid> rows
   * and we are goind to lock row in X mode
   */
  tuple = tuptable->vals[0];
  dbOidRaw = SPI_getvalue(tuple, tupdesc, 1);

  elog(yezzey_log_level, "[YEZZEY_SMGR_BG] got %lu results, oid is %s", cnt,
       dbOidRaw);

  /*
   * Convert to desired data type
   */
  dbOid = strtoll(dbOidRaw, NULL, 10);
  /* comlete transaction */
  yezzey_finish();

  yezzey_start_database_worker(dbOid);
}

void yezzey_ProcessConfigFile() {}

static void yezzey_detach_shmem(int code, Datum arg);
/*
 * Clear our PID from autoprewarm shared state.
 */
static void yezzey_detach_shmem(int code, Datum arg) {
  LWLockAcquire(&yezzey_state->lock, LW_EXCLUSIVE);
  if (yezzey_state->bgworker_pid == MyProcPid)
    yezzey_state->bgworker_pid = InvalidPid;
  LWLockRelease(&yezzey_state->lock);
}

// Launcher worker
#if PG_VERSION_NUM < 100000
static void
#else
void
#endif
yezzey_main(Datum main_arg) {
  bool first_time;

  pqsignal(SIGHUP, yezzey_sighup);
  pqsignal(SIGTERM, yezzey_sigterm);

  BackgroundWorkerUnblockSignals();

  /* Create (if necessary) and attach to our shared memory area. */
  if (yezzey_init_shmem())
    first_time = false;

  /* Set on-detach hook so that our PID will be cleared on exit. */
  on_shmem_exit(yezzey_detach_shmem, 0);

  /*
   * Store our PID in the shared memory area --- unless there's already
   * another worker running, in which case just exit.
   */
  LWLockAcquire(&yezzey_state->lock, LW_EXCLUSIVE);
  if (yezzey_state->bgworker_pid != InvalidPid) {
    LWLockRelease(&yezzey_state->lock);
    ereport(LOG, (errmsg("yezzey worker is already running under PID %lu",
                         (unsigned long)yezzey_state->bgworker_pid)));
    return;
  }
  yezzey_state->bgworker_pid = MyProcPid;
  LWLockRelease(&yezzey_state->lock);

#ifdef GPBUILD
  if (IS_QUERY_DISPATCHER()) {
    return;
  }
#endif
  /* run only on query executers */

  /* without this, we will fail out attempts
   * to modify tables on segments (sql without QD)
   */
  Gp_role = GP_ROLE_UTILITY;
  Gp_session_role = GP_ROLE_UTILITY;

  /*
   * acquire connection to database,
   * otherwise we will not be able to run queries.
   * yezzey axillary database will contain tables wirth
   * relnodes meta-information, which can be used for
   * backgroup relation offloading to external storage.
   */
#if PG_VERSION_NUM < 110000
  BackgroundWorkerInitializeConnection("postgres", NULL);
#else
  BackgroundWorkerInitializeConnection("postgres", NULL, 0);
#endif

  while (!got_sigterm) {
    int rc;

    elog(yezzey_log_level, "[YEZZEY_SMGR_BG] waiting for processing tables");

    rc = WaitLatch(&MyProc->procLatch,
                   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
#if PG_VERSION_NUM < 100000
                   yezzey_naptime
#else
                   yezzey_naptime, PG_WAIT_EXTENSION
#endif
    );

    ResetLatch(&MyProc->procLatch);

    if (rc & WL_POSTMASTER_DEATH)
      proc_exit(1);

    if (got_sighup) {
      yezzey_ProcessConfigFile();
      got_sighup = false;
      ereport(LOG, (errmsg("bgworker yezzey signal: processed SIGHUP")));
    }

    if (got_sigterm) {
      ereport(LOG, (errmsg("bgworker yezzey signal: processed SIGTERM")));
      proc_exit(0);
    }

    elog(yezzey_log_level,
         "[YEZZEY_SMGR_BG] start to process offload databases");
    yezzey_offload_databases();
  }
}

/*
 * Start yezzey primary worker process.
 */
static void yezzey_start_launcher_worker(void) {
  BackgroundWorker worker;
  BackgroundWorkerHandle *handle;
  BgwHandleStatus status;

  pid_t pid;

  MemSet(&worker, 0, sizeof(BackgroundWorker));
  worker.bgw_flags =
      BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
  worker.bgw_start_time = BgWorkerStart_ConsistentState;

#if PG_VERSION_NUM < 100000
  worker.bgw_main = yezzey_main;
#else
  sprintf(worker.bgw_library_name, "yezzey");
  sprintf(worker.bgw_function_name, "yezzey_main");
#endif

  snprintf(worker.bgw_name, BGW_MAXLEN, "%s", worker_name);
#if PG_VERSION_NUM >= 110000
  snprintf(worker.bgw_type, BGW_MAXLEN, "yezzey");
#endif

  worker.bgw_restart_time = 10;

#if PG_VERSION_NUM >= 90400
  worker.bgw_notify_pid = 0;
#endif

  RegisterBackgroundWorker(&worker);

  if (process_shared_preload_libraries_in_progress) {
    RegisterBackgroundWorker(&worker);
    return;
  }

  /* must set notify PID to wait for startup */
  worker.bgw_notify_pid = MyProcPid;

  if (!RegisterDynamicBackgroundWorker(&worker, &handle))
    ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                    errmsg("could not register background process"),
                    errhint("You may need to increase max_worker_processes.")));

  status = WaitForBackgroundWorkerStartup(handle, &pid);
  if (status != BGWH_STARTED)
    ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
             errmsg("could not start background process"),
             errhint("More details may be available in the server log.")));
}

/* GUC variables. */
static bool yezzey_autooffload = true; /* start yezzey worker? */

void yezzey_define_gucs() {

  DefineCustomStringVariable("yezzey.storage_prefix", "segment name prefix",
                             NULL, &storage_prefix, "", PGC_SUSET, 0, NULL,
                             NULL, NULL);

  DefineCustomStringVariable("yezzey.storage_bucket", "external storage bucket",
                             NULL, &storage_bucket, "", PGC_SUSET, 0, NULL,
                             NULL, NULL);

  DefineCustomStringVariable("yezzey.storage_config",
                             "Storage config path for yezzey external storage.",
                             NULL, &storage_config, "", PGC_SUSET, 0, NULL,
                             NULL, NULL);

  DefineCustomStringVariable("yezzey.storage_host", "external storage host",
                             NULL, &storage_host, "", PGC_SUSET, 0, NULL, NULL,
                             NULL);

  DefineCustomStringVariable("yezzey.gpg_key_id", "gpg key id", NULL,
                             &gpg_key_id, "", PGC_SUSET, 0, NULL, NULL, NULL);

  DefineCustomBoolVariable("yezzey.use_gpg_crypto", "use gpg crypto", NULL,
                           &use_gpg_crypto, true, PGC_SUSET, 0, NULL, NULL,
                           NULL);

  DefineCustomStringVariable("yezzey.gpg_engine_path", "gpg engine path", NULL,
                             &gpg_engine_path, "/usr/bin/gpg", PGC_SUSET, 0,
                             NULL, NULL, NULL);

  DefineCustomBoolVariable(
      "yezzey.autooffload", "enable auto-offloading worker", NULL,
      &yezzey_autooffload, false, PGC_USERSET, 0, NULL, NULL, NULL);

  DefineCustomEnumVariable("yezzey.log_level",
                           "Log level for yezzey functions.", NULL,
                           &yezzey_log_level, DEBUG1, loglevel_options,
                           PGC_SUSET, 0, NULL, NULL, NULL);

  DefineCustomEnumVariable("yezzey.ao_log_level",
                           "Log level for yezzey functions.", NULL,
                           &yezzey_ao_log_level, DEBUG1, loglevel_options,
                           PGC_SUSET, 0, NULL, NULL, NULL);

  DefineCustomIntVariable(
      "yezzey.oflload_worker_naptime", "Auto-offloading worker naptime", NULL,
      &yezzey_naptime, 10000, 500, INT_MAX, PGC_SUSET, 0, NULL, NULL, NULL);
}

void _PG_init(void) {
  /* Allocate shared memory for yezzey workers */

  /* Yezzey GUCS define */
  (void)yezzey_define_gucs();

  RequestAddinShmemSpace(MAXALIGN(sizeof(YezzeySharedState)));

  elog(yezzey_log_level, "[YEZZEY_SMGR] setting up bgworker");

  if (yezzey_autooffload) {
    /* dispatch yezzey worker */
    yezzey_start_launcher_worker();
  }

  elog(yezzey_log_level, "[YEZZEY_SMGR] set hook");

  smgr_hook = smgr_yezzey;
#if GPBUILD
  smgrao_hook = smgrao_yezzey;
#endif
  smgr_init_hook = smgr_init_yezzey;
}
