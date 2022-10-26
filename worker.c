

#include "postgres.h"
#include "fmgr.h"

#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>

#include "utils/snapmgr.h"
#include "miscadmin.h"

#include "external_storage.h"

#if PG_VERSION_NUM >= 130000
#include "postmaster/interrupt.h"
#endif

#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "executor/spi.h"
#include "storage/smgr.h"
#include "storage/md.h"
#include "common/relpath.h"
#include "catalog/catalog.h"

#if PG_VERSION_NUM >= 100000
#include "common/file_perm.h"
#else
#include "access/xact.h"
#endif

#include "utils/guc.h"
#include "lib/stringinfo.h"
#include "postmaster/bgworker.h"
#include "storage/proc.h"
#include "storage/latch.h"
#include "pgstat.h"

#include "utils/elog.h"

#ifdef GPBUILD
#include "cdb/cdbvars.h"
#endif

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


// options for yezzey logging
static const struct config_enum_entry loglevel_options[] = {
        {"debug5", DEBUG5, false},
        {"debug4", DEBUG4, false},
        {"debug3", DEBUG3, false},
        {"debug2", DEBUG2, false},
        {"debug1", DEBUG1, false},
        {"debug", DEBUG2, true},
        {"info", INFO, false},
        {"notice", NOTICE, false},
        {"warning", WARNING, false},
        {"error", ERROR, false},
        {"log", LOG, false},
        {"fatal", FATAL, false},
        {"panic", PANIC, false},
        {NULL, 0, false}
};

int yezzey_bgworker_bootstrap(void);
void offloadExpiredRealtions(void);

void
yezzey_prepare(void)
{
	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "initializing yezzey schema");
	SetCurrentStatementStartTimestamp();
}

void
yezzey_finish(void)
{
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
	pgstat_report_stat(true);
}

static void
yezzey_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;
	got_sigterm = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);
	errno = save_errno;
}

static void
yezzey_sighup(SIGNAL_ARGS)
{
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
const char * yezzeyBoostrapSchemaName = "yezzey_boostrap";
const char * yezzeyRelocateTableName = "relocate_table";
/* TODO: make this guc*/
const int processTableLimit = 10;

void processTables(void)
{
	int ret, i;
	long unsigned int cnt;
	TupleDesc tupdesc;
	SPITupleTable *tuptable;
	HeapTuple tuple;
	char * tableOidRaw;
	Oid tableOid;
	StringInfoData buf;
	int rc;
	
	elog(yezzey_log_level, "[YEZZEY_SMGR_BG] putting unprocessed tables in relocate table");

	yezzey_prepare();

	/*construct query string */

	initStringInfo(&buf);

	appendStringInfo(&buf, "SELECT * FROM %s.%s WHERE expiration_date <= NOW()", yezzeyBoostrapSchemaName, yezzeyRelocateTableName);

	ret = SPI_execute(buf.data, true, processTableLimit);	
	if (ret != SPI_OK_SELECT) {
		elog(FATAL, "[YEZZEY_SMGR_BG] Error while trying to get unprocessed tables");
	}

	tupdesc = SPI_tuptable->tupdesc;
    tuptable = SPI_tuptable;
	cnt = SPI_processed;
	
	elog(yezzey_log_level, "[YEZZEY_SMGR_BG] got %lu results", cnt);

	for (i = 0; i < cnt; i++)
	{
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

		if ((rc = offload_relation_internal(tableOid)) < 0) {
			elog(yezzey_log_level, "[YEZZEY_SMGR_BG] got %u for relation %d", rc, tableOid);
		}
	}
	
	yezzey_finish();
}

void offloadExpiredRealtions(void)
{
	int ret, i;
	long unsigned int cnt;
	TupleDesc tupdesc;
	SPITupleTable *tuptable;
	HeapTuple tuple;
	char * tableOidRaw;
	Oid tableOid;
	int rc;
	
	elog(yezzey_log_level, "[YEZZEY_SMGR_BG] putting unprocessed tables in relocate table");

	yezzey_prepare();

	ret = SPI_execute("SELECT * FROM yezzey.auto_offload_relations WHERE expire_date <= NOW()", true, processTableLimit);	
	if (ret != SPI_OK_SELECT) {
		elog(FATAL, "[YEZZEY_SMGR_BG] Error while trying to get unprocessed tables");
	}

	tupdesc = SPI_tuptable->tupdesc;
    tuptable = SPI_tuptable;
	cnt = SPI_processed;
	
	elog(yezzey_log_level, "[YEZZEY_SMGR_BG] got %lu results", cnt);

	for (i = 0; i < cnt; i++)
	{
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

		if ((rc = offload_relation_internal(tableOid)) < 0) {
			elog(yezzey_log_level, "[YEZZEY_SMGR_BG] got %u for relation %d", rc, tableOid);
		} else {
			elog(yezzey_log_level, "[YEZZEY_SMGR_BG] %s offloaded to external storage due expiration", tableOidRaw);
        }
	}
	
	yezzey_finish();
}


/*
* we use relocate table, witch is created like this:
*  CREATE TABLE yezzey.relocate_table(relNode OID, segno INT, last_access_time TIMESTAMP)
* to track last modification time for each logical segment in offloading tables
*/


int 
yezzey_bgworker_bootstrap(void) {
    int ret;
    StringInfoData buf;

	/* start transaction */
	yezzey_prepare();
	/* 
	* CREATE relocate table if not yet
	*/
    initStringInfo(&buf);
    appendStringInfo(&buf, ""
	"SELECT * FROM pg_catalog.pg_tables "
            "WHERE schemaname = '%s' AND tablename = '%s'", yezzeyBoostrapSchemaName, yezzeyRelocateTableName);

    pgstat_report_activity(STATE_RUNNING, buf.data);
    ret = SPI_execute(buf.data, true, 1);
    if (ret != SPI_OK_SELECT) {
        elog(FATAL, "Error while yezzey relocate table lookup");
	}

	/* reinitialize for reuse  */
	pfree(buf.data);
    initStringInfo(&buf);

    if (!SPI_processed) {
		/* 
		* we got 0 rows from catalog lookup, meaning there is no
		* relocate table exists;
		*/
		/* 
		* XXX: we create this auxiliar table on each
		* greenplum QE, which is not indended 
		* bgworker behaviour in greenplum
		*/
		/* XXX: do we need index on this? */


        appendStringInfo(&buf, ""
		"CREATE SCHEMA %s", yezzeyBoostrapSchemaName);

        pgstat_report_activity(STATE_RUNNING, buf.data);
        ret = SPI_execute(buf.data, false, 0);

        if (ret != SPI_OK_UTILITY) {
            elog(FATAL, "Error while creating yezzey relocate table");
		}
		/*  */
		pfree(buf.data);
        initStringInfo(&buf);

        appendStringInfo(&buf, ""
		"CREATE TABLE "
			"%s.%s("
				"relNode OID, "
				"expiration_date DATE);", yezzeyBoostrapSchemaName, yezzeyRelocateTableName);

        pgstat_report_activity(STATE_RUNNING, buf.data);
        ret = SPI_execute(buf.data, false, 0);

        if (ret != SPI_OK_UTILITY) {
            elog(FATAL, "Error while creating yezzey relocate table");
		}
    }


	pfree(buf.data);
	/* commit transaction, release transaction snapshot */
	yezzey_finish();

	return 0;
}

#if PG_VERSION_NUM < 100000
static void
#else
void
#endif
yezzey_main	(Datum main_arg)
{
	pqsignal(SIGHUP, yezzey_sighup);
	pqsignal(SIGTERM, yezzey_sigterm);

	BackgroundWorkerUnblockSignals();

#ifdef GPBUILD
	if (IS_QUERY_DISPATCHER())
	{
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
	BackgroundWorkerInitializeConnection("yezzey", NULL);
#else
	BackgroundWorkerInitializeConnection("yezzey", NULL, 0);
#endif

	/* boostrap: create work table if needed. */
	yezzey_bgworker_bootstrap();
	
	while (!got_sigterm)
	{
		int rc;
		
		elog(yezzey_log_level, "[YEZZEY_SMGR_BG] waiting for processing tables");
				
		rc = WaitLatch(&MyProc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
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
			ProcessConfigFile(PGC_SIGHUP);
			got_sighup = false;
			ereport(LOG, (errmsg("bgworker yezzey signal: processed SIGHUP")));
		}

		if (got_sigterm) {
			ereport(LOG, (errmsg("bgworker yezzey signal: processed SIGTERM")));
			proc_exit(0);
		}
		
		if (yezzey_naptime > 0) {
			offloadExpiredRealtions();
		}
		
		elog(yezzey_log_level, "[YEZZEY_SMGR_BG] waiting for sending tables");
	}
}

void
_PG_init(void)
{
	BackgroundWorker worker;

	/* XXX: yezzey naptime interval should be here */

	DefineCustomStringVariable("yezzey.S3_getter",
				   "getting file from S3",
				   NULL, &s3_getter,
				   "",PGC_USERSET,0,
				   NULL, NULL, NULL);

	DefineCustomStringVariable("yezzey.S3_putter",
                                   "putting file to S3",
                                   NULL, &s3_putter,
                                   "",PGC_USERSET,0,
                                   NULL, NULL, NULL);

	DefineCustomStringVariable("yezzey.S3_prefix",
				   "segment name prefix",
				   NULL, &s3_prefix,
				   "",PGC_USERSET,0,
				   NULL, NULL, NULL);

	DefineCustomEnumVariable("yezzey.log_level",
				 "Log level for yezzey functions.",
				 NULL,&yezzey_log_level,
				 LOG,loglevel_options,PGC_SUSET,
				 0, NULL, NULL, NULL);
	DefineCustomEnumVariable("yezzey.ao_log_level",
				 "Log level for yezzey functions.",
				 NULL, &yezzey_ao_log_level,
				 WARNING,loglevel_options,PGC_SUSET,
				 0, NULL, NULL, NULL);

	elog(yezzey_log_level, "[YEZZEY_SMGR] setting up bgworker");

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;


#if PG_VERSION_NUM < 100000
	worker.bgw_main = yezzey_main;
#endif

	snprintf(worker.bgw_name, BGW_MAXLEN, "%s", worker_name);
#if PG_VERSION_NUM >= 110000
	snprintf(worker.bgw_type, BGW_MAXLEN, "yezzey");
#endif

#if PG_VERSION_NUM >= 100000
	sprintf(worker.bgw_library_name, "yezzey");
	sprintf(worker.bgw_function_name, "yezzey_main");
#endif
	
	worker.bgw_restart_time = 10;
	worker.bgw_main_arg = (Datum) 0;

#if PG_VERSION_NUM >= 90400
	worker.bgw_notify_pid = 0;
#endif

	RegisterBackgroundWorker(&worker);

	elog(yezzey_log_level, "[YEZZEY_SMGR] set hook");
	
	smgr_hook = smgr_yezzey;
#if GPBUILD
	smgrao_hook = smgrao_yezzey;
#endif
	smgr_init_hook = smgr_init_yezzey;
}
