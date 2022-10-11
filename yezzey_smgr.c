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

static int interval = 10000;
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
/*
* Construct external storage filepath. 
* 
* Assepts initialized StringInfoData as its first param
*/

static void
constructExtenrnalStorageFilepath(
	StringInfoData* path,	
	RelFileNode rnode, BackendId backend, ForkNumber forkNum, BlockNumber blkno
) {
	char *relpath;
	BlockNumber blockNum;

	relpath = relpathbackend(rnode, backend, forkNum);

	appendStringInfoString(&path, relpath);
	blockNum = blkno / ((BlockNumber) RELSEG_SIZE);

	if (blockNum > 0)
		appendStringInfo(&path, ".%u", blockNum);

	pfree(relpath);
}


static void
constructExtenrnalStorageAOFilepath(
	StringInfoData* path,	
	RelFileNode rnode, BackendId backend, int segno
) {
	char *relpath;
	relpath = relpathbackend(rnode, backend, MAIN_FORKNUM);

	if (segno > 0)
		appendStringInfo(&path, ".%u", segno);

	pfree(relpath);
}

void
loadFileFromExtnernalStorage(RelFileNode rnode, BackendId backend, ForkNumber forkNum, BlockNumber blkno)
{
	StringInfoData path;
	bool result;
	initStringInfo(&path);

	constructExtenrnalStorageFilepath(&path, rnode, backend, forkNum, blkno);
	
	getFilepathFromS3(path.cursor);
}

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

void
yezzey_init(void)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] init");
	mdinit();	
}

#ifndef GPBUILD
void
yezzey_open(SMgrRelation reln)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] open");
	if (!ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0))
		loadFileFromExtnernalStorage((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0);
	
	mdopen(reln);
}
#endif

void
yezzey_close(SMgrRelation reln, ForkNumber forkNum)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] close");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0))
		loadFileFromExtnernalStorage((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0);
	
	mdclose(reln, forkNum);
}

void
yezzey_create(SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] create");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0))
		loadFileFromExtnernalStorage((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0);
	
	mdcreate(reln, forkNum, isRedo);
}

#ifdef GPBUILD
void
yezzey_create_ao(RelFileNodeBackend rnode, int32 segmentFileNum, bool isRedo)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] create ao");
	if (!ensureFileLocal((rnode).node, (rnode).backend, MAIN_FORKNUM, (uint32)0))
		loadFileFromExtnernalStorage((rnode).node, (rnode).backend, MAIN_FORKNUM, (uint32)0);

	mdcreate_ao(rnode, segmentFileNum, isRedo);
}
#endif

bool
yezzey_exists(SMgrRelation reln, ForkNumber forkNum)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] exists");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0))
		loadFileFromExtnernalStorage((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0);

	return mdexists(reln, forkNum);
}

void
#ifndef GPBUILD
yezzey_unlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo)
#else
yezzey_unlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo, char relstorage)
#endif
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] unlink");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((rnode).node, (rnode).backend, MAIN_FORKNUM, (uint32)0))
                loadFileFromExtnernalStorage((rnode).node, (rnode).backend, MAIN_FORKNUM, (uint32)0);

#ifndef GPBUILD
	mdunlink(rnode, forkNum, isRedo);
#else
	mdunlink(rnode, forkNum, isRedo, relstorage);
#endif
}

void
yezzey_extend(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, char *buffer, bool skipFsync)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] extend");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum))
		loadFileFromExtnernalStorage((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum);
	
	mdextend(reln, forkNum, blockNum, buffer, skipFsync);
}

#if PG_VERSION_NUM >= 130000
bool
#else
void
#endif
yezzey_prefetch(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] prefetch");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum))
		loadFileFromExtnernalStorage((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum);
	
	return mdprefetch(reln, forkNum, blockNum);
}

void
yezzey_read(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, char *buffer)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] read");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum))
		loadFileFromExtnernalStorage((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum);

	mdread(reln, forkNum, blockNum, buffer);
}

void
yezzey_write(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, char *buffer, bool skipFsync)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] write");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum))
		loadFileFromExtnernalStorage((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum);
	
	mdwrite(reln, forkNum, blockNum, buffer, skipFsync);
}

#ifndef GPBUILD
void
yezzey_writeback(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, BlockNumber nBlocks)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] writeback");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum))
		loadFileFromExtnernalStorage((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum);

	mdwriteback(reln, forkNum, blockNum, nBlocks);
}
#endif

BlockNumber
yezzey_nblocks(SMgrRelation reln, ForkNumber forkNum)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] nblocks");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0))
		loadFileFromExtnernalStorage((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0);
	
	return mdnblocks(reln, forkNum);
}

void
yezzey_truncate(SMgrRelation reln, ForkNumber forkNum, BlockNumber nBlocks)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] truncate");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0))
		loadFileFromExtnernalStorage((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0);
	
	mdtruncate(reln, forkNum, nBlocks);
}

void
yezzey_immedsync(SMgrRelation reln, ForkNumber forkNum)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] immedsync");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0))
		loadFileFromExtnernalStorage((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0);
	
	mdimmedsync(reln, forkNum);
}

static const struct f_smgr yezzey_smgr =
{
	.smgr_init = yezzey_init,
	.smgr_shutdown = NULL,
#ifndef GPBUILD
	.smgr_open = yezzey_open,
#endif
	.smgr_close = yezzey_close,
	.smgr_create = yezzey_create,
	.smgr_create_ao = yezzey_create_ao,
	.smgr_exists = yezzey_exists,
	.smgr_unlink = yezzey_unlink,
	.smgr_extend = yezzey_extend,
	.smgr_prefetch = yezzey_prefetch,
	.smgr_read = yezzey_read,
	.smgr_write = yezzey_write,
#ifndef GPBUILD
	.smgr_writeback = yezzey_writeback,
#endif
	.smgr_nblocks = yezzey_nblocks,
	.smgr_truncate = yezzey_truncate,
	.smgr_immedsync = yezzey_immedsync,
};

const f_smgr *
smgr_yezzey(BackendId backend, RelFileNode rnode)
{
	return &yezzey_smgr;
}

#ifdef GPBUILD
void
smgr_warmup_yezzey(RelFileNode rnode, char *filepath)
{	
	elog(INFO, "[YEZZEY_SMGR] ao hook");
	if (!ensureFilepathLocal(filepath))
		getFilepathFromS3(filepath);
}
#endif

void
smgr_init_yezzey(void)
{
	smgr_init_standard();
	yezzey_init();
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

void processTables(void)
{
	int ret, i;
	long unsigned int cnt;
	TupleDesc tupdesc;
	SPITupleTable *tuptable;
	
	elog(yezzey_log_level, "[YEZZEY_SMGR_BG] putting unprocessed tables in move_table");

	yezzey_prepare();
	
	ret = SPI_execute("SELECT * FROM yezzey.metatable", true, 0);
	
	if (ret != SPI_OK_SELECT)
		elog(FATAL, "[YEZZEY_SMGR_BG] Error while trying to get unprocessed tables");
	
	tupdesc = SPI_tuptable->tupdesc;
    tuptable = SPI_tuptable;
	cnt = SPI_processed;
	
	elog(yezzey_log_level, "[YEZZEY_SMGR_BG] got %lu results", cnt);

	for (i = 0; i < cnt; i++)
	{
		HeapTuple tuple;
		char *tableName;
		Oid oid;

		tuple = tuptable->vals[i];
		tableName = SPI_getvalue(tuple, tupdesc, 1);

		StringInfoData buf;
		initStringInfo(&buf);
		appendStringInfo(&buf, "SELECT '%s'::regclass::oid", tableName);


		ret = SPI_execute(buf.data, true, 1);
		if (ret != SPI_OK_SELECT) {
            elog(FATAL, "[YEZZEY_SMGR_BG] Error while resolvind table oid");
		}

		
		tuple = tuptable->vals[0];
		oid = SPI_getvalue(tuple, tupdesc, 1);

		StringInfo query;
		initStringInfo(&query);

		appendStringInfo(&query, "SELECT segno FROM pg_aoseg.pg_aoseg_%lld", oid);

		/**
		 * @brief traverse pg_aoseg.pg_aoseg_<oid> relation
		 * and try to lock each segment. If there is any transaction
		 * in progress accessing some segment, lock attemt will fail, 
		 * because transactions holds locks on pg_aoseg.pg_aoseg_<oid> rows
		 * and we are goind to lock row in X mode
		 */

		// TupleDesc tupdesc1;
        // SPITupleTable *tuptable1;
        // HeapTuple tuple1;
		// int cnt1;

		// elog(yezzey_log_level, "[YEZZEY_SMGR_BG] process '%s' table", tableName);
		
		// initStringInfo(&buf);

		// appendStringInfoString(&buf, "SELECT pg_relation_filepath('");
		// appendStringInfoString(&buf, tableName);
		// appendStringInfoString(&buf, "');");

		// ret = SPI_execute(buf.data, true, 1);

		// if (ret != SPI_OK_SELECT) {
        //     elog(FATAL, "[YEZZEY_SMGR_BG] Error while finding path to table");
		// }

		// tupdesc1 = SPI_tuptable->tupdesc;
        // tuptable1 = SPI_tuptable;
	    // tuple1 = tuptable1->vals[0];
		
        // oid = SPI_getvalue(tuple1, tupdesc1, 1);

		// elog(yezzey_log_level, "[YEZZEY_SMGR_BG] table's filepath = %s", oid);
		
		// initStringInfo(&b1);

		// appendStringInfoString(&b1, "SELECT * FROM yezzey.move_table WHERE oid = '");
		// appendStringInfoString(&b1, oid);
		// appendStringInfoString(&b1, "';");
		
		// ret = SPI_execute(b1.data, true, 0);

		// if (ret != SPI_OK_SELECT)
		// 	elog(FATAL, "[YEZZEY_SMGR_BG] Error while finding oid in move table");
		
		// cnt1 = SPI_processed;
		
		// elog(yezzey_log_level, "[YEZZEY_SMGR_BG] found %d instances of oid in move table", cnt1);
	}
	
	yezzey_finish();
}

#if PG_VERSION_NUM < 100000
static void
#else
void
#endif
yezzey_main(Datum main_arg)
{
	int ret;

	pqsignal(SIGHUP, yezzey_sighup);
	pqsignal(SIGTERM, yezzey_sigterm);

	BackgroundWorkerUnblockSignals();

#ifdef GPBUILD
	if (!IS_QUERY_DISPATCHER())
	{
		Gp_role = GP_ROLE_UTILITY;
		Gp_session_role = GP_ROLE_UTILITY;
#endif
		
#if PG_VERSION_NUM < 110000
		BackgroundWorkerInitializeConnection("postgres", NULL);
#else
		BackgroundWorkerInitializeConnection("postgres", NULL, 0);
#endif
		
		while (!got_sigterm)
		{
			int rc, rc1;
			
			elog(yezzey_log_level, "[YEZZEY_SMGR_BG] waiting for processing tables");
					
			rc1 = WaitLatch(&MyProc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
#if PG_VERSION_NUM < 100000
                    interval
#else
                    interval, PG_WAIT_EXTENSION
#endif
            );

            ResetLatch(&MyProc->procLatch);

			if (rc1 & WL_POSTMASTER_DEATH)
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
			
			if (interval > 0) {
					processTables();
			}
			
			elog(yezzey_log_level, "[YEZZEY_SMGR_BG] waiting for sending tables");
			
			rc = WaitLatch(&MyProc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
#if PG_VERSION_NUM < 100000
			interval
#else
			interval, PG_WAIT_EXTENSION
#endif
			);

			ResetLatch(&MyProc->procLatch);
			
			if (rc & WL_POSTMASTER_DEATH)
				proc_exit(1);
			
			if (got_sighup)
			{
				ProcessConfigFile(PGC_SIGHUP);
				got_sighup = false;
				ereport(LOG, (errmsg("bgworker yezzey signal: processed SIGHUP")));
			}
			
			if (got_sigterm)
			{
				ereport(LOG, (errmsg("bgworker yezzey signal: processed SIGTERM")));
				proc_exit(0);
			}
			
			if (interval > 0)
			{
				processTables();
			}
		}
#ifdef GPBUILD
	}
#endif
}

void
_PG_init(void)
{
	BackgroundWorker worker;

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
	
	elog(yezzey_log_level, "[YEZZEY_SMGR] setting up bgworker");

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
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
	
	smgrwarmup_hook = smgr_warmup_yezzey;
	smgr_hook = smgr_yezzey;
	smgr_init_hook = smgr_init_yezzey;
}
