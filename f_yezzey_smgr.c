#include "postgres.h"
#include "fmgr.h"

#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>

#include "utils/snapmgr.h"
#include "miscadmin.h"

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

#include "yezzey.h"

static void yezzey_prepare(void);
static void yezzey_finish(void);
void sendTablesToS3(void);
void getFileFromS3(SMgrRelation reln, ForkNumber forkNum);
static char *buildS3Command(const char *s3Command, const char *s3Path, const char *localPath);

void yezzey_init(void);
#ifndef GPBUILD
void yezzey_open(SMgrRelation reln);
#endif
void yezzey_close(SMgrRelation reln, ForkNumber forkNum);
void yezzey_create(SMgrRelation reln, ForkNumber forkNum, bool isRedo);
bool yezzey_exists(SMgrRelation reln, ForkNumber forkNum);
#ifndef GPBUILD
void yezzey_unlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo);
#else
void yezzey_unlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo, char relstorage);
#endif
void yezzey_extend(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, char *buffer, bool skipFsync);
#if PG_VERSION_NUM >= 130000
bool yezzey_prefetch(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum);
#else
void yezzey_prefetch(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum);
#endif

void yezzey_read(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, char *buffer);
void yezzey_write(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, char *buffer, bool skipFsync);
void yezzey_writeback(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, BlockNumber nBlocks);
BlockNumber yezzey_nblocks(SMgrRelation reln, ForkNumber forkNum);
void yezzey_truncate(SMgrRelation reln, ForkNumber forkNum, BlockNumber nBlocks);
void yezzey_immedsync(SMgrRelation reln, ForkNumber forkNum);

const f_smgr *smgr_yezzey(BackendId backend, RelFileNode rnode);
void smgr_init_yezzey(void);

#if PG_VERSION_NUM >= 100000
void yezzey_main(Datum main_arg);
#else
static void yezzey_main(Datum arg);
#endif

void _PG_init(void);

static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup = false;

static int interval = 10000;
static char *worker_name = "yezzey";

static int yezzey_log_level = LOG;
static char *s3_getter;
static char *s3_putter;

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

bool
ensureFileLocal(SMgrRelation reln, ForkNumber forkNum)
{	
	char *path;
	int fd;

	path = relpath(reln->smgr_rnode, forkNum);
	fd = open(path, O_RDONLY);
	
	elog(yezzey_log_level, "[YEZZEY_SMGR] trying to open %s, result - %d", path, fd);
	
	return fd >= 0;
}

void
sendFileToS3(const char *localPath)
{
	char *s3Path = strstr(localPath, "/base/");
	char *cd;
	int rc;
	
	s3Path++;

	cd = buildS3Command(s3_putter, localPath, s3Path);
        rc = system(cd);
	
	elog(yezzey_log_level, "[YEZZEY_SMGR_BG] tried \"%s\", got %d", cd, rc);
}

void
updateMoveTable(const char *oid, const char *forkName, const char *segNum, const bool isLocal)
{
	char *buf = (char *)palloc0(100);
	int ret;
	
	strcpy(buf, "UPDATE yezzey.move_table SET isLocal = ");
	if (isLocal)
		strcat(buf, "true");
	else
		strcat(buf, "false");
	strcat(buf, " WHERE oid = '");
	strcat(buf, oid);
	strcat(buf, "' AND forkName = '");
	strcat(buf, forkName);
	strcat(buf, "' AND segNum = '");
	strcat(buf, segNum);
	strcat(buf, "';");

	ret = SPI_execute(buf, false, 1);
	
	elog(yezzey_log_level, "[YEZZEY_SMGR] tried %s, result = %d", buf, ret);
	
	if (ret != SPI_OK_UPDATE)
		elog(ERROR, "[YEZZEY_SMGR] failed to update move_table");
}

void
removeLocalFile(const char *localPath)
{
	char *rmd = (char *)palloc0(100);
	int rc;
	
	strcpy(rmd, "rm -f ");
	strcat(rmd, localPath);

	rc = system(rmd);

	elog(yezzey_log_level, "[YEZZEY_SMGR_BG] tried \"%s\", got %d", rmd, rc);
}

void
sendOidToS3(const char *oid, const char *forkName, const char *segNum)
{
	char *path = (char *)palloc0(100);
	int fd;
	
	strcpy(path, "/home/fstilus/pg_build/e/base/5/");
	strcat(path, oid);

	if (segNum[0] != '0')
	{
		strcat(path, ".");
		strcat(path, segNum);
	}

	if (forkName[0] != 'm')
	{
		strcat(path, "_");
		strcat(path, forkName);
	}

	fd = open(path, O_RDONLY);

	elog(yezzey_log_level, "[YEZZEY_SMGR_BG] trying to open %s, result - %d", path, fd);
	
	if (fd >= 0)
	{
		sendFileToS3(path);
		updateMoveTable(oid, forkName, segNum, false);
		removeLocalFile(path);
	}
	else
		updateMoveTable(oid, forkName, segNum, false);
}

static void
yezzey_prepare(void)
{
	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "initializing yezzey schema");
	SetCurrentStatementStartTimestamp();
}

static void
yezzey_finish(void)
{
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
	pgstat_report_stat(true);
}

void
sendTablesToS3(void)
{
	char *buf = (char *)palloc0(100);
	int ret, i;
	long unsigned int cnt;
	TupleDesc tupdesc;
	SPITupleTable *tuptable;

	elog(yezzey_log_level, "[YEZZEY_SMGR_BG] sending table to S3");
	
	yezzey_prepare();

	strcpy(buf, "SELECT * FROM yezzey.move_table;");
	
	elog(yezzey_log_level, "[YEZZEY_SMGR_BG] trying '%s'", buf);

	ret = SPI_execute(buf, true, 0);

	if (ret != SPI_OK_SELECT)
                elog(FATAL, "[YEZZEY_SMGR_BG] Error while trying to get info about move_table");

	tupdesc = SPI_tuptable->tupdesc;
        tuptable = SPI_tuptable;
	cnt = SPI_processed;

	elog(yezzey_log_level, "[YEZZEY_SMGR_BG] tried %s, result = %d, cnt = %lu", buf, ret, cnt);
        
	for (i = 0; i < cnt; i++)
	{
		HeapTuple tuple = tuptable->vals[i];
		char *oid = SPI_getvalue(tuple, tupdesc, 1);
		char *forkName = SPI_getvalue(tuple, tupdesc, 2);
		char *segNum = SPI_getvalue(tuple, tupdesc, 3);
		bool chlen = true;
		bool isLocal = DatumGetBool(SPI_getbinval(tuple, tupdesc, 4, &chlen));

		elog(yezzey_log_level, "[YEZZEY_SMGR_BG] got %s oid, %s forkName, %s segNum, local? %s", oid, forkName, segNum, isLocal?"true":"false");
		if (isLocal)
			sendOidToS3(oid, forkName, segNum);
	}	

	yezzey_finish();
}

void
getFileFromS3(SMgrRelation reln, ForkNumber forkNum)
{
	char *localPath = relpath(reln->smgr_rnode, forkNum);
	char *s3Path = (char *)palloc0(100);
	char *cd;
	int rc;
	
	strcpy(s3Path, localPath);
	strcat(s3Path, ".lz4");
	
	elog(yezzey_log_level, "[YEZZEY_SMGR] getting %s from ...", s3Path);

	cd = buildS3Command(s3_getter, s3Path, localPath);
	rc = system(cd);

	elog(yezzey_log_level, "[YEZZEY_SMGR] tried \"%s\", got %d", cd, rc);
	pfree(cd);

	if (rc == 0)
	{
		char *oid = (char *)palloc0(100);
		char *forkName = (char *)palloc0(100);
		char *segNum = (char *)palloc0(100);
		char *start = localPath;
		char *sp, *sl;
		bool fork = false, seg = false;

		while (true)
		{
			sl = strstr(start, "/");
			if (sl == NULL)
				break;
			start = sl + 1;
		}

		for (sp = start; *sp; sp++)
		{
			switch (sp[0])
			{
				case '.':
					{
						seg = true;
						break;
					}
				case '_':
					{
						fork = true;
						break;
					}
				default:
					{
						if (!seg && !fork)
							strncat(oid, sp, 1);
						else if (fork)
							strncat(forkName, sp, 1);
						else
							strncat(segNum, sp, 1);
					}
			}
		}

		if (!seg)
			strcpy(segNum, "0");
		if (!fork)
			strcpy(forkName, "main");
		
		elog(yezzey_log_level, "[YEZZEY_SMGR] got oid = %s, forkName = %s, segNum = %s", oid, forkName, segNum);
		
		SPI_connect();
		updateMoveTable(oid, forkName, segNum, true);
		SPI_finish();

		pfree(oid);
		pfree(forkName);
		pfree(segNum);
	}
}

static char *
buildS3Command(const char *s3Command, const char *firstPath, const char *secondPath)
{
	StringInfoData result;
	const char *sp;

	initStringInfo(&result);
	
	elog(yezzey_log_level, "[YEZZEY_SMGR] updating \"%s\" with \"%s\" and \"%s\"", s3Command, firstPath, secondPath);

	for (sp = s3Command; *sp; sp++)
	{
		if (*sp == '%')
		{
			switch (sp[1])
			{
				case 'f':
					{
						sp++;
						appendStringInfoString(&result, firstPath);
						break;
					}
				case 's':
					{
						sp++;
						appendStringInfoString(&result, secondPath);
						break;
					}
				case '%':
					{
						sp++;
						appendStringInfoChar(&result, *sp);
						break;
					}
				default:
					{
						appendStringInfoChar(&result, *sp);
						break;
					}
			}
		}
		else
		{
			appendStringInfoChar(&result, *sp);
		}
	}

	return result.data;
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
	mdopen(reln);
}
#endif

void
yezzey_close(SMgrRelation reln, ForkNumber forkNum)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] close");
	if (!ensureFileLocal(reln, forkNum))
		getFileFromS3(reln, forkNum);
	
	mdclose(reln, forkNum);
}

void
yezzey_create(SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] create");
	if (!ensureFileLocal(reln, forkNum))
		getFileFromS3(reln, forkNum);
	
	mdcreate(reln, forkNum, isRedo);
}

bool
yezzey_exists(SMgrRelation reln, ForkNumber forkNum)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] exists");
	if (!ensureFileLocal(reln, forkNum))
		getFileFromS3(reln, forkNum);

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
	if (!ensureFileLocal(reln, forkNum))
		getFileFromS3(reln, forkNum);
	
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
	if (!ensureFileLocal(reln, forkNum))
		getFileFromS3(reln, forkNum);
	return mdprefetch(reln, forkNum, blockNum);
}

void
yezzey_read(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, char *buffer)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] read");
	if (!ensureFileLocal(reln, forkNum))
		getFileFromS3(reln, forkNum);

	mdread(reln, forkNum, blockNum, buffer);
}

void
yezzey_write(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, char *buffer, bool skipFsync)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] write");
	if (!ensureFileLocal(reln, forkNum))
		getFileFromS3(reln, forkNum);
	
	mdwrite(reln, forkNum, blockNum, buffer, skipFsync);
}

void
yezzey_writeback(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, BlockNumber nBlocks)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] writeback");
	if (!ensureFileLocal(reln, forkNum))
		getFileFromS3(reln, forkNum);

	mdwriteback(reln, forkNum, blockNum, nBlocks);
}

BlockNumber
yezzey_nblocks(SMgrRelation reln, ForkNumber forkNum)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] nblocks");
	if (!ensureFileLocal(reln, forkNum))
		getFileFromS3(reln, forkNum);
	
	return mdnblocks(reln, forkNum);
}

void
yezzey_truncate(SMgrRelation reln, ForkNumber forkNum, BlockNumber nBlocks)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] truncate");
	if (!ensureFileLocal(reln, forkNum))
		getFileFromS3(reln, forkNum);
	
	mdtruncate(reln, forkNum, nBlocks);
}

void
yezzey_immedsync(SMgrRelation reln, ForkNumber forkNum)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] immedsync");
	if (!ensureFileLocal(reln, forkNum))
		getFileFromS3(reln, forkNum);
	
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
	.smgr_exists = yezzey_exists,
	.smgr_unlink = yezzey_unlink,
	.smgr_extend = yezzey_extend,
	.smgr_prefetch = yezzey_prefetch,
	.smgr_read = yezzey_read,
	.smgr_write = yezzey_write,
	.smgr_writeback = yezzey_writeback,
	.smgr_nblocks = yezzey_nblocks,
	.smgr_truncate = yezzey_truncate,
	.smgr_immedsync = yezzey_immedsync,
};

const f_smgr *
smgr_yezzey(BackendId backend, RelFileNode rnode)
{
	return &yezzey_smgr;
}

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


#if PG_VERSION_NUM < 100000
static void
#else
void
#endif
yezzey_main(Datum main_arg)
{
	pqsignal(SIGHUP, yezzey_sighup);
	pqsignal(SIGTERM, yezzey_sigterm);

	BackgroundWorkerUnblockSignals();

#if PG_VERSION_NUM < 110000
	BackgroundWorkerInitializeConnection("postgres", NULL);
#else
	BackgroundWorkerInitializeConnection("postgres", NULL, 0);
#endif
	
	while (!got_sigterm)
	{
		int rc;
		

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
			ereport(DEBUG1, (errmsg("bgworker yezzey signal: processed SIGHUP")));
		}

		if (got_sigterm)
		{
			ereport(DEBUG1, (errmsg("bgworker yezzey signal: processed SIGTERM")));
			proc_exit(0);
		}

		if (interval > 0)
		{
			sendTablesToS3();
		}
	}
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
                                NULL, 
								NULL, 
								NULL);
	
	DefineCustomEnumVariable("yezzey.log_level",
								"Log level for yezzey functions.",
								NULL,
								&yezzey_log_level,
								yezzey_log_level,
								loglevel_options,
								PGC_SUSET,
								0,
								NULL,
								NULL,
								NULL);
	
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
	
	smgr_hook = smgr_yezzey;
	smgr_init_hook = smgr_init_yezzey;
}
