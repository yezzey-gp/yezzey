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

static char *s3_getter;
static char *s3_putter;
static char *s3_prefix = "";

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
ensureFileLocal(RelFileNode rnode, BackendId backend, ForkNumber forkNum, BlockNumber blkno)
{	
	if (IsCrashRecoveryOnly()) {
		/* MDB-19689: do not consult catalog 
			if crash recovery is in progress */
		return true;
	}
	char *path = (char *)palloc0(1000);
	char *sn = (char *)palloc0(100);
	BlockNumber blockNum = blkno / ((BlockNumber) RELSEG_SIZE);

	sprintf(sn, ".%u", blockNum);
	strcpy(path, relpathbackend(rnode, backend, forkNum));
	if (blockNum > 0)
		strcat(path, sn);

	return ensureFilepathLocal(path);
}

bool
ensureFilepathLocal(char *filepath)
{
	int fd, cp, errno_;

	fd = open(filepath, O_RDONLY);
        errno_ = errno;

        cp = fd;
        elog(yezzey_log_level, "[YEZZEY_SMGR] trying to open %s, result - %d, %d", filepath, fd, errno_);
        close(fd);

        return cp >= 0;
}

void
sendFileToS3(const char *localPath)
{
	char *cd, *s3Path = (char *)palloc0(1000);
	int rc;
	
	strcpy(s3Path, s3_prefix);
	strcat(s3Path, localPath);

	cd = buildS3Command(s3_putter, localPath, s3Path);
	rc = system(cd);
	
	elog(yezzey_log_level, "[YEZZEY_SMGR_BG] tried \"%s\", got %d", cd, rc);
	
	pfree(s3Path);
}

void
updateMoveTable(const char *oid, const char *forkName, const uint32 segNum, const bool isLocal)
{
	StringInfoData result;
	char *sn = (char *)palloc0(100);
	int ret;

	initStringInfo(&result);

	appendStringInfoString(&result, "UPDATE yezzey.move_table SET isLocal = ");
	if (isLocal) {
		appendStringInfoString(&result, "true");
	} else {
		appendStringInfoString(&result, "false");
	}

	appendStringInfoString(&result, " WHERE oid = '");

	appendStringInfoString(&result, oid);
	appendStringInfoString(&result, "' AND forkName = '");
	appendStringInfoString(&result, forkName);
	appendStringInfoString(&result,  "' AND segNum = '");
	appendStringInfo(&result, "%d", segNum);
	appendStringInfoString(&result, "';");

	ret = SPI_execute(result.data, false, 1);
	
	sprintf(sn, "%u", segNum);
	initStringInfo(&result);

	appendStringInfoString(&result, "UPDATE yezzey.move_table SET isLocal = ");
	if (isLocal) {
		appendStringInfoString(&result, "true");
	} else {
		appendStringInfoString(&result, "false");
	}
	appendStringInfoString(&result, " WHERE oid = '");
	appendStringInfoString(&result, oid);
	appendStringInfoString(&result, "' AND forkName = '");
	appendStringInfoString(&result, forkName);
	appendStringInfoString(&result,  "' AND segNum = '");
	appendStringInfoString(&result, sn);
	appendStringInfoString(&result, "';");

	/*ret = SPI_execute(result.data, false, 1);
	
	elog(yezzey_log_level, "[YEZZEY_SMGR] tried %s, result = %d", result.data, ret);

	if (ret != SPI_OK_UPDATE)
		elog(ERROR, "[YEZZEY_SMGR] failed to update move_table");
	*/
	pfree(result.data);
	pfree(sn);
}

int
removeLocalFile(const char *localPath)
{
	int rc = remove(localPath);

	elog(yezzey_log_level, "[YEZZEY_SMGR_BG] tried to remove local file \"%s\", result: %d", localPath, rc);
	return rc;
}

void
sendOidToS3(const char *oid, const char *forkName, const uint32 segNum)
{
	char *path = (char *)palloc0(100);
	char *sn = (char *)palloc0(100);
	int fd;
	
	strcpy(path, oid);
	sprintf(sn, "%u", segNum);

	if (forkName[0] != 'm')
	{
		strcat(path, "_");
		strcat(path, forkName);
	}

	if (segNum != 0)
	{
		strcat(path, ".");
		strcat(path, sn);
	}

	fd = open(path, O_RDONLY);

	elog(yezzey_log_level, "[YEZZEY_SMGR_BG] trying to open %s, result - %d", path, fd);
	
	if (fd >= 0)
	{
		char *mxpath = (char *)palloc0(100);
		char *stmxsn = (char *)palloc0(100);
		StringInfoData buff;
		int fet, tto;
		bool chlen = true;
		uint32 mxsn;

		initStringInfo(&buff);
		
		appendStringInfoString(&buff, "SELECT MAX(segnum) AS mxsn FROM yezzey.move_table WHERE oid='");
		appendStringInfoString(&buff, oid);
		appendStringInfoString(&buff, "' AND forkname='");
		appendStringInfoString(&buff, forkName);
		appendStringInfoString(&buff, "';");

		fet = SPI_execute(buff.data, true, 0);

		elog(yezzey_log_level, "[YEZZEY_SMGR_BG] done '%s', result = %d", buff.data, fet);

		mxsn = (uint32)DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &chlen));
		mxsn++;	

		strcpy(mxpath, oid);
		sprintf(stmxsn, "%u", mxsn);
	
		if (forkName[0] != 'm')
		{
				strcat(mxpath, "_");
				strcat(mxpath, forkName);
		}
	
		if (mxsn != 0)
		{
				strcat(mxpath, ".");
				strcat(mxpath, stmxsn);
		}

		tto = open(mxpath, O_RDONLY);

		if (tto >= 0) {
			StringInfoData result;
			char *sn1 = (char *)palloc0(100);
			int ret;

			sprintf(sn1, "%u", mxsn);
			
			initStringInfo(&result);

			appendStringInfoString(&result, "INSERT INTO yezzey.move_table VALUES('");
			appendStringInfoString(&result, oid);
			appendStringInfoString(&result, "', '");
			appendStringInfoString(&result, forkName);
			appendStringInfoString(&result, "', '");
			appendStringInfoString(&result, sn1);
			appendStringInfoString(&result, "', true);");

			ret = SPI_execute(result.data, false, 1);

			mxsn++;
			elog(yezzey_log_level, "[YEZZEY_SMGR_BG] done '%s', result = %d", result.data, ret);

			pfree(sn1);
		}
		
		elog(yezzey_log_level, "[YEZZEY_SMGR_BG] %d total segs", mxsn);
		
		if (segNum < mxsn - 1)
		{
			sendFileToS3(path);
			updateMoveTable(oid, forkName, segNum, false);
			removeLocalFile(path);
		}
		
		close(tto);
		pfree(mxpath);
	}
	else
		updateMoveTable(oid, forkName, segNum, false);
	
	close(fd);
	pfree(path);
	pfree(sn);
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
sendTablesToS3(void)
{
	char *buf = "SELECT * FROM yezzey.move_table;";
	int ret, i;
	long unsigned int cnt;
	TupleDesc tupdesc;
	SPITupleTable *tuptable;

	elog(yezzey_log_level, "[YEZZEY_SMGR_BG] sending table to S3");
	
	yezzey_prepare();

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
		bool chlen = true;
		uint32 segNum = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 3, &chlen));
		bool isLocal = DatumGetBool(SPI_getbinval(tuple, tupdesc, 4, &chlen));

		elog(yezzey_log_level, "[YEZZEY_SMGR_BG] got %s oid, %s forkName, %u segNum, local? %s", oid, forkName, segNum, isLocal?"true":"false");
		//if (isLocal)
		sendOidToS3(oid, forkName, segNum);
	}	

	yezzey_finish();
}

void
getFileFromS3(RelFileNode rnode, BackendId backend, ForkNumber forkNum, BlockNumber blkno)
{
	char *localPath = (char *)palloc0(1000);
	char *sn = (char *)palloc0(100);
	BlockNumber blockNum = blkno / ((BlockNumber) RELSEG_SIZE);	
		
	strcpy(localPath, relpathbackend(rnode, backend, forkNum));
	sprintf(sn, ".%u", blockNum);
	if (blockNum > 0)
		strcat(localPath, sn);
	
	getFilepathFromS3(localPath);
}

void
getFilepathFromS3(char *filepath)
{
	char *s3Path = (char *)palloc0(1000);
	char *cd;
	int rc;
	
	strcpy(s3Path, s3_prefix);
        strcat(s3Path, filepath);
        strcat(s3Path, ".lz4");
	
	elog(yezzey_log_level, "[YEZZEY_SMGR] getting %s from ...", s3Path);

        cd = buildS3Command(s3_getter, s3Path, filepath);
        rc = system(cd);

        elog(yezzey_log_level, "[YEZZEY_SMGR] tried \"%s\", got %d", cd, rc);
        pfree(cd);

        if (rc == 0)
        {
                char *oid = (char *)palloc0(100);
                char *forkName = (char *)palloc0(100);
                char *segNum = (char *)palloc0(100);
                char *start = filepath;
                char *sp;
                bool fork = false, seg = false;

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
                                                else if (seg)
                                                        strncat(segNum, sp, 1);
                                                else
                                                        strncat(forkName, sp, 1);
                                        }
                        }
                }

                if (!seg)
                        strcpy(segNum, "0");
                if (!fork)
                        strcpy(forkName, "main");

                elog(yezzey_log_level, "[YEZZEY_SMGR] got oid = %s, forkName = %s, segNum = %s", oid, forkName, segNum);

                SPI_connect();
                updateMoveTable(oid, forkName, (uint32)strtoul(segNum, NULL, 0), true);
                SPI_finish();

                pfree(oid);
                pfree(forkName);
                pfree(segNum);
        }

        pfree(s3Path);
}

char *
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
	if (!ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0))
		getFileFromS3((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0);
	
	mdopen(reln);
}
#endif

void
yezzey_close(SMgrRelation reln, ForkNumber forkNum)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] close");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0))
		getFileFromS3((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0);
	
	mdclose(reln, forkNum);
}

void
yezzey_create(SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] create");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0))
		getFileFromS3((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0);
	
	mdcreate(reln, forkNum, isRedo);
}

#ifdef GPBUILD
void
yezzey_create_ao(RelFileNodeBackend rnode, int32 segmentFileNum, bool isRedo)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] create ao");
	if (!ensureFileLocal((rnode).node, (rnode).backend, MAIN_FORKNUM, (uint32)0))
		getFileFromS3((rnode).node, (rnode).backend, MAIN_FORKNUM, (uint32)0);

	mdcreate_ao(rnode, segmentFileNum, isRedo);
}
#endif

bool
yezzey_exists(SMgrRelation reln, ForkNumber forkNum)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] exists");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0))
		getFileFromS3((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0);

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
                getFileFromS3((rnode).node, (rnode).backend, MAIN_FORKNUM, (uint32)0);

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
		getFileFromS3((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum);
	
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
		getFileFromS3((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum);
	
	return mdprefetch(reln, forkNum, blockNum);
}

void
yezzey_read(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, char *buffer)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] read");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum))
		getFileFromS3((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum);

	mdread(reln, forkNum, blockNum, buffer);
}

void
yezzey_write(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, char *buffer, bool skipFsync)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] write");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum))
		getFileFromS3((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum);
	
	mdwrite(reln, forkNum, blockNum, buffer, skipFsync);
}

#ifndef GPBUILD
void
yezzey_writeback(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, BlockNumber nBlocks)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] writeback");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum))
		getFileFromS3((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum);

	mdwriteback(reln, forkNum, blockNum, nBlocks);
}
#endif

BlockNumber
yezzey_nblocks(SMgrRelation reln, ForkNumber forkNum)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] nblocks");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0))
		getFileFromS3((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0);
	
	return mdnblocks(reln, forkNum);
}

void
yezzey_truncate(SMgrRelation reln, ForkNumber forkNum, BlockNumber nBlocks)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] truncate");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0))
		getFileFromS3((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0);
	
	mdtruncate(reln, forkNum, nBlocks);
}

void
yezzey_immedsync(SMgrRelation reln, ForkNumber forkNum)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] immedsync");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0))
		getFileFromS3((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0);
	
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
	elog(yezzey_log_level, "[YEZZEY_SMGR] ao hook");
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

void addToMoveTable(char *tableName)
{
	char *oid = (char *)palloc(100);
	char *sel = (char *)palloc(1000);
	char *ao = (char *)palloc(1000);
	int ret;
	TupleDesc tupdesc;
	SPITupleTable *tuptable;
	HeapTuple tuple;
	StringInfoData buf, b1, b2;
	uint32 i, maxSeg;
	bool chlen = true;

	initStringInfo(&buf);

	elog(yezzey_log_level, "[YEZZEY_SMGR] selecting oid from %s", tableName);

	strcpy(sel, "SELECT pg_relation_filepath('");
	strcat(sel, tableName);
	strcat(sel, "');");
	ret = SPI_execute(sel, true, 1);

	elog(yezzey_log_level, "[YEZZEY_SMGR] tried %s, result = %d", sel, ret);

	if (ret != SPI_OK_SELECT || SPI_processed < 1)
			elog(FATAL, "Error while trying to get info about table");

	tupdesc = SPI_tuptable->tupdesc;
	tuptable = SPI_tuptable;
	tuple = tuptable->vals[0];

	strcpy(oid, SPI_getvalue(tuple, tupdesc, 1));

	elog(yezzey_log_level, "[YEZZEY_SMGR] moving %s into move_table", oid);

	//maxseg

	initStringInfo(&b1);
	appendStringInfoString(&b1, "SELECT aosegtablefqn FROM (SELECT seg.aooid, quote_ident(aoseg_c.relname) AS aosegtablefqn, seg.relfilenode FROM pg_class aoseg_c JOIN ");
	appendStringInfoString(&b1, "(SELECT pg_ao.relid AS aooid, pg_ao.segrelid, aotables.aotablefqn, aotables.relstorage, aotables.relnatts, aotables.relfilenode, aotables.reltablespace FROM pg_appendonly pg_ao JOIN ");
	appendStringInfoString(&b1, "(SELECT c.oid, quote_ident(n.nspname)|| '.' || quote_ident(c.relname) AS aotablefqn, c.relstorage, c.relnatts, c.relfilenode, c.reltablespace FROM pg_class c JOIN ");
	appendStringInfoString(&b1, "pg_namespace n ON c.relnamespace = n.oid WHERE relstorage IN ( 'ao', 'co' ) AND relpersistence='p') aotables ON pg_ao.relid = aotables.oid) seg ON aoseg_c.oid = seg.segrelid) ");
	appendStringInfoString(&b1, "AS x WHERE aooid = '");
	appendStringInfoString(&b1, tableName);
	appendStringInfoString(&b1, "'::regclass::oid;");

	ret = SPI_execute(b1.data, true, 0);

	if (ret != SPI_OK_SELECT) {
        elog(FATAL, "Error while finding ao info");
	}
	if (SPI_processed < 1)
			elog(ERROR, "Not an ao or aocs table");

	tupdesc = SPI_tuptable->tupdesc;
	tuptable = SPI_tuptable;
	tuple = tuptable->vals[0];

	strcpy(ao, SPI_getvalue(tuple, tupdesc, 1));

	elog(yezzey_log_level, "[YEZZEY_SMGR] found aosegtablefqn = %s", ao);

	initStringInfo(&b2);
	switch (ao[5])
	{
			case 's':       //ao
					{
							appendStringInfoString(&b2, "SELECT MAX(segno) AS mxsn FROM pg_aoseg.");
							appendStringInfoString(&b2, ao);
							appendStringInfoString(&b2, ";");

							break;
					}
			case 'c':       //aocs
					{
							appendStringInfoString(&b2, "SELECT MAX(aocs.physical_segno) AS mxsn FROM gp_toolkit.__gp_aocsseg('");
							appendStringInfoString(&b2, tableName);
							appendStringInfoString(&b2, "'::regclass::oid) aocs;");

							break;
					}
			default:
					{
							elog(FATAL, "Wrong symbol in aosegtablefqn");
							break;
					}
	}

	ret = SPI_execute(b2.data, true, 0);

	if (ret != SPI_OK_SELECT)
			elog(FATAL, "Error while finding maxseg");

	tupdesc = SPI_tuptable->tupdesc;
	tuptable = SPI_tuptable;
	tuple = tuptable->vals[0];

	maxSeg = (uint32)DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &chlen));

	elog(yezzey_log_level, "[YEZZEY_SMGR] found maxseg = %u", maxSeg);

	appendStringInfoString(&buf, "INSERT INTO yezzey.move_table VALUES ('");
	appendStringInfoString(&buf, oid);
	appendStringInfoString(&buf, "', 'main', '0', true)");

	for (i = 1; i <= maxSeg; i++)
	{
			char *sn = (char *)palloc(100);

			sprintf(sn, "%d", i);

	appendStringInfoString(&buf, ", ('");
			appendStringInfoString(&buf, oid);
			appendStringInfoString(&buf, "', 'main', '");
			appendStringInfoString(&buf, sn);
			appendStringInfoString(&buf, "', true)");

			pfree(sn);
	}

	appendStringInfoString(&buf, ";");

	ret = SPI_execute(buf.data, false, 0);

	if (ret != SPI_OK_INSERT) {
		elog(FATAL, "Error while trying to insert oid into move_table");
	}
}

void processTables(void)
{
	int ret, i;
	long unsigned int cnt;
	TupleDesc tupdesc;
	SPITupleTable *tuptable;
	
	elog(yezzey_log_level, "[YEZZEY_SMGR_BG] putting unprocessed tables in move_table");

	yezzey_prepare();
	
	ret = SPI_execute("SELECT * FROM yezzey.metatable WHERE processed = FALSE;", true, 0);
	
	if (ret != SPI_OK_SELECT)
		elog(FATAL, "[YEZZEY_SMGR_BG] Error while trying to get unprocessed tables");
	
	tupdesc = SPI_tuptable->tupdesc;
        tuptable = SPI_tuptable;
	cnt = SPI_processed;
	
	elog(yezzey_log_level, "[YEZZEY_SMGR_BG] got %lu results", cnt);

	for (i = 0; i < cnt; i++)
	{
		HeapTuple tuple = tuptable->vals[i];
		char *tableName = SPI_getvalue(tuple, tupdesc, 1);
		char *oid = (char *)palloc0(100);
		StringInfoData buf, b1;
		TupleDesc tupdesc1;
        	SPITupleTable *tuptable1;
        	HeapTuple tuple1;
		int cnt1;

		elog(yezzey_log_level, "[YEZZEY_SMGR_BG] found '%s' table", tableName);
		
		initStringInfo(&buf);

		appendStringInfoString(&buf, "SELECT pg_relation_filepath('");
		appendStringInfoString(&buf, tableName);
		appendStringInfoString(&buf, "');");

		ret = SPI_execute(buf.data, true, 1);

		if (ret != SPI_OK_SELECT)
                	elog(FATAL, "[YEZZEY_SMGR_BG] Error while finding path to table");
		
		tupdesc1 = SPI_tuptable->tupdesc;
        	tuptable1 = SPI_tuptable;
	        tuple1 = tuptable1->vals[0];
		
        	strcpy(oid, SPI_getvalue(tuple1, tupdesc1, 1));

		elog(yezzey_log_level, "[YEZZEY_SMGR_BG] table's filepath = %s", oid);
		
		initStringInfo(&b1);

		appendStringInfoString(&b1, "SELECT * FROM yezzey.move_table WHERE oid = '");
		appendStringInfoString(&b1, oid);
		appendStringInfoString(&b1, "';");
		
		ret = SPI_execute(b1.data, true, 0);

		if (ret != SPI_OK_SELECT)
			elog(FATAL, "[YEZZEY_SMGR_BG] Error while finding oid in move table");
		
		cnt1 = SPI_processed;
		
		elog(yezzey_log_level, "[YEZZEY_SMGR_BG] found %d instances of oid in move table", cnt1);

		if (cnt1 < 1)
			addToMoveTable(tableName);
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
		
		yezzey_prepare();
		
                ret = SPI_execute("CREATE TABLE IF NOT EXISTS yezzey.move_table(oid text NOT NULL, forkName text NOT NULL, segNum int NOT NULL, isLocal bool, UNIQUE (oid, forkName, segNum));", false, 0);
		
                if (ret <= 0)
                        elog(FATAL, "[YEZZEY_SMGR_BG] couldn't create table");

                yezzey_finish();
		
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
				sendTablesToS3();
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
