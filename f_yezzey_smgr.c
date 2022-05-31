#include "postgres.h"

#include <fcntl.h>

#include "storage/smgr.h"
#include "storage/md.h"
#include "common/relpath.h"
#include "common/file_perm.h"
#include "utils/guc.h"
#include "lib/stringinfo.h"

PG_MODULE_MAGIC;

const int SmgrTrace = LOG;

char *s3_archive;

char *
buildS3Command(const char *s3Command, const char *s3Path, const char *localPath);

bool
ensureFileLocal(SMgrRelation reln, ForkNumber forkNum)
{	
	char *path;
	int fd;

	path = relpath(reln->smgr_rnode, forkNum);
	fd = open(path, O_RDONLY);
	
	elog(SmgrTrace, "[YEZZEY_SMGR] trying to open %s, result - %d", path, fd);
	
	return fd >= 0;
}

void
getFileFromS3(SMgrRelation reln, ForkNumber forkNum)
{
	char *path;
	char *cd;
	int rc;

	path = relpath(reln->smgr_rnode, forkNum);

	elog(SmgrTrace, "[YEZZEY_SMGR] getting %s from ...", path);

	cd = buildS3Command(s3_archive, path, path);
	rc = system(cd);

	elog(SmgrTrace, "[YEZZEY_SMGR] tried %s, got %d", cd, rc);
	pfree(cd);
}

char *
buildS3Command(const char *s3Command, const char *s3Path, const char *localPath)
{
	StringInfoData result;
	const char *sp;

	initStringInfo(&result);
	
	elog(SmgrTrace, "[YEZZEY_SMGR] updating \"%s\" with \"%s\" and \"%s\"", s3Command, s3Path, localPath);

	for (sp = s3Command; *sp; sp++)
	{
		if (*sp == '%')
		{
			switch (sp[1])
			{
				case 's':
					{
						sp++;
						appendStringInfoString(&result, s3Path);
						appendStringInfoString(&result, ".lz4");
						break;
					}
				case 'l':
					{
						sp++;
						appendStringInfoString(&result, localPath);
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
	elog(SmgrTrace, "[YEZZEY_SMGR] init");
	mdinit();	
}

void
yezzey_open(SMgrRelation reln)
{
	elog(SmgrTrace, "[YEZZEY_SMGR] open");
	mdopen(reln);
}

void
yezzey_close(SMgrRelation reln, ForkNumber forkNum)
{
	elog(SmgrTrace, "[YEZZEY_SMGR] close");
	if (!ensureFileLocal(reln, forkNum))
		getFileFromS3(reln, forkNum);
	
	mdclose(reln, forkNum);
}

void
yezzey_create(SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{
	elog(SmgrTrace, "[YEZZEY_SMGR] create");
	if (!ensureFileLocal(reln, forkNum))
		getFileFromS3(reln, forkNum);
	
	mdcreate(reln, forkNum, isRedo);
}

bool
yezzey_exists(SMgrRelation reln, ForkNumber forkNum)
{
	elog(SmgrTrace, "[YEZZEY_SMGR] exists");
	if (!ensureFileLocal(reln, forkNum))
		getFileFromS3(reln, forkNum);

	return mdexists(reln, forkNum);
}

void
yezzey_unlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo)
{
	elog(SmgrTrace, "[YEZZEY_SMGR] unlink");	
	mdunlink(rnode, forkNum, isRedo);
}

void
yezzey_extend(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, char *buffer, bool skipFsync)
{
	elog(SmgrTrace, "[YEZZEY_SMGR] extend");
	if (!ensureFileLocal(reln, forkNum))
		getFileFromS3(reln, forkNum);
	
	mdextend(reln, forkNum, blockNum, buffer, skipFsync);
}

bool
yezzey_prefetch(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum)
{
	elog(SmgrTrace, "[YEZZEY_SMGR] prefetch");
	if (!ensureFileLocal(reln, forkNum))
		getFileFromS3(reln, forkNum);
	
	return mdprefetch(reln, forkNum, blockNum);
}

void
yezzey_read(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, char *buffer)
{
	elog(SmgrTrace, "[YEZZEY_SMGR] read");
	if (!ensureFileLocal(reln, forkNum))
		getFileFromS3(reln, forkNum);

	mdread(reln, forkNum, blockNum, buffer);
}

void
yezzey_write(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, char *buffer, bool skipFsync)
{
	elog(SmgrTrace, "[YEZZEY_SMGR] write");
	if (!ensureFileLocal(reln, forkNum))
		getFileFromS3(reln, forkNum);
	
	mdwrite(reln, forkNum, blockNum, buffer, skipFsync);
}

void
yezzey_writeback(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, BlockNumber nBlocks)
{
	elog(SmgrTrace, "[YEZZEY_SMGR] writeback");
	if (!ensureFileLocal(reln, forkNum))
		getFileFromS3(reln, forkNum);

	mdwriteback(reln, forkNum, blockNum, nBlocks);
}

BlockNumber
yezzey_nblocks(SMgrRelation reln, ForkNumber forkNum)
{
	elog(SmgrTrace, "[YEZZEY_SMGR] nblocks");
	if (!ensureFileLocal(reln, forkNum))
		getFileFromS3(reln, forkNum);
	
	return mdnblocks(reln, forkNum);
}

void
yezzey_truncate(SMgrRelation reln, ForkNumber forkNum, BlockNumber nBlocks)
{
	elog(SmgrTrace, "[YEZZEY_SMGR] truncate");
	if (!ensureFileLocal(reln, forkNum))
		getFileFromS3(reln, forkNum);
	
	mdtruncate(reln, forkNum, nBlocks);
}

void
yezzey_immedsync(SMgrRelation reln, ForkNumber forkNum)
{
	elog(SmgrTrace, "[YEZZEY_SMGR] immedsync");
	if (!ensureFileLocal(reln, forkNum))
		getFileFromS3(reln, forkNum);
	
	mdimmedsync(reln, forkNum);
}

static const struct f_smgr yezzey_smgr =
{
	.smgr_init = yezzey_init,
	.smgr_shutdown = NULL,
	.smgr_open = yezzey_open,
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

void smgr_init_yezzey(void)
{
	smgr_init_standard();
	yezzey_init();
}

void
_PG_init(void)
{
	DefineCustomStringVariable("yezzey.S3_archive",
				   "getting file from S3",
				   NULL,
				   &s3_archive,
				   "",
				   PGC_USERSET,
				   0,
				   NULL, NULL, NULL);

	elog(SmgrTrace, "[YEZZEY_SMGR] set hook");
	smgr_hook = smgr_yezzey;
	smgr_init_hook = smgr_init_yezzey;
}
