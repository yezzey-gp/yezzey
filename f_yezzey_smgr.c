#include "postgres.h"

#include "storage/smgr.h"
#include "storage/md.h"

PG_MODULE_MAGIC;

const int SmgrTrace = LOG;

yezzey_init(void)
{
	mdinit();	
	elog(SmgrTrace, "[YEZZEY_SMGR] init");
}

void
yezzey_open(SMgrRelation reln)
{
	mdopen(reln);
	elog(SmgrTrace, "[YEZZEY_SMGR] open");
}

void
yezzey_close(SMgrRelation reln, ForkNumber forkNum)
{
	mdclose(reln, forkNum);
	elog(SmgrTrace, "[YEZZEY_SMGR] close");
}

void
yezzey_create(SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{
	mdcreate(reln, forkNum, isRedo);
	elog(SmgrTrace, "[YEZZEY_SMGR] create");
}

bool
yezzey_exists(SMgrRelation reln, ForkNumber forkNum)
{
	mdexists(reln, forkNum);
	elog(SmgrTrace, "[YEZZEY_SMGR] exists");
}

void
yezzey_unlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo)
{
	mdunlink(rnode, forkNum, isRedo);
	elog(SmgrTrace, "[YEZZEY_SMGR] unlink");
}

void
yezzey_extend(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, char *buffer, bool skipFsync)
{
	mdextend(reln, forkNum, blockNum, buffer, skipFsync);
	elog(SmgrTrace, "[YEZZEY_SMGR] extend");
}

bool
yezzey_prefetch(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum)
{
	mdprefetch(reln, forkNum, blockNum);
	elog(SmgrTrace, "[YEZZEY_SMGR] prefetch");
}

void
yezzey_read(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, char *buffer)
{
	mdread(reln, forkNum, blockNum, buffer);
	elog(SmgrTrace, "[YEZZEY_SMGR] read");
}

void
yezzey_write(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, char *buffer, bool skipFsync)
{
	mdwrite(reln, forkNum, blockNum, buffer, skipFsync);
	elog(SmgrTrace, "[YEZZEY_SMGR] write");
}

void
yezzey_writeback(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, BlockNumber nBlocks)
{
	mdwriteback(reln, forkNum, blockNum, nBlocks);
	elog(SmgrTrace, "[YEZZEY_SMGR] writeback");
}

BlockNumber
yezzey_nblocks(SMgrRelation reln, ForkNumber forkNum)
{
	mdnblocks(reln, forkNum);
	elog(SmgrTrace, "[YEZZEY_SMGR] nblocks");
}

void
yezzey_truncate(SMgrRelation reln, ForkNumber forkNum, BlockNumber nBlocks)
{
	mdtruncate(reln, forkNum, nBlocks);
	elog(SmgrTrace, "[YEZZEY_SMGR] truncate");
}

void
yezzey_immedsync(SMgrRelation reln, ForkNumber forkNum)
{
	mdimmedsync(reln, forkNum);
	elog(SmgrTrace, "[YEZZEY_SMGR] immedsync");
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

void
_PG_init(void)
{
	smgr_hook = smgr_yezzey;
}
