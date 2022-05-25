#include "postgres.h"

#include "storage/smgr.h"
#include "storage/md.h"

PG_MODULE_MAGIC;

const int SmgrTrace = LOG;

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
	mdclose(reln, forkNum);
}

void
yezzey_create(SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{
	elog(SmgrTrace, "[YEZZEY_SMGR] create");
	mdcreate(reln, forkNum, isRedo);
}

bool
yezzey_exists(SMgrRelation reln, ForkNumber forkNum)
{
	elog(SmgrTrace, "[YEZZEY_SMGR] exists");
	return mdexists(reln, forkNum);
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
	elog(SmgrTrace, "[YEZZEY_SMGR] extend");
	mdextend(reln, forkNum, blockNum, buffer, skipFsync);
}

bool
yezzey_prefetch(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum)
{
	elog(SmgrTrace, "[YEZZEY_SMGR] prefetch");
	return mdprefetch(reln, forkNum, blockNum);
}

void
yezzey_read(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, char *buffer)
{
	elog(SmgrTrace, "[YEZZEY_SMGR] read");
	mdread(reln, forkNum, blockNum, buffer);
}

void
yezzey_write(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, char *buffer, bool skipFsync)
{
	elog(SmgrTrace, "[YEZZEY_SMGR] write");
	mdwrite(reln, forkNum, blockNum, buffer, skipFsync);
}

void
yezzey_writeback(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, BlockNumber nBlocks)
{
	elog(SmgrTrace, "[YEZZEY_SMGR] writeback");
	mdwriteback(reln, forkNum, blockNum, nBlocks);
}

BlockNumber
yezzey_nblocks(SMgrRelation reln, ForkNumber forkNum)
{
	elog(SmgrTrace, "[YEZZEY_SMGR] nblocks");
	return mdnblocks(reln, forkNum);
}

void
yezzey_truncate(SMgrRelation reln, ForkNumber forkNum, BlockNumber nBlocks)
{
	elog(SmgrTrace, "[YEZZEY_SMGR] truncate");
	mdtruncate(reln, forkNum, nBlocks);
}

void
yezzey_immedsync(SMgrRelation reln, ForkNumber forkNum)
{
	elog(SmgrTrace, "[YEZZEY_SMGR] immedsync");
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
	elog(LOG, "[YEZZEY_SMGR] set hook");
	smgr_hook = smgr_yezzey;
	smgr_init_hook = smgr_init_yezzey;
}
