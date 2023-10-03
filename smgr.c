
#include "postgres.h"

#include "miscadmin.h"
#include "utils/snapmgr.h"

#if PG_VERSION_NUM >= 130000
#include "postmaster/interrupt.h"
#endif

#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/md.h"
#include "storage/shmem.h"
#include "storage/smgr.h"

#if PG_VERSION_NUM >= 100000
#include "common/file_perm.h"
#else
#include "access/xact.h"
#endif

#include "utils/elog.h"

#ifdef GPBUILD
#include "cdb/cdbvars.h"
#endif

#include "catalog/pg_tablespace.h"

#include "yezzey.h"

// For GpIdentity
#include "c.h"
#include "cdb/cdbvars.h"

#if GP_VERSION_NUM >= 70000
#include "access/aomd.h"
#endif

#include "storage.h"

#include "proxy.h"

/*
 * Construct external storage filepath.
 *
 * Assepts initialized StringInfoData as its first param
 */

static void constructExtenrnalStorageFilepath(StringInfoData *path,
                                              RelFileNode rnode,
                                              BackendId backend,
                                              ForkNumber forkNum,
                                              BlockNumber blkno) {
  char *relpath;
  BlockNumber blockNum;

  relpath = relpathbackend(rnode, backend, forkNum);

  appendStringInfoString(path, relpath);
  blockNum = blkno / ((BlockNumber)RELSEG_SIZE);

  if (blockNum > 0)
    appendStringInfo(path, ".%u", blockNum);

  pfree(relpath);
}

/* TODO: remove, or use external_storage.h funcs */
int loadFileFromExternalStorage(RelFileNode rnode, BackendId backend,
                                ForkNumber forkNum, BlockNumber blkno) {
  StringInfoData path;
  initStringInfo(&path);

  constructExtenrnalStorageFilepath(&path, rnode, backend, forkNum, blkno);

  return 0;
}

void yezzey_init(void) {
  elog(yezzey_log_level, "[YEZZEY_SMGR] init");
  mdinit();
}

#ifndef GPBUILD
void yezzey_open(SMgrRelation reln) {
  if ((reln->smgr_rnode).node.spcNode == YEZZEYTABLESPACE_OID) {
    /*do nothing */
    return;
  }

  mdopen(reln);
}
#endif

void yezzey_close(SMgrRelation reln, ForkNumber forkNum) {
  if ((reln->smgr_rnode).node.spcNode == YEZZEYTABLESPACE_OID) {
    /*do nothing */
    return;
  }

  mdclose(reln, forkNum);
}

void yezzey_create(SMgrRelation reln, ForkNumber forkNum, bool isRedo) {
  if ((reln->smgr_rnode).node.spcNode == YEZZEYTABLESPACE_OID) {
    /*do nothing */
    return;
  }

  mdcreate(reln, forkNum, isRedo);
}

#ifdef GPBUILD
void yezzey_create_ao(RelFileNodeBackend rnode, int32 segmentFileNum,
                      bool isRedo) {
  if (rnode.node.spcNode == YEZZEYTABLESPACE_OID) {
    /*do nothing */
    return;
  }

  mdcreate_ao(rnode, segmentFileNum, isRedo);
}
#endif

bool yezzey_exists(SMgrRelation reln, ForkNumber forkNum) {
  if ((reln->smgr_rnode).node.spcNode == YEZZEYTABLESPACE_OID) {
    /*do nothing */
    return true;
  }

  return mdexists(reln, forkNum);
}


#if PG_VERSION_NUM < 150000
void yezzey_unlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo)
#else
void yezzey_unlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo,
                   char relstorage)
#endif
{
  if (rnode.node.spcNode == YEZZEYTABLESPACE_OID) {
    /*do nothing */
    return;
  }

#if PG_VERSION_NUM < 150000
  mdunlink(rnode, forkNum, isRedo);
#else
  mdunlink(rnode, forkNum, isRedo, relstorage);
#endif
}


#if GP_VERSION_NUM >= 70000
void
yezzey_unlink_ao(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo)
{
  if (rnode.node.spcNode == YEZZEYTABLESPACE_OID) {
    /*do nothing */
    return;
  }
  mdunlink_ao(rnode, forkNum, isRedo);
}
#endif


void yezzey_extend(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum,
                   char *buffer, bool skipFsync) {
  if ((reln->smgr_rnode).node.spcNode == YEZZEYTABLESPACE_OID) {
    /*do nothing */
    return;
  }
  mdextend(reln, forkNum, blockNum, buffer, skipFsync);
}

#if PG_VERSION_NUM >= 130000
bool
#else
void
#endif
yezzey_prefetch(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum)
{
  if ((reln->smgr_rnode).node.spcNode == YEZZEYTABLESPACE_OID) {
    /*do nothing */
#if PG_VERSION_NUM >= 130000
    return true;
#else
    return;
#endif
  }

  return mdprefetch(reln, forkNum, blockNum);
}

void yezzey_read(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum,
                 char *buffer) {
  if ((reln->smgr_rnode).node.spcNode == YEZZEYTABLESPACE_OID) {
    /*do nothing */
    return;
  }

  mdread(reln, forkNum, blockNum, buffer);
}

void yezzey_write(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum,
                  char *buffer, bool skipFsync) {
  if ((reln->smgr_rnode).node.spcNode == YEZZEYTABLESPACE_OID) {
    /*do nothing */
    return;
  }

  mdwrite(reln, forkNum, blockNum, buffer, skipFsync);
}

#if GP_VERSION_NUM >= 70000
void yezzey_writeback(SMgrRelation reln, ForkNumber forkNum,
                      BlockNumber blockNum, BlockNumber nBlocks) {
  if ((reln->smgr_rnode).node.spcNode == YEZZEYTABLESPACE_OID) {
    /*do nothing */
    return;
  }

  mdwriteback(reln, forkNum, blockNum, nBlocks);
}
#endif

BlockNumber yezzey_nblocks(SMgrRelation reln, ForkNumber forkNum) {
  if ((reln->smgr_rnode).node.spcNode == YEZZEYTABLESPACE_OID) {
    /*do nothing */
    return 0;
  }

  return mdnblocks(reln, forkNum);
}

void yezzey_truncate(SMgrRelation reln, ForkNumber forkNum,
                     BlockNumber nBlocks) {
  if ((reln->smgr_rnode).node.spcNode == YEZZEYTABLESPACE_OID) {
    /*do nothing */
    return;
  }

  mdtruncate(reln, forkNum, nBlocks);
}

void yezzey_immedsync(SMgrRelation reln, ForkNumber forkNum) {
  if ((reln->smgr_rnode).node.spcNode == YEZZEYTABLESPACE_OID) {
    /*do nothing */
    return;
  }

  mdimmedsync(reln, forkNum);
}


#if GP_VERSION_NUM < 70000
static const struct f_smgr yezzey_smgr = {
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
#else

static const f_smgr yezzey_smgrsw[] = {
	/* magnetic disk */
	{
		.smgr_init = yezzey_init,
		.smgr_shutdown = NULL,
		.smgr_close = yezzey_close,
		.smgr_create = yezzey_create,
    .smgr_create_ao = yezzey_create_ao,
		.smgr_exists = yezzey_exists,
		.smgr_unlink = yezzey_unlink,
		.smgr_extend = yezzey_extend,
		.smgr_prefetch = yezzey_prefetch,
		.smgr_read = yezzey_read,
		.smgr_write = yezzey_write,
		.smgr_writeback = yezzey_writeback,
		.smgr_nblocks = mdnblocks,
		.smgr_truncate = yezzey_nblocks,
		.smgr_immedsync = yezzey_immedsync,
	},
	/*
	 * Relation files that are different from heap, characterised by:
	 *     1. variable blocksize
	 *     2. block numbers are not consecutive
	 *     3. shared buffers are not used
	 * Append-optimized relation files currently fall in this category.
	 */
	{
		.smgr_init = NULL,
		.smgr_shutdown = NULL,
		.smgr_close = yezzey_close,
		.smgr_create = yezzey_create,
    .smgr_create_ao = yezzey_create_ao,
		.smgr_exists = yezzey_exists,
		.smgr_unlink = yezzey_unlink_ao,
		.smgr_extend = yezzey_extend,
		.smgr_prefetch = yezzey_prefetch,
		.smgr_read = yezzey_read,
		.smgr_write = yezzey_write,
		.smgr_writeback = yezzey_writeback,
		.smgr_nblocks = yezzey_nblocks,
		.smgr_truncate = yezzey_truncate,
		.smgr_immedsync = yezzey_immedsync,
	}
};
#endif

/*

typedef struct f_smgr_ao {
	int64       (*smgr_NonVirtualCurSeek) (SMGRFile file);
	int64 		(*smgr_FileSeek) (SMGRFile file, int64 offset, int whence);
	void 		(*smgr_FileClose)(SMGRFile file);
	int         (*smgr_FileTruncate) (SMGRFile file, int64 offset, uint32 wait_event_info);
	SMGRFile    (*smgr_PathNameOpenFile) (const char * fileName, int fileFlags, int fileMode);
	int         (*smgr_FileWrite)(SMGRFile file, char *buffer, int amount, off_t offset, uint32 wait_event_info);
  int         (*smgr_FileRead)(SMGRFile file, char *buffer, int amount,off_t offset, uint32 wait_event_info );
	int	        (*smgr_FileSync)(SMGRFile file);
} f_smgr_ao;

*/

static const struct f_smgr_ao yezzey_smgr_ao = {
    .smgr_FileClose = yezzey_FileClose,
    .smgr_AORelOpenSegFile = yezzey_AORelOpenSegFile,
    .smgr_FileWrite = yezzey_FileWrite,
    .smgr_FileRead = yezzey_FileRead,
    .smgr_FileSync = yezzey_FileSync,
    .smgr_FileTruncate = yezzey_FileTruncate,
#if GP_VERSION_NUM < 70000
    .smgr_NonVirtualCurSeek = yezzey_NonVirtualCurSeek,
    .smgr_FileSeek = yezzey_FileSeek,
#else
    .smgr_FileDiskSize = yezzey_FileDiskSize,
#endif
};

#if GP_VERSION_NUM < 70000
const f_smgr *smgr_yezzey(BackendId backend, RelFileNode rnode) {
  return &yezzey_smgr;
}
#else
const f_smgr *smgr_yezzey(BackendId backend, RelFileNode rnode, SMgrImpl which) {
  return &yezzey_smgrsw[which];
}
#endif

const f_smgr_ao *smgrao_yezzey() { return &yezzey_smgr_ao; }

void smgr_init_yezzey(void) {
  smgr_init_standard();
  yezzey_init();
}
