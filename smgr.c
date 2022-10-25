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

#include "smgr_s3.h"

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


// For GpIdentity
#include "c.h"
#include "cdb/cdbvars.h"

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

	appendStringInfoString(path, relpath);
	blockNum = blkno / ((BlockNumber) RELSEG_SIZE);

	if (blockNum > 0)
		appendStringInfo(path, ".%u", blockNum);

	pfree(relpath);
}


/* TODO: remove, or use external_storage.h funcs */ 
int
loadFileFromExternalStorage(RelFileNode rnode, BackendId backend, ForkNumber forkNum, BlockNumber blkno)
{
	StringInfoData path;
	initStringInfo(&path);

	constructExtenrnalStorageFilepath(&path, rnode, backend, forkNum, blkno);
	
	return getFilepathFromS3(path.data);
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
		loadFileFromExternalStorage((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0);
	
	mdopen(reln);
}
#endif

void
yezzey_close(SMgrRelation reln, ForkNumber forkNum)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] close");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0))
		loadFileFromExternalStorage((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0);
	
	mdclose(reln, forkNum);
}

void
yezzey_create(SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] create");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0))
		loadFileFromExternalStorage((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0);
	
	mdcreate(reln, forkNum, isRedo);
}

#ifdef GPBUILD
void
yezzey_create_ao(RelFileNodeBackend rnode, int32 segmentFileNum, bool isRedo)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] create ao");
	if (!ensureFileLocal((rnode).node, (rnode).backend, MAIN_FORKNUM, (uint32)0))
		loadFileFromExternalStorage((rnode).node, (rnode).backend, MAIN_FORKNUM, (uint32)0);

	mdcreate_ao(rnode, segmentFileNum, isRedo);
}
#endif

bool
yezzey_exists(SMgrRelation reln, ForkNumber forkNum)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] exists");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0))
		loadFileFromExternalStorage((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0);

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
                loadFileFromExternalStorage((rnode).node, (rnode).backend, MAIN_FORKNUM, (uint32)0);

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
		loadFileFromExternalStorage((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum);
	
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
		loadFileFromExternalStorage((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum);
	
	return mdprefetch(reln, forkNum, blockNum);
}

void
yezzey_read(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, char *buffer)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] read");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum))
		loadFileFromExternalStorage((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum);

	mdread(reln, forkNum, blockNum, buffer);
}

void
yezzey_write(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, char *buffer, bool skipFsync)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] write");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum))
		loadFileFromExternalStorage((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum);
	
	mdwrite(reln, forkNum, blockNum, buffer, skipFsync);
}

#ifndef GPBUILD
void
yezzey_writeback(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, BlockNumber nBlocks)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] writeback");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum))
		loadFileFromExternalStorage((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, blockNum);

	mdwriteback(reln, forkNum, blockNum, nBlocks);
}
#endif

BlockNumber
yezzey_nblocks(SMgrRelation reln, ForkNumber forkNum)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] nblocks");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0))
		loadFileFromExternalStorage((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0);
	
	return mdnblocks(reln, forkNum);
}

void
yezzey_truncate(SMgrRelation reln, ForkNumber forkNum, BlockNumber nBlocks)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] truncate");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0))
		loadFileFromExternalStorage((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0);
	
	mdtruncate(reln, forkNum, nBlocks);
}

void
yezzey_immedsync(SMgrRelation reln, ForkNumber forkNum)
{
	elog(yezzey_log_level, "[YEZZEY_SMGR] immedsync");
	if (forkNum == MAIN_FORKNUM && !ensureFileLocal((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0))
		loadFileFromExternalStorage((reln->smgr_rnode).node, (reln->smgr_rnode).backend, MAIN_FORKNUM, (uint32)0);
	
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



/*
typedef struct f_smgr_ao {
	int64       (*smgr_NonVirtualCurSeek) (SMGRFile file);
	int64 		(*smgr_FileSeek) (SMGRFile file, int64 offset, int whence);
	void 		(*smgr_FileClose)(SMGRFile file);
	SMGRFile    (*smgr_PathNameOpenFile) (FileName fileName, int fileFlags, int fileMode);
	int         (*smgr_FileWrite)(SMGRFile file, char *buffer, int amount);
    int         (*smgr_FileRead)(SMGRFile file, char *buffer, int amount);
} f_smgr_ao;
*/



int64 yezzey_NonVirtualCurSeek (SMGRFile file);
void yezzey_FileClose(SMGRFile file);
int64 yezzey_FileSeek(SMGRFile file, int64 offset, int whence);
int yezzey_FileSync(SMGRFile file);
SMGRFile yezzey_PathNameOpenFile (FileName fileName, int fileFlags, int fileMode);
int yezzey_FileWrite(SMGRFile file, char *buffer, int amount);
int yezzey_FileRead(SMGRFile file, char *buffer, int amount);


typedef struct yezzey_vfd {
	int y_vfd;
	int localTmpVfd; /* for writing files */
	char * filepath;
	int fileFlags; 
	int fileMode;
	int64 offset;
	int64 virtualSize;
	void * rhandle;
	void * whandle;
} yezzey_vfd;

#define YEZZEY_VANANT_VFD 0
#define YEZZEY_NOT_OPENED 1
#define YEZZEY_OPENED 2
#define YEZZEY_MIN_VFD 3
#define MAXVFD 100

File virtualEnsure(SMGRFile file);

yezzey_vfd yezzey_vfd_cache[MAXVFD];

File s3ext = -2;

/* lazy allocate external storage connections */
void readprepare(SMGRFile file) {
	StringInfoData localTmpPath;

	if (yezzey_vfd_cache[file].rhandle) {
		return;
	}
	yezzey_vfd_cache[file].rhandle = createReaderHandle(GpIdentity.segindex, yezzey_vfd_cache[file].filepath);
	Assert(yezzey_vfd_cache[file].rhandle != NULL);


	initStringInfo(&localTmpPath);
	if (yezzey_vfd_cache[file].localTmpVfd) return;

	appendStringInfo(&localTmpPath, "%s_tmpbuf", yezzey_vfd_cache[file].filepath);

	yezzey_vfd_cache[file].localTmpVfd = 
	PathNameOpenFile(localTmpPath.data, yezzey_vfd_cache[file].fileFlags, yezzey_vfd_cache[file].fileMode);
	if (yezzey_vfd_cache[file].localTmpVfd <= 0) {
		// is ok
		elog(WARNING, "failed to open proxy file for read %s", localTmpPath.data);
	} else {
		elog(WARNING, "opened proxy file for read %s", localTmpPath.data);
	}
}

void writeprepare(SMGRFile file) {
	StringInfoData localTmpPath;
	initStringInfo(&localTmpPath);
	if (yezzey_vfd_cache[file].localTmpVfd) return;

	appendStringInfo(&localTmpPath, "%s_tmpbuf", yezzey_vfd_cache[file].filepath);

	elog(WARNING, "creating proxy file for write %s", localTmpPath.data);

	yezzey_vfd_cache[file].localTmpVfd = 
	PathNameOpenFile(localTmpPath.data, yezzey_vfd_cache[file].fileFlags | O_CREAT, yezzey_vfd_cache[file].fileMode);
	if (yezzey_vfd_cache[file].localTmpVfd <= 0) {
		elog(WARNING, "error creating proxy file for write %s: %d", localTmpPath.data, errno);
	}
	FileSeek(yezzey_vfd_cache[file].localTmpVfd, 0, SEEK_END);
}

File virtualEnsure(SMGRFile file) {
	File internal_vfd;
	int rc;
	if (yezzey_vfd_cache[file].y_vfd == YEZZEY_VANANT_VFD) {
		elog(ERROR, "attempt to ensure locality of not opened file");
	}	
	if (yezzey_vfd_cache[file].y_vfd == YEZZEY_NOT_OPENED) {
		// not opened yet
		if (!ensureFilepathLocal(yezzey_vfd_cache[file].filepath)) {
			// do s3 read
			return s3ext;
			// if ((rc = getFilepathFromS3(yezzey_vfd_cache[file].filepath)) < 0) {
       		// 	elog(ERROR, "failed to download open file");
			// }
		}

		internal_vfd = PathNameOpenFile(yezzey_vfd_cache[file].filepath,
		 yezzey_vfd_cache[file].fileFlags, yezzey_vfd_cache[file].fileMode);
		if (internal_vfd == -1) {
			// error
			elog(ERROR, "failed to proxy open file");
		}
		elog(yezzey_ao_log_level, "y vfd become %d", internal_vfd);
		yezzey_vfd_cache[file].y_vfd = internal_vfd;
		/* allocate handle struct */
	}

	return yezzey_vfd_cache[file].y_vfd;
}

int64 yezzey_NonVirtualCurSeek(SMGRFile file) {
	File actual_fd = virtualEnsure(file);
	elog(yezzey_ao_log_level, "non virt file seek with %d actual %d", file, actual_fd);
	if (actual_fd == s3ext) {
		return yezzey_vfd_cache[file].offset;
	}
	return FileNonVirtualCurSeek(actual_fd);
}


int64 yezzey_FileSeek(SMGRFile file, int64 offset, int whence) {
	File actual_fd = virtualEnsure(file);
	if (actual_fd == s3ext) {
		// what?
		return offset; 
	}
	elog(yezzey_ao_log_level, "file seek with fd %d offset %ld actual %d", file, offset, actual_fd);
	return FileSeek(actual_fd, offset, whence);
}

int	yezzey_FileSync(SMGRFile file) {
	File actual_fd = virtualEnsure(file);
	if (actual_fd == s3ext) {
		/* s3 always sync ? */
		return 0;
	}
	elog(yezzey_ao_log_level, "file sync with fd %d actual %d", file, actual_fd);
	return FileSync(actual_fd);
}

SMGRFile yezzey_PathNameOpenFile(FileName fileName, int fileFlags, int fileMode) {
	int i;
	elog(yezzey_ao_log_level, "path name open file %s", fileName);

	/* lookup for virtual file desc entry */
	for (i = 0; i < MAXVFD; ++i) {
		if (yezzey_vfd_cache[i].y_vfd == YEZZEY_VANANT_VFD) {
			yezzey_vfd_cache[i].filepath = strdup(fileName);
			yezzey_vfd_cache[i].fileFlags = fileFlags;
			yezzey_vfd_cache[i].fileMode = fileMode;
			if (yezzey_vfd_cache[i].filepath == NULL) {
				elog(ERROR, "out of mem");
			}

			yezzey_vfd_cache[i].y_vfd = YEZZEY_NOT_OPENED;
			return i;
		}
	}
/* no martch*/
	return -1;
}

void yezzey_FileClose(SMGRFile file) {
	File actual_fd = virtualEnsure(file);
	elog(yezzey_ao_log_level, "file close with %d actual %d", file, actual_fd);
	if (actual_fd == s3ext) {
		yezzey_complete_r_transfer_data(&yezzey_vfd_cache[file].rhandle);
		yezzey_complete_w_transfer_data(&yezzey_vfd_cache[file].whandle);
	} else {
		FileClose(actual_fd);
	}

	if (yezzey_vfd_cache[file].localTmpVfd > 0) {
		FileClose(yezzey_vfd_cache[file].localTmpVfd);
	}

	memset(&yezzey_vfd_cache[file], 0, sizeof(yezzey_vfd));
}

#define ALLOW_MODIFY_EXTERNAL_TABLE

int yezzey_FileWrite(SMGRFile file, char *buffer, int amount) {
	File actual_fd = virtualEnsure(file);
	if (actual_fd == s3ext) {
#ifdef ALLOW_MODIFY_EXTERNAL_TABLE
		int curr = amount;
		writeprepare(file);
		/*local writes*/
		return FileWrite(yezzey_vfd_cache[file].localTmpVfd, buffer, amount);
		// if (!yezzey_writer_transfer_data(yezzey_vfd_cache[file].whandle, buffer, &curr)) {
		// 	elog(yezzey_ao_log_level, "problem while direct write from s3 with %d", file);
		// 	return -1;
		// }

		// yezzey_vfd_cache[file].offset += curr;
		
		// elog(yezzey_ao_log_level, "file write with %d", file);
		// return curr;
#else
		elog(ERROR, "external table modifications are not supported yet");
#endif

	}
	elog(yezzey_ao_log_level, "file write with %d, actual %d", file, actual_fd);
	return FileWrite(actual_fd, buffer, amount);
}

int yezzey_FileRead(SMGRFile file, char *buffer, int amount) {
	File actual_fd = virtualEnsure(file);
	int curr = amount;

	if (actual_fd == s3ext) {
		readprepare(file);
		if (yezzey_reader_empty(yezzey_vfd_cache[file].rhandle)) {
			if (yezzey_vfd_cache[file].localTmpVfd <= 0) {
				return 0;
			}
			/* tring to read proxy file */
			curr = FileRead(yezzey_vfd_cache[file].localTmpVfd, buffer, amount);
			/* fall throught */

			elog(WARNING, "read from proxy file %d", curr);
		} else {
			if (!yezzey_reader_transfer_data(yezzey_vfd_cache[file].rhandle, buffer, &curr)) {
				elog(yezzey_ao_log_level, "problem while direct read from s3 read with %d curr: %d", file, curr);
				return -1;
			}

			if (yezzey_reader_empty(yezzey_vfd_cache[file].rhandle)) {
				if (yezzey_vfd_cache[file].localTmpVfd <= 0) {
					return 0;
				}
				/* tring to read proxy file */
				curr = FileRead(yezzey_vfd_cache[file].localTmpVfd, buffer, amount);
				/* fall throught */

				elog(WARNING, "read from proxy file %d", curr);
			}
		}

		yezzey_vfd_cache[file].offset += curr;

		elog(yezzey_ao_log_level, "file read with %d, actual %d, amount %d real %d", file, actual_fd, amount, curr);
		return curr;
	}
	return FileRead(actual_fd, buffer, amount);
}

static const struct f_smgr_ao yezzey_smgr_ao =
{
	.smgr_NonVirtualCurSeek = yezzey_NonVirtualCurSeek,
	.smgr_FileSeek = yezzey_FileSeek,
	.smgr_FileClose = yezzey_FileClose,
	.smgr_PathNameOpenFile = yezzey_PathNameOpenFile,
	.smgr_FileWrite = yezzey_FileWrite,
	.smgr_FileRead = yezzey_FileRead,
	.smgr_FileSync = yezzey_FileSync,
};


const f_smgr *
smgr_yezzey(BackendId backend, RelFileNode rnode)
{
	return &yezzey_smgr;
}

const f_smgr_ao *
smgrao_yezzey()
{
	return &yezzey_smgr_ao;
}

void
smgr_init_yezzey(void)
{
	smgr_init_standard();
	yezzey_init();
}