/*-------------------------------------------------------------------------
 *
 * yezzey.h
 *        GP/PG Extention to offload cold data segments to external storage
 *
 * PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *                yezzey.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef YEZZEY_H
#define YEZZEY_H

#include "utils/guc.h"

void sendFileToS3(const char *localPath);
void updateMoveTable(const char *oid, const char *forkName, const uint32 segNum, const bool isLocal);
int removeLocalFile(const char *localPath);
void sendOidToS3(const char *oid, const char *forkName, const uint32 segNum);

void yezzey_prepare(void);
void yezzey_finish(void);
void sendTablesToS3(void);
void getFileFromS3(RelFileNode rnode, BackendId backend, ForkNumber forkNum, BlockNumber blockNum);
char *buildS3Command(const char *s3Command, const char *s3Path, const char *localPath);

int offload_relation_internal(Oid reloid);
int load_relation_internal(Oid reloid);

int loadFileFromExternalStorage(RelFileNode rnode, BackendId backend, ForkNumber forkNum, BlockNumber blkno);

void yezzey_init(void);

/*
* SMGR - related functions
*/
#ifndef GPBUILD
void yezzey_open(SMgrRelation reln);
#endif
void yezzey_close(SMgrRelation reln, ForkNumber forkNum);
void yezzey_create(SMgrRelation reln, ForkNumber forkNum, bool isRedo);
#ifdef GPBUILD
void yezzey_create_ao(RelFileNodeBackend rnode, int32 segmentFileNum, bool isRedo);
#endif
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
#ifndef GPBUILD
void yezzey_writeback(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum, BlockNumber nBlocks);
#endif
BlockNumber yezzey_nblocks(SMgrRelation reln, ForkNumber forkNum);
void yezzey_truncate(SMgrRelation reln, ForkNumber forkNum, BlockNumber nBlocks);
void yezzey_immedsync(SMgrRelation reln, ForkNumber forkNum);

void addToMoveTable(char *tableName);
void processTables(void);

const f_smgr *smgr_yezzey(BackendId backend, RelFileNode rnode);
#ifdef GPBUILD
void smgr_warmup_yezzey(RelFileNode rnode, char *filepath);
#endif
void smgr_init_yezzey(void);

int yezzey_log_level;

void _PG_init(void);


#endif /* YEZZEY_H */
