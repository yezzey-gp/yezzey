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


#include "postgres.h"

#include "gucs.h"
#include "ystat.h"
#include "yezzey_conf.h"


void yezzey_prepare(void);
void yezzey_finish(void);

int yezzey_offload_relation_internal(Oid reloid, bool remove_locally,
                                     const char *external_path);
int yezzey_load_relation_internal(Oid reloid, const char *dst_path);

int loadFileFromExternalStorage(RelFileNode rnode, BackendId backend,
                                ForkNumber forkNum, BlockNumber blkno);

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
void yezzey_create_ao(RelFileNodeBackend rnode, int32 segmentFileNum,
                      bool isRedo);
#endif
bool yezzey_exists(SMgrRelation reln, ForkNumber forkNum);

#if IsGreenplum7 || IsCloudBerry
void yezzey_unlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo);
#else
void yezzey_unlink(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo,
                   char relstorage);
#endif

#if IsGreenplum7 || IsCloudBerry
void yezzey_unlink_ao(RelFileNodeBackend rnode, ForkNumber forkNum, bool isRedo);
#endif


void yezzey_extend(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum,
                   char *buffer, bool skipFsync);
#if PG_VERSION_NUM >= 130000
bool yezzey_prefetch(SMgrRelation reln, ForkNumber forkNum,
                     BlockNumber blockNum);
#else
void yezzey_prefetch(SMgrRelation reln, ForkNumber forkNum,
                     BlockNumber blockNum);
#endif
void yezzey_read(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum,
                 char *buffer);
void yezzey_write(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum,
                  char *buffer, bool skipFsync);

#if IsGreenplum7 || IsCloudBerry
void yezzey_writeback(SMgrRelation reln, ForkNumber forkNum,
                      BlockNumber blockNum, BlockNumber nBlocks);
#endif
BlockNumber yezzey_nblocks(SMgrRelation reln, ForkNumber forkNum);
void yezzey_truncate(SMgrRelation reln, ForkNumber forkNum,
                     BlockNumber nBlocks);
void yezzey_immedsync(SMgrRelation reln, ForkNumber forkNum);

void addToMoveTable(char *tableName);
void processTables(void);

#if IsGreenplum7 || IsCloudBerry
const f_smgr *smgr_yezzey(BackendId backend, RelFileNode rnode);
#else
const f_smgr *smgr_yezzey(BackendId backend, RelFileNode rnode, SMgrImpl which);
#endif

#ifdef GPBUILD
const f_smgr_ao *smgrao_yezzey();
#endif
void smgr_init_yezzey(void);

extern Datum yezzey_stat_get_external_storage_usage(PG_FUNCTION_ARGS);

void _PG_init(void);

#endif /* YEZZEY_H */
