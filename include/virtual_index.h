#pragma once

#include "pg.h"

#ifdef __cplusplus

#include <string>
#include <vector>

#include "chunkinfo.h"

#endif

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

/* ----------------
 *		compiler constants for pg_database
 * ----------------
 */

typedef struct {
  int segno;               /* AO relation block file no */
  int64_t start_offset;    /* start_offset of block file chunk */
  int64_t finish_offset;   /* finish_offset of block file chunk */
  int32_t encrypted;       /* Is chunk in external storage encrypted */
  int32_t reused;          /* Is chunk writted by yezzey or reused from backup */
  int64_t modcount;        /* modcount of block file chunk */
  XLogRecPtr lsn;          /* Chunk lsn */
  text x_path;             /* external path */
} FormData_yezzey_virtual_index;

#define Natts_yezzey_virtual_index 9
#define Anum_yezzey_virtual_index_blkno 1
#define Anum_yezzey_virtual_index_filenode 2
#define Anum_yezzey_virtual_start_off 3
#define Anum_yezzey_virtual_finish_off 4
#define Anum_yezzey_virtual_encrypted 5
#define Anum_yezzey_virtual_reused_from_backup 6
#define Anum_yezzey_virtual_modcount 7
#define Anum_yezzey_virtual_lsn 8
#define Anum_yezzey_virtual_x_path 9

EXTERNC Oid YezzeyCreateAuxIndex(Relation aorel);

EXTERNC Oid YezzeyFindAuxIndex(Oid reloid);

EXTERNC void emptyYezzeyIndex(Oid yezzey_index_oid);

EXTERNC void emptyYezzeyIndexBlkno(Oid yezzey_index_oid, int blkno,
                                   Oid relfilenode);

#ifdef __cplusplus
void YezzeyVirtualIndexInsert(Oid yandexoid /*yezzey auxiliary index oid*/,
                              int64_t segindx, Oid relfilenodeOid,
                              int64_t offset_start, int64_t offset_finish,
                              int32_t encrypted, int32_t reused,
                              int64_t modcount, XLogRecPtr lsn,
                              const char *x_path /* external path */);

std::vector<ChunkInfo>
YezzeyVirtualGetOrder(Oid yandexoid /*yezzey auxiliary index oid*/, int blkno,
                      Oid relfilenode);
#else
#endif
