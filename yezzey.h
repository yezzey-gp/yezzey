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

bool ensureFileLocal(SMgrRelation reln, ForkNumber forkNum);
void sendFileToS3(const char *localPath);
void updateMoveTable(const char *oid, const char *forkName, const char *segNum, const bool isLocal);
void removeLocalFile(const char *localPath);
void sendOidToS3(const char *oid, const char *forkName, const char *segNum);

#endif /* YEZZEY_H */