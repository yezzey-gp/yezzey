

#ifndef EXTERNAL_STORAGE_H
#define EXTERNAL_STORAGE_H

// XXX: todo proder interface for external storage offloading


extern char *s3_getter;
extern char *s3_putter;
extern char *s3_prefix;

int
offloadFileToExternalStorage(const char *localPath, const char * external_path, int64 modcount, int64 logicalEof);

int
offloadRelationSegment(Relation aorel, RelFileNode rnode, int segno, int64 modcount, int64 logicalEof);

int
loadRelationSegment(RelFileNode rnode, int segno);

char *
buildExternalStorageCommand(const char *s3Command, const char *localPath, const char *externalPath);

bool
ensureFilepathLocal(char *filepath);
bool
ensureFileLocal(RelFileNode rnode, BackendId backend, ForkNumber forkNum, BlockNumber blkno);

int
getFilepathFromS3(const char *filepath);

#endif /* EXTERNAL_STORAGE */