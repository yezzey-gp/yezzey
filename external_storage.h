

#ifndef EXTERNAL_STORAGE_H
#define EXTERNAL_STORAGE_H

// XXX: todo proder interface for external storage offloading


extern char *s3_getter;
extern char *s3_putter;
extern char *s3_prefix;

int
offloadFileToExternalStorage(const char *localPath);

int
offloadRelationSegment(RelFileNode rnode, int segno);

char *
buildExternalStorageCommand(const char *s3Command, const char *localPath, const char *externalPath);

bool
ensureFilepathLocal(char *filepath);
bool
ensureFileLocal(RelFileNode rnode, BackendId backend, ForkNumber forkNum, BlockNumber blkno);

void
getFilepathFromS3(const char *filepath);

#endif /* EXTERNAL_STORAGE */