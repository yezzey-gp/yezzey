

#ifndef EXTERNAL_STORAGE_H
#define EXTERNAL_STORAGE_H

// XXX: todo proder interface for external storage offloading


extern char *storage_prefix;
extern char *storage_bucket;

int
offloadFileToExternalStorage(const char *relname, const char *localPath, int64 modcount, int64 logicalEof);

int
offloadRelationSegment(Relation aorel, RelFileNode rnode, int segno, int64 modcount, int64 logicalEof);

int
loadRelationSegment(RelFileNode rnode, int segno);

bool
ensureFilepathLocal(const char *filepath);
bool
ensureFileLocal(RelFileNode rnode, BackendId backend, ForkNumber forkNum, BlockNumber blkno);

int
statRelationSpaceUsage(
	Relation aorel, int segno, int64 modcount, int64 logicalEof, size_t *local_bytes, size_t *local_commited_bytes, size_t *external_bytes);

#endif /* EXTERNAL_STORAGE */