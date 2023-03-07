

#ifndef EXTERNAL_STORAGE_H
#define EXTERNAL_STORAGE_H

// XXX: todo proder interface for external storage offloading

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif



typedef struct yezzeyChunkMeta {
	const char * chunkName;
	size_t chunkSize;
} yezzeyChunkMeta;


EXTERNC int
offloadFileToExternalStorage(
    const char * nspname,
    const char *relname, 
    const char *localPath, 
    int64 modcount, 
    int64 logicalEof,
    const char * external_storage_path);

EXTERNC int
offloadRelationSegment(
    Relation aorel, 
    RelFileNode rnode, 
    int segno, 
    int64 modcount, 
    int64 logicalEof,
    bool remove_locally,
    const char * external_storage_path);

EXTERNC int
loadRelationSegment(RelFileNode rnode, int segno);

EXTERNC bool
ensureFilepathLocal(const char *filepath);
EXTERNC bool
ensureFileLocal(RelFileNode rnode, BackendId backend, ForkNumber forkNum, BlockNumber blkno);

EXTERNC int
statRelationSpaceUsage(
	Relation aorel, 
    int segno, 
    int64 modcount, 
    int64 logicalEof, 
    size_t *local_bytes, 
    size_t *local_commited_bytes, 
    size_t *external_bytes);


EXTERNC int
statRelationSpaceUsagePerExternalChunk(
    Relation aorel, 
    int segno, 
    int64 modcount, 
    int64 logicalEof, 
    size_t *local_bytes, 
    size_t *local_commited_bytes, 
    yezzeyChunkMeta **list, 
    size_t *cnt_chunks);

#endif /* EXTERNAL_STORAGE */