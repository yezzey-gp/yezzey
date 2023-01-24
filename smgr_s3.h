
#ifndef SMGR_S3_H
#define SMGR_S3_H

//#include "storage/smgr.h"



struct externalChunkMeta {
	int64_t chunkSize;
	const char * chunkName;
};

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

// create external storage reading handle, able to 
// read all files related to given relation
EXTERNC void * createReaderHandle(
	const char * relname, 
	const char * bucket, 
	const char * external_storage_prefix, 
	const char * fileName, 
	int32_t segid);

// create external storage write handle, 
// to transfer segment file of given AO/AOCS relation,
// with operation modcound $modcount$
EXTERNC void * createWriterHandle(
	const char * rhandle_ptr, 
	const char * relname, 
	const char * bucket, 
	const char * external_storage_prefix, 
	const char * fileName, 
	int32_t segid, 
	int64_t modcount);

EXTERNC void * createWriterHandleToPath(
	const char * path,
	int32_t segid, 
	int64_t modcount);

EXTERNC bool yezzey_reader_transfer_data(void * handle, char *buffer, int *amount);
EXTERNC bool yezzey_writer_transfer_data(void * handle, char *buffer, int *amount);

EXTERNC bool yezzey_reader_empty(void *handle);

EXTERNC int64_t yezzey_virtual_relation_size(const char * relname, const char * bucket, const char * external_storage_prefix, const char * fileName, int32_t segid);

EXTERNC int64_t yezzey_calc_virtual_relation_size(void * rhandle_ptr);

EXTERNC void * yezzey_list_relation_chunks(const char * relname, const char * bucket, const char * external_storage_prefix, const char * fileName,  int32_t segid, size_t * cnt_chunks);
EXTERNC int64_t yezzey_copy_relation_chunks(void *rhandle_ptr, struct externalChunkMeta * chunks);

EXTERNC int64_t yezzey_list_relation_chunks_cleanup(void *rhandle_ptr);

EXTERNC bool yezzey_complete_r_transfer_data(void ** handle);
EXTERNC bool yezzey_complete_w_transfer_data(void ** handle);

#undef EXTERNC

#endif /* SMGR_S3_H */