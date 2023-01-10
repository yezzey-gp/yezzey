
#ifndef SMGR_S3_H
#define SMGR_S3_H

//#include "storage/smgr.h"

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

EXTERNC void * createReaderHandle(char * relname, char * bucket, char * external_storage_prefix, const char * fileName, int32_t segid);
EXTERNC void * createWriterHandle(char * rhandle_ptr, char * relname, char * bucket, char * external_storage_prefix, char * fileName, int32_t segid, int64_t modcount);
EXTERNC bool yezzey_reader_transfer_data(void * handle, char *buffer, int *amount);
EXTERNC bool yezzey_writer_transfer_data(void * handle, char *buffer, int *amount);

EXTERNC bool yezzey_reader_empty(void *handle);
EXTERNC int64_t yezzey_virtual_relation_size(int32_t segid, const char * fileName);

EXTERNC bool yezzey_complete_r_transfer_data(void ** handle);
EXTERNC bool yezzey_complete_w_transfer_data(void ** handle);

#undef EXTERNC

#endif /* SMGR_S3_H */