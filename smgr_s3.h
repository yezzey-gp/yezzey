
#ifndef SMGR_S3_H
#define SMGR_S3_H

//#include "storage/smgr.h"

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

EXTERNC void * createReaderHandle(const char * prefix, const char * fileName);
EXTERNC void * createWriterHandle(const char * prefix, const char * fileName);
EXTERNC bool yezzey_reader_transfer_data(void * handle, char *buffer, int *amount);
EXTERNC bool yezzey_writer_transfer_data(void * handle, char *buffer, int *amount);

EXTERNC bool yezzey_complete_r_transfer_data(void ** handle);
EXTERNC bool yezzey_complete_w_transfer_data(void ** handle);

#undef EXTERNC

#endif /* SMGR_S3_H */