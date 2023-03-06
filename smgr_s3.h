
#ifndef YEZZEY_SMGR_S3_H
#define YEZZEY_SMGR_S3_H

//#include "storage/smgr.h"
#include <openssl/md5.h>

struct externalChunkMeta {
	int64_t chunkSize;
	const char * chunkName;
};

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

extern const char * basebackupsPath;


#ifdef __cplusplus

#include "io_adv.h"
#include "io.h"

// create external storage reading handle, able to 
// read all files related to given relation
void * createReaderHandle(
	yezzey_io_handler * handler,
	int32_t segid);

// create external storage write handle, 
// to transfer segment file of given AO/AOCS relation,
// with operation modcound $modcount$
void * createWriterHandle(
	yezzey_io_handler * handler,
	int32_t segid, 
	int64_t modcount);

void * createWriterHandleToPath(
	yezzey_io_handler * handler,
	int32_t segid, 
	int64_t modcount);


bool yezzey_reader_transfer_data(yezzey_io_handler * handle, char *buffer, int *amount);
bool yezzey_writer_transfer_data(yezzey_io_handler * handle, char *buffer, int *amount);

bool yezzey_reader_empty(yezzey_io_handler * handler);

int64_t yezzey_virtual_relation_size(
	yezzey_io_handler * handler,
	int32_t segid);

int64_t yezzey_calc_virtual_relation_size(yezzey_io_handler * handler);

void yezzey_list_relation_chunks(
	yezzey_io_handler * handler,
	int32_t segid,
	size_t * cnt_chunks);

int64_t yezzey_list_relation_chunks_cleanup(yezzey_io_handler * handler);

bool yezzey_complete_r_transfer_data(void ** handle);
bool yezzey_complete_w_transfer_data(void ** handle);

int64_t yezzey_copy_relation_chunks(yezzey_io_handler * handler, struct externalChunkMeta * chunks);

#endif

EXTERNC void getYezzeyExternalStoragePath(
    const char * nspname, 
    const char * relationName,
    const char * host,
    const char * bukcet,
    const char * prefix,
    const char * filename,
    int32_t segid,
	char ** dest
);


#undef EXTERNC

#endif /* YEZZEY_SMGR_S3_H */