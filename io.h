#ifndef YEZZEY_IO_H
#define YEZZEY_IO_H

#include <thread>

#include "blocking_buf.h"

#include "gpgme.h"

#include "gpreader.h"
#include "gpwriter.h"

struct yezzey_io_handler {

// 
    BlockingBuffer buf;

// private fields

//  GPG - related
    /* "/usr/bin/gpg" or something */
    const char * engine_path; 
    const char * gpg_key_id;

//  storage + WAL-G related

	const char * config_path;
     // schema name for relation
    const char * nspname;
    // relation name itself
	const char * relname;
    // s3 host
	const char * host;
    // s3 bucked
	const char * bucket;
    // wal-g specific prefix
	const char * external_storage_prefix;
    // base/5/12345.1
	const char * fileName;

    bool use_gpg_crypto;

//
//  GPG - related structs
    gpgme_data_t crypto_in;
    gpgme_data_t crypto_out;
    
    gpgme_ctx_t crypto_ctx;
    gpgme_key_t keys[2];

//  S3 + WAL-G - related structs

    GPReader * read_ptr;
    GPWriter * write_ptr;
    // handler thread
    std::thread wt;
};


yezzey_io_handler * yezzey_io_handler_allocate(
    const char * engine_path,
    const char * gpg_key_id,
    bool         use_gpg_crypto,
    const char * config_path,
    const char * nspname,
	const char * relname,
	const char * host,
	const char * bucket,
	const char * external_storage_prefix,
	const char * fileName
);

int yezzey_io_free(yezzey_io_handler * handle);

bool yezzey_io_read(yezzey_io_handler * handle, char *buffer, size_t *amount);

bool yezzey_io_write(yezzey_io_handler * handle, char *buffer, size_t *amount);

bool yezzey_io_close(yezzey_io_handler * handle);

void yezzey_io_read_prepare(yezzey_io_handler * handle);
void yezzey_io_write_prepare(yezzey_io_handler * handle);

#endif /* YEZZEY_IO_H */