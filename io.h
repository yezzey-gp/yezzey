#ifndef YEZZEY_IO_H
#define YEZZEY_IO_H

#include <thread>

#include "blocking_buf.h"

#include "gpgme.h"

#include "gpreader.h"
#include "gpwriter.h"

#include <vector>
#include <memory>


struct yezzey_io_handler {

// 
    BlockingBuffer buf;

// private fields

//  GPG - related
    /* "/usr/bin/gpg" or something */
    std::string engine_path; 
    std::string gpg_key_id;

//  storage + WAL-G related

	std::string config_path;
     // schema name for relation
    std::string nspname;
    // relation name itself
	std::string relname;
    // s3 host
	std::string host;
    // s3 bucked
	std::string bucket;
    // wal-g specific prefix
	std::string external_storage_prefix;
    // base/5/12345.1
	std::string fileName;

    bool use_gpg_crypto;

//
//  GPG - related structs
    gpgme_data_t crypto_in;
    gpgme_data_t crypto_out;
    
    gpgme_ctx_t crypto_ctx;
    gpgme_key_t keys[2]{NULL,NULL};
    gpgme_key_t key;

    bool crypto_initialized_{false};

//  S3 + WAL-G - related structs

    GPReader * read_ptr{nullptr};
    bool read_initialized_{false};
    GPWriter * write_ptr{nullptr};
    bool write_initialized_{false};
    // handler thread
    // std::unique_ptr<std::thread> wt;
    std::unique_ptr<std::thread> wt {nullptr};

    void waitio();

    // construcor
    yezzey_io_handler(
        const std::string &engine_path,
        const std::string &gpg_key_id,
        bool         use_gpg_crypto,
        const std::string & config_path,
        const std::string &nspname,
        const std::string &relname,
        const std::string &host,
        const std::string &bucket,
        const std::string &external_storage_prefix,
        const std::string &fileName
    );
    // copyable
    yezzey_io_handler(const yezzey_io_handler& buf);
    yezzey_io_handler& operator=(const yezzey_io_handler&);

    yezzey_io_handler();

    ~yezzey_io_handler();
};


bool yezzey_io_read(yezzey_io_handler &handle, char *buffer, size_t *amount);

bool yezzey_io_write(yezzey_io_handler &handle, char *buffer, size_t *amount);

bool yezzey_io_close(yezzey_io_handler &handle);

int yezzey_io_read_prepare(yezzey_io_handler &handle);
int yezzey_io_write_prepare(yezzey_io_handler &handle);

#endif /* YEZZEY_IO_H */
