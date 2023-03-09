
#include "io.h"


#include "gpreader.h"
#include "gpwriter.h"

#include "io_adv.h"

#include "external_storage_smgr.h"

#include "crypto.h"



#ifdef __cplusplus
extern "C"
{
#endif

#include "postgres.h"
#include "fmgr.h"

#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>

#if PG_VERSION_NUM >= 130000
#include "postmaster/interrupt.h"
#endif


#include "utils/elog.h"

#ifdef GPBUILD
#include "cdb/cdbvars.h"
#endif

#ifdef __cplusplus
}
#endif


yezzey_io_handler::yezzey_io_handler() {
    wt = nullptr;
    buf = BlockingBuffer(1<<12);
}

yezzey_io_handler::yezzey_io_handler(
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
) {
    this->engine_path = engine_path;
    this->gpg_key_id = gpg_key_id;
    this->config_path = config_path;
    this->nspname = nspname;
    this->relname = relname;    
    this->host = host;
    this->bucket = bucket;
    this->external_storage_prefix = external_storage_prefix;
    this->fileName = fileName;

    this->buf = BlockingBuffer(1 << 12);

    this->use_gpg_crypto = use_gpg_crypto;
    this->read_initialized_ = this->write_initialized_ = false;
}

yezzey_io_handler::yezzey_io_handler(const yezzey_io_handler& other)  {
    engine_path = other.engine_path;
    gpg_key_id = other.gpg_key_id;
    config_path = other.config_path;
    nspname = other.nspname;    
    relname = other.relname;    
    host = other.host;
    bucket = other.bucket;
    external_storage_prefix = other.external_storage_prefix;
    fileName = other.fileName;

    use_gpg_crypto = other.use_gpg_crypto;
    read_initialized_ = write_initialized_ = false;

    buf = other.buf;
}

yezzey_io_handler& yezzey_io_handler::operator=(const yezzey_io_handler &other) {
    engine_path = other.engine_path;
    gpg_key_id = other.gpg_key_id;
    config_path = other.config_path;
    nspname = other.nspname;    
    relname = other.relname;    
    host = other.host;
    bucket = other.bucket;
    external_storage_prefix = other.external_storage_prefix;
    fileName = other.fileName;

    use_gpg_crypto = other.use_gpg_crypto;
    read_initialized_ = write_initialized_ = false;
    buf = other.buf;
    return *this;
}

void yezzey_io_handler::waitio() {
    if (wt && wt->joinable()) {
        wt->join();
    }
    wt = nullptr;
}

yezzey_io_handler::~yezzey_io_handler() {
    waitio();    

    if (crypto_initialized_) {
        gpgme_data_release(crypto_in);
        gpgme_data_release(crypto_out);
        gpgme_release(crypto_ctx);
    }
}

bool yezzey_io_read(yezzey_io_handler &handle, char *buffer, size_t *amount) {
    if (handle.use_gpg_crypto) {
        // 
        if (!handle.read_initialized_) {
            if (yezzey_io_read_prepare(handle)) {
                elog(ERROR, "failed to prepare read");
                return false;
            }
            handle.read_initialized_ = true;
        }

        auto ret = handle.buf.read((char *)buffer, *amount);
        *amount = ret;
        return ret > 0;
    }

    int inner_amount = *amount;
    auto res = reader_transfer_data((GPReader*) handle.read_ptr, buffer, inner_amount);
    *amount = inner_amount;
    return res;
}


bool yezzey_io_write(yezzey_io_handler &handle, char *buffer, size_t *amount) {
    if (handle.use_gpg_crypto) {
        // 
        if (!handle.write_initialized_) {
            if (yezzey_io_write_prepare(handle)) {
                elog(ERROR, "failed to prepare write");
                return false;
            }
            handle.write_initialized_ = true;
        }
        auto ret = handle.buf.write((const char *)buffer, *amount);
        *amount = ret;
        return ret > 0;
    }

    int inner_amount = *amount;
    auto res =  writer_transfer_data((GPWriter*) handle.write_ptr, buffer, inner_amount);
    *amount = inner_amount;
    return res; 
}

bool yezzey_io_close(yezzey_io_handler &handle) {

	handle.buf.close();
	handle.waitio();

    auto read_res = yezzey_complete_r_transfer_data(handle);
    auto write_res = yezzey_complete_w_transfer_data(handle);
    return read_res & write_res;
}


int yezzey_io_read_prepare(yezzey_io_handler &handle) {
    auto rc = yezzey_io_prepare_crypt(handle, true);
    if (rc) {
        return rc;
    }
    yezzey_io_dispatch_decrypt(handle);
    return 0;
}

int yezzey_io_write_prepare(yezzey_io_handler &handle) {
    auto rc = yezzey_io_prepare_crypt(handle, false);
    if (rc) {
        return rc;
    }
    yezzey_io_dispatch_encrypt(handle);
    return 0;
}
