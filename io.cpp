
#include "io.h"


#include "gpreader.h"
#include "gpwriter.h"

#include "io_adv.h"

#include "external_storage_smgr.h"

#include "crypto.h"

yezzey_io_handler::yezzey_io_handler() {

}

yezzey_io_handler::yezzey_io_handler(
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
) : buf(BlockingBuffer()) {
    this->engine_path = std::string(engine_path);
    this->gpg_key_id = std::string(gpg_key_id);
    this->config_path = std::string(config_path);
    this->nspname = std::string(nspname);    
    this->relname = std::string(relname);    
    this->host = std::string(host);
    this->bucket = std::string(bucket);
    this->external_storage_prefix = std::string(external_storage_prefix);
    this->fileName = std::string(fileName);

    this->use_gpg_crypto = use_gpg_crypto;
    this->read_initialized_ = this->write_initialized_ = false;
}

yezzey_io_handler::yezzey_io_handler(const yezzey_io_handler& other) : buf(other.buf) {
    this->engine_path = other.engine_path;
    this->gpg_key_id = other.gpg_key_id;
    this->config_path = other.config_path;
    this->nspname = other.nspname;    
    this->relname = other.relname;    
    this->host = other.host;
    this->bucket = other.bucket;
    this->external_storage_prefix = other.external_storage_prefix;
    this->fileName = other.fileName;

    this->use_gpg_crypto = other.use_gpg_crypto;
    this->read_initialized_ = this->write_initialized_ = false;
}

yezzey_io_handler& yezzey_io_handler::operator=(const yezzey_io_handler &other) {
    this->engine_path = other.engine_path;
    this->gpg_key_id = other.gpg_key_id;
    this->config_path = other.config_path;
    this->nspname = other.nspname;    
    this->relname = other.relname;    
    this->host = other.host;
    this->bucket = other.bucket;
    this->external_storage_prefix = other.external_storage_prefix;
    this->fileName = other.fileName;

    this->use_gpg_crypto = other.use_gpg_crypto;
    this->read_initialized_ = this->write_initialized_ = false;
    return *this;
}

yezzey_io_handler::~yezzey_io_handler() {
    if (wt->joinable()) {
        wt->join();
    }
    gpgme_data_release(crypto_in);
    gpgme_data_release(crypto_out);
    gpgme_release(crypto_ctx);
}

bool yezzey_io_read(yezzey_io_handler &handle, char *buffer, size_t *amount) {
    if (handle.use_gpg_crypto) {
        // 
        if (!handle.read_initialized_) {
            if (yezzey_io_read_prepare(handle)) {
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
