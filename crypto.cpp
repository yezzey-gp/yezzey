

#include "crypto.h"
#include <iostream>
#include "external_storage_smgr.h"
#include <memory>

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


// decrypt operation

// first arg should be (expected to be) yezzey_io_handler * 
ssize_t
yezzey_crypto_stream_dec_read(void *handler, void *buffer, size_t size) {
    auto y_handler = (yezzey_io_handler *) handler;
    int inner_amount = size;
    if (!reader_transfer_data((GPReader*) y_handler->read_ptr, (char*)buffer, inner_amount)) {
        /* failed read, probably eof */
        // y_handler->buf.close();
        return -1;
    }

    if (inner_amount <= 0) {
        /* failed read, probably eof */
        // y_handler->buf.close();
    }


    return inner_amount;
}

// first arg should be (expected to be) yezzey_io_handler * 
ssize_t
yezzey_crypto_stream_dec_write(void *handler, const void *buffer, size_t size) {
    auto y_handler = (yezzey_io_handler *) handler;
    auto ret = y_handler->buf.write((const char *)buffer, size);
    return ret;
}

// first arg should be (expected to be) yezzey_io_handler * 
ssize_t
yezzey_crypto_stream_enc_read(void *handler, void *buffer, size_t size) {
    auto y_handler = (yezzey_io_handler *) handler;
    auto ret = y_handler->buf.read((char *)buffer, size);
    return ret;
}


// first arg should be (expected to be) yezzey_io_handler * 
ssize_t
yezzey_crypto_stream_enc_write(void *handler, const void *buffer, size_t size) {
    auto y_handler = (yezzey_io_handler *) handler;

    int inner_amount = size;
    (void)writer_transfer_data((GPWriter*) y_handler->write_ptr, (char*)buffer, inner_amount);
    return inner_amount; 
}


void
yezzey_crypto_stream_free(void *handler)
{
    auto ioh = (yezzey_io_handler *)handler;
	/* no-op */
    // handler
}


void
init_gpgme (gpgme_protocol_t proto)
{
    gpgme_error_t err;

    gpgme_check_version (NULL);
    setlocale (LC_ALL, "");
    gpgme_set_locale (NULL, LC_CTYPE, setlocale (LC_CTYPE, NULL));

    err = gpgme_engine_check_version (proto);
}


int
yezzey_io_prepare_crypt(yezzey_io_handler &ptr, bool dec) {
    gpgme_error_t err;
    char *agent_info;
    agent_info = getenv("GPG_AGENT_INFO");

    init_gpgme (GPGME_PROTOCOL_OpenPGP);

    err = gpgme_new (&ptr.crypto_ctx);
    if (err) {
        auto errstr = gpgme_strerror(err);
        std::cerr << errstr << "\n";
        return -1;
    }

    // ste ACSII armor to true
    gpgme_set_armor (ptr.crypto_ctx, 1);
    
//   fail_if_err (err);
    /* Might want to comment gpgme_ctx_set_engine_info() below. */
    err = gpgme_ctx_set_engine_info(ptr.crypto_ctx, GPGME_PROTOCOL_OpenPGP, ptr.engine_path.c_str(), NULL);
    if (err) {
        auto errstr = gpgme_strerror(err);
        std::cerr << errstr << "\n";
        return -1;
    }
    /* Search key for encryption. */
    err = gpgme_get_key (ptr.crypto_ctx,  ptr.gpg_key_id.c_str(), &ptr.key, 1);
    if (err) {
        auto errstr = gpgme_strerror(err);
        std::cerr << errstr << "\n";
        return -1;
    }
    /* Initialize output buffer. */
    err = gpgme_data_new (&ptr.crypto_out);
    if (err) {
        auto errstr = gpgme_strerror(err);
        std::cerr << errstr << "\n";
        return -1;
    }

    ptr.crypto_initialized_ = true;

    // fail_if_err (err);

    if (dec) {
        /* Initialize input buffer. */
        err = gpgme_data_new_from_cbs(&ptr.crypto_in, &yezzey_crypto_dec_cbs, &ptr);
        if (err) {
            return -1;
        }
        // fail_if_err (err);

        err = gpgme_data_new_from_cbs(&ptr.crypto_out, &yezzey_crypto_dec_cbs, &ptr);
        if (err) {
            return -1;
        }
    } else {
        /* Initialize input buffer. */
        err = gpgme_data_new_from_cbs(&ptr.crypto_in, &yezzey_crypto_enc_cbs, &ptr);
        if (err) {
            return -1;
        }
        // fail_if_err (err);

        err = gpgme_data_new_from_cbs(&ptr.crypto_out, &yezzey_crypto_enc_cbs, &ptr);
        if (err) {
            return -1;
        }
    }

    /* Encrypt data. */
    ptr.keys[0] = ptr.key;

    /* Decrypt data. */

    //fprintf (stderr, "start\n");
    return 0;
}


void
yezzey_io_dispatch_encrypt(yezzey_io_handler &ptr) {
    ptr.wt = std::make_unique<std::thread>([&](){
        gpgme_error_t err;
        err = gpgme_op_encrypt(ptr.crypto_ctx, (gpgme_key_t*)ptr.keys, GPGME_ENCRYPT_ALWAYS_TRUST, ptr.crypto_in, ptr.crypto_out);
        if (err) {
            auto errstr = gpgme_strerror(err);
            std::cerr << "failed to dipatch encrypt " << errstr<< '\n';
            return;
            // bad
        }
        // fail_if_err (err);
    });
}

void
yezzey_io_dispatch_decrypt(yezzey_io_handler &ptr) {
    ptr.wt = std::make_unique<std::thread>([&](){
        gpgme_error_t err;
        err = gpgme_op_decrypt(ptr.crypto_ctx, ptr.crypto_in, ptr.crypto_out);

        ptr.buf.close();
        if (err) {            
            auto errstr = gpgme_strerror(err);
            std::cerr << "failed to dipatch decrypt " << errstr << '\n';
            return;
            // bad
        }
        // fail_if_err (err);
    });
}

