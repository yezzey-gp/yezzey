

#include "crypto.h"

#include "external_storage_smgr.h"


// decrypt operation

// first arg should be (expected to be) yezzey_io_handler * 
ssize_t
yezzey_crypto_stream_read(void *handler, void *buffer, size_t size) {
    auto y_handler = (yezzey_io_handler *) handler;

    size_t inner_amount = size;
    auto res = yezzey_reader_transfer_data(y_handler, (char *) buffer, &inner_amount);

    size_t tot_offset_ = 0;
    for (;tot_offset_ < inner_amount;) {
        auto buf_res = y_handler->buf.write((const char *)(buffer + tot_offset_), inner_amount - tot_offset_);
        if (buf_res <= 0) {
            return buf_res;
        }
    }

    return inner_amount;
}


// first arg should be (expected to be) yezzey_io_handler * 
ssize_t
yezzey_crypto_stream_write(void *handler, const void *buffer, size_t size) {
    auto y_handler = (yezzey_io_handler *) handler;

    size_t inner_amount = size;
    auto res = yezzey_writer_transfer_data(y_handler, (const char *) buffer, &inner_amount);


    return inner_amount;
}

void
yezzey_crypto_stream_free(void *handler)
{
    auto ioh = (yezzey_io_handler *)handler;
	/* no-op */
    // handler
}



int
yezzey_io_prepare_crypt(yezzey_io_handler * ptr) {
    gpgme_error_t err;
    gpgme_genkey_result_t gen_result;
    gpgme_encrypt_result_t enc_result;
    gpgme_decrypt_result_t dec_result;
    gpgme_encrypt_result_t en_result;

    // char *agent_info;
    // agent_info = getenv("GPG_AGENT_INFO");

    // init_gpgme (GPGME_PROTOCOL_OpenPGP);

    err = gpgme_new (&ptr->crypto_ctx);
    if (err) {
        return -1;
    }

    // ste ACSII armor to true
    gpgme_set_armor (ptr->crypto_ctx, 1);
    
//   fail_if_err (err);
    /* Might want to comment gpgme_ctx_set_engine_info() below. */
    err = gpgme_ctx_set_engine_info(ptr->crypto_ctx, GPGME_PROTOCOL_OpenPGP, ptr->engine_path, NULL);
    if (err) {
        return -1;
    }
    /* Search key for encryption. */
    gpgme_key_t key;
    err = gpgme_get_key (ptr->crypto_ctx,  ptr->gpg_key_id, &key, 1);
    if (err) {
        return -1;
    }
    /* Initialize output buffer. */
    err = gpgme_data_new (&ptr->crypto_out);
    if (err) {
        return -1;
    }

    // fail_if_err (err);

    /* Initialize input buffer. */
    err = gpgme_data_new_from_cbs(&ptr->crypto_in, &yezzey_crypto_cbs, ptr);
    // fail_if_err (err);

    err = gpgme_data_new_from_cbs(&ptr->crypto_out, &yezzey_crypto_cbs, ptr);

    /* Encrypt data. */
    ptr->keys[0] = key;

    /* Decrypt data. */

    fprintf (stderr, "start\n");
    return 0;
}


void
yezzey_io_dispatch_encrypt(yezzey_io_handler * ptr) {
    ptr->wt = std::move(std::thread([&](){
        gpgme_error_t err;
        err = gpgme_op_encrypt(ptr->crypto_ctx, ptr->keys, GPGME_ENCRYPT_ALWAYS_TRUST, ptr->crypto_in, ptr->crypto_out);
        if (err) {
            // bad
        }
        // fail_if_err (err);
    }));
}

void
yezzey_io_dispatch_decrypt(yezzey_io_handler * ptr) {
    ptr->wt = std::move(std::thread([&](){
        gpgme_error_t err;
        err = gpgme_op_decrypt(ptr->crypto_ctx, ptr->crypto_in, ptr->crypto_out);
        if (err) {
            // bad
        }
        // fail_if_err (err);
    }));
}

