
#include "io.h"


#include "gpreader.h"
#include "gpwriter.h"

#include "io_adv.h"

#include "external_storage_smgr.h"


yezzey_io_handler * yezzey_io_handler_allocate(
    const char * engine_path,
    const char * gpg_key_id,
    const char * config_path,
    const char * nspname,
	const char * relname,
	const char * host,
	const char * bucket,
	const char * external_storage_prefix,
	const char * fileName
) {
    auto ptr = (yezzey_io_handler *) malloc(sizeof(yezzey_io_handler));
    memset(ptr, 0, sizeof(yezzey_io_handler));

    ptr->engine_path = strdup(engine_path);
    ptr->gpg_key_id = strdup(gpg_key_id);
    ptr->config_path = strdup(config_path);
    ptr->nspname = strdup(nspname);    
    ptr->relname = strdup(relname);    
    ptr->host = strdup(host);
    ptr->bucket = strdup(bucket);
    ptr->external_storage_prefix = strdup(external_storage_prefix);
    ptr->fileName = strdup(fileName);

    return ptr;
}

int
yezzey_io_free(yezzey_io_handler * ptr) {
    if (ptr->wt.joinable()) {
        ptr->wt.join();
    }
    gpgme_data_release(ptr->crypto_in);
    gpgme_data_release(ptr->crypto_out);
    gpgme_release(ptr->crypto_ctx);

    free((void*)ptr->engine_path);
    free((void*)ptr->gpg_key_id);

    free((void*)ptr->config_path);
    free((void*)ptr->nspname);
    free((void*)ptr->relname);
    free((void*)ptr->host);
    free((void*)ptr->bucket);
    free((void*)ptr->external_storage_prefix);
    free((void*)ptr->fileName);
    return 0;
}


bool yezzey_io_read(yezzey_io_handler * handle, char *buffer, size_t *amount) {
    int inner_amount = *amount;
    auto res = reader_transfer_data((GPReader*) handle->read_ptr, buffer, inner_amount);
    *amount = inner_amount;
    return res;
}


bool yezzey_io_write(yezzey_io_handler * handle, char *buffer, size_t *amount) {
    int inner_amount = *amount;
    auto res =  writer_transfer_data((GPWriter*) handle->write_ptr, buffer, inner_amount);
    *amount = inner_amount;
    return res; 
}

bool yezzey_io_close(yezzey_io_handler * handle) {
    auto read_res = yezzey_complete_r_transfer_data(handle);
    auto write_res = yezzey_complete_w_transfer_data(handle);
    return read_res & write_res;
}
