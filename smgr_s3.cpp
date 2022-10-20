
#include "gpreader.h"
#include "gpwriter.h"
#include <string>
#include "smgr_s3.h"

void * createReaderHandle(const char * prefix, const char * fileName) {
    std::string url = "s3://storage.yandexcloud.net/loh228/tmp/";
    url += prefix;
    url += "/";
    url += fileName;
    url += " config=/home/reshke/s3test.conf region=us-east-1";

    return reader_init(url.c_str());
}

void * createWriterHandle(const char * prefix, const char * fileName) {
    std::string url = "s3://storage.yandexcloud.net/loh228/tmp/";
    url += prefix;
    url += "/";
    url += fileName;
    url += " config=/home/reshke/s3test.conf region=us-east-1";

    return writer_init(url.c_str());
}

bool yezzey_reader_transfer_data(void * handle, char *buffer, int *amount) {
    int inner_amount = *amount;
    auto res = reader_transfer_data((GPReader*) handle, buffer, inner_amount);
    *amount = inner_amount;
    return res;
}

bool yezzey_writer_transfer_data(void * handle, char *buffer, int *amount) {
    int inner_amount = *amount;
    auto res =  writer_transfer_data((GPWriter*) handle, buffer, inner_amount);
    *amount = inner_amount;
    return res; 
}

bool yezzey_complete_r_transfer_data(void ** handle) {
    if (handle ==NULL) return true;
    if (*handle == NULL) return true;
    auto res = reader_cleanup((GPReader**) handle);
    *handle = NULL;
    return res;
}

bool yezzey_complete_w_transfer_data(void ** handle) {
    if (handle ==NULL) return true;
    if (*handle == NULL) return true;
    auto res = writer_cleanup((GPWriter**) handle);
    *handle = NULL;
    return res;
}