
#include "gpreader.h"
#include "gpwriter.h"
#include <string>
#include "smgr_s3.h"

void * createReaderHandle(int32_t segid, const char * fileName) {
    std::string url = "s3://storage.yandexcloud.net/loh228/tmp_new/";
    url += "segment_" + std::to_string(segid);
    url += "/";
    url += fileName;
    url += "."; /* modcount follows */
    url += " config=/home/reshke/s3test.conf region=us-east-1";

    return reader_init(url.c_str());
}

void * createWriterHandle(int32_t segid, int64_t modcount, const char * fileName) {
    std::string url = "s3://storage.yandexcloud.net/loh228/tmp_new/";
    url += "segment_" + std::to_string(segid);
    url += "/";
    url += fileName;
    url += "." + std::to_string(modcount);
    url += "_";
    url += " config=/home/reshke/s3test.conf region=us-east-1";

    return writer_init(url.c_str());
}

bool yezzey_reader_transfer_data(void * handle, char *buffer, int *amount) {
    int inner_amount = *amount;
    auto res = reader_transfer_data((GPReader*) handle, buffer, inner_amount);
    *amount = inner_amount;
    return res;
}

int64_t yezzey_virtual_relation_size(int32_t segid, const char * fileName) {
    std::string url = "s3://storage.yandexcloud.net/loh228/tmp_new/";
    url += "segment_" + std::to_string(segid);
    url += "/";
    url += fileName;
    url += "."; /* modcount follows */
    url += " config=/home/reshke/s3test.conf region=us-east-1";

    GPReader * reader = reader_init(url.c_str());
    
    int64_t sz = 0;
    for (auto key : reader->getKeyList().contents) {
        sz += reader->bucketReader.constructReaderParams(key).getKeySize();
    }

    reader_cleanup(&reader);

    return sz;
}

bool yezzey_reader_empty(void * handle) {
    return reader_empty((GPReader*) handle);
}

bool yezzey_writer_transfer_data(void * handle, char *buffer, int *amount) {
    int inner_amount = *amount;
    auto res =  writer_transfer_data((GPWriter*) handle, buffer, inner_amount);
    *amount = inner_amount;
    return res; 
}

/*XXX: fix cleanup*/

bool yezzey_complete_r_transfer_data(void ** handle) {
    if (handle == NULL) return true;
    if (*handle == NULL) return true;
    auto res = reader_cleanup((GPReader**) handle);
    *handle = NULL;
    return res;
}

bool yezzey_complete_w_transfer_data(void ** handle) {
    if (handle == NULL) return true;
    if (*handle == NULL) return true;
    auto res = writer_cleanup((GPWriter**) handle);
    *handle = NULL;
    return res;
}