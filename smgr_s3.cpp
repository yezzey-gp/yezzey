
#include "gpreader.h"
#include "gpwriter.h"
#include <string>
#include <vector>
#include <map>
#include "smgr_s3.h"
#include <openssl/md5.h>

#define DEFAULTTABLESPACE_OID 1663 /* FIXME */


std::string getYezzeyExtrenalStorageBucket(const char * bucket) {
    std::string url = "s3://storage.yandexcloud.net/";
    url += bucket;
    url += "/";

    return url;
}

std::string getYezzeyRelationUrl(char * relname, const char * external_storage_prefix, const char * fileName, int32_t segid) {
    std::string url = "";
    url += external_storage_prefix;
    url += "/";
    url += "seg" + std::to_string(segid);
    url += "/basebackups_005/aosegments/";
    size_t len = strlen(fileName);

    int64_t dboid = 0, tableoid = 0;

    for (size_t it = 0; it < len;) {
        if (!isdigit(fileName[it])) {
            ++it;
            continue;
        }
        if (dboid && tableoid) {
            break; //seg num follows
        }
        if (dboid) {
            while (it < len && isdigit(fileName[it])) {
                tableoid *= 10;
                tableoid += fileName[it++] - '0';
            }
        } else {
            while (it < len && isdigit(fileName[it])) {
                dboid *= 10;
                dboid += fileName[it++] - '0';
            }
        }
    }

    unsigned char md[MD5_DIGEST_LENGTH];


    url += std::to_string(DEFAULTTABLESPACE_OID) + "_" + std::to_string(dboid) + "_";

    /* compute AO/AOCS relation name, just like walg does*/
    (void)MD5((const unsigned char*)relname, strlen(relname), md);

    for (size_t i = 0; i < MD5_DIGEST_LENGTH; ++i) {
        char chunk[3];
        (void)sprintf(chunk,"%.2x", md[i]);
        url += chunk[0];
        url += chunk[1];
    }

    url += "_" + std::to_string(tableoid) + "_";

    return url;
}

/*
* fileName is in form 'base=DEFAULTTABLESPACE_OID/<dboid>/<tableoid>.<seg>'
*/

std::vector<int64_t> parseModcounts(const std::string &prefix, std::string name){
    std::vector<int64_t> res;
    auto indx = name.find(prefix);
    if (indx == std::string::npos) {
        return res;
    }
    indx += prefix.size();
    auto endindx = name.find("_aoseg", indx);

    size_t prev = 0;

    /* name[endindx] -> not digit */
    for (size_t it = indx + 1; it <= endindx; ++ it) {
        if (!isdigit(name[it])) {
            if (prev) {
                res.push_back(prev);
            }
            prev = 0;
            continue;
        }
        prev *= 10;
        prev += name[it] - '0';
    }

    return res;
}

void * createReaderHandle(char * relname, char * bucket, const char * external_storage_prefix, const char * fileName, int32_t segid) {
    auto prefix = getYezzeyRelationUrl(relname, external_storage_prefix, fileName, segid);

    // add config path FIXME
    auto reader = reader_init(
        (getYezzeyExtrenalStorageBucket(bucket) + prefix +  " config=/home/reshke/s3test.conf region=us-east-1").c_str());

    if (reader == NULL) {
        /* error while external storage initialization */
        return NULL;
    }

    auto content = reader->getKeyList().contents;
    std::map<std::string, std::vector<int64_t>> cache;

    std::sort(content.begin(), content.end(), [&cache, &prefix](const BucketContent & c1, const BucketContent & c2){
        auto v1 = cache[c1.getName()] = cache.count(c1.getName()) ? cache[c1.getName()] : parseModcounts(prefix, c1.getName());
        auto v2 = cache[c2.getName()] = cache.count(c2.getName()) ? cache[c2.getName()] : parseModcounts(prefix, c2.getName());

        return v1 <= v2;
    });

    return reader;
}

std::string make_yezzey_url(const std::string &prefix, const std::vector<int64_t> & modcounts) {
    auto ret = prefix;

    for (size_t i = 0; i < modcounts.size(); ++ i) {
        ret += std::to_string(modcounts[i]);
        if (i + 1 != modcounts.size()) {
            ret += "_D_";
        }
    }

    ret += "_aoseg_yezzey";

    return ret;
}

void * createWriterHandle(
    char * rhandle_ptr,
    char * relname, 
    char * bucket, 
    const char * external_storage_prefix, 
    const char * fileName, int32_t segid, int64_t modcount) {
    if (rhandle_ptr == NULL) {
        return NULL;
    }

    auto prefix = getYezzeyRelationUrl(relname, external_storage_prefix, fileName, segid);

    GPReader * reader = (GPReader *) rhandle_ptr;

    auto content = reader->getKeyList().contents;
    if (content.size() == 0) {
        auto url = make_yezzey_url(getYezzeyExtrenalStorageBucket(bucket) + prefix, {modcount, modcount, modcount, modcount});
        url += " config=/home/reshke/s3test.conf region=us-east-1";
        return writer_init(url.c_str());
    }

    auto largest = parseModcounts(prefix, content.back().getName());
    if (largest.size() <= 3) {
        largest.push_back(modcount);
    } else if (largest.size() == 4) {
        largest.back() = modcount;
    } else {
        // else error
    }

    auto url = make_yezzey_url(
        getYezzeyExtrenalStorageBucket(bucket) + prefix, largest);

    // config path
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
#if 0 /* TODO: fix */

    auto prefix = getYezzeyRelationUrl(relname, bucket, external_storage_prefix, fileName, segid);
    GPReader * reader = reader_init(prefix.c_str());
    
    int64_t sz = 0;
    auto content = reader->getKeyList().contents;
    for (auto key : content) {
        sz += reader->bucketReader.constructReaderParams(key).getKeySize();
    }

    reader_cleanup(&reader);
    return sz;
#endif
    return 0;
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