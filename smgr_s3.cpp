
/**
 * @file smgr_s3.cpp
 * 
 * @author your name (you@domain.com)
 * @brief 
 * @version 0.1
 * @date 2023-02-20
 * 
 * @copyright Copyright (c) 2023
 * 
 */


#include "gpreader.h"
#include "gpwriter.h"
#include <string>
#include <vector>
#include <map>
#include "smgr_s3.h"
#include <openssl/md5.h>

#define DEFAULTTABLESPACE_OID 1663 /* FIXME */


std::string getYezzeyExtrenalStorageBucket(const char * host, const char * bucket) {
    std::string url = "s3://";
    url += host;
    url += "/";
    url += bucket;
    url += "/";

    return url;
}

std::string yezzey_url_add_options(const std::string &s3path, const char * config_path) {
    auto ret = s3path;

    ret +=  " config=";
    ret += config_path;
    ret += " region=us-east-1";

    return ret;
}

std::string getYezzeyRelationUrl(
    const char * nspname,
    const char * relname,
    const char * external_storage_prefix,
    const char * fileName,
    int32_t segid) {

    std::string url = "";
    url += external_storage_prefix;
    url += "/seg" + std::to_string(segid) + "/basebackups_005/aosegments/";
    size_t len = strlen(fileName);

    int64_t dboid = 0, tableoid = 0;
    int64_t relation_segment = 0;

    for (size_t it = 0; it < len;) {
        if (!isdigit(fileName[it])) {
            ++it;
            continue;
        }
        if (dboid && tableoid && relation_segment) {
            break; //seg num follows
        }
        if (dboid == 0) {
            while (it < len && isdigit(fileName[it])) {
                dboid *= 10;
                dboid += fileName[it++] - '0';
            }
        } else if (tableoid == 0) {
            while (it < len && isdigit(fileName[it])) {
                tableoid *= 10;
                tableoid += fileName[it++] - '0';
            }
        } else if (relation_segment == 0) { 
            while (it < len && isdigit(fileName[it])) {
                relation_segment *= 10;
                relation_segment += fileName[it++] - '0';
            }
        }
    }

    unsigned char md[MD5_DIGEST_LENGTH];


    url += std::to_string(DEFAULTTABLESPACE_OID) + "_" + std::to_string(dboid) + "_";

    std::string full_name = "";
    full_name += nspname;
    full_name += ".";
    full_name += relname;
    /* compute AO/AOCS relation name, just like walg does*/
    (void)MD5((const unsigned char *)full_name.c_str(), full_name.size(), md);

    for (size_t i = 0; i < MD5_DIGEST_LENGTH; ++i) {
        char chunk[3];
        (void)sprintf(chunk,"%.2x", md[i]);
        url += chunk[0];
        url += chunk[1];
    }

    url += "_" + std::to_string(tableoid) + "_" + std::to_string(relation_segment) + "_";

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
    /* mc1_D_mc2_D_mc3_D_mc4 */
    for (size_t it = indx; it <= endindx; ++ it) {
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

void
getYezzeyExternalStoragePath(
    const char * nspname,
    const char * relname,
    const char * host,
    const char * bucket, 
    const char * external_storage_prefix, 
    const char * fileName, 
    int32_t segid,
    char ** dest
) {
    auto prefix = getYezzeyRelationUrl(nspname, relname, external_storage_prefix, fileName, segid);
    auto path = getYezzeyExtrenalStorageBucket(host, bucket) + prefix;

    *dest = (char*)malloc(sizeof(char) * path.size());
    strcpy(*dest, path.c_str());
    return;
}

void * createReaderHandle(
    const char * config_path,
    const char * nspname,
    const char * relname,
    const char * host,
    const char * bucket, 
    const char * external_storage_prefix, 
    const char * fileName, 
    int32_t segid) {

    auto prefix = getYezzeyRelationUrl(nspname, relname, external_storage_prefix, fileName, segid);

    // add config path FIXME
    auto reader = reader_init(
        yezzey_url_add_options((getYezzeyExtrenalStorageBucket(host, bucket) + prefix), config_path).c_str());

    if (reader == NULL) {
        /* error while external storage initialization */
        return NULL;
    }

    auto content = reader->getKeyList().contents;
    std::map<std::string, std::vector<int64_t>> cache;

    std::sort(content.begin(), content.end(), [&cache, &prefix](const BucketContent & c1, const BucketContent & c2){
        auto v1 = cache[c1.getName()] = cache.count(c1.getName()) ? cache[c1.getName()] : parseModcounts(prefix, c1.getName());
        auto v2 = cache[c2.getName()] = cache.count(c2.getName()) ? cache[c2.getName()] : parseModcounts(prefix, c2.getName());
        return v1 < v2;
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
    const char * config_path,
    const char * rhandle_ptr,
    const char * nspname,
    const char * relname,
    const char * host,
    const char * bucket, 
    const char * external_storage_prefix, 
    const char * fileName, int32_t segid, int64_t modcount) {
    if (rhandle_ptr == NULL) {
        return NULL;
    }

    auto prefix = getYezzeyRelationUrl(nspname, relname, external_storage_prefix, fileName, segid);

    GPReader * reader = (GPReader *) rhandle_ptr;

    auto content = reader->getKeyList().contents;
    if (content.size() == 0) {
        auto url = make_yezzey_url(getYezzeyExtrenalStorageBucket(host, bucket) + prefix, {modcount, modcount, modcount, modcount});
        url = yezzey_url_add_options(url, config_path);
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
        getYezzeyExtrenalStorageBucket(host, bucket) + prefix, largest);

    // config path
    url = yezzey_url_add_options(url, config_path);

    return writer_init(url.c_str());
}


void * createWriterHandleToPath(
    const char * config_path,
    const char * host,
    const char * bucket,
    const char * external_storage_prefix,
	const char * path,
	int32_t segid, 
	int64_t modcount) {
    std::string url = getYezzeyExtrenalStorageBucket(host, bucket) ;
    url += external_storage_prefix;
    url += "/seg" + std::to_string(segid) + "/basebackups_005/aosegments/";
    url += path;

    // config path
    url = yezzey_url_add_options(url, config_path);

    return writer_init(url.c_str());
}

bool yezzey_reader_transfer_data(void * handle, char *buffer, int *amount) {
    int inner_amount = *amount;
    auto res = reader_transfer_data((GPReader*) handle, buffer, inner_amount);
    *amount = inner_amount;
    return res;
}

int64_t yezzey_virtual_relation_size(
    const char * config_path,
    const char * nspname,
    const char * relname, 
    const char * host,
    const char * bucket, 
    const char * external_storage_prefix, 
    const char * fileName, 
    int32_t segid) {

	GPReader * rhandle = (GPReader * )createReaderHandle(
        config_path,
        nspname,
        relname, 
        host /*host*/,
		bucket/*bucket*/, 
        external_storage_prefix /*prefix*/,
        fileName,
        segid);

    if (!rhandle) {
        return -1;
    }
    
    int64_t sz = 0;
    auto content = rhandle->getKeyList().contents;
    for (auto key : content) {
        sz += rhandle->bucketReader.constructReaderParams(key).getKeySize();
    }

    reader_cleanup(&rhandle);
    return sz;
}

int64_t yezzey_calc_virtual_relation_size(void * rhandle_ptr) {
	GPReader * rhandle = (GPReader * )rhandle_ptr;
    
    int64_t sz = 0;
    auto content = rhandle->getKeyList().contents;
    for (auto key : content) {
        sz += rhandle->bucketReader.constructReaderParams(key).getKeySize();
    }
    return sz;
}


void * yezzey_list_relation_chunks(
    const char * config_path,
    const char * nspname,
    const char * relname, 
    const char * host,
    const char * bucket, 
    const char * external_storage_prefix,
    const char * fileName,
    int32_t segid, 
    size_t * cnt_chunks) 
{
	GPReader * rhandle = (GPReader * )createReaderHandle(
        config_path,
        nspname,
        relname, 
        host /*host*/,
		bucket/*bucket*/, 
        external_storage_prefix /*prefix*/, 
        fileName, 
        segid);

    auto content = rhandle->getKeyList().contents;
    *cnt_chunks = content.size();
    return rhandle;
}

int64_t yezzey_copy_relation_chunks(
    void *rhandle_ptr, 
    externalChunkMeta * chunks){
	GPReader * rhandle = (GPReader * )rhandle_ptr;
    
    auto content = rhandle->getKeyList().contents;
    for (size_t i = 0; i < content.size(); ++ i) {
        auto key = content[i];
        chunks[i].chunkName = strdup(key.getName().c_str());
        chunks[i].chunkSize = key.getSize();
    }
    return 0;
}


int64_t yezzey_list_relation_chunks_cleanup(void *rhandle_ptr) {
    auto reader = (GPReader*) rhandle_ptr;
    reader_cleanup(&reader);
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