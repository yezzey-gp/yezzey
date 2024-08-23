#pragma once

#include <openssl/md5.h>

#ifdef __cplusplus
extern "C" {
#include "postgres.h"
}

#endif

#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

extern const char *baseYezzeyPath;
#ifdef __cplusplus

#include "io.h"
#include "io_adv.h"
#include "types.h"

// create external storage reading handle, able to
// read all files related to given relation

// create external storage write handle,
// to transfer segment file of given AO/AOCS relation,
// with operation modcound $modcount$

int64_t yezzey_virtual_relation_size(std::shared_ptr<IOadv> adv, int32_t segid);

int64_t yezzey_calc_virtual_relation_size(std::shared_ptr<IOadv> adv,
                                          ssize_t segindx, ssize_t modcount,
                                          const std::string &storage_path);

std::string storage_url_add_options(const std::string &s3path,
                                    const char *config_path);

std::string getYezzeyExtrenalStorageBucket(const char *host,
                                           const char *bucket);

std::string make_yezzey_url(const std::string &prefix, int64_t modcounts,
                            XLogRecPtr current_recptr);

std::vector<int64_t> parseModcounts(const std::string &prefix,
                                    std::string name);
#endif

EXTERNC void getYezzeyExternalStoragePath(const char *nspname,
                                          const char *relationName,
                                          const char *host, const char *bukcet,
                                          const char *prefix,
                                          const char *filename, int32_t segid,
                                          char **dest);

EXTERNC void getYezzeyExternalStoragePathByCoords(
    const char *nspname, const char *relname, const char *host,
    const char *bucket, const char *storage_prefix, Oid spcNode, Oid dbNode, Oid relNode,
    int32_t segblockno /* segment no*/, int32_t segid, char **dest);

EXTERNC XLogRecPtr yezzeyGetXStorageInsertLsn(void);

#undef EXTERNC