
#include "io_adv.h"
#include "util.h"
#include "ylister.h"

std::string yezzey_fqrelname_md5(const std::string &nspname,
                                 const std::string &relname);

/* prefix in external storage, block file path */
/* Craft path in format
 *  handler->external_storage_prefix + seg_{segid} + ...
 * example:           (prefix in wal-g compatable form) / tablespace oid /
 * reloid / md5(schema + relname) / relfilenode / modcount
 * segments_005/seg0/basebackups_005/aosegments/1663_98304_527e1c67fae2e4f3e5caf632d5473cf5_73728_1_1_DY_<lsn>
 *
 */
std::string yezzey_block_file_path(const std::string &nspname,
                                   const std::string &relname,
                                   relnodeCoord coords, int32_t segid);

std::string craftStoragePrefixedPath(const std::shared_ptr<IOadv> &adv,
                                     ssize_t segindx, ssize_t modcount,
                                     const std::string &prefix,
                                     XLogRecPtr current_recptr);

/* un-prefixed version of `craftStoragePrefixedPath` */
std::string craftStorageUnPrefixedPath(const std::shared_ptr<IOadv> &adv,
                                       ssize_t segindx, ssize_t modcount,
                                       XLogRecPtr current_recptr);

std::string
getYezzeyRelationUrl_internal(const std::string &nspname,
                              const std::string &relname,
                              const std::string &external_storage_prefix,
                              relnodeCoord coords, int32_t segid);
