
#include "url.h"

#include "libpq/md5.h"

/*
 * Create an md5 hash of a text string and return it as hex
 *
 * md5 produces a 16 byte (128 bit) hash; double it for hex
 */

std::string yezzey_fqrelname_md5(const std::string &nspname,
                                 const std::string &relname) {
  unsigned char md[MD5_DIGEST_LENGTH];
  std::string full_name = nspname + "." + relname;
  /* compute AO/AOCS relation name, just like walg does*/
  (void)MD5((const unsigned char *)full_name.c_str(), full_name.size(), md);

  std::string relmd5;

  for (size_t i = 0; i < MD5_DIGEST_LENGTH; ++i) {
    char chunk[3];
    (void)sprintf(chunk, "%.2x", md[i]);
    relmd5 += chunk[0];
    relmd5 += chunk[1];
  }

  return relmd5;
}

/* creates yezzey xternal storage prefix path */
std::string yezzey_block_file_path(const std::string &nspname,
                                   const std::string &relname,
                                   relnodeCoord coords, int32_t segid) {
  std::string url =
      "/segments_005/seg" + std::to_string(segid) + baseYezzeyPath;

  url +=
      std::to_string(coords.spcNode) + "_" + std::to_string(coords.dboid) + "_";

  auto md = yezzey_fqrelname_md5(nspname, relname);
  url += md;
  url += "_" + std::to_string(coords.filenode) + "_" +
         std::to_string(coords.blkno) + "_";

  return url;
}

std::string craftStoragePrefixedPath(const std::shared_ptr<IOadv> &adv,
                                     ssize_t segindx, ssize_t modcount,
                                     const std::string &storage_prefix,
                                     XLogRecPtr current_recptr) {
  auto prefix = getYezzeyRelationUrl_internal(
      adv->nspname, adv->relname, storage_prefix, adv->coords_, segindx);

  return make_yezzey_url(prefix, modcount, current_recptr);
}

/* prefix-independent WAL-G compatable path */
std::string craftStorageUnPrefixedPath(const std::shared_ptr<IOadv> &adv,
                                       ssize_t segindx, ssize_t modcount,
                                       XLogRecPtr current_recptr) {
  auto prefix =
      yezzey_block_file_path(adv->nspname, adv->relname, adv->coords_, segindx);

  return make_yezzey_url(prefix, modcount, current_recptr);
}


/* creates yezzey xternal storage prefix path */
std::string
getYezzeyRelationUrl_internal(const std::string &nspname,
                              const std::string &relname,
                              const std::string &external_storage_prefix,
                              relnodeCoord coords, int32_t segid) {
  return external_storage_prefix +
         yezzey_block_file_path(nspname, relname, coords, segid);
}
