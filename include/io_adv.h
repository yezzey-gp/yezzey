#ifndef YEZZEY_IO_ADV_H
#define YEZZEY_IO_ADV_H

#include <string>

/* storage & gpg related configration */
struct IOadv {
  // private fields

  //  GPG - related
  /* "/usr/bin/gpg" or something */
  const std::string engine_path;
  const std::string gpg_key_id;

  //  storage + WAL-G related

  const std::string config_path;
  // schema name for relation
  const std::string nspname;
  // relation name itself
  const std::string relname;
  // s3 host
  const std::string host;
  // s3 bucked
  const std::string bucket;
  // wal-g specific prefix
  const std::string external_storage_prefix;
  // base/5/12345.1
  const std::string fileName;

  bool use_gpg_crypto;

  IOadv(const std::string &engine_path, const std::string &gpg_key_id,
        const std::string &config_path, const std::string &nspname,
        const std::string &relname, const std::string &host,
        const std::string &bucket, const std::string &external_storage_prefix,
        const std::string &fileName, bool use_gpg_crypto);
};

#endif /*YEZZEY_IO_ADV_H*/