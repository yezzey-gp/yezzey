#ifndef YEZZEY_IO_ADV_H
#define YEZZEY_IO_ADV_H

#include "pg.h"
#include "types.h"
#include <string>

/* storage & gpg related configration */
struct IOadv {
  // private fields

  //  GPG - related
  /* "/usr/bin/gpg" or something */
  const std::string engine_path;
  const std::string gpg_key_id;

  //  storage + YPROXY related

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

  // s3 storage class
  const std::string storage_class;

  // base/5/12345.1

  const relnodeCoord coords_;

  const Oid reloid;

  // Origin tablespace of offloaded relation
  const std::string tableSpace;

  // /usr/bin/wal-g-gp
  const std::string walg_bin_path;
  const std::string walg_config_path;

  bool use_gpg_crypto;

  // yproxy
  const std::string yproxy_socket;

  IOadv(const std::string &engine_path, const std::string &gpg_key_id,
        const std::string &config_path, const std::string &nspname,
        const std::string &relname, const std::string &host,
        const std::string &bucket, const std::string &external_storage_prefix,
        const std::string &storage_class,
        /*unparse coords*/ Oid spcNode, const std::string &fileName,
        const Oid reloid, const std::string &walg_bin_path,
        const std::string &walg_config_path, bool use_gpg_crypto,
        const std::string &yproxy_socket);

  IOadv(const std::string &engine_path, const std::string &gpg_key_id,
        const std::string &config_path, const std::string &nspname,
        const std::string &relname, const std::string &host,
        const std::string &bucket, const std::string &external_storage_prefix,
        const std::string &storage_class, const relnodeCoord &coords,
        const Oid reloid, const std::string &walg_bin_path,
        const std::string &walg_config_path, bool use_gpg_crypto,
        const std::string &yproxy_socket);
};

#endif /*YEZZEY_IO_ADV_H*/