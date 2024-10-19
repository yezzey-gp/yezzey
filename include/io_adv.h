#ifndef YEZZEY_IO_ADV_H
#define YEZZEY_IO_ADV_H

#include "pg.h"
#include "types.h"
#include <string>

/* storage & gpg related configration */
struct IOadv {
  // private fields
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
  const int multipart_chunksize;
  bool multipart_upload;

  // base/5/12345.1

  const relnodeCoord coords_;

  const Oid reloid;

  // Origin tablespace of offloaded relation
  const std::string tableSpace;

  bool use_gpg_crypto;

  // yproxy
  const std::string yproxy_socket;

  IOadv(const std::string &nspname,
        const std::string &relname, const std::string &host,
        const std::string &bucket, const std::string &external_storage_prefix,
        const std::string &storage_class, const int &multipart_chunksize,
        /*unparse coords*/ Oid spcNode, const std::string &fileName,
        const Oid reloid, bool use_gpg_crypto,
        const std::string &yproxy_socket);

  IOadv(const std::string &nspname,
        const std::string &relname, const std::string &host,
        const std::string &bucket, const std::string &external_storage_prefix,
        const std::string &storage_class, const int &multipart_chunksize,
        const relnodeCoord &coords, const Oid reloid,
        bool use_gpg_crypto, const std::string &yproxy_socket);
};

#endif /*YEZZEY_IO_ADV_H*/