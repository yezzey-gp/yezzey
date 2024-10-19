#include "io_adv.h"

#include "offload_tablespace_map.h"

IOadv::IOadv(const std::string &nspname,
             const std::string &relname, const std::string &host,
             const std::string &bucket,
             const std::string &external_storage_prefix,
             const std::string &storage_class, const int &multipart_chunksize,
             const Oid spcNode, const std::string &fileName, const Oid reloid,
             bool use_gpg_crypto,
             const std::string &yproxy_socket)
    :
      nspname(nspname), relname(relname), host(host),
      bucket(bucket), external_storage_prefix(external_storage_prefix),
      storage_class(storage_class), multipart_chunksize(multipart_chunksize),
      coords_(getRelnodeCoordinate(spcNode, fileName)), reloid(reloid),
      tableSpace(reloid ? YezzeyGetRelationOriginTablespace(nspname.c_str(), relname.c_str(), reloid) : "none"),
      use_gpg_crypto(use_gpg_crypto), yproxy_socket(yproxy_socket) {
  multipart_upload = true;
}

IOadv::IOadv(const std::string &nspname,
             const std::string &relname, const std::string &host,
             const std::string &bucket,
             const std::string &external_storage_prefix,
             const std::string &storage_class, const int &multipart_chunksize,
             const relnodeCoord &coords, const Oid reloid,bool use_gpg_crypto,
             const std::string &yproxy_socket)
    : nspname(nspname), relname(relname), host(host),
      bucket(bucket), external_storage_prefix(external_storage_prefix),
      storage_class(storage_class), multipart_chunksize(multipart_chunksize),
      coords_(coords), reloid(reloid),
      tableSpace(reloid ? YezzeyGetRelationOriginTablespace(nspname.c_str(), relname.c_str(), reloid) : "none"),
      use_gpg_crypto(use_gpg_crypto), yproxy_socket(yproxy_socket) {
  multipart_upload = true;
}