#include "io_adv.h"

#include "offload_tablespace_map.h"

IOadv::IOadv(const std::string &engine_path, const std::string &gpg_key_id,
             const std::string &config_path, const std::string &nspname,
             const std::string &relname, const std::string &host,
             const std::string &bucket,
             const std::string &external_storage_prefix,
             const std::string &storage_class, const int &multipart_chunksize,
             const int &multipart_threshold, const Oid spcNode,
             const std::string &fileName, const Oid reloid,
             const std::string &walg_bin_path,
             const std::string &walg_config_path, bool use_gpg_crypto,
             const std::string &yproxy_socket)
    : engine_path(engine_path), gpg_key_id(gpg_key_id),
      config_path(config_path), nspname(nspname), relname(relname), host(host),
      bucket(bucket), external_storage_prefix(external_storage_prefix),
      storage_class(storage_class),
      multipart_chunksize(multipart_chunksize),
      multipart_threshold(multipart_threshold),
      coords_(getRelnodeCoordinate(spcNode, fileName)), reloid(reloid),
      tableSpace(reloid ? YezzeyGetRelationOriginTablespace(reloid) : "none"),
      walg_bin_path(walg_bin_path), walg_config_path(walg_config_path),
      use_gpg_crypto(use_gpg_crypto), yproxy_socket(yproxy_socket) {}

IOadv::IOadv(const std::string &engine_path, const std::string &gpg_key_id,
             const std::string &config_path, const std::string &nspname,
             const std::string &relname, const std::string &host,
             const std::string &bucket,
             const std::string &external_storage_prefix,
             const std::string &storage_class, const int &multipart_chunksize,
             const int &multipart_threshold, const relnodeCoord &coords,
             const Oid reloid, const std::string &walg_bin_path,
             const std::string &walg_config_path, bool use_gpg_crypto,
             const std::string &yproxy_socket)
    : engine_path(engine_path), gpg_key_id(gpg_key_id),
      config_path(config_path), nspname(nspname), relname(relname), host(host),
      bucket(bucket), external_storage_prefix(external_storage_prefix),
      storage_class(storage_class), multipart_chunksize(multipart_chunksize),
      multipart_threshold(multipart_threshold), coords_(coords), reloid(reloid),
      tableSpace(reloid ? YezzeyGetRelationOriginTablespace(reloid) : "none"),
      walg_bin_path(walg_bin_path), walg_config_path(walg_config_path),
      use_gpg_crypto(use_gpg_crypto), yproxy_socket(yproxy_socket) {}