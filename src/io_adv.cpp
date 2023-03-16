#include "io_adv.h"

IOadv::IOadv(const std::string &engine_path, const std::string &gpg_key_id,
             const std::string &config_path, const std::string &nspname,
             const std::string &relname, const std::string &host,
             const std::string &bucket,
             const std::string &external_storage_prefix,
             const std::string &fileName, const std::string &walg_bin_path,
             const std::string &walg_config_path,
             bool use_gpg_crypto)
    : engine_path(engine_path), gpg_key_id(gpg_key_id),
      config_path(config_path), nspname(nspname), relname(relname), host(host),
      bucket(bucket), external_storage_prefix(external_storage_prefix),
      coords_(getRelnodeCoordinate(fileName)), walg_bin_path(walg_bin_path),
      walg_config_path(walg_config_path),
      use_gpg_crypto(use_gpg_crypto) {}

IOadv::IOadv(const std::string &engine_path, const std::string &gpg_key_id,
             const std::string &config_path, const std::string &nspname,
             const std::string &relname, const std::string &host,
             const std::string &bucket,
             const std::string &external_storage_prefix,
             const relnodeCoord &coords, const std::string &walg_bin_path,
             const std::string &walg_config_path,
             bool use_gpg_crypto)
    : engine_path(engine_path), gpg_key_id(gpg_key_id),
      config_path(config_path), nspname(nspname), relname(relname), host(host),
      bucket(bucket), external_storage_prefix(external_storage_prefix),
      coords_(coords), walg_bin_path(walg_bin_path), walg_config_path(walg_config_path),
      use_gpg_crypto(use_gpg_crypto) {}