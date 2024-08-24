#include "xvacuum.h"
#include "gpcleaner.h"
#include "gucs.h"
#include "pg.h"
#include <string>
#include <url.h>
#include <util.h>

/*
 * yezzey_delete_chunk_internal:
 * Given external chunk path, remove it from external storage
 * TBD: check, that chunk status is obsolete and other sanity checks
 * to avoid deleting chunk, which can we needed to read relation data
 */
int yezzey_delete_chunk_internal(const char *external_chunk_path) {
  try {
    auto ioadv = std::make_shared<IOadv>(
        std::string(gpg_engine_path), std::string(gpg_key_id),
        std::string(storage_config), "", "", std::string(storage_host /*host*/),
        std::string(storage_bucket /*bucket*/),
        std::string(storage_prefix /*prefix*/),
        std::string(storage_class /*storage_class*/),
        "BASE" /* FIXME */,
        DEFAULTTABLESPACE_OID, "" /* coords */,
        InvalidOid /* reloid */, std::string(walg_bin_path),
        std::string(walg_config_path), use_gpg_crypto, yproxy_socket);

    auto x_path = std::string(external_chunk_path);
    auto init_url = getYezzeyExtrenalStorageBucket(ioadv->host.c_str(),
                                                   ioadv->bucket.c_str()) +
                    storage_url_add_options(x_path, ioadv->config_path.c_str());

    elog(LOG, "removing external chunk with url %s", init_url.c_str());

    auto cleaner = cleaner_init(init_url.c_str());
    int rc = cleaner->clean();
    return rc;
  } catch (...) {
    elog(ERROR, "failed to prepare x-storage reader for chunk");
    return 0;
  }
  return 0;
}
