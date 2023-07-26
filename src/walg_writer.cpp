#include "walg_writer.h"
#include "meta.h"
#include "url.h"
#include "util.h"

/*
wal-g
    --config=<conf>
    aosegfile-offload
    <path> (will be prefixed)
*/

/*

std::string craftStoragePath(const std::shared_ptr<YLister> &lister,
                      const std::shared_ptr<IOADV> &adv,
                      ssize_t segindx, ssize_t modcount, const std::string
&prefix);
*/

std::string WALGWriter::craftString(const std::shared_ptr<IOadv> &adv,
                                    ssize_t segindx, ssize_t modcount) {

  std::string cmd = adv->walg_bin_path;

  cmd += " --config=" + adv->walg_config_path;
  cmd += " aosegfile-offload ";

  auto modified_x_path = std::string(storage_path_);
  modified_x_path.erase(modified_x_path.begin(),
                        modified_x_path.begin() +
                            adv->external_storage_prefix.size());

  cmd += modified_x_path;
  return cmd;
}

std::string WALGWriter::createXPath() {
  return craftStoragePath(adv_, segindx_, modcount_,
                          adv_->external_storage_prefix, insertion_rec_ptr_);
}

WALGWriter::WALGWriter(std::shared_ptr<IOadv> adv, ssize_t segindx,
                       ssize_t modcount, const std::string &storage_path)
    : adv_(adv), segindx_(segindx), modcount_(modcount),
      insertion_rec_ptr_(yezzeyGetXStorageInsertLsn()),
      storage_path_(createXPath()), cmd_(craftString(adv, segindx, modcount)) {}

WALGWriter::~WALGWriter() { close(); }

bool WALGWriter::close() {
  if (initialized_) {
    wal_g_->close();
  }
  return true;
}

bool WALGWriter::write(const char *buffer, size_t *amount) {
  if (!initialized_) {
    wal_g_ = make_unique<redi::pstream>(cmd_);
    initialized_ = true;
  }

  wal_g_->write(buffer, *amount);
  if (wal_g_->fail()) {
    *amount = 0;
    return false;
  }
  return true;
}
