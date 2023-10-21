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
  cmd += " st put --no-compress --read-stdin ";

  cmd += storage_path_;
  return cmd;
}

std::string WALGWriter::createXPath() {
  return craftStorageUnPrefixedPath(adv_, segindx_, modcount_,
                                    insertion_rec_ptr_);
}

WALGWriter::WALGWriter(std::shared_ptr<IOadv> adv, ssize_t segindx,
                       ssize_t modcount, const std::string &storage_path)
    : adv_(adv), segindx_(segindx), modcount_(modcount),
      insertion_rec_ptr_(yezzeyGetXStorageInsertLsn()),
      storage_path_(createXPath()), cmd_(craftString(adv, segindx, modcount)) {}

WALGWriter::~WALGWriter() { close(); }

bool WALGWriter::close() {
  if (initialized_) {
    // wal_g_();
    if (wal_g_ == nullptr) {
      // out of sync
      return false;
    }
    fclose(wal_g_);
    wal_g_ = nullptr;
    initialized_ = false;
  }
  return true;
}

bool WALGWriter::write(const char *buffer, size_t *amount) {
  if (!initialized_) {
    // wal_g_ = make_unique<redi::pstream>(cmd_);
    wal_g_ = popen(cmd_.c_str(), "w");
    initialized_ = true;
  }

  auto rv = fwrite(buffer, sizeof(char), *amount, wal_g_);
  
  if (rv <= 0) {
    *amount = 0;
    return false;
  }
  *amount = rv;
  return true;
}
