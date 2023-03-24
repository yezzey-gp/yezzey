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

std::string craftString(const std::shared_ptr<YLister> &lister,
                        const std::shared_ptr<IOadv> &adv, ssize_t segindx,
                        ssize_t modcount) {

  std::string cmd = adv->walg_bin_path;

  cmd += " --config=" + adv->walg_config_path;
  cmd += " aosegfile-offload ";
  cmd += craftStoragePath(lister, adv, segindx, modcount, "segments_005");
  return cmd;
}

WALGWriter::WALGWriter(std::shared_ptr<IOadv> adv, ssize_t segindx,
                       ssize_t modcount, const std::string &storage_path,
                       std::shared_ptr<YLister> lister)
    : adv_(adv), segindx_(segindx), lister_(lister), modcount_(modcount),
      cmd_(craftString(lister, adv, segindx, modcount)) {}

WALGWriter::~WALGWriter() { close(); }

bool WALGWriter::close() {
  if (initialized_) {
    wal_g_->close();
  }
  return true;
}

bool WALGWriter::write(const char *buffer, size_t *amount) {
  if (!initialized_) {
    wal_g_ = make_unique<redi::opstream>(cmd_);
    initialized_ = true;
  }

  wal_g_->write(buffer, *amount);
  if (wal_g_->fail()) {
    return -1;
  }
  return *amount;
}
