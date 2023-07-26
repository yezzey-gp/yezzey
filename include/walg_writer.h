#pragma once

#include "io_adv.h"
#include "pstreams/pstream.h"
#include "ylister.h"
#include "ywriter.h"
#include <memory>
#include <string>

// write to external storage, using gpwriter.
// encrypt all data with gpg
class WALGWriter : public YWriter {

public:
  explicit WALGWriter(std::shared_ptr<IOadv> adv, ssize_t segindx,
                      ssize_t modcount, const std::string &storage_path);

  virtual ~WALGWriter();

  virtual bool close();

  virtual bool write(const char *buffer, size_t *amount);

  

private:
  std::string craftString(const std::shared_ptr<IOadv> &adv, ssize_t segindx,
                        ssize_t modcount);
  std::string createXPath();

  std::shared_ptr<IOadv> adv_;
  ssize_t segindx_;
  ssize_t modcount_;
  std::string storage_path_;
  std::string cmd_;

  std::unique_ptr<redi::pstream> wal_g_{nullptr};
  bool initialized_{false};


public:
  std::string getExternalStoragePath() { return storage_path_; }
};