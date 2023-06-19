#pragma once

#include "external_reader.h"
#include "gpwriter.h"
#include "io_adv.h"
#include "util.h"
#include "ylister.h"
#include "ywriter.h"
#include <memory>
#include <string>

// write to external storage, using gpwriter.
// encrypt all data with gpg
class ExternalWriter : public YWriter {

public:
  explicit ExternalWriter(std::shared_ptr<IOadv> adv, ssize_t segindx,
                          ssize_t modcount, const std::string &storage_path);

  virtual ~ExternalWriter();

  virtual bool close();

  virtual bool write(const char *buffer, size_t *amount);

private:
  void createWriterHandleToPath();
  void createWriterHandle();

private:
  std::shared_ptr<IOadv> adv_;
  ssize_t segindx_;
  ssize_t modcount_;
  const std::string storage_path_;

protected:
  GPWriter *writer_{nullptr};
  bool cleaned_{false};

private:
  const std::string storage_offload_path_;

public:
  std::string getExternalStoragePath() { return storage_offload_path_; }
};