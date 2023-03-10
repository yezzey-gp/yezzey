#pragma once

#include "external_reader.h"
#include "gpwriter.h"
#include "io_adv.h"
#include "util.h"
#include "yreader.h"
#include "ywriter.h"
#include <memory>
#include <string>

// write to external storage, using gpwriter.
// encrypt all data with gpg
class ExternalWriter : public YWriter {

public:
  explicit ExternalWriter(std::shared_ptr<IOadv> adv, ssize_t segindx,
                          ssize_t modcount, const std::string &storage_path,
                          std::shared_ptr<YReader> reader_);

  virtual ~ExternalWriter();

  virtual bool close();

  virtual bool write(const char *buffer, size_t *amount);

protected:
  GPWriter *writer_{nullptr};
  std::shared_ptr<YReader> reader_{nullptr};
  bool cleaned_{false};

private:
  void createWriterHandleToPath();
  void createWriterHandle();

private:
  std::shared_ptr<IOadv> adv_;
  const std::string storage_path_;
  ssize_t segindx_;
  ssize_t modcount_;
};