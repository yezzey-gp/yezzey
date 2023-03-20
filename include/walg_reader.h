#pragma once

#include "io_adv.h"
#include "pstreams/pstream.h"
#include "yreader.h"
#include <memory>

class WALGReader : public YReader {
public:
  friend class ExternalWriter;
  explicit WALGReader(std::shared_ptr<IOadv> adv, ssize_t segindx);
  ~WALGReader();

public:
  virtual bool close();
  virtual bool read(char *buffer, size_t *amount);
  virtual void BumpArenda(size_t count);

  virtual bool empty();

public:
protected:
  /* prepare command dispatch */

  int prepare();

private:
  std::shared_ptr<IOadv> adv_;
  ssize_t segindx_;
  std::string cmd_;

  std::unique_ptr<redi::ipstream> wal_g_{nullptr};
  bool initialized_{false};
};