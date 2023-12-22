#pragma once

#include "chunkinfo.h"
#include "io_adv.h"
#include "pstreams/pstream.h"
#include "yreader.h"
#include <memory>
#include <string>
#include <vector>

class WALGReader : public YReader {
public:
  friend class ExternalWriter;
  explicit WALGReader(std::shared_ptr<IOadv> adv, ssize_t segindx);
  ~WALGReader();

public:
  virtual bool close();
  virtual bool read(char *buffer, size_t *amount);

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

/* wal-g reader using only wal-g st */
class WALGSTReader : public YReader {
public:
  friend class ExternalWriter;
  explicit WALGSTReader(std::shared_ptr<IOadv> adv, ssize_t segindx,
                        std::vector<ChunkInfo> order);
  ~WALGSTReader();

public:
  virtual bool close();
  virtual bool read(char *buffer, size_t *amount);

  virtual bool empty();

public:
  std::string craftString(std::string x_path, size_t segindx);

protected:
  /* prepare command dispatch */

  int prepare();

private:
  std::shared_ptr<IOadv> adv_;
  ssize_t segindx_;
  int64_t order_ptr_{0};
  const std::vector<ChunkInfo> order_;
  std::string cmd_;

  FILE *wal_g_{nullptr};
};