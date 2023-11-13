
#pragma once

#include "io_adv.h"
#include "yreader.h"
#include <memory>
#include <string>
#include <vector>


/* wal-g reader useing only wal-g st */
class YProxyReader : public YReader {
public:
  friend class ExternalWriter;
  explicit YProxyReader(std::shared_ptr<IOadv> adv, ssize_t segindx,
                        std::vector<ChunkInfo> order);
  ~YProxyReader();

public:
  virtual bool close();
  virtual bool read(char *buffer, size_t *amount);

  virtual bool empty();

public:

protected:
  /* prepare connection for chunk reading */ 
  int prepareYproxyConnection(const ChunkInfo & ci);
  std::vector<char> CostructCatRequest(const ChunkInfo & ci);

private:
  std::shared_ptr<IOadv> adv_{nullptr};
  ssize_t segindx_{0};
  int64_t order_ptr_{0};
  int64_t current_chunk_remaining_bytes_{0};
  const std::vector<ChunkInfo> order_;

  int client_fd_{-1};
};