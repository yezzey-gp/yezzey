
#pragma once

#include "chunkinfo.h"
#include "io_adv.h"
#include "ylister.h"
#include "yreader.h"
#include "ywriter.h"
#include <memory>
#include <string>
#include <vector>

/* reader using yproxy */
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
  int prepareYproxyConnection(const ChunkInfo &ci, size_t start_off);
  std::vector<char> ConstructCatRequest(const ChunkInfo &ci, size_t start_off);

private:
  std::shared_ptr<IOadv> adv_{nullptr};
  ssize_t segindx_{0};
  int64_t order_ptr_{0};
  const std::vector<ChunkInfo> order_;
  int64_t current_chunk_remaining_bytes_{0};
  int64_t current_chunk_offset_{0};

  int client_fd_{-1};

  int current_retry{0};
  int retry_limit{1};
};

// write to external storage, using gpwriter.
// encrypt all data with gpg
class YProxyWriter : public YWriter {
public:
  explicit YProxyWriter(std::shared_ptr<IOadv> adv, ssize_t segindx,
                        ssize_t modcount, const std::string &storage_path);

  virtual ~YProxyWriter();

  virtual bool close();

  virtual bool write(const char *buffer, size_t *amount);

protected:
  /* prepare connection for chunk reading */
  int prepareYproxyConnection();
  std::vector<char> ConstructPutRequest(std::string fileName);
  std::vector<char> ConstructCopyDataRequest(const char *buffer, size_t amount);
  std::vector<char> CostructCommandCompleteRequest();
  int readRFQResponce();

private:
  std::string createXPath();
  int write_full(const std::vector<char> &msg);

  std::shared_ptr<IOadv> adv_;
  ssize_t segindx_;
  ssize_t modcount_;
  XLogRecPtr insertion_rec_ptr_;
  std::string storage_path_;

  int client_fd_{-1};

public:
  std::string getExternalStoragePath() { return storage_path_; }

  XLogRecPtr getInsertionStorageLsn() { return insertion_rec_ptr_; }
};

// list external storage using yproxy
class YProxyLister : public YLister {
public:
  explicit YProxyLister(std::shared_ptr<IOadv> adv, ssize_t segindx);

  virtual ~YProxyLister();

  virtual bool close();

  virtual std::vector<storageChunkMeta> list_relation_chunks();
  virtual std::vector<std::string> list_chunk_names();

protected:
  std::vector<char> ConstructListRequest(std::string fileName);
  int prepareYproxyConnection();

  struct message {
    char type;
    std::vector<char> content;
    int retCode;
  };
  message readMessage();
  std::vector<storageChunkMeta> readObjectMetaBody(std::vector<char> *body);

private:
  std::shared_ptr<IOadv> adv_;
  ssize_t segindx_;

  int client_fd_{-1};
};