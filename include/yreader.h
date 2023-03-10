#pragma once

#include <string>
#include <vector>

struct storageChunkMeta {
  int64_t chunkSize;
  std::string chunkName;
};

class YReader {
public:
  virtual ~YReader(){

  };

  virtual bool read(char *buffer, size_t *amount) = 0;
  virtual bool empty() = 0;

  virtual std::vector<storageChunkMeta> list_relation_chunks() = 0;
  virtual std::vector<std::string> list_chunk_names() = 0;
  // limit number of relation external storage chunks to revieve
  virtual void BumpArenda(size_t count) = 0;
  virtual bool close() = 0;
};
