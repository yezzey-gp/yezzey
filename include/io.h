#ifndef YEZZEY_IO_H
#define YEZZEY_IO_H

#include <memory>
#include <vector>

#include "yreader.h"
#include "ywriter.h"

#include "io_adv.h"

struct YIO {
  // private fields

  // bool use_gpg_crypto;

  // GPG

  //  S3 + WAL-G - related structs

  //   reader and writer
  std::shared_ptr<YReader> reader_{nullptr};
  std::shared_ptr<YWriter> writer_{nullptr};

  std::shared_ptr<IOadv> adv_;
  ssize_t segindx_;
  ssize_t modcount_;

  // construcor
  YIO(std::shared_ptr<IOadv> adv, ssize_t segindx, ssize_t modcount,
      const std::string &storage_path);

  ~YIO();

  // copyable
  YIO(const YIO &buf) = default;
  YIO &operator=(const YIO &) = default;

  bool reader_empty();

  std::vector<storageChunkMeta> list_relation_chunks();

  bool io_read(char *buffer, size_t *amount);
  bool io_write(char *buffer, size_t *amount);
  bool io_close();
};

#endif /* YEZZEY_IO_H */
