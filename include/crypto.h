
#pragma once

#include "blocking_buf.h"
#include "x_reader.h"
#include "io_adv.h"
#include "yreader.h"
#include "ywriter.h"
#include <memory>
#include <thread>

#ifdef HAVE_CRYPTO

#include <gpgme.h>
// first arg should be (expected to be) YIO *
ssize_t yezzey_crypto_stream_dec_read(void *handler, void *buffer, size_t size);

// first arg should be (expected to be) YIO *
ssize_t yezzey_crypto_stream_dec_write(void *handler, const void *buffer,
                                       size_t size);

// first arg should be (expected to be) YIO *
ssize_t yezzey_crypto_stream_enc_read(void *handler, void *buffer, size_t size);

// first arg should be (expected to be) YIO *
ssize_t yezzey_crypto_stream_enc_write(void *handler, const void *buffer,
                                       size_t size);

// YIO
void yezzey_crypto_stream_free(void *handler);

static struct gpgme_data_cbs yezzey_crypto_dec_cbs = {
    yezzey_crypto_stream_dec_read,
    yezzey_crypto_stream_dec_write,
    NULL,
    yezzey_crypto_stream_free,
};

static struct gpgme_data_cbs yezzey_crypto_enc_cbs = {
    yezzey_crypto_stream_enc_read,
    yezzey_crypto_stream_enc_write,
    NULL,
    yezzey_crypto_stream_free,
};

class Crypter {
public:
  //  GPG - related structs
  gpgme_data_t crypto_in;
  gpgme_data_t crypto_out;

  gpgme_ctx_t crypto_ctx;
  gpgme_key_t keys[2]{NULL, NULL};
  gpgme_key_t key;

  bool crypto_initialized_{false};
  // handler thread
  std::unique_ptr<std::thread> wt{nullptr};

  int64_t tot_read{0};

  //

  std::shared_ptr<IOadv> adv_;
  std::shared_ptr<YReader> reader_{nullptr};
  std::shared_ptr<YWriter> writer_{nullptr};

  std::shared_ptr<BlockingBuffer> buf_;

  void waitio();

  Crypter(std::shared_ptr<IOadv> adv, std::shared_ptr<YReader> reader,
          std::shared_ptr<BlockingBuffer> buf);

  Crypter(std::shared_ptr<IOadv> adv, std::shared_ptr<YWriter> writer,
          std::shared_ptr<BlockingBuffer> buf);

  ~Crypter();

  void io_dispatch_encrypt();
  void io_dispatch_decrypt();

  int io_prepare_crypt(bool dec);
};

#else

class Crypter {
public:
  bool crypto_initialized_{false};
  // handler thread
  std::unique_ptr<std::thread> wt{nullptr};

  //

  std::shared_ptr<IOadv> adv_;
  std::shared_ptr<YReader> reader_{nullptr};
  std::shared_ptr<YWriter> writer_{nullptr};

  std::shared_ptr<BlockingBuffer> buf_;

  void waitio();

  Crypter(std::shared_ptr<IOadv> adv, std::shared_ptr<YReader> reader,
          std::shared_ptr<BlockingBuffer> buf);

  Crypter(std::shared_ptr<IOadv> adv, std::shared_ptr<YWriter> writer,
          std::shared_ptr<BlockingBuffer> buf);

  ~Crypter();

  void io_dispatch_encrypt();
  void io_dispatch_decrypt();

  int io_prepare_crypt(bool dec);
};

#endif