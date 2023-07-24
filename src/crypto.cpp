

#include "crypto.h"
#include "meta.h"
#include <iostream>
#ifdef HAVE_CRYPTO
// decrypt operation

// first arg should be (expected to be) YIO *
ssize_t yezzey_crypto_stream_dec_read(void *handler, void *buffer,
                                      size_t size) {
  auto y_handler = (Crypter *)handler;
  size_t inner_amount = size;
  auto res = y_handler->reader_->read((char *)buffer, &inner_amount);
  y_handler->tot_read += inner_amount;
  if (!res) {
    return -1;
  }
  return inner_amount;
}

// first arg should be (expected to be) YIO *
ssize_t yezzey_crypto_stream_dec_write(void *handler, const void *buffer,
                                       size_t size) {
  auto y_handler = (Crypter *)handler;
  auto ret = y_handler->buf_->write((const char *)buffer, size);
  return ret;
}

// first arg should be (expected to be) YIO *
ssize_t yezzey_crypto_stream_enc_read(void *handler, void *buffer,
                                      size_t size) {
  auto y_handler = (Crypter *)handler;
  auto ret = y_handler->buf_->read((char *)buffer, size);
  return ret;
}

// first arg should be (expected to be) YIO *
ssize_t yezzey_crypto_stream_enc_write(void *handler, const void *buffer,
                                       size_t size) {
  auto y_handler = (Crypter *)handler;

  size_t inner_amount = size;
  auto res = y_handler->writer_->write((const char *)buffer, &inner_amount);
  if (!res || inner_amount <= 0) {
    return -1;
  }
  return inner_amount;
}

void yezzey_crypto_stream_free(void *handler) {
  /* no-op */
  // handler
}

Crypter::Crypter(std::shared_ptr<IOadv> adv, std::shared_ptr<YReader> reader,
                 std::shared_ptr<BlockingBuffer> buf)
    : adv_(adv), reader_(reader), buf_(buf) {}

Crypter::Crypter(std::shared_ptr<IOadv> adv, std::shared_ptr<YWriter> writer,
                 std::shared_ptr<BlockingBuffer> buf)
    : adv_(adv), writer_(writer), buf_(buf) {}

void init_gpgme(gpgme_protocol_t proto) {
  gpgme_error_t err;

  gpgme_check_version(NULL);
  setlocale(LC_ALL, "");
  gpgme_set_locale(NULL, LC_CTYPE, setlocale(LC_CTYPE, NULL));

  err = gpgme_engine_check_version(proto);
}

Crypter::~Crypter() {
  if (crypto_initialized_) {
    gpgme_data_release(crypto_in);
    gpgme_data_release(crypto_out);
    gpgme_release(crypto_ctx);
  }
}

int Crypter::io_prepare_crypt(bool dec) {
  gpgme_error_t err;
  char *agent_info;
  agent_info = getenv("GPG_AGENT_INFO");

  init_gpgme(GPGME_PROTOCOL_OpenPGP);

  err = gpgme_new(&crypto_ctx);
  if (err) {
    auto errstr = gpgme_strerror(err);
    std::cerr << errstr << "\n";
    return -1;
  }

  // set ACSII armor to false
  // wal-g uses opengpg, which does not support
  // ACSII armor
  gpgme_set_armor(crypto_ctx, 0);

  //   fail_if_err (err);
  /* Might want to comment gpgme_ctx_set_engine_info() below. */
  err = gpgme_ctx_set_engine_info(crypto_ctx, GPGME_PROTOCOL_OpenPGP,
                                  adv_->engine_path.c_str(), NULL);
  if (err) {
    auto errstr = gpgme_strerror(err);
    std::cerr << errstr << "\n";
    return -1;
  }
  /* Search key for encryption. */
  err = gpgme_get_key(crypto_ctx, adv_->gpg_key_id.c_str(), &key, 0);
  if (err) {
    auto errstr = gpgme_strerror(err);
    std::cerr << errstr << "\n";
    return -1;
  }
  /* Initialize output buffer. */
  err = gpgme_data_new(&crypto_out);
  if (err) {
    auto errstr = gpgme_strerror(err);
    std::cerr << errstr << "\n";
    return -1;
  }

  crypto_initialized_ = true;

  // fail_if_err (err);

  Crypter *own = this;

  if (dec) {
    /* Initialize input buffer. */
    err = gpgme_data_new_from_cbs(&crypto_in, &yezzey_crypto_dec_cbs, own);
    if (err) {
      return -1;
    }
    // fail_if_err (err);

    err = gpgme_data_new_from_cbs(&crypto_out, &yezzey_crypto_dec_cbs, own);
    if (err) {
      return -1;
    }
  } else {
    /* Initialize input buffer. */
    err = gpgme_data_new_from_cbs(&crypto_in, &yezzey_crypto_enc_cbs, own);
    if (err) {
      return -1;
    }
    // fail_if_err (err);

    err = gpgme_data_new_from_cbs(&crypto_out, &yezzey_crypto_enc_cbs, own);
    if (err) {
      return -1;
    }
  }

  /* Encrypt data. */
  keys[0] = key;

  /* Decrypt data. */

  return 0;
}

void Crypter::io_dispatch_encrypt() {
  wt = make_unique<std::thread>([&]() {
    gpgme_error_t err;
    err = gpgme_op_encrypt(crypto_ctx, (gpgme_key_t *)keys,
                           GPGME_ENCRYPT_ALWAYS_TRUST, crypto_in, crypto_out);
    if (err) {
      auto errstr = gpgme_strerror(err);
      std::cerr << "failed to dipatch encrypt " << errstr << '\n';
      return;
      // bad
    }
  });
}

void Crypter::waitio() {
  if (wt && wt->joinable()) {
    wt->join();
  }
  wt = nullptr;
}

void Crypter::io_dispatch_decrypt() {
  wt = make_unique<std::thread>([&]() {
    for (; !reader_->empty();) {
      /* operation per file */
      gpgme_error_t err;
      err = gpgme_op_decrypt(crypto_ctx, crypto_in, crypto_out);

      if (err) {
        buf_->close();
        auto errstr = gpgme_strerror(err);
        std::cerr << "failed to dispatch decrypt " << errstr << '\n';
        return;
        // bad
      }
    }
    buf_->close();
    // fail_if_err (err);
  });
}

#else

Crypter::Crypter(std::shared_ptr<IOadv> adv, std::shared_ptr<YReader> reader,
                 std::shared_ptr<BlockingBuffer> buf)
    : adv_(adv), reader_(reader), buf_(buf) {}

Crypter::Crypter(std::shared_ptr<IOadv> adv, std::shared_ptr<YWriter> writer,
                 std::shared_ptr<BlockingBuffer> buf)
    : adv_(adv), writer_(writer), buf_(buf) {}

Crypter::~Crypter() {}

int Crypter::io_prepare_crypt(bool dec) { return -1; }

void Crypter::io_dispatch_encrypt() {}

void Crypter::waitio() {}

void Crypter::io_dispatch_decrypt() {}

#endif
