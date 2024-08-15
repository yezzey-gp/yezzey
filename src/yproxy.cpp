#include "yproxy.h"
#include "gucs.h"
#include "meta.h"
#include "url.h"
#include "util.h"
#include <iostream>
#include <sys/socket.h>

#include <unistd.h>

const int kDefaultRetryLimit = 100;


YProxyReader::YProxyReader(std::shared_ptr<IOadv> adv, ssize_t segindx,
                           const std::vector<ChunkInfo> order)
    : adv_(adv), segindx_(segindx), order_ptr_(0), order_(order),
      current_chunk_remaining_bytes_(0), client_fd_(-1), current_retry(0), retry_limit(kDefaultRetryLimit) {}

YProxyReader::~YProxyReader() { close(); }

bool YProxyReader::close() {
  if (client_fd_ != -1) {
    ::close(client_fd_);
    client_fd_ = -1;
  }
  return true;
}

const char DecryptRequest = 1;
const char NoDecryptRequest = 0;
const char ExtendedMessage = 1;
const char EncryptRequest = 1;
const char NoEncryptRequest = 0;
const char MessageTypeCat = 42;
const char MessageTypePut = 43;
const char MessageTypeCommandComplete = 44;
const char MessageTypeReadyForQuery = 45;
const char MessageTypeCopyData = 46;
const char MessageTypeList = 48;
const char MessageTypeObjectMeta = 49;

const size_t MSG_HEADER_SIZE = 8;
const size_t PROTO_HEADER_SIZE = 4;
const size_t OFFSET_SZ = 8;

std::vector<char> YProxyReader::ConstructCatRequest(const ChunkInfo &ci, size_t start_off) {
  std::vector<char> buff(
      MSG_HEADER_SIZE + PROTO_HEADER_SIZE + ci.x_path.size() + 1 + (start_off == 0 ? 0 : OFFSET_SZ), 0);
  buff[8] = MessageTypeCat;
  if (ci.enc) {
    buff[9] = DecryptRequest;
  } else {
    buff[9] = NoDecryptRequest;
  }

  if (start_off != 0) {
    buff[10] = ExtendedMessage;
  }
  uint64_t len = buff.size();

  strncpy(buff.data() + MSG_HEADER_SIZE + PROTO_HEADER_SIZE, ci.x_path.c_str(),
          ci.x_path.size());

  uint64_t cp = len;
  for (ssize_t i = 7; i >= 0; --i) {
    buff[i] = cp & ((1 << 8) - 1);
    cp >>= 8;
  }

  if (start_off != 0) {
    cp = start_off;
    for (ssize_t i = 7; i >= 0; --i) {
      buff[MSG_HEADER_SIZE + PROTO_HEADER_SIZE + ci.x_path.size() + 1 + i] = cp & ((1 << 8) - 1);
      cp >>= 8;
    }
  }

  return buff;
}

int YProxyReader::prepareYproxyConnection(const ChunkInfo &ci, size_t start_off) {
  // open unix data socket

  client_fd_ = socket(AF_UNIX, SOCK_STREAM, 0);
  if (client_fd_ == -1) {
    // throw here?
    elog(WARNING, "failed to create unix socket, errno: %m");
    return -1;
  }

  struct sockaddr_un addr;
  /* Bind socket to socket name. */

  memset(&addr, 0, sizeof(addr));

  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, adv_->yproxy_socket.c_str(),
          sizeof(addr.sun_path) - 1);

  auto ret =
      ::connect(client_fd_, (const struct sockaddr *)&addr, sizeof(addr));
  if (ret == -1) {
    // THROW here?
    elog(WARNING, "failed to acquire connection to unix socket on %s, errno: %m", adv_->yproxy_socket.c_str());
    return -1;
  }

  auto msg = ConstructCatRequest(ci, start_off);

  size_t rc = ::write(client_fd_, msg.data(), msg.size());

  if (rc <= 0) {
    // handle
    return -1;
  }

  /* reset retry count */
  this->current_retry = 0;

  // now we are ready to read our request data
  return client_fd_;
}

bool YProxyReader::read(char *buffer, size_t *amount) {
  // preparing done, read data

  while (1) {
    if (current_chunk_remaining_bytes_ == 0) {
      // no more data to read
      if (order_ptr_ == order_.size()) {
        *amount = 0;
        return false;
      }

      // close previous read socket, if any
      if (!this->close()) {
        // wtf?
        return false;
      }
      auto rc = this->prepareYproxyConnection(order_[order_ptr_], 0);
      if (rc < 0) {
        continue;
      }
      current_chunk_offset_ = 0;
      current_chunk_remaining_bytes_ = order_[order_ptr_].size;
    }

    auto rc = ::read(client_fd_, buffer, *amount);
    if (rc <= 0) {
      elog(WARNING, "reacquiring connection on offset %d", current_chunk_offset_);

      if (++this->current_retry < this->retry_limit) {
        auto rrc = this->prepareYproxyConnection(order_[order_ptr_], current_chunk_offset_);
        if (rrc < 0) {
          sleep(1);
          continue;
        }
      } else {
        // error, and we are out of retries.
        *amount = rc;
        return false;
      }
      continue;
    }
    // what if rc > current_chunk_remaining_bytes_ ?
    current_chunk_remaining_bytes_ -= rc;
    current_chunk_offset_ += rc;
    if (current_chunk_remaining_bytes_ == 0) {
      ++order_ptr_;
    }
    *amount = rc;

    return true;
  }
}

bool YProxyReader::empty() {
  return order_ptr_ == order_.size() && current_chunk_remaining_bytes_ <= 0;
};

std::string YProxyWriter::createXPath() {
  return craftStorageUnPrefixedPath(adv_, segindx_, modcount_,
                                    insertion_rec_ptr_);
}

YProxyWriter::YProxyWriter(std::shared_ptr<IOadv> adv, ssize_t segindx,
                           ssize_t modcount, const std::string &storage_path)
    : adv_(adv), segindx_(segindx), modcount_(modcount),
      insertion_rec_ptr_(yezzeyGetXStorageInsertLsn()),
      storage_path_(createXPath()) {}

YProxyWriter::~YProxyWriter() { close(); }

// complete external storage interaction.
// TBD: smgr_FileSync() here ?

int YProxyWriter::write_full(const std::vector<char> &msg) {
  int len = msg.size();
  int sync_offset = 0;
  while (len > 0) {
    auto rc = ::write(client_fd_, msg.data() + sync_offset, len);

    if (rc <= 0) {
      // handle
      return -1;
    }
    len -= rc;
    sync_offset += rc;
  }
  return 0;
}

bool YProxyWriter::close() {
  if (client_fd_ == -1) {
    return true;
  }
  auto msg = CostructCommandCompleteRequest();

  // signal that current chunk is full
  if (write_full(msg) == -1) {
    ::close(client_fd_);
    client_fd_ = -1;
    return false;
  }
  // wait for responce
  if (readRFQResponce() != 0) {
    ::close(client_fd_);
    client_fd_ = -1;
    // some error, handle
    return false;
  }
  ::close(client_fd_);
  client_fd_ = -1;
  return true;
}

bool YProxyWriter::write(const char *buffer, size_t *amount) {
  if (client_fd_ == -1) {
    if (prepareYproxyConnection() == -1) {
      // Throw here?
      return false;
    }
  }

  // TODO: split to chunks
  auto msg = ConstructCopyDataRequest(buffer, *amount);

  if (write_full(msg) == -1) {
    *amount = 0;
    return false;
  }
  // *amount does not need to change in case of successfull write

  return true;
}

int YProxyWriter::prepareYproxyConnection() {
  // open unix data socket

  client_fd_ = socket(AF_UNIX, SOCK_STREAM, 0);
  if (client_fd_ == -1) {
    elog(WARNING, "failed to create unix socket, errno: %m");
    // throw here?
    return -1;
  }

  struct sockaddr_un addr;
  /* Bind socket to socket name. */

  memset(&addr, 0, sizeof(addr));

  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, adv_->yproxy_socket.c_str(),
          sizeof(addr.sun_path) - 1);

  auto ret =
      ::connect(client_fd_, (const struct sockaddr *)&addr, sizeof(addr));

  if (ret == -1) {
    elog(WARNING, "failed to acquire connection to unix socket on %s, errno: %m", adv_->yproxy_socket.c_str());
    return -1;
  }

  auto msg = ConstructPutRequest(storage_path_);

  if (write_full(msg) == -1) {
    return -1;
  }

  return 0;
}

std::vector<char> YProxyWriter::ConstructPutRequest(std::string fileName) {
  std::vector<char> buff(
      MSG_HEADER_SIZE + PROTO_HEADER_SIZE + fileName.size() + 1, 0);
  buff[8] = MessageTypePut;
  if (adv_->use_gpg_crypto) {
    buff[9] = EncryptRequest;
  } else {
    buff[9] = NoEncryptRequest;
  }
  uint64_t len = buff.size();

  strncpy(buff.data() + MSG_HEADER_SIZE + PROTO_HEADER_SIZE, fileName.c_str(),
          fileName.size());

  uint64_t cp = len;
  for (ssize_t i = 7; i >= 0; --i) {
    buff[i] = cp & ((1 << 8) - 1);
    cp >>= 8;
  }

  return buff;
}

std::vector<char> YProxyWriter::ConstructCopyDataRequest(const char *buffer,
                                                         size_t amount) {
  std::vector<char> buff(MSG_HEADER_SIZE + PROTO_HEADER_SIZE + 8 + amount, 0);
  buff[8] = MessageTypeCopyData;
  uint64_t len = buff.size();

  memcpy(buff.data() + 20, buffer, amount);

  uint64_t cp = amount;
  for (ssize_t i = 19; i >= 12; --i) {
    buff[i] = cp & ((1 << 8) - 1);
    cp >>= 8;
  }

  cp = len;
  for (ssize_t i = 7; i >= 0; --i) {
    buff[i] = cp & ((1 << 8) - 1);
    cp >>= 8;
  }

  return buff;
}

std::vector<char> YProxyWriter::CostructCommandCompleteRequest() {
  std::vector<char> buff(MSG_HEADER_SIZE + PROTO_HEADER_SIZE, 0);
  buff[8] = MessageTypeCommandComplete;
  uint64_t len = MSG_HEADER_SIZE + PROTO_HEADER_SIZE;

  uint64_t cp = len;
  for (ssize_t i = 7; i >= 0; --i) {
    buff[i] = cp & ((1 << 8) - 1);
    cp >>= 8;
  }

  return buff;
}

int YProxyWriter::readRFQResponce() {
  int len = MSG_HEADER_SIZE;
  char buffer[len];
  // try to read small number of bytes in one op
  // if failed, give up
  int rc = ::read(client_fd_, buffer, len);
  if (rc != len) {
    // handle
    return -1;
  }

  uint64_t msgLen = 0;
  for (int i = 0; i < 8; i++) {
    msgLen <<= 8;
    msgLen += uint8_t(buffer[i]);
  }

  if (msgLen != MSG_HEADER_SIZE + PROTO_HEADER_SIZE) {
    // protocol violation
    return 1;
  }

  // substract header
  msgLen -= len;

  char data[msgLen];
  rc = ::read(client_fd_, data, msgLen);

  if (rc != msgLen) {
    // handle
    return -1;
  }

  if (data[0] != MessageTypeReadyForQuery) {
    return 2;
  }
  return 0;
}

YProxyLister::YProxyLister(std::shared_ptr<IOadv> adv, ssize_t segindx)
    : adv_(adv), segindx_(segindx) { }


YProxyLister::~YProxyLister() { close(); }

bool YProxyLister::close() {
  if (client_fd_ != -1) {
    ::close(client_fd_);
    client_fd_ = -1;
  }
  return true;
}

int YProxyLister::prepareYproxyConnection() {
  // open unix data socket

  client_fd_ = socket(AF_UNIX, SOCK_STREAM, 0);
  if (client_fd_ == -1) {
    // throw here?
    elog(WARNING, "failed to create unix socket, errno: %m");
    return -1;
  }

  struct sockaddr_un addr;

  /* Bind socket to socket name. */
  memset(&addr, 0, sizeof(addr));

  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, adv_->yproxy_socket.c_str(),
          sizeof(addr.sun_path) - 1);

  auto ret =
      ::connect(client_fd_, (const struct sockaddr *)&addr, sizeof(addr));

  if (ret == -1) {
    elog(WARNING, "failed to acquire connection to unix socket on %s, errno: %m", adv_->yproxy_socket.c_str());
    return -1;
  }

  return 0;
}

std::vector<storageChunkMeta> YProxyLister::list_relation_chunks() {
  std::vector<storageChunkMeta> res;
  auto ret = prepareYproxyConnection();
  if (ret != 0) {
    // throw?
    return res;
  }
  
  auto msg = ConstructListRequest(yezzey_block_file_path(adv_->nspname, adv_->relname,
                                         adv_->coords_, segindx_));
  size_t rc = ::write(client_fd_, msg.data(), msg.size());
  if (rc <= 0) {
    // throw?
    return res;
  }

  std::vector<storageChunkMeta> meta;
  while(true) {
    auto message = readMessage();
    switch (message.type)
    {
    case MessageTypeObjectMeta:
      meta = readObjectMetaBody(&message.content);
      res.insert(res.end(), meta.begin(), meta.end());
      break;
    case MessageTypeReadyForQuery:
      return res;
    
    default:
      //throw?
      return res;
    }
  }  
}

std::vector<std::string> YProxyLister::list_chunk_names() {
  auto chunk_meta = list_relation_chunks();
  std::vector<std::string> res(chunk_meta.size());

  for (int i = 0; i < chunk_meta.size(); i++) {
    res[i] = chunk_meta[i].chunkName;
  }
  return res;
}

std::vector<char> YProxyLister::ConstructListRequest(std::string fileName) {
  std::vector<char> buff(
      MSG_HEADER_SIZE + PROTO_HEADER_SIZE + fileName.size() + 1, 0);
  buff[8] = MessageTypeList;
  uint64_t len = buff.size();

  strncpy(buff.data() + MSG_HEADER_SIZE + PROTO_HEADER_SIZE, fileName.c_str(),
          fileName.size());

  uint64_t cp = len;
  for (ssize_t i = 7; i >= 0; --i) {
    buff[i] = cp & ((1 << 8) - 1);
    cp >>= 8;
  }

  return buff;
}

YProxyLister::message YProxyLister::readMessage() {
  YProxyLister::message res;
  int len = MSG_HEADER_SIZE;
  char buffer[len];
  // try to read small number of bytes in one op
  // if failed, give up
  int rc = ::read(client_fd_, buffer, len);
  if (rc != len) {
    // handle
    res.retCode = -1;
    return res;
  }

  uint64_t msgLen = 0;
  for (int i = 0; i < 8; i++) {
    msgLen <<= 8;
    msgLen += uint8_t(buffer[i]);
  }

  // substract header
  msgLen -= len;

  char data[msgLen];
  rc = ::read(client_fd_, data, msgLen);

  if (rc != msgLen) {
    // handle
    res.retCode = -1;
    return res;
  }

  res.type = data[0];
  res.content = std::vector<char>(data, data + msgLen);
  return res;
}

std::vector<storageChunkMeta> YProxyLister::readObjectMetaBody(std::vector<char> *body) {
  std::vector<storageChunkMeta> res;
  int i = PROTO_HEADER_SIZE;
  while (i < body->size())
  {
    std::vector<char> buff;
    while (body->at(i) != 0 && i < body->size()) {
      buff.push_back(body->at(i));
      i++;
    }
    i++;
    std::string path(buff.begin(), buff.end());
    if (body->size() - i < 8) {
      //throw?
      return res;
    }
    int64_t size = 0;
    for (int j = i; j < i + 8; j++) {
      size <<= 8;
      size += uint8_t(body->at(j));
    }
    i += 8;

    storageChunkMeta meta;
    meta.chunkName = path;
    meta.chunkSize = size;
    res.push_back(meta);
  }
  
  return res;
}
