#include "meta.h"
#include "url.h"
#include "util.h"
#include "gucs.h"
#include "yproxy.h"
#include <sys/socket.h>
#include <iostream>

#include <unistd.h>

YProxyReader::YProxyReader(std::shared_ptr<IOadv> adv, ssize_t segindx,
                           const std::vector<ChunkInfo> order)
    : adv_(adv), segindx_(segindx), order_ptr_(0), order_(order), current_chunk_remaining_bytes_(0), client_fd_(-1) {
}

YProxyReader::~YProxyReader() { close(); }

bool YProxyReader::close() {
    if (client_fd_ != -1) {
        ::close(client_fd_);
        client_fd_ = -1;
    }
    return true;
}

const char DecrpytRequest = 1;
const char EncryptRequest = 1;
const char MessageTypeCat = 42;
const char MessageTypePut = 43;
const char MessageTypeCommandComplete = 44;
const char MessageTypeReadyForQuery = 45;
const char MessageTypeCopyData = 46;

const size_t MSG_HEADER_SIZE = 8;
const size_t PROTO_HEADER_SIZE = 4;


std::vector<char> YProxyReader::ConstructCatRequest(const ChunkInfo & ci) {
    std::vector<char> buff(MSG_HEADER_SIZE + PROTO_HEADER_SIZE + ci.x_path.size() + 1, 0);
    buff[8] = MessageTypeCat;
    buff[9] = DecrpytRequest;
    uint64_t len = buff.size();

    strncpy(buff.data() + MSG_HEADER_SIZE + PROTO_HEADER_SIZE, ci.x_path.c_str(), ci.x_path.size());

    uint64_t cp = len;
    for (ssize_t i = 7; i >= 0; --i) {
        buff[i] = cp & ((1 << 8) - 1);
        cp >>= 8;
    }

    return buff;
}

int YProxyReader::prepareYproxyConnection(const ChunkInfo & ci) {
    // open unix data socket
    
    client_fd_ = socket(AF_UNIX, SOCK_STREAM, 0);
    if (client_fd_ == -1) {
        // throw here?
        return -1;
    }

    struct sockaddr_un addr;
    /* Bind socket to socket name. */

    memset(&addr, 0, sizeof(addr));

    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, adv_->yproxy_socket.c_str(), sizeof(addr.sun_path) - 1);

    auto ret = ::connect(client_fd_, (const struct sockaddr *) &addr,
                          sizeof(addr));
    if (ret == -1) {
        // THROW here?
        return -1;
    }

    auto msg = ConstructCatRequest(ci);

    size_t rc = ::write(client_fd_, msg.data(), msg.size());

    if (rc <= 0) {
        // handle
        return -1;
    }

    // now we are ready to read our request data
    return client_fd_;
}

bool YProxyReader::read(char *buffer, size_t *amount) {
    if (current_chunk_remaining_bytes_ <= 0) {

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
        this->prepareYproxyConnection(order_[order_ptr_]);
        current_chunk_remaining_bytes_ = order_[order_ptr_++].size;
    }

    // preparing done, read data

    auto rc = ::read(client_fd_, buffer, *amount);
    if (rc <= 0) {
        // error 
        *amount = rc;
        return false;
    }
    // what if rc > current_chunk_remaining_bytes_ ? 
    current_chunk_remaining_bytes_ -= rc;
    *amount = rc;

    return true;
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

YProxyWriter::~YProxyWriter() { 
    close();
}

// complete external storage interaction. 
// TBD: smgr_FileSync() here ? 

int YProxyWriter::write_full(const std::vector<char> &msg) {
    int len = msg.size();
    int sync_offset = 0;
    while (len > 0)
    {
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
        // throw here?
        return -1;
    }

    struct sockaddr_un addr;
    /* Bind socket to socket name. */

    memset(&addr, 0, sizeof(addr));

    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, adv_->yproxy_socket.c_str(), sizeof(addr.sun_path) - 1);

    auto ret = ::connect(client_fd_, (const struct sockaddr *) &addr,
                          sizeof(addr));

    if (ret == -1) {
        return -1;
    }

    auto msg = ConstructPutRequest(storage_path_);

    if (write_full(msg) == -1) {
        return -1;
    }

    return 0;
}

std::vector<char> YProxyWriter::ConstructPutRequest(std::string fileName) {
    std::vector<char> buff(MSG_HEADER_SIZE + PROTO_HEADER_SIZE + fileName.size() + 1, 0);
    buff[8] = MessageTypePut;
    buff[9] = EncryptRequest;
    uint64_t len = buff.size();

    strncpy(buff.data() + MSG_HEADER_SIZE + PROTO_HEADER_SIZE, fileName.c_str(), fileName.size());

    uint64_t cp = len;
    for (ssize_t i = 7; i >= 0; --i) {
        buff[i] = cp & ((1 << 8) - 1);
        cp >>= 8;
    }

    return buff;
}

std::vector<char> YProxyWriter::ConstructCopyDataRequest(const char *buffer, size_t amount) {
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
        msgLen += buffer[i];
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
