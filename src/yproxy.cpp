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
const char MessageTypeCat = 42;

std::vector<char> YProxyReader::ConstructCatRequest(const ChunkInfo & ci) {
    std::vector<char> buff(8 + 4 + ci.x_path.size() + 1, 0);
    buff[8] = MessageTypeCat;
    buff[9] = DecrpytRequest;
    uint64_t len = buff.size();

    strncpy(buff.data() + 12, ci.x_path.c_str(), ci.x_path.size());

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