#ifndef YEZZEY_BLOCKING_BUF_H
#define YEZZEY_BLOCKING_BUF_H

#include <stdlib.h>

#include <cstring>
#include <mutex>
#include <condition_variable>

class BlockingBuffer {
    char * buffer_;
    size_t sz_;
    size_t offset_;

    std::mutex mu_;
    std::condition_variable cv_;

    const size_t default_sz_ = 1 << 12;

    bool closed_;

public:
    // Non-copyable
    BlockingBuffer(const BlockingBuffer&) = delete;
    BlockingBuffer& operator=(const BlockingBuffer&) = delete;



    BlockingBuffer(size_t sz) : sz_(sz), offset_(0), closed_(false) {
        buffer_ = (char*)malloc(sizeof(char) * sz_);
    }

    BlockingBuffer() : sz_(default_sz_), offset_(0), closed_(false) {
        buffer_ = (char*)malloc(sizeof(char) * sz_);
    }

    int read(char * dest, size_t sz) {
        std::unique_lock<std::mutex> lk(mu_);

        cv_.wait(lk, [&] { return sz_ - offset_ || closed_; } );

        if (closed_) {
            return 0;
        }

        size_t mpy_sz = std::min(sz, offset_);
        std::memcpy(dest, buffer_, mpy_sz);
        std::memmove(buffer_, buffer_ + offset_, mpy_sz);
        offset_ -= mpy_sz;
        // notify writer
        cv_.notify_one();
        return mpy_sz;
    }

    int write(const char * source, size_t sz) {
        std::unique_lock<std::mutex> lk(mu_);

        cv_.wait(lk, [&] { return sz_ - offset_ || closed_; } );

        if (closed_) {
            return 0;
        }

        size_t mpy_sz = std::min(sz, sz_ - offset_);
        std::memcpy(buffer_ + offset_, source, mpy_sz);
        offset_ += mpy_sz;
        return mpy_sz;
    }

    void clear() {
        std::unique_lock<std::mutex> lk(mu_);

        offset_ = 0;
        cv_.notify_one();
    }

    void close () {
        std::unique_lock<std::mutex> lk(mu_);

        closed_ = true;
        cv_.notify_all();
    }

    ~BlockingBuffer() {
        free(buffer_);
    }
};


#endif /* YEZZEY_BLOCKING_BUF_H */