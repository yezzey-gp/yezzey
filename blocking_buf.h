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
    std::condition_variable cv_read_;
    std::condition_variable cv_write_;
    // std::condition_variable cv_full_;

    const size_t default_sz_ = 1 << 12;

    bool closed_;

public:
    // // Non-copyable
    BlockingBuffer(const BlockingBuffer& buf) {
        sz_ = buf.sz_;
        buffer_ = new char[sz_];
    };

    BlockingBuffer& operator=(const BlockingBuffer& buf) {
        this->sz_ = buf.sz_;
        this->buffer_ = new char[sz_];
        return *this;
    }
    // BlockingBuffer& operator=(const BlockingBuffer&) = delete;

    BlockingBuffer(size_t sz) : sz_(sz), offset_(0), closed_(false) {
        buffer_ = (char*)malloc(sizeof(char) * sz_);
    }

    BlockingBuffer() : sz_(default_sz_), offset_(0), closed_(false) {
        buffer_ = (char*)malloc(sizeof(char) * sz_);
    }

    int read(char * dest, size_t sz) {
        std::unique_lock<std::mutex> lk(mu_);

        cv_read_.wait(lk, [&] { return offset_ || closed_; } );

        if (offset_) {
            size_t mpy_sz = std::min(sz, offset_);
            std::memcpy(dest, buffer_, mpy_sz);
            std::memmove(buffer_, buffer_ + mpy_sz, offset_ - mpy_sz);
            offset_ -= mpy_sz;

            // notify writer
            cv_write_.notify_one();
            return mpy_sz;
        }

        // else closed_
        return 0;
    }

    int write(const char * source, size_t sz) {
        std::unique_lock<std::mutex> lk(mu_);

        cv_write_.wait(lk, [&] { return sz_ - offset_ || closed_; } );

        if (closed_) {
            return 0;
        }

        size_t mpy_sz = std::min(sz, sz_ - offset_);
        std::memcpy(buffer_ + offset_, source, mpy_sz);
        offset_ += mpy_sz;

        cv_read_.notify_one();
        return mpy_sz;
    }

    // void waitdone() {
    //     std::unique_lock<std::mutex> lk(mu_);
    //     cv_full_.wait(lk, [&] { return offset_ == 0; } );
    // }

    void clear() {
        std::unique_lock<std::mutex> lk(mu_);

        offset_ = 0;
        cv_read_.notify_all();
        cv_write_.notify_all();
    }

    void close () {
        std::unique_lock<std::mutex> lk(mu_);

        closed_ = true;
        cv_read_.notify_all();
        cv_write_.notify_all();
    }

    ~BlockingBuffer() {
        delete []buffer_;
    }
};


#endif /* YEZZEY_BLOCKING_BUF_H */