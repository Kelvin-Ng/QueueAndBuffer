/*
 * Buffer. A ring buffer.
 * Copyright (C) 2017  Kelvin Ng
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <unistd.h>

#include <vector>
#include <string>
#include <cassert>
#include <atomic>

/*
 * A continuous linear buffer. Not a ring buffer. Designed for used in networking.
 * Thread safety:
 *     If atomic<size_t> is used as PosT: Functions that change the reserved size are not thread-safe. Other functions are thread-safe when there is one writer and one reader.
 *     If size_t is used as PosT: Not thread-safe
 */
// Warning: not actively maintained. TODO: Subject to reimplementation.
// TODO: Refine the atomic operations
template <typename PosT>
class Buffer {
 public:
    Buffer() : wpos_(0), rpos_(0) {}
    explicit Buffer(size_t init_reserve) : buf_(init_reserve), wpos_(0), rpos_(0) {}

    inline void reserve(size_t len) {
        buf_.resize(len);
    }

    inline void enlarge(size_t len) {
        buf_.resize(buf_.size() + len);
    }

    inline void reset(int len) {
        reserve(len);
        wpos_ = 0;
        rpos_ = 0;
    }

    void write(const std::string& str) {
        size_t size = str.size();
        write(size);
        std::copy(str.begin(), str.end(), buf_.begin() + wpos_);
        wpos_ += str.size();
    }

    template <typename T>
    void write(const T& ptr) {
        std::copy((char*)&ptr, ((char*)&ptr) + sizeof(T), buf_.begin() + wpos_);
        wpos_ += sizeof(T);
    }

    template <typename T>
    const T* read() {
        const T* res = (const T*)&(buf_[rpos_]);
        rpos_ += sizeof(T);
        return res;
    }

    std::string get_string() {
        const size_t* len = read<size_t>();
        std::string str((const char*)&(buf_[rpos_]), *len);
        rpos_ += *len;
        return str;
    }

    inline const void* get_rptr(size_t pos) const {
        return &buf_[pos];
    }

    inline void* get_rptr(size_t pos) {
        return &buf_[pos];
    }

    inline void* get_wptr(size_t pos) {
        return &buf_[pos];
    }

    inline const void* get_rptr() const {
        return &buf_[rpos_];
    }

    inline void* get_rptr() {
        return &buf_[rpos_];
    }

    inline void* get_wptr() {
        return &buf_[wpos_];
    }

    inline void inc_wpos(size_t inc) {
        wpos_ += inc;
    }

    inline void inc_rpos(size_t inc) {
        rpos_ += inc;
    }

    inline size_t get_wpos() const {
        return wpos_;
    }

    inline size_t get_rpos() const {
        return rpos_;
    }

    inline size_t size() const {
        return wpos_;
    }

    inline size_t capacity() const {
        return buf_.size();
    }

    inline size_t remaining() const {
        return wpos_ - rpos_;
    }

    inline bool empty() const {
        return wpos_ == rpos_;
    }

    inline ssize_t input_from_fd(int fd) {
        ssize_t len = ::read(fd, get_wptr(), capacity() - wpos_);
        if (len > 0) {
            wpos_ += len;
        }
        return len;
    }

    inline ssize_t output_to_fd(int fd) {
        ssize_t len = ::write(fd, get_rptr(), size() - rpos_);
        if (len > 0) {
            rpos_ += len;
        }
        assert(rpos_ <= size());
        return len;
    }

 private:
    std::vector<char> buf_;
    PosT wpos_, rpos_;
};

