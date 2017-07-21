/*
 * BlockBuffer. An infinite-size buffer.
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

#include <queue>
#include <memory>
#include <cassert>

// Warning: not actively maintained. TODO: Subject to reimplementation.
// Some guarantees:
// 1. buf_ is never empty
// 2. buf_.back().second == (size_t)-1
// 3. All elements in preserved list are with .second != (size_t)-1
// 4. Memory is never moved
// 5. Data is always valid until explicitly removed with clear_preserved()
class BlockBuffer {
 public:
    BlockBuffer() : rpos_(0), wpos_(0) {
        block_size_ = sysconf(_SC_PAGESIZE);
        buf_.emplace(new char[block_size_], -1);
    }

    BlockBuffer(ssize_t block_size) : rpos_(0), wpos_(0) {
        if (block_size == -1) {
            block_size_ = sysconf(_SC_PAGESIZE);
        } else {
            block_size_ = block_size;
        }
        buf_.emplace(new char[block_size_], -1);
    }

    // write [write_start, write_end) to the buffer
    void write(const char* write_start, const char* write_end) {
        //// TODO: Probably an optimization for branch prediction
        //if (write_start >= write_end) {
        //    return;
        //}
        while (write_start < write_end) {
            add_block_if_needed();

            size_t to_write = std::min((size_t)(write_end - write_start), block_size_ - wpos_);
            std::copy(write_start, write_start + to_write, buf_.back().first.get() + wpos_);
            write_start += to_write;
            wpos_ += to_write;
        }
    }

    template <typename T>
    inline void write(const T& ptr) {
        write((const char*)&ptr, (const char*)&ptr + sizeof(T));
    }

    void write(const std::string& str) {
        size_t size = str.size();
        write(size);
        write(str.c_str(), str.c_str() + size);
    }

    template <typename T>
    const T* read() {
        pop_block_if_needed(sizeof(T));

        const T* res = (const T*)&(buf_.front().first[rpos_]);
        rpos_ += sizeof(T);
        return res;
    }

    std::string get_string() {
        const size_t* len = read<size_t>();

        pop_block_if_needed(*len);

        std::string str(buf_.front().first.get() + rpos_, *len);
        rpos_ += *len;
        return str;
    }

    // write [write_start, write_end) to the buffer
    void write_cont(const char* write_start, const char* write_end) {
        if (write_start >= write_end) {
            return;
        }

        size_t to_write = write_end - write_start;
        assert(to_write <= block_size_);
        add_block_if_needed(to_write);

        std::copy(write_start, write_start + to_write, buf_.back().first.get() + wpos_);
        wpos_ += to_write;
    }

    template <typename T>
    inline void write_cont(const T& ptr) {
        write_cont((const char*)&ptr, (const char*)&ptr + sizeof(T));
    }

    void write_cont(const std::string& str) {
        size_t size = str.size();
        write_cont(size);
        write_cont(str.c_str(), str.c_str() + size);
    }

    inline ssize_t input_from_fd(int fd, bool cont = false) {
        ssize_t total_len = 0;

        for (;;) {
            add_block_if_needed();

            ssize_t len = ::read(fd, buf_.back().first.get() + wpos_, block_size_ - wpos_);
            if (len < 0) {
                if (total_len == 0) {
                    return len;
                } else {
                    break;
                }
            } else if (len == 0) {
                break;
            }
            wpos_ += len;
            total_len += len;

            if (cont) {
                break;
            }
        }

        return total_len;
    }

    inline ssize_t output_to_fd(int fd) {
        ssize_t total_len = 0;

        for (;;) {
            ssize_t len;
            if (buf_.front().second == (size_t)-1) {
                len = ::write(fd, buf_.front().first.get() + rpos_, wpos_ - rpos_);
            } else {
                len = ::write(fd, buf_.front().first.get() + rpos_, buf_.front().second - rpos_);
            }
            if (len < 0) {
                if (total_len == 0) {
                    return len;
                } else {
                    break;
                }
            } else if (len == 0) {
                break;
            }
            rpos_ += len;
            total_len += len;

            if (buf_.front().second != (size_t)-1 && rpos_ == buf_.front().second) {
                // TODO: May want to actually free the memory
                preserved_list_.push(std::move(buf_.front()));
                buf_.pop();
                rpos_ = 0;

                if (buf_.empty()) {
                    break;
                }
            }
        }

        return total_len;
    }

    inline char* ensure_cont(size_t size) {
        add_block_if_needed(size);
        return buf_.back().first.get() + wpos_;
    }

    inline bool empty() const {
        return (buf_.front().second == (size_t)-1 && rpos_ == wpos_);
    }

    inline void clear_preserved(size_t len) {
        size_t cleared_len = 0;
        for (;;) {
            if (preserved_list_.front().second + cleared_len <= len) {
                cleared_len += preserved_list_.front().second;
                free_list_.push(std::move(preserved_list_.front().first));
                preserved_list_.pop();
            } else {
                break;
            }
        }
    }

 private:
    void add_block() {
        buf_.back().second = wpos_;
        wpos_ = 0;
        if (free_list_.empty()) {
            buf_.emplace(new char[block_size_], -1);
        } else {
            buf_.emplace(std::move(free_list_.front()), -1);
            free_list_.pop();
        }
    }

    inline void add_block_if_needed() {
        if (wpos_ == block_size_) {
            add_block();
        }
    }

    inline void add_block_if_needed(size_t cont_write_len) {
        if (cont_write_len > block_size_ - wpos_) {
            add_block();
        }
    }

    void pop_block() {
        preserved_list_.push(std::move(buf_.front()));
        buf_.pop();
        rpos_ = 0;
    }

    inline void pop_block_if_needed(size_t size) {
        if (buf_.front().second != (size_t)-1 && buf_.front().second - rpos_ < size) {
            pop_block();
        }
    }

    size_t block_size_;
    std::queue<std::pair<std::unique_ptr<char[]>, size_t>> buf_;
    std::queue<std::unique_ptr<char[]>> free_list_;
    std::queue<std::pair<std::unique_ptr<char[]>, size_t>> preserved_list_;
    size_t rpos_;
    size_t wpos_;
};
