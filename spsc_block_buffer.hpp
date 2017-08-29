/*
 * SPSCBlockBuffer. An infinite-size buffer which allows thread-safe manipulations in a single-producer single-consumer setting.
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

#include "spsc_queue.hpp"

#include <unistd.h>
#include <sys/eventfd.h>

#include <queue>
#include <memory>
#include <cassert>

#if __cpp_if_constexpr >= 201606
#define IF_CONSTEXPR if constexpr
#else
#define IF_CONSTEXPR if
#endif

// Some guarantees:
// 1. buf_ is never empty
// 2. &buf_.back().second == wpos
// 3. &buf_.front().second == wpos if there is exactly one block
// 4. Memory is never moved
// 5. Data is always valid until explicitly removed with clear_preserved()
// 6. rpos_ is read or written only by the consumer
// 7. wpos_ is written only by the producer, but is read by both the producer and consumer
// 8. *wpos_ is written only by the producer, but is read by both the producer and consumer
// 9. non_notified_size_ is read or written only by the producer
//10. one_block_left_ is read or written only by the consumer
//11. When one_block_left_ is false, there must be more than one block. No guarantee when one_block_left_ is true

template <int mode, unsigned notify_interval = 1, unsigned long long wait_timeout = 0, unsigned wait_spin_cv_num = 1>
class SPSCBlockBufferBase {
 public:
    SPSCBlockBufferBase() = default;

    SPSCBlockBufferBase(ssize_t block_size) {
        init(block_size);
    }

    SPSCBlockBufferBase(SPSCBlockBufferBase&&) = default;

    void init(ssize_t block_size = -1) {
        rpos_ = 0;
        wpos_private_ = 0;
        one_block_left_ = true;
        if (block_size == -1) {
            block_size_ = sysconf(_SC_PAGESIZE);
            //block_size_ = sysconf(_SC_PAGESIZE) * 100;
        } else {
            block_size_ = block_size;
        }
        buf_.emplace(new char[block_size_], 0);
        wpos_ = &buf_.back().second;
        if (mode == 5) {
            eventfd_ = eventfd(0, EFD_NONBLOCK);
        }
    }

    inline int get_eventfd() const {
        return eventfd_;
    }

    // write [write_start, write_end) to the buffer
    void write(const char* write_start, const char* write_end, bool notify = true) {
        //// TODO: Probably an optimization for branch prediction
        //if (write_start >= write_end) {
        //    return;
        //}

        while (write_start < write_end) {
            add_block_if_needed();

            size_t to_write = std::min((size_t)(write_end - write_start), block_size_ - wpos_private_);
            std::copy(write_start, write_start + to_write, buf_.back().first.get() + wpos_private_);
            write_start += to_write;
            wpos_private_ += to_write;
        }

        if (notify) {
            this->notify();
        }
    }

    template <typename T>
    inline void write(const T& ptr, bool notify = true) {
        write((const char*)&ptr, (const char*)(&ptr + 1), notify);
    }

    void write(const std::string& str, bool notify = true) {
        size_t size = str.size();
        write(size, false);
        write(str.c_str(), str.c_str() + size, notify);
    }

    template <typename T>
    const T* read() {
        pop_block_if_needed(sizeof(T));

        const T* res = (const T*)&(buf_.front().first[rpos_]);
        rpos_ += sizeof(T);
        return res;
    }

    template <typename T>
    T get() {
        T res = *read<T>();
        clear_preserved(sizeof(T));
        return res;
    }

    const void* read_cont(size_t len) {
        pop_block_if_needed(len);

        const void* res = &(buf_.front().first[rpos_]);
        rpos_ += len;
        return res;
    }

    void get_cont(char* dest, size_t len) {
        pop_block_if_needed(len);

        std::copy(&buf_.front().first[rpos_], &buf_.front().first[rpos_ + len], dest);
        rpos_ += len;

        clear_preserved(len);
    }

    std::string get_string() {
        const size_t* len = read<size_t>();

        pop_block_if_needed(*len);

        std::string str(buf_.front().first.get() + rpos_, *len);
        rpos_ += *len;

        clear_preserved(sizeof(size_t) + *len);

        return str;
    }

    // write [write_start, write_end) to the buffer
    void write_cont(const char* write_start, const char* write_end, bool notify = true) {
        if (write_start >= write_end) {
            return;
        }

        size_t to_write = write_end - write_start;
        add_block_if_needed(to_write);

        std::copy(write_start, write_end, buf_.back().first.get() + wpos_private_);

        wpos_private_ += to_write;

        if (notify) {
            this->notify();
        }
    }

    template <typename T>
    inline void write_cont(const T& ptr, bool notify = true) {
        write_cont((const char*)&ptr, (const char*)&ptr + sizeof(T), notify);
    }

    void write_cont(const std::string& str, bool notify = true) {
        size_t size = str.size();
        write_cont(size, false);
        write_cont(str.c_str(), str.c_str() + size, notify);
    }

    inline ssize_t input_from_fd(int fd, bool cont = false, ssize_t max_len = -1) {
        ssize_t total_len = 0;

        for (;;) {
            add_block_if_needed();

            ssize_t len;
            if (max_len == -1) {
                len = ::read(fd, buf_.back().first.get() + wpos_private_, block_size_ - wpos_private_);
            } else {
                len = ::read(fd, buf_.back().first.get() + wpos_private_, std::min((size_t)(max_len - total_len), block_size_ - wpos_private_));
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
            total_len += len;
            wpos_private_ += len;

            if (cont) {
                break;
            }
        }

        // FIXME: the if statement can cause problems in general
        if (total_len > 0) {
            this->notify();
        }

        return total_len;
    }

    // Non-blocking
    inline ssize_t output_to_fd(int fd) {
        ssize_t total_len = 0;

        for (;;) {
            pop_block_if_needed_and_available(1);

            ssize_t len;
            // TODO: should I check the writable length before calling `write()` for efficiency?
            if (one_block_left_) {
                len = ::write(fd, buf_.front().first.get() + rpos_, __atomic_load_n(&buf_.front().second, __ATOMIC_ACQUIRE) - rpos_);
            } else {
                len = ::write(fd, buf_.front().first.get() + rpos_, buf_.front().second - rpos_);
            }
            if (len < 0) {
#ifdef BLOCKING_SEND
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    perror("[WARN] output_to_fd()");
                }
#endif
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

            /*if (one_block_left_) {
                if (!check_one_block_left() && buf_.front().second == rpos_) {
                    // everything in the current block is written to the fd, because there is more than one block
                    pop_block();
                }
                // TODO: should I break otherwise? buf_.front().second > rpos_ is quite unlikely because it means that someone writes something between ::write() and here. So, it is more likely that there is only one block that is completely read and we cannot proceed
            } else if (buf_.front().second == rpos_) {
                // everything in the current block is written to the fd, because there is more than one block
                pop_block();
            }*/
        }

        clear_preserved(total_len);

        return total_len;
    }

    void notify() {
        IF_CONSTEXPR(mode == 2 || mode == 3) {
            std::unique_lock<std::mutex> lk(mtx_);
            // Atomic is still needed because empty() does not acquire the lock
            __atomic_store_n(wpos_, wpos_private_, __ATOMIC_RELEASE);
            lk.unlock();
            cv_.notify_one();
        } else IF_CONSTEXPR(mode == 4) {
            ++notify_counter;
            if (notify_counter == notify_interval) {
                notify_counter = 0;
                std::unique_lock<std::mutex> lk(mtx_);
                // Atomic is still needed because empty() does not acquire the lock
                __atomic_store_n(wpos_, wpos_private_, __ATOMIC_RELEASE);
                lk.unlock();
                cv_.notify_one();
            } else {
                __atomic_store_n(wpos_, wpos_private_, __ATOMIC_RELEASE);
            }
        } else IF_CONSTEXPR(mode == 5) {
            __atomic_store_n(wpos_, wpos_private_, __ATOMIC_RELEASE);
            uint64_t tmp = 1;
            ::write(eventfd_, &tmp, sizeof(tmp));
        } else {
            __atomic_store_n(wpos_, wpos_private_, __ATOMIC_RELEASE);
        }
    }

    inline char* ensure_cont(size_t size) {
        add_block_if_needed(size);
        return buf_.back().first.get() + wpos_private_;
    }

    // For consumer only
    inline bool empty() const {
        return one_block_left_ && check_one_block_left() && rpos_ == __atomic_load_n(wpos_, __ATOMIC_ACQUIRE);
    }

    // For consumer only
    inline bool empty() {
        return one_block_left_ && (one_block_left_ = check_one_block_left()) && rpos_ == __atomic_load_n(wpos_, __ATOMIC_ACQUIRE);
    }

    inline void clear_preserved(size_t len) {
        size_t cleared_len = 0;
        for (;;) {
            if (!preserved_list_.empty() && preserved_list_.front().second + cleared_len <= len) {
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
        __atomic_store_n(wpos_, wpos_private_, __ATOMIC_RELEASE);
        wpos_private_ = 0;

        if (free_list_.empty()) {
            buf_.emplace(new char[block_size_], 0);
        } else {
            buf_.emplace(std::move(free_list_.front()), 0);
            free_list_.pop();
        }
        __atomic_store_n(&wpos_, &buf_.back().second, __ATOMIC_RELEASE);
    }

    inline void add_block_if_needed() {
        if (wpos_private_ == block_size_) {
            add_block();
        }
    }

    inline void add_block_if_needed(size_t cont_write_len) {
        if (cont_write_len > block_size_ - wpos_private_) {
            add_block();
        }
    }

    inline bool check_one_block_left() const {
        return (&buf_.front().second == __atomic_load_n(&wpos_, __ATOMIC_ACQUIRE));
    }

    void pop_block() {
        preserved_list_.push(std::move(buf_.front()));
        buf_.pop();
        rpos_ = 0;
        one_block_left_ = check_one_block_left();
    }

    // @return: true when the required size is available
    inline void pop_block_if_needed_and_available(size_t size) {
        if (one_block_left_) {
            if (!check_one_block_left() && buf_.front().second - rpos_ < size) {
                pop_block();
            }
        } else if (buf_.front().second - rpos_ < size) {
            pop_block();
        }
    }

    inline void pop_block_if_needed(size_t size) {
        IF_CONSTEXPR(mode == 0) {
            if (one_block_left_) {
                if (!check_one_block_left() && buf_.front().second - rpos_ < size) {
                    pop_block();
                }
            } else if (buf_.front().second - rpos_ < size) {
                pop_block();
            }
        /*} else if (mode == 1) {
            if (one_block_left_) {
                while (check_one_block_left() && __atomic_load_n(&buf_.front().second, __ATOMIC_ACQUIRE) - rpos_ < size);
                if (__atomic_load_n(&buf_.front().second, __ATOMIC_ACQUIRE) - rpos_ < size) { // there is more than one block if it is true
                    pop_block();
                    if (one_block_left_) {
                        while (__atomic_load_n(&buf_.front().second, __ATOMIC_ACQUIRE) < size); // rpos_ == 0
                    }
                    // There is always enough length to read given that
                    // 1. The user is reading correctly
                    // 2. There is more than one block left
                    // 3. The assumption that size <= block_size_ holds
                }
            } else {
                if (buf_.front().second - rpos_ < size) { // no need atomic because there is more than one block
                    pop_block();
                    if (one_block_left_) {
                        while (__atomic_load_n(&buf_.front().second, __ATOMIC_ACQUIRE) < size); // rpos_ == 0
                    }
                    // There is always enough length to read given that
                    // 1. The user is reading correctly
                    // 2. There is more than one block left
                    // 3. The assumption that size <= block_size_ holds
                }
            }
        } else if (mode == 2) {*/
        } else IF_CONSTEXPR(mode == 1 || mode == 2 || mode == 3 || mode == 4) {
            if (one_block_left_) {
                wait([&]{return !(check_one_block_left() && __atomic_load_n(&buf_.front().second, __ATOMIC_ACQUIRE) - rpos_ < size);});
                if (__atomic_load_n(&buf_.front().second, __ATOMIC_ACQUIRE) - rpos_ < size) {
                    pop_block();
                    if (one_block_left_) {
                        wait([&]{return (__atomic_load_n(&buf_.front().second, __ATOMIC_ACQUIRE) >= size);});
                    }
                }
            } else {
                if (buf_.front().second - rpos_ < size) { // no need atomic because there is more than one block
                    pop_block();
                    if (one_block_left_) {
                        wait([&]{return (__atomic_load_n(&buf_.front().second, __ATOMIC_ACQUIRE) >= size);});
                    }
                }
            }
        }
    }

    template <typename PredicateT>
    inline void wait(PredicateT pred) {
        IF_CONSTEXPR(mode == 1) {
            while (!pred()); // TODO: it seems that the NOT operation takes some time, but for now keep it for easy maintainence
        } else IF_CONSTEXPR(mode == 2) {
            if (!pred()) {
                std::unique_lock<std::mutex> lk(mtx_);
                cv_.wait(lk, pred);
            }
        } else IF_CONSTEXPR(mode == 3) {
            for (int i = 0; i < wait_spin_cv_num; ++i) {
                if (pred()) {
                    return;
                }
            }
            std::unique_lock<std::mutex> lk(mtx_);
            cv_.wait(lk, pred);
        } else IF_CONSTEXPR(mode == 4) {
            while (!pred()) {
                std::unique_lock<std::mutex> lk(mtx_);
                cv_.wait_for(lk, std::chrono::microseconds(wait_timeout), pred);
            }
        }
    }

    size_t block_size_;
    SPSCQueue<std::pair<std::unique_ptr<char[]>, size_t>> buf_;
    SPSCQueue<std::unique_ptr<char[]>> free_list_;
    SPSCQueue<std::pair<std::unique_ptr<char[]>, size_t>> preserved_list_;
    size_t rpos_;
    size_t* wpos_;
    size_t wpos_private_;

    bool one_block_left_;

    std::mutex mtx_;
    std::condition_variable cv_;
    int notify_counter = 0;

    int eventfd_;
};

using SPSCBlockBuffer = SPSCBlockBufferBase<0>;
using SPSCBlockBufferSpin = SPSCBlockBufferBase<1>;
using SPSCBlockBufferCV = SPSCBlockBufferBase<2>;
using SPSCBlockBufferEventFd = SPSCBlockBufferBase<5>;

template <unsigned wait_spin_cv_num>
using SPSCBlockBufferSpinCV = SPSCBlockBufferBase<3, 1, 0, wait_spin_cv_num>;

template <unsigned notify_interval, unsigned long long wait_timeout>
using SPSCBlockBufferCVTimeout = SPSCBlockBufferBase<4, notify_interval, wait_timeout>;
