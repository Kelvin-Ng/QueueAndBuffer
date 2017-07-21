/*
 * SPSCQueue. An infinite-size queue which allows thread-safe manipulations in a single-producer single-consumer setting.
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

#include <utility>
#include <atomic>
#include <memory>
#include <condition_variable>

/*
 * A queue for single-consumer and single-producer setting.
 * mode:
 *     - 0: wait-free
 *     - 1: wait by spinning
 *     - 2: wait by condition variable
 */

// Some guarantees:
//  1. Thread-safe with single-consumer and single-producer
//  2. There is always one node
//  3. Empty when there is only one node
//  4. head_->next is the front
//  5. tail_ is the back
//  6. Elements are never moved
//  7. head_ is read or written only by the consumer
//  8. tail_ is written only by the producer, but is read by both the producer and consumer
//  9. free_head_ is read or written only by the producer
// 10. free_tail_ is written only by the consumer, but is read by both the producer and consumer

// TODO: can I not to create mtx_ and cv_ when is_blocking == false?
template <typename T, int mode>
class SPSCQueueBase {
 public:
    // Not thread-safe
    SPSCQueueBase() {
        head_ = new Node();
        tail_ = head_;
        free_head_ = new Node();
        free_tail_ = free_head_;
    }

    SPSCQueueBase(SPSCQueueBase&&) = default;

    // Not thread-safe
    ~SPSCQueueBase() {
        while (head_ != nullptr) {
            Node* tmp = head_->next;
            delete head_;
            head_ = tmp;
        }
        while (free_head_ != nullptr) {
            Node* tmp = free_head_->next;
            delete free_head_;
            free_head_ = tmp;
        }
    }

    // Thread-safe for only one producer
    void push(const T& obj) {
        if (free_list_empty()) {
            tail_->next = new Node(nullptr, obj);
        } else {
            tail_->next = free_head_;
            free_head_ = free_head_->next;
            new (tail_->next) Node(nullptr, obj);
        }

        if (mode == 2) {
            std::unique_lock<std::mutex> lk(mtx_);
            // Atomic is still needed because empty() does not acquire the lock
            __atomic_store(&tail_, &tail_->next, __ATOMIC_RELEASE);
            lk.unlock();
            cv_.notify_one();
        } else {
            __atomic_store(&tail_, &tail_->next, __ATOMIC_RELEASE);
        }
    }

    // Thread-safe for only one producer
    void push(T&& obj) {
        if (free_list_empty()) {
            tail_->next = new Node(nullptr, std::move(obj));
        } else {
            tail_->next = free_head_;
            __atomic_store(&free_head_, &free_head_->next, __ATOMIC_RELEASE);
            new (tail_->next) Node(nullptr, std::move(obj));
        }

        if (mode == 2) {
            std::unique_lock<std::mutex> lk(mtx_);
            // Atomic is still needed because empty() does not acquire the lock
            __atomic_store(&tail_, &tail_->next, __ATOMIC_RELEASE);
            lk.unlock();
            cv_.notify_one();
        } else {
            __atomic_store(&tail_, &tail_->next, __ATOMIC_RELEASE);
        }
    }

    // Thread-safe for only one producer
    template <typename... Args>
    void emplace(Args&&... args) {
        if (free_list_empty()) {
            tail_->next = new Node(nullptr, std::forward<Args>(args)...);
        } else {
            tail_->next = free_head_;
            free_head_ = free_head_->next;
            new (tail_->next) Node(nullptr, std::forward<Args>(args)...);
        }

        if (mode == 2) {
            std::unique_lock<std::mutex> lk(mtx_);
            // Atomic is still needed because empty() does not acquire the lock
            __atomic_store(&tail_, &tail_->next, __ATOMIC_RELEASE);
            lk.unlock();
            cv_.notify_one();
        } else {
            __atomic_store(&tail_, &tail_->next, __ATOMIC_RELEASE);
        }
    }

    // Thread-safe for only one consumer
    void pop() {
        if (mode == 2 && empty()) {
            std::unique_lock<std::mutex> lk(mtx_);
            cv_.wait(lk, [&]{return !empty();});
        } else if (mode == 1) {
            while (empty());
        }

        free_tail_->next = head_;
        head_ = head_->next;
        free_tail_->next->obj.~T();
        free_tail_->next->next = nullptr;
        __atomic_store(&free_tail_, &free_tail_->next, __ATOMIC_RELEASE);
    }

    // Thread-safe for only one consumer
    inline T& front() {
        if (mode == 2 && empty()) {
            std::unique_lock<std::mutex> lk(mtx_);
            cv_.wait(lk, [&]{return !empty();});
        } else if (mode == 1) {
            while (empty());
        }

        return head_->next->obj;
    }

    // Thread-safe for only one producer
    inline T& back() {
        return tail_->obj;
    }

    // Thread-safe for only one consumer
    // User of mode == 2 should be careful. It does NOT block.
    inline const T& front() const {
        if (mode == 1) {
            while (empty());
        }
        return head_->next->obj;
    }

    // Thread-safe for only one producer
    // TODO: it seems that this method does not make sense...
    inline const T& back() const {
        return tail_->obj;
    }

    // Thread-safe for only one consumer
    inline bool empty() const {
        return head_ == __atomic_load_n(&tail_, __ATOMIC_ACQUIRE);
    }

 private:
    class Node {
     public:
        Node() = default;
        Node(Node* next, const T& obj) : obj(obj), next(next) {}
        Node(Node* next, T&& obj) : obj(std::move(obj)), next(next) {}
        template <typename... Args>
        Node(Node* next, Args&&... args) : obj(std::forward<Args>(args)...), next(next) {}

        T obj;
        Node* next;
    };

    // Thread-safe for only one producer
    inline bool free_list_empty() const {
        return free_head_ == __atomic_load_n(&free_tail_, __ATOMIC_ACQUIRE);
    }
    
    Node* head_;
    Node* tail_;

    Node* free_head_;
    Node* free_tail_;

    std::mutex mtx_;
    std::condition_variable cv_;
};

template <typename T>
using SPSCQueue = SPSCQueueBase<T, 0>;

template <typename T>
using SPSCQueueSpin = SPSCQueueBase<T, 1>;

template <typename T>
using SPSCQueueCV = SPSCQueueBase<T, 2>;

