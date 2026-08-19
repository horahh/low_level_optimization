#pragma once

#include <atomic>
#include <cstddef>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

namespace low_level_optimization {

template <typename T>
class CircularBuffer {
  public:
    explicit CircularBuffer(std::size_t capacity)
        : storage_(capacity + 1U), head_(0U), tail_(0U) {}

    std::size_t capacity() const { return storage_.size() - 1U; }

    std::size_t len() const {
        if (head_ >= tail_) {
            return head_ - tail_;
        }

        return storage_.size() - (tail_ - head_);
    }

    bool empty() const { return head_ == tail_; }

    bool full() const { return increment(head_) == tail_; }

    bool push(T value) {
        if (full()) {
            return false;
        }

        storage_[head_] = std::move(value);
        head_ = increment(head_);
        return true;
    }

    std::optional<T> pop() {
        if (empty()) {
            return std::nullopt;
        }

        auto value = std::move(storage_[tail_]);
        storage_[tail_].reset();
        tail_ = increment(tail_);
        return value;
    }

  private:
    std::size_t increment(std::size_t index) const {
        return (index + 1U) % storage_.size();
    }

    std::vector<std::optional<T>> storage_;
    std::size_t head_;
    std::size_t tail_;
};

template <typename T>
class MutexCircularBuffer {
  public:
    explicit MutexCircularBuffer(std::size_t capacity) : inner_(capacity) {}

    bool push(T value) {
        std::lock_guard<std::mutex> guard(mutex_);
        return inner_.push(std::move(value));
    }

    std::optional<T> pop() {
        std::lock_guard<std::mutex> guard(mutex_);
        return inner_.pop();
    }

  private:
    CircularBuffer<T> inner_;
    mutable std::mutex mutex_;
};

struct AtomicOrderings {
    std::memory_order producer_tail_load{std::memory_order_acquire};
    std::memory_order producer_head_store{std::memory_order_release};
    std::memory_order consumer_head_load{std::memory_order_acquire};
    std::memory_order consumer_tail_store{std::memory_order_release};
};

template <typename T>
class AtomicSpscCircularBuffer {
  public:
    explicit AtomicSpscCircularBuffer(
        std::size_t capacity,
        AtomicOrderings orderings = {}
    )
        : storage_(capacity + 1U),
          head_(0U),
          tail_(0U),
          orderings_(orderings) {}

    bool push(const T &value) {
        const auto head = head_.load(std::memory_order_relaxed);
        const auto next = increment(head);
        if (next == tail_.load(orderings_.producer_tail_load)) {
            return false;
        }

        storage_[head] = value;
        head_.store(next, orderings_.producer_head_store);
        return true;
    }

    std::optional<T> pop() {
        const auto tail = tail_.load(std::memory_order_relaxed);
        if (tail == head_.load(orderings_.consumer_head_load)) {
            return std::nullopt;
        }

        auto value = storage_[tail];
        tail_.store(increment(tail), orderings_.consumer_tail_store);
        return value;
    }

  private:
    std::size_t increment(std::size_t index) const {
        return (index + 1U) % storage_.size();
    }

    std::vector<T> storage_;
    std::atomic<std::size_t> head_;
    std::atomic<std::size_t> tail_;
    AtomicOrderings orderings_;
};

}  // namespace low_level_optimization
