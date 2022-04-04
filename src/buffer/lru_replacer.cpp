//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : capacity_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lk(latch_);
  if (lru_map_.empty()) {
    *frame_id = INVALID_PAGE_ID;
    return false;
  }
  *frame_id = lru_list_.back();
  lru_list_.pop_back();
  lru_map_.erase(*frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lk(latch_);
  if (lru_map_.find(frame_id) != lru_map_.end()) {
    lru_list_.erase(lru_map_[frame_id]);
    lru_map_.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lk(latch_);
  if (lru_map_.find(frame_id) == lru_map_.end()) {
    if (Size() >= capacity_) {
      lru_map_.erase(lru_list_.back());
      lru_list_.pop_back();
    }
    lru_list_.push_front(frame_id);
    lru_map_[frame_id] = lru_list_.begin();
  }
}

size_t LRUReplacer::Size() { return lru_list_.size(); }

}  // namespace bustub
