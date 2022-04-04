//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
    pages_[i].page_id_ = INVALID_PAGE_ID;
    pages_[i].is_dirty_ = false;
    pages_[i].pin_count_ = 0;
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

// 找一个空闲的物理页frame_id 如果没有就到buffer pool 中牺牲一个页面
bool BufferPoolManagerInstance::FindPage(frame_id_t *frame_id) {
  // 1. 查看 free_list_，如果有空闲，则 Buffer Pool 没有满，从前面拿一个 frameId 返回。
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }
  // 2. Buffer Pool满了，则寻找是否有可以被替换的页，没有则返回 false。
  if (!replacer_->Victim(frame_id)) {
    return false;
  }
  // 3. 获得当前 frame_id对应的 page。
  Page *page = &pages_[*frame_id];
  if (page->is_dirty_) {
    // 4. 刷新到磁盘。
    disk_manager_->WritePage(page->page_id_, page->data_);
  }
  page_table_.erase(page->page_id_);
  return true;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> lk(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end() || page_id == INVALID_PAGE_ID) {
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[iter->second].data_);
  pages_[iter->second].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::lock_guard<std::mutex> lk(latch_);
  for (auto it : page_table_) {
    disk_manager_->WritePage(it.first, pages_[it.second].data_);
    pages_[it.second].is_dirty_ = false;
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> lk(latch_);

  // 挑选一个空闲的物理帧frame_id
  frame_id_t victim_frame_id = -1;
  if (!FindPage(&victim_frame_id)) {
    return nullptr;
  }
  // 分配一个新页号page_id
  *page_id = AllocatePage();
  // 更新 page 的元数据。
  Page *new_page = &pages_[victim_frame_id];
  new_page->page_id_ = *page_id;
  new_page->is_dirty_ = false;
  new_page->pin_count_++;
  memset(pages_[victim_frame_id].GetData(), 0, PAGE_SIZE);
  // 添加到 page table。
  page_table_[*page_id] = victim_frame_id;
  replacer_->Pin(victim_frame_id);
  // 创建的新页需要写回磁盘。
  disk_manager_->WritePage(*page_id, new_page->data_);
  return new_page;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> lk(latch_);
  auto page_iter = page_table_.find(page_id);
  if (page_iter != page_table_.end()) {  // 在缓存中
    frame_id_t frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_++;
    replacer_->Pin(frame_id);
    return &pages_[frame_id];
  }

  frame_id_t replace_fid;
  if (!FindPage(&replace_fid)) {
    return nullptr;
  }
  Page *replace_page = &pages_[replace_fid];  // 拿到页
  if (replace_page->IsDirty()) {
    disk_manager_->WritePage(replace_page->page_id_, replace_page->data_);
  }
  // 重新映射
  page_table_.erase(replace_page->page_id_);
  page_table_[page_id] = replace_fid;
  // 更新replace_page信息
  disk_manager_->ReadPage(page_id, replace_page->data_);
  replace_page->page_id_ = page_id;
  replace_page->pin_count_++;
  replace_page->is_dirty_ = false;
  replacer_->Pin(replace_fid);
  return replace_page;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> lk(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }
  if (page->IsDirty()) {
    disk_manager_->WritePage(page_id, page->data_);
  }
  DeallocatePage(page_id);
  // 更新metadata
  page_table_.erase(page_id);
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  free_list_.push_back(frame_id);
  return true;
}

// 多线程
bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lk(latch_);
  auto page_iter = page_table_.find(page_id);
  if (page_iter == page_table_.end()) {
    return false;
  }
  frame_id_t unpinned_fid = page_iter->second;
  Page *page = &pages_[unpinned_fid];
  if (is_dirty) {
    page->is_dirty_ = true;
  }
  if (page->pin_count_ == 0) {
    return false;
  }
  page->pin_count_--;
  if (page->GetPinCount() == 0) {
    replacer_->Unpin(unpinned_fid);
  }
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
