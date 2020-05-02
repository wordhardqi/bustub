//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  auto itr = page_table_.find(page_id);
  Page *target_page = nullptr;
  frame_id_t frame_id;
  if (itr != page_table_.end()) {
    frame_id = itr->second;  // frame_id is filled either by the free_list_ or by the replacer_.
    target_page = &pages_[frame_id];
  } else {
    if (free_list_.size() != 0) {
      frame_id = free_list_.back();
      free_list_.pop_back();
    } else {
      bool has_victim = replacer_->Victim(&frame_id);
      if (!has_victim) {
        return nullptr;
      }
      target_page = &pages_[frame_id];

      // flush victim_page as needed.
      if (target_page->IsDirty()) {
        disk_manager_->WritePage(target_page->GetPageId(), target_page->GetData());
      }
      // erase victim page.
      page_table_.erase(target_page->GetPageId());
    }

    target_page->ResetAll(page_id);
    page_table_.emplace(page_id, frame_id);
    disk_manager_->ReadPage(page_id, target_page->GetData());
  }

  target_page->pin_count_ += 1;
  replacer_->Pin(frame_id);
  return target_page;
}  // namespace bustub

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  auto itr = page_table_.find(page_id);
  if (itr == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = itr->second;

  replacer_->Unpin(frame_id);
  pages_[frame_id].pin_count_ -= 1;
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  auto itr = page_table_.find(page_id);
  if (itr == page_table_.end()) {
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[itr->second].GetData());
  pages_[itr->second].is_dirty_ = false;

  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  if (free_list_.size() == 0 && replacer_->Size() == 0) {
    return nullptr;
  }

  page_id_t allocated_page_id = disk_manager_->AllocatePage();

  frame_id_t new_frame_id;
  Page *target_page = nullptr;
  if (free_list_.size() != 0) {
    new_frame_id = free_list_.back();
    free_list_.pop_back();
    target_page = &pages_[new_frame_id];
  } else {
    bool has_victim = replacer_->Victim(&new_frame_id);
    if (!has_victim) {
      return nullptr;
    }

    target_page = &pages_[new_frame_id];

    // flush victim_page as needed.
    if (target_page->IsDirty()) {
      disk_manager_->WritePage(target_page->GetPageId(), target_page->GetData());
    }

    page_table_.erase(target_page->GetPageId());
  }

  target_page->ResetAll(allocated_page_id);
  *page_id = allocated_page_id;
  page_table_.emplace(allocated_page_id, new_frame_id);
  return target_page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  auto itr = page_table_.find(page_id);
  if (itr != page_table_.end()) {
    frame_id_t frame_id = itr->second;
    Page *current_page = &pages_[frame_id];
    if (current_page->GetPinCount() != 0) {
      return false;
    }

    current_page->ResetAll(INVALID_PAGE_ID);
    page_table_.erase(itr);

    free_list_.push_back(frame_id);
  }
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  for (auto itr = page_table_.begin(); itr != page_table_.end(); ++itr) {
    FlushPageImpl(itr->first);
  }
}

}  // namespace bustub
