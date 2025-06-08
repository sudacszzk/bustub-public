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
      next_page_id_(static_cast<page_id_t>(instance_index)),
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
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}



bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id)
{
    latch_.lock();
    if (page_table_.find(page_id)==page_table_.end() || page_id==INVALID_PAGE_ID)
    {
      latch_.unlock();
      return false;
    }
    disk_manager_->WritePage(page_id, pages_[page_table_.find(page_id)->second].data_);
    pages_[page_table_.find(page_id)->second].is_dirty_=false;
    latch_.unlock();
    return true; 
}



void BufferPoolManagerInstance::FlushAllPgsImp()
{
  for(size_t i=0;i<pool_size_;i++)
  {
    FlushPage(pages_[i].page_id_);
  }
}


Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) 
{ 
    latch_.lock();
    frame_id_t fra;
    if(!free_list_.empty())
    {
      fra=free_list_.front();
      free_list_.pop_front();
    }
    else 
    {
      bool is_s=replacer_->Victim(&fra);
      if (is_s==false)
      {
        latch_.unlock();
        return nullptr;
      }
      if (pages_[fra].is_dirty_==true)
      {
        disk_manager_->WritePage(pages_[fra].page_id_, pages_[fra].data_);
      }
      page_table_.erase(pages_[fra].page_id_);
    }
    page_id_t page_i=AllocatePage();
    *page_id = page_i;
    page_table_.insert(std::pair<page_id_t, frame_id_t>(page_i,fra));
    replacer_->Pin(fra);
    pages_[fra].ResetMemory();
    pages_[fra].page_id_ = page_i;
    pages_[fra].pin_count_ = 1;
    pages_[fra].is_dirty_ = false;
    latch_.unlock();
    return &pages_[fra]; 
}


Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id)
{
    latch_.lock();
    frame_id_t fra;
    if(page_table_.find(page_id)!=page_table_.end())
    {
      fra=page_table_.find(page_id)->second;
      pages_[fra].pin_count_ ++;
      replacer_->Pin(fra);
      latch_.unlock();
      return &pages_[fra];
    }
    if(!free_list_.empty())
    {
      fra=free_list_.front();
      free_list_.pop_front();
    }
    else 
    {
      bool is_s=replacer_->Victim(&fra);
      if (is_s==false)
      {
        latch_.unlock();
        return nullptr;
      }
      if (pages_[fra].is_dirty_==true)
      {
        disk_manager_->WritePage(pages_[fra].page_id_, pages_[fra].data_);
      }
      page_table_.erase(pages_[fra].page_id_);
    }
    page_table_.insert(std::pair<page_id_t, frame_id_t>(page_id,fra));
    pages_[fra].ResetMemory();
    pages_[fra].page_id_ = page_id;
    pages_[fra].pin_count_ = 1;
    pages_[fra].is_dirty_ = false;
    replacer_->Pin(fra);
    disk_manager_->ReadPage(page_id, pages_[fra].data_);
    latch_.unlock();
    return &pages_[fra];
}



bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) 
{
    latch_.lock();
    if (page_id==INVALID_PAGE_ID || page_table_.find(page_id)==page_table_.end())
    {
      latch_.unlock();
      return true;
    }
    if (pages_[page_table_.find(page_id)->second].pin_count_>0)
    {
      latch_.unlock();
      return false;
    }
    frame_id_t fra=page_table_.find(page_id)->second;
    free_list_.push_back(fra);
    replacer_->Pin(fra);
    page_table_.erase(page_id);
    pages_[fra].ResetMemory();
    DeallocatePage(page_id);
    latch_.unlock();
    return true; 
}


bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty)
{
    latch_.lock();
    if (page_table_.find(page_id)==page_table_.end() || page_id==INVALID_PAGE_ID || pages_[page_table_.find(page_id)->second].pin_count_==0)
    {
        latch_.unlock();
        return false;
    }
    auto iter = page_table_.find(page_id);
    frame_id_t frame_id = iter->second;
    Page *page = &pages_[frame_id];
    if (page->GetPinCount() <= 0) {  // 如果没被pin过，直接返回false
    return false;
    }
    pages_[page_table_.find(page_id)->second].pin_count_--;
    pages_[page_table_.find(page_id)->second].is_dirty_ |=is_dirty;
    if (page->GetPinCount() <= 0) {
    replacer_->Unpin(frame_id);
    }
    latch_.unlock();
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
