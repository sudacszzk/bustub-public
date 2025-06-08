//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }
/*
Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
   * are currently in use and not evictable (in another word, pinned).
   *
   * You should pick the replacement frame from either the free list or the replacer (always find from the free list
   * first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
   * you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
   * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
   * so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
   * Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
   * @param[out] page_id id of created page
   * @return nullptr if no new pages could be created, otherwise pointer to new page
*/
auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * 
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
      bool is_s=replacer_->Evict(&fra);
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
    replacer_->RecordAccess(fra);
    replacer_->SetEvictable(fra,false);
    page_id_t page_i=AllocatePage();
    *page_id = page_i;
    page_table_.insert(std::pair<page_id_t, frame_id_t>(page_i,fra));
    pages_[fra].ResetMemory();
    pages_[fra].page_id_ = page_i;
    pages_[fra].pin_count_ = 1;
    pages_[fra].is_dirty_ = false;
    latch_.unlock();
    return &pages_[fra]; 


}

auto BufferPoolManager::FetchPage(page_id_t page_id) -> Page * 
{
    latch_.lock();
    frame_id_t fra;
    if(page_table_.find(page_id)!=page_table_.end())
    {
      fra=page_table_.find(page_id)->second;
      replacer_->RecordAccess(fra);
      replacer_->SetEvictable(fra,false);
      pages_[fra].pin_count_ ++;
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
      bool is_s=replacer_->Evict(&fra);
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
    
    replacer_->RecordAccess(fra);
    replacer_->SetEvictable(fra,false);

    
    page_table_.insert(std::pair<page_id_t, frame_id_t>(page_id,fra));
    pages_[fra].ResetMemory();
    pages_[fra].page_id_ = page_id;
    pages_[fra].pin_count_ = 1;
    pages_[fra].is_dirty_ = false;
    disk_manager_->ReadPage(page_id, pages_[fra].data_);
    latch_.unlock();
    return &pages_[fra];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) -> bool 
{
    latch_.lock();
    if (page_table_.find(page_id)==page_table_.end() || page_id==INVALID_PAGE_ID || pages_[page_table_.find(page_id)->second].pin_count_==0)
    {
        latch_.unlock();
        return false;
    }
    pages_[page_table_.find(page_id)->second].pin_count_--;
    if (pages_[page_table_.find(page_id)->second].pin_count_==0)
    {
      replacer_->SetEvictable(page_table_.find(page_id)->second, true); 
    }
    pages_[page_table_.find(page_id)->second].is_dirty_ |=is_dirty;
    latch_.unlock();
    return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool 
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

void BufferPoolManager::FlushAllPages()
{
  latch_.lock();
  for(size_t i=0;i<pool_size_;i++)
  {
    FlushPage(pages_[i].page_id_);
  }

  latch_.unlock();
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool 
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
    replacer_->Remove(fra);
    page_table_.erase(page_id);
    pages_[fra].ResetMemory();
    DeallocatePage(page_id);
    latch_.unlock();
    return true; 



}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
