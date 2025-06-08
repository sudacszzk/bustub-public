//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
//#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(0), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool 
{
    latch_.lock();
    size_t te=0;
    frame_id_t frame=1;
    //不满k次的
    for(auto it=node_store_.begin();it!=node_store_.end();it++)
    {
        if (it->second.get_history_size()<k_ && it->second.is_evictable()==true && current_timestamp_ - it->second.get_front() >te)
        {
            te=current_timestamp_ - it->second.get_front();
            frame=it->first;
        }
    }
    if (te!=0)
    {
        *frame_id=frame;
        node_store_.erase(frame);
        replacer_size_--;
        latch_.unlock();
        return true;
    }
    //k次的
    te=0;
    for(auto it=node_store_.begin();it!=node_store_.end();it++)
    {
        if (it->second.get_history_size()==k_ && it->second.is_evictable()==true && current_timestamp_ - it->second.get_front() > te)
        {
            te=current_timestamp_ - it->second.get_front();
            frame=it->first;
        }

    }
    if (te!=0)
    {
        *frame_id=frame;
        node_store_.erase(frame);
        replacer_size_--;
        latch_.unlock();
        return true;
    }
    

    
    latch_.unlock();
    return false;


}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) 
{
    latch_.lock();
    auto it=node_store_.find(frame_id);
    if (it==node_store_.end())
    {

        bustub::LRUKNode te;
        node_store_.insert(std::pair<frame_id_t,bustub::LRUKNode>(frame_id,te));
    }
    it=node_store_.find(frame_id);
    it->second.setk(k_);
    it->second.push(current_timestamp_);
    current_timestamp_++;
    latch_.unlock();

}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) 
{
    latch_.lock();
    auto it=node_store_.find(frame_id);
    if (set_evictable==true && it->second.is_evictable()==false)
    {
        replacer_size_++;
    }
    if (set_evictable==false && it->second.is_evictable()==true)
    {
        replacer_size_--;
    }
    it->second.set_evictab(set_evictable);
    latch_.unlock();


}

void LRUKReplacer::Remove(frame_id_t frame_id) 
{
    latch_.lock();
    if (node_store_.find(frame_id)->second.is_evictable())
    {
        replacer_size_--;
    }
    node_store_.erase(frame_id);
    
    latch_.unlock();

}

auto LRUKReplacer::Size() -> size_t 
{ 

    return replacer_size_;
}

}  // namespace bustub
