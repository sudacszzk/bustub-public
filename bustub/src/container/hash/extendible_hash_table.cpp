//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <fstream>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
      //std::move作用主要可以将一个左值转换成右值引用，从而可以调用C++11右值引用的拷贝构造函数
  //  implement me!
  directory_page_id_ = INVALID_PAGE_ID;
  // std::ifstream file("/autograder/bustub/test/container/grading_hash_table_concurrent_test.cpp");
  // std::string str;
  // while (file.good()) {
  //   std::getline(file, str);
  //   std::cout << str << std::endl;
  // }
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

/**
 * KeyToDirectoryIndex - maps a key to a directory index
 *
 * In Extendible Hashing we map a key to a directory index
 * using the following hash + mask function.
 *
 * DirectoryIndex = Hash(key) & GLOBAL_DEPTH_MASK
 *
 * where GLOBAL_DEPTH_MASK is a mask with exactly GLOBAL_DEPTH 1's from LSB
 * upwards.  For example, global depth 3 corresponds to 0x00000007 in a 32-bit
 * representation.
 *
 * @param key the key to use for lookup
 * @param dir_page to use for lookup of global depth
 * @return the directory index
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  // 参考注释，直接用掩码做与运算即可
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

/**
 * Get the bucket page_id corresponding to a key.
 *
 * @param key the key for lookup
 * @param dir_page a pointer to the hash table's directory page
 * @return the bucket page_id corresponding to the input key
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
page_id_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) 
{
  // 调用现有函数即可
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}



template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage()
{
  directory_lock_.lock();
  HashTableDirectoryPage *temp=nullptr;
  if (directory_page_id_ == INVALID_PAGE_ID)
  {
    page_id_t new_direpageid;
    Page *new_direpage=buffer_pool_manager_->NewPage(&new_direpageid);

    temp=reinterpret_cast<HashTableDirectoryPage *>(new_direpage->GetData());

    directory_page_id_ = new_direpageid;
    temp->SetPageId(directory_page_id_);

    page_id_t new_buckpageid;
    Page *new_buckpage=buffer_pool_manager_->NewPage(&new_buckpageid);
    assert(new_buckpage != nullptr);

    temp->SetBucketPageId(0,new_buckpageid);
    buffer_pool_manager_->UnpinPage(new_direpageid, true);
    buffer_pool_manager_->UnpinPage(new_buckpageid, true);
    



  }

  Page *page = buffer_pool_manager_->FetchPage(directory_page_id_);
  assert(page != nullptr);
  temp = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
  directory_lock_.unlock();
  return temp;
  


}

/**
 * Fetches the a bucket page from the buffer pool manager using the bucket's page_id.
 * 使用pageid从BufferPoolManager中得到一个Page，其GetData就是bucket对象。
 * 修改，添加一个函数，修改Fetch函数的签名。将Page与data分开处理
 *
 * @param bucket_page_id the page_id to fetch
 * @return a pointer to a bucket page
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
Page *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  assert(page != nullptr);
  return page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::GetBucketPageData(Page *page) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}


template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result)
{
  table_latch_.RLock();
  
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  Page *page=FetchBucketPage(KeyToPageId(key, dir_page));
  page->RLatch();
  
  HASH_TABLE_BUCKET_TYPE *buck_page=reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  
  bool res=buck_page->GetValue(key,comparator_,result);
  page->RUnlatch();

  assert(buffer_pool_manager_->UnpinPage(KeyToPageId(key, dir_page), false));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));

  
  table_latch_.RUnlock();
  return res;

}



template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value)
{
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  Page *page=FetchBucketPage(KeyToPageId(key, dir_page));
  page->WLatch();

  HASH_TABLE_BUCKET_TYPE *buck_page=reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  if(!buck_page->IsFull())//不需要分裂
  {
    bool res= buck_page->Insert(key, value, comparator_);


    page->WUnlatch();

    assert(buffer_pool_manager_->UnpinPage(KeyToPageId(key, dir_page), true));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));

    table_latch_.WUnlock();
    return res;
  }

    page->WUnlatch();

    assert(buffer_pool_manager_->UnpinPage(KeyToPageId(key, dir_page), false));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));

    table_latch_.WUnlock();
    return SplitInsert(transaction, key, value);
}



template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) 
{
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  Page *page=FetchBucketPage(KeyToPageId(key, dir_page));
  page->WLatch();
  uint32_t bucket_idx=KeyToDirectoryIndex(key, dir_page);
  uint32_t localdepth=dir_page->GetLocalDepth(bucket_idx);
  page_id_t split_bucket_page_id=KeyToPageId(key, dir_page);
  if (localdepth>=MAX_BUCKET_DEPTH)
  {
    page->WUnlatch();
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    assert(buffer_pool_manager_->UnpinPage(KeyToPageId(key, dir_page), false));
    table_latch_.WUnlock();
    return false;

  }
  bool glodep_inc=false;
  if (localdepth == dir_page->GetGlobalDepth())
  {
    dir_page->IncrGlobalDepth();
    glodep_inc=true;
  }
  dir_page->IncrLocalDepth(bucket_idx);

  HASH_TABLE_BUCKET_TYPE *split_bucket = GetBucketPageData(page);
  uint32_t origin_array_size = split_bucket->NumReadable();
  MappingType *origin_array = split_bucket->GetArrayCopy();
  split_bucket->Reset();

  // 创建一个image bucket，并初始化该image bucket
  page_id_t image_bucket_page_id;
  Page *image_bucket_page = buffer_pool_manager_->NewPage(&image_bucket_page_id);
  assert(image_bucket_page != nullptr);
  image_bucket_page->WLatch();
  HASH_TABLE_BUCKET_TYPE *image_bucket = GetBucketPageData(image_bucket_page);
  uint32_t split_image_bucket_index = dir_page->GetSplitImageIndex(bucket_idx);
  dir_page->SetLocalDepth(split_image_bucket_index, dir_page->GetLocalDepth(bucket_idx));
  dir_page->SetBucketPageId(split_image_bucket_index, image_bucket_page_id);

  uint32_t diff = 1 << dir_page->GetLocalDepth(bucket_idx);
  //INVALID_PAGE_ID
  if (glodep_inc)
  {
    for (uint32_t i = dir_page->Size()/2; i < dir_page->Size(); i ++)
    {
      dir_page->SetBucketPageId(i, INVALID_PAGE_ID);
    }

  }
  for (int i = bucket_idx; i >= 0; i -= diff) 
  {
    uint32_t ii=(uint32_t)i;
    dir_page->SetBucketPageId(ii, split_bucket_page_id);
    dir_page->SetLocalDepth(ii, dir_page->GetLocalDepth(bucket_idx));
    
  }
  for (uint32_t i = bucket_idx; i < dir_page->Size(); i += diff) {
    dir_page->SetBucketPageId(i, split_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(bucket_idx));
  }
  for (int i = split_image_bucket_index; i >= 0; i -= diff) {
    uint32_t ii=(uint32_t)i;
    dir_page->SetBucketPageId(ii, image_bucket_page_id);
    dir_page->SetLocalDepth(ii, dir_page->GetLocalDepth(bucket_idx));
  }
  for (uint32_t i = split_image_bucket_index; i < dir_page->Size(); i += diff) {
    dir_page->SetBucketPageId(i, image_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(bucket_idx));
  }

  if (glodep_inc)
  {
    for (uint32_t i = dir_page->Size()/2; i < dir_page->Size(); i ++)
    {
      if (dir_page->GetBucketPageId(i)== INVALID_PAGE_ID)
      {
        dir_page->SetBucketPageId(i, dir_page->GetBucketPageId(i-dir_page->Size()/2));
        dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(i-dir_page->Size()/2));
      }
    }

  }

  for (uint32_t i = 0; i < origin_array_size; i++)
  {
    uint32_t target_bucket_index = Hash(origin_array[i].first) & dir_page->GetLocalDepthMask(bucket_idx);
    page_id_t target_bucket_index_page = dir_page->GetBucketPageId(target_bucket_index);
    assert(target_bucket_index_page == split_bucket_page_id || target_bucket_index_page == image_bucket_page_id);
    // 这里根据新计算的hash结果决定插入哪个bucket
    if (target_bucket_index_page == split_bucket_page_id) {
      assert(split_bucket->Insert(origin_array[i].first, origin_array[i].second, comparator_));
    } else {
      assert(image_bucket->Insert(origin_array[i].first, origin_array[i].second, comparator_));
    }
  }
  delete []origin_array;
  page->WUnlatch();
  image_bucket_page->WUnlatch();
  // Unpin
  assert(buffer_pool_manager_->UnpinPage(split_bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(image_bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));

  table_latch_.WUnlock();
  // 最后重新尝试插入
  return Insert(transaction, key, value);

}



template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value)
{

  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  Page *page=FetchBucketPage(KeyToPageId(key, dir_page));
  page->WLatch();
  uint32_t bucket_idx=KeyToDirectoryIndex(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket = GetBucketPageData(page);
  bool res=bucket->Remove(key, value, comparator_);
  if(bucket->IsEmpty())
  {
    page->WUnlatch();
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    assert(buffer_pool_manager_->UnpinPage(KeyToPageId(key, dir_page), true));
    table_latch_.WUnlock();
    Merge(transaction, bucket_idx);
    return res;

  }
  
  page->WUnlatch();

  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  assert(buffer_pool_manager_->UnpinPage(KeyToPageId(key, dir_page), true));

  table_latch_.WUnlock();
  return res;

}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, uint32_t target_bucket_index) 
{
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t localdepth=dir_page->GetLocalDepth(target_bucket_index);
  if (localdepth==0)
  {
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }

  uint32_t image_bucket_index = dir_page->GetSplitImageIndex(target_bucket_index);
  page_id_t image_bucket_page_id = dir_page->GetBucketPageId(image_bucket_index);

  dir_page->DecrLocalDepth(target_bucket_index);
  uint32_t diff = 1 << dir_page->GetLocalDepth(target_bucket_index);

  for (int i =target_bucket_index; i >= 0; i -= diff) 
  {
    uint32_t ii=(uint32_t)i;
    dir_page->SetBucketPageId(ii,  image_bucket_page_id );
    dir_page->SetLocalDepth(ii, dir_page->GetLocalDepth(target_bucket_index));
    
  }
  for (uint32_t i = target_bucket_index; i < dir_page->Size(); i += diff) {
    dir_page->SetBucketPageId(i,  image_bucket_page_id );
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(target_bucket_index));
  }

  if (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
    table_latch_.WUnlock();


  

}
/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
