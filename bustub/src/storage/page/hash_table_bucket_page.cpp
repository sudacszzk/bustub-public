//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {


template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) 
{
  bool found=false;
  for(uint32_t i=0;i<BUCKET_ARRAY_SIZE;i++)
  {
    if (IsReadable(i) && cmp(key,array_[i].first)==0)
    {
      result->push_back(array_[i].second);
      found=true;
    }
  }
  return found;

}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp)
{
  for(uint32_t i=0;i<BUCKET_ARRAY_SIZE;i++)
  {
    if (IsReadable(i) && cmp(key,array_[i].first)==0 && value == array_[i].second)
    {
      return false;
    }
  }

  for(uint32_t i=0;i<BUCKET_ARRAY_SIZE;i++)
  {
    if (!IsReadable(i))
    {
      array_[i]=MappingType(key, value);
      SetOccupied(i);
      SetReadable(i);
      return true;
    }

  }
  return false;


}


template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) 
{
  for(uint32_t i=0;i<BUCKET_ARRAY_SIZE;i++)
  {
    if (IsReadable(i) && cmp(key,array_[i].first)==0 && value == array_[i].second)
    {
      RemoveAt(i);
      return true;

    }
  }
  return false;

}
/**
 * Gets the key at an index in the bucket.
 *
 * @param bucket_idx the index in the bucket to get the key at
 * @return key at index bucket_idx of the bucket
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

/**
 * Gets the value at an index in the bucket.
 *
 * @param bucket_idx the index in the bucket to get the value at
 * @return value at index bucket_idx of the bucket
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}



template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx)
{
  uint8_t di=bucket_idx/8;
  uint8_t yu=bucket_idx%8;
  
  uint8_t temp=static_cast<uint8_t>(readable_[di]);
  temp=temp&(~(1<<yu));
  readable_[di]=static_cast<char>(temp);


}


//const 放在函数后表示这个函数是常成员函数, 常成员函数是不能改变成员变量值的函数。
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const 
{
  uint8_t di=bucket_idx/8;
  uint8_t yu=bucket_idx%8;
  return static_cast<bool>((occupied_[di]>>yu)&1);

}



template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx)
{
  uint8_t di=bucket_idx/8;
  uint8_t yu=bucket_idx%8;
  uint8_t temp=static_cast<uint8_t>(occupied_[di]);
  temp=temp|(1<<yu);
  occupied_[di]=static_cast<char>(temp);

}



template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const
{
  uint8_t di=bucket_idx/8;
  uint8_t yu=bucket_idx%8;
  return static_cast<bool>((readable_[di]>>yu)&1);
}


template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) 
{
  uint8_t di=bucket_idx/8;
  uint8_t yu=bucket_idx%8;
  uint8_t temp=static_cast<uint8_t>(readable_[di]);
  temp=temp|(1<<yu);
  readable_[di]=static_cast<char>(temp);

}


template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull()
{
  for(uint32_t i=0;i<BUCKET_ARRAY_SIZE;i++)
  {
    if (!IsReadable(i))
    {
      return false;

    }
  }
  return true;
}


template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable()
{
  uint32_t res=0;
  for(uint32_t i=0;i<BUCKET_ARRAY_SIZE;i++)
  {
    if (IsReadable(i))
    {
      res++;

    }
  }
  return res;

}
/**
 * @return whether the bucket is empty
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() 
{
  for(uint32_t i=0;i<BUCKET_ARRAY_SIZE;i++)
  {
    if (IsReadable(i))
    {
      return false;

    }
  }
  return true;
}


template <typename KeyType, typename ValueType, typename KeyComparator>
MappingType *HASH_TABLE_BUCKET_TYPE::GetArrayCopy() 
{
  uint32_t num = NumReadable();
  MappingType *copy = new MappingType[num];
  for (uint32_t i = 0, index = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) 
    {
      copy[index++] = array_[i];
    }
  }
  return copy;
}


template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::Reset() {
  memset(occupied_, 0, sizeof(occupied_));
  memset(readable_, 0, sizeof(readable_));
  
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
