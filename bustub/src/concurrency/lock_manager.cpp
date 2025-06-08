//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <utility>
#include <vector>

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"
/*
SERIALIZABLE: Obtain all locks first; plus index
locks, plus strict 2PL.
REPEATABLE READS: Same as above, but no index
locks.
READ COMMITTED: Same as above, but S locks are
released immediately.
READ UNCOMMITTED: Same as above but allows
dirty reads (no S locks).


1. 如果事务想要读取对象，必须先以共享模式获得锁。可以有多个事务同时获得一个对象的共享锁，但是如果某个事务已经获得了对象的独占锁，则所有其他事务必须等待。
2. 如果事务要修改对象，必须以独占模式获取锁，不允许多个事务同时持有该锁。
3. 如果事务首先读取对象，然后尝试写入对象，则需要将共享锁升级为独占锁，升级锁的流程等价于直接获得独占锁。
4. 事务获得锁之后，一致持有锁直到事务结束，这也是名字两阶段的由来，在第一阶段即事务执行之前要获取锁，第二阶段则释放锁。
*/
namespace bustub {

inline void LockManager::try_Insert(LockRequestQueue *lock_queue, txn_id_t txn_id, LockMode lock_mode,bool grant) 
{
    bool is_inserted = false;
    for (auto &itor : lock_queue->request_queue_) 
    {
      if (itor.txn_id_ == txn_id) {
        is_inserted = true;
        itor.granted_ = itor.granted_|grant;
        break;
      }
    }
    if (!is_inserted) 
    {
      lock_queue->request_queue_.emplace_back(LockRequest{txn_id, lock_mode,grant});
    }

}



bool LockManager::LockShared(Transaction *txn, const RID &rid)
{
    std::unique_lock<std::mutex> ul(latch_);
    while(true)
    {
        bool fl=false;
        if(txn->GetState()==TransactionState::ABORTED || txn->GetState()==TransactionState::COMMITTED)
        {
            return false;
        }
        if (txn->GetIsolationLevel()==IsolationLevel::READ_UNCOMMITTED)
        {
            txn->SetState(TransactionState::ABORTED);
            return false;
        }
        if (txn->GetIsolationLevel()==IsolationLevel::REPEATABLE_READ && txn->GetState()==TransactionState::SHRINKING)
        {
            txn->SetState(TransactionState::ABORTED);
            return false;
        }
        
        if (txn->IsSharedLocked(rid))
        {
            return true;
        }

        auto iterque = lock_table_[rid].request_queue_.begin();
        while (iterque != lock_table_[rid].request_queue_.end())
        {
            if (iterque->txn_id_>txn->GetTransactionId() && iterque->lock_mode_==LockMode::EXCLUSIVE)
            //新的事务已经拥有排他锁,abort掉新的事务
            {
                //在 C++ 中，可以使用类名和双冒号来访问类中的静态成员
                TransactionManager::GetTransaction(iterque->txn_id_)->SetState(TransactionState::ABORTED);
                TransactionManager::GetTransaction(iterque->txn_id_)->GetExclusiveLockSet()->erase(rid);
                iterque=lock_table_[rid].request_queue_.erase(iterque);


            }
            else if (iterque->txn_id_<txn->GetTransactionId() && iterque->lock_mode_==LockMode::EXCLUSIVE)
            //等待旧的事务释放排他锁
            {
                try_Insert(&lock_table_[rid], txn->GetTransactionId(), LockMode::SHARED,false);
                lock_table_[rid].cv_.wait(ul);
                fl=true;
                break;
            }
            else if (iterque->txn_id_==txn->GetTransactionId() )
            {
                break;
            }
            else
            {
                iterque++;
            }

        }
        
        if (fl)
        {
            continue;
        }
        else
        {
            txn->SetState(TransactionState::GROWING);
            try_Insert(&lock_table_[rid], txn->GetTransactionId(), LockMode::SHARED,true);
            
            txn->GetSharedLockSet()->emplace(rid);
            return true;
        }
        
    }

    
}



bool LockManager::LockExclusive(Transaction *txn, const RID &rid) 
{
    std::unique_lock<std::mutex> ul(latch_);
    while(true)
    {
        bool fl=false;
        if(txn->GetState()==TransactionState::ABORTED || txn->GetState()==TransactionState::COMMITTED)
        {
            return false;
        }
       
        if (txn->GetIsolationLevel()==IsolationLevel::REPEATABLE_READ && txn->GetState()==TransactionState::SHRINKING)
        {
            txn->SetState(TransactionState::ABORTED);
            return false;
        }
        
        if (txn->IsExclusiveLocked(rid))
        {
            return true;
        }

        auto iterque = lock_table_[rid].request_queue_.begin();
        while (iterque != lock_table_[rid].request_queue_.end())
        {
            if (iterque->txn_id_>txn->GetTransactionId())
            //新的事务已经拥有锁,abort掉新的事务
            {
                //在 C++ 中，可以使用类名和双冒号来访问类中的静态成员
                TransactionManager::GetTransaction(iterque->txn_id_)->SetState(TransactionState::ABORTED);
                TransactionManager::GetTransaction(iterque->txn_id_)->GetSharedLockSet()->erase(rid);
                TransactionManager::GetTransaction(iterque->txn_id_)->GetExclusiveLockSet()->erase(rid);
                iterque=lock_table_[rid].request_queue_.erase(iterque);


            }
            else if (iterque->txn_id_ < txn->GetTransactionId() )
            //等待旧的事务释放锁
            {
                try_Insert(&lock_table_[rid], txn->GetTransactionId(), LockMode::EXCLUSIVE,false);
                lock_table_[rid].cv_.wait(ul);
                fl=true;
                break;
            }
            else if (iterque->txn_id_==txn->GetTransactionId() )
            {
                break;
            }
            else
            {
                iterque++;
            }

        }
        
        if (fl)
        {
            continue;
        }
        else
        {
            txn->SetState(TransactionState::GROWING);
            try_Insert(&lock_table_[rid], txn->GetTransactionId(), LockMode::EXCLUSIVE,true);
            
            txn->GetExclusiveLockSet()->emplace(rid);
            return true;
            
        }
        
    }

    
        

}


bool LockManager::LockUpgrade(Transaction *txn, const RID &rid)
{
    std::unique_lock<std::mutex> ul(latch_);
    while(true)
    {
        bool fl=false;
        if(txn->GetState()==TransactionState::ABORTED || txn->GetState()==TransactionState::COMMITTED)
        {
            return false;
        }
        
        if (txn->GetIsolationLevel()==IsolationLevel::REPEATABLE_READ && txn->GetState()==TransactionState::SHRINKING)
        {
            txn->SetState(TransactionState::ABORTED);
            return false;
        }
        
        if (txn->IsExclusiveLocked(rid))
        {
            return true;
        }
        if(lock_table_[rid].upgrading_ == true)
        {
            return false;
        }
        lock_table_[rid].upgrading_=true;
        auto iterque= lock_table_[rid].request_queue_.begin();
        while (iterque != lock_table_[rid].request_queue_.end())
        {
            if (iterque->txn_id_>txn->GetTransactionId())
            //新的事务已经拥有锁,abort掉新的事务
            {
                //在 C++ 中，可以使用类名和双冒号来访问类中的静态成员
                TransactionManager::GetTransaction(iterque->txn_id_)->SetState(TransactionState::ABORTED);
                TransactionManager::GetTransaction(iterque->txn_id_)->GetSharedLockSet()->erase(rid);
                TransactionManager::GetTransaction(iterque->txn_id_)->GetExclusiveLockSet()->erase(rid);
                iterque=lock_table_[rid].request_queue_.erase(iterque);


            }
            else if (iterque->txn_id_<txn->GetTransactionId() )
            //等待旧的事务释放锁
            {
                lock_table_[rid].cv_.wait(ul);
                fl=true;
                break;
            }
            
            else
            {
                iterque++;
            }

        }
        if (fl)
        {
            continue;
        }
        else
        {

            try_Insert(&lock_table_[rid], txn->GetTransactionId(), LockMode::EXCLUSIVE,true);
            
            
           
            LockRequest &request_item = lock_table_[rid].request_queue_.front();
            
            request_item.lock_mode_ = LockMode::EXCLUSIVE;
            txn->GetSharedLockSet()->erase(rid);
            txn->GetExclusiveLockSet()->emplace(rid);
            lock_table_[rid].upgrading_ = false;
            return true;
                      
        }
        
    }

}



bool LockManager::Unlock(Transaction *txn, const RID &rid)
{
    std::unique_lock<std::mutex> ul(latch_);
    
    if (txn->GetIsolationLevel()==IsolationLevel::REPEATABLE_READ && txn->GetState()==TransactionState::GROWING)
    {
        txn->SetState(TransactionState::SHRINKING);
        
    }
  LockRequestQueue &lock_queue = lock_table_[rid];
  std::list<LockRequest> &request_queue = lock_queue.request_queue_;
  auto itor = request_queue.begin();
  while (itor != request_queue.end()) {
    if (itor->txn_id_ == txn->GetTransactionId()) 
    { 
      // 当前事务解锁
      //assert(itor->lock_mode_ == txn_lockmode);
      request_queue.erase(itor);
      // 通知睡眠的事务并在事务中释放锁
      txn->GetSharedLockSet()->erase(rid);
      txn->GetExclusiveLockSet()->erase(rid);
      lock_queue.cv_.notify_all();
      return true;
    }
    itor++;
  }
  return false;




}

}  // namespace bustub
