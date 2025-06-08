//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() 
{
  catalog_ = exec_ctx_->GetCatalog();
  table_info_ = catalog_->GetTable(plan_->TableOid());
  table_heap_ = table_info_->table_.get();
}


bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) 
{
  if (plan_->IsRawInsert())
  {
    for(uint32_t i=0;i<plan_->RawValues().size();i++)
    {
      Tuple tu=Tuple(plan_->RawValues()[i], &table_info_->schema_);
      InsertIntoTableWithIndex(&tu);
    }
    return false;
  }

  
  child_executor_->Init();
  Tuple tu;
  RID ri;
  while(child_executor_->Next(&tu,&ri))
  {
    InsertIntoTableWithIndex(&tu);
  }
   return false;
 
  
}

void InsertExecutor::InsertIntoTableWithIndex(Tuple *cur_tuple) 
{
  RID rid;
  table_heap_->InsertTuple(*cur_tuple, &rid, GetExecutorContext()->GetTransaction());
  
  for (const auto &inde :catalog_->GetTableIndexes(table_info_->name_))
  {
    inde->index_->InsertEntry(cur_tuple->KeyFromTuple(table_info_->schema_, *inde->index_->GetKeySchema(),inde->index_->GetKeyAttrs()), rid, GetExecutorContext()->GetTransaction());

  }
  
  

}

}  // namespace bustub
