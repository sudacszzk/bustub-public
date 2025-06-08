//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  child_executor_->Init();
}


bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid)
{

  Tuple tu;
  RID ri;
  while(child_executor_->Next(&tu,&ri))
  {
    
    table_info_->table_->MarkDelete(ri, GetExecutorContext()->GetTransaction());
    for (const auto &inde :exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_))
    {
      
      inde->index_->DeleteEntry(tu.KeyFromTuple(table_info_->schema_, *inde->index_->GetKeySchema(),inde->index_->GetKeyAttrs()),ri, GetExecutorContext()->GetTransaction());

      
    }

  }
  return false;

}
}  // namespace bustub
