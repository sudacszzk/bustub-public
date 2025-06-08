//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) 
{

  Tuple tu;
  RID ri;
  while(child_executor_->Next(&tu,&ri))
  {
    Tuple new_tu=GenerateUpdatedTuple(tu);
    table_info_->table_->UpdateTuple(new_tu, ri, GetExecutorContext()->GetTransaction());
    for (const auto &inde :exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_))
    {
      inde->index_->InsertEntry(new_tu.KeyFromTuple(table_info_->schema_, *inde->index_->GetKeySchema(),inde->index_->GetKeyAttrs()), ri, GetExecutorContext()->GetTransaction());
      inde->index_->DeleteEntry(tu.KeyFromTuple(table_info_->schema_, *inde->index_->GetKeySchema(),inde->index_->GetKeyAttrs()),ri, GetExecutorContext()->GetTransaction());

      
    }

  }
  return false;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple)
{
  const Schema table_sche = table_info_->schema_;
  std::vector<Value> vals;
  vals.reserve(table_sche.GetColumnCount());//不能用->
  const std::unordered_map<uint32_t, UpdateInfo> update_attrs_te=plan_->GetUpdateAttr();
  for (uint32_t i = 0; i < vals.capacity(); i++)
  {
    if(update_attrs_te.find(i)==update_attrs_te.end())
    {
        vals.push_back(src_tuple.GetValue(&table_sche, i));
         //ColumnValueExpression
    }
    else
    {
      if(update_attrs_te.find(i)->second.type_==UpdateType::Add)
      {
          Value ad(src_tuple.GetValue(&table_sche, i).GetTypeId(),update_attrs_te.find(i)->second.update_val_);
          vals.push_back(src_tuple.GetValue(&table_sche, i).Add(ad));
      }
      else
      {
          Value se(src_tuple.GetValue(&table_sche, i).GetTypeId(),update_attrs_te.find(i)->second.update_val_);
          vals.push_back(se);
      }
    }
  }
  Tuple temp_tuple(vals,&table_sche);
  return temp_tuple;
}
}  // namespace bustub
