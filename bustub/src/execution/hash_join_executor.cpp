//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_executor_(std::move(left_child)),
      right_child_executor_(std::move(right_child)) {}



void HashJoinExecutor::Init() 
{
    left_child_executor_->Init();
    right_child_executor_->Init();
    fl=-1;
    Tuple left_tu;
    RID left_rid;
    
    while(left_child_executor_->Next(&left_tu,&left_rid))
    {
        HashJoinKey temp_joinkey{plan_->LeftJoinKeyExpression()->Evaluate(&left_tu,left_child_executor_->GetOutputSchema())};
        if(map_.find(temp_joinkey)==map_.end())
        {
            map_[temp_joinkey]=std::vector<Tuple>();
            map_.find(temp_joinkey)->second.push_back(left_tu);

        }
        else
        {
            map_.find(temp_joinkey)->second.push_back(left_tu);
        }
        
    }

}


bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) 
{
  while(true)
  {
    if (fl==-1)
    {
        if (right_child_executor_->Next(&right_tu,&right_rid)==false)
        {
            return false;
        }
    } 
    HashJoinKey temp_joinkey{plan_->RightJoinKeyExpression()->Evaluate(&right_tu,right_child_executor_->GetOutputSchema())};
    fl++;
    if(map_.find(temp_joinkey)==map_.end() || fl>=(int)map_.find(temp_joinkey)->second.size())
    {
        fl=-1;
        continue;
    }
    const Schema *output_schema = plan_->OutputSchema();
   
          std::vector<Value> vals;
          vals.reserve(output_schema->GetColumnCount());
          
          for (const auto &col : GetOutputSchema()->GetColumns()) 
          {
          vals.push_back(col.GetExpr()->EvaluateJoin(&map_.find(temp_joinkey)->second[fl], left_child_executor_->GetOutputSchema(), &right_tu,right_child_executor_->GetOutputSchema()));
          }


          Tuple temp_tuple(vals, output_schema);
          *tuple=temp_tuple;
          *rid=temp_tuple.GetRid();

    return true;
    
  }

}


}  // namespace bustub
