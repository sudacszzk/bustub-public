//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() 
{
  left_executor_->Init();
  right_executor_->Init();

}



bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) 
{
  
  Tuple left_tu;
  RID left_rid;
  Tuple right_tu;
  RID right_rid;
  while(left_executor_->Next(&left_tu,&left_rid))
  {
    while (right_executor_->Next(&right_tu,&right_rid))
    {
        const AbstractExpression *predict = plan_->Predicate();
        if (predict == nullptr || predict->EvaluateJoin(&left_tu, left_executor_->GetOutputSchema(), &right_tu,right_executor_->GetOutputSchema()).GetAs<bool>())
        {


          const Schema *output_schema = plan_->OutputSchema();
   
          std::vector<Value> vals;
          vals.reserve(output_schema->GetColumnCount());
          
          for (const auto &col : GetOutputSchema()->GetColumns()) {
          vals.push_back(col.GetExpr()->EvaluateJoin(&left_tu, left_executor_->GetOutputSchema(), &right_tu,
                                                       right_executor_->GetOutputSchema()));
        }



    // 构造要返回的行
          Tuple temp_tuple(vals, output_schema);
          *tuple=temp_tuple;
          *rid=temp_tuple.GetRid();
          return true;
        }
    }
    right_executor_->Init();


  }
  return false;
}


}  // namespace bustub
