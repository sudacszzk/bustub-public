//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}


void AggregationExecutor::Init()
{
    child_->Init();
    Tuple te_tu;
    RID te_rid;
    while(child_->Next(&te_tu,&te_rid))
    {
        aht_.InsertCombine(MakeAggregateKey(&te_tu), MakeAggregateValue(&te_tu));
    }
    aht_iterator_=aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) 
{
    while(aht_iterator_!=aht_.End())
    {
      const AggregateKey agg_key = aht_iterator_.Key();
      const AggregateValue agg_value = aht_iterator_.Val();
      if  (plan_->GetHaving() == nullptr ||plan_->GetHaving()->EvaluateAggregate(agg_key.group_bys_, agg_value.aggregates_).GetAs<bool>())
      {
          const Schema *output_schema = plan_->OutputSchema();
   
          std::vector<Value> vals;
          vals.reserve(output_schema->GetColumnCount());
          
          for (const auto &col : GetOutputSchema()->GetColumns()) 
          {
              vals.push_back(col.GetExpr()->EvaluateAggregate(agg_key.group_bys_, agg_value.aggregates_));
          }
          Tuple temp_tuple(vals, output_schema);
          *tuple=temp_tuple;
          *rid=temp_tuple.GetRid();
          ++aht_iterator_;
          return true;
      }
      else
      {
          ++aht_iterator_;
      }
       
    }
    return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub