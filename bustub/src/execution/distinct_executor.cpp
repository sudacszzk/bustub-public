//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init()
{
    child_executor_->Init();
}


bool DistinctExecutor::Next(Tuple *tuple, RID *rid) 
{
    Tuple te_tu;
    RID te_rid;

    if (child_executor_->Next(&te_tu,&te_rid))
    {
        DistinctKey te_diskey;
        const Schema *output_schema = plan_->OutputSchema();
        te_diskey.distincts_.reserve(output_schema->GetColumnCount());
        
        for (uint32_t idx = 0; idx < te_diskey.distincts_.capacity(); idx++) {
          te_diskey.distincts_.push_back(te_tu.GetValue(plan_->OutputSchema(), idx));
        }
        Tuple temp_tuple(te_diskey.distincts_, output_schema);
        if (map_.find(te_diskey)==map_.end())
        {
            map_[te_diskey]=temp_tuple;
            *tuple=temp_tuple;
            *rid=temp_tuple.GetRid();
            return true;

        }
        else
        {
          return Next(tuple, rid);

        }        

    }
    else
    {
        return false;
    }

    
}

}  // namespace bustub
