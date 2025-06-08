//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  child_executor_->Init();
  output_num_ = 0;
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid)
{
    if(output_num_>=plan_->GetLimit())
    {
        
        return false;
    }
    Tuple te_tu;
    RID te_rid;
    
    
    if (child_executor_->Next(&te_tu,&te_rid))
    {
        *tuple=te_tu;//不能tuple=&te_tu;
        //*a = b;不改变a,但它改变了a指向的东西.a = &b;确实发生了变化,a但并未改变所a指出的内容.
        *rid=te_rid;
        output_num_++;
        return true;
    }
    else
    {
        return false;
    }
    
        
    
    
}

}  // namespace bustub
