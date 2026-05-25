#include "ReduceContext.h"

// implement here your constructor and destructor

void ReduceContext::addOutput(std::shared_ptr<K3> key, std::shared_ptr<V3> value)
{
    std::lock_guard<std::mutex> lock(*mtx);
    outputVec->push_back({key, value});
}

 