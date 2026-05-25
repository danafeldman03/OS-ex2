#ifndef REDUCE_CONTEXT_H
#define REDUCE_CONTEXT_H

#include <mutex>
#include "MapReduceKeys.h"

class ReduceContext
{
public:
    std::mutex* mtx;
    OutputVec* outputVec;

    ReduceContext(std::mutex* m, OutputVec* out)
    : mtx(m), outputVec(out) {}


    void addOutput(std::shared_ptr<K3> key, std::shared_ptr<V3> value);

};

#endif // REDUCE_CONTEXT_H