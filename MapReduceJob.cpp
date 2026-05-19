#include "MapReduceJob.h"

/*
===============================================
Implement:
===============================================
*/

MapReduceJob::MapReduceJob(const MapReduceClient &client, const InputVec &inputVec, int multiThreadLevel)
: client(client), inputVec(inputVec), multiThreadLevel(multiThreadLevel), barrier(multiThreadLevel), nextInputIndex(0), jobState(0)
{
    // TODO: implement this constructor
    //for each 0-multithreadLevel, create a thread for it, and start it.
}

MapReduceState MapReduceJob::getState(void) const
{
    // TODO: implement this function
}

void MapReduceJob::wait(void)
{
    // TODO: implement this function
}

OutputVec MapReduceJob::getOutput(void)
{
    // TODO: implement this function
}

bool MapReduceJob::isDone(void) const
{
    // TODO: implement this function
}

MapReduceJob::~MapReduceJob()
{
    // TODO: implement this destructor
}
