#include "MapReduceJob.h"

/*
===============================================
Implement:
===============================================
*/
static const uint64_t STAGE_MASK = 0x3ULL;
static const uint64_t TOTAL_MASK = 0x7FFFFFFFULL;
static const uint64_t PROCESSED_MASK = 0x7FFFFFFFULL;
static const uint64_t STAGE_SHIFT = 62;
static const uint64_t TOTAL_SHIFT = 31;


//-------private functions implementations-------

void MapReduceJob::setStage(MapReduceStage stage, uint64_t totalToProcess)
{
    jobState.store(((uint64_t)(stage & STAGE_MASK) << STAGE_SHIFT)|((uint64_t)(totalToProcess & TOTAL_MASK) << TOTAL_SHIFT));
}

//this funciton might be unnecassary, maybe we sould delete it and simply use jobState.fetch_add(inc) every time
void MapReduceJob::incProcessed(int inc)
{
    jobState.fetch_add(inc);
}

//-------threads functions implementations------- 

void MapReduceJob::threadRun(MapReduceJob *job, int threadId)
{
    job->runMap(threadId);
    job->runSort(threadId);
    job->barrier->arrive_and_wait();
    //TODO: implement the shuffle and reduce stages
    /*
    if(threadId == 0)
    {
        setStage(SHUFFLE_STAGE, totalToProcess); // totalToProcess should be the total number of intermediate pairs to process in the reduce stage
        job->runShuffle();
    }
    else{
        // other threads wait for the shuffle to finish
    }
    job->runReduce(threadId);
    */
}


void MapReduceJob::runMap(int threadId)
{
    while(true){
        //fetch_add returns the value before the addition, so we can use it as an index to process
        uint64_t idx = mapInputIndex.fetch_add(1);
        if(idx >= inputVec.size())
        {
            break;
        }
        client.map(inputVec[idx].first, inputVec[idx].second, mapContexts[threadId]);
        incProcessed(1);
    }
}


void MapReduceJob::runSort(int threadId)
{
    std::sort(mapContexts[threadId].intermediateVec.begin(), mapContexts[threadId].intermediateVec.end(),
    [](const IntermediatePair& a, const IntermediatePair& b){
        return *(a.first) < *(b.first);
    });
}

//void MapReduceJob::runShuffle(){}
//void MapReduceJob::runReduce(int threadId){}



//--------public API implementations--------

MapReduceJob::MapReduceJob(const MapReduceClient &client, const InputVec &inputVec, int multiThreadLevel)
: client(client), inputVec(inputVec), multiThreadLevel(multiThreadLevel)
{
    barrier = new std::barrier<>(multiThreadLevel);
    setStage(MAP_STAGE, inputVec.size());
    threads.reserve(multiThreadLevel);
    mapContexts.resize(multiThreadLevel);
    for(int i = 0; i < multiThreadLevel; ++i)
    {
        threads.emplace_back(threadRun, this, i);
    }
}


MapReduceState MapReduceJob::getState(void) const
{
    uint64_t val = jobState.load();
    uint64_t stage = (val >> STAGE_SHIFT) & STAGE_MASK;
    uint64_t total = (val >> TOTAL_SHIFT) & TOTAL_MASK;
    uint64_t processed = val & PROCESSED_MASK;

    return {MapReduceState((MapReduceStage)stage, 100.0 * (double)processed / (double)total)};
}

void MapReduceJob::wait(void)
{
    std::lock_guard<std::mutex> lock(waitMutex);
    for (std::thread &t : threads)
    {
        if (t.joinable())
        {
            t.join();
        }
    }
}

OutputVec MapReduceJob::getOutput(void)
{
    // TODO: implement this function
}


bool MapReduceJob::isDone(void) const
{
    uint64_t val = jobState.load();
    uint64_t stage = (val >> STAGE_SHIFT) & STAGE_MASK;
    uint64_t total = (val >> TOTAL_SHIFT) & TOTAL_MASK;
    uint64_t processed = val & PROCESSED_MASK;

    // Done = in reduce stage and all pairs processed
    return (stage == REDUCE_STAGE) && (processed >= total);
}

MapReduceJob::~MapReduceJob()
{
    wait();
    delete barrier;
}
