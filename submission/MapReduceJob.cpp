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
    if(threadId == 0)
    {
        job->countIntermediate();
        job->setStage(SHUFFLE_STAGE, job->intermediateCount);
        job->runShuffle();
        job->setStage(REDUCE_STAGE,  job->intermediateCount);
    }
    job->barrier->arrive_and_wait();
    job->runReduce();
}

// only thread 0 is allowed to call this and only after all threads map & sort
void MapReduceJob::countIntermediate(){
    uint64_t total = 0;
    for (auto &mapContext : mapContexts){
    total += mapContext.intermediateVec.size();
    }
    intermediateCount = total;
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



// only thread 0 gets here 
void MapReduceJob::runShuffle(){
    //while there is still stuff in vectors 
    while (true){
        //find the next lagrset element
        K2* maxKey = nullptr;
        for (size_t i = 0; i < mapContexts.size(); i++){
            auto &vec = mapContexts[i].intermediateVec;
                if (vec.empty()){
                    continue;
                }
            K2* currentKey = vec.back().first.get();
            if (maxKey == nullptr || *maxKey < *currentKey){
                maxKey = currentKey;
            }
        }
        if (maxKey == nullptr){
            break;
        }
        //collect all of that same element and pop each one
        IntermediateVec group;
        for (size_t i = 0; i < mapContexts.size(); i++){
            auto &vec = mapContexts[i].intermediateVec;
            while (!vec.empty() && !(*vec.back().first < *maxKey) && !(*maxKey < *vec.back().first)){
                group.push_back(vec.back());
                vec.pop_back();
            }
        }
        //add collected vec to shuffeld output
        shuffleOutput.push_back(std::move(group));
        incProcessed(group.size());
    }
}

void MapReduceJob::runReduce(){    
    // give each thread a vector 
        while(true){
        //fetch_add returns the value before the addition, so we can use it as an index to process
        uint64_t idx = reduceInputIndex.fetch_add(1);
        if(idx >= shuffleOutput.size())
        {
            break;
        }
        // run reduce of the client
        const IntermediateVec &group = shuffleOutput[idx];
        client.reduce(group, reduceContext);
        incProcessed(group.size());
    }
}



//--------public API implementations--------

MapReduceJob::MapReduceJob(const MapReduceClient &client, const InputVec &inputVec, int multiThreadLevel)
: client(client), inputVec(inputVec), multiThreadLevel(multiThreadLevel),reduceContext(&reduceMutex, &outputVec)
{
    barrier = new std::barrier<>(multiThreadLevel);
    // todo: initial state should be undefined and only whaen thread started map state should be map(?)
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
    //NOT THREAD SAFE YET - if called from multiple threads, might cause issuesq
    //need to add mutex
    //std::lock_guard<std::mutex> lock(waitMutex);
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
    if (!isDone()){
        wait();
    }
    std::sort(outputVec.begin(), outputVec.end(),
        [](const OutputPair &a, const OutputPair &b){
            return *(a.first) < *(b.first);
        });
    return outputVec;
     
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
