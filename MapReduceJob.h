#ifndef MAP_REDUCE_JOB_H
#define MAP_REDUCE_JOB_H

#include "MapReduceClient.h"
#include "MapContext.h"
#include "ReduceContext.h"
#include <barrier>
#include <atomic>
#include <thread>
#include <vector>
#include <algorithm>
// you can add other includes here

enum MapReduceStage
{
	UNDEFINED_STAGE, // 0
	MAP_STAGE, // 1
	SHUFFLE_STAGE, // 2
	REDUCE_STAGE // 3
};

class MapReduceState
{
public:
	MapReduceStage stage;
	double percentage;

	inline bool operator==(const MapReduceState &other) const
	{
		return this->stage == other.stage && std::abs(this->percentage - other.percentage) < 1e-6;
	}

	inline bool operator!=(const MapReduceState &other) const
	{
		return !(*this == other);
	}
};

class MapReduceJob
{
public:
	/*
	You CAN NOT change or add properties to this part (public API).
	*/

	MapReduceJob(const MapReduceClient &client, const InputVec &inputVec, int multiThreadLevel);

	~MapReduceJob();

	MapReduceState getState(void) const;

	bool isDone(void) const;
	
	void wait(void);

	OutputVec getOutput(void);

private:
	/*
		You can change everything on this part (these are just recommendations)
	*/
	const MapReduceClient &client;
	const InputVec& inputVec;
	int multiThreadLevel;
	std::vector<MapContext> mapContexts;
	std::vector<std::thread> threads;
	std::atomic<uint64_t> mapInputIndex;
	std::atomic<uint64_t> jobState; // 2 bits-stage, 31 bits-total to process, 31 bits-processed 
	std::barrier<>* barrier;
	OutputVec outputVec;

	void setStage(MapReduceStage stage, uint64_t totalToProcess);
	static void threadRun(MapReduceJob *job, int threadId);
	void runMap(int threadId);
	void runSort(int threadId);
	void runShuffle();
	void runReduce(int threadId);
	void incProcessed(int inc);

};
	
#endif // MAP_REDUCE_JOB_H
