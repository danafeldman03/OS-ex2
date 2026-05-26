#include "MapReduceJob.h"
#include "MapReduceJob.cpp"
#include "MapContext.h"
#include "MapContext.cpp"
#include "ReduceContext.h"
#include "ReduceContext.cpp"
#include "MapReduceKeys.h"
#include "MapReduceClient.h"

#include <cassert>
#include <iostream>
#include <thread>
#include <atomic>
#include <chrono>
#include <random>

/*
========================================
Test Key/Value Types
========================================
*/

class IntK1 : public K1
{
public:
    int val;

    explicit IntK1(int v) : val(v) {}

    bool operator<(const K1 &other) const override
    {
        return val < dynamic_cast<const IntK1 &>(other).val;
    }
};

class IntV1 : public V1
{
public:
    int val;

    explicit IntV1(int v) : val(v) {}
};

class IntK2 : public K2
{
public:
    int val;

    explicit IntK2(int v) : val(v) {}

    bool operator<(const K2 &other) const override
    {
        return val < dynamic_cast<const IntK2 &>(other).val;
    }
};

class IntV2 : public V2
{
public:
    int val;

    explicit IntV2(int v) : val(v) {}
};

class IntK3 : public K3
{
public:
    int val;

    explicit IntK3(int v) : val(v) {}

    bool operator<(const K3 &other) const override
    {
        return val < dynamic_cast<const IntK3 &>(other).val;
    }
};

class IntV3 : public V3
{
public:
    int val;

    explicit IntV3(int v) : val(v) {}
};

/*
========================================
Test Client
========================================
*/

class TestClient : public MapReduceClient
{
public:
    mutable std::atomic<int> mapCalls{0};
    mutable std::atomic<int> reduceCalls{0};

    bool duplicateKeys;
    int emitCount;
    bool randomSleep;

    TestClient(bool dup = false,
               int emits = 1,
               bool sleep = false)
        : duplicateKeys(dup),
          emitCount(emits),
          randomSleep(sleep)
    {
    }

    void map(const std::shared_ptr<K1> key,
             const std::shared_ptr<V1> value,
             MapContext &context) const override
    {
        (void)value;

        mapCalls++;

        if (randomSleep)
        {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(rand() % 3));
        }

        int base =
            dynamic_cast<IntK1 *>(key.get())->val;

        for (int i = 0; i < emitCount; i++)
        {
            int k = duplicateKeys ? 0 : (base * 100 + i);

            context.addIntermediate(
                std::make_shared<IntK2>(k),
                std::make_shared<IntV2>(1));
        }
    }

    void reduce(const IntermediateVec &pairs,
                ReduceContext &context) const override
    {
        reduceCalls++;

        if (randomSleep)
        {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(rand() % 3));
        }

        int key =
            dynamic_cast<IntK2 *>(pairs[0].first.get())->val;

        int sum = pairs.size();

        context.addOutput(
            std::make_shared<IntK3>(key),
            std::make_shared<IntV3>(sum));
    }
};

/*
========================================
Helpers
========================================
*/

InputVec makeInput(int n)
{
    InputVec input;

    for (int i = 0; i < n; i++)
    {
        input.push_back({
            std::make_shared<IntK1>(i),
            std::make_shared<IntV1>(i)
        });
    }

    return input;
}

/*
========================================
Tests
========================================
*/

void test_empty_input()
{
    TestClient client;

    InputVec input;

    MapReduceJob job(client, input, 4);

    job.wait();

    auto out = job.getOutput();

    assert(out.empty());
    assert(job.isDone());

    std::cout << "PASS empty input\n";
}

void test_single_thread()
{
    TestClient client;

    auto input = makeInput(100);

    MapReduceJob job(client, input, 1);

    job.wait();

    assert(client.mapCalls == 100);

    auto out = job.getOutput();

    assert(out.size() == 100);

    std::cout << "PASS single thread\n";
}

void test_more_threads_than_inputs()
{
    TestClient client;

    auto input = makeInput(3);

    MapReduceJob job(client, input, 100);

    job.wait();

    assert(client.mapCalls == 3);

    auto out = job.getOutput();

    assert(out.size() == 3);

    std::cout << "PASS more threads than inputs\n";
}

void test_duplicate_keys()
{
    TestClient client(true);

    auto input = makeInput(1000);

    MapReduceJob job(client, input, 8);

    job.wait();

    assert(client.reduceCalls == 1);

    auto out = job.getOutput();

    assert(out.size() == 1);

    auto v =
        dynamic_cast<IntV3 *>(out[0].second.get())->val;

    assert(v == 1000);

    std::cout << "PASS duplicate keys\n";
}

void test_many_emits()
{
    TestClient client(false, 50);

    auto input = makeInput(100);

    MapReduceJob job(client, input, 8);

    job.wait();

    auto out = job.getOutput();

    assert(out.size() == 5000);

    std::cout << "PASS many emits\n";
}

void test_concurrent_wait()
{
    TestClient client(false, 10, true);

    auto input = makeInput(1000);

    MapReduceJob job(client, input, 8);

    std::thread t1([&]() {
        job.wait();
    });

    std::thread t2([&]() {
        job.wait();
    });

    t1.join();
    t2.join();

    assert(job.isDone());

    std::cout << "PASS concurrent wait\n";
}

void test_get_state()
{
    TestClient client(false, 100, true);

    auto input = makeInput(500);

    MapReduceJob job(client, input, 8);

    while (!job.isDone())
    {
        auto s = job.getState();

        assert(s.percentage >= 0.0);
        assert(s.percentage <= 100.0);
    }

    job.wait();

    std::cout << "PASS getState polling\n";
}

void test_concurrent_get_output()
{
    TestClient client(false, 10, true);

    auto input = makeInput(500);

    MapReduceJob job(client, input, 8);

    std::thread t1([&]() {
        job.getOutput();
    });

    std::thread t2([&]() {
        job.getOutput();
    });

    t1.join();
    t2.join();

    std::cout << "PASS concurrent getOutput\n";
}

void test_destructor_waits()
{
    TestClient client(false, 100, true);

    auto input = makeInput(1000);

    {
        MapReduceJob job(client, input, 8);
    }

    std::cout << "PASS destructor waits\n";
}

void test_stress()
{
    for (int i = 0; i < 100; i++)
    {
        TestClient client(
            rand() % 2,
            1 + rand() % 20,
            true);

        auto input =
            makeInput(1 + rand() % 1000);

        MapReduceJob job(
            client,
            input,
            1 + rand() % 16);

        job.wait();

        assert(job.isDone());
    }

    std::cout << "PASS stress test\n";
}

/*
========================================
Main
========================================
*/

int main()
{
    srand(time(nullptr));

    test_empty_input();
    test_single_thread();
    test_more_threads_than_inputs();
    test_duplicate_keys();
    test_many_emits();
    test_concurrent_wait();
    test_get_state();
    test_concurrent_get_output();
    test_destructor_waits();
    test_stress();

    std::cout << "\nALL TESTS PASSED\n";

    return 0;
}