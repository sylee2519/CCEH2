#include "src/CCEH.h"
#include <unistd.h>
#include <cstdlib>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <thread>
#include <vector>
#include <ctime>

using namespace std;

void clear_cache(){
    int* dummy = new int[1024*1024*256];
    for(int i=0; i<1024*1024*256; i++){
	dummy[i] = i;
    }

    for(int i=100; i<1024*1024*256-100; i++){
	dummy[i] = dummy[i-rand()%100] + dummy[i+rand()%100];
    }

    delete[] dummy;
}


int main(int argc, char* argv[]){
    const size_t initialTableSize = 16*1024;
    size_t numData = atoi(argv[1]);
	size_t numShards;
#ifdef MULTITHREAD
    size_t numThreads = atoi(argv[2]);
	numShards = atoi(argv[3]); //number of shard as input
#else
	numShards = 1;
#endif
	
    struct timespec start, end;
    uint64_t* keys = (uint64_t*)malloc(sizeof(uint64_t)*numData);

    ifstream ifs;
//    string dataset = "/home/chahg0129/dataset/input_rand.txt";
	string dataset = "/home/seoyeong/CCEH/util/data";
	ifs.open(dataset);
    if(!ifs){
	cerr << "no file" << endl;
	return 0;
    }

    cout << dataset << " is used" << endl;
    for(int i=0; i<numData; i++){
	ifs >> keys[i];
    }
    cout << "Reading dataset Completed" << endl;

	Hash *shard0, *shard1,*shard2, *shard3, *shard4, *shard5;

    switch(numShards){
        case 1:
            shard0 = new CCEH(initialTableSize/Segment:: kNumSlot);
            break;
        case 2:
            shard0 = new CCEH(initialTableSize/Segment:: kNumSlot);
            shard1 = new CCEH(initialTableSize/Segment:: kNumSlot);
            break;
        case 3:
            shard0 = new CCEH(initialTableSize/Segment:: kNumSlot);
            shard1 = new CCEH(initialTableSize/Segment:: kNumSlot);
            shard2 = new CCEH(initialTableSize/Segment:: kNumSlot);
            break;
        case 4:
            shard0 = new CCEH(initialTableSize/Segment:: kNumSlot);
            shard1 = new CCEH(initialTableSize/Segment:: kNumSlot);
            shard2 = new CCEH(initialTableSize/Segment:: kNumSlot);
            shard3 = new CCEH(initialTableSize/Segment:: kNumSlot);
            break;
        case 5:
            shard0 = new CCEH(initialTableSize/Segment:: kNumSlot);
            shard1 = new CCEH(initialTableSize/Segment:: kNumSlot);
            shard2 = new CCEH(initialTableSize/Segment:: kNumSlot);
            shard3 = new CCEH(initialTableSize/Segment:: kNumSlot);
            shard4 = new CCEH(initialTableSize/Segment:: kNumSlot);
            break;
        case 6:
            shard0 = new CCEH(initialTableSize/Segment:: kNumSlot);
            shard1 = new CCEH(initialTableSize/Segment:: kNumSlot);
            shard2 = new CCEH(initialTableSize/Segment:: kNumSlot);
            shard3 = new CCEH(initialTableSize/Segment:: kNumSlot);
            shard4 = new CCEH(initialTableSize/Segment:: kNumSlot);
            shard5 = new CCEH(initialTableSize/Segment:: kNumSlot);
            break;
    }

    cout << "Hashtable Initialized" << endl;

#ifndef MULTITHREAD
    cout << "Start Insertion" << endl;
    clear_cache();
    clock_gettime(CLOCK_MONOTONIC, &start);
    for(int i=0; i<numData; i++){
	shard0->Insert(keys[i], reinterpret_cast<Value_t>(keys[i]));
    }
    clock_gettime(CLOCK_MONOTONIC, &end);

    cout << "NumData(" << numData << ")" << endl;
    uint64_t elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    cout << "Insertion: " << elapsed/1000 << " usec\t" << (uint64_t)(1000000*(numData/(elapsed/1000.0))) << " ops/sec" << endl;

    cout << "Start Search" << endl;
    clear_cache();
    int failedSearch = 0;
    clock_gettime(CLOCK_MONOTONIC, &start);
    for(int i=0; i<numData; i++){
	auto ret = shard0->Get(keys[i]);
	if(ret != reinterpret_cast<Value_t>(keys[i]))
	    failedSearch++;
    }
    clock_gettime(CLOCK_MONOTONIC, &end);

    elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    cout << "Search: " << elapsed/1000 << " usec\t" << (uint64_t)(1000000*(numData/(elapsed/1000.0))) << " ops/sec" << endl;
    cout << "failedSearch: " << failedSearch << endl;
#else
    vector<thread> insertingThreads;
    vector<thread> searchingThreads;
    vector<int> failed(numThreads);

    auto insert = [&shard0, &shard1, &shard2, &shard3, &shard4, &shard5, &keys, &numShards](int from, int to){
	for(int i=from; i<to; i++){
		size_t shardid =keys[i]%numShards;
		switch(shardid){
			case 0:
				shard0->Insert(keys[i], reinterpret_cast<Value_t>(keys[i]));
				break;
			case 1:
				shard1->Insert(keys[i], reinterpret_cast<Value_t>(keys[i]));
				break;
			case 2:
				shard2->Insert(keys[i], reinterpret_cast<Value_t>(keys[i]));
				break;
			case 3:
				shard3->Insert(keys[i], reinterpret_cast<Value_t>(keys[i]));
				break;
			case 4:
				shard4->Insert(keys[i], reinterpret_cast<Value_t>(keys[i]));
				break;
			case 5:
				shard5->Insert(keys[i], reinterpret_cast<Value_t>(keys[i]));
				break;
		}
	}
    };
    
    auto search = [&shard0, &shard1, &shard2, &shard3, &shard4, &shard5, &keys, &failed, &numShards](int from, int to, int tid){
	int fail = 0;
	for(int i=from; i<to; i++){
		size_t shardid = keys[i]%numShards;
		switch(shardid){
			case 0:{
				auto ret = shard0->Get(keys[i]);
				if(ret != reinterpret_cast<Value_t>(keys[i])){
					fail++;
				}
				break;
				   }
            case 1:{
                auto ret1 = shard1->Get(keys[i]);
                if(ret1 != reinterpret_cast<Value_t>(keys[i])){
                    fail++;
                }   
                break;
				   }
            case 2:{
                auto ret2 = shard2->Get(keys[i]);
                if(ret2 != reinterpret_cast<Value_t>(keys[i])){
                    fail++;
                }   
                break;
				   }
            case 3:{
                auto ret3 = shard3->Get(keys[i]);
                if(ret3 != reinterpret_cast<Value_t>(keys[i])){
                    fail++;
                }   
                break;
				   }
            case 4:{
                auto ret4 = shard4->Get(keys[i]);
                if(ret4 != reinterpret_cast<Value_t>(keys[i])){
                    fail++;
                }   
                break;
				   }
            case 5:{
                auto ret5 = shard5->Get(keys[i]);
                if(ret5 != reinterpret_cast<Value_t>(keys[i])){
                    fail++;
                }   
                break;
				   }
		}
	}
	failed[tid] = fail;
    };

    cout << "Start Insertion" << endl;
    clear_cache();
    const size_t chunk = numData/numThreads;
    clock_gettime(CLOCK_MONOTONIC, &start);
    for(int i=0; i<numThreads; i++){
	if(i != numThreads-1)
	    insertingThreads.emplace_back(thread(insert, chunk*i, chunk*(i+1)));
	else
	    insertingThreads.emplace_back(thread(insert, chunk*i, numData));
    }

    for(auto& t: insertingThreads) t.join();
    clock_gettime(CLOCK_MONOTONIC, &end);
    cout << "NumData(" << numData << "), numThreads(" << numThreads << "), numShards(" << numShards << ")" << endl;
    uint64_t elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    cout << "Insertion: " << elapsed/1000 << " usec\t" << (uint64_t)(1000000*(numData/(elapsed/1000.0))) << " ops/sec" << endl;

    cout << "Start Search" << endl;
    clear_cache();
    clock_gettime(CLOCK_MONOTONIC, &start);
    for(int i=0; i<numThreads; i++){
	if(i != numThreads-1)
	    searchingThreads.emplace_back(thread(search, chunk*i, chunk*(i+1), i));
	else
	    searchingThreads.emplace_back(thread(search, chunk*i, numData, i));
    }

    for(auto& t: searchingThreads) t.join();
    clock_gettime(CLOCK_MONOTONIC, &end);
    elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    cout << "Search: " << elapsed/1000 << " usec\t" << (uint64_t)(1000000*(numData/(elapsed/1000.0))) << " ops/sec" << endl;


    int failedSearch = 0;
    for(auto& v: failed) failedSearch += v;
    cout << failedSearch << " failedSearch" << endl;
#endif

    auto util = shard0->Utilization();
    auto cap = shard0->Capacity();

    cout << "Shard0 Util( " << util << " ), Capacity( " << cap << " )" << endl;
	
	if(numShards !=1){
		auto util2 = shard1->Utilization();
		auto cap2 = shard1->Capacity();
		cout << "Shard1 Util( " << util2 << " ), Capacity( " << cap2 << " )" << endl;
	}
    if(numShards == 4){
        auto util3 = shard2->Utilization();
        auto cap3 = shard2->Capacity();
		auto util4 = shard3->Utilization();
        auto cap4 = shard3->Capacity();

        cout << "Shard2 Util( " << util3 << " ), Capacity( " << cap3 << " )" << endl;
		cout << "Shard3 Util( " << util4 << " ), Capacity( " << cap4 << " )" << endl;

	}	

    return 0;
}

