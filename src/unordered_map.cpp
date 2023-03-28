#include <iostream>
#include <unordered_map>
#include <cstdlib>
#include <vector>
#include <ctime>
#include <fstream>
#include <unistd.h>
#include <algorithm>
#include <thread>
#include <mutex>

using namespace std;

typedef size_t Key_t;
typedef const char* Value_t;

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
	
	mutex m;
	size_t numData = atoi(argv[1]);
	size_t numThreads = atoi(argv[2]);

	struct timespec start, end;
    uint64_t* keys = (uint64_t*)malloc(sizeof(uint64_t)*numData);

    ifstream ifs;
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
	
	unordered_map<Key_t, Value_t> umap;

	vector<thread> insertingThreads;
	vector<thread> searchingThreads;
	vector<int> failed(numThreads);

	auto insert = [&m, &umap, &keys, &numData](int from, int to){
		for(int i=0; i<numData; i++){
			m.lock();
			umap.insert(make_pair(keys[i], reinterpret_cast<Value_t>(keys[i])));
			m.unlock();
		}
	};
	
	auto search = [&umap, &keys, &numData, &failed](int from, int to, int tid){
		int fail = 0;
		for(int i=0; i<numData; i++){
			auto ret = umap.at(keys[i]);
			if(ret != reinterpret_cast<Value_t>(keys[i]))
				fail++;
		}
		failed[tid] = fail;
	};

    cout << "Start Insertion" << endl;
	clear_cache();
	const size_t chunk = numData/numThreads;
    clock_gettime(CLOCK_MONOTONIC, &start);

	for(int i = 0; i<numThreads; i++){
		if(i!= numThreads-1)
			insertingThreads.emplace_back(thread(insert, chunk*i, chunk*(i+1)));
		else
			insertingThreads.emplace_back(thread(insert, chunk*i, numData));
	}
	for(auto& t:insertingThreads) t.join();

	clock_gettime(CLOCK_MONOTONIC, &end);
	cout << "NumData(" << numData << "), numThreads(" << numThreads << ")" << endl;
    uint64_t elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    cout << "Insertion: " << elapsed/1000 << " usec\t" << (uint64_t)(1000000*(numData/(elapsed/1000.0))) << " ops/sec" << endl;

    cout << "Start Search" << endl;
	clear_cache();
    clock_gettime(CLOCK_MONOTONIC, &start);

	for(int i = 0; i<numThreads; i++){
		if(i!= numThreads-1)
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

	return 0;
}
