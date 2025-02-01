#ifndef __C_MapReduce__
#define __C_MapReduce__
/*
		TODO :

				1. First mapping threads map
				2. then reducing threads, sort and then do reducing.

				1) mapping :

						library should divide the input to send to all the threads via a sechduling algo 
						each thread then needs to map the data to some intermidiate key and value based on user map function 
						then all these intermidate key,value pairs should be stored in the mr class for processing and reducing with the help of library provided function. 
						And this storing function should partition the storing data based on the intermidiate key and the partitioning function 

				2) reducing :

						first each thread will sort the data based on intermidiate key,value 
						then while traversing the data it sends this key and the iterator on the values to the user given reduce function.

		
		Assumptions : 
				1) num_of_partitions == num_reduce_threads.
				2) The inputs given are roughly of same size. (for performance)

*/

#include <algorithm>
#include <functional>
#include <map>
#include <memory>
#include <semaphore>
#include <thread>
#include <utility>
#include <vector>

template <typename k2, typename v2, typename inp_t>
class MapReduce {
 private:
	std::vector<std::unique_ptr<std::vector<std::pair<k2, v2>>>> partition_space;	//holds all the key,value pairs of the partition space
	std::vector<std::unique_ptr<std::counting_semaphore<100>>> partition_lock;		// lock for each partition space
	std::counting_semaphore<100> inp_lock;											// lock to access input list concurrently	
	std::vector<inp_t> inp;
	int to_proc_idx{}, inp_len{};

 public:
	using map_ft = std::function<std::vector<std::pair<k2, v2>>(inp_t&)>;
	using red_ft = std::function<void(k2, typename std::vector<v2>::iterator, typename std::vector<v2>::iterator)>;
	using part_ft = std::function<int(k2)>;

	MapReduce(std::vector<inp_t> inp, int map_cnt, map_ft map, int red_cnt, red_ft reduce, part_ft partition_fun) : inp{inp}, inp_len{(int)inp.size()}, inp_lock(1) {

		//initializing 
		for (int i{}; i < red_cnt; i++) {                                                 
			partition_space.push_back(std::make_unique<std::vector<std::pair<k2, v2>>>());
			partition_lock.push_back(std::make_unique<std::counting_semaphore<100>>(1));
		}

		// map section
		std::vector<std::thread> map_threads;											
		for (int i{}; i < map_cnt; i++) map_threads.emplace_back(&MapReduce::map_thread_fun, this, map, partition_fun);       // threads are created 
		for (auto& t : map_threads) t.join();      //waiting for threads to finish

		// reduce section
		std::vector<std::thread> reduce_threads;
		for (int partition{}; partition < red_cnt; partition++) reduce_threads.emplace_back(&MapReduce::reduce_thread_fun, this, partition, reduce);
		for (auto& t : reduce_threads) t.join();
	}

 private:
	void map_thread_fun(map_ft map, part_ft partition_fun) {

		while (true) {
			inp_lock.acquire(); 		//acquiring lock to allocate one input to the thread

			if (to_proc_idx == inp_len) {    //every input is processed, so release lock and finsh thread 
				inp_lock.release();
				return;
			}

			int cur_idx = to_proc_idx++;	// allocating the input 
			inp_lock.release();

			std::vector<std::pair<k2, v2>> key_values = map(inp[cur_idx]);  // processing input

			for (auto [key, value] : key_values) { 			//storing key,value pairs in partition space
				store(key, value, partition_fun);
			}
		}
	};

	void store(k2 key, v2 value, part_ft partition_fun) {
		int partition = partition_fun(key);
		partition_lock[partition]->acquire(); 						//acquiring lock to push the pair to partition space
		(partition_space[partition])->push_back({key, value});
		partition_lock[partition]->release();
	}

	void reduce_thread_fun(int partition, red_ft reduce) {
		std::map<k2, std::shared_ptr<std::vector<v2>>> value_list;								 
		for (auto [key, value] : *partition_space[partition]) {
			if (value_list.count(key)) 									
				(value_list[key])->push_back(value);					
			else														// this is first value of this key hence we create new value list for this key  
				value_list.insert({key , std::make_shared<std::vector<v2>>(1, value)});
		}

		for (auto [key, ptr] : value_list) {
			std::sort(ptr->begin(), ptr->end());						//sorting values before sending to user reduce.
			reduce(key, ptr->begin(), ptr->end());
		}
	}
};

#endif