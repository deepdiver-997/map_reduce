#pragma once

#include <utility>
#include <vector>
#include <numeric>
#include <thread>
#include <future>
#include <functional>
#include <iostream>
#include <algorithm>

namespace map_reduce {

class thread_pool {
private:
    thread_pool(size_t threadsCount) : threadsCount_(threadsCount), workingCount_(0) {
        
    }
public:
    thread_pool(const thread_pool&) = delete;
    thread_pool& operator=(const thread_pool&) = delete;
    ~thread_pool() {
        join();
    }

    void wait_all() {
        std::unique_lock<std::mutex> lock(queueMutex_);
        condition_.wait(lock, [this] {
            return tasks_.empty() && workingCount_ == 0;
        });
    }

    void enqueue(const std::function<void()>& task, int owner = -1) {
        {
            std::unique_lock<std::mutex> lock(queueMutex_);
            tasks_.emplace(owner, task);
        }
        condition_.notify_one();
    }
    
    void join(){
        if(stop_) return;
        stop_ = true;
        {
            std::unique_lock<std::mutex> lock(queueMutex_);
            condition_.wait(lock, [this] {
                return workingCount_ == 0;
            });
        }
        condition_.notify_all();
        for (auto& thread : workers_) {
            thread.join();
        }
    }

    void run() {
        for (size_t i = 0; i < threadsCount_; ++i) {
            workers_.emplace_back([this, i] {
                while (true) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(this->queueMutex_);
                        this->condition_.wait(lock, [this, i] {
                            return this->stop_ || !this->tasks_.empty() && 
                            (this->tasks_.front().first == -1 || this->tasks_.front().first == i);
                        });
                        if (this->stop_ && this->tasks_.empty())
                            return;
                        // if (this->tasks_.front().first != -1 && this->tasks_.front().first != i) {
                        //     // If the task is not for this thread, skip it
                        //     continue;    导致一直忙等
                        // }
                        task = std::move(this->tasks_.front().second);
                        this->tasks_.pop();
                        workingCount_++;
                    }
                    task();
                    --workingCount_;
                    condition_.notify_all();
                }
            });
        }
    }

    size_t threadCount() {
        return threadsCount_;
    }

    size_t queuedTasks() {
        std::lock_guard<std::mutex> lock(queueMutex_);
        return tasks_.size();
    }

    int workingCount() {
        return workingCount_.load();
    }

    static thread_pool& Instance(size_t threadsCount = std::thread::hardware_concurrency()) {
        static thread_pool instance(threadsCount);
        return instance;
    }
private:
    std::vector<std::thread> workers_;
    std::queue<std::pair<int, std::function<void()>>> tasks_;
    std::mutex queueMutex_;
    std::condition_variable condition_;
    bool stop_ = false;
    size_t threadsCount_;
    std::atomic<int> workingCount_;
};

template <typename K, typename V>
class Mapper {
public:
    virtual std::vector<std::pair<K, V>> map(const std::vector<K>& input) = 0;
    // virtual std::vector<std::pair<K, V>> map(const std::vector<std::pair<K,V>>& input) = 0;
    virtual ~Mapper() = default;
};

template <typename K, typename V>
class Reducer {
public:
    virtual V reduce(const K& key, const std::vector<V>& input) = 0;
    // void final_sort(std::vector<std::pair<K, V>>&) = 0;
    virtual ~Reducer() = default;
};

template <typename K>
class Divider {
public:
    Divider() = default;
    virtual std::vector<std::vector<K>> divide(const std::vector<K>& input, size_t chunks_size) = 0;
    virtual ~Divider() = default;
protected:
};

struct Config {
    bool is_distributed;
    bool is_main;
    int threadsCount;
    size_t chunks_size;
};

template <typename K, typename V>
class MapReduce {
public:
    MapReduce(Config config, Mapper<K, V>* mapper, Reducer<K, V>* reducer, Divider<K>* divider)
        : config_(std::move(config)), mapper_(mapper), reducer_(reducer), divider_(divider), pool_(thread_pool::Instance(config.threadsCount)) {
            pool_.run();
        }

    ~MapReduce() {
        if (mapper_) delete mapper_;
        if (reducer_) delete reducer_;
        if (divider_) delete divider_;
    }

    void run(const std::vector<K>& input, std::vector<std::pair<K, V>>& output) {
        if (config_.is_distributed) {
            runDistributed(input, output);
        } else {
            std::cout << "Running in local mode...\n";
            runLocal(input, output);
        }
    }

private:
    Config config_;
    Divider<K>* divider_;
    Mapper<K, V>* mapper_;
    Reducer<K, V>* reducer_;
    thread_pool& pool_;
    K key_;
    std::mutex mtx;

    void runDistributed(const std::vector<K>& input, std::vector<std::pair<K, V>>& output);
    void runLocal(const std::vector<K>& input, std::vector<std::pair<K, V>>& output);
    std::vector<V> receiveDataFromWorkers() {
        // This function is a placeholder for receiving data from distributed workers.
        // In a real implementation, it would handle network communication to gather results.
        return {};
    }
    void sendToMain() {
        // Placeholder for distributed mode
    }
    // void map(const std::vector<V>& input,
    //          const std::function<void(std::pair<K, V>)>& outputFunc);
    // void reduce(const std::function<V(K, V, size_t)>& func,
    //             const std::function<void(std::pair<K, V>)>& outputFunc);
};

template <typename K, typename V>
void MapReduce<K, V>::runLocal(const std::vector<K>& input, std::vector<std::pair<K, V>>& output) {
    std::vector<std::vector<K>> chunks = divider_->divide(input, config_.chunks_size);
    std::vector<std::vector<std::pair<K, V>>> mappedResults;
    std::cout << "mappedResults.size : " << mappedResults.size() << std::endl;
    for(auto& v : chunks) {
        pool_.enqueue([&](){
            std::cout << "Thread " << std::this_thread::get_id() 
                      << " is working on a task. Working count: " << pool_.workingCount() << std::endl;
            auto results = mapper_->map(v);
            std::lock_guard<std::mutex> lock(mtx);
            mappedResults.push_back(results);
            std::cout << "mappedResults.size after down: " << mappedResults.size() << std::endl;
            // failed to exit this task
        });
    }

    // auto wait_for_finish = [&](){
    //     std::condition_variable cv;
    //     std::unique_lock<std::mutex> lock(mtx);
    //     cv.wait(lock, [&] { 
    //         bool done = (pool_.workingCount() == 0);
    //         if (done) {
    //             std::cout << "All tasks completed.\n";
    //         }
    //         return done;
    //     });
    // };       线程池唤醒工作线程需要时间，使用此函数可能会在任务还未开始前就判断完成，导致和后面的wait_for_finish产生死等
    pool_.wait_all();
    std::cout << "mapping is complete\nworkingCount : " << pool_.workingCount() << std::endl;
    std::cout << "mappedResults.size after wait: " << mappedResults.size() << std::endl;

    std::unordered_map<K, std::vector<V>> groupedResults;
    for (const auto& result : mappedResults) {
        for (const auto& pair : result) {
            groupedResults[pair.first].push_back(pair.second);
        }
    }

    if(!config_.is_distributed) {
        for(auto& map : groupedResults) {
            pool_.enqueue([&]() {
                std::cout << "Thread " << std::this_thread::get_id() 
                          << " is working on a task. Working count: " << pool_.workingCount() << std::endl;
                if(reducer_ == nullptr){
                    std::cerr << "reducer_ is nullptr\n";
                    return;
                }
                auto reducedResult = reducer_->reduce(map.first, map.second);
                std::lock_guard<std::mutex> lock(mtx);
                output.emplace_back(map.first, reducedResult);
            });
        }
        pool_.wait_all();
        std::cout << "reduction is complete\n";
        //     for(const auto& pair : output) {
        //     std::cout << pair.first << ": " << pair.second << std::endl;
        // }
        return;
    } else {
            std::vector<V> reducedResults;
            reducedResults = receiveDataFromWorkers();
            for(auto& reduced : reducedResults) {
                pool_.enqueue([&](){
                    std::cout << "Thread " << std::this_thread::get_id() 
                              << " is working on a task. Working count: " << pool_.workingCount() << std::endl;
                    std::lock_guard<std::mutex> lock(mtx);
                    output.emplace_back(key_, reduced);
                });
            }
            pool_.wait_all();
            sendToMain();
    }
}

template <typename K, typename V>
void MapReduce<K, V>::runDistributed(const std::vector<K>& input, std::vector<std::pair<K, V>>& output) {
    if (config_.is_main) {
        std::cout << "Distributed mode is not implemented yet.\n";
        return;
    }
    else {
        runLocal(input, output);
    }
}

} // namespace map_reduce
