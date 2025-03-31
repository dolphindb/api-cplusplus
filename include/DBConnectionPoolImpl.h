// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include "TaskStatusMgmt.h"

namespace dolphindb {

class DBConnectionPoolImpl{
public:
    struct Task{
        Task(const std::string& sc = "", int id = 0, int pr = 4, int pa = 2, int fetchSize = 0, bool clearM = false)
                : script(sc), identity(id), priority(pr), parallelism(pa), fetchSize(fetchSize), clearMemory(clearM) {}
        Task(const std::string& function, const std::vector<ConstantSP>& args, int id = 0, int pr = 4, int pa = 2, int fetchSize = 0, bool clearM = false)
            : script(function), arguments(args), identity(id), priority(pr), parallelism(pa), fetchSize(fetchSize), clearMemory(clearM){ isFunc = true; }
        std::string script;
        std::vector<ConstantSP> arguments;
        int identity;
        int priority;
        int parallelism;
        int fetchSize;
        bool clearMemory;
        bool isFunc = false;
    };
    
    DBConnectionPoolImpl(const std::string& hostName, int port, int threadNum = 10, const std::string& userId = "", const std::string& password = "",
        bool loadBalance = true, bool highAvailability = true, bool compress = false, bool reConnect=false, bool python = false);
    
    ~DBConnectionPoolImpl(){
        shutDown();
        Task emptyTask;
        for (size_t i = 0; i < workers_.size(); i++)
            queue_->push(emptyTask);
        for (auto& work : workers_) {
            work->join();
        }
    }
    void run(const std::string& script, int identity, int priority=4, int parallelism=64, int fetchSize=0, bool clearMemory = false){
        queue_->push(Task(script, identity, priority, parallelism, fetchSize, clearMemory));
        taskStatus_.setResult(identity, TaskStatusMgmt::Result());
    }

    void run(const std::string& functionName, const std::vector<ConstantSP>& args, int identity, int priority=4, int parallelism=64, int fetchSize=0, bool clearMemory = false){
        queue_->push(Task(functionName, args, identity, priority, parallelism, fetchSize, clearMemory));
        taskStatus_.setResult(identity, TaskStatusMgmt::Result());
    }

    bool isFinished(int identity){
        return taskStatus_.isFinished(identity);
    }

    ConstantSP getData(int identity){
        return taskStatus_.getData(identity);
    }

    void shutDown(){
        shutDownFlag_.store(true);
        latch_->wait();
    }

    bool isShutDown(){
        return shutDownFlag_.load();
    }

    int getConnectionCount(){
        return static_cast<int>(workers_.size());
    }
private:
    std::atomic<bool> shutDownFlag_;
    CountDownLatchSP latch_;
    std::vector<ThreadSP> workers_;
    SmartPointer<SynchronizedQueue<Task>> queue_;
    TaskStatusMgmt taskStatus_;
};

}