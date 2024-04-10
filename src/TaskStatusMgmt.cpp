#include "TaskStatusMgmt.h"

namespace dolphindb {

bool TaskStatusMgmt::isFinished(int identity){
    LockGuard<Mutex> guard(&mutex_);
    if(results.count(identity) == 0)
        throw RuntimeException("Task [" + std::to_string(identity) + "] does not exist.");
    if(results[identity].stage == ERRORED)
        throw RuntimeException("Task [" + std::to_string(identity) + "] come across exception : " + results[identity].errMsg);
    return results[identity].stage == FINISHED;
}

void TaskStatusMgmt::setResult(int identity, Result r){
    LockGuard<Mutex> guard(&mutex_);
    results[identity] = r;
}

ConstantSP TaskStatusMgmt::getData(int identity){
    LockGuard<Mutex> guard(&mutex_);
    if(results.count(identity) == 0)
        throw RuntimeException("Task [" + std::to_string(identity) + "] does not exist, the result may be fetched yet.");
    assert(results[identity].stage == FINISHED);
    ConstantSP re = results[identity].result;
    results.erase(identity);
    return re;
}

}