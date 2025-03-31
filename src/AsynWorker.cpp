#include "AsynWorker.h"
#include "DolphinDB.h"
#include "ScalarImp.h"
#include "SysIO.h"
#include <iostream>

namespace dolphindb {
    void AsynWorker::run() {
    while(true) {
        if(pool_.isShutDown()){
            conn_->close();
            latch_->countDown();
            std::cout<<"Asyn worker closed peacefully."<<std::endl;
            break;
        }

        Task task;
        ConstantSP result;
        bool errorFlag = false;
        if (!queue_->blockingPop(task, 1000))
            continue;
        if (task.script.empty())
            continue;
        while(true) {
            try {
                if(task.isFunc){
                    result = conn_->run(task.script, task.arguments, task.priority, task.parallelism, task.fetchSize, task.clearMemory);
                }
                else{
                    result = conn_->run(task.script, task.priority, task.parallelism, task.fetchSize, task.clearMemory);
                }
                break;
            }
            catch(IOException & ex){
                errorFlag = true;
                std::cerr<<"Async task worker come across exception : "<<ex.what()<<std::endl;
                taskStatus_.setResult(task.identity, TaskStatusMgmt::Result(TaskStatusMgmt::ERRORED, new Void(), ex.what()));
                break;
            }
        }
        if(!errorFlag)
            taskStatus_.setResult(task.identity, TaskStatusMgmt::Result(TaskStatusMgmt::FINISHED,  result));
    }
}
}