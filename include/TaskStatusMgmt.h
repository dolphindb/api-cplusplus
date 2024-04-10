#pragma once

#include "Constant.h"
#include "Concurrent.h"

namespace dolphindb {

class TaskStatusMgmt{
public:
    enum TASK_STAGE{WAITING, FINISHED, ERRORED};
    struct Result{
        Result(TASK_STAGE s = WAITING, const ConstantSP c = Constant::void_, const string &msg = "") : stage(s), result(c), errMsg(msg){}
        TASK_STAGE stage;
        ConstantSP result;
        string errMsg;
    };

    bool isFinished(int identity);
    ConstantSP getData(int identity);
    void setResult(int identity, Result);
private:
    Mutex mutex_;
    std::unordered_map<int, Result> results;
};

}