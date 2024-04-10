#pragma once

#include "Exports.h"
#include "Constant.h"
namespace dolphindb {

class EXPORT_DECL ErrorCodeInfo {
public:
    enum ErrorCode {
        EC_None = 0,
        EC_InvalidObject=1,
        EC_InvalidParameter=2,
        EC_InvalidTable=3,
        EC_InvalidColumnType=4,
        EC_Server=5,
        EC_UserBreak=6,
        EC_DestroyedObject=7,
        EC_Other=8,
    };
    ErrorCodeInfo() {
    }
    void clearError(){
        errorCode.clear();
    }
    bool hasError(){
        return errorCode.empty() == false;
    }
    bool succeed() {
        return errorCode.empty();
    }
    static string formatApiCode(int code){
        if(code != EC_None)
            return "A" + std::to_string(code);
        else
            return "";
    }
    void set(int apiCode, const string &info);
    void set(const string &code, const string &info);
    void set(const ErrorCodeInfo &src);
    string errorCode;
    string errorInfo;
};

}