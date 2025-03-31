// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include "Exports.h"
#include "Constant.h"

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4251 )
#endif

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
    static std::string formatApiCode(int code){
        if(code != EC_None)
            return "A" + std::to_string(code);
        else
            return "";
    }
    void set(int apiCode, const std::string &info);
    void set(const std::string &code, const std::string &info);
    void set(const ErrorCodeInfo &src);
    std::string errorCode;
    std::string errorInfo;
};

}

#ifdef _MSC_VER
#pragma warning( pop )
#endif
