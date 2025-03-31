// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include "Constant.h"
#include "Util.h"
namespace dolphindb {

class EXPORT_DECL Domain{
public:
    Domain(PARTITION_TYPE partitionType, DATA_TYPE partitionColType): partitionType_(partitionType), partitionColType_(partitionColType){
        partitionColCategory_ = Util::getCategory(partitionColType_);
    }
    virtual ~Domain(){}
    virtual std::vector<int> getPartitionKeys(const ConstantSP& partitionCol) const = 0;
    virtual PARTITION_TYPE getPartitionType(){
        return partitionType_;
    }
protected:
    PARTITION_TYPE partitionType_;
    DATA_TYPE partitionColType_;
    DATA_CATEGORY partitionColCategory_;
};

}