// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include "Domain.h"
#include "Dictionary.h"
#include "Constant.h"

namespace dolphindb{

class HashDomain : public Domain{
public:
    HashDomain(DATA_TYPE partitionColType, ConstantSP partitionSchema) : Domain(HASH, partitionColType){
        buckets_ = partitionSchema->getInt();
    }

	virtual std::vector<int> getPartitionKeys(const ConstantSP& partitionCol) const;

private:
    int buckets_;
};

class ListDomain : public Domain {
public:
    ListDomain(DATA_TYPE partitionColType, ConstantSP partitionSchema);

    virtual std::vector<int> getPartitionKeys(const ConstantSP& partitionCol) const;

private:
	DictionarySP dict_;
};


class ValueDomain : public Domain{
public:
	ValueDomain(DATA_TYPE partitionColType, ConstantSP partitionSchema) : Domain(VALUE, partitionColType){ std::ignore = partitionSchema; }
	
	virtual std::vector<int> getPartitionKeys(const ConstantSP& partitionCol) const;
};

class RangeDomain : public Domain{
public:
    RangeDomain(DATA_TYPE partitionColType, ConstantSP partitionSchema) : Domain(RANGE, partitionColType), range_(partitionSchema){ }
	
	virtual std::vector<int> getPartitionKeys(const ConstantSP& partitionCol) const;
private:
    VectorSP range_;
};

}
