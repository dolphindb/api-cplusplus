/*
 * ConstantFactory.h
 *
 *  Created on: Jan 19, 2013
 *      Author: dzhou
 */

#ifndef DOMAIN_H
#define DOMAIN_H

#include "DolphinDB.h"
#include "ConstantImp.h"

namespace dolphindb{

class HashDomain : public Domain{
public:
    HashDomain(DATA_TYPE partitionColType, ConstantSP partitionSchema) : Domain(HASH, partitionColType){
        buckets_ = partitionSchema->getInt();
    }

	virtual vector<int> getPartitionKeys(const ConstantSP& partitionCol) const;

private:
    int buckets_;
};

class ListDomain : public Domain {
public:
    ListDomain(DATA_TYPE partitionColType, ConstantSP partitionSchema);

    virtual vector<int> getPartitionKeys(const ConstantSP& partitionCol) const;

private:
	DictionarySP dict_;
};


class ValueDomain : public Domain{
public:
	ValueDomain(DATA_TYPE partitionColType, ConstantSP partitionSchema) : Domain(VALUE, partitionColType){}
	
	virtual vector<int> getPartitionKeys(const ConstantSP& partitionCol) const;
};

class RangeDomain : public Domain{
public:
    RangeDomain(DATA_TYPE partitionColType, ConstantSP partitionSchema) : Domain(RANGE, partitionColType), range_(partitionSchema){ }
	
	virtual vector<int> getPartitionKeys(const ConstantSP& partitionCol) const;
private:
    VectorSP range_;
};

}

#endif /* TABLE_H_ */