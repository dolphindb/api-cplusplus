#include "DomainImp.h"
#include "Util.h"


namespace dolphindb{


vector<int> HashDomain::getPartitionKeys(const ConstantSP& partitionColTable) const {
    if(partitionColTable->getCategory() != partitionColCategory_)
        throw RuntimeException("Data category incompatible.");
	ConstantSP partitionCol = partitionColTable;
    if(partitionColCategory_ == TEMPORAL && partitionColType_ != partitionCol->getType()){
    	partitionCol = ((FastTemporalVector*)partitionCol.get())->castTemporal(partitionColType_);
    	if(partitionCol == NULL)
    		throw new RuntimeException("Can't convert partition column");
    }
    int rows = partitionCol->rows();
    vector<int> keys(rows);
    INDEX count = 0;
    INDEX start = 0;
    int *pbuf;
    while(start < rows){
        count = std::min(Util::BUF_SIZE, rows - start);
        pbuf = keys.data() + start;
        if(!partitionCol->getHash(start, count, buckets_, pbuf)){
            throw RuntimeException("Can't get the partition keys");
        }
        start += count;
    }
    return keys;
}

ListDomain::ListDomain(DATA_TYPE partitionColType, ConstantSP partitionSchema) : Domain(LIST, partitionColType){
    if(!partitionSchema->isVector()){
        throw RuntimeException("The input list must be a tuple.");
    }
    if(partitionColType_ == DT_SYMBOL)
        dict_ = Util::createDictionary(DT_STRING,DT_INT);
    else
        dict_ = Util::createDictionary(partitionColType_,DT_INT);
    int partitions = partitionSchema->size();
    for(int i = 0; i < partitions; i++){
        ConstantSP cur = partitionSchema->get(i);
        if(cur->isScalar()){
            dict_->set(cur, new Int(i));
        }
        else{
            for(int j=0; j<cur->size(); ++j){
                dict_->set(cur->get(j), new Int(i));
            }
        }
    }
}

vector<int> ListDomain::getPartitionKeys(const ConstantSP& partitionColTable) const {
    if(partitionColTable->getCategory() != partitionColCategory_)
        throw RuntimeException("Data category incompatible.");
	ConstantSP partitionCol = partitionColTable;
	if (partitionColCategory_ == TEMPORAL && partitionColType_ != partitionCol->getType()) {
		partitionCol = ((FastTemporalVector*)partitionCol.get())->castTemporal(partitionColType_);
		if (partitionCol == NULL)
			throw new RuntimeException("Can't convert partition column");
	}
    int rows = partitionCol->rows();
    vector<int> keys(rows);
    for(int i=0; i<rows; ++i){
        ConstantSP index = dict_->getMember(partitionCol->get(i));
        if(index->isNull())
            keys[i] = -1;
        else
            keys[i] = index->getInt();
    }
    return keys;
}
	
vector<int> ValueDomain::getPartitionKeys(const ConstantSP& partitionColTable) const {
    if(partitionColTable->getCategory() != partitionColCategory_)
        throw RuntimeException("Data category incompatible.");
	ConstantSP partitionCol = partitionColTable;
	if (partitionColCategory_ == TEMPORAL && partitionColType_ != partitionCol->getType()) {
		partitionCol = ((FastTemporalVector*)partitionCol.get())->castTemporal(partitionColType_);
		if (partitionCol == NULL)
			throw new RuntimeException("Can't convert partition column");
	}
    if(partitionColType_ == DT_LONG)
        throw RuntimeException("Long type value can't be used as a partition column.");
    int rows = partitionCol->rows();
    vector<int> keys(rows);
    INDEX count = 0;
    INDEX start = 0;
    int *pbuf;
    while(start < rows){
        count = std::min(Util::BUF_SIZE, rows - start);
        pbuf = keys.data() + start;
        if(!partitionCol->getHash(start, count, 1048576, pbuf)){
            throw RuntimeException("Can't get the partition keys");
        }
        start += count;
    }
    return keys;
}

vector<int> RangeDomain::getPartitionKeys(const ConstantSP& partitionColTable) const {
    if(partitionColTable->getCategory() != partitionColCategory_)
        throw RuntimeException("Data category incompatible.");
	ConstantSP partitionCol = partitionColTable;
	if (partitionColCategory_ == TEMPORAL && partitionColType_ != partitionCol->getType()) {
		partitionCol = ((FastTemporalVector*)partitionCol.get())->castTemporal(partitionColType_);
		if (partitionCol == NULL)
			throw new RuntimeException("Can't convert partition column");
	}
    int rows = partitionCol->rows();
    int partitions = range_->size() - 1;
    vector<int> keys(rows);
    for(int i=0; i<rows; ++i){
        int index = range_->asof(partitionCol->get(i));
        if(index >= partitions)
            keys[i] = -1;
        else
            keys[i] = index;
    }
    return keys;
}
};
