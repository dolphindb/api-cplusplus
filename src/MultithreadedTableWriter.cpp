#include "MultithreadedTableWriter.h"
#include "ScalarImp.h"
#include <thread>

namespace dolphindb{

MultithreadedTableWriter::MultithreadedTableWriter(const std::string& hostName, int port, const std::string& userId, const std::string& password,
										const string& dbName, const string& tableName, bool useSSL,
										bool enableHighAvailability, const vector<string> *pHighAvailabilitySites,
										int batchSize, float throttle, int threadCount, const string& partitionCol,
										const vector<COMPRESS_METHOD> *pCompressMethods):
                        dbName_(dbName),
                        tableName_(tableName),
                        batchSize_(batchSize),
                        throttleMilsecond_(throttle*1000),
						hasError_(false){
    if(threadCount < 1){
        throw RuntimeException("Thread count must be greater or equal than 1.");
    }
    if(batchSize < 1){
        throw RuntimeException("Batch size must be greater than 1.");
    }
    if(throttle < 0){
        throw RuntimeException("Throttle must be greater than 0.");
    }
	if (threadCount > 1 && partitionCol.empty()) {
		throw RuntimeException("partitionCol must be specified in muti-thread mode.");
	}
	bool isCompress = false;
	int keepAliveTime = 7200;
	if (pCompressMethods != NULL && pCompressMethods->size() > 0) {
		for (auto one : *pCompressMethods) {
			if (one != COMPRESS_DELTA && one != COMPRESS_LZ4) {
				throw RuntimeException("Unsupported compress method.");
			}
		}
		compressMethods_ = *pCompressMethods;
		isCompress = true;
	}
    SmartPointer<DBConnection> pConn=new DBConnection(useSSL, false, keepAliveTime, isCompress);
	vector<string> highAvailabilitySites;
	if (pHighAvailabilitySites != NULL) {
		highAvailabilitySites.assign(pHighAvailabilitySites->begin(), pHighAvailabilitySites->end());
	}
    bool ret = pConn->connect(hostName, port, userId, password, "", enableHighAvailability, highAvailabilitySites);
    if(!ret){
        throw RuntimeException("Failed to connect to server.");
    }

    DictionarySP schema;
    if(tableName.empty()){
        schema = pConn->run("schema(" + dbName + ")");
    }else{
        //DLogger::Info("schema single partitioned table ",tableName);
        schema = pConn->run(std::string("schema(loadTable(\"") + dbName + "\",\"" + tableName + "\"))");
    }
    ConstantSP partColNames = schema->getMember("partitionColumnName");
    if(partColNames->isNull()==false){//partitioned table
        isPartionedTable_ = true;
    }else{//Not partitioned table
        if (tableName.empty() == false) {//Single partitioned table
			if (threadCount > 1) {
				throw RuntimeException("Single partitioned table support single thread only.");
			}
		}
		isPartionedTable_ = false;
    }

    TableSP colDefs = schema->getMember("colDefs");

    //destTable->conn = new DBConnection(std::move(conn));
    ConstantSP colDefsTypeInt = colDefs->getColumn("typeInt");
    int columnSize = colDefs->size();
	if (compressMethods_.size() > 0 && compressMethods_.size() != columnSize) {
		throw RuntimeException("Compress type size doesn't match column size "+std::to_string(columnSize));
	}
    
    ConstantSP colDefsName = colDefs->getColumn("name");
    ConstantSP colDefsTypeString = colDefs->getColumn("typeString");
    for(int i = 0; i < columnSize; i++){
        colNames_.push_back(colDefsName->getString(i));
        colTypes_.push_back(static_cast<DATA_TYPE>(colDefsTypeInt->getInt(i)));
        colTypeString_.push_back(colDefsTypeString->getString(i));
    }
	if (threadCount > 1) {//Only multithread need partition col info
		if (isPartionedTable_) {
			ConstantSP partitionSchema;
			int partitionType;
			DATA_TYPE partitionColType;
			if (partColNames->isScalar()) {
				if (partColNames->getString() != partitionCol) {
					throw RuntimeException("PartitionColumnName mismatch specified value, is " + partColNames->getString() + " ?");
				}
				partitionColumnIdx_ = schema->getMember("partitionColumnIndex")->getInt();
				partitionSchema = schema->getMember("partitionSchema");
				partitionType = schema->getMember("partitionType")->getInt();
				partitionColType = (DATA_TYPE)schema->getMember("partitionColumnType")->getInt();
			}
			else {
				int dims = partColNames->size();
				if (dims > 1 && partitionCol.empty()) {
					throw RuntimeException("Please specify partitionCol for this partitioned table.");
				}
				int index = -1;
				for (int i = 0; i < dims; ++i) {
					if (partColNames->getString(i) == partitionCol) {
						index = i;
						break;
					}
				}
				if (index < 0)
					throw RuntimeException("Can't find partitionCol in column.");
				partitionColumnIdx_ = schema->getMember("partitionColumnIndex")->getInt(index);
				partitionSchema = schema->getMember("partitionSchema")->get(index);
				partitionType = schema->getMember("partitionType")->getInt(index);
				partitionColType = (DATA_TYPE)schema->getMember("partitionColumnType")->getInt(index);
			}
			if (colTypes_[partitionColumnIdx_] >= ARRAY_TYPE_BASE) {//arrayVector can't be partitioned
				throw RuntimeException(Util::getDataTypeString(colTypes_[partitionColumnIdx_])+" column can't be partitioned.");
			}
			//DATA_TYPE partitionColType = colTypes_[partitionColumnIdx_];
			partitionDomain_ = Util::createDomain((PARTITION_TYPE)partitionType, partitionColType, partitionSchema);
		}
		else {//isPartionedTable_==false
			if (partitionCol.empty() == false) {
				int threadcolindex = -1;
				for (unsigned int i = 0; i < colNames_.size(); i++) {
					if (colNames_[i] == partitionCol) {
						threadcolindex = i;
						break;
					}
				}
				if (threadcolindex < 0) {
					throw RuntimeException("Can't find column name for " + partitionCol);
				}
				if (colTypes_[threadcolindex] >= ARRAY_TYPE_BASE) {//arrayVector can't be partitioned
					throw RuntimeException(Util::getDataTypeString(colTypes_[threadcolindex]) + " column can't be partitioned.");
				}
				threadByColIndexForNonPartion_ = threadcolindex;
			}
		}
	}
    // init done, start thread now.
    threads_.resize(threadCount);
    for(unsigned int i = 0; i < threads_.size(); i++){
        WriterThread &writerThread = threads_[i];
        writerThread.threadId = 0;
        writerThread.sentRows = 0;
		writerThread.exit = false;
        if(i==0){
            writerThread.conn=pConn;
        }else{
            writerThread.conn = new DBConnection(useSSL, false, keepAliveTime, isCompress);
            if(writerThread.conn->connect(hostName, port, userId, password, "", enableHighAvailability, highAvailabilitySites)==false){
                throw RuntimeException("Can't connection to server.");
            }
        }
        writerThread.writeThread = new Thread(new Executor(*this,writerThread));
        writerThread.writeThread->start();
    }
}

MultithreadedTableWriter::~MultithreadedTableWriter(){
	waitForThreadCompletion();
}

void MultithreadedTableWriter::waitForThreadCompletion() {
	for (auto &thread : threads_) {
		thread.exit = true;
		thread.writeNotifier.notify();
	}
	for (auto &thread : threads_) {
		thread.writeThread->join();
		thread.conn->close();
	}
	setError(ErrorCodeInfo::EC_UserBreak, "User break;");
}

void MultithreadedTableWriter::setError(int code, const string &info){
    LockGuard<Mutex> LockGuard(&tableMutex_);
    if(hasError_)
        return;
    errorInfo_.set(code, info);
	hasError_ = true;
}

bool MultithreadedTableWriter::Executor::init(){
	writeThread_.threadId = Util::getCurThreadId();
    if(tableWriter_.tableName_.empty()){//memory table
        writeThread_.scriptTableInsert = std::move(std::string("tableInsert{\"") + tableWriter_.dbName_ + "\"}");
    }else if(tableWriter_.isPartionedTable_){//partitioned table
        writeThread_.scriptTableInsert = std::move(std::string("tableInsert{loadTable(\"") + tableWriter_.dbName_ + "\",\"" + tableWriter_.tableName_ + "\")}");
    }else{//single partitioned table
        writeThread_.scriptTableInsert = std::move(std::string("tableInsert{loadTable(\"") + tableWriter_.dbName_ + "\",\"" + tableWriter_.tableName_ + "\")}");
        //Remove support for disk table
        /*{
            std::string tempTableName = "tmp" +  tableWriter_.tableName_;
            std::string colNames;
            std::string colTypes;
            for(unsigned int i = 0; i < tableWriter_.colNames_.size(); i++){
                colNames += "`" + tableWriter_.colNames_[i];
                colTypes += "`" + tableWriter_.colTypeString_[i];
            }
            std::string scriptCreateTmpTable = std::move(std::string("tempTable = table(") + "1000:0," + colNames + "," + colTypes + ")");
            try{
                writeThread_.conn->run(scriptCreateTmpTable);
            }catch(std::exception &e){
                DLogger::Error("threadid=", writeThread_.threadId, " Init table error: ", e.what()," script:", scriptCreateTmpTable);
                tableWriter_.setDestroyed(ErrorCodeInfo::EC_Server,std::string("Init table error: ")+e.what()+" script: "+scriptCreateTmpTable);
                //std::cerr << Util::createTimestamp(Util::getEpochTime())->getString() << " Backgroud thread of table (" << tableWriter_.dbName_ << " " << tableWriter_.tableName_ << "). Failed to init data to server, with exception: " << e.what() << std::endl;
                return false;
            }
        }
        writeThread_.scriptTableInsert = std::move(std::string("tableInsert{tempTable}"));
        writeThread_.scriptSaveTable = std::move(std::string("saveTable(database(\"") + tableWriter_.dbName_ + "\")" + ",tempTable,\"" + tableWriter_.tableName_ + "\", 1);tempTable.clear!();");
        */
    }
    return true;
}

bool MultithreadedTableWriter::insert(const std::vector<ConstantSP> &record, ErrorCodeInfo &errorInfo) {
	std::vector<std::vector<ConstantSP>> records;
	records.push_back(std::move(record));
	return insert(records, errorInfo);
}

bool MultithreadedTableWriter::insert(const std::vector<std::vector<ConstantSP>> &records, ErrorCodeInfo &errorInfo){
	//To speed up, ignore check
	/*
	for (auto &row : vectorOfVector) {
		if (row.size() != colNames_.size()) {
			errorInfo.set(ErrorCodeInfo::EC_InvalidObject, "Invalid vector size " + std::to_string(row.size()) + ", expect " + std::to_string(colNames_.size()));
			return false;
		}
		int index = 0;
		DATA_TYPE dataType;
		for (auto &param : row) {
			dataType = getColDataType(index);
			if (param->getType() != dataType && dataType != DATA_TYPE::DT_SYMBOL) {
				errorInfo.set(ErrorCodeInfo::EC_InvalidObject, "Object type mismatch " + Util::getDataTypeString(param->getType()) + ", expect " + Util::getDataTypeString(dataType));
				return false;
			}
			index++;
		}
	}
	*/
    if(threads_.size() > 1){
        if(isPartionedTable_){
			VectorSP pvector = Util::createVector(getColDataType(partitionColumnIdx_), 0, records.size());
            for(auto &row : records){
                pvector->append(row[partitionColumnIdx_]);
            }
            vector<int> threadindexes = partitionDomain_->getPartitionKeys(pvector);
            for(unsigned int row = 0; row < threadindexes.size(); row++){
                if(insertThreadWrite(threadindexes[row], records[row], errorInfo) == false){
                    return false;
                }
            }
        }else{
            int threadindex;
            for(auto &row : records){
                //DLogger::Info("threadByColIndexForNonPartion_ ",threadByColIndexForNonPartion_);
                threadindex = row[threadByColIndexForNonPartion_]->getHash(threads_.size());
                //DLogger::Info("insertVector ",threadindex,",");
                if(insertThreadWrite(threadindex, row, errorInfo) == false){
                    return false;
                }
            }
        }
    }else{
        for(auto &row : records){
            if(insertThreadWrite(0, row, errorInfo) == false){
                return false;
            }
        }
    }
    //DLogger::Info("insertVector done");
    return true;
}

void MultithreadedTableWriter::getStatus(Status &status){
    status.isExiting = hasError_;
    status.errorInfo = errorInfo_;
	status.sentRows = status.unsentRows = status.sendFailedRows = 0;
	status.threadStatus.resize(threads_.size());
	for(unsigned int i = 0; i < threads_.size(); i++){
        ThreadStatus &threadStatus = status.threadStatus[i];
        WriterThread &writeThread = threads_[i];
        threadStatus.threadId = writeThread.threadId;
        threadStatus.sentRows = writeThread.sentRows;
        threadStatus.unsentRows = writeThread.writeQueue.size();
        threadStatus.sendFailedRows = writeThread.failedQueue.size();
		status.sentRows += threadStatus.sentRows;
		status.unsentRows += threadStatus.unsentRows;
		status.sendFailedRows += threadStatus.sendFailedRows;
    }
}

void MultithreadedTableWriter::getUnwrittenData(std::vector<std::vector<ConstantSP>> &unwrittenData){
    for(auto &writeThread : threads_){
        writeThread.failedQueue.pop(unwrittenData, writeThread.failedQueue.size());
        writeThread.writeQueue.pop(unwrittenData, writeThread.writeQueue.size());
    }
}

bool MultithreadedTableWriter::insertRecursive(ErrorCodeInfo &errorInfo, SmartPointer<std::vector<ConstantSP>> prow, int colIndex){
    std::vector<ConstantSP> &row=*prow;
    int threadindex;
    if(threads_.size() > 1){
        if(isPartionedTable_){
			DATA_TYPE dataType = getColDataType(partitionColumnIdx_);
            VectorSP pvector = Util::createVector(dataType, 0, 1);
            if(row[partitionColumnIdx_]->isNull()){
                row[partitionColumnIdx_]=Util::createNullConstant(dataType);
            }
            pvector->append(row[partitionColumnIdx_]);
            {
                vector<int> indexes = partitionDomain_->getPartitionKeys(pvector);
                if(indexes.empty()==false){
                    threadindex = indexes.at(0);
                }else{
                    errorInfo.set(ErrorCodeInfo::EC_Server,"getPartitionKeys failed.");
                    return false;
                }
            }
        }else{
            if(row[threadByColIndexForNonPartion_]->isNull()){
                row[threadByColIndexForNonPartion_]=Util::createNullConstant(getColDataType(threadByColIndexForNonPartion_));
            }
            threadindex = row[threadByColIndexForNonPartion_]->getHash(threads_.size());
        }
    }else{
        threadindex = 0;
    }
    return insertThreadWrite(threadindex, row, errorInfo);
}

bool MultithreadedTableWriter::insertThreadWrite(int threadhashkey, const std::vector<ConstantSP> &row, ErrorCodeInfo &errorInfo){
    if(threadhashkey < 0){
        //DLogger::Warn("add invalid hash=",threadhashkey);
        threadhashkey = 0;
        //errorInfo.set(ErrorCodeInfo::EC_InvalidColumnType, "Failed to get thread by coluname.");
        //return false;
    }
    if(hasError_){
        errorInfo.set(errorInfo_);
        return false;
    }
    int threadIndex = threadhashkey % threads_.size();
    WriterThread &writerThread = threads_[threadIndex];
	//DLogger::Info("insertThreadWrite", writerThread.threadId, ", hashvalue", threadhashkey, ".");
    //DLogger::Info("insertThreadWrite ",threadIndex,", new size ",writerThread.writeQueue.size());
    writerThread.writeQueue.push(std::move(row));
    //std::cout << "notify size:" << writerThread.writeQueue.size() << std::endl;
    writerThread.writeNotifier.notify();
    return true;
}

void MultithreadedTableWriter::Executor::run(){
    if(init()==false){
        return;
    }
	long batchWaitTimeout = 0, diff;
    while(isExit() == false){
		//DLogger::Info(" run start wait first");
		if(writeThread_.writeQueue.size() < 1){//Wait for first data
            writeThread_.writeMutex.lock();
            writeThread_.writeNotifier.wait(writeThread_.writeMutex);
            //std::cout << "Recv notify size:" << writeThread_.writeQueue.size() << std::endl;
        }
		if (isExit())
			break;
		//DLogger::Info(" run start wait batch");
		//wait for batchsize
		if (tableWriter_.batchSize_ > 1 && tableWriter_.throttleMilsecond_ > 0) {
			batchWaitTimeout = Util::getEpochTime() + tableWriter_.throttleMilsecond_;
			while (isExit() == false && writeThread_.writeQueue.size() < tableWriter_.batchSize_) {//check batchsize
				diff = batchWaitTimeout - Util::getEpochTime();
				if (diff > 0) {
					writeThread_.writeMutex.lock();
					writeThread_.writeNotifier.wait(writeThread_.writeMutex, diff);
				}
				else {
					break;
				}
			}
		}
		//Start write data
		//DLogger::Info(" run start write");
		while (isExit() == false && writeAllData());
		//DLogger::Info(" run end write");
    }
	// Write left data
	while (tableWriter_.hasError_ == false && writeAllData());
}

bool MultithreadedTableWriter::Executor::writeAllData(){
    std::vector<std::vector<ConstantSP>> items;
    {
        long size = std::min(writeThread_.writeQueue.size(), 65535);
        if (size < 1)
            return false;
        items.reserve(size);
        writeThread_.writeQueue.pop(items, size);
    }
    int size = items.size();
    //std::cout << "writeTableAllData size:" << size << std::endl;
	//DLogger::Info(" writeAllData start size=", size, " leftsize=", writeThread_.writeQueue.size());
    if(size < 1)
        return false;
    string runscript;
	bool writeOK = true;
    try{
        TableSP writeTable;
		int addRowCount = 0;
        {//create table
            RECORDTIME("MTTW:createTable");
            writeTable = Util::createTable(tableWriter_.colNames_, tableWriter_.colTypes_, 0, size);
			writeTable->setColumnCompressMethods(tableWriter_.compressMethods_);
            INDEX insertedRows;
            std::string errMsg;
            for (int i = 0; i < size; i++){
				//DLogger::Info("create table ", i);
                if(writeTable->append(items[i], insertedRows, errMsg) == false){
                    //DLogger::Error("threadid=", writeThread_.threadId, " Append column failed: ", errMsg);
                    tableWriter_.setError(ErrorCodeInfo::EC_InvalidObject, "Append data to table failed: "+errMsg);
					writeOK = false;
                    //std::cerr << Util::createTimestamp(Util::getEpochTime())->getString() << " Backgroud thread of table (" << tableWriter_.dbName_ << " " << tableWriter_.tableName_ << "). Failed to create table, with error: " << errMsg << std::endl;
                    break;
                }
				addRowCount++;
            }
        }
		if(writeOK && addRowCount > 0){//save table
            RECORDTIME("MTTW:saveTable");
            std::vector<ConstantSP> args;
            args.reserve(1);
            args.push_back(writeTable);
            runscript = writeThread_.scriptTableInsert;
            //DLogger::Info("runscript start ", addRowCount);
            ConstantSP constsp = writeThread_.conn->run(runscript, args);
			int addresult = constsp->getInt();
			if (addresult != addRowCount) {
				std::cout << "Write complete size " << addresult << " mismatch insert size "<< addRowCount;
			}
			//DLogger::Info("runscript end ", addRowCount);
            if (writeThread_.scriptSaveTable.empty() == false){
                runscript = writeThread_.scriptSaveTable;
                //DLogger::Info("runscript ", runscript);
                writeThread_.conn->run(runscript);
            }
            writeThread_.sentRows += addRowCount;
        }
    }catch (std::exception &e){
        DLogger::Error("threadid=", writeThread_.threadId, " Save table error: ", e.what()," script:", runscript);
        tableWriter_.setError(ErrorCodeInfo::EC_Server,std::string("Save table error: ")+e.what()+" script: "+runscript);
        //std::cerr << Util::createTimestamp(Util::getEpochTime())->getString() << " Backgroud thread of table (" << tableWriter_.dbName_ << " " << tableWriter_.tableName_ << "). Failed to send data to server, with exception: " << e.what() << std::endl
		writeOK = false;
    }
    if (writeOK == false){
        for (auto &unwriteItem : items)
            writeThread_.failedQueue.push(unwriteItem);
    }
	//DLogger::Info(" writeAllData end size=", size, " leftsize=", writeThread_.writeQueue.size());
    return true;
}

};
