#include "MultithreadedTableWriter.h"
#include "ScalarImp.h"
#include "Domain.h"
#include "Logger.h"
#include "DolphinDB.h"
#include <thread>
//#include "DdbPythonUtil.h"

using std::string;
using std::vector;
namespace dolphindb {

MultithreadedTableWriter::MultithreadedTableWriter(const std::string& hostName, int port, const std::string& userId, const std::string& password,
    const string& dbName, const string& tableName, bool useSSL,
    bool enableHighAvailability, const vector<string>* pHighAvailabilitySites,
    int batchSize, float throttle, int threadCount, const string& partitionCol,
    const vector<COMPRESS_METHOD>* pCompressMethods, MultithreadedTableWriter::Mode mode,
    vector<string>* pModeOption, const std::function<void(ConstantSP)>& callbackFunc, bool enableStreamTableTimestamp) :
    dbName_(dbName),
    tableName_(tableName),
    batchSize_(batchSize),
    throttleMilsecond_(static_cast<int>(throttle * 1000)),
    exited_(false),
    hasError_(false),
    partitionColumnIdx_(-1),
    threadByColIndexForNonPartion_(-1),
    //,pytoDdb_(new PytoDdbRowPool(*this))
    mode_(mode),
    callbackFunc_(callbackFunc),
    enableStreamTableTimestamp_(enableStreamTableTimestamp)
{
    if (threadCount < 1) {
        throw RuntimeException("The parameter threadCount must be greater than or equal to 1");
    }
    if (batchSize < 1) {
        throw RuntimeException("The parameter batchSize must be greater than or equal to 1");
    }
    if (throttle < 0) {
        throw RuntimeException("The parameter throttle must be greater than 0");
    }
    if (threadCount > 1 && partitionCol.empty()) {
        throw RuntimeException("The parameter partitionCol must be specified when threadCount is greater than 1");
    }
    bool isCompress = false;
    int keepAliveTime = 7200;
    if (pCompressMethods != nullptr && pCompressMethods->size() > 0) {
        for (auto one : *pCompressMethods) {
            if (one != COMPRESS_DELTA && one != COMPRESS_LZ4) {
                throw RuntimeException("Unsupported compression method " + std::to_string(one));
            }
        }
        saveCompressMethods_ = *pCompressMethods;
        isCompress = true;
    }
    SmartPointer<DBConnection> pConn = new DBConnection(useSSL, false, keepAliveTime, isCompress);
    vector<string> highAvailabilitySites;
    if (pHighAvailabilitySites != nullptr) {
        highAvailabilitySites.assign(pHighAvailabilitySites->begin(), pHighAvailabilitySites->end());
    }
    bool ret = pConn->connect(hostName, port, userId, password, "", enableHighAvailability, highAvailabilitySites, 30);
    if (!ret) {
        throw RuntimeException("Failed to connect to server " + hostName + ":" + std::to_string(port));
    }

    DictionarySP schema;
    if (dbName.empty()) {
        schema = pConn->run("schema(" + tableName + ")");
    }
    else {
        schema = pConn->run(std::string("schema(loadTable(\"") + dbName + "\",\"" + tableName + "\"))");
    }
    ConstantSP partColNames = schema->getMember("partitionColumnName");
    if (partColNames->isNull() == false) {//partitioned table
        isPartionedTable_ = true;
    }
    else {//Not partitioned table
        if (dbName.empty() == false) {//Single partitioned table
            if (threadCount > 1) {
                throw RuntimeException("The parameter threadCount must be 1 for a dimension table");
            }
        }
        isPartionedTable_ = false;
    }
    TableSP colDefs = schema->getMember("colDefs");

    ConstantSP colDefsTypeInt = colDefs->getColumn("typeInt");
    size_t columnSize = colDefs->size() - enableStreamTableTimestamp_;
    if (saveCompressMethods_.size() > 0 && saveCompressMethods_.size() != columnSize) {
        throw RuntimeException("The number of elements in parameter compressMethods does not match the column size " + std::to_string(columnSize));
    }
    colExtras_.resize(columnSize);
    int colIndex = colDefs->getColumnIndex("extra");
    if (colIndex >= 0) {
        ConstantSP colExtra = colDefs->getColumn(colIndex);
        for (int i = 0; i < static_cast<int>(colExtras_.size()); i++) {
            colExtras_[i] = colExtra->getInt(i);
        }
    }

    ConstantSP colDefsName = colDefs->getColumn("name");
    ConstantSP colDefsTypeString = colDefs->getColumn("typeString");
    for (INDEX i = 0; i < static_cast<INDEX>(columnSize); i++) {
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
                    throw RuntimeException("The parameter partionCol must be the partitioning column '" + partColNames->getString() + "' in the partitioned table");
                }
                partitionColumnIdx_ = schema->getMember("partitionColumnIndex")->getInt();
                partitionSchema = schema->getMember("partitionSchema");
                partitionType = schema->getMember("partitionType")->getInt();
                partitionColType = (DATA_TYPE)schema->getMember("partitionColumnType")->getInt();
            }
            else {
                int dims = partColNames->size();
                if (dims > 1 && partitionCol.empty()) {
                    throw RuntimeException("The parameter partitionCol must be specified for a partitioned table");
                }
                int index = -1;
                for (int i = 0; i < dims; ++i) {
                    if (partColNames->getString(i) == partitionCol) {
                        index = i;
                        break;
                    }
                }
                if (index < 0)
                    throw RuntimeException("The parameter partionCol must be the partitioning columns in the partitioned table");
                partitionColumnIdx_ = schema->getMember("partitionColumnIndex")->getInt(index);
                partitionSchema = schema->getMember("partitionSchema")->get(index);
                partitionType = schema->getMember("partitionType")->getInt(index);
                partitionColType = (DATA_TYPE)schema->getMember("partitionColumnType")->getInt(index);
            }
            if (colTypes_[partitionColumnIdx_] >= ARRAY_TYPE_BASE) {//arrayVector can't be partitioned
                throw RuntimeException("The parameter partitionCol cannot be array vector");
            }
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
                    throw RuntimeException("No match found for " + partitionCol);
                }
                if (colTypes_[threadcolindex] >= ARRAY_TYPE_BASE) {//arrayVector can't be partitioned
                    throw RuntimeException("The parameter partitionCol cannot be array vector");
                }
                threadByColIndexForNonPartion_ = threadcolindex;
            }
        }
    }
    saveColNames_ = colNames_;
    saveColTypes_ = colTypes_;
    saveColExtras_ = colExtras_;
    if (callbackFunc_ != nullptr) {
        colExtras_.insert(colExtras_.begin(), 0);
        colNames_.insert(colNames_.begin(), "callbackId");
        colTypes_.insert(colTypes_.begin(), DT_STRING);
        colTypeString_.insert(colTypeString_.begin(), Util::getDataTypeString(colTypes_.front()));
        if (threadByColIndexForNonPartion_ >= 0)
            threadByColIndexForNonPartion_++;
        if (partitionColumnIdx_ >= 0)
            partitionColumnIdx_++;
    }
    if (mode == M_Append) {
        if (dbName_.empty()) {//memory table
            scriptTableInsert_ = std::move(std::string("tableInsert{") + tableName_ + "}");
        }
        else if (isPartionedTable_) {//partitioned table
            scriptTableInsert_ = std::move(std::string("tableInsert{loadTable(\"") + dbName_ + "\",\"" + tableName_ + "\")}");
        }
        else {//single partitioned table
            scriptTableInsert_ = std::move(std::string("tableInsert{loadTable(\"") + dbName_ + "\",\"" + tableName_ + "\")}");
        }
    }
    else if (mode == M_Upsert) {
        //upsert!(obj, newData, [ignoreNull=false], [keyColNames], [sortColumns])
        if (dbName_.empty()) {//memory table
            scriptTableInsert_ = std::move(std::string("upsert!{") + tableName_);
        }
        else if (isPartionedTable_) {//partitioned table
            scriptTableInsert_ = std::move(std::string("upsert!{loadTable(\"") + dbName_ + "\",\"" + tableName_ + "\")");
        }
        else {//single partitioned table
            scriptTableInsert_ = std::move(std::string("upsert!{loadTable(\"") + dbName_ + "\",\"" + tableName_ + "\")");
        }
        //ignore newData
        scriptTableInsert_ += ",";
        if (pModeOption != nullptr && pModeOption->empty() == false) {
            for (auto& one : *pModeOption) {
                scriptTableInsert_ += "," + one;
            }
        }
        scriptTableInsert_ += "}";
    }
    // init done, start thread now.
    perBlockSize_ = batchSize_ < 65535 ? 65535 : batchSize_;
    threads_.resize(threadCount);
    for (unsigned int i = 0; i < threads_.size(); i++) {
        WriterThread& writerThread = threads_[i];
        writerThread.threadId = 0;
        writerThread.sentRows = 0;
        writerThread.writeQueue_.push_back(createColumnBlock());
        writerThread.exit = false;
        LockGuard<Mutex> _(&writerThread.mutex_);

        writerThread.conn = new DBConnection(useSSL, false, keepAliveTime, isCompress);
        if (writerThread.conn->connect(hostName, port, userId, password, "", enableHighAvailability, highAvailabilitySites, 30, true) == false) {
            throw RuntimeException("Failed to connect to server " + hostName + ":" + std::to_string(port));
        }

        writerThread.writeThread = new Thread(new SendExecutor(*this, writerThread));
        writerThread.writeThread->start();
    }
}

MultithreadedTableWriter::~MultithreadedTableWriter() {
    waitForThreadCompletion();
    {
        std::vector<ConstantSP>* pitem = nullptr;
        for (auto& thread : threads_) {
            LockGuard<Mutex> _(&thread.mutex_);
            for (auto& one : thread.writeQueue_) {
                delete one;
            }
            for (auto& one : thread.failedQueue_) {
                delete one;
            }
        }
        while (unusedQueue_.pop(pitem)) {
            delete pitem;
        }
    }
}

void MultithreadedTableWriter::waitForThreadCompletion() {
    RWLockGuard<RWLock> guard(&insertRWLock_, true);
    if (exited_)
        return;
    //if(pytoDdb_->isExit())
    //    return;
    //pytoDdb_->startExit();
    for (auto& thread : threads_) {
        thread.exit = true;
        thread.nonemptySignal.set();
    }
    for (auto& thread : threads_) {
        thread.writeThread->join();
    }
    for (auto& thread : threads_) {
        thread.conn->close();
    }
    //pytoDdb_->endExit();
    setError(0, "");
    exited_ = true;
}

void MultithreadedTableWriter::setError(const ErrorCodeInfo& errorInfo) {
    if (hasError_.exchange(true))
        return;
    errorInfo_.set(errorInfo);
}

void MultithreadedTableWriter::setError(int code, const string& info) {
    ErrorCodeInfo errorInfo;
    errorInfo.set(code, info);
    setError(errorInfo);
}

bool MultithreadedTableWriter::SendExecutor::init() {
    writeThread_.threadId = Util::getCurThreadId();
    return true;
}

bool MultithreadedTableWriter::insert(std::vector<ConstantSP>** records, int recordCount, ErrorCodeInfo& errorInfo, bool isNeedReleaseMemory) {
    if (hasError_.load()) {
        errorInfo.set(ErrorCodeInfo::EC_DestroyedObject, "Thread is exiting.");
        return false;
    }
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
    if (threads_.size() > 1) {
        if (isPartionedTable_) {
            VectorSP pvector = Util::createVector(getColDataType(partitionColumnIdx_), recordCount, 0);
            for (int i = 0; i < recordCount; i++) {
                pvector->set(i, records[i]->at(partitionColumnIdx_));
            }
            vector<int> threadindexes = partitionDomain_->getPartitionKeys(pvector);
            for (unsigned int row = 0; row < threadindexes.size(); row++) {
                insertThreadWrite(threadindexes[row], records[row]);
            }
        }
        else {
            int threadindex;
            for (int i = 0; i < recordCount; i++) {
                threadindex = records[i]->at(threadByColIndexForNonPartion_)->getHash(static_cast<int>(threads_.size()));
                insertThreadWrite(threadindex, records[i]);
            }
        }
    }
    else {
        for (int i = 0; i < recordCount; i++) {
            insertThreadWrite(0, records[i]);
        }
    }
    if(isNeedReleaseMemory){
        for (int i = 0; i < recordCount; i++) {
            delete records[i];
        }
    }
    return true;
}

void MultithreadedTableWriter::getStatus(Status& status) {
    RWLockGuard<RWLock> guard(&insertRWLock_, true);
    status.isExiting = hasError_.load();
    status.errorCode = errorInfo_.errorCode;
    status.errorInfo = errorInfo_.errorInfo;
    status.sentRows = status.unsentRows = status.sendFailedRows = 0;
    status.threadStatus_.resize(threads_.size());
    for (size_t i = 0; i < threads_.size(); i++) {
        ThreadStatus& threadStatus = status.threadStatus_[i];
        WriterThread& writeThread = threads_[i];
        LockGuard<Mutex> _(&writeThread.mutex_);
        threadStatus.threadId = writeThread.threadId;
        threadStatus.sentRows = writeThread.sentRows;
        threadStatus.unsentRows = static_cast<long>((writeThread.writeQueue_.size() - 1) * perBlockSize_ + writeThread.writeQueue_.back()->front()->size());
        threadStatus.sendFailedRows = static_cast<long>(writeThread.failedQueue_.size());
        status.plus(threadStatus);
    }
}

void MultithreadedTableWriter::getUnwrittenData(std::vector<std::vector<ConstantSP>*>& unwrittenData) {
    if (callbackFunc_ != nullptr) {
        throw RuntimeException("getUnwrittenData is disabled when callbackFunc is enabled.");
    }
    RWLockGuard<RWLock> guard(&insertRWLock_, true);
    for (auto& writeThread : threads_) {
        LockGuard<Mutex> _(&writeThread.mutex_);
        unwrittenData.insert(unwrittenData.end(), writeThread.failedQueue_.begin(), writeThread.failedQueue_.end());
        writeThread.failedQueue_.clear();
        size_t colNum = colTypes_.size();
        for(std::vector<ConstantSP>* block : writeThread.writeQueue_){
            int rows = block->front()->size();
            for(int i = 0; i < rows; ++i){
                std::vector<ConstantSP>* oneUnwrittenRow = new std::vector<ConstantSP>;
                for(size_t j = 0; j < colNum; ++j){
                    oneUnwrittenRow->push_back(block->at(j)->get(i));
                }
                unwrittenData.push_back(oneUnwrittenRow);
            }
        }
        writeThread.writeQueue_.clear();
        writeThread.writeQueue_.push_back(createColumnBlock());
    }
}

void MultithreadedTableWriter::insertThreadWrite(int threadhashkey, std::vector<ConstantSP>* prow) {
    if (threadhashkey < 0) {
        threadhashkey = 0;
    }
    int threadIndex = threadhashkey % threads_.size();
    WriterThread& writerThread = threads_[threadIndex];
    {
        LockGuard<Mutex> _(&writerThread.writeQueueMutex_);
        if(writerThread.writeQueue_.back()->front()->size() > perBlockSize_){
            writerThread.writeQueue_.push_back(createColumnBlock());
        }
        std::vector<ConstantSP>* q = writerThread.writeQueue_.back();
        size_t size = prow->size();
        for(size_t i = 0; i < size; ++i){
            reinterpret_cast<Vector*>(q->at(i).get())->append(prow->at(i));
        }
    }
    writerThread.nonemptySignal.set();
}

void MultithreadedTableWriter::callBack(std::function<void(ConstantSP)> callbackFunc,bool result,vector<ConstantSP>* block) {
    if (callbackFunc == nullptr)
        return;
    INDEX size = block->front()->size();
    if (size < 1)
        return;
    VectorSP callbackIds = block->front();
    DdbVector<char> results(size);
    for (int i = 0; i < size; i++) {
        results.set(i, result);
    }
    TableSP table = Util::createTable({ "callbackId","isSuccess" }, { callbackIds,results.createVector(DT_BOOL) });
    callbackFunc(table);
}

std::vector<ConstantSP>* MultithreadedTableWriter::createColumnBlock(){
    std::vector<ConstantSP>* res = nullptr;
    if(unusedQueue_.pop(res)){
        return res;
    }
    res = new std::vector<ConstantSP>;
    size_t length = colTypes_.size();
    for(size_t i = 0; i < length; ++i){
        res->push_back(Util::createVector(colTypes_[i], 0, perBlockSize_, true, colExtras_[i]));
    }
    return res;
}

void MultithreadedTableWriter::SendExecutor::run(){
    if(init()==false){
        return;
    }
    long long batchWaitTimeout = 0, diff;
    while(isExit() == false){
        {
            if(writeThread_.writeQueue_.back()->front()->size() < 1){     //Wait for first data
                writeThread_.nonemptySignal.wait();
            }
            if (isExit())
                break;
            //wait for batchsize
            if (tableWriter_.batchSize_ > 1 && tableWriter_.throttleMilsecond_ > 0) {
                batchWaitTimeout = Util::getEpochTime() + tableWriter_.throttleMilsecond_;
                while (isExit() == false && (writeThread_.writeQueue_.size() - 1) * tableWriter_.perBlockSize_ + writeThread_.writeQueue_.back()->front()->size() < static_cast<std::size_t>(tableWriter_.batchSize_)) {//check batchsize
                    diff = batchWaitTimeout - Util::getEpochTime();
                    if (diff > 0) {
                        writeThread_.nonemptySignal.tryWait(static_cast<int>(diff));
                    }
                    else {
                        break;
                    }
                }
            }
        }
        while (isExit() == false && writeAllData());//write all data
    }
    //write left data
    while (tableWriter_.hasError_.load() == false && writeAllData()){}
    //call back
    {
        LockGuard<Mutex> _(&writeThread_.writeQueueMutex_);
        for(auto block : writeThread_.writeQueue_){
            MultithreadedTableWriter::callBack(tableWriter_.callbackFunc_, false, block);
        }
    }
}

bool MultithreadedTableWriter::SendExecutor::writeAllData(){
    //reset idle
    LockGuard<Mutex> _(&writeThread_.mutex_);
    std::vector<ConstantSP>* items = nullptr;
    int size = 0;
    {
        LockGuard<Mutex> __(&writeThread_.writeQueueMutex_);
        items = writeThread_.writeQueue_.front();
        size = items->front()->size();
        if (size < 1){
            return false;
        }
        writeThread_.writeQueue_.pop_front();
        if(writeThread_.writeQueue_.empty()){
            writeThread_.writeQueue_.push_back(tableWriter_.createColumnBlock());
        }
    }
    string runscript;
    try{
        TableSP writeTable;
        //create table
        if(tableWriter_.callbackFunc_ != nullptr){
            writeTable = Util::createTable(tableWriter_.saveColNames_, {items->begin() + 1, items->end()});
        }
        else{
            writeTable = Util::createTable(tableWriter_.saveColNames_, *items);
        }
        writeTable->setColumnCompressMethods(tableWriter_.saveCompressMethods_);

        std::vector<ConstantSP> args(1);
        args[0] = writeTable;
        runscript = tableWriter_.scriptTableInsert_;
        ConstantSP constsp = writeThread_.conn->run(runscript, args);
        writeThread_.sentRows += size;
        MultithreadedTableWriter::callBack(tableWriter_.callbackFunc_, true, items);
        if (tableWriter_.unusedQueue_.size() < 10) {
            for(ConstantSP& item : *items){
                dynamic_cast<Vector*>(item.get())->clear();
            }
            tableWriter_.unusedQueue_.push(items);
        }
        else {
            delete items;
        }
    }catch (std::exception &e){//failed
        string errmsg=e.what();
        if(runscript.empty()==false && errmsg.find(" script:")==string::npos){
            errmsg+=" script: "+runscript;
        }
        DLogger::Error("threadid", writeThread_.threadId, "Failed to save the inserted data: ", errmsg);
        {
            size_t columnNum = items->size();
            for(int i = 0; i < size; ++i){
                std::vector<ConstantSP>* oneFailedRow = new std::vector<ConstantSP>;
                for(size_t j = 0; j < columnNum; ++j){
                    oneFailedRow->push_back(items->at(j)->get(i));
                }
                writeThread_.failedQueue_.push_back(oneFailedRow);
            }
            MultithreadedTableWriter::callBack(tableWriter_.callbackFunc_, false, items);
        }
        tableWriter_.setError(ErrorCodeInfo::EC_Server, std::string("Failed to save the inserted data: ") + errmsg);
    }
    return true;
}

}
