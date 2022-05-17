#include "BatchTableWriter.h"
#include "ScalarImp.h"

namespace dolphindb{

class Executor : public dolphindb::Runnable {
    using Func = std::function<void()>;

public:
    explicit Executor(Func f) : func_(std::move(f)){};
    void run() override { func_(); };

private:
    Func func_;
};

BatchTableWriter::BatchTableWriter(const std::string& hostName, int port, const std::string& userId, const std::string& password, bool acquireLock):
    hostName_(hostName),
    port_(port),
    userId_(userId),
    password_(password),
    acquireLock_(acquireLock)
    {}

BatchTableWriter::~BatchTableWriter(){
    vector<SmartPointer<DestTable>> dt;
    for(auto& i: destTables_){
        if(i.second->destroy == false){
            dt.push_back(i.second);
            i.second->destroy = true;
            i.second->writeNotifier.notify();
        }
    }
    for(auto& i: dt){
        i->writeThread->join();
    }
    for(auto& i: dt){
        i->conn->close();
    }
}

void BatchTableWriter::addTable(const string& dbName, const string& tableName, bool partitioned){
    {
        SmartPointer<DestTable> destTable;
        RWLockGuard<RWLock> _(&rwLock, false, acquireLock_);
        if(destTables_.find(std::make_pair(dbName, tableName)) != destTables_.end())
            throw RuntimeException("Failed to add table, the specified table has not been removed yet.");
    }

    DBConnection conn(false, false);
    bool ret = conn.connect(hostName_, port_, userId_, password_);
    if(!ret)
        throw RuntimeException("Failed to connect to server.");

    std::string tableInsert;
    std::string saveTable;
    DictionarySP schema;
    std::string tmpDiskGlobal;
    if(tableName.empty()){
        tableInsert = std::move(std::string("tableInsert{") + dbName + "}");
        schema = conn.run("schema(" + dbName + ")");
    }else if(partitioned){
        tableInsert = std::move(std::string("tableInsert{loadTable(\"") + dbName + "\",\"" + tableName + "\")}");
        schema = conn.run(std::string("schema(loadTable(\"") + dbName + "\",\"" + tableName + "\"))");
    }else{
        tmpDiskGlobal = "tmpDiskGlobal";
        std::string tmpDbName(dbName);
        tmpDbName.erase(std::remove(tmpDbName.begin(), tmpDbName.end(), ':'), tmpDbName.end());
        tmpDbName.erase(std::remove(tmpDbName.begin(), tmpDbName.end(), '\\'), tmpDbName.end());
        tmpDbName.erase(std::remove(tmpDbName.begin(), tmpDbName.end(), '/'), tmpDbName.end());
        tmpDiskGlobal = tmpDiskGlobal +  tmpDbName + tableName;
        tableInsert = std::move(std::string("tableInsert{") + tmpDiskGlobal + "}");
        saveTable = std::move(std::string("saveTable(database(\"") + dbName + "\")" + "," + tmpDiskGlobal +  ",\"" + tableName + "\", 1)");
        schema = conn.run(std::string("schema(loadTable(\"") + dbName + "\",\"" + tableName + "\"))");
    }

    TableSP colDefs = schema->getMember("colDefs");

    SmartPointer<DestTable> destTable;
    {
        RWLockGuard<RWLock> _(&rwLock, true, acquireLock_);
        if(destTables_.find(std::make_pair(dbName, tableName)) != destTables_.end())
            throw RuntimeException("Failed to add table, the specified table has not been removed yet.");
        destTables_[std::make_pair(dbName, tableName)] = new DestTable();
        destTable = destTables_[std::make_pair(dbName, tableName)];
    }
    destTable->dbName = dbName;
    destTable->tableName = tableName;
    destTable->conn = new DBConnection(std::move(conn));
    destTable->tableInsert = std::move(tableInsert);
    destTable->saveTable = std::move(saveTable);
    destTable->colDefs = colDefs;
    destTable->columnNum = colDefs->size();
    destTable->colDefsTypeInt = colDefs->getColumn("typeInt");
    destTable->destroy = false;
    
    std::vector<string> colNames;
    std::vector<DATA_TYPE> colTypes;
    ConstantSP colDefsName = colDefs->getColumn("name");
    for(int i = 0; i < destTable->columnNum; i++){
        colNames.push_back(colDefsName->getString(i));
        colTypes.push_back(static_cast<DATA_TYPE>(destTable->colDefsTypeInt->getInt(i)));
    }
    destTable->colNames = std::move(colNames);
    destTable->colTypes = std::move(colTypes);

    if(tmpDiskGlobal.empty() == false){//文件表需要创建中间表
        std::string colNames;
        std::string colTypes;
        ConstantSP colDefsTypeString = colDefs->getColumn("typeString");
        for(int i = 0; i < destTable->columnNum; i++){
            colNames += "`" + colDefsName->getString(i);
            colTypes += "`" + colDefsTypeString->getString(i);
        }
        destTable->createTmpSharedTable = std::move(std::string("share table(") + "1000:0," + colNames + "," + colTypes + ") as " + tmpDiskGlobal);
    }

    DestTable *destTableRawPtr = destTable.get();
    destTable->writeThread = new Thread(new Executor([=](){
        while(destTableRawPtr->destroy==false){
            destTableRawPtr->writeMutex.lock();
            destTableRawPtr->writeNotifier.wait(destTableRawPtr->writeMutex);
            while(true){
                if(writeTableAllData(destTable,partitioned)==false)
                    break;
            }
        }
        //写入尚未完成的数据
        while(destTableRawPtr->writeQueue.size()>0&&writeTableAllData(destTable,partitioned));
    }));
    destTable->writeThread->start();
}

bool BatchTableWriter::writeTableAllData(SmartPointer<DestTable> destTable,bool partitioned){
    DestTable *destTableRawPtr = destTable.get();
    if(destTableRawPtr->finished)
        return false;
    bool ret;
    auto size = destTableRawPtr->writeQueue.size();
    if (size < 1)
        return false;
    std::vector<std::vector<ConstantSP>> items;
    while (true)
    {
        try
        {
            items.reserve(size);
            break;
        }
        catch (...)
        {
            std::cerr << "Failed to reserve memory for std::vector." << std::endl;
            Util::sleep(20);
        }
    }
    destTableRawPtr->writeQueue.pop(items, size);

    size = items.size();

    bool writedataok = true;
    {
        destTableRawPtr->writeTable = Util::createTable(destTableRawPtr->colNames, destTableRawPtr->colTypes, 0, size);
        INDEX insertedRows;
        std::string errMsg;
        for (int i = 0; i < size; i++)
        {
            ret = destTableRawPtr->writeTable->append(items[i], insertedRows, errMsg);
            if (!ret)
            {
                std::cerr << Util::createTimestamp(Util::getEpochTime())->getString() << " Backgroud thread of table (" << destTableRawPtr->dbName << " " << destTableRawPtr->tableName << "). Failed to create table, with error: " << errMsg << std::endl;
                writedataok = false;
                break;
            }
        }
    }
    if (writedataok)
    {
        try
        {
            std::vector<ConstantSP> args;
            args.reserve(1);
            if (destTableRawPtr->createTmpSharedTable.empty() == false)
                destTableRawPtr->conn->run(destTableRawPtr->createTmpSharedTable);
            args.push_back(destTableRawPtr->writeTable);
            destTableRawPtr->conn->run(destTableRawPtr->tableInsert, args);
            if (!partitioned)
                destTableRawPtr->conn->run(destTableRawPtr->saveTable);
            destTableRawPtr->sendedRows += size;
        }
        catch (std::exception &e)
        {
            writedataok = false;
            std::cerr << Util::createTimestamp(Util::getEpochTime())->getString() << " Backgroud thread of table (" << destTableRawPtr->dbName << " " << destTableRawPtr->tableName << "). Failed to send data to server, with exception: " << e.what() << std::endl;
        }
    }
    if (writedataok == false)
    {
        destTableRawPtr->finished = true;
        for (int i = 0; i < size; i++)
            destTableRawPtr->saveQueue.push(items[i]);
    }
    return writedataok;
}

std::tuple<int,bool,bool> BatchTableWriter::getStatus(const string& dbName, const string& tableName){
    RWLockGuard<RWLock> _(&rwLock, false, acquireLock_);
    if(destTables_.find(std::make_pair(dbName, tableName)) == destTables_.end())
        throw RuntimeException("Failed to get queue depth. Please use addTable to add infomation of database and table first.");
    SmartPointer<DestTable> destTable = destTables_[std::make_pair(dbName, tableName)];
    return std::make_tuple(destTable->writeQueue.size(), destTable->destroy, destTable->finished);
}

TableSP BatchTableWriter::getAllStatus(){
    int columnNum = 6;
    std::vector<std::string> colNames{"DatabaseName","TableName","WriteQueueDepth","SendedRows","Removing","Finished"};
    std::vector<DATA_TYPE> colTypes{DATA_TYPE::DT_STRING,DATA_TYPE::DT_STRING,DATA_TYPE::DT_INT,DATA_TYPE::DT_INT,DATA_TYPE::DT_BOOL,DATA_TYPE::DT_BOOL};
    std::vector<VectorSP> columnVecs;
    columnVecs.reserve(columnNum);
    TableSP table;

    RWLockGuard<RWLock> _(&rwLock, false, acquireLock_);
    int rowNum = destTables_.size();
    table = Util::createTable(colNames, colTypes, rowNum, rowNum);
    for(int i = 0; i < columnNum; i++)
        columnVecs.push_back(table->getColumn(i));
    int i = 0;
    for(auto &destTable: destTables_){
        columnVecs[0]->set(i, Util::createString(destTable.second->dbName));
        columnVecs[1]->set(i, Util::createString(destTable.second->tableName));
        columnVecs[2]->set(i, Util::createInt(destTable.second->writeQueue.size()));
        columnVecs[3]->set(i, Util::createInt(destTable.second->sendedRows));
        columnVecs[4]->set(i, Util::createBool(destTable.second->destroy));
        columnVecs[5]->set(i, Util::createBool(destTable.second->finished));
        i++;
    }
    return table;
}

TableSP BatchTableWriter::getUnwrittenData(const string& dbName, const string& tableName){
    RWLockGuard<RWLock> _(&rwLock, true, acquireLock_);
    if(destTables_.find(std::make_pair(dbName, tableName)) == destTables_.end())
        throw RuntimeException("Failed to get unwritten data. Please use addTable to add infomation of database and table first.");
    SmartPointer<DestTable> destTable = destTables_[std::make_pair(dbName, tableName)];

    int saveQueueSize = destTable->saveQueue.size();
    int writeQueueSize = destTable->writeQueue.size();
    int size = saveQueueSize + writeQueueSize;
    TableSP table = Util::createTable(destTable->colNames, destTable->colTypes, 0, size);

    std::vector<std::vector<ConstantSP>> items;
    items.reserve(size);
    destTable->saveQueue.pop(items, saveQueueSize);
    destTable->writeQueue.pop(items, writeQueueSize);

    size = items.size();

    INDEX insertedRows;
    std::string errMsg;
    for(int i = 0; i < size; i++){
        bool ret = table->append(items[i], insertedRows, errMsg);
        if(!ret)
            throw RuntimeException("Failed to create table, with error: " + errMsg);
    }
    return table;
}

void BatchTableWriter::removeTable(const string& dbName, const string& tableName){
    SmartPointer<DestTable> destTable;
    {
        RWLockGuard<RWLock> _(&rwLock, true, acquireLock_);
        if(destTables_.find(std::make_pair(dbName, tableName)) != destTables_.end()){
            destTable = destTables_[std::make_pair(dbName, tableName)];
            if(destTable->destroy)
                return;
            else{
                destTable->destroy = true;
                destTable->writeNotifier.notify();
            }
        }
    }
    if(!destTable.isNull()){
        destTable->writeThread->join();
        destTable->conn->close();

        RWLockGuard<RWLock> _(&rwLock, true, acquireLock_);
        destTables_.erase(std::make_pair(dbName, tableName));
    }
}

ConstantSP BatchTableWriter::createObject(int dataType, Constant* val){
    return val;
}
ConstantSP BatchTableWriter::createObject(int dataType, ConstantSP val){
    return val;
}
ConstantSP BatchTableWriter::createObject(int dataType, char val){
    switch(dataType){
        case DATA_TYPE::DT_BOOL:
            return Util::createBool(val);
            break;
        case DATA_TYPE::DT_CHAR:
            return Util::createChar(val);
            break;
        default:
            throw RuntimeException("Failed to insert data, unsupported data type.");
            break;
    }
}
ConstantSP BatchTableWriter::createObject(int dataType, short val){
    switch(dataType){
        case DATA_TYPE::DT_SHORT:
            return Util::createShort(val);
            break;
        default:
            throw RuntimeException("Failed to insert data, unsupported data type.");
            break;
    }
}
ConstantSP BatchTableWriter::createObject(int dataType, const char* val){
    switch(dataType){
        case DATA_TYPE::DT_SYMBOL:
            {
                ConstantSP tmp = Util::createConstant(DATA_TYPE::DT_SYMBOL);
                tmp->setString(val);
                return tmp;
            }
            break;
        case DATA_TYPE::DT_STRING:
            return Util::createString(val);
            break;
        default:
            throw RuntimeException("Failed to insert data, unsupported data type.");
            break;
    }
}
ConstantSP BatchTableWriter::createObject(int dataType, std::string val){
    switch(dataType){
        case DATA_TYPE::DT_SYMBOL:
            {
                ConstantSP tmp = Util::createConstant(DATA_TYPE::DT_SYMBOL);
                tmp->setString(val);
                return tmp;
            }
            break;
        case DATA_TYPE::DT_STRING:
            return Util::createString(val);
            break;
        default:
            throw RuntimeException("Failed to insert data, unsupported data type.");
            break;
    }
}
ConstantSP BatchTableWriter::createObject(int dataType, const unsigned char* val){
    switch(dataType){
        case DATA_TYPE::DT_INT128:
            {
                ConstantSP tmp = Util::createConstant(DATA_TYPE::DT_INT128);
                tmp->setBinary(val, 16);
                return tmp;
            }
            break;
        case DATA_TYPE::DT_UUID:
            {
                ConstantSP tmp = Util::createConstant(DATA_TYPE::DT_UUID);
                tmp->setBinary(val, 16);
                return tmp;
            }
            break;
        case DATA_TYPE::DT_IP:
            {
                ConstantSP tmp = Util::createConstant(DATA_TYPE::DT_IP);
                tmp->setBinary(val, 16);
                return tmp;
            }
            break;
        default:
            throw RuntimeException("Failed to insert data, unsupported data type.");
            break;
    }
}
ConstantSP BatchTableWriter::createObject(int dataType, unsigned char val[]){
    return createObject(dataType, (const unsigned char*)val);
}
ConstantSP BatchTableWriter::createObject(int dataType, long long val){
    switch(dataType){
        case DATA_TYPE::DT_LONG:
            return Util::createLong(val);
            break;
        case DATA_TYPE::DT_NANOTIME:
            return Util::createNanoTime(val);
            break;
        case DATA_TYPE::DT_NANOTIMESTAMP:
            return Util::createNanoTimestamp(val);
            break;
        case DATA_TYPE::DT_TIMESTAMP:
            return Util::createTimestamp(val);
            break;
        default:
            throw RuntimeException("Failed to insert data, unsupported data type.");
            break;
    }
}
ConstantSP BatchTableWriter::createObject(int dataType, float val){
    switch(dataType){
        case DATA_TYPE::DT_FLOAT:
            return Util::createFloat(val);
            break;
        default:
            throw RuntimeException("Failed to insert data, unsupported data type.");
            break;
    }
}
ConstantSP BatchTableWriter::createObject(int dataType, double val){
    switch(dataType){
        case DATA_TYPE::DT_DOUBLE:
            return Util::createDouble(val);
            break;
        default:
            throw RuntimeException("Failed to insert data, unsupported data type.");
            break;
    }
}
ConstantSP BatchTableWriter::createObject(int dataType, int val){
    switch(dataType){
        case DATA_TYPE::DT_INT:
            return Util::createInt(val);
            break;
        case DATA_TYPE::DT_DATE:
            return Util::createDate(val);
            break;
        case DATA_TYPE::DT_MONTH:
            return Util::createMonth(val);
            break;
        case DATA_TYPE::DT_TIME:
            return Util::createTime(val);
            break;
        case DATA_TYPE::DT_SECOND:
            return Util::createSecond(val);
            break;
        case DATA_TYPE::DT_MINUTE:
            return Util::createMinute(val);
            break;
        case DATA_TYPE::DT_DATETIME:
            return Util::createDateTime(val);
            break;
        case DATA_TYPE::DT_DATEHOUR:
            return Util::createDateHour(val);
            break;
        default:
            throw RuntimeException("Failed to insert data, unsupported data type.");
            break;
    }
}
};

