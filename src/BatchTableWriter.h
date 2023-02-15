#ifndef BATCHTABLEWRITER_H_
#define BATCHTABLEWRITER_H_

#include "Concurrent.h"
#include "DolphinDB.h"
#include "Util.h"
#include "Types.h"
#include "Exceptions.h"
#include <unordered_map>
#include <string>
#include <vector>
#include <memory>
#include <functional>
#include <tuple>
#include <cassert>

#ifdef _MSC_VER
	#ifdef _DDBAPIDLL	
		#define EXPORT_DECL _declspec(dllexport)
	#else
		#define EXPORT_DECL __declspec(dllimport)
	#endif
#else
	#define EXPORT_DECL 
#endif

namespace dolphindb{

class EXPORT_DECL  BatchTableWriter {
public:
    /**
     * If fail to connect to the specified DolphinDB server, this function throw an exception.
     */
    BatchTableWriter(const std::string& hostName, int port, const std::string& userId, const std::string& password, bool acquireLock=true);

    ~BatchTableWriter();

    BatchTableWriter(const BatchTableWriter&) = delete;

    BatchTableWriter(const BatchTableWriter&&) = delete;

    /**
     * Add the name of the database and table that you want to insert data into before actually call insert.
     * The parameter partitioned indicates whether the added table is a partitioned table. If this function
     * is called to add an in-memory table, the parameter dbName indicates the name of the shared in-memory
     * table, and the parameter tableName should be a null string. If error is raised on the server, this
     * function throws an exception.
     */
    void addTable(const string& dbName, const string& tableName="", bool partitioned=true);

    /**
     * Gets the current size of the specified table write queue and whethre the specified table is removed. If
     * the specified table is not added first, this function throw and exception.
     */
    std::tuple<int,bool,bool> getStatus(const string& dbName, const string& tableName="");

    /**
     * Gets the current size of all table write queue, number of rows already sended to server, whether a
     * table is being removed, and whether the backgroud thread is returned because of error.
     */
    TableSP getAllStatus();

    TableSP getUnwrittenData(const string& dbName, const string& tableName="");

    /**
     * Release the resouces occupied by the specified table, including write queue and write thread. If this
     * function is called to add an in-memory table, the parameter dbName indicates the name of the in-memory
     * table, and the parameter tableName should be a null string.
     */
    void removeTable(const string& dbName, const string& tableName="");

    /**
     * Insert a row into the specified table. If this function is called to insert data into an in-memory table,
     * the parameter dbName indicates the name of the shared in-memory table, and one must pass in a null string
     * to the parameter tableName. If the number of parameters does not match the number of columns of added table,
     * this function throws an exception. If the type of the parameter and the type of the corresponding column
     * cannot be matched or converted, this function throws an exception. If error is raised on the server,
     * this function throws an exception. If the specified table is being removed, this function throws an exception.
     * If the background thread fails to write a row, it will print error message to std::cerr and return.
     */
    template<typename... Targs>
    void insert(const string& dbName, const string& tableName, Targs... Fargs){
        //RECORDTIME("BTW::insert");
        SmartPointer<DestTable> destTable;
        {
            RWLockGuard<RWLock> _(&rwLock, false, acquireLock_);
            if(destTables_.find(std::make_pair(dbName, tableName)) == destTables_.end())
                throw RuntimeException("Failed to insert into table, please use addTable to add infomation of database and table first.");
            destTable = destTables_[std::make_pair(dbName, tableName)];
        }
        assert(!destTable.isNull());
        if(destTable->destroy)
            throw RuntimeException("Failed to insert into table, the table is being removed.");
        auto argSize = sizeof...(Fargs);
        if(argSize != destTable->columnNum)
            throw RuntimeException("Failed to insert into table, number of arguments must match the number of columns of table.");
        if(argSize == 0)
            return;
        SmartPointer<std::vector<ConstantSP>> row(new std::vector<ConstantSP>());
        row->reserve(destTable->columnNum);
        insertRecursive(row.get(), destTable.get(), 0, Fargs...);
    }

private:
    struct DestTable{
        SmartPointer<DBConnection> conn;
        std::string dbName;
        std::string tableName;
        std::string tableInsert;
        std::string saveTable;
        TableSP colDefs;
        int columnNum;
        ConstantSP colDefsTypeInt;
        std::vector<string> colNames;
        std::vector<DATA_TYPE> colTypes;
        std::string createTmpSharedTable;
        SynchronizedQueue<std::vector<ConstantSP>> writeQueue;
        SynchronizedQueue<std::vector<ConstantSP>> saveQueue;
        ThreadSP writeThread;
        TableSP writeTable;

        Mutex writeMutex;
        ConditionalVariable writeNotifier;
        
        int sendedRows = 0;
        bool destroy = false;
        bool finished = false;
    };
    //write failed or no data
    bool writeTableAllData(SmartPointer<DestTable> destTable,bool partitioned);
    void insertRecursive(std::vector<ConstantSP>* row, DestTable* destTable, int colIndex){
        assert(colIndex == destTable->columnNum);
        RWLockGuard<RWLock> _(&rwLock, false, acquireLock_);
        if(destTable->finished){
            throw RuntimeException(std::string("Failed to insert data. Error writing data in backgroud thread. Please use getUnwrittenData to get data not written to server and remove talbe (") + destTable->dbName + " " + destTable->tableName + ").");
        }
        destTable->writeQueue.push(std::move(*row));
        destTable->writeNotifier.notify();
    }

    template<typename T, typename... Targs>
    void insertRecursive(std::vector<ConstantSP>* row, DestTable* destTable, int colIndex, T first, Targs... Fargs){
        ConstantSP tmp;
        try{
            tmp =  createObject(destTable->colDefsTypeInt->getInt(colIndex), first);
        }
        catch(RuntimeException &e){
            throw RuntimeException("Failed to insert data, unsupported data type at column: " + std::to_string(colIndex));
        }
        if(destTable->colDefsTypeInt->getInt(colIndex) != DT_SYMBOL && int(tmp->getType()) != destTable->colDefsTypeInt->getInt(colIndex))
            throw RuntimeException("Failed to insert data, the type of argument does not match the type of column at column: " + std::to_string(colIndex));
        row->push_back(tmp);
        colIndex++;
        insertRecursive(row, destTable, colIndex, Fargs...);
    }

    template<typename T>
    ConstantSP createObject(int dataType, T val){
        throw RuntimeException("Failed to insert data, unsupported data type.");
    }
    ConstantSP createObject(int dataType, Constant* val);
    ConstantSP createObject(int dataType, ConstantSP val);
    ConstantSP createObject(int dataType, char val);
    ConstantSP createObject(int dataType, short val);
    ConstantSP createObject(int dataType, const char* val);
    ConstantSP createObject(int dataType, std::string val);
    ConstantSP createObject(int dataType, const unsigned char* val);
    ConstantSP createObject(int dataType, unsigned char val[]);
    ConstantSP createObject(int dataType, long long val);
    ConstantSP createObject(int dataType, float val);
    ConstantSP createObject(int dataType, double val);
    ConstantSP createObject(int dataType, int val);

private:
    const std::string hostName_;
    const int port_;
    const std::string userId_;
    const std::string password_;
    bool acquireLock_;

    struct pairHash{
        std::size_t operator()(std::pair<std::string,std::string> const& p) const noexcept{
            std::size_t h1 = std::hash<std::string>{}(p.first);
            std::size_t h2 = std::hash<std::string>{}(p.second);
            return h1 ^ h2;
        }
    };
    std::unordered_map<std::pair<std::string,std::string>, SmartPointer<DestTable>, pairHash> destTables_;
    RWLock rwLock;
};


};
#endif /* BATCHTABLEWRITER_H_ */
