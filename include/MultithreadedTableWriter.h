#ifndef MUTITHREADEDTABLEWRITER_H_
#define MUTITHREADEDTABLEWRITER_H_

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
#include <unordered_map>

#ifdef _MSC_VER
	#define EXPORT_DECL _declspec(dllexport)
#else
	#define EXPORT_DECL 
#endif

#ifndef RECORDTIME
#undef RECORDTIME
#endif
#define RECORDTIME(name) //RecordTime _recordTime(name)

namespace dolphindb{

class EXPORT_DECL  MultithreadedTableWriter {
public:
    struct ThreadStatus{
        long threadId;
        long sentRows,unsentRows,sendFailedRows;
    };
    struct Status{
        bool isExiting;
        ErrorCodeInfo errorInfo;
		long sentRows, unsentRows, sendFailedRows;
        std::vector<ThreadStatus> threadStatus;
    };
    /**
     * If fail to connect to the specified DolphinDB server, this function throw an exception.
     */
    MultithreadedTableWriter(const std::string& hostName, int port, const std::string& userId, const std::string& password,
                            const string& dbName, const string& tableName, bool useSSL, bool enableHighAvailability = false, const vector<string> *pHighAvailabilitySites = NULL,
							int batchSize = 1, float throttle = 0.01f,int threadCount = 1, const string& partitionCol ="",
							const vector<COMPRESS_METHOD> *pCompressMethods = NULL);

    ~MultithreadedTableWriter();

    void getStatus(Status &status);
    void getUnwrittenData(std::vector<std::vector<ConstantSP>> &unwrittenData);
    bool insert(const std::vector<std::vector<ConstantSP>> &records, ErrorCodeInfo &errorInfo);
	bool insert(const std::vector<ConstantSP> &record, ErrorCodeInfo &errorInfo);
	void waitForThreadCompletion();

    template<typename... TArgs>
    bool insert(ErrorCodeInfo &errorInfo, TArgs... args){
        //RECORDTIME("MTTW:insertValue");
        if(hasError_){
			errorInfo.set(errorInfo_);
            return false;
        }
        auto argSize = sizeof...(args);
        if(argSize != colTypes_.size()){
			errorInfo.set(ErrorCodeInfo::EC_InvalidParameter,"Argument size mismatch.");
            return false;
        }
        SmartPointer<std::vector<ConstantSP>> prow=new std::vector<ConstantSP>();
        prow->reserve(colTypes_.size());
        return insertRecursive(errorInfo, prow, 0, args...);
    }

private:
    void setError(int code, const string &info);
	DATA_TYPE getColDataType(int colIndex) {
		DATA_TYPE dataType = colTypes_[colIndex];
		if (dataType >= ARRAY_TYPE_BASE)
			dataType = (DATA_TYPE)(dataType - ARRAY_TYPE_BASE);
		return dataType;
	}
	bool insertRecursive(ErrorCodeInfo &errorInfo, SmartPointer<std::vector<ConstantSP>> prow, int colIndex);
    template<typename TA, typename... TArgs>
    bool insertRecursive(ErrorCodeInfo &errorInfo, SmartPointer<std::vector<ConstantSP>> prow, int colIndex, TA first, TArgs... args){
		DATA_TYPE dataType = getColDataType(colIndex);
		ConstantSP tmp;
        try{
            tmp =  Util::createObject(dataType, first);
        }
        catch(RuntimeException &e){
            errorInfo.set(ErrorCodeInfo::EC_InvalidObject, "Unknow type for column '"+colNames_[colIndex]+"' , error: "+e.what());
            return false;
        }
        prow->push_back(tmp);
        colIndex++;
        return insertRecursive(errorInfo, prow, colIndex, args...);
    }
    bool insertThreadWrite(int threadhashkey, const std::vector<ConstantSP> &row, ErrorCodeInfo &errorInfo);

    struct WriterThread{
        SmartPointer<DBConnection> conn;
        
        std::string scriptTableInsert;
        std::string scriptSaveTable;
    
        SynchronizedQueue<std::vector<ConstantSP>> writeQueue;
        SynchronizedQueue<std::vector<ConstantSP>> failedQueue;
        ThreadSP writeThread;

        Mutex writeMutex;
        ConditionalVariable writeNotifier;
        unsigned int threadId;
        long sentRows;
		bool exit;
    };
    class Executor : public dolphindb::Runnable {
    public:
        Executor(MultithreadedTableWriter &tableWriter,WriterThread &writeThread):
                        tableWriter_(tableWriter),
                        writeThread_(writeThread){};
        virtual void run();
    private:
		bool isExit() { return tableWriter_.hasError_ || writeThread_.exit; }
        bool init();
        bool writeAllData();
        MultithreadedTableWriter &tableWriter_;
        WriterThread &writeThread_;
    };
    
private:
    friend class Executor;
    const std::string dbName_;
    const std::string tableName_;
    const int batchSize_;
    const int throttleMilsecond_;
    bool isPartionedTable_, hasError_;
    std::vector<string> colNames_,colTypeString_;
    std::vector<DATA_TYPE> colTypes_;
	std::vector<COMPRESS_METHOD> compressMethods_;
	//Following parameters only valid in multithread mode
    SmartPointer<Domain> partitionDomain_;
    int partitionColumnIdx_;
    int threadByColIndexForNonPartion_;
	//End of following parameters only valid in multithread mode
    std::vector<WriterThread> threads_;
    Mutex tableMutex_;
    ErrorCodeInfo errorInfo_;
};

};

#endif //MUTITHREADEDTABLEWRITER_H_