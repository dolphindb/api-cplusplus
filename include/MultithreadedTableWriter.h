// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include "Exports.h"
#include "Concurrent.h"
#include "Util.h"
#include "Types.h"
#include "Exceptions.h"
#include "ScalarImp.h"
#include "Dictionary.h"
#include "DolphinDB.h"
#include <unordered_map>
#include <string>
#include <vector>
#include <list>
#include <functional>
#include <tuple>
#include <cassert>
#include <unordered_map>
#include <mutex>
#include <chrono>

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4251 )
#endif

namespace dolphindb{

inline const std::string TableScript(const std::string& dbName, const std::string& tableName)
{
    return "loadTable(\"" + dbName + "\",\"" + tableName + "\")";
}

enum class WriteMode {
    Append,
    Upsert,
};

enum class MTWState {
    Initializing,      // MTW初始化阶段
    ConnectedOne,      // 一个连接已建立
    ConnectedAll,      // 所有连接均已建立
    ReconnectingOne,   // 一个连接正在重试
    ReconnectingAll,   // 所有连接正在重试
    Terminated         // MTW退出
};

class EXPORT_DECL MTWConfig {
    friend class MultithreadedTableWriter;
public:
    MTWConfig(const std::shared_ptr<DBConnection> conn, const std::string &tableName);
    template<typename Rep, typename Period>
    MTWConfig& setBatching(const size_t batchSize, const std::chrono::duration<Rep,Period> throttle)
    {
        if (batchSize < 1) {
            throw RuntimeException("The parameter batchSize must be greater than or equal to 1");
        }
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(throttle).count();
        if (ms <= 0) {
            throw RuntimeException("The parameter throttle must be greater than 0");
        }
        inited_ = true;
        batchSize_ = batchSize;
        throttleTimeMs_ = static_cast<size_t>(ms);
        return *this;
    }
    MTWConfig& setCompression(const std::vector<COMPRESS_METHOD> &compressMethods);
    MTWConfig& setThreads(const size_t threadNum, const std::string& partitionColumnName);
    MTWConfig& setWriteMode(const WriteMode mode, const std::vector<std::string> &option = std::vector<std::string>());
    MTWConfig& setStreamTableTimestamp();
    using dataCallbackT = std::function<void(ConstantSP)>;
    MTWConfig& onDataWrite(const dataCallbackT &callback)
    {
        dataCallback_ = callback;
        return *this;
    }
    using stateCallbackT = std::function<bool(const MTWState state, const std::string &host, const int port)>;
    MTWConfig& onConnectionStateChange(const stateCallbackT &callback)
    {
        stateCallback_ = callback;
        return *this;
    }

private:
    void getStringVector(const ConstantSP &in, std::vector<std::string> &out)
    {
        int size = in->size();
        for (int i = 0; i < size; ++i) {
            out.push_back(in->getString(i));
        }
    }
    std::shared_ptr<DBConnection> conn_;
    std::string tableName_;
    stateCallbackT stateCallback_{[](const MTWState, const std::string &, const int){ return true; }};
    WriteMode writeMode_{WriteMode::Append};
    std::vector<std::string> writeOptions_;
    size_t threadNum_{1};
    std::string partitionColumnName_;
    size_t batchSize_{1};
    size_t throttleTimeMs_{10};
    std::vector<COMPRESS_METHOD> compression_;
    bool hasStreamTableTimestamp_{false};
    std::function<void(ConstantSP)> dataCallback_{nullptr};

    // table info
    bool inited_{false};
    DictionarySP schema_;
    std::vector<std::string> partitionColumnNames_;
    std::vector<std::pair<std::string, DATA_TYPE>> columnInfo_;
    TableSP colDefs_;
    std::string insertScript_;
    size_t partitionColumnIndex_;
};

class EXPORT_DECL MultithreadedTableWriter {
public:
    enum Mode{
        M_Append,
        M_Upsert,
    };
    struct ThreadStatus{
        long threadId;
        long sentRows,unsentRows,sendFailedRows;
        ThreadStatus(){
            threadId = 0;
            sentRows = unsentRows = sendFailedRows = 0;
        }
    };
    struct Status : ErrorCodeInfo{
        bool isExiting;
        long sentRows, unsentRows, sendFailedRows;
        std::vector<ThreadStatus> threadStatus_;
        void plus(const ThreadStatus &threadStatus){
            sentRows += threadStatus.sentRows;
            unsentRows += threadStatus.unsentRows;
            sendFailedRows += threadStatus.sendFailedRows;
        }
    };

    MultithreadedTableWriter(const MTWConfig &config);
    MultithreadedTableWriter(const std::string& host, int port, const std::string& userId, const std::string& password,
        const std::string& dbPath, const std::string& tableName, bool useSSL, bool enableHighAvailability = false, const std::vector<std::string> *pHighAvailabilitySites = nullptr,
        int batchSize = 1, float throttle = 0.01f,int threadCount = 1, const std::string& partitionCol ="",
        const std::vector<COMPRESS_METHOD> *pCompressMethods = nullptr, Mode mode = M_Append,
        std::vector<std::string> *pModeOption = nullptr, const std::function<void(ConstantSP)> &callbackFunc = nullptr, bool enableStreamTableTimestamp = false);
    MultithreadedTableWriter(const MultithreadedTableWriter &) = delete;
    MultithreadedTableWriter(MultithreadedTableWriter &&) = delete;
    MultithreadedTableWriter& operator=(const MultithreadedTableWriter &) = delete;
    MultithreadedTableWriter& operator=(MultithreadedTableWriter &&) = delete;
    virtual ~MultithreadedTableWriter();

    template<typename... TArgs>
    bool insert(ErrorCodeInfo &errorInfo, TArgs... args) {
        RWLockGuard<RWLock> guard(&insertRWLock_, true);
        if (hasError_.load()) {
            throw RuntimeException("Thread is exiting for " + errorInfo_.errorInfo);
        }
        {
            auto argSize = sizeof...(args);
            if (argSize != colTypes_.size()) {
                errorInfo.set(ErrorCodeInfo::EC_InvalidParameter, "Column counts don't match " + std::to_string(argSize));
                return false;
            }
        }
        {
            errorInfo.clearError();
            int colIndex1 = 0, colIndex2 = 0, colIndex3 = 0;
            if(oneRowData_.empty()){
                int size = colTypes_.size();
                for(int i = 0; i < size; ++i){
                    if(colTypes_[i] >= ARRAY_TYPE_BASE){
                        oneRowData_.push_back(Util::createVector(colTypes_[i], 0, 0, true, colExtras_[i]));
                    }else{
                        oneRowData_.push_back(Util::createConstant(colTypes_[i], colExtras_[i]));
                    }
                }
            }
            bool results[] = {Util::setValue(oneRowData_[colIndex1++], getColDataType(colIndex2++), args, errorInfo, colExtras_[colIndex3++])...};
            if (errorInfo.hasError()){
                for(int i = static_cast<int>(colTypes_.size() - 1); i >= 0; --i){
                    if(results[i] == false){
                        errorInfo.errorInfo.append(" for col ").append(std::to_string(i + 1));
                        return false;
                    }
                }
                return false;
            }
            std::vector<ConstantSP>* pRow = &oneRowData_;
            return insert(&pRow, 1, errorInfo);
        }
    }

    void waitForThreadCompletion();
    void getStatus(Status &status);
    void getUnwrittenData(std::vector<std::vector<ConstantSP>*> &unwrittenData);
    bool insertUnwrittenData(std::vector<std::vector<ConstantSP>*> &records, ErrorCodeInfo &errorInfo) { return insert(records.data(), static_cast<int>(records.size()), errorInfo, true); }

private:
    bool stateCallback(const ConnectionState state, const std::string &host, int port);
    bool insert(std::vector<ConstantSP> **records, int recordCount, ErrorCodeInfo &errorInfo, bool isNeedReleaseMemory = false);
    void setError(int code, const std::string &info);
    void setError(const ErrorCodeInfo &errorInfo);
    DATA_TYPE getColDataType(int colIndex) {
        DATA_TYPE dataType = colTypes_[colIndex];
        if (dataType >= ARRAY_TYPE_BASE)
            dataType = (DATA_TYPE)(dataType - ARRAY_TYPE_BASE);
        return dataType;
    }
    void insertThreadWrite(int threadhashkey, std::vector<ConstantSP> *prow);
    static void callBack(std::function<void(ConstantSP)> callbackFunc, bool result, std::vector<ConstantSP>* block);
    std::vector<ConstantSP>* createColumnBlock();
    struct WriterThread {
        WriterThread() : nonemptySignal(false,true){}
        std::shared_ptr<DBConnection> conn;

        std::list<std::vector<ConstantSP>*> writeQueue_;
        std::vector<std::vector<ConstantSP>*> failedQueue_;
        ThreadSP writeThread;
        Signal nonemptySignal;

        Mutex mutex_, writeQueueMutex_;
        unsigned int threadId;
        long sentRows;
        bool exit;
    };
    class SendExecutor : public dolphindb::Runnable {
    public:
        SendExecutor(MultithreadedTableWriter &tableWriter,WriterThread &writeThread):
                        tableWriter_(tableWriter),
                        writeThread_(writeThread){};
        virtual void run();
    private:
        void callBack();
        bool isExit() { return tableWriter_.hasError_.load() || writeThread_.exit; }
        bool init();
        bool writeAllData();
        MultithreadedTableWriter &tableWriter_;
        WriterThread &writeThread_;
    };

private:
    friend class SendExecutor;
    friend class InsertExecutor;
    std::string dbName_;
    std::string tableName_;
    int batchSize_{1};
    int perBlockSize_{0};               //每一段预分配的空间可以容纳的行数 perBlockSize_ = batchSize < 65535 ? 65535 : batchSize_
    int throttleMilsecond_{10};
    bool isPartionedTable_{false};
    bool exited_{false};
    std::atomic_bool hasError_{false};
    //all column include callback id
    std::vector<std::string> colNames_;
    std::vector<DATA_TYPE> colTypes_;
    std::vector<int> colExtras_;
    //columns except callback id which no need to save.
    std::vector<std::string> saveColNames_;
    std::vector<DATA_TYPE> saveColTypes_;
    std::vector<int> saveColExtras_;
    std::vector<COMPRESS_METHOD> saveCompressMethods_;
    //Following parameters only valid in multithread mode
    SmartPointer<Domain> partitionDomain_;
    int partitionColumnIdx_;
    int threadByColIndexForNonPartion_;
    //End of following parameters only valid in multithread mode
    std::vector<WriterThread> threads_;
    ErrorCodeInfo errorInfo_;
    std::string scriptTableInsert_;

    SynchronizedQueue<std::vector<ConstantSP>*> unusedQueue_;
    Mode mode_{M_Append};
    std::function<void(ConstantSP)> callbackFunc_{nullptr};
    RWLock insertRWLock_;

    bool enableStreamTableTimestamp_{false};
    std::vector<ConstantSP> oneRowData_;
    MTWConfig::stateCallbackT stateCallback_;
    std::string host_;
    int port_;
    std::string user_;
    std::string pswd_;
    bool ssl_;
    std::vector<std::string> haSites_;
    int threadCount_{1};
    std::string partitionCol_;
    std::vector<std::string> modeOption_;

    std::shared_ptr<DBConnection> conn_;
    std::mutex stateMtx_;
    MTWState state_{MTWState::Initializing};
    size_t readyThreadCnt_{0};
};

}

#ifdef _MSC_VER
#pragma warning( pop )
#endif
