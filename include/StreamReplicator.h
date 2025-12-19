// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4251 )
#endif

#include "DolphinDB.h"
#include "Logger.h"

#include <chrono>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace dolphindb
{

struct HostInfo {
    std::string ip;
    int port;
    std::string userId;
    std::string password;
    std::string label;
};

class EXPORT_DECL ReplicatorConfig
{
    friend class StreamReplicator;

  public:
    ReplicatorConfig() = default;
    template <typename Rep, typename Period>
    ReplicatorConfig &setBatching(const size_t batchSize, const std::chrono::duration<Rep, Period> batchInterval)
    {
        if (batchSize < 1) {
            throw RuntimeException("The parameter batchSize must be greater than or equal to 1");
        }
        if (batchInterval.count() <= 0) {
            throw RuntimeException("The parameter batchInterval must be greater than 0");
        }
        batchSize_ = batchSize;
        batchInterval_ = batchInterval;
        return *this;
    }
    template <typename Rep, typename Period>
    ReplicatorConfig &setRetry(const size_t maxRetry, const std::chrono::duration<Rep, Period> retryInterval)
    {
        if (maxRetry > std::numeric_limits<int>::max() && maxRetry != static_cast<size_t>(-1)) {
            throw IllegalArgumentException(DDB_FUNCNAME, "maxRetry must be less than " + std::to_string(std::numeric_limits<int>::max()));
        }
        if (retryInterval.count() < 0) {
            throw IllegalArgumentException(DDB_FUNCNAME, "retryInterval must be greater than 0.");
        }
        maxRetry_ = maxRetry;
        retryInterval_ = retryInterval;
        return *this;
    }
    ReplicatorConfig &setCompression(const std::vector<COMPRESS_METHOD> &compressMethods)
    {
        compression_ = compressMethods;
        return *this;
    }
    using DataCallback = std::function<bool(const std::string &hostLabel, ConstantSP table)>;
    ReplicatorConfig &onDataDump(const DataCallback &callback)
    {
        dataCallback_ = callback;
        return *this;
    }
    using StateCallback = std::function<bool(const std::string &hostLabel, ConnectionState state)>;
    ReplicatorConfig &onConnectionStateChange(const StateCallback &callback)
    {
        stateCallback_ = callback;
        return *this;
    }

  private:
    size_t batchSize_{1};
    int maxRetry_{3};
    std::chrono::milliseconds batchInterval_{0};
    std::chrono::milliseconds retryInterval_{1000};
    std::vector<COMPRESS_METHOD> compression_;
    DataCallback dataCallback_{[](const std::string &, ConstantSP) { return true; }};
    StateCallback stateCallback_{[](const std::string &, const ConnectionState) { return true; }};
};

struct ReplicatorStreamStatus {
    size_t insertedRows{0};
    size_t dumpedRows{0};
    ConnectionState connectionState_;
    int maxRetry{-1};
    std::string errorMsg;
};

struct ReplicatorStatus {
    size_t totalRows{0};
    std::map<std::string, ReplicatorStreamStatus> streams; // key: hostName
};

class EXPORT_DECL StreamReplicator
{
  public:
    explicit StreamReplicator(const std::vector<HostInfo> &hosts, const std::string &tableName,
                              const ReplicatorConfig &config = ReplicatorConfig());
    StreamReplicator(const StreamReplicator &) = delete;
    StreamReplicator(StreamReplicator &&) = delete;
    StreamReplicator &operator=(const StreamReplicator &) = delete;
    StreamReplicator &operator=(StreamReplicator &&) = delete;
    ~StreamReplicator();

    template <typename... TArgs> void insert(TArgs... args)
    {
        std::unique_lock<std::mutex> lock(queueMutex_);
        auto argSize = sizeof...(args);
        if (argSize != colTypes_.size()) {
            throw IllegalArgumentException(DDB_FUNCNAME, "column number mismatch");
        }
        ErrorCodeInfo errorInfo;
        if (oneRowData_.empty()) {
            int size = colTypes_.size();
            for (int i = 0; i < size; ++i) {
                if (colTypes_[i] >= ARRAY_TYPE_BASE) {
                    oneRowData_.emplace_back(Util::createVector(colTypes_[i], 0, 0, true, colExtras_[i]));
                } else {
                    oneRowData_.emplace_back(Util::createConstant(colTypes_[i], colExtras_[i]));
                }
            }
        }
        int idx1 = 0;
        int idx2 = 0;
        int idx3 = 0;
        bool results[] = {
            Util::setValue(oneRowData_[idx1++], getColDataType(idx2++), args, errorInfo, colExtras_[idx3++])...};
        if (errorInfo.hasError()) {
            for (int i = static_cast<int>(colTypes_.size() - 1); i >= 0; --i) {
                if (results[i] == false) {
                    throw IllegalArgumentException(DDB_FUNCNAME, "Failed to convert data type for column [" + colNames_[i] + "]: " + errorInfo.errorInfo);
                }
            }
        }
        insert(oneRowData_);
    }

    ReplicatorStatus getStatus()
    {
        std::unique_lock<std::mutex> lock(queueMutex_);
        return status_;
    }

  private:
    struct DataBlock {
        TableSP data;
        size_t cnt;
    };
    using BlockIt = std::list<DataBlock>::iterator;

    void sendData(const std::shared_ptr<DBConnection> &conn, ReplicatorStreamStatus &status);
    BlockIt flush(const std::shared_ptr<DBConnection> &conn, ReplicatorStreamStatus &status, BlockIt front,
                  bool flushAll);
    void insert(std::vector<ConstantSP> &records);
    DATA_TYPE getColDataType(int colIndex)
    {
        DATA_TYPE dataType = colTypes_[colIndex];
        if (dataType >= ARRAY_TYPE_BASE)
            dataType = (DATA_TYPE)(dataType - ARRAY_TYPE_BASE);
        return dataType;
    }

    ReplicatorConfig config_;

    std::mutex queueMutex_;
    ReplicatorStatus status_;
    std::list<DataBlock> writeQueue_;
    BlockIt inputBlock_;
    std::condition_variable queueCond_;

    std::vector<std::thread> threads_;
    std::vector<std::shared_ptr<DBConnection>> conns_;
    std::atomic<bool> exited_{false};

    std::vector<std::string> colNames_;
    std::vector<DATA_TYPE> colTypes_;
    std::vector<int> colExtras_;
    std::string scriptTableInsert_;

    std::vector<ConstantSP> oneRowData_;
};

} // namespace dolphindb

#ifdef _MSC_VER
#pragma warning( pop )
#endif
