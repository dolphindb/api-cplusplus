#include "StreamReplicator.h"
namespace dolphindb
{

StreamReplicator::StreamReplicator(const std::vector<HostInfo> &hosts, const std::string &tableName,
                                   const ReplicatorConfig &config)
{
    constexpr size_t minHosts{2};
    if (hosts.size() < minHosts) {
        throw IllegalArgumentException(DDB_FUNCNAME, "StreamReplicator needs at least 2 hosts.");
    }
    for (const auto &host : hosts) {
        auto conn = std::make_shared<DBConnection>(host.ip, host.port, host.userId, host.password);
        conn->setHostLabel(host.label.empty() ? (host.ip + ":" + std::to_string(host.port)) : host.label);
        if (!config_.compression_.empty()) {
            conn->setCompress(true);
        }
        conns_.emplace_back(std::move(conn));
    }
    ConstantSP colDefs{nullptr};
    auto colDefScript = "schema(" + tableName + ").colDefs.values()";
    for (auto &conn : conns_) {
        auto hostColDefs = (TableSP)conn->run(colDefScript);
        if (colDefs.isNull()) {
            colDefs = hostColDefs;
        } else {
            if (!conn->call("eqObj{" + colDefScript + "}", colDefs)->getBool()) {
                throw RuntimeException("Table schema mismatch.");
            }
        }
    }
    // colDefs: name typeString typeInt extra comment
    constexpr INDEX expectedSize{5};
    if (colDefs->size() != expectedSize) {
        throw RuntimeException("Host returned an unexpected table schema.");
    }
    ConstantSP colTypes = colDefs->get(2);
    ConstantSP colExtras = colDefs->get(3);
    int n = colTypes->size();
    ConstantSP colNames = colDefs->get(0);
    for (int i = 0; i < n; ++i) {
        auto type = (DATA_TYPE)colTypes->getInt(i);
        if (type == DT_ANY) {
            throw RuntimeException("Table with column of type ANY is not supported.");
        }
        colNames_.push_back(colNames->getString(i));
        colTypes_.push_back((DATA_TYPE)colTypes->getInt(i));
        colExtras_.push_back(colExtras->getInt(i));
    }

    scriptTableInsert_ = std::string("tableInsert{") + tableName + "}";
    config_ = config;

    auto tbl = (TableSP)conns_.front()->run("select top 0* from " + tableName);
    tbl->setColumnCompressMethods(std::move(config.compression_));
    writeQueue_.push_back({tbl, hosts.size()});
    inputBlock_ = writeQueue_.begin();
    for (const auto &conn : conns_) {
        auto &status = status_.streams[conn->getHostLabel()];
        status.maxRetry = config_.maxRetry_;
        status.connectionState_ = ConnectionState::Connected;
        threads_.emplace_back([this, &conn, &status]() { sendData(conn, status); });
    }
}

void StreamReplicator::sendData(const std::shared_ptr<DBConnection> &conn, ReplicatorStreamStatus &status)
{
    auto front = writeQueue_.begin();
    while (!exited_) {
        {
            std::unique_lock<std::mutex> lock(queueMutex_);
            auto checkFront = [this, &front]() { return (front != inputBlock_) || exited_; };
            if (config_.batchInterval_.count() > 0) {
                queueCond_.wait_for(lock, config_.batchInterval_, checkFront);
            } else {
                queueCond_.wait(lock, checkFront);
            }
        }
        front = flush(conn, status, front, false);
    }
    flush(conn, status, front, true);
}

StreamReplicator::BlockIt StreamReplicator::flush(const std::shared_ptr<DBConnection> &conn,
                                                  ReplicatorStreamStatus &status, BlockIt front, bool flushAll)
{
    while (true) {
        ConstantSP data;
        int maxTry;
        bool reconnect = false;
        {
            std::unique_lock<std::mutex> lock(queueMutex_);
            if(front == writeQueue_.end()){
                break;
            }
            auto back = flushAll ? writeQueue_.end() : inputBlock_;
            if (front == back) {
                if(front->data->size() == 0){
                    break;
                }else{
                    auto tbl = front->data->getInstance();
                    writeQueue_.push_back({tbl, threads_.size()});
                    ++inputBlock_;
                }
            }
            data = (ConstantSP)front->data;
            maxTry = status.maxRetry;
            if (maxTry > 0) {
                ++maxTry;
            }
            if (status.connectionState_ == ConnectionState::Reconnecting) {
                reconnect = true;
            }
        }
        bool success = false;
        // skip last (empty) block
        if (data->size() == 0) {
            success = true;
            reconnect = false;
        }
        for (int retry = 1; !success && (maxTry == -1 || retry <= maxTry); ++retry) {
            try {
                if (reconnect) {
                    conn->reconnect();
                }
                if (data->size() != 0) {
                    ConstantSP ret = conn->call(scriptTableInsert_, data);
                }
                success = true;
            } catch (std::exception &e) {
                std::unique_lock<std::mutex> lock(queueMutex_);
                status.errorMsg = e.what();
                DLogger::Error(status.errorMsg);
                if (status.connectionState_ != ConnectionState::Reconnecting) {
                    status.connectionState_ = ConnectionState::Reconnecting;
                    config_.stateCallback_(conn->getHostLabel(), status.connectionState_);
                }
                if (retry < maxTry) {
                    std::this_thread::sleep_for(config_.retryInterval_);
                }
            }
        }
        if (!success) {
            config_.dataCallback_(conn->getHostLabel(), data);
        }
        {
            std::unique_lock<std::mutex> lock(queueMutex_);
            if (success) {
                status.insertedRows += data->size();
                status.errorMsg.clear();
                status.maxRetry = config_.maxRetry_;
                if (reconnect) {
                    status.connectionState_ = ConnectionState::Connected;
                    config_.stateCallback_(conn->getHostLabel(), status.connectionState_);
                }
            } else {
                status.dumpedRows += data->size();
                status.maxRetry = 0;
            }
            auto prev = front;
            ++front;
            if (prev->cnt == 1) {
                writeQueue_.pop_front();
            } else {
                --prev->cnt;
            }
        }
    }
    return front;
}

StreamReplicator::~StreamReplicator()
{
    exited_ = true;
    queueCond_.notify_all();
    for (auto &thread : threads_) {
        thread.join();
    }
    for (auto &conn : conns_) {
        conn->close();
    }
}

void StreamReplicator::insert(std::vector<ConstantSP> &records)
{
    auto tbl = inputBlock_->data;
    INDEX n;
    std::string err;
    tbl->append(records, n, err);
    if(!err.empty()) {
        throw RuntimeException("Failed to append data to StreamReplicator buffer table: " + err);
    }
    if ((size_t)tbl->size() >= config_.batchSize_) {
        tbl = tbl->getInstance();
        writeQueue_.push_back({tbl, threads_.size()});
        ++inputBlock_;
    }
    status_.totalRows += n;
    queueCond_.notify_all();
}

} // namespace dolphindb
