// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include "Exports.h"
#include "Constant.h"
#include "Concurrent.h"
#include "Dictionary.h"
#include "Vector.h"
#include "ErrorCodeInfo.h"

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4251 )
#endif

namespace dolphindb {

class DBConnection;

//using Message = ConstantSP;
class Message;
class StreamDeserializer;
typedef SmartPointer<StreamDeserializer> StreamDeserializerSP;
using MessageQueue = BlockingQueue<Message>;
using MessageQueueSP = SmartPointer<MessageQueue>;
using MessageHandler = std::function<void(Message)>;
using MessageBatchHandler = std::function<void(std::vector<Message>)>;
using EventMessageHandler = std::function<void(const std::string&, std::vector<ConstantSP>&)>;
using IPCInMemoryTableReadHandler = std::function<void(ConstantSP)>;
using MessageBatchHandlerUDP = std::function<void(MessageQueue)>;

#define DEFAULT_ACTION_NAME "cppStreamingAPI"

class EXPORT_DECL StreamDeserializer {
public:
    //symbol->[dbPath,tableName], dbPath can be empty for table in memory.
    StreamDeserializer(const std::unordered_map<std::string, std::pair<std::string, std::string>> &sym2tableName, DBConnection *pconn = nullptr);
    StreamDeserializer(const std::unordered_map<std::string, DictionarySP> &sym2schema);
    // do not use this constructor if there are decimal or decimal-array columns (need schema to get decimal scale)
    StreamDeserializer(const std::unordered_map<std::string, std::vector<DATA_TYPE>> &symbol2col);
    virtual ~StreamDeserializer() = default;
    bool parseBlob(const ConstantSP &src, std::vector<VectorSP> &rows, std::vector<std::string> &symbols, ErrorCodeInfo &errorInfo);
private:
    class TableInfo{
    public:
        TableInfo(std::vector<DATA_TYPE> cols) : cols_(cols), scales_(cols.size()), queuelimit_(65535){
        }
        TableInfo(std::vector<DATA_TYPE> cols, std::vector<int> scales) : cols_(cols), scales_(scales), queuelimit_(65535){
        }
        ConstantSP newTuple();
        void setLimit(INDEX limit){
            queuelimit_ = limit;
        }
        void returnTuple(ConstantSP tuple){
            LockGuard<Mutex> locker(&mutex_);
            if((INDEX) queue_.size() < queuelimit_){
                queue_.push_back(tuple);
            }
        }
    private:
        std::vector<DATA_TYPE> cols_;
        std::vector<int> scales_;
        INDEX queuelimit_;
        std::vector<ConstantSP> queue_;
        Mutex mutex_;
    };
    void setTupleLimit(INDEX limit);
    void returnMessage(Message *msg);
    void create(DBConnection &conn);
    void parseSchema(const std::unordered_map<std::string, DictionarySP> &sym2schema);
    std::unordered_map<std::string, std::pair<std::string, std::string>> sym2tableName_;
    std::unordered_map<std::string, SmartPointer<TableInfo>> symbol2tableInfo_;
    Mutex mutex_;
    friend class StreamingClientImpl;
    friend class Message;
};

class EXPORT_DECL Message : public ConstantSP {
public:
    Message() : offset_(-1) {
    }
    Message(const ConstantSP &sp, int offset = -1) : ConstantSP(sp), offset_(offset) {
    }
    Message(const ConstantSP &sp, const std::string & symbol, int offset = -1) : ConstantSP(sp), symbol_(symbol), offset_(offset) {
    }
    Message(const ConstantSP &sp, const std::string &symbol, const StreamDeserializerSP &sd, int offset = -1)
            : ConstantSP(sp), symbol_(symbol), sd_(sd), offset_(offset) {
    }
    Message(const Message &msg) : ConstantSP(msg), symbol_(msg.symbol_), offset_(msg.offset_) {
    }
    ~Message() {
        if(sd_.isNull()==false && count()==1){
            sd_->returnMessage(this);
            sd_.clear();
        }
        clear();
    }
    Message& operator =(const Message& msg) {
        ConstantSP::operator=(msg);
        symbol_ = msg.symbol_;
        sd_ = msg.sd_;
        offset_ = msg.offset_;
        return *this;
    }
    const std::string& getSymbol() { return symbol_; }
    int getOffset() {return offset_;}
private:
    std::string symbol_;
    StreamDeserializerSP sd_;
    int offset_;
};
}

#ifdef _MSC_VER
#pragma warning( pop )
#endif
