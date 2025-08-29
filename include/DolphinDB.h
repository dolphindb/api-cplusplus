// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include "Concurrent.h"
#include "Constant.h"
#include "Dictionary.h"
#include "Domain.h"
#include "Exceptions.h"
#include "Exports.h"
#include "SmartPointer.h"
#include "SysIO.h"
#include "Table.h"
#include "Types.h"
#include "Util.h"
#include "Version.h"
#include "internal/WideInteger.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4251 )
#endif

#ifdef USE_OPENSSL
constexpr bool HAS_OPENSSL{true};
#else
constexpr bool HAS_OPENSSL{false};
#endif

namespace dolphindb {

class DBConnectionImpl;
class BlockReader;
class DBConnectionPoolImpl;
class PartitionedTableAppender;
class DBConnection;
class DBConnectionPool;

using BlockReaderSP = SmartPointer<BlockReader>;
using DomainSP = SmartPointer<Domain>;
using DBConnectionSP = SmartPointer<DBConnection>;
using DBConnectionPoolSP = SmartPointer<DBConnectionPool>;
using PartitionedTableAppenderSP = SmartPointer<PartitionedTableAppender>;

enum class TransportationProtocol {
    TCP, UDP,
};

// Parameters of subscribe

struct SubscribeInfo {
    std::string hostName;
    int port;
    std::string tableName;
    std::string actionName;
    bool operator<(const SubscribeInfo &info) const
    {
        if (hostName != info.hostName) {
            return hostName < info.hostName;
        }
        if (port != info.port) {
            return port < info.port;
        }
        if (tableName != info.tableName) {
            return tableName < info.tableName;
        }
        return actionName < info.actionName;
    }
};

struct SubscribeConfig {
    int64_t offset;
    bool msgAsTable;
    bool allowExists;
    bool resetOffset;
};

enum class ConnectionState {
    Initializing,
    Connected,
    Terminated,
    Reconnecting,
};

class EXPORT_DECL DBConnection {
public:
    explicit DBConnection(bool enableSSL = false, bool asyncTask = false, int keepAliveTime = 7200, bool compress = false, bool python = false, bool isReverseStreaming = false, bool enableSCRAM = false);
    DBConnection(const std::string &host, int port, const std::string &userName="", const std::string &password="")
        :DBConnection(false)
    {
        if (!connect(host, port)) {
            throw RuntimeException("Failed to connect to server: " + host + ":" + std::to_string(port));
        }
        if (!userName.empty()) {
            login(userName, password, HAS_OPENSSL);
        }
    }
    DBConnection(DBConnection&& oth) noexcept;
    DBConnection& operator=(DBConnection&& oth) noexcept;
    virtual ~DBConnection();

    using stateCallbackT = std::function<bool(const ConnectionState state, const std::string &host, const int port)>;
    void onConnectionStateChange(const stateCallbackT &callback) { callback_ = callback; }
    std::shared_ptr<DBConnection> copy()
    {
        if (state_ != ConnectionState::Connected) {
            return nullptr;
        }
        auto ret = std::make_shared<DBConnection>();
        ret->host_ = host_;
        ret->port_ = port_;
        ret->uid_ = uid_;
        ret->pwd_ = pwd_;
        ret->enableSSL_ = enableSSL_;
        ret->enableSCRAM_ = enableSCRAM_;
        ret->nodes_ = nodes_;
        ret->haSitesNum_ = haSitesNum_;
        ret->lastConnNodeIndex_ = -1;
        ret->state_ = ConnectionState::Initializing;
        ret->initialScript_ = initialScript_;
        ret->closed_ = false;
        ret->keepAliveTime_ = keepAliveTime_;

        // deprecated
        ret->ha_ = ha_;
        ret->reconnect_ = reconnect_;

        return ret;
    }
    void setCompress(bool compress) { compress_ = compress; }
    void setAsync(bool async) { asynTask_ = async; }
    bool connect();

	/**
	 * Connect to the specified DolphinDB server. If userId and password are specified, authentication
	 * will be performed along with connecting. If one would send userId and password in encrypted mode,
	 * please use the login function for authentication separately.
	 */
	bool connect(const std::string& hostName, int port, const std::string& userId = "", const std::string& password = "", const std::string& initialScript = "",
		bool highAvailability = false, const std::vector<std::string>& highAvailabilitySites = std::vector<std::string>(), int keepAliveTime=7200, bool reconnect = false);

    bool checkVersion(const std::vector<VersionT> &vers)
    {
        return version_.check(vers);
    }

	/**
	 * Log onto the DolphinDB server using the given userId and password. If the parameter enableEncryption
	 * is true, the client obtains a public key from the server and then encrypts the userId and password
	 * using the public key. If the parameter enableEncryption is false, userId and password are communicated
	 * in plain text.
	 */
	void login(const std::string& userId, const std::string& password, bool enableEncryption);

    /*
     * Internal function for stream subscription.
     * Server function prototype: getSubscriptionTopic(tableName, actionName)
     * Server return value: (topic, columns)
     */
    std::pair<std::string, std::vector<std::string>> getSubscriptionTopic(const SubscribeInfo &info);

    /*
     * Internal function for stream subscription.
     * Server function prototype: publishTable(host, port, tableName, [actionName], [offset], [filter], [allowExists], [resetOffset], [udpMulticast])
     * Server return value: columnNames for TCP or (columnNames, port) for UDP
     */
    int publishTable(const SubscribeInfo &info, const SubscribeConfig &config, TransportationProtocol protocol);

    /*
     * Internal function for stream unsubscription.
     * Server function prototype: stopPublishTable(host, port, tableName, actionName, false, [udpMulticast])
     * the 5th arg is an internal parameter
     */
    void stopPublishTable(const SubscribeInfo &info, TransportationProtocol protocol);

    /*
     * Internal function for stream multicast.
     * Server function prototype: triggerMulticastOnceInternal(tableName, topic, offset)
     */
    void restartMulticast(const SubscribeInfo &info, const std::string &topic, const SubscribeConfig &config)
    {
        auto ret = call("triggerMulticastOnceInternal", info.tableName, topic, config.offset);
    }

	/**
	 * Run the script on the DolphinDB server and return the result to the client.If nothing returns,
	 * the function returns a void object. If error is raised on the server, the function throws an
	 * exception.
	 */
	ConstantSP run(const std::string& script, int priority=4, int parallelism=64, int fetchSize=0, bool clearMemory = false);

	/**
	 * Run the given function on the DolphinDB server using the local objects as the arguments
	 * for the function and return the result to the client. If nothing returns, the function
	 * returns a void object. If error is raised on the server, the function throws an exception.
	 */
	ConstantSP run(const std::string& funcName, std::vector<ConstantSP>& args, int priority=4, int parallelism=64, int fetchSize=0, bool clearMemory = false);

    // Internal helper interface.
    template <typename... ArgsT>
    ConstantSP call(const std::string &func, ArgsT &&... args)
    {
        std::vector<ConstantSP> argVec;
        Util::getConstantSP(argVec, std::forward<ArgsT>(args)...);
        return run(func, argVec);
    }

	/**
	 * upload a local object to the DolphinDB server and assign the given name in the session.
	 */
	ConstantSP upload(const std::string& name, const ConstantSP& obj);

	/**
	 * upload multiple local objects to the DolphinDB server and assign each object the given
	 * name in the session.
	 */
	ConstantSP upload(std::vector<std::string>& names, std::vector<ConstantSP>& objs);

	/**
	 * Close the current session and release all resources.
	 */
	void close();

	/**
	 * It is required to call initialize function below before one uses the DolphinDB API.
	 */
	static void initialize(){}

	void setInitScript(const std::string& script);

	const std::string& getInitScript() const;

	DataInputStreamSP getDataInputStream();
private:
    DBConnection(DBConnection& oth); // = delete
    DBConnection& operator=(DBConnection& oth); // = delete


	enum ExceptionType {
		ET_UNKNOWN = 1,
		ET_NEWLEADER = 2,
		ET_NODENOTAVAIL = 3,
	};
    void switchDataNode(const std::string &host = "", int port = -1);
	bool connectNode(std::string hostName, int port, int keepAliveTime = -1);
    bool connected();
	//0 - ignored exception, eg : other data node not avail;
	//1 - throw exception;
	//2 - new leader, host&port is valid
	//3 - this data node not avail
	ExceptionType parseException(const std::string &msg, std::string &host, int &port);


	struct Node{
		std::string hostName_;
		int port_;
		double load_;//DBL_MAX : unknow

		bool isEqual(const Node &node) {
			return hostName_ == node.hostName_ && port_ == node.port_;
		}
		Node() = default;
		Node(std::string hostName, int port, double load = DBL_MAX): hostName_(std::move(hostName)), port_(port), load_(load){}
		explicit Node(const std::string &ipport, double loadValue = DBL_MAX);
	};
	static bool parseIpPort(const std::string &ipport, std::string &ip, int &port);
	long nextSeqNo();

    std::unique_ptr<DBConnectionImpl> conn_;
    std::string uid_;
    std::string pwd_;
    std::string initialScript_;
    bool ha_;
    std::string host_;
    int port_;
    size_t haSitesNum_;
    int keepAliveTime_;
    bool enableSSL_;
    bool enableSCRAM_;
    bool asynTask_;
    bool compress_;
    std::vector<Node> nodes_;
    int lastConnNodeIndex_;
    bool enablePickle_, python_;
    bool reconnect_;
    std::atomic<bool> closed_{true};
    static const int maxRerunCnt_ = 30;
    long   runSeqNo_;
    Mutex	mutex_;
    VersionT version_;
    static const std::string udpIP_;
    static constexpr int udpPort_{1234};
	stateCallbackT callback_{[](const ConnectionState, const std::string&, const int){ return true; }};
	ConnectionState state_{ConnectionState::Initializing};
};

class EXPORT_DECL BlockReader : public Constant{
public:
    explicit BlockReader(const DataInputStreamSP& in );
	~BlockReader() override = default;
    ConstantSP read();
    void skipAll();
    bool hasNext() const {return currentIndex_ < total_;}
    DATA_TYPE getType() const override {return DT_ANY;}
    DATA_TYPE getRawType() const override {return DT_ANY;}
    DATA_CATEGORY getCategory() const override {return MIXED;}
    ConstantSP getInstance() const override {return nullptr;}
    ConstantSP getValue() const override {return nullptr;}
private:
    DataInputStreamSP in_;
    int total_;
    int currentIndex_;
};

#if __cplusplus < 201402L
struct RpcParam {
    RpcParam(int priority_ = 4, uint32_t parallelism_ = 64, uint32_t fetchSize_ = 0, bool clearMemory_ = false)
        :priority(priority_), parallelism(parallelism_), fetchSize(fetchSize_), clearMemory(clearMemory_) {}
    int priority {4};
    uint32_t parallelism {64};
    uint32_t fetchSize {0};
    bool clearMemory {false};
};
#else
struct RpcParam {
    int priority {4};
    uint32_t parallelism {64};
    uint32_t fetchSize {0};
    bool clearMemory {false};
};
#endif

class EXPORT_DECL DBConnectionPool{
public:
    DBConnectionPool(const std::string& hostName, int port, int threadNum = 10, const std::string& userId = "", const std::string& password = "",
		bool loadBalance = false, bool highAvailability = false, bool compress = false, bool reConnect = false, bool python = false);
	virtual ~DBConnectionPool() = default;

	int run(std::string script, std::shared_ptr<std::condition_variable> finished = nullptr, const RpcParam &param = RpcParam());
	int run(std::string functionName, const std::vector<ConstantSP>& args, std::shared_ptr<std::condition_variable> finished = nullptr, const RpcParam &param = RpcParam());

	bool isFinished(int identity);

    ConstantSP getData(int identity);

    void shutDown();

    bool isShutDown();

	int getConnectionCount();

	[[deprecated]] void run(const std::string& script, int identity, int priority=4, int parallelism=64, int fetchSize=0, bool clearMemory = false);
	[[deprecated]] void run(const std::string& functionName, const std::vector<ConstantSP>& args, int identity, int priority=4, int parallelism=64, int fetchSize=0, bool clearMemory = false);

private:
    std::atomic_int id_{0};
    std::shared_ptr<DBConnectionPoolImpl> pool_;
    friend class PartitionedTableAppender;
};

class EXPORT_DECL PartitionedTableAppender {
public:
	PartitionedTableAppender(const std::string& dbUrl, const std::string& tableName, const std::string& partitionColName, DBConnectionPool& pool);

	PartitionedTableAppender(const std::string& dbUrl, const std::string& tableName, const std::string& partitionColName, const std::string& appendFunction, DBConnectionPool& pool);
	virtual ~PartitionedTableAppender() = default;
	int append(const TableSP& table);

private:
 	void init(const std::string& dbUrl, const std::string& tableName, const std::string& partitionColName, const std::string& appendFunction);

	void checkColumnType(int col, DATA_CATEGORY category, DATA_TYPE type);


	std::shared_ptr<DBConnectionPoolImpl> pool_;
	std::string appendScript_;
	int threadCount_;
    DictionarySP tableInfo_;
	int partitionColumnIdx_;
	int cols_;
    DomainSP domain_;
    std::vector<DATA_CATEGORY> columnCategories_;
 	std::vector<DATA_TYPE> columnTypes_;
	int identity_ = -1;
    std::vector<std::vector<int>> chunkIndices_;
};


class EXPORT_DECL AutoFitTableAppender {
public:
	AutoFitTableAppender(const std::string& dbUrl, const std::string& tableName, DBConnection& conn);
	virtual ~AutoFitTableAppender() = default;
	int append(const TableSP& table);

private:
	void checkColumnType(int col, DATA_CATEGORY category, DATA_TYPE type);


    DBConnection& conn_;
	std::string appendScript_;
	int cols_;
    std::vector<DATA_CATEGORY> columnCategories_;
 	std::vector<DATA_TYPE> columnTypes_;
	std::vector<std::string> columnNames_;
};

class EXPORT_DECL AutoFitTableUpsert {
public:
	AutoFitTableUpsert(const std::string& dbUrl, const std::string& tableName, DBConnection& conn,bool ignoreNull=false,
                                        std::vector<std::string> *pkeyColNames=nullptr,std::vector<std::string> *psortColumns=nullptr);
	virtual ~AutoFitTableUpsert() = default;
	int upsert(const TableSP& table);

private:
	void checkColumnType(int col, DATA_CATEGORY category, DATA_TYPE type);


    DBConnection& conn_;
	std::string upsertScript_;
	int cols_;
    std::vector<DATA_CATEGORY> columnCategories_;
 	std::vector<DATA_TYPE> columnTypes_;
	std::vector<std::string> columnNames_;
};

} // namespace dolphindb

#ifdef _MSC_VER
#pragma warning( pop )
#endif
