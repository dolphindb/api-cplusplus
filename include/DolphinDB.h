/*
 * DolphinDB.h
 *
 *  Created on: Sep 22, 2018
 *      Author: dzhou
 */

#ifndef DOLPHINDB_H_
#define DOLPHINDB_H_

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <vector>
#include <deque>
#include <algorithm>
#include <memory>
#include <chrono>
#include <cstring>
#include <functional>

#include "Exports.h"
#include "Types.h"
#include "SmartPointer.h"
#include "Exceptions.h"
#include "SysIO.h"
#include "WideInteger.h"
#include "Constant.h"
#include "Dictionary.h"
#include "Table.h"
// MSVC warning C4150: destructor of Domain is required during instantiation of SmartPointer<Domain>
#include "Domain.h"
#include "Util.h"

namespace dolphindb {

class DBConnectionImpl;
class BlockReader;
class DBConnectionPoolImpl;
class PartitionedTableAppender;
class DBConnection;
class DBConnectionPool;

typedef SmartPointer<BlockReader> BlockReaderSP;
typedef SmartPointer<Domain> DomainSP;
typedef SmartPointer<DBConnection> DBConnectionSP;
typedef SmartPointer<DBConnectionPool> DBConnectionPoolSP;
typedef SmartPointer<PartitionedTableAppender> PartitionedTableAppenderSP;

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
        } else if (port != info.port) {
            return port < info.port;
        } else if (tableName != info.tableName) {
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

class EXPORT_DECL DBConnection {
public:
    DBConnection(std::string host, int port)
        :DBConnection()
    {
        if (!connect(host, port)) {
            throw RuntimeException("Failed to connect to server: " + host + ":" + std::to_string(port));
        }
    }
	DBConnection(bool enableSSL = false, bool asyncTask = false, int keepAliveTime = 7200, bool compress = false, bool python = false, bool isReverseStreaming = false);
	virtual ~DBConnection();
	DBConnection(DBConnection&& oth);
	DBConnection& operator=(DBConnection&& oth);

	/**
	 * Connect to the specified DolphinDB server. If userId and password are specified, authentication
	 * will be performed along with connecting. If one would send userId and password in encrypted mode,
	 * please use the login function for authentication separately.
	 */
	bool connect(const std::string& hostName, int port, const std::string& userId = "", const std::string& password = "", const std::string& initialScript = "",
		bool highAvailability = false, const std::vector<std::string>& highAvailabilitySites = std::vector<std::string>(), int keepAliveTime=7200, bool reconnect = false);

	std::string version();

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
        auto ret = run("triggerMulticastOnceInternal", info.tableName, topic, config.offset);
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

    template <typename... ArgsT>
    ConstantSP run(const std::string &func, ArgsT &&... args)
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

private:
	enum ExceptionType {
		ET_IGNORE = 0,
		ET_UNKNOW = 1,
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

private:
	struct Node{
		std::string hostName_;
		int port_;
		double load_;//DBL_MAX : unknow

		bool isEqual(const Node &node) {
			return hostName_.compare(node.hostName_) == 0 && port_ == node.port_;
		}
		Node(){}
		Node(const std::string &hostName, int port, double load = DBL_MAX): hostName_(hostName), port_(port), load_(load){}
		Node(const std::string &ipport, double loadValue = DBL_MAX);
	};
	static void parseIpPort(const std::string &ipport, std::string &ip, int &port);
	long nextSeqNo();
	void initClientID();


    std::unique_ptr<DBConnectionImpl> conn_;
    std::string uid_;
    std::string pwd_;
    std::string initialScript_;
    bool ha_;
	bool enableSSL_;
    bool asynTask_;
    bool compress_;
	std::vector<Node> nodes_;
	int lastConnNodeIndex_;
	bool enablePickle_, python_;
	bool reconnect_, closed_;
	static const int maxRerunCnt_ = 30;
	long   runSeqNo_;
	Mutex	mutex_;
    static const std::string udpIP_;
    static constexpr int udpPort_{1234};
};

class EXPORT_DECL BlockReader : public Constant{
public:
    BlockReader(const DataInputStreamSP& in );
	virtual ~BlockReader();
    ConstantSP read();
    void skipAll();
    bool hasNext() const {return currentIndex_ < total_;}
    virtual DATA_TYPE getType() const {return DT_ANY;}
    virtual DATA_TYPE getRawType() const {return DT_ANY;}
    virtual DATA_CATEGORY getCategory() const {return MIXED;}
    virtual ConstantSP getInstance() const {return nullptr;}
    virtual ConstantSP getValue() const {return nullptr;}
private:
    DataInputStreamSP in_;
    int total_;
    int currentIndex_;
};


class EXPORT_DECL DBConnectionPool{
public:
    DBConnectionPool(const std::string& hostName, int port, int threadNum = 10, const std::string& userId = "", const std::string& password = "",
		bool loadBalance = false, bool highAvailability = false, bool compress = false, bool reConnect = false, bool python = false);
	virtual ~DBConnectionPool();
	void run(const std::string& script, int identity, int priority=4, int parallelism=64, int fetchSize=0, bool clearMemory = false);
	
	void run(const std::string& functionName, const std::vector<ConstantSP>& args, int identity, int priority=4, int parallelism=64, int fetchSize=0, bool clearMemory = false);
    
	bool isFinished(int identity);

    ConstantSP getData(int identity);
	
    void shutDown();

    bool isShutDown();

	int getConnectionCount();
private:
	std::shared_ptr<DBConnectionPoolImpl> pool_;
	friend class PartitionedTableAppender; 

};

class EXPORT_DECL PartitionedTableAppender {
public:
	PartitionedTableAppender(std::string dbUrl, std::string tableName, std::string partitionColName, DBConnectionPool& pool);

	PartitionedTableAppender(std::string dbUrl, std::string tableName, std::string partitionColName, std::string appendFunction, DBConnectionPool& pool);
	virtual ~PartitionedTableAppender();
	int append(TableSP table);

private:
 	void init(std::string dbUrl, std::string tableName, std::string partitionColName, std::string appendFunction);

	void checkColumnType(int col, DATA_CATEGORY category, DATA_TYPE type);

private:
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
	AutoFitTableAppender(std::string dbUrl, std::string tableName, DBConnection& conn);
	virtual ~AutoFitTableAppender() = default;
	int append(TableSP table);

private:
	void checkColumnType(int col, DATA_CATEGORY category, DATA_TYPE type);

private:
    DBConnection& conn_;
	std::string appendScript_;
	int cols_;
    std::vector<DATA_CATEGORY> columnCategories_;
 	std::vector<DATA_TYPE> columnTypes_;
	std::vector<std::string> columnNames_;
};

class EXPORT_DECL AutoFitTableUpsert {
public:
	AutoFitTableUpsert(std::string dbUrl, std::string tableName, DBConnection& conn,bool ignoreNull=false,
                                        std::vector<std::string> *pkeyColNames=nullptr,std::vector<std::string> *psortColumns=nullptr);
	virtual ~AutoFitTableUpsert() = default;
	int upsert(TableSP table);

private:
	void checkColumnType(int col, DATA_CATEGORY category, DATA_TYPE type);

private:
    DBConnection& conn_;
	std::string upsertScript_;
	int cols_;
    std::vector<DATA_CATEGORY> columnCategories_;
 	std::vector<DATA_TYPE> columnTypes_;
	std::vector<std::string> columnNames_;
};


class RecordTime {
public:
	RecordTime(const std::string &name);
	~RecordTime();
	static std::string printAllTime();
private:
	const std::string name_;
	long recordOrder_;
	long long startTime_;
	struct Node {
		std::string name;
		long minOrder;
		std::vector<long long> costTime;//ns
	};
	static long lastRecordOrder_;
	static Mutex mapMutex_;
	static std::unordered_map<std::string, RecordTime::Node*> codeMap_;
};

}




#endif /* DOLPHINDB_H_ */
