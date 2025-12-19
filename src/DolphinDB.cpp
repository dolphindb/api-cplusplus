/*
 * DolphinDB.cpp
 *
 *  Created on: Sep 22, 2018
 *      Author: dzhou
 */

#include "DolphinDB.h"
#include "Concurrent.h"
#include "Constant.h"
#include "ConstantMarshall.h"
#include "DBConnectionPoolImpl.h"
#include "DolphinDBImp.h"
#include "Domain.h"
#include "ErrorCodeInfo.h"
#include "Exceptions.h"
#include "Logger.h"
#include "ScalarImp.h"
#include "SymbolBase.h"
#include "SysIO.h"
#include "Table.h"
#include "Types.h"
#include "Util.h"
#include "Vector.h"

#include <algorithm>
#include <cfloat>
#include <chrono> // NOLINT(misc-include-cleaner): chrono literals
#include <condition_variable>
#include <cstdint>
#include <ctime>
#include <exception>
#include <fstream>
#include <iostream>
#include <memory>
#include <ostream>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using std::ifstream;
using std::string;
using std::vector;
#ifdef INDEX64
namespace std {
int min(int a, INDEX b) {
    return a < b ? a : (int)b;
}

int min(INDEX a, int b) {
    return a < b ? (int)a : b;
}
}    // namespace std
#endif

//#define APIMinVersionRequirement 100

namespace dolphindb {

std::string Constant::EMPTY;
std::string Constant::NULL_STR("nullptr");
ConstantSP Constant::void_(new Void());
ConstantSP Constant::null_(new Void(true));
ConstantSP Constant::true_(new Bool(true));
ConstantSP Constant::false_(new Bool(false));
ConstantSP Constant::one_(new Int(1));

int Constant::serialize(char* buf, int bufSize, INDEX indexStart, int offset, int cellCountToSerialize, int& numElement, int& partial) const {
    std::ignore = buf;
    std::ignore = bufSize;
    std::ignore = indexStart;
    std::ignore = offset;
    std::ignore = cellCountToSerialize;
    std::ignore = numElement;
    std::ignore = partial;
    throw RuntimeException(Util::getDataFormString(getForm())+"_"+Util::getDataTypeString(getType())+" serialize cell method not supported");
}

int Constant::serialize(char* buf, int bufSize, INDEX indexStart, int offset, int& numElement, int& partial) const {
    std::ignore = buf;
    std::ignore = bufSize;
    std::ignore = indexStart;
    std::ignore = offset;
    std::ignore = numElement;
    std::ignore = partial;
    throw RuntimeException(Util::getDataFormString(getForm())+"_"+Util::getDataTypeString(getType())+" serialize method not supported");
}

IO_ERR Constant::deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement) {
    std::ignore = in;
    std::ignore = indexStart;
    std::ignore = targetNumElement;
    std::ignore = numElement;
    throw RuntimeException(Util::getDataFormString(getForm())+"_"+Util::getDataTypeString(getType())+" deserialize method not supported");
}

ConstantSP Constant::getRowLabel() const {
    return void_;
}

ConstantSP Constant::getColumnLabel() const {
    return void_;
}

const std::string DBConnection::udpIP_{"224.1.1.1"};

DBConnection::DBConnection(bool enableSSL, bool asyncTask, int keepAliveTime, bool compress, bool python, bool isReverseStreaming, bool enableSCRAM) :
	conn_(new DBConnectionImpl(enableSSL, asyncTask, keepAliveTime, compress, python, isReverseStreaming, enableSCRAM)), ha_(false),
		enableSSL_(enableSSL), enableSCRAM_(enableSCRAM), asynTask_(asyncTask), compress_(compress), nodes_({}),
		lastConnNodeIndex_(0), python_(python), reconnect_(false), closed_(true), runSeqNo_(0){
        if (asyncTask && enableSCRAM) {
            throw IOException("SCRAM login is not supported in async mode.");
        }
}

DBConnection::DBConnection(DBConnection&& oth) noexcept :
		conn_(std::move(oth.conn_)), uid_(std::move(oth.uid_)), pwd_(std::move(oth.pwd_)),
		initialScript_(std::move(oth.initialScript_)), ha_(oth.ha_), keepAliveTime_(oth.keepAliveTime_), enableSSL_(oth.enableSSL_), enableSCRAM_(oth.enableSCRAM_),
		asynTask_(oth.asynTask_),compress_(oth.compress_),nodes_(std::move(oth.nodes_)),lastConnNodeIndex_(0),
		reconnect_(oth.reconnect_), runSeqNo_(oth.runSeqNo_)
{
    closed_ = oth.closed_.load();
}

DBConnection& DBConnection::operator=(DBConnection&& oth) noexcept
{
    if (this == &oth) { return *this; }
    conn_ = std::move(oth.conn_);
    uid_ = std::move(oth.uid_);
    pwd_ = std::move(oth.pwd_);
    initialScript_ = std::move(oth.initialScript_);
    ha_ = oth.ha_;
    nodes_ = oth.nodes_;
    oth.nodes_.clear();
    enableSSL_ = oth.enableSSL_;
    enableSCRAM_ = oth.enableSCRAM_;
    asynTask_ = oth.asynTask_;
	compress_ = oth.compress_;
	lastConnNodeIndex_ = oth.lastConnNodeIndex_;
	reconnect_ = oth.reconnect_;
	closed_ = oth.closed_.load();
    runSeqNo_ = oth.runSeqNo_;
    version_ = oth.version_;
    return *this;
}

DBConnection::~DBConnection() {
    close();
}

bool DBConnection::connect() {
    if (nodes_.empty()) {
        closed_ = false;
        if (!connectNode(host_, port_, keepAliveTime_)) {
            return false;
        }
    } else {
        if (haSitesNum_ > 1) {
            std::random_device rd;
            std::mt19937 g(rd());
            std::shuffle(nodes_.begin(), nodes_.begin() + haSitesNum_, g);
        }
        switchDataNode();
    }
    version_ = conn_->version();
    if(!nodes_.empty()){
        if (checkVersion({{1,3020,5},{2,9,0}})) {
            Uuid uuid(true);
            std::string clientId_ = uuid.getValue()->getString();
            conn_->setClientId(clientId_);
        }
    }
    if (!uid_.empty()) {
        login(uid_, pwd_, enableSSL_);
    }
    return true;
}

bool DBConnection::connect(const std::string & hostName, int port, const std::string & userId, const std::string & password, const std::string & startup,
                           bool ha, const vector<string>& highAvailabilitySites, int keepAliveTime, bool reconnect) {
    host_ = hostName;
    port_ = port;
    state_ = ConnectionState::Initializing;
    ha_ = ha;
	uid_ = userId;
	pwd_ = password;
    initialScript_ = startup;
	reconnect_ = reconnect;
	closed_ = false;
    if (ha_) {
		for (const auto &one : highAvailabilitySites)
			nodes_.emplace_back(one);
		{
			bool foundfirst = false;
			Node firstnode(hostName, port);
			for (auto &one : nodes_)
				if (one.isEqual(firstnode)) {
					foundfirst = true;
					break;
				}
			if(!foundfirst)
				nodes_.push_back(firstnode);
		}
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(nodes_.begin(), nodes_.end(), g);
        haSitesNum_ = nodes_.size();
		Node connectedNode;
		TableSP table;
		while (closed_ == false) {
			while(conn_->isConnected()==false && closed_ == false) {
				for (auto &one : nodes_) {
					if (connectNode(one.hostName_, one.port_, keepAliveTime)) {
						connectedNode = one;
						break;
					}
#if __cplusplus < 201402L
                    using namespace std::chrono;
                    std::this_thread::sleep_for(milliseconds(100));
#else
                    using namespace std::chrono_literals;
                    std::this_thread::sleep_for(100ms); // NOLINT(misc-include-cleaner): <chrono>
#endif
				}
			}
			try {
				table = (TableSP)conn_->run("rpc(getControllerAlias(), getClusterPerf)");
				break;
			}
			catch (std::exception& e) {
				std::cerr << "ERROR getting other data nodes, exception: " << e.what() << std::endl;
				string host;
				int portNumber = 0;
				ExceptionType type = parseException(e.what(), host, portNumber);
				if (type != ET_UNKNOWN || !connected()) {
					switchDataNode(host, portNumber);
				}
			}
		}
        if(table->getForm() != DF_TABLE){
            throw IOException("Run getClusterPerf() failed.");
        }
		auto colHost = (VectorSP)table->getColumn("host");
		auto colPort = (VectorSP)table->getColumn("port");
		auto colMode = (VectorSP)table->getColumn("mode");
		auto colmaxConnections = (VectorSP)table->getColumn("maxConnections");
		auto colconnectionNum = (VectorSP)table->getColumn("connectionNum");
		auto colworkerNum = (VectorSP)table->getColumn("workerNum");
		auto colexecutorNum = (VectorSP)table->getColumn("executorNum");
		double load;
		for (int i = 0; i < colMode->rows(); i++) {
			if (colMode->getInt(i) == 0) {
				string nodeHost = colHost->getString(i);
				int nodePort = colPort->getInt(i);
				Node *pexistNode = nullptr;
				if (!highAvailabilitySites.empty()) {
					for (auto &node : nodes_) {
						if (node.hostName_ == nodeHost &&
							node.port_ == nodePort) {
							pexistNode = &node;
							break;
						}
					}
					//node is out of highAvailabilitySites
					if (pexistNode == nullptr) {
						DLogger::Info("Site", nodeHost, ":", nodePort,"is not in cluster.");
						continue;
					}
				}
				if (colconnectionNum->getInt(i) < colmaxConnections->getInt(i)) {
					load = (colconnectionNum->getInt(i) + colworkerNum->getInt(i) + colexecutorNum->getInt(i)) / 3.0;
					//DLogger::Info("Site", nodeHost, ":", nodePort,"load",load);
				}
				else {
					load = DBL_MAX;
				}
				if (pexistNode != nullptr) {
					pexistNode->load_ = load;
				}
				else {
					nodes_.emplace_back(colHost->get(i)->getString(), colPort->get(i)->getInt(), load);
				}
			}
		}
		Node *pMinNode=nullptr;
		for (auto &one : nodes_) {
			if (pMinNode == nullptr ||
				(one.load_ >= 0 && pMinNode->load_ > one.load_)) {
				pMinNode = &one;
			}
		}
		//DLogger::Info("Connect to min load", pMinNode->load, "site", pMinNode->hostName, ":", pMinNode->port);

		if (!pMinNode->isEqual(connectedNode)) {
			conn_->close();
			state_ = ConnectionState::Terminated;
			callback_(state_, conn_->getHost(), conn_->getPort());
			switchDataNode(pMinNode->hostName_, pMinNode->port_);
			return true;
		}
	} else {
		if (reconnect_) {
			nodes_.emplace_back(hostName, port);
			haSitesNum_ = nodes_.size();
			switchDataNode();
		} else {
			if (!connectNode(hostName, port, keepAliveTime))
				return false;
		}
    }

    version_ = conn_->version();
    if(!nodes_.empty()){
        if (checkVersion({{1,3020,5},{2,9,0}})) {
            Uuid uuid(true);
            std::string clientId_ = uuid.getValue()->getString();
            conn_->setClientId(clientId_);
        }
    }
	if (!initialScript_.empty()) {
		run(initialScript_);
	}
	return true;
}

bool DBConnection::reconnect() {
    if (!nodes_.empty()) {
        throw RuntimeException("Manual reconnect is not allowed when auto reconnect is set.");
    }
    state_ = ConnectionState::Initializing;
	closed_ = false;
	if (!connectNode(host_, port_, keepAliveTime_))
		return false;

	if (!initialScript_.empty()) {
		run(initialScript_);
	}
	return true;
}

bool DBConnection::connected() {
    try {
        conn_->version();
    } catch (std::exception&) {
        return false;
    }
    return true;
}

bool DBConnection::connectNode(string hostName, int port, int keepAliveTime) {
    while (closed_ == false) {
        try {
            bool online = conn_->connect(hostName, port, uid_, pwd_, enableSSL_, asynTask_, keepAliveTime, compress_,python_);
            if (!online) {
                return false;
            }
            bool inited{false};
            // check whether server support init stage check.
            try {
                std::vector<ConstantSP> args;
                inited = (conn_->run("isNodeInitialized", args)->getBool() != 0);
                if (inited) {
                    DLogger::Debug("Connection successfully established, and the node has been initialized.");
                }
            } catch (std::exception &) {
                DLogger::Warn("Server does not support the initialization check. Please upgrade to a newer version.");
                inited = true;
            }
            if (inited && state_ != ConnectionState::Connected) {
                state_ = ConnectionState::Connected;
                callback_(state_, hostName, port);
            }
            return inited;
        } catch (IOException& e) {
            ExceptionType type = parseException(e.what(), hostName, port);
            DLogger::Debug(e.what());
            if (!connected() || type != ET_NEWLEADER) {
                return false;
            }
            Util::sleep(100);
        }
    }
    return false;
}

DBConnection::ExceptionType DBConnection::parseException(const string &msg, string &host, int &port) {
    size_t index = msg.find("<NotLeader>");
    if (index != string::npos) {
        index = msg.find('>');
        auto end = msg.find('\'', index);
        string ipport = msg.substr(index + 1, end - index);
        if (parseIpPort(ipport,host,port)) {
            DLogger::Info("Got NotLeaderException, switch to leader node [",host,":",port,"] to run script");
            return ET_NEWLEADER;
        }
    }
    static string ignoreMsgs[] = { "<ChunkInTransaction>","<DataNodeNotAvail>","<DataNodeNotReady>","<ControllerNotReady>","<ControllerNotAvail>","DFS is not enabled" };
    static int ignoreMsgSize = sizeof(ignoreMsgs) / sizeof(string);
    for (int i = 0; i < ignoreMsgSize; i++) {
        index = msg.find(ignoreMsgs[i]);
        if (index != string::npos) {
            if (i == 0) {//case ChunkInTransaction should sleep 1 minute for transaction timeout
                Util::sleep(10000);
            }
            host.clear();
            return ET_NODENOTAVAIL;
        }
    }
    return ET_UNKNOWN;
}

void DBConnection::switchDataNode(const string &host, int port) {
    bool isConnected = false;
    while (isConnected == false && closed_ == false){
        if (!host.empty()) {
            if (connectNode(host, port)) {
                isConnected = true;
                break;
            }
        }
        if (nodes_.empty()) {
            throw RuntimeException("Failed to connect to " + host + ":" + std::to_string(port));
        }
        for (int i = static_cast<int>(nodes_.size() - 1); i >= 0; i--) {
            lastConnNodeIndex_ = (lastConnNodeIndex_ + 1) % nodes_.size();
            if (connectNode(nodes_[lastConnNodeIndex_].hostName_, nodes_[lastConnNodeIndex_].port_)) {
                isConnected = true;
                break;
            }
        }
        if(isConnected) break;
#if __cplusplus < 201402L
        using namespace std::chrono;
        std::this_thread::sleep_for(seconds(1));
#else
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(1s); // NOLINT(misc-include-cleaner): <chrono>
#endif
    }
    if (isConnected && initialScript_.empty() == false)
        run(initialScript_);
}

void DBConnection::login(const std::string & userId, const std::string & password, bool enableEncryption) {
    conn_->login(userId, password, enableEncryption);
    uid_ = userId;
    pwd_ = password;
}

auto DBConnection::getSubscriptionTopic(const SubscribeInfo &info) -> std::pair<std::string, std::vector<std::string>>
{
    auto result = call("getSubscriptionTopic", info.tableName, info.actionName);
    auto topic = result->get(0)->getString();
    result = result->get(1);
    auto len = result->size();
    std::vector<std::string> columns;
    columns.reserve(len);
    for (INDEX i = 0; i < len; ++i)
    {
        columns.emplace_back(result->getString(i));
    }
    return std::make_pair(topic, std::move(columns));
}

int DBConnection::publishTable(const SubscribeInfo &info, const SubscribeConfig &config, TransportationProtocol protocol)
{
    bool isUDP = bool(protocol == TransportationProtocol::UDP);
    std::string hostName = isUDP ? udpIP_ : info.hostName;
    int port = isUDP ? udpPort_ : info.port;
    auto ret = call("publishTable", hostName, port, info.tableName, info.actionName, config.offset,
                   ConstantSP(Util::createConstant(DT_VOID)), config.allowExists, config.resetOffset, isUDP);
    if (isUDP)
    {
        return ret->get(1)->getInt();
    }
    return -1;
}

void DBConnection::stopPublishTable(const SubscribeInfo &info, TransportationProtocol protocol)
{
    bool isUDP = bool(protocol == TransportationProtocol::UDP);
    std::string hostName = isUDP ? udpIP_ : info.hostName;
    int port = isUDP ? udpPort_ : info.port;
    auto ret = call("stopPublishTable", hostName, port, info.tableName, info.actionName, false, isUDP);
}

ConstantSP DBConnection::run(const std::string & script, int priority, int parallelism, int fetchSize, bool clearMemory) {
    if (parallelism <= 0) {
        throw IllegalArgumentException(DDB_FUNCNAME, "Invalid parallelism.");
    }
    LockGuard<Mutex> LockGuard(&mutex_);
    if (nodes_.empty()) {
        return conn_->run(script, priority, parallelism, fetchSize, clearMemory, 0);
    }
    long seqNo = nextSeqNo();
    while(closed_ == false){
        try {
            return conn_->run(script, priority, parallelism, fetchSize, clearMemory, seqNo);
        } catch (IOException& e) {
            DLogger::Warn(script, "failed: ", e.what(), ", retrying.");
            if(seqNo > 0)
                seqNo = -seqNo;
            string host;
            int port = 0;
            ExceptionType type = parseException(e.what(), host, port);
            if (type == ET_UNKNOWN) {
                if (connected()) {
                    throw;
                }
                if (state_ != ConnectionState::Reconnecting) {
                    callback_(ConnectionState::Reconnecting, conn_->getHost(), conn_->getPort());
                    state_ = ConnectionState::Reconnecting;
                }
            }
            switchDataNode(host, port);
        }
    }
    return nullptr;
}

ConstantSP DBConnection::run(const std::string & funcName, vector<dolphindb::ConstantSP>& args, int priority, int parallelism, int fetchSize, bool clearMemory) {
    if (parallelism <= 0) {
        throw IllegalArgumentException(DDB_FUNCNAME, "Invalid parallelism.");
    }
    LockGuard<Mutex> LockGuard(&mutex_);
    if (nodes_.empty()) {
        auto ret = conn_->run(funcName, args, priority, parallelism, fetchSize, clearMemory, 0);
        return ret;
    }
    long seqNo = nextSeqNo();
    while (closed_ == false) {
        try {
            auto ret = conn_->run(funcName, args, priority, parallelism, fetchSize, clearMemory, seqNo);
            return ret;
        } catch (IOException& e) {
            DLogger::Warn(funcName, "failed: ", e.what(), ", retrying.");
            if(seqNo > 0) {
                seqNo = -seqNo;
            }
            std::string host;
            int port = 0;
            ExceptionType type = parseException(e.what(), host, port);
            if (type == ET_UNKNOWN && connected()) {
                throw;
            }
            if (type != ET_NEWLEADER) {
                state_ = ConnectionState::Reconnecting;
                callback_(state_, conn_->getHost(), conn_->getPort());
            }
            switchDataNode(host, port);
        }
    }
    return nullptr;
}

ConstantSP DBConnection::upload(const std::string & name, const ConstantSP& obj) {
    LockGuard<Mutex> LockGuard(&mutex_);
    if (nodes_.empty()) {
        return conn_->upload(name, obj);
    }
	while (!closed_) {
		try {
			return conn_->upload(name, obj);
		} catch (IOException& e) {
			DLogger::Warn("upload failed: ", e.what(), ", retrying.");
			string host;
			int port = 0;
			ExceptionType type = parseException(e.what(), host, port);
            if (type == ET_UNKNOWN && connected()) {
                if (state_ != ConnectionState::Initializing) {
                    callback_(ConnectionState::Terminated, host, port);
                }
                throw;
            }
			switchDataNode(host, port);
		}
	}
	return Constant::void_;
}

ConstantSP DBConnection::upload(vector<string>& names, vector<ConstantSP>& objs) {
    LockGuard<Mutex> LockGuard(&mutex_);
    if (nodes_.empty() == false) {
		while(closed_ == false){
			try {
				auto res = conn_->upload(names, objs);
			}
			catch (IOException& e) {
				DLogger::Warn("upload failed: ", e.what(), ", retrying.");
				string host;
				int port = 0;
				ExceptionType type = parseException(e.what(), host, port);
                if (type == ET_UNKNOWN && connected()) {
                    if (state_ != ConnectionState::Initializing) {
                        callback_(ConnectionState::Terminated, host, port);
                    }
					throw;
                }
				switchDataNode(host, port);
			}
		}
    } else {
        return conn_->upload(names, objs);
    }
	return Constant::void_;
}

bool DBConnection::parseIpPort(const string &ipport, string &ip, int &port) {
    auto v = Util::split(ipport, ':');
    if (v.size() < 2) {
        return false;
    }
    ip = v[0];
    port = std::stoi(v[1]);
    if (port <= 0 || port > 65535) {
        return false;
    }
    return true;
}

long DBConnection::nextSeqNo(){
    runSeqNo_++;
    if(runSeqNo_ <= 0){
        runSeqNo_ = 1;
    }
    return runSeqNo_;
}

DBConnection::Node::Node(const string &ipport, double loadValue) {
	DBConnection::parseIpPort(ipport, hostName_, port_);
	load_ = loadValue;
}

void DBConnection::close() {
	closed_ = true;
	LockGuard<Mutex> LockGuard(&mutex_);
    if (conn_) conn_->close();
}

const std::string& DBConnection::getInitScript() const {
    return initialScript_;
}

DataInputStreamSP DBConnection::getDataInputStream()
{
    return conn_->getDataInputStream();
}

void DBConnection::setInitScript(const std::string & script) {
    initialScript_ = script;
}

BlockReader::BlockReader(const DataInputStreamSP& in ) : in_(in), total_(0), currentIndex_(0){
    int rowNum, colNum;
    if(in->read(rowNum) != OK)
        throw IOException("Failed to read rows for data block.");
    if(in->read(colNum) != OK)
        throw IOException("Faield to read col for data block.");
    total_ = (long long)rowNum * (long long)colNum;
}

ConstantSP BlockReader::read(){
    if(currentIndex_>=total_)
        return nullptr;
    IO_ERR ret;
    uint16_t flag;
    if ((ret = in_->read(flag)) != OK)
        throw IOException("Failed to read object flag from the socket with IO error type " + std::to_string(ret));

    auto form = static_cast<DATA_FORM>(flag >> 8U);
    ConstantUnmarshallFactory factory(in_);
    ConstantUnmarshall* unmarshall = factory.getConstantUnmarshall(form);
    if(unmarshall==nullptr)
        throw IOException("Failed to parse the incoming object" + std::to_string(form));
    if (!unmarshall->start(flag, true, ret)) {
        unmarshall->reset();
        throw IOException("Failed to parse the incoming object with IO error type " + std::to_string(ret));
    }
    ConstantSP result = unmarshall->getConstant();
    unmarshall->reset();
    currentIndex_ ++;
    return result;
}

void BlockReader::skipAll(){
    while(read().isNull()==false);
}

DBConnectionPool::DBConnectionPool(const std::string & hostName, int port, int threadNum, const std::string & userId, const std::string & password,
                bool loadBalance, bool highAvailability, bool compress, bool reConnect, bool python)
    : pool_(new DBConnectionPoolImpl(hostName, port, threadNum, userId, password, loadBalance, highAvailability, compress,reConnect,python))
{}

int DBConnectionPool::run(std::string script,
    std::shared_ptr<std::condition_variable> finished, const RpcParam &param)
{
    auto id = id_++;
    pool_->run(std::move(script), id, std::move(finished), param);
    return id;
}

int DBConnectionPool::run(std::string functionName, const std::vector<ConstantSP>& args,
    std::shared_ptr<std::condition_variable> finished, const RpcParam &param)
{
    auto id = id_++;
    pool_->run(std::move(functionName), args, id, std::move(finished), param);
    return id;
}

void DBConnectionPool::run(const std::string & script, int identity, int priority, int parallelism, int fetchSize, bool clearMemory){
    if(identity < 0)
        throw RuntimeException("Invalid identity: " + std::to_string(identity) + ". Identity must be a non-negative integer.");
    if (parallelism <= 0) {
        throw IllegalArgumentException(DDB_FUNCNAME, "Invalid parallelism.");
    }
    pool_->run(script, identity, nullptr, RpcParam{priority, (uint32_t)parallelism, (uint32_t)fetchSize, clearMemory});
}

void DBConnectionPool::run(const std::string & functionName, const vector<ConstantSP>& args, int identity, int priority, int parallelism, int fetchSize, bool clearMemory){
    if(identity < 0)
        throw RuntimeException("Invalid identity: " + std::to_string(identity) + ". Identity must be a non-negative integer.");
    if (parallelism <= 0) {
        throw IllegalArgumentException(DDB_FUNCNAME, "Invalid parallelism.");
    }
    pool_->run(functionName, args, identity, nullptr, RpcParam{priority, (uint32_t)parallelism, (uint32_t)fetchSize, clearMemory});
}

bool DBConnectionPool::isFinished(int identity){
    return pool_->isFinished(identity);
}

ConstantSP DBConnectionPool::getData(int identity){
    return pool_->getData(identity);
}

void DBConnectionPool::shutDown(){
    pool_->shutDown();
}

bool DBConnectionPool::isShutDown(){
    return pool_->isShutDown();
}

int DBConnectionPool::getConnectionCount(){
    return pool_->getConnectionCount();
}

PartitionedTableAppender::PartitionedTableAppender(const std::string & dbUrl, const std::string & tableName, const std::string & partitionColName, DBConnectionPool& pool) {
    pool_ = pool.pool_;
    init(dbUrl, tableName, partitionColName, "");
}

PartitionedTableAppender::PartitionedTableAppender(const std::string & dbUrl, const std::string & tableName, const std::string & partitionColName, const std::string & appendFunction, DBConnectionPool& pool) {
    pool_ = pool.pool_;
    init(dbUrl, tableName, partitionColName, appendFunction);
}

void PartitionedTableAppender::init(const std::string & dbUrl, const std::string & tableName, const std::string & partitionColName, const std::string & appendFunction){
    threadCount_ = pool_->getConnectionCount();
    chunkIndices_.resize(threadCount_);
    ConstantSP partitionSchema;
    TableSP colDefs;
    VectorSP typeInts;
    int partitionType;
    DATA_TYPE partitionColType;

    try {
        string task;
        if(dbUrl.empty()){
            task = "schema(" + tableName+ ")";
            appendScript_ = "tableInsert{" + tableName + "}";
        }
        else{
            task = "schema(loadTable(\"" + dbUrl + "\", \"" + tableName + "\"))";
            appendScript_ = "tableInsert{loadTable('" + dbUrl + "', '" + tableName + "')}";
        }
        if(!appendFunction.empty()){
            appendScript_ = appendFunction;
        }

        pool_->run(task,identity_);

        while(!pool_->isFinished(identity_)){
            Util::sleep(10);
        }

        tableInfo_ = (DictionarySP)pool_->getData(identity_);
        identity_ --;
        ConstantSP partColNames = tableInfo_->getMember("partitionColumnName");
        if(partColNames->isNull())
            throw RuntimeException("Can't find specified partition column name.");

        if(partColNames->isScalar()){
            if(partColNames->getString() != partitionColName)
                throw  RuntimeException("Can't find specified partition column name.");
            partitionColumnIdx_ = tableInfo_->getMember("partitionColumnIndex")->getInt();
            partitionSchema = tableInfo_->getMember("partitionSchema");
            partitionType =  tableInfo_->getMember("partitionType")->getInt();
            partitionColType = (DATA_TYPE)tableInfo_->getMember("partitionColumnType")->getInt();
        }
        else{
            int dims = partColNames->size();
            int index = -1;
            for(int i=0; i<dims; ++i){
                if(partColNames->getString(i) == partitionColName){
                    index = i;
                    break;
                }
            }
            if(index < 0)
                throw RuntimeException("Can't find specified partition column name.");
            partitionColumnIdx_ = tableInfo_->getMember("partitionColumnIndex")->getInt(index);
            partitionSchema = tableInfo_->getMember("partitionSchema")->get(index);
            partitionType =  tableInfo_->getMember("partitionType")->getInt(index);
			partitionColType = (DATA_TYPE)tableInfo_->getMember("partitionColumnType")->getInt(index);
        }

        colDefs = (TableSP)tableInfo_->getMember("colDefs");
        cols_ = colDefs->rows();
        typeInts = (VectorSP)colDefs->getColumn("typeInt");
        columnCategories_.resize(cols_);
        columnTypes_.resize(cols_);
        for (int i = 0; i < cols_; ++i) {
            columnTypes_[i] = (DATA_TYPE)typeInts->getInt(i);
            columnCategories_[i] = Util::getCategory(columnTypes_[i]);
        }

        domain_ = Util::createDomain((PARTITION_TYPE)partitionType, partitionColType, partitionSchema);
    } catch (std::exception&) {
        throw;
    }
}

int PartitionedTableAppender::append(const TableSP& table){
    if(cols_ != table->columns())
        throw RuntimeException("The input table doesn't match the schema of the target table.");
    for(int i=0; i<cols_; ++i){
        auto curCol = (VectorSP)table->getColumn(i);
        checkColumnType(i, curCol->getCategory(), curCol->getType());
		if (columnCategories_[i] == TEMPORAL && curCol->getType() != columnTypes_[i]) {
			curCol = (VectorSP)curCol->castTemporal(columnTypes_[i]);
			table->setColumn(i, (ConstantSP)curCol);
		}
    }

    for(int i=0; i<threadCount_; ++i)
        chunkIndices_[i].clear();
    vector<int> keys = domain_->getPartitionKeys(table->getColumn(partitionColumnIdx_));
    vector<int> tasks;
    int rows = static_cast<int>(keys.size());
    for (int i = 0; i < rows; ++i) {
        int key = keys[i];
        if (key >= 0)
            chunkIndices_[key % threadCount_].emplace_back(i);
        else {
            throw RuntimeException("A value-partition column contain null value at row " + std::to_string(i) + ".");
        }
    }
    for(int i=0; i<threadCount_; ++i){
        if(chunkIndices_[i].empty())
            continue;
        auto subTable = (TableSP)table->getSubTable(chunkIndices_[i]);
        tasks.push_back(identity_);
        vector<ConstantSP> args = {(ConstantSP)subTable};
        pool_->run(appendScript_, args, identity_--);
    }
    int affected = 0;
    for(auto& task : tasks){
        while(!pool_->isFinished(task)){
            Util::sleep(100);
        }
        ConstantSP res = pool_->getData(task);
        if(res->isNull()){
            affected = 0;
        }
        else{
            affected += res->getInt();
        }
    }
    return affected;
}

void PartitionedTableAppender::checkColumnType(int col, DATA_CATEGORY category, DATA_TYPE type) {
    if(columnTypes_[col] != type){
        DATA_CATEGORY expectCategory = columnCategories_[col];
        //Add conversion
        //DATA_TYPE expectType = columnTypes_[col];
        if (category != expectCategory) {
            throw  RuntimeException("column " + std::to_string(col) + ", expect category " + Util::getCategoryString(expectCategory) + ", got category " + Util::getCategoryString(category));
        }// else if (category == TEMPORAL && type != expectType) {
        //    throw  RuntimeException("column " + std::to_string(col) + ", temporal column must have exactly the same type, expect " + Util::getDataTypeString(expectType) + ", got " + Util::getDataTypeString(type));
        //}
    }
}

AutoFitTableAppender::AutoFitTableAppender(const std::string & dbUrl, const std::string & tableName, DBConnection& conn) : conn_(conn){
    ConstantSP schema;
    TableSP colDefs;
    VectorSP typeInts;
    DictionarySP tableInfo;
    VectorSP colNames;
    string task;
    if(dbUrl.empty()){
        task = "schema(" + tableName+ ")";
        appendScript_ = "tableInsert{" + tableName + "}";
    } else{
        task = "schema(loadTable(\"" + dbUrl + "\", \"" + tableName + "\"))";
        appendScript_ = "tableInsert{loadTable('" + dbUrl + "', '" + tableName + "')}";
    }

    tableInfo =  (DictionarySP)conn_.run(task);
    colDefs = (TableSP)tableInfo->getMember("colDefs");
    cols_ = colDefs->rows();
    typeInts = (VectorSP)colDefs->getColumn("typeInt");
    colNames = (VectorSP)colDefs->getColumn("name");
    columnCategories_.resize(cols_);
    columnTypes_.resize(cols_);
    columnNames_.resize(cols_);
    for (int i = 0; i < cols_; ++i) {
        columnTypes_[i] = (DATA_TYPE)typeInts->getInt(i);
        columnCategories_[i] = Util::getCategory(columnTypes_[i]);
        columnNames_[i] = colNames->getString(i);
    }
}

int AutoFitTableAppender::append(const TableSP& table){
    if(cols_ != table->columns())
        throw RuntimeException("The input table columns doesn't match the columns of the target table.");

    vector<ConstantSP> columns;
    for(int i = 0; i < cols_; i++){
        auto curCol = (VectorSP)table->getColumn(i);
        checkColumnType(i, curCol->getCategory(), curCol->getType());
        if(columnCategories_[i] == TEMPORAL && curCol->getType() != columnTypes_[i]){
            columns.push_back(curCol->castTemporal(columnTypes_[i]));
        }else{
            columns.emplace_back(curCol);
        }
    }
    TableSP tableInput = Util::createTable(columnNames_, columns);
    vector<ConstantSP> arg = {(ConstantSP)tableInput};
    ConstantSP res =  conn_.run(appendScript_, arg);
    if(res->isNull())
        return 0;
    return res->getInt();
}

void AutoFitTableAppender::checkColumnType(int col, DATA_CATEGORY category, DATA_TYPE type) {
    if(columnTypes_[col] != type){
        DATA_CATEGORY expectCategory = columnCategories_[col];
        if (category != expectCategory) {
            throw  RuntimeException("column " + std::to_string(col) + ", expect category " + Util::getCategoryString(expectCategory) + ", got category " + Util::getCategoryString(category));
        }
    }
}

AutoFitTableUpsert::AutoFitTableUpsert(const std::string & dbUrl, const std::string & tableName, DBConnection& conn,bool ignoreNull,
                                        vector<string> *pkeyColNames,vector<string> *psortColumns)
                        : conn_(conn){
    ConstantSP schema;
    TableSP colDefs;
    VectorSP typeInts;
    DictionarySP tableInfo;
    VectorSP colNames;
    try {
        //upsert!(obj, newData, [ignoreNull=false], [keyColNames], [sortColumns])
        string task;
        if(dbUrl.empty()){
            task = "schema(" + tableName+ ")";
            upsertScript_ = "upsert!{" + tableName + "";
        }
        else{
            task = "schema(loadTable(\"" + dbUrl + "\", \"" + tableName + "\"))";
            upsertScript_ = "upsert!{loadTable('" + dbUrl + "', '" + tableName + "')";
        }
        //ignore newData
        upsertScript_+=",";
        if(ignoreNull == false)
            upsertScript_+=",ignoreNull=false";
        else
            upsertScript_+=",ignoreNull=true";
        int ignoreParamCount=0;
        if(pkeyColNames!=nullptr && pkeyColNames->empty() == false){
            while(ignoreParamCount>0){
                upsertScript_+=",";
                ignoreParamCount--;
            }
            upsertScript_+=",keyColNames=";
            for(auto &one:*pkeyColNames){
                upsertScript_+="`"+one;
            }
        }else{
            ignoreParamCount++;
        }
        if(psortColumns!=nullptr && psortColumns->empty() == false){
            while(ignoreParamCount>0){
                upsertScript_+=",";
                ignoreParamCount--;
            }
            upsertScript_+=",sortColumns=";
            for(auto &one:*psortColumns){
                upsertScript_+="`"+one;
            }
        }else{
            ignoreParamCount++;
        }
        upsertScript_+="}";

        tableInfo = (DictionarySP)conn_.run(task);
        colDefs = (TableSP)tableInfo->getMember("colDefs");
        cols_ = colDefs->rows();
        typeInts = (VectorSP)colDefs->getColumn("typeInt");
        colNames = (VectorSP)colDefs->getColumn("name");
        columnCategories_.resize(cols_);
        columnTypes_.resize(cols_);
        columnNames_.resize(cols_);
        for (int i = 0; i < cols_; ++i) {
            columnTypes_[i] = (DATA_TYPE)typeInts->getInt(i);
            columnCategories_[i] = Util::getCategory(columnTypes_[i]);
            columnNames_[i] = colNames->getString(i);
        }

    } catch (std::exception& e) {
        throw e;
    }
}

int AutoFitTableUpsert::upsert(const TableSP& table){
    if(cols_ != table->columns())
        throw RuntimeException("The input table columns doesn't match the columns of the target table.");

    vector<ConstantSP> columns;
    for(int i = 0; i < cols_; i++){
        auto curCol = (VectorSP)table->getColumn(i);
        checkColumnType(i, curCol->getCategory(), curCol->getType());
        if(columnCategories_[i] == TEMPORAL && curCol->getType() != columnTypes_[i]){
            columns.push_back(curCol->castTemporal(columnTypes_[i]));
        }else{
            columns.emplace_back(curCol);
        }
    }
    TableSP tableInput = Util::createTable(columnNames_, columns);
    vector<ConstantSP> arg = {(ConstantSP)tableInput};
    ConstantSP res =  conn_.run(upsertScript_, arg);
    if(res->getType() == DT_INT && res->getForm() == DF_SCALAR)
        return res->getInt();
    return 0;
}

void AutoFitTableUpsert::checkColumnType(int col, DATA_CATEGORY category, DATA_TYPE type) {
    if(columnTypes_[col] != type){
        DATA_CATEGORY expectCategory = columnCategories_[col];
        if (category != expectCategory) {
            throw  RuntimeException("column " + std::to_string(col) + ", expect category " + Util::getCategoryString(expectCategory) + ", got category " + Util::getCategoryString(category));
        }
    }
}

void ErrorCodeInfo::set(int apiCode, const string &info){
    set(formatApiCode(apiCode), info);
}

void ErrorCodeInfo::set(const string &code, const string &info) {
	errorCode = code;
	errorInfo = info;
}

void ErrorCodeInfo::set(const ErrorCodeInfo &src) {
	set(src.errorCode, src.errorInfo);
}

} // namespace dolphindb
