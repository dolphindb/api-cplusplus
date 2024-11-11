/*
 * DolphinDB.cpp
 *
 *  Created on: Sep 22, 2018
 *      Author: dzhou
 */

#include <ctime>
#include <fstream>
#include <istream>
#include <stack>
#include <algorithm>
#include <random>
#include "Concurrent.h"
#ifndef WINDOWS
#include <uuid/uuid.h>
#else
#include <Objbase.h>
#endif

#include "ConstantImp.h"
#include "ConstantMarshall.h"
#include "DolphinDB.h"
#include "ScalarImp.h"
#include "DolphinDBImp.h"
#include "Util.h"
#include "Logger.h"
#include "Domain.h"
#include "DBConnectionPoolImpl.h"
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



#define RECORDTIME(name) //RecordTime _recordTime(name)




namespace dolphindb {

string Constant::EMPTY("");
string Constant::NULL_STR("NULL");
ConstantSP Constant::void_(new Void());
ConstantSP Constant::null_(new Void(true));
ConstantSP Constant::true_(new Bool(true));
ConstantSP Constant::false_(new Bool(false));
ConstantSP Constant::one_(new Int(1));

int Constant::serialize(char* buf, int bufSize, INDEX indexStart, int offset, int cellCountToSerialize, int& numElement, int& partial) const {
    throw RuntimeException(Util::getDataFormString(getForm())+"_"+Util::getDataTypeString(getType())+" serialize cell method not supported");
}

int Constant::serialize(char* buf, int bufSize, INDEX indexStart, int offset, int& numElement, int& partial) const {
    throw RuntimeException(Util::getDataFormString(getForm())+"_"+Util::getDataTypeString(getType())+" serialize method not supported");
}

IO_ERR Constant::deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement) {
    throw RuntimeException(Util::getDataFormString(getForm())+"_"+Util::getDataTypeString(getType())+" deserialize method not supported");
}

ConstantSP Constant::getRowLabel() const {
    return void_;
}

ConstantSP Constant::getColumnLabel() const {
    return void_;
}

const std::string DBConnection::udpIP_{"224.1.1.1"};

DBConnection::DBConnection(bool enableSSL, bool asyncTask, int keepAliveTime, bool compress, bool python, bool isReverseStreaming) :
	conn_(new DBConnectionImpl(enableSSL, asyncTask, keepAliveTime, compress, python, isReverseStreaming)), uid_(""), pwd_(""), ha_(false),
		enableSSL_(enableSSL), asynTask_(asyncTask), compress_(compress), nodes_({}),
		lastConnNodeIndex_(0), python_(python), reconnect_(false), closed_(true), runSeqNo_(0){
}

DBConnection::DBConnection(DBConnection&& oth) :
		conn_(move(oth.conn_)), uid_(move(oth.uid_)), pwd_(move(oth.pwd_)),
		initialScript_(move(oth.initialScript_)), ha_(oth.ha_), enableSSL_(oth.enableSSL_),
		asynTask_(oth.asynTask_),compress_(oth.compress_),nodes_(oth.nodes_),lastConnNodeIndex_(0),
		reconnect_(oth.reconnect_), closed_(oth.closed_), runSeqNo_(oth.runSeqNo_){}

DBConnection& DBConnection::operator=(DBConnection&& oth) {
    if (this == &oth) { return *this; }
    conn_ = move(oth.conn_);
    uid_ = move(oth.uid_);
    pwd_ = move(oth.pwd_);
    initialScript_ = move(oth.initialScript_);
    ha_ = oth.ha_;
    nodes_ = oth.nodes_;
    oth.nodes_.clear();
    enableSSL_ = oth.enableSSL_;
    asynTask_ = oth.asynTask_;
	compress_ = oth.compress_;
	lastConnNodeIndex_ = oth.lastConnNodeIndex_;
	reconnect_ = oth.reconnect_;
	closed_ = oth.closed_;
    runSeqNo_ = oth.runSeqNo_;
    return *this;
}

DBConnection::~DBConnection() {
    close();
}

bool DBConnection::connect(const string& hostName, int port, const string& userId, const string& password, const string& startup,
                           bool ha, const vector<string>& highAvailabilitySites, int keepAliveTime, bool reconnect) {
    ha_ = ha;
	uid_ = userId;
	pwd_ = password;
    initialScript_ = startup;
	reconnect_ = reconnect;
	closed_ = false;
    if (ha_) {
		for (auto &one : highAvailabilitySites)
			nodes_.push_back(Node(one));
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
		Node connectedNode;
		TableSP table;
		while (closed_ == false) {
			while(conn_->isConnected()==false && closed_ == false) {
				for (auto &one : nodes_) {
					if (connectNode(one.hostName_, one.port_, keepAliveTime)) {
						connectedNode = one;
						break;
					}
					Thread::sleep(100);
				}
			}
			try {
				table = conn_->run("rpc(getControllerAlias(), getClusterPerf)");
                break;
			}
			catch (std::exception& e) {
				std::cerr << "ERROR getting other data nodes, exception: " << e.what() << std::endl;
				string host;
				int portNumber = 0;
				if (connected()) {
					ExceptionType type = parseException(e.what(), host, portNumber);
					if (type == ET_IGNORE)
						continue;
					else if (type == ET_NEWLEADER || type == ET_NODENOTAVAIL) {
						switchDataNode(host, portNumber);
					}
				}
				else {
                    parseException(e.what(), host, portNumber);
					switchDataNode(host, portNumber);
				}
			}
		}
        if(table->getForm() != DF_TABLE){
            throw IOException("Run getClusterPerf() failed.");
        }
		VectorSP colHost = table->getColumn("host");
		VectorSP colPort = table->getColumn("port");
		VectorSP colMode = table->getColumn("mode");
		VectorSP colmaxConnections = table->getColumn("maxConnections");
		VectorSP colconnectionNum = table->getColumn("connectionNum");
		VectorSP colworkerNum = table->getColumn("workerNum");
		VectorSP colexecutorNum = table->getColumn("executorNum");
		double load;
		for (int i = 0; i < colMode->rows(); i++) {
			if (colMode->getInt(i) == 0) {
				string nodeHost = colHost->getString(i);
				int nodePort = colPort->getInt(i);
				Node *pexistNode = NULL;
				if (!highAvailabilitySites.empty()) {
					for (auto &node : nodes_) {
						if (node.hostName_.compare(nodeHost) == 0 &&
							node.port_ == nodePort) {
							pexistNode = &node;
							break;
						}
					}
					//node is out of highAvailabilitySites
					if (pexistNode == NULL) {
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
				if (pexistNode != NULL) {
					pexistNode->load_ = load;
				}
				else {
					nodes_.push_back(Node(colHost->get(i)->getString(), colPort->get(i)->getInt(), load));
				}
			}
		}
		Node *pMinNode=NULL;
		for (auto &one : nodes_) {
			if (pMinNode == NULL || 
				(one.load_ >= 0 && pMinNode->load_ > one.load_)) {
				pMinNode = &one;
			}
		}
		//DLogger::Info("Connect to min load", pMinNode->load, "site", pMinNode->hostName, ":", pMinNode->port);
		
		if (!pMinNode->isEqual(connectedNode)) {
			conn_->close();
			switchDataNode(pMinNode->hostName_, pMinNode->port_);
			return true;
		}
    } else {
		if (reconnect_) {
			nodes_.push_back(Node(hostName, port));
			switchDataNode();
		}
		else {
			if (!connectNode(hostName, port, keepAliveTime))
				return false;
		}
    }
    initClientID();
	if (!initialScript_.empty()) {
		run(initialScript_);
	}
	return true;
}

bool DBConnection::connected() {
    try {
        ConstantSP ret = conn_->run("1+1");
        return !ret.isNull() && (ret->getInt() == 2);
    } catch (std::exception&) {
        return false;
    }
}

bool DBConnection::connectNode(string hostName, int port, int keepAliveTime) {
	//int attempt = 0;
	while (closed_ == false) {
		try {
			return conn_->connect(hostName, port, uid_, pwd_, enableSSL_, asynTask_, keepAliveTime, compress_,python_);
		}
		catch (IOException& e) {
			if (connected()) {
				ExceptionType type = parseException(e.what(), hostName, port);
				if (type != ET_NEWLEADER) {
					if (type == ET_IGNORE)
						return true;
					else if (type == ET_NODENOTAVAIL)
						return false;
					else { //UNKNOW
						std::cerr << "Connect " << hostName << ":" << port << " failed, exception message: " << e.what() << std::endl;
						return false;
						//throw;
					}
				}
			}
			else {
				return false;
			}
		}
		Util::sleep(100);
	}
	return false;
}

DBConnection::ExceptionType DBConnection::parseException(const string &msg, string &host, int &port) {
	size_t index = msg.find("<NotLeader>");
	if (index != string::npos) {
		index = msg.find(">");
		string ipport = msg.substr(index + 1);
		parseIpPort(ipport,host,port);
		DLogger::Info("Got NotLeaderException, switch to leader node [",host,":",port,"] to run script");
		return ET_NEWLEADER;
	}
	else {
		static string ignoreMsgs[] = { "<ChunkInTransaction>","<DataNodeNotAvail>","<DataNodeNotReady>","DFS is not enabled" };
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
		return ET_UNKNOW;
	}
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
        Thread::sleep(1000);
    }
    if (isConnected && initialScript_.empty() == false)
        run(initialScript_);
}

std::string DBConnection::version()
{
    auto versionStr = run("version()")->getString();
    auto versions = Util::split(versionStr, ' ');
    return std::move(versions[0]);
}

void DBConnection::login(const string& userId, const string& password, bool enableEncryption) {
    conn_->login(userId, password, enableEncryption);
    uid_ = userId;
    pwd_ = password;
}

auto DBConnection::getSubscriptionTopic(const SubscribeInfo &info) -> std::pair<std::string, std::vector<std::string>>
{
    auto result = run("getSubscriptionTopic", info.tableName, info.actionName);
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
    // TODO: support filters
    bool isUDP = bool(protocol == TransportationProtocol::UDP);
    std::string hostName = isUDP ? udpIP_ : info.hostName;
    int port = isUDP ? udpPort_ : info.port;
    auto ret = run("publishTable", hostName, port, info.tableName, info.actionName, config.offset,
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
    auto ret = run("stopPublishTable", hostName, port, info.tableName, info.actionName, false, isUDP);
}

ConstantSP DBConnection::run(const string& script, int priority, int parallelism, int fetchSize, bool clearMemory) {
    LockGuard<Mutex> LockGuard(&mutex_);
    if (nodes_.empty()==false) {
        long seqNo = nextSeqNo();
		while(closed_ == false){
			try {
				return conn_->run(script, priority, parallelism, fetchSize, clearMemory, seqNo);
			}
			catch (IOException& e) {
                if(seqNo > 0)
                    seqNo = -seqNo;
				string host;
				int port = 0;
				if (connected()) {
					ExceptionType type = parseException(e.what(), host, port);
					if (type == ET_IGNORE)
						return new Void();
					else if (type == ET_UNKNOW)
						throw;
				}
				else {
					parseException(e.what(), host, port);
				}
				switchDataNode(host, port);
			}
		}
    } else {
        return conn_->run(script, priority, parallelism, fetchSize, clearMemory, 0);
    }
    return NULL;
}

ConstantSP DBConnection::run(const string& funcName, vector<dolphindb::ConstantSP>& args, int priority, int parallelism, int fetchSize, bool clearMemory) {
    LockGuard<Mutex> LockGuard(&mutex_);
    if (nodes_.empty() == false) {
        long seqNo = nextSeqNo();
        while (closed_ == false) {
			try {
				return conn_->run(funcName, args, priority, parallelism, fetchSize, clearMemory, seqNo);
			}
			catch (IOException& e) {
                if(seqNo > 0)
                    seqNo = -seqNo;
				string host;
				int port = 0;
				if (connected()) {
					ExceptionType type = parseException(e.what(), host, port);
					if (type == ET_IGNORE)
						return new Void();
					else if (type == ET_UNKNOW)
						throw;
				}
				else {
					parseException(e.what(), host, port);
				}
				switchDataNode(host, port);
			}
		}
    } else {
        return conn_->run(funcName, args, priority, parallelism, fetchSize, clearMemory, 0);
    }
    return NULL;
}

ConstantSP DBConnection::upload(const string& name, const ConstantSP& obj) {
    LockGuard<Mutex> LockGuard(&mutex_);
    if (nodes_.empty() == false) {
		while (closed_ == false) {
			try {
				return conn_->upload(name, obj);
			}
			catch (IOException& e) {
				string host;
				int port = 0;
				if (connected()) {
					ExceptionType type = parseException(e.what(), host, port);
					if (type == ET_IGNORE)
						return Constant::void_;
					else if (type == ET_UNKNOW)
						throw;
				}
				else {
					parseException(e.what(), host, port);
				}
				switchDataNode(host, port);
			}
		}
    } else {
        return conn_->upload(name, obj);
    }
	return Constant::void_;
}

ConstantSP DBConnection::upload(vector<string>& names, vector<ConstantSP>& objs) {
    LockGuard<Mutex> LockGuard(&mutex_);
    if (nodes_.empty() == false) {
		while(closed_ == false){
			try {
				return conn_->upload(names, objs);
			}
			catch (IOException& e) {
				string host;
				int port = 0;
				if (connected()) {
					ExceptionType type = parseException(e.what(), host, port);
					if (type == ET_IGNORE)
						return Constant::void_;
					else if (type == ET_UNKNOW)
						throw;
				}
				else {
					parseException(e.what(), host, port);
				}
				switchDataNode(host, port);
			}
		}
    } else {
        return conn_->upload(names, objs);
    }
	return Constant::void_;
}

void DBConnection::parseIpPort(const string &ipport, string &ip, int &port) {
	auto v = Util::split(ipport, ':');
	if (v.size() < 2) {
		throw RuntimeException("The format of highAvailabilitySite " + ipport +
			" is incorrect, should be host:port, e.g. 192.168.1.1:8848");
	}
	ip = v[0];
	port = std::stoi(v[1]);
	if (port <= 0 || port > 65535) {
		throw RuntimeException("The format of highAvailabilitySite " + ipport +
			" is incorrect, port should be a positive integer less or equal to 65535");
	}
}

long DBConnection::nextSeqNo(){
    runSeqNo_++;
    if(runSeqNo_ <= 0){
        runSeqNo_ = 1;
    }
    return runSeqNo_;
}

void DBConnection::initClientID(){
    if(nodes_.empty()){
        return;
    }
    auto versionStr = conn_->run("version()")->getString();
    versionStr = Util::split(versionStr, ' ')[0];

    if ((versionStr >= "1.30.20.5" && versionStr < "2") || versionStr >= "2.00.9") {
        //server support clientId and seqNo
        Uuid uuid(true);
        std::string clientId_ = uuid.getValue()->getString();
        conn_->setClientId(clientId_);
    }
}

DBConnection::Node::Node(const string &ipport, double loadValue) {
	DBConnection::parseIpPort(ipport, hostName_, port_);
	load_ = loadValue;
}

void DBConnection::close() {
	closed_ = true;
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
    if(in->readInt(rowNum) != OK)
        throw IOException("Failed to read rows for data block.");
    if(in->readInt(colNum) != OK)
        throw IOException("Faield to read col for data block.");
    total_ = (long long)rowNum * (long long)colNum;
}

BlockReader::~BlockReader(){
}

ConstantSP BlockReader::read(){
    if(currentIndex_>=total_)
        return NULL;
    IO_ERR ret;
    short flag;
    if ((ret = in_->readShort(flag)) != OK)
        throw IOException("Failed to read object flag from the socket with IO error type " + std::to_string(ret));

    DATA_FORM form = static_cast<DATA_FORM>(flag >> 8);
    ConstantUnmarshallFactory factory(in_);
    ConstantUnmarshall* unmarshall = factory.getConstantUnmarshall(form);
    if(unmarshall==NULL)
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






DBConnectionPool::DBConnectionPool(const string& hostName, int port, int threadNum, const string& userId, const string& password,
                bool loadBalance, bool highAvailability, bool compress, bool reConnect, bool python)
    : pool_(new DBConnectionPoolImpl(hostName, port, threadNum, userId, password, loadBalance, highAvailability, compress,reConnect,python))
{}
DBConnectionPool::~DBConnectionPool(){}
void DBConnectionPool::run(const string& script, int identity, int priority, int parallelism, int fetchSize, bool clearMemory){
    if(identity < 0)
        throw RuntimeException("Invalid identity: " + std::to_string(identity) + ". Identity must be a non-negative integer.");
    pool_->run(script, identity, priority, parallelism, fetchSize, clearMemory);
}

void DBConnectionPool::run(const string& functionName, const vector<ConstantSP>& args, int identity, int priority, int parallelism, int fetchSize, bool clearMemory){
    if(identity < 0)
        throw RuntimeException("Invalid identity: " + std::to_string(identity) + ". Identity must be a non-negative integer.");
    pool_->run(functionName, args, identity, priority, parallelism, fetchSize, clearMemory);
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

PartitionedTableAppender::PartitionedTableAppender(string dbUrl, string tableName, string partitionColName, DBConnectionPool& pool) {
    pool_ = pool.pool_;
    init(dbUrl, tableName, partitionColName, "");
}

PartitionedTableAppender::PartitionedTableAppender(string dbUrl, string tableName, string partitionColName, string appendFunction, DBConnectionPool& pool) {
    pool_ = pool.pool_;
    init(dbUrl, tableName, partitionColName, appendFunction);
}
PartitionedTableAppender::~PartitionedTableAppender(){}
void PartitionedTableAppender::init(string dbUrl, string tableName, string partitionColName, string appendFunction){
    threadCount_ = pool_->getConnectionCount();
    chunkIndices_.resize(threadCount_);
    ConstantSP partitionSchema;
    TableSP colDefs;
    VectorSP typeInts;
    int partitionType;
    DATA_TYPE partitionColType;
    
    try {
        string task;
        if(dbUrl == ""){
            task = "schema(" + tableName+ ")";
            appendScript_ = "tableInsert{" + tableName + "}";
        }
        else{
            task = "schema(loadTable(\"" + dbUrl + "\", \"" + tableName + "\"))";
            appendScript_ = "tableInsert{loadTable('" + dbUrl + "', '" + tableName + "')}";
        }
        if(appendFunction != ""){
            appendScript_ = appendFunction;
        }
        
        pool_->run(task,identity_);

        while(!pool_->isFinished(identity_)){
            Util::sleep(10);
        }
        
        tableInfo_ = pool_->getData(identity_);
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

        colDefs = tableInfo_->getMember("colDefs");
        cols_ = colDefs->rows();
        typeInts = colDefs->getColumn("typeInt");
        columnCategories_.resize(cols_);
        columnTypes_.resize(cols_);
        for (int i = 0; i < cols_; ++i) {
            columnTypes_[i] = (DATA_TYPE)typeInts->getInt(i);
            columnCategories_[i] = Util::getCategory(columnTypes_[i]);
        }
        
        domain_ = Util::createDomain((PARTITION_TYPE)partitionType, partitionColType, partitionSchema);
    } catch (std::exception& e) {
        throw e;
    } 
}

int PartitionedTableAppender::append(TableSP table){
    if(cols_ != table->columns())
        throw RuntimeException("The input table doesn't match the schema of the target table.");
    for(int i=0; i<cols_; ++i){
        VectorSP curCol = table->getColumn(i);
        checkColumnType(i, curCol->getCategory(), curCol->getType());
		if (columnCategories_[i] == TEMPORAL && curCol->getType() != columnTypes_[i]) {
			curCol = curCol->castTemporal(columnTypes_[i]);
			table->setColumn(i, curCol);
		}
    }
    
    for(int i=0; i<threadCount_; ++i)
        chunkIndices_[i].clear();
    vector<int> keys = domain_->getPartitionKeys(table->getColumn(partitionColumnIdx_));
    vector<int> tasks;
    int rows = static_cast<int>(keys.size());
    for(int i=0; i<rows; ++i){
        int key = keys[i];
        if(key >= 0)
            chunkIndices_[key % threadCount_].emplace_back(i);
		else {
			throw RuntimeException("A value-partition column contain null value at row " + std::to_string(i) + ".");
		}
    }
    for(int i=0; i<threadCount_; ++i){
        if(chunkIndices_[i].size() == 0)
            continue;
        TableSP subTable = table->getSubTable(chunkIndices_[i]);
        tasks.push_back(identity_);
        vector<ConstantSP> args = {subTable};
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

AutoFitTableAppender::AutoFitTableAppender(string dbUrl, string tableName, DBConnection& conn) : conn_(conn){
    ConstantSP schema;
    TableSP colDefs;
    VectorSP typeInts;
    DictionarySP tableInfo;
    VectorSP colNames;
    try {
        string task;
        if(dbUrl == ""){
            task = "schema(" + tableName+ ")";
            appendScript_ = "tableInsert{" + tableName + "}";
        }
        else{
            task = "schema(loadTable(\"" + dbUrl + "\", \"" + tableName + "\"))";
            appendScript_ = "tableInsert{loadTable('" + dbUrl + "', '" + tableName + "')}";
        }
        
        tableInfo =  conn_.run(task);
        colDefs = tableInfo->getMember("colDefs");
        cols_ = colDefs->rows();
        typeInts = colDefs->getColumn("typeInt");
        colNames = colDefs->getColumn("name");
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

int AutoFitTableAppender::append(TableSP table){
    if(cols_ != table->columns())
        throw RuntimeException("The input table columns doesn't match the columns of the target table.");
    
    vector<ConstantSP> columns;
    for(int i = 0; i < cols_; i++){
        VectorSP curCol = table->getColumn(i);
        checkColumnType(i, curCol->getCategory(), curCol->getType());
        if(columnCategories_[i] == TEMPORAL && curCol->getType() != columnTypes_[i]){
            columns.push_back(curCol->castTemporal(columnTypes_[i]));
        }else{
            columns.push_back(curCol);
        }
    }
    TableSP tableInput = Util::createTable(columnNames_, columns);
    vector<ConstantSP> arg = {tableInput};
    ConstantSP res =  conn_.run(appendScript_, arg);
    if(res->isNull())
        return 0;
    else
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

AutoFitTableUpsert::AutoFitTableUpsert(string dbUrl, string tableName, DBConnection& conn,bool ignoreNull,
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
        if(dbUrl == ""){
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
        
        tableInfo =  conn_.run(task);
        colDefs = tableInfo->getMember("colDefs");
        cols_ = colDefs->rows();
        typeInts = colDefs->getColumn("typeInt");
        colNames = colDefs->getColumn("name");
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

int AutoFitTableUpsert::upsert(TableSP table){
    if(cols_ != table->columns())
        throw RuntimeException("The input table columns doesn't match the columns of the target table.");
    
    vector<ConstantSP> columns;
    for(int i = 0; i < cols_; i++){
        VectorSP curCol = table->getColumn(i);
        checkColumnType(i, curCol->getCategory(), curCol->getType());
        if(columnCategories_[i] == TEMPORAL && curCol->getType() != columnTypes_[i]){
            columns.push_back(curCol->castTemporal(columnTypes_[i]));
        }else{
            columns.push_back(curCol);
        }
    }
    TableSP tableInput = Util::createTable(columnNames_, columns);
    vector<ConstantSP> arg = {tableInput};
    ConstantSP res =  conn_.run(upsertScript_, arg);
    if(res->getType() == DT_INT && res->getForm() == DF_SCALAR)
        return res->getInt();
    else
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



DLogger::Level DLogger::minLevel_ = DLogger::LevelDebug;
std::string DLogger::levelText_[] = { "Debug","Info","Warn","Error" };
//std::string DLogger::logFilePath_="/tmp/ddb_python_api.log";
std::string DLogger::logFilePath_;
void DLogger::SetMinLevel(Level level) {
	minLevel_ = level;
}

bool DLogger::WriteLog(std::string &text){
    puts(text.data());
    if(logFilePath_.empty()==false){
        text+="\n";
        Util::writeFile(logFilePath_.data(),text.data(),text.length());
    }
    return true;
}

bool DLogger::FormatFirst(std::string &text, Level level) {
	if (level < minLevel_) {
		return false;
	}
	std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
	text = text + Util::toMicroTimestampStr(now, true) + ": [" +
		std::to_string(Util::getCurThreadId()) + "] " + levelText_[level] + ":";
	return true;
}

std::unordered_map<std::string, RecordTime::Node*> RecordTime::codeMap_;
Mutex RecordTime::mapMutex_;
long RecordTime::lastRecordOrder_ = 0;
RecordTime::RecordTime(const string &name) :
	name_(name) {
	startTime_ = Util::getNanoEpochTime();
	LockGuard<Mutex> LockGuard(&mapMutex_);
	lastRecordOrder_++;
	recordOrder_ = lastRecordOrder_;
	//std::cout<<Util::getEpochTime()<<" "<<name_<<recordOrder_<<" start..."<<std::endl;
}
RecordTime::~RecordTime() {
	long long diff = Util::getNanoEpochTime() - startTime_;
	LockGuard<Mutex> LockGuard(&mapMutex_);
	std::unordered_map<std::string, RecordTime::Node*>::iterator iter = codeMap_.find(name_);
	RecordTime::Node *pnode;
	if (iter != codeMap_.end()) {
		pnode = iter->second;
	}
	else {
		pnode = new Node();
		pnode->minOrder = recordOrder_;
		pnode->name = name_;
		codeMap_[name_] = pnode;
	}
	if (pnode->minOrder > recordOrder_) {
		pnode->minOrder = recordOrder_;
	}
	pnode->costTime.push_back(diff);
}
std::string RecordTime::printAllTime() {
	std::string output;
	LockGuard<Mutex> LockGuard(&mapMutex_);
	std::vector<RecordTime::Node*> nodes;
	nodes.reserve(codeMap_.size());
	for (std::unordered_map<std::string, RecordTime::Node*>::iterator iter = codeMap_.begin(); iter != codeMap_.end(); iter++) {
		nodes.push_back(iter->second);
	}
	std::sort(nodes.begin(), nodes.end(), [](RecordTime::Node *a, RecordTime::Node *b) {
		return a->minOrder < b->minOrder;
	});
	static double ns2s = 1000000.0;
	for (RecordTime::Node *node : nodes) {
		long sumNsOverflow = 0;
		long long sumNs = 0;//ns
		double maxNs = 0, minNs = 0;
		for (long long one : node->costTime) {
			sumNs += one;
			if (sumNs < 0) {
				sumNsOverflow++;
				sumNs = -(sumNs + LLONG_MAX);
			}
			if (maxNs < one) {
				maxNs = static_cast<double>(one);
			}
			if (minNs == 0 || minNs > one) {
				minNs = static_cast<double>(one);
			}
		}
        size_t timeCount = node->costTime.size();
		double sum = sumNsOverflow * (LLONG_MAX / ns2s) + sumNs / ns2s;
        double avg = sum / timeCount;
		double min = minNs / ns2s;
		double max = maxNs / ns2s;
        double stdDev = 0.0;
        double diff;
        for (long long one : node->costTime) {
            diff = one/ ns2s - avg;
            stdDev += (diff * diff) / timeCount;
        }
        stdDev = sqrt(stdDev);
		output = output + node->name + ": sum = " + std::to_string(sum) + " count = " + std::to_string(node->costTime.size()) +
			" avg = " + std::to_string(avg) + " stdDev = " + std::to_string(stdDev) +
			" min = " + std::to_string(min) + " max = " + std::to_string(max) + "\n";
		delete node;
	}
	codeMap_.clear();
	return output;
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

}    // namespace dolphindb
