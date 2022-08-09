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
#ifndef WINDOWS
#include <uuid/uuid.h>
#else
#include <Objbase.h>
#endif

#include "Concurrent.h"
#include "ConstantImp.h"
#include "ConstantMarshall.h"
#include "DolphinDB.h"
#include "ScalarImp.h"
#include "Util.h"

using std::ifstream;

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
#define APIMinVersionRequirement 210
#define SYMBOLBASE_MAX_SIZE 1<<21

#define RECORDTIME(name) //RecordTime _recordTime(name)
#define DLOG //DLogger::Info

static inline uint32_t murmur32_16b (const unsigned char* key){
	const uint32_t m = 0x5bd1e995;
	const int r = 24;
	uint32_t h = 16;

	uint32_t k1 = *(uint32_t*)(key);
	uint32_t k2 = *(uint32_t*)(key + 4);
	uint32_t k3 = *(uint32_t*)(key + 8);
	uint32_t k4 = *(uint32_t*)(key + 12);

	k1 *= m;
	k1 ^= k1 >> r;
	k1 *= m;

	k2 *= m;
	k2 ^= k2 >> r;
	k2 *= m;

	k3 *= m;
	k3 ^= k3 >> r;
	k3 *= m;

	k4 *= m;
	k4 ^= k4 >> r;
	k4 *= m;

	// Mix 4 bytes at a time into the hash
	h *= m;
	h ^= k1;
	h *= m;
	h ^= k2;
	h *= m;
	h ^= k3;
	h *= m;
	h ^= k4;

	// Do a few final mixes of the hash to ensure the last few
	// bytes are well-incorporated.
	h ^= h >> 13;
	h *= m;
	h ^= h >> 15;

	return h;
}

namespace dolphindb {

class DdbInit {
public:
	DdbInit() {
#ifdef WINDOWS
		WSADATA wsaData;
		if (WSAStartup(MAKEWORD(2, 1), &wsaData) != 0) {
			throw IOException("Failed to initialize the windows socket.");
		}
#endif
		initFormatters();
	}
};
class EXPORT_DECL DBConnectionImpl {
public:
    DBConnectionImpl(bool sslEnable = false, bool asynTask = false, int keepAliveTime = 7200, bool compress = false, bool python = false);
    ~DBConnectionImpl();
    bool connect(const string& hostName, int port, const string& userId = "", const string& password = "",bool sslEnable = false, bool asynTask = false, int keepAliveTime = -1, bool compress= false, bool python = false);
    void login(const string& userId, const string& password, bool enableEncryption);
    ConstantSP run(const string& script, int priority = 4, int parallelism = 2, int fetchSize = 0, bool clearMemory = false);
    ConstantSP run(const string& funcName, vector<ConstantSP>& args, int priority = 4, int parallelism = 2, int fetchSize = 0, bool clearMemory = false);
    ConstantSP upload(const string& name, const ConstantSP& obj);
	ConstantSP upload(vector<string>& names, vector<ConstantSP>& objs);
    void close();
    bool isConnected() { return isConnected_; }
	void getHostPort(string &host, int &port) { host = hostName_; port = port_; }
	
private:
	long generateRequestFlag(bool clearSessionMemory = false, bool disablepickle = false, bool pickleTableToList = false);
    ConstantSP run(const string& script, const string& scriptType, vector<ConstantSP>& args, int priority = 4, int parallelism = 2,int fetchSize = 0, bool clearMemory = false);
    bool connect();
    void login();

private:
    SocketSP conn_;
    string sessionId_;
    string hostName_;
    int port_;
    string userId_;
    string pwd_;
    bool encrypted_;
    bool isConnected_;
    bool littleEndian_;
    bool sslEnable_;
    bool asynTask_;
    int keepAliveTime_;
	bool compress_;
	bool enablePickle_, python_;
    static DdbInit ddbInit_;
};

class TaskStatusMgmt{
public:
    enum TASK_STAGE{WAITING, FINISHED, ERRORED};
    struct Result{
        Result(TASK_STAGE s = WAITING, const ConstantSP c = Constant::void_, const string &msg = "") : stage(s), result(c), errMsg(msg){}
        TASK_STAGE stage;
        ConstantSP result;
        string errMsg;
    };

    bool isFinished(int identity);
    ConstantSP getData(int identity);
    void setResult(int identity, Result);
private:
    Mutex mutex_;
    unordered_map<int, Result> results;
};

class DBConnectionPoolImpl{
public:
    struct Task{
        Task(const string& sc = "", int id = 0, int pr = 4, int pa = 2, bool clearM = false)
        		: script(sc), identity(id), priority(pr), parallelism(pa), clearMemory(clearM){}
        Task(const string& function, const vector<ConstantSP>& args, int id = 0, int pr = 4, int pa = 2, bool clearM = false) 
			: script(function), arguments(args), identity(id), priority(pr), parallelism(pa), clearMemory(clearM){ isFunc = true; }
		string script;
		vector<ConstantSP> arguments;
        int identity;
        int priority;
        int parallelism;
		bool clearMemory;
		bool isFunc = false;
    };
    
    DBConnectionPoolImpl(const string& hostName, int port, int threadNum = 10, const string& userId = "", const string& password = "",
		bool loadBalance = true, bool highAvailability = true, bool compress = false, bool reConnect=false, bool python = false);
    
    ~DBConnectionPoolImpl(){
        shutDown();
		Task emptyTask;
		for (int i = 0; i < workers_.size(); i++)
			queue_->push(emptyTask);
		for (auto& work : workers_) {
			work->join();
		}
    }
    void run(const string& script, int identity, int priority=4, int parallelism=2, int fetchSize=0, bool clearMemory = false){
        queue_->push(Task(script, identity, priority, parallelism, clearMemory));
        taskStatus_.setResult(identity, TaskStatusMgmt::Result());
    }

	void run(const string& functionName, const vector<ConstantSP>& args, int identity, int priority=4, int parallelism=2, int fetchSize=0, bool clearMemory = false){
		queue_->push(Task(functionName, args, identity, priority, parallelism, clearMemory));
        taskStatus_.setResult(identity, TaskStatusMgmt::Result());
    }

    bool isFinished(int identity){
        return taskStatus_.isFinished(identity);
    }

    ConstantSP getData(int identity){
        return taskStatus_.getData(identity);
    }

    void shutDown(){
        shutDownFlag_.store(true);
        latch_->wait();
    }

    bool isShutDown(){
        return shutDownFlag_.load();
    }

	int getConnectionCount(){
		return workers_.size();
	}
private:
    std::atomic<bool> shutDownFlag_;
    CountDownLatchSP latch_;
    vector<ThreadSP> workers_;
    SmartPointer<SynchronizedQueue<Task>> queue_;
    TaskStatusMgmt taskStatus_;
};

class AsynWorker: public Runnable {
public:
    using Task = DBConnectionPoolImpl::Task;
    AsynWorker(DBConnectionPoolImpl& pool, CountDownLatchSP latch, const SmartPointer<DBConnection>& conn,
               const SmartPointer<SynchronizedQueue<Task>>& queue, TaskStatusMgmt& status,
               const string& hostName, int port, const string& userId , const string& password)
            : pool_(pool), latch_(latch), conn_(conn), queue_(queue),taskStatus_(status),
              hostName_(hostName), port_(port), userId_(userId), password_(password){}
protected:
    virtual void run();

private:
    DBConnectionPoolImpl& pool_;
    CountDownLatchSP latch_;
    SmartPointer<DBConnection> conn_;
	SmartPointer<SynchronizedQueue<Task>> queue_;
    TaskStatusMgmt& taskStatus_;
    const string hostName_;
    int port_;
    const string userId_;
    const string password_;
};

string Constant::EMPTY("");
string Constant::NULL_STR("NULL");
ConstantSP Constant::void_(new Void());
ConstantSP Constant::null_(new Void(true));
ConstantSP Constant::true_(new Bool(true));
ConstantSP Constant::false_(new Bool(false));
ConstantSP Constant::one_(new Int(1));

Guid::Guid(bool newGuid) {
    if (!newGuid) {
        memset(uuid_, 0, 16);
    } else {
#ifdef WINDOWS
        CoCreateGuid((GUID*)uuid_);
#else
        uuid_generate(uuid_);
#endif
    }
}

Guid::Guid(unsigned char* guid) {
    memcpy(uuid_, guid, 16);
}

Guid::Guid(const string& guid) {
	if(guid.size() != 36 || !Util::fromGuid(guid.c_str(), uuid_))
		throw RuntimeException("Invalid UUID string");
}

Guid::Guid(const Guid& copy) {
    memcpy(uuid_, copy.uuid_, 16);
}

bool Guid::isZero() const {
    const unsigned char* a = (const unsigned char*)uuid_;
    return (*(long long*)a) == 0 && (*(long long*)(a + 8)) == 0;
}

string Guid::getString() const {
	return getString(uuid_);
}

string Guid::getString(const unsigned char* uuid) {
	char buf[36];
	Util::toGuid(uuid, buf);
	return string(buf, 36);
}

uint64_t GuidHash::operator()(const Guid& guid) const {
   return murmur32_16b(guid.bytes());
}

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

ConstantSP Vector::getColumnLabel() const {
    return new String(name_);
}

string Vector::getString() const {
    if (getForm() == DF_PAIR) {
        return getScript();
    } else {
        int len = (std::min)(Util::DISPLAY_ROWS, size());
        bool notTuple = getType() != DT_ANY;
        string str(notTuple ? "[" : "(");

        if (len > 0) {
            if (len == 1 && isNull(0))
                str.append(get(0)->getScript());
            else {
                if (isNull(0)) {
                    // do nothing
                } else if (notTuple || get(0)->isScalar())
                    str.append(get(0)->getScript());
                else
                    str.append(getString(0));
            }
        }
        for (int i = 1; i < len; ++i) {
            str.append(",");
            if (isNull(i)) {
                // do nothing
            } else if (notTuple || get(i)->isScalar())
                str.append(get(i)->getScript());
            else
                str.append(getString(i));
        }
        if (size() > len)
            str.append("...");
        str.append(notTuple ? "]" : ")");
        return str;
    }
}

string Vector::getScript() const {
    if (getForm() == DF_PAIR) {
        string str = get(0)->getScript();
        str.append(" : ");
        str.append(get(1)->getScript());
        return str;
    } else if (getForm() == DF_MATRIX) {
        return name_.empty() ? "matrix()" : name_;
    } else {
        int len = size();
        if (len > Util::CONST_VECTOR_MAX_SIZE)
            return name_.empty() ? "array()" : name_;

        string str("[");
        if (len > 0)
            str.append(get(0)->getScript());
        for (int i = 1; i < len; ++i) {
            str.append(",");
            str.append(get(i)->getScript());
        }
        str.append("]");
        return str;
    }
}

Matrix::Matrix(int cols, int rows) : cols_(cols), rows_(rows), rowLabel_(Constant::void_), colLabel_(Constant::void_) {
}

void Matrix::setRowLabel(const ConstantSP& label) {
    if (label->getType() == DT_VOID)
        rowLabel_ = label;
    else if (label->isTemporary())
        rowLabel_ = label;
    else
        rowLabel_ = label->getValue();
    rowLabel_->setTemporary(false);
}

void Matrix::setColumnLabel(const ConstantSP& label) {
    if (label->getType() == DT_VOID)
        colLabel_ = label;
    else if (label->isTemporary())
        colLabel_ = label;
    else
        colLabel_ = label->getValue();
    colLabel_->setTemporary(false);
}

bool Matrix::reshape(INDEX cols, INDEX rows) {
    if (cols_ == cols && rows_ == rows)
        return true;
    if (cols_ * rows_ != cols * rows && rows != rows_)
        return false;
    cols_ = cols;
    rows_ = rows;
    if (!colLabel_->isNothing() && colLabel_->size() != cols_)
        colLabel_ = Constant::void_;
    if (!rowLabel_->isNothing() && rowLabel_->size() != rows_)
        rowLabel_ = Constant::void_;
    return true;
}
string Matrix::getString() const {
    int rows = (std::min)(Util::DISPLAY_ROWS, rows_);
    int limitColMaxWidth = 25;
    int length = 0;
    int curCol = 0;
    int maxColWidth;
    vector<string> list(rows + 1);
    vector<string> listTmp(rows + 1);
    string separator;
    int i, curSize;

    // display row label
    if (!rowLabel_->isNull()) {
        listTmp[0] = "";
        maxColWidth = 0;
        for (i = 0; i < rows; i++) {
            listTmp[i + 1] = rowLabel_->getString(i);
            if ((int)listTmp[i + 1].size() > maxColWidth)
                maxColWidth = listTmp[i + 1].size();
        }

        for (i = 0; i <= rows; i++) {
            curSize = listTmp[i].size();
            if (curSize <= maxColWidth) {
                list[i].append(listTmp[i]);
                if (curSize < maxColWidth)
                    list[i].append(maxColWidth - curSize, ' ');
            } else {
                if (maxColWidth > 3)
                    list[i].append(listTmp[i].substr(0, maxColWidth - 3));
                list[i].append("...");
            }
            list[i].append(1, i == 0 ? ' ' : '|');
        }

        maxColWidth++;
        separator.append(maxColWidth, ' ');
        length += maxColWidth;
    }

    while (length < Util::DISPLAY_WIDTH && curCol < cols_) {
        listTmp[0] = colLabel_->isNull() ? "#" + Util::convert(curCol) : colLabel_->getString(curCol);
        maxColWidth = 0;
        for (i = 0; i < rows; i++) {
            listTmp[i + 1] = getString(curCol, i);
            if ((int)listTmp[i + 1].size() > maxColWidth)
                maxColWidth = listTmp[i + 1].size();
        }
        if (maxColWidth > limitColMaxWidth)
            maxColWidth = limitColMaxWidth;
        if ((int)listTmp[0].size() > maxColWidth)
            maxColWidth = (std::min)(limitColMaxWidth, (int)listTmp[0].size());
        separator.append(maxColWidth, '-');
        if (curCol < cols_ - 1) {
            maxColWidth++;
            separator.append(1, ' ');
        }

        if (length + maxColWidth > Util::DISPLAY_WIDTH && curCol + 1 < cols_)
            break;

        for (i = 0; i <= rows; i++) {
            curSize = listTmp[i].size();
            if (curSize <= maxColWidth) {
                list[i].append(listTmp[i]);
                if (curSize < maxColWidth)
                    list[i].append(maxColWidth - curSize, ' ');
            } else {
                if (maxColWidth > 3)
                    list[i].append(listTmp[i].substr(0, maxColWidth - 3));
                list[i].append("...");
            }
        }
        length += maxColWidth;
        curCol++;
    }

    if (curCol < cols_) {
        for (i = 0; i <= rows; i++)
            list[i].append("...");
        separator.append(3, '-');
    }

    string resultStr(list[0]);
    resultStr.append("\n");
    resultStr.append(separator);
    resultStr.append("\n");
    for (i = 1; i <= rows; i++) {
        resultStr.append(list[i]);
        resultStr.append("\n");
    }
    if (rows < rows_)
        resultStr.append("...\n");
    return resultStr;
}

string Matrix::getString(INDEX index) const {
    int len = (std::min)(Util::DISPLAY_ROWS, rows_);
    string str("{");

    if (len > 0)
        str.append(getString(index, 0));
    for (int i = 1; i < len; ++i) {
        str.append(",");
        str.append(getString(index, i));
    }
    if (rows_ > len)
        str.append("...");
    str.append("}");
    return str;
}

ConstantSP Matrix::get(const ConstantSP& index) const {
    if (index->isScalar()) {
        int col = index->getInt();
        if (col < 0 || col >= cols_)
            throw OperatorRuntimeException("matrix", "The column index " + Util::convert(col) + " is out of range.");
        return getColumn(col);
    }

    ConstantSP indexCols(index);
    if (index->isPair()) {
        int colStart = index->isNull(0) ? 0 : index->getInt(0);
        int colEnd = index->isNull(1) ? cols_ : index->getInt(1);
        int length = std::abs(colEnd - colStart);

        indexCols = Util::createIndexVector(length, true);
        INDEX* data = indexCols->getIndexArray();
        if (colStart <= colEnd) {
            for (int i = 0; i < length; ++i)
                data[i] = colStart + i;
        } else {
            --colStart;
            for (int i = 0; i < length; ++i)
                data[i] = colStart - i;
        }
    }

    // create a matrix
    int cols = indexCols->size();
    ConstantSP result = getInstance(cols);
    if (!rowLabel_.isNull())
        result->setRowLabel(rowLabel_->getValue());
    if (!colLabel_.isNull()) {
        result->setColumnLabel(colLabel_->get(indexCols));
    }
    for (int i = 0; i < cols; ++i) {
        int cur = indexCols->getInt(i);
        if (cur < 0 || cur >= cols_)
            throw OperatorRuntimeException("matrix", "The column index " + Util::convert(cur) + " is out of range.");
        result->setColumn(i, getColumn(cur));
    }
    return result;
}

bool Matrix::set(const ConstantSP index, const ConstantSP& value) {
    int cols = index->size();
    bool scalar = value->isScalar();
    if (value->size() != rows_ * cols && !scalar)
        throw OperatorRuntimeException("matrix", "matrix and assigned value are not compatible");
    if (cols == 1) {
        int cur = index->getInt(0);
        if (cur >= cols_ || cur < 0)
            throw OperatorRuntimeException("matrix", "The column index " + Util::convert(cur) + " is out of range.");
        setColumn(cur, value);
        return true;
    }

    for (int i = 0; i < cols; ++i) {
        int cur = index->getInt(i);
        if (cur >= cols_ || cur < 0)
            throw OperatorRuntimeException("matrix", "The column index " + Util::convert(cur) + " is out of range.");
        setColumn(cur, scalar ? value : ((Vector*)value.get())->getSubVector(rows_ * i, rows_));
    }
    return true;
}

DFSChunkMeta::DFSChunkMeta(const string& path, const Guid& id, int version, int size, CHUNK_TYPE chunkType, const vector<string>& sites, long long cid)
    : Constant(2051), type_(chunkType), replicaCount_(sites.size()), version_(version), size_(size), sites_(0), path_(path), cid_(cid), id_(id) {
    if (replicaCount_ == 0)
        return;
    sites_ = new string[replicaCount_];
    for (int i = 0; i < replicaCount_; ++i)
        sites_[i] = sites[i];
}

DFSChunkMeta::DFSChunkMeta(const string& path, const Guid& id, int version, int size, CHUNK_TYPE chunkType, const string* sites, int siteCount, long long cid)
    : Constant(2051), type_(chunkType), replicaCount_(siteCount), version_(version), size_(size), sites_(0), path_(path), cid_(cid), id_(id) {
    if (replicaCount_ == 0)
        return;
    sites_ = new string[replicaCount_];
    for (int i = 0; i < replicaCount_; ++i)
        sites_[i] = sites[i];
}

DFSChunkMeta::DFSChunkMeta(const DataInputStreamSP& in) : Constant(2051), sites_(0), id_(false) {
    IO_ERR ret = in->readString(path_);
    if (ret != OK)
        throw RuntimeException("Failed to deserialize DFSChunkMeta object.");

    char guid[16];
    in->read(guid, 16);
    in->readInt(version_);
    in->readIndex(size_);
    in->readChar(type_);
    ret = in->readChar(replicaCount_);
    if (ret != OK)
        throw RuntimeException("Failed to deserialize DFSChunkMeta object.");
    if (replicaCount_ > 0)
        sites_ = new string[replicaCount_];
    for (int i = 0; i < replicaCount_; ++i) {
        string site;
        if ((ret = in->readString(site)) != OK)
            throw RuntimeException("Failed to deserialize DFSChunkMeta object.");
        sites_[i] = site;
    }
    id_ = Guid((unsigned char*)guid);
    if (in->readLong(cid_) != OK)
        throw RuntimeException("Failed to deserialize DFSChunkMeta object.");
}

DFSChunkMeta::~DFSChunkMeta() {
    if (sites_)
        delete[] sites_;
}

string DFSChunkMeta::getString() const {
    string str(isTablet() ? "Tablet[" : "FileBlock[");
    str.append(path_);
    str.append(", ");
    str.append(id_.getString());
    str.append(", {");
    for (int i = 0; i < replicaCount_; ++i) {
        if (i > 0)
            str.append(", ");
        str.append(sites_[i]);
    }
    str.append("}, v");
    str.append(std::to_string(version_));
    str.append(", ");
    str.append(std::to_string(size_));
    str.append(", c");
    str.append(std::to_string(cid_));
    if (isSplittable())
        str.append(", splittable]");
    else
        str.append("]");
    return str;
}

long long DFSChunkMeta::getAllocatedMemory() const {
    long long length = 33 + sizeof(sites_) + (1 + replicaCount_) * (1 + sizeof(string)) + path_.size();
    for (int i = 0; i < replicaCount_; ++i)
        length += sites_[i].size();
    return length;
}

ConstantSP DFSChunkMeta::getMember(const ConstantSP& key) const {
    if (key->getCategory() != LITERAL || (!key->isScalar() && !key->isArray()))
        throw RuntimeException("DFSChunkMeta attribute must be string type scalar or vector.");
    if (key->isScalar())
        return getAttribute(key->getString());
    else {
        int keySize = key->size();
        ConstantSP result = Util::createVector(DT_ANY, keySize);
        for (int i = 0; i < keySize; ++i) {
            result->set(i, getAttribute(key->getString(i)));
        }
        return result;
    }
}

ConstantSP DFSChunkMeta::getSiteVector() const {
    ConstantSP vec = new StringVector(replicaCount_, replicaCount_);
    for (int i = 0; i < replicaCount_; ++i)
        vec->setString(i, sites_[i]);
    return vec;
}

ConstantSP DFSChunkMeta::getAttribute(const string& attr) const {
    if (attr == "path")
        return new String(path_);
    else if (attr == "id")
        return new String(id_.getString());
    else if (attr == "cid")
        return new Long(cid_);
    else if (attr == "version")
        return new Int(version_);
    else if (attr == "sites")
        return getSiteVector();
    else if (attr == "size") {
        ConstantSP obj = Util::createConstant(DT_INDEX);
        obj->setIndex(size_);
        return obj;
    } else if (attr == "isTablet")
        return new Bool(isTablet());
    else if (attr == "splittable")
        return new Bool(isSplittable());
    else
        return Constant::void_;
}

ConstantSP DFSChunkMeta::keys() const {
    vector<string> attrs({"path", "id", "version", "size", "isTablet", "splittable", "sites", "cid"});
    return new StringVector(attrs, attrs.size(), false);
}

ConstantSP DFSChunkMeta::values() const {
    ConstantSP result = Util::createVector(DT_ANY, 8);
    result->set(0, new String(path_));
    result->set(1, new String(id_.getString()));
    result->set(2, new Int(version_));
    ConstantSP sizeObj = Util::createConstant(DT_INDEX);
    sizeObj->setIndex(size_);
    result->set(3, sizeObj);
    result->set(4, new Bool(isTablet()));
    result->set(5, new Bool(isSplittable()));
    result->set(6, getSiteVector());
    result->set(7, new Long(cid_));
    return result;
}

DdbInit DBConnectionImpl::ddbInit_;
DBConnectionImpl::DBConnectionImpl(bool sslEnable, bool asynTask, int keepAliveTime, bool compress, bool python)
		: port_(0), encrypted_(false), isConnected_(false), littleEndian_(Util::isLittleEndian()), 
		sslEnable_(sslEnable),asynTask_(asynTask), keepAliveTime_(keepAliveTime), compress_(compress),
		enablePickle_(false), python_(python){
}

DBConnectionImpl::~DBConnectionImpl() {
	close();
	conn_.clear();
}

void DBConnectionImpl::close() {
	if (!isConnected_)
		return;
	isConnected_ = false;
    if (!conn_.isNull()) {
        conn_->close();
    }
}

bool DBConnectionImpl::connect(const string& hostName, int port, const string& userId,
		const string& password, bool sslEnable,bool asynTask, int keepAliveTime, bool compress,
		bool python) {
    hostName_ = hostName;
    port_ = port;
    userId_ = userId;
    pwd_ = password;
    encrypted_ = false;
    sslEnable_ = sslEnable;
    asynTask_ = asynTask;
    if(keepAliveTime > 0){
        keepAliveTime_ = keepAliveTime;
    }
	compress_ = compress;
	python_ = python;
    return connect();
}

bool DBConnectionImpl::connect() {
    DLOG("Imp.connect start");
	close();

    SocketSP conn = new Socket(hostName_, port_, true, keepAliveTime_, sslEnable_);
    IO_ERR ret = conn->connect();
    if (ret != OK) {
        return false;
    }
    DLOG("Imp.connect socket ready");

    string body = "connect\n";
    if (!userId_.empty() && !encrypted_)
        body.append("login\n" + userId_ + "\n" + pwd_ + "\nfalse");
    string out("API 0 ");
    out.append(Util::convert((int)body.size()));
	out.append(" / "+ std::to_string(generateRequestFlag())+"_1_" + std::to_string(4) + "_" + std::to_string(2));
    out.append(1, '\n');
    out.append(body);
    size_t actualLength;
    ret = conn->write(out.c_str(), out.size(), actualLength);
    if (ret != OK)
        throw IOException("Couldn't send login message to the given host/port with IO error type " + std::to_string(ret));

    DataInputStreamSP in = new DataInputStream(conn);
    string line;
    ret = in->readLine(line);
    if (ret != OK)
        throw IOException("Failed to read message from the socket with IO error type " + std::to_string(ret));

    vector<string> headers;
    Util::split(line.c_str(), ' ', headers);
    if (headers.size() != 3)
        throw IOException("Received invalid header");
    string sessionId = headers[0];
    int numObject = atoi(headers[1].c_str());
    bool remoteLittleEndian = (headers[2] != "0");

    if ((ret = in->readLine(line)) != OK)
        throw IOException("Failed to read response message from the socket with IO error type " + std::to_string(ret));
    if (line != "OK")
		throw IOException("Server connection response: '" + line);

    if (numObject == 1) {
        short flag;
        if ((ret = in->readShort(flag)) != OK)
            throw IOException("Failed to read object flag from the socket with IO error type " + std::to_string(ret));
        DATA_FORM form = static_cast<DATA_FORM>(flag >> 8);

        ConstantUnmarshallFactory factory(in);
        ConstantUnmarshall* unmarshall = factory.getConstantUnmarshall(form);
        if(unmarshall==NULL)
            throw IOException("Failed to parse the incoming object" + std::to_string(form));
        if (!unmarshall->start(flag, true, ret)) {
            unmarshall->reset();
            throw IOException("Failed to parse the incoming object with IO error type " + std::to_string(ret));
        }
        ConstantSP result = unmarshall->getConstant();
        unmarshall->reset();
        if (!result->getBool())
            throw IOException("Failed to authenticate the user");
    }

    conn_ = conn;
    sessionId_ = sessionId;
    isConnected_ = true;
    littleEndian_ = remoteLittleEndian;

    if (!userId_.empty() && encrypted_) {
        try {
            login();
        } catch (...) {
			close();
            throw;
        }
    }

    ConstantSP requiredVersion;
    
    try {
        if(asynTask_) {
            SmartPointer<DBConnection> newConn = new DBConnection(false, false);
            newConn->connect(hostName_, port_, userId_, pwd_);
            requiredVersion =newConn->run("getRequiredAPIVersion()");
        }else{
            requiredVersion = run("getRequiredAPIVersion()");
        }
    }
    catch(...){
        return true;
    }
    if(!requiredVersion->isTuple()){
        return true;
    }else{
        int apiVersion = requiredVersion->get(0)->getInt();
        if(apiVersion > APIMinVersionRequirement){
			close();
            throw IOException("Required C++ API version at least "  + std::to_string(apiVersion) + ". Current C++ API version is "+ std::to_string(APIMinVersionRequirement) +". Please update DolphinDB C++ API. ");
        }
        if(requiredVersion->size() >= 2 && requiredVersion->get(1)->getString() != ""){
            std::cout<<requiredVersion->get(1)->getString() <<std::endl;
        }
    }
    return true;
}

void DBConnectionImpl::login(const string& userId, const string& password, bool enableEncryption) {
    userId_ = userId;
    pwd_ = password;
    encrypted_ = enableEncryption;
    login();
}

void DBConnectionImpl::login() {
    DLOG("Imp.connect login");
    // TODO: handle the case of encryption.
    vector<ConstantSP> args;
    args.push_back(new String(userId_));
    args.push_back(new String(pwd_));
    args.push_back(new Bool(false));
    ConstantSP result = run("login", args);
    if (!result->getBool())
        throw IOException("Failed to authenticate the user " + userId_);
}

ConstantSP DBConnectionImpl::run(const string& script, int priority, int parallelism, int fetchSize, bool clearMemory) {
    vector<ConstantSP> args;
    return run(script, "script", args, priority, parallelism, fetchSize, clearMemory);
}

ConstantSP DBConnectionImpl::run(const string& funcName, vector<ConstantSP>& args, int priority, int parallelism, int fetchSize, bool clearMemory) {
    return run(funcName, "function", args, priority, parallelism, fetchSize, clearMemory);
}

ConstantSP DBConnectionImpl::upload(const string& name, const ConstantSP& obj) {
    if (!Util::isVariableCandidate(name))
        throw RuntimeException(name + " is not a qualified variable name.");
    vector<ConstantSP> args(1, obj);
    return run(name, "variable", args);
}

ConstantSP DBConnectionImpl::upload(vector<string>& names, vector<ConstantSP>& objs) {
    if (names.size() != objs.size())
        throw RuntimeException("the size of variable names doesn't match the size of objects.");
    if (names.empty())
        return Constant::void_;

    string varNames;
    for (unsigned int i = 0; i < names.size(); ++i) {
        if (!Util::isVariableCandidate(names[i]))
            throw RuntimeException(names[i] + " is not a qualified variable name.");
        if (i > 0)
            varNames.append(1, ',');
        varNames.append(names[i]);
    }
    return run(varNames, "variable", objs);
}

long DBConnectionImpl::generateRequestFlag(bool clearSessionMemory, bool disablepickle, bool pickleTableToList) {
	long flag = 32; //32 API client
	if (asynTask_){
        DLOG("async");
		flag += 4;
    }
	if (clearSessionMemory){
        DLOG("clearMem");
		flag += 16;
    }
	if (enablePickle_ == false || disablepickle) {
		if (compress_)
			flag += 64;
	}
	else {//enable pickle
        DLOG("pickle",pickleTableToList?"toList":"");
		flag += 8;
		if (pickleTableToList) {
			flag += (1 << 15);
		}
	}
	if (python_){
        DLOG("python");
		flag += 2048;
    }
	return flag;
}

ConstantSP DBConnectionImpl::run(const string& script, const string& scriptType, vector<ConstantSP>& args,
			int priority, int parallelism, int fetchSize, bool clearMemory) {
	DLOG("run1",script,"start");
    if (!isConnected_)
        throw IOException("Couldn't send script/function to the remote host because the connection has been closed");

    if(fetchSize > 0 && fetchSize < 8192)
        throw IOException("fetchSize must be greater than 8192");
    string body;
    int argCount = args.size();
    if (scriptType == "script")
        body = "script\n" + script;
    else {
        body = scriptType + "\n" + script;
        body.append("\n" + std::to_string(argCount));
        body.append("\n");
        body.append(Util::isLittleEndian() ? "1" : "0");
    }
    string out("API2 " + sessionId_ + " ");
    out.append(Util::convert((int)body.size()));
    out.append(" / " + std::to_string(generateRequestFlag(clearMemory,true)) + "_1_" + std::to_string(priority) + "_" + std::to_string(parallelism));
    if(fetchSize > 0)
        out.append("__" + std::to_string(fetchSize));
    out.append(1, '\n');
    out.append(body);
    DLOG("run1",script,"header",out);

    IO_ERR ret;
    if (argCount > 0) {
        for (int i = 0; i < argCount; ++i) {
            if (args[i]->containNotMarshallableObject()) {
                throw IOException("The function argument or uploaded object is not marshallable.");
            }
        }
		DataOutputStreamSP outStream = new DataOutputStream(conn_);
		ConstantMarshallFactory marshallFactory(outStream);
        for (int i = 0; i < argCount; ++i) {
            ConstantMarshall* marshall = marshallFactory.getConstantMarshall(args[i]->getForm());
            if (i == 0)
                marshall->start(out.c_str(), out.size(), args[i], true, compress_, ret);
            else
                marshall->start(args[i], true, compress_, ret);
            marshall->reset();
            if (ret != OK) {
				close();
                throw IOException("Couldn't send function argument to the remote host with IO error type " + std::to_string(ret));
            }
        }
        ret = outStream->flush();
        if (ret != OK) {
			close();
            throw IOException("Failed to marshall code with IO error type " + std::to_string(ret));
        }
    } else {
        size_t actualLength;
        IO_ERR ret = conn_->write(out.c_str(), out.size(), actualLength);
        if (ret != OK) {
			close();
            throw IOException("Couldn't send script/function to the remote host because the connection has been closed");
        }
    }
	
    if(asynTask_)
        return new Void();
    DLOG("run1",script,"read");
	DataInputStreamSP in = new DataInputStream(conn_);
    if (littleEndian_ != (char)Util::isLittleEndian())
        in->enableReverseIntegerByteOrder();

    string line;
    if ((ret = in->readLine(line)) != OK) {
		close();
        throw IOException("Failed to read response header from the socket with IO error type " + std::to_string(ret));
    }
	while (line == "MSG") {
		if ((ret = in->readString(line)) != OK) {
			close();
			throw IOException("Failed to read response msg from the socket with IO error type " + std::to_string(ret));
		}
		std::cout << line << std::endl;
		if ((ret = in->readLine(line)) != OK) {
			close();
			throw IOException("Failed to read response header from the socket with IO error type " + std::to_string(ret));
		}
	}
	vector<string> headers;
    Util::split(line.c_str(), ' ', headers);
    if (headers.size() != 3) {
		close();
        throw IOException("Received invalid header");
    }
    sessionId_ = headers[0];
    int numObject = atoi(headers[1].c_str());

    if ((ret = in->readLine(line)) != OK) {
		close();
        throw IOException("Failed to read response message from the socket with IO error type " + std::to_string(ret));
    }

    if (line != "OK") {
        throw IOException(hostName_+":"+std::to_string(port_)+" Server response: '" + line + "' script: '" + script + "'");
    }

    if (numObject == 0) {
        return new Void();
    }
	
    short flag;
    if ((ret = in->readShort(flag)) != OK) {
		close();
        throw IOException("Failed to read object flag from the socket with IO error type " + std::to_string(ret));
    }
    
    DATA_FORM form = static_cast<DATA_FORM>(flag >> 8);
    DATA_TYPE type = static_cast<DATA_TYPE >(flag & 0xff);
    if(fetchSize > 0 && form == DF_VECTOR && type == DT_ANY)
        return new BlockReader(in);
    ConstantUnmarshallFactory factory(in);
    ConstantUnmarshall* unmarshall = factory.getConstantUnmarshall(form);
    if(unmarshall == NULL){
        DLOG("run1",script,"invalid form",form);
        //FIXME ignore it now until server fix this bug.
        //DLogger::Error("Unknow incoming object form",form,"of type",type);
        in->reset(0);
        conn_->skipAll();
        return Constant::void_;
    }
    DLOG("run1",script,"start unmarshall");
    if (!unmarshall->start(flag, true, ret)) {
        unmarshall->reset();
		close();
        throw IOException("Failed to parse the incoming object with IO error type " + std::to_string(ret));
    }

    ConstantSP result = unmarshall->getConstant();
    unmarshall->reset();
	DLOG("run1",script,"end");
    return result;
}

DBConnection::DBConnection(bool enableSSL, bool asyncTask, int keepAliveTime, bool compress, bool python) :
	conn_(new DBConnectionImpl(enableSSL, asyncTask, keepAliveTime, compress, python)), uid_(""), pwd_(""), ha_(false),
		enableSSL_(enableSSL), asynTask_(asyncTask), compress_(compress), python_(python), nodes_(NULL),
		lastConnNodeIndex_(0), reconnect_(false), closed_(true){
}

DBConnection::DBConnection(DBConnection&& oth) :
		conn_(move(oth.conn_)), uid_(move(oth.uid_)), pwd_(move(oth.pwd_)),
		initialScript_(move(oth.initialScript_)), ha_(oth.ha_), enableSSL_(oth.enableSSL_),
		asynTask_(oth.asynTask_),compress_(oth.compress_),nodes_(oth.nodes_),lastConnNodeIndex_(0),
		reconnect_(oth.reconnect_), closed_(oth.closed_){}

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
		Node connectedNode;
		TableSP table;
		while (closed_ == false) {
			while(conn_->isConnected()==false && closed_ == false) {
				for (auto &one : nodes_) {
					if (connectNode(one.hostName, one.port, keepAliveTime)) {
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
			catch (exception& e) {
				std::cerr << "ERROR getting other data nodes, exception: " << e.what() << std::endl;
				string host;
				int port = 0;
				if (connected()) {
					ExceptionType type = parseException(e.what(), host, port);
					if (type == ET_IGNORE)
						continue;
					else if (type == ET_NEWLEADER || type == ET_NODENOTAVAIL) {
						switchDataNode(host, port);
					}
				}
				else {
                    parseException(e.what(), host, port);
					switchDataNode(host, port);
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
						if (node.hostName.compare(nodeHost) == 0 &&
							node.port == nodePort) {
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
					pexistNode->load = load;
				}
				else {
					nodes_.push_back(Node(colHost->get(i)->getString(), colPort->get(i)->getInt(), load));
				}
			}
		}
		Node *pMinNode=NULL;
		for (auto &one : nodes_) {
			if (pMinNode == NULL || 
				(one.load >= 0 && pMinNode->load > one.load)) {
				pMinNode = &one;
			}
		}
		//DLogger::Info("Connect to min load", pMinNode->load, "site", pMinNode->hostName, ":", pMinNode->port);
		
		if (!pMinNode->isEqual(connectedNode)) {
			conn_->close();
			switchDataNode(pMinNode->hostName, pMinNode->port);
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
	if (!initialScript_.empty()) {
		run(initialScript_);
	}
	return true;
}

bool DBConnection::connected() {
    try {
        ConstantSP ret = conn_->run("1+1");
        return !ret.isNull() && (ret->getInt() == 2);
    } catch (exception& e) {
        return false;
    }
}

bool DBConnection::connectNode(string hostName, int port, int keepAliveTime) {
	DLOG("Connect to",hostName ,":",port,".");
	int attempt = 0;
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
	int index = msg.find("<NotLeader>");
	if (index != string::npos) {
		index = msg.find(">");
		string ipport = msg.substr(index + 1);
		parseIpPort(ipport,host,port);
		//std::cerr << "Got new leader exception, new leader is " << host << ":" << port << std::endl;
		DLogger::Info("New leader is",host,":",port,".");
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
	bool connected = false;
	while (connected == false && closed_ == false){
		if (!host.empty()) {
			if (connectNode(host, port)) {
				connected = true;
				break;
			}
		}
		if (nodes_.empty()) {
			throw RuntimeException("Failed to connect to " + host + ":" + std::to_string(port));
		}
		for (int i = nodes_.size() - 1; i >= 0; i--) {
			lastConnNodeIndex_ = (lastConnNodeIndex_ + 1) % nodes_.size();
			if (connectNode(nodes_[lastConnNodeIndex_].hostName, nodes_[lastConnNodeIndex_].port)) {
				connected = true;
				break;
			}
		}
		Thread::sleep(1000);
	}
    if (connected && initialScript_.empty() == false)
		run(initialScript_);
}

void DBConnection::login(const string& userId, const string& password, bool enableEncryption) {
    conn_->login(userId, password, enableEncryption);
    uid_ = userId;
    pwd_ = password;
}

ConstantSP DBConnection::run(const string& script, int priority, int parallelism, int fetchSize, bool clearMemory) {
    if (nodes_.empty()==false) {
		while(closed_ == false){
			try {
				return conn_->run(script, priority, parallelism, fetchSize, clearMemory);
			}
			catch (IOException& e) {
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
        return conn_->run(script, priority, parallelism, fetchSize, clearMemory);
    }
    return NULL;
}

ConstantSP DBConnection::run(const string& funcName, vector<dolphindb::ConstantSP>& args, int priority, int parallelism, int fetchSize, bool clearMemory) {
    if (nodes_.empty() == false) {
        while (closed_ == false) {
			try {
				return conn_->run(funcName, args, priority, parallelism, fetchSize, clearMemory);
			}
			catch (IOException& e) {
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
        return conn_->run(funcName, args, priority, parallelism, fetchSize, clearMemory);
    }
    return NULL;
}

ConstantSP DBConnection::upload(const string& name, const ConstantSP& obj) {
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

DBConnection::Node::Node(const string &ipport, double loadValue) {
	DBConnection::parseIpPort(ipport, hostName, port);
	load = loadValue;
}

void DBConnection::close() {
	closed_ = true;
    if (conn_) conn_->close();
}

const std::string& DBConnection::getInitScript() const {
    return initialScript_;
}

void DBConnection::setInitScript(const std::string & script) {
    initialScript_ = script;
}

BlockReader::BlockReader(const DataInputStreamSP& in ) : in_(in), total_(0), currentIndex_(0){
    int rows, cols;
    if(in->readInt(rows) != OK)
        throw IOException("Failed to read rows for data block.");
    if(in->readInt(cols) != OK)
        throw IOException("Faield to read col for data block.");
    total_ = (long long)rows * (long long)cols;
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


Domain::Domain(PARTITION_TYPE partitionType, DATA_TYPE partitionColType) : partitionType_(partitionType), partitionColType_(partitionColType){
	partitionColCategory_ = Util::getCategory(partitionColType_);
}

DBConnectionPoolImpl::DBConnectionPoolImpl(const string& hostName, int port, int threadNum, const string& userId, const string& password,
	bool loadBalance, bool highAvailability, bool compress,bool reConnect, bool python) :shutDownFlag_(
        false), queue_(new SynchronizedQueue<Task>){
    latch_ = new CountDownLatch(threadNum);
    if(!loadBalance){
        for(int i = 0 ;i < threadNum; i++){
            SmartPointer<DBConnection> conn = new DBConnection(false, false, 7200, compress, python);
			bool ret = conn->connect(hostName, port, userId, password, "", highAvailability, {},7200, reConnect);
            if(!ret)
                throw IOException("Failed to connect to " + hostName + ":" + std::to_string(port));
            workers_.push_back(new Thread(new AsynWorker(*this,latch_, conn, queue_, taskStatus_, hostName, port, userId, password)));
            workers_.back()->start();
        }
    }
    else{
        SmartPointer<DBConnection> entryPoint = new DBConnection(false, false, 7200, compress, python);
        bool ret = entryPoint->connect(hostName, port, userId, password, "", highAvailability, {},7200, reConnect);
        if(!ret)
           throw IOException("Failed to connect to " + hostName + ":" + std::to_string(port));
	    ConstantSP nodes = entryPoint->run("rpc(getControllerAlias(), getClusterLiveDataNodes{false})");
        INDEX nodeCount = nodes->size();
        vector<string> hosts(nodeCount);
        vector<int> ports(nodeCount);
        for(int i = 0; i < nodeCount; i++){
            string fields = nodes->getString(i);
            size_t p = fields.find(":");
            if(p == string::npos)
				throw  RuntimeException("Invalid data node address: " + fields);
            hosts[i] = fields.substr(0, p);
            ports[i] = std::atoi(fields.substr(p + 1, fields.size()).data());
        }
        for(int i = 0 ;i < threadNum; i++){
            SmartPointer<DBConnection> conn = new DBConnection(false, false, 7200, compress, python);
			string &curhost = hosts[i % nodeCount];
			int &curport = ports[i % nodeCount];
            bool ret = conn->connect(curhost, curport, userId, password, "", highAvailability, {}, 7200, reConnect);
            if(!ret)
                throw IOException("Failed to connect to " + curhost + ":" + std::to_string(curport));
            workers_.push_back(new Thread(new AsynWorker(*this,latch_, conn, queue_, taskStatus_, curhost, curport, userId, password)));
            workers_.back()->start();
        }
    }
}

void AsynWorker::run() {
    while(true) {
        if(pool_.isShutDown()){
            conn_->close();
            latch_->countDown();
            std::cout<<"Asyn worker closed peacefully."<<std::endl;
            break;
        }

        Task task;
        ConstantSP result;
        bool errorFlag = false;
        if (!queue_->blockingPop(task, 1000))
            continue;
		if (task.script.empty())
			continue;
        while(true) {
            try {
                if(task.isFunc){
                    result = conn_->run(task.script, task.arguments, task.priority, task.parallelism, 0, task.clearMemory);
                }
                else{
                    result = conn_->run(task.script, task.priority, task.parallelism, 0, task.clearMemory);
                }
                break;
            }
            catch(IOException & ex){
				errorFlag = true;
                std::cerr<<"Async task worker come across exception : "<<ex.what()<<std::endl;
                taskStatus_.setResult(task.identity, TaskStatusMgmt::Result(TaskStatusMgmt::ERRORED, new Void(), ex.what()));
                break;
                // if(reConnectFlag_ && !conn_->connected()){
                //     while(true){
                //         try {
                //             if(conn_->connect(hostName_, port_, userId_, password_))
                //                 break;
                //             std::cerr << "Connect Failed, retry in one second." << std::endl;
                //             Thread::sleep(1000);
                //         } catch (IOException &e) {
                //             std::cerr << "Connect Failed, retry in one second." << std::endl;
                //             Thread::sleep(1000);
                //         }
                //     }
                // } else {
                //     
                // }
            }
        }
        if(!errorFlag)
            taskStatus_.setResult(task.identity, TaskStatusMgmt::Result(TaskStatusMgmt::FINISHED,  result));
    }
}

bool TaskStatusMgmt::isFinished(int identity){
    LockGuard<Mutex> guard(&mutex_);
    if(results.count(identity) == 0)
        throw RuntimeException("Task [" + std::to_string(identity) + "] does not exist.");
    if(results[identity].stage == ERRORED)
        throw RuntimeException("Task [" + std::to_string(identity) + "] come across exception : " + results[identity].errMsg);
    return results[identity].stage == FINISHED;
}

void TaskStatusMgmt::setResult(int identity, Result r){
    LockGuard<Mutex> guard(&mutex_);
    results[identity] = r;
}

ConstantSP TaskStatusMgmt::getData(int identity){
    LockGuard<Mutex> guard(&mutex_);
    if(results.count(identity) == 0)
        throw RuntimeException("Task [" + std::to_string(identity) + "] does not exist, the result may be fetched yet.");
    assert(results[identity].stage == FINISHED);
    ConstantSP re = results[identity].result;
    results.erase(identity);
    return re;
}

DBConnectionPool::DBConnectionPool(const string& hostName, int port, int threadNum, const string& userId, const string& password,
				bool loadBalance, bool highAvailability, bool compress, bool reConnect, bool python){
    pool_ = new DBConnectionPoolImpl(hostName, port, threadNum, userId, password, loadBalance, highAvailability, compress,reConnect,python);
}

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
    } catch (exception& e) {
        throw;
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
    int rows = keys.size();
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
        
    } catch (exception& e) {
        throw;
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
        
    } catch (exception& e) {
        throw;
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

SymbolBase::SymbolBase(const DataInputStreamSP& in, IO_ERR& ret){
    ret = in->readInt(id_);
    if(ret != OK)
        return;
    int size;
    ret = in->readInt(size);
    if(ret != OK)
        return;

    for(int i = 0; i < size; i++){
        string s;
        ret = in->readString(s);
        if(ret != OK) return;
        syms_.emplace_back(s);
    }
}

SymbolBase::SymbolBase(int id, const DataInputStreamSP& in, IO_ERR& ret){
    id_ = id;
    int size;
    ret =  in->readInt(size);
    if(ret != OK)
        return;
    for(int i = 0; i < size; i++){
        string s;
        ret = in->readString(s);
        if(ret != OK)
            return;
        syms_.emplace_back(s);
    }
}

int SymbolBase::serialize(char* buf, int bufSize, INDEX indexStart, int offset, int& numElement, int& partial) const {
    if(indexStart >= (INDEX)syms_.size())
        return -1;
    int index = indexStart;
    int initSize = bufSize;
    while(index < (int)syms_.size() && bufSize > 0){
        int size = std::min(bufSize, (int)syms_[index].size() + 1 - offset);
        memcpy(buf, syms_[index].data() + offset,  size);
        buf += size;
        bufSize -= size;
        offset += size;
        if(offset == (int)syms_[index].size() + 1){
            offset = 0;
            index ++;
        }
    }
    partial = offset;
    numElement = index - indexStart;
    return initSize - bufSize;
}

int SymbolBase::find(const string& symbol){
    if(symMap_.empty()){
        if(syms_.size() > 0 && syms_[0] != "")
            throw RuntimeException("A symbol base's first key must be empty string.");
        if(syms_.size() == 0){
            symMap_[""] = 0;
            syms_.emplace_back("");
        }	
        else {
            int count = syms_.size();
            for(int i = 0; i < count; ++i)
                symMap_[syms_[i]] = i;
        }
    }
    int index = -1;
    auto it = symMap_.find(symbol);
    if(it != symMap_.end()) index = it->second;
    return index;
}

int SymbolBase::findAndInsert(const string& symbol){
	//remove following line to support empty symbol
    //if(symbol == "")
    //    throw RuntimeException("A symbol base key string can't be null.");
    if(symMap_.empty()){
        if(syms_.size() > 0 && syms_[0] != "")
            throw RuntimeException("A symbol base's first key must be empty string.");
        if(syms_.size() == 0){
            symMap_[""] = 0;
            syms_.emplace_back("");
        }	
        else {
            int count = syms_.size();
            for(int i = 0; i < count; ++i)
                symMap_[syms_[i]] = i;
        }
    }
    int index = -1;
    auto it = symMap_.find(symbol);
    if(it == symMap_.end()){
        index = symMap_.size();
        if(index >= SYMBOLBASE_MAX_SIZE){
            throw RuntimeException("One symbol base's size can't exceed 2097152.");
        }
        symMap_[symbol] = index;
        syms_.emplace_back(symbol);
    }
    else{
        index = it->second;
    }
    return index;
}

DLogger::Level DLogger::minLevel_ = DLogger::LevelDebug;
std::string DLogger::levelText_[] = { "Debug","Info","Warn","Error" };
//std::string DLogger::logFilePath_="/tmp/ddb_python_api.log";
std::string DLogger::logFilePath_;
void DLogger::SetMinLevel(Level level) {
	minLevel_ = level;
}

void DLogger::WriteLog(std::string &text){
    puts(text.data());
    if(logFilePath_.empty()==false){
        text+="\n";
        Util::writeFile(logFilePath_.data(),text.data(),text.length());
    }
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
				maxNs = one;
			}
			if (minNs == 0 || minNs > one) {
				minNs = one;
			}
		}
		double sum = sumNsOverflow * (LLONG_MAX / ns2s) + sumNs / ns2s;
		double min = minNs / ns2s;
		double max = maxNs / ns2s;
		output = output + node->name + ": sum = " + std::to_string(sum) + " count = " + std::to_string(node->costTime.size()) +
			" avg = " + std::to_string(sum / node->costTime.size()) +
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

};    // namespace dolphindb

namespace std {
size_t hash<dolphindb::Guid>::operator()(const dolphindb::Guid & val) const{
    return murmur32_16b(val.bytes());
}
}
