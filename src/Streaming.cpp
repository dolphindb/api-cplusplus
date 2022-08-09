#include "Streaming.h"
#include "Concurrent.h"
#include "ConstantMarshall.h"
#include "Exceptions.h"
#include "Util.h"
#include "TableImp.h"
#include "ScalarImp.h"

#ifndef WINDOWS
#include <arpa/inet.h>
#endif

#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(x) (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif

#ifdef WINDOWS
namespace {
bool WSAStarted = false;

int startWSA() {
    WORD wVersionRequested;
    WSADATA wsaData;
    int err;
    wVersionRequested = MAKEWORD(2, 2);
    err = WSAStartup(wVersionRequested, &wsaData);
    if (err != 0) {
        printf("WSAStartup failed with error: %d\n", err);
        return 1;
    }
    if (LOBYTE(wsaData.wVersion) != 2 || HIBYTE(wsaData.wVersion) != 2) {
        printf("Could not find a usable version of Winsock.dll\n");
        WSACleanup();
        return 1;
    } else
        printf("The Winsock 2.2 dll was found okay\n");
    return 0;
}
}  // namespace
#endif

using std::cerr;
using std::cout;
using std::endl;

constexpr int DEFAULT_QUEUE_CAPACITY = 65536;
class Executor : public dolphindb::Runnable {
    using Func = std::function<void()>;

public:
    explicit Executor(Func f) : func_(std::move(f)){};
    void run() override { func_(); };

private:
    Func func_;
};

namespace dolphindb {

template <typename T>
ConstantSP arg(SmartPointer<T> v) {
	if (UNLIKELY(v.isNull())) {
		static ConstantSP void_ = Util::createConstant(DT_VOID);
		return void_;
	}
	return ConstantSP(v);
}

template <typename T>
ConstantSP arg(T v);

template <>
ConstantSP arg(bool v) {
	return Util::createBool((char)v);
}

template <>
ConstantSP arg(char v) {
	return Util::createChar(v);
}

template <>
ConstantSP arg(short v) {
	return Util::createShort(v);
}

template <>
ConstantSP arg(int v) {
	return Util::createInt(v);
}

template <>
ConstantSP arg(long long v) {
	return Util::createLong(v);
}

template <>
ConstantSP arg(float v) {
	return Util::createFloat(v);
}

template <>
ConstantSP arg(double v) {
	return Util::createDouble(v);
}

template <>
ConstantSP arg(string v) {
	return Util::createString(v);
}

template <>
ConstantSP arg(const char *c) {
	return arg(string(c));
}

template <typename T>
vector<ConstantSP> argVec(T &&v) {
	return{ arg(std::forward<T>(v)) };
}

template <typename T, typename... Args>
vector<ConstantSP> argVec(T &&first, Args &&... oth) {
	auto ret = argVec(std::forward<Args>(oth)...);
	ret.emplace_back(arg(std::forward<T>(first)));
	return ret;
}

template <typename... Args>
ConstantSP run(DBConnection &conn, const string &func, Args &&... args) {
    auto v = argVec(std::forward<Args>(args)...);
    std::reverse(v.begin(), v.end());
    return conn.run(func, v);
}

template <typename Key, typename T, typename Hash = std::hash<Key>>
class Hashmap {
public:
    Hashmap() = default;
	Hashmap(unordered_map<Key, T> &src) : mp_(src) {}
    size_t count(const Key &key) {
        LockGuard<Mutex> _(&mtx_);
        return mp_.count(key);
    }
    bool find(const Key &key, T &t) {
        LockGuard<Mutex> _(&mtx_);
        auto kv = mp_.find(key);
        if (kv == mp_.end()) {
            return false;
        } else {
            t = kv->second;
            return true;
        }
    }
    bool findWithExternalLock(const Key &key, T &t) {
        auto kv = mp_.find(key);
        if (kv == mp_.end()) {
            return false;
        } else {
            t = kv->second;
            return true;
        }
    }
    void op(std::function<void(unordered_map<Key, T, Hash> &mp)> func) {
        LockGuard<Mutex> _(&mtx_);
        func(mp_);
    }
    void upsert(const Key &key, std::function<void(T &v)> processor, const T &default_value) {
        LockGuard<Mutex> _(&mtx_);
        auto kv = mp_.find(key);
        if (kv != mp_.end()) {
            processor(mp_[key]);
        } else
            mp_[key] = default_value;
    }
    void insert(const Key &key, const T &val) {
        LockGuard<Mutex> _(&mtx_);
        mp_.insert({key, val});
    }
    void erase(const Key &key) {
        LockGuard<Mutex> _(&mtx_);
        mp_.erase(key);
    }
    void erase_if_eq(const Key &key, const T &val) {
        LockGuard<Mutex> _(&mtx_);
        auto kv = mp_.find(key);
        if (kv != mp_.end() && kv->second == val) {
            mp_.erase(key);
        }
    }
    void erase_if_eq(const Key &key, const T &val, std::function<void()> &&extra) {
        LockGuard<Mutex> _(&mtx_);
        auto kv = mp_.find(key);
        if (kv != mp_.end() && kv->second == val) {
            mp_.erase(key);
            extra();
        }
    }
    vector<pair<Key, T>> getElements() {
        LockGuard<Mutex> _(&mtx_);
        vector<pair<Key, T>> ret;
        for (auto kv = mp_.begin(); kv != mp_.end(); ++kv) {
            ret.emplace_back(*kv);
        }
        return ret;
    }
    T &operator[](const Key &key) {
        LockGuard<Mutex> _(&mtx_);
        return mp_[key];
    }
    Mutex *getLock() { return &mtx_; }
    int size() { return mp_.size(); }

private:
    unordered_map<Key, T, Hash> mp_;
    Mutex mtx_;
};

ConstantSP convertTupleToTable(const vector<string>& colLabels, const ConstantSP& msg) {
    int colCount = colLabels.size();
    vector<ConstantSP> cols(colCount);
    for(int i=0; i<colCount; ++i){
        cols[i] = msg->get(i);
        cols[i]->setTemporary(true);
    }
    return new BasicTable(cols, colLabels);
}

bool mergeTable(const Message &dest, const vector<Message> &src) {
	Table *table = (Table*)dest.get();
	INDEX colSize = table->columns();
	for(auto &one : src) {
		if (one.isNull())
			return false;
		Table *src = (Table*)one.get();
		for (INDEX colIndex = 0; colIndex < colSize; colIndex++) {
			((Vector*)table->getColumn(colIndex).get())->append(src->getColumn(colIndex));
		}
	}
	return true;
}

}  // namespace dolphindb

namespace dolphindb {

StreamDeserializer::StreamDeserializer(const unordered_map<string, pair<string, string>> &sym2tableName, DBConnection *pconn)
					: sym2tableName_(sym2tableName) {
	if (pconn != NULL) {
		create(*pconn);
	}
}

StreamDeserializer::StreamDeserializer(const unordered_map<string, DictionarySP> &sym2schema) {
	parseSchema(sym2schema);
}

StreamDeserializer::StreamDeserializer(const unordered_map<string, vector<DATA_TYPE>> &symbol2col)
					:symbol2col_(symbol2col){

}

void StreamDeserializer::create(DBConnection &conn) {
	if (symbol2col_.size() > 0 || sym2tableName_.empty())
		return;
	unordered_map<string, DictionarySP> sym2schema;
	DictionarySP schema;
	for (auto &one : sym2tableName_) {
		if (one.second.first.empty()) {
			schema = conn.run("schema(" + one.second.second + ")");
		}
		else {
			schema = conn.run(std::string("schema(loadTable(\"") + one.second.first + "\",\"" + one.second.second + "\"))");
		}
		sym2schema[one.first] = schema;
	}
	parseSchema(sym2schema);
}
bool StreamDeserializer::parseBlob(const ConstantSP &src, vector<VectorSP> &rows, vector<string> &symbols, ErrorCodeInfo &errorInfo) {
	const VectorSP &symbolVec = src->get(1);
	const VectorSP &blobVec = src->get(2);
	INDEX rowSize = symbolVec->rows();
	rows.resize(rowSize);
	symbols.resize(rowSize);
	unordered_map<string, vector<DATA_TYPE>>::iterator iter;
	for (INDEX rowIndex = 0; rowIndex < rowSize; rowIndex++) {
		string symbol = symbolVec->getString(rowIndex);
		{
			LockGuard<Mutex> lock(&mutex_);
			iter = symbol2col_.find(symbol);
			if (iter == symbol2col_.end()) {
				errorInfo.set(ErrorCodeInfo::EC_InvalidParameter, string("Unknow symbol ") + symbol);
				return false;
			}
		}
		symbols[rowIndex] = std::move(symbol);
		vector<DATA_TYPE> &cols = iter->second;
		const string &blob = blobVec->getStringRef(rowIndex);
		DataInputStreamSP dis = new DataInputStream(blob.data(), blob.size(), false);
		INDEX num;
		IO_ERR ioError;
		ConstantSP value;
		int colIndex = 0;
		VectorSP rowVec = Util::createVector(DT_ANY, cols.size());
		for (auto &colOne : cols) {
			if (colOne < ARRAY_TYPE_BASE) {
				value = Util::createConstant(colOne);
				ioError = value->deserialize(dis.get(), 0, 1, num);
				if (ioError != OK) {
					errorInfo.set(ErrorCodeInfo::EC_InvalidObject, "Deserialize blob error " + std::to_string(ioError));
					return false;
				}
				rowVec->set(colIndex, value);
			}
			else {
				value = Util::createConstant(DATA_TYPE(colOne - ARRAY_TYPE_BASE));
				ioError = value->deserialize(dis.get(), 0, 1, num);
				if (ioError != OK) {
					errorInfo.set(ErrorCodeInfo::EC_InvalidObject, "Deserialize blob error " + std::to_string(ioError));
					return false;
				}
				VectorSP anyVector = Util::createVector(DT_ANY, 1);
				anyVector->set(0, value);
				rowVec->set(colIndex, anyVector);
			}
			colIndex++;
		}
		rows[rowIndex] = rowVec;
	}
	return true;
}
void StreamDeserializer::parseSchema(const unordered_map<string, DictionarySP> &sym2schema) {
	for (auto &one : sym2schema) {
		const DictionarySP &schema = one.second;
		TableSP colDefs = schema->getMember("colDefs");
		ConstantSP colDefsTypeInt = colDefs->getColumn("typeInt");
		ConstantSP colDefsTypeString = colDefs->getColumn("typeString");
		size_t columnSize = colDefs->size();
			
		vector<DATA_TYPE> colTypes;
		//tableInfo.colNames.resize(columnSize);
		colTypes.resize(columnSize);
		for (size_t i = 0; i < columnSize; i++) {
			colTypes[i] = (DATA_TYPE)colDefsTypeInt->getInt(i);
			//tableInfo.colNames[i] = colDefsTypeString->getString(i);
		}
		LockGuard<Mutex> lock(&mutex_);
		symbol2col_[one.first] = colTypes;
	}
}

class StreamingClientImpl {
    struct SubscribeInfo {
        SubscribeInfo()
            : host("INVAILD"),
              port(-1),
              tableName("INVALID"),
              actionName("INVALID"),
              offset(-1),
              resub(false),
              filter(nullptr),
              msgAsTable(false),
              allowExists(false),
              haSites(0),
              queue(nullptr),
			  userName(""),
			  password(""),
			  streamDeserializer(nullptr) {}
        explicit SubscribeInfo(const string &host, int port, const string &tableName, const string &actionName, long long offset, bool resub,
                               const VectorSP &filter, bool msgAsTable, bool allowExists, int batchSize,
								const string &userName, const string &password, const StreamDeserializerSP &blobDeserializer)
            : host(move(host)),
              port(port),
              tableName(move(tableName)),
              actionName(move(actionName)),
              offset(offset),
              resub(resub),
              filter(filter),
              msgAsTable(msgAsTable),
              allowExists(allowExists),
              attributes(),
              haSites(0),
              queue(new MessageQueue(std::max(DEFAULT_QUEUE_CAPACITY, batchSize), batchSize)),
			  userName(move(userName)),
			  password(move(password)),
			  streamDeserializer(blobDeserializer){
		}

        string host;
        int port;
        string tableName;
        string actionName;
        long long offset;
        bool resub;
        VectorSP filter;
        bool msgAsTable;
        bool allowExists;
        vector<string> attributes;
        vector<pair<string, int>> haSites;
		StreamDeserializerSP streamDeserializer;
		string userName, password;

        MessageQueueSP queue;
		vector<ThreadSP> handleThread;
		void setExitFlag() {
			queue->push(Message());
		}
		void exit() {
			queue->push(Message());
			for (auto &one : handleThread) {
				one->join();
			}
			handleThread.clear();
		}
    };
	class ActivePublisher {
	public:
		ActivePublisher(const string &ip, int port, vector<ConstantSP> &publishArgs) : ip_(ip), port_(port), publishArgs_(publishArgs) {}
		~ActivePublisher() { stop(); }
		IO_ERR start();
		IO_ERR ack();
		IO_ERR stop();
		SocketSP getSocket() { return socket_; }
	private:
		static short encodeFlag(int high, int low) {
			return (short)((high & 0xff) << 8 | (low & 0xff));
		}
		IO_ERR startWrite(DataOutputStreamSP &output, int type, int form, int argsize);
		enum SubscriberRPCType { RPC_OK, RPC_START, RPC_END };
		enum SubscriberFromType { FROM_DDB, FROM_API };
		SocketSP socket_;
		string ip_;
		int port_;
		vector<ConstantSP> publishArgs_;
	};
	typedef SmartPointer<ActivePublisher> ActivePublisherSP;
	struct SocketThread {
		SocketThread() {}
		SocketThread(const SocketThread &src) {
			thread = src.thread;
			socket = src.socket;
			publisher = src.publisher;
		}
		SocketThread(const SocketSP &socket1, const ThreadSP &thread1,const ActivePublisherSP &publisher1) {
			thread = thread1;
			socket = socket1;
			publisher = publisher1;
		}
		SocketThread& operator =(const SocketThread& src) {
			thread = src.thread;
			socket = src.socket;
			publisher = src.publisher;
			return *this;
		}
		void stop() {
			if (publisher.isNull() == false) {
				publisher->stop();
			}
			if (socket.isNull() == false) {
				socket->close();
			}
		}
		ThreadSP thread;
		SocketSP socket;
		ActivePublisherSP publisher;
	};
	
    struct KeepAliveAttr {
        int enabled = 1;             // default = 1 enabled
        int idleTime = 30;           // default = 30s idle time will trigger detection
        int interval = 5;            // default = 5s/detection
        int count = 3;               // default = 3 unsuccessful detections mean disconnected
    };

public:
    explicit StreamingClientImpl(int listeningPort) : listeningPort_(listeningPort), publishers_(5){
		if (listeningPort_ <= 0) {
			throw RuntimeException("Invalid listening port value " + std::to_string(listeningPort));
		}
#ifdef WINDOWS
        if (!WSAStarted && startWSA()) {
            throw RuntimeException("Can't start WSA");
        }
        WSAStarted = true;
#endif
      if (isListenMode()) {
			listenerSocket_ = new Socket("", listeningPort, true, 30);
			if (listenerSocket_->bind() != OK) {
				throw RuntimeException("Failed to bind the socket on port " + Util::convert(listeningPort_) +
					". Couldn't start the subscription daemon.");
			}
			if (listenerSocket_->listen() != OK) {
				throw RuntimeException("Failed to listen the socket on port " + Util::convert(listeningPort_) +
					". Couldn't start the subscription daemon.");
			}
		}
		exit_ = false;
        reconnectThread_ = new Thread(new Executor(std::bind(&StreamingClientImpl::reconnect, this)));
        reconnectThread_->start();
        daemonThread_ = new Thread(new Executor(std::bind(&StreamingClientImpl::daemon, this)));
        daemonThread_->start();
    }
    ~StreamingClientImpl() {
		exit();
    }
	void exit() {
		if (exit_)
			return;
		DLOG("exit start.");
		exit_ = true;
		if(listenerSocket_.isNull()==false)
			listenerSocket_->close();
		if(reconnectThread_.isNull()==false)
			reconnectThread_->join();
		if(daemonThread_.isNull()==false)
			daemonThread_->join();
		{
			SocketThread socketthread;
			while (parseSocketThread_.pop(socketthread)) {
				socketthread.stop();
				socketthread.thread->join();
			}
		}
		topicSubInfos_.op([&](unordered_map<string, SubscribeInfo>& mp) {
			for (auto &one : mp) {
				one.second.exit();
			}
		});
		ActivePublisherSP publisher;
		while (publishers_.poll(publisher, 0)) {
			publisher->stop();
		}
		DLOG("exit done.");
	}
	inline bool isExit() {
		return exit_;
	}
	MessageQueueSP subscribeInternal(const string &host, int port, const string &tableName,
                                     const string &actionName = DEFAULT_ACTION_NAME, int64_t offset = -1,
                                     bool resubscribe = true, const VectorSP &filter = nullptr, bool msgAsTable = false,
                                     bool allowExists = false, int batchSize  = 1,
									const string &userName="", const string &password="",
									const StreamDeserializerSP &sdsp = nullptr);
    string subscribeInternal(DBConnection &conn, SubscribeInfo &info);
    void insertMeta(SubscribeInfo &info, const string &topic);
    bool delMeta(const string &topic, bool exitFlag);
    void unsubscribeInternal(const string &host, int port, const string &tableName,
                             const string &actionName = DEFAULT_ACTION_NAME);
	void addHandleThread(const MessageQueueSP& queue, const ThreadSP &thread) {
		topicSubInfos_.op([&](unordered_map<string, SubscribeInfo>& mp) {
			for (auto &one : mp) {
				if (one.second.queue == queue) {
					one.second.handleThread.push_back(thread);
					return;
				}
			}
			DLogger::Error("can't find message queue in exist topic.");
		});
	}
	long getQueueDepth(const ThreadSP &thread) {
		MessageQueueSP queue=findMessageQueue(thread);
		if (queue.isNull()) {
			return 0;
		}
		return queue->size();
	}
	MessageQueueSP findMessageQueue(const ThreadSP &thread) {
		MessageQueueSP queue;
		topicSubInfos_.op([&](unordered_map<string, SubscribeInfo>& mp) {
			for (auto &one : mp) {
				for(auto &handle : one.second.handleThread) {
					if (handle == thread) {
						queue = one.second.queue;
						return;
					}
				}
			}
		});
		return queue;
	}
private:
	bool isListenMode() {
		return listeningPort_ > 0;
	}
    void parseMessage(SocketSP socket, ActivePublisherSP publisher);
    void reconnect();
	static bool initSocket(const SocketSP &socket) {
		if (socket.isNull())
			return false;
		static KeepAliveAttr keepAliveAttr;
#ifdef WINDOWS
		if (::setsockopt(socket->getHandle(), SOL_SOCKET, SO_KEEPALIVE, (const char*)&keepAliveAttr.enabled, sizeof(int)) != 0) {
			cerr << "Subscription socket failed to enable TCP_KEEPALIVE with error: " << errno << endl;
			return false;
		}
#elif defined MAC
        if(::setsockopt(socket->getHandle(), SOL_SOCKET, SO_KEEPALIVE, (const char*)&keepAliveAttr.enabled, sizeof(int)) != 0) {
            cerr << "Subscription socket failed to enable TCP_KEEPALIVE with error: " <<  errno << endl;
            return false;
        }
#else
		if (::setsockopt(socket->getHandle(), SOL_SOCKET, SO_KEEPALIVE, &keepAliveAttr.enabled, sizeof(int)) != 0) {
			cerr << "Subscription socket failed to enable TCP_KEEPALIVE with error: " << errno << endl;
		}
		::setsockopt(socket->getHandle(), SOL_TCP, TCP_KEEPIDLE, &keepAliveAttr.idleTime, sizeof(int));
		::setsockopt(socket->getHandle(), SOL_TCP, TCP_KEEPINTVL, &keepAliveAttr.interval, sizeof(int));
		::setsockopt(socket->getHandle(), SOL_TCP, TCP_KEEPCNT, &keepAliveAttr.count, sizeof(int));
#endif
		return true;
	}

private:
    void daemon() {
		DLOG("daemon start.");
		SocketSP socket;
		ActivePublisherSP publisher;
		IO_ERR ret;
        while (isExit() == false) {
            try {
				if (isListenMode()) {
					socket = listenerSocket_->accept();
					if (!initSocket(socket))
						break;
				}
				else {
					publishers_.pop(publisher);
					if (publisher.isNull())
						break;
					socket = publisher->getSocket();
				}
                if (socket.isNull()) {
                    //cerr << "Streaming Daemon socket accept failed, aborting." << endl;
                    break;
                };

                ThreadSP t = new Thread(new Executor(std::bind(&StreamingClientImpl::parseMessage, this, socket, publisher)));
                t->start();
				parseSocketThread_.push(SocketThread(socket,t,publisher));
            } catch (exception &e) {
                cerr << "Daemon exception: " << e.what() << endl;
                cerr << "Restart Daemon in 1 second" << endl;
                Util::sleep(1000);
            } catch (...) {
                cerr << "Daemon unknown exception: " << endl;
                cerr << "Restart Daemon in 1 second" << endl;
                Util::sleep(1000);
            }
        }
		DLOG("daemon exit.");
    }
	string getLocalHostname(string remoteHost, int remotePort) {
        int attempt = 0;
        while (isExit()==false) {
            try {
#ifndef WINDOWS
                char myIP[16];
                struct sockaddr_in server_addr, my_addr;
                int sockfd;

                if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
                    throw RuntimeException("Error in getLocalHostName: Can't open stream socket.");
                }

                bzero(&server_addr, sizeof(server_addr));
                memset(&server_addr, 0, sizeof(server_addr));
                server_addr.sin_family = AF_INET;
                server_addr.sin_addr.s_addr = inet_addr(remoteHost.c_str());
                server_addr.sin_port = htons(remotePort);

                if (connect(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
                    throw RuntimeException("Error in getLocalHostName: can't connect to server.");
                }

                bzero(&my_addr, sizeof(my_addr));
                socklen_t len = sizeof(my_addr);
                getsockname(sockfd, (struct sockaddr *)&my_addr, &len);
                inet_ntop(AF_INET, &my_addr.sin_addr, myIP, sizeof(myIP));

                close(sockfd);
                return string(myIP);
#else
                SOCKET SendingSocket;
                SOCKADDR_IN ServerAddr, ThisSenderInfo;
                if ((SendingSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) == INVALID_SOCKET) {
                    throw RuntimeException("Client: socket() failed!");
                }

                ServerAddr.sin_family = AF_INET;
                ServerAddr.sin_port = htons(remotePort);
                ServerAddr.sin_addr.s_addr = inet_addr(remoteHost.c_str());
                if (connect(SendingSocket, (SOCKADDR *)&ServerAddr, sizeof(ServerAddr)) != 0) {
                    throw RuntimeException("Client: connect() failed!");
                }

                memset(&ThisSenderInfo, 0, sizeof(ThisSenderInfo));
                int nlen = sizeof(ThisSenderInfo);
                if (getsockname(SendingSocket, (SOCKADDR *)&ThisSenderInfo, &nlen) != 0) {
                    throw RuntimeException("ERROR in getsockname with error code: " +
                                           std::to_string(WSAGetLastError()));
                }
                shutdown(SendingSocket, SD_SEND);
                closesocket(SendingSocket);
                return string(inet_ntoa(ThisSenderInfo.sin_addr));
#endif
            } catch (RuntimeException &e) {
                if (attempt++ == 10) throw;
                cerr << "getLocalHostName #attempt=" << attempt << " come across an error: " << e.what()
                     << ", Will retry in 1 second." << endl;
                Util::sleep(1000);
            }
        }
    }
    inline string stripActionName(string topic) { return topic.substr(0, topic.find_last_of('/')); }
    inline string getSite(string topic) { return topic.substr(0, topic.find_first_of('/')); }
    DBConnection buildConn(string host, int port) {
        DBConnection conn;
        if (!conn.connect(host, port)) {
            throw RuntimeException("Failed to connect to server: " + host + " " + std::to_string(port));
        }

        return conn;
    }

    string getLocalIP() {
        if (localIP_.empty()) localIP_ = "localhost";
        return localIP_;
    }

    bool getNewLeader(const string& s, string &host, int &port) {
        string msg{s};
		int index = msg.find("<NotLeader>");
		if (index != string::npos) {
			index = msg.find(">");
			string ipport = msg.substr(index + 1);

			auto v = Util::split(ipport, ':');
			if (v.size() < 2) {
				return false;
			}
			host = v[0];
			port = std::stoi(v[1]);
			if (port <= 0 || port > 65535) {
				return false;
			}
			return true;
		}
		return false;
    }

private:
    SocketSP listenerSocket_;
    ThreadSP daemonThread_;
    ThreadSP reconnectThread_;
    SynchronizedQueue<SocketThread> parseSocketThread_;
	int listeningPort_;
    string localIP_;
    Hashmap<string, SubscribeInfo> topicSubInfos_;
    Hashmap<string, int> actionCntOnTable_;
    Hashmap<string, set<string>> liveSubsOnSite_;  // living site -> topic
    Hashmap<string, pair<long long, long long>> siteReconn_;
    Hashmap<string, pair<long long, long long>> topicReconn_;
	BlockingQueue<ActivePublisherSP> publishers_;
    Mutex mtx_;
    std::queue<SubscribeInfo> initResub_;
	bool exit_;
#ifdef WINDOWS
    static bool WSAStarted_;
    static void WSAStart();
#endif
};

IO_ERR StreamingClientImpl::ActivePublisher::startWrite(DataOutputStreamSP &output, int type, int from, int argsize) {
	if (socket_.isNull() || socket_->isValid() == false)
		return IO_ERR::DISCONNECTED;
	char outbuff[256];
	int offset = 0;
	{
		short flag = encodeFlag(type, from);
		memcpy(outbuff + offset, &flag, sizeof(flag));
		offset += sizeof(flag);
		char islittleendian = (char)Util::isLittleEndian();
		memcpy(outbuff + offset, &islittleendian, sizeof(islittleendian));
		offset += sizeof(islittleendian);
		int argsize = publishArgs_.size();
		memcpy(outbuff + offset, &argsize, sizeof(argsize));
		offset += sizeof(argsize);
	}
	output = new DataOutputStream(socket_);
	size_t actwritelen;
	return output->write(outbuff, offset, actwritelen);
}

IO_ERR StreamingClientImpl::ActivePublisher::start() {
	if (!socket_.isNull()) {
		socket_->close();
	}
	socket_ = new Socket();
	IO_ERR ret=socket_->connect(ip_, port_, true, 30, false);
	if (ret != OK)
		return ret;
	initSocket(socket_);
	DataOutputStreamSP output;
	ret = startWrite(output, RPC_START, FROM_API, publishArgs_.size());
	if (ret != OK)
		return ret;
	ret = output->flush();
	if (ret != OK)
		return ret;
	ConstantMarshallFactory marshallFactory(output);
	for (ConstantSP one : publishArgs_) {
		if (one->containNotMarshallableObject()) {
			return IO_ERR::INVALIDDATA;
		}
		ConstantMarshallSP marshall= marshallFactory.getInstance(one->getForm(), output);
		if (!marshall->start(one, true, false, ret)) {
			return ret;
		}
		marshall->reset();
	}
	ret = output->flush();
	return ret;
}

IO_ERR StreamingClientImpl::ActivePublisher::ack() {
	if (socket_.isNull() || socket_->isValid() == false)
		return DISCONNECTED;
	DataOutputStreamSP output;
	IO_ERR ret = startWrite(output, RPC_OK, FROM_API, 0);
	if (ret != OK)
		return ret;
	ret = output->flush();
	if (ret != OK)
		return ret;
	return ret;
}
IO_ERR StreamingClientImpl::ActivePublisher::stop() {
	if (socket_.isNull() || socket_->isValid() == false)
		return DISCONNECTED;
	DataOutputStreamSP output;
	IO_ERR ret = startWrite(output, RPC_END, FROM_API, 0);
	if (ret == OK){
		ret = output->flush();
	}
	socket_->close();
	return ret;
}

void StreamingClientImpl::reconnect() {
	DLOG("reconnect start.");
    const int reconnect_timeout = 3000;  // reconn every 3s
    while (isExit()==false) {
        siteReconn_.op([&](unordered_map<string, pair<long long, long long>> &mp) {
            for (auto &p : mp) {
				if (isExit()) {
					DLOG("reconnect exit by flag.");
					return;
				}
                if (Util::getEpochTime() - p.second.first <= reconnect_timeout) continue;

                auto v = Util::split(p.first, ':');
                string host = v[0];
                int port = std::stoi(v[1]);

                try {
                    DBConnection conn(buildConn(host, port));

                    auto versionStr = conn.run("version()")->getString();
                    auto _ = Util::split(Util::split(versionStr, ' ')[0], '.');
                    auto v0 = std::stoi(_[0]);
                    auto v1 = std::stoi(_[1]);
                    auto v2 = std::stoi(_[2]);

                    if (v0 > 1 || (v1 >= 99 && v2 >= 5)) {
                        run(conn, "activeClosePublishConnection", getLocalIP(), listeningPort_, true);
                    } else {
                        run(conn, "activeClosePublishConnection", getLocalIP(), listeningPort_);
                    }

                } catch (exception &e) {
                    cerr << "#attempt= " << p.second.first << "activeClosePublishConnection on site got an exception "
                         << e.what() << ", site: " << host << ":" << port << endl;
                }
                p.second.first = Util::getEpochTime();
                ++p.second.second;
            }
        });
		if (isExit()) {
			DLOG("reconnect exit by flag.");
			return;
		}

        topicReconn_.op([&](unordered_map<string, pair<long long, long long>> &mp) {
            for (auto &p : mp) {
				if (Util::getEpochTime() - p.second.first <= reconnect_timeout) continue;
                SubscribeInfo info;
                if (!topicSubInfos_.find(p.first, info)) continue;
                if (!info.resub) continue;
//                cout << "resub offset: " << info.offset << " " << &info.offset << endl;
				string topic = p.first;
                string host = info.host;
                int port = info.port;
                string newTopic = topic;
                for (int i = 0; i < 3; ++i) {
					if (isExit()) {
						DLOG("reconnect exit by flag.");
						return;
					}
					DLOG("reconnect", host,"for", topic);
                    try {
                        auto conn = buildConn(host, port);
                        newTopic = subscribeInternal(conn, info);
                        if (newTopic != topic) {
                            delMeta(topic, false);
                            insertMeta(info, newTopic);
                        }
                        break;
                    } catch (exception &e) {
                        string msg = e.what();
                        if (getNewLeader(e.what(), host, port)) {
                            cerr << "#attempt=" << p.second.second++ << ", got NewLeaderException, new leader is "
                                 << host << ":" << port << endl;
                            info.host = host;
                            info.port = port;
                        } else {
                            cerr << "#attempt=" << p.second.second++ << ", failed to resubscribe, exception:{"
                                 << e.what() << "}";
                            if (!info.haSites.empty()) {
                                int k = rand() % info.haSites.size();
                                host = info.haSites[k].first;
                                port = info.haSites[k].second;
                                cerr << ", will retry site: " << host << ":" << port << endl;
                            } else {
                                cerr << endl;
                            }
                        }
                    }
                }

                p.second.first = Util::getEpochTime();
            }
        });
		if (isExit()) {
			DLOG("reconnect exit by flag.");
			return;
		}

        {
            LockGuard<Mutex> _(&mtx_);
            vector<SubscribeInfo> v;
            while (!initResub_.empty() && isExit()==false) {
                auto info = initResub_.front();
                initResub_.pop();
                try {
                    DBConnection conn = buildConn(info.host, info.port);
                    auto topic = subscribeInternal(conn, info);
                    insertMeta(info, topic);
                } catch (exception &e) {
                    v.emplace_back(info);
                    std::cerr << "failed to resub with exception: " << e.what() << endl;
                }
            }
			if (isExit()) {
				DLOG("reconnect exit by flag.");
				return;
			}

            for (auto &i : v) {
                initResub_.push(i);
            }
        }

        Util::sleep(1000);
    }
	DLOG("reconnect exit.");
}

void StreamingClientImpl::parseMessage(SocketSP socket, ActivePublisherSP publisher) {
	DLOG("parseMessage start.");
    DataInputStreamSP in = new DataInputStream(socket);
    auto factory = ConstantUnmarshallFactory(in);
    ConstantUnmarshall *unmarshall = nullptr;

    IO_ERR ret = OK;
    long long sentTime;
    long long offset = -1;
    short previousDataFormFlag = 0x7fff;
    // Have no idea which topic it is parsing until one message came in
    string aliasTableName;
    string topicMsg;
    vector<string> topics;
	vector<string> symbols;
	bool ackDone = false;
	
    while (isExit() == false) {
		if (ackDone == false) {
			if (publisher.isNull() == false) {
				ret = publisher->ack();
				if (ret != OK) {
					cerr << "Streaming Daemon socket ack failed" << ret << "." << endl;
					break;
				}
			}
			ackDone = true;
		}
        if (ret != OK) {  // blocking mode, ret won't be NODATA
			DLOG("parseMessage exit with error",ret);
            if (!actionCntOnTable_.count(aliasTableName) || actionCntOnTable_[aliasTableName] == 0) {
                break;
            };
            if (topicMsg.empty()) {
                cerr << "WARNING: ERROR occured before receiving first message, can't do recovery." << endl;
                break;
            }
            // close this socket, and do resub
            in->close();
            socket.clear();
			if (topics.empty())
				break;
			auto site = getSite(topics[0]);
            set<string> ts;
            if (liveSubsOnSite_.find(site, ts)) {
                siteReconn_.insert(site, {Util::getEpochTime() + 3000, 0});
                for (auto &t : ts) {
                    topicReconn_.insert(t, {Util::getEpochTime(), 0});
                }
            }
            break;
        }

        char littleEndian;
        ret = in->readChar(littleEndian);
        if (ret != OK) continue;

        ret = in->bufferBytes(16);
        if (ret != OK) continue;

		ret = in->readLong(sentTime);
		if (ret != OK) continue;
		ret = in->readLong(offset);
		if (ret != OK) continue;
//        cout << offset << endl;

        ret = in->readString(topicMsg);
		if (ret != OK) continue;
        topics = Util::split(topicMsg, ',');
		if (topics.empty()) {
			break;
		}
        aliasTableName = stripActionName(topics[0]);

        short flag;
        ret = in->readShort(flag);
        if (ret != OK) continue;

        if (UNLIKELY(flag != previousDataFormFlag)) {
            auto form = static_cast<DATA_FORM>((unsigned short)flag >> 8u);
            unmarshall = factory.getConstantUnmarshall(form);
            if (UNLIKELY(unmarshall == nullptr)) {
                cerr << "[ERROR] Invalid data from: 0x" << std::hex << flag
                     << " , unable to continue. Will stop this parseMessage thread." << endl;
                ret = OTHERERR;
                continue;
            }
            previousDataFormFlag = flag;
        }

        unmarshall->start(flag, true, ret);
        if (ret != OK) continue;

        ConstantSP obj = unmarshall->getConstant();
        if (obj->isTable()) {
            if (obj->rows() != 0) {
                cerr << "[ERROR] schema table shuold have zero rows, stopping this parse thread." << endl;
                return;
            }
            siteReconn_.erase(getSite(topics[0]));
            for (auto &t : topics) {
                topicReconn_.erase(t);
            }
        } else if (LIKELY(obj->isVector())) {
			int colSize = obj->size();
            int rowSize = obj->get(0)->size();
//            offset += rowSize;
			if (rowSize == 1) {
                // 1d array to 2d
                VectorSP newObj = Util::createVector(DT_ANY, colSize);
                for (int i = 0; i < colSize; ++i) {
                    ConstantSP val = obj->get(i);
                    VectorSP col = Util::createVector(val->getType(), 1);
                    col->set(0, val);
                    newObj->set(i, col);
                }
                obj = newObj;
            }
            vector<VectorSP> cache, rows;
			ErrorCodeInfo errorInfo;
			
            for (auto &t : topics) {
                SubscribeInfo info;
                if (topicSubInfos_.find(t, info)) {
                    if (info.queue.isNull()) continue;
					if (info.streamDeserializer.isNull()==false) {
						if (rows.empty()) {
							if (!info.streamDeserializer->parseBlob(obj, rows, symbols, errorInfo)) {
								cerr << "[ERROR] parse BLOB field failed: " << errorInfo.errorInfo << ", stopping this parse thread." << endl;
								return;
							}
						}
						for (int rowIdx = 0; rowIdx < rowSize; ++rowIdx) {
							info.queue->push(Message(rows[rowIdx], symbols[rowIdx]));
						}
					}
					else {
						if (info.msgAsTable) {
							if (info.attributes.empty()) {
								std::cerr << "table colName is empty, can not convert to table" << std::endl;
								info.queue->push(Message(obj));
							}
							else {
								info.queue->push(Message(convertTupleToTable(info.attributes, obj)));
							}
						}
						else {
							if (UNLIKELY(cache.empty())) { // split once
								cache.resize(rowSize);
								for (int rowIdx = 0; rowIdx < rowSize; ++rowIdx) {
									VectorSP tmp = Util::createVector(DT_ANY, colSize, colSize);
									for (int colIdx = 0; colIdx < colSize; ++colIdx) {
										tmp->set(colIdx, obj->get(colIdx)->get(rowIdx));
									}
									cache[rowIdx] = tmp;
								}
							}
							for (auto &one : cache) {
								info.queue->push(Message(one));
							}
						}
					}
					topicSubInfos_.op([&](unordered_map<string, SubscribeInfo>& mp){
						if(mp.count(t) != 0)
                        	mp[t].offset = offset + 1;
//                        cout << "set offset to " << offset << " add: " << &mp[t].offset << endl;
                    });
//                    topicSubInfos_.upsert(
//                        t, [&](SubscribeInfo &info) { info.offset = offset; }, SubscribeInfo());
                }
            }
        } else {
			cerr << "Message body has an invalid format. Vector is expected." << endl;
			break;
        }
    }
	if (ackDone && publisher.isNull() == false) {
		publisher->stop();
	}
	parseSocketThread_.removeItem([&](const SocketThread &socketthread) {
		return socketthread.socket == socket;
	});
	DLOG("parseMessage exit");
}

string StreamingClientImpl::subscribeInternal(DBConnection &conn, SubscribeInfo &info) {
	if (info.userName.empty() == false)
		conn.login(info.userName, info.password, true);
	ConstantSP result = run(conn, "getSubscriptionTopic", info.tableName, info.actionName);
	auto topic = result->get(0)->getString();
	ConstantSP colLabels = result->get(1);
	if (!colLabels->isArray()) throw RuntimeException("The publisher doesn't have the table [" + info.tableName + "].");

	if (info.streamDeserializer.isNull() == false) {
		info.streamDeserializer->create(conn);
	}

	int colCount = colLabels->size();
	vector<string> colNames;
	colNames.reserve(colCount);
	for (int i = 0; i < colCount; ++i) colNames.push_back(colLabels->getString(i));
	info.attributes = colNames;

	if (isListenMode() == false) {
		VectorSP addressPort = conn.run("getPubAddress()");
		if (addressPort.isNull()) {
			throw RuntimeException("Server doesn't config pubPort parameter.");
		}
		//const string &ip, int port, vector<ConstantSP> &publishArgs
		vector<ConstantSP> args;
		{
			VectorSP userpassword = Util::createVector(DT_STRING, 0, 2);
			userpassword->appendString(&info.userName, 1);
			userpassword->appendString(&info.password, 1);
			args = argVec(getLocalIP(), listeningPort_, info.tableName, info.actionName,
				info.offset, info.filter, info.allowExists);
			args.insert(args.begin(), userpassword);
		}
		string ip = addressPort->getString(0);
		int port = addressPort->getInt(1);
		ActivePublisherSP publisher = new ActivePublisher(ip, port, args);
		IO_ERR ret = publisher->start();
		if (ret != OK) {
			throw RuntimeException("Subscribe stream " + ip + ":" + std::to_string(port) + " failed, error code " + std::to_string(ret));
		}
		publishers_.push(publisher);
	}
	else {
		ConstantSP re;
		if (info.userName.empty()) {
			re = run(conn, "publishTable", getLocalIP(), listeningPort_, info.tableName, info.actionName,
				info.offset, info.filter, info.allowExists);
		}
		else {
			conn.login(info.userName, info.password, true);
			re = run(conn, "publishTable", getLocalIP(), listeningPort_, info.tableName, info.actionName,
				info.offset, info.filter, info.allowExists);
		}

		if (re->isVector() && re->getType() == DT_ANY) {
			info.haSites.clear();
			auto vec = re->get(1);
			for (int i = 0; i < vec->size(); ++i) {
				auto s = vec->get(i)->getString();
				auto p = Util::split(s, ':');
				//            cerr << p[0] << ":" << p[1] << endl;
				info.haSites.emplace_back(p[0], std::stoi(p[1]));
			}
		}
	}
	return topic;
}

void StreamingClientImpl::insertMeta(SubscribeInfo &info, const string &topic) {
    if (!info.haSites.empty()) info.resub = true;
    topicSubInfos_.upsert(
        topic, [&](SubscribeInfo &_info) { _info = info; }, info);
    liveSubsOnSite_.upsert(getSite(topic), [&](set<string> &s) { s.insert(topic); }, {topic});
    actionCntOnTable_.upsert(
        stripActionName(topic), [&](int &cnt) { ++cnt; }, 1);
}

bool StreamingClientImpl::delMeta(const string &topic, bool exitFlag) {
	SubscribeInfo oldinfo;
	topicSubInfos_.op([&](unordered_map<string, SubscribeInfo> &mp) {
		if (topicSubInfos_.count(topic) < 1){
			cerr << "[WARN] subscription of topic " << topic << " not existed" << endl;
			return false;
		}
		oldinfo=mp[topic];
		mp.erase(topic);
	});
    liveSubsOnSite_.upsert(getSite(topic), [&](set<string> &s) { s.erase(topic); }, {});
    actionCntOnTable_.upsert(
        stripActionName(topic), [&](int &cnt) { --cnt; }, 0);
	if(exitFlag)
		oldinfo.exit();
	return true;
}

MessageQueueSP StreamingClientImpl::subscribeInternal(const string &host, int port, const string &tableName,
                                                      const string &actionName, int64_t offset, bool resubscribe,
                                                      const VectorSP &filter, bool msgAsTable, bool allowExists, int batchSize,
													  const string &userName, const string &password,
													  const StreamDeserializerSP &blobDeserializer) {
	if (msgAsTable && !blobDeserializer.isNull()) {
		throw RuntimeException("msgAsTable must be false when StreamDeserializer is set.");
	}
    string topic;
    int attempt = 0;
    string _host = host;
    int _port = port;
    while (isExit()==false) {
        ++attempt;
        SubscribeInfo info(_host, _port, tableName, actionName, offset, resubscribe, filter, msgAsTable, allowExists,
			batchSize, userName,password,blobDeserializer);
        try {
            DBConnection conn = buildConn(_host, _port);
			topic = subscribeInternal(conn, info);
			insertMeta(info, topic);
            return info.queue;
        } catch (exception &e) {
            if (attempt <= 10 && getNewLeader(e.what(), _host, _port)) {
                cerr << "Got NotLeaderException, new leader is " << _host << ":" << _port << " #attempt=" << attempt
                     << endl;
            } else if (resubscribe) {
                LockGuard<Mutex> _(&mtx_);
                initResub_.push(info);
                insertMeta(info, topic);
                return info.queue;
            } else {
                throw;
            }
        }
    }
	return nullptr;
}

void StreamingClientImpl::unsubscribeInternal(const string &host, int port, const string &tableName,
                                              const string &actionName) {
	DBConnection conn = buildConn(host, port);
    string topic = run(conn, "getSubscriptionTopic", tableName, actionName)->get(0)->getString();
    if (!topicSubInfos_.count(topic)) {
        cerr << "[WARN] subscription of topic " << topic << " not existed" << endl;
        return;
    }
	delMeta(topic, true);
    run(conn, "stopPublishTable", getLocalIP(), listeningPort_, tableName, actionName);
}

StreamingClient::StreamingClient(int listeningPort) : impl_(new StreamingClientImpl(listeningPort)) {}

StreamingClient::~StreamingClient() {
	exit();
}

void StreamingClient::exit() {
	impl_->exit();
}

bool StreamingClient::isExit() {
	return impl_->isExit();
}

MessageQueueSP StreamingClient::subscribeInternal(string host, int port, string tableName, string actionName,
                                                  int64_t offset, bool resubscribe, const dolphindb::VectorSP &filter,
                                                  bool msgAsTable, bool allowExists, int batchSize,
												  string userName, string password,
												  const StreamDeserializerSP &blobDeserializer) {
    return impl_->subscribeInternal(host, port, tableName, actionName, offset, resubscribe, filter, msgAsTable,
                                    allowExists, batchSize,
									userName,password,
									blobDeserializer);
}

void StreamingClient::unsubscribeInternal(string host, int port, string tableName, string actionName) {
    impl_->unsubscribeInternal(std::move(host), port, std::move(tableName), std::move(actionName));
}

/// ThreadedClient impl
ThreadedClient::ThreadedClient(int listeningPort) : StreamingClient(listeningPort) {}

size_t ThreadedClient::getQueueDepth(const ThreadSP &thread) {
	return impl_->getQueueDepth(thread);
}

ThreadSP ThreadedClient::subscribe(string host, int port, const MessageBatchHandler &handler, string tableName,
                                   string actionName, int64_t offset, bool resub, const VectorSP &filter,
                                   bool allowExists, int batchSize, double throttle, bool msgAsTable,
									string userName, string password,
								   const StreamDeserializerSP &blobDeserializer) {
    MessageQueueSP queue = subscribeInternal(std::move(host), port, std::move(tableName), std::move(actionName), offset,
                                             resub, filter, msgAsTable, allowExists, batchSize, userName, password, blobDeserializer);
    if (queue.isNull()) {
        cerr << "Subscription already made, handler loop not created." << endl;
        ThreadSP t = new Thread(new Executor([]() {}));
        t->start();
        return t;
    }
    int throttleTime;
    if(batchSize <= 0){
        throttleTime = 0; 
    }else{
        throttleTime = std::max(1, (int)(throttle * 1000));
    }
	SmartPointer<StreamingClientImpl> impl=impl_;
	ThreadSP thread = new Thread(new Executor([handler, queue, throttleTime, impl]() {
		DLOG("handle start.");
        vector<Message> msgs;
		bool foundnull = false;
        while (impl->isExit()==false && foundnull == false) {
			if(queue->pop(msgs, throttleTime)){
				while (msgs.empty() == false && msgs.back().isNull()) {
					msgs.pop_back();
					foundnull=true;
                }
				if(!msgs.empty())
                	handler(msgs);
            }
        }
		queue->push(Message());
		DLOG("handle exit.");
    }));
	impl_->addHandleThread(queue, thread);
	thread->start();
    return thread;
}

ThreadSP newHandleThread(const MessageHandler handler, MessageQueueSP queue, bool msgAsTable, SmartPointer<StreamingClientImpl> impl) {
	ThreadSP thread = new Thread(new Executor([handler, queue, msgAsTable, impl]() {
		DLOG("nht handle start.");
		Message msg;
		vector<Message> tables;
		bool foundnull = false;
		while (foundnull == false && impl->isExit() == false) {
			queue->pop(msg);
			// quit handler loop if msg is nullptr
			if (UNLIKELY(msg.isNull())){
				foundnull = true;
				break;
			}
			if (msgAsTable && queue->pop(tables, 0)) {
				if (!mergeTable(msg, tables)) {
					foundnull = true;
				}
			}
			handler(msg);
		}
		queue->push(Message());
		DLOG("nht handle exit.");
	}));
	impl->addHandleThread(queue,thread);
	return thread;
}

ThreadSP ThreadedClient::subscribe(string host, int port, const MessageHandler &handler, string tableName,
                                   string actionName, int64_t offset, bool resub, const VectorSP &filter,
                                   bool msgAsTable, bool allowExists, 
									string userName, string password,
									const StreamDeserializerSP &blobDeserializer) {
    MessageQueueSP queue = subscribeInternal(std::move(host), port, std::move(tableName), std::move(actionName), offset,
                                             resub, filter, msgAsTable, false,1, userName, password, blobDeserializer);
    if (queue.isNull()) {
        cerr << "Subscription already made, handler loop not created." << endl;
        ThreadSP t = new Thread(new Executor([]() {}));
        t->start();
        return t;
    }

	ThreadSP t = newHandleThread(handler, queue, msgAsTable, impl_);
    t->start();
    return t;
}

void ThreadedClient::unsubscribe(string host, int port, string tableName, string actionName) {
    unsubscribeInternal(std::move(host), port, std::move(tableName), std::move(actionName));
}

/// PollingClient IMPL
PollingClient::PollingClient(int listeningPort) : StreamingClient(listeningPort) {}

MessageQueueSP PollingClient::subscribe(string host, int port, string tableName, string actionName, int64_t offset,
                                        bool resub, const VectorSP &filter, bool msgAsTable, bool allowExists,
										string userName, string password,
										const StreamDeserializerSP &blobDeserializer) {
    return subscribeInternal(std::move(host), port, std::move(tableName), std::move(actionName), offset, resub, filter,
                             msgAsTable, allowExists,1, userName, password, blobDeserializer);
}

void PollingClient::unsubscribe(string host, int port, string tableName, string actionName) {
    unsubscribeInternal(std::move(host), port, std::move(tableName), std::move(actionName));
}

/// ThreadPooledClient IMPL
ThreadPooledClient::ThreadPooledClient(int listeningPort, int threadCount)
    : StreamingClient(listeningPort), threadCount_(threadCount) {}

void ThreadPooledClient::unsubscribe(string host, int port, string tableName, string actionName) {
    unsubscribeInternal(std::move(host), port, std::move(tableName), std::move(actionName));
}

size_t ThreadPooledClient::getQueueDepth(const ThreadSP &thread){
	return impl_->getQueueDepth(thread);
}

vector<ThreadSP> ThreadPooledClient::subscribe(string host, int port, const MessageHandler &handler, string tableName,
                                               string actionName, int64_t offset, bool resub, const VectorSP &filter,
                                               bool msgAsTable, bool allowExists,
												string userName, string password,
												const StreamDeserializerSP &blobDeserializer) {
    auto queue = subscribeInternal(std::move(host), port, std::move(tableName), std::move(actionName), offset, resub,
                                   filter, msgAsTable, allowExists,1, userName,password, blobDeserializer);
    vector<ThreadSP> ret;
    for (int i = 0; i < threadCount_ && isExit() == false; ++i) {
		ThreadSP t = newHandleThread(handler, queue, msgAsTable, impl_);
        t->start();
        ret.emplace_back(t);
    }
    return ret;
}

#ifdef LINUX

IPCInMemoryStreamClient::~IPCInMemoryStreamClient() {
	for (const auto& iter : tableName2thread_) {
		ThreadSP thread = iter.second.thread;
		if (iter.second.isExit == false && !thread.isNull()) {
			thread->join();
		}
	}
}

ThreadSP IPCInMemoryStreamClient::newHandleThread(const string& tableName, const IPCInMemoryTableReadHandler handler,
	SmartPointer<IPCInMemTable> memTable,
	TableSP outputTable, bool overwrite) {
	ThreadSP thread = new Thread(new Executor([tableName, handler, memTable, this, outputTable, overwrite]() {
		while (this->tableName2thread_[tableName].isExit == false) {
			TableSP table = outputTable;
			// if (outputTable != nullptr) {
			//     table = outputTable;
			// }
			size_t readRows = 0;
			int timeout = 0;
			table = memTable->read(table, overwrite, readRows, timeout);
			if (table.isNull() || this->tableName2thread_[tableName].isExit) {
				break;
			}
			if (handler != nullptr) {
				handler(table);
			}
		}
	}));

	return thread;
}

bool checkSchema(TableSP table, TableMetaData& tableMetaData) {
	if ((unsigned int)table->columns() != tableMetaData.colTypes_.size()) {
		return false;
	}
	for (int i = 0; i < table->columns(); i++) {
		if (table->getColumnType(i) != tableMetaData.colTypes_[i]) {
			return false;
		}
	}
	return true;
}

SmartPointer<IPCInMemTable> loadIPCInMemoryTableAndCheckSchema(const string& tableName, const TableSP& outputTable, string& errMsg) {
	size_t bufSize = MIN_MEM_ROW_SIZE * sizeof(long long);
	string shmPath = SHM_KEY_HEADER_STR + tableName;
	SmartPointer<SharedMemStream> sharedMem = new SharedMemStream(false, shmPath, bufSize);
	sharedMem->parseTableMeta(false);
	auto it = sharedMem->tableMeta_.find(tableName);
	if (it == sharedMem->tableMeta_.end()) {
		errMsg = "IPCInMemTable " + tableName + " not found";
		return nullptr;
	}
	/* check schema */
	if (!outputTable.isNull() && checkSchema(outputTable, it->second) == false) {
		errMsg = "SharedStream readData, outputTable col types are not identical to table Data";
		return nullptr;
	}

	std::vector<ConstantSP> cols;
	for (unsigned int i = 0; i < it->second.colTypes_.size(); i++) {
		ConstantSP col = Util::createVector(it->second.colTypes_[i], 0);
		cols.push_back(col);
	}

	bufSize = sharedMem->getBufSize();
	bufSize = bufSize * sizeof(long long);
	SmartPointer<IPCInMemTable> memTable = new IPCInMemTable(
		false, shmPath, tableName, cols, it->second.colNames_, (size_t)bufSize);

	return memTable;
}

ThreadSP IPCInMemoryStreamClient::subscribe(const string& tableName, const IPCInMemoryTableReadHandler& handler, TableSP outputTable, bool overwrite) {
	if (tableName == "") {
		throw RuntimeException("Cannot pass an empty string");
	}
	if (tableName2thread_.find(tableName) != tableName2thread_.end()) {
		throw RuntimeException("A table can only be subscribed once at a time");
	}
	if (handler == nullptr && outputTable.isNull()) {
		throw RuntimeException("At least one of the handler and outputTable must be provided");
	}
	tableName2thread_[tableName] = { nullptr, false };
	string errMsg;
	SmartPointer<IPCInMemTable> table = loadIPCInMemoryTableAndCheckSchema(tableName, outputTable, errMsg);
	if (table.isNull()) {
		throw RuntimeException(errMsg);
	}
	ThreadSP thread = newHandleThread(tableName, handler, table, outputTable, overwrite);
	tableName2thread_[tableName].thread = thread;
	thread->start();

	return thread;
}

// // TODO 
// void IPCInMemoryStreamClient::dropIPCInMemoryTable() {
//     int bufSize = MIN_MEM_ROW_SIZE * sizeof(long long);

//     std::string shmPath = SHM_KEY_HEADER_STR + tableName_;
//     SmartPointer<SharedMemStream> sharedMem = new SharedMemStream(true, shmPath, bufSize);
//     sharedMem->closeShm();
//     sharedMem->unlinkShm();
// }

void wakeupIPCInMemoryStreamClient(const string& tableName) {
	size_t bufSize = MIN_MEM_ROW_SIZE * sizeof(long long);
	string shmPath = SHM_KEY_HEADER_STR + tableName;
	SmartPointer<SharedMemStream> sharedMem = new SharedMemStream(false, shmPath, bufSize);
	sharedMem->readSemPost();
}

void IPCInMemoryStreamClient::unsubscribe(const string& tableName) {
	if (tableName == "") {
		throw RuntimeException("Cannot pass an empty string");
	}
	if (tableName2thread_.find(tableName) == tableName2thread_.end()) {
		throw RuntimeException("Table '" + tableName + "' does not exist");
	}

	ThreadWrapper& threadWrapper = tableName2thread_[tableName];
	if (threadWrapper.isExit == true) {
		return;
	}
	threadWrapper.isExit = true;
	wakeupIPCInMemoryStreamClient(tableName);
	ThreadSP thread = threadWrapper.thread;
	if (thread != nullptr) {
		thread->join();
	}
}

#endif //LINUX

}  // namespace dolphindb
