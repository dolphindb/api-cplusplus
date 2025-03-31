#include "Streaming.h"
#include "Concurrent.h"
#include "ConstantMarshall.h"
#include "Exceptions.h"
#include "Util.h"
#include "TableImp.h"
#include "ScalarImp.h"
#include "Logger.h"
#include "DolphinDB.h"
#include "ConstantImp.h"
#include "Platform.h"
#include <list>
#include <map>
#include <atomic>

#ifdef USE_AERON

#include <mutex>
#if defined(_MSC_VER)
#pragma warning( push )
#pragma warning( disable : 4100 )
#elif defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#else // gcc
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

#include "FragmentAssembler.h"
#include "concurrent/BackOffIdleStrategy.h"
#include "internal/EmbeddedMediaDriver.h"
#include "concurrent/status/StatusIndicatorReader.h"

#if defined(_MSC_VER)
#pragma warning( pop )
#elif defined(__clang__)
#pragma clang diagnostic pop
#else // gcc
#pragma GCC diagnostic pop
#endif

namespace {

std::shared_ptr<aeron::EmbeddedMediaDriver> AeronDriver;
std::mutex AeronMutex;
std::string AeronDriverDir;
int AeronCount;

std::shared_ptr<aeron::Aeron> AeronStart()
{
    std::unique_lock<std::mutex> aeronLock{AeronMutex};
    if (AeronDriver == nullptr) {
        AeronDriver = std::make_shared<aeron::EmbeddedMediaDriver>();
        AeronDriverDir =  "/dev/shm/dolphindb_udp_" + std::to_string(getpid());
    }
    if (AeronCount == 0) {
        AeronDriver->start(AeronDriverDir.c_str());
    }
    ++AeronCount;
    // context is aeron's config struct
    aeron::Context context;
    context.aeronDir(AeronDriverDir);
    context.preTouchMappedMemory(true);
    return aeron::Aeron::connect(context);
}

void AeronStop()
{
    std::unique_lock<std::mutex> aeronLock{AeronMutex};
    --AeronCount;
    if (AeronCount == 0) {
        AeronDriver->stop();
        AeronDriver = nullptr;
    }
}

}

#endif

#ifdef _WIN32
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
using std::endl;
using std::pair;
using std::set;
using std::unordered_map;
using std::string;
using std::vector;

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
    int colCount = static_cast<int>(colLabels.size());
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
		Table *t = (Table*)one.get();
		for (INDEX colIndex = 0; colIndex < colSize; colIndex++) {
			((Vector*)table->getColumn(colIndex).get())->append(t->getColumn(colIndex));
		}
	}
	auto basicTable = dynamic_cast<BasicTable*>(table);
	if(basicTable != nullptr){
		basicTable->updateSize();
	}
	return true;
}

}  // namespace dolphindb

namespace dolphindb {

class StreamingClientImpl {
    struct HAStreamTableInfo{
        std::string followIp;
        int         followPort;
        std::string tableName;
        std::string action;
        std::string leaderIp;
        int         leaderPort;
    };
    struct SubscribeParam {
        SubscribeParam()
            : ID_("INVALID"),
              info_(),
              offset_(-1),
              resub_(false),
              filter_(nullptr),
              msgAsTable_(false),
              allowExists_(false),
              haSites_(0),
              queue_(nullptr),
              userName_(""),
              password_(""),
              streamDeserializer_(nullptr),
              currentSiteIndex_(-1),
              isEvent_(false),
              resubscribeInterval_(100),
              subOnce_(false),
              lastSiteIndex_(-1),
              batchSize_(1),
              resubscribeTimeout_(0)
        {
            subscribeTime_ = std::chrono::steady_clock::now();
        }
        explicit SubscribeParam(const string& id, const SubscribeInfo &info, long long offset, bool resub,
                               const VectorSP &filter, bool msgAsTable, bool allowExists, int batchSize,
								const string &userName, const string &password, const StreamDeserializerSP &blobDeserializer, bool isEvent, int resubscribeInterval, bool subOnce, bool convertMsgRowData, int resubscribeTimeout)
            : ID_(std::move(id)),
              info_(info),
              offset_(offset),
              resub_(resub),
              filter_(filter),
              msgAsTable_(msgAsTable),
              allowExists_(allowExists),
              attributes_(),
              haSites_(0),
              queue_(new MessageQueue(std::max(DEFAULT_QUEUE_CAPACITY, batchSize), batchSize)),
			  userName_(std::move(userName)),
			  password_(std::move(password)),
			  streamDeserializer_(blobDeserializer),
			  currentSiteIndex_(-1),
			  isEvent_(isEvent),
			  resubscribeInterval_(resubscribeInterval),
			  subOnce_(subOnce),
			  lastSiteIndex_(-1),

              batchSize_(batchSize),
              convertMsgRowData_(convertMsgRowData),
              stopped_(std::make_shared<std::atomic<bool>>(false)),
              resubscribeTimeout_(resubscribeTimeout)
        {
            subscribeTime_ = std::chrono::steady_clock::now();
		}

        string ID_;
        SubscribeInfo info_;
        long long offset_;
        bool resub_;
        VectorSP filter_;
        bool msgAsTable_;
        bool allowExists_;
        vector<string> attributes_;
        vector<pair<string, int>> haSites_;
		MessageQueueSP queue_;
		string userName_, password_;
		StreamDeserializerSP streamDeserializer_;
		SocketSP socket_;
        vector<ThreadSP> handleThread_;
        vector<pair<string, int>> availableSites_;
        int currentSiteIndex_;
        bool isEvent_;
        int resubscribeInterval_;
        bool subOnce_;
        int lastSiteIndex_;
        int batchSize_;
        bool convertMsgRowData_;
        std::shared_ptr<std::atomic<bool>> stopped_;
        int resubscribeTimeout_;
        std::chrono::steady_clock::time_point subscribeTime_;

        bool operator<(const SubscribeParam &other) const {
            return ID_ < other.ID_;
        }

		void exit() {
			if (!socket_.isNull()) {
				socket_->close();
			}
			if(queue_.isNull())
				return;
			*stopped_ = true;
			queue_->push(Message());
			for (auto &one : handleThread_) {
				one->join();
			}
			handleThread_.clear();
		}

		void updateByReconnect(int currentReconnSiteIndex) {
            auto thisTopicLastSuccessfulNode = this->lastSiteIndex_;

            if (this->subOnce_ && thisTopicLastSuccessfulNode != currentReconnSiteIndex) {
                        // update currentSiteIndex
                        if (thisTopicLastSuccessfulNode < currentReconnSiteIndex){
                            currentReconnSiteIndex --;
                        }
                        // update info
                        this->availableSites_.erase(this->availableSites_.begin() + thisTopicLastSuccessfulNode);
                        this->currentSiteIndex_ = currentReconnSiteIndex;

                        // update lastSuccessfulNode
                        this->lastSiteIndex_ = currentReconnSiteIndex;
            }
		}
    };
	class ActivePublisher {
	public:
		ActivePublisher(std::shared_ptr<DBConnection> conn) : conn_(conn) {}
		~ActivePublisher() {}
		DataInputStreamSP getDataInputStream(){ return conn_->getDataInputStream();}
	private:
		enum SubscriberRPCType { RPC_OK, RPC_START, RPC_END };
		enum SubscriberFromType { FROM_DDB, FROM_API };
		std::shared_ptr<DBConnection> conn_;
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
    explicit StreamingClientImpl(int listeningPort) : listeningPort_(listeningPort), publishers_(5), isInitialized_(false){
		if (listeningPort_ < 0) {
			throw RuntimeException("Invalid listening port value " + std::to_string(listeningPort));
		}
#ifdef _WIN32
        if (!WSAStarted && startWSA()) {
            throw RuntimeException("Can't start WSA");
        }
        WSAStarted = true;
#endif
    }
    ~StreamingClientImpl() {
		exit();
    }
	void exit() {
		if (exit_)
			return;
		exit_ = true;
		if(!isListenMode()){
			publishers_.push(ActivePublisherSP());
		}
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
		topicSubInfos_.op([&](unordered_map<string, SubscribeParam>& mp) {
			for (auto &one : mp) {
				one.second.exit();
			}
		});

	}
	inline bool isExit() {
		return exit_;
	}
	SubscribeQueue subscribeInternal(const SubscribeInfo &subInfo, int64_t offset = -1,
                                     bool resubscribe = true, const VectorSP &filter = nullptr, bool msgAsTable = false,
                                     bool allowExists = false, int batchSize  = 1,
									const string &userName="", const string &password="",
									const StreamDeserializerSP &sdsp = nullptr, const std::vector<std::string>& backupSites = std::vector<std::string>(), bool isEvent = false, int resubscribeInterval = 100, bool subOnce = false, bool convertMsgRowData = false, int resubscribeTimeout = 3600000);
    string subscribeInternal(DBConnection &conn, SubscribeParam &info);
    void insertMeta(SubscribeParam &info, const string &topic);
    bool delMeta(const string &topic, bool exitFlag);
    bool unsubscribeInternal(const SubscribeInfo &subInfo);
	void addHandleThread(const MessageQueueSP& queue, const ThreadSP &thread) {
		topicSubInfos_.op([&](unordered_map<string, SubscribeParam>& mp) {
			for (auto &one : mp) {
				if (one.second.queue_ == queue) {
					one.second.handleThread_.push_back(thread);
					return;
				}
			}
			DLogger::Error("can't find message queue in exist topic.");
		});
	}
	std::size_t getQueueDepth(const ThreadSP &thread) {
		MessageQueueSP queue=findMessageQueue(thread);
		if (queue.isNull()) {
			return 0;
		}
		return queue->size();
	}
	MessageQueueSP findMessageQueue(const ThreadSP &thread) {
		MessageQueueSP queue;
		topicSubInfos_.op([&](unordered_map<string, SubscribeParam>& mp) {
			for (auto &one : mp) {
				for(auto &handle : one.second.handleThread_) {
					if (handle == thread) {
						queue = one.second.queue_;
						return;
					}
				}
			}
		});
		return queue;
	}
    void onSubscribeStateChange(const SubscribeCallbackT &callback)
    {
        callback_ = callback;
    }

private:
    //if server support reverse connect, set the port to 0
    //if server Not support reverse connect and the port is 0, throw a RuntimeException
    void checkServerVersion(std::string host, int port, const std::vector<std::string>& backupSites);
    void init();
	bool isListenMode() {
		return listeningPort_ > 0;
	}
    void parseMessage(DataInputStreamSP in);
	void sendPublishRequest(DBConnection &conn, SubscribeParam &info);
    void reconnect();
	static bool initSocket(const SocketSP &socket) {
		if (socket.isNull())
			return false;
		static KeepAliveAttr keepAliveAttr;
#ifdef _WIN32
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
		DataInputStreamSP inputStream;
		ActivePublisherSP publisher;
		//IO_ERR ret;
        while (isExit() == false) {
            try {
				if (isListenMode()) {
					SocketSP socket = listenerSocket_->accept();
					if (!initSocket(socket))
						break;
					inputStream = new DataInputStream(socket);
				}
				else {
					publishers_.pop(publisher);
					if (publisher.isNull())
						break;
					inputStream = publisher->getDataInputStream();
				}
                if (inputStream->getSocket().isNull()) {
                    //cerr << "Streaming Daemon socket accept failed, aborting." << endl;
                    break;
                };

                ThreadSP t = new Thread(new Executor(std::bind(&StreamingClientImpl::parseMessage, this, inputStream)));
                t->start();
				parseSocketThread_.push(SocketThread(inputStream->getSocket(),t,publisher));
            } catch (std::exception &e) {
                cerr << "Daemon exception: " << e.what() << endl;
                cerr << "Restart Daemon in 1 second" << endl;
                Util::sleep(1000);
            } catch (...) {
                cerr << "Daemon unknown exception: " << endl;
                cerr << "Restart Daemon in 1 second" << endl;
                Util::sleep(1000);
            }
        }
    }

    inline string stripActionName(string topic) { return topic.substr(0, topic.find_last_of('/')); }
    inline string getSite(string topic) { return topic.substr(0, topic.find_first_of('/')); }

    string getLocalIP() {
        if (localIP_.empty()) localIP_ = "localhost";
        return localIP_;
    }

    void parseIpPort(const string &ipport, string &ip, int &port) {
        auto v = Util::split(ipport, ':');
        if (v.size() < 2) {
            throw RuntimeException("The format of backupSite " + ipport + " is incorrect, should be host:port, e.g. 192.168.1.1:8848");
        }
        ip = v[0];
        port = std::stoi(v[1]);
        if (port <= 0 || port > 65535) {
            throw RuntimeException("The format of backupSite " + ipport + " is incorrect, port should be a positive integer less or equal to 65535");
        }
    }

    bool getNewLeader(const string& s, string &host, int &port)
    {
        size_t index = s.find("<NotLeader>");
        if (index == string::npos) {
            return false;
        }
        index = s.find(">");
        string ipport = s.substr(index + 1);

        auto v = Util::split(ipport, ':');
        if (v.size() < 2) {
            return false;
        }
        host = v[0];
        port = std::stoi(v[1]);
        return (port > 0 && port <= 65535);
    }

private:
    SocketSP listenerSocket_;
    ThreadSP daemonThread_;
    ThreadSP reconnectThread_;
    SynchronizedQueue<SocketThread> parseSocketThread_;
	int listeningPort_;
    string localIP_;
    Hashmap<string, SubscribeParam> topicSubInfos_;
    Hashmap<string, int> actionCntOnTable_;
    Hashmap<string, set<string>> liveSubsOnSite_;  // living site -> topic
    Hashmap<string, pair<long long, long long>> topicReconn_;
	BlockingQueue<ActivePublisherSP> publishers_;
    Mutex mtx_;
    std::set<SubscribeParam> initResub_;
	bool exit_;
	Mutex readyMutex_;
    std::list<HAStreamTableInfo> haStreamTableInfo_;
    bool isInitialized_;
    std::map<std::string, std::string> topics_;     //ID -> current topic
    SubscribeCallbackT callback_{[](const SubscribeState, const SubscribeInfo &){return true;}};
#ifdef _WIN32
    static bool WSAStarted_;
    static void WSAStart();
#endif
};

struct SubscribeClient {
    SubscribeConfig config;
    std::shared_ptr<DBConnection> conn;
    std::thread receiveThread;
    std::thread callbackThread;
    std::shared_ptr<MessageQueue> msgQueue;
    std::string topic;
    std::vector<std::string> columnNames;
    int port;
    std::atomic<bool> exit{false};
    int64_t offset{-1};
    MessageHandler handler;
    StreamDeserializerSP deserializer;
#ifdef USE_AERON
    std::shared_ptr<aeron::Aeron> aeron;
    std::shared_ptr<aeron::Subscription> subscription;
#endif
};

#ifdef USE_AERON

class UDPStreamingImpl {
public:
    UDPStreamingImpl(StreamingClientConfig config)
        : config_(config) {}
    ~UDPStreamingImpl();
    void subscribe(const SubscribeInfo &info, const SubscribeConfig &config,
        const std::shared_ptr<DBConnection> &conn, const MessageHandler &handlerconst, const StreamDeserializerSP &blobDeserializer);
    bool unsubscribe(const SubscribeInfo &info);
private:
    bool parseMessage(DataInputStreamSP data, SubscribeClient &client);
    using clientItT = std::map<SubscribeInfo, std::shared_ptr<SubscribeClient>>::iterator;
    void stop(const clientItT &clientIt);
    void cleanup();
private:
    StreamingClientConfig config_;
    std::mutex clientsMutex_;
    std::map<SubscribeInfo, std::shared_ptr<SubscribeClient>> clients_;
    std::set<std::shared_ptr<SubscribeClient>> deadClients_;
};

void UDPStreamingImpl::subscribe(const SubscribeInfo &info, const SubscribeConfig &config,
    const std::shared_ptr<DBConnection> &conn, const MessageHandler &handler, const StreamDeserializerSP &blobDeserializer)
{
    if (config.msgAsTable && !blobDeserializer.isNull()) {
        throw RuntimeException("msgAsTable must be false when StreamDeserializer is set.");
    }
    {
        std::unique_lock<std::mutex> clientsLock{clientsMutex_};
        cleanup();
        auto clientIt = clients_.find(info);
        if (clientIt != clients_.end()) {
            throw RuntimeException("The subscription to table " + info.tableName + " with actionName " + info.actionName + " was already made.");
        }
    }
    auto newClient = std::make_shared<SubscribeClient>();
    auto &client = *newClient;
    client.config = config;
    client.offset = config.offset;
    client.handler = handler;
    client.conn = conn;
    auto getInfo = conn->getSubscriptionTopic(info);
    client.topic = std::move(getInfo.first);
    client.columnNames = std::move(getInfo.second);
    client.port = conn->publishTable(info, config, config_.protocol);
    client.msgQueue = std::make_shared<MessageQueue>(1024);
    client.deserializer = blobDeserializer;
    client.aeron = AeronStart();
    std::string channel = "aeron:udp?endpoint=224.1.1.1:";
    channel += std::to_string(client.port);
    // copied from Aeron's subscriber example code.
    int64_t id = client.aeron->addSubscription(channel, 0);
    client.subscription = client.aeron->findSubscription(id);
    while (!client.subscription) {
        std::this_thread::yield();
        client.subscription = client.aeron->findSubscription(id);
    }
    client.receiveThread = std::thread([this, &client](){
        aeron::fragment_handler_t h = [this, &client](aeron::concurrent::AtomicBuffer &buffer, aeron::util::index_t offset, aeron::util::index_t length, aeron::Header &header) {
            std::ignore = header;
            DataInputStreamSP input(new DataInputStream(reinterpret_cast<const char *>(buffer.buffer()+offset), length));
            if (!parseMessage(input, client)) {
                throw RuntimeException("Invalid Stream Data.");
            }
        };
        aeron::FragmentAssembler fragmentAssembler(h);
        aeron::concurrent::logbuffer::fragment_handler_t handler = fragmentAssembler.handler();
        aeron::BackoffIdleStrategy idleStrategy;
        while(!client.exit){
            const int fragmentsRead = client.subscription->poll(handler, 1);
            idleStrategy.idle(fragmentsRead);
        }
    });
    client.callbackThread = std::thread([&client](){
        while (!client.exit) {
            Message msg;
            client.msgQueue->pop(msg);
            if (msg.isNull()){
                break;
            }
            if (client.config.msgAsTable) {
                client.handler(Message(convertTupleToTable(client.columnNames, msg), msg.getOffset()));
                continue;
            }
            auto offset = msg.getOffset();
            auto &des = client.deserializer;
            if (!des.isNull()) {
                vector<VectorSP> rows;
                vector<std::string> symbols;
                ErrorCodeInfo errorInfo;
                if (!des->parseBlob(msg, rows, symbols, errorInfo)) {
                    cerr << "[ERROR] parse BLOB field failed: " << errorInfo.errorInfo << ", stopping this parse thread." << endl;
                    break;
                }
                auto symbolIt = symbols.begin();
                for (auto &row : rows) {
                    Message m(row, *symbolIt++, des, offset++);
                    client.handler(m);
                }
                rows.clear();
            } else {
                auto colSize = msg->size();
                auto rowSize = msg->get(0)->size();
                for (INDEX i = 0; i < rowSize && !client.exit; ++i) {
                    VectorSP row = Util::createVector(DT_ANY, colSize, colSize);
                    for (INDEX j = 0; j < colSize; ++j) {
                        row->set(j, msg->get(j)->get(i));
                    }
                    Message rowMsg(row, offset++);
                    client.handler(rowMsg);
                }
            }
        }
    });

    {
        std::unique_lock<std::mutex> clientsLock{clientsMutex_};
        clients_[info] = newClient;
        client.conn->restartMulticast(info, client.topic, config);
    }
}

bool UDPStreamingImpl::unsubscribe(const SubscribeInfo &info)
{
    std::unique_lock<std::mutex> clientsLock{clientsMutex_};
    cleanup();
    auto clientIt = clients_.find(info);
    if (clientIt == clients_.end()) {
        return false;
    }
    auto &client = clientIt->second;
    client->conn->stopPublishTable(info, config_.protocol);
    stop(clientIt);
    return true;
}

bool UDPStreamingImpl::parseMessage(DataInputStreamSP data, SubscribeClient &client)
{
    IO_ERR ret = OK;
    std::string msgTopic;
    ret = data->readString(msgTopic);
    if (ret != OK) {
        return false;
    }
    std::vector<std::string> topics = Util::split(msgTopic, ',');
    if (std::find(topics.begin(), topics.end(), client.topic) == topics.end()) {
        return true;
    }
    long long msgOffset;
    ret = data->readLong(msgOffset);
    if(ret != OK) {
        return false;
    }
    if(client.offset != -1 && client.offset != msgOffset){
        return true;
    }
    if (client.offset == -1) {
        client.offset = msgOffset;
    }
    char littleEndian;
    ret = data->readChar(littleEndian);
    if(ret != OK) {
        return false;
    }
    long long sentTime;
    ret = data->readLong(sentTime);
    if(ret != OK) {
        return false;
    }
    long long msgId;
    ret = data->readLong(msgId);
    if(ret != OK) {
        return false;
    }
    long long msgLength;
    ret = data->readLong(msgLength);
    if(ret != OK) {
        return false;
    }
    if(client.offset == msgOffset && msgId == -1){
        client.offset = msgOffset + msgLength;
        return true;
    }
    short flag;
    ret = data->readShort(flag);
    if(ret != OK) {
        return false;
    }
    auto form = static_cast<DATA_FORM>(flag >> 8);
    ConstantUnmarshallSP unmarshall = ConstantUnmarshallFactory::getInstance(form, data);
    if (unmarshall.isNull()) {
        return false;
    }
    unmarshall->start(flag, true, ret);
    if (ret != OK) {
        return false;
    }
    ConstantSP obj = unmarshall->getConstant();
    if (obj->isTable()) {
        return true;
    }
    Message message(obj, client.offset);
    client.msgQueue->push(message);
    client.offset += msgLength;
    return true;
}

void UDPStreamingImpl::cleanup()
{
    auto myId = std::this_thread::get_id();
    for (auto it = deadClients_.begin(); it != deadClients_.end(); ) {
        auto &client = *it;
        // If user call unsubscribe in handler, callbackThread is unable to join itself.
        if (client->callbackThread.get_id() == myId) {
            ++it;
            continue;
        }
        client->callbackThread.join();
        client->receiveThread.join();
        client->subscription = nullptr;
        client->aeron = nullptr;
        AeronStop();
        auto nextIt = it;
        ++nextIt;
        deadClients_.erase(it);
        it = nextIt;
    }
}

void UDPStreamingImpl::stop(const clientItT &clientIt)
{
    std::shared_ptr<SubscribeClient> client;
    client = clientIt->second;
    clients_.erase(clientIt);
    client->exit = true;
    client->msgQueue->push(Message());
    deadClients_.emplace(std::move(client));
    cleanup();
}

UDPStreamingImpl::~UDPStreamingImpl()
{
    while (!clients_.empty()) {
        stop(clients_.begin());
    }
    cleanup();
}

#endif

void StreamingClientImpl::init(){
    if(isInitialized_){
        return;
    }
    isInitialized_ = true;
    if (isListenMode()) {
        listenerSocket_ = new Socket("", listeningPort_, true, 30);
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

void StreamingClientImpl::checkServerVersion(std::string host, int port, const std::vector<std::string>& backupSites){
    DBConnection conn;
    unsigned index = 0;
    while(true){
        try{
            conn = DBConnection(host, port, "", "");
            break;
        }
        catch(const std::exception& e){
            if(index == backupSites.size()){
                throw e;
            }
            parseIpPort(backupSites[index], host, port);
            index++;
        }
    }

    if (conn.checkVersion({{2,9,0},{3,0,0}})) {
        if(listeningPort_ != 0){
            DLogger::Warn("The server only supports subscription through reverse connection (connection initiated by the subscriber). The specified port will not take effect.");
            listeningPort_ = 0;
        }
    } else {
        if(listeningPort_ == 0){
            throw RuntimeException("The server does not support subscription through reverse connection (connection initiated by the subscriber). Specify a valid port parameter.");
        }
    }
}

void StreamingClientImpl::reconnect() {
    while (!isExit()) {
        topicReconn_.op([&](unordered_map<string, pair<long long, long long>> &mp) {
            std::vector<std::string> failedResubs;
            for (auto &p : mp) {
                SubscribeParam info;
                if (!topicSubInfos_.find(p.first, info)) continue;
                if (!info.resub_) continue;
                string topic = p.first;
                string host = info.info_.hostName;
                int port = info.info_.port;
                string newTopic = topic;
                bool isReconnected = false;

                // reconn every info.resubscribeInterval ms
                if (Util::getEpochTime() - p.second.first <= info.resubscribeInterval_) continue;
                if (info.resubscribeTimeout_ > 0 && (std::chrono::steady_clock::now() - info.subscribeTime_) > std::chrono::milliseconds(info.resubscribeTimeout_)) {
                    cerr << "[" << Timestamp(Util::getEpochTime()).getString() << "]" << ": resubscribe reached resubscribeTimeout" << std::endl;
                    failedResubs.push_back(p.first);
                    continue;
                }

                if(info.availableSites_.empty()){
                    for (int i = 0; i < 3; ++i) {
                        if (isExit()) {
                            return;
                        }
                        callback_(SubscribeState::Resubscribing, info.info_);
                        try {
                            auto conn = DBConnection(host, port, info.userName_, info.password_);
                            LockGuard<Mutex> lock(&readyMutex_);
                            newTopic = subscribeInternal(conn, info);
                            if (newTopic != topic) {
                                delMeta(topic, false);
                                insertMeta(info, newTopic);
                            }
                            break;
                        } catch (std::exception &e) {
                            string msg = e.what();
                            if (getNewLeader(e.what(), host, port)) {
                                cerr << "In reconnect: Got NotLeaderException, switch to leader node [" << host << ":" << port << "] for subscription"  << endl;
                                HAStreamTableInfo haInfo{info.info_.hostName, info.info_.port, info.info_.tableName, info.info_.actionName, host, port};
                                haStreamTableInfo_.push_back(haInfo);
                                info.info_.hostName = host;
                                info.info_.port = port;
                            } else {
                                DLogger::Info("#attempt=", p.second.second++, ", failed to resubscribe, exception:{", e.what(), "}");
                                if (!info.haSites_.empty()) {
                                    int k = rand() % info.haSites_.size();
                                    host = info.haSites_[k].first;
                                    port = info.haSites_[k].second;
                                    cerr << ", will retry site: " << host << ":" << port << endl;
                                } else {
                                    cerr << endl;
                                }
                            }
                        }
                    }
                }
                else{
                    // if current node needs to reconnect for the first time, it will be recorded
                    if (info.lastSiteIndex_ == -1) {
                        info.lastSiteIndex_ = info.currentSiteIndex_;
                    }

                    // try every site twice
                    int currentSiteIndex = info.currentSiteIndex_;
                    for (unsigned i = 0; i < info.availableSites_.size() && !isReconnected; ++i) {
                        // info.availableSites.size is not empty
                        // init new currentSite
                        info.currentSiteIndex_ = currentSiteIndex;
                        info.info_.hostName = info.availableSites_[info.currentSiteIndex_].first;
                        info.info_.port = info.availableSites_[info.currentSiteIndex_].second;
                        host = info.info_.hostName;
                        port = info.info_.port;
                        topicSubInfos_.upsert(p.first, [&](SubscribeParam &_info) { _info = info; }, info);

                        // currentSite will be reconnected twice
                        for (int j = 0; j < 2 && !isReconnected; ++ j) {
                            if (isExit()) {
                                return;
                            }
                            try {
                                auto conn = DBConnection(host, port, info.userName_, info.password_);
                                LockGuard<Mutex> lock(&readyMutex_);
                                newTopic = subscribeInternal(conn, info);
                                if (newTopic != topic) {
                                    delMeta(topic, false);
                                    insertMeta(info, newTopic);
                                }

                                // set status flag
                                isReconnected = true;
                                // update info data
                                info.updateByReconnect(currentSiteIndex);
                                topicSubInfos_.upsert(newTopic, [&](SubscribeParam &_info) { _info = info; }, info);
                                break;
                            } catch (std::exception &e) {
                                string msg = e.what();
                                if(!info.availableSites_.empty()){
                                    cerr << "#attempt=" << p.second.second++ << ", failed to resubscribe, exception:{" << e.what() << "}\n";
                                }
                                else if (getNewLeader(e.what(), host, port)) {
                                    cerr << "In reconnect: Got NotLeaderException, switch to leader node [" << host << ":" << port << "] for subscription"  << endl;
                                    HAStreamTableInfo haInfo{info.info_.hostName, info.info_.port, info.info_.tableName, info.info_.actionName, host, port};
                                    haStreamTableInfo_.push_back(haInfo);
                                    info.info_.hostName = host;
                                    info.info_.port = port;
                                } else {
                                    cerr << "#attempt=" << p.second.second++ << ", failed to resubscribe, exception:{"
                                        << e.what() << "}";
                                    if (!info.haSites_.empty()) {
                                        int k = rand() % info.haSites_.size();
                                        host = info.haSites_[k].first;
                                        port = info.haSites_[k].second;
                                        cerr << ", will retry site: " << host << ":" << port << endl;
                                    } else {
                                        cerr << endl;
                                    }
                                }
                            }
                        }
                        currentSiteIndex = (currentSiteIndex + 1) % info.availableSites_.size();
                    }

                    // clear currentSite
                    if (!isReconnected) {
                        info.currentSiteIndex_ = 0;
                    }
                }
                p.second.first = Util::getEpochTime();
            }
            for (auto &topic : failedResubs) {
                mp.erase(topic);
            }
        });

        // if init unsuccessfully
        {
            LockGuard<Mutex> _(&mtx_);
            vector<SubscribeParam> v;
            for (auto info : initResub_) {
                string _host;
                int _port = 0;
                auto now = std::chrono::steady_clock::now();
                if (info.resubscribeTimeout_ > 0 && (now - info.subscribeTime_) > std::chrono::milliseconds(info.resubscribeTimeout_)) {
                    cerr << "[" << Timestamp(Util::getEpochTime()).getString() << "]" << ": resubscribe reached resubscribeTimeout" << std::endl;
                    continue;
                }
                try {
                    DBConnection conn = DBConnection(info.info_.hostName, info.info_.port, info.userName_, info.password_);
                    LockGuard<Mutex> lock(&readyMutex_);
                    auto topic = subscribeInternal(conn, info);
                    insertMeta(info, topic);
                } catch (std::exception &e) {
                    if (getNewLeader(e.what(), _host, _port)) {
                        DLogger::Warn("when handle initResub_, Got NotLeaderException, switch to leader node [", _host, ":", _port, "] for subscription");
                        HAStreamTableInfo haInfo{info.info_.hostName, info.info_.port, info.info_.tableName, info.info_.actionName, _host, _port};
                        haStreamTableInfo_.push_back(haInfo);
                        info.info_.hostName = _host;
                        info.info_.port = _port;
                    } else {
                        DLogger::Warn("failed to resub with exception: ", e.what());
                        if(!info.availableSites_.empty()){
                            info.currentSiteIndex_ = (info.currentSiteIndex_ + 1) % info.availableSites_.size();
                            info.info_.hostName = info.availableSites_[info.currentSiteIndex_].first;
                            info.info_.port = info.availableSites_[info.currentSiteIndex_].second;
                        }
                    }
                    v.push_back(info);
                }
            }
            initResub_.clear();
            for (auto &i : v) {
                initResub_.insert(i);
            }
        }

        // check reconnected interval time
        Util::sleep(10);
    }
}

void StreamingClientImpl::parseMessage(DataInputStreamSP in) {
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

    while (isExit() == false) {
        if (ret != OK) {  // blocking mode, ret won't be NODATA
            if (!actionCntOnTable_.count(aliasTableName) || actionCntOnTable_[aliasTableName] == 0) {
                break;
            };
            if (topicMsg.empty()) {
                cerr << "WARNING: ERROR occured before receiving first message, can't do recovery." << endl;
                break;
            }
            // close this socket, and do resub
            in->close();
            in->getSocket().clear();
			if (topics.empty())
				break;
			auto site = getSite(topics[0]);
            set<string> ts;
            if (liveSubsOnSite_.find(site, ts)) {
                for (auto &t : ts) {
                    SubscribeParam info;
                    if (topicSubInfos_.find(t, info)) {
                        callback_(SubscribeState::Disconnected, info.info_);
                    }
                    topicSubInfos_.op([&t](unordered_map<string, SubscribeParam> &mp) { mp[t].subscribeTime_ = std::chrono::steady_clock::now(); });
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
            for (auto &t : topics) {
                topicReconn_.erase(t);
            }
        } else if (LIKELY(obj->isVector())) {
			int colSize = obj->size();
            int rowSize = obj->get(0)->size();
//            offset += rowSize;
			if (isListenMode() && rowSize == 1) {
                // 1d array to 2d
                VectorSP newObj = Util::createVector(DT_ANY, colSize);
                for (int i = 0; i < colSize; ++i) {
                    ConstantSP val = obj->get(i);
                    VectorSP col = Util::createVector(val->getType(), 1);
					if(val->getForm() == DF_VECTOR) {
						col = Util::createArrayVector((DATA_TYPE)((int)val->getType()+ARRAY_TYPE_BASE), 1);
					}
                    col->set(0, val);
                    newObj->set(i, col);
                }
                obj = newObj;
            }
            vector<VectorSP> cache, rows;
			ErrorCodeInfo errorInfo;

            //wait for insertMeta() finish, or there is no topic in topicSubInfos_
            LockGuard<Mutex> lock(&readyMutex_);

            for (auto &t : topics) {
                SubscribeParam info;
                if (topicSubInfos_.find(t, info)) {
                    if (info.queue_.isNull()) continue;
                    int startOffset = static_cast<int>(offset - rowSize + 1);
                    if (info.isEvent_){
                        info.queue_->push(Message(obj));
                    }
					else if (info.streamDeserializer_.isNull()==false) {
						if (rows.empty()) {
							if (!info.streamDeserializer_->parseBlob(obj, rows, symbols, errorInfo)) {
								cerr << "[ERROR] parse BLOB field failed: " << errorInfo.errorInfo << ", stopping this parse thread." << endl;
								return;
							}
						}
						for (int rowIdx = 0; rowIdx < rowSize; ++rowIdx) {
							Message m(rows[rowIdx], symbols[rowIdx], info.streamDeserializer_, startOffset++);
							rows[rowIdx].clear();
							info.queue_->push(m);
						}
					}
					else {
						if (info.convertMsgRowData_ || info.msgAsTable_) {
							if (info.attributes_.empty()) {
								std::cerr << "table colName is empty, can not convert to table" << std::endl;
								info.queue_->push(Message(obj, startOffset));
							}
							else {
								info.queue_->push(Message(convertTupleToTable(info.attributes_, obj), startOffset));
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
								info.queue_->push(Message(one, startOffset++));
							}
						}
					}
					topicSubInfos_.op([&](unordered_map<string, SubscribeParam>& mp){
						if(mp.count(t) != 0)
                        	mp[t].offset_ = offset + 1;
                    });
                }
            }
        } else {
			cerr << "Message body has an invalid format. Vector is expected." << endl;
			break;
        }
    }
}

void StreamingClientImpl::sendPublishRequest(DBConnection &conn, SubscribeParam &info){
	ConstantSP re;
	if (info.userName_.empty()) {
		re = run(conn, "publishTable", getLocalIP(), listeningPort_, info.info_.tableName, info.info_.actionName,
			info.offset_, info.filter_, info.allowExists_);
	}
	else {
		conn.login(info.userName_, info.password_, false);
		re = run(conn, "publishTable", getLocalIP(), listeningPort_, info.info_.tableName, info.info_.actionName,
			info.offset_, info.filter_, info.allowExists_);
	}

	if (re->isVector() && re->getType() == DT_ANY) {
		info.haSites_.clear();
		auto vec = re->get(1);
		for (int i = 0; i < vec->size(); ++i) {
			auto s = vec->get(i)->getString();
			auto p = Util::split(s, ':');
			//            cerr << p[0] << ":" << p[1] << endl;
			info.haSites_.emplace_back(p[0], std::stoi(p[1]));
		}
	}
}

string StreamingClientImpl::subscribeInternal(DBConnection &conn, SubscribeParam &info) {
	if (info.userName_.empty() == false)
		conn.login(info.userName_, info.password_, false);
	ConstantSP result = run(conn, "getSubscriptionTopic", info.info_.tableName, info.info_.actionName);
	auto topic = result->get(0)->getString();
	ConstantSP colLabels = result->get(1);
	if (!colLabels->isArray()) throw RuntimeException("The publisher doesn't have the table [" + info.info_.tableName + "].");

	if (info.streamDeserializer_.isNull() == false) {
		info.streamDeserializer_->create(conn);
        // dolphindb will send 1024 rows at a time, make sure we have enough space for deserialization
        info.streamDeserializer_->setTupleLimit(std::max(info.batchSize_ + 1024, 65536));
	}

	int colCount = colLabels->size();
	vector<string> colNames;
	colNames.reserve(colCount);
	for (int i = 0; i < colCount; ++i) colNames.push_back(colLabels->getString(i));
	info.attributes_ = colNames;

	if (isListenMode() == false) {
		std::shared_ptr<DBConnection> activeConn = std::make_shared<DBConnection>(false, false, 30, false, false, true);
		if (!activeConn->connect(info.info_.hostName, info.info_.port, "", "", "", false, vector<string>(), 30)) {
			throw RuntimeException("Failed to connect to server: " + info.info_.hostName + " " + std::to_string(info.info_.port));
		}
		sendPublishRequest(*activeConn, info);
		callback_(SubscribeState::Connected, info.info_);
		ActivePublisherSP publisher = new ActivePublisher(activeConn);
		info.socket_ = publisher->getDataInputStream()->getSocket();
		publishers_.push(publisher);
	}
	else {
		sendPublishRequest(conn, info);
	}
	return topic;
}

void StreamingClientImpl::insertMeta(SubscribeParam &info, const string &topic) {
    topicSubInfos_.upsert(
        topic, [&](SubscribeParam &_info) { _info = info; }, info);
    liveSubsOnSite_.upsert(getSite(topic), [&](set<string> &s) { s.insert(topic); }, {topic});
    actionCntOnTable_.upsert(
        stripActionName(topic), [&](int &cnt) { ++cnt; }, 1);
    topics_[info.ID_] = topic;
}

bool StreamingClientImpl::delMeta(const string &topic, bool exitFlag) {
	if (topicSubInfos_.count(topic) < 1)
		return false;
	SubscribeParam oldinfo;
	topicSubInfos_.op([&](unordered_map<string, SubscribeParam> &mp) {
		oldinfo=mp[topic];
		mp.erase(topic);
	});
    topics_.erase(oldinfo.ID_);
    liveSubsOnSite_.upsert(getSite(topic), [&](set<string> &s) { s.erase(topic); }, {});
    actionCntOnTable_.upsert(
        stripActionName(topic), [&](int &cnt) { --cnt; }, 0);
	if(exitFlag)
		oldinfo.exit();
	return true;
}

SubscribeQueue StreamingClientImpl::subscribeInternal(const SubscribeInfo &subInfo, int64_t offset, bool resubscribe,
                                                      const VectorSP &filter, bool msgAsTable, bool allowExists, int batchSize,
													  const string &userName, const string &password,
													  const StreamDeserializerSP &blobDeserializer, const std::vector<std::string>& backupSites, bool isEvent, int resubscribeInterval, bool subOnce, bool convertMsgRowData, int resubscribeTimeout) {
    if (subInfo.port <= 0 || subInfo.port > 65535) {
        throw IllegalArgumentException(__FUNCTION__, "Invalid port: " + std::to_string(subInfo.port));
    }
    if (msgAsTable && !blobDeserializer.isNull()) {
        throw RuntimeException("msgAsTable must be false when StreamDeserializer is set.");
    }
    string topic;
    int attempt = 0;
    string _host = subInfo.hostName;
    string _id = subInfo.hostName + std::to_string(subInfo.port) + subInfo.tableName + subInfo.actionName;
    int _port = subInfo.port;
    checkServerVersion(subInfo.hostName, subInfo.port, backupSites);
    init();
    if(msgAsTable){
        // A table may contain several messages, ignore batch size to make sure the table is handled instantly
        batchSize = 1;
    }
    while (isExit()==false) {
        ++attempt;
        SubscribeParam info(_id, subInfo, offset, resubscribe, filter, msgAsTable, allowExists,
			batchSize, userName,password,blobDeserializer, isEvent, resubscribeInterval, subOnce, convertMsgRowData, resubscribeTimeout);
        if(!backupSites.empty()){
            info.availableSites_.push_back({subInfo.hostName, subInfo.port});
            info.currentSiteIndex_ = 0;
            std::string backupIP;
            int         backupPort;
            for(const auto& backupSite : backupSites){
                parseIpPort(backupSite, backupIP, backupPort);
                info.availableSites_.push_back({backupIP, backupPort});
            }
        }
        try {
            info.info_.hostName = _host;
            info.info_.port = _port;
            DBConnection conn = DBConnection(_host, _port, userName, password);
			LockGuard<Mutex> lock(&readyMutex_);
			topic = subscribeInternal(conn, info);
			insertMeta(info, topic);
            return SubscribeQueue(info.queue_, info.stopped_);
        } catch (std::exception &e) {
            cerr << e.what() << endl;
            if(!backupSites.empty()){
                LockGuard<Mutex> _(&mtx_);
                initResub_.insert(info);
                insertMeta(info, topic);
                return SubscribeQueue(info.queue_, info.stopped_);
            }
            else if (attempt <= 10 && getNewLeader(e.what(), _host, _port)) {
                DLogger::Warn("Got NotLeaderException, switch to leader node [", _host, ":", _port, "] for subscription");
                HAStreamTableInfo haInfo{subInfo.hostName, subInfo.port, subInfo.tableName, subInfo.actionName, _host, _port};
                haStreamTableInfo_.push_back(haInfo);
            } else if (resubscribe) {
                LockGuard<Mutex> _(&mtx_);
                initResub_.insert(info);
                insertMeta(info, topic);
                return SubscribeQueue(info.queue_, info.stopped_);
            } else {
                throw;
            }
        }
    }
	return SubscribeQueue();
}

bool StreamingClientImpl::unsubscribeInternal(const SubscribeInfo &subInfo) {
    std::string host_ = subInfo.hostName;
    int port_ = subInfo.port;
    string topic;
    string _id = subInfo.hostName + std::to_string(subInfo.port) + subInfo.tableName + subInfo.actionName;
    DBConnection conn;
    auto iter = topics_.find(_id);
    // Maybe different node in a cluster
    if (iter == topics_.end()) {
        // subscribe leader, unsubscribe follower
        auto iter1 = std::find_if(haStreamTableInfo_.begin(), haStreamTableInfo_.end(), [&subInfo](const HAStreamTableInfo& info){
            return info.followIp == subInfo.hostName && info.followPort == subInfo.port && info.tableName == subInfo.tableName && info.action == subInfo.actionName;
        });
        if(iter1 != haStreamTableInfo_.end()) {
            host_ = iter1->leaderIp;
            port_ = iter1->leaderPort;
            haStreamTableInfo_.erase(iter1);
        } else {
            // subscribe follower, unsubscribe leader
            auto iter2 = std::find_if(haStreamTableInfo_.begin(), haStreamTableInfo_.end(), [&subInfo](const HAStreamTableInfo& info){
                return info.leaderIp == subInfo.hostName && info.leaderPort == subInfo.port && info.tableName == subInfo.tableName && info.action == subInfo.actionName;
            });
            if(iter2 != haStreamTableInfo_.end()){
                host_ = iter1->leaderIp;
                port_ = iter1->leaderPort;
                haStreamTableInfo_.erase(iter2);
            }
        }
        _id = subInfo.hostName + std::to_string(subInfo.port) + subInfo.tableName + subInfo.actionName;
        iter = topics_.find(_id);
    }
    if (iter == topics_.end()) {
        DLogger::Warn("subscription of topic not existed: ", topic);
        return false;
    }
    topic = iter->second;
    SubscribeParam info;
    if (!topicSubInfos_.find(topic, info)) {
        DLogger::Warn("subscription of topic not existed: ", topic);
        return false;
    }
    if(!info.availableSites_.empty()){
        host_ = info.availableSites_[info.currentSiteIndex_].first;
        port_ = info.availableSites_[info.currentSiteIndex_].second;
    }
	delMeta(topic, true);
    topicReconn_.op([&topic](unordered_map<string, pair<long long, long long>> &mp) {
        mp.erase(topic);
    });
    {
        LockGuard<Mutex> _(&mtx_);
        initResub_.erase(info);
    }
	if (isListenMode()) {
        conn = DBConnection(host_, port_, info.userName_, info.password_);
    	run(conn, "stopPublishTable", getLocalIP(), listeningPort_, subInfo.tableName, subInfo.actionName);
	}
    return true;
}

StreamingClient::StreamingClient(const StreamingClientConfig &config)
    : impl_(new StreamingClientImpl(0))
{
    if (config.protocol == TransportationProtocol::UDP) {
#ifdef USE_AERON
        udpImpl_ = std::make_shared<UDPStreamingImpl>(config);
#else
        throw IllegalArgumentException(__FUNCNAME__, "UDP transportation protocol is unavailable without Aeron.");
#endif
    } else {
        if (config.callback) {
            impl_->onSubscribeStateChange(config.callback);
        }
    }
}

StreamingClient::StreamingClient(int listeningPort)
    : impl_(new StreamingClientImpl(listeningPort)) {}

StreamingClient::~StreamingClient() {
	exit();
}

void StreamingClient::exit() {
	impl_->exit();
}

bool StreamingClient::isExit() {
	return impl_->isExit();
}

SubscribeQueue StreamingClient::subscribeInternal(const SubscribeInfo &info,
                                                  int64_t offset, bool resubscribe, const dolphindb::VectorSP &filter,
                                                  bool msgAsTable, bool allowExists, int batchSize,
												  string userName, string password,
												  const StreamDeserializerSP &blobDeserializer, const std::vector<std::string>& backupSites, bool isEvent, int resubscribeInterval, bool subOnce, bool convertMsgRowData, int resubscribeTimeout) {
    return impl_->subscribeInternal(info, offset, resubscribe, filter, msgAsTable,
                                    allowExists, batchSize,
									userName,password,
									blobDeserializer, backupSites, isEvent, resubscribeInterval, subOnce, convertMsgRowData, resubscribeTimeout);
}

bool StreamingClient::unsubscribeInternal(string host, int port, string tableName, string actionName) {
    SubscribeInfo info { host, port, tableName, actionName };
    return impl_->unsubscribeInternal(info);
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
								   const StreamDeserializerSP &blobDeserializer, const std::vector<std::string>& backupSites, int resubscribeInterval, bool subOnce, int resubscribeTimeout) {
    SubscribeInfo info { host, port, tableName, actionName };
    auto subscribeQueue = subscribeInternal(info, offset,
                              resub, filter, msgAsTable, allowExists, batchSize, userName, password, blobDeserializer,
                              backupSites, false, resubscribeInterval, subOnce, false, resubscribeTimeout);
    if (subscribeQueue.queue_.isNull()) {
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
	ThreadSP thread = new Thread(new Executor([handler, subscribeQueue, msgAsTable, batchSize, throttleTime, impl]() {
        vector<Message> msgs;
        vector<Message> tableMsg(1);
        bool foundnull = false;
        long long startTime = 0;
        int       currentSize = 0;
        MessageQueueSP queue{subscribeQueue.queue_};
        std::shared_ptr<std::atomic<bool>> stopped{subscribeQueue.stopped_};
        while (impl->isExit()==false && foundnull == false) {
            if(msgAsTable){
                startTime = Util::getEpochTime();
                currentSize = 0;
                //when msgAsTable is true, the batchSize of the queue is set to 1, so only can pop one msg
                if(queue->pop(msgs, throttleTime)){
                    if(*stopped){
                        break;
                    }
                    tableMsg[0] = msgs[0];
                    currentSize = tableMsg[0]->size();
                    long long leftTime = startTime + throttleTime - Util::getEpochTime();
                    while(leftTime > 0 && currentSize < batchSize && queue->pop(msgs, static_cast<int>(leftTime))) {
                        if(*stopped){
                            break;
                        }
                        mergeTable(tableMsg[0], msgs);
                        msgs.clear();
                        leftTime = startTime + throttleTime - Util::getEpochTime();
                        currentSize = tableMsg[0]->size();
                    }
                    handler(tableMsg);
                    msgs.clear();
                }
            }
            else{
                if(queue->pop(msgs, throttleTime)){
                    if (*stopped) {
                        msgs.clear();
                        break;
                    }
                    if(!msgs.empty()){
                        handler(msgs);
                        msgs.clear();
                    }
                }
            }
        }
		queue->push(Message());
    }));
	impl_->addHandleThread(subscribeQueue.queue_, thread);
	thread->start();
    return thread;
}


void handleConvertRowData(Message& msg, const MessageHandler& handler,  ConstantSP& callBackArgs, vector<ConstantSP>& callBackScalarArgs, vector<DATA_TYPE>& types, vector<int>& extras){
    if (callBackScalarArgs.empty()){
        int cols = msg->columns();
        callBackScalarArgs.resize(cols);
        callBackArgs = Util::createVector(DT_ANY, cols);
        for (int i = 0; i < cols; ++i){
            DATA_TYPE type = msg->getColumn(i)->getType();
            types.push_back(type);
            int extra = msg->getColumn(i)->getExtraParamForType();
            extras.push_back(extra);
            if (type < ARRAY_TYPE_BASE){
                callBackScalarArgs[i] = Util::createConstant(type, extra);
            }else{
                callBackScalarArgs[i] = Util::createVector((DATA_TYPE)((int)type - ARRAY_TYPE_BASE), 0, 0, true, extra);
            }
            callBackArgs->set(i, callBackScalarArgs[i]);
        }
    }
    auto cols = callBackScalarArgs.size();
    int rows = msg->rows();
    vector<ConstantSP> columns;
    vector<ConstantSP> arrayIndex;
    vector<ConstantSP> arrayValue;
    vector<INDEX> lastArrayIndex;
    arrayIndex.resize(cols);
    arrayValue.resize(cols);
    lastArrayIndex.resize(cols);
    int offsetEnd = msg.getOffset();

    for (int col = 0; col < static_cast<int>(cols); ++col){
        columns.push_back(msg->getColumn(col));
        VectorSP column = msg->getColumn(col);
        if(types[col] >= ARRAY_TYPE_BASE){
            arrayIndex[col] = ((SmartPointer<FastArrayVector>)column)->getSourceIndex();
            arrayValue[col] = ((SmartPointer<FastArrayVector>)column)->getSourceValue();
        }
        lastArrayIndex[col] = 0;
    }
    for (int row = 0; row < rows; ++row){
        for (int col = 0; col < static_cast<int>(cols); ++col){
            if (types[col] >= ARRAY_TYPE_BASE){
                // callBackArgs->set(col, columns[col]->get(row));

                INDEX index = arrayIndex[col]->getIndex(row);
                int size = index - lastArrayIndex[col];
                ((VectorSP)callBackScalarArgs[col])->resize(size);
                int begin = lastArrayIndex[col], offset = 0;
                while (offset < size)
                {
                    int len = std::min(size - offset, Util::BUF_SIZE);
                    switch (types[col] - ARRAY_TYPE_BASE){
                    case DT_BOOL:
                    {
                        char buffer[Util::BUF_SIZE];
                        const char *ptr = arrayValue[col]->getBoolConst(begin + offset, len, buffer);
                        callBackScalarArgs[col]->setBool(offset, len, ptr);
                    }
                    break;
                    case DT_CHAR:
                    {
                        char buffer[Util::BUF_SIZE];
                        const char *ptr = arrayValue[col]->getCharConst(begin + offset, len, buffer);
                        callBackScalarArgs[col]->setChar(offset, len, ptr);
                    }
                    break;
                    case DT_SHORT:
                    {
                        short buffer[Util::BUF_SIZE];
                        const short *ptr = arrayValue[col]->getShortConst(begin + offset, len, buffer);
                        callBackScalarArgs[col]->setShort(offset, len, ptr);
                    }
                    break;
                    case DT_INT:
                    case DT_DATEHOUR:
                    case DT_DATEMINUTE:
                    case DT_DATE:
                    case DT_TIME:
                    case DT_MONTH:
                    case DT_MINUTE:
                    case DT_SECOND:
                    case DT_DATETIME:
                    {
                        int buffer[Util::BUF_SIZE];
                        const int *ptr = arrayValue[col]->getIntConst(begin + offset, len, buffer);
                        callBackScalarArgs[col]->setInt(offset, len, ptr);
                    }
                    break;
                    case DT_LONG:
                    case DT_TIMESTAMP:
                    case DT_NANOTIME:
                    case DT_NANOTIMESTAMP:
                    {
                        long long buffer[Util::BUF_SIZE];
                        const long long *ptr = arrayValue[col]->getLongConst(begin + offset, len, buffer);
                        callBackScalarArgs[col]->setLong(offset, len, ptr);
                    }
                    break;
                    case DT_FLOAT:
                    {
                        float buffer[Util::BUF_SIZE];
                        const float *ptr = arrayValue[col]->getFloatConst(begin + offset, len, buffer);
                        callBackScalarArgs[col]->setFloat(offset, len, ptr);
                    }
                    break;
                    case DT_DOUBLE:
                    {
                        double buffer[Util::BUF_SIZE];
                        const double *ptr = arrayValue[col]->getDoubleConst(begin + offset, len, buffer);
                        callBackScalarArgs[col]->setDouble(offset, len, ptr);
                    }
                    break;
                    case DT_SYMBOL:
                    case DT_STRING:
                    case DT_BLOB:
                    {
                        char* buffer[Util::BUF_SIZE];
                        arrayValue[col]->getStringConst(begin + offset, len, buffer);
                        callBackScalarArgs[col]->setString(offset, len, buffer);
                    }
                    break;
                    case DT_UUID:
                    case DT_IP:
                    case DT_INT128:
                    {
                        unsigned char buffer[Util::BUF_SIZE * 16];
                        const unsigned char *ptr = arrayValue[col]->getBinaryConst(begin + offset, len, 16, buffer);
                        callBackScalarArgs[col]->setBinary(offset, len, 16, ptr);
                    }
                    break;
                    case DT_DECIMAL32:
                    {
                        unsigned char buffer[Util::BUF_SIZE * 4];
                        const unsigned char *ptr = arrayValue[col]->getBinaryConst(begin + offset, len, 4, buffer);
                        callBackScalarArgs[col]->setBinary(offset, len, 4, ptr);
                    }
                    break;
                    case DT_DECIMAL64:
                    {
                        unsigned char buffer[Util::BUF_SIZE * 8];
                        const unsigned char *ptr = arrayValue[col]->getBinaryConst(begin + offset, len, 8, buffer);
                        callBackScalarArgs[col]->setBinary(offset, len, 8, ptr);
                    }
                    break;
                    case DT_DECIMAL128:
                    {
                        unsigned char buffer[Util::BUF_SIZE * 16];
                        const unsigned char *ptr = arrayValue[col]->getBinaryConst(begin + offset, len, 16, buffer);
                        callBackScalarArgs[col]->setBinary(offset, len, 16, ptr);
                    }
                    break;
                    default:
                        throw RuntimeException("Unsupported data type: " + std::to_string(types[col]));
                    }
                    offset += len;
                }
                lastArrayIndex[col] = index;
            }
            else{
                switch (types[col]){
                case DT_BOOL:{
                    callBackScalarArgs[col]->setBool(columns[col]->getBool(row));
                    break;
                }
                case DT_CHAR:{
                    callBackScalarArgs[col]->setChar(columns[col]->getChar(row));
                    break;
                }
                case DT_SHORT:{
                    callBackScalarArgs[col]->setShort(columns[col]->getShort(row));
                    break;
                }
                case DT_INT:
                case DT_DATEHOUR:
                case DT_DATEMINUTE:
                case DT_DATE:
                case DT_TIME:
                case DT_MONTH:
                case DT_MINUTE:
                case DT_SECOND:
                case DT_DATETIME:{
                    callBackScalarArgs[col]->setInt(columns[col]->getInt(row));
                    break;
                }
                case DT_LONG:
                case DT_TIMESTAMP:
                case DT_NANOTIME:
                case DT_NANOTIMESTAMP:{
                    callBackScalarArgs[col]->setLong(columns[col]->getLong(row));
                    break;
                }
                case DT_FLOAT:{
                    callBackScalarArgs[col]->setFloat(columns[col]->getFloat(row));
                    break;
                }
                case DT_DOUBLE:{
                    callBackScalarArgs[col]->setDouble(columns[col]->getDouble(row));
                    break;
                }
                case DT_SYMBOL:
                case DT_STRING:
                case DT_BLOB:{
                    callBackScalarArgs[col]->setString(columns[col]->getString(row));
                    break;
                }
                case DT_UUID:
                case DT_IP:
                case DT_INT128:{
                    unsigned char buf[16];
                    columns[col]->getBinary(row, 1, 16, buf);
                    static_cast<SmartPointer<Int128>>(callBackScalarArgs[col])->setBinary(buf, 8);
                    break;
                }
                case DT_DECIMAL32:{
                    unsigned char buf[4];
                    columns[col]->getBinary(row, 1, 4, buf);
                    int* ptr = reinterpret_cast<int*>(buf);
                    static_cast<SmartPointer<Decimal32>>(callBackScalarArgs[col])->setRawData(*ptr);
                    break;
                }
                case DT_DECIMAL64:{
                    unsigned char buf[8];
                    columns[col]->getBinary(row, 1, 8, buf);
                    long long *ptr = reinterpret_cast<long long*>(buf);
                    static_cast<SmartPointer<Decimal64>>(callBackScalarArgs[col])->setRawData(*ptr);
                    break;
                }
                case DT_DECIMAL128:{
                    unsigned char buf[16];
                    columns[col]->getBinary(row, 1, 16, buf);
                    wide_integer::int128* ptr = reinterpret_cast<wide_integer::int128*>(buf);
                    static_cast<SmartPointer<Decimal128>>(callBackScalarArgs[col])->setRawData(*ptr);
                    break;
                }
                default:
                    throw RuntimeException("Unsupported data type: " + std::to_string(types[col]));
                }
            }
        }
        Message convertedMsg(callBackArgs, offsetEnd + row);
        handler(convertedMsg);
    }
}

ThreadSP newHandleThread(const MessageHandler handler, SubscribeQueue subscribeQueue, bool msgAsTable, bool isStreamDeserialize, SmartPointer<StreamingClientImpl> impl) {
	ThreadSP thread = new Thread(new Executor([handler, subscribeQueue, msgAsTable, isStreamDeserialize, impl]() {
		vector<Message> tables;
        ConstantSP callBackArgs;
        vector<ConstantSP> callBackScalarArgs;
        vector<DATA_TYPE> types;
        vector<int> extras;
		auto queue = subscribeQueue.queue_;
		while (impl->isExit() == false && !*subscribeQueue.stopped_) {
            Message msg;
			queue->pop(msg);
			// quit handler loop if msg is nullptr
			if (UNLIKELY(*subscribeQueue.stopped_)) {
				break;
			}
			if (msgAsTable && queue->pop(tables, 0)) {
				if (*subscribeQueue.stopped_ || !mergeTable(msg, tables)) {
					break;
				}
			}
            if(!msgAsTable && !isStreamDeserialize){
                handleConvertRowData(msg, handler, callBackArgs, callBackScalarArgs, types, extras);
            }else{
			    handler(msg);
            }
		}
		queue->push(Message());
	}));
	impl->addHandleThread(subscribeQueue.queue_,thread);
	return thread;
}

ThreadSP ThreadedClient::subscribe(string host, int port, const MessageHandler &handler, string tableName,
                                   string actionName, int64_t offset, bool resub, const VectorSP &filter,
                                   bool msgAsTable, bool allowExists,
									string userName, string password,
									const StreamDeserializerSP &blobDeserializer, const std::vector<std::string>& backupSites, int resubscribeInterval, bool subOnce, int resubscribeTimeout) {
        SubscribeInfo info { host, port, tableName, actionName };
#ifdef USE_AERON
    if (udpImpl_ != nullptr) {
        auto conn = std::make_shared<DBConnection>(host, port);
        if (!conn->checkVersion({{3,0,0}})) {
            throw RuntimeException("UDP connection only supports server version at least 3.00.0");
        }
        SubscribeConfig config { offset, msgAsTable, allowExists, false };
        if (!userName.empty()) {
            conn->login(userName, password, false);
        }
        udpImpl_->subscribe(info, config, conn, handler, blobDeserializer);
        return nullptr;
    }
#endif
    auto subscribeQueue = subscribeInternal(info, offset,
                                             resub, filter, msgAsTable, allowExists, 1, userName, password, blobDeserializer, backupSites, false, resubscribeInterval, subOnce, true, resubscribeTimeout);
    if (subscribeQueue.queue_.isNull()) {
        cerr << "Subscription already made, handler loop not created." << endl;
        ThreadSP t = new Thread(new Executor([]() {}));
        t->start();
        return t;
    }

	ThreadSP t = newHandleThread(handler, subscribeQueue, msgAsTable, blobDeserializer.isNull() == false, impl_);
    t->start();
    return t;
}

bool ThreadedClient::unsubscribe(string host, int port, string tableName, string actionName) {
#ifdef USE_AERON
    if (udpImpl_ != nullptr) {
        SubscribeInfo info { host, port, tableName, actionName };
        return udpImpl_->unsubscribe(info);
    }
#endif
    return unsubscribeInternal(std::move(host), port, std::move(tableName), std::move(actionName));
}

/// PollingClient IMPL
PollingClient::PollingClient(int listeningPort) : StreamingClient(listeningPort) {}

MessageQueueSP PollingClient::subscribe(string host, int port, string tableName, string actionName, int64_t offset,
                                        bool resub, const VectorSP &filter, bool msgAsTable, bool allowExists,
										string userName, string password,
										const StreamDeserializerSP &blobDeserializer, const std::vector<std::string>& backupSites, int resubscribeInterval, bool subOnce, int resubscribeTimeout) {
    SubscribeInfo info { host, port, tableName, actionName };
    return subscribeInternal(info, offset, resub, filter,
                             msgAsTable, allowExists,1, userName, password, blobDeserializer, backupSites, false, resubscribeInterval, subOnce, false, resubscribeTimeout).queue_;
}

bool PollingClient::unsubscribe(string host, int port, string tableName, string actionName) {
    return unsubscribeInternal(std::move(host), port, std::move(tableName), std::move(actionName));
}

/// ThreadPooledClient IMPL
ThreadPooledClient::ThreadPooledClient(int listeningPort, int threadCount)
    : StreamingClient(listeningPort), threadCount_(threadCount) {}

bool ThreadPooledClient::unsubscribe(string host, int port, string tableName, string actionName) {
    return unsubscribeInternal(std::move(host), port, std::move(tableName), std::move(actionName));
}

size_t ThreadPooledClient::getQueueDepth(const ThreadSP &thread){
	return impl_->getQueueDepth(thread);
}

vector<ThreadSP> ThreadPooledClient::subscribe(string host, int port, const MessageHandler &handler, string tableName,
                                               string actionName, int64_t offset, bool resub, const VectorSP &filter,
                                               bool msgAsTable, bool allowExists,
												string userName, string password,
												const StreamDeserializerSP &blobDeserializer, const std::vector<std::string>& backupSites, int resubscribeInterval, bool subOnce, int resubscribeTimeout) {
    SubscribeInfo info { host, port, tableName, actionName };
    auto queue = subscribeInternal(info, offset, resub,
                                   filter, msgAsTable, allowExists,1, userName,password, blobDeserializer, backupSites, false, resubscribeInterval, subOnce, true, resubscribeTimeout);
    vector<ThreadSP> ret;
    for (int i = 0; i < threadCount_ && isExit() == false; ++i) {
		ThreadSP t = newHandleThread(handler, queue, msgAsTable, blobDeserializer.isNull() == false, impl_);
        t->start();
        ret.emplace_back(t);
    }
    return ret;
}

EventClient::EventClient(const std::vector<EventSchema>& eventSchemas, const std::vector<std::string>& eventTimeFields, const std::vector<std::string>& commonFields)
    : StreamingClient(0), eventHandler_(eventSchemas, eventTimeFields, commonFields)
{

}

ThreadSP EventClient::subscribe(const string& host, int port, const EventMessageHandler &handler, const string& tableName, const string& actionName, int64_t offset, bool resub, const string& userName, const string& password, int resubscribeTimeout){
    if(tableName.empty()){
        throw RuntimeException("tableName must not be empty.");
    }

    DBConnection tempConn;
    if(!tempConn.connect(host, port, userName, password)){
        throw RuntimeException("Subscribe Fail, cannot connect to " + host + " : " + std::to_string(port));
    }
    std::string sql = "select top 0 * from " + tableName;
    std::string errMsg;
    ConstantSP outputTable = tempConn.run(sql);
    if(!eventHandler_.checkOutputTable(outputTable, errMsg)){
        throw RuntimeException(errMsg);
    }
    tempConn.close();
    SubscribeInfo info { host, port, tableName, actionName };
    SubscribeQueue subscribeQueue = subscribeInternal(info, offset, resub, nullptr, false, false, 1, userName, password, nullptr, std::vector<std::string>(), true, 100, false, false, resubscribeTimeout);
    if (subscribeQueue.queue_.isNull()) {
        cerr << "Subscription already made, handler loop not created." << endl;
        return nullptr;
    }

    SmartPointer<StreamingClientImpl> impl = impl_;
    ThreadSP thread = new Thread(new Executor([this, handler, subscribeQueue, impl]() {
		auto queue = subscribeQueue.queue_;
		Message msg;
		vector<Message> tables;
        std::vector<std::string> eventTypes;
        std::vector<std::vector<ConstantSP>> attributes;
        ErrorCodeInfo errorInfo;
		while (impl->isExit() == false && !*subscribeQueue.stopped_) {
			queue->pop(msg);
			// quit handler loop if msg is nullptr
			if (UNLIKELY(*subscribeQueue.stopped_)){
				break;
			}
            eventTypes.clear();
            attributes.clear();
            if(!eventHandler_.deserializeEvent(msg, eventTypes, attributes, errorInfo)){
                std::cerr << "deserialize fail " << errorInfo.errorInfo << std::endl;
                continue;
            }
            size_t rowSize = eventTypes.size();
            for(size_t i = 0; i < rowSize; ++i){
                handler(eventTypes[i], attributes[i]);
            }
			//handler(msg);
		}
		queue->push(Message());
	}));
	impl_->addHandleThread(subscribeQueue.queue_, thread);
	thread->start();
    return thread;
}

bool EventClient::unsubscribe(const string& host, int port, const string& tableName, const string& actionName){
    return unsubscribeInternal(host, port, tableName, actionName);
}

#ifdef __linux__

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
