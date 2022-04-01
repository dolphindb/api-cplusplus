#include "Streaming.h"
#include "Concurrent.h"
#include "ConstantMarshall.h"
#include "Exceptions.h"
#include "Util.h"
#include "TableImp.h"

#ifdef LINUX
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

char const *DEFAULT_ACTION_NAME = "cppStreamingAPI";

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
    return {arg(std::forward<T>(v))};
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

}  // namespace dolphindb

namespace dolphindb {
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
              queue(nullptr) {}
        explicit SubscribeInfo(string host, int port, string tableName, string actionName, long long offset, bool resub,
                               const VectorSP &filter, bool msgAsTable, bool allowExists, int batchSize)
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
              queue(new MessageQueue(std::max(DEFAULT_QUEUE_CAPACITY, batchSize), batchSize)){}

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

        MessageQueueSP queue;
    };

    struct KeepAliveAttr {
        int enabled = 1;             // default = 1 enabled
        int idleTime = 30;           // default = 30s idle time will trigger detection
        int interval = 5;            // default = 5s/detection
        int count = 3;               // default = 3 unsuccessful detections mean disconnected
    };

public:
    explicit StreamingClientImpl(int listeningPort) : listeningPort_(listeningPort) {
#ifdef WINDOWS
        if (!WSAStarted && startWSA()) {
            throw RuntimeException("Can't start WSA");
        }
        WSAStarted = true;
#endif
        DBConnection::initialize();
        listenerSocket_ = new Socket("", listeningPort, true, 30);
        if (listenerSocket_->bind() != OK) {
            throw RuntimeException("Failed to bind the socket on port " + Util::convert(listeningPort_) +
                                   ". Couldn't start the subscription daemon.");
        }
        if (listenerSocket_->listen() != OK) {
            throw RuntimeException("Failed to listen the socket on port " + Util::convert(listeningPort_) +
                                   ". Couldn't start the subscription daemon.");
        }
        reconnectThread_ = new Thread(new Executor(std::bind(&StreamingClientImpl::reconnect, this)));
        reconnectThread_->start();
        daemonThread_ = new Thread(new Executor(std::bind(&StreamingClientImpl::daemon, this)));
        daemonThread_->start();
    }
    ~StreamingClientImpl() {
        listenerSocket_->close();
    }
    MessageQueueSP subscribeInternal(const string &host, int port, const string &tableName,
                                     const string &actionName = DEFAULT_ACTION_NAME, int64_t offset = -1,
                                     bool resubscribe = true, const VectorSP &filter = nullptr, bool msgAsTable = false,
                                     bool allowExists = false, int batchSize  = 1);
    string subscribeInternal(DBConnection &conn, SubscribeInfo &info);
    void insertMeta(SubscribeInfo &info, const string &topic);
    void delMeta(const string &topic);
    void unsubscribeInternal(const string &host, int port, const string &tableName,
                             const string &actionName = DEFAULT_ACTION_NAME);

private:
    void parseMessage(SocketSP socket);
    [[noreturn]] void reconnect();

private:
    [[noreturn]] void daemon() {
        while (true) {
            try {
                SocketSP socket = listenerSocket_->accept();
                if (socket.isNull()) {
                    cerr << "Streaming Daemon socket accept failed, aborting." << endl;
                    break;
                };

#ifdef WINDOWS
                if(::setsockopt(socket->getHandle(), SOL_SOCKET, SO_KEEPALIVE, (const char*)&keepAliveAttr_.enabled, sizeof(int)) != 0) {
                    cerr << "Subscription socket failed to enable TCP_KEEPALIVE with error: " <<  errno << endl;
                    break;
                }
#else
                if(::setsockopt(socket->getHandle(), SOL_SOCKET, SO_KEEPALIVE, &keepAliveAttr_.enabled, sizeof(int)) != 0) {
                    cerr << "Subscription socket failed to enable TCP_KEEPALIVE with error: " << errno << endl;
                }
                ::setsockopt(socket->getHandle(), SOL_TCP, TCP_KEEPIDLE, &keepAliveAttr_.idleTime, sizeof(int));
                ::setsockopt(socket->getHandle(), SOL_TCP, TCP_KEEPINTVL, &keepAliveAttr_.interval, sizeof(int));
                ::setsockopt(socket->getHandle(), SOL_TCP, TCP_KEEPCNT, &keepAliveAttr_.count, sizeof(int));
#endif

                ThreadSP t = new Thread(new Executor(std::bind(&StreamingClientImpl::parseMessage, this, socket)));
                t->start();
                parseThreads_.emplace_back(t);
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
    }
    string getLocalHostname(string remoteHost, int remotePort) {
        int attempt = 0;
        while (true) {
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

    string getLocalIP(const SubscribeInfo &info) { return getLocalIP(info.host, info.port); }

    string getLocalIP(const string &host, int port) {
        if (localIP_.empty()) localIP_ = getLocalHostname(host, port);
        return localIP_;
    }

    bool getNewLeader(const string& s, string &host, int &port) {
        string msg{s};
        if (msg.substr(0, 11) == "<NotLeader>") {
            msg = msg.substr(11);
            auto v = Util::split(msg, ':');
            host = v[0];
            port = std::stoi(v[1]);
            return true;
        } else {
            return false;
        }
    }

private:
    SocketSP listenerSocket_;
    ThreadSP daemonThread_;
    ThreadSP reconnectThread_;
    KeepAliveAttr keepAliveAttr_;
    vector<ThreadSP> parseThreads_;
    int listeningPort_;
    string localIP_;
    Hashmap<string, SubscribeInfo> topicSubInfos_;
    Hashmap<string, int> actionCntOnTable_;
    Hashmap<string, set<string>> liveSubsOnSite_;  // living site -> topic
    Hashmap<string, pair<long long, long long>> siteReconn_;
    Hashmap<string, pair<long long, long long>> topicReconn_;
    Mutex mtx_;
    std::queue<SubscribeInfo> initResub_;
#ifdef WINDOWS
    static bool WSAStarted_;
    static void WSAStart();
#endif
};

[[noreturn]] void StreamingClientImpl::reconnect() {
    const int reconnect_timeout = 3000;  // reconn every 3s
    while (true) {
        siteReconn_.op([&](unordered_map<string, pair<long long, long long>> &mp) {
            for (auto &p : mp) {
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
                        run(conn, "activeClosePublishConnection", getLocalIP(host, port), listeningPort_, true);
                    } else {
                        run(conn, "activeClosePublishConnection", getLocalIP(host, port), listeningPort_);
                    }

                } catch (exception &e) {
                    cerr << "#attempt= " << p.second.first << "activeClosePublishConnection on site got an exception "
                         << e.what() << ", site: " << host << ":" << port << endl;
                }
                p.second.first = Util::getEpochTime();
                ++p.second.second;
            }
        });

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
                    try {
                        auto conn = buildConn(host, port);
                        newTopic = subscribeInternal(conn, info);
                        if (newTopic != topic) {
                            delMeta(topic);
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

        {
            LockGuard<Mutex> _(&mtx_);
            vector<SubscribeInfo> v;
            while (!initResub_.empty()) {
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

            for (auto &i : v) {
                initResub_.push(i);
            }
        }

        Util::sleep(1000);
    }
}

void StreamingClientImpl::parseMessage(SocketSP socket) {
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

    while (true) {
        if (ret != OK) {  // blocking mode, ret won't be NODATA
            if (!actionCntOnTable_.count(aliasTableName) || actionCntOnTable_[aliasTableName] == 0) {
                return;
            };
            if (topicMsg.empty()) {
                cerr << "WARNING: ERROR occured before receiving first message, can't do recovery." << endl;
                return;
            }
            // close this socket, and do resub
            in->close();
            socket.clear();

            auto site = getSite(topics[0]);
            set<string> ts;
            if (liveSubsOnSite_.find(site, ts)) {
                siteReconn_.insert(site, {Util::getEpochTime() + 3000, 0});
                for (auto &t : ts) {
                    topicReconn_.insert(t, {Util::getEpochTime(), 0});
                }
            }
            return;
        }

        char littleEndian;
        ret = in->readChar(littleEndian);
        if (ret != OK) continue;

        ret = in->bufferBytes(16);
        if (ret != OK) continue;

        in->readLong(sentTime);
        in->readLong(offset);
//        cout << offset << endl;

        ret = in->readString(topicMsg);
        topics = Util::split(topicMsg, ',');
        aliasTableName = stripActionName(topics[0]);

        if (ret != OK) continue;

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
            vector<VectorSP> cache;

            for (auto &t : topics) {
                SubscribeInfo info;
                if (topicSubInfos_.find(t, info)) {
                    if (info.queue.isNull()) continue;
                    if (info.msgAsTable) {
                        if(info.attributes.empty()){
                            std::cerr << "table colName is empty, can not convert to table" << std::endl;
                            info.queue->push(obj);
                        }else{
                            info.queue->push(convertTupleToTable(info.attributes, obj));
                        }
                    } else {
                        if(UNLIKELY(cache.empty())) { // split once
                            cache.resize(rowSize);
                            for (int rowIdx = 0; rowIdx < rowSize; ++rowIdx) {
                                VectorSP tmp = Util::createVector(DT_ANY, colSize, colSize);
                                for (int colIdx = 0; colIdx < colSize; ++colIdx) {
                                    tmp->set(colIdx, obj->get(colIdx)->get(rowIdx));
                                }
                                cache[rowIdx] = tmp;
                            }
                        }
                        for (int rowIdx = 0; rowIdx < rowSize; ++rowIdx) { info.queue->push(cache[rowIdx]); }
                    }
                    topicSubInfos_.op([&](unordered_map<string, SubscribeInfo>& mp){
                        mp[t].offset = offset + 1;
//                        cout << "set offset to " << offset << " add: " << &mp[t].offset << endl;
                    });
//                    topicSubInfos_.upsert(
//                        t, [&](SubscribeInfo &info) { info.offset = offset; }, SubscribeInfo());
                }
            }
        } else {
            throw RuntimeException("Message body has an invalid format. Vector is expected.");
        }
    }
}

string StreamingClientImpl::subscribeInternal(DBConnection &conn, SubscribeInfo &info) {
    ConstantSP result = run(conn, "getSubscriptionTopic", info.tableName, info.actionName);
    auto topic = result->get(0)->getString();
    ConstantSP colLabels = result->get(1);
    if (!colLabels->isArray()) throw RuntimeException("The publisher doesn't have the table [" + info.tableName + "].");
    int colCount = colLabels->size();
    vector<string> colNames;
    colNames.reserve(colCount);
    for (int i = 0; i < colCount; ++i) colNames.push_back(colLabels->getString(i));
    info.attributes = colNames;

    ConstantSP re = run(conn, "publishTable", getLocalIP(info), listeningPort_, info.tableName, info.actionName,
                        info.offset, info.filter, info.allowExists);

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

void StreamingClientImpl::delMeta(const string &topic) {
    topicSubInfos_.erase(topic);
    liveSubsOnSite_.upsert(getSite(topic), [&](set<string> &s) { s.erase(topic); }, {});
    actionCntOnTable_.upsert(
        stripActionName(topic), [&](int &cnt) { --cnt; }, 0);
}

MessageQueueSP StreamingClientImpl::subscribeInternal(const string &host, int port, const string &tableName,
                                                      const string &actionName, int64_t offset, bool resubscribe,
                                                      const VectorSP &filter, bool msgAsTable, bool allowExists, int batchSize) {
    string topic;
    int attempt = 0;
    string _host = host;
    int _port = port;
    while (true) {
        ++attempt;
        SubscribeInfo info(_host, _port, tableName, actionName, offset, resubscribe, filter, msgAsTable, allowExists, batchSize);
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
}

void StreamingClientImpl::unsubscribeInternal(const string &host, int port, const string &tableName,
                                              const string &actionName) {
    DBConnection conn = buildConn(host, port);
    string topic = run(conn, "getSubscriptionTopic", tableName, actionName)->get(0)->getString();
    if (!topicSubInfos_.count(topic)) {
        cerr << "[WARN] subscription of topic " << topic << " not existed" << endl;
        return;
    }

    run(conn, "stopPublishTable", host, listeningPort_, tableName, actionName);

    topicSubInfos_.op([&](unordered_map<string, SubscribeInfo> &mp) { mp[topic].queue->push(nullptr); });
    delMeta(topic);
}
}  // namespace dolphindb

namespace dolphindb {
StreamingClient::StreamingClient(int listeningPort) : impl_(new StreamingClientImpl(listeningPort)) {}

StreamingClient::~StreamingClient() {}

MessageQueueSP StreamingClient::subscribeInternal(string host, int port, string tableName, string actionName,
                                                  int64_t offset, bool resubscribe, const dolphindb::VectorSP &filter,
                                                  bool msgAsTable, bool allowExists, int batchSize) {
    return impl_->subscribeInternal(host, port, tableName, actionName, offset, resubscribe, filter, msgAsTable,
                                    allowExists, batchSize);
}

void StreamingClient::unsubscribeInternal(string host, int port, string tableName, string actionName) {
    impl_->unsubscribeInternal(std::move(host), port, std::move(tableName), std::move(actionName));
}

/// ThreadedClient impl
ThreadedClient::ThreadedClient(int listeningPort) : StreamingClient(listeningPort) {}

ThreadSP ThreadedClient::subscribe(string host, int port, const MessageBatchHandler &handler, string tableName,
                                   string actionName, int64_t offset, bool resub, const VectorSP &filter,
                                   bool allowExists, int batchSize, double throttle) {
    MessageQueueSP queue = subscribeInternal(std::move(host), port, std::move(tableName), std::move(actionName), offset,
                                             resub, filter, false, false, batchSize);
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
    ThreadSP t = new Thread(new Executor([handler, queue, throttleTime]() {
        vector<Message> msgs;
        while (true) {       
            if(queue->pop(msgs, throttleTime)){
                for(size_t i = 0; i < msgs.size(); i++){
                    if (UNLIKELY(msgs[i].isNull())) {
                        if(LIKELY(i == msgs.size() - 1 && i != 0)){
                            msgs.pop_back();
                            handler(msgs);
                        }
                        return;
                    }
                }
                handler(msgs);
            }
        }
    }));
    t->start();
    return t;
}


ThreadSP ThreadedClient::subscribe(string host, int port, const MessageHandler &handler, string tableName,
                                   string actionName, int64_t offset, bool resub, const VectorSP &filter,
                                   bool msgAsTable, bool allowExists) {
    MessageQueueSP queue = subscribeInternal(std::move(host), port, std::move(tableName), std::move(actionName), offset,
                                             resub, filter, msgAsTable);
    if (queue.isNull()) {
        cerr << "Subscription already made, handler loop not created." << endl;
        ThreadSP t = new Thread(new Executor([]() {}));
        t->start();
        return t;
    }

    ThreadSP t = new Thread(new Executor([handler, queue]() {
        Message msg;
        while (true) {
            queue->pop(msg);
            // quit handler loop if msg is nullptr
            if (UNLIKELY(msg.isNull())) break;
            handler(msg);
        }
    }));
    t->start();
    return t;
}

void ThreadedClient::unsubscribe(string host, int port, string tableName, string actionName) {
    unsubscribeInternal(std::move(host), port, std::move(tableName), std::move(actionName));
}

/// PollingClient IMPL
PollingClient::PollingClient(int listeningPort) : StreamingClient(listeningPort) {}

MessageQueueSP PollingClient::subscribe(string host, int port, string tableName, string actionName, int64_t offset,
                                        bool resub, const VectorSP &filter, bool msgAsTable, bool allowExists) {
    return subscribeInternal(std::move(host), port, std::move(tableName), std::move(actionName), offset, resub, filter,
                             msgAsTable, allowExists);
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

vector<ThreadSP> ThreadPooledClient::subscribe(string host, int port, const MessageHandler &handler, string tableName,
                                               string actionName, int64_t offset, bool resub, const VectorSP &filter,
                                               bool msgAsTable, bool allowExists) {
    auto queue = subscribeInternal(std::move(host), port, std::move(tableName), std::move(actionName), offset, resub,
                                   filter, msgAsTable, allowExists);
    vector<ThreadSP> ret;
    for (int i = 0; i < threadCount_; ++i) {
        ThreadSP t = new Thread(new Executor([handler, queue]() {
            Message msg;
            while (true) {
                queue->pop(msg);
                if (UNLIKELY(msg.isNull())) {
                    // quit handler loop, push nullptr back so that other
                    // handler loop could quit as well
                    queue->push(nullptr);
                    break;
                }
                handler(msg);
            }
        }));
        t->start();
        ret.emplace_back(t);
    }
    return ret;
}

}  // namespace dolphindb
