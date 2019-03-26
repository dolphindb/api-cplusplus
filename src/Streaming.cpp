#include "Streaming.h"
#include "ConstantMarshall.h"
#include "Exceptions.h"
#include "Util.h"

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
#define sleep(x) Sleep((x)*1000)
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

namespace dolphindb {
using std::cerr;
using std::cout;
using std::endl;

StreamingClient::StreamingClient(int listeningPort) : listeningPort_(listeningPort) {
#ifdef WINDOWS
    if (!WSAStarted && startWSA()) {
        throw RuntimeException("Can't start WSA");
    }
    WSAStarted = true;
#endif
    listenerSocket_ = new Socket("", listeningPort, true);
    if (listenerSocket_->bind() != OK) {
        throw RuntimeException("Failed to bind the socket on port " + Util::convert(listeningPort_) +
                               ". Couldn't start the subscription daemon.");
    }

    if (listenerSocket_->listen() != OK) {
        throw RuntimeException("Failed to listen the socket on port " + Util::convert(listeningPort_) +
                               ". Couldn't start the subscription daemon.");
    }
#ifdef DEBUG
    cerr << "client listening on: " << listeningPort << endl;
#endif
    daemonThread_ = new Thread(new Executor(std::bind(&StreamingClient::daemon, this)));
    daemonThread_->start();
}

StreamingClient::~StreamingClient() { listenerSocket_->close(); }

void StreamingClient::daemon() {
    try {
        int i = 0;
        while (true) {
            SocketSP socket = listenerSocket_->accept();
            if (socket.isNull()) continue;
            ThreadSP t = new Thread(new Executor(std::bind(&StreamingClient::parseMessage, this, socket)));
            t->start();
            parseThreads_.emplace_back(t);
        }
    } catch (exception& e) {
        cerr << "Daemon exception: " << e.what() << endl;
        cerr << "Restart Daemon in 1 second" << endl;
        sleep(1);
        daemon();
    }
}

void StreamingClient::parseMessage(SocketSP socket) {
    DataInputStreamSP in = new DataInputStream(socket);
    auto factory = ConstantUnmarshallFactory(in);
    ConstantUnmarshall* unmarshall = nullptr;

    IO_ERR ret = OK;
    long long sentTime;
    long long offset = -1;
    short previousFlag = 0x7fff;
    // Have no idea which topic it is parsing until one message came in
    string aliasTableName;
    string topicMsg;
    vector<string> topics;

    while (true) {
        if (ret != OK) {  // blocking mode, ret won't be NODATA
            LockGuard<Mutex> lock(&actionCntOnTableMutex_);
            if (actionCntOnTable_[aliasTableName] == 0) return;
            lock.unlock();
            if (topicMsg.empty()) {
                cerr << "WARNING: ERROR occured before receiving first message, can't do recovery." << endl;
            }
            return resubscribe(getSite(topics[0]));
        }

        char littleEndian;
        ret = in->readChar(littleEndian);
        if (ret != OK) continue;

        ret = in->bufferBytes(16);
        if (ret != OK) continue;

        in->readLong(sentTime);
        in->readLong(offset);

        ret = in->readString(topicMsg);
        topics = Util::split(topicMsg, ',');
        aliasTableName = stripActionName(topics[0]);

        if (ret != OK) continue;

        short flag;
        ret = in->readShort(flag);
        if (ret != OK) continue;

        if(flag != previousFlag){
            DATA_FORM form = static_cast<DATA_FORM>(flag >> 8);
            unmarshall = factory.getConstantUnmarshall(form);
            if (unmarshall == nullptr) throw RuntimeException("Invalid data form: " + std::to_string(form));
            previousFlag = flag;
        }

        unmarshall->start(flag, true, ret);
        if (ret != OK) continue;

        ConstantSP obj = unmarshall->getConstant();
        if(obj->isTable()) {
            assert(obj->rows() == 0);
        } else if (LIKELY(obj->isVector())) {
            int colSize = obj->size();
            int rowSize = obj->get(0)->size();
            offset += rowSize;
            vector<VectorSP> cache(rowSize);
            if (rowSize == 1) {
                cache[0] = obj;
            } else {
                for (int rowIdx = 0; rowIdx < rowSize; ++rowIdx) {
                    VectorSP tmp = Util::createVector(DT_ANY, colSize, colSize);
                    for (int colIdx = 0; colIdx < colSize; ++colIdx) {
                        tmp->set(colIdx, obj->get(colIdx)->get(rowIdx));
                    }
                    cache[rowIdx] = tmp;
                }
            }

            LockGuard<Mutex> lk(&queueMutex_);
            for (auto& t : topics) {
                if (LIKELY(queues_.count(t) && !queues_[t].isNull())) {
                    auto& queue = queues_[t];
                    for (int rowIdx = 0; rowIdx < rowSize; ++rowIdx) queue->push(cache[rowIdx]);
                    LockGuard<Mutex> saveSubLock(&saveSubMutex_);
                    saveSub_[t].offset = offset;
                }
            }
        } else {
            throw RuntimeException("Message body has an invalid format. Vector is expected.");
        }
    }
}

string StreamingClient::getLocalHostname(string remoteHost, int remotePort) {
    try {
#ifdef LINUX
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

        if (connect(sockfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
            throw RuntimeException("Error in getLocalHostName: can't onnect to server.");
        }

        bzero(&my_addr, sizeof(my_addr));
        socklen_t len = sizeof(my_addr);
        getsockname(sockfd, (struct sockaddr*)&my_addr, &len);
        inet_ntop(AF_INET, &my_addr.sin_addr, myIP, sizeof(myIP));

        close(sockfd);
        return string(myIP);
#elif WINDOWS
        WSADATA wsaData;
        SOCKET SendingSocket;
        SOCKADDR_IN ServerAddr, ThisSenderInfo;
        if ((SendingSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) == INVALID_SOCKET) {
            throw RuntimeException("Client: socket() failed!");
        }

        ServerAddr.sin_family = AF_INET;
        ServerAddr.sin_port = htons(remotePort);
        ServerAddr.sin_addr.s_addr = inet_addr(remoteHost.c_str());
        if (connect(SendingSocket, (SOCKADDR*)&ServerAddr, sizeof(ServerAddr)) != 0) {
            throw RuntimeException("Client: connect() failed!");
        }

        memset(&ThisSenderInfo, 0, sizeof(ThisSenderInfo));
        int nlen = sizeof(ThisSenderInfo);
        if (getsockname(SendingSocket, (SOCKADDR*)&ThisSenderInfo, &nlen) != 0) {
            throw RuntimeException("ERROR in getsockname with error code: " + std::to_string(WSAGetLastError()));
        }
        shutdown(SendingSocket, SD_SEND);
        closesocket(SendingSocket);
        return string(inet_ntoa(ThisSenderInfo.sin_addr));
#endif
    } catch (RuntimeException& e) {
        cerr << e.what() << " Retry getLocalHostName in 1 second." << endl;
        sleep(1);
        return getLocalHostname(remoteHost, remotePort);
    }
}

MessageQueueSP StreamingClient::subscribeInternal(
    string host, int port, string tableName, string actionName, int64_t offset, bool resubscribe) {
    LockGuard<Mutex> subunsubLock(&subUnsubMutex_);
    string topic;
    DBConnection::initialize();
    DBConnection conn;

    try {
        conn.connect(host, port);

        vector<ConstantSP> args;
        args.emplace_back(Util::createString(tableName));
        args.emplace_back(Util::createString(actionName));
        ConstantSP re = conn.run("getSubscriptionTopic", args);
        topic = re->get(0)->getString();

    } catch (IOException& e) {
        cerr << "ERROR occured in subscribeInternal: " << e.what() << ", retry in 1 second." << endl;
        conn.close();
        subunsubLock.unlock();
        sleep(1);
        return subscribeInternal(host, port, tableName, actionName, offset, resubscribe);
    }

    try {
        vector<ConstantSP> args;

        localIP_ = getLocalHostname(host, port);
        // assuming localIP won't change
        if (localIP_.empty()) localIP_ = getLocalHostname(host, port);
        args.emplace_back(Util::createString(localIP_));
        args.emplace_back(Util::createInt(listeningPort_));
        args.emplace_back(Util::createString(tableName));
        args.emplace_back(Util::createString(actionName));
        if (offset != -1) {
            args.emplace_back(Util::createLong(offset));
        }

        // shouldn't change queue_ if failed to sub
        LockGuard<Mutex> lock(&queueMutex_);
        if (!resubscribe) {
            assert(!queues_.count(topic));
            increase(topic);

            queues_[topic] = new MessageQueue(DEFAULT_QUEUE_CAPACITY);
            assert(!queues_[topic].isNull());

            LockGuard<Mutex> saveSubLock(&saveSubMutex_);  // no dead lock when param resub is true
            saveSub_[topic].host = host;
            saveSub_[topic].port = port;
            saveSub_[topic].tableName = tableName;
            saveSub_[topic].actionName = actionName;
        }

        ConstantSP re = conn.run("publishTable", args);
        LockGuard<Mutex> liveLock(&liveSubsOnSiteMutex_);
        liveSubsOnSite_.emplace(getSite(topic), topic);
        conn.close();

        return queues_[topic];
    } catch (IOException& e) {
        conn.close();
        cerr << "ERROR in subscribeInternal err: " << e.what() << endl;

        // fixme: dangerous operation
        if (string(e.what()).find("already made") != string::npos) {
            cerr << "subscription already made, ignored." << endl;
            return nullptr;
        }

        cerr << "Retry subscribe in 1 second" << endl;
        subunsubLock.unlock();
        sleep(1);
        return subscribeInternal(host, port, tableName, actionName, offset, resubscribe);
    }
}

void StreamingClient::unsubscribeInternal(string host, int port, string tableName, string actionName) {
    LockGuard<Mutex> subunsubLock(&subUnsubMutex_);
    DBConnection conn;
    string topic;
    try {
        if (!conn.connect(host, port)) {
            subunsubLock.unlock();
            cerr << "Failed to unsubscribe: unable to connect to " + host + " " + std::to_string(port)
                 << ", Retry in 1 second." << endl;
            conn.close();
            sleep(1);
            return unsubscribeInternal(host, port, tableName, actionName);
        }

        vector<ConstantSP> args;
        args.emplace_back(Util::createString(tableName));
        args.emplace_back(Util::createString(actionName));
        ConstantSP re = conn.run("getSubscriptionTopic", args);
        topic = re->get(0)->getString();
    } catch (IOException& e) {
        subunsubLock.unlock();
        cerr << "Failed in unsubscribeInternal: " << e.what() << " Retry in 1 second." << endl;
        sleep(1);
        return unsubscribeInternal(host, port, tableName, actionName);
    }

    LockGuard<Mutex> lk(&queueMutex_);
    if (!queues_.count(topic)) {
        return;
    }
    lk.unlock();

    try {
        vector<ConstantSP> args;
        args.emplace_back(Util::createString(localIP_));
        args.emplace_back(Util::createInt(listeningPort_));
        args.emplace_back(Util::createString(tableName));
        args.emplace_back(Util::createString(actionName));
        conn.run("stopPublishTable", args);
        conn.close();
    } catch (IOException& e) {
        subunsubLock.unlock();
        cerr << "Failed in unsubscribeInternal: " << e.what() << " Retry in 1 second." << endl;
        sleep(1);
        return unsubscribeInternal(host, port, tableName, actionName);
    }

    LockGuard<Mutex> liveLock(&liveSubsOnSiteMutex_);
    bool found = false;
    auto range = liveSubsOnSite_.equal_range(getSite(topic));
    for (auto it = range.first; it != range.second; ++it) {
        if (it->second == topic) {
            liveSubsOnSite_.erase(it);
            found = true;
            break;
        }
    }
    assert(found);
    liveLock.unlock();

    // handler threads will stop on NULL messages
    LockGuard<Mutex> lk2(&queueMutex_);
    assert(queues_.count(topic));
    if (!queues_[topic].isNull()) queues_[topic]->push(nullptr);
    queues_.erase(topic);
    decrease(topic);
}

void StreamingClient::increase(string topic) {
    LockGuard<Mutex> lock(&actionCntOnTableMutex_);
    auto aliasTableName = stripActionName(topic);
    actionCntOnTable_[aliasTableName]++;
}

void StreamingClient::decrease(string topic) {
    LockGuard<Mutex> lock(&actionCntOnTableMutex_);
    auto aliasTableName = stripActionName(topic);
    if (--actionCntOnTable_[aliasTableName] < 0) {
        actionCntOnTable_[aliasTableName] = 0;
    }
}

void StreamingClient::resubscribe(string site) {
    LockGuard<Mutex> liveLock(&liveSubsOnSiteMutex_);
    auto range = liveSubsOnSite_.equal_range(site);
    vector<string> topics;
    for (auto it = range.first; it != range.second; ++it) {
        topics.push_back(it->second);
    }
    liveLock.unlock();

    for (auto& topic : topics) {
        LockGuard<Mutex> saveSubLock(&saveSubMutex_);  // no dead lock when param resub is true
        auto host = saveSub_[topic].host;
        auto port = saveSub_[topic].port;
        auto tableName = saveSub_[topic].tableName;
        auto actionName = saveSub_[topic].actionName;
        auto offset = saveSub_[topic].offset;
        saveSubLock.unlock();  // unlock or deadlock
        subscribeInternal(host, port, tableName, actionName, offset, true);
    }
}

string StreamingClient::stripActionName(string topic) { return topic.substr(0, topic.find_last_of('/')); }

string StreamingClient::getSite(string topic) { return topic.substr(0, topic.find_first_of('/')); }

/// ThreadedClient impl
ThreadedClient::ThreadedClient(int listeningPort) : StreamingClient(listeningPort) {}

ThreadedClient::~ThreadedClient() = default;

ThreadSP ThreadedClient::subscribe(
    string host, int port, MessageHandler handler, string tableName, string actionName, int64_t offset) {
    MessageQueueSP queue = subscribeInternal(host, port, tableName, actionName, offset);
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
    unsubscribeInternal(host, port, tableName, actionName);
}

/// PollingClient IMPL
PollingClient::PollingClient(int listeningPort) : StreamingClient(listeningPort) {}

PollingClient::~PollingClient() = default;

MessageQueueSP PollingClient::subscribe(string host, int port, string tableName, string actionName, int64_t offset) {
    return subscribeInternal(host, port, tableName, actionName, offset);
}

void PollingClient::unsubscribe(string host, int port, string tableName, string actionName) {
    unsubscribeInternal(host, port, tableName, actionName);
}

/// ThreadPooledClient IMPL
ThreadPooledClient::ThreadPooledClient(int listeningPort, int threadCount)
    : StreamingClient(listeningPort), threadCount_(threadCount) {}

ThreadPooledClient::~ThreadPooledClient() {}

void ThreadPooledClient::unsubscribe(string host, int port, string tableName, string actionName) {
    unsubscribeInternal(host, port, tableName, actionName);
}

vector<ThreadSP> ThreadPooledClient::subscribe(
    string host, int port, MessageHandler handler, string tableName, string actionName, int64_t offset) {
    auto queue = subscribeInternal(host, port, tableName, actionName, offset);
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

string getIOERRStr(IO_ERR err) {
#define cm(x) \
    case x:   \
        return #x
    switch (err) {
        cm(OK);
        cm(DISCONNECTED);
        cm(NODATA);
        cm(NOSPACE);
        cm(TOO_LARGE_DATA);
        cm(INPROGRESS);
        cm(INVALIDDATA);
        cm(END_OF_STREAM);
        cm(READONLY);
        cm(WRITEONLY);
        cm(NOTEXIST);
        cm(CORRUPT);
        cm(NOT_LEADER);
        cm(OTHERERR);
        default:
            return "";
    }
#undef cm
    //
}

}  // namespace dolphindb
