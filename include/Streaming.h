// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include <string>
#include <vector>
#include <map>
#include <thread>
#include <mutex>
#include "SharedMem.h"
#include "EventHandler.h"
#include "StreamingUtil.h"
#include "DolphinDB.h"

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4251 )
#endif

namespace dolphindb {

class StreamingClientImpl;
class UDPStreamingImpl;

struct SubscribeQueue {
	MessageQueueSP queue_;
	std::shared_ptr<std::atomic<bool>> stopped_{nullptr};
	SubscribeQueue()
		:queue_(nullptr), stopped_(nullptr) {}
	SubscribeQueue(MessageQueueSP queue, std::shared_ptr<std::atomic<bool>> stopped)
		:queue_(queue), stopped_(stopped) {}
};

enum class SubscribeState {
    Connected,
    Disconnected,
    Resubscribing,
};

using SubscribeCallbackT = std::function<bool(const SubscribeState state, const SubscribeInfo &info)>;

struct StreamingClientConfig {
    TransportationProtocol protocol{TransportationProtocol::TCP};
    SubscribeCallbackT callback;
};

class EXPORT_DECL StreamingClient {
public:
    explicit StreamingClient(const StreamingClientConfig &config);
	//listeningPort > 0 : listen mode, wait for server connection
	//listeningPort = 0 : active mode, connect server by DBConnection socket
	explicit StreamingClient(int listeningPort);
    virtual ~StreamingClient();
	bool isExit();
	void exit();

protected:
    SubscribeQueue subscribeInternal(const SubscribeInfo &info,
                                     int64_t offset, bool resubscribe, const VectorSP &filter,
                                     bool msgAsTable, bool allowExists, int batchSize,
									 std::string userName, std::string password,
									 const StreamDeserializerSP &blobDeserializer, const std::vector<std::string>& backupSites, bool isEvent, int resubscribeInterval, bool subOnce, bool convertMsgRowData, int resubscribeTimeout);
    bool unsubscribeInternal(std::string host, int port, std::string tableName, std::string actionName = DEFAULT_ACTION_NAME);

protected:
    SmartPointer<StreamingClientImpl> impl_;
    std::shared_ptr<UDPStreamingImpl> udpImpl_;
};

class EXPORT_DECL EventClient : public StreamingClient{
public:
    EventClient(const std::vector<EventSchema>& eventSchema, const std::vector<std::string>& eventTimeFields, const std::vector<std::string>& commonFields);
    ThreadSP subscribe(const std::string& host, int port, const EventMessageHandler &handler, const std::string& tableName, const std::string& actionName = DEFAULT_ACTION_NAME, int64_t offset = -1,
        bool resub = true, const std::string& userName="", const std::string& password="", int resubscribeTimeout=0);
    bool unsubscribe(const std::string& host, int port, const std::string& tableName, const std::string& actionName = DEFAULT_ACTION_NAME);

private:
    EventHandler      eventHandler_;
};

class EXPORT_DECL ThreadedClient : public StreamingClient {
public:
    explicit ThreadedClient(const StreamingClientConfig &config) : StreamingClient(config) {}
	//listeningPort > 0 : listen mode, wait for server connection
	//listeningPort = 0 : active mode, connect server by DBConnection socket
    explicit ThreadedClient(int listeningPort = 0);
    ~ThreadedClient() override {}
    ThreadSP subscribe(std::string host, int port, const MessageHandler &handler, std::string tableName,
                       std::string actionName = DEFAULT_ACTION_NAME, int64_t offset = -1, bool resub = true,
                       const VectorSP &filter = nullptr, bool msgAsTable = false, bool allowExists = false,
						std::string userName="", std::string password="",
					   const StreamDeserializerSP &blobDeserializer = nullptr, const std::vector<std::string>& backupSites = std::vector<std::string>(),int resubscribeInterval = 100, bool subOnce = false, int resubscribeTimeout = 0);
    ThreadSP subscribe(std::string host, int port, const MessageBatchHandler &handler, std::string tableName,
                       std::string actionName = DEFAULT_ACTION_NAME, int64_t offset = -1, bool resub = true,
                       const VectorSP &filter = nullptr, bool allowExists = false, int batchSize = 1,
						double throttle = 1,bool msgAsTable = false,
						std::string userName = "", std::string password = "",
						const StreamDeserializerSP &blobDeserializer = nullptr, const std::vector<std::string>& backupSites = std::vector<std::string>(),int resubscribeInterval = 100,bool subOnce = false, int resubscribeTimeout = 0);
	size_t getQueueDepth(const ThreadSP &thread);
    bool unsubscribe(std::string host, int port, std::string tableName, std::string actionName = DEFAULT_ACTION_NAME);
};

class EXPORT_DECL ThreadPooledClient : public StreamingClient {
public:
	//listeningPort > 0 : listen mode, wait for server connection
	//listeningPort = 0 : active mode, connect server by DBConnection socket
    explicit ThreadPooledClient(int listeningPort = 0, int threadCount = 3);
    ~ThreadPooledClient() override = default;
    std::vector<ThreadSP> subscribe(std::string host, int port, const MessageHandler &handler, std::string tableName,
                               std::string actionName, int64_t offset = -1, bool resub = true,
                               const VectorSP &filter = nullptr, bool msgAsTable = false, bool allowExists = false,
								std::string userName = "", std::string password = "",
							   const StreamDeserializerSP &blobDeserializer = nullptr, const std::vector<std::string>& backupSites = std::vector<std::string>(), int resubscribeInterval = 100, bool subOnce = false, int resubscribeTimeout = 0);
    bool unsubscribe(std::string host, int port, std::string tableName, std::string actionName = DEFAULT_ACTION_NAME);
	size_t getQueueDepth(const ThreadSP &thread);

private:
    int threadCount_;
};

class EXPORT_DECL PollingClient : public StreamingClient {
public:
	explicit PollingClient(const StreamingClientConfig &config) : StreamingClient(config) {}
	//listeningPort > 0 : listen mode, wait for server connection
	//listeningPort = 0 : active mode, connect server by DBConnection socket
    explicit PollingClient(int listeningPort = 0);
    ~PollingClient() override = default;
    MessageQueueSP subscribe(std::string host, int port, std::string tableName, std::string actionName = DEFAULT_ACTION_NAME,
                             int64_t offset = -1, bool resub = true, const VectorSP &filter = nullptr,
                             bool msgAsTable = false, bool allowExists = false,
							std::string userName="", std::string password="",
							 const StreamDeserializerSP &blobDeserializer = nullptr, const std::vector<std::string>& backupSites = std::vector<std::string>(), int resubscribeInterval = 100, bool subOnce = false, int resubscribeTimeout = 0);
    bool unsubscribe(std::string host, int port, std::string tableName, std::string actionName = DEFAULT_ACTION_NAME);
};

#ifdef __linux__

class EXPORT_DECL IPCInMemoryStreamClient {
public:
	IPCInMemoryStreamClient() = default;
	~IPCInMemoryStreamClient();
	ThreadSP subscribe(const std::string& tableName, const IPCInMemoryTableReadHandler& handler, TableSP outputTable = nullptr, bool overwrite = true);
	void unsubscribe(const std::string& tableName);
private:
	struct ThreadWrapper {
		ThreadSP thread;
		bool isExit;
	};
	std::unordered_map<std::string, ThreadWrapper> tableName2thread_; // tableName -> (thread, isExit)
	ThreadSP newHandleThread(const std::string& tableName, const IPCInMemoryTableReadHandler handler,
		SmartPointer<IPCInMemTable> memTable,
		TableSP outputTable, bool overwrite);
};

#endif//LINUX

}  // namespace dolphindb

#ifdef _MSC_VER
#pragma warning( pop )
#endif
