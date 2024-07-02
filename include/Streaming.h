#ifndef _STREAMING_H_
#define _STREAMING_H_

#include <string>
#include <vector>
#include "SharedMem.h"
#include "EventHandler.h"
#include "StreamingUtil.h"
namespace dolphindb {
class DBConnection;
class StreamingClientImpl;

class EXPORT_DECL StreamingClient {
public:
	//listeningPort > 0 : listen mode, wait for server connection
	//listeningPort = 0 : active mode, connect server by DBConnection socket
	explicit StreamingClient(int listeningPort);
    virtual ~StreamingClient();
	bool isExit();
	void exit();

protected:
    MessageQueueSP subscribeInternal(std::string host, int port, std::string tableName, std::string actionName = DEFAULT_ACTION_NAME,
                                     int64_t offset = -1, bool resubscribe = true, const VectorSP &filter = nullptr,
                                     bool msgAsTable = false, bool allowExists = false, int batchSize  = 1,
									 std::string userName="", std::string password="",
									 const StreamDeserializerSP &blobDeserializer = nullptr, const std::vector<std::string>& backupSites = std::vector<std::string>(), bool isEvent = false, int resubTimeout = 100, bool subOnce = false);
    void unsubscribeInternal(std::string host, int port, std::string tableName, std::string actionName = DEFAULT_ACTION_NAME);

protected:
    SmartPointer<StreamingClientImpl> impl_;
};

class EXPORT_DECL EventClient : public StreamingClient{
public:
    EventClient(const std::vector<EventSchema>& eventSchema, const std::vector<std::string>& eventTimeFields, const std::vector<std::string>& commonFields);
    ThreadSP subscribe(const std::string& host, int port, const EventMessageHandler &handler, const std::string& tableName, const std::string& actionName = DEFAULT_ACTION_NAME, int64_t offset = -1,
        bool resub = true, const std::string& userName="", const std::string& password="");
    void unsubscribe(const std::string& host, int port, const std::string& tableName, const std::string& actionName = DEFAULT_ACTION_NAME);

private:
    EventHandler      eventHandler_;
};

class EXPORT_DECL ThreadedClient : public StreamingClient {
public:
	//listeningPort > 0 : listen mode, wait for server connection
	//listeningPort = 0 : active mode, connect server by DBConnection socket
    explicit ThreadedClient(int listeningPort = 0);
    ~ThreadedClient() override = default;
    ThreadSP subscribe(std::string host, int port, const MessageHandler &handler, std::string tableName,
                       std::string actionName = DEFAULT_ACTION_NAME, int64_t offset = -1, bool resub = true,
                       const VectorSP &filter = nullptr, bool msgAsTable = false, bool allowExists = false,
						std::string userName="", std::string password="",
					   const StreamDeserializerSP &blobDeserializer = nullptr, const std::vector<std::string>& backupSites = std::vector<std::string>(),int resubTimeout = 100, bool subOnce = false);
    ThreadSP subscribe(std::string host, int port, const MessageBatchHandler &handler, std::string tableName,
                       std::string actionName = DEFAULT_ACTION_NAME, int64_t offset = -1, bool resub = true,
                       const VectorSP &filter = nullptr, bool allowExists = false, int batchSize = 1,
						double throttle = 1,bool msgAsTable = false,
						std::string userName = "", std::string password = "",
						const StreamDeserializerSP &blobDeserializer = nullptr, const std::vector<std::string>& backupSites = std::vector<std::string>(),int resubTimeout = 100,bool subOnce = false);
	size_t getQueueDepth(const ThreadSP &thread);
    void unsubscribe(std::string host, int port, std::string tableName, std::string actionName = DEFAULT_ACTION_NAME);
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
							   const StreamDeserializerSP &blobDeserializer = nullptr, const std::vector<std::string>& backupSites = std::vector<std::string>(), int resubTimeout = 100, bool subOnce = false);
    void unsubscribe(std::string host, int port, std::string tableName, std::string actionName = DEFAULT_ACTION_NAME);
	size_t getQueueDepth(const ThreadSP &thread);

private:
    int threadCount_;
};

class EXPORT_DECL PollingClient : public StreamingClient {
public:
	//listeningPort > 0 : listen mode, wait for server connection
	//listeningPort = 0 : active mode, connect server by DBConnection socket
    explicit PollingClient(int listeningPort = 0);
    ~PollingClient() override = default;
    MessageQueueSP subscribe(std::string host, int port, std::string tableName, std::string actionName = DEFAULT_ACTION_NAME,
                             int64_t offset = -1, bool resub = true, const VectorSP &filter = nullptr,
                             bool msgAsTable = false, bool allowExists = false,
							std::string userName="", std::string password="",
							 const StreamDeserializerSP &blobDeserializer = nullptr, const std::vector<std::string>& backupSites = std::vector<std::string>(), int resubTimeout = 100, bool subOnce = false);
    void unsubscribe(std::string host, int port, std::string tableName, std::string actionName = DEFAULT_ACTION_NAME);
};

#ifdef LINUX

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
#endif  // _STREAMING_H_