#pragma once

#include "ConstantImp.h"
#include <string>
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

class DBConnectionImpl {
public:
    DBConnectionImpl(bool sslEnable = false, bool asynTask = false, int keepAliveTime = 7200, bool compress = false, bool python = false, bool isReverseStreaming = false);
    ~DBConnectionImpl();
    bool connect(const string& hostName, int port, const string& userId = "", const string& password = "",bool sslEnable = false, bool asynTask = false, int keepAliveTime = -1, bool compress= false, bool python = false);
    void login(const string& userId, const string& password, bool enableEncryption);
    ConstantSP run(const string& script, int priority = 4, int parallelism = 2, int fetchSize = 0, bool clearMemory = false, long seqNum = 0);
    ConstantSP run(const string& funcName, vector<ConstantSP>& args, int priority = 4, int parallelism = 2, int fetchSize = 0, bool clearMemory = false, long seqNum = 0);
    ConstantSP upload(const string& name, const ConstantSP& obj);
    ConstantSP upload(vector<string>& names, vector<ConstantSP>& objs);
    void close();
    bool isConnected() { return isConnected_; }
    void getHostPort(string &host, int &port) { host = hostName_; port = port_; }
    void setClientId(const std::string& clientId){runClientId_ = clientId;}
    DataInputStreamSP getDataInputStream(){return inputStream_;}
private:
    long generateRequestFlag(bool clearSessionMemory = false, bool disablepickle = false, bool pickleTableToList = false);
    ConstantSP run(const string& script, const string& scriptType, vector<ConstantSP>& args, int priority = 4, int parallelism = 2,int fetchSize = 0, bool clearMemory = false, long seqNum = 0);
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
    bool isReverseStreaming_;
    string runClientId_;
    DataInputStreamSP inputStream_;
};

}