#include "DolphinDBImp.h"
#include "DolphinDB.h"
#include "Logger.h"
#include "ConstantMarshall.h"
#include "internal/Crypto.h"

#define APIMinVersionRequirement 300

using std::string;
namespace dolphindb {

DdbInit DBConnectionImpl::ddbInit_;
DBConnectionImpl::DBConnectionImpl(bool sslEnable, bool asynTask, int keepAliveTime, bool compress, bool python, bool isReverseStreaming)
    : port_(0), encrypted_(false), isConnected_(false), littleEndian_(Util::isLittleEndian()), sslEnable_(sslEnable),asynTask_(asynTask)
    , keepAliveTime_(keepAliveTime), compress_(compress), enablePickle_(false), python_(python), isReverseStreaming_(isReverseStreaming)
{
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
    close();

    SocketSP conn = new Socket(hostName_, port_, true, keepAliveTime_, sslEnable_);
    IO_ERR ret = conn->connect();
    if (ret != OK) {
        return false;
    }

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

    std::vector<string> headers;
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
    inputStream_ = new DataInputStream(conn_);
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
    if (encrypted_) {
#ifdef USE_OPENSSL
        std::vector<ConstantSP> args;
        std::string publicKey = run("getDynamicPublicKey", args)->getString();
        if (publicKey.empty()) {
            throw RuntimeException("Failed to obtain RSA public key from server.");
        }
        Crypto ssl(publicKey);
        userId_ = ssl.RSAEncrypt(userId);
        pwd_ = ssl.RSAEncrypt(password);
#else
        throw RuntimeException("Encrypted login is unavailble without OpenSSL.");
#endif
    }
    login();
}

void DBConnectionImpl::login() {
    std::vector<ConstantSP> args;
    args.push_back(new String(userId_));
    args.push_back(new String(pwd_));
    args.push_back(new Bool(encrypted_));
    ConstantSP result = run("login", args);
    if (!result->getBool())
        throw IOException("Failed to authenticate the user " + userId_);
}

ConstantSP DBConnectionImpl::run(const string& script, int priority, int parallelism, int fetchSize, bool clearMemory, long seqNum) {
    std::vector<ConstantSP> args;
    return run(script, "script", args, priority, parallelism, fetchSize, clearMemory, seqNum);
}

ConstantSP DBConnectionImpl::run(const string& funcName, std::vector<ConstantSP>& args, int priority, int parallelism, int fetchSize, bool clearMemory, long seqNum) {
    return run(funcName, "function", args, priority, parallelism, fetchSize, clearMemory, seqNum);
}

ConstantSP DBConnectionImpl::upload(const string& name, const ConstantSP& obj) {
    if (!Util::isVariableCandidate(name))
        throw RuntimeException(name + " is not a qualified variable name.");
    std::vector<ConstantSP> args(1, obj);
    return run(name, "variable", args);
}

ConstantSP DBConnectionImpl::upload(std::vector<string>& names, std::vector<ConstantSP>& objs) {
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
        flag += 4;
    }
    if (clearSessionMemory){
        flag += 16;
    }
    if (enablePickle_ == false || disablepickle) {
        if (compress_)
            flag += 64;
    }
    else {//enable pickle
        flag += 8;
        if (pickleTableToList) {
            flag += (1 << 15);
        }
    }
    if (python_){
        flag += 2048;
    }
    if(isReverseStreaming_){
        flag += 131072;
    }
    return flag;
}

ConstantSP DBConnectionImpl::run(const string& script, const string& scriptType, std::vector<ConstantSP>& args,
            int priority, int parallelism, int fetchSize, bool clearMemory, long seqNum) {
    if (!isConnected_)
        throw IOException("Couldn't send script/function to the remote host because the connection has been closed");

    if(fetchSize < 8192 && fetchSize != 0)
        throw IOException("fetchSize must be greater than 8192 and not less than 0");
    string body;
    size_t argCount = args.size();
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
    out.append("__" + std::to_string(fetchSize));
    if(!runClientId_.empty() && seqNum != 0){
        out.append(std::string("__").append(runClientId_).append("_").append(std::to_string(seqNum)));
    }
    out.append(1, '\n');
    out.append(body);

    IO_ERR ret;
    if (argCount > 0) {
        for (size_t i = 0; i < argCount; ++i) {
            if (args[i]->containNotMarshallableObject()) {
                throw IOException("The function argument or uploaded object is not marshallable.");
            }
        }
        DataOutputStreamSP outStream = new DataOutputStream(conn_);
        ConstantMarshallFactory marshallFactory(outStream);
        bool enableCompress = false;
        for (size_t i = 0; i < argCount; ++i) {
            enableCompress = (args[i]->getForm() == DATA_FORM::DF_TABLE) ? compress_ : false;
            ConstantMarshall* marshall = marshallFactory.getConstantMarshall(args[i]->getForm());
            if (i == 0)
                marshall->start(out.c_str(), out.size(), args[i], true, enableCompress, ret);
            else
                marshall->start(args[i], true, enableCompress, ret);
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
        ret = conn_->write(out.c_str(), out.size(), actualLength);
        if (ret != OK) {
            close();
            throw IOException("Couldn't send script/function to the remote host because the connection has been closed, IO error type " + std::to_string(ret));
        }
    }
    
    if(asynTask_)
        return new Void();
    
    if (littleEndian_ != (char)Util::isLittleEndian())
        inputStream_->enableReverseIntegerByteOrder();

    string line;
    if ((ret = inputStream_->readLine(line)) != OK) {
        close();
        throw IOException("Failed to read response header from the socket with IO error type " + std::to_string(ret));
    }
    while (line == "MSG") {
        if ((ret = inputStream_->readString(line)) != OK) {
            close();
            throw IOException("Failed to read response msg from the socket with IO error type " + std::to_string(ret));
        }
        std::cout << line << std::endl;
        if ((ret = inputStream_->readLine(line)) != OK) {
            close();
            throw IOException("Failed to read response header from the socket with IO error type " + std::to_string(ret));
        }
    }
    std::vector<string> headers;
    Util::split(line.c_str(), ' ', headers);
    if (headers.size() != 3) {
        close();
        throw IOException("Received invalid header");
    }
    sessionId_ = headers[0];
    int numObject = atoi(headers[1].c_str());

    if ((ret = inputStream_->readLine(line)) != OK) {
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
    if ((ret = inputStream_->readShort(flag)) != OK) {
        close();
        throw IOException("Failed to read object flag from the socket with IO error type " + std::to_string(ret));
    }
    
    DATA_FORM form = static_cast<DATA_FORM>(flag >> 8);
    DATA_TYPE type = static_cast<DATA_TYPE >(flag & 0xff);
    if(fetchSize > 0 && form == DF_VECTOR && type == DT_ANY)
        return new BlockReader(inputStream_);
    ConstantUnmarshallFactory factory(inputStream_);
    ConstantUnmarshall* unmarshall = factory.getConstantUnmarshall(form);
    if(unmarshall == NULL){
        DLogger::Error("Unknow incoming object form",form,"of type",type);
        inputStream_->reset(0);
        conn_->skipAll();
        return Constant::void_;
    }
    if (!unmarshall->start(flag, true, ret)) {
        unmarshall->reset();
        close();
        throw IOException("Failed to parse the incoming object with IO error type " + std::to_string(ret));
    }

    ConstantSP result = unmarshall->getConstant();
    unmarshall->reset();
    return result;
}

}