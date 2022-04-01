/*
 * Socket.cpp
 *
 *  Created on: Mar 14, 2015
 *      Author: dzhou
 */

#if defined LINUX
	#include <unistd.h>
	#include <netdb.h>
	#include <sys/types.h>
	#include <sys/socket.h>
	#include <arpa/inet.h>
	#include <fcntl.h>
	#include <error.h>
	#define closesocket(s) ::close(s)
#else
	#undef UNICODE
	#include <winsock2.h>
	#include <windows.h>
	#include <ws2tcpip.h>
#endif
#include <string.h>
#include <iostream>

#include "SysIO.h"
#include "Util.h"

#ifdef _MSC_VER
	#define ftello64 _ftelli64
	#define fseeko64 _fseeki64
#endif

namespace dolphindb {

#define RECORD_READ(pbytes, bytelen) //Util::writeFile("/tmp/ddb_read.bin", pbytes, bytelen);
#define RECORD_WRITE(pbytes, bytelen) //Util::writeFile("/tmp/ddb_write.bin", pbytes, bytelen);

bool Socket::ENABLE_TCP_NODELAY = true;

void LOG_ERR(const string& msg){
	std::cout<<msg<<std::endl;
}

void LOG_INFO(const string& msg){
	std::cout<<msg<<std::endl;
}

Socket::Socket():host_(""), port_(-1), blocking_(true), autoClose_(true), enableSSL_(false), ctx_(nullptr), ssl_(nullptr), keepAliveTime_(30) {
    handle_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if(INVALID_SOCKET == handle_) {
    	throw IOException("Couldn't create a socket with error code " + std::to_string(getErrorCode()));
    }
	else if(!blocking_){
		setNonBlocking();
	}
    if(ENABLE_TCP_NODELAY)
    	setTcpNoDelay();
}

Socket::Socket(const string& host, int port, bool blocking, int keepAliveTime, bool enableSSL) : host_(host), port_(port), blocking_(blocking), autoClose_(true), enableSSL_(enableSSL), ctx_(nullptr), ssl_(nullptr), keepAliveTime_(keepAliveTime) {
	if(host.empty() && port > 0){
		//server mode
		handle_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
		if(INVALID_SOCKET == handle_) {
			throw IOException("Couldn't create a socket with error code " + std::to_string(getErrorCode()));
		}
		else if(!blocking_){
			setNonBlocking();
		}
	    if(ENABLE_TCP_NODELAY)
	    	setTcpNoDelay();
	}
	else if(!host.empty() && port > 0){
		//client mode

	}
	else
		handle_ = INVALID_SOCKET;
}

Socket::Socket(SOCKET handle, bool blocking, int keepAliveTime) : host_(""), port_(-1), handle_(handle), blocking_(blocking), autoClose_(true), enableSSL_(false), ctx_(nullptr), ssl_(nullptr), keepAliveTime_(keepAliveTime) {
	if(INVALID_SOCKET == handle_) {
		throw IOException("The given socket is invalid.");
	}
	else if(!blocking){
		setNonBlocking();
	}
    if(ENABLE_TCP_NODELAY)
    	setTcpNoDelay();
}

Socket::~Socket(){
	if(autoClose_)
		close();
}

bool Socket::isValid(){
	return handle_ != INVALID_SOCKET;
}

IO_ERR Socket::read(char* buffer, size_t length, size_t& actualLength, bool msgPeek){
	//RecordTime record("Socket.read");
	if (!enableSSL_) {
#ifdef WINDOWS
		actualLength = recv(handle_, buffer, length, msgPeek ? MSG_PEEK : 0);
		RECORD_READ(buffer, actualLength);
		if (actualLength <= 0) {
			DLogger::Error("socket read error", actualLength);
		}
		if (actualLength == 0)
			return DISCONNECTED;
		else if (actualLength != (size_t)SOCKET_ERROR)
			return OK;
		else {
			actualLength = 0;
			int error = WSAGetLastError();
			if (error == WSAENOTCONN || error == WSAESHUTDOWN || error == WSAENETRESET)
				return DISCONNECTED;
			else if (error == WSAEWOULDBLOCK)
				return NODATA;
			else
				return OTHERERR;
		}
#else //Linux
readdata:
		actualLength = recv(handle_, (void*)buffer, length, (blocking_ ? 0 : MSG_DONTWAIT) | (msgPeek ? MSG_PEEK : 0));
		RECORD_READ(buffer, actualLength);
		if (actualLength <= 0) {
			DLogger::Error("socket read error", actualLength);
		}
		if (actualLength == (size_t)SOCKET_ERROR && errno == EINTR) goto readdata;
		if (actualLength == 0)
			return DISCONNECTED;
		else if (actualLength != (size_t)SOCKET_ERROR)
			return OK;
		else if (errno == EAGAIN || errno == EWOULDBLOCK)
			return NODATA;
		else {
			actualLength = 0;
			return OTHERERR;
		}
#endif// end of enableSSL_=false
	} else {
readdata2:
        actualLength = SSL_read(ssl_, buffer, length);
		RECORD_READ(buffer, actualLength);
		if (actualLength <= 0) {
			DLogger::Error("socket read error", actualLength);
		}
        if(actualLength <= 0){
        int err = SSL_get_error(ssl_, actualLength);
        if(err == SSL_ERROR_WANT_READ) goto readdata2;
            LOG_ERR("Socket(SSL)::read err =" + std::to_string(err));
            return OTHERERR;
        }
        return OK;
    }
}

IO_ERR Socket::write(const char* buffer, size_t length, size_t& actualLength){
	//RecordTime record("Socket.write");
	if(!enableSSL_){
#ifdef WINDOWS
		actualLength=send(handle_, buffer, length, 0);
		RECORD_WRITE(buffer, actualLength);
		if(actualLength != (size_t)SOCKET_ERROR)
			return OK;
		else{
			actualLength=0;
			int error=WSAGetLastError();
			DLogger::Error("socket write error", error);
			if(error==WSAENOTCONN || error==WSAESHUTDOWN || error==WSAENETRESET)
				return DISCONNECTED;
			else if(error==WSAEWOULDBLOCK || error==WSAENOBUFS)
				return NOSPACE;
			else{
				LOG_ERR("Socket::write errno =" + std::to_string(error));
				return OTHERERR;
			}
		}
#else
		senddata:
		actualLength=send(handle_, (const void*)buffer, length,blocking_ ? 0: MSG_DONTWAIT|MSG_NOSIGNAL);
		RECORD_WRITE(buffer, actualLength);
		if(actualLength == (size_t)SOCKET_ERROR && errno == EINTR) goto senddata;

		if(actualLength != (size_t)SOCKET_ERROR)
			return OK;
		else{
			DLogger::Error("socket write error", errno);
			actualLength=0;
			if(errno==EAGAIN || errno==EWOULDBLOCK)
				return NOSPACE;
			else if(errno==ECONNRESET || errno==EPIPE || errno==EBADF || errno==ENOTCONN)
				return DISCONNECTED;
			else{
				LOG_ERR("Socket::write errno =" + std::to_string(errno));
				return OTHERERR;
			}
		}
#endif
	}
	else {
		senddata2:
		actualLength = SSL_write(ssl_, (const void*)buffer, length);
		RECORD_WRITE(buffer, actualLength);
		if (actualLength <= 0) {
			int err = SSL_get_error(ssl_, actualLength);
			if (err == SSL_ERROR_WANT_WRITE) goto senddata2;
			DLogger::Error("socket write error", err);
			LOG_ERR("Socket(SSL)::write err =" + std::to_string(err));
			return OTHERERR;
		}
		return OK;
	}
}

IO_ERR Socket::bind(){
	if(port_<0 || handle_==INVALID_SOCKET)
		return OTHERERR;

	struct sockaddr_in addr;
	memset((void*)&addr, 0, sizeof(addr));
	addr.sin_family      = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_ANY);
	addr.sin_port        = htons(port_);

	int flag=1;
	setsockopt(handle_, SOL_SOCKET, SO_REUSEADDR, (char*)&flag, sizeof(int));
	int r = ::bind(handle_, (struct sockaddr*)&addr, sizeof(addr));
	if(SOCKET_ERROR == r) {
		LOG_ERR("Failed to bind the socket on port " + std::to_string(port_) + " with error code " + std::to_string(getErrorCode()));
		closesocket(handle_);
		return OTHERERR;
	}
	return OK;
}

IO_ERR Socket::listen(){
    int r = ::listen(handle_, SOMAXCONN);
    if(r!=SOCKET_ERROR)
    	return OK;
    else{
    	LOG_ERR("Failed to bind the socket on port " + std::to_string(port_) + " with error code " + std::to_string(getErrorCode()));
    	closesocket(handle_);
    	return OTHERERR;
    }
}

void Socket::enableTcpNoDelay(bool enable){
	 ENABLE_TCP_NODELAY = enable;
}

IO_ERR Socket::connect(const string& host, int port, bool blocking, int keepAliveTime, bool sslEnable){
	host_ = host;
	port_ = port;
	blocking_ = blocking;
	enableSSL_ = sslEnable;
	keepAliveTime_ = keepAliveTime;
	return connect();
}

IO_ERR Socket::connect(){
	if(port_ == -1 || host_.empty())
		return OTHERERR;

	struct addrinfo hints, *servinfo, *p;
	memset(&hints, 0, sizeof hints);
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;
	string portStr=std::to_string(port_);
	if (getaddrinfo(host_.c_str(), portStr.c_str(), &hints, &servinfo)!= 0) {
		LOG_ERR("Failed to call getaddrinfo for host = " + host_ + " port = " + portStr + " with error code " + std::to_string(getErrorCode()));
		return OTHERERR;
	}

	// loop through all the results and connect to the first we can
	for(p = servinfo; p != NULL; p = p->ai_next) {
		if ((handle_ = socket(p->ai_family, p->ai_socktype,	p->ai_protocol)) == (SOCKET)SOCKET_ERROR)
			continue;
		if(!blocking_ && !setNonBlocking()){
			freeaddrinfo(servinfo);
			return OTHERERR;
		}
	    if(ENABLE_TCP_NODELAY && !setTcpNoDelay()){
	    	freeaddrinfo(servinfo);
	    	return OTHERERR;
	    }

        int enabled = 1;
#ifdef WINDOWS
        if(::setsockopt(handle_, SOL_SOCKET, SO_KEEPALIVE, (const char*)&enabled, sizeof(int)) != 0)
            LOG_ERR("Subscription socket failed to enable TCP_KEEPALIVE with error: " +  std::to_string(getErrorCode()));
#else
        int idleTime = keepAliveTime_;
        int interval = 5;
        int count = 3;
        unsigned int timeout = 30000;
        if(::setsockopt(handle_, SOL_SOCKET, SO_KEEPALIVE, &enabled, sizeof(enabled)) != 0)
            LOG_ERR("Failed to enable SO_KEEPALIVE with error: " +  std::to_string(getErrorCode()));
        if(::setsockopt(handle_, SOL_TCP, TCP_KEEPIDLE, &idleTime, sizeof(idleTime)) != 0)
            LOG_ERR("Failed to enable TCP_KEEPIDLE with error: " +  std::to_string(getErrorCode()));
        if(::setsockopt(handle_, SOL_TCP, TCP_KEEPINTVL, &interval, sizeof(interval)) != 0)
            LOG_ERR("Failed to enable TCP_KEEPINTVL with error: " +  std::to_string(getErrorCode()));
        if(::setsockopt(handle_, SOL_TCP, TCP_KEEPCNT, &count, sizeof(count)) != 0)
            LOG_ERR("Failed to enable TCP_KEEPCNT with error: " +  std::to_string(getErrorCode()));
        if(::setsockopt(handle_, IPPROTO_TCP, TCP_USER_TIMEOUT, &timeout, sizeof(timeout)) != 0)
            LOG_ERR("Failed to enable TCP_USER_TIMEOUT with error: " +  std::to_string(getErrorCode()));
#endif

		if(::connect(handle_, p->ai_addr, p->ai_addrlen) == SOCKET_ERROR) {
			if(!blocking_){
#ifdef WINDOWS
				if(WSAGetLastError () == WSAEWOULDBLOCK){
					freeaddrinfo(servinfo);
					return INPROGRESS;
				}
#else
				if(errno == EINPROGRESS){
					freeaddrinfo(servinfo);
					return INPROGRESS;
				}
#endif
			}
			LOG_ERR("Failed to connect to host = " + host_ + " port = " + portStr + " with error code " + std::to_string(getErrorCode()));
			closesocket(handle_);
			handle_=INVALID_SOCKET;
			continue;
		}
		break;
	}
	freeaddrinfo(servinfo);
	if(handle_==INVALID_SOCKET)
		return DISCONNECTED;

	if(!enableSSL_)
		return OK;
	else if(sslConnect() != OK)
		return DISCONNECTED;
	else
		return OK;
}

IO_ERR Socket::close(){
	if(ssl_ != nullptr) {
		SSL_shutdown(ssl_);
		SSL_free(ssl_);
		ssl_ = nullptr;
	}
	if(handle_!= INVALID_SOCKET){
		if(closesocket(handle_) != 0){
			//LOG_ERR("Failed to close the socket handle with error code " + std::to_string(getErrorCode()));
			handle_=INVALID_SOCKET;
			return OTHERERR;
		}
		handle_=INVALID_SOCKET;
	}
	if (ctx_ != nullptr) {
		SSL_CTX_free(ctx_);
		ctx_ = nullptr;
	}
	return OK;
}

Socket* Socket::accept(){
	struct sockaddr_in r_addr;

#ifdef WINDOWS
	int addrLen=sizeof(r_addr);
#else
	socklen_t addrLen=sizeof(r_addr);
#endif
	SOCKET t = ::accept(handle_, (struct sockaddr*)&r_addr, &addrLen);
	if(t==INVALID_SOCKET){
#ifdef WINDOWS
		int errCode = WSAGetLastError();
		if(errCode != WSAEWOULDBLOCK)
			LOG_ERR("Failed to accept one incoming connection with error code " + std::to_string(errCode));
#else
		int errCode = errno;
		if(errCode != EWOULDBLOCK && errCode != EAGAIN)
			LOG_ERR("Failed to accept one incoming connection with error code " + std::to_string(errCode));
#endif
		return NULL;
	}
	else
		return new Socket(t, blocking_, keepAliveTime_);
}

SOCKET Socket::getHandle(){
	return handle_;
}

bool Socket::setNonBlocking(){
#ifdef WINDOWS
	unsigned long value = 1;
	return ioctlsocket(handle_, FIONBIO, &value) == 0;
#else
	int flags = fcntl(handle_, F_GETFL, 0);
	if(flags == -1)
		return false;
	flags |= O_NONBLOCK;
	return fcntl (handle_, F_SETFL, flags) != -1;
#endif
}

bool Socket::setTcpNoDelay(){
	int ret;
#ifdef WINDOWS
	char value = 1;
	ret = setsockopt(handle_, IPPROTO_TCP, TCP_NODELAY, &value, sizeof(char));
#else
	int value = 1;
	ret = setsockopt(handle_, IPPROTO_TCP, TCP_NODELAY, (const void*)&value, sizeof(int));
#endif
	if(ret != 0){
		LOG_ERR("Failed to enable TCP_NODELAY with error code " + std::to_string(getErrorCode()));
		return false;
	}
	else
		return true;
}

int Socket::getErrorCode(){
#ifdef WINDOWS
	return WSAGetLastError();
#else
	return errno;
#endif
}

SSL_CTX* Socket::initCTX(){
    const SSL_METHOD* method;
    SSL_CTX* ctx;

    OpenSSL_add_all_algorithms();
    SSL_load_error_strings();
	#ifdef SSL1_1
		method = TLS_client_method();
	#else
	    method = TLSv1_2_client_method();
	#endif
    ctx = SSL_CTX_new(method);
    return ctx;
}

bool Socket::sslInit() {
    SSL_library_init();
    ctx_ = initCTX();
    if (ctx_ == nullptr) {
        return false;
    }
    ssl_ = SSL_new(ctx_);
    if (ssl_ == nullptr) {
        return false;
    }
    SSL_set_fd(ssl_, handle_);
    return true;
}

IO_ERR Socket::sslConnect() {
    enableSSL_ = true;
    if (!sslInit()) {
        return OTHERERR;
    }
    if (SSL_connect(ssl_) == -1) {
        if (!blocking_) {
            //TODO: solve error
        }
        LOG_ERR("Failed to SSL connect to host = " + host_ + " port = " + std::to_string(port_));
        return OTHERERR;
    }
    return OK;
}

void Socket::showCerts(SSL *ssl) {
    X509 *cert;
    char *line;
    cert = SSL_get_peer_certificate(ssl); /* get the server's certificate */
    if ( cert != NULL ){
        std::cout << "Server certificates:\n";
        line = X509_NAME_oneline(X509_get_subject_name(cert), 0, 0);
        std::cout << "Server certificates: " << line << std::endl;
        delete [] line;
        line = X509_NAME_oneline(X509_get_issuer_name(cert), 0, 0);
        std::cout << "Issuer: " << line << std::endl;
        delete [] line;
        X509_free(cert);
    }
    else
        printf("Info: No client certificates configured.\n");
}

UdpSocket::UdpSocket(int port) : port_(port), remotePort_(-1){
    if ((handle_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) < 0){
    	throw IOException("Couldn't create a udp socket with error code " + std::to_string(getErrorCode()));
    }
}

UdpSocket::UdpSocket(const string& remoteHost, int remotePort ) : port_(0), remoteHost_(remoteHost), remotePort_(remotePort){
    if ((handle_ = socket(AF_INET, SOCK_DGRAM, 0)) < 0){
    	throw IOException("Couldn't create a udp socket with error code " + std::to_string(getErrorCode()));
    }
    if(remoteHost_.empty()){
		const int opt = 1;
		if(setsockopt(handle_, SOL_SOCKET, SO_BROADCAST, (char *)&opt, sizeof(opt)) < 0){
			throw IOException("set udp socket with error code " + std::to_string(getErrorCode()));
		}
		memset(&addrRemote_, 0 , sizeof(struct sockaddr_in));
		addrRemote_.sin_family=AF_INET;
		addrRemote_.sin_addr.s_addr=htonl(INADDR_BROADCAST);
		addrRemote_.sin_port=htons(remotePort_);
    }
    else{
		memset(&addrRemote_, 0 , sizeof(struct sockaddr_in));
		addrRemote_.sin_family=AF_INET;
		addrRemote_.sin_addr.s_addr=inet_addr(remoteHost.c_str());
		addrRemote_.sin_port=htons(remotePort_);
    }
}

UdpSocket::~UdpSocket(){
#ifdef WINDOWS
	closesocket(handle_);
#else
	close(handle_);
#endif
}

IO_ERR UdpSocket::bind(){
    struct sockaddr_in addrto;
    memset(&addrto,0,sizeof(struct sockaddr_in));
    addrto.sin_family = AF_INET;
    addrto.sin_addr.s_addr=INADDR_ANY;
    addrto.sin_port = htons(port_);
    int on = 1;
    setsockopt(handle_, SOL_SOCKET, SO_REUSEADDR, (char*)&on, sizeof(on));
	int r = ::bind(handle_,(struct sockaddr *)&(addrto), sizeof(struct sockaddr_in));
	if(SOCKET_ERROR == r) {
		LOG_ERR("Failed to bind the udp socket on port " + std::to_string(port_) + " with error code " + std::to_string(getErrorCode()));
		closesocket(handle_);
		return OTHERERR;
	}
	return OK;
}

IO_ERR UdpSocket::recv(char* buffer, size_t length, size_t& actualLength){
    int ret=recvfrom(handle_, buffer, length, 0, NULL,NULL);
    if(ret < 0 ){
        LOG_ERR("UdpSocket::recv error code " + std::to_string(getErrorCode()));
        return OTHERERR;
    }
    else{
    	actualLength = ret;
    	return OK;
    }
}

IO_ERR UdpSocket::send(const char* buffer, size_t length){
    int ret=sendto(handle_, buffer, length, 0, (sockaddr*)&addrRemote_, sizeof(addrRemote_));
    if(ret<0){
        LOG_ERR("UdpSocket::send error code " + std::to_string(getErrorCode()));
        return OTHERERR;
    }
    else
    	return OK;
}

int UdpSocket::getErrorCode(){
#ifdef WINDOWS
	return WSAGetLastError();
#else
	return errno;
#endif
}

DataInputStream::DataInputStream(STREAM_TYPE type, int bufSize) : file_(0), buf_(new char[bufSize]), source_(type), reverseOrder_(false), externalBuf_(false),
		closed_(false), capacity_(bufSize), size_(0), cursor_(0){
}

DataInputStream::DataInputStream(const SocketSP& socket, int bufSize) : socket_(socket), file_(0), buf_(new char[bufSize]), source_(SOCKET_STREAM), reverseOrder_(false),
		externalBuf_(false), closed_(false), capacity_(bufSize), size_(0), cursor_(0){
}

DataInputStream::DataInputStream(FILE* file, int bufSize) : file_(file), buf_(new char[bufSize]), source_(FILE_STREAM), reverseOrder_(false), externalBuf_(false),
		closed_(false), capacity_(bufSize), size_(0), cursor_(0){
}

DataInputStream::DataInputStream(const char* data, int size, bool copy) : file_(0), source_(ARRAY_STREAM), reverseOrder_(false), externalBuf_(!copy), closed_(false),
		capacity_(size), size_(size), cursor_(0){
	if(copy){
		buf_ = new char[size];
		memcpy(buf_, data, size);
	}
	else{
		buf_ = (char*)data;
	}
}

DataInputStream::~DataInputStream(){
	if(!externalBuf_)
		delete[] buf_;
	if(source_ == FILE_STREAM)
		close();
}

IO_ERR DataInputStream::internalStreamRead(char* buf, size_t length, size_t& actualLength){
	throw RuntimeException("DataInputStream::internalStreamRead not implemented yet.");
}

IO_ERR DataInputStream::internalClose(){
	throw RuntimeException("DataInputStream::internalStreamRead not implemented yet.");
}

bool DataInputStream::reset(int size){
	if(!externalBuf_)
		return false;
	else{
		cursor_ = 0;
		capacity_ = size;
		size_ = size;
		return true;
	}
}

long long DataInputStream::getPosition() const {
	if(source_ == FILE_STREAM && file_ != NULL){
		long long lastPos = ftello64(file_);
		if(lastPos < 0)
			return -1;
		else
			return lastPos - size_;
	}
	else
		return cursor_;
}

bool DataInputStream::moveToPosition(long long offset){
	if(source_ == FILE_STREAM){
		if(fseeko64(file_, offset, SEEK_SET) == 0){
			cursor_ = 0;
			size_ = 0;
			return true;
		}
		else
			return false;
	}
	else if(source_ > FILE_STREAM){
		bool ret = internalMoveToPosition(offset);
		if(ret){
			cursor_ = 0;
			size_ = 0;
		}
		return ret;
	}
	else
		return false;
}

IO_ERR DataInputStream::readBool(bool& value){
	return readBytes((char*)&value, 1, false);
}

IO_ERR DataInputStream::readBool(char& value){
	return readBytes(&value, 1, false);
}

IO_ERR DataInputStream::readChar(char& value){
	return readBytes(&value, 1, false);
}

IO_ERR DataInputStream::readUnsignedChar(unsigned char& value){
	return readBytes((char*)(&value), 1, false);
}

IO_ERR DataInputStream::readShort(short& value){
	return readBytes((char*)(&value), 2, reverseOrder_);
}

IO_ERR DataInputStream::readUnsignedShort(unsigned short& value){
	return readBytes((char*)(&value), 2, reverseOrder_);
}

IO_ERR DataInputStream::readInt(int& value){
	return readBytes((char*)(&value), 4, reverseOrder_);
}

IO_ERR DataInputStream::readUnsignedInt(unsigned int& value){
	return readBytes((char*)(&value), 4, reverseOrder_);
}

IO_ERR DataInputStream::readLong(long long& value){
	return readBytes((char*)(&value), 8, reverseOrder_);
}

IO_ERR DataInputStream::readIndex(INDEX& value){
#ifdef INDEX64
	return readBytes((char*)(&value), 8, reverseOrder_);
#else
	return readBytes((char*)(&value), 4, reverseOrder_);
#endif
}

IO_ERR DataInputStream::readFloat(float& value){
	return readBytes((char*)(&value), 4, reverseOrder_);
}

IO_ERR DataInputStream::readDouble(double& value){
	return readBytes((char*)(&value), 8, reverseOrder_);
}

IO_ERR DataInputStream::readString(string& value){
	size_t endPos;
	IO_ERR ret = prepareBytesEndWith(0, endPos);
	if(ret != OK)
		return ret;

	size_ -= endPos - cursor_ + 1;
	size_t lineLength = endPos - cursor_;
	if(lineLength > 0 && buf_[endPos - 1] == '\r')
		lineLength--;
	value.clear();
	value.append(buf_ + cursor_, lineLength);
	cursor_ = endPos + 1;
	return OK;
}

IO_ERR DataInputStream::readString(string& value, size_t length){
	if(size_ < length){
		IO_ERR ret =prepareBytes(length);
		if(ret != OK)
			return ret;
	}

	value.clear();
	value.append(buf_+cursor_, length);
	size_ -= length;
	cursor_ += length;
	return OK;
}

IO_ERR DataInputStream::readLine(string& value){
	size_t endPos;
	IO_ERR ret = prepareBytesEndWith('\n', endPos);
	if(ret != OK)
		return ret;

	size_ -= endPos - cursor_ + 1;
	size_t lineLength = endPos - cursor_;
	if(lineLength > 0 && buf_[endPos - 1] == '\r')
		lineLength--;
	value.clear();
	value.append(buf_ + cursor_, lineLength);
	cursor_ = endPos + 1;
	return OK;
}

IO_ERR DataInputStream::peekBuffer(char* buf, size_t length){
	if(size_ < length){
		IO_ERR ret =prepareBytes(length);
		if(ret != OK)
			return ret;
	}

	memcpy(buf, buf_+cursor_, length);
	return OK;
}

IO_ERR DataInputStream::peekLine(string& value){
	size_t endPos;
	IO_ERR ret = prepareBytesEndWith('\n', endPos);
	if(ret != OK)
		return ret;

	size_t lineLength = endPos - cursor_;
	if(lineLength > 0 && buf_[endPos - 1] == '\r')
		lineLength--;
	value.clear();
	value.append(buf_ + cursor_, lineLength);
	return OK;
}

IO_ERR DataInputStream::bufferBytes(size_t length){
	if(size_ >= length)
		return OK;
	else
		return prepareBytes(length);
}

IO_ERR DataInputStream::readBytes(char* buf, size_t length, bool reverseOrder){
	if(size_ < length){
		IO_ERR ret =prepareBytes(length);
		if(ret != OK)
			return ret;
	}

	if(length == 1)
		*buf = buf_[cursor_];
	else if(reverseOrder){
		char* src = buf_ + (cursor_ + length - 1);
		size_t count = length;
		while(count){
			*buf++ = *src--;
			--count;
		}
	}
	else{
		memcpy(buf, buf_+cursor_, length);
	}
	size_ -= length;
	cursor_ += length;
	return OK;
}

IO_ERR DataInputStream::readBytes(char* buf, size_t length, size_t& actualLength){
	actualLength = 0;
    size_t count = ((std::min))(size_, length);
    if(count){
		memcpy(buf, buf_+cursor_, count);
		actualLength += count;
		size_ -= count;
		cursor_ += count;
		if(count == length)
			return OK;
    }

    if(source_ == SOCKET_STREAM){
    	count = 0;
    	IO_ERR ret = OK;
    	while(ret == OK && actualLength < length){
    		ret = socket_->read(buf+actualLength, length-actualLength, count);
			if(ret == OK)
				actualLength += count;
    	}
    	return ret;
    }
    else if(source_ == FILE_STREAM){
    	count = fread(buf + actualLength, 1, length-actualLength, file_);
    	actualLength += count;
    	if(count == 0){
    		if(feof(file_))
    			return END_OF_STREAM;
    		else
    			return OTHERERR;
    	}
    	else
    		return OK;
    }
    else if(source_ == ARRAY_STREAM){
    	if(actualLength == 0)
    		return END_OF_STREAM;
    	else
    		return OK;
    }
    else{
    	size_t readCount;
    	IO_ERR ret = internalStreamRead(buf + actualLength, length - actualLength, readCount);
   		actualLength += readCount;
    	return ret;
    }
}

IO_ERR DataInputStream::readBytes(char* buf, size_t unitLength, size_t length, size_t& actualLength){
	if(unitLength == 1)
		return readBytes(buf, length, actualLength);
	else if(unitLength > MAX_CAPACITY)
		return TOO_LARGE_DATA;
	IO_ERR ret = readBytes(buf, length * unitLength, actualLength);
	int remainder = actualLength % unitLength;
	actualLength = actualLength / unitLength;
	if(remainder >0){
		cursor_ = 0;
		size_ = remainder;
		memcpy(buf_, buf + unitLength * actualLength, size_);
	}
	return ret;
}

IO_ERR DataInputStream::prepareBytes(size_t length){
	if(source_ == ARRAY_STREAM)
		return END_OF_STREAM;

	if(capacity_ < length){
		if(length > MAX_CAPACITY)
			return TOO_LARGE_DATA;
		char* tmp = new char[length];
		memcpy(tmp, buf_+cursor_, size_);
		capacity_ = length;
		cursor_ =0;
		delete[] buf_;
		buf_ = tmp;
	}
	else if(capacity_ - cursor_ <length || (source_ > FILE_STREAM && (cursor_ << 1) > capacity_)){
		memmove(buf_, buf_+cursor_, size_);
		cursor_ =0;
	}

	size_t actualLength;
	size_t usedSpace = cursor_ + size_;
	if(source_ == SOCKET_STREAM){
		while(size_ < length){
			IO_ERR ret = socket_->read(buf_ + usedSpace, capacity_ - usedSpace, actualLength);
			if(ret != OK)
				return ret;
			size_ += actualLength;
			usedSpace += actualLength;
		}
		return OK;
	}
	else if(source_ == FILE_STREAM){
		size_t num = capacity_ - usedSpace;
		actualLength = fread(buf_ + usedSpace, 1, num, file_);
		size_ += actualLength;
		if(actualLength == num)
			return OK;
		else if(feof(file_)){
			if(size_ >= length)
				return OK;
			else
				return END_OF_STREAM;
		}
		else
			return OTHERERR;
	}
	else{
		IO_ERR ret  = internalStreamRead(buf_ + usedSpace, capacity_ - usedSpace, actualLength);
		size_ += actualLength;
		if(size_ < length){
			return END_OF_STREAM;
		}
		else
			return ret;
	}
}

IO_ERR DataInputStream::prepareBytesEndWith(char endChar, size_t& endPos){
	size_t searchedSize = 0;
	bool found = false;

	while(!found){
		char* cur = buf_ + cursor_ + searchedSize;
		int count = size_ - searchedSize;
		while(count && *cur != endChar){
			--count;
			++cur;
		}
		if(count >0){
			endPos = cur - buf_;
			found = true;
		}
		else{
			if(source_ == ARRAY_STREAM)
				return END_OF_STREAM;

			searchedSize = size_;

			if(capacity_ - size_ - cursor_ >= 1024){
			}
			else if( cursor_ >= 1024){
				//remove used data
				memmove(buf_, buf_+cursor_, size_);
				cursor_ =0;
			}
			else if(size_ >= MAX_CAPACITY)
				return TOO_LARGE_DATA;
			else {
				//increase capacity
				capacity_ = ((std::min))(2 * capacity_, (size_t)MAX_CAPACITY);
				char* tmp = new char[capacity_];
				memcpy(tmp, buf_ + cursor_, size_);
				delete[] buf_;
				buf_ = tmp;
				cursor_ = 0;
			}

			size_t actualLength;
			size_t usedSpace = cursor_ + size_;
			if(source_ == SOCKET_STREAM){
				IO_ERR ret = socket_->read(buf_ + usedSpace, capacity_ - usedSpace, actualLength);
				if( ret != OK)
					return ret;
				size_ += actualLength;
				usedSpace += actualLength;
			}
			else if(source_ == FILE_STREAM){
				actualLength = fread(buf_ + usedSpace, 1, capacity_ - usedSpace, file_);
				if(actualLength == 0){
					if(feof(file_))
						return END_OF_STREAM;
					else
						return OTHERERR;
				}
				size_ += actualLength;
			}
			else{
				IO_ERR ret  = internalStreamRead(buf_ + usedSpace, capacity_ - usedSpace, actualLength);
				size_ += actualLength;
				if( ret != OK)
					return ret;
			}
		}
	}
	return OK;
}

IO_ERR DataInputStream::close(){
	if(closed_ || source_ == ARRAY_STREAM)
		return OK;

	if(source_ == SOCKET_STREAM){
		IO_ERR ret = socket_->close();
		if(ret == OK)
			closed_ = true;
		return ret;
	}
	else if(source_ == FILE_STREAM && file_ != NULL){
		if(fclose(file_) == 0){
			file_ = NULL;
			closed_ = true;
			return OK;
		}
		else
			return OTHERERR;
	}
	else {
		IO_ERR ret  = internalClose();
		if(ret == OK)
			closed_ = true;
		return ret;
	}
}

DataOutputStream::DataOutputStream(const SocketSP& socket, size_t flushThreshold) : source_(SOCKET_STREAM),
		flushThreshold_(flushThreshold), socket_(socket), file_(0), buf_(0), capacity_(flushThreshold * 2), size_(0), autoClose_(false){
	if(capacity_ > 0)
		buf_ = new char[capacity_];
}

DataOutputStream::DataOutputStream(FILE* file, bool autoClose) : source_(FILE_STREAM), flushThreshold_(0),file_(file),
		buf_(0), capacity_(0), size_(0), autoClose_(autoClose){

}

DataOutputStream::DataOutputStream(size_t capacity) : source_(ARRAY_STREAM), flushThreshold_(0),file_(0), buf_(new char[capacity]),
		capacity_(capacity), size_(0), autoClose_(false){

}

DataOutputStream::DataOutputStream(STREAM_TYPE source) : source_(source), flushThreshold_(0), file_(0), buf_(0), capacity_(0), size_(0), autoClose_(false){
}

DataOutputStream::~DataOutputStream(){
	if(buf_ != NULL && source_ <= FILE_STREAM){
		delete[] buf_;
	}
	if(autoClose_ && file_ != NULL){
		fclose(file_);
	}
}

IO_ERR DataOutputStream::close(){
	if(source_ == SOCKET_STREAM){
		return socket_->close();
	}
	else if(source_ == FILE_STREAM && file_ != NULL){
		if(fclose(file_) == 0){
			file_ = NULL;
			return OK;
		}
		else
			return OTHERERR;
	}
	else if(source_ > FILE_STREAM){
		if(size_ > 0){
			IO_ERR ret = internalFlush(size_);
			if(ret != OK)
				LOG_ERR("Failed to write cached data to the underlying device before closing. size=" + std::to_string(size_) + " streamType=" + std::to_string(source_));
			size_ = 0;
		}
		return internalClose();
	}
	else
		return OK;
}

IO_ERR DataOutputStream::write(const char* buffer, size_t length, size_t& actualWritten){
	IO_ERR ret = OK;
	size_t sent= 0;
	switch(source_){
	case SOCKET_STREAM:
		if(size_ + length < flushThreshold_){
			memcpy(buf_ + size_, buffer, length);
			size_ += length;
			actualWritten = length;
			return OK;
		}

		actualWritten = 0;
		if(size_ > 0){
			int count = ((std::min))(flushThreshold_ - size_, length);
			if(count > 0){
				memcpy(buf_ + size_, buffer, count);
				size_ += count;
				actualWritten += count;
			}

			int cursor = 0;
			while(size_ > 0 && (ret = socket_->write(buf_ + cursor, size_, sent)) == OK){
				cursor += sent;
				size_ -= sent;
			}
			if(ret != OK){
				if(cursor > 0){
					memmove(buf_, buf_ + cursor, size_);
				}
				return ret;
			}
		}

		while(actualWritten < length){
			ret = socket_->write(buffer + actualWritten, length - actualWritten, sent);
			if(ret != OK)
				return ret;
			actualWritten += sent;
		}
		return ret;

	case FILE_STREAM:
		actualWritten = fwrite(buffer, 1, length, file_);
		if(actualWritten < length)
			return OTHERERR;
		else
			return OK;

	case ARRAY_STREAM:
		if(size_ + length > capacity_){
			if(capacity_ >= MAX_ARRAY_BUFFER)
				return TOO_LARGE_DATA;
			char* tmp = buf_;
			size_t newCapacity = (std::max)(size_ + length, 2 * capacity_);
			buf_ = new char[newCapacity];
			capacity_ = newCapacity;
			memcpy(buf_, tmp, size_);
			delete[] tmp;
		}
		memcpy(buf_+size_, buffer, length);
		size_ += length;
		actualWritten = length;
		return OK;

	default:
		if(capacity_ == 0){
			buf_ = createBuffer(capacity_);
		}
		actualWritten = 0;
		if(size_ + length < capacity_){
			memcpy(buf_ + size_, buffer, length);
			size_ += length;
			actualWritten += length;
			return OK;
		}

		while(actualWritten < length){
			size_t count = ((std::min))(length - actualWritten, capacity_ - size_);
			if(count > 0)
				memcpy(buf_ + size_, buffer + actualWritten, count);
			if(size_ + count < capacity_){
				actualWritten += count;
				size_ += count;
				return OK;
			}
			ret = internalFlush(size_ + count);
			if(ret != OK)
				return ret;
			actualWritten += count;
			size_ = 0;
			if(capacity_ == 0){
				buf_ = createBuffer(capacity_);
			}
		}
		return OK;
	}
}

IO_ERR DataOutputStream::write(const char* buffer, size_t length){
	size_t actualLength;

	switch(source_){
	case SOCKET_STREAM:
		//socket write must use another method write(const char* buffer, size_t length, size_t& actualWritten)
		return OTHERERR;

	case FILE_STREAM:
		actualLength = fwrite(buffer, 1, length, file_);
		if(actualLength < length)
			return OTHERERR;
		else
			return OK;

	case ARRAY_STREAM:
		if(size_ + length > capacity_){
			if(capacity_ >= MAX_ARRAY_BUFFER)
				return TOO_LARGE_DATA;
			char* tmp = buf_;
			size_t newCapacity = (std::max)(size_ + length, 2 * capacity_);
			buf_ = new char[newCapacity];
			capacity_ = newCapacity;
			memcpy(buf_, tmp, size_);
			delete[] tmp;
		}
		memcpy(buf_+size_, buffer, length);
		size_ += length;
		return OK;

	default:
		if(capacity_ == 0){
			buf_ = createBuffer(capacity_);
		}

		if(size_ + length < capacity_){
			memcpy(buf_ + size_, buffer, length);
			size_ += length;
			return OK;
		}

		IO_ERR ret;
		size_t actualWritten = 0;
		while(actualWritten < length){
			size_t count = ((std::min))(length - actualWritten, capacity_ - size_);
			if(count > 0)
				memcpy(buf_ + size_, buffer + actualWritten, count);
			if(size_ + count < capacity_){
				actualWritten += count;
				size_ += count;
				return OK;
			}
			ret = internalFlush(size_ + count);
			if(ret != OK)
				return ret;
			actualWritten += count;
			size_ = 0;
			if(capacity_ == 0){
				buf_ = createBuffer(capacity_);
			}
		}
		return OK;
	}
}

IO_ERR DataOutputStream::resume(){
	if(size_ == 0 || source_ != SOCKET_STREAM)
		return OK;

	IO_ERR ret = OK;
	size_t actualWritten = 0;
	size_t cursor = 0;

	while(size_>0 && (ret = socket_->write(buf_ + cursor, size_, actualWritten))==OK  && actualWritten <= size_){
		cursor += actualWritten;
		size_ -= actualWritten;
	}
	if(cursor > 0 && size_ > 0)
		memmove(buf_, buf_ + cursor, size_);
	else if(ret == OK)
		size_ = 0;
	return ret;
}

IO_ERR DataOutputStream::flush(){
	if(source_ == SOCKET_STREAM && size_ > 0){
		return resume();
	}
	else if(source_ == FILE_STREAM){
		fflush(file_);
	}
	else if(source_ > FILE_STREAM){
		if(size_ > 0){
			IO_ERR ret = internalFlush(size_);
			if(ret != OK)
				return ret;
			size_ = 0;
		}
	}
	return OK;
}

IO_ERR DataOutputStream::internalFlush(size_t size){
	throw RuntimeException("DataOutputStream::internalFlush not implemented yet.");
}

IO_ERR DataOutputStream::internalClose(){
	throw RuntimeException("DataOutputStream::internalClose not implemented yet.");
}

char* DataOutputStream::createBuffer(size_t& capacity){
	throw RuntimeException("DataOutputStream::createBuffer not implemented yet.");
}

DataStream::DataStream(FILE* file, bool readable, bool writable) : DataInputStream(file),
		outBuf_(new char[2048]), outSize_(2048){
	isReadable(readable);
	isWritable(writable);
}

DataStream::~DataStream(){
	delete[] outBuf_;
}

bool DataStream::isReadable() const{
	return flag_ & 1;
}

void DataStream::isReadable(bool option){
	if(option)
		flag_ |= 1;
	else
		flag_ &= ~1;
}

bool DataStream::isWritable() const{
	return flag_ & 2;
}

void DataStream::isWritable(bool option){
	if(option)
		flag_ |= 2;
	else
		flag_ &= ~2;
}

IO_ERR DataStream::clearReadBuffer(){
	if(size_ >0){
		//there is cached data in read buffer so that we have to move file pointer to the position it should be.
		long long offset = 0 - size_;
		if(fseeko64(file_, offset, SEEK_CUR)!=0)
			return OTHERERR;
		size_ = 0;
		cursor_ = 0;
	}
	return OK;
}

IO_ERR DataStream::write(const char* buf, int length, int& sent){

	if(source_ == FILE_STREAM){
		if(size_ >0){
			clearReadBuffer();
		}
		sent = fwrite(buf, 1, length, file_);
		if(sent < length){
			LOG_ERR("disk writing failure: " + Util::getLastErrorMessage());
			return NOSPACE;
		}
		else
			return OK;
	}
	else{
		sent = 0;
		while(length){
			size_t actualSent;
			IO_ERR ret = socket_->write(buf+sent, length, actualSent);
			if(ret != OK)
				return ret;
			sent += actualSent;
			length -= actualSent;
		}
		return OK;
	}
}

IO_ERR DataStream::write(Constant* obj, INDEX offset, INDEX length, INDEX& sent){
	int partial = 0;
	int readBytes = 0;
	int count = 0;
	sent = 0;
	if(source_ == FILE_STREAM){
		if(size_ >0){
			clearReadBuffer();
		}
		while((readBytes=obj->serialize(outBuf_, outSize_, offset + sent , partial, count, partial))>0){
			int sentBytes = fwrite(outBuf_, 1, readBytes, file_);
			if(sentBytes < readBytes){
				LOG_ERR("disk writing failure: " + Util::getLastErrorMessage());
				return NOSPACE;
			}
			sent += count;
			if(sent >= length)
				return OK;
		}
		return OK;
	}
	else{
		while((readBytes=obj->serialize(outBuf_, outSize_, offset + sent , partial, count, partial))>0){
			size_t actualSent;
			IO_ERR ret = socket_->write(outBuf_, readBytes, actualSent);
			if(ret != OK)
				return ret;
			sent += count;
			if(sent >= length)
				return OK;
		}
		return OK;
	}
}

IO_ERR DataStream::writeLine(const char* obj, const char* newline){
	if(source_ == FILE_STREAM){
		if(size_ >0){
			IO_ERR ret = clearReadBuffer();
			if(ret != OK)
				return ret;
		}
		// write to file
		if(fputs(obj, file_)<0){
			std::cout<<ferror(file_)<<std::endl;
			return OTHERERR;
		}
		if(fputs(newline, file_) >= 0)
			return OK;
		else
			return OTHERERR;
	}
	else{
		// write to socket
		size_t actualSent;
		IO_ERR ret = socket_->write(obj, strlen(obj), actualSent);
		if(ret != OK)
			return ret;
		return socket_->write(newline, strlen(newline), actualSent);
	}
}

IO_ERR DataStream::writeLine(Constant* obj, INDEX offset, INDEX length, const char* newline, INDEX& sent){
	IO_ERR ret;
	if(source_ == FILE_STREAM){
		if(size_ >0){
			if((ret = clearReadBuffer()) != OK)
				return ret;
		}
	}

	size_t additionalChars = strlen(newline);
	size_t sentBytes;
	INDEX initOffset = offset;
	sent = 0;

	int count;
	const int bufSize = Util::BUF_SIZE;
	char* buf[bufSize];

	while(length){
		count = ((std::min))(bufSize, length);
		obj->getString(offset, count, buf);

		size_t cursor = 0;
		for(int i=0; i<count; ++i){
			size_t curSize = strlen(buf[i]);
			if(cursor + curSize + additionalChars > outSize_){
				if(cursor){
					if(source_ == FILE_STREAM){
						sentBytes = fwrite(outBuf_, 1, cursor, file_);
						if(sentBytes < cursor)
							return OTHERERR;
					}
					else{
						ret = socket_->write(outBuf_, cursor, sentBytes);
						if(ret != OK)
							return ret;
					}
					sent = offset + i - initOffset;
					cursor = 0;
				}
			}
			if(curSize + additionalChars > outSize_){
				if(source_ == FILE_STREAM){
					sentBytes = fwrite(buf[i], 1, curSize, file_);
					if(sentBytes < curSize)
						return OTHERERR;
					if(fputs(newline, file_) >= 0)
						ret = OK;
					else
						ret = OTHERERR;
				}
				else{
					ret = socket_->write(buf[i], curSize, sentBytes);
					if(ret != OK)
						return ret;
					ret = socket_->write(newline, additionalChars, sentBytes);
				}
				if(ret == OK)
					++sent;
				else
					return ret;
			}
			else{
				memcpy(outBuf_ + cursor, buf[i], curSize);
				cursor += curSize;
				memcpy(outBuf_ + cursor, newline, additionalChars);
				cursor += additionalChars;
			}
		}

		if(cursor){
			if(source_ == FILE_STREAM){
				sentBytes = fwrite(outBuf_, 1, cursor, file_);
				if(sentBytes < cursor)
					return OTHERERR;
			}
			else{
				ret = socket_->write(outBuf_, cursor, sentBytes);
				if(ret != OK)
					return ret;
			}
			sent = offset + count - initOffset;
		}

		offset += count;
		length -= count;
	}
	return OK;

}

IO_ERR DataStream::seek(long long offset, int mode, long long& newPosition){
	//mode == 1 : relative to the current position
	IO_ERR ret;
	if(mode == 1 && size_ > 0){
		offset -= size_;
		if(offset == 0){
			ret = OK;
		}
		else if(fseeko64(file_, offset, mode) == 0){
			size_ = 0;
			cursor_ = 0;
			ret = OK;
		}
		else
			ret = OTHERERR;
	}
	else if(fseeko64(file_, offset, mode) == 0)
		ret = OK;
	else
		ret = OTHERERR;
	if(ret == OK)
		newPosition = ftello64(file_);
	return ret;
}

string DataStream::getDescription() const {
	if(source_ == SOCKET_STREAM)
		return "SocketStream[" + std::to_string(socket_->getHandle()) + "]";
	else if(source_ == FILE_STREAM)
		return "FileStream[" + std::to_string((long long)file_) + "]";
	else
		return "ArrayStream";
}

IO_ERR Buffer::write(const char* buffer, int length, int& actualLength){
	actualLength = 0;
	if(size_ + length > capacity_){
		if(external_ || capacity_ >= MAX_ARRAY_BUFFER)
			return TOO_LARGE_DATA;
		char* tmp = buf_;
		size_t newCapacity = (std::max)(size_ + length, 2 * capacity_);
		buf_ = new char[newCapacity];
		capacity_ = newCapacity;
		memcpy(buf_, tmp, size_);
		delete[] tmp;
	}
	memcpy(buf_+size_, buffer, length);
	size_ += length;
	actualLength = length;
	return OK;
}

IO_ERR Buffer::write(const char* buffer, int length){
	if(size_ + length > capacity_){
		if(external_ || capacity_ >= MAX_ARRAY_BUFFER)
			return TOO_LARGE_DATA;
		char* tmp = buf_;
		size_t newCapacity = (std::max)(size_ + length, 2 * capacity_);
		buf_ = new char[newCapacity];
		capacity_ = newCapacity;
		memcpy(buf_, tmp, size_);
		delete[] tmp;
	}
	memcpy(buf_+size_, buffer, length);
	size_ += length;
	return OK;
}

void Buffer::clear() {
	size_ = 0;
}

};
