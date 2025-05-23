// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4251 )
#endif

#include <iostream>
#include <string>
#include "SmartPointer.h"
#include "Types.h"
#include "Concurrent.h"
#include "Platform.h"

#ifdef USE_OPENSSL

#include <openssl/err.h>
#include <openssl/ssl.h>

#endif

#ifdef __linux__
typedef int SOCKET;
#define INVALID_SOCKET -1
#define SOCKET_ERROR   -1
#endif

namespace dolphindb {

class Constant;
class DataInputStream;
class DataOutputStream;
class DataStream;
class DataBlock;
typedef SmartPointer<DataInputStream> DataInputStreamSP;
typedef SmartPointer<DataOutputStream> DataOutputStreamSP;
typedef SmartPointer<DataStream> DataStreamSP;
typedef SmartPointer<BlockingQueue<DataBlock>> DataQueueSP;

class EXPORT_DECL Socket{
public:
	Socket();
	Socket(const std::string& host, int port, bool blocking, int keepAliveTime, bool enableSSL = false);
	Socket(SOCKET handle, bool blocking, int keepAliveTime);
	~Socket();
	const std::string& getHost() const {return host_;}
	int getPort() const {return port_;}
	IO_ERR read(char* buffer, size_t length, size_t& actualLength, bool msgPeek = false);
	IO_ERR write(const char* buffer, size_t length, size_t& actualLength);
	IO_ERR bind();
	IO_ERR listen();
	IO_ERR connect(const std::string& host, int port, bool blocking, int keepAliveTime, bool enableSSL = false);
	IO_ERR connect();
	IO_ERR close();
	Socket* accept();
	SOCKET getHandle();
	bool isBlockingMode() const {return blocking_;}
	bool isValid();
	void setAutoClose(bool option) { autoClose_ = option;}
	static void enableTcpNoDelay(bool enable);
	static bool ENABLE_TCP_NODELAY;
	bool skipAll();

private:
	void getTimeout(int &timeoutMs);
	void setTimeout(int timeoutMs);
	bool setNonBlocking();
	bool setBlocking();
	bool setTcpNoDelay();
	int getErrorCode();
	void* initCTX();
	bool sslInit();
	IO_ERR sslConnect();
	void showCerts(void* ssl);
private:
	std::string host_;
	int port_;
	SOCKET handle_;
	bool blocking_;
	bool autoClose_;
	bool enableSSL_;
	void* ctx_;
	void* ssl_;
	int keepAliveTime_;
};

typedef SmartPointer<Socket> SocketSP;

class DataBlock{
public:
	DataBlock() : buf_(nullptr), length_(0){}
	DataBlock(char* buf, size_t length) : buf_(buf), length_(length){}
	char* getDataBuf() const {return buf_;}
	size_t getDataLength() const {return length_;}
private:
	char* buf_;
	size_t length_;
};

class EXPORT_DECL DataInputStream{
public:
	DataInputStream(STREAM_TYPE type, std::size_t bufSize = 2048);
	DataInputStream(const char* data, std::size_t size, bool copy = true);
	DataInputStream(const SocketSP& socket, std::size_t bufSize = 2048);
	DataInputStream(FILE* file, std::size_t bufSize = 2048);
	DataInputStream(DataQueueSP dataQueue);
	virtual ~DataInputStream();
	IO_ERR close();
	void enableReverseIntegerByteOrder() { reverseOrder_ = true;}
	void disableReverseIntegerByteOrder() { reverseOrder_ = false;}
	IO_ERR bufferBytes(size_t length);
	IO_ERR read(char* buf, size_t length) { return readBytes(buf, length, false);}
	IO_ERR readBytes(char* buf, size_t length, bool reverseOrder);

	/**
	 * This method is designed to read a large block of stream data.  When the length is too small, say less than 8192,
	 * it may affect IO throughput. This method first retrieves data from the buffer and reads the remaining from the
	 * underlying device.
	 */
	IO_ERR readBytes(char* buf, size_t length, size_t& actualLength);
	IO_ERR readBytes(char* buf, size_t unitLength, size_t length, size_t& actualLength);
	IO_ERR readBool(bool& value);
	IO_ERR readBool(char& value);
	IO_ERR readChar(char& value);
	IO_ERR readUnsignedChar(unsigned char& value);
	IO_ERR readShort(short& value);
	IO_ERR readUnsignedShort(unsigned short& value);
	IO_ERR readInt(int& value);
	IO_ERR readUnsignedInt(unsigned int& value);
	IO_ERR readLong(long long& value);
	IO_ERR readIndex(INDEX& value);
	IO_ERR readFloat(float& value);
	IO_ERR readDouble(double& value);
	IO_ERR readString(std::string& value);
	IO_ERR readString(std::string& value, size_t length);
	IO_ERR readLine(std::string& value);
	/**
	 * Preview the given size of stream data from the current position. The internal current position will not change
	 * after this operation. If the available data in the internal buffer from the current position is less than the
	 * requested size, the method will read data from the socket and be blocked or immediately return an error if there
	 * is not data available unfortunately depending on the socket mode, blocking or non-blocking.
	 */
	IO_ERR peekBuffer(char* buf, size_t size);
	IO_ERR peekLine(std::string& value);

	inline bool isSocketStream() const {return source_ == SOCKET_STREAM;}
	inline bool isFileStream() const { return source_ == FILE_STREAM;}
	inline bool isArrayStream() const {return source_ == ARRAY_STREAM;}
	inline STREAM_TYPE getStreamType() const {return source_;}
	SocketSP getSocket() const { return socket_;}
	FILE* getFileHandle() const { return file_;}

	/**
	 * The position of the cursor in the file or buffer after last read. This function works for the case of file input or
	 * buffer input. It always return zero for socket input.
	 */
	long long getPosition() const;


	std::size_t getDataSizeInArray() const { return size_;}
	bool isIntegerReversed() const {return reverseOrder_;}

	/**
	 * Move to the given position of a file.It always returns false for other type of stream.
	 * The internal buffer will be cleared if the file position is moved successfully.
	 */
	bool moveToPosition(long long offset);

	/**
	 * Reset the size of an external buffer. The cursor moves to the beginning of the buffer.
	 */
	bool reset(int size);

private:
	IO_ERR prepareBytes(size_t length);
	IO_ERR prepareBytesEndWith(char endChar, size_t& endPos);
	//use in QUEUE_STREAM, if there is already data to read, return immediately, or block util data ready
	IO_ERR prepareData();
	//use in QUEUE_STREAM, if there is a #endChar in buf_, record #endPos and return true, or return false
	bool   isHaveBytesEndWith(char endChar, size_t& endPos);
protected:
	SocketSP socket_;
	FILE* file_;
	char* buf_;
	STREAM_TYPE source_;
	bool reverseOrder_;
	bool externalBuf_;
	bool closed_;
	size_t capacity_;
	size_t size_;
	size_t cursor_;
	DataQueueSP dataQueue_;
};

class EXPORT_DECL DataOutputStream {
public:
	DataOutputStream(const SocketSP& socket, size_t flushThreshold = 4096);
	DataOutputStream(FILE* file, bool autoClose = false);
	DataOutputStream(size_t capacity = 1024) ;
	DataOutputStream(STREAM_TYPE source) ;
	DataOutputStream(DataQueueSP dataQueue);
	virtual ~DataOutputStream();
	IO_ERR write(const char* buffer, size_t length, size_t& actualWritten);
	IO_ERR write(const char* buffer, size_t length);
	IO_ERR resume();
	inline IO_ERR start(const char* buffer, size_t length){return write(buffer, length);}
	inline IO_ERR write(const std::string& buffer){ return write(buffer.c_str(), buffer.length() + 1);}
	inline IO_ERR writeData(const std::string& buffer){ return write(buffer.data(), buffer.length());}
	inline IO_ERR write(bool val){ return write((const char*)&val, 1);}
	inline IO_ERR write(char val){ return write(&val, 1);}
	inline IO_ERR write(short val){ return write((const char*)&val, 2);}
	inline IO_ERR write(unsigned short val){ return write((const char*)&val, 2);}
	inline IO_ERR write(int val){ return write((const char*)&val, 4);}
	inline IO_ERR write(long long val){ return write((const char*)&val, 8);}
	inline IO_ERR write(float val){ return write((const char*)&val, 4);}
	inline IO_ERR write(double val){ return write((const char*)&val, 8);}
	SocketSP getSocket() const { return socket_;}
	FILE* getFile() const { return file_;}
	const char * getBuffer() const { return buf_;}
	size_t size() const { return size_;}
	IO_ERR flush();
	IO_ERR close();

protected:
	STREAM_TYPE source_;
	size_t flushThreshold_;
	SocketSP socket_;
	FILE* file_;
	char* buf_;
	size_t capacity_;
	size_t size_;
	bool autoClose_;
	DataQueueSP dataQueue_;
};

class EXPORT_DECL Buffer {
public:
	Buffer(size_t cap) : buf_(new char[cap]), capacity_(cap), size_(0), external_(false){}
	Buffer() : buf_(new char[256]), capacity_(256), size_(0), external_(false){}
	Buffer(char* buf, size_t cap) : buf_(buf), capacity_(cap), size_(0), external_(true){}
	Buffer(char* buf, size_t offset, size_t cap, bool external = true) : buf_(buf), capacity_(cap), size_(offset), external_(external){}
	~Buffer() { if(!external_) delete[] buf_;}
	IO_ERR write(const char* buffer, size_t length, size_t& actualLength);
	IO_ERR write(const char* buffer, size_t length);
	inline IO_ERR write(const std::string& buffer){ return write(buffer.c_str(), buffer.length() + 1);}
	inline IO_ERR writeData(const std::string& buffer){ return write(buffer.data(), buffer.length());}
	inline IO_ERR write(bool val){ return write((const char*)&val, 1);}
	inline IO_ERR write(char val){ return write(&val, 1);}
	inline IO_ERR write(short val){ return write((const char*)&val, 2);}
	inline IO_ERR write(unsigned short val){ return write((const char*)&val, 2);}
	inline IO_ERR write(int val){ return write((const char*)&val, 4);}
	inline IO_ERR write(long long val){ return write((const char*)&val, 8);}
	inline IO_ERR write(float val){ return write((const char*)&val, 4);}
	inline IO_ERR write(double val){ return write((const char*)&val, 8);}
	size_t size() const { return size_;}
	size_t capacity() const { return capacity_;}
	const char * getBuffer() const { return buf_;}
	void clear();

private:
	char* buf_;
	size_t capacity_;
	size_t size_;
	bool external_;
};

template<class T>
class EXPORT_DECL BufferWriter{
public:
	BufferWriter(const T& out) : out_(out), buffer_(0), size_(0){}

	IO_ERR start(const char* buffer, size_t length){
		IO_ERR ret = OK;
		size_t actualWritten = 0;

		buffer_ = (char*)buffer;
		size_ = length;
		while((ret = out_->write(buffer_, size_, actualWritten))==OK  && actualWritten < size_){
			buffer_ += actualWritten;
			size_ -= actualWritten;
		}
		if(ret == NOSPACE){
			buffer_ += actualWritten;
			size_ -= actualWritten;
		}
		else
			size_ = 0;
		return ret;
	}

	IO_ERR resume(){
		IO_ERR ret = OK;
		size_t actualWritten = 0;

		while((ret = out_->write(buffer_, size_, actualWritten))==OK  && actualWritten < size_){
			buffer_ += actualWritten;
			size_ -= actualWritten;
		}
		if(ret == NOSPACE){
			buffer_ += actualWritten;
			size_ -= actualWritten;
		}
		else
			size_ = 0;
		return ret;
	}

	inline size_t size() const { return size_;}
	inline T getDataOutputStream() const { return out_;}

private:
	T out_;
	char* buffer_;
	size_t size_;
};

class EXPORT_DECL DataStream : public DataInputStream{
public:
	DataStream(const char* data, int size) : DataInputStream(data, size), flag_(1), outBuf_(0) {}
	DataStream(const SocketSP& socket) : DataInputStream(socket), flag_(3), outBuf_(new char[2048]) {}
	DataStream(FILE* file, bool readable, bool writable);
	virtual ~DataStream();
	bool isReadable() const;
	void isReadable(bool option);
	bool isWritable() const;
	void isWritable(bool option);
	IO_ERR clearReadBuffer();
	IO_ERR write(const char* buf, std::size_t length, std::size_t& sent);
	IO_ERR writeLine(const char* obj, const char* newline);
	IO_ERR seek(long long offset, int mode, long long& newPosition);
	std::string getDescription() const;

private:
	char flag_; // bit0: readable bit1: writable
	char* outBuf_;
};

struct EXPORT_DECL FileAttributes{
	std::string name;
	bool isDir;
	long long size;
	long long lastModified; //epoch time in milliseconds
	long long lastAccessed; //epoch time in milliseconds
};

}

#ifdef _MSC_VER
#pragma warning( pop )
#endif
