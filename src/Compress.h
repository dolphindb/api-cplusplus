#pragma once
#ifndef COMPRESSION_H_
#define COMPRESSION_H_

#include "Types.h"
#include "SysIO.h"
#include "DolphinDB.h"
namespace dolphindb {

class CompressEncoderDecoder;
typedef SmartPointer<CompressEncoderDecoder> CompressEncoderDecoderSP;

class CompressionFactory {
public:
#pragma pack (1)
	struct Header {
		int byteSize;
		int colCount;
		char version;
		char flag;
		char charCode;
		char compressedType;
		char dataType;
		char unitLength;
		short reserved;
		int extra;
		int elementCount;
		int checkSum;
	};
#pragma pack ()
	static CompressEncoderDecoderSP GetEncodeDecoder(COMPRESS_METHOD type);
	static IO_ERR decode(DataInputStreamSP compressSrc, DataOutputStreamSP &uncompressResult, Header &header);
	static IO_ERR encodeContent(const VectorSP &vec, const DataOutputStreamSP &compressResult, Header &header, bool checkSum);
};

class CompressEncoderDecoder {
public:
	virtual IO_ERR decode(DataInputStreamSP compressSrc, DataOutputStreamSP &uncompressResult, const CompressionFactory::Header &header) = 0;
	virtual IO_ERR encodeContent(const VectorSP &vec, const DataOutputStreamSP &compressResult, CompressionFactory::Header &header, bool checkSum) = 0;
	virtual ~CompressEncoderDecoder(){}
};

class CompressLZ4 : public CompressEncoderDecoder {
public:
	virtual IO_ERR decode(DataInputStreamSP compressSrc, DataOutputStreamSP &uncompressResult, const CompressionFactory::Header &header) override;
	virtual IO_ERR encodeContent(const VectorSP &vec, const DataOutputStreamSP &compressResult, CompressionFactory::Header &header, bool checkSum) override;
	virtual ~CompressLZ4() override;
private:
	char * newBuffer(int size);
	std::vector<char*> tempBufList_;
};

class CompressDeltaofDelta : public CompressEncoderDecoder {
public:
	virtual IO_ERR decode(DataInputStreamSP compressSrc, DataOutputStreamSP &uncompressResult, const CompressionFactory::Header &header) override;
	virtual IO_ERR encodeContent(const VectorSP &vec, const DataOutputStreamSP &compressResult, CompressionFactory::Header &header, bool checkSum) override;
	virtual ~CompressDeltaofDelta() override;
private:
	char * newBuffer(int size);
	std::vector<char*> tempBufList_;
	static const int maxDecompressedSize_;
	static const int maxCompressedSize_;
};

};//dolphindb
#endif//COMPRESSION_H_