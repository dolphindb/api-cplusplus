#include "Compress.h"
#include "Util.h"
#include "LZ4.h"
#include "DolphinDB.h"

const int MAX_DECOMPRESSED_SIZE = 1 << 16;
const int MAX_COMPRESSED_SIZE = LZ4_compressBound(1 << 16);

namespace dolphindb {

const int CompressDeltaofDelta::maxDecompressedSize_ = 1 << 16;
const int CompressDeltaofDelta::maxCompressedSize_ = (1 << 16) * 2;

CompressEncoderDecoderSP CompressionFactory::GetEncodeDecoder(COMPRESS_METHOD type) {
	switch (type) {
	default:
	case COMPRESS_NONE:
		return NULL;
	case COMPRESS_LZ4:
		return new CompressLZ4;
	case COMPRESS_DELTA:
		return new CompressDeltaofDelta;
	}
}

IO_ERR CompressionFactory::decode(DataInputStreamSP compressSrc, DataOutputStreamSP &uncompressResult, Header &header) {
	CompressEncoderDecoderSP decoder=GetEncodeDecoder((COMPRESS_METHOD)header.compressedType);
	if (decoder.isNull()) {
		return INVALIDDATA;
	}
	return decoder->decode(compressSrc, uncompressResult, header);
}
IO_ERR CompressionFactory::encodeContent(const VectorSP &vec, const DataOutputStreamSP &compressResult, Header &header, bool checkSum) {
	CompressEncoderDecoderSP decoder = GetEncodeDecoder((COMPRESS_METHOD)header.compressedType);
	if (decoder.isNull()) {
		return INVALIDDATA;
	}
	IO_ERR ret = decoder->encodeContent(vec, compressResult, header, checkSum);
	return ret;
}

class CheckSum {
public:
	unsigned int crc32(unsigned int prev, const unsigned char* buf, int len) {
		unsigned int crc = ~prev;
		for (int i = 0; i < len; i++)
			crc = crcTable_[*buf++ ^ (crc & 0xff)] ^ (crc >> 8);
		return (~crc);
	}

	CheckSum() {
		unsigned int POLYNOMIAL = 0xEDB88320;
		unsigned int remainder;
		unsigned char b = 0;
		do {
			remainder = b;
			for (unsigned long bit = 8; bit > 0; --bit) {
				if (remainder & 1)
					remainder = (remainder >> 1) ^ POLYNOMIAL;
				else
					remainder = (remainder >> 1);
			}
			crcTable_[(size_t)b] = remainder;
		} while (0 != ++b);
	}
	int crcTable_[256];
};
static CheckSum g_CheckSum;


class mask {
public:
	unsigned long long MASK_ARRAY[64];
	mask() {
		unsigned long long mask = 1;
		unsigned long long value = 0;
		for (int i = 0; i <64; i++) {
			value = value | mask;
			mask = mask << 1;
			MASK_ARRAY[i] = value;
		}
	}
	~mask() {}
};

class DeltaBufferRead {
public:
	DeltaBufferRead() : buffer_(0), b_(0) {}
	void getBuf(long long* input, int size);
	~DeltaBufferRead() {}
	bool readBits(int bits, unsigned long long* value);
	void rollBack(int bits) {
		if (sizeof(long long) * 8 - bitsAvailable_ >= (size_t)bits) {
			bitsAvailable_ += bits;
		}
		else {
			b_--;
			position_--;
			bitsAvailable_ = bits - sizeof(long long) * 8 + bitsAvailable_;
		}
	}

private:
	long long* buffer_;
	long long* b_;
	int position_ = 0;
	int limit_ = 0;
	int bitsAvailable_ = 0;

	mask m;
	bool flipByte() {
		if (position_ >= limit_)
			return false;
		b_ += 1;
		position_++;
		bitsAvailable_ = sizeof(long long) * 8;
		return true;
	}
};

class DeltaBufferWrite {
public:
	DeltaBufferWrite() : buffer_(0), b_(0) {}
	~DeltaBufferWrite() {};
	void writeBits(unsigned long long value, int bits);
	void skipBit();
	int getPosition() {
		return position_;
	}
	void setBuf(long long *buf, int size);

private:
	mask m;
	void checkAndFlipByte();
	void flipWord();

private:
	long long *buffer_;
	long long *b_;
	int position_ = 0;
	int bitsAvailable_ = sizeof(long long) * 8;
	int limit_ = 0;
};

template <class T>
class DeltaCompressor {
public:
	DeltaCompressor() {
		firstDeltaBits_ = sizeof(T) * 8;
	}
	int writeData(const T *data, int DataSize, long long *buf, int bufferSize);
	~DeltaCompressor() {}

private:
	void close();
	long long previousData_ = 0;
	long long previousDelta_ = 0;
	long long blockData_ = 0;
	int firstDeltaBits_;
	void compressDataNull();
	DeltaBufferWrite write_;

	void writeHeaderData(T data) {
		data = (T)encodeZigZag64((long long)data);
		write_.writeBits(data, sizeof(T) * 8);
	}

	void writeFirstDelta(T data);

	void compressData(T data);

	unsigned long long encodeZigZag64(long long n);
};

template <class T>
class DeltaDecompressor {
public:
	DeltaDecompressor(T nullVal);
	int readData(long long *buf, int bufferSize, T *data, int DataSize);
	~DeltaDecompressor() {}

private:
	T nullVal_;
	long long previousData_ = 0;
	long long previousDelta_ = 0;
	int dataEncodings_[5];
	int firstDeltaBits_;
	long long blockData_ = 0;

	DeltaBufferRead read_;
	bool readHeaderData() {
		unsigned long long flag;
		if (!read_.readBits(5, &flag))
			return false;
		if (flag == 30) {
			unsigned long long closeFlag;
			if (read_.readBits(64, &closeFlag) && closeFlag == 0xFFFFFFFFFFFFFFFFULL) {
				return false;
			}
			else {
				read_.rollBack(5);
				read_.rollBack(64);
			}
		}
		else {
			read_.rollBack(5);
		}
		if (!read_.readBits(sizeof(T) * 8, (unsigned long long*) &blockData_))
			return false;
		blockData_ = decodeZigZag64((unsigned long long) blockData_);
		return true;
	}
	bool decompressData(T* value);
	bool readFirstDelta();
	int findTheFirstZeroBit(int limit);
	long long decodeZigZag64(unsigned long long n) {
		return ((n) >> 1) ^ -((long long)(n & 1));
	}
};

void DeltaBufferWrite::setBuf(long long *buf, int size) {
	buffer_ = buf;
	b_ = buffer_;
	limit_ = size;
	bitsAvailable_ = sizeof(long long) * 8;
	position_ = 1;
}

void DeltaBufferWrite::writeBits(unsigned long long value, int bits) {
	if (bits <= bitsAvailable_) {
		int lastBitPosition = bitsAvailable_ - bits;
		*b_ |= (value << lastBitPosition) & m.MASK_ARRAY[bitsAvailable_ - 1];
		bitsAvailable_ -= bits;
		checkAndFlipByte(); // We could be at 0 bits left because of the <= condition .. would it be faster with
							// the other one?
	}
	else {
		//long long temp = (m.MASK_ARRAY[bits - 1]) & value;
		value &= m.MASK_ARRAY[bits - 1];
		int firstBitPosition = bits - bitsAvailable_;
		*b_ |= (value) >> firstBitPosition;
		bits -= bitsAvailable_;
		flipWord();
		*b_ |= value << (64 - bits);
		bitsAvailable_ -= bits;
	}
}

void DeltaBufferWrite::skipBit() {
	bitsAvailable_--;
	checkAndFlipByte();
}

void DeltaBufferWrite::checkAndFlipByte() {
	// Wish I could avoid this check in most cases...
	if (bitsAvailable_ == 0) {
		flipWord();
	}
}


void DeltaBufferWrite::flipWord() {
	if (position_ >= limit_) {
		throw RuntimeException("out of Compress buffer size");
	}
	b_ = b_ + 1;
	position_++;
	bitsAvailable_ = sizeof(long long) * 8;
}

void DeltaBufferRead::getBuf(long long *buf, int size) {
	buffer_ = buf;
	b_ = buffer_;
	position_ = 1;
	limit_ = size;
	bitsAvailable_ = sizeof(long long) * 8;
}

bool DeltaBufferRead::readBits(int bits, unsigned long long *value) {
	*value = 0;
	if (position_ >= limit_ && bitsAvailable_ == 0)
		return false;
	if (bitsAvailable_ == 0) {
		if (!flipByte()) {
			return false;
		}
	}
	if (bits <= bitsAvailable_) {
		// We can read from this word only
		// Shift to correct position and take only n least significant bits
		*value = ((unsigned long long)(*b_) >> (bitsAvailable_ - bits)) & m.MASK_ARRAY[bits - 1];
		bitsAvailable_ -= bits; // We ate n bits from it
	}
	else {
		// This word and next one, no more (max bits is 64)
		*value = (*b_) & m.MASK_ARRAY[bitsAvailable_ - 1]; // Read what's left first
		bits -= bitsAvailable_;
		if (!flipByte()) {
			return false;
		}
		*value <<= bits; // Give n bits of space to value
		*value |= ((unsigned long long)(*b_) >> (bitsAvailable_ - bits));
		bitsAvailable_ -= bits;
	}
	return true;
}

template <class T>
DeltaDecompressor<T>::DeltaDecompressor(T nullVal) : nullVal_(nullVal) {
	dataEncodings_[0] = 7;
	dataEncodings_[1] = 9;
	dataEncodings_[2] = 16;
	dataEncodings_[3] = 32;
	dataEncodings_[4] = 64;
	firstDeltaBits_ = sizeof(T) * 8;
}

template <class T>
int DeltaDecompressor<T>::readData(long long *buf, int bufferSize, T *data, int dataSize) {
	read_.getBuf(buf, bufferSize);
	int count = 0;
	unsigned long long flag;
	if (!read_.readBits(1, &flag)) {
		return count;
	}
	while (flag == 0) {
		data[count] = nullVal_;
		count++;
		if (!read_.readBits(1, &flag) || count > dataSize) {
			return count;
		}
	}
	if (!readHeaderData()) {
		return count;
	}
	data[count++] = (T)blockData_;
	if (!read_.readBits(1, &flag))
		return count;
	while (flag == 0) {
		data[count] = nullVal_;
		count++;
		if (!read_.readBits(1, &flag) || count > dataSize) {
			return count;
		}
	}
	if (!readFirstDelta()) {
		return count;
	}
	data[count++] = (T)previousData_;
	while (true) {
		if (!decompressData(&data[count]) || count > dataSize) {
			return count;
		}
		count++;
	}
}

template <class T>
bool DeltaDecompressor<T>::decompressData(T *value) {
	int type = findTheFirstZeroBit(6);
	if (type == 6) {
		*value = nullVal_;
		return true;
	}
	if (type > 0) {
		// Delta of delta is non zero. Calculate the new delta. `index`
		// will be used to find the right length for the value that is
		// read.
		int index = type - 1;
		unsigned long long decodedValue = 0;
		if (!read_.readBits(dataEncodings_[index], &decodedValue) || decodedValue == 0xFFFFFFFFFFFFFFFF) {
			return false;
		}
		decodedValue++;
		long long decodedZigZagValue = decodeZigZag64(decodedValue);
		previousDelta_ += decodedZigZagValue;
		previousData_ += previousDelta_;
		*value = (T)previousData_;
	}
	else if (type == 0) {
		previousData_ += previousDelta_;
		*value = (T)previousData_;
	}
	else {
		return false;
	}
	return true;
}

template <class T>
int DeltaDecompressor<T>::findTheFirstZeroBit(int limit) {
	int bits = 0;
	while (bits < limit) {
		unsigned long long bit;
		if (!read_.readBits(1, &bit)) {
			return -1;
		}
		if (bit == 0) {
			return bits;
		}
		bits++;
	}
	return bits;
}

template <class T>
bool DeltaDecompressor<T>::readFirstDelta() {
	unsigned long long flag;
	if (!read_.readBits(5, &flag))
		return false;
	if (flag == 30) {
		unsigned long long closeFlag;
		if (read_.readBits(64, &closeFlag) && closeFlag == 0xFFFFFFFFFFFFFFFFULL) {
			return false;
		}
		else {
			read_.rollBack(5);
			read_.rollBack(64);
		}
	}
	else {
		read_.rollBack(5);
	}
	if (!read_.readBits(firstDeltaBits_, (unsigned long long*)&previousDelta_)) {
		return false;
	}
	previousDelta_ = decodeZigZag64((unsigned long long)previousDelta_);
	previousData_ = blockData_ + previousDelta_;
	return true;
}

template <class T>
int DeltaCompressor<T>::writeData(const T *data, int DataSize, long long *buf, int bufferSize) {
	if (DataSize <= 0) {
		throw RuntimeException("too few data");
	}
	int count = 0;
	int blockSize;
	write_.setBuf(buf, bufferSize);
	while (count<DataSize) {
		if ((data[count] == INT_MIN && sizeof(T) == sizeof(int)) || (data[count] == LLONG_MIN && sizeof(T) == sizeof(long long)) || (data[count] == SHRT_MIN && sizeof(T) == sizeof(short))) {
			write_.writeBits(0, 1);
			count++;
		}
		else {
			break;
		}
	}
	if (count >= DataSize) {
		close();
		return write_.getPosition();
	}
	blockData_ = (long long)data[count];

	write_.writeBits(1, 1);
	writeHeaderData(blockData_);

	count++;
	while (count<DataSize) {
		if ((data[count] == INT_MIN && sizeof(T) == sizeof(int)) || (data[count] == LLONG_MIN && sizeof(T) == sizeof(long long)) || (data[count] == SHRT_MIN && sizeof(T) == sizeof(short))) {
			write_.writeBits(0, 1);
			count++;
		}
		else {
			break;
		}
	}
	if (count >= DataSize) {
		close();
		return write_.getPosition();
	}

	write_.writeBits(1, 1);
	writeFirstDelta(data[count++]);

	while (count<DataSize) {
		compressData(data[count++]);
	}
	close();
	blockSize = write_.getPosition();

	return blockSize;
}

template <class T>
void DeltaCompressor<T>::writeFirstDelta(T data) {
	previousData_ = (long long)data;
	previousDelta_ = previousData_ - blockData_;
	if (((previousData_ < 0 && blockData_ > 0 && previousDelta_ >= 0) || (previousData_ > 0 && blockData_ < 0 && previousDelta_ <= 0)))
		throw RuntimeException("Delta out of range");

	unsigned long long firstD = encodeZigZag64(previousDelta_);
	write_.writeBits(firstD, firstDeltaBits_);
}

template <class T>
void DeltaCompressor<T>::compressDataNull() {
	write_.writeBits(63, 6);
}

template <class T>
void DeltaCompressor<T>::compressData(T data) {
	if ((data == INT_MIN && sizeof(T) == sizeof(int)) || (data == LLONG_MIN && sizeof(T) == sizeof(long long)) || (data == SHRT_MIN && sizeof(T) == sizeof(short))) {
		compressDataNull();
		return;
	}
	long long delta = (long long)data - previousData_;
	if (((data < 0 && previousData_ > 0 && delta >= 0) || (data > 0 && previousData_ < 0 && delta <= 0))) {
		throw RuntimeException("Delta out of range");
	}
	long long deltaOfDelta = delta - previousDelta_;
	if (((delta < 0 && previousDelta_ > 0 && deltaOfDelta >= 0) || (delta > 0 && previousDelta_ < 0 && deltaOfDelta <= 0)))
		throw RuntimeException("Delta out of range");
	if (deltaOfDelta == 0) {
		write_.skipBit();
		previousData_ = (long long)data;
		previousDelta_ = delta;
		return;
	}
	unsigned long long codedData = 0;

	codedData = encodeZigZag64(deltaOfDelta);
	// There are no zeros. Shift by one to fit in x number of bits
	codedData--;

	if (codedData < ((unsigned long long)1 << 7)) {
		write_.writeBits(2, 2);
		write_.writeBits(codedData, 7);
	}
	else if (codedData < ((unsigned long long)1 << 9)) {
		write_.writeBits(6, 3);
		write_.writeBits(codedData, 9);
	}
	else if (codedData < ((unsigned long long)1 << 16)) {
		write_.writeBits(14, 4);
		write_.writeBits(codedData, 16);
	}
	else if (codedData < ((unsigned long long)1 << 32)) {
		write_.writeBits(30, 5);
		write_.writeBits(codedData, 32);
	}
	else {
		write_.writeBits(62, 6);
		write_.writeBits(codedData, 64);
	}
	previousData_ = (long long)data;
	previousDelta_ = delta;
}

template <class T>
unsigned long long DeltaCompressor<T>::encodeZigZag64(long long n) {
	// Note:  the right-shift must be arithmetic
	return (n << 1) ^ (n >> 63);
}

template <class T>
void DeltaCompressor<T>::close() {
	write_.writeBits(62, 6);
	write_.writeBits(0xFFFFFFFFFFFFFFFF, 64);
	write_.skipBit();
}

CompressDeltaofDelta::~CompressDeltaofDelta() {
	for (auto one : tempBufList_) {
		delete[] one;
	}
}

//This part must match code in VectorUnmarshall::start
static IO_ERR writeVectorMetaValue(const CompressionFactory::Header &header, BufferWriter<DataOutputStreamSP> &out) {
	int size = sizeof(header.elementCount) + sizeof(header.colCount) + sizeof(header.extra);
	char* buf = new char[size];
	memcpy(buf, &(header.elementCount), sizeof(header.elementCount));
	memcpy(buf + sizeof(header.elementCount), &(header.colCount), sizeof(header.colCount));
	int actualLength = sizeof(header.elementCount) + sizeof(header.colCount);	
	DATA_TYPE type = (DATA_TYPE)header.dataType;

	if (Util::getCategory(type) == DENARY || type == DT_DECIMAL32_ARRAY || type == DT_DECIMAL64_ARRAY) {
		int scale = header.reserved;
		memcpy(buf + actualLength, &(scale), sizeof(scale));
		actualLength += sizeof(scale);
	}
	return out.start(buf, actualLength);
}

IO_ERR CompressDeltaofDelta::decode(DataInputStreamSP compressSrc, DataOutputStreamSP &decompressResult, const CompressionFactory::Header &header) {
	int unitLength = header.unitLength;
	INDEX start = 0;
	long long fileCursor = 20;
	long long lsn = -1;
	int checksum = -1;
	long long byteSize = header.byteSize;
	INDEX len = header.elementCount;
	int decompressedBufSize;
	int blockSize;
	int count;
	size_t actualRead;
	IO_ERR ret;
	//DATA_TYPE type = (DATA_TYPE)header.dataType;
	bool calcChecksum = (checksum != -1) && !compressSrc->isIntegerReversed();
	unsigned int cksum = 0;
	bool containLSN;
	char *compressedBuf = newBuffer(maxCompressedSize_);
	CheckSum checkSum;

	BufferWriter<DataOutputStreamSP> out(decompressResult);
	ret = writeVectorMetaValue(header,out);
	if (ret != OK)
		return ret;
	while (fileCursor < byteSize && start < len) {
		//new here, after out.start(), it's handled and deleted in DataIutputStreamSP
		char *decompressedBuf = new char[maxDecompressedSize_];
		ret = compressSrc->readInt(blockSize);
		if (ret != OK)
			return ret; 
		if (calcChecksum)
			cksum = checkSum.crc32(cksum, (const unsigned char*)&blockSize, 4);
		if (blockSize < 0) {
			containLSN = true;
			blockSize = blockSize & 2147483647;
		}
		else
			containLSN = false;
		fileCursor += 4;
		if (blockSize <= 0 || blockSize > maxCompressedSize_) {
			std::cout << "Failed to decode. streamType=" + std::to_string(compressSrc->getStreamType()) + " blockSize=" + std::to_string(blockSize) + " fileCursor=" +
				std::to_string(fileCursor) + " fileLength=" + std::to_string(byteSize) + " decodedRows=" + std::to_string(start) + " totalRows=" +
				std::to_string(len) + " ret=" + std::to_string(ret) << std::endl;
			return INVALIDDATA;
		}
		ret = compressSrc->readBytes(compressedBuf, blockSize, actualRead);
		if (ret != OK) {
			std::cout << "Failed to decode. fileCursor=" + std::to_string(fileCursor) + " fileLength=" + std::to_string(byteSize) + " decodedRows=" +
				std::to_string(start) + " totalRows=" + std::to_string(len) + "blockSize=" + std::to_string(blockSize) + " actualRead=" +
				std::to_string(actualRead) + " ret=" + std::to_string(ret) << std::endl;
			return ret;
		}
		fileCursor += blockSize;
		if (calcChecksum) {
			cksum = checkSum.crc32(cksum, (const unsigned char*)compressedBuf, blockSize);
		}
		
		count = std::min((INDEX)maxDecompressedSize_ / unitLength, len - start);
		memset(decompressedBuf, 0, maxDecompressedSize_);
		int actualRead;
		decompressedBufSize = count * unitLength;;
		if (header.unitLength == 4) {
			DeltaDecompressor<int> decoder(INT_MIN);
			actualRead = decoder.readData((long long *)compressedBuf, blockSize / sizeof(long long), (int *)decompressedBuf, count);
		}
		else if (header.unitLength == 8) {
			DeltaDecompressor<long long> decoder(LLONG_MIN);
			actualRead = decoder.readData((long long *)compressedBuf, blockSize / sizeof(long long), (long long *)decompressedBuf, count);
		}
		else {
			DeltaDecompressor<short> decoder(SHRT_MIN);
			actualRead = decoder.readData((long long *)compressedBuf, blockSize / sizeof(long long), (short *)decompressedBuf, count);
		}

		if (actualRead <= 0) {
			std::cout << "Failed to decode. LZ4 block offset=" + std::to_string(fileCursor - blockSize) +
				" fileLength=" + std::to_string(byteSize) + " decodedRows=" + std::to_string(start) + " totalRows=" + std::to_string(len) +
				" blockSize=" + std::to_string(blockSize) + " bufSize=" + std::to_string(decompressedBufSize) << std::endl;
			return INVALIDDATA;
		}
		count = actualRead;
		ret = out.start(decompressedBuf, count*unitLength);
		if (ret != OK)
			return ret;
		start += count;

		if (containLSN && fileCursor + 8 <= byteSize) {
			fileCursor += 8;
			ret = compressSrc->readLong(lsn);
			if (ret != OK)
				return ret;
			if (calcChecksum)
				cksum = checkSum.crc32(cksum, (const unsigned char*)&lsn, sizeof(long long));
		}
	}
	//delete[] compressedBuf;
	//delete[] decompressedBuf;
	return ret;
}

char * CompressDeltaofDelta::newBuffer(int size) {
	char *buf = new char[size];
	tempBufList_.push_back(buf);
	return buf;
}

IO_ERR CompressDeltaofDelta::encodeContent(const VectorSP &vec, const DataOutputStreamSP &compressResult,
									CompressionFactory::Header &header, bool needcheckSum) {
	std::vector<char*> blockBufList;
	std::vector<int> blockSizeList;
	int compressedbyteSize = 0;
	IO_ERR ret = OK;
	bool lsnFlag = false;
	long long *compressedBuf = (long long*)newBuffer(maxCompressedSize_ + sizeof(int));
	//int compressedBufSize = maxCompressedSize_ / sizeof(long long);
	char *decompressedBuf = newBuffer(maxDecompressedSize_);
	//int decompressedBufSize = maxDecompressedSize_ / header.unitLength;
	unsigned int cksum = 0;
	INDEX start = 0;
	{
		//DATA_TYPE type = (DATA_TYPE)header.dataType;
		INDEX len = header.elementCount;
		int offset;
		int blockSize;
		char* blockBuf;
		//size_t actualWritten, actualLength;
		CheckSum checkSum;
		while (start < len) {
			int count = std::min(maxDecompressedSize_ / header.unitLength, len - start);
			memset(compressedBuf, 0, maxCompressedSize_);
			if (ret != OK)
				return ret;
			if (header.unitLength == 4) {
				const int *p = vec->getIntConst(start, count, (int *)decompressedBuf);
				DeltaCompressor<int> coder;
				blockSize = coder.writeData((const int *)p, count, compressedBuf, maxCompressedSize_ / sizeof(long long));
			}
			else if (header.unitLength == 8) {
				const long long *p = vec->getLongConst(start, count, (long long *)decompressedBuf);
				DeltaCompressor<long long> coder;
				blockSize = coder.writeData((const long long *)p, count, compressedBuf, maxCompressedSize_ / sizeof(long long));
			}
			else {
				const short *p = vec->getShortConst(start, count, (short *)decompressedBuf);
				DeltaCompressor<short> coder;
				blockSize = coder.writeData((const short *)p, count, compressedBuf, maxCompressedSize_ / sizeof(long long));
			}
			blockSize = blockSize * sizeof(long long);
			//assert(blockSize < maxCompressedSize_);
			blockBuf = newBuffer(blockSize + sizeof(int));
			offset = 0;
			if (lsnFlag && start + count >= len) {
				int blockSizeWithFlag = blockSize | (1 << 31);
				memcpy(blockBuf + offset, (char*)&blockSizeWithFlag, sizeof(int));
			}
			else
				memcpy(blockBuf + offset, (char*)&blockSize, sizeof(int));
			offset += sizeof(int);
			memcpy(blockBuf + offset, compressedBuf, blockSize);
			offset += blockSize;

			compressedbyteSize += offset;
			blockBufList.push_back(blockBuf);
			blockSizeList.push_back(offset);
			if (needcheckSum)
				cksum = checkSum.crc32(cksum, (const unsigned char*)blockBuf, blockSize + sizeof(int));

			start += count;
		}
	}
	{
		header.byteSize = compressedbyteSize + 20;
		if (needcheckSum) {
			header.checkSum = cksum;
		}
		BufferWriter<DataOutputStreamSP> out(compressResult);
		ret = out.start((char*)&header, sizeof(header));
		if (ret != OK)
			return ret;
		for (size_t i = 0; i < blockBufList.size(); i++) {
			ret = out.start(blockBufList[i], blockSizeList[i]);
			//delete[] blockBufList[i];
			if (ret != OK)
				return ret;
		}
		//delete[] compressedBuf;
		//delete[] decompressedBuf;
	}
	return OK;
}


CompressLZ4::~CompressLZ4() {
	for (auto one : tempBufList_) {
		delete[] one;
	}
}

IO_ERR CompressLZ4::decode(DataInputStreamSP compressSrc, DataOutputStreamSP &uncompressResult, const CompressionFactory::Header &header) {
	int unitLength = header.unitLength;
	long long fileCursor = 20;
	//long long lsn = -1;
	long long byteSize = header.byteSize;
	//bool calcChecksum = false;
	INDEX start = 0;
	INDEX len = header.elementCount;
	int blockSize;
	int count;
	size_t actualRead;
	IO_ERR ret = OK;
	DATA_TYPE type = (DATA_TYPE)header.dataType;
	//bool containLSN;
	
	char *compressedBuf = newBuffer(MAX_COMPRESSED_SIZE);
	bool isMappingMode = (!compressSrc->isIntegerReversed() && type != DT_STRING && type != DT_BLOB && type < ARRAY_TYPE_BASE);
	
	//int pattial = 0;
	BufferWriter<DataOutputStreamSP> out(uncompressResult);
	ret = writeVectorMetaValue(header, out);
	if (ret != OK)
		return ret;
	while (fileCursor < byteSize && start < len) {
		//new here, after out.start(), it's handled and deleted in DataIutputStreamSP
		char *decompressedBuf = new char[MAX_DECOMPRESSED_SIZE];
		ret = compressSrc->readInt(blockSize);
		if (ret != OK)
			return ret;
		if (blockSize < 0) {
			//containLSN = true;
			blockSize = blockSize & 2147483647;
		}
		//else
		//	containLSN = false;
		fileCursor += 4;
		if (ret != OK || blockSize <= 0 || blockSize > MAX_COMPRESSED_SIZE || fileCursor + blockSize > byteSize) {
			std::cout << "Failed to decode. blockSize=" + std::to_string(blockSize) + " fileCursor=" +
				std::to_string(fileCursor) + " fileLength=" + std::to_string(byteSize) + " decodedRows=" + std::to_string(start) + " totalRows=" +
				std::to_string(len) + " ret=" + std::to_string(ret) << std::endl;;
			return INVALIDDATA;
		}
		ret = compressSrc->readBytes(compressedBuf, blockSize, actualRead);
		if (ret != OK) {
			std::cout << "Failed to decode. fileCursor=" + std::to_string(fileCursor) + " fileLength=" + std::to_string(byteSize) + " decodedRows=" +
				std::to_string(start) + " totalRows=" + std::to_string(len) + "blockSize=" + std::to_string(blockSize) + " actualRead=" +
				std::to_string(actualRead) + " ret=" + std::to_string(ret) << std::endl;
			return ret;
		}
		fileCursor += blockSize;

		//if (calcChecksum) {
		//	cksum = incCheckSum(cksum, (const unsigned char*)compressedBuf_, blockSize);
		//}

		if (isMappingMode) {
			count = std::min((INDEX)MAX_DECOMPRESSED_SIZE / unitLength, len - start);
			int bytes = LZ4_decompress_safe(compressedBuf, decompressedBuf, blockSize, MAX_DECOMPRESSED_SIZE);
			if (bytes <= 0){
				long long decompressedBufSize = count * unitLength;
				std::cout << "Failed to decode. LZ4 block offset=" + std::to_string(fileCursor - blockSize) +
					" fileLength=" + std::to_string(byteSize) + " decodedRows=" + std::to_string(start) + " totalRows=" + std::to_string(len) +
					" blockSize=" + std::to_string(blockSize) + " bufSize=" + std::to_string(decompressedBufSize) << std::endl;
				return INVALIDDATA;
			}
			count = bytes / unitLength;
			ret = out.start(decompressedBuf,count*unitLength);
			if (ret != OK)
				return ret;
			start += count;
		}
		else{
			int bytes = LZ4_decompress_safe(compressedBuf, decompressedBuf, blockSize, MAX_DECOMPRESSED_SIZE);
			if (bytes < 0) {
				std::cout << "Failed to decode. LZ4 block offset=" + std::to_string(fileCursor - blockSize) +
					" fileLength=" + std::to_string(byteSize) + " decodedRows=" + std::to_string(start) + " totalRows=" + std::to_string(len) +
					"blockSize=" + std::to_string(blockSize) << std::endl;
				return INVALIDDATA;
			}
			if ((DATA_TYPE)header.dataType == DT_SYMBOL) {
				//int *buf = (int*)decompressedBuf;
				count = bytes / unitLength;

				bool done = false;
				if (start + count > len) {
					count = len - start;
					done = true;	
				}
				ret = out.start(decompressedBuf, count*unitLength);
				if (ret != OK)
					return ret;
				start += count;
				if (done) {
					return OK;
				}
			}
			else {
				ret = out.start(decompressedBuf, bytes);
				if (ret != OK)
					return ret;
			}
		}
	}
	return ret;
}

char * CompressLZ4::newBuffer(int size) {
	char *buf = new char[size];
	tempBufList_.push_back(buf);
	return buf;
}


IO_ERR CompressLZ4::encodeContent(const VectorSP &vec, const DataOutputStreamSP &compressResult, CompressionFactory::Header &header, bool needcheckSum) {
	std::vector<char*> blockBufList;
	std::vector<int> blockSizeList;
	int compressedbyteSize = 0;
	IO_ERR ret = OK;
	unsigned int cksum = 0;
	int decompressedBufSize;
	int count;
	char *decompressedBuf = newBuffer(MAX_DECOMPRESSED_SIZE);//
	{
		DATA_TYPE type = (DATA_TYPE)header.dataType;
		INDEX start = 0;
		INDEX len = header.elementCount;
		int offset = 0;
		int blockSize;
		//size_t actualWritten, actualLength;
		CheckSum checkSum;
		bool lsnFlag = false;
		if (type != DT_SYMBOL) {
			while (start < len){
				char* blockBuf = newBuffer(MAX_COMPRESSED_SIZE + sizeof(int));
				memset(blockBuf, 0, MAX_COMPRESSED_SIZE);
				if (type != DT_STRING) {
					decompressedBufSize = vec->serialize(decompressedBuf, MAX_DECOMPRESSED_SIZE, start, offset, count, offset);
				}
				else {
					decompressedBufSize = vec->serialize(decompressedBuf, MAX_DECOMPRESSED_SIZE, start, 0, count, offset);
					decompressedBufSize -= offset;
					if (decompressedBufSize == 0)
						decompressedBufSize = vec->serialize(decompressedBuf, MAX_DECOMPRESSED_SIZE, start, offset, count, offset);
					if (decompressedBufSize == 0)
						return TOO_LARGE_DATA;
				}
				if (ret != OK)
					return ret;
				
				blockSize = LZ4_compress_default(decompressedBuf, blockBuf + sizeof(int), decompressedBufSize, MAX_COMPRESSED_SIZE);
				
				if (lsnFlag && (start + count >= len)) {
					int blockSizeWithFlag = blockSize | (1 << 31);
					memcpy(blockBuf, (char*)&blockSizeWithFlag, sizeof(int));
				}
				else
					memcpy(blockBuf, (char*)&blockSize, sizeof(int));
				blockSize += sizeof(int);

				if (needcheckSum)
					cksum = checkSum.crc32(cksum, (const unsigned char*)blockBuf, blockSize);
				
				compressedbyteSize += blockSize;
				start += count;
				blockBufList.push_back(blockBuf);
				blockSizeList.push_back(blockSize);
			}
		}
		else
		{
			throw RuntimeException("Vector compression of symbol type is not supported. ");
		}
	}
	{
		header.byteSize = compressedbyteSize + 20;
		if (needcheckSum) {
			header.checkSum = cksum;
		}
		BufferWriter<DataOutputStreamSP> out(compressResult);
		ret = out.start((char*)&header, sizeof(header));
		if (ret != OK)
			return ret;
		for (size_t i = 0; i < blockBufList.size(); i++) {
			ret = out.start(blockBufList[i], blockSizeList[i]);
			//delete[] blockBufList[i];
			if (ret != OK)
				return ret;
		}
		//delete[] decompressedBuf;
	}
	return OK;
}

};//dolphindb