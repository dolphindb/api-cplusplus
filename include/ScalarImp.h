// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include "internal/Decimal.h"

#include "Constant.h"
#include "Format.h"
#include "Guid.h"
#include "SysIO.h"
#include "Util.h"

#define FMT_UNICODE 0
#define FMT_HEADER_ONLY
#include "spdlog/fmt/bundled/format.h"

#include <climits>
#include <utility>

#if defined(_MSC_VER)
#pragma warning( push )
#pragma warning( disable : 4100 )
#elif defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#else // gcc
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

namespace dolphindb {

class Void : public Constant
{
  public:
    explicit Void(bool explicitNull = false) { setNothing(!explicitNull); }
    ConstantSP getInstance() const override { return ConstantSP(new Void(!isNothing())); }
    ConstantSP getValue() const override { return ConstantSP(new Void(!isNothing())); }
    DATA_TYPE getType() const override { return DT_VOID; }
    DATA_TYPE getRawType() const override { return DT_VOID; }
    DATA_CATEGORY getCategory() const override { return NOTHING; }
    std::string getString() const override { return Constant::EMPTY; }
    std::string getScript() const override { return isNothing() ? Constant::EMPTY : Constant::NULL_STR; }
    const std::string &getStringRef() const override { return Constant::EMPTY; }
    char getBool() const override { return CHAR_MIN; }
    char getChar() const override { return CHAR_MIN; }
    short getShort() const override { return SHRT_MIN; }
    int getInt() const override { return INT_MIN; }
    long long getLong() const override { return LLONG_MIN; }
    float getFloat() const override { return FLT_NMIN; }
    double getDouble() const override { return DBL_NMIN; }
    bool isNull() const override { return true; }
    bool isNull(INDEX start, int len, char *buf) const override { return set1<char>(start, len, buf, 1); }
    bool isValid(INDEX start, int len, char *buf) const override { return set1<char>(start, len, buf, 0); }
    void nullFill(const ConstantSP &val) override {}

    bool getBool(INDEX start, int len, char *buf) const override { return set1<char>(start, len, buf); }
    bool getChar(INDEX start, int len, char *buf) const override { return set1(start, len, buf); }
    bool getShort(INDEX start, int len, short *buf) const override { return set1(start, len, buf); }
    bool getInt(INDEX start, int len, int *buf) const override { return set1(start, len, buf); }
    bool getLong(INDEX start, int len, long long *buf) const override { return set1(start, len, buf); }
    bool getIndex(INDEX start, int len, INDEX *buf) const override { return set1(start, len, buf); }
    bool getFloat(INDEX start, int len, float *buf) const override { return set1(start, len, buf); }
    bool getDouble(INDEX start, int len, double *buf) const override { return set1(start, len, buf); }
    bool getString(INDEX start, int len, std::string **buf) const override
    {
        return set1(start, len, buf, &Constant::EMPTY);
    }
    bool getBinary(INDEX start, int len, int unitLength, unsigned char *buf) const override
	{
        return set1<unsigned char>(start, len * unitLength, buf, 0);
	}
    const char *getBoolConst(INDEX start, int len, char *buf) const override { return set2(start, len, buf); }
    const char *getCharConst(INDEX start, int len, char *buf) const override { return set2(start, len, buf); }
    const short *getShortConst(INDEX start, int len, short *buf) const override { return set2(start, len, buf); }
    const int *getIntConst(INDEX start, int len, int *buf) const override { return set2(start, len, buf); }
    const long long *getLongConst(INDEX start, int len, long long *buf) const override { return set2(start, len, buf); }
    const INDEX *getIndexConst(INDEX start, int len, INDEX *buf) const override { return set2(start, len, buf); }
    const float *getFloatConst(INDEX start, int len, float *buf) const override { return set2(start, len, buf); }
    const double *getDoubleConst(INDEX start, int len, double *buf) const override { return set2(start, len, buf); }
    std::string **getStringConst(INDEX start, int len, std::string **buf) const override
    {
        std::ignore = start;
		std::fill(buf, buf + len, &Constant::EMPTY);
        return buf;
    }
    int serialize(char *buf, int bufSize, INDEX indexStart, int offset, int &numElement, int &partial) const override
    {

        std::ignore = bufSize;
        std::ignore = indexStart;
        std::ignore = offset;
        buf[0] = isNothing() ? 0 : 1;
        numElement = 1;
        partial = 0;
        return 1;
    }
    IO_ERR deserialize(DataInputStream *in, INDEX indexStart, INDEX targetNumElement, INDEX &numElement) override
    {

        std::ignore = indexStart;
        std::ignore = targetNumElement;
        bool explicitNull;
        IO_ERR ret = in->read(explicitNull);
        if (ret == OK)
            numElement = 1;
        setNothing(!explicitNull);
        return ret;
    }
    int compare(INDEX index, const ConstantSP &target) const override { return target->getType() == DT_VOID ? 0 : -1; }
    long long getAllocatedMemory() const override { return sizeof(Void); }
private:
	template<typename T>
    bool set1(INDEX start, int len, T *buf, T value = getNullValue<T>()) const
    {
        std::ignore = start;
		std::fill(buf, buf + len, value);
        return true;
    }
	template<typename T>
    const T * set2(INDEX start, int len, T *buf, T value = getNullValue<T>()) const
    {
        std::ignore = start;
		std::fill(buf, buf + len, value);
        return buf;
    }
};

class Int128 : public Constant
{
  public:
    Int128() {};
    explicit Int128(const unsigned char *data) { memcpy(uuid_, data, 16); }
    ~Int128() override = default;
    const unsigned char *bytes() const { return uuid_; }
    std::string getString() const override { return toString(uuid_); }
    Guid getInt128() const override { return Guid(uuid_); }
    const unsigned char *getBinary() const override { return uuid_; }
    bool isNull() const override
    {
        const auto *a = (const unsigned char *)uuid_;
        return (*(long long *)a) == 0 && (*(long long *)(a + 8)) == 0;
    }
    void setNull() override { memset((void *)uuid_, 0, 16); }
    void nullFill(const ConstantSP &val) override
    {
        if (isNull())
            memcpy(uuid_, val->getInt128().bytes(), 16);
    }
    bool isNull(INDEX start, int len, char *buf) const override
    {
        char null = isNull();
        for (int i = 0; i < len; ++i)
            buf[i] = null;
        return true;
    }
    bool isValid(INDEX start, int len, char *buf) const override
    {
        char valid = !isNull();
        for (int i = 0; i < len; ++i)
            buf[i] = valid;
        return true;
    }
    int compare(INDEX index, const ConstantSP &target) const override
    {
        std::ignore = index;
        return guid_.compare(target->getInt128());
    }
    void setBinary(const unsigned char *val, int unitLength) override
    {
        std::ignore = unitLength;
        memcpy(uuid_, val, 16);
    }
    bool getBinary(INDEX start, int len, int unitLength, unsigned char *buf) const override
    {
        std::ignore = start;
        if (unitLength != 16)
            return false;
        for (int i = 0; i < len; ++i) {
            memcpy(buf, uuid_, 16);
            buf += 16;
        }
        return true;
    }
    const unsigned char *getBinaryConst(INDEX start, int len, int unitLength, unsigned char *buf) const override
    {
        std::ignore = start;
        std::ignore = unitLength;
        unsigned char *original = buf;
        for (int i = 0; i < len; ++i) {
            memcpy(buf, uuid_, 16);
            buf += 16;
        }
        return original;
    }
    ConstantSP getInstance() const override { return new Int128(); }
    ConstantSP getValue() const override { return new Int128(uuid_); }
    DATA_TYPE getType() const override { return DT_INT128; }
    DATA_TYPE getRawType() const override { return DT_INT128; }
    DATA_CATEGORY getCategory() const override { return BINARY; }
    long long getAllocatedMemory() const override { return sizeof(Int128); }
    int serialize(char *buf, int bufSize, INDEX indexStart, int offset, int &numElement, int &partial) const override
    {
        std::ignore = indexStart;
        int len = 16 - offset;
        if (len < 0)
            return -1;
        if (bufSize >= len) {
            numElement = 1;
            partial = 0;
            memcpy(buf, ((char *)uuid_) + offset, len);
            return len;
        }
        len = bufSize;
        numElement = 0;
        partial = offset + bufSize;
        memcpy(buf, ((char *)uuid_) + offset, len);
        return len;
    }
    IO_ERR deserialize(DataInputStream *in, INDEX indexStart, INDEX targetNumElement, INDEX &numElement) override
    {
        std::ignore = indexStart;
        std::ignore = targetNumElement;
        IO_ERR ret = in->readBytes((char *)uuid_, 16, false);
        if (ret == OK)
            numElement = 1;
        return ret;
    }
    int getHash(int buckets) const override { return murmur32_16b(uuid_) % buckets; }
    static std::string toString(const unsigned char *data)
    {
        char buf[32];
        Util::toHex(data, 16, Util::LITTLE_ENDIAN_ORDER, buf);
        return std::string(buf, 32);
    }
    static Int128 *parseInt128(const char *str, size_t len)
    {
        unsigned char buf[16];
        if (len == 0) {
            memset(buf, 0, 16);
            return new Int128(buf);
        }
        if (len == 32 && Util::fromHex(str, len, Util::LITTLE_ENDIAN_ORDER, buf))
            return new Int128(buf);
        return nullptr;
    }
    static bool parseInt128(const char *str, size_t len, unsigned char *buf)
    {
        if (len == 0) {
            memset(buf, 0, 16);
            return true;
        }
        if (len == 32 && Util::fromHex(str, len, Util::LITTLE_ENDIAN_ORDER, buf))
            return true;
        return false;
    }

  protected:
    union {
        mutable unsigned char uuid_[16];
        Guid guid_{};
    };
};

class Uuid : public Int128 {
public:
	explicit Uuid(bool newUuid = false);
	explicit Uuid(const unsigned char* uuid);
	Uuid(const char* uuid, size_t len);
	Uuid(const Uuid& copy);
	~Uuid() override = default;
	ConstantSP getInstance() const override {return new Uuid(false);}
	ConstantSP getValue() const override {return new Uuid(uuid_);}
	DATA_TYPE getType() const override {return DT_UUID;}
	DATA_TYPE getRawType() const override { return DT_INT128;}
	std::string getString() const override { return Guid::getString(uuid_);}
	static Uuid* parseUuid(const char* str, size_t len);
	static bool parseUuid(const char* str, size_t len, unsigned char *buf);
};

class IPAddr : public Int128 {
public:
	IPAddr() = default;
	IPAddr(const char* ip, int len);
	explicit IPAddr(const unsigned char* data);
	~IPAddr() override = default;
	ConstantSP getInstance() const override {return new IPAddr();}
	ConstantSP getValue() const override {return new IPAddr(uuid_);}
	DATA_TYPE getType() const override {return DT_IP;}
	DATA_TYPE getRawType() const override { return DT_INT128;}
	std::string getString() const override { return toString(uuid_);}
	static std::string toString(const unsigned char* data);
	static IPAddr* parseIPAddr(const char* str, size_t len);
	static bool parseIPAddr(const char* str, size_t len, unsigned char* buf);

private:
	static bool parseIP4(const char* str, size_t len, unsigned char* buf);
	static bool parseIP6(const char* str, size_t len, unsigned char* buf);
};

class String : public Constant
{
  public:
    explicit String(std::string val = "", bool blob = false) : val_(std::move(val)), blob_(blob)
    {
        if (!blob_) {
            if (val_.find('\0') != std::string::npos) {
                throw RuntimeException("A String cannot contain the character '\\0'");
            }
        }
    }
    ~String() override = default;
    char getBool() const override { throw IncompatibleTypeException(DT_BOOL, DT_STRING); }
    char getChar() const override { throw IncompatibleTypeException(DT_CHAR, DT_STRING); }
    short getShort() const override { throw IncompatibleTypeException(DT_SHORT, DT_STRING); }
    int getInt() const override { throw IncompatibleTypeException(DT_INT, DT_STRING); }
    long long getLong() const override { throw IncompatibleTypeException(DT_LONG, DT_STRING); }
    INDEX getIndex() const override { throw IncompatibleTypeException(DT_INDEX, DT_STRING); }
    float getFloat() const override { throw IncompatibleTypeException(DT_FLOAT, DT_STRING); }
    double getDouble() const override { throw IncompatibleTypeException(DT_DOUBLE, DT_STRING); }
    std::string getString() const override { return val_; }
    std::string getScript() const override { return Util::literalConstant(val_); }
    const std::string &getStringRef() const override { return val_; }
    const std::string &getStringRef(INDEX index) const override { return val_; }
    bool isNull() const override { return val_.empty(); }
    void setString(const std::string &val) override
    {
        if (!blob_) {
            if (val.find('\0') != std::string::npos) {
                throw RuntimeException("A String cannot contain the character '\\0'");
            }
        }
        val_ = val;
    }
    void setNull() override { val_ = ""; }
    void nullFill(const ConstantSP &val) override
    {
        if (isNull())
            val_ = val->getStringRef();
    }
    bool isNull(INDEX start, int len, char *buf) const override
    {
        char null = isNull();
        for (int i = 0; i < len; ++i)
            buf[i] = null;
        return true;
    }
    bool isValid(INDEX start, int len, char *buf) const override
    {
        char valid = !isNull();
        for (int i = 0; i < len; ++i)
            buf[i] = valid;
        return true;
    }
    bool getString(INDEX start, int len, std::string **buf) const override
    {
        for (int i = 0; i < len; ++i)
            buf[i] = &val_;
        return true;
    }
    std::string **getStringConst(INDEX start, int len, std::string **buf) const override
    {
        for (int i = 0; i < len; ++i)
            buf[i] = &val_;
        return buf;
    }
    char **getStringConst(INDEX start, int len, char **buf) const override
    {
        char *val = (char *)val_.c_str();
        for (int i = 0; i < len; ++i)
            buf[i] = val;
        return buf;
    }
    ConstantSP getInstance() const override { return ConstantSP(new String("", blob_)); }
    ConstantSP getValue() const override { return ConstantSP(new String(val_, blob_)); }
    DATA_TYPE getType() const override { return blob_ == false ? DT_STRING : DT_BLOB; }
    DATA_TYPE getRawType() const override { return blob_ == false ? DT_STRING : DT_BLOB; }
    DATA_CATEGORY getCategory() const override { return LITERAL; }
    long long getAllocatedMemory() const override { return sizeof(std::string) + val_.size(); }
    int serialize(char *buf, int bufSize, INDEX indexStart, int offset, int &numElement, int &partial) const override
    {
        std::ignore = indexStart;
        int len = static_cast<int>(val_.size());
        if (len >= 262144) {
            throw RuntimeException("String too long, Serialization failed, length must be less than 256K bytes");
        }
        if (!blob_) {
            if (offset > len)
                return -1;
            if (bufSize >= len - offset + 1) {
                numElement = 1;
                partial = 0;
                memcpy(buf, val_.data() + offset, len - offset + 1);
                return len - offset + 1;
            }
            numElement = 0;
            partial = offset + bufSize;
            memcpy(buf, val_.data() + offset, bufSize);
            return bufSize;
        }
        int bytes = 0;
        if (offset > 0) {
            offset -= 4;
            if (UNLIKELY(offset < 0))
                return -1;
        } else {
            if (UNLIKELY((size_t)bufSize < sizeof(int)))
                return 0;
            int sz = static_cast<int>(val_.size());
            memcpy(buf, &sz, sizeof(int));
            buf += sizeof(int);
            bufSize -= sizeof(int);
            bytes += sizeof(int);
        }
        if (bufSize >= len - offset) {
            numElement = 1;
            partial = 0;
            memcpy(buf, val_.data() + offset, len - offset);
            bytes += len - offset;
        } else {
            numElement = 0;
            partial = sizeof(int) + offset + bufSize;
            memcpy(buf, val_.data() + offset, bufSize);
            bytes += bufSize;
        }
        return bytes;
    }
    IO_ERR deserialize(DataInputStream *in, INDEX indexStart, INDEX targetNumElement, INDEX &numElement) override
    {
        std::ignore = indexStart;
        std::ignore = targetNumElement;
        IO_ERR ret;
        if (blob_) {
            int len;
            if ((ret = in->read(len)) != OK)
                return ret;
            std::unique_ptr<char[]> buf(new char[len]);
            if ((ret = in->read(buf.get(), len)) != OK)
                return ret;
            val_.clear();
            val_.append(buf.get(), len);
        } else {
            ret = in->readString(val_);
            if (ret == OK)
                numElement = 1;
        }
        return ret;
    }
    int compare(INDEX index, const ConstantSP &target) const override { return val_.compare(target->getString()); }
    int getHash(int buckets) const override { return murmur32(val_.data(), val_.size()) % buckets; }

  private:
    mutable std::string val_;
    bool blob_;
};

template <class T>
class AbstractScalar: public Constant{
public:
	explicit AbstractScalar(T val=0):val_(val){}
	~AbstractScalar() override = default;
	char getBool() const override {return isNull()?CHAR_MIN:(bool)val_;}
	char getChar() const override {return isNull() ? CHAR_MIN : static_cast<char>(val_);}
	short getShort() const override {return isNull() ? SHRT_MIN : static_cast<short>(val_);}
	int getInt() const override {return isNull() ? INT_MIN : static_cast<int>(val_);}
	long long getLong() const override {return isNull() ? LLONG_MIN : static_cast<long long>(val_);}
	INDEX getIndex() const override {return isNull() ? INDEX_MIN : static_cast<INDEX>(val_);}
	float getFloat() const override {return isNull() ? FLT_NMIN : static_cast<float>(val_);}
	double getDouble() const override {return isNull() ? DBL_NMIN : static_cast<double>(val_);}

	void setBool(char val) override {if(val != CHAR_MIN) val_=(T)val; else setNull();}
	void setChar(char val) override {if(val != CHAR_MIN) val_=(T)val; else setNull();}
	void setShort(short val) override {if(val != SHRT_MIN) val_=(T)val; else setNull();}
	void setInt(int val) override {if(val != INT_MIN) val_=(T)val; else setNull();}
	void setLong(long long val) override {if(val != LLONG_MIN) val_=(T)val; else setNull();}
	void setIndex(INDEX val) override {if(val != INDEX_MIN) val_=(T)val; else setNull();}
	void setFloat(float val) override {if(val != FLT_NMIN) val_=(T)val; else setNull();}
	void setDouble(double val) override {if(val != DBL_NMIN) val_=(T)val; else setNull();}
	void setString(const std::string& val) override {}
	bool isNull() const override = 0;
	std::string getScript() const override {
		if(isNull()){
			std::string str("00");
			return str.append(1, Util::getDataTypeSymbol(getType()));
		}
		return getString();
	}

	void nullFill(const ConstantSP& val) override {
		if(isNull()){
			if(val->getCategory()==FLOATING)
				val_ = static_cast<T>(val->getDouble());
			else
				val_ = static_cast<T>(val->getLong());
		}
	}
	bool isNull(INDEX start, int len, char* buf) const override {
		char null=isNull();
		for(int i=0;i<len;++i)
			buf[i]=null;
		return true;
	}
	bool isValid(INDEX start, int len, char* buf) const override {
		char valid=!isNull();
		for(int i=0;i<len;++i)
			buf[i]=valid;
		return true;
	}
	bool getBool(INDEX start, int len, char* buf) const override {
		char tmp=isNull()?CHAR_MIN:(bool)val_;
		for(int i=0;i<len;++i)
			buf[i]=tmp;
		return true;
	}
	const char* getBoolConst(INDEX start, int len, char* buf) const override {
		char tmp=isNull()?CHAR_MIN:(bool)val_;
		for(int i=0;i<len;++i)
			buf[i]=tmp;
		return buf;
	}
	bool getChar(INDEX start, int len, char* buf) const override {
		char tmp = isNull() ? CHAR_MIN : static_cast<char>(val_);
		for(int i=0;i<len;++i)
			buf[i]=tmp;
		return true;
	}
	const char* getCharConst(INDEX start, int len, char* buf) const override {
		char tmp = isNull() ? CHAR_MIN : static_cast<char>(val_);
		for(int i=0;i<len;++i)
			buf[i]=tmp;
		return buf;
	}
	bool getShort(INDEX start, int len, short* buf) const override {
		short tmp = isNull() ? SHRT_MIN : static_cast<short>(val_);
		for(int i=0;i<len;++i)
			buf[i]=tmp;
		return true;
	}
	const short* getShortConst(INDEX start, int len, short* buf) const override {
		short tmp = isNull() ? SHRT_MIN : static_cast<short>(val_);
		for(int i=0;i<len;++i)
			buf[i]=tmp;
		return buf;
	}
	bool getInt(INDEX start, int len, int* buf) const override {
		int tmp = isNull() ? INT_MIN : static_cast<int>(val_);
		for(int i=0;i<len;++i)
			buf[i]=tmp;
		return true;
	}
	const int* getIntConst(INDEX start, int len, int* buf) const override {
		int tmp = isNull() ? INT_MIN : static_cast<int>(val_);
		for(int i=0;i<len;++i)
			buf[i]=tmp;
		return buf;
	}
	bool getLong(INDEX start, int len, long long* buf) const override {
		long long tmp = isNull() ? LLONG_MIN : static_cast<long long>(val_);
		for(int i=0;i<len;++i)
			buf[i]=tmp;
		return true;
	}
	const long long* getLongConst(INDEX start, int len, long long* buf) const override {
		long long tmp = isNull() ? LLONG_MIN : static_cast<long long>(val_);
		for(int i=0;i<len;++i)
			buf[i]=tmp;
		return buf;
	}
	bool getIndex(INDEX start, int len, INDEX* buf) const override {
		INDEX tmp = isNull() ? INDEX_MIN : static_cast<INDEX>(val_);
		for(int i=0;i<len;++i)
			buf[i]=tmp;
		return true;
	}
	const INDEX* getIndexConst(INDEX start, int len, INDEX* buf) const override {
		INDEX tmp = isNull() ? INDEX_MIN : static_cast<INDEX>(val_);
		for(int i=0;i<len;++i)
			buf[i]=tmp;
		return buf;
	}
	bool getFloat(INDEX start, int len, float* buf) const override {
		float tmp = isNull() ? FLT_NMIN : static_cast<float>(val_);
		for(int i=0;i<len;++i)
			buf[i]=tmp;
		return true;
    }
	const float* getFloatConst(INDEX start, int len, float* buf) const override {
		float tmp = isNull() ? FLT_NMIN : static_cast<float>(val_);
		for(int i=0;i<len;++i)
			buf[i]=tmp;
		return buf;
    }
	bool getDouble(INDEX start, int len, double* buf) const override {
		double tmp=isNull()?DBL_NMIN:val_;
		for(int i=0;i<len;++i)
			buf[i]=tmp;
		return true;
	}
	const double* getDoubleConst(INDEX start, int len, double* buf) const override {
		double tmp=isNull()?DBL_NMIN:val_;
		for(int i=0;i<len;++i)
			buf[i]=tmp;
		return buf;
	}
	long long getAllocatedMemory() const override{return sizeof(AbstractScalar);}
	int serialize(char* buf, int bufSize, INDEX indexStart, int offset, int& numElement, int& partial) const override {
		int len = sizeof(T)-offset;
		if(len < 0)
			return -1;
		if(bufSize >= len){
			numElement = 1;
			partial = 0;
			memcpy(buf,((char*)&val_)+offset, len);
			return len;
		}
		len = bufSize;
		numElement = 0;
		partial = offset + bufSize;
		memcpy(buf, ((char *)&val_) + offset, len);
		return len;
	}

	int32_t getDecimal32(INDEX index, int scale) const override {
		int32_t result = 0;
		getDecimal32(index, /*len*/1, scale, &result);
		return result;
	}
	int64_t getDecimal64(INDEX index, int scale) const override {
		int64_t result = 0;
		getDecimal64(index, /*len*/1, scale, &result);
		return result;
	}

	wide_integer::int128 getDecimal128(INDEX index, int scale) const override {
		wide_integer::int128 result = 0;
		getDecimal128(index, /*len*/1, scale, &result);
		return result;
	}

	bool getDecimal32(INDEX start, int len, int scale, int32_t *buf) const override {
		return getDecimal(start, len, scale, buf);
	}
	bool getDecimal64(INDEX start, int len, int scale, int64_t *buf) const override {
		return getDecimal(start, len, scale, buf);
	}

	bool getDecimal128(INDEX start, int len, int scale, wide_integer::int128 *buf) const override {
		return getDecimal(start, len, scale, buf);
	}

	bool add(INDEX start, INDEX length, long long inc) override {
		if(isNull())
			return false;
		val_ += static_cast<T>(inc);
		return true;
	}

	bool add(INDEX start, INDEX length, double inc) override {
		if(isNull())
			return false;
		val_ += static_cast<T>(inc);
		return true;
	}

	int compare(INDEX index, const ConstantSP& target) const override {
		if(getCategory() == FLOATING){
			T val= (T)target->getDouble();
			return val_==val?0:(val_<val?-1:1);
		}
		T val= (T)target->getLong();
		return val_==val?0:(val_<val?-1:1);
	}

    IO_ERR deserialize(DataInputStream *in, INDEX start, INDEX len, INDEX &numElement) override
    {
        std::ignore = start;
        std::ignore = len;
        IO_ERR ret = in->read(val_);
        if (ret == OK)
            numElement = 1;
        return ret;
    }

private:
	template <typename R>
	bool getDecimal(INDEX start, int len, int scale, R *buf) const
	{
		std::ignore = start;
		R value{};
		decimal_util::valueToDecimalraw(val_, scale, &value);
		std::fill(buf, buf + len, value);
		return true;
	}
protected:
	static std::string DoubleToString(double val){
		double absVal = std::abs(val);
		if((absVal>0 && absVal<=0.000001) || absVal>=1000000.0) {
			static NumberFormat floatingSciFormat("0.0#####E0");
			return floatingSciFormat.format(val);
		}
		static NumberFormat floatingNormFormat("0.######");
		return floatingNormFormat.format(val);
	}

	// set rounded float point value to buf
	template<typename U>
	bool setF1(INDEX start, int len, U* buf) const {
		std::ignore = start;
		U tmp = isNull() ? getNullValue<U>() : static_cast<U>(std::round(val_));
		std::fill(buf, buf + len, tmp);
		return true;
	}
	template<typename U>
	const U* setF2(INDEX start, int len, U* buf) const {
		std::ignore = start;
		U tmp = isNull() ? getNullValue<U>() : static_cast<U>(std::round(val_));
		std::fill(buf, buf + len, tmp);
		return buf;
	}
	T val_;
};

class Bool : public AbstractScalar<char>
{
  public:
    explicit Bool(bool val = false) : AbstractScalar(val) {}
    explicit Bool(char val) : AbstractScalar(val) {}
    ~Bool() override = default;
    bool isNull() const override { return val_ == CHAR_MIN; }
    void setNull() override { val_ = CHAR_MIN; }
    void setBool(char val) override { val_ = val; }
    DATA_TYPE getType() const override { return DT_BOOL; }
    DATA_TYPE getRawType() const override { return DT_BOOL; }
    ConstantSP getInstance() const override { return ConstantSP(new Bool()); }
    ConstantSP getValue() const override { return ConstantSP(new Bool(val_)); }
    DATA_CATEGORY getCategory() const override { return LOGICAL; }
    std::string getString() const override { return toString(val_); }
    bool add(INDEX start, INDEX length, long long inc) override { return false; }
    bool add(INDEX start, INDEX length, double inc) override { return false; }
    static Bool *parseBool(const std::string &str)
    {
        Bool *p = nullptr;

        if (str == "00") {
            p = new Bool();
            p->setNull();
        } else if (Util::equalIgnoreCase(str, "true")) {
            p = new Bool(true);
        } else if (Util::equalIgnoreCase(str, "false")) {
            p = new Bool(false);
        } else {
            p = new Bool(atoi(str.c_str()) != 0);
        }
        return p;
    }
    static std::string toString(char val)
    {
        if (val == CHAR_MIN)
            return "";
        if (val)
            return "1";
        return "0";
    }
};

class Char: public AbstractScalar<char>{
public:
	explicit Char(char val=0):AbstractScalar(val){}
	~Char() override = default;
	bool isNull() const override {return val_==CHAR_MIN;}
	void setNull() override {val_=CHAR_MIN;}
	void setChar(char val) override { val_ = val;}
	DATA_TYPE getType() const override {return DT_CHAR;}
	DATA_TYPE getRawType() const override { return DT_CHAR;}
	ConstantSP getInstance() const override {return ConstantSP(new Char());}
	ConstantSP getValue() const override {return ConstantSP(new Char(val_));}
	DATA_CATEGORY getCategory() const override {return INTEGRAL;}
	std::string getString() const override { return toString(val_);}
	std::string getScript() const override {
		if(isNull())
			return "00c";
		if(val_>31 && val_<127){
			std::string str("' '");
			str[1] = val_;
			return str;
		}
		char buf[5];
	#ifdef _MSC_VER
		sprintf_s(buf, 5, "%d", val_);
	#else
		sprintf(buf, "%d", val_);
	#endif
		return std::string(buf);
	}
	int getHash(int buckets) const override { return val_ == CHAR_MIN ? -1 : ((unsigned int)val_) % buckets;}
	static Char* parseChar(const std::string& str) {
		Char* p=nullptr;
		if(str=="00" || str.empty()){
			p=new Char();
			p->setNull();
		}
		else if(str[0]=='\''){
			char ch=CHAR_MIN;
			if(str.size()==4 && str[3]=='\'' && str[1]=='\\'){
				ch=Util::escape(str[2]);
				if(ch==0)
					ch=str[2];
			} else if(str.size()==3 && str[2]=='\'')
				ch=str[1];
			p=new Char(ch);
		}
		else{
			int val = atoi(str.c_str());
			if(val>127 || val < -128)
				return nullptr;
			p=new Char(static_cast<char>(atoi(str.c_str())));
		}
		return p;
	}
	static std::string toString(char val) {
		if(val == CHAR_MIN)
			return "";
		if(val>31 && val<127)
			return std::string(1, val);
		return std::to_string(val);
	}
};

class Short: public AbstractScalar<short>{
public:
	explicit Short(short val=0):AbstractScalar(val){}
	~Short() override = default;
	bool isNull() const override {return val_==SHRT_MIN;}
	void setNull() override {val_=SHRT_MIN;}
	void setShort(short val) override { val_ = val;}
	DATA_TYPE getType() const override {return DT_SHORT;}
	DATA_TYPE getRawType() const override { return DT_SHORT;}
	ConstantSP getInstance() const override {return ConstantSP(new Short());}
	ConstantSP getValue() const override {return ConstantSP(new Short(val_));}
	DATA_CATEGORY getCategory() const override {return INTEGRAL;}
	std::string getString() const override { return toString(val_);}
	int getHash(int buckets) const override { return val_ == SHRT_MIN ? -1 : ((unsigned int)val_) % buckets;}
	static Short* parseShort(const std::string& str) {
		Short* p=nullptr;

		if(str=="00"){
			p=new Short();
			p->setNull();
		}
		else{
			int val = atoi(str.c_str());
			if(val>65535 || val < -65536)
				return nullptr;
			p=new Short(static_cast<short>(atoi(str.c_str())));
		}
		return p;
	}
	static std::string toString(short val) {
		if(val == SHRT_MIN)
			return "";
		return std::to_string(val);
	}
};

class Int: public AbstractScalar<int>{
public:
	explicit Int(int val=0):AbstractScalar(val){}
	~Int() override = default;
	bool isNull() const override {return val_==INT_MIN;}
	void setNull() override {val_=INT_MIN;}
	void setInt(int val) override { val_ = val;}
	DATA_TYPE getType() const override {return DT_INT;}
	DATA_TYPE getRawType() const override { return DT_INT;}
	ConstantSP getInstance() const override {return ConstantSP(new Int());}
	ConstantSP getValue() const override {return ConstantSP(new Int(val_));}
	DATA_CATEGORY getCategory() const override {return INTEGRAL;}
	std::string getString() const override { return toString(val_);}
	int getHash(int buckets) const override { return val_ == INT_MIN ? -1 : ((uint32_t)val_) % buckets;}
	static Int* parseInt(const std::string& str) {
		Int* p=nullptr;
		if(str=="00"){
			p=new Int();
			p->setNull();
		}
		else
			p=new Int(atoi(str.c_str()));
		return p;
	}
	static std::string toString(int val) {
		if(val == INT_MIN)
			return "";
		return std::to_string(val);
	}
};

class EnumInt : public Int {
public:
	EnumInt(std::string desc, int val):Int(val), desc_(std::move(desc)){}
	~EnumInt() override = default;
	std::string getScript() const override {return desc_;}
	ConstantSP getValue() const override {return ConstantSP(new EnumInt(desc_, val_));}
	ConstantSP getInstance() const override {return ConstantSP(new EnumInt(desc_, val_));}
	std::string getString() const override {return desc_;}

private:
	std::string desc_;
};

class Long: public AbstractScalar<long long>{
public:
	explicit Long(long long val=0):AbstractScalar(val){}
	~Long() override = default;
	bool isNull() const override {return val_==LLONG_MIN;}
	void setNull() override {val_=LLONG_MIN;}
	void setLong(long long val) override { val_ = val;}
	DATA_TYPE getType() const override {return DT_LONG;}
	DATA_TYPE getRawType() const override { return DT_LONG;}
	ConstantSP getInstance() const override {return ConstantSP(new Long());}
	ConstantSP getValue() const override {return ConstantSP(new Long(val_));}
	std::string getString() const override { return toString(val_);}
	DATA_CATEGORY getCategory() const override {return INTEGRAL;}
	int getHash(int buckets) const override { return val_ == LLONG_MIN ? -1 : ((uint64_t)val_) % buckets;}
	static Long* parseLong(const std::string& str) {
		Long* p=nullptr;

		if(str=="00"){
			p=new Long();
			p->setNull();
		}
		else
			p=new Long(atoll(str.c_str()));
		return p;
	}
	static std::string toString(long long val) {
		if(val == LLONG_MIN)
			return "";
		return std::to_string(val);
	}
};

class Float: public AbstractScalar<float>{
public:
	explicit Float(float val=0):AbstractScalar(val){}
	~Float() override = default;
	bool isNull() const override {return val_==FLT_NMIN;}
	void setNull() override {val_=FLT_NMIN;}
	void setFloat(float val) override { val_ = val;}
	DATA_TYPE getType() const override {return DT_FLOAT;}
	DATA_TYPE getRawType() const override { return DT_FLOAT;}
	ConstantSP getInstance() const override {return ConstantSP(new Float());}
	ConstantSP getValue() const override {return ConstantSP(new Float(val_));}
	DATA_CATEGORY getCategory() const override {return FLOATING;}
	char getChar() const override {return isNull() ? CHAR_MIN : static_cast<char>(val_ < 0 ? val_ - 0.5 : val_ + 0.5);}
	short getShort() const override {return isNull() ? SHRT_MIN : static_cast<short>(val_ < 0 ? val_ - 0.5 : val_ + 0.5);}
	int getInt() const override {return isNull() ? INT_MIN : static_cast<int>(val_ < 0 ? val_ - 0.5 : val_ + 0.5);}
	long long  getLong() const override {return isNull() ? LLONG_MIN : static_cast<long long>(val_ < 0 ? val_ - 0.5 : val_ + 0.5);}
    bool getChar(INDEX idx, int len, char *buf) const override { return setF1(idx, len, buf); }
    bool getShort(INDEX idx, int len, short *buf) const override { return setF1(idx, len, buf); }
    bool getInt(INDEX idx, int len, int *buf) const override { return setF1(idx, len, buf); }
    bool getLong(INDEX idx, int len, long long *buf) const override { return setF1(idx, len, buf); }
    const char *getCharConst(INDEX idx, int len, char *buf) const override { return setF2(idx, len, buf); }
    const short *getShortConst(INDEX idx, int len, short *buf) const override { return setF2(idx, len, buf); }
    const int *getIntConst(INDEX idx, int len, int *buf) const override { return setF2(idx, len, buf); }
    const long long *getLongConst(INDEX idx, int len, long long *buf) const override { return setF2(idx, len, buf); }
	std::string getString() const override { return toString(val_);}
	static Float* parseFloat(const std::string& str) {
		Float* p=nullptr;

		if(str=="00"){
			p=new Float();
			p->setNull();
		}
		else
			p=new Float(static_cast<float>(atof(str.c_str())));
		return p;
	}
	static std::string toString(float val) {
		if(val == FLT_NMIN)
			return "";
		if(std::isnan(val))
			return "NaN";
		if(std::isinf(val))
			return "inf";
		return DoubleToString(val);
	}
};

class Double : public AbstractScalar<double>
{
  public:
    explicit Double(double val = 0) : AbstractScalar(val) {}
    ~Double() override = default;
    bool isNull() const override { return val_ == DBL_NMIN; }
    void setNull() override { val_ = DBL_NMIN; }
    void setDouble(double val) override { val_ = val; }
    DATA_TYPE getType() const override { return DT_DOUBLE; }
    DATA_TYPE getRawType() const override { return DT_DOUBLE; }
    ConstantSP getInstance() const override { return ConstantSP(new Double()); }
    ConstantSP getValue() const override { return ConstantSP(new Double(val_)); }
    DATA_CATEGORY getCategory() const override { return FLOATING; }
    char getChar() const override
    {
        return isNull() ? CHAR_MIN : static_cast<char>(val_ < 0 ? val_ - 0.5 : val_ + 0.5);
    }
    short getShort() const override
    {
        return isNull() ? SHRT_MIN : static_cast<short>(val_ < 0 ? val_ - 0.5 : val_ + 0.5);
    }
    int getInt() const override { return isNull() ? INT_MIN : static_cast<int>(val_ < 0 ? val_ - 0.5 : val_ + 0.5); }
    long long getLong() const override
    {
        return isNull() ? LLONG_MIN : static_cast<long long>(val_ < 0 ? val_ - 0.5 : val_ + 0.5);
    }
    bool getChar(INDEX idx, int len, char *buf) const override { return setF1(idx, len, buf); }
    bool getShort(INDEX idx, int len, short *buf) const override { return setF1(idx, len, buf); }
    bool getInt(INDEX idx, int len, int *buf) const override { return setF1(idx, len, buf); }
    bool getLong(INDEX idx, int len, long long *buf) const override { return setF1(idx, len, buf); }
    const char *getCharConst(INDEX idx, int len, char *buf) const override { return setF2(idx, len, buf); }
    const short *getShortConst(INDEX idx, int len, short *buf) const override { return setF2(idx, len, buf); }
    const int *getIntConst(INDEX idx, int len, int *buf) const override { return setF2(idx, len, buf); }
    const long long *getLongConst(INDEX idx, int len, long long *buf) const override { return setF2(idx, len, buf); }
    std::string getString() const override { return toString(val_); }
    static Double *parseDouble(const std::string &str)
    {
        Double *p = nullptr;

        if (str == "00") {
            p = new Double();
            p->setNull();
        } else
            p = new Double(atof(str.c_str()));
        return p;
    }
    static std::string toString(double val)
    {
        if (val == DBL_NMIN)
            return "";
        if (std::isnan(val))
            return "NaN";
        if (std::isinf(val))
            return "inf";
        return DoubleToString(val);
    }
};

class EnumDouble : public Double {
public:
	EnumDouble(std::string desc, double val):Double(val), desc_(std::move(desc)){}
	~EnumDouble() override = default;
	std::string getScript() const override {return desc_;}
	ConstantSP getValue() const override {return ConstantSP(new EnumDouble(desc_, val_));}
	ConstantSP getInstance() const override {return ConstantSP(new EnumDouble(desc_, val_));}
	std::string getString() const override { return desc_;}

private:
	std::string desc_;
};

class TemporalScalar:public Int{
public:
	explicit TemporalScalar(int val=0):Int(val){}
	~TemporalScalar() override = default;
	DATA_CATEGORY getCategory() const override {return TEMPORAL;}
};

inline int countTemporalUnit(int days, int multiplier, int remainder){
	return days == INT_MIN ? INT_MIN : (days * multiplier) + remainder;
}

inline long long countTemporalUnit(int days, long long multiplier, long long remainder){
	return days == INT_MIN ? LLONG_MIN : (days * multiplier) + remainder;
}

class Date:public TemporalScalar{
public:
	explicit Date(int val=0):TemporalScalar(val){}
	~Date() override = default;
	Date(int year, int month, int day):TemporalScalar(Util::countDays(year,month,day)){}
	DATA_TYPE getType() const override {return DT_DATE;}
	ConstantSP getInstance() const override {return ConstantSP(new Date());}
	ConstantSP getValue() const override {return ConstantSP(new Date(val_));}
	std::string getString() const override { return toString(val_);}
	static Date* parseDate(const std::string& date) {
		//format: 2012.09.01
		Date* p=nullptr;

		if(date=="00"){
			p=new Date();
			p->setNull();
			return p;
		}

		int year,month,day;
		year=atoi(date.substr(0,4).c_str());
		if(year==0)
			return p;
		if(date[4]!='.')
			return p;
		month=atoi(date.substr(5,2).c_str());
		if(month==0)
			return p;
		if(date[7]!='.')
			return p;
		if(date[8]=='0')
			day=atoi(date.substr(9,1).c_str());
		else
			day=atoi(date.substr(8,2).c_str());
		if(day==0)
			return p;
		p=new Date(year,month,day);
		return p;
	}
	static std::string toString(int val) {
        if (val == INT_MIN)
            return "";
        int y;
        int m;
        int d;
        Util::parseDate(val, y, m, d);
        return fmt::format("{:04d}.{:02d}.{:02d}", y, m, d);
    }
	ConstantSP castTemporal(DATA_TYPE expectType) override {
		if (expectType != DT_DATEHOUR && (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP)) {
			throw RuntimeException("castTemporal from DATE to " + Util::getDataTypeString(expectType) + " not supported ");
		}
		if (expectType != DT_DATE && expectType != DT_TIMESTAMP && expectType != DT_NANOTIMESTAMP && expectType != DT_MONTH && expectType != DT_DATETIME && expectType != DT_DATEHOUR) {
			throw RuntimeException("castTemporal from DATE to " + Util::getDataTypeString(expectType) + " not supported ");
		}
		if (expectType == DT_DATE)
			return getValue();

		if (expectType == DT_DATEHOUR) {
			int result;
			val_ == INT_MIN ? result = INT_MIN : result = val_ * 24;
			return Util::createObject(expectType, result);
		}
		long long ratio = Util::getTemporalConversionRatio(DT_DATE, expectType);
		if (expectType == DT_NANOTIMESTAMP || expectType == DT_TIMESTAMP) {
			long long result;

			val_ == INT_MIN ? result = LLONG_MIN : result = (long long)val_ * ratio;
			return Util::createObject(expectType, result);
		}
		if (expectType == DT_DATETIME) {
			int result;

			val_ == INT_MIN ? result = INT_MIN : result = static_cast<int>(val_ * ratio);
			return Util::createObject(expectType, result);
		}
		int result;

		if (val_ == INT_MIN) {
			result = INT_MIN;
		} else {
			int year, month, day;
			Util::parseDate(val_, year, month, day);
			result = year * 12 + month - 1;
		}
		return Util::createObject(expectType, result);
	}
};

class Month : public TemporalScalar
{
  public:
    Month() : TemporalScalar((1999 * 12) + 11) {}
    explicit Month(int val) : TemporalScalar(val) {}
    Month(int year, int month) : TemporalScalar((year * 12) + month - 1) {}
    ~Month() override = default;
    DATA_TYPE getType() const override { return DT_MONTH; }
    ConstantSP getInstance() const override { return ConstantSP(new Month()); }
    ConstantSP getValue() const override { return ConstantSP(new Month(val_)); }
    std::string getString() const override { return toString(val_); }
    static Month *parseMonth(const std::string &str)
    {
        // 2012.01
        Month *p = nullptr;

        if (str == "00") {
            p = new Month();
            p->setNull();
            return p;
        }
        if (str.length() != 7)
            return p;

        int year, month;
        year = atoi(str.substr(0, 4).c_str());
        if (year == 0)
            return p;
        if (str[4] != '.')
            return p;
        month = atoi(str.substr(5, 2).c_str());
        if (month == 0 || month > 12)
            return p;

        p = new Month(year, month);
        return p;
    }
    static std::string toString(int val)
    {
        if (val == INT_MIN)
            return "";
        static TemporalFormat monthFormat("yyyy.MM\\M");
        return monthFormat.format(val, DT_MONTH);
    }
    ConstantSP castTemporal(DATA_TYPE expectType) override
    {
        if (expectType == DT_MONTH)
            return getValue();
        throw RuntimeException("castTemporal from MONTH to " + Util::getDataTypeString(expectType) + " not supported ");
    }
};

class Time : public TemporalScalar
{
  public:
    explicit Time(int val = 0) : TemporalScalar(val) {}
    Time(int hour, int minute, int second, int milliSecond)
        : TemporalScalar((((hour * 60 + minute) * 60 + second) * 1000) + milliSecond)
    {
    }
    ~Time() override = default;
    DATA_TYPE getType() const override { return DT_TIME; }
    ConstantSP getInstance() const override { return ConstantSP(new Time()); }
    ConstantSP getValue() const override { return ConstantSP(new Time(val_)); }
    std::string getString() const override { return toString(val_); }
    void validate() override
    {
        if (val_ >= 86400000 || val_ < 0)
            val_ = INT_MIN;
    }
    static Time *parseTime(const std::string &str)
    {
        // 23:04:59:001
        Time *p = nullptr;
        if (str == "00") {
            p = new Time();
            p->setNull();
            return p;
        }
        if (str.size() != 12)
            return p;
        int hour, minute, second, milliSecond = 0;
        hour = atoi(str.substr(0, 2).c_str());
        if (hour >= 24 || str[2] != ':')
            return p;
        minute = atoi(str.substr(3, 2).c_str());
        if (minute >= 60 || str[5] != ':')
            return p;
        second = atoi(str.substr(6, 2).c_str());
        if (second >= 60)
            return p;
        if (str[8] == '.')
            milliSecond = atoi(str.substr(9, str.length() - 9).c_str());
        p = new Time(hour, minute, second, milliSecond);
        return p;
    }
    static std::string toString(int val)
    {
        if (val < 0 || val >= 86400000)
            return "";
        static TemporalFormat timeFormat("HH:mm:ss.SSS");
        return timeFormat.format(val, DT_TIME);
    }
    ConstantSP castTemporal(DATA_TYPE expectType) override
    {
        if (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP) {
            throw RuntimeException("castTemporal from TIME to " + Util::getDataTypeString(expectType) +
                                   " not supported ");
        }
        if (expectType != DT_TIME && expectType != DT_NANOTIME && expectType != DT_SECOND && expectType != DT_MINUTE) {
            throw RuntimeException("castTemporal from TIME to " + Util::getDataTypeString(expectType) +
                                   " not supported ");
        }
        if (expectType == DT_TIME)
            return getValue();

        long long ratio = Util::getTemporalConversionRatio(DT_TIME, expectType);
        if (expectType == DT_NANOTIME) {
            long long result;

            val_ == INT_MIN ? result = LLONG_MIN : result = (long long)val_ * ratio;
            return Util::createObject(expectType, result);
        }
        int result;

        val_ == INT_MIN ? result = INT_MIN : result = static_cast<int>(val_ / (-ratio));
        return Util::createObject(expectType, result);
    }
};

class NanoTime : public Long
{
  public:
    explicit NanoTime(long long val = 0) : Long(val) {}
    NanoTime(int hour, int minute, int second, int nanoSecond)
        : Long((((hour * 60 + minute) * 60 + second) * 1000000000LL) + nanoSecond)
    {
    }
    ~NanoTime() override = default;
    DATA_TYPE getType() const override { return DT_NANOTIME; }
    DATA_CATEGORY getCategory() const override { return TEMPORAL; }
    ConstantSP castTemporal(DATA_TYPE expectType) override
    {
        if (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP) {
            throw RuntimeException("castTemporal from NANOTIME to " + Util::getDataTypeString(expectType) +
                                   " not supported ");
        }
        if (expectType != DT_TIME && expectType != DT_NANOTIME && expectType != DT_SECOND && expectType != DT_MINUTE) {
            throw RuntimeException("castTemporal from NANOTIME to " + Util::getDataTypeString(expectType) +
                                   " not supported ");
        }
        if (expectType == DT_NANOTIME)
            return getValue();

        long long ratio = Util::getTemporalConversionRatio(DT_NANOTIME, expectType);

        int result;
        val_ == LLONG_MIN ? result = INT_MIN : result = static_cast<int>(val_ / (-ratio));
        return Util::createObject(expectType, result);
    }
    ConstantSP getInstance() const override { return ConstantSP(new NanoTime()); }
    ConstantSP getValue() const override { return ConstantSP(new NanoTime(val_)); }
    std::string getString() const override { return toString(val_); }
    void validate() override
    {
        if (val_ >= 86400000000000LL || val_ < 0)
            val_ = LLONG_MIN;
    }
    static NanoTime *parseNanoTime(const std::string &str)
    {
        // 23:04:59:000000001
        NanoTime *p = nullptr;
        if (str == "00") {
            p = new NanoTime();
            p->setNull();
            return p;
        }

        int hour, minute, second, nanoSecond = 0;
        hour = atoi(str.substr(0, 2).c_str());
        if (hour >= 24 || str[2] != ':')
            return p;
        minute = atoi(str.substr(3, 2).c_str());
        if (minute >= 60 || str[5] != ':')
            return p;
        second = atoi(str.substr(6, 2).c_str());
        if (second >= 60)
            return p;
        if (str[8] == '.') {
            std::size_t len = str.length() - 9;
            if (len == 9)
                nanoSecond = atoi(str.substr(9, len).c_str());
            else if (len == 6)
                nanoSecond = atoi(str.substr(9, len).c_str()) * 1000;
            else if (len == 3)
                nanoSecond = atoi(str.substr(9, len).c_str()) * 1000000;
            else
                return p;
        }
        p = new NanoTime(hour, minute, second, nanoSecond);
        return p;
    }
    static std::string toString(long long val)
    {
        if (val < 0 || val >= 86400000000000LL)
            return "";
        static TemporalFormat nanotimeFormat("HH:mm:ss.nnnnnnnnn");
        return nanotimeFormat.format(val, DT_NANOTIME);
    }
};

class Timestamp : public Long
{
  public:
    explicit Timestamp(long long val = 0) : Long(val) {}
    Timestamp(int year, int month, int day, int hour, int minute, int second, int milliSecond)
        : Long(countTemporalUnit(Util::countDays(year, month, day), 86400000LL,
                                 (((hour * 60 + minute) * 60 + second) * 1000LL) + milliSecond))
    {
    }
    ~Timestamp() override = default;
    DATA_TYPE getType() const override { return DT_TIMESTAMP; }
    DATA_CATEGORY getCategory() const override { return TEMPORAL; }
    ConstantSP castTemporal(DATA_TYPE expectType) override
    {
        if (expectType != DT_DATEHOUR && (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP)) {
            throw RuntimeException("castTemporal from TIMESTAMP to " + Util::getDataTypeString(expectType) +
                                   " not supported ");
        }
        if (expectType == DT_TIMESTAMP)
            return getValue();

        if (expectType == DT_DATEHOUR) {
            int result;

            int tail = static_cast<int>((val_ < 0) && ((val_ % 3600000)) != 0);
            val_ == LLONG_MIN ? result = INT_MIN : result = static_cast<int>((val_ / 3600000LL) - tail);
            return Util::createObject(expectType, result);
        }
        long long ratio = Util::getTemporalConversionRatio(DT_TIMESTAMP, expectType);
        if (expectType == DT_NANOTIMESTAMP) {
            long long result;

            val_ == LLONG_MIN ? result = LLONG_MIN : result = val_ * ratio;
            return Util::createObject(expectType, result);
        }
        if (expectType == DT_DATE || expectType == DT_DATETIME) {
            int result;
            ratio = -ratio;

            int tail = static_cast<int>((val_ < 0) && ((val_ % ratio)) != 0);
            val_ == LLONG_MIN ? result = INT_MIN : result = static_cast<int>((val_ / ratio) - tail);
            return Util::createObject(expectType, result);
        }
        if (expectType == DT_MONTH) {
            int result;

            if (val_ == LLONG_MIN) {
                result = INT_MIN;
            } else {
                int year, month, day;
                Util::parseDate(static_cast<int>(val_ / 86400000), year, month, day);
                result = year * 12 + month - 1;
            }
            return Util::createObject(expectType, result);
        }
        if (expectType == DT_NANOTIME) {
            long long result;

            int remainder = val_ % 86400000;
            val_ == LLONG_MIN
                ? result = LLONG_MIN
                : result = (remainder + static_cast<int>((val_ < 0) && (remainder != 0)) * 86400000) * 1000000LL;
            return Util::createObject(expectType, result);
        }
        ratio = Util::getTemporalConversionRatio(DT_TIME, expectType);
        int result;
        if (ratio < 0)
            ratio = -ratio;

        int remainder = val_ % 86400000;
        val_ == LLONG_MIN
            ? result = INT_MIN
            : result =
                  static_cast<int>((remainder + static_cast<int>((val_ < 0) && (remainder != 0)) * 86400000) / ratio);
        return Util::createObject(expectType, result);
    }
    ConstantSP getInstance() const override { return ConstantSP(new Timestamp()); }
    ConstantSP getValue() const override { return ConstantSP(new Timestamp(val_)); }
    std::string getString() const override { return toString(val_); }
    static Timestamp *parseTimestamp(const std::string &str)
    {
        Timestamp *p = nullptr;

        if (str == "00") {
            p = new Timestamp();
            p->setNull();
            return p;
        }

        std::size_t len = str.size();
        if (len < 19)
            return p;

        int year, month, day, hour, minute, second, millisecond = 0;
        year = atoi(str.substr(0, 4).c_str());
        if (year == 0 || str[4] != '.')
            return p;
        month = atoi(str.substr(5, 2).c_str());
        if (month == 0 || str[7] != '.')
            return p;
        day = atoi(str.substr(8, 2).c_str());
        if (day == 0 || (str[10] != ' ' && str[10] != 'T'))
            return p;

        hour = atoi(str.substr(11, 2).c_str());
        if (hour >= 24 || str[13] != ':')
            return p;
        minute = atoi(str.substr(14, 2).c_str());
        if (minute >= 60 || str[16] != ':')
            return p;
        second = atoi(str.substr(17, 2).c_str());
        if (second >= 60)
            return p;
        if (len > 19 && str[19] == '.') {
            if (len > 22)
                millisecond = atoi(str.substr(20, 3).c_str());
            else
                return p;
        }
        p = new Timestamp(year, month, day, hour, minute, second, millisecond);
        return p;
    }
    static std::string toString(long long val)
    {
        if (val == LLONG_MIN)
            return "";
        static TemporalFormat timestampFormat("yyyy.MM.ddTHH:mm:ss.SSS");
        return timestampFormat.format(val, DT_TIMESTAMP);
    }
};

class NanoTimestamp : public Long
{
  public:
    explicit NanoTimestamp(long long val = 0) : Long(val) {}
    NanoTimestamp(int year, int month, int day, int hour, int minute, int second, int nanoSecond)
        : Long(countTemporalUnit(Util::countDays(year, month, day), 86400000000000LL,
                                 (((hour * 60 + minute) * 60 + second) * 1000000000LL) + nanoSecond))
    {
    }
    ~NanoTimestamp() override = default;
    DATA_TYPE getType() const override { return DT_NANOTIMESTAMP; }
    DATA_CATEGORY getCategory() const override { return TEMPORAL; }
    ConstantSP castTemporal(DATA_TYPE expectType) override
    {
        if (expectType != DT_DATEHOUR && (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP)) {
            throw RuntimeException("castTemporal from NANOTIMESTAMP to " + Util::getDataTypeString(expectType) +
                                   " not supported ");
        }
        if (expectType == DT_NANOTIMESTAMP)
            return getValue();

        if (expectType == DT_DATEHOUR) {
            int result;

            int tail = static_cast<int>((val_ < 0) && ((val_ % 3600000000000LL)) != 0);
            val_ == LLONG_MIN ? result = INT_MIN : result = static_cast<int>((val_ / 3600000000000LL) - tail);
            return Util::createObject(expectType, result);
        }
        long long ratio = -Util::getTemporalConversionRatio(DT_NANOTIMESTAMP, expectType);
        if (expectType == DT_TIMESTAMP) {
            long long result;

            int tail = static_cast<int>((val_ < 0) && ((val_ % ratio)) != 0);
            val_ == LLONG_MIN ? result = LLONG_MIN : result = val_ / ratio - tail;
            return Util::createObject(expectType, result);
        }
        if (expectType == DT_DATE || expectType == DT_DATETIME) {
            int result;

            int tail = static_cast<int>((val_ < 0) && ((val_ % ratio)) != 0);
            val_ == LLONG_MIN ? result = INT_MIN : result = static_cast<int>((val_ / ratio) - tail);
            return Util::createObject(expectType, result);
        }
        if (expectType == DT_MONTH) {
            int result;

            if (val_ == LLONG_MIN) {
                result = INT_MIN;
            } else {
                int year, month, day;
                Util::parseDate(static_cast<int>(val_ / 86400000000000LL), year, month, day);
                result = year * 12 + month - 1;
            }
            return Util::createObject(expectType, result);
        }
        if (expectType == DT_NANOTIME) {
            long long result;

            long long remainder = val_ % 86400000000000LL;
            val_ == LLONG_MIN
                ? result = LLONG_MIN
                : result = (remainder + static_cast<long long>(val_ < 0 && (remainder != 0)) * 86400000000000LL);
            return Util::createObject(expectType, result);
        }
        ratio = Util::getTemporalConversionRatio(DT_NANOTIME, expectType);
        int result;
        ratio = -ratio;

        long long remainder = val_ % 86400000000000LL;
        val_ == LLONG_MIN
            ? result = INT_MIN
            : result = static_cast<int>(
                  (remainder + static_cast<long long>(val_ < 0 && (remainder != 0)) * 86400000000000LL) / ratio);
        return Util::createObject(expectType, result);
    }
    ConstantSP getInstance() const override { return ConstantSP(new NanoTimestamp()); }
    ConstantSP getValue() const override { return ConstantSP(new NanoTimestamp(val_)); }
    std::string getString() const override { return toString(val_); }
    static NanoTimestamp *parseNanoTimestamp(const std::string &str)
    {
        NanoTimestamp *p = nullptr;

        if (str == "00") {
            p = new NanoTimestamp();
            p->setNull();
            return p;
        }

        int year, month, day, hour, minute, second, nanosecond = 0;
        year = atoi(str.substr(0, 4).c_str());
        if (year == 0 || str[4] != '.')
            return p;
        month = atoi(str.substr(5, 2).c_str());
        if (month == 0 || str[7] != '.')
            return p;
        day = atoi(str.substr(8, 2).c_str());
        if (day == 0 || (str[10] != ' ' && str[10] != 'T'))
            return p;

        hour = atoi(str.substr(11, 2).c_str());
        if (hour >= 24 || str[13] != ':')
            return p;
        minute = atoi(str.substr(14, 2).c_str());
        if (minute >= 60 || str[16] != ':')
            return p;
        second = atoi(str.substr(17, 2).c_str());
        if (second >= 60)
            return p;
        if (str[19] == '.') {
            std::size_t len = str.length() - 20;
            if (len == 9)
                nanosecond = atoi(str.substr(20, len).c_str());
            else if (len == 6)
                nanosecond = atoi(str.substr(20, len).c_str()) * 1000;
            else if (len == 3)
                nanosecond = atoi(str.substr(20, len).c_str()) * 1000000;
            else
                return p;
        }
        p = new NanoTimestamp(year, month, day, hour, minute, second, nanosecond);
        return p;
    }
    static std::string toString(long long val)
    {
        if (val == LLONG_MIN)
            return "";
        static TemporalFormat nanotimestampFormat("yyyy.MM.ddTHH:mm:ss.nnnnnnnnn");
        return nanotimestampFormat.format(val, DT_NANOTIMESTAMP);
    }
};

class Minute : public TemporalScalar
{
  public:
    explicit Minute(int val = 0) : TemporalScalar(val) {}
    Minute(int hour, int minute) : TemporalScalar((hour * 60) + minute) {}
    ~Minute() override = default;
    DATA_TYPE getType() const override { return DT_MINUTE; }
    ConstantSP getInstance() const override { return ConstantSP(new Minute()); }
    ConstantSP getValue() const override { return ConstantSP(new Minute(val_)); }
    std::string getString() const override { return toString(val_); }
    void validate() override
    {
        if (val_ >= 1440 || val_ < 0)
            val_ = INT_MIN;
    }
    static Minute *parseMinute(const std::string &str)
    {
        Minute *p = nullptr;
        if (str == "00") {
            p = new Minute();
            p->setNull();
            return p;
        }

        int hour, minute;
        hour = atoi(str.substr(0, 2).c_str());
        if (hour >= 24 || str[2] != ':')
            return p;
        minute = atoi(str.substr(3, 2).c_str());
        if (minute >= 60)
            return p;
        p = new Minute(hour, minute);
        return p;
    }
    static std::string toString(int val)
    {
        if (val < 0 || val >= 1440)
            return "";
        static TemporalFormat minuteFormat("HH:mm\\m");
        return minuteFormat.format(val, DT_MINUTE);
    }
    ConstantSP castTemporal(DATA_TYPE expectType) override
    {
        if (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP) {
            throw RuntimeException("castTemporal from MINUTE to " + Util::getDataTypeString(expectType) +
                                   " not supported ");
        }
        if (expectType != DT_TIME && expectType != DT_NANOTIME && expectType != DT_SECOND && expectType != DT_MINUTE) {
            throw RuntimeException("castTemporal from MINUTE to " + Util::getDataTypeString(expectType) +
                                   " not supported ");
        }
        if (expectType == DT_MINUTE)
            return getValue();

        long long ratio = Util::getTemporalConversionRatio(DT_MINUTE, expectType);
        if (expectType == DT_NANOTIME) {
            long long result;

            val_ == INT_MIN ? result = LLONG_MIN : result = (long long)val_ * ratio;
            return Util::createObject(expectType, result);
        }
        int result;

        val_ == INT_MIN ? result = INT_MIN : result = static_cast<int>(val_ * ratio);
        return Util::createObject(expectType, result);
    }
};

class Second : public TemporalScalar
{
  public:
    explicit Second(int val = 0) : TemporalScalar(val) {}
    Second(int hour, int minute, int second) : TemporalScalar(((hour * 60 + minute) * 60) + second) {}
    ~Second() override = default;
    DATA_TYPE getType() const override { return DT_SECOND; }
    ConstantSP getInstance() const override { return ConstantSP(new Second()); }
    ConstantSP getValue() const override { return ConstantSP(new Second(val_)); }
    std::string getString() const override { return toString(val_); }
    void validate() override
    {
        if (val_ >= 86400 || val_ < 0)
            val_ = INT_MIN;
    }
    static Second *parseSecond(const std::string &str)
    {
        Second *p = nullptr;
        if (str == "00") {
            p = new Second();
            p->setNull();
            return p;
        }

        int hour, minute, second;
        hour = atoi(str.substr(0, 2).c_str());
        if (hour >= 24 || str[2] != ':')
            return p;
        minute = atoi(str.substr(3, 2).c_str());
        if (minute >= 60 || str[5] != ':')
            return p;
        second = atoi(str.substr(6, 2).c_str());
        if (second >= 60)
            return p;
        p = new Second(hour, minute, second);
        return p;
    }
    static std::string toString(int val)
    {
        if (val < 0 || val >= 86400)
            return "";
        static TemporalFormat secondFormat("HH:mm:ss");
        return secondFormat.format(val, DT_SECOND);
    }
    ConstantSP castTemporal(DATA_TYPE expectType) override
    {
        if (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP) {
            throw RuntimeException("castTemporal from SECOND to " + Util::getDataTypeString(expectType) +
                                   " not supported ");
        }
        if (expectType != DT_TIME && expectType != DT_NANOTIME && expectType != DT_SECOND && expectType != DT_MINUTE) {
            throw RuntimeException("castTemporal from SECOND to " + Util::getDataTypeString(expectType) +
                                   " not supported ");
        }
        if (expectType == DT_SECOND)
            return getValue();

        long long ratio = Util::getTemporalConversionRatio(DT_SECOND, expectType);
        if (expectType == DT_NANOTIME) {
            long long result;

            val_ == INT_MIN ? result = LLONG_MIN : result = val_ * ratio;
            return Util::createObject(expectType, result);
        }
        if (expectType == DT_TIME) {
            int result;

            val_ == INT_MIN ? result = INT_MIN : result = static_cast<int>(val_ * ratio);
            return Util::createObject(expectType, result);
        }
        int result;

        val_ == INT_MIN ? result = INT_MIN : result = static_cast<int>(val_ / (-ratio));
        return Util::createObject(expectType, result);
    }
};

class DateTime : public TemporalScalar
{
  public:
    explicit DateTime(int val = 0) : TemporalScalar(val) {}
    DateTime(int year, int month, int day, int hour, int minute, int second)
        : TemporalScalar(
              countTemporalUnit(Util::countDays(year, month, day), 86400, ((hour * 60 + minute) * 60) + second))
    {
    }
    ~DateTime() override = default;
    DATA_TYPE getType() const override { return DT_DATETIME; }
    ConstantSP getInstance() const override { return ConstantSP(new DateTime()); }
    ConstantSP getValue() const override { return ConstantSP(new DateTime(val_)); }
    std::string getString() const override { return toString(val_); }
    static DateTime *parseDateTime(const std::string &str)
    {
        DateTime *p = nullptr;

        if (str == "00") {
            p = new DateTime();
            p->setNull();
            return p;
        }

        int year, month, day, hour, minute, second;
        year = atoi(str.substr(0, 4).c_str());
        if (year == 0 || str[4] != '.')
            return p;
        month = atoi(str.substr(5, 2).c_str());
        if (month == 0 || str[7] != '.')
            return p;
        day = atoi(str.substr(8, 2).c_str());
        if (day == 0 || (str[10] != ' ' && str[10] != 'T'))
            return p;

        hour = atoi(str.substr(11, 2).c_str());
        if (hour >= 24 || str[13] != ':')
            return p;
        minute = atoi(str.substr(14, 2).c_str());
        if (minute >= 60 || str[16] != ':')
            return p;
        second = atoi(str.substr(17, 2).c_str());
        if (second >= 60)
            return p;
        p = new DateTime(year, month, day, hour, minute, second);
        return p;
    }
    static std::string toString(int val)
    {
        if (val == INT_MIN)
            return "";
        static TemporalFormat datetimeFormat("yyyy.MM.ddTHH:mm:ss");
        return datetimeFormat.format(val, DT_DATETIME);
    }
    ConstantSP castTemporal(DATA_TYPE expectType) override
    {
        if (expectType != DT_DATEHOUR && (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP)) {
            throw RuntimeException("castTemporal from DATETIME to " + Util::getDataTypeString(expectType) +
                                   " not supported ");
        }
        if (expectType == DT_DATETIME)
            return getValue();

        if (expectType == DT_DATEHOUR) {
            int result;

            int tail = static_cast<int>((val_ < 0) && ((val_ % 3600)) != 0);
            val_ == INT_MIN ? result = INT_MIN : result = val_ / 3600 - tail;
            return Util::createObject(expectType, result);
        }
        long long ratio = Util::getTemporalConversionRatio(DT_DATETIME, expectType);
        if (expectType == DT_NANOTIMESTAMP || expectType == DT_TIMESTAMP) {
            long long result;

            val_ == INT_MIN ? result = LLONG_MIN : result = (long long)val_ * ratio;
            return Util::createObject(expectType, result);
        }
        if (expectType == DT_DATE) {
            int result;
            ratio = -ratio;

            int tail = static_cast<int>((val_ < 0) && ((val_ % ratio)) != 0);
            val_ == INT_MIN ? result = INT_MIN : result = static_cast<int>((val_ / ratio) - tail);
            return Util::createObject(expectType, result);
        }
        if (expectType == DT_MONTH) {
            int result;

            if (val_ == INT_MIN) {
                result = INT_MIN;
            } else {
                int year, month, day;
                Util::parseDate(val_ / 86400, year, month, day);
                result = year * 12 + month - 1;
            }
            return Util::createObject(expectType, result);
        }
        if (expectType == DT_NANOTIME) {
            long long result;

            int remainder = val_ % 86400;
            val_ == INT_MIN
                ? result = LLONG_MIN
                : result = (long long)(remainder + (static_cast<int>((val_ < 0) && (remainder != 0)) * 86400)) *
                           1000000000LL;
            return Util::createObject(expectType, result);
        }
        ratio = Util::getTemporalConversionRatio(DT_SECOND, expectType);
        int result;
        if (ratio > 0) {
            int remainder = val_ % 86400;
            val_ == INT_MIN
                ? result = INT_MIN
                : result =
                      static_cast<int>((remainder + static_cast<int>((val_ < 0) && (remainder != 0)) * 86400) * ratio);
        } else {
            ratio = -ratio;
            int remainder = val_ % 86400;
            val_ == INT_MIN
                ? result = INT_MIN
                : result =
                      static_cast<int>((remainder + static_cast<int>((val_ < 0) && (remainder != 0)) * 86400) / ratio);
        }
        return Util::createObject(expectType, result);
    }
};

class DateHour : public TemporalScalar
{
  public:
    explicit DateHour(int val = 0) : TemporalScalar(val) {}
    DateHour(int year, int month, int day, int hour)
        : TemporalScalar(countTemporalUnit(Util::countDays(year, month, day), 24, hour))
    {
    }
    ~DateHour() override = default;
    DATA_TYPE getType() const override { return DT_DATEHOUR; }
    ConstantSP getInstance() const override { return ConstantSP(new DateHour()); }
    ConstantSP getValue() const override { return ConstantSP(new DateHour(val_)); }
    std::string getString() const override { return toString(val_); }
    static DateHour *parseDateHour(const std::string &str)
    {
        DateHour *p = nullptr;

        if (str == "00") {
            p = new DateHour();
            p->setNull();
            return p;
        }

        if (UNLIKELY(str.size() < 13))
            return p;
        int year, month, day, hour;
        year = atoi(str.substr(0, 4).c_str());
        if (year == 0 || str[4] != '.')
            return p;
        month = atoi(str.substr(5, 2).c_str());
        if (month == 0 || str[7] != '.')
            return p;
        day = atoi(str.substr(8, 2).c_str());
        if (day == 0 || (str[10] != ' ' && str[10] != 'T'))
            return p;

        hour = atoi(str.substr(11, 2).c_str());
        if (hour >= 24)
            return p;
        p = new DateHour(year, month, day, hour);
        return p;
    }
    static std::string toString(int val)
    {
        if (val == INT_MIN)
            return "";
        static TemporalFormat datehourFormat("yyyy.MM.ddTHH");
        return datehourFormat.format(val, DT_DATEHOUR);
    }
    ConstantSP castTemporal(DATA_TYPE expectType) override
    {
        if (expectType != DT_DATEHOUR && (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP)) {
            throw RuntimeException("castTemporal from DATEHOUR to " + Util::getDataTypeString(expectType) +
                                   " not supported ");
        }
        if (expectType == DT_DATEHOUR)
            return getValue();

        long long ratio = Util::getTemporalConversionRatio(DT_DATETIME, expectType);
        if (expectType == DT_NANOTIMESTAMP || expectType == DT_TIMESTAMP) {
            long long result;
            val_ == INT_MIN ? result = LLONG_MIN : result = (long long)val_ * ratio * 3600;
            return Util::createObject(expectType, result);
        }
        if (expectType == DT_DATETIME) {
            int result;
            val_ == INT_MIN ? result = INT_MIN : result = val_ * 3600;
            return Util::createObject(expectType, result);
        }
        if (expectType == DT_DATE) {
            int result;
            val_ == INT_MIN ? result = INT_MIN : result = static_cast<int>(val_ * 3600 / (-ratio));
            return Util::createObject(expectType, result);
        }
        if (expectType == DT_MONTH) {
            int result;
            if (val_ == INT_MIN) {
                result = INT_MIN;
            } else {
                int year, month, day;
                Util::parseDate(val_ * 3600 / 86400, year, month, day);
                result = year * 12 + month - 1;
            }
            return Util::createObject(expectType, result);
        }
        if (expectType == DT_NANOTIME) {
            long long result;
            val_ == INT_MIN ? result = LLONG_MIN : result = (long long)val_ * 3600 % 86400 * 1000000000LL;
            return Util::createObject(expectType, result);
        }
        ratio = Util::getTemporalConversionRatio(DT_SECOND, expectType);
        int result;
        if (ratio > 0) {
            val_ == INT_MIN ? result = INT_MIN : result = static_cast<int>(val_ * 3600 % 86400 * ratio);
        } else {
            val_ == INT_MIN ? result = INT_MIN : result = static_cast<int>(val_ * 3600 % 86400 / (-ratio));
        }
        return Util::createObject(expectType, result);
    }
};

inline ConstantSP operator ""_d(const char *s, size_t len)
{
    return Date::parseDate(s);
}

inline ConstantSP operator ""_M(const char *s, size_t len)
{
    return Month::parseMonth(s);
}

inline ConstantSP operator ""_t(const char *s, size_t len)
{
    return Time::parseTime(s);
}

inline ConstantSP operator ""_m(const char *s, size_t len)
{
    return Minute::parseMinute(s);
}

inline ConstantSP operator ""_s(const char *s, size_t len)
{
    return Second::parseSecond(s);
}

inline ConstantSP operator ""_D(const char *s, size_t len)
{
    return DateTime::parseDateTime(s);
}

inline ConstantSP operator ""_T(const char *s, size_t len)
{
    return Timestamp::parseTimestamp(s);
}

inline ConstantSP operator ""_n(const char *s, size_t len)
{
    return NanoTime::parseNanoTime(s);
}

inline ConstantSP operator ""_N(const char *s, size_t len)
{
    return NanoTimestamp::parseNanoTimestamp(s);
}

} // namespace dolphindb

#if defined(_MSC_VER)
#pragma warning( pop )
#elif defined(__clang__)
#pragma clang diagnostic pop
#else // gcc
#pragma GCC diagnostic pop
#endif
