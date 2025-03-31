// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include <climits>

#include "Constant.h"
#include "Util.h"
#include "Guid.h"

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

class Void: public Constant{
public:
	Void(bool explicitNull = false){setNothing(!explicitNull);}
	ConstantSP getInstance() const override {return ConstantSP(new Void(!isNothing()));}
	ConstantSP getValue() const override {return ConstantSP(new Void(!isNothing()));}
	DATA_TYPE getType() const override {return DT_VOID;}
	DATA_TYPE getRawType() const override { return DT_VOID;}
	DATA_CATEGORY getCategory() const override {return NOTHING;}
	std::string getString() const override {return Constant::EMPTY;}
	std::string getScript() const override {return isNothing() ? Constant::EMPTY : Constant::NULL_STR;}
	const std::string& getStringRef() const override {return Constant::EMPTY;}
	char getBool() const override {return CHAR_MIN;}
	char getChar() const override {return CHAR_MIN;}
	short getShort() const override {return SHRT_MIN;}
	int getInt() const override {return INT_MIN;}
	long long  getLong() const override {return LLONG_MIN;}
	float getFloat() const override {return FLT_NMIN;}
	double getDouble() const override {return DBL_NMIN;}
	bool isNull() const override {return true;}
	void nullFill(const ConstantSP& val) override {}
	bool isNull(INDEX start, int len, char* buf) const override;
	bool isValid(INDEX start, int len, char* buf) const override;
	bool getBool(INDEX start, int len, char* buf) const override;
	const char* getBoolConst(INDEX start, int len, char* buf) const override;
	bool getChar(INDEX start, int len, char* buf) const override;
	const char* getCharConst(INDEX start, int len, char* buf) const override;
	bool getShort(INDEX start, int len, short* buf) const override;
	const short* getShortConst(INDEX start, int len, short* buf) const override;
	bool getInt(INDEX start, int len, int* buf) const override;
	const int* getIntConst(INDEX start, int len, int* buf) const override;
	bool getLong(INDEX start, int len, long long* buf) const override;
	const long long* getLongConst(INDEX start, int len, long long* buf) const override;
	bool getIndex(INDEX start, int len, INDEX* buf) const override;
	const INDEX* getIndexConst(INDEX start, int len, INDEX* buf) const override;
	bool getFloat(INDEX start, int len, float* buf) const override;
	const float* getFloatConst(INDEX start, int len, float* buf) const override;
	bool getDouble(INDEX start, int len, double* buf) const override;
	const double* getDoubleConst(INDEX start, int len, double* buf) const override;
	bool getString(INDEX start, int len, std::string** buf) const override;
	std::string** getStringConst(INDEX start, int len, std::string** buf) const override;
	long long getAllocatedMemory() const override;
	int serialize(char* buf, int bufSize, INDEX indexStart, int offset, int& numElement, int& partial) const override;
	IO_ERR deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement) override ;
	int compare(INDEX index, const ConstantSP& target) const override {return target->getType() == DT_VOID ? 0 : -1;}
	bool getBinary(INDEX start, int len, int unitLength, unsigned char* buf) const override;
};

class Int128: public Constant{
public:
	Int128();
	Int128(const unsigned char* data);
	virtual ~Int128(){}
	inline const unsigned char* bytes() const { return uuid_;}
	std::string getString() const override { return toString(uuid_);}
	const Guid getInt128() const override { return uuid_;}
	const unsigned char* getBinary() const override {return uuid_;}
	bool isNull() const override;
	void setNull() override;
	void nullFill(const ConstantSP& val) override {
		if(isNull())
			memcpy(uuid_, val->getInt128().bytes(), 16);
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
	int compare(INDEX index, const ConstantSP& target) const override;
	void setBinary(const unsigned char* val, int unitLength) override;
	bool getBinary(INDEX start, int len, int unitLenght, unsigned char* buf) const override;
	const unsigned char* getBinaryConst(INDEX start, int len, int unitLength, unsigned char* buf) const override;
	ConstantSP getInstance() const override {return new Int128();}
	ConstantSP getValue() const override {return new Int128(uuid_);}
	DATA_TYPE getType() const override {return DT_INT128;}
	DATA_TYPE getRawType() const override { return DT_INT128;}
	DATA_CATEGORY getCategory() const override {return BINARY;}
	long long getAllocatedMemory() const override{return sizeof(Int128);}
	int serialize(char* buf, int bufSize, INDEX indexStart, int offset, int& numElement, int& partial) const override;
	IO_ERR deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement) override ;
	int getHash(int buckets) const override { return murmur32_16b(uuid_) % buckets;}
	static std::string toString(const unsigned char* data);
	static Int128* parseInt128(const char* str, size_t len);
	static bool parseInt128(const char* str, size_t len, unsigned char *buf);

protected:
	union{
		mutable unsigned char uuid_[16];
		Guid guid_;
	};
};

class Uuid : public Int128 {
public:
	Uuid(bool newUuid = false);
	Uuid(const unsigned char* uuid);
	Uuid(const char* uuid, size_t len);
	Uuid(const Uuid& copy);
	virtual ~Uuid(){}
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
	IPAddr();
	IPAddr(const char* ip, int len);
	IPAddr(const unsigned char* data);
	virtual ~IPAddr(){}
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

class String: public Constant{
public:
	String(std::string val="", bool blob=false):val_(val), blob_(blob){
		if(!blob_){
			if(val_.find('\0') != std::string::npos){
				throw RuntimeException("A String cannot contain the character '\\0'");
			}
		}
	}
	virtual ~String(){}
	char getBool() const override {throw IncompatibleTypeException(DT_BOOL,DT_STRING);}
	char getChar() const override {throw IncompatibleTypeException(DT_CHAR,DT_STRING);}
	short getShort() const override {throw IncompatibleTypeException(DT_SHORT,DT_STRING);}
	int getInt() const override {throw IncompatibleTypeException(DT_INT,DT_STRING);}
	long long getLong() const override {throw IncompatibleTypeException(DT_LONG,DT_STRING);}
	INDEX getIndex() const override {throw IncompatibleTypeException(DT_INDEX,DT_STRING);}
	float getFloat() const override {throw IncompatibleTypeException(DT_FLOAT,DT_STRING);}
	double getDouble() const override {throw IncompatibleTypeException(DT_DOUBLE,DT_STRING);}
	std::string getString() const override {return val_;}
	std::string getScript() const override {return Util::literalConstant(val_);}
	const std::string& getStringRef() const override {return val_;}
	const std::string& getStringRef(INDEX index) const override {return val_;}
	bool isNull() const override {return val_.empty();}
	void setString(const std::string& val) override {
		if(!blob_){
			if(val.find('\0') != std::string::npos){
				throw RuntimeException("A String cannot contain the character '\\0'");
			}
		}
		val_=val;
	}
	void setNull() override {val_="";}
	void nullFill(const ConstantSP& val) override {
		if(isNull())
			val_=val->getStringRef();
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
	bool getString(INDEX start, int len, std::string** buf) const override {
		for(int i=0;i<len;++i)
			buf[i]=&val_;
		return true;
	}
	std::string** getStringConst(INDEX start, int len, std::string** buf) const override {
		for(int i=0;i<len;++i)
			buf[i]=&val_;
		return buf;
	}
	char** getStringConst(INDEX start, int len, char** buf) const override {
		char* val = (char*)val_.c_str();
		for(int i=0;i<len;++i)
			buf[i]=val;
		return buf;
	}
	ConstantSP getInstance() const override {return ConstantSP(new String("", blob_));}
	ConstantSP getValue() const override {return ConstantSP(new String(val_, blob_));}
	DATA_TYPE getType() const override {return blob_ == false ? DT_STRING : DT_BLOB;}
	DATA_TYPE getRawType() const override {return blob_ == false ? DT_STRING : DT_BLOB;}
	DATA_CATEGORY getCategory() const override {return LITERAL;}
	long long getAllocatedMemory() const override{return sizeof(std::string)+val_.size();}
	int serialize(char* buf, int bufSize, INDEX indexStart, int offset, int& numElement, int& partial) const override;
	IO_ERR deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement) override ;
	int compare(INDEX index, const ConstantSP& target) const override {
		return val_.compare(target->getString());
	}
	int getHash(int buckets) const override { return murmur32(val_.data(), val_.size()) % buckets;}

private:
	mutable std::string val_;
    bool blob_;
};

template <class T>
class AbstractScalar: public Constant{
public:
	AbstractScalar(T val=0):val_(val){}
	virtual ~AbstractScalar(){}
	char getBool() const override {return isNull()?CHAR_MIN:(bool)val_;}
	char getChar() const override {return isNull() ? CHAR_MIN : static_cast<char>(val_);}
	short getShort() const override {return isNull() ? SHRT_MIN : static_cast<short>(val_);}
	int getInt() const override {return isNull() ? INT_MIN : static_cast<int>(val_);}
	long long getLong() const override {return isNull() ? LLONG_MIN : static_cast<long long>(val_);}
	INDEX getIndex() const override {return isNull() ? INDEX_MIN : static_cast<INDEX>(val_);}
	float getFloat() const override {return isNull() ? FLT_NMIN : static_cast<float>(val_);}
	double getDouble() const override {return isNull() ? DBL_NMIN : static_cast<double>(val_);}

	virtual void setBool(char val) override {if(val != CHAR_MIN) val_=(T)val; else setNull();}
	virtual void setChar(char val) override {if(val != CHAR_MIN) val_=(T)val; else setNull();}
	virtual void setShort(short val) override {if(val != SHRT_MIN) val_=(T)val; else setNull();}
	virtual void setInt(int val) override {if(val != INT_MIN) val_=(T)val; else setNull();}
	virtual void setLong(long long val) override {if(val != LLONG_MIN) val_=(T)val; else setNull();}
	virtual void setIndex(INDEX val) override {if(val != INDEX_MIN) val_=(T)val; else setNull();}
	virtual void setFloat(float val) override {if(val != FLT_NMIN) val_=(T)val; else setNull();}
	virtual void setDouble(double val) override {if(val != DBL_NMIN) val_=(T)val; else setNull();}
	virtual void setString(const std::string& val) override {}
	bool isNull() const override = 0;
	std::string getScript() const override {
		if(isNull()){
			std::string str("00");
			return str.append(1, Util::getDataTypeSymbol(getType()));
		}
		else
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
		else if(bufSize >= len){
			numElement = 1;
			partial = 0;
			memcpy(buf,((char*)&val_)+offset, len);
			return len;
		}
		else{
			len = bufSize;
			numElement = 0;
			partial = offset+bufSize;
			memcpy(buf,((char*)&val_)+offset, len);
			return len;
		}
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
		else{
			T val= (T)target->getLong();
			return val_==val?0:(val_<val?-1:1);
		}
	}

private:
	template <typename R>
	bool getDecimal(INDEX /*start*/, int len, int scale, R *buf) const;
protected:
	T val_;
};

class Bool: public AbstractScalar<char>{
public:
	Bool(char val=0):AbstractScalar(val){}
	virtual ~Bool(){}
	bool isNull() const override {return val_==CHAR_MIN;}
	void setNull() override {val_= CHAR_MIN;}
	void setBool(char val) override { val_ = val;}
	DATA_TYPE getType() const override {return DT_BOOL;}
	DATA_TYPE getRawType() const override { return DT_BOOL;}
	ConstantSP getInstance() const override {return ConstantSP(new Bool());}
	ConstantSP getValue() const override {return ConstantSP(new Bool(val_));}
	DATA_CATEGORY getCategory() const override {return LOGICAL;}
	std::string getString() const override { return toString(val_);}
	bool add(INDEX start, INDEX length, long long inc) override { return false;}
	bool add(INDEX start, INDEX length, double inc) override { return false;}
	IO_ERR deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement) override ;
	static Bool* parseBool(const std::string& str);
	static std::string toString(char val){
		if(val == CHAR_MIN)
			return "";
		else if(val)
			return "1";
		else
			return "0";
	}
};

class Char: public AbstractScalar<char>{
public:
	Char(char val=0):AbstractScalar(val){}
	virtual ~Char(){}
	bool isNull() const override {return val_==CHAR_MIN;}
	void setNull() override {val_=CHAR_MIN;}
	void setChar(char val) override { val_ = val;}
	DATA_TYPE getType() const override {return DT_CHAR;}
	DATA_TYPE getRawType() const override { return DT_CHAR;}
	ConstantSP getInstance() const override {return ConstantSP(new Char());}
	ConstantSP getValue() const override {return ConstantSP(new Char(val_));}
	DATA_CATEGORY getCategory() const override {return INTEGRAL;}
	std::string getString() const override { return toString(val_);}
	std::string getScript() const override;
	IO_ERR deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement) override ;
	int getHash(int buckets) const override { return val_ == CHAR_MIN ? -1 : ((unsigned int)val_) % buckets;}
	static Char* parseChar(const std::string& str);
	static std::string toString(char val);
};

class Short: public AbstractScalar<short>{
public:
	Short(short val=0):AbstractScalar(val){}
	virtual ~Short(){}
	bool isNull() const override {return val_==SHRT_MIN;}
	void setNull() override {val_=SHRT_MIN;}
	void setShort(short val) override { val_ = val;}
	DATA_TYPE getType() const override {return DT_SHORT;}
	DATA_TYPE getRawType() const override { return DT_SHORT;}
	ConstantSP getInstance() const override {return ConstantSP(new Short());}
	ConstantSP getValue() const override {return ConstantSP(new Short(val_));}
	DATA_CATEGORY getCategory() const override {return INTEGRAL;}
	std::string getString() const override { return toString(val_);}
	IO_ERR deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement) override ;
	int getHash(int buckets) const override { return val_ == SHRT_MIN ? -1 : ((unsigned int)val_) % buckets;}
	static Short* parseShort(const std::string& str);
	static std::string toString(short val);
};

class Int: public AbstractScalar<int>{
public:
	Int(int val=0):AbstractScalar(val){}
	virtual ~Int(){}
	bool isNull() const override {return val_==INT_MIN;}
	void setNull() override {val_=INT_MIN;}
	void setInt(int val) override { val_ = val;}
	DATA_TYPE getType() const override {return DT_INT;}
	DATA_TYPE getRawType() const override { return DT_INT;}
	ConstantSP getInstance() const override {return ConstantSP(new Int());}
	ConstantSP getValue() const override {return ConstantSP(new Int(val_));}
	DATA_CATEGORY getCategory() const override {return INTEGRAL;}
	std::string getString() const override { return toString(val_);}
	IO_ERR deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement) override ;
	int getHash(int buckets) const override { return val_ == INT_MIN ? -1 : ((uint32_t)val_) % buckets;}
	static Int* parseInt(const std::string& str);
	static std::string toString(int val);
};

class EnumInt : public Int {
public:
	EnumInt(const std::string& desc, int val):Int(val), desc_(desc){}
	virtual ~EnumInt(){}
	std::string getScript() const override {return desc_;}
	ConstantSP getValue() const override {return ConstantSP(new EnumInt(desc_, val_));}
	ConstantSP getInstance() const override {return ConstantSP(new EnumInt(desc_, val_));}
	std::string getString() const override {return desc_;}

private:
	std::string desc_;
};

class Long: public AbstractScalar<long long>{
public:
	Long(long long val=0):AbstractScalar(val){}
	virtual ~Long(){}
	bool isNull() const override {return val_==LLONG_MIN;}
	void setNull() override {val_=LLONG_MIN;}
	void setLong(long long val) override { val_ = val;}
	DATA_TYPE getType() const override {return DT_LONG;}
	DATA_TYPE getRawType() const override { return DT_LONG;}
	ConstantSP getInstance() const override {return ConstantSP(new Long());}
	ConstantSP getValue() const override {return ConstantSP(new Long(val_));}
	std::string getString() const override { return toString(val_);}
	DATA_CATEGORY getCategory() const override {return INTEGRAL;}
	IO_ERR deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement) override ;
	int getHash(int buckets) const override { return val_ == LLONG_MIN ? -1 : ((uint64_t)val_) % buckets;}
	static Long* parseLong(const std::string& str);
	static std::string toString(long long val);
};

class Float: public AbstractScalar<float>{
public:
	Float(float val=0):AbstractScalar(val){}
	virtual ~Float(){}
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
	bool getChar(INDEX start, int len, char* buf) const override;
	const char* getCharConst(INDEX start, int len, char* buf) const override;
	bool getShort(INDEX start, int len, short* buf) const override;
	const short* getShortConst(INDEX start, int len, short* buf) const override;
	bool getInt(INDEX start, int len, int* buf) const override;
	const int* getIntConst(INDEX start, int len, int* buf) const override;
	bool getLong(INDEX start, int len, long long* buf) const override;
	const long long* getLongConst(INDEX start, int len, long long* buf) const override;
	std::string getString() const override { return toString(val_);}
	IO_ERR deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement) override ;
	static Float* parseFloat(const std::string& str);
	static std::string toString(float val);
};

class Double: public AbstractScalar<double>{
public:
	Double(double val=0):AbstractScalar(val){}
	virtual ~Double(){}
	bool isNull() const override {return val_==DBL_NMIN;}
	void setNull() override {val_=DBL_NMIN;}
	void setDouble(double val) override { val_ = val;}
	DATA_TYPE getType() const override {return DT_DOUBLE;}
	DATA_TYPE getRawType() const override { return DT_DOUBLE;}
	ConstantSP getInstance() const override {return ConstantSP(new Double());}
	ConstantSP getValue() const override {return ConstantSP(new Double(val_));}
	DATA_CATEGORY getCategory() const override {return FLOATING;}
	char getChar() const override {return isNull() ? CHAR_MIN : static_cast<char>(val_ < 0 ? val_ - 0.5 : val_ + 0.5);}
	short getShort() const override {return isNull() ? SHRT_MIN : static_cast<short>(val_ < 0 ? val_ - 0.5 : val_ + 0.5);}
	int getInt() const override {return isNull() ? INT_MIN : static_cast<int>(val_ < 0 ? val_ - 0.5 : val_ + 0.5);}
	long long  getLong() const override {return isNull() ? LLONG_MIN : static_cast<long long>(val_ < 0 ? val_ - 0.5 : val_ + 0.5);}
	bool getChar(INDEX start, int len, char* buf) const override;
	const char* getCharConst(INDEX start, int len, char* buf) const override;
	bool getShort(INDEX start, int len, short* buf) const override;
	const short* getShortConst(INDEX start, int len, short* buf) const override;
	bool getInt(INDEX start, int len, int* buf) const override;
	const int* getIntConst(INDEX start, int len, int* buf) const override;
	bool getLong(INDEX start, int len, long long* buf) const override;
	const long long* getLongConst(INDEX start, int len, long long* buf) const override;
	std::string getString() const override {return toString(val_);}
	IO_ERR deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement) override ;
	static Double* parseDouble(const std::string& str);
	static std::string toString(double val);
};

class EnumDouble : public Double {
public:
	EnumDouble(const std::string& desc, double val):Double(val), desc_(desc){}
	virtual ~EnumDouble(){}
	std::string getScript() const override {return desc_;}
	ConstantSP getValue() const override {return ConstantSP(new EnumDouble(desc_, val_));}
	ConstantSP getInstance() const override {return ConstantSP(new EnumDouble(desc_, val_));}
	std::string getString() const override { return desc_;}

private:
	std::string desc_;
};

class TemporalScalar:public Int{
public:
	TemporalScalar(int val=0):Int(val){}
	virtual ~TemporalScalar(){}
	DATA_CATEGORY getCategory() const override {return TEMPORAL;}
};

class Date:public TemporalScalar{
public:
	Date(int val=0):TemporalScalar(val){}
	virtual ~Date(){}
	Date(int year, int month, int day):TemporalScalar(Util::countDays(year,month,day)){}
	DATA_TYPE getType() const override {return DT_DATE;}
	ConstantSP getInstance() const override {return ConstantSP(new Date());}
	ConstantSP getValue() const override {return ConstantSP(new Date(val_));}
	std::string getString() const override { return toString(val_);}
	static Date* parseDate(const std::string& str);
	static std::string toString(int val);
	ConstantSP castTemporal(DATA_TYPE expectType) override;
};

class Month:public TemporalScalar{
public:
	Month():TemporalScalar(1999*12+11){}
	Month(int val):TemporalScalar(val){}
	Month(int year, int month):TemporalScalar(year*12+month-1){}
	virtual ~Month(){}
	DATA_TYPE getType() const override {return DT_MONTH;}
	ConstantSP getInstance() const override {return ConstantSP(new Month());}
	ConstantSP getValue() const override {return ConstantSP(new Month(val_));}
	std::string getString() const override { return toString(val_);}
	static Month* parseMonth(const std::string& str);
	static std::string toString(int val);
	ConstantSP castTemporal(DATA_TYPE expectType) override;
};

class Time:public TemporalScalar{
public:
	Time(int val=0):TemporalScalar(val){}
	Time(int hour, int minute, int second, int milliSecond):TemporalScalar(((hour*60+minute)*60+second)*1000+milliSecond){}
	virtual ~Time(){}
	DATA_TYPE getType() const override {return DT_TIME;}
	ConstantSP getInstance() const override {return ConstantSP(new Time());}
	ConstantSP getValue() const override {return ConstantSP(new Time(val_));}
	std::string getString() const override { return toString(val_);}
	void validate() override;
	static Time* parseTime(const std::string& str);
	static std::string toString(int val);
	ConstantSP castTemporal(DATA_TYPE expectType) override;
};

class NanoTime:public Long{
public:
	NanoTime(long long val=0):Long(val){}
	NanoTime(int hour, int minute, int second, int nanoSecond):Long(((hour*60+minute)*60+second)*1000000000ll+ nanoSecond){}
	virtual ~NanoTime(){}
	DATA_TYPE getType() const override {return DT_NANOTIME;}
	DATA_CATEGORY getCategory() const override {return TEMPORAL;}
	ConstantSP castTemporal(DATA_TYPE expectType) override;
	ConstantSP getInstance() const override {return ConstantSP(new NanoTime());}
	ConstantSP getValue() const override {return ConstantSP(new NanoTime(val_));}
	std::string getString() const override { return toString(val_);}
	void validate() override;
	static NanoTime* parseNanoTime(const std::string& str);
	static std::string toString(long long val);
};

class Timestamp:public Long{
public:
	Timestamp(long long val=0):Long(val){}
	Timestamp(int year, int month, int day,int hour, int minute, int second, int milliSecond);
	virtual ~Timestamp(){}
	DATA_TYPE getType() const override {return DT_TIMESTAMP;}
	DATA_CATEGORY getCategory() const override {return TEMPORAL;}
	ConstantSP castTemporal(DATA_TYPE expectType) override;
	ConstantSP getInstance() const override {return ConstantSP(new Timestamp());}
	ConstantSP getValue() const override {return ConstantSP(new Timestamp(val_));}
	std::string getString() const override { return toString(val_);}
	static Timestamp* parseTimestamp(const std::string& str);
	static std::string toString(long long val);
};

class NanoTimestamp:public Long{
public:
	NanoTimestamp(long long val=0):Long(val){}
	NanoTimestamp(int year, int month, int day,int hour, int minute, int second, int nanoSecond);
	virtual ~NanoTimestamp(){}
	DATA_TYPE getType() const override {return DT_NANOTIMESTAMP;}
	DATA_CATEGORY getCategory() const override {return TEMPORAL;}
	ConstantSP castTemporal(DATA_TYPE expectType) override;
	ConstantSP getInstance() const override {return ConstantSP(new NanoTimestamp());}
	ConstantSP getValue() const override {return ConstantSP(new NanoTimestamp(val_));}
	std::string getString() const override { return toString(val_);}
	static NanoTimestamp* parseNanoTimestamp(const std::string& str);
	static std::string toString(long long val);
};

class Minute:public TemporalScalar{
public:
	Minute(int val=0):TemporalScalar(val){}
	Minute(int hour, int minute):TemporalScalar(hour*60+minute){}
	virtual ~Minute(){}
	DATA_TYPE getType() const override {return DT_MINUTE;}
	ConstantSP getInstance() const override {return ConstantSP(new Minute());}
	ConstantSP getValue() const override {return ConstantSP(new Minute(val_));}
	std::string getString() const override { return toString(val_);}
	void validate() override;
	static Minute* parseMinute(const std::string& str);
	static std::string toString(int val);
	ConstantSP castTemporal(DATA_TYPE expectType) override;
};

class Second:public TemporalScalar{
public:
	Second(int val=0):TemporalScalar(val){}
	Second(int hour, int minute,int second):TemporalScalar((hour*60+minute)*60+second){}
	virtual ~Second(){}
	DATA_TYPE getType() const override {return DT_SECOND;}
	ConstantSP getInstance() const override {return ConstantSP(new Second());}
	ConstantSP getValue() const override {return ConstantSP(new Second(val_));}
	std::string getString() const override { return toString(val_);}
	void validate() override;
	static Second* parseSecond(const std::string& str);
	static std::string toString(int val);
	ConstantSP castTemporal(DATA_TYPE expectType) override;
};

class DateTime:public TemporalScalar{
public:
	DateTime(int val=0):TemporalScalar(val){}
	DateTime(int year, int month, int day, int hour, int minute,int second);
	virtual ~DateTime(){}
	DATA_TYPE getType() const override {return DT_DATETIME;}
	ConstantSP getInstance() const override {return ConstantSP(new DateTime());}
	ConstantSP getValue() const override {return ConstantSP(new DateTime(val_));}
	std::string getString() const override { return toString(val_);}
	static DateTime* parseDateTime(const std::string& str);
	static std::string toString(int val);
	ConstantSP castTemporal(DATA_TYPE expectType) override;
};

class DateHour:public TemporalScalar{
public:
    DateHour(int val=0):TemporalScalar(val){}
    DateHour(int year, int month, int day, int hour);
    virtual ~DateHour(){}
    DATA_TYPE getType() const override {return DT_DATEHOUR;}
    ConstantSP getInstance() const override {return ConstantSP(new DateHour());}
    ConstantSP getValue() const override {return ConstantSP(new DateHour(val_));}
    std::string getString() const override { return toString(val_);}
    static DateHour* parseDateHour(const std::string& str);
    static std::string toString(int val);
	ConstantSP castTemporal(DATA_TYPE expectType) override;
};

}

#if defined(_MSC_VER)
#pragma warning( pop )
#elif defined(__clang__)
#pragma clang diagnostic pop
#else // gcc
#pragma GCC diagnostic pop
#endif
