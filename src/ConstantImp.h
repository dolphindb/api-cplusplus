/*
 * ConstantImp.cpp
 *
 *  Created on: Aug 18, 2012
 *      Author: dzhou
 */

#ifndef CONSTANTIMP_H_
#define CONSTANTIMP_H_

#include <climits>
#include <algorithm>
#include <functional>
#include <stdio.h>
#include <cstring>
#include <iostream>
#include <unordered_set>
#include <typeinfo>

#include "Util.h"
#include "ScalarImp.h"
#include "Types.h"

namespace dolphindb {

class AnyVector:public Vector{
public:
	AnyVector(int size):data_(size, Constant::void_), containNull_(false){}
	AnyVector(const deque<ConstantSP>& data, bool containNull):data_(data), containNull_(containNull){}
	AnyVector(const vector<ConstantSP>& data, bool containNull):data_(data.begin(), data.end()), containNull_(containNull){}
	virtual ~AnyVector(){}
	virtual bool containNotMarshallableObject() const;
	virtual bool isLargeConstant() const {return !containNotMarshallableObject();}
	virtual bool getNullFlag() const {return containNull_;}
	virtual void setNullFlag(bool containNull){containNull_=containNull;}
	virtual INDEX getCapacity() const {return 0;}
	virtual bool isFastMode() const {return false;}
	virtual short getUnitLength() const {return 0;}
	virtual void clear();
	virtual bool sizeable() const {return true;}
	virtual DATA_TYPE getType() const {return DT_ANY;}
	virtual DATA_TYPE getRawType() const { return DT_ANY;}
	virtual DATA_CATEGORY getCategory() const {return MIXED;}
	virtual string getString(INDEX index) const {return data_[index]->getString();}
	virtual const string& getStringRef(INDEX index) const { throw RuntimeException("getStringRef method not supported for AnyVector");}
	virtual bool set(INDEX index, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value);
	virtual bool assign(const ConstantSP& value);
	virtual ConstantSP get(INDEX index) const {return data_[index];}
	virtual ConstantSP get(const ConstantSP& index) const;
	virtual bool hasNull(){return  hasNull(0, data_.size());}
	virtual bool hasNull(INDEX start, INDEX length);
	virtual bool isNull(INDEX index) const {return data_[index]->isNull();}
	virtual bool isNull() const {return false;}
	virtual void setNull(INDEX index){data_[index]= Constant::void_;}
	virtual void setNull(){}
	virtual void fill(INDEX start, INDEX length, const ConstantSP& value);
	virtual void nullFill(const ConstantSP& val);
	virtual bool isNull(INDEX start, int len, char* buf) const;
	virtual bool isValid(INDEX start, int len, char* buf) const;
	virtual ConstantSP getSubVector(INDEX start, INDEX length) const;
	virtual ConstantSP getInstance(INDEX size) const {return ConstantSP(new AnyVector(size));}
	virtual ConstantSP getValue() const;
	virtual ConstantSP getValue(INDEX capacity) const {return ConstantSP(new AnyVector(data_, containNull_));}
	virtual bool append(const ConstantSP& value);
	virtual bool append(const ConstantSP& value, INDEX appendSize){return false;}
	virtual bool remove(INDEX count);
	virtual void prev(INDEX steps);
	virtual void next(INDEX steps);
	virtual char getBool() const;
	virtual char getChar() const;
	virtual short getShort() const;
	virtual int getInt() const;
	virtual long long getLong() const;
	virtual INDEX getIndex() const;
	virtual float getFloat() const;
	virtual double getDouble() const;
	virtual char getBool(INDEX index) const {return get(index)->getBool();}
	virtual char getChar(INDEX index) const { return get(index)->getChar();}
	virtual short getShort(INDEX index) const { return get(index)->getShort();}
	virtual int getInt(INDEX index) const {return get(index)->getInt();}
	virtual long long getLong(INDEX index) const {return get(index)->getLong();}
	virtual INDEX getIndex(INDEX index) const {return get(index)->getIndex();}
	virtual float getFloat(INDEX index) const {return get(index)->getFloat();}
	virtual double getDouble(INDEX index) const {return get(index)->getDouble();}
	virtual int serialize(char* buf, int bufSize, INDEX indexStart, int offset, int& numElement, int& partial) const {
		throw RuntimeException("serialize method not supported for AnyVector");
	}
	virtual bool getBool(INDEX start, int len, char* buf) const;
	virtual bool getChar(INDEX start, int len,char* buf) const;
	virtual bool getShort(INDEX start, int len, short* buf) const;
	virtual bool getInt(INDEX start, int len, int* buf) const;
	virtual bool getLong(INDEX start, int len, long long* buf) const;
	virtual bool getIndex(INDEX start, int len, INDEX* buf) const;
	virtual bool getFloat(INDEX start, int len, float* buf) const;
	virtual bool getDouble(INDEX start, int len, double* buf) const;
	virtual const char* getBoolConst(INDEX start, int len, char* buf) const;
	virtual const char* getCharConst(INDEX start, int len,char* buf) const;
	virtual const short* getShortConst(INDEX start, int len, short* buf) const;
	virtual const int* getIntConst(INDEX start, int len, int* buf) const;
	virtual const long long* getLongConst(INDEX start, int len, long long* buf) const;
	virtual const INDEX* getIndexConst(INDEX start, int len, INDEX* buf) const;
	virtual const float* getFloatConst(INDEX start, int len, float* buf) const;
	virtual const double* getDoubleConst(INDEX start, int len, double* buf) const;
	virtual bool getString(INDEX start, int len, string** buf) const {
		throw RuntimeException("getString method not supported for AnyVector");
	}

	virtual bool getString(INDEX start, int len, char** buf) const {
		throw RuntimeException("getString method not supported for AnyVector");
	}

	virtual string** getStringConst(INDEX start, int len, string** buf) const {
		throw RuntimeException("getStringConst method not supported for AnyVector");
	}

	virtual char** getStringConst(INDEX start, int len, char** buf) const {
		throw RuntimeException("getStringConst method not supported for AnyVector");
	}
	virtual INDEX size() const {return data_.size();}
	virtual void neg(){throw RuntimeException("neg method not supported for AnyVector");}
	virtual void reverse(){std::reverse(data_.begin(),data_.end());}
	virtual void reverse(INDEX start, INDEX length){
		std::reverse(data_.begin()+start,data_.begin()+ start + length);
	}
	virtual void replace(const ConstantSP& oldVal, const ConstantSP& newVal){
		throw RuntimeException("replace method not supported for AnyVector");
	}
	virtual bool rank(bool sorted, INDEX* indices, INDEX* ranking){return false;}
	virtual long long getAllocatedMemory();
	bool isHomogeneousScalar(DATA_TYPE& type) const;
	bool isTabular() const;
	ConstantSP convertToRegularVector() const;
	virtual int asof(const ConstantSP& value) const {throw RuntimeException("asof not supported.");}

private:
	mutable deque<ConstantSP> data_;
	bool containNull_;
};

template<class T>
class AbstractFastVector: public Vector{
public:
	AbstractFastVector(int size, int capacity, T* srcData, T nullVal, bool containNull):nullVal_(nullVal),size_(size),
			capacity_(capacity), containNull_(containNull){
		if(capacity<size)
			capacity_=size;
		data_ = srcData;
	}
	virtual ~AbstractFastVector(){
		delete[] data_;
	}

	virtual INDEX reserve(INDEX capacity){
		if(capacity > capacity_){
			INDEX newCapacity= (std::max)((INDEX)(capacity_ * 1.2), capacity);
			T* newData = new T[newCapacity];
			memcpy(newData,data_,size_*sizeof(T));
			delete[] data_;
			data_=newData;
			capacity_=newCapacity;
		}
		return capacity_;
	}

	virtual bool isSorted(bool asc, bool strict=false) const {
		if (size_ <= 1) {
			return true;
		}
		for (int i = 1; i < size_; i++) {
			if (asc) {
				if (strict && data_[i] <= data_[i-1]) return false;
				if (!strict && data_[i] < data_[i-1]) return false;
			} else {
				if (strict && data_[i] >= data_[i-1]) return false;
				if (!strict && data_[i] > data_[i-1]) return false;
			}
		}
		return true;
	}

	virtual ConstantSP getSubVector(INDEX start, INDEX length) const {
		return getSubVector(start, length, std::abs(length));
	}

	virtual ConstantSP getSubVector(INDEX start, INDEX length, INDEX capacity) const {
		DATA_TYPE type = getType();
		T* data = getDataArray(start,length,capacity);
		if(data)
			return Util::createVector(type,std::abs(length), capacity, true, getExtraParamForType(), data, containNull_);
		else
			throw MemoryException();
	}

	VECTOR_TYPE getVectorType() const {return VECTOR_TYPE::ARRAY;}

	IO_ERR deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement){
		IO_ERR ret;
		INDEX end = indexStart + targetNumElement;
		if(end > capacity_ && !checkCapacity(end - size_))
			return NOSPACE;
		INDEX i=indexStart;
		size_t unitLength = sizeof(T);
		if(!in->isIntegerReversed()){
			size_t actualLength;
			ret = in->readBytes((char*)(data_ + i), unitLength,  targetNumElement, actualLength);
			i += actualLength;
		}
		else{
			for(; i<end; ++i){
				if((ret = in->readBytes((char*)(data_+i), unitLength, true)) != OK){
					numElement = i - indexStart;
					if(i > size_)
						size_ = i;
					if(!containNull_)
						containNull_ = hasNullInRange(indexStart, i);
					return ret;
				}
			}
		}
		numElement = i - indexStart;
		if(i > size_)
			size_ = i;
		if(!containNull_)
			containNull_ = hasNullInRange(indexStart, i);
		return ret;
	}

	ConstantSP retrieve(Vector* index) const {
		INDEX length = index->size();
		bool hasNull =containNull_;
		DATA_TYPE type = getType();

		T* data = getDataArray(index, hasNull);
		if(data){
			return Util::createVector(type,length, 0, true, getExtraParamForType(), (void*)data, hasNull);
		}
		else
			throw MemoryException();
	}

	virtual ConstantSP getInstance(INDEX size) const {
		DATA_TYPE type = getType();
		INDEX capacity = (std::max)(1, size);
		T* data = new T[capacity];
		return Util::createVector(type,size, capacity, true, getExtraParamForType(), (void*)data, false);
	}

	virtual ConstantSP getValue() const {
		DATA_TYPE type = getType();
		Vector* copy;
		T* data = getDataArray(0, size_);
		if(data != NULL){
			copy = Util::createVector(type,size_, 0, true, getExtraParamForType(), (void*)data, containNull_);
			copy->setForm(getForm());
			return copy;
		}
		else
			throw MemoryException();
	}

	virtual ConstantSP getValue(INDEX capacity) const {
		DATA_TYPE type = getType();
		Vector* copy;
		T* data = NULL;
		capacity = (std::max)(capacity, (INDEX)size_);
		data = new T[capacity];
		memcpy(data, data_, sizeof(T) * size_);
		copy = Util::createVector(type,size_, capacity, true, getExtraParamForType(), (void*)data, containNull_);
		copy->setForm(getForm());
		return copy;
	}

	virtual void initialize(){
		memset((void*)data_,0,sizeof(T)*size_);
	}

	virtual bool getNullFlag() const {
		return containNull_;
	}
	virtual void setNullFlag(bool containNull){
		containNull_=containNull;
	}

	virtual bool hasNull(){
		return hasNullInRange(0, size_);
	}

	virtual bool hasNull(INDEX start, INDEX length){
		return hasNullInRange(start, start + length);
	}

	bool hasNullInRange(INDEX start, INDEX end){
		for(;start<end && data_[start]!=nullVal_;++start);
		return start<end;
	}

	virtual INDEX getCapacity() const {return capacity_;}
	virtual bool isFastMode() const {return true;}
	virtual void* getDataArray() const {return (void*)data_;}
	virtual short getUnitLength() const {return sizeof(T);}
	virtual bool sizeable() const {return capacity_>0;}
	virtual ConstantSP get(INDEX index) const = 0;
	virtual char getBool() const {
		if(size_ == 1)
			return getBool(0);
		else
			throw RuntimeException("The vector can't be converted to bool scalar.");
	}
	virtual char getChar() const {
		if(size_ == 1)
			return getChar(0);
		else
			throw RuntimeException("The vector can't be converted to char scalar.");
	}
	virtual short getShort() const {
		if(size_ == 1)
			return getShort(0);
		else
			throw RuntimeException("The vector can't be converted to short scalar.");
	}
	virtual int getInt() const {
		if(size_ == 1)
			return getInt(0);
		else
			throw RuntimeException("The vector can't be converted to int scalar.");
	}
	virtual long long getLong() const {
		if(size_ == 1)
			return getLong(0);
		else
			throw RuntimeException("The vector can't be converted to long scalar.");
	}
	virtual INDEX getIndex() const {
		if(size_ == 1)
			return getIndex(0);
		else
			throw RuntimeException("The vector can't be converted to index scalar.");
	}
	virtual float getFloat() const {
		if(size_ == 1)
			return getFloat(0);
		else
			throw RuntimeException("The vector can't be converted to float scalar.");
	}
	virtual double getDouble() const {
		if(size_ == 1)
			return getDouble(0);
		else
			throw RuntimeException("The vector can't be converted to double scalar.");
	}
	virtual void setBool(char val){data_[0]=(T)val;}
	virtual void setChar(char val){data_[0]=(T)val;}
	virtual void setShort(short val){data_[0]=(T)val;}
	virtual void setInt(int val){data_[0]=(T)val;}
	virtual void setLong(long long val){data_[0]=(T)val;}
	virtual void setIndex(INDEX val){data_[0]=(T)val;}
	virtual void setFloat(float val){data_[0]=(T)val;}
	virtual void setDouble(double val){data_[0]=(T)val;}
	virtual void setString(const string& val){}
	virtual void setNull(){}

	virtual char getBool(INDEX index) const {return data_[index]==nullVal_?CHAR_MIN:data_[index];}
	virtual char getChar(INDEX index) const { return data_[index]==nullVal_?CHAR_MIN:data_[index];}
	virtual short getShort(INDEX index) const { return data_[index]==nullVal_?SHRT_MIN:data_[index];}
	virtual int getInt(INDEX index) const {return data_[index]==nullVal_?INT_MIN:data_[index];}
	virtual long long getLong(INDEX index) const {return data_[index]==nullVal_?LLONG_MIN:data_[index];}
	virtual INDEX getIndex(INDEX index) const {return data_[index]==nullVal_?INDEX_MIN:data_[index];}
	virtual float getFloat(INDEX index) const {return data_[index]==nullVal_?FLT_NMIN:data_[index];}
	virtual double getDouble(INDEX index) const {return data_[index]==nullVal_?DBL_NMIN:data_[index];}
	virtual string getString(INDEX index) const = 0;
	virtual bool isNull(INDEX index) const {return data_[index]==nullVal_;}
	virtual void setBool(INDEX index,bool val) {data_[index]=(T)val;}
	virtual void setChar(INDEX index,char val){data_[index]=(T)val;}
	virtual void setShort(INDEX index,short val){data_[index]=(T)val;}
	virtual void setInt(INDEX index,int val){data_[index]=(T)val;}
	virtual void setLong(INDEX index,long long val){data_[index]=(T)val;}
	virtual void setIndex(INDEX index, INDEX val){data_[index]=(T)val;}
	virtual void setFloat(INDEX index,float val){data_[index]=(T)val;}
	virtual void setDouble(INDEX index, double val){data_[index]=(T)val;}
	virtual void setNull(INDEX index){data_[index]=nullVal_;}

	virtual void clear(){
		size_ = 0;
		containNull_ = false;
	}

	virtual bool remove(const ConstantSP& index){
		INDEX size = index->size();
		INDEX invSize = size_ - size;
		if(invSize <= 0){
			size_ = 0;
			containNull_ = false;
			return true;
		}

		INDEX* a[1];
		INDEX** dataSeg = a;
		INDEX segmentSize  = size;
		int segCount = 1;
		if(index->isIndexArray())
			dataSeg[0] = index->getIndexArray();
		else if(index->isHugeIndexArray()){
			dataSeg = index->getHugeIndexArray();
			segmentSize = index->getSegmentSize();
			segCount = (size >> index->getSegmentSizeInBit()) + (size & (segmentSize - 1) ? 1 : 0);
		}
		else
			return false;
		INDEX prevIndex = dataSeg[0][0];
		INDEX cursor = prevIndex;
		INDEX j = 1;

		for(int i=0; i<segCount; ++i){
			INDEX* delIndices = dataSeg[i];
			INDEX count = ((std::min))(segmentSize, size - i * segmentSize);
			for(; j<count; ++j){
				if(delIndices[j] > prevIndex + 1){
					INDEX end = delIndices[j];
					for(INDEX k = prevIndex + 1; k<end; ++k)
						data_[cursor++] = data_[k];
				}
				prevIndex = delIndices[j];
			}
			j = 0;
		}

		for(INDEX k = prevIndex + 1; k<size_; ++k)
			data_[cursor++] = data_[k];

		size_ = invSize;
		if(containNull_){
			containNull_ = hasNullInRange(0, size_);
		}
		return true;
	}

	virtual bool remove(INDEX count){
		if(!sizeable() || std::abs(count)>size_)
			return false;
		if(count<0){
			count=-count;
			memmove(data_,data_+count,(size_-count)*sizeof(T));
		}
		size_-=count;
		return true;
	}
    virtual void next(INDEX steps){
       	if(steps>size_ || steps<0)
        	return;
    	memmove(data_,data_+steps,(size_-steps)*sizeof(T));
		for(int i=size_-steps;i<size_;++i)
			data_[i]=nullVal_;
		containNull_=true;
    }
    virtual void prev(INDEX steps){
    	if(steps>size_ || steps<0)
    		return;
    	memmove(data_+steps,data_,(size_-steps)*sizeof(T));
    	for(int i=0;i<steps;++i)
    		data_[i]=nullVal_;
    	containNull_=true;
	}

	virtual INDEX size() const {return size_;}

	T getMember(const int& index) const { return data_[index];}
	void setMember(const int& index, const T& val){data_[index]=val;}
	bool has(const T& val) const {
		int i;
		for(i=0;i<size_ && data_[i]!=val;++i);
		if(i<size_)
			return true;
		else
			return false;
	}

	INDEX search(const T& val){
		INDEX i;
		for(i=0;i<size_ && data_[i]!=val;++i);
		if(i<size_)
			return i;
		else
			return -1;
	}

	virtual void nullFill(const ConstantSP& val){
		if(!containNull_)
			return;
		T replace;
		if(val->getCategory()==FLOATING)
			replace=val->getDouble();
		else
			replace=val->getLong();
		replaceNull(replace);
	}

	virtual bool isNull(INDEX start, int len, char* buf) const {
		if(containNull_){
			for(int i=0;i<len;++i)
				buf[i]=(data_[start+i]==nullVal_);
		}
		else{
			memset(buf,0,len);
		}
		return true;
	}
	virtual bool isValid(INDEX start, int len, char* buf) const {
		if(containNull_){
			for(int i=0;i<len;++i)
				buf[i]=(data_[start+i]!=nullVal_);
		}
		else{
			memset(buf,1,len);
		}
		return true;
	}

	virtual bool getBool(INDEX start, int len, char* buf) const {
		return copyData<char>(start, len, DT_BOOL, CHAR_MIN, buf);
	}

	virtual const char* getBoolConst(INDEX start, int len, char* buf) const {
		return getDataConst<char>(start, len, DT_BOOL, CHAR_MIN, buf);
	}

	virtual char* getBoolBuffer(INDEX start, int len, char* buf) const {
		return getDataBuffer<char>(start, len, DT_BOOL, buf);
	}

	virtual bool getChar(INDEX start, int len, char* buf) const {
		return copyData<char>(start, len, DT_CHAR, CHAR_MIN, buf);
	}

	virtual const char* getCharConst(INDEX start, int len, char* buf) const {
		return getDataConst<char>(start, len, DT_CHAR, CHAR_MIN, buf);
	}

	virtual char* getCharBuffer(INDEX start, int len, char* buf) const {
		return getDataBuffer<char>(start, len, DT_CHAR, buf);
	}

	virtual bool getShort(INDEX start, int len, short* buf) const {
		return copyData<short>(start, len, DT_SHORT, SHRT_MIN, buf);
	}

	virtual const short* getShortConst(INDEX start, int len, short* buf) const {
		return getDataConst<short>(start, len, DT_SHORT, SHRT_MIN, buf);
	}

	virtual short* getShortBuffer(INDEX start, int len, short* buf) const {
		return getDataBuffer<short>(start, len, DT_SHORT, buf);
	}

	virtual bool getInt(INDEX start, int len, int* buf) const {
		DATA_TYPE type = getRawType()==DT_INT ? getType() : DT_INT;
		return copyData<int>(start, len, type, INT_MIN, buf);
	}

	virtual const int* getIntConst(INDEX start, int len, int* buf) const {
		DATA_TYPE type = getRawType()==DT_INT ? getType() : DT_INT;
		return getDataConst<int>(start, len, type, INT_MIN, buf);
	}

	virtual int* getIntBuffer(INDEX start, int len, int* buf) const {
		DATA_TYPE type = getRawType()==DT_INT ? getType() : DT_INT;
		return getDataBuffer<int>(start, len, type, buf);
	}

	virtual bool getLong(INDEX start, int len, long long* buf) const {
		DATA_TYPE type = getRawType()== DT_LONG ? getType() : DT_LONG;
		return copyData<long long>(start, len, type, LLONG_MIN, buf);
	}

	virtual const long long* getLongConst(INDEX start, int len, long long* buf) const {
		DATA_TYPE type = getRawType()== DT_LONG ? getType() : DT_LONG;
		return getDataConst<long long>(start, len, type, LLONG_MIN, buf);
	}

	virtual long long* getLongBuffer(INDEX start, int len, long long* buf) const {
		DATA_TYPE type = getRawType()== DT_LONG ? getType() : DT_LONG;
		return getDataBuffer<long long>(start, len, type, buf);
	}

	virtual bool getIndex(INDEX start, int len, INDEX* buf) const {
		return copyData<INDEX>(start, len, DT_INDEX, INDEX_MIN, buf);
	}

	virtual const INDEX* getIndexConst(INDEX start, int len, INDEX* buf) const {
		return getDataConst<INDEX>(start, len, DT_INDEX, INDEX_MIN, buf);
	}

	virtual INDEX* getIndexBuffer(INDEX start, int len, INDEX* buf) const {
		return getDataBuffer<INDEX>(start, len, DT_INDEX, buf);
	}

	virtual bool getFloat(INDEX start, int len, float* buf) const {
		return copyData<float>(start, len, DT_FLOAT, FLT_NMIN, buf);
	}

	virtual const float* getFloatConst(INDEX start, int len, float* buf) const {
		return getDataConst<float>(start, len, DT_FLOAT, FLT_NMIN, buf);
	}

	virtual float* getFloatBuffer(INDEX start, int len, float* buf) const {
		return getDataBuffer<float>(start, len, DT_FLOAT, buf);
	}

	virtual bool getDouble(INDEX start, int len, double* buf) const {
		return copyData<double>(start, len, DT_DOUBLE, DBL_NMIN, buf);
	}

	virtual const double* getDoubleConst(INDEX start, int len, double* buf) const {
		return getDataConst<double>(start, len, DT_DOUBLE, DBL_NMIN, buf);
	}

	virtual double* getDoubleBuffer(INDEX start, int len, double* buf) const {
		return getDataBuffer<double>(start, len, DT_DOUBLE, buf);
	}

	virtual void* getDataBuffer(INDEX start, int len, void* buf) const {
		return (T*)(data_+ start);
	}

	virtual bool getString(INDEX start, int len, string* buf) const {
		for(int i=0;i<len;++i)
			buf[i]=getString(i);
		return true;
	}

	virtual bool setBool(INDEX start, int len, const char* buf){
		return setData<char>(start, len, DT_BOOL, CHAR_MIN, buf);
	}

	virtual bool setChar(INDEX start, int len, const char* buf){
		return setData<char>(start, len, DT_CHAR, CHAR_MIN, buf);
	}

	virtual bool setShort(INDEX start, int len, const short* buf){
		return setData<short>(start, len, DT_SHORT, SHRT_MIN, buf);
	}

	virtual bool setInt(INDEX start, int len, const int* buf){
		DATA_TYPE type = getRawType()==DT_INT ? getType() : DT_INT;
		return setData<int>(start, len, type, INT_MIN, buf);
	}

	virtual bool setLong(INDEX start, int len, const long long* buf){
		DATA_TYPE type = getRawType()== DT_LONG ? getType() : DT_LONG;
		return setData<long long>(start, len, type, LLONG_MIN, buf);
	}

	virtual bool setIndex(INDEX start, int len, const INDEX* buf){
		return setData<INDEX>(start, len, DT_INDEX, INDEX_MIN, buf);
	}

	virtual bool setFloat(INDEX start, int len, const float* buf){
		return setData<float>(start, len, DT_FLOAT, FLT_NMIN, buf);
	}

	virtual bool setDouble(INDEX start, int len, const double* buf){
		return setData<double>(start, len, DT_DOUBLE, DBL_NMIN, buf);
	}

	virtual bool setData(INDEX start, int len, void* buf){
		if(buf != (void*)(data_+start))
			memcpy(data_+start,buf,sizeof(T)*len);
		return true;
	}

	virtual bool appendBool(char* buf, int len){
		return appendData<char>(buf, len, DT_BOOL, CHAR_MIN);
	}

	virtual bool appendChar(char* buf, int len){
		return appendData<char>(buf, len, DT_CHAR, CHAR_MIN);
	}

	virtual bool appendShort(short* buf, int len){
		return appendData<short>(buf, len, DT_SHORT, SHRT_MIN);
	}

	virtual bool appendInt(int* buf, int len){
		DATA_TYPE type = getRawType()==DT_INT ? getType() : DT_INT;
		return appendData<int>(buf, len, type, INT_MIN);
	}

	virtual bool appendLong(long long* buf, int len){
		DATA_TYPE type = getRawType()== DT_LONG ? getType() : DT_LONG;
		return appendData<long long>(buf, len, type, LLONG_MIN);
	}

	virtual bool appendIndex(INDEX* buf, int len){
		DATA_TYPE type = getRawType()== DT_INDEX ? getType() : DT_INDEX;
		return appendData<INDEX>(buf, len, type, INDEX_MIN);
	}

	virtual bool appendFloat(float* buf, int len){
		return appendData<float>(buf, len, DT_FLOAT, FLT_NMIN);
	}

	virtual bool appendDouble(double* buf, int len){
		return appendData<double>(buf, len, DT_DOUBLE, DBL_NMIN);
	}

	virtual bool assign(const ConstantSP& value){
		if(value->isVector()){
			if(size_!=value->size())
				return false;
		}
		fill(0,size_,value);
		return true;
	}

	virtual bool add(INDEX start, INDEX length, long long inc) {
		T incVal = (T)inc;
		if(containNull_){
			for(INDEX i=start; i<length; ++i)
				if(data_[i] != nullVal_)
					data_[i] += incVal;
		}
		else{
			for(INDEX i=start; i<length; ++i)
				data_[i] += incVal;
		}
		return true;
	}

	virtual bool add(INDEX start, INDEX length, double inc) {
		T incVal = (T)inc;
		if(containNull_){
			for(INDEX i=start; i<length; ++i)
				if(data_[i] != nullVal_)
					data_[i] += incVal;
		}
		else{
			for(INDEX i=start; i<length; ++i)
				data_[i] += incVal;
		}
		return true;
	}

	virtual void addIndex(INDEX start, INDEX length, INDEX offset){
		T incVal = (T)offset;
		for(INDEX i = 0; i < length; ++i)
			if(data_[i + start] >= 0)
				data_[i + start] += incVal;
	}

	virtual void neg(){
		int len=size();
		if(containNull_){
			for(int i=0;i<len;++i)
				if(data_[i]!=nullVal_)
					data_[i]=-data_[i];
		}
		else{
			for(int i=0;i<len;++i)
				data_[i]=-data_[i];
		}
	}

	virtual void replace(const ConstantSP& oldVal, const ConstantSP& newVal){
		T ov,nv;
		if(oldVal->isNull())
			ov=nullVal_;
		else if(getCategory()==FLOATING)
			ov=(T)oldVal->getLong();
		else
			ov=(T)oldVal->getDouble();

		if(newVal->isNull())
			nv=nullVal_;
		else if(getCategory()==FLOATING)
			nv=(T)newVal->getLong();
		else
			nv=(T)newVal->getDouble();

		for(int i=0;i<size_;++i)
			if(data_[i]==ov)
				data_[i]=nv;
	}

	virtual void reverse(){
		reverse(0,size_-1);
	}

	virtual void reverse(INDEX start, INDEX end){
		int len=(end-start+1)/2;
		end=end-start;
		T tmp;
		T* data=data_+start;
		for(int i=0;i<len;++i){
			tmp=data[i];
			data[i]=data[end-i];
			data[end-i]=tmp;
		}
	}

	virtual long long getAllocatedMemory() const {
		return sizeof(T)*(capacity_==0?size_:capacity_);
	}

	virtual int serialize(char* buf, int bufSize, INDEX indexStart, int offset, int& numElement, int& partial) const {
		//assume offset==0 and bufSize>=sizeof(T)
		if(indexStart >= size_)
			return -1;
		int len = sizeof(T);
		partial = 0;
		numElement = ((std::min))(bufSize / len, size_ -indexStart);
		memcpy(buf,(char*)(data_+indexStart),len * numElement);
		return len * numElement;
	}
	
	int serialize(char* buf, int bufSize, INDEX indexStart, int offset, int cellCountToSerialize, int& numElement, int& partial) const {
		//assume offset==0 and bufSize>=sizeof(T)
		if(indexStart >= size_)
			return -1;
		int len = sizeof(T);
		partial = 0;
		numElement = ((std::min))(bufSize / len, cellCountToSerialize);
		memcpy(buf,(char*)(data_+indexStart),len * numElement);
		return len * numElement;
	}
	
	virtual int asof(const ConstantSP& value) const{
		T target;
		try{
			if(getCategory() == FLOATING){
				target = (T)value->getDouble();
			}
			else{
				target = (T)value->getLong();
			}
		}
		catch(exception ex){
			throw ex;
		}

		int start = 0;
		int end = size_ - 1;
		int mid;
		while(start <= end){
			mid = (start + end)/2;
			if(data_[mid] <= target)
				start = mid + 1;
			else
				end = mid - 1;
		}
		return end;
	}

	virtual void resize(INDEX size) {
		if(size < 0)
        	return;
		if(size > capacity_){
			checkCapacity(size - size_);
		}
		size_ = size;
	}

protected:
	void replaceNull(T replace){
		for(int i=0;i<size_;++i)
			if(data_[i]==nullVal_)
				data_[i]=replace;
		containNull_=false;
	}

	template<typename Y>
	inline bool copyData(int start, int len, DATA_TYPE targetType, Y targetNullVal, Y* buf) const {
		if(getType() == targetType)
			memcpy(buf, data_+start, sizeof(Y) * len);
		else if(containNull_){
			if(targetType == DT_BOOL){
				for(int i=0;i<len;++i)
					buf[i] = (data_[start+i] == nullVal_)? targetNullVal : (bool)data_[start+i];
			}
			else{
				for(int i=0;i<len;++i)
					buf[i] = (data_[start+i] == nullVal_)? targetNullVal : data_[start+i];
			}
		}
		else{
			if(targetType == DT_BOOL){
				for(int i=0;i<len;++i)
					buf[i] = (bool)data_[start+i];
			}
			else{
				for(int i=0;i<len;++i)
					buf[i] = data_[start+i];
			}
		}
		return true;
	}

	template<typename Y>
	inline const Y* getDataConst(int start, int len, DATA_TYPE targetType, Y targetNullVal, Y* buf) const {
		if(getType() == targetType)
			return (Y*)data_+start;
		else if(containNull_)
			if(targetType == DT_BOOL){
				for(int i=0;i<len;++i)
					buf[i] = (data_[start+i] == nullVal_)? targetNullVal : (bool)data_[start+i];
			}
			else{
				for(int i=0;i<len;++i)
					buf[i] = (data_[start+i] == nullVal_)? targetNullVal : data_[start+i];
			}
		else{
			if(targetType == DT_BOOL){
				for(int i=0;i<len;++i)
					buf[i] = (bool)data_[start+i];
			}
			else{
				for(int i=0;i<len;++i)
					buf[i] = data_[start+i];
			}
		}
		return buf;
	}

	template<typename Y>
	inline Y* getDataBuffer(int start, int len, DATA_TYPE targetType, Y* buf) const {
		if(getType() == targetType)
			return (Y*)data_ + start;
		else
			return buf;
	}

	template<typename Y>
	inline bool setData(int start, int len, DATA_TYPE sourceType, Y sourceNullVal, const Y* buf){
		if(buf == (Y*)data_+start)
			return true;
		else if(getType() == sourceType)
			memcpy(data_+start,buf,sizeof(Y)*len);
		else
			for(int i=0;i<len;++i)
				data_[start+i]=(buf[i] == sourceNullVal)? nullVal_: buf[i];
		return true;
	}

	template<typename Y>
	inline bool appendData(Y* buf, int len, DATA_TYPE sourceType, Y sourceNullVal){
		if(!checkCapacity(len))
			return false;
		if(getType() == sourceType)
			memcpy(data_+size_, buf, sizeof(Y) * len);
		else
			for(int i=0;i<len;++i)
				data_[size_+i]=(buf[i] == sourceNullVal)? nullVal_ : buf[i];
		size_+=len;
		return true;
	}

	bool checkCapacity(int appendSize){
		if(size_+appendSize>capacity_){
			INDEX newCapacity= (size_+appendSize)*1.2;
			T* newData = new T[newCapacity];
			memcpy(newData,data_,size_*sizeof(T));
			delete[] data_;
			capacity_=newCapacity;
			data_=newData;
		}
		return true;
	}

	T* getDataArray(const Vector* indexVector, bool& hasNull) const {
		INDEX len = indexVector->size();
		T* buf = new T[len];

		UINDEX size=size_;
		if(containNull_){
			hasNull=containNull_;
			if(indexVector->isIndexArray()){
				UINDEX* bufIndex=(UINDEX*)indexVector->getIndexArray();
				for(INDEX i=0;i<len;++i)
					buf[i]=bufIndex[i]<size?data_[bufIndex[i]]:nullVal_;
			}
			else{
				UINDEX bufIndex[Util::BUF_SIZE];
				const UINDEX* pbufIndex;
				INDEX start=0;
				int count=0;
				int i;
				while(start<len){
					count=((std::min))(len-start,Util::BUF_SIZE);
					pbufIndex = (const UINDEX*)indexVector->getIndexConst(start,count,(INDEX*)bufIndex);
					for(i=0;i<count;++i)
						buf[start+i]=pbufIndex[i]<size?data_[pbufIndex[i]]:nullVal_;
					start+=count;
				}
			}
		}
		else{
			if(indexVector->isIndexArray()){
				UINDEX* bufIndex=(UINDEX*)indexVector->getIndexArray();
				for(INDEX i=0;i<len;++i){
					if(bufIndex[i]<size)
						buf[i]=data_[bufIndex[i]];
					else{
						buf[i]=nullVal_;
						hasNull=true;
					}
				}
			}
			else{
				UINDEX bufIndex[Util::BUF_SIZE];
				const UINDEX* pbufIndex;
				INDEX start=0;
				int count=0;
				int i;
				while(start<len){
					count=((std::min))(len-start,Util::BUF_SIZE);
					pbufIndex = (const UINDEX*)indexVector->getIndexConst(start,count,(INDEX*)bufIndex);
					for(i=0;i<count;++i){
						if(pbufIndex[i]<size)
							buf[start+i]=data_[pbufIndex[i]];
						else{
							buf[start+i]=nullVal_;
							hasNull=true;
						}
					}
					start+=count;
				}
			}
		}
		return buf;
	}

	T* getDataArray(INDEX start, INDEX length) const {
		return getDataArray(start, length, std::abs(length));
	}

	T* getDataArray(INDEX start, INDEX length, INDEX capacity) const {
		T* buf= new T[capacity];

		if(length>0)
			memcpy(buf,data_+start,length*sizeof(T));
		else{
			T* src=data_+start;
			T* dest=buf;
			length=std::abs(length);
			while(length>0){
				*dest=*src;
				++dest;
				--src;
				--length;
			}
		}
		return buf;
	}

	void getDataArray(INDEX start, INDEX length, T* buf) const {
		if(length>0)
			memcpy(buf,data_+start,length*sizeof(T));
		else{
			T* src=data_+start;
			T* dest=buf;
			length=std::abs(length);
			while(length>0){
				*dest=*src;
				++dest;
				--src;
				--length;
			}
		}
	}

	int getSegmentCount(INDEX size, int segmentSizeInBit) const {
		return (size >> segmentSizeInBit) + (size & ((1 << segmentSizeInBit) - 1) ? 1 : 0);
	}



protected:
	typedef typename unordered_map<T, INDEX>::const_iterator iterator;
	T* data_;
	T nullVal_;
	int size_;
	int capacity_;
	bool containNull_;
	DATA_TYPE dataType_;
};

class FastBoolVector:public AbstractFastVector<char>{
public:
	FastBoolVector(int size, int capacity, char* srcData, bool containNull):AbstractFastVector(size,capacity,srcData,CHAR_MIN, containNull){
		dataType_ = DT_BOOL;
	}
	virtual ~FastBoolVector(){}
	virtual DATA_TYPE getType() const {return DT_BOOL;}
	virtual DATA_TYPE getRawType() const { return DT_BOOL;}
	virtual DATA_CATEGORY getCategory() const {return LOGICAL;}
	virtual string getString(INDEX index) const { return Bool::toString(data_[index]);}
	virtual bool set(INDEX index, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value);
	virtual ConstantSP get(INDEX index) const;
	virtual ConstantSP get(const ConstantSP& index) const;
	virtual void fill(INDEX start, INDEX length, const ConstantSP& value);
	virtual bool append(const ConstantSP& value, INDEX appendSize);
	virtual bool append(const ConstantSP value, INDEX start, INDEX appendSize);
	virtual bool add(INDEX start, INDEX length, long long inc) {return false;}
	virtual bool add(INDEX start, INDEX length, double inc) {return false;}
	virtual int compare(INDEX index, const ConstantSP& target) const;
	virtual int asof(const ConstantSP& value) const {throw RuntimeException("asof not supported.");}
};

class FastCharVector:public AbstractFastVector<char>{
public:
	FastCharVector(int size, int capacity, char* srcData, bool containNull):AbstractFastVector(size,capacity,srcData,CHAR_MIN,containNull){
		dataType_ = DT_CHAR;
	}
	virtual ~FastCharVector(){}
	virtual DATA_TYPE getType() const {return DT_CHAR;}
	virtual DATA_TYPE getRawType() const { return DT_CHAR;}
	virtual DATA_CATEGORY getCategory() const {return INTEGRAL;}
	virtual string getString(INDEX index) const { return Char::toString(data_[index]);}
	virtual bool set(INDEX index, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value);
	virtual ConstantSP get(INDEX index) const;
	virtual ConstantSP get(const ConstantSP& index) const;
	virtual void fill(INDEX start, INDEX length, const ConstantSP& value);
	virtual bool append(const ConstantSP& value, INDEX appendSize);
	virtual bool append(const ConstantSP value, INDEX start, INDEX appendSize);
	virtual bool validIndex(INDEX uplimit);
	virtual bool validIndex(INDEX start, INDEX length, INDEX uplimit);
	virtual int compare(INDEX index, const ConstantSP& target) const;
	virtual bool getHash(INDEX start, int len, int buckets, int* buf) const {
		for(int i=0; i<len; ++i)
			buf[i] = (data_[start + i]==CHAR_MIN) ? -1 : (((unsigned int)data_[start + i]) % buckets);
		return true;
	}
	virtual void upper();
	virtual void lower();
};

class FastShortVector:public AbstractFastVector<short>{
public:
	FastShortVector(int size, int capacity, short* srcData,bool containNull):AbstractFastVector(size,capacity,srcData,SHRT_MIN,containNull){
		dataType_ = DT_SHORT;
	}
	virtual ~FastShortVector(){}
	virtual DATA_TYPE getType() const {return DT_SHORT;}
	virtual DATA_TYPE getRawType() const { return DT_SHORT;}
	virtual DATA_CATEGORY getCategory() const {return INTEGRAL;}
	virtual string getString(INDEX index) const { return Short::toString(data_[index]);}
	virtual bool set(INDEX index, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value);
	virtual ConstantSP get(INDEX index) const;
	virtual ConstantSP get(const ConstantSP& index) const;
	virtual void fill(INDEX start, INDEX length, const ConstantSP& value);
	virtual bool append(const ConstantSP& value, INDEX appendSize);
	virtual bool append(const ConstantSP value, INDEX start, INDEX appendSize);
	virtual bool validIndex(INDEX uplimit);
	virtual bool validIndex(INDEX start, INDEX length, INDEX uplimit);
	virtual int compare(INDEX index, const ConstantSP& target) const;
	virtual bool getHash(INDEX start, int len, int buckets, int* buf) const {
		for(int i=0; i<len; ++i)
			buf[i] = (data_[start + i]==SHRT_MIN) ? -1 : (((unsigned int)data_[start + i]) % buckets);
		return true;
	}
};

class FastIntVector:public AbstractFastVector<int>{
public:
	FastIntVector(int size, int capacity, int* srcData, bool containNull):AbstractFastVector(size,capacity,srcData,INT_MIN,containNull){
		dataType_ = DT_INT;
	}
	virtual ~FastIntVector(){}
	virtual DATA_TYPE getType() const {return DT_INT;}
	virtual DATA_TYPE getRawType() const { return DT_INT;}
	virtual DATA_CATEGORY getCategory() const {return INTEGRAL;}
#ifndef INDEX64
	virtual bool isIndexArray() const { return true;}
	virtual INDEX* getIndexArray() const { return data_;}
#endif
	virtual string getString(INDEX index) const { return Int::toString(data_[index]);}
	virtual bool set(INDEX index, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value);
	virtual ConstantSP get(INDEX index) const;
	virtual ConstantSP get(const ConstantSP& index) const;
	virtual void fill(INDEX start, INDEX length, const ConstantSP& value);
	virtual bool append(const ConstantSP& value, INDEX appendSize);
	virtual bool append(const ConstantSP value, INDEX start, INDEX appendSize);
	virtual bool validIndex(INDEX uplimit);
	virtual bool validIndex(INDEX start, INDEX length, INDEX uplimit);
	virtual int compare(INDEX index, const ConstantSP& target) const;
	virtual bool getHash(INDEX start, int len, int buckets, int* buf) const {
		for(int i=0; i<len; ++i)
			buf[i] = (data_[start + i]==INT_MIN) ? -1 : (((uint32_t)data_[start + i]) % buckets);
		return true;
	}
};

class FastLongVector:public AbstractFastVector<long long>{
public:
	FastLongVector(int size, int capacity, long long* srcData, bool containNull):AbstractFastVector(size,capacity,srcData,LLONG_MIN,containNull){
		dataType_ = DT_LONG;
	}
	virtual ~FastLongVector(){}
	virtual DATA_TYPE getType() const {return DT_LONG;}
	virtual DATA_TYPE getRawType() const { return DT_LONG;}
	virtual DATA_CATEGORY getCategory() const {return INTEGRAL;}
#ifdef INDEX64
	virtual bool isIndexArray() const { return true;}
	virtual INDEX* getIndexArray() const { return data_;}
#endif
	virtual string getString(INDEX index) const { return Long::toString(data_[index]);}
	virtual bool set(INDEX index, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value);
	virtual ConstantSP get(INDEX index) const;
	virtual ConstantSP get(const ConstantSP& index) const;
	virtual void fill(INDEX start, INDEX length, const ConstantSP& value);
	virtual bool append(const ConstantSP& value, INDEX appendSize);
	virtual bool append(const ConstantSP value, INDEX start, INDEX appendSize);
	virtual bool validIndex(INDEX uplimit);
	virtual bool validIndex(INDEX start, INDEX length, INDEX uplimit);
	virtual int compare(INDEX index, const ConstantSP& target) const;
	virtual bool getHash(INDEX start, int len, int buckets, int* buf) const {
		for(int i=0; i<len; ++i)
			buf[i] = (data_[start + i]==LLONG_MIN) ? -1 : (((uint64_t)data_[start + i]) % buckets);
		return true;
	}
};

class FastFloatVector:public AbstractFastVector<float>{
public:
	FastFloatVector(int size, int capacity, float* srcData, bool containNull):AbstractFastVector(size,capacity,srcData,FLT_NMIN,containNull){
		dataType_ = DT_FLOAT;
	}
	virtual ~FastFloatVector(){}
	virtual DATA_TYPE getType() const {return DT_FLOAT;}
	virtual DATA_TYPE getRawType() const { return DT_FLOAT;}
	virtual DATA_CATEGORY getCategory() const {return FLOATING;}
	virtual char getChar(INDEX index) const { return getData<char>(index, CHAR_MIN);}
	virtual short getShort(INDEX index) const { return getData<short>(index, SHRT_MIN);}
	virtual int getInt(INDEX index) const {return getData<int>(index, INT_MIN);}
	virtual long long getLong(INDEX index) const {return getData<long long>(index, LLONG_MIN);}

	virtual bool getChar(INDEX start, int len, char* buf) const {
		return copyData<char>(start, len, CHAR_MIN, buf);
	}
	virtual const char* getCharConst(INDEX start, int len, char* buf) const {
		copyData<char>(start, len, CHAR_MIN, buf);
		return buf;
	}
	virtual bool getShort(INDEX start, int len, short* buf) const {
		return copyData<short>(start, len, SHRT_MIN, buf);
	}
	virtual const short* getShortConst(INDEX start, int len, short* buf) const {
		copyData<short>(start, len, SHRT_MIN, buf);
		return buf;
	}
	virtual bool getInt(INDEX start, int len, int* buf) const {
		return copyData<int>(start, len, INT_MIN, buf);
	}
	virtual const int* getIntConst(INDEX start, int len, int* buf) const {
		copyData<int>(start, len, INT_MIN, buf);
		return buf;
	}
	virtual bool getLong(INDEX start, int len, long long* buf) const {
		return copyData<long long>(start, len, LLONG_MIN, buf);
	}
	virtual const long long* getLongConst(INDEX start, int len, long long* buf) const {
		copyData<long long>(start, len, LLONG_MIN, buf);
		return buf;
	}

	virtual string getString(INDEX index) const { return Float::toString(data_[index]);}
	virtual bool set(INDEX index, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value);
	virtual ConstantSP get(INDEX index) const;
	virtual ConstantSP get(const ConstantSP& index) const;
	virtual void fill(INDEX start, INDEX length, const ConstantSP& value);
	virtual bool append(const ConstantSP& value, INDEX appendSize);
	virtual bool append(const ConstantSP value, INDEX start, INDEX appendSize);
	virtual int compare(INDEX index, const ConstantSP& target) const;

private:
	template<typename Y>
	inline Y getData(INDEX index, Y targetNullVal) const {
		float x = data_[index];
		return (x == nullVal_) ? targetNullVal : (x < 0 ? (Y)(x - 0.5) : (Y)(x + 0.5));
	}
	template<typename Y>
	inline bool copyData(INDEX start, int len, Y targetNullVal, Y* buf) const {
		if(containNull_)
			for(int i=0;i<len;++i){
				float x=data_[start+i];
				buf[i] = (x==nullVal_)? targetNullVal : (x<0?(Y)(x-0.5):(Y)(x+0.5));
			}
		else
			for(int i=0;i<len;++i){
				float x=data_[start+i];
				buf[i] = x<0? (Y)(x-0.5) : (Y)(x+0.5);
			}
		return true;
	}
};

class FastDoubleVector:public AbstractFastVector<double>{
public:
	FastDoubleVector(int size, int capacity, double* srcData, bool containNull):AbstractFastVector(size,capacity,srcData,DBL_NMIN,containNull){
		dataType_ = DT_DOUBLE;
	}
	virtual ~FastDoubleVector(){}
	virtual DATA_TYPE getType() const {return DT_DOUBLE;}
	virtual DATA_TYPE getRawType() const { return DT_DOUBLE;}
	virtual DATA_CATEGORY getCategory() const {return FLOATING;}
	virtual char getChar(INDEX index) const { return getData<char>(index, CHAR_MIN);}
	virtual short getShort(INDEX index) const { return getData<short>(index, SHRT_MIN);}
	virtual int getInt(INDEX index) const {return getData<int>(index, INT_MIN);}
	virtual long long getLong(INDEX index) const {return getData<long long>(index, LLONG_MIN);}

	virtual bool getChar(INDEX start, int len, char* buf) const {
		return copyData<char>(start, len, CHAR_MIN, buf);
	}
	virtual const char* getCharConst(INDEX start, int len, char* buf) const {
		copyData<char>(start, len, CHAR_MIN, buf);
		return buf;
	}
	virtual bool getShort(INDEX start, int len, short* buf) const {
		return copyData<short>(start, len, SHRT_MIN, buf);
	}
	virtual const short* getShortConst(INDEX start, int len, short* buf) const {
		copyData<short>(start, len, SHRT_MIN, buf);
		return buf;
	}
	virtual bool getInt(INDEX start, int len, int* buf) const {
		return copyData<int>(start, len, INT_MIN, buf);
	}
	virtual const int* getIntConst(INDEX start, int len, int* buf) const {
		copyData<int>(start, len, INT_MIN, buf);
		return buf;
	}
	virtual bool getLong(INDEX start, int len, long long* buf) const {
		return copyData<long long>(start, len, LLONG_MIN, buf);
	}
	virtual const long long* getLongConst(INDEX start, int len, long long* buf) const {
		copyData<long long>(start, len, LLONG_MIN, buf);
		return buf;
	}
	virtual string getString(INDEX index) const { return Double::toString(data_[index]);}
	virtual bool set(INDEX index, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value);
	virtual ConstantSP get(INDEX index) const;
	virtual ConstantSP get(const ConstantSP& index) const;
	virtual void fill(INDEX start, INDEX length, const ConstantSP& value);
	virtual bool append(const ConstantSP& value, INDEX appendSize);
	virtual bool append(const ConstantSP value, INDEX start, INDEX );
	virtual int compare(INDEX index, const ConstantSP& target) const;

private:
	template<typename Y>
	inline Y getData(INDEX index, Y targetNullVal) const{
		double x = data_[index];
		return (x == nullVal_) ? targetNullVal : (x < 0 ? (Y)(x - 0.5) : (Y)(x + 0.5));
	}
	template<typename Y>
	inline bool copyData(INDEX start, int len, Y targetNullVal, Y* buf) const {
		if(containNull_)
			for(int i=0;i<len;++i){
				double x=data_[start+i];
				buf[i] = (x==nullVal_)? targetNullVal : (x<0?(Y)(x-0.5):(Y)(x+0.5));
			}
		else
			for(int i=0;i<len;++i){
				double x=data_[start+i];
				buf[i] = x<0? (Y)(x-0.5) : (Y)(x+0.5);
			}
		return true;
	}

};

class FastTemporalVector:public FastIntVector{
public:
	FastTemporalVector(int size,int capacity,int* srcData,bool containNull):FastIntVector(size,capacity,srcData,containNull){}
	virtual ~FastTemporalVector(){}
	virtual DATA_CATEGORY getCategory() const {return TEMPORAL;}
	virtual bool isIndexArray() const { return false;}
	virtual INDEX* getIndexArray() const { return NULL;}
	virtual ConstantSP castTemporal(DATA_TYPE expectType) = 0;
};

class FastArrayVector: public Vector {
public:
	FastArrayVector(int size, int capacity, char* srcData, bool containNull, DATA_TYPE dataType, INDEX *pindex=NULL) {
		stage_ = 0;
		countBytes_ = 1;
		rowCount_ = 0;
		rowsRead_ = 1;

		index_ 	     	= NULL;
		dataType_ 		= dataType;	// e.g. DT_INT + ARRAY_TYPE_BASE
		dataCategory 	= ARRAY;
		baseType_ 	 	= DATA_TYPE(dataType_ - ARRAY_TYPE_BASE);	// e.g. DT_INT
		index_ 			= Util::createVector(DT_INT, size, capacity, true, 0, pindex);
		valueSize_      = 0;
		value_          = Util::createVector(baseType_, 0, 0, true, 0, NULL);
		size_           = index_->size();
		baseUnitLength_ = value_->getUnitLength();
	}

	FastArrayVector(const VectorSP& index, const VectorSP& value) {
		index_ = index;
		value_ = value;
		stage_ = 0;
		countBytes_ = 1;
		rowCount_ = 0;
		rowsRead_ = 1;
		size_ = index->size();
		if(!index->isFastMode())
			throw RuntimeException("The index vector for the constructor of FastArrayVector must be a regular vector.");
		baseType_ = value_->getType();
		baseUnitLength_ = value_->getUnitLength();
		dataType_ = (DATA_TYPE)(baseType_ + ARRAY_TYPE_BASE);
		size_ = index->size();
		valueSize_ = value->size();
		if(value->getNullFlag())
			containNull_ = hasNull(0, size_);
		else
			containNull_ = false;
		index_->setTemporary(false);
		value_->setTemporary(false);
	}

	virtual ~FastArrayVector(){}
	virtual DATA_TYPE 	  getType() const override {return dataType_;};
	virtual DATA_TYPE 	  getRawType() const { return baseType_;}
	virtual DATA_CATEGORY getCategory() const {return ARRAY;}
	virtual VECTOR_TYPE   getVectorType() const {return VECTOR_TYPE::ARRAYVECTOR;}
	virtual DATA_FORM     getForm() const { return DF_VECTOR;}
	virtual INDEX		  getValueSize() const { return valueSize_; }
	virtual void*		  getDataArray() const { return value_->getDataArray(); }
	virtual bool		  validIndex(INDEX uplimit) { return value_->validIndex(uplimit); }
	virtual bool		  validIndex(INDEX start, INDEX length, INDEX uplimit) { return value_->validIndex(start, length, uplimit); }

	virtual void 		  fill(INDEX start, INDEX length, const ConstantSP& value){throw RuntimeException("Array Vector doesn't support method fill");}
	// Note: Currently, the update and delete operations on arrayVector are not supported.
	virtual bool 	      set(INDEX index, const ConstantSP& value) {throw RuntimeException("Array Vector doesn't support method set");}
	virtual bool		  set(const ConstantSP& index, const ConstantSP& value) {throw RuntimeException("Array Vector doesn't support method set");}

	virtual string 		  getString(INDEX index) const;
	virtual string 		  getString() const;
	// get a sub-array-vector, corresponding to `arrayVector[start, start + length]`
	virtual ConstantSP 	  getSubVector(INDEX start, INDEX length) const { return getSubVector(start, length, length); }
	virtual ConstantSP    getSubVector(INDEX start, INDEX length, INDEX capacity) const;
	virtual	bool 		  isNull(INDEX index) const;
	virtual	bool   		  isNull(INDEX start, int len, char* buf) const;
	virtual	bool 		  isValid(INDEX start, int len, char* buf) const;
	virtual bool  		  remove(INDEX count);
	virtual bool 	      remove(const ConstantSP& index);
	virtual ConstantSP    getValue() const;
	virtual ConstantSP    getValue(INDEX capacity) const;
	virtual INDEX 		  getCapacity() const { return index_->getCapacity(); }
	virtual short 		  getUnitLength() const { return value_->getUnitLength(); }
	virtual ConstantSP    getInstance(INDEX size) const;
	virtual INDEX 		  size() const { return size_; }
	virtual bool 		  sizeable() const {return value_->sizeable();}
	virtual long long     count() const { return count(0, size_);}
	virtual long long 	  count(INDEX start, INDEX length) const;

#ifndef INDEX64
	virtual bool isIndexArray() const { return true;}
	virtual INDEX* getIndexArray() const { return index_->getIndexArray();}
#endif
	
	// get the `index`th element each row, corresponding to `arrayVector[index]`
	virtual ConstantSP 		get(INDEX index) const;
	virtual ConstantSP 		get(INDEX column, INDEX rowStart,INDEX rowEnd) const;
	virtual ConstantSP 		get(INDEX offset, const ConstantSP& index) const;
	virtual ConstantSP 		get(const ConstantSP& index) const;
	virtual void 	   		clear();
	virtual void 			reverse();
	virtual void 			reverse(INDEX start, INDEX length);
	
	virtual bool   append(const ConstantSP& value, INDEX count);
	virtual bool   append(const ConstantSP& value);
	virtual bool   append(const ConstantSP& value, INDEX start, INDEX len);
	virtual bool   append(const ConstantSP& value, const ConstantSP& index);
	virtual int    compare(INDEX index, const ConstantSP& target) const {throw RuntimeException("Array Vector doesn't support method compare");}
	virtual void   neg() {throw RuntimeException("Array Vector doesn't support method neg");}
	virtual void   prev(INDEX steps) {throw RuntimeException("Array Vector doesn't support method prev");}
	virtual void   next(INDEX steps) {throw RuntimeException("Array Vector doesn't support method next");}
	virtual bool   getHash(INDEX start, int len, int buckets, int* buf) const {throw RuntimeException("Array Vector doesn't support method getHash");}
	virtual int    serialize(char* buf, int bufSize, INDEX indexStart, int offset, int& numElement, int& partial) const;
	virtual IO_ERR deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement);
	virtual INDEX rows() const { return index_->rows(); }
	virtual INDEX columns() const { return value_->rows(); }
	void reserveValue(INDEX capacity) { value_->reserve(capacity); }

private:
	int 		serializeFixedLength(char* buf, int bufSize, INDEX indexStart, int offset, int targetNumElement, int& numElement, int& partial) const;
	int 		serializeVariableLength(char* buf, int bufSize, INDEX indexStart, int offset, int targetNumElement, int& numElement, int& partial) const;
	IO_ERR 	    deserializeFixedLength(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement);
	IO_ERR 		deserializeVariableLength(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement);
	INDEX 		lowerBoundIndex(INDEX* data, INDEX size, INDEX start, INDEX value) const;
	ConstantSP  convertRowIndexToValueIndex(INDEX offset, const ConstantSP& rowIndexVector) const;
	ConstantSP  sliceRows(INDEX offset, const ConstantSP& rowIndexVector) const;
	ConstantSP  sliceColumnRange(int colStart, int colEnd, INDEX rowStart, INDEX rowEnd) const;
	ConstantSP  sliceOneColumn(int colIndex, INDEX rowStart, INDEX rowEnd) const;

private:

	bool 		   containNull_;
	DATA_TYPE 	   baseType_;
	DATA_TYPE      dataType_;
	DATA_CATEGORY  dataCategory;
	int 		   baseUnitLength_;
	INDEX 		   valueSize_;
	INDEX		   size_;
	VectorSP 	   index_;
	VectorSP 	   value_;

	/* Variables related to deserialization */
	int 			rowsRead_; 	  // applicable when stage_ = 1 or 2
	unsigned char 	stage_; 	  // 0: block header, 1: array of count, 2:array of data
	unsigned char 	countBytes_;  // 1: unsigned char, 2: unsigned short, 4: unsigned int
	unsigned short 	rowCount_; 	  // number of rows in this block
};


class FastDateVector:public FastTemporalVector{
public:
	FastDateVector(int size,int capacity,int* srcData,bool containNull):FastTemporalVector(size,capacity,srcData,containNull){
		dataType_ = DT_DATE;
	}
	virtual ~FastDateVector(){}
	virtual DATA_TYPE getType() const {return DT_DATE;}
	virtual ConstantSP get(INDEX index) const {return ConstantSP(new Date(data_[index]));}
	virtual ConstantSP get(const ConstantSP& index) const;
	virtual string getString(INDEX index) const {return Date::toString(data_[index]);}
	virtual ConstantSP castTemporal(DATA_TYPE expectType);
};

class FastDateTimeVector:public FastTemporalVector{
public:
	FastDateTimeVector(int size,int capacity,int* srcData,bool containNull):FastTemporalVector(size,capacity,srcData,containNull){
		dataType_ = DT_DATETIME;
	}
	virtual ~FastDateTimeVector(){}
	virtual DATA_TYPE getType() const {return DT_DATETIME;}
	virtual ConstantSP get(INDEX index) const {return ConstantSP(new DateTime(data_[index]));}
	virtual ConstantSP get(const ConstantSP& index) const;
	virtual string getString(INDEX index) const { return DateTime::toString(data_[index]);}
	virtual ConstantSP castTemporal(DATA_TYPE expectType);
};

class FastDateHourVector:public FastTemporalVector{
public:
    FastDateHourVector(int size,int capacity,int* srcData,bool containNull):FastTemporalVector(size,capacity,srcData,containNull){
		dataType_ = DT_DATEHOUR;
	}
    virtual ~FastDateHourVector(){}
    virtual DATA_TYPE getType() const {return DT_DATEHOUR;}
    virtual ConstantSP get(INDEX index) const {return ConstantSP(new DateHour(data_[index]));}
    virtual ConstantSP get(const ConstantSP& index) const;
    virtual string getString(INDEX index) const { return DateHour::toString(data_[index]);}
    virtual ConstantSP castTemporal(DATA_TYPE expectType);
};

class FastMonthVector:public FastTemporalVector{
public:
	FastMonthVector(int size,int capacity,int* srcData,bool containNull):FastTemporalVector(size,capacity,srcData,containNull){
		dataType_ = DT_MONTH;
	}
	virtual ~FastMonthVector(){}
	virtual DATA_TYPE getType() const {return DT_MONTH;}
	virtual ConstantSP get(INDEX index) const {return ConstantSP(new Month(data_[index]));}
	virtual ConstantSP get(const ConstantSP& index) const;
	virtual string getString(INDEX index) const { return Month::toString(data_[index]);}
	virtual ConstantSP castTemporal(DATA_TYPE expectType){
		if(expectType == DT_MONTH)
			return getValue();
		else
			throw RuntimeException("castTemporal from MONTH to "+ Util::getDataTypeString(expectType)+" not supported ");
	}
};

class FastTimeVector:public FastTemporalVector{
public:
	FastTimeVector(int size,int capacity,int* srcData,bool containNull):FastTemporalVector(size,capacity,srcData,containNull){
		dataType_ = DT_TIME;
	}
	virtual ~FastTimeVector(){}
	virtual DATA_TYPE getType() const {return DT_TIME;}
	virtual ConstantSP get(INDEX index) const {return ConstantSP(new Time(data_[index]));}
	virtual ConstantSP get(const ConstantSP& index) const;
	virtual string getString(INDEX index) const { return Time::toString(data_[index]);}
	virtual void validate();
	virtual ConstantSP castTemporal(DATA_TYPE expectType);
};

class FastMinuteVector:public FastTemporalVector{
public:
	FastMinuteVector(int size,int capacity,int* srcData,bool containNull):FastTemporalVector(size,capacity,srcData,containNull){
		dataType_ = DT_MINUTE;
	}
	virtual ~FastMinuteVector(){}
	virtual DATA_TYPE getType() const {return DT_MINUTE;}
	virtual ConstantSP get(INDEX index) const {return ConstantSP(new Minute(data_[index]));}
	virtual ConstantSP get(const ConstantSP& index) const;
	virtual string getString(INDEX index) const { return Minute::toString(data_[index]);}
	virtual void validate();
	virtual ConstantSP castTemporal(DATA_TYPE expectType);
};

class FastSecondVector:public FastTemporalVector{
public:
	FastSecondVector(int size,int capacity,int* srcData,bool containNull):FastTemporalVector(size,capacity,srcData,containNull){
		dataType_ = DT_SECOND;
	}
	virtual ~FastSecondVector(){}
	virtual DATA_TYPE getType() const {return DT_SECOND;}
	virtual ConstantSP get(INDEX index) const {return ConstantSP(new Second(data_[index]));}
	virtual ConstantSP get(const ConstantSP& index) const;
	virtual string getString(INDEX index) const { return Second::toString(data_[index]);}
	virtual void validate();
	virtual ConstantSP castTemporal(DATA_TYPE expectType);
};

class FastNanoTimeVector:public FastLongVector{
public:
	FastNanoTimeVector(int size,int capacity,long long* srcData,bool containNull):FastLongVector(size,capacity,srcData,containNull){
		dataType_ = DT_NANOTIME;
	}
	virtual ~FastNanoTimeVector(){}
	virtual DATA_TYPE getType() const {return DT_NANOTIME;}
	virtual DATA_CATEGORY getCategory() const {return TEMPORAL;}
	virtual ConstantSP get(INDEX index) const {return ConstantSP(new NanoTime(data_[index]));}
	virtual ConstantSP get(const ConstantSP& index) const;
	virtual string getString(INDEX index) const { return NanoTime::toString(data_[index]);}
	virtual void validate();
	virtual ConstantSP castTemporal(DATA_TYPE expectType);
};

class FastTimestampVector:public FastLongVector{
public:
	FastTimestampVector(int size,int capacity,long long* srcData,bool containNull):FastLongVector(size,capacity,srcData,containNull){
		dataType_ = DT_TIMESTAMP;
	}
	virtual ~FastTimestampVector(){}
	virtual DATA_TYPE getType() const {return DT_TIMESTAMP;}
	virtual DATA_CATEGORY getCategory() const {return TEMPORAL;}
	virtual ConstantSP get(INDEX index) const {return ConstantSP(new Timestamp(data_[index]));}
	virtual ConstantSP get(const ConstantSP& index) const;
	virtual string getString(INDEX index) const { return Timestamp::toString(data_[index]);}
	virtual ConstantSP castTemporal(DATA_TYPE expectType);
};

class FastNanoTimestampVector:public FastLongVector{
public:
	FastNanoTimestampVector(int size,int capacity,long long* srcData,bool containNull):FastLongVector(size,capacity,srcData,containNull){
		dataType_ = DT_NANOTIMESTAMP;
	}
	virtual ~FastNanoTimestampVector(){}
	virtual DATA_TYPE getType() const {return DT_NANOTIMESTAMP;}
	virtual DATA_CATEGORY getCategory() const {return TEMPORAL;}
	virtual ConstantSP get(INDEX index) const {return ConstantSP(new NanoTimestamp(data_[index]));}
	virtual ConstantSP get(const ConstantSP& index) const;
	virtual string getString(INDEX index) const { return NanoTimestamp::toString(data_[index]);}
	virtual ConstantSP castTemporal(DATA_TYPE expectType);
};

class FastCompressedVector : public FastCharVector {
public:
	FastCompressedVector(int size, int capacity, char* srcData)
		:FastCharVector(size, capacity, srcData, false){}
	virtual ~FastCompressedVector(){}
	virtual DATA_TYPE getType() const {return DT_COMPRESS;}
	virtual INDEX uncompressedRows() const;
	virtual int asof(const ConstantSP& value) const {throw RuntimeException("asof not supported.");}
};

class FastBoolMatrix:public Matrix, public FastBoolVector{
public:
	FastBoolMatrix(int cols, int rows, int colCapacity, char* data, bool containNull):Matrix(cols,rows),FastBoolVector(cols*rows,colCapacity*rows,data,containNull){setForm(DF_MATRIX);}
	virtual ~FastBoolMatrix(){}
	virtual ConstantSP getValue() const;
	virtual INDEX columns() const {return cols_;}
	virtual INDEX rows() const {return rows_;}
	virtual bool reshape(INDEX cols, INDEX rows) {return Matrix::reshape(cols, rows);}
	virtual ConstantSP getRowLabel() const {return rowLabel_;}
	virtual void setRowLabel(const ConstantSP& label){Matrix::setRowLabel(label);}
	virtual ConstantSP getColumnLabel() const {return colLabel_;}
	virtual void setColumnLabel(const ConstantSP& label){Matrix::setColumnLabel(label);}
	virtual string getString(int column, int row) const {return FastBoolVector::getString(column*rows_+row);}
	virtual string getString() const { return Matrix::getString();}
	virtual string getString(INDEX index) const {return Matrix::getString(index);}
	virtual bool set(INDEX column, INDEX row, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value){return Matrix::set(index,value);}
	virtual ConstantSP get(const ConstantSP& index) const {return Matrix::get(index);}
	virtual ConstantSP getItem(INDEX index) const {return getColumn(index);}
	virtual bool setItem(INDEX index, const ConstantSP& value){return setColumn(index,value);}
	virtual ConstantSP getRow(INDEX index) const;
	virtual ConstantSP getWindow(INDEX colStart, int colLength,INDEX rowStart, int rowLength) const;
	virtual ConstantSP getColumn(INDEX index) const;
	virtual bool setColumn(INDEX index, const ConstantSP& value){fill(index*rows_,rows_,value);return true;}
	virtual ConstantSP getInstance(INDEX size) const { return Util::createMatrix(getType(),size,rows_,size,getExtraParamForType());}
	virtual ConstantSP getInstance() const { return Util::createMatrix(getType(),cols_,rows_,cols_,getExtraParamForType());}
	virtual ConstantSP get(INDEX column, INDEX row) const {return FastBoolVector::get(column*rows_+row);}
};

class FastCharMatrix:public Matrix, public FastCharVector{
public:
	FastCharMatrix(int cols, int rows, int colCapacity, char* data, bool containNull):Matrix(cols,rows),FastCharVector(cols*rows,colCapacity*rows,data,containNull){setForm(DF_MATRIX);}
	virtual ~FastCharMatrix(){}
	virtual ConstantSP getValue() const;
	virtual INDEX columns() const {return cols_;}
	virtual INDEX rows() const {return rows_;}
	virtual bool reshape(INDEX cols, INDEX rows) {return Matrix::reshape(cols, rows);}
	virtual ConstantSP getRowLabel() const {return rowLabel_;}
	virtual void setRowLabel(const ConstantSP& label){Matrix::setRowLabel(label);}
	virtual ConstantSP getColumnLabel() const {return colLabel_;}
	virtual void setColumnLabel(const ConstantSP& label){Matrix::setColumnLabel(label);}
	virtual string getString(int column, int row) const {return FastCharVector::getString(column*rows_+row);}
	virtual string getString() const { return Matrix::getString();}
	virtual string getString(INDEX index) const {return Matrix::getString(index);}
	virtual bool set(INDEX column, INDEX row, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value){return Matrix::set(index,value);}
	virtual ConstantSP get(const ConstantSP& index) const {return Matrix::get(index);}
	virtual ConstantSP getItem(INDEX index) const {return getColumn(index);}
	virtual bool setItem(INDEX index, const ConstantSP& value){return setColumn(index,value);}
	virtual ConstantSP getRow(INDEX index) const;
	virtual ConstantSP getWindow(INDEX colStart, int colLength,INDEX rowStart, int rowLength) const;
	virtual ConstantSP  getColumn(INDEX index) const;
	virtual bool setColumn(INDEX index, const ConstantSP& value){fill(index*rows_,rows_,value);return true;}
	virtual ConstantSP getInstance(INDEX size) const { return Util::createMatrix(getType(),size,rows_,size,getExtraParamForType());}
	virtual ConstantSP getInstance() const { return Util::createMatrix(getType(),cols_,rows_,cols_,getExtraParamForType());}
	virtual ConstantSP get(INDEX column, INDEX row) const {return FastCharVector::get(column*rows_+row);}
};

class FastShortMatrix:public Matrix, public FastShortVector{
public:
	FastShortMatrix(int cols, int rows, int colCapacity, short* data, bool containNull):Matrix(cols,rows),FastShortVector(cols*rows,colCapacity*rows,data,containNull){setForm(DF_MATRIX);}
	virtual ~FastShortMatrix(){}
	virtual ConstantSP getValue() const;
	virtual INDEX columns() const {return cols_;}
	virtual INDEX rows() const {return rows_;}
	virtual bool reshape(INDEX cols, INDEX rows) {return Matrix::reshape(cols, rows);}
	virtual ConstantSP getRowLabel() const {return rowLabel_;}
	virtual void setRowLabel(const ConstantSP& label){Matrix::setRowLabel(label);}
	virtual ConstantSP getColumnLabel() const {return colLabel_;}
	virtual void setColumnLabel(const ConstantSP& label){Matrix::setColumnLabel(label);}
	virtual string getString(int column, int row) const {return FastShortVector::getString(column*rows_+row);}
	virtual string getString() const { return Matrix::getString();}
	virtual string getString(INDEX index) const {return Matrix::getString(index);}
	virtual bool set(INDEX column, INDEX row, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value){return Matrix::set(index,value);}
	virtual ConstantSP get(const ConstantSP& index) const {return Matrix::get(index);}
	virtual ConstantSP getItem(INDEX index) const {return getColumn(index);}
	virtual bool setItem(INDEX index, const ConstantSP& value){return setColumn(index,value);}
	virtual ConstantSP getRow(INDEX index) const;
	virtual ConstantSP getWindow(INDEX colStart, int colLength,INDEX rowStart, int rowLength) const;
	virtual ConstantSP  getColumn(INDEX index) const;
	virtual bool setColumn(INDEX index, const ConstantSP& value){fill(index*rows_,rows_,value);return true;}
	virtual ConstantSP getInstance(INDEX size) const { return Util::createMatrix(getType(),size,rows_,size,getExtraParamForType());}
	virtual ConstantSP getInstance() const { return Util::createMatrix(getType(),cols_,rows_,cols_,getExtraParamForType());}
	virtual ConstantSP get(INDEX column, INDEX row) const {return FastShortVector::get(column*rows_+row);}
};

class FastIntMatrix:public Matrix, public FastIntVector{
public:
	FastIntMatrix(int cols, int rows, int colCapacity, int* data, bool containNull):Matrix(cols,rows),FastIntVector(cols*rows,colCapacity*rows,data,containNull){setForm(DF_MATRIX);}
	virtual ~FastIntMatrix(){}
	virtual ConstantSP getValue() const;
	virtual INDEX columns() const {return cols_;}
	virtual INDEX rows() const {return rows_;}
	virtual bool reshape(INDEX cols, INDEX rows) {return Matrix::reshape(cols, rows);}
	virtual ConstantSP getRowLabel() const {return rowLabel_;}
	virtual void setRowLabel(const ConstantSP& label){Matrix::setRowLabel(label);}
	virtual ConstantSP getColumnLabel() const {return colLabel_;}
	virtual void setColumnLabel(const ConstantSP& label){Matrix::setColumnLabel(label);}
	virtual string getString(int column, int row) const {return FastIntVector::getString(column*rows_+row);}
	virtual string getString() const { return Matrix::getString();}
	virtual string getString(INDEX index) const {return Matrix::getString(index);}
	virtual bool set(INDEX column, INDEX row, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value){return Matrix::set(index,value);}
	virtual ConstantSP get(const ConstantSP& index) const {return Matrix::get(index);}
	virtual ConstantSP getItem(INDEX index) const {return getColumn(index);}
	virtual bool setItem(INDEX index, const ConstantSP& value){return setColumn(index,value);}
	virtual ConstantSP getRow(INDEX index) const;
	virtual ConstantSP getWindow(INDEX colStart, int colLength,INDEX rowStart, int rowLength) const;
	virtual ConstantSP  getColumn(INDEX index) const;
	virtual bool setColumn(INDEX index, const ConstantSP& value){fill(index*rows_,rows_,value);return true;}
	virtual ConstantSP getInstance(INDEX size) const { return Util::createMatrix(getType(),size,rows_,size,getExtraParamForType());}
	virtual ConstantSP getInstance() const { return Util::createMatrix(getType(),cols_,rows_,cols_,getExtraParamForType());}
	virtual ConstantSP get(INDEX column, INDEX row) const {return FastIntVector::get(column*rows_+row);}
};

class FastLongMatrix:public Matrix, public FastLongVector{
public:
	FastLongMatrix(int cols, int rows, int colCapacity, long long* data, bool containNull):Matrix(cols,rows),FastLongVector(cols*rows,colCapacity*rows,data,containNull){setForm(DF_MATRIX);}
	virtual ~FastLongMatrix(){}
	virtual ConstantSP getValue() const;
	virtual INDEX columns() const {return cols_;}
	virtual INDEX rows() const {return rows_;}
	virtual bool reshape(INDEX cols, INDEX rows) {return Matrix::reshape(cols, rows);}
	virtual ConstantSP getRowLabel() const {return rowLabel_;}
	virtual void setRowLabel(const ConstantSP& label){Matrix::setRowLabel(label);}
	virtual ConstantSP getColumnLabel() const {return colLabel_;}
	virtual void setColumnLabel(const ConstantSP& label){Matrix::setColumnLabel(label);}
	virtual string getString(int column, int row) const {return FastLongVector::getString(column*rows_+row);}
	virtual string getString() const { return Matrix::getString();}
	virtual string getString(INDEX index) const {return Matrix::getString(index);}
	virtual bool set(INDEX column, INDEX row, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value){return Matrix::set(index,value);}
	virtual ConstantSP get(const ConstantSP& index) const {return Matrix::get(index);}
	virtual ConstantSP getItem(INDEX index) const {return getColumn(index);}
	virtual bool setItem(INDEX index, const ConstantSP& value){return setColumn(index,value);}
	virtual ConstantSP getRow(INDEX index) const;
	virtual ConstantSP getWindow(INDEX colStart, int colLength,INDEX rowStart, int rowLength) const;
	virtual ConstantSP  getColumn(INDEX index) const;
	virtual bool setColumn(INDEX index, const ConstantSP& value){fill(index*rows_,rows_,value);return true;}
	virtual ConstantSP getInstance(INDEX size) const { return Util::createMatrix(getType(),size,rows_,size,getExtraParamForType());}
	virtual ConstantSP getInstance() const { return Util::createMatrix(getType(),cols_,rows_,cols_,getExtraParamForType());}
	virtual ConstantSP get(INDEX column, INDEX row) const {return FastLongVector::get(column*rows_+row);}
};

class FastFloatMatrix:public Matrix, public FastFloatVector{
public:
	FastFloatMatrix(int cols, int rows, int colCapacity, float* data, bool containNull):Matrix(cols,rows),FastFloatVector(cols*rows,colCapacity*rows,data,containNull){setForm(DF_MATRIX);}
	virtual ~FastFloatMatrix(){}
	virtual ConstantSP getValue() const;
	virtual INDEX columns() const {return cols_;}
	virtual INDEX rows() const {return rows_;}
	virtual bool reshape(INDEX cols, INDEX rows) {return Matrix::reshape(cols, rows);}
	virtual ConstantSP getRowLabel() const {return rowLabel_;}
	virtual void setRowLabel(const ConstantSP& label){Matrix::setRowLabel(label);}
	virtual ConstantSP getColumnLabel() const {return colLabel_;}
	virtual void setColumnLabel(const ConstantSP& label){Matrix::setColumnLabel(label);}
	virtual string getString(int column, int row) const {return FastFloatVector::getString(column*rows_+row);}
	virtual string getString() const { return Matrix::getString();}
	virtual string getString(INDEX index) const {return Matrix::getString(index);}
	virtual bool set(INDEX column, INDEX row, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value){return Matrix::set(index,value);}
	virtual ConstantSP get(const ConstantSP& index) const {return Matrix::get(index);}
	virtual ConstantSP getItem(INDEX index) const {return getColumn(index);}
	virtual bool setItem(INDEX index, const ConstantSP& value){return setColumn(index,value);}
	virtual ConstantSP getRow(INDEX index) const;
	virtual ConstantSP getWindow(INDEX colStart, int colLength,INDEX rowStart, int rowLength) const;
	virtual ConstantSP  getColumn(INDEX index) const;
	virtual bool setColumn(INDEX index, const ConstantSP& value){fill(index*rows_,rows_,value);return true;}
	virtual ConstantSP getInstance(INDEX size) const { return Util::createMatrix(getType(),size,rows_,size,getExtraParamForType());}
	virtual ConstantSP getInstance() const { return Util::createMatrix(getType(),cols_,rows_,cols_,getExtraParamForType());}
	virtual ConstantSP get(INDEX column, INDEX row) const {return FastFloatVector::get(column*rows_+row);}
};

class FastDoubleMatrix:public Matrix, public FastDoubleVector{
public:
	FastDoubleMatrix(int cols, int rows, int colCapacity, double* data, bool containNull):Matrix(cols,rows),FastDoubleVector(cols*rows,colCapacity*rows,data,containNull){setForm(DF_MATRIX);}
	virtual ~FastDoubleMatrix(){}
	virtual ConstantSP getValue() const;
	virtual INDEX columns() const {return cols_;}
	virtual INDEX rows() const {return rows_;}
	virtual bool reshape(INDEX cols, INDEX rows) {return Matrix::reshape(cols, rows);}
	virtual ConstantSP getRowLabel() const {return rowLabel_;}
	virtual void setRowLabel(const ConstantSP& label){Matrix::setRowLabel(label);}
	virtual ConstantSP getColumnLabel() const {return colLabel_;}
	virtual void setColumnLabel(const ConstantSP& label){Matrix::setColumnLabel(label);}
	virtual string getString(int column, int row) const {return FastDoubleVector::getString(column*rows_+row);}
	virtual string getString() const { return Matrix::getString();}
	virtual string getString(INDEX index) const {return Matrix::getString(index);}
	virtual bool set(INDEX column, INDEX row, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value){return Matrix::set(index,value);}
	virtual ConstantSP get(const ConstantSP& index) const {return Matrix::get(index);}
	virtual ConstantSP getItem(INDEX index) const {return getColumn(index);}
	virtual bool setItem(INDEX index, const ConstantSP& value){return setColumn(index,value);}
	virtual ConstantSP getRow(INDEX index) const;
	virtual ConstantSP getWindow(INDEX colStart, int colLength,INDEX rowStart, int rowLength) const;
	virtual ConstantSP  getColumn(INDEX index) const;
	virtual bool setColumn(INDEX index, const ConstantSP& value){fill(index*rows_,rows_,value);return true;}
	virtual ConstantSP getInstance(INDEX size) const { return Util::createMatrix(getType(),size,rows_,size,getExtraParamForType());}
	virtual ConstantSP getInstance() const { return Util::createMatrix(getType(),cols_,rows_,cols_,getExtraParamForType());}
	virtual ConstantSP get(INDEX column, INDEX row) const {return FastDoubleVector::get(column*rows_+row);}
};

class FastDateMatrix:public Matrix, public FastDateVector{
public:
	FastDateMatrix(int cols, int rows, int colCapacity, int* data, bool containNull):Matrix(cols,rows),FastDateVector(cols*rows,colCapacity*rows,data,containNull){setForm(DF_MATRIX);}
	virtual ~FastDateMatrix(){}
	virtual ConstantSP getValue() const;
	virtual INDEX columns() const {return cols_;}
	virtual INDEX rows() const {return rows_;}
	virtual bool reshape(INDEX cols, INDEX rows) {return Matrix::reshape(cols, rows);}
	virtual ConstantSP getRowLabel() const {return rowLabel_;}
	virtual void setRowLabel(const ConstantSP& label){Matrix::setRowLabel(label);}
	virtual ConstantSP getColumnLabel() const {return colLabel_;}
	virtual void setColumnLabel(const ConstantSP& label){Matrix::setColumnLabel(label);}
	virtual string getString(int column, int row) const {
		return FastDateVector::getString(column*rows_+row);
	}
	virtual string getString() const { return Matrix::getString();}
	virtual string getString(INDEX index) const {return Matrix::getString(index);}
	virtual bool set(INDEX column, INDEX row, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value){return Matrix::set(index,value);}
	virtual ConstantSP get(const ConstantSP& index) const {return Matrix::get(index);}
	virtual ConstantSP getItem(INDEX index) const {return getColumn(index);}
	virtual bool setItem(INDEX index, const ConstantSP& value){return setColumn(index,value);}
	virtual ConstantSP getRow(INDEX index) const;
	virtual ConstantSP getWindow(INDEX colStart, int colLength,INDEX rowStart, int rowLength) const;
	virtual ConstantSP  getColumn(INDEX index) const;
	virtual bool setColumn(INDEX index, const ConstantSP& value){fill(index*rows_,rows_,value);return true;}
	virtual ConstantSP getInstance(INDEX size) const { return Util::createMatrix(getType(),size,rows_,size,getExtraParamForType());}
	virtual ConstantSP getInstance() const { return Util::createMatrix(getType(),cols_,rows_,cols_,getExtraParamForType());}
	virtual ConstantSP get(INDEX column, INDEX row) const {return FastDateVector::get(column*rows_+row);}
};

class FastDateTimeMatrix:public Matrix, public FastDateTimeVector{
public:
	FastDateTimeMatrix(int cols, int rows, int colCapacity, int* data, bool containNull):Matrix(cols,rows),FastDateTimeVector(cols*rows,colCapacity*rows,data,containNull){setForm(DF_MATRIX);}
	virtual ~FastDateTimeMatrix(){}
	virtual ConstantSP getValue() const;
	virtual INDEX columns() const {return cols_;}
	virtual INDEX rows() const {return rows_;}
	virtual bool reshape(INDEX cols, INDEX rows) {return Matrix::reshape(cols, rows);}
	virtual ConstantSP getRowLabel() const {return rowLabel_;}
	virtual void setRowLabel(const ConstantSP& label){Matrix::setRowLabel(label);}
	virtual ConstantSP getColumnLabel() const {return colLabel_;}
	virtual void setColumnLabel(const ConstantSP& label){Matrix::setColumnLabel(label);}
	virtual string getString(int column, int row) const {
		return FastDateTimeVector::getString(column*rows_+row);
	}
	virtual string getString() const { return Matrix::getString();}
	virtual string getString(INDEX index) const {return Matrix::getString(index);}
	virtual bool set(INDEX column, INDEX row, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value){return Matrix::set(index,value);}
	virtual ConstantSP get(const ConstantSP& index) const {return Matrix::get(index);}
	virtual ConstantSP getItem(INDEX index) const {return getColumn(index);}
	virtual bool setItem(INDEX index, const ConstantSP& value){return setColumn(index,value);}
	virtual ConstantSP getRow(INDEX index) const;
	virtual ConstantSP getWindow(INDEX colStart, int colLength,INDEX rowStart, int rowLength) const;
	virtual ConstantSP  getColumn(INDEX index) const;
	virtual bool setColumn(INDEX index, const ConstantSP& value){fill(index*rows_,rows_,value);return true;}
	virtual ConstantSP getInstance(INDEX size) const { return Util::createMatrix(getType(),size,rows_,size,getExtraParamForType());}
	virtual ConstantSP getInstance() const { return Util::createMatrix(getType(),cols_,rows_,cols_,getExtraParamForType());}
	virtual ConstantSP get(INDEX column, INDEX row) const {return FastDateTimeVector::get(column*rows_+row);}
};

class FastDateHourMatrix:public Matrix, public FastDateHourVector{
public:
    FastDateHourMatrix(int cols, int rows, int colCapacity, int* data, bool containNull):Matrix(cols,rows),FastDateHourVector(cols*rows,colCapacity*rows,data,containNull){setForm(DF_MATRIX);}
    virtual ~FastDateHourMatrix(){}
    virtual ConstantSP getValue() const;
    virtual INDEX columns() const {return cols_;}
    virtual INDEX rows() const {return rows_;}
    virtual bool reshape(INDEX cols, INDEX rows) {return Matrix::reshape(cols, rows);}
    virtual ConstantSP getRowLabel() const {return rowLabel_;}
    virtual void setRowLabel(const ConstantSP& label){Matrix::setRowLabel(label);}
    virtual ConstantSP getColumnLabel() const {return colLabel_;}
    virtual void setColumnLabel(const ConstantSP& label){Matrix::setColumnLabel(label);}
    virtual string getString(int column, int row) const {
        return FastDateHourVector::getString(column*rows_+row);
    }
    virtual string getString() const { return Matrix::getString();}
    virtual string getString(INDEX index) const {return Matrix::getString(index);}
    virtual bool set(INDEX column, INDEX row, const ConstantSP& value);
    virtual bool set(const ConstantSP& index, const ConstantSP& value){return Matrix::set(index,value);}
    virtual ConstantSP get(const ConstantSP& index) const {return Matrix::get(index);}
    virtual ConstantSP getItem(INDEX index) const {return getColumn(index);}
    virtual bool setItem(INDEX index, const ConstantSP& value){return setColumn(index,value);}
    virtual ConstantSP getRow(INDEX index) const;
    virtual ConstantSP getWindow(INDEX colStart, int colLength,INDEX rowStart, int rowLength) const;
    virtual ConstantSP  getColumn(INDEX index) const;
    virtual bool setColumn(INDEX index, const ConstantSP& value){fill(index*rows_,rows_,value);return true;}
    virtual ConstantSP getInstance(INDEX size) const { return Util::createMatrix(getType(),size,rows_,size,getExtraParamForType());}
    virtual ConstantSP getInstance() const { return Util::createMatrix(getType(),cols_,rows_,cols_,getExtraParamForType());}
    virtual ConstantSP get(INDEX column, INDEX row) const {return FastDateHourVector::get(column*rows_+row);}
};

class FastMonthMatrix:public Matrix, public FastMonthVector{
public:
	FastMonthMatrix(int cols, int rows, int colCapacity, int* data, bool containNull):Matrix(cols,rows),FastMonthVector(cols*rows,colCapacity*rows,data,containNull){setForm(DF_MATRIX);}
	virtual ~FastMonthMatrix(){}
	virtual ConstantSP getValue() const;
	virtual INDEX columns() const {return cols_;}
	virtual INDEX rows() const {return rows_;}
	virtual bool reshape(INDEX cols, INDEX rows) {return Matrix::reshape(cols, rows);}
	virtual ConstantSP getRowLabel() const {return rowLabel_;}
	virtual void setRowLabel(const ConstantSP& label){Matrix::setRowLabel(label);}
	virtual ConstantSP getColumnLabel() const {return colLabel_;}
	virtual void setColumnLabel(const ConstantSP& label){Matrix::setColumnLabel(label);}
	virtual string getString(int column, int row) const {
		return FastMonthVector::getString(column*rows_+row);
	}
	virtual string getString() const { return Matrix::getString();}
	virtual string getString(INDEX index) const {return Matrix::getString(index);}
	virtual bool set(INDEX column, INDEX row, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value){return Matrix::set(index,value);}
	virtual ConstantSP get(const ConstantSP& index) const {return Matrix::get(index);}
	virtual ConstantSP getItem(INDEX index) const {return getColumn(index);}
	virtual bool setItem(INDEX index, const ConstantSP& value){return setColumn(index,value);}
	virtual ConstantSP getRow(INDEX index) const;
	virtual ConstantSP getWindow(INDEX colStart, int colLength,INDEX rowStart, int rowLength) const;
	virtual ConstantSP  getColumn(INDEX index) const;
	virtual bool setColumn(INDEX index, const ConstantSP& value){fill(index*rows_,rows_,value);return true;}
	virtual ConstantSP getInstance(INDEX size) const { return Util::createMatrix(getType(),size,rows_,size,getExtraParamForType());}
	virtual ConstantSP getInstance() const { return Util::createMatrix(getType(),cols_,rows_,cols_,getExtraParamForType());}
	virtual ConstantSP get(INDEX column, INDEX row) const {return FastMonthVector::get(column*rows_+row);}
};

class FastTimeMatrix:public Matrix, public FastTimeVector{
public:
	FastTimeMatrix(int cols, int rows, int colCapacity, int* data, bool containNull):Matrix(cols,rows),FastTimeVector(cols*rows,colCapacity*rows,data,containNull){setForm(DF_MATRIX);}
	virtual ~FastTimeMatrix(){}
	virtual ConstantSP getValue() const;
	virtual INDEX columns() const {return cols_;}
	virtual INDEX rows() const {return rows_;}
	virtual bool reshape(INDEX cols, INDEX rows) {return Matrix::reshape(cols, rows);}
	virtual ConstantSP getRowLabel() const {return rowLabel_;}
	virtual void setRowLabel(const ConstantSP& label){Matrix::setRowLabel(label);}
	virtual ConstantSP getColumnLabel() const {return colLabel_;}
	virtual void setColumnLabel(const ConstantSP& label){Matrix::setColumnLabel(label);}
	virtual string getString(int column, int row) const {
		return FastTimeVector::getString(column*rows_+row);
	}
	virtual string getString() const { return Matrix::getString();}
	virtual string getString(INDEX index) const {return Matrix::getString(index);}
	virtual bool set(INDEX column, INDEX row, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value){return Matrix::set(index,value);}
	virtual ConstantSP get(const ConstantSP& index) const {return Matrix::get(index);}
	virtual ConstantSP getItem(INDEX index) const {return getColumn(index);}
	virtual bool setItem(INDEX index, const ConstantSP& value){return setColumn(index,value);}
	virtual ConstantSP getRow(INDEX index) const;
	virtual ConstantSP getWindow(INDEX colStart, int colLength,INDEX rowStart, int rowLength) const;
	virtual ConstantSP  getColumn(INDEX index) const;
	virtual bool setColumn(INDEX index, const ConstantSP& value){fill(index*rows_,rows_,value);return true;}
	virtual ConstantSP getInstance(INDEX size) const { return Util::createMatrix(getType(),size,rows_,size,getExtraParamForType());}
	virtual ConstantSP getInstance() const { return Util::createMatrix(getType(),cols_,rows_,cols_,getExtraParamForType());}
	virtual ConstantSP get(INDEX column, INDEX row) const {return FastTimeVector::get(column*rows_+row);}
};

class FastSecondMatrix:public Matrix, public FastSecondVector{
public:
	FastSecondMatrix(int cols, int rows, int colCapacity, int* data, bool containNull):Matrix(cols,rows),FastSecondVector(cols*rows,colCapacity*rows,data,containNull){setForm(DF_MATRIX);}
	virtual ~FastSecondMatrix(){}
	virtual ConstantSP getValue() const;
	virtual INDEX columns() const {return cols_;}
	virtual INDEX rows() const {return rows_;}
	virtual bool reshape(INDEX cols, INDEX rows) {return Matrix::reshape(cols, rows);}
	virtual ConstantSP getRowLabel() const {return rowLabel_;}
	virtual void setRowLabel(const ConstantSP& label){Matrix::setRowLabel(label);}
	virtual ConstantSP getColumnLabel() const {return colLabel_;}
	virtual void setColumnLabel(const ConstantSP& label){Matrix::setColumnLabel(label);}
	virtual string getString(int column, int row) const {
		return FastSecondVector::getString(column*rows_+row);
	}
	virtual string getString() const { return Matrix::getString();}
	virtual string getString(INDEX index) const {return Matrix::getString(index);}
	virtual bool set(INDEX column, INDEX row, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value){return Matrix::set(index,value);}
	virtual ConstantSP get(const ConstantSP& index) const {return Matrix::get(index);}
	virtual ConstantSP getItem(INDEX index) const {return getColumn(index);}
	virtual bool setItem(INDEX index, const ConstantSP& value){return setColumn(index,value);}
	virtual ConstantSP getRow(INDEX index) const;
	virtual ConstantSP getWindow(INDEX colStart, int colLength,INDEX rowStart, int rowLength) const;
	virtual ConstantSP  getColumn(INDEX index) const;
	virtual bool setColumn(INDEX index, const ConstantSP& value){fill(index*rows_,rows_,value);return true;}
	virtual ConstantSP getInstance(INDEX size) const { return Util::createMatrix(getType(),size,rows_,size,getExtraParamForType());}
	virtual ConstantSP getInstance() const { return Util::createMatrix(getType(),cols_,rows_,cols_,getExtraParamForType());}
	virtual ConstantSP get(INDEX column, INDEX row) const {return FastSecondVector::get(column*rows_+row);}
};

class FastMinuteMatrix:public Matrix, public FastMinuteVector{
public:
	FastMinuteMatrix(int cols, int rows, int colCapacity, int* data, bool containNull):Matrix(cols,rows),FastMinuteVector(cols*rows,colCapacity*rows,data,containNull){setForm(DF_MATRIX);}
	virtual ~FastMinuteMatrix(){}
	virtual ConstantSP getValue() const;
	virtual INDEX columns() const {return cols_;}
	virtual INDEX rows() const {return rows_;}
	virtual bool reshape(INDEX cols, INDEX rows) {return Matrix::reshape(cols, rows);}
	virtual ConstantSP getRowLabel() const {return rowLabel_;}
	virtual void setRowLabel(const ConstantSP& label){Matrix::setRowLabel(label);}
	virtual ConstantSP getColumnLabel() const {return colLabel_;}
	virtual void setColumnLabel(const ConstantSP& label){Matrix::setColumnLabel(label);}
	virtual string getString(int column, int row) const {
		return FastMinuteVector::getString(column*rows_+row);
	}
	virtual string getString() const { return Matrix::getString();}
	virtual string getString(INDEX index) const {return Matrix::getString(index);}
	virtual bool set(INDEX column, INDEX row, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value){return Matrix::set(index,value);}
	virtual ConstantSP get(const ConstantSP& index) const {return Matrix::get(index);}
	virtual ConstantSP getItem(INDEX index) const {return getColumn(index);}
	virtual bool setItem(INDEX index, const ConstantSP& value){return setColumn(index,value);}
	virtual ConstantSP getRow(INDEX index) const;
	virtual ConstantSP getWindow(INDEX colStart, int colLength,INDEX rowStart, int rowLength) const;
	virtual ConstantSP  getColumn(INDEX index) const;
	virtual bool setColumn(INDEX index, const ConstantSP& value){fill(index*rows_,rows_,value);return true;}
	virtual ConstantSP getInstance(INDEX size) const { return Util::createMatrix(getType(),size,rows_,size,getExtraParamForType());}
	virtual ConstantSP getInstance() const { return Util::createMatrix(getType(),cols_,rows_,cols_,getExtraParamForType());}
	virtual ConstantSP get(INDEX column, INDEX row) const {return FastMinuteVector::get(column*rows_+row);}
};

class FastNanoTimeMatrix:public Matrix, public FastNanoTimeVector{
public:
	FastNanoTimeMatrix(int cols, int rows, int colCapacity, long long* data, bool containNull):Matrix(cols,rows),FastNanoTimeVector(cols*rows,colCapacity*rows,data,containNull){setForm(DF_MATRIX);}
	virtual ~FastNanoTimeMatrix(){}
	virtual ConstantSP getValue() const;
	virtual INDEX columns() const {return cols_;}
	virtual INDEX rows() const {return rows_;}
	virtual bool reshape(INDEX cols, INDEX rows) {return Matrix::reshape(cols, rows);}
	virtual ConstantSP getRowLabel() const {return rowLabel_;}
	virtual void setRowLabel(const ConstantSP& label){Matrix::setRowLabel(label);}
	virtual ConstantSP getColumnLabel() const {return colLabel_;}
	virtual void setColumnLabel(const ConstantSP& label){Matrix::setColumnLabel(label);}
	virtual string getString(int column, int row) const {
		return FastNanoTimeVector::getString(column*rows_+row);
	}
	virtual string getString() const { return Matrix::getString();}
	virtual string getString(INDEX index) const {return Matrix::getString(index);}
	virtual bool set(INDEX column, INDEX row, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value){return Matrix::set(index,value);}
	virtual ConstantSP get(const ConstantSP& index) const {return Matrix::get(index);}
	virtual ConstantSP getItem(INDEX index) const {return getColumn(index);}
	virtual bool setItem(INDEX index, const ConstantSP& value){return setColumn(index,value);}
	virtual ConstantSP getRow(INDEX index) const;
	virtual ConstantSP getWindow(INDEX colStart, int colLength,INDEX rowStart, int rowLength) const;
	virtual ConstantSP  getColumn(INDEX index) const;
	virtual bool setColumn(INDEX index, const ConstantSP& value){fill(index*rows_,rows_,value);return true;}
	virtual ConstantSP getInstance(INDEX size) const { return Util::createMatrix(getType(),size,rows_,size,getExtraParamForType());}
	virtual ConstantSP getInstance() const { return Util::createMatrix(getType(),cols_,rows_,cols_,getExtraParamForType());}
	virtual ConstantSP get(INDEX column, INDEX row) const {return FastNanoTimeVector::get(column*rows_+row);}
};

class FastTimestampMatrix:public Matrix, public FastTimestampVector{
public:
	FastTimestampMatrix(int cols, int rows, int colCapacity, long long* data, bool containNull):Matrix(cols,rows),FastTimestampVector(cols*rows,colCapacity*rows,data,containNull){setForm(DF_MATRIX);}
	virtual ~FastTimestampMatrix(){}
	virtual ConstantSP getValue() const;
	virtual INDEX columns() const {return cols_;}
	virtual INDEX rows() const {return rows_;}
	virtual bool reshape(INDEX cols, INDEX rows) {return Matrix::reshape(cols, rows);}
	virtual ConstantSP getRowLabel() const {return rowLabel_;}
	virtual void setRowLabel(const ConstantSP& label){Matrix::setRowLabel(label);}
	virtual ConstantSP getColumnLabel() const {return colLabel_;}
	virtual void setColumnLabel(const ConstantSP& label){Matrix::setColumnLabel(label);}
	virtual string getString(int column, int row) const {
		return FastTimestampVector::getString(column*rows_+row);
	}
	virtual string getString() const { return Matrix::getString();}
	virtual string getString(INDEX index) const {return Matrix::getString(index);}
	virtual bool set(INDEX column, INDEX row, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value){return Matrix::set(index,value);}
	virtual ConstantSP get(const ConstantSP& index) const {return Matrix::get(index);}
	virtual ConstantSP getItem(INDEX index) const {return getColumn(index);}
	virtual bool setItem(INDEX index, const ConstantSP& value){return setColumn(index,value);}
	virtual ConstantSP getRow(INDEX index) const;
	virtual ConstantSP getWindow(INDEX colStart, int colLength,INDEX rowStart, int rowLength) const;
	virtual ConstantSP  getColumn(INDEX index) const;
	virtual bool setColumn(INDEX index, const ConstantSP& value){fill(index*rows_,rows_,value);return true;}
	virtual ConstantSP getInstance(INDEX size) const { return Util::createMatrix(getType(),size,rows_,size,getExtraParamForType());}
	virtual ConstantSP getInstance() const { return Util::createMatrix(getType(),cols_,rows_,cols_,getExtraParamForType());}
	virtual ConstantSP get(INDEX column, INDEX row) const {return FastTimestampVector::get(column*rows_+row);}
};

class FastNanoTimestampMatrix:public Matrix, public FastNanoTimestampVector{
public:
	FastNanoTimestampMatrix(int cols, int rows, int colCapacity, long long* data, bool containNull):Matrix(cols,rows),FastNanoTimestampVector(cols*rows,colCapacity*rows,data,containNull){setForm(DF_MATRIX);}
	virtual ~FastNanoTimestampMatrix(){}
	virtual ConstantSP getValue() const;
	virtual INDEX columns() const {return cols_;}
	virtual INDEX rows() const {return rows_;}
	virtual bool reshape(INDEX cols, INDEX rows) {return Matrix::reshape(cols, rows);}
	virtual ConstantSP getRowLabel() const {return rowLabel_;}
	virtual void setRowLabel(const ConstantSP& label){Matrix::setRowLabel(label);}
	virtual ConstantSP getColumnLabel() const {return colLabel_;}
	virtual void setColumnLabel(const ConstantSP& label){Matrix::setColumnLabel(label);}
	virtual string getString(int column, int row) const {
		return FastNanoTimestampVector::getString(column*rows_+row);
	}
	virtual string getString() const { return Matrix::getString();}
	virtual string getString(INDEX index) const {return Matrix::getString(index);}
	virtual bool set(INDEX column, INDEX row, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value){return Matrix::set(index,value);}
	virtual ConstantSP get(const ConstantSP& index) const {return Matrix::get(index);}
	virtual ConstantSP getItem(INDEX index) const {return getColumn(index);}
	virtual bool setItem(INDEX index, const ConstantSP& value){return setColumn(index,value);}
	virtual ConstantSP getRow(INDEX index) const;
	virtual ConstantSP getWindow(INDEX colStart, int colLength,INDEX rowStart, int rowLength) const;
	virtual ConstantSP  getColumn(INDEX index) const;
	virtual bool setColumn(INDEX index, const ConstantSP& value){fill(index*rows_,rows_,value);return true;}
	virtual ConstantSP getInstance(INDEX size) const { return Util::createMatrix(getType(),size,rows_,size,getExtraParamForType());}
	virtual ConstantSP getInstance() const { return Util::createMatrix(getType(),cols_,rows_,cols_,getExtraParamForType());}
	virtual ConstantSP get(INDEX column, INDEX row) const {return FastNanoTimestampVector::get(column*rows_+row);}
};

class AbstractStringVector : public Vector{
public:
	virtual bool getNullFlag() const {return containNull_;}
	virtual void setNullFlag(bool containNull){containNull_=containNull;}
	virtual char getBool() const {throw IncompatibleTypeException(DT_BOOL,DT_STRING);}
	virtual char getChar() const {throw IncompatibleTypeException(DT_CHAR,DT_STRING);}
	virtual short getShort() const {throw IncompatibleTypeException(DT_SHORT,DT_STRING);}
	virtual int getInt() const {throw IncompatibleTypeException(DT_INT,DT_STRING);}
	virtual long long getLong() const {throw IncompatibleTypeException(DT_LONG,DT_STRING);}
	virtual INDEX getIndex() const {throw IncompatibleTypeException(DT_INDEX,DT_STRING);}
	virtual float getFloat() const {throw IncompatibleTypeException(DT_FLOAT,DT_STRING);}
	virtual double getDouble() const {throw IncompatibleTypeException(DT_DOUBLE,DT_STRING);}
	virtual void neg(){throw IncompatibleTypeException(DT_DOUBLE,DT_STRING);}

protected:
	bool containNull_;
};

class StringVector: public AbstractStringVector{
public:
	StringVector(INDEX size, INDEX capacity, bool blob = false);
	StringVector(const vector<string>& data, INDEX capacity, bool containNull, bool blob = false);
    virtual DATA_TYPE getType() const {return blob_ ? DT_BLOB: DT_STRING;}
    virtual DATA_TYPE getRawType() const { return blob_ ? DT_BLOB: DT_STRING;}
    virtual DATA_CATEGORY getCategory() const {return blob_ ? BINARY : LITERAL;}

	virtual ~StringVector(){}
	virtual IO_ERR deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement);
	virtual INDEX getCapacity() const {return data_.capacity();}
	virtual	INDEX reserve(INDEX capacity);
	virtual bool isFastMode() const {return false;}
	virtual short getUnitLength() const {return 0;}
	virtual void clear(){
		data_.clear();
		containNull_ = false;
	}
	virtual bool sizeable() const {return true;}
	virtual int compare(INDEX index, const ConstantSP& target) const {return data_[index].compare(target->getString());}
	virtual string getString(INDEX index) const {return data_[index];}
	virtual const string& getStringRef() const {return data_[0];}
	virtual const string& getStringRef(INDEX index) const { return data_[index];}
	virtual bool set(INDEX index, const ConstantSP& value){
		data_[index]=value->getString();
		if(data_[index].empty())
			containNull_ = true;
		return true;
	}
	virtual bool set(const ConstantSP& index, const ConstantSP& value);
	virtual bool assign(const ConstantSP& value);
	virtual ConstantSP get(INDEX index) const {return ConstantSP(new String(data_[index], blob_));}
	virtual ConstantSP get(const ConstantSP& index) const;
	virtual bool isNull(INDEX index) const {return data_[index].size()==0;}
	virtual bool isNull() const {return false;}
	virtual void setNull(INDEX index){data_[index]="";}
	virtual void setNull(){}
	virtual bool hasNull(){return hasNullInRange(0, data_.size());}
	virtual bool hasNull(INDEX start, INDEX length){return hasNullInRange(start, start + length);}
	bool hasNullInRange(INDEX start, INDEX end);
	virtual void fill(INDEX start, INDEX length, const ConstantSP& value);
	virtual void nullFill(const ConstantSP& val);
	virtual bool isNull(INDEX start, int len, char* buf) const;
	virtual ConstantSP getSubVector(INDEX start, INDEX length) const { return getSubVector(start, length, std::abs(length));}
	virtual ConstantSP getSubVector(INDEX start, INDEX length, INDEX capacity) const;
	virtual ConstantSP getInstance(INDEX size) const {return ConstantSP(new StringVector(size, size, blob_));}
	virtual ConstantSP getValue() const;
	virtual ConstantSP getValue(INDEX capacity) const {return ConstantSP(new StringVector(data_, capacity, containNull_, blob_));}
	virtual bool append(const ConstantSP& value, INDEX appendSize);
	virtual bool appendString(string* buf, int len);
	virtual bool appendString(char** buf, int len);
	virtual bool remove(INDEX count);
	virtual bool remove(const ConstantSP& index);
	virtual void prev(INDEX steps);
	virtual void next(INDEX steps);
	virtual int serialize(char* buf, int bufSize, INDEX indexStart, int offset, int& numElement, int& partial) const;
	virtual bool getString(INDEX start, int len, string** buf) const;
	virtual bool getString(INDEX start, int len, char** buf) const;
	virtual string** getStringConst(INDEX start, int len, string** buf) const;
	virtual char** getStringConst(INDEX start, int len, char** buf) const;
	virtual void setString(const string& val){data_[0]=val;}
	virtual void setString(INDEX index, const string& val){data_[index]=val;}
	virtual bool setString(INDEX start, int len, const string* buf){
		copy(buf,buf+len,data_.begin()+start);
		return true;
	}
	virtual bool setString(INDEX start, int len, char** buf){
		copy(buf,buf+len,data_.begin()+start);
		return true;
	}
	bool has(const string& val) const {	return std::find(data_.begin(),data_.end(),val)!=data_.end();}
	INDEX search(const string& val);
	virtual INDEX size() const {return data_.size();}
	string getMember(const int& index) const { return data_[index];}
	void setMember(const int& index, const string& val){data_[index]=val;}
	virtual void upper();
	virtual void lower();
	virtual void trim();
	virtual void strip();
	virtual void reverse(){std::reverse(data_.begin(),data_.end());}
	virtual void reverse(INDEX start, INDEX end){std::reverse(data_.begin()+start,data_.begin()+end+1);}
	virtual void replace(const ConstantSP& oldVal, const ConstantSP& newVal);
	virtual long long getAllocatedMemory() const;
	virtual long long getAllocatedMemory(INDEX size) const;
	virtual bool getHash(INDEX start, int len, int buckets, int* buf) const {
		for(int i=0; i<len; ++i)
			buf[i] = murmur32(data_[start + i].data(), data_[start + i].size()) % buckets;
		return true;
	}
	virtual void* getDataArray() const {return (void*)&data_[0];}

	virtual int asof(const ConstantSP& value) const{
		const string& target = value->getStringRef();
		int start = 0;
		int end = size() - 1;
		int mid;
		while(start <= end){
			mid = (start + end) / 2;
			if(data_[mid].compare(target) <= 0){
				start = mid + 1;
			}
			else {
				end = mid - 1;
			}
		}
		return end;
	}

private:
	mutable vector<string> data_;
    bool blob_;
};

class FastFixedLengthVector : public Vector {
public:
	FastFixedLengthVector(DATA_TYPE type, int fixedLength, int size, int capacity, unsigned char* srcData, bool containNull);
	virtual ~FastFixedLengthVector();
	virtual DATA_CATEGORY getCategory() const { return BINARY;}
	virtual int getExtraParamForType() const {return fixedLength_;}
	virtual DATA_TYPE getType() const { return type_;}
	virtual INDEX getCapacity() const {return capacity_;}
	virtual INDEX size() const {return size_;}
	virtual	INDEX reserve(INDEX capacity);
	virtual bool isFastMode() const {return true;}
	virtual short getUnitLength() const {return fixedLength_;}
	virtual void clear();
	virtual void initialize();
	virtual bool sizeable() const {return true;}
	virtual bool set(INDEX index, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value);
	virtual bool assign(const ConstantSP& value);
	virtual void setNull(INDEX index);
	virtual bool isNull() const {return false;}
	virtual void setNull(){}
	virtual bool hasNull(){return hasNullInRange(data_, 0, size_);}
	virtual bool hasNull(INDEX start, INDEX length){return hasNullInRange(data_, start, start + length);}
	virtual void fill(INDEX start, INDEX length, const ConstantSP& value);
	virtual ConstantSP getSubVector(INDEX start, INDEX length) const { return getSubVector(start, length, std::abs(length));}
	virtual ConstantSP getSubVector(INDEX start, INDEX length, INDEX capacity) const;
	virtual ConstantSP getInstance(INDEX size) const;
	virtual ConstantSP getValue() const;
	virtual ConstantSP getValue(INDEX capacity) const;
	virtual bool append(const ConstantSP& value, INDEX appendSize);
	virtual bool remove(INDEX count);
	virtual bool remove(const ConstantSP& index);
	virtual void prev(INDEX steps);
	virtual void next(INDEX steps);
	virtual long long getAllocatedMemory() const;
	virtual IO_ERR deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement);
	virtual int serialize(char* buf, int bufSize, INDEX indexStart, int offset, int& numElement, int& partial) const;
	virtual void reverse(){reverse(0, size_);}
	virtual void reverse(INDEX start, INDEX length);
	virtual bool getNullFlag() const {return containNull_;}
	virtual void setNullFlag(bool containNull){containNull_=containNull;}
	virtual char getBool() const {throw IncompatibleTypeException(DT_BOOL,type_);}
	virtual char getChar() const {throw IncompatibleTypeException(DT_CHAR,type_);}
	virtual short getShort() const {throw IncompatibleTypeException(DT_SHORT,type_);}
	virtual int getInt() const {throw IncompatibleTypeException(DT_INT,type_);}
	virtual long long getLong() const {throw IncompatibleTypeException(DT_LONG,type_);}
	virtual INDEX getIndex() const {throw IncompatibleTypeException(DT_INDEX,type_);}
	virtual float getFloat() const {throw IncompatibleTypeException(DT_FLOAT,type_);}
	virtual double getDouble() const {throw IncompatibleTypeException(DT_DOUBLE,type_);}
	virtual bool getBinary(INDEX start, int len, int unitLenght, unsigned char* buf) const;
	virtual const unsigned char* getBinaryConst(INDEX start, int len, int unitLength, unsigned char* buf) const;
	virtual unsigned char* getBinaryBuffer(INDEX start, int len, int unitLength, unsigned char* buf) const;
	virtual void* getDataBuffer(INDEX start, int len, void* buf) const;
	virtual void setBinary(INDEX index, int unitLength, const unsigned char* val);
	virtual bool setBinary(INDEX start, int len, int unitLength, const unsigned char* buf);
	virtual bool setData(INDEX start, int len, void* buf);
	virtual void neg(){throw IncompatibleTypeException(DT_DOUBLE,type_);}

protected:
	bool checkCapacity(int appendSize);
	ConstantSP retrieve(Vector* index) const;
	unsigned char* getDataArray(const Vector* indexVector, bool& hasNull) const;
	unsigned char** getSegmentDataArray(const Vector* indexVector, bool& hasNull) const;
	unsigned char* getDataArray(INDEX start, INDEX length) const;
	unsigned char* getDataArray(INDEX start, INDEX length, INDEX capacity) const;
	unsigned char** getSegmentDataArray(INDEX start, INDEX length) const;
	unsigned char** getSegmentDataArray(INDEX start, INDEX length, INDEX capacity) const;
	void getDataArray(INDEX start, INDEX length, unsigned char* buf) const;
	int getSegmentCount(INDEX size, int segmentSizeInBit) const;
	//start is inclusive, but end is exclusive
	virtual bool hasNullInRange(const unsigned char* buf, INDEX start, INDEX end) const { return true;}


protected:
	int fixedLength_;
	INDEX size_;
	INDEX capacity_;
	DATA_TYPE type_;
	bool containNull_;
	unsigned char* data_;
};

template<class T, class H>
class FastRecordVector : public FastFixedLengthVector {
public:
	FastRecordVector(DATA_TYPE type, int size, int capacity, unsigned char* srcData, bool containNull): FastFixedLengthVector(type, sizeof(T), size, capacity, srcData, containNull){}
	virtual ConstantSP get(const ConstantSP& index) const {
		if(index->isVector()){
			return retrieve((Vector*)index.get());
		}
		else{
			UINDEX idx=(UINDEX)index->getIndex();
			if(idx<(UINDEX)size_){
				return createScalar(data_ + idx*fixedLength_);
			}
			else
				return Util::createNullConstant(type_);
		}
	}
	virtual ConstantSP get(int index) const { return createScalar(data_ + index * fixedLength_);}
	virtual int compare(INDEX index, const ConstantSP& target) const {
		T* cur = (T*)(data_ + index*fixedLength_);
		return cur->compare(*(T*)target->getBinary());
	}
	const unsigned char* getBinary() const {
		if(size_ == 1)
			return data_;
		else
			throw RuntimeException("A scalar object is expected. But the actual object is a vector.");
	}
	virtual void replace(const ConstantSP& oldVal, const ConstantSP& newVal){
		const T oldV = *(T*)oldVal->getBinary();
		const T newV = *(T*)newVal->getBinary();
		T* data = (T*)data_;
		for(INDEX i=0; i<size_; ++i){
			if(data[i] == oldV)
				data[i] = newV;
		}
	}
	virtual bool isNull(INDEX index) const {
		return ((T*)(data_ + index*fixedLength_))->isNull();
	}
	virtual void nullFill(const ConstantSP& val){
		if(val->getType() != type_)
			throw IncompatibleTypeException(type_, val->getType());
		const unsigned char* buf = val->getBinary();
		const T* data = (const T*)data_;
		for(INDEX i=0; i<size_; ++i){
			if(data[i].isNull())
				memcpy(data_ + i* fixedLength_, buf, fixedLength_);
		}
	}
	virtual bool isNull(INDEX start, int len, char* buf) const {
		const T* data = (const T*)data_;
		for(int i=0; i<len; ++i)
			buf[i] = data[start + i].isNull();
		return true;
	}
	virtual bool isValid(INDEX start, int len, char* buf) const {
		const T* data = (const T*)data_;
		for(int i=0; i<len; ++i)
			buf[i] = data[start + i].isValid();
		return true;
	}


protected:
	virtual bool hasNullInRange(const unsigned char* buf, INDEX start, INDEX end) const {
		const T* data = (const T*)buf;
		for(INDEX i=start; i<end; ++i){
			if(data[i].isNull())
				return true;
		}
		return false;
	}
	ConstantSP createScalar(const unsigned char* data) const{
		ConstantSP obj = Util::createConstant(type_);
		obj->setBinary(data, fixedLength_);
		return obj;
	}

};

class FastInt128Vector : public FastRecordVector<Guid, GuidHash> {
public:
	FastInt128Vector(DATA_TYPE type, int size, int capacity, unsigned char* srcData, bool containNull);
	virtual ~FastInt128Vector(){}
	virtual DATA_TYPE getRawType() const { return DT_INT128;}
	virtual const Guid getInt128() const;
	virtual string getString(INDEX index) const { return Int128::toString(data_ + index*fixedLength_);}
	virtual bool getHash(INDEX start, int len, int buckets, int* buf) const {
		for(int i=0; i<len; ++i)
			buf[i] = murmur32_16b(data_ + (start + i)*16) % buckets;
		return true;
	}

};

class FastUuidVector : public FastInt128Vector {
public:
	FastUuidVector(int size, int capacity, unsigned char* srcData, bool containNull);
	virtual ~FastUuidVector(){}
	virtual string getString(INDEX index) const { return Guid::getString(data_ + index * fixedLength_);}
};

class FastIPAddrVector : public FastInt128Vector {
public:
	FastIPAddrVector(int size, int capacity, unsigned char* srcData, bool containNull);
	virtual ~FastIPAddrVector(){}
	virtual string getString(INDEX index) const { return IPAddr::toString(data_ + index * fixedLength_);}
};

class FastSymbolVector : public AbstractFastVector<int> {
public:
	FastSymbolVector(SymbolBaseSP base, int size, int capacity, int* srcData, bool containNull) :AbstractFastVector(size, capacity, srcData, 0, containNull),  base_(base){
		if(size > 0){
			base_->find("");
		}
	}
	virtual ~FastSymbolVector(){}
	virtual DATA_TYPE getType() const {return DT_SYMBOL;}
	virtual DATA_CATEGORY getCategory() const {return LITERAL;}
	virtual DATA_TYPE getRawType() const {return DT_INT;};
	virtual int compare(INDEX index, const ConstantSP& target) const {return base_->getSymbol(data_[index]).compare(target->getString());}
	virtual string getString(INDEX index) const {return base_->getSymbol(data_[index]);}
	virtual bool set(INDEX index, const ConstantSP& value);
	virtual bool set(const ConstantSP& index, const ConstantSP& value);
	virtual ConstantSP get(INDEX index) const;
	virtual ConstantSP get(const ConstantSP& index) const;
	virtual void fill(INDEX start, INDEX length, const ConstantSP& value);
	virtual void nullFill(const ConstantSP& val){
		int id = base_->findAndInsert(val->getString());
		for(int i = 0; i < size_; i++){
			if(data_[i] == 0) data_[i] = id;
		}
	}
	virtual ConstantSP getSubVector(INDEX start, INDEX length) const { return getSubVector(start, length, std::abs(length));}
	virtual ConstantSP getSubVector(INDEX start, INDEX length, INDEX capacity) const{
		int* data = getDataArray(start,length,capacity);
		if(data)
			return new FastSymbolVector(base_, length, capacity, data, false);
		else
			throw MemoryException();
	}
	virtual ConstantSP getInstance(INDEX size) const {
		INDEX capacity = (std::max)(1, size);
		int* data = new int[capacity];
		return new FastSymbolVector(new SymbolBase(0), size, capacity, data, false);

	}
	virtual ConstantSP getValue() const{return ConstantSP(new FastSymbolVector(base_, size_, 0, data_, false));}
	virtual ConstantSP getValue(INDEX capacity) const {return ConstantSP(new FastSymbolVector(base_, size_, capacity, data_, false));}
	virtual bool append(const ConstantSP& value, INDEX appendSize){
		if(!checkCapacity(appendSize))
			return false;

		if(appendSize==1)
			data_[size_] = base_->findAndInsert(value->getString());
		else{
			if(value->getCategory() != LITERAL || value->size() < appendSize)
				return false;
			for(int i = 0; i < appendSize; i++){
				int fillVal = base_->findAndInsert(value->getString(i));
				data_[i + size_]=fillVal;
			}
		}
		size_+=appendSize;
		if(value->getNullFlag())
			containNull_=true;
		return true;
	}
	virtual bool appendString(string* buf, int len){
		if(!checkCapacity(len))
			return false;
		for(int i=0;i<len;++i)
			data_[size_+i] = base_->findAndInsert(buf[i]);
		size_+=len;
		return true;
	}
	virtual bool appendString(char** buf, int len){
		if(!checkCapacity(len))
			return false;
		for(int i=0;i<len;++i)
			data_[size_+i] = base_->findAndInsert(string(buf[i]));
		size_+=len;
		return true;
	}
	virtual bool getString(INDEX start, int len, char** buf) const{
		for(int i=0;i<len;++i)
			buf[i]=(char*)getString(i).c_str();
		return true;
	}
	virtual void setString(const string& val){data_[0] = base_->findAndInsert(val);}
	virtual void setString(INDEX index, const string& val){data_[index] = base_->findAndInsert(val);}
	virtual bool setString(INDEX start, int len, const string* buf){
		if(start + len > size()) return false;
		for(int i = 0; i < len; i++){
			data_[start + i] = base_->findAndInsert(buf[i]);
		}
		return true;
	}
	virtual bool setString(INDEX start, int len, char** buf){
		if(start + len > size()) return false;
		for(int i = 0; i < len; i++){
			data_[start + i] = base_->findAndInsert(buf[i]);
		}
		return true;
	}
	bool has(const string& val) const {
		int id = base_->find(val);
		if(id == -1){
			return false;
		}else{
			int i;
			for(i=0;i<size_ && data_[i]!=id;++i);
			if(i<size_)
				return true;
			else
				return false;
		}
	}
	INDEX search(const string& val){
		int id = base_->find(val);
		if(id != -1){
			int i;
			for(i=0;i<size_ && data_[i]!=id;++i);
			if(i<size_)
				return i;
			else
				return -1;
		}else{
			return -1;
		}
	}
	virtual void replace(const ConstantSP& oldVal, const ConstantSP& newVal){
		const string oldV = oldVal->getString();
		const string newV = newVal->getString();
		int oldId = base_->find(oldV);
		if(oldId == -1) return;
		int newId = base_->findAndInsert(newV);
		for(INDEX i=0; i<size_; ++i){
			if(data_[i] == oldId)
				data_[i] = newId;
		}
	}
	virtual bool isIndexArray() const { return false;}
	virtual INDEX* getIndexArray() const { return NULL;}
	virtual bool validIndex(INDEX uplimit);
	virtual bool validIndex(INDEX start, INDEX length, INDEX uplimit);
	virtual bool getHash(INDEX start, int len, int buckets, int* buf) const {
		for(int i=0; i<len; ++i){
			string s = base_->getSymbol(data_[i + start]);
			buf[i] = murmur32(s.data(), s.size()) % buckets;
		}
		return true;
	}
	virtual int asof(const ConstantSP& value) const{
		const string& target = value->getStringRef();
		int start = 0;
		int end = size() - 1;
		int mid;
		while(start <= end){
			mid = (start + end) / 2;
			if(base_->getSymbol(data_[mid]).compare(target) <= 0)
				start = mid + 1;
			else 
				end = mid - 1;
		}
		return end;
	}
	ConstantSP retrieve(Vector* index) const {
		INDEX length = index->size();
		bool hasNull =containNull_;
		int* data = getDataArray(index, hasNull);
		if(data)
			return new FastSymbolVector(base_, length, length, data, hasNull);
		else
			throw MemoryException();
	}
	virtual char getBool() const {throw IncompatibleTypeException(DT_BOOL,DT_SYMBOL);}
	virtual char getChar() const {throw IncompatibleTypeException(DT_CHAR,DT_SYMBOL);}
	virtual short getShort() const {throw IncompatibleTypeException(DT_SHORT,DT_SYMBOL);}
	virtual long long getLong() const {throw IncompatibleTypeException(DT_LONG,DT_SYMBOL);}
	virtual INDEX getIndex() const {throw IncompatibleTypeException(DT_INDEX,DT_SYMBOL);}
	virtual float getFloat() const {throw IncompatibleTypeException(DT_FLOAT,DT_SYMBOL);}
	virtual double getDouble() const {throw IncompatibleTypeException(DT_DOUBLE,DT_SYMBOL);}
	virtual void neg(){throw IncompatibleTypeException(DT_DOUBLE,DT_SYMBOL);}
	virtual SymbolBaseSP getSymbolBase() const {return base_;}
private:
	SymbolBaseSP base_;

};

};
#endif /* CONSTANTIMP_H_ */

