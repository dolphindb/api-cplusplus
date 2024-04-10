#pragma once

#include "Constant.h"
#include "SmartPointer.h"

namespace dolphindb {

class EXPORT_DECL Vector:public Constant{
public:
    Vector(): Constant(259){}
    virtual ~Vector(){}
    virtual ConstantSP getColumnLabel() const;
    const string& getName() const {return name_;}
    void setName(const string& name){name_=name;}
    virtual bool isLargeConstant() const { return isMatrix() || size()>1024; }
    virtual void initialize(){}
    virtual INDEX getCapacity() const = 0;
    virtual	INDEX reserve(INDEX capacity) {throw RuntimeException("Vector::reserve method not supported");}
    virtual	void resize(INDEX size) {throw RuntimeException("Vector::resize method not supported");}
    virtual INDEX getValueSize() const {throw RuntimeException("Vector::getValueSize method not supported"); return 0;}
    virtual short getUnitLength() const = 0;
    virtual void clear()=0;
    virtual bool remove(INDEX count){return false;}
    virtual bool remove(const ConstantSP& index){return false;}
    virtual bool append(const ConstantSP& value){return append(value, value->size());}
    virtual bool append(const ConstantSP& value, INDEX count){return false;}
    virtual bool append(const ConstantSP value, INDEX start, INDEX length){return false;}
    virtual bool appendBool(char* buf, int len){return false;}
    virtual bool appendChar(char* buf, int len){return false;}
    virtual bool appendShort(short* buf, int len){return false;}
    virtual bool appendInt(int* buf, int len){return false;}
    virtual bool appendLong(long long* buf, int len){return false;}
    virtual bool appendIndex(INDEX* buf, int len){return false;}
    virtual bool appendFloat(float* buf, int len){return false;}
    virtual bool appendDouble(double* buf, int len){return false;}
    virtual bool appendString(string* buf, int len){return false;}
    virtual bool appendString(char** buf, int len){return false;}
    virtual string getString() const;
    virtual string getScript() const;
    virtual string getString(INDEX index) const = 0;
    virtual VECTOR_TYPE getVectorType() const{return VECTOR_TYPE::ARRAY;}
    virtual bool isSorted(bool asc, bool strict = false) const {throw RuntimeException("Vector::isSorted method not supported"); return false;}
    virtual ConstantSP getInstance() const {return getInstance(size());}
    virtual ConstantSP getInstance(INDEX size) const = 0;
    virtual ConstantSP getValue(INDEX capacity) const {throw RuntimeException("Vector::getValue method not supported");}
    virtual ConstantSP get(INDEX column, INDEX rowStart,INDEX rowEnd) const {return getSubVector(column*rows()+rowStart,rowEnd-rowStart);}
    virtual ConstantSP get(INDEX index) const = 0;
    virtual ConstantSP getWindow(INDEX colStart, int colLength, INDEX rowStart, int rowLength) const {return getSubVector(rowStart,rowLength);}
    virtual ConstantSP getSubVector(INDEX start, INDEX length) const { throw RuntimeException("getSubVector method not supported");}
    virtual ConstantSP getSubVector(INDEX start, INDEX length, INDEX capacity) const { return getSubVector(start, length);}
    virtual void fill(INDEX start, INDEX length, const ConstantSP& value)=0;
    virtual void next(INDEX steps)=0;
    virtual void prev(INDEX steps)=0;
    virtual void reverse()=0;
    virtual void reverse(INDEX start, INDEX length)=0;
    virtual void replace(const ConstantSP& oldVal, const ConstantSP& newVal){}
    virtual bool validIndex(INDEX uplimit){return false;}
    virtual bool validIndex(INDEX start, INDEX length, INDEX uplimit){return false;}
    virtual void addIndex(INDEX start, INDEX length, INDEX offset){}
    virtual void neg()=0;
    virtual void upper(){throw RuntimeException("upper method not supported");}
    virtual void lower(){throw RuntimeException("lower method not supported");}
    virtual void trim(){throw RuntimeException("trim method not supported");}
    virtual void strip(){throw RuntimeException("strip method not supported");}
    virtual long long getAllocatedMemory(INDEX size) const {return Constant::getAllocatedMemory();}
    virtual int asof(const ConstantSP& value) const {throw RuntimeException("asof not supported.");}
    virtual ConstantSP castTemporal(DATA_TYPE expectType){throw RuntimeException("castTemporal not supported");}
private:
    string name_;
};
typedef SmartPointer<Vector> VectorSP;
}