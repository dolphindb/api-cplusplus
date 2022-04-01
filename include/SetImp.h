/*
 * SetImp.h
 *
 *  Created on: Jan 17, 2016
 *      Author: dzhou
 */

#ifndef SETIMP_H_
#define SETIMP_H_

#include <unordered_set>

#include "DolphinDB.h"
#include "Util.h"

namespace dolphindb {

template<class T>
class AbstractSet : public Set {
public:
	AbstractSet(DATA_TYPE type, INDEX capacity = 0) : type_(type), category_(Util::getCategory(type_)){
		if(capacity > 0) data_.reserve(capacity);
	}
	AbstractSet(DATA_TYPE type, const unordered_set<T>& data) : type_(type), category_(Util::getCategory(type_)), data_(data){}
	virtual bool sizeable() const {return true;}
	virtual INDEX size() const {return data_.size();}
	virtual ConstantSP keys() const {
		return getSubVector(0, data_.size());
	}
	virtual DATA_TYPE getType() const {return type_;}
	virtual DATA_TYPE getRawType() const {return type_ == DT_SYMBOL ? DT_INT : Util::convertToIntegralDataType(type_);}
	virtual DATA_CATEGORY getCategory() const {return category_;}
	virtual long long getAllocatedMemory() const { return sizeof(T) * data_.bucket_count();}
	virtual string getString() const {
		int len=(std::min)(Util::DISPLAY_ROWS,size());
		ConstantSP keys = getSubVector(0, len);
		string str("set(");

		if(len>0){
			if(len == 1 && keys->isNull(0))
				str.append(keys->get(0)->getScript());
			else{
				if(isNull(0)){
					//do nothing
				}
				else
					str.append(keys->get(0)->getScript());
			}
		}
		for(int i=1;i<len;++i){
			str.append(",");
			if(isNull(i)){
				//do nothing
			}
			else
				str.append(keys->get(i)->getScript());
		}
		if(size()>len)
			str.append("...");
		str.append(")");
		return str;
	}
	virtual void clear(){ data_.clear();}
	virtual ConstantSP get(const ConstantSP& index) const {throw RuntimeException("set doesn't support random access.");}
	virtual const string& getStringRef() const {throw RuntimeException("set doesn't support random access.");}
	virtual ConstantSP get(INDEX index) const {throw RuntimeException("set doesn't support random access.");}
	virtual ConstantSP get(INDEX column, INDEX row) const {throw RuntimeException("set doesn't support random access.");}
	virtual ConstantSP getColumn(INDEX index) const {throw RuntimeException("set doesn't support random access.");}
	virtual ConstantSP getRow(INDEX index) const {throw RuntimeException("set doesn't support random access.");}
	virtual ConstantSP getItem(INDEX index) const {throw RuntimeException("set doesn't support random access.");}

protected:
	DATA_TYPE type_;
	DATA_CATEGORY category_;
	unordered_set<T> data_;
};

class CharSet : public AbstractSet<char> {
public:
	CharSet(INDEX capacity = 0) : AbstractSet<char>(DT_CHAR, capacity){}
	CharSet(const unordered_set<char>& data) : AbstractSet<char>(DT_CHAR, data){}
	virtual ConstantSP getInstance() const { return new CharSet();}
	virtual ConstantSP getValue() const { return new CharSet(data_);}
	virtual void contain(const ConstantSP& target, const ConstantSP& resultSP) const;
	virtual bool remove(const ConstantSP& value) { return manipulate(value, true);}
	virtual bool append(const ConstantSP& value) { return manipulate(value, false);}
	virtual bool inverse(const ConstantSP& value);
	virtual bool isSuperset(const ConstantSP& target) const;
	virtual ConstantSP interaction(const ConstantSP& target) const;
	virtual ConstantSP getSubVector(INDEX start, INDEX length) const;
	bool manipulate(const ConstantSP& value, bool deletion);
};

class ShortSet : public AbstractSet<short> {
public:
	ShortSet(INDEX capacity = 0) : AbstractSet<short>(DT_SHORT, capacity){}
	ShortSet(const unordered_set<short>& data) : AbstractSet<short>(DT_SHORT, data){}
	virtual ConstantSP getInstance() const { return new ShortSet();}
	virtual ConstantSP getValue() const { return new ShortSet(data_);}
	virtual void contain(const ConstantSP& target, const ConstantSP& resultSP) const;
	virtual bool remove(const ConstantSP& value) { return manipulate(value, true);}
	virtual bool append(const ConstantSP& value) { return manipulate(value, false);}
	virtual bool inverse(const ConstantSP& value);
	virtual bool isSuperset(const ConstantSP& target) const;
	virtual ConstantSP interaction(const ConstantSP& target) const;
	virtual ConstantSP getSubVector(INDEX start, INDEX length) const;
	bool manipulate(const ConstantSP& value, bool deletion);
};

class IntSet : public AbstractSet<int> {
public:
	IntSet(DATA_TYPE type = DT_INT, INDEX capacity = 0) : AbstractSet<int>(type, capacity){}
	IntSet(DATA_TYPE type, const unordered_set<int>& data) : AbstractSet<int>(type, data){}
	virtual ConstantSP getInstance() const { return new IntSet(type_);}
	virtual ConstantSP getValue() const { return new IntSet(type_, data_);}
	virtual void contain(const ConstantSP& target, const ConstantSP& resultSP) const;
	virtual bool remove(const ConstantSP& value) { return manipulate(value, true);}
	virtual bool append(const ConstantSP& value) { return manipulate(value, false);}
	virtual bool inverse(const ConstantSP& value);
	virtual bool isSuperset(const ConstantSP& target) const;
	virtual ConstantSP interaction(const ConstantSP& target) const;
	virtual ConstantSP getSubVector(INDEX start, INDEX length) const;
	bool manipulate(const ConstantSP& value, bool deletion);
};

class LongSet : public AbstractSet<long long> {
public:
	LongSet(DATA_TYPE type = DT_LONG, INDEX capacity = 0) : AbstractSet<long long>(type, capacity){}
	LongSet(DATA_TYPE type, const unordered_set<long long>& data) : AbstractSet<long long>(type, data){}
	virtual ConstantSP getInstance() const { return new LongSet(type_);}
	virtual ConstantSP getValue() const { return new LongSet(type_, data_);}
	virtual void contain(const ConstantSP& target, const ConstantSP& resultSP) const;
	virtual bool remove(const ConstantSP& value) { return manipulate(value, true);}
	virtual bool append(const ConstantSP& value) { return manipulate(value, false);}
	virtual bool inverse(const ConstantSP& value);
	virtual bool isSuperset(const ConstantSP& target) const;
	virtual ConstantSP interaction(const ConstantSP& target) const;
	virtual ConstantSP getSubVector(INDEX start, INDEX length) const;
	bool manipulate(const ConstantSP& value, bool deletion);
};

class FloatSet : public AbstractSet<float> {
public:
	FloatSet(INDEX capacity = 0) : AbstractSet<float>(DT_FLOAT, capacity){}
	FloatSet(const unordered_set<float>& data) : AbstractSet<float>(DT_FLOAT, data){}
	virtual ConstantSP getInstance() const { return new FloatSet();}
	virtual ConstantSP getValue() const { return new FloatSet(data_);}
	virtual void contain(const ConstantSP& target, const ConstantSP& resultSP) const;
	virtual bool remove(const ConstantSP& value) { return manipulate(value, true);}
	virtual bool append(const ConstantSP& value) { return manipulate(value, false);}
	virtual bool inverse(const ConstantSP& value);
	virtual bool isSuperset(const ConstantSP& target) const;
	virtual ConstantSP interaction(const ConstantSP& target) const;
	virtual ConstantSP getSubVector(INDEX start, INDEX length) const;
	bool manipulate(const ConstantSP& value, bool deletion);
};

class DoubleSet : public AbstractSet<double> {
public:
	DoubleSet(INDEX capacity = 0) : AbstractSet<double>(DT_DOUBLE, capacity){}
	DoubleSet(const unordered_set<double>& data) : AbstractSet<double>(DT_DOUBLE, data){}
	virtual ConstantSP getInstance() const { return new DoubleSet();}
	virtual ConstantSP getValue() const { return new DoubleSet(data_);}
	virtual void contain(const ConstantSP& target, const ConstantSP& resultSP) const;
	virtual bool remove(const ConstantSP& value) { return manipulate(value, true);}
	virtual bool append(const ConstantSP& value) { return manipulate(value, false);}
	virtual bool inverse(const ConstantSP& value);
	virtual bool isSuperset(const ConstantSP& target) const;
	virtual ConstantSP interaction(const ConstantSP& target) const;
	virtual ConstantSP getSubVector(INDEX start, INDEX length) const;
	bool manipulate(const ConstantSP& value, bool deletion);
};

class StringSet : public AbstractSet<string> {
public:
	StringSet(INDEX capacity = 0) : AbstractSet<string>(DT_STRING, capacity){}
	StringSet(const unordered_set<string>& data) : AbstractSet<string>(DT_STRING, data){}
	virtual ConstantSP getInstance() const { return new StringSet();}
	virtual ConstantSP getValue() const { return new StringSet(data_);}
	virtual void contain(const ConstantSP& target, const ConstantSP& resultSP) const;
	virtual bool remove(const ConstantSP& value) { return manipulate(value, true);}
	virtual bool append(const ConstantSP& value) { return manipulate(value, false);}
	virtual bool inverse(const ConstantSP& value);
	virtual bool isSuperset(const ConstantSP& target) const;
	virtual ConstantSP interaction(const ConstantSP& target) const;
	virtual ConstantSP getSubVector(INDEX start, INDEX length) const;
	bool manipulate(const ConstantSP& value, bool deletion);
};

class Int128Set : public AbstractSet<Guid> {
public:
	Int128Set(DATA_TYPE type = DT_INT128, INDEX capacity = 0) : AbstractSet<Guid>(type, capacity){}
	Int128Set(DATA_TYPE type, const unordered_set<Guid>& data) : AbstractSet<Guid>(type, data){}
	virtual ConstantSP getInstance() const { return new Int128Set(type_);}
	virtual ConstantSP getValue() const { return new Int128Set(type_, data_);}
	virtual void contain(const ConstantSP& target, const ConstantSP& resultSP) const;
	virtual bool remove(const ConstantSP& value) { return manipulate(value, true);}
	virtual bool append(const ConstantSP& value) { return manipulate(value, false);}
	virtual bool inverse(const ConstantSP& value);
	virtual bool isSuperset(const ConstantSP& target) const;
	virtual ConstantSP interaction(const ConstantSP& target) const;
	virtual ConstantSP getSubVector(INDEX start, INDEX length) const;
	bool manipulate(const ConstantSP& value, bool deletion);
};

};
#endif /* SETIMP_H_ */
