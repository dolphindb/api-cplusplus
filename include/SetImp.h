/*
 * SetImp.h
 *
 *  Created on: Jan 17, 2016
 *      Author: dzhou
 */

#ifndef SETIMP_H_
#define SETIMP_H_

#include <unordered_set>

#include "Set.h"
#include "Util.h"

namespace dolphindb {

template<class T>
class AbstractSet : public Set {
public:
	AbstractSet(DATA_TYPE type, INDEX capacity = 0) : type_(type), category_(Util::getCategory(type_)){
		if(capacity > 0) data_.reserve(capacity);
	}
	AbstractSet(DATA_TYPE type, const std::unordered_set<T>& data) : type_(type), category_(Util::getCategory(type_)), data_(data){}
	virtual bool sizeable() const {return true;}
	virtual INDEX size() const {return static_cast<INDEX>(data_.size());}
	virtual ConstantSP keys() const {
		return getSubVector(0, static_cast<INDEX>(data_.size()));
	}
	virtual DATA_TYPE getType() const {return type_;}
	virtual DATA_TYPE getRawType() const {return type_ == DT_SYMBOL ? DT_INT : Util::convertToIntegralDataType(type_);}
	virtual DATA_CATEGORY getCategory() const {return category_;}
	virtual long long getAllocatedMemory() const { return sizeof(T) * data_.bucket_count();}
	virtual std::string getString() const {
		int len=(std::min)(Util::DISPLAY_ROWS,size());
		ConstantSP key = getSubVector(0, len);
		std::string str("set(");

		if(len>0){
			if(len == 1 && key->isNull(0))
				str.append(key->get(0)->getScript());
			else{
				if(isNull(0)){
					//do nothing
				}
				else
					str.append(key->get(0)->getScript());
			}
		}
		for(int i=1;i<len;++i){
			str.append(",");
			if(isNull(i)){
				//do nothing
			}
			else
				str.append(key->get(i)->getScript());
		}
		if(size()>len)
			str.append("...");
		str.append(")");
		return str;
	}
	virtual void clear(){ data_.clear();}
	virtual ConstantSP get(const ConstantSP& index) const {throw RuntimeException("set doesn't support random access.");}
	virtual const std::string& getStringRef() const {throw RuntimeException("set doesn't support random access.");}
	virtual ConstantSP get(INDEX index) const {throw RuntimeException("set doesn't support random access.");}
	virtual ConstantSP get(INDEX column, INDEX row) const {throw RuntimeException("set doesn't support random access.");}
	virtual ConstantSP getColumn(INDEX index) const {throw RuntimeException("set doesn't support random access.");}
	virtual ConstantSP getRow(INDEX index) const {throw RuntimeException("set doesn't support random access.");}
	virtual ConstantSP getItem(INDEX index) const {throw RuntimeException("set doesn't support random access.");}

protected:
	DATA_TYPE type_;
	DATA_CATEGORY category_;
	std::unordered_set<T> data_;
};

class CharSet : public AbstractSet<char> {
public:
	CharSet(INDEX capacity = 0) : AbstractSet<char>(DT_CHAR, capacity){}
	CharSet(const std::unordered_set<char>& data) : AbstractSet<char>(DT_CHAR, data){}
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
	ShortSet(const std::unordered_set<short>& data) : AbstractSet<short>(DT_SHORT, data){}
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
	IntSet(DATA_TYPE type, const std::unordered_set<int>& data) : AbstractSet<int>(type, data){}
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
	LongSet(DATA_TYPE type, const std::unordered_set<long long>& data) : AbstractSet<long long>(type, data){}
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
	FloatSet(const std::unordered_set<float>& data) : AbstractSet<float>(DT_FLOAT, data){}
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
	DoubleSet(const std::unordered_set<double>& data) : AbstractSet<double>(DT_DOUBLE, data){}
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

class StringSet : public AbstractSet<std::string> {
public:
	StringSet(INDEX capacity = 0, bool isBlob = false, bool isSymbol = false)
		: AbstractSet<std::string>(isBlob ? DT_BLOB : isSymbol ? DT_SYMBOL : DT_STRING, capacity), isBlob_(isBlob), isSymbol_(isSymbol){}
	StringSet(const std::unordered_set<std::string>& data, bool isBlob = false, bool isSymbol = false)
		: AbstractSet<std::string>(isBlob ? DT_BLOB : isSymbol ? DT_SYMBOL : DT_STRING, data), isBlob_(isBlob), isSymbol_(isSymbol){}
	virtual ConstantSP getInstance() const { return new StringSet(0, isBlob_, isSymbol_);}
	virtual ConstantSP getValue() const { return new StringSet(data_, isBlob_, isSymbol_);}
	virtual void contain(const ConstantSP& target, const ConstantSP& resultSP) const;
	virtual bool remove(const ConstantSP& value) { return manipulate(value, true);}
	virtual bool append(const ConstantSP& value) { return manipulate(value, false);}
	virtual bool inverse(const ConstantSP& value);
	virtual bool isSuperset(const ConstantSP& target) const;
	virtual ConstantSP interaction(const ConstantSP& target) const;
	virtual ConstantSP getSubVector(INDEX start, INDEX length) const;
	bool manipulate(const ConstantSP& value, bool deletion);
private:
	bool isBlob_;
	bool isSymbol_;
};

class Int128Set : public AbstractSet<Guid> {
public:
	Int128Set(DATA_TYPE type = DT_INT128, INDEX capacity = 0) : AbstractSet<Guid>(type, capacity){}
	Int128Set(DATA_TYPE type, const std::unordered_set<Guid>& data) : AbstractSet<Guid>(type, data){}
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

}
#endif /* SETIMP_H_ */
