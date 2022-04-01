/*
 * DictionaryImp.h
 *
 *  Created on: Oct 23, 2013
 *      Author: dzhou
 */

#ifndef DICTIONARYIMP_H_
#define DICTIONARYIMP_H_

#include "Util.h"
#include <unordered_map>

namespace dolphindb {

typedef void (*U8VectorReader)(const ConstantSP& value, int start, int count, U8* output);
typedef void (*U8ScalarReader)(const ConstantSP& value, U8& output);
typedef void (*U8VectorWriter)(U8* input, const ConstantSP& output, int start, int count);
typedef void (*U8ScalarWriter)(const U8& value, const ConstantSP& output);

class AbstractDictionary: public Dictionary{
public:
	AbstractDictionary(DATA_TYPE keyType, DATA_TYPE valueType):type_(valueType),keyType_(keyType){
		internalType_=Util::convertToIntegralDataType(valueType);
		keyCategory_=Util::getCategory(keyType_);
		init();
	}

	virtual ~AbstractDictionary(){}
	virtual DATA_TYPE getType() const {return type_;}
	virtual DATA_TYPE getRawType() const {return internalType_ == DT_SYMBOL ? DT_INT : internalType_;}
	virtual DATA_CATEGORY getCategory() const {return Util::getCategory(type_);}
	virtual DATA_TYPE getKeyType() const {return keyType_;}
	virtual DATA_CATEGORY getKeyCategory() const {return keyCategory_;}
	virtual const string& getStringRef() const {throw RuntimeException("dictionary doesn't support random access.");}
	virtual ConstantSP get(INDEX index) const {throw RuntimeException("dictionary doesn't support random access.");}
	virtual ConstantSP get(INDEX column, INDEX row) const {throw RuntimeException("dictionary doesn't support random access.");}
	virtual ConstantSP getColumn(INDEX index) const {throw RuntimeException("dictionary doesn't support random access.");}
	virtual ConstantSP getRow(INDEX index) const {throw RuntimeException("dictionary doesn't support random access.");}
	virtual ConstantSP getItem(INDEX index) const {throw RuntimeException("dictionary doesn't support random access.");}

protected:
	void init();
	ConstantSP createValues(const ConstantSP& keys) const;

protected:
	DATA_TYPE internalType_;
	DATA_TYPE type_;
	DATA_TYPE keyType_;
	DATA_CATEGORY keyCategory_;
	U8VectorReader vreader_;
	U8ScalarReader sreader_;
	U8VectorWriter vwriter_;
	U8ScalarWriter swriter_;
	U8 nullVal_;
};

class CharDictionary: public AbstractDictionary{
public:
	CharDictionary(DATA_TYPE keyType, DATA_TYPE type):AbstractDictionary(keyType,type){}
	CharDictionary(const unordered_map<char,U8>& dict, DATA_TYPE keyType, DATA_TYPE type);
	virtual ~CharDictionary();
	virtual void clear(){dict_.clear();}
	virtual INDEX size() const {return (INDEX)dict_.size();}
	virtual INDEX count() const {return (INDEX)dict_.size();}
	virtual ConstantSP getInstance() const { return new CharDictionary(keyType_,type_);}
	virtual ConstantSP getValue() const {return new CharDictionary(dict_,keyType_,type_);}
	virtual bool set(const ConstantSP& key, const ConstantSP& value);
	virtual bool remove(const ConstantSP& value);
	virtual ConstantSP getMember(const ConstantSP& key) const;
	virtual ConstantSP keys() const;
	virtual ConstantSP values() const;
	virtual void contain(const ConstantSP& target, const ConstantSP& resultSP) const;
	virtual string getString() const;
	virtual long long getAllocatedMemory() const;
private:
	unordered_map<char,U8> dict_;
};

class ShortDictionary: public AbstractDictionary{
public:
	ShortDictionary(DATA_TYPE keyType, DATA_TYPE type):AbstractDictionary(keyType,type){}
	ShortDictionary(const unordered_map<short,U8>& dict, DATA_TYPE keyType, DATA_TYPE type);
	virtual ~ShortDictionary();
	virtual void clear(){dict_.clear();}
	virtual INDEX size() const {return (INDEX)dict_.size();}
	virtual INDEX count() const {return (INDEX)dict_.size();}
	virtual ConstantSP getInstance() const { return new ShortDictionary(keyType_,type_);}
	virtual ConstantSP getValue() const {return new ShortDictionary(dict_,keyType_,type_);}
	virtual bool set(const ConstantSP& key, const ConstantSP& value);
	virtual bool remove(const ConstantSP& value);
	virtual ConstantSP getMember(const ConstantSP& key) const;
	virtual ConstantSP keys() const;
	virtual ConstantSP values() const;
	virtual void contain(const ConstantSP& target, const ConstantSP& resultSP) const;
	virtual string getString() const;
	virtual long long getAllocatedMemory() const;
private:
	unordered_map<short,U8> dict_;
};

class IntDictionary: public AbstractDictionary{
public:
	IntDictionary(DATA_TYPE keyType, DATA_TYPE type):AbstractDictionary(keyType,type){}
	IntDictionary(const unordered_map<int,U8>& dict, DATA_TYPE keyType, DATA_TYPE type);
	virtual ~IntDictionary();
	virtual void clear(){dict_.clear();}
	virtual INDEX size() const {return (INDEX)dict_.size();}
	virtual INDEX count() const {return (INDEX)dict_.size();}
	virtual ConstantSP getInstance() const { return new IntDictionary(keyType_,type_);}
	virtual ConstantSP getValue() const {return new IntDictionary(dict_,keyType_,type_);}
	virtual bool set(const ConstantSP& key, const ConstantSP& value);
	virtual bool remove(const ConstantSP& value);
	virtual ConstantSP getMember(const ConstantSP& key) const;
	virtual ConstantSP keys() const;
	virtual ConstantSP values() const;
	virtual void contain(const ConstantSP& target, const ConstantSP& resultSP) const;
	virtual string getString() const;
	virtual long long getAllocatedMemory() const;
private:
	unordered_map<int,U8> dict_;
};

class LongDictionary: public AbstractDictionary{
public:
	LongDictionary(DATA_TYPE keyType, DATA_TYPE type):AbstractDictionary(keyType,type){}
	LongDictionary(const unordered_map<long long,U8>& dict, DATA_TYPE keyType,DATA_TYPE type);
	virtual ~LongDictionary();
	virtual void clear(){dict_.clear();}
	virtual INDEX size() const {return (INDEX)dict_.size();}
	virtual INDEX count() const {return (INDEX)dict_.size();}
	virtual ConstantSP getInstance() const { return new LongDictionary(keyType_,type_);}
	virtual ConstantSP getValue() const {return new LongDictionary(dict_,keyType_,type_);}
	virtual bool set(const ConstantSP& key, const ConstantSP& value);
	virtual bool remove(const ConstantSP& value);
	virtual ConstantSP getMember(const ConstantSP& key) const;
	virtual ConstantSP keys() const;
	virtual ConstantSP values() const;
	virtual void contain(const ConstantSP& target, const ConstantSP& resultSP) const;
	virtual string getString() const;
	virtual long long getAllocatedMemory() const;
private:
	unordered_map<long long,U8> dict_;
};

class FloatDictionary: public AbstractDictionary{
public:
	FloatDictionary(DATA_TYPE type):AbstractDictionary(DT_FLOAT,type){}
	FloatDictionary(const unordered_map<float,U8>& dict, DATA_TYPE type);
	virtual ~FloatDictionary();
	virtual void clear(){dict_.clear();}
	virtual INDEX size() const {return (INDEX)dict_.size();}
	virtual INDEX count() const {return (INDEX)dict_.size();}
	virtual ConstantSP getInstance() const { return new FloatDictionary(type_);}
	virtual ConstantSP getValue() const {return new FloatDictionary(dict_,type_);}
	virtual bool set(const ConstantSP& key, const ConstantSP& value);
	virtual bool remove(const ConstantSP& value);
	virtual ConstantSP getMember(const ConstantSP& key) const;
	virtual ConstantSP keys() const;
	virtual ConstantSP values() const;
	virtual void contain(const ConstantSP& target, const ConstantSP& resultSP) const;
	virtual string getString() const;
	virtual long long getAllocatedMemory() const;
private:
	unordered_map<float,U8> dict_;
};

class DoubleDictionary: public AbstractDictionary{
public:
	DoubleDictionary(DATA_TYPE type):AbstractDictionary(DT_DOUBLE,type){}
	DoubleDictionary(const unordered_map<double,U8>& dict, DATA_TYPE type);
	virtual ~DoubleDictionary();
	virtual void clear(){dict_.clear();}
	virtual INDEX size() const {return (INDEX)dict_.size();}
	virtual INDEX count() const {return (INDEX)dict_.size();}
	virtual ConstantSP getInstance() const { return new DoubleDictionary(type_);}
	virtual ConstantSP getValue() const {return new DoubleDictionary(dict_,type_);}
	virtual bool set(const ConstantSP& key, const ConstantSP& value);
	virtual bool remove(const ConstantSP& value);
	virtual ConstantSP getMember(const ConstantSP& key) const;
	virtual ConstantSP keys() const;
	virtual ConstantSP values() const;
	virtual void contain(const ConstantSP& target, const ConstantSP& resultSP) const;
	virtual string getString() const;
	virtual long long getAllocatedMemory() const;
private:
	unordered_map<double,U8> dict_;
};

class StringDictionary: public AbstractDictionary{
public:
	StringDictionary(DATA_TYPE keyType, DATA_TYPE type):AbstractDictionary(keyType,type){}
	StringDictionary(const unordered_map<string,U8>& dict, DATA_TYPE keyType,DATA_TYPE type);
	virtual ~StringDictionary();
	virtual void clear(){dict_.clear();}
	virtual INDEX size() const {return (INDEX)dict_.size();}
	virtual INDEX count() const {return (INDEX)dict_.size();}
	virtual ConstantSP getInstance() const { return new StringDictionary(keyType_,type_);}
	virtual ConstantSP getValue() const {return new StringDictionary(dict_,keyType_,type_);}
	virtual bool set(const string& key, const ConstantSP& value);
	virtual bool set(const ConstantSP& key, const ConstantSP& value);
	virtual bool remove(const ConstantSP& value);
	virtual ConstantSP getMember(const ConstantSP& key) const;
	virtual ConstantSP getMember(const string& key) const;
	virtual ConstantSP keys() const;
	virtual ConstantSP values() const;
	virtual void contain(const ConstantSP& target, const ConstantSP& resultSP) const;
	virtual string getString() const;
	virtual long long getAllocatedMemory() const;
private:
	unordered_map<string,U8> dict_;
};

class Int128Dictionary: public AbstractDictionary{
public:
	Int128Dictionary(DATA_TYPE keyType, DATA_TYPE type):AbstractDictionary(keyType,type){}
	Int128Dictionary(const unordered_map<Guid,U8>& dict, DATA_TYPE keyType, DATA_TYPE type);
	unordered_map<Guid,U8>& getInternalDict() { return dict_;}
	virtual ~Int128Dictionary();
	virtual void clear(){dict_.clear();}
	virtual INDEX size() const {return (INDEX)dict_.size();}
	virtual INDEX count() const {return (INDEX)dict_.size();}
	virtual ConstantSP getInstance() const { return new Int128Dictionary(keyType_,type_);}
	virtual ConstantSP getValue() const {return new Int128Dictionary(dict_,keyType_,type_);}
	virtual bool set(const ConstantSP& key, const ConstantSP& value);
	virtual bool remove(const ConstantSP& value);
	virtual ConstantSP getMember(const ConstantSP& key) const;
	virtual ConstantSP keys() const;
	virtual ConstantSP values() const;
	virtual void contain(const ConstantSP& target, const ConstantSP& resultSP) const;
	virtual string getString() const;
	virtual long long getAllocatedMemory() const;
private:
	unordered_map<Guid,U8> dict_;
};

class AnyDictionary: public AbstractDictionary{
public:
	AnyDictionary():AbstractDictionary(DT_STRING,DT_ANY){}
	AnyDictionary(const unordered_map<string,ConstantSP>& dict):AbstractDictionary(DT_STRING,DT_ANY),dict_(dict){}
	virtual ~AnyDictionary(){};
	virtual void clear(){dict_.clear();}
	virtual INDEX size() const {return (INDEX)dict_.size();}
	virtual INDEX count() const {return (INDEX)dict_.size();}
	virtual ConstantSP getInstance() const { return new AnyDictionary();}
	virtual ConstantSP getValue() const {return new AnyDictionary(dict_);}
	virtual bool set(const string& key, const ConstantSP& value);
	virtual bool set(const ConstantSP& key, const ConstantSP& value);
	virtual bool remove(const ConstantSP& value);
	virtual ConstantSP getMember(const string& key) const;
	virtual ConstantSP getMember(const ConstantSP& key) const;
	virtual ConstantSP keys() const;
	virtual ConstantSP values() const;
	virtual void contain(const ConstantSP& target, const ConstantSP& resultSP) const;
	virtual string getString() const;
	virtual long long getAllocatedMemory() const;
	virtual bool containNotMarshallableObject() const;
private:
	unordered_map<string,ConstantSP> dict_;
};

class IntAnyDictionary: public AbstractDictionary{
public:
	IntAnyDictionary(DATA_TYPE keyType = DT_INT):AbstractDictionary(keyType,DT_ANY){}
	IntAnyDictionary(const unordered_map<int,ConstantSP>& dict, DATA_TYPE keyType = DT_INT):AbstractDictionary(keyType,DT_ANY),dict_(dict){}
	virtual ~IntAnyDictionary(){};
	virtual void clear(){dict_.clear();}
	virtual INDEX size() const {return (INDEX)dict_.size();}
	virtual INDEX count() const {return (INDEX)dict_.size();}
	virtual ConstantSP getInstance() const { return new IntAnyDictionary(keyType_);}
	virtual ConstantSP getValue() const {return new IntAnyDictionary(dict_, keyType_);}
	virtual bool set(const ConstantSP& key, const ConstantSP& value);
	virtual bool set(int key, const ConstantSP& value);
	virtual bool remove(const ConstantSP& value);
	virtual ConstantSP getMember(const ConstantSP& key) const;
	virtual ConstantSP keys() const;
	virtual ConstantSP values() const;
	virtual void contain(const ConstantSP& target, const ConstantSP& resultSP) const;
	virtual string getString() const;
	virtual long long getAllocatedMemory() const;
	virtual bool containNotMarshallableObject() const;
private:
	unordered_map<int,ConstantSP> dict_;
};


class LongAnyDictionary: public AbstractDictionary{
public:
	LongAnyDictionary(DATA_TYPE keyType = DT_LONG):AbstractDictionary(keyType,DT_ANY){}
	LongAnyDictionary(const unordered_map<long long,ConstantSP>& dict, DATA_TYPE keyType = DT_LONG):AbstractDictionary(keyType,DT_ANY), dict_(dict){}
	unordered_map<long long,ConstantSP>& getInternalDict() { return dict_;}
	virtual ~LongAnyDictionary(){};
	virtual void clear(){dict_.clear();}
	virtual INDEX size() const {return (INDEX)dict_.size();}
	virtual INDEX count() const {return (INDEX)dict_.size();}
	virtual ConstantSP getInstance() const { return new LongAnyDictionary(keyType_);}
	virtual ConstantSP getValue() const {return new LongAnyDictionary(dict_, keyType_);}
	virtual bool set(const ConstantSP& key, const ConstantSP& value);
	virtual bool remove(const ConstantSP& value);
	virtual ConstantSP getMember(const ConstantSP& key) const;
	virtual ConstantSP keys() const;
	virtual ConstantSP values() const;
	virtual void contain(const ConstantSP& target, const ConstantSP& resultSP) const;
	virtual string getString() const;
	virtual long long getAllocatedMemory() const;
	virtual bool containNotMarshallableObject() const;
private:
	unordered_map<long long,ConstantSP> dict_;
};

class Int128AnyDictionary: public AbstractDictionary{
public:
	Int128AnyDictionary(DATA_TYPE keyType = DT_INT128):AbstractDictionary(keyType,DT_ANY){}
	Int128AnyDictionary(const unordered_map<Guid,ConstantSP>& dict, DATA_TYPE keyType = DT_INT128):AbstractDictionary(keyType,DT_ANY), dict_(dict){}
	unordered_map<Guid,ConstantSP>& getInternalDict() { return dict_;}
	virtual ~Int128AnyDictionary(){};
	virtual void clear(){dict_.clear();}
	virtual INDEX size() const {return (INDEX)dict_.size();}
	virtual INDEX count() const {return (INDEX)dict_.size();}
	virtual ConstantSP getInstance() const { return new Int128AnyDictionary(keyType_);}
	virtual ConstantSP getValue() const {return new Int128AnyDictionary(dict_, keyType_);}
	virtual bool set(const ConstantSP& key, const ConstantSP& value);
	virtual bool remove(const ConstantSP& value);
	virtual ConstantSP getMember(const ConstantSP& key) const;
	virtual ConstantSP keys() const;
	virtual ConstantSP values() const;
	virtual void contain(const ConstantSP& target, const ConstantSP& resultSP) const;
	virtual string getString() const;
	virtual long long getAllocatedMemory() const;
	virtual bool containNotMarshallableObject() const;
private:
	unordered_map<Guid,ConstantSP> dict_;
};

};

#endif /* DICTIONARYIMP_H_ */
