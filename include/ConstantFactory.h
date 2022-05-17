/*
 * ConstantFactory.h
 *
 *  Created on: Jan 19, 2013
 *      Author: dzhou
 */

#ifndef CONSTANTFACTORY_H_
#define CONSTANTFACTORY_H_

#include "Types.h"
#include "ScalarImp.h"
#include "ConstantImp.h"
#include "SetImp.h"
#include "DictionaryImp.h"
#include "Exceptions.h"
#include "Util.h"

namespace dolphindb {

class ConstantFactory{
	typedef Constant*(ConstantFactory::*ConstantParser)(const string&);
	typedef Constant*(ConstantFactory::*ConstantFunc)();
	typedef Vector*(ConstantFactory::*ConstantVectorFunc)(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull);
	typedef Vector*(ConstantFactory::*ConstantArrayVectorFunc)(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull);
	typedef Vector*(ConstantFactory::*ConstantMatrixFunc)(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull);
	typedef Vector*(ConstantFactory::*ConstantRptVectorFunc)(const ConstantSP& scalar, INDEX size);
public:
	ConstantFactory(){
		init();
	}

	Constant* parseConstant(int type, const string& word){
		ConstantParser func=arrConstParser[type];
		if(func==NULL)
			return NULL;
		else
			return (this->*func)(word);
	}

	Constant* createConstant(DATA_TYPE type){
		if(type <0 || type >= TYPE_COUNT)
			throw RuntimeException("Invalid data type value " + Util::getDataTypeString(type));
		ConstantFunc func=arrConstFactory[type];
		if(func==NULL)
			throw RuntimeException("Not allowed to create a scalar with type " + Util::getDataTypeString(type));
		else
			return (this->*func)();
	}

	Vector* createConstantVector(DATA_TYPE type ,INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(type <0 || type >= TYPE_COUNT)
			throw RuntimeException("Invalid Vector data type value " + Util::getDataTypeString(type));
		ConstantVectorFunc func=arrConstVectorFactory[type];
		if(func==NULL)
			throw RuntimeException("Not allowed to create a vector with type " + Util::getDataTypeString(type));
		else
			return (this->*func)(size,capacity,fastMode,extraParam, data, dataSegment, segmentSizeInBit, containNull);
	}

	Vector* createConstantArrayVector(DATA_TYPE type ,INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, INDEX *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(type < ARRAY_TYPE_BASE || type >= TYPE_COUNT + ARRAY_TYPE_BASE)
			throw RuntimeException("Invalid ArrayVector data type value " + Util::getDataTypeString(type));
		ConstantArrayVectorFunc func = arrConstArrayVectorFactory[type - 64];
		if(func == NULL)
			throw RuntimeException("Not allowed to create a vector with type " + Util::getDataTypeString(type));
		else
			return (this->*func)(size,capacity,fastMode,extraParam, data, (void *)pindex, dataSegment, segmentSizeInBit, containNull);
	}

	Vector* createConstantRepeatingVector(DATA_TYPE type,const ConstantSP& scalar, INDEX size){
		if(type <0 || type >= TYPE_COUNT)
			throw RuntimeException("Invalid RepeatingVector data type value " + Util::getDataTypeString(type));
		ConstantRptVectorFunc func=arrConstRptVectorFactory[type];
		if(func==NULL)
			throw RuntimeException("Not allowed to create a repeating vector with type " + Util::getDataTypeString(type));
		else
			return (this->*func)(scalar,size);
	}

	Vector* createConstantMatrix(DATA_TYPE type,int cols, int rows, int colCapacity, int extraParam, void* data=0, void** dataSegment=0, int segmentSizeInBit=0, bool containNull=false){
		if(type <0 || type >= TYPE_COUNT)
			throw RuntimeException("Invalid Matrix data type value " + Util::getDataTypeString(type));
		ConstantMatrixFunc func=arrConstMatrixFactory[type];
		if(func==NULL)
			throw RuntimeException("Not allowed to create a matrix with type " + Util::getDataTypeString(type));
		else
			return (this->*func)(cols,rows,colCapacity,extraParam, data, dataSegment, segmentSizeInBit, containNull);
	}

	DATA_TYPE getDataType(const string& type) const {
		unordered_map<string,DATA_TYPE>::const_iterator it=typeMap_.find(type);
		if(it==typeMap_.end())
			return DT_VOID;
		else
			return it->second;
	}

	DATA_FORM getDataForm(const string& form) const {
		unordered_map<string,DATA_FORM>::const_iterator it=formMap_.find(form);
		if(it==formMap_.end())
			return (DATA_FORM)-1;
		else
			return it->second;
	}


	char getDataTypeSymbol(DATA_TYPE type) const {
		return arrTypeSymbol[type];
	}

	string getDataTypeString(DATA_TYPE type) const {
		if(type >= 0 && type < TYPE_COUNT)
			return arrTypeStr[type];
		else if(type >= ARRAY_TYPE_BASE){
			return "["+getDataTypeString((DATA_TYPE)(type-ARRAY_TYPE_BASE))+"]";
		}else{
			return "UknownType"+std::to_string(type);
		}
	}

	string getDataFormString(DATA_FORM form) const {
		if(form >= 0 && form < 9)
			return arrFormStr[form];
		else
			return "UknownForm"+std::to_string(form);
	}

	string getTableTypeString(TABLE_TYPE type) const {
		if(type >= 0 && type < 10)
			return arrTableTypeStr[type];
		else
			return "UknownTable"+std::to_string(type);
	}

	Dictionary* createDictionary(DATA_TYPE keyInternalType, DATA_TYPE keyType, DATA_TYPE valueType){
		if(valueType>DT_STRING)
			valueType = DT_ANY;
		if(valueType != DT_ANY){
			switch(keyInternalType){
				case DT_BOOL :
				case DT_CHAR : return new CharDictionary(keyType,valueType);
				case DT_SHORT : return new ShortDictionary(keyType,valueType);
				case DT_INT : return new IntDictionary(keyType,valueType);
				case DT_LONG : return new LongDictionary(keyType,valueType);
				case DT_FLOAT : return new FloatDictionary(valueType);
				case DT_DOUBLE : return new DoubleDictionary(valueType);
				case DT_INT128 : return new Int128Dictionary(keyType,valueType);
				case DT_STRING : return new StringDictionary(keyType,valueType);
				default : return NULL;
			}
		}
		else{
			switch(keyInternalType){
				case DT_BOOL :
				case DT_CHAR :
				case DT_SHORT :
				case DT_INT : return new IntAnyDictionary(keyType);
				case DT_LONG : return new LongAnyDictionary(keyType);
				case DT_INT128 : return new Int128AnyDictionary(keyType);
				case DT_STRING : return new AnyDictionary();
				default : return NULL;
			}
		}
	}

	Set* createSet(DATA_TYPE keyInternalType, DATA_TYPE keyType, INDEX capacity){
		switch(keyInternalType){
			case DT_CHAR : return new CharSet(capacity);
			case DT_SHORT : return new ShortSet(capacity);
			case DT_INT : return new IntSet(keyType, capacity);
			case DT_LONG : return new LongSet(keyType, capacity);
			case DT_FLOAT : return new FloatSet(capacity);
			case DT_DOUBLE : return new DoubleSet(capacity);
			case DT_STRING : return new StringSet(capacity);
			case DT_INT128 : return new Int128Set(keyType, capacity);
			default : return NULL;
		}
	}

	string getPartitionTypeString(PARTITION_TYPE type) const {
		if(type >= 0 && type < 6)
			return arrPartitionTypeStr[type];
		else
			return "";
	}

	string getCategoryString(DATA_CATEGORY type) const {
		if(type >= 0 && type < 9)
			return arrCategoryTypeStr[type];
		else
			return "";
	} 
private:
	int getSegmentCount(INDEX size, int segmentSizeInBit) const {
		return (size >> segmentSizeInBit) + (size & ((1 << segmentSizeInBit) - 1) ? 1 : 0);
	}

	template<typename Y>
	void allocate(INDEX size, INDEX capacity, bool fastMode, int& segmentSizeInBit, void*& data, void**& dataSegment){
		try{
			data = (void*)new Y[(std::max)(size, capacity)];
		}
		catch(...){
			data = NULL;
		}
	}

	Constant* parseVoid(const string& word){return new Void(true);}
	Constant* parseBool(const string& word){return Bool::parseBool(word);}
	Constant* parseChar(const string& word){return Char::parseChar(word);}
	Constant* parseShort(const string& word){return Short::parseShort(word);}
	Constant* parseInt(const string& word){return Int::parseInt(word);}
	Constant* parseLong(const string& word){return Long::parseLong(word);}
	Constant* parseFloat(const string& word){return Float::parseFloat(word);}
	Constant* parseDouble(const string& word){return Double::parseDouble(word);}
	Constant* parseDate(const string& word) {return Date::parseDate(word);}
	Constant* parseDateTime(const string& word) {return DateTime::parseDateTime(word);}
	Constant* parseDateHour(const string& word) {return DateHour::parseDateHour(word);}
	Constant* parseMonth(const string& word) {return Month::parseMonth(word);}
	Constant* parseTime(const string& word) {return Time::parseTime(word);}
	Constant* parseNanoTime(const string& word) {return NanoTime::parseNanoTime(word);}
	Constant* parseTimestamp(const string& word) {return Timestamp::parseTimestamp(word);}
	Constant* parseNanoTimestamp(const string& word) {return NanoTimestamp::parseNanoTimestamp(word);}
	Constant* parseMinute(const string& word) {return Minute::parseMinute(word);}
	Constant* parseSecond(const string& word) {return Second::parseSecond(word);}
	Constant* parseString(const string& word){return new String(word);}
	Constant* parseInt128(const string& word){return Int128::parseInt128(word.c_str(), word.size());}
	Constant* parseUuid(const string& word){return Uuid::parseUuid(word.c_str(), word.size());}
	Constant* parseIPAddr(const string& word){return IPAddr::parseIPAddr(word.c_str(), word.size());}
	Constant* parseDoubleEnum(const string& word){
		char ch=word[0];
		if(ch=='p')
			return new EnumDouble(word, MC_PI);
		else
			return new EnumDouble(word, MC_E);
	}

	Constant* createVoid(){return new Void();}
	Constant* createBool(){return new Bool();}
	Constant* createChar(){return new Char();}
	Constant* createShort(){return new Short();}
	Constant* createInt(){return new Int();}
	Constant* createLong(){return new Long();}
	Constant* createFloat(){return new Float();}
	Constant* createDouble(){return new Double();}
	Constant* createDate() {return new Date();}
	Constant* createDateTime() {return new DateTime();}
	Constant* createDateHour() {return new DateHour();}
	Constant* createMonth() {return new Month();}
	Constant* createTime() {return new Time();}
	Constant* createNanoTime() {return new NanoTime();}
	Constant* createTimestamp() {return new Timestamp();}
	Constant* createNanoTimestamp() {return new NanoTimestamp();}
	Constant* createMinute() {return new Minute();}
	Constant* createSecond() {return new Second();}
	Constant* createString(){return new String();}
    Constant* createBlob(){return new String("", true);}
	Constant* createFunctionDef(){return new String();}
	Constant* createHandle(){return new String();}
	Constant* createMetaCode(){return new String();}
	Constant* createDataSource(){return new String();}
	Constant* createResource(){return new String();}
	Constant* createInt128(){return new Int128();}
	Constant* createUuid(){return new Uuid();}
	Constant* createIPAddr(){return new IPAddr();}

	Vector* createVoidVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		throw RuntimeException("Not allowed to create void vector");
	}

	Vector* createBoolVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<char>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		if(data != NULL)
			return new FastBoolVector(size, capacity, (char*)data, containNull);
		else
			return NULL;
	}
	Vector* createCharVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<char>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		if(data != NULL)
			return new FastCharVector(size, capacity, (char*)data, containNull);
		else
			return NULL;
	}
	Vector* createShortVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<short>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		if(data != NULL)
			return new FastShortVector(size, capacity, (short*)data, containNull);
		else
			return NULL;
	}
	Vector* createIntVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<int>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		if(data != NULL)
			return new FastIntVector(size, capacity, (int*)data, containNull);
		else
			return NULL;
	}
	Vector* createLongVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<long long>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		if(data != NULL)
			return new FastLongVector(size, capacity, (long long*)data, containNull);
		else
			return NULL;
	}
	Vector* createFloatVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<float>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		if(data != NULL)
			return new FastFloatVector(size, capacity, (float*)data, containNull);
		else
			return NULL;
	}
	Vector* createDoubleVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<double>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		if(data != NULL)
			return new FastDoubleVector(size, capacity, (double*)data, containNull);
		else
			return NULL;
	}
	Vector* createDateVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<int>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		if(data != NULL)
			return new FastDateVector(size, capacity, (int*)data, containNull);
		else
			return NULL;
	}
	Vector* createMonthVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<int>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		if(data != NULL)
			return new FastMonthVector(size, capacity, (int*)data, containNull);
		else
			return NULL;
	}
	Vector* createDateTimeVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<int>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		if(data != NULL)
			return new FastDateTimeVector(size, capacity, (int*)data, containNull);
		else
			return NULL;
	}
	Vector* createDateHourVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
        if(data == NULL && dataSegment == NULL){
            allocate<int>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
        }
        if(data != NULL)
            return new FastDateHourVector(size, capacity, (int*)data, containNull);
        else
            return NULL;
    }
	Vector* createTimeVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<int>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		if(data != NULL)
			return new FastTimeVector(size, capacity, (int*)data, containNull);
		else
			return NULL;
	}
	Vector* createNanoTimeVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<long long>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		if(data != NULL)
			return new FastNanoTimeVector(size, capacity, (long long*)data, containNull);
		else
			return NULL;
	}
	Vector* createTimestampVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<long long>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		if(data != NULL)
			return new FastTimestampVector(size, capacity, (long long*)data, containNull);
		else
			return NULL;
	}
	Vector* createNanoTimestampVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<long long>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		if(data != NULL)
			return new FastNanoTimestampVector(size, capacity, (long long*)data, containNull);
		else
			return NULL;
	}
	Vector* createMinuteVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<int>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		if(data != NULL)
			return new FastMinuteVector(size, capacity, (int*)data, containNull);
		else
			return NULL;
	}
	Vector* createSecondVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<int>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		if(data != NULL)
			return new FastSecondVector(size, capacity, (int*)data, containNull);
		else
			return NULL;
	}
	Vector* createStringVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		return new StringVector(size, capacity);
	}
	Vector* createSymbolVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data){
			throw RuntimeException("data must be null if create a symbol vector without a symbolbase. ");
		}
		if(data == NULL && dataSegment == NULL){
			allocate<int>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
			memset(data, 0, sizeof(int) * size);
		}
		if(data != NULL)
			return new FastSymbolVector(new SymbolBase(0), size, capacity, (int*)data, containNull);
		else
			return NULL;
	}
    Vector* createBlobVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
        return new StringVector(size, capacity, true);
    }
	Vector* createAnyVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		return new AnyVector(size);
	}
	Vector* createInt128Vector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<Guid>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		if(data != NULL)
			return new FastInt128Vector(DT_INT128, size, capacity, (unsigned char*)data, containNull);
		else
			return NULL;
	}
	Vector* createUuidVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<Guid>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		if(data != NULL)
			return new FastUuidVector(size, capacity, (unsigned char*)data, containNull);
		else
			return NULL;
	}
	Vector* createIPAddrVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<Guid>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		if(data != NULL)
			return new FastIPAddrVector(size, capacity, (unsigned char*)data, containNull);
		else
			return NULL;
	}

	Vector* createBoolArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void *data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull) {
		if(data == NULL && dataSegment == NULL){
			allocate<char>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_BOOL + ARRAY_TYPE_BASE), (INDEX *)pindex);

	}
	Vector* createCharArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void *data, void *pindex,  void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<char>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_CHAR + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createShortArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void *data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<short>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_SHORT + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createIntArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void *data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<int>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_INT + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createLongArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void *data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<long long>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_LONG + ARRAY_TYPE_BASE), (INDEX *)pindex);

	}
	Vector* createFloatArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void *data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<float>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_FLOAT + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createDoubleArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void *data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<double>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_DOUBLE + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createDateArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void *data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<int>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_DATE + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createTimestampArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void *data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<long long>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_TIMESTAMP + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createDateHourArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void *data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<int>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_DATEHOUR + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createDatetimeArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void *data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<int>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_DATETIME + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createTimeArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<int>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_TIME + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createMinuteArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<int>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_MINUTE + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createMonthArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<int>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_MONTH + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createSecondArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<int>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_SECOND + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createNanotimeArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<long long>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_NANOTIME + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createNanoTimestampArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == NULL && dataSegment == NULL){
			allocate<long long>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_NANOTIMESTAMP + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}

	Vector* createVoidMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		throw RuntimeException("Not allowed to create a void matrix");
	}

	Vector* createBoolMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastBoolMatrix(cols, rows, colCapacity,  new char[colCapacity * rows], false);
		else
			return new FastBoolMatrix(cols, rows, colCapacity, (char*)data, containNull);
	}

	Vector* createCharMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastCharMatrix(cols, rows, colCapacity,  new char[colCapacity * rows], false);
		else
			return new FastCharMatrix(cols, rows, colCapacity, (char*)data, containNull);
	}

	Vector* createShortMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastShortMatrix(cols, rows, colCapacity,  new short[colCapacity * rows], false);
		else
			return new FastShortMatrix(cols, rows, colCapacity, (short*)data, containNull);
	}

	Vector* createIntMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastIntMatrix(cols, rows, colCapacity,  new int[colCapacity * rows], false);
		else
			return new FastIntMatrix(cols, rows, colCapacity, (int*)data, containNull);
	}

	Vector* createLongMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastLongMatrix(cols, rows, colCapacity,  new long long[colCapacity * rows], false);
		else
			return new FastLongMatrix(cols, rows, colCapacity, (long long*)data, containNull);
	}

	Vector* createFloatMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data ==0)
			return new FastFloatMatrix(cols, rows, colCapacity,  new float[colCapacity * rows], false);
		else
			return new FastFloatMatrix(cols, rows, colCapacity, (float*)data, containNull);
	}

	Vector* createDoubleMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastDoubleMatrix(cols, rows, colCapacity,  new double[colCapacity * rows], false);
		else
			return new FastDoubleMatrix(cols, rows, colCapacity, (double*)data, containNull);
	}

	Vector* createDateMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastDateMatrix(cols, rows, colCapacity,  new int[colCapacity * rows], false);
		else
			return new FastDateMatrix(cols, rows, colCapacity, (int*)data, containNull);
	}

	Vector* createDateTimeMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastDateTimeMatrix(cols, rows, colCapacity, new int[colCapacity * rows], false);
		else
			return new FastDateTimeMatrix(cols, rows, colCapacity, (int*)data, containNull);
	}

	Vector* createDateHourMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
        if(data == 0)
            return new FastDateHourMatrix(cols, rows, colCapacity, new int[colCapacity * rows], false);
        else
            return new FastDateHourMatrix(cols, rows, colCapacity, (int*)data, containNull);
    }

	Vector* createMonthMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastMonthMatrix(cols, rows, colCapacity,  new int[colCapacity * rows], false);
		else
			return new FastMonthMatrix(cols, rows, colCapacity, (int*)data, containNull);
	}

	Vector* createTimeMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastTimeMatrix(cols, rows, colCapacity,  new int[colCapacity * rows], false);
		else
			return new FastTimeMatrix(cols, rows, colCapacity, (int*)data, containNull);
	}

	Vector* createSecondMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastSecondMatrix(cols, rows, colCapacity,  new int[colCapacity * rows], false);
		else
			return new FastSecondMatrix(cols, rows, colCapacity, (int*)data, containNull);
	}

	Vector* createMinuteMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastMinuteMatrix(cols, rows, colCapacity,  new int[colCapacity * rows], false);
		else
			return new FastMinuteMatrix(cols, rows, colCapacity, (int*)data, containNull);
	}

	Vector* createNanoTimeMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastNanoTimeMatrix(cols, rows, colCapacity,  new long long[colCapacity * rows], false);
		else
			return new FastNanoTimeMatrix(cols, rows, colCapacity, (long long*)data, containNull);
	}

	Vector* createTimestampMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastTimestampMatrix(cols, rows, colCapacity,  new long long[colCapacity * rows], false);
		else
			return new FastTimestampMatrix(cols, rows, colCapacity, (long long*)data, containNull);
	}

	Vector* createNanoTimestampMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastNanoTimestampMatrix(cols, rows, colCapacity,  new long long[colCapacity * rows], false);
		else
			return new FastNanoTimestampMatrix(cols, rows, colCapacity, (long long*)data, containNull);
	}

	void init(){
		arrConstParser[CONSTANT_VOID]=&ConstantFactory::parseVoid;
		arrConstParser[CONSTANT_BOOL]=&ConstantFactory::parseBool;
		arrConstParser[CONSTANT_CHAR]=&ConstantFactory::parseChar;
		arrConstParser[CONSTANT_SHORT]=&ConstantFactory::parseShort;
		arrConstParser[CONSTANT_INT]=&ConstantFactory::parseInt;
		arrConstParser[CONSTANT_LONG]=&ConstantFactory::parseLong;
		arrConstParser[CONSTANT_FLOAT]=&ConstantFactory::parseFloat;
		arrConstParser[CONSTANT_DOUBLE]=&ConstantFactory::parseDouble;
		arrConstParser[CONSTANT_DATE]=&ConstantFactory::parseDate;
		arrConstParser[CONSTANT_MONTH]=&ConstantFactory::parseMonth;
		arrConstParser[CONSTANT_DATETIME]=&ConstantFactory::parseDateTime;
		arrConstParser[CONSTANT_DATEHOUR]=&ConstantFactory::parseDateHour;
		arrConstParser[CONSTANT_TIME]=&ConstantFactory::parseTime;
		arrConstParser[CONSTANT_NANOTIME]=&ConstantFactory::parseNanoTime;
		arrConstParser[CONSTANT_TIMESTAMP]=&ConstantFactory::parseTimestamp;
		arrConstParser[CONSTANT_NANOTIMESTAMP]=&ConstantFactory::parseNanoTimestamp;
		arrConstParser[CONSTANT_MINUTE]=&ConstantFactory::parseMinute;
		arrConstParser[CONSTANT_SECOND]=&ConstantFactory::parseSecond;
		arrConstParser[CONSTANT_STRING]=&ConstantFactory::parseString;
		arrConstParser[CONSTANT_UUID]=&ConstantFactory::parseUuid;
		arrConstParser[CONSTANT_IP]=&ConstantFactory::parseIPAddr;
		arrConstParser[CONSTANT_INT128]=&ConstantFactory::parseInt128;
		arrConstParser[CONSTANT_DOUBLE_ENUM]=&ConstantFactory::parseDoubleEnum;

		arrConstFactory[DT_VOID]=&ConstantFactory::createVoid;
		arrConstFactory[DT_BOOL]=&ConstantFactory::createBool;
		arrConstFactory[DT_CHAR]=&ConstantFactory::createChar;
		arrConstFactory[DT_SHORT]=&ConstantFactory::createShort;
		arrConstFactory[DT_INT]=&ConstantFactory::createInt;
		arrConstFactory[DT_LONG]=&ConstantFactory::createLong;
		arrConstFactory[DT_FLOAT]=&ConstantFactory::createFloat;
		arrConstFactory[DT_DOUBLE]=&ConstantFactory::createDouble;
		arrConstFactory[DT_DATE]=&ConstantFactory::createDate;
		arrConstFactory[DT_MONTH]=&ConstantFactory::createMonth;
		arrConstFactory[DT_DATETIME]=&ConstantFactory::createDateTime;
		arrConstFactory[DT_DATEHOUR]=&ConstantFactory::createDateHour;
		arrConstFactory[DT_TIME]=&ConstantFactory::createTime;
		arrConstFactory[DT_NANOTIME]=&ConstantFactory::createNanoTime;
		arrConstFactory[DT_TIMESTAMP]=&ConstantFactory::createTimestamp;
		arrConstFactory[DT_NANOTIMESTAMP]=&ConstantFactory::createNanoTimestamp;
		arrConstFactory[DT_MINUTE]=&ConstantFactory::createMinute;
		arrConstFactory[DT_SECOND]=&ConstantFactory::createSecond;
		arrConstFactory[DT_SYMBOL]=&ConstantFactory::createString;
		arrConstFactory[DT_STRING]=&ConstantFactory::createString;
        arrConstFactory[DT_BLOB]=&ConstantFactory::createBlob;
		arrConstFactory[DT_FUNCTIONDEF]=&ConstantFactory::createFunctionDef;
		arrConstFactory[DT_HANDLE]=&ConstantFactory::createHandle;
		arrConstFactory[DT_CODE]=&ConstantFactory::createMetaCode;
		arrConstFactory[DT_DATASOURCE]=&ConstantFactory::createDataSource;
		arrConstFactory[DT_RESOURCE]=&ConstantFactory::createResource;
		arrConstFactory[DT_ANY]=&ConstantFactory::createVoid;
		arrConstFactory[DT_INT128]=&ConstantFactory::createInt128;
		arrConstFactory[DT_UUID]=&ConstantFactory::createUuid;
		arrConstFactory[DT_IP]=&ConstantFactory::createIPAddr;
		arrConstFactory[DT_DICTIONARY]=NULL;

		arrConstVectorFactory[DT_VOID]=&ConstantFactory::createVoidVector;
		arrConstVectorFactory[DT_BOOL]=&ConstantFactory::createBoolVector;
		arrConstVectorFactory[DT_CHAR]=&ConstantFactory::createCharVector;
		arrConstVectorFactory[DT_SHORT]=&ConstantFactory::createShortVector;
		arrConstVectorFactory[DT_INT]=&ConstantFactory::createIntVector;
		arrConstVectorFactory[DT_LONG]=&ConstantFactory::createLongVector;
		arrConstVectorFactory[DT_FLOAT]=&ConstantFactory::createFloatVector;
		arrConstVectorFactory[DT_DOUBLE]=&ConstantFactory::createDoubleVector;
		arrConstVectorFactory[DT_DATE]=&ConstantFactory::createDateVector;
		arrConstVectorFactory[DT_MONTH]=&ConstantFactory::createMonthVector;
		arrConstVectorFactory[DT_DATETIME]=&ConstantFactory::createDateTimeVector;
		arrConstVectorFactory[DT_DATEHOUR]=&ConstantFactory::createDateHourVector;
		arrConstVectorFactory[DT_TIME]=&ConstantFactory::createTimeVector;
		arrConstVectorFactory[DT_NANOTIME]=&ConstantFactory::createNanoTimeVector;
		arrConstVectorFactory[DT_TIMESTAMP]=&ConstantFactory::createTimestampVector;
		arrConstVectorFactory[DT_NANOTIMESTAMP]=&ConstantFactory::createNanoTimestampVector;
		arrConstVectorFactory[DT_MINUTE]=&ConstantFactory::createMinuteVector;
		arrConstVectorFactory[DT_SECOND]=&ConstantFactory::createSecondVector;
		arrConstVectorFactory[DT_STRING]=&ConstantFactory::createStringVector;
		arrConstVectorFactory[DT_SYMBOL]=&ConstantFactory::createSymbolVector;
		arrConstVectorFactory[DT_BLOB]=&ConstantFactory::createBlobVector;
		arrConstVectorFactory[DT_INT128]=&ConstantFactory::createInt128Vector;
		arrConstVectorFactory[DT_UUID]=&ConstantFactory::createUuidVector;
		arrConstVectorFactory[DT_IP]=&ConstantFactory::createIPAddrVector;
		arrConstVectorFactory[DT_FUNCTIONDEF]=&ConstantFactory::createAnyVector;
		arrConstVectorFactory[DT_HANDLE]=&ConstantFactory::createAnyVector;
		arrConstVectorFactory[DT_ANY]=&ConstantFactory::createAnyVector;
		arrConstVectorFactory[DT_DICTIONARY]=NULL;
		arrConstVectorFactory[DT_CODE]=&ConstantFactory::createAnyVector;
		arrConstVectorFactory[DT_RESOURCE]=&ConstantFactory::createAnyVector;
		arrConstVectorFactory[DT_DATASOURCE]=&ConstantFactory::createAnyVector;
		arrConstVectorFactory[DT_COMPRESS]=NULL;

		arrConstArrayVectorFactory[DT_BOOL] 	= &ConstantFactory::createBoolArrayVector;
		arrConstArrayVectorFactory[DT_CHAR] 	= &ConstantFactory::createCharArrayVector;
		arrConstArrayVectorFactory[DT_SHORT] 	= &ConstantFactory::createShortArrayVector;
		arrConstArrayVectorFactory[DT_INT] 		= &ConstantFactory::createIntArrayVector;
		arrConstArrayVectorFactory[DT_LONG] 	= &ConstantFactory::createLongArrayVector;
		arrConstArrayVectorFactory[DT_FLOAT] 	= &ConstantFactory::createFloatArrayVector;
		arrConstArrayVectorFactory[DT_DOUBLE] 	= &ConstantFactory::createDoubleArrayVector;
		arrConstArrayVectorFactory[DT_DATE] 	= &ConstantFactory::createDateArrayVector;
		arrConstArrayVectorFactory[DT_TIMESTAMP]= &ConstantFactory::createTimestampArrayVector;
		arrConstArrayVectorFactory[DT_DATEHOUR] = &ConstantFactory::createDateHourArrayVector;
		arrConstArrayVectorFactory[DT_DATETIME] = &ConstantFactory::createDatetimeArrayVector;
		arrConstArrayVectorFactory[DT_TIME] 	= &ConstantFactory::createTimeArrayVector;
		arrConstArrayVectorFactory[DT_MINUTE]	= &ConstantFactory::createMinuteArrayVector;
		arrConstArrayVectorFactory[DT_MONTH] 	= &ConstantFactory::createMonthArrayVector;
		arrConstArrayVectorFactory[DT_SECOND] 	= &ConstantFactory::createSecondArrayVector;
		arrConstArrayVectorFactory[DT_NANOTIME] = &ConstantFactory::createNanotimeArrayVector;
		arrConstArrayVectorFactory[DT_NANOTIMESTAMP] = &ConstantFactory::createNanoTimestampArrayVector;

		arrConstMatrixFactory[DT_VOID]=&ConstantFactory::createVoidMatrix;
		arrConstMatrixFactory[DT_BOOL]=&ConstantFactory::createBoolMatrix;
		arrConstMatrixFactory[DT_CHAR]=&ConstantFactory::createCharMatrix;
		arrConstMatrixFactory[DT_SHORT]=&ConstantFactory::createShortMatrix;
		arrConstMatrixFactory[DT_INT]=&ConstantFactory::createIntMatrix;
		arrConstMatrixFactory[DT_LONG]=&ConstantFactory::createLongMatrix;
		arrConstMatrixFactory[DT_FLOAT]=&ConstantFactory::createFloatMatrix;
		arrConstMatrixFactory[DT_DOUBLE]=&ConstantFactory::createDoubleMatrix;
		arrConstMatrixFactory[DT_DATE]=&ConstantFactory::createDateMatrix;
		arrConstMatrixFactory[DT_MONTH]=&ConstantFactory::createMonthMatrix;
		arrConstMatrixFactory[DT_DATETIME]=&ConstantFactory::createDateTimeMatrix;
		arrConstMatrixFactory[DT_DATEHOUR]=&ConstantFactory::createDateHourMatrix;
		arrConstMatrixFactory[DT_TIME]=&ConstantFactory::createTimeMatrix;
		arrConstMatrixFactory[DT_NANOTIME]=&ConstantFactory::createNanoTimeMatrix;
		arrConstMatrixFactory[DT_TIMESTAMP]=&ConstantFactory::createTimestampMatrix;
		arrConstMatrixFactory[DT_NANOTIMESTAMP]=&ConstantFactory::createNanoTimestampMatrix;
		arrConstMatrixFactory[DT_MINUTE]=&ConstantFactory::createMinuteMatrix;
		arrConstMatrixFactory[DT_SECOND]=&ConstantFactory::createSecondMatrix;
		arrConstMatrixFactory[DT_STRING]=NULL;
		arrConstMatrixFactory[DT_DICTIONARY]=NULL;
		arrConstMatrixFactory[DT_ANY]=NULL;

		typeMap_.insert(pair<string,DATA_TYPE>("void",DT_VOID));
		typeMap_.insert(pair<string,DATA_TYPE>("bool",DT_BOOL));
		typeMap_.insert(pair<string,DATA_TYPE>("char",DT_CHAR));
		typeMap_.insert(pair<string,DATA_TYPE>("short",DT_SHORT));
		typeMap_.insert(pair<string,DATA_TYPE>("int",DT_INT));
		typeMap_.insert(pair<string,DATA_TYPE>("long",DT_LONG));
		typeMap_.insert(pair<string,DATA_TYPE>("float",DT_FLOAT));
		typeMap_.insert(pair<string,DATA_TYPE>("double",DT_DOUBLE));
		typeMap_.insert(pair<string,DATA_TYPE>("date",DT_DATE));
		typeMap_.insert(pair<string,DATA_TYPE>("month",DT_MONTH));
		typeMap_.insert(pair<string,DATA_TYPE>("datetime",DT_DATETIME));
		typeMap_.insert(pair<string,DATA_TYPE>("datehour",DT_DATEHOUR));
		typeMap_.insert(pair<string,DATA_TYPE>("time",DT_TIME));
		typeMap_.insert(pair<string,DATA_TYPE>("nanotime",DT_NANOTIME));
		typeMap_.insert(pair<string,DATA_TYPE>("timestamp",DT_TIMESTAMP));
		typeMap_.insert(pair<string,DATA_TYPE>("nanotimestamp",DT_NANOTIMESTAMP));
		typeMap_.insert(pair<string,DATA_TYPE>("minute",DT_MINUTE));
		typeMap_.insert(pair<string,DATA_TYPE>("second",DT_SECOND));
		typeMap_.insert(pair<string,DATA_TYPE>("symbol",DT_SYMBOL));
		typeMap_.insert(pair<string,DATA_TYPE>("string",DT_STRING));
		typeMap_.insert(pair<string,DATA_TYPE>("any",DT_ANY));
		typeMap_.insert(pair<string,DATA_TYPE>("int128",DT_INT128));
		typeMap_.insert(pair<string,DATA_TYPE>("uuid",DT_UUID));
		typeMap_.insert(pair<string,DATA_TYPE>("ipaddr",DT_IP));
		typeMap_.insert(pair<string,DATA_TYPE>("dictionary",DT_DICTIONARY));

		formMap_.insert(pair<string,DATA_FORM>("scalar",DF_SCALAR));
		formMap_.insert(pair<string,DATA_FORM>("pair",DF_PAIR));
		formMap_.insert(pair<string,DATA_FORM>("vector",DF_VECTOR));
		formMap_.insert(pair<string,DATA_FORM>("matrix",DF_MATRIX));
		formMap_.insert(pair<string,DATA_FORM>("set",DF_SET));
		formMap_.insert(pair<string,DATA_FORM>("dictionary",DF_DICTIONARY));
		formMap_.insert(pair<string,DATA_FORM>("table",DF_TABLE));
		formMap_.insert(pair<string,DATA_FORM>("chart",DF_CHART));
		formMap_.insert(pair<string,DATA_FORM>("chunk",DF_CHUNK));

		arrTypeSymbol[DT_VOID]=' ';
		arrTypeSymbol[DT_BOOL]='b';
		arrTypeSymbol[DT_CHAR]='c';
		arrTypeSymbol[DT_SHORT]='h';
		arrTypeSymbol[DT_INT]='i';
		arrTypeSymbol[DT_LONG]='l';
		arrTypeSymbol[DT_FLOAT]='f';
		arrTypeSymbol[DT_DOUBLE]='F';
		arrTypeSymbol[DT_DATE]='d';
		arrTypeSymbol[DT_MONTH]='M';
		arrTypeSymbol[DT_DATETIME]='D';
		arrTypeSymbol[DT_DATEHOUR]=' ';
		arrTypeSymbol[DT_TIME]='t';
		arrTypeSymbol[DT_TIMESTAMP]='T';
		arrTypeSymbol[DT_NANOTIME]='n';
		arrTypeSymbol[DT_NANOTIMESTAMP]='N';
		arrTypeSymbol[DT_MINUTE]='m';
		arrTypeSymbol[DT_SECOND]='s';
		arrTypeSymbol[DT_SYMBOL]='W';
		arrTypeSymbol[DT_STRING]='S';
		arrTypeSymbol[DT_FUNCTIONDEF]=' ';
		arrTypeSymbol[DT_HANDLE]=' ';
		arrTypeSymbol[DT_DATASOURCE]=' ';
		arrTypeSymbol[DT_RESOURCE]=' ';
		arrTypeSymbol[DT_ANY]=' ';
		arrTypeSymbol[DT_INT128]=' ';
		arrTypeSymbol[DT_UUID]=' ';
		arrTypeSymbol[DT_IP]=' ';
		arrTypeSymbol[DT_DICTIONARY]=' ';

		arrTypeStr[DT_VOID]="VOID";
		arrTypeStr[DT_BOOL]="BOOL";
		arrTypeStr[DT_CHAR]="CHAR";
		arrTypeStr[DT_SHORT]="SHORT";
		arrTypeStr[DT_INT]="INT";
		arrTypeStr[DT_LONG]="LONG";
		arrTypeStr[DT_FLOAT]="FLOAT";
		arrTypeStr[DT_DOUBLE]="DOUBLE";
		arrTypeStr[DT_DATE]="DATE";
		arrTypeStr[DT_MONTH]="MONTH";
		arrTypeStr[DT_DATETIME]="DATETIME";
		arrTypeStr[DT_DATEHOUR]="DATEHOUR";
		arrTypeStr[DT_TIME]="TIME";
		arrTypeStr[DT_TIMESTAMP]="TIMESTAMP";
		arrTypeStr[DT_NANOTIME]="NANOTIME";
		arrTypeStr[DT_NANOTIMESTAMP]="NANOTIMESTAMP";
		arrTypeStr[DT_MINUTE]="MINUTE";
		arrTypeStr[DT_SECOND]="SECOND";
		arrTypeStr[DT_SYMBOL]="SYMBOL";
		arrTypeStr[DT_STRING]="STRING";
		arrTypeStr[DT_FUNCTIONDEF]="FUNCTIONDEF";
		arrTypeStr[DT_HANDLE]="HANDLE";
		arrTypeStr[DT_DATASOURCE]="DATASOURCE";
		arrTypeStr[DT_RESOURCE]="RESOURCE";
		arrTypeStr[DT_ANY]="ANY";
		arrTypeStr[DT_INT128]="INT128";
		arrTypeStr[DT_UUID]="UUID";
		arrTypeStr[DT_IP]="IPADDR";
		arrTypeStr[DT_DICTIONARY]="DICTIONARY";
		arrTypeStr[DT_CODE]="CODE";

		arrFormStr[DF_SCALAR]="SCALAR";
		arrFormStr[DF_PAIR]="PAIR";
		arrFormStr[DF_VECTOR]="VECTOR";
		arrFormStr[DF_MATRIX]="MATRIX";
		arrFormStr[DF_TABLE]="TABLE";
		arrFormStr[DF_SET]="SET";
		arrFormStr[DF_DICTIONARY]="DICTIONARY";
		arrFormStr[DF_CHART]="CHART";
		arrFormStr[DF_CHUNK]="CHUNK";

		arrTableTypeStr[BASICTBL] = "BASIC";
		arrTableTypeStr[REALTIMETBL] = "REALTIME";
		arrTableTypeStr[SNAPTBL] = "SNAPSHOT";
		arrTableTypeStr[FILETBL] = "FILE";
		arrTableTypeStr[CHUNKTBL] = "CHUNK";

		arrTableTypeStr[JOINTBL] = "JOIN";
		arrTableTypeStr[SEGTBL] = "SEGMENT";
		arrTableTypeStr[ALIASTBL] = "ALIAS";
		arrTableTypeStr[COMPRESSTBL] = "COMPRESS";
		arrTableTypeStr[LOGROWTBL] = "LOGROW";

		arrPartitionTypeStr[SEQ] = "SEQ";
		arrPartitionTypeStr[VALUE] = "VALUE";
		arrPartitionTypeStr[RANGE] = "RANGE";
		arrPartitionTypeStr[LIST] = "LIST";
		arrPartitionTypeStr[HASH] = "HASH";
		arrPartitionTypeStr[HIER] = "HIER";

		arrCategoryTypeStr[NOTHING] = "NOTHING";
		arrCategoryTypeStr[LOGICAL] = "LOGICAL";
		arrCategoryTypeStr[INTEGRAL] = "INTEGRAL";
		arrCategoryTypeStr[FLOATING] = "FLOATING";
		arrCategoryTypeStr[TEMPORAL] = "TEMPORAL";
		arrCategoryTypeStr[LITERAL] = "LITERAL";
		arrCategoryTypeStr[SYSTEM] = "SYSTEM";
		arrCategoryTypeStr[MIXED] = "MIXED";
		arrCategoryTypeStr[BINARY] = "BINARY";
	}

	ConstantParser arrConstParser[TYPE_COUNT];
	ConstantFunc arrConstFactory[TYPE_COUNT];
	ConstantVectorFunc arrConstVectorFactory[TYPE_COUNT];
	ConstantArrayVectorFunc arrConstArrayVectorFactory[TYPE_COUNT];
	ConstantMatrixFunc arrConstMatrixFactory[TYPE_COUNT];
	ConstantRptVectorFunc arrConstRptVectorFactory[TYPE_COUNT];
	unordered_map<string,DATA_TYPE> typeMap_;
	unordered_map<string,DATA_FORM> formMap_;
	char arrTypeSymbol[TYPE_COUNT];
	string arrTypeStr[TYPE_COUNT];
	string arrFormStr[9];
	string arrTableTypeStr[10];
	string arrPartitionTypeStr[6];
	string arrCategoryTypeStr[9];
};

};
#endif /* CONSTANTFACTORY_H_ */
