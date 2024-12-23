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

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4100 )
#endif

namespace dolphindb {

class ConstantFactory{
	typedef Constant*(ConstantFactory::*ConstantParser)(const std::string&);
	typedef Constant*(ConstantFactory::*ConstantFunc)(int extraParam);
	typedef Vector*(ConstantFactory::*ConstantVectorFunc)(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull);
	typedef Vector*(ConstantFactory::*ConstantArrayVectorFunc)(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull);
	typedef Vector*(ConstantFactory::*ConstantMatrixFunc)(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull);
	typedef Vector*(ConstantFactory::*ConstantRptVectorFunc)(const ConstantSP& scalar, INDEX size);
public:
	ConstantFactory()
		: arrConstParser{}, arrConstFactory{}, arrConstVectorFactory{}, arrConstArrayVectorFactory{}, arrConstMatrixFactory{}, arrTypeSymbol{}
	{
		init();
	}

	Constant* parseConstant(int type, const std::string& word){
		if(type < 0 || type >= TYPE_COUNT)
			throw RuntimeException("Invalid data type value " + Util::getDataTypeString((DATA_TYPE)type));
		ConstantParser func=arrConstParser[type];
		if(func==NULL)
			return NULL;
		else
			return (this->*func)(word);
	}

	Constant* createConstant(DATA_TYPE type, int extraParam){
		if(type <0 || type >= TYPE_COUNT)
			throw RuntimeException("Invalid data type value " + Util::getDataTypeString(type));
		ConstantFunc func=arrConstFactory[type];
		if(func==NULL)
			throw RuntimeException("Not allowed to create a scalar with type " + Util::getDataTypeString(type));
		else
			return (this->*func)(extraParam);
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

	Vector* createConstantMatrix(DATA_TYPE type,int cols, int rows, int colCapacity, int extraParam, void* data=0, void** dataSegment=0, int segmentSizeInBit=0, bool containNull=false){
		if(type <0 || type >= TYPE_COUNT)
			throw RuntimeException("Invalid Matrix data type value " + Util::getDataTypeString(type));
		ConstantMatrixFunc func=arrConstMatrixFactory[type];
		if(func==NULL)
			throw RuntimeException("Not allowed to create a matrix with type " + Util::getDataTypeString(type));
		else
			return (this->*func)(cols,rows,colCapacity,extraParam, data, dataSegment, segmentSizeInBit, containNull);
	}

	DATA_TYPE getDataType(const std::string& type) const {
		std::unordered_map<std::string,DATA_TYPE>::const_iterator it=typeMap_.find(type);
		if(it==typeMap_.end())
			return DT_VOID;
		else
			return it->second;
	}

	DATA_FORM getDataForm(const std::string& form) const {
		std::unordered_map<std::string,DATA_FORM>::const_iterator it=formMap_.find(form);
		if(it==formMap_.end())
			return (DATA_FORM)-1;
		else
			return it->second;
	}


	char getDataTypeSymbol(DATA_TYPE type) const {
		return arrTypeSymbol[type];
	}

	std::string getDataTypeString(DATA_TYPE type) const {
		return dolphindb::getDataTypeName(type);
	}

	std::string getDataFormString(DATA_FORM form) const {
		if(form >= 0 && form < 9)
			return arrFormStr[form];
		else
			return "Uknown data form " + std::to_string(form);
	}

	std::string getTableTypeString(TABLE_TYPE type) const {
		switch (type) {
		case BASICTBL: return "BASIC";
		case COMPRESSTBL: return "COMPRESS";
		default: return "Uknown table type " + std::to_string(type);
		}
	}

	Dictionary* createDictionary(DATA_TYPE keyInternalType, DATA_TYPE keyType, DATA_TYPE valueType){
		if (valueType > DT_STRING && (valueType!= DT_BLOB && valueType != DT_DATEHOUR && valueType != DT_DATEMINUTE)) {
			valueType = DT_ANY;
		}
		if(valueType != DT_ANY){
			switch(keyInternalType){
				case DT_CHAR : return new CharDictionary(keyType,valueType);
				case DT_SHORT : return new ShortDictionary(keyType,valueType);
				case DT_INT : return new IntDictionary(keyType,valueType);
				case DT_LONG : return new LongDictionary(keyType,valueType);
				case DT_FLOAT : return new FloatDictionary(valueType);
				case DT_DOUBLE : return new DoubleDictionary(valueType);
				case DT_INT128 : return new Int128Dictionary(keyType,valueType);
				case DT_STRING :
				case DT_BLOB : return new StringDictionary(keyType,valueType);
				default : throw RuntimeException("Not allowed to create Dictionary for the key type " + Util::getDataTypeString(keyType));
			}
		}
		else{
			switch(keyInternalType){
				case DT_CHAR :
				case DT_SHORT :
				case DT_INT : return new IntAnyDictionary(keyType);
				case DT_LONG : return new LongAnyDictionary(keyType);
				case DT_FLOAT: return new FloatAnyDictionary(keyType);
				case DT_DOUBLE: return new DoubleAnyDictionary(keyType);
				case DT_INT128 : return new Int128AnyDictionary(keyType);
				case DT_STRING :
				case DT_BLOB : return new AnyDictionary();
				default : throw RuntimeException("Not allowed to create Dictionary for this key type " + Util::getDataTypeString(keyType));
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
			case DT_SYMBOL : return new StringSet(capacity, false, true);
			case DT_STRING : return new StringSet(capacity, false);
			case DT_INT128 : return new Int128Set(keyType, capacity);
			case DT_BLOB: return new StringSet(capacity, true);
			default : return NULL;
		}
	}

	std::string getPartitionTypeString(PARTITION_TYPE type) const {
		if(type >= 0 && type < 6)
			return arrPartitionTypeStr[type];
		else
			return "UnknowPartition"+std::to_string(type);
	}

	std::string getCategoryString(DATA_CATEGORY type) const {
		if(type >= 0 && type < 12)
			return arrCategoryTypeStr[type];
		else
			return "UnknowCategory"+std::to_string(type);
	} 
private:
	template<typename Y>
	void allocate(INDEX size, INDEX capacity, bool fastMode, int& segmentSizeInBit, void*& data, void**& dataSegment){
		try{
			data = (void*)new Y[(std::max)(size, capacity)];
		}
		catch(...){
			data = NULL;
		}
	}
	template<typename Y>
	Y* allocateZero(int cols, int rows, int colCapacity) {
		try {
			long size = (long)(std::max)(cols, colCapacity) * (long)rows;
			Y* data = new Y[size];
			memset(data, 0, sizeof(Y)*size);
			return data;
		}
		catch (...) {
			return NULL;
		}
	}

	Constant* parseVoid(const std::string& word){return new Void(true);}
	Constant* parseBool(const std::string& word){return Bool::parseBool(word);}
	Constant* parseChar(const std::string& word){return Char::parseChar(word);}
	Constant* parseShort(const std::string& word){return Short::parseShort(word);}
	Constant* parseInt(const std::string& word){return Int::parseInt(word);}
	Constant* parseLong(const std::string& word){return Long::parseLong(word);}
	Constant* parseFloat(const std::string& word){return Float::parseFloat(word);}
	Constant* parseDouble(const std::string& word){return Double::parseDouble(word);}
	Constant* parseDate(const std::string& word) {return Date::parseDate(word);}
	Constant* parseDateTime(const std::string& word) {return DateTime::parseDateTime(word);}
	Constant* parseDateHour(const std::string& word) {return DateHour::parseDateHour(word);}
	Constant* parseMonth(const std::string& word) {return Month::parseMonth(word);}
	Constant* parseTime(const std::string& word) {return Time::parseTime(word);}
	Constant* parseNanoTime(const std::string& word) {return NanoTime::parseNanoTime(word);}
	Constant* parseTimestamp(const std::string& word) {return Timestamp::parseTimestamp(word);}
	Constant* parseNanoTimestamp(const std::string& word) {return NanoTimestamp::parseNanoTimestamp(word);}
	Constant* parseMinute(const std::string& word) {return Minute::parseMinute(word);}
	Constant* parseSecond(const std::string& word) {return Second::parseSecond(word);}
	Constant* parseString(const std::string& word){return new String(word);}
	Constant* parseInt128(const std::string& word){return Int128::parseInt128(word.c_str(), word.size());}
	Constant* parseUuid(const std::string& word){return Uuid::parseUuid(word.c_str(), word.size());}
	Constant* parseIPAddr(const std::string& word){return IPAddr::parseIPAddr(word.c_str(), word.size());}
	Constant* parseDoubleEnum(const std::string& word){
		char ch=word[0];
		if(ch=='p')
			return new EnumDouble(word, MC_PI);
		else
			return new EnumDouble(word, MC_E);
	}

	Constant* createVoid(int extraParam){return new Void();}
	Constant* createBool(int extraParam){return new Bool();}
	Constant* createChar(int extraParam){return new Char();}
	Constant* createShort(int extraParam){return new Short();}
	Constant* createInt(int extraParam){return new Int();}
	Constant* createLong(int extraParam){return new Long();}
	Constant* createFloat(int extraParam){return new Float();}
	Constant* createDouble(int extraParam){return new Double();}
	Constant* createDate(int extraParam) {return new Date();}
	Constant* createDateTime(int extraParam) {return new DateTime();}
	Constant* createDateHour(int extraParam) {return new DateHour();}
	Constant* createMonth(int extraParam) {return new Month();}
	Constant* createTime(int extraParam) {return new Time();}
	Constant* createNanoTime(int extraParam) {return new NanoTime();}
	Constant* createTimestamp(int extraParam) {return new Timestamp();}
	Constant* createNanoTimestamp(int extraParam) {return new NanoTimestamp();}
	Constant* createMinute(int extraParam) {return new Minute();}
	Constant* createSecond(int extraParam) {return new Second();}
	Constant* createString(int extraParam){return new String();}
    Constant* createBlob(int extraParam){return new String("", true);}
	Constant* createFunctionDef(int extraParam){return new String();}
	Constant* createHandle(int extraParam){return new String();}
	Constant* createMetaCode(int extraParam){return new String();}
	Constant* createDataSource(int extraParam){return new String();}
	Constant* createResource(int extraParam){return new String();}
	Constant* createInt128(int extraParam){return new Int128();}
	Constant* createUuid(int extraParam){return new Uuid();}
	Constant* createIPAddr(int extraParam){return new IPAddr();}
	Constant* createDecima32(int extraParam) { return new Decimal32(extraParam); }
	Constant* createDecima64(int extraParam) { return new Decimal64(extraParam); }
	Constant* createDecima128(int extraParam) { return new Decimal128(extraParam); }

	Vector* createVoidVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		// throw RuntimeException("Not allowed to create void vector");
		return new FastVoidVector(size, capacity, (char*)data, containNull);
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
	Vector* createDecimal32Vector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull) {
		if (data == NULL && dataSegment == NULL) {
			allocate<int32_t>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		if (data != NULL)
			return new FastDecimalVector<int32_t>(extraParam, size, capacity, (int32_t*)data, containNull);
		else
			return NULL;
	}
	Vector* createDecimal64Vector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull) {
		if (data == NULL && dataSegment == NULL) {
			allocate<int64_t>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		if (data != NULL)
			return new FastDecimalVector<int64_t>(extraParam, size, capacity, (int64_t*)data, containNull);
		else
			return NULL;
	}

	Vector* createDecimal128Vector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull) {
		if (data == NULL && dataSegment == NULL) {
			allocate<wide_integer::int128>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		}
		if (data != NULL)
			return new FastDecimalVector<wide_integer::int128>(extraParam, size, capacity, (wide_integer::int128*)data, containNull);
		else
			return NULL;
	}

	Vector* createBoolArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void *data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull) {
		//if(data == NULL && dataSegment == NULL){
		//	allocate<char>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		//}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_BOOL + ARRAY_TYPE_BASE), (INDEX *)pindex);

	}
	Vector* createCharArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void *data, void *pindex,  void** dataSegment, int segmentSizeInBit, bool containNull){
		//if(data == NULL && dataSegment == NULL){
		//	allocate<char>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		//}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_CHAR + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createShortArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void *data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		//if(data == NULL && dataSegment == NULL){
		//	allocate<short>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		//}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_SHORT + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createIntArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void *data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		//if(data == NULL && dataSegment == NULL){
		//	allocate<int>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		//}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_INT + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createLongArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void *data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		//if(data == NULL && dataSegment == NULL){
		//	allocate<long long>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		//}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_LONG + ARRAY_TYPE_BASE), (INDEX *)pindex);

	}
	Vector* createFloatArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void *data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		//if(data == NULL && dataSegment == NULL){
		//	allocate<float>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		//}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_FLOAT + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createDoubleArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void *data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		//if(data == NULL && dataSegment == NULL){
		//	allocate<double>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		//}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_DOUBLE + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createDateArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void *data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		//if(data == NULL && dataSegment == NULL){
		//	allocate<int>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		//}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_DATE + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createTimestampArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void *data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		//if(data == NULL && dataSegment == NULL){
		//	allocate<long long>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		//}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_TIMESTAMP + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createDateHourArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void *data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		//if(data == NULL && dataSegment == NULL){
		//	allocate<int>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		//}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_DATEHOUR + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createDatetimeArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void *data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		//if(data == NULL && dataSegment == NULL){
		//	allocate<int>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		//}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_DATETIME + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createTimeArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		//if(data == NULL && dataSegment == NULL){
		//	allocate<int>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		//}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_TIME + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createMinuteArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		//if(data == NULL && dataSegment == NULL){
		//	allocate<int>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		//}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_MINUTE + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createMonthArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		//if(data == NULL && dataSegment == NULL){
		//	allocate<int>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		//}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_MONTH + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createSecondArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		//if(data == NULL && dataSegment == NULL){
		//	allocate<int>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		//}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_SECOND + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createNanotimeArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		//if(data == NULL && dataSegment == NULL){
		//	allocate<long long>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		//}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_NANOTIME + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createNanoTimestampArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull){
		//if(data == NULL && dataSegment == NULL){
		//	allocate<long long>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		//}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_NANOTIMESTAMP + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createUuidArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull) {
		//if (data == NULL && dataSegment == NULL) {
		//	allocate<Guid>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		//}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_UUID + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createInt128ArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull) {
		//if (data == NULL && dataSegment == NULL) {
		//	allocate<Int128>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		//}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_INT128 + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}
	Vector* createIpArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull) {
		//if (data == NULL && dataSegment == NULL) {
		//	allocate<IPAddr>(size, capacity, fastMode, segmentSizeInBit, data, dataSegment);
		//}
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_IP + ARRAY_TYPE_BASE), (INDEX *)pindex);
	}

	Vector* createDecimal32ArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull) {
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_DECIMAL32 + ARRAY_TYPE_BASE), (INDEX *)pindex, extraParam);
	}

	Vector* createDecimal64ArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull) {
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_DECIMAL64 + ARRAY_TYPE_BASE), (INDEX *)pindex, extraParam);
	}

	Vector* createDecimal128ArrayVector(INDEX size, INDEX capacity, bool fastMode, int extraParam, void* data, void *pindex, void** dataSegment, int segmentSizeInBit, bool containNull) {
		return new FastArrayVector(size, capacity, (char*)data, containNull, DATA_TYPE(DT_DECIMAL128 + ARRAY_TYPE_BASE), (INDEX *)pindex, extraParam);
	}

	Vector* createVoidMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		throw RuntimeException("Not allowed to create a void matrix");
	}

	Vector* createBoolMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastBoolMatrix(cols, rows, colCapacity, allocateZero<char>(cols, rows, colCapacity), false);
		else
			return new FastBoolMatrix(cols, rows, colCapacity, (char*)data, containNull);
	}

	Vector* createCharMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastCharMatrix(cols, rows, colCapacity, allocateZero<char>(cols, rows, colCapacity), false);
		else
			return new FastCharMatrix(cols, rows, colCapacity, (char*)data, containNull);
	}

	Vector* createShortMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastShortMatrix(cols, rows, colCapacity, allocateZero<short>(cols, rows, colCapacity), false);
		else
			return new FastShortMatrix(cols, rows, colCapacity, (short*)data, containNull);
	}

	Vector* createIntMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastIntMatrix(cols, rows, colCapacity, allocateZero<int>(cols, rows, colCapacity), false);
		else
			return new FastIntMatrix(cols, rows, colCapacity, (int*)data, containNull);
	}

	Vector* createLongMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastLongMatrix(cols, rows, colCapacity, allocateZero<long long>(cols, rows, colCapacity), false);
		else
			return new FastLongMatrix(cols, rows, colCapacity, (long long*)data, containNull);
	}

	Vector* createFloatMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data ==0)
			return new FastFloatMatrix(cols, rows, colCapacity, allocateZero<float>(cols, rows, colCapacity), false);
		else
			return new FastFloatMatrix(cols, rows, colCapacity, (float*)data, containNull);
	}

	Vector* createDoubleMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastDoubleMatrix(cols, rows, colCapacity, allocateZero<double>(cols, rows, colCapacity), false);
		else
			return new FastDoubleMatrix(cols, rows, colCapacity, (double*)data, containNull);
	}

	Vector* createDateMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastDateMatrix(cols, rows, colCapacity, allocateZero<int>(cols, rows, colCapacity), false);
		else
			return new FastDateMatrix(cols, rows, colCapacity, (int*)data, containNull);
	}

	Vector* createDateTimeMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastDateTimeMatrix(cols, rows, colCapacity, allocateZero<int>(cols, rows, colCapacity), false);
		else
			return new FastDateTimeMatrix(cols, rows, colCapacity, (int*)data, containNull);
	}

	Vector* createDateHourMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
        if(data == 0)
            return new FastDateHourMatrix(cols, rows, colCapacity, allocateZero<int>(cols, rows, colCapacity), false);
        else
            return new FastDateHourMatrix(cols, rows, colCapacity, (int*)data, containNull);
    }

	Vector* createMonthMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastMonthMatrix(cols, rows, colCapacity, allocateZero<int>(cols, rows, colCapacity), false);
		else
			return new FastMonthMatrix(cols, rows, colCapacity, (int*)data, containNull);
	}

	Vector* createTimeMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastTimeMatrix(cols, rows, colCapacity, allocateZero<int>(cols, rows, colCapacity), false);
		else
			return new FastTimeMatrix(cols, rows, colCapacity, (int*)data, containNull);
	}

	Vector* createSecondMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastSecondMatrix(cols, rows, colCapacity, allocateZero<int>(cols, rows, colCapacity), false);
		else
			return new FastSecondMatrix(cols, rows, colCapacity, (int*)data, containNull);
	}

	Vector* createMinuteMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastMinuteMatrix(cols, rows, colCapacity, allocateZero<int>(cols, rows, colCapacity), false);
		else
			return new FastMinuteMatrix(cols, rows, colCapacity, (int*)data, containNull);
	}

	Vector* createNanoTimeMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastNanoTimeMatrix(cols, rows, colCapacity, allocateZero<long long>(cols, rows, colCapacity), false);
		else
			return new FastNanoTimeMatrix(cols, rows, colCapacity, (long long*)data, containNull);
	}

	Vector* createTimestampMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastTimestampMatrix(cols, rows, colCapacity, allocateZero<long long>(cols, rows, colCapacity), false);
		else
			return new FastTimestampMatrix(cols, rows, colCapacity, (long long*)data, containNull);
	}

	Vector* createNanoTimestampMatrix(int cols, int rows, int colCapacity, int extraParam, void* data, void** dataSegment, int segmentSizeInBit, bool containNull){
		if(data == 0)
			return new FastNanoTimestampMatrix(cols, rows, colCapacity, allocateZero<long long>(cols, rows, colCapacity), false);
		else
			return new FastNanoTimestampMatrix(cols, rows, colCapacity, (long long*)data, containNull);
	}

	void init(){
		arrConstParser[DT_VOID]=&ConstantFactory::parseVoid;
		arrConstParser[DT_BOOL]=&ConstantFactory::parseBool;
		arrConstParser[DT_CHAR]=&ConstantFactory::parseChar;
		arrConstParser[DT_SHORT]=&ConstantFactory::parseShort;
		arrConstParser[DT_INT]=&ConstantFactory::parseInt;
		arrConstParser[DT_LONG]=&ConstantFactory::parseLong;
		arrConstParser[DT_FLOAT]=&ConstantFactory::parseFloat;
		arrConstParser[DT_DOUBLE]=&ConstantFactory::parseDouble;
		arrConstParser[DT_DATE]=&ConstantFactory::parseDate;
		arrConstParser[DT_MONTH]=&ConstantFactory::parseMonth;
		arrConstParser[DT_DATETIME]=&ConstantFactory::parseDateTime;
		arrConstParser[DT_DATEHOUR]=&ConstantFactory::parseDateHour;
		arrConstParser[DT_TIME]=&ConstantFactory::parseTime;
		arrConstParser[DT_NANOTIME]=&ConstantFactory::parseNanoTime;
		arrConstParser[DT_TIMESTAMP]=&ConstantFactory::parseTimestamp;
		arrConstParser[DT_NANOTIMESTAMP]=&ConstantFactory::parseNanoTimestamp;
		arrConstParser[DT_MINUTE]=&ConstantFactory::parseMinute;
		arrConstParser[DT_SECOND]=&ConstantFactory::parseSecond;
		arrConstParser[DT_STRING]=&ConstantFactory::parseString;
		arrConstParser[DT_UUID]=&ConstantFactory::parseUuid;
		arrConstParser[DT_IP]=&ConstantFactory::parseIPAddr;
		arrConstParser[DT_INT128]=&ConstantFactory::parseInt128;
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
		arrConstFactory[DT_DECIMAL32] = &ConstantFactory::createDecima32;
		arrConstFactory[DT_DECIMAL64] = &ConstantFactory::createDecima64;
		arrConstFactory[DT_DECIMAL128] = &ConstantFactory::createDecima128;

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
		arrConstVectorFactory[DT_DECIMAL32] = &ConstantFactory::createDecimal32Vector;
		arrConstVectorFactory[DT_DECIMAL64] = &ConstantFactory::createDecimal64Vector;
		arrConstVectorFactory[DT_DECIMAL128] = &ConstantFactory::createDecimal128Vector;

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
		arrConstArrayVectorFactory[DT_UUID] = &ConstantFactory::createUuidArrayVector;
		arrConstArrayVectorFactory[DT_INT128] = &ConstantFactory::createInt128ArrayVector;
		arrConstArrayVectorFactory[DT_IP] = &ConstantFactory::createIpArrayVector;
		arrConstArrayVectorFactory[DT_DECIMAL32] = &ConstantFactory::createDecimal32ArrayVector;
		arrConstArrayVectorFactory[DT_DECIMAL64] = &ConstantFactory::createDecimal64ArrayVector;
		arrConstArrayVectorFactory[DT_DECIMAL128] = &ConstantFactory::createDecimal128ArrayVector;

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

		typeMap_.insert(std::pair<std::string,DATA_TYPE>("void",DT_VOID));
		typeMap_.insert(std::pair<std::string,DATA_TYPE>("bool",DT_BOOL));
		typeMap_.insert(std::pair<std::string,DATA_TYPE>("char",DT_CHAR));
		typeMap_.insert(std::pair<std::string,DATA_TYPE>("short",DT_SHORT));
		typeMap_.insert(std::pair<std::string,DATA_TYPE>("int",DT_INT));
		typeMap_.insert(std::pair<std::string,DATA_TYPE>("long",DT_LONG));
		typeMap_.insert(std::pair<std::string,DATA_TYPE>("float",DT_FLOAT));
		typeMap_.insert(std::pair<std::string,DATA_TYPE>("double",DT_DOUBLE));
		typeMap_.insert(std::pair<std::string,DATA_TYPE>("date",DT_DATE));
		typeMap_.insert(std::pair<std::string,DATA_TYPE>("month",DT_MONTH));
		typeMap_.insert(std::pair<std::string,DATA_TYPE>("datetime",DT_DATETIME));
		typeMap_.insert(std::pair<std::string,DATA_TYPE>("datehour",DT_DATEHOUR));
		typeMap_.insert(std::pair<std::string,DATA_TYPE>("time",DT_TIME));
		typeMap_.insert(std::pair<std::string,DATA_TYPE>("nanotime",DT_NANOTIME));
		typeMap_.insert(std::pair<std::string,DATA_TYPE>("timestamp",DT_TIMESTAMP));
		typeMap_.insert(std::pair<std::string,DATA_TYPE>("nanotimestamp",DT_NANOTIMESTAMP));
		typeMap_.insert(std::pair<std::string,DATA_TYPE>("minute",DT_MINUTE));
		typeMap_.insert(std::pair<std::string,DATA_TYPE>("second",DT_SECOND));
		typeMap_.insert(std::pair<std::string,DATA_TYPE>("symbol",DT_SYMBOL));
		typeMap_.insert(std::pair<std::string,DATA_TYPE>("std::string",DT_STRING));
		typeMap_.insert(std::pair<std::string,DATA_TYPE>("any",DT_ANY));
		typeMap_.insert(std::pair<std::string,DATA_TYPE>("int128",DT_INT128));
		typeMap_.insert(std::pair<std::string,DATA_TYPE>("uuid",DT_UUID));
		typeMap_.insert(std::pair<std::string,DATA_TYPE>("ipaddr",DT_IP));
		typeMap_.insert(std::pair<std::string,DATA_TYPE>("dictionary",DT_DICTIONARY));
		typeMap_.insert(std::pair<std::string,DATA_TYPE>("decimal32", DT_DECIMAL32));
		typeMap_.insert(std::pair<std::string, DATA_TYPE>("decimal64", DT_DECIMAL64));
		typeMap_.insert(std::pair<std::string, DATA_TYPE>("decimal128", DT_DECIMAL128));

		formMap_.insert(std::pair<std::string,DATA_FORM>("scalar",DF_SCALAR));
		formMap_.insert(std::pair<std::string,DATA_FORM>("pair",DF_PAIR));
		formMap_.insert(std::pair<std::string,DATA_FORM>("vector",DF_VECTOR));
		formMap_.insert(std::pair<std::string,DATA_FORM>("matrix",DF_MATRIX));
		formMap_.insert(std::pair<std::string,DATA_FORM>("set",DF_SET));
		formMap_.insert(std::pair<std::string,DATA_FORM>("dictionary",DF_DICTIONARY));
		formMap_.insert(std::pair<std::string,DATA_FORM>("table",DF_TABLE));
		formMap_.insert(std::pair<std::string,DATA_FORM>("chart",DF_CHART));
		formMap_.insert(std::pair<std::string,DATA_FORM>("chunk",DF_CHUNK));

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
		arrTypeSymbol[DT_DECIMAL32] = ' ';
		arrTypeSymbol[DT_DECIMAL64] = ' ';
		arrTypeSymbol[DT_DECIMAL128] = ' ';

		arrFormStr[DF_SCALAR]="SCALAR";
		arrFormStr[DF_PAIR]="PAIR";
		arrFormStr[DF_VECTOR]="VECTOR";
		arrFormStr[DF_MATRIX]="MATRIX";
		arrFormStr[DF_TABLE]="TABLE";
		arrFormStr[DF_SET]="SET";
		arrFormStr[DF_DICTIONARY]="DICTIONARY";
		arrFormStr[DF_CHART]="CHART";
		arrFormStr[DF_CHUNK]="CHUNK";
		arrFormStr[DF_SYSOBJ]="SYSOBJ";
		arrFormStr[DF_TENSOR] = "TENSOR";

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
		arrCategoryTypeStr[COMPLEX] = "COMPLEX";
		arrCategoryTypeStr[ARRAY] = "ARRAY";
		arrCategoryTypeStr[DENARY] = "DENARY";
	}

	ConstantParser arrConstParser[TYPE_COUNT];
	ConstantFunc arrConstFactory[TYPE_COUNT];
	ConstantVectorFunc arrConstVectorFactory[TYPE_COUNT];
	ConstantArrayVectorFunc arrConstArrayVectorFactory[TYPE_COUNT];
	ConstantMatrixFunc arrConstMatrixFactory[TYPE_COUNT];
	std::unordered_map<std::string,DATA_TYPE> typeMap_;
	std::unordered_map<std::string,DATA_FORM> formMap_;
	char arrTypeSymbol[TYPE_COUNT];
	std::string arrFormStr[MAX_DATA_FORM];
	std::string arrPartitionTypeStr[MAX_PARTITION_TYPE];
	std::string arrCategoryTypeStr[MAX_CATEGORY];
};

}

#ifdef _MSC_VER
#pragma warning( pop )
#endif

#endif /* CONSTANTFACTORY_H_ */
