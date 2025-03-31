// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include <string.h>
#include <vector>
#include <unordered_set>
#include <ctime>
#include <cassert>
#include <random>
#include <chrono>
#include "Exports.h"
#include "internal/WideInteger.h"
#include "Constant.h"
#include "Vector.h"
#include "Table.h"
#include "ErrorCodeInfo.h"

#if defined(_MSC_VER)
#pragma warning( push )
#pragma warning( disable : 4100 4251 )
#elif defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#else // gcc
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

namespace dolphindb {

class Dictionary;
class Set;
class Domain;

class EXPORT_DECL Util {
public:
	static std::string VER;
	static int VERNUM;
	static std::string BUILD;
#ifdef _MSC_VER
	const static int BUF_SIZE = 1024;
#else
	static constexpr int BUF_SIZE = 1024;
#endif
	static int DISPLAY_ROWS;
	static int DISPLAY_COLS;
	static int DISPLAY_WIDTH;
	static int CONST_VECTOR_MAX_SIZE;
	static int SEQUENCE_SEARCH_NUM_THRESHOLD;
	static double SEQUENCE_SEARCH_RATIO_THRESHOLD;
	static int MAX_LENGTH_FOR_ANY_VECTOR;
	static const bool LITTLE_ENDIAN_ORDER;

private:
	static int cumMonthDays[13];
	static int monthDays[12];
	static int cumLeapMonthDays[13];
	static int leapMonthDays[12];
	static char escapes[128];
	static std::string duSyms[10];
	static long long tmporalDurationRatioMatrix[9][10];
	static long long tmporalRatioMatrix[81];
	static long long tmporalUplimit[9];

public:
	static Constant* parseConstant(int type, const std::string& word);
	static Constant* createConstant(DATA_TYPE dataType, int extraParam = 0);
	static Constant* createNullConstant(DATA_TYPE dataType, int extraParam = 0);
	static Constant* createBool(char val);
	static Constant* createChar(char val);
	static Constant* createShort(short val);
	static Constant* createInt(int val);
	static Constant* createLong(long long val);
	static Constant* createFloat(float val);
	static Constant* createDouble(double val);
	static Constant* createString(const std::string& val);
	static Constant* createBlob(const std::string& val);
	static Constant* createDate(int year, int month, int day);
	static Constant* createDate(int days);
	static Constant* createMonth(int year, int month);
	static Constant* createMonth(int months);
	static Constant* createNanoTime(int hour, int minute, int second, int nanosecond);
	static Constant* createNanoTime(long long nanoseconds);
	static Constant* createTime(int hour, int minute, int second, int millisecond);
	static Constant* createTime(int milliseconds);
	static Constant* createSecond(int hour, int minute, int second);
	static Constant* createSecond(int seconds);
	static Constant* createMinute(int hour, int minute);
	static Constant* createMinute(int minutes);
	static Constant* createNanoTimestamp(int year, int month, int day, int hour, int minute, int second, int nanosecond);
	static Constant* createNanoTimestamp(long long nanoseconds);
	static Constant* createTimestamp(int year, int month, int day, int hour, int minute, int second, int millisecond);
	static Constant* createTimestamp(long long milliseconds);
	static Constant* createDateTime(int year, int month, int day, int hour, int minute, int second);
	static Constant* createDateTime(int seconds);
	static Constant* createDateHour(int hours);
	static Constant* createDateHour(int year, int month, int day, int hour);
	static Constant* createDecimal32(int scale, double value);
	static Constant* createDecimal64(int scale, double value);
	static Constant* createDecimal128(int scale, double value);

	static bool isFlatDictionary(Dictionary* dict);
	static Table* createTable(Dictionary* dict, int size);
	static Table* createTable(const std::vector<std::string>& colNames, const std::vector<DATA_TYPE>& colTypes, INDEX size, INDEX capacity, const std::vector<int>& extraParams = {});
	static Table* createTable(const std::vector<std::string>& colNames, const std::vector<ConstantSP>& cols);
	static Set* createSet(DATA_TYPE keyType, INDEX capacity);
	static Dictionary* createDictionary(DATA_TYPE keyType, DATA_TYPE valueType);
	static Vector* createVector(DATA_TYPE type, INDEX size, INDEX capacity = 0, bool fast = true, int extraParam = 0, void* data = 0, bool containNull = false);
	static Vector* createArrayVector(VectorSP index, VectorSP value);
	static Vector* createArrayVector(DATA_TYPE type, INDEX size, INDEX capacity = 0, bool fast = true, int extraParam = 0, void *data = NULL, INDEX *pindex = NULL, bool containNull = false);
	static Vector* createMatrix(DATA_TYPE type, int cols, int rows, int colCapacity, int extraParam = 0, void* data = 0, bool containNull = false);
	static Vector* createDoubleMatrix(int cols, int rows);
	static Vector* createPair(DATA_TYPE type, int extraParam = 0) {
		if (type == DT_ANY) {
			return NULL;
		}
		Vector* pair = createVector(type, 2, 2, true, extraParam);
		if (pair == NULL)
			return NULL;
		pair->setForm(DF_PAIR);
		return pair;
	}
	static Vector* createIndexVector(INDEX start, INDEX length);
	static Vector* createIndexVector(INDEX length, bool arrayOnly, INDEX capacity = 0);

	/**
	* Convert unsigned byte sequences to hex string.
	*
	* littleEndian: if true, the first byte is the least significant and should be printed at the most right.
	* str: the length of buffer must be at least 2 * len.
	*/
	static void toHex(const unsigned char* data, size_t len, bool littleEndian, char* str);
	/**
	* Convert hex string to unsigned byte sequences.
	*
	* len: must be a positive even number.
	* littleEndian: if true, the first byte is the least significant, i.e. the leftmost characters would be converted to the rightmost byte.
	* data: the length of buffer must be at least len/2
	*/
	static bool fromHex(const char* str, size_t len, bool littleEndian, unsigned char* data);

	static void toGuid(const unsigned char*, char* str);
	static bool fromGuid(const char* str, unsigned char* data);

	static DATA_TYPE convertToIntegralDataType(DATA_TYPE type);
	static long long getTemporalConversionRatio(DATA_TYPE first, DATA_TYPE second);
	static char getDataTypeSymbol(DATA_TYPE type);
	static std::string getDataTypeString(DATA_TYPE type);
	static std::string getDataFormString(DATA_FORM form);
	static std::string getTableTypeString(TABLE_TYPE type);
	static DATA_TYPE getDataType(const std::string& typestr);
	static DATA_FORM getDataForm(const std::string& formstr);
	static int getDataTypeSize(DATA_TYPE type);
	static DATA_TYPE getDataType(char ch);
	static DATA_CATEGORY getCategory(DATA_TYPE type);
	static DURATION_UNIT getDurationUnit(const std::string& typestr);
	static long long getTemporalDurationConversionRatio(DATA_TYPE t, DURATION_UNIT du);
	static long long getTemporalUplimit(DATA_TYPE t);

	static bool equalIgnoreCase(const std::string& str1, const std::string& str2);
	static std::string lower(const std::string& str);
	static std::string upper(const std::string& str);
	static char toUpper(char ch);
	static char toLower(char ch);
	static std::string ltrim(const std::string& str);
	static std::string trim(const std::string& str);
	static std::string strip(const std::string& str);
	static int wc(const char* str);
	static bool endWith(const std::string& str, const std::string& end);
	static bool startWith(const std::string& str, const std::string& start);
	static bool strWildCmp(const char* wildstring, const char* matchstring);
	static bool strCaseInsensitiveWildCmp(const char* str, const char* pat);
	static std::string replace(const std::string& str, const std::string& pattern, const std::string& replacement);
	static std::string replace(const std::string& str, char pattern, char replacement);
	static std::string convert(int val);
	static std::string longToString(long long val);
	static std::string doubleToString(double val);
	static bool isVariableCandidate(const std::string& word);
	static std::string literalConstant(const std::string& str);
	static void split(const char* s, char delim, std::vector<std::string> &elems);
	static std::vector<std::string> split(const std::string &s, char delim);
	inline static bool isDigit(char ch) { return '0' <= ch && ch <= '9'; }
	inline static bool isDateDelimitor(char ch) { return ch == '.' || ch == '/' || ch == '-'; }
	inline static bool isLetter(char ch) { return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z'); }
	static char escape(char original);
	static void writeDoubleQuotedString(std::string& dest, const std::string& source);

	static int countDays(int year, int month, int day);
	static int parseYear(int days);
	static void parseDate(int days, int& year, int& month, int& day);
	static int getMonthEnd(int days);
	static int getMonthStart(int days);
	static long long getNanoBenchmark();
	static long long getEpochTime();
	static long long getNanoEpochTime();
	static bool getLocalTime(struct tm& result);
	static bool getLocalTime(time_t t, struct tm& result);
	static int toLocalDateTime(int epochTime);
	static int* toLocalDateTime(int* epochTimes, int n);
	static long long toLocalTimestamp(long long epochTime);
	static long long* toLocalTimestamp(long long* epochTimes, int n);
	static long long toLocalNanoTimestamp(long long epochNanoTime);
	static long long* toLocalNanoTimestamp(long long* epochNanoTimes, int n);
	static std::string toMicroTimestampStr(std::chrono::system_clock::time_point& tp, bool printDate = false);

	static char* allocateMemory(INDEX size, bool throwIfFail = true);
	static bool isLittleEndian() { int x = 1; return *(char *)&x == 1; }
	static bool is64BIT() { return sizeof(char*) == 8; }
	static bool isWindows();
	static int getCoreCount();
	static long long getPhysicalMemorySize();
	static void sleep(int milliSeconds);
	static int getLastErrorCode();
	static std::string getLastErrorMessage();
	static std::string getErrorMessage(int errCode);
	static std::string getPartitionTypeString(PARTITION_TYPE type);
	static Domain* createDomain(PARTITION_TYPE type, DATA_TYPE partitionColType, const ConstantSP& partitionSchema);
	static Vector* createSubVector(const VectorSP& source, std::vector<int> indices);
	static std::string getCategoryString(DATA_CATEGORY type);
	static Vector* createSymbolVector(const SymbolBaseSP& symbolBase, INDEX size, INDEX capacity = 0, bool fast = true,
		void* data = 0, void** dataSegment = 0, int segmentSizeInBit = 0, bool containNull = false);

	static void SetOrThrowErrorInfo(ErrorCodeInfo *errorCodeInfo, int errorCode, const std::string &errorInfo);
	template<typename T>
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, T val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0) {
		SetOrThrowErrorInfo(&errorCodeInfo, ErrorCodeInfo::EC_InvalidObject, "It cannot be converted to " + getDataTypeString(dataType) + " in setValue");
		return false;
	}
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, std::nullptr_t val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, Constant* val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, const ConstantSP& val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, bool val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, char val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, short val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, const char* val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, std::string val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, const unsigned char* val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, unsigned char val[], ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, char val[], ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, long long val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, long int val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, int val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, float val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, double val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, const void* val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);

	static bool setValue(ConstantSP& data, DATA_TYPE dataType, std::vector<std::nullptr_t> val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, std::vector<Constant*> val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, std::vector<ConstantSP> val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, std::vector<bool> val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, std::vector<char> val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, std::vector<short> val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, std::vector<const char*> val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, std::vector<std::string> val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, std::vector<const unsigned char*> val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, std::vector<long long> val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, std::vector<long int> val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, std::vector<int> val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, std::vector<float> val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, std::vector<double> val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, std::vector<const void*> val, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);
	static bool setValue(ConstantSP& data, DATA_TYPE dataType, long long val, const char *pTypeName, ErrorCodeInfo &errorCodeInfo, int extraParam = 0);

	template<typename T>
	static ConstantSP createObject(DATA_TYPE dataType, T val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0) {
		SetOrThrowErrorInfo(errorCodeInfo, ErrorCodeInfo::EC_InvalidObject, "It cannot be converted to " + getDataTypeString(dataType));
		return NULL;
	}
	static ConstantSP createObject(DATA_TYPE dataType, std::nullptr_t val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, Constant* val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, const ConstantSP& val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, bool val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, char val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, short val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, const char* val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, std::string val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, const unsigned char* val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, unsigned char val[], ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, char val[], ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, long long val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, long int val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, int val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, float val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, double val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, const void* val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);

	static ConstantSP createObject(DATA_TYPE dataType, std::vector<std::nullptr_t> val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, std::vector<Constant*> val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, std::vector<ConstantSP> val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, std::vector<bool> val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, std::vector<char> val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, std::vector<short> val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, std::vector<const char*> val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, std::vector<std::string> val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, std::vector<const unsigned char*> val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, std::vector<long long> val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, std::vector<long int> val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, std::vector<int> val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, std::vector<float> val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, std::vector<double> val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createObject(DATA_TYPE dataType, std::vector<const void*> val, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static ConstantSP createValue(DATA_TYPE dataType, long long val, const char *pTypeName, ErrorCodeInfo *errorCodeInfo = NULL, int extraParam = 0);
	static bool checkColDataType(DATA_TYPE colDataType, bool isColTemporal, ConstantSP &constsp);
	static unsigned long getCurThreadId();
	static void writeFile(const char *pfilepath, const void *pbytes, std::size_t bytelen);

	static void enumBoolVector(const VectorSP &pVector, std::function<bool(const char *pbuf, INDEX startIndex, INDEX size)> func, INDEX offset = 0) {
		enumDdbVector<char>(pVector, &Vector::getBoolConst, func, offset);
	}
	static void enumIntVector(const VectorSP &pVector, std::function<bool(const int *pbuf, INDEX startIndex, int length)> func, INDEX offset = 0) {
		enumDdbVector<int>(pVector, &Vector::getIntConst, func, offset);
	}
	static void enumShortVector(const VectorSP &pVector, std::function<bool(const short *pbuf, INDEX startIndex, int length)> func, INDEX offset = 0) {
		enumDdbVector<short>(pVector, &Vector::getShortConst, func, offset);
	}
	static void enumCharVector(const VectorSP &pVector, std::function<bool(const char *pbuf, INDEX startIndex, int length)> func, INDEX offset = 0) {
		enumDdbVector<char>(pVector, &Vector::getCharConst, func, offset);
	}
	static void enumLongVector(const VectorSP &pVector, std::function<bool(const long long *pbuf, INDEX startIndex, int length)> func, INDEX offset = 0) {
		enumDdbVector<long long>(pVector, &Vector::getLongConst, func, offset);
	}
	static void enumFloatVector(const VectorSP &pVector, std::function<bool(const float *pbuf, INDEX startIndex, int length)> func, INDEX offset = 0) {
		enumDdbVector<float>(pVector, &Vector::getFloatConst, func, offset);
	}
	static void enumDoubleVector(const VectorSP &pVector, std::function<bool(const double *pbuf, INDEX startIndex, int length)> func, INDEX offset = 0) {
		enumDdbVector<double>(pVector, &Vector::getDoubleConst, func, offset);
	}
	static void enumStringVector(const VectorSP &pVector, std::function<bool(std::string **pbuf, INDEX startIndex, int length)> func, INDEX offset = 0) {
		std::string* buffer[Util::BUF_SIZE];
		std::string** pbuf;
		INDEX startIndex = offset;
		int size;
		INDEX leftSize = pVector->size() - startIndex;
		while (leftSize > 0) {
			size = leftSize;
			if (size > Util::BUF_SIZE)
				size = Util::BUF_SIZE;
			pbuf = pVector->getStringConst(startIndex, size, buffer);
			if (func(pbuf, startIndex, size) == false)
				break;
			leftSize -= size;
			startIndex += size;
		}
	}
	static void enumInt128Vector(const VectorSP &pVector, std::function<bool(const Guid *pbuf, INDEX startIndex, int length)> func, INDEX offset = 0) {
		enumBinaryVector<Guid>(pVector, func, offset);
	}
	static void enumDecimal32Vector(const VectorSP &pVector, std::function<bool(const int32_t *pbuf, INDEX startIndex, int length)> func, INDEX offset = 0) {
		enumBinaryVector<int32_t>(pVector, func, offset);
	}
	static void enumDecimal64Vector(const VectorSP &pVector, std::function<bool(const int64_t *pbuf, INDEX startIndex, int length)> func, INDEX offset = 0) {
		enumBinaryVector<int64_t>(pVector, func, offset);
	}
	static void enumDecimal128Vector(const VectorSP &pVector, std::function<bool(const wide_integer::int128 *pbuf, INDEX startIndex, int length)> func, INDEX offset = 0) {
		enumBinaryVector<wide_integer::int128>(pVector, func, offset);
	}

    static ConstantSP getConstantSP(const std::string &s) { return Util::createString(s); }
    static ConstantSP getConstantSP(bool v) { return Util::createBool(v); }
    static ConstantSP getConstantSP(int v) { return Util::createInt(v); }
    static ConstantSP getConstantSP(int64_t v) { return Util::createLong(v); }
    static ConstantSP getConstantSP(const ConstantSP &c) { return c; }

    static void getConstantSP(std::vector<ConstantSP> &args) { std::ignore = args; }

    template <typename T, typename... Args>
    static void getConstantSP(std::vector<ConstantSP> &args, T &&first, Args &&... other)
    {
        args.push_back(getConstantSP(first));
        getConstantSP(args, std::forward<Args>(other)...);
    }

private:
	template <class T>
	static void enumBinaryVector(const VectorSP &pVector, std::function<bool(const T *pbuf, INDEX startIndex, int length)> func, INDEX offset = 0) {
		std::vector<T> buffer(Util::BUF_SIZE);
		const T* pbuf;
		INDEX startIndex = offset;
		int size;
		INDEX leftSize = pVector->size() - startIndex;
		while (leftSize > 0) {
			size = leftSize;
			if (size > Util::BUF_SIZE)
				size = Util::BUF_SIZE;
			pbuf = (const T*)pVector->getBinaryConst(startIndex, size, sizeof(T), (unsigned char*)buffer.data());
			if (func(pbuf, startIndex, size) == false)
				break;
			leftSize -= size;
			startIndex += size;
		}
	}
	template <class T>
	static void enumDdbVector(const VectorSP &pVector,
		const T* (Vector::*getConst)(INDEX, int, T*) const,
		std::function<bool(const T *buf, INDEX startIndex, int length)> func, INDEX offset = 0) {
		T buffer[Util::BUF_SIZE];
		const T *pbuf;
		INDEX startIndex = offset;
		int size;
		INDEX leftSize = pVector->size() - startIndex;
		while (leftSize > 0) {
			size = leftSize;
			if (size > Util::BUF_SIZE)
				size = Util::BUF_SIZE;
			pbuf = (pVector.get()->*getConst)(startIndex, size, buffer);
			if (func(pbuf, startIndex, size) == false)
				break;
			leftSize -= size;
			startIndex += size;
		}
	}
};

template <typename T>
inline T getNullValue();
template <>
inline char getNullValue<char>() { return CHAR_MIN; }
template <>
inline short getNullValue<short>() { return SHRT_MIN; }
template <>
inline int getNullValue<int>() { return INT_MIN; }
template <>
inline long int getNullValue<long int>() { return LONG_MIN; }
template <>
inline long long getNullValue<long long>() { return LLONG_MIN; }
template <>
inline float getNullValue<float>() { return FLT_NMIN; }
template <>
inline double getNullValue<double>() { return DBL_NMIN; }
template <>
inline std::string getNullValue<std::string>() { return ""; }
template <>
inline Guid getNullValue<Guid>() { return Guid(); }
template <>
inline wide_integer::int128 getNullValue<wide_integer::int128>() { return std::numeric_limits<wide_integer::int128>::min(); }

template <class T>
class DdbVector {
public:
	DdbVector(int sz, int capacity = 0) : size_(sz), capacity_(capacity), dataNeedRelease_(true), containNull_(false){
		if (capacity_ < size_)
			capacity_ = size_;
		if (capacity_ < 1) {
			throw RuntimeException("can't create empty DdbVector.");
		}
		data_ = new T[capacity_];
	}
	//DdbVector own data and it will be released, don't delete data in the future.
	DdbVector(T *dt, int sz, int capacity = 0) : data_(dt), size_(sz), capacity_(capacity), dataNeedRelease_(true), containNull_(false) {
		if (capacity_ < size_)
			capacity_ = size_;
	}
	DdbVector(const DdbVector &src) = delete;
	~DdbVector() {
		if (dataNeedRelease_) {
			delete[] data_;
		}
	}
	int size() const {
		return size_;
	}
	const T* data() const {
		assert(dataNeedRelease_);
		return data_;
	}
	T* data() {
		assert(dataNeedRelease_);
		return data_;
	}
	void addNull() {
		add(getNullValue<T>());
		containNull_ = true;
	}
	void add(const T& value) {
		assert(dataNeedRelease_);
		assert(size_  < capacity_);
		if(containNull_ == false && value == getNullValue<T>()){
			containNull_ = true;
		}
		data_[size_++] = value;
	}
	void add(T&& value) {
		assert(dataNeedRelease_);
		assert(size_  < capacity_);
		if (containNull_ == false && value == getNullValue<T>()) {
			containNull_ = true;
		}
		data_[size_++] = std::move(value);
	}
	void appendString(const std::string *buf, int len) {
		assert(dataNeedRelease_);
		assert(size_ + len <= capacity_);
		for (auto i = 0; i < len; i++) {
			if (containNull_ == false && buf[i] == getNullValue<T>()) {
				containNull_ = true;
			}
			data_[size_++] = std::move(buf[i]);
		}
	}
	void appendString(std::string *buf, int len) {
		assert(dataNeedRelease_);
		assert(size_ + len <= capacity_);
		for (auto i = 0; i < len; i++) {
			if (containNull_ == false && buf[i] == getNullValue<T>()) {
				containNull_ = true;
			}
			data_[size_++] = std::move(buf[i]);
		}
	}
	//This function is invalid for string DdbVector, please use appendString instead.
	void append(const T *buf, int len) {
		assert(dataNeedRelease_);
		assert(size_ + len <= capacity_);
		if (containNull_ == false) {
			for (auto i = 0; i < len; i++) {
				if (buf[i] == getNullValue<T>()) {
					containNull_ = true;
					break;
				}
			}
		}
		memcpy(data_ + size_, buf, len * sizeof(T));
		size_ += len;
	}
	void setNull(int index) {
		set(index, getNullValue<T>());
		containNull_ = true;
	}
	void set(int index, const T& value) {
		assert(dataNeedRelease_);
		assert(index < size_);
		if (containNull_ == false && value == getNullValue<T>()) {
			containNull_ = true;
		}
		data_[index] = value;
	}
	void set(int index, T&& value) {
		assert(dataNeedRelease_);
		assert(index < size_);
		if (containNull_ == false && value == getNullValue<T>()) {
			containNull_ = true;
		}
		data_[index] = std::move(value);
	}
	void setString(int start, int len, const std::string *buf) {
		assert(dataNeedRelease_);
		assert(start < size_);
		assert(start + len <= size_);
		for (auto i = 0, index = start; i < len; i++, index++) {
			if (containNull_ == false && buf[i] == getNullValue<T>()) {
				containNull_ = true;
			}
			data_[index] = std::move(buf[i]);
		}
	}
	void setString(int start, int len, std::string *buf) {
		assert(dataNeedRelease_);
		assert(start < size_);
		assert(start + len <= size_);
		for (auto i = 0, index = start; i < len; i++, index++) {
			if (containNull_ == false && buf[i] == getNullValue<T>()) {
				containNull_ = true;
			}
			data_[index] = std::move(buf[i]);
		}
	}
	//This function is invalid for string DdbVector, please use appendString instead.
	void set(int start, int len, const T *buf) {
		assert(dataNeedRelease_);
		assert(start < size_);
		assert(start + len <= size_);
		if (containNull_ == false) {
			for (auto i = 0; i < len; i++) {
				if (buf[i] == getNullValue<T>()) {
					containNull_ = true;
					break;
				}
			}
		}
		memcpy(data_ + start, buf, len * sizeof(T));
	}
	Vector* createVector(DATA_TYPE type, int extraParam = 0) {
		if (dataNeedRelease_ == false) {
			throw RuntimeException(Util::getDataTypeString(type) + "'s createVector can only be called once.");
		}
		if (type != DT_STRING && type != DT_BLOB && type != DT_SYMBOL) {
			assert(Util::getDataTypeSize(type) == sizeof(T));
			Vector* pVector;
			pVector = Util::createVector(type, size_, size_, true, extraParam, data_, containNull_);
			dataNeedRelease_ = false;
			return pVector;
		}
		else {
			Vector* pVector = Util::createVector(type, 0, size_, true, extraParam);
			pVector->appendString((std::string*)data_, size_);
			return pVector;
		}
	}
private:
	T * data_;
	int size_;
	int capacity_;
	bool dataNeedRelease_, containNull_;
};

}

#if defined(_MSC_VER)
#pragma warning( pop )
#elif defined(__clang__)
#pragma clang diagnostic pop
#else // gcc
#pragma GCC diagnostic pop
#endif
