/*
 * ScalarImp.cpp
 *
 *  Created on: May 10, 2017
 *      Author: dzhou
 */

#ifdef WINDOWS
	#include <winsock2.h>
	#include <windows.h>
	#include <objbase.h>
#else
	#include <uuid/uuid.h>
#endif

#include "ScalarImp.h"
#include "Format.h"

namespace dolphindb {

static TemporalFormat* monthFormat_;
static TemporalFormat* dateFormat_;
static TemporalFormat* minuteFormat_;
static TemporalFormat* secondFormat_;
static TemporalFormat* timeFormat_;
static TemporalFormat* timestampFormat_;
static TemporalFormat* nanotimeFormat_;
static TemporalFormat* nanotimestampFormat_;
static TemporalFormat* datetimeFormat_;
static TemporalFormat* datehourFormat_;
static NumberFormat* floatingNormFormat_;
static NumberFormat* floatingSciFormat_;

void initFormatters(){
	monthFormat_ = new TemporalFormat("yyyy.MM\\M");
	dateFormat_ = new TemporalFormat("yyyy.MM.dd");
	minuteFormat_ = new TemporalFormat("HH:mm\\m");
	secondFormat_ = new TemporalFormat("HH:mm:ss");
	timeFormat_ = new TemporalFormat("HH:mm:ss.SSS");
	timestampFormat_ = new TemporalFormat("yyyy.MM.ddTHH:mm:ss.SSS");
	nanotimeFormat_ = new TemporalFormat("HH:mm:ss.nnnnnnnnn");
	nanotimestampFormat_ = new TemporalFormat("yyyy.MM.ddTHH:mm:ss.nnnnnnnnn");
	datetimeFormat_ = new TemporalFormat("yyyy.MM.ddTHH:mm:ss");
	datehourFormat_ = new TemporalFormat("yyyy.MM.ddTHH");
	floatingNormFormat_  = new NumberFormat("0.######");
	floatingSciFormat_ = new NumberFormat("0.0#####E0");
}

inline int countTemporalUnit(int days, int multiplier, int remainder){
	return days == INT_MIN ? INT_MIN : days * multiplier + remainder;
}

inline long long countTemporalUnit(int days, long long multiplier, long long remainder){
	return days == INT_MIN ? LLONG_MIN : days * multiplier + remainder;
}

bool Void::isNull(INDEX start, int len, char* buf) const {
	memset(buf,1,len);
	return true;
}

bool Void::isValid(INDEX start, int len, char* buf) const {
	memset(buf,0,len);
	return true;
}

bool Void::getBool(INDEX start, int len, char* buf) const {
	for(int i=0;i<len;++i)
		buf[i]=CHAR_MIN;
	return true;
}

const char* Void::getBoolConst(INDEX start, int len, char* buf) const {
	for(int i=0;i<len;++i)
		buf[i]=CHAR_MIN;
	return buf;
}

bool Void::getChar(INDEX start, int len, char* buf) const {
	char tmp=CHAR_MIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const char* Void::getCharConst(INDEX start, int len, char* buf) const {
	char tmp=CHAR_MIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Void::getShort(INDEX start, int len, short* buf) const {
	short tmp=SHRT_MIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const short* Void::getShortConst(INDEX start, int len, short* buf) const {
	short tmp=SHRT_MIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Void::getInt(INDEX start, int len, int* buf) const {
	int tmp=INT_MIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const int* Void::getIntConst(INDEX start, int len, int* buf) const {
	int tmp=INT_MIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Void::getLong(INDEX start, int len, long long* buf) const {
	long long tmp=LLONG_MIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const long long* Void::getLongConst(INDEX start, int len, long long* buf) const {
	long long tmp=LLONG_MIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Void::getIndex(INDEX start, int len, INDEX* buf) const {
	INDEX tmp=INDEX_MIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const INDEX* Void::getIndexConst(INDEX start, int len, INDEX* buf) const {
	INDEX tmp=INDEX_MIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Void::getFloat(INDEX start, int len, float* buf) const {
	float tmp=FLT_NMIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const float* Void::getFloatConst(INDEX start, int len, float* buf) const {
	float tmp=FLT_NMIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Void::getDouble(INDEX start, int len, double* buf) const {
	double tmp=DBL_NMIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const double* Void::getDoubleConst(INDEX start, int len, double* buf) const {
	double tmp=DBL_NMIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Void::getString(INDEX start, int len, string** buf) const {
	for(int i=0;i<len;++i)
		buf[i]=&Constant::EMPTY;
	return true;
}

string** Void::getStringConst(INDEX start, int len, string** buf) const {
	for(int i=0;i<len;++i)
		buf[i]=&Constant::EMPTY;
	return buf;
}

long long Void::getAllocatedMemory() const {return sizeof(Void);}


int Void::serialize(char* buf, int bufSize, INDEX indexStart, int offset, int& numElement, int& partial) const {
	buf[0] = isNothing() ? 0 : 1;
	numElement = 1;
	partial = 0;
	return 1;
}

IO_ERR Void::deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement) {
	bool explicitNull;
	IO_ERR ret = in->readBool(explicitNull);
	if(ret == OK)
		numElement = 1;
	setNothing(!explicitNull);
	return ret;
}

int String::serialize(char* buf, int bufSize, INDEX indexStart, int offset, int& numElement, int& partial) const {
    int len = val_.size();
    if (!blob_) {
        if (offset > len)
            return -1;
        if (bufSize >= len - offset + 1) {
            numElement = 1;
            partial = 0;
            memcpy(buf, val_.data() + offset, len - offset + 1);
            return len - offset + 1;
        } else {
            numElement = 0;
            partial = offset + bufSize;
            memcpy(buf, val_.data() + offset, bufSize);
            return bufSize;
        }
    } else {
        int bytes = 0;
        if (offset > 0) {
            offset -= 4;
            if (UNLIKELY(offset < 0))
                return -1;
        } else {
            if (UNLIKELY((size_t)bufSize < sizeof(int)))
                return 0;
            int len = val_.size();
            memcpy(buf, &len, sizeof(int));
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
}

IO_ERR String::deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement){
    IO_ERR ret;
    if (blob_) {
        int len;
        if ((ret = in->readInt(len)) != OK)
            return ret;
        std::unique_ptr<char[]> buf(new char[len]);
        if ((ret = in->read(buf.get(), len)) != OK)
            return ret;
        val_.clear();
        val_.append(buf.get(), len);
    } else {
        // numElement < 0 indicate read line from input stream
        ret = numElement >= 0 ? in->readString(val_) : in->readLine(val_);
        if (ret == OK)
            numElement = 1;
    }
    return ret;
}

Bool* Bool::parseBool(const string& str){
	Bool* p=0;

	if(str.compare("00")==0){
		p=new Bool();
		p->setNull();
	}
	else if(Util::equalIgnoreCase(str,"true")){
		p=new Bool(1);
	}
	else if(Util::equalIgnoreCase(str,"false")){
		p=new Bool(0);
	}
	else{
		p=new Bool(atoi(str.c_str()) != 0);
	}
	return p;
}

IO_ERR Bool::deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement){
	IO_ERR ret = in->readChar(val_);
	if(ret == OK)
		numElement = 1;
	return ret;
}

string Char::toString(char val) {
	if(val == CHAR_MIN)
		return "";
	else if(val>31 && val<127)
		return string(1, val);
	else
		return NumberFormat::toString(val);
}

string Char::getScript() const {
	if(isNull())
		return "00c";
	else if(val_>31 && val_<127){
		string str("' '");
		str[1] = val_;
		return str;
	}
	else{
		char buf[5];
		sprintf(buf,"%d",val_);
		return string(buf);
	}
}

Char* Char::parseChar(const string& str){
	Char* p=0;
	if(str.compare("00")==0 || str.empty()){
		p=new Char();
		p->setNull();
	}
	else if(str[0]=='\''){
		char ch=CHAR_MIN;
		if(str.size()==4 && str[3]=='\'' && str[1]=='\\'){
			ch=Util::escape(str[2]);
			if(ch==0)
				ch=str[2];
		}
		else if(str.size()==3 && str[2]=='\'')
			ch=str[1];
		p=new Char(ch);
	}
	else{
		int val = atoi(str.c_str());
		if(val>127 || val < -128)
			return NULL;
		p=new Char(atoi(str.c_str()));
	}
	return p;
}

IO_ERR Char::deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement){
	IO_ERR ret = in->readChar(val_);
	if(ret == OK)
		numElement = 1;
	return ret;
}

Short* Short::parseShort(const string& str){
	Short* p=0;

	if(str.compare("00")==0){
		p=new Short();
		p->setNull();
	}
	else{
		int val = atoi(str.c_str());
		if(val>65535 || val < -65536)
			return NULL;
		p=new Short(atoi(str.c_str()));
	}
	return p;
}

string Short::toString(short val){
	if(val == SHRT_MIN)
		return "";
	else
		return NumberFormat::toString(val);
}

IO_ERR Short::deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement){
	IO_ERR ret = in->readShort(val_);
	if(ret == OK)
		numElement = 1;
	return ret;
}

Int* Int::parseInt(const string& str){
	Int* p=0;
	if(str.compare("00")==0){
		p=new Int();
		p->setNull();
	}
	else
		p=new Int(atoi(str.c_str()));
	return p;
}

string Int::toString(int val){
	if(val == INT_MIN)
		return "";
	else
		return NumberFormat::toString(val);
}

IO_ERR Int::deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement){
	IO_ERR ret = in->readInt(val_);
	if(ret == OK)
		numElement = 1;
	return ret;
}

Long* Long::parseLong(const string& str){
	Long* p=0;

	if(str.compare("00")==0){
		p=new Long();
		p->setNull();
	}
	else
		p=new Long(atoll(str.c_str()));
	return p;
}

string Long::toString(long long val){
	if(val == LLONG_MIN)
		return "";
	else
		return NumberFormat::toString(val);
}

IO_ERR Long::deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement){
	IO_ERR ret = in->readLong(val_);
	if(ret == OK)
		numElement = 1;
	return ret;
}


Int128::Int128(const unsigned char* data){
	memcpy(uuid_, data, 16);
}

Int128::Int128(){
}

int Int128::serialize(char* buf, int bufSize, INDEX indexStart, int offset, int& numElement, int& partial) const {
	int len = 16 - offset;
	if(len < 0)
		return -1;
	else if(bufSize >= len){
		numElement = 1;
		partial = 0;
		memcpy(buf,((char*)uuid_)+offset, len);
		return len;
	}
	else{
		len = bufSize;
		numElement = 0;
		partial = offset+bufSize;
		memcpy(buf,((char*)uuid_)+offset, len);
		return len;
	}
}

IO_ERR Int128::deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement) {
	IO_ERR ret = in->readBytes((char*)uuid_, 16, false);
	if(ret == OK)
		numElement = 1;
	return ret;
}

bool Int128::isNull() const {
	const unsigned char* a = (const unsigned char*)uuid_;
	return (*(long long*)a) == 0 && (*(long long*)(a+8)) == 0;
}

void Int128::setNull(){
	memset((void*)uuid_, 0, 16);
}

void Int128::setBinary(const unsigned char* val, int unitLength){
	memcpy(uuid_, val, 16);
}

bool Int128::getBinary(INDEX start, int len, int unitLength, unsigned char* buf) const{
	if(unitLength != 16)
		return false;
	for(int i=0; i<len; ++i){
		memcpy(buf, uuid_, 16);
		buf += 16;
	}
	return true;
}

const unsigned char* Int128::getBinaryConst(INDEX start, int len, int unitLength, unsigned char* buf) const {
	unsigned char* original = buf;
	for(int i=0; i<len; ++i){
		memcpy(buf, uuid_, 16);
		buf += 16;
	}
	return original;
}

string Int128::toString(const unsigned char* data) {
	char buf[32];
	Util::toHex(data, 16, Util::LITTLE_ENDIAN_ORDER, buf);
	return string(buf, 32);
}

Int128* Int128::parseInt128(const char* str, int len){
	unsigned char buf[16];
	if(len == 0){
		memset(buf, 0, 16);
		return new Int128(buf);
	}
	else if(len == 32 && Util::fromHex(str, len, Util::LITTLE_ENDIAN_ORDER, buf))
		return new Int128(buf);
	else
		return nullptr;
}

int Int128::compare(INDEX index, const ConstantSP& target) const {
	return ((Guid*)uuid_)->compare(target->getInt128());
}

Uuid::Uuid(bool newUuid){
	if(!newUuid){
		memset(uuid_, 0, 16);
	}
	else{
#ifdef WINDOWS
	CoCreateGuid((GUID*)uuid_);
#else
	uuid_generate(uuid_);
#endif
	}
}

Uuid::Uuid(const unsigned char* uuid){
	memcpy(uuid_, uuid, 16);
}

Uuid::Uuid(const char* guid, int len){
	if(len == 0)
		memset(uuid_, 0, 16);
	else if(len != 36 || !Util::fromGuid(guid, uuid_))
		throw RuntimeException("Invalid UUID string");
}

Uuid::Uuid(const Uuid& copy){
	memcpy(uuid_, copy.uuid_, 16);
}

Uuid* Uuid::parseUuid(const char* str, int len){
	return new Uuid(str, len);
}

IPAddr::IPAddr(){
}

/**
 * Assume big-endian encoding for ipv4 and ipv6
 */
IPAddr::IPAddr(const char* ip, int len){
	if(len == 0 || !parseIPAddr(ip, len, uuid_))
		memset(uuid_, 0, 16);
}

IPAddr::IPAddr(const unsigned char* data) : Int128(data){
}

/**
 * Assume big-endian encoding for ipv4 and ipv6
 */
string IPAddr::toString(const unsigned char* data) {
	char buf[40];
	int cursor = 0;
	bool isIP4;

	if(LIKELY(Util::LITTLE_ENDIAN_ORDER))
		isIP4 = *(unsigned long long*)(data + 8) == 0 && *(unsigned int*)(data+4) == 0;
	else
		isIP4 = *(unsigned long long*)data == 0 && *(unsigned int*)(data+8) == 0;
	if(isIP4){
		if(LIKELY(Util::LITTLE_ENDIAN_ORDER)){
			for(int i=3; i>=0; --i){
				cursor += sprintf(buf + cursor, "%d", data[i]);
				buf[cursor++] = '.';
			}
		}
		else{
			for(int i=12; i<16; ++i){
				cursor += sprintf(buf + cursor, "%d", data[i]);
				buf[cursor++] = '.';
			}
		}
	}
	else{
		bool consecutiveZeros = false;
		for(int i=0; i<16; i=i+2){
			if(i > 0 && i<12 && !consecutiveZeros && *(unsigned int*)(data + (Util::LITTLE_ENDIAN_ORDER ? 11 -i : i)) == 0){
				//consecutive zeros
				consecutiveZeros = true;
				i += 2;
				while(i<12 && *(unsigned int*)(data + (Util::LITTLE_ENDIAN_ORDER ? 11 -i : i)) == 0) i += 2;
			}
			else{
				bool skipZero = true;
				unsigned char ch = data[Util::LITTLE_ENDIAN_ORDER ? 15 - i : i];
				if(ch > 0){
					if(ch>15){
						unsigned char high = ch >> 4;
						buf[cursor++] = high >= 10 ? high + 87 : high + 48;
						ch &= 15;
					}
					buf[cursor++] = ch >= 10 ? ch + 87 : ch + 48;
					skipZero = false;
				}
				ch = data[Util::LITTLE_ENDIAN_ORDER ? 14 - i : i + 1];
				if(!skipZero || ch>15){
					unsigned char high = ch >> 4;
					buf[cursor++] = high >= 10 ? high + 87 : high + 48;
					ch &= 15;
				}
				buf[cursor++] = ch >= 10 ? ch + 87 : ch + 48;
			}
			buf[cursor++] = ':';
		}
	}
	return string(buf, cursor - 1);
}

IPAddr* IPAddr::parseIPAddr(const char* str, int len){
	unsigned char buf[16];
	if(parseIPAddr(str, len, buf))
		return new IPAddr(buf);
	else
		return nullptr;
}

bool IPAddr::parseIPAddr(const char* str, size_t len, unsigned char* buf){
	if(len < 7)
		return false;
	int i = 0;
	for(i=0; i<4 && str[i] != '.'; ++i);
	if(i >= 4)
		return parseIP6(str, len, buf);
	else
		return parseIP4(str, len, buf);
}

bool IPAddr::parseIP4(const char* str, size_t len, unsigned char* buf){
	int byteIndex = 0;
	int curByte = 0;

	for(size_t i=0; i<=len; ++i){
		if(i==len || str[i] == '.'){
			if(curByte < 0 || curByte > 255 || byteIndex > 3)
				return false;
			buf[Util::LITTLE_ENDIAN_ORDER ? 3 - byteIndex++ : 12 + byteIndex++ ] = curByte;
			curByte = 0;
			continue;
		}
		if(str[i] < '0' || str[i] > '9')
			return false;
		curByte = curByte * 10 + str[i] - 48;
	}
	if(byteIndex != 4)
		return false;
	memset(buf + (Util::LITTLE_ENDIAN_ORDER ? 4 : 0), 0, 12);
	return true;
}

bool IPAddr::parseIP6(const char* str, size_t len, unsigned char* buf){
	int byteIndex = 0;
	int curByte = 0;
	int lastColonPos = -1;
	for(size_t i=0; i<=len; ++i){
		if(i==len || str[i] == ':'){
			//check consecutive colon
			if(lastColonPos == (int)i - 1){
				//check how many colons in the remaining string
				int colons = byteIndex/2;
				for(size_t k=i+1; k<len; ++k)
					if(str[k] ==':')
						++colons;
				int consecutiveZeros = (7 - colons)*2;
				if(Util::LITTLE_ENDIAN_ORDER){
					for(int k=0; k<consecutiveZeros; ++k)
						buf[15 - byteIndex++] = 0;
				}
				else{
					for(int k=0; k<consecutiveZeros; ++k)
						buf[byteIndex++] = 0;
				}
			}
			else{
				if(curByte < 0 || curByte > 65535 || byteIndex > 15)
					return false;
				if(Util::LITTLE_ENDIAN_ORDER){
					buf[15 - byteIndex++] = curByte>>8;
					buf[15 - byteIndex++] = curByte & 255;
				}
				else{
					buf[byteIndex++] = curByte>>8;
					buf[byteIndex++] = curByte & 255;
				}
				curByte = 0;
			}
			lastColonPos = i;
			continue;
		}
		char ch = str[i];
		char value = ch >= 97 ? ch -87 : (ch >= 65 ? ch -55 : ch -48);
		if(value < 0 || value > 15)
			return false;
		curByte = (curByte<<4) + value;
	}
	return byteIndex==16;
}

bool Float::getChar(INDEX start, int len, char* buf) const {
	char tmp=isNull()?CHAR_MIN:(val_<0?(val_-0.5):(val_+0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const char* Float::getCharConst(INDEX start, int len, char* buf) const {
	char tmp=isNull()?CHAR_MIN:(val_<0?(val_-0.5):(val_+0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Float::getShort(INDEX start, int len, short* buf) const {
	short tmp=isNull()?SHRT_MIN:(val_<0?(val_-0.5):(val_+0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const short* Float::getShortConst(INDEX start, int len, short* buf) const {
	short tmp=isNull()?SHRT_MIN:(val_<0?(val_-0.5):(val_+0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Float::getInt(INDEX start, int len, int* buf) const {
	int tmp=isNull()?INT_MIN:(val_<0?(val_-0.5):(val_+0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const int* Float::getIntConst(INDEX start, int len, int* buf) const {
	int tmp=isNull()?INT_MIN:(val_<0?(val_-0.5):(val_+0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Float::getLong(INDEX start, int len, long long* buf) const {
	long long tmp=isNull()?LLONG_MIN:(val_<0?(val_-0.5):(val_+0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const long long* Float::getLongConst(INDEX start, int len, long long* buf) const {
	long long tmp=isNull()?LLONG_MIN:(val_<0?(val_-0.5):(val_+0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

string Float::toString(float val){
	if(val == FLT_NMIN)
		return "";
	else if(std::isnan(val))
		return "NaN";
	else if(std::isinf(val))
		return "inf";

	float absVal = std::abs(val);
	if((absVal>0 && absVal<=0.000001) || absVal>=1000000.0){
		return floatingSciFormat_->format(val);
	}
	else
		return floatingNormFormat_->format(val);
}

Float* Float::parseFloat(const string& str){
	Float* p=0;

	if(str.compare("00")==0){
		p=new Float();
		p->setNull();
	}
	else
		p=new Float(atof(str.c_str()));
	return p;
}

IO_ERR Float::deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement){
	IO_ERR ret = in->readFloat(val_);
	if(ret == OK)
		numElement = 1;
	return ret;
}

bool Double::getChar(INDEX start, int len, char* buf) const {
	char tmp=isNull()?CHAR_MIN:(val_<0?(val_-0.5):(val_+0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const char* Double::getCharConst(INDEX start, int len, char* buf) const {
	char tmp=isNull()?CHAR_MIN:(val_<0?(val_-0.5):(val_+0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Double::getShort(INDEX start, int len, short* buf) const {
	short tmp=isNull()?SHRT_MIN:(val_<0?(val_-0.5):(val_+0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const short* Double::getShortConst(INDEX start, int len, short* buf) const {
	short tmp=isNull()?SHRT_MIN:(val_<0?(val_-0.5):(val_+0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Double::getInt(INDEX start, int len, int* buf) const {
	int tmp=isNull()?INT_MIN:(val_<0?(val_-0.5):(val_+0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const int* Double::getIntConst(INDEX start, int len, int* buf) const {
	int tmp=isNull()?INT_MIN:(val_<0?(val_-0.5):(val_+0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Double::getLong(INDEX start, int len, long long* buf) const {
	long long tmp=isNull()?LLONG_MIN:(val_<0?(val_-0.5):(val_+0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}
const long long* Double::getLongConst(INDEX start, int len, long long* buf) const {
	long long tmp=isNull()?LLONG_MIN:(val_<0?(val_-0.5):(val_+0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

string Double::toString(double val){
	if(val == DBL_NMIN)
		return "";
	else if(std::isnan(val))
		return "NaN";
	else if(std::isinf(val))
		return "inf";

	double absVal = std::abs(val);
	if((absVal>0 && absVal<=0.000001) || absVal>=1000000.0){
		return floatingSciFormat_->format(val);
	}
	else
		return floatingNormFormat_->format(val);
}

Double* Double::parseDouble(const string& str){
	Double* p=0;

	if(str.compare("00")==0){
		p=new Double();
		p->setNull();
	}
	else
		p=new Double(atof(str.c_str()));
	return p;
}

IO_ERR Double::deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement){
	IO_ERR ret = in->readDouble(val_);
	if(ret == OK)
		numElement = 1;
	return ret;
}

Date* Date::parseDate(const string& date){
	//format: 2012.09.01
	Date* p=0;

	if(date.compare("00")==0){
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

string Date::toString(int val){
	if(val == INT_MIN)
		return "";
	else
		return dateFormat_->format(val, DT_DATE);
}

DateTime::DateTime(int year, int month, int day, int hour, int minute,int second):
		TemporalScalar(countTemporalUnit(Util::countDays(year,month,day),86400,(hour*60+minute)*60+second)){
}

string DateTime::toString(int val){
	if(val == INT_MIN)
		return "";
	else
		return datetimeFormat_->format(val, DT_DATETIME);
}

string Month::toString(int val){
	if(val == INT_MIN)
		return "";
	else
		return monthFormat_->format(val, DT_MONTH);
}

string Time::toString(int val){
	if(val == INT_MIN)
		return "";
	else
		return timeFormat_->format(val, DT_TIME);
}

void Time::validate(){
	if(val_ >= 86400000 || val_ <0)
		val_ = INT_MIN;
}

string Minute::toString(int val){
	if(val == INT_MIN)
		return "";
	else
		return minuteFormat_->format(val, DT_MINUTE);
}

void Minute::validate(){
	if(val_ >= 1440 || val_ <0)
		val_ = INT_MIN;
}

string Second::toString(int val){
	if(val == INT_MIN)
		return "";
	else
		return secondFormat_->format(val, DT_SECOND);
}

void Second::validate(){
	if(val_ >= 86400 || val_ <0)
		val_ = INT_MIN;
}

Timestamp::Timestamp(int year, int month, int day,int hour, int minute, int second, int milliSecond):
	Long(countTemporalUnit(Util::countDays(year,month,day), 86400000ll, ((hour*60+minute)*60+second)*1000ll+milliSecond)){
}

string Timestamp::toString(long long val){
	if(val == LLONG_MIN)
		return "";
	else
		return timestampFormat_->format(val, DT_TIMESTAMP);
}

NanoTimestamp::NanoTimestamp(int year, int month, int day,int hour, int minute, int second, int nanoSecond):
	Long(countTemporalUnit(Util::countDays(year,month,day), 86400000000000ll, ((hour*60+minute)*60+second)*1000000000ll+nanoSecond)){
}

string NanoTimestamp::toString(long long val){
	if(val == LLONG_MIN)
		return "";
	else
		return nanotimestampFormat_->format(val, DT_NANOTIMESTAMP);
}

string NanoTime::toString(long long val){
	if(val == LLONG_MIN)
		return "";
	else
		return nanotimeFormat_->format(val, DT_NANOTIME);
}

void NanoTime::validate(){
	if(val_ >= 86400000000000ll || val_ <0)
		val_ = LLONG_MIN;
}

Month* Month::parseMonth(const string& str){
	//2012.01
	Month* p=0;

	if(str.compare("00")==0){
		p=new Month();
		p->setNull();
		return p;
	}
	else if(str.length()!=7)
		return p;

	int year,month;
	year=atoi(str.substr(0,4).c_str());
	if(year==0)
		return p;
	if(str[4]!='.')
		return p;
	month=atoi(str.substr(5,2).c_str());
	if(month==0 || month>12)
		return p;

	p=new Month(year,month);
	return p;
}

Time* Time::parseTime(const string& str){
	//23:04:59:001
	Time* p=0;
	if(str.compare("00")==0){
		p=new Time();
		p->setNull();
		return p;
	}

	int hour,minute,second,milliSecond=0;
	hour=atoi(str.substr(0,2).c_str());
	if(hour>=24 || str[2]!=':')
		return p;
	minute=atoi(str.substr(3,2).c_str());
	if(minute>=60 || str[5]!=':')
		return p;
	second=atoi(str.substr(6,2).c_str());
	if(second>=60)
		return p;
	if(str[8]=='.')
		milliSecond=atoi(str.substr(9,str.length()-9).c_str());
	p=new Time(hour,minute,second,milliSecond);
	return p;
}

NanoTime* NanoTime::parseNanoTime(const string& str){
	//23:04:59:000000001
	NanoTime* p=0;
	if(str.compare("00")==0){
		p=new NanoTime();
		p->setNull();
		return p;
	}

	int hour,minute,second,nanoSecond=0;
	hour=atoi(str.substr(0,2).c_str());
	if(hour>=24 || str[2]!=':')
		return p;
	minute=atoi(str.substr(3,2).c_str());
	if(minute>=60 || str[5]!=':')
		return p;
	second=atoi(str.substr(6,2).c_str());
	if(second>=60)
		return p;
	if(str[8]=='.'){
		int len  = str.length()-9;
		if(len == 9)
			nanoSecond=atoi(str.substr(9, len).c_str());
		else if(len == 6)
			nanoSecond=atoi(str.substr(9, len).c_str()) * 1000;
		else if(len == 3)
			nanoSecond=atoi(str.substr(9, len).c_str()) * 1000000;
		else
			return p;
	}
	p=new NanoTime(hour,minute,second,nanoSecond);
	return p;
}

Minute* Minute::parseMinute(const string& str){
	Minute* p=0;
	if(str.compare("00")==0){
		p=new Minute();
		p->setNull();
		return p;
	}

	int hour,minute;
	hour=atoi(str.substr(0,2).c_str());
	if(hour>=24 || str[2]!=':')
		return p;
	minute=atoi(str.substr(3,2).c_str());
	if(minute>=60)
		return p;
	p=new Minute(hour,minute);
	return p;
}

Second* Second::parseSecond(const string& str){
	Second* p=0;
	if(str.compare("00")==0){
		p=new Second();
		p->setNull();
		return p;
	}

	int hour,minute,second;
	hour=atoi(str.substr(0,2).c_str());
	if(hour>=24 || str[2]!=':')
		return p;
	minute=atoi(str.substr(3,2).c_str());
	if(minute>=60 || str[5]!=':')
		return p;
	second=atoi(str.substr(6,2).c_str());
	if(second>=60)
		return p;
	p=new Second(hour,minute,second);
	return p;
}

DateTime* DateTime::parseDateTime(const string& str){
	DateTime* p=0;

	if(str.compare("00")==0){
		p=new DateTime();
		p->setNull();
		return p;
	}

	int year,month,day,hour,minute,second;
	year=atoi(str.substr(0,4).c_str());
	if(year==0 ||str[4]!='.')
		return p;
	month=atoi(str.substr(5,2).c_str());
	if(month==0 || str[7]!='.')
		return p;
	day=atoi(str.substr(8,2).c_str());
	if(day==0 || (str[10]!=' ' && str[10]!='T'))
		return p;

	hour=atoi(str.substr(11,2).c_str());
	if(hour>=24 || str[13]!=':')
		return p;
	minute=atoi(str.substr(14,2).c_str());
	if(minute>=60 || str[16]!=':')
		return p;
	second=atoi(str.substr(17,2).c_str());
	if(second>=60)
		return p;
	p=new DateTime(year,month,day,hour,minute,second);
	return p;
}

Timestamp* Timestamp::parseTimestamp(const string& str){
	Timestamp* p=0;

	if(str.compare("00")==0){
		p=new Timestamp();
		p->setNull();
		return p;
	}

	int year,month,day,hour,minute,second,millisecond=0;
	year=atoi(str.substr(0,4).c_str());
	if(year==0 ||str[4]!='.')
		return p;
	month=atoi(str.substr(5,2).c_str());
	if(month==0 || str[7]!='.')
		return p;
	day=atoi(str.substr(8,2).c_str());
	if(day==0 || (str[10]!=' ' && str[10]!='T'))
		return p;

	hour=atoi(str.substr(11,2).c_str());
	if(hour>=24 || str[13]!=':')
		return p;
	minute=atoi(str.substr(14,2).c_str());
	if(minute>=60 || str[16]!=':')
		return p;
	second=atoi(str.substr(17,2).c_str());
	if(second>=60)
		return p;
	if(str[19]=='.')
		millisecond=atoi(str.substr(20,str.length()-20).c_str());
	p=new Timestamp(year,month,day,hour,minute,second,millisecond);
	return p;
}

NanoTimestamp* NanoTimestamp::parseNanoTimestamp(const string& str){
	NanoTimestamp* p=0;

	if(str.compare("00")==0){
		p=new NanoTimestamp();
		p->setNull();
		return p;
	}

	int year,month,day,hour,minute,second,nanosecond=0;
	year=atoi(str.substr(0,4).c_str());
	if(year==0 ||str[4]!='.')
		return p;
	month=atoi(str.substr(5,2).c_str());
	if(month==0 || str[7]!='.')
		return p;
	day=atoi(str.substr(8,2).c_str());
	if(day==0 || (str[10]!=' ' && str[10]!='T'))
		return p;

	hour=atoi(str.substr(11,2).c_str());
	if(hour>=24 || str[13]!=':')
		return p;
	minute=atoi(str.substr(14,2).c_str());
	if(minute>=60 || str[16]!=':')
		return p;
	second=atoi(str.substr(17,2).c_str());
	if(second>=60)
		return p;
	if(str[19]=='.'){
		int len = str.length()-20;
		if(len == 9)
			nanosecond=atoi(str.substr(20, len).c_str());
		else if(len == 6)
			nanosecond=atoi(str.substr(20, len).c_str()) * 1000;
		else if(len == 3)
			nanosecond=atoi(str.substr(20, len).c_str()) * 1000000;
		else
			return p;
	}
	p=new NanoTimestamp(year,month,day,hour,minute,second,nanosecond);
	return p;
}

DateHour::DateHour(int year, int month, int day, int hour):
    TemporalScalar(countTemporalUnit(Util::countDays(year,month,day),24,hour)){
}

string DateHour::toString(int val){
    if(val == INT_MIN)
        return "";
    else
        return datehourFormat_->format(val, DT_DATEHOUR);
}

DateHour* DateHour::parseDateHour(const string& str){
    DateHour* p=0;

    if(str.compare("00")==0){
        p=new DateHour();
        p->setNull();
        return p;
    }

    if(UNLIKELY(str.size() < 13))
        return p;
    int year,month,day,hour;
    year=atoi(str.substr(0,4).c_str());
    if(year==0 ||str[4]!='.')
        return p;
    month=atoi(str.substr(5,2).c_str());
    if(month==0 || str[7]!='.')
        return p;
    day=atoi(str.substr(8,2).c_str());
    if(day==0 || (str[10]!=' ' && str[10]!='T'))
        return p;

    hour=atoi(str.substr(11,2).c_str());
    if(hour>=24)
        return p;
    p=new DateHour(year,month,day,hour);
    return p;
}

ConstantSP Date::castTemporal(DATA_TYPE expectType) {
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
	else if (expectType == DT_DATETIME) {
		int result;

		val_ == INT_MIN ? result = INT_MIN : result = val_ * ratio;
		return Util::createObject(expectType, result);
	}
	else {
		int result;

		if (val_ == INT_MIN) {
			result = INT_MIN;
		}
		else {
			int year, month, day;
			Util::parseDate(val_, year, month, day);
			result = year * 12 + month - 1;
		}
		return Util::createObject(expectType, result);
	}
}
ConstantSP DateHour::castTemporal(DATA_TYPE expectType) {
	if (expectType != DT_DATEHOUR && (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP)) {
		throw RuntimeException("castTemporal from DATEHOUR to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType == DT_DATEHOUR)
		return getValue();

	long long ratio = Util::getTemporalConversionRatio(DT_DATETIME, expectType);
	if (expectType == DT_NANOTIMESTAMP || expectType == DT_TIMESTAMP) {
		long long result;
		val_ == INT_MIN ? result = LLONG_MIN : result = (long long)val_ * ratio * 3600;
		return Util::createObject(expectType, result);
	}
	else if (expectType == DT_DATETIME) {
		int result;
		val_ == INT_MIN ? result = INT_MIN : result = val_ * 3600;
		return Util::createObject(expectType, result);
	}
	else if (expectType == DT_DATE) {
		int result;
		val_ == INT_MIN ? result = INT_MIN : result = val_ * 3600 / (-ratio);
		return Util::createObject(expectType, result);
	}
	else if (expectType == DT_MONTH) {
		int result;
		if (val_ == INT_MIN) {
			result = INT_MIN;
		}
		else {
			int year, month, day;
			Util::parseDate(val_ * 3600 / 86400, year, month, day);
			result = year * 12 + month - 1;
		}
		return Util::createObject(expectType, result);
	}
	else if (expectType == DT_NANOTIME) {
		long long result;
		val_ == INT_MIN ? result = LLONG_MIN : result = (long long)val_ * 3600 % 86400 * 1000000000LL;
		return Util::createObject(expectType, result);
	}
	else {
		ratio = Util::getTemporalConversionRatio(DT_SECOND, expectType);
		int result;
		if (ratio > 0) {
			val_ == INT_MIN ? result = INT_MIN : result = val_ * 3600 % 86400 * ratio;
		}
		else {
			val_ == INT_MIN ? result = INT_MIN : result = val_ * 3600 % 86400 / (-ratio);
		}
		return Util::createObject(expectType, result);
	}
}

ConstantSP DateTime::castTemporal(DATA_TYPE expectType) {
	if (expectType != DT_DATEHOUR && (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP)) {
		throw RuntimeException("castTemporal from DATETIME to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType == DT_DATETIME)
		return getValue();

	if (expectType == DT_DATEHOUR) {
		int result;

		int tail = (val_ < 0) && (val_ % 3600);
		val_ == INT_MIN ? result = INT_MIN : result = val_ / 3600 - tail;
		return Util::createObject(expectType, result);
	}
	long long ratio = Util::getTemporalConversionRatio(DT_DATETIME, expectType);
	if (expectType == DT_NANOTIMESTAMP || expectType == DT_TIMESTAMP) {
		long long result;

		val_ == INT_MIN ? result = LLONG_MIN : result = (long long)val_ * ratio;
		return Util::createObject(expectType, result);
	}
	else if (expectType == DT_DATE) {
		int result;
		ratio = -ratio;

		int tail = (val_ < 0) && (val_ % ratio);
		val_ == INT_MIN ? result = INT_MIN : result = val_ / ratio - tail;
		return Util::createObject(expectType, result);
	}
	else if (expectType == DT_MONTH) {
		int result;

		if (val_ == INT_MIN) {
			result = INT_MIN;
		}
		else {
			int year, month, day;
			Util::parseDate(val_ / 86400, year, month, day);
			result = year * 12 + month - 1;
		}
		return Util::createObject(expectType, result);
	}
	else if (expectType == DT_NANOTIME) {
		long long result;

		int remainder = val_ % 86400;
		val_ == INT_MIN ? result = LLONG_MIN : result = (long long)(remainder + ((val_ < 0) && remainder) * 86400) * 1000000000LL;
		return Util::createObject(expectType, result);
	}
	else {
		ratio = Util::getTemporalConversionRatio(DT_SECOND, expectType);
		int result;
		if (ratio > 0) {
			int remainder = val_ % 86400;
			val_ == INT_MIN ? result = INT_MIN : result = (remainder + ((val_ < 0) && remainder) * 86400) * ratio;
		}
		else {
			ratio = -ratio;
			int remainder = val_ % 86400;
			val_ == INT_MIN ? result = INT_MIN : result = (remainder + ((val_ < 0) && remainder) * 86400) / ratio;
		}
		return Util::createObject(expectType, result);
	}
}

ConstantSP Minute::castTemporal(DATA_TYPE expectType) {
	if (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP) {
		throw RuntimeException("castTemporal from MINUTE to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType != DT_TIME && expectType != DT_NANOTIME && expectType != DT_SECOND && expectType != DT_MINUTE) {
		throw RuntimeException("castTemporal from MINUTE to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType == DT_MINUTE)
		return getValue();

	long long ratio = Util::getTemporalConversionRatio(DT_MINUTE, expectType);
	if (expectType == DT_NANOTIME) {
		long long result;

		val_ == INT_MIN ? result = LLONG_MIN : result = (long long)val_ * ratio;
		return Util::createObject(expectType, result);
	}
	else {
		int result;

		val_ == INT_MIN ? result = INT_MIN : result = val_ * ratio;
		return Util::createObject(expectType, result);
	}
}

ConstantSP Month::castTemporal(DATA_TYPE expectType) {
	if (expectType == DT_MONTH)
		return getValue();
	else
		throw RuntimeException("castTemporal from MONTH to " + Util::getDataTypeString(expectType) + " not supported ");
}

ConstantSP Second::castTemporal(DATA_TYPE expectType) {
	if (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP) {
		throw RuntimeException("castTemporal from SECOND to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType != DT_TIME && expectType != DT_NANOTIME && expectType != DT_SECOND && expectType != DT_MINUTE) {
		throw RuntimeException("castTemporal from SECOND to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType == DT_SECOND)
		return getValue();

	long long ratio = Util::getTemporalConversionRatio(DT_SECOND, expectType);
	if (expectType == DT_NANOTIME) {
		long long result;

		val_ == INT_MIN ? result = LLONG_MIN : result = val_ * ratio;
		return Util::createObject(expectType, result);
	}
	else if (expectType == DT_TIME) {
		int result;

		val_ == INT_MIN ? result = INT_MIN : result = val_ * ratio;
		return Util::createObject(expectType, result);
	}
	else {
		int result;

		val_ == INT_MIN ? result = INT_MIN : result = val_ / (-ratio);
		return Util::createObject(expectType, result);
	}
}
ConstantSP Time::castTemporal(DATA_TYPE expectType) {
	if (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP) {
		throw RuntimeException("castTemporal from TIME to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType != DT_TIME && expectType != DT_NANOTIME && expectType != DT_SECOND && expectType != DT_MINUTE) {
		throw RuntimeException("castTemporal from TIME to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType == DT_TIME)
		return getValue();

	long long ratio = Util::getTemporalConversionRatio(DT_TIME, expectType);
	if (expectType == DT_NANOTIME) {
		long long result;

		val_ == INT_MIN ? result = LLONG_MIN : result = (long long)val_ * ratio;
		return Util::createObject(expectType, result);
	}
	else {
		int result;

		val_ == INT_MIN ? result = INT_MIN : result = val_ / (-ratio);
		return Util::createObject(expectType, result);
	}
}

ConstantSP NanoTime::castTemporal(DATA_TYPE expectType) {
	if (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP) {
		throw RuntimeException("castTemporal from NANOTIME to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType != DT_TIME && expectType != DT_NANOTIME && expectType != DT_SECOND && expectType != DT_MINUTE) {
		throw RuntimeException("castTemporal from NANOTIME to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType == DT_NANOTIME)
		return getValue();

	long long ratio = Util::getTemporalConversionRatio(DT_NANOTIME, expectType);

	int result;
	val_ == LLONG_MIN ? result = INT_MIN : result = val_ / (-ratio);
	return Util::createObject(expectType, result);
}

ConstantSP Timestamp::castTemporal(DATA_TYPE expectType) {
	if (expectType != DT_DATEHOUR && (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP)) {
		throw RuntimeException("castTemporal from TIMESTAMP to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType == DT_TIMESTAMP)
		return getValue();

	if (expectType == DT_DATEHOUR) {
		int result;

		int tail = (val_ < 0) && (val_ % 3600);
		val_ == LLONG_MIN ? result = INT_MIN : result = val_ / 3600000LL - tail;
		return Util::createObject(expectType, result);
	}
	long long ratio = Util::getTemporalConversionRatio(DT_TIMESTAMP, expectType);
	if (expectType == DT_NANOTIMESTAMP) {
		long long result;

		val_ == LLONG_MIN ? result = LLONG_MIN : result = val_ * ratio;
		return Util::createObject(expectType, result);
	}
	else if (expectType == DT_DATE || expectType == DT_DATETIME) {
		int result;
		ratio = -ratio;

		int tail = (val_ < 0) && (val_ % ratio);
		val_ == LLONG_MIN ? result = INT_MIN : result = val_ / ratio - tail;
		return Util::createObject(expectType, result);
	}
	else if (expectType == DT_MONTH) {
		int result;

		if (val_ == LLONG_MIN) {
			result = INT_MIN;
		}
		else {
			int year, month, day;
			Util::parseDate(val_ / 86400000, year, month, day);
			result = year * 12 + month - 1;
		}
		return Util::createObject(expectType, result);
	}
	else if (expectType == DT_NANOTIME) {
		long long result;

		int remainder = val_ % 86400000;
		val_ == LLONG_MIN ? result = LLONG_MIN : result = (remainder + ((val_ < 0) && remainder) * 86400000) * 1000000ll;
		return Util::createObject(expectType, result);
	}
	else {
		ratio = Util::getTemporalConversionRatio(DT_TIME, expectType);
		int result;
		if (ratio < 0) ratio = -ratio;

		int remainder = val_ % 86400000;
		val_ == LLONG_MIN ? result = INT_MIN : result = (remainder + ((val_ < 0) && remainder) * 86400000) / ratio;
		return Util::createObject(expectType, result);
	}
}

ConstantSP NanoTimestamp::castTemporal(DATA_TYPE expectType) {
	if (expectType != DT_DATEHOUR && (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP)) {
		throw RuntimeException("castTemporal from NANOTIMESTAMP to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType == DT_NANOTIMESTAMP)
		return getValue();

	if (expectType == DT_DATEHOUR) {
		int result;

		int tail = (val_ < 0) && (val_ % 3600000000000ll);
		val_ == LLONG_MIN ? result = INT_MIN : result = val_ / 3600000000000ll - tail;
		return Util::createObject(expectType, result);
	}
	long long ratio = -Util::getTemporalConversionRatio(DT_NANOTIMESTAMP, expectType);
	if (expectType == DT_TIMESTAMP) {
		long long result;

		int tail = (val_ < 0) && (val_ % ratio);
		val_ == LLONG_MIN ? result = LLONG_MIN : result = val_ / ratio - tail;
		return Util::createObject(expectType, result);
	}
	else if (expectType == DT_DATE || expectType == DT_DATETIME) {
		int result;

		int tail = (val_ < 0) && (val_ % ratio);
		val_ == LLONG_MIN ? result = INT_MIN : result = val_ / ratio - tail;
		return Util::createObject(expectType, result);
	}
	else if (expectType == DT_MONTH) {
		int result;

		if (val_ == LLONG_MIN) {
			result = INT_MIN;
		}
		else {
			int year, month, day;
			Util::parseDate(val_ / 86400000000000ll, year, month, day);
			result = year * 12 + month - 1;
		}
		return Util::createObject(expectType, result);
	}
	else if (expectType == DT_NANOTIME) {
		long long result;

		long long remainder = val_ % 86400000000000ll;
		val_ == LLONG_MIN ? result = LLONG_MIN : result = (remainder + (val_ < 0 && remainder) * 86400000000000ll);
		return Util::createObject(expectType, result);
	}
	else {
		ratio = Util::getTemporalConversionRatio(DT_NANOTIME, expectType);
		int result;
		ratio = -ratio;

		long long remainder = val_ % 86400000000000ll;
		val_ == LLONG_MIN ? result = INT_MIN : result = (remainder + (val_ < 0 && remainder) * 86400000000000ll) / ratio;
		return Util::createObject(expectType, result);
	}
}

};
