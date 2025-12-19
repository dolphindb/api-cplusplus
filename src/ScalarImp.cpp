/*
 * ScalarImp.cpp
 *
 *  Created on: May 10, 2017
 *      Author: dzhou
 */

#include "ScalarImp.h"
#include "Constant.h"
#include "ConstantImp.h"
#include "Exceptions.h"
#include "Format.h"
#include "SysIO.h"
#include "Types.h"
#include "Util.h"

#define FMT_UNICODE 0
#include "spdlog/fmt/bundled/format.h"

#ifdef __linux__
#include <uuid/uuid.h>
#else
#include <objbase.h>
#endif

#include <climits>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <tuple>

namespace dolphindb {

Uuid::Uuid(bool newUuid){
	if(!newUuid){
		memset(uuid_, 0, 16);
	}
	else{
#ifdef _WIN32
	CoCreateGuid((GUID*)uuid_);
#else
	uuid_generate(uuid_);
#endif
	}
}

Uuid::Uuid(const unsigned char* uuid){
	memcpy(uuid_, uuid, 16);
}

Uuid::Uuid(const char* guid, size_t len){
	if(len == 0)
		memset(uuid_, 0, 16);
	else if(len != 36 || !Util::fromGuid(guid, uuid_))
		throw RuntimeException("Invalid UUID string");
}

Uuid::Uuid(const Uuid& copy)
	: Int128()
{
	memcpy(uuid_, copy.uuid_, 16);
}

Uuid* Uuid::parseUuid(const char* str, size_t len){
	return new Uuid(str, len);
}

bool Uuid::parseUuid(const char* str, size_t len, unsigned char *buf){
	if(len == 0)
		memset(buf, 0, 16);
	else if(len != 36 || !Util::fromGuid(str, buf))
		return false;
	return true;
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
std::string IPAddr::toString(const unsigned char* data) {
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
#ifdef _MSC_VER
				cursor += sprintf_s(buf + cursor, 40 - cursor, "%d", data[i]);
#else
				cursor += sprintf(buf + cursor, "%d", data[i]);
#endif
				buf[cursor++] = '.';
			}
		} else{
			for(int i=12; i<16; ++i){
#ifdef _MSC_VER
				cursor += sprintf_s(buf + cursor, 40 - cursor, "%d", data[i]);
#else
				cursor += sprintf(buf + cursor, "%d", data[i]);
#endif
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
	return std::string(buf, cursor - 1);
}

IPAddr* IPAddr::parseIPAddr(const char* str, size_t len){
	unsigned char buf[16];
	if(parseIPAddr(str, len, buf))
		return new IPAddr(buf);
	return nullptr;
}

bool IPAddr::parseIPAddr(const char* str, size_t len, unsigned char* buf){
	if(len < 7)
		return false;
	int i = 0;
	for(i=0; i<4 && str[i] != '.'; ++i);
	if(i >= 4)
		return parseIP6(str, len, buf);
	return parseIP4(str, len, buf);
}

bool IPAddr::parseIP4(const char* str, size_t len, unsigned char* buf){
	int byteIndex = 0;
	int curByte = 0;

	for(size_t i=0; i<=len; ++i){
		if(i==len || str[i] == '.'){
			if(curByte < 0 || curByte > 255 || byteIndex > 3)
				return false;
			buf[Util::LITTLE_ENDIAN_ORDER ? 3 - byteIndex++ : 12 + byteIndex++ ] = static_cast<unsigned char>(curByte);
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
	size_t curByte = 0;
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
				if(curByte > 65535 || byteIndex > 15)
					return false;
				if(Util::LITTLE_ENDIAN_ORDER){
					buf[15 - byteIndex++] = static_cast<unsigned char>(curByte>>8);
					buf[15 - byteIndex++] = curByte & 255U;
				}
				else{
					buf[byteIndex++] = static_cast<unsigned char>(curByte>>8);
					buf[byteIndex++] = curByte & 255;
				}
				curByte = 0;
			}
			lastColonPos = static_cast<int>(i);
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

} // namespace dolphindb
