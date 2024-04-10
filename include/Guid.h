#pragma once

#include "Exports.h"
#include <string>
#include <string.h>

namespace dolphindb {

uint32_t murmur32_16b(const unsigned char* key);
uint32_t murmur32(const char *key, size_t len);

class EXPORT_DECL Guid {
public:
    Guid(bool newGuid = false);
    Guid(unsigned char* guid);
    Guid(const std::string& guid);
    Guid(const Guid& copy);
    Guid(unsigned long long high, unsigned long long low){
#ifndef BIGENDIANNESS
        memcpy((char*)uuid_, (char*)&low, 8);
        memcpy((char*)uuid_ + 8, (char*)&high, 8);
#else
        memcpy((char*)uuid_, (char*)&high, 8);
        memcpy((char*)uuid_ + 8, (char*)&low, 8);
#endif
    }
    inline bool operator==(const Guid &other) const {
        const unsigned char* a = (const unsigned char*)uuid_;
        const unsigned char* b = (const unsigned char*)other.uuid_;
        return (*(long long*)a) == (*(long long*)b) && (*(long long*)(a+8)) == (*(long long*)(b+8));
    }
    inline bool operator!=(const Guid &other) const {
        const unsigned char* a = (const unsigned char*)uuid_;
        const unsigned char* b = (const unsigned char*)other.uuid_;
        return (*(long long*)a) != (*(long long*)b) || (*(long long*)(a+8)) != (*(long long*)(b+8));
    }
    inline bool operator<(const Guid &other) const {
        const unsigned char* a = (const unsigned char*)uuid_;
        const unsigned char* b = (const unsigned char*)other.uuid_;
#ifndef BIGENDIANNESS
        return (*(unsigned long long*)(a+8)) < (*(unsigned long long*)(b+8)) || ((*(unsigned long long*)(a+8)) == (*(unsigned long long*)(b+8)) && (*(unsigned long long*)a) < (*(unsigned long long*)b));
#else
        return (*(unsigned long long*)a) < (*(unsigned long long*)b) || ((*(unsigned long long*)a) == (*(unsigned long long*)b) && (*(unsigned long long*)(a+8)) < (*(unsigned long long*)(b+8)));
#endif
    }
    inline bool operator>(const Guid &other) const {
        const unsigned char* a = (const unsigned char*)uuid_;
        const unsigned char* b = (const unsigned char*)other.uuid_;
#ifndef BIGENDIANNESS
        return (*(unsigned long long*)(a+8)) > (*(unsigned long long*)(b+8)) || ((*(unsigned long long*)(a+8)) == (*(unsigned long long*)(b+8)) && (*(unsigned long long*)a) > (*(unsigned long long*)b));
#else
        return (*(unsigned long long*)a) > (*(unsigned long long*)b) || ((*(unsigned long long*)a) == (*(unsigned long long*)b) && (*(unsigned long long*)(a+8)) > (*(unsigned long long*)(b+8)));
#endif
    }
    inline bool operator<=(const Guid &other) const {
        const unsigned char* a = (const unsigned char*)uuid_;
        const unsigned char* b = (const unsigned char*)other.uuid_;
#ifndef BIGENDIANNESS
        return (*(unsigned long long*)(a+8)) < (*(unsigned long long*)(b+8)) || ((*(unsigned long long*)(a+8)) == (*(unsigned long long*)(b+8)) && (*(unsigned long long*)a) <= (*(unsigned long long*)b));
#else
        return (*(unsigned long long*)a) < (*(unsigned long long*)b) || ((*(unsigned long long*)a) == (*(unsigned long long*)b) && (*(unsigned long long*)(a+8)) <= (*(unsigned long long*)(b+8)));
#endif
    }
    inline bool operator>=(const Guid &other) const {
        const unsigned char* a = (const unsigned char*)uuid_;
        const unsigned char* b = (const unsigned char*)other.uuid_;
#ifndef BIGENDIANNESS
        return (*(unsigned long long*)(a+8)) > (*(unsigned long long*)(b+8)) || ((*(unsigned long long*)(a+8)) == (*(unsigned long long*)(b+8)) && (*(unsigned long long*)a) >= (*(unsigned long long*)b));
#else
        return (*(unsigned long long*)a) > (*(unsigned long long*)b) || ((*(unsigned long long*)a) == (*(unsigned long long*)b) && (*(unsigned long long*)(a+8)) >= (*(unsigned long long*)(b+8)));
#endif
    }
    inline int compare(const Guid &other) const { return (*this < other) ? -1 : (*this > other ? 1 : 0);}
    inline unsigned char operator[](int i) const { return uuid_[i];}
    bool isZero() const;
    inline bool isNull() const {
        const unsigned char* a = (const unsigned char*)uuid_;
        return (*(long long*)a) == 0 && (*(long long*)(a+8)) == 0;
    }
    inline bool isValid() const {
        const unsigned char* a = (const unsigned char*)uuid_;
        return (*(long long*)a) != 0 || (*(long long*)(a+8)) != 0;
    }
    std::string getString() const;
    inline const unsigned char* bytes() const { return uuid_;}
    static std::string getString(const unsigned char* guid);

private:
    unsigned char uuid_[16];
};

struct GuidHash {
	uint64_t operator()(const Guid& guid) const;
};


}

namespace std {
template<>
struct hash<dolphindb::Guid> {
    size_t operator()(const dolphindb::Guid& val) const{
        return dolphindb::murmur32_16b(val.bytes());
    }
};

}
