#include "Guid.h"
#include "Util.h"

#ifdef WINDOWS
    #include <objbase.h>
#else
    #include <uuid/uuid.h>
#endif

namespace dolphindb {

uint32_t murmur32_16b(const unsigned char* key){
    const uint32_t m = 0x5bd1e995;
    const int r = 24;
    uint32_t h = 16;

    uint32_t k1 = *(uint32_t*)(key);
    uint32_t k2 = *(uint32_t*)(key + 4);
    uint32_t k3 = *(uint32_t*)(key + 8);
    uint32_t k4 = *(uint32_t*)(key + 12);

    k1 *= m;
    k1 ^= k1 >> r;
    k1 *= m;

    k2 *= m;
    k2 ^= k2 >> r;
    k2 *= m;

    k3 *= m;
    k3 ^= k3 >> r;
    k3 *= m;

    k4 *= m;
    k4 ^= k4 >> r;
    k4 *= m;

    // Mix 4 bytes at a time into the hash
    h *= m;
    h ^= k1;
    h *= m;
    h ^= k2;
    h *= m;
    h ^= k3;
    h *= m;
    h ^= k4;

    // Do a few final mixes of the hash to ensure the last few
    // bytes are well-incorporated.
    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;

    return h;
}

//-----------------------------------------------------------------------------
// MurmurHash2, by Austin Appleby

// Note - This code makes a few assumptions about how your machine behaves -

// 1. We can read a 4-byte value from any address without crashing
// 2. sizeof(int) == 4

// And it has a few limitations -

// 1. It will not work incrementally.
// 2. It will not produce the same results on little-endian and big-endian
//    machines.

uint32_t murmur32(const char *key, size_t len){
    // 'm' and 'r' are mixing constants generated offline.
    // They're not really 'magic', they just happen to work well.

    const uint32_t m = 0x5bd1e995;
    const int r = 24;

    // Initialize the hash to a 'random' value
    uint32_t h = static_cast<uint32_t>(len);

    // Mix 4 bytes at a time into the hash

    const unsigned char *data = (const unsigned char *)key;

    while(len >= 4)
    {
        uint32_t k = *(uint32_t *)data;

        k *= m;
        k ^= k >> r;
        k *= m;

        h *= m;
        h ^= k;

        data += 4;
        len -= 4;
    }

    // Handle the last few bytes of the input array

    switch(len)
    {
    case 3: h ^= data[2] << 16;
    /* no break */
    case 2: h ^= data[1] << 8;
    /* no break */
    case 1: h ^= data[0];
            h *= m;
    };

    // Do a few final mixes of the hash to ensure the last few
    // bytes are well-incorporated.

    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;

    return h;
}
Guid::Guid(bool newGuid) {
    if (!newGuid) {
        memset(uuid_, 0, 16);
    } else {
#ifdef WINDOWS
        CoCreateGuid((GUID*)uuid_);
#else
        uuid_generate(uuid_);
#endif
    }
}

Guid::Guid(unsigned char* guid) {
    memcpy(uuid_, guid, 16);
}

Guid::Guid(const std::string& guid) {
    if(guid.size() != 36 || !Util::fromGuid(guid.c_str(), uuid_))
        throw RuntimeException("Invalid UUID string");
}

Guid::Guid(const Guid& copy) {
    memcpy(uuid_, copy.uuid_, 16);
}

bool Guid::isZero() const {
    const unsigned char* a = (const unsigned char*)uuid_;
    return (*(long long*)a) == 0 && (*(long long*)(a + 8)) == 0;
}

string Guid::getString() const {
    return getString(uuid_);
}

string Guid::getString(const unsigned char* uuid) {
    char buf[36];
    Util::toGuid(uuid, buf);
    return string(buf, 36);
}

uint64_t GuidHash::operator()(const Guid& guid) const {
    return murmur32_16b(guid.bytes());
}

}