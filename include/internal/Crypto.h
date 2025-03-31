// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#ifdef USE_OPENSSL
#include <openssl/rsa.h>
#endif
#include <string>
#include <vector>

namespace dolphindb {

#ifdef USE_OPENSSL

class Crypto {
public:
    Crypto(const std::string &publicKey);
    ~Crypto();
    std::string RSAEncrypt(const std::string &text) const;
    static std::string Base64Encode(const std::vector<unsigned char> &text, int flags = 0);
    static std::vector<unsigned char> Base64Decode(const std::string &text, int flags = 0);
    static std::string generateNonce(int length = 16);

private:
    void freeCrypto();
    void printOpenSSLError() const;

private:
    BIO *keyBio_{nullptr};
    EVP_PKEY *rsa_{nullptr};
    EVP_PKEY_CTX *ctx_{nullptr};
};

#endif

}
