#ifdef USE_OPENSSL
#include "internal/Crypto.h"
#include "Exceptions.h"
#include <openssl/pem.h>
#include <openssl/err.h>
#include <string>
#include <vector>
#include <iostream>
#include <tuple>

namespace dolphindb {

Crypto::Crypto(const std::string &publicKey)
{
    bool initSuccess{false};
    keyBio_ = BIO_new_mem_buf(publicKey.data(), static_cast<int>(publicKey.size()) + 1);
    rsa_ = PEM_read_bio_PUBKEY(keyBio_, nullptr, nullptr, nullptr);
    if (rsa_ != nullptr) {
        ctx_ = EVP_PKEY_CTX_new(rsa_, nullptr);
    }
    if (ctx_ != nullptr) {
        initSuccess = (EVP_PKEY_encrypt_init(ctx_) == 1);
    }
    if (!initSuccess) {
        freeCrypto();
        throw RuntimeException("Invalid RSA public key.");
    }
}

void Crypto::freeCrypto()
{
    // All OpenSSL *_free API does nothing if the argument is null.
    EVP_PKEY_CTX_free(ctx_);
    EVP_PKEY_free(rsa_);
    BIO_free(keyBio_);
}

Crypto::~Crypto()
{
    freeCrypto();
}

auto Crypto::RSAEncrypt(const std::string &text) const -> std::string
{
    int rsa_size = EVP_PKEY_size(rsa_);
    std::vector<unsigned char> encrypted(rsa_size);
    size_t encrypted_len;
    if (EVP_PKEY_encrypt(ctx_, encrypted.data(), &encrypted_len, (const unsigned char*)text.c_str(), text.size()) != 1) {
        printOpenSSLError();
        throw RuntimeException("Failed to encrypt userId or password.");
    }
    return Base64Encode(encrypted);
}

void Crypto::printOpenSSLError() const
{
    unsigned long errCode;
    while ((errCode = ERR_get_error()) != 0) {
        char errMessage[120];
        ERR_error_string_n(errCode, errMessage, sizeof(errMessage));
        std::cerr << "OpenSSL error: " << errMessage << std::endl;
    }
}

std::string Crypto::Base64Encode(const std::vector<unsigned char> &text) const
{
    BIO* b64 = BIO_new(BIO_f_base64());
    BIO* bio = BIO_new(BIO_s_mem());
    bio = BIO_push(b64, bio);
    BIO_write(bio, text.data(), static_cast<int>(text.size()));
    std::ignore = BIO_flush(bio);
    BUF_MEM* bufferPtr;
    BIO_get_mem_ptr(bio, &bufferPtr);
    std::string encoded(bufferPtr->data, bufferPtr->length);
    BIO_free_all(bio);
    return encoded;
}

}

#endif
