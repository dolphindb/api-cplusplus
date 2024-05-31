#include "DFSChunkMeta.h"
#include "ScalarImp.h"
#include "SysIO.h"
#include "ConstantImp.h"
namespace dolphindb {

DFSChunkMeta::DFSChunkMeta(const std::string& path, const Guid& id, int version, int sz, CHUNK_TYPE chunkType, const std::vector<std::string>& sites, long long cid)
    : Constant(2051), type_(chunkType), replicaCount_(static_cast<char>(sites.size())), version_(version), size_(sz), sites_(0), path_(path), cid_(cid), id_(id) {
    if (replicaCount_ == 0)
        return;
    sites_ = new std::string[replicaCount_];
    for (int i = 0; i < replicaCount_; ++i)
        sites_[i] = sites[i];
}

DFSChunkMeta::DFSChunkMeta(const std::string& path, const Guid& id, int version, int sz, CHUNK_TYPE chunkType, const std::string* sites, int siteCount, long long cid)
    : Constant(2051), type_(chunkType), replicaCount_(siteCount), version_(version), size_(sz), sites_(0), path_(path), cid_(cid), id_(id) {
    if (replicaCount_ == 0)
        return;
    sites_ = new std::string[replicaCount_];
    for (int i = 0; i < replicaCount_; ++i)
        sites_[i] = sites[i];
}

DFSChunkMeta::DFSChunkMeta(const DataInputStreamSP& in) : Constant(2051), sites_(0), id_(false) {
    IO_ERR ret = in->readString(path_);
    if (ret != OK)
        throw RuntimeException("Failed to deserialize DFSChunkMeta object.");

    char guid[16];
    in->read(guid, 16);
    in->readInt(version_);
    in->readIndex(size_);
    in->readChar(type_);
    ret = in->readChar(replicaCount_);
    if (ret != OK)
        throw RuntimeException("Failed to deserialize DFSChunkMeta object.");
    if (replicaCount_ > 0)
        sites_ = new std::string[replicaCount_];
    for (int i = 0; i < replicaCount_; ++i) {
        std::string site;
        if ((ret = in->readString(site)) != OK)
            throw RuntimeException("Failed to deserialize DFSChunkMeta object.");
        sites_[i] = site;
    }
    id_ = Guid((unsigned char*)guid);
    if (in->readLong(cid_) != OK)
        throw RuntimeException("Failed to deserialize DFSChunkMeta object.");
}

DFSChunkMeta::~DFSChunkMeta() {
    if (sites_)
        delete[] sites_;
}

std::string DFSChunkMeta::getString() const {
    std::string str(isTablet() ? "Tablet[" : "FileBlock[");
    str.append(path_);
    str.append(", ");
    str.append(id_.getString());
    str.append(", {");
    for (int i = 0; i < replicaCount_; ++i) {
        if (i > 0)
            str.append(", ");
        str.append(sites_[i]);
    }
    str.append("}, v");
    str.append(std::to_string(version_));
    str.append(", ");
    str.append(std::to_string(size_));
    str.append(", c");
    str.append(std::to_string(cid_));
    if (isSplittable())
        str.append(", splittable]");
    else
        str.append("]");
    return str;
}

long long DFSChunkMeta::getAllocatedMemory() const {
    long long length = 33 + sizeof(sites_) + (1 + replicaCount_) * (1 + sizeof(std::string)) + path_.size();
    for (int i = 0; i < replicaCount_; ++i)
        length += sites_[i].size();
    return length;
}

ConstantSP DFSChunkMeta::getMember(const ConstantSP& key) const {
    if (key->getCategory() != LITERAL || (!key->isScalar() && !key->isArray()))
        throw RuntimeException("DFSChunkMeta attribute must be string type scalar or vector.");
    if (key->isScalar())
        return getAttribute(key->getString());
    else {
        int keySize = key->size();
        ConstantSP result = Util::createVector(DT_ANY, keySize);
        for (int i = 0; i < keySize; ++i) {
            result->set(i, getAttribute(key->getString(i)));
        }
        return result;
    }
}

ConstantSP DFSChunkMeta::getSiteVector() const {
    ConstantSP vec = new StringVector(replicaCount_, replicaCount_);
    for (int i = 0; i < replicaCount_; ++i)
        vec->setString(i, sites_[i]);
    return vec;
}

ConstantSP DFSChunkMeta::getAttribute(const std::string& attr) const {
    if (attr == "path")
        return new String(path_);
    else if (attr == "id")
        return new String(id_.getString());
    else if (attr == "cid")
        return new Long(cid_);
    else if (attr == "version")
        return new Int(version_);
    else if (attr == "sites")
        return getSiteVector();
    else if (attr == "size") {
        ConstantSP obj = Util::createConstant(DT_INDEX);
        obj->setIndex(size_);
        return obj;
    } else if (attr == "isTablet")
        return new Bool(isTablet());
    else if (attr == "splittable")
        return new Bool(isSplittable());
    else
        return Constant::void_;
}

ConstantSP DFSChunkMeta::keys() const {
    std::vector<std::string> attrs({"path", "id", "version", "size", "isTablet", "splittable", "sites", "cid"});
    return new StringVector(attrs, static_cast<INDEX>(attrs.size()), false);
}

ConstantSP DFSChunkMeta::values() const {
    ConstantSP result = Util::createVector(DT_ANY, 8);
    result->set(0, new String(path_));
    result->set(1, new String(id_.getString()));
    result->set(2, new Int(version_));
    ConstantSP sizeObj = Util::createConstant(DT_INDEX);
    sizeObj->setIndex(size_);
    result->set(3, sizeObj);
    result->set(4, new Bool(isTablet()));
    result->set(5, new Bool(isSplittable()));
    result->set(6, getSiteVector());
    result->set(7, new Long(cid_));
    return result;
}

}