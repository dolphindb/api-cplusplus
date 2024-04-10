#pragma once

#include "Constant.h"
#include "SmartPointer.h"
namespace dolphindb {

class EXPORT_DECL DFSChunkMeta : public Constant{
public:
    DFSChunkMeta(const string& path, const Guid& id, int version, int size, CHUNK_TYPE chunkType, const std::vector<string>& sites, long long cid);
    DFSChunkMeta(const string& path, const Guid& id, int version, int size, CHUNK_TYPE chunkType, const string* sites, int siteCount, long long cid);
    DFSChunkMeta(const DataInputStreamSP& in);
    virtual ~DFSChunkMeta();
    virtual int size() const {return size_;}
    virtual string getString() const;
    virtual long long getAllocatedMemory() const;
    virtual ConstantSP getMember(const ConstantSP& key) const;
    virtual ConstantSP get(const ConstantSP& index) const {return getMember(index);}
    virtual ConstantSP keys() const;
    virtual ConstantSP values() const;
    virtual DATA_TYPE getType() const {return DT_DICTIONARY;}
    virtual DATA_TYPE getRawType() const {return DT_DICTIONARY;}
    virtual DATA_CATEGORY getCategory() const {return MIXED;}
    virtual ConstantSP getInstance() const {return getValue();}
    virtual ConstantSP getValue() const {return new DFSChunkMeta(path_, id_, version_, size_, (CHUNK_TYPE)type_, sites_, replicaCount_, cid_);}
    inline const string& getPath() const {return path_;}
    inline const Guid& getId() const {return id_;}
    inline long long getCommitId() const {return cid_;}
    inline void setCommitId(long long cid) { cid_ = cid;}
    inline int getVersion() const {return version_;}
    inline void setVersion(int version){version_ = version;}
    inline void setSize(int size){size_ = size;}
    inline int getCopyCount() const {return replicaCount_;}
    inline const string& getCopySite(int index) const {return sites_[index];}
    inline bool isTablet() const { return type_ == TABLET_CHUNK;}
    inline bool isFileBlock() const { return type_ == FILE_CHUNK;}
    inline bool isSplittable() const { return type_ == SPLIT_TABLET_CHUNK;}
    inline bool isSmallFileBlock() const {return type_ == SMALLFILE_CHUNK;}
    inline CHUNK_TYPE getChunkType() const {return (CHUNK_TYPE)type_;}

protected:
    ConstantSP getAttribute(const string& attr) const;
    ConstantSP getSiteVector() const;

private:
    char type_;
    char replicaCount_;
    int version_;
    int size_;
    string* sites_;
    string path_;
    long long cid_;
    Guid id_;
};
typedef SmartPointer<DFSChunkMeta> DFSChunkMetaSP;
}