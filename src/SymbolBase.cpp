#include "SymbolBase.h"
#include "SysIO.h"
#include "Exceptions.h"
#include <string.h>

#define SYMBOLBASE_MAX_SIZE 1<<21

namespace dolphindb {

SymbolBase::SymbolBase(int id, const DataInputStreamSP& in, IO_ERR& ret){
    id_ = id;
    int sz;
    ret =  in->readInt(sz);
    if(ret != OK)
        return;
    for(int i = 0; i < sz; i++){
        std::string s;
        ret = in->readString(s);
        if(ret != OK)
            return;
        syms_.emplace_back(s);
    }
}

int SymbolBase::serialize(char* buf, int bufSize, INDEX indexStart, int offset, int& numElement, int& partial) const {
    if(indexStart >= (INDEX)syms_.size())
        return -1;
    int index = indexStart;
    int initSize = bufSize;
    while(index < (int)syms_.size() && bufSize > 0){
        if(syms_[index].size() >= 262144){
            throw RuntimeException("String in symbol too long, Serialization failed, length must be less than 256K bytes");
        }
        int sz = std::min(bufSize, (int)syms_[index].size() + 1 - offset);
        memcpy(buf, syms_[index].data() + offset,  sz);
        buf += sz;
        bufSize -= sz;
        offset += sz;
        if(offset == (int)syms_[index].size() + 1){
            offset = 0;
            index ++;
        }
    }
    partial = offset;
    numElement = index - indexStart;
    return initSize - bufSize;
}

int SymbolBase::find(const std::string& symbol){
    if(symMap_.empty()){
        if(syms_.size() > 0 && syms_[0] != "")
            throw RuntimeException("A symbol base's first key must be empty string.");
        if(syms_.size() == 0){
            symMap_[""] = 0;
            syms_.emplace_back("");
        }    
        else {
            int count = static_cast<int>(syms_.size());
            for(int i = 0; i < count; ++i)
                symMap_[syms_[i]] = i;
        }
    }
    int index = -1;
    auto it = symMap_.find(symbol);
    if(it != symMap_.end()) index = it->second;
    return index;
}

int SymbolBase::findAndInsert(const std::string& symbol){
    if(symbol.find('\0') != std::string::npos){
        throw RuntimeException("A String cannot contain the character '\\0'");
    }
    if(symMap_.empty()){
        if(syms_.size() > 0 && syms_[0] != "")
            throw RuntimeException("A symbol base's first key must be empty string.");
        if(syms_.size() == 0){
            symMap_[""] = 0;
            syms_.emplace_back("");
        }    
        else {
            int count = static_cast<int>(syms_.size());
            for(int i = 0; i < count; ++i)
                symMap_[syms_[i]] = i;
        }
    }
    int index = -1;
    auto it = symMap_.find(symbol);
    if(it == symMap_.end()){
        index = static_cast<int>(symMap_.size());
        if(index >= SYMBOLBASE_MAX_SIZE){
            throw RuntimeException("One symbol base's size can't exceed 2097152.");
        }
        symMap_[symbol] = index;
        syms_.emplace_back(symbol);
    }
    else{
        index = it->second;
    }
    return index;
}

}