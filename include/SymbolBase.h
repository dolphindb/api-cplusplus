// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4251 )
#endif

#include "SmartPointer.h"
#include "Types.h"
#include <unordered_map>
#include <vector>

namespace dolphindb {

class DataInputStream;
typedef SmartPointer<DataInputStream> DataInputStreamSP;

class EXPORT_DECL SymbolBase{
public:
    SymbolBase(int id):id_(id){}

    SymbolBase(int id, const DataInputStreamSP& in, IO_ERR& ret);

    const std::string& getSymbol(int index) const { return syms_[index];}

    int serialize(char* buf, int bufSize, INDEX indexStart, int offset, int& numElement, int& partial) const;
    
    int find(const std::string& symbol);

    int findAndInsert(const std::string& symbol);

    std::size_t size() const {return  symMap_.size();}

    const int& getID(){return id_;}
private:
    int id_;
    std::unordered_map<std::string, int> symMap_;
    std::vector<std::string> syms_;
};

typedef SmartPointer<SymbolBase> SymbolBaseSP;
}

#ifdef _MSC_VER
#pragma warning( pop )
#endif
