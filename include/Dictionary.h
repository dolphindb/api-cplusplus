// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#if defined(_MSC_VER)
#pragma warning( push )
#pragma warning( disable : 4100 )
#elif defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#else // gcc
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

#include "Constant.h"
#include "SmartPointer.h"
namespace dolphindb {

class EXPORT_DECL Dictionary:public Constant{
public:
    Dictionary() : Constant(1283){}
    virtual ~Dictionary() {}
    virtual INDEX size() const = 0;
    virtual INDEX count() const = 0;
    virtual void clear()=0;
    virtual ConstantSP getMember(const ConstantSP& key) const =0;
    virtual ConstantSP getMember(const std::string& key) const {throw RuntimeException("String key not supported");}
    virtual ConstantSP get(INDEX column, INDEX row){throw RuntimeException("Dictionary does not support cell function");}
    virtual DATA_TYPE getKeyType() const = 0;
    virtual DATA_CATEGORY getKeyCategory() const = 0;
    virtual DATA_TYPE getType() const = 0;
    virtual DATA_CATEGORY getCategory() const = 0;
    virtual ConstantSP keys() const = 0;
    virtual ConstantSP values() const = 0;
    virtual std::string getString() const = 0;
    virtual std::string getScript() const {return "dict()";}
    virtual std::string getString(int index) const {throw RuntimeException("Dictionary::getString(int index) not supported");}
    virtual bool remove(const ConstantSP& key) = 0;
    using Constant::set;
    virtual bool set(const ConstantSP& key, const ConstantSP& value)=0;
    virtual bool set(const std::string& key, const ConstantSP& value){throw RuntimeException("String key not supported");}
    using Constant::get;
    virtual ConstantSP get(const ConstantSP& key) const {return getMember(key);}
    virtual void contain(const ConstantSP& target, const ConstantSP& resultSP) const = 0;
    virtual bool isLargeConstant() const {return true;}
};

typedef SmartPointer<Dictionary> DictionarySP;
}

#if defined(_MSC_VER)
#pragma warning( pop )
#elif defined(__clang__)
#pragma clang diagnostic pop
#else // gcc
#pragma GCC diagnostic pop
#endif
