#pragma once

#include "Constant.h"
#include "SmartPointer.h"

namespace dolphindb {

class EXPORT_DECL Set: public Constant {
public:
    Set() : Constant(1027){}
    virtual ~Set() {}
    virtual void clear()=0;
    virtual bool remove(const ConstantSP& value) = 0;
    virtual bool append(const ConstantSP& value) = 0;
    virtual bool inverse(const ConstantSP& value) = 0;
    virtual void contain(const ConstantSP& target, const ConstantSP& resultSP) const = 0;
    virtual bool isSuperset(const ConstantSP& target) const = 0;
    virtual ConstantSP interaction(const ConstantSP& target) const = 0;
    virtual ConstantSP getSubVector(INDEX start, INDEX length) const = 0;
    virtual std::string getScript() const {return "set()";}
    virtual bool isLargeConstant() const {return true;}
};
typedef SmartPointer<Set> SetSP;
}