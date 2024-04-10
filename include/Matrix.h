#pragma once

#include "Constant.h"
#include "Util.h"
namespace dolphindb {

class EXPORT_DECL Matrix{
public:
    Matrix(int cols, int rows);
    virtual ~Matrix(){}
    void setRowLabel(const ConstantSP& label);
    void setColumnLabel(const ConstantSP& label);
    bool reshape(INDEX cols, INDEX rows);
    string getString() const;
    string getString(INDEX index) const ;
    ConstantSP get(const ConstantSP& index) const ;
    bool set(const ConstantSP index, const ConstantSP& value);
    virtual string getString(int column, int row) const = 0;
    virtual ConstantSP getInstance(INDEX size) const = 0;
    virtual ConstantSP getColumn(INDEX index) const = 0;
    virtual bool setColumn(INDEX index, const ConstantSP& value)=0;
    virtual int asof(const ConstantSP& value) const {throw RuntimeException("asof not supported.");}
protected:
    int cols_;
    int rows_;
    ConstantSP rowLabel_;
    ConstantSP colLabel_;
};

}