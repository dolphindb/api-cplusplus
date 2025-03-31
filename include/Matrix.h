// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4251 )
#endif

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
    std::string getString() const;
    std::string getString(INDEX index) const ;
    ConstantSP get(const ConstantSP& index) const ;
    bool set(const ConstantSP index, const ConstantSP& value);
    virtual std::string getString(int column, int row) const = 0;
    virtual ConstantSP getInstance(INDEX size) const = 0;
    virtual ConstantSP getColumn(INDEX index) const = 0;
    virtual bool setColumn(INDEX index, const ConstantSP& value)=0;
    virtual int asof(const ConstantSP& value) const
    {
        std::ignore = value;
        throw RuntimeException("asof not supported.");
    }

protected:
    void calculateInvalidLength(INDEX colStart, int colLength,INDEX rowStart, int rowLength, int& invalidLenBeginning, int& invalidLenEnding) const;

protected:
    int cols_;
    int rows_;
    ConstantSP rowLabel_;
    ConstantSP colLabel_;
};

}

#ifdef _MSC_VER
#pragma warning( pop )
#endif
