#include "Matrix.h"

namespace dolphindb {

Matrix::Matrix(int cols, int rows) : cols_(cols), rows_(rows), rowLabel_(Constant::void_), colLabel_(Constant::void_) {
}

void Matrix::setRowLabel(const ConstantSP& label) {
    if (label->getType() == DT_VOID)
        rowLabel_ = label;
    else {
        if (label->getForm() != DF_VECTOR) {
            throw RuntimeException("Matrix's label must be a vector.");
        }
        if (label->isTemporary())
            rowLabel_ = label;
        else
            rowLabel_ = label->getValue();
    }
    rowLabel_->setTemporary(false);
}

void Matrix::setColumnLabel(const ConstantSP& label) {
    if (label->getType() == DT_VOID)
        colLabel_ = label;
    else {
        if (label->getForm() != DF_VECTOR) {
            throw RuntimeException("Matrix's label must be a vector.");
        }
        if (label->isTemporary())
            colLabel_ = label;
        else
            colLabel_ = label->getValue();
    }
    colLabel_->setTemporary(false);
}

bool Matrix::reshape(INDEX cols, INDEX rows) {
    if (cols_ == cols && rows_ == rows)
        return true;
    if (cols_ * rows_ != cols * rows && rows != rows_)
        return false;
    cols_ = cols;
    rows_ = rows;
    if (!colLabel_->isNothing() && colLabel_->size() != cols_)
        colLabel_ = Constant::void_;
    if (!rowLabel_->isNothing() && rowLabel_->size() != rows_)
        rowLabel_ = Constant::void_;
    return true;
}
std::string Matrix::getString() const {
    int rows = (std::min)(Util::DISPLAY_ROWS, rows_);
    int limitColMaxWidth = 25;
    std::size_t length = 0;
    int curCol = 0;
    int i;
    std::vector<std::string> list(rows + 1);
    std::vector<std::string> listTmp(rows + 1);
    std::string separator;
    std::size_t maxColWidth, curSize;

    // display row label
    if (!rowLabel_->isNull()) {
        listTmp[0] = "";
        maxColWidth = 0;
        for (i = 0; i < rows; i++) {
            listTmp[i + 1] = rowLabel_->getString(i);
            if (listTmp[i + 1].size() > maxColWidth)
                maxColWidth = listTmp[i + 1].size();
        }

        for (i = 0; i <= rows; i++) {
            curSize = listTmp[i].size();
            if (curSize <= maxColWidth) {
                list[i].append(listTmp[i]);
                if (curSize < maxColWidth)
                    list[i].append(maxColWidth - curSize, ' ');
            } else {
                if (maxColWidth > 3)
                    list[i].append(listTmp[i].substr(0, maxColWidth - 3));
                list[i].append("...");
            }
            list[i].append(1, i == 0 ? ' ' : '|');
        }

        maxColWidth++;
        separator.append(maxColWidth, ' ');
        length += maxColWidth;
    }

    while (length < static_cast<std::size_t>(Util::DISPLAY_WIDTH) && curCol < cols_) {
        listTmp[0] = colLabel_->isNull() ? "#" + Util::convert(curCol) : colLabel_->getString(curCol);
        maxColWidth = 0;
        for (i = 0; i < rows; i++) {
            listTmp[i + 1] = getString(curCol, i);
            if (listTmp[i + 1].size() > maxColWidth)
                maxColWidth = listTmp[i + 1].size();
        }
        if (maxColWidth > static_cast<std::size_t>(limitColMaxWidth))
            maxColWidth = limitColMaxWidth;
        if (listTmp[0].size() > maxColWidth)
            maxColWidth = (std::min)(limitColMaxWidth, (int)listTmp[0].size());
        separator.append(maxColWidth, '-');
        if (curCol < cols_ - 1) {
            maxColWidth++;
            separator.append(1, ' ');
        }

        if (length + maxColWidth > static_cast<std::size_t>(Util::DISPLAY_WIDTH) && curCol + 1 < cols_)
            break;

        for (i = 0; i <= rows; i++) {
            curSize = listTmp[i].size();
            if (curSize <= maxColWidth) {
                list[i].append(listTmp[i]);
                if (curSize < maxColWidth)
                    list[i].append(maxColWidth - curSize, ' ');
            } else {
                if (maxColWidth > 3)
                    list[i].append(listTmp[i].substr(0, maxColWidth - 3));
                list[i].append("...");
            }
        }
        length += maxColWidth;
        curCol++;
    }

    if (curCol < cols_) {
        for (i = 0; i <= rows; i++)
            list[i].append("...");
        separator.append(3, '-');
    }

    std::string resultStr(list[0]);
    resultStr.append("\n");
    resultStr.append(separator);
    resultStr.append("\n");
    for (i = 1; i <= rows; i++) {
        resultStr.append(list[i]);
        resultStr.append("\n");
    }
    if (rows < rows_)
        resultStr.append("...\n");
    return resultStr;
}

std::string Matrix::getString(INDEX index) const {
    int len = (std::min)(Util::DISPLAY_ROWS, rows_);
    std::string str("{");

    if (len > 0)
        str.append(getString(index, 0));
    for (int i = 1; i < len; ++i) {
        str.append(",");
        str.append(getString(index, i));
    }
    if (rows_ > len)
        str.append("...");
    str.append("}");
    return str;
}

ConstantSP Matrix::get(const ConstantSP& index) const {
    if (index->isScalar()) {
        int col = index->getInt();
        if (col < 0 || col >= cols_)
            throw OperatorRuntimeException("matrix", "The column index " + Util::convert(col) + " is out of range.");
        return getColumn(col);
    }

    ConstantSP indexCols(index);
    if (index->isPair()) {
        int colStart = index->isNull(0) ? 0 : index->getInt(0);
        int colEnd = index->isNull(1) ? cols_ : index->getInt(1);
        int length = std::abs(colEnd - colStart);

        indexCols = Util::createIndexVector(length, true);
        INDEX* data = indexCols->getIndexArray();
        if (colStart <= colEnd) {
            for (int i = 0; i < length; ++i)
                data[i] = colStart + i;
        } else {
            --colStart;
            for (int i = 0; i < length; ++i)
                data[i] = colStart - i;
        }
    }

    // create a matrix
    int cols = indexCols->size();
    ConstantSP result = getInstance(cols);
    if (!rowLabel_.isNull())
        result->setRowLabel(rowLabel_->getValue());
    if (!colLabel_.isNull()) {
        result->setColumnLabel(colLabel_->get(indexCols));
    }
    for (int i = 0; i < cols; ++i) {
        int cur = indexCols->getInt(i);
        if (cur < 0 || cur >= cols_)
            throw OperatorRuntimeException("matrix", "The column index " + Util::convert(cur) + " is out of range.");
        result->setColumn(i, getColumn(cur));
    }
    return result;
}

bool Matrix::set(const ConstantSP index, const ConstantSP& value) {
    int cols = index->size();
    bool scalar = value->isScalar();
    if (value->size() != rows_ * cols && !scalar)
        throw OperatorRuntimeException("matrix", "matrix and assigned value are not compatible");
    if (cols == 1) {
        int cur = index->getInt(0);
        if (cur >= cols_ || cur < 0)
            throw OperatorRuntimeException("matrix", "The column index " + Util::convert(cur) + " is out of range.");
        setColumn(cur, value);
        return true;
    }

    for (int i = 0; i < cols; ++i) {
        int cur = index->getInt(i);
        if (cur >= cols_ || cur < 0)
            throw OperatorRuntimeException("matrix", "The column index " + Util::convert(cur) + " is out of range.");
        setColumn(cur, scalar ? value : ((Vector*)value.get())->getSubVector(rows_ * i, rows_));
    }
    return true;
}

}