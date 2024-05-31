#include "Vector.h"
#include "ScalarImp.h"
#include "Util.h"
namespace dolphindb {

ConstantSP Vector::getColumnLabel() const {
    return new String(name_);
}

std::string Vector::getString() const {
    if (getForm() == DF_PAIR) {
        return getScript();
    } else {
        int len = (std::min)(Util::DISPLAY_ROWS, size());
        bool notTuple = getType() != DT_ANY;
        std::string str(notTuple ? "[" : "(");

        if (len > 0) {
            if (len == 1 && isNull(0))
                str.append(get(0)->getScript());
            else {
                if (isNull(0)) {
                    // do nothing
                } else if (notTuple || get(0)->isScalar())
                    str.append(get(0)->getScript());
                else
                    str.append(getString(0));
            }
        }
        for (int i = 1; i < len; ++i) {
            str.append(",");
            if (isNull(i)) {
                // do nothing
            } else if (notTuple || get(i)->isScalar())
                str.append(get(i)->getScript());
            else
                str.append(getString(i));
        }
        if (size() > len)
            str.append("...");
        str.append(notTuple ? "]" : ")");
        return str;
    }
}

std::string Vector::getScript() const {
    if (getForm() == DF_PAIR) {
        std::string str = get(0)->getScript();
        str.append(" : ");
        str.append(get(1)->getScript());
        return str;
    } else if (getForm() == DF_MATRIX) {
        return name_.empty() ? "matrix()" : name_;
    } else {
        int len = size();
        if (len > Util::CONST_VECTOR_MAX_SIZE)
            return name_.empty() ? "array()" : name_;

        std::string str("[");
        if (len > 0)
            str.append(get(0)->getScript());
        for (int i = 1; i < len; ++i) {
            str.append(",");
            str.append(get(i)->getScript());
        }
        str.append("]");
        return str;
    }
}

}