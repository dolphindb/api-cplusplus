// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include "../Exceptions.h"
#include "../FixSTL.h"
#include "../Types.h"
#include "../Util.h"
#include "WideInteger.h"

#include <cstdint>
#include <sstream>

namespace dolphindb {

namespace decimal_util
{
template <typename T> struct MinPrecision {
    static constexpr int value = 1;
};

template <typename T> struct MaxPrecision;
template <> struct MaxPrecision<int32_t> {
    static constexpr int value = 9;
};
template <> struct MaxPrecision<int64_t> {
    static constexpr int value = 18;
};
template <> struct MaxPrecision<wide_integer::int128> {
    static constexpr int value = 38;
};

template <typename T> struct DecimalType;
template <> struct DecimalType<int32_t> {
    static constexpr DATA_TYPE value = DT_DECIMAL32;
};
template <> struct DecimalType<int64_t> {
    static constexpr DATA_TYPE value = DT_DECIMAL64;
};
template <> struct DecimalType<wide_integer::int128> {
    static constexpr DATA_TYPE value = DT_DECIMAL128;
};

#define ENABLE_FOR_DECIMAL(rawType, T, return_type_t)                                                                  \
    template <typename U = T> typename std::enable_if<std::is_same<U, rawType>::value == true, return_type_t>::type

#define ENABLE_FOR_DECIMAL32(T, return_type_t) ENABLE_FOR_DECIMAL(int32_t, T, return_type_t)
#define ENABLE_FOR_DECIMAL64(T, return_type_t) ENABLE_FOR_DECIMAL(int64_t, T, return_type_t)
#define ENABLE_FOR_DECIMAL128(T, return_type_t) ENABLE_FOR_DECIMAL(wide_integer::int128, T, return_type_t)

template <typename T> struct wrapper {
    static T getDecimal(const ConstantSP &value, INDEX index, int scale)
    {
        return getDecimal(value.get(), index, scale);
    }

    ENABLE_FOR_DECIMAL32(T, T)
    static getDecimal(const Constant *value, INDEX index, int scale) { return value->getDecimal32(index, scale); }
    ENABLE_FOR_DECIMAL64(T, T)
    static getDecimal(const Constant *value, INDEX index, int scale) { return value->getDecimal64(index, scale); }

    ENABLE_FOR_DECIMAL128(T, T)
    static getDecimal(const Constant *value, INDEX index, int scale) { return value->getDecimal128(index, scale); }
};

template <typename T, typename U, typename R = typename std::conditional<sizeof(T) >= sizeof(U), T, U>::type>
inline bool mulOverflow(T a, U b, R &result)
{
    result = a * b;
    if (a == 0 || b == 0) {
        return false;
    }

    if ((a < 0) != (b < 0)) { // different sign
        if (a == std::numeric_limits<R>::min()) {
            return b > 1;
        }
        if (b == std::numeric_limits<R>::min()) {
            return a > 1;
        }
        if (a < 0) {
            return (-a) > std::numeric_limits<R>::max() / b;
        }
        if (b < 0) {
            return a > std::numeric_limits<R>::max() / (-b);
        }
    } else if (a < 0 && b < 0) {
        if (a == std::numeric_limits<R>::min()) {
            return b <= -1;
        }
        if (b == std::numeric_limits<R>::min()) {
            return a <= -1;
        }
        return (-a) > std::numeric_limits<R>::max() / (-b);
    }

    return a > std::numeric_limits<R>::max() / b;
}

inline int exp10_i32(int x)
{
    constexpr int values[] = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};
    return values[x];
}

inline int64_t exp10_i64(int x)
{
    constexpr int64_t values[] = {1LL,
                                  10LL,
                                  100LL,
                                  1000LL,
                                  10000LL,
                                  100000LL,
                                  1000000LL,
                                  10000000LL,
                                  100000000LL,
                                  1000000000LL,
                                  10000000000LL,
                                  100000000000LL,
                                  1000000000000LL,
                                  10000000000000LL,
                                  100000000000000LL,
                                  1000000000000000LL,
                                  10000000000000000LL,
                                  100000000000000000LL,
                                  1000000000000000000LL};
    return values[x];
}

inline wide_integer::int128 exp10_i128(int x)
{
    using int128 = wide_integer::int128;
    int128 values[] = {static_cast<int128>(1LL),
                       static_cast<int128>(10LL),
                       static_cast<int128>(100LL),
                       static_cast<int128>(1000LL),
                       static_cast<int128>(10000LL),
                       static_cast<int128>(100000LL),
                       static_cast<int128>(1000000LL),
                       static_cast<int128>(10000000LL),
                       static_cast<int128>(100000000LL),
                       static_cast<int128>(1000000000LL),
                       static_cast<int128>(10000000000LL),
                       static_cast<int128>(100000000000LL),
                       static_cast<int128>(1000000000000LL),
                       static_cast<int128>(10000000000000LL),
                       static_cast<int128>(100000000000000LL),
                       static_cast<int128>(1000000000000000LL),
                       static_cast<int128>(10000000000000000LL),
                       static_cast<int128>(100000000000000000LL),
                       static_cast<int128>(1000000000000000000LL),
                       static_cast<int128>(1000000000000000000LL) * 10LL,
                       static_cast<int128>(1000000000000000000LL) * 100LL,
                       static_cast<int128>(1000000000000000000LL) * 1000LL,
                       static_cast<int128>(1000000000000000000LL) * 10000LL,
                       static_cast<int128>(1000000000000000000LL) * 100000LL,
                       static_cast<int128>(1000000000000000000LL) * 1000000LL,
                       static_cast<int128>(1000000000000000000LL) * 10000000LL,
                       static_cast<int128>(1000000000000000000LL) * 100000000LL,
                       static_cast<int128>(1000000000000000000LL) * 1000000000LL,
                       static_cast<int128>(1000000000000000000LL) * 10000000000LL,
                       static_cast<int128>(1000000000000000000LL) * 100000000000LL,
                       static_cast<int128>(1000000000000000000LL) * 1000000000000LL,
                       static_cast<int128>(1000000000000000000LL) * 10000000000000LL,
                       static_cast<int128>(1000000000000000000LL) * 100000000000000LL,
                       static_cast<int128>(1000000000000000000LL) * 1000000000000000LL,
                       static_cast<int128>(1000000000000000000LL) * 10000000000000000LL,
                       static_cast<int128>(1000000000000000000LL) * 100000000000000000LL,
                       static_cast<int128>(1000000000000000000LL) * 100000000000000000LL * 10LL,
                       static_cast<int128>(1000000000000000000LL) * 100000000000000000LL * 100LL,
                       static_cast<int128>(1000000000000000000LL) * 100000000000000000LL * 1000LL};
    assert(x >= 0 && static_cast<size_t>(x) < sizeof(values) / sizeof(values[0]));
    return values[x];
}

template <typename T> inline T scaleMultiplier(int scale);

template <> inline int32_t scaleMultiplier<int32_t>(int scale) { return exp10_i32(scale); }

template <> inline int64_t scaleMultiplier<int64_t>(int scale) { return exp10_i64(scale); }

template <> inline wide_integer::int128 scaleMultiplier<wide_integer::int128>(int scale) { return exp10_i128(scale); }

template <typename T> inline std::string toString(int scale, T rawData)
{
    std::stringstream ss;

    if (scale == 0) {
        ss << rawData;
    } else {
        auto multiplier = scaleMultiplier<T>(scale);

        T integer = rawData / multiplier;
        if (rawData < 0 && integer == 0) {
            ss << '-';
        }
        ss << integer;

        int sign = rawData < 0 ? -1 : 1;
        auto frac = rawData % multiplier * sign;
        ss << "." << std::setw(scale) << std::setfill('0') << std::right << frac;
    }

    return ss.str();
}

/**
 * @brief Parse string to decimal.
 *
 * @param[out] rawData The Integer representation of decimal.
 * @param[in,out] scale The scale of decimal. If less then 0 (e.g. -1), will automatically identify scale.
 * @param[out] errMsg The reason if parse failed.
 * @param strict If true, parse invalid decimal string (e.g., "2013.06.13") will fail.
 * @return Whether the parsing is successful.
 *
 * @note Only support string like "0.0000000123", not support "15e16"|"-0x1afp-2"|"inF"|"Nan"|"invalid".
 */
template <typename T>
inline bool parseString(const char *str, size_t str_len, T &rawData, int &scale, std::string &errMsg,
                        bool strict = false)
{
    const char dec_point = '.';

    enum StateEnum { IN_SIGN, IN_BEFORE_FIRST_DIG, IN_BEFORE_DEC, IN_AFTER_DEC, IN_END } state = IN_SIGN;
    enum ErrorCodes {
        NO_ERR = 0,
        ERR_WRONG_CHAR = 1,
        ERR_NO_DIGITS = 2,
        ERR_WRONG_STATE = 3,
        ERR_SCALE_ERROR = 4,
        ERR_OVERFLOW = 5,
        ERROR_CODE_COUNT = 6,
    };
    const char *const msg[] = {
        "", "illegal string", "no digits", "illegal string", "the number of digits exceed scale", "decimal overflow"};

    StateEnum prevState = IN_SIGN;

    rawData = 0;

    bool determine_scale = false;
    if (scale < 0) {
        determine_scale = true;
        scale = decimal_util::MaxPrecision<T>::value;
    }

    int sign = 1;
    ErrorCodes error = NO_ERR;
    int digitsCount = 0;         // including '+' '-'
    int noneZeroDigitsCount = 0; // ignore zero before numbers ( which in left of '.' or '1-9')
    int afterDigitCount = 0;

    bool rounding = false;

    char c;
    size_t i = 0;
    while ((i < str_len) && (state != IN_END)) // loop while extraction from file is possible
    {
        c = str[i++];

        switch (state) {
        case IN_SIGN:
            if (c == '-') {
                sign = -1;
                state = IN_BEFORE_FIRST_DIG;
                digitsCount++;
            } else if (c == '+') {
                state = IN_BEFORE_FIRST_DIG;
                digitsCount++;
            } else if ((c >= '0') && (c <= '9')) {
                state = IN_BEFORE_DEC;
                rawData = static_cast<int>(c - '0');
                digitsCount++;
                if (c != '0') {
                    noneZeroDigitsCount++;
                }
            } else if (c == dec_point) {
                state = IN_AFTER_DEC;
            } else if ((c != ' ') && (c != '\t')) {
                error = ERR_WRONG_CHAR;
                state = IN_END;
                prevState = IN_SIGN;
            }
            // else ignore char
            break;
        case IN_BEFORE_FIRST_DIG:
            if ((c >= '0') && (c <= '9')) {
                if (noneZeroDigitsCount + 1 > decimal_util::MaxPrecision<T>::value) {
                    error = ERR_OVERFLOW;
                    state = IN_END;
                    break;
                }
                digitsCount++;
                if (c != '0') {
                    noneZeroDigitsCount++;
                }
                rawData = 10 * rawData + static_cast<int>(c - '0');
                state = IN_BEFORE_DEC;
            } else if (c == dec_point) {
                state = IN_AFTER_DEC;
            } else if ((c != ' ') && (c != '\t')) {
                error = ERR_WRONG_CHAR;
                state = IN_END;
                prevState = IN_BEFORE_FIRST_DIG;
            }
            break;
        case IN_BEFORE_DEC:
            if ((c >= '0') && (c <= '9')) {
                if (noneZeroDigitsCount + 1 > decimal_util::MaxPrecision<T>::value) {
                    error = ERR_OVERFLOW;
                    state = IN_END;
                    break;
                }
                digitsCount++;
                if (noneZeroDigitsCount != 0 || c != '0') {
                    noneZeroDigitsCount++;
                }
                rawData = 10 * rawData + static_cast<int>(c - '0');
            } else if (c == dec_point) {
                state = IN_AFTER_DEC;
            } else if ((c != ' ') && (c != '\t')) {
                error = ERR_WRONG_CHAR;
                state = IN_END;
                prevState = IN_BEFORE_DEC;
            }
            break;
        case IN_AFTER_DEC:
            if ((c >= '0') && (c <= '9')) {
                if (afterDigitCount + 1 > scale) {
                    // error = ERR_SCALE_ERROR;
                    rounding = (static_cast<int>(c - '0') >= 5);
                    state = IN_END;
                    break;
                }
                if (noneZeroDigitsCount + 1 > decimal_util::MaxPrecision<T>::value) {
                    error = ERR_OVERFLOW;
                    state = IN_END;
                    break;
                }
                digitsCount++;
                noneZeroDigitsCount++;
                afterDigitCount++;
                rawData = 10 * rawData + static_cast<int>(c - '0');
            } else if ((c != ' ') && (c != '\t')) {
                error = ERR_WRONG_CHAR;
                state = IN_END;
                prevState = IN_AFTER_DEC;
            }
            break;
        default:
            error = ERR_WRONG_STATE;
            state = IN_END;
            break;
        } // switch state
    }

    if (rounding) {
        rawData += 1;
    }

    if (determine_scale) {
        scale = afterDigitCount;
    }

    auto buildErrorMsg = [&](ErrorCodes err) {
        assert(err < ERROR_CODE_COUNT);
        return "parse `" + std::string(str, str_len) + "` to " + Util::getDataTypeString(DecimalType<T>::value) + "(" +
               std::to_string(scale) + ") failed: " + msg[err];
    };

    if (error == NO_ERR || (strict == false && error == ERR_WRONG_CHAR && prevState != IN_SIGN)) {
        if (digitsCount == 0) {
            rawData = std::numeric_limits<T>::min();
            return true;
        }
        if (determine_scale || scale > afterDigitCount) {
            if (noneZeroDigitsCount + scale - afterDigitCount > decimal_util::MaxPrecision<T>::value) {
                errMsg = buildErrorMsg(ERR_OVERFLOW);
                return false;
            }
            rawData = rawData * decimal_util::scaleMultiplier<T>(scale - afterDigitCount);
        }
        if (sign < 0) {
            rawData = -rawData;
        }
        error = NO_ERR;
    } else {
        if (error == ERR_WRONG_CHAR && prevState == IN_SIGN) {
            rawData = std::numeric_limits<T>::min();
            return true;
        }
        rawData = 0;
        errMsg = buildErrorMsg(error);
    }

    return (error == NO_ERR);
}

// For example, Decimal32(4) can contain numbers from -99999.9999 to 99999.9999 with 0.0001 step.
template <typename T> inline void toDecimal(const std::string &str, int scale, T &rawData)
{
    if (scale < 0 || scale > decimal_util::MaxPrecision<T>::value) {
        throw RuntimeException("Scale out of bound (valid range: [0, " +
                               std::to_string(decimal_util::MaxPrecision<T>::value) +
                               "], but get: " + std::to_string(scale) + ")");
    }
    T tempRawData = 0;
    std::string errMsg;
    if (!parseString(str.c_str(), str.length(), tempRawData, scale, errMsg)) {
        throw RuntimeException("ToDecimal illegal: " + errMsg);
    }
    rawData = tempRawData;
}

template <typename T, typename R, std::enable_if_t<std::is_floating_point<T>::value, bool> = true>
inline void valueToDecimalraw(T value, int scale, R *result)
{
    if (scale < 0 || scale > decimal_util::MaxPrecision<R>::value) {
        throw RuntimeException("Scale out of bound (valid range: [0, " +
                               std::to_string(decimal_util::MaxPrecision<R>::value) +
                               "], but get: " + std::to_string(scale) + ")");
    }
    if (getNullValue<T>() == value) {
        *result = std::numeric_limits<R>::min();
        return;
    }

    const auto factor = decimal_util::scaleMultiplier<R>(scale);
    if (std::trunc(value) != value) {
        using UU = typename std::conditional<std::is_same<R, wide_integer::int128>::value, long double, double>::type;
        const UU tmp = static_cast<UU>(value) * static_cast<UU>(factor);
        if (tmp > static_cast<UU>(std::numeric_limits<R>::max()) ||
            tmp <= static_cast<UU>(std::numeric_limits<R>::min())) {
            throw MathException("Decimal math overflow");
        }
        *result = static_cast<R>(tmp);
    } else {
        if (static_cast<wide_integer::int128>(value) >
                static_cast<wide_integer::int128>(std::numeric_limits<R>::max()) ||
            static_cast<wide_integer::int128>(value) <=
                static_cast<wide_integer::int128>(std::numeric_limits<R>::min())) {
            throw MathException("Decimal math overflow");
        }
        bool overflow = decimal_util::mulOverflow(static_cast<R>(value), factor, *result);
        if (overflow || static_cast<wide_integer::int128>(value) ==
                            static_cast<wide_integer::int128>(std::numeric_limits<R>::min())) {
            throw MathException("Decimal math overflow");
        }
    }
}

template <typename T, typename R, std::enable_if_t<std::is_integral<T>::value, bool> = true>
inline void valueToDecimalraw(T value, int scale, R *result)
{
    if (scale < 0 || scale > decimal_util::MaxPrecision<R>::value) {
        throw RuntimeException("Scale out of bound (valid range: [0, " +
                               std::to_string(decimal_util::MaxPrecision<R>::value) +
                               "], but get: " + std::to_string(scale) + ")");
    }
    if (getNullValue<T>() == value) {
        *result = std::numeric_limits<R>::min();
        return;
    }

    const auto factor = decimal_util::scaleMultiplier<R>(scale);
    if (static_cast<wide_integer::int128>(value) > static_cast<wide_integer::int128>(std::numeric_limits<R>::max()) ||
        static_cast<wide_integer::int128>(value) <= static_cast<wide_integer::int128>(std::numeric_limits<R>::min())) {
        throw MathException("Decimal math overflow");
    }
    bool overflow = decimal_util::mulOverflow(static_cast<R>(value), factor, *result);
    if (overflow ||
        static_cast<wide_integer::int128>(value) == static_cast<wide_integer::int128>(std::numeric_limits<R>::min())) {
        throw MathException("Decimal math overflow");
    }
}

template <typename T, typename R> void valueToDecimalraw(const T *data, int scale, INDEX start, int len, R *buf)
{
    for (auto i = 0, index = start; i < len; i++, index++) {
        decimal_util::valueToDecimalraw(data[i], scale, buf + index);
    }
}

} // namespace decimal_util

} // namespace dolphindb
