// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include <string>
#include <vector>

#include "Exceptions.h"
#include "Types.h"
#include "Util.h"

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4251 )
#endif

namespace dolphindb {

class EXPORT_DECL TemporalFormat
{
  public:
    explicit TemporalFormat(const std::string &format) { initialize(format); }
    std::string format(long long nowtime, DATA_TYPE dtype) const
    {
        int timeNumber[10];
        memset(timeNumber, 0, sizeof(timeNumber));
        timeNumber[0] = 1970;
        timeNumber[1] = 1;
        timeNumber[2] = 1;
        switch (dtype) {
        case DT_MINUTE: {
            timeNumber[4] = static_cast<int>(nowtime / 60);
            timeNumber[6] = nowtime % 60;
            timeNumber[5] = timeNumber[4] / 12;
            timeNumber[3] = timeNumber[4] % 12;
            break;
        }
        case DT_SECOND: {
            timeNumber[7] = nowtime % 60;
            nowtime = nowtime / 60;
            timeNumber[6] = nowtime % 60;
            timeNumber[4] = static_cast<int>(nowtime / 60);
            timeNumber[5] = timeNumber[4] / 12;
            timeNumber[3] = timeNumber[4] % 12;
            break;
        }
        case DT_TIME: {
            timeNumber[8] = nowtime % 1000;
            nowtime = nowtime / 1000;
            timeNumber[7] = nowtime % 60;
            nowtime = nowtime / 60;
            timeNumber[6] = nowtime % 60;
            timeNumber[4] = static_cast<int>(nowtime / 60);
            timeNumber[5] = timeNumber[4] / 12;
            timeNumber[3] = timeNumber[4] % 12;
            break;
        }
        case DT_NANOTIME: {
            timeNumber[9] = nowtime % 1000000000;
            nowtime = nowtime / 1000000000;
            timeNumber[7] = nowtime % 60;
            nowtime = nowtime / 60;
            timeNumber[6] = nowtime % 60;
            timeNumber[4] = static_cast<int>(nowtime / 60);
            timeNumber[5] = timeNumber[4] / 12;
            timeNumber[3] = timeNumber[4] % 12;
            break;
        }
        case DT_MONTH: {
            timeNumber[0] = static_cast<int>(nowtime / 12);
            timeNumber[1] = nowtime % 12 + 1;
            break;
        }
        case DT_DATE: {
            Util::parseDate((int)nowtime, timeNumber[0], timeNumber[1], timeNumber[2]);
            break;
        }
        case DT_DATETIME: {
            int tmp = static_cast<int>(nowtime / 86400LL);
            nowtime = nowtime % 86400LL;
            Util::parseDate(tmp, timeNumber[0], timeNumber[1], timeNumber[2]);
            timeNumber[7] = nowtime % 60;
            nowtime = nowtime / 60;
            timeNumber[6] = nowtime % 60;
            timeNumber[4] = static_cast<int>(nowtime / 60);
            timeNumber[5] = timeNumber[4] / 12;
            timeNumber[3] = timeNumber[4] % 12;
            break;
        }
        case DT_DATEHOUR: {
            int tmp = static_cast<int>(nowtime / 24);
            nowtime = nowtime % 24;
            if (nowtime < 0) {
                --tmp;
                nowtime += 24;
            }
            Util::parseDate(tmp, timeNumber[0], timeNumber[1], timeNumber[2]);
            timeNumber[4] = static_cast<int>(nowtime);
            timeNumber[5] = timeNumber[4] / 12;
            timeNumber[3] = timeNumber[4] % 12;
            break;
        }
        case DT_TIMESTAMP: {
            int tmp = static_cast<int>(nowtime / 86400000LL);
            nowtime = nowtime % 86400000LL;
            if (nowtime < 0) {
                --tmp;
                nowtime += 86400000LL;
            }
            Util::parseDate(tmp, timeNumber[0], timeNumber[1], timeNumber[2]);
            timeNumber[8] = nowtime % 1000;
            nowtime = nowtime / 1000;
            timeNumber[7] = nowtime % 60;
            nowtime = nowtime / 60;
            timeNumber[6] = nowtime % 60;
            timeNumber[4] = static_cast<int>(nowtime / 60);
            timeNumber[5] = timeNumber[4] / 12;
            timeNumber[3] = timeNumber[4] % 12;
            break;
        }
        default: {
            int tmp = static_cast<int>(nowtime / 86400000000000LL);
            nowtime = nowtime % 86400000000000LL;
            Util::parseDate(tmp, timeNumber[0], timeNumber[1], timeNumber[2]);
            timeNumber[9] = nowtime % 1000000000;
            nowtime = nowtime / 1000000000;
            timeNumber[7] = nowtime % 60;
            nowtime = nowtime / 60;
            timeNumber[6] = nowtime % 60;
            timeNumber[4] = static_cast<int>(nowtime / 60);
            timeNumber[5] = timeNumber[4] / 12;
            timeNumber[3] = timeNumber[4] % 12;
            break;
        }
        }

        if (quickFormat_) {
            std::string templateString(format_);
            for (int i = 0; i < segmentCount_; ++i) {
                int leftPos = segments_[i].startPos_;
                int rightPos = segments_[i].endPos_;
                int id = segments_[i].timeUnitIndex_;
                if (id == 5) {
                    for (int j = leftPos, cnt = 0; j <= rightPos; ++j, ++cnt) {
                        templateString[j] = cnt < 2 ? ((timeNumber[5] != 0) ? pmString[cnt] : amString[cnt]) : ' ';
                    }
                } else if (id == 1 && rightPos - leftPos + 1 == 3) {
                    const char *month = monthName[timeNumber[1] - 1];
                    for (int j = leftPos; j <= rightPos; ++j) {
                        templateString[j] = month[j - leftPos];
                    }
                } else {
                    int tmp = timeNumber[id];
                    for (int j = rightPos; j >= leftPos; --j) {
                        templateString[j] = tmp % 10 + '0';
                        tmp /= 10;
                    }
                }
            }
            return templateString;
        }
        std::string resultString;
        int prePos = 0;
        for (int i = 0; i < segmentCount_; ++i) {
            std::string tmpString;
            int leftPos = segments_[i].startPos_;
            int rightPos = segments_[i].endPos_;
            int index = segments_[i].timeUnitIndex_;
            int unitDemandlength = segments_[i].maxLength_;
            int formatDemandLength = rightPos - leftPos + 1;

            resultString += format_.substr(prePos, leftPos - prePos);
            if (index == 5) {
                for (int j = 0; j < formatDemandLength; ++j) {
                    tmpString += (j < 2) ? ((timeNumber[5] != 0) ? pmString[j] : amString[j]) : ' ';
                }
            } else if (index == 1 && formatDemandLength == 3) {
                const char *month = monthName[timeNumber[1] - 1];
                for (int j = 0; j < 3; ++j) {
                    tmpString += month[j];
                }
            } else if (index == 0 && formatDemandLength == 2) {
                resultString += ((timeNumber[0] / 10) % 10) + '0';
                resultString += (timeNumber[0] % 10) + '0';
            } else {
                int tmpNumber = timeNumber[index];
                int cntDigits = 0;

                for (int j = 0; j < unitDemandlength; ++j) {
                    cntDigits++;
                    tmpString += (tmpNumber % 10) + '0';
                    tmpNumber /= 10;
                    if (cntDigits >= formatDemandLength && tmpNumber == 0)
                        break;
                }
                for (int j = 0; j < formatDemandLength - cntDigits; ++j)
                    tmpString += '0';
                std::reverse(tmpString.begin(), tmpString.end());
            }
            resultString += tmpString;
            prePos = rightPos + 1;
        }
        resultString += format_.substr(prePos, format_.length() - prePos);
        return resultString;
    }
    static std::vector<std::pair<int, int>> initFormatMap()
    {
        std::vector<std::pair<int, int>> initvector(128, std::make_pair(-1, -1));
        initvector['y'] = std::make_pair(0, 4);
        initvector['M'] = std::make_pair(1, 2);
        initvector['d'] = std::make_pair(2, 2);
        initvector['h'] = std::make_pair(3, 2);
        initvector['H'] = std::make_pair(4, 2);
        initvector['a'] = std::make_pair(5, 2);
        initvector['m'] = std::make_pair(6, 2);
        initvector['s'] = std::make_pair(7, 2);
        initvector['S'] = std::make_pair(8, 3);
        initvector['n'] = std::make_pair(9, 9);
        return initvector;
    }

  private:
    void initialize(const std::string &format)
    {
        if (formatMap.empty())
            formatMap = TemporalFormat::initFormatMap();
        int len = static_cast<int>(format.length());
        if (len == 0)
            throw RuntimeException("The format string can't be empty.");
        if (len > 128)
            throw RuntimeException("The format string is too big.");
        format_.reserve(len);

        // process escape first
        std::vector<bool> escape(len);
        int cursor = 0;
        int i = 0;
        while (i < len) {
            char ch = format[i];
            if (ch == '\\') {
                if (i == len - 1)
                    throw RuntimeException("Invalid escape (\\)in the end of the format string.");
                format_.append(1, format[i + 1]);
                escape[cursor++] = true;
                i += 2;
            } else {
                format_.append(1, ch);
                escape[cursor++] = false;
                ++i;
            }
        }
        int cnt = 0;
        quickFormat_ = true;
        len = cursor;

        for (i = 0; i <= len; ++i) {
            if (i == len || (i != 0 && (format_[i] != format_[i - 1] || escape[i] != escape[i - 1]))) {
                char ch = format_[i - 1];
                if (ch >= 0 && !escape[i - 1] && formatMap[ch].first != -1) {
                    segments_[segmentCount_++] =
                        FormatSegment(formatMap[ch].first, formatMap[ch].second, i - cnt, i - 1);
                    if (cnt < segments_[segmentCount_ - 1].maxLength_) {
                        quickFormat_ = false;
                    }

                    if (segmentCount_ == 12) {
                        throw RuntimeException("The format contains too many superfluous symbols.");
                    }
                }
                cnt = 0;
            }
            cnt++;
        }
    }

    struct FormatSegment {
        int timeUnitIndex_;
        int maxLength_;
        int startPos_;
        int endPos_;

        FormatSegment() : timeUnitIndex_(0), maxLength_(0), startPos_(0), endPos_(0) {}
        FormatSegment(int timeUnitIndex, int maxLength, int startPos, int endPos)
            : timeUnitIndex_(timeUnitIndex), maxLength_(maxLength), startPos_(startPos), endPos_(endPos)
        {
        }
    };

    std::string format_;
    bool quickFormat_;
    int segmentCount_{0};
    FormatSegment segments_[12];

    static std::vector<std::pair<int, int>> formatMap; // first:timeUnitIndex_ second:timeUnit max length
    static const std::string pmString;
    static const std::string amString;
    static const char *monthName[12];
};

class EXPORT_DECL NumberFormat {
public:
    explicit NumberFormat(const std::string &format)
        : percent_(false), science_(0), segmentLength_(INT_MAX), integerMinDigits_(0), fractionMinDigits_(0),
            fractionOptionalDigits_(0), headSize_(0), tailSize_(0), rounding_(0)
    {
        initialize(format);
    }

    std::string format(double x) const;
    static std::string toString(long long x);

private:
	 void initialize(const std::string& format);
	 static int printFraction(char* buf, int digitCount, bool optional, double& fraction);


    bool percent_;
    bool point_;
    int science_;
    int segmentLength_;
    int integerMinDigits_;
    int fractionMinDigits_;
    int fractionOptionalDigits_;
    int headSize_;
    int tailSize_;
    std::string head_;
	std::string tail_;
	double rounding_;

	static const long long power10_[10];
	static const double enableScientificNotationBeyond_;
	static const double epsilon_;
};

class DecimalFormat {
public:
	explicit DecimalFormat(const std::string& format);
	~DecimalFormat();
	std::string format(double x) const;

private:
	NumberFormat* format_;
	NumberFormat* negFormat_;
};

} // namespace dolphindb

#ifdef _MSC_VER
#pragma warning( pop )
#endif
