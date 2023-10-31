/*
 * Format.h
 *
 *  Created on: Sep 21, 2018
 *      Author: dzhou
 */

#ifndef FORMAT_H_
#define FORMAT_H_

#include <string>
#include <vector>

#include "Types.h"

using namespace std;

namespace dolphindb {

class TemporalFormat {
public:
    TemporalFormat(const string& format);
    string format(long long nowtime, DATA_TYPE dtype) const;
    static vector<pair<int, int> > initFormatMap();

private:
    void initialize(const string& format);

private:
    struct FormatSegment {
        int timeUnitIndex_;
        int maxLength_;
        int startPos_;
        int endPos_;

        FormatSegment() : timeUnitIndex_(0), maxLength_(0), startPos_(0), endPos_(0) {}
        FormatSegment(int timeUnitIndex, int maxLength, int startPos, int endPos)
            : timeUnitIndex_(timeUnitIndex), maxLength_(maxLength), startPos_(startPos), endPos_(endPos) {}
    };

    string format_;
    bool quickFormat_;
    int segmentCount_;
    FormatSegment segments_[12];

    static vector<pair<int, int> > formatMap;  // first:timeUnitIndex_ second:timeUnit max length
    static const string pmString;
    static const string amString;
    static const char* monthName[12];
};

class NumberFormat {
public:
    NumberFormat(const string& format);
    string format(double x) const;
    static string toString(long long x);

private:
	 void initialize(const string& format);
	 static int printFraction(char* buf, int digitCount, bool optional, double& fraction);

private:
    bool percent_;
    bool point_;
    int science_;
    int segmentLength_;
    int integerMinDigits_;
    int fractionMinDigits_;
    int fractionOptionalDigits_;
    int headSize_;
    int tailSize_;
    string head_;
	string tail_;
	double rounding_;

	static const long long power10_[10];
	static const double enableScientificNotationBeyond_;
	static const double epsilon_;
};

class DecimalFormat {
public:
	DecimalFormat(const string& format);
	~DecimalFormat();
	string format(double x) const;

private:
	NumberFormat* format_;
	NumberFormat* negFormat_;
};

};

#endif /* FORMAT_H_ */
