/*
 * Format.cpp
 *
 *  Created on: Sep 21, 2018
 *      Author: dzhou
 */

#include "Format.h"
#include "Exceptions.h"
#include "Types.h"
#include "Util.h"

#include <algorithm>
#include <climits>
#include <cstddef>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

namespace dolphindb {
const std::string TemporalFormat::pmString = "PM";
const std::string TemporalFormat::amString = "AM";
const char* TemporalFormat::monthName[12] = {"JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"};
std::vector<std::pair<int, int> > TemporalFormat::formatMap;
const long long NumberFormat::power10_[10] = {10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000LL, 10000000000LL};
const double NumberFormat::enableScientificNotationBeyond_ = 1.0e+15;
const double NumberFormat::epsilon_ = 1.0e-12;


int NumberFormat::printFraction(char* buf, int digitCount, bool optional, double& fraction)
{
	int cursor = 0;
	while(digitCount != 0){
		int count = (std::min)(digitCount, 10);
		digitCount -= count;
		double val = fraction * power10_[count - 1];
		auto x = (long long)val;
		fraction = val - x;
		int start = cursor;
		bool notZero = x != 0;
		while(x != 0){
			buf[cursor++] = x % 10 + '0';
			x = x / 10;
		}
		if(cursor - start < count && (!optional || (digitCount != 0) || notZero)){
			for(int i = count - cursor + start; i > 0; --i)
				buf[cursor++] = '0';
		}
		int intLen = (cursor - start)/2;
		for(int i=0; i<intLen; ++i){
			char tmp = buf[start + i];
			buf[start + i] = buf[cursor - 1 - i];
			buf[cursor - 1 - i] = tmp;
		}
	}
	if(optional){
		while((cursor != 0) && buf[cursor -1] == '0') --cursor;
	}
	return cursor;
}

void NumberFormat::initialize(const std::string & form)
{
	if(form.empty())
		throw RuntimeException("The format string can't be empty.");
	int firstSymPos = -1;
	int lastSymPos = 0;
	int len = static_cast<int>(form.size());

	int scienceCount = 0, dotCount = 0, commaCount = 0, percentCount = 0, digitCount = 0;
	int sciencePos = -1, pointPos = -1, commaPos = -1;
	for (int i = 0; i < len; ++i) {
		char ch = form[i];
		if (ch == '#' || ch == '.' || ch == '0' || ch == ',' ||	ch == 'E' || ch == '%') {
			if(firstSymPos < 0)
				firstSymPos = i;
			else if (lastSymPos + 1 != i)
				throw RuntimeException("Characters other than 0/#/./,/E/% can't appear in the middle of a number format.");
			lastSymPos = i;
			if (ch == 'E'){
				sciencePos = i;
				scienceCount++;
			}
			else if (ch == '.'){
				dotCount++;
				pointPos = i;
				point_ = true;
			}
			else if (ch == ','){
				commaCount++;
				commaPos = i;
			}
			else if (form[i] == '%')
				percentCount++;
			else
				digitCount++;
		}
	}
	if (scienceCount > 1 || dotCount > 1 || commaCount > 1 || percentCount > 1)
		throw RuntimeException("Symbol (./,/E/%) can't occur more than once in number format.");
	if(digitCount == 0)
		throw RuntimeException("The number format doesn't contain '0' or '#'.");
	if (percentCount > 0){
		percent_ = true;
		if(form[lastSymPos] != '%')
			throw RuntimeException("The percent sign(%) must be the last symbol of a number format.");
	}
	if(sciencePos >= 0){
		if(sciencePos == firstSymPos || sciencePos == lastSymPos)
			throw RuntimeException("The scientific notation(E) can't be the first symbol or last symbol of a number format.");
		for (int i = sciencePos + 1; i <= lastSymPos; ++i) {
			if(form[i] != '0')
				throw RuntimeException("The format symbol after scientific notation (E) must be 0");
		}
		science_ = lastSymPos - sciencePos;
	}

	int lastIntegerPos = lastSymPos;
	if(percent_)
		--lastIntegerPos;
	if(commaPos >= 0){
		if(commaPos == firstSymPos || commaPos == lastSymPos)
			throw RuntimeException("The decimal separator(,) can't be the first symbol or last symbol of a number format.");
		if(pointPos >= 0){
			if(pointPos < commaPos)
				throw RuntimeException("The decimal separator(,) must appear before decimal point(.) in a number format.");
			lastIntegerPos = pointPos -1;
		}
		segmentLength_ = lastIntegerPos - commaPos;
	}

	if(pointPos >= 0 && pointPos <= lastIntegerPos)
		lastIntegerPos = pointPos -1;
	integerMinDigits_ = 0;
	for (int i = lastIntegerPos; i >= firstSymPos; --i) {
		if (form[i] == '0')
			integerMinDigits_ ++;
		else if(form[i] == ',')
			continue;
		else
			break;
	}
	if(science_ > 0 && integerMinDigits_ == 0)
		integerMinDigits_ = 1;

	fractionMinDigits_ = 0;
	fractionOptionalDigits_ = 0;
	if(pointPos >= 0){
		int lastFractionPos = lastSymPos;
		if(percent_)
			--lastFractionPos;
		if(sciencePos >= 0)
			lastFractionPos = sciencePos - 1;
		int i = pointPos + 1;
		while(i <= lastFractionPos && form[i] == '0') ++i;
		fractionMinDigits_ = i - pointPos -1;

		while(i <= lastFractionPos && form[i] == '#') ++i;
		fractionOptionalDigits_ = i - pointPos -1 - fractionMinDigits_;
	}

	rounding_ = 0.5;
	int digits = fractionMinDigits_ + fractionOptionalDigits_;
	while(digits != 0){
		int count = (std::min)(digits, 10);
		digits -= count;
		rounding_ /= power10_[count - 1];
	}
	if(epsilon_ < rounding_/10)
		rounding_ += epsilon_;

	headSize_ = firstSymPos;
	head_ = form.substr(0, headSize_);
	tailSize_ = len - lastSymPos - 1;
	tail_ = form.substr(lastSymPos + 1, tailSize_);
}

std::string NumberFormat::format(double x) const
{
    char buf[128];
    int cursor = 0;

    for (int i = 0; i < headSize_; ++i)
        buf[cursor++] = head_[i];

    if (x < 0) {
        buf[cursor++] = '-';
        x = -x;
    }
    if (percent_)
        x *= 100;

    bool enableSciNotation = (science_ != 0) || x >= enableScientificNotationBeyond_;
    int sciencePower = 0;
    if (enableSciNotation) {
        while (x < 1) {
            sciencePower -= 10;
            x *= power10_[9];
        }
        if (x >= 10) {
            while (x >= power10_[9]) {
                sciencePower += 10;
                x /= power10_[9];
            }
            int i = 0;
            while (i < 10 && x >= power10_[i]) {
                ++i;
            }
            if (i >= 1) {
                sciencePower += i;
                x /= power10_[i - 1];
            }
        }
        if (integerMinDigits_ > 1) {
            int count = integerMinDigits_ - 1;
            while (count != 0) {
                int tmp = (std::min)(count, 10);
                x *= power10_[tmp - 1];
                count -= tmp;
            }
            sciencePower -= integerMinDigits_ - 1;
        }
    }
    x += rounding_;
    auto integral = (long long)x;
    double fraction = x - integral;

    // output integer part
    int intStart = cursor;
    int digits = 0;
    while (integral != 0) {
        if ((digits != 0) && digits % segmentLength_ == 0)
            buf[cursor++] = ',';
        buf[cursor++] = integral % 10 + '0';
        ++digits;
        integral /= 10;
    }
    for (; digits < integerMinDigits_; ++digits)
        buf[cursor++] = '0';
    // reverse the digits of integer part
    int intLen = (cursor - intStart) / 2;
    for (int i = 0; i < intLen; ++i) {
        char tmp = buf[intStart + i];
        buf[intStart + i] = buf[cursor - 1 - i];
        buf[cursor - 1 - i] = tmp;
    }
    if (enableSciNotation && cursor - intStart == integerMinDigits_ + 1) {
        ++sciencePower;
        fraction /= 10;
        --cursor;
    }

    // output fraction part
    if (point_) {
        buf[cursor++] = '.';
        if (fractionMinDigits_ != 0) {
            cursor += printFraction(buf + cursor, fractionMinDigits_, false, fraction);
        }
        if (fractionOptionalDigits_ != 0) {
            cursor += printFraction(buf + cursor, fractionOptionalDigits_, true, fraction);
        }
        if (buf[cursor - 1] == '.')
            --cursor;
    }

    if (enableSciNotation) {
        buf[cursor++] = 'E';
        if (sciencePower < 0) {
            sciencePower = -sciencePower;
            buf[cursor++] = '-';
        }
        intStart = cursor;
        digits = 0;
        while (sciencePower != 0) {
            buf[cursor++] = sciencePower % 10 + '0';
            ++digits;
            sciencePower /= 10;
        }
        for (; digits < science_; ++digits)
            buf[cursor++] = '0';
        // reverse the digits of integer part
        intLen = (cursor - intStart) / 2;
        for (int i = 0; i < intLen; ++i) {
            char tmp = buf[intStart + i];
            buf[intStart + i] = buf[cursor - 1 - i];
            buf[cursor - 1 - i] = tmp;
        }
    }
    if (percent_)
        buf[cursor++] = '%';
    for (int i = 0; i < tailSize_; ++i)
        buf[cursor++] = tail_[i];
    buf[cursor] = 0;
    return buf;
}

DecimalFormat::DecimalFormat(const std::string & form) : format_(nullptr), negFormat_(nullptr)
{
	size_t pos = form.find(';');
	if(pos == std::string::npos || pos == 0 || pos == form.size() - 1)
		format_ = new NumberFormat(form);
	else{
		format_ = new NumberFormat(form.substr(0, pos));
		negFormat_ = new NumberFormat(form.substr(pos + 1));
	}
}

DecimalFormat::~DecimalFormat()
{
	if(format_ != nullptr)
		delete format_;
	if(negFormat_ != nullptr)
		delete negFormat_;
}

std::string DecimalFormat::format(double x) const
{
	if(negFormat_ != nullptr && x < 0)
		return negFormat_->format(-x);
	return format_->format(x);
}

} // namespace dolphindb
