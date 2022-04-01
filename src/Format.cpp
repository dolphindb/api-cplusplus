/*
 * Format.cpp
 *
 *  Created on: Sep 21, 2018
 *      Author: dzhou
 */

#include <climits>

#include "Format.h"
#include "Exceptions.h"
#include "Util.h"

namespace dolphindb {
const string TemporalFormat::pmString = "PM";
const string TemporalFormat::amString = "AM";
const char* TemporalFormat::monthName[12] = {"JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"};
const vector<pair<int, int> > TemporalFormat::formatMap = std::move(TemporalFormat::initFormatMap());
const long long NumberFormat::power10_[10] = {10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000ll, 10000000000ll};
const double NumberFormat::enableScientificNotationBeyond_ = 1.0e+15;
const double NumberFormat::epsilon_ = 1.0e-12;


int NumberFormat::printFraction(char* buf, int digitCount, bool optional, double& fraction){
	int cursor = 0;
	while(digitCount){
		int count = (std::min)(digitCount, 10);
		digitCount -= count;
		double val = fraction * power10_[count - 1];
		long long x = (long long)val;
		fraction = val - x;
		int start = cursor;
		bool notZero = x != 0;
		while(x){
			buf[cursor++] = x % 10 + '0';
			x = x / 10;
		}
		if(cursor - start < count && (!optional || digitCount || notZero)){
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
		while(cursor && buf[cursor -1] == '0') --cursor;
	}
	return cursor;
}

NumberFormat::NumberFormat(const string& format): percent_(0), science_(0), segmentLength_(INT_MAX),
		integerMinDigits_(0), fractionMinDigits_(0), fractionOptionalDigits_(0), headSize_(0), tailSize_(0), rounding_(0) {
    initialize(format);
}

void NumberFormat::initialize(const string& format) {
	if(format.empty())
		throw RuntimeException("The format string can't be empty.");
	int firstSymPos = -1;
	int lastSymPos = 0;
	int len = format.size();

	int scienceCount = 0, dotCount = 0, commaCount = 0, percentCount = 0, digitCount = 0;
	int sciencePos = -1, pointPos = -1, commaPos = -1;
	for (int i = 0; i < len; ++i) {
		char ch = format[i];
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
			else if (format[i] == '%')
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
		if(format[lastSymPos] != '%')
			throw RuntimeException("The percent sign(%) must be the last symbol of a number format.");
	}
	if(sciencePos >= 0){
		if(sciencePos == firstSymPos || sciencePos == lastSymPos)
			throw RuntimeException("The scientific notation(E) can't be the first symbol or last symbol of a number format.");
		for (int i = sciencePos + 1; i <= lastSymPos; ++i) {
			if(format[i] != '0')
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
		if (format[i] == '0')
			integerMinDigits_ ++;
		else if(format[i] == ',')
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
		while(i <= lastFractionPos && format[i] == '0') ++i;
		fractionMinDigits_ = i - pointPos -1;

		while(i <= lastFractionPos && format[i] == '#') ++i;
		fractionOptionalDigits_ = i - pointPos -1 - fractionMinDigits_;
	}

	rounding_ = 0.5;
	int digits = fractionMinDigits_ + fractionOptionalDigits_;
	while(digits){
		int count = (std::min)(digits, 10);
		digits -= count;
		rounding_ /= power10_[count - 1];
	}
	if(epsilon_ < rounding_/10)
		rounding_ += epsilon_;

	headSize_ = firstSymPos;
	head_ = format.substr(0, headSize_);
	tailSize_ = len - lastSymPos - 1;
	tail_ = format.substr(lastSymPos + 1, tailSize_);
}

string NumberFormat::format(double x) const{
	char buf[128];
	int cursor = 0;

   for(int i=0; i<headSize_; ++i)
	   buf[cursor++] = head_[i];

    if (x < 0) {
        buf[cursor++] = '-';
        x = -x;
    }
    if (percent_)
        x *= 100;

    bool enableSciNotation = science_ || x >= enableScientificNotationBeyond_;
    int sciencePower = 0;
    if (enableSciNotation) {
    	while(x < 1){
    		sciencePower -= 10;
    		x *= power10_[9];
    	}
        if(x >= 10){
        	while(x >= power10_[9]){
        		sciencePower += 10;
        		x /= power10_[9];
        	}
        	int i = 0;
        	while(i<10 && x >= power10_[i]) ++i;
        	if(i >= 1){
        		sciencePower += i;
        		x /= power10_[i - 1];
        	}
        }
        if (integerMinDigits_ > 1) {
        	int count = integerMinDigits_ - 1;
        	while(count){
        		int tmp = (std::min)(count, 10);
        		x *= power10_[tmp -1];
        		count -= tmp;
        	}
            sciencePower -= integerMinDigits_ - 1;
        }
    }
    x += rounding_;
    long long integral = (long long)x;
    double fraction = x - integral;

    // output integer part
    int intStart = cursor;
    int digits = 0;
    while (integral) {
    	if(digits && digits % segmentLength_ == 0)
    		buf[cursor++] = ',';
        buf[cursor++] = integral % 10 + '0';
        ++digits;
        integral /= 10;
    }
	for (; digits < integerMinDigits_; ++digits)
		buf[cursor++] = '0';
    //reverse the digits of integer part
	int intLen = (cursor - intStart)/2;
	for(int i=0; i<intLen; ++i){
		char tmp = buf[intStart + i];
		buf[intStart + i] = buf[cursor - 1 - i];
		buf[cursor - 1 - i] = tmp;
	}
	if(enableSciNotation && cursor - intStart == integerMinDigits_ + 1){
		++sciencePower;
		fraction /= 10;
		--cursor;
	}

	//output fraction part
    if (point_){
        buf[cursor++] = '.';
        if(fractionMinDigits_){
        	cursor += printFraction(buf + cursor, fractionMinDigits_, false, fraction);
        }
        if(fractionOptionalDigits_){
        	cursor += printFraction(buf + cursor, fractionOptionalDigits_, true, fraction);
        }
        if(buf[cursor -1] == '.') --cursor;
    }

    if (enableSciNotation) {
        buf[cursor++] = 'E';
        if (sciencePower < 0){
            sciencePower = -sciencePower;
            buf[cursor++] = '-';
        }
        intStart = cursor;
		digits = 0;
		while (sciencePower) {
			buf[cursor++] = sciencePower % 10 + '0';
			++digits;
			sciencePower /= 10;
		}
		for (; digits < science_; ++digits)
			buf[cursor++] = '0';
		//reverse the digits of integer part
		int intLen = (cursor - intStart)/2;
		for(int i=0; i<intLen; ++i){
			char tmp = buf[intStart + i];
			buf[intStart + i] = buf[cursor - 1 - i];
			buf[cursor - 1 - i] = tmp;
		}
    }
    if (percent_)
        buf[cursor++] = '%';
    for(int i=0; i<tailSize_; ++i)
    	buf[cursor++] = tail_[i];
    buf[cursor] = 0;
    return buf;
}

string NumberFormat::toString(long long x){
	char buf[30];
	int cursor = 0;
	int start = 0;
	if(x < 0){
		buf[cursor++] = '-';
		start = 1;
		x = -x;
	}
	while (x) {
		buf[cursor++] = x % 10 + '0';
		x /= 10;
	}
	if(cursor == start)
		buf[cursor++] = '0';

	int intLen = (cursor - start)/2;
	for(int i=0; i<intLen; ++i){
		char tmp = buf[start + i];
		buf[start + i] = buf[cursor - 1 - i];
		buf[cursor - 1 - i] = tmp;
	}
	buf[cursor] = 0;
	return buf;
}

DecimalFormat::DecimalFormat(const string& format) : format_(0), negFormat_(0){
	size_t pos = format.find(';');
	if(pos == string::npos || pos == 0 || pos == format.size() - 1)
		format_ = new NumberFormat(format);
	else{
		format_ = new NumberFormat(format.substr(0, pos));
		negFormat_ = new NumberFormat(format.substr(pos + 1));
	}
}

DecimalFormat::~DecimalFormat(){
	if(format_ != NULL)
		delete format_;
	if(negFormat_ != NULL)
		delete negFormat_;
}

string DecimalFormat::format(double x) const {
	if(negFormat_ != NULL && x < 0)
		return negFormat_->format(-x);
	else
		return format_->format(x);
}

vector<pair<int, int> > TemporalFormat::initFormatMap() {
	vector<pair<int, int> > initvector(128, std::make_pair(-1, -1));
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

TemporalFormat::TemporalFormat(const string& format) : quickFormat_(false), segmentCount_(0) {
	initialize(format);
}

void TemporalFormat::initialize(const string& format) {
	int len = format.length();
	if(len == 0)
		throw RuntimeException("The format string can't be empty.");
	else if(len > 128)
		throw RuntimeException("The format string is too big.");
	format_.reserve(len);

	//process escape first
	vector<bool> escape(len);
	int cursor = 0;
	int i = 0;
	while(i < len){
		char ch = format[i];
		if(ch == '\\'){
			if(i == len  - 1)
				throw RuntimeException("Invalid escape (\\)in the end of the format string.");
			format_.append(1, format[i+1]);
			escape[cursor++] = true;
			i += 2;
		}
		else{
			format_.append(1, ch);
			escape[cursor++] = false;
			++i;
		}
	}
	int cnt = 0;
	quickFormat_ = true;
	len = cursor;

	for (int i = 0; i <= len; ++i) {
		if (i == len || (i != 0 && (format_[i] != format_[i - 1] || escape[i] != escape[i - 1]))) {
			char ch = format_[i - 1];
			if (ch >= 0 && !escape[i - 1] && formatMap[ch].first != -1) {
				segments_[segmentCount_++] = FormatSegment(formatMap[ch].first, formatMap[ch].second, i - cnt, i - 1);
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

string TemporalFormat::format(long long nowtime, DATA_TYPE dtype) const{
	int timeNumber[10];
	memset(timeNumber, 0, sizeof(timeNumber));
	timeNumber[0] = 1970;
	timeNumber[1] = 1;
	timeNumber[2] = 1;
	switch (dtype) {
		case DT_MINUTE: {
			timeNumber[4] = nowtime / 60;
			timeNumber[6] = nowtime % 60;
			timeNumber[5] = timeNumber[4] / 12;
			timeNumber[3] = timeNumber[4] % 12;
			break;
		}
		case DT_SECOND: {
			timeNumber[7] = nowtime % 60;
			nowtime = nowtime / 60;
			timeNumber[6] = nowtime % 60;
			timeNumber[4] = nowtime / 60;
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
			timeNumber[4] = nowtime / 60;
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
			timeNumber[4] = nowtime / 60;
			timeNumber[5] = timeNumber[4] / 12;
			timeNumber[3] = timeNumber[4] % 12;
			break;
		}
		case DT_MONTH: {
			timeNumber[0] = nowtime / 12;
			timeNumber[1] = nowtime % 12 + 1;
			break;
		}
		case DT_DATE: {
			Util::parseDate((int)nowtime, timeNumber[0], timeNumber[1], timeNumber[2]);
			break;
		}
		case DT_DATETIME: {
			int tmp = nowtime / 86400ll;
			nowtime = nowtime % 86400ll;
			Util::parseDate(tmp, timeNumber[0], timeNumber[1], timeNumber[2]);
			timeNumber[7] = nowtime % 60;
			nowtime = nowtime / 60;
			timeNumber[6] = nowtime % 60;
			timeNumber[4] = nowtime / 60;
			timeNumber[5] = timeNumber[4] / 12;
			timeNumber[3] = timeNumber[4] % 12;
			break;
		}
		case DT_DATEHOUR: {
            int tmp = nowtime / 24;
            timeNumber[4] = nowtime % 24;
            Util::parseDate(tmp, timeNumber[0], timeNumber[1], timeNumber[2]);
            timeNumber[5] = timeNumber[4] / 12;
            timeNumber[3] = timeNumber[4] % 12;
            break;
		}
		case DT_TIMESTAMP: {
			int tmp = nowtime / 86400000ll;
			nowtime = nowtime % 86400000ll;
			Util::parseDate(tmp, timeNumber[0], timeNumber[1], timeNumber[2]);
			timeNumber[8] = nowtime % 1000;
			nowtime = nowtime / 1000;
			timeNumber[7] = nowtime % 60;
			nowtime = nowtime / 60;
			timeNumber[6] = nowtime % 60;
			timeNumber[4] = nowtime / 60;
			timeNumber[5] = timeNumber[4] / 12;
			timeNumber[3] = timeNumber[4] % 12;
			break;
		}
		default: {
			int tmp = nowtime / 86400000000000ll;
			nowtime = nowtime % 86400000000000ll;
			Util::parseDate(tmp, timeNumber[0], timeNumber[1], timeNumber[2]);
			timeNumber[9] = nowtime % 1000000000;
			nowtime = nowtime / 1000000000;
			timeNumber[7] = nowtime % 60;
			nowtime = nowtime / 60;
			timeNumber[6] = nowtime % 60;
			timeNumber[4] = nowtime / 60;
			timeNumber[5] = timeNumber[4] / 12;
			timeNumber[3] = timeNumber[4] % 12;
			break;
		}
	}

	if (quickFormat_) {
		string templateString(format_);
		for (int i = 0; i < segmentCount_; ++i) {
			int leftPos = segments_[i].startPos_;
			int rightPos = segments_[i].endPos_;
			int id = segments_[i].timeUnitIndex_;
			if (id == 5) {
				for (int j = leftPos, cnt = 0; j <= rightPos; ++j, ++cnt) {
					templateString[j] = cnt < 2 ? (timeNumber[5] ? pmString[cnt] : amString[cnt]) : ' ';
				}
			} else if (id == 1 && rightPos - leftPos + 1 == 3) {
				const char* month = monthName[timeNumber[1] - 1];
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
	else {
		string resultString;
		int prePos = 0;
		for (int i = 0; i < segmentCount_; ++i) {
			string tmpString;
			int leftPos = segments_[i].startPos_;
			int rightPos = segments_[i].endPos_;
			int index = segments_[i].timeUnitIndex_;
			int unitDemandlength = segments_[i].maxLength_;
			int formatDemandLength = rightPos - leftPos + 1;

			resultString += format_.substr(prePos, leftPos - prePos);
			if (index == 5) {
				for (int j = 0; j < formatDemandLength; ++j) {
					tmpString += (j < 2) ? (timeNumber[5] ? pmString[j] : amString[j]) : ' ';
				}
			} else if (index == 1 && formatDemandLength == 3) {
				const char* month = monthName[timeNumber[1] - 1];
				for (int j = 0; j < 3; ++j) {
					tmpString += month[j];
				}
			} else if (index == 0 && formatDemandLength == 2) {
				resultString += (timeNumber[0] / 10) % 10 + '0';
				resultString += timeNumber[0] % 10 + '0';
			} else {
				int tmpNumber = timeNumber[index];
				int cntDigits = 0;

				for (int j = 0; j < unitDemandlength; ++j) {
					cntDigits++;
					tmpString += tmpNumber % 10 + '0';
					tmpNumber /= 10;
					if (cntDigits >= formatDemandLength && tmpNumber == 0)
						break;
				}
				for (int i = 0; i < formatDemandLength - cntDigits; ++i)
					tmpString += '0';
				reverse(tmpString.begin(), tmpString.end());
			}
			resultString += tmpString;
			prePos = rightPos + 1;
		}
		resultString += format_.substr(prePos, format_.length() - prePos);
		return resultString;
	}
}

};
