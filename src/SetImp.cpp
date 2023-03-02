/*
 * SetImp.cpp
 *
 *  Created on: Jan 17, 2016
 *      Author: dzhou
 */

#include "SetImp.h"

namespace dolphindb {

void CharSet::contain(const ConstantSP& target, const ConstantSP& resultSP) const {
	if(target->isScalar()){
		resultSP->setBool(data_.find(target->getChar()) != data_.end());
	}
	else{
		ConstantSP source = (target->isSet() ? target->keys() : target);
		INDEX len = source->size();
		const int bufSize=Util::BUF_SIZE;
		char ret[bufSize];
		char* pret;
		char buf[bufSize];
		const char* pbuf;
		INDEX start=0;
		int count;

		unordered_set<char>::const_iterator end=data_.end();
		while(start<len){
			count=(std::min)(len-start,bufSize);
			pbuf=source->getCharConst(start,count,buf);
			pret=resultSP->getBoolBuffer(start,count,ret);
			for(int i=0;i<count;++i)
				pret[i]=data_.find(pbuf[i])!=end;
			resultSP->setBool(start,count,pret);
			start+=count;
		}
	}
}

bool CharSet::manipulate(const ConstantSP& value, bool deletion) {
	DATA_FORM form = value->getForm();
	if(form == DF_SCALAR){
		if(deletion){
			data_.erase(value->getChar());
		}
		else{
			data_.insert(value->getChar());
		}
	}
	else {
		ConstantSP source = (form == DF_SET ? value->keys() : value);
		INDEX len = source->size();
		const int bufSize = Util::BUF_SIZE;
		char buf[bufSize];
		const char* pbuf;
		INDEX start = 0;
		int count;

		while(start<len){
			count=(std::min)(len-start,bufSize);
			pbuf=source->getCharConst(start,count,buf);
			if(deletion){
				for(int i=0; i<count; ++i)
					data_.erase(pbuf[i]);
			}
			else
				data_.insert(pbuf, pbuf+count);
			start+=count;
		}
	}
	return true;
}

bool CharSet::inverse(const ConstantSP& value){
	if(!value->isSet() || value->getCategory() != getCategory())
		return false;
	ConstantSP source = value->keys();
	INDEX len = source->size();
	const int bufSize = Util::BUF_SIZE;
	char buf[bufSize];
	const char* pbuf;
	INDEX start = 0;
	int count;

	unordered_set<char>::iterator end = data_.end();
	while(start<len){
		count=(std::min)(len-start,bufSize);
		pbuf=source->getCharConst(start,count,buf);
		for(int i=0; i<count; ++i){
			unordered_set<char>::iterator  it = data_.find(pbuf[i]);
			if(it != end)
				data_.erase(it);
			else
				data_.insert(pbuf[i]);
		}
		start+=count;
	}
	return true;
}

bool CharSet::isSuperset(const ConstantSP& target) const {
	ConstantSP source = (target->isSet() ? target->keys() : target);
	INDEX len = source->size();
	const int bufSize = Util::BUF_SIZE;
	char buf[bufSize];
	const char* pbuf;
	INDEX start=0;
	int count;

	unordered_set<char>::const_iterator end=data_.end();
	while(start<len){
		count=(std::min)(len-start,bufSize);
		pbuf=source->getCharConst(start,count,buf);
		for(int i=0;i<count;++i){
			if(data_.find(pbuf[i]) == end)
				return false;
		}
		start+=count;
	}
	return true;
}

ConstantSP CharSet::interaction(const ConstantSP& value) const {
	CharSet* result = new CharSet(type_);
	ConstantSP resultSP(result);

	ConstantSP source = (value->isSet() ? value->keys() : value);
	INDEX len = source->size();
	const int bufSize = Util::BUF_SIZE;
	char buf[bufSize];
	const char* pbuf;
	INDEX start=0;
	int count;

	unordered_set<char>::const_iterator end=data_.end();
	while(start<len){
		count=(std::min)(len-start,bufSize);
		pbuf=source->getCharConst(start,count,buf);
		for(int i=0;i<count;++i){
			if(data_.find(pbuf[i])!=end)
				result->data_.insert(pbuf[i]);
		}
		start+=count;
	}
	return resultSP;
}

ConstantSP CharSet::getSubVector(INDEX start, INDEX length) const {
	unordered_set<char>::const_iterator it = data_.begin();
	for(int i=0; i<start; ++i) ++it;
	ConstantSP result = Util::createVector(type_, length, 0,true);

	const int bufSize = Util::BUF_SIZE;
	char buf[bufSize];
	char* pbuf;
	int count = 0;
	start = 0;

	while(start<length){
		count = (std::min)(bufSize, length - start);
		pbuf = result->getCharBuffer(start, count, buf);
		for(int i=0; i<count; ++i)
			pbuf[i] = *it++;
		result->setChar(start, count, pbuf);
		start += count;
	}
	result->setNullFlag(result->hasNull());
	return result;
}

void ShortSet::contain(const ConstantSP& target, const ConstantSP& resultSP) const {
	if(target->isScalar()){
		resultSP->setBool(data_.find(target->getShort()) != data_.end());
	}
	else{
		ConstantSP source = (target->isSet() ? target->keys() : target);
		INDEX len = source->size();
		const int bufSize = Util::BUF_SIZE;
		char ret[bufSize];
		char* pret;
		short buf[bufSize];
		const short* pbuf;
		INDEX start=0;
		int count;

		unordered_set<short>::const_iterator end=data_.end();
		while(start<len){
			count=(std::min)(len-start,bufSize);
			pbuf=source->getShortConst(start,count,buf);
			pret=resultSP->getBoolBuffer(start,count,ret);
			for(int i=0;i<count;++i)
				pret[i]=data_.find(pbuf[i])!=end;
			resultSP->setBool(start,count,pret);
			start+=count;
		}
	}
}

bool ShortSet::manipulate(const ConstantSP& value, bool deletion) {
	DATA_FORM form = value->getForm();
	if(form == DF_SCALAR){
		if(deletion){
			data_.erase(value->getShort());
		}
		else{
			data_.insert(value->getShort());
		}
	}
	else {
		ConstantSP source = (form == DF_SET ? value->keys() : value);
		INDEX len = source->size();
		const int bufSize = Util::BUF_SIZE;
		short buf[bufSize];
		const short* pbuf;
		INDEX start = 0;
		int count;

		while(start<len){
			count=(std::min)(len-start,bufSize);
			pbuf=source->getShortConst(start,count,buf);
			if(deletion){
				for(int i=0; i<count; ++i)
					data_.erase(pbuf[i]);
			}
			else
				data_.insert(pbuf, pbuf+count);
			start+=count;
		}
	}
	return true;
}

bool ShortSet::inverse(const ConstantSP& value){
	if(!value->isSet() || value->getCategory() != getCategory())
		return false;
	ConstantSP source = value->keys();
	INDEX len = source->size();
	const int bufSize = Util::BUF_SIZE;
	short buf[bufSize];
	const short* pbuf;
	INDEX start = 0;
	int count;

	unordered_set<short>::iterator end = data_.end();
	while(start<len){
		count=(std::min)(len-start,bufSize);
		pbuf=source->getShortConst(start,count,buf);
		for(int i=0; i<count; ++i){
			unordered_set<short>::iterator  it = data_.find(pbuf[i]);
			if(it != end)
				data_.erase(it);
			else
				data_.insert(pbuf[i]);
		}
		start+=count;
	}
	return true;
}

bool ShortSet::isSuperset(const ConstantSP& target) const {
	ConstantSP source = (target->isSet() ? target->keys() : target);
	INDEX len = source->size();
	const int bufSize = Util::BUF_SIZE;
	short buf[bufSize];
	const short* pbuf;
	INDEX start=0;
	int count;

	unordered_set<short>::const_iterator end=data_.end();
	while(start<len){
		count=(std::min)(len-start,bufSize);
		pbuf=source->getShortConst(start,count,buf);
		for(int i=0;i<count;++i){
			if(data_.find(pbuf[i]) == end)
				return false;
		}
		start+=count;
	}
	return true;
}

ConstantSP ShortSet::interaction(const ConstantSP& value) const {
	ShortSet* result = new ShortSet();
	ConstantSP resultSP(result);

	ConstantSP source = (value->isSet() ? value->keys() : value);
	INDEX len = source->size();
	const int bufSize = Util::BUF_SIZE;
	short buf[bufSize];
	const short* pbuf;
	INDEX start=0;
	int count;

	unordered_set<short>::const_iterator end=data_.end();
	while(start<len){
		count=(std::min)(len-start,bufSize);
		pbuf=source->getShortConst(start,count,buf);
		for(int i=0;i<count;++i){
			if(data_.find(pbuf[i])!=end)
				result->data_.insert(pbuf[i]);
		}
		start+=count;
	}
	return resultSP;
}

ConstantSP ShortSet::getSubVector(INDEX start, INDEX length) const {
	unordered_set<short>::const_iterator it = data_.begin();
	for(int i=0; i<start; ++i) ++it;
	ConstantSP result = Util::createVector(type_, length, 0,true);

	const int bufSize = Util::BUF_SIZE;
	short buf[bufSize];
	short* pbuf;
	int count = 0;
	start = 0;

	while(start<length){
		count = (std::min)(bufSize, length - start);
		pbuf = result->getShortBuffer(start, count, buf);
		for(int i=0; i<count; ++i)
			pbuf[i] = *it++;
		result->setShort(start, count, pbuf);
		start += count;
	}
	result->setNullFlag(result->hasNull());
	return result;
}

void IntSet::contain(const ConstantSP& target, const ConstantSP& resultSP) const {
	if(target->isScalar()){
		resultSP->setBool(data_.find(target->getInt()) != data_.end());
	}
	else{
		ConstantSP source = (target->isSet() ? target->keys() : target);
		INDEX len = source->size();
		const int bufSize = Util::BUF_SIZE;
		char ret[bufSize];
		char* pret;
		int buf[bufSize];
		const int* pbuf;
		INDEX start=0;
		int count;

		unordered_set<int>::const_iterator end=data_.end();
		while(start<len){
			count=(std::min)(len-start,bufSize);
			pbuf=source->getIntConst(start,count,buf);
			pret=resultSP->getBoolBuffer(start,count,ret);
			for(int i=0;i<count;++i)
				pret[i]=data_.find(pbuf[i])!=end;
			resultSP->setBool(start,count,pret);
			start+=count;
		}
	}
}

bool IntSet::manipulate(const ConstantSP& value, bool deletion) {
	DATA_FORM form = value->getForm();
	if(form == DF_SCALAR){
		if(deletion){
			data_.erase(value->getInt());
		}
		else{
			data_.insert(value->getInt());
		}
	}
	else {
		ConstantSP source = (form == DF_SET ? value->keys() : value);
		INDEX len = source->size();
		const int bufSize = Util::BUF_SIZE;
		int buf[bufSize];
		const int* pbuf;
		INDEX start = 0;
		int count;

		while(start<len){
			count=(std::min)(len-start,bufSize);
			pbuf=source->getIntConst(start,count,buf);
			if(deletion){
				for(int i=0; i<count; ++i)
					data_.erase(pbuf[i]);
			}
			else
				data_.insert(pbuf, pbuf+count);
			start+=count;
		}
	}
	return true;
}

bool IntSet::inverse(const ConstantSP& value){
	if(!value->isSet() || value->getCategory() != getCategory())
		return false;
	ConstantSP source = value->keys();
	INDEX len = source->size();
	const int bufSize = Util::BUF_SIZE;
	int buf[bufSize];
	const int* pbuf;
	INDEX start = 0;
	int count;

	unordered_set<int>::iterator end = data_.end();
	while(start<len){
		count=(std::min)(len-start,bufSize);
		pbuf=source->getIntConst(start,count,buf);
		for(int i=0; i<count; ++i){
			unordered_set<int>::iterator  it = data_.find(pbuf[i]);
			if(it != end)
				data_.erase(it);
			else
				data_.insert(pbuf[i]);
		}
		start+=count;
	}
	return true;
}

bool IntSet::isSuperset(const ConstantSP& target) const {
	ConstantSP source = (target->isSet() ? target->keys() : target);
	INDEX len = source->size();
	const int bufSize = Util::BUF_SIZE;
	int buf[bufSize];
	const int* pbuf;
	INDEX start=0;
	int count;

	unordered_set<int>::const_iterator end=data_.end();
	while(start<len){
		count=(std::min)(len-start,bufSize);
		pbuf=source->getIntConst(start,count,buf);
		for(int i=0;i<count;++i){
			if(data_.find(pbuf[i]) == end)
				return false;
		}
		start+=count;
	}
	return true;
}

ConstantSP IntSet::interaction(const ConstantSP& value) const {
	IntSet* result = new IntSet(type_);
	ConstantSP resultSP(result);

	ConstantSP source = (value->isSet() ? value->keys() : value);
	INDEX len = source->size();
	const int bufSize = Util::BUF_SIZE;
	int buf[bufSize];
	const int* pbuf;
	INDEX start=0;
	int count;

	unordered_set<int>::const_iterator end=data_.end();
	while(start<len){
		count=(std::min)(len-start,bufSize);
		pbuf=source->getIntConst(start,count,buf);
		for(int i=0;i<count;++i){
			if(data_.find(pbuf[i])!=end)
				result->data_.insert(pbuf[i]);
		}
		start+=count;
	}
	return resultSP;
}

ConstantSP IntSet::getSubVector(INDEX start, INDEX length) const {
	unordered_set<int>::const_iterator it = data_.begin();
	for(int i=0; i<start; ++i) ++it;
	ConstantSP result = Util::createVector(type_, length, 0,true);

	const int bufSize = Util::BUF_SIZE;
	int buf[bufSize];
	int* pbuf;
	int count = 0;
	start = 0;

	while(start<length){
		count = (std::min)(bufSize, length - start);
		pbuf = result->getIntBuffer(start, count, buf);
		for(int i=0; i<count; ++i)
			pbuf[i] = *it++;
		result->setInt(start, count, pbuf);
		start += count;
	}
	result->setNullFlag(result->hasNull());
	return result;
}

void LongSet::contain(const ConstantSP& target, const ConstantSP& resultSP) const {
	if(target->isScalar()){
		resultSP->setBool(data_.find(target->getLong()) != data_.end());
	}
	else{
		ConstantSP source = (target->isSet() ? target->keys() : target);
		INDEX len = source->size();
		const int bufSize = Util::BUF_SIZE;
		char ret[bufSize];
		char* pret;
		long long buf[bufSize];
		const long long* pbuf;
		INDEX start=0;
		int count;

		unordered_set<long long>::const_iterator end=data_.end();
		while(start<len){
			count=(std::min)(len-start,bufSize);
			pbuf=source->getLongConst(start,count,buf);
			pret=resultSP->getBoolBuffer(start,count,ret);
			for(int i=0;i<count;++i)
				pret[i]=data_.find(pbuf[i])!=end;
			resultSP->setBool(start,count,pret);
			start+=count;
		}
	}
}

bool LongSet::manipulate(const ConstantSP& value, bool deletion) {
	DATA_FORM form = value->getForm();
	if(form == DF_SCALAR){
		if(deletion){
			data_.erase(value->getLong());
		}
		else{
			data_.insert(value->getLong());
		}
	}
	else {
		ConstantSP source = (form == DF_SET ? value->keys() : value);
		INDEX len = source->size();
		const int bufSize = Util::BUF_SIZE;
		long long buf[bufSize];
		const long long* pbuf;
		INDEX start = 0;
		int count;

		while(start<len){
			count=(std::min)(len-start,bufSize);
			pbuf=source->getLongConst(start,count,buf);
			if(deletion){
				for(int i=0; i<count; ++i)
					data_.erase(pbuf[i]);
			}
			else
				data_.insert(pbuf, pbuf+count);
			start+=count;
		}
	}
	return true;
}

bool LongSet::inverse(const ConstantSP& value){
	if(!value->isSet() || value->getCategory() != getCategory())
		return false;
	ConstantSP source = value->keys();
	INDEX len = source->size();
	const int bufSize = Util::BUF_SIZE;
	long long buf[bufSize];
	const long long* pbuf;
	INDEX start = 0;
	int count;

	unordered_set<long long>::iterator end = data_.end();
	while(start<len){
		count=(std::min)(len-start,bufSize);
		pbuf=source->getLongConst(start,count,buf);
		for(int i=0; i<count; ++i){
			unordered_set<long long>::iterator  it = data_.find(pbuf[i]);
			if(it != end)
				data_.erase(it);
			else
				data_.insert(pbuf[i]);
		}
		start+=count;
	}
	return true;
}

bool LongSet::isSuperset(const ConstantSP& target) const {
	ConstantSP source = (target->isSet() ? target->keys() : target);
	INDEX len = source->size();
	const int bufSize = Util::BUF_SIZE;
	long long buf[bufSize];
	const long long* pbuf;
	INDEX start=0;
	int count;

	unordered_set<long long>::const_iterator end=data_.end();
	while(start<len){
		count=(std::min)(len-start,bufSize);
		pbuf=source->getLongConst(start,count,buf);
		for(int i=0;i<count;++i){
			if(data_.find(pbuf[i]) == end)
				return false;
		}
		start+=count;
	}
	return true;
}

ConstantSP LongSet::interaction(const ConstantSP& value) const {
	LongSet* result = new LongSet(type_);
	ConstantSP resultSP(result);

	ConstantSP source = (value->isSet() ? value->keys() : value);
	INDEX len = source->size();
	const int bufSize = Util::BUF_SIZE;
	long long buf[bufSize];
	const long long* pbuf;
	INDEX start=0;
	int count;

	unordered_set<long long>::const_iterator end=data_.end();
	while(start<len){
		count=(std::min)(len-start,bufSize);
		pbuf=source->getLongConst(start,count,buf);
		for(int i=0;i<count;++i){
			if(data_.find(pbuf[i])!=end)
				result->data_.insert(pbuf[i]);
		}
		start+=count;
	}
	return resultSP;
}

ConstantSP LongSet::getSubVector(INDEX start, INDEX length) const {
	unordered_set<long long>::const_iterator it = data_.begin();
	for(int i=0; i<start; ++i) ++it;
	ConstantSP result = Util::createVector(type_, length, 0,true);

	const int bufSize = Util::BUF_SIZE;
	long long buf[bufSize];
	long long* pbuf;
	int count = 0;
	start = 0;

	while(start<length){
		count = (std::min)(bufSize, length - start);
		pbuf = result->getLongBuffer(start, count, buf);
		for(int i=0; i<count; ++i)
			pbuf[i] = *it++;
		result->setLong(start, count, pbuf);
		start += count;
	}
	result->setNullFlag(result->hasNull());
	return result;
}

void FloatSet::contain(const ConstantSP& target, const ConstantSP& resultSP) const {
	if(target->isScalar()){
		resultSP->setBool(data_.find(target->getFloat()) != data_.end());
	}
	else{
		ConstantSP source = (target->isSet() ? target->keys() : target);
		INDEX len = source->size();
		const int bufSize = Util::BUF_SIZE;
		char ret[bufSize];
		char* pret;
		float buf[bufSize];
		const float* pbuf;
		INDEX start=0;
		int count;

		unordered_set<float>::const_iterator end=data_.end();
		while(start<len){
			count=(std::min)(len-start,bufSize);
			pbuf=source->getFloatConst(start,count,buf);
			pret=resultSP->getBoolBuffer(start,count,ret);
			for(int i=0;i<count;++i)
				pret[i]=data_.find(pbuf[i])!=end;
			resultSP->setBool(start,count,pret);
			start+=count;
		}
	}
}

bool FloatSet::manipulate(const ConstantSP& value, bool deletion) {
	DATA_FORM form = value->getForm();
	if(form == DF_SCALAR){
		if(deletion){
			data_.erase(value->getFloat());
		}
		else{
			data_.insert(value->getFloat());
		}
	}
	else {
		ConstantSP source = (form == DF_SET ? value->keys() : value);
		INDEX len = source->size();
		const int bufSize = Util::BUF_SIZE;
		float buf[bufSize];
		const float* pbuf;
		INDEX start = 0;
		int count;

		while(start<len){
			count=(std::min)(len-start,bufSize);
			pbuf=source->getFloatConst(start,count,buf);
			if(deletion){
				for(int i=0; i<count; ++i)
					data_.erase(pbuf[i]);
			}
			else
				data_.insert(pbuf, pbuf+count);
			start+=count;
		}
	}
	return true;
}

bool FloatSet::inverse(const ConstantSP& value){
	if(!value->isSet() || value->getCategory() != getCategory())
		return false;
	ConstantSP source = value->keys();
	INDEX len = source->size();
	const int bufSize = Util::BUF_SIZE;
	float buf[bufSize];
	const float* pbuf;
	INDEX start = 0;
	int count;

	unordered_set<float>::iterator end = data_.end();
	while(start<len){
		count=(std::min)(len-start,bufSize);
		pbuf=source->getFloatConst(start,count,buf);
		for(int i=0; i<count; ++i){
			unordered_set<float>::iterator  it = data_.find(pbuf[i]);
			if(it != end)
				data_.erase(it);
			else
				data_.insert(pbuf[i]);
		}
		start+=count;
	}
	return true;
}

bool FloatSet::isSuperset(const ConstantSP& target) const {
	ConstantSP source = (target->isSet() ? target->keys() : target);
	INDEX len = source->size();
	const int bufSize = Util::BUF_SIZE;
	float buf[bufSize];
	const float* pbuf;
	INDEX start=0;
	int count;

	unordered_set<float>::const_iterator end=data_.end();
	while(start<len){
		count=(std::min)(len-start,bufSize);
		pbuf=source->getFloatConst(start,count,buf);
		for(int i=0;i<count;++i){
			if(data_.find(pbuf[i]) == end)
				return false;
		}
		start+=count;
	}
	return true;
}

ConstantSP FloatSet::interaction(const ConstantSP& value) const {
	FloatSet* result = new FloatSet();
	ConstantSP resultSP(result);

	ConstantSP source = (value->isSet() ? value->keys() : value);
	INDEX len = source->size();
	const int bufSize = Util::BUF_SIZE;
	float buf[bufSize];
	const float* pbuf;
	INDEX start=0;
	int count;

	unordered_set<float>::const_iterator end=data_.end();
	while(start<len){
		count=(std::min)(len-start,bufSize);
		pbuf=source->getFloatConst(start,count,buf);
		for(int i=0;i<count;++i){
			if(data_.find(pbuf[i])!=end)
				result->data_.insert(pbuf[i]);
		}
		start+=count;
	}
	return resultSP;
}

ConstantSP FloatSet::getSubVector(INDEX start, INDEX length) const {
	unordered_set<float>::const_iterator it = data_.begin();
	for(int i=0; i<start; ++i) ++it;
	ConstantSP result = Util::createVector(type_, length, 0,true);

	const int bufSize = Util::BUF_SIZE;
	float buf[bufSize];
	float* pbuf;
	int count = 0;
	start = 0;

	while(start<length){
		count = (std::min)(bufSize, length - start);
		pbuf = result->getFloatBuffer(start, count, buf);
		for(int i=0; i<count; ++i)
			pbuf[i] = *it++;
		result->setFloat(start, count, pbuf);
		start += count;
	}
	result->setNullFlag(result->hasNull());
	return result;
}

void DoubleSet::contain(const ConstantSP& target, const ConstantSP& resultSP) const {
	if(target->isScalar()){
		resultSP->setBool(data_.find(target->getDouble()) != data_.end());
	}
	else{
		ConstantSP source = (target->isSet() ? target->keys() : target);
		INDEX len = source->size();
		const int bufSize = Util::BUF_SIZE;
		char ret[bufSize];
		char* pret;
		double buf[bufSize];
		const double* pbuf;
		INDEX start=0;
		int count;

		unordered_set<double>::const_iterator end=data_.end();
		while(start<len){
			count=(std::min)(len-start,bufSize);
			pbuf=source->getDoubleConst(start,count,buf);
			pret=resultSP->getBoolBuffer(start,count,ret);
			for(int i=0;i<count;++i)
				pret[i]=data_.find(pbuf[i])!=end;
			resultSP->setBool(start,count,pret);
			start+=count;
		}
	}
}

bool DoubleSet::manipulate(const ConstantSP& value, bool deletion) {
	DATA_FORM form = value->getForm();
	if(form == DF_SCALAR){
		if(deletion){
			data_.erase(value->getDouble());
		}
		else{
			data_.insert(value->getDouble());
		}
	}
	else {
		ConstantSP source = (form == DF_SET ? value->keys() : value);
		INDEX len = source->size();
		const int bufSize = Util::BUF_SIZE;
		double buf[bufSize];
		const double* pbuf;
		INDEX start = 0;
		int count;

		while(start<len){
			count=(std::min)(len-start,bufSize);
			pbuf=source->getDoubleConst(start,count,buf);
			if(deletion){
				for(int i=0; i<count; ++i)
					data_.erase(pbuf[i]);
			}
			else
				data_.insert(pbuf, pbuf+count);
			start+=count;
		}
	}
	return true;
}

bool DoubleSet::inverse(const ConstantSP& value){
	if(!value->isSet() || value->getCategory() != getCategory())
		return false;
	ConstantSP source = value->keys();
	INDEX len = source->size();
	const int bufSize = Util::BUF_SIZE;
	double buf[bufSize];
	const double* pbuf;
	INDEX start = 0;
	int count;

	unordered_set<double>::iterator end = data_.end();
	while(start<len){
		count=(std::min)(len-start,bufSize);
		pbuf=source->getDoubleConst(start,count,buf);
		for(int i=0; i<count; ++i){
			unordered_set<double>::iterator  it = data_.find(pbuf[i]);
			if(it != end)
				data_.erase(it);
			else
				data_.insert(pbuf[i]);
		}
		start+=count;
	}
	return true;
}

bool DoubleSet::isSuperset(const ConstantSP& target) const {
	ConstantSP source = (target->isSet() ? target->keys() : target);
	INDEX len = source->size();
	const int bufSize = Util::BUF_SIZE;
	double buf[bufSize];
	const double* pbuf;
	INDEX start=0;
	int count;

	unordered_set<double>::const_iterator end=data_.end();
	while(start<len){
		count=(std::min)(len-start,bufSize);
		pbuf=source->getDoubleConst(start,count,buf);
		for(int i=0;i<count;++i){
			if(data_.find(pbuf[i]) == end)
				return false;
		}
		start+=count;
	}
	return true;
}

ConstantSP DoubleSet::interaction(const ConstantSP& value) const {
	DoubleSet* result = new DoubleSet();
	ConstantSP resultSP(result);

	ConstantSP source = (value->isSet() ? value->keys() : value);
	INDEX len = source->size();
	const int bufSize = Util::BUF_SIZE;
	double buf[bufSize];
	const double* pbuf;
	INDEX start=0;
	int count;

	unordered_set<double>::const_iterator end=data_.end();
	while(start<len){
		count=(std::min)(len-start,bufSize);
		pbuf=source->getDoubleConst(start,count,buf);
		for(int i=0;i<count;++i){
			if(data_.find(pbuf[i])!=end)
				result->data_.insert(pbuf[i]);
		}
		start+=count;
	}
	return resultSP;
}

ConstantSP DoubleSet::getSubVector(INDEX start, INDEX length) const {
	unordered_set<double>::const_iterator it = data_.begin();
	for(int i=0; i<start; ++i) ++it;
	ConstantSP result = Util::createVector(type_, length, 0,true);

	const int bufSize = Util::BUF_SIZE;
	double buf[bufSize];
	double* pbuf;
	int count = 0;
	start = 0;

	while(start<length){
		count = (std::min)(bufSize, length - start);
		pbuf = result->getDoubleBuffer(start, count, buf);
		for(int i=0; i<count; ++i)
			pbuf[i] = *it++;
		result->setDouble(start, count, pbuf);
		start += count;
	}
	result->setNullFlag(result->hasNull());
	return result;
}

void StringSet::contain(const ConstantSP& target, const ConstantSP& resultSP) const {
	if(target->isScalar()){
		resultSP->setBool(data_.find(target->getString()) != data_.end());
	}
	else{
		ConstantSP source = (target->isSet() ? target->keys() : target);
		INDEX len = source->size();
		const int bufSize = Util::BUF_SIZE;
		char ret[bufSize];
		char* pret;
		char* buf[bufSize];
		char** pbuf;
		INDEX start=0;
		int count;

		unordered_set<string>::const_iterator end=data_.end();
		while(start<len){
			count=(std::min)(len-start,bufSize);
			pbuf=source->getStringConst(start,count,buf);
			pret=resultSP->getBoolBuffer(start,count,ret);
			for(int i=0;i<count;++i)
				pret[i]=data_.find(pbuf[i])!=end;
			resultSP->setBool(start,count,pret);
			start+=count;
		}
	}
}

bool StringSet::manipulate(const ConstantSP& value, bool deletion) {
	DATA_FORM form = value->getForm();
	if(form == DF_SCALAR){
		if(deletion){
			data_.erase(value->getString());
		}
		else{
			data_.insert(value->getString());
		}
	}
	else {
		ConstantSP source = (form == DF_SET ? value->keys() : value);
		INDEX len = source->size();
		const int bufSize = Util::BUF_SIZE;
		char* buf[bufSize];
		char** pbuf;
		INDEX start = 0;
		int count;

		while(start<len){
			count=(std::min)(len-start,bufSize);
			pbuf=source->getStringConst(start,count,buf);
			if(deletion){
				for(int i=0; i<count; ++i)
					data_.erase(pbuf[i]);
			}
			else
				data_.insert(pbuf, pbuf+count);
			start+=count;
		}
	}
	return true;
}

bool StringSet::inverse(const ConstantSP& value){
	if(!value->isSet() || value->getCategory() != getCategory())
		return false;
	ConstantSP source = value->keys();
	INDEX len = source->size();
	const int bufSize = Util::BUF_SIZE;
	char* buf[bufSize];
	char** pbuf;
	INDEX start = 0;
	int count;

	unordered_set<string>::iterator end = data_.end();
	while(start<len){
		count=(std::min)(len-start,bufSize);
		pbuf=source->getStringConst(start,count,buf);
		for(int i=0; i<count; ++i){
			unordered_set<string>::iterator  it = data_.find(pbuf[i]);
			if(it != end)
				data_.erase(it);
			else
				data_.insert(pbuf[i]);
		}
		start+=count;
	}
	return true;
}

bool StringSet::isSuperset(const ConstantSP& target) const {
	ConstantSP source = (target->isSet() ? target->keys() : target);
	INDEX len = source->size();
	const int bufSize = Util::BUF_SIZE;
	char* buf[bufSize];
	char** pbuf;
	INDEX start=0;
	int count;

	unordered_set<string>::const_iterator end=data_.end();
	while(start<len){
		count=(std::min)(len-start,bufSize);
		pbuf=source->getStringConst(start,count,buf);
		for(int i=0;i<count;++i){
			if(data_.find(pbuf[i]) == end)
				return false;
		}
		start+=count;
	}
	return true;
}

ConstantSP StringSet::interaction(const ConstantSP& value) const {
	StringSet* result = new StringSet();
	ConstantSP resultSP(result);

	ConstantSP source = (value->isSet() ? value->keys() : value);
	INDEX len = source->size();
	const int bufSize = Util::BUF_SIZE;
	char* buf[bufSize];
	char** pbuf;
	INDEX start=0;
	int count;

	unordered_set<string>::const_iterator end=data_.end();
	while(start<len){
		count=(std::min)(len-start,bufSize);
		pbuf=source->getStringConst(start,count,buf);
		for(int i=0;i<count;++i){
			if(data_.find(pbuf[i])!=end)
				result->data_.insert(pbuf[i]);
		}
		start+=count;
	}
	return resultSP;
}

ConstantSP StringSet::getSubVector(INDEX start, INDEX length) const {
	unordered_set<string>::const_iterator it = data_.begin();
	for(int i=0; i<start; ++i)
		++it;
	ConstantSP result = Util::createVector(type_, length, 0,true);
	const int bufSize = Util::BUF_SIZE;
	char* buf[bufSize];
	int count = 0;
	start = 0;

	while(start<length){
		count = (std::min)(bufSize, length - start);
		for(int i=0; i<count; ++i)
			buf[i] = (char*)(*it++).c_str();
		result->setString(start, count, buf);
		start += count;
	}
	return result;
}


void Int128Set::contain(const ConstantSP& target, const ConstantSP& resultSP) const {
	if(target->isScalar()){
		resultSP->setBool(data_.find(target->getInt128()) != data_.end());
	}
	else{
		ConstantSP source = (target->isSet() ? target->keys() : target);
		INDEX len = source->size();
		int bufSize=std::min(len,Util::BUF_SIZE);
		std::unique_ptr< char[]> ret(new  char[bufSize ]); //char ret[bufSize];
		char* pret;
		std::unique_ptr<unsigned char[]> buf(new unsigned char[bufSize * 16]); //unsigned char buf[bufSize * 16];
		const Guid* pbuf;
		INDEX start=0;
		int count;

		unordered_set<Guid>::const_iterator end=data_.end();
		while(start<len){
			count=std::min(len-start,bufSize);
			pbuf=(const Guid*)source->getBinaryConst(start,count,16,buf.get());
			pret=resultSP->getBoolBuffer(start,count,ret.get());
			for(int i=0;i<count;++i)
				pret[i]=data_.find(pbuf[i])!=end;
			resultSP->setBool(start,count,pret);
			start+=count;
		}
	}
}

bool Int128Set::manipulate(const ConstantSP& value, bool deletion) {
	DATA_FORM form = value->getForm();
	if(form == DF_SCALAR){
		if(deletion){
			data_.erase(value->getInt128());
		}
		else{
			data_.insert(value->getInt128());
		}
	}
	else {
		ConstantSP source = (form == DF_SET ? value->keys() : value);
		INDEX len = source->size();
		int bufSize = std::min(len,Util::BUF_SIZE);
		std::unique_ptr<unsigned char[]> buf(new unsigned char[bufSize * 16]); //unsigned char buf[bufSize * 16];
		const Guid* pbuf;
		INDEX start = 0;
		int count;

		while(start<len){
			count=std::min(len-start,bufSize);
			pbuf=(const Guid*)source->getBinaryConst(start,count,16,buf.get());
			if(deletion){
				for(int i=0; i<count; ++i)
					data_.erase(pbuf[i]);
			}
			else
				data_.insert(pbuf, pbuf+count);
			start+=count;
		}
	}
	return true;
}

bool Int128Set::inverse(const ConstantSP& value){
	if(!value->isSet() || value->getCategory() != getCategory())
		return false;
	ConstantSP source = value->keys();
	INDEX len = source->size();
	int bufSize = std::min(len,Util::BUF_SIZE);
	std::unique_ptr<unsigned char[]> buf(new unsigned char[bufSize * 16]); //unsigned char buf[bufSize * 16];
	const Guid* pbuf;
	INDEX start = 0;
	int count;

	unordered_set<Guid>::iterator end = data_.end();
	while(start<len){
		count=std::min(len-start,bufSize);
		pbuf=(const Guid*)source->getBinaryConst(start,count,16,buf.get());
		for(int i=0; i<count; ++i){
			unordered_set<Guid>::iterator  it = data_.find(pbuf[i]);
			if(it != end)
				data_.erase(it);
			else
				data_.insert(pbuf[i]);
		}
		start+=count;
	}
	return true;
}

bool Int128Set::isSuperset(const ConstantSP& target) const {
	ConstantSP source = (target->isSet() ? target->keys() : target);
	INDEX len = source->size();
	int bufSize=std::min(len,Util::BUF_SIZE);
	std::unique_ptr<unsigned char[]> buf(new unsigned char[bufSize * 16]); //unsigned char buf[bufSize * 16];
	const Guid* pbuf;
	INDEX start=0;
	int count;

	unordered_set<Guid>::const_iterator end=data_.end();
	while(start<len){
		count=std::min(len-start,bufSize);
		pbuf=(const Guid*)source->getBinaryConst(start,count,16,buf.get());
		for(int i=0;i<count;++i){
			if(data_.find(pbuf[i]) == end)
				return false;
		}
		start+=count;
	}
	return true;
}

ConstantSP Int128Set::interaction(const ConstantSP& value) const {
	Int128Set* result = new Int128Set(type_);
	ConstantSP resultSP(result);

	ConstantSP source = (value->isSet() ? value->keys() : value);
	INDEX len = source->size();
	int bufSize=std::min(len,Util::BUF_SIZE);
	std::unique_ptr<unsigned char[]> buf(new unsigned char[bufSize * 16]); //unsigned char buf[bufSize*16];
	const Guid* pbuf;
	INDEX start=0;
	int count;

	unordered_set<Guid>::const_iterator end=data_.end();
	while(start<len){
		count=std::min(len-start,bufSize);
		pbuf=(const Guid*)source->getBinaryConst(start,count,16,buf.get());
		for(int i=0;i<count;++i){
			if(data_.find(pbuf[i])!=end)
				result->data_.insert(pbuf[i]);
		}
		start+=count;
	}
	return resultSP;
}

ConstantSP Int128Set::getSubVector(INDEX start, INDEX length) const {
	unordered_set<Guid>::const_iterator it = data_.begin();
	for(int i=0; i<start; ++i) ++it;
	ConstantSP result = Util::createVector(type_, length, 0,true);

	INDEX bufSize=std::min((INDEX)data_.size(),Util::BUF_SIZE);
	std::unique_ptr<unsigned char[]> buf(new unsigned char[bufSize * 16]); //unsigned char buf[bufSize*16];
	Guid* pbuf;
	int count = 0;
	start = 0;

	while(start<length){
		count = std::min(bufSize, length - start);
		pbuf = (Guid*)result->getBinaryBuffer(start, count, 16, buf.get());
		for(int i=0; i<count; ++i)
			pbuf[i] = *it++;
		result->setBinary(start, count, 16, (unsigned char*)pbuf);
		start += count;
	}
	result->setNullFlag(result->hasNull());
	return result;
}

};
