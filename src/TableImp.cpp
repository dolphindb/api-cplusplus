/*
 * Table.cpp
 *
 *  Created on: Nov 3, 2012
 *      Author: dzhou
 */

#include <algorithm>

#include "TableImp.h"
#include "Dictionary.h"
#include "Util.h"
#include "ConstantImp.h"
#include "ConstantMarshall.h"

namespace dolphindb {

using std::vector;
using std::string;

AbstractTable::AbstractTable(const SmartPointer<vector<string>>& colNames) : colNames_(colNames){
	colMap_ = new std::unordered_map<string, int>();
	for(unsigned int i=0; i<colNames->size(); ++i){
		colMap_->insert(std::pair<string, int>(Util::lower(colNames->at(i)),i));
	}
}

AbstractTable::AbstractTable(const SmartPointer<vector<string>>& colNames, SmartPointer<std::unordered_map<string,int>> colMap) : colNames_(colNames), colMap_(colMap){

}

string AbstractTable::getTableClassName() const {
	switch(getTableType()){
	case BASICTBL:
		return "BasicTable";
	default:
		return "";
	}
}

string AbstractTable::getTableTypeName() const {
	switch(getTableType()){
	case BASICTBL:
		return "A basic table";
	default:
		return "";
	}
}

ConstantSP AbstractTable::getColumn(const string& name) const{
	std::unordered_map<string,int>::const_iterator it=colMap_->find(Util::lower(name));
	if(it==colMap_->end()){
		throw TableRuntimeException("Unrecognized column name " + name);
	}
	return getColumn(it->second);
}

ConstantSP AbstractTable::getColumn(const string& qualifier, const string& name) const{
	std::unordered_map<string,int>::const_iterator it=colMap_->find(Util::lower(name));
	if(it!=colMap_->end()){
		if(Util::equalIgnoreCase(qualifier, name_))
			return getColumn(it->second);
	}
	throw TableRuntimeException("Unrecognized column name " + qualifier + "." + name);
}

ConstantSP AbstractTable::getColumn(const string& name, const ConstantSP& rowFilter) const{
	if(rowFilter.isNull())
		return getColumn(name);
	else
		return getColumn(name)->get(rowFilter);
}

ConstantSP AbstractTable::getColumn(const string& qualifier, const string& name, const ConstantSP& rowFilter) const{
	if(rowFilter.isNull())
		return getColumn(qualifier, name);
	else
		return getColumn(qualifier, name)->get(rowFilter);
}

ConstantSP AbstractTable::getColumn(INDEX index, const ConstantSP& rowFilter) const{
	if(!rowFilter.isNull())
		return getColumn(index)->get(rowFilter);
	else
		return getColumn(index);
}

ConstantSP AbstractTable::get(INDEX col, INDEX row) const {
	return getColumn(col)->get(row);
}

void AbstractTable::setColumnName(int index, const string& name){
	std::ignore = index;
	std::ignore = name;
	throw TableRuntimeException(getTableTypeName() + " can't rename columns.");
}

int AbstractTable::getColumnIndex(const string& name) const {
	std::unordered_map<string,int>::const_iterator it = colMap_->find(Util::lower(name));
	if(it == colMap_->end())
		return -1;
	else
		return it->second;
}

bool AbstractTable::contain(const string& name) const {
	return colMap_->find(Util::lower(name))!=colMap_->end();
}

bool AbstractTable::contain(const string& qualifier, const string& name) const {
	return colMap_->find(Util::lower(name))!=colMap_->end() && Util::equalIgnoreCase(qualifier, name_);
}

ConstantSP AbstractTable::getColumnLabel() const {
	VectorSP colNames = Util::createVector(DT_STRING, static_cast<INDEX>(colNames_->size()));
	for(unsigned int i=0; i<colNames_->size(); ++i)
		colNames->setString(i, colNames_->at(i));
	return ConstantSP(colNames);
}

ConstantSP AbstractTable::values() const {
	int sz = columns();
	ConstantSP result = Util::createVector(DT_ANY, sz);
	for(int i=0; i<sz; ++i)
		result->set(i, getColumn(i));
	return result;
}


string AbstractTable::getString(INDEX index) const {
	string str;
	string tmp;
	int remains;
	int cols=columns();

	for(int i=0;i<cols;i++){
		tmp=get(i,index)->getString();
		if((int)(str.size()+tmp.size())<Util::DISPLAY_WIDTH){
			str.append(1,' ');
			str.append(tmp);
		}
		else{
			remains= static_cast<int>(Util::DISPLAY_WIDTH-1-str.size());
			if(remains>0){
				str.append(1,' ');
				str.append(tmp.substr(0,remains));
			}
			str.append("...");
			break;
		}
	}
	return str;
}

string AbstractTable::getString() const {
	int rowNum=(std::min)(Util::DISPLAY_ROWS,size());
    int strColMaxWidth=Util::DISPLAY_WIDTH/(std::min)(columns(),Util::DISPLAY_COLS)+5;
    int length=0;
    int curCol=0;
    int maxColWidth;
	vector<string> list(rowNum+1);
	vector<string> listTmp(rowNum+1);
	string separator;
    int i,curSize;

    while(length<Util::DISPLAY_WIDTH && curCol<columns()){
    	listTmp[0]=getColumnName(curCol);
    	maxColWidth=0;
    	for(i=0;i<rowNum;i++){
    		listTmp[i+1]=get(curCol,i)->getString();
    		if((int)listTmp[i+1].size()>maxColWidth)
    			maxColWidth=static_cast<int>(listTmp[i+1].size());
    	}
    	if(maxColWidth>strColMaxWidth)
    		maxColWidth=strColMaxWidth;
    	if((int)listTmp[0].size()>maxColWidth)
    		maxColWidth=(std::min)(strColMaxWidth,(int)listTmp[0].size());

    	if(length+maxColWidth>Util::DISPLAY_WIDTH && curCol+1<columns())
    		break;

    	separator.append(maxColWidth, '-');
    	if(curCol<columns()-1){
    		maxColWidth++;
    		separator.append(1, ' ');
    	}

    	for(i=0;i<=rowNum;i++){
    		curSize=static_cast<int>(listTmp[i].size());
    		if(curSize<=maxColWidth){
    			list[i].append(listTmp[i]);
    			if(curSize<maxColWidth)
    				list[i].append(maxColWidth-curSize,' ');
    		}
    		else{
    			if(maxColWidth>3)
    				list[i].append(listTmp[i].substr(0,maxColWidth-3));
    			list[i].append("...");
    		}
    	}
    	length+=maxColWidth;
    	curCol++;
    }

    if(curCol<columns()){
    	for(i=0;i<=rowNum;i++)
    		list[i].append("...");
    	separator.append("---");
    }

    string resultStr(list[0]);
    resultStr.append("\n");
    resultStr.append(separator);
    resultStr.append("\n");
    for(i=1;i<=rowNum;i++){
    	resultStr.append(list[i]);
    	resultStr.append("\n");
    }
    if(rowNum<size())
    	resultStr.append("...\n");
    return resultStr;
}

COMPRESS_METHOD AbstractTable::getColumnCompressMethod(INDEX index) {
	if (index < (INDEX)colCompresses_.size())
		return colCompresses_[index];
	else
		return COMPRESS_NONE;
}

void AbstractTable::setColumnCompressMethods(const vector<COMPRESS_METHOD> &colCompresses) {
	if (colCompresses.size() > 0 && colCompresses.size() != colNames_->size()) {
		throw RuntimeException("The number of elements in parameter compressMethods does not match the column size "+std::to_string(colNames_->size())+".");
	}
	for (INDEX i = 0; i < static_cast<INDEX>(colCompresses.size()); i++) {
		if (colCompresses[i] == COMPRESS_DELTA) {
			DATA_TYPE dataType = getColumn(i)->getRawType();
			if (dataType != DT_SHORT && dataType != DT_INT && dataType != DT_LONG && dataType != DT_DECIMAL32 && dataType != DT_DECIMAL64) {
				throw RuntimeException("Cannot apply compression method DELTA to column "+colNames_->at(i)+", Only integral and temporal and Decimal32/Decimal64 data supports DELTA compression");
			}
			if (((Vector*)getColumn(i).get())->getVectorType() == VECTOR_TYPE::ARRAYVECTOR) {
				throw RuntimeException("Cannot apply compression method DELTA to array vector at column "+colNames_->at(i));
			}
		}
		else if (colCompresses[i] == COMPRESS_LZ4) {
		}
		else {
			throw RuntimeException("Unsupported compression method at column "+colNames_->at(i));
		}
	}
	colCompresses_ = colCompresses;
}

ConstantSP AbstractTable::getInternal(INDEX index) const {
	Dictionary* dict=Util::createDictionary(DT_STRING,DT_ANY);
	ConstantSP resultSP(dict);

	int numCol=columns();
	for(int i=0;i<numCol;++i)
		dict->set(colNames_->at(i),getColumn(i)->get(index));
	return resultSP;
}

bool AbstractTable::set(INDEX index, const ConstantSP& value) {
	std::ignore = index;
	std::ignore = value;
	throw TableRuntimeException(getTableClassName() + " does not support direct data update.");
}

ConstantSP AbstractTable::getInternal(const ConstantSP& index) const {
	if(index->getCategory()==LITERAL)
		return getMember(index);
	else if(index->isScalar()){
		Dictionary* dict=Util::createDictionary(DT_STRING,DT_ANY);
		ConstantSP resultSP(dict);

		int numCol=columns();
		for(int i=0;i<numCol;++i)
			dict->set(colNames_->at(i),getColumn(i)->get(index));
		return resultSP;
	}

	if(index->isPair()){
		UINDEX start = index->isNull(0) ? 0 : (UINDEX)index->getIndex(0);
		UINDEX end = index->isNull(1) ? size() : (UINDEX)index->getIndex(1);
		INDEX length = end-start;
		if(start > end)
			--start;
		return getWindow((INDEX)0,columns(),(INDEX)start,(int)length);
	}
	else{
		vector<ConstantSP> newCols;
		int columnNum = static_cast<int>(colNames_->size());
		for(int i=0;i<columnNum;i++){
			newCols.push_back(getColumn(i)->get(index));
		}

		return new BasicTable(newCols, *colNames_);
	}
}

ConstantSP AbstractTable::getWindowInternal(int columnStart, int columnLength,int rowStart, int rowLength) const {
	vector<ConstantSP> newCols;
	int sign=1;
	if(columnLength<0){
		sign=-1;
		columnLength=-columnLength;
	}

	if(rowStart==0 && rowLength==size()){
		for(int i=0;i<columnLength;i++)
			newCols.push_back(getColumn(columnStart+sign*i)->getValue());
	}
	else{
		for(int i=0;i<columnLength;i++)
			newCols.push_back(((Vector*)getColumn(columnStart+sign*i).get())->getSubVector(rowStart,rowLength));
	}

	if(columnStart==0 && columnLength==columns())
		return new BasicTable(newCols, *colNames_);
	else{
		vector<string> names;
		for(int i=0;i<columnLength;i++)
			names.push_back(colNames_->at(columnStart+sign*i));
		return new BasicTable(newCols, names);
	}
}

ConstantSP AbstractTable::getMemberInternal(const ConstantSP& key) const{
	if(key->isScalar())
		return getColumn(key->getString(0));
	else {
		int sz = key->size();
		ConstantSP result = Util::createVector(DT_ANY, sz);
		for(int i=0; i<sz; ++i)
			result->set(i, getColumn(key->getString(i)));
		return result;
	}
}

ConstantSP AbstractTable::getInstance(int sz) const{
	std::ignore = sz;
	throw TableRuntimeException(getTableTypeName() + " can't be copied.");
}

ConstantSP AbstractTable::getValue() const {
	throw TableRuntimeException(getTableTypeName() + " can't be copied.");
}

ConstantSP AbstractTable::getValue(INDEX capacity) const{
	std::ignore = capacity;
	throw TableRuntimeException(getTableTypeName() + " can't be copied.");
}

bool AbstractTable::append(vector<ConstantSP>& value, INDEX& insertedRows, string& errMsg){
	std::ignore = value;
	std::ignore = insertedRows;
	errMsg = getTableTypeName() + " doesn't support data append.";
	return false;
}

bool AbstractTable::update(vector<ConstantSP>& value, const ConstantSP& indexSP, vector<string>& colNames, string& errMsg){
	std::ignore = value;
	std::ignore = indexSP;
	std::ignore = colNames;
	errMsg = getTableTypeName() + " doesn't support data update.";
	return false;
}

bool AbstractTable::remove(const ConstantSP& indexSP, string& errMsg) {
	std::ignore = indexSP;
	errMsg = getTableTypeName() + " doesn't support data deletion.";
	return false;
}

BasicTable::BasicTable(const vector<ConstantSP>& cols, const vector<string>& colNames) : AbstractTable(new vector<string>(colNames)){
	initData(cols, colNames);
}

BasicTable::BasicTable(const vector<ConstantSP>& cols, const vector<string>& colNames, const vector<int>& key) : AbstractTable(new vector<string>(colNames)){
	std::ignore = key;
	initData(cols, colNames);
}

void BasicTable::initData(const vector<ConstantSP>& cols, const vector<string>& colNames){
	std::size_t len = cols.size();
	if(len==0)
		throw TableRuntimeException("A table has at least one column.");
	if(len != colNames.size())
		throw TableRuntimeException("Number of column names must be the same as number of column vectors.");

	int rowNum= -1;
	for(std::size_t i=0;i<len;i++){
		if(cols[i].isNull())
			throw TableRuntimeException("Column vector cannot be null.");
		if(cols[i]->isScalar())
			continue;
		if(rowNum < 0)
			rowNum=cols[i]->size();
		else if(rowNum!=cols[i]->size())
			throw TableRuntimeException("All columns must have the same size");
	}
	if(rowNum < 0)
		rowNum = 1;

	capacity_ = INDEX_MAX;
	for(std::size_t i=0;i<len;i++){
		if(!cols[i]->isArray()){
			Vector* tmp=Util::createVector(cols[i]->getType(),rowNum,0,true,cols[i]->getExtraParamForType());
			tmp->fill(0,rowNum,cols[i]);
			cols_.push_back(ConstantSP(tmp));
		}
		else{
			Vector* cur=(Vector*)cols[i].get();
			if(cur->isTemporary())
				cols_.push_back(cols[i]);
			else
				cols_.push_back(cur->getValue(cur->getCapacity()));
		}
		cols_[i]->setTemporary(false);
		cols_[i]->setIndependent(false);
		((Vector*)cols_[i].get())->setName(colNames[i]);

		INDEX curCapacity  = ((Vector*)cols_[i].get())->getCapacity();
		if(curCapacity < capacity_)
			capacity_ = curCapacity;
	}
	size_=rowNum;
}

BasicTable::~BasicTable(){

}

void BasicTable::setColumnName(int index, const string& name){
	string oldName = colNames_->at(index);
	(*colNames_)[index]=name;
	colMap_->erase(Util::lower(oldName));
	(*colMap_)[Util::lower(name)] = index;
}

ConstantSP BasicTable::getColumn(INDEX index) const{
	return cols_[index];
}

bool BasicTable::set(INDEX index, const ConstantSP& value){
	if(!value->isDictionary() || ((Dictionary*)value.get())->getKeyCategory()!=LITERAL)
		return false;

	int numCol=columns();
	for(int i=0;i<numCol;++i)
		cols_[i]->set(index,((Dictionary*)value.get())->getMember(colNames_->at(i)));
	return true;
}

ConstantSP BasicTable::get(INDEX index) const {
	return getInternal(index);
}

ConstantSP BasicTable::get(const ConstantSP& index) const {
	return getInternal(index);
}

ConstantSP BasicTable::getWindow(INDEX colStart, int colLength, INDEX rowStart, int rowLength) const {
	return getWindowInternal(colStart, colLength, rowStart, rowLength);
}

ConstantSP BasicTable::getMember(const ConstantSP& key) const {
	return getMemberInternal(key);
}

ConstantSP BasicTable::getValue() const {
	ConstantSP copy = ConstantSP(new BasicTable(cols_,*colNames_));
	((Table*)copy.get())->setName(name_);
	return copy;
}

ConstantSP BasicTable::getValue(INDEX capacity) const {
	vector<ConstantSP> newCols;
	if(capacity == 0)
		capacity = 1;
	for(unsigned int i=0;i<cols_.size();i++)
		newCols.push_back(((Vector*)cols_[i].get())->getValue(capacity));
	ConstantSP copy = ConstantSP(new BasicTable(newCols,*colNames_));
	((Table*)copy.get())->setName(name_);
	return copy;
}

ConstantSP BasicTable::getInstance(int sz) const {
	vector<ConstantSP> newCols;
	for(unsigned int i=0;i<cols_.size();i++)
		newCols.push_back(((Vector*)cols_[i].get())->getInstance(sz));
	ConstantSP copy = ConstantSP(new BasicTable(newCols,*colNames_));
	((Table*)copy.get())->setName(name_);
	return copy;
}

bool BasicTable::append(vector<ConstantSP>& valueVec, INDEX& insertedRows, string& errMsg){
	if(isReadOnly()){
		errMsg = "Can't modify read only table.";
		return false;
	}

	int num = static_cast<int>(valueVec.size());
	INDEX rowNum;
	if(num==1 && valueVec[0]->isTable()){
		Table* tbl=(Table*)valueVec[0].get();
		num=tbl->columns();
		if(num!=(int)cols_.size()){
			errMsg = "Number of columns for the original table and the table to insert are different.";
			return false;
		}

		rowNum = tbl->rows();
		if(size_ + rowNum > capacity_ && !increaseCapacity(size_ + rowNum, errMsg))
			return false;
		int i=0;
		try{
			for(;i<num;++i){
				if(!((Vector*)cols_[i].get())->append(tbl->getColumn(i))){
					errMsg = "data type " + Util::getDataTypeString(tbl->getColumn(i)->getType()) + ", expect "+
							Util::getDataTypeString(cols_[i]->getType());
					break;
				}
			}
			if(i >= num){
				insertedRows = rowNum;
				size_ += rowNum;
				return true;
			}
			else{
				for(int k=0; k<i; ++k)
					((Vector*)cols_[k].get())->remove(rowNum);
				errMsg = "Failed to append data to column '" + getColumnName(i) +"' reason: " + errMsg;
				return false;
			}
		}
		catch(std::exception& ex){
			for(int k=0; k<i; ++k)
				((Vector*)cols_[k].get())->remove(rowNum);
			errMsg = "Failed to append data to column '" + getColumnName(i) +"' with error: " + ex.what();
			return false;
		}
	}
	if(num==1 && valueVec[0]->isTuple()){
		AnyVector* tbl=(AnyVector*)valueVec[0].get();
		num=tbl->rows();
		if(num!=(int)cols_.size()){
			errMsg = "Number of columns for the original table and the table to insert are different.";
			return false;
		}

		rowNum = tbl->get(0)->rows();
		if(size_ + rowNum > capacity_ && !increaseCapacity(size_ + rowNum, errMsg))
			return false;
		int i=0;
		try{
			for(;i<num;++i){
				ConstantSP col = tbl->get(i);
				if(col->size() != rowNum || !((Vector*)cols_[i].get())->append(col)){
					errMsg = "data type " + Util::getDataTypeString(col->getType()) + ", expect "+
							Util::getDataTypeString(cols_[i]->getType());
					break;
				}
			}
			if(i >= num){
				insertedRows = rowNum;
				size_ += rowNum;
				return true;
			}
			else{
				for(int k=0; k<i; ++k)
					((Vector*)cols_[k].get())->remove(rowNum);
				errMsg = "Failed to append data to column '" + getColumnName(i) +"' reason: " + errMsg;
				return false;
			}
		}
		catch(std::exception& ex){
			for(int k=0; k<i; ++k)
				((Vector*)cols_[k].get())->remove(rowNum);
			errMsg = "Failed to append data to column '" + getColumnName(i) +"' with error: " + ex.what();
			return false;
		}
	}
	else{
		if(num!=(int)cols_.size()){
			errMsg = "The number of table columns doesn't match the number of columns to append.";
			return false;
		}
		rowNum=valueVec[0]->size();
		for(int i=1;i<num;++i){
			if(valueVec[i]->size()!=rowNum ){
				errMsg = "Inconsistent length of values to insert.";
				return false;
			}
		}
		if(size_ + rowNum > capacity_ && !increaseCapacity(size_ + rowNum, errMsg))
			return false;

		int i=0;
		try{
			for(;i<num;i++){
				if(!((Vector*)cols_[i].get())->append(valueVec[i])){
					errMsg = "data type " + Util::getDataTypeString(valueVec[i]->getType()) + ", expect "+
							Util::getDataTypeString(cols_[i]->getType());
					break;
				}
			}
			if(i >= num){
				insertedRows = rowNum;
				size_+=rowNum;
				return true;
			}
			else{
				for(int k=0; k<i; ++k){
					((Vector*)cols_[k].get())->remove(rowNum);
				}
				errMsg = "Failed to append data to column '" + getColumnName(i) +"' reason: " + errMsg;
				return false;
			}
		}
		catch(std::exception& ex){
			for(int k=0; k<i; ++k)
				((Vector*)cols_[k].get())->remove(rowNum);
			errMsg = "Failed to append data to column '" + getColumnName(i) +"' with error: " + ex.what();
			return false;
		}
	}
}
/*
bool BasicTable::internalAppend(vector<ConstantSP>& values, string& errMsg){
	int rows = values[0]->size();
	if(size_ + rows > capacity_ && !increaseCapacity(size_ + rows, errMsg))
		return false;

	int i=0;
	try{
		string msg;
		int cols = values.size();
		for(; i<cols; i++){
			if(!((Vector*)cols_[i].get())->append(values[i])){
				msg = "data type " + Util::getDataTypeString(values[i]->getType()) + ", expect "+
						Util::getDataTypeString(cols_[i]->getType());
				break;
			}
		}
		if(i >= cols){
			size_+=rows;
			return true;
		}
		else{
			for(int k=0; k<i; ++k){
				((Vector*)cols_[k].get())->remove(rows);
			}
			errMsg = "Failed to append data to column '" + getColumnName(i) +"' reason: " + msg;
			return false;
		}
	}
	catch(exception& ex){
		for(int k=0; k<i; ++k)
			((Vector*)cols_[k].get())->remove(rows);
		errMsg = "Failed to append data to column '" + getColumnName(i) +"' with error: " + ex.what();
		return false;
	}
}
*/
bool BasicTable::update(vector<ConstantSP>& valueVec, const ConstantSP& indexSP, vector<string>& colNames, string& errMsg){
	if(isReadOnly()){
		errMsg = "Can't modify read only table.";
		return false;
	}
	return internalUpdate(valueVec, indexSP, colNames, errMsg);
}

bool BasicTable::internalUpdate(vector<ConstantSP>& valueVec, const ConstantSP& indexSP, vector<string>& colNames, string& errMsg){
	std::size_t num=valueVec.size();
	vector<int> colIndex(num);
	vector<std::pair<string, ConstantSP>> newCols;
	std::unordered_map<string,int>::iterator it;

	INDEX rowNum= indexSP->isNothing() ? size() : indexSP->size();
	bool allowNewCol = rowNum == size_;
	for(std::size_t i=0;i<num;i++){
		it=colMap_->find(Util::lower(colNames[i]));
		if(it==colMap_->end()){
			colIndex[i] = -1;
			if(!allowNewCol || (!valueVec[i]->isScalar() && valueVec[i]->size() != rowNum)){
				errMsg.append("The column "+ colNames[i]+ " does not exist. To add a new column, the table shouldn't be shared and the value size must match the table.");
				return false;
			}
			else{
				ConstantSP value = valueVec[i];
				if(value->isScalar()){
					VectorSP vec = Util::createVector(value->getType(), rowNum);
					vec->fill(0, rowNum, value);
					value = vec;
				}
				else if(!value->isTemporary())
					value = value->getValue();
				newCols.push_back(std::pair<string, ConstantSP>(colNames[i], value));
			}
		}
		else{
			colIndex[i]=it->second;

			if(cols_[colIndex[i]]->getCategory()!=valueVec[i]->getCategory() && (!valueVec[i]->isNumber() || !cols_[colIndex[i]]->isNumber()) && valueVec[i]->getCategory()!=NOTHING){
				errMsg.append("The category of the value to update does not match the column ");
				errMsg.append(colNames_->at(colIndex[i]));
				return false;
			}
			int curSize=valueVec[i]->size();
			if(curSize!=1 && rowNum!=curSize){
				errMsg.append("Inconsistent length of values to update.");
				return false;
			}
		}
	}

	for(std::size_t i=0;i<num;i++){
		if(colIndex[i] < 0)
			continue;
		if(!indexSP->isNothing())
			cols_[colIndex[i]]->set(indexSP,valueVec[i]);
		else if(valueVec[i]->isScalar())
			((Vector*)cols_[colIndex[i]].get())->fill(0,rowNum,valueVec[i]);
		else
			cols_[colIndex[i]]->assign(valueVec[i]);
	}
	for(std::size_t i=0; i<newCols.size(); ++i){
		colNames_->push_back(newCols[i].first);
		colMap_->insert(std::pair<string, int>(Util::lower(newCols[i].first), static_cast<int>(colMap_->size())));
		cols_.push_back(newCols[i].second);
		cols_.back()->setTemporary(false);
	}
	return true;
}


bool BasicTable::remove(const ConstantSP& indexSP, string& errMsg){
	if(isReadOnly()){
		errMsg = "Can't remove rows from a read only in-memory table.";
		return false;
	}
	return internalRemove(indexSP, errMsg);
}

bool BasicTable::internalRemove(const ConstantSP& indexSP, string& errMsg){
	std::ignore = errMsg;
	bool noIndex = indexSP.isNull() || indexSP->isNothing();
	std::size_t colCount = cols_.size();

	for(std::size_t i=0; i<colCount; ++i){
		VectorSP curCol(cols_[i]);
		if (noIndex)
			curCol->clear();
		else
			if (!curCol->remove(indexSP)) {
				//FIXME If we need restor vector?
				throw RuntimeException("Invalid index array.");
			}
	}
	size_ = cols_[0]->size();
	return true;
}

void BasicTable::drop(vector<int>& columnNum){
	if(isReadOnly())
		throw RuntimeException("Can't drop columns of a read only in-memory table.");
	internalDrop(columnNum);
}

void BasicTable::internalDrop(vector<int>& columnNum){
	std::unordered_set<int> dropColumns;
	dropColumns.insert(columnNum.begin(), columnNum.end());

	vector<ConstantSP> newCols;
	SmartPointer<vector<string>> newColNames = new vector<string>();
	SmartPointer<std::unordered_map<string,int>> newColMap = new std::unordered_map<string,int>();
	vector<COMPRESS_METHOD> newColCompresses;
	int numCol = static_cast<int>(colNames_->size());
	for(int i=0; i<numCol; ++i){
		if(dropColumns.find(i) != dropColumns.end())
			continue;
		newCols.push_back(cols_[i]);
		newColNames->push_back(colNames_->at(i));
		if(!colCompresses_.empty())
			newColCompresses.push_back(colCompresses_.at(i));
		newColMap->insert(std::pair<string,int>(Util::lower(colNames_->at(i)), static_cast<int>(newCols.size()-1)));
	}
	cols_ = newCols;
	colNames_ = newColNames;
	colMap_ = newColMap;
	colCompresses_ = newColCompresses;
}

bool BasicTable::join(vector<ConstantSP>& columnNum){
	if(isReadOnly())
		return false;

	std::size_t num = columnNum.size();
	for(std::size_t i=0; i<num; ++i){
		ConstantSP& col = columnNum[i];
		string name = ((Vector*)col.get())->getName();
		if(!col->isArray() || col->size()!= size_ || name.empty() || colMap_->find(Util::lower(name)) != colMap_->end())
			return false;
	}
	for(std::size_t i=0; i<num; ++i){
		ConstantSP& col = columnNum[i];
		col->setTemporary(false);
		string name = ((Vector*)col.get())->getName();
		cols_.push_back(col);
		colNames_->push_back(name);
		colMap_->insert(std::pair<string,int>(Util::lower(name), static_cast<int>(cols_.size() - 1)));
	}
	return true;
}

bool BasicTable::clear(){
	if(isReadOnly())
		return false;
	int num = columns();
	for(int i=0; i<num; ++i)
		VectorSP(cols_[i])->clear();
	size_ = 0;
	return true;
}

long long BasicTable::getAllocatedMemory() const {
	long long sz=sizeof(BasicTable)+sizeof(string)*colNames_->capacity();
	sz+=sizeof(ConstantSP)*cols_.capacity();
	for(unsigned int i=0;i<cols_.size();++i)
		sz+=cols_[i]->getAllocatedMemory();
	return sz;
}

void BasicTable::updateSize() {
	if(isReadOnly())
		return;

	/* All columns are updated outside the table. So we need to update the size internally. */
	int newSize = cols_[0]->size();
	for(unsigned int i=1; i<cols_.size(); ++i){
		if(cols_[i]->size() != newSize)
			throw TableRuntimeException("The length of all columns are inconsistent in table " + name_);
	}
	size_ = newSize;
}

bool BasicTable::increaseCapacity(long long newCapacity, string& errMsg){
	std::size_t colCount = cols_.size();
	INDEX finalCapacity = INDEX_MAX;

	try{
		if(newCapacity < 0 || newCapacity > INT_MAX){
			errMsg = "An in-memory table can't exceed 2 billion rows.";
			return false;
		}

		for(std::size_t i=0; i<colCount; ++i){
			Vector* vec = (Vector*)cols_[i].get();
			if(newCapacity > vec->getCapacity()){
				INDEX capacity = (std::min)(INDEX_MAX, static_cast<int>(newCapacity * 1.2));
				if(vec->isFastMode()){
					cols_[i] = vec->getValue(capacity);
					cols_[i]->setTemporary(false);
				}
				else
					((Vector*)cols_[i].get())->reserve(capacity);
				vec = (Vector*)cols_[i].get();
			}
			finalCapacity = (std::min)(finalCapacity, vec->getCapacity());
		}
		capacity_ = finalCapacity;
		return true;
	}
	catch(std::exception& ex){
		errMsg = string(ex.what());
		return false;
	}
	catch(...){
		errMsg.append("Unknown exception in BasicTable::increaseCapacity");
		return false;
	}
}

ConstantSP BasicTable::getSubTable(vector<int> indices) const{
	std::size_t colCount = cols_.size();
	vector<ConstantSP> cols(colCount);
	for(std::size_t i = 0; i < colCount; i++){
		cols[i] = Util::createSubVector(cols_[i], indices);
	}
	return new BasicTable(cols, *colNames_.get());
}
}
