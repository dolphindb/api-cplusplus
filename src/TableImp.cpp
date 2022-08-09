/*
 * Table.cpp
 *
 *  Created on: Nov 3, 2012
 *      Author: dzhou
 */

#include <algorithm>

#include "TableImp.h"
#include "Util.h"
#include "ConstantImp.h"
#include "ConstantMarshall.h"

namespace dolphindb {

AbstractTable::AbstractTable(const SmartPointer<vector<string>>& colNames) : colNames_(colNames){
	colMap_ = new unordered_map<string, int>();
	for(unsigned int i=0; i<colNames->size(); ++i){
		colMap_->insert(pair<string, int>(Util::lower(colNames->at(i)),i));
	}
}

AbstractTable::AbstractTable(const SmartPointer<vector<string>>& colNames, SmartPointer<unordered_map<string,int>> colMap) : colNames_(colNames), colMap_(colMap){

}

string AbstractTable::getTableClassName() const {
	switch(getTableType()){
	case BASICTBL:
		return "BasicTable";
	case REALTIMETBL:
		return "RealtimeTable";
	case SNAPTBL:
		return "SnapshotTable";
	case JOINTBL:
		return "JoinTable";
	case FILETBL:
		return "FileBackedTable";
	case SEGTBL:
		return "SegmentedTable";
	case COMPRESSTBL:
		return "CompressedTable";
	case LOGROWTBL:
		return "LogRowTable";
	default:
		return "";
	}
}

string AbstractTable::getTableTypeName() const {
	switch(getTableType()){
	case BASICTBL:
		return "A basic table";
	case REALTIMETBL:
		return "A realtime table";
	case SNAPTBL:
		return "A snapshot table";
	case JOINTBL:
		return "A join table";
	case FILETBL:
		return "A file backed table";
	case SEGTBL:
		return "A segmented table";
	case COMPRESSTBL:
		return "A compressed table";
	case LOGROWTBL:
		return "A log table";
	default:
		return "";
	}
}

ConstantSP AbstractTable::getColumn(const string& name) const{
	unordered_map<string,int>::const_iterator it=colMap_->find(Util::lower(name));
	if(it==colMap_->end()){
		throw TableRuntimeException("Unrecognized column name " + name);
	}
	return getColumn(it->second);
}

ConstantSP AbstractTable::getColumn(const string& qualifier, const string& name) const{
	unordered_map<string,int>::const_iterator it=colMap_->find(Util::lower(name));
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
	throw TableRuntimeException(getTableTypeName() + " can't rename columns.");
}

int AbstractTable::getColumnIndex(const string& name) const {
	unordered_map<string,int>::const_iterator it = colMap_->find(Util::lower(name));
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
	VectorSP colNames = Util::createVector(DT_STRING, colNames_->size());
	for(unsigned int i=0; i<colNames_->size(); ++i)
		colNames->setString(i, colNames_->at(i));
	return ConstantSP(colNames);
}

ConstantSP AbstractTable::values() const {
	int size = columns();
	ConstantSP result = Util::createVector(DT_ANY, size);
	for(int i=0; i<size; ++i)
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
			remains=Util::DISPLAY_WIDTH-1-str.size();
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
	int rows=(std::min)(Util::DISPLAY_ROWS,size());
    int strColMaxWidth=Util::DISPLAY_WIDTH/(std::min)(columns(),Util::DISPLAY_COLS)+5;
    int length=0;
    int curCol=0;
    int maxColWidth;
	vector<string> list(rows+1);
	vector<string> listTmp(rows+1);
	string separator;
    int i,curSize;

    while(length<Util::DISPLAY_WIDTH && curCol<columns()){
    	listTmp[0]=getColumnName(curCol);
    	maxColWidth=0;
    	for(i=0;i<rows;i++){
    		listTmp[i+1]=get(curCol,i)->getString();
    		if((int)listTmp[i+1].size()>maxColWidth)
    			maxColWidth=listTmp[i+1].size();
    	}
    	if(maxColWidth>strColMaxWidth && getColumn(curCol)->getCategory()==LITERAL)
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

    	for(i=0;i<=rows;i++){
    		curSize=listTmp[i].size();
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
    	for(i=0;i<=rows;i++)
    		list[i].append("...");
    	separator.append("---");
    }

    string resultStr(list[0]);
    resultStr.append("\n");
    resultStr.append(separator);
    resultStr.append("\n");
    for(i=1;i<=rows;i++){
    	resultStr.append(list[i]);
    	resultStr.append("\n");
    }
    if(rows<size())
    	resultStr.append("...\n");
    return resultStr;
}

COMPRESS_METHOD AbstractTable::getColumnCompressMethod(INDEX index) {
	if (index < colCompresses_.size())
		return colCompresses_[index];
	else
		return COMPRESS_NONE;
}

void AbstractTable::setColumnCompressMethods(const vector<COMPRESS_METHOD> &colCompresses) {
	if (colCompresses.size() > 0 && colCompresses.size() != colNames_->size()) {
		throw RuntimeException("The number of elements in parameter compressMethods does not match the column size "+std::to_string(colNames_->size())+".");
	}
	for (size_t i = 0; i < colCompresses.size(); i++) {
		if (colCompresses[i] == COMPRESS_DELTA) {
			DATA_TYPE dataType = getColumn(i)->getRawType();
			if (dataType != DT_SHORT && dataType != DT_INT && dataType != DT_LONG) {
				throw RuntimeException("Cannot apply compression method DELTA to column "+colNames_->at(i)+", Only SHORT/INT/LONG or temporal data supports DELTA compression");
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
		int columns = colNames_->size();
		for(int i=0;i<columns;i++){
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
		int size = key->size();
		ConstantSP result = Util::createVector(DT_ANY, size);
		for(int i=0; i<size; ++i)
			result->set(i, getColumn(key->getString(i)));
		return result;
	}
}

ConstantSP AbstractTable::getInstance(int size) const{
	throw TableRuntimeException(getTableTypeName() + " can't be copied.");
}

ConstantSP AbstractTable::getValue() const {
	throw TableRuntimeException(getTableTypeName() + " can't be copied.");
}

ConstantSP AbstractTable::getValue(INDEX capacity) const{
	throw TableRuntimeException(getTableTypeName() + " can't be copied.");
}

bool AbstractTable::append(vector<ConstantSP>& values, INDEX& insertedRows, string& errMsg){
	errMsg = getTableTypeName() + " doesn't support data append.";
	return false;
}

bool AbstractTable::update(vector<ConstantSP>& values, const ConstantSP& indexSP, vector<string>& colNames, string& errMsg){
	errMsg = getTableTypeName() + " doesn't support data update.";
	return false;
}

bool AbstractTable::remove(const ConstantSP& indexSP, string& errMsg) {
	errMsg = getTableTypeName() + " doesn't support data deletion.";
	return false;
}

BasicTable::BasicTable(const vector<ConstantSP>& cols, const vector<string>& colNames) : AbstractTable(new vector<string>(colNames)){
	initData(cols, colNames);
}

BasicTable::BasicTable(const vector<ConstantSP>& cols, const vector<string>& colNames, const vector<int>& keys) : AbstractTable(new vector<string>(colNames)){
	initData(cols, colNames);
}

void BasicTable::initData(const vector<ConstantSP>& cols, const vector<string>& colNames){
	int len=cols.size();
	if(len==0)
		throw TableRuntimeException("A table has at least one column.");
	if(len != (int)colNames.size())
		throw TableRuntimeException("Number of column names must be the same as number of column vectors.");

	int rows= -1;
	for(int i=0;i<len;i++){
		if(cols[i].isNull())
			throw TableRuntimeException("Column vector cannot be null.");
		if(cols[i]->isScalar())
			continue;
		if(rows < 0)
			rows=cols[i]->size();
		else if(rows!=cols[i]->size())
			throw TableRuntimeException("All columns must have the same size");
	}
	if(rows < 0)
		rows = 1;

	capacity_ = INDEX_MAX;
	for(int i=0;i<len;i++){
		if(!cols[i]->isArray()){
			Vector* tmp=Util::createVector(cols[i]->getType(),rows);
			tmp->fill(0,rows,cols[i]);
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
	size_=rows;
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

ConstantSP BasicTable::getInstance(int size) const {
	vector<ConstantSP> newCols;
	for(unsigned int i=0;i<cols_.size();i++)
		newCols.push_back(((Vector*)cols_[i].get())->getInstance(size));
	ConstantSP copy = ConstantSP(new BasicTable(newCols,*colNames_));
	((Table*)copy.get())->setName(name_);
	return copy;
}

bool BasicTable::append(vector<ConstantSP>& values, INDEX& insertedRows, string& errMsg){
	if(isReadOnly()){
		errMsg = "Can't modify read only table.";
		return false;
	}

	int num=values.size();
	long long rows;
	if(num==1 && values[0]->isTable()){
		Table* tbl=(Table*)values[0].get();
		num=tbl->columns();
		if(num!=(int)cols_.size()){
			errMsg = "Number of columns for the original table and the table to insert are different.";
			return false;
		}

		rows = tbl->rows();
		if(size_ + rows > capacity_ && !increaseCapacity(size_ + rows, errMsg))
			return false;
		int i=0;
		try{
			string msg;
			for(;i<num;++i){
				if(!((Vector*)cols_[i].get())->append(tbl->getColumn(i))){
					msg = "data type " + Util::getDataTypeString(tbl->getColumn(i)->getType()) + ", expect "+
							Util::getDataTypeString(cols_[i]->getType());
					break;
				}
			}
			if(i >= num){
				insertedRows = rows;
				size_ += rows;
				return true;
			}
			else{
				for(int k=0; k<i; ++k)
					((Vector*)cols_[k].get())->remove(rows);
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
	if(num==1 && values[0]->isTuple()){
		AnyVector* tbl=(AnyVector*)values[0].get();
		num=tbl->rows();
		if(num!=(int)cols_.size()){
			errMsg = "Number of columns for the original table and the table to insert are different.";
			return false;
		}

		rows = tbl->get(0)->rows();
		if(size_ + rows > capacity_ && !increaseCapacity(size_ + rows, errMsg))
			return false;
		int i=0;
		try{
			string msg;
			for(;i<num;++i){
				ConstantSP col = tbl->get(i);
				if(col->size() != rows || !((Vector*)cols_[i].get())->append(col)){
					msg = "data type " + Util::getDataTypeString(col->getType()) + ", expect "+
							Util::getDataTypeString(cols_[i]->getType());
					break;
				}
			}
			if(i >= num){
				insertedRows = rows;
				size_ += rows;
				return true;
			}
			else{
				for(int k=0; k<i; ++k)
					((Vector*)cols_[k].get())->remove(rows);
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
	else{
		if(num!=(int)cols_.size()){
			errMsg = "The number of table columns doesn't match the number of columns to append.";
			return false;
		}
		rows=values[0]->size();
		for(int i=1;i<num;++i){
			if(values[i]->size()!=rows ){
				errMsg = "Inconsistent length of values to insert.";
				return false;
			}
		}
		if(size_ + rows > capacity_ && !increaseCapacity(size_ + rows, errMsg))
			return false;

		int i=0;
		try{
			string msg;
			for(;i<num;i++){
				if(!((Vector*)cols_[i].get())->append(values[i])){
					msg = "data type " + Util::getDataTypeString(values[i]->getType()) + ", expect "+
							Util::getDataTypeString(cols_[i]->getType());
					break;
				}
			}
			if(i >= num){
				insertedRows = rows;
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
}

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

bool BasicTable::update(vector<ConstantSP>& values, const ConstantSP& indexSP, vector<string>& colNames, string& errMsg){
	if(isReadOnly()){
		errMsg = "Can't modify read only table.";
		return false;
	}
	return internalUpdate(values, indexSP, colNames, errMsg);
}

bool BasicTable::internalUpdate(vector<ConstantSP>& values, const ConstantSP& indexSP, vector<string>& colNames, string& errMsg){
	int num=values.size();
	vector<int> colIndex(num);
	vector<pair<string, ConstantSP>> newCols;
	unordered_map<string,int>::iterator it;

	INDEX rows= indexSP->isNothing() ? size() : indexSP->size();
	bool allowNewCol = rows == size_;
	for(int i=0;i<num;i++){
		it=colMap_->find(Util::lower(colNames[i]));
		if(it==colMap_->end()){
			colIndex[i] = -1;
			if(!allowNewCol || (!values[i]->isScalar() && values[i]->size() != rows)){
				errMsg.append("The column "+ colNames[i]+ " does not exist. To add a new column, the table shouldn't be shared and the value size must match the table.");
				return false;
			}
			else{
				ConstantSP value = values[i];
				if(value->isScalar()){
					VectorSP vec = Util::createVector(value->getType(), rows);
					vec->fill(0, rows, value);
					value = vec;
				}
				else if(!value->isTemporary())
					value = value->getValue();
				newCols.push_back(pair<string, ConstantSP>(colNames[i], value));
			}
		}
		else{
			colIndex[i]=it->second;

			if(cols_[colIndex[i]]->getCategory()!=values[i]->getCategory() && (!values[i]->isNumber() || !cols_[colIndex[i]]->isNumber()) && values[i]->getCategory()!=NOTHING){
				errMsg.append("The category of the value to update does not match the column ");
				errMsg.append(colNames_->at(colIndex[i]));
				return false;
			}
			int curSize=values[i]->size();
			if(curSize!=1 && rows!=curSize){
				errMsg.append("Inconsistent length of values to update.");
				return false;
			}
		}
	}

	for(int i=0;i<num;i++){
		if(colIndex[i] < 0)
			continue;
		if(!indexSP->isNothing())
			cols_[colIndex[i]]->set(indexSP,values[i]);
		else if(values[i]->isScalar())
			((Vector*)cols_[colIndex[i]].get())->fill(0,rows,values[i]);
		else
			cols_[colIndex[i]]->assign(values[i]);
	}
	for(unsigned int i=0; i<newCols.size(); ++i){
		colNames_->push_back(newCols[i].first);
		colMap_->insert(pair<string, int>(Util::lower(newCols[i].first), colMap_->size()));
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
	bool noIndex = indexSP.isNull() || indexSP->isNothing();
	int colCount = cols_.size();

	for(int i=0; i<colCount; ++i){
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

void BasicTable::drop(vector<int>& columns){
	if(isReadOnly())
		throw RuntimeException("Can't drop columns of a read only in-memory table.");
	internalDrop(columns);
}

void BasicTable::internalDrop(vector<int>& columns){
	unordered_set<int> dropColumns;
	dropColumns.insert(columns.begin(), columns.end());

	vector<ConstantSP> newCols;
	SmartPointer<vector<string>> newColNames = new vector<string>();
	SmartPointer<unordered_map<string,int>> newColMap = new unordered_map<string,int>();
	int numCol = colNames_->size();
	for(int i=0; i<numCol; ++i){
		if(dropColumns.find(i) != dropColumns.end())
			continue;
		newCols.push_back(cols_[i]);
		newColNames->push_back(colNames_->at(i));
		newColMap->insert(pair<string,int>(Util::lower(colNames_->at(i)), newCols.size()-1));
	}
	cols_ = newCols;
	colNames_ = newColNames;
	colMap_ = newColMap;
}

bool BasicTable::join(vector<ConstantSP>& columns){
	if(isReadOnly())
		return false;

	int num = columns.size();
	for(int i=0; i<num; ++i){
		ConstantSP& col = columns[i];
		string name = ((Vector*)col.get())->getName();
		if(!col->isArray() || col->size()!= size_ || name.empty() || colMap_->find(Util::lower(name)) != colMap_->end())
			return false;
	}
	for(int i=0; i<num; ++i){
		ConstantSP& col = columns[i];
		col->setTemporary(false);
		string name = ((Vector*)col.get())->getName();
		cols_.push_back(col);
		colNames_->push_back(name);
		colMap_->insert(pair<string,int>(Util::lower(name), cols_.size() - 1));
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
	long long size=sizeof(BasicTable)+sizeof(string)*colNames_->capacity();
	size+=sizeof(ConstantSP)*cols_.capacity();
	for(unsigned int i=0;i<cols_.size();++i)
		size+=cols_[i]->getAllocatedMemory();
	return size;
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
	int colCount = cols_.size();
	INDEX finalCapacity = INDEX_MAX;

	try{
		if(newCapacity < 0 || newCapacity > INT_MAX){
			errMsg = "An in-memory table can't exceed 2 billion rows.";
			return false;
		}

		for(int i=0; i<colCount; ++i){
			Vector* vec = (Vector*)cols_[i].get();
			if(newCapacity > vec->getCapacity()){
				INDEX capacity = (std::min)((long long)INDEX_MAX, (long long)(newCapacity * 1.2));
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
	catch(exception& ex){
		errMsg = string(ex.what());
		return false;
	}
	catch(...){
		errMsg.append("Unknown exception in BasicTable::increaseCapacity");
		return false;
	}
}

ConstantSP BasicTable::getSubTable(vector<int> indices) const{
	int colCount = cols_.size();
	vector<ConstantSP> cols(colCount);
	for(int i = 0; i < colCount; i++){
		cols[i] = Util::createSubVector(cols_[i], indices);
	}
	return new BasicTable(cols, *colNames_.get());
}
};
