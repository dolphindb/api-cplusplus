// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include <atomic>

#include "Table.h"
namespace dolphindb {

class AbstractTable : public Table {
public:
	AbstractTable(const SmartPointer<std::vector<std::string>>& colNames);
	AbstractTable(const SmartPointer<std::vector<std::string>>& colNames, SmartPointer<std::unordered_map<std::string,int>> colMap);
	virtual ~AbstractTable(){}
	virtual ConstantSP getColumn(const std::string& name) const;
	virtual ConstantSP getColumn(const std::string& qualifier, const std::string& name) const;
	virtual ConstantSP getColumn(const std::string& name, const ConstantSP& rowFilter) const;
	virtual ConstantSP getColumn(const std::string& qualifier, const std::string& name, const ConstantSP& rowFilter) const;
	virtual ConstantSP getColumn(INDEX index, const ConstantSP& rowFilter) const;
	virtual ConstantSP getColumn(INDEX index) const = 0;
	virtual ConstantSP get(INDEX col, INDEX row) const = 0;
	virtual INDEX columns() const {return static_cast<INDEX>(colNames_->size());}
	virtual const std::string& getColumnName(int index) const {return colNames_->at(index);}
	virtual const std::string& getColumnQualifier(int index) const
	{
		std::ignore = index;
		return name_;
	}
	virtual void setColumnName(int index, const std::string& name);
	virtual int getColumnIndex(const std::string& name) const;
	virtual bool contain(const std::string& name) const;
	virtual bool contain(const std::string& qualifier, const std::string& name) const;
	virtual ConstantSP getColumnLabel() const;
	virtual ConstantSP values() const;
	virtual ConstantSP keys() const { return getColumnLabel();}
	virtual void setName(const std::string& name){name_=name;}
	virtual const std::string& getName() const { return name_;}
	virtual bool isTemporary() const {return false;}
	virtual void setTemporary(bool temp){std::ignore = temp;}
	virtual bool sizeable() const {return false;}
	virtual std::string getString(INDEX index) const;
	virtual std::string getString() const;
	virtual ConstantSP get(INDEX index) const { return getInternal(index);}
	virtual bool set(INDEX index, const ConstantSP& value);
	virtual ConstantSP get(const ConstantSP& index) const { return getInternal(index);}
	virtual ConstantSP getWindow(int colStart, int colLength, int rowStart, int rowLength) const {return getWindowInternal(colStart, colLength, rowStart, rowLength);}
	virtual ConstantSP getMember(const ConstantSP& key) const { return getMemberInternal(key);}
	virtual ConstantSP getInstance() const {return getInstance(0);}
	virtual ConstantSP getInstance(int size) const;
	virtual ConstantSP getValue() const;
	virtual ConstantSP getValue(INDEX capacity) const;
	virtual bool append(std::vector<ConstantSP>& values, INDEX& insertedRows, std::string& errMsg);
	virtual bool update(std::vector<ConstantSP>& values, const ConstantSP& indexSP, std::vector<std::string>& colNames, std::string& errMsg);
	virtual bool remove(const ConstantSP& indexSP, std::string& errMsg);
	virtual ConstantSP getSubTable(std::vector<int> indices) const = 0;
	virtual COMPRESS_METHOD getColumnCompressMethod(INDEX index);
	virtual void setColumnCompressMethods(const std::vector<COMPRESS_METHOD> &methods);
	virtual bool clear()=0;
	virtual void updateSize() = 0;
protected:
	ConstantSP getInternal(INDEX index) const;
	ConstantSP getInternal(const ConstantSP& index) const;
	ConstantSP getWindowInternal(int colStart, int colLength, int rowStart, int rowLength) const;
	ConstantSP getMemberInternal(const ConstantSP& key) const;

private:
	std::string getTableClassName() const;
	std::string getTableTypeName() const;

protected:
	SmartPointer<std::vector<std::string>> colNames_;
	SmartPointer<std::unordered_map<std::string,int>> colMap_;
	std::string name_;
	std::vector<COMPRESS_METHOD> colCompresses_;
};


class BasicTable: public AbstractTable{
public:
	BasicTable(const std::vector<ConstantSP>& cols, const std::vector<std::string>& colNames, const std::vector<int>& keys);
	BasicTable(const std::vector<ConstantSP>& cols, const std::vector<std::string>& colNames);
	virtual ~BasicTable();
	virtual bool isBasicTable() const {return true;}
	virtual ConstantSP getColumn(INDEX index) const;
	virtual ConstantSP get(INDEX col, INDEX row) const {return cols_[col]->get(row);}
	virtual DATA_TYPE getColumnType(int index) const { return cols_[index]->getType();}
	virtual void setColumnName(int index, const std::string& name);
	virtual INDEX size() const {return size_;}
	virtual bool sizeable() const {return isReadOnly()==false;}
	virtual bool set(INDEX index, const ConstantSP& value);
	virtual ConstantSP get(INDEX index) const;
	virtual ConstantSP get(const ConstantSP& index) const;
	virtual ConstantSP getWindow(INDEX colStart, int colLength, INDEX rowStart, int rowLength) const;
	virtual ConstantSP getMember(const ConstantSP& key) const;
	virtual ConstantSP getInstance(int size) const;
	virtual ConstantSP getValue() const;
	virtual ConstantSP getValue(INDEX capacity) const;
	virtual bool append(std::vector<ConstantSP>& values, INDEX& insertedRows, std::string& errMsg);
	virtual bool update(std::vector<ConstantSP>& values, const ConstantSP& indexSP, std::vector<std::string>& colNames, std::string& errMsg);
	virtual bool remove(const ConstantSP& indexSP, std::string& errMsg);
	virtual long long getAllocatedMemory() const;
	virtual TABLE_TYPE getTableType() const {return BASICTBL;}
	virtual void drop(std::vector<int>& columns);
	bool join(std::vector<ConstantSP>& columns);
	virtual bool clear();
	virtual void updateSize();
	virtual ConstantSP getSubTable(std::vector<int> indices) const;

private:
	bool increaseCapacity(long long newCapacity, std::string& errMsg);
	void initData(const std::vector<ConstantSP>& cols, const std::vector<std::string>& colNames);
	//bool internalAppend(std::vector<ConstantSP>& values, string& errMsg);
	bool internalRemove(const ConstantSP& indexSP, std::string& errMsg);
	void internalDrop(std::vector<int>& columns);
	bool internalUpdate(std::vector<ConstantSP>& values, const ConstantSP& indexSP, std::vector<std::string>& colNames, std::string& errMsg);

private:
	std::vector<ConstantSP> cols_;
	//bool readOnly_;
	INDEX size_;
	INDEX capacity_;
};

typedef SmartPointer<BasicTable> BasicTableSP;

}
