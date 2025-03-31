#include "Table.h"
#include "Vector.h"
#include "Util.h"
#include "ConstantImp.h"

namespace dolphindb {

template<typename T>
class OneRowInArrayVector{
public:
	OneRowInArrayVector(const T* data, size_t length, bool isNeedCopy) 
		: dataPtr_(data), length_(length)
	{
		if(isNeedCopy) {
			data_.resize(length);
			memcpy((void*)data_.data(), data, length * sizeof(T));
			dataPtr_ = data_.data();
		}
	}
	size_t getLength() const {
		return length_;
	}
	T operator[](size_t index) const {
		if(index >= length_) {
			throw RuntimeException("index is out of range.");
		}
		return dataPtr_[index];
	}

	std::vector<T> copyToVector() const {
		std::vector<T> data(length_);
		memcpy(data.data(), dataPtr_, length_ * sizeof(T));
		return data;
	}

private:
	const T* dataPtr_;
	size_t length_;
	std::vector<T> data_;
};

class ResultSet {
public:
	ResultSet(const TableSP &table)
		: table_(table), position_(0)
	{
		rows_ = table_->rows();
		cols_ = table_->columns();
		column_ = new ColumnPointer[cols_];
		for (int i = 0; i < cols_; i++) {
			column_[i].pVector = table_->getColumn(i);
			VectorSP &pVector = column_[i].pVector;
			DATA_TYPE type = pVector->getRawType();
			if(pVector->getVectorType() == VECTOR_TYPE::ARRAYVECTOR){
				ConstantSP temp = Util::createConstant(type);
				type = temp->getRawType();
			}
			switch (type) {
			case DT_BOOL:
				column_[i].charCol = new Column<char>(pVector, [=](const VectorSP &vector, INDEX position, int len, char *buf) {
					return vector->getBoolConst(position, len, buf);
				});
				break;
			case DT_CHAR:
				column_[i].charCol = new Column<char>(pVector, [=](const VectorSP &vector, INDEX position, int len, char *buf) {
					return vector->getCharConst(position, len, buf);
				});
				break;
			case DT_SHORT:
				column_[i].shortCol = new Column<short>(pVector, [=](const VectorSP &vector, INDEX position, int len, short *buf) {
					return vector->getShortConst(position, len, buf);
				});
				break;
			case DT_INT:
				if (pVector->getType() == DT_SYMBOL) {
					column_[i].stringCol = new Column<std::string*>(pVector, [=](const VectorSP &vector, INDEX position, int len, std::string** buf) {
						return vector->getStringConst(position, len, buf);
					});
				}
				else {
					column_[i].intCol = new Column<int>(pVector, [=](const VectorSP &vector, INDEX position, int len, int *buf) {
						return vector->getIntConst(position, len, buf);
					});
				}
				break;
			case DT_LONG:
				column_[i].longCol = new Column<long long>(pVector, [=](const VectorSP &vector, INDEX position, int len, long long *buf) {
					return vector->getLongConst(position, len, buf);
				});
				break;
			case DT_FLOAT:
				column_[i].floatCol = new Column<float>(pVector, [=](const VectorSP &vector, INDEX position, int len, float *buf) {
					return vector->getFloatConst(position, len, buf);
				});
				break;
			case DT_DOUBLE:
				column_[i].doubleCol = new Column<double>(pVector, [=](const VectorSP &vector, INDEX position, int len, double *buf) {
					return vector->getDoubleConst(position, len, buf);
				});
				break;
			case DT_BLOB:
			case DT_STRING:
				column_[i].stringCol = new Column<std::string*>(pVector, [=](const VectorSP &vector, INDEX position, int len, std::string** buf) {
					return vector->getStringConst(position, len, buf);
				});
				break;
			case DT_INT128:
				column_[i].int128Col = new Column<Guid>(pVector, [=](const VectorSP &vector, INDEX position, int len, Guid *buf) {
					return (const Guid*)vector->getBinaryConst(position, len, sizeof(Guid), (unsigned char*)buf);
				});
				break;
			case DT_DECIMAL32:
				column_[i].decimal32Col = new Column<int>(pVector, [=](const VectorSP &vector, INDEX position, int len, int *buf) {
					return (const int*)vector->getBinaryConst(position, len, sizeof(int), (unsigned char*)buf);
				});
				break;
			case DT_DECIMAL64:
				column_[i].decimal64Col = new Column<long long>(pVector, [=](const VectorSP &vector, INDEX position, int len, long long *buf) {
					return (const long long*)vector->getBinaryConst(position, len, sizeof(long long), (unsigned char*)buf);
				});
				break;
			case DT_DECIMAL128:
				column_[i].decimal128Col = new Column<wide_integer::int128>(pVector, [=](const VectorSP &vector, INDEX position, int len, wide_integer::int128 *buf) {
					return (const wide_integer::int128*)vector->getBinaryConst(position, len, sizeof(wide_integer::int128), (unsigned char*)buf);
				});
				break;
			default:
				throw RuntimeException("ResultSet doesn't support data type " + Util::getDataTypeString(pVector->getType()));
				break;
			}
		}
	}
	~ResultSet() {
		delete[] column_;
	}
	INDEX position() {
		return position_;
	}
	void position(INDEX pos) {
		if (pos >= rows_ || pos < 0) {
			throw RuntimeException("Position exceed row limit.");
		}
		position_ = pos;
	}
	bool next() {
		if (position_ < rows_) {
			position_++;
			return true;
		}
		return false;
	}
	bool first() {
		if (rows_ > 0) {
			position_ = 0;
			return true;
		}
		return false;
	}
	bool isFirst() {
		return position_ == 0;
	}
	bool last() {
		if (rows_ > 0) {
			position_ = rows_ - 1;
			return true;
		}
		return false;
	}
	bool isLast() {
		return position_ + 1 == rows_;
	}
	bool isAfterLast() {
		return position_ >= rows_;
	}
	bool isBeforeFirst() {
		return position_ < 0;
	}
	DATA_TYPE getDataType(int col) {
		return table_->getColumnType(col);
	}
	char getBool(int col) {
		assert(column_[col].charCol != nullptr);
		return column_[col].charCol->getValue(position_);
	}
	char getChar(int col) {
		assert(column_[col].charCol != nullptr);
		return column_[col].charCol->getValue(position_);
	}
	short getShort(int col) {
		assert(column_[col].shortCol != nullptr);
		return column_[col].shortCol->getValue(position_);
	}
	int getInt(int col) {
		assert(column_[col].intCol != nullptr);
		return column_[col].intCol->getValue(position_);
	}
	long long getLong(int col) {
		assert(column_[col].longCol != nullptr);
		return column_[col].longCol->getValue(position_);
	}
	float getFloat(int col) {
		assert(column_[col].floatCol != nullptr);
		return column_[col].floatCol->getValue(position_);
	}
	double getDouble(int col) {
		assert(column_[col].doubleCol != nullptr);
		return column_[col].doubleCol->getValue(position_);
	}
	template<typename T>
	inline OneRowInArrayVector<T> getDataInArrayVector(int col){
		DATA_TYPE type = getDataType(col);
		throw RuntimeException("Not support This Type in ResultSet " + std::to_string(type));
	}
	const std::string& getString(int col) const {
		assert(column_[col].stringCol != nullptr);
		return *column_[col].stringCol->getValue(position_);
	}
	const unsigned char* getBinary(int col) const {
		ColumnPointer &column = column_[col];
		if(column.charCol!=nullptr)
			return (unsigned char*)&column.charCol->getValue(position_);
		else if (column.shortCol != nullptr)
			return (unsigned char*)&column.shortCol->getValue(position_);
		else if (column.intCol != nullptr)
			return (unsigned char*)&column.intCol->getValue(position_);
		else if (column.longCol != nullptr)
			return (unsigned char*)&column.longCol->getValue(position_);
		else if (column.floatCol != nullptr)
			return (unsigned char*)&column.floatCol->getValue(position_);
		else if (column.doubleCol != nullptr)
			return (unsigned char*)&column.doubleCol->getValue(position_);
		else if (column.stringCol != nullptr)
			return (unsigned char*)column.stringCol->getValue(position_)->data();
		else if(column.int128Col != nullptr)
			return (unsigned char*)&column.int128Col->getValue(position_);
		else if (column.decimal32Col != nullptr)
			return (unsigned char*)&column.decimal32Col->getValue(position_);
		else if (column.decimal64Col != nullptr)
			return (unsigned char*)&column.decimal64Col->getValue(position_);
		else if (column.decimal128Col != nullptr)
			return (unsigned char*)&column.decimal128Col->getValue(position_);
		else {
			throw RuntimeException("This instance doesn't support getBinary.");
		}
	}
	ConstantSP getObject(int col) const {
		return column_[col].pVector->get(position_);
	}
private:
	template <class T>
	class Column {
	public:
		Column(const VectorSP &vector,
			std::function<const T*(const VectorSP &pVector, INDEX position,int len, T *buf)> getBufConst)
			: pVector_(vector), getBufConst_(getBufConst),
				constRefBegin_(0), constRefEnd_(0) {
			rows_ = pVector_->rows();
			isArrayVector_ = pVector_->getVectorType() == VECTOR_TYPE::ARRAYVECTOR;
			if(isArrayVector_) {
				sourceValue_ = dynamic_cast<FastArrayVector *>(pVector_.get())->getSourceValue();
				sourceIndex_ = dynamic_cast<FastArrayVector *>(pVector_.get())->getSourceIndex();
				dataBuffer_.resize(Util::BUF_SIZE);
				prepareNextSection(0);
			}
			else {
				buffer_=new T[Util::BUF_SIZE];
			}
		}
		~Column() {
			if(!isArrayVector_){
				delete[] buffer_;
			}
		}
		const T& getValue(INDEX position) {
			int offset = offsetConst(position);
			return constRef_[offset];
		}

		OneRowInArrayVector<T> getOneRowInArrayVector(INDEX position) {
			if(!isArrayVector_) {
				throw RuntimeException("This column is not arrayVector");
			}
			int offset = offsetConst(position);
			INDEX end = indexPtr_[offset];
			INDEX start = offset == 0 ? IndexStart_ : indexPtr_[offset - 1];
			size_t count = end - start;
			if(count > dataBuffer_.size()) {
				dataBuffer_.resize(count);
			}
			const T* value = getBufConst_(sourceValue_, start, count, dataBuffer_.data());
			bool isNeedCopy = value == dataBuffer_.data();
			return OneRowInArrayVector<T>{value, count, isNeedCopy};
		}
	private:
		VectorSP pVector_;
		std::function<const T*(const VectorSP &pVector, INDEX position, int len, T *buf)> getBufConst_;
		T *buffer_;
		const T *constRef_;
		INDEX constRefBegin_, constRefEnd_;
		INDEX rows_;
		bool isArrayVector_;
		//used for arrayVector start
		INDEX nextIndexStart_ {0};
		INDEX IndexStart_ {0};
		VectorSP sourceValue_;
		VectorSP sourceIndex_;
		INDEX indexBuffer_[Util::BUF_SIZE];
		const INDEX *indexPtr_;
		std::vector<T> dataBuffer_;
		//used for arrayVector end
		int offsetConst(INDEX position) {
			if (position >= constRefBegin_ && position < constRefEnd_) {
				return position - constRefBegin_;
			}
			if (position < 0 || position >= rows_) {
				throw RuntimeException("Position is out of range.");
			}
			if (isArrayVector_) {
				prepareNextSection(position);
			} else {
				int size = std::min(Util::BUF_SIZE, rows_ - position);
				constRef_ = getBufConst_(pVector_, position, size, buffer_);
				constRefBegin_ = position;
				constRefEnd_ = position + size;
			}
			return 0;
		}
		void prepareNextSection(INDEX startIndex) {
			int size = std::min(Util::BUF_SIZE, rows_ - startIndex);
			if(size == 0) { 
				return;
			}
			indexPtr_ = sourceIndex_->getIndexConst(0, size, indexBuffer_);
			IndexStart_ = nextIndexStart_;
			nextIndexStart_ = indexPtr_[size - 1];
			constRefBegin_ = startIndex;
			constRefEnd_ = startIndex + size;
		}
	};
	struct ColumnPointer {
		VectorSP pVector;
		Column<char> *charCol = nullptr;
		Column<short> *shortCol = nullptr;
		Column<int> *intCol = nullptr;
		Column<long long> *longCol = nullptr;
		Column<float> *floatCol = nullptr;
		Column<double> *doubleCol = nullptr;
		Column<std::string*> *stringCol = nullptr;
		Column<Guid> *int128Col = nullptr;
		Column<int> *decimal32Col = nullptr;
		Column<long long> *decimal64Col = nullptr;
		Column<wide_integer::int128> *decimal128Col = nullptr;

		~ColumnPointer() {
			delete charCol;
			delete shortCol;
			delete intCol;
			delete longCol;
			delete floatCol;
			delete doubleCol;
			delete stringCol;
			delete int128Col;
			delete decimal32Col;
			delete decimal64Col;
			delete decimal128Col;
		}
	};
	TableSP table_;
	long position_;
	int rows_, cols_;
	ColumnPointer *column_;
};

template<>
inline OneRowInArrayVector<double> ResultSet::getDataInArrayVector<double>(int col){
	assert(column_[col].doubleCol != nullptr);
	return column_[col].doubleCol->getOneRowInArrayVector(position_);
}

template<>
inline OneRowInArrayVector<int> ResultSet::getDataInArrayVector<int>(int col){
	if(getDataType(col) == DT_DECIMAL32_ARRAY) {
		assert(column_[col].decimal32Col != nullptr);
		return column_[col].decimal32Col->getOneRowInArrayVector(position_);
	}
	assert(column_[col].intCol != nullptr);
	return column_[col].intCol->getOneRowInArrayVector(position_);
}

template<>
inline OneRowInArrayVector<char> ResultSet::getDataInArrayVector<char>(int col){
	assert(column_[col].charCol != nullptr);
	return column_[col].charCol->getOneRowInArrayVector(position_);
}

template<>
inline OneRowInArrayVector<short> ResultSet::getDataInArrayVector<short>(int col){
	assert(column_[col].shortCol != nullptr);
	return column_[col].shortCol->getOneRowInArrayVector(position_);
}

template<>
inline OneRowInArrayVector<long long> ResultSet::getDataInArrayVector<long long>(int col){
	if(getDataType(col) == DT_DECIMAL64_ARRAY) {
		assert(column_[col].decimal64Col != nullptr);
		return column_[col].decimal64Col->getOneRowInArrayVector(position_);
	}
	assert(column_[col].longCol != nullptr);
	return column_[col].longCol->getOneRowInArrayVector(position_);
}

template<>
inline OneRowInArrayVector<wide_integer::int128> ResultSet::getDataInArrayVector<wide_integer::int128>(int col){
	assert(column_[col].decimal128Col != nullptr);
	return column_[col].decimal128Col->getOneRowInArrayVector(position_);
}

template<>
inline OneRowInArrayVector<Guid> ResultSet::getDataInArrayVector<Guid>(int col){
	assert(column_[col].int128Col != nullptr);
	return column_[col].int128Col->getOneRowInArrayVector(position_);
}

template<>
inline OneRowInArrayVector<float> ResultSet::getDataInArrayVector<float>(int col){
	assert(column_[col].floatCol != nullptr);
	return column_[col].floatCol->getOneRowInArrayVector(position_);
}

}  // namespace dolphindb
