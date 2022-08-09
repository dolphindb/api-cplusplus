#ifdef LINUX

#include "Util.h"
#include "SharedMem.h"

namespace dolphindb {


TableSP SharedMemStream::readData(TableSP& copy, const bool& overwrite, size_t& readRows, int& microCost) {
    int writeTimeSeconds = 0;
    int writeTimeMicroSeconds = 0;

    if (!shmpHeader_) {
        std::string errMsg;
        if(!openSharedMem(errMsg)) {
            throw RuntimeException("shm init error, msg:" + errMsg);
        }
    }

    if (sem_wait(&shmpHeader_->readSem_) == -1) {
        throw RuntimeException("sem wait error!");
    }

    int startRows = 0;
    if (!copy.isNull()) {
        if (overwrite) {
            ((BasicTable*)copy.get())->clear();
        }
        startRows = copy->rows();
    }

    if (shmpHeader_->writePage_ == shmpHeader_->readPage_ && shmpHeader_->readOffset_ == shmpHeader_->writeOffset_) {
        auto it = tableMeta_.begin();
        if (copy.isNull()) {
            TableSP table = Util::createTable(it->second.colNames_, it->second.colTypes_, 0, 0);
            return table;
        } else {
            return copy;
        }
    }

    char* dataStartBuf = shmpHeader_->data_ + shmpHeader_->readOffset_;
    DataHeader* dataHeader = (DataHeader*)(dataStartBuf);

    // LOG_INFO("SharedMemStream readData, write_page:", shmpHeader_->writePage_, ", read_page:", shmpHeader_->readPage_, ", data length:", 
    //  dataHeader->length_, "write offset:", shmpHeader_->writeOffset_, "read offset:", shmpHeader_->readOffset_);
    if ((shmpHeader_->writePage_ > shmpHeader_->readPage_) && (dataHeader->length_ == 0 || \
        shmpHeader_->writeOffset_ > shmpHeader_->readOffset_)) {
        shmpHeader_->readOffset_ = 0;
        shmpHeader_->readPage_ = shmpHeader_->writePage_;
        dataStartBuf = shmpHeader_->data_ + shmpHeader_->readOffset_;
        dataHeader = (DataHeader*)(dataStartBuf);
    }

    writeTimeSeconds = dataHeader->timestampSeconds_;
    writeTimeMicroSeconds = dataHeader->timestampMicro_;

    dataStartBuf += DATA_HEADER_BASE_LENGTH + dataHeader->tablenameLen_;
    std::string tableName(dataHeader->tablename_, dataHeader->tablenameLen_);
    auto tableMetaIt = tableMeta_.find(tableName);
    if (tableMetaIt == tableMeta_.end()) {
        parseTableMeta(true);
        tableMetaIt = tableMeta_.find(tableName);
        if (tableMetaIt == tableMeta_.end()) {
            throw RuntimeException("not found table meta for table data, table name:" + tableName);
        }
    }

    int colDataLength;
    TableSP table;
    if (!copy.isNull()) {
        if (dataHeader->rows_ + startRows > copy->size()) {
            for (int colIndex = 0; colIndex < copy->columns(); colIndex++) {
                if (copy->getColumnType(colIndex) == DT_STRING || copy->getColumnType(colIndex) == DT_SYMBOL || copy->getColumnType(colIndex) == DT_BLOB) {
                    continue;
                }
                VectorSP colData = copy->getColumn(colIndex);
                colData->resize(startRows + dataHeader->rows_);
            }
        }
        table = copy;
    } else {
        table = Util::createTable(tableMetaIt->second.colNames_, tableMetaIt->second.colTypes_, dataHeader->rows_, dataHeader->rows_);
    }

    for (unsigned int i = 0; i < tableMetaIt->second.colLength_.size(); i++) {
        VectorSP colToWrite = table->getColumn(i);
        if (table->getColumnType(i) == DT_STRING || table->getColumnType(i) == DT_SYMBOL || table->getColumnType(i) == DT_BLOB) {
            for (int j = 0; j < dataHeader->rows_; j++) {
                int* stringLen = (int*)dataStartBuf;
                dataStartBuf += sizeof(int);
                std::string str = std::string(dataStartBuf, *stringLen);
                if (j + startRows < table->size()) {
                    colToWrite->setString(j + startRows, str);
                } else {
                    // TODO store `str` in buf, then append to table once.
                    colToWrite->appendString(&str, 1);
                }
                dataStartBuf += *stringLen;
            }
        } else {
            for (int j = 0; j < dataHeader->rows_;) {
                int copyRows = std::min(Util::BUF_SIZE, dataHeader->rows_ - j);
                colDataLength = tableMetaIt->second.colLength_[i] * copyRows;
                void* colData = colToWrite->getDataBuffer(startRows + j, colDataLength, NULL);
                memcpy((char*)colData, dataStartBuf, colDataLength);
                dataStartBuf += colDataLength;
                j += copyRows;
            }
        }
    }
    shmpHeader_->readOffset_ += dataHeader->length_;

    ((BasicTable*)(table.get()))->updateSize();

    // LOG_INFO("shared mem get data, micro cost:", IpcMemUtil::calculateMicroCost(writeTimeSeconds, writeTimeMicroSeconds));
    //microCost = IpcMemUtil::calculateMicroCost(writeTimeSeconds, writeTimeMicroSeconds);
    readRows = dataHeader->rows_;
    return table;
}

bool SharedMemStream::openSharedMem(std::string& errMsg) {
    int length = sizeof(MemTableHeader) + size_;

    shmFd_ = shm_open(path_.c_str(), O_RDWR, 0);
    if (shmFd_ == -1) {
        throw RuntimeException("shared memory, open failed");
    }

    void* startPos = mmap(NULL, length,
                            PROT_READ | PROT_WRITE,
                            MAP_SHARED, shmFd_, 0);

    if (startPos == MAP_FAILED) {
        errMsg = std::string("mmap err");
        return false;
    }

    shmpHeader_ = static_cast<MemTableHeader*>(startPos);
    return true;
}

void SharedMemStream::parseTableMeta(bool print) {
    if(!shmpHeader_) {
        std::string errMsg;
        if(!openSharedMem(errMsg)) {
            throw RuntimeException("shared memory, open failed");
        }
    }

    int offset = 0;
    for (unsigned int i = 0; i < shmpHeader_->tableCnt_; i++) {
        TableMeta*  tablemeta = (TableMeta*)(shmpHeader_->meta_ + offset);
        std::string tableName(tablemeta->name_, tablemeta->nameLen_);

        auto it = tableMeta_.find(tableName);
        if (it == tableMeta_.end()) {
            TableMetaData  tableData(tableName);

            for (int i = 0;  i < tablemeta->colNum_; i++) {
                ColMeta* colmeta = (ColMeta*)(tablemeta->colMeta_) + i;
                tableData.colNames_.push_back(std::string(colmeta->name_, colmeta->nameLen_));
                tableData.colTypes_.push_back((DATA_TYPE)(colmeta->type_));
                tableData.colLength_.push_back(colmeta->length_);
            }
            tableMeta_.insert(std::pair<std::string, TableMetaData>(tableName, tableData));
        }
        offset += TABLE_META_BASE_LEN + sizeof(ColMeta) * (tablemeta->colNum_);
    }

}

IPCInMemTable::IPCInMemTable(bool create, std::string shmPath, std::string tableName, const vector<ConstantSP>& cols, \
  const vector<string>& colNames, size_t bufSize) : BasicTable(cols, colNames), tablename_(tableName) {
    pShmStream_ = std::shared_ptr<SharedMemStream>(new SharedMemStream(create, shmPath, bufSize));

    if (!pShmStream_) {
        throw RuntimeException("IPCInMemTable init shared mem failed");
    }
    microCost_ = 0;
}

IPCInMemTable::~IPCInMemTable() {
    closeShm();
}


TableSP IPCInMemTable::read(TableSP& copy, const bool& overwrite, size_t& readRows, int& timeout) {
    return pShmStream_->readData(copy, overwrite, readRows, microCost_);
}

int IPCInMemTable::getMicroCost() {
    return microCost_;
}

void IPCInMemTable::closeShm() {
    if (!pShmStream_) {
        throw RuntimeException("IpcMemTable read error, shm not inited!"); 
    }

    return pShmStream_->closeShm();
}

void IPCInMemTable::unlinkShm() {
    if (!pShmStream_) {
        throw RuntimeException("IpcMemTable read error, shm not inited!"); 
    }

    return pShmStream_->unlinkShm();
}
} // namespace dolphindb

#endif