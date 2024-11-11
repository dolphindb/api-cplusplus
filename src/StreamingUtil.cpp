#include "StreamingUtil.h"
#include "Util.h"
#include "DolphinDB.h"

namespace dolphindb {

StreamDeserializer::StreamDeserializer(const std::unordered_map<std::string, std::pair<std::string, std::string>> &sym2tableName, DBConnection *pconn)
                    : sym2tableName_(sym2tableName) {
    if (pconn != NULL) {
        create(*pconn);
    }
}

StreamDeserializer::StreamDeserializer(const std::unordered_map<std::string, DictionarySP> &sym2schema) {
    parseSchema(sym2schema);
}

StreamDeserializer::StreamDeserializer(const std::unordered_map<std::string, std::vector<DATA_TYPE>> &symbol2col){
    for(auto one : symbol2col){
        symbol2tableInfo_[one.first] = new TableInfo(one.second);
    }
}

void StreamDeserializer::returnMessage(Message *msg){
    symbol2tableInfo_[msg->getSymbol()]->returnTuple(*msg);
}

void StreamDeserializer::setTupleLimit(INDEX limit){
    for(auto one : symbol2tableInfo_){
        one.second->setLimit(limit);
    }
}

ConstantSP StreamDeserializer::TableInfo::newTuple(){
    {
        LockGuard<Mutex> locker(&mutex_);
        if(queue_.size()>0){
            auto res=queue_.back();
            queue_.pop_back();
            return res;
        }
    }
    ConstantSP rowVec=Util::createVector(DT_ANY, static_cast<INDEX>(cols_.size()));
    for (auto i = 0; i < (int)cols_.size(); i++) {
        auto &colOne = cols_[i];
        auto &scale = scales_[i];
        if (colOne < ARRAY_TYPE_BASE) {
            auto value = Util::createConstant(colOne, scale);
            rowVec->set(i, value);
        }
        else {
            auto value = Util::createArrayVector(colOne, 1, 1, true, scale);
            rowVec->set(i, value);
        }
    }
    return rowVec;
}

void StreamDeserializer::create(DBConnection &conn) {
    if (symbol2tableInfo_.size() > 0 || sym2tableName_.empty())
        return;
    std::unordered_map<std::string, DictionarySP> sym2schema;
    DictionarySP schema;
    for (auto &one : sym2tableName_) {
        if (one.second.first.empty()) {
            schema = conn.run("schema(" + one.second.second + ")");
        }
        else {
            schema = conn.run(std::string("schema(loadTable(\"") + one.second.first + "\",\"" + one.second.second + "\"))");
        }
        sym2schema[one.first] = schema;
    }
    parseSchema(sym2schema);
}
bool StreamDeserializer::parseBlob(const ConstantSP &src, std::vector<VectorSP> &rows, std::vector<std::string> &symbols, ErrorCodeInfo &errorInfo) {
    const VectorSP &symbolVec = src->get(1);
    const VectorSP &blobVec = src->get(2);
    INDEX rowSize = symbolVec->rows();
    rows.resize(rowSize);
    symbols.resize(rowSize);
    std::unordered_map<std::string, SmartPointer<TableInfo>>::iterator colTableIter;
    std::unordered_map<std::string, std::vector<int>>::iterator colScaleIter;
    for (INDEX rowIndex = 0; rowIndex < rowSize; rowIndex++) {
        std::string symbol = symbolVec->getString(rowIndex);
        {
            LockGuard<Mutex> lock(&mutex_);
            colTableIter = symbol2tableInfo_.find(symbol);
            if (colTableIter == symbol2tableInfo_.end()) {
                errorInfo.set(ErrorCodeInfo::EC_InvalidParameter, std::string("Unknown symbol ") + symbol);
                return false;
            }
        }
        symbols[rowIndex] = std::move(symbol);

        const std::string &blob = blobVec->getStringRef(rowIndex);
        DataInputStreamSP dis = new DataInputStream(blob.data(), blob.size(), false);
        INDEX num;
        IO_ERR ioError;
        VectorSP rowVec = colTableIter->second->newTuple();
        for (auto i = 0; i < (int)rowVec->size(); i++) {
            ioError = rowVec->get(i)->deserialize(dis.get(), 0, 1, num);
            if (ioError != OK) {
                errorInfo.set(ErrorCodeInfo::EC_InvalidObject, "Deserialize blob error " + std::to_string(ioError));
                return false;
            }
        }
        rows[rowIndex] = rowVec;
    }
    return true;
}
void StreamDeserializer::parseSchema(const std::unordered_map<std::string, DictionarySP> &sym2schema) {

    LockGuard<Mutex> lock(&mutex_);

    for (auto &one : sym2schema) {
        const DictionarySP &schema = one.second;
        TableSP colDefs = schema->getMember("colDefs");
        size_t columnSize = colDefs->size();

        // types
        ConstantSP colDefsTypeInt = colDefs->getColumn("typeInt");
        std::vector<DATA_TYPE> colTypes(columnSize);
        for (auto i = 0; i < (int)columnSize; i++) {
            colTypes[i] = (DATA_TYPE)colDefsTypeInt->getInt(i);
        }
        std::vector<int> colScales(columnSize);
        // scales for decimals (server 130 doesn't have this column)
        if (colDefs->contain("extra")) {
            ConstantSP colDefsScales = colDefs->getColumn("extra");
            for (auto i = 0; i < (int)columnSize; i++) {
                colScales[i] = colDefsScales->getInt(i);
            }
        }
        symbol2tableInfo_[one.first] = new TableInfo(colTypes, colScales);
    }
}

}
