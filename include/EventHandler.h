#pragma once

#include "SmartPointer.h"
#include "ErrorCodeInfo.h"
#include "SysIO.h"
#include "Types.h"
#include "DolphinDB.h"
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
namespace dolphindb {

struct EXPORT_DECL EventSchema{
    std::string                 eventType_;
    std::vector<std::string>    fieldNames_;
    std::vector<DATA_TYPE>      fieldTypes_;
    std::vector<DATA_FORM>      fieldForms_;
    std::vector<int>            fieldExtraParams_;
};

struct EXPORT_DECL EventSchemaEx{
    EventSchema     schema_;
    int             timeIndex_;
    std::vector<int>    commonKeyIndex_;
};

class AttributeSerializer{
public:
    AttributeSerializer(int unitLen, DATA_FORM form): unitLen_(unitLen), form_(form) {}
    virtual ~AttributeSerializer() = default;
    virtual IO_ERR serialize(const ConstantSP& attribute, DataOutputStreamSP outStream);

protected:
    int unitLen_;
    DATA_FORM form_;
};

class FastArrayAttributeSerializer : public AttributeSerializer{
public:
    FastArrayAttributeSerializer(int unitLen) : AttributeSerializer(unitLen, DF_VECTOR) {}
    ~FastArrayAttributeSerializer() = default;
    virtual IO_ERR serialize(const ConstantSP& attribute, DataOutputStreamSP outStream) override;
};

class ScalarAttributeSerializer : public AttributeSerializer {
public:
    ScalarAttributeSerializer(int unitLen) : AttributeSerializer(unitLen, DF_SCALAR) {buf_.resize(unitLen_);}
    ~ScalarAttributeSerializer() = default;
    virtual IO_ERR serialize(const ConstantSP& attribute, DataOutputStreamSP outStream) override;

private:
    std::string buf_;
};

class StringScalarAttributeSerializer : public AttributeSerializer {
public:
    StringScalarAttributeSerializer(bool isBlob) : AttributeSerializer(-1, DF_SCALAR), isBlob_(isBlob) {}
    ~StringScalarAttributeSerializer() = default;
    virtual IO_ERR serialize(const ConstantSP& attribute, DataOutputStreamSP outStream) override;

private:
    bool isBlob_;
};

using AttributeSerializerSP = SmartPointer<AttributeSerializer>;
using EventSchemaExSP = SmartPointer<EventSchemaEx>;

struct EventInfo{
    std::vector<AttributeSerializerSP>  attributeSerializers_;
    EventSchemaExSP                     eventSchema_;
};

class EventHandler{
public:
    EventHandler(const std::vector<EventSchema>& eventSchemas, const std::vector<std::string>& eventTimeKeys, const std::vector<std::string>& commonKeys);
    bool checkOutputTable(TableSP outputTable, std::string& errMsg);
    bool serializeEvent(const std::string& eventType, const std::vector<ConstantSP>& attributes, std::vector<ConstantSP>& serializedEvent, std::string& errMsg);
    bool deserializeEvent(ConstantSP obj, std::vector<std::string>& eventTypes, std::vector<std::vector<ConstantSP>>& attributes, ErrorCodeInfo& errorInfo);
private:
    bool checkSchema(const std::vector<EventSchema>& eventSchemas, const std::vector<std::string> &expandTimeKeys, const std::vector<std::string>& commonKeys, std::string& errMsg);
    ConstantSP deserializeScalar(DATA_TYPE type, int extraParam, DataInputStreamSP input, IO_ERR& ret);
    ConstantSP deserializeFastArray(DATA_TYPE type, int extraParam, DataInputStreamSP input, IO_ERR& ret);
    ConstantSP deserializeAny(DATA_TYPE type, DATA_FORM form, DataInputStreamSP input, IO_ERR& ret);
private:
    std::unordered_map<std::string, EventInfo> eventInfos_;
    bool isNeedEventTime_;

    int outputColNums_;                 //the number of columns of the output table
    int commonKeySize_;
};

class EXPORT_DECL EventSender{
public:
    EventSender(DBConnectionSP conn, const std::string& tableName, const std::vector<EventSchema>& eventSchema, const std::vector<std::string>& eventTimeFields = std::vector<std::string>(), const std::vector<std::string>& commonFields = std::vector<std::string>());
    void sendEvent(const std::string& eventType, const std::vector<ConstantSP>& attributes);

private:
    std::string             insertScript_;
    EventHandler            eventHandler_;
    DBConnectionSP          conn_;
};

}