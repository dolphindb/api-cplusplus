#include <gtest/gtest.h>
#include "config.h"
#include "Streaming.h"
#include "EventHandler.h"
#include <future>

class StreamingCEPEventTest : public testing::Test
{
    public:
        static dolphindb::DBConnectionSP conn;
        static void SetUpTestSuite()
        {
            bool ret = conn->connect(HOST, PORT, USER, PASSWD);
            if (!ret)
            {
                std::cout << "Failed to connect to the server" << std::endl;
            }
            else
            {
                std::cout << "connect to " + HOST + ":" + std::to_string(PORT) << std::endl;
            }
        }
        static void TearDownTestSuite()
        {
            conn->close();
        }

    protected:
        virtual void SetUp()
        {

        }
        virtual void TearDown()
        {

        }
};

dolphindb::DBConnectionSP StreamingCEPEventTest::conn(new dolphindb::DBConnection());

auto test_handler = [](std::string eventType, std::vector<dolphindb::ConstantSP> attribute)
{
    std::string case_=getCaseName();
    try
    {
        StreamingCEPEventTest::conn->run("tableInsert{"+case_+"_outputTable}", attribute);
    }
    catch (std::exception &e)
    {
        throw dolphindb::RuntimeException(e.what());
    }
};

std::vector<std::string> subInputTable(const std::string &t, const int target_rows, const std::vector<dolphindb::EventSchema>& EventSchemas, const std::vector<std::string>& eventTimeFields, const std::vector<std::string>& commonFields){
    dolphindb::EventClient *client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    int total_rows = 0;
    std::map<std::string, std::string> tmap = {};
    for (auto &schema: EventSchemas)
    {
        std::vector<std::string> colNames = schema.fieldNames_;
        std::vector<dolphindb::DATA_TYPE> colTypes = schema.fieldTypes_;
        dolphindb::TableSP t = dolphindb::Util::createTable(colNames, colTypes, 0, 0, schema.fieldExtraParams_);
        std::string curTabName = "output_" + schema.eventType_;
        StreamingCEPEventTest::conn->upload(curTabName, t);
        tmap.insert(std::pair<std::string, std::string>(schema.eventType_, curTabName));
    }

    auto _handler = [&](const std::string& eventType, std::vector<dolphindb::ConstantSP>& attribute)
    {
        total_rows += 1;
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        for (auto &schema: EventSchemas)
        {
            if (schema.eventType_ == eventType)
            {
                std::vector<dolphindb::ConstantSP> columnVecs;
                columnVecs.reserve(attribute.size());
                for (auto i=0;i<attribute.size();i++)
                {
                    if (attribute[i]->getForm() == dolphindb::DF_VECTOR){
                        dolphindb::VectorSP avCol = dolphindb::Util::createArrayVector(attribute[i]->getType(), 0, 0, true, attribute[i]->getExtraParamForType());
                        avCol->append(attribute[i]);
                        columnVecs.emplace_back(avCol);
                    }else{
                        dolphindb::VectorSP col = dolphindb::Util::createVector(attribute[i]->getType(), 0, 0, true, attribute[i]->getExtraParamForType());
                        for (auto j=0;j<attribute[i]->rows();j++)
                        {
                            col->append(attribute[i]->get(j));
                        }
                        columnVecs.emplace_back(col);
                    }
                }

                dolphindb::TableSP t = dolphindb::Util::createTable(schema.fieldNames_, columnVecs);
                dolphindb::AutoFitTableAppender appender = dolphindb::AutoFitTableAppender("", tmap[eventType], *StreamingCEPEventTest::conn);
                ASSERT_EQ(appender.append(t), 1);
            }
        }
        if (total_rows % 100 == 0)
            std::cout << "total_rows: " << total_rows << std::endl;

        if (total_rows == target_rows) notify.set();
    };

    auto th = client->subscribe(HOST, PORT, _handler, t, DEFAULT_ACTION_NAME, 0, true, USER, PASSWD);
    notify.wait();

    client->unsubscribe(HOST, PORT, t, DEFAULT_ACTION_NAME);
    dolphindb::Util::sleep(1000);
    bool r=th->isComplete();
    delete client;

    std::vector<std::string> outputNames = {};
    for (auto &p : tmap){
        outputNames.push_back(p.second);
    }
    return outputNames;
}

TEST_F(StreamingCEPEventTest, test_EventSchema_null)
{
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    std::vector<dolphindb::EventSchema> EventSchemas = {};
    std::vector<std::string> eventTimeFields = {};
    std::vector<std::string> commonFields = {};
    std::string re = "";
    try
    {
        dolphindb::EventSender *sender = new dolphindb::EventSender(conn, "", EventSchemas, eventTimeFields, commonFields);
    }
    catch (std::exception &ex)
    {
        re = ex.what();
    }
    ASSERT_EQ("eventSchemas must not be empty", re);

    std::string re1 = "";
    try
    {
        dolphindb::EventClient *client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
    }
    catch (std::exception &ex)
    {
        re1 = ex.what();
    }
    ASSERT_EQ("eventSchemas must not be empty", re1);
}

TEST_F(StreamingCEPEventTest, test_EventType_null)
{
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->fieldNames_ = {"market", "code", "price", "qty", "eventTime"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_STRING, dolphindb::DT_DOUBLE, dolphindb::DT_INT, dolphindb::DT_TIMESTAMP};
    schema->fieldForms_ = {dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {};
    std::vector<std::string> eventTimeFields = {};
    std::vector<std::string> commonFields = {};
    std::string re = "";
    try
    {
        dolphindb::EventSender *sender = new dolphindb::EventSender(conn, "", EventSchemas, eventTimeFields, commonFields);
    }
    catch (std::exception &ex)
    {
        re = ex.what();
    }
    ASSERT_EQ("eventSchemas must not be empty", re);

    std::string re1 = "";
    try
    {
        dolphindb::EventClient *client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
    }
    catch (std::exception &ex)
    {
        re1 = ex.what();
    }
    ASSERT_EQ("eventSchemas must not be empty", re1);
}

TEST_F(StreamingCEPEventTest, test_EventType_null2)
{
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "";
    schema->fieldNames_ = {"market", "code", "price", "qty", "eventTime"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_STRING, dolphindb::DT_DOUBLE, dolphindb::DT_INT, dolphindb::DT_TIMESTAMP};
    schema->fieldForms_ = {dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {};
    std::vector<std::string> commonFields = {};
    std::string re = "";
    try
    {
        dolphindb::EventSender *sender = new dolphindb::EventSender(conn, "", EventSchemas, eventTimeFields, commonFields);
    }
    catch (std::exception &ex)
    {
        re = ex.what();
    }
    ASSERT_EQ("eventType must not be empty.", re);

    std::string re1 = "";
    try
    {
        dolphindb::EventClient *client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
    }
    catch (std::exception &ex)
    {
        re1 = ex.what();
    }
    ASSERT_EQ("eventType must not be empty.", re1);
}

TEST_F(StreamingCEPEventTest, test_EventType_special_character)
{
    std::string case_=getCaseName();
    std::string script = "share streamTable(1:0, `eventType`event, [STRING,BLOB]) as "+case_+"_inputTable;\n"
                    "share streamTable(1:0, `market`code`price`qty`eventTime, [STRING,STRING,DOUBLE,INT,TIMESTAMP]) as "+case_+"_outputTable;";
    conn->run(script);
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "!@#$%&*()_+《》>{}[]-=';./,~`1^;中文 \"\n\t\r1";
    schema->fieldNames_ = {"market", "code", "price", "qty", "eventTime"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_STRING, dolphindb::DT_DOUBLE, dolphindb::DT_INT, dolphindb::DT_TIMESTAMP};
    schema->fieldForms_ = {dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {};
    std::vector<std::string> commonFields = {};
    dolphindb::EventSender *sender = new dolphindb::EventSender(conn, case_+"_inputTable", EventSchemas, eventTimeFields, commonFields);

    dolphindb::EventClient* client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
    client->subscribe(HOST, PORT, test_handler, case_+"_inputTable", DEFAULT_ACTION_NAME, 0, true, USER, PASSWD);
    sender->sendEvent(schema->eventType_, {dolphindb::Util::createString("sz"), dolphindb::Util::createString("000001"), dolphindb::Util::createDouble(10.5), dolphindb::Util::createInt(100), dolphindb::Util::createTimestamp(2021, 1, 1, 10, 30, 0, 100)});
    dolphindb::Util::sleep(2000);

    client->unsubscribe(HOST, PORT, case_+"_inputTable", DEFAULT_ACTION_NAME);
    dolphindb::TableSP re = conn->run(case_+"_outputTable");
    ASSERT_EQ(1, re->rows());
    ASSERT_EQ("sz", re->getColumn(0)->get(0)->getString());
    ASSERT_EQ("000001", re->getColumn(1)->get(0)->getString());
    ASSERT_EQ(10.5, re->getColumn(2)->get(0)->getDouble());
    ASSERT_EQ(100, re->getColumn(3)->get(0)->getInt());
    ASSERT_EQ("2021.01.01T10:30:00.100", re->getColumn(4)->get(0)->getString());
}

TEST_F(StreamingCEPEventTest, test_EventType_same_with_eventTimeFields)
{
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time", "decimal32", "decimal64", "decimal128"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME, dolphindb::DT_DECIMAL32, dolphindb::DT_DECIMAL64, dolphindb::DT_DECIMAL128};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    dolphindb::EventSchema *schema1 = new dolphindb::EventSchema(*schema);

    std::vector<dolphindb::EventSchema> EventSchemas = {};
    EventSchemas.push_back(*schema);
    EventSchemas.push_back(*schema1);
    std::vector<std::string> eventTimeFields = {"market"};
    std::vector<std::string> commonFields = {};
    std::string re = "";
    try{
        dolphindb::EventSender* sender = new dolphindb::EventSender(conn, "", EventSchemas, eventTimeFields, commonFields);
    }catch(const std::exception& ex){
        re = ex.what();
    }
    ASSERT_EQ("EventType must be unique", re);
    std::string re1 = "";
    try{
        dolphindb::EventClient* client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
    }catch(const std::exception& ex){
        re1 = ex.what();
    }
    ASSERT_EQ("EventType must be unique", re1);
}

TEST_F(StreamingCEPEventTest, test_fieldNames_null)
{
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME, dolphindb::DT_DECIMAL32, dolphindb::DT_DECIMAL64, dolphindb::DT_DECIMAL128};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {};
    std::vector<std::string> commonFields = {};
    std::string re = "";
    try{
        dolphindb::EventSender* sender = new dolphindb::EventSender(conn, "", EventSchemas, eventTimeFields, commonFields);
    }catch(const std::exception& ex){
        re = ex.what();
    }
    ASSERT_EQ("Vectors in EventSchema must be of the same length.", re);
    std::string re1 = "";
    try{
        dolphindb::EventClient* client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
    }catch(const std::exception& ex){
        re1 = ex.what();
    }
    ASSERT_EQ("Vectors in EventSchema must be of the same length.", re1);
}

TEST_F(StreamingCEPEventTest, test_EventSender_AttrKeys_one_colume)
{
    std::string case_=getCaseName();
    conn->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as "+case_+"_inputTable;");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"time"};
    schema->fieldTypes_ = {dolphindb::DT_TIME};
    schema->fieldForms_ = {dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {"time"};
    std::vector<std::string> commonFields = {};
    dolphindb::EventSender *sender = new dolphindb::EventSender(conn, case_+"_inputTable", EventSchemas, eventTimeFields, commonFields);
    std::vector<dolphindb::ConstantSP> attributes = {dolphindb::Util::createTime(10,45,3,100)};
    sender->sendEvent("market", attributes);
    dolphindb::TableSP re = conn->run(case_+"_inputTable");
    ASSERT_EQ(1, re->rows());
    ASSERT_EQ("10:45:03.100", re->getColumn(0)->get(0)->getString());
}

TEST_F(StreamingCEPEventTest, test_AttrTypes_null)
{
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time", "decimal32", "decimal64", "decimal128"};
    schema->fieldTypes_ = {};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {};
    std::vector<std::string> commonFields = {};
    std::string re = "";
    try{
        dolphindb::EventSender* sender = new dolphindb::EventSender(conn, "", EventSchemas, eventTimeFields, commonFields);
    }catch(std::exception& ex){
        re = ex.what();
    }
    ASSERT_EQ("Vectors in EventSchema must be of the same length.", re);
    std::string re1 = "";
    try{
        dolphindb::EventClient* client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
    }catch(std::exception& ex){
        re1 = ex.what();
    }
    ASSERT_EQ("Vectors in EventSchema must be of the same length.", re1);
}

TEST_F(StreamingCEPEventTest, test_AttrForms_null)
{
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time", "decimal32", "decimal64", "decimal128"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME, dolphindb::DT_DECIMAL32, dolphindb::DT_DECIMAL64, dolphindb::DT_DECIMAL128};
    schema->fieldForms_={};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {};
    std::vector<std::string> commonFields = {};
    std::string re = "";
    try{
        dolphindb::EventSender* sender = new dolphindb::EventSender(conn, "", EventSchemas, eventTimeFields, commonFields);
    }catch(std::exception& ex){
        re = ex.what();
    }
    ASSERT_EQ("Vectors in EventSchema must be of the same length.", re);
    std::string re1 = "";
    try{
        dolphindb::EventClient* client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
    }catch(std::exception& ex){
        re1 = ex.what();
    }
    ASSERT_EQ("Vectors in EventSchema must be of the same length.", re1);
}

TEST_F(StreamingCEPEventTest, test_attrExtraParams_overflow)
{
    std::string case_=getCaseName();
    conn->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as "+case_+"_inputTable;");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time", "decimal32", "decimal64", "decimal128"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME, dolphindb::DT_DECIMAL32, dolphindb::DT_DECIMAL64, dolphindb::DT_DECIMAL128};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    schema->fieldExtraParams_ = {0, 0, 10, 19, 39};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {};
    std::vector<std::string> commonFields = {};
    std::string re = "";
    try{
        dolphindb::EventSender* sender = new dolphindb::EventSender(conn, case_+"_inputTable", EventSchemas, eventTimeFields, commonFields);
    }catch(std::exception& ex){
        re = ex.what();
    }
    ASSERT_EQ("Scale out of bound (valid range: [0, 9], but get: 10)", re);
    schema->fieldExtraParams_ = {0, 0, -1, -1, -1};
    std::string re1 = "";
    try{
        dolphindb::EventClient* client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
    }catch(std::exception& ex){
        re1 = ex.what();
    }
    ASSERT_EQ("Scale out of bound (valid range: [0, 9], but get: 10)", re1);
}

TEST_F(StreamingCEPEventTest, test_eventTimeFields_not_exist)
{
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time", "decimal32", "decimal64", "decimal128"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME, dolphindb::DT_DECIMAL32, dolphindb::DT_DECIMAL64, dolphindb::DT_DECIMAL128};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {"time1"};
    std::vector<std::string> commonFields = {};
    std::string re = "";
    try{
        dolphindb::EventSender* sender = new dolphindb::EventSender(conn, "", EventSchemas, eventTimeFields, commonFields);
    }catch(std::exception& ex){
        re = ex.what();
    }
    ASSERT_EQ("Event market doesn't contain eventTimeField time1", re);
    std::string re1 = "";
    try{
        dolphindb::EventClient* client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
    }catch(std::exception& ex){
        re1 = ex.what();
    }
    ASSERT_EQ("Event market doesn't contain eventTimeField time1", re1);
}

TEST_F(StreamingCEPEventTest, test_eventTimeFields_not_time_column)
{
    std::string case_=getCaseName();
    conn->run("share streamTable(1:0, `time`eventType`event, [STRING,STRING,BLOB]) as "+case_+"_inputTable;");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time", "decimal32", "decimal64", "decimal128"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_STRING, dolphindb::DT_DECIMAL32, dolphindb::DT_DECIMAL64, dolphindb::DT_DECIMAL128};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {"time"};
    std::vector<std::string> commonFields = {};
    std::string re = "";
    try{
        dolphindb::EventSender* sender = new dolphindb::EventSender(conn, case_+"_inputTable", EventSchemas, eventTimeFields, commonFields);
    }catch(std::exception& ex){
        re = ex.what();
    }
    ASSERT_EQ("The first column of the output table must be temporal if eventTimeField is specified.", re);
    std::string re1 = "";
    try{
        dolphindb::EventClient* client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
        client->subscribe(HOST, PORT, test_handler, case_+"_inputTable", DEFAULT_ACTION_NAME, 0, true, USER, PASSWD);
    }catch(std::exception& ex){
        re1 = ex.what();
    }
    ASSERT_EQ("The first column of the output table must be temporal if eventTimeField is specified.", re1);
}

TEST_F(StreamingCEPEventTest, test_eventTimeFields_two_column)
{
    std::string case_=getCaseName();
    conn->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as "+case_+"_inputTable;"
                "share streamTable(1:0, `market`time, [STRING,TIME]) as "+case_+"_outputTable;");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    dolphindb::EventSchema *schema1 = new dolphindb::EventSchema();
    schema1->eventType_ = "market1";
    schema1->fieldNames_ = {"time", "time1"};
    schema1->fieldTypes_ = {dolphindb::DT_TIME, dolphindb::DT_TIME};
    schema1->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema, *schema1};
    std::vector<std::string> eventTimeFields = {"time", "time1"};
    std::vector<std::string> commonFields = {};
    dolphindb::EventSender* sender = new dolphindb::EventSender(conn, case_+"_inputTable", EventSchemas, eventTimeFields, commonFields);
    dolphindb::EventClient* client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
    client->subscribe(HOST, PORT, test_handler, case_+"_inputTable", DEFAULT_ACTION_NAME, 0, true, USER, PASSWD);
    std::vector<dolphindb::ConstantSP> attributes = {dolphindb::Util::createString("str"), dolphindb::Util::createTime(12,45,3,100)};
    sender->sendEvent("market", attributes);
    std::vector<dolphindb::ConstantSP> attributes1 = {dolphindb::Util::createTime(1,1,20,100), dolphindb::Util::createTime(8,30,59,100)};
    sender->sendEvent("market1", attributes1);
    dolphindb::Util::sleep(2000);
    client->unsubscribe(HOST, PORT, case_+"_inputTable", DEFAULT_ACTION_NAME);
    std::vector<std::string> outs = subInputTable(case_+"_inputTable", 2, EventSchemas, eventTimeFields, commonFields);
    dolphindb::TableSP re = conn->run(outs[0]);
    dolphindb::TableSP re1 = conn->run(outs[1]);
    dolphindb::TableSP re2 = conn->run(case_+"_outputTable");
    ASSERT_EQ(1, re->rows());
    ASSERT_EQ("str", re->getColumn(0)->get(0)->getString());
    ASSERT_EQ("12:45:03.100", re->getColumn(1)->get(0)->getString());
    ASSERT_EQ(1, re1->rows());
    ASSERT_EQ("01:01:20.100", re1->getColumn(0)->get(0)->getString());
    ASSERT_EQ("08:30:59.100", re1->getColumn(1)->get(0)->getString());
    ASSERT_EQ(2, re2->rows());
    ASSERT_EQ("str", re2->getColumn(0)->get(0)->getString());
    ASSERT_EQ("12:45:03.100", re2->getColumn(1)->get(0)->getString());
    ASSERT_EQ("01:01:20.100", re2->getColumn(0)->get(1)->getString());
    ASSERT_EQ("08:30:59.100", re2->getColumn(1)->get(1)->getString());
    delete sender, schema, schema1, client;
}

TEST_F(StreamingCEPEventTest, test_EventClient_fields_null_but_subTable_has_column)
{
    std::string case_=getCaseName();
    conn->run("share streamTable(1:0, `time`eventType`event`common, [TIME,STRING,BLOB, INT]) as "+case_+"_inputTable;"
                "share streamTable(1:0, `market`time, [STRING,TIME]) as "+case_+"_outputTable;");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    dolphindb::EventClient* client = new dolphindb::EventClient(EventSchemas, {}, {});
    ASSERT_ANY_THROW(client->subscribe(HOST, PORT, test_handler, case_+"_inputTable", DEFAULT_ACTION_NAME, 0, true, USER, PASSWD));
    delete schema, client;
}

TEST_F(StreamingCEPEventTest, test_commonFields_one_column)
{
    std::string case_=getCaseName();
    conn->run("share streamTable(1:0, `time`eventType`event`time1, [TIME,STRING,BLOB,TIME]) as "+case_+"_inputTable;"
                "share streamTable(1:0, `market`time, [STRING,TIME]) as "+case_+"_outputTable;");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    dolphindb::EventSchema *schema1 = new dolphindb::EventSchema();
    schema1->eventType_ = "market1";
    schema1->fieldNames_ = {"time", "time1"};
    schema1->fieldTypes_ = {dolphindb::DT_TIME, dolphindb::DT_TIME};
    schema1->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema, *schema1};
    std::vector<std::string> eventTimeFields = {"time", "time1"};
    std::vector<std::string> commonFields = {"time"};
    dolphindb::EventSender* sender = new dolphindb::EventSender(conn, case_+"_inputTable", EventSchemas, eventTimeFields, commonFields);
    dolphindb::EventClient* client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
    client->subscribe(HOST, PORT, test_handler, case_+"_inputTable", DEFAULT_ACTION_NAME, 0, true, USER, PASSWD);
    std::vector<dolphindb::ConstantSP> attributes = {dolphindb::Util::createString("str"), dolphindb::Util::createTime(12,45,3,100)};
    sender->sendEvent("market", attributes);
    std::vector<dolphindb::ConstantSP> attributes1 = {dolphindb::Util::createTime(1,1,20,100), dolphindb::Util::createTime(8,30,59,100)};
    sender->sendEvent("market1", attributes1);
    dolphindb::Util::sleep(2000);
    client->unsubscribe(HOST, PORT, case_+"_inputTable", DEFAULT_ACTION_NAME);
    auto outs = subInputTable(case_+"_inputTable", 2, EventSchemas, eventTimeFields, commonFields);
    dolphindb::TableSP re = conn->run(outs[0]);
    dolphindb::TableSP re1 = conn->run(outs[1]);
    dolphindb::TableSP re2 = conn->run(case_+"_outputTable");
    ASSERT_EQ(1, re->rows());
    ASSERT_EQ("str", re->getColumn(0)->get(0)->getString());
    ASSERT_EQ("12:45:03.100", re->getColumn(1)->get(0)->getString());
    ASSERT_EQ(1, re1->rows());
    ASSERT_EQ("01:01:20.100", re1->getColumn(0)->get(0)->getString());
    ASSERT_EQ("08:30:59.100", re1->getColumn(1)->get(0)->getString());
    ASSERT_EQ(2, re2->rows());
    ASSERT_EQ("str", re2->getColumn(0)->get(0)->getString());
    ASSERT_EQ("12:45:03.100", re2->getColumn(1)->get(0)->getString());
    ASSERT_EQ("01:01:20.100", re2->getColumn(0)->get(1)->getString());
    ASSERT_EQ("08:30:59.100", re2->getColumn(1)->get(1)->getString());
}

TEST_F(StreamingCEPEventTest, test_eventSchema_exception)
{
    std::string case_=getCaseName();
    conn->run("share streamTable(1:0, `eventType`event, [STRING,BLOB]) as "+case_+"_inputTable;");
    dolphindb::EventSchema *schema1 = new dolphindb::EventSchema();
    schema1->eventType_ = "market";
    schema1->fieldNames_ = {"market", "error_col"};
    schema1->fieldTypes_ = {dolphindb::DT_STRING, (dolphindb::DATA_TYPE)-1}; // negative type
    schema1->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    dolphindb::EventSchema *schema2 = new dolphindb::EventSchema();
    schema2->eventType_ = "market";
    schema2->fieldNames_ = {"market", "error_col"};
    schema2->fieldTypes_ = {dolphindb::DT_STRING, (dolphindb::DATA_TYPE)110}; // type > dolphindb::DATA_TYPE::dolphindb::DT_OBJECT
    schema2->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas1 = {*schema1};
    std::vector<dolphindb::EventSchema> EventSchemas2 = {*schema2};
    ASSERT_ANY_THROW(dolphindb::EventSender(conn, "", EventSchemas1, {}, {}));
    ASSERT_ANY_THROW(dolphindb::EventSender(conn, "", EventSchemas2, {}, {}));
    ASSERT_ANY_THROW(dolphindb::EventClient(EventSchemas1, {}, {}));
    ASSERT_ANY_THROW(dolphindb::EventClient(EventSchemas2, {}, {}));
    delete schema1, schema2;
}


TEST_F(StreamingCEPEventTest, test_two_schemas_two_commonFields)
{
    std::string case_=getCaseName();
    conn->run("share streamTable(1:0, `eventType`event`time`sym, [STRING,BLOB,TIME,STRING]) as "+case_+"_inputTable;"
                "share streamTable(1:0, `market`time`sym, [STRING,TIME,STRING]) as "+case_+"_outputTable;");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time", "sym"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME, dolphindb::DT_STRING};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    dolphindb::EventSchema *schema1 = new dolphindb::EventSchema();
    schema1->eventType_ = "market1";
    schema1->fieldNames_ = {"market1", "time", "sym"};
    schema1->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME, dolphindb::DT_STRING};
    schema1->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema, *schema1};
    std::vector<std::string> eventTimeFields = {};
    std::vector<std::string> commonFields = {"time", "sym"};
    dolphindb::EventSender* sender = new dolphindb::EventSender(conn, case_+"_inputTable", EventSchemas, eventTimeFields, commonFields);
    dolphindb::EventClient* client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
    client->subscribe(HOST, PORT, test_handler, case_+"_inputTable", DEFAULT_ACTION_NAME, 0, true, USER, PASSWD);
    std::vector<dolphindb::ConstantSP> attributes = {dolphindb::Util::createString("sz"), dolphindb::Util::createTime(12,45,3,100), dolphindb::Util::createString("sz000001")};
    sender->sendEvent("market", attributes);
    std::vector<dolphindb::ConstantSP> attributes1 = {dolphindb::Util::createString("sh"), dolphindb::Util::createTime(8,30,59,100), dolphindb::Util::createString("sh000001")};
    sender->sendEvent("market1", attributes1);
    dolphindb::Util::sleep(2000);
    client->unsubscribe(HOST, PORT, case_+"_inputTable", DEFAULT_ACTION_NAME);
    auto outs = subInputTable(case_+"_inputTable", 2, EventSchemas, eventTimeFields, commonFields);
    dolphindb::TableSP re = conn->run(outs[0]);
    dolphindb::TableSP re1 = conn->run(outs[1]);
    dolphindb::TableSP re2 = conn->run(case_+"_outputTable");
    ASSERT_EQ(1, re->rows());
    ASSERT_EQ("sz", re->getColumn(0)->get(0)->getString());
    ASSERT_EQ("12:45:03.100", re->getColumn(1)->get(0)->getString());
    ASSERT_EQ("sz000001", re->getColumn(2)->get(0)->getString());
    ASSERT_EQ(1, re1->rows());
    ASSERT_EQ("sh", re1->getColumn(0)->get(0)->getString());
    ASSERT_EQ("08:30:59.100", re1->getColumn(1)->get(0)->getString());
    ASSERT_EQ("sh000001", re1->getColumn(2)->get(0)->getString());
    ASSERT_EQ(2, re2->rows());
    ASSERT_EQ("sz", re2->getColumn(0)->get(0)->getString());
    ASSERT_EQ("12:45:03.100", re2->getColumn(1)->get(0)->getString());
    ASSERT_EQ("sz000001", re2->getColumn(2)->get(0)->getString());
    ASSERT_EQ("sh", re2->getColumn(0)->get(1)->getString());
    ASSERT_EQ("08:30:59.100", re2->getColumn(1)->get(1)->getString());
    ASSERT_EQ("sh000001", re2->getColumn(2)->get(1)->getString());
    delete sender, schema, schema1;
}


TEST_F(StreamingCEPEventTest, test_commonFields_eventTimeFields_with_same_col)
{
    std::string case_=getCaseName();
    conn->run("share streamTable(1:0, `time`eventType`event`time2`sym, [TIME,STRING,BLOB,TIME,STRING]) as "+case_+"_inputTable;"
                "share streamTable(1:0, `market`time`sym, [STRING,TIME,STRING]) as "+case_+"_outputTable;");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time", "sym"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME, dolphindb::DT_STRING};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    dolphindb::EventSchema *schema1 = new dolphindb::EventSchema();
    schema1->eventType_ = "market1";
    schema1->fieldNames_ = {"market1", "time", "sym"};
    schema1->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME, dolphindb::DT_STRING};
    schema1->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema, *schema1};
    std::vector<std::string> eventTimeFields = {"time"};
    std::vector<std::string> commonFields = {"time", "sym"};
    dolphindb::EventSender* sender = new dolphindb::EventSender(conn, case_+"_inputTable", EventSchemas, eventTimeFields, commonFields);
    dolphindb::EventClient* client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
    client->subscribe(HOST, PORT, test_handler, case_+"_inputTable", DEFAULT_ACTION_NAME, 0, true, USER, PASSWD);
    std::vector<dolphindb::ConstantSP> attributes = {dolphindb::Util::createString("sz"), dolphindb::Util::createTime(12,45,3,100), dolphindb::Util::createString("sz000001")};
    sender->sendEvent("market", attributes);
    std::vector<dolphindb::ConstantSP> attributes1 = {dolphindb::Util::createString("sh"), dolphindb::Util::createTime(8,30,59,100), dolphindb::Util::createString("sh000001")};
    sender->sendEvent("market1", attributes1);
    dolphindb::Util::sleep(2000);
    client->unsubscribe(HOST, PORT, case_+"_inputTable", DEFAULT_ACTION_NAME);
    auto outs = subInputTable(case_+"_inputTable", 2, EventSchemas, eventTimeFields, commonFields);
    dolphindb::TableSP re = conn->run(outs[0]);
    dolphindb::TableSP re1 = conn->run(outs[1]);
    dolphindb::TableSP re2 = conn->run(case_+"_outputTable");
    ASSERT_EQ(1, re->rows());
    ASSERT_EQ("sz", re->getColumn(0)->get(0)->getString());
    ASSERT_EQ("12:45:03.100", re->getColumn(1)->get(0)->getString());
    ASSERT_EQ("sz000001", re->getColumn(2)->get(0)->getString());
    ASSERT_EQ(1, re1->rows());
    ASSERT_EQ("sh", re1->getColumn(0)->get(0)->getString());
    ASSERT_EQ("08:30:59.100", re1->getColumn(1)->get(0)->getString());
    ASSERT_EQ("sh000001", re1->getColumn(2)->get(0)->getString());
    ASSERT_EQ(2, re2->rows());
    ASSERT_EQ("sz", re2->getColumn(0)->get(0)->getString());
    ASSERT_EQ("12:45:03.100", re2->getColumn(1)->get(0)->getString());
    ASSERT_EQ("sz000001", re2->getColumn(2)->get(0)->getString());
    ASSERT_EQ("sh", re2->getColumn(0)->get(1)->getString());
    ASSERT_EQ("08:30:59.100", re2->getColumn(1)->get(1)->getString());
    ASSERT_EQ("sh000001", re2->getColumn(2)->get(1)->getString());
    delete sender, schema, schema1;
}

TEST_F(StreamingCEPEventTest, test_EventSender_connect_not_connect)
{
    std::string case_=getCaseName();
    conn->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as "+case_+"_inputTable;");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {"time"};
    std::vector<std::string> commonFields = {};
    std::string re = "";
    dolphindb::DBConnectionSP tmp_connsp = new dolphindb::DBConnection();
    try{
        dolphindb::EventSender* sender = new dolphindb::EventSender(tmp_connsp, case_+"_inputTable", EventSchemas, eventTimeFields, commonFields);
    }catch(std::exception& ex){
        re = ex.what();
    }
    ASSERT_EQ("Couldn't send script/function to the remote host because the connection has been closed", re);
    delete schema;
}

TEST_F(StreamingCEPEventTest, test_EventClient_sub_twice_with_same_actionName)
{
    std::string case_=getCaseName();
    conn->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as "+case_+"_inputTable;");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {"time"};
    std::vector<std::string> commonFields = {};
    dolphindb::EventClient* client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
    client->subscribe(HOST, PORT, test_handler, case_+"_inputTable", DEFAULT_ACTION_NAME, 0, true, USER, PASSWD, false);
    std::string re = "";
    try{
        client->subscribe(HOST, PORT, test_handler, case_+"_inputTable", DEFAULT_ACTION_NAME, 0, false, USER, PASSWD, false);
    }catch(std::exception& ex){
        re = ex.what();
    }
    auto pos = re.find("already be subscribed");
    ASSERT_TRUE(pos != std::string::npos);
    client->unsubscribe(HOST, PORT, case_+"_inputTable", DEFAULT_ACTION_NAME);
    delete client, schema;
}

TEST_F(StreamingCEPEventTest, test_EventSender_commonFields_cols_not_match)
{
    std::string case_=getCaseName();
    conn->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as "+case_+"_inputTable;");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time", "col1"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME, dolphindb::DT_INT};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {};
    std::vector<std::string> commonFields = {"time", "col1"};
    std::string re = "";
    try{
        dolphindb::EventSender* sender = new dolphindb::EventSender(conn, case_+"_inputTable", EventSchemas, eventTimeFields, commonFields);
    }catch(std::exception &ex){
        re = ex.what();
    }
    ASSERT_EQ("Incompatible outputTable columnns, expected: 4, got: 3", re);
    delete schema;
}

TEST_F(StreamingCEPEventTest, test_EventClient_commonFields_cols_not_match)
{
    std::string case_=getCaseName();
    conn->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as "+case_+"_inputTable;"
                "share streamTable(1:0, `market`time`col1, [STRING,TIME, INT]) as "+case_+"_outputTable;");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time", "col1"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME, dolphindb::DT_INT};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {"time"};
    std::vector<std::string> err_commonFields = {"time", "col2"};
    std::string re = "";
    try{
        dolphindb::EventClient* client = new dolphindb::EventClient(EventSchemas, eventTimeFields, err_commonFields);
    }catch(std::exception &ex){
        re = ex.what();
    }
    ASSERT_EQ("Event market doesn't contain commonField col2", re);
    delete schema;
}

TEST_F(StreamingCEPEventTest, test_EventSender_conn_asynchronousTask_true)
{
    std::string case_=getCaseName();
    conn->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as "+case_+"_inputTable;");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {"time"};
    std::vector<std::string> commonFields = {};
    dolphindb::DBConnectionSP tmp_connsp = new dolphindb::DBConnection(false, true);
    tmp_connsp->connect(HOST, PORT, USER, PASSWD);
    ASSERT_ANY_THROW(dolphindb::EventSender(tmp_connsp, case_+"_inputTable", EventSchemas, eventTimeFields, commonFields));
    delete schema, tmp_connsp;
}

TEST_F(StreamingCEPEventTest, test_EventSender_conn_ssl_true)
{
    std::string case_=getCaseName();
    conn->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as "+case_+"_inputTable;");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {"time"};
    std::vector<std::string> commonFields = {};
    dolphindb::DBConnectionSP tmp_connsp = new dolphindb::DBConnection(true, false);
    tmp_connsp->connect(HOST, PORT, USER, PASSWD);
    dolphindb::EventSender* sender = new dolphindb::EventSender(tmp_connsp, case_+"_inputTable", EventSchemas, eventTimeFields, commonFields);
    std::vector<dolphindb::ConstantSP> attributes = {dolphindb::Util::createString("sz"), dolphindb::Util::createTime(12,45,3,100)};
    sender->sendEvent("market", attributes);
    auto outs = subInputTable(case_+"_inputTable", 1, EventSchemas, eventTimeFields, commonFields);
    dolphindb::TableSP re = conn->run(outs[0]);
    ASSERT_EQ(1, re->rows());
    ASSERT_EQ("sz", re->getColumn(0)->get(0)->getString());
    ASSERT_EQ("12:45:03.100", re->getColumn(1)->get(0)->getString());
    delete sender, schema, tmp_connsp;
}

TEST_F(StreamingCEPEventTest, test_EventSender_conn_compress_true)
{
    std::string case_=getCaseName();
    conn->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as "+case_+"_inputTable;");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {"time"};
    std::vector<std::string> commonFields = {};
    dolphindb::DBConnectionSP tmp_connsp = new dolphindb::DBConnection(false, false, 7200, true);
    tmp_connsp->connect(HOST, PORT, USER, PASSWD);
    dolphindb::EventSender* sender = new dolphindb::EventSender(tmp_connsp, case_+"_inputTable", EventSchemas, eventTimeFields, commonFields);
    std::vector<dolphindb::ConstantSP> attributes = {dolphindb::Util::createString("sz"), dolphindb::Util::createTime(12,45,3,100)};
    sender->sendEvent("market", attributes);
    auto outs = subInputTable(case_+"_inputTable", 1, EventSchemas, eventTimeFields, commonFields);
    dolphindb::TableSP re = conn->run(outs[0]);
    ASSERT_EQ(1, re->rows());
    ASSERT_EQ("sz", re->getColumn(0)->get(0)->getString());
    ASSERT_EQ("12:45:03.100", re->getColumn(1)->get(0)->getString());
    delete sender, schema, tmp_connsp;
}

TEST_F(StreamingCEPEventTest, test_EventSender_conn_not_admin)
{
    std::string case_=getCaseName();
    conn->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as "+case_+"_inputTable;");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {"time"};
    std::vector<std::string> commonFields = {};
    conn->run("try{createUser(`yzou, `123456)}catch(ex){};go");
    dolphindb::DBConnectionSP tmp_connsp = new dolphindb::DBConnection(false, false, 7200, false);
    tmp_connsp->connect(HOST, PORT, "yzou", "123456");
    ASSERT_EQ(tmp_connsp->run("getCurrentSessionAndUser()[1]")->getString(), "yzou");
    dolphindb::EventSender* sender = new dolphindb::EventSender(tmp_connsp, case_+"_inputTable", EventSchemas, eventTimeFields, commonFields);
    std::vector<dolphindb::ConstantSP> attributes = {dolphindb::Util::createString("sz"), dolphindb::Util::createTime(12,45,3,100)};
    sender->sendEvent("market", attributes);
    auto outs = subInputTable(case_+"_inputTable", 1, EventSchemas, eventTimeFields, commonFields);
    dolphindb::TableSP re = conn->run("select * from " + outs[0]);
    ASSERT_EQ(1, re->rows());
    ASSERT_EQ("sz", re->getColumn(0)->get(0)->getString());
    ASSERT_EQ("12:45:03.100", re->getColumn(1)->get(0)->getString());
    delete sender, schema, tmp_connsp;
}

TEST_F(StreamingCEPEventTest, test_eventTable_not_exist)
{
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {"time"};
    std::vector<std::string> commonFields = {};
    std::string re = "";
    try{
        dolphindb::EventSender* sender = new dolphindb::EventSender(conn, "notExistTable", EventSchemas, eventTimeFields, commonFields);
    }catch(const std::exception& ex){
        re = ex.what();
    }
    ASSERT_TRUE(re.find("Can't find the object with name notExistTable") != std::string::npos);
    dolphindb::EventClient* client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
    std::string re1 = "";
    try{
        auto th = client->subscribe(HOST, PORT, test_handler, "notExistTable", DEFAULT_ACTION_NAME, 0, true, USER, PASSWD, false);
        th->join();
    }catch(const std::exception& ex){
        re1 = ex.what();
    }
    ASSERT_PRED_FORMAT2(testing::IsSubstring,"Can't find the object with name notExistTable", re1);
    delete schema, client;
}

TEST_F(StreamingCEPEventTest, test_eventTable_not_contail_eventCol)
{
    std::string case_=getCaseName();
    conn->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,STRING]) as "+case_+"_inputTable;");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {"time"};
    std::vector<std::string> commonFields = {};
    std::string re = "";
    try{
        dolphindb::EventSender* sender = new dolphindb::EventSender(conn, case_+"_inputTable", EventSchemas, eventTimeFields, commonFields);
    }catch(const std::exception& ex){
        re = ex.what();
    }
    ASSERT_TRUE(re.find("The event column of the output table must be BLOB type") != std::string::npos);
    dolphindb::EventClient* client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
    std::string re1 = "";
    try{
        auto th = client->subscribe(HOST, PORT, test_handler, case_+"_inputTable", DEFAULT_ACTION_NAME, 0, true, USER, PASSWD, false);
        th->join();
    }catch(const std::exception& ex){
        re1 = ex.what();
    }
    ASSERT_PRED_FORMAT2(testing::IsSubstring,"The event column of the output table must be BLOB type.", re1);
    delete schema, client;
}

TEST_F(StreamingCEPEventTest, test_connect_tableName_null)
{
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {"time"};
    std::vector<std::string> commonFields = {};
    std::string re = "";
    try{
        dolphindb::EventSender* sender = new dolphindb::EventSender(conn, "", EventSchemas, eventTimeFields, commonFields);
    }catch(std::exception& ex){
        re = ex.what();
    }
    ASSERT_EQ("tableName must not be empty.", re);
    dolphindb::EventClient* client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
    std::string re1 = "";
    try{
        auto th = client->subscribe(HOST, PORT, test_handler, "", DEFAULT_ACTION_NAME, 0, true, USER, PASSWD, false);
        th->join();
    }catch(std::exception& ex){
        re1 = ex.what();
    }
    ASSERT_EQ("tableName must not be empty.", re1);
    delete client, schema;
}

TEST_F(StreamingCEPEventTest, test_EventSender_sendEvent_eventType_not_exist)
{
    std::string case_=getCaseName();
    conn->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as "+case_+"_inputTable;");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {"time"};
    std::vector<std::string> commonFields = {};
    dolphindb::EventSender* sender = new dolphindb::EventSender(conn, case_+"_inputTable", EventSchemas, eventTimeFields, commonFields);
    std::string re = "";
    try{
        sender->sendEvent("market1", {dolphindb::Util::createString("sz"), dolphindb::Util::createTime(12,45,3,100)});
    }catch(std::exception& ex){
        re = ex.what();
    }
    ASSERT_EQ("serialize event Fail for unknown eventType market1", re);
    delete sender, schema;
}

TEST_F(StreamingCEPEventTest, test_EventSender_sendEvent_eventType_null)
{
    std::string case_=getCaseName();
    conn->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as "+case_+"_inputTable;");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {"time"};
    std::vector<std::string> commonFields = {};
    dolphindb::EventSender* sender = new dolphindb::EventSender(conn, case_+"_inputTable", EventSchemas, eventTimeFields, commonFields);
    std::string re = "";
    try{
        sender->sendEvent("", {dolphindb::Util::createString("sz"), dolphindb::Util::createTime(12,45,3,100)});
    }catch(std::exception& ex){
        re = ex.what();
    }
    ASSERT_EQ("serialize event Fail for unknown eventType ", re);
    delete sender, schema;
}

TEST_F(StreamingCEPEventTest, test_EventClient_sub_eventType_null)
{
    std::string case_=getCaseName();
    conn->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as "+case_+"_inputTable;");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {"time"};
    std::vector<std::string> commonFields = {};
    std::string re = "";
    try{
        dolphindb::EventClient* client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
        auto th = client->subscribe(HOST, PORT, test_handler, case_+"_inputTable", DEFAULT_ACTION_NAME, 0, true, USER, PASSWD);
        th->join();
    }catch(std::exception& ex){
        re = ex.what();
    }
    ASSERT_EQ("eventType must not be empty.", re);
    delete schema;
}

TEST_F(StreamingCEPEventTest, test_EventSender_sendEvent_attributes_column_not_match)
{
    std::string case_=getCaseName();
    conn->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as "+case_+"_inputTable;");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {"time"};
    std::vector<std::string> commonFields = {};
    dolphindb::EventSender* sender = new dolphindb::EventSender(conn, case_+"_inputTable", EventSchemas, eventTimeFields, commonFields);
    std::string re = "";
    try{
        sender->sendEvent("market", {dolphindb::Util::createString("sz"), dolphindb::Util::createTime(12,45,3,100), dolphindb::Util::createString("sz")});
    }catch(const std::exception& ex){
        re = ex.what();
    }
    ASSERT_EQ("serialize event Fail for the number of event values does not match market", re);
    delete sender, schema;
}

TEST_F(StreamingCEPEventTest, test_EventSender_sendEvent_attributes_type_not_match)
{
    std::string case_=getCaseName();
    conn->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as "+case_+"_inputTable;");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {"time"};
    std::vector<std::string> commonFields = {};
    dolphindb::EventSender* sender = new dolphindb::EventSender(conn, case_+"_inputTable", EventSchemas, eventTimeFields, commonFields);
    std::string re = "";
    try{
        sender->sendEvent("market", {dolphindb::Util::createString("sz"), dolphindb::Util::createInt(12)});
    }catch(const std::exception& ex){
        re = ex.what();
    }
    ASSERT_EQ("serialize event Fail for Expected type for the field time of market : TIME, but now it is INT", re);
    delete sender, schema;
}

class StreamingCEPEventTest_EventSender_alltypes : public StreamingCEPEventTest, public ::testing::WithParamInterface<std::tuple<dolphindb::DATA_TYPE, std::string>> {
public:
    static std::vector<std::tuple<dolphindb::DATA_TYPE, std::string>> get_scalar_data(){
        return {
            std::make_tuple(dolphindb::DT_BOOL, "array(BOOL).append!(true false NULL)"),std::make_tuple(dolphindb::DT_CHAR, "-127c 127c NULL"),
            std::make_tuple(dolphindb::DT_SHORT, "1000h NULL"),std::make_tuple(dolphindb::DT_INT, "10000 NULL"),
            std::make_tuple(dolphindb::DT_LONG, "1000000l NULL"),std::make_tuple(dolphindb::DT_FLOAT, "100.000f NULL"),
            std::make_tuple(dolphindb::DT_DOUBLE, "100.0000 NULL"),std::make_tuple(dolphindb::DT_DATE, "2023.01.01 NULL"),
            std::make_tuple(dolphindb::DT_MONTH, "2023.01M NULL"),std::make_tuple(dolphindb::DT_TIME, "23:45:03.100 NULL"),
            std::make_tuple(dolphindb::DT_MINUTE, "23:45m NULL"),std::make_tuple(dolphindb::DT_SECOND, "23:45:03 NULL"),
            std::make_tuple(dolphindb::DT_DATEHOUR, "datehour('2021.01.01T12'`)"),std::make_tuple(dolphindb::DT_STRING, "`szstr`"),
            std::make_tuple(dolphindb::DT_DATETIME, "2021.01.01T12:45:03 NULL"),std::make_tuple(dolphindb::DT_TIMESTAMP, "2021.01.01T12:45:03.100 NULL"),
            std::make_tuple(dolphindb::DT_NANOTIME, "23:45:03.100000000 NULL"),std::make_tuple(dolphindb::DT_NANOTIMESTAMP, "2021.01.01T12:45:03.100000000 NULL"),
            // std::make_tuple(dolphindb::DT_SYMBOL, "`sz`sh`bj`"), // symbol scalar not support in server
            std::make_tuple(dolphindb::DT_UUID, "uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87''5d212a78-cc48-e3b1-4235-b4d91473ee10'`)"),
            std::make_tuple(dolphindb::DT_IP, "ipaddr('127.0.0.1''1.1.1.1'`)"),
            std::make_tuple(dolphindb::DT_INT128, "int128(`e1671797c52e15f763380b45e841ec32`e1671797c52e15f763380b45e841ec42`)"),
            std::make_tuple(dolphindb::DT_BLOB, "blob(`blob`blob2`)"),
            std::make_tuple(dolphindb::DT_DECIMAL32, "decimal32('1.123456789'`'-135.1', 5)"),std::make_tuple(dolphindb::DT_DECIMAL64, "decimal64('-11.456464'`'300.1', 15)"),
            std::make_tuple(dolphindb::DT_DECIMAL128, "decimal128('999.64621462333'`'-1326', 25)"),
        };
    };
};

INSTANTIATE_TEST_SUITE_P(EventSender_alltype, StreamingCEPEventTest_EventSender_alltypes, testing::ValuesIn(StreamingCEPEventTest_EventSender_alltypes::get_scalar_data()));
TEST_P(StreamingCEPEventTest_EventSender_alltypes, test_EventSender_alltype)
{
    std::string case_=getCaseName();
    dolphindb::DATA_TYPE dataType = std::get<0>(GetParam());
    std::string type_data_str = std::get<1>(GetParam());
    std::string typeString = dolphindb::Util::getDataTypeString(dataType);
    int extraParam = 0;
    if (typeString.compare(0, 9, "DECIMAL32") == 0){typeString = "DECIMAL32(5)"; extraParam = 5;}
    else if (typeString.compare(0, 9, "DECIMAL64") == 0){typeString = "DECIMAL64(15)"; extraParam = 15;}
    else if (typeString.compare(0, 10, "DECIMAL128") == 0){typeString = "DECIMAL128(25)"; extraParam = 25;}
    conn->run("share streamTable(1:0, `eventTime`eventType`event`commonCol, [TIMESTAMP,STRING,BLOB,"+typeString+"]) as "+case_+"_fromApi;"
                "share streamTable(1:0, `eventTime`eventType`event`commonCol, [TIMESTAMP,STRING,BLOB,"+typeString+"]) as "+case_+"_fromCEP;go;"
                "class md{"
                    "market :: STRING\n"
                    "eventTime :: TIMESTAMP\n"
                    "testCol :: "+typeString+"\n"
                    "commonCol :: "+typeString+"\n"
                    "def md(a,b,c,d){"
                        "market = a\n"
                        "eventTime = b\n"
                        "testCol = c\n"
                        "commonCol = d\n}"
                "};"
                "class MainMonitor:CEPMonitor{"
                    "def MainMonitor(){}\n"
                    "def updateMarketData(event)\n"
                    "def onload(){"
                        "addEventListener(updateMarketData,'md')}\n"
                    "def updateMarketData(event){"
                        "emitEvent(event)}};"
                "try{dropStreamEngine(`"+case_+"_os)}catch(ex){};go;"
                "outputSerializer = streamEventSerializer(name=`"+case_+"_os, EventSchema=md, outputTable="+case_+"_fromCEP, eventTimeField = 'eventTime', commonField='commonCol');"
                "try{dropStreamEngine(`"+case_+"_CEPengine)}catch(ex){};go;"
                "engine = createCEPEngine(`"+case_+"_CEPengine, <MainMonitor()>, "+case_+"_fromCEP, [md], 1, 'eventTime', 10000, outputSerializer);");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "md";
    schema->fieldNames_ = {"market", "time", "testCol", "commonCol"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIMESTAMP, dataType, dataType};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    schema->fieldExtraParams_ = {0, 0, extraParam, extraParam};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {"time"};
    std::vector<std::string> commonFields = {"commonCol"};
    dolphindb::EventSender* sender = new dolphindb::EventSender(conn, case_+"_fromApi", EventSchemas, eventTimeFields, commonFields);
    int rows = 1000;
    dolphindb::VectorSP col0 = conn->run("market =rand(`sz`sh`bj`adk, "+std::to_string(rows)+");market");
    dolphindb::VectorSP col1 = conn->run("time = timestamp(rand(999000000000l, "+std::to_string(rows)+"));time");
    dolphindb::VectorSP col_test = conn->run("testCol = rand("+type_data_str+", "+std::to_string(rows)+");commonCol=testCol;testCol");
    if (dataType == dolphindb::DT_BLOB){
        col_test = conn->run("testCol = blob(string(rand(`blob`blob2`, "+std::to_string(rows)+")));commonCol=testCol;testCol");
    }
    for (int i = 0; i < rows; i++){
        std::vector<dolphindb::ConstantSP> attributes = {col0->get(i), col1->get(i), col_test->get(i), col_test->get(i)};
        sender->sendEvent("md", attributes);
        conn->run("ind="+std::to_string(i)+";rowData = md(market[ind], time[ind], testCol[ind], commonCol[ind]);appendEvent(outputSerializer, [rowData])");
    }
    ASSERT_TRUE(conn->run("all(each(eqObj, "+case_+"_fromApi.values(), "+case_+"_fromCEP.values()))")->getBool());
    conn->run("dropStreamEngine(`"+case_+"_CEPengine);dropStreamEngine(`"+case_+"_os);");
    delete sender, schema;
}

class StreamingCEPEventTest_EventSender_vector : public StreamingCEPEventTest, public ::testing::WithParamInterface<std::tuple<dolphindb::DATA_TYPE, std::string>> {
public:
    static std::vector<std::tuple<dolphindb::DATA_TYPE, std::string>> get_vector_data(){
        return {
            std::make_tuple(dolphindb::DT_BOOL, "true false"),std::make_tuple(dolphindb::DT_CHAR, "rand(127c,2)"),
            std::make_tuple(dolphindb::DT_SHORT, "rand(1000h,2)"),std::make_tuple(dolphindb::DT_INT, "rand(10000,2)"),
            std::make_tuple(dolphindb::DT_LONG, "rand(1000000l,2)"),std::make_tuple(dolphindb::DT_FLOAT, "rand(100.000f,2)"),
            std::make_tuple(dolphindb::DT_DOUBLE, "rand(100.0000,2)"),std::make_tuple(dolphindb::DT_DATE, "rand(2023.01.01,2)"),
            std::make_tuple(dolphindb::DT_MONTH, "rand(2023.01M,2)"),std::make_tuple(dolphindb::DT_TIME, "rand(23:45:03.100,2)"),
            std::make_tuple(dolphindb::DT_MINUTE, "rand(23:45m,2)"),std::make_tuple(dolphindb::DT_SECOND, "rand(23:45:03,2)"),
            std::make_tuple(dolphindb::DT_DATEHOUR, "rand(datehour('2021.01.01T12'),2)"),std::make_tuple(dolphindb::DT_DATETIME, "rand(2021.01.01T12:45:03,2)"),
            std::make_tuple(dolphindb::DT_TIMESTAMP, "rand(2021.01.01T12:45:03.100,2)"),std::make_tuple(dolphindb::DT_NANOTIME, "rand(23:45:03.100000000,2)"),
            std::make_tuple(dolphindb::DT_NANOTIMESTAMP, "rand(2021.01.01T12:45:03.100000000,2)"),
            std::make_tuple(dolphindb::DT_STRING, "rand(`sz`sh`bj,2)"),
            std::make_tuple(dolphindb::DT_SYMBOL, "symbol(rand(`sz`sh`bj,2))"),
            std::make_tuple(dolphindb::DT_UUID, "rand(uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87''5d212a78-cc48-e3b1-4235-b4d91473ee10'),2)"),
            std::make_tuple(dolphindb::DT_IP, "rand(ipaddr('1.1.1.1''127.0.0.1'),2)"),
            std::make_tuple(dolphindb::DT_INT128, "rand(int128(`e1671797c52e15f763380b45e841ec32`e1671797c52e15f763380b45e841ec42),2)"),
            std::make_tuple(dolphindb::DT_BLOB, "blob(`blob`blob2)"),
            std::make_tuple(dolphindb::DT_DECIMAL32, "rand(decimal32('1.123456789'`0'-135.1', 5),2)"),std::make_tuple(dolphindb::DT_DECIMAL64, "rand(decimal64('-11.456464'`0'300.1', 15),2)"),
            std::make_tuple(dolphindb::DT_DECIMAL128, "rand(decimal128('999.64621462333'`0'-1326', 25),2)"),
        };
    };
    static std::vector<std::tuple<dolphindb::DATA_TYPE, std::string>> get_vector_allNull_data(){
        return {
            std::make_tuple(dolphindb::DT_BOOL, "array(BOOL).append!(take(bool(NULL), 2))"), std::make_tuple(dolphindb::DT_CHAR, "array(CHAR).append!(take(char(NULL), 2))"),
            std::make_tuple(dolphindb::DT_SHORT, "array(SHORT).append!(take(short(NULL), 2))"), std::make_tuple(dolphindb::DT_INT, "array(INT).append!(take(int(NULL), 2))"),
            std::make_tuple(dolphindb::DT_LONG, "array(LONG).append!(take(long(NULL), 2))"), std::make_tuple(dolphindb::DT_FLOAT, "array(FLOAT).append!(take(float(NULL), 2))"),
            std::make_tuple(dolphindb::DT_DOUBLE, "array(DOUBLE).append!(take(double(NULL), 2))"), std::make_tuple(dolphindb::DT_DATE, "array(DATE).append!(take(date(NULL), 2))"),
            std::make_tuple(dolphindb::DT_MONTH, "array(MONTH).append!(take(month(NULL), 2))"), std::make_tuple(dolphindb::DT_TIME, "array(TIME).append!(take(time(NULL), 2))"),
            std::make_tuple(dolphindb::DT_MINUTE, "array(MINUTE).append!(take(minute(NULL), 2))"), std::make_tuple(dolphindb::DT_SECOND, "array(SECOND).append!(take(second(NULL), 2))"),
            std::make_tuple(dolphindb::DT_DATEHOUR, "array(DATEHOUR).append!(take(datehour(NULL), 2))"), std::make_tuple(dolphindb::DT_DATETIME, "array(DATETIME).append!(take(datetime(NULL), 2))"),
            std::make_tuple(dolphindb::DT_TIMESTAMP, "array(TIMESTAMP).append!(take(timestamp(NULL), 2))"), std::make_tuple(dolphindb::DT_NANOTIME, "array(NANOTIME).append!(take(nanotime(NULL), 2))"),
            std::make_tuple(dolphindb::DT_NANOTIMESTAMP, "array(NANOTIMESTAMP).append!(take(nanotimestamp(NULL), 2))"),
            std::make_tuple(dolphindb::DT_STRING, "array(string).append!(take(string(NULL), 2))"),
            std::make_tuple(dolphindb::DT_SYMBOL, "array(SYMBOL).append!(take(symbol(\"\"\"\"), 2))"),
            std::make_tuple(dolphindb::DT_UUID, "array(UUID).append!(take(uuid(''), 2))"),std::make_tuple(dolphindb::DT_IP, "array(IPADDR).append!(take(ipaddr(''), 2))"),
            std::make_tuple(dolphindb::DT_INT128, "array(INT128).append!(take(int128(''), 2))"), std::make_tuple(dolphindb::DT_BLOB, "array(BLOB).append!(take(blob(''), 2))"),
            std::make_tuple(dolphindb::DT_DECIMAL32, "array(DECIMAL32(5)).append!(take(decimal32(NULL,5), 2))"), std::make_tuple(dolphindb::DT_DECIMAL64, "array(DECIMAL64(15)).append!(take(decimal64(NULL,15), 2))"),
            std::make_tuple(dolphindb::DT_DECIMAL128, "array(DECIMAL128(25)).append!(take(decimal128(NULL,25), 2))"),
        };
    };
};

INSTANTIATE_TEST_SUITE_P(vector_noNull, StreamingCEPEventTest_EventSender_vector, testing::ValuesIn(StreamingCEPEventTest_EventSender_vector::get_vector_data()));
INSTANTIATE_TEST_SUITE_P(vector_allNull, StreamingCEPEventTest_EventSender_vector, testing::ValuesIn(StreamingCEPEventTest_EventSender_vector::get_vector_allNull_data()));
TEST_P(StreamingCEPEventTest_EventSender_vector, test_EventSender_vector)
{
    std::string case_=getCaseName();
    dolphindb::DATA_TYPE dataType = std::get<0>(GetParam());
    std::string type_data_str = std::get<1>(GetParam());
    std::string typeString = dolphindb::Util::getDataTypeString(dataType);
    int extraParam = 0;
    if (typeString.compare(0, 9, "DECIMAL32") == 0){typeString = "DECIMAL32(5)"; extraParam = 5;}
    else if (typeString.compare(0, 9, "DECIMAL64") == 0){typeString = "DECIMAL64(15)"; extraParam = 15;}
    else if (typeString.compare(0, 10, "DECIMAL128") == 0){typeString = "DECIMAL128(25)"; extraParam = 25;}
    std::string formatStr1 ="", formatStr2 ="";
    if (dataType != dolphindb::DT_SYMBOL && dataType != dolphindb::DT_STRING && dataType != dolphindb::DT_BLOB){
        formatStr1 = typeString + "[]";
        formatStr2 = typeString + " VECTOR";
    }else{
        formatStr1 = "INT[]";
        formatStr2 = "INT VECTOR";
    }
    conn->run("share streamTable(1:0, `eventTime`eventType`event`commonCol, [TIMESTAMP,STRING,BLOB,"+formatStr1+"]) as "+case_+"_fromApi;"
                "share streamTable(1:0, `eventTime`eventType`event`commonCol, [TIMESTAMP,STRING,BLOB,"+formatStr1+"]) as "+case_+"_fromCEP;go;"
                "class md{"
                    "market :: STRING\n"
                    "eventTime :: TIMESTAMP\n"
                    "testCol :: "+typeString+" VECTOR\n"
                    "commonCol :: "+formatStr2+"\n"
                    "def md(a,b,c,d){"
                        "market = a\n"
                        "eventTime = b\n"
                        "testCol = c\n"
                        "commonCol = d\n}"
                "};"
                "class MainMonitor:CEPMonitor{"
                    "def MainMonitor(){}\n"
                    "def updateMarketData(event)\n"
                    "def onload(){"
                        "addEventListener(updateMarketData,'md')}\n"
                    "def updateMarketData(event){"
                        "emitEvent(event)}};"
                "try{dropStreamEngine(`"+case_+"_os)}catch(ex){};go;"
                "outputSerializer = streamEventSerializer(name=`"+case_+"_os, EventSchema=md, outputTable="+case_+"_fromCEP, eventTimeField = 'eventTime', commonField='commonCol');"
                "try{dropStreamEngine(`"+case_+"_CEPengine)}catch(ex){};go;"
                "engine = createCEPEngine(`"+case_+"_CEPengine, <MainMonitor()>, "+case_+"_fromCEP, [md], 1, 'eventTime', 10000, outputSerializer);");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "md";
    schema->fieldNames_ = {"market", "time", "testCol", "commonCol"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIMESTAMP, dataType, dataType == dolphindb::DT_SYMBOL||dataType == dolphindb::DT_STRING||dataType == dolphindb::DT_BLOB? dolphindb::DT_INT:dataType};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_VECTOR, dolphindb::DF_VECTOR};
    schema->fieldExtraParams_ = {0, 0, extraParam, extraParam};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {"time"};
    std::vector<std::string> commonFields = {"commonCol"};
    dolphindb::EventSender* sender = new dolphindb::EventSender(conn, case_+"_fromApi", EventSchemas, eventTimeFields, commonFields);
    int rows = 1000;
    dolphindb::VectorSP col0 = conn->run("market =rand(`sz`sh`bj`adk, "+std::to_string(rows)+");market");
    dolphindb::VectorSP col1 = conn->run("time = timestamp(rand(999000000000l, "+std::to_string(rows)+"));time");
    dolphindb::VectorSP col2 = conn->run("testCol = rand([rand("+type_data_str+", rand(2,1)[0])], "+std::to_string(rows)+");testCol");
    dolphindb::VectorSP col3 = conn->run("commonCol = rand([rand("+type_data_str+", rand(2,1)[0])], "+std::to_string(rows)+");commonCol");
    if (dataType == dolphindb::DT_BLOB)
        col2 = conn->run("testCol = rand([blob(string(rand(`blob`blob2, rand(2,1)[0])))], "+std::to_string(rows)+");testCol");
    if (dataType == dolphindb::DT_SYMBOL)
        col2 = conn->run("testCol = rand([symbol(string(rand([\"blob\"], 10)))], "+std::to_string(rows)+");testCol");
    if (dataType == dolphindb::DT_STRING || dataType == dolphindb::DT_SYMBOL || dataType == dolphindb::DT_BLOB)
        col3 = conn->run("commonCol = rand([rand(10000, rand(2,1)[0])], "+std::to_string(rows)+");commonCol");
    for (int i = 0; i < rows; i++){
        std::vector<dolphindb::ConstantSP> attributes = {col0->get(i), col1->get(i), col2->get(i), col3->get(i)};
        sender->sendEvent("md", attributes);
        conn->run("ind="+std::to_string(i)+";rowData = md(market[ind], time[ind], testCol[ind], commonCol[ind]);appendEvent(outputSerializer, [rowData])");
    }
    ASSERT_TRUE(conn->run("all(each(eqObj, "+case_+"_fromApi.values(), "+case_+"_fromCEP.values()))")->getBool());
    conn->run("dropStreamEngine(`"+case_+"_CEPengine);dropStreamEngine(`"+case_+"_os);");
    delete sender, schema;
}

class StreamingCEPEventTest_EventSender_arrayVector : public StreamingCEPEventTest, public ::testing::WithParamInterface<std::tuple<dolphindb::DATA_TYPE, std::string>> {
public:
    static std::vector<std::tuple<dolphindb::DATA_TYPE, std::string>> get_arrayVector_data(){
        return {
            std::make_tuple(dolphindb::DT_BOOL_ARRAY, "array(BOOL[]).append!([true false])"), std::make_tuple(dolphindb::DT_CHAR_ARRAY, "array(CHAR[]).append!([1c NULL])"),
            std::make_tuple(dolphindb::DT_SHORT_ARRAY, "array(SHORT[]).append!([1000h NULL])"), std::make_tuple(dolphindb::DT_INT_ARRAY, "array(INT[]).append!([10000 NULL])"),
            std::make_tuple(dolphindb::DT_LONG_ARRAY, "array(LONG[]).append!([1000000l NULL])"), std::make_tuple(dolphindb::DT_FLOAT_ARRAY, "array(FLOAT[]).append!([100.000f NULL])"),
            std::make_tuple(dolphindb::DT_DOUBLE_ARRAY, "array(DOUBLE[]).append!([100.0000 NULL])"), std::make_tuple(dolphindb::DT_DATE_ARRAY, "array(DATE[]).append!([2023.01.01 NULL])"),
            std::make_tuple(dolphindb::DT_MONTH_ARRAY, "array(MONTH[]).append!([2023.01M NULL])"), std::make_tuple(dolphindb::DT_TIME_ARRAY, "array(TIME[]).append!([23:45:03.100 NULL])"),
            std::make_tuple(dolphindb::DT_MINUTE_ARRAY, "array(MINUTE[]).append!([23:45m NULL])"), std::make_tuple(dolphindb::DT_SECOND_ARRAY, "array(SECOND[]).append!([23:45:03 NULL])"),
            std::make_tuple(dolphindb::DT_DATEHOUR_ARRAY, "array(DATEHOUR[]).append!([datehour('2021.01.01T12'`)])"),
            std::make_tuple(dolphindb::DT_DATETIME_ARRAY, "array(DATETIME[]).append!([2021.01.01T12:45:03 NULL])"),
            std::make_tuple(dolphindb::DT_TIMESTAMP_ARRAY, "array(TIMESTAMP[]).append!([2021.01.01T12:45:03.100 NULL])"),
            std::make_tuple(dolphindb::DT_NANOTIME_ARRAY, "array(NANOTIME[]).append!([23:45:03.100000000 NULL])"),
            std::make_tuple(dolphindb::DT_NANOTIMESTAMP_ARRAY, "array(NANOTIMESTAMP[]).append!([2021.01.01T12:45:03.100000000 NULL])"),
            std::make_tuple(dolphindb::DT_UUID_ARRAY, "array(UUID[]).append!([uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87'`)])"),
            std::make_tuple(dolphindb::DT_IP_ARRAY, "array(IPADDR[]).append!([ipaddr('1.1.1.1'`)])"),
            std::make_tuple(dolphindb::DT_INT128_ARRAY, "array(INT128[]).append!([int128(`e1671797c52e15f763380b45e841ec32`)])"),
            std::make_tuple(dolphindb::DT_DECIMAL32_ARRAY, "array(DECIMAL32(5)[]).append!([decimal32('1.123456789'`, 5)])"),
            std::make_tuple(dolphindb::DT_DECIMAL64_ARRAY, "array(DECIMAL64(15)[]).append!([decimal64('-11.456464'`, 15)])"),
            std::make_tuple(dolphindb::DT_DECIMAL128_ARRAY, "array(DECIMAL128(25)[]).append!([decimal128('999.64621462333'`, 25)])"),
        };
    };
    static std::vector<std::tuple<dolphindb::DATA_TYPE, std::string>> get_arrayVector_allNull_data(){
        return {
            std::make_tuple(dolphindb::DT_BOOL_ARRAY, "array(BOOL[]).append!([take(bool(NULL), 2)])"), std::make_tuple(dolphindb::DT_CHAR_ARRAY, "array(CHAR[]).append!([take(char(NULL), 2)])"),
            std::make_tuple(dolphindb::DT_SHORT_ARRAY, "array(SHORT[]).append!([take(short(NULL), 2)])"), std::make_tuple(dolphindb::DT_INT_ARRAY, "array(INT[]).append!([take(int(NULL), 2)])"),
            std::make_tuple(dolphindb::DT_LONG_ARRAY, "array(LONG[]).append!([take(long(NULL), 2)])"), std::make_tuple(dolphindb::DT_FLOAT_ARRAY, "array(FLOAT[]).append!([take(float(NULL), 2)])"),
            std::make_tuple(dolphindb::DT_DOUBLE_ARRAY, "array(DOUBLE[]).append!([take(double(NULL), 2)])"), std::make_tuple(dolphindb::DT_DATE_ARRAY, "array(DATE[]).append!([take(date(NULL), 2)])"),
            std::make_tuple(dolphindb::DT_MONTH_ARRAY, "array(MONTH[]).append!([take(month(NULL), 2)])"), std::make_tuple(dolphindb::DT_TIME_ARRAY, "array(TIME[]).append!([take(time(NULL), 2)])"),
            std::make_tuple(dolphindb::DT_MINUTE_ARRAY, "array(MINUTE[]).append!([take(minute(NULL), 2)])"), std::make_tuple(dolphindb::DT_SECOND_ARRAY, "array(SECOND[]).append!([take(second(NULL), 2)])"),
            std::make_tuple(dolphindb::DT_DATEHOUR_ARRAY, "array(DATEHOUR[]).append!([take(datehour(NULL), 2)])"),
            std::make_tuple(dolphindb::DT_DATETIME_ARRAY, "array(DATETIME[]).append!([take(datetime(NULL), 2)])"),std::make_tuple(dolphindb::DT_TIMESTAMP_ARRAY, "array(TIMESTAMP[]).append!([take(timestamp(NULL), 2)])"),
            std::make_tuple(dolphindb::DT_NANOTIME_ARRAY, "array(NANOTIME[]).append!([take(nanotime(NULL), 2)])"),std::make_tuple(dolphindb::DT_NANOTIMESTAMP_ARRAY, "array(NANOTIMESTAMP[]).append!([take(nanotimestamp(NULL), 2)])"),
            std::make_tuple(dolphindb::DT_UUID_ARRAY, "array(UUID[]).append!([take(uuid(), 2)])"),std::make_tuple(dolphindb::DT_IP_ARRAY, "array(IPADDR[]).append!([take(ipaddr(), 2)])"),
            std::make_tuple(dolphindb::DT_INT128_ARRAY, "array(INT128[]).append!([take(int128(), 2)])"),
            std::make_tuple(dolphindb::DT_DECIMAL32_ARRAY, "array(DECIMAL32(5)[]).append!([take(decimal32('', 5), 2)])"),
            std::make_tuple(dolphindb::DT_DECIMAL64_ARRAY, "array(DECIMAL64(15)[]).append!([take(decimal64('', 15), 2)])"),
            std::make_tuple(dolphindb::DT_DECIMAL128_ARRAY, "array(DECIMAL128(25)[]).append!([take(decimal128('', 25), 2)])"),
        };
    };
};
INSTANTIATE_TEST_SUITE_P(EventSender_arrayVector, StreamingCEPEventTest_EventSender_arrayVector, testing::ValuesIn(StreamingCEPEventTest_EventSender_arrayVector::get_arrayVector_data()));
INSTANTIATE_TEST_SUITE_P(EventSender_arrayVector_allNull, StreamingCEPEventTest_EventSender_arrayVector, testing::ValuesIn(StreamingCEPEventTest_EventSender_arrayVector::get_arrayVector_allNull_data()));
TEST_P(StreamingCEPEventTest_EventSender_arrayVector, test_EventSender_arrayVector) // commonFields不能包含array vector类型
{
    std::string case_=getCaseName();
    dolphindb::DATA_TYPE dataType = std::get<0>(GetParam());
    std::string type_data_str = std::get<1>(GetParam());
    std::string typeString = dolphindb::Util::getDataTypeString(dataType);
    int extraParam = 0;
    if (typeString.compare(0, 9, "DECIMAL32") == 0){typeString = "DECIMAL32(5)[]"; extraParam = 5;}
    else if (typeString.compare(0, 9, "DECIMAL64") == 0){typeString = "DECIMAL64(15)[]"; extraParam = 15;}
    else if (typeString.compare(0, 10, "DECIMAL128") == 0){typeString = "DECIMAL128(25)[]"; extraParam = 25;}
    conn->run("share streamTable(1:0, `time`eventType`event`commonCol, [TIME,STRING,BLOB,INT]) as "+case_+"_inputTable;");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time", "testCol", "commonCol"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME, dataType, dolphindb::DT_INT};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_VECTOR, dolphindb::DF_SCALAR};
    schema->fieldExtraParams_ = {0, 0, extraParam, extraParam};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {"time"};
    std::vector<std::string> commonFields = {"commonCol"};
    dolphindb::EventSender* sender = new dolphindb::EventSender(conn, case_+"_inputTable", EventSchemas, eventTimeFields, commonFields);
    int rows = 1000;
    dolphindb::VectorSP col0 = conn->run("rand(`sz`sh`bj`adk, "+std::to_string(rows)+")");
    dolphindb::VectorSP col1 = conn->run("time(rand(50000000, "+std::to_string(rows)+"))");
    dolphindb::VectorSP col_test = conn->run("rand(["+type_data_str+"], "+std::to_string(rows)+")");
    std::vector<dolphindb::ConstantSP> columnVecs;
    columnVecs.reserve(4);
    dolphindb::VectorSP col_market = dolphindb::Util::createVector(dolphindb::DT_STRING, 0);
    dolphindb::VectorSP col_time = dolphindb::Util::createVector(dolphindb::DT_TIME, 0);
    dolphindb::VectorSP col_testCol = dolphindb::Util::createVector(dataType, 0, 0, true, extraParam);
    dolphindb::VectorSP col_commonCol = dolphindb::Util::createVector(dolphindb::DT_INT, 0);
    std::future<std::vector<std::string>> outs = std::async(subInputTable, case_+"_inputTable", rows, EventSchemas, eventTimeFields, commonFields);
    for (int i = 0; i < rows; i++){
        std::vector<dolphindb::ConstantSP> attributes = {col0->get(i), col1->get(i), col_test->get(i), dolphindb::Util::createInt(i)};
        col_market->append(col0->get(i));
        col_time->append(col1->get(i));
        col_testCol->append(col_test->get(i));
        col_commonCol->append(dolphindb::Util::createInt(i));
        sender->sendEvent("market", attributes);
    }
    columnVecs.emplace_back(col_market);
    columnVecs.emplace_back(col_time);
    columnVecs.emplace_back(col_testCol);
    columnVecs.emplace_back(col_commonCol);
    dolphindb::TableSP exTab = dolphindb::Util::createTable(schema->fieldNames_, columnVecs);
    conn->upload("ex", exTab);
    std::vector<std::string> tabNames = outs.get();
    ASSERT_TRUE(conn->run("all(each(eqObj, "+tabNames[0]+".values(), ex.values()))")->getBool());
    delete sender, schema;
}

class StreamingCEPEventTest_EventClient_alltypes : public StreamingCEPEventTest, public ::testing::WithParamInterface<std::tuple<dolphindb::DATA_TYPE, std::string>> {
public:
    static std::vector<std::tuple<dolphindb::DATA_TYPE, std::string>> get_scalar_data(){
        return {
            std::make_tuple(dolphindb::DT_BOOL, "array(BOOL).append!(true false NULL)"),std::make_tuple(dolphindb::DT_CHAR, "-127c 127c NULL"),
            std::make_tuple(dolphindb::DT_SHORT, "1000h NULL"),std::make_tuple(dolphindb::DT_INT, "10000 NULL"),
            std::make_tuple(dolphindb::DT_LONG, "1000000l NULL"),std::make_tuple(dolphindb::DT_FLOAT, "100.000f NULL"),
            std::make_tuple(dolphindb::DT_DOUBLE, "100.0000 NULL"),std::make_tuple(dolphindb::DT_DATE, "2023.01.01 NULL"),
            std::make_tuple(dolphindb::DT_MONTH, "2023.01M NULL"),std::make_tuple(dolphindb::DT_TIME, "23:45:03.100 NULL"),
            std::make_tuple(dolphindb::DT_MINUTE, "23:45m NULL"),std::make_tuple(dolphindb::DT_SECOND, "23:45:03 NULL"),
            std::make_tuple(dolphindb::DT_DATEHOUR, "datehour('2021.01.01T12'`)"),std::make_tuple(dolphindb::DT_STRING, "`szstr`"),
            std::make_tuple(dolphindb::DT_DATETIME, "2021.01.01T12:45:03 NULL"),std::make_tuple(dolphindb::DT_TIMESTAMP, "2021.01.01T12:45:03.100 NULL"),
            std::make_tuple(dolphindb::DT_NANOTIME, "23:45:03.100000000 NULL"),std::make_tuple(dolphindb::DT_NANOTIMESTAMP, "2021.01.01T12:45:03.100000000 NULL"),
            std::make_tuple(dolphindb::DT_SYMBOL, "`sz`sh`bj`"),
            std::make_tuple(dolphindb::DT_UUID, "uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87''5d212a78-cc48-e3b1-4235-b4d91473ee10'`)"),
            std::make_tuple(dolphindb::DT_IP, "ipaddr('127.0.0.1''1.1.1.1'`)"),
            std::make_tuple(dolphindb::DT_INT128, "int128(`e1671797c52e15f763380b45e841ec32`e1671797c52e15f763380b45e841ec42`)"),
            std::make_tuple(dolphindb::DT_BLOB, "blob(`blob`blob2`)"),
            std::make_tuple(dolphindb::DT_DECIMAL32, "decimal32('1.123456789'`'-135.1', 5)"),std::make_tuple(dolphindb::DT_DECIMAL64, "decimal64('-11.456464'`'300.1', 15)"),
            std::make_tuple(dolphindb::DT_DECIMAL128, "decimal128('999.64621462333'`'-1326', 25)"),
        };
    };
};

INSTANTIATE_TEST_SUITE_P(EventClient_alltype, StreamingCEPEventTest_EventClient_alltypes, testing::ValuesIn(StreamingCEPEventTest_EventClient_alltypes::get_scalar_data()));
TEST_P(StreamingCEPEventTest_EventClient_alltypes, test_EventClient_alltype)
{
    std::string case_=getCaseName();
    dolphindb::DATA_TYPE dataType = std::get<0>(GetParam());
    std::string type_data_str = std::get<1>(GetParam());
    std::string typeString = dolphindb::Util::getDataTypeString(dataType);
    if (dataType == dolphindb::DT_SYMBOL) GTEST_SKIP() << "Class attributes cannot be symbol scalar.";
    int extraParam = 0;
    if (typeString.compare(0, 9, "DECIMAL32") == 0){typeString = "DECIMAL32(5)"; extraParam = 5;}
    else if (typeString.compare(0, 9, "DECIMAL64") == 0){typeString = "DECIMAL64(15)"; extraParam = 15;}
    else if (typeString.compare(0, 10, "DECIMAL128") == 0){typeString = "DECIMAL128(25)"; extraParam = 25;}
    std::cout << "test type: " << typeString << std::endl;
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "md";
    schema->fieldNames_ = {"market", "eventTime", "testCol", "commonCol"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIMESTAMP, dataType, dataType};
    schema->fieldForms_ = {dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    schema->fieldExtraParams_ = {0, 0, extraParam, extraParam};
    conn->run(
        "share streamTable(1:0, `time`eventType`event`commonCol, [TIMESTAMP,STRING,BLOB,"+typeString+"]) as "+case_+"_fromCEP;"
        "share streamTable(1:0, `time`eventType`event`commonCol, [TIMESTAMP,STRING,BLOB,"+typeString+"]) as "+case_+"_fromApi;go;"
        "class md{"
            "market :: STRING\n"
            "eventTime :: TIMESTAMP\n"
            "testCol :: "+typeString+"\n"
            "commonCol :: "+typeString+"\n"
            "def md(a,b,c,d){"
                "market = a\n"
                "eventTime = b\n"
                "testCol = c\n"
                "commonCol = d\n}"
        "};"
        "try{dropStreamEngine(`"+case_+"_EventSer)}catch(ex){};go;"
        "inputSerializer = streamEventSerializer(name=`"+case_+"_EventSer, EventSchema=md, outputTable="+case_+"_fromCEP, eventTimeField = \"eventTime\", commonField = \"commonCol\");"
    );
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    int sub_rows = 0, test_rows = 1000;
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {"eventTime"};
    std::vector<std::string> commonFields = {"commonCol"};
    dolphindb::EventClient *client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
    dolphindb::EventSender *sender = new dolphindb::EventSender(conn, case_+"_fromApi", EventSchemas, eventTimeFields, commonFields);
    auto imp_handler = [&](std::string eventType, std::vector<dolphindb::ConstantSP> attribute)
    {
        sub_rows += 1;
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        sender->sendEvent(eventType, attribute);
        if (sub_rows == test_rows)
        {
            notify.set();
        }
    };
    client->subscribe(HOST, PORT, imp_handler, case_+"_fromCEP", DEFAULT_ACTION_NAME, 0, true, USER, PASSWD);
    conn->run(
        "rows = "+std::to_string(test_rows)+";"
        "val_col0=rand(`sz`sh`bj`adk, rows);val_col1=now()..temporalAdd(now(), rows-1, 'ms');val_col2=rand("+type_data_str+", rows);val_col3=rand("+type_data_str+", rows);"
        "for (i in 0:rows){"
        "data = md(val_col0[i], val_col1[i], val_col2[i], val_col3[i]);"
        "appendEvent(getStreamEngine(`"+case_+"_EventSer), [data]);}"
    );
    notify.wait();
    dolphindb::Util::sleep(1000);
    client->unsubscribe(HOST, PORT, case_+"_fromCEP", DEFAULT_ACTION_NAME);
    client->exit();
    ASSERT_TRUE(client->isExit());
    ASSERT_TRUE(conn->run("all(eqObj("+case_+"_fromApi.values(), "+case_+"_fromCEP.values()))")->getBool());
    delete schema, client, sender;
}


class StreamingCEPEventTest_EventClient_alltypes_vector : public StreamingCEPEventTest, public ::testing::WithParamInterface<std::tuple<dolphindb::DATA_TYPE, std::string>> {
public:
    static std::vector<std::tuple<dolphindb::DATA_TYPE, std::string>> get_vector_data(){
        return {
            std::make_tuple(dolphindb::DT_BOOL, "array(BOOL).append!(true false)"),std::make_tuple(dolphindb::DT_CHAR, "-127c 127c"),
            std::make_tuple(dolphindb::DT_SHORT, "1000h"),std::make_tuple(dolphindb::DT_INT, "10000"),
            std::make_tuple(dolphindb::DT_LONG, "1000000l"),std::make_tuple(dolphindb::DT_FLOAT, "100.000f"),
            std::make_tuple(dolphindb::DT_DOUBLE, "100.0000"),std::make_tuple(dolphindb::DT_DATE, "2023.01.01"),
            std::make_tuple(dolphindb::DT_MONTH, "2023.01M"),std::make_tuple(dolphindb::DT_TIME, "23:45:03.100"),
            std::make_tuple(dolphindb::DT_MINUTE, "23:45m"),std::make_tuple(dolphindb::DT_SECOND, "23:45:03"),
            std::make_tuple(dolphindb::DT_DATEHOUR, "datehour('2021.01.01T12'`)"),
            std::make_tuple(dolphindb::DT_DATETIME, "2021.01.01T12:45:03"),std::make_tuple(dolphindb::DT_TIMESTAMP, "2021.01.01T12:45:03.100"),
            std::make_tuple(dolphindb::DT_NANOTIME, "23:45:03.100000000"),std::make_tuple(dolphindb::DT_NANOTIMESTAMP, "2021.01.01T12:45:03.100000000"),
            std::make_tuple(dolphindb::DT_SYMBOL, "symbol(take(`sz`da`jt, 100000))"),std::make_tuple(dolphindb::DT_STRING, "`szstr`"),
            std::make_tuple(dolphindb::DT_UUID, "uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87''5d212a78-cc48-e3b1-4235-b4d91473ee10')"),
            std::make_tuple(dolphindb::DT_IP, "ipaddr('127.0.0.1''1.1.1.1')"),
            std::make_tuple(dolphindb::DT_INT128, "int128(`e1671797c52e15f763380b45e841ec32`e1671797c52e15f763380b45e841ec42)"),
            std::make_tuple(dolphindb::DT_BLOB, "blob(`blob`blob2)"),
            std::make_tuple(dolphindb::DT_DECIMAL32, "decimal32('1.123456789'`0'-135.1', 5)"),std::make_tuple(dolphindb::DT_DECIMAL64, "decimal64('-11.456464'`0'300.1', 15)"),
            std::make_tuple(dolphindb::DT_DECIMAL128, "decimal128('999.64621462333'`0'-1326', 25)"),
        };
    };
    static std::vector<std::tuple<dolphindb::DATA_TYPE, std::string>> get_vector_allNull_data(){
        return {
            std::make_tuple(dolphindb::DT_BOOL, "array(BOOL).append!([bool(NULL), bool(NULL)])"),std::make_tuple(dolphindb::DT_CHAR, "array(CHAR).append!([char(NULL), char(NULL)])"),
            std::make_tuple(dolphindb::DT_SHORT, "array(SHORT).append!([short(NULL), short(NULL)])"),std::make_tuple(dolphindb::DT_INT, "array(INT).append!([int(NULL), int(NULL)])"),
            std::make_tuple(dolphindb::DT_LONG, "array(LONG).append!([long(NULL), long(NULL)])"),std::make_tuple(dolphindb::DT_FLOAT, "array(FLOAT).append!([float(NULL), float(NULL)])"),
            std::make_tuple(dolphindb::DT_DOUBLE, "array(DOUBLE).append!([double(NULL), double(NULL)])"),std::make_tuple(dolphindb::DT_DATE, "array(DATE).append!([date(NULL), date(NULL)])"),
            std::make_tuple(dolphindb::DT_MONTH, "array(MONTH).append!([month(NULL), month(NULL)])"),std::make_tuple(dolphindb::DT_TIME, "array(TIME).append!([time(NULL), time(NULL)])"),
            std::make_tuple(dolphindb::DT_MINUTE, "array(MINUTE).append!([minute(NULL), minute(NULL)])"),std::make_tuple(dolphindb::DT_SECOND, "array(SECOND).append!([second(NULL), second(NULL)])"),
            std::make_tuple(dolphindb::DT_DATEHOUR, "array(DATEHOUR).append!([datehour(NULL), datehour(NULL)])"),
            std::make_tuple(dolphindb::DT_DATETIME, "array(DATETIME).append!([datetime(NULL), datetime(NULL)])"),std::make_tuple(dolphindb::DT_TIMESTAMP, "array(TIMESTAMP).append!([timestamp(NULL), timestamp(NULL)])"),
            std::make_tuple(dolphindb::DT_NANOTIME, "array(NANOTIME).append!([nanotime(NULL), nanotime(NULL)])"),std::make_tuple(dolphindb::DT_NANOTIMESTAMP, "array(NANOTIMESTAMP).append!([nanotimestamp(NULL), nanotimestamp(NULL)])"),
            std::make_tuple(dolphindb::DT_UUID, "array(UUID).append!([uuid(), uuid()])"),std::make_tuple(dolphindb::DT_IP, "array(IPADDR).append!([ipaddr(), ipaddr()])"),
            std::make_tuple(dolphindb::DT_INT128, "array(INT128).append!([int128(), int128()])"),
            std::make_tuple(dolphindb::DT_DECIMAL32, "array(DECIMAL32(5)).append!([decimal32('', 5), decimal32('', 5)])"),
            std::make_tuple(dolphindb::DT_DECIMAL64, "array(DECIMAL64(15)).append!([decimal64('', 15), decimal64('', 15)])"),
            std::make_tuple(dolphindb::DT_DECIMAL128, "array(DECIMAL128(25)).append!([decimal128('', 25), decimal128('', 25)])"),
            std::make_tuple(dolphindb::DT_SYMBOL, "symbol(take(``,10000))"),
            std::make_tuple(dolphindb::DT_STRING, "string(``)"),
            std::make_tuple(dolphindb::DT_BLOB, "blob(``)"),
        };
    };
};

INSTANTIATE_TEST_SUITE_P(vector_noNull, StreamingCEPEventTest_EventClient_alltypes_vector, testing::ValuesIn(StreamingCEPEventTest_EventClient_alltypes_vector::get_vector_data()));
INSTANTIATE_TEST_SUITE_P(vector_allNull, StreamingCEPEventTest_EventClient_alltypes_vector, testing::ValuesIn(StreamingCEPEventTest_EventClient_alltypes_vector::get_vector_allNull_data()));
TEST_P(StreamingCEPEventTest_EventClient_alltypes_vector, test_EventClient_alltype_vector)
{
    std::string case_=getCaseName();
    dolphindb::DATA_TYPE dataType = std::get<0>(GetParam());
    std::string type_data_str = std::get<1>(GetParam());
    std::string typeString = dolphindb::Util::getDataTypeString(dataType);
    int extraParam = 0;
    if (typeString.compare(0, 9, "DECIMAL32") == 0){typeString = "DECIMAL32(5)"; extraParam = 5;}
    else if (typeString.compare(0, 9, "DECIMAL64") == 0){typeString = "DECIMAL64(15)"; extraParam = 15;}
    else if (typeString.compare(0, 10, "DECIMAL128") == 0){typeString = "DECIMAL128(25)"; extraParam = 25;}
    std::cout << "test type: " << typeString << std::endl;
    #define NOT_LITERAL dataType != dolphindb::DT_SYMBOL && dataType != dolphindb::DT_STRING && dataType != dolphindb::DT_BLOB
    std::string formatCommonColstr ="", formatClassstr ="";
    if (NOT_LITERAL){
        formatCommonColstr = typeString + "[]";
        formatClassstr = typeString + " VECTOR";
    }else{
        formatCommonColstr = "INT";
        formatClassstr = "INT";
    }
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "md";
    schema->fieldNames_ = {"market", "eventTime", "testCol", "commonCol"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIMESTAMP, dataType, NOT_LITERAL?dataType:dolphindb::DT_INT};
    schema->fieldForms_ = {dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_VECTOR, NOT_LITERAL?dolphindb::DF_VECTOR:dolphindb::DF_SCALAR};
    schema->fieldExtraParams_ = {0, 0, extraParam, extraParam};
    conn->run(
        "share streamTable(1:0, `time`eventType`event`commonCol, [TIMESTAMP,STRING,BLOB,"+formatCommonColstr+"]) as "+case_+"_fromCEP;"
        "share streamTable(1:0, `time`eventType`event`commonCol, [TIMESTAMP,STRING,BLOB,"+formatCommonColstr+"]) as "+case_+"_fromApi;go;"
        "class md{"
            "market :: STRING\n"
            "eventTime :: TIMESTAMP\n"
            "testCol :: "+typeString+" VECTOR\n"
            "commonCol :: "+formatClassstr+"\n"
            "def md(a,b,c,d){"
                "market = a\n"
                "eventTime = b\n"
                "testCol = c\n"
                "commonCol = d\n}"
        "};"
        "try{dropStreamEngine(`"+case_+"_EventSer)}catch(ex){};go;"
        "inputSerializer = streamEventSerializer(name=`"+case_+"_EventSer, EventSchema=md, outputTable="+case_+"_fromCEP, eventTimeField=\"eventTime\",commonField=\"commonCol\");"
    );

    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    int sub_rows = 0, test_rows = 100;
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {"eventTime"};
    std::vector<std::string> commonFields = {"commonCol"};
    dolphindb::EventClient *client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
    dolphindb::EventSender* sender = new dolphindb::EventSender(conn, case_+"_fromApi", EventSchemas, eventTimeFields, commonFields);
    auto imp_handler = [&](std::string eventType, std::vector<dolphindb::ConstantSP> attribute)
    {
        sub_rows += 1;
        if (sub_rows % 100 == 0) std::cout << "sub_rows: " << sub_rows << std::endl;
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        sender->sendEvent(eventType, attribute);
        if (sub_rows == test_rows)
        {
            notify.set();
        }
    };

    client->subscribe(HOST, PORT, imp_handler, case_+"_fromCEP", DEFAULT_ACTION_NAME, 0, true, USER, PASSWD);
    std::vector<std::string> val_col2_scripts ={"rand("+type_data_str+", rows)", "rand(val_col2, vector_size)"};
    if (dataType == dolphindb::DT_BLOB)
        val_col2_scripts = {"blob(string(rand("+type_data_str+", rows)))", "blob(string(rand(val_col2, vector_size)))"};
    if (dataType == dolphindb::DT_SYMBOL)
        val_col2_scripts = {"symbol(string(rand([\"blob\"], 10)))", "symbol(string(rand([\"blob\"], 10)))"};
    conn->run(
        "rows = "+std::to_string(test_rows)+";"
        "val_col0=rand(`sz`sh`bj`adk, rows);val_col1=now()..temporalAdd(now(), rows-1, 'ms');val_col2="+val_col2_scripts[0]+";val_col3=rand("+(NOT_LITERAL?type_data_str:"1000")+", rows);"
        "vector_size = rand(2000 0, 1)[0];"
        "for (i in 0:rows){"
            "col2 = "+val_col2_scripts[1]+";col3="+(NOT_LITERAL?"rand(val_col3, vector_size)":"val_col3[i]")+";"
            "data = md(val_col0[i], val_col1[i], col2, col3);"
            "appendEvent(getStreamEngine(`"+case_+"_EventSer), [data]);};go;");
    notify.wait();
    dolphindb::Util::sleep(1000);
    client->unsubscribe(HOST, PORT, case_+"_fromCEP", DEFAULT_ACTION_NAME);
    client->exit();
    ASSERT_TRUE(client->isExit());
    ASSERT_TRUE(conn->run("all(each(eqObj, "+case_+"_fromApi.values(),"+case_+"_fromCEP.values()))")->getBool());
    delete schema, client, sender;
}

class StreamingCEPEventTest_EventClient_arrayVector : public StreamingCEPEventTest, public ::testing::WithParamInterface<std::tuple<dolphindb::DATA_TYPE, std::string>> {
public:
    static std::vector<std::tuple<dolphindb::DATA_TYPE, std::string>> get_arrayVector_data(){
        return {
            std::make_tuple(dolphindb::DT_BOOL_ARRAY, "array(BOOL[]).append!([rand(true false, 2)]).append!([rand(true false, 2)])"), std::make_tuple(dolphindb::DT_CHAR_ARRAY, "array(CHAR[]).append!([rand(127c, 2)]).append!([rand(127c, 2)])"),
            std::make_tuple(dolphindb::DT_SHORT_ARRAY, "array(SHORT[]).append!([rand(1000h, 2)]).append!([rand(1000h, 2)])"), std::make_tuple(dolphindb::DT_INT_ARRAY, "array(INT[]).append!([rand(10000, 2)]).append!([rand(10000, 2)])"),
            std::make_tuple(dolphindb::DT_LONG_ARRAY, "array(LONG[]).append!([rand(1000000l, 2)]).append!([rand(1000000l, 2)])"), std::make_tuple(dolphindb::DT_FLOAT_ARRAY, "array(FLOAT[]).append!([rand(100.000f, 2)]).append!([rand(100.000f, 2)])"),
            std::make_tuple(dolphindb::DT_DOUBLE_ARRAY, "array(DOUBLE[]).append!([rand(100.0000, 2)]).append!([rand(100.0000, 2)])"), std::make_tuple(dolphindb::DT_DATE_ARRAY, "array(DATE[]).append!([rand(2023.01.01, 2)]).append!([rand(2023.01.01, 2)])"),
            std::make_tuple(dolphindb::DT_MONTH_ARRAY, "array(MONTH[]).append!([rand(2023.01M, 2)]).append!([rand(2023.01M, 2)])"), std::make_tuple(dolphindb::DT_TIME_ARRAY, "array(TIME[]).append!([rand(23:45:03.100, 2)]).append!([rand(23:45:03.100, 2)])"),
            std::make_tuple(dolphindb::DT_MINUTE_ARRAY, "array(MINUTE[]).append!([rand(23:45m, 2)]).append!([rand(23:45m, 2)])"), std::make_tuple(dolphindb::DT_SECOND_ARRAY, "array(SECOND[]).append!([rand(23:45:03, 2)]).append!([rand(23:45:03, 2)])"),
            std::make_tuple(dolphindb::DT_DATEHOUR_ARRAY, "array(DATEHOUR[]).append!([rand(datehour('2021.01.01T12'`), 2)]).append!([rand(datehour('2021.01.01T12'`), 2)])"),
            std::make_tuple(dolphindb::DT_DATETIME_ARRAY, "array(DATETIME[]).append!([rand(2021.01.01T12:45:03, 2)]).append!([rand(2021.01.01T12:45:03, 2)])"),std::make_tuple(dolphindb::DT_TIMESTAMP_ARRAY, "array(TIMESTAMP[]).append!([rand(2021.01.01T12:45:03.100, 2)]).append!([rand(2021.01.01T12:45:03.100, 2)])"),
            std::make_tuple(dolphindb::DT_NANOTIME_ARRAY, "array(NANOTIME[]).append!([rand(23:45:03.100000000, 2)]).append!([rand(23:45:03.100000000, 2)])"),std::make_tuple(dolphindb::DT_NANOTIMESTAMP_ARRAY, "array(NANOTIMESTAMP[]).append!([rand(2021.01.01T12:45:03.100000000, 2)]).append!([rand(2021.01.01T12:45:03.100000000, 2)])"),
            std::make_tuple(dolphindb::DT_UUID_ARRAY, "array(UUID[]).append!([rand(uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87''5d212a78-cc48-e3b1-4235-b4d91473ee10'), 2)]).append!([rand(uuid('5d212a78-cc48-e3b1-4235-b4d91473ee44''5d212a78-cc48-e3b1-4235-b4d91473ee15'), 2)])"),std::make_tuple(dolphindb::DT_IP_ARRAY, "array(IPADDR[]).append!([rand(ipaddr('127.0.0.1''1.1.1.1'), 2)]).append!([rand(ipaddr('2.2.2.2''255.255.0.0'), 2)])"),
            std::make_tuple(dolphindb::DT_INT128_ARRAY, "array(INT128[]).append!([rand(int128(`e1671797c52e15f763380b45e841ec32`e1671797c52e15f763380b45e841ec42), 2)]).append!([rand(int128(`e1671797c52e15f763380b45e841ec76`e1671797c52e15f763380b45e841ec89), 2)])"),
        };
    };
    static std::vector<std::tuple<dolphindb::DATA_TYPE, std::string>> get_arrayVector_allNull_data(){
        return {
            std::make_tuple(dolphindb::DT_BOOL_ARRAY, "array(BOOL[]).append!([take(bool(NULL), 2)])"), std::make_tuple(dolphindb::DT_CHAR_ARRAY, "array(CHAR[]).append!([take(char(NULL), 2)])"),
            std::make_tuple(dolphindb::DT_SHORT_ARRAY, "array(SHORT[]).append!([take(short(NULL), 2)])"), std::make_tuple(dolphindb::DT_INT_ARRAY, "array(INT[]).append!([take(int(NULL), 2)])"),
            std::make_tuple(dolphindb::DT_LONG_ARRAY, "array(LONG[]).append!([take(long(NULL), 2)])"), std::make_tuple(dolphindb::DT_FLOAT_ARRAY, "array(FLOAT[]).append!([take(float(NULL), 2)])"),
            std::make_tuple(dolphindb::DT_DOUBLE_ARRAY, "array(DOUBLE[]).append!([take(double(NULL), 2)])"), std::make_tuple(dolphindb::DT_DATE_ARRAY, "array(DATE[]).append!([take(date(NULL), 2)])"),
            std::make_tuple(dolphindb::DT_MONTH_ARRAY, "array(MONTH[]).append!([take(month(NULL), 2)])"), std::make_tuple(dolphindb::DT_TIME_ARRAY, "array(TIME[]).append!([take(time(NULL), 2)])"),
            std::make_tuple(dolphindb::DT_MINUTE_ARRAY, "array(MINUTE[]).append!([take(minute(NULL), 2)])"), std::make_tuple(dolphindb::DT_SECOND_ARRAY, "array(SECOND[]).append!([take(second(NULL), 2)])"),
            std::make_tuple(dolphindb::DT_DATEHOUR_ARRAY, "array(DATEHOUR[]).append!([take(datehour(NULL), 2)])"),
            std::make_tuple(dolphindb::DT_DATETIME_ARRAY, "array(DATETIME[]).append!([take(datetime(NULL), 2)])"),std::make_tuple(dolphindb::DT_TIMESTAMP_ARRAY, "array(TIMESTAMP[]).append!([take(timestamp(NULL), 2)])"),
            std::make_tuple(dolphindb::DT_NANOTIME_ARRAY, "array(NANOTIME[]).append!([take(nanotime(NULL), 2)])"),std::make_tuple(dolphindb::DT_NANOTIMESTAMP_ARRAY, "array(NANOTIMESTAMP[]).append!([take(nanotimestamp(NULL), 2)])"),
            std::make_tuple(dolphindb::DT_UUID_ARRAY, "array(UUID[]).append!([take(uuid(), 2)])"),std::make_tuple(dolphindb::DT_IP_ARRAY, "array(IPADDR[]).append!([take(ipaddr(), 2)])"),
            std::make_tuple(dolphindb::DT_INT128_ARRAY, "array(INT128[]).append!([take(int128(), 2)])"),
            std::make_tuple(dolphindb::DT_DECIMAL32_ARRAY, "array(DECIMAL32(5)[]).append!([take(decimal32('', 5), 2)])"),
            std::make_tuple(dolphindb::DT_DECIMAL64_ARRAY, "array(DECIMAL64(15)[]).append!([take(decimal64('', 15), 2)])"),
            std::make_tuple(dolphindb::DT_DECIMAL128_ARRAY, "array(DECIMAL128(25)[]).append!([take(decimal128('', 25), 2)])"),
        };
    };
};
INSTANTIATE_TEST_SUITE_P(arrayVector_noNull, StreamingCEPEventTest_EventClient_arrayVector, testing::ValuesIn(StreamingCEPEventTest_EventClient_arrayVector::get_arrayVector_data()));
INSTANTIATE_TEST_SUITE_P(arrayVector_allNull, StreamingCEPEventTest_EventClient_arrayVector, testing::ValuesIn(StreamingCEPEventTest_EventClient_arrayVector::get_arrayVector_allNull_data()));
TEST_P(StreamingCEPEventTest_EventClient_arrayVector, test_EventClient_arrayVector)
{
    GTEST_SKIP() << "ArrayVector in class is not supported yet.";
    std::string case_=getCaseName();
    dolphindb::DATA_TYPE dataType = std::get<0>(GetParam());
    std::string type_data_str = std::get<1>(GetParam());
    std::string typeString = dolphindb::Util::getDataTypeString(dataType);
    int extraParam = 0;
    if (typeString.compare(0, 9, "DECIMAL32") == 0){typeString = "DECIMAL32(5)[]"; extraParam = 5;}
    else if (typeString.compare(0, 9, "DECIMAL64") == 0){typeString = "DECIMAL64(15)[]"; extraParam = 15;}
    else if (typeString.compare(0, 10, "DECIMAL128") == 0){typeString = "DECIMAL128(25)[]"; extraParam = 25;}
    std::cout << "test type: " << typeString << std::endl;

    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "md";
    schema->fieldNames_ = {"market", "eventTime", "testCol", "commonCol"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIMESTAMP, dataType, dataType};
    schema->fieldForms_ = {dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_VECTOR, dolphindb::DF_VECTOR};
    schema->fieldExtraParams_ = {0, 0, extraParam, extraParam};
    conn->run(
        "share streamTable(1:0, `time`eventType`event`commonCol, [TIMESTAMP,STRING,BLOB,"+typeString+"]) as "+case_+"_fromCEP;"
        "share streamTable(1:0, `time`eventType`event`commonCol, [TIMESTAMP,STRING,BLOB,"+typeString+"]) as "+case_+"_fromApi;go;"
        "class md{"
            "market :: STRING\n"
            "eventTime :: TIMESTAMP\n"
            "testCol :: "+typeString+" VECTOR\n"
            "commonCol :: "+typeString+" VECTOR\n"
            "def md(a,b,c,d){"
                "market = a\n"
                "eventTime = b\n"
                "testCol = c\n"
                "commonCol = d\n}"
        "};"
        "try{dropStreamEngine(`"+case_+"_EventSer)}catch(ex){};go;"
        "inputSerializer = streamEventSerializer(name=`"+case_+"_EventSer, EventSchema=md, outputTable="+case_+"_fromCEP, eventTimeField=\"eventTime\",commonField=\"commonCol\");"
    );
    dolphindb::Signal notify;
    dolphindb::Mutex mutex;
    int sub_rows = 0, test_rows = 100;
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {"eventTime"};
    std::vector<std::string> commonFields = {"commonCol"};
    dolphindb::EventClient *client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
    dolphindb::EventSender* sender = new dolphindb::EventSender(conn, case_+"_fromApi", EventSchemas, eventTimeFields, commonFields);
    auto imp_handler = [&](std::string eventType, std::vector<dolphindb::ConstantSP> attribute)
    {
        sub_rows += 1;
        dolphindb::LockGuard<dolphindb::Mutex> lock(&mutex);
        sender->sendEvent(eventType, attribute);
        if (sub_rows == test_rows)
        {
            notify.set();
        }
    };
    client->subscribe(HOST, PORT, imp_handler, case_+"_fromCEP", DEFAULT_ACTION_NAME, 0, true, USER, PASSWD);
    std::vector<std::string> val_col2_scripts ={"rand("+type_data_str+", rows)", "rand(val_col2, vector_size)"};
    conn->run(
        "rows = "+std::to_string(test_rows)+";"
        "val_col0=rand(`sz`sh`bj`adk, rows);val_col1=now()..temporalAdd(now(), rows-1, 'ms');val_col2="+val_col2_scripts[0]+";val_col3=rand("+type_data_str+", rows);"
        "vector_size = rand(0 2000, 1)[0];"
        "for (i in 0:rows){"
            "col2 = "+val_col2_scripts[1]+";col3=rand(val_col3, vector_size);"
            "data = md(val_col0[i], val_col1[i], col2, col3);"
            "appendEvent(getStreamEngine(`"+case_+"_EventSer), [data]);};go;");
    notify.wait();
    dolphindb::Util::sleep(1000);
    client->unsubscribe(HOST, PORT, case_+"_fromCEP", DEFAULT_ACTION_NAME);
    client->exit();
    ASSERT_TRUE(client->isExit());
    ASSERT_TRUE(conn->run("all(each(eqObj, "+case_+"_fromApi.values(),"+case_+"_fromCEP.values()))")->getBool());
    delete schema, client, sender;
}


TEST_F(StreamingCEPEventTest, test_resub_true_with_resubscribeTimeout)
{
    std::string case_=getCaseName();
    conn->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as "+case_+"_inputTable;");
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_TIME};
    schema->fieldForms_={dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {"time"};
    std::vector<std::string> commonFields = {};
    dolphindb::EventClient* client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
    dolphindb::EventClient* client2 = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
    unsigned int resubscribeTimeout = 500;
    client->subscribe(HOST, PORT, nullptr, case_+"_inputTable", DEFAULT_ACTION_NAME, 0, true, USER, PASSWD);
    auto t = client2->subscribe(HOST, PORT, nullptr, case_+"_inputTable", DEFAULT_ACTION_NAME, 0, true, USER, PASSWD, resubscribeTimeout);
    dolphindb::Util::sleep(resubscribeTimeout+1000);
    client->unsubscribe(HOST, PORT, case_+"_inputTable", DEFAULT_ACTION_NAME);
    client2->exit();
    dolphindb::Util::sleep(1000);
    ASSERT_TRUE(t->isComplete());
    ASSERT_TRUE(conn->run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`"+case_+"_inputTable) ==0")->getBool());
    delete client, client2, schema;
}

TEST_F(StreamingCEPEventTest, test_CEP_SCRAM_user){
    std::string case_=getCaseName();
    std::string userName=getRandString(20);
    conn->run(
        "userName='"+userName+"';"
        "try{deleteUser(userName)}catch(ex){};go;createUser(userName, `123456, authMode='scram')"
    );
    dolphindb::DBConnectionSP conn_scram = new dolphindb::DBConnection(false, false, 7200, false, false, false, true);
    conn_scram->connect(HOST, PORT, userName, "123456");
    std::string script = "share streamTable(1:0, `eventType`event, [STRING,BLOB]) as "+case_+"_inputTable;\n"
                    "share streamTable(1:0, `market`code`price`qty`eventTime, [STRING,STRING,DOUBLE,INT,TIMESTAMP]) as "+case_+"_outputTable;";
    conn_scram->run(script);
    dolphindb::EventSchema *schema = new dolphindb::EventSchema();
    //TODO 用C++API写入\r结尾的字符串数据，写入后会缺少\r，正在跟server讨论这个问题，这里先规避，后面加个1
    schema->eventType_ = "!@#$%&*()_+《》>{}[]-=';./,~`1^;中文 \"\n\t\r1";
    schema->fieldNames_ = {"market", "code", "price", "qty", "eventTime"};
    schema->fieldTypes_ = {dolphindb::DT_STRING, dolphindb::DT_STRING, dolphindb::DT_DOUBLE, dolphindb::DT_INT, dolphindb::DT_TIMESTAMP};
    schema->fieldForms_ = {dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR, dolphindb::DF_SCALAR};
    std::vector<dolphindb::EventSchema> EventSchemas = {*schema};
    std::vector<std::string> eventTimeFields = {};
    std::vector<std::string> commonFields = {};
    dolphindb::EventSender *sender = new dolphindb::EventSender(conn_scram, case_+"_inputTable", EventSchemas, eventTimeFields, commonFields);
    dolphindb::EventClient* client = new dolphindb::EventClient(EventSchemas, eventTimeFields, commonFields);
    client->subscribe(HOST, PORT, test_handler, case_+"_inputTable", DEFAULT_ACTION_NAME, 0, true, userName, "123456");
    sender->sendEvent(schema->eventType_, {dolphindb::Util::createString("sz"), dolphindb::Util::createString("000001"), dolphindb::Util::createDouble(10.5), dolphindb::Util::createInt(100), dolphindb::Util::createTimestamp(2021, 1, 1, 10, 30, 0, 100)});
    dolphindb::Util::sleep(2000);
    client->unsubscribe(HOST, PORT, case_+"_inputTable", DEFAULT_ACTION_NAME);
    dolphindb::TableSP re = conn_scram->run(case_+"_outputTable");
    ASSERT_EQ(1, re->rows());
    ASSERT_EQ("sz", re->getColumn(0)->get(0)->getString());
    ASSERT_EQ("000001", re->getColumn(1)->get(0)->getString());
    ASSERT_EQ(10.5, re->getColumn(2)->get(0)->getDouble());
    ASSERT_EQ(100, re->getColumn(3)->get(0)->getInt());
    ASSERT_EQ("2021.01.01T10:30:00.100", re->getColumn(4)->get(0)->getString());
}