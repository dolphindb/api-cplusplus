#include "config.h"
#include <future>

class CEPEventTest : public testing::Test
{
public:
    static string get_clear_script()
    {
        return "a = getStreamingStat().pubTables\n"
               "for(i in a){\n"
               "\ttry{stopPublishTable(i.subscriber.split(\":\")[0],int(i.subscriber.split(\":\")[1]),i.tableName,i.actions)}catch(ex){}\n"
               "};"
               "def getAllShare(){\n"
               "\treturn select name from objs(true) where shared=1\n"
               "\t}\n"
               "\n"
               "def clearShare(){\n"
               "\tlogin(`admin,`123456)\n"
               "\tallShare=exec name from pnodeRun(getAllShare)\n"
               "\tfor(i in allShare){\n"
               "\t\ttry{\n"
               "\t\t\trpc((exec node from pnodeRun(getAllShare) where name =i)[0],clearTablePersistence,objByName(i))\n"
               "\t\t\t}catch(ex1){}\n"
               "\t\trpc((exec node from pnodeRun(getAllShare) where name =i)[0],undef,i,SHARED)\n"
               "\t}\n"
               "\ttry{\n"
               "\t\tPST_DIR=rpc(getControllerAlias(),getDataNodeConfig{getNodeAlias()})['persistenceDir']\n"
               "\t}catch(ex1){}\n"
               "}\n"
               "clearShare();undef all;";
    }
    static void SetUpTestSuite()
    {

        conn300->initialize();
        bool ret = conn300->connect(hostName, port300, "admin", "123456");
        cout<<"check connect...";
        try
        {
            ConstantSP res = conn300->run("1+1");
        }
        catch(const std::exception& e)
        {
            conn300->connect(hostName, port300, "admin", "123456");
        }
    }

    virtual void SetUp()
    {
        if (!isNewServer(*conn300, 3, 0, 0)) {GTEST_SKIP() << "at least server v3.00.0.0 support CEP";}
        conn300->run(get_clear_script());
    }
    virtual void TearDown()
    {
        conn300->run(get_clear_script());
    }
    static void TearDownTestSuite()
    {
        conn300->close();
    }
};

auto test_handler = [](string eventType, std::vector<ConstantSP> attribute)
{
    Util::sleep(1000);
    cout << "eventType: " + eventType << endl;
    // for (auto &at : attribute)
    //     cout << at->getString() << endl;
    try
    {
        conn300->run("tableInsert{outputTable}", attribute);
    }
    catch (exception &e)
    {
        throw RuntimeException(e.what());
    }
};

// 返回handler订阅输出表的名称，其中包含订阅到的inputTable的数据
vector<string> subInputTable(const string &t, const int target_rows, const std::vector<EventSchema>& eventSchemas, const std::vector<std::string>& eventTimeKeys, const std::vector<std::string>& commonKeys){
    EventClient *client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
    Signal notify;
    Mutex mutex;
    int total_rows = 0;
    std::map<string, string> tmap = {};
    for (auto &schema: eventSchemas)
    {
        vector<string> colNames = schema.fieldNames_;
        vector<DATA_TYPE> colTypes = schema.fieldTypes_;
        TableSP t = Util::createTable(colNames, colTypes, 0, 0, schema.fieldExtraParams_);
        string curTabName = "output_" + schema.eventType_;
        conn300->upload("tmp", t);
        conn300->run("share tmp as " + curTabName + ";");
        tmap.insert(std::pair<string, string>(schema.eventType_, curTabName));
    }

    auto _handler = [&](const string& eventType, std::vector<ConstantSP>& attribute)
    {
        // cout << "订阅到数据：[";
        // for (auto i=0; i<attribute.size(); i++){
        //     cout << attribute[i]->getString();
        //     if (i != attribute.size()-1){
        //         cout << ", ";
        //     }
        // }
        // cout << "]" << endl;
        total_rows += 1;
        LockGuard<Mutex> lock(&mutex);
        for (auto &schema: eventSchemas)
        {
            if (schema.eventType_ == eventType)
            {
                vector<ConstantSP> columnVecs;
                columnVecs.reserve(attribute.size());
                for (auto i=0;i<attribute.size();i++)
                {
                    if (attribute[i]->getForm() == DF_VECTOR){
                        VectorSP avCol = Util::createArrayVector(attribute[i]->getType(), 0, 0, true, attribute[i]->getExtraParamForType());
                        avCol->append(attribute[i]);
                        columnVecs.emplace_back(avCol);
                    }else{
                        VectorSP col = Util::createVector(attribute[i]->getType(), 0, 0, true, attribute[i]->getExtraParamForType());
                        for (auto j=0;j<attribute[i]->rows();j++)
                        {
                            col->append(attribute[i]->get(j));
                        }
                        columnVecs.emplace_back(col);
                    }
                }

                TableSP t = Util::createTable(schema.fieldNames_, columnVecs);
                AutoFitTableAppender appender = AutoFitTableAppender("", tmap[eventType], *conn300);
                EXPECT_EQ(appender.append(t), 1);
            }
        }
        if (total_rows % 100 == 0)
            cout << "total_rows: " << total_rows << endl;

        if (total_rows == target_rows) notify.set();
    };

    auto th = client->subscribe(hostName, port300, _handler, t, DEFAULT_ACTION_NAME, 0);
    notify.wait();

    client->unsubscribe(hostName, port300, t, DEFAULT_ACTION_NAME);
    Util::sleep(1000);
    EXPECT_TRUE(th->isComplete());
    EXPECT_FALSE(th->isRunning());
    delete client;

    vector<string> outputNames = {};
    for (auto &p : tmap){
        outputNames.push_back(p.second);
    }
    return outputNames;
}

TEST_F(CEPEventTest, test_EventSchema_null)
{
    EventSchema *schema = new EventSchema();
    vector<EventSchema> eventSchemas = {};
    vector<string> eventTimeKeys = {};
    vector<string> commonKeys = {};
    string re = "";
    try
    {
        EventSender *sender = new EventSender(conn300, "", eventSchemas, eventTimeKeys, commonKeys);
    }
    catch (exception &ex)
    {
        re = ex.what();
    }
    EXPECT_EQ("eventSchemas must not be empty", re);

    string re1 = "";
    try
    {
        EventClient *client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
    }
    catch (exception &ex)
    {
        re1 = ex.what();
    }
    EXPECT_EQ("eventSchemas must not be empty", re1);
}

TEST_F(CEPEventTest, test_EventType_null)
{
    EventSchema *schema = new EventSchema();
    schema->fieldNames_ = {"market", "code", "price", "qty", "eventTime"};
    schema->fieldTypes_ = {DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP};
    schema->fieldForms_ = {DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {};
    vector<string> eventTimeKeys = {};
    vector<string> commonKeys = {};
    string re = "";
    try
    {
        EventSender *sender = new EventSender(conn300, "", eventSchemas, eventTimeKeys, commonKeys);
    }
    catch (exception &ex)
    {
        re = ex.what();
    }
    EXPECT_EQ("eventSchemas must not be empty", re);

    string re1 = "";
    try
    {
        EventClient *client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
    }
    catch (exception &ex)
    {
        re1 = ex.what();
    }
    EXPECT_EQ("eventSchemas must not be empty", re1);
}

TEST_F(CEPEventTest, test_EventType_null2)
{
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "";
    schema->fieldNames_ = {"market", "code", "price", "qty", "eventTime"};
    schema->fieldTypes_ = {DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP};
    schema->fieldForms_ = {DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {};
    vector<string> commonKeys = {};
    string re = "";
    try
    {
        EventSender *sender = new EventSender(conn300, "", eventSchemas, eventTimeKeys, commonKeys);
    }
    catch (exception &ex)
    {
        re = ex.what();
    }
    EXPECT_EQ("eventType must not be empty.", re);

    string re1 = "";
    try
    {
        EventClient *client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
    }
    catch (exception &ex)
    {
        re1 = ex.what();
    }
    EXPECT_EQ("eventType must not be empty.", re1);
}

TEST_F(CEPEventTest, test_EventType_special_character)
{
    string script = "share streamTable(1:0, `eventType`event, [STRING,BLOB]) as inputTable;\n"
                    "share streamTable(1:0, `market`code`price`qty`eventTime, [STRING,STRING,DOUBLE,INT,TIMESTAMP]) as outputTable;";
    conn300->run(script);
    EventSchema *schema = new EventSchema();
    //TODO 用C++API写入\r结尾的字符串数据，写入后会缺少\r，正在跟server讨论这个问题，这里先规避，后面加个1
    schema->eventType_ = "!@#$%&*()_+《》>{}[]-=';./,~`1^;中文 \"\n\t\r1";
    schema->fieldNames_ = {"market", "code", "price", "qty", "eventTime"};
    schema->fieldTypes_ = {DT_STRING, DT_STRING, DT_DOUBLE, DT_INT, DT_TIMESTAMP};
    schema->fieldForms_ = {DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {};
    vector<string> commonKeys = {};
    EventSender *sender = new EventSender(conn300, "inputTable", eventSchemas, eventTimeKeys, commonKeys);

    EventClient* client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
    client->subscribe(hostName, port300, test_handler, "inputTable", DEFAULT_ACTION_NAME, 0);
    sender->sendEvent(schema->eventType_, {Util::createString("sz"), Util::createString("000001"), Util::createDouble(10.5), Util::createInt(100), Util::createTimestamp(2021, 1, 1, 10, 30, 0, 100)});
    Util::sleep(2000);

    client->unsubscribe(hostName, port300, "inputTable", DEFAULT_ACTION_NAME);
    TableSP re = conn300->run("select * from outputTable");
    EXPECT_EQ(1, re->rows());
    EXPECT_EQ("sz", re->getColumn(0)->get(0)->getString());
    EXPECT_EQ("000001", re->getColumn(1)->get(0)->getString());
    EXPECT_EQ(10.5, re->getColumn(2)->get(0)->getDouble());
    EXPECT_EQ(100, re->getColumn(3)->get(0)->getInt());
    EXPECT_EQ("2021.01.01T10:30:00.100", re->getColumn(4)->get(0)->getString());
}

TEST_F(CEPEventTest, test_EventType_same_with_eventTimeKeys)
{
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time", "decimal32", "decimal64", "decimal128"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR};
    EventSchema *schema1 = new EventSchema(*schema);

    vector<EventSchema> eventSchemas = {};
    eventSchemas.push_back(*schema);
    eventSchemas.push_back(*schema1);
    vector<string> eventTimeKeys = {"market"};
    vector<string> commonKeys = {};
    string re = "";
    try{
        EventSender* sender = new EventSender(conn300, "", eventSchemas, eventTimeKeys, commonKeys);

    }catch(const exception& ex){
        re = ex.what();
    }
    EXPECT_EQ("EventType must be unique", re);

    string re1 = "";
    try{
        EventClient* client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);

    }catch(const exception& ex){
        re1 = ex.what();
    }
    EXPECT_EQ("EventType must be unique", re1);
}

TEST_F(CEPEventTest, test_fieldNames_null)
{
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {};
    schema->fieldTypes_ = {DT_STRING, DT_TIME, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {};
    vector<string> commonKeys = {};
    string re = "";
    try{
        EventSender* sender = new EventSender(conn300, "", eventSchemas, eventTimeKeys, commonKeys);

    }catch(const exception& ex){
        re = ex.what();
    }
    EXPECT_EQ("the eventKey in eventSchema must not be empty", re);

    string re1 = "";
    try{
        EventClient* client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);

    }catch(const exception& ex){
        re1 = ex.what();
    }
    EXPECT_EQ("the eventKey in eventSchema must not be empty", re1);
}

TEST_F(CEPEventTest, test_EventSender_AttrKeys_one_colume)
{
    conn300->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"time"};
    schema->fieldTypes_ = {DT_TIME};
    schema->fieldForms_ = {DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {"time"};
    vector<string> commonKeys = {};
    EventSender *sender = new EventSender(conn300, "inputTable", eventSchemas, eventTimeKeys, commonKeys);
    vector<ConstantSP> attributes = {Util::createTime(10,45,3,100)};
    sender->sendEvent("market", attributes);
    TableSP re = conn300->run("select * from inputTable");

    EXPECT_EQ(1, re->rows());
    EXPECT_EQ("10:45:03.100", re->getColumn(0)->get(0)->getString());
}

TEST_F(CEPEventTest, test_AttrTypes_null)
{
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time", "decimal32", "decimal64", "decimal128"};
    schema->fieldTypes_ = {};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {};
    vector<string> commonKeys = {};
    string re = "";
    try{
        EventSender* sender = new EventSender(conn300, "", eventSchemas, eventTimeKeys, commonKeys);

    }catch(exception& ex){
        re = ex.what();
    }
    EXPECT_EQ("The number of eventKey, eventTypes, eventForms and eventExtraParams must have the same length.", re);

    string re1 = "";
    try{
        EventClient* client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);

    }catch(exception& ex){
        re1 = ex.what();
    }
    EXPECT_EQ("The number of eventKey, eventTypes, eventForms and eventExtraParams must have the same length.", re1);
}

TEST_F(CEPEventTest, test_AttrForms_null)
{
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time", "decimal32", "decimal64", "decimal128"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128};
    schema->fieldForms_={};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {};
    vector<string> commonKeys = {};
    string re = "";
    try{
        EventSender* sender = new EventSender(conn300, "", eventSchemas, eventTimeKeys, commonKeys);

    }catch(exception& ex){
        re = ex.what();
    }
    EXPECT_EQ("The number of eventKey, eventTypes, eventForms and eventExtraParams must have the same length.", re);

    string re1 = "";
    try{
        EventClient* client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);

    }catch(exception& ex){
        re1 = ex.what();
    }
    EXPECT_EQ("The number of eventKey, eventTypes, eventForms and eventExtraParams must have the same length.", re1);
}

TEST_F(CEPEventTest, test_attrExtraParams_overflow)
{
    conn300->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time", "decimal32", "decimal64", "decimal128"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR};
    schema->fieldExtraParams_ = {0, 0, 10, 19, 39};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {};
    vector<string> commonKeys = {};
    string re = "";
    try{
        EventSender* sender = new EventSender(conn300, "inputTable", eventSchemas, eventTimeKeys, commonKeys);

    }catch(exception& ex){
        re = ex.what();
    }
    EXPECT_EQ("Scale out of bound (valid range: [0, 9], but get: 10)", re);

    schema->fieldExtraParams_ = {0, 0, -1, -1, -1};
    string re1 = "";
    try{
        EventClient* client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);

    }catch(exception& ex){
        re1 = ex.what();
    }
    EXPECT_EQ("Scale out of bound (valid range: [0, 9], but get: 10)", re1);
}

TEST_F(CEPEventTest, test_eventTimeKeys_not_exist)
{
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time", "decimal32", "decimal64", "decimal128"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {"time1"};
    vector<string> commonKeys = {};
    string re = "";
    try{
        EventSender* sender = new EventSender(conn300, "", eventSchemas, eventTimeKeys, commonKeys);

    }catch(exception& ex){
        re = ex.what();
    }
    EXPECT_EQ("Event market doesn't contain eventTimeKey time1", re);

    string re1 = "";
    try{
        EventClient* client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);

    }catch(exception& ex){
        re1 = ex.what();
    }
    EXPECT_EQ("Event market doesn't contain eventTimeKey time1", re1);
}

TEST_F(CEPEventTest, test_eventTimeKeys_not_time_column)
{
    conn300->run("share streamTable(1:0, `time`eventType`event, [STRING,STRING,BLOB]) as inputTable;");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time", "decimal32", "decimal64", "decimal128"};
    schema->fieldTypes_ = {DT_STRING, DT_STRING, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {"time"};
    vector<string> commonKeys = {};
    string re = "";
    try{
        EventSender* sender = new EventSender(conn300, "inputTable", eventSchemas, eventTimeKeys, commonKeys);

    }catch(exception& ex){
        re = ex.what();
    }
    EXPECT_EQ("The first column of the output table must be temporal if eventTimeKey is specified.", re);

    string re1 = "";
    try{
        EventClient* client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
        client->subscribe(hostName, port300, test_handler, "inputTable", DEFAULT_ACTION_NAME, 0);
    }catch(exception& ex){
        re1 = ex.what();
    }
    EXPECT_EQ("The first column of the output table must be temporal if eventTimeKey is specified.", re1);
}

TEST_F(CEPEventTest, test_eventTimeKeys_two_column)
{
    conn300->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;"
                "share streamTable(1:0, `market`time, [STRING,TIME]) as outputTable;");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR};
    EventSchema *schema1 = new EventSchema();
    schema1->eventType_ = "market1";
    schema1->fieldNames_ = {"time", "time1"};
    schema1->fieldTypes_ = {DT_TIME, DT_TIME};
    schema1->fieldForms_={DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema, *schema1};
    vector<string> eventTimeKeys = {"time", "time1"};
    vector<string> commonKeys = {};
    EventSender* sender = new EventSender(conn300, "inputTable", eventSchemas, eventTimeKeys, commonKeys);
    EventClient* client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
    client->subscribe(hostName, port300, test_handler, "inputTable", DEFAULT_ACTION_NAME, 0);

    vector<ConstantSP> attributes = {Util::createString("str"), Util::createTime(12,45,3,100)};
    sender->sendEvent("market", attributes);
    vector<ConstantSP> attributes1 = {Util::createTime(1,1,20,100), Util::createTime(8,30,59,100)};
    sender->sendEvent("market1", attributes1);
    Util::sleep(2000);
    client->unsubscribe(hostName, port300, "inputTable", DEFAULT_ACTION_NAME);

    vector<string> outs = subInputTable("inputTable", 2, eventSchemas, eventTimeKeys, commonKeys);
    TableSP re = conn300->run("select * from " + outs[0]);
    TableSP re1 = conn300->run("select * from " + outs[1]);
    TableSP re2 = conn300->run("select * from outputTable");
    // cout << re2->getString() << endl;
    EXPECT_EQ(1, re->rows());
    EXPECT_EQ("str", re->getColumn(0)->get(0)->getString());
    EXPECT_EQ("12:45:03.100", re->getColumn(1)->get(0)->getString());
    EXPECT_EQ(1, re1->rows());
    EXPECT_EQ("01:01:20.100", re1->getColumn(0)->get(0)->getString());
    EXPECT_EQ("08:30:59.100", re1->getColumn(1)->get(0)->getString());
    EXPECT_EQ(2, re2->rows());
    EXPECT_EQ("str", re2->getColumn(0)->get(0)->getString());
    EXPECT_EQ("12:45:03.100", re2->getColumn(1)->get(0)->getString());
    EXPECT_EQ("01:01:20.100", re2->getColumn(0)->get(1)->getString());
    EXPECT_EQ("08:30:59.100", re2->getColumn(1)->get(1)->getString());

    delete sender, schema, schema1, client;

}

TEST_F(CEPEventTest, test_EventClient_fields_null_but_subTable_has_column)
{
    conn300->run("share streamTable(1:0, `time`eventType`event`common, [TIME,STRING,BLOB, INT]) as inputTable;"
                "share streamTable(1:0, `market`time, [STRING,TIME]) as outputTable;");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema};
    EventClient* client = new EventClient(eventSchemas, {}, {});

    EXPECT_ANY_THROW(client->subscribe(hostName, port300, test_handler, "inputTable", DEFAULT_ACTION_NAME, 0));
    delete schema, client;
}

TEST_F(CEPEventTest, test_commonKeys_one_column)
{
    conn300->run("share streamTable(1:0, `time`eventType`event`time1, [TIME,STRING,BLOB,TIME]) as inputTable;"
                "share streamTable(1:0, `market`time, [STRING,TIME]) as outputTable;");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR};
    EventSchema *schema1 = new EventSchema();
    schema1->eventType_ = "market1";
    schema1->fieldNames_ = {"time", "time1"};
    schema1->fieldTypes_ = {DT_TIME, DT_TIME};
    schema1->fieldForms_={DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema, *schema1};
    vector<string> eventTimeKeys = {"time", "time1"};
    vector<string> commonKeys = {"time"};
    EventSender* sender = new EventSender(conn300, "inputTable", eventSchemas, eventTimeKeys, commonKeys);
    EventClient* client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
    client->subscribe(hostName, port300, test_handler, "inputTable", DEFAULT_ACTION_NAME, 0);

    vector<ConstantSP> attributes = {Util::createString("str"), Util::createTime(12,45,3,100)};
    sender->sendEvent("market", attributes);
    vector<ConstantSP> attributes1 = {Util::createTime(1,1,20,100), Util::createTime(8,30,59,100)};
    sender->sendEvent("market1", attributes1);
    Util::sleep(2000);
    client->unsubscribe(hostName, port300, "inputTable", DEFAULT_ACTION_NAME);

    auto outs = subInputTable("inputTable", 2, eventSchemas, eventTimeKeys, commonKeys);
    TableSP re = conn300->run("select * from " + outs[0]);
    TableSP re1 = conn300->run("select * from " + outs[1]);
    TableSP re2 = conn300->run("select * from outputTable");
    EXPECT_EQ(1, re->rows());
    EXPECT_EQ("str", re->getColumn(0)->get(0)->getString());
    EXPECT_EQ("12:45:03.100", re->getColumn(1)->get(0)->getString());
    EXPECT_EQ(1, re1->rows());
    EXPECT_EQ("01:01:20.100", re1->getColumn(0)->get(0)->getString());
    EXPECT_EQ("08:30:59.100", re1->getColumn(1)->get(0)->getString());
    EXPECT_EQ(2, re2->rows());
    EXPECT_EQ("str", re2->getColumn(0)->get(0)->getString());
    EXPECT_EQ("12:45:03.100", re2->getColumn(1)->get(0)->getString());
    EXPECT_EQ("01:01:20.100", re2->getColumn(0)->get(1)->getString());
    EXPECT_EQ("08:30:59.100", re2->getColumn(1)->get(1)->getString());
}

TEST_F(CEPEventTest, test_eventSchema_exception)
{
    conn300->run("share streamTable(1:0, `eventType`event, [STRING,BLOB]) as inputTable;");
    EventSchema *schema1 = new EventSchema();
    schema1->eventType_ = "market";
    schema1->fieldNames_ = {"market", "error_col"};
    schema1->fieldTypes_ = {DT_STRING, (DATA_TYPE)-1}; // negative type
    schema1->fieldForms_={DF_SCALAR, DF_SCALAR};

    EventSchema *schema2 = new EventSchema();
    schema2->eventType_ = "market";
    schema2->fieldNames_ = {"market", "error_col"};
    schema2->fieldTypes_ = {DT_STRING, (DATA_TYPE)110}; // type > DATA_TYPE::DT_OBJECT
    schema2->fieldForms_={DF_SCALAR, DF_SCALAR};

    vector<EventSchema> eventSchemas1 = {*schema1};
    vector<EventSchema> eventSchemas2 = {*schema2};

    EXPECT_ANY_THROW(EventSender(conn300, "", eventSchemas1, {}, {}));
    EXPECT_ANY_THROW(EventSender(conn300, "", eventSchemas2, {}, {}));
    EXPECT_ANY_THROW(EventClient(eventSchemas1, {}, {}));
    EXPECT_ANY_THROW(EventClient(eventSchemas2, {}, {}));

    delete schema1, schema2;
}


TEST_F(CEPEventTest, test_two_schemas_two_commonKeys)
{
    conn300->run("share streamTable(1:0, `eventType`event`time`sym, [STRING,BLOB,TIME,STRING]) as inputTable;"
                "share streamTable(1:0, `market`time`sym, [STRING,TIME,STRING]) as outputTable;");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time", "sym"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME, DT_STRING};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR, DF_SCALAR};
    EventSchema *schema1 = new EventSchema();
    schema1->eventType_ = "market1";
    schema1->fieldNames_ = {"market1", "time", "sym"};
    schema1->fieldTypes_ = {DT_STRING, DT_TIME, DT_STRING};
    schema1->fieldForms_={DF_SCALAR, DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema, *schema1};
    vector<string> eventTimeKeys = {};
    vector<string> commonKeys = {"time", "sym"};
    EventSender* sender = new EventSender(conn300, "inputTable", eventSchemas, eventTimeKeys, commonKeys);
    EventClient* client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
    client->subscribe(hostName, port300, test_handler, "inputTable", DEFAULT_ACTION_NAME, 0);

    vector<ConstantSP> attributes = {Util::createString("sz"), Util::createTime(12,45,3,100), Util::createString("sz000001")};
    sender->sendEvent("market", attributes);
    vector<ConstantSP> attributes1 = {Util::createString("sh"), Util::createTime(8,30,59,100), Util::createString("sh000001")};
    sender->sendEvent("market1", attributes1);
    Util::sleep(2000);
    client->unsubscribe(hostName, port300, "inputTable", DEFAULT_ACTION_NAME);

    auto outs = subInputTable("inputTable", 2, eventSchemas, eventTimeKeys, commonKeys);
    TableSP re = conn300->run("select * from " + outs[0]);
    TableSP re1 = conn300->run("select * from " + outs[1]);
    TableSP re2 = conn300->run("select * from outputTable");
    EXPECT_EQ(1, re->rows());
    EXPECT_EQ("sz", re->getColumn(0)->get(0)->getString());
    EXPECT_EQ("12:45:03.100", re->getColumn(1)->get(0)->getString());
    EXPECT_EQ("sz000001", re->getColumn(2)->get(0)->getString());
    EXPECT_EQ(1, re1->rows());
    EXPECT_EQ("sh", re1->getColumn(0)->get(0)->getString());
    EXPECT_EQ("08:30:59.100", re1->getColumn(1)->get(0)->getString());
    EXPECT_EQ("sh000001", re1->getColumn(2)->get(0)->getString());
    EXPECT_EQ(2, re2->rows());
    EXPECT_EQ("sz", re2->getColumn(0)->get(0)->getString());
    EXPECT_EQ("12:45:03.100", re2->getColumn(1)->get(0)->getString());
    EXPECT_EQ("sz000001", re2->getColumn(2)->get(0)->getString());
    EXPECT_EQ("sh", re2->getColumn(0)->get(1)->getString());
    EXPECT_EQ("08:30:59.100", re2->getColumn(1)->get(1)->getString());
    EXPECT_EQ("sh000001", re2->getColumn(2)->get(1)->getString());

    delete sender, schema, schema1;
}


TEST_F(CEPEventTest, test_commonKeys_eventTimeKeys_with_same_col)
{
    conn300->run("share streamTable(1:0, `time`eventType`event`time2`sym, [TIME,STRING,BLOB,TIME,STRING]) as inputTable;"
                "share streamTable(1:0, `market`time`sym, [STRING,TIME,STRING]) as outputTable;");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time", "sym"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME, DT_STRING};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR, DF_SCALAR};
    EventSchema *schema1 = new EventSchema();
    schema1->eventType_ = "market1";
    schema1->fieldNames_ = {"market1", "time", "sym"};
    schema1->fieldTypes_ = {DT_STRING, DT_TIME, DT_STRING};
    schema1->fieldForms_={DF_SCALAR, DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema, *schema1};
    vector<string> eventTimeKeys = {"time"};
    vector<string> commonKeys = {"time", "sym"};
    EventSender* sender = new EventSender(conn300, "inputTable", eventSchemas, eventTimeKeys, commonKeys);
    EventClient* client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
    client->subscribe(hostName, port300, test_handler, "inputTable", DEFAULT_ACTION_NAME, 0);

    vector<ConstantSP> attributes = {Util::createString("sz"), Util::createTime(12,45,3,100), Util::createString("sz000001")};
    sender->sendEvent("market", attributes);
    vector<ConstantSP> attributes1 = {Util::createString("sh"), Util::createTime(8,30,59,100), Util::createString("sh000001")};
    sender->sendEvent("market1", attributes1);
    Util::sleep(2000);
    client->unsubscribe(hostName, port300, "inputTable", DEFAULT_ACTION_NAME);

    auto outs = subInputTable("inputTable", 2, eventSchemas, eventTimeKeys, commonKeys);
    TableSP re = conn300->run("select * from " + outs[0]);
    TableSP re1 = conn300->run("select * from " + outs[1]);
    TableSP re2 = conn300->run("select * from outputTable");

    EXPECT_EQ(1, re->rows());
    EXPECT_EQ("sz", re->getColumn(0)->get(0)->getString());
    EXPECT_EQ("12:45:03.100", re->getColumn(1)->get(0)->getString());
    EXPECT_EQ("sz000001", re->getColumn(2)->get(0)->getString());
    EXPECT_EQ(1, re1->rows());
    EXPECT_EQ("sh", re1->getColumn(0)->get(0)->getString());
    EXPECT_EQ("08:30:59.100", re1->getColumn(1)->get(0)->getString());
    EXPECT_EQ("sh000001", re1->getColumn(2)->get(0)->getString());
    EXPECT_EQ(2, re2->rows());
    EXPECT_EQ("sz", re2->getColumn(0)->get(0)->getString());
    EXPECT_EQ("12:45:03.100", re2->getColumn(1)->get(0)->getString());
    EXPECT_EQ("sz000001", re2->getColumn(2)->get(0)->getString());
    EXPECT_EQ("sh", re2->getColumn(0)->get(1)->getString());
    EXPECT_EQ("08:30:59.100", re2->getColumn(1)->get(1)->getString());
    EXPECT_EQ("sh000001", re2->getColumn(2)->get(1)->getString());

    delete sender, schema, schema1;
}

TEST_F(CEPEventTest, test_EventSender_connect_not_connect)
{
    conn300->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {"time"};
    vector<string> commonKeys = {};

    string re = "";
    DBConnectionSP tmp_connsp = new DBConnection();
    try{
        EventSender* sender = new EventSender(tmp_connsp, "inputTable", eventSchemas, eventTimeKeys, commonKeys);
    }catch(exception& ex){
        re = ex.what();
    }
    EXPECT_EQ("Couldn't send script/function to the remote host because the connection has been closed", re);

    delete schema;
}

TEST_F(CEPEventTest, test_EventClient_error_hostinfo)
{
    conn300->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {"time"};
    vector<string> commonKeys = {};
    EventClient* client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
    string re = "";

    try{
        client->subscribe(hostName, -100, test_handler, "inputTable", DEFAULT_ACTION_NAME, 0);
    }catch(exception& ex){
        re = ex.what();
    }
    EXPECT_EQ("Subscribe Fail, cannot connect to 127.0.0.1 : -100", re);
    EXPECT_ANY_THROW(client->unsubscribe("", port300, "inputTable", DEFAULT_ACTION_NAME));

    delete client, schema;
}

TEST_F(CEPEventTest, test_EventClient_sub_twice_with_same_actionName)
{
    conn300->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {"time"};
    vector<string> commonKeys = {};
    EventClient* client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
    client->subscribe(hostName, port300, test_handler, "inputTable", DEFAULT_ACTION_NAME, 0, false);
    string re = "";
    try{
        client->subscribe(hostName, port300, test_handler, "inputTable", DEFAULT_ACTION_NAME, 0, false);
    }catch(exception& ex){
        re = ex.what();
    }
    auto pos = re.find("already be subscribed");
    EXPECT_TRUE(pos != std::string::npos);
    client->unsubscribe(hostName, port300, "inputTable", DEFAULT_ACTION_NAME);
    delete client, schema;

}

TEST_F(CEPEventTest, test_EventSender_commonKeys_cols_not_match)
{
    conn300->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time", "col1"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME, DT_INT};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {};
    vector<string> commonKeys = {"time", "col1"};

    string re = "";
    try{
        EventSender* sender = new EventSender(conn300, "inputTable", eventSchemas, eventTimeKeys, commonKeys);
    }catch(exception &ex){
        re = ex.what();
    }
    EXPECT_EQ("Incompatible outputTable columnns, expected: 4, got: 3", re);
    delete schema;
}

TEST_F(CEPEventTest, test_EventClient_commonKeys_cols_not_match)
{
    conn300->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;"
                "share streamTable(1:0, `market`time`col1, [STRING,TIME, INT]) as outputTable;");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time", "col1"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME, DT_INT};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {"time"};
    vector<string> err_commonKeys = {"time", "col2"};
    string re = "";
    try{
        EventClient* client = new EventClient(eventSchemas, eventTimeKeys, err_commonKeys);
    }catch(exception &ex){
        re = ex.what();
    }
    EXPECT_EQ("Event market doesn't contain commonField col2", re);

    delete schema;
}

TEST_F(CEPEventTest, test_EventSender_conn_asynchronousTask_true)
{
    conn300->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {"time"};
    vector<string> commonKeys = {};
    DBConnectionSP tmp_connsp = new DBConnection(false, true);
    tmp_connsp->connect(hostName, port300, "admin", "123456");
    EXPECT_ANY_THROW(EventSender(tmp_connsp, "inputTable", eventSchemas, eventTimeKeys, commonKeys));

    delete schema, tmp_connsp;
}

TEST_F(CEPEventTest, test_EventSender_conn_ssl_true)
{
    conn300->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {"time"};
    vector<string> commonKeys = {};
    DBConnectionSP tmp_connsp = new DBConnection(true, false);
    tmp_connsp->connect(hostName, port300, "admin", "123456");
    EventSender* sender = new EventSender(tmp_connsp, "inputTable", eventSchemas, eventTimeKeys, commonKeys);

    vector<ConstantSP> attributes = {Util::createString("sz"), Util::createTime(12,45,3,100)};
    sender->sendEvent("market", attributes);
    auto outs = subInputTable("inputTable", 1, eventSchemas, eventTimeKeys, commonKeys);
    TableSP re = conn300->run("select * from " + outs[0]);
    EXPECT_EQ(1, re->rows());
    EXPECT_EQ("sz", re->getColumn(0)->get(0)->getString());
    EXPECT_EQ("12:45:03.100", re->getColumn(1)->get(0)->getString());

    delete sender, schema, tmp_connsp;
}

TEST_F(CEPEventTest, test_EventSender_conn_compress_true)
{
    conn300->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {"time"};
    vector<string> commonKeys = {};
    DBConnectionSP tmp_connsp = new DBConnection(false, false, 7200, true);
    tmp_connsp->connect(hostName, port300, "admin", "123456");
    EventSender* sender = new EventSender(tmp_connsp, "inputTable", eventSchemas, eventTimeKeys, commonKeys);

    vector<ConstantSP> attributes = {Util::createString("sz"), Util::createTime(12,45,3,100)};
    sender->sendEvent("market", attributes);
    auto outs = subInputTable("inputTable", 1, eventSchemas, eventTimeKeys, commonKeys);
    TableSP re = conn300->run("select * from " + outs[0]);
    EXPECT_EQ(1, re->rows());
    EXPECT_EQ("sz", re->getColumn(0)->get(0)->getString());
    EXPECT_EQ("12:45:03.100", re->getColumn(1)->get(0)->getString());

    delete sender, schema, tmp_connsp;
}

TEST_F(CEPEventTest, test_EventSender_conn_not_admin)
{
    conn300->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {"time"};
    vector<string> commonKeys = {};
    DBConnectionSP tmp_connsp = new DBConnection(false, false, 7200, false);
    tmp_connsp->connect(hostName, port300);
    EXPECT_EQ(tmp_connsp->run("getCurrentSessionAndUser()[1]")->getString(), "guest");
    EventSender* sender = new EventSender(tmp_connsp, "inputTable", eventSchemas, eventTimeKeys, commonKeys);

    vector<ConstantSP> attributes = {Util::createString("sz"), Util::createTime(12,45,3,100)};
    sender->sendEvent("market", attributes);
    auto outs = subInputTable("inputTable", 1, eventSchemas, eventTimeKeys, commonKeys);
    TableSP re = conn300->run("select * from " + outs[0]);
    EXPECT_EQ(1, re->rows());
    EXPECT_EQ("sz", re->getColumn(0)->get(0)->getString());
    EXPECT_EQ("12:45:03.100", re->getColumn(1)->get(0)->getString());

    delete sender, schema, tmp_connsp;
}

TEST_F(CEPEventTest, test_eventTable_not_exist)
{
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {"time"};
    vector<string> commonKeys = {};

    string re = "";
    try{
        EventSender* sender = new EventSender(conn300, "notExistTable", eventSchemas, eventTimeKeys, commonKeys);
    }catch(const exception& ex){
        re = ex.what();
    }
    EXPECT_TRUE(re.find("Can't find the object with name notExistTable") != std::string::npos);

    EventClient* client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
    string re1 = "";
    try{
        auto th = client->subscribe(hostName, port300, test_handler, "notExistTable", DEFAULT_ACTION_NAME, 0, false);
        th->join();
    }catch(const exception& ex){
        re1 = ex.what();
    }
    EXPECT_PRED_FORMAT2(testing::IsSubstring,"Can't find the object with name notExistTable", re1);

    delete schema, client;
}

TEST_F(CEPEventTest, test_eventTable_not_contail_eventCol)
{
    conn300->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,STRING]) as inputTable;");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {"time"};
    vector<string> commonKeys = {};

    string re = "";
    try{
        EventSender* sender = new EventSender(conn300, "inputTable", eventSchemas, eventTimeKeys, commonKeys);
    }catch(const exception& ex){
        re = ex.what();
    }
    EXPECT_TRUE(re.find("The event column of the output table must be BLOB type") != std::string::npos);

    EventClient* client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
    string re1 = "";
    try{
        auto th = client->subscribe(hostName, port300, test_handler, "inputTable", DEFAULT_ACTION_NAME, 0, false);
        th->join();
    }catch(const exception& ex){
        re1 = ex.what();
    }
    EXPECT_PRED_FORMAT2(testing::IsSubstring,"The event column of the output table must be BLOB type.", re1);

    delete schema, client;
}

TEST_F(CEPEventTest, test_connect_tableName_null)
{
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {"time"};
    vector<string> commonKeys = {};

    string re = "";
    try{
        EventSender* sender = new EventSender(conn300, "", eventSchemas, eventTimeKeys, commonKeys);
    }catch(exception& ex){
        re = ex.what();
    }
    EXPECT_EQ("tableName must not be empty.", re);

    EventClient* client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
    string re1 = "";
    try{
        auto th = client->subscribe(hostName, port300, test_handler, "", DEFAULT_ACTION_NAME, 0, false);
        th->join();
    }catch(exception& ex){
        re1 = ex.what();
    }
    EXPECT_EQ("tableName must not be empty.", re1);

    delete client, schema;
}

TEST_F(CEPEventTest, test_EventSender_sendEvent_eventType_not_exist)
{
    conn300->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {"time"};
    vector<string> commonKeys = {};
    EventSender* sender = new EventSender(conn300, "inputTable", eventSchemas, eventTimeKeys, commonKeys);
    string re = "";
    try{
        sender->sendEvent("market1", {Util::createString("sz"), Util::createTime(12,45,3,100)});
    }catch(exception& ex){
        re = ex.what();
    }
    EXPECT_EQ("serialize event Fail for unknown eventType market1", re);
    delete sender, schema;
}

TEST_F(CEPEventTest, test_EventSender_sendEvent_eventType_null)
{
    conn300->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {"time"};
    vector<string> commonKeys = {};
    EventSender* sender = new EventSender(conn300, "inputTable", eventSchemas, eventTimeKeys, commonKeys);
    string re = "";
    try{
        sender->sendEvent("", {Util::createString("sz"), Util::createTime(12,45,3,100)});
    }catch(exception& ex){
        re = ex.what();
    }
    EXPECT_EQ("serialize event Fail for unknown eventType ", re);
    delete sender, schema;
}

TEST_F(CEPEventTest, test_EventClient_sub_eventType_null)
{
    conn300->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {"time"};
    vector<string> commonKeys = {};
    string re = "";
    try{
        EventClient* client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);


        auto th = client->subscribe(hostName, port300, test_handler, "inputTable", DEFAULT_ACTION_NAME, 0);
        th->join();
    }catch(exception& ex){
        re = ex.what();
    }
    EXPECT_EQ("eventType must not be empty.", re);
    delete schema;

}

TEST_F(CEPEventTest, test_EventSender_sendEvent_attributes_column_not_match)
{
    conn300->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {"time"};
    vector<string> commonKeys = {};
    EventSender* sender = new EventSender(conn300, "inputTable", eventSchemas, eventTimeKeys, commonKeys);
    string re = "";
    try{
        sender->sendEvent("market", {Util::createString("sz"), Util::createTime(12,45,3,100), Util::createString("sz")});
    }catch(const exception& ex){
        re = ex.what();
    }
    EXPECT_EQ("serialize event Fail for the number of event values does not match market", re);
    delete sender, schema;
}

TEST_F(CEPEventTest, test_EventSender_sendEvent_attributes_type_not_match)
{
    conn300->run("share streamTable(1:0, `time`eventType`event, [TIME,STRING,BLOB]) as inputTable;");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {"time"};
    vector<string> commonKeys = {};
    EventSender* sender = new EventSender(conn300, "inputTable", eventSchemas, eventTimeKeys, commonKeys);
    string re = "";
    try{
        sender->sendEvent("market", {Util::createString("sz"), Util::createInt(12)});
    }catch(const exception& ex){
        re = ex.what();
    }
    EXPECT_EQ("serialize event Fail for Expected type for the field time of market : TIME, but now it is INT", re);
    delete sender, schema;
}

class CEPEventTest_EventSender_alltypes : public CEPEventTest, public ::testing::WithParamInterface<tuple<DATA_TYPE, string>> {
public:
    static vector<tuple<DATA_TYPE, string>> get_scalar_data(){
        return {
            std::make_tuple(DT_BOOL, "array(BOOL).append!(true false NULL)"),std::make_tuple(DT_CHAR, "-127c 127c NULL"),
            std::make_tuple(DT_SHORT, "1000h NULL"),std::make_tuple(DT_INT, "10000 NULL"),
            std::make_tuple(DT_LONG, "1000000l NULL"),std::make_tuple(DT_FLOAT, "100.000f NULL"),
            std::make_tuple(DT_DOUBLE, "100.0000 NULL"),std::make_tuple(DT_DATE, "2023.01.01 NULL"),
            std::make_tuple(DT_MONTH, "2023.01M NULL"),std::make_tuple(DT_TIME, "23:45:03.100 NULL"),
            std::make_tuple(DT_MINUTE, "23:45m NULL"),std::make_tuple(DT_SECOND, "23:45:03 NULL"),
            std::make_tuple(DT_DATEHOUR, "datehour('2021.01.01T12'`)"),std::make_tuple(DT_STRING, "`szstr`"),
            std::make_tuple(DT_DATETIME, "2021.01.01T12:45:03 NULL"),std::make_tuple(DT_TIMESTAMP, "2021.01.01T12:45:03.100 NULL"),
            std::make_tuple(DT_NANOTIME, "23:45:03.100000000 NULL"),std::make_tuple(DT_NANOTIMESTAMP, "2021.01.01T12:45:03.100000000 NULL"),
            // std::make_tuple(DT_SYMBOL, "`sz`sh`bj`"), // symbol scalar not support in server
            std::make_tuple(DT_UUID, "uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87''5d212a78-cc48-e3b1-4235-b4d91473ee10'`)"),
            std::make_tuple(DT_IP, "ipaddr('127.0.0.1''1.1.1.1'`)"),
            std::make_tuple(DT_INT128, "int128(`e1671797c52e15f763380b45e841ec32`e1671797c52e15f763380b45e841ec42`)"),
            std::make_tuple(DT_BLOB, "blob(`blob`blob2`)"),
            std::make_tuple(DT_DECIMAL32, "decimal32('1.123456789'`'-135.1', 5)"),std::make_tuple(DT_DECIMAL64, "decimal64('-11.456464'`'300.1', 15)"),
            std::make_tuple(DT_DECIMAL128, "decimal128('999.64621462333'`'-1326', 25)"),
        };
    };
};

INSTANTIATE_TEST_SUITE_P(EventSender_alltype, CEPEventTest_EventSender_alltypes, testing::ValuesIn(CEPEventTest_EventSender_alltypes::get_scalar_data()));
TEST_P(CEPEventTest_EventSender_alltypes, test_EventSender_alltype)
{
    DATA_TYPE dataType = get<0>(GetParam());
    string type_data_str = get<1>(GetParam());
    string typeString = Util::getDataTypeString(dataType);
    int extraParam = 0;
    if (typeString.compare(0, 9, "DECIMAL32") == 0){typeString = "DECIMAL32(5)"; extraParam = 5;}
    else if (typeString.compare(0, 9, "DECIMAL64") == 0){typeString = "DECIMAL64(15)"; extraParam = 15;}
    else if (typeString.compare(0, 10, "DECIMAL128") == 0){typeString = "DECIMAL128(25)"; extraParam = 25;}

    cout << "test type: " << typeString << endl;

    conn300->run("share streamTable(1:0, `eventTime`eventType`event`commonCol, [TIMESTAMP,STRING,BLOB,"+typeString+"]) as fromApi;"
                "share streamTable(1:0, `eventTime`eventType`event`commonCol, [TIMESTAMP,STRING,BLOB,"+typeString+"]) as fromCEP;go;"
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
                "class MainMonitor{"
                    "def MainMonitor(){}\n"
                    "def updateMarketData(event)\n"
                    "def onload(){"
                        "addEventListener(updateMarketData,'md')}\n"
                    "def updateMarketData(event){"
                        "emitEvent(event)}};"
                "try{dropStreamEngine(`ses)}catch(ex){};go;"
                "outputSerializer = streamEventSerializer(name=`ses, eventSchema=md, outputTable=fromCEP, eventTimeField = 'eventTime', commonField='commonCol');"
                "try{dropStreamEngine(`CEPengine)}catch(ex){};go;"
                "engine = createCEPEngine(`CEPengine, <MainMonitor()>, fromCEP, [md], 1, 'eventTime', 10000, outputSerializer);");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "md";
    schema->fieldNames_ = {"market", "time", "testCol", "commonCol"};
    schema->fieldTypes_ = {DT_STRING, DT_TIMESTAMP, dataType, dataType};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR};
    schema->fieldExtraParams_ = {0, 0, extraParam, extraParam};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {"time"};
    vector<string> commonKeys = {"commonCol"};

    EventSender* sender = new EventSender(conn300, "fromApi", eventSchemas, eventTimeKeys, commonKeys);
    int rows = 1000;
    VectorSP col0 = conn300->run("market =rand(`sz`sh`bj`adk, "+to_string(rows)+");market");
    VectorSP col1 = conn300->run("time = timestamp(rand(999000000000l, "+to_string(rows)+"));time");
    VectorSP col_test = conn300->run("testCol = rand("+type_data_str+", "+to_string(rows)+");commonCol=testCol;testCol");

    if (dataType == DT_BLOB){
        col_test = conn300->run("testCol = blob(string(rand(`blob`blob2`, "+to_string(rows)+")));commonCol=testCol;testCol");
    }

    for (int i = 0; i < rows; i++){
        vector<ConstantSP> attributes = {col0->get(i), col1->get(i), col_test->get(i), col_test->get(i)};
        sender->sendEvent("md", attributes);
        conn300->run("ind="+to_string(i)+";rowData = md(market[ind], time[ind], testCol[ind], commonCol[ind]);appendEvent(outputSerializer, [rowData])");
    }
    EXPECT_TRUE(conn300->run("all(each(eqObj, fromApi.values(), fromCEP.values()))")->getBool());
    conn300->run("dropStreamEngine(`CEPengine);dropStreamEngine(`ses);");
    delete sender, schema;
}

class CEPEventTest_EventSender_vector : public CEPEventTest, public ::testing::WithParamInterface<tuple<DATA_TYPE, string>> {
public:
    static vector<tuple<DATA_TYPE, string>> get_vector_data(){
        return {
            std::make_tuple(DT_BOOL, "true false"),std::make_tuple(DT_CHAR, "rand(127c,2)"),
            std::make_tuple(DT_SHORT, "rand(1000h,2)"),std::make_tuple(DT_INT, "rand(10000,2)"),
            std::make_tuple(DT_LONG, "rand(1000000l,2)"),std::make_tuple(DT_FLOAT, "rand(100.000f,2)"),
            std::make_tuple(DT_DOUBLE, "rand(100.0000,2)"),std::make_tuple(DT_DATE, "rand(2023.01.01,2)"),
            std::make_tuple(DT_MONTH, "rand(2023.01M,2)"),std::make_tuple(DT_TIME, "rand(23:45:03.100,2)"),
            std::make_tuple(DT_MINUTE, "rand(23:45m,2)"),std::make_tuple(DT_SECOND, "rand(23:45:03,2)"),
            std::make_tuple(DT_DATEHOUR, "rand(datehour('2021.01.01T12'),2)"),std::make_tuple(DT_DATETIME, "rand(2021.01.01T12:45:03,2)"),
            std::make_tuple(DT_TIMESTAMP, "rand(2021.01.01T12:45:03.100,2)"),std::make_tuple(DT_NANOTIME, "rand(23:45:03.100000000,2)"),
            std::make_tuple(DT_NANOTIMESTAMP, "rand(2021.01.01T12:45:03.100000000,2)"),
            std::make_tuple(DT_STRING, "rand(`sz`sh`bj,2)"),
            std::make_tuple(DT_SYMBOL, "symbol(rand(`sz`sh`bj,2))"),
            std::make_tuple(DT_UUID, "rand(uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87''5d212a78-cc48-e3b1-4235-b4d91473ee10'),2)"),
            std::make_tuple(DT_IP, "rand(ipaddr('1.1.1.1''127.0.0.1'),2)"),
            std::make_tuple(DT_INT128, "rand(int128(`e1671797c52e15f763380b45e841ec32`e1671797c52e15f763380b45e841ec42),2)"),
            std::make_tuple(DT_BLOB, "blob(`blob`blob2)"),
            std::make_tuple(DT_DECIMAL32, "rand(decimal32('1.123456789'`0'-135.1', 5),2)"),std::make_tuple(DT_DECIMAL64, "rand(decimal64('-11.456464'`0'300.1', 15),2)"),
            std::make_tuple(DT_DECIMAL128, "rand(decimal128('999.64621462333'`0'-1326', 25),2)"),
        };
    };
    static vector<tuple<DATA_TYPE, string>> get_vector_allNull_data(){
        return {
            std::make_tuple(DT_BOOL, "array(BOOL).append!(take(bool(NULL), 2))"), std::make_tuple(DT_CHAR, "array(CHAR).append!(take(char(NULL), 2))"),
            std::make_tuple(DT_SHORT, "array(SHORT).append!(take(short(NULL), 2))"), std::make_tuple(DT_INT, "array(INT).append!(take(int(NULL), 2))"),
            std::make_tuple(DT_LONG, "array(LONG).append!(take(long(NULL), 2))"), std::make_tuple(DT_FLOAT, "array(FLOAT).append!(take(float(NULL), 2))"),
            std::make_tuple(DT_DOUBLE, "array(DOUBLE).append!(take(double(NULL), 2))"), std::make_tuple(DT_DATE, "array(DATE).append!(take(date(NULL), 2))"),
            std::make_tuple(DT_MONTH, "array(MONTH).append!(take(month(NULL), 2))"), std::make_tuple(DT_TIME, "array(TIME).append!(take(time(NULL), 2))"),
            std::make_tuple(DT_MINUTE, "array(MINUTE).append!(take(minute(NULL), 2))"), std::make_tuple(DT_SECOND, "array(SECOND).append!(take(second(NULL), 2))"),
            std::make_tuple(DT_DATEHOUR, "array(DATEHOUR).append!(take(datehour(NULL), 2))"), std::make_tuple(DT_DATETIME, "array(DATETIME).append!(take(datetime(NULL), 2))"),
            std::make_tuple(DT_TIMESTAMP, "array(TIMESTAMP).append!(take(timestamp(NULL), 2))"), std::make_tuple(DT_NANOTIME, "array(NANOTIME).append!(take(nanotime(NULL), 2))"),
            std::make_tuple(DT_NANOTIMESTAMP, "array(NANOTIMESTAMP).append!(take(nanotimestamp(NULL), 2))"),
            std::make_tuple(DT_STRING, "array(STRING).append!(take(string(NULL), 2))"),
            std::make_tuple(DT_SYMBOL, "array(SYMBOL).append!(take(symbol(\"\"\"\"), 2))"),
            std::make_tuple(DT_UUID, "array(UUID).append!(take(uuid(''), 2))"),std::make_tuple(DT_IP, "array(IPADDR).append!(take(ipaddr(''), 2))"),
            std::make_tuple(DT_INT128, "array(INT128).append!(take(int128(''), 2))"), std::make_tuple(DT_BLOB, "array(BLOB).append!(take(blob(''), 2))"),
            std::make_tuple(DT_DECIMAL32, "array(DECIMAL32(5)).append!(take(decimal32(NULL,5), 2))"), std::make_tuple(DT_DECIMAL64, "array(DECIMAL64(15)).append!(take(decimal64(NULL,15), 2))"),
            std::make_tuple(DT_DECIMAL128, "array(DECIMAL128(25)).append!(take(decimal128(NULL,25), 2))"),
        };
    };
};

INSTANTIATE_TEST_SUITE_P(vector_noNull, CEPEventTest_EventSender_vector, testing::ValuesIn(CEPEventTest_EventSender_vector::get_vector_data()));
INSTANTIATE_TEST_SUITE_P(vector_allNull, CEPEventTest_EventSender_vector, testing::ValuesIn(CEPEventTest_EventSender_vector::get_vector_allNull_data()));
TEST_P(CEPEventTest_EventSender_vector, test_EventSender_vector)
{
    DATA_TYPE dataType = get<0>(GetParam());
    string type_data_str = get<1>(GetParam());
    string typeString = Util::getDataTypeString(dataType);
    int extraParam = 0;
    if (typeString.compare(0, 9, "DECIMAL32") == 0){typeString = "DECIMAL32(5)"; extraParam = 5;}
    else if (typeString.compare(0, 9, "DECIMAL64") == 0){typeString = "DECIMAL64(15)"; extraParam = 15;}
    else if (typeString.compare(0, 10, "DECIMAL128") == 0){typeString = "DECIMAL128(25)"; extraParam = 25;}

    cout << "test type: " << typeString << endl;
    string formatStr1 ="", formatStr2 ="";
    if (dataType != DT_SYMBOL && dataType != DT_STRING && dataType != DT_BLOB){
        formatStr1 = typeString + "[]";
        formatStr2 = typeString + " VECTOR";
    }else{
        formatStr1 = "INT[]";
        formatStr2 = "INT VECTOR";
    }
    conn300->run("share streamTable(1:0, `eventTime`eventType`event`commonCol, [TIMESTAMP,STRING,BLOB,"+formatStr1+"]) as fromApi;"
                "share streamTable(1:0, `eventTime`eventType`event`commonCol, [TIMESTAMP,STRING,BLOB,"+formatStr1+"]) as fromCEP;go;"
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
                "class MainMonitor{"
                    "def MainMonitor(){}\n"
                    "def updateMarketData(event)\n"
                    "def onload(){"
                        "addEventListener(updateMarketData,'md')}\n"
                    "def updateMarketData(event){"
                        "emitEvent(event)}};"
                "try{dropStreamEngine(`ses)}catch(ex){};go;"
                "outputSerializer = streamEventSerializer(name=`ses, eventSchema=md, outputTable=fromCEP, eventTimeField = 'eventTime', commonField='commonCol');"
                "try{dropStreamEngine(`CEPengine)}catch(ex){};go;"
                "engine = createCEPEngine(`CEPengine, <MainMonitor()>, fromCEP, [md], 1, 'eventTime', 10000, outputSerializer);");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "md";
    schema->fieldNames_ = {"market", "time", "testCol", "commonCol"};
    schema->fieldTypes_ = {DT_STRING, DT_TIMESTAMP, dataType, dataType == DT_SYMBOL||dataType == DT_STRING||dataType == DT_BLOB? DT_INT:dataType};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR, DF_VECTOR, DF_VECTOR};
    schema->fieldExtraParams_ = {0, 0, extraParam, extraParam};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {"time"};
    vector<string> commonKeys = {"commonCol"};

    EventSender* sender = new EventSender(conn300, "fromApi", eventSchemas, eventTimeKeys, commonKeys);
    int rows = 1000;
    VectorSP col0 = conn300->run("market =rand(`sz`sh`bj`adk, "+to_string(rows)+");market");
    VectorSP col1 = conn300->run("time = timestamp(rand(999000000000l, "+to_string(rows)+"));time");
    VectorSP col2 = conn300->run("testCol = rand([rand("+type_data_str+", rand(2,1)[0])], "+to_string(rows)+");testCol");
    VectorSP col3 = conn300->run("commonCol = rand([rand("+type_data_str+", rand(2,1)[0])], "+to_string(rows)+");commonCol");

    if (dataType == DT_BLOB)
        col2 = conn300->run("testCol = rand([blob(string(rand(`blob`blob2, rand(2,1)[0])))], "+to_string(rows)+");testCol");
    if (dataType == DT_SYMBOL)
        col2 = conn300->run("testCol = rand([symbol(string(rand([\"blob\"], 10)))], "+to_string(rows)+");testCol");
    if (dataType == DT_STRING || dataType == DT_SYMBOL || dataType == DT_BLOB)
        col3 = conn300->run("commonCol = rand([rand(10000, rand(2,1)[0])], "+to_string(rows)+");commonCol");

    for (int i = 0; i < rows; i++){
        vector<ConstantSP> attributes = {col0->get(i), col1->get(i), col2->get(i), col3->get(i)};
        sender->sendEvent("md", attributes);
        conn300->run("ind="+to_string(i)+";rowData = md(market[ind], time[ind], testCol[ind], commonCol[ind]);appendEvent(outputSerializer, [rowData])");
    }

    EXPECT_TRUE(conn300->run("all(each(eqObj, fromApi.values(), fromCEP.values()))")->getBool());
    conn300->run("dropStreamEngine(`CEPengine);dropStreamEngine(`ses);");
    delete sender, schema;
}

class CEPEventTest_EventSender_arrayVector : public CEPEventTest, public ::testing::WithParamInterface<tuple<DATA_TYPE, string>> {
public:
    static vector<tuple<DATA_TYPE, string>> get_arrayVector_data(){
        return {
            std::make_tuple(DT_BOOL_ARRAY, "array(BOOL[]).append!([true false])"), std::make_tuple(DT_CHAR_ARRAY, "array(CHAR[]).append!([1c NULL])"),
            std::make_tuple(DT_SHORT_ARRAY, "array(SHORT[]).append!([1000h NULL])"), std::make_tuple(DT_INT_ARRAY, "array(INT[]).append!([10000 NULL])"),
            std::make_tuple(DT_LONG_ARRAY, "array(LONG[]).append!([1000000l NULL])"), std::make_tuple(DT_FLOAT_ARRAY, "array(FLOAT[]).append!([100.000f NULL])"),
            std::make_tuple(DT_DOUBLE_ARRAY, "array(DOUBLE[]).append!([100.0000 NULL])"), std::make_tuple(DT_DATE_ARRAY, "array(DATE[]).append!([2023.01.01 NULL])"),
            std::make_tuple(DT_MONTH_ARRAY, "array(MONTH[]).append!([2023.01M NULL])"), std::make_tuple(DT_TIME_ARRAY, "array(TIME[]).append!([23:45:03.100 NULL])"),
            std::make_tuple(DT_MINUTE_ARRAY, "array(MINUTE[]).append!([23:45m NULL])"), std::make_tuple(DT_SECOND_ARRAY, "array(SECOND[]).append!([23:45:03 NULL])"),
            std::make_tuple(DT_DATEHOUR_ARRAY, "array(DATEHOUR[]).append!([datehour('2021.01.01T12'`)])"),
            std::make_tuple(DT_DATETIME_ARRAY, "array(DATETIME[]).append!([2021.01.01T12:45:03 NULL])"),
            std::make_tuple(DT_TIMESTAMP_ARRAY, "array(TIMESTAMP[]).append!([2021.01.01T12:45:03.100 NULL])"),
            std::make_tuple(DT_NANOTIME_ARRAY, "array(NANOTIME[]).append!([23:45:03.100000000 NULL])"),
            std::make_tuple(DT_NANOTIMESTAMP_ARRAY, "array(NANOTIMESTAMP[]).append!([2021.01.01T12:45:03.100000000 NULL])"),
            std::make_tuple(DT_UUID_ARRAY, "array(UUID[]).append!([uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87'`)])"),
            std::make_tuple(DT_IP_ARRAY, "array(IPADDR[]).append!([ipaddr('1.1.1.1'`)])"),
            std::make_tuple(DT_INT128_ARRAY, "array(INT128[]).append!([int128(`e1671797c52e15f763380b45e841ec32`)])"),
            std::make_tuple(DT_DECIMAL32_ARRAY, "array(DECIMAL32(5)[]).append!([decimal32('1.123456789'`, 5)])"),
            std::make_tuple(DT_DECIMAL64_ARRAY, "array(DECIMAL64(15)[]).append!([decimal64('-11.456464'`, 15)])"),
            std::make_tuple(DT_DECIMAL128_ARRAY, "array(DECIMAL128(25)[]).append!([decimal128('999.64621462333'`, 25)])"),
        };
    };
    static vector<tuple<DATA_TYPE, string>> get_arrayVector_allNull_data(){
        return {
            std::make_tuple(DT_BOOL_ARRAY, "array(BOOL[]).append!([take(bool(NULL), 2)])"), std::make_tuple(DT_CHAR_ARRAY, "array(CHAR[]).append!([take(char(NULL), 2)])"),
            std::make_tuple(DT_SHORT_ARRAY, "array(SHORT[]).append!([take(short(NULL), 2)])"), std::make_tuple(DT_INT_ARRAY, "array(INT[]).append!([take(int(NULL), 2)])"),
            std::make_tuple(DT_LONG_ARRAY, "array(LONG[]).append!([take(long(NULL), 2)])"), std::make_tuple(DT_FLOAT_ARRAY, "array(FLOAT[]).append!([take(float(NULL), 2)])"),
            std::make_tuple(DT_DOUBLE_ARRAY, "array(DOUBLE[]).append!([take(double(NULL), 2)])"), std::make_tuple(DT_DATE_ARRAY, "array(DATE[]).append!([take(date(NULL), 2)])"),
            std::make_tuple(DT_MONTH_ARRAY, "array(MONTH[]).append!([take(month(NULL), 2)])"), std::make_tuple(DT_TIME_ARRAY, "array(TIME[]).append!([take(time(NULL), 2)])"),
            std::make_tuple(DT_MINUTE_ARRAY, "array(MINUTE[]).append!([take(minute(NULL), 2)])"), std::make_tuple(DT_SECOND_ARRAY, "array(SECOND[]).append!([take(second(NULL), 2)])"),
            std::make_tuple(DT_DATEHOUR_ARRAY, "array(DATEHOUR[]).append!([take(datehour(NULL), 2)])"),
            std::make_tuple(DT_DATETIME_ARRAY, "array(DATETIME[]).append!([take(datetime(NULL), 2)])"),std::make_tuple(DT_TIMESTAMP_ARRAY, "array(TIMESTAMP[]).append!([take(timestamp(NULL), 2)])"),
            std::make_tuple(DT_NANOTIME_ARRAY, "array(NANOTIME[]).append!([take(nanotime(NULL), 2)])"),std::make_tuple(DT_NANOTIMESTAMP_ARRAY, "array(NANOTIMESTAMP[]).append!([take(nanotimestamp(NULL), 2)])"),
            std::make_tuple(DT_UUID_ARRAY, "array(UUID[]).append!([take(uuid(), 2)])"),std::make_tuple(DT_IP_ARRAY, "array(IPADDR[]).append!([take(ipaddr(), 2)])"),
            std::make_tuple(DT_INT128_ARRAY, "array(INT128[]).append!([take(int128(), 2)])"),
            std::make_tuple(DT_DECIMAL32_ARRAY, "array(DECIMAL32(5)[]).append!([take(decimal32('', 5), 2)])"),
            std::make_tuple(DT_DECIMAL64_ARRAY, "array(DECIMAL64(15)[]).append!([take(decimal64('', 15), 2)])"),
            std::make_tuple(DT_DECIMAL128_ARRAY, "array(DECIMAL128(25)[]).append!([take(decimal128('', 25), 2)])"),
        };
    };
};
INSTANTIATE_TEST_SUITE_P(EventSender_arrayVector, CEPEventTest_EventSender_arrayVector, testing::ValuesIn(CEPEventTest_EventSender_arrayVector::get_arrayVector_data()));
INSTANTIATE_TEST_SUITE_P(EventSender_arrayVector_allNull, CEPEventTest_EventSender_arrayVector, testing::ValuesIn(CEPEventTest_EventSender_arrayVector::get_arrayVector_allNull_data()));
TEST_P(CEPEventTest_EventSender_arrayVector, test_EventSender_arrayVector) // commonFields不能包含array vector类型
{
    DATA_TYPE dataType = get<0>(GetParam());
    string type_data_str = get<1>(GetParam());
    string typeString = Util::getDataTypeString(dataType);
    int extraParam = 0;
    if (typeString.compare(0, 9, "DECIMAL32") == 0){typeString = "DECIMAL32(5)[]"; extraParam = 5;}
    else if (typeString.compare(0, 9, "DECIMAL64") == 0){typeString = "DECIMAL64(15)[]"; extraParam = 15;}
    else if (typeString.compare(0, 10, "DECIMAL128") == 0){typeString = "DECIMAL128(25)[]"; extraParam = 25;}

    cout << "test type: " << typeString << endl;

    conn300->run("share streamTable(1:0, `time`eventType`event`commonCol, [TIME,STRING,BLOB,INT]) as inputTable;");
    EventSchema *schema = new EventSchema();
    schema->eventType_ = "market";
    schema->fieldNames_ = {"market", "time", "testCol", "commonCol"};
    schema->fieldTypes_ = {DT_STRING, DT_TIME, dataType, DT_INT};
    schema->fieldForms_={DF_SCALAR, DF_SCALAR, DF_VECTOR, DF_SCALAR};
    schema->fieldExtraParams_ = {0, 0, extraParam, extraParam};
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {"time"};
    vector<string> commonKeys = {"commonCol"};

    EventSender* sender = new EventSender(conn300, "inputTable", eventSchemas, eventTimeKeys, commonKeys);
    int rows = 1000;
    VectorSP col0 = conn300->run("rand(`sz`sh`bj`adk, "+to_string(rows)+")");
    VectorSP col1 = conn300->run("time(rand(50000000, "+to_string(rows)+"))");
    VectorSP col_test = conn300->run("rand(["+type_data_str+"], "+to_string(rows)+")");
    // cout << "col_test: " << col_test->getString() << endl;
    vector<ConstantSP> columnVecs;
    columnVecs.reserve(4);
    VectorSP col_market = Util::createVector(DT_STRING, 0);
    VectorSP col_time = Util::createVector(DT_TIME, 0);
    VectorSP col_testCol = Util::createVector(dataType, 0, 0, true, extraParam);
    VectorSP col_commonCol = Util::createVector(DT_INT, 0);

    std::future<vector<string>> outs = std::async(subInputTable, "inputTable", rows, eventSchemas, eventTimeKeys, commonKeys);
    for (int i = 0; i < rows; i++){
        vector<ConstantSP> attributes = {col0->get(i), col1->get(i), col_test->get(i), Util::createInt(i)};
        col_market->append(col0->get(i));
        col_time->append(col1->get(i));
        col_testCol->append(col_test->get(i));
        col_commonCol->append(Util::createInt(i));
        // cout << col_test->get(i)->getString() << endl;
        sender->sendEvent("market", attributes);
    }

    columnVecs.emplace_back(col_market);
    columnVecs.emplace_back(col_time);
    columnVecs.emplace_back(col_testCol);
    columnVecs.emplace_back(col_commonCol);
    TableSP exTab = Util::createTable(schema->fieldNames_, columnVecs);
    // cout << exTab->getString() << endl;
    conn300->upload("ex", exTab);

    vector<string> tabNames = outs.get();
    EXPECT_TRUE(conn300->run("all(each(eqObj, "+tabNames[0]+".values(), ex.values()))")->getBool());

    delete sender, schema;
}

class CEPEventTest_EventClient_alltypes : public CEPEventTest, public ::testing::WithParamInterface<tuple<DATA_TYPE, string>> {
public:
    static vector<tuple<DATA_TYPE, string>> get_scalar_data(){
        return {
            std::make_tuple(DT_BOOL, "array(BOOL).append!(true false NULL)"),std::make_tuple(DT_CHAR, "-127c 127c NULL"),
            std::make_tuple(DT_SHORT, "1000h NULL"),std::make_tuple(DT_INT, "10000 NULL"),
            std::make_tuple(DT_LONG, "1000000l NULL"),std::make_tuple(DT_FLOAT, "100.000f NULL"),
            std::make_tuple(DT_DOUBLE, "100.0000 NULL"),std::make_tuple(DT_DATE, "2023.01.01 NULL"),
            std::make_tuple(DT_MONTH, "2023.01M NULL"),std::make_tuple(DT_TIME, "23:45:03.100 NULL"),
            std::make_tuple(DT_MINUTE, "23:45m NULL"),std::make_tuple(DT_SECOND, "23:45:03 NULL"),
            std::make_tuple(DT_DATEHOUR, "datehour('2021.01.01T12'`)"),std::make_tuple(DT_STRING, "`szstr`"),
            std::make_tuple(DT_DATETIME, "2021.01.01T12:45:03 NULL"),std::make_tuple(DT_TIMESTAMP, "2021.01.01T12:45:03.100 NULL"),
            std::make_tuple(DT_NANOTIME, "23:45:03.100000000 NULL"),std::make_tuple(DT_NANOTIMESTAMP, "2021.01.01T12:45:03.100000000 NULL"),
            std::make_tuple(DT_SYMBOL, "`sz`sh`bj`"),
            std::make_tuple(DT_UUID, "uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87''5d212a78-cc48-e3b1-4235-b4d91473ee10'`)"),
            std::make_tuple(DT_IP, "ipaddr('127.0.0.1''1.1.1.1'`)"),
            std::make_tuple(DT_INT128, "int128(`e1671797c52e15f763380b45e841ec32`e1671797c52e15f763380b45e841ec42`)"),
            std::make_tuple(DT_BLOB, "blob(`blob`blob2`)"),
            std::make_tuple(DT_DECIMAL32, "decimal32('1.123456789'`'-135.1', 5)"),std::make_tuple(DT_DECIMAL64, "decimal64('-11.456464'`'300.1', 15)"),
            std::make_tuple(DT_DECIMAL128, "decimal128('999.64621462333'`'-1326', 25)"),
        };
    };
};

INSTANTIATE_TEST_SUITE_P(EventClient_alltype, CEPEventTest_EventClient_alltypes, testing::ValuesIn(CEPEventTest_EventClient_alltypes::get_scalar_data()));
TEST_P(CEPEventTest_EventClient_alltypes, test_EventClient_alltype)
{
    DATA_TYPE dataType = get<0>(GetParam());
    string type_data_str = get<1>(GetParam());
    string typeString = Util::getDataTypeString(dataType);
    if (dataType == DT_SYMBOL) GTEST_SKIP() << "Class attributes cannot be symbol scalar.";

    int extraParam = 0;
    if (typeString.compare(0, 9, "DECIMAL32") == 0){typeString = "DECIMAL32(5)"; extraParam = 5;}
    else if (typeString.compare(0, 9, "DECIMAL64") == 0){typeString = "DECIMAL64(15)"; extraParam = 15;}
    else if (typeString.compare(0, 10, "DECIMAL128") == 0){typeString = "DECIMAL128(25)"; extraParam = 25;}
    cout << "test type: " << typeString << endl;

    EventSchema *schema = new EventSchema();
    schema->eventType_ = "md";
    schema->fieldNames_ = {"market", "eventTime", "testCol", "commonCol"};
    schema->fieldTypes_ = {DT_STRING, DT_TIMESTAMP, dataType, dataType};
    schema->fieldForms_ = {DF_SCALAR, DF_SCALAR, DF_SCALAR, DF_SCALAR};
    schema->fieldExtraParams_ = {0, 0, extraParam, extraParam};
    conn300->run(
        "share streamTable(1:0, `time`eventType`event`commonCol, [TIMESTAMP,STRING,BLOB,"+typeString+"]) as fromCEP;"
        "share streamTable(1:0, `time`eventType`event`commonCol, [TIMESTAMP,STRING,BLOB,"+typeString+"]) as fromApi;go;"
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
        "try{dropStreamEngine(`EventSer)}catch(ex){};go;"
        "inputSerializer = streamEventSerializer(name=`EventSer, eventSchema=md, outputTable=fromCEP, eventTimeField = \"eventTime\", commonField = \"commonCol\");"
    );

    Signal notify;
    Mutex mutex;
    int sub_rows = 0, test_rows = 1000;
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {"eventTime"};
    vector<string> commonKeys = {"commonCol"};
    EventClient *client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
    EventSender *sender = new EventSender(conn300, "fromApi", eventSchemas, eventTimeKeys, commonKeys);

    auto imp_handler = [&](string eventType, std::vector<ConstantSP> attribute)
    {
        sub_rows += 1;
        if (sub_rows % 100 == 0) cout << "sub_rows: " << sub_rows << endl;
        LockGuard<Mutex> lock(&mutex);
        sender->sendEvent(eventType, attribute);
        if (sub_rows == test_rows)
        {
            notify.set();
        }
    };

    client->subscribe(hostName, port300, imp_handler, "fromCEP", DEFAULT_ACTION_NAME, 0);
    conn300->run(
        "rows = "+to_string(test_rows)+";"
        "val_col0=rand(`sz`sh`bj`adk, rows);val_col1=now()..temporalAdd(now(), rows-1, 'ms');val_col2=rand("+type_data_str+", rows);val_col3=rand("+type_data_str+", rows);"
        "for (i in 0:rows){"
        "data = md(val_col0[i], val_col1[i], val_col2[i], val_col3[i]);"
        "appendEvent(getStreamEngine(`EventSer), [data]);}"
    );
    notify.wait();
    Util::sleep(1000);

    client->unsubscribe(hostName, port300, "fromCEP", DEFAULT_ACTION_NAME);
    client->exit();
    EXPECT_TRUE(client->isExit());
    EXPECT_TRUE(conn300->run("all(eqObj(fromApi.values(), fromCEP.values()))")->getBool());

    delete schema, client, sender;
}


class CEPEventTest_EventClient_alltypes_vector : public CEPEventTest, public ::testing::WithParamInterface<tuple<DATA_TYPE, string>> {
public:
    static vector<tuple<DATA_TYPE, string>> get_vector_data(){
        return {
            std::make_tuple(DT_BOOL, "array(BOOL).append!(true false)"),std::make_tuple(DT_CHAR, "-127c 127c"),
            std::make_tuple(DT_SHORT, "1000h"),std::make_tuple(DT_INT, "10000"),
            std::make_tuple(DT_LONG, "1000000l"),std::make_tuple(DT_FLOAT, "100.000f"),
            std::make_tuple(DT_DOUBLE, "100.0000"),std::make_tuple(DT_DATE, "2023.01.01"),
            std::make_tuple(DT_MONTH, "2023.01M"),std::make_tuple(DT_TIME, "23:45:03.100"),
            std::make_tuple(DT_MINUTE, "23:45m"),std::make_tuple(DT_SECOND, "23:45:03"),
            std::make_tuple(DT_DATEHOUR, "datehour('2021.01.01T12'`)"),
            std::make_tuple(DT_DATETIME, "2021.01.01T12:45:03"),std::make_tuple(DT_TIMESTAMP, "2021.01.01T12:45:03.100"),
            std::make_tuple(DT_NANOTIME, "23:45:03.100000000"),std::make_tuple(DT_NANOTIMESTAMP, "2021.01.01T12:45:03.100000000"),
            std::make_tuple(DT_SYMBOL, "symbol(take(`sz`da`jt, 100000))"),std::make_tuple(DT_STRING, "`szstr`"),
            std::make_tuple(DT_UUID, "uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87''5d212a78-cc48-e3b1-4235-b4d91473ee10')"),
            std::make_tuple(DT_IP, "ipaddr('127.0.0.1''1.1.1.1')"),
            std::make_tuple(DT_INT128, "int128(`e1671797c52e15f763380b45e841ec32`e1671797c52e15f763380b45e841ec42)"),
            std::make_tuple(DT_BLOB, "blob(`blob`blob2)"),
            std::make_tuple(DT_DECIMAL32, "decimal32('1.123456789'`0'-135.1', 5)"),std::make_tuple(DT_DECIMAL64, "decimal64('-11.456464'`0'300.1', 15)"),
            std::make_tuple(DT_DECIMAL128, "decimal128('999.64621462333'`0'-1326', 25)"),
        };
    };
    static vector<tuple<DATA_TYPE, string>> get_vector_allNull_data(){
        return {
            std::make_tuple(DT_BOOL, "array(BOOL).append!([bool(NULL), bool(NULL)])"),std::make_tuple(DT_CHAR, "array(CHAR).append!([char(NULL), char(NULL)])"),
            std::make_tuple(DT_SHORT, "array(SHORT).append!([short(NULL), short(NULL)])"),std::make_tuple(DT_INT, "array(INT).append!([int(NULL), int(NULL)])"),
            std::make_tuple(DT_LONG, "array(LONG).append!([long(NULL), long(NULL)])"),std::make_tuple(DT_FLOAT, "array(FLOAT).append!([float(NULL), float(NULL)])"),
            std::make_tuple(DT_DOUBLE, "array(DOUBLE).append!([double(NULL), double(NULL)])"),std::make_tuple(DT_DATE, "array(DATE).append!([date(NULL), date(NULL)])"),
            std::make_tuple(DT_MONTH, "array(MONTH).append!([month(NULL), month(NULL)])"),std::make_tuple(DT_TIME, "array(TIME).append!([time(NULL), time(NULL)])"),
            std::make_tuple(DT_MINUTE, "array(MINUTE).append!([minute(NULL), minute(NULL)])"),std::make_tuple(DT_SECOND, "array(SECOND).append!([second(NULL), second(NULL)])"),
            std::make_tuple(DT_DATEHOUR, "array(DATEHOUR).append!([datehour(NULL), datehour(NULL)])"),
            std::make_tuple(DT_DATETIME, "array(DATETIME).append!([datetime(NULL), datetime(NULL)])"),std::make_tuple(DT_TIMESTAMP, "array(TIMESTAMP).append!([timestamp(NULL), timestamp(NULL)])"),
            std::make_tuple(DT_NANOTIME, "array(NANOTIME).append!([nanotime(NULL), nanotime(NULL)])"),std::make_tuple(DT_NANOTIMESTAMP, "array(NANOTIMESTAMP).append!([nanotimestamp(NULL), nanotimestamp(NULL)])"),
            std::make_tuple(DT_UUID, "array(UUID).append!([uuid(), uuid()])"),std::make_tuple(DT_IP, "array(IPADDR).append!([ipaddr(), ipaddr()])"),
            std::make_tuple(DT_INT128, "array(INT128).append!([int128(), int128()])"),
            std::make_tuple(DT_DECIMAL32, "array(DECIMAL32(5)).append!([decimal32('', 5), decimal32('', 5)])"),
            std::make_tuple(DT_DECIMAL64, "array(DECIMAL64(15)).append!([decimal64('', 15), decimal64('', 15)])"),
            std::make_tuple(DT_DECIMAL128, "array(DECIMAL128(25)).append!([decimal128('', 25), decimal128('', 25)])"),
            std::make_tuple(DT_SYMBOL, "symbol(take(``,10000))"),
            std::make_tuple(DT_STRING, "string(``)"),
            std::make_tuple(DT_BLOB, "blob(``)"),
        };
    };
};

INSTANTIATE_TEST_SUITE_P(vector_noNull, CEPEventTest_EventClient_alltypes_vector, testing::ValuesIn(CEPEventTest_EventClient_alltypes_vector::get_vector_data()));
INSTANTIATE_TEST_SUITE_P(vector_allNull, CEPEventTest_EventClient_alltypes_vector, testing::ValuesIn(CEPEventTest_EventClient_alltypes_vector::get_vector_allNull_data()));
TEST_P(CEPEventTest_EventClient_alltypes_vector, test_EventClient_alltype_vector)
{
    DATA_TYPE dataType = get<0>(GetParam());
    string type_data_str = get<1>(GetParam());
    string typeString = Util::getDataTypeString(dataType);
    // if (dataType == DT_SYMBOL) GTEST_SKIP() << "Class attributes cannot be symbol scalar.";

    int extraParam = 0;
    if (typeString.compare(0, 9, "DECIMAL32") == 0){typeString = "DECIMAL32(5)"; extraParam = 5;}
    else if (typeString.compare(0, 9, "DECIMAL64") == 0){typeString = "DECIMAL64(15)"; extraParam = 15;}
    else if (typeString.compare(0, 10, "DECIMAL128") == 0){typeString = "DECIMAL128(25)"; extraParam = 25;}
    cout << "test type: " << typeString << endl;

    #define NOT_LITERAL dataType != DT_SYMBOL && dataType != DT_STRING && dataType != DT_BLOB
    string formatCommonColstr ="", formatClassstr ="";
    if (NOT_LITERAL){
        formatCommonColstr = typeString + "[]";
        formatClassstr = typeString + " VECTOR";
    }else{
        formatCommonColstr = "INT";
        formatClassstr = "INT";
    }

    EventSchema *schema = new EventSchema();
    schema->eventType_ = "md";
    schema->fieldNames_ = {"market", "eventTime", "testCol", "commonCol"};
    schema->fieldTypes_ = {DT_STRING, DT_TIMESTAMP, dataType, NOT_LITERAL?dataType:DT_INT};
    schema->fieldForms_ = {DF_SCALAR, DF_SCALAR, DF_VECTOR, NOT_LITERAL?DF_VECTOR:DF_SCALAR};
    schema->fieldExtraParams_ = {0, 0, extraParam, extraParam};
    conn300->run(
        "share streamTable(1:0, `time`eventType`event`commonCol, [TIMESTAMP,STRING,BLOB,"+formatCommonColstr+"]) as fromCEP;"
        "share streamTable(1:0, `time`eventType`event`commonCol, [TIMESTAMP,STRING,BLOB,"+formatCommonColstr+"]) as fromApi;go;"
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
        "try{dropStreamEngine(`EventSer)}catch(ex){};go;"
        "inputSerializer = streamEventSerializer(name=`EventSer, eventSchema=md, outputTable=fromCEP, eventTimeField=\"eventTime\",commonField=\"commonCol\");"
    );

    Signal notify;
    Mutex mutex;
    int sub_rows = 0, test_rows = 100;
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {"eventTime"};
    vector<string> commonKeys = {"commonCol"};
    EventClient *client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
    EventSender* sender = new EventSender(conn300, "fromApi", eventSchemas, eventTimeKeys, commonKeys);

    auto imp_handler = [&](string eventType, std::vector<ConstantSP> attribute)
    {
        // cout << "订阅到数据：[";
        // for (auto i=0; i<attribute.size(); i++){
        //     cout << attribute[i]->getString();
        //     if (i != attribute.size()-1){
        //         cout << ", ";
        //     }
        // }
        // cout << "]" << endl;
        sub_rows += 1;
        if (sub_rows % 100 == 0) cout << "sub_rows: " << sub_rows << endl;
        LockGuard<Mutex> lock(&mutex);
        sender->sendEvent(eventType, attribute);
        if (sub_rows == test_rows)
        {
            notify.set();
        }
    };

    client->subscribe(hostName, port300, imp_handler, "fromCEP", DEFAULT_ACTION_NAME, 0);
    vector<string> val_col2_scripts ={"rand("+type_data_str+", rows)", "rand(val_col2, vector_size)"};
    if (dataType == DT_BLOB)
        val_col2_scripts = {"blob(string(rand("+type_data_str+", rows)))", "blob(string(rand(val_col2, vector_size)))"};
    if (dataType == DT_SYMBOL)
        val_col2_scripts = {"symbol(string(rand([\"blob\"], 10)))", "symbol(string(rand([\"blob\"], 10)))"};
    conn300->run(
        "rows = "+to_string(test_rows)+";"
        "val_col0=rand(`sz`sh`bj`adk, rows);val_col1=now()..temporalAdd(now(), rows-1, 'ms');val_col2="+val_col2_scripts[0]+";val_col3=rand("+(NOT_LITERAL?type_data_str:"1000")+", rows);"
        "vector_size = rand(2000 0, 1)[0];print('测试的array size: '+ string(vector_size));"
        "for (i in 0:rows){"
            "col2 = "+val_col2_scripts[1]+";col3="+(NOT_LITERAL?"rand(val_col3, vector_size)":"val_col3[i]")+";"
            "data = md(val_col0[i], val_col1[i], col2, col3);"
            "appendEvent(getStreamEngine(`EventSer), [data]);};go;");
    notify.wait();
    Util::sleep(1000);

    client->unsubscribe(hostName, port300, "fromCEP", DEFAULT_ACTION_NAME);
    client->exit();
    EXPECT_TRUE(client->isExit());
    EXPECT_TRUE(conn300->run("all(each(eqObj, fromApi.values(),fromCEP.values()))")->getBool());

    delete schema, client, sender;
}

class CEPEventTest_EventClient_arrayVector : public CEPEventTest, public ::testing::WithParamInterface<tuple<DATA_TYPE, string>> {
public:
    static vector<tuple<DATA_TYPE, string>> get_arrayVector_data(){
        return {
            std::make_tuple(DT_BOOL_ARRAY, "array(BOOL[]).append!([rand(true false, 2)]).append!([rand(true false, 2)])"), std::make_tuple(DT_CHAR_ARRAY, "array(CHAR[]).append!([rand(127c, 2)]).append!([rand(127c, 2)])"),
            std::make_tuple(DT_SHORT_ARRAY, "array(SHORT[]).append!([rand(1000h, 2)]).append!([rand(1000h, 2)])"), std::make_tuple(DT_INT_ARRAY, "array(INT[]).append!([rand(10000, 2)]).append!([rand(10000, 2)])"),
            std::make_tuple(DT_LONG_ARRAY, "array(LONG[]).append!([rand(1000000l, 2)]).append!([rand(1000000l, 2)])"), std::make_tuple(DT_FLOAT_ARRAY, "array(FLOAT[]).append!([rand(100.000f, 2)]).append!([rand(100.000f, 2)])"),
            std::make_tuple(DT_DOUBLE_ARRAY, "array(DOUBLE[]).append!([rand(100.0000, 2)]).append!([rand(100.0000, 2)])"), std::make_tuple(DT_DATE_ARRAY, "array(DATE[]).append!([rand(2023.01.01, 2)]).append!([rand(2023.01.01, 2)])"),
            std::make_tuple(DT_MONTH_ARRAY, "array(MONTH[]).append!([rand(2023.01M, 2)]).append!([rand(2023.01M, 2)])"), std::make_tuple(DT_TIME_ARRAY, "array(TIME[]).append!([rand(23:45:03.100, 2)]).append!([rand(23:45:03.100, 2)])"),
            std::make_tuple(DT_MINUTE_ARRAY, "array(MINUTE[]).append!([rand(23:45m, 2)]).append!([rand(23:45m, 2)])"), std::make_tuple(DT_SECOND_ARRAY, "array(SECOND[]).append!([rand(23:45:03, 2)]).append!([rand(23:45:03, 2)])"),
            std::make_tuple(DT_DATEHOUR_ARRAY, "array(DATEHOUR[]).append!([rand(datehour('2021.01.01T12'`), 2)]).append!([rand(datehour('2021.01.01T12'`), 2)])"),
            std::make_tuple(DT_DATETIME_ARRAY, "array(DATETIME[]).append!([rand(2021.01.01T12:45:03, 2)]).append!([rand(2021.01.01T12:45:03, 2)])"),std::make_tuple(DT_TIMESTAMP_ARRAY, "array(TIMESTAMP[]).append!([rand(2021.01.01T12:45:03.100, 2)]).append!([rand(2021.01.01T12:45:03.100, 2)])"),
            std::make_tuple(DT_NANOTIME_ARRAY, "array(NANOTIME[]).append!([rand(23:45:03.100000000, 2)]).append!([rand(23:45:03.100000000, 2)])"),std::make_tuple(DT_NANOTIMESTAMP_ARRAY, "array(NANOTIMESTAMP[]).append!([rand(2021.01.01T12:45:03.100000000, 2)]).append!([rand(2021.01.01T12:45:03.100000000, 2)])"),
            std::make_tuple(DT_UUID_ARRAY, "array(UUID[]).append!([rand(uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87''5d212a78-cc48-e3b1-4235-b4d91473ee10'), 2)]).append!([rand(uuid('5d212a78-cc48-e3b1-4235-b4d91473ee44''5d212a78-cc48-e3b1-4235-b4d91473ee15'), 2)])"),std::make_tuple(DT_IP_ARRAY, "array(IPADDR[]).append!([rand(ipaddr('127.0.0.1''1.1.1.1'), 2)]).append!([rand(ipaddr('2.2.2.2''255.255.0.0'), 2)])"),
            std::make_tuple(DT_INT128_ARRAY, "array(INT128[]).append!([rand(int128(`e1671797c52e15f763380b45e841ec32`e1671797c52e15f763380b45e841ec42), 2)]).append!([rand(int128(`e1671797c52e15f763380b45e841ec76`e1671797c52e15f763380b45e841ec89), 2)])"),
        };
    };
    static vector<tuple<DATA_TYPE, string>> get_arrayVector_allNull_data(){
        return {
            std::make_tuple(DT_BOOL_ARRAY, "array(BOOL[]).append!([take(bool(NULL), 2)])"), std::make_tuple(DT_CHAR_ARRAY, "array(CHAR[]).append!([take(char(NULL), 2)])"),
            std::make_tuple(DT_SHORT_ARRAY, "array(SHORT[]).append!([take(short(NULL), 2)])"), std::make_tuple(DT_INT_ARRAY, "array(INT[]).append!([take(int(NULL), 2)])"),
            std::make_tuple(DT_LONG_ARRAY, "array(LONG[]).append!([take(long(NULL), 2)])"), std::make_tuple(DT_FLOAT_ARRAY, "array(FLOAT[]).append!([take(float(NULL), 2)])"),
            std::make_tuple(DT_DOUBLE_ARRAY, "array(DOUBLE[]).append!([take(double(NULL), 2)])"), std::make_tuple(DT_DATE_ARRAY, "array(DATE[]).append!([take(date(NULL), 2)])"),
            std::make_tuple(DT_MONTH_ARRAY, "array(MONTH[]).append!([take(month(NULL), 2)])"), std::make_tuple(DT_TIME_ARRAY, "array(TIME[]).append!([take(time(NULL), 2)])"),
            std::make_tuple(DT_MINUTE_ARRAY, "array(MINUTE[]).append!([take(minute(NULL), 2)])"), std::make_tuple(DT_SECOND_ARRAY, "array(SECOND[]).append!([take(second(NULL), 2)])"),
            std::make_tuple(DT_DATEHOUR_ARRAY, "array(DATEHOUR[]).append!([take(datehour(NULL), 2)])"),
            std::make_tuple(DT_DATETIME_ARRAY, "array(DATETIME[]).append!([take(datetime(NULL), 2)])"),std::make_tuple(DT_TIMESTAMP_ARRAY, "array(TIMESTAMP[]).append!([take(timestamp(NULL), 2)])"),
            std::make_tuple(DT_NANOTIME_ARRAY, "array(NANOTIME[]).append!([take(nanotime(NULL), 2)])"),std::make_tuple(DT_NANOTIMESTAMP_ARRAY, "array(NANOTIMESTAMP[]).append!([take(nanotimestamp(NULL), 2)])"),
            std::make_tuple(DT_UUID_ARRAY, "array(UUID[]).append!([take(uuid(), 2)])"),std::make_tuple(DT_IP_ARRAY, "array(IPADDR[]).append!([take(ipaddr(), 2)])"),
            std::make_tuple(DT_INT128_ARRAY, "array(INT128[]).append!([take(int128(), 2)])"),
            std::make_tuple(DT_DECIMAL32_ARRAY, "array(DECIMAL32(5)[]).append!([take(decimal32('', 5), 2)])"),
            std::make_tuple(DT_DECIMAL64_ARRAY, "array(DECIMAL64(15)[]).append!([take(decimal64('', 15), 2)])"),
            std::make_tuple(DT_DECIMAL128_ARRAY, "array(DECIMAL128(25)[]).append!([take(decimal128('', 25), 2)])"),
        };
    };
};
INSTANTIATE_TEST_SUITE_P(arrayVector_noNull, CEPEventTest_EventClient_arrayVector, testing::ValuesIn(CEPEventTest_EventClient_arrayVector::get_arrayVector_data()));
INSTANTIATE_TEST_SUITE_P(arrayVector_allNull, CEPEventTest_EventClient_arrayVector, testing::ValuesIn(CEPEventTest_EventClient_arrayVector::get_arrayVector_allNull_data()));
TEST_P(CEPEventTest_EventClient_arrayVector, test_EventClient_arrayVector)
{
    GTEST_SKIP() << "ArrayVector in class is not supported yet.";
    DATA_TYPE dataType = get<0>(GetParam());
    string type_data_str = get<1>(GetParam());
    string typeString = Util::getDataTypeString(dataType);
    // if (dataType == DT_SYMBOL) GTEST_SKIP() << "Class attributes cannot be symbol scalar.";

    int extraParam = 0;
    if (typeString.compare(0, 9, "DECIMAL32") == 0){typeString = "DECIMAL32(5)[]"; extraParam = 5;}
    else if (typeString.compare(0, 9, "DECIMAL64") == 0){typeString = "DECIMAL64(15)[]"; extraParam = 15;}
    else if (typeString.compare(0, 10, "DECIMAL128") == 0){typeString = "DECIMAL128(25)[]"; extraParam = 25;}
    cout << "test type: " << typeString << endl;

    EventSchema *schema = new EventSchema();
    schema->eventType_ = "md";
    schema->fieldNames_ = {"market", "eventTime", "testCol", "commonCol"};
    schema->fieldTypes_ = {DT_STRING, DT_TIMESTAMP, dataType, dataType};
    schema->fieldForms_ = {DF_SCALAR, DF_SCALAR, DF_VECTOR, DF_VECTOR};
    schema->fieldExtraParams_ = {0, 0, extraParam, extraParam};
    conn300->run(
        "share streamTable(1:0, `time`eventType`event`commonCol, [TIMESTAMP,STRING,BLOB,"+typeString+"]) as fromCEP;"
        "share streamTable(1:0, `time`eventType`event`commonCol, [TIMESTAMP,STRING,BLOB,"+typeString+"]) as fromApi;go;"
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
        "try{dropStreamEngine(`EventSer)}catch(ex){};go;"
        "inputSerializer = streamEventSerializer(name=`EventSer, eventSchema=md, outputTable=fromCEP, eventTimeField=\"eventTime\",commonField=\"commonCol\");"
    );

    Signal notify;
    Mutex mutex;
    int sub_rows = 0, test_rows = 100;
    vector<EventSchema> eventSchemas = {*schema};
    vector<string> eventTimeKeys = {"eventTime"};
    vector<string> commonKeys = {"commonCol"};
    EventClient *client = new EventClient(eventSchemas, eventTimeKeys, commonKeys);
    EventSender* sender = new EventSender(conn300, "fromApi", eventSchemas, eventTimeKeys, commonKeys);

    auto imp_handler = [&](string eventType, std::vector<ConstantSP> attribute)
    {
        // cout << "订阅到数据：[";
        // for (auto i=0; i<attribute.size(); i++){
        //     cout << attribute[i]->getString();
        //     if (i != attribute.size()-1){
        //         cout << ", ";
        //     }
        // }
        // cout << "]" << endl;
        sub_rows += 1;
        if (sub_rows % 100 == 0) cout << "sub_rows: " << sub_rows << endl;
        LockGuard<Mutex> lock(&mutex);
        sender->sendEvent(eventType, attribute);
        if (sub_rows == test_rows)
        {
            notify.set();
        }
    };

    client->subscribe(hostName, port300, imp_handler, "fromCEP", DEFAULT_ACTION_NAME, 0);
    vector<string> val_col2_scripts ={"rand("+type_data_str+", rows)", "rand(val_col2, vector_size)"};
    conn300->run(
        "rows = "+to_string(test_rows)+";"
        "val_col0=rand(`sz`sh`bj`adk, rows);val_col1=now()..temporalAdd(now(), rows-1, 'ms');val_col2="+val_col2_scripts[0]+";val_col3=rand("+type_data_str+", rows);"
        "vector_size = rand(0 2000, 1)[0];print('测试的array size: '+ string(vector_size));"
        "for (i in 0:rows){"
            "col2 = "+val_col2_scripts[1]+";col3=rand(val_col3, vector_size);"
            "data = md(val_col0[i], val_col1[i], col2, col3);"
            "appendEvent(getStreamEngine(`EventSer), [data]);};go;");
    notify.wait();
    Util::sleep(1000);

    client->unsubscribe(hostName, port300, "fromCEP", DEFAULT_ACTION_NAME);
    client->exit();
    EXPECT_TRUE(client->isExit());
    EXPECT_TRUE(conn300->run("all(each(eqObj, fromApi.values(),fromCEP.values()))")->getBool());

    delete schema, client, sender;
}