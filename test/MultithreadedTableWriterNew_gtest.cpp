#include <gtest/gtest.h>
#include "config.h"
#include "MultithreadedTableWriter.h"

class MultithreadedTableWriterNewTest : public testing::Test
{
    public:
        static std::shared_ptr<dolphindb::DBConnection> pConn;
        static void SetUpTestSuite()
        {
            bool ret = pConn->connect(HOST, PORT, USER, PASSWD);
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
            pConn->close();
        }
    protected:
        // Case
        virtual void SetUp()
        {

        }
        virtual void TearDown()
        {

        }
};

std::shared_ptr<dolphindb::DBConnection> MultithreadedTableWriterNewTest::pConn = std::make_shared<dolphindb::DBConnection>(false, false);

TEST_F(MultithreadedTableWriterNewTest, connectDisabled)
{
    std::shared_ptr<dolphindb::DBConnection> disabled_conn = std::make_shared<dolphindb::DBConnection>(false, false);
    ASSERT_ANY_THROW(dolphindb::MTWConfig config(disabled_conn, "t1"));
}

TEST_F(MultithreadedTableWriterNewTest, connectError)
{
    std::shared_ptr<dolphindb::DBConnection> err_conn = std::make_shared<dolphindb::DBConnection>(false, false);
    err_conn->connect(HOST, PORT, USER, "11111");
    ASSERT_ANY_THROW(dolphindb::MTWConfig config(err_conn, "t1"));
}

TEST_F(MultithreadedTableWriterNewTest, tableNameNull)
{
    ASSERT_ANY_THROW(dolphindb::MTWConfig config(pConn, ""));
}

TEST_F(MultithreadedTableWriterNewTest, userHasNotaccess)
{
    std::string userName=getRandString(20);
    std::shared_ptr<dolphindb::DBConnection> conn_ctl = std::make_shared<dolphindb::DBConnection>(false, false);
    std::shared_ptr<dolphindb::DBConnection> conn_user = std::make_shared<dolphindb::DBConnection>(false, false);

    conn_ctl->connect(HOST_CLUSTER, PORT_CONTROLLER, USER_CLUSTER, PASSWD_CLUSTER);
    conn_ctl->run(
        "userName='"+userName+"';"
        "try{deleteUser(userName)}catch(ex){};go;createUser(userName, `123456)"
    );
    conn_user->connect(HOST, PORT, userName, "123456");

    std::string dbName = "dfs://test_" + getRandString(10);
    std::string script = "dbName = \"" +
                    dbName + "\";"
                             "if(exists(\"" +
                    dbName + "\")){\n"
                             "\tdropDatabase(\"" +
                    dbName + "\");\t\n"
                             "}"
                             "db = database(\"" +
                    dbName + "\", VALUE,`A`B`C`D);"
                             "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
                             "pt = db.createPartitionedTable(t,`pt,`sym);";
    pConn->run(script);
    ASSERT_ANY_THROW(dolphindb::MTWConfig config(conn_user, "loadTable(\"" + dbName + "\",`pt)"));
}

TEST_F(MultithreadedTableWriterNewTest, useSSL)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::shared_ptr<dolphindb::DBConnection> conn_ssl = std::make_shared<dolphindb::DBConnection>(true);
    conn_ssl->connect(HOST, PORT, USER, PASSWD);
    std::string script = "dbName = \"" + dbName + "\";"
                                             "if(exists(\"" +
                    dbName + "\")){\n"
                             "\tdropDatabase(\"" +
                    dbName + "\");\t\n"
                             "}"
                             "db = database(\"" +
                    dbName + "\", VALUE,`A`B`C`D);"
                             "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
                             "pt = db.createPartitionedTable(t,`pt,`sym);";
    conn_ssl->run(script);
    dolphindb::ErrorCodeInfo pErrorInfo;
    dolphindb::MTWConfig config(conn_ssl, "loadTable(\"" + dbName + "\",`pt)");
    config.setThreads(1, "sym");
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);

    std::string sym[] = {"A", "B", "C", "D"};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;

    srand((int)time(NULL));
    for (int i = 0; i < 1000; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createString(sym[rand() % 4]));
        row.push_back(dolphindb::Util::createInt(i * 12));
        row.push_back(dolphindb::Util::createInt(i + 64));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    mtwsp->waitForThreadCompletion();

    dolphindb::MultithreadedTableWriter::Status status;
    mtwsp->getStatus(status);
    std::cout << status.errorInfo << std::endl;

    std::vector<std::vector<dolphindb::ConstantSP> *> unwriteDatas;
    mtwsp->getUnwrittenData(unwriteDatas);
    ASSERT_TRUE(unwriteDatas.empty());
    ASSERT_EQ(unwriteDatas.size(), 0);

    dolphindb::ConstantSP pt_rows = pConn->run("exec count(*) from loadTable(\"" + dbName + "\",`pt)");
    ASSERT_EQ(pt_rows->getInt(), 1000);
    conn_ssl->close();
}

TEST_F(MultithreadedTableWriterNewTest, threadCountNull)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\";"
                                             "if(exists(\"" +
                    dbName + "\")){\n"
                             "\tdropDatabase(\"" +
                    dbName + "\");\t\n"
                             "}"
                             "db = database(\"" +
                    dbName + "\", VALUE,`A`B`C`D);"
                             "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
                             "pt = db.createPartitionedTable(t,`pt,`sym);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, "loadTable(\"" + dbName + "\",`pt)");
    ASSERT_ANY_THROW(config.setThreads(0, "sym"));
}

TEST_F(MultithreadedTableWriterNewTest, threadCountZero)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\";"
                                             "if(exists(\"" +
                    dbName + "\")){\n"
                             "\tdropDatabase(\"" +
                    dbName + "\");\t\n"
                             "}"
                             "db = database(\"" +
                    dbName + "\", VALUE,`A`B`C`D);"
                             "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
                             "pt = db.createPartitionedTable(t,`pt,`sym);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, "loadTable(\"" + dbName + "\",`pt)");
    ASSERT_ANY_THROW(config.setThreads(0, "sym"));
}

TEST_F(MultithreadedTableWriterNewTest, threadByColNameNullWithMemoryTable)
{
    std::string case_=getCaseName();
    std::string script = "share table(1000:0, `sym`id`value,[SYMBOL, INT, INT]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    ASSERT_ANY_THROW(config.setThreads(5, ""));
}

TEST_F(MultithreadedTableWriterNewTest, threadByColNameErrorWithMemoryTable)
{
    std::string case_=getCaseName();
    std::string script = "share table(1000:0, `sym`id`value,[SYMBOL, INT, INT]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    ASSERT_ANY_THROW(config.setThreads(5, "date"));
}

TEST_F(MultithreadedTableWriterNewTest, threadByColNameNULLAndmutiThreadWithMemoryTable)
{
    std::string case_=getCaseName();
    std::string script = "share table(1000:0, `sym`id`value,[SYMBOL, INT, INT]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    ASSERT_ANY_THROW(config.setThreads(5, ""));
}

TEST_F(MultithreadedTableWriterNewTest, threadByColNameNullWithFileTable)
{
    std::string case_=getCaseName();
    std::string dir = pConn->run("getHomeDir()")->getString();
    replace(dir.begin(), dir.end(), '\\', '/');
    std::string dbName = dir + "/cpp_test/"+case_;

    std::string script = "dir = \"" + dbName + "\";\n"
                        "if(exists(dir)){\n"
                        "\tdropDatabase(dir);\t\n"
                        "}"
                        "db  = database(dir, VALUE,`A`B`C`D);\n"
                        "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);\n"
                        "pt = db.createPartitionedTable(t,`pt,`sym);\n";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, "loadTable(\"" + dbName + "\",`pt)");
    ASSERT_NO_THROW(config.setThreads(1, ""));
}

TEST_F(MultithreadedTableWriterNewTest, threadByColNameNullWithPartitionTable)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\";"
                                             "if(exists(\"" +
                    dbName + "\")){\n"
                             "\tdropDatabase(\"" +
                    dbName + "\");\t\n"
                             "}"
                             "db = database(\"" +
                    dbName + "\", VALUE,`A`B`C`D);"
                             "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
                             "pt = db.createPartitionedTable(t,`pt,`sym);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, "loadTable(\"" + dbName + "\",`pt)");
    ASSERT_NO_THROW(config.setThreads(1, ""));
}

TEST_F(MultithreadedTableWriterNewTest, threadByColNamedifferenceWithVectorPartitionTable)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\";"
                                             "if(exists(\"" +
                    dbName + "\")){\n"
                             "\tdropDatabase(\"" +
                    dbName + "\");\t\n"
                             "}"
                             "dbData = database(,VALUE,`A`B`C`D);"
                             "dbID = database(,RANGE, 0 5 10);"
                             "db = database(\"" +
                    dbName + "\", COMPO,[dbData,dbID]);"
                             "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
                             "pt = db.createPartitionedTable(t,`pt,`sym`id);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, "loadTable(\"" + dbName + "\",`pt)");
    ASSERT_ANY_THROW(config.setThreads(3, "value"));
}

TEST_F(MultithreadedTableWriterNewTest, threadByColNameDifferenceWithPartitionTable)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\";"
                                             "if(exists(\"" +
                    dbName + "\")){\n"
                             "\tdropDatabase(\"" +
                    dbName + "\");\t\n"
                             "}"
                             "db = database(\"" +
                    dbName + "\", VALUE,`A`B`C`D);"
                             "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
                             "pt = db.createPartitionedTable(t,`pt,`sym);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, "loadTable(\"" + dbName + "\",`pt)");
    ASSERT_ANY_THROW(config.setThreads(3, "id"));
}

TEST_F(MultithreadedTableWriterNewTest, threadByColNameNullWithCompoPartitionTable)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                                             "if(exists(\"" +
                    dbName + "\")){\n"
                             "\tdropDatabase(\"" +
                    dbName + "\")\t\n"
                             "}\n"
                             "dbData = database(,VALUE,`A`B`C`D)\n"
                             "dbID = database(,RANGE, 0 5 10)\n"
                             "db = database(\"" +
                    dbName + "\", COMPO,[dbData,dbID])\n"
                             "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
                             "pt = db.createPartitionedTable(t,`pt,`sym`id)\n"
                             "select * from loadTable(dbName, `pt)";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, "loadTable(\"" + dbName + "\",`pt)");
    ASSERT_ANY_THROW(config.setThreads(3, ""));
}

TEST_F(MultithreadedTableWriterNewTest, threadByColNameDifferenceWithCompoPartitionTable)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                                             "if(exists(\"" +
                    dbName + "\")){\n"
                             "\tdropDatabase(\"" +
                    dbName + "\")\t\n"
                             "}\n"
                             "dbData = database(,VALUE,`A`B`C`D)\n"
                             "dbID = database(,RANGE, 0 5 10)\n"
                             "db = database(\"" +
                    dbName + "\", COMPO,[dbData,dbID])\n"
                             "t = table(1000:0, `sym`id`value`price,[SYMBOL, INT, INT,DOUBLE])\n"
                             "pt = db.createPartitionedTable(t,`pt,`sym`id)\n"
                             "select * from loadTable(dbName, `pt)";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, "loadTable(\"" + dbName + "\",`pt)");
    ASSERT_ANY_THROW(config.setThreads(3, "value"));
}

TEST_F(MultithreadedTableWriterNewTest, threadByColNameSameWithCompoPartitionTable_1)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                                             "if(exists(\"" +
                    dbName + "\")){\n"
                             "\tdropDatabase(\"" +
                    dbName + "\")\t\n"
                             "}\n"
                             "dbData = database(,VALUE,`A`B`C`D)\n"
                             "dbID = database(,RANGE, 0 5 10)\n"
                             "db = database(\"" +
                    dbName + "\", COMPO,[dbData,dbID])\n"
                             "t = table(1000:0, `sym`id`value`price,[SYMBOL, INT, INT,DOUBLE])\n"
                             "pt = db.createPartitionedTable(t,`pt,`sym`id)\n"
                             "select * from loadTable(dbName, `pt)";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, "loadTable(\"" + dbName + "\",`pt)");
    ASSERT_NO_THROW(config.setThreads(3, "id"));
}

TEST_F(MultithreadedTableWriterNewTest, threadByColNameSameWithCompoPartitionTable_2)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                                             "if(exists(\"" +
                    dbName + "\")){\n"
                             "\tdropDatabase(\"" +
                    dbName + "\")\t\n"
                             "}\n"
                             "dbData = database(,VALUE,`A`B`C`D)\n"
                             "dbID = database(,RANGE, 0 5 10)\n"
                             "db = database(\"" +
                    dbName + "\", COMPO,[dbData,dbID])\n"
                             "t = table(1000:0, `sym`id`value`price,[SYMBOL, INT, INT,DOUBLE])\n"
                             "pt = db.createPartitionedTable(t,`pt,`sym`id)\n"
                             "select * from loadTable(dbName, `pt)";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, "loadTable(\"" + dbName + "\",`pt)");
    ASSERT_NO_THROW(config.setThreads(3, "sym"));
}

TEST_F(MultithreadedTableWriterNewTest, insertValueLongerThanMemoryTable)
{
    std::string case_=getCaseName();
    std::string script = "share table(1000:0, `sym`id`value,[SYMBOL, INT, INT]) as "+case_;
    pConn->run(script);
    dolphindb::ErrorCodeInfo pErrorInfo;
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    bool flag = mtwsp->insert(pErrorInfo, "A", 12, 12, 12);
    ASSERT_FALSE(flag);
    ASSERT_EQ(pErrorInfo.errorInfo, "Column counts don't match 4");
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorLongerThanMemoryTable)
{
    std::string case_=getCaseName();
    std::string script = "share table(1000:0, `sym`id`value,[SYMBOL, INT, INT]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    for (int i = 0; i < 10; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createString("A"));
        row.push_back(dolphindb::Util::createInt(i));
        row.push_back(dolphindb::Util::createInt(i + 1));
        row.push_back(dolphindb::Util::createString(std::to_string(i)));
        datas.push_back(prow);
    }
    bool flag = mtwsp->insert(pErrorInfo, datas);
    std::vector<std::vector<dolphindb::ConstantSP> *> unwrite;
    mtwsp->waitForThreadCompletion();
    mtwsp->getUnwrittenData(unwrite);
    ASSERT_FALSE(flag);
    ASSERT_EQ(unwrite.size(), 0);
    ASSERT_EQ(pErrorInfo.errorInfo, "Column counts don't match 1");
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorShorterThanMemoryTable)
{
    std::string case_=getCaseName();
    std::string script = "share table(1000:0, `sym`id`value,[SYMBOL, INT, INT]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    for (int i = 0; i < 10; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createString("A"));
        row.push_back(dolphindb::Util::createInt(i));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    dolphindb::MultithreadedTableWriter::Status status;
    mtwsp->getStatus(status);
    std::cout << status.errorInfo;
    std::vector<std::vector<dolphindb::ConstantSP> *> unwrite;
    mtwsp->waitForThreadCompletion();
    mtwsp->getUnwrittenData(unwrite);
    ASSERT_EQ(unwrite.size(), 10);
}

TEST_F(MultithreadedTableWriterNewTest, insertValuesShorterThanMemoryTable)
{
    std::string case_=getCaseName();
    std::string script = "share table(1000:0, `sym`id`value,[SYMBOL, INT, INT]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    bool flag = mtwsp->insert(pErrorInfo, "A", 12);
    ASSERT_FALSE(flag);
    ASSERT_EQ(pErrorInfo.errorInfo, "Column counts don't match 2");
}

TEST_F(MultithreadedTableWriterNewTest, insertValuesErrorTypeDataMemoryTable_1)
{
    std::string case_=getCaseName();
    std::string script = "share table(1000:0, `sym`id`value,[SYMBOL, INT, INT]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);;
    dolphindb::ErrorCodeInfo pErrorInfo;
    bool flag = mtwsp->insert(pErrorInfo, "A", 12, 12.9);
    ASSERT_EQ(flag, false);
    ASSERT_EQ(pErrorInfo.errorInfo, "Cannot convert double to INT for col 3");
}

TEST_F(MultithreadedTableWriterNewTest, insertValuesMemoryTable)
{
    std::string case_=getCaseName();
    std::string script = "share table(1000:0, `sym`id`value,[SYMBOL, INT, INT]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    bool flag = mtwsp->insert(pErrorInfo, "A", 12, 12);
    ASSERT_EQ(flag, true);
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run(case_);
    ASSERT_EQ((t1->getRow(0)->getString() == "value->12\nsym->A\nid->12\n") || (t1->getRow(0)->getString() == "sym->A\nid->12\nvalue->12\n") || (t1->getRow(0)->getString() == "value->12\nid->12\nsym->A\n"), true);
}

TEST_F(MultithreadedTableWriterNewTest, insertValuesMemoryTableDifferentTypeData)
{
    std::string case_=getCaseName();
    std::string script = "share table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`blob,"
                    "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,BLOB]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    unsigned char data[16] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    bool flag = mtwsp->insert(pErrorInfo, false, char(12), (short)12, (long)12, dolphindb::Util::createDate(2012, 11, 12), dolphindb::Util::createMonth(2012, 11), dolphindb::Util::createSecond(134),
                              dolphindb::Util::createDateTime(2012, 11, 13, 6, 12, 12), dolphindb::Util::createTimestamp(43241), dolphindb::Util::createNanoTime(532432),
                              dolphindb::Util::createNanoTimestamp(85932494), 12.4f, 3412.3, "AAPL", "A", data, data, data, 12, "0f0e0d0c0b0a09080706050403020100");
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run(case_);
    ASSERT_EQ(t1->getColumn(0)->getRow(0)->getString(), "0");
    ASSERT_EQ(t1->getColumn(1)->getRow(0)->getString(), "12");
    ASSERT_EQ(t1->getColumn(2)->getRow(0)->getString(), "12");
    ASSERT_EQ(t1->getColumn(3)->getRow(0)->getString(), "12");
    ASSERT_EQ(t1->getColumn(4)->getRow(0)->getString(), "2012.11.12");
    ASSERT_EQ(t1->getColumn(5)->getRow(0)->getString(), "2012.11M");
    ASSERT_EQ(t1->getColumn(6)->getRow(0)->getString(), "00:02:14");
    ASSERT_EQ(t1->getColumn(7)->getRow(0)->getString(), dolphindb::Util::createDateTime(2012, 11, 13, 6, 12, 12)->getString());
    ASSERT_EQ(t1->getColumn(8)->getRow(0)->getString(), dolphindb::Util::createTimestamp(43241)->getString());
    ASSERT_EQ(t1->getColumn(9)->getRow(0)->getString(), dolphindb::Util::createNanoTime(532432)->getString());
    ASSERT_EQ(t1->getColumn(10)->getRow(0)->getString(), dolphindb::Util::createNanoTimestamp(85932494)->getString());
    ASSERT_EQ(t1->getColumn(11)->getRow(0)->getString(), "12.4");
    ASSERT_EQ(t1->getColumn(12)->getRow(0)->getString(), "3412.3");
    ASSERT_EQ(t1->getColumn(13)->getRow(0)->getString(), "AAPL");
    ASSERT_EQ(t1->getColumn(14)->getRow(0)->getString(), "A");
    ASSERT_EQ(t1->getColumn(15)->getRow(0)->getString(), "0f0e0d0c-0b0a-0908-0706-050403020100");
    ASSERT_EQ(t1->getColumn(16)->getRow(0)->getString(), "f0e:d0c:b0a:908:706:504:302:100");
    ASSERT_EQ(t1->getColumn(17)->getRow(0)->getString(), "0f0e0d0c0b0a09080706050403020100");
    ASSERT_EQ(t1->getColumn(19)->getRow(0)->getString(), "0f0e0d0c0b0a09080706050403020100");
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorMemoryTableDifferentTypeDataLessThan256)
{
    std::string case_=getCaseName();
    std::string script = "share table(1000:0, `bool`char`short`long`date`month`datetime`second`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`blob,"
                    "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,DATETIME,SECOND,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,BLOB]) as "+case_;
    pConn->run(script);
    dolphindb::Util::sleep(3000);
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    int data[] = {0, 1};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    for (int i = 0; i < 10; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createBool((char)(data[i % 2])));
        row.push_back(dolphindb::Util::createChar(i * 2));
        row.push_back(dolphindb::Util::createShort(i + 12));
        row.push_back(dolphindb::Util::createLong((long)i * 100));
        row.push_back(dolphindb::Util::createDate(i + 432));
        row.push_back(dolphindb::Util::createMonth(i + 21));
        row.push_back(dolphindb::Util::createDateTime(i * 192));
        row.push_back(dolphindb::Util::createSecond((long long)i * 1932));
        row.push_back(dolphindb::Util::createTimestamp((long long)i * 2342));
        row.push_back(dolphindb::Util::createNanoTime((long long)i * 4214));
        row.push_back(dolphindb::Util::createNanoTimestamp((long long)i * 4264));
        row.push_back(dolphindb::Util::createFloat(i * 42.64));
        row.push_back(dolphindb::Util::createDouble(i * 4.264));
        row.push_back(dolphindb::Util::createString(std::to_string(i)));
        row.push_back(dolphindb::Util::createString("A" + std::to_string(i)));
        // uuid,ipAddr, int128, blob
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100"));
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.2." + std::to_string(i)));
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_INT128, "0f0e0d0c0b0a09080706050403020100"));
        row.push_back(dolphindb::Util::createInt(i));
        row.push_back(dolphindb::Util::createBlob("0f0e0d0c0b0a0908070605040302010" + std::to_string(i)));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run("select * from "+case_+" order by long;");
    for (int i = 0; i < 10; i++)
    {
        ASSERT_EQ(t1->getColumn(0)->getRow(i)->getString(), dolphindb::Util::createBool((char)(data[i % 2]))->getString());
        ASSERT_EQ(t1->getColumn(1)->getRow(i)->getString(), dolphindb::Util::createChar(i * 2)->getString());
        ASSERT_EQ(t1->getColumn(2)->getRow(i)->getString(), dolphindb::Util::createShort(i + 12)->getString());
        ASSERT_EQ(t1->getColumn(3)->getRow(i)->getString(), dolphindb::Util::createLong((long)i * 100)->getString());
        ASSERT_EQ(t1->getColumn(4)->getRow(i)->getString(), dolphindb::Util::createDate(i + 432)->getString());
        ASSERT_EQ(t1->getColumn(5)->getRow(i)->getString(), dolphindb::Util::createMonth(i + 21)->getString());
        ASSERT_EQ(t1->getColumn(6)->getRow(i)->getString(), dolphindb::Util::createDateTime(i * 192)->getString());
        ASSERT_EQ(t1->getColumn(7)->getRow(i)->getString(), dolphindb::Util::createSecond((long long)i * 1932)->getString());
        ASSERT_EQ(t1->getColumn(8)->getRow(i)->getString(), dolphindb::Util::createTimestamp((long long)i * 2342)->getString());
        ASSERT_EQ(t1->getColumn(9)->getRow(i)->getString(), dolphindb::Util::createNanoTime((long long)i * 4214)->getString());
        ASSERT_EQ(t1->getColumn(10)->getRow(i)->getString(), dolphindb::Util::createNanoTimestamp((long long)i * 4264)->getString());
        ASSERT_EQ(t1->getColumn(11)->getRow(i)->getString(), dolphindb::Util::createFloat(i * 42.64)->getString());
        ASSERT_EQ(t1->getColumn(12)->getRow(i)->getString(), dolphindb::Util::createDouble(i * 4.264)->getString());
        ASSERT_EQ(t1->getColumn(13)->getRow(i)->getString(), dolphindb::Util::createString(std::to_string(i))->getString());
        ASSERT_EQ(t1->getColumn(14)->getRow(i)->getString(), dolphindb::Util::createString("A" + std::to_string(i))->getString());
        ASSERT_EQ(t1->getColumn(15)->getRow(i)->getString(), dolphindb::Util::parseConstant(dolphindb::DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100")->getString());
        ASSERT_EQ(t1->getColumn(16)->getRow(i)->getString(), dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.2." + std::to_string(i))->getString());
        ASSERT_EQ(t1->getColumn(17)->getRow(i)->getString(), dolphindb::Util::parseConstant(dolphindb::DT_INT128, "0f0e0d0c0b0a09080706050403020100")->getString());
        ASSERT_EQ(t1->getColumn(18)->getRow(i)->getString(), dolphindb::Util::createInt(i)->getString());
        ASSERT_EQ(t1->getColumn(19)->getRow(i)->getString(), dolphindb::Util::createString("0f0e0d0c0b0a0908070605040302010" + std::to_string(i))->getString());
    }
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorMemoryTableDifferentTypeDataBiggerThan1048576)
{
    std::string case_=getCaseName();
    std::string script = "share table(1000:0, `bool`char`short`long`date`month`datetime`second`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`blob,"
                    "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,DATETIME,SECOND,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,BLOB]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    int data[] = {0, 1};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    for (int i = 0; i < 3098576; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createBool((char)(data[i % 2])));
        row.push_back(dolphindb::Util::createChar(i * 2));
        row.push_back(dolphindb::Util::createShort(i + 12));
        row.push_back(dolphindb::Util::createLong((long)i * 100));
        row.push_back(dolphindb::Util::createDate(i + 432));
        row.push_back(dolphindb::Util::createMonth(i + 21));
        row.push_back(dolphindb::Util::createDateTime(i * 192));
        row.push_back(dolphindb::Util::createSecond(i * 12));
        row.push_back(dolphindb::Util::createTimestamp((long long)i * 2342));
        row.push_back(dolphindb::Util::createNanoTime((long long)i * 4214));
        row.push_back(dolphindb::Util::createNanoTimestamp((long long)i * 4264));
        row.push_back(dolphindb::Util::createFloat(i * 42.64));
        row.push_back(dolphindb::Util::createDouble(i * 4.264));
        row.push_back(dolphindb::Util::createString(std::to_string(i % 10)));
        row.push_back(dolphindb::Util::createString("A" + std::to_string(i % 10)));
        // uuid,ipAddr, int128, blob
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100"));
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.2." + std::to_string(i % 255)));
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_INT128, "0f0e0d0c0b0a09080706050403020100"));
        row.push_back(dolphindb::Util::createInt(i));
        row.push_back(dolphindb::Util::createBlob("0f0e0d0c0b0a0908070605040302010" + std::to_string(i)));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run("select * from "+case_+" order by long;");
    for (int i = 0; i < 10985; i++)
    {
        ASSERT_EQ(t1->getColumn(0)->getRow(i)->getString(), dolphindb::Util::createBool((char)(data[i % 2]))->getString());
        ASSERT_EQ(t1->getColumn(1)->getRow(i)->getString(), dolphindb::Util::createChar(i * 2)->getString());
        ASSERT_EQ(t1->getColumn(2)->getRow(i)->getString(), dolphindb::Util::createShort(i + 12)->getString());
        ASSERT_EQ(t1->getColumn(3)->getRow(i)->getString(), dolphindb::Util::createLong((long)i * 100)->getString());
        ASSERT_EQ(t1->getColumn(4)->getRow(i)->getString(), dolphindb::Util::createDate(i + 432)->getString());
        ASSERT_EQ(t1->getColumn(5)->getRow(i)->getString(), dolphindb::Util::createMonth(i + 21)->getString());
        ASSERT_EQ(t1->getColumn(6)->getRow(i)->getString(), dolphindb::Util::createDateTime(i * 192)->getString());
        ASSERT_EQ(t1->getColumn(7)->getRow(i)->getString(), dolphindb::Util::createSecond(i * 12)->getString());
        ASSERT_EQ(t1->getColumn(8)->getRow(i)->getString(), dolphindb::Util::createTimestamp((long long)i * 2342)->getString());
        ASSERT_EQ(t1->getColumn(9)->getRow(i)->getString(), dolphindb::Util::createNanoTime((long long)i * 4214)->getString());
        ASSERT_EQ(t1->getColumn(10)->getRow(i)->getString(), dolphindb::Util::createNanoTimestamp((long long)i * 4264)->getString());
        ASSERT_EQ(t1->getColumn(11)->getRow(i)->getString(), dolphindb::Util::createFloat(i * 42.64)->getString());
        ASSERT_EQ(t1->getColumn(12)->getRow(i)->getString(), dolphindb::Util::createDouble(i * 4.264)->getString());
        ASSERT_EQ(t1->getColumn(13)->getRow(i)->getString(), dolphindb::Util::createString(std::to_string(i % 10))->getString());
        ASSERT_EQ(t1->getColumn(14)->getRow(i)->getString(), dolphindb::Util::createString("A" + std::to_string(i % 10))->getString());
        ASSERT_EQ(t1->getColumn(15)->getRow(i)->getString(), dolphindb::Util::parseConstant(dolphindb::DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100")->getString());
        ASSERT_EQ(t1->getColumn(16)->getRow(i)->getString(), dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.2." + std::to_string(i % 255))->getString());
        ASSERT_EQ(t1->getColumn(17)->getRow(i)->getString(), dolphindb::Util::parseConstant(dolphindb::DT_INT128, "0f0e0d0c0b0a09080706050403020100")->getString());
        ASSERT_EQ(t1->getColumn(18)->getRow(i)->getString(), dolphindb::Util::createInt(i)->getString());
        ASSERT_EQ(t1->getColumn(19)->getRow(i)->getString(), dolphindb::Util::createString("0f0e0d0c0b0a0908070605040302010" + std::to_string(i))->getString());
    }
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorStreamTableDifferentTypeDataBiggerThan1048576)
{
    std::string case_=getCaseName();
    std::string script = "share streamTable(1000:0, `bool`char`short`long`date`month`datetime`second`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`blob,"
                    "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,DATETIME,SECOND,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,BLOB]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    int data[] = {0, 1};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    for (int i = 0; i < 3098576; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createBool((char)(data[i % 2])));
        row.push_back(dolphindb::Util::createChar(i * 2));
        row.push_back(dolphindb::Util::createShort(i + 12));
        row.push_back(dolphindb::Util::createLong((long)i * 100));
        row.push_back(dolphindb::Util::createDate(i + 432));
        row.push_back(dolphindb::Util::createMonth(i + 21));
        row.push_back(dolphindb::Util::createDateTime(i * 192));
        row.push_back(dolphindb::Util::createSecond((long long)i * 1932));
        row.push_back(dolphindb::Util::createTimestamp((long long)i * 2342));
        row.push_back(dolphindb::Util::createNanoTime((long long)i * 4214));
        row.push_back(dolphindb::Util::createNanoTimestamp((long long)i * 4264));
        row.push_back(dolphindb::Util::createFloat(i * 42.64));
        row.push_back(dolphindb::Util::createDouble(i * 4.264));
        row.push_back(dolphindb::Util::createString(std::to_string(i % 10)));
        row.push_back(dolphindb::Util::createString("A" + std::to_string(i % 10)));
        // uuid,ipAddr, int128, blob
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100"));
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.2." + std::to_string(i % 255)));
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_INT128, "0f0e0d0c0b0a09080706050403020100"));
        row.push_back(dolphindb::Util::createInt(i));
        row.push_back(dolphindb::Util::createBlob("0f0e0d0c0b0a0908070605040302010" + std::to_string(i)));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run("select * from "+case_+" order by long;");
    for (int i = 0; i < 10985; i++)
    {
        ASSERT_EQ(t1->getColumn(0)->getRow(i)->getString(), dolphindb::Util::createBool((char)(data[i % 2]))->getString());
        ASSERT_EQ(t1->getColumn(1)->getRow(i)->getString(), dolphindb::Util::createChar(i * 2)->getString());
        ASSERT_EQ(t1->getColumn(2)->getRow(i)->getString(), dolphindb::Util::createShort(i + 12)->getString());
        ASSERT_EQ(t1->getColumn(3)->getRow(i)->getString(), dolphindb::Util::createLong((long)i * 100)->getString());
        ASSERT_EQ(t1->getColumn(4)->getRow(i)->getString(), dolphindb::Util::createDate(i + 432)->getString());
        ASSERT_EQ(t1->getColumn(5)->getRow(i)->getString(), dolphindb::Util::createMonth(i + 21)->getString());
        ASSERT_EQ(t1->getColumn(6)->getRow(i)->getString(), dolphindb::Util::createDateTime(i * 192)->getString());
        ASSERT_EQ(t1->getColumn(7)->getRow(i)->getString(), dolphindb::Util::createSecond((long long)i * 1932)->getString());
        ASSERT_EQ(t1->getColumn(8)->getRow(i)->getString(), dolphindb::Util::createTimestamp((long long)i * 2342)->getString());
        ASSERT_EQ(t1->getColumn(9)->getRow(i)->getString(), dolphindb::Util::createNanoTime((long long)i * 4214)->getString());
        ASSERT_EQ(t1->getColumn(10)->getRow(i)->getString(), dolphindb::Util::createNanoTimestamp((long long)i * 4264)->getString());
        ASSERT_EQ(t1->getColumn(11)->getRow(i)->getString(), dolphindb::Util::createFloat(i * 42.64)->getString());
        ASSERT_EQ(t1->getColumn(12)->getRow(i)->getString(), dolphindb::Util::createDouble(i * 4.264)->getString());
        ASSERT_EQ(t1->getColumn(13)->getRow(i)->getString(), dolphindb::Util::createString(std::to_string(i % 10))->getString());
        ASSERT_EQ(t1->getColumn(14)->getRow(i)->getString(), dolphindb::Util::createString("A" + std::to_string(i % 10))->getString());
        ASSERT_EQ(t1->getColumn(15)->getRow(i)->getString(), dolphindb::Util::parseConstant(dolphindb::DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100")->getString());
        ASSERT_EQ(t1->getColumn(16)->getRow(i)->getString(), dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.2." + std::to_string(i % 255))->getString());
        ASSERT_EQ(t1->getColumn(17)->getRow(i)->getString(), dolphindb::Util::parseConstant(dolphindb::DT_INT128, "0f0e0d0c0b0a09080706050403020100")->getString());
        ASSERT_EQ(t1->getColumn(18)->getRow(i)->getString(), dolphindb::Util::createInt(i)->getString());
        ASSERT_EQ(t1->getColumn(19)->getRow(i)->getString(), dolphindb::Util::createString("0f0e0d0c0b0a0908070605040302010" + std::to_string(i))->getString());
    }
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorkeyedTableDifferentTypeDataBiggerThan1048576)
{
    std::string case_=getCaseName();
    std::string script = "share keyedTable(`symbol`string,1000:0, `bool`char`short`long`date`month`datetime`second`timestamp`nanotime`nanotimestamp`datehour`float`double`symbol`string`uuid`ipaddr`int128`id`blob,"
                    "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,DATETIME,SECOND,TIMESTAMP,NANOTIME,NANOTIMESTAMP,DATEHOUR,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,BLOB]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    int data[] = {0, 1};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    for (int i = 0; i < 3000000; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createBool((char)(data[i % 2])));
        row.push_back(dolphindb::Util::createChar(i % 200));
        row.push_back(dolphindb::Util::createShort(i % 100));
        row.push_back(dolphindb::Util::createLong(i % 100));
        row.push_back(dolphindb::Util::createDate(i % 3432));
        row.push_back(dolphindb::Util::createMonth(i % 1221));
        row.push_back(dolphindb::Util::createDateTime(i + 192));
        row.push_back(dolphindb::Util::createSecond(i % 1932));
        row.push_back(dolphindb::Util::createTimestamp(i + 2342));
        row.push_back(dolphindb::Util::createNanoTime(i + 4214));
        row.push_back(dolphindb::Util::createNanoTimestamp(i + 4264));
        row.push_back(dolphindb::Util::createDateHour(i + 4264));
        row.push_back(dolphindb::Util::createFloat(i * 42.64));
        row.push_back(dolphindb::Util::createDouble(i * 4.264));
        row.push_back(dolphindb::Util::createString(std::to_string(i % 100)));
        row.push_back(dolphindb::Util::createString("A" + std::to_string(i % 100)));
        // uuid,ipAddr, int128, blob
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100"));
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.2." + std::to_string(i % 255)));
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_INT128, "0f0e0d0c0b0a09080706050403020100"));
        row.push_back(dolphindb::Util::createInt(i));
        row.push_back(dolphindb::Util::createBlob("0f0e0d0c0b0a0908070605040302010" + std::to_string(i)));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run("select * from "+case_+" order by long;");
    std::string script2 = "t2 = keyedTable(`symbol`string,1000:0, `bool`char`short`long`date`month`datetime`second`timestamp`nanotime`nanotimestamp`datehour`float`double`symbol`string`uuid`ipaddr`int128`id`blob,[BOOL,CHAR,SHORT,LONG,DATE,MONTH,DATETIME,SECOND,TIMESTAMP,NANOTIME,NANOTIMESTAMP,DATEHOUR,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,BLOB])\n"
                     "boolvector = take(false true, 3000000)\n"
                     "charvector = take(char(0..199), 3000000)\n"
                     "shortvecot = take(short(0..99), 3000000)\n"
                     "longvector = take(long(0..99), 3000000)\n"
                     "datevector = take(date((0..2999999)%3432),3000000)\n"
                     "montvector = take(month((0..2999999)%1221),3000000)\n"
                     "datetimevector = datetime((0..2999999)+192)\n"
                     "secondvector = second((0..2999999)%1932)\n"
                     "timestampvector =timestamp((0..2999999)+2342)\n"
                     "nanotimevector = nanotime((0..2999999)+4214)\n"
                     "nanotimestampvector = nanotimestamp((0..2999999)+4264) \n"
                     "datehourvector = datehour((0..2999999)+4264)\n"
                     "floatvector = float((0..2999999)+42.64)\n"
                     "doublevector = double((0..2999999)+4.264)\n"
                     "symbolvector = take(symbol(string((0..2999999)%100)),3000000)\n"
                     "stringvector = take(\"A\"+string((0..2999999)%100),3000000)\n"
                     "uuidvector = take(uuid(\"0f0e0d0c-0b0a-0908-0706-050403020100\"),3000000)\n"
                     "ipvector = ipaddr(\"192.168.2.\"+string((0..2999999)%255))\n"
                     "int128vector = take(int128(\"0f0e0d0c0b0a09080706050403020100\"),3000000)\n"
                     "idvector = 0..2999999\n"
                     "blobvector = blob(\"0f0e0d0c0b0a0908070605040302010\"+string(0..2999999))\n"
                     "t2.append!(table(boolvector,charvector,shortvecot,longvector,datevector,montvector,datetimevector,secondvector,timestampvector,nanotimevector,nanotimestampvector,datehourvector,floatvector,doublevector,symbolvector,stringvector,uuidvector,ipvector,int128vector,idvector ,blobvector))\n"
                     "select * from t2 order by long";
    dolphindb::TableSP t2 = pConn->run(script2);
    ASSERT_EQ(t1->getString(), t2->getString());
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorMemoryTableWithNullptr)
{
    std::string case_=getCaseName();
    std::string script = "share table(1000:0, `sym`id`value,[SYMBOL, INT, INT]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    bool flag = mtwsp->insert(pErrorInfo, "A", (std::nullptr_t)0, 12);
    ASSERT_EQ(flag, true);
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run(case_);
    ASSERT_EQ((t1->getRow(0)->getString() == "value->12\nsym->A\nid->\n") || (t1->getRow(0)->getString() == "sym->A\nid->\nvalue->12\n") || (t1->getRow(0)->getString() == "value->12\nid->\nsym->A\n"), true);
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorMemoryTableWithbatchSize)
{
    std::string case_=getCaseName();
    std::string script = "share table(1000:0, `sym`id`values,[SYMBOL,INT,INT]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    std::string sym[] = {"A", "B", "C", "D"};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    for (int i = 0; i < 999; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        srand((int)time(NULL));
        row.push_back(dolphindb::Util::createString(sym[rand() % 4]));
        row.push_back(dolphindb::Util::createInt(i * 12));
        row.push_back(dolphindb::Util::createInt(i + 64));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    dolphindb::ConstantSP t1 = pConn->run("select * from "+case_+" order by values;");
    mtwsp->insert(pErrorInfo, "A", 12, 16);
    mtwsp->insert(pErrorInfo, "B", 12, 16);
    mtwsp->waitForThreadCompletion();
    t1 = pConn->run("select * from "+case_+" order by values;");
    ASSERT_EQ(t1->size(), 1001);
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorMemoryTableWithbatchSize_2)
{
    std::string case_=getCaseName();
    std::string script = "share table(1000:0, `sym`id`values,[SYMBOL,INT,INT]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    mtwsp->insert(pErrorInfo, "A", 12, 16000);
    std::string sym[] = {"A", "B", "C", "D"};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    srand((int)time(NULL));
    for (int i = 0; i < 1001; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createString(sym[rand() % 4]));
        row.push_back(dolphindb::Util::createInt(i * 12));
        row.push_back(dolphindb::Util::createInt(i + 64));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run("select * from "+case_+" order by values;");
    ASSERT_EQ(t1->size(), 1002);
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorMemoryTableWithThrottle)
{
    std::string case_=getCaseName();
    std::string script = "share table(1000:0, `sym`id`values,[SYMBOL,INT,INT]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    std::string sym[] = {"A", "B", "C", "D"};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    for (int i = 0; i < 560; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        srand((int)time(NULL));
        row.push_back(dolphindb::Util::createString(sym[rand() % 4]));
        row.push_back(dolphindb::Util::createInt(i * 12));
        row.push_back(dolphindb::Util::createInt(i + 64));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    dolphindb::ConstantSP t1 = pConn->run("select * from "+case_+" order by values;");
    mtwsp->waitForThreadCompletion();
    t1 = pConn->run("select * from "+case_+" order by values;");
    ASSERT_EQ(t1->size(), 560);
}

TEST_F(MultithreadedTableWriterNewTest, insertToPartitionTableWithpartitionColNull)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D)\n"
                        "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
                        "pt = db.createPartitionedTable(t,`pt,`sym)";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    ASSERT_ANY_THROW(config.setThreads(5, ""));
}

TEST_F(MultithreadedTableWriterNewTest, insertToPartitionTableWithpartitionColValueNull_threadCount1)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D)\n"
                        "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
                        "pt = db.createPartitionedTable(t,`pt,`sym)";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    bool flag = mtwsp->insert(pErrorInfo, "", 12, 12);
    ASSERT_EQ(flag, true);
    mtwsp->waitForThreadCompletion();
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    mtwsp->getUnwrittenData(datas);
    ASSERT_EQ(datas[0][0][0]->getString(), "");
    ASSERT_EQ(datas[0][0][1]->getInt(), 12);
    ASSERT_EQ(datas[0][0][2]->getInt(), 12);
    ASSERT_EQ(datas.size(), 1);
}

TEST_F(MultithreadedTableWriterNewTest, insertToPartitionTableWithpartitionColValueNull_threadCount5)
{
    std::string dbName = "dfs://test_" + getRandString(10);
    std::string script = "dbName = \"" + dbName + "\"\n"
                                             "if(exists(dbName)){\n"
                                             "\tdropDatabase(dbName)\t\n"
                                             "}\n"
                                             "db  = database(dbName, VALUE,`A`B`C`D)\n"
                                             "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
                                             "pt = db.createPartitionedTable(t,`pt,`sym)";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    bool flag = mtwsp->insert(pErrorInfo, "", 12, 12);
    ASSERT_EQ(flag, true);
    mtwsp->waitForThreadCompletion();
    std::cout << pErrorInfo.errorInfo << std::endl;
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    mtwsp->getUnwrittenData(datas);
    ASSERT_EQ(datas[0][0][0]->getString(), "");
    ASSERT_EQ(datas[0][0][1]->getInt(), 12);
    ASSERT_EQ(datas[0][0][2]->getInt(), 12);
    ASSERT_EQ(datas.size(), 1);
}

TEST_F(MultithreadedTableWriterNewTest, insertValueLongerThanPartitionTable)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D)\n"
                        "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
                        "pt = db.createPartitionedTable(t,`pt,`sym)";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    bool flag = mtwsp->insert(pErrorInfo, "A", 12, 12, 12);
    ASSERT_EQ(flag, false);
    ASSERT_EQ(pErrorInfo.errorInfo, "Column counts don't match 4");
}

TEST_F(MultithreadedTableWriterNewTest, insertValuesShorterThanPartitionTable)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D)\n"
                        "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
                        "pt = db.createPartitionedTable(t,`pt,`sym)";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    bool flag = mtwsp->insert(pErrorInfo, "A", 12);
    ASSERT_EQ(flag, false);
    ASSERT_EQ(pErrorInfo.errorInfo, "Column counts don't match 2");
}

TEST_F(MultithreadedTableWriterNewTest, insertValuesErrorTypeDataPartitionTable_1)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D)\n"
                        "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
                        "pt = db.createPartitionedTable(t,`pt,`sym)";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    bool flag = mtwsp->insert(pErrorInfo, "A", 12, 12.9);
    ASSERT_EQ(flag, false);
}

TEST_F(MultithreadedTableWriterNewTest, insertValuesPartitionTable)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D)\n"
                        "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
                        "pt = db.createPartitionedTable(t,`pt,`sym)";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    bool flag = mtwsp->insert(pErrorInfo, "A", 12, 12);
    ASSERT_EQ(flag, true);
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt)");
    ASSERT_EQ((t1->getRow(0)->getString() == "value->12\nsym->A\nid->12\n") || (t1->getRow(0)->getString() == "sym->A\nid->12\nvalue->12\n") || (t1->getRow(0)->getString() == "value->12\nid->12\nsym->A\n"), true);
}

TEST_F(MultithreadedTableWriterNewTest, insertValuesPartitionTableDifferentTypeData)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D);\n";
    script += "t = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id,"
              "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT]);"
              "pt = db.createPartitionedTable(t,`pt,`symbol);";
    pConn->run(script);
    dolphindb::Util::sleep(3000);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    unsigned char data[16] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    bool flag = mtwsp->insert(pErrorInfo, false, char(12), (short)12, (long)12, dolphindb::Util::createDate(2012, 11, 12), dolphindb::Util::createMonth(2012, 11), dolphindb::Util::createSecond(134),
                              dolphindb::Util::createDateTime(2012, 11, 13, 6, 12, 12), dolphindb::Util::createTimestamp(43241), dolphindb::Util::createNanoTime(532432),
                              dolphindb::Util::createNanoTimestamp(85932494), 12.4f, 3412.3, "A", "AAPL", data, data, data, 12);
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt);");
    ASSERT_EQ(t1->getColumn(0)->getRow(0)->getString(), "0");
    ASSERT_EQ(t1->getColumn(1)->getRow(0)->getString(), "12");
    ASSERT_EQ(t1->getColumn(2)->getRow(0)->getString(), "12");
    ASSERT_EQ(t1->getColumn(3)->getRow(0)->getString(), "12");
    ASSERT_EQ(t1->getColumn(4)->getRow(0)->getString(), "2012.11.12");
    ASSERT_EQ(t1->getColumn(5)->getRow(0)->getString(), "2012.11M");
    ASSERT_EQ(t1->getColumn(6)->getRow(0)->getString(), "00:02:14");
    ASSERT_EQ(t1->getColumn(7)->getRow(0)->getString(), dolphindb::Util::createDateTime(2012, 11, 13, 6, 12, 12)->getString());
    ASSERT_EQ(t1->getColumn(8)->getRow(0)->getString(), dolphindb::Util::createTimestamp(43241)->getString());
    ASSERT_EQ(t1->getColumn(9)->getRow(0)->getString(), dolphindb::Util::createNanoTime(532432)->getString());
    ASSERT_EQ(t1->getColumn(10)->getRow(0)->getString(), dolphindb::Util::createNanoTimestamp(85932494)->getString());
    ASSERT_EQ(t1->getColumn(11)->getRow(0)->getString(), "12.4");
    ASSERT_EQ(t1->getColumn(12)->getRow(0)->getString(), "3412.3");
    ASSERT_EQ(t1->getColumn(13)->getRow(0)->getString(), "A");
    ASSERT_EQ(t1->getColumn(14)->getRow(0)->getString(), "AAPL");
    ASSERT_EQ(t1->getColumn(15)->getRow(0)->getString(), "0f0e0d0c-0b0a-0908-0706-050403020100");
    ASSERT_EQ(t1->getColumn(16)->getRow(0)->getString(), "f0e:d0c:b0a:908:706:504:302:100");
    ASSERT_EQ(t1->getColumn(17)->getRow(0)->getString(), "0f0e0d0c0b0a09080706050403020100");
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorPartitionTableDifferentTypeDataBiggerThan1048576)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D);\n";
    script += "t = table(1000:0, `bool`char`short`long`date`month`datetime`second`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id,"
              "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,DATETIME,SECOND,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT]);"
              "pt = db.createPartitionedTable(t,`pt,`symbol);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    int data[] = {0, 1};
    std::string sym[] = {"A", "B", "C", "D"};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    srand((int)time(NULL));
    for (int i = 0; i < 3098576; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createBool((char)(data[i % 2])));
        row.push_back(dolphindb::Util::createChar(i * 2));
        row.push_back(dolphindb::Util::createShort(i + 12));
        row.push_back(dolphindb::Util::createLong((long)i * 100));
        row.push_back(dolphindb::Util::createDate(i + 432));
        row.push_back(dolphindb::Util::createMonth(i + 21));
        row.push_back(dolphindb::Util::createDateTime(i * 192));
        row.push_back(dolphindb::Util::createSecond((long long)i * 1932));
        row.push_back(dolphindb::Util::createTimestamp((long long)i * 2342));
        row.push_back(dolphindb::Util::createNanoTime((long long)i * 4214));
        row.push_back(dolphindb::Util::createNanoTimestamp((long long)i * 4264));
        row.push_back(dolphindb::Util::createFloat(i * 42.64));
        row.push_back(dolphindb::Util::createDouble(i * 4.264));
        row.push_back(dolphindb::Util::createString(sym[rand() % 4]));
        row.push_back(dolphindb::Util::createString("A" + std::to_string(i)));
        // uuid,ipAddr, int128, blob
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100"));
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.2." + std::to_string(i % 255)));
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_INT128, "0f0e0d0c0b0a09080706050403020100"));
        row.push_back(dolphindb::Util::createInt(i));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by long limit 1024;");
    for (int i = 0; i < 1024; i++)
    {
        ASSERT_EQ(t1->getColumn(0)->getRow(i)->getString(), dolphindb::Util::createBool((char)(data[i % 2]))->getString());
        ASSERT_EQ(t1->getColumn(1)->getRow(i)->getString(), dolphindb::Util::createChar(i * 2)->getString());
        ASSERT_EQ(t1->getColumn(2)->getRow(i)->getString(), dolphindb::Util::createShort(i + 12)->getString());
        ASSERT_EQ(t1->getColumn(3)->getRow(i)->getString(), dolphindb::Util::createLong((long)i * 100)->getString());
        ASSERT_EQ(t1->getColumn(4)->getRow(i)->getString(), dolphindb::Util::createDate(i + 432)->getString());
        ASSERT_EQ(t1->getColumn(5)->getRow(i)->getString(), dolphindb::Util::createMonth(i + 21)->getString());
        ASSERT_EQ(t1->getColumn(6)->getRow(i)->getString(), dolphindb::Util::createDateTime(i * 192)->getString());
        ASSERT_EQ(t1->getColumn(7)->getRow(i)->getString(), dolphindb::Util::createSecond((long long)i * 1932)->getString());
        ASSERT_EQ(t1->getColumn(8)->getRow(i)->getString(), dolphindb::Util::createTimestamp((long long)i * 2342)->getString());
        ASSERT_EQ(t1->getColumn(9)->getRow(i)->getString(), dolphindb::Util::createNanoTime((long long)i * 4214)->getString());
        ASSERT_EQ(t1->getColumn(10)->getRow(i)->getString(), dolphindb::Util::createNanoTimestamp((long long)i * 4264)->getString());
        ASSERT_EQ(t1->getColumn(11)->getRow(i)->getString(), dolphindb::Util::createFloat(i * 42.64)->getString());
        ASSERT_EQ(t1->getColumn(12)->getRow(i)->getString(), dolphindb::Util::createDouble(i * 4.264)->getString());
        ASSERT_EQ(t1->getColumn(14)->getRow(i)->getString(), dolphindb::Util::createString("A" + std::to_string(i))->getString());
        ASSERT_EQ(t1->getColumn(15)->getRow(i)->getString(), dolphindb::Util::parseConstant(dolphindb::DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100")->getString());
        ASSERT_EQ(t1->getColumn(16)->getRow(i)->getString(), dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.2." + std::to_string(i % 255))->getString());
        ASSERT_EQ(t1->getColumn(17)->getRow(i)->getString(), dolphindb::Util::parseConstant(dolphindb::DT_INT128, "0f0e0d0c0b0a09080706050403020100")->getString());
        ASSERT_EQ(t1->getColumn(18)->getRow(i)->getString(), dolphindb::Util::createInt(i)->getString());
    }
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorPartitionTableWithNullptr)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                            "if(exists(dbName)){\n"
                            "\tdropDatabase(dbName)\t\n"
                            "}\n"
                            "db  = database(dbName, VALUE,symbol(\"A\"+string(0..10)));\n";
    script += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
              "pt = db.createPartitionedTable(t,`pt,`symbol);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    bool flag = mtwsp->insert(pErrorInfo, "A1", (std::nullptr_t)0, 12);
    ASSERT_EQ(flag, true);
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt)");
    ASSERT_EQ((t1->getRow(0)->getString() == "value->12\nsymbol->A1\nid->\n") || (t1->getRow(0)->getString() == "symbol->A1\nid->\nvalue->12\n") || (t1->getRow(0)->getString() == "value->12\nid->\nsymbol->A1\n"), true);
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorPartitionTableWithbatchSize)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                            "if(exists(dbName)){\n"
                            "\tdropDatabase(dbName)\t\n"
                            "}\n"
                            "db  = database(dbName, VALUE,`A`B`C`D);\n";
    script += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
              "pt = db.createPartitionedTable(t,`pt,`symbol);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    std::string sym[] = {"A", "B", "C", "D"};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    srand((int)time(NULL));
    for (int i = 0; i < 999; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createString(sym[rand() % 4]));
        row.push_back(dolphindb::Util::createInt(i * 12));
        row.push_back(dolphindb::Util::createInt(i + 64));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    ASSERT_EQ(t1->size(), 0);
    mtwsp->insert(pErrorInfo, "A", 12, 16);
    mtwsp->waitForThreadCompletion();
    t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    ASSERT_EQ(t1->size(), 1000);
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorPartitionTableWithThrottle)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D);\n";
    script += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
              "pt = db.createPartitionedTable(t,`pt,`symbol);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    std::string sym[] = {"A", "B", "C", "D"};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    for (int i = 0; i < 999; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        srand((int)time(NULL));
        row.push_back(dolphindb::Util::createString(sym[rand() % 4]));
        row.push_back(dolphindb::Util::createInt(i * 12));
        row.push_back(dolphindb::Util::createInt(i + 64));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    ASSERT_EQ(t1->size(), 0);
    mtwsp->waitForThreadCompletion();
    t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    ASSERT_EQ(t1->size(), 999);
}

TEST_F(MultithreadedTableWriterNewTest, insertValueLongerThanTSDBPartitionTable)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                            "if(exists(dbName)){\n"
                            "\tdropDatabase(dbName)\t\n"
                            "}\n"
                            "db  = database(dbName, VALUE,symbol(\"A\"+string(0..10)),,`TSDB);\n"
                            "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
                            "pt = db.createPartitionedTable(t,`pt,`sym,,`sym`id)";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    bool flag = mtwsp->insert(pErrorInfo, "A1", 12, 12, 12);
    ASSERT_EQ(flag, false);
    ASSERT_EQ(pErrorInfo.errorInfo, "Column counts don't match 4");
}

TEST_F(MultithreadedTableWriterNewTest, insertValuesShorterThanTSDBPartitionTable)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,symbol(\"A\"+string(0..10)),,`TSDB);\n"
                        "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
                        "pt = db.createPartitionedTable(t,`pt,`sym,,`sym`id)";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    bool flag = mtwsp->insert(pErrorInfo, "A1", 12);
    ASSERT_EQ(flag, false);
    ASSERT_EQ(pErrorInfo.errorInfo, "Column counts don't match 2");
}

TEST_F(MultithreadedTableWriterNewTest, insertValuesErrorTypeTSDBPartitionTable_1)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,symbol(\"A\"+string(0..10)),,`TSDB);\n"
                        "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
                        "pt = db.createPartitionedTable(t,`pt,`sym,,`sym`id)";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    bool flag = mtwsp->insert(pErrorInfo, "A1", 12, "A");
    ASSERT_EQ(flag, false);
}

TEST_F(MultithreadedTableWriterNewTest, insertValuesTSDBPartitionTable)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,symbol(\"A\"+string(0..10)),,`TSDB);\n"
                        "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
                        "pt = db.createPartitionedTable(t,`pt,`sym,,`sym`id)";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    bool flag = mtwsp->insert(pErrorInfo, "A1", 12, 12);
    ASSERT_EQ(flag, true);
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt)");
    ASSERT_EQ((t1->getRow(0)->getString() == "value->12\nsym->A1\nid->12\n") || (t1->getRow(0)->getString() == "sym->A1\nid->12\nvalue->12\n") || (t1->getRow(0)->getString() == "value->12\nid->12\nsym->A1\n"), true);
}

TEST_F(MultithreadedTableWriterNewTest, insertValuesTSDBPartitionTableDifferentTypeData)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D,,`TSDB);\n";
    script += "t = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`bolb,"
              "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,BLOB]);"
              "pt = db.createPartitionedTable(t,`pt,`symbol,,`symbol`id);";
    pConn->run(script);
    dolphindb::Util::sleep(3000);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    unsigned char data[16] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    bool flag = mtwsp->insert(pErrorInfo, false, char(12), (short)12, (long)12, dolphindb::Util::createDate(2012, 11, 12), dolphindb::Util::createMonth(2012, 11), dolphindb::Util::createSecond(134),
                              dolphindb::Util::createDateTime(2012, 11, 13, 6, 12, 12), dolphindb::Util::createTimestamp(43241), dolphindb::Util::createNanoTime(532432),
                              dolphindb::Util::createNanoTimestamp(85932494), 12.4f, 3412.3, "A", "AAPL", data, data, data, 12, "0f0e0d0c0b0a0908070605040302010");
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt);");
    ASSERT_EQ(t1->getColumn(0)->getRow(0)->getString(), "0");
    ASSERT_EQ(t1->getColumn(1)->getRow(0)->getString(), "12");
    ASSERT_EQ(t1->getColumn(2)->getRow(0)->getString(), "12");
    ASSERT_EQ(t1->getColumn(3)->getRow(0)->getString(), "12");
    ASSERT_EQ(t1->getColumn(4)->getRow(0)->getString(), "2012.11.12");
    ASSERT_EQ(t1->getColumn(5)->getRow(0)->getString(), "2012.11M");
    ASSERT_EQ(t1->getColumn(6)->getRow(0)->getString(), "00:02:14");
    ASSERT_EQ(t1->getColumn(7)->getRow(0)->getString(), dolphindb::Util::createDateTime(2012, 11, 13, 6, 12, 12)->getString());
    ASSERT_EQ(t1->getColumn(8)->getRow(0)->getString(), dolphindb::Util::createTimestamp(43241)->getString());
    ASSERT_EQ(t1->getColumn(9)->getRow(0)->getString(), dolphindb::Util::createNanoTime(532432)->getString());
    ASSERT_EQ(t1->getColumn(10)->getRow(0)->getString(), dolphindb::Util::createNanoTimestamp(85932494)->getString());
    ASSERT_EQ(t1->getColumn(11)->getRow(0)->getString(), "12.4");
    ASSERT_EQ(t1->getColumn(12)->getRow(0)->getString(), "3412.3");
    ASSERT_EQ(t1->getColumn(13)->getRow(0)->getString(), "A");
    ASSERT_EQ(t1->getColumn(14)->getRow(0)->getString(), "AAPL");
    ASSERT_EQ(t1->getColumn(15)->getRow(0)->getString(), "0f0e0d0c-0b0a-0908-0706-050403020100");
    ASSERT_EQ(t1->getColumn(16)->getRow(0)->getString(), "f0e:d0c:b0a:908:706:504:302:100");
    ASSERT_EQ(t1->getColumn(17)->getRow(0)->getString(), "0f0e0d0c0b0a09080706050403020100");
    ASSERT_EQ(t1->getColumn(19)->getRow(0)->getString(), "0f0e0d0c0b0a0908070605040302010");
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorTSDBPartitionTableDifferentTypeDataLessThan256)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,string(0..10),,`TSDB);\n";
    script += "t = table(1000:0, `bool`char`short`long`date`month`datetime`second`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`bolb,"
              "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,DATETIME,SECOND,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,BLOB]);"
              "pt = db.createPartitionedTable(t,`pt,`symbol,,`symbol`id);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    int data[] = {0, 1};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    for (int i = 0; i < 10; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createBool((char)(data[i % 2])));
        row.push_back(dolphindb::Util::createChar(i * 2));
        row.push_back(dolphindb::Util::createShort(i + 12));
        row.push_back(dolphindb::Util::createLong((long)i * 100));
        row.push_back(dolphindb::Util::createDate(i + 432));
        row.push_back(dolphindb::Util::createMonth(i + 21));
        row.push_back(dolphindb::Util::createDateTime(i * 192));
        row.push_back(dolphindb::Util::createSecond((long long)i * 1932));
        row.push_back(dolphindb::Util::createTimestamp((long long)i * 2342));
        row.push_back(dolphindb::Util::createNanoTime((long long)i * 4214));
        row.push_back(dolphindb::Util::createNanoTimestamp((long long)i * 4264));
        row.push_back(dolphindb::Util::createFloat(i * 42.64));
        row.push_back(dolphindb::Util::createDouble(i * 4.264));
        row.push_back(dolphindb::Util::createString(std::to_string(i)));
        row.push_back(dolphindb::Util::createString("A" + std::to_string(i)));
        // uuid,ipAddr, int128, blob
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100"));
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.2." + std::to_string(i % 255)));
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_INT128, "0f0e0d0c0b0a09080706050403020100"));
        row.push_back(dolphindb::Util::createInt(i));
        row.push_back(dolphindb::Util::createBlob("0f0e0d0c0b0a0908070605040302010" + std::to_string(i)));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    std::cout << pErrorInfo.errorInfo;
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by long;");
    for (int i = 0; i < 10; i++)
    {
        ASSERT_EQ(t1->getColumn(0)->getRow(i)->getString(), dolphindb::Util::createBool((char)(data[i % 2]))->getString());
        ASSERT_EQ(t1->getColumn(1)->getRow(i)->getString(), dolphindb::Util::createChar(i * 2)->getString());
        ASSERT_EQ(t1->getColumn(2)->getRow(i)->getString(), dolphindb::Util::createShort(i + 12)->getString());
        ASSERT_EQ(t1->getColumn(3)->getRow(i)->getString(), dolphindb::Util::createLong((long)i * 100)->getString());
        ASSERT_EQ(t1->getColumn(4)->getRow(i)->getString(), dolphindb::Util::createDate(i + 432)->getString());
        ASSERT_EQ(t1->getColumn(5)->getRow(i)->getString(), dolphindb::Util::createMonth(i + 21)->getString());
        ASSERT_EQ(t1->getColumn(6)->getRow(i)->getString(), dolphindb::Util::createDateTime(i * 192)->getString());
        ASSERT_EQ(t1->getColumn(7)->getRow(i)->getString(), dolphindb::Util::createSecond((long long)i * 1932)->getString());
        ASSERT_EQ(t1->getColumn(8)->getRow(i)->getString(), dolphindb::Util::createTimestamp((long long)i * 2342)->getString());
        ASSERT_EQ(t1->getColumn(9)->getRow(i)->getString(), dolphindb::Util::createNanoTime((long long)i * 4214)->getString());
        ASSERT_EQ(t1->getColumn(10)->getRow(i)->getString(), dolphindb::Util::createNanoTimestamp((long long)i * 4264)->getString());
        ASSERT_EQ(t1->getColumn(11)->getRow(i)->getString(), dolphindb::Util::createFloat(i * 42.64)->getString());
        ASSERT_EQ(t1->getColumn(12)->getRow(i)->getString(), dolphindb::Util::createDouble(i * 4.264)->getString());
        ASSERT_EQ(t1->getColumn(13)->getRow(i)->getString(), dolphindb::Util::createString(std::to_string(i))->getString());
        ASSERT_EQ(t1->getColumn(14)->getRow(i)->getString(), dolphindb::Util::createString("A" + std::to_string(i))->getString());
        ASSERT_EQ(t1->getColumn(15)->getRow(i)->getString(), dolphindb::Util::parseConstant(dolphindb::DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100")->getString());
        ASSERT_EQ(t1->getColumn(16)->getRow(i)->getString(), dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.2." + std::to_string(i))->getString());
        ASSERT_EQ(t1->getColumn(17)->getRow(i)->getString(), dolphindb::Util::parseConstant(dolphindb::DT_INT128, "0f0e0d0c0b0a09080706050403020100")->getString());
        ASSERT_EQ(t1->getColumn(18)->getRow(i)->getString(), dolphindb::Util::createInt(i)->getString());
        ASSERT_EQ(t1->getColumn(19)->getRow(i)->getString(), dolphindb::Util::createString("0f0e0d0c0b0a0908070605040302010" + std::to_string(i))->getString());
    }
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorTSDBPartitionTableWithFilter)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,1..9,,`TSDB);\n";
    script += "t = table(1000:0,`id`symbol`value,[INT,SYMBOL,INT]);"
              "pt = db.createPartitionedTable(t,`pt,`id,,`symbol`id,LAST);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    int ids[] = {1, 2, 3, 4, 5, 6, 7, 8, 9};
    std::string syms[] = {"A", "B", "C", "D"};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    for (int i = 0; i < 2000; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createInt(ids[i % 9]));
        row.push_back(dolphindb::Util::createString(syms[i % 4]));
        row.push_back(dolphindb::Util::createInt(i));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by id;");
    ASSERT_EQ(t1->size(), 36);
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorTSDBPartitionTableDifferentTypeDataBiggerThan1048576)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D,,`TSDB);\n";
    script += "t = table(1000:0, `bool`char`short`long`date`month`datetime`second`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`bolb,"
              "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,DATETIME,SECOND,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,BLOB]);"
              "pt = db.createPartitionedTable(t,`pt,`symbol,,`symbol`id);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    int data[] = {0, 1};
    std::string sym[] = {"A", "B", "C", "D"};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    srand((int)time(NULL));
    for (int i = 0; i < 1098576; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createBool((char)(data[i % 2])));
        row.push_back(dolphindb::Util::createChar(i * 2));
        row.push_back(dolphindb::Util::createShort(i + 12));
        row.push_back(dolphindb::Util::createLong((long)i * 100));
        row.push_back(dolphindb::Util::createDate(i + 432));
        row.push_back(dolphindb::Util::createMonth(i + 21));
        row.push_back(dolphindb::Util::createDateTime(i * 192));
        row.push_back(dolphindb::Util::createSecond((long long)i * 1932));
        row.push_back(dolphindb::Util::createTimestamp((long long)i * 2342));
        row.push_back(dolphindb::Util::createNanoTime((long long)i * 4214));
        row.push_back(dolphindb::Util::createNanoTimestamp((long long)i * 4264));
        row.push_back(dolphindb::Util::createFloat(i * 42.64));
        row.push_back(dolphindb::Util::createDouble(i * 4.264));
        row.push_back(dolphindb::Util::createString(sym[rand() % 4]));
        row.push_back(dolphindb::Util::createString("A" + std::to_string(i)));
        // uuid,ipAddr, int128, blob
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100"));
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.2." + std::to_string(i % 255)));
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_INT128, "0f0e0d0c0b0a09080706050403020100"));
        row.push_back(dolphindb::Util::createInt(i));
        row.push_back(dolphindb::Util::createBlob("0f0e0d0c0b0a0908070605040302010" + std::to_string(i)));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by long limit 1024;");
    for (int i = 0; i < 1024; i++)
    {
        ASSERT_EQ(t1->getColumn(0)->getRow(i)->getString(), dolphindb::Util::createBool((char)(data[i % 2]))->getString());
        ASSERT_EQ(t1->getColumn(1)->getRow(i)->getString(), dolphindb::Util::createChar(i * 2)->getString());
        ASSERT_EQ(t1->getColumn(2)->getRow(i)->getString(), dolphindb::Util::createShort(i + 12)->getString());
        ASSERT_EQ(t1->getColumn(3)->getRow(i)->getString(), dolphindb::Util::createLong((long)i * 100)->getString());
        ASSERT_EQ(t1->getColumn(4)->getRow(i)->getString(), dolphindb::Util::createDate(i + 432)->getString());
        ASSERT_EQ(t1->getColumn(5)->getRow(i)->getString(), dolphindb::Util::createMonth(i + 21)->getString());
        ASSERT_EQ(t1->getColumn(6)->getRow(i)->getString(), dolphindb::Util::createDateTime(i * 192)->getString());
        ASSERT_EQ(t1->getColumn(7)->getRow(i)->getString(), dolphindb::Util::createSecond((long long)i * 1932)->getString());
        ASSERT_EQ(t1->getColumn(8)->getRow(i)->getString(), dolphindb::Util::createTimestamp((long long)i * 2342)->getString());
        ASSERT_EQ(t1->getColumn(9)->getRow(i)->getString(), dolphindb::Util::createNanoTime((long long)i * 4214)->getString());
        ASSERT_EQ(t1->getColumn(10)->getRow(i)->getString(), dolphindb::Util::createNanoTimestamp((long long)i * 4264)->getString());
        ASSERT_EQ(t1->getColumn(11)->getRow(i)->getString(), dolphindb::Util::createFloat(i * 42.64)->getString());
        ASSERT_EQ(t1->getColumn(12)->getRow(i)->getString(), dolphindb::Util::createDouble(i * 4.264)->getString());
        ASSERT_EQ(t1->getColumn(14)->getRow(i)->getString(), dolphindb::Util::createString("A" + std::to_string(i))->getString());
        ASSERT_EQ(t1->getColumn(15)->getRow(i)->getString(), dolphindb::Util::parseConstant(dolphindb::DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100")->getString());
        ASSERT_EQ(t1->getColumn(16)->getRow(i)->getString(), dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.2." + std::to_string(i % 255))->getString());
        ASSERT_EQ(t1->getColumn(17)->getRow(i)->getString(), dolphindb::Util::parseConstant(dolphindb::DT_INT128, "0f0e0d0c0b0a09080706050403020100")->getString());
        ASSERT_EQ(t1->getColumn(18)->getRow(i)->getString(), dolphindb::Util::createInt(i)->getString());
        ASSERT_EQ(t1->getColumn(19)->getRow(i)->getString(), dolphindb::Util::createString("0f0e0d0c0b0a0908070605040302010" + std::to_string(i))->getString());
    }
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorTSDBPartitionTableWithNullptr)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,symbol(\"A\"+string(0..10)),,`TSDB);\n";
    script += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
              "pt = db.createPartitionedTable(t,`pt,`symbol,,`symbol`id);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    bool flag = mtwsp->insert(pErrorInfo, "A1", (std::nullptr_t)0, 12);
    ASSERT_EQ(flag, true);
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt)");
    ASSERT_EQ((t1->getRow(0)->getString() == "value->12\nsymbol->A1\nid->\n") || (t1->getRow(0)->getString() == "symbol->A1\nid->\nvalue->12\n") || (t1->getRow(0)->getString() == "value->12\nid->\nsymbol->A1\n"), true);
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorTSDBPartitionTableWithbatchSize)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D,,`TSDB);\n";
    script += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
              "pt = db.createPartitionedTable(t,`pt,`symbol,,`symbol`id);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    std::string sym[] = {"A", "B", "C", "D"};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    srand((int)time(NULL));
    for (int i = 0; i < 999; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createString(sym[rand() % 4]));
        row.push_back(dolphindb::Util::createInt(i * 12));
        row.push_back(dolphindb::Util::createInt(i + 64));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    ASSERT_EQ(t1->size(), 0);
    mtwsp->insert(pErrorInfo, "A", 12, 16);
    mtwsp->waitForThreadCompletion();
    t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    ASSERT_EQ(t1->size(), 1000);
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorTSDBPartitionTableWithThrottle)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D,,`TSDB);\n";
    script += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
              "pt = db.createPartitionedTable(t,`pt,`symbol,,`symbol`id);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    std::string sym[] = {"A", "B", "C", "D"};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    srand((int)time(NULL));
    for (int i = 0; i < 999; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createString(sym[rand() % 4]));
        row.push_back(dolphindb::Util::createInt(i * 12));
        row.push_back(dolphindb::Util::createInt(i + 64));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    ASSERT_EQ(t1->size(), 0);
    mtwsp->waitForThreadCompletion();
    t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    ASSERT_EQ(t1->size(), 999);
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorPartitionTableRangeType)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, RANGE,1 5000 15000);\n";
    script += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
              "pt = db.createPartitionedTable(t,`pt,`id);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    std::string sym[] = {"A", "B", "C", "D"};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    srand((int)time(NULL));
    for (int i = 0; i < 999; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createString(sym[rand() % 4]));
        row.push_back(dolphindb::Util::createInt(i * 12));
        row.push_back(dolphindb::Util::createInt(i + 64));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    ASSERT_EQ(t1->size(), 0);
    mtwsp->waitForThreadCompletion();
    t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    ASSERT_EQ(t1->size(), 998);
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorPartitionTableVALUEType)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D);\n";
    script += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
              "pt = db.createPartitionedTable(t,`pt,`symbol);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    std::string sym[] = {"A", "B", "C", "D"};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    srand((int)time(NULL));
    for (int i = 0; i < 999; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createString(sym[rand() % 4]));
        row.push_back(dolphindb::Util::createInt(i * 12));
        row.push_back(dolphindb::Util::createInt(i + 64));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    ASSERT_EQ(t1->size(), 0);
    mtwsp->waitForThreadCompletion();
    t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    ASSERT_EQ(t1->size(), 999);
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorPartitionTableHASHTSymbolType)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, HASH,[SYMBOL, 4]);\n";
    script += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
              "pt = db.createPartitionedTable(t,`pt,`symbol);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    std::string sym[] = {"A", "B", "C", "D"};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    srand((int)time(NULL));
    for (int i = 0; i < 999; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createString(sym[rand() % 4]));
        row.push_back(dolphindb::Util::createInt(i * 12));
        row.push_back(dolphindb::Util::createInt(i + 64));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    ASSERT_EQ(t1->size(), 0);
    mtwsp->waitForThreadCompletion();
    t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    ASSERT_EQ(t1->size(), 999);
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorPartitionTableHASHTDateType)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,datehour(1970.01.01T02:46:40)+0..4);\n";
    script += "t = table(1000:0, `symbol`date`value,[SYMBOL, DATETIME, INT]);"
              "pt = db.createPartitionedTable(t,`pt,`date);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    std::string sym[] = {"A", "B", "C", "D"};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    srand((int)time(NULL));
    for (int i = 0; i < 999; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createString(sym[rand() % 4]));
        row.push_back(dolphindb::Util::createDateTime(i + 10000));
        row.push_back(dolphindb::Util::createInt(i + 64));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    mtwsp->insert(pErrorInfo, "A", dolphindb::Util::createDateTime(999 + 10000), 999 + 64);
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by date;");
    ASSERT_EQ(t1->size(), 0);
    mtwsp->waitForThreadCompletion();
    t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    for (int i = 0; i < 1000; i++)
    {
        ASSERT_EQ(t1->getColumn(1)->getRow(i)->getString(), dolphindb::Util::createDateTime(i + 10000)->getString());
    }
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorPartitionTableHASHTDateTimeType)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, HASH,[DATETIME, 4]);\n";
    script += "t = table(1000:0, `symbol`date`value,[SYMBOL, DATETIME, INT]);"
              "pt = db.createPartitionedTable(t,`pt,`date);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    std::string sym[] = {"A", "B", "C", "D"};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    srand((int)time(NULL));
    for (int i = 0; i < 999; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createString(sym[rand() % 4]));
        row.push_back(dolphindb::Util::createDateTime(i));
        row.push_back(dolphindb::Util::createInt(i + 64));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by date;");
    ASSERT_EQ(t1->size(), 0);
    mtwsp->waitForThreadCompletion();
    t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    for (int i = 0; i < 999; i++)
    {
        ASSERT_EQ(t1->getColumn(1)->getRow(i)->getString(), dolphindb::Util::createDateTime(i)->getString());
    }
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorPartitionTableHASHTTimeType)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, HASH,[TIME, 4]);\n";
    script += "t = table(1000:0, `symbol`date`value,[SYMBOL, TIME, INT]);"
              "pt = db.createPartitionedTable(t,`pt,`date);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    std::string sym[] = {"A", "B", "C", "D"};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    srand((int)time(NULL));
    for (int i = 0; i < 999; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createString(sym[rand() % 4]));
        row.push_back(dolphindb::Util::createTime(i));
        row.push_back(dolphindb::Util::createInt(i + 64));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by date;");
    ASSERT_EQ(t1->size(), 0);
    mtwsp->waitForThreadCompletion();
    t1 = pConn->run("select * from loadTable(dbName,`pt) order by date;");
    for (int i = 0; i < 999; i++)
    {
        ASSERT_EQ(t1->getColumn(1)->getRow(i)->getString(), dolphindb::Util::createTime(i)->getString());
    }
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorPartitionTableHASHIntType)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, HASH,[INT, 4]);\n";
    script += "t = table(1000:0, `symbol`date`value,[SYMBOL, INT, INT]);"
              "pt = db.createPartitionedTable(t,`pt,`date);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    std::string sym[] = {"A", "B", "C", "D"};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    srand((int)time(NULL));
    for (int i = 0; i < 999; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createString(sym[rand() % 4]));
        row.push_back(dolphindb::Util::createInt(i));
        row.push_back(dolphindb::Util::createInt(i + 64));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by date;");
    ASSERT_EQ(t1->size(), 0);
    mtwsp->waitForThreadCompletion();
    t1 = pConn->run("select * from loadTable(dbName,`pt) order by date;");
    for (int i = 0; i < 999; i++)
    {
        ASSERT_EQ(t1->getColumn(1)->getRow(i)->getString(), dolphindb::Util::createInt(i)->getString());
    }
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorPartitionTableLISTSymbolType)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, LIST,[`A`B`C,`F`D]);\n";
    script += "t = table(1000:0, `symbol`date`value,[SYMBOL, INT, INT]);"
              "pt = db.createPartitionedTable(t,`pt,`symbol);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    std::string sym[] = {"A", "B", "C", "D", "F"};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    srand((int)time(NULL));
    for (int i = 0; i < 999; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createString(sym[rand() % 5]));
        row.push_back(dolphindb::Util::createInt(i));
        row.push_back(dolphindb::Util::createInt(i + 64));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by date;");
    ASSERT_EQ(t1->size(), 0);
    mtwsp->waitForThreadCompletion();
    t1 = pConn->run("select * from loadTable(dbName,`pt) order by date;");
    for (int i = 0; i < 999; i++)
    {
        ASSERT_EQ(t1->getColumn(1)->getRow(i)->getString(), dolphindb::Util::createInt(i)->getString());
    }
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorPartitionTableLISTIntType)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, LIST,[1..400,401..1000]);\n";
    script += "t = table(1000:0, `symbol`date`value,[SYMBOL, INT, INT]);"
              "pt = db.createPartitionedTable(t,`pt,`date);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    std::string sym[] = {"A", "B", "C", "D", "F"};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    srand((int)time(NULL));
    for (int i = 0; i < 999; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createString(sym[rand() % 5]));
        row.push_back(dolphindb::Util::createInt(i));
        row.push_back(dolphindb::Util::createInt(i + 64));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by date;");
    ASSERT_EQ(t1->size(), 0);
    mtwsp->waitForThreadCompletion();
    t1 = pConn->run("select * from loadTable(dbName,`pt) order by date;");
    for (int i = 0; i < 998; i++)
    {
        ASSERT_EQ(t1->getColumn(1)->getRow(i)->getString(), dolphindb::Util::createInt(i + 1)->getString());
    }
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorPartitionTableLISTDateType)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, LIST,[2012.11.12 2012.11.13 2012.11.14,2012.12.12 2012.12.13 2012.12.14]);\n";
    script += "t = table(1000:0, `symbol`date`value,[SYMBOL, DATE, INT]);"
              "pt = db.createPartitionedTable(t,`pt,`date);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    int date[] = {15657, 15656, 15658, 15686, 15687, 15688};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    srand((int)time(NULL));
    for (int i = 0; i < 999; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createString("A"));
        row.push_back(dolphindb::Util::createDate(date[rand() % 6]));
        row.push_back(dolphindb::Util::createInt(i + 64));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    ASSERT_EQ(t1->size(), 0);
    mtwsp->waitForThreadCompletion();
    t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    for (int i = 0; i < 999; i++)
    {
        ASSERT_EQ(t1->getColumn(2)->getRow(i)->getString(), dolphindb::Util::createInt(i + 64)->getString());
    }
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorPartitionTableCompoDateAndIntType)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\n"
                        "}\n"
                        "dbDate = database(,VALUE,2012.11.10..2015.01.01)\n"
                        "dbId= database(,RANGE,1 564 1200)\n"
                        "db  = database(dbName, COMPO,[dbDate, dbId])\n"
                        "t = table(1000:0, `date`id`value,[DATE,INT,INT])\n"
                        "pt = db.createPartitionedTable(t,`pt,`date`id)";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    int date[] = {15657, 15656, 15658, 15686, 15687, 15688};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    for (int i = 0; i < 999; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createDate(i + 15654));
        row.push_back(dolphindb::Util::createInt(i));
        row.push_back(dolphindb::Util::createInt(i + 64));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    ASSERT_EQ(t1->size(), 0);
    mtwsp->waitForThreadCompletion();
    t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    for (int i = 0; i < 998; i++)
    {
        ASSERT_EQ(t1->getColumn(2)->getRow(i)->getString(), dolphindb::Util::createInt(i + 65)->getString());
    }
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorPartitionTableCompoDateAndSymbolType)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\n"
                        "}\n"
                        "dbDate = database(,VALUE,2012.11.12 2012.11.13 2012.11.14 2012.12.12 2012.12.13 2012.12.14)\n"
                        "dbId= database(,HASH,[SYMBOL,3])\n"
                        "db  = database(dbName, COMPO,[dbDate, dbId])\n"
                        "t = table(1000:0, `date`id`value,[DATE,SYMBOL,INT])\n"
                        "pt = db.createPartitionedTable(t,`pt,`date`id)";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    int date[] = {15657, 15656, 15658, 15686, 15687, 15688};
    std::string sym[] = {"A", "B", "C", "D", "F"};
    srand((int)time(NULL));
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    for (int i = 0; i < 999; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createDate(date[rand() % 6]));
        row.push_back(dolphindb::Util::createString(sym[rand() % 5]));
        row.push_back(dolphindb::Util::createInt(i + 64));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    ASSERT_EQ(t1->size(), 0);
    mtwsp->waitForThreadCompletion();
    t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    for (int i = 0; i < 998; i++)
    {
        ASSERT_EQ(t1->getColumn(2)->getRow(i)->getString(), dolphindb::Util::createInt(i + 64)->getString());
    }
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorPartitionTableCompoDateAndSymbolType_2)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\n"
                        "}\n"
                        "dbDate = database(,LIST,2012.11.12 2012.11.13 2012.11.14 2012.12.12 2012.12.13 2012.12.14)\n"
                        "dbId= database(,HASH,[SYMBOL,3])\n"
                        "db  = database(dbName, COMPO,[dbDate, dbId])\n"
                        "t = table(1000:0, `date`id`value,[DATE,SYMBOL,INT])\n"
                        "pt = db.createPartitionedTable(t,`pt,`date`id)";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    int date[] = {15657, 15656, 15658, 15686, 15687, 15688};
    std::string sym[] = {"A", "B", "C", "D", "F"};
    srand((int)time(NULL));
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    for (int i = 0; i < 999; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createDate(date[rand() % 6]));
        row.push_back(dolphindb::Util::createString(sym[rand() % 5]));
        row.push_back(dolphindb::Util::createInt(i + 64));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    ASSERT_EQ(t1->size(), 0);
    mtwsp->waitForThreadCompletion();
    t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    for (int i = 0; i < 998; i++)
    {
        ASSERT_EQ(t1->getColumn(2)->getRow(i)->getString(), dolphindb::Util::createInt(i + 64)->getString());
    }
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorPartitionTableCompoIntAndSymbolType_2)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\n"
                        "}\n"
                        "dbDate = database(,RANGE,0 500 1200)\n"
                        "dbId= database(,HASH,[SYMBOL,3])\n"
                        "db  = database(dbName, COMPO,[dbDate, dbId])\n"
                        "t = table(1000:0, `date`id`value,[INT,SYMBOL,INT])\n"
                        "pt = db.createPartitionedTable(t,`pt,`date`id)";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    int date[] = {15657, 15656, 15658, 15686, 15687, 15688};
    std::string sym[] = {"A", "B", "C", "D", "F"};
    srand((int)time(NULL));
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    for (int i = 0; i < 999; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createInt(i));
        row.push_back(dolphindb::Util::createString(sym[rand() % 5]));
        row.push_back(dolphindb::Util::createInt(i + 64));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    ASSERT_EQ(t1->size(), 0);
    mtwsp->waitForThreadCompletion();
    t1 = pConn->run("select * from loadTable(dbName,`pt) order by value;");
    for (int i = 0; i < 998; i++)
    {
        ASSERT_EQ(t1->getColumn(2)->getRow(i)->getString(), dolphindb::Util::createInt(i + 64)->getString());
    }
}

TEST_F(MultithreadedTableWriterNewTest, insertValuesUnWritten)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D`E)\n"
                        "t = table(1000:0, `date`id`value,[DATE,SYMBOL,INT])\n"
                        "pt = db.createPartitionedTable(t,`pt,`id)";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    std::vector<std::vector<dolphindb::ConstantSP> *> data;
    std::string sym[] = {"A", "B", "C", "D", "E"};
    srand((int)time(NULL));
    for (int i = 0; i < 1000; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createDate(i));
        row.push_back(dolphindb::Util::createString(sym[rand() % 5]));
        row.push_back(dolphindb::Util::createInt(i));
        data.push_back(prow);
    }
    mtwsp->insertUnwrittenData(data, pErrorInfo);
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    mtwsp->getUnwrittenData(datas);
}

TEST_F(MultithreadedTableWriterNewTest, getStatus)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D`E)\n"
                        "t = table(1000:0, `date`id`value,[DATE,SYMBOL,INT])\n"
                        "pt = db.createPartitionedTable(t,`pt,`id)";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    std::vector<std::vector<dolphindb::ConstantSP> *> data;
    std::string sym[] = {"A", "B", "C", "D", "E"};
    srand((int)time(NULL));
    for (int i = 0; i < 1000; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createDate(i));
        row.push_back(dolphindb::Util::createString(sym[rand() % 5]));
        row.push_back(dolphindb::Util::createInt(i));
        data.push_back(prow);
    }
    mtwsp->insertUnwrittenData(data, pErrorInfo);
    mtwsp->waitForThreadCompletion();
    dolphindb::MultithreadedTableWriter::Status statu;
    mtwsp->getStatus(statu);
    ASSERT_EQ(statu.sentRows, 1000);
    ASSERT_EQ(statu.unsentRows, 0);
}

TEST_F(MultithreadedTableWriterNewTest, getStatus2)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D`E)\n"
                        "t = table(1000:0, `date`id`value,[DATE,SYMBOL,INT])\n"
                        "pt = db.createPartitionedTable(t,`pt,`id)";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    std::vector<std::vector<dolphindb::ConstantSP> *> data;
    std::string sym[] = {"A", "B", "C", "D", "E"};
    srand((int)time(NULL));
    for (int i = 0; i < 1000; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createDate(i));
        row.push_back(dolphindb::Util::createInt(rand() % 5));
        row.push_back(dolphindb::Util::createInt(i));
        data.push_back(prow);
    }
    dolphindb::MultithreadedTableWriter::Status statu;
    mtwsp->getStatus(statu);
    ASSERT_EQ(statu.errorInfo, pErrorInfo.errorInfo);
}

TEST_F(MultithreadedTableWriterNewTest, getErrorStatus)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D`E)\n"
                        "t = table(1000:0, `date`id`value,[DATE,SYMBOL,INT])\n"
                        "pt = db.createPartitionedTable(t,`pt,`id)";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    bool flag = mtwsp->insert(pErrorInfo, "A", "A", 23);
    dolphindb::MultithreadedTableWriter::Status statu;
    mtwsp->getStatus(statu);
}

TEST_F(MultithreadedTableWriterNewTest, threadByColNameNULLOlapDimensionTable)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D`E)\n"
                        "t = table(1000:0, `date`id`value,[DATE,SYMBOL,INT])\n"
                        "pt = db.createTable(t,`pt)";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    ASSERT_ANY_THROW(config.setThreads(5, ""));
}

TEST_F(MultithreadedTableWriterNewTest, insertValuesOlapDimensionTableDifferentType)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    int scale32 = rand() % 9;
    int scale64 = rand() % 18;

    dolphindb::ConstantSP decimal32val = dolphindb::Util::createDecimal32(scale32, rand() % 1000 / (double)100);
    dolphindb::ConstantSP decimal64val = dolphindb::Util::createDecimal64(scale64, rand() % 1000 / (double)100);
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D);\n";
    script += "t = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`deci32`deci64`deci32extra`deci64extra,"
              "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,DECIMAL32(" +
              std::to_string(scale32) + "),DECIMAL64(" + std::to_string(scale64) + "),DECIMAL32(" + std::to_string(scale32) + "),DECIMAL64(" + std::to_string(scale64) + ")]);pt = db.createTable(t,`pt);";
    pConn->run(script);
    dolphindb::Util::sleep(1000);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    unsigned char data[16] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    bool flag = mtwsp->insert(pErrorInfo, false, char(12), (short)12, (long)12, dolphindb::Util::createDate(2012, 11, 12), dolphindb::Util::createMonth(2012, 11), dolphindb::Util::createSecond(134),
                              dolphindb::Util::createDateTime(2012, 11, 13, 6, 12, 12), dolphindb::Util::createTimestamp(43241), dolphindb::Util::createNanoTime(532432),
                              dolphindb::Util::createNanoTimestamp(85932494), 12.4f, 3412.3, "A", "AAPL", data, data, data, 12, decimal32val, decimal64val, 2.36735, 4);
    mtwsp->waitForThreadCompletion();
    dolphindb::TableSP t1 = pConn->run("select * from loadTable(dbName,`pt);");
    ASSERT_EQ(t1->rows(), 1);
    ASSERT_EQ(t1->getColumn(0)->getRow(0)->getString(), "0");
    ASSERT_EQ(t1->getColumn(1)->getRow(0)->getString(), "12");
    ASSERT_EQ(t1->getColumn(2)->getRow(0)->getString(), "12");
    ASSERT_EQ(t1->getColumn(3)->getRow(0)->getString(), "12");
    ASSERT_EQ(t1->getColumn(4)->getRow(0)->getString(), "2012.11.12");
    ASSERT_EQ(t1->getColumn(5)->getRow(0)->getString(), "2012.11M");
    ASSERT_EQ(t1->getColumn(6)->getRow(0)->getString(), "00:02:14");
    ASSERT_EQ(t1->getColumn(7)->getRow(0)->getString(), dolphindb::Util::createDateTime(2012, 11, 13, 6, 12, 12)->getString());
    ASSERT_EQ(t1->getColumn(8)->getRow(0)->getString(), dolphindb::Util::createTimestamp(43241)->getString());
    ASSERT_EQ(t1->getColumn(9)->getRow(0)->getString(), dolphindb::Util::createNanoTime(532432)->getString());
    ASSERT_EQ(t1->getColumn(10)->getRow(0)->getString(), dolphindb::Util::createNanoTimestamp(85932494)->getString());
    ASSERT_EQ(t1->getColumn(11)->getRow(0)->getString(), "12.4");
    ASSERT_EQ(t1->getColumn(12)->getRow(0)->getString(), "3412.3");
    ASSERT_EQ(t1->getColumn(13)->getRow(0)->getString(), "A");
    ASSERT_EQ(t1->getColumn(14)->getRow(0)->getString(), "AAPL");
    ASSERT_EQ(t1->getColumn(15)->getRow(0)->getString(), "0f0e0d0c-0b0a-0908-0706-050403020100");
    ASSERT_EQ(t1->getColumn(16)->getRow(0)->getString(), "f0e:d0c:b0a:908:706:504:302:100");
    ASSERT_EQ(t1->getColumn(17)->getRow(0)->getString(), "0f0e0d0c0b0a09080706050403020100");
    ASSERT_EQ(t1->getColumn(18)->getRow(0)->getString(), "12");
    ASSERT_EQ(t1->getColumn(19)->getRow(0)->getString(), decimal32val->getString());
    ASSERT_EQ(t1->getColumn(20)->getRow(0)->getString(), decimal64val->getString());
    ASSERT_EQ(t1->getColumn(21)->getRow(0)->getString(), dolphindb::Util::createDecimal32(scale32, 2.36735)->getString());
    ASSERT_EQ(t1->getColumn(22)->getRow(0)->getString(), dolphindb::Util::createDecimal64(scale64, 4)->getString());
}

TEST_F(MultithreadedTableWriterNewTest, insertValuesOlapDimensionTableDifferentTypeDatabiggerThan65535)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D);\n";
    script += "t = table(1000:0, `bool`char`short`long`date`month`datetime`second`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id,"
              "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,DATETIME,SECOND,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT]);"
              "pt = db.createTable(t,`pt);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    int data[] = {0, 1};
    std::string syms[] = {"A", "B", "C", "D"};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    for (int i = 0; i < 3131070; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createBool((char)(data[i % 2])));
        row.push_back(dolphindb::Util::createChar(i * 2));
        row.push_back(dolphindb::Util::createShort(i + 12));
        row.push_back(dolphindb::Util::createLong((long)i * 100));
        row.push_back(dolphindb::Util::createDate(i + 432));
        row.push_back(dolphindb::Util::createMonth(i + 21));
        row.push_back(dolphindb::Util::createDateTime(i * 192));
        row.push_back(dolphindb::Util::createSecond((long long)i * 1932));
        row.push_back(dolphindb::Util::createTimestamp((long long)i * 2342));
        row.push_back(dolphindb::Util::createNanoTime((long long)i * 4214));
        row.push_back(dolphindb::Util::createNanoTimestamp((long long)i * 4264));
        row.push_back(dolphindb::Util::createFloat(i * 42.64));
        row.push_back(dolphindb::Util::createDouble(i * 4.264));
        row.push_back(dolphindb::Util::createString(syms[i % 2]));
        row.push_back(dolphindb::Util::createString("A" + std::to_string(i)));
        // uuid,ipAddr, int128, blob
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100"));
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.2." + std::to_string(i % 255)));
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_INT128, "0f0e0d0c0b0a09080706050403020100"));
        row.push_back(dolphindb::Util::createInt(i));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by long;");
    for (int i = 0; i < 2048; i++)
    {
        ASSERT_EQ(t1->getColumn(0)->getRow(i)->getString(), dolphindb::Util::createBool((char)(data[i % 2]))->getString());
        ASSERT_EQ(t1->getColumn(1)->getRow(i)->getString(), dolphindb::Util::createChar(i * 2)->getString());
        ASSERT_EQ(t1->getColumn(2)->getRow(i)->getString(), dolphindb::Util::createShort(i + 12)->getString());
        ASSERT_EQ(t1->getColumn(3)->getRow(i)->getString(), dolphindb::Util::createLong((long)i * 100)->getString());
        ASSERT_EQ(t1->getColumn(4)->getRow(i)->getString(), dolphindb::Util::createDate(i + 432)->getString());
        ASSERT_EQ(t1->getColumn(5)->getRow(i)->getString(), dolphindb::Util::createMonth(i + 21)->getString());
        ASSERT_EQ(t1->getColumn(6)->getRow(i)->getString(), dolphindb::Util::createDateTime(i * 192)->getString());
        ASSERT_EQ(t1->getColumn(7)->getRow(i)->getString(), dolphindb::Util::createSecond((long long)i * 1932)->getString());
        ASSERT_EQ(t1->getColumn(8)->getRow(i)->getString(), dolphindb::Util::createTimestamp((long long)i * 2342)->getString());
        ASSERT_EQ(t1->getColumn(9)->getRow(i)->getString(), dolphindb::Util::createNanoTime((long long)i * 4214)->getString());
        ASSERT_EQ(t1->getColumn(10)->getRow(i)->getString(), dolphindb::Util::createNanoTimestamp((long long)i * 4264)->getString());
        ASSERT_EQ(t1->getColumn(11)->getRow(i)->getString(), dolphindb::Util::createFloat(i * 42.64)->getString());
        ASSERT_EQ(t1->getColumn(12)->getRow(i)->getString(), dolphindb::Util::createDouble(i * 4.264)->getString());
        ASSERT_EQ(t1->getColumn(13)->getRow(i)->getString(), dolphindb::Util::createString(syms[i % 2])->getString());
        ASSERT_EQ(t1->getColumn(14)->getRow(i)->getString(), dolphindb::Util::createString("A" + std::to_string(i))->getString());
        ASSERT_EQ(t1->getColumn(15)->getRow(i)->getString(), dolphindb::Util::parseConstant(dolphindb::DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100")->getString());
        ASSERT_EQ(t1->getColumn(16)->getRow(i)->getString(), dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.2." + std::to_string(i % 255))->getString());
        ASSERT_EQ(t1->getColumn(17)->getRow(i)->getString(), dolphindb::Util::parseConstant(dolphindb::DT_INT128, "0f0e0d0c0b0a09080706050403020100")->getString());
        ASSERT_EQ(t1->getColumn(18)->getRow(i)->getString(), dolphindb::Util::createInt(i)->getString());
    }
}

TEST_F(MultithreadedTableWriterNewTest, threadByColNameNULLTSDBDimensionTable)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,symbol(\"A\"+string(0..10)),,`TSDB);\n"
                        "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
                        "pt = db.createTable(t,`pt,,`sym)";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    ASSERT_ANY_THROW(config.setThreads(5, ""));
}

TEST_F(MultithreadedTableWriterNewTest, insertValuesTSDBDimensionTableDifferentType)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D,,`TSDB);\n";
    script += "t = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`bolb,"
              "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,BLOB]);"
              "pt = db.createTable(t,`pt,,`symbol);";
    pConn->run(script);
    dolphindb::Util::sleep(3000);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    unsigned char data[16] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    bool flag = mtwsp->insert(pErrorInfo, false, char(12), (short)12, (long)12,
                              dolphindb::Util::createDate(2012, 11, 12), dolphindb::Util::createMonth(2012, 11), dolphindb::Util::createSecond(134),
                              dolphindb::Util::createDateTime(2012, 11, 13, 6, 12, 12), dolphindb::Util::createTimestamp(43241),
                              dolphindb::Util::createNanoTime(532432),
                              dolphindb::Util::createNanoTimestamp(85932494), 12.4f, 3412.3, "A", "AAPL", data, data, data, 12,
                              "0f0e0d0c0b0a0908070605040302010");
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt);");
    ASSERT_EQ(t1->getColumn(0)->getRow(0)->getString(), "0");
    ASSERT_EQ(t1->getColumn(1)->getRow(0)->getString(), "12");
    ASSERT_EQ(t1->getColumn(2)->getRow(0)->getString(), "12");
    ASSERT_EQ(t1->getColumn(3)->getRow(0)->getString(), "12");
    ASSERT_EQ(t1->getColumn(4)->getRow(0)->getString(), "2012.11.12");
    ASSERT_EQ(t1->getColumn(5)->getRow(0)->getString(), "2012.11M");
    ASSERT_EQ(t1->getColumn(6)->getRow(0)->getString(), "00:02:14");
    ASSERT_EQ(t1->getColumn(7)->getRow(0)->getString(), dolphindb::Util::createDateTime(2012, 11, 13, 6, 12, 12)->getString());
    ASSERT_EQ(t1->getColumn(8)->getRow(0)->getString(), dolphindb::Util::createTimestamp(43241)->getString());
    ASSERT_EQ(t1->getColumn(9)->getRow(0)->getString(), dolphindb::Util::createNanoTime(532432)->getString());
    ASSERT_EQ(t1->getColumn(10)->getRow(0)->getString(), dolphindb::Util::createNanoTimestamp(85932494)->getString());
    ASSERT_EQ(t1->getColumn(11)->getRow(0)->getString(), "12.4");
    ASSERT_EQ(t1->getColumn(12)->getRow(0)->getString(), "3412.3");
    ASSERT_EQ(t1->getColumn(13)->getRow(0)->getString(), "A");
    ASSERT_EQ(t1->getColumn(14)->getRow(0)->getString(), "AAPL");
    ASSERT_EQ(t1->getColumn(15)->getRow(0)->getString(), "0f0e0d0c-0b0a-0908-0706-050403020100");
    ASSERT_EQ(t1->getColumn(16)->getRow(0)->getString(), "f0e:d0c:b0a:908:706:504:302:100");
    ASSERT_EQ(t1->getColumn(17)->getRow(0)->getString(), "0f0e0d0c0b0a09080706050403020100");
    ASSERT_EQ(t1->getColumn(19)->getRow(0)->getString(), "0f0e0d0c0b0a0908070605040302010");
}

TEST_F(MultithreadedTableWriterNewTest, insertValuesTSDBDimensionTableBiggerThan1048576)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D,,`TSDB);\n";
    script += "t = table(1000:0, `bool`char`short`long`date`month`datetime`second`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`bolb,"
              "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,DATETIME,SECOND,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,BLOB]);"
              "pt = db.createTable(t,`pt,,`symbol);";
    pConn->run(script);
    dolphindb::Util::sleep(3000);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    unsigned char data[16] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    std::string syms[] = {"A", "B", "C", "D"};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    for (int i = 0; i < 3098576; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createBool((char)(data[i % 2])));
        row.push_back(dolphindb::Util::createChar(i * 2));
        row.push_back(dolphindb::Util::createShort(i + 12));
        row.push_back(dolphindb::Util::createLong((long)i * 100));
        row.push_back(dolphindb::Util::createDate(i + 432));
        row.push_back(dolphindb::Util::createMonth(i + 21));
        row.push_back(dolphindb::Util::createDateTime(i * 192));
        row.push_back(dolphindb::Util::createSecond((long long)i * 1932));
        row.push_back(dolphindb::Util::createTimestamp((long long)i * 2342));
        row.push_back(dolphindb::Util::createNanoTime((long long)i * 4214));
        row.push_back(dolphindb::Util::createNanoTimestamp((long long)i * 4264));
        row.push_back(dolphindb::Util::createFloat(i * 42.64));
        row.push_back(dolphindb::Util::createDouble(i * 4.264));
        row.push_back(dolphindb::Util::createString(syms[i % 4]));
        row.push_back(dolphindb::Util::createString(std::to_string(i)));
        // uuid,ipAddr, int128, blob
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100"));
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.2." + std::to_string(i % 255)));
        row.push_back(dolphindb::Util::parseConstant(dolphindb::DT_INT128, "0f0e0d0c0b0a09080706050403020100"));
        row.push_back(dolphindb::Util::createInt(i));
        row.push_back(dolphindb::Util::createBlob("0f0e0d0c0b0a0908070605040302010" + std::to_string(i)));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by long;");
    for (int i = 0; i < 165536; i++)
    {
        ASSERT_EQ(t1->getColumn(0)->getRow(i)->getString(), dolphindb::Util::createBool((char)(data[i % 2]))->getString());
        ASSERT_EQ(t1->getColumn(1)->getRow(i)->getString(), dolphindb::Util::createChar(i * 2)->getString());
        ASSERT_EQ(t1->getColumn(2)->getRow(i)->getString(), dolphindb::Util::createShort(i + 12)->getString());
        ASSERT_EQ(t1->getColumn(3)->getRow(i)->getString(), dolphindb::Util::createLong((long)i * 100)->getString());
        ASSERT_EQ(t1->getColumn(4)->getRow(i)->getString(), dolphindb::Util::createDate(i + 432)->getString());
        ASSERT_EQ(t1->getColumn(5)->getRow(i)->getString(), dolphindb::Util::createMonth(i + 21)->getString());
        ASSERT_EQ(t1->getColumn(6)->getRow(i)->getString(), dolphindb::Util::createDateTime(i * 192)->getString());
        ASSERT_EQ(t1->getColumn(7)->getRow(i)->getString(), dolphindb::Util::createSecond((long long)i * 1932)->getString());
        ASSERT_EQ(t1->getColumn(8)->getRow(i)->getString(), dolphindb::Util::createTimestamp((long long)i * 2342)->getString());
        ASSERT_EQ(t1->getColumn(9)->getRow(i)->getString(), dolphindb::Util::createNanoTime((long long)i * 4214)->getString());
        ASSERT_EQ(t1->getColumn(10)->getRow(i)->getString(), dolphindb::Util::createNanoTimestamp((long long)i * 4264)->getString());
        ASSERT_EQ(t1->getColumn(11)->getRow(i)->getString(), dolphindb::Util::createFloat(i * 42.64)->getString());
        ASSERT_EQ(t1->getColumn(12)->getRow(i)->getString(), dolphindb::Util::createDouble(i * 4.264)->getString());
        ASSERT_EQ(t1->getColumn(13)->getRow(i)->getString(), dolphindb::Util::createString(syms[i % 4])->getString());
        ASSERT_EQ(t1->getColumn(14)->getRow(i)->getString(), dolphindb::Util::createString(std::to_string(i))->getString());
        ASSERT_EQ(t1->getColumn(15)->getRow(i)->getString(), dolphindb::Util::parseConstant(dolphindb::DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100")->getString());
        ASSERT_EQ(t1->getColumn(16)->getRow(i)->getString(), dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.2." + std::to_string(i % 255))->getString());
        ASSERT_EQ(t1->getColumn(17)->getRow(i)->getString(), dolphindb::Util::parseConstant(dolphindb::DT_INT128, "0f0e0d0c0b0a09080706050403020100")->getString());
        ASSERT_EQ(t1->getColumn(18)->getRow(i)->getString(), dolphindb::Util::createInt(i)->getString());
        ASSERT_EQ(t1->getColumn(19)->getRow(i)->getString(), dolphindb::Util::createString("0f0e0d0c0b0a0908070605040302010" + std::to_string(i))->getString());
    }
}

TEST_F(MultithreadedTableWriterNewTest, insertMoreColumns)
{
    std::string case_=getCaseName();
    std::string script = "colName = [`id]\n"
                    "for(i in 1..400){\n"
                    "\tcolName.append!(\"factor\"+string(i))\t\t\n"
                    "}\n"
                    "colType = [SYMBOL]\n"
                    "for(i in 1..400){\n"
                    "\tcolType.append!(DOUBLE)\t\n"
                    "}\n"
                    "share table(1:0,colName, colType) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    for (int i = 0; i < 50; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &row = *prow;
        row.push_back(dolphindb::Util::createString("S" + std::to_string(i)));
        for (int j = 0; j < 400; j++)
        {
            row.push_back(dolphindb::Util::createDouble(i + j));
        }
        datas.push_back(prow);
    }
    dolphindb::MultithreadedTableWriter::Status status;
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    for (int i = 0; i < 10; i++)
    {
        mtwsp->getStatus(status);
        if (status.sentRows == 50)
        {
            dolphindb::ConstantSP t1 = pConn->run("select * from "+case_+" order by factor1;");
            ASSERT_EQ(t1->size(), 50);
            for (int j = 0; j < 50; j++)
            {
                ASSERT_EQ(t1->getColumn(0)->getString(j), dolphindb::Util::createString("S" + std::to_string(j))->getString());
                for (int m = 1; m < 401; m++)
                {
                    ASSERT_EQ(t1->getColumn(m)->getString(j), dolphindb::Util::createDouble(j + m - 1)->getString());
                }
            }
            break;
        }
        else
        {
            mtwsp->waitForThreadCompletion();
        }
    }
}

TEST_F(MultithreadedTableWriterNewTest, insertArrayVector)
{
    std::string case_=getCaseName();
    std::string script = "share streamTable(1:0,`id`name`value,[INT,SYMBOL,LONG[]]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    // arrayVector
    std::vector<int> val(10);
    for (int i = 0; i < 10; i++)
    {
        val[i] = i + 10;
    }
    int values[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    dolphindb::VectorSP array = dolphindb::Util::createVector(dolphindb::DT_LONG, 10, 10);
    array->setInt(0, 10, val.data());
    dolphindb::VectorSP pvectorAny = dolphindb::Util::createVector(dolphindb::DT_ANY, 0, 1);
    pvectorAny->append(array);
    for (int i = 0; i < 100; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &rows = *prow;
        rows.push_back(dolphindb::Util::createInt(i));
        rows.push_back(dolphindb::Util::createString("A"));
        rows.push_back(pvectorAny);
        datas.push_back(prow);
    }
    dolphindb::MultithreadedTableWriter ::Status status;
    std::vector<std::string> colvalVec;
    for (int i = 0; i < 100; ++i)
    {
        colvalVec.push_back((*(*datas[i])[2]).getString().substr(1, 31));
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    mtwsp->waitForThreadCompletion();
    mtwsp->getStatus(status);
    dolphindb::TableSP t1 = pConn->run("select * from "+case_+" order by id");
    for (int i = 0; i < 100; i++)
    {
        std::string rowvals = t1->getRow(i)->values()->getString();
        ASSERT_FALSE(rowvals.find(colvalVec[i]) == std::string::npos);
    }
}

TEST_F(MultithreadedTableWriterNewTest, insertArrayVectordiffType)
{
    std::string case_=getCaseName();
    std::string script = "colName = [`id,`name]\n"
                    "for(i in 1..17){\n"
                    "\tcolName.append!(\"factor\"+string(i))\t\n"
                    "}\n"
                    "colType =[INT,SYMBOL,BOOL[], CHAR[], SHORT[], INT[], LONG[], FLOAT[], DOUBLE[], DATE[], TIMESTAMP[], DATEHOUR[], DATETIME[], TIME[], MINUTE[], MONTH[], SECOND[], NANOTIME[], NANOTIMESTAMP[]]\n"
                    "share streamTable(1:0,colName,colType) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    // arrayVector
    std::vector<int> val(10);
    for (int i = 0; i < 10; i++)
    {
        val[i] = i + 10;
    }
    std::string names[] = {"A", "B", "C"};
    dolphindb::VectorSP boolvector = dolphindb::Util::createVector(dolphindb::DT_BOOL, 10, 10);
    dolphindb::VectorSP charvector = dolphindb::Util::createVector(dolphindb::DT_CHAR, 10, 10);
    dolphindb::VectorSP shortvector = dolphindb::Util::createVector(dolphindb::DT_SHORT, 10, 10);
    dolphindb::VectorSP intvector = dolphindb::Util::createVector(dolphindb::DT_INT, 10, 10);
    dolphindb::VectorSP longvector = dolphindb::Util::createVector(dolphindb::DT_LONG, 10, 10);
    dolphindb::VectorSP floatvector = dolphindb::Util::createVector(dolphindb::DT_FLOAT, 10, 10);
    dolphindb::VectorSP doublevector = dolphindb::Util::createVector(dolphindb::DT_DOUBLE, 10, 10);
    dolphindb::VectorSP datevector = dolphindb::Util::createVector(dolphindb::DT_DATE, 10, 10);
    dolphindb::VectorSP timestampvector = dolphindb::Util::createVector(dolphindb::DT_TIMESTAMP, 10, 10);
    dolphindb::VectorSP datehourvector = dolphindb::Util::createVector(dolphindb::DT_DATEHOUR, 10, 10);
    dolphindb::VectorSP datetimevector = dolphindb::Util::createVector(dolphindb::DT_DATETIME, 10, 10);
    dolphindb::VectorSP timevector = dolphindb::Util::createVector(dolphindb::DT_TIME, 10, 10);
    dolphindb::VectorSP minutevector = dolphindb::Util::createVector(dolphindb::DT_MINUTE, 10, 10);
    dolphindb::VectorSP monthvector = dolphindb::Util::createVector(dolphindb::DT_MONTH, 10, 10);
    dolphindb::VectorSP secondvector = dolphindb::Util::createVector(dolphindb::DT_SECOND, 10, 10);
    dolphindb::VectorSP nanotimevector = dolphindb::Util::createVector(dolphindb::DT_NANOTIME, 10, 10);
    dolphindb::VectorSP nanotimestampVector = dolphindb::Util::createVector(dolphindb::DT_NANOTIMESTAMP, 10, 10);

    boolvector->setInt(0, 10, val.data());
    charvector->setInt(0, 10, val.data());
    shortvector->setInt(0, 10, val.data());
    intvector->setInt(0, 10, val.data());
    longvector->setInt(0, 10, val.data());
    floatvector->setInt(0, 10, val.data());
    doublevector->setInt(0, 10, val.data());
    datevector->setInt(0, 10, val.data());
    timestampvector->setInt(0, 10, val.data());
    datehourvector->setInt(0, 10, val.data());
    datetimevector->setInt(0, 10, val.data());
    timevector->setInt(0, 10, val.data());
    minutevector->setInt(0, 10, val.data());
    monthvector->setInt(0, 10, val.data());
    secondvector->setInt(0, 10, val.data());
    nanotimevector->setInt(0, 10, val.data());
    nanotimestampVector->setInt(0, 10, val.data());
    std::vector<dolphindb::VectorSP> pvectorAny;
    for (int i = 0; i < 17; i++)
    {
        pvectorAny.push_back(dolphindb::Util::createVector(dolphindb::DT_ANY, 0, 1));
    }
    pvectorAny[0]->append(boolvector);
    pvectorAny[1]->append(charvector);
    pvectorAny[2]->append(shortvector);
    pvectorAny[3]->append(intvector);
    pvectorAny[4]->append(longvector);
    pvectorAny[5]->append(floatvector);
    pvectorAny[6]->append(doublevector);
    pvectorAny[7]->append(datevector);
    pvectorAny[8]->append(timestampvector);
    pvectorAny[9]->append(datehourvector);
    pvectorAny[10]->append(datetimevector);
    pvectorAny[11]->append(timevector);
    pvectorAny[12]->append(minutevector);
    pvectorAny[13]->append(monthvector);
    pvectorAny[14]->append(secondvector);
    pvectorAny[15]->append(nanotimevector);
    pvectorAny[16]->append(nanotimestampVector);
    for (int i = 0; i < 100; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &rows = *prow;
        rows.push_back(dolphindb::Util::createInt(i));
        rows.push_back(dolphindb::Util::createString(names[i % 3]));
        for (int j = 0; j < 17; j++)
        {
            rows.push_back(pvectorAny[j]);
        }
        datas.push_back(prow);
    }
    dolphindb::MultithreadedTableWriter ::Status status;
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    mtwsp->waitForThreadCompletion();
    dolphindb::TableSP t1 = pConn->run(case_);
    for (int i = 0; i < 100; i++)
    {
        for (int j = 2; j < 19; j++)
        {
            ASSERT_EQ("(" + t1->getColumn(j)->getRow(i)->getString() + ")", pvectorAny[j - 2]->getString());
        }
    }
}

TEST_F(MultithreadedTableWriterNewTest, vectorinsertdifftype)
{
    std::string case_=getCaseName();
    // int, short, char, long|| float,double
    std::string script = "share streamTable(1:0,`cint`cshort`cchar`clong`cfloat`cdouble,[INT,SHORT,CHAR,LONG,FLOAT,DOUBLE]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    mtwsp->insert(pErrorInfo, char(12), char(32), char(11), char(43), float(12.4), float(12.8));
    mtwsp->insert(pErrorInfo, short(12), short(32), short(11), short(43), float(12.4), float(12.8));
    mtwsp->insert(pErrorInfo, long(12), long(32), long(11), long(43), double(12.4), double(12.8));
    mtwsp->insert(pErrorInfo, int(12), int(32), int(11), int(43), double(12.4), double(12.8));
    mtwsp->waitForThreadCompletion();
    dolphindb::TableSP t1 = pConn->run(case_);
    ASSERT_EQ(t1->getColumn(0)->getString(), "[12,12,12,12]");
    ASSERT_EQ(t1->getColumn(1)->getString(), "[32,32,32,32]");
    ASSERT_EQ(t1->getColumn(2)->getString(), "[11,11,11,11]");
    ASSERT_EQ(t1->getColumn(3)->getString(), "[43,43,43,43]");
    ASSERT_EQ(t1->getColumn(4)->getString(), "[12.4,12.4,12.4,12.4]");
    ASSERT_EQ(t1->getColumn(5)->getString(), "[12.8,12.8,12.8,12.8]");
}

TEST_F(MultithreadedTableWriterNewTest, insertWithCompressVectorLessThanTable)
{
    std::string case_=getCaseName();
    // int, short, char, long|| float,double
    std::string script = "share streamTable(1:0,`cint`cshort`cchar`clong`cfloat`cdouble,[INT,SHORT,CHAR,LONG,FLOAT,DOUBLE]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    std::vector<dolphindb::COMPRESS_METHOD> compress = {dolphindb::COMPRESS_DELTA, dolphindb::COMPRESS_DELTA, dolphindb::COMPRESS_DELTA, dolphindb::COMPRESS_DELTA, dolphindb::COMPRESS_DELTA};
    ASSERT_ANY_THROW(config.setCompression(compress));
}

TEST_F(MultithreadedTableWriterNewTest, insertWithCompressVectorlongerThanTable)
{
    std::string case_=getCaseName();
    // int, short, char, long|| float,double
    std::string script = "share streamTable(1:0,`cint`cshort`cchar`clong`cfloat`cdouble,[INT,SHORT,CHAR,LONG,FLOAT,DOUBLE]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    std::vector<dolphindb::COMPRESS_METHOD> compress = {dolphindb::COMPRESS_DELTA, dolphindb::COMPRESS_DELTA, dolphindb::COMPRESS_DELTA, dolphindb::COMPRESS_DELTA, dolphindb::COMPRESS_DELTA, dolphindb::COMPRESS_DELTA, dolphindb::COMPRESS_DELTA};
    ASSERT_ANY_THROW(config.setCompression(compress));
}

TEST_F(MultithreadedTableWriterNewTest, insertWithCompress)
{
    std::string case_=getCaseName();
    // int, short, char, long|| float,double
    std::string script = "share streamTable(1:0,`cint`cshort`cchar`clong`cfloat`cdouble,[INT,SHORT,CHAR,LONG,FLOAT,DOUBLE]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;

    std::vector<dolphindb::COMPRESS_METHOD> compress = {dolphindb::COMPRESS_DELTA, dolphindb::COMPRESS_DELTA, dolphindb::COMPRESS_LZ4, dolphindb::COMPRESS_DELTA, dolphindb::COMPRESS_LZ4, dolphindb::COMPRESS_LZ4};
    int count = 6000000;
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    dolphindb::VectorSP cint = dolphindb::Util::createVector(dolphindb::DT_INT, count, count);
    dolphindb::VectorSP cshort = dolphindb::Util::createVector(dolphindb::DT_SHORT, count, count);
    dolphindb::VectorSP cchar = dolphindb::Util::createVector(dolphindb::DT_CHAR, count, count);
    dolphindb::VectorSP clong = dolphindb::Util::createVector(dolphindb::DT_LONG, count, count);
    dolphindb::VectorSP cfloat = dolphindb::Util::createVector(dolphindb::DT_FLOAT, count, count);
    dolphindb::VectorSP cdouble = dolphindb::Util::createVector(dolphindb::DT_DOUBLE, count, count);
    for (int i = 0; i < count; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &rows = *prow;
        rows.push_back(dolphindb::Util::createInt(i));
        cint->setInt(i, i);
        rows.push_back(dolphindb::Util::createShort(i + 1));
        cshort->setShort(i, i + 1);
        rows.push_back(dolphindb::Util::createChar(i + 2));
        cchar->setChar(i, i + 2);
        rows.push_back(dolphindb::Util::createLong(i + 3));
        clong->setLong(i, i + 3);
        rows.push_back(dolphindb::Util::createFloat(i + 4));
        cfloat->setFloat(i, i + 4);
        rows.push_back(dolphindb::Util::createDouble(i + 5));
        cdouble->setDouble(i, i + 5);
        datas.push_back(prow);
    }

    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    mtwsp->waitForThreadCompletion();
    dolphindb::TableSP t1 = pConn->run("select * from "+case_+" order by cint");
    std::vector<std::string> colName{"cint", "cshort", "cchar", "clong", "cfloat", "cdouble"};
    std::vector<dolphindb::ConstantSP> colData{cint, cshort, cchar, clong, cfloat, cdouble};
    dolphindb::TableSP exception = dolphindb::Util::createTable(colName, colData);
    ASSERT_EQ(t1->getString(), exception->getString());
}

TEST_F(MultithreadedTableWriterNewTest, inserTimetWithCompress)
{
    std::string case_=getCaseName();
    // int, short, char, long|| float,double
    std::string script = "share streamTable(1:0,`cint`cshort`cchar`clong`cfloat`cdouble,[INT,SHORT,CHAR,LONG,FLOAT,DOUBLE]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;

    std::vector<dolphindb::COMPRESS_METHOD> compress = {dolphindb::COMPRESS_DELTA, dolphindb::COMPRESS_DELTA, dolphindb::COMPRESS_LZ4, dolphindb::COMPRESS_DELTA, dolphindb::COMPRESS_LZ4, dolphindb::COMPRESS_LZ4};
    int count = 6000000;
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    dolphindb::VectorSP cint = dolphindb::Util::createVector(dolphindb::DT_INT, count, count);
    dolphindb::VectorSP cshort = dolphindb::Util::createVector(dolphindb::DT_SHORT, count, count);
    dolphindb::VectorSP cchar = dolphindb::Util::createVector(dolphindb::DT_CHAR, count, count);
    dolphindb::VectorSP clong = dolphindb::Util::createVector(dolphindb::DT_LONG, count, count);
    dolphindb::VectorSP cfloat = dolphindb::Util::createVector(dolphindb::DT_FLOAT, count, count);
    dolphindb::VectorSP cdouble = dolphindb::Util::createVector(dolphindb::DT_DOUBLE, count, count);
    for (int i = 0; i < count; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &rows = *prow;
        rows.push_back(dolphindb::Util::createInt(i));
        cint->setInt(i, i);
        rows.push_back(dolphindb::Util::createShort(i + 1));
        cshort->setShort(i, i + 1);
        rows.push_back(dolphindb::Util::createChar(i + 2));
        cchar->setChar(i, i + 2);
        rows.push_back(dolphindb::Util::createLong(i + 3));
        clong->setLong(i, i + 3);
        rows.push_back(dolphindb::Util::createFloat(i + 4));
        cfloat->setFloat(i, i + 4);
        rows.push_back(dolphindb::Util::createDouble(i + 5));
        cdouble->setDouble(i, i + 5);
        datas.push_back(prow);
    }

    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    mtwsp->waitForThreadCompletion();
    dolphindb::TableSP t1 = pConn->run("select * from "+case_+" order by cint");
    std::vector<std::string> colName{"cint", "cshort", "cchar", "clong", "cfloat", "cdouble"};
    std::vector<dolphindb::ConstantSP> colData{cint, cshort, cchar, clong, cfloat, cdouble};
    dolphindb::TableSP exception = dolphindb::Util::createTable(colName, colData);
    ASSERT_EQ(t1->getString(), exception->getString());
}

TEST_F(MultithreadedTableWriterNewTest, insertArrayDiffTypeWithCompress)
{
    std::string case_=getCaseName();
    std::string script = "colName = [`id,`name]\n"
                    "for(i in 1..17){\n"
                    "\tcolName.append!(\"factor\"+string(i))\t\n"
                    "}\n"
                    "colType =[INT,SYMBOL,BOOL[], CHAR[], SHORT[], INT[], LONG[], FLOAT[], DOUBLE[], DATE[], TIMESTAMP[], DATEHOUR[], DATETIME[], TIME[], MINUTE[], MONTH[], SECOND[], NANOTIME[], NANOTIMESTAMP[]]\n"
                    "share streamTable(1:0,colName,colType) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    std::vector<dolphindb::COMPRESS_METHOD> typeVec(19);
    typeVec[0] = dolphindb::COMPRESS_DELTA;
    typeVec[1] = dolphindb::COMPRESS_DELTA;
    for (int i = 0; i < 17; i++)
    {
        typeVec[i + 2] = dolphindb::COMPRESS_LZ4;
    }
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    // arrayVector
    std::vector<int> val(10);
    for (int i = 0; i < 10; i++)
    {
        val[i] = i + 10;
    }
    std::string names[] = {"A", "B", "C"};
    dolphindb::VectorSP boolvector = dolphindb::Util::createVector(dolphindb::DT_BOOL, 10, 10);
    dolphindb::VectorSP charvector = dolphindb::Util::createVector(dolphindb::DT_CHAR, 10, 10);
    dolphindb::VectorSP shortvector = dolphindb::Util::createVector(dolphindb::DT_SHORT, 10, 10);
    dolphindb::VectorSP intvector = dolphindb::Util::createVector(dolphindb::DT_INT, 10, 10);
    dolphindb::VectorSP longvector = dolphindb::Util::createVector(dolphindb::DT_LONG, 10, 10);
    dolphindb::VectorSP floatvector = dolphindb::Util::createVector(dolphindb::DT_FLOAT, 10, 10);
    dolphindb::VectorSP doublevector = dolphindb::Util::createVector(dolphindb::DT_DOUBLE, 10, 10);
    dolphindb::VectorSP datevector = dolphindb::Util::createVector(dolphindb::DT_DATE, 10, 10);
    dolphindb::VectorSP timestampvector = dolphindb::Util::createVector(dolphindb::DT_TIMESTAMP, 10, 10);
    dolphindb::VectorSP datehourvector = dolphindb::Util::createVector(dolphindb::DT_DATEHOUR, 10, 10);
    dolphindb::VectorSP datetimevector = dolphindb::Util::createVector(dolphindb::DT_DATETIME, 10, 10);
    dolphindb::VectorSP timevector = dolphindb::Util::createVector(dolphindb::DT_TIME, 10, 10);
    dolphindb::VectorSP minutevector = dolphindb::Util::createVector(dolphindb::DT_MINUTE, 10, 10);
    dolphindb::VectorSP monthvector = dolphindb::Util::createVector(dolphindb::DT_MONTH, 10, 10);
    dolphindb::VectorSP secondvector = dolphindb::Util::createVector(dolphindb::DT_SECOND, 10, 10);
    dolphindb::VectorSP nanotimevector = dolphindb::Util::createVector(dolphindb::DT_NANOTIME, 10, 10);
    dolphindb::VectorSP nanotimestampVector = dolphindb::Util::createVector(dolphindb::DT_NANOTIMESTAMP, 10, 10);

    boolvector->setInt(0, 10, val.data());
    charvector->setInt(0, 10, val.data());
    shortvector->setInt(0, 10, val.data());
    intvector->setInt(0, 10, val.data());
    longvector->setInt(0, 10, val.data());
    floatvector->setInt(0, 10, val.data());
    doublevector->setInt(0, 10, val.data());
    datevector->setInt(0, 10, val.data());
    timestampvector->setInt(0, 10, val.data());
    datehourvector->setInt(0, 10, val.data());
    datetimevector->setInt(0, 10, val.data());
    timevector->setInt(0, 10, val.data());
    minutevector->setInt(0, 10, val.data());
    monthvector->setInt(0, 10, val.data());
    secondvector->setInt(0, 10, val.data());
    nanotimevector->setInt(0, 10, val.data());
    nanotimestampVector->setInt(0, 10, val.data());
    std::vector<dolphindb::VectorSP> pvectorAny;
    for (int i = 0; i < 17; i++)
    {
        pvectorAny.push_back(dolphindb::Util::createVector(dolphindb::DT_ANY, 0, 1));
    }

    pvectorAny[0]->append(boolvector);
    pvectorAny[1]->append(charvector);
    pvectorAny[2]->append(shortvector);
    pvectorAny[3]->append(intvector);
    pvectorAny[4]->append(longvector);
    pvectorAny[5]->append(floatvector);
    pvectorAny[6]->append(doublevector);
    pvectorAny[7]->append(datevector);
    pvectorAny[8]->append(timestampvector);
    pvectorAny[9]->append(datehourvector);
    pvectorAny[10]->append(datetimevector);
    pvectorAny[11]->append(timevector);
    pvectorAny[12]->append(minutevector);
    pvectorAny[13]->append(monthvector);
    pvectorAny[14]->append(secondvector);
    pvectorAny[15]->append(nanotimevector);
    pvectorAny[16]->append(nanotimestampVector);
    for (int i = 0; i < 100; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &rows = *prow;
        rows.push_back(dolphindb::Util::createInt(i));
        rows.push_back(dolphindb::Util::createString(names[i % 3]));
        for (int j = 0; j < 17; j++)
        {
            rows.push_back(pvectorAny[j]);
        }
        datas.push_back(prow);
    }
    dolphindb::MultithreadedTableWriter ::Status status;
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    mtwsp->waitForThreadCompletion();
    dolphindb::TableSP t1 = pConn->run("select * from "+case_+" order by id");
    for (int i = 0; i < 100; i++)
    {
        for (int j = 2; j < 19; j++)
        {
            ASSERT_EQ("(" + t1->getColumn(j)->getRow(i)->getString() + ")", pvectorAny[j - 2]->getString());
        }
    }
}

TEST_F(MultithreadedTableWriterNewTest, TimeTypedata)
{
    std::string case_=getCaseName();
    std::string script = "share streamTable(1:0, `cint`csym`cdate`cdatetime`cdatehour`cmonth`ctime`ctimestamp`cminute`second`nanotime`nanotimestamp,"
                    "[INT,SYMBOL,DATE,DATETIME,DATEHOUR,MONTH,TIME,TIMESTAMP,MINUTE,SECOND,NANOTIME,NANOTIMESTAMP]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    bool flag2 = mtwsp->insert(pErrorInfo, 43, "A", long(421), 421, 421, 421, long(421), long(421), 421, long(421), long(421), long(432));
    std::cout << flag2 << std::endl;
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run(case_);
}

TEST_F(MultithreadedTableWriterNewTest, PartitionSchemeDatehour)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                                             "if(exists(dbName)){\n"
                                             "\tdropDatabase(dbName)\t\n"
                                             "}\n"
                                             "db  = database(dbName, VALUE,datehour(1..100));\n"
                                             "t = table(1000:0, `sym`id`data,[SYMBOL, INT, DATETIME])\n"
                                             "pt = db.createPartitionedTable(t, `pt, `data)\n"
                                             "t2 = table(1000:0, `sym`id`data,[SYMBOL, INT, TIMESTAMP])\n"
                                             "pt2= db.createPartitionedTable(t2, `pt2, `data)\n"
                                             "t3 = table(1000:0, `sym`id`data,[SYMBOL, INT, nanotimestamp])\n"
                                             "pt3= db.createPartitionedTable(t3, `pt3, `data)\n";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, "loadTable('"+dbName+"',`pt)");
    dolphindb::MTWConfig config2(pConn, "loadTable('"+dbName+"',`pt2)");
    dolphindb::MTWConfig config3(pConn, "loadTable('"+dbName+"',`pt3)");
    config.setThreads(3, "data");
    config2.setThreads(3, "data");
    config3.setThreads(3, "data");
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp2 = new dolphindb::MultithreadedTableWriter(config2);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp3 = new dolphindb::MultithreadedTableWriter(config3);
    dolphindb::ErrorCodeInfo pErrorInfo;
    // datatime
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    for (int i = 0; i < 100; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &rows = *prow;
        rows.push_back(dolphindb::Util::createString("A"));
        rows.push_back(dolphindb::Util::createInt(i));
        rows.push_back(dolphindb::Util::createDateTime(i));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    mtwsp->waitForThreadCompletion();
    dolphindb::TableSP re = pConn->run("select * from loadTable(dbName,`pt)\n");
    for (int i = 0; i < 100; i++)
    {
        ASSERT_EQ(re->getColumn(1)->getRow(i)->getString(), dolphindb::Util::createInt(i)->getString());
        ASSERT_EQ(re->getColumn(0)->getRow(i)->getString(), "A");
        ASSERT_EQ(re->getColumn(2)->getRow(i)->getString(), dolphindb::Util::createDateTime(i)->getString());
    }

    // timestamp
    std::vector<std::vector<dolphindb::ConstantSP> *> datas2;
    for (int i = 0; i < 100; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &rows = *prow;
        rows.push_back(dolphindb::Util::createString("A"));
        rows.push_back(dolphindb::Util::createInt(i));
        rows.push_back(dolphindb::Util::createTimestamp(i));
        datas2.push_back(prow);
    }
    mtwsp2->insertUnwrittenData(datas2, pErrorInfo);
    mtwsp2->waitForThreadCompletion();
    dolphindb::TableSP re2 = pConn->run("select * from loadTable(dbName,`pt2)\n");
    for (int i = 0; i < 100; i++)
    {
        ASSERT_EQ(re2->getColumn(1)->getRow(i)->getString(), dolphindb::Util::createInt(i)->getString());
        ASSERT_EQ(re2->getColumn(0)->getRow(i)->getString(), "A");
        ASSERT_EQ(re2->getColumn(2)->getRow(i)->getString(), dolphindb::Util::createTimestamp(i)->getString());
    }

    // nanotimestamp
    std::vector<std::vector<dolphindb::ConstantSP> *> datas3;
    for (int i = 0; i < 100; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &rows = *prow;
        rows.push_back(dolphindb::Util::createString("A"));
        rows.push_back(dolphindb::Util::createInt(i));
        rows.push_back(dolphindb::Util::createNanoTimestamp(i));
        datas3.push_back(prow);
    }
    mtwsp3->insertUnwrittenData(datas3, pErrorInfo);
    mtwsp3->waitForThreadCompletion();
    dolphindb::TableSP re3 = pConn->run("select * from loadTable(dbName,`pt3)\n");
    for (int i = 0; i < 100; i++)
    {
        ASSERT_EQ(re3->getColumn(1)->getRow(i)->getString(), dolphindb::Util::createInt(i)->getString());
        ASSERT_EQ(re3->getColumn(0)->getRow(i)->getString(), "A");
        ASSERT_EQ(re3->getColumn(2)->getRow(i)->getString(), dolphindb::Util::createNanoTimestamp(i)->getString());
    }
}

TEST_F(MultithreadedTableWriterNewTest, PartitionSchemeDate)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                                             "if(exists(dbName)){\n"
                                             "\tdropDatabase(dbName)\t\n"
                                             "}\n"
                                             "db  = database(dbName, VALUE,date(1..100));\n"
                                             "t = table(1000:0, `sym`id`data,[SYMBOL, INT, DATETIME])\n"
                                             "pt = db.createPartitionedTable(t, `pt, `data)\n"
                                             "t2 = table(1000:0, `sym`id`data,[SYMBOL, INT, TIMESTAMP])\n"
                                             "pt2= db.createPartitionedTable(t2, `pt2, `data)\n"
                                             "t3 = table(1000:0, `sym`id`data,[SYMBOL, INT, nanotimestamp])\n"
                                             "pt3= db.createPartitionedTable(t3, `pt3, `data)\n";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, "loadTable('"+dbName+"',`pt)");
    dolphindb::MTWConfig config2(pConn, "loadTable('"+dbName+"',`pt2)");
    dolphindb::MTWConfig config3(pConn, "loadTable('"+dbName+"',`pt3)");
    config.setThreads(3, "data");
    config2.setThreads(3, "data");
    config3.setThreads(3, "data");
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp2 = new dolphindb::MultithreadedTableWriter(config2);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp3 = new dolphindb::MultithreadedTableWriter(config3);
    dolphindb::ErrorCodeInfo pErrorInfo;
    // datatime
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    for (int i = 0; i < 100; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &rows = *prow;
        rows.push_back(dolphindb::Util::createString("A"));
        rows.push_back(dolphindb::Util::createInt(i));
        rows.push_back(dolphindb::Util::createDateTime(i));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    mtwsp->waitForThreadCompletion();
    dolphindb::TableSP re = pConn->run("select * from loadTable(dbName,`pt)\n");
    for (int i = 0; i < 100; i++)
    {
        ASSERT_EQ(re->getColumn(1)->getRow(i)->getString(), dolphindb::Util::createInt(i)->getString());
        ASSERT_EQ(re->getColumn(0)->getRow(i)->getString(), "A");
        ASSERT_EQ(re->getColumn(2)->getRow(i)->getString(), dolphindb::Util::createDateTime(i)->getString());
    }

    // timestamp
    std::vector<std::vector<dolphindb::ConstantSP> *> datas2;
    for (int i = 0; i < 100; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &rows = *prow;
        rows.push_back(dolphindb::Util::createString("A"));
        rows.push_back(dolphindb::Util::createInt(i));
        rows.push_back(dolphindb::Util::createTimestamp(i));
        datas2.push_back(prow);
    }
    mtwsp2->insertUnwrittenData(datas2, pErrorInfo);
    mtwsp2->waitForThreadCompletion();
    dolphindb::TableSP re2 = pConn->run("select * from loadTable(dbName,`pt2)\n");
    for (int i = 0; i < 100; i++)
    {
        ASSERT_EQ(re2->getColumn(1)->getRow(i)->getString(), dolphindb::Util::createInt(i)->getString());
        ASSERT_EQ(re2->getColumn(0)->getRow(i)->getString(), "A");
        ASSERT_EQ(re2->getColumn(2)->getRow(i)->getString(), dolphindb::Util::createTimestamp(i)->getString());
    }

    // nanotimestamp
    std::vector<std::vector<dolphindb::ConstantSP> *> datas3;
    for (int i = 0; i < 100; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &rows = *prow;
        rows.push_back(dolphindb::Util::createString("A"));
        rows.push_back(dolphindb::Util::createInt(i));
        rows.push_back(dolphindb::Util::createNanoTimestamp(i));
        datas3.push_back(prow);
    }
    mtwsp3->insertUnwrittenData(datas3, pErrorInfo);
    mtwsp3->waitForThreadCompletion();
    dolphindb::TableSP re3 = pConn->run("select * from loadTable(dbName,`pt3)\n");
    for (int i = 0; i < 100; i++)
    {
        ASSERT_EQ(re3->getColumn(1)->getRow(i)->getString(), dolphindb::Util::createInt(i)->getString());
        ASSERT_EQ(re3->getColumn(0)->getRow(i)->getString(), "A");
        ASSERT_EQ(re3->getColumn(2)->getRow(i)->getString(), dolphindb::Util::createNanoTimestamp(i)->getString());
    }
}

TEST_F(MultithreadedTableWriterNewTest, PartitionSchemeMonth)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                                             "if(exists(dbName)){\n"
                                             "\tdropDatabase(dbName)\t\n"
                                             "}\n"
                                             "db  = database(dbName, VALUE,month(1..100));\n"
                                             "t = table(1000:0, `sym`id`data,[SYMBOL, INT, DATETIME])\n"
                                             "pt = db.createPartitionedTable(t, `pt, `data)\n"
                                             "t2 = table(1000:0, `sym`id`data,[SYMBOL, INT, TIMESTAMP])\n"
                                             "pt2= db.createPartitionedTable(t2, `pt2, `data)\n"
                                             "t3 = table(1000:0, `sym`id`data,[SYMBOL, INT, nanotimestamp])\n"
                                             "pt3= db.createPartitionedTable(t3, `pt3, `data)\n";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, "loadTable('"+dbName+"',`pt)");
    dolphindb::MTWConfig config2(pConn, "loadTable('"+dbName+"',`pt2)");
    dolphindb::MTWConfig config3(pConn, "loadTable('"+dbName+"',`pt3)");
    config.setThreads(3, "data");
    config2.setThreads(3, "data");
    config3.setThreads(3, "data");
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp2 = new dolphindb::MultithreadedTableWriter(config2);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp3 = new dolphindb::MultithreadedTableWriter(config3);
    dolphindb::ErrorCodeInfo pErrorInfo;
    // datatime
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    for (int i = 0; i < 100; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &rows = *prow;
        rows.push_back(dolphindb::Util::createString("A"));
        rows.push_back(dolphindb::Util::createInt(i));
        rows.push_back(dolphindb::Util::createDateTime(i));
        datas.push_back(prow);
    }
    mtwsp->insertUnwrittenData(datas, pErrorInfo);
    mtwsp->waitForThreadCompletion();
    dolphindb::TableSP re = pConn->run("select * from loadTable(dbName,`pt)\n");
    for (int i = 0; i < 100; i++)
    {
        ASSERT_EQ(re->getColumn(1)->getRow(i)->getString(), dolphindb::Util::createInt(i)->getString());
        ASSERT_EQ(re->getColumn(0)->getRow(i)->getString(), "A");
        ASSERT_EQ(re->getColumn(2)->getRow(i)->getString(), dolphindb::Util::createDateTime(i)->getString());
    }

    // timestamp
    std::vector<std::vector<dolphindb::ConstantSP> *> datas2;
    for (int i = 0; i < 100; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &rows = *prow;
        rows.push_back(dolphindb::Util::createString("A"));
        rows.push_back(dolphindb::Util::createInt(i));
        rows.push_back(dolphindb::Util::createTimestamp(i));
        datas2.push_back(prow);
    }
    mtwsp2->insertUnwrittenData(datas2, pErrorInfo);
    mtwsp2->waitForThreadCompletion();
    dolphindb::TableSP re2 = pConn->run("select * from loadTable(dbName,`pt2)\n");
    for (int i = 0; i < 100; i++)
    {
        ASSERT_EQ(re2->getColumn(1)->getRow(i)->getString(), dolphindb::Util::createInt(i)->getString());
        ASSERT_EQ(re2->getColumn(0)->getRow(i)->getString(), "A");
        ASSERT_EQ(re2->getColumn(2)->getRow(i)->getString(), dolphindb::Util::createTimestamp(i)->getString());
    }

    // nanotimestamp
    std::vector<std::vector<dolphindb::ConstantSP> *> datas3;
    for (int i = 0; i < 100; i++)
    {
        std::vector<dolphindb::ConstantSP> *prow = new std::vector<dolphindb::ConstantSP>;
        std::vector<dolphindb::ConstantSP> &rows = *prow;
        rows.push_back(dolphindb::Util::createString("A"));
        rows.push_back(dolphindb::Util::createInt(i));
        rows.push_back(dolphindb::Util::createNanoTimestamp(i));
        datas3.push_back(prow);
    }
    mtwsp3->insertUnwrittenData(datas3, pErrorInfo);
    mtwsp3->waitForThreadCompletion();
    dolphindb::TableSP re3 = pConn->run("select * from loadTable(dbName,`pt3)\n");
    for (int i = 0; i < 100; i++)
    {
        ASSERT_EQ(re3->getColumn(1)->getRow(i)->getString(), dolphindb::Util::createInt(i)->getString());
        ASSERT_EQ(re3->getColumn(0)->getRow(i)->getString(), "A");
        ASSERT_EQ(re3->getColumn(2)->getRow(i)->getString(), dolphindb::Util::createNanoTimestamp(i)->getString());
    }
}

TEST_F(MultithreadedTableWriterNewTest, basicDateData)
{
    std::string case_=getCaseName();
    std::string script = "share streamTable(1:0, `cint`csym`cdouble,[INT,SYMBOL,DOUBLE]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    bool flag2 = mtwsp->insert(pErrorInfo, 12, nullptr, 23.5);
    std::cout << flag2 << std::endl;
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run(case_);
    std::cout << t1->getString();
    dolphindb::MultithreadedTableWriter::Status status;
    mtwsp->getStatus(status);
}

TEST_F(MultithreadedTableWriterNewTest, upsertToPartitionTableRangeType)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string tableName = "pt";
    std::string script1;
    script1 += "dbName = \"" + dbName + "\"\n";
    script1 += "tableName=\"" + tableName + "\"\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db  = database(dbName, RANGE,0 5000 15000);\n";
    script1 += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
               "tableInsert(t,[`asd,0,10]);"
               "pt = db.createPartitionedTable(t,tableName,`id);"
               "tableInsert(pt,t)";
    pConn->run(script1);

    dolphindb::MTWConfig config(pConn, "loadTable('"+dbName+"', '"+tableName+"')");
    std::vector<std::string> modeOption = {"ignoreNull=false", "keyColNames=`id"};
    config.setThreads(1, "id").setWriteMode(dolphindb::WriteMode::Upsert, modeOption).setBatching(1000, std::chrono::seconds(1));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwspr = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;

    int colNum = 3, rowNum = 1000;
    std::vector<std::string> colNames = {"symbol", "id", "value"};
    std::vector<dolphindb::DATA_TYPE> colTypes = {dolphindb::DT_SYMBOL, dolphindb::DT_INT, dolphindb::DT_INT};

    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    std::string sym[] = {"A", "B", "C", "D"};

    std::vector<dolphindb::ConstantSP> *prow0 = new std::vector<dolphindb::ConstantSP>({dolphindb::Util::createString(sym[rand() % 4]), dolphindb::Util::createInt(0), dolphindb::Util::createNullConstant(dolphindb::DT_INT)});
    datas.emplace_back(prow0);

    for (int i = 1; i < rowNum; i++)
    {
        std::vector<dolphindb::ConstantSP> *prows = new std::vector<dolphindb::ConstantSP>();
        std::vector<dolphindb::ConstantSP> &rows = *prows;
        rows.emplace_back(dolphindb::Util::createString(sym[rand() % 4]));
        rows.emplace_back(dolphindb::Util::createInt(i));
        rows.emplace_back(dolphindb::Util::createInt((int)(rand() % 1000)));
        datas.emplace_back(prows);
    }
    std::vector<std::vector<std::string>> datasVec;
    for (int i = 0; i < rowNum; i++)
    {
        std::vector<std::string> oneRow;
        for (int j = 0; j < colNum; j++)
        {
            oneRow.push_back(datas[i][0][j]->getString());
        }
        datasVec.push_back(oneRow);
    }
    bool s = mtwspr->insertUnwrittenData(datas, pErrorInfo);
    mtwspr->waitForThreadCompletion();

    dolphindb::MultithreadedTableWriter::Status status;
    mtwspr->getStatus(status);
    std::cout << status.errorInfo << std::endl;

    dolphindb::TableSP res = pConn->run("select * from pt;");
    std::cout << res->getString() << std::endl;
    ASSERT_EQ((res->getColumnType(0) == 18 || res->getColumnType(0) == 17), true);
    ASSERT_EQ(res->getColumnType(1), 4);
    ASSERT_EQ(res->getColumnType(2), 4);

    for (int i = 0; i < rowNum; i++)
    {
        for (int j = 0; j < colNum; j++)
        {
            ASSERT_EQ(res->getColumn(j)->getRow(i)->getString(), datasVec[i][j]);
        }
    }
}

TEST_F(MultithreadedTableWriterNewTest, upsertToPartitionTableRangeTypeWithsortColumns)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string tableName = "pt";
    std::string script1;
    script1 += "dbName = \"" + dbName + "\"\n";
    script1 += "tableName=\"" + tableName + "\"\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db  = database(dbName, RANGE,0 5000 15000);\n";
    script1 += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
               "pt = db.createPartitionedTable(t,tableName,`id);";
    pConn->run(script1);

    dolphindb::MTWConfig config(pConn, "loadTable('"+dbName+"', '"+tableName+"')");
    std::vector<std::string> modeOption = {"ignoreNull=false", "keyColNames=`id", "sortColumns=`value"};
    config.setBatching(1000, std::chrono::milliseconds(100)).setThreads(1, "id").setWriteMode(dolphindb::WriteMode::Upsert, modeOption);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwspr = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;

    int colNum = 3, rowNum = 1000;
    std::vector<std::string> colNames = {"symbol", "id", "value"};
    std::vector<dolphindb::DATA_TYPE> colTypes = {dolphindb::DT_SYMBOL, dolphindb::DT_INT, dolphindb::DT_INT};
    std::vector<std::vector<dolphindb::ConstantSP> *> datas;

    dolphindb::TableSP tmp1 = dolphindb::Util::createTable(colNames, colTypes, rowNum, rowNum);
    std::vector<dolphindb::VectorSP> columnVecs;
    columnVecs.reserve(colNum);
    for (int i = 0; i < colNum; i++)
    {
        columnVecs.emplace_back(tmp1->getColumn(i));
    }
    std::string sym[] = {"A", "B", "C", "D"};
    columnVecs[0]->set(0, dolphindb::Util::createString("D"));
    columnVecs[1]->set(0, dolphindb::Util::createInt(0));
    columnVecs[2]->setInt(1000);
    for (int i = 1; i < rowNum; i++)
    {
        columnVecs[0]->set(i, dolphindb::Util::createString(sym[rand() % 4]));
        columnVecs[1]->set(i, dolphindb::Util::createInt(0));
        columnVecs[2]->set(i, dolphindb::Util::createInt((int)(rowNum - i)));
    }
    pConn->upload("tmp1", {tmp1});
    pConn->run("tableInsert(pt,tmp1)");
    dolphindb::Util::sleep(2000);

    std::vector<dolphindb::ConstantSP> *prow0 = new std::vector<dolphindb::ConstantSP>({dolphindb::Util::createString("ccc"), dolphindb::Util::createInt(0), dolphindb::Util::createInt(0)});
    datas.emplace_back(prow0);
    bool s = mtwspr->insertUnwrittenData(datas, pErrorInfo);
    mtwspr->waitForThreadCompletion();

    dolphindb::MultithreadedTableWriter::Status status;
    mtwspr->getStatus(status);
    std::cout << status.errorInfo << std::endl;

    dolphindb::TableSP res = pConn->run("select * from pt;");
    std::cout << res->getString() << std::endl;
    ASSERT_EQ((res->getColumnType(0) == 18 || res->getColumnType(0) == 17), true);
    ASSERT_EQ(res->getColumnType(1), 4);
    ASSERT_EQ(res->getColumnType(2), 4);

    for (int i = 1; i < rowNum; i++)
    {
        ASSERT_EQ((res->getColumn(2)->getRow(i)->getInt() > res->getColumn(2)->getRow(i - 1)->getInt()), true);
    }
}

TEST_F(MultithreadedTableWriterNewTest, upsertToPartitionTableHashType)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string tableName = "pt";
    std::string script1;
    script1 += "dbName = \"" + dbName + "\"\n";
    script1 += "tableName=\"" + tableName + "\"\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db  = database(dbName, HASH,[INT, 1]);\n";
    script1 += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
               "tableInsert(t,[`asd,0,10]);"
               "pt = db.createPartitionedTable(t,tableName,`id);"
               "tableInsert(pt,t)";
    // std::cout<<script1<<std::endl;
    pConn->run(script1);

    dolphindb::MTWConfig config(pConn, "loadTable('"+dbName+"', '"+tableName+"')");
    std::vector<std::string> modeOption = {"ignoreNull=false", "keyColNames=`id"};
    config.setThreads(1, "id").setWriteMode(dolphindb::WriteMode::Upsert, modeOption).setBatching(1000, std::chrono::seconds(1));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwspr = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;

    int colNum = 3, rowNum = 1000;
    std::vector<std::string> colNames = {"symbol", "id", "value"};
    std::vector<dolphindb::DATA_TYPE> colTypes = {dolphindb::DT_SYMBOL, dolphindb::DT_INT, dolphindb::DT_INT};

    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    std::string sym[] = {"A", "B", "C", "D"};

    std::vector<dolphindb::ConstantSP> *prow0 = new std::vector<dolphindb::ConstantSP>({dolphindb::Util::createString(sym[rand() % 4]), dolphindb::Util::createInt(0), dolphindb::Util::createNullConstant(dolphindb::DT_INT)});
    datas.emplace_back(prow0);

    for (int i = 1; i < rowNum; i++)
    {
        std::vector<dolphindb::ConstantSP> *prows = new std::vector<dolphindb::ConstantSP>();
        std::vector<dolphindb::ConstantSP> &rows = *prows;
        rows.emplace_back(dolphindb::Util::createString(sym[rand() % 4]));
        rows.emplace_back(dolphindb::Util::createInt(i));
        rows.emplace_back(dolphindb::Util::createInt((int)(rand() % 1000)));
        datas.emplace_back(prows);
    }

    std::vector<std::vector<std::string>> datasVec;
    for (int i = 0; i < rowNum; i++)
    {
        std::vector<std::string> oneRow;
        for (int j = 0; j < colNum; j++)
        {
            oneRow.push_back(datas[i][0][j]->getString());
        }
        datasVec.push_back(oneRow);
    }

    bool s = mtwspr->insertUnwrittenData(datas, pErrorInfo);
    mtwspr->waitForThreadCompletion();

    dolphindb::MultithreadedTableWriter::Status status;
    mtwspr->getStatus(status);
    std::cout << status.errorInfo << std::endl;

    dolphindb::TableSP res = pConn->run("select * from pt;");
    std::cout << res->getString() << std::endl;
    ASSERT_EQ((res->getColumnType(0) == 18 || res->getColumnType(0) == 17), true);
    ASSERT_EQ(res->getColumnType(1), 4);
    ASSERT_EQ(res->getColumnType(2), 4);
    for (int i = 0; i < rowNum; i++)
    {
        for (int j = 0; j < colNum; j++)
        {
            ASSERT_EQ(res->getColumn(j)->getRow(i)->getString(), datasVec[i][j]);
        }
    }
}

TEST_F(MultithreadedTableWriterNewTest, upsertToPartitionTableValueType)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string tableName = "pt";
    std::string script1;
    script1 += "dbName = \"" + dbName + "\"\n";
    script1 += "tableName=\"" + tableName + "\"\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db  = database(dbName, VALUE,0..1000);\n";
    script1 += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
               "tableInsert(t,[`asd,0,10]);"
               "pt = db.createPartitionedTable(t,tableName,`id);"
               "tableInsert(pt,t)";
    pConn->run(script1);

    dolphindb::MTWConfig config(pConn, "loadTable('"+dbName+"', '"+tableName+"')");
    std::vector<std::string> modeOption = {"ignoreNull=false", "keyColNames=`id"};
    config.setThreads(1, "id").setWriteMode(dolphindb::WriteMode::Upsert, modeOption).setBatching(1000, std::chrono::seconds(1));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwspr = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;

    int colNum = 3, rowNum = 1000;
    std::vector<std::string> colNames = {"symbol", "id", "value"};
    std::vector<dolphindb::DATA_TYPE> colTypes = {dolphindb::DT_SYMBOL, dolphindb::DT_INT, dolphindb::DT_INT};

    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    std::string sym[] = {"A", "B", "C", "D"};

    std::vector<dolphindb::ConstantSP> *prow0 = new std::vector<dolphindb::ConstantSP>({dolphindb::Util::createString(sym[rand() % 4]), dolphindb::Util::createInt(0), dolphindb::Util::createNullConstant(dolphindb::DT_INT)});
    datas.emplace_back(prow0);

    for (int i = 1; i < rowNum; i++)
    {
        std::vector<dolphindb::ConstantSP> *prows = new std::vector<dolphindb::ConstantSP>();
        std::vector<dolphindb::ConstantSP> &rows = *prows;
        rows.emplace_back(dolphindb::Util::createString(sym[rand() % 4]));
        rows.emplace_back(dolphindb::Util::createInt(i));
        rows.emplace_back(dolphindb::Util::createInt((int)(rand() % 1000)));
        datas.emplace_back(prows);
    }

    std::vector<std::vector<std::string>> datasVec;
    for (int i = 0; i < rowNum; i++)
    {
        std::vector<std::string> oneRow;
        for (int j = 0; j < colNum; j++)
        {
            oneRow.push_back(datas[i][0][j]->getString());
        }
        datasVec.push_back(oneRow);
    }

    bool s = mtwspr->insertUnwrittenData(datas, pErrorInfo);
    mtwspr->waitForThreadCompletion();

    dolphindb::MultithreadedTableWriter::Status status;
    mtwspr->getStatus(status);
    std::cout << status.errorInfo << std::endl;

    dolphindb::TableSP res = pConn->run("select * from pt;");
    std::cout << res->getString() << std::endl;
    ASSERT_EQ((res->getColumnType(0) == 18 || res->getColumnType(0) == 17), true);
    ASSERT_EQ(res->getColumnType(1), 4);
    ASSERT_EQ(res->getColumnType(2), 4);

    for (int i = 0; i < rowNum; i++)
    {
        for (int j = 0; j < colNum; j++)
        {
            ASSERT_EQ(res->getColumn(j)->getRow(i)->getString(), datasVec[i][j]);
        }
    }
}

TEST_F(MultithreadedTableWriterNewTest, upsertToPartitionTableListType)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string tableName = "pt";
    std::string script1;
    script1 += "dbName = \"" + dbName + "\"\n";
    script1 += "tableName=\"" + tableName + "\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db  = database(dbName, LIST,[`A`B`C`D]);\n";
    script1 += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
               "tableInsert(t,[`asd,0,10]);"
               "pt = db.createPartitionedTable(t,tableName,`symbol);"
               "tableInsert(pt,t)";
    pConn->run(script1);

    dolphindb::MTWConfig config(pConn, "loadTable('"+dbName+"', '"+tableName+"')");
    std::vector<std::string> modeOption = {"ignoreNull=false", "keyColNames=`symbol"};
    config.setThreads(1, "symbol").setWriteMode(dolphindb::WriteMode::Upsert, modeOption).setBatching(1000, std::chrono::seconds(1));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwspr = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;

    int colNum = 3, rowNum = 1000;
    std::vector<std::string> colNames = {"symbol", "id", "value"};
    std::vector<dolphindb::DATA_TYPE> colTypes = {dolphindb::DT_SYMBOL, dolphindb::DT_INT, dolphindb::DT_INT};

    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    std::string sym[] = {"A", "B", "C", "D"};

    std::vector<dolphindb::ConstantSP> *prow0 = new std::vector<dolphindb::ConstantSP>({dolphindb::Util::createString(sym[rand() % 4]), dolphindb::Util::createInt(0), dolphindb::Util::createNullConstant(dolphindb::DT_INT)});
    datas.emplace_back(prow0);

    for (int i = 1; i < rowNum; i++)
    {
        std::vector<dolphindb::ConstantSP> *prows = new std::vector<dolphindb::ConstantSP>();
        std::vector<dolphindb::ConstantSP> &rows = *prows;
        rows.emplace_back(dolphindb::Util::createString(sym[rand() % 4]));
        rows.emplace_back(dolphindb::Util::createInt(i));
        rows.emplace_back(dolphindb::Util::createInt((int)(rand() % 1000)));
        datas.emplace_back(prows);
    }

    std::vector<std::vector<std::string>> datasVec;
    for (int i = 0; i < rowNum; i++)
    {
        std::vector<std::string> oneRow;
        for (int j = 0; j < colNum; j++)
        {
            oneRow.push_back(datas[i][0][j]->getString());
        }
        datasVec.push_back(oneRow);
    }

    bool s = mtwspr->insertUnwrittenData(datas, pErrorInfo);
    mtwspr->waitForThreadCompletion();

    dolphindb::MultithreadedTableWriter::Status status;
    mtwspr->getStatus(status);
    std::cout << status.errorInfo << std::endl;

    dolphindb::TableSP res = pConn->run("select * from pt;");
    std::cout << res->getString() << std::endl;

    ASSERT_EQ((res->getColumnType(0) == 18 || res->getColumnType(0) == 17), true);
    ASSERT_EQ(res->getColumnType(1), 4);
    ASSERT_EQ(res->getColumnType(2), 4);

    for (int i = 0; i < rowNum; i++)
    {
        for (int j = 0; j < colNum; j++)
        {
            ASSERT_EQ(res->getColumn(j)->getRow(i)->getString(), datasVec[i][j]);
        }
    }
}

TEST_F(MultithreadedTableWriterNewTest, upsertToPartitionTableRangeTypeIgnoreNull)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script1;
    std::string tableName = "pt";
    script1 += "dbName = \"" + dbName + "\"\n";
    script1 += "tableName=\"" + tableName + "\"\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db  = database(dbName, RANGE,0 5000 15000);\n";
    script1 += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
               "tableInsert(t,[`asd,0,10]);"
               "pt = db.createPartitionedTable(t,tableName,`id);"
               "tableInsert(pt,t)";
    pConn->run(script1);

    dolphindb::MTWConfig config(pConn, "loadTable('"+dbName+"', '"+tableName+"')");
    std::vector<std::string> modeOption = {"ignoreNull=true", "keyColNames=`id"};
    config.setThreads(1, "id").setWriteMode(dolphindb::WriteMode::Upsert, modeOption).setBatching(1000, std::chrono::seconds(1));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwspr = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;

    int colNum = 3, rowNum = 1000;
    std::vector<std::string> colNames = {"symbol", "id", "value"};
    std::vector<dolphindb::DATA_TYPE> colTypes = {dolphindb::DT_SYMBOL, dolphindb::DT_INT, dolphindb::DT_INT};

    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    std::string sym[] = {"A", "B", "C", "D"};

    std::vector<dolphindb::ConstantSP> *prow0 = new std::vector<dolphindb::ConstantSP>({dolphindb::Util::createString(sym[rand() % 4]), dolphindb::Util::createInt(0), dolphindb::Util::createNullConstant(dolphindb::DT_INT)});
    datas.emplace_back(prow0);

    for (int i = 1; i < rowNum; i++)
    {
        std::vector<dolphindb::ConstantSP> *prows = new std::vector<dolphindb::ConstantSP>();
        std::vector<dolphindb::ConstantSP> &rows = *prows;
        rows.emplace_back(dolphindb::Util::createString(sym[rand() % 4]));
        rows.emplace_back(dolphindb::Util::createInt(i));
        rows.emplace_back(dolphindb::Util::createInt((int)(rand() % 1000)));
        datas.emplace_back(prows);
    }

    std::vector<std::vector<std::string>> datasVec;
    for (int i = 0; i < rowNum; i++)
    {
        std::vector<std::string> oneRow;
        for (int j = 0; j < colNum; j++)
        {
            oneRow.push_back(datas[i][0][j]->getString());
        }
        datasVec.push_back(oneRow);
    }
    std::string prow0Str = prow0[0][0]->getString();
    bool s = mtwspr->insertUnwrittenData(datas, pErrorInfo);
    mtwspr->waitForThreadCompletion();

    dolphindb::MultithreadedTableWriter::Status status;
    mtwspr->getStatus(status);
    std::cout << status.errorInfo << std::endl;

    dolphindb::TableSP res = pConn->run("select * from pt;");
    std::cout << res->getString() << std::endl;
    ASSERT_EQ((res->getColumnType(0) == 18 || res->getColumnType(0) == 17), true);
    ASSERT_EQ(res->getColumnType(1), 4);
    ASSERT_EQ(res->getColumnType(2), 4);

    ASSERT_EQ(res->getColumn(0)->getRow(0)->getString(), prow0Str);
    ASSERT_EQ(res->getColumn(1)->getRow(0)->getInt(), 0);
    ASSERT_EQ(res->getColumn(2)->getRow(0)->getInt(), 10);
    for (int i = 1; i < rowNum; i++)
    {
        for (int j = 0; j < colNum; j++)
        {
            ASSERT_EQ(res->getColumn(j)->getRow(i)->getString(), datasVec[i][j]);
        }
    }
}

TEST_F(MultithreadedTableWriterNewTest, upsertToKeyedTable)
{
    std::string tableName=getCaseName();
    std::string script1;
    script1 = "t = table(5:0, `symbol`id`value,[SYMBOL, INT, INT]);"
               "share keyedTable(`id, t) as "+tableName+";"
              "tableInsert("+tableName+",`asd,0,10);";
    pConn->run(script1);
    dolphindb::MTWConfig config(pConn, tableName);
    std::vector<std::string> modeOption = {"ignoreNull=false", "keyColNames=`id"};
    config.setThreads(1, "id").setWriteMode(dolphindb::WriteMode::Upsert, modeOption).setBatching(1000, std::chrono::seconds(1));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwspr = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;

    int colNum = 3, rowNum = 5;
    std::vector<std::string> colNames = {"symbol", "id", "value"};
    std::vector<dolphindb::DATA_TYPE> colTypes = {dolphindb::DT_SYMBOL, dolphindb::DT_INT, dolphindb::DT_INT};

    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    std::string sym[] = {"A", "B", "C", "D"};

    std::vector<dolphindb::ConstantSP> *prow0 = new std::vector<dolphindb::ConstantSP>({dolphindb::Util::createString(sym[rand() % 4]), dolphindb::Util::createInt(0), dolphindb::Util::createNullConstant(dolphindb::DT_INT)});
    datas.emplace_back(prow0);

    for (int i = 1; i < rowNum; i++)
    {
        std::vector<dolphindb::ConstantSP> *prows = new std::vector<dolphindb::ConstantSP>();
        std::vector<dolphindb::ConstantSP> &rows = *prows;
        rows.emplace_back(dolphindb::Util::createString(sym[rand() % 4]));
        rows.emplace_back(dolphindb::Util::createInt(i));
        rows.emplace_back(dolphindb::Util::createInt((int)(rand() % 1000)));
        datas.emplace_back(prows);
    }

    std::vector<std::vector<std::string>> datasVec;
    for (int i = 0; i < rowNum; i++)
    {
        std::vector<std::string> oneRow;
        for (int j = 0; j < colNum; j++)
        {
            oneRow.push_back(datas[i][0][j]->getString());
        }
        datasVec.push_back(oneRow);
    }

    bool s = mtwspr->insertUnwrittenData(datas, pErrorInfo);

    mtwspr->waitForThreadCompletion();

    dolphindb::MultithreadedTableWriter::Status status;
    mtwspr->getStatus(status);

    dolphindb::TableSP res = pConn->run(tableName);

    ASSERT_EQ((res->getColumnType(0) == 18 || res->getColumnType(0) == 17), true);
    ASSERT_EQ(res->getColumnType(1), 4);
    ASSERT_EQ(res->getColumnType(2), 4);
    for (int i = 0; i < rowNum; i++)
    {
        for (int j = 0; j < colNum; j++)
        {
            ASSERT_EQ(res->getColumn(j)->getRow(i)->getString(), datasVec[i][j]);
        }
    }
}

TEST_F(MultithreadedTableWriterNewTest, upsertToIndexedTable)
{
    std::string script1;
    std::string tableName=getCaseName();
    script1 = "t = table(5:0, `symbol`id`value,[SYMBOL, INT, INT]);"
              "share indexedTable(`id, t) as "+tableName+";"
              "tableInsert("+tableName+",`asd,0,10);";
    pConn->run(script1);

    dolphindb::MTWConfig config(pConn, tableName);
    std::vector<std::string> modeOption = {"ignoreNull=false", "keyColNames=`id"};
    config.setThreads(1, "id").setWriteMode(dolphindb::WriteMode::Upsert, modeOption).setBatching(1, std::chrono::seconds(1));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwspr = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;

    int colNum = 3, rowNum = 5;
    std::vector<std::string> colNames = {"symbol", "id", "value"};
    std::vector<dolphindb::DATA_TYPE> colTypes = {dolphindb::DT_SYMBOL, dolphindb::DT_INT, dolphindb::DT_INT};

    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    std::string sym[] = {"A", "B", "C", "D"};

    std::vector<dolphindb::ConstantSP> *prow0 = new std::vector<dolphindb::ConstantSP>({dolphindb::Util::createString(sym[rand() % 4]), dolphindb::Util::createInt(0), dolphindb::Util::createNullConstant(dolphindb::DT_INT)});
    datas.emplace_back(prow0);

    for (int i = 1; i < rowNum; i++)
    {
        std::vector<dolphindb::ConstantSP> *prows = new std::vector<dolphindb::ConstantSP>();
        std::vector<dolphindb::ConstantSP> &rows = *prows;
        rows.emplace_back(dolphindb::Util::createString(sym[rand() % 4]));
        rows.emplace_back(dolphindb::Util::createInt(i));
        rows.emplace_back(dolphindb::Util::createInt((int)(rand() % 1000)));
        datas.emplace_back(prows);
    }

    std::vector<std::vector<std::string>> datasVec;
    for (int i = 0; i < rowNum; i++)
    {
        std::vector<std::string> oneRow;
        for (int j = 0; j < colNum; j++)
        {
            oneRow.push_back(datas[i][0][j]->getString());
        }
        datasVec.push_back(oneRow);
    }

    bool s = mtwspr->insertUnwrittenData(datas, pErrorInfo);

    mtwspr->waitForThreadCompletion();

    dolphindb::MultithreadedTableWriter::Status status;
    mtwspr->getStatus(status);

    dolphindb::TableSP res = pConn->run(tableName);

    ASSERT_EQ((res->getColumnType(0) == 18 || res->getColumnType(0) == 17), true);
    ASSERT_EQ(res->getColumnType(1), 4);
    ASSERT_EQ(res->getColumnType(2), 4);
    for (int i = 0; i < rowNum; i++)
    {
        for (int j = 0; j < colNum; j++)
        {
            ASSERT_EQ(res->getColumn(j)->getRow(i)->getString(), datasVec[i][j]);
        }
    }
}

TEST_F(MultithreadedTableWriterNewTest, upsertIndexedTableIgnoreNull)
{
    std::string script1;
    std::string tableName=getCaseName();
    script1 = "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
              "share indexedTable(`id, t) as "+tableName+";"
              "tableInsert("+tableName+",`asd,0,10);";
    pConn->run(script1);

    dolphindb::MTWConfig config(pConn, tableName);
    std::vector<std::string> modeOption = {"ignoreNull=true", "keyColNames=`id"};
    config.setThreads(1, "id").setWriteMode(dolphindb::WriteMode::Upsert, modeOption).setBatching(1000, std::chrono::seconds(1));
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwspr = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;

    int colNum = 3, rowNum = 1000;
    std::vector<std::string> colNames = {"symbol", "id", "value"};
    std::vector<dolphindb::DATA_TYPE> colTypes = {dolphindb::DT_SYMBOL, dolphindb::DT_INT, dolphindb::DT_INT};

    std::vector<std::vector<dolphindb::ConstantSP> *> datas;
    std::string sym[] = {"A", "B", "C", "D"};

    std::vector<dolphindb::ConstantSP> *prow0 = new std::vector<dolphindb::ConstantSP>({dolphindb::Util::createString(sym[rand() % 4]), dolphindb::Util::createInt(0), dolphindb::Util::createNullConstant(dolphindb::DT_INT)});
    datas.emplace_back(prow0);

    for (int i = 1; i < rowNum; i++)
    {
        std::vector<dolphindb::ConstantSP> *prows = new std::vector<dolphindb::ConstantSP>();
        std::vector<dolphindb::ConstantSP> &rows = *prows;
        rows.emplace_back(dolphindb::Util::createString(sym[rand() % 4]));
        rows.emplace_back(dolphindb::Util::createInt(i));
        rows.emplace_back(dolphindb::Util::createInt((int)(rand() % 1000)));
        datas.emplace_back(prows);
    }

    std::vector<std::vector<std::string>> datasVec;
    for (int i = 0; i < rowNum; i++)
    {
        std::vector<std::string> oneRow;
        for (int j = 0; j < colNum; j++)
        {
            oneRow.push_back(datas[i][0][j]->getString());
        }
        datasVec.push_back(oneRow);
    }
    std::string prow0Str = prow0[0][0]->getString();
    bool s = mtwspr->insertUnwrittenData(datas, pErrorInfo);
    mtwspr->waitForThreadCompletion();

    dolphindb::MultithreadedTableWriter::Status status;
    mtwspr->getStatus(status);
    dolphindb::TableSP res = pConn->run(tableName);
    ASSERT_EQ((res->getColumnType(0) == 18 || res->getColumnType(0) == 17), true);
    ASSERT_EQ(res->getColumnType(1), 4);
    ASSERT_EQ(res->getColumnType(2), 4);

    ASSERT_EQ(res->getColumn(0)->getRow(0)->getString(), prow0Str);
    ASSERT_EQ(res->getColumn(1)->getRow(0)->getInt(), 0);
    ASSERT_EQ(res->getColumn(2)->getRow(0)->getInt(), 10);
    for (int i = 1; i < rowNum; i++)
    {
        for (int j = 0; j < colNum; j++)
        {
            ASSERT_EQ(res->getColumn(j)->getRow(i)->getString(), datasVec[i][j]);
        }
    }
}

TEST_F(MultithreadedTableWriterNewTest, insertTodfsTablewithAlldataTypes)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    int colNum = 25, rowNum = 2000;
    std::vector<std::string> colNamesVec1;
    for (int i = 0; i < colNum; i++)
    {
        colNamesVec1.emplace_back("col" + std::to_string(i));
    }
    std::vector<dolphindb::DATA_TYPE> colTypesVec1;
    colTypesVec1.emplace_back(dolphindb::DT_CHAR);
    colTypesVec1.emplace_back(dolphindb::DT_BOOL);
    colTypesVec1.emplace_back(dolphindb::DT_SHORT);
    colTypesVec1.emplace_back(dolphindb::DT_INT);
    colTypesVec1.emplace_back(dolphindb::DT_LONG);
    colTypesVec1.emplace_back(dolphindb::DT_DATE);
    colTypesVec1.emplace_back(dolphindb::DT_MONTH);
    colTypesVec1.emplace_back(dolphindb::DT_TIME);
    colTypesVec1.emplace_back(dolphindb::DT_MINUTE);
    colTypesVec1.emplace_back(dolphindb::DT_DATETIME);
    colTypesVec1.emplace_back(dolphindb::DT_SECOND);
    colTypesVec1.emplace_back(dolphindb::DT_TIMESTAMP);
    colTypesVec1.emplace_back(dolphindb::DT_NANOTIME);
    colTypesVec1.emplace_back(dolphindb::DT_NANOTIMESTAMP);
    colTypesVec1.emplace_back(dolphindb::DT_FLOAT);
    colTypesVec1.emplace_back(dolphindb::DT_DOUBLE);
    colTypesVec1.emplace_back(dolphindb::DT_STRING);
    colTypesVec1.emplace_back(dolphindb::DT_UUID);
    colTypesVec1.emplace_back(dolphindb::DT_IP);
    colTypesVec1.emplace_back(dolphindb::DT_INT128);
    colTypesVec1.emplace_back(dolphindb::DT_BLOB);
    colTypesVec1.emplace_back(dolphindb::DT_DATEHOUR);
    colTypesVec1.emplace_back(dolphindb::DT_DECIMAL32);
    colTypesVec1.emplace_back(dolphindb::DT_DECIMAL64);
    colTypesVec1.emplace_back(dolphindb::DT_SYMBOL);

    srand(time(NULL));
    dolphindb::TableSP tab1 = dolphindb::Util::createTable(colNamesVec1, colTypesVec1, rowNum, rowNum);
    std::vector<dolphindb::VectorSP> columnVecs;
    columnVecs.reserve(colNum);
    for (int i = 0; i < colNum; i++)
    {
        columnVecs.emplace_back(tab1->getColumn(i));
    }
    for (int i = 0; i < rowNum; i++)
    {
        columnVecs[0]->set(i, dolphindb::Util::createChar(rand() % CHAR_MAX));
        columnVecs[1]->set(i, dolphindb::Util::createBool((char)(rand() % 2)));
        columnVecs[2]->set(i, dolphindb::Util::createShort(rand() % SHRT_MAX));
        columnVecs[3]->set(i, dolphindb::Util::createInt(i));
        columnVecs[4]->set(i, dolphindb::Util::createLong(rand() % LLONG_MAX));
        columnVecs[5]->set(i, dolphindb::Util::createDate(rand() % INT_MAX));
        columnVecs[6]->set(i, dolphindb::Util::createMonth(rand() % INT_MAX));
        columnVecs[7]->set(i, dolphindb::Util::createTime(rand() % INT_MAX));
        columnVecs[8]->set(i, dolphindb::Util::createMinute(rand() % 1440));
        columnVecs[9]->set(i, dolphindb::Util::createDateTime(rand() % INT_MAX));
        columnVecs[10]->set(i, dolphindb::Util::createSecond(rand() % 86400));
        columnVecs[11]->set(i, dolphindb::Util::createTimestamp(rand() % LLONG_MAX));
        columnVecs[12]->set(i, dolphindb::Util::createNanoTime(rand() % LLONG_MAX));
        columnVecs[13]->set(i, dolphindb::Util::createNanoTimestamp(rand() % LLONG_MAX));
        columnVecs[14]->set(i, dolphindb::Util::createFloat(rand() / float(RAND_MAX)));
        columnVecs[15]->set(i, dolphindb::Util::createDouble(rand() / double(RAND_MAX)));
        columnVecs[16]->set(i, dolphindb::Util::createString("str" + std::to_string(i)));
        columnVecs[17]->set(i, dolphindb::Util::parseConstant(dolphindb::DT_UUID, "5d212a78-cc48-e3b1-4235-b4d91473ee87"));
        columnVecs[18]->set(i, dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.0.0." + std::to_string(rand() % 255)));
        columnVecs[19]->set(i, dolphindb::Util::parseConstant(dolphindb::DT_INT128, "e1671797c52e15f763380b45e841ec32"));
        columnVecs[20]->set(i, dolphindb::Util::createBlob("blob"));
        columnVecs[21]->set(i, dolphindb::Util::createDateHour(rand() % INT_MAX));
        columnVecs[22]->set(i, dolphindb::Util::createDecimal32(rand() % 10, rand() / float(RAND_MAX)));
        columnVecs[23]->set(i, dolphindb::Util::createDecimal64(rand() % 19, rand() / double(RAND_MAX)));
        columnVecs[24]->set(i, dolphindb::Util::createString("sym"));
    }

    pConn->upload("tab1", {tab1});

    std::string script = "dbName=\""+dbName+"\";"
                    "if(existsDatabase(dbName)){dropDatabase(dbName)};go;"
                    "db=database(dbName, HASH, [SYMBOL,3],,'TSDB');"
                    "pt = db.createPartitionedTable(tab1, `pt, `col24,,`col3)";
    pConn->run(script);

    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    config.setBatching(1000, std::chrono::seconds(1)).setThreads(1, "col24");
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;

    dolphindb::MultithreadedTableWriter::Status status;
    std::vector<dolphindb::ConstantSP> datas;
    datas.reserve(rowNum * colNum);
    for (auto i = 0; i < rowNum; i++)
    {
        for (auto j = 0; j < colNum; j++)
            datas.emplace_back(tab1->getColumn(j)->get(i));
    }
    for (auto i = 0; i < rowNum; i++)
    {
        if (!mtwsp->insert(pErrorInfo, datas[i * 25 + 0], datas[i * 25 + 1], datas[i * 25 + 2], datas[i * 25 + 3],
                           datas[i * 25 + 4], datas[i * 25 + 5], datas[i * 25 + 6], datas[i * 25 + 7], datas[i * 25 + 8],
                           datas[i * 25 + 9], datas[i * 25 + 10], datas[i * 25 + 11], datas[i * 25 + 12], datas[i * 25 + 13], datas[i * 25 + 14],
                           datas[i * 25 + 15], datas[i * 25 + 16], datas[i * 25 + 17], datas[i * 25 + 18], datas[i * 25 + 19], datas[i * 25 + 20],
                           datas[i * 25 + 21], datas[i * 25 + 22], datas[i * 25 + 23], datas[i * 25 + 24]))
        {
            break;
        }
    }

    mtwsp->waitForThreadCompletion();

    dolphindb::VectorSP res = pConn->run("ex = exec * from loadTable(\""+dbName+"\", `pt) order by col3;"
                            "res = exec * from tab1 order by col3;"
                            "each(eqObj,res.values(), ex.values())");

    for (auto i = 0; i < res->size(); i++)
        ASSERT_TRUE(res->get(i)->getBool());
}

TEST_F(MultithreadedTableWriterNewTest, insertTo_inMemoryTable_with_onDataWrite_fail)
{
    std::string case_=getCaseName();
    int rowNum = 10;
    std::string script = "col0 = 1 2 3 4 5;col1 = `APPL`TESLA`GO`WSD`NIKE;col2 = [0,1.3215,23.444,566.345,68.243];"
                    "pt= table(col0,col1,col2);share(pt,`"+case_+",readonly=true)";
    pConn->run(script);
    auto callback = [&](dolphindb::ConstantSP callbackTable)
    {
        int size = callbackTable->size();
        dolphindb::VectorSP id = callbackTable->getColumn(0);
        dolphindb::VectorSP status = callbackTable->getColumn(1);

        for (auto i = 0; i < size; i++)
        {
            ASSERT_EQ(status->get(i)->getBool(), false);
        }
    };

    dolphindb::MTWConfig config(pConn, case_);
    config.setBatching(1000, std::chrono::seconds(1)).setWriteMode(dolphindb::WriteMode::Append, {}).onDataWrite(callback);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    dolphindb::MultithreadedTableWriter::Status status;

    std::string callbackfunc_id = "";
    for (auto i = 0; i < rowNum; i++)
    {
        callbackfunc_id = "row" + std::to_string(i);
        mtwsp->insert(pErrorInfo, callbackfunc_id, 1, "str" + std::to_string(i), double(i));
    }

    mtwsp->waitForThreadCompletion();
    ASSERT_EQ(pConn->run("exec count(*) from "+case_)->getInt(), 5);
}

TEST_F(MultithreadedTableWriterNewTest, insertTo_dfsTable_with_onDataWrite_success)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName=\""+dbName+"\";"
                    "if(existsDatabase(dbName)){dropDatabase(dbName)};go;"
                    "db=database(dbName, HASH, [SYMBOL,3],,'TSDB');"
                    "tab1 = table(0 1 2 3 4 as col0, `str0`str1`str2`str3`str4 as col1, double(0 1 2 3 4) as col2);"
                    "pt = db.createPartitionedTable(tab1, `pt, `col1,,`col0)";
    pConn->run(script);
    auto callback = [&](dolphindb::ConstantSP callbackTable)
    {
        int size = callbackTable->size();
        dolphindb::VectorSP id = callbackTable->getColumn(0);
        dolphindb::VectorSP status = callbackTable->getColumn(1);

        for (auto i = 0; i < size; i++)
        {
            ASSERT_EQ(status->get(i)->getBool(), true);
        }
    };

    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    config.setBatching(1000, std::chrono::seconds(1)).setThreads(5, "col1").setWriteMode(dolphindb::WriteMode::Append, {}).onDataWrite(callback);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;

    dolphindb::MultithreadedTableWriter::Status status;
    std::vector<dolphindb::ConstantSP> datas;

    std::string callbackfunc_id = "";
    for (auto i = 0; i < 5; i++)
    {
        callbackfunc_id = "row" + std::to_string(i);
        mtwsp->insert(pErrorInfo, callbackfunc_id, int(i), "str" + std::to_string(i), double(i));
    }

    mtwsp->waitForThreadCompletion();

    dolphindb::VectorSP res = pConn->run("ex = exec * from loadTable(\""+dbName+"\", `pt) order by col0;"
                            "res = exec * from tab1 order by col0;"
                            "each(eqObj,res.values(), ex.values())");

    for (auto i = 0; i < res->size(); i++)
        ASSERT_TRUE(res->get(i)->getBool());
}

TEST_F(MultithreadedTableWriterNewTest, insertTo_inMemorytable_with_onDataWrite_success)
{
    std::string case_=getCaseName();
    int colNum = 3, rowNum = 10;
    std::vector<std::string> colNamesVec1;
    for (int i = 0; i < colNum; i++)
    {
        colNamesVec1.emplace_back("col" + std::to_string(i));
    }
    std::vector<dolphindb::DATA_TYPE> colTypesVec1;
    colTypesVec1.emplace_back(dolphindb::DT_INT);
    colTypesVec1.emplace_back(dolphindb::DT_SYMBOL);
    colTypesVec1.emplace_back(dolphindb::DT_DOUBLE);

    dolphindb::TableSP tab1 = dolphindb::Util::createTable(colNamesVec1, colTypesVec1, rowNum, rowNum);
    std::vector<dolphindb::VectorSP> columnVecs;
    columnVecs.reserve(colNum);
    for (int i = 0; i < colNum; i++)
    {
        columnVecs.emplace_back(tab1->getColumn(i));
    }
    for (int i = 0; i < rowNum; i++)
    {
        columnVecs[0]->set(i, dolphindb::Util::createInt(i));
        columnVecs[1]->set(i, dolphindb::Util::createString("str" + std::to_string(i)));
        columnVecs[2]->set(i, dolphindb::Util::createDouble(i));
    }

    pConn->upload("tab1", {tab1});

    std::string script = "share table(100:0, [`col0,`col1,`col2],[INT, SYMBOL, DOUBLE]) as "+case_;
    pConn->run(script);
    auto callback = [&](dolphindb::ConstantSP callbackTable)
    {
        int size = callbackTable->size();
        dolphindb::VectorSP id = callbackTable->getColumn(0);
        dolphindb::VectorSP status = callbackTable->getColumn(1);

        for (auto i = 0; i < size; i++)
        {
            ASSERT_EQ(status->get(i)->getBool(), true);
        }
    };

    dolphindb::MTWConfig config(pConn, case_);
    config.setBatching(1000, std::chrono::seconds(1)).setWriteMode(dolphindb::WriteMode::Append, {}).onDataWrite(callback);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;

    dolphindb::MultithreadedTableWriter::Status status;
    std::vector<dolphindb::ConstantSP> datas;
    datas.reserve(rowNum * colNum);
    for (auto i = 0; i < rowNum; i++)
    {
        for (auto j = 0; j < colNum; j++)
            datas.emplace_back(tab1->getColumn(j)->get(i));
    }

    std::string callbackfunc_id = "";
    for (auto i = 0; i < rowNum; i++)
    {
        callbackfunc_id = "row" + std::to_string(i);
        mtwsp->insert(pErrorInfo, callbackfunc_id, int(i), "str" + std::to_string(i), double(i));
    }

    mtwsp->waitForThreadCompletion();
    dolphindb::VectorSP res = pConn->run("ex = exec * from "+case_+" order by col0;"
                            "res = exec * from tab1 order by col0;"
                            "each(eqObj,res.values(), ex.values())");

    for (auto i = 0; i < res->size(); i++)
        ASSERT_TRUE(res->get(i)->getBool());
}

TEST_F(MultithreadedTableWriterNewTest, insert_with_onDataWrite_getUnwrittenData)
{
    std::string case_=getCaseName();
    int colNum = 3, rowNum = 10;
    std::vector<std::string> colNamesVec1;
    for (int i = 0; i < colNum; i++)
    {
        colNamesVec1.emplace_back("col" + std::to_string(i));
    }
    std::vector<dolphindb::DATA_TYPE> colTypesVec1;
    colTypesVec1.emplace_back(dolphindb::DT_INT);
    colTypesVec1.emplace_back(dolphindb::DT_SYMBOL);
    colTypesVec1.emplace_back(dolphindb::DT_DOUBLE);

    dolphindb::TableSP tab1 = dolphindb::Util::createTable(colNamesVec1, colTypesVec1, rowNum, rowNum);
    std::vector<dolphindb::VectorSP> columnVecs;
    columnVecs.reserve(colNum);
    for (int i = 0; i < colNum; i++)
    {
        columnVecs.emplace_back(tab1->getColumn(i));
    }
    for (int i = 0; i < rowNum; i++)
    {
        columnVecs[0]->set(i, dolphindb::Util::createInt(i));
        columnVecs[1]->set(i, dolphindb::Util::createString("str" + std::to_string(i)));
        columnVecs[2]->set(i, dolphindb::Util::createDouble(i));
    }

    pConn->upload("tab1", {tab1});

    std::string script = "share table(100:0, [`col0,`col1,`col2],[INT, SYMBOL, DOUBLE]) as "+case_;
    pConn->run(script);
    auto callback = [&](dolphindb::ConstantSP callbackTable)
    {
        int size = callbackTable->size();
        dolphindb::VectorSP id = callbackTable->getColumn(0);
        dolphindb::VectorSP status = callbackTable->getColumn(1);

        dolphindb::DLogger::Info("callback", callbackTable->getString());
    };

    dolphindb::MTWConfig config(pConn, case_);
    config.setBatching(1000, std::chrono::seconds(1)).setWriteMode(dolphindb::WriteMode::Append, {}).onDataWrite(callback);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;

    dolphindb::MultithreadedTableWriter::Status status;
    std::vector<dolphindb::ConstantSP> datas;
    datas.reserve(rowNum * colNum);
    for (auto i = 0; i < rowNum; i++)
    {
        for (auto j = 0; j < colNum; j++)
            datas.emplace_back(tab1->getColumn(j)->get(i));
    }

    std::string callbackfunc_id = "";
    for (auto i = 0; i < rowNum; i++)
    {
        callbackfunc_id = "row" + std::to_string(i);
        mtwsp->insert(pErrorInfo, callbackfunc_id, int(i), int(i), double(i));
    }

    mtwsp->waitForThreadCompletion();

    std::vector<std::vector<dolphindb::ConstantSP> *> unwrittenData;
    ASSERT_ANY_THROW(mtwsp->getUnwrittenData(unwrittenData));
}

TEST_F(MultithreadedTableWriterNewTest, test_column_info_in_errMsg)
{
    std::string case_=getCaseName();
    std::string script =
        "colName =  `cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`csymbol`cstring`cblob`cipaddr`cuuid`cint128`cdecimal32`cdecimal64;"
        "colType = [BOOL, CHAR, SHORT, INT,LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, DATEHOUR, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, IPADDR, UUID, INT128,DECIMAL32(2),DECIMAL64(11)];"
        "share table(1:0, colName,colType) as "+case_;
    pConn->run(script);
    dolphindb::ErrorCodeInfo errorInfo;
    dolphindb::MTWConfig config = dolphindb::MTWConfig(pConn, case_);
    dolphindb::MTWConfig config2 = dolphindb::MTWConfig(pConn, case_);
    config.setThreads(1, "csymbol");
    config2.setThreads(1, "csymbol");
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> writer = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> writer2 = new dolphindb::MultithreadedTableWriter(config2);
    char msg[] = "123456msg";

    writer->insert(errorInfo, false, '1', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1ll, 1l, 1l, 1, 10.0, -0.123, msg, msg, msg, "192.168.2.1", "0f0e0d0c-0b0a-0908-0706-050403020100", "0f0e0d0c0b0a09080706050403020100", 0.2313, -1);
    writer->waitForThreadCompletion();
    ASSERT_TRUE(pConn->run("eqObj(values "+case_+", [[false], ['1'], [short(1)], [int(1)], [long(1)], [date(1)], [month(1)], [time(1)], [minute(1)], [second(1)], [datetime(1)], [timestamp(1)], [nanotime(1)], [nanotimestamp(1)], [datehour(1)], [float(10.0)], [double(-0.123)], symbol([`123456msg]), [`123456msg], [blob(`123456msg)], [ipaddr('192.168.2.1')], [uuid('0f0e0d0c-0b0a-0908-0706-050403020100')], [int128('0f0e0d0c0b0a09080706050403020100')], decimal32([0.2313], 2), decimal64([-1], 11)])")->getBool());

    // insert int to timestamp
    if (!writer2->insert(errorInfo, false, '1', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1ll, 1l, 1, 10.0, -0.123, msg, msg, msg, "192.168.2.1", "0f0e0d0c-0b0a-0908-0706-050403020100", "0f0e0d0c0b0a09080706050403020100", 0.2353, -1))
    {
        std::cout << "insert fail " << errorInfo.errorInfo << std::endl;
        ASSERT_EQ(errorInfo.errorInfo, "Cannot convert int to TIMESTAMP for col 12");
    }

    // insert int to nanotime
    if (!writer2->insert(errorInfo, false, '1', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1ll, 1, 1l, 1, 10.0, -0.123, msg, msg, msg, "192.168.2.1", "0f0e0d0c-0b0a-0908-0706-050403020100", "0f0e0d0c0b0a09080706050403020100", 0.2353, -1))
    {
        std::cout << "insert fail " << errorInfo.errorInfo << std::endl;
        ASSERT_EQ(errorInfo.errorInfo, "Cannot convert int to NANOTIME for col 13");
    }

    // insert int to nanotimestamp
    if (!writer2->insert(errorInfo, false, '1', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1ll, 1ll, 1, 1, 10.0, -0.123, msg, msg, msg, "192.168.2.1", "0f0e0d0c-0b0a-0908-0706-050403020100", "0f0e0d0c0b0a09080706050403020100", 0.2353, -1))
    {
        std::cout << "insert fail " << errorInfo.errorInfo << std::endl;
        ASSERT_EQ(errorInfo.errorInfo, "Cannot convert int to NANOTIMESTAMP for col 14");
    }

    // insert int to float
    if (!writer2->insert(errorInfo, false, '1', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1ll, 1ll, 1ll, 1, 1, -0.123, msg, msg, msg, "192.168.2.1", "0f0e0d0c-0b0a-0908-0706-050403020100", "0f0e0d0c0b0a09080706050403020100", 0.2353, -1))
    {
        std::cout << "insert fail " << errorInfo.errorInfo << std::endl;
        ASSERT_EQ(errorInfo.errorInfo, "Cannot convert int to FLOAT for col 16");
    }

    // insert double to int
    if (!writer2->insert(errorInfo, false, '1', 1, 1.123, 1, 1, 1, 1, 1, 1, 1, 1ll, 1ll, 1ll, 1, 10.0, -0.123, msg, msg, msg, "192.168.2.1", "0f0e0d0c-0b0a-0908-0706-050403020100", "0f0e0d0c0b0a09080706050403020100", 1, -1))
    {
        std::cout << "insert fail " << errorInfo.errorInfo << std::endl;
        ASSERT_EQ(errorInfo.errorInfo, "Cannot convert double to INT for col 4");
    }

    // insert error form ipaddr to ipaddr
    if (!writer2->insert(errorInfo, false, '1', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1ll, 1ll, 1ll, 1, 10.0, -0.123, msg, msg, msg, "baskdjwjkn", "0f0e0d0c-0b0a-0908-0706-050403020100", "0f0e0d0c0b0a09080706050403020100", 1, -1))
    {
        std::cout << "insert fail " << errorInfo.errorInfo << std::endl;
        ASSERT_EQ(errorInfo.errorInfo, "Cannot convert string to IPADDR for col 21");
    }

    // insert int to std::string/symbol/blob
    if (!writer2->insert(errorInfo, true, '1', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1ll, 1ll, 1ll, 1, 10.0, -0.123, 1, 1, 1, "192.168.2.1", "0f0e0d0c-0b0a-0908-0706-050403020100", "0f0e0d0c0b0a09080706050403020100", 1, -1))
    {
        std::cout << "insert fail " << errorInfo.errorInfo << std::endl;
        ASSERT_EQ(errorInfo.errorInfo, "Cannot convert int to BLOB for col 20");
    }
    writer2->waitForThreadCompletion();
}

TEST_F(MultithreadedTableWriterNewTest, test_insert_dfs_column_info_in_errMsg)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script =
        "colName =  `cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`csymbol`cstring`cblob`cipaddr`cuuid`cint128`cdecimal32`cdecimal64;"
        "colType = [BOOL, CHAR, SHORT, INT,LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, DATEHOUR, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, IPADDR, UUID, INT128,DECIMAL32(2),DECIMAL64(11)];"
        "t = table(1:0, colName,colType);"
        "dbpath = '"+dbName+"';if(existsDatabase(dbpath)){dropDatabase(dbpath)};"
        "db = database(dbpath, HASH,[INT, 1],,'TSDB');"
        "db.createPartitionedTable(t,`pt,`cint,,`cint);";
    pConn->run(script);
    dolphindb::ErrorCodeInfo errorInfo;
    dolphindb::MTWConfig config = dolphindb::MTWConfig(pConn, "loadTable('"+dbName+"',`pt)");
    dolphindb::MTWConfig config2 = dolphindb::MTWConfig(pConn, "loadTable('"+dbName+"',`pt)");
    config.setThreads(1, "cint");
    config2.setThreads(1, "cint");
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> writer = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> writer2 = new dolphindb::MultithreadedTableWriter(config2);
    char msg[] = "123456msg";

    writer->insert(errorInfo, false, '1', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1ll, 1l, 1l, 1, 10.0, -0.123, msg, msg, msg, "192.168.2.1", "0f0e0d0c-0b0a-0908-0706-050403020100", "0f0e0d0c0b0a09080706050403020100", 0.2313, -1);
    writer->waitForThreadCompletion();
    ASSERT_TRUE(pConn->run("eqObj((select * from loadTable('"+dbName+"',`pt)).values(), [[false], ['1'], [short(1)], [int(1)], [long(1)], [date(1)], [month(1)], [time(1)], [minute(1)], [second(1)], [datetime(1)], [timestamp(1)], [nanotime(1)], [nanotimestamp(1)], [datehour(1)], [float(10.0)], [double(-0.123)], symbol([`123456msg]), [`123456msg], [blob(`123456msg)], [ipaddr('192.168.2.1')], [uuid('0f0e0d0c-0b0a-0908-0706-050403020100')], [int128('0f0e0d0c0b0a09080706050403020100')], decimal32([0.2313], 2), decimal64([-1], 11)])")->getBool());

    // insert int to timestamp
    if (!writer2->insert(errorInfo, false, '1', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1ll, 1l, 1, 10.0, -0.123, msg, msg, msg, "192.168.2.1", "0f0e0d0c-0b0a-0908-0706-050403020100", "0f0e0d0c0b0a09080706050403020100", 0.2353, -1))
    {
        std::cout << "insert fail " << errorInfo.errorInfo << std::endl;
        ASSERT_EQ(errorInfo.errorInfo, "Cannot convert int to TIMESTAMP for col 12");
    }

    // insert int to nanotime
    if (!writer2->insert(errorInfo, false, '1', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1ll, 1, 1l, 1, 10.0, -0.123, msg, msg, msg, "192.168.2.1", "0f0e0d0c-0b0a-0908-0706-050403020100", "0f0e0d0c0b0a09080706050403020100", 0.2353, -1))
    {
        std::cout << "insert fail " << errorInfo.errorInfo << std::endl;
        ASSERT_EQ(errorInfo.errorInfo, "Cannot convert int to NANOTIME for col 13");
    }

    // insert int to nanotimestamp
    if (!writer2->insert(errorInfo, false, '1', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1ll, 1ll, 1, 1, 10.0, -0.123, msg, msg, msg, "192.168.2.1", "0f0e0d0c-0b0a-0908-0706-050403020100", "0f0e0d0c0b0a09080706050403020100", 0.2353, -1))
    {
        std::cout << "insert fail " << errorInfo.errorInfo << std::endl;
        ASSERT_EQ(errorInfo.errorInfo, "Cannot convert int to NANOTIMESTAMP for col 14");
    }

    // insert int to float
    if (!writer2->insert(errorInfo, false, '1', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1ll, 1ll, 1ll, 1, 1, -0.123, msg, msg, msg, "192.168.2.1", "0f0e0d0c-0b0a-0908-0706-050403020100", "0f0e0d0c0b0a09080706050403020100", 0.2353, -1))
    {
        std::cout << "insert fail " << errorInfo.errorInfo << std::endl;
        ASSERT_EQ(errorInfo.errorInfo, "Cannot convert int to FLOAT for col 16");
    }

    // insert double to int
    if (!writer2->insert(errorInfo, false, '1', 1, 1.123, 1, 1, 1, 1, 1, 1, 1, 1ll, 1ll, 1ll, 1, 10.0, -0.123, msg, msg, msg, "192.168.2.1", "0f0e0d0c-0b0a-0908-0706-050403020100", "0f0e0d0c0b0a09080706050403020100", 1, -1))
    {
        std::cout << "insert fail " << errorInfo.errorInfo << std::endl;
        ASSERT_EQ(errorInfo.errorInfo, "Cannot convert double to INT for col 4");
    }

    // insert error form ipaddr to ipaddr
    if (!writer2->insert(errorInfo, false, '1', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1ll, 1ll, 1ll, 1, 10.0, -0.123, msg, msg, msg, "baskdjwjkn", "0f0e0d0c-0b0a-0908-0706-050403020100", "0f0e0d0c0b0a09080706050403020100", 1, -1))
    {
        std::cout << "insert fail " << errorInfo.errorInfo << std::endl;
        ASSERT_EQ(errorInfo.errorInfo, "Cannot convert string to IPADDR for col 21");
    }

    // insert int to std::string/symbol/blob
    if (!writer2->insert(errorInfo, true, '1', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1ll, 1ll, 1ll, 1, 10.0, -0.123, 1, 1, 1, "192.168.2.1", "0f0e0d0c-0b0a-0908-0706-050403020100", "0f0e0d0c0b0a09080706050403020100", 1, -1))
    {
        std::cout << "insert fail " << errorInfo.errorInfo << std::endl;
        ASSERT_EQ(errorInfo.errorInfo, "Cannot convert int to BLOB for col 20");
    }
    writer2->waitForThreadCompletion();
}

class MTW_insert_null_data_new : public MultithreadedTableWriterNewTest, public testing::WithParamInterface<std::string>
{
public:
    static std::vector<std::string> testTypes()
    {
        return {"BOOL", "CHAR", "SHORT", "INT", "LONG", "DATE", "MONTH", "TIME", "MINUTE", "SECOND", "DATETIME", "TIMESTAMP", "NANOTIME", "NANOTIMESTAMP", "DATEHOUR", "FLOAT", "DOUBLE", "STRING", "SYMBOL", "BLOB", "IPADDR", "UUID", "INT128", "DECIMAL32(8)", "DECIMAL64(15)", "DECIMAL128(28)",
                "BOOL[]", "CHAR[]", "SHORT[]", "INT[]", "LONG[]", "DATE[]", "MONTH[]", "TIME[]", "MINUTE[]", "SECOND[]", "DATETIME[]", "TIMESTAMP[]", "NANOTIME[]", "NANOTIMESTAMP[]", "DATEHOUR[]", "FLOAT[]", "DOUBLE[]", "IPADDR[]", "UUID[]", "INT128[]", "DECIMAL32(8)[]", "DECIMAL64(15)[]", "DECIMAL128(25)[]"};
    }
};
INSTANTIATE_TEST_SUITE_P(, MTW_insert_null_data_new, testing::ValuesIn(MTW_insert_null_data_new::testTypes()));

TEST_P(MTW_insert_null_data_new, test_insert_all_null_data)
{
    std::string type = GetParam();
    std::cout << "test type: " << type << std::endl;
    std::string colName = "c1";
    std::string table=getCaseName();
    std::string script1 =
        "colName = [`" + colName + "];"
                                   "colType = [" +
        type + "];"
               "share table(1:0, colName, colType) as "+table;

    pConn->run(script1);
    dolphindb::ErrorCodeInfo errorInfo;
    dolphindb::MTWConfig config = dolphindb::MTWConfig(pConn, table);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> writer = new dolphindb::MultithreadedTableWriter(config);
    bool success = false;
    if (type == "STRING" || type == "SYMBOL" || type == "BLOB")
    {
        success = writer->insert(errorInfo, "");
    }
    else if (type.find("IPADDR") != std::string::npos)
    {
        success = writer->insert(errorInfo, dolphindb::Util::createNullConstant(dolphindb::DT_IP));
    }
    else if (type.find("UUID") != std::string::npos)
    {
        success = writer->insert(errorInfo, dolphindb::Util::createNullConstant(dolphindb::DT_UUID));
    }
    else if (type.find("INT128") != std::string::npos)
    {
        success = writer->insert(errorInfo, dolphindb::Util::createNullConstant(dolphindb::DT_INT128));
    }
    else if (type == "BOOL")
    {
        success = writer->insert(errorInfo, dolphindb::Util::createNullConstant(dolphindb::DT_BOOL));
    }
    else
    {
        success = writer->insert(errorInfo, dolphindb::Util::createNullConstant(dolphindb::DT_CHAR));
    }
    ASSERT_FALSE(errorInfo.hasError()) << errorInfo.errorInfo;
    ASSERT_TRUE(success);
    writer->waitForThreadCompletion();

    auto res = pConn->run("exec * from "+table);
    ASSERT_EQ(res->rows(), 1);
    auto val = res->getColumn(0)->get(0);
    ASSERT_TRUE(val->get(0)->isNull()) << val->getString();
}

TEST_F(MultithreadedTableWriterNewTest, test_insertToinMemoryTable_threadCount_gt1)
{
    std::string case_=getCaseName();
    std::string script1 =
        "colName = [`c1];"
        "colType = [INT];"
        "share table(1:0, colName, colType) as "+case_;
    pConn->run(script1);
    dolphindb::ErrorCodeInfo errorInfo;
    unsigned int threadCount = 2;
    dolphindb::MTWConfig config = dolphindb::MTWConfig(pConn, case_);
    ASSERT_ANY_THROW(config.setThreads(threadCount, ""));
}

TEST_F(MultithreadedTableWriterNewTest, test_timeCol_position)
{
    std::string case_=getCaseName();
    pConn->run("try{dropStreamTable(`"+case_+")}catch(ex){};go;"
                "share streamTable(1:0, `sym`ind`val`ts, [SYMBOL, INT, DOUBLE, TIMESTAMP]) as "+case_+";"
                "setStreamTableTimestamp("+case_+", `ts)");

    dolphindb::MTWConfig config = dolphindb::MTWConfig(pConn, case_);
    // Change: timestamp should be the first config
    config.setStreamTableTimestamp().setWriteMode(dolphindb::WriteMode::Append, {}).setBatching(1, std::chrono::milliseconds(10)).setWriteMode(dolphindb::WriteMode::Append, {});
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> writer = new dolphindb::MultithreadedTableWriter(config);
    const int rows = 1000;
    std::vector<std::string> syms = {"APPL", "MSFT", "GOOL", "ORCL", "BABA"};
    std::vector<int> inds = {0, -10823, 94, 305, 19};
    std::vector<double> vals = {0.0, -2.394, 99.2016, 50.002, 9};
    dolphindb::ErrorCodeInfo err;
    for (auto i = 0; i < rows; i++)
    {
        if (!writer->insert(err, syms[rand() % 5], inds[rand() % 5], vals[rand() % 5]))
            throw dolphindb::RuntimeException(err.errorInfo);
    }

    writer->waitForThreadCompletion();
    ASSERT_EQ(pConn->run(case_+".rows()")->getInt(), 1000);
    pConn->run("try{dropStreamTable(`"+case_+")}catch(ex){};");
}

TEST_F(MultithreadedTableWriterNewTest, test_insertTable_arrayVector)
{
    std::string case_=getCaseName();
    dolphindb::TableSP table1 = pConn->run(R"(
        cindex = [0]
        cbool= array(BOOL[]).append!([(0..8).append!(NULL)]);
        cchar = array(CHAR[]).append!([(0..8).append!(NULL)]);
        cshort = array(SHORT[]).append!([(0..8).append!(NULL)]);
        cint = array(INT[]).append!([(0..8).append!(NULL)]);
        clong = array(LONG[]).append!([(0..8).append!(NULL)]);
        cdate = array(DATE[]).append!([(0..8).append!(NULL)]);
        cmonth = array(MONTH[]).append!([(0..8).append!(NULL)]);
        ctime = array(TIME[]).append!([(0..8).append!(NULL)]);
        cminute = array(MINUTE[]).append!([(0..8).append!(NULL)]);
        csecond = array(SECOND[]).append!([(0..8).append!(NULL)]);
        cdatetime = array(DATETIME[]).append!([(0..8).append!(NULL)]);
        ctimestamp = array(TIMESTAMP[]).append!([(0..8).append!(NULL)]);
        cnanotime = array(NANOTIME[]).append!([(0..8).append!(NULL)]);
        cnanotimestamp = array(NANOTIMESTAMP[]).append!([(0..8).append!(NULL)]);
        cdatehour = array(DATEHOUR[]).append!([(0..8).append!(NULL)]);
        cfloat = array(FLOAT[]).append!([(0..8).append!(NULL)]);
        cdouble = array(DOUBLE[]).append!([(0..8).append!(NULL)]);
        cipaddr = array(IPADDR[]).append!([(take(ipaddr(['192.168.1.13']),9)).append!(NULL)]);
        cuuid = array(UUID[]).append!([(take(uuid(['5d212a78-cc48-e3b1-4235-b4d91473ee87']),9)).append!(NULL)]);
        cint128 = array(INT128[]).append!([(take(int128(['e1671797c52e15f763380b45e841ec32']),9)).append!(NULL)]);
        cdecimal32 = array(DECIMAL32(6)[], 0, 10).append!(decimal32([(0..8).append!(NULL)], 6));
        cdecimal64 = array(DECIMAL64(16)[], 0, 10).append!(decimal64([(0..8).append!(NULL)], 16));
        cdecimal128 = array(DECIMAL128(26)[], 0, 10).append!(decimal128([(0..8).append!(NULL)], 26));
        table1=table(cindex, cchar,cbool,cshort,cint,clong,cdate,cmonth,ctime,cminute,cdatetime,csecond,ctimestamp,cnanotime,cnanotimestamp,cdatehour,cfloat,cdouble,cuuid,cipaddr,cint128,cdecimal32,cdecimal64,cdecimal128);
        for (i in 1:100){
            tableInsert(table1, i, cchar,cbool,cshort,cint,clong,cdate,cmonth,ctime,cminute,cdatetime,csecond,ctimestamp,cnanotime,cnanotimestamp,cdatehour,cfloat,cdouble,cuuid,cipaddr,cint128,cdecimal32,cdecimal64,cdecimal128)}
        tableInsert(table1,100, NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
        table1
    )");
    pConn->run("tmp = select top 0* from table1;share tmp as "+case_);

    dolphindb::MTWConfig config = dolphindb::MTWConfig(pConn, case_);
    config.setWriteMode(dolphindb::WriteMode::Append, {}).setThreads(1, "cindex");
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtw = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;

    for (auto i = 0; i < table1->rows(); i++)
    {
        if (!mtw->insert(pErrorInfo, table1->getColumn(0)->get(i), table1->getColumn(1)->get(i), table1->getColumn(2)->get(i), table1->getColumn(3)->get(i), table1->getColumn(4)->get(i), table1->getColumn(5)->get(i), table1->getColumn(6)->get(i), table1->getColumn(7)->get(i), table1->getColumn(8)->get(i), table1->getColumn(9)->get(i), table1->getColumn(10)->get(i), table1->getColumn(11)->get(i), table1->getColumn(12)->get(i), table1->getColumn(13)->get(i), table1->getColumn(14)->get(i), table1->getColumn(15)->get(i), table1->getColumn(16)->get(i), table1->getColumn(17)->get(i), table1->getColumn(18)->get(i), table1->getColumn(19)->get(i), table1->getColumn(20)->get(i), table1->getColumn(21)->get(i), table1->getColumn(22)->get(i), table1->getColumn(23)->get(i)))
        {
            std::cout << "insert failed, reason: " << pErrorInfo.errorInfo << std::endl;
            break;
        }
    }
    mtw->waitForThreadCompletion();

    ASSERT_TRUE(pConn->run(
                        "res = select * from "+case_+" order by cindex;"
                        "ex = select * from table1 order by cindex;"
                        "all(each(eqObj, res.values(), ex.values()))")
                    ->getBool());
}

TEST_F(MultithreadedTableWriterNewTest, MTW_with_SCRAM_user){
    std::string case_=getCaseName();
    std::string userName=getRandString(20);
    pConn->run(
        "userName='"+userName+"';"
        "try{deleteUser(userName)}catch(ex){};go;createUser(userName, `123456, authMode='scram')"
    );
    std::shared_ptr<dolphindb::DBConnection> conn_scram = std::make_shared<dolphindb::DBConnection>(false, false, 7200, false, false, false, true);
    conn_scram->connect(HOST, PORT, userName, "123456");

    dolphindb::TableSP data = conn_scram->run("t = table(1 2 3 as c1, rand(100.00, 3) as c2);share table(1:0, `c1`c2, [INT, DOUBLE]) as "+case_+"; t");
    dolphindb::MTWConfig config = dolphindb::MTWConfig(conn_scram, case_);
    config.setThreads(1, "c1");
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;

    for (auto i = 0; i < data->rows(); i++){
        if (!mtwsp->insert(pErrorInfo, data->getColumn(0)->get(i), data->getColumn(1)->get(i))){
            std::cout << "insert failed, reason: " << pErrorInfo.errorInfo<< std::endl;
            break;
        }
    }
    mtwsp->waitForThreadCompletion();
    ASSERT_TRUE(conn_scram->run("res = select * from "+case_+" order by c1;ex = select * from t order by c1;all(each(eqObj, res.values(), ex.values()))")->getBool());
    conn_scram->close();
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorMemoryTable_in_vector)
{
    std::string case_=getCaseName();
    std::string script = "share table(1000:0, `bool`char`short`long`date`month`datetime`second`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`blob`decimal32`decimal64`decimal128`datehour`minute`time,"
                    "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,DATETIME,SECOND,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,BLOB,DECIMAL32(2),DECIMAL64(6),DECIMAL128(8),DATEHOUR,MINUTE,TIME]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    int data[] = {0, 1};
    for (int i = 0; i < 1024; i++)
    {
        std::vector<dolphindb::ConstantSP> datas;
        datas.emplace_back(dolphindb::Util::createBool((char)(data[i % 2])));
        datas.emplace_back(dolphindb::Util::createChar(i * 2));
        datas.emplace_back(dolphindb::Util::createShort(i + 12));
        datas.emplace_back(dolphindb::Util::createLong((long)i * 100));
        datas.emplace_back(dolphindb::Util::createDate(i + 432));
        datas.emplace_back(dolphindb::Util::createMonth(i + 21));
        datas.emplace_back(dolphindb::Util::createDateTime(i * 192));
        datas.emplace_back(dolphindb::Util::createSecond(i % 86399));
        datas.emplace_back(dolphindb::Util::createTimestamp((long long)i * 2342));
        datas.emplace_back(dolphindb::Util::createNanoTime((long long)i * 4214));
        datas.emplace_back(dolphindb::Util::createNanoTimestamp((long long)i * 4264));
        datas.emplace_back(dolphindb::Util::createFloat(i * 42.64));
        datas.emplace_back(dolphindb::Util::createDouble(i * 4.264));
        datas.emplace_back(dolphindb::Util::createString(std::to_string(i % 10)));
        datas.emplace_back(dolphindb::Util::createString("A" + std::to_string(i % 10)));
        datas.emplace_back(dolphindb::Util::parseConstant(dolphindb::DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100"));
        datas.emplace_back(dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.2." + std::to_string(i % 255)));
        datas.emplace_back(dolphindb::Util::parseConstant(dolphindb::DT_INT128, "0f0e0d0c0b0a09080706050403020100"));
        datas.emplace_back(dolphindb::Util::createInt(i));
        datas.emplace_back(dolphindb::Util::createBlob("0f0e0d0c0b0a0908070605040302010" + std::to_string(i)));
        datas.emplace_back(dolphindb::Util::createDecimal32(2, i / 100));
        datas.emplace_back(dolphindb::Util::createDecimal64(6, i / 1000000));
        datas.emplace_back(dolphindb::Util::createDecimal128(8, i / 100000000));
        datas.emplace_back(dolphindb::Util::createDateHour(i + 21));
        datas.emplace_back(dolphindb::Util::createMinute(i + 21));
        datas.emplace_back(dolphindb::Util::createTime(i + 21));
        mtwsp->insert(datas, pErrorInfo);
    }
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run("select * from "+case_+" order by long limit 1024;");
    for (int i = 0; i < 1024; i++)
    {
        ASSERT_EQ(t1->getColumn(0)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createBool((char)(data[i % 2])))->getString());
        ASSERT_EQ(t1->getColumn(1)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createChar(i * 2))->getString());
        ASSERT_EQ(t1->getColumn(2)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createShort(i + 12))->getString());
        ASSERT_EQ(t1->getColumn(3)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createLong((long)i * 100))->getString());
        ASSERT_EQ(t1->getColumn(4)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createDate(i + 432))->getString());
        ASSERT_EQ(t1->getColumn(5)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createMonth(i + 21))->getString());
        ASSERT_EQ(t1->getColumn(6)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createDateTime(i * 192))->getString());
        ASSERT_EQ(t1->getColumn(7)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createSecond(i % 86399))->getString());
        ASSERT_EQ(t1->getColumn(8)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createTimestamp((long long)i * 2342))->getString());
        ASSERT_EQ(t1->getColumn(9)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createNanoTime((long long)i * 4214))->getString());
        ASSERT_EQ(t1->getColumn(10)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createNanoTimestamp((long long)i * 4264))->getString());
        ASSERT_EQ(t1->getColumn(11)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createFloat(i * 42.64))->getString());
        ASSERT_EQ(t1->getColumn(12)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createDouble(i * 4.264))->getString());
        ASSERT_EQ(t1->getColumn(13)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createString(std::to_string(i % 10)))->getString());
        ASSERT_EQ(t1->getColumn(14)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createString("A" + std::to_string(i % 10)))->getString());
        ASSERT_EQ(t1->getColumn(15)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::parseConstant(dolphindb::DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100"))->getString());
        ASSERT_EQ(t1->getColumn(16)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.2." + std::to_string(i % 255)))->getString());
        ASSERT_EQ(t1->getColumn(17)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::parseConstant(dolphindb::DT_INT128, "0f0e0d0c0b0a09080706050403020100"))->getString());
        ASSERT_EQ(t1->getColumn(18)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createInt(i))->getString());
        ASSERT_EQ(t1->getColumn(19)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createString("0f0e0d0c0b0a0908070605040302010" + std::to_string(i)))->getString());
        ASSERT_EQ(t1->getColumn(20)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createDecimal32(2, i / 100))->getString());
        ASSERT_EQ(t1->getColumn(21)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createDecimal64(6, i / 1000000))->getString());
        ASSERT_EQ(t1->getColumn(23)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createDateHour(i + 21))->getString());
        ASSERT_EQ(t1->getColumn(24)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createMinute(i + 21))->getString());
        ASSERT_EQ(t1->getColumn(25)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createTime(i + 21))->getString());
    }
}

TEST_F(MultithreadedTableWriterNewTest, insertVectorMemoryTable_arrayVector_in_vector)
{
    std::string case_=getCaseName();
    std::string script = "share table(1000:0, `bool`char`short`long`date`month`datetime`second`timestamp`nanotime`nanotimestamp`float`double`uuid`ipaddr`int128`id`decimal32`decimal64`decimal128`datehour`minute`time,"
                    "[BOOL[],CHAR[],SHORT[],LONG[],DATE[],MONTH[],DATETIME[],SECOND[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],FLOAT[],DOUBLE[],UUID[],IPADDR[],INT128[],INT[],DECIMAL32(2)[],DECIMAL64(6)[],DECIMAL128(8)[],DATEHOUR[],MINUTE[],TIME[]]) as "+case_;
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, case_);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    // BOOL[]
    dolphindb::VectorSP bool_1 = dolphindb::Util::createVector(dolphindb::DT_BOOL,0,3);
    dolphindb::ConstantSP bool_true = dolphindb::Util::createBool(true);
    dolphindb::ConstantSP bool_false = dolphindb::Util::createBool(false);
    dolphindb::ConstantSP bool_null = dolphindb::Util::createNullConstant(dolphindb::DT_BOOL);
    bool_1->append(bool_true);
    bool_1->append(bool_false);
    bool_1->append(bool_null);
    // CHAR[]
    dolphindb::VectorSP char_1 = dolphindb::Util::createVector(dolphindb::DT_CHAR,0,3);
    dolphindb::ConstantSP char_max = dolphindb::Util::createChar(127);
    dolphindb::ConstantSP char_min = dolphindb::Util::createChar(-127);
    dolphindb::ConstantSP char_0 = dolphindb::Util::createChar(0);
    dolphindb::ConstantSP char_null = dolphindb::Util::createNullConstant(dolphindb::DT_CHAR);
    char_1->append(char_max);
    char_1->append(char_min);
    char_1->append(char_0);
    char_1->append(char_null);
    // SHORT[]
    dolphindb::VectorSP short_1 = dolphindb::Util::createVector(dolphindb::DT_SHORT,0,3);
    dolphindb::ConstantSP short_max = dolphindb::Util::createShort(32767);
    dolphindb::ConstantSP short_min = dolphindb::Util::createShort(-32767);
    dolphindb::ConstantSP short_0 = dolphindb::Util::createShort(0);
    dolphindb::ConstantSP short_null = dolphindb::Util::createNullConstant(dolphindb::DT_SHORT);
    short_1->append(short_max);
    short_1->append(short_min);
    short_1->append(short_0);
    short_1->append(short_null);
    // LONG[]
    dolphindb::VectorSP long_1 = dolphindb::Util::createVector(dolphindb::DT_LONG,0,3);
    dolphindb::ConstantSP long_max = dolphindb::Util::createLong(9223372036854775807);
    dolphindb::ConstantSP long_min = dolphindb::Util::createLong(-9223372036854775807);
    dolphindb::ConstantSP long_0 = dolphindb::Util::createLong(0);
    dolphindb::ConstantSP long_null = dolphindb::Util::createNullConstant(dolphindb::DT_LONG);
    long_1->append(long_max);
    long_1->append(long_min);
    long_1->append(long_0);
    long_1->append(long_null);
    // DATE[]
    dolphindb::VectorSP date_1 = dolphindb::Util::createVector(dolphindb::DT_DATE,0,2);
    dolphindb::ConstantSP date_0 = dolphindb::Util::createDate(0);
    dolphindb::ConstantSP date_ = dolphindb::Util::createDate(19132);
    dolphindb::ConstantSP date_null = dolphindb::Util::createNullConstant(dolphindb::DT_DATE);
    date_1->append(date_0);
    date_1->append(date_);
    date_1->append(date_null);
    // MONTH[]
    dolphindb::VectorSP month_1 = dolphindb::Util::createVector(dolphindb::DT_MONTH,0,2);
    dolphindb::ConstantSP month_0 = dolphindb::Util::createMonth(0);
    dolphindb::ConstantSP month_ = dolphindb::Util::createMonth(23640);
    dolphindb::ConstantSP month_null = dolphindb::Util::createNullConstant(dolphindb::DT_MONTH);
    month_1->append(month_0);
    month_1->append(month_);
    month_1->append(month_null);
    // DATETIME[]
    dolphindb::VectorSP datetime_1 = dolphindb::Util::createVector(dolphindb::DT_DATETIME,0,2);
    dolphindb::ConstantSP datetime_0 = dolphindb::Util::createDateTime(0);
    dolphindb::ConstantSP datetime_ = dolphindb::Util::createDateTime(1);
    dolphindb::ConstantSP datetime_null = dolphindb::Util::createNullConstant(dolphindb::DT_DATETIME);
    datetime_1->append(datetime_0);
    datetime_1->append(datetime_);
    datetime_1->append(datetime_null);
    // SECOND[]
    dolphindb::VectorSP second_1 = dolphindb::Util::createVector(dolphindb::DT_SECOND,0,2);
    dolphindb::ConstantSP second_0 = dolphindb::Util::createSecond(0);
    dolphindb::ConstantSP second_ = dolphindb::Util::createSecond(1);
    dolphindb::ConstantSP second_null = dolphindb::Util::createNullConstant(dolphindb::DT_SECOND);
    second_1->append(second_0);
    second_1->append(second_);
    second_1->append(second_null);
    // TIMESTAMP[]
    dolphindb::VectorSP timestamp_1 = dolphindb::Util::createVector(dolphindb::DT_TIMESTAMP,0,2);
    dolphindb::ConstantSP timestamp_0 = dolphindb::Util::createTimestamp(0);
    dolphindb::ConstantSP timestamp_ = dolphindb::Util::createTimestamp(1);
    dolphindb::ConstantSP timestamp_null = dolphindb::Util::createNullConstant(dolphindb::DT_TIMESTAMP);
    timestamp_1->append(timestamp_0);
    timestamp_1->append(timestamp_);
    timestamp_1->append(timestamp_null);
    // NANOTIME[]
    dolphindb::VectorSP nanotime_1 = dolphindb::Util::createVector(dolphindb::DT_NANOTIME,0,2);
    dolphindb::ConstantSP nanotime_0 = dolphindb::Util::createNanoTime(0);
    dolphindb::ConstantSP nanotime_ = dolphindb::Util::createNanoTime(1);
    dolphindb::ConstantSP nanotime_null = dolphindb::Util::createNullConstant(dolphindb::DT_NANOTIME);
    nanotime_1->append(nanotime_0);
    nanotime_1->append(nanotime_);
    nanotime_1->append(nanotime_null);
    // NANOTIMESTAMP[]
    dolphindb::VectorSP nanotimestamp_1 = dolphindb::Util::createVector(dolphindb::DT_NANOTIMESTAMP,0,2);
    dolphindb::ConstantSP nanotimestamp_0 = dolphindb::Util::createNanoTimestamp(0);
    dolphindb::ConstantSP nanotimestamp_ = dolphindb::Util::createNanoTimestamp(1);
    dolphindb::ConstantSP nanotimestamp_null = dolphindb::Util::createNullConstant(dolphindb::DT_NANOTIMESTAMP);
    nanotimestamp_1->append(nanotimestamp_0);
    nanotimestamp_1->append(nanotimestamp_);
    nanotimestamp_1->append(nanotimestamp_null);
    // FLOAT[]
    dolphindb::VectorSP float_1 = dolphindb::Util::createVector(dolphindb::DT_FLOAT,0,2);
    dolphindb::ConstantSP float_ = dolphindb::Util::createFloat(3.14);
    dolphindb::ConstantSP float_nan = dolphindb::Util::createFloat(NAN);
    dolphindb::ConstantSP float_inf = dolphindb::Util::createFloat(INFINITY);
    dolphindb::ConstantSP float_null = dolphindb::Util::createNullConstant(dolphindb::DT_FLOAT);
    float_1->append(float_);
    float_1->append(float_nan);
    float_1->append(float_inf);
    float_1->append(float_null);
    // DOUBLE[]
    dolphindb::VectorSP double_1 = dolphindb::Util::createVector(dolphindb::DT_DOUBLE,0,2);
    dolphindb::ConstantSP double_ = dolphindb::Util::createDouble(3.14);
    dolphindb::ConstantSP double_nan = dolphindb::Util::createDouble(NAN);
    dolphindb::ConstantSP double_inf = dolphindb::Util::createDouble(INFINITY);
    dolphindb::ConstantSP double_null = dolphindb::Util::createNullConstant(dolphindb::DT_DOUBLE);
    double_1->append(double_);
    double_1->append(double_nan);
    double_1->append(double_inf);
    double_1->append(double_null);
    // UUID[]
    dolphindb::VectorSP uuid_1 = dolphindb::Util::createVector(dolphindb::DT_UUID,0,2);
    dolphindb::ConstantSP uuid_normal = dolphindb::Util::parseConstant(dolphindb::DT_UUID,"5d212a78-cc48-e3b1-4235-b4d91473ee87");
    dolphindb::ConstantSP uuid_null = dolphindb::Util::createNullConstant(dolphindb::DT_UUID);
    uuid_1->append(uuid_normal);
    uuid_1->append(uuid_normal);
    uuid_1->append(uuid_normal);
    // IPADDR[]
    dolphindb::VectorSP ip_1 = dolphindb::Util::createVector(dolphindb::DT_IP,0,2);
    dolphindb::ConstantSP ip_normal = dolphindb::Util::parseConstant(dolphindb::DT_IP,"127.0.0.1");
    dolphindb::ConstantSP ip_null = dolphindb::Util::createNullConstant(dolphindb::DT_IP);
    ip_1->append(ip_normal);
    ip_1->append(ip_normal);
    ip_1->append(ip_normal);
    // INT128[]
    dolphindb::VectorSP int128_1 = dolphindb::Util::createVector(dolphindb::DT_INT128,0,2);
    dolphindb::ConstantSP int128_normal = dolphindb::Util::parseConstant(dolphindb::DT_INT128,"e1671797c52e15f763380b45e841ec32");
    dolphindb::ConstantSP int128_null = dolphindb::Util::createNullConstant(dolphindb::DT_INT128);
    int128_1->append(int128_normal);
    int128_1->append(int128_normal);
    int128_1->append(int128_normal);
    // INT[]
    dolphindb::VectorSP int_1 = dolphindb::Util::createVector(dolphindb::DT_INT,0,3);
    dolphindb::ConstantSP int_max = dolphindb::Util::createInt(2147483647);
    dolphindb::ConstantSP int_min = dolphindb::Util::createInt(-2147483647);
    dolphindb::ConstantSP int_0 = dolphindb::Util::createInt(0);
    dolphindb::ConstantSP int_null = dolphindb::Util::createNullConstant(dolphindb::DT_INT);
    int_1->append(int_max);
    int_1->append(int_min);
    int_1->append(int_0);
    int_1->append(int_null);
    // DECIMAL32(2)[]
    dolphindb::VectorSP decimal32_1 = dolphindb::Util::createVector(dolphindb::DT_DECIMAL32,0,2,true,2);
    dolphindb::ConstantSP decimal32_314 = dolphindb::Util::createDecimal32(2, 3.14);
    dolphindb::ConstantSP decimal32_315 = dolphindb::Util::createDecimal32(2, 3.15);
    dolphindb::ConstantSP decimal32_null = dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL32, 2);
    decimal32_1->append(decimal32_314);
    decimal32_1->append(decimal32_315);
    decimal32_1->append(decimal32_null);
    // DECIMAL64(6)[]
    dolphindb::VectorSP decimal64_1 = dolphindb::Util::createVector(dolphindb::DT_DECIMAL64,0,2,true,6);
    dolphindb::ConstantSP decimal64_3141592 = dolphindb::Util::createDecimal64(6, 3.141592);
    dolphindb::ConstantSP decimal64_3141593 = dolphindb::Util::createDecimal64(6, 3.141593);
    dolphindb::ConstantSP decimal64_null = dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL64, 6);
    decimal64_1->append(decimal64_3141592);
    decimal64_1->append(decimal64_3141593);
    decimal64_1->append(decimal64_null);
    // DECIMAL128(8)[]
    dolphindb::VectorSP decimal128_1 = dolphindb::Util::createVector(dolphindb::DT_DECIMAL128,0,2,true,8);
    dolphindb::ConstantSP decimal128_314159265 = dolphindb::Util::createDecimal32(8, 3.14159265);
    dolphindb::ConstantSP decimal128_314159266 = dolphindb::Util::createDecimal32(8, 3.14159266);
    dolphindb::ConstantSP decimal128_null = dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL128, 8);
    decimal128_1->append(decimal128_314159265);
    decimal128_1->append(decimal128_314159266);
    decimal128_1->append(decimal128_null);
    // DATEHOUR[]
    dolphindb::VectorSP datehour_1 = dolphindb::Util::createVector(dolphindb::DT_DATEHOUR,0,2);
    dolphindb::ConstantSP datehour_0 = dolphindb::Util::createDateHour(0);
    dolphindb::ConstantSP datehour_ = dolphindb::Util::createDateHour(1);
    dolphindb::ConstantSP datehour_null = dolphindb::Util::createNullConstant(dolphindb::DT_DATEHOUR);
    datehour_1->append(datehour_0);
    datehour_1->append(datehour_);
    datehour_1->append(datehour_null);
    // MINUTE[]
    dolphindb::VectorSP minute_1 = dolphindb::Util::createVector(dolphindb::DT_MINUTE,0,2);
    dolphindb::ConstantSP minute_0 = dolphindb::Util::createMinute(0);
    dolphindb::ConstantSP minute_ = dolphindb::Util::createMinute(1);
    dolphindb::ConstantSP minute_null = dolphindb::Util::createNullConstant(dolphindb::DT_MINUTE);
    minute_1->append(minute_0);
    minute_1->append(minute_);
    minute_1->append(minute_null);
    // TIME[]
    dolphindb::VectorSP time_1 = dolphindb::Util::createVector(dolphindb::DT_TIME,0,2);
    dolphindb::ConstantSP time_0 = dolphindb::Util::createTime(0);
    dolphindb::ConstantSP time_ = dolphindb::Util::createTime(1);
    dolphindb::ConstantSP time_null = dolphindb::Util::createNullConstant(dolphindb::DT_TIME);
    time_1->append(time_0);
    time_1->append(time_);
    time_1->append(time_null);
    std::vector<dolphindb::ConstantSP> datas;
    datas.emplace_back(bool_1);
    datas.emplace_back(char_1);
    datas.emplace_back(short_1);
    datas.emplace_back(long_1);
    datas.emplace_back(date_1);
    datas.emplace_back(month_1);
    datas.emplace_back(datetime_1);
    datas.emplace_back(second_1);
    datas.emplace_back(timestamp_1);
    datas.emplace_back(nanotime_1);
    datas.emplace_back(nanotimestamp_1);
    datas.emplace_back(float_1);
    datas.emplace_back(double_1);
    datas.emplace_back(uuid_1);
    datas.emplace_back(ip_1);
    datas.emplace_back(int128_1);
    datas.emplace_back(int_1);
    datas.emplace_back(decimal32_1);
    datas.emplace_back(decimal64_1);
    datas.emplace_back(decimal128_1);
    datas.emplace_back(datehour_1);
    datas.emplace_back(minute_1);
    datas.emplace_back(time_1);
    for (int i = 0; i < 1024; i++)
        mtwsp->insert(datas, pErrorInfo);
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run("select * from "+case_+" limit 1024;");
    for (int i = 0; i < 1024; i++)
    {
        ASSERT_EQ(t1->getColumn(0)->getRow(i)->getString(), datas[0]->getString());
        ASSERT_EQ(t1->getColumn(1)->getRow(i)->getString(), datas[1]->getString());
        ASSERT_EQ(t1->getColumn(2)->getRow(i)->getString(), datas[2]->getString());
        ASSERT_EQ(t1->getColumn(3)->getRow(i)->getString(), datas[3]->getString());
        ASSERT_EQ(t1->getColumn(4)->getRow(i)->getString(), datas[4]->getString());
        ASSERT_EQ(t1->getColumn(5)->getRow(i)->getString(), datas[5]->getString());
        ASSERT_EQ(t1->getColumn(6)->getRow(i)->getString(), datas[6]->getString());
        ASSERT_EQ(t1->getColumn(7)->getRow(i)->getString(), datas[7]->getString());
        ASSERT_EQ(t1->getColumn(8)->getRow(i)->getString(), datas[8]->getString());
        ASSERT_EQ(t1->getColumn(9)->getRow(i)->getString(), datas[9]->getString());
        ASSERT_EQ(t1->getColumn(10)->getRow(i)->getString(), datas[10]->getString());
        ASSERT_EQ(t1->getColumn(11)->getRow(i)->getString(), datas[11]->getString());
        ASSERT_EQ(t1->getColumn(12)->getRow(i)->getString(), datas[12]->getString());
        ASSERT_EQ(t1->getColumn(13)->getRow(i)->getString(), datas[13]->getString());
        ASSERT_EQ(t1->getColumn(14)->getRow(i)->getString(), datas[14]->getString());
        ASSERT_EQ(t1->getColumn(15)->getRow(i)->getString(), datas[15]->getString());
        ASSERT_EQ(t1->getColumn(16)->getRow(i)->getString(), datas[16]->getString());
        ASSERT_EQ(t1->getColumn(17)->getRow(i)->getString(), datas[17]->getString());
        ASSERT_EQ(t1->getColumn(18)->getRow(i)->getString(), datas[18]->getString());
        ASSERT_EQ(t1->getColumn(19)->getRow(i)->getString(), datas[19]->getString());
        ASSERT_EQ(t1->getColumn(20)->getRow(i)->getString(), datas[20]->getString());
        ASSERT_EQ(t1->getColumn(21)->getRow(i)->getString(), datas[21]->getString());
        ASSERT_EQ(t1->getColumn(22)->getRow(i)->getString(), datas[22]->getString());
    }
}

#ifdef __linux__

TEST_F(MultithreadedTableWriterNewTest, insert_bind_cpu)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D);\n";
    script += "t = table(1000:0, `bool`char`short`long`date`month`datetime`second`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`decimal32`decimal64`decimal128`datehour`minute`time,"
              "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,DATETIME,SECOND,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,DECIMAL32(2),DECIMAL64(6),DECIMAL128(8),DATEHOUR,MINUTE,TIME]);"
              "pt = db.createPartitionedTable(t,`pt,`symbol);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    config.setThreads(4, "symbol");
    std::vector<size_t> cpu{1, 2, 3, 4};
    config.setCpuIds(cpu);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    int data[] = {0, 1};
    std::string sym[] = {"A", "B", "C", "D"};
    for (int i = 0; i < 1024; i++)
    {
        std::vector<dolphindb::ConstantSP> datas;
        datas.emplace_back(dolphindb::Util::createBool((char)(data[i % 2])));
        datas.emplace_back(dolphindb::Util::createChar(i * 2));
        datas.emplace_back(dolphindb::Util::createShort(i + 12));
        datas.emplace_back(dolphindb::Util::createLong((long)i * 100));
        datas.emplace_back(dolphindb::Util::createDate(i + 432));
        datas.emplace_back(dolphindb::Util::createMonth(i + 21));
        datas.emplace_back(dolphindb::Util::createDateTime(i * 192));
        datas.emplace_back(dolphindb::Util::createSecond(((long long)i * 1932)%86400));
        datas.emplace_back(dolphindb::Util::createTimestamp((long long)i * 2342));
        datas.emplace_back(dolphindb::Util::createNanoTime((long long)i * 4214));
        datas.emplace_back(dolphindb::Util::createNanoTimestamp((long long)i * 4264));
        datas.emplace_back(dolphindb::Util::createFloat(i * 42.64));
        datas.emplace_back(dolphindb::Util::createDouble(i * 4.264));
        datas.emplace_back(dolphindb::Util::createString(sym[rand() % 4]));
        datas.emplace_back(dolphindb::Util::createString("A" + std::to_string(i)));
        // uuid,ipAddr, int128, blob
        datas.emplace_back(dolphindb::Util::parseConstant(dolphindb::DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100"));
        datas.emplace_back(dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.2." + std::to_string(i % 255)));
        datas.emplace_back(dolphindb::Util::parseConstant(dolphindb::DT_INT128, "0f0e0d0c0b0a09080706050403020100"));
        datas.emplace_back(dolphindb::Util::createInt(i));
        datas.emplace_back(dolphindb::Util::createDecimal32(2, i / 100));
        datas.emplace_back(dolphindb::Util::createDecimal64(6, i / 1000000));
        datas.emplace_back(dolphindb::Util::createDecimal128(8, i / 100000000));
        datas.emplace_back(dolphindb::Util::createDateHour(i + 21));
        datas.emplace_back(dolphindb::Util::createMinute(i + 21));
        datas.emplace_back(dolphindb::Util::createTime(i + 21));
        mtwsp->insert(datas, pErrorInfo);
    }
    mtwsp->getThreadHandles();
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by long limit 1024;");
    for (int i = 0; i < 1024; i++)
    {
        ASSERT_EQ(t1->getColumn(0)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createBool((char)(data[i % 2])))->getString());
        ASSERT_EQ(t1->getColumn(1)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createChar(i * 2))->getString());
        ASSERT_EQ(t1->getColumn(2)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createShort(i + 12))->getString());
        ASSERT_EQ(t1->getColumn(3)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createLong((long)i * 100))->getString());
        ASSERT_EQ(t1->getColumn(4)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createDate(i + 432))->getString());
        ASSERT_EQ(t1->getColumn(5)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createMonth(i + 21))->getString());
        ASSERT_EQ(t1->getColumn(6)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createDateTime(i * 192))->getString());
        ASSERT_EQ(t1->getColumn(7)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createSecond(((long long)i * 1932)%86400))->getString());
        ASSERT_EQ(t1->getColumn(8)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createTimestamp((long long)i * 2342))->getString());
        ASSERT_EQ(t1->getColumn(9)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createNanoTime((long long)i * 4214))->getString());
        ASSERT_EQ(t1->getColumn(10)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createNanoTimestamp((long long)i * 4264))->getString());
        ASSERT_EQ(t1->getColumn(11)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createFloat(i * 42.64))->getString());
        ASSERT_EQ(t1->getColumn(12)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createDouble(i * 4.264))->getString());
        ASSERT_EQ(t1->getColumn(14)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createString("A" + std::to_string(i)))->getString());
        ASSERT_EQ(t1->getColumn(15)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::parseConstant(dolphindb::DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100"))->getString());
        ASSERT_EQ(t1->getColumn(16)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.2." + std::to_string(i % 255)))->getString());
        ASSERT_EQ(t1->getColumn(17)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::parseConstant(dolphindb::DT_INT128, "0f0e0d0c0b0a09080706050403020100"))->getString());
        ASSERT_EQ(t1->getColumn(18)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createInt(i))->getString());
        ASSERT_EQ(t1->getColumn(19)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createDecimal32(2, i / 100))->getString());
        ASSERT_EQ(t1->getColumn(20)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createDecimal64(6, i / 1000000))->getString());
        ASSERT_EQ(t1->getColumn(21)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createDecimal128(8, i / 100000000))->getString());
        ASSERT_EQ(t1->getColumn(22)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createDateHour(i + 21))->getString());
        ASSERT_EQ(t1->getColumn(23)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createMinute(i + 21))->getString());
        ASSERT_EQ(t1->getColumn(24)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createTime(i + 21))->getString());
    }
}

TEST_F(MultithreadedTableWriterNewTest, insert_bind_cpu_same)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D);\n";
    script += "t = table(1000:0, `bool`char`short`long`date`month`datetime`second`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`decimal32`decimal64`decimal128`datehour`minute`time,"
              "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,DATETIME,SECOND,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,DECIMAL32(2),DECIMAL64(6),DECIMAL128(8),DATEHOUR,MINUTE,TIME]);"
              "pt = db.createPartitionedTable(t,`pt,`symbol);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    config.setThreads(4, "symbol");
    std::vector<size_t> cpu{1, 1, 2};
    config.setCpuIds(cpu);
    dolphindb::SmartPointer<dolphindb::MultithreadedTableWriter> mtwsp = new dolphindb::MultithreadedTableWriter(config);
    dolphindb::ErrorCodeInfo pErrorInfo;
    int data[] = {0, 1};
    std::string sym[] = {"A", "B", "C", "D"};
    for (int i = 0; i < 1024; i++)
    {
        std::vector<dolphindb::ConstantSP> datas;
        datas.emplace_back(dolphindb::Util::createBool((char)(data[i % 2])));
        datas.emplace_back(dolphindb::Util::createChar(i * 2));
        datas.emplace_back(dolphindb::Util::createShort(i + 12));
        datas.emplace_back(dolphindb::Util::createLong((long)i * 100));
        datas.emplace_back(dolphindb::Util::createDate(i + 432));
        datas.emplace_back(dolphindb::Util::createMonth(i + 21));
        datas.emplace_back(dolphindb::Util::createDateTime(i * 192));
        datas.emplace_back(dolphindb::Util::createSecond(((long long)i * 1932)%86400));
        datas.emplace_back(dolphindb::Util::createTimestamp((long long)i * 2342));
        datas.emplace_back(dolphindb::Util::createNanoTime((long long)i * 4214));
        datas.emplace_back(dolphindb::Util::createNanoTimestamp((long long)i * 4264));
        datas.emplace_back(dolphindb::Util::createFloat(i * 42.64));
        datas.emplace_back(dolphindb::Util::createDouble(i * 4.264));
        datas.emplace_back(dolphindb::Util::createString(sym[rand() % 4]));
        datas.emplace_back(dolphindb::Util::createString("A" + std::to_string(i)));
        // uuid,ipAddr, int128, blob
        datas.emplace_back(dolphindb::Util::parseConstant(dolphindb::DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100"));
        datas.emplace_back(dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.2." + std::to_string(i % 255)));
        datas.emplace_back(dolphindb::Util::parseConstant(dolphindb::DT_INT128, "0f0e0d0c0b0a09080706050403020100"));
        datas.emplace_back(dolphindb::Util::createInt(i));
        datas.emplace_back(dolphindb::Util::createDecimal32(2, i / 100));
        datas.emplace_back(dolphindb::Util::createDecimal64(6, i / 1000000));
        datas.emplace_back(dolphindb::Util::createDecimal128(8, i / 100000000));
        datas.emplace_back(dolphindb::Util::createDateHour(i + 21));
        datas.emplace_back(dolphindb::Util::createMinute(i + 21));
        datas.emplace_back(dolphindb::Util::createTime(i + 21));
        mtwsp->insert(datas, pErrorInfo);
    }
    mtwsp->waitForThreadCompletion();
    dolphindb::ConstantSP t1 = pConn->run("select * from loadTable(dbName,`pt) order by long limit 1024;");
    for (int i = 0; i < 1024; i++)
    {
        ASSERT_EQ(t1->getColumn(0)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createBool((char)(data[i % 2])))->getString());
        ASSERT_EQ(t1->getColumn(1)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createChar(i * 2))->getString());
        ASSERT_EQ(t1->getColumn(2)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createShort(i + 12))->getString());
        ASSERT_EQ(t1->getColumn(3)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createLong((long)i * 100))->getString());
        ASSERT_EQ(t1->getColumn(4)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createDate(i + 432))->getString());
        ASSERT_EQ(t1->getColumn(5)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createMonth(i + 21))->getString());
        ASSERT_EQ(t1->getColumn(6)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createDateTime(i * 192))->getString());
        ASSERT_EQ(t1->getColumn(7)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createSecond(((long long)i * 1932)%86400))->getString());
        ASSERT_EQ(t1->getColumn(8)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createTimestamp((long long)i * 2342))->getString());
        ASSERT_EQ(t1->getColumn(9)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createNanoTime((long long)i * 4214))->getString());
        ASSERT_EQ(t1->getColumn(10)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createNanoTimestamp((long long)i * 4264))->getString());
        ASSERT_EQ(t1->getColumn(11)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createFloat(i * 42.64))->getString());
        ASSERT_EQ(t1->getColumn(12)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createDouble(i * 4.264))->getString());
        ASSERT_EQ(t1->getColumn(14)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createString("A" + std::to_string(i)))->getString());
        ASSERT_EQ(t1->getColumn(15)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::parseConstant(dolphindb::DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100"))->getString());
        ASSERT_EQ(t1->getColumn(16)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.2." + std::to_string(i % 255)))->getString());
        ASSERT_EQ(t1->getColumn(17)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::parseConstant(dolphindb::DT_INT128, "0f0e0d0c0b0a09080706050403020100"))->getString());
        ASSERT_EQ(t1->getColumn(18)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createInt(i))->getString());
        ASSERT_EQ(t1->getColumn(19)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createDecimal32(2, i / 100))->getString());
        ASSERT_EQ(t1->getColumn(20)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createDecimal64(6, i / 1000000))->getString());
        ASSERT_EQ(t1->getColumn(21)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createDecimal128(8, i / 100000000))->getString());
        ASSERT_EQ(t1->getColumn(22)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createDateHour(i + 21))->getString());
        ASSERT_EQ(t1->getColumn(23)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createMinute(i + 21))->getString());
        ASSERT_EQ(t1->getColumn(24)->getRow(i)->getString(), ((dolphindb::ConstantSP)dolphindb::Util::createTime(i + 21))->getString());
    }
}

TEST_F(MultithreadedTableWriterNewTest, bind_cpu_id_error)
{
    std::string case_=getCaseName();
    std::string dbName="dfs://" + case_;
    std::string script = "dbName = \"" + dbName + "\"\n"
                        "if(exists(dbName)){\n"
                        "\tdropDatabase(dbName)\t\n"
                        "}\n"
                        "db  = database(dbName, VALUE,`A`B`C`D);\n";
    script += "t = table(1000:0, `bool`char`short`long`date`month`datetime`second`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`decimal32`decimal64`decimal128`datehour`minute`time,"
              "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,DATETIME,SECOND,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,DECIMAL32(2),DECIMAL64(6),DECIMAL128(8),DATEHOUR,MINUTE,TIME]);"
              "pt = db.createPartitionedTable(t,`pt,`symbol);";
    pConn->run(script);
    dolphindb::MTWConfig config(pConn, dolphindb::TableScript(dbName, "pt"));
    config.setThreads(4, "symbol");
    std::vector<size_t> cpu{1, 2, 3, 100};
    ASSERT_ANY_THROW(config.setCpuIds(cpu));
}

#endif