class MultithreadedTableWriterTest : public testing::Test
{
protected:
	// Suite
	static void SetUpTestCase()
	{
		// DBConnection conn;
		conn.initialize();
		bool ret = conn.connect(hostName, port, "admin", "123456");
		if (!ret)
		{
			cout << "Failed to connect to the server" << endl;
		}
		else
		{
			cout << "connect to " + hostName + ":" + std::to_string(port) << endl;
		}
	}
	static void TearDownTestCase()
	{
		conn.close();
	}

	// Case
	virtual void SetUp()
	{
		cout << "check connect...";
		ConstantSP res = conn.run("1+1");

		cout << "ok" << endl;
	}
	virtual void TearDown()
	{
		conn.run("undef all;");
	}
};

TEST_F(MultithreadedTableWriterTest, hostNameNULL)
{
	//
	string script = "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	// mulwrite = new MultithreadedTableWriter("",port, "admin","123456","","t1",false,10000,0.1f,5,"sym");
	EXPECT_ANY_THROW(new MultithreadedTableWriter("", port, "admin", "123456", "", "t1", false));
}

TEST_F(MultithreadedTableWriterTest, hostNameError)
{
	//
	string script = "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	// mulwrite = new MultithreadedTableWriter("192.178.1.321",port, "admin","123456","","t1",false,10000,0.1f,5,"sym");
	EXPECT_ANY_THROW(new MultithreadedTableWriter("192.178.1.321", port, "admin", "123456", "", "t1", false));
}

TEST_F(MultithreadedTableWriterTest, PortError)
{
	//
	string script = "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	// mulwrite = new MultithreadedTableWriter("192.178.1.321",port, "admin","123456","","t1",false,10000,0.1f,5,"sym");
	EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, 848, "admin", "123456", "", "t1", false));
}

TEST_F(MultithreadedTableWriterTest, userIdNull)
{
	//
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\";"
					"if(exists(\"dfs://test_MultithreadedTableWriter\")){\n"
					"\tdropDatabase(\"dfs://test_MultithreadedTableWriter\");\t\n"
					"}"
					"db = database(\"dfs://test_MultithreadedTableWriter\", VALUE,`A`B`C`D);"
					"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"pt = db.createPartitionedTable(t,`pt,`sym);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "", "123456", dbName, "pt", false));
}

TEST_F(MultithreadedTableWriterTest, userIdError)
{
	//
	string script = "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	// mulwrite = new MultithreadedTableWriter("192.178.1.321",port, "admin","123456","","t1",false,10000,0.1f,5,"sym");
	EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "user1", "123456", "", "t1", false));
}

TEST_F(MultithreadedTableWriterTest, passwordNull)
{
	//
	string script = "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	// mulwrite = new MultithreadedTableWriter("192.178.1.321",port, "admin","123456","","t1",false,10000,0.1f,5,"sym");

	EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "admin", "", "", "t1", false));
}

TEST_F(MultithreadedTableWriterTest, passwordError)
{
	//
	string script = "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	// mulwrite = new MultithreadedTableWriter("192.178.1.321",port, "admin","123456","","t1",false,10000,0.1f,5,"sym");
	EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "admin", "1234", "", "t1", false));
}

TEST_F(MultithreadedTableWriterTest, dbNameNULL)
{
	//
	string script = "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	// mulwrite = new MultithreadedTableWriter("192.178.1.321",port, "admin","123456","","t1",false,10000,0.1f,5,"sym");
	EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "", false));
}

TEST_F(MultithreadedTableWriterTest, dbNameError)
{
	//
	string script = "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	// mulwrite = new MultithreadedTableWriter("192.178.1.321",port, "admin","123456","","t1",false,10000,0.1f,5,"sym");
	EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "admin", "123456", "t2", "", false));
}

TEST_F(MultithreadedTableWriterTest, tableNameNULL)
{
	//
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\";"
					"if(exists(\"dfs://test_MultithreadedTableWriter\")){\n"
					"\tdropDatabase(\"dfs://test_MultithreadedTableWriter\");\t\n"
					"}"
					"db = database(\"dfs://test_MultithreadedTableWriter\", VALUE,`A`B`C`D);"
					"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"pt = db.createPartitionedTable(t,`pt,`sym);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	// mulwrite = new MultithreadedTableWriter("192.178.1.321",port, "admin","123456","","t1",false,10000,0.1f,5,"sym");
	EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "admin", "123456", "dbName", "", false));
	conn.run("dropDatabase(\"dfs://test_MultithreadedTableWriter\")");
}

TEST_F(MultithreadedTableWriterTest, userHasNotaccess)
{
	//
	DBConnection conn_ctl(false, false);
	conn_ctl.connect(hostName, ctl_port, "admin", "123456");
	conn_ctl.run("createUser(`user1, `123456)");

	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "login(`admin,`123456);\n"
					"dbName = \"dfs://test_MultithreadedTableWriter\";"
					"if(exists(\"dfs://test_MultithreadedTableWriter\")){\n"
					"\tdropDatabase(\"dfs://test_MultithreadedTableWriter\");\t\n"
					"}"
					"db = database(\"dfs://test_MultithreadedTableWriter\", VALUE,`A`B`C`D);"
					"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"pt = db.createPartitionedTable(t,`pt,`sym);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "user1", "123456", "dbName", "pt", false, false, nullptr, 10000, 0.1f, 5, "sym"));
	conn.run("dropDatabase(\"dfs://test_MultithreadedTableWriter\");");
	conn_ctl.run("deleteUser(`user1)");
	conn_ctl.close();
	// EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName,port, "user1","123456","dbName","pt",false,10000,0.1f,5,"sym"));
}

TEST_F(MultithreadedTableWriterTest, tableNameError)
{
	//
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\";"
					"if(exists(\"dfs://test_MultithreadedTableWriter\")){\n"
					"\tdropDatabase(\"dfs://test_MultithreadedTableWriter\");\t\n"
					"}"
					"db = database(\"dfs://test_MultithreadedTableWriter\", VALUE,`A`B`C`D);"
					"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"pt = db.createPartitionedTable(t,`pt,`sym);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	// mulwrite = new MultithreadedTableWriter(hostName,port, "admin","123456","dbName","t2",false,10000,0.1,5,"");
	EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "admin", "123456", "dbName", "t2", false));
}

// need controller.cfg with parameter "enableHTTPS=true"
TEST_F(MultithreadedTableWriterTest, useSSL)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\";"
					"if(exists(\"dfs://test_MultithreadedTableWriter\")){\n"
					"\tdropDatabase(\"dfs://test_MultithreadedTableWriter\");\t\n"
					"}"
					"db = database(\"dfs://test_MultithreadedTableWriter\", VALUE,`A`B`C`D);"
					"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"pt = db.createPartitionedTable(t,`pt,`sym);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", true, false, nullptr, 1, 0.1, 5, "sym");
	string sym[] = {"A", "B", "C", "D"};
	vector<vector<ConstantSP> *> datas;
	srand((int)time(NULL));
	for (int i = 0; i < 1000; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createString(sym[rand() % 4]));
		row.push_back(Util::createInt(i * 12));
		row.push_back(Util::createInt(i + 64));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	mulwrite->waitForThreadCompletion();

	MultithreadedTableWriter::Status status;
	mulwrite->getStatus(status);
	cout << status.errorInfo << endl;

	vector<vector<ConstantSP> *> unwriteDatas;
	mulwrite->getUnwrittenData(unwriteDatas);
	EXPECT_TRUE(unwriteDatas.empty());
	EXPECT_EQ(unwriteDatas.size(), 0);

	ConstantSP pt_rows = conn.run("exec count(*) from loadTable(\"dfs://test_MultithreadedTableWriter\",`pt)");
	EXPECT_EQ(pt_rows->getInt(), 1000);
}

TEST_F(MultithreadedTableWriterTest, batchFloat)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\";"
					"if(exists(\"dfs://test_MultithreadedTableWriter\")){\n"
					"\tdropDatabase(\"dfs://test_MultithreadedTableWriter\");\t\n"
					"}"
					"db = database(\"dfs://test_MultithreadedTableWriter\", VALUE,`A`B`C`D);"
					"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"pt = db.createPartitionedTable(t,`pt,`sym);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	// mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,-3,0.1,5,"");
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 2.5);
}

TEST_F(MultithreadedTableWriterTest, batchSizeNegtive)
{
	//
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\";"
					"if(exists(\"dfs://test_MultithreadedTableWriter\")){\n"
					"\tdropDatabase(\"dfs://test_MultithreadedTableWriter\");\t\n"
					"}"
					"db = database(\"dfs://test_MultithreadedTableWriter\", VALUE,`A`B`C`D);"
					"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"pt = db.createPartitionedTable(t,`pt,`sym);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	// mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,-3,0.1,5,"");
	EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, -4));
}

TEST_F(MultithreadedTableWriterTest, throttleNULL)
{
	//
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\";"
					"if(exists(\"dfs://test_MultithreadedTableWriter\")){\n"
					"\tdropDatabase(\"dfs://test_MultithreadedTableWriter\");\t\n"
					"}"
					"db = database(\"dfs://test_MultithreadedTableWriter\", VALUE,`A`B`C`D);"
					"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"pt = db.createPartitionedTable(t,`pt,`sym);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1000, NULL, 1);
}

TEST_F(MultithreadedTableWriterTest, throttleNegative)
{
	//
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\";"
					"if(exists(\"dfs://test_MultithreadedTableWriter\")){\n"
					"\tdropDatabase(\"dfs://test_MultithreadedTableWriter\");\t\n"
					"}"
					"db = database(\"dfs://test_MultithreadedTableWriter\", VALUE,`A`B`C`D);"
					"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"pt = db.createPartitionedTable(t,`pt,`sym);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	// mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,10000,-2.1,5,"");
	EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1000, -12));
}

TEST_F(MultithreadedTableWriterTest, threadCountNull)
{
	//
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\";"
					"if(exists(\"dfs://test_MultithreadedTableWriter\")){\n"
					"\tdropDatabase(\"dfs://test_MultithreadedTableWriter\");\t\n"
					"}"
					"db = database(\"dfs://test_MultithreadedTableWriter\", VALUE,`A`B`C`D);"
					"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"pt = db.createPartitionedTable(t,`pt,`sym);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	// mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,10000,-2.1,5,"");
	EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 10000, 2, NULL, ""));
}

TEST_F(MultithreadedTableWriterTest, threadCountZero)
{
	//
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\";"
					"if(exists(\"dfs://test_MultithreadedTableWriter\")){\n"
					"\tdropDatabase(\"dfs://test_MultithreadedTableWriter\");\t\n"
					"}"
					"db = database(\"dfs://test_MultithreadedTableWriter\", VALUE,`A`B`C`D);"
					"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"pt = db.createPartitionedTable(t,`pt,`sym);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	// mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,10000,1,0,"");
	EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 10000, 1, 0, ""));
}

TEST_F(MultithreadedTableWriterTest, threadCountNegative)
{
	//
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\";"
					"if(exists(\"dfs://test_MultithreadedTableWriter\")){\n"
					"\tdropDatabase(\"dfs://test_MultithreadedTableWriter\");\t\n"
					"}"
					"db = database(\"dfs://test_MultithreadedTableWriter\", VALUE,`A`B`C`D);"
					"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"pt = db.createPartitionedTable(t,`pt,`sym);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	// mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,10000,1,0,"");
	EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 10000, 1, -3, ""));
}

TEST_F(MultithreadedTableWriterTest, insertVectorPartitionTableLISTDateCompatiblediffType)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, VALUE, [2012.11.12 ,2012.11.13 ,2012.11.14, 2012.12.12 ,2012.12.13 ,2012.12.14]);\n";
	script += "t = table(1000:0, `symbol`date`value,[SYMBOL, DATE, INT]);"
			  "pt = db.createPartitionedTable(t,`pt,`date);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 500, 1, 5, "date");
	int date[] = {375756, 375780, 1376524, 2376475, 3376476, 1375756};
	vector<vector<ConstantSP> *> datas;
	srand((int)time(NULL));
	for (int i = 0; i < 999; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createString("A"));
		row.push_back(Util::createDateTime(date[rand() % 6]));
		row.push_back(Util::createInt(i + 64));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	cout << pErrorInfo.errorInfo;
	ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
	EXPECT_EQ(t1->size(), 0);
	mulwrite->waitForThreadCompletion();
	t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
	//    for(int i=0;i<999;i++){
	//            //cout << t1->getColumn(1)->getRow(i)->getString() << endl;
	//            EXPECT_EQ(t1->getColumn(1)->getRow(i)->getString(), datas[i]->at(1)->getString().substr(0,10));
	//    }
}

TEST_F(MultithreadedTableWriterTest, threadByColNameNullWithMemoryTable)
{
	//
	// string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"share t as t1;";
	;
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	// mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456","","t1",NULL,10000,1,5,"");
	EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 10000, 1, 5, ""));
}

TEST_F(MultithreadedTableWriterTest, threadByColNameErrorWithMemoryTable)
{
	//
	// string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	conn.run("undef(`t1,SHARED)");
	EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 10000, 1, 5, "date"));
}

TEST_F(MultithreadedTableWriterTest, threadByColNameNULLAndmutiThreadWithMemoryTable)
{
	//
	// string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	conn.run("undef(`t1,SHARED)");
	EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 10000, 1, 5, ""));
}

TEST_F(MultithreadedTableWriterTest, threadByColNameNullWithFileTable)
{
	//
	string dir = conn.run("getHomeDir()")->getString();
	replace(dir.begin(), dir.end(), '\\', '/');
	string dbName = dir + "/multitablewrite";

	string script = "dir = \"" + dir + "\";\n"
									   "if(exists(dir+\"/multitablewrite\")){\n"
									   "\tdropDatabase(dir+\"/multitablewrite\");\t\n"
									   "}"
									   "db  = database(dir+\"/multitablewrite\", VALUE,`A`B`C`D);\n"
									   "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);\n"
									   "pt = db.createPartitionedTable(t,`pt,`sym);\n"
									   "select * from loadTable(dir+\"/multitablewrite\", `pt);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 10000, 1, 1, "");
	string script2 = "if(exists(dir+\"/multitablewrite\")){\n"
					 "\tdropDatabase(dir+\"/multitablewrite\");\t\n"
					 "}";
	conn.run(script2);
}

TEST_F(MultithreadedTableWriterTest, mutiThreadWithDimensionTable)
{

	ConstantSP server_version = conn.run("version()");
	string v = server_version->getString();
	if (v.find("1.30") == 0)
	{
		cout << "server version: 1.30.xx, skip this case." << endl;
		EXPECT_EQ(1, 1);
	}
	else if (v.find("2.00") == 0)
	{
		cout << "server version: 2.00.xx, run this case." << endl;
		string dbName = "dfs://test_MultithreadedTableWriter";
		string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
						"if(exists(dbName)){\n"
						"                   dropDatabase(dbName)\n"
						"}\n"
						"db  = database(dbName, VALUE,symbol(\"A\"+string(0..10)),,`TSDB);\n"
						"t = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id,[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT])\n"
						"pt = db.createTable(t,`pt,,`symbol)";
		conn.run(script);
		SmartPointer<MultithreadedTableWriter> mulwrite;
		EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 10000, 1, 5, "sym"));
		string script2 = "if(exists(dbName)){\n"
						 "     dropDatabase(dbName)\n"
						 "}\n";
		conn.run(script2);
	}
	else
	{
		cout << "server version is not 1.30 and 2.00, please check.";
		EXPECT_EQ(1, 2);
	}
}

TEST_F(MultithreadedTableWriterTest, threadByColNameNullWithPartitionTable)
{
	//
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\";"
					"if(exists(\"dfs://test_MultithreadedTableWriter\")){\n"
					"\tdropDatabase(\"dfs://test_MultithreadedTableWriter\");\t\n"
					"}"
					"db = database(\"dfs://test_MultithreadedTableWriter\", VALUE,`A`B`C`D);"
					"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"pt = db.createPartitionedTable(t,`pt,`sym);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 10000, 1, 1, "");
}

TEST_F(MultithreadedTableWriterTest, threadByColNamedifferenceWithVectorPartitionTable)
{
	//
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\";"
					"if(exists(\"dfs://test_MultithreadedTableWriter\")){\n"
					"\tdropDatabase(\"dfs://test_MultithreadedTableWriter\");\t\n"
					"}"
					"dbData = database(,VALUE,`A`B`C`D);"
					"dbID = database(,RANGE, 0 5 10);"
					"db = database(\"dfs://test_MultithreadedTableWriter\", COMPO,[dbData,dbID]);"
					"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"pt = db.createPartitionedTable(t,`pt,`sym`id);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 10000, 1, 3, "value"));
}

TEST_F(MultithreadedTableWriterTest, threadByColNameDifferenceWithPartitionTable)
{
	//
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\";"
					"if(exists(\"dfs://test_MultithreadedTableWriter\")){\n"
					"\tdropDatabase(\"dfs://test_MultithreadedTableWriter\");\t\n"
					"}"
					"db = database(\"dfs://test_MultithreadedTableWriter\", VALUE,`A`B`C`D);"
					"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"pt = db.createPartitionedTable(t,`pt,`sym);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	EXPECT_ANY_THROW(MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 10000, 1, 3, "id"));
	// EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,10000,1,3,"id"));
}

TEST_F(MultithreadedTableWriterTest, threadByColNameNullWithCompoPartitionTable)
{
	//
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(\"dfs://test_MultithreadedTableWriter\")){\n"
					"\tdropDatabase(\"dfs://test_MultithreadedTableWriter\")\t\n"
					"}\n"
					"dbData = database(,VALUE,`A`B`C`D)\n"
					"dbID = database(,RANGE, 0 5 10)\n"
					"db = database(\"dfs://test_MultithreadedTableWriter\", COMPO,[dbData,dbID])\n"
					"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
					"pt = db.createPartitionedTable(t,`pt,`sym`id)\n"
					"select * from loadTable(dbName, `pt)";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	// mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,10000,1,3,"");
	EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 10000, 1, 3, ""));
}

TEST_F(MultithreadedTableWriterTest, threadByColNameDifferenceWithCompoPartitionTable)
{
	//
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(\"dfs://test_MultithreadedTableWriter\")){\n"
					"\tdropDatabase(\"dfs://test_MultithreadedTableWriter\")\t\n"
					"}\n"
					"dbData = database(,VALUE,`A`B`C`D)\n"
					"dbID = database(,RANGE, 0 5 10)\n"
					"db = database(\"dfs://test_MultithreadedTableWriter\", COMPO,[dbData,dbID])\n"
					"t = table(1000:0, `sym`id`value`price,[SYMBOL, INT, INT,DOUBLE])\n"
					"pt = db.createPartitionedTable(t,`pt,`sym`id)\n"
					"select * from loadTable(dbName, `pt)";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	// mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,10000,1,3,"value");
	EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 10000, 1, 3, "value"));
}

TEST_F(MultithreadedTableWriterTest, threadByColNameSameWithCompoPartitionTable_1)
{
	//
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(\"dfs://test_MultithreadedTableWriter\")){\n"
					"\tdropDatabase(\"dfs://test_MultithreadedTableWriter\")\t\n"
					"}\n"
					"dbData = database(,VALUE,`A`B`C`D)\n"
					"dbID = database(,RANGE, 0 5 10)\n"
					"db = database(\"dfs://test_MultithreadedTableWriter\", COMPO,[dbData,dbID])\n"
					"t = table(1000:0, `sym`id`value`price,[SYMBOL, INT, INT,DOUBLE])\n"
					"pt = db.createPartitionedTable(t,`pt,`sym`id)\n"
					"select * from loadTable(dbName, `pt)";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 10000, 1, 3, "id");
	// EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",false,false,nullptr,10000,1,3,"id"));
}

TEST_F(MultithreadedTableWriterTest, threadByColNameSameWithCompoPartitionTable_2)
{
	//
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(\"dfs://test_MultithreadedTableWriter\")){\n"
					"\tdropDatabase(\"dfs://test_MultithreadedTableWriter\")\t\n"
					"}\n"
					"dbData = database(,VALUE,`A`B`C`D)\n"
					"dbID = database(,RANGE, 0 5 10)\n"
					"db = database(\"dfs://test_MultithreadedTableWriter\", COMPO,[dbData,dbID])\n"
					"t = table(1000:0, `sym`id`value`price,[SYMBOL, INT, INT,DOUBLE])\n"
					"pt = db.createPartitionedTable(t,`pt,`sym`id)\n"
					"select * from loadTable(dbName, `pt)";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 10000, 1, 3, "sym");
	// EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",false,false,nullptr,10000,1,3,"id"));
}

TEST_F(MultithreadedTableWriterTest, memoryTableanddbNameNULL)
{
	//
	string script = "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	// mulwrite = new MultithreadedTableWriter(hostName,port, "admin","123456","","t1",false);
	EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "admin", "123456", NULL, "t1", false));
}
////////////////////////memory table
TEST_F(MultithreadedTableWriterTest, insertValueLongerThanMemoryTable)
{
	//
	// string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 1, 0.01, 5, "sym");
	bool flag = mulwrite->insert(pErrorInfo, "A", 12, 12, 12);
	// bool flag = mulwrite->insert(pErrorInfo.get(), "A",12,12,12);
	EXPECT_EQ(flag, false);
	EXPECT_EQ(pErrorInfo.errorInfo, "Column counts don't match 4");
}

TEST_F(MultithreadedTableWriterTest, insertVectorLongerThanMemoryTable)
{
	//
	// string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	vector<vector<ConstantSP> *> datas;
	for (int i = 0; i < 10; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createString("A"));
		row.push_back(Util::createInt(i));
		row.push_back(Util::createInt(i + 1));
		row.push_back(Util::createString(to_string(i)));
		datas.push_back(prow);
	}
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 1, 0.01, 5, "sym");
	bool flag = mulwrite->insert(pErrorInfo, datas);
	// cout<< flag<<endl;
	// cout<< pErrorInfo.errorInfo<<endl;
	vector<vector<ConstantSP> *> unwrite;
	mulwrite->waitForThreadCompletion();
	mulwrite->getUnwrittenData(unwrite);
	EXPECT_EQ(flag, 0);
	EXPECT_EQ(unwrite.size(), 0);
	EXPECT_EQ(pErrorInfo.errorInfo, "Column counts don't match 1");
}

TEST_F(MultithreadedTableWriterTest, insertVectorShorterThanMemoryTable)
{
	string script = "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	vector<vector<ConstantSP> *> datas;
	for (int i = 0; i < 10; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createString("A"));
		row.push_back(Util::createInt(i));
		datas.push_back(prow);
	}
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 1, 0.01, 5, "sym");
	mulwrite->insertUnwrittenData(datas, pErrorInfo);

	MultithreadedTableWriter::Status status;
	mulwrite->getStatus(status);
	cout << status.errorInfo;
	vector<vector<ConstantSP> *> unwrite;
	mulwrite->waitForThreadCompletion();
	mulwrite->getUnwrittenData(unwrite);
	EXPECT_EQ(unwrite.size(), 10);
}

TEST_F(MultithreadedTableWriterTest, insertValuesShorterThanMemoryTable)
{
	//
	// string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 1, 0.1, 5, "sym");
	bool flag = mulwrite->insert(pErrorInfo, "A", 12);
	EXPECT_EQ(flag, false);
	EXPECT_EQ(pErrorInfo.errorInfo, "Column counts don't match 2");
}

TEST_F(MultithreadedTableWriterTest, insertValuesErrorTypeDataMemoryTable_1)
{
	//
	// string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 1, 0, 5, "sym");
	bool flag = mulwrite->insert(pErrorInfo, "A", 12, 12.9);
	EXPECT_EQ(flag, false);
	EXPECT_EQ(pErrorInfo.errorInfo, "Cannot convert double to INT");
}

TEST_F(MultithreadedTableWriterTest, insertValuesMemoryTable)
{
	//
	// string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 1, 0, 5, "sym");
	bool flag = mulwrite->insert(pErrorInfo, "A", 12, 12);
	EXPECT_EQ(flag, true);
	mulwrite->waitForThreadCompletion();
	ConstantSP t1 = conn.run("select * from t1");
	EXPECT_EQ((t1->getRow(0)->getString() == "value->12\nsym->A\nid->12\n") || (t1->getRow(0)->getString() == "value->12\nid->12\nsym->A\n"), true);
	//    cout << pErrorInfo->errorCode << "\n";
	//    cout << pErrorInfo->errorInfo;
}

TEST_F(MultithreadedTableWriterTest, insertValuesMemoryTableDifferentTypeData)
{
	//
	// string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "t = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`blob,"
					"[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,BLOB]);"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	unsigned char data[16] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 1, 0.1, 5, "symbol");
	bool flag = mulwrite->insert(pErrorInfo, false, char(12), (short)12, (long)12, Util::createDate(2012, 11, 12), Util::createMonth(2012, 11), Util::createSecond(134),
								 Util::createDateTime(2012, 11, 13, 6, 12, 12), Util::createTimestamp(43241), Util::createNanoTime(532432),
								 Util::createNanoTimestamp(85932494), 12.4f, 3412.3, "AAPL", "A", data, data, data, 12, "0f0e0d0c0b0a09080706050403020100");
	// EXPECT_EQ(flag, true);
	mulwrite->waitForThreadCompletion();
	ConstantSP t1 = conn.run("select * from t1;");
	EXPECT_EQ(t1->getColumn(0)->getRow(0)->getString(), "0");
	EXPECT_EQ(t1->getColumn(1)->getRow(0)->getString(), "12");
	EXPECT_EQ(t1->getColumn(2)->getRow(0)->getString(), "12");
	EXPECT_EQ(t1->getColumn(3)->getRow(0)->getString(), "12");
	EXPECT_EQ(t1->getColumn(4)->getRow(0)->getString(), "2012.11.12");
	EXPECT_EQ(t1->getColumn(5)->getRow(0)->getString(), "2012.11M");
	EXPECT_EQ(t1->getColumn(6)->getRow(0)->getString(), "00:02:14");
	EXPECT_EQ(t1->getColumn(7)->getRow(0)->getString(), Util::createDateTime(2012, 11, 13, 6, 12, 12)->getString());
	EXPECT_EQ(t1->getColumn(8)->getRow(0)->getString(), Util::createTimestamp(43241)->getString());
	EXPECT_EQ(t1->getColumn(9)->getRow(0)->getString(), Util::createNanoTime(532432)->getString());
	EXPECT_EQ(t1->getColumn(10)->getRow(0)->getString(), Util::createNanoTimestamp(85932494)->getString());
	EXPECT_EQ(t1->getColumn(11)->getRow(0)->getString(), "12.4");
	EXPECT_EQ(t1->getColumn(12)->getRow(0)->getString(), "3412.3");
	EXPECT_EQ(t1->getColumn(13)->getRow(0)->getString(), "AAPL");
	EXPECT_EQ(t1->getColumn(14)->getRow(0)->getString(), "A");
	EXPECT_EQ(t1->getColumn(15)->getRow(0)->getString(), "0f0e0d0c-0b0a-0908-0706-050403020100");
	EXPECT_EQ(t1->getColumn(16)->getRow(0)->getString(), "f0e:d0c:b0a:908:706:504:302:100");
	EXPECT_EQ(t1->getColumn(17)->getRow(0)->getString(), "0f0e0d0c0b0a09080706050403020100");
	EXPECT_EQ(t1->getColumn(19)->getRow(0)->getString(), "0f0e0d0c0b0a09080706050403020100");
	conn.run("undef(`t1,SHARED);");
}

TEST_F(MultithreadedTableWriterTest, insertVectorMemoryTableDifferentTypeDataLessThan256)
{
	//
	string script = "t = table(1000:0, `bool`char`short`long`date`month`datetime`second`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`blob,"
					"[BOOL,CHAR,SHORT,LONG,DATE,MONTH,DATETIME,SECOND,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,BLOB]);"
					"share t as t1;";
	conn.run(script);
	Util::sleep(3000);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	int data[] = {0, 1};
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 1, 0.1, 5, "symbol");
	vector<vector<ConstantSP> *> datas;
	for (int i = 0; i < 10; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createBool(data[i % 2]));
		row.push_back(Util::createChar(i * 2));
		row.push_back(Util::createShort(i + 12));
		row.push_back(Util::createLong((long)i * 100));
		row.push_back(Util::createDate(i + 432));
		row.push_back(Util::createMonth(i + 21));
		row.push_back(Util::createDateTime(i * 192));
		row.push_back(Util::createSecond((long long)i * 1932));
		row.push_back(Util::createTimestamp((long long)i * 2342));
		row.push_back(Util::createNanoTime((long long)i * 4214));
		row.push_back(Util::createNanoTimestamp((long long)i * 4264));
		row.push_back(Util::createFloat(i * 42.64));
		row.push_back(Util::createDouble(i * 4.264));
		row.push_back(Util::createString(std::to_string(i)));
		row.push_back(Util::createString("A" + to_string(i)));
		// uuid,ipAddr, int128, blob
		row.push_back(Util::parseConstant(DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100"));
		row.push_back(Util::parseConstant(DT_IP, "192.168.2." + to_string(i)));
		row.push_back(Util::parseConstant(DT_INT128, "0f0e0d0c0b0a09080706050403020100"));
		row.push_back(Util::createInt(i));
		row.push_back(Util::createBlob("0f0e0d0c0b0a0908070605040302010" + to_string(i)));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	mulwrite->waitForThreadCompletion();
	ConstantSP t1 = conn.run("select * from t1 order by long;");
	for (int i = 0; i < 10; i++)
	{
		EXPECT_EQ(t1->getColumn(0)->getRow(i)->getString(), Util::createBool(data[i % 2])->getString());
		EXPECT_EQ(t1->getColumn(1)->getRow(i)->getString(), Util::createChar(i * 2)->getString());
		EXPECT_EQ(t1->getColumn(2)->getRow(i)->getString(), Util::createShort(i + 12)->getString());
		EXPECT_EQ(t1->getColumn(3)->getRow(i)->getString(), Util::createLong((long)i * 100)->getString());
		EXPECT_EQ(t1->getColumn(4)->getRow(i)->getString(), Util::createDate(i + 432)->getString());
		EXPECT_EQ(t1->getColumn(5)->getRow(i)->getString(), Util::createMonth(i + 21)->getString());
		EXPECT_EQ(t1->getColumn(6)->getRow(i)->getString(), Util::createDateTime(i * 192)->getString());
		EXPECT_EQ(t1->getColumn(7)->getRow(i)->getString(), Util::createSecond((long long)i * 1932)->getString());
		EXPECT_EQ(t1->getColumn(8)->getRow(i)->getString(), Util::createTimestamp((long long)i * 2342)->getString());
		EXPECT_EQ(t1->getColumn(9)->getRow(i)->getString(), Util::createNanoTime((long long)i * 4214)->getString());
		EXPECT_EQ(t1->getColumn(10)->getRow(i)->getString(), Util::createNanoTimestamp((long long)i * 4264)->getString());
		EXPECT_EQ(t1->getColumn(11)->getRow(i)->getString(), Util::createFloat(i * 42.64)->getString());
		EXPECT_EQ(t1->getColumn(12)->getRow(i)->getString(), Util::createDouble(i * 4.264)->getString());
		EXPECT_EQ(t1->getColumn(13)->getRow(i)->getString(), Util::createString(std::to_string(i))->getString());
		EXPECT_EQ(t1->getColumn(14)->getRow(i)->getString(), Util::createString("A" + to_string(i))->getString());
		EXPECT_EQ(t1->getColumn(15)->getRow(i)->getString(), Util::parseConstant(DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100")->getString());
		EXPECT_EQ(t1->getColumn(16)->getRow(i)->getString(), Util::parseConstant(DT_IP, "192.168.2." + to_string(i))->getString());
		EXPECT_EQ(t1->getColumn(17)->getRow(i)->getString(), Util::parseConstant(DT_INT128, "0f0e0d0c0b0a09080706050403020100")->getString());
		EXPECT_EQ(t1->getColumn(18)->getRow(i)->getString(), Util::createInt(i)->getString());
		EXPECT_EQ(t1->getColumn(19)->getRow(i)->getString(), Util::createString("0f0e0d0c0b0a0908070605040302010" + to_string(i))->getString());
		//  cout << t1->getColumn(0)->getRow(i)->getString() << endl;
	}
}

TEST_F(MultithreadedTableWriterTest, insertVectorMemoryTableDifferentTypeDataBiggerThan1048576)
{
	//
	string script = "t = table(1000:0, `bool`char`short`long`date`month`datetime`second`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`blob,"
					"[BOOL,CHAR,SHORT,LONG,DATE,MONTH,DATETIME,SECOND,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,BLOB]);"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	int data[] = {0, 1};
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 1, 0.1, 5, "symbol");
	vector<vector<ConstantSP> *> datas;
	for (int i = 0; i < 3098576; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createBool(data[i % 2]));
		row.push_back(Util::createChar(i * 2));
		row.push_back(Util::createShort(i + 12));
		row.push_back(Util::createLong((long)i * 100));
		row.push_back(Util::createDate(i + 432));
		row.push_back(Util::createMonth(i + 21));
		row.push_back(Util::createDateTime(i * 192));
		row.push_back(Util::createSecond(i * 12));
		row.push_back(Util::createTimestamp((long long)i * 2342));
		row.push_back(Util::createNanoTime((long long)i * 4214));
		row.push_back(Util::createNanoTimestamp((long long)i * 4264));
		row.push_back(Util::createFloat(i * 42.64));
		row.push_back(Util::createDouble(i * 4.264));
		row.push_back(Util::createString(std::to_string(i % 10)));
		row.push_back(Util::createString("A" + to_string(i % 10)));
		// uuid,ipAddr, int128, blob
		row.push_back(Util::parseConstant(DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100"));
		row.push_back(Util::parseConstant(DT_IP, "192.168.2." + to_string(i % 255)));
		row.push_back(Util::parseConstant(DT_INT128, "0f0e0d0c0b0a09080706050403020100"));
		row.push_back(Util::createInt(i));
		row.push_back(Util::createBlob("0f0e0d0c0b0a0908070605040302010" + to_string(i)));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	mulwrite->waitForThreadCompletion();
	ConstantSP t1 = conn.run("select * from t1 order by long;");
	for (int i = 0; i < 10985; i++)
	{
		EXPECT_EQ(t1->getColumn(0)->getRow(i)->getString(), Util::createBool(data[i % 2])->getString());
		EXPECT_EQ(t1->getColumn(1)->getRow(i)->getString(), Util::createChar(i * 2)->getString());
		EXPECT_EQ(t1->getColumn(2)->getRow(i)->getString(), Util::createShort(i + 12)->getString());
		EXPECT_EQ(t1->getColumn(3)->getRow(i)->getString(), Util::createLong((long)i * 100)->getString());
		EXPECT_EQ(t1->getColumn(4)->getRow(i)->getString(), Util::createDate(i + 432)->getString());
		EXPECT_EQ(t1->getColumn(5)->getRow(i)->getString(), Util::createMonth(i + 21)->getString());
		EXPECT_EQ(t1->getColumn(6)->getRow(i)->getString(), Util::createDateTime(i * 192)->getString());
		EXPECT_EQ(t1->getColumn(7)->getRow(i)->getString(), Util::createSecond(i * 12)->getString());
		EXPECT_EQ(t1->getColumn(8)->getRow(i)->getString(), Util::createTimestamp((long long)i * 2342)->getString());
		EXPECT_EQ(t1->getColumn(9)->getRow(i)->getString(), Util::createNanoTime((long long)i * 4214)->getString());
		EXPECT_EQ(t1->getColumn(10)->getRow(i)->getString(), Util::createNanoTimestamp((long long)i * 4264)->getString());
		EXPECT_EQ(t1->getColumn(11)->getRow(i)->getString(), Util::createFloat(i * 42.64)->getString());
		EXPECT_EQ(t1->getColumn(12)->getRow(i)->getString(), Util::createDouble(i * 4.264)->getString());
		EXPECT_EQ(t1->getColumn(13)->getRow(i)->getString(), Util::createString(std::to_string(i % 10))->getString());
		EXPECT_EQ(t1->getColumn(14)->getRow(i)->getString(), Util::createString("A" + to_string(i % 10))->getString());
		EXPECT_EQ(t1->getColumn(15)->getRow(i)->getString(), Util::parseConstant(DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100")->getString());
		EXPECT_EQ(t1->getColumn(16)->getRow(i)->getString(), Util::parseConstant(DT_IP, "192.168.2." + to_string(i % 255))->getString());
		EXPECT_EQ(t1->getColumn(17)->getRow(i)->getString(), Util::parseConstant(DT_INT128, "0f0e0d0c0b0a09080706050403020100")->getString());
		EXPECT_EQ(t1->getColumn(18)->getRow(i)->getString(), Util::createInt(i)->getString());
		EXPECT_EQ(t1->getColumn(19)->getRow(i)->getString(), Util::createString("0f0e0d0c0b0a0908070605040302010" + to_string(i))->getString());
		//  cout << t1->getColumn(0)->getRow(i)->getString() << endl;
	}
	conn.run("undef(`t1,SHARED)");
}

TEST_F(MultithreadedTableWriterTest, insertVectorStreamTableDifferentTypeDataBiggerThan1048576)
{
	//
	string script = "t = streamTable(1000:0, `bool`char`short`long`date`month`datetime`second`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`blob,"
					"[BOOL,CHAR,SHORT,LONG,DATE,MONTH,DATETIME,SECOND,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,BLOB]);"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	int data[] = {0, 1};
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 1, 0.1, 5, "symbol");
	vector<vector<ConstantSP> *> datas;
	for (int i = 0; i < 3098576; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createBool(data[i % 2]));
		row.push_back(Util::createChar(i * 2));
		row.push_back(Util::createShort(i + 12));
		row.push_back(Util::createLong((long)i * 100));
		row.push_back(Util::createDate(i + 432));
		row.push_back(Util::createMonth(i + 21));
		row.push_back(Util::createDateTime(i * 192));
		row.push_back(Util::createSecond((long long)i * 1932));
		row.push_back(Util::createTimestamp((long long)i * 2342));
		row.push_back(Util::createNanoTime((long long)i * 4214));
		row.push_back(Util::createNanoTimestamp((long long)i * 4264));
		row.push_back(Util::createFloat(i * 42.64));
		row.push_back(Util::createDouble(i * 4.264));
		row.push_back(Util::createString(std::to_string(i % 10)));
		row.push_back(Util::createString("A" + to_string(i % 10)));
		// uuid,ipAddr, int128, blob
		row.push_back(Util::parseConstant(DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100"));
		row.push_back(Util::parseConstant(DT_IP, "192.168.2." + to_string(i % 255)));
		row.push_back(Util::parseConstant(DT_INT128, "0f0e0d0c0b0a09080706050403020100"));
		row.push_back(Util::createInt(i));
		row.push_back(Util::createBlob("0f0e0d0c0b0a0908070605040302010" + to_string(i)));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	mulwrite->waitForThreadCompletion();
	ConstantSP t1 = conn.run("select * from t1 order by long;");
	for (int i = 0; i < 10985; i++)
	{
		EXPECT_EQ(t1->getColumn(0)->getRow(i)->getString(), Util::createBool(data[i % 2])->getString());
		EXPECT_EQ(t1->getColumn(1)->getRow(i)->getString(), Util::createChar(i * 2)->getString());
		EXPECT_EQ(t1->getColumn(2)->getRow(i)->getString(), Util::createShort(i + 12)->getString());
		EXPECT_EQ(t1->getColumn(3)->getRow(i)->getString(), Util::createLong((long)i * 100)->getString());
		EXPECT_EQ(t1->getColumn(4)->getRow(i)->getString(), Util::createDate(i + 432)->getString());
		EXPECT_EQ(t1->getColumn(5)->getRow(i)->getString(), Util::createMonth(i + 21)->getString());
		EXPECT_EQ(t1->getColumn(6)->getRow(i)->getString(), Util::createDateTime(i * 192)->getString());
		EXPECT_EQ(t1->getColumn(7)->getRow(i)->getString(), Util::createSecond((long long)i * 1932)->getString());
		EXPECT_EQ(t1->getColumn(8)->getRow(i)->getString(), Util::createTimestamp((long long)i * 2342)->getString());
		EXPECT_EQ(t1->getColumn(9)->getRow(i)->getString(), Util::createNanoTime((long long)i * 4214)->getString());
		EXPECT_EQ(t1->getColumn(10)->getRow(i)->getString(), Util::createNanoTimestamp((long long)i * 4264)->getString());
		EXPECT_EQ(t1->getColumn(11)->getRow(i)->getString(), Util::createFloat(i * 42.64)->getString());
		EXPECT_EQ(t1->getColumn(12)->getRow(i)->getString(), Util::createDouble(i * 4.264)->getString());
		EXPECT_EQ(t1->getColumn(13)->getRow(i)->getString(), Util::createString(std::to_string(i % 10))->getString());
		EXPECT_EQ(t1->getColumn(14)->getRow(i)->getString(), Util::createString("A" + to_string(i % 10))->getString());
		EXPECT_EQ(t1->getColumn(15)->getRow(i)->getString(), Util::parseConstant(DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100")->getString());
		EXPECT_EQ(t1->getColumn(16)->getRow(i)->getString(), Util::parseConstant(DT_IP, "192.168.2." + to_string(i % 255))->getString());
		EXPECT_EQ(t1->getColumn(17)->getRow(i)->getString(), Util::parseConstant(DT_INT128, "0f0e0d0c0b0a09080706050403020100")->getString());
		EXPECT_EQ(t1->getColumn(18)->getRow(i)->getString(), Util::createInt(i)->getString());
		EXPECT_EQ(t1->getColumn(19)->getRow(i)->getString(), Util::createString("0f0e0d0c0b0a0908070605040302010" + to_string(i))->getString());
		//  cout << t1->getColumn(0)->getRow(i)->getString() << endl;
	}
	conn.run("undef(`t1,SHARED)");
}

TEST_F(MultithreadedTableWriterTest, insertVectorkeyedTableDifferentTypeDataBiggerThan1048576)
{
	//
	string script = "t = keyedTable(`symbol`string,1000:0, `bool`char`short`long`date`month`datetime`second`timestamp`nanotime`nanotimestamp`datehour`float`double`symbol`string`uuid`ipaddr`int128`id`blob,"
					"[BOOL,CHAR,SHORT,LONG,DATE,MONTH,DATETIME,SECOND,TIMESTAMP,NANOTIME,NANOTIMESTAMP,DATEHOUR,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,BLOB]);"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	int data[] = {0, 1};
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 1, 0.1, 5, "symbol");
	vector<vector<ConstantSP> *> datas;
	for (int i = 0; i < 3000000; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createBool(data[i % 2]));
		row.push_back(Util::createChar(i % 200));
		row.push_back(Util::createShort(i % 100));
		row.push_back(Util::createLong(i % 100));
		row.push_back(Util::createDate(i % 3432));
		row.push_back(Util::createMonth(i % 1221));
		row.push_back(Util::createDateTime(i + 192));
		row.push_back(Util::createSecond(i % 1932));
		row.push_back(Util::createTimestamp(i + 2342));
		row.push_back(Util::createNanoTime(i + 4214));
		row.push_back(Util::createNanoTimestamp(i + 4264));
		row.push_back(Util::createDateHour(i + 4264));
		row.push_back(Util::createFloat(i * 42.64));
		row.push_back(Util::createDouble(i * 4.264));
		row.push_back(Util::createString(std::to_string(i % 100)));
		row.push_back(Util::createString("A" + to_string(i % 100)));
		// uuid,ipAddr, int128, blob
		row.push_back(Util::parseConstant(DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100"));
		row.push_back(Util::parseConstant(DT_IP, "192.168.2." + to_string(i % 255)));
		row.push_back(Util::parseConstant(DT_INT128, "0f0e0d0c0b0a09080706050403020100"));
		row.push_back(Util::createInt(i));
		row.push_back(Util::createBlob("0f0e0d0c0b0a0908070605040302010" + to_string(i)));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	mulwrite->waitForThreadCompletion();
	ConstantSP t1 = conn.run("select * from t1 order by long;");
	string script2 = "t2 = keyedTable(`symbol`string,1000:0, `bool`char`short`long`date`month`datetime`second`timestamp`nanotime`nanotimestamp`datehour`float`double`symbol`string`uuid`ipaddr`int128`id`blob,[BOOL,CHAR,SHORT,LONG,DATE,MONTH,DATETIME,SECOND,TIMESTAMP,NANOTIME,NANOTIMESTAMP,DATEHOUR,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,BLOB])\n"
					 "share t2 as table2\n"
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
					 "table2.append!(table(boolvector,charvector,shortvecot,longvector,datevector,montvector,datetimevector,secondvector,timestampvector,nanotimevector,nanotimestampvector,datehourvector,floatvector,doublevector,symbolvector,stringvector,uuidvector,ipvector,int128vector,idvector ,blobvector))\n"
					 "select * from table2 order by long";
	TableSP t2 = conn.run(script2);
	EXPECT_EQ(t1->getString(), t2->getString());
	conn.run("undef(`t1,SHARED)");
}

// TEST_F(MultithreadedTableWriterTest,insertVectorMemoryTableErrordata){
//     string script = "t = table(1000:0, `int`ipaddr,[INT,IPADDR]);"
//                     "share t as t1;";
//     conn.run(script);
//     SmartPointer<MultithreadedTableWriter> mulwrite;
//     SmartPointer<ErrorCodeInfo> pErrorInfo=new ErrorCodeInfo();
//     mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456","","t1",NULL,0,0,5,"int");
//     vector<vector<ConstantSP>> datas;
//     for(int i=0;i < 5;i++){
//         vector<ConstantSP> row;
//         row.push_back(Util::createInt(i));
//         row.push_back(Util::parseConstant(DT_IP,"192.168.2.342"));
//         datas.push_back(row);
//     }
//     bool flag = mulwrite->insertVector(datas,pErrorInfo.get());
// }

TEST_F(MultithreadedTableWriterTest, insertVectorMemoryTableWithNullptr)
{
	//
	// string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT]);"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 1, 0, 5, "sym");
	bool flag = mulwrite->insert(pErrorInfo, "A", (std::nullptr_t)0, 12);
	EXPECT_EQ(flag, true);
	mulwrite->waitForThreadCompletion();
	ConstantSP t1 = conn.run("select * from t1");
	EXPECT_EQ((t1->getRow(0)->getString() == "value->12\nsym->A\nid->\n") || (t1->getRow(0)->getString() == "value->12\nid->\nsym->A\n"), true);
}

TEST_F(MultithreadedTableWriterTest, insertVectorMemoryTableWithbatchSize)
{
	string script = "t = table(1000:0, `sym`id`values,[SYMBOL,INT,INT]);"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 1000, 10, 1, "sym");
	string sym[] = {"A", "B", "C", "D"};
	vector<vector<ConstantSP> *> datas;
	for (int i = 0; i < 999; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		srand((int)time(NULL));
		row.push_back(Util::createString(sym[rand() % 4]));
		row.push_back(Util::createInt(i * 12));
		row.push_back(Util::createInt(i + 64));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	ConstantSP t1 = conn.run("select * from t1 order by values;");
	// cout << t1->size() << endl;
	EXPECT_EQ(t1->size(), 0);
	mulwrite->insert(pErrorInfo, "A", 12, 16);
	mulwrite->insert(pErrorInfo, "B", 12, 16);
	mulwrite->waitForThreadCompletion();
	t1 = conn.run("select * from t1 order by values;");
	EXPECT_EQ(t1->size(), 1001);
}

TEST_F(MultithreadedTableWriterTest, insertVectorMemoryTableWithbatchSize_2)
{
	string script = "t = table(1000:0, `sym`id`values,[SYMBOL,INT,INT]);"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 1000, 1, 2, "sym");
	mulwrite->insert(pErrorInfo, "A", 12, 16000);
	string sym[] = {"A", "B", "C", "D"};
	vector<vector<ConstantSP> *> datas;
	srand((int)time(NULL));
	for (int i = 0; i < 1001; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createString(sym[rand() % 4]));
		row.push_back(Util::createInt(i * 12));
		row.push_back(Util::createInt(i + 64));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	mulwrite->waitForThreadCompletion();
	ConstantSP t1 = conn.run("select * from t1 order by values;");
	// cout << t1->size() << endl;
	EXPECT_EQ(t1->size(), 1002);
}

TEST_F(MultithreadedTableWriterTest, insertVectorMemoryTableWithThrottle)
{
	string script = "t = table(1000:0, `sym`id`values,[SYMBOL,INT,INT]);"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 1000, 1, 1, "sym");
	string sym[] = {"A", "B", "C", "D"};
	vector<vector<ConstantSP> *> datas;
	for (int i = 0; i < 560; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		srand((int)time(NULL));
		row.push_back(Util::createString(sym[rand() % 4]));
		row.push_back(Util::createInt(i * 12));
		row.push_back(Util::createInt(i + 64));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	ConstantSP t1 = conn.run("select * from t1 order by values;");
	EXPECT_EQ(t1->size(), 0);
	mulwrite->waitForThreadCompletion();
	t1 = conn.run("select * from t1 order by values;");
	EXPECT_EQ(t1->size(), 560);
}

// partitionTable
TEST_F(MultithreadedTableWriterTest, insertToPartitionTableWithpartitionColNull)
{
	//
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, VALUE,`A`B`C`D)\n"
					"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
					"pt = db.createPartitionedTable(t,`pt,`sym)";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	EXPECT_ANY_THROW(mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1, 0.1, 5, ""));
}

TEST_F(MultithreadedTableWriterTest, insertToPartitionTableWithpartitionColValueNull_threadCount1)
{
	//
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, VALUE,`A`B`C`D)\n"
					"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
					"pt = db.createPartitionedTable(t,`pt,`sym)";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1, 0.1, 1, "sym");
	bool flag = mulwrite->insert(pErrorInfo, "", 12, 12);
	EXPECT_EQ(flag, true);

	mulwrite->waitForThreadCompletion();

	vector<vector<ConstantSP> *> datas;
	mulwrite->getUnwrittenData(datas);
	EXPECT_EQ(datas[0][0][0]->getString(), "");
	EXPECT_EQ(datas[0][0][1]->getInt(), 12);
	EXPECT_EQ(datas[0][0][2]->getInt(), 12);
	EXPECT_EQ(datas.size(), 1);
}

TEST_F(MultithreadedTableWriterTest, insertToPartitionTableWithpartitionColValueNull_threadCount5)
{
	//
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, VALUE,`A`B`C`D)\n"
					"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
					"pt = db.createPartitionedTable(t,`pt,`sym)";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1, 0.1, 5, "sym");
	bool flag = mulwrite->insert(pErrorInfo, "", 12, 12);
	EXPECT_EQ(flag, true);

	mulwrite->waitForThreadCompletion();
	cout << pErrorInfo.errorInfo << endl;

	vector<vector<ConstantSP> *> datas;
	mulwrite->getUnwrittenData(datas);
	EXPECT_EQ(datas[0][0][0]->getString(), "");
	EXPECT_EQ(datas[0][0][1]->getInt(), 12);
	EXPECT_EQ(datas[0][0][2]->getInt(), 12);
	EXPECT_EQ(datas.size(), 1);
}

TEST_F(MultithreadedTableWriterTest, insertValueLongerThanPartitionTable)
{
	//
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, VALUE,`A`B`C`D)\n"
					"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
					"pt = db.createPartitionedTable(t,`pt,`sym)";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1, 0.1, 5, "sym");
	bool flag = mulwrite->insert(pErrorInfo, "A", 12, 12, 12);
	EXPECT_EQ(flag, false);
	EXPECT_EQ(pErrorInfo.errorInfo, "Column counts don't match 4");
}

TEST_F(MultithreadedTableWriterTest, insertValuesShorterThanPartitionTable)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, VALUE,`A`B`C`D)\n"
					"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
					"pt = db.createPartitionedTable(t,`pt,`sym)";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1, 0.1, 5, "sym");
	bool flag = mulwrite->insert(pErrorInfo, "A", 12);
	EXPECT_EQ(flag, false);
	EXPECT_EQ(pErrorInfo.errorInfo, "Column counts don't match 2");
}

TEST_F(MultithreadedTableWriterTest, insertValuesErrorTypeDataPartitionTable_1)
{
	//
	// string dbName = "dfs://test_MultithreadedTableWriter";
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, VALUE,`A`B`C`D)\n"
					"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
					"pt = db.createPartitionedTable(t,`pt,`sym)";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 1, 0.1, 5, "sym");
	bool flag = mulwrite->insert(pErrorInfo, "A", 12, 12.9);
	EXPECT_EQ(flag, false);
	cout << pErrorInfo.errorInfo;
	// EXPECT_EQ(pErrorInfo->errorInfo,"Argument size mismatch.");
}

TEST_F(MultithreadedTableWriterTest, insertValuesPartitionTable)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, VALUE,`A`B`C`D)\n"
					"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
					"pt = db.createPartitionedTable(t,`pt,`sym)";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1, 0.1, 5, "sym");
	bool flag = mulwrite->insert(pErrorInfo, "A", 12, 12);
	EXPECT_EQ(flag, true);
	mulwrite->waitForThreadCompletion();
	ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt)");
	EXPECT_EQ((t1->getRow(0)->getString() == "value->12\nsym->A\nid->12\n") || (t1->getRow(0)->getString() == "value->12\nid->12\nsym->A\n"), true);
}

TEST_F(MultithreadedTableWriterTest, insertValuesPartitionTableDifferentTypeData)
{
	//
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, VALUE,`A`B`C`D);\n";
	script += "t = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id,"
			  "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT]);"
			  "pt = db.createPartitionedTable(t,`pt,`symbol);";
	conn.run(script);
	Util::sleep(3000);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	unsigned char data[16] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1, 0.1, 5, "symbol");
	bool flag = mulwrite->insert(pErrorInfo, false, char(12), (short)12, (long)12, Util::createDate(2012, 11, 12), Util::createMonth(2012, 11), Util::createSecond(134),
								 Util::createDateTime(2012, 11, 13, 6, 12, 12), Util::createTimestamp(43241), Util::createNanoTime(532432),
								 Util::createNanoTimestamp(85932494), 12.4f, 3412.3, "A", "AAPL", data, data, data, 12);
	// EXPECT_EQ(flag, true);
	mulwrite->waitForThreadCompletion();
	ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt);");
	EXPECT_EQ(t1->getColumn(0)->getRow(0)->getString(), "0");
	EXPECT_EQ(t1->getColumn(1)->getRow(0)->getString(), "12");
	EXPECT_EQ(t1->getColumn(2)->getRow(0)->getString(), "12");
	EXPECT_EQ(t1->getColumn(3)->getRow(0)->getString(), "12");
	EXPECT_EQ(t1->getColumn(4)->getRow(0)->getString(), "2012.11.12");
	EXPECT_EQ(t1->getColumn(5)->getRow(0)->getString(), "2012.11M");
	EXPECT_EQ(t1->getColumn(6)->getRow(0)->getString(), "00:02:14");
	EXPECT_EQ(t1->getColumn(7)->getRow(0)->getString(), Util::createDateTime(2012, 11, 13, 6, 12, 12)->getString());
	EXPECT_EQ(t1->getColumn(8)->getRow(0)->getString(), Util::createTimestamp(43241)->getString());
	EXPECT_EQ(t1->getColumn(9)->getRow(0)->getString(), Util::createNanoTime(532432)->getString());
	EXPECT_EQ(t1->getColumn(10)->getRow(0)->getString(), Util::createNanoTimestamp(85932494)->getString());
	EXPECT_EQ(t1->getColumn(11)->getRow(0)->getString(), "12.4");
	EXPECT_EQ(t1->getColumn(12)->getRow(0)->getString(), "3412.3");
	EXPECT_EQ(t1->getColumn(13)->getRow(0)->getString(), "A");
	EXPECT_EQ(t1->getColumn(14)->getRow(0)->getString(), "AAPL");
	EXPECT_EQ(t1->getColumn(15)->getRow(0)->getString(), "0f0e0d0c-0b0a-0908-0706-050403020100");
	EXPECT_EQ(t1->getColumn(16)->getRow(0)->getString(), "f0e:d0c:b0a:908:706:504:302:100");
	EXPECT_EQ(t1->getColumn(17)->getRow(0)->getString(), "0f0e0d0c0b0a09080706050403020100");
}

TEST_F(MultithreadedTableWriterTest, insertVectorPartitionTableDifferentTypeDataBiggerThan1048576)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, VALUE,`A`B`C`D);\n";
	script += "t = table(1000:0, `bool`char`short`long`date`month`datetime`second`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id,"
			  "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,DATETIME,SECOND,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT]);"
			  "pt = db.createPartitionedTable(t,`pt,`symbol);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	int data[] = {0, 1};
	string sym[] = {"A", "B", "C", "D"};
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1, 0, 5, "symbol");
	vector<vector<ConstantSP> *> datas;
	srand((int)time(NULL));
	for (int i = 0; i < 3098576; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createBool(data[i % 2]));
		row.push_back(Util::createChar(i * 2));
		row.push_back(Util::createShort(i + 12));
		row.push_back(Util::createLong((long)i * 100));
		row.push_back(Util::createDate(i + 432));
		row.push_back(Util::createMonth(i + 21));
		row.push_back(Util::createDateTime(i * 192));
		row.push_back(Util::createSecond((long long)i * 1932));
		row.push_back(Util::createTimestamp((long long)i * 2342));
		row.push_back(Util::createNanoTime((long long)i * 4214));
		row.push_back(Util::createNanoTimestamp((long long)i * 4264));
		row.push_back(Util::createFloat(i * 42.64));
		row.push_back(Util::createDouble(i * 4.264));
		row.push_back(Util::createString(sym[rand() % 4]));
		row.push_back(Util::createString("A" + to_string(i)));
		// uuid,ipAddr, int128, blob
		row.push_back(Util::parseConstant(DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100"));
		row.push_back(Util::parseConstant(DT_IP, "192.168.2." + to_string(i % 255)));
		row.push_back(Util::parseConstant(DT_INT128, "0f0e0d0c0b0a09080706050403020100"));
		row.push_back(Util::createInt(i));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	mulwrite->waitForThreadCompletion();
	ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt) order by long limit 1024;");
	for (int i = 0; i < 1024; i++)
	{
		EXPECT_EQ(t1->getColumn(0)->getRow(i)->getString(), Util::createBool(data[i % 2])->getString());
		EXPECT_EQ(t1->getColumn(1)->getRow(i)->getString(), Util::createChar(i * 2)->getString());
		EXPECT_EQ(t1->getColumn(2)->getRow(i)->getString(), Util::createShort(i + 12)->getString());
		EXPECT_EQ(t1->getColumn(3)->getRow(i)->getString(), Util::createLong((long)i * 100)->getString());
		EXPECT_EQ(t1->getColumn(4)->getRow(i)->getString(), Util::createDate(i + 432)->getString());
		EXPECT_EQ(t1->getColumn(5)->getRow(i)->getString(), Util::createMonth(i + 21)->getString());
		EXPECT_EQ(t1->getColumn(6)->getRow(i)->getString(), Util::createDateTime(i * 192)->getString());
		EXPECT_EQ(t1->getColumn(7)->getRow(i)->getString(), Util::createSecond((long long)i * 1932)->getString());
		EXPECT_EQ(t1->getColumn(8)->getRow(i)->getString(), Util::createTimestamp((long long)i * 2342)->getString());
		EXPECT_EQ(t1->getColumn(9)->getRow(i)->getString(), Util::createNanoTime((long long)i * 4214)->getString());
		EXPECT_EQ(t1->getColumn(10)->getRow(i)->getString(), Util::createNanoTimestamp((long long)i * 4264)->getString());
		EXPECT_EQ(t1->getColumn(11)->getRow(i)->getString(), Util::createFloat(i * 42.64)->getString());
		EXPECT_EQ(t1->getColumn(12)->getRow(i)->getString(), Util::createDouble(i * 4.264)->getString());
		EXPECT_EQ(t1->getColumn(14)->getRow(i)->getString(), Util::createString("A" + to_string(i))->getString());
		EXPECT_EQ(t1->getColumn(15)->getRow(i)->getString(), Util::parseConstant(DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100")->getString());
		EXPECT_EQ(t1->getColumn(16)->getRow(i)->getString(), Util::parseConstant(DT_IP, "192.168.2." + to_string(i % 255))->getString());
		EXPECT_EQ(t1->getColumn(17)->getRow(i)->getString(), Util::parseConstant(DT_INT128, "0f0e0d0c0b0a09080706050403020100")->getString());
		EXPECT_EQ(t1->getColumn(18)->getRow(i)->getString(), Util::createInt(i)->getString());
		;
		//  cout << t1->getColumn(0)->getRow(i)->getString() << endl;
	}
	cout << pErrorInfo.errorInfo << endl;
}

TEST_F(MultithreadedTableWriterTest, insertVectorPartitionTableWithNullptr)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, VALUE,symbol(\"A\"+string(0..10)));\n";
	script += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
			  "pt = db.createPartitionedTable(t,`pt,`symbol);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1, 0.1, 5, "symbol");
	bool flag = mulwrite->insert(pErrorInfo, "A1", (std::nullptr_t)0, 12);
	EXPECT_EQ(flag, true);
	mulwrite->waitForThreadCompletion();
	ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt)");
	// cout << t1->getRow(0)->getString();
	EXPECT_EQ((t1->getRow(0)->getString() == "value->12\nsymbol->A1\nid->\n") || (t1->getRow(0)->getString() == "value->12\nid->\nsymbol->A1\n"), true);
}

TEST_F(MultithreadedTableWriterTest, insertVectorPartitionTableWithbatchSize)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, VALUE,`A`B`C`D);\n";
	script += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
			  "pt = db.createPartitionedTable(t,`pt,`symbol);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1000, 10, 4, "symbol");
	string sym[] = {"A", "B", "C", "D"};
	vector<vector<ConstantSP> *> datas;
	srand((int)time(NULL));
	for (int i = 0; i < 999; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createString(sym[rand() % 4]));
		row.push_back(Util::createInt(i * 12));
		row.push_back(Util::createInt(i + 64));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
	EXPECT_EQ(t1->size(), 0);
	mulwrite->insert(pErrorInfo, "A", 12, 16);
	mulwrite->waitForThreadCompletion();
	t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
	EXPECT_EQ(t1->size(), 1000);
}

TEST_F(MultithreadedTableWriterTest, insertVectorPartitionTableWithThrottle)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, VALUE,`A`B`C`D);\n";
	script += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
			  "pt = db.createPartitionedTable(t,`pt,`symbol);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1000, 1, 1, "sym");
	string sym[] = {"A", "B", "C", "D"};
	vector<vector<ConstantSP> *> datas;
	for (int i = 0; i < 999; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		srand((int)time(NULL));
		row.push_back(Util::createString(sym[rand() % 4]));
		row.push_back(Util::createInt(i * 12));
		row.push_back(Util::createInt(i + 64));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
	EXPECT_EQ(t1->size(), 0);
	mulwrite->waitForThreadCompletion();
	t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
	EXPECT_EQ(t1->size(), 999);
}

// TSDBPartitionTable

TEST_F(MultithreadedTableWriterTest, insertValueLongerThanTSDBPartitionTable)
{
	ConstantSP server_version = conn.run("version()");
	string v = server_version->getString();
	if (v.find("1.30") == 0)
	{
		cout << "server version: 1.30.xx, skip this case." << endl;
		EXPECT_EQ(1, 1);
	}
	else if (v.find("2.00") == 0)
	{
		string dbName = "dfs://test_MultithreadedTableWriter";
		string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
						"if(exists(dbName)){\n"
						"\tdropDatabase(dbName)\t\n"
						"}\n"
						"db  = database(dbName, VALUE,symbol(\"A\"+string(0..10)),,`TSDB);\n"
						"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
						"pt = db.createPartitionedTable(t,`pt,`sym,,`sym`id)";
		conn.run(script);
		SmartPointer<MultithreadedTableWriter> mulwrite;
		ErrorCodeInfo pErrorInfo;
		mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1, 0.1, 5, "sym");
		bool flag = mulwrite->insert(pErrorInfo, "A1", 12, 12, 12);
		EXPECT_EQ(flag, false);
		EXPECT_EQ(pErrorInfo.errorInfo, "Column counts don't match 4");
	}
	else
	{
		cout << "server version is not 1.30 and 2.00, please check.";
		EXPECT_EQ(1, 2);
	}
}

TEST_F(MultithreadedTableWriterTest, insertValuesShorterThanTSDBPartitionTable)
{
	ConstantSP server_version = conn.run("version()");
	string v = server_version->getString();
	if (v.find("1.30") == 0)
	{
		cout << "server version: 1.30.xx, skip this case." << endl;
		EXPECT_EQ(1, 1);
	}
	else if (v.find("2.00") == 0)
	{
		string dbName = "dfs://test_MultithreadedTableWriter";
		string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
						"if(exists(dbName)){\n"
						"\tdropDatabase(dbName)\t\n"
						"}\n"
						"db  = database(dbName, VALUE,symbol(\"A\"+string(0..10)),,`TSDB);\n"
						"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
						"pt = db.createPartitionedTable(t,`pt,`sym,,`sym`id)";
		conn.run(script);
		SmartPointer<MultithreadedTableWriter> mulwrite;
		ErrorCodeInfo pErrorInfo;
		mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1, 0.1, 5, "sym");
		bool flag = mulwrite->insert(pErrorInfo, "A1", 12);
		EXPECT_EQ(flag, false);
		EXPECT_EQ(pErrorInfo.errorInfo, "Column counts don't match 2");
	}
	else
	{
		cout << "server version is not 1.30 and 2.00, please check.";
		EXPECT_EQ(1, 2);
	}
}

TEST_F(MultithreadedTableWriterTest, insertValuesErrorTypeTSDBPartitionTable_1)
{
	ConstantSP server_version = conn.run("version()");
	string v = server_version->getString();
	if (v.find("1.30") == 0)
	{
		cout << "server version: 1.30.xx, skip this case." << endl;
		EXPECT_EQ(1, 1);
	}
	else if (v.find("2.00") == 0)
	{
		string dbName = "dfs://test_MultithreadedTableWriter";
		string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
						"if(exists(dbName)){\n"
						"\tdropDatabase(dbName)\t\n"
						"}\n"
						"db  = database(dbName, VALUE,symbol(\"A\"+string(0..10)),,`TSDB);\n"
						"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
						"pt = db.createPartitionedTable(t,`pt,`sym,,`sym`id)";
		conn.run(script);
		SmartPointer<MultithreadedTableWriter> mulwrite;
		ErrorCodeInfo pErrorInfo;
		mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1, 0.1, 5, "sym");
		bool flag = mulwrite->insert(pErrorInfo, "A1", 12, "A");
		EXPECT_EQ(flag, false);
		// EXPECT_EQ(pErrorInfo->errorInfo,"Argument size mismatch.");
	}
	else
	{
		cout << "server version is not 1.30 and 2.00, please check.";
		EXPECT_EQ(1, 2);
	}
}

TEST_F(MultithreadedTableWriterTest, insertValuesTSDBPartitionTable)
{
	ConstantSP server_version = conn.run("version()");
	string v = server_version->getString();
	if (v.find("1.30") == 0)
	{
		cout << "server version: 1.30.xx, skip this case." << endl;
		EXPECT_EQ(1, 1);
	}
	else if (v.find("2.00") == 0)
	{
		string dbName = "dfs://test_MultithreadedTableWriter";
		string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
						"if(exists(dbName)){\n"
						"\tdropDatabase(dbName)\t\n"
						"}\n"
						"db  = database(dbName, VALUE,symbol(\"A\"+string(0..10)),,`TSDB);\n"
						"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
						"pt = db.createPartitionedTable(t,`pt,`sym,,`sym`id)";
		conn.run(script);
		SmartPointer<MultithreadedTableWriter> mulwrite;
		ErrorCodeInfo pErrorInfo;
		mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1, 0.1, 5, "sym");
		bool flag = mulwrite->insert(pErrorInfo, "A1", 12, 12);
		EXPECT_EQ(flag, true);
		mulwrite->waitForThreadCompletion();
		ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt)");
		EXPECT_EQ((t1->getRow(0)->getString() == "value->12\nsym->A1\nid->12\n") || (t1->getRow(0)->getString() == "value->12\nid->12\nsym->A1\n"), true);
		//  cout << pErrorInfo->errorCode << "\n";
		//  cout << pErrorInfo->errorInfo;
	}
	else
	{
		cout << "server version is not 1.30 and 2.00, please check.";
		EXPECT_EQ(1, 2);
	}
}

TEST_F(MultithreadedTableWriterTest, chear)
{
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n";
	conn.run(script);
}

TEST_F(MultithreadedTableWriterTest, insertValuesTSDBPartitionTableDifferentTypeData)
{
	ConstantSP server_version = conn.run("version()");
	string v = server_version->getString();
	if (v.find("1.30") == 0)
	{
		cout << "server version: 1.30.xx, skip this case." << endl;
		EXPECT_EQ(1, 1);
	}
	else if (v.find("2.00") == 0)
	{
		string dbName = "dfs://test_MultithreadedTableWriter";
		string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
						"if(exists(dbName)){\n"
						"\tdropDatabase(dbName)\t\n"
						"}\n"
						"db  = database(dbName, VALUE,`A`B`C`D,,`TSDB);\n";
		script += "t = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`bolb,"
				  "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,BLOB]);"
				  "pt = db.createPartitionedTable(t,`pt,`symbol,,`symbol`id);";
		conn.run(script);
		Util::sleep(3000);
		SmartPointer<MultithreadedTableWriter> mulwrite;
		ErrorCodeInfo pErrorInfo;
		unsigned char data[16] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
		mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1, 0.1, 5, "symbol");
		bool flag = mulwrite->insert(pErrorInfo, false, char(12), (short)12, (long)12, Util::createDate(2012, 11, 12), Util::createMonth(2012, 11), Util::createSecond(134),
									 Util::createDateTime(2012, 11, 13, 6, 12, 12), Util::createTimestamp(43241), Util::createNanoTime(532432),
									 Util::createNanoTimestamp(85932494), 12.4f, 3412.3, "A", "AAPL", data, data, data, 12, "0f0e0d0c0b0a0908070605040302010");
		// EXPECT_EQ(flag, true);
		mulwrite->waitForThreadCompletion();
		ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt);");
		EXPECT_EQ(t1->getColumn(0)->getRow(0)->getString(), "0");
		EXPECT_EQ(t1->getColumn(1)->getRow(0)->getString(), "12");
		EXPECT_EQ(t1->getColumn(2)->getRow(0)->getString(), "12");
		EXPECT_EQ(t1->getColumn(3)->getRow(0)->getString(), "12");
		EXPECT_EQ(t1->getColumn(4)->getRow(0)->getString(), "2012.11.12");
		EXPECT_EQ(t1->getColumn(5)->getRow(0)->getString(), "2012.11M");
		EXPECT_EQ(t1->getColumn(6)->getRow(0)->getString(), "00:02:14");
		EXPECT_EQ(t1->getColumn(7)->getRow(0)->getString(), Util::createDateTime(2012, 11, 13, 6, 12, 12)->getString());
		EXPECT_EQ(t1->getColumn(8)->getRow(0)->getString(), Util::createTimestamp(43241)->getString());
		EXPECT_EQ(t1->getColumn(9)->getRow(0)->getString(), Util::createNanoTime(532432)->getString());
		EXPECT_EQ(t1->getColumn(10)->getRow(0)->getString(), Util::createNanoTimestamp(85932494)->getString());
		EXPECT_EQ(t1->getColumn(11)->getRow(0)->getString(), "12.4");
		EXPECT_EQ(t1->getColumn(12)->getRow(0)->getString(), "3412.3");
		EXPECT_EQ(t1->getColumn(13)->getRow(0)->getString(), "A");
		EXPECT_EQ(t1->getColumn(14)->getRow(0)->getString(), "AAPL");
		EXPECT_EQ(t1->getColumn(15)->getRow(0)->getString(), "0f0e0d0c-0b0a-0908-0706-050403020100");
		EXPECT_EQ(t1->getColumn(16)->getRow(0)->getString(), "f0e:d0c:b0a:908:706:504:302:100");
		EXPECT_EQ(t1->getColumn(17)->getRow(0)->getString(), "0f0e0d0c0b0a09080706050403020100");
		EXPECT_EQ(t1->getColumn(19)->getRow(0)->getString(), "0f0e0d0c0b0a0908070605040302010");
	}
	else
	{
		cout << "server version is not 1.30 and 2.00, please check.";
		EXPECT_EQ(1, 2);
	}
}

TEST_F(MultithreadedTableWriterTest, insertVectorTSDBPartitionTableDifferentTypeDataLessThan256)
{
	ConstantSP server_version = conn.run("version()");
	string v = server_version->getString();
	if (v.find("1.30") == 0)
	{
		cout << "server version: 1.30.xx, skip this case." << endl;
		EXPECT_EQ(1, 1);
	}
	else if (v.find("2.00") == 0)
	{
		string dbName = "dfs://test_MultithreadedTableWriter";
		string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
						"if(exists(dbName)){\n"
						"\tdropDatabase(dbName)\t\n"
						"}\n"
						"db  = database(dbName, VALUE,string(0..10),,`TSDB);\n";
		script += "t = table(1000:0, `bool`char`short`long`date`month`datetime`second`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`bolb,"
				  "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,DATETIME,SECOND,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,BLOB]);"
				  "pt = db.createPartitionedTable(t,`pt,`symbol,,`symbol`id);";
		conn.run(script);
		SmartPointer<MultithreadedTableWriter> mulwrite;
		ErrorCodeInfo pErrorInfo;
		int data[] = {0, 1};
		mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1, 0.1, 5, "symbol");
		vector<vector<ConstantSP> *> datas;
		for (int i = 0; i < 10; i++)
		{
			vector<ConstantSP> *prow = new vector<ConstantSP>;
			vector<ConstantSP> &row = *prow;
			row.push_back(Util::createBool(data[i % 2]));
			row.push_back(Util::createChar(i * 2));
			row.push_back(Util::createShort(i + 12));
			row.push_back(Util::createLong((long)i * 100));
			row.push_back(Util::createDate(i + 432));
			row.push_back(Util::createMonth(i + 21));
			row.push_back(Util::createDateTime(i * 192));
			row.push_back(Util::createSecond((long long)i * 1932));
			row.push_back(Util::createTimestamp((long long)i * 2342));
			row.push_back(Util::createNanoTime((long long)i * 4214));
			row.push_back(Util::createNanoTimestamp((long long)i * 4264));
			row.push_back(Util::createFloat(i * 42.64));
			row.push_back(Util::createDouble(i * 4.264));
			row.push_back(Util::createString(std::to_string(i)));
			row.push_back(Util::createString("A" + to_string(i)));
			// uuid,ipAddr, int128, blob
			row.push_back(Util::parseConstant(DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100"));
			row.push_back(Util::parseConstant(DT_IP, "192.168.2." + to_string(i % 255)));
			row.push_back(Util::parseConstant(DT_INT128, "0f0e0d0c0b0a09080706050403020100"));
			row.push_back(Util::createInt(i));
			row.push_back(Util::createBlob("0f0e0d0c0b0a0908070605040302010" + to_string(i)));
			datas.push_back(prow);
		}
		mulwrite->insertUnwrittenData(datas, pErrorInfo);
		cout << pErrorInfo.errorInfo;
		mulwrite->waitForThreadCompletion();
		ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt) order by long;");
		// cout << t1->getColumn(19)->getRow(1)->getString();
		for (int i = 0; i < 10; i++)
		{
			EXPECT_EQ(t1->getColumn(0)->getRow(i)->getString(), Util::createBool(data[i % 2])->getString());
			EXPECT_EQ(t1->getColumn(1)->getRow(i)->getString(), Util::createChar(i * 2)->getString());
			EXPECT_EQ(t1->getColumn(2)->getRow(i)->getString(), Util::createShort(i + 12)->getString());
			EXPECT_EQ(t1->getColumn(3)->getRow(i)->getString(), Util::createLong((long)i * 100)->getString());
			EXPECT_EQ(t1->getColumn(4)->getRow(i)->getString(), Util::createDate(i + 432)->getString());
			EXPECT_EQ(t1->getColumn(5)->getRow(i)->getString(), Util::createMonth(i + 21)->getString());
			EXPECT_EQ(t1->getColumn(6)->getRow(i)->getString(), Util::createDateTime(i * 192)->getString());
			EXPECT_EQ(t1->getColumn(7)->getRow(i)->getString(), Util::createSecond((long long)i * 1932)->getString());
			EXPECT_EQ(t1->getColumn(8)->getRow(i)->getString(), Util::createTimestamp((long long)i * 2342)->getString());
			EXPECT_EQ(t1->getColumn(9)->getRow(i)->getString(), Util::createNanoTime((long long)i * 4214)->getString());
			EXPECT_EQ(t1->getColumn(10)->getRow(i)->getString(), Util::createNanoTimestamp((long long)i * 4264)->getString());
			EXPECT_EQ(t1->getColumn(11)->getRow(i)->getString(), Util::createFloat(i * 42.64)->getString());
			EXPECT_EQ(t1->getColumn(12)->getRow(i)->getString(), Util::createDouble(i * 4.264)->getString());
			EXPECT_EQ(t1->getColumn(13)->getRow(i)->getString(), Util::createString(std::to_string(i))->getString());
			EXPECT_EQ(t1->getColumn(14)->getRow(i)->getString(), Util::createString("A" + to_string(i))->getString());
			EXPECT_EQ(t1->getColumn(15)->getRow(i)->getString(), Util::parseConstant(DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100")->getString());
			EXPECT_EQ(t1->getColumn(16)->getRow(i)->getString(), Util::parseConstant(DT_IP, "192.168.2." + to_string(i))->getString());
			EXPECT_EQ(t1->getColumn(17)->getRow(i)->getString(), Util::parseConstant(DT_INT128, "0f0e0d0c0b0a09080706050403020100")->getString());
			EXPECT_EQ(t1->getColumn(18)->getRow(i)->getString(), Util::createInt(i)->getString());
			EXPECT_EQ(t1->getColumn(19)->getRow(i)->getString(), Util::createString("0f0e0d0c0b0a0908070605040302010" + to_string(i))->getString());
			//  cout << t1->getColumn(0)->getRow(i)->getString() << endl;
		}
	}
	else
	{
		cout << "server version is not 1.30 and 2.00, please check.";
		EXPECT_EQ(1, 2);
	}
}

TEST_F(MultithreadedTableWriterTest, insertVectorTSDBPartitionTableWithFilter)
{
	ConstantSP server_version = conn.run("version()");
	string v = server_version->getString();
	if (v.find("1.30") == 0)
	{
		cout << "server version: 1.30.xx, skip this case." << endl;
		EXPECT_EQ(1, 1);
	}
	else if (v.find("2.00") == 0)
	{
		string dbName = "dfs://test_MultithreadedTableWriter";
		string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
						"if(exists(dbName)){\n"
						"\tdropDatabase(dbName)\t\n"
						"}\n"
						"db  = database(dbName, VALUE,1..9,,`TSDB);\n";
		script += "t = table(1000:0,`id`symbol`value,[INT,SYMBOL,INT]);"
				  "pt = db.createPartitionedTable(t,`pt,`id,,`symbol`id,LAST);";
		conn.run(script);
		SmartPointer<MultithreadedTableWriter> mulwrite;
		ErrorCodeInfo pErrorInfo;
		int ids[] = {1, 2, 3, 4, 5, 6, 7, 8, 9};
		string syms[] = {"A", "B", "C", "D"};
		mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 100, 0.1, 5, "id");
		vector<vector<ConstantSP> *> datas;
		for (int i = 0; i < 2000; i++)
		{
			vector<ConstantSP> *prow = new vector<ConstantSP>;
			vector<ConstantSP> &row = *prow;
			row.push_back(Util::createInt(ids[i % 9]));
			row.push_back(Util::createString(syms[i % 4]));
			row.push_back(Util::createInt(i));
			datas.push_back(prow);
		}
		mulwrite->insertUnwrittenData(datas, pErrorInfo);
		mulwrite->waitForThreadCompletion();
		ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt) order by id;");
		EXPECT_EQ(t1->size(), 36);
	}
	else
	{
		cout << "server version is not 1.30 and 2.00, please check.";
		EXPECT_EQ(1, 2);
	}
}

TEST_F(MultithreadedTableWriterTest, insertVectorTSDBPartitionTableDifferentTypeDataBiggerThan1048576)
{
	ConstantSP server_version = conn.run("version()");
	string v = server_version->getString();
	if (v.find("1.30") == 0)
	{
		cout << "server version: 1.30.xx, skip this case." << endl;
		EXPECT_EQ(1, 1);
	}
	else if (v.find("2.00") == 0)
	{
		string dbName = "dfs://test_MultithreadedTableWriter";
		string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
						"if(exists(dbName)){\n"
						"\tdropDatabase(dbName)\t\n"
						"}\n"
						"db  = database(dbName, VALUE,`A`B`C`D,,`TSDB);\n";
		script += "t = table(1000:0, `bool`char`short`long`date`month`datetime`second`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`bolb,"
				  "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,DATETIME,SECOND,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,BLOB]);"
				  "pt = db.createPartitionedTable(t,`pt,`symbol,,`symbol`id);";
		conn.run(script);
		SmartPointer<MultithreadedTableWriter> mulwrite;
		ErrorCodeInfo pErrorInfo;
		int data[] = {0, 1};
		string sym[] = {"A", "B", "C", "D"};
		mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1, 0.1, 5, "symbol");
		vector<vector<ConstantSP> *> datas;
		srand((int)time(NULL));
		for (int i = 0; i < 1098576; i++)
		{
			vector<ConstantSP> *prow = new vector<ConstantSP>;
			vector<ConstantSP> &row = *prow;
			row.push_back(Util::createBool(data[i % 2]));
			row.push_back(Util::createChar(i * 2));
			row.push_back(Util::createShort(i + 12));
			row.push_back(Util::createLong((long)i * 100));
			row.push_back(Util::createDate(i + 432));
			row.push_back(Util::createMonth(i + 21));
			row.push_back(Util::createDateTime(i * 192));
			row.push_back(Util::createSecond((long long)i * 1932));
			row.push_back(Util::createTimestamp((long long)i * 2342));
			row.push_back(Util::createNanoTime((long long)i * 4214));
			row.push_back(Util::createNanoTimestamp((long long)i * 4264));
			row.push_back(Util::createFloat(i * 42.64));
			row.push_back(Util::createDouble(i * 4.264));
			row.push_back(Util::createString(sym[rand() % 4]));
			row.push_back(Util::createString("A" + to_string(i)));
			// uuid,ipAddr, int128, blob
			row.push_back(Util::parseConstant(DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100"));
			row.push_back(Util::parseConstant(DT_IP, "192.168.2." + to_string(i % 255)));
			row.push_back(Util::parseConstant(DT_INT128, "0f0e0d0c0b0a09080706050403020100"));
			row.push_back(Util::createInt(i));
			row.push_back(Util::createBlob("0f0e0d0c0b0a0908070605040302010" + to_string(i)));
			datas.push_back(prow);
		}
		mulwrite->insertUnwrittenData(datas, pErrorInfo);
		mulwrite->waitForThreadCompletion();
		ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt) order by long limit 1024;");
		for (int i = 0; i < 1024; i++)
		{
			EXPECT_EQ(t1->getColumn(0)->getRow(i)->getString(), Util::createBool(data[i % 2])->getString());
			EXPECT_EQ(t1->getColumn(1)->getRow(i)->getString(), Util::createChar(i * 2)->getString());
			EXPECT_EQ(t1->getColumn(2)->getRow(i)->getString(), Util::createShort(i + 12)->getString());
			EXPECT_EQ(t1->getColumn(3)->getRow(i)->getString(), Util::createLong((long)i * 100)->getString());
			EXPECT_EQ(t1->getColumn(4)->getRow(i)->getString(), Util::createDate(i + 432)->getString());
			EXPECT_EQ(t1->getColumn(5)->getRow(i)->getString(), Util::createMonth(i + 21)->getString());
			EXPECT_EQ(t1->getColumn(6)->getRow(i)->getString(), Util::createDateTime(i * 192)->getString());
			EXPECT_EQ(t1->getColumn(7)->getRow(i)->getString(), Util::createSecond((long long)i * 1932)->getString());
			EXPECT_EQ(t1->getColumn(8)->getRow(i)->getString(), Util::createTimestamp((long long)i * 2342)->getString());
			EXPECT_EQ(t1->getColumn(9)->getRow(i)->getString(), Util::createNanoTime((long long)i * 4214)->getString());
			EXPECT_EQ(t1->getColumn(10)->getRow(i)->getString(), Util::createNanoTimestamp((long long)i * 4264)->getString());
			EXPECT_EQ(t1->getColumn(11)->getRow(i)->getString(), Util::createFloat(i * 42.64)->getString());
			EXPECT_EQ(t1->getColumn(12)->getRow(i)->getString(), Util::createDouble(i * 4.264)->getString());
			EXPECT_EQ(t1->getColumn(14)->getRow(i)->getString(), Util::createString("A" + to_string(i))->getString());
			EXPECT_EQ(t1->getColumn(15)->getRow(i)->getString(), Util::parseConstant(DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100")->getString());
			EXPECT_EQ(t1->getColumn(16)->getRow(i)->getString(), Util::parseConstant(DT_IP, "192.168.2." + to_string(i % 255))->getString());
			EXPECT_EQ(t1->getColumn(17)->getRow(i)->getString(), Util::parseConstant(DT_INT128, "0f0e0d0c0b0a09080706050403020100")->getString());
			EXPECT_EQ(t1->getColumn(18)->getRow(i)->getString(), Util::createInt(i)->getString());
			EXPECT_EQ(t1->getColumn(19)->getRow(i)->getString(), Util::createString("0f0e0d0c0b0a0908070605040302010" + to_string(i))->getString());
			//  cout << t1->getColumn(0)->getRow(i)->getString() << endl;
		}
	}
	else
	{
		cout << "server version is not 1.30 and 2.00, please check.";
		EXPECT_EQ(1, 2);
	}
}

TEST_F(MultithreadedTableWriterTest, insertVectorTSDBPartitionTableWithNullptr)
{
	ConstantSP server_version = conn.run("version()");
	string v = server_version->getString();
	if (v.find("1.30") == 0)
	{
		cout << "server version: 1.30.xx, skip this case." << endl;
		EXPECT_EQ(1, 1);
	}
	else if (v.find("2.00") == 0)
	{
		string dbName = "dfs://test_MultithreadedTableWriter";
		string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
						"if(exists(dbName)){\n"
						"\tdropDatabase(dbName)\t\n"
						"}\n"
						"db  = database(dbName, VALUE,symbol(\"A\"+string(0..10)),,`TSDB);\n";
		script += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
				  "pt = db.createPartitionedTable(t,`pt,`symbol,,`symbol`id);";
		conn.run(script);
		SmartPointer<MultithreadedTableWriter> mulwrite;
		ErrorCodeInfo pErrorInfo;
		mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1, 0.1, 5, "symbol");
		bool flag = mulwrite->insert(pErrorInfo, "A1", (std::nullptr_t)0, 12);
		EXPECT_EQ(flag, true);
		mulwrite->waitForThreadCompletion();
		ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt)");
		EXPECT_EQ((t1->getRow(0)->getString() == "value->12\nsymbol->A1\nid->\n") || (t1->getRow(0)->getString() == "value->12\nid->\nsymbol->A1\n"), true);
	}
	else
	{
		cout << "server version is not 1.30 and 2.00, please check.";
		EXPECT_EQ(1, 2);
	}
}

TEST_F(MultithreadedTableWriterTest, insertVectorTSDBPartitionTableWithbatchSize)
{
	ConstantSP server_version = conn.run("version()");
	string v = server_version->getString();
	if (v.find("1.30") == 0)
	{
		cout << "server version: 1.30.xx, skip this case." << endl;
		EXPECT_EQ(1, 1);
	}
	else if (v.find("2.00") == 0)
	{
		string dbName = "dfs://test_MultithreadedTableWriter";
		string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
						"if(exists(dbName)){\n"
						"\tdropDatabase(dbName)\t\n"
						"}\n"
						"db  = database(dbName, VALUE,`A`B`C`D,,`TSDB);\n";
		script += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
				  "pt = db.createPartitionedTable(t,`pt,`symbol,,`symbol`id);";
		conn.run(script);
		SmartPointer<MultithreadedTableWriter> mulwrite;
		ErrorCodeInfo pErrorInfo;
		mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1000, 10, 1, "symbol");
		string sym[] = {"A", "B", "C", "D"};
		vector<vector<ConstantSP> *> datas;
		srand((int)time(NULL));
		for (int i = 0; i < 999; i++)
		{
			vector<ConstantSP> *prow = new vector<ConstantSP>;
			vector<ConstantSP> &row = *prow;
			row.push_back(Util::createString(sym[rand() % 4]));
			row.push_back(Util::createInt(i * 12));
			row.push_back(Util::createInt(i + 64));
			datas.push_back(prow);
		}
		mulwrite->insertUnwrittenData(datas, pErrorInfo);
		ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
		EXPECT_EQ(t1->size(), 0);
		mulwrite->insert(pErrorInfo, "A", 12, 16);
		mulwrite->waitForThreadCompletion();
		t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
		EXPECT_EQ(t1->size(), 1000);
	}
	else
	{
		cout << "server version is not 1.30 and 2.00, please check.";
		EXPECT_EQ(1, 2);
	}
}

TEST_F(MultithreadedTableWriterTest, insertVectorTSDBPartitionTableWithThrottle)
{
	ConstantSP server_version = conn.run("version()");
	string v = server_version->getString();
	if (v.find("1.30") == 0)
	{
		cout << "server version: 1.30.xx, skip this case." << endl;
		EXPECT_EQ(1, 1);
	}
	else if (v.find("2.00") == 0)
	{
		string dbName = "dfs://test_MultithreadedTableWriter";
		string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
						"if(exists(dbName)){\n"
						"\tdropDatabase(dbName)\t\n"
						"}\n"
						"db  = database(dbName, VALUE,`A`B`C`D,,`TSDB);\n";
		script += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
				  "pt = db.createPartitionedTable(t,`pt,`symbol,,`symbol`id);";
		conn.run(script);
		SmartPointer<MultithreadedTableWriter> mulwrite;
		ErrorCodeInfo pErrorInfo;
		mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1000, 2, 5, "symbol");
		string sym[] = {"A", "B", "C", "D"};
		vector<vector<ConstantSP> *> datas;
		srand((int)time(NULL));
		for (int i = 0; i < 999; i++)
		{
			vector<ConstantSP> *prow = new vector<ConstantSP>;
			vector<ConstantSP> &row = *prow;
			row.push_back(Util::createString(sym[rand() % 4]));
			row.push_back(Util::createInt(i * 12));
			row.push_back(Util::createInt(i + 64));
			datas.push_back(prow);
		}
		mulwrite->insertUnwrittenData(datas, pErrorInfo);
		ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
		EXPECT_EQ(t1->size(), 0);
		mulwrite->waitForThreadCompletion();
		t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
		EXPECT_EQ(t1->size(), 999);
	}
	else
	{
		cout << "server version is not 1.30 and 2.00, please check.";
		EXPECT_EQ(1, 2);
	}
}

// different partition type
TEST_F(MultithreadedTableWriterTest, insertVectorPartitionTableRangeType)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, RANGE,1 5000 15000);\n";
	script += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
			  "pt = db.createPartitionedTable(t,`pt,`id);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1000, 1, 1, "id");
	string sym[] = {"A", "B", "C", "D"};
	vector<vector<ConstantSP> *> datas;
	srand((int)time(NULL));
	for (int i = 0; i < 999; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createString(sym[rand() % 4]));
		row.push_back(Util::createInt(i * 12));
		row.push_back(Util::createInt(i + 64));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
	EXPECT_EQ(t1->size(), 0);
	mulwrite->waitForThreadCompletion();
	t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
	EXPECT_EQ(t1->size(), 998);
}

TEST_F(MultithreadedTableWriterTest, insertVectorPartitionTableVALUEType)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, VALUE,`A`B`C`D);\n";
	script += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
			  "pt = db.createPartitionedTable(t,`pt,`symbol);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1000, 1, 4, "symbol");
	string sym[] = {"A", "B", "C", "D"};
	vector<vector<ConstantSP> *> datas;
	srand((int)time(NULL));
	for (int i = 0; i < 999; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createString(sym[rand() % 4]));
		row.push_back(Util::createInt(i * 12));
		row.push_back(Util::createInt(i + 64));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
	EXPECT_EQ(t1->size(), 0);
	mulwrite->waitForThreadCompletion();
	t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
	EXPECT_EQ(t1->size(), 999);
}

TEST_F(MultithreadedTableWriterTest, insertVectorPartitionTableHASHTSymbolType)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, HASH,[SYMBOL, 4]);\n";
	script += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
			  "pt = db.createPartitionedTable(t,`pt,`symbol);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1000, 1, 5, "symbol");
	string sym[] = {"A", "B", "C", "D"};
	vector<vector<ConstantSP> *> datas;
	srand((int)time(NULL));
	for (int i = 0; i < 999; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createString(sym[rand() % 4]));
		row.push_back(Util::createInt(i * 12));
		row.push_back(Util::createInt(i + 64));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
	EXPECT_EQ(t1->size(), 0);
	mulwrite->waitForThreadCompletion();
	t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
	EXPECT_EQ(t1->size(), 999);
}

TEST_F(MultithreadedTableWriterTest, insertVectorPartitionTableHASHTDateType)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, VALUE,datehour(1970.01.01T02:46:40)+0..4);\n";
	script += "t = table(1000:0, `symbol`date`value,[SYMBOL, DATETIME, INT]);"
			  "pt = db.createPartitionedTable(t,`pt,`date);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1000, 1, 5, "date");
	string sym[] = {"A", "B", "C", "D"};
	vector<vector<ConstantSP> *> datas;
	srand((int)time(NULL));
	for (int i = 0; i < 999; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createString(sym[rand() % 4]));
		row.push_back(Util::createDateTime(i + 10000));
		row.push_back(Util::createInt(i + 64));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	mulwrite->insert(pErrorInfo, "A", Util::createDateTime(999 + 10000), 999 + 64);
	ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt) order by date;");
	EXPECT_EQ(t1->size(), 0);
	mulwrite->waitForThreadCompletion();
	t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
	for (int i = 0; i < 1000; i++)
	{
		EXPECT_EQ(t1->getColumn(1)->getRow(i)->getString(), Util::createDateTime(i + 10000)->getString());
	}
}

TEST_F(MultithreadedTableWriterTest, insertVectorPartitionTableHASHTDateTimeType)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, HASH,[DATETIME, 4]);\n";
	script += "t = table(1000:0, `symbol`date`value,[SYMBOL, DATETIME, INT]);"
			  "pt = db.createPartitionedTable(t,`pt,`date);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1000, 1, 5, "date");
	string sym[] = {"A", "B", "C", "D"};
	vector<vector<ConstantSP> *> datas;
	srand((int)time(NULL));
	for (int i = 0; i < 999; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createString(sym[rand() % 4]));
		row.push_back(Util::createDateTime(i));
		row.push_back(Util::createInt(i + 64));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt) order by date;");
	EXPECT_EQ(t1->size(), 0);
	mulwrite->waitForThreadCompletion();
	t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
	for (int i = 0; i < 999; i++)
	{
		EXPECT_EQ(t1->getColumn(1)->getRow(i)->getString(), Util::createDateTime(i)->getString());
	}
}

TEST_F(MultithreadedTableWriterTest, insertVectorPartitionTableHASHTTimeType)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, HASH,[TIME, 4]);\n";
	script += "t = table(1000:0, `symbol`date`value,[SYMBOL, TIME, INT]);"
			  "pt = db.createPartitionedTable(t,`pt,`date);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1000, 1, 5, "date");
	string sym[] = {"A", "B", "C", "D"};
	vector<vector<ConstantSP> *> datas;
	srand((int)time(NULL));
	for (int i = 0; i < 999; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createString(sym[rand() % 4]));
		row.push_back(Util::createTime(i));
		row.push_back(Util::createInt(i + 64));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt) order by date;");
	EXPECT_EQ(t1->size(), 0);
	mulwrite->waitForThreadCompletion();
	t1 = conn.run("select * from loadTable(dbName,`pt) order by date;");
	for (int i = 0; i < 999; i++)
	{
		EXPECT_EQ(t1->getColumn(1)->getRow(i)->getString(), Util::createTime(i)->getString());
	}
}

TEST_F(MultithreadedTableWriterTest, insertVectorPartitionTableHASHIntType)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, HASH,[INT, 4]);\n";
	script += "t = table(1000:0, `symbol`date`value,[SYMBOL, INT, INT]);"
			  "pt = db.createPartitionedTable(t,`pt,`date);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1000, 1, 5, "date");
	string sym[] = {"A", "B", "C", "D"};
	vector<vector<ConstantSP> *> datas;
	srand((int)time(NULL));
	for (int i = 0; i < 999; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		;
		row.push_back(Util::createString(sym[rand() % 4]));
		row.push_back(Util::createInt(i));
		row.push_back(Util::createInt(i + 64));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt) order by date;");
	EXPECT_EQ(t1->size(), 0);
	mulwrite->waitForThreadCompletion();
	t1 = conn.run("select * from loadTable(dbName,`pt) order by date;");
	for (int i = 0; i < 999; i++)
	{
		EXPECT_EQ(t1->getColumn(1)->getRow(i)->getString(), Util::createInt(i)->getString());
	}
}

TEST_F(MultithreadedTableWriterTest, insertVectorPartitionTableLISTSymbolType)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, LIST,[`A`B`C,`F`D]);\n";
	script += "t = table(1000:0, `symbol`date`value,[SYMBOL, INT, INT]);"
			  "pt = db.createPartitionedTable(t,`pt,`symbol);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1000, 1, 5, "symbol");
	string sym[] = {"A", "B", "C", "D", "F"};
	vector<vector<ConstantSP> *> datas;
	srand((int)time(NULL));
	for (int i = 0; i < 999; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createString(sym[rand() % 5]));
		row.push_back(Util::createInt(i));
		row.push_back(Util::createInt(i + 64));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt) order by date;");
	EXPECT_EQ(t1->size(), 0);
	mulwrite->waitForThreadCompletion();
	t1 = conn.run("select * from loadTable(dbName,`pt) order by date;");
	for (int i = 0; i < 999; i++)
	{
		EXPECT_EQ(t1->getColumn(1)->getRow(i)->getString(), Util::createInt(i)->getString());
	}
}

TEST_F(MultithreadedTableWriterTest, insertVectorPartitionTableLISTIntType)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, LIST,[1..400,401..1000]);\n";
	script += "t = table(1000:0, `symbol`date`value,[SYMBOL, INT, INT]);"
			  "pt = db.createPartitionedTable(t,`pt,`date);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1000, 1, 5, "date");
	string sym[] = {"A", "B", "C", "D", "F"};
	vector<vector<ConstantSP> *> datas;
	srand((int)time(NULL));
	for (int i = 0; i < 999; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createString(sym[rand() % 5]));
		row.push_back(Util::createInt(i));
		row.push_back(Util::createInt(i + 64));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt) order by date;");
	EXPECT_EQ(t1->size(), 0);
	mulwrite->waitForThreadCompletion();
	t1 = conn.run("select * from loadTable(dbName,`pt) order by date;");
	for (int i = 0; i < 998; i++)
	{
		EXPECT_EQ(t1->getColumn(1)->getRow(i)->getString(), Util::createInt(i + 1)->getString());
	}
}

TEST_F(MultithreadedTableWriterTest, insertVectorPartitionTableLISTDateType)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, LIST,[2012.11.12 2012.11.13 2012.11.14,2012.12.12 2012.12.13 2012.12.14]);\n";
	script += "t = table(1000:0, `symbol`date`value,[SYMBOL, DATE, INT]);"
			  "pt = db.createPartitionedTable(t,`pt,`date);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1000, 1, 5, "date");
	int date[] = {15657, 15656, 15658, 15686, 15687, 15688};
	vector<vector<ConstantSP> *> datas;
	srand((int)time(NULL));
	for (int i = 0; i < 999; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createString("A"));
		row.push_back(Util::createDate(date[rand() % 6]));
		row.push_back(Util::createInt(i + 64));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
	EXPECT_EQ(t1->size(), 0);
	mulwrite->waitForThreadCompletion();
	t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
	for (int i = 0; i < 999; i++)
	{
		EXPECT_EQ(t1->getColumn(2)->getRow(i)->getString(), Util::createInt(i + 64)->getString());
	}
}

TEST_F(MultithreadedTableWriterTest, insertVectorPartitionTableCompoDateAndIntType)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\n"
					"}\n"
					"dbDate = database(,VALUE,2012.11.10..2015.01.01)\n"
					"dbId= database(,RANGE,1 564 1200)\n"
					"db  = database(dbName, COMPO,[dbDate, dbId])\n"
					"t = table(1000:0, `date`id`value,[DATE,INT,INT])\n"
					"pt = db.createPartitionedTable(t,`pt,`date`id)";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1000, 1, 5, "date");
	int date[] = {15657, 15656, 15658, 15686, 15687, 15688};
	vector<vector<ConstantSP> *> datas;
	for (int i = 0; i < 999; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createDate(i + 15654));
		row.push_back(Util::createInt(i));
		row.push_back(Util::createInt(i + 64));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
	EXPECT_EQ(t1->size(), 0);
	mulwrite->waitForThreadCompletion();
	t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
	for (int i = 0; i < 998; i++)
	{
		EXPECT_EQ(t1->getColumn(2)->getRow(i)->getString(), Util::createInt(i + 65)->getString());
	}
}

TEST_F(MultithreadedTableWriterTest, insertVectorPartitionTableCompoDateAndSymbolType)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\n"
					"}\n"
					"dbDate = database(,VALUE,2012.11.12 2012.11.13 2012.11.14 2012.12.12 2012.12.13 2012.12.14)\n"
					"dbId= database(,HASH,[SYMBOL,3])\n"
					"db  = database(dbName, COMPO,[dbDate, dbId])\n"
					"t = table(1000:0, `date`id`value,[DATE,SYMBOL,INT])\n"
					"pt = db.createPartitionedTable(t,`pt,`date`id)";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1000, 1, 3, "date");
	int date[] = {15657, 15656, 15658, 15686, 15687, 15688};
	string sym[] = {"A", "B", "C", "D", "F"};
	srand((int)time(NULL));
	vector<vector<ConstantSP> *> datas;
	for (int i = 0; i < 999; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createDate(date[rand() % 6]));
		row.push_back(Util::createString(sym[rand() % 5]));
		row.push_back(Util::createInt(i + 64));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
	EXPECT_EQ(t1->size(), 0);
	mulwrite->waitForThreadCompletion();
	t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
	for (int i = 0; i < 998; i++)
	{
		EXPECT_EQ(t1->getColumn(2)->getRow(i)->getString(), Util::createInt(i + 64)->getString());
	}
}

TEST_F(MultithreadedTableWriterTest, insertVectorPartitionTableCompoDateAndSymbolType_2)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\n"
					"}\n"
					"dbDate = database(,LIST,2012.11.12 2012.11.13 2012.11.14 2012.12.12 2012.12.13 2012.12.14)\n"
					"dbId= database(,HASH,[SYMBOL,3])\n"
					"db  = database(dbName, COMPO,[dbDate, dbId])\n"
					"t = table(1000:0, `date`id`value,[DATE,SYMBOL,INT])\n"
					"pt = db.createPartitionedTable(t,`pt,`date`id)";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1000, 1, 5, "date");
	int date[] = {15657, 15656, 15658, 15686, 15687, 15688};
	string sym[] = {"A", "B", "C", "D", "F"};
	srand((int)time(NULL));
	vector<vector<ConstantSP> *> datas;
	for (int i = 0; i < 999; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createDate(date[rand() % 6]));
		row.push_back(Util::createString(sym[rand() % 5]));
		row.push_back(Util::createInt(i + 64));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
	EXPECT_EQ(t1->size(), 0);
	mulwrite->waitForThreadCompletion();
	t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
	for (int i = 0; i < 998; i++)
	{
		EXPECT_EQ(t1->getColumn(2)->getRow(i)->getString(), Util::createInt(i + 64)->getString());
	}
}

TEST_F(MultithreadedTableWriterTest, insertVectorPartitionTableCompoIntAndSymbolType_2)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\n"
					"}\n"
					"dbDate = database(,RANGE,0 500 1200)\n"
					"dbId= database(,HASH,[SYMBOL,3])\n"
					"db  = database(dbName, COMPO,[dbDate, dbId])\n"
					"t = table(1000:0, `date`id`value,[INT,SYMBOL,INT])\n"
					"pt = db.createPartitionedTable(t,`pt,`date`id)";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1000, 1, 1, "date");
	int date[] = {15657, 15656, 15658, 15686, 15687, 15688};
	string sym[] = {"A", "B", "C", "D", "F"};
	srand((int)time(NULL));
	vector<vector<ConstantSP> *> datas;
	for (int i = 0; i < 999; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createInt(i));
		row.push_back(Util::createString(sym[rand() % 5]));
		row.push_back(Util::createInt(i + 64));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
	EXPECT_EQ(t1->size(), 0);
	mulwrite->waitForThreadCompletion();
	t1 = conn.run("select * from loadTable(dbName,`pt) order by value;");
	for (int i = 0; i < 998; i++)
	{
		EXPECT_EQ(t1->getColumn(2)->getRow(i)->getString(), Util::createInt(i + 64)->getString());
	}
}

TEST_F(MultithreadedTableWriterTest, insertValuesUnWritten)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\n"
					"}\n"
					"db  = database(dbName, VALUE,`A`B`C`D`E)\n"
					"t = table(1000:0, `date`id`value,[DATE,SYMBOL,INT])\n"
					"pt = db.createPartitionedTable(t,`pt,`id)";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 100000, 3, 2, "id");
	vector<vector<ConstantSP> *> data;
	string sym[] = {"A", "B", "C", "D", "E"};
	srand((int)time(NULL));
	for (int i = 0; i < 1000; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createDate(i));
		row.push_back(Util::createString(sym[rand() % 5]));
		row.push_back(Util::createInt(i));
		data.push_back(prow);
	}
	mulwrite->insertUnwrittenData(data, pErrorInfo);
	vector<vector<ConstantSP> *> datas;
	mulwrite->getUnwrittenData(datas);
	EXPECT_EQ(datas.size(), 1000);
}

// Status
TEST_F(MultithreadedTableWriterTest, getStatus)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\n"
					"}\n"
					"db  = database(dbName, VALUE,`A`B`C`D`E)\n"
					"t = table(1000:0, `date`id`value,[DATE,SYMBOL,INT])\n"
					"pt = db.createPartitionedTable(t,`pt,`id)";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 100, 2, 3, "id");
	vector<vector<ConstantSP> *> data;
	string sym[] = {"A", "B", "C", "D", "E"};
	srand((int)time(NULL));
	for (int i = 0; i < 1000; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createDate(i));
		row.push_back(Util::createString(sym[rand() % 5]));
		row.push_back(Util::createInt(i));
		data.push_back(prow);
	}
	mulwrite->insertUnwrittenData(data, pErrorInfo);
	mulwrite->waitForThreadCompletion();
	MultithreadedTableWriter::Status statu;
	mulwrite->getStatus(statu);
	EXPECT_EQ(statu.sentRows, 1000);
	EXPECT_EQ(statu.unsentRows, 0);
	// mulwrite->getStatus();
}

TEST_F(MultithreadedTableWriterTest, getStatus2)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\n"
					"}\n"
					"db  = database(dbName, VALUE,`A`B`C`D`E)\n"
					"t = table(1000:0, `date`id`value,[DATE,SYMBOL,INT])\n"
					"pt = db.createPartitionedTable(t,`pt,`id)";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1000000, 2, 3, "id");
	vector<vector<ConstantSP> *> data;
	string sym[] = {"A", "B", "C", "D", "E"};
	srand((int)time(NULL));
	for (int i = 0; i < 1000; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createDate(i));
		row.push_back(Util::createInt(rand() % 5));
		row.push_back(Util::createInt(i));
		data.push_back(prow);
	}
	MultithreadedTableWriter::Status statu;
	mulwrite->getStatus(statu);
	EXPECT_EQ(statu.errorInfo, pErrorInfo.errorInfo);
}

TEST_F(MultithreadedTableWriterTest, getErrorStatus)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\n"
					"}\n"
					"db  = database(dbName, VALUE,`A`B`C`D`E)\n"
					"t = table(1000:0, `date`id`value,[DATE,SYMBOL,INT])\n"
					"pt = db.createPartitionedTable(t,`pt,`id)";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 100, 2, 3, "id");
	bool flag = mulwrite->insert(pErrorInfo, "A", "A", 23);
	MultithreadedTableWriter::Status statu;
	mulwrite->getStatus(statu);
	cout << statu.hasError() << endl;
	cout << statu.succeed();
}

// olap dimension table
TEST_F(MultithreadedTableWriterTest, threadByColNameNULLOlapDimensionTable)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\n"
					"}\n"
					"db  = database(dbName, VALUE,`A`B`C`D`E)\n"
					"t = table(1000:0, `date`id`value,[DATE,SYMBOL,INT])\n"
					"pt = db.createTable(t,`pt)";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	// mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,1000000,13,5,"");
	EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1000000, 13, 5, ""));
}

TEST_F(MultithreadedTableWriterTest, insertValuesOlapDimensionTableDifferentType)
{
	//
	srand((int)time(NULL));
	int scale32 = rand() % 9;
	int scale64 = rand() % 18;

	ConstantSP decimal32val = Util::createDecimal32(scale32, rand() % 1000 / (double)100);
	ConstantSP decimal64val = Util::createDecimal64(scale64, rand() % 1000 / (double)100);
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, VALUE,`A`B`C`D);\n";
	script += "t = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`deci32`deci64`deci32extra`deci64extra,"
			  "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,DECIMAL32(" +
			  to_string(scale32) + "),DECIMAL64(" + to_string(scale64) + "),DECIMAL32(" + to_string(scale32) + "),DECIMAL64(" + to_string(scale64) + ")]);"
																																					 "pt = db.createTable(t,`pt);";
	conn.run(script);
	Util::sleep(1000);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	unsigned char data[16] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1, 0.1, 1, "symbol");
	bool flag = mulwrite->insert(pErrorInfo, false, char(12), (short)12, (long)12, Util::createDate(2012, 11, 12), Util::createMonth(2012, 11), Util::createSecond(134),
								 Util::createDateTime(2012, 11, 13, 6, 12, 12), Util::createTimestamp(43241), Util::createNanoTime(532432),
								 Util::createNanoTimestamp(85932494), 12.4f, 3412.3, "A", "AAPL", data, data, data, 12, decimal32val, decimal64val, 2.36735, 4);
	// EXPECT_EQ(flag, true);
	mulwrite->waitForThreadCompletion();
	TableSP t1 = conn.run("select * from loadTable(dbName,`pt);");
	EXPECT_EQ(t1->rows(), 1);
	EXPECT_EQ(t1->getColumn(0)->getRow(0)->getString(), "0");
	EXPECT_EQ(t1->getColumn(1)->getRow(0)->getString(), "12");
	EXPECT_EQ(t1->getColumn(2)->getRow(0)->getString(), "12");
	EXPECT_EQ(t1->getColumn(3)->getRow(0)->getString(), "12");
	EXPECT_EQ(t1->getColumn(4)->getRow(0)->getString(), "2012.11.12");
	EXPECT_EQ(t1->getColumn(5)->getRow(0)->getString(), "2012.11M");
	EXPECT_EQ(t1->getColumn(6)->getRow(0)->getString(), "00:02:14");
	EXPECT_EQ(t1->getColumn(7)->getRow(0)->getString(), Util::createDateTime(2012, 11, 13, 6, 12, 12)->getString());
	EXPECT_EQ(t1->getColumn(8)->getRow(0)->getString(), Util::createTimestamp(43241)->getString());
	EXPECT_EQ(t1->getColumn(9)->getRow(0)->getString(), Util::createNanoTime(532432)->getString());
	EXPECT_EQ(t1->getColumn(10)->getRow(0)->getString(), Util::createNanoTimestamp(85932494)->getString());
	EXPECT_EQ(t1->getColumn(11)->getRow(0)->getString(), "12.4");
	EXPECT_EQ(t1->getColumn(12)->getRow(0)->getString(), "3412.3");
	EXPECT_EQ(t1->getColumn(13)->getRow(0)->getString(), "A");
	EXPECT_EQ(t1->getColumn(14)->getRow(0)->getString(), "AAPL");
	EXPECT_EQ(t1->getColumn(15)->getRow(0)->getString(), "0f0e0d0c-0b0a-0908-0706-050403020100");
	EXPECT_EQ(t1->getColumn(16)->getRow(0)->getString(), "f0e:d0c:b0a:908:706:504:302:100");
	EXPECT_EQ(t1->getColumn(17)->getRow(0)->getString(), "0f0e0d0c0b0a09080706050403020100");
	EXPECT_EQ(t1->getColumn(18)->getRow(0)->getString(), "12");
	EXPECT_EQ(t1->getColumn(19)->getRow(0)->getString(), decimal32val->getString());
	EXPECT_EQ(t1->getColumn(20)->getRow(0)->getString(), decimal64val->getString());
	EXPECT_EQ(t1->getColumn(21)->getRow(0)->getString(), Util::createDecimal32(scale32, 2.36735)->getString());
	EXPECT_EQ(t1->getColumn(22)->getRow(0)->getString(), Util::createDecimal64(scale64, 4)->getString());
}

TEST_F(MultithreadedTableWriterTest, insertValuesOlapDimensionTableDifferentTypeDatabiggerThan65535)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, VALUE,`A`B`C`D);\n";
	script += "t = table(1000:0, `bool`char`short`long`date`month`datetime`second`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id,"
			  "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,DATETIME,SECOND,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT]);"
			  "pt = db.createTable(t,`pt);";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	int data[] = {0, 1};
	string syms[] = {"A", "B", "C", "D"};
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1, 0.1, 1, "symbol");
	vector<vector<ConstantSP> *> datas;
	for (int i = 0; i < 3131070; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createBool(data[i % 2]));
		row.push_back(Util::createChar(i * 2));
		row.push_back(Util::createShort(i + 12));
		row.push_back(Util::createLong((long)i * 100));
		row.push_back(Util::createDate(i + 432));
		row.push_back(Util::createMonth(i + 21));
		row.push_back(Util::createDateTime(i * 192));
		row.push_back(Util::createSecond((long long)i * 1932));
		row.push_back(Util::createTimestamp((long long)i * 2342));
		row.push_back(Util::createNanoTime((long long)i * 4214));
		row.push_back(Util::createNanoTimestamp((long long)i * 4264));
		row.push_back(Util::createFloat(i * 42.64));
		row.push_back(Util::createDouble(i * 4.264));
		row.push_back(Util::createString(syms[i % 2]));
		row.push_back(Util::createString("A" + to_string(i)));
		// uuid,ipAddr, int128, blob
		row.push_back(Util::parseConstant(DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100"));
		row.push_back(Util::parseConstant(DT_IP, "192.168.2." + to_string(i % 255)));
		row.push_back(Util::parseConstant(DT_INT128, "0f0e0d0c0b0a09080706050403020100"));
		row.push_back(Util::createInt(i));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	mulwrite->waitForThreadCompletion();
	ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt) order by long;");
	for (int i = 0; i < 2048; i++)
	{
		EXPECT_EQ(t1->getColumn(0)->getRow(i)->getString(), Util::createBool(data[i % 2])->getString());
		EXPECT_EQ(t1->getColumn(1)->getRow(i)->getString(), Util::createChar(i * 2)->getString());
		EXPECT_EQ(t1->getColumn(2)->getRow(i)->getString(), Util::createShort(i + 12)->getString());
		EXPECT_EQ(t1->getColumn(3)->getRow(i)->getString(), Util::createLong((long)i * 100)->getString());
		EXPECT_EQ(t1->getColumn(4)->getRow(i)->getString(), Util::createDate(i + 432)->getString());
		EXPECT_EQ(t1->getColumn(5)->getRow(i)->getString(), Util::createMonth(i + 21)->getString());
		EXPECT_EQ(t1->getColumn(6)->getRow(i)->getString(), Util::createDateTime(i * 192)->getString());
		EXPECT_EQ(t1->getColumn(7)->getRow(i)->getString(), Util::createSecond((long long)i * 1932)->getString());
		EXPECT_EQ(t1->getColumn(8)->getRow(i)->getString(), Util::createTimestamp((long long)i * 2342)->getString());
		EXPECT_EQ(t1->getColumn(9)->getRow(i)->getString(), Util::createNanoTime((long long)i * 4214)->getString());
		EXPECT_EQ(t1->getColumn(10)->getRow(i)->getString(), Util::createNanoTimestamp((long long)i * 4264)->getString());
		EXPECT_EQ(t1->getColumn(11)->getRow(i)->getString(), Util::createFloat(i * 42.64)->getString());
		EXPECT_EQ(t1->getColumn(12)->getRow(i)->getString(), Util::createDouble(i * 4.264)->getString());
		EXPECT_EQ(t1->getColumn(13)->getRow(i)->getString(), Util::createString(syms[i % 2])->getString());
		EXPECT_EQ(t1->getColumn(14)->getRow(i)->getString(), Util::createString("A" + to_string(i))->getString());
		EXPECT_EQ(t1->getColumn(15)->getRow(i)->getString(), Util::parseConstant(DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100")->getString());
		EXPECT_EQ(t1->getColumn(16)->getRow(i)->getString(), Util::parseConstant(DT_IP, "192.168.2." + to_string(i % 255))->getString());
		EXPECT_EQ(t1->getColumn(17)->getRow(i)->getString(), Util::parseConstant(DT_INT128, "0f0e0d0c0b0a09080706050403020100")->getString());
		EXPECT_EQ(t1->getColumn(18)->getRow(i)->getString(), Util::createInt(i)->getString());
		//  cout << t1->getColumn(0)->getRow(i)->getString() << endl;
	}
	// cout<< pErrorInfo.errorInfo <<endl ;
}

// TSDB dimension table
TEST_F(MultithreadedTableWriterTest, threadByColNameNULLTSDBDimensionTable)
{
	ConstantSP server_version = conn.run("version()");
	string v = server_version->getString();
	if (v.find("1.30") == 0)
	{
		cout << "server version: 1.30.xx, skip this case." << endl;
		EXPECT_EQ(1, 1);
	}
	else if (v.find("2.00") == 0)
	{
		string dbName = "dfs://test_MultithreadedTableWriter";
		string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
						"if(exists(dbName)){\n"
						"\tdropDatabase(dbName)\t\n"
						"}\n"
						"db  = database(dbName, VALUE,symbol(\"A\"+string(0..10)),,`TSDB);\n"
						"t = table(1000:0, `sym`id`value,[SYMBOL, INT, INT])\n"
						"pt = db.createTable(t,`pt,,`sym)";
		conn.run(script);
		SmartPointer<MultithreadedTableWriter> mulwrite;
		ErrorCodeInfo pErrorInfo;
		// mulwrite =new MultithreadedTableWriter(hostName,port, "admin","123456",dbName,"pt",NULL,1000000,13,5,"");
		EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 1000000, 13, 5, ""));
		string script2 = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
						 "if(exists(dbName)){\n"
						 "\tdropDatabase(dbName)\t\n"
						 "}\n";
		conn.run(script2);
	}
	else
	{
		cout << "server version is not 1.30 and 2.00, please check.";
		EXPECT_EQ(1, 2);
	}
}

TEST_F(MultithreadedTableWriterTest, insertValuesTSDBDimensionTableDifferentType)
{
	ConstantSP server_version = conn.run("version()");
	string v = server_version->getString();
	if (v.find("1.30") == 0)
	{
		cout << "server version: 1.30.xx, skip this case." << endl;
		EXPECT_EQ(1, 1);
	}
	else if (v.find("2.00") == 0)
	{
		string dbName = "dfs://test_MultithreadedTableWriter";
		string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
						"if(exists(dbName)){\n"
						"\tdropDatabase(dbName)\t\n"
						"}\n"
						"db  = database(dbName, VALUE,`A`B`C`D,,`TSDB);\n";
		script += "t = table(1000:0, `bool`char`short`long`date`month`second`datetime`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`bolb,"
				  "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,BLOB]);"
				  "pt = db.createTable(t,`pt,,`symbol);";
		conn.run(script);
		Util::sleep(3000);
		SmartPointer<MultithreadedTableWriter> mulwrite;
		ErrorCodeInfo pErrorInfo;
		unsigned char data[16] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
		mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", NULL, false, nullptr, 1, 0.1, 1, "symbol");
		bool flag = mulwrite->insert(pErrorInfo, false, char(12), (short)12, (long)12,
									 Util::createDate(2012, 11, 12), Util::createMonth(2012, 11), Util::createSecond(134),
									 Util::createDateTime(2012, 11, 13, 6, 12, 12), Util::createTimestamp(43241),
									 Util::createNanoTime(532432),
									 Util::createNanoTimestamp(85932494), 12.4f, 3412.3, "A", "AAPL", data, data, data, 12,
									 "0f0e0d0c0b0a0908070605040302010");
		// EXPECT_EQ(flag, true);
		mulwrite->waitForThreadCompletion();
		ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt);");
		EXPECT_EQ(t1->getColumn(0)->getRow(0)->getString(), "0");
		EXPECT_EQ(t1->getColumn(1)->getRow(0)->getString(), "12");
		EXPECT_EQ(t1->getColumn(2)->getRow(0)->getString(), "12");
		EXPECT_EQ(t1->getColumn(3)->getRow(0)->getString(), "12");
		EXPECT_EQ(t1->getColumn(4)->getRow(0)->getString(), "2012.11.12");
		EXPECT_EQ(t1->getColumn(5)->getRow(0)->getString(), "2012.11M");
		EXPECT_EQ(t1->getColumn(6)->getRow(0)->getString(), "00:02:14");
		EXPECT_EQ(t1->getColumn(7)->getRow(0)->getString(), Util::createDateTime(2012, 11, 13, 6, 12, 12)->getString());
		EXPECT_EQ(t1->getColumn(8)->getRow(0)->getString(), Util::createTimestamp(43241)->getString());
		EXPECT_EQ(t1->getColumn(9)->getRow(0)->getString(), Util::createNanoTime(532432)->getString());
		EXPECT_EQ(t1->getColumn(10)->getRow(0)->getString(), Util::createNanoTimestamp(85932494)->getString());
		EXPECT_EQ(t1->getColumn(11)->getRow(0)->getString(), "12.4");
		EXPECT_EQ(t1->getColumn(12)->getRow(0)->getString(), "3412.3");
		EXPECT_EQ(t1->getColumn(13)->getRow(0)->getString(), "A");
		EXPECT_EQ(t1->getColumn(14)->getRow(0)->getString(), "AAPL");
		EXPECT_EQ(t1->getColumn(15)->getRow(0)->getString(), "0f0e0d0c-0b0a-0908-0706-050403020100");
		EXPECT_EQ(t1->getColumn(16)->getRow(0)->getString(), "f0e:d0c:b0a:908:706:504:302:100");
		EXPECT_EQ(t1->getColumn(17)->getRow(0)->getString(), "0f0e0d0c0b0a09080706050403020100");
		EXPECT_EQ(t1->getColumn(19)->getRow(0)->getString(), "0f0e0d0c0b0a0908070605040302010");
	}
	else
	{
		cout << "server version is not 1.30 and 2.00, please check.";
		EXPECT_EQ(1, 2);
	}
}

TEST_F(MultithreadedTableWriterTest, insertValuesTSDBDimensionTableBiggerThan1048576)
{
	ConstantSP server_version = conn.run("version()");
	string v = server_version->getString();
	if (v.find("1.30") == 0)
	{
		cout << "server version: 1.30.xx, skip this case." << endl;
		EXPECT_EQ(1, 1);
	}
	else if (v.find("2.00") == 0)
	{
		string dbName = "dfs://test_MultithreadedTableWriter";
		string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
						"if(exists(dbName)){\n"
						"\tdropDatabase(dbName)\t\n"
						"}\n"
						"db  = database(dbName, VALUE,`A`B`C`D,,`TSDB);\n";
		script += "t = table(1000:0, `bool`char`short`long`date`month`datetime`second`timestamp`nanotime`nanotimestamp`float`double`symbol`string`uuid`ipaddr`int128`id`bolb,"
				  "[BOOL,CHAR,SHORT,LONG,DATE,MONTH,DATETIME,SECOND,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,BLOB]);"
				  "pt = db.createTable(t,`pt,,`symbol);";
		conn.run(script);
		Util::sleep(3000);
		SmartPointer<MultithreadedTableWriter> mulwrite;
		ErrorCodeInfo pErrorInfo;
		unsigned char data[16] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
		string syms[] = {"A", "B", "C", "D"};
		mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", NULL, false, nullptr, 1, 0.1, 1, "symbol");
		vector<vector<ConstantSP> *> datas;
		for (int i = 0; i < 3098576; i++)
		{
			vector<ConstantSP> *prow = new vector<ConstantSP>;
			vector<ConstantSP> &row = *prow;
			row.push_back(Util::createBool(data[i % 2]));
			row.push_back(Util::createChar(i * 2));
			row.push_back(Util::createShort(i + 12));
			row.push_back(Util::createLong((long)i * 100));
			row.push_back(Util::createDate(i + 432));
			row.push_back(Util::createMonth(i + 21));
			row.push_back(Util::createDateTime(i * 192));
			row.push_back(Util::createSecond((long long)i * 1932));
			row.push_back(Util::createTimestamp((long long)i * 2342));
			row.push_back(Util::createNanoTime((long long)i * 4214));
			row.push_back(Util::createNanoTimestamp((long long)i * 4264));
			row.push_back(Util::createFloat(i * 42.64));
			row.push_back(Util::createDouble(i * 4.264));
			row.push_back(Util::createString(syms[i % 4]));
			row.push_back(Util::createString(std::to_string(i)));
			// uuid,ipAddr, int128, blob
			row.push_back(Util::parseConstant(DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100"));
			row.push_back(Util::parseConstant(DT_IP, "192.168.2." + to_string(i % 255)));
			row.push_back(Util::parseConstant(DT_INT128, "0f0e0d0c0b0a09080706050403020100"));
			row.push_back(Util::createInt(i));
			row.push_back(Util::createBlob("0f0e0d0c0b0a0908070605040302010" + to_string(i)));
			datas.push_back(prow);
		}
		mulwrite->insertUnwrittenData(datas, pErrorInfo);
		mulwrite->waitForThreadCompletion();
		ConstantSP t1 = conn.run("select * from loadTable(dbName,`pt) order by long;");
		// cout << t1->getColumn(19)->getRow(1)->getString();
		for (int i = 0; i < 165536; i++)
		{
			EXPECT_EQ(t1->getColumn(0)->getRow(i)->getString(), Util::createBool(data[i % 2])->getString());
			EXPECT_EQ(t1->getColumn(1)->getRow(i)->getString(), Util::createChar(i * 2)->getString());
			EXPECT_EQ(t1->getColumn(2)->getRow(i)->getString(), Util::createShort(i + 12)->getString());
			EXPECT_EQ(t1->getColumn(3)->getRow(i)->getString(), Util::createLong((long)i * 100)->getString());
			EXPECT_EQ(t1->getColumn(4)->getRow(i)->getString(), Util::createDate(i + 432)->getString());
			EXPECT_EQ(t1->getColumn(5)->getRow(i)->getString(), Util::createMonth(i + 21)->getString());
			EXPECT_EQ(t1->getColumn(6)->getRow(i)->getString(), Util::createDateTime(i * 192)->getString());
			EXPECT_EQ(t1->getColumn(7)->getRow(i)->getString(), Util::createSecond((long long)i * 1932)->getString());
			EXPECT_EQ(t1->getColumn(8)->getRow(i)->getString(), Util::createTimestamp((long long)i * 2342)->getString());
			EXPECT_EQ(t1->getColumn(9)->getRow(i)->getString(), Util::createNanoTime((long long)i * 4214)->getString());
			EXPECT_EQ(t1->getColumn(10)->getRow(i)->getString(), Util::createNanoTimestamp((long long)i * 4264)->getString());
			EXPECT_EQ(t1->getColumn(11)->getRow(i)->getString(), Util::createFloat(i * 42.64)->getString());
			EXPECT_EQ(t1->getColumn(12)->getRow(i)->getString(), Util::createDouble(i * 4.264)->getString());
			EXPECT_EQ(t1->getColumn(13)->getRow(i)->getString(), Util::createString(syms[i % 4])->getString());
			EXPECT_EQ(t1->getColumn(14)->getRow(i)->getString(), Util::createString(std::to_string(i))->getString());
			EXPECT_EQ(t1->getColumn(15)->getRow(i)->getString(), Util::parseConstant(DT_UUID, "0f0e0d0c-0b0a-0908-0706-050403020100")->getString());
			EXPECT_EQ(t1->getColumn(16)->getRow(i)->getString(), Util::parseConstant(DT_IP, "192.168.2." + to_string(i % 255))->getString());
			EXPECT_EQ(t1->getColumn(17)->getRow(i)->getString(), Util::parseConstant(DT_INT128, "0f0e0d0c0b0a09080706050403020100")->getString());
			EXPECT_EQ(t1->getColumn(18)->getRow(i)->getString(), Util::createInt(i)->getString());
			EXPECT_EQ(t1->getColumn(19)->getRow(i)->getString(), Util::createString("0f0e0d0c0b0a0908070605040302010" + to_string(i))->getString());
			//  cout << t1->getColumn(0)->getRow(i)->getString() << endl;
		}
	}
	else
	{
		cout << "server version is not 1.30 and 2.00, please check.";
		EXPECT_EQ(1, 2);
	}
}

TEST_F(MultithreadedTableWriterTest, insertMoreColumns)
{
	string script = "colName = [`id]\n"
					"for(i in 1..400){\n"
					"\tcolName.append!(\"factor\"+string(i))\t\t\n"
					"}\n"
					"colType = [SYMBOL]\n"
					"for(i in 1..400){\n"
					"\tcolType.append!(DOUBLE)\t\n"
					"}\n"
					"t = table(1:0,colName, colType)"
					"share t as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", NULL, false, nullptr, 10, 0.1, 3, "id");
	vector<vector<ConstantSP> *> datas;
	for (int i = 0; i < 50; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &row = *prow;
		row.push_back(Util::createString("S" + to_string(i)));
		for (int j = 0; j < 400; j++)
		{
			row.push_back(Util::createDouble(i + j));
		}
		datas.push_back(prow);
	}
	MultithreadedTableWriter::Status status;
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	for (int i = 0; i < 10; i++)
	{
		mulwrite->getStatus(status);
		if (status.sentRows == 50)
		{
			ConstantSP t1 = conn.run("select * from t1 order by factor1;");
			EXPECT_EQ(t1->size(), 50);
			for (int j = 0; j < 50; j++)
			{
				EXPECT_EQ(t1->getColumn(0)->getString(j), Util::createString("S" + to_string(j))->getString());
				for (int m = 1; m < 401; m++)
				{
					EXPECT_EQ(t1->getColumn(m)->getString(j), Util::createDouble(j + m - 1)->getString());
				}
			}
			break;
		}
		else
		{
			mulwrite->waitForThreadCompletion();
		}
	}
	// ConstantSP t1 = conn.run("select * from t1;");
	// cout << t1->size();
}

TEST_F(MultithreadedTableWriterTest, insertArrayVector)
{
	ConstantSP server_version = conn.run("version()");
	string v = server_version->getString();
	if (v.find("1.30") == 0)
	{
		cout << "server version: 1.30.xx, skip this case." << endl;
		EXPECT_EQ(1, 1);
	}
	else if (v.find("2.00") == 0)
	{
		string script = "share streamTable(1:0,`id`name`value,[INT,SYMBOL,LONG[]]) as t1;";
		conn.run(script);
		SmartPointer<MultithreadedTableWriter> mulwrite;
		ErrorCodeInfo pErrorInfo;
		mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 10, 0.1, 3, "id");
		vector<vector<ConstantSP> *> datas;
		// arrayVector
		vector<int> val(10);
		for (int i = 0; i < 10; i++)
		{
			val[i] = i + 10;
		}
		int values[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
		VectorSP array = Util::createVector(DT_LONG, 10, 10);
		array->setInt(0, 10, val.data());
		// cout << array->getString();
		VectorSP pvectorAny = Util::createVector(DT_ANY, 0, 1);
		pvectorAny->append(array);
		for (int i = 0; i < 100; i++)
		{
			vector<ConstantSP> *prow = new vector<ConstantSP>;
			vector<ConstantSP> &rows = *prow;
			rows.push_back(Util::createInt(i));
			rows.push_back(Util::createString("A"));
			rows.push_back(pvectorAny);
			datas.push_back(prow);
		}
		MultithreadedTableWriter ::Status status;
		mulwrite->insertUnwrittenData(datas, pErrorInfo);
		mulwrite->waitForThreadCompletion();
		mulwrite->getStatus(status);
		TableSP t1 = conn.run("select * from t1 order by id");
		// cout<<t1->getRow(0)->values()->get(1)->getString()<<endl;
		for (int i = 0; i < 100; i++)
		{
			string rowvals = t1->getRow(i)->values()->getString();
			string colval = (*(*datas[i])[2]).getString().substr(1, 31);
			EXPECT_FALSE(rowvals.find(colval) == string::npos);
		}
	}
	else
	{
		cout << "server version is not 1.30 and 2.00, please check.";
		EXPECT_EQ(1, 2);
	}
}

TEST_F(MultithreadedTableWriterTest, insertArrayVectordiffType)
{
	ConstantSP server_version = conn.run("version()");
	string v = server_version->getString();
	if (v.find("1.30") == 0)
	{
		cout << "server version: 1.30.xx, skip this case." << endl;
		EXPECT_EQ(1, 1);
	}
	else if (v.find("2.00") == 0)
	{
		DBConnection conn1(false, false, 7200, true);
		conn1.connect(hostName, port, "admin", "123456");
		string script = "colName = [`id,`name]\n"
						"for(i in 1..17){\n"
						"\tcolName.append!(\"factor\"+string(i))\t\n"
						"}\n"
						"colType =[INT,SYMBOL,BOOL[], CHAR[], SHORT[], INT[], LONG[], FLOAT[], DOUBLE[], DATE[], TIMESTAMP[], DATEHOUR[], DATETIME[], TIME[], MINUTE[], MONTH[], SECOND[], NANOTIME[], NANOTIMESTAMP[]]\n"
						"share streamTable(1:0,colName,colType) as t1;";
		conn1.run(script);
		SmartPointer<MultithreadedTableWriter> mulwrite;
		ErrorCodeInfo pErrorInfo;
		mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 10, 0.1, 3, "id");
		vector<vector<ConstantSP> *> datas;
		// arrayVector
		vector<int> val(10);
		for (int i = 0; i < 10; i++)
		{
			val[i] = i + 10;
		}
		string names[] = {"A", "B", "C"};
		VectorSP boolvector = Util::createVector(DT_BOOL, 10, 10);
		VectorSP charvector = Util::createVector(DT_CHAR, 10, 10);
		VectorSP shortvector = Util::createVector(dolphindb::DT_SHORT, 10, 10);
		VectorSP intvector = Util::createVector(dolphindb::DT_INT, 10, 10);
		VectorSP longvector = Util::createVector(DT_LONG, 10, 10);
		VectorSP floatvector = Util::createVector(dolphindb::DT_FLOAT, 10, 10);
		VectorSP doublevector = Util::createVector(DT_DOUBLE, 10, 10);
		VectorSP datevector = Util::createVector(dolphindb::DT_DATE, 10, 10);
		VectorSP timestampvector = Util::createVector(dolphindb::DT_TIMESTAMP, 10, 10);
		VectorSP datehourvector = Util::createVector(dolphindb::DT_DATEHOUR, 10, 10);
		VectorSP datetimevector = Util::createVector(dolphindb::DT_DATETIME, 10, 10);
		VectorSP timevector = Util::createVector(DT_TIME, 10, 10);
		VectorSP minutevector = Util::createVector(DT_MINUTE, 10, 10);
		VectorSP monthvector = Util::createVector(dolphindb::DT_MONTH, 10, 10);
		VectorSP secondvector = Util::createVector(DT_SECOND, 10, 10);
		VectorSP nanotimevector = Util::createVector(dolphindb::DT_NANOTIME, 10, 10);
		VectorSP nanotimestampVector = Util::createVector(dolphindb::DT_NANOTIMESTAMP, 10, 10);

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
		vector<VectorSP> pvectorAny;
		for (int i = 0; i < 17; i++)
		{
			pvectorAny.push_back(Util::createVector(DT_ANY, 0, 1));
		}

		// cout << array->getString();
		// VectorSP pvectorAny=Util::createVector(DT_ANY,0,17);
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
			vector<ConstantSP> *prow = new vector<ConstantSP>;
			vector<ConstantSP> &rows = *prow;
			rows.push_back(Util::createInt(i));
			rows.push_back(Util::createString(names[i % 3]));
			for (int j = 0; j < 17; j++)
			{
				rows.push_back(pvectorAny[j]);
			}
			datas.push_back(prow);
		}
		MultithreadedTableWriter ::Status status;
		mulwrite->insertUnwrittenData(datas, pErrorInfo);
		mulwrite->waitForThreadCompletion();
		TableSP t1 = conn1.run("select * from t1");

		// EXPECT_EQ(t1->size(), 100);
		for (int i = 0; i < 100; i++)
		{
			for (int j = 2; j < 19; j++)
			{
				// cout << (*(*datas[i])[j]).getString();
				EXPECT_EQ("(" + t1->getColumn(j)->getRow(i)->getString() + ")", pvectorAny[j - 2]->getString());
			}
		}
		conn1.close();
	}
	else
	{
		cout << "server version is not 1.30 and 2.00, please check.";
		EXPECT_EQ(1, 2);
	}
}

TEST_F(MultithreadedTableWriterTest, vectorinsertdifftype)
{
	// int, short, char, long|| float,double
	string script = "share streamTable(1:0,`cint`cshort`cchar`clong`cfloat`cdouble,[INT,SHORT,CHAR,LONG,FLOAT,DOUBLE]) as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 1, 0.1, 3, "cint");
	mulwrite->insert(pErrorInfo, char(12), char(32), char(11), char(43), float(12.4), float(12.8));
	mulwrite->insert(pErrorInfo, short(12), short(32), short(11), short(43), float(12.4), float(12.8));
	mulwrite->insert(pErrorInfo, long(12), long(32), long(11), long(43), double(12.4), double(12.8));
	mulwrite->insert(pErrorInfo, int(12), int(32), int(11), int(43), double(12.4), double(12.8));
	mulwrite->waitForThreadCompletion();
	TableSP t1 = conn.run("select * from t1");
	EXPECT_EQ(t1->getColumn(0)->getString(), "[12,12,12,12]");
	EXPECT_EQ(t1->getColumn(1)->getString(), "[32,32,32,32]");
	EXPECT_EQ(t1->getColumn(2)->getString(), "[11,11,11,11]");
	EXPECT_EQ(t1->getColumn(3)->getString(), "[43,43,43,43]");
	EXPECT_EQ(t1->getColumn(4)->getString(), "[12.4,12.4,12.4,12.4]");
	EXPECT_EQ(t1->getColumn(5)->getString(), "[12.8,12.8,12.8,12.8]");
}

TEST_F(MultithreadedTableWriterTest, insertWithCompressVectorLessThanTable)
{
	// int, short, char, long|| float,double
	string script = "share streamTable(1:0,`cint`cshort`cchar`clong`cfloat`cdouble,[INT,SHORT,CHAR,LONG,FLOAT,DOUBLE]) as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;

	vector<COMPRESS_METHOD> compress = {COMPRESS_DELTA, COMPRESS_DELTA, COMPRESS_DELTA, COMPRESS_DELTA, COMPRESS_DELTA};
	EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 1, 0.1, 3, "cint", &compress));
}

TEST_F(MultithreadedTableWriterTest, insertWithCompressVectorlongerThanTable)
{
	// int, short, char, long|| float,double
	string script = "share streamTable(1:0,`cint`cshort`cchar`clong`cfloat`cdouble,[INT,SHORT,CHAR,LONG,FLOAT,DOUBLE]) as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;

	vector<COMPRESS_METHOD> compress = {COMPRESS_DELTA, COMPRESS_DELTA, COMPRESS_DELTA, COMPRESS_DELTA, COMPRESS_DELTA, COMPRESS_DELTA, COMPRESS_DELTA};
	EXPECT_ANY_THROW(new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 1, 0.1, 3, "cint", &compress));
}

TEST_F(MultithreadedTableWriterTest, insertWithCompress)
{
	// int, short, char, long|| float,double
	string script = "share streamTable(1:0,`cint`cshort`cchar`clong`cfloat`cdouble,[INT,SHORT,CHAR,LONG,FLOAT,DOUBLE]) as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;

	vector<COMPRESS_METHOD> compress = {COMPRESS_DELTA, COMPRESS_DELTA, COMPRESS_LZ4, COMPRESS_DELTA, COMPRESS_LZ4, COMPRESS_LZ4};
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 1, 0.1, 3, "cint", &compress);
	int count = 6000000;
	vector<vector<ConstantSP> *> datas;
	VectorSP cint = Util::createVector(DT_INT, count, count);
	VectorSP cshort = Util::createVector(DT_SHORT, count, count);
	VectorSP cchar = Util::createVector(DT_CHAR, count, count);
	VectorSP clong = Util::createVector(DT_LONG, count, count);
	VectorSP cfloat = Util::createVector(DT_FLOAT, count, count);
	VectorSP cdouble = Util::createVector(DT_DOUBLE, count, count);
	for (int i = 0; i < count; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &rows = *prow;
		rows.push_back(Util::createInt(i));
		cint->setInt(i, i);
		rows.push_back(Util::createShort(i + 1));
		cshort->setShort(i, i + 1);
		rows.push_back(Util::createChar(i + 2));
		cchar->setChar(i, i + 2);
		rows.push_back(Util::createLong(i + 3));
		clong->setLong(i, i + 3);
		rows.push_back(Util::createFloat(i + 4));
		cfloat->setFloat(i, i + 4);
		rows.push_back(Util::createDouble(i + 5));
		cdouble->setDouble(i, i + 5);
		datas.push_back(prow);
	}

	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	mulwrite->waitForThreadCompletion();
	TableSP t1 = conn.run("select * from t1 order by cint");
	vector<string> colName{"cint", "cshort", "cchar", "clong", "cfloat", "cdouble"};
	vector<ConstantSP> colData{cint, cshort, cchar, clong, cfloat, cdouble};
	TableSP exception = Util::createTable(colName, colData);
	EXPECT_EQ(t1->getString(), exception->getString());
}

TEST_F(MultithreadedTableWriterTest, inserTimetWithCompress)
{
	// int, short, char, long|| float,double
	string script = "share streamTable(1:0,`cint`cshort`cchar`clong`cfloat`cdouble,[INT,SHORT,CHAR,LONG,FLOAT,DOUBLE]) as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;

	vector<COMPRESS_METHOD> compress = {COMPRESS_DELTA, COMPRESS_DELTA, COMPRESS_LZ4, COMPRESS_DELTA, COMPRESS_LZ4, COMPRESS_LZ4};
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 1, 0.1, 3, "cint", &compress);
	int count = 6000000;
	vector<vector<ConstantSP> *> datas;
	VectorSP cint = Util::createVector(DT_INT, count, count);
	VectorSP cshort = Util::createVector(DT_SHORT, count, count);
	VectorSP cchar = Util::createVector(DT_CHAR, count, count);
	VectorSP clong = Util::createVector(DT_LONG, count, count);
	VectorSP cfloat = Util::createVector(DT_FLOAT, count, count);
	VectorSP cdouble = Util::createVector(DT_DOUBLE, count, count);
	for (int i = 0; i < count; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &rows = *prow;
		rows.push_back(Util::createInt(i));
		cint->setInt(i, i);
		rows.push_back(Util::createShort(i + 1));
		cshort->setShort(i, i + 1);
		rows.push_back(Util::createChar(i + 2));
		cchar->setChar(i, i + 2);
		rows.push_back(Util::createLong(i + 3));
		clong->setLong(i, i + 3);
		rows.push_back(Util::createFloat(i + 4));
		cfloat->setFloat(i, i + 4);
		rows.push_back(Util::createDouble(i + 5));
		cdouble->setDouble(i, i + 5);
		datas.push_back(prow);
	}

	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	mulwrite->waitForThreadCompletion();
	TableSP t1 = conn.run("select * from t1 order by cint");
	vector<string> colName{"cint", "cshort", "cchar", "clong", "cfloat", "cdouble"};
	vector<ConstantSP> colData{cint, cshort, cchar, clong, cfloat, cdouble};
	TableSP exception = Util::createTable(colName, colData);
	EXPECT_EQ(t1->getString(), exception->getString());
}

TEST_F(MultithreadedTableWriterTest, insertArrayDiffTypeWithCompress)
{
	ConstantSP server_version = conn.run("version()");
	string v = server_version->getString();
	if (v.find("1.30") == 0)
	{
		cout << "server version: 1.30.xx, skip this case." << endl;
		EXPECT_EQ(1, 1);
	}
	else if (v.find("2.00") == 0)
	{
		DBConnection conn1(false, false, 7200, true);
		conn1.connect(hostName, port, "admin", "123456");
		string script = "colName = [`id,`name]\n"
						"for(i in 1..17){\n"
						"\tcolName.append!(\"factor\"+string(i))\t\n"
						"}\n"
						"colType =[INT,SYMBOL,BOOL[], CHAR[], SHORT[], INT[], LONG[], FLOAT[], DOUBLE[], DATE[], TIMESTAMP[], DATEHOUR[], DATETIME[], TIME[], MINUTE[], MONTH[], SECOND[], NANOTIME[], NANOTIMESTAMP[]]\n"
						"share streamTable(1:0,colName,colType) as t1;";
		conn1.run(script);
		SmartPointer<MultithreadedTableWriter> mulwrite;
		ErrorCodeInfo pErrorInfo;
		vector<COMPRESS_METHOD> typeVec(19);
		typeVec[0] = COMPRESS_DELTA;
		typeVec[1] = COMPRESS_DELTA;
		for (int i = 0; i < 17; i++)
		{
			typeVec[i + 2] = COMPRESS_LZ4;
		}
		mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 10, 0.1, 3, "id", &typeVec);
		vector<vector<ConstantSP> *> datas;
		// arrayVector
		vector<int> val(10);
		for (int i = 0; i < 10; i++)
		{
			val[i] = i + 10;
		}
		string names[] = {"A", "B", "C"};
		VectorSP boolvector = Util::createVector(DT_BOOL, 10, 10);
		VectorSP charvector = Util::createVector(DT_CHAR, 10, 10);
		VectorSP shortvector = Util::createVector(dolphindb::DT_SHORT, 10, 10);
		VectorSP intvector = Util::createVector(dolphindb::DT_INT, 10, 10);
		VectorSP longvector = Util::createVector(DT_LONG, 10, 10);
		VectorSP floatvector = Util::createVector(dolphindb::DT_FLOAT, 10, 10);
		VectorSP doublevector = Util::createVector(DT_DOUBLE, 10, 10);
		VectorSP datevector = Util::createVector(dolphindb::DT_DATE, 10, 10);
		VectorSP timestampvector = Util::createVector(dolphindb::DT_TIMESTAMP, 10, 10);
		VectorSP datehourvector = Util::createVector(dolphindb::DT_DATEHOUR, 10, 10);
		VectorSP datetimevector = Util::createVector(dolphindb::DT_DATETIME, 10, 10);
		VectorSP timevector = Util::createVector(DT_TIME, 10, 10);
		VectorSP minutevector = Util::createVector(DT_MINUTE, 10, 10);
		VectorSP monthvector = Util::createVector(dolphindb::DT_MONTH, 10, 10);
		VectorSP secondvector = Util::createVector(DT_SECOND, 10, 10);
		VectorSP nanotimevector = Util::createVector(dolphindb::DT_NANOTIME, 10, 10);
		VectorSP nanotimestampVector = Util::createVector(dolphindb::DT_NANOTIMESTAMP, 10, 10);

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
		vector<VectorSP> pvectorAny;
		for (int i = 0; i < 17; i++)
		{
			pvectorAny.push_back(Util::createVector(DT_ANY, 0, 1));
		}

		// cout << array->getString();
		//  VectorSP pvectorAny=Util::createVector(DT_ANY,0,17);
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
			vector<ConstantSP> *prow = new vector<ConstantSP>;
			vector<ConstantSP> &rows = *prow;
			rows.push_back(Util::createInt(i));
			rows.push_back(Util::createString(names[i % 3]));
			for (int j = 0; j < 17; j++)
			{
				rows.push_back(pvectorAny[j]);
			}
			datas.push_back(prow);
		}
		MultithreadedTableWriter ::Status status;
		mulwrite->insertUnwrittenData(datas, pErrorInfo);
		mulwrite->waitForThreadCompletion();
		TableSP t1 = conn1.run("select * from t1 order by id");
		// EXPECT_EQ(t1->size(), 100);
		for (int i = 0; i < 100; i++)
		{
			for (int j = 2; j < 19; j++)
			{
				EXPECT_EQ("(" + t1->getColumn(j)->getRow(i)->getString() + ")", pvectorAny[j - 2]->getString());
			}
		}
		conn1.close();
	}
	else
	{
		cout << "server version is not 1.30 and 2.00, please check.";
		EXPECT_EQ(1, 2);
	}
}

TEST_F(MultithreadedTableWriterTest, vectorIntable)
{
	ConstantSP server_version = conn.run("version()");
	string v = server_version->getString();
	if (v.find("1.30") == 0)
	{
		cout << "server version: 1.30.xx, skip this case." << endl;
		EXPECT_EQ(1, 1);
	}
	else if (v.find("2.00") == 0)
	{
		const int count = 6;
		vector<string> colName = {"time", "value"};
		vector<int> time(24);
		vector<long long> value(count);
		vector<int> index = {1, 3, 6, 11, 14, 24};
		int basetime = Util::countDays(2012, 1, 1);
		for (int i = 0; i < 24; i++)
		{
			if (i < 6)
			{
				value[i] = i + 10000;
			}
			time[i] = basetime + (i % 15);
		}
		VectorSP idvector = Util::createVector(DT_INT, count, count);
		VectorSP timeVector = Util::createVector(DT_DATE, 24, count);
		VectorSP valueVector = Util::createVector(DT_LONG, count, count);
		timeVector->setInt(0, 24, time.data());
		valueVector->setLong(0, count, value.data());
		idvector->setInt(0, count, index.data());
		VectorSP timearray = Util::createArrayVector(idvector, timeVector);
		vector<ConstantSP> colVector{timearray, valueVector};
		TableSP table = Util::createTable(colName, colVector);

		vector<ConstantSP> args{table};
		conn.run("share streamTable(1:0, `time`value,[DATE[],LONG]) as table1");
		int success = conn.run("tableInsert{table1}", args)->getInt();
		EXPECT_EQ(success, count);

		TableSP t1 = conn.run("select * from table1");
		EXPECT_EQ(t1->getString(), table->getString());
	}
	else
	{
		cout << "server version is not 1.30 and 2.00, please check.";
		EXPECT_EQ(1, 2);
	}
}

TEST_F(MultithreadedTableWriterTest, TimeTypedata)
{
	string script = "share streamTable(1:0, `cint`csym`cdate`cdatetime`cdatehour`cmonth`ctime`ctimestamp`cminute`second`nanotime`nanotimestamp,"
					"[INT,SYMBOL,DATE,DATETIME,DATEHOUR,MONTH,TIME,TIMESTAMP,MINUTE,SECOND,NANOTIME,NANOTIMESTAMP]) as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 10, 0.1, 3, "cint");
	//    time_t now = time(NULL);
	//    struct tm *ltm = localtime(&now);
	//    bool flag = mulwrite->insert(pErrorInfo,43,"A",ltm, ltm, ltm, ltm, ltm, ltm,ltm,ltm,ltm,ltm);
	//    EXPECT_EQ(flag,false);
	bool flag2 = mulwrite->insert(pErrorInfo, 43, "A", long(421), 421, 421, 421, long(421), long(421), 421, long(421), long(421), long(432));
	cout << flag2 << endl;
	mulwrite->waitForThreadCompletion();
	ConstantSP t1 = conn.run("select * from objByName(`t1)");
	cout << t1->getString();
	cout << pErrorInfo.errorInfo << endl;
}

// temporal compatible
TEST_F(MultithreadedTableWriterTest, PartitionSchemeDatehour)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
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
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	SmartPointer<MultithreadedTableWriter> mulwrite2;
	SmartPointer<MultithreadedTableWriter> mulwrite3;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 10, 0.1, 3, "data");
	// datatime
	vector<vector<ConstantSP> *> datas;
	for (int i = 0; i < 100; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &rows = *prow;
		rows.push_back(Util::createString("A"));
		rows.push_back(Util::createInt(i));
		rows.push_back(Util::createDateTime(i));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	mulwrite->waitForThreadCompletion();
	TableSP re = conn.run("select * from loadTable(dbName,`pt)\n");
	//    MultithreadedTableWriter::Status status;
	//    mulwrite->getStatus(status);
	//    cout << status.errorInfo.errorInfo;
	for (int i = 0; i < 100; i++)
	{
		EXPECT_EQ(re->getColumn(1)->getRow(i)->getString(), Util::createInt(i)->getString());
		EXPECT_EQ(re->getColumn(0)->getRow(i)->getString(), "A");
		EXPECT_EQ(re->getColumn(2)->getRow(i)->getString(), Util::createDateTime(i)->getString());
	}

	// timestamp
	mulwrite2 = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt2", false, false, nullptr, 10, 0.1, 3, "data");
	vector<vector<ConstantSP> *> datas2;
	for (int i = 0; i < 100; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &rows = *prow;
		rows.push_back(Util::createString("A"));
		rows.push_back(Util::createInt(i));
		rows.push_back(Util::createTimestamp(i));
		datas2.push_back(prow);
	}
	mulwrite2->insertUnwrittenData(datas2, pErrorInfo);
	mulwrite2->waitForThreadCompletion();
	TableSP re2 = conn.run("select * from loadTable(dbName,`pt2)\n");
	for (int i = 0; i < 100; i++)
	{
		EXPECT_EQ(re2->getColumn(1)->getRow(i)->getString(), Util::createInt(i)->getString());
		EXPECT_EQ(re2->getColumn(0)->getRow(i)->getString(), "A");
		EXPECT_EQ(re2->getColumn(2)->getRow(i)->getString(), Util::createTimestamp(i)->getString());
	}

	// nanotimestamp
	mulwrite2 = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt3", false, false, nullptr, 10, 0.1, 3, "data");
	vector<vector<ConstantSP> *> datas3;
	for (int i = 0; i < 100; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &rows = *prow;
		rows.push_back(Util::createString("A"));
		rows.push_back(Util::createInt(i));
		rows.push_back(Util::createNanoTimestamp(i));
		datas3.push_back(prow);
	}
	mulwrite2->insertUnwrittenData(datas3, pErrorInfo);
	mulwrite2->waitForThreadCompletion();
	TableSP re3 = conn.run("select * from loadTable(dbName,`pt3)\n");
	for (int i = 0; i < 100; i++)
	{
		EXPECT_EQ(re3->getColumn(1)->getRow(i)->getString(), Util::createInt(i)->getString());
		EXPECT_EQ(re3->getColumn(0)->getRow(i)->getString(), "A");
		EXPECT_EQ(re3->getColumn(2)->getRow(i)->getString(), Util::createNanoTimestamp(i)->getString());
	}
}

TEST_F(MultithreadedTableWriterTest, PartitionSchemeDate)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
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
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	SmartPointer<MultithreadedTableWriter> mulwrite2;
	SmartPointer<MultithreadedTableWriter> mulwrite3;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 10, 0.1, 3, "data");
	// datatime
	vector<vector<ConstantSP> *> datas;
	for (int i = 0; i < 100; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &rows = *prow;
		rows.push_back(Util::createString("A"));
		rows.push_back(Util::createInt(i));
		rows.push_back(Util::createDateTime(i));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	mulwrite->waitForThreadCompletion();
	TableSP re = conn.run("select * from loadTable(dbName,`pt)\n");
	//    MultithreadedTableWriter::Status status;
	//    mulwrite->getStatus(status);
	//    cout << status.errorInfo.errorInfo;
	for (int i = 0; i < 100; i++)
	{
		EXPECT_EQ(re->getColumn(1)->getRow(i)->getString(), Util::createInt(i)->getString());
		EXPECT_EQ(re->getColumn(0)->getRow(i)->getString(), "A");
		EXPECT_EQ(re->getColumn(2)->getRow(i)->getString(), Util::createDateTime(i)->getString());
	}

	// timestamp
	mulwrite2 = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt2", false, false, nullptr, 10, 0.1, 3, "data");
	vector<vector<ConstantSP> *> datas2;
	for (int i = 0; i < 100; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &rows = *prow;
		rows.push_back(Util::createString("A"));
		rows.push_back(Util::createInt(i));
		rows.push_back(Util::createTimestamp(i));
		datas2.push_back(prow);
	}
	mulwrite2->insertUnwrittenData(datas2, pErrorInfo);
	mulwrite2->waitForThreadCompletion();
	TableSP re2 = conn.run("select * from loadTable(dbName,`pt2)\n");
	for (int i = 0; i < 100; i++)
	{
		EXPECT_EQ(re2->getColumn(1)->getRow(i)->getString(), Util::createInt(i)->getString());
		EXPECT_EQ(re2->getColumn(0)->getRow(i)->getString(), "A");
		EXPECT_EQ(re2->getColumn(2)->getRow(i)->getString(), Util::createTimestamp(i)->getString());
	}

	// nanotimestamp
	mulwrite2 = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt3", false, false, nullptr, 10, 0.1, 3, "data");
	vector<vector<ConstantSP> *> datas3;
	for (int i = 0; i < 100; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &rows = *prow;
		rows.push_back(Util::createString("A"));
		rows.push_back(Util::createInt(i));
		rows.push_back(Util::createNanoTimestamp(i));
		datas3.push_back(prow);
	}
	mulwrite2->insertUnwrittenData(datas3, pErrorInfo);
	mulwrite2->waitForThreadCompletion();
	TableSP re3 = conn.run("select * from loadTable(dbName,`pt3)\n");
	for (int i = 0; i < 100; i++)
	{
		EXPECT_EQ(re3->getColumn(1)->getRow(i)->getString(), Util::createInt(i)->getString());
		EXPECT_EQ(re3->getColumn(0)->getRow(i)->getString(), "A");
		EXPECT_EQ(re3->getColumn(2)->getRow(i)->getString(), Util::createNanoTimestamp(i)->getString());
	}
}

TEST_F(MultithreadedTableWriterTest, PartitionSchemeMonth)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
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
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	SmartPointer<MultithreadedTableWriter> mulwrite2;
	SmartPointer<MultithreadedTableWriter> mulwrite3;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 10, 0.1, 3, "data");
	// datatime
	vector<vector<ConstantSP> *> datas;
	for (int i = 0; i < 100; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &rows = *prow;
		rows.push_back(Util::createString("A"));
		rows.push_back(Util::createInt(i));
		rows.push_back(Util::createDateTime(i));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	mulwrite->waitForThreadCompletion();
	TableSP re = conn.run("select * from loadTable(dbName,`pt)\n");
	//    MultithreadedTableWriter::Status status;
	//    mulwrite->getStatus(status);
	//    cout << status.errorInfo.errorInfo;
	for (int i = 0; i < 100; i++)
	{
		EXPECT_EQ(re->getColumn(1)->getRow(i)->getString(), Util::createInt(i)->getString());
		EXPECT_EQ(re->getColumn(0)->getRow(i)->getString(), "A");
		EXPECT_EQ(re->getColumn(2)->getRow(i)->getString(), Util::createDateTime(i)->getString());
	}

	// timestamp
	mulwrite2 = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt2", false, false, nullptr, 10, 0.1, 3, "data");
	vector<vector<ConstantSP> *> datas2;
	for (int i = 0; i < 100; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &rows = *prow;
		rows.push_back(Util::createString("A"));
		rows.push_back(Util::createInt(i));
		rows.push_back(Util::createTimestamp(i));
		datas2.push_back(prow);
	}
	mulwrite2->insertUnwrittenData(datas2, pErrorInfo);
	mulwrite2->waitForThreadCompletion();
	TableSP re2 = conn.run("select * from loadTable(dbName,`pt2)\n");
	for (int i = 0; i < 100; i++)
	{
		EXPECT_EQ(re2->getColumn(1)->getRow(i)->getString(), Util::createInt(i)->getString());
		EXPECT_EQ(re2->getColumn(0)->getRow(i)->getString(), "A");
		EXPECT_EQ(re2->getColumn(2)->getRow(i)->getString(), Util::createTimestamp(i)->getString());
	}

	// nanotimestamp
	mulwrite2 = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt3", false, false, nullptr, 10, 0.1, 3, "data");
	vector<vector<ConstantSP> *> datas3;
	for (int i = 0; i < 100; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &rows = *prow;
		rows.push_back(Util::createString("A"));
		rows.push_back(Util::createInt(i));
		rows.push_back(Util::createNanoTimestamp(i));
		datas3.push_back(prow);
	}
	mulwrite2->insertUnwrittenData(datas3, pErrorInfo);
	mulwrite2->waitForThreadCompletion();
	TableSP re3 = conn.run("select * from loadTable(dbName,`pt3)\n");
	for (int i = 0; i < 100; i++)
	{
		EXPECT_EQ(re3->getColumn(1)->getRow(i)->getString(), Util::createInt(i)->getString());
		EXPECT_EQ(re3->getColumn(0)->getRow(i)->getString(), "A");
		EXPECT_EQ(re3->getColumn(2)->getRow(i)->getString(), Util::createNanoTimestamp(i)->getString());
	}
}

TEST_F(MultithreadedTableWriterTest, PartitionSchemeLISTDate)
{
	string dbName = "dfs://test_MultithreadedTableWriter";
	string script = "dbName = \"dfs://test_MultithreadedTableWriter\"\n"
					"if(exists(dbName)){\n"
					"\tdropDatabase(dbName)\t\n"
					"}\n"
					"db  = database(dbName, LIST,date([0..12,13..56, 57..101]));\n"
					"t = table(1000:0, `sym`id`data,[SYMBOL, INT, DATETIME])\n"
					"pt = db.createPartitionedTable(t, `pt, `data)\n"
					"t2 = table(1000:0, `sym`id`data,[SYMBOL, INT, TIMESTAMP])\n"
					"pt2= db.createPartitionedTable(t2, `pt2, `data)\n"
					"t3 = table(1000:0, `sym`id`data,[SYMBOL, INT, nanotimestamp])\n"
					"pt3= db.createPartitionedTable(t3, `pt3, `data)\n"
					"t4 = table(1000:0, `sym`id`data,[SYMBOL, INT, DATE])\n"
					"pt3= db.createPartitionedTable(t4, `pt4, `data)\n";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	SmartPointer<MultithreadedTableWriter> mulwrite2;
	SmartPointer<MultithreadedTableWriter> mulwrite3;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt", false, false, nullptr, 10, 0.1, 3, "data");
	// datatime
	vector<vector<ConstantSP> *> datas;
	for (int i = 0; i < 100; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &rows = *prow;
		rows.push_back(Util::createString("A"));
		rows.push_back(Util::createInt(i));
		rows.push_back(Util::createDateTime(i));
		datas.push_back(prow);
	}
	mulwrite->insertUnwrittenData(datas, pErrorInfo);
	mulwrite->waitForThreadCompletion();
	TableSP re = conn.run("select * from loadTable(dbName,`pt)\n");
	//    MultithreadedTableWriter::Status status;
	//    mulwrite->getStatus(status);
	//    cout << status.errorInfo.errorInfo;
	for (int i = 0; i < 100; i++)
	{
		EXPECT_EQ(re->getColumn(1)->getRow(i)->getString(), Util::createInt(i)->getString());
		EXPECT_EQ(re->getColumn(0)->getRow(i)->getString(), "A");
		EXPECT_EQ(re->getColumn(2)->getRow(i)->getString(), Util::createDateTime(i)->getString());
	}

	// timestamp
	mulwrite2 = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt2", false, false, nullptr, 10, 0.1, 3, "data");
	vector<vector<ConstantSP> *> datas2;
	for (int i = 0; i < 100; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &rows = *prow;
		rows.push_back(Util::createString("A"));
		rows.push_back(Util::createInt(i));
		rows.push_back(Util::createTimestamp(i));
		datas2.push_back(prow);
	}
	mulwrite2->insertUnwrittenData(datas2, pErrorInfo);
	mulwrite2->waitForThreadCompletion();
	TableSP re2 = conn.run("select * from loadTable(dbName,`pt2)\n");
	for (int i = 0; i < 100; i++)
	{
		EXPECT_EQ(re2->getColumn(1)->getRow(i)->getString(), Util::createInt(i)->getString());
		EXPECT_EQ(re2->getColumn(0)->getRow(i)->getString(), "A");
		EXPECT_EQ(re2->getColumn(2)->getRow(i)->getString(), Util::createTimestamp(i)->getString());
	}

	// nanotimestamp
	mulwrite2 = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt3", false, false, nullptr, 10, 0.1, 3, "data");
	vector<vector<ConstantSP> *> datas3;
	for (int i = 0; i < 100; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &rows = *prow;
		rows.push_back(Util::createString("A"));
		rows.push_back(Util::createInt(i));
		rows.push_back(Util::createNanoTimestamp(i));
		datas3.push_back(prow);
	}
	mulwrite2->insertUnwrittenData(datas3, pErrorInfo);
	mulwrite2->waitForThreadCompletion();
	TableSP re3 = conn.run("select * from loadTable(dbName,`pt3)\n");
	for (int i = 0; i < 100; i++)
	{
		EXPECT_EQ(re3->getColumn(1)->getRow(i)->getString(), Util::createInt(i)->getString());
		EXPECT_EQ(re3->getColumn(0)->getRow(i)->getString(), "A");
		EXPECT_EQ(re3->getColumn(2)->getRow(i)->getString(), Util::createNanoTimestamp(i)->getString());
	}

	// date
	mulwrite2 = new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, "pt4", false, false, nullptr, 10, 0.1, 3, "data");
	vector<vector<ConstantSP> *> datas4;
	for (int i = 0; i < 100; i++)
	{
		vector<ConstantSP> *prow = new vector<ConstantSP>;
		vector<ConstantSP> &rows = *prow;
		rows.push_back(Util::createString("A"));
		rows.push_back(Util::createInt(i));
		rows.push_back(Util::createNanoTimestamp(i));
		datas4.push_back(prow);
	}
	mulwrite2->insertUnwrittenData(datas4, pErrorInfo);
	mulwrite2->waitForThreadCompletion();
	re3 = conn.run("select * from loadTable(dbName,`pt4)\n");
	for (int i = 0; i < 100; i++)
	{
		EXPECT_EQ(re3->getColumn(1)->getRow(i)->getString(), Util::createInt(i)->getString());
		EXPECT_EQ(re3->getColumn(0)->getRow(i)->getString(), "A");
		EXPECT_EQ(re3->getColumn(2)->getRow(i)->getString(), Util::createDate(i)->getString());
	}
}

TEST_F(MultithreadedTableWriterTest, basicDateData)
{
	string script = "share streamTable(1:0, `cint`csym`cdouble,"
					"[INT,SYMBOL,DOUBLE]) as t1;";
	conn.run(script);
	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "t1", false, false, nullptr, 10, 0.1, 3, "cint");
	bool flag2 = mulwrite->insert(pErrorInfo, 12, nullptr, 23.5);
	cout << flag2 << endl;
	mulwrite->waitForThreadCompletion();
	ConstantSP t1 = conn.run("select * from objByName(`t1)");
	cout << t1->getString();
	MultithreadedTableWriter::Status status;
	mulwrite->getStatus(status);
	cout << status.errorInfo;
}

TEST_F(MultithreadedTableWriterTest, upsertToPartitionTableRangeType)
{
	string script1;

	string dbName = "dfs://test_upsertToPartitionTableRangeType";
	string tableName = "pt";
	script1 += "dbName = \"" + dbName + "\"\n";
	script1 += "tableName=\"" + tableName + "\"\n";
	script1 += "login(\"admin\",\"123456\")\n";
	script1 += "if(existsDatabase(dbName)){\n";
	script1 += " dropDatabase(dbName)\n";
	script1 += "}\n";
	script1 += "db  = database(dbName, RANGE,0 5000 15000);\n";
	script1 += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
			   "tableInsert(t,[`asd,0,10]);"
			   "pt = db.createPartitionedTable(t,tableName,`id);"
			   "tableInsert(pt,t)";
	// cout<<script1<<endl;
	conn.run(script1);

	SmartPointer<MultithreadedTableWriter> mulwriter;
	ErrorCodeInfo pErrorInfo;
	vector<string> modeOption = {"ignoreNull=false", "keyColNames=`id"};

	mulwriter =
		new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, tableName, false, false, nullptr, 1000, 1, 1, "id", nullptr, MultithreadedTableWriter::Mode::M_Upsert, &modeOption);

	int colNum = 3, rowNum = 1000;
	vector<string> colNames = {"symbol", "id", "value"};
	vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_INT, DT_INT};

	vector<vector<ConstantSP> *> datas;
	string sym[] = {"A", "B", "C", "D"};

	vector<ConstantSP> *prow0 = new vector<ConstantSP>({Util::createString(sym[rand() % 4]), Util::createInt(0), Util::createNullConstant(DT_INT)});
	datas.emplace_back(prow0);

	for (int i = 1; i < rowNum; i++)
	{
		vector<ConstantSP> *prows = new vector<ConstantSP>();
		vector<ConstantSP> &rows = *prows;
		rows.emplace_back(Util::createString(sym[rand() % 4]));
		rows.emplace_back(Util::createInt(i));
		rows.emplace_back(Util::createInt((int)(rand() % 1000)));
		datas.emplace_back(prows);
	}

	bool s = mulwriter->insertUnwrittenData(datas, pErrorInfo);
	mulwriter->waitForThreadCompletion();

	MultithreadedTableWriter::Status status;
	mulwriter->getStatus(status);
	cout << status.errorInfo << endl;

	TableSP res = conn.run("select * from pt;");
	cout << res->getString() << endl;
	EXPECT_EQ((res->getColumnType(0) == 18 || res->getColumnType(0) == 17), true);
	EXPECT_EQ(res->getColumnType(1), 4);
	EXPECT_EQ(res->getColumnType(2), 4);

	// cout<< datas[0][0][0]->getString()<<endl;
	// cout<< datas[0][0][1]->getString()<<endl;
	// cout<< datas[0][0][2]->getString()<<endl;

	// cout<< datas[1][0][0]->getString()<<endl;
	// cout<< datas[1][0][1]->getString()<<endl;
	// cout<< datas[1][0][2]->getString()<<endl;
	for (int i = 0; i < rowNum; i++)
	{
		for (int j = 0; j < colNum; j++)
		{
			EXPECT_EQ(res->getColumn(j)->getRow(i)->getString(), datas[i][0][j]->getString());
		}
	}
}

TEST_F(MultithreadedTableWriterTest, upsertToPartitionTableRangeTypeWithsortColumns)
{
	string script1;

	string dbName = "dfs://test_upsertToPartitionTableRangeType";
	string tableName = "pt";
	script1 += "dbName = \"" + dbName + "\"\n";
	script1 += "tableName=\"" + tableName + "\"\n";
	script1 += "login(\"admin\",\"123456\")\n";
	script1 += "if(existsDatabase(dbName)){\n";
	script1 += " dropDatabase(dbName)\n";
	script1 += "}\n";
	script1 += "db  = database(dbName, RANGE,0 5000 15000);\n";
	script1 += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
			   "pt = db.createPartitionedTable(t,tableName,`id);";
	// cout<<script1<<endl;
	conn.run(script1);

	SmartPointer<MultithreadedTableWriter> mulwriter;
	ErrorCodeInfo pErrorInfo;
	vector<string> modeOption = {"ignoreNull=false", "keyColNames=`id", "sortColumns=`value"};

	mulwriter =
		new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, tableName, false, false, nullptr, 1000, 1, 1, "id", nullptr, MultithreadedTableWriter::Mode::M_Upsert, &modeOption);

	int colNum = 3, rowNum = 1000;
	vector<string> colNames = {"symbol", "id", "value"};
	vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_INT, DT_INT};
	vector<vector<ConstantSP> *> datas;

	TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
	{
		columnVecs.emplace_back(tmp1->getColumn(i));
	}
	string sym[] = {"A", "B", "C", "D"};
	columnVecs[0]->set(0, Util::createString("D"));
	columnVecs[1]->set(0, Util::createInt(0));
	columnVecs[2]->setInt(1000);
	for (int i = 1; i < rowNum; i++)
	{
		columnVecs[0]->set(i, Util::createString(sym[rand() % 4]));
		columnVecs[1]->set(i, Util::createInt(0));
		columnVecs[2]->set(i, Util::createInt((int)(rowNum - i)));
	}
	conn.upload("tmp1", {tmp1});
	conn.run("tableInsert(pt,tmp1)");
	Util::sleep(2000);

	vector<ConstantSP> *prow0 = new vector<ConstantSP>({Util::createString("ccc"), Util::createInt(0), Util::createInt(0)});
	datas.emplace_back(prow0);
	bool s = mulwriter->insertUnwrittenData(datas, pErrorInfo);
	mulwriter->waitForThreadCompletion();

	MultithreadedTableWriter::Status status;
	mulwriter->getStatus(status);
	cout << status.errorInfo << endl;

	TableSP res = conn.run("select * from pt;");
	cout << res->getString() << endl;
	EXPECT_EQ((res->getColumnType(0) == 18 || res->getColumnType(0) == 17), true);
	EXPECT_EQ(res->getColumnType(1), 4);
	EXPECT_EQ(res->getColumnType(2), 4);

	for (int i = 1; i < rowNum; i++)
	{
		EXPECT_EQ((res->getColumn(2)->getRow(i)->getInt() > res->getColumn(2)->getRow(i - 1)->getInt()), true);
	}
}

TEST_F(MultithreadedTableWriterTest, upsertToPartitionTableHashType)
{
	string script1;

	string dbName = "dfs://test_upsertToPartitionTableHashType";
	string tableName = "pt";
	script1 += "dbName = \"" + dbName + "\"\n";
	script1 += "tableName=\"" + tableName + "\"\n";
	script1 += "login(\"admin\",\"123456\")\n";
	script1 += "if(existsDatabase(dbName)){\n";
	script1 += " dropDatabase(dbName)\n";
	script1 += "}\n";
	script1 += "db  = database(dbName, HASH,[INT, 1]);\n";
	script1 += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
			   "tableInsert(t,[`asd,0,10]);"
			   "pt = db.createPartitionedTable(t,tableName,`id);"
			   "tableInsert(pt,t)";
	// cout<<script1<<endl;
	conn.run(script1);

	SmartPointer<MultithreadedTableWriter> mulwriter;
	ErrorCodeInfo pErrorInfo;
	vector<string> modeOption = {"ignoreNull=false", "keyColNames=`id"};

	mulwriter =
		new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, tableName, false, false, nullptr, 1000, 1, 1, "id", nullptr, MultithreadedTableWriter::Mode::M_Upsert, &modeOption);

	int colNum = 3, rowNum = 1000;
	vector<string> colNames = {"symbol", "id", "value"};
	vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_INT, DT_INT};

	vector<vector<ConstantSP> *> datas;
	string sym[] = {"A", "B", "C", "D"};

	vector<ConstantSP> *prow0 = new vector<ConstantSP>({Util::createString(sym[rand() % 4]), Util::createInt(0), Util::createNullConstant(DT_INT)});
	datas.emplace_back(prow0);

	for (int i = 1; i < rowNum; i++)
	{
		vector<ConstantSP> *prows = new vector<ConstantSP>();
		vector<ConstantSP> &rows = *prows;
		rows.emplace_back(Util::createString(sym[rand() % 4]));
		rows.emplace_back(Util::createInt(i));
		rows.emplace_back(Util::createInt((int)(rand() % 1000)));
		datas.emplace_back(prows);
	}

	bool s = mulwriter->insertUnwrittenData(datas, pErrorInfo);
	mulwriter->waitForThreadCompletion();

	MultithreadedTableWriter::Status status;
	mulwriter->getStatus(status);
	cout << status.errorInfo << endl;

	TableSP res = conn.run("select * from pt;");
	cout << res->getString() << endl;
	EXPECT_EQ((res->getColumnType(0) == 18 || res->getColumnType(0) == 17), true);
	EXPECT_EQ(res->getColumnType(1), 4);
	EXPECT_EQ(res->getColumnType(2), 4);

	// cout<< datas[0][0][0]->getString()<<endl;
	// cout<< datas[0][0][1]->getString()<<endl;
	// cout<< datas[0][0][2]->getString()<<endl;

	// cout<< datas[1][0][0]->getString()<<endl;
	// cout<< datas[1][0][1]->getString()<<endl;
	// cout<< datas[1][0][2]->getString()<<endl;
	for (int i = 0; i < rowNum; i++)
	{
		for (int j = 0; j < colNum; j++)
		{
			EXPECT_EQ(res->getColumn(j)->getRow(i)->getString(), datas[i][0][j]->getString());
		}
	}
}

TEST_F(MultithreadedTableWriterTest, upsertToPartitionTableValueType)
{
	string script1;

	string dbName = "dfs://test_upsertToPartitionTableValueType";
	string tableName = "pt";
	script1 += "dbName = \"" + dbName + "\"\n";
	script1 += "tableName=\"" + tableName + "\"\n";
	script1 += "login(\"admin\",\"123456\")\n";
	script1 += "if(existsDatabase(dbName)){\n";
	script1 += " dropDatabase(dbName)\n";
	script1 += "}\n";
	script1 += "db  = database(dbName, VALUE,0..1000);\n";
	script1 += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
			   "tableInsert(t,[`asd,0,10]);"
			   "pt = db.createPartitionedTable(t,tableName,`id);"
			   "tableInsert(pt,t)";
	// cout<<script1<<endl;
	conn.run(script1);

	SmartPointer<MultithreadedTableWriter> mulwriter;
	ErrorCodeInfo pErrorInfo;
	vector<string> modeOption = {"ignoreNull=false", "keyColNames=`id"};

	mulwriter =
		new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, tableName, false, false, nullptr, 1000, 1, 1, "id", nullptr, MultithreadedTableWriter::Mode::M_Upsert, &modeOption);

	int colNum = 3, rowNum = 1000;
	vector<string> colNames = {"symbol", "id", "value"};
	vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_INT, DT_INT};

	vector<vector<ConstantSP> *> datas;
	string sym[] = {"A", "B", "C", "D"};

	vector<ConstantSP> *prow0 = new vector<ConstantSP>({Util::createString(sym[rand() % 4]), Util::createInt(0), Util::createNullConstant(DT_INT)});
	datas.emplace_back(prow0);

	for (int i = 1; i < rowNum; i++)
	{
		vector<ConstantSP> *prows = new vector<ConstantSP>();
		vector<ConstantSP> &rows = *prows;
		rows.emplace_back(Util::createString(sym[rand() % 4]));
		rows.emplace_back(Util::createInt(i));
		rows.emplace_back(Util::createInt((int)(rand() % 1000)));
		datas.emplace_back(prows);
	}

	bool s = mulwriter->insertUnwrittenData(datas, pErrorInfo);
	mulwriter->waitForThreadCompletion();

	MultithreadedTableWriter::Status status;
	mulwriter->getStatus(status);
	cout << status.errorInfo << endl;

	TableSP res = conn.run("select * from pt;");
	cout << res->getString() << endl;
	EXPECT_EQ((res->getColumnType(0) == 18 || res->getColumnType(0) == 17), true);
	EXPECT_EQ(res->getColumnType(1), 4);
	EXPECT_EQ(res->getColumnType(2), 4);

	// cout<< datas[0][0][0]->getString()<<endl;
	// cout<< datas[0][0][1]->getString()<<endl;
	// cout<< datas[0][0][2]->getString()<<endl;

	// cout<< datas[1][0][0]->getString()<<endl;
	// cout<< datas[1][0][1]->getString()<<endl;
	// cout<< datas[1][0][2]->getString()<<endl;
	for (int i = 0; i < rowNum; i++)
	{
		for (int j = 0; j < colNum; j++)
		{
			EXPECT_EQ(res->getColumn(j)->getRow(i)->getString(), datas[i][0][j]->getString());
		}
	}
}

TEST_F(MultithreadedTableWriterTest, upsertToPartitionTableListType)
{
	string script1;

	string dbName = "dfs://test_upsertToPartitionTableListType";
	string tableName = "pt";
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
	// cout<<script1<<endl;
	conn.run(script1);

	SmartPointer<MultithreadedTableWriter> mulwriter;
	ErrorCodeInfo pErrorInfo;
	vector<string> modeOption = {"ignoreNull=false", "keyColNames=`id"};

	mulwriter =
		new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, tableName, false, false, nullptr, 1000, 1, 1, "id", nullptr, MultithreadedTableWriter::Mode::M_Upsert, &modeOption);

	int colNum = 3, rowNum = 1000;
	vector<string> colNames = {"symbol", "id", "value"};
	vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_INT, DT_INT};

	vector<vector<ConstantSP> *> datas;
	string sym[] = {"A", "B", "C", "D"};

	vector<ConstantSP> *prow0 = new vector<ConstantSP>({Util::createString(sym[rand() % 4]), Util::createInt(0), Util::createNullConstant(DT_INT)});
	datas.emplace_back(prow0);

	for (int i = 1; i < rowNum; i++)
	{
		vector<ConstantSP> *prows = new vector<ConstantSP>();
		vector<ConstantSP> &rows = *prows;
		rows.emplace_back(Util::createString(sym[rand() % 4]));
		rows.emplace_back(Util::createInt(i));
		rows.emplace_back(Util::createInt((int)(rand() % 1000)));
		datas.emplace_back(prows);
	}

	bool s = mulwriter->insertUnwrittenData(datas, pErrorInfo);
	mulwriter->waitForThreadCompletion();

	MultithreadedTableWriter::Status status;
	mulwriter->getStatus(status);
	cout << status.errorInfo << endl;

	TableSP res = conn.run("select * from pt;");
	cout << res->getString() << endl;

	EXPECT_EQ((res->getColumnType(0) == 18 || res->getColumnType(0) == 17), true);
	EXPECT_EQ(res->getColumnType(1), 4);
	EXPECT_EQ(res->getColumnType(2), 4);

	for (int i = 0; i < rowNum; i++)
	{
		for (int j = 0; j < colNum; j++)
		{
			EXPECT_EQ(res->getColumn(j)->getRow(i)->getString(), datas[i][0][j]->getString());
		}
	}
}

TEST_F(MultithreadedTableWriterTest, upsertToPartitionTableRangeTypeIgnoreNull)
{
	string script1;

	string dbName = "dfs://test_upsertToPartitionTableRangeType";
	string tableName = "pt";
	script1 += "dbName = \"" + dbName + "\"\n";
	script1 += "tableName=\"" + tableName + "\"\n";
	script1 += "login(\"admin\",\"123456\")\n";
	script1 += "if(existsDatabase(dbName)){\n";
	script1 += " dropDatabase(dbName)\n";
	script1 += "}\n";
	script1 += "db  = database(dbName, RANGE,0 5000 15000);\n";
	script1 += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"
			   "tableInsert(t,[`asd,0,10]);"
			   "pt = db.createPartitionedTable(t,tableName,`id);"
			   "tableInsert(pt,t)";
	// cout<<script1<<endl;
	conn.run(script1);

	SmartPointer<MultithreadedTableWriter> mulwriter;
	ErrorCodeInfo pErrorInfo;
	vector<string> modeOption = {"ignoreNull=true", "keyColNames=`id"};

	mulwriter =
		new MultithreadedTableWriter(hostName, port, "admin", "123456", dbName, tableName, false, false, nullptr, 1000, 1, 1, "id", nullptr, MultithreadedTableWriter::Mode::M_Upsert, &modeOption);

	int colNum = 3, rowNum = 1000;
	vector<string> colNames = {"symbol", "id", "value"};
	vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_INT, DT_INT};

	vector<vector<ConstantSP> *> datas;
	string sym[] = {"A", "B", "C", "D"};

	vector<ConstantSP> *prow0 = new vector<ConstantSP>({Util::createString(sym[rand() % 4]), Util::createInt(0), Util::createNullConstant(DT_INT)});
	datas.emplace_back(prow0);

	for (int i = 1; i < rowNum; i++)
	{
		vector<ConstantSP> *prows = new vector<ConstantSP>();
		vector<ConstantSP> &rows = *prows;
		rows.emplace_back(Util::createString(sym[rand() % 4]));
		rows.emplace_back(Util::createInt(i));
		rows.emplace_back(Util::createInt((int)(rand() % 1000)));
		datas.emplace_back(prows);
	}

	bool s = mulwriter->insertUnwrittenData(datas, pErrorInfo);
	mulwriter->waitForThreadCompletion();

	MultithreadedTableWriter::Status status;
	mulwriter->getStatus(status);
	cout << status.errorInfo << endl;

	TableSP res = conn.run("select * from pt;");
	cout << res->getString() << endl;
	EXPECT_EQ((res->getColumnType(0) == 18 || res->getColumnType(0) == 17), true);
	EXPECT_EQ(res->getColumnType(1), 4);
	EXPECT_EQ(res->getColumnType(2), 4);

	// cout<< datas[0][0][0]->getString()<<endl;
	// cout<< datas[0][0][1]->getString()<<endl;
	// cout<< datas[0][0][2]->getString()<<endl;

	// cout<< datas[1][0][0]->getString()<<endl;
	// cout<< datas[1][0][1]->getString()<<endl;
	// cout<< datas[1][0][2]->getString()<<endl;
	EXPECT_EQ(res->getColumn(0)->getRow(0)->getString(), prow0[0][0]->getString());
	EXPECT_EQ(res->getColumn(1)->getRow(0)->getInt(), 0);
	EXPECT_EQ(res->getColumn(2)->getRow(0)->getInt(), 10);
	for (int i = 1; i < rowNum; i++)
	{
		for (int j = 0; j < colNum; j++)
		{
			EXPECT_EQ(res->getColumn(j)->getRow(i)->getString(), datas[i][0][j]->getString());
		}
	}
}

TEST_F(MultithreadedTableWriterTest, upsertToKeyedTable)
{
	string script1;

	string tableName = "pt_k1";
	script1 = "t = table(5:0, `symbol`id`value,[SYMBOL, INT, INT]);\
              t1_k1 = keyedTable(`id, t);\
              tableInsert(t1_k1,`asd,0,10);\
              share t1_k1 as pt_k1;";
	// cout<<script1<<endl;
	conn.run(script1);

	SmartPointer<MultithreadedTableWriter> mulwriter;
	ErrorCodeInfo pErrorInfo;
	vector<string> modeOption = {"ignoreNull=false", "keyColNames=`id"};

	mulwriter =
		new MultithreadedTableWriter(hostName, port, "admin", "123456", "", tableName, false, false, nullptr, 1, 1, 1, "id", nullptr, MultithreadedTableWriter::Mode::M_Upsert, &modeOption);

	int colNum = 3, rowNum = 5;
	vector<string> colNames = {"symbol", "id", "value"};
	vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_INT, DT_INT};

	vector<vector<ConstantSP> *> datas;
	string sym[] = {"A", "B", "C", "D"};

	vector<ConstantSP> *prow0 = new vector<ConstantSP>({Util::createString(sym[rand() % 4]), Util::createInt(0), Util::createNullConstant(DT_INT)});
	datas.emplace_back(prow0);

	for (int i = 1; i < rowNum; i++)
	{
		vector<ConstantSP> *prows = new vector<ConstantSP>();
		vector<ConstantSP> &rows = *prows;
		rows.emplace_back(Util::createString(sym[rand() % 4]));
		rows.emplace_back(Util::createInt(i));
		rows.emplace_back(Util::createInt((int)(rand() % 1000)));
		datas.emplace_back(prows);
	}

	bool s = mulwriter->insertUnwrittenData(datas, pErrorInfo);

	mulwriter->waitForThreadCompletion();

	MultithreadedTableWriter::Status status;
	mulwriter->getStatus(status);

	TableSP res = conn.run("select * from pt_k1;");

	EXPECT_EQ((res->getColumnType(0) == 18 || res->getColumnType(0) == 17), true);
	EXPECT_EQ(res->getColumnType(1), 4);
	EXPECT_EQ(res->getColumnType(2), 4);
	for (int i = 0; i < rowNum; i++)
	{
		for (int j = 0; j < colNum; j++)
		{
			EXPECT_EQ(res->getColumn(j)->getRow(i)->getString(), datas[i][0][j]->getString());
		}
	}
}

TEST_F(MultithreadedTableWriterTest, upsertToKeyedTableIgnoreNull)
{
	string script1;

	string tableName = "pt_k2";
	script1 = "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);\
              t1_k2 = keyedTable(`id, t);\
              tableInsert(t1_k2,`asd,0,10);\
              share t1_k2 as pt_k2;";
	// cout<<script1<<endl;
	conn.run(script1);

	SmartPointer<MultithreadedTableWriter> mulwriter;
	ErrorCodeInfo pErrorInfo;
	vector<string> modeOption = {"ignoreNull=true", "keyColNames=`id"};

	mulwriter =
		new MultithreadedTableWriter(hostName, port, "admin", "123456", "", tableName, false, false, nullptr, 1000, 1, 1, "id", nullptr, MultithreadedTableWriter::Mode::M_Upsert, &modeOption);

	int colNum = 3, rowNum = 1000;
	vector<string> colNames = {"symbol", "id", "value"};
	vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_INT, DT_INT};

	vector<vector<ConstantSP> *> datas;
	string sym[] = {"A", "B", "C", "D"};

	vector<ConstantSP> *prow0 = new vector<ConstantSP>({Util::createString(sym[rand() % 4]), Util::createInt(0), Util::createNullConstant(DT_INT)});
	datas.emplace_back(prow0);

	for (int i = 1; i < rowNum; i++)
	{
		vector<ConstantSP> *prows = new vector<ConstantSP>();
		vector<ConstantSP> &rows = *prows;
		rows.emplace_back(Util::createString(sym[rand() % 4]));
		rows.emplace_back(Util::createInt(i));
		rows.emplace_back(Util::createInt((int)(rand() % 1000)));
		datas.emplace_back(prows);
	}

	bool s = mulwriter->insertUnwrittenData(datas, pErrorInfo);
	mulwriter->waitForThreadCompletion();

	MultithreadedTableWriter::Status status;
	mulwriter->getStatus(status);
	cout << status.errorInfo << endl;

	TableSP res = conn.run("select * from pt_k2;");

	EXPECT_EQ((res->getColumnType(0) == 18 || res->getColumnType(0) == 17), true);
	EXPECT_EQ(res->getColumnType(1), 4);
	EXPECT_EQ(res->getColumnType(2), 4);

	EXPECT_EQ(res->getColumn(0)->getRow(0)->getString(), prow0[0][0]->getString());
	EXPECT_EQ(res->getColumn(1)->getRow(0)->getInt(), 0);
	EXPECT_EQ(res->getColumn(2)->getRow(0)->getInt(), 10);
	for (int i = 1; i < rowNum; i++)
	{
		for (int j = 0; j < colNum; j++)
		{
			EXPECT_EQ(res->getColumn(j)->getRow(i)->getString(), datas[i][0][j]->getString());
		}
	}
}

TEST_F(MultithreadedTableWriterTest, upsertToIndexedTable)
{
	string script1;

	string tableName = "pt_i1";
	script1 = "t = table(5:0, `symbol`id`value,[SYMBOL, INT, INT]);\
              t1_i1 = indexedTable(`id, t);\
              tableInsert(t1_i1,`asd,0,10);\
              share t1_i1 as pt_i1;";
	// cout<<script1<<endl;
	conn.run(script1);

	SmartPointer<MultithreadedTableWriter> mulwriter;
	ErrorCodeInfo pErrorInfo;
	vector<string> modeOption = {"ignoreNull=false", "keyColNames=`id"};

	mulwriter =
		new MultithreadedTableWriter(hostName, port, "admin", "123456", "", tableName, false, false, nullptr, 1, 1, 1, "id", nullptr, MultithreadedTableWriter::Mode::M_Upsert, &modeOption);

	int colNum = 3, rowNum = 5;
	vector<string> colNames = {"symbol", "id", "value"};
	vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_INT, DT_INT};

	vector<vector<ConstantSP> *> datas;
	string sym[] = {"A", "B", "C", "D"};

	vector<ConstantSP> *prow0 = new vector<ConstantSP>({Util::createString(sym[rand() % 4]), Util::createInt(0), Util::createNullConstant(DT_INT)});
	datas.emplace_back(prow0);

	for (int i = 1; i < rowNum; i++)
	{
		vector<ConstantSP> *prows = new vector<ConstantSP>();
		vector<ConstantSP> &rows = *prows;
		rows.emplace_back(Util::createString(sym[rand() % 4]));
		rows.emplace_back(Util::createInt(i));
		rows.emplace_back(Util::createInt((int)(rand() % 1000)));
		datas.emplace_back(prows);
	}

	bool s = mulwriter->insertUnwrittenData(datas, pErrorInfo);

	mulwriter->waitForThreadCompletion();

	MultithreadedTableWriter::Status status;
	mulwriter->getStatus(status);

	TableSP res = conn.run("select * from pt_i1;");

	EXPECT_EQ((res->getColumnType(0) == 18 || res->getColumnType(0) == 17), true);
	EXPECT_EQ(res->getColumnType(1), 4);
	EXPECT_EQ(res->getColumnType(2), 4);
	for (int i = 0; i < rowNum; i++)
	{
		for (int j = 0; j < colNum; j++)
		{
			EXPECT_EQ(res->getColumn(j)->getRow(i)->getString(), datas[i][0][j]->getString());
		}
	}
}

TEST_F(MultithreadedTableWriterTest, upsertIndexedTableIgnoreNull)
{
	string script1;

	string tableName = "pt_i2";
	script1 = "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);\
              t1_i2 = indexedTable(`id, t);\
              tableInsert(t1_i2,`asd,0,10);\
              share t1_i2 as pt_i2;";
	// cout<<script1<<endl;
	conn.run(script1);

	SmartPointer<MultithreadedTableWriter> mulwriter;
	ErrorCodeInfo pErrorInfo;
	vector<string> modeOption = {"ignoreNull=true", "keyColNames=`id"};

	mulwriter =
		new MultithreadedTableWriter(hostName, port, "admin", "123456", "", tableName, false, false, nullptr, 1000, 1, 1, "id", nullptr, MultithreadedTableWriter::Mode::M_Upsert, &modeOption);

	int colNum = 3, rowNum = 1000;
	vector<string> colNames = {"symbol", "id", "value"};
	vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_INT, DT_INT};

	vector<vector<ConstantSP> *> datas;
	string sym[] = {"A", "B", "C", "D"};

	vector<ConstantSP> *prow0 = new vector<ConstantSP>({Util::createString(sym[rand() % 4]), Util::createInt(0), Util::createNullConstant(DT_INT)});
	datas.emplace_back(prow0);

	for (int i = 1; i < rowNum; i++)
	{
		vector<ConstantSP> *prows = new vector<ConstantSP>();
		vector<ConstantSP> &rows = *prows;
		rows.emplace_back(Util::createString(sym[rand() % 4]));
		rows.emplace_back(Util::createInt(i));
		rows.emplace_back(Util::createInt((int)(rand() % 1000)));
		datas.emplace_back(prows);
	}

	bool s = mulwriter->insertUnwrittenData(datas, pErrorInfo);
	mulwriter->waitForThreadCompletion();

	MultithreadedTableWriter::Status status;
	mulwriter->getStatus(status);
	cout << status.errorInfo << endl;

	TableSP res = conn.run("select * from pt_i2;");

	EXPECT_EQ((res->getColumnType(0) == 18 || res->getColumnType(0) == 17), true);
	EXPECT_EQ(res->getColumnType(1), 4);
	EXPECT_EQ(res->getColumnType(2), 4);

	EXPECT_EQ(res->getColumn(0)->getRow(0)->getString(), prow0[0][0]->getString());
	EXPECT_EQ(res->getColumn(1)->getRow(0)->getInt(), 0);
	EXPECT_EQ(res->getColumn(2)->getRow(0)->getInt(), 10);
	for (int i = 1; i < rowNum; i++)
	{
		for (int j = 0; j < colNum; j++)
		{
			EXPECT_EQ(res->getColumn(j)->getRow(i)->getString(), datas[i][0][j]->getString());
		}
	}
}

TEST_F(MultithreadedTableWriterTest, insertWorkflow)
{
	DBConnection conn_wf(false, false);
	conn_wf.connect(hostName, port, "admin", "123456");
	conn_wf.run("dbName = 'dfs://valuedb3'\
                if(exists(dbName)){\
                dropDatabase(dbName);\
                }\
                datetest=table(1000:0,`date`symbol`id`ip`int128s`uuids,[DATE,SYMBOL,LONG,IPADDR,INT128,UUID]);\
                db = database(directory=dbName, partitionType=HASH, partitionScheme=[INT, 10]);\
                pt=db.createPartitionedTable(datetest,'pdatetest','id');");
	conn_wf.run("mtwCreateTime=now()");
	vector<COMPRESS_METHOD> compress;
	compress.push_back(COMPRESS_LZ4);
	compress.push_back(COMPRESS_LZ4);
	compress.push_back(COMPRESS_DELTA);
	compress.push_back(COMPRESS_LZ4);
	compress.push_back(COMPRESS_LZ4);
	compress.push_back(COMPRESS_LZ4);
	MultithreadedTableWriter writer(hostName, port, "admin", "123456", "dfs://valuedb3", "pdatetest", false, false, NULL, 10000, 1, 5, "id", &compress);
	thread t([&]
			 {
        try {
            ErrorCodeInfo errorInfo;
            for (int i = 0; i < 100; i++) {
                if (writer.insert(errorInfo, rand() % 10000, "AAAAAAAB", rand() % 10000,"192.168.1.123", "e1671797c52e15f763380b45e841ec32", "5d212a78-cc48-e3b1-4235-b4d91473ee87") == false) {
                    cout << "insert failed: " << errorInfo.errorInfo << endl;
                    break;
                }
            }
            if (writer.insert(errorInfo, rand() % 10000, 222, rand() % 10000, "192.168.1.123","e1671797c52e15f763380b45e841ec32", "5d212a78-cc48-e3b1-4235-b4d91473ee87") == false) {
                cout << "insert failed: " << errorInfo.errorInfo << endl;
            }
            if (writer.insert(errorInfo, rand() % 10000, "AAAAAAAB", "192.168.1.123","e1671797c52e15f763380b45e841ec32", "5d212a78-cc48-e3b1-4235-b4d91473ee87") == false) {
                cout << "insert failed: " << errorInfo.errorInfo << endl;
            }
            conn.run("id = exec sessionid from getSessionMemoryStat() where temporalAdd(gmtime(createTime), 16, 'h') > mtwCreateTime; for(closeid in id)closeSessions(closeid);");
            Util::sleep(2000);
            if (writer.insert(errorInfo, rand() % 10000, "AAAAAAAB", rand() % 10000, "192.168.1.123","e1671797c52e15f763380b45e841ec32", "5d212a78-cc48-e3b1-4235-b4d91473ee87") == false) {
                cout << "insert failed: " << errorInfo.errorInfo << endl;
            }
                cout << "Never run here.";
        }
        catch (exception &e) {
            cerr << "MTW exit with exception: " << e.what() << endl;
        } });
	MultithreadedTableWriter::Status status;
	writer.getStatus(status);
	if (status.hasError())
	{
		cout << "error in writing: " << status.errorInfo << endl;
	}
	t.join();
	writer.waitForThreadCompletion();
	writer.getStatus(status);
	if (status.hasError())
	{
		cout << "error after write complete: " << status.errorInfo << endl;
		std::vector<std::vector<ConstantSP> *> unwrittenData;
		writer.getUnwrittenData(unwrittenData);
		cout << "unwriterdata length " << unwrittenData.size() << endl;
		if (!unwrittenData.empty())
		{
			try
			{
				cout << "create new MTW and write again." << endl;
				MultithreadedTableWriter newWriter(hostName, port, "admin", "123456", "dfs://valuedb3", "pdatetest", false, false, NULL, 10000, 1, 2, "id", &compress);
				ErrorCodeInfo errorInfo;
				if (newWriter.insertUnwrittenData(unwrittenData, errorInfo))
				{
					newWriter.waitForThreadCompletion();
					newWriter.getStatus(status);
					if (status.hasError())
					{
						cout << "error in write again: " << status.errorInfo << endl;
					}
				}
				else
				{
					cout << "error in write again: " << errorInfo.errorInfo << endl;
				}
			}
			catch (exception &e)
			{
				cerr << "new MTW exit with exception: " << e.what() << endl;
			}
		}
	}
	cout << conn_wf.run("select * from pt")->getString() << endl;
	EXPECT_EQ(conn_wf.run("a=select count(*) from pt;a[0][\"count\"]")->getInt(), 100);
	conn_wf.close();
}

TEST_F(MultithreadedTableWriterTest, insertTodfsTablewithAlldataTypes)
{
	int colNum = 25, rowNum = 2000;
	vector<string> colNamesVec1;
	for (int i = 0; i < colNum; i++)
	{
		colNamesVec1.emplace_back("col" + to_string(i));
	}
	vector<DATA_TYPE> colTypesVec1;
	colTypesVec1.emplace_back(DT_CHAR);
	colTypesVec1.emplace_back(DT_BOOL);
	colTypesVec1.emplace_back(DT_SHORT);
	colTypesVec1.emplace_back(DT_INT);
	colTypesVec1.emplace_back(DT_LONG);
	colTypesVec1.emplace_back(DT_DATE);
	colTypesVec1.emplace_back(DT_MONTH);
	colTypesVec1.emplace_back(DT_TIME);
	colTypesVec1.emplace_back(DT_MINUTE);
	colTypesVec1.emplace_back(DT_DATETIME);
	colTypesVec1.emplace_back(DT_SECOND);
	colTypesVec1.emplace_back(DT_TIMESTAMP);
	colTypesVec1.emplace_back(DT_NANOTIME);
	colTypesVec1.emplace_back(DT_NANOTIMESTAMP);
	colTypesVec1.emplace_back(DT_FLOAT);
	colTypesVec1.emplace_back(DT_DOUBLE);
	colTypesVec1.emplace_back(DT_STRING);
	colTypesVec1.emplace_back(DT_UUID);
	colTypesVec1.emplace_back(DT_IP);
	colTypesVec1.emplace_back(DT_INT128);
	colTypesVec1.emplace_back(DT_BLOB);
	colTypesVec1.emplace_back(DT_DATEHOUR);
	colTypesVec1.emplace_back(DT_DECIMAL32);
	colTypesVec1.emplace_back(DT_DECIMAL64);
	colTypesVec1.emplace_back(DT_SYMBOL);

	srand(time(NULL));
	TableSP tab1 = Util::createTable(colNamesVec1, colTypesVec1, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
	{
		columnVecs.emplace_back(tab1->getColumn(i));
	}
	for (int i = 0; i < rowNum; i++)
	{
		columnVecs[0]->set(i, Util::createChar(rand() % CHAR_MAX));
		columnVecs[1]->set(i, Util::createBool(rand() % 2));
		columnVecs[2]->set(i, Util::createShort(rand() % SHRT_MAX));
		columnVecs[3]->set(i, Util::createInt(i));
		columnVecs[4]->set(i, Util::createLong(rand() % LLONG_MAX));
		columnVecs[5]->set(i, Util::createDate(rand() % INT_MAX));
		columnVecs[6]->set(i, Util::createMonth(rand() % INT_MAX));
		columnVecs[7]->set(i, Util::createTime(rand() % INT_MAX));
		columnVecs[8]->set(i, Util::createMinute(rand() % 1440));
		columnVecs[9]->set(i, Util::createDateTime(rand() % INT_MAX));
		columnVecs[10]->set(i, Util::createSecond(rand() % 86400));
		columnVecs[11]->set(i, Util::createTimestamp(rand() % LLONG_MAX));
		columnVecs[12]->set(i, Util::createNanoTime(rand() % LLONG_MAX));
		columnVecs[13]->set(i, Util::createNanoTimestamp(rand() % LLONG_MAX));
		columnVecs[14]->set(i, Util::createFloat(rand() / float(RAND_MAX)));
		columnVecs[15]->set(i, Util::createDouble(rand() / double(RAND_MAX)));
		columnVecs[16]->set(i, Util::createString("str" + to_string(i)));
		columnVecs[17]->set(i, Util::parseConstant(DT_UUID, "5d212a78-cc48-e3b1-4235-b4d91473ee87"));
		columnVecs[18]->set(i, Util::parseConstant(DT_IP, "192.0.0." + to_string(rand() % 255)));
		columnVecs[19]->set(i, Util::parseConstant(DT_INT128, "e1671797c52e15f763380b45e841ec32"));
		columnVecs[20]->set(i, Util::createBlob("blob"));
		columnVecs[21]->set(i, Util::createDateHour(rand() % INT_MAX));
		columnVecs[22]->set(i, Util::createDecimal32(rand() % 10, rand() / float(RAND_MAX)));
		columnVecs[23]->set(i, Util::createDecimal64(rand() % 19, rand() / double(RAND_MAX)));
		columnVecs[24]->set(i, Util::createString("sym"));
	}
	// for (int j = 0; j < colNum; j++)
	// 	columnVecs[j]->setNull(rowNum-1);

	conn.upload("tab1", {tab1});

	string script = "dbName=\"dfs://mtw_alldataTypes\";"
					"if(existsDatabase(dbName)){dropDatabase(dbName)};go;"
					"db=database(dbName, HASH, [SYMBOL,3],,'TSDB');"
					"pt = db.createPartitionedTable(tab1, `pt, `col24,,`col3)";
	conn.run(script);

	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "dfs://mtw_alldataTypes", "pt", false, false, NULL, 1000, 1, 1, "col24");

	MultithreadedTableWriter::Status status;
	vector<ConstantSP> datas;
	datas.reserve(rowNum * colNum);
	for (auto i = 0; i < rowNum; i++)
	{
		for (auto j = 0; j < colNum; j++)
			datas.emplace_back(tab1->getColumn(j)->get(i));
	}
	for (auto i = 0; i < rowNum; i++)
	{
		if (!mulwrite->insert(pErrorInfo, datas[i * 25 + 0], datas[i * 25 + 1], datas[i * 25 + 2], datas[i * 25 + 3],
							  datas[i * 25 + 4], datas[i * 25 + 5], datas[i * 25 + 6], datas[i * 25 + 7], datas[i * 25 + 8],
							  datas[i * 25 + 9], datas[i * 25 + 10], datas[i * 25 + 11], datas[i * 25 + 12], datas[i * 25 + 13], datas[i * 25 + 14],
							  datas[i * 25 + 15], datas[i * 25 + 16], datas[i * 25 + 17], datas[i * 25 + 18], datas[i * 25 + 19], datas[i * 25 + 20],
							  datas[i * 25 + 21], datas[i * 25 + 22], datas[i * 25 + 23], datas[i * 25 + 24]))
		{
			break;
		}
	}

	mulwrite->waitForThreadCompletion();

	VectorSP res = conn.run("ex = exec * from loadTable(\"dfs://mtw_alldataTypes\", `pt) order by col3;"
							"res = exec * from tab1 order by col3;"
							"each(eqObj,res.values(), ex.values())");

	for (auto i = 0; i < res->size(); i++)
		EXPECT_TRUE(res->get(i)->getBool());
}

TEST_F(MultithreadedTableWriterTest, insertTo_inMemoryTable_with_CallbackHandlerFunc_fail)
{
	int rowNum = 10;
	string script = "col0 = 1 2 3 4 5;col1 = `APPL`TESLA`GO`WSD`NIKE;col2 = [0,1.3215,23.444,566.345,68.243];"
					"pt= table(col0,col1,col2);share(pt,`tab,readonly=true)";
	conn.run(script);

	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	auto callback = [&](ConstantSP callbackTable) {
		/***
			callbackTable schema:
			column 0: id->string
			column 1: success->bool
		***/
		int size = callbackTable->size();
		VectorSP id = callbackTable->getColumn(0);
		VectorSP status = callbackTable->getColumn(1);

		for (auto i = 0; i < size; i++)
		{
			EXPECT_EQ(status->get(i)->getBool(), false);
		}
	};

	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "tab", false, false, NULL,
											1000, 1, 5, "col1", nullptr, MultithreadedTableWriter::M_Append, nullptr, callback);
	MultithreadedTableWriter::Status status;

	string callbackfunc_id = "";
	for (auto i = 0; i < rowNum; i++)
	{
		callbackfunc_id = "row" + to_string(i);
		mulwrite->insert(pErrorInfo, callbackfunc_id, 1, "str"+to_string(i), double(i));
	}

	mulwrite->waitForThreadCompletion();
	EXPECT_EQ(conn.run("exec count(*) from tab")->getInt(), 5);
}

TEST_F(MultithreadedTableWriterTest, insertTo_dfsTable_with_CallbackHandlerFunc_success)
{
	string script = "dbName=\"dfs://mtw_alldataTypes\";"
					"if(existsDatabase(dbName)){dropDatabase(dbName)};go;"
					"db=database(dbName, HASH, [SYMBOL,3],,'TSDB');"
					"tab1 = table(0 1 2 3 4 as col0, `str0`str1`str2`str3`str4 as col1, double(0 1 2 3 4) as col2);"
					"pt = db.createPartitionedTable(tab1, `pt, `col1,,`col0)";
	conn.run(script);

	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	auto callback = [&](ConstantSP callbackTable) {
		/***
			callbackTable schema:
			column 0: id->string
			column 1: success->bool
		***/
		int size = callbackTable->size();
		VectorSP id = callbackTable->getColumn(0);
		VectorSP status = callbackTable->getColumn(1);

		for (auto i = 0; i < size; i++)
		{
			EXPECT_EQ(status->get(i)->getBool(), true);
		}
	};

	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "dfs://mtw_alldataTypes", "pt", false, false, NULL,
											1000, 1, 5, "col1", nullptr, MultithreadedTableWriter::M_Append, nullptr, callback);
	MultithreadedTableWriter::Status status;
	vector<ConstantSP> datas;

	string callbackfunc_id = "";
	for (auto i = 0; i < 5; i++)
	{
		callbackfunc_id = "row" + to_string(i);
		mulwrite->insert(pErrorInfo, callbackfunc_id, int(i), "str" + to_string(i), double(i));
	}

	mulwrite->waitForThreadCompletion();

	VectorSP res = conn.run("ex = exec * from loadTable(\"dfs://mtw_alldataTypes\", `pt) order by col0;"
							"res = exec * from tab1 order by col0;"
							"each(eqObj,res.values(), ex.values())");

	for (auto i = 0; i < res->size(); i++)
		EXPECT_TRUE(res->get(i)->getBool());
}

TEST_F(MultithreadedTableWriterTest, insertTo_inMemorytable_with_CallbackHandlerFunc_success)
{
	int colNum = 3, rowNum = 10;
	vector<string> colNamesVec1;
	for (int i = 0; i < colNum; i++)
	{
		colNamesVec1.emplace_back("col" + to_string(i));
	}
	vector<DATA_TYPE> colTypesVec1;
	colTypesVec1.emplace_back(DT_INT);
	colTypesVec1.emplace_back(DT_SYMBOL);
	colTypesVec1.emplace_back(DT_DOUBLE);

	TableSP tab1 = Util::createTable(colNamesVec1, colTypesVec1, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
	{
		columnVecs.emplace_back(tab1->getColumn(i));
	}
	for (int i = 0; i < rowNum; i++)
	{
		columnVecs[0]->set(i, Util::createInt(i));
		columnVecs[1]->set(i, Util::createString("str" + to_string(i)));
		columnVecs[2]->set(i, Util::createDouble(i));
	}

	conn.upload("tab1", {tab1});

	string script = "tab2 = table(100:0, [`col0,`col1,`col2],[INT, SYMBOL, DOUBLE]);share tab2 as target";
	conn.run(script);

	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	auto callback = [&](ConstantSP callbackTable) {
		/***
			callbackTable schema:
			column 0: id->string
			column 1: success->bool
		***/
		int size = callbackTable->size();
		VectorSP id = callbackTable->getColumn(0);
		VectorSP status = callbackTable->getColumn(1);

		for (auto i = 0; i < size; i++)
		{
			EXPECT_EQ(status->get(i)->getBool(), true);
		}
	};

	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "target", false, false, NULL,
											1000, 1, 1, "", nullptr, MultithreadedTableWriter::M_Append, nullptr, callback);

	MultithreadedTableWriter::Status status;
	vector<ConstantSP> datas;
	datas.reserve(rowNum * colNum);
	for (auto i = 0; i < rowNum; i++)
	{
		for (auto j = 0; j < colNum; j++)
			datas.emplace_back(tab1->getColumn(j)->get(i));
	}

	string callbackfunc_id = "";
	for (auto i = 0; i < rowNum; i++)
	{
		callbackfunc_id = "row" + to_string(i);
		mulwrite->insert(pErrorInfo, callbackfunc_id, int(i), "str" + to_string(i), double(i));
	}

	mulwrite->waitForThreadCompletion();
	VectorSP res = conn.run("ex = exec * from target order by col0;"
							"res = exec * from tab1 order by col0;"
							"each(eqObj,res.values(), ex.values())");

	for (auto i = 0; i < res->size(); i++)
		EXPECT_TRUE(res->get(i)->getBool());
}


TEST_F(MultithreadedTableWriterTest, insert_with_CallbackHandlerFunc_getUnwrittenData)
{
	int colNum = 3, rowNum = 10;
	vector<string> colNamesVec1;
	for (int i = 0; i < colNum; i++)
	{
		colNamesVec1.emplace_back("col" + to_string(i));
	}
	vector<DATA_TYPE> colTypesVec1;
	colTypesVec1.emplace_back(DT_INT);
	colTypesVec1.emplace_back(DT_SYMBOL);
	colTypesVec1.emplace_back(DT_DOUBLE);

	TableSP tab1 = Util::createTable(colNamesVec1, colTypesVec1, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
	{
		columnVecs.emplace_back(tab1->getColumn(i));
	}
	for (int i = 0; i < rowNum; i++)
	{
		columnVecs[0]->set(i, Util::createInt(i));
		columnVecs[1]->set(i, Util::createString("str" + to_string(i)));
		columnVecs[2]->set(i, Util::createDouble(i));
	}

	conn.upload("tab1", {tab1});

	string script = "tab2 = table(100:0, [`col0,`col1,`col2],[INT, SYMBOL, DOUBLE]);share tab2 as target";
	conn.run(script);

	SmartPointer<MultithreadedTableWriter> mulwrite;
	ErrorCodeInfo pErrorInfo;
	auto callback = [&](ConstantSP callbackTable) {
		/***
			callbackTable schema:
			column 0: id->string
			column 1: success->bool
		***/
		int size = callbackTable->size();
		VectorSP id = callbackTable->getColumn(0);
		VectorSP status = callbackTable->getColumn(1);

		cout << "callbackTable is below: " << endl;
		DLogger::Info("callback", callbackTable->getString());
	};


	mulwrite = new MultithreadedTableWriter(hostName, port, "admin", "123456", "", "target", false, false, NULL,
											1000, 1, 1, "", nullptr, MultithreadedTableWriter::M_Append, nullptr, callback);

	MultithreadedTableWriter::Status status;
	vector<ConstantSP> datas;
	datas.reserve(rowNum * colNum);
	for (auto i = 0; i < rowNum; i++)
	{
		for (auto j = 0; j < colNum; j++)
			datas.emplace_back(tab1->getColumn(j)->get(i));
	}

	string callbackfunc_id = "";
	for (auto i = 0; i < rowNum; i++)
	{
		callbackfunc_id = "row" + to_string(i);
		mulwrite->insert(pErrorInfo, callbackfunc_id, int(i), int(i), double(i));
	}

	mulwrite->waitForThreadCompletion();

	vector<vector<ConstantSP> *> unwrittenData;
	EXPECT_ANY_THROW(mulwrite->getUnwrittenData(unwrittenData));
}