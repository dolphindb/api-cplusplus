class PartitionedTableAppenderTest:public testing::Test
{
protected:
    //Suite
    static void SetUpTestCase() {
        //DBConnection conn;
		conn.initialize();
        bool ret = conn.connect(hostName, port, "admin", "123456");
        if (!ret) {
            cout << "Failed to connect to the server" << endl;
        }
        else {
            cout << "connect to " + hostName + ":" + std::to_string(port)<< endl;
        }
    }
    static void TearDownTestCase(){
        conn.close();
    }

    //Case
    virtual void SetUp()
    {
        cout<<"check connect...";
		ConstantSP res = conn.run("1+1");
		if(!(res->getBool())){
			cout<<"Server not responed, please check."<<endl;
		}
		else
		{
			cout<<"ok"<<endl;
			
		}
    }
    virtual void TearDown()
    {
        pass;
    }
};

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_value_int){
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_value_int\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,VALUE,0..9);";
	script += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`id);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_value_int", "pt", "id", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	EXPECT_EQ(total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		EXPECT_EQ(result->getInt(i), 1);
}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_value_date1){
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_value_date1\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,VALUE,2012.01.01..2012.01.10);";
	script += "dummy = table(1:0, `id`sym`date`value, [INT, SYMBOL, DATE, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`date);";
	conn.run(script);
	vector<string> colNames = { "id", "sym", "date", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_DATE, DT_INT };
	int colNum = 4, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createDate(2012, 1, i % 10 + 1));
		columnVecs[3]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_value_date1", "pt", "date", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	EXPECT_EQ(total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, take(2012.01.01..2012.01.10, 1000000) as date, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by date, id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by date, id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<4; i++)
		EXPECT_EQ(result->getInt(i), 1);
}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_value_date2){
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_value_date2\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,VALUE,2012.01.01..2012.01.10);";
	script += "dummy = table(1:0, `id`sym`time`value, [INT, SYMBOL, DATETIME, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`time);";
	conn.run(script);
	vector<string> colNames = { "id", "sym", "time", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_DATETIME, DT_INT };
	int colNum = 4, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createDateTime(2012, 1, i % 10 + 1, 9, 30, 30));
		columnVecs[3]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_value_date2", "pt", "time", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	EXPECT_EQ(total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, concatDateTime(take(2012.01.01..2012.01.10, 1000000), 09:30:30) as time, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by time, id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by time, id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<4; i++)
		EXPECT_EQ(result->getInt(i), 1);
}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_value_date3){
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_value_date3\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,VALUE,2012.01.01..2012.01.10);";
	script += "dummy = table(1:0, `id`sym`time`value, [INT, SYMBOL, TIMESTAMP, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`time);";
	conn.run(script);
	vector<string> colNames = { "id", "sym", "time", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_TIMESTAMP, DT_INT };
	int colNum = 4, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createTimestamp(2012, 1, i % 10 + 1, 9, 30, 30, 125));
		columnVecs[3]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_value_date3", "pt", "time", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	EXPECT_EQ(total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, concatDateTime(take(2012.01.01..2012.01.10, 1000000), 09:30:30.125) as time, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by time, id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by time, id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<4; i++)
		EXPECT_EQ(result->getInt(i), 1);
}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_value_date4){
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_value_date4\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,VALUE,2012.01.01..2012.01.10);";
	script += "dummy = table(1:0, `id`sym`time`value, [INT, SYMBOL, NANOTIMESTAMP, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`time);";
	conn.run(script);
	vector<string> colNames = { "id", "sym", "time", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_NANOTIMESTAMP, DT_INT };
	int colNum = 4, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createNanoTimestamp(2012, 1, i % 10 + 1, 9, 30, 30, 00));
		columnVecs[3]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_value_date4", "pt", "time", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	EXPECT_EQ(total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, concatDateTime(take(2012.01.01..2012.01.10, 1000000), 09:30:30.000000000) as time, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by time, id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by time, id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<4; i++)
		EXPECT_EQ(result->getInt(i), 1);
}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_value_month1){
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_value_month1\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,VALUE,2012.01M..2012.10M);";
	script += "dummy = table(1:0, `id`sym`date`value, [INT, SYMBOL, DATE, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`date);";
	conn.run(script);
	vector<string> colNames = { "id", "sym", "date", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_DATE, DT_INT };
	int colNum = 4, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createDate(2012, i % 10 + 1, 10));
		columnVecs[3]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_value_month1", "pt", "date", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	EXPECT_EQ(total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, take(temporalAdd(2012.01.10, 0..9, \"M\"), 1000000) as date, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by date, id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by date, id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<4; i++)
		EXPECT_EQ(result->getInt(i), 1);
}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_value_month2){
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_value_month2\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,VALUE,2012.01M..2012.10M);";
	script += "dummy = table(1:0, `id`sym`time`value, [INT, SYMBOL, DATETIME, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`time);";
	conn.run(script);
	vector<string> colNames = { "id", "sym", "time", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_DATETIME, DT_INT };
	int colNum = 4, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createDateTime(2012, i % 10 + 1, 10, 9, 30, 30));
		columnVecs[3]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_value_month2", "pt", "time", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	EXPECT_EQ(total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, concatDateTime(take(temporalAdd(2012.01.10, 0..9, \"M\"), 1000000), 09:30:30) as time, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by time, id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by time, id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<4; i++)
		EXPECT_EQ(result->getInt(i), 1);
}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_value_month3){
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_value_month3\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,VALUE,2012.01M..2012.10M);";
	script += "dummy = table(1:0, `id`sym`time`value, [INT, SYMBOL, TIMESTAMP, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`time);";
	conn.run(script);
	vector<string> colNames = { "id", "sym", "time", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_TIMESTAMP, DT_INT };
	int colNum = 4, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createTimestamp(2012, i % 10 + 1, 10, 9, 30, 30, 125));
		columnVecs[3]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_value_month3", "pt", "time", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	EXPECT_EQ(total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, concatDateTime(take(temporalAdd(2012.01.10, 0..9, \"M\"), 1000000), 09:30:30.125) as time, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by time, id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by time, id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<4; i++)
		EXPECT_EQ(result->getInt(i), 1);
}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_value_string){
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_value_string\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,VALUE,\"A\"+string(0..9));";
	script += "dummy = table(1:0, `id`sym`value, [INT, STRING, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`sym);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 10)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_value_string", "pt", "sym", pool);
	appender.append(t1);
	// string script1;
	// script1 += "login(`admin,`123456);";
	// script1 += "exec count(*) from loadTable(dbPath, `pt)";
	// int total = 0;
	// total = conn.run(script1)->getInt();
	// EXPECT_EQ("test_appender_value_string",total,1000000);
	// string script2;
	// script2 += "login(`admin,`123456);";
	// script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..9), 1000000) as sym, 0..999999 as value);";
	// script2 += "expected = select *  from tmp order by id, sym, value;";
	// script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	// script2 += "each(eqObj, re.values(), expected.values()).int();";
	// ConstantSP result = conn.run(script2);
	// for(int i=0; i<3; i++)
	//     EXPECT_EQ("test_appender_value_string", result->getInt(i), 1);
}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_value_symbol){
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_value_symbol\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,VALUE,\"A\"+string(0..9));";
	script += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`sym);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 10)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_value_symbol", "pt", "sym", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	EXPECT_EQ(total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..9), 1000000) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		EXPECT_EQ(result->getInt(i), 1);
}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_range_int){
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_range_int\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "ranges = cutPoints(0..999999, 10);";
	script += "db = database(dbPath,RANGE,ranges);";
	script += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`id);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 10)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_range_int", "pt", "id", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	EXPECT_EQ(total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(0..999999 as id, take(\"A\"+string(0..9), 1000000) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		EXPECT_EQ(result->getInt(i), 1);
}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_range_date){
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_range_date\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,RANGE,date(2012.01M..2012.11M));";
	script += "dummy = table(1:0, `id`sym`date`value, [INT, SYMBOL, DATE, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`date);";
	conn.run(script);
	vector<string> colNames = { "id", "sym", "date", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_DATE, DT_INT };
	int colNum = 4, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createDate(2012, i % 10 + 1, 10));
		columnVecs[3]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_range_date", "pt", "date", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	EXPECT_EQ(total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, take(temporalAdd(2012.01.10, 0..9, \"M\"), 1000000) as date, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by date, id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by date, id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<4; i++)
		EXPECT_EQ(result->getInt(i), 1);
}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_range_symbol){
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_range_symbol\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "ranges = cutPoints(\"A\"+string(0..999999), 10);";
	script += "db = database(dbPath,RANGE,ranges);";
	script += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`sym);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_range_symbol", "pt", "sym", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	EXPECT_EQ(total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(0..999999 as id, \"A\"+string(0..999999) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		EXPECT_EQ(result->getInt(i), 1);
}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_range_string){
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_range_string\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "ranges = cutPoints(\"A\"+string(0..999999), 10);";
	script += "db = database(dbPath,RANGE,ranges);";
	script += "dummy = table(1:0, `id`sym`value, [INT, STRING, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`sym);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_range_string", "pt", "sym", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	EXPECT_EQ(total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(0..999999 as id, \"A\"+string(0..999999) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		EXPECT_EQ(result->getInt(i), 1);
}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_hash_int){
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_hash_int\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,HASH,[INT, 10]);";
	script += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`id);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 10)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_hash_int", "pt", "id", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	EXPECT_EQ(total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(0..999999 as id, take(\"A\"+string(0..9), 1000000) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		EXPECT_EQ(result->getInt(i), 1);
}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_hash_date){
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_hash_date\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,HASH,[DATE,10]);";
	script += "dummy = table(1:0, `id`sym`date`value, [INT, SYMBOL, DATE, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`date);";
	conn.run(script);
	vector<string> colNames = { "id", "sym", "date", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_DATE, DT_INT };
	int colNum = 4, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createDate(2012, i % 10 + 1, 10));
		columnVecs[3]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_hash_date", "pt", "date", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	EXPECT_EQ(total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, take(temporalAdd(2012.01.10, 0..9, \"M\"), 1000000) as date, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by date, id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by date, id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<4; i++)
		EXPECT_EQ(result->getInt(i), 1);
}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_hash_symbol){
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_hash_symbol\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,HASH,[SYMBOL,10]);";
	script += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`sym);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_hash_symbol", "pt", "sym", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	EXPECT_EQ(total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(0..999999 as id, \"A\"+string(0..999999) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		EXPECT_EQ(result->getInt(i), 1);
}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_hash_string){
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_hash_string\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,HASH,[STRING,10]);";
	script += "dummy = table(1:0, `id`sym`value, [INT, STRING, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`sym);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_hash_string", "pt", "sym", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	EXPECT_EQ(total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(0..999999 as id, \"A\"+string(0..999999) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		EXPECT_EQ(result->getInt(i), 1);
}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_list_int){
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_list_int\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,LIST,[0..3, 4, 5..7, 8..9]);";
	script += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`id);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_list_int", "pt", "id", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	EXPECT_EQ(total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		EXPECT_EQ(result->getInt(i), 1);
}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_list_date){
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_list_date\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,LIST,[2012.01.01..2012.01.03, 2012.01.04..2012.01.07, 2012.01.08..2012.01.10]);";
	script += "dummy = table(1:0, `id`sym`date`value, [INT, SYMBOL, DATE, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`date);";
	conn.run(script);
	vector<string> colNames = { "id", "sym", "date", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_DATE, DT_INT };
	int colNum = 4, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createDate(2012, 1, i % 10 + 1));
		columnVecs[3]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_list_date", "pt", "date", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	EXPECT_EQ(total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, take(2012.01.01..2012.01.10, 1000000) as date, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by date, id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by date, id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<4; i++)
		EXPECT_EQ(result->getInt(i), 1);
}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_list_string){
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_list_string\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,LIST,[`A0`A1, `A2, `A3, `A4`A5`A6`A7, `A8`A9]);";
	script += "dummy = table(1:0, `id`sym`value, [INT, STRING, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`sym);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_STRING, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 10)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_list_string", "pt", "sym", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	EXPECT_EQ(total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..9), 1000000) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		EXPECT_EQ(result->getInt(i), 1);
}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_list_symbol){
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://list_symbol\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,LIST,[`A0`A1, `A2, `A3, `A4`A5`A6`A7, `A8`A9]);";
	script += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`sym);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 10)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://list_symbol", "pt", "sym", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	EXPECT_EQ(total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..9), 1000000) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		EXPECT_EQ(result->getInt(i), 1);
}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_value_hash){
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_value_hash\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db1 = database(\"\",VALUE,`A0`A1`A2`A3`A4`A5`A6`A7`A8`A9);";
	script += "db2 = database(\"\",HASH,[INT, 2]);";
	script += "db = database(dbPath, COMPO, [db1, db2]);";
	script += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`sym`id);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 10)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_value_hash", "pt", "sym", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	EXPECT_EQ(total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..9), 1000000) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		EXPECT_EQ(result->getInt(i), 1);

}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_value_range){
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_value_range\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db1 = database(\"\",VALUE,`A0`A1`A2`A3`A4`A5`A6`A7`A8`A9);";
	script += "db2 = database(\"\",RANGE,0 5 10);";
	script += "db = database(dbPath, COMPO, [db1, db2]);";
	script += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`sym`id);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 10)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_value_range", "pt", "sym", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	EXPECT_EQ(total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..9), 1000000) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		EXPECT_EQ(result->getInt(i), 1);
}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withIntArrayVector){
    string dbName ="dfs://test_PartitionedTableAppender_withIntArrayVector";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withIntArrayVector\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, INT[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);

	VectorSP av1 = Util::createArrayVector(DT_INT_ARRAY, 0, 3);
	av1->append(v1);
	av1->append(v1);
	av1->append(v1);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_INT_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withIntNullArrayVector){
    string dbName ="dfs://test_PartitionedTableAppender_withIntNullArrayVector";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withIntNullArrayVector\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, INT[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_INT, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->setNull(0);
	v2->setNull(1);
	v2->setNull(2);	

	VectorSP av1 = Util::createArrayVector(DT_INT_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_INT_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withIntArrayVectorMorethan65535){
    string dbName ="dfs://test_PartitionedTableAppender_withIntArrayVector";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withIntArrayVector\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,0 70000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, INT[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_INT, 70000, 70000);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	for(unsigned int i=0;i<70000;i++)
		v2->setInt(i, i);

	VectorSP av1 = Util::createArrayVector(DT_INT_ARRAY, 0, 70000);
	for(unsigned int i=0;i<3;i++)
		av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_INT_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withCharArrayVector){
    string dbName ="dfs://test_PartitionedTableAppender_withCharArrayVector";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withCharArrayVector\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, CHAR[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
    VectorSP v2 = Util::createVector(DT_CHAR, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->setChar(0, 1);
	v2->setChar(1, 0);
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_CHAR_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_CHAR_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withCharNullArrayVector){
    string dbName ="dfs://test_PartitionedTableAppender_withCharNullArrayVector";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withCharNullArrayVector\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, CHAR[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
    VectorSP v2 = Util::createVector(DT_CHAR, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->setNull(0);
	v2->setNull(1);
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_CHAR_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_CHAR_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withCharArrayVectorMorethan65535){
    string dbName ="dfs://test_PartitionedTableAppender_withCharArrayVectorMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withCharArrayVectorMorethan65535\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,0 70000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, CHAR[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_CHAR, 70000, 70000);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	for(unsigned int i=0;i<70000;i++)
		v2->setChar(i, i);

	VectorSP av1 = Util::createArrayVector(DT_CHAR_ARRAY, 0, 70000);
	for(unsigned int i=0;i<3;i++)
		av1->append(v2);
		
	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_CHAR_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withFloatArrayVector){
    string dbName ="dfs://test_PartitionedTableAppender_withFloatArrayVector";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withFloatArrayVector\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, FLOAT[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
    VectorSP v2 = Util::createVector(DT_FLOAT, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->setFloat(0, 1);
	v2->setFloat(1, 0);
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_FLOAT_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_FLOAT_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withFloatNullArrayVector){
    string dbName ="dfs://test_PartitionedTableAppender_withFloatNullArrayVector";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withFloatNullArrayVector\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, FLOAT[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
    VectorSP v2 = Util::createVector(DT_FLOAT, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->setNull(0);
	v2->setNull(1);
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_FLOAT_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_FLOAT_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withFloatArrayVectorMorethan65535){
    string dbName ="dfs://test_PartitionedTableAppender_withFloatArrayVectorMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withFloatArrayVectorMorethan65535\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,0 70000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, FLOAT[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_FLOAT, 70000, 70000);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	for(unsigned int i=0;i<70000;i++)
		v2->setFloat(i, i);

	VectorSP av1 = Util::createArrayVector(DT_FLOAT_ARRAY, 0, 70000);
	for(unsigned int i=0;i<3;i++)
		av1->append(v2);
		
	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_FLOAT_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withDateArrayVector){
    string dbName ="dfs://test_PartitionedTableAppender_withDateArrayVector";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withDateArrayVector\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, DATE[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
    VectorSP v2 = Util::createVector(DT_DATE, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->set(0, Util::createDate(0));
	v2->set(1, Util::createDate(1000));
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_DATE_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_DATE_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withDateNullArrayVector){
    string dbName ="dfs://test_PartitionedTableAppender_withDateNullArrayVector";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withDateNullArrayVector\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, DATE[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
    VectorSP v2 = Util::createVector(DT_DATE, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->setNull(0);
	v2->setNull(1);
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_DATE_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_DATE_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withDateArrayVectorMorethan65535){
    string dbName ="dfs://test_PartitionedTableAppender_withDateArrayVectorMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withDateArrayVectorMorethan65535\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,0 70000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, DATE[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_DATE, 70000, 70000);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	for(unsigned int i=0;i<70000;i++)
		v2->set(i, Util::createDate(i));

	VectorSP av1 = Util::createArrayVector(DT_DATE_ARRAY, 0, 70000);
	for(unsigned int i=0;i<3;i++)
		av1->append(v2);
		
	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_DATE_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withMonthArrayVector){
    string dbName ="dfs://test_PartitionedTableAppender_withMonthArrayVector";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withMonthArrayVector\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, MONTH[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
    VectorSP v2 = Util::createVector(DT_MONTH, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->set(0, Util::createMonth(0));
	v2->set(1, Util::createMonth(1000));
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_MONTH_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_MONTH_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withMonthNullArrayVector){
    string dbName ="dfs://test_PartitionedTableAppender_withMonthNullArrayVector";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withMonthNullArrayVector\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, MONTH[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
    VectorSP v2 = Util::createVector(DT_MONTH, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->setNull(0);
	v2->setNull(1);
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_MONTH_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_MONTH_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withMonthArrayVectorMorethan65535){
    string dbName ="dfs://test_PartitionedTableAppender_withMonthArrayVectorMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withMonthArrayVectorMorethan65535\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,0 70000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, MONTH[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_MONTH, 70000, 70000);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	for(unsigned int i=0;i<70000;i++)
		v2->set(i, Util::createMonth(i));

	VectorSP av1 = Util::createArrayVector(DT_MONTH_ARRAY, 0, 70000);
	for(unsigned int i=0;i<3;i++)
		av1->append(v2);
		
	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_MONTH_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withTimeArrayVector){
    string dbName ="dfs://test_PartitionedTableAppender_withTimeArrayVector";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withTimeArrayVector\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, TIME[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
    VectorSP v2 = Util::createVector(DT_TIME, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->set(0, Util::createTime(0));
	v2->set(1, Util::createTime(1000));
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_TIME_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_TIME_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withTimeNullArrayVector){
    string dbName ="dfs://test_PartitionedTableAppender_withTimeNullArrayVector";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withTimeNullArrayVector\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, TIME[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
    VectorSP v2 = Util::createVector(DT_TIME, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->setNull(0);
	v2->setNull(1);
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_TIME_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_TIME_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withTimeArrayVectorMorethan65535){
    string dbName ="dfs://test_PartitionedTableAppender_withTimeArrayVectorMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withTimeArrayVectorMorethan65535\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,0 70000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, TIME[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_TIME, 70000, 70000);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	for(unsigned int i=0;i<70000;i++)
		v2->set(i, Util::createTime(i));

	VectorSP av1 = Util::createArrayVector(DT_TIME_ARRAY, 0, 70000);
	for(unsigned int i=0;i<3;i++)
		av1->append(v2);
		
	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_TIME_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withSecondArrayVector){
    string dbName ="dfs://test_PartitionedTableAppender_withSecondArrayVector";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withSecondArrayVector\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, SECOND[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
    VectorSP v2 = Util::createVector(DT_SECOND, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->set(0, Util::createSecond(0));
	v2->set(1, Util::createSecond(1000));
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_SECOND_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SECOND_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withSecondNullArrayVector){
    string dbName ="dfs://test_PartitionedTableAppender_withSecondNullArrayVector";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withSecondNullArrayVector\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, SECOND[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
    VectorSP v2 = Util::createVector(DT_SECOND, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->setNull(0);
	v2->setNull(1);
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_SECOND_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SECOND_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withSecondArrayVectorMorethan65535){
    string dbName ="dfs://test_PartitionedTableAppender_withSecondArrayVectorMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withSecondArrayVectorMorethan65535\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,0 70000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, SECOND[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_SECOND, 70000, 70000);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	for(unsigned int i=0;i<70000;i++)
		v2->set(i, Util::createSecond(i));

	VectorSP av1 = Util::createArrayVector(DT_SECOND_ARRAY, 0, 70000);
	for(unsigned int i=0;i<3;i++)
		av1->append(v2);
		
	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SECOND_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withDatehourArrayVector){
    string dbName ="dfs://test_PartitionedTableAppender_withDatehourArrayVector";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withDatehourArrayVector\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, DATEHOUR[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
    VectorSP v2 = Util::createVector(DT_DATEHOUR, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->set(0, Util::createDateHour(0));
	v2->set(1, Util::createDateHour(1000));
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_DATEHOUR_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_DATEHOUR_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withDatehourNullArrayVector){
    string dbName ="dfs://test_PartitionedTableAppender_withDatehourNullArrayVector";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withDatehourNullArrayVector\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, DATEHOUR[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
    VectorSP v2 = Util::createVector(DT_DATEHOUR, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->setNull(0);
	v2->setNull(1);
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_DATEHOUR_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_DATEHOUR_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withDatehourArrayVectorMorethan65535){
    string dbName ="dfs://test_PartitionedTableAppender_withDatehourArrayVectorMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withDatehourArrayVectorMorethan65535\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,0 70000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, DATEHOUR[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_DATEHOUR, 70000, 70000);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	for(unsigned int i=0;i<70000;i++)
		v2->set(i, Util::createDateHour(i));

	VectorSP av1 = Util::createArrayVector(DT_DATEHOUR_ARRAY, 0, 70000);
	for(unsigned int i=0;i<3;i++)
		av1->append(v2);
		
	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_DATEHOUR_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withUuidArrayVector){
    string dbName ="dfs://test_PartitionedTableAppender_withUuidArrayVector";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withUuidArrayVector\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, UUID[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
    VectorSP v2 = Util::createVector(DT_UUID, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->setString(0, "5d212a78-cc48-e3b1-4235-b4d91473ee87");
	v2->setString(1, "5d212a78-cc48-e3b1-4235-b4d91473ee99");
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_UUID_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_UUID_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}


	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withUuidNullArrayVector){
    string dbName ="dfs://test_PartitionedTableAppender_withUuidNullArrayVector";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withUuidNullArrayVector\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, UUID[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
    VectorSP v2 = Util::createVector(DT_UUID, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->setNull(0);
	v2->setNull(1);
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_UUID_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_UUID_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}


	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest, test_PartitionedTableAppender_withUuidArrayVectorMorethan65535){
    string dbName ="dfs://test_PartitionedTableAppender_withUuidArrayVectorMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_PartitionedTableAppender_withUuidArrayVectorMorethan65535\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,0 70000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, UUID[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_UUID, 70000, 70000);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	for(unsigned int i=0;i<70000;i++)
		v2->setString(i, "5d212a78-cc48-e3b1-4235-b4d91473ee87");

	VectorSP av1 = Util::createArrayVector(DT_UUID_ARRAY, 0, 70000);
	for(unsigned int i=0;i<3;i++)
		av1->append(v2);
		
	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_UUID_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

	PartitionedTableAppender appender(dbName, tableName, "id", pool);
	appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_partitionColNameNull){
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_PartitionedTableAppender_partitionColName_Null\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db1 = database(\"\",VALUE,`A0`A1`A2`A3`A4`A5`A6`A7`A8`A9);";
	script += "db2 = database(\"\",RANGE,0 5 10);";
	script += "db = database(dbPath, COMPO, [db1, db2]);";
	script += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`sym`id);";
	conn.run(script);

	EXPECT_ANY_THROW(PartitionedTableAppender appender("dfs://test_PartitionedTableAppender_partitionColName_Null", "pt", "", pool));
}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_partitionColNameValueNull){
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_PartitionedTableAppender_partitionColNameValueNull\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db1 = database(\"\",VALUE,`A0`A1`A2`A3`A4`A5`A6`A7`A8`A9);";
	script += "db2 = database(\"\",RANGE,0 5 10);";
	script += "db = database(dbPath, COMPO, [db1, db2]);";
	script += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`sym`id);";
	conn.run(script);

	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString(""));
		columnVecs[2]->set(i, Util::createInt(i));
	}

	PartitionedTableAppender appender("dfs://test_PartitionedTableAppender_partitionColNameValueNull", "pt", "sym", pool);
	EXPECT_ANY_THROW(appender.append(t1));

}

TEST_F(PartitionedTableAppenderTest,test_PartitionedTableAppender_compo3){
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_compo3\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db1 = database(\"\",VALUE,2012.01.01..2012.01.10);";
	script += "ranges=cutPoints(\"A\"+string(0..999), 5);";
	script += "db2 = database(\"\",RANGE,ranges);";
	script += "db3 = database(\"\",HASH,[INT, 2]);";
	script += "db = database(dbPath,COMPO,[db1, db2, db3]);";
	script += "dummy = table(1:0, `id`sym`date`value, [INT, SYMBOL, DATE, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`date`sym`id);";
	conn.run(script);
	vector<string> colNames = { "id", "sym", "date", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_DATE, DT_INT };
	int colNum = 4, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createDate(2012, 1, i % 10 + 1));
		columnVecs[3]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_compo3", "pt", "date", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from pt";
	int total = 0;
	total = conn.run(script1)->getInt();
	EXPECT_EQ(total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, take(2012.01.01..2012.01.10, 1000000) as date, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by date, id, sym, value;";
	script2 += "re = select * from pt order by date, id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<4; i++)
		EXPECT_EQ(result->getInt(i), 1);
}

TEST_F(PartitionedTableAppenderTest,test_mutithread_schema){
	int64_t startTime, time;
	startTime = Util::getEpochTime();
	pool.run("sleep(1000)", 0);
	pool.run("sleep(3000)", 1);
	pool.run("sleep(5000)", 2);
	while (true) {
		if (pool.isFinished(0) && pool.isFinished(1) && pool.isFinished(2)) {
			time = (Util::getEpochTime() - startTime) / 1000;
			EXPECT_EQ((int)time, 5);
			break;
		}
	}
}

TEST_F(PartitionedTableAppenderTest,test_mutithread_WandR){
	int64_t startTime, time;
	startTime = Util::getEpochTime();
	string script, script1;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_mutithread_\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db1 = database(\"\",VALUE,`A0`A1`A2`A3`A4`A5`A6`A7`A8`A9);";
	script += "db2 = database(\"\",RANGE,0 5 10);";
	script += "db = database(dbPath, COMPO, [db1, db2]);";
	script += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`sym`id);";
	pool.run(script, 0);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 10)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_mutithread_", "pt", "sym", pool);
	appender.append(t);

	script1 += "login(`admin,`123456);";
	script1 += "dbPath = \"dfs://test_mutithread_1\";";
	script1 += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script1 += "db1 = database(\"\",VALUE,`A0`A1`A2`A3`A4`A5`A6`A7`A8`A9);";
	script1 += "db2 = database(\"\",RANGE,0 5 10);";
	script1 += "db = database(dbPath, COMPO, [db1, db2]);";
	script1 += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script1 += "pt = db.createPartitionedTable(dummy,`pt,`sym`id);";
	conn.run(script1);
	PartitionedTableAppender appender1("dfs://test_mutithread_1", "pt", "sym", pool);
	appender1.append(t);
	time = Util::getEpochTime() - startTime;
	cout << "test_mutithread_:" << time << "ms" << endl;
}

TEST_F(PartitionedTableAppenderTest,test_mutithread_Session){
	string script, script1;
	script += "getSessionMemoryStat().size()";
	int count1 = conn.run(script)->getInt();
	pool.shutDown();
	int count2 = conn.run(script)->getInt();
	EXPECT_EQ(count1 - count2, 10);
}