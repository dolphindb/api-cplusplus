class AutoFitTableappenderTest:public testing::Test
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
		
        cout<<"ok"<<endl;
    }
    virtual void TearDown()
    {
        conn.run("undef all;");
    }
};

TEST_F(AutoFitTableappenderTest,test_AutoFitTableAppender_unconnected){
	DBConnection connNew(false, false);
	EXPECT_ANY_THROW(AutoFitTableAppender appender("", "st1", connNew));

}

TEST_F(AutoFitTableappenderTest,test_AutoFitTableAppender_appendErrTableColNums){
	conn.run("tab = table(`a`x`d`s`cs as col1, 2 3 4 5 6 as col2);share tab as temp_tab");
	AutoFitTableAppender appender("", "temp_tab", conn);
	vector<string> colNames = {"col1", "col2", "col3"};
	vector<ConstantSP> cols ={Util::createString("zzz123中文a"), Util::createInt(7), Util::createInt(5)};
	TableSP t = Util::createTable(colNames, cols);
	EXPECT_ANY_THROW(appender.append(t));
}

TEST_F(AutoFitTableappenderTest,test_AutoFitTableAppender_appendErrTableColType){
	conn.run("tab = table(`a`x`d`s`cs as col1, 2 3 4 5 6 as col2);share tab as temp_tab");
	AutoFitTableAppender appender("", "temp_tab", conn);
	vector<string> colNames = {"col1", "col2"};
	vector<ConstantSP> cols ={Util::createString("zzz123中文a"), Util::createDate(1000)};
	TableSP t = Util::createTable(colNames, cols);
	EXPECT_ANY_THROW(appender.append(t));
}

TEST_F(AutoFitTableappenderTest,test_AutoFitTableAppender_appendAllDataTypesToinMemoryTable){
	srand((int)time(NULL));
	int colNum = 24, rowNum = 1000;
	int scale32 = rand()%9, scale64 = rand()%18;
	vector<string> colNamesVec1;
	for (int i = 0; i < colNum; i++){
		colNamesVec1.emplace_back("col"+to_string(i));
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

	TableSP tab1 = Util::createTable(colNamesVec1, colTypesVec1, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab1->getColumn(i));

	}
	for (int i = 0; i < rowNum-1; i++){
		columnVecs[0]->set(i, Util::createChar(rand()%CHAR_MAX));
		columnVecs[1]->set(i, Util::createBool(rand()%2));
		columnVecs[2]->set(i, Util::createShort(rand()%SHRT_MAX));
		columnVecs[3]->set(i, Util::createInt(rand()%INT_MAX));
		columnVecs[4]->set(i, Util::createLong(rand()%LLONG_MAX));
		columnVecs[5]->set(i, Util::createDate(rand()%INT_MAX));
		columnVecs[6]->set(i, Util::createMonth(rand()%INT_MAX));
		columnVecs[7]->set(i, Util::createTime(rand()%INT_MAX));
		columnVecs[8]->set(i, Util::createMinute(rand()%1440));
		columnVecs[9]->set(i, Util::createDateTime(rand()%INT_MAX));
		columnVecs[10]->set(i, Util::createSecond(rand()%86400));
		columnVecs[11]->set(i, Util::createTimestamp(rand()%LLONG_MAX));
		columnVecs[12]->set(i, Util::createNanoTime(rand()%LLONG_MAX));
		columnVecs[13]->set(i, Util::createNanoTimestamp(rand()%LLONG_MAX));
		columnVecs[14]->set(i, Util::createFloat(rand()/float(RAND_MAX)));
		columnVecs[15]->set(i, Util::createDouble(rand()/double(RAND_MAX)));
		columnVecs[16]->set(i, Util::createString("str"+to_string(i)));
		columnVecs[17]->set(i, Util::parseConstant(DT_UUID,"5d212a78-cc48-e3b1-4235-b4d91473ee87"));	
		columnVecs[18]->set(i, Util::parseConstant(DT_IP,"192.0.0."+to_string(rand()%255)));
		columnVecs[19]->set(i, Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec32"));
		columnVecs[20]->set(i, Util::createBlob("blob"+to_string(i)));
		columnVecs[21]->set(i, Util::createDateHour(rand()%INT_MAX));
		columnVecs[22]->set(i, Util::createDecimal32(scale32,rand()/float(RAND_MAX)));
		columnVecs[23]->set(i, Util::createDecimal64(scale64,rand()/double(RAND_MAX)));
	}
	for (int j = 0; j < colNum; j++)
		columnVecs[j]->setNull(rowNum-1);

	string script1;
	script1 += "login('admin', '123456');";
	script1 += "try{undef(`st1, SHARED)}catch(ex){print ex};";
	script1 += "temp = table(100:0, take(`col,24)+string(take(0..23,24)), \
	[CHAR, BOOL, SHORT, INT, LONG, DATE, MONTH, TIME, MINUTE, DATETIME, SECOND, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, STRING, UUID, IPADDR, INT128, BLOB, DATEHOUR, DECIMAL32("+to_string(scale32)+"), DECIMAL64("+to_string(scale64)+")]);";
	script1 += "share temp as st1;";
	conn.run(script1);
	AutoFitTableAppender appender("", "st1", conn);
	int res = appender.append(tab1);
	EXPECT_EQ(res, rowNum);

	conn.upload("tab1",tab1);
	string script3;
	script3 += "each(eqObj, tab1.values(), st1.values());";
	ConstantSP result2 = conn.run(script3);
	for (int i = 0; i<result2->size(); i++)
		EXPECT_TRUE(result2->get(i)->getBool());

	conn.run("undef(`st1, SHARED)");
}


TEST_F(AutoFitTableappenderTest,test_AutoFitTableAppender_appendAllDataTypesToindexedTable){
	srand((int)time(NULL));
	int colNum = 24, rowNum = 1000;
	int scale32 = rand()%9, scale64 = rand()%18;
	vector<string> colNamesVec1;
	for (int i = 0; i < colNum; i++){
		colNamesVec1.emplace_back("col"+to_string(i));
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

	TableSP tab1 = Util::createTable(colNamesVec1, colTypesVec1, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab1->getColumn(i));

	}
	for (int i = 0; i < rowNum-1; i++){
		columnVecs[0]->set(i, Util::createChar(rand()%CHAR_MAX));
		columnVecs[1]->set(i, Util::createBool(rand()%2));
		columnVecs[2]->set(i, Util::createShort(rand()%SHRT_MAX));
		columnVecs[3]->set(i, Util::createInt(rand()%INT_MAX));
		columnVecs[4]->set(i, Util::createLong(rand()%LLONG_MAX));
		columnVecs[5]->set(i, Util::createDate(rand()%INT_MAX));
		columnVecs[6]->set(i, Util::createMonth(rand()%INT_MAX));
		columnVecs[7]->set(i, Util::createTime(rand()%INT_MAX));
		columnVecs[8]->set(i, Util::createMinute(rand()%1440));
		columnVecs[9]->set(i, Util::createDateTime(rand()%INT_MAX));
		columnVecs[10]->set(i, Util::createSecond(rand()%86400));
		columnVecs[11]->set(i, Util::createTimestamp(rand()%LLONG_MAX));
		columnVecs[12]->set(i, Util::createNanoTime(rand()%LLONG_MAX));
		columnVecs[13]->set(i, Util::createNanoTimestamp(rand()%LLONG_MAX));
		columnVecs[14]->set(i, Util::createFloat(rand()/float(RAND_MAX)));
		columnVecs[15]->set(i, Util::createDouble(rand()/double(RAND_MAX)));
		columnVecs[16]->set(i, Util::createString("str"+to_string(i)));
		columnVecs[17]->set(i, Util::parseConstant(DT_UUID,"5d212a78-cc48-e3b1-4235-b4d91473ee87"));	
		columnVecs[18]->set(i, Util::parseConstant(DT_IP,"192.0.0."+to_string(rand()%255)));
		columnVecs[19]->set(i, Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec32"));
		columnVecs[20]->set(i, Util::createBlob("blob"+to_string(i)));
		columnVecs[21]->set(i, Util::createDateHour(rand()%INT_MAX));
		columnVecs[22]->set(i, Util::createDecimal32(scale32,rand()/float(RAND_MAX)));
		columnVecs[23]->set(i, Util::createDecimal64(scale64,rand()/double(RAND_MAX)));
	}
	for (int j = 0; j < colNum; j++)
		columnVecs[j]->setNull(rowNum-1);

	string script1;
	script1 += "login('admin', '123456');";
	script1 += "try{undef(`st1, SHARED)}catch(ex){};";
	script1 += "temp = table(100:0, take(`col,24)+string(take(0..23,24)), \
	[CHAR, BOOL, SHORT, INT, LONG, DATE, MONTH, TIME, MINUTE, DATETIME, SECOND, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, STRING, UUID, IPADDR, INT128, BLOB, DATEHOUR, DECIMAL32("+to_string(scale32)+"), DECIMAL64("+to_string(scale64)+")]);";
	script1 += "st1 = indexedTable(`col16,temp);";
	conn.run(script1);
	AutoFitTableAppender appender("", "st1", conn);
	int res = appender.append(tab1);
	EXPECT_EQ(res, rowNum);

	conn.upload("tab1",tab1);
	string script3;
	script3 += "each(eqObj, tab1.values(), st1.values());";
	ConstantSP result2 = conn.run(script3);
	for (int i = 0; i<result2->size(); i++)
		EXPECT_TRUE(result2->get(i)->getBool());

	// conn.run("undef(`st1, SHARED)");
}


TEST_F(AutoFitTableappenderTest,test_AutoFitTableAppender_appendAllDataTypesTokeyedTable){
	srand((int)time(NULL));
	int colNum = 24, rowNum = 1000;
	int scale32 = rand()%9, scale64 = rand()%18;
	vector<string> colNamesVec1;
	for (int i = 0; i < colNum; i++){
		colNamesVec1.emplace_back("col"+to_string(i));
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

	TableSP tab1 = Util::createTable(colNamesVec1, colTypesVec1, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab1->getColumn(i));

	}
	for (int i = 0; i < rowNum-1; i++){
		columnVecs[0]->set(i, Util::createChar(rand()%CHAR_MAX));
		columnVecs[1]->set(i, Util::createBool(rand()%2));
		columnVecs[2]->set(i, Util::createShort(rand()%SHRT_MAX));
		columnVecs[3]->set(i, Util::createInt(rand()%INT_MAX));
		columnVecs[4]->set(i, Util::createLong(rand()%LLONG_MAX));
		columnVecs[5]->set(i, Util::createDate(rand()%INT_MAX));
		columnVecs[6]->set(i, Util::createMonth(rand()%INT_MAX));
		columnVecs[7]->set(i, Util::createTime(rand()%INT_MAX));
		columnVecs[8]->set(i, Util::createMinute(rand()%1440));
		columnVecs[9]->set(i, Util::createDateTime(rand()%INT_MAX));
		columnVecs[10]->set(i, Util::createSecond(rand()%86400));
		columnVecs[11]->set(i, Util::createTimestamp(rand()%LLONG_MAX));
		columnVecs[12]->set(i, Util::createNanoTime(rand()%LLONG_MAX));
		columnVecs[13]->set(i, Util::createNanoTimestamp(rand()%LLONG_MAX));
		columnVecs[14]->set(i, Util::createFloat(rand()/float(RAND_MAX)));
		columnVecs[15]->set(i, Util::createDouble(rand()/double(RAND_MAX)));
		columnVecs[16]->set(i, Util::createString("str"+to_string(i)));
		columnVecs[17]->set(i, Util::parseConstant(DT_UUID,"5d212a78-cc48-e3b1-4235-b4d91473ee87"));	
		columnVecs[18]->set(i, Util::parseConstant(DT_IP,"192.0.0."+to_string(rand()%255)));
		columnVecs[19]->set(i, Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec32"));
		columnVecs[20]->set(i, Util::createBlob("blob"+to_string(i)));
		columnVecs[21]->set(i, Util::createDateHour(rand()%INT_MAX));
		columnVecs[22]->set(i, Util::createDecimal32(scale32,rand()/float(RAND_MAX)));
		columnVecs[23]->set(i, Util::createDecimal64(scale64,rand()/double(RAND_MAX)));
	}
	for (int j = 0; j < colNum; j++)
		columnVecs[j]->setNull(rowNum-1);

	string script1;
	script1 += "login('admin', '123456');";
	script1 += "try{undef(`st1, SHARED)}catch(ex){};";
	script1 += "temp = table(100:0, take(`col,24)+string(take(0..23,24)), \
	[CHAR, BOOL, SHORT, INT, LONG, DATE, MONTH, TIME, MINUTE, DATETIME, SECOND, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, STRING, UUID, IPADDR, INT128, BLOB, DATEHOUR, DECIMAL32("+to_string(scale32)+"), DECIMAL64("+to_string(scale64)+")]);";
	script1 += "st1 = keyedTable(`col16,temp);";
	conn.run(script1);
	AutoFitTableAppender appender("", "st1", conn);
	int res = appender.append(tab1);
	EXPECT_EQ(res, rowNum);

	conn.upload("tab1",tab1);
	string script3;
	script3 += "each(eqObj, tab1.values(), st1.values());";
	ConstantSP result2 = conn.run(script3);
	for (int i = 0; i<result2->size(); i++)
		EXPECT_TRUE(result2->get(i)->getBool());

	// conn.run("undef(`st1, SHARED)");
}


TEST_F(AutoFitTableappenderTest,test_AutoFitTableAppender_appendAllDataTypesTopartitionedTableOLAP){ // OLAP not support datatype blob
	srand((int)time(NULL));
	int colNum = 23, rowNum = 1000;
	int scale32 = rand()%9, scale64 = rand()%18;
	vector<string> colNamesVec1;
	for (int i = 0; i < colNum; i++){
		colNamesVec1.emplace_back("col"+to_string(i));
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
	colTypesVec1.emplace_back(DT_DATEHOUR);
	colTypesVec1.emplace_back(DT_DECIMAL32);
	colTypesVec1.emplace_back(DT_DECIMAL64);

	TableSP tab1 = Util::createTable(colNamesVec1, colTypesVec1, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab1->getColumn(i));

	}
	for (int i = 0; i < rowNum-1; i++){
		columnVecs[0]->set(i, Util::createChar(rand()%CHAR_MAX));
		columnVecs[1]->set(i, Util::createBool(rand()%2));
		columnVecs[2]->set(i, Util::createShort(rand()%SHRT_MAX));
		columnVecs[3]->set(i, Util::createInt(rand()%INT_MAX));
		columnVecs[4]->set(i, Util::createLong(rand()%LLONG_MAX));
		columnVecs[5]->set(i, Util::createDate(rand()%INT_MAX));
		columnVecs[6]->set(i, Util::createMonth(rand()%INT_MAX));
		columnVecs[7]->set(i, Util::createTime(rand()%INT_MAX));
		columnVecs[8]->set(i, Util::createMinute(rand()%1440));
		columnVecs[9]->set(i, Util::createDateTime(rand()%INT_MAX));
		columnVecs[10]->set(i, Util::createSecond(rand()%86400));
		columnVecs[11]->set(i, Util::createTimestamp(rand()%LLONG_MAX));
		columnVecs[12]->set(i, Util::createNanoTime(rand()%LLONG_MAX));
		columnVecs[13]->set(i, Util::createNanoTimestamp(rand()%LLONG_MAX));
		columnVecs[14]->set(i, Util::createFloat(rand()/float(RAND_MAX)));
		columnVecs[15]->set(i, Util::createDouble(rand()/double(RAND_MAX)));
		columnVecs[16]->set(i, Util::createString("str"+to_string(i)));
		columnVecs[17]->set(i, Util::parseConstant(DT_UUID,"5d212a78-cc48-e3b1-4235-b4d91473ee87"));	
		columnVecs[18]->set(i, Util::parseConstant(DT_IP,"192.0.0."+to_string(rand()%255)));
		columnVecs[19]->set(i, Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec32"));
		columnVecs[20]->set(i, Util::createDateHour(rand()%INT_MAX));
		columnVecs[21]->set(i, Util::createDecimal32(scale32,rand()/float(RAND_MAX)));
		columnVecs[22]->set(i, Util::createDecimal64(scale64,rand()/double(RAND_MAX)));
	}
	for (int j = 0; j < colNum; j++){
		if(j == 3)
			columnVecs[3]->set(rowNum-1, Util::createInt(rand()%INT_MAX));  //partition-column's value must be not null
		else
			columnVecs[j]->setNull(rowNum-1);
	}

    string dbName ="dfs://test_AutoFitTableAppender_appendAllDataTypesTopartitionedTable";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_appendAllDataTypesTopartitionedTable\";"
			"if(exists(dbName)){dropDatabase(dbName)};"
			"db  = database(dbName, HASH,[INT,1]);"
			"temp = table(1000:0, take(`col,23)+string(take(0..22,23)), \
			[CHAR, BOOL, SHORT, INT, LONG, DATE, MONTH, TIME, MINUTE, DATETIME, SECOND, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, STRING, UUID, IPADDR, INT128, DATEHOUR, DECIMAL32("+to_string(scale32)+"), DECIMAL64("+to_string(scale64)+")]);"
			"pt = createPartitionedTable(db,temp,`pt,`col3);";
	conn.run(script);
	AutoFitTableAppender appender(dbName, tableName, conn);
	int res = appender.append(tab1);
	EXPECT_EQ(res, rowNum);

	conn.upload("tab1",tab1);
	string script3;
	script3 += "st1 = select * from pt;";
	script3 += "each(eqObj, tab1.values(), st1.values());";
	ConstantSP result2 = conn.run(script3);
	// cout<<conn.run("st1")->getString();
	for (int i = 0; i<result2->size(); i++)
		EXPECT_TRUE(result2->get(i)->getBool());
}


// TEST_F(AutoFitTableappenderTest,test_AutoFitTableAppender_appendAllDataTypesTopartitionedTableTSDB){
// 	int colNum = 24, rowNum = 1000;
// 	int scale32 = rand()%9, scale64 = rand()%18;
// 	vector<string> colNamesVec1;
// 	for (int i = 0; i < colNum; i++){
// 		colNamesVec1.emplace_back("col"+to_string(i));
// 	}
// 	vector<DATA_TYPE> colTypesVec1;
// 	colTypesVec1.emplace_back(DT_CHAR);
// 	colTypesVec1.emplace_back(DT_BOOL);
// 	colTypesVec1.emplace_back(DT_SHORT);
// 	colTypesVec1.emplace_back(DT_INT);
// 	colTypesVec1.emplace_back(DT_LONG);
// 	colTypesVec1.emplace_back(DT_DATE);
// 	colTypesVec1.emplace_back(DT_MONTH);
// 	colTypesVec1.emplace_back(DT_TIME);
// 	colTypesVec1.emplace_back(DT_MINUTE);
// 	colTypesVec1.emplace_back(DT_DATETIME);
// 	colTypesVec1.emplace_back(DT_SECOND);
// 	colTypesVec1.emplace_back(DT_TIMESTAMP);
// 	colTypesVec1.emplace_back(DT_NANOTIME);
// 	colTypesVec1.emplace_back(DT_NANOTIMESTAMP);
// 	colTypesVec1.emplace_back(DT_FLOAT);
// 	colTypesVec1.emplace_back(DT_DOUBLE);
// 	colTypesVec1.emplace_back(DT_STRING);
// 	colTypesVec1.emplace_back(DT_UUID);
// 	colTypesVec1.emplace_back(DT_IP);
// 	colTypesVec1.emplace_back(DT_INT128);
// 	colTypesVec1.emplace_back(DT_BLOB);
// 	colTypesVec1.emplace_back(DT_DATEHOUR);
// 	colTypesVec1.emplace_back(DT_DECIMAL32);
// 	colTypesVec1.emplace_back(DT_DECIMAL64);

// 	srand((int)time(NULL));
// 	TableSP tab1 = Util::createTable(colNamesVec1, colTypesVec1, rowNum, rowNum);
// 	vector<VectorSP> columnVecs;
// 	columnVecs.reserve(colNum);
// 	for (int i = 0; i < colNum; i++){
// 		columnVecs.emplace_back(tab1->getColumn(i));

// 	}
// 	for (int i = 0; i < rowNum-1; i++){
// 		columnVecs[0]->set(i, Util::createChar(rand()%CHAR_MAX));
// 		columnVecs[1]->set(i, Util::createBool(rand()%2));
// 		columnVecs[2]->set(i, Util::createShort(rand()%SHRT_MAX));
// 		columnVecs[3]->set(i, Util::createInt(rand()%INT_MAX));
// 		columnVecs[4]->set(i, Util::createLong(rand()%LLONG_MAX));
// 		columnVecs[5]->set(i, Util::createDate(rand()%INT_MAX));
// 		columnVecs[6]->set(i, Util::createMonth(rand()%INT_MAX));
// 		columnVecs[7]->set(i, Util::createTime(rand()%INT_MAX));
// 		columnVecs[8]->set(i, Util::createMinute(rand()%1440));
// 		columnVecs[9]->set(i, Util::createDateTime(rand()%INT_MAX));
// 		columnVecs[10]->set(i, Util::createSecond(rand()%86400));
// 		columnVecs[11]->set(i, Util::createTimestamp(rand()%LLONG_MAX));
// 		columnVecs[12]->set(i, Util::createNanoTime(rand()%LLONG_MAX));
// 		columnVecs[13]->set(i, Util::createNanoTimestamp(rand()%LLONG_MAX));
// 		columnVecs[14]->set(i, Util::createFloat(rand()/float(RAND_MAX)));
// 		columnVecs[15]->set(i, Util::createDouble(rand()/double(RAND_MAX)));
// 		columnVecs[16]->set(i, Util::createString("str"+to_string(i)));
// 		columnVecs[17]->set(i, Util::parseConstant(DT_UUID,"5d212a78-cc48-e3b1-4235-b4d91473ee87"));	
// 		columnVecs[18]->set(i, Util::parseConstant(DT_IP,"192.0.0."+to_string(rand()%255)));
// 		columnVecs[19]->set(i, Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec32"));
// 		columnVecs[20]->set(i, Util::createBlob("blob"+to_string(i)));
// 		columnVecs[21]->set(i, Util::createDateHour(rand()%INT_MAX));
// 		columnVecs[22]->set(i, Util::createDecimal32(scale32,rand()/float(RAND_MAX)));
// 		columnVecs[23]->set(i, Util::createDecimal64(scale64,rand()/double(RAND_MAX)));
// 	}
	// for (int j = 0; j < colNum; j++){
	// 	if(j == 3)
	// 		columnVecs[3]->set(rowNum-1, Util::createInt(rand()%INT_MAX));  //partition-column's value must be not null
	// 	else
	// 		columnVecs[j]->setNull(rowNum-1);
	// }
//     string dbName ="dfs://test_AutoFitTableAppender_appendAllDataTypesTopartitionedTable";
//     string tableName = "pt";
// 	string script = "dbName = \"dfs://test_AutoFitTableAppender_appendAllDataTypesTopartitionedTable\";"
// 			"if(exists(dbName)){dropDatabase(dbName)};"
// 			"db  = database(dbName, HASH,[STRING,1],,'TSDB');"
// 			"temp = table(100:0, take(`col,24)+string(take(0..23,24)), \
// 			[CHAR, BOOL, SHORT, INT, LONG, DATE, MONTH, TIME, MINUTE, DATETIME, SECOND, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, STRING, UUID, IPADDR, INT128, BLOB, DATEHOUR, DECIMAL32("+to_string(scale32)+"), DECIMAL64("+to_string(scale64)+")]);"
// 			"pt = db.createPartitionedTable(temp,`pt,`col16,,`col16);";
// 	conn.run(script);
// 	AutoFitTableAppender appender(dbName, tableName, conn);
// 	int res = appender.append(tab1);
// 	EXPECT_EQ(res, rowNum);

// 	// conn.upload("tab1",tab1);
// 	// string script3;
// 	// script3 += "st1 = select * from pt;";
// 	// script3 += "each(eqObj, tab1.values(), st1.values());";
// 	// ConstantSP result2 = conn.run(script3);
// 	// // cout<<conn.run("st1")->getString();
// 	// for (int i = 0; i<result2->size(); i++)
// 	// 	EXPECT_TRUE(result2->get(i)->getBool());
// }

TEST_F(AutoFitTableappenderTest,test_AutoFitTableAppender_convert_to_date){
	string script1;
	script1 += "login('admin', '123456');";
	script1 += "try{undef(`st1);undef(`st1, SHARED)}catch(ex){print ex};";
	script1 += "share table(100:0, `date1`date2`date3`date4, [DATE, DATE, DATE, DATE]) as st1;";
	conn.run(script1);
	AutoFitTableAppender appender("", "st1", conn);
	vector<string> colNames = { "date1", "date2", "date3", "date4" };
	vector<DATA_TYPE> colTypes = { DT_DATETIME, DT_TIMESTAMP, DT_NANOTIMESTAMP, DT_DATE };
	int colNum = 4, rowNum = 10;
	TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(tmp1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createDateTime(1969, 1, i + 1, 12, 30, 30));
		columnVecs[1]->set(i, Util::createTimestamp(1969, 1, i + 1, 12, 30, 30, 123));
		columnVecs[2]->set(i, Util::createNanoTimestamp(1969, 1, i + 1, 12, 30, 30, 123456789));
		columnVecs[3]->set(i, Util::createDate(1969, 1, i + 1));
	}
	int res = appender.append(tmp1);
	EXPECT_EQ(res, rowNum);
	string script2;
	script2 += "expected = table(1969.01.01+0..9 as date1, 1969.01.01+0..9 as date2, 1969.01.01+0..9 as date3, 1969.01.01+0..9 as date4);";
	script2 += "each(eqObj, st1.values(), expected.values());";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<4; i++)
		EXPECT_EQ(result->getInt(i), 1);

	TableSP tmp2 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs1;
	columnVecs1.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs1.emplace_back(tmp2->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs1[0]->set(i, Util::createDateTime(2012, 1, i + 1, 12, 30, 30));
		columnVecs1[1]->set(i, Util::createTimestamp(2012, 1, i + 1, 12, 30, 30, 123));
		columnVecs1[2]->set(i, Util::createNanoTimestamp(2012, 1, i + 1, 12, 30, 30, 123456789));
		columnVecs1[3]->set(i, Util::createDate(2012, 1, i + 1));
	}
	int res2 = appender.append(tmp2);
	EXPECT_EQ(res2, rowNum);
	string script3;
	script3 += "expected = table(2012.01.01+0..9 as date1, 2012.01.01+0..9 as date2, 2012.01.01+0..9 as date3, 2012.01.01+0..9 as date4);";
	script3 += "each(eqObj, (select * from st1 where date1>1970.01.01).values(), expected.values());";
	ConstantSP result2 = conn.run(script3);
	for (int i = 0; i<4; i++)
		EXPECT_EQ(result2->getInt(i), 1);
}

TEST_F(AutoFitTableappenderTest,test_AutoFitTableAppender_convert_to_month){
	string script1;
	script1 += "login('admin', '123456');";
	script1 += "try{undef(`st1);undef(`st1, SHARED)}catch(ex){print ex};";
	script1 += "share table(100:0, `month1`month2`month3`month4`month5, [MONTH, MONTH, MONTH, MONTH, MONTH]) as st1;";
	conn.run(script1);
	AutoFitTableAppender appender("", "st1", conn);
	vector<string> colNames = { "month1", "month2", "month3", "month4", "month5" };
	vector<DATA_TYPE> colTypes = { DT_DATETIME, DT_TIMESTAMP, DT_NANOTIMESTAMP, DT_DATE, DT_MONTH };
	int colNum = 5, rowNum = 10;
	TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(tmp1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createDateTime(1969, i + 1, 1, 12, 30, 30));
		columnVecs[1]->set(i, Util::createTimestamp(1969, i + 1, 1, 12, 30, 30, 123));
		columnVecs[2]->set(i, Util::createNanoTimestamp(1969, i + 1, 1, 12, 30, 30, 123456789));
		columnVecs[3]->set(i, Util::createDate(1969, i + 1, 1));
		columnVecs[4]->set(i, Util::createMonth(1969, i + 1));
	}
	int res1 = appender.append(tmp1);
	EXPECT_EQ(res1, rowNum);
	string script2;
	script2 += "expected = table(1969.01M+0..9 as month1, 1969.01M+0..9 as month2, 1969.01M+0..9 as month3, 1969.01M+0..9 as month4, 1969.01M+0..9 as month5);";
	script2 += "each(eqObj, st1.values(), expected.values());";
	ConstantSP result1 = conn.run(script2);
	for (int i = 0; i<5; i++)
		EXPECT_EQ(result1->getInt(i), 1);

	TableSP tmp2 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs2;
	columnVecs2.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs2.emplace_back(tmp2->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs2[0]->set(i, Util::createDateTime(2000, i + 1, 1, 12, 30, 30));
		columnVecs2[1]->set(i, Util::createTimestamp(2000, i + 1, 1, 12, 30, 30, 123));
		columnVecs2[2]->set(i, Util::createNanoTimestamp(2000, i + 1, 1, 12, 30, 30, 123456789));
		columnVecs2[3]->set(i, Util::createDate(2000, i + 1, 1));
		columnVecs2[4]->set(i, Util::createMonth(2000, i + 1));
	}
	int res2 = appender.append(tmp2);
	EXPECT_EQ(res2, rowNum);
	string script3;
	script3 += "expected = table(2000.01M+0..9 as month1, 2000.01M+0..9 as month2, 2000.01M+0..9 as month3, 2000.01M+0..9 as month4, 2000.01M+0..9 as month5);";
	script3 += "each(eqObj,(select * from st1 where month1>1970.01M).values(), expected.values());";
	ConstantSP result2 = conn.run(script3);
	for (int i = 0; i<5; i++)
		EXPECT_EQ(result2->getInt(i), 1);
}

TEST_F(AutoFitTableappenderTest,test_AutoFitTableAppender_convert_to_datetime){
	string script1;
	script1 += "login('admin', '123456');";
	script1 += "try{undef(`st1);undef(`st1, SHARED)}catch(ex){print ex};";
	script1 += "share table(100:0, `col1`col2`col3`col4, [DATETIME, DATETIME, DATETIME, DATETIME]) as st1;";
	conn.run(script1);
	AutoFitTableAppender appender("", "st1", conn);
	vector<string> colNames = { "col1", "col2", "col3", "col4" };
	vector<DATA_TYPE> colTypes = { DT_DATETIME, DT_TIMESTAMP, DT_NANOTIMESTAMP, DT_DATE };
	int colNum = 4, rowNum = 10;
	TableSP tmp = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(tmp->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createDateTime(1969, 1, 1, 12, 30, i));
		columnVecs[1]->set(i, Util::createTimestamp(1969, 1, 1, 12, 30, i, 123));
		columnVecs[2]->set(i, Util::createNanoTimestamp(1969, 1, 1, 12, 30, i, 123456789));
		columnVecs[3]->set(i, Util::createDate(1969, 1, 1));
	}
	int res1 = appender.append(tmp);
	EXPECT_EQ(res1, rowNum);
	string script2;
	script2 += "expected = table(1969.01.01T12:30:00+0..9 as col1, 1969.01.01T12:30:00+0..9 as col2, 1969.01.01T12:30:00+0..9 as col3, take(1969.01.01T00:00:00, 10) as col4);";
	script2 += "each(eqObj, st1.values(), expected.values());";
	ConstantSP result1 = conn.run(script2);
	for (int i = 0; i<colNum; i++)
		EXPECT_EQ(result1->getInt(i), 1);

	TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs1;
	columnVecs1.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs1.emplace_back(tmp1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs1[0]->set(i, Util::createDateTime(2001, 1, 1, 12, 30, i));
		columnVecs1[1]->set(i, Util::createTimestamp(2001, 1, 1, 12, 30, i, 123));
		columnVecs1[2]->set(i, Util::createNanoTimestamp(2001, 1, 1, 12, 30, i, 123456789));
		columnVecs1[3]->set(i, Util::createDate(2001, 1, 1));
	}
	int res2 = appender.append(tmp1);
	EXPECT_EQ(res2, rowNum);
	string script3;
	script3 += "expected = table(2001.01.01T12:30:00+0..9 as col1, 2001.01.01T12:30:00+0..9 as col2, 2001.01.01T12:30:00+0..9 as col3, take(2001.01.01T00:00:00, 10) as col4);";
	script3 += "each(eqObj, (select * from st1 where col1>1970.01.01T00:00:00).values(), expected.values());";
	ConstantSP result2 = conn.run(script3);
	for (int i = 0; i<colNum; i++)
		EXPECT_EQ(result2->getInt(i), 1);
}

TEST_F(AutoFitTableappenderTest,test_AutoFitTableAppender_convert_to_timestamp){
	string script1;
	script1 += "login('admin', '123456');";
	script1 += "try{undef(`st1);undef(`st1, SHARED)}catch(ex){print ex};";
	script1 += "share table(100:0, `col1`col2`col3`col4`col5, [TIMESTAMP, TIMESTAMP, TIMESTAMP, TIMESTAMP,TIMESTAMP]) as st1;";
	conn.run(script1);
	AutoFitTableAppender appender("", "st1", conn);
	vector<string> colNames = { "col1", "col2", "col3", "col4", "col5" };
	vector<DATA_TYPE> colTypes = { DT_DATETIME, DT_TIMESTAMP, DT_NANOTIMESTAMP, DT_NANOTIMESTAMP, DT_DATE };
	int colNum = 5, rowNum = 10;
	TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(tmp1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createDateTime(1969, 1, 1, 12, 30, i));
		columnVecs[1]->set(i, Util::createTimestamp(1969, 1, 1, 12, 30, i, 123));
		columnVecs[2]->set(i, Util::createNanoTimestamp(1969, 1, 1, 12, 30, i, 123456789));
		columnVecs[3]->set(i, Util::createNanoTimestamp(1960, 1, 1, 12, 30, 10, 8000000));
		columnVecs[4]->set(i, Util::createDate(1969, 1, 1));
	}
	int res = appender.append(tmp1);
	EXPECT_EQ(res, rowNum);
	string script2;
	script2 += "expected = table(temporalAdd(1969.01.01T12:30:00.000, 0..9, \"s\") as col1, temporalAdd(1969.01.01T12:30:00.123, 0..9, \"s\") as col2, temporalAdd(1969.01.01T12:30:00.123, 0..9, \"s\") as col3, take(1960.01.01T12:30:10.008, 10) as col4, take(1969.01.01T00:00:00.000, 10) as col5);";
	script2 += "each(eqObj, st1.values(), expected.values());";
	ConstantSP result1 = conn.run(script2);
	for (int i = 0; i<colNum; i++)
		EXPECT_EQ(result1->getInt(i), 1);

	TableSP tmp2 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs1;
	columnVecs1.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs1.emplace_back(tmp2->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs1[0]->set(i, Util::createDateTime(2000, 1, 1, 12, 30, i));
		columnVecs1[1]->set(i, Util::createTimestamp(2000, 1, 1, 12, 30, i, 123));
		columnVecs1[2]->set(i, Util::createNanoTimestamp(2000, 1, 1, 12, 30, i, 123456789));
		columnVecs1[3]->set(i, Util::createNanoTimestamp(2000, 1, 1, 12, 30, 10, 8000000));
		columnVecs1[4]->set(i, Util::createDate(2000, 1, 1));
	}
	int res2 = appender.append(tmp2);
	EXPECT_EQ(res2, rowNum);
	string script3;
	script3 += "expected = table(temporalAdd(2000.01.01T12:30:00.000, 0..9, \"s\") as col1, temporalAdd(2000.01.01T12:30:00.123, 0..9, \"s\") as col2, temporalAdd(2000.01.01T12:30:00.123, 0..9, \"s\") as col3, take(2000.01.01T12:30:10.008, 10) as col4, take(2000.01.01T00:00:00.000, 10) as col5);";
	script3 += "each(eqObj, (select * from st1 where date(col1)>1970.01.01).values(), expected.values());";
	ConstantSP result2 = conn.run(script3);
	for (int i = 0; i<colNum; i++)
		EXPECT_EQ(result2->getInt(i), 1);	
}

TEST_F(AutoFitTableappenderTest,test_AutoFitTableAppender_convert_to_nanotimestamp){
	string script1;
	script1 += "login('admin', '123456');";
	script1 += "try{undef(`st1);undef(`st1, SHARED)}catch(ex){print ex};";
	script1 += "share table(100:0, `col1`col2`col3`col4, [NANOTIMESTAMP, NANOTIMESTAMP, NANOTIMESTAMP, NANOTIMESTAMP]) as st1;";
	conn.run(script1);
	AutoFitTableAppender appender("", "st1", conn);
	vector<string> colNames = { "col1", "col2", "col3", "col4" };
	vector<DATA_TYPE> colTypes = { DT_DATETIME, DT_TIMESTAMP, DT_NANOTIMESTAMP, DT_DATE };
	int colNum = 4, rowNum = 10;
	TableSP tmp = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(tmp->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createDateTime(1969, 1, 1, 12, 30, i));
		columnVecs[1]->set(i, Util::createTimestamp(1969, 1, 1, 12, 30, i, 123));
		columnVecs[2]->set(i, Util::createNanoTimestamp(1969, 1, 1, 12, 30, i, 123456789));
		columnVecs[3]->set(i, Util::createDate(1969, 1, 1));
	}
	int res1 = appender.append(tmp);
	EXPECT_EQ(res1, rowNum);
	string script2;
	script2 += "expected = table(temporalAdd(1969.01.01T12:30:00.000000000, 0..9, \"s\") as col1, temporalAdd(1969.01.01T12:30:00.123000000, 0..9, \"s\") as col2, temporalAdd(1969.01.01T12:30:00.123456789, 0..9, \"s\") as col3, take(1969.01.01T00:00:00.000000000, 10) as col4);";
	script2 += "each(eqObj, st1.values(), expected.values());";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<colNum; i++)
		EXPECT_EQ(result->getInt(i), 1);

	TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs1;
	columnVecs1.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs1.emplace_back(tmp1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs1[0]->set(i, Util::createDateTime(2012, 1, 1, 12, 30, i));
		columnVecs1[1]->set(i, Util::createTimestamp(2012, 1, 1, 12, 30, i, 123));
		columnVecs1[2]->set(i, Util::createNanoTimestamp(2012, 1, 1, 12, 30, i, 123456789));
		columnVecs1[3]->set(i, Util::createDate(2012, 1, 1));
	}
	int res2 = appender.append(tmp1);
	EXPECT_EQ(res2, rowNum);
	string script3;
	script3 += "expected = table(temporalAdd(2012.01.01T12:30:00.000000000, 0..9, \"s\") as col1, temporalAdd(2012.01.01T12:30:00.123000000, 0..9, \"s\") as col2, temporalAdd(2012.01.01T12:30:00.123456789, 0..9, \"s\") as col3, take(2012.01.01T00:00:00.000000000, 10) as col4);";
	script3 += "each(eqObj, (select * from st1 where col1>=2012.01.01T12:30:00.000000000).values(), expected.values());";
	ConstantSP result2 = conn.run(script3);
	for (int i = 0; i<colNum; i++)
		EXPECT_EQ(result2->getInt(i), 1);	
}

TEST_F(AutoFitTableappenderTest,test_AutoFitTableAppender_convert_to_minute){
	string script1;
	script1 += "login('admin', '123456');";
	script1 += "try{undef(`st1);undef(`st1, SHARED)}catch(ex){print ex};";
	script1 += "share table(100:0, `col1`col2`col3`col4`col5`col6`col7, [MINUTE, MINUTE, MINUTE, MINUTE, MINUTE, MINUTE, MINUTE]) as st1;";
	conn.run(script1);
	AutoFitTableAppender appender("", "st1", conn);
	vector<string> colNames = { "col1", "col2", "col3", "col4", "col5", "col6", "col7" };
	vector<DATA_TYPE> colTypes = { DT_DATETIME, DT_TIMESTAMP, DT_NANOTIMESTAMP, DT_TIME, DT_SECOND, DT_MINUTE, DT_NANOTIME };
	int colNum = 7, rowNum = 10;
	TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(tmp1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createDateTime(1969, 1, 1, 12, i, 30));
		columnVecs[1]->set(i, Util::createTimestamp(1969, 1, 1, 12, i, 30, 123));
		columnVecs[2]->set(i, Util::createNanoTimestamp(1969, 1, 1, 12, i, 30, 123456789));
		columnVecs[3]->set(i, Util::createTime(12, i, 30, 123));
		columnVecs[4]->set(i, Util::createSecond(12, i, 30));
		columnVecs[5]->set(i, Util::createMinute(12, i));
		columnVecs[6]->set(i, Util::createNanoTime(12, i, 30, 123456789));
	}
	int res1 = appender.append(tmp1);
	EXPECT_EQ(res1, rowNum);
	string script2;
	script2 += "expected = table(12:00m+0..9 as col1, 12:00m+0..9 as col2, 12:00m+0..9 as col3, 12:00m+0..9 as col4, 12:00m+0..9 as col5, 12:00m+0..9 as col6, 12:00m+0..9 as col7);";
	script2 += "each(eqObj, st1.values(), expected.values());";
	ConstantSP result1 = conn.run(script2);
	for (int i = 0; i<colNum; i++)
		EXPECT_EQ(result1->getInt(i), 1);

	TableSP tmp2 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs1;
	columnVecs1.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs1.emplace_back(tmp2->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs1[0]->set(i, Util::createDateTime(2000, 1, 1, 12, i + 10, 30));
		columnVecs1[1]->set(i, Util::createTimestamp(2000, 1, 1, 12, i + 10, 30, 123));
		columnVecs1[2]->set(i, Util::createNanoTimestamp(2000, 1, 1, 12, i + 10, 30, 123456789));
		columnVecs1[3]->set(i, Util::createTime(12, i + 10, 30, 123));
		columnVecs1[4]->set(i, Util::createSecond(12, i + 10, 30));
		columnVecs1[5]->set(i, Util::createMinute(12, i + 10));
		columnVecs1[6]->set(i, Util::createNanoTime(12, i + 10, 30, 123456789));
	}
	int res2 = appender.append(tmp2);
	EXPECT_EQ(res2, rowNum);
	string script3;
	script3 += "expected = table(12:10m+0..9 as col1, 12:10m+0..9 as col2, 12:10m+0..9 as col3, 12:10m+0..9 as col4, 12:10m+0..9 as col5, 12:10m+0..9 as col6, 12:10m+0..9 as col7);";
	script3 += "each(eqObj, (select * from st1 where col1>=12:10m).values(), expected.values());";
	ConstantSP result2 = conn.run(script3);
	for (int i = 0; i<colNum; i++)
		EXPECT_EQ(result2->getInt(i), 1);	
}

TEST_F(AutoFitTableappenderTest,test_AutoFitTableAppender_convert_to_time){
	string script1;
	script1 += "login('admin', '123456');";
	script1 += "try{undef(`st1);undef(`st1, SHARED)}catch(ex){print ex};";
	script1 += "share table(100:0, `col1`col2`col3`col4`col5`col6`col7, [TIME, TIME, TIME, TIME, TIME, TIME, TIME]) as st1;";
	conn.run(script1);
	AutoFitTableAppender appender("", "st1", conn);
	vector<string> colNames = { "col1", "col2", "col3", "col4", "col5", "col6", "col7" };
	vector<DATA_TYPE> colTypes = { DT_DATETIME, DT_TIMESTAMP, DT_NANOTIMESTAMP, DT_TIME, DT_SECOND, DT_MINUTE, DT_NANOTIME };
	int colNum = 7, rowNum = 10;
	TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(tmp1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createDateTime(1969, 1, 1, 12, i, 30));
		columnVecs[1]->set(i, Util::createTimestamp(1969, 1, 1, 12, i, 30, 123));
		columnVecs[2]->set(i, Util::createNanoTimestamp(1969, 1, 1, 12, i, 30, 123456789));
		columnVecs[3]->set(i, Util::createTime(12, i, 30, 123));
		columnVecs[4]->set(i, Util::createSecond(12, i, 30));
		columnVecs[5]->set(i, Util::createMinute(12, i));
		columnVecs[6]->set(i, Util::createNanoTime(12, i, 30, 123456789));
	}
	int res1 = appender.append(tmp1);
	EXPECT_EQ(res1, rowNum);
	string script2;
	script2 += "expected = table(temporalAdd(12:00:30.000, 0..9, \"m\") as col1, temporalAdd(12:00:30.123, 0..9, \"m\") as col2, temporalAdd(12:00:30.123, 0..9, \"m\") as col3, temporalAdd(12:00:30.123, 0..9, \"m\") as col4, temporalAdd(12:00:30.000, 0..9, \"m\") as col5, temporalAdd(12:00:00.000, 0..9, \"m\") as col6, temporalAdd(12:00:30.123, 0..9, \"m\") as col7);";
	script2 += "each(eqObj, st1.values(), expected.values());";
	ConstantSP result1 = conn.run(script2);
	for (int i = 0; i<colNum; i++)
		EXPECT_EQ(result1->getInt(i), 1);

	TableSP tmp2 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs1;
	columnVecs1.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs1.emplace_back(tmp2->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs1[0]->set(i, Util::createDateTime(2012, 1, 1, 12, i + 10, 30));
		columnVecs1[1]->set(i, Util::createTimestamp(2012, 1, 1, 12, i + 10, 30, 123));
		columnVecs1[2]->set(i, Util::createNanoTimestamp(2012, 1, 1, 12, i + 10, 30, 123456789));
		columnVecs1[3]->set(i, Util::createTime(12, i + 10, 30, 123));
		columnVecs1[4]->set(i, Util::createSecond(12, i + 10, 30));
		columnVecs1[5]->set(i, Util::createMinute(12, i + 10));
		columnVecs1[6]->set(i, Util::createNanoTime(12, i + 10, 30, 123456789));
	}
	int res2 = appender.append(tmp2);
	EXPECT_EQ(res2, rowNum);
	string script3;
	script3 += "expected = table(temporalAdd(12:10:30.000, 0..9, \"m\") as col1, temporalAdd(12:10:30.123, 0..9, \"m\") as col2, temporalAdd(12:10:30.123, 0..9, \"m\") as col3, temporalAdd(12:10:30.123, 0..9, \"m\") as col4, temporalAdd(12:10:30.000, 0..9, \"m\") as col5, temporalAdd(12:10:00.000, 0..9, \"m\") as col6, temporalAdd(12:10:30.123, 0..9, \"m\") as col7);";
	script3 += "each(eqObj, (select * from st1 where col1>=12:10:30.000).values(), expected.values());";
	ConstantSP result2 = conn.run(script3);
	for (int i = 0; i<colNum; i++)
		EXPECT_EQ(result2->getInt(i), 1);	
}

TEST_F(AutoFitTableappenderTest,test_AutoFitTableAppender_convert_to_second){
	string script1;
	script1 += "login('admin', '123456');";
	script1 += "try{undef(`st1);undef(`st1, SHARED)}catch(ex){print ex};";
	script1 += "share table(100:0, `col1`col2`col3`col4`col5`col6`col7, [SECOND, SECOND, SECOND, SECOND, SECOND, SECOND, SECOND]) as st1;";
	conn.run(script1);
	AutoFitTableAppender appender("", "st1", conn);
	vector<string> colNames = { "col1", "col2", "col3", "col4", "col5", "col6", "col7" };
	vector<DATA_TYPE> colTypes = { DT_DATETIME, DT_TIMESTAMP, DT_NANOTIMESTAMP, DT_TIME, DT_SECOND, DT_MINUTE, DT_NANOTIME };
	int colNum = 7, rowNum = 10;
	TableSP tmp = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(tmp->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createDateTime(1969, 1, 1, 12, i, 30));
		columnVecs[1]->set(i, Util::createTimestamp(1969, 1, 1, 12, i, 30, 123));
		columnVecs[2]->set(i, Util::createNanoTimestamp(1969, 1, 1, 12, i, 30, 123456789));
		columnVecs[3]->set(i, Util::createTime(12, i, 30, 123));
		columnVecs[4]->set(i, Util::createSecond(12, i, 30));
		columnVecs[5]->set(i, Util::createMinute(12, i));
		columnVecs[6]->set(i, Util::createNanoTime(12, i, 30, 123456789));
	}
	int res = appender.append(tmp);
	EXPECT_EQ(res, rowNum);
	string script2;
	script2 += "expected = table(temporalAdd(12:00:30, 0..9, \"m\") as col1, temporalAdd(12:00:30, 0..9, \"m\") as col2, temporalAdd(12:00:30, 0..9, \"m\") as col3, temporalAdd(12:00:30, 0..9, \"m\") as col4, temporalAdd(12:00:30, 0..9, \"m\") as col5, temporalAdd(12:00:00, 0..9, \"m\") as col6, temporalAdd(12:00:30, 0..9, \"m\") as col7);";
	script2 += "each(eqObj, st1.values(), expected.values());";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<colNum; i++)
		EXPECT_EQ(result->getInt(i), 1);

	TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs1;
	columnVecs1.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs1.emplace_back(tmp1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs1[0]->set(i, Util::createDateTime(2012, 1, 1, 12, i + 10, 30));
		columnVecs1[1]->set(i, Util::createTimestamp(2012, 1, 1, 12, i + 10, 30, 123));
		columnVecs1[2]->set(i, Util::createNanoTimestamp(2012, 1, 1, 12, i + 10, 30, 123456789));
		columnVecs1[3]->set(i, Util::createTime(12, i + 10, 30, 123));
		columnVecs1[4]->set(i, Util::createSecond(12, i + 10, 30));
		columnVecs1[5]->set(i, Util::createMinute(12, i + 10));
		columnVecs1[6]->set(i, Util::createNanoTime(12, i + 10, 30, 123456789));
	}
	int res2 = appender.append(tmp1);
	EXPECT_EQ(res2, rowNum);
	string script3;
	script3 += "expected = table(temporalAdd(12:10:30, 0..9, \"m\") as col1, temporalAdd(12:10:30, 0..9, \"m\") as col2, temporalAdd(12:10:30, 0..9, \"m\") as col3, temporalAdd(12:10:30, 0..9, \"m\") as col4, temporalAdd(12:10:30, 0..9, \"m\") as col5, temporalAdd(12:10:00, 0..9, \"m\") as col6, temporalAdd(12:10:30, 0..9, \"m\") as col7);";
	script3 += "each(eqObj, (select * from st1 where col1>=12:10:30).values(), expected.values());";
	ConstantSP result2 = conn.run(script3);
	for (int i = 0; i<colNum; i++)
		EXPECT_EQ(result2->getInt(i), 1);	
}

TEST_F(AutoFitTableappenderTest,test_AutoFitTableAppender_convert_to_nanotime){
	string script1;
	script1 += "login('admin', '123456');";
	script1 += "try{undef(`st1);undef(`st1, SHARED)}catch(ex){print ex};";
	script1 += "share table(100:0, `col1`col2`col3`col4`col5`col6`col7, [NANOTIME, NANOTIME, NANOTIME, NANOTIME, NANOTIME, NANOTIME, NANOTIME]) as st1;";
	conn.run(script1);
	AutoFitTableAppender appender("", "st1", conn);
	vector<string> colNames = { "col1", "col2", "col3", "col4", "col5", "col6", "col7" };
	vector<DATA_TYPE> colTypes = { DT_DATETIME, DT_TIMESTAMP, DT_NANOTIMESTAMP, DT_TIME, DT_SECOND, DT_MINUTE, DT_NANOTIME };
	int colNum = 7, rowNum = 10;
	TableSP tmp = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(tmp->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createDateTime(1969, 1, 1, 12, i, 30));
		columnVecs[1]->set(i, Util::createTimestamp(1969, 1, 1, 12, i, 30, 123));
		columnVecs[2]->set(i, Util::createNanoTimestamp(1969, 1, 1, 12, i, 30, 123456789));
		columnVecs[3]->set(i, Util::createTime(12, i, 30, 123));
		columnVecs[4]->set(i, Util::createSecond(12, i, 30));
		columnVecs[5]->set(i, Util::createMinute(12, i));
		columnVecs[6]->set(i, Util::createNanoTime(12, i, 30, 123456789));
	}
	int res = appender.append(tmp);
	EXPECT_EQ(res, rowNum);
	string script2;
	script2 += "expected = table(temporalAdd(12:00:30.000000000, 0..9, \"m\") as col1, temporalAdd(12:00:30.123000000, 0..9, \"m\") as col2, temporalAdd(12:00:30.123456789, 0..9, \"m\") as col3, temporalAdd(12:00:30.123000000, 0..9, \"m\") as col4, temporalAdd(12:00:30.000000000, 0..9, \"m\") as col5, temporalAdd(12:00:00.000000000, 0..9, \"m\") as col6, temporalAdd(12:00:30.123456789, 0..9, \"m\") as col7);";
	script2 += "each(eqObj, st1.values(), expected.values());";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<colNum; i++)
		EXPECT_EQ(result->getInt(i), 1);

	TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs1;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs1.emplace_back(tmp1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs1[0]->set(i, Util::createDateTime(2012, 1, 1, 12, i + 10, 30));
		columnVecs1[1]->set(i, Util::createTimestamp(2012, 1, 1, 12, i + 10, 30, 123));
		columnVecs1[2]->set(i, Util::createNanoTimestamp(2012, 1, 1, 12, i + 10, 30, 123456789));
		columnVecs1[3]->set(i, Util::createTime(12, i + 10, 30, 123));
		columnVecs1[4]->set(i, Util::createSecond(12, i + 10, 30));
		columnVecs1[5]->set(i, Util::createMinute(12, i + 10));
		columnVecs1[6]->set(i, Util::createNanoTime(12, i + 10, 30, 123456789));
	}
	int res2 = appender.append(tmp1);
	EXPECT_EQ(res2, rowNum);
	string script3;
	script3 += "expected = table(temporalAdd(12:10:30.000000000, 0..9, \"m\") as col1, temporalAdd(12:10:30.123000000, 0..9, \"m\") as col2, temporalAdd(12:10:30.123456789, 0..9, \"m\") as col3, temporalAdd(12:10:30.123000000, 0..9, \"m\") as col4, temporalAdd(12:10:30.000000000, 0..9, \"m\") as col5, temporalAdd(12:10:00.000000000, 0..9, \"m\") as col6, temporalAdd(12:10:30.123456789, 0..9, \"m\") as col7);";
	script3 += "each(eqObj, (select * from st1 where col1>=12:10:30.000000000).values(), expected.values());";
	ConstantSP result2 = conn.run(script3);
	for (int i = 0; i<colNum; i++)
		EXPECT_EQ(result2->getInt(i), 1);	
}

TEST_F(AutoFitTableappenderTest,test_AutoFitTableAppender_dfs_table){
	//convert datetime to date
	string script1;
	script1 += "login('admin', '123456');";
	script1 += "dbPath='dfs://test_autoFitTableAppender';";
	script1 += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script1 += "db=database(dbPath, VALUE, 2012.01.01..2012.01.10);";
	script1 += "t=table(1:0, [`date], [DATE]);";
	script1 += "if(existsTable(dbPath,`pt)){dropTable(db,`pt)};";
	script1 += "pt=db.createPartitionedTable(t, `pt, `date);";
	conn.run(script1);
	AutoFitTableAppender appender("dfs://test_autoFitTableAppender", "pt", conn);
	vector<string> colNames = { "date" };
	vector<DATA_TYPE> colTypes = { DT_DATETIME };
	int colNum = 1, rowNum = 10;
	TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(tmp1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createDateTime(2012, 1, i + 1, 12, 30, 30));
	}
	// TableSP res_tab=conn.run("select count(*) from pt");
	// cout<< res_tab->rows()<<endl;
	int res = appender.append(tmp1);
	EXPECT_EQ(res, rowNum);
	string script;
	script += "expected = table(temporalAdd(2012.01.01, 0..9, 'd') as date);";
	script += "each(eqObj, (select * from pt order by date).values(), (select * from expected order by date).values());";
	ConstantSP result = conn.run(script);
	EXPECT_EQ(result->getInt(0), 1);	
}

TEST_F(AutoFitTableappenderTest,test_AutoFitTableAppender_convert_to_datehour){
	string script1;
	script1 += "login('admin', '123456');";
	script1 += "try{undef(`st1);undef(`st1, SHARED)}catch(ex){print ex};";
	script1 += "share table(100:0, `col1`col2`col3`col4`col5, [DATEHOUR, DATEHOUR , DATEHOUR , DATEHOUR, DATEHOUR]) as st1;";
	conn.run(script1);
	AutoFitTableAppender appender("", "st1", conn);
	vector<string> colNames = { "col1", "col2", "col3", "col4","col5" };
	vector<DATA_TYPE> colTypes = { DT_DATE,DT_DATEHOUR, DT_DATETIME,DT_TIMESTAMP, DT_NANOTIMESTAMP, };
	int colNum = 5, rowNum = 10;
	TableSP tmp = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(tmp->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createDate(1969, 1, 1));
		columnVecs[1]->set(i, Util::createDateHour(1969, 1, 1, i + 1));
		columnVecs[2]->set(i, Util::createDateTime(1969, 1, 1, i + 1, 30, 30));
		columnVecs[3]->set(i, Util::createTimestamp(1969, 1, 1, i + 1, 30, 30, 123));
		columnVecs[4]->set(i, Util::createNanoTimestamp(1969, 1, 1, i + 1, 30, 30, 123456789));

	}
	int res1 = appender.append(tmp);
	EXPECT_EQ(res1, rowNum);
	string script2;
	script2 += "expected =table(take(datehour(1969.01.01T00:00:00),10) as col1, datehour(1969.01.01T01:00:00)+0..9 as col2, datehour(1969.01.01T01:00:00)+0..9 as col3, datehour(1969.01.01T01:00:00)+0..9 as col4, datehour(1969.01.01T01:00:00)+0..9 as col5  );";
	script2 += "each(eqObj, st1.values(), expected.values());";
	ConstantSP result1 = conn.run(script2);


	for (int i = 0; i<colNum; i++)
		EXPECT_EQ(result1->getInt(i), 1);

	TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs1;
	columnVecs1.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs1.emplace_back(tmp1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs1[0]->set(i, Util::createDate(2001, 1, 1));
		columnVecs1[1]->set(i, Util::createDateHour(2001, 1, 1, i + 1));
		columnVecs1[2]->set(i, Util::createDateTime(2001, 1, 1, i + 1, 30, 30));
		columnVecs1[3]->set(i, Util::createTimestamp(2001, 1, 1, i + 1, 30, 30, 123));
		columnVecs1[4]->set(i, Util::createNanoTimestamp(2001, 1, 1, i + 1, 30, 30, 123456789));
	}
	int res2 = appender.append(tmp1);
	EXPECT_EQ(res2, rowNum);
	string script3;

	script3 += "expected =table(take(datehour(2001.01.01T00:00:00),10) as col1, datehour(2001.01.01T01:00:00)+0..9 as col2, datehour(2001.01.01T01:00:00)+0..9 as col3, datehour(2001.01.01T01:00:00)+0..9 as col4, datehour(2001.01.01T01:00:00)+0..9 as col5  );";
	script3 += "each(eqObj, (select * from st1 where col1>datehour(1970.01.01T00:00:00)).values(), expected.values());";
	ConstantSP result2 = conn.run(script3);
	for (int i = 0; i<colNum; i++)
		EXPECT_EQ(result2->getInt(i), 1);	
}


TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withIntArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_AutoFitTableAppender_withIntArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withIntArrayVectorToPartitionTableRangeType\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    
    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withIntArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_AutoFitTableAppender_withIntArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withIntArrayVectorNullToPartitionTableRangeType\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withIntArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_AutoFitTableAppender_withIntArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withIntArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withCharArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_AutoFitTableAppender_withCharArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withCharArrayVectorToPartitionTableRangeType\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withCharArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_AutoFitTableAppender_withCharArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withCharArrayVectorNullToPartitionTableRangeType\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withCharArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_AutoFitTableAppender_withCharArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withCharArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withFloatArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_AutoFitTableAppender_withFloatArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withFloatArrayVectorToPartitionTableRangeType\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withFloatArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_AutoFitTableAppender_withFloatArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withFloatArrayVectorNullToPartitionTableRangeType\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withFloatArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_AutoFitTableAppender_withFloatArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withFloatArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withDateArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_AutoFitTableAppender_withDateArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withDateArrayVectorToPartitionTableRangeType\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withDateArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_AutoFitTableAppender_withDateArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withDateArrayVectorNullToPartitionTableRangeType\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withDateArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_AutoFitTableAppender_withDateArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withDateArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withMonthArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_AutoFitTableAppender_withMonthArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withMonthArrayVectorToPartitionTableRangeType\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withMonthArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_AutoFitTableAppender_withMonthArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withMonthArrayVectorNullToPartitionTableRangeType\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withMonthArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_AutoFitTableAppender_withMonthArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withMonthArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withTimeArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_AutoFitTableAppender_withTimeArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withTimeArrayVectorToPartitionTableRangeType\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withTimeArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_AutoFitTableAppender_withTimeArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withTimeArrayVectorNullToPartitionTableRangeType\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withTimeArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_AutoFitTableAppender_withTimeArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withTimeArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withSecondArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_AutoFitTableAppender_withSecondArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withSecondArrayVectorToPartitionTableRangeType\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withSecondArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_AutoFitTableAppender_withSecondArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withSecondArrayVectorNullToPartitionTableRangeType\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withSecondArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_AutoFitTableAppender_withSecondArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withSecondArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withDatehourArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_AutoFitTableAppender_withDatehourArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withDatehourArrayVectorToPartitionTableRangeType\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withDatehourArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_AutoFitTableAppender_withDatehourArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withDatehourArrayVectorNullToPartitionTableRangeType\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withDatehourArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_AutoFitTableAppender_withDatehourArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withDatehourArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withUuidArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_AutoFitTableAppender_withUuidArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withUuidArrayVectorToPartitionTableRangeType\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withUuidArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_AutoFitTableAppender_withUuidArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withUuidArrayVectorNullToPartitionTableRangeType\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableappenderTest, test_AutoFitTableAppender_withUuidArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_AutoFitTableAppender_withUuidArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableAppender_withUuidArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
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
	
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    AutoFitTableAppender appender(dbName, tableName, conn);
    appender.append(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}