class DolphinDBTest:public testing::Test
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

int64_t getTimeStampMs() {
	return Util::getEpochTime();
}

string genRandString(int maxSize) {
	string result;
	int size = rand() % maxSize;
	for (int i = 0;i < size;i++) {
		int r = rand() % alphas.size();
		result += alphas[r];
	}
	return result;
}

static string getSymbolVector(const string& name, int size)
{
	int kind = 50;
	int count = size / kind;

	string result;
	char temp[200];
	result += name;
	sprintf(temp, "=symbol(array(STRING,%d,%d,`0));", count, count);
	result += temp;
	for (int i = 1;i<kind;i++) {
		sprintf(temp, ".append!(symbol(array(STRING,%d,%d,`%d)));", count, count, i);
		result += name;
		result += string(temp);
	}

	return result;
}

TEST(DolphinDBDataTypeTest,testDataTypeWithoutConnect){
        VectorSP arrayVector = Util::createArrayVector(DT_DATETIME_ARRAY, 0, 100);
        for (int i = 0; i < 10; i++) {
            VectorSP time = Util::createVector(DT_DATETIME, 5);
            for (int j = 0; j < 5; j++) {
                time->set(j, Util::createDateTime(j * 100000));
            }
            arrayVector->append(time);
        }
		cout<< arrayVector->getString()<<endl;

		ConstantSP intval= Util::createConstant(DT_INT);
		intval->setInt(1);
		EXPECT_EQ(intval->getInt(),1);

		ConstantSP boolval= Util::createConstant(DT_BOOL);
		boolval->setBool(1);
		EXPECT_EQ(boolval->getBool(),true);

		ConstantSP floatval= Util::createConstant(DT_FLOAT);
		floatval->setFloat(2.33);
		EXPECT_EQ(floatval->getFloat(),(float)2.33);

		ConstantSP longval= Util::createConstant(DT_LONG);
		longval->setLong(10000000);
		EXPECT_EQ(longval->getLong(),(long)10000000);

		ConstantSP stringval= Util::createConstant(DT_STRING);
		stringval->setString("134asd");
		EXPECT_EQ(stringval->getString(),"134asd");

		ConstantSP dateval= Util::createConstant(DT_DATE);
		dateval=Util::createDate(1);
		EXPECT_EQ(dateval->getString(),"1970.01.02");

		ConstantSP minuteval= Util::createMinute(1439);
		EXPECT_EQ(minuteval->getString(),"23:59m");

		ConstantSP nanotimestampval= Util::createNanoTimestamp((long long)100000000000000000);
		EXPECT_EQ(nanotimestampval->getString(),"1973.03.03T09:46:40.000000000");

		ConstantSP uuidval= Util::parseConstant(DT_UUID,"5d212a78-cc48-e3b1-4235-b4d91473ee87");
		EXPECT_EQ(uuidval->getString(),"5d212a78-cc48-e3b1-4235-b4d91473ee87");

		ConstantSP ipaddrval= Util::parseConstant(DT_IP,"192.168.0.16");
		EXPECT_EQ(ipaddrval->getString(),"192.168.0.16");

		vector<string> colname={"col1","col2","col3"};
		vector<DATA_TYPE> coltype={DT_INT,DT_BLOB, DT_SYMBOL};
		TableSP tableval= Util::createTable(colname,coltype,0,3);
		cout<< tableval->getString()<<endl;	
}

TEST_F(DolphinDBTest,testSymbol){
	vector<string> expectResults = { "XOM","y" };
	string script;
	script += "x=`XOM`y;y=symbol x;y;";
	ConstantSP result = conn.run(script);
	for (unsigned int i = 0;i < expectResults.size(); i++) {
		EXPECT_EQ(result->getString(i), expectResults[i]);
	}
}

TEST_F(DolphinDBTest,testSymbolBase){
	int64_t startTime, time;

	conn.run("v=symbol(string(1..2000000))");
	startTime = getTimeStampMs();
	conn.run("v");
	time = getTimeStampMs() - startTime;
	cout << "symbol vector: " << time << "ms" << endl;

	conn.run("undef(all)");
	conn.run(getSymbolVector("v", 2000000));
	startTime = getTimeStampMs();
	conn.run("v");
	time = getTimeStampMs() - startTime;
	cout << "symbol vector optimize:" << time << "ms" << endl;

	conn.run("undef(all)");
	conn.run("t=table(symbol(string(1..2000000)) as sym)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with one symbol vector:" << time << "ms" << endl;

	conn.run("undef(all)");
	conn.run(getSymbolVector("v", 2000000));
	conn.run("t=table(v as sym)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with one symbol vector optimize:" << time << "ms" << endl;

	conn.run("undef(all)");
	conn.run("t=table(symbol(string(1..2000000)) as sym,symbol(string(1..2000000)) as sym1,symbol(string(1..2000000)) as sym2,symbol(string(1..2000000)) as sym3,symbol(string(1..2000000)) as sym4,symbol(string(1..2000000)) as sym5,symbol(string(1..2000000)) as sym6,symbol(string(1..2000000)) as sym7,symbol(string(1..2000000)) as sym8,symbol(string(1..2000000)) as sym9)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with same symbol vectors:" << time << "ms" << endl;

	conn.run("undef(all)");
	conn.run("t=table(symbol(take(string(0..20000),2000000)) as sym,symbol(take(string(20000..40000),2000000)) as sym1,symbol(take(string(40000..60000),2000000)) as sym2,symbol(take(string(60000..80000),2000000)) as sym3,symbol(take(string(80000..100000),2000000)) as sym4,symbol(take(string(100000..120000),2000000)) as sym5,symbol(take(string(120000..140000),2000000)) as sym6,symbol(take(string(140000..160000),2000000)) as sym7,symbol(take(string(160000..180000),2000000)) as sym8,symbol(take(string(180000..200000),2000000)) as sym9)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with diff symbol vectors:" << time << "ms" << endl;

	//    conn.run("undef(all)");
	//    conn.run("m=symbol(string(1..2000000))$1000:2000");
	//    startTime = getTimeStampMs();
	//    conn.run("m");
	//    time = getTimeStampMs()-startTime;
	//    cout << "symbol matrix:" << time << "ms" << endl;

	conn.run("undef(all)");
	conn.run("d=dict(symbol(string(1..2000000)),symbol(string(1..2000000)))");
	startTime = getTimeStampMs();
	conn.run("d");
	time = getTimeStampMs() - startTime;
	cout << "symbol dict:" << time << "ms" << endl;

	conn.run("undef(all)");
	conn.run("s=set(symbol(string(1..2000000)))");
	startTime = getTimeStampMs();
	conn.run("s");
	time = getTimeStampMs() - startTime;
	cout << "symbol set:" << time << "ms" << endl;

	conn.run("undef(all)");
	conn.run("t=(symbol(string(1..2000000)),symbol(string(1..2000000)))");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "tuple symbol tuple:" << time << "ms" << endl;

	conn.run("undef(all)");
}

TEST_F(DolphinDBTest,testSymbolSmall){
	int64_t startTime, time;
	conn.run("v=symbol(string(1..200))");
	startTime = getTimeStampMs();
	conn.run("v");
	time = getTimeStampMs() - startTime;
	cout << "symbol vector:" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run(getSymbolVector("v", 200));
	startTime = getTimeStampMs();
	conn.run("v");
	time = getTimeStampMs() - startTime;
	cout << "symbol vector optimize:" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("t=table(symbol(string(1..200)) as sym)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with one symbol vector:" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run(getSymbolVector("v", 200));
	conn.run("t=table(v as sym)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with one symbol vector optimize:" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("t=table(symbol(string(1..200)) as sym,symbol(string(1..200)) as sym1,symbol(string(1..200)) as sym2,symbol(string(1..200)) as sym3,symbol(string(1..200)) as sym4,symbol(string(1..200)) as sym5,symbol(string(1..200)) as sym6,symbol(string(1..200)) as sym7,symbol(string(1..200)) as sym8,symbol(string(1..200)) as sym9)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with same symbol vectors:" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("t=table(symbol(take(string(0..20000),200)) as sym,symbol(take(string(20000..40000),200)) as sym1,symbol(take(string(40000..60000),200)) as sym2,symbol(take(string(60000..80000),200)) as sym3,symbol(take(string(80000..100000),200)) as sym4,symbol(take(string(100000..120000),200)) as sym5,symbol(take(string(120000..140000),200)) as sym6,symbol(take(string(140000..160000),200)) as sym7,symbol(take(string(160000..180000),200)) as sym8,symbol(take(string(180000..200000),200)) as sym9)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with diff symbol vectors:" << time << "ms" << endl;
	conn.run("undef(all)");

	//    conn.run("m =symbol(string(1..200))$10:20");
	//    startTime = getTimeStampMs();
	//    conn.run("m");
	//    time = getTimeStampMs()-startTime;
	//    cout << "symbol matrix:" << time << "ms" << endl;
	//    conn.run("undef(all)");

	conn.run("d=dict(symbol(string(1..200)),symbol(string(1..200)))");
	startTime = getTimeStampMs();
	conn.run("d");
	time = getTimeStampMs() - startTime;
	cout << "symbol dict:" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("s=set(symbol(string(1..200)))");
	startTime = getTimeStampMs();
	conn.run("s");
	time = getTimeStampMs() - startTime;
	cout << "symbol set:" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("t=(symbol(string(1..200)),symbol(string(1..200)))");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "tuple symbol tuple:" << time << "ms" << endl;
	conn.run("undef(all)");
}

TEST_F(DolphinDBTest,testSymbolNull){
	int64_t startTime, time;
	conn.run("v=take(symbol(`cy`fty``56e`f65dfyfv),2000000)");
	startTime = getTimeStampMs();
	conn.run("v");
	time = getTimeStampMs() - startTime;
	cout << "symbol vector:" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("t=table(take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with one symbol vector:" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("t=table(take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym1,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym2,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym3,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym4,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym5,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym6,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym7,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym8,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym9)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with same symbol vectors:" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("t=table(take(symbol(`cdwy`fty``56e`f652dfyfv),2000000) as sym,take(symbol(`cy`f8ty``56e`f65dfyfv),2000000) as sym1,take(symbol(`c2587y`fty``56e`f65dfyfv),2000000) as sym2,take(symbol(`cy````f65dfy4fv),2000000) as sym3,take(symbol(`cy```56e`f65dfgyfv),2000000) as sym4,take(symbol(`cy`fty``56e`12547),2000000) as sym5,take(symbol(`cy`fty``e`f65d728fyfv),2000000) as sym6,take(symbol(`cy`fty``56e`),2000000) as sym7,take(symbol(`cy`fty``56e`111),2000000) as sym8,take(symbol(`c412y`ft575y```f65dfyfv),2000000) as sym9)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with diff symbol vectors:" << time << "ms" << endl;
	conn.run("undef(all)");

	//    conn.run("m =take(symbol(`cy`fty```f65dfyfv),2000000)$1000:2000");
	//    startTime = getTimeStampMs();
	//    conn.run("m");
	//    time = getTimeStampMs()-startTime;
	//    cout << "symbol matrix:" << time << "ms" << endl;
	//    conn.run("undef(all)");

	conn.run("d=dict(take(symbol(`cy`fty``56e`f65dfyfv),2000000),take(symbol(`cy`fty``56e`f65dfyfv),2000000))");
	startTime = getTimeStampMs();
	conn.run("d");
	time = getTimeStampMs() - startTime;
	cout << "symbol dict:" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("s=set(take(symbol(`cy`fty``56e`f65dfyfv),2000000))");
	startTime = getTimeStampMs();
	conn.run("s");
	time = getTimeStampMs() - startTime;
	cout << "symbol set:" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("t=(take(symbol(`cy`fty``56e`f65dfyfv),2000000),take(symbol(`cy`fty``56e`f65dfyfv),2000000))");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "tuple symbol tuple:" << time << "ms" << endl;
	conn.run("undef(all)");
}



TEST_F(DolphinDBTest,testmixtimevectorUpload){
	VectorSP dates = Util::createVector(DT_ANY, 5, 100);
	dates->set(0, Util::createMonth(2016, 6));
	dates->set(1, Util::createDate(2016, 5, 16));
	dates->set(2, Util::createDateTime(2016, 6, 6, 6, 12, 12));
	dates->set(3, Util::createNanoTime(6, 28, 36, 00));
	dates->set(4, Util::createNanoTimestamp(2020, 8, 20, 2, 20, 20, 00));
	vector<ConstantSP> mixtimedata = { dates };
	vector<string> mixtimename = { "Mixtime" };
	conn.upload(mixtimename, mixtimedata);
}

TEST_F(DolphinDBTest,testFunctionDef){
	string script = "def funcAdd(a,b){return a + b};funcAdd(100,200);";
	ConstantSP result = conn.run(script);
	EXPECT_EQ(result->getString(), string("300"));
}


TEST_F(DolphinDBTest,testMatrix){
	vector<string> expectResults = { "{1,2}","{3,4}","{5,6}" };
	string script = "1..6$2:3";
	ConstantSP result = conn.run(script);
	for (unsigned int i = 0;i < expectResults.size(); i++) {
		EXPECT_EQ(result->getString(i), expectResults[i]);
	}
}

TEST_F(DolphinDBTest,testTable){
	string script;
	script += "n=20000\n";
	script += "syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n";
	script += "mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms,n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price, 1..n as number,rand(syms,n) as sym_2);\n";
	script += "select min(number) as minNum, max(number) as maxNum from mytrades";

	ConstantSP table = conn.run(script);
	EXPECT_EQ(table->getColumn(0)->getString(0), string("1"));
	EXPECT_EQ(table->getColumn(1)->getString(0), string("20000"));
}

TEST_F(DolphinDBTest,testDictionary){
	string script;
	script += "dict(1 2 3,symbol(`IBM`MSFT`GOOG))";
	DictionarySP dict = conn.run(script);

	EXPECT_EQ(dict->get(Util::createInt(1))->getString(), string("IBM"));
	EXPECT_EQ(dict->get(Util::createInt(2))->getString(), string("MSFT"));
	EXPECT_EQ(dict->get(Util::createInt(3))->getString(), string("GOOG"));
}


TEST_F(DolphinDBTest,testSet){
	string script;
	script += "x=set(4 5 5 2 3 11 11 11 6 6  6 6  6);x;";
	ConstantSP set = conn.run(script);
	EXPECT_EQ(set->size(), 6);
}

TEST_F(DolphinDBTest,testMemoryTable){
	string script;
	//simulation to generate data to be saved to the memory table
	VectorSP names = Util::createVector(DT_STRING, 5, 100);
	VectorSP dates = Util::createVector(DT_DATE, 5, 100);
	VectorSP prices = Util::createVector(DT_DOUBLE, 5, 100);
	for (int i = 0;i < 5;i++) {
		names->set(i, Util::createString("name_" + std::to_string(i)));
		dates->set(i, Util::createDate(2010, 1, i + 1));
		prices->set(i, Util::createDouble(i*i));
	}
	vector<string> allnames = { "names","dates","prices" };
	vector<ConstantSP> allcols = { names,dates,prices };
	conn.upload(allnames, allcols);//upload data to server
	script += "tglobal=table(names,dates,prices);";
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://demodb2\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db=database(dbPath,VALUE,2010.01.01..2010.01.30);";
	script += "pt=db.createPartitionedTable(tglobal,`pt,`dates);";
	script += "pt.append!(tglobal);";
	script += "dropPartition(db,2010.01.01,tableName=`pt);";
	//script += "dropTable(db,`tglobal);";
	//script += "dropDatabase(\"dfs://demodb2\");";
	//script += "existsDatabase(\"dfs://demodb2\");";
	//script += "insert into tglobal values(names,dates,prices);";
	script += "select * from pt;";
	TableSP table = conn.run(script);
	cout << table->getString() << endl;
}

TableSP createDemoTable() {
	vector<string> colNames = { "name","date","price" };
	vector<DATA_TYPE> colTypes = { DT_STRING,DT_DATE,DT_DOUBLE };
	int colNum = 3, rowNum = 3;
	ConstantSP table = Util::createTable(colNames, colTypes, rowNum, 100);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0;i < colNum;i++)
		columnVecs.emplace_back(table->getColumn(i));

	for (int i = 0;i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createString("name_" + std::to_string(i)));
		columnVecs[1]->set(i, Util::createDate(2010, 1, i + 1));
		columnVecs[2]->set(i, Util::createDouble(i*i));
	}
	return table;
}

TableSP createDemoTableSetStringWrong() {
	vector<string> colNames = { "name","date","price" };
	vector<DATA_TYPE> colTypes = { DT_STRING,DT_DATE,DT_DOUBLE };
	int colNum = 3, rowNum = 3;
	ConstantSP table = Util::createTable(colNames, colTypes, rowNum, 100);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0;i < colNum;i++)
		columnVecs.emplace_back(table->getColumn(i));

	for (int i = 0;i < rowNum; i++) {
		//wrong usage
		columnVecs[0]->setString("name_" + std::to_string(i));
		columnVecs[1]->set(i, Util::createDate(2010, 1, i + 1));
		columnVecs[2]->set(i, Util::createDouble(i*i));
	}
	return table;
}

TableSP createDemoTableSetString() {
	vector<string> colNames = { "name","date","price" };
	vector<DATA_TYPE> colTypes = { DT_STRING,DT_DATE,DT_DOUBLE };
	int colNum = 3, rowNum = 3;
	ConstantSP table = Util::createTable(colNames, colTypes, rowNum, 100);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0;i < colNum;i++)
		columnVecs.emplace_back(table->getColumn(i));

	for (int i = 0;i < rowNum; i++) {
		//wrong usage
		columnVecs[0]->setString(i, "name_" + std::to_string(i));
		columnVecs[1]->set(i, Util::createDate(2010, 1, i + 1));
		columnVecs[2]->set(i, Util::createDouble(i*i));
	}
	return table;
}

TEST_F(DolphinDBTest,testDiskTable){
	TableSP table = createDemoTable();
	conn.upload("mt", table);
	string dbPath = conn.run("getHomeDir()+\"/cpp_test\" ")->getString();
	string script;
	script += "dbPath=\""+dbPath+"\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db=database(dbPath,VALUE,2010.01.01..2010.01.30);";
	script += "tDiskGlobal=db.createPartitionedTable(mt,`tDiskGlobal,`date);";
	script += "tDiskGlobal.append!(mt);";
	//script += "saveTable(db,tDiskGlobal,`tDiskGlobal);";
	script += "select * from tDiskGlobal;";
	TableSP result = conn.run(script);
	cout << result->getString() << endl;
}

TEST_F(DolphinDBTest,testDFSTable){
	string script;
	TableSP table = createDemoTable();
	conn.upload("mt", table);
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://SAMPLE_TRDDB\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "tableName = `demoTable;";
	script += "db = database(dbPath,VALUE,2010.01.01..2010.01.30);";
	script += "date = db.createPartitionedTable(mt,tableName,`date);";
	//script += "date.tableInsert(mt);";
	script += "tradTable=database(dbPath).loadTable(tableName).append!(mt);";
	//script += "tradTable= database(dbPath).loadTable(tableName);";
	//script += "dropPartition(db,2010.01.01);";
	//script += "dropTable(db,`demoTable);";
	//script += "existsTable(\"dfs://SAMPLE_TRDDB\",`demoTable);";
	//script += "dropDatabase(\"dfs://SAMPLE_TRDDB\");";
	//script += "existsDatabase(\"dfs://SAMPLE_TRDDB\");";
	script += "select * from date;";
	//script += "select * from date where date>2020.01;";
	TableSP result = conn.run(script);
	cout << result->getString() << endl;
}

TEST_F(DolphinDBTest,testDimensionTable){
	string script;
	TableSP table = createDemoTable();
	conn.upload("mt", table);
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://db1\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,VALUE,2010.01.01..2010.01.30);";
	script += "dt = db.createTable(mt,`dt).append!(mt);";
	script += "select * from dt;";
	TableSP result = conn.run(script);
	cout << result->getString() << endl;
}

TEST_F(DolphinDBTest,testDFSTableSetStringWrong){
	string script;
	TableSP table = createDemoTableSetStringWrong();
	conn.upload("mt", table);
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://SAMPLE_TRDDB\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "tableName = `demoTable;";
	script += "db = database(dbPath,VALUE,2010.01.01..2010.01.30);";
	script += "db.createPartitionedTable(mt,tableName,`date);";
	script += "tradTable=database(dbPath).loadTable(tableName).append!(mt);";
	script += "select * from tradTable;";
	TableSP result = conn.run(script);
	cout << result->getString() << endl;
}

TEST_F(DolphinDBTest,testDFSTableSetString){
	string script;
	TableSP table = createDemoTableSetString();
	conn.upload("mt", table);
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://SAMPLE_TRDDB\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "tableName = `demoTable;";
	script += "db = database(dbPath,VALUE,2010.01.01..2010.01.30);";
	script += "db.createPartitionedTable(mt,tableName,`date);";
	script += "tradTable=database(dbPath).loadTable(tableName).append!(mt);";
	script += "select * from tradTable;";
	TableSP result = conn.run(script);
	cout << result->getString() << endl;
}

TEST_F(DolphinDBTest,testCharVectorHash){
	vector<char> testValues{ 127,-127,12,0,-12,-128 };
	int buckets[5] = { 13,43,71,97,4097 };
	int expected[5][6] = {
		{ 10,12,12,0,10,-1 },
	{ 41,18,12,0,4,-1 },
	{ 56,24,12,0,68,-1 },
	{ 30,5,12,0,23,-1 },
	{ 127,129,12,0,244,-1 } };
	int hv[6] = { 0 };

	for (unsigned int j = 0;j<5;j++) {
		for (unsigned int i = 0;i < testValues.size(); i++) {
			ConstantSP val = Util::createChar(testValues[i]);
			hv[i] = val->getHash(buckets[j]);
		}
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
		
	}
	VectorSP v = Util::createVector(DT_CHAR, 0);
	v->appendChar(testValues.data(), testValues.size());
	for (unsigned int j = 0;j<5;j++) {
		v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
}

TEST_F(DolphinDBTest,testShortVectorHash){
	vector<short> testValues{ 32767,-32767,12,0,-12,-32768 };
	int buckets[5] = { 13,43,71,97,4097 };
	int expected[5][6] = {
		{ 7,2,12,0,10,-1 },
	{ 1,15,12,0,4,-1 },
	{ 36,44,12,0,68,-1 },
	{ 78,54,12,0,23,-1 },
	{ 4088,265,12,0,244,-1 } };
	int hv[6] = { 0 };

	for (unsigned int j = 0;j<5;j++) {
		for (unsigned int i = 0;i < testValues.size(); i++) {
			ConstantSP val = Util::createShort(testValues[i]);
			hv[i] = val->getHash(buckets[j]);
		}
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
	VectorSP v = Util::createVector(DT_SHORT, 0);
	v->appendShort(testValues.data(), testValues.size());
	for (unsigned int j = 0;j<5;j++) {
		v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
}

TEST_F(DolphinDBTest,testIntVectorHash){
	vector<int> testValues{ INT_MAX,INT_MAX*(-1),12,0,-12,INT_MIN };
	int buckets[5] = { 13,43,71,97,4097 };
	int expected[5][6] = {
		{ 10,12,12,0,10,-1 },
	{ 7,9,12,0,4,-1 },
	{ 39,41,12,0,68,-1 },
	{ 65,67,12,0,23,-1 },
	{ 127,129,12,0,244,-1 } };
	int hv[6] = { 0 };

	for (unsigned int j = 0;j<5;j++) {
		for (unsigned int i = 0;i < testValues.size(); i++) {
			ConstantSP val = Util::createInt(testValues[i]);
			hv[i] = val->getHash(buckets[j]);
		}
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
	VectorSP v = Util::createVector(DT_INT, 0);
	v->appendInt(testValues.data(), testValues.size());
	for (unsigned int j = 0;j<5;j++) {
		v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
}

TEST_F(DolphinDBTest,testLongVectorHash){
	vector<long long> testValues{ LLONG_MAX,(-1)*LLONG_MAX,12,0,-12,LLONG_MIN };
	int buckets[5] = { 13,43,71,97,4097 };
	int expected[5][6] = {
		{ 7,9,12,0,4,-1 },
	{ 41,0,12,0,29,-1 },
	{ 4,6,12,0,69,-1 },
	{ 78,80,12,0,49,-1 },
	{ 4088,4090,12,0,4069,-1 } };
	int hv[6] = { 0 };

	for (unsigned int j = 0;j<5;j++) {
		for (unsigned int i = 0;i < testValues.size(); i++) {
			ConstantSP val = Util::createLong(testValues[i]);
			hv[i] = val->getHash(buckets[j]);
		}
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
	VectorSP v = Util::createVector(DT_LONG, 0);
	v->appendLong(testValues.data(), testValues.size());
	for (unsigned int j = 0;j<5;j++) {
		v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
}

TEST_F(DolphinDBTest,testStringVectorHash){
	vector<string> testValues{ "9223372036854775807","helloworldabcdefghijklmnopqrstuvwxyz","智臾科技a","hello,智臾科技a","123abc您好！a","" };
	int buckets[5] = { 13,43,71,97,4097 };
	int expected[5][6] = {
		{ 8,1,9,3,3,0 },
	{ 37,20,25,25,27,0 },
	{ 31,0,65,54,15,0 },
	{ 24,89,46,52,79,0 },
	{ 739,3737,2208,1485,376,0 } };
	int hv[6] = { 0 };

	for (unsigned int j = 0;j<5;j++) {
		for (unsigned int i = 0;i < testValues.size(); i++) {
			ConstantSP val = Util::createString(testValues[i]);
			hv[i] = val->getHash(buckets[j]);
		}
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
	VectorSP v = Util::createVector(DT_STRING, 0);
	v->appendString(testValues.data(), testValues.size());
	for (unsigned int j = 0;j<5;j++) {
		v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
}

TEST_F(DolphinDBTest,testUUIDvectorHash){
	string script;
	script = "a=rand(uuid(),6);table(a as k,hashBucket(a,13) as v1,hashBucket(a,43) as v2,hashBucket(a,71) as v3,hashBucket(a,97) as v4,hashBucket(a,4097) as v5)";
	TableSP t = conn.run(script);

	int buckets[5] = { 13,43,71,97,4097 };
	int hv[6] = { 0 };
	int expected[5][6] = { 0 };

	for (unsigned int j = 0;j<5;j++) {
		for (int i = 0;i < t->size(); i++) {
			ConstantSP val = Util::parseConstant(DT_UUID, t->getColumn(0)->getString(i));
			hv[i] = val->getHash(buckets[j]);
			expected[j][i] = t->getColumn(j + 1)->getInt(i);
		}
		for (unsigned int k = 0;k < t->size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
	VectorSP v = Util::createVector(DT_UUID, 0);
	for (int i = 0;i < t->size(); i++)
		v->append(Util::parseConstant(DT_UUID, t->getColumn(0)->getString(i)));
	for (unsigned int j = 0;j<5;j++) {
		v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < t->size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
}

TEST_F(DolphinDBTest,testIpAddrvectorHash){
	string script;
	script = "a=rand(ipaddr(),6);table(a as k,hashBucket(a,13) as v1,hashBucket(a,43) as v2,hashBucket(a,71) as v3,hashBucket(a,97) as v4,hashBucket(a,4097) as v5)";
	TableSP t = conn.run(script);

	int buckets[5] = { 13,43,71,97,4097 };
	int hv[6] = { 0 };
	int expected[5][6] = { 0 };

	for (unsigned int j = 0;j<5;j++) {
		for (int i = 0;i < t->size(); i++) {
			ConstantSP val = Util::parseConstant(DT_IP, t->getColumn(0)->getString(i));
			hv[i] = val->getHash(buckets[j]);
			expected[j][i] = t->getColumn(j + 1)->getInt(i);
		}
		for (unsigned int k = 0;k < t->size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
	VectorSP v = Util::createVector(DT_IP, 0);
	for (int i = 0;i < t->size(); i++)
		v->append(Util::parseConstant(DT_IP, t->getColumn(0)->getString(i)));
	for (unsigned int j = 0;j<5;j++) {
		v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < t->size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
}

TEST_F(DolphinDBTest,testInt128vectorHash){
	string script;
	script = "a=rand(int128(),6);table(a as k,hashBucket(a,13) as v1,hashBucket(a,43) as v2,hashBucket(a,71) as v3,hashBucket(a,97) as v4,hashBucket(a,4097) as v5)";
	TableSP t = conn.run(script);

	int buckets[5] = { 13,43,71,97,4097 };
	int hv[6] = { 0 };
	int expected[5][6] = { 0 };

	for (unsigned int j = 0;j<5;j++) {
		for (int i = 0;i < t->size(); i++) {
			ConstantSP val = Util::parseConstant(DT_INT128, t->getColumn(0)->getString(i));
			hv[i] = val->getHash(buckets[j]);
			expected[j][i] = t->getColumn(j + 1)->getInt(i);
		}
		for (unsigned int k = 0;k < t->size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
	VectorSP v = Util::createVector(DT_INT128, 0);
	for (int i = 0;i < t->size(); i++)
		v->append(Util::parseConstant(DT_INT128, t->getColumn(0)->getString(i)));
	for (unsigned int j = 0;j<5;j++) {
		v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < t->size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
}


TEST_F(DolphinDBTest,testCharVectorHash2){
    vector<char> testValues { 127, -127, 12, 0, -12, -128 };
    int buckets[5] = { 13, 43, 71, 97, 4097 };
    int expected[5][6] = { { 10, 12, 12, 0, 10, -1 }, { 41, 18, 12, 0, 4, -1 }, { 56, 24, 12, 0, 68, -1 }, { 30, 5, 12, 0, 23, -1 }, { 127, 129, 12, 0, 244, -1 } };
    int hv[6] = { 0 };

    for (unsigned int j = 0; j < 5; j++) {
        for (unsigned int i = 0; i < testValues.size(); i++) {
            ConstantSP val = Util::createChar(testValues[i]);
            hv[i] = val->getHash(buckets[j]);
            EXPECT_EQ(hv[i], expected[j][i]);
        }
    }
    VectorSP v = Util::createVector(DT_CHAR, 0);
    v->appendChar(testValues.data(), testValues.size());
    for (unsigned int j = 0; j < 5; j++) {
        v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
    }
}

TEST_F(DolphinDBTest,testShortVectorHash2){
    vector<short> testValues { 32767, -32767, 12, 0, -12, -32768 };
    int buckets[5] = { 13, 43, 71, 97, 4097 };
    int expected[5][6] = { { 7, 2, 12, 0, 10, -1 }, { 1, 15, 12, 0, 4, -1 }, { 36, 44, 12, 0, 68, -1 }, { 78, 54, 12, 0, 23, -1 }, { 4088, 265, 12, 0, 244, -1 } };
    int hv[6] = { 0 };

    for (unsigned int j = 0; j < 5; j++) {
        for (unsigned int i = 0; i < testValues.size(); i++) {
            ConstantSP val = Util::createShort(testValues[i]);
            hv[i] = val->getHash(buckets[j]);
            EXPECT_EQ(hv[i], expected[j][i]);
        }
    }
    VectorSP v = Util::createVector(DT_SHORT, 0);
    v->appendShort(testValues.data(), testValues.size());
    for (unsigned int j = 0; j < 5; j++) {
        v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
    }
}

TEST_F(DolphinDBTest,testIntVectorHash2){
    vector<int> testValues { INT_MAX, INT_MAX * (-1), 12, 0, -12, INT_MIN };
    int buckets[5] = { 13, 43, 71, 97, 4097 };
    int expected[5][6] = { { 10, 12, 12, 0, 10, -1 }, { 7, 9, 12, 0, 4, -1 }, { 39, 41, 12, 0, 68, -1 }, { 65, 67, 12, 0, 23, -1 }, { 127, 129, 12, 0, 244, -1 } };
    int hv[6] = { 0 };

    for (unsigned int j = 0; j < 5; j++) {
        for (unsigned int i = 0; i < testValues.size(); i++) {
            ConstantSP val = Util::createInt(testValues[i]);
            hv[i] = val->getHash(buckets[j]);
            EXPECT_EQ(hv[i], expected[j][i]);
        }
    }
    VectorSP v = Util::createVector(DT_INT, 0);
    v->appendInt(testValues.data(), testValues.size());
    for (unsigned int j = 0; j < 5; j++) {
        v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
    }
}

TEST_F(DolphinDBTest,testLongVectorHash2){
    vector<long long> testValues { LLONG_MAX, (-1) * LLONG_MAX, 12, 0, -12, LLONG_MIN };
    int buckets[5] = { 13, 43, 71, 97, 4097 };
    int expected[5][6] = { { 7, 9, 12, 0, 4, -1 }, { 41, 0, 12, 0, 29, -1 }, { 4, 6, 12, 0, 69, -1 }, { 78, 80, 12, 0, 49, -1 }, { 4088, 4090, 12, 0, 4069, -1 } };
    int hv[6] = { 0 };

    for (unsigned int j = 0; j < 5; j++) {
        for (unsigned int i = 0; i < testValues.size(); i++) {
            ConstantSP val = Util::createLong(testValues[i]);
            hv[i] = val->getHash(buckets[j]);
            EXPECT_EQ(hv[i], expected[j][i]);
        }
    }
    VectorSP v = Util::createVector(DT_LONG, 0);
    v->appendLong(testValues.data(), testValues.size());
    for (unsigned int j = 0; j < 5; j++) {
        v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
    }
}

TEST_F(DolphinDBTest,testStringVectorHash2){
    vector < string > testValues { "9223372036854775807", "helloworldabcdefghijklmnopqrstuvwxyz", "智臾科技a", "hello,智臾科技a", "123abc您好！a", ""};
    int buckets[5] = { 13, 43, 71, 97, 4097 };
    int expected[5][6] = { { 8, 1, 9, 3, 3, 0 }, { 37, 20, 25, 25, 27, 0 }, { 31, 0, 65, 54, 15, 0 }, { 24, 89, 46, 52, 79, 0 }, { 739, 3737, 2208, 1485, 376, 0 } };
    int hv[6] = { 0 };

    for (unsigned int j = 0; j < 5; j++) {
        for (unsigned int i = 0; i < testValues.size(); i++) {
            ConstantSP val = Util::createString(testValues[i]);
            hv[i] = val->getHash(buckets[j]);
            EXPECT_EQ(hv[i], expected[j][i]);
        }
    }
    VectorSP v = Util::createVector(DT_STRING, 0);
    v->appendString(testValues.data(), testValues.size());
    for (unsigned int j = 0; j < 5; j++) {
        v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
    }
}


TEST_F(DolphinDBTest,testUUIDvectorHash2){
    string script;
    script = "a=rand(uuid(),6);table(a as k,hashBucket(a,13) as v1,hashBucket(a,43) as v2,hashBucket(a,71) as v3,hashBucket(a,97) as v4,hashBucket(a,4097) as v5)";
    TableSP t = conn.run(script);

    int buckets[5] = { 13, 43, 71, 97, 4097 };
    int hv[6] = { 0 };
    int expected[5][6] = { 0 };

    for (unsigned int j = 0; j < 5; j++) {
        for (int i = 0; i < t->size(); i++) {
            ConstantSP val = Util::parseConstant(DT_UUID, t->getColumn(0)->getString(i));
            hv[i] = val->getHash(buckets[j]);
            expected[j][i] = t->getColumn(j + 1)->getInt(i);
            EXPECT_EQ(hv[i], expected[j][i]);
        }
    }
    VectorSP v = Util::createVector(DT_UUID, 0);
    for (int i = 0; i < t->size(); i++)
        v->append(Util::parseConstant(DT_UUID, t->getColumn(0)->getString(i)));
    for (unsigned int j = 0; j < 5; j++) {
        v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < t->size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
    }
}

TEST_F(DolphinDBTest,testIpAddrvectorHash2){
    string script;
    script = "a=rand(ipaddr(),6);table(a as k,hashBucket(a,13) as v1,hashBucket(a,43) as v2,hashBucket(a,71) as v3,hashBucket(a,97) as v4,hashBucket(a,4097) as v5)";
    TableSP t = conn.run(script);

    int buckets[5] = { 13, 43, 71, 97, 4097 };
    int hv[6] = { 0 };
    int expected[5][6] = { 0 };

    for (unsigned int j = 0; j < 5; j++) {
        for (int i = 0; i < t->size(); i++) {
            ConstantSP val = Util::parseConstant(DT_IP, t->getColumn(0)->getString(i));
            hv[i] = val->getHash(buckets[j]);
            expected[j][i] = t->getColumn(j + 1)->getInt(i);
            EXPECT_EQ(hv[i], expected[j][i]);
        }
    }
    VectorSP v = Util::createVector(DT_IP, 0);
    for (int i = 0; i < t->size(); i++)
        v->append(Util::parseConstant(DT_IP, t->getColumn(0)->getString(i)));
    for (unsigned int j = 0; j < 5; j++) {
        v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < t->size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
    }
}

TEST_F(DolphinDBTest,testInt128vectorHash2){
    string script;
    script = "a=rand(int128(),6);table(a as k,hashBucket(a,13) as v1,hashBucket(a,43) as v2,hashBucket(a,71) as v3,hashBucket(a,97) as v4,hashBucket(a,4097) as v5)";
    TableSP t = conn.run(script);

    int buckets[5] = { 13, 43, 71, 97, 4097 };
    int hv[6] = { 0 };
    int expected[5][6] = { 0 };

    for (unsigned int j = 0; j < 5; j++) {
        for (int i = 0; i < t->size(); i++) {
            ConstantSP val = Util::parseConstant(DT_INT128, t->getColumn(0)->getString(i));
            hv[i] = val->getHash(buckets[j]);
            expected[j][i] = t->getColumn(j + 1)->getInt(i);
            EXPECT_EQ(hv[i], expected[j][i]);
        }
    }
    VectorSP v = Util::createVector(DT_INT128, 0);
    for (int i = 0; i < t->size(); i++)
        v->append(Util::parseConstant(DT_INT128, t->getColumn(0)->getString(i)));
    for (unsigned int j = 0; j < 5; j++) {
        v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < t->size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
    }
}
/*
void testshare(){
string dbPath = conn.run(" getHomeDir()+\"/cpp_test\"")->getString();
string script;
script += "TickDB = database(dbPath, RANGE, `A`M`ZZZZ, `DFS_NODE1`DFS_NODE2);";
script += "t=table(rand(`AAPL`IBM`C`F,100) as sym, rand(1..10, 100) as qty, rand(10.25 10.5 10.75, 100) as price);";
script += "share t as TickDB.Trades on sym;";
//script += "dropTable(TickDB,`TickDB.Trades);";
script += "select top 10 * from TickDB.Trades;";

//script += "select count(*) from TickDB.Trades;";
TableSP result = conn.run(script);
cout<<result->getString()<<endl;

}
*/

TEST_F(DolphinDBTest,testRun){
	//所有参数都在服务器端
	/*conn.run("x = [1, 3, 5]; y = [2, 4, 6]");
	ConstantSP result = conn.run("add(x,y)");
	cout<<result->getString()<<endl;*/
	//仅有一个参数在服务器端
	/*conn.run("x = [1, 3, 5]");
	vector<ConstantSP> args;
	ConstantSP y = Util::createVector(DT_DOUBLE, 3);
	double array_y[] = {1.5, 2.5, 7};
	y->setDouble(0, 3, array_y);
	args.push_back(y);
	ConstantSP result = conn.run("add{x,}", args);
	cout<<result->getString()<<endl;*/
	//两个参数都在客户端
	vector<ConstantSP> args;
	ConstantSP x = Util::createVector(DT_DOUBLE, 3);
	double array_x[] = { 1.5, 2.5, 7 };
	x->setDouble(0, 3, array_x);
	ConstantSP y = Util::createVector(DT_DOUBLE, 3);
	double array_y[] = { 8.5, 7.5, 3 };
	y->setDouble(0, 3, array_y);
	args.push_back(x);
	args.push_back(y);
	ConstantSP result = conn.run("add", args);
	cout << result->getString() << endl;
}

static void Block_Reader_DFStable() {
	string script;
	string script1;
	TableSP table = createDemoTable();
	conn.upload("mt", table);
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://TEST_BLOCK\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "tableName = `pt;";
	script += "db = database(dbPath,VALUE,2010.01.01..2010.01.30);";
	script += "pt = db.createPartitionedTable(mt,tableName,`date);";
	script += "pt.append!(mt);";
	script += "n=12450;";
	script += "t_Block_Reader_DFStable=table(take(`name_3,n) as name,take(2010.01.01,n) as date,rand(30,n) as price);";
	script += "pt.append!(t_Block_Reader_DFStable);";
	conn.run(script);
	script1 += "select * from pt;";
	int fetchsize1 = 12453;
	BlockReaderSP reader = conn.run(script1, 4, 2, fetchsize1);
	ConstantSP t;
	int total = 0;
	while (reader->hasNext()) {
		t = reader->read();
		total += t->size();
		//cout<< "read" <<t->size()<<endl;
		EXPECT_EQ(t->size(), fetchsize1);
	}
	//cout<<"total is"<<total<<endl;
	EXPECT_EQ(total, 12453);

	int fetchsize2 = 8200;
	BlockReaderSP reader2 = conn.run(script1, 4, 2, fetchsize2);
	ConstantSP t2;
	int total2 = 0;
	int tmp = fetchsize2;
	while (reader2->hasNext()) {
		t2 = reader2->read();
		total2 += t2->size();
		//cout<< "read" <<t2->size()<<endl;
		EXPECT_EQ(t2->size(), tmp);
		tmp = 12453 - tmp;
	}
	//cout<<"total is"<<total<<endl;
	EXPECT_EQ(total2, 12453);

	int fetchsize3 = 15000;
	BlockReaderSP reader3 = conn.run(script1, 4, 2, fetchsize3);
	ConstantSP t3;
	int total3 = 0;
	while (reader3->hasNext()) {
		t3 = reader3->read();
		total3 += t3->size();
		//cout<< "read" <<t2->size()<<endl;
		EXPECT_EQ(t3->size(), 12453);
	}
	//cout<<"total is"<<total<<endl;
	EXPECT_EQ(total3, 12453);
}

TEST_F(DolphinDBTest,test_Block_Reader_DFStable){
	Block_Reader_DFStable();
}


static void Block_Table() {
	string script;
	string script1;
	script += "rows=12453;";
	script += "testblock=table(take(1,rows) as id,take(`A,rows) as symbol,take(2020.08.01..2020.10.01,rows) as date, rand(50,rows) as size,rand(50.5,rows) as price);";
	//script += "select count(*) from testblock;";
	conn.run(script);
	script1 += "select * from testblock ";
	int fetchsize1 = 12453;
	BlockReaderSP reader = conn.run(script1, 4, 2, fetchsize1);
	ConstantSP t;
	int total = 0;
	while (reader->hasNext()) {
		t = reader->read();
		total += t->size();
		//cout<< "read" <<t->size()<<endl;
		EXPECT_EQ(t->size(), fetchsize1);
	}
	//cout<<"total is"<<total<<endl;
	EXPECT_EQ(total, 12453);

	int fetchsize2 = 8200;
	BlockReaderSP reader2 = conn.run(script1, 4, 2, fetchsize2);
	ConstantSP t2;
	int total2 = 0;
	int tmp = fetchsize2;
	while (reader2->hasNext()) {
		t2 = reader2->read();
		total2 += t2->size();
		//cout<< "read" <<t2->size()<<endl;
		EXPECT_EQ(t2->size(), tmp);
		tmp = 12453 - tmp;
	}
	//cout<<"total is"<<total<<endl;
	EXPECT_EQ(total2, 12453);

	int fetchsize3 = 15000;
	BlockReaderSP reader3 = conn.run(script1, 4, 2, fetchsize3);
	ConstantSP t3;
	int total3 = 0;
	while (reader3->hasNext()) {
		t3 = reader3->read();
		total3 += t3->size();
		//cout<< "read" <<t2->size()<<endl;
		EXPECT_EQ(t3->size(), 12453);
	}
	//cout<<"total is"<<total<<endl;
	EXPECT_EQ(total3, 12453);
}

TEST_F(DolphinDBTest,test_Block_Table){
	Block_Table();
}

TEST_F(DolphinDBTest,test_block_skipALL){
	string script;
	script += "login(`admin,`123456);";
	script += R"(select * from loadTable("dfs://TEST_BLOCK","pt");)";
	BlockReaderSP reader = conn.run(script, 4, 2, 8200);
	ConstantSP t = reader->read();
	reader->skipAll();
	TableSP result = conn.run("table(1..100 as id1)");
	//cout<<result->getString()<<endl;
	EXPECT_EQ(result->size(), 100);
}

TEST_F(DolphinDBTest,test_huge_table){
	string script;
	string script1;
	script += "rows=20000000;";
	script += "testblock=table(take(1,rows) as id,take(`A,rows) as symbol,take(2020.08.01..2020.10.01,rows) as date, rand(50,rows) as size,rand(50.5,rows) as price);";
	conn.run(script);
	script1 += "select * from testblock;";
	BlockReaderSP reader = conn.run(script1, 4, 2, 8200);
	ConstantSP t;
	int total = 0;
	int i = 1;
	int fetchsize = 8200;
	while (reader->hasNext()) {
		t = reader->read();
		total += t->size();
		//cout<< "read" <<t->size()<<endl;

		if (t->size() == 8200) {
			EXPECT_EQ(t->size(), 8200);
		}
		else {
			EXPECT_EQ(t->size(), 200);
		}

	}
	//cout<<"total is"<<total<<endl;
	EXPECT_EQ(total, 20000000);
}

TEST_F(DolphinDBTest,test_huge_DFS){
	string script;
	string script1;
	TableSP table = createDemoTable();
	conn.upload("mt", table);
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://TEST_Huge_BLOCK\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "tableName = `pt;";
	script += "db = database(dbPath,VALUE,2010.01.01..2010.01.30);";
	script += "pt = db.createPartitionedTable(mt,tableName,`date);";
	script += "pt.append!(mt);";
	script += "n=20000000;";
	script += "t_test_huge_DFS=table(take(`name_3,n) as name,take(2010.01.01,n) as date,rand(30,n) as price);";
	script += "pt.append!(t_test_huge_DFS);";
	conn.run(script);
	script1 += "select * from pt;";
	int fetchsize1 = 8200;
	BlockReaderSP reader = conn.run(script1, 4, 2, fetchsize1);
	ConstantSP t;
	int total = 0;
	while (reader->hasNext()) {
		t = reader->read();
		total += t->size();
		//cout<< "read" <<t->size()<<endl;
		if (t->size() == 8200) {
			EXPECT_EQ(t->size(), 8200);
		}
		else {
			EXPECT_EQ(t->size(), 203);
		}

	}
	//cout<<"total is"<<total<<endl;
	EXPECT_EQ(total, 20000003);

	int fetchsize2 = 2000000;
	BlockReaderSP reader2 = conn.run(script1, 4, 2, fetchsize2);
	ConstantSP t2;
	int total2 = 0;
	while (reader2->hasNext()) {
		t2 = reader2->read();
		total2 += t2->size();
		//cout<< "read" <<t2->size()<<endl;

		if (t2->size() == 2000000) {
			EXPECT_EQ(t2->size(), 2000000);
		}
		else {
			EXPECT_EQ(t2->size(), 3);
		}

		//EXPECT_EQ("tets_Block_Reader_DFStable",t->size(),fetchsize1);
	}
	//cout<<"total is"<<total<<endl;
	EXPECT_EQ( total2, 20000003);

}



TEST_F(DolphinDBTest,test_symbol_optimization){
	string script;
	conn.run(script);
	vector<string> colNames = { "col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10" };
	vector<DATA_TYPE> colTypes = { DT_SYMBOL, DT_SYMBOL, DT_SYMBOL, DT_SYMBOL, DT_SYMBOL, DT_SYMBOL, DT_SYMBOL, DT_SYMBOL, DT_SYMBOL, DT_SYMBOL };
	int colNum = 10, rowNum = 2000000;
	TableSP t11 = Util::createTable(colNames, colTypes, rowNum, 2000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t11->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[1]->set(i, Util::createString("B" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createString("C" + std::to_string(i % 1000)));
		columnVecs[3]->set(i, Util::createString("D" + std::to_string(i % 1000)));
		columnVecs[4]->set(i, Util::createString("E" + std::to_string(i % 1000)));
		columnVecs[5]->set(i, Util::createString("F" + std::to_string(i % 1000)));
		columnVecs[6]->set(i, Util::createString("G" + std::to_string(i % 1000)));
		columnVecs[7]->set(i, Util::createString("H" + std::to_string(i % 1000)));
		columnVecs[8]->set(i, Util::createString("I" + std::to_string(i % 1000)));
		columnVecs[9]->set(i, Util::createString("J" + std::to_string(i % 1000)));
	}
	int64_t startTime, time;
	startTime = getTimeStampMs();
	conn.upload("t11", { t11 });
	time = getTimeStampMs() - startTime;
	cout << "symbol table:" << time << "ms" << endl;
	string script1;
	script1 += "n=2000000;";
	script1 += "tmp=table(take(symbol(\"A\"+string(0..999)), n) as col1, take(symbol(\"B\"+string(0..999)), n) as col2,\
	 take(symbol(\"C\"+string(0..999)), n) as col3, take(symbol(\"D\"+string(0..999)), n) as col4, \
	 take(symbol(\"E\"+string(0..999)), n) as col5, take(symbol(\"F\"+string(0..999)), n) as col6,\
	  take(symbol(\"G\"+string(0..999)), n) as col7, take(symbol(\"H\"+string(0..999)), n) as col8, \
	  take(symbol(\"I\"+string(0..999)), n) as col9, take(symbol(\"J\"+string(0..999)), n) as col10);";
	script1 += "each(eqObj, t11.values(), tmp.values());";
	ConstantSP result = conn.run(script1);
	for (int i = 0; i<10; i++){
		cout<<result->getInt(i);
		EXPECT_EQ(result->getInt(i), 1);}
	ConstantSP res = conn.run("t11");
}

TEST_F(DolphinDBTest,testClearMemory_var){
	string script, script1;
	script += "login('admin', '123456');";
	script += "testVar=1000000";
	conn.run(script, 4, 2, 0, true);
	string result = conn.run("objs()[`name][0]")->getString();

	EXPECT_EQ(result.length(), size_t(0));	
}

TEST_F(DolphinDBTest,testClearMemory_){
	string script, script1, script2;
	script += "login('admin', '123456');";
	script += "dbPath='dfs://testClearMemory_';";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db=database(dbPath, VALUE, 2012.01.01..2012.01.10);";
	script += "t=table(1:0, [`date], [DATE]);";
	script += "pt=db.createPartitionedTable(t, `pt, `date);";
	script += "getSessionMemoryStat()[`memSize][0];";
	script += "select * from pt;";
	conn.run(script, 4, 2, 0, true);
	string result = conn.run("objs()[`name][0]")->getString();
	EXPECT_EQ(result.length(), size_t(0));
}

TEST_F(DolphinDBTest,test_symbol_base_exceed_2097152){
	vector < string > colNames = { "name", "id", "str" };
	vector<DATA_TYPE> colTypes = { DT_SYMBOL, DT_INT, DT_STRING };
	int colNum = 3, rowNum = 30000000;
	ConstantSP table = Util::createTable(colNames, colTypes, rowNum, 100);
	vector<VectorSP> columnVecs;
	for (int i = 0;i<colNum;i++) {
		columnVecs.push_back(table->getColumn(i));
	}
	try {
		for (int i = 0;i<rowNum;i++) {
			columnVecs[0]->set(i, Util::createString("name_" + std::to_string(i)));
			columnVecs[1]->setInt(i, i);
			columnVecs[2]->setString(i, std::to_string(i));
		}
	}
	catch (exception e) {
		cout << e.what() << endl;
	}
}

TEST_F(DolphinDBTest,test_Block_Reader_DFStable_While_Block_Table){
	std::thread t1(Block_Table);
	t1.join();
	std::thread t2(Block_Reader_DFStable);
	t2.join();
}

TEST_F(DolphinDBTest,test_printMsg){
	string script5 = "a=int(1);\
						b=bool(1);\
						c=char(1);\
						d=NULL;\
						ee=short(1);\
						f=long(1);\
						g=date(1);\
						h=month(1);\
						i=time(1);\
						j=minute(1);\
						k=second(1);\
						l=datetime(1);\
						m=timestamp(1);\
						n=nanotime(1);\
						o=nanotimestamp(1);\
						p=float(1);\
						q=double(1);\
						r=\"1\";\
						s=uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\");\
						t=blob(string[1]);\
						u=table(1 2 3 as col1, `a`b`c as col2);\
        				v=arrayVector(1 2 3 , 9 9 9)";
	conn.run(script5);					
    conn.run("print(a,b,c,d,ee,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v)");
}