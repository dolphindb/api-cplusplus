class BatchTableWriterTest:public testing::Test
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


TEST_F(BatchTableWriterTest,test_batchTableWriter_insert){
	string script;
	script += "login('admin', '123456');";
	script += "dbPath = 'dfs://test_batchTableWriter';";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db=database(dbPath,VALUE,2010.01.01..2010.01.10);";
	script += "t=table(100:0,`dbbool `dbchar `dbshort `dbstring_char `dbstring `dblong `dbnanotime `dbnanotimestamp `dbtimestamp `dbfloat `dbdouble `dbint `dbdate `dbmonth `dbtime `dbsecond `dbminute `dbdatetime `dbdatehour , [BOOL, CHAR, SHORT, STRING, STRING, LONG, NANOTIME, NANOTIMESTAMP, TIMESTAMP, FLOAT, DOUBLE, INT, DATE, MONTH, TIME, SECOND, MINUTE, DATETIME, DATEHOUR]);";
	script += "share table(100:0,`dbbool `dbchar `dbshort `dbstring_char `dbstring `dblong `dbnanotime `dbnanotimestamp `dbtimestamp `dbfloat `dbdouble `dbint `dbdate `dbmonth `dbtime `dbsecond `dbminute `dbdatetime `dbdatehour , [BOOL, CHAR, SHORT, STRING, STRING, LONG, NANOTIME, NANOTIMESTAMP, TIMESTAMP, FLOAT, DOUBLE, INT, DATE, MONTH, TIME, SECOND, MINUTE, DATETIME, DATEHOUR]) as table1;";
	script += "db.createTable(t,`dtable).append!(t);";
	script += "db.createPartitionedTable(t,`ptable,`dbdate).append!(t);";
	conn.run(script);
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	btw.addTable("table1", "");
	const char* strtest = "test";
	long long testlong = 100;
	string expected = "expected = table( bool(1) as dbbool, char('t') as dbchar, short(100) as dbshort, string('test') as dbstring_char, string('test') as dbstring, long(100) as dblong, nanotime(100) as dbnanotime, nanotimestamp(100) as dbnanotimestamp, timestamp(100) as dbtimestamp,float(100) as dbfloat, double(100) as dbdouble, int(100) as dbint, date(100) as dbdate, month(100) as dbmonth, time(100) as dbtime, second(100) as dbsecond, minute(100) as dbminute, datetime(100) as dbdatetime, datehour(100) as dbdatehour);";
	conn.run(expected);
	string expected1 = "expected1 = table( take(bool(1),3003000) as dbbool, take(char('t'),3003000) as dbchar, take(short(100),3003000) as dbshort, take(string('test'),3003000) as dbstring_char, take(string('test'),3003000) as dbstring, take(long(100),3003000) as dblong, take(nanotime(100),3003000) as dbnanotime, take(nanotimestamp(100),3003000) as dbnanotimestamp, take(timestamp(100),3003000) as dbtimestamp, take(float(100),3003000) as dbfloat, take(double(100),3003000) as dbdouble, take(int(100),3003000) as dbint, take(date(100),3003000) as dbdate, take(month(100),3003000) as dbmonth, take(time(100),3003000) as dbtime, take(second(100),3003000) as dbsecond, take(minute(100),3003000) as dbminute, take(datetime(100),3003000) as dbdatetime, take(datehour(100),3003000) as dbdatehour);";
	conn.run(expected1);

	cout<<"test_batchTableWriter_insertTo_inMemoryTable"<<endl;
	EXPECT_ANY_THROW(btw.insert("table1", "", int(0), int(0), short(100), strtest, string("test"), testlong, testlong, testlong, testlong, float(100), double(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100)));
	EXPECT_ANY_THROW(btw.insert("table1", "", char('0'), char('t'), short(100), strtest, string("test"), testlong, testlong, testlong, testlong, float(100), double(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100)));

	//size1000
	for (int i = 0;i<1000;i++) {
		btw.insert("table1", "", char('0'), char('t'), short(100), strtest, string("test"), testlong, testlong, testlong, testlong, float(100), double(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100));
	}
	{
		while(btw.getAllStatus()->getColumn(3)->getString(0).c_str() != to_string(1000)) {
			this_thread::sleep_for(chrono::seconds(1));
			cout<<"now rows:"<<btw.getAllStatus()->getColumn(3)->getString(0).c_str()<<endl;
		}
	}
	int result1 = 0;
	for (int i = 0;i<1000;i++) {
		string script = "eqObj(table1[" + std::to_string(i) + "].values(),expected[0].values());";
		result1 += conn.run(script)->getInt();
	}
	EXPECT_EQ(result1, 1000);
	//size2000
	for (int i = 0;i<2000;i++) {
		btw.insert("table1", "", char('0'), char('t'), short(100), strtest, string("test"), testlong, testlong, testlong, testlong, float(100), double(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100));
	}
	{
		
		while(btw.getAllStatus()->getColumn(3)->getString(0).c_str() != to_string(3000)) {
			this_thread::sleep_for(chrono::seconds(1));
			cout<<"now rows:"<<btw.getAllStatus()->getColumn(3)->getString(0).c_str()<<endl;
		}
	}
	result1 = 0;
	for (int i = 0;i<3000;i++) {
		string script = "eqObj(table1[" + std::to_string(i) + "].values(),expected[0].values());";
		result1 += conn.run(script)->getInt();
	}
	EXPECT_EQ(result1, 3000);
	//size3000000
	for (int i = 0;i<3000000;i++) {
		btw.insert("table1", "", char('0'), char('t'), short(100), strtest, string("test"), testlong, testlong, testlong, testlong, float(100), double(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100));
	}
	{
		
		while(btw.getAllStatus()->getColumn(3)->getString(0).c_str() != to_string(3003000)) {
			this_thread::sleep_for(chrono::seconds(1));
			cout<<"now rows:"<<btw.getAllStatus()->getColumn(3)->getString(0).c_str()<<endl;
		}
	}
	script = "eqObj(table1.values(),expected1.values());";
	result1 = conn.run(script)->getInt();
	EXPECT_EQ(result1, 1);
	btw.removeTable("table1", "");

	cout<<"test_batchTableWriter_insertTo_partitionedTable"<<endl;
	//size1000
	btw.addTable("dfs://test_batchTableWriter", "dtable");
	for (int i = 0;i<1000;i++) {
		btw.insert("dfs://test_batchTableWriter", "dtable", char('0'), char('t'), short(100), strtest, string("test"), testlong, testlong, testlong, testlong, float(100), double(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100));
	}
	{
		
		while(btw.getAllStatus()->getColumn(3)->getString(0).c_str() != to_string(1000)) {
			this_thread::sleep_for(chrono::seconds(1));
			cout<<"now rows:"<<btw.getAllStatus()->getColumn(3)->getString(0).c_str()<<endl;
		}
	}
	int result2 = 0;
	for (int i = 0;i<1000;i++) {
		string script = "dt=loadTable('dfs://test_batchTableWriter', 'dtable'); eqObj(( select * from dt)[" + std::to_string(i) + "].values(),expected[0].values());";
		result2 += conn.run(script)->getInt();
	}
	EXPECT_EQ(result2, 1000);
	//size2000
	for (int i = 0;i<2000;i++) {
		btw.insert("dfs://test_batchTableWriter", "dtable", char('0'), char('t'), short(100), strtest, string("test"), testlong, testlong, testlong, testlong, float(100), double(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100));
	}
	{
		
		while(btw.getAllStatus()->getColumn(3)->getString(0).c_str() != to_string(3000)) {
			this_thread::sleep_for(chrono::seconds(1));
			cout<<"now rows:"<<btw.getAllStatus()->getColumn(3)->getString(0).c_str()<<endl;
		}
	}
	result2 = 0;
	for (int i = 0;i<3000;i++) {
		string script = "dt=loadTable('dfs://test_batchTableWriter', 'dtable'); eqObj(( select * from dt)[" + std::to_string(i) + "].values(),expected[0].values());";
		result2 += conn.run(script)->getInt();
	}
	EXPECT_EQ(result2, 3000);
	//size3000000
	for (int i = 0;i<3000000;i++) {
		btw.insert("dfs://test_batchTableWriter", "dtable", char('0'), char('t'), short(100), strtest, string("test"), testlong, testlong, testlong, testlong, float(100), double(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100));
	}
	{
		
		while(btw.getAllStatus()->getColumn(3)->getString(0).c_str() != to_string(3003000)) {
			this_thread::sleep_for(chrono::seconds(1));
			cout<<"now rows:"<<btw.getAllStatus()->getColumn(3)->getString(0).c_str()<<endl;
		}
	}
	result2 = 0;
	script = "eqObj(( select * from dt).values(),expected1.values());";
	result2 = conn.run(script)->getInt();
	EXPECT_EQ(result2, 1);
	btw.removeTable("dfs://test_batchTableWriter", "dtable");

	cout<<"test_batchTableWriter_insertTo_partitionedTable_2"<<endl;
	//size1000
	btw.addTable("dfs://test_batchTableWriter", "ptable");
	for (int i = 0;i<1000;i++) {
		btw.insert("dfs://test_batchTableWriter", "ptable", char('0'), char('t'), short(100), strtest, string("test"), testlong, testlong, testlong, testlong, float(100), double(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100));
	}
	{
		
		while(btw.getAllStatus()->getColumn(3)->getString(0).c_str() != to_string(1000)) {
			this_thread::sleep_for(chrono::seconds(1));
			cout<<"now rows:"<<btw.getAllStatus()->getColumn(3)->getString(0).c_str()<<endl;
		}
	}
	int result3 = 0;
	for (int i = 0;i<1000;i++) {
		string script = "pt=loadTable('dfs://test_batchTableWriter', 'ptable'); eqObj(( select * from pt)[" + std::to_string(i) + "].values(),expected[0].values());";
		result3 += conn.run(script)->getInt();
	}
	EXPECT_EQ(result3, 1000);
	//size2000
	for (int i = 0;i<2000;i++) {
		btw.insert("dfs://test_batchTableWriter", "ptable", char('0'), char('t'), short(100), strtest, string("test"), testlong, testlong, testlong, testlong, float(100), double(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100));
	}
	{
		
		while(btw.getAllStatus()->getColumn(3)->getString(0).c_str() != to_string(3000)) {
			this_thread::sleep_for(chrono::seconds(1));
			cout<<"now rows:"<<btw.getAllStatus()->getColumn(3)->getString(0).c_str()<<endl;
		}
	}
	result3 = 0;
	for (int i = 0;i<3000;i++) {
		string script = "pt=loadTable('dfs://test_batchTableWriter', 'ptable'); eqObj(( select * from pt)[" + std::to_string(i) + "].values(),expected[0].values());";
		result3 += conn.run(script)->getInt();
	}
	EXPECT_EQ(result3, 3000);
	//size3000000
	for (int i = 0;i<3000000;i++) {
		btw.insert("dfs://test_batchTableWriter", "ptable", char('0'), char('t'), short(100), strtest, string("test"), testlong, testlong, testlong, testlong, float(100), double(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100));
	}
	{
		while(btw.getAllStatus()->getColumn(3)->getString(0).c_str() != to_string(3003000)) {
			this_thread::sleep_for(chrono::seconds(1));
			cout<<"now rows:"<<btw.getAllStatus()->getColumn(3)->getString(0).c_str()<<endl;
		}
	}
	result3 = 0;
	script = "eqObj(( select * from pt).values(),expected1.values());";
	result3 = conn.run(script)->getInt();
	EXPECT_EQ(result3, 1);
	btw.removeTable("dfs://test_batchTableWriter", "ptable");
}

TEST_F(BatchTableWriterTest,test_batchTableWriter_insert_symbol_in_memory){
	string script;
	script += "try{undef(`st, SHARED)}catch(ex){}; share table(3000000:0, `c1`c2`c3, [SYMBOL, SYMBOL, SYMBOL]) as st;";
	conn.run(script);
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	btw.addTable("st");
	for (int i = 0; i < 3000000; i++) {
		btw.insert("st", "", "A" + std::to_string(i%10), "B" + std::to_string(i%10), "C" + std::to_string(i%10));
	}
	
	while(btw.getAllStatus()->getColumn(3)->getString(0).c_str() != to_string(3000000)) {
		this_thread::sleep_for(chrono::seconds(1));
		cout<<"now rows:"<<btw.getAllStatus()->getColumn(3)->getString(0).c_str()<<endl;
	}
	string script2;
	script2 += "c1v=`A + string(0..9);";
	script2 += "c2v=`B + string(0..9);";
	script2 += "c3v=`C + string(0..9);";
	script2 += "expected=table(symbol(loop(take{, 300000}, c1v).flatten()) as c1, symbol(loop(take{, 300000}, c2v).flatten()) as c2, symbol(loop(take{, 300000}, c3v).flatten()) as c3);";
	script2 += "each(eqObj, (select * from expected order by c1, c2, c3).values(), (select * from st order by c1, c2, c3).values()).all()";
	int result1;
	result1 = conn.run(script2)->getInt();
	EXPECT_EQ(result1, 1);
	btw.removeTable("st");
}

TEST_F(BatchTableWriterTest,test_batchTableWriter_insert_symbol_dfs){
	string script;
	script += "dbName='dfs://test_batchTableWriter_symbol';";
	script += "if(existsDatabase(dbName)){dropDatabase(dbName)};";
	script += "db=database(dbName, HASH, [SYMBOL, 10]);";
	script += "t = table(1:0, `c1`c2`c3, [SYMBOL, SYMBOL, SYMBOL]);";
	script += "pt=db.createPartitionedTable(t, `pt, `c1);";
	conn.run(script);
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	btw.addTable("dfs://test_batchTableWriter_symbol", "pt");
	for (int i = 0; i < 3000000; i++) {
		btw.insert("dfs://test_batchTableWriter_symbol", "pt", "A" + std::to_string(i % 10), "B" + std::to_string(i % 10), "C" + std::to_string(i % 10));
	}
	
	while(btw.getAllStatus()->getColumn(3)->getString(0).c_str() != to_string(3000000)) {
		this_thread::sleep_for(chrono::seconds(1));
		cout<<"now rows:"<<btw.getAllStatus()->getColumn(3)->getString(0).c_str()<<endl;
	}
	string script2;
	script2 += "c1v=`A + string(0..9);";
	script2 += "c2v=`B + string(0..9);";
	script2 += "c3v=`C + string(0..9);";
	script2 += "expected=table(symbol(loop(take{, 300000}, c1v).flatten()) as c1, symbol(loop(take{, 300000}, c2v).flatten()) as c2, symbol(loop(take{, 300000}, c3v).flatten()) as c3);";
	script2 += "each(eqObj, (select * from expected order by c1, c2, c3).values(), (select * from pt order by c1, c2, c3).values()).all()";
	int result1;
	result1 = conn.run(script2)->getInt();
	EXPECT_EQ(result1, 1);
	btw.removeTable("dfs://test_batchTableWriter_symbol", "pt");
}

TEST_F(BatchTableWriterTest,test_batchTableWriter_insert_16_bytes){
	string script;
	script += "try{undef(`st, SHARED)}catch(ex){}; share table(3000000:0, `c1`c2`c3, [UUID, INT128, IPADDR]) as st;";
	conn.run(script);
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	btw.addTable("st");
	unsigned char data[16] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
	for (int i = 0; i < 10; i++) {
		btw.insert("st", "", data, data, data);
	}
	
	while(btw.getAllStatus()->getColumn(3)->getString(0).c_str() != to_string(10)) {
		this_thread::sleep_for(chrono::seconds(1));
		cout<<"now rows:"<<btw.getAllStatus()->getColumn(3)->getString(0).c_str()<<endl;
	}
	btw.removeTable("st");
	EXPECT_EQ(1, 1);
}

TEST_F(BatchTableWriterTest,test_batchTableWriter_insert_char_len_not_16){
	string script;
	script += "try{undef(`st, SHARED)}catch(ex){}; share table(3000000:0, `c1`c2`c3, [UUID, INT128, IPADDR]) as st;";
	conn.run(script);
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	btw.addTable("st");
	unsigned char data[15] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 };
	for (int i = 0; i < 10; i++) {
		btw.insert("st", "", data, data, data);
	}
	while(btw.getAllStatus()->getColumn(3)->getString(0).c_str() != to_string(10)) {	
		this_thread::sleep_for(chrono::seconds(1));
		cout<<"now rows:"<<btw.getAllStatus()->getColumn(3)->getString(0).c_str()<<endl;
	}
	TableSP result;
	result = conn.run("select * from st");
	cout << result->getString() << endl;
	EXPECT_EQ(1, 1);
}

TEST_F(BatchTableWriterTest,test_batchTableWriter_getUnwrittenData){
	string script;
	script += "try{undef(`st, SHARED)}catch(ex){}; share table(3000000:0, `c1`c2`c3, [SYMBOL, SYMBOL, SYMBOL]) as st;";
	conn.run(script);
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	btw.addTable("st");
	for (int i = 0; i < 65536; i++) {
		btw.insert("st", "", "A" + std::to_string(i % 10), "B" + std::to_string(i % 10), "C" + std::to_string(i % 10));
	}
	string cur_rows = btw.getAllStatus()->getColumn(3)->getString(0).c_str();
	while(cur_rows != to_string(65536)) {
		this_thread::sleep_for(chrono::seconds(1));
		cout<<"now rows:"<<btw.getAllStatus()->getColumn(3)->getString(0).c_str()<<endl;
		cur_rows = btw.getAllStatus()->getColumn(3)->getString(0).c_str();
	}

	TableSP tableUnwritten2;
	tableUnwritten2 = btw.getUnwrittenData("st");
	int rowNum2;
	rowNum2 = tableUnwritten2->getColumn(0)->size();
	EXPECT_EQ(rowNum2, 0);
	btw.removeTable("st");
}

TEST_F(BatchTableWriterTest,test_batchTableWriter_addTable){
	string script;
	script += "login('admin', '123456');";
	script += "dbPath = 'dfs://test_batchTableWriter';";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "share table(100:0,`test1 `test2, [INT,INT]) as testadd;";
	conn.run(script);
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	btw.addTable("testadd", "");
	EXPECT_ANY_THROW(btw.addTable("testadd", ""));
	this_thread::sleep_for(chrono::seconds(1));
	btw.removeTable("testadd", "");
}

TEST_F(BatchTableWriterTest,test_batchTableWriter_removeTable){
	string script;
	script += "login('admin', '123456');";
	script += "dbPath = 'dfs://test_batchTableWriter';";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "share table(100:0,`test1 `test2, [INT,INT]) as testremove;";
	conn.run(script);
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	btw.addTable("testremove", "");
	this_thread::sleep_for(chrono::seconds(1));
	btw.removeTable("test_remove", "");
	EXPECT_ANY_THROW(get<0>(btw.getStatus("test_remove", ""))); // table has been removed, so get one exception.
}

static void test_batchTableWriter_insert_thread_fuction(int id, BatchTableWriter &btw, string dbName, string tableName,
	TableSP data) {
	DBConnection connNew;
	connNew.connect(hostName, port, "admin", "123456");
	size_t dataRow = data->rows();
	size_t dataColumn = data->columns();
	vector<VectorSP> dataVec;
	for (int i = 0; i < dataColumn; ++i) {
		dataVec.push_back(data->getColumn(i));
	}

	vector<char> boolBuffer(10);
	vector<char> charBuffer(10);
	vector<short> shortBuffer(10);
	vector<char*> stringBuffer(10);
	vector<long long> longBuffer(10);
	vector<long long> nanotimeBuffer(10);
	vector<long long> nanotimestampBuffer(10);
	vector<long long> timestampBuffer(10);
	vector<float> floatBuffer(10);
	vector<double> doubleBuffer(10);
	vector<int> intBuffer(10);
	vector<int> dateBuffer(10);
	vector<int> monthBuffer(10);
	vector<int> timeBuffer(10);
	vector<int> secondBuffer(10);
	vector<int> minuteBuffer(10);
	vector<int> datetimeBuffer(10);
	vector<int> datehourBuffer(10);
	vector<int> iidBuffer(10);

	const int maxIndex = 10;
	const char *boolPtr = dataVec[0]->getBoolConst(0, maxIndex, boolBuffer.data());
	const char *charPtr = dataVec[1]->getCharConst(0, maxIndex, charBuffer.data());
	const short *shortPtr = dataVec[2]->getShortConst(0, maxIndex, shortBuffer.data());
	char **stringPtr = dataVec[3]->getStringConst(0, maxIndex, (char **)stringBuffer.data());
	const long long *longPtr = dataVec[4]->getLongConst(0, maxIndex, longBuffer.data());
	const long long *nanotimePtr = dataVec[5]->getLongConst(0, maxIndex, nanotimeBuffer.data());
	const long long *nanotimestampPtr = dataVec[6]->getLongConst(0, maxIndex, nanotimestampBuffer.data());
	const long long *timestampPtr = dataVec[7]->getLongConst(0, maxIndex, timestampBuffer.data());
	const float *floatPtr = dataVec[8]->getFloatConst(0, maxIndex, floatBuffer.data());
	const double *doublePtr = dataVec[9]->getDoubleConst(0, maxIndex, doubleBuffer.data());
	const int *intPtr = dataVec[10]->getIntConst(0, maxIndex, intBuffer.data());
	const int *datePtr = dataVec[11]->getIntConst(0, maxIndex, dateBuffer.data());
	const int *monthPtr = dataVec[12]->getIntConst(0, maxIndex, monthBuffer.data());
	const int *timePtr = dataVec[13]->getIntConst(0, maxIndex, timeBuffer.data());
	const int *secondPtr = dataVec[14]->getIntConst(0, maxIndex, secondBuffer.data());
	const int *minutePtr = dataVec[15]->getIntConst(0, maxIndex, minuteBuffer.data());
	const int *dateTimePtr = dataVec[16]->getIntConst(0, maxIndex, dateBuffer.data());
	const int *datehourPtr = dataVec[17]->getIntConst(0, maxIndex, datehourBuffer.data());
	const int *iidPtr = dataVec[18]->getIntConst(0, maxIndex, datehourBuffer.data());
	for (int i = 0; i < dataRow; ++i) {
		if (i % maxIndex == 0) {
			int getSize = min(i - i / maxIndex * maxIndex, maxIndex);
			boolPtr = dataVec[0]->getBoolConst(i, getSize, boolBuffer.data());
			charPtr = dataVec[1]->getCharConst(i, getSize, charBuffer.data());
			shortPtr = dataVec[2]->getShortConst(i, getSize, shortBuffer.data());
			stringPtr = dataVec[3]->getStringConst(i, getSize, (char **)stringBuffer.data());
			longPtr = dataVec[4]->getLongConst(i, getSize, longBuffer.data());
			nanotimePtr = dataVec[5]->getLongConst(i, getSize, nanotimeBuffer.data());
			nanotimestampPtr = dataVec[6]->getLongConst(i, getSize, nanotimestampBuffer.data());
			timestampPtr = dataVec[7]->getLongConst(i, getSize, timestampBuffer.data());
			floatPtr = dataVec[8]->getFloatConst(i, getSize, floatBuffer.data());
			doublePtr = dataVec[9]->getDoubleConst(i, getSize, doubleBuffer.data());
			intPtr = dataVec[10]->getIntConst(i, getSize, intBuffer.data());
			datePtr = dataVec[11]->getIntConst(i, getSize, dateBuffer.data());
			monthPtr = dataVec[12]->getIntConst(i, getSize, monthBuffer.data());
			timePtr = dataVec[13]->getIntConst(i, getSize, timeBuffer.data());
			secondPtr = dataVec[14]->getIntConst(i, getSize, secondBuffer.data());
			minutePtr = dataVec[15]->getIntConst(i, getSize, minuteBuffer.data());
			dateTimePtr = dataVec[16]->getIntConst(i, getSize, dateBuffer.data());
			datehourPtr = dataVec[17]->getIntConst(i, getSize, datehourBuffer.data());
			iidPtr = dataVec[18]->getIntConst(i, getSize, datehourBuffer.data());
		}
		btw.insert(dbName, tableName, Util::createInt(id),
			Util::createBool(boolPtr[i % maxIndex]),
			Util::createChar(charPtr[i % maxIndex]),
			Util::createShort(shortPtr[i%maxIndex]),
			Util::createString(string((char *)(stringPtr[i % maxIndex]))),
			Util::createLong(longPtr[i % maxIndex]),
			Util::createNanoTime(nanotimePtr[i % maxIndex]),
			Util::createNanoTimestamp(nanotimestampPtr[i % maxIndex]),
			Util::createTimestamp(timestampPtr[i % maxIndex]),
			Util::createFloat(floatPtr[i % maxIndex]),
			Util::createDouble(doublePtr[i % maxIndex]),
			Util::createInt(intPtr[i % maxIndex]),
			Util::createDate(datePtr[i % maxIndex]),
			Util::createMonth(monthPtr[i % maxIndex]),
			Util::createTime(timePtr[i % maxIndex]),
			Util::createSecond(secondPtr[i % maxIndex]),
			Util::createMinute(minutePtr[i % maxIndex]),
			Util::createDateTime(dateTimePtr[i % maxIndex]),
			Util::createDateHour(datehourPtr[i % maxIndex]),
			Util::createInt(iidPtr[i % maxIndex])
		);
	}
	connNew.close();
}

static void test_batchTableWriter_insert_thread_fuction_using_cpp_type(int id, BatchTableWriter &btw, string dbName, string tableName,
	TableSP data) {
	DBConnection connNew;
	connNew.connect(hostName, port, "admin", "123456");
	size_t dataRow = data->rows();
	size_t dataColumn = data->columns();
	vector<VectorSP> dataVec;
	for (int i = 0; i < dataColumn; ++i) {
		dataVec.push_back(data->getColumn(i));
	}
	vector<char> boolBuffer(10);
	vector<char> charBuffer(10);
	vector<short> shortBuffer(10);
	vector<char*> stringBuffer(10);
	vector<long long> longBuffer(10);
	vector<long long> nanotimeBuffer(10);
	vector<long long> nanotimestampBuffer(10);
	vector<long long> timestampBuffer(10);
	vector<float> floatBuffer(10);
	vector<double> doubleBuffer(10);
	vector<int> intBuffer(10);
	vector<int> dateBuffer(10);
	vector<int> monthBuffer(10);
	vector<int> timeBuffer(10);
	vector<int> secondBuffer(10);
	vector<int> minuteBuffer(10);
	vector<int> datetimeBuffer(10);
	vector<int> datehourBuffer(10);
	vector<int> iidBuffer(10);
	vector<char*>stringCPPBuffer(10);

	const int maxIndex = 10;

	const char *boolPtr = dataVec[0]->getBoolConst(0, maxIndex, boolBuffer.data());
	const char *charPtr = dataVec[1]->getCharConst(0, maxIndex, charBuffer.data());
	const short *shortPtr = dataVec[2]->getShortConst(0, maxIndex, shortBuffer.data());
	char **stringPtr = dataVec[3]->getStringConst(0, maxIndex, (char **)stringBuffer.data());
	const long long *longPtr = dataVec[4]->getLongConst(0, maxIndex, longBuffer.data());
	const long long *nanotimePtr = dataVec[5]->getLongConst(0, maxIndex, nanotimeBuffer.data());
	const long long *nanotimestampPtr = dataVec[6]->getLongConst(0, maxIndex, nanotimestampBuffer.data());
	const long long *timestampPtr = dataVec[7]->getLongConst(0, maxIndex, timestampBuffer.data());
	const float *floatPtr = dataVec[8]->getFloatConst(0, maxIndex, floatBuffer.data());
	const double *doublePtr = dataVec[9]->getDoubleConst(0, maxIndex, doubleBuffer.data());
	const int *intPtr = dataVec[10]->getIntConst(0, maxIndex, intBuffer.data());
	const int *datePtr = dataVec[11]->getIntConst(0, maxIndex, dateBuffer.data());
	const int *monthPtr = dataVec[12]->getIntConst(0, maxIndex, monthBuffer.data());
	const int *timePtr = dataVec[13]->getIntConst(0, maxIndex, timeBuffer.data());
	const int *secondPtr = dataVec[14]->getIntConst(0, maxIndex, secondBuffer.data());
	const int *minutePtr = dataVec[15]->getIntConst(0, maxIndex, minuteBuffer.data());
	const int *dateTimePtr = dataVec[16]->getIntConst(0, maxIndex, dateBuffer.data());
	const int *datehourPtr = dataVec[17]->getIntConst(0, maxIndex, datehourBuffer.data());
	const int *iidPtr = dataVec[18]->getIntConst(0, maxIndex, datehourBuffer.data());
	char **stringCPPPtr = dataVec[3]->getStringConst(0, maxIndex, (char **)stringCPPBuffer.data());
	for (int i = 0; i < dataRow; ++i) {
		if (i % maxIndex == 0) {
			int getSize = min(i - i / maxIndex * maxIndex, maxIndex);
			boolPtr = dataVec[0]->getBoolConst(i, getSize, boolBuffer.data());
			charPtr = dataVec[1]->getCharConst(i, getSize, charBuffer.data());
			shortPtr = dataVec[2]->getShortConst(i, getSize, shortBuffer.data());
			stringPtr = dataVec[3]->getStringConst(i, getSize, (char **)stringBuffer.data());
			longPtr = dataVec[4]->getLongConst(i, getSize, longBuffer.data());
			nanotimePtr = dataVec[5]->getLongConst(i, getSize, nanotimeBuffer.data());
			nanotimestampPtr = dataVec[6]->getLongConst(i, getSize, nanotimestampBuffer.data());
			timestampPtr = dataVec[7]->getLongConst(i, getSize, timestampBuffer.data());
			floatPtr = dataVec[8]->getFloatConst(i, getSize, floatBuffer.data());
			doublePtr = dataVec[9]->getDoubleConst(i, getSize, doubleBuffer.data());
			intPtr = dataVec[10]->getIntConst(i, getSize, intBuffer.data());
			datePtr = dataVec[11]->getIntConst(i, getSize, dateBuffer.data());
			monthPtr = dataVec[12]->getIntConst(i, getSize, monthBuffer.data());
			timePtr = dataVec[13]->getIntConst(i, getSize, timeBuffer.data());
			secondPtr = dataVec[14]->getIntConst(i, getSize, secondBuffer.data());
			minutePtr = dataVec[15]->getIntConst(i, getSize, minuteBuffer.data());
			dateTimePtr = dataVec[16]->getIntConst(i, getSize, dateBuffer.data());
			datehourPtr = dataVec[17]->getIntConst(i, getSize, datehourBuffer.data());
			iidPtr = dataVec[18]->getIntConst(i, getSize, datehourBuffer.data());
			stringCPPPtr = dataVec[3]->getStringConst(i, getSize, (char **)stringCPPBuffer.data());
		}
		btw.insert(dbName, tableName, id,
			boolPtr[i%maxIndex],
			charPtr[i%maxIndex],
			shortPtr[i%maxIndex],
			(const char*)(stringPtr[i%maxIndex]),
			longPtr[i%maxIndex],
			nanotimePtr[i%maxIndex],
			nanotimestampPtr[i%maxIndex],
			timestampPtr[i%maxIndex],
			floatPtr[i%maxIndex],
			doublePtr[i%maxIndex],
			intPtr[i%maxIndex],
			datePtr[i%maxIndex],
			monthPtr[i%maxIndex],
			timePtr[i%maxIndex],
			secondPtr[i%maxIndex],
			minutePtr[i%maxIndex],
			dateTimePtr[i%maxIndex],
			datehourPtr[i%maxIndex],
			iidPtr[i%maxIndex],
			string(stringCPPPtr[i%maxIndex])
		);
	}
	connNew.close();
}

static bool stopFlag = false;

static void batchTableWriter_thread_getAllStatus(BatchTableWriter &bts) {
	while (!stopFlag) {
		this_thread::sleep_for(chrono::milliseconds(10000));
		TableSP t = bts.getAllStatus();
	}
}

static void batchTableWriter_thread_getStatus(BatchTableWriter &bts, string dbName, string tableName) {
	while (!stopFlag) {
		this_thread::sleep_for(chrono::milliseconds(10000));
		std::tuple<int, bool, bool> t = bts.getStatus(dbName, tableName);
	}
}


// static void
// test_batchTableWriter_insert_unMultithread(BatchTableWriter &btw, string dbName, string tableName, bool partitionTable,
// 	string assertStr, int testRow) {

// 	string script;
// 	DBConnection connNew(false, false);
// 	connNew.connect(hostName, port, "admin", "123456");

// 	script += "login('admin', '123456');";
// 	script += "dbName = '" + dbName + "';";
// 	script += "tableName = '" + tableName + "';";
// 	if (tableName != "") {
// 		script += "dbPath = dbName;";
// 		script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
// 		script += "db=database(dbPath,VALUE,1..100);";
// 		script += "t=table(100:0,`id`dbbool `dbchar `dbshort  `dbstring `dblong `dbnanotime `dbnanotimestamp `dbtimestamp `dbfloat `dbdouble `dbint `dbdate `dbmonth `dbtime `dbsecond `dbminute `dbdatetime `dbdatehour `iid, [INT, BOOL, CHAR, SHORT, STRING, LONG, NANOTIME, NANOTIMESTAMP, TIMESTAMP, FLOAT, DOUBLE, INT, DATE, MONTH, TIME, SECOND, MINUTE, DATETIME, DATEHOUR, INT]);";
// 		if (partitionTable)
// 			script += "db.createPartitionedTable(t,tableName,`id).append!(t);";
// 		else
// 			script += "db.createTable(t,tableName).append!(t);";
// 	}
// 	else
// 		script +=
// 		"share table(100:0,`id`dbbool `dbchar `dbshort `dbstring `dblong `dbnanotime `dbnanotimestamp `dbtimestamp `dbfloat `dbdouble `dbint `dbdate `dbmonth `dbtime `dbsecond `dbminute `dbdatetime `dbdatehour `iid, [INT, BOOL, CHAR, SHORT, STRING, LONG, NANOTIME, NANOTIMESTAMP, TIMESTAMP, FLOAT, DOUBLE, INT, DATE, MONTH, TIME, SECOND, MINUTE, DATETIME, DATEHOUR, INT]) as " +
// 		dbName + ";";
// 	connNew.run(script);

// 	//string dbName = "dfs://test_batchTableWriter", tableName = "ptable";
// 	btw.addTable(dbName, tableName);

// 	//int testRow = 100000;
// 	string tableLocation = tableName != "" ? "loadTable('" + dbName + "', '" + tableName + "')" : dbName;
// 	connNew.run("testRow = " + to_string(testRow) + ";"
// 		"data = table(\n"
// 		"take([true, false, NULL], testRow) as dbbool, \n"
// 		"take((rand(100, 100) - 50).join([pow(2,7)-1, NULL, 0]), testRow).char() as dbchar, \n"
// 		"take((rand(100, 100) - 50).join([pow(2,15)-1, NULL, 0]), testRow).short() as dbshort, \n"
// 		"take(`AAA`BBB`CCC`中文, testRow) as dbstring, \n"
// 		"take((rand(100, 100) - 50).long().join([pow(2,63)-1, NULL, 0]), testRow) as dblong, \n"
// 		"take((rand(100, 100) - 50).long().join([pow(2,63)-1, NULL, 0]), testRow).nanotime() as dbnanotime,  \n"
// 		"take((rand(100, 100) - 50).long().join([pow(2,63)-1, NULL, 0]), testRow).nanotimestamp() as dbnanotimestamp,  \n"
// 		"take((rand(100, 100) - 50).long().join([pow(2,63)-1, NULL, 0]), testRow).timestamp() as dbtimestamp,\n"
// 		"take(rand(10000000.0, 100).float().join(NULL), testRow) as dbfloat,\n"
// 		"take(rand(10000000.0, 100).join(NULL), testRow) as dbdouble,\n"
// 		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).int() as dbint, \n"
// 		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).date() as dbdate,\n"
// 		"take(rand(100, 100).join([pow(2,31)-1, NULL, 0]), testRow).month() as dbmonth , \n"
// 		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).time() as dbtime, \n"
// 		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).second() as dbsecond, \n"
// 		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).minute() as dbminute, \n"
// 		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).datetime() as dbdatetime, \n"
// 		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).datehour() as dbdatehour, \n"
// 		"0.." + to_string(testRow - 1) + " as iid\n"
// 		");");

// 	TableSP data = connNew.run("select * from data order by iid");
// 	int testNum = 8;
// 	for (int i = 0; i < testNum; ++i) {
// 		test_batchTableWriter_insert_thread_fuction(i, btw, dbName, tableName, data);
// 	}

// 	for (int j = 0; j < 200; ++j) {
// 		TableSP t = btw.getAllStatus();
// 		VectorSP sendedRows = t->getColumn(3);
// 		int size = sendedRows->size();
// 		int tmp[4];
// 		sendedRows->getInt(0, size, tmp);
// 		bool flag = true;
// 		for (int i = 0; i < size; ++i) {
// 			if (tmp[i] != testRow * testNum) {
// 				flag = false;
// 			}
// 		}
// 		if (flag)
// 			break;
// 		else {
// 			Until::sleep(1000);
// 		}
// 	}

// 	if (tableName == "")
// 		connNew.run("share data as " + dbName + "Data");
// 	else
// 		connNew.run("share data as " + tableName + "Data");
// }

static void
test_batchTableWriter_insert_multithread_using_CPP_type(BatchTableWriter &btw, string dbName, string tableName, bool partitionTable,
	string assertStr, int testRow) {
	string script;
	DBConnection connNew(false, false);
	connNew.connect(hostName, port, "admin", "123456");
	script += "login('admin', '123456');";
	script += "dbName = '" + dbName + "';";
	script += "tableName = '" + tableName + "';";
	if (tableName != "") {
		script += "dbPath = dbName;";
		script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
		script += "db=database(dbPath,VALUE,1..100);";
		script += "t=table(100:0,`id`dbbool `dbchar `dbshort  `dbstring `dblong `dbnanotime `dbnanotimestamp `dbtimestamp `dbfloat `dbdouble `dbint `dbdate `dbmonth `dbtime `dbsecond `dbminute `dbdatetime `dbdatehour `iid `CStinrg, [INT, BOOL, CHAR, SHORT, STRING, LONG, NANOTIME, NANOTIMESTAMP, TIMESTAMP, FLOAT, DOUBLE, INT, DATE, MONTH, TIME, SECOND, MINUTE, DATETIME, DATEHOUR, INT, STRING]);";
		if (partitionTable)
			script += "db.createPartitionedTable(t,tableName,`id).append!(t);";
		else
			script += "db.createTable(t,tableName).append!(t);";
	}
	else
		script +=
		"share table(100:0,`id`dbbool `dbchar `dbshort `dbstring `dblong `dbnanotime `dbnanotimestamp `dbtimestamp `dbfloat `dbdouble `dbint `dbdate `dbmonth `dbtime `dbsecond `dbminute `dbdatetime `dbdatehour `iid`CStinrg, [INT, BOOL, CHAR, SHORT, STRING, LONG, NANOTIME, NANOTIMESTAMP, TIMESTAMP, FLOAT, DOUBLE, INT, DATE, MONTH, TIME, SECOND, MINUTE, DATETIME, DATEHOUR, INT, STRING]) as " +
		dbName + ";";
	connNew.run(script);
	//string dbName = "dfs://test_batchTableWriter", tableName = "ptable";
	btw.addTable(dbName, tableName);
	//int testRow = 100000;
	string tableLocation = tableName != "" ? "loadTable('" + dbName + "', '" + tableName + "')" : dbName;
	connNew.run("testRow = " + to_string(testRow) + ";"
		"data = table(\n"
		"take([true, false, NULL], testRow) as dbbool,\n"
		"take((rand(100, 100) - 50).join([pow(2,7)-1, NULL, 0]), testRow).char() as dbchar,\n"
		"take((rand(100, 100) - 50).join([pow(2,15)-1, NULL, 0]), testRow).short() as dbshort,\n"
		"take(`AAA`BBB`CCC`中文, testRow) as dbstring,\n"
		"take((rand(100, 100) - 50).long().join([pow(2,63)-1, NULL, 0]), testRow) as dblong,\n"
		"take((rand(100, 100) - 50).long().join([pow(2,63)-1, NULL, 0]), testRow).nanotime() as dbnanotime,\n"
		"take((rand(100, 100) - 50).long().join([pow(2,63)-1, NULL, 0]), testRow).nanotimestamp() as dbnanotimestamp,\n"
		"take((rand(100, 100) - 50).long().join([pow(2,63)-1, NULL, 0]), testRow).timestamp() as dbtimestamp,\n"
		"take(rand(10000000.0, 100).float().join(NULL), testRow) as dbfloat,\n"
		"take(rand(10000000.0, 100).join(NULL), testRow) as dbdouble,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).int() as dbint,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).date() as dbdate,\n"
		"take(rand(100, 100).join([pow(2,31)-1, NULL, 0]), testRow).month() as dbmonth ,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).time() as dbtime,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).second() as dbsecond,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).minute() as dbminute,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).datetime() as dbdatetime,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).datehour() as dbdatehour,\n"
		"0.." + to_string(testRow - 1) + " as iid,\n"
		"take(`AAA`BBB`CCC`中文, testRow) as CStinrg\n"
		");");
	TableSP data = connNew.run("select * from data order by iid");
	stopFlag = false;
	thread getAllStatus = thread(batchTableWriter_thread_getAllStatus, ref(btw));
	thread getStatus = thread(batchTableWriter_thread_getStatus, ref(btw), dbName, tableName);
	const int threadNum = 8;
	thread threadVec[threadNum];
	for (int i = 0; i < threadNum; ++i) {
		threadVec[i] = thread(test_batchTableWriter_insert_thread_fuction_using_cpp_type, i, ref(btw), dbName, tableName, data);
	}
	for (int j = 0; j < 200; ++j) {
		TableSP t = btw.getAllStatus();
		VectorSP sendedRows = t->getColumn(3);
		int size = sendedRows->size();
		int tmp[4];
		sendedRows->getInt(0, size, tmp);
		bool flag = true;
		for (int i = 0; i < size; ++i) {
			if (tmp[i] != testRow * threadNum) {
				flag = false;
			}
		}
		if (flag)
			break;
		else {
			this_thread::sleep_for(chrono::seconds(1));
		}
	}
	if (tableName == "")
		connNew.run("share data as '" + dbName + "Data'");
	else
		connNew.run("share data as '" + tableName + "Data'");
	for (int i = 0; i < threadNum; ++i) {
		threadVec[i].join();
	}
	connNew.close();
	stopFlag = true;
	//pthread_cancel(getAllStatus.native_handle());
	getAllStatus.join();
	//pthread_cancel(getStatus.native_handle());
	getStatus.join();
}

TEST_F(BatchTableWriterTest,test_batchTableWriter_insert_multithread_dfsTable_using_CPP_type){
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	test_batchTableWriter_insert_multithread_using_CPP_type(ref(btw), "dfs://batchTableWriter", "batchTableWriter", true,
		"test_batchTableWriter_insert_multithread_dfsTable_using_CPP_type", 100000);
	ConstantSP ret = conn.run("flag = array(BOOL, 0, 10);"
		"ret = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid, CStinrg from batchTableWriterData order by iid;"
		"equalRet = eqObj((select * from ret order by iid).values(), (select * from objByName(\"batchTableWriterData\") order by iid).values());"
		"flag.append!(equalRet);"
		"writeLog('cpp_api_test_batchTableWriter_insert_multithread_dfsTable_using_CPP_type = ' + flag.all());flag.all();");
	if (assertObj)
		EXPECT_EQ((bool)(ret->getBool()), true);
}

static void
test_batchTableWriter_insert_multithread(BatchTableWriter &btw, string dbName, string tableName, bool partitionTable,
	string assertStr, int testRow) {
	string script;
	DBConnection connNew(false, false);
	connNew.connect(hostName, port, "admin", "123456");
	script += "login('admin', '123456');";
	script += "dbName = '" + dbName + "';";
	script += "tableName = '" + tableName + "';";
	if (tableName != "") {
		script += "dbPath = dbName;";
		script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
		script += "db=database(dbPath,VALUE,1..100);";
		script += "t=table(100:0,`id`dbbool `dbchar `dbshort  `dbstring `dblong `dbnanotime `dbnanotimestamp `dbtimestamp `dbfloat `dbdouble `dbint `dbdate `dbmonth `dbtime `dbsecond `dbminute `dbdatetime `dbdatehour `iid, [INT, BOOL, CHAR, SHORT, STRING, LONG, NANOTIME, NANOTIMESTAMP, TIMESTAMP, FLOAT, DOUBLE, INT, DATE, MONTH, TIME, SECOND, MINUTE, DATETIME, DATEHOUR, INT]);";
		if (partitionTable)
			script += "db.createPartitionedTable(t,tableName,`id).append!(t);";
		else
			script += "db.createTable(t,tableName).append!(t);";
	}
	else
		script +=
		"share table(100:0,`id`dbbool `dbchar `dbshort `dbstring `dblong `dbnanotime `dbnanotimestamp `dbtimestamp `dbfloat `dbdouble `dbint `dbdate `dbmonth `dbtime `dbsecond `dbminute `dbdatetime `dbdatehour `iid, [INT, BOOL, CHAR, SHORT, STRING, LONG, NANOTIME, NANOTIMESTAMP, TIMESTAMP, FLOAT, DOUBLE, INT, DATE, MONTH, TIME, SECOND, MINUTE, DATETIME, DATEHOUR, INT]) as " +
		dbName + ";";
	connNew.run(script);
	btw.addTable(dbName, tableName);
	string tableLocation = tableName != "" ? "loadTable('" + dbName + "', '" + tableName + "')" : dbName;
	connNew.run("testRow = " + to_string(testRow) + ";"
		"data = table(\n"
		"take([true, false, NULL], testRow) as dbbool,\n"
		"take((rand(100, 100) - 50).join([pow(2,7)-1, NULL, 0]), testRow).char() as dbchar,\n"
		"take((rand(100, 100) - 50).join([pow(2,15)-1, NULL, 0]), testRow).short() as dbshort,\n"
		"take(`AAA`BBB`CCC`中文, testRow) as dbstring,\n"
		"take((rand(100, 100) - 50).long().join([pow(2,63)-1, NULL, 0]), testRow) as dblong,\n"
		"take((rand(100, 100) - 50).long().join([pow(2,63)-1, NULL, 0]), testRow).nanotime() as dbnanotime,\n"
		"take((rand(100, 100) - 50).long().join([pow(2,63)-1, NULL, 0]), testRow).nanotimestamp() as dbnanotimestamp,\n"
		"take((rand(100, 100) - 50).long().join([pow(2,63)-1, NULL, 0]), testRow).timestamp() as dbtimestamp,\n"
		"take(rand(10000000.0, 100).float().join(NULL), testRow) as dbfloat,\n"
		"take(rand(10000000.0, 100).join(NULL), testRow) as dbdouble,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).int() as dbint,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).date() as dbdate,\n"
		"take(rand(100, 100).join([pow(2,31)-1, NULL, 0]), testRow).month() as dbmonth ,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).time() as dbtime,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).second() as dbsecond,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).minute() as dbminute,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).datetime() as dbdatetime,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).datehour() as dbdatehour,\n"
		"0.." + to_string(testRow - 1) + " as iid\n"
		");");
	TableSP data = connNew.run("select * from data order by iid");
	stopFlag = false;
	thread getAllStatus = thread(batchTableWriter_thread_getAllStatus, ref(btw));
	thread getStatus = thread(batchTableWriter_thread_getStatus, ref(btw), dbName, tableName);
	const int threadNum = 4;
	thread threadVec[threadNum];
	for (int i = 0; i < threadNum; ++i) {
		threadVec[i] = thread(test_batchTableWriter_insert_thread_fuction, i, ref(btw), dbName, tableName, data);
	}
	for (int j = 0; j < 200; ++j) {
		TableSP t = btw.getAllStatus();
		VectorSP sendedRows = t->getColumn(3);
		int size = sendedRows->size();
		int tmp[4];
		sendedRows->getInt(0, size, tmp);
		bool flag = true;
		for (int i = 0; i < size; ++i) {
			if (tmp[i] != testRow * threadNum) {
				flag = false;
			}
		}
		if (flag)
			break;
		else {
			this_thread::sleep_for(chrono::seconds(1));
		}
	}
	if (tableName == "")
		connNew.run("share data as " + dbName + "Data");
	else
		connNew.run("share data as " + tableName + "Data");
	for (int i = 0; i < threadNum; ++i) {
		threadVec[i].join();
	}
	connNew.close();
	
	stopFlag = true;
	//pthread_cancel(getAllStatus.native_handle());
	getAllStatus.join();
	//pthread_cancel(getStatus.native_handle());
	getStatus.join();
}

// TEST_F(BatchTableWriterTest,test_batchTableWriter_insert_unMultithread_dfsTable){

// 	BatchTableWriter btw(hostName, port, "admin", "123456", false);

// 	test_batchTableWriter_insert_unMultithread(btw,
// 		"dfs://batchTableWriter" , "batchTableWriter" , true,
// 		"test_batchTableWriter_insert_multithread_dfsTable", 100000);

// 	ConstantSP ret = conn.run("flag = array(BOOL, 0, 10);"
// 		"login(\"admin\",\"123456\");"
// 		"for(idnum in 0..3){"
// 		"for(i in 0..3){"
// 		"ret = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from loadTable(\"dfs://batchTableWriter\" + idnum, \"batchTableWriter\" + idnum ) where id = i order by iid;"
// 		"equalRet = eqObj((select * from ret order by iid).values(), (select * from objByName(\"batchTableWriter\" + idnum + \"Data\") order by iid).values());"
// 		"flag.append!(equalRet);"
// 		"}"
// 		"}; writeLog('cpp_api_test_batchTableWriter_insert_unMultithread_dfsTable = ' + flag.all());flag.all();");
// 	if (assertObj)
// 		EXPECT_EQ((bool)(ret->getBool()), true);

// }

// TEST_F(BatchTableWriterTest,test_batchTableWriter_insert_unMultithread_memoryTable){
// 	BatchTableWriter btw(hostName, port, "admin", "123456", false);
// 	for (int i = 0; i < 4; ++i) {
// 		test_batchTableWriter_insert_unMultithread(ref(btw), "batchTableWriter" + to_string(i), "",
// 			true, "test_batchTableWriter_insert_multithread_memoryTable" + to_string(i), 100000);
// 	}
// 	ConstantSP ret = conn.run("flag = array(BOOL, 0, 10);"
// 		"login(\"admin\",\"123456\");"
// 		"for(idnum in 0..3){"
// 		"for(i in 0..3){"
// 		"ret = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from objByName(\"batchTableWriter\" + idnum) where id = i order by iid;"
// 		"equalRet = eqObj((select * from ret order by iid).values(), (select * from objByName(\"batchTableWriter\" + idnum + \"Data\") order by iid).values());  \n"
// 		"flag.append!(equalRet);"
// 		"}"
// 		"}"
// 		"writeLog('cpp_api_test_batchTableWriter_insert_unMultithread_memoryTable = ' + flag.all());flag.all();");
// 	if (assertObj)
// 		EXPECT_EQ((bool)(ret->getBool()), true);
// }

// TEST_F(BatchTableWriterTest,test_batchTableWriter_insert_unMultithread_latitudeTable){
// 	BatchTableWriter btw(hostName, port, "admin", "123456", true);
// 	for (int i = 0; i < 4; ++i) {
// 		test_batchTableWriter_insert_unMultithread(ref(btw),
// 			"dfs://batchTableWriter" + to_string(i), "batchTableWriter" + to_string(i), true,
// 			"test_batchTableWriter_insert_multithread_latitudeTable" + to_string(i), 100000);
// 	}
// 	ConstantSP ret = conn.run("flag = array(BOOL, 0, 10);"
// 		"login(\"admin\",\"123456\");"
// 		"for(idnum in 0..3){"
// 		"for(i in 0..7){"
// 		"ret = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from loadTable(\"dfs://batchTableWriter\" + idnum, \"batchTableWriter\" + idnum ) where id = i order by iid;"
// 		"equalRet = eqObj((select * from ret order by iid).values(), (select * from objByName(\"batchTableWriter\" + idnum + \"Data\") order by iid).values());"
// 		"flag.append!(equalRet);"
// 		"}"
// 		"}"
// 		"writeLog('cpp_api_test_batchTableWriter_insert_unMultithread_latitudeTable = ' + flag.all());flag.all();");
// 	if (assertObj)
// 		EXPECT_EQ((bool)(ret->getBool()), true);
// }


TEST_F(BatchTableWriterTest,test_batchTableWriter_insert_multithread_one_dfsTable){
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	test_batchTableWriter_insert_multithread(ref(btw), "dfs://batchTableWriter", "batchTableWriter", true,
		"test_batchTableWriter_insert_multithread_one_dfsTable", 100000);
	ConstantSP ret = conn.run("flag = array(BOOL, 0, 10);"
		"login(\"admin\",\"123456\");"
		"ret = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from batchTableWriterData order by iid;"
		"equalRet = eqObj((select * from ret order by iid).values(), (select * from objByName(\"batchTableWriterData\") order by iid).values());  \n"
		"flag.append!(equalRet);"
		"writeLog('cpp_api_test_batchTableWriter_insert_multithread_one_dfsTable = ' + flag.all()); flag.all();");
	if (assertObj)
		EXPECT_EQ((bool)(ret->getBool()), true);
}

TEST_F(BatchTableWriterTest,test_batchTableWriter_insert_multithread_one_memoryTable){
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	test_batchTableWriter_insert_multithread(ref(btw), "batchTableWriter", "", true,
		"test_batchTableWriter_insert_multithread_one_memoryTable", 100000);
	ConstantSP ret = conn.run("flag = array(BOOL, 0, 10);"
		"login(\"admin\",\"123456\");"
		"ret = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from batchTableWriterData order by iid;"
		"equalRet = eqObj((select * from ret order by iid).values(), (select * from objByName(\"batchTableWriterData\") order by iid).values());  \n"
		"flag.append!(equalRet);"
		"writeLog('cpp_api_test_batchTableWriter_insert_multithread_one_memoryTable = ' + flag.all());flag.all();");
	if (assertObj)
		EXPECT_EQ((bool)(ret->getBool()), true);
}

TEST_F(BatchTableWriterTest,test_batchTableWriter_insert_multithread_one_latitudeTable){
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	test_batchTableWriter_insert_multithread(ref(btw), "dfs://batchTableWriter", "batchTableWriter", false,
		"test_batchTableWriter_insert_multithread_one_latitudeTable", 100000);
	ConstantSP ret = conn.run("flag = array(BOOL, 0, 10);"
		"login(\"admin\",\"123456\");"
		"ret = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from batchTableWriterData order by iid;"
		"equalRet = eqObj((select * from ret order by iid).values(), (select * from objByName(\"batchTableWriterData\") order by iid).values());"
		"flag.append!(equalRet);"
		"writeLog('cpp_api_test_test_batchTableWriter_insert_multithread_one_latitudeTable = ' + flag.all());flag.all();");
	if (assertObj)
		EXPECT_EQ((bool)(ret->getBool()), true);
}

TEST_F(BatchTableWriterTest,test_batchTableWriter_insert_multithread_dfsTable){

	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	thread threadVec[4];

	for (int i = 0; i < 4; ++i) {
		threadVec[i] = thread(test_batchTableWriter_insert_multithread, ref(btw),
			"dfs://batchTableWriter" + to_string(i), "batchTableWriter" + to_string(i), true,
			"test_batchTableWriter_insert_multithread_dfsTable" + to_string(i), 100000);
	}
	for (int i = 0; i < 4; ++i) {
		threadVec[i].join();
	}
	ConstantSP ret = conn.run("flag = array(BOOL, 0, 10);"
		"login(\"admin\",\"123456\");"
		"ret1 = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from batchTableWriter0Data  order by iid;"
		"equalRet1 = eqObj((select * from ret1 order by iid).values(), (select * from objByName(\"batchTableWriter0Data\") order by iid).values());  \n"
		"ret2 = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from batchTableWriter1Data  order by iid;"
		"equalRet2 = eqObj((select * from ret2 order by iid).values(), (select * from objByName(\"batchTableWriter1Data\") order by iid).values());  \n"
		"ret3 = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from batchTableWriter2Data  order by iid;"
		"equalRet3 = eqObj((select * from ret3 order by iid).values(), (select * from objByName(\"batchTableWriter2Data\") order by iid).values());  \n"
		"ret4 = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from batchTableWriter3Data  order by iid;"
		"equalRet4 = eqObj((select * from ret4 order by iid).values(), (select * from objByName(\"batchTableWriter3Data\") order by iid).values());  \n"
		"flag.append!(equalRet1);"
		"flag.append!(equalRet2);"
		"flag.append!(equalRet3);"
		"flag.append!(equalRet4);"
		"writeLog('cpp_api_test_batchTableWriter_insert_multithread_dfsTable = ' + flag.all());flag.all();");
	if (assertObj)
		EXPECT_EQ((bool)(ret->getBool()), true);
}

TEST_F(BatchTableWriterTest,test_batchTableWriter_insert_multithread_memoryTable){
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	thread threadVec[4];
	for (int i = 0; i < 4; ++i) {
		threadVec[i] = thread(test_batchTableWriter_insert_multithread, ref(btw), "batchTableWriter" + to_string(i), "",
			true, "test_batchTableWriter_insert_multithread_memoryTable" + to_string(i), 100000);
	}
	for (int i = 0; i < 4; ++i) {
		threadVec[i].join();
	}
	ConstantSP ret = conn.run("flag = array(BOOL, 0, 10);"
		"login(\"admin\",\"123456\");"
		"ret1 = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from batchTableWriter0Data  order by iid;"
		"equalRet1 = eqObj((select * from ret1 order by iid).values(), (select * from objByName(\"batchTableWriter0Data\") order by iid).values());  \n"
		"ret2 = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from batchTableWriter1Data  order by iid;"
		"equalRet2 = eqObj((select * from ret2 order by iid).values(), (select * from objByName(\"batchTableWriter1Data\") order by iid).values());  \n"
		"ret3 = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from batchTableWriter2Data  order by iid;"
		"equalRet3 = eqObj((select * from ret3 order by iid).values(), (select * from objByName(\"batchTableWriter2Data\") order by iid).values());  \n"
		"ret4 = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from batchTableWriter3Data  order by iid;"
		"equalRet4 = eqObj((select * from ret4 order by iid).values(), (select * from objByName(\"batchTableWriter3Data\") order by iid).values());  \n"
		"flag.append!(equalRet1);"
		"flag.append!(equalRet2);"
		"flag.append!(equalRet3);"
		"flag.append!(equalRet4);"
		"writeLog('cpp_api_test_batchTableWriter_insert_multithread_dfsTable = ' + flag.all());flag.all();");
	if (assertObj)
		EXPECT_EQ((bool)(ret->getBool()), true);
}

TEST_F(BatchTableWriterTest,test_batchTableWriter_insert_multithread_latitudeTable){
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	thread threadVec[4];
	for (int i = 0; i < 4; ++i) {
		threadVec[i] = thread(test_batchTableWriter_insert_multithread, ref(btw),
			"dfs://batchTableWriter" + to_string(i), "batchTableWriter" + to_string(i), true,
			"test_batchTableWriter_insert_multithread_latitudeTable" + to_string(i), 100000);
	}
	for (int i = 0; i < 4; ++i) {
		threadVec[i].join();
	}

	ConstantSP ret = conn.run("flag = array(BOOL, 0, 10);"
		"login(\"admin\",\"123456\");"
		"ret1 = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from batchTableWriter0Data  order by iid;"
		"equalRet1 = eqObj((select * from ret1 order by iid).values(), (select * from objByName(\"batchTableWriter0Data\") order by iid).values());  \n"
		"ret2 = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from batchTableWriter1Data  order by iid;"
		"equalRet2 = eqObj((select * from ret2 order by iid).values(), (select * from objByName(\"batchTableWriter1Data\") order by iid).values());  \n"
		"ret3 = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from batchTableWriter2Data  order by iid;"
		"equalRet3 = eqObj((select * from ret3 order by iid).values(), (select * from objByName(\"batchTableWriter2Data\") order by iid).values());  \n"
		"ret4 = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from batchTableWriter3Data  order by iid;"
		"equalRet4 = eqObj((select * from ret4 order by iid).values(), (select * from objByName(\"batchTableWriter3Data\") order by iid).values());  \n"
		"flag.append!(equalRet1);"
		"flag.append!(equalRet2);"
		"flag.append!(equalRet3);"
		"flag.append!(equalRet4);"
		"writeLog('cpp_api_test_batchTableWriter_insert_multithread_dfsTable = ' + flag.all());flag.all();");
	if (assertObj)
		EXPECT_EQ((bool)(ret->getBool()), true);
}

TEST_F(BatchTableWriterTest,test_batchTableWriter_getAllStatus){
	string script;
	script += "dbPath = 'dfs://test_batchTableWriter';";
	script += "login('admin', '123456');go;";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "share table(100:0,`test1 `test2, [INT,INT]) as teststatus;";
	conn.run(script);
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	TableSP status = btw.getAllStatus();
	vector<string> colName;
	int size = status->columns();
	for (int i = 0; i < size; ++i) {
		colName.push_back(status->getColumnName(i));
	}
	if (colName[0] != "DatabaseName")
		EXPECT_EQ(colName[0], string("DatabaseName"));
	if (colName[1] != "TableName")
		EXPECT_EQ(colName[1], string("TableName"));
	if (colName[2] != "WriteQueueDepth")
		EXPECT_EQ(colName[2], string("WriteQueueDepth"));
	if (colName[3] != "SendedRows")
		EXPECT_EQ(colName[3], string("SendedRows"));
	if (colName[4] != "Removing")
		EXPECT_EQ(colName[4], string("Removing"));
	if (colName[5] != "Finished")
		EXPECT_EQ(colName[5], string("Finished"));
	if (status->getColumn(0)->getType() != DT_STRING)
		EXPECT_EQ(status->getColumn(0)->getType(), DT_STRING);
	if (status->getColumn(1)->getType() != DT_STRING)
		EXPECT_EQ(status->getColumn(1)->getType(), DT_STRING);
	if (status->getColumn(2)->getType() != DT_INT)
		EXPECT_EQ(status->getColumn(2)->getType(), DT_INT);
	if (status->getColumn(3)->getType() != DT_INT)
		EXPECT_EQ(status->getColumn(3)->getType(), DT_INT);
	if (status->getColumn(4)->getType() != DT_BOOL)
		EXPECT_EQ(status->getColumn(4)->getType(), DT_BOOL);
	if (status->getColumn(5)->getType() != DT_BOOL)
		EXPECT_EQ(status->getColumn(5)->getType(), DT_BOOL);
}

TEST_F(BatchTableWriterTest,test_batchTableWriter_getStatus_exception){
	string script;
	script += "login('admin', '123456');go;";
	script += "dbPath = 'dfs://test_batchTableWriter';";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "share table(100:0,`test1 `test2, [INT,INT]) as teststatus;";
	conn.run(script);
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	try {
		std::tuple<int, bool, bool> status = btw.getStatus("dfs://test_batchTableWriter_no_exist", "");
	}
	catch (exception& e) {
		cout<<e.what()<<endl;
		EXPECT_STREQ(e.what(),"Failed to get queue depth. Please use addTable to add infomation of database and table first.");
	}
}

static void test_fuc_addTable(BatchTableWriter &btw, string dbName, string tableName, int cycles) {
	try {
		for (int i = 0; i < cycles; ++i) {
			btw.addTable(dbName, tableName);
		}
	}
	catch (exception &e) {
		cout<<e.what()<<endl;
		cout<<"FAILED--test_fuc_addTable"<<endl;
		EXPECT_EQ(1,2);
	}
}

static void test_fuc_removeTable(BatchTableWriter &btw, string dbName, string tableName, int cycles) {
	try {
		for (int i = 0; i < cycles; ++i) {
			btw.removeTable(dbName, tableName);
		}
	}
	catch (exception &e) {
		cout<<e.what()<<endl;
		cout<<"FAILED--test_fuc_removeTable"<<endl;
		EXPECT_EQ(1,2);
	}
	
}

static void test_fuc_getStatus(BatchTableWriter &btw, string dbName, string tableName, int cycles) {
	try {
		for (int i = 0; i < cycles; ++i) {
			std::tuple<int, bool, bool> status =btw.getStatus(dbName, tableName);
			EXPECT_EQ(get<0>(status),0);
		}
	}
	catch (exception &e) {
		cout<<e.what()<<endl;
		cout<<"FAILED--test_fuc_getStatus"<<endl;
		EXPECT_EQ(1,2);
	}
}

static void test_fuc_getAllStatus(BatchTableWriter &btw, int cycles) {	
	try {
		for (int i = 0; i < cycles; ++i) {
			TableSP allStatus = btw.getAllStatus();
			for(int j=0;j<allStatus->rows();j++)
			EXPECT_EQ(allStatus->getColumn(2)->get(j)->getInt(),0);
		}
	}
	catch (exception &e) {
		cout<<e.what()<<endl;
		cout<<"FAILED--test_fuc_getAllStatus"<<endl;
		EXPECT_EQ(1,2);			
	}
}

TEST_F(BatchTableWriterTest,test_multithread){
	string script;
	script += "login('admin', '123456');go;";
	script += "dbPath = 'dfs://test_batchTableWriter';";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db=database(dbPath,VALUE,1..100);";
	script += "t=table(100:0,`id`dbbool `dbchar `dbshort  `dbstring `dblong `dbnanotime `dbnanotimestamp `dbtimestamp `dbfloat `dbdouble `dbint `dbdate `dbmonth `dbtime `dbsecond `dbminute `dbdatetime `dbdatehour `iid, [INT, BOOL, CHAR, SHORT, STRING, LONG, NANOTIME, NANOTIMESTAMP, TIMESTAMP, FLOAT, DOUBLE, INT, DATE, MONTH, TIME, SECOND, MINUTE, DATETIME, DATEHOUR, INT]);";
	script += "db.createPartitionedTable(t,`test_multithread,`id);";
	conn.run(script);
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	string dbName = "dfs://test_batchTableWriter";
	string tableName = "test_multithread";
	int cycles = 10000;
	btw.addTable(dbName, tableName);
	thread t2(test_fuc_getStatus, ref(btw), dbName, tableName, cycles);
	thread t3(test_fuc_getAllStatus, ref(btw), cycles);
	t2.join();
	t3.join();
	thread t4(test_fuc_removeTable, ref(btw), dbName, tableName, cycles);
	t4.join();
}


static void test_BatchTableWriter_create_network() {
	EXPECT_ANY_THROW(BatchTableWriter btw(nullptr, port, "admin", "123456", true));
	EXPECT_ANY_THROW(BatchTableWriter btw(hostName, port, nullptr, "123456", true));
	EXPECT_ANY_THROW(BatchTableWriter btw(hostName, port, "admin", nullptr, true));
	BatchTableWriter btw(hostName, NULL, "admin", "123456", true);//no exception

}

static void test_BatchTableWriter_addTable_network() {
	string script;
	script += "login('admin', '123456');";
	script += "dbPath = 'dfs://test_batchTableWriter';";
	script += "tableName = 'BatchTableWriter';";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db=database(dbPath,VALUE,1..100);";
	script += "t=table(100:0,`id`dbbool `dbchar `dbshort  `dbstring `dblong `dbnanotime `dbnanotimestamp `dbtimestamp `dbfloat `dbdouble `dbint `dbdate `dbmonth `dbtime `dbsecond `dbminute `dbdatetime `dbdatehour `iid, [INT, BOOL, CHAR, SHORT, STRING, LONG, NANOTIME, NANOTIMESTAMP, TIMESTAMP, FLOAT, DOUBLE, INT, DATE, MONTH, TIME, SECOND, MINUTE, DATETIME, DATEHOUR, INT]);";
	script += "db.createPartitionedTable(t,tableName,`id).append!(t);";
	conn.run(script);

	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	try {//broken
		btw.addTable("dfs://test_batchTableWriter", "BatchTableWriter");
		cout << "PASSED--test_BatchTableWriter_addTable_network1" << endl;

	}
	catch (exception &e) {
		cout << "FAIL--test_BatchTableWriter_addTable_network1" << endl;
		cout << e.what() << endl;
		//what():  Failed to connect to server.
	}
	try {//connect
		btw.addTable("dfs://test_batchTableWriter", "BatchTableWriter");
	}
	catch (exception &e) {
		cout << e.what() << endl;
		//what():  Failed to add table, the specified table has not been removed yet.
		cout << "PASSED--test_BatchTableWriter_addTable_network2" << endl;
	}
	try {//connect
		if (btw.getAllStatus()->rows() != 1)
			throw RuntimeException("The allStatus should have 1 row");
		try {
			btw.addTable("dfs://test_batchTableWriter", "BatchTableWriter");
			cout << "FAIL--test_BatchTableWriter_addTable_network3-1" << endl;
		}
		catch (exception& e) {
			cout << "PASSED--test_BatchTableWriter_addTable_network3-1" << endl;
		}
		btw.removeTable("dfs://test_batchTableWriter", "BatchTableWriter");
		if (btw.getAllStatus()->rows() != 0)
			throw RuntimeException("The allStatus should have 0 row");
		string script;
		script += "login('admin', '123456');";
		script += "dbPath = 'dfs://test_batchTableWriter';";
		script += "tableName = 'BatchTableWriter';";
		script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
		script += "db=database(dbPath,VALUE,1..100);";
		script += "t=table(100:0,`id`dbbool `dbchar `dbshort  `dbstring `dblong `dbnanotime `dbnanotimestamp `dbtimestamp `dbfloat `dbdouble `dbint `dbdate `dbmonth `dbtime `dbsecond `dbminute `dbdatetime `dbdatehour `iid, [INT, BOOL, CHAR, SHORT, STRING, LONG, NANOTIME, NANOTIMESTAMP, TIMESTAMP, FLOAT, DOUBLE, INT, DATE, MONTH, TIME, SECOND, MINUTE, DATETIME, DATEHOUR, INT]);";
		script += "db.createPartitionedTable(t,tableName,`id).append!(t);";
		conn.run(script);

		btw.addTable("dfs://test_batchTableWriter", "BatchTableWriter");
		cout << "PASSED--test_BatchTableWriter_addTable_network3" << endl;
		this_thread::sleep_for(chrono::seconds(1));
	}
	catch (exception &e) {
		cout << e.what() << endl;
		cout << "FAIL--test_BatchTableWriter_addTable_network3" << endl;
	}
}

static void test_BatchTableWriter_remove_exception() {
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	EXPECT_ANY_THROW(btw.removeTable(nullptr, "BatchTableWriter"));
	EXPECT_ANY_THROW(btw.removeTable("dfs://test_batchTableWriter", nullptr));
	btw.removeTable("BatchTableWriter", "");//no exception
}

static void test_BatchTableWriter_insert_network() {
	string script;
	string tableName = "test_BatchTableWriter_insert_network";
	string dbName = "dfs://test_batchTableWriter";
	script += "login('admin', '123456');";
	script += "dbPath = \""+dbName+"\";";
	script += "tableName = \""+tableName+"\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db=database(dbPath,HASH,[INT,1]);";
	script += "t=table(100:0,`id`dbbool, [INT, BOOL]);";
	script += "db.createPartitionedTable(t,tableName,`id);";
	conn.run(script);

	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	btw.addTable(dbName,tableName);
	EXPECT_ANY_THROW(btw.insert(dbName,tableName, 1, int(1)));

	btw.insert(dbName,tableName, 1, char(1));
	this_thread::sleep_for(chrono::milliseconds(1000));
	EXPECT_EQ(get<0>(btw.getStatus(dbName,tableName)),0);
	EXPECT_EQ(btw.getUnwrittenData(dbName,tableName)->rows(),0);
	string res = conn.run("exec * from loadTable(\"dfs://test_batchTableWriter\",\"test_BatchTableWriter_insert_network\")")->values()->getString();
	EXPECT_EQ(res,"([1],[1])");
	btw.removeTable(dbName,tableName);
}


static void test_BatchTableWriter_remove_and_insert() {
	//connect
	string script;
	script += "login('admin', '123456');";
	script += "dbPath = 'dfs://test_batchTableWriter';";
	script += "tableName = 'BatchTableWriter';";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db=database(dbPath,VALUE,1..100);";
	script += "t=table(100:0,`id`dbbool, [INT, INT]);";
	script += "db.createPartitionedTable(t,tableName,`id).append!(t);";
	conn.run(script);

	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	btw.addTable("dfs://test_batchTableWriter", "BatchTableWriter");
	this_thread::sleep_for(chrono::seconds(1));
    btw.removeTable("dfs://test_batchTableWriter", "BatchTableWriter");

    ConstantSP ret = conn.run("exec count(*) from loadTable(dbPath, tableName)");
	
	EXPECT_EQ(ret->getInt(),0);
    EXPECT_ANY_THROW(btw.getStatus("dfs://test_batchTableWriter", "BatchTableWriter"));

}


TEST_F(BatchTableWriterTest,test_BatchTableWriter_exception){
	test_BatchTableWriter_create_network();
	cout<<"PASSED--test_BatchTableWriter_creat_network"<<endl;
	test_BatchTableWriter_addTable_network();
	cout<<"PASSED--test_BatchTableWriter_addTable_network"<<endl;
	test_BatchTableWriter_remove_exception();
	cout<<"PASSED--test_BatchTableWriter_remove_exception"<<endl;
	test_BatchTableWriter_insert_network();
	cout<<"PASSED--test_BatchTableWriter_insert_network"<<endl;
	test_BatchTableWriter_remove_and_insert();
	cout<<"PASSED--test_BatchTableWriter_remove_and_insert"<<endl;
}



static void BatchTableWriter_insert_error_type(string destType, BatchTableWriter& btw) {
	string passedStr = "PASSED--test_BatchTableWriter_insert_error_type_";
	string FAILStr = "FAIL--test_BatchTableWriter_insert_error_type_";
	if (destType != "BOOL") {
		try {
			if (destType != "CHAR") {
				btw.insert("batchTableWriter", "", char(1));
				cout << FAILStr + "BOOL_to_" + destType << endl;
			}
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createBool(char(1)));
			cout << FAILStr + "BOOL_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "CHAR") {
		try {
			if (destType != "BOOL") {
				btw.insert("batchTableWriter", "", char(1));
				cout << FAILStr + "CHAR_to_" + destType << endl;
			}
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createChar(char(1)));
			cout << FAILStr + "CHAR_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "SHORT") {
		try {
			btw.insert("batchTableWriter", "", short(1));
			cout << FAILStr + "SHORT_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createShort(short(1)));
			cout << FAILStr + "SHORT_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "STRING") {
		string str = "test";
		try {
			btw.insert("batchTableWriter", "", str);
			cout << FAILStr + "STRING_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", str.c_str());
			cout << FAILStr + "STRING_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createString(str));
			cout << FAILStr + "STRING_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "LONG") {
		try {
			btw.insert("batchTableWriter", "", long(1));
			cout << FAILStr + "LONG_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createLong(long(1)));
			cout << FAILStr + "LONG_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "NANOTIME") {
		try {
			btw.insert("batchTableWriter", "", long(1));
			cout << FAILStr + "NANOTIME_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createNanoTime(long(1)));
			cout << FAILStr + "NANOTIME_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "NANOTIMESTAMP") {
		try {
			btw.insert("batchTableWriter", "", long(1));
			cout << FAILStr + "NANOTIMESTAMP_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createNanoTimestamp(long(1)));
			cout << FAILStr + "NANOTIMESTAMP_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "TIMESTAMP") {
		try {
			btw.insert("batchTableWriter", "", long(1));
			cout << FAILStr + "TIMESTAMP_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createTimestamp(long(1)));
			cout << FAILStr + "TIMESTAMP_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "FLOAT") {
		try {
			btw.insert("batchTableWriter", "", float(1));
			cout << FAILStr + "FLOAT_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createFloat(float(1)));
			cout << FAILStr + "FLOAT_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "DOUBLE") {
		try {
			btw.insert("batchTableWriter", "", double(1));
			cout << FAILStr + "DOUBLE_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createDouble(double(1)));
			cout << FAILStr + "DOUBLE_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "INT") {
		try {
			btw.insert("batchTableWriter", "", int(1));
			cout << FAILStr + "INT_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createInt(int(1)));
			cout << FAILStr + "INT_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "DATE") {
		try {
			btw.insert("batchTableWriter", "", int(1));
			cout << FAILStr + "DATE_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createDate(int(1)));
			cout << FAILStr + "DATE_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "MONTH") {
		try {
			btw.insert("batchTableWriter", "", int(1));
			cout << FAILStr + "MONTH_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createMonth(int(1)));
			cout << FAILStr + "MONTH_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "TIME") {
		try {
			btw.insert("batchTableWriter", "", int(1));
			cout << FAILStr + "TIME_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createTime(int(1)));
			cout << FAILStr + "TIME_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "SECOND") {
		try {
			btw.insert("batchTableWriter", "", int(1));
			cout << FAILStr + "SECOND_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createSecond(int(1)));
			cout << FAILStr + "SECOND_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "MINUTE") {
		try {
			btw.insert("batchTableWriter", "", int(1));
			cout << FAILStr + "MINUTE_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createMinute(int(1)));
			cout << FAILStr + "MINUTE_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "DATETIME") {
		try {
			btw.insert("batchTableWriter", "", int(1));
			cout << FAILStr + "DATETIME_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createDateTime(int(1)));
			cout << FAILStr + "DATETIME_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "DATEHOUR") {
		try {
			btw.insert("batchTableWriter", "", int(1));
			cout << FAILStr + "DATEHOUR_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createDateHour(int(1)));
			cout << FAILStr + "DATEHOUR_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
}

TEST_F(BatchTableWriterTest,test_BatchTableWriter_insert_error_type){
	vector<string> type = { "BOOL", "CHAR","SHORT", "STRING", "LONG", "NANOTIME","NANOTIMESTAMP", "TIMESTAMP","FLOAT", "DOUBLE","INT", "DATE","MONTH", "TIME","SECOND", "MINUTE","DATETIME", "DATEHOUR", "IPADDR", "INT128", "SYMBOL" };
	for (int i = 0; i < 18; ++i) {
		string script;
		script += "login('admin', '123456');go;";
		script += "share table(100:0,[`test], [" + type[i] + "]) as batchTableWriter;";
		conn.run(script);
		BatchTableWriter btw(hostName, port, "admin", "123456", true);
		btw.addTable("batchTableWriter", "");
		BatchTableWriter_insert_error_type(type[i], ref(btw));
	}
	string script;
	conn.run(script);
}