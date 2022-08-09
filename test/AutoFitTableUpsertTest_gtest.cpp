class AutoFitTableUpsertTest:public testing::Test
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


TEST_F(AutoFitTableUpsertTest, test_upsertToPartitionTableRangeType){
    string script1;

    string dbName = "dfs://test_upsertToPartitionTableRangeType";
    string tableName = "pt";
    script1 += "dbName = \""+dbName+"\"\n";
    script1 += "tableName=\""+tableName+"\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db  = database(dbName, RANGE,0 5000 15000);\n";
    script1 += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"\
              "pt = db.createPartitionedTable(t,tableName,`id);";
    //cout<<script1<<endl;
    conn.run(script1);
    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);

	int colNum = 3, rowNum = 1000;
    vector<string> colNames = {"symbol", "id", "value"};
    vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_INT, DT_INT};

    TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tmp1->getColumn(i));
        }
    string sym[] = {"A", "B", "C", "D"};
    columnVecs[0]->set(0, Util::createString(sym[rand()%4]));
    columnVecs[1]->set(0, Util::createInt(0));
    columnVecs[2]->setNull(0);
    for (int i = 1; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createString(sym[rand()%4]));
        columnVecs[1]->set(i, Util::createInt(i));
        columnVecs[2]->set(i, Util::createInt((int)(rand()%1000)));
	}
    aftu.upsert(tmp1);

    TableSP res=conn.run("select * from pt;");
    // cout<< tmp1->getString()<<endl;
    // cout<< res->getString()<<endl;
	EXPECT_EQ((res->getColumnType(0)==18 || res->getColumnType(0)==17),true);
    EXPECT_EQ(res->getColumnType(1), 4);
    EXPECT_EQ(res->getColumnType(2), 4);
    EXPECT_EQ(tmp1->getString(), res->getString());


}


TEST_F(AutoFitTableUpsertTest, test_upsertToPartitionTableRangeTypeIgnoreNull){
    string script1;
    string dbName = "dfs://test_upsertToPartitionTableRangeTypeIgnoreNull";
    string tableName = "pt";
    script1 += "dbName = \""+dbName+"\"\n";
    script1 += "tableName=\""+tableName+"\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db  = database(dbName, RANGE,0 5000 15000);\n";
    script1 += "t = table(10:0, `symbol`id`value,[SYMBOL, INT, INT]);"\
               "tableInsert(t,[`asd,0,10]);"\
               "pt = db.createPartitionedTable(t,tableName,`id);"\
               "tableInsert(pt,t)";
    //cout<<script1<<endl;
    conn.run(script1);
    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, true, &keycolName);
	int colNum = 3, rowNum = 10;
    vector<string> colNames = {"symbol", "id", "value"};
    vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_INT, DT_INT};

    TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tmp1->getColumn(i));
        }
    string sym[] = {"A", "B", "C", "D"};
    columnVecs[0]->set(0, Util::createString("D"));
    columnVecs[1]->set(0, Util::createInt(0));
    columnVecs[2]->setNull(0);
    for (int i = 1; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createString(sym[rand()%4]));
        columnVecs[1]->set(i, Util::createInt(i));
        columnVecs[2]->set(i, Util::createInt((int)(rand()%1000)));
	}
    cout<< tmp1->getString()<<endl;
    aftu.upsert(tmp1);
    TableSP res=conn.run("select * from pt;");

	EXPECT_EQ((res->getColumnType(0)==18 || res->getColumnType(0)==17),true);
    EXPECT_EQ(res->getColumnType(1), 4);
    EXPECT_EQ(res->getColumnType(2), 4);

    // cout<< tmp1->getString()<<endl<<res->getString()<<endl;

    EXPECT_EQ(res->getRow(0)->getString(), "value->10\nid->0\nsymbol->D\n");
    
    for (int i = 1; i < rowNum; i++) {
        // cout<<tmp1->getColumn(0)->getRow(i)->getString()<<" "<<res->getColumn(0)->getRow(i)->getString()<<endl;
        EXPECT_EQ(tmp1->getColumn(0)->getRow(i)->getString(), res->getColumn(0)->getRow(i)->getString());
        EXPECT_EQ(tmp1->getColumn(1)->getRow(i)->getString(), res->getColumn(1)->getRow(i)->getString());
        EXPECT_EQ(tmp1->getColumn(2)->getRow(i)->getString(), res->getColumn(2)->getRow(i)->getString());
    }

}

TEST_F(AutoFitTableUpsertTest, test_upsertToKeyedTable){
    string script1;

    string tableName = "pt";
    script1 += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"\
              "pt = keyedTable(`id, t);";
    //cout<<script1<<endl;
    conn.run(script1);

    AutoFitTableUpsert aftu("", tableName, conn, false);

	int colNum = 3, rowNum = 1000;
    vector<string> colNames = {"symbol", "id", "value"};
    vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_INT, DT_INT};

    TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tmp1->getColumn(i));
        }
    string sym[] = {"A", "B", "C", "D"};
    columnVecs[0]->set(0, Util::createString(sym[rand()%4]));
    columnVecs[1]->set(0, Util::createInt(0));
    columnVecs[2]->setNull(0);
    for (int i = 1; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createString(sym[rand()%4]));
        columnVecs[1]->set(i, Util::createInt(i));
        columnVecs[2]->set(i, Util::createInt((int)(rand()%1000)));
	}
    aftu.upsert(tmp1);

    TableSP res=conn.run("select * from pt;");
    // cout<< tmp1->getString()<<endl;
    // cout<< res->getString()<<endl;
	EXPECT_EQ((res->getColumnType(0)==18 || res->getColumnType(0)==17),true);
    EXPECT_EQ(res->getColumnType(1), 4);
    EXPECT_EQ(res->getColumnType(2), 4);
    EXPECT_EQ(tmp1->getString(), res->getString());


}


TEST_F(AutoFitTableUpsertTest, test_upsertToKeyedTableIgnoreNull){
    string script1;

    string tableName = "pt";
    script1 += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"\
              "pt = keyedTable(`id, t);\
              tableInsert(pt,`asd,0,10)";
    //cout<<script1<<endl;
    conn.run(script1);

    AutoFitTableUpsert aftu("", tableName, conn, true);

	int colNum = 3, rowNum = 1000;
    vector<string> colNames = {"symbol", "id", "value"};
    vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_INT, DT_INT};

    TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tmp1->getColumn(i));
        }
    string sym[] = {"A", "B", "C", "D"};
    columnVecs[0]->set(0, Util::createString("D"));
    columnVecs[1]->set(0, Util::createInt(0));
    columnVecs[2]->setNull(0);
    for (int i = 1; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createString(sym[rand()%4]));
        columnVecs[1]->set(i, Util::createInt(i));
        columnVecs[2]->set(i, Util::createInt((int)(rand()%1000)));
	}
    aftu.upsert(tmp1);

    TableSP res=conn.run("select * from pt;");
	EXPECT_EQ((res->getColumnType(0)==18 || res->getColumnType(0)==17),true);
    EXPECT_EQ(res->getColumnType(1), 4);
    EXPECT_EQ(res->getColumnType(2), 4);
    // cout<< tmp1->getString()<<endl<<res->getString()<<endl;

    EXPECT_EQ(res->getRow(0)->getString(), "value->10\nid->0\nsymbol->D\n");
    
    for (int i = 1; i < rowNum; i++) {
        // cout<<tmp1->getColumn(0)->getRow(i)->getString()<<" "<<res->getColumn(0)->getRow(i)->getString()<<endl;
        EXPECT_EQ(tmp1->getColumn(0)->getRow(i)->getString(), res->getColumn(0)->getRow(i)->getString());
        EXPECT_EQ(tmp1->getColumn(1)->getRow(i)->getString(), res->getColumn(1)->getRow(i)->getString());
        EXPECT_EQ(tmp1->getColumn(2)->getRow(i)->getString(), res->getColumn(2)->getRow(i)->getString());
    }

}

TEST_F(AutoFitTableUpsertTest, test_upsertToindexedTable){
    string script1;

    string tableName = "pt";
    script1 += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"\
              "pt = indexedTable(`id, t);";
    //cout<<script1<<endl;
    conn.run(script1);

    AutoFitTableUpsert aftu("", tableName, conn, false);

	int colNum = 3, rowNum = 1000;
    vector<string> colNames = {"symbol", "id", "value"};
    vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_INT, DT_INT};

    TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tmp1->getColumn(i));
        }
    string sym[] = {"A", "B", "C", "D"};
    columnVecs[0]->set(0, Util::createString(sym[rand()%4]));
    columnVecs[1]->set(0, Util::createInt(0));
    columnVecs[2]->setNull(0);
    for (int i = 1; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createString(sym[rand()%4]));
        columnVecs[1]->set(i, Util::createInt(i));
        columnVecs[2]->set(i, Util::createInt((int)(rand()%1000)));
	}
    aftu.upsert(tmp1);

    TableSP res=conn.run("select * from pt;");
    // cout<< tmp1->getString()<<endl;
    // cout<< res->getString()<<endl;
	EXPECT_EQ((res->getColumnType(0)==18 || res->getColumnType(0)==17),true);
    EXPECT_EQ(res->getColumnType(1), 4);
    EXPECT_EQ(res->getColumnType(2), 4);
    EXPECT_EQ(tmp1->getString(), res->getString());


}

TEST_F(AutoFitTableUpsertTest, test_upsertToindexedTableIgnoreNull){
    string script1;

    string tableName = "pt";
    script1 += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"\
              "pt = indexedTable(`id, t);\
              tableInsert(pt,`asd,0,10)";
    //cout<<script1<<endl;
    conn.run(script1);

    AutoFitTableUpsert aftu("", tableName, conn, true);

	int colNum = 3, rowNum = 1000;
    vector<string> colNames = {"symbol", "id", "value"};
    vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_INT, DT_INT};

    TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tmp1->getColumn(i));
        }
    string sym[] = {"A", "B", "C", "D"};
    columnVecs[0]->set(0, Util::createString("D"));
    columnVecs[1]->set(0, Util::createInt(0));
    columnVecs[2]->setNull(0);
    for (int i = 1; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createString(sym[rand()%4]));
        columnVecs[1]->set(i, Util::createInt(i));
        columnVecs[2]->set(i, Util::createInt((int)(rand()%1000)));
	}
    aftu.upsert(tmp1);

    TableSP res=conn.run("select * from pt;");
	EXPECT_EQ((res->getColumnType(0)==18 || res->getColumnType(0)==17),true);
    EXPECT_EQ(res->getColumnType(1), 4);
    EXPECT_EQ(res->getColumnType(2), 4);

    // cout<< tmp1->getString()<<endl<<res->getString()<<endl;

    EXPECT_EQ(res->getRow(0)->getString(), "value->10\nid->0\nsymbol->D\n");
    
    for (int i = 1; i < rowNum; i++) {
        // cout<<tmp1->getColumn(0)->getRow(i)->getString()<<" "<<res->getColumn(0)->getRow(i)->getString()<<endl;
        EXPECT_EQ(tmp1->getColumn(0)->getRow(i)->getString(), res->getColumn(0)->getRow(i)->getString());
        EXPECT_EQ(tmp1->getColumn(1)->getRow(i)->getString(), res->getColumn(1)->getRow(i)->getString());
        EXPECT_EQ(tmp1->getColumn(2)->getRow(i)->getString(), res->getColumn(2)->getRow(i)->getString());
    }

}

TEST_F(AutoFitTableUpsertTest, test_upsertToPartitionTableRangeTypeWithsortColumns){
    string script1;

    string dbName = "dfs://test_upsertToPartitionTableRangeType";
    string tableName = "pt";
    script1 += "dbName = \""+dbName+"\"\n";
    script1 += "tableName=\""+tableName+"\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db  = database(dbName, RANGE,0 5000 15000);\n";
    script1 += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"\
               "pt = db.createPartitionedTable(t,tableName,`id);";
    //cout<<script1<<endl;
    conn.run(script1);
	
	int colNum = 3;
    int rowNum = 1000;
    vector<VectorSP> columnVecs;
    vector<VectorSP> columnVecs2;
    vector<string> colNames = {"symbol", "id", "value"};
    vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_INT, DT_INT};

    TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);

	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tmp1->getColumn(i));
        }
    string sym[] = {"A", "B", "C", "D"};
    columnVecs[0]->set(0, Util::createString("D"));
    columnVecs[1]->set(0, Util::createInt(0));
    columnVecs[2]->setInt(1000);
    for (int i = 1; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createString(sym[rand()%4]));
        columnVecs[1]->set(i, Util::createInt(0));
        columnVecs[2]->set(i, Util::createInt((int)(rowNum-i)));
	}
    conn.upload("tmp1",{tmp1});
    conn.run("tableInsert(pt,tmp1)");
    Util::sleep(2000);
    vector<string> keycolName = {"id"};
    vector<string> sortColName = {"value"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName, &sortColName);

	int rowNum2 = 1;

    TableSP tmp2 = Util::createTable(colNames, colTypes, rowNum2, rowNum2);
    
	columnVecs2.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs2.emplace_back(tmp2->getColumn(i));
        }

    columnVecs2[0]->set(0, Util::createString("D"));
    columnVecs2[1]->set(0, Util::createInt(0));
    columnVecs2[2]->setInt(0);

    aftu.upsert(tmp2);

    TableSP res=conn.run("select * from pt;");
    // cout<< res->getString()<<endl;
	EXPECT_EQ((res->getColumnType(0)==18 || res->getColumnType(0)==17),true);
    EXPECT_EQ(res->getColumnType(1), 4);
    EXPECT_EQ(res->getColumnType(2), 4);
    
    for (int i = 1; i < rowNum; i++){
        // cout<<res->getColumn(2)->getRow(i-1)->getInt()<<" "<<res->getColumn(2)->getRow(i)->getString()<<endl;
        EXPECT_EQ((res->getColumn(2)->getRow(i)->getInt() > res->getColumn(2)->getRow(i-1)->getInt()), true);
    }
}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithIntArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithIntArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithIntArrayVectorToPartitionTableRangeType\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithIntArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithIntArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithIntArrayVectorNullToPartitionTableRangeType\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithIntArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_upsertTablewithIntArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithIntArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithCharArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithCharArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithCharArrayVectorToPartitionTableRangeType\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithCharArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithCharArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithCharArrayVectorNullToPartitionTableRangeType\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithCharArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_upsertTablewithCharArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithCharArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithFloatArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithFloatArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithFloatArrayVectorToPartitionTableRangeType\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithFloatArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithFloatArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithFloatArrayVectorNullToPartitionTableRangeType\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithFloatArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_upsertTablewithFloatArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithFloatArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithDateArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithDateArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithDateArrayVectorToPartitionTableRangeType\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithDateArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithDateArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithDateArrayVectorNullToPartitionTableRangeType\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithDateArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_upsertTablewithDateArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithDateArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithMonthArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithMonthArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithMonthArrayVectorToPartitionTableRangeType\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithMonthArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithMonthArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithMonthArrayVectorNullToPartitionTableRangeType\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithMonthArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_upsertTablewithMonthArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithMonthArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithTimeArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithTimeArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithTimeArrayVectorToPartitionTableRangeType\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithTimeArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithTimeArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithTimeArrayVectorNullToPartitionTableRangeType\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithTimeArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_upsertTablewithTimeArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithTimeArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithSecondArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithSecondArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithSecondArrayVectorToPartitionTableRangeType\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithSecondArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithSecondArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithSecondArrayVectorNullToPartitionTableRangeType\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithSecondArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_upsertTablewithSecondArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithSecondArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithDatehourArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithDatehourArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithDatehourArrayVectorToPartitionTableRangeType\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithDatehourArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithDatehourArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithDatehourArrayVectorNullToPartitionTableRangeType\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithDatehourArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_upsertTablewithDatehourArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithDatehourArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithUuidArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithUuidArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithUuidArrayVectorToPartitionTableRangeType\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithUuidArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithUuidArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithUuidArrayVectorNullToPartitionTableRangeType\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithUuidArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_upsertTablewithUuidArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithUuidArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
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

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}