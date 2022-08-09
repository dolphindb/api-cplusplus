class DataformTableTest:public testing::Test
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


TEST_F(DataformTableTest,testStringTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_ANY);
	dict1->set("string",Util::createString("*-/%**%#~！#“》（a" ));
	dict1->set("sym", Util::createString("zzz123中文a"));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->set(0, Util::createString("zzz123中文a"));
	colVecs[1]->set(0, Util::createString("*-/%**%#~！#“》（a" ));

	vector<string> colNames = { "sym", "string" };
	vector<DATA_TYPE> colTypes = { DT_STRING, DT_STRING };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->set(0, Util::createString("zzz123中文a"));
	columnVecs[1]->set(0, Util::createString("*-/%**%#~！#“》（a" ));

	vector<ConstantSP> cols ={Util::createString("zzz123中文a"),Util::createString("*-/%**%#~！#“》（a" )};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	cout<<conn.run("tab1")->getScript()<<endl;
	cout<<conn.run("tab2")->getScript()<<endl;
	cout<<conn.run("tab3")->getScript()<<endl;

	string script = "a = table(\"zzz123中文a\" as sym,\"*-/%**%#~！#“》（a\" as string);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->get(i)->get(Util::createString("string"))->getBool());
			EXPECT_TRUE(judgeres->get(i)->get(Util::createString("sym"))->getBool());
		}
	}

	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}



TEST_F(DataformTableTest,testBoolTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_BOOL);
	dict1->set("col2",Util::createBool(1));
	dict1->set("col1", Util::createBool(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->set(0, Util::createBool(1));
	colVecs[1]->set(0, Util::createBool(0));

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_BOOL, DT_BOOL };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->set(0, Util::createBool(1));
	columnVecs[1]->set(0, Util::createBool(0));

	vector<ConstantSP> cols ={Util::createBool(1),Util::createBool(0)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(bool(1) as col1,bool(0) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testCharTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_CHAR);
	dict1->set("col2",Util::createChar(1));
	dict1->set("col1", Util::createChar(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->set(0, Util::createChar(1));
	colVecs[1]->set(0, Util::createChar(0));

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_CHAR, DT_CHAR };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->set(0, Util::createChar(1));
	columnVecs[1]->set(0, Util::createChar(0));

	vector<ConstantSP> cols ={Util::createChar(1),Util::createChar(0)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(char(1) as col1,char(0) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testCharNullTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_CHAR);
	dict1->set("col2",Util::createChar(1));
	dict1->set("col1", Util::createChar(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->setNull(0);
	colVecs[1]->setNull(0);

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_CHAR, DT_CHAR };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->setNull(0);
	columnVecs[1]->setNull(0);

	vector<ConstantSP> cols ={Util::createNullConstant(DT_CHAR),Util::createNullConstant(DT_CHAR)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(char(NULL) as col1,char(NULL) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testIntTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_INT);
	dict1->set("col2",Util::createInt(1));
	dict1->set("col1", Util::createInt(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->set(0, Util::createInt(1));
	colVecs[1]->set(0, Util::createInt(0));

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_INT };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->set(0, Util::createInt(1));
	columnVecs[1]->set(0, Util::createInt(0));

	vector<ConstantSP> cols ={Util::createInt(1),Util::createInt(0)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(int(1) as col1,int(0) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testIntNullTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_INT);
	dict1->set("col2",Util::createInt(1));
	dict1->set("col1", Util::createInt(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->setNull(0);
	colVecs[1]->setNull(0);

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_INT };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->setNull(0);
	columnVecs[1]->setNull(0);

	vector<ConstantSP> cols ={Util::createNullConstant(DT_INT),Util::createNullConstant(DT_INT)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(int(NULL) as col1,int(NULL) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}


TEST_F(DataformTableTest,testLongTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_LONG);
	dict1->set("col2",Util::createLong(1));
	dict1->set("col1", Util::createLong(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->set(0, Util::createLong(1));
	colVecs[1]->set(0, Util::createLong(0));

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_LONG, DT_LONG };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->set(0, Util::createLong(1));
	columnVecs[1]->set(0, Util::createLong(0));

	vector<ConstantSP> cols ={Util::createLong(1),Util::createLong(0)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(long(1) as col1,long(0) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testLongNullTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_LONG);
	dict1->set("col2",Util::createLong(1));
	dict1->set("col1", Util::createLong(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->setNull(0);
	colVecs[1]->setNull(0);

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_LONG, DT_LONG };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->setNull(0);
	columnVecs[1]->setNull(0);

	vector<ConstantSP> cols ={Util::createNullConstant(DT_LONG),Util::createNullConstant(DT_LONG)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(long(NULL) as col1,long(NULL) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testShortTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_SHORT);
	dict1->set("col2",Util::createShort(1));
	dict1->set("col1", Util::createShort(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->set(0, Util::createShort(1));
	colVecs[1]->set(0, Util::createShort(0));

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_SHORT, DT_SHORT };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->set(0, Util::createShort(1));
	columnVecs[1]->set(0, Util::createShort(0));

	vector<ConstantSP> cols ={Util::createShort(1),Util::createShort(0)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(short(1) as col1,short(0) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testShortNullTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_SHORT);
	dict1->set("col2",Util::createShort(1));
	dict1->set("col1", Util::createShort(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->setNull(0);
	colVecs[1]->setNull(0);

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_SHORT, DT_SHORT };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->setNull(0);
	columnVecs[1]->setNull(0);

	vector<ConstantSP> cols ={Util::createNullConstant(DT_SHORT),Util::createNullConstant(DT_SHORT)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(short(NULL) as col1,short(NULL) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}


TEST_F(DataformTableTest,testFloatTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_FLOAT);
	dict1->set("col2",Util::createFloat(1));
	dict1->set("col1", Util::createFloat(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->set(0, Util::createFloat(1));
	colVecs[1]->set(0, Util::createFloat(0));

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_FLOAT, DT_FLOAT };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->set(0, Util::createFloat(1));
	columnVecs[1]->set(0, Util::createFloat(0));

	vector<ConstantSP> cols ={Util::createFloat(1),Util::createFloat(0)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(float(1) as col1,float(0) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}


TEST_F(DataformTableTest,testFloatNullTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_FLOAT);
	dict1->set("col2",Util::createFloat(1));
	dict1->set("col1", Util::createFloat(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->setNull(0);
	colVecs[1]->setNull(0);

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_FLOAT, DT_FLOAT };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->setNull(0);
	columnVecs[1]->setNull(0);

	vector<ConstantSP> cols ={Util::createNullConstant(DT_FLOAT),Util::createNullConstant(DT_FLOAT)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(float(NULL) as col1,float(NULL) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testDoubleTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_DOUBLE);
	dict1->set("col2",Util::createDouble(1));
	dict1->set("col1", Util::createDouble(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->set(0, Util::createDouble(1));
	colVecs[1]->set(0, Util::createDouble(0));

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_DOUBLE, DT_DOUBLE };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->set(0, Util::createDouble(1));
	columnVecs[1]->set(0, Util::createDouble(0));

	vector<ConstantSP> cols ={Util::createDouble(1),Util::createDouble(0)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(double(1) as col1,double(0) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}


TEST_F(DataformTableTest,testDoubleNullTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_DOUBLE);
	dict1->set("col2",Util::createDouble(1));
	dict1->set("col1", Util::createDouble(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->setNull(0);
	colVecs[1]->setNull(0);

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_DOUBLE, DT_DOUBLE };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->setNull(0);
	columnVecs[1]->setNull(0);

	vector<ConstantSP> cols ={Util::createNullConstant(DT_DOUBLE),Util::createNullConstant(DT_DOUBLE)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(double(NULL) as col1,double(NULL) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testDatehourTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_DATEHOUR);
	dict1->set("col2",Util::createDateHour(1));
	dict1->set("col1", Util::createDateHour(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->set(0, Util::createDateHour(1));
	colVecs[1]->set(0, Util::createDateHour(0));

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_DATEHOUR, DT_DATEHOUR };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->set(0, Util::createDateHour(1));
	columnVecs[1]->set(0, Util::createDateHour(0));

	vector<ConstantSP> cols ={Util::createDateHour(1),Util::createDateHour(0)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(datehour(1) as col1,datehour(0) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testDatehourNullTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_DATEHOUR);
	dict1->set("col2",Util::createDateHour(1));
	dict1->set("col1", Util::createDateHour(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->setNull(0);
	colVecs[1]->setNull(0);

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_DATEHOUR, DT_DATEHOUR };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->setNull(0);
	columnVecs[1]->setNull(0);

	vector<ConstantSP> cols ={Util::createNullConstant(DT_DATEHOUR),Util::createNullConstant(DT_DATEHOUR)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(datehour(NULL) as col1,datehour(NULL) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testDateTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_DATE);
	dict1->set("col2",Util::createDate(1));
	dict1->set("col1", Util::createDate(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->set(0, Util::createDate(1));
	colVecs[1]->set(0, Util::createDate(0));

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_DATE, DT_DATE };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->set(0, Util::createDate(1));
	columnVecs[1]->set(0, Util::createDate(0));

	vector<ConstantSP> cols ={Util::createDate(1),Util::createDate(0)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(date(1) as col1,date(0) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testDateNullTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_DATE);
	dict1->set("col2",Util::createDate(1));
	dict1->set("col1", Util::createDate(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->setNull(0);
	colVecs[1]->setNull(0);

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_DATE, DT_DATE };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->setNull(0);
	columnVecs[1]->setNull(0);

	vector<ConstantSP> cols ={Util::createNullConstant(DT_DATE),Util::createNullConstant(DT_DATE)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(date(NULL) as col1,date(NULL) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testMinuteTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_MINUTE);
	dict1->set("col2",Util::createMinute(1));
	dict1->set("col1", Util::createMinute(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->set(0, Util::createMinute(1));
	colVecs[1]->set(0, Util::createMinute(0));

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_MINUTE, DT_MINUTE };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->set(0, Util::createMinute(1));
	columnVecs[1]->set(0, Util::createMinute(0));

	vector<ConstantSP> cols ={Util::createMinute(1),Util::createMinute(0)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(minute(1) as col1,minute(0) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testMinuteNullTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_MINUTE);
	dict1->set("col2",Util::createMinute(1));
	dict1->set("col1", Util::createMinute(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->setNull(0);
	colVecs[1]->setNull(0);

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_MINUTE, DT_MINUTE };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->setNull(0);
	columnVecs[1]->setNull(0);

	vector<ConstantSP> cols ={Util::createNullConstant(DT_MINUTE),Util::createNullConstant(DT_MINUTE)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(minute(NULL) as col1,minute(NULL) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());

}

TEST_F(DataformTableTest,testDatetimeTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_DATETIME);
	dict1->set("col2",Util::createDateTime(1));
	dict1->set("col1", Util::createDateTime(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->set(0, Util::createDateTime(1));
	colVecs[1]->set(0, Util::createDateTime(0));

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_DATETIME, DT_DATETIME };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->set(0, Util::createDateTime(1));
	columnVecs[1]->set(0, Util::createDateTime(0));

	vector<ConstantSP> cols ={Util::createDateTime(1),Util::createDateTime(0)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(datetime(1) as col1,datetime(0) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testDatetimeNullTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_DATETIME);
	dict1->set("col2",Util::createDateTime(1));
	dict1->set("col1", Util::createDateTime(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->setNull(0);
	colVecs[1]->setNull(0);

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_DATETIME, DT_DATETIME };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->setNull(0);
	columnVecs[1]->setNull(0);

	vector<ConstantSP> cols ={Util::createNullConstant(DT_DATETIME),Util::createNullConstant(DT_DATETIME)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(datetime(NULL) as col1,datetime(NULL) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testTimestampTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_TIMESTAMP);
	dict1->set("col2",Util::createTimestamp(1));
	dict1->set("col1", Util::createTimestamp(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->set(0, Util::createTimestamp(1));
	colVecs[1]->set(0, Util::createTimestamp(0));

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_TIMESTAMP, DT_TIMESTAMP };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->set(0, Util::createTimestamp(1));
	columnVecs[1]->set(0, Util::createTimestamp(0));

	vector<ConstantSP> cols ={Util::createTimestamp(1),Util::createTimestamp(0)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(timestamp(1) as col1,timestamp(0) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testTimestampNullTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_TIMESTAMP);
	dict1->set("col2",Util::createTimestamp(1));
	dict1->set("col1", Util::createTimestamp(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->setNull(0);
	colVecs[1]->setNull(0);

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_TIMESTAMP, DT_TIMESTAMP };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->setNull(0);
	columnVecs[1]->setNull(0);

	vector<ConstantSP> cols ={Util::createNullConstant(DT_TIMESTAMP),Util::createNullConstant(DT_TIMESTAMP)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(timestamp(NULL) as col1,timestamp(NULL) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testnanotimeTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_NANOTIME);
	dict1->set("col2",Util::createNanoTime(1));
	dict1->set("col1", Util::createNanoTime(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->set(0, Util::createNanoTime(1));
	colVecs[1]->set(0, Util::createNanoTime(0));

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_NANOTIME, DT_NANOTIME };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->set(0, Util::createNanoTime(1));
	columnVecs[1]->set(0, Util::createNanoTime(0));

	vector<ConstantSP> cols ={Util::createNanoTime(1),Util::createNanoTime(0)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(nanotime(1) as col1,nanotime(0) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testNanotimeNullTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_NANOTIME);
	dict1->set("col2",Util::createNanoTime(1));
	dict1->set("col1", Util::createNanoTime(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->setNull(0);
	colVecs[1]->setNull(0);

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_NANOTIME, DT_NANOTIME };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->setNull(0);
	columnVecs[1]->setNull(0);

	vector<ConstantSP> cols ={Util::createNullConstant(DT_NANOTIME),Util::createNullConstant(DT_NANOTIME)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(nanotime(NULL) as col1,nanotime(NULL) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testnanotimestampTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_NANOTIMESTAMP);
	dict1->set("col2",Util::createNanoTimestamp(1));
	dict1->set("col1", Util::createNanoTimestamp(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->set(0, Util::createNanoTimestamp(1));
	colVecs[1]->set(0, Util::createNanoTimestamp(0));

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_NANOTIMESTAMP, DT_NANOTIMESTAMP };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->set(0, Util::createNanoTimestamp(1));
	columnVecs[1]->set(0, Util::createNanoTimestamp(0));

	vector<ConstantSP> cols ={Util::createNanoTimestamp(1),Util::createNanoTimestamp(0)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(nanotimestamp(1) as col1,nanotimestamp(0) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testNanotimestampNullTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_NANOTIMESTAMP);
	dict1->set("col2",Util::createNanoTimestamp(1));
	dict1->set("col1", Util::createNanoTimestamp(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->setNull(0);
	colVecs[1]->setNull(0);

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_NANOTIMESTAMP, DT_NANOTIMESTAMP };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->setNull(0);
	columnVecs[1]->setNull(0);

	vector<ConstantSP> cols ={Util::createNullConstant(DT_NANOTIMESTAMP),Util::createNullConstant(DT_NANOTIMESTAMP)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(nanotimestamp(NULL) as col1,nanotimestamp(NULL) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testMonthTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_MONTH);
	dict1->set("col2",Util::createMonth(1));
	dict1->set("col1", Util::createMonth(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->set(0, Util::createMonth(1));
	colVecs[1]->set(0, Util::createMonth(0));

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_MONTH, DT_MONTH };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->set(0, Util::createMonth(1));
	columnVecs[1]->set(0, Util::createMonth(0));

	vector<ConstantSP> cols ={Util::createMonth(1),Util::createMonth(0)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(month(1) as col1,month(0) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testMonthNullTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_MONTH);
	dict1->set("col2",Util::createMonth(1));
	dict1->set("col1", Util::createMonth(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->setNull(0);
	colVecs[1]->setNull(0);

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_MONTH, DT_MONTH };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->setNull(0);
	columnVecs[1]->setNull(0);

	vector<ConstantSP> cols ={Util::createNullConstant(DT_MONTH),Util::createNullConstant(DT_MONTH)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(month(NULL) as col1,month(NULL) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testtimeTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_TIME);
	dict1->set("col2",Util::createTime(1));
	dict1->set("col1", Util::createTime(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->set(0, Util::createTime(1));
	colVecs[1]->set(0, Util::createTime(0));

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_TIME, DT_TIME };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->set(0, Util::createTime(1));
	columnVecs[1]->set(0, Util::createTime(0));

	vector<ConstantSP> cols ={Util::createTime(1),Util::createTime(0)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(time(1) as col1,time(0) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testTimeNullTable){
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_TIME);
	dict1->set("col2",Util::createTime(1));
	dict1->set("col1", Util::createTime(0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->setNull(0);
	colVecs[1]->setNull(0);

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_TIME, DT_TIME };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->setNull(0);
	columnVecs[1]->setNull(0);

	vector<ConstantSP> cols ={Util::createNullConstant(DT_TIME),Util::createNullConstant(DT_TIME)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	//cout<<conn.run("tab1")->getString()<<endl;
	//cout<<conn.run("tab2")->getString()<<endl;
	//cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(time(NULL) as col1,time(NULL) as col2);a";
	TableSP tab_res = conn.run(script);

	string judgestr= "res1=(a==tab1);res2=(a==tab2);res3=(a==tab3);\n\
					res1.append!(res2)\n\
					res1.append!(res3)\n\
					res1";

	TableSP judgeres = conn.run(judgestr);
	for(unsigned int i =0;i<judgeres->columns();i++){
		for(unsigned int j =0;j<judgeres->rows();j++){
			EXPECT_TRUE(judgeres->getColumn(i)->get(j)->getBool());
		}
	}
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}


TableSP createDemoTable1() {
    vector < string > colNames = { "name", "date", "price" };
    vector<DATA_TYPE> colTypes = { DT_STRING, DT_DATE, DT_DOUBLE };
    int colNum = 3, rowNum = 3;
    ConstantSP table = Util::createTable(colNames, colTypes, rowNum, 100);
    vector<VectorSP> columnVecs;
    for (int i = 0; i < colNum; i++)
        columnVecs.push_back(table->getColumn(i));

    for (int i = 0; i < rowNum; i++) {
        columnVecs[0]->set(i, Util::createString("name_" + std::to_string(i)));
        columnVecs[1]->set(i, Util::createDate(2010, 1, i + 1));
        columnVecs[2]->set(i, Util::createDouble(i * i));
    }
    return table;
}

TEST_F(DataformTableTest,testMemoryTable){
    string script;
    //simulation to generate data to be saved to the memory table
    VectorSP names = Util::createVector(DT_STRING, 5, 100);
    VectorSP dates = Util::createVector(DT_DATE, 5, 100);
    VectorSP prices = Util::createVector(DT_DOUBLE, 5, 100);
    for (int i = 0; i < 5; i++) {
        names->set(i, Util::createString("name_" + std::to_string(i)));
        dates->set(i, Util::createDate(2010, 1, i + 1));
        prices->set(i, Util::createDouble(i * i));
    }
    vector < string > allnames = { "names", "dates", "prices" };
    vector<DATA_TYPE> colTypes = { DT_STRING, DT_DATE, DT_DOUBLE };
    vector<ConstantSP> allcols = { names, dates, prices };
    ConstantSP table = Util::createTable(allnames, colTypes, 5, 100);
    conn.upload("tglobal", table);
    conn.upload(allnames, allcols); //upload data to server
    script += "insert into tglobal values(names,dates,prices);";
    script += "select * from tglobal;";
    TableSP res = conn.run(script);
    cout << res->getString() << endl;
}

TEST_F(DataformTableTest,testDiskTable){
    TableSP table = createDemoTable1();
    conn.upload("mt", table);
    vector < string > allnames = { "names", "dates", "prices" };
    vector<DATA_TYPE> colTypes = { DT_STRING, DT_DATE, DT_DOUBLE };
    ConstantSP tDiskGlobal = Util::createTable(allnames, colTypes, 3, 100);
    conn.upload("tDiskGlobal", tDiskGlobal);
    string script;
    script += "db=database(\"getHomeDir()\");";
    script += "tDiskGlobal.append!(mt);";
    script += "saveTable(db,tDiskGlobal,`dt);";
    script += "select * from tDiskGlobal;";
    TableSP result = conn.run(script);
    cout << result->getString() << endl;
}

TEST_F(DataformTableTest,testDFSTable){
    string script;
    TableSP table = createDemoTable1();
    conn.upload("mt", table);
    script += "login(`admin,`123456);";
    script += "dbPath = \"dfs://SAMPLE_TRDDB\";";
    script += "tableName = `demoTable;";
    script += "database(dbPath).loadTable(tableName).append!(mt);";
    script += "tradTable= database(dbPath).loadTable(tableName);";
    script += "select * from tradTable;";
    TableSP result = conn.run(script);
    cout << result->getString() << endl;
}


TEST_F(DataformTableTest,testinMemoryTableMoreThan65535){
	int colNum = 21, rowNum = 3000;
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

	srand((int)time(NULL));
	TableSP tab1 = Util::createTable(colNamesVec1, colTypesVec1, rowNum, colNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab1->getColumn(i));

	}
	for (int i = 0; i < rowNum; i++){
		columnVecs[0]->set(i, Util::createChar(rand()%CHAR_MAX));
		columnVecs[1]->set(i, Util::createBool(rand()%2));
		columnVecs[2]->set(i, Util::createShort(rand()%SHRT_MAX));
		columnVecs[3]->set(i, Util::createInt(rand()%INT_MAX));
		columnVecs[4]->set(i, Util::createLong(rand()%LONG_MAX));
		columnVecs[5]->set(i, Util::createDate(rand()%LONG_MAX));
		columnVecs[6]->set(i, Util::createMonth(rand()%LONG_MAX));
		columnVecs[7]->set(i, Util::createTime(rand()%LONG_MAX));
		columnVecs[8]->set(i, Util::createMinute(rand()%1439));
		columnVecs[9]->set(i, Util::createDateTime(rand()%LONG_LONG_MAX));
		columnVecs[10]->set(i, Util::createSecond(rand()%86400));
		columnVecs[11]->set(i, Util::createTimestamp(rand()%LONG_LONG_MAX));
		columnVecs[12]->set(i, Util::createNanoTime(rand()%LONG_LONG_MAX));
		columnVecs[13]->set(i, Util::createNanoTimestamp(rand()%LONG_LONG_MAX));
		columnVecs[14]->set(i, Util::createFloat(rand()/float(RAND_MAX)));
		columnVecs[15]->set(i, Util::createDouble(rand()/double(RAND_MAX)));
		columnVecs[16]->set(i, Util::createString("str"+to_string(i)));
		columnVecs[17]->set(i, Util::parseConstant(DT_UUID,"5d212a78-cc48-e3b1-4235-b4d91473ee87"));
		columnVecs[18]->set(i, Util::parseConstant(DT_IP,"192.0.0."+to_string(rand()%255)));
		columnVecs[19]->set(i, Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec32"));
		columnVecs[20]->set(i, Util::createBlob("blob"+to_string(i)));
	}

	conn.upload("tab1",{tab1});
	TableSP res_tab = conn.run("tab1");
	
	EXPECT_EQ(tab1->getString(), res_tab->getString());
	for (int i = 0; i < colNum; i++)
		EXPECT_EQ(tab1->getColumn(i)->getType(), res_tab->getColumn(i)->getType());
}

TEST_F(DataformTableTest,testinMemoryTableMoreThan1048576){
	int colNum = 21, rowNum = 48000;
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

	srand((int)time(NULL));
	TableSP tab1 = Util::createTable(colNamesVec1, colTypesVec1, rowNum, colNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab1->getColumn(i));

	}
	for (int i = 0; i < rowNum; i++){
		columnVecs[0]->set(i, Util::createChar(rand()%CHAR_MAX));
		columnVecs[1]->set(i, Util::createBool(rand()%2));
		columnVecs[2]->set(i, Util::createShort(rand()%SHRT_MAX));
		columnVecs[3]->set(i, Util::createInt(rand()%INT_MAX));
		columnVecs[4]->set(i, Util::createLong(rand()%LONG_MAX));
		columnVecs[5]->set(i, Util::createDate(rand()%LONG_MAX));
		columnVecs[6]->set(i, Util::createMonth(rand()%LONG_MAX));
		columnVecs[7]->set(i, Util::createTime(rand()%LONG_MAX));
		columnVecs[8]->set(i, Util::createMinute(rand()%1439));
		columnVecs[9]->set(i, Util::createDateTime(rand()%LONG_LONG_MAX));
		columnVecs[10]->set(i, Util::createSecond(rand()%86400));
		columnVecs[11]->set(i, Util::createTimestamp(rand()%LONG_LONG_MAX));
		columnVecs[12]->set(i, Util::createNanoTime(rand()%LONG_LONG_MAX));
		columnVecs[13]->set(i, Util::createNanoTimestamp(rand()%LONG_LONG_MAX));
		columnVecs[14]->set(i, Util::createFloat(rand()/float(RAND_MAX)));
		columnVecs[15]->set(i, Util::createDouble(rand()/double(RAND_MAX)));
		columnVecs[16]->set(i, Util::createString("str"+to_string(i)));
		columnVecs[17]->set(i, Util::parseConstant(DT_UUID,"5d212a78-cc48-e3b1-4235-b4d91473ee87"));	
		columnVecs[18]->set(i, Util::parseConstant(DT_IP,"192.0.0."+to_string(rand()%255)));
		columnVecs[19]->set(i, Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec32"));
		columnVecs[20]->set(i, Util::createBlob("blob"+to_string(i)));
	}

	conn.upload("tab1",{tab1});
	TableSP res_tab = conn.run("tab1");
	
	EXPECT_EQ(tab1->getString(), res_tab->getString());
	for (int i = 0; i < colNum; i++)
		EXPECT_EQ(tab1->getColumn(i)->getType(), res_tab->getColumn(i)->getType());
}