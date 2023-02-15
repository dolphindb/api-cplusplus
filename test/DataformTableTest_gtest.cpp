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
		
        cout<<"ok"<<endl;
    }
    virtual void TearDown()
    {
        conn.run("undef all;");
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
	EXPECT_EQ(tab1->getTableType(),BASICTBL);
	EXPECT_EQ(tab2->getTableType(),BASICTBL);
	EXPECT_EQ(tab3->getTableType(),BASICTBL);

	tab2->setName("tab2");
	EXPECT_EQ(tab2->keys()->getString(),"[\"sym\",\"string\"]");
	EXPECT_EQ(tab2->getColumnQualifier(0),tab2->getName());

	EXPECT_TRUE(tab2->isTemporary());
	tab2->setReadOnly(true);
	EXPECT_FALSE(tab2->sizeable());
	tab2->setTemporary(1); // nothing to do

	string errMsg;
	int rows=1;
	vector<ConstantSP> vals = {Util::createString("val1"), Util::createString("val2")};
	tab2->append(vals,rows,errMsg);
	EXPECT_EQ(errMsg,"Can't modify read only table.");
	tab1->append(vals,rows,errMsg);

	EXPECT_EQ(tab1->get(0)->get(Util::createString("sym"))->getString(),dict1->get(Util::createString("sym"))->getString());
	EXPECT_EQ(tab1->get(0)->get(Util::createString("string"))->getString(),dict1->get(Util::createString("string"))->getString());
	EXPECT_EQ(tab1->get(Util::createString("sym"))->getString(),tab1->getColumn(0)->getString());
	EXPECT_EQ(tab1->getMember(Util::createString("sym"))->getString(),tab1->getColumn(0)->getString());

	TableSP col0 = tab1->getWindow(0,1,0,2);
	EXPECT_EQ(col0->getColumn(0)->get(0)->getString(),tab1->getColumn(0)->get(0)->getString());
	EXPECT_EQ(col0->getColumn(0)->get(1)->getString(),tab1->getColumn(0)->get(1)->getString());

	TableSP instanceTab1=tab1->getInstance(0);
	EXPECT_EQ(instanceTab1->keys()->getString(),"[\"sym\",\"string\"]");

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

	vector<ConstantSP> colsNull ={};
	vector<string> col3Names = { "col1", "col2", "col3"};
	EXPECT_ANY_THROW(TableSP tab4 = Util::createTable(colNames,colsNull));
	EXPECT_ANY_THROW(TableSP tab4 = Util::createTable(col3Names,cols));

	VectorSP col1=Util::createVector(DT_CHAR,2,2);
	col1->set(0,Util::createChar(1));
	col1->set(1,Util::createChar(2));
	VectorSP col2=Util::createVector(DT_CHAR,1,1);
	col2->set(0,Util::createChar(1));
	vector<ConstantSP> colsDiffrows ={col1,col2};
	EXPECT_ANY_THROW(TableSP tab4 = Util::createTable(colNames,colsDiffrows));

	TableSP tab4=tab1->getInstance(1);
	tab4->set(0,dict1);
	EXPECT_EQ(tab4->getString(),"col1 col2\n---- ----\n0    1   \n");
	EXPECT_EQ(tab4->get(Util::createInt(0))->getMember(Util::createString("col1"))->getChar(),(char)0);
	EXPECT_EQ(tab4->get(Util::createInt(0))->getMember(Util::createString("col2"))->getChar(),(char)1);

	cout<<tab4->getAllocatedMemory()<<endl;

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

	tab1->set(0,Util::createVector(DT_INT,0,2));
	string errCol="errCol";
	EXPECT_ANY_THROW(tab1->getColumn(errCol));
	EXPECT_ANY_THROW(tab1->getColumn("sym",errCol));
	EXPECT_ANY_THROW(tab1->getColumn(errCol,errCol));

	EXPECT_FALSE(tab1->set(0,Util::createInt(1)));

	tab1->setName("tab1");
	tab1->setColumnName(1,"modifyedname");
	EXPECT_EQ(tab1->getColumnName(1),"modifyedname");
	EXPECT_EQ(tab1->getColumnIndex("errcol"),-1);
	EXPECT_EQ(tab1->getColumnIndex("modifyedname"),1);

	EXPECT_TRUE(tab1->contain("modifyedname"));
	EXPECT_TRUE(tab1->contain("tab1","modifyedname"));
	
	EXPECT_EQ(tab1->getColumnName(0),"col1");
	EXPECT_EQ(tab1->getColumn("tab1","col1")->getString(),tab1->getColumn(0)->getString());
	EXPECT_EQ(tab1->getColumn("col1",Util::createInt(0))->getString(),tab1->getColumn(0)->get(0)->getString());
	EXPECT_EQ(tab1->getColumn("col1")->getString(),tab1->getColumn(0)->getString());
	EXPECT_EQ(tab1->getColumn("tab1","col1",Util::createInt(0))->getString(),tab1->getColumn(0)->get(0)->getString());
	EXPECT_EQ(tab1->getColumn("tab1","col1")->getString(),tab1->getColumn(0)->getString());
	EXPECT_EQ(tab1->getColumn(0,Util::createInt(0))->getString(),tab1->getColumn(0)->get(0)->getString());
	EXPECT_EQ(tab1->getColumn(0)->getString(),tab1->getColumn(0)->getString());
	// EXPECT_EQ(tab1->get(0,0)->getString(),tab1->getColumn(0)->get(0)->getString());

	EXPECT_EQ(tab1->getColumnLabel()->get(0)->getString(),tab1->getColumnName(0));
	EXPECT_EQ(tab1->getColumnLabel()->get(1)->getString(),tab1->getColumnName(1));

	EXPECT_EQ(tab1->getString(0)," 1 0");
	EXPECT_EQ(tab1->get(Util::createString("col1"))->get(0)->getInt(),1);
	EXPECT_EQ(tab1->get(Util::createInt(0))->getMember(Util::createString("col1"))->getInt(),1);
	EXPECT_EQ(tab1->get(Util::createInt(0))->getMember(Util::createString("modifyedname"))->getInt(),0);

	VectorSP pair1 = Util::createPair(DT_INT);
    pair1->set(0, Util::createInt(1));
	pair1->set(1, Util::createInt(0));
	EXPECT_EQ(tab1->getString(),tab1->get(pair1)->getString());

	VectorSP vec1 = Util::createVector(DT_INT,1,1);
    vec1->set(0, Util::createInt(0));
	EXPECT_EQ(tab1->getString(),tab1->get(vec1)->getString());
	EXPECT_EQ(tab1->getString(),tab1->getWindow(0,2,0,1)->getString());

	DictionarySP dict_size121 = Util::createDictionary(DT_STRING,DT_INT);
	for(int i=0; i<121; i++){
		dict_size121->set("col"+to_string(i),Util::createInt(1));
	}
	TableSP tab_121cols = Util::createTable(dict_size121.get(), 1);
	for(int i=0; i<121; i++){
		tab_121cols->getColumn(i)->set(0, Util::createInt(1));
	}
	EXPECT_EQ(tab_121cols->getString(0).substr(Util::DISPLAY_WIDTH),"...");
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


TEST_F(DataformTableTest,testMemoryTable){
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

	for(unsigned int i =0; i<4; i++){
		EXPECT_EQ(table->getColumn(0)->get(i)->getString(),names->get(i+1)->getString());
		EXPECT_EQ(table->getColumn(1)->get(i)->getString(),dates->get(i+1)->getString());
		EXPECT_EQ(table->getColumn(2)->get(i)->getString(),prices->get(i+1)->getString());
	}
}


TEST_F(DataformTableTest,testDecimal32Table){
	srand((int)time(NULL));
	int scale1 = rand()%10;
	int scale2 = rand()%10;
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_DECIMAL32);
	dict1->set("col2",Util::createDecimal32(scale2,1));
	dict1->set("col1", Util::createDecimal32(scale1,0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->set(0, Util::createDecimal32(scale1,1));
	colVecs[1]->set(0, Util::createDecimal32(scale2,0));

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_DECIMAL32, DT_DECIMAL32 };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1, {scale1, scale2});
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->set(0, Util::createDecimal32(scale1,1));
	columnVecs[1]->set(0, Util::createDecimal32(scale2,0));

	vector<ConstantSP> cols ={Util::createDecimal32(scale1,1),Util::createDecimal32(scale2,0)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	// cout<<conn.run("tab1")->getString()<<endl;
	// cout<<conn.run("tab2")->getString()<<endl;
	// cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(decimal32(1,"+to_string(scale1)+") as col1,decimal32(0,"+to_string(scale2)+") as col2);a";
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


	EXPECT_EQ(tab_res->getColumn(0)->getExtraParamForType(),scale1);
	EXPECT_EQ(tab_res->getColumn(1)->getExtraParamForType(),scale2);
	
	EXPECT_EQ(tab1->getColumn(0)->getExtraParamForType(),scale1);
	EXPECT_EQ(tab2->getColumn(0)->getExtraParamForType(),scale1);
	EXPECT_EQ(tab3->getColumn(0)->getExtraParamForType(),scale1);
	EXPECT_EQ(tab1->getColumn(1)->getExtraParamForType(),scale2);
	EXPECT_EQ(tab2->getColumn(1)->getExtraParamForType(),scale2);
	EXPECT_EQ(tab3->getColumn(1)->getExtraParamForType(),scale2);
	
	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testDecimal32NullTable){
	srand((int)time(NULL));
	int scale1 = rand()%10;
	int scale2 = rand()%10;
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_DECIMAL32);
	dict1->set("col2",Util::createDecimal32(scale2,1));
	dict1->set("col1", Util::createDecimal32(scale1,1));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->setNull(0);
	colVecs[1]->setNull(0);

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_DECIMAL32, DT_DECIMAL32 };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1, {scale1,scale2});
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->setNull(0);
	columnVecs[1]->setNull(0);

	vector<ConstantSP> cols ={Util::createNullConstant(DT_DECIMAL32, scale1),Util::createNullConstant(DT_DECIMAL32, scale2)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	// cout<<conn.run("tab1")->getString()<<endl;
	// cout<<conn.run("tab2")->getString()<<endl;
	// cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(decimal32(NULL,"+to_string(scale1)+") as col1,decimal32(NULL,"+to_string(scale2)+") as col2);a";
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
	
	EXPECT_EQ(tab_res->getColumn(0)->getExtraParamForType(),scale1);
	EXPECT_EQ(tab_res->getColumn(1)->getExtraParamForType(),scale2);
	
	EXPECT_EQ(tab1->getColumn(0)->getExtraParamForType(),scale1);
	EXPECT_EQ(tab2->getColumn(0)->getExtraParamForType(),scale1);
	EXPECT_EQ(tab3->getColumn(0)->getExtraParamForType(),scale1);
	EXPECT_EQ(tab1->getColumn(1)->getExtraParamForType(),scale2);
	EXPECT_EQ(tab2->getColumn(1)->getExtraParamForType(),scale2);
	EXPECT_EQ(tab3->getColumn(1)->getExtraParamForType(),scale2);

	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}


TEST_F(DataformTableTest,testDecimal64Table){
	srand((int)time(NULL));
	int scale1 = rand()%19;
	int scale2 = rand()%19;
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_DECIMAL64);
	dict1->set("col2",Util::createDecimal64(scale2,1));
	dict1->set("col1", Util::createDecimal64(scale1,0));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->set(0, Util::createDecimal64(scale1,1));
	colVecs[1]->set(0, Util::createDecimal64(scale2,0));

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_DECIMAL64, DT_DECIMAL64 };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1, {scale1, scale2});
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->set(0, Util::createDecimal64(scale1,1));
	columnVecs[1]->set(0, Util::createDecimal64(scale2,0));

	vector<ConstantSP> cols ={Util::createDecimal64(scale1,1),Util::createDecimal64(scale2,0)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	// cout<<conn.run("tab1")->getString()<<endl;
	// cout<<conn.run("tab2")->getString()<<endl;
	// cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(decimal64(1,"+to_string(scale1)+") as col1,decimal64(0,"+to_string(scale2)+") as col2);a";
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
	
	EXPECT_EQ(tab_res->getColumn(0)->getExtraParamForType(),scale1);
	EXPECT_EQ(tab_res->getColumn(1)->getExtraParamForType(),scale2);
	
	EXPECT_EQ(tab1->getColumn(0)->getExtraParamForType(),scale1);
	EXPECT_EQ(tab2->getColumn(0)->getExtraParamForType(),scale1);
	EXPECT_EQ(tab3->getColumn(0)->getExtraParamForType(),scale1);
	EXPECT_EQ(tab1->getColumn(1)->getExtraParamForType(),scale2);
	EXPECT_EQ(tab2->getColumn(1)->getExtraParamForType(),scale2);
	EXPECT_EQ(tab3->getColumn(1)->getExtraParamForType(),scale2);

	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
}

TEST_F(DataformTableTest,testDecimal64NullTable){
	srand((int)time(NULL));
	int scale1 = rand()%19;
	int scale2 = rand()%19;
	DictionarySP dict1 = Util::createDictionary(DT_STRING,DT_DECIMAL64);
	dict1->set("col2",Util::createDecimal64(scale2,1));
	dict1->set("col1", Util::createDecimal64(scale1,1));
	TableSP tab1 = Util::createTable(dict1.get(), 1);
	vector<VectorSP> colVecs;
	colVecs.reserve(2);
	for (int i = 0; i < 2; i++){
		colVecs.emplace_back(tab1->getColumn(i));
	}
	colVecs[0]->setNull(0);
	colVecs[1]->setNull(0);

	vector<string> colNames = { "col1", "col2" };
	vector<DATA_TYPE> colTypes = { DT_DECIMAL64, DT_DECIMAL64 };
	int colNum = 2, rowNum = 1;
	TableSP tab2 = Util::createTable(colNames, colTypes, rowNum, 1, {scale1,scale2});
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab2->getColumn(i));
	}
	columnVecs[0]->setNull(0);
	columnVecs[1]->setNull(0);

	vector<ConstantSP> cols ={Util::createNullConstant(DT_DECIMAL64, scale1),Util::createNullConstant(DT_DECIMAL64, scale2)};
	TableSP tab3 = Util::createTable(colNames,cols);

	conn.upload("tab1",{tab1});
	conn.upload("tab2",{tab2});
	conn.upload("tab3",{tab3});

	// cout<<conn.run("tab1")->getString()<<endl;
	// cout<<conn.run("tab2")->getString()<<endl;
	// cout<<conn.run("tab3")->getString()<<endl;

	string script = "a = table(decimal64(NULL,"+to_string(scale1)+") as col1,decimal64(NULL,"+to_string(scale2)+") as col2);a";
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

	EXPECT_EQ(tab_res->getColumn(0)->getExtraParamForType(),scale1);
	EXPECT_EQ(tab_res->getColumn(1)->getExtraParamForType(),scale2);
	
	EXPECT_EQ(tab1->getColumn(0)->getExtraParamForType(),scale1);
	EXPECT_EQ(tab2->getColumn(0)->getExtraParamForType(),scale1);
	EXPECT_EQ(tab3->getColumn(0)->getExtraParamForType(),scale1);
	EXPECT_EQ(tab1->getColumn(1)->getExtraParamForType(),scale2);
	EXPECT_EQ(tab2->getColumn(1)->getExtraParamForType(),scale2);
	EXPECT_EQ(tab3->getColumn(1)->getExtraParamForType(),scale2);

	EXPECT_EQ(tab1->getString(), tab_res->getString());
	EXPECT_EQ(tab2->getString(), tab_res->getString());
	EXPECT_EQ(tab3->getString(), tab_res->getString());	
	EXPECT_EQ(tab1->getType(), tab_res->getType());
	EXPECT_EQ(tab2->getType(), tab_res->getType());
	EXPECT_EQ(tab3->getType(), tab_res->getType());
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

TEST_F(DataformTableTest,testDiskTable){
	TableSP table = createDemoTable();
	conn.upload("mt", table);
	string dbPath = conn.run("getHomeDir()")->getString()+"/cpp_test";
	replace(dbPath.begin(),dbPath.end(),'\\','/');
	string script;
	script += "dbPath=\""+dbPath+"\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db=database(dbPath,VALUE,2010.01.01..2010.01.30);";
	script += "tDiskGlobal=db.createPartitionedTable(mt,`tDiskGlobal,`date);";
	script += "tDiskGlobal.append!(mt);";
	//script += "saveTable(db,tDiskGlobal,`tDiskGlobal);";
	script += "res = select * from tDiskGlobal;";
	conn.run(script);
	// for(int i=0; i<3; i++)
	// 	EXPECT_TRUE(conn.run("each(eqObj, res.values(), mt.values());")->get(i)->getBool());
	cout<<conn.run("res")->getString();
}

TEST_F(DataformTableTest,testDFSTable){
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
	script += "res = select * from date;";
	//script += "select * from date where date>2020.01;";
	conn.run(script);
	for(int i=0; i<3; i++)
		EXPECT_TRUE(conn.run("each(eqObj, res.values(), mt.values());")->get(i)->getBool());

}

TEST_F(DataformTableTest,testDimensionTable){
	string script;
	TableSP table = createDemoTable();
	conn.upload("mt", table);
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://db1\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,VALUE,2010.01.01..2010.01.30);";
	script += "dt = db.createTable(mt,`dt).append!(mt);";
	script += "res = select * from dt;";
	conn.run(script);
	for(int i=0; i<3; i++)
		EXPECT_TRUE(conn.run("each(eqObj, res.values(), mt.values());")->get(i)->getBool());
}

TEST_F(DataformTableTest,testDFSTableSetStringWrong){
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

TEST_F(DataformTableTest,testDFSTableSetString){
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
	script += "res = select * from tradTable;";
	conn.run(script);
	for(int i=0; i<3; i++)
		EXPECT_TRUE(conn.run("each(eqObj, res.values(), mt.values());")->get(i)->getBool());
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
		columnVecs[9]->set(i, Util::createDateTime(rand()%LLONG_MAX));
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
	}

	conn.upload("tab1",{tab1});
	TableSP res_tab = conn.run("tab1");
	
	EXPECT_EQ(tab1->getString(), res_tab->getString());
	for (int i = 0; i < colNum; i++)
		EXPECT_EQ(tab1->getColumn(i)->getType(), res_tab->getColumn(i)->getType());
}


TEST_F(DataformTableTest,testRun){
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

TEST_F(DataformTableTest,test_BlockReader_getInmemoryTable){
    int64_t tm1, tm2, tm3=0;
    long total_size=10000000;
    string script;
    script.append("n="+to_string(total_size)+"\n"); 
    script.append("syms= `IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n"); 
    script.append("mytrades=table(09:30:00+rand(18000, n) as timestamp, rand(syms, n) as sym, 10*(1+rand(100, n)) as qty, 5.0+rand(100.0, n) as price); \n"); 
    conn.run(script);

    string sb = "select * from mytrades";
    int fetchSize = 100000;
    
    tm1 = Util::getEpochTime();
    TableSP mytrades = conn.run(sb);
    tm2 = Util::getEpochTime();
    BlockReaderSP reader = conn.run(sb,4,2,fetchSize);
    tm3 = Util::getEpochTime();
    vector<TableSP> fetchTableVec;
    long long total = 0;
    while(reader->hasNext()){
        for(int i=0;i<total_size/fetchSize;i++){
            TableSP fetchTable=reader->read();
            fetchTableVec.emplace_back(fetchTable);
            total += fetchTable->size();
            cout<<fetchTable->size()<<".";
        }

    }
	EXPECT_EQ(total,mytrades->rows());
    cout<<"total get: "+to_string(total)<<endl;
    cout<<"without fetchSize,time spend: "<<(double)(tm2-tm1)/(double)1000<<endl;
    cout<<"with fetchSize,time spend: "<<(double)(tm3-tm2)/(double)1000<<endl;
}

TEST_F(DataformTableTest,test_BlockReader_getPartitionedTable){
    int64_t tm1, tm2, tm3=0;
    long total_size=10000000;
    string script;
    script = "n="+to_string(total_size)+"\n\
            syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n\
            t=table(09:30:00+rand(18000, n) as timestamp, rand(syms, n) as sym, 10*(1+rand(100, n)) as qty, 5.0+rand(100.0, n) as price);\n\
            if(existsDatabase(\"dfs://test_run\")){dropDatabase(\"dfs://test_run\")};\n\
            db = database(\"dfs://test_run\", VALUE, syms)\n\
            test_run_tab = db.createPartitionedTable(table=t, tableName=\"test_run_tab\", partitionColumns=\"sym\");\n\
            test_run_tab.append!(t)";
    conn.run(script);

    string sb = "select * from test_run_tab";
    int fetchSize = 100000;
    
    tm1 = Util::getEpochTime();
    TableSP mytrades = conn.run(sb);
    tm2 = Util::getEpochTime();
    BlockReaderSP reader = conn.run(sb,4,2,fetchSize);
    tm3 = Util::getEpochTime();
    vector<TableSP> fetchTableVec;
    long long total = 0;
    while(reader->hasNext()){
        for(int i=0;i<total_size/fetchSize;i++){
            TableSP fetchTable=reader->read();
            fetchTableVec.emplace_back(fetchTable);
            total += fetchTable->size();
            cout<<fetchTable->size()<<".";
        }
    }
	EXPECT_EQ(total,mytrades->rows());
    cout<<"total get: "+to_string(total)<<endl;
    cout<<"without fetchSize,time spend: "<<(double)(tm2-tm1)/(double)1000<<endl;
    cout<<"with fetchSize,time spend: "<<(double)(tm3-tm2)/(double)1000<<endl;

}

TEST_F(DataformTableTest,test_BlockReader_skipALL){
	string script;
	script += R"(login("admin","123456");)";
	script += "pt = table(1..100000 as col1);";
	script += "select * from pt;";
	BlockReaderSP reader = conn.run(script, 4, 2, 8200);
	ConstantSP t = reader->read();
	cout<<reader->read()->rows()<<endl;
	EXPECT_TRUE(reader->hasNext());
	reader->skipAll();
	EXPECT_FALSE(reader->hasNext());
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

TEST_F(DataformTableTest,test_Block_Reader_DFStable){
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

TEST_F(DataformTableTest,test_Block_Table){
	Block_Table();
}

TEST_F(DataformTableTest,test_block_skipALL){
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

TEST_F(DataformTableTest,test_huge_table){
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

TEST_F(DataformTableTest,test_huge_DFS){
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

TEST_F(DataformTableTest,test_Block_Reader_DFStable_While_Block_Table){
	std::thread t1(Block_Table);
	t1.join();
	std::thread t2(Block_Reader_DFStable);
	t2.join();
}


TEST_F(DataformTableTest,testinMemoryTableMoreThan1048576withAlldataTypes){
	int colNum = 25, rowNum = 70000; // create a table with 70000 rows.
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
	colTypesVec1.emplace_back(DT_SYMBOL);

	srand((int)time(NULL));
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
		columnVecs[22]->set(i, Util::createDecimal32(rand()%10,rand()/float(RAND_MAX)));
		columnVecs[23]->set(i, Util::createDecimal64(rand()%19,rand()/double(RAND_MAX)));
		columnVecs[24]->set(i, Util::createString("sym"+to_string(i)));
	}
	for (int j = 0; j < colNum; j++)
		columnVecs[j]->setNull(rowNum-1);

	conn.upload("tab1",{tab1});
	TableSP res_tab = conn.run("tab1");
	cout<<res_tab->getString();
	EXPECT_EQ(tab1->getString(), res_tab->getString());
	for (int i = 0; i < colNum; i++){
		if(i == 24)
			EXPECT_EQ(tab1->getColumn(i)->getType(), DT_SYMBOL);
		else
			EXPECT_EQ(tab1->getColumn(i)->getType(), res_tab->getColumn(i)->getType());
	}
		

	{ // test class ResultSet(TableSP)
		ResultSet resultSet(tab1);
		EXPECT_FALSE(resultSet.isBeforeFirst());
		EXPECT_EQ(resultSet.position(), 0);
		resultSet.first();
		while (resultSet.isAfterLast() == false) {
			int rowIndex = resultSet.position();
			int colIndex = 0;
			if(resultSet.isFirst())
				cout<<"resultset assert begins"<<endl;
			
			for(auto i = 0;i < colNum;i++){
				if(i == 22 || i == 23)
					EXPECT_EQ(*resultSet.getBinary(i), *(res_tab->getColumn(i)->get(rowIndex)->getBinary()));
				else if (i == 24)
					EXPECT_EQ(resultSet.getDataType(i), DT_SYMBOL);
				else
					EXPECT_EQ(resultSet.getDataType(i), res_tab->getColumnType(i));
				
			}
			ASSERT_EQ(Util::createChar(resultSet.getChar(colIndex++))->getString(), res_tab->getColumn(0)->get(rowIndex)->getString());
			ASSERT_EQ(Util::createBool(resultSet.getBool(colIndex++))->getString(), res_tab->getColumn(1)->get(rowIndex)->getString());
			ASSERT_EQ(Util::createShort(resultSet.getShort(colIndex++))->getString(), res_tab->getColumn(2)->get(rowIndex)->getString());
			ASSERT_EQ(Util::createInt(resultSet.getInt(colIndex++))->getString(), res_tab->getColumn(3)->get(rowIndex)->getString());
			ASSERT_EQ(Util::createLong(resultSet.getLong(colIndex++))->getString(), res_tab->getColumn(4)->get(rowIndex)->getString());
			ASSERT_EQ(Util::createDate(resultSet.getInt(colIndex++))->getString(), res_tab->getColumn(5)->get(rowIndex)->getString());
			ASSERT_EQ(Util::createMonth(resultSet.getInt(colIndex++))->getString(), res_tab->getColumn(6)->get(rowIndex)->getString());
			ASSERT_EQ(Util::createTime(resultSet.getInt(colIndex++))->getString(), res_tab->getColumn(7)->get(rowIndex)->getString());
			ASSERT_EQ(Util::createMinute(resultSet.getInt(colIndex++))->getString(), res_tab->getColumn(8)->get(rowIndex)->getString());
			ASSERT_EQ(Util::createDateTime(resultSet.getInt(colIndex++))->getString(), res_tab->getColumn(9)->get(rowIndex)->getString());
			ASSERT_EQ(Util::createSecond(resultSet.getInt(colIndex++))->getString(), res_tab->getColumn(10)->get(rowIndex)->getString());
			ASSERT_EQ(Util::createTimestamp(resultSet.getLong(colIndex++))->getString(), res_tab->getColumn(11)->get(rowIndex)->getString());
			ASSERT_EQ(Util::createNanoTime(resultSet.getLong(colIndex++))->getString(), res_tab->getColumn(12)->get(rowIndex)->getString());
			ASSERT_EQ(Util::createNanoTimestamp(resultSet.getLong(colIndex++))->getString(), res_tab->getColumn(13)->get(rowIndex)->getString());
			ASSERT_EQ(Util::createFloat(resultSet.getFloat(colIndex++))->getString(), res_tab->getColumn(14)->get(rowIndex)->getString());
			ASSERT_EQ(Util::createDouble(resultSet.getDouble(colIndex++))->getString(), res_tab->getColumn(15)->get(rowIndex)->getString());
			ASSERT_EQ(Util::createString(resultSet.getString(colIndex++))->getString(), res_tab->getColumn(16)->get(rowIndex)->getString());
			ASSERT_EQ(resultSet.getObject(colIndex++)->getString(), res_tab->getColumn(17)->get(rowIndex)->getString());
			ASSERT_EQ(resultSet.getObject(colIndex++)->getString(), res_tab->getColumn(18)->get(rowIndex)->getString());
			ASSERT_EQ(resultSet.getObject(colIndex++)->getString(), res_tab->getColumn(19)->get(rowIndex)->getString());
			ASSERT_EQ(resultSet.getString(colIndex++), res_tab->getColumn(20)->get(rowIndex)->getString());
			ASSERT_EQ(Util::createDateHour(resultSet.getInt(colIndex++))->getString(), res_tab->getColumn(21)->get(rowIndex)->getString());
			ASSERT_EQ(resultSet.getObject(colIndex++)->getString(), res_tab->getColumn(22)->get(rowIndex)->getString());
			ASSERT_EQ(resultSet.getObject(colIndex++)->getString(), res_tab->getColumn(23)->get(rowIndex)->getString());
			ASSERT_EQ(resultSet.getString(colIndex++), res_tab->getColumn(24)->get(rowIndex)->getString());

			if(resultSet.isLast())
				cout<<"resultset assert finished"<<endl;
			resultSet.next();
		}
		resultSet.last();
		resultSet.next();
		EXPECT_TRUE(resultSet.isAfterLast());
		EXPECT_EQ(resultSet.position(), tab1->rows());
	}
}


TEST_F(DataformTableTest,testTablewithArrayVector){
	int colNum = 20, rowNum = 100, size = 10; // create a table with 70000 rows.
	vector<string> colNamesVec1;
	for (int i = 0; i < colNum; i++){
		colNamesVec1.emplace_back("col"+to_string(i));
	}
	vector<DATA_TYPE> colTypesVec1;
	colTypesVec1.emplace_back(DT_CHAR_ARRAY);
	colTypesVec1.emplace_back(DT_BOOL_ARRAY);
	colTypesVec1.emplace_back(DT_SHORT_ARRAY);
	colTypesVec1.emplace_back(DT_INT_ARRAY);
	colTypesVec1.emplace_back(DT_LONG_ARRAY);
	colTypesVec1.emplace_back(DT_DATE_ARRAY);
	colTypesVec1.emplace_back(DT_MONTH_ARRAY);
	colTypesVec1.emplace_back(DT_TIME_ARRAY);
	colTypesVec1.emplace_back(DT_MINUTE_ARRAY);
	colTypesVec1.emplace_back(DT_DATETIME_ARRAY);
	colTypesVec1.emplace_back(DT_SECOND_ARRAY);
	colTypesVec1.emplace_back(DT_TIMESTAMP_ARRAY);
	colTypesVec1.emplace_back(DT_NANOTIME_ARRAY);
	colTypesVec1.emplace_back(DT_NANOTIMESTAMP_ARRAY);
	colTypesVec1.emplace_back(DT_DATEHOUR_ARRAY);
	colTypesVec1.emplace_back(DT_FLOAT_ARRAY);
	colTypesVec1.emplace_back(DT_DOUBLE_ARRAY);
	colTypesVec1.emplace_back(DT_UUID_ARRAY);
	colTypesVec1.emplace_back(DT_IP_ARRAY);
	colTypesVec1.emplace_back(DT_INT128_ARRAY);

	srand(time(NULL));
	TableSP tab1 = Util::createTable(colNamesVec1, colTypesVec1, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab1->getColumn(i));

	}

	DdbVector<char> charV(0, size);
	DdbVector<char> boolV(0, size);
	DdbVector<short> shortV(0, size);
	DdbVector<int> intV(0, size);
	DdbVector<long long> longV(0, size);
	DdbVector<int> dateV(0, size);
	DdbVector<int> monthV(0, size);
	DdbVector<int> timeV(0, size);
	DdbVector<int> minuteV(0, size);
	DdbVector<int> datetimeV(0, size);
	DdbVector<int> secondV(0, size);
	DdbVector<long long> timestampV(0, size);
	DdbVector<long long> nanotimeV(0, size);
	DdbVector<long long> nanotimestampV(0, size);
	DdbVector<int> datehourV(0, size);
	DdbVector<float> floatV(0, size);
	DdbVector<double> doubleV(0, size);
	unsigned char uuid[16];
	unsigned char int128[16];
	unsigned char ip[16];
	DdbVector<Guid> uuidV(0, size);
	DdbVector<Guid> ipV(0, size);
	DdbVector<Guid> int128V(0, size);
	for (auto i = 0; i < size-1; i++) {
		doubleV.add(i);
		floatV.add(i);
		intV.add(i);
		shortV.add(i);
		longV.add(i);
		boolV.add(i%1);
		charV.add(i%CHAR_MAX);
		int128[i % 16] = i % CHAR_MAX;
		uuid[i % 16] = i % CHAR_MAX;
		ip[i % 16] = i % CHAR_MAX;
		uuidV.add(uuid);
		int128V.add(int128);
		ipV.add(ip);
		dateV.add(i);
		minuteV.add(i);
		datetimeV.add(i);
		nanotimeV.add((long long)i);
		datehourV.add(i);
		monthV.add(i);
		timeV.add(i);
		secondV.add(i);
		timestampV.add(i);
		nanotimestampV.add(i);
	}
	doubleV.addNull();
	floatV.addNull();
	intV.addNull();
	shortV.addNull();
	longV.addNull();
	boolV.addNull();
	charV.addNull();
	int128V.addNull();
	uuidV.addNull();
	ipV.addNull();
	dateV.addNull();
	minuteV.addNull();
	datetimeV.addNull();
	nanotimeV.addNull();
	datehourV.addNull();
	monthV.addNull();
	timeV.addNull();
	secondV.addNull();
	timestampV.addNull();
	nanotimestampV.addNull();

	VectorSP ddbcharV = charV.createVector(DT_CHAR);
	VectorSP ddbboolV = boolV.createVector(DT_BOOL);
	VectorSP ddbshortV = shortV.createVector(DT_SHORT);
	VectorSP ddbintV = intV.createVector(DT_INT);
	VectorSP ddblongV = longV.createVector(DT_LONG);
	VectorSP ddbdateV = dateV.createVector(DT_DATE);
	VectorSP ddbmonthV = monthV.createVector(DT_MONTH);
	VectorSP ddbtimeV = timeV.createVector(DT_TIME);
	VectorSP ddbminuteV = minuteV.createVector(DT_MINUTE);
	VectorSP ddbdatetimeV = datetimeV.createVector(DT_DATETIME);
	VectorSP ddbsecondV = secondV.createVector(DT_SECOND);
	VectorSP ddbtimestampV = timestampV.createVector(DT_TIMESTAMP);
	VectorSP ddbnanotimeV = nanotimeV.createVector(DT_NANOTIME);
	VectorSP ddbnanotimestampV = nanotimestampV.createVector(DT_NANOTIMESTAMP);
	VectorSP ddbdatehourV = datehourV.createVector(DT_DATEHOUR);
	VectorSP ddbfloatV = floatV.createVector(DT_FLOAT);
	VectorSP ddbdoubleV = doubleV.createVector(DT_DOUBLE);
	VectorSP ddbuuidV = uuidV.createVector(DT_UUID);
	VectorSP ddbipV = ipV.createVector(DT_IP);
	VectorSP ddbint128V = int128V.createVector(DT_INT128);

	for (int i = 0; i < rowNum-1; i++){
		columnVecs[0]->set(i, ddbcharV);
		columnVecs[1]->set(i, ddbboolV);
		columnVecs[2]->set(i, ddbshortV);
		columnVecs[3]->set(i, ddbintV);
		columnVecs[4]->set(i, ddblongV);
		columnVecs[5]->set(i, ddbdateV);
		columnVecs[6]->set(i, ddbmonthV);
		columnVecs[7]->set(i, ddbtimeV);
		columnVecs[8]->set(i, ddbminuteV);
		columnVecs[9]->set(i, ddbdatetimeV);
		columnVecs[10]->set(i, ddbsecondV);
		columnVecs[11]->set(i, ddbtimestampV);
		columnVecs[12]->set(i, ddbnanotimeV);
		columnVecs[13]->set(i, ddbnanotimestampV);
		columnVecs[14]->set(i, ddbdatehourV);
		columnVecs[15]->set(i, ddbfloatV);
		columnVecs[16]->set(i, ddbdoubleV);
		columnVecs[17]->set(i, ddbuuidV);
		columnVecs[18]->set(i, ddbipV);	
		columnVecs[19]->set(i, ddbint128V);
	}
	for (int j = 0; j < colNum; j++)
		columnVecs[j]->setNull(rowNum-1);

	conn.upload("tab1",{tab1});
	TableSP res_tab = conn.run("tab1");
	cout<<res_tab->getString();
	EXPECT_EQ(tab1->getString(), res_tab->getString());
	for (int i = 0; i < colNum; i++){
		EXPECT_EQ(tab1->getColumn(i)->getType(), res_tab->getColumn(i)->getType());
	}
}