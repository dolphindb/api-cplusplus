class FunctionTest:public testing::Test
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
            string script = "a=int(1);\
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
                    v=arrayVector(1 2 3 , 9 9 9);\
                    w=set(1 2 3);\
                    x=matrix([0],[0]);\
                    y={\"sym\":123};\
                    z=int128(\"e1671797c52e15f763380b45e841ec32\");\
                    vec=1 2 3;\
                    sym=symbol(`a`b`c`d);";
	        conn.run(script);
            cout << "connect to " + hostName + ":" + std::to_string(port)<< endl;
        }
    }
    static void TearDownTestCase(){
        conn.run("undef all;");
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
        pass;
    }
};


TEST_F(FunctionTest,test_function_get){
    TableSP tab1=conn.run("u");
    ConstantSP intval=conn.run("a");
    VectorSP vec1= conn.run("vec");
    VectorSP av1=conn.run("v");
    SetSP set1=conn.run("w");
    DictionarySP dict1=conn.run("y");
    ConstantSP voidval=conn.run("d");
    ConstantSP uuidval=conn.run("s");
    ConstantSP symval=conn.run("sym");

    // cout<<symval->getSymbolBase()->getID();
    cout<<tab1->getObjectType()<<endl;
    cout<<tab1->getString()<<endl;
    cout<<Util::getTableTypeString(tab1->getTableType())<<endl;
    cout<<vec1->getInstance()->getString()<<endl;
    cout<<vec1->get(0,0,0)->getString()<<endl;
    cout<<vec1->getWindow(0,1,0,1)->getString()<<endl;
    cout<<vec1->getSubVector(0,3,3)->getString()<<endl;
    cout<<vec1->getSubVector(0,3)->getString()<<endl;
    cout<<vec1->getAllocatedMemory(3)<<endl;
    cout<<Util::getDataTypeString(dict1->getRawType())<<endl;
    cout<<Util::getCategoryString(dict1->getCategory())<<endl;
    cout<<Util::getDataTypeString(dict1->getKeyType())<<endl;
    cout<<"----------------------------------------------"<<endl;

    EXPECT_THROW(dict1->getString(0),RuntimeException);

    EXPECT_ANY_THROW(tab1->getBool());
    EXPECT_ANY_THROW(tab1->getChar());
    EXPECT_ANY_THROW(tab1->getShort());
    EXPECT_ANY_THROW(tab1->getInt());
    EXPECT_ANY_THROW(tab1->getLong());
    EXPECT_ANY_THROW(voidval->getIndex());
    EXPECT_ANY_THROW(tab1->getFloat());
    EXPECT_ANY_THROW(tab1->getDouble());
    EXPECT_ANY_THROW(tab1->getBinary());
    EXPECT_ANY_THROW(vec1->getValueSize());
    EXPECT_ANY_THROW(dict1->getStringRef());
    EXPECT_ANY_THROW(dict1->get(1,1));
    EXPECT_ANY_THROW(dict1->getColumn(1));
    EXPECT_ANY_THROW(dict1->getRow(1));
    EXPECT_ANY_THROW(dict1->getItem(1));

    int buckets=2;
    EXPECT_EQ(voidval->getHash(buckets),-1);
    EXPECT_EQ(voidval->getString(),"");
    EXPECT_EQ(voidval->getStringRef(),Constant::EMPTY);
    EXPECT_EQ(voidval->getStringRef(0),Constant::EMPTY);
    
    EXPECT_EQ(intval->getIndex(0),1);
    EXPECT_EQ(intval->get(0)->getInt(),1);
    EXPECT_EQ(intval->get(0,0)->getInt(),1);
    EXPECT_EQ(intval->get(Util::createInt(0))->getInt(),1);
    EXPECT_EQ(intval->getColumn(0)->getInt(),1);
    EXPECT_EQ(intval->getWindow(0,1,0,1)->getInt(),1);
    vec1->setName("vec");
    EXPECT_EQ(vec1->getName(),"vec");
    EXPECT_EQ(set1->getRawType(),DT_INT);
    EXPECT_EQ(tab1->getRawType(),DT_DICTIONARY);
    EXPECT_EQ(Util::getCategoryString(intval->getCategory()),"INTEGRAL");
    EXPECT_EQ(Util::getCategoryString(set1->getCategory()),"INTEGRAL");
    EXPECT_EQ(Util::getCategoryString(tab1->getCategory()),"MIXED");

    char *buf = new char[2]; 
    short *buf1 = new short[2];
    int *buf2 = new int[2];
    INDEX *buf4 = new INDEX[2];
    long long *buf3 = new long long[2];
    float *buf5 = new float[2];
    double *buf6 = new double[2];
    string **buf8 = new string*[2];
    char **buf9 = new char*[2];
    unsigned char *buf10 = new unsigned char[2];
    SymbolBase *symbase= new SymbolBase(1);
    void *buf11=NULL;

    EXPECT_FALSE(uuidval->getBool(0,1,buf));
    EXPECT_FALSE(uuidval->getChar(0,1,buf));
    EXPECT_FALSE(uuidval->getShort(0,1,buf1)); 
    EXPECT_FALSE(uuidval->getInt(0,1,buf2));
    EXPECT_FALSE(uuidval->getLong(0,1,buf3));
    EXPECT_FALSE(uuidval->getIndex(0,1,buf4));
    EXPECT_FALSE(uuidval->getFloat(0,1,buf5));
    EXPECT_FALSE(uuidval->getDouble(0,1,buf6));   
    EXPECT_FALSE(uuidval->getSymbol(0,1,buf2,symbase,false)); 
    EXPECT_FALSE(uuidval->getString(0,1,buf8));
    EXPECT_FALSE(uuidval->getString(0,1,buf9));
    EXPECT_FALSE(voidval->getBinary(0,1,1,buf10));
    EXPECT_FALSE(voidval->getHash(0,1,buckets,buf2));

    EXPECT_ANY_THROW(uuidval->getBoolConst(0,1,buf));
    EXPECT_ANY_THROW(uuidval->getCharConst(0,1,buf));
    EXPECT_ANY_THROW(uuidval->getShortConst(0,1,buf1)); 
    EXPECT_ANY_THROW(uuidval->getIntConst(0,1,buf2));
    EXPECT_ANY_THROW(uuidval->getLongConst(0,1,buf3));
    EXPECT_ANY_THROW(uuidval->getIndexConst(0,1,buf4));
    EXPECT_ANY_THROW(uuidval->getFloatConst(0,1,buf5));
    EXPECT_ANY_THROW(uuidval->getDoubleConst(0,1,buf6));   
    EXPECT_ANY_THROW(uuidval->getSymbolConst(0,1,buf2,symbase,false)); 
    EXPECT_ANY_THROW(uuidval->getStringConst(0,1,buf8));
    EXPECT_ANY_THROW(uuidval->getStringConst(0,1,buf9));
    EXPECT_ANY_THROW(voidval->getBinaryConst(0,1,1,buf10));
    EXPECT_ANY_THROW(set1->get(Util::createInt(0)));
    EXPECT_ANY_THROW(set1->getStringRef());
    EXPECT_ANY_THROW(set1->get(0));
    EXPECT_ANY_THROW(set1->get(0,1));
    EXPECT_ANY_THROW(set1->getColumn(0));
    EXPECT_ANY_THROW(set1->getRow(0));
    EXPECT_ANY_THROW(set1->getItem(0));

    cout<<uuidval->getBoolBuffer(0,1,buf)<<endl;
    cout<<uuidval->getCharBuffer(0,1,buf)<<endl;
    cout<<uuidval->getShortBuffer(0,1,buf1)<<endl; 
    cout<<uuidval->getIntBuffer(0,1,buf2)<<endl;
    cout<<uuidval->getLongBuffer(0,1,buf3)<<endl;
    cout<<uuidval->getIndexBuffer(0,1,buf4)<<endl;
    cout<<uuidval->getFloatBuffer(0,1,buf5)<<endl;
    cout<<uuidval->getDoubleBuffer(0,1,buf6)<<endl;   
    cout<<voidval->getBinaryBuffer(0,1,1,buf10)<<endl;
    cout<<voidval->getDataBuffer(0,1,buf11)<<endl;
    cout<<uuidval->getAllocatedMemory()<<endl;
    cout<<set1->getAllocatedMemory()<<endl;

    EXPECT_ANY_THROW(uuidval->getMember(Util::createConstant(DT_INT)));
    EXPECT_ANY_THROW(uuidval->keys());
    EXPECT_ANY_THROW(uuidval->values());

    EXPECT_EQ(uuidval->getDataArray(),(void* )0);
    EXPECT_EQ(uuidval->getDataSegment(),(void** )0);
    EXPECT_EQ(uuidval->getIndexArray(),(INDEX* )NULL);
    EXPECT_EQ(uuidval->getHugeIndexArray(),(INDEX** )NULL);
    EXPECT_EQ(uuidval->getSegmentSize(),1);
    EXPECT_EQ(uuidval->getSegmentSizeInBit(),0);
    EXPECT_ANY_THROW(uuidval->castTemporal(DT_INT128));

	EXPECT_ANY_THROW(tab1->getColumn("abcd"));
	tab1->setName("table1");
    EXPECT_EQ(tab1->get(0)->getString(),"col2->a\ncol1->1\n");
	EXPECT_EQ(tab1->getColumn("table1","col1")->getString(),"[1,2,3]");
	EXPECT_EQ(tab1->getColumn("table1","col1",0)->getString(),"[1,2,3]");
	EXPECT_EQ(tab1->getColumn(1,0)->getString(),"[\"a\",\"b\",\"c\"]");
    EXPECT_ANY_THROW(tab1->getColumn("table2","col2"));

    tab1->setColumnName(0, "col111");
	EXPECT_EQ(tab1->getColumnName(0),"col111");
    EXPECT_EQ(tab1->getColumnIndex("col111"),0);
    EXPECT_EQ(tab1->getColumnIndex("col1111"),-1);
    EXPECT_TRUE(tab1->contain("col111"));
	
    EXPECT_EQ(tab1->getColumnLabel()->getString(),"[\"col111\",\"col2\"]");
    EXPECT_EQ(tab1->values()->getString(),"([1,2,3],[\"a\",\"b\",\"c\"])");

    cout<<"-------------------------------------"<<endl;
    EXPECT_EQ(tab1->getWindow(0,2,0,2)->getColumn(0)->getValue()->getString(),"[1,2]");
    EXPECT_EQ(tab1->getWindow(0,2,0,2)->getColumn(1)->getValue()->getString(),"[\"a\",\"b\"]");

    EXPECT_EQ(tab1->getWindow(0,-1,0,2)->getColumn(0)->getValue()->getString(),"[1,2]");
    EXPECT_EQ(tab1->getWindow(0,1,0,1)->getColumn(0)->getValue()->getString(),"[1]");
    EXPECT_EQ(tab1->getMember(Util::createString("col111"))->getString(),"[1,2,3]");
    VectorSP memvec=Util::createVector(DT_STRING,2,2);
    memvec->set(0,Util::createString("col111"));
    memvec->set(1,Util::createString("col2"));
    EXPECT_EQ(tab1->getMember(memvec)->get(0)->getString(),"[1,2,3]");
    EXPECT_EQ(tab1->getMember(memvec)->get(1)->getString(),"[\"a\",\"b\",\"c\"]");
    EXPECT_EQ(tab1->getValue()->getColumn(0)->getString(),"[1,2,3]");
    EXPECT_EQ(tab1->getValue()->getColumn(1)->getString(),"[\"a\",\"b\",\"c\"]");
    EXPECT_EQ(tab1->getValue(0)->getColumn(0)->getString(),"[1,2,3]");
    EXPECT_EQ(tab1->getValue(0)->getColumn(1)->getString(),"[\"a\",\"b\",\"c\"]");
    EXPECT_EQ(tab1->getValue(2)->getColumn(0)->getString(),"[1,2,3]");
    EXPECT_EQ(tab1->getValue(2)->getColumn(1)->getString(),"[\"a\",\"b\",\"c\"]");
    EXPECT_EQ(tab1->getSubTable({0})->getForm(),DF_TABLE);
    EXPECT_EQ(tab1->getSubTable({0})->getRow(0)->getString(),tab1->getRow(0)->getString());

    cout<<tab1->getInstance(tab1->size())->getColumn(0)->getString()<<endl;
    cout<<tab1->getInstance(tab1->size())->getColumn(1)->getString()<<endl;

}
TEST_F(FunctionTest,test_function_is){
    TableSP tab1=conn.run("u");
    ConstantSP intval=conn.run("a");
    VectorSP vec1= conn.run("vec");
    VectorSP av1=conn.run("v");
    SetSP set1=conn.run("w");
    DictionarySP dict1=conn.run("y");
    ConstantSP voidval=conn.run("d");
    ConstantSP uuidval=conn.run("s");
    VectorSP matrixval=conn.run("x");
    EXPECT_FALSE(tab1->isDatabase());
    EXPECT_FALSE(intval->isDatabase());
    EXPECT_TRUE(intval->isNumber());
    char *buf = new char[2];
    EXPECT_FALSE(dict1->isValid(0,1,buf));
    EXPECT_FALSE(dict1->isNull(0,1,buf));
    EXPECT_FALSE(intval->isLargeConstant());
    EXPECT_FALSE(intval->isIndexArray());
    EXPECT_FALSE(vec1->isHugeIndexArray());
    EXPECT_ANY_THROW(av1->isSorted(true));
    EXPECT_TRUE(set1->isLargeConstant());
    EXPECT_TRUE(dict1->isLargeConstant());
    EXPECT_TRUE(tab1->isLargeConstant());

    EXPECT_FALSE(uuidval->isIndexArray());
    EXPECT_FALSE(uuidval->isHugeIndexArray());
    EXPECT_TRUE(matrixval->isLargeConstant());
    EXPECT_FALSE(vec1->isLargeConstant());
}

TEST_F(FunctionTest,test_function_set){
    VectorSP vec1= conn.run("vec");
    ConstantSP uuidval=conn.run("s");
    DictionarySP dict1=conn.run("y");
    ConstantSP voidval=conn.run("d");
    ConstantSP intval=conn.run("a");
    TableSP tab1=conn.run("u");

    intval->setChar((char)5);
    EXPECT_EQ(intval->getChar(),(char)5);
    intval->setLong((long long)5);
    EXPECT_EQ(intval->getLong(),(long long)5);
    intval->setIndex((INDEX)5);
    EXPECT_EQ(intval->getIndex(),(INDEX)5);
    intval->setFloat((float)5);
    EXPECT_EQ(intval->getFloat(),(float)5);
    unsigned char* val=new unsigned char[1];
    intval->setBinary(val,1);

    vec1->setChar(0,1);
    EXPECT_EQ(vec1->get(0)->getChar(),(char)1);
    vec1->setLong(0,1);
    EXPECT_EQ(vec1->get(0)->getLong(),(long long)1);
    vec1->setIndex(0,1);
    EXPECT_EQ(vec1->get(0)->getIndex(),(INDEX)1);
    vec1->setFloat(0,1);
    EXPECT_EQ(vec1->get(0)->getFloat(),(float)1);
    vec1->setNull(0);
    EXPECT_TRUE(vec1->get(0)->isNull());
    vec1->setItem(0,Util::createInt(6));
    EXPECT_EQ(vec1->get(0)->getInt(),6); 

    EXPECT_FALSE(uuidval->set(0,Util::createInt(1)));
    EXPECT_FALSE(uuidval->set(0,0,Util::createInt(1)));
    EXPECT_FALSE(uuidval->set(Util::createInt(0),Util::createInt(1)));
    EXPECT_FALSE(uuidval->setColumn(0,Util::createInt(1)));
    uuidval->setRowLabel(Util::createInt(1));
    uuidval->setColumnLabel(Util::createInt(1));
    uuidval->setNullFlag(false);

    char *buf = new char[2]; 
    short *buf1 = new short[2];
    int *buf2 = new int[2];
    INDEX *buf4 = new INDEX[2];
    long long *buf3 = new long long[2];
    float *buf5 = new float[2];
    double *buf6 = new double[2];
    char **buf9 = new char*[2];
    unsigned char *buf10 = new unsigned char[2];
    SymbolBase *symbase= new SymbolBase(1);
    void *buf11=NULL;

    EXPECT_FALSE(uuidval->setBool(0,1,buf));
    EXPECT_FALSE(uuidval->setChar(0,1,buf));
    EXPECT_FALSE(uuidval->setShort(0,1,buf1)); 
    EXPECT_FALSE(uuidval->setInt(0,1,buf2));
    EXPECT_FALSE(uuidval->setLong(0,1,buf3));
    EXPECT_FALSE(uuidval->setIndex(0,1,buf4));
    EXPECT_FALSE(uuidval->setFloat(0,1,buf5));
    EXPECT_FALSE(uuidval->setDouble(0,1,buf6));   
    EXPECT_FALSE(uuidval->setString(0,1,buf9));
    EXPECT_FALSE(voidval->setBinary(0,1,1,buf10));
    EXPECT_FALSE(voidval->setData(0,1,buf2));

    dict1->set("123",Util::createInt(1));
    EXPECT_FALSE(tab1->set(0,Util::createInt(5)));
    cout<<tab1->getString();

}

TEST_F(FunctionTest,test_function_has){}

TEST_F(FunctionTest,test_function_append){
    TableSP tab1=conn.run("u");
    string errMsg1;
    INDEX insertedRows=1;
    vector<ConstantSP> cols4={Util::createInt(4),Util::createString("d")};

    tab1->setReadOnly(true);
    EXPECT_FALSE(tab1->append(cols4,insertedRows,errMsg1));
    EXPECT_EQ(errMsg1,"Can't modify read only table.");
    tab1->setReadOnly(false);

    //append constant
    vector<string> colNames = { "col1", "col2" };
    vector<ConstantSP> err_colnum_values={Util::createInt(4)};
    EXPECT_FALSE(tab1->append(err_colnum_values,insertedRows,errMsg1));
    EXPECT_EQ(errMsg1, "The number of table columns doesn't match the number of columns to append.");

    errMsg1.clear();
    VectorSP tempVec=Util::createVector(DT_STRING,2,2);
    vector<ConstantSP> err_colsize_values={Util::createInt(4),tempVec};
    EXPECT_FALSE(tab1->append(err_colsize_values,insertedRows,errMsg1));
    EXPECT_EQ(errMsg1, "Inconsistent length of values to insert.");

    errMsg1.clear();
    vector<ConstantSP> err_coltype_values={Util::createString("2"),Util::createString("d")};
    EXPECT_FALSE(tab1->append(err_coltype_values,insertedRows,errMsg1));
    EXPECT_EQ(errMsg1, "Failed to append data to column 'col1' with error: Incompatible type. Expected: INT, Actual: STRING");


    //append tuple
    errMsg1.clear();
    VectorSP tuple_value=Util::createVector(DT_ANY,2,2);
    tuple_value->set(0,Util::createInt(4));
    tuple_value->set(1,Util::createString("d"));
    VectorSP err_colnum_tuple=Util::createVector(DT_ANY,1,2);
    err_colnum_tuple->set(0,Util::createInt(4));
    VectorSP err_coltype_tuple=Util::createVector(DT_ANY,2,2);
    err_coltype_tuple->set(0,Util::createString("4"));
    err_coltype_tuple->set(1,Util::createInt(5));

    vector<ConstantSP> tuple_values={tuple_value};
	EXPECT_TRUE(tab1->append(tuple_values,insertedRows,errMsg1));
    vector<ConstantSP> tuple_values1={err_colnum_tuple};
	EXPECT_FALSE(tab1->append(tuple_values1,insertedRows,errMsg1));
    vector<ConstantSP> tuple_values2={err_coltype_tuple};
	EXPECT_FALSE(tab1->append(tuple_values2,insertedRows,errMsg1));

    //append table
    cout<<tab1->getString();
    errMsg1.clear();
    vector<ConstantSP> tabVec={};
  
    vector<ConstantSP> errSizeCols={Util::createInt(4),Util::createString("d"),Util::createInt(0)};
    vector<string> errSizecolNames = { "col1", "col2", "col3"};
    TableSP tab_3_cols=Util::createTable(errSizecolNames, errSizeCols);
    tabVec.emplace_back(tab_3_cols);
    EXPECT_FALSE(tab1->append(tabVec,insertedRows,errMsg1));
    EXPECT_EQ(errMsg1,"Number of columns for the original table and the table to insert are different.");

    errMsg1.clear();
    tabVec.clear();
    vector<ConstantSP> errTypeCols={Util::createString("2"),Util::createString("d")};
    TableSP tab_errType_cols=Util::createTable(colNames, errTypeCols);
    tabVec.emplace_back(tab_errType_cols);
    EXPECT_FALSE(tab1->append(tabVec,insertedRows,errMsg1));
    EXPECT_EQ(errMsg1,"Failed to append data to column 'col1' with error: Incompatible type. Expected: INT, Actual: STRING");

}

TEST_F(FunctionTest,test_function_update){
    TableSP tab1=conn.run("u");
    vector<string> colNames = { "col1", "col2" };
    VectorSP indexs = Util::createIndexVector(0,1);

    VectorSP val1 = Util::createVector(DT_INT,1,1);
    val1->set(0,Util::createInt(5));
    VectorSP val2 = Util::createVector(DT_STRING,1,1);
    val2->set(0,Util::createString("f"));
    vector<ConstantSP> vector_cols={val1,val2};
    vector<ConstantSP> row={Util::createInt(4),Util::createString("d")};
    vector<ConstantSP> errtype_cols={Util::createString("4"),Util::createString("d")};
    vector<ConstantSP> err_colnum_values={Util::createVector(DT_INT,2,2)};
    string errMsg1;
    INDEX insertedRows=1;

    tab1->setReadOnly(true);
    EXPECT_FALSE(tab1->update(row,Util::createInt(0),colNames,errMsg1));
    EXPECT_EQ(errMsg1,"Can't modify read only table.");
    errMsg1.clear();
    tab1->setReadOnly(false); 

    EXPECT_TRUE(tab1->update(row,Util::createInt(2),colNames,errMsg1));
    // EXPECT_ANY_THROW(tab1->update(row,Util::createInt(3),colNames,errMsg1));
    tab1->update(errtype_cols,Util::createInt(2),colNames,errMsg1);
    EXPECT_EQ(errMsg1,"The category of the value to update does not match the column col1");

    errMsg1.clear();
    vector<string> err_colname={"col3","col2"};
    tab1->update(row,Util::createInt(2),err_colname,errMsg1);
    EXPECT_EQ(errMsg1,"The column col3 does not exist. To add a new column, the table shouldn't be shared and the value size must match the table.");

    errMsg1.clear();
    tab1->update(err_colnum_values,Util::createInt(2),colNames,errMsg1);
    EXPECT_EQ(errMsg1,"Inconsistent length of values to update.");

    errMsg1.clear();
    cout<<tab1->getString()<<endl;
    EXPECT_TRUE(tab1->update(row,indexs,colNames,errMsg1));
    cout<<tab1->getString()<<endl;
    EXPECT_EQ(tab1->getRow(0)->getMember(Util::createString("col1"))->getInt(),4);
    EXPECT_EQ(tab1->getRow(0)->getMember(Util::createString("col2"))->getString(),"d");

    errMsg1.clear();
    tab1->update(vector_cols,indexs,colNames,errMsg1);
    EXPECT_EQ(tab1->getRow(0)->getMember(Util::createString("col1"))->getInt(),5);
    EXPECT_EQ(tab1->getRow(0)->getMember(Util::createString("col2"))->getString(),"f");

}

TEST_F(FunctionTest,test_function_remove){
    TableSP tab1=conn.run("u");
    
    string errMsg1;
    VectorSP vec1=Util::createVector(DT_INT,0,1);
    vec1->append(Util::createInt(1));

    tab1->setReadOnly(true);
    EXPECT_FALSE(tab1->remove(vec1,errMsg1));
    EXPECT_EQ(errMsg1,"Can't remove rows from a read only in-memory table.");
    errMsg1.clear();
    tab1->setReadOnly(false);

    EXPECT_ANY_THROW(tab1->remove(Util::createInt(1),errMsg1)); //not surport remove with scalar parameter.
    EXPECT_TRUE(tab1->remove(vec1,errMsg1));

    errMsg1.clear();
    EXPECT_EQ(tab1->rows(),2);
    EXPECT_TRUE(tab1->remove(NULL,errMsg1));

    errMsg1.clear();
    EXPECT_EQ(tab1->rows(),0);
}

TEST_F(FunctionTest,test_function_drop){
    TableSP tab1=conn.run("u");

    tab1->setReadOnly(true);
    vector<int> *dropColsIndex=new vector<int>;
    dropColsIndex->push_back(1);
    EXPECT_ANY_THROW(tab1->drop(*dropColsIndex));
    tab1->setReadOnly(false);

    dropColsIndex->clear();
    dropColsIndex->push_back(1);
    tab1->drop(*dropColsIndex);
    EXPECT_EQ(tab1->columns(),1);

    TableSP tab2=conn.run("u");
    dropColsIndex->clear();
    dropColsIndex->push_back(2);
    tab2->drop(*dropColsIndex);
    EXPECT_EQ(tab2->columns(),2);

    TableSP tab3=conn.run("u");
    dropColsIndex->clear();
    dropColsIndex->push_back(0);
    dropColsIndex->push_back(1);
    tab3->drop(*dropColsIndex);
    EXPECT_EQ(tab3->columns(),0);

}


TEST_F(FunctionTest,test_function_add){}

TEST_F(FunctionTest,test_Util_functions){
    cout << Util::escape((char)14)<<endl;

    EXPECT_EQ(Util::getMonthEnd(1),30);
    EXPECT_EQ(Util::getMonthEnd(31),58);
    EXPECT_EQ(Util::getMonthStart(1),0);
    EXPECT_EQ(Util::getMonthStart(60),59);

    char *buf = Util::allocateMemory(10);
    buf="0123456789";
    EXPECT_TRUE(Util::allocateMemory(-1,false)==NULL);
    EXPECT_ANY_THROW(Util::allocateMemory(-1));

    DictionarySP dict1=Util::createDictionary(DT_TIME,DT_ANY);
    DictionarySP dict2=Util::createDictionary(DT_STRING,DT_ANY);
    DictionarySP dict3=Util::createDictionary(DT_STRING,DT_DATEHOUR);
    EXPECT_FALSE(Util::isFlatDictionary(dict1.get()));
    EXPECT_TRUE(Util::isFlatDictionary(dict2.get()));
    EXPECT_TRUE(Util::isFlatDictionary(dict3.get()));
    dict2->set(Util::createString("str1"),Util::createNullConstant(DT_INT));
    EXPECT_FALSE(Util::isFlatDictionary(dict2.get()));
    dict3->set(Util::createString("str1"),Util::createDateHour(1000));
    EXPECT_FALSE(Util::isFlatDictionary(dict3.get()));

    EXPECT_EQ(Util::getDataType("int"),DT_INT);
    EXPECT_EQ(Util::getDataForm("vector"),DF_VECTOR);
    EXPECT_EQ(Util::getDataTypeString(DT_INT),"INT");
    EXPECT_EQ(Util::getDataFormString(DF_VECTOR),"VECTOR");

    EXPECT_EQ(Util::getDataType('v'),DT_VOID);
    EXPECT_EQ(Util::getDataType('b'),DT_BOOL);
    EXPECT_EQ(Util::getDataType('c'),DT_CHAR);
    EXPECT_EQ(Util::getDataType('h'),DT_SHORT);
    EXPECT_EQ(Util::getDataType('i'),DT_INT);
    EXPECT_EQ(Util::getDataType('f'),DT_FLOAT);
    EXPECT_EQ(Util::getDataType('F'),DT_DOUBLE);
    EXPECT_EQ(Util::getDataType('d'),DT_DATE);
    EXPECT_EQ(Util::getDataType('M'),DT_MONTH);
    EXPECT_EQ(Util::getDataType('m'),DT_MINUTE);
    EXPECT_EQ(Util::getDataType('s'),DT_SECOND);
    EXPECT_EQ(Util::getDataType('t'),DT_TIME);
    EXPECT_EQ(Util::getDataType('D'),DT_DATETIME);
    EXPECT_EQ(Util::getDataType('T'),DT_TIMESTAMP);
    EXPECT_EQ(Util::getDataType('n'),DT_NANOTIME);
    EXPECT_EQ(Util::getDataType('N'),DT_NANOTIMESTAMP);
    EXPECT_EQ(Util::getDataType('S'),DT_SYMBOL);
    EXPECT_EQ(Util::getDataType('W'),DT_STRING);
    EXPECT_EQ(Util::getDataType('l'),DT_LONG);

    VectorSP matrixval = Util::createDoubleMatrix(1,1);
    string ex_martval=conn.run("matrix(DOUBLE,1,1)")->getString();
    EXPECT_EQ(ex_martval,matrixval->getString());
    EXPECT_TRUE(matrixval->isMatrix());
    EXPECT_EQ(matrixval->getForm(),DF_MATRIX);
    EXPECT_EQ(matrixval->getType(),DT_DOUBLE);

    VectorSP indexvec=Util::createIndexVector(-1,-1);
    VectorSP indexvec1=Util::createIndexVector(0,1);
    cout<<indexvec->getString()<<endl;
    string ex_indexvec=conn.run("array(INDEX,1,1)")->getString();
    EXPECT_EQ(indexvec1->getString(),ex_indexvec);

    EXPECT_EQ(Util::trim(" 1 2 3      "),"1 2 3");
    EXPECT_EQ(Util::ltrim("   1 2 3      "), "1 2 3      ");

    EXPECT_EQ(Util::strip(" \t\r\n 1 2 3 \t\n\r"),"1 2 3");
    EXPECT_EQ(Util::wc("1 23 4 abc A *&^%$#!\t\n\r"),5);

    EXPECT_EQ(Util::replace("abc","d","e"),"abc");
    EXPECT_EQ(Util::replace("abc","c","e"),"abe");
    EXPECT_EQ(Util::replace("abc","a","cba"),"cbabc");

    EXPECT_EQ(Util::replace("abc",'d','e'),"abc");
    EXPECT_EQ(Util::replace("abc",'c','e'),"abe");
    // EXPECT_EQ(Util::replace("abc",'a','cba'),"cbabc");

    EXPECT_EQ(Util::upper("abc"),"ABC");
    EXPECT_EQ(Util::toUpper('a'),'A');

    EXPECT_EQ(Util::longToString((long long)999999999999999),"999999999999999");
    EXPECT_EQ(Util::doubleToString((double)2.321597810),"2.321598");

    EXPECT_FALSE(Util::endWith("dolphindb", ""));
    EXPECT_FALSE(Util::endWith("dolphindb", "nihao"));
    EXPECT_TRUE(Util::endWith("dolphindb", "db"));

    EXPECT_FALSE(Util::startWith("dolphindb", ""));
    EXPECT_FALSE(Util::startWith("dolphindb", "nihao"));
    EXPECT_TRUE(Util::startWith("dolphindb", "dolphin"));

    string teststrval="abc\"123\" dolphindb";
    EXPECT_EQ(Util::literalConstant(teststrval),"\"abc\\\"123\\\" dolphindb\"");

    cout<<Util::getNanoBenchmark()<<endl;
    cout<<Util::getNanoEpochTime()<<endl;
    tm local_time;
    Util::getLocalTime(local_time);
    cout<<to_string(1900+local_time.tm_year)+"."+to_string(1+local_time.tm_mon)+"."+to_string(local_time.tm_mday)+" "+to_string(local_time.tm_hour)+":"+to_string(local_time.tm_min)+":"+to_string(local_time.tm_sec)<<endl;
    
    int* timeval_int=new int[1];
    timeval_int[0]=60;
    Util::toLocalDateTime(timeval_int,1);
    EXPECT_EQ(timeval_int[0],Util::toLocalDateTime(60));

    // long long* timeval_long=new long long[1];
    // timeval_long[0]=(long long)10000000000;
    // Util::toLocalNanoTimestamp(timeval_long,1);
    // EXPECT_EQ(timeval_long[0],Util::toLocalNanoTimestamp((long long)10000000000));

    long long* timeval_long2=new long long[1];
    timeval_long2[0]=(long long)10000000000;
    Util::toLocalTimestamp(timeval_long2,1);
    EXPECT_EQ(timeval_long2[0],Util::toLocalTimestamp((long long)10000000000));

    EXPECT_FALSE(Util::strWildCmp("dolphindb","DolphinDB"));
    EXPECT_TRUE(Util::strWildCmp("DolphinDB","DolphinDB"));

    #ifdef WINDOWS
    EXPECT_TRUE(Util::isWindows());
    #else
    EXPECT_FALSE(Util::isWindows());
    #endif

    cout<<Util::getCoreCount()<<endl;
    cout<<Util::getPhysicalMemorySize()<<endl;

    string dest = "dolphindb";
    string source = "1";
    Util::writeDoubleQuotedString(dest,source);
    EXPECT_EQ(dest,"dolphindb\"1\"");

    cout<<Util::getLastErrorCode()<<endl;
    cout<<Util::getLastErrorMessage()<<endl;
    cout<<Util::getErrorMessage(Util::getLastErrorCode())<<endl;

    EXPECT_EQ(Util::getPartitionTypeString(VALUE),"VALUE");

    cout<<"-----------test Util::createObject()--------------"<<endl;
    nullptr_t voidconst = nullptr;
    bool boolconst = true;
    char charconst = 1;
    short shortconst = 1;
    const char* pcharconst = "1";
    string strconst = "dolphindb";
    // unsigned char charconst2 = 1;
    // const unsigned char* pval = "1";
    unsigned char charconstvec[] = {1};
    long long longconst = 1;
    long int longintconst = 1;
    int intconst = 1;
    float floatconst = 1;
    double doubleconst = 1;
    vector<DATA_TYPE> testTypes = {DT_BOOL,DT_CHAR,DT_SHORT,DT_INT,DT_LONG,DT_DATE,DT_MONTH,DT_TIME,
                                    DT_MINUTE,DT_SECOND,DT_DATETIME,DT_TIMESTAMP,DT_NANOTIME,DT_NANOTIMESTAMP,
                                    DT_FLOAT,DT_DOUBLE,DT_SYMBOL,DT_STRING,DT_UUID,DT_DATEHOUR,DT_IP,DT_INT128,DT_BLOB };

    for(int i =0;i<testTypes.size();i++){
        ConstantSP ddbval=Util::createObject((DATA_TYPE)testTypes[i],voidconst);
        EXPECT_TRUE(ddbval->getType()==(DATA_TYPE)testTypes[i] || ddbval->getType()==DT_STRING);
        EXPECT_TRUE(ddbval->isNull());
    }

    for(int i =0;i<testTypes.size();i++){
        DATA_TYPE dataType = (DATA_TYPE)testTypes[i];
        switch (dataType) {
        case DATA_TYPE::DT_BOOL:
        {
            ConstantSP ddbval=Util::createObject(dataType,boolconst);
            EXPECT_EQ(ddbval->getBool(),boolconst);
            EXPECT_TRUE(ddbval->getType()==dataType);
            break;
        }
	    default:
        {
            EXPECT_ANY_THROW(Util::createObject(dataType,boolconst));
            break;
        }
        }
    }

    for(int i =0;i<testTypes.size();i++){
        DATA_TYPE dataType = (DATA_TYPE)testTypes[i];
        if(i<5){
            ConstantSP ddbval=Util::createObject(dataType,charconst);
            EXPECT_EQ(ddbval->getBool(),charconst);
            EXPECT_TRUE(ddbval->getType()==dataType);
        }
	    else
        {
            EXPECT_ANY_THROW(Util::createObject(dataType,charconst));
        }

    }

    for(int i =0;i<testTypes.size();i++){
        DATA_TYPE dataType = (DATA_TYPE)testTypes[i];
        if(i<5 && i!=0){
            ConstantSP ddbval=Util::createObject(dataType,shortconst);
            EXPECT_EQ(ddbval->getShort(),shortconst);
            EXPECT_TRUE(ddbval->getType()==dataType);
        }
	    else
        {
            EXPECT_ANY_THROW(Util::createObject(dataType,shortconst));
        }

    }

    for(int i =0;i<testTypes.size();i++){
        DATA_TYPE dataType = (DATA_TYPE)testTypes[i];
        if((i<14 && i!=0) || i==19){
            ConstantSP ddbval=Util::createObject(dataType,longconst);
            EXPECT_EQ(ddbval->getLong(),longconst);
            EXPECT_TRUE(ddbval->getType()==dataType);
        }
	    else
        {
            EXPECT_ANY_THROW(Util::createObject(dataType,longconst));
        }

    }

    for(int i =0;i<testTypes.size();i++){
        DATA_TYPE dataType = (DATA_TYPE)testTypes[i];
        if((i<14 && i!=0) || i==19){
            ConstantSP ddbval=Util::createObject(dataType,longintconst);
            EXPECT_EQ(ddbval->getInt(),longintconst);
            EXPECT_TRUE(ddbval->getType()==dataType);
        }
	    else
        {
            EXPECT_ANY_THROW(Util::createObject(dataType,longintconst));
        }

    }

    for(int i =0;i<testTypes.size();i++){
        DATA_TYPE dataType = (DATA_TYPE)testTypes[i];
        if((i<11 && i!=0) || i==19){
            ConstantSP ddbval=Util::createObject(dataType,intconst);
            EXPECT_EQ(ddbval->getInt(),intconst);
            EXPECT_TRUE(ddbval->getType()==dataType);
        }
	    else
        {
            EXPECT_ANY_THROW(Util::createObject(dataType,intconst));
        }

    }

    for(int i =0;i<testTypes.size();i++){
        DATA_TYPE dataType = (DATA_TYPE)testTypes[i];
        if(i==14||i==15) {
            ConstantSP ddbval=Util::createObject(dataType,doubleconst);
            EXPECT_EQ(ddbval->getDouble(),doubleconst);
            EXPECT_TRUE(ddbval->getType()==dataType);
        }
	    else
        {
            EXPECT_ANY_THROW(Util::createObject(dataType,doubleconst));
        }

    }

    for(int i =0;i<testTypes.size();i++){
        DATA_TYPE dataType = (DATA_TYPE)testTypes[i];
        if(i==14||i==15) {
            ConstantSP ddbval=Util::createObject(dataType,floatconst);
            EXPECT_EQ(ddbval->getFloat(),floatconst);
            EXPECT_TRUE(ddbval->getType()==dataType);
        }
	    else
        {
            EXPECT_ANY_THROW(Util::createObject(dataType,floatconst));
        }

    }

    for(int i =0;i<testTypes.size();i++){
        DATA_TYPE dataType = (DATA_TYPE)testTypes[i];
        if( i==16 || i==17 || i==22){
            ConstantSP ddbval=Util::createObject(dataType,pcharconst);
            EXPECT_EQ(ddbval->getString(),"1");
            EXPECT_TRUE(ddbval->getType()==dataType || ddbval->getType()==DT_STRING);
        }
	    else
        {
            EXPECT_ANY_THROW(Util::createObject(dataType,pcharconst));
        }

    }

    for(int i =0;i<testTypes.size();i++){
        DATA_TYPE dataType = (DATA_TYPE)testTypes[i];
        if(i==16 || i==17 || i==22){
            ConstantSP ddbval=Util::createObject(dataType,strconst);
            EXPECT_EQ(ddbval->getString(),strconst);
            EXPECT_TRUE(ddbval->getType()==dataType || ddbval->getType()==DT_STRING);
        }
	    else
        {
            EXPECT_ANY_THROW(Util::createObject(dataType,strconst));
        }

    }

    vector<nullptr_t> voidconstVec = {nullptr,nullptr};
    vector<bool> boolconstVec = {true};
    vector<char> charconstVec = {1};
    vector<short> shortconstVec = {1};
    vector<const char*> pcharconstVec = {"1"};
    vector<string> strconstVec = {"dolphindb"};
    vector<unsigned char> charconst2Vec = {1};
    vector<const unsigned char*> pvalVec = {&charconst2Vec[0]};
    vector<long long> longconstVec = {1};
    vector<long int> longintconstVec = {1};
    vector<int> intconstVec = {1};
    vector<float> floatconstVec = {1};
    vector<double> doubleconstVec = {1};

    for(int i =0;i<testTypes.size();i++){
        VectorSP ddbval=Util::createObject((DATA_TYPE)testTypes[i],voidconstVec);
        EXPECT_EQ(ddbval->getForm(), DF_VECTOR);
        EXPECT_TRUE(ddbval->get(0)->getType()==(DATA_TYPE)testTypes[i] || ddbval->getType()==DT_STRING);
        EXPECT_TRUE(ddbval->get(0)->get(0)->isNull());
        EXPECT_TRUE(ddbval->get(0)->get(1)->isNull());
        // EXPECT_EQ(ddbval->get(0)->isNull());
    }

    for(int i =0;i<testTypes.size();i++){
        DATA_TYPE dataType = (DATA_TYPE)testTypes[i];
        {
            EXPECT_ANY_THROW(Util::createObject(dataType,boolconstVec));
        }

    }

    for(int i =0;i<testTypes.size();i++){
        DATA_TYPE dataType = (DATA_TYPE)testTypes[i];
        if(i<5){
            VectorSP ddbval=Util::createObject(dataType,charconstVec);
            EXPECT_EQ(ddbval->getForm(), DF_VECTOR);
            EXPECT_EQ(ddbval->get(0)->getChar(),charconstVec[0]);
            EXPECT_TRUE(ddbval->get(0)->getType()==dataType);
        }
	    else
        {
            EXPECT_ANY_THROW(Util::createObject(dataType,charconstVec));
        }

    }

    for(int i =0;i<testTypes.size();i++){
        DATA_TYPE dataType = (DATA_TYPE)testTypes[i];
        {
            EXPECT_ANY_THROW(Util::createObject(dataType,charconst2Vec));
        }

    }

    for(int i =0;i<testTypes.size();i++){
        DATA_TYPE dataType = (DATA_TYPE)testTypes[i];
        if(i<5 && i!=0){
            VectorSP ddbval=Util::createObject(dataType,shortconstVec);
            EXPECT_EQ(ddbval->getForm(), DF_VECTOR);
            EXPECT_EQ(ddbval->get(0)->getShort(),shortconstVec[0]);
            EXPECT_TRUE(ddbval->get(0)->getType()==dataType);
        }
	    else
        {
            EXPECT_ANY_THROW(Util::createObject(dataType,shortconstVec));
        }

    }

    for(int i =0;i<testTypes.size();i++){
        DATA_TYPE dataType = (DATA_TYPE)testTypes[i];
        if((i<14 && i!=0) || i==19){
            VectorSP ddbval=Util::createObject(dataType,longconstVec);
            EXPECT_EQ(ddbval->getForm(), DF_VECTOR);
            EXPECT_EQ(ddbval->get(0)->getLong(),longconstVec[0]);
            EXPECT_TRUE(ddbval->get(0)->getType()==dataType);
        }
	    else
        {
            EXPECT_ANY_THROW(Util::createObject(dataType,longconstVec));
        }

    }

    for(int i =0;i<testTypes.size();i++){
        DATA_TYPE dataType = (DATA_TYPE)testTypes[i];
        if((i<14 && i!=0) || i==19){
            VectorSP ddbval=Util::createObject(dataType,longintconstVec);
            EXPECT_EQ(ddbval->getForm(), DF_VECTOR);
            EXPECT_EQ(ddbval->get(0)->getInt(),longintconstVec[0]);
            EXPECT_TRUE(ddbval->get(0)->getType()==dataType);
        }
	    else
        {
            EXPECT_ANY_THROW(Util::createObject(dataType,longintconstVec));
        }

    }

    for(int i =0;i<testTypes.size();i++){
        DATA_TYPE dataType = (DATA_TYPE)testTypes[i];
        if((i<11 && i!=0) || i==19){
            VectorSP ddbval=Util::createObject(dataType,intconstVec);
            EXPECT_EQ(ddbval->getForm(), DF_VECTOR);
            EXPECT_EQ(ddbval->get(0)->getInt(),intconstVec[0]);
            EXPECT_TRUE(ddbval->get(0)->getType()==dataType);
        }
	    else
        {
            EXPECT_ANY_THROW(Util::createObject(dataType,intconstVec));
        }

    }

    for(int i =0;i<testTypes.size();i++){
        DATA_TYPE dataType = (DATA_TYPE)testTypes[i];
        if(i==14||i==15) {
            VectorSP ddbval=Util::createObject(dataType,doubleconstVec);
            EXPECT_EQ(ddbval->getForm(), DF_VECTOR);
            EXPECT_EQ(ddbval->get(0)->getDouble(),doubleconstVec[0]);
            EXPECT_TRUE(ddbval->get(0)->getType()==dataType);
        }
	    else
        {
            EXPECT_ANY_THROW(Util::createObject(dataType,doubleconstVec));
        }

    }

    for(int i =0;i<testTypes.size();i++){
        DATA_TYPE dataType = (DATA_TYPE)testTypes[i];
        if(i==14||i==15) {
            VectorSP ddbval=Util::createObject(dataType,floatconstVec);
            EXPECT_EQ(ddbval->getForm(), DF_VECTOR);
            EXPECT_EQ(ddbval->get(0)->getFloat(),floatconstVec[0]);
            EXPECT_TRUE(ddbval->get(0)->getType()==dataType);
        }
	    else
        {
            EXPECT_ANY_THROW(Util::createObject(dataType,floatconstVec));
        }

    }

    for(int i =0;i<testTypes.size();i++){
        DATA_TYPE dataType = (DATA_TYPE)testTypes[i];
        if( i==16 || i==17 || i==22){
            VectorSP ddbval=Util::createObject(dataType,pcharconstVec);
            EXPECT_EQ(ddbval->get(0)->getString(),"[\"1\"]");
            EXPECT_TRUE(ddbval->get(0)->getType()==dataType || ddbval->get(0)->getType()==DT_STRING);
        }
	    else
        {
            EXPECT_ANY_THROW(Util::createObject(dataType,pcharconstVec));
        }

    }

    for(int i =0;i<testTypes.size();i++){
        DATA_TYPE dataType = (DATA_TYPE)testTypes[i];
        if(i==16 || i==17 || i==22){
            VectorSP ddbval=Util::createObject(dataType,strconstVec);
            EXPECT_EQ(ddbval->get(0)->getString(),"[\""+strconstVec[0]+"\"]");
            EXPECT_TRUE(ddbval->get(0)->getType()==dataType || ddbval->get(0)->getType()==DT_STRING);
        }
	    else
        {
            EXPECT_ANY_THROW(Util::createObject(dataType,strconstVec));
        }

    }


    cout<<"-----------test Util::parseConstant()--------------"<<endl;
    EXPECT_EQ(Util::parseYear(365),1971);
    EXPECT_EQ(Util::parseYear(0),1970);
    int year,month,day;
    Util::parseDate(365,year,month,day);
    cout<<year<<month<<day<<endl;
    EXPECT_EQ(year,1971);
    EXPECT_EQ(month,1);
    EXPECT_EQ(day,1);

    ConstantSP voidval=Util::parseConstant(DT_VOID,"");
    ConstantSP boolval=Util::parseConstant(DT_BOOL,"1");
    ConstantSP charval=Util::parseConstant(DT_CHAR,"1");
    ConstantSP shortval=Util::parseConstant(DT_SHORT,"1");
    ConstantSP intval=Util::parseConstant(DT_INT,"1");
    ConstantSP longval=Util::parseConstant(DT_LONG,"1");
    ConstantSP dateval=Util::parseConstant(DT_DATE,"2013.06.13");
    ConstantSP monthval=Util::parseConstant(DT_MONTH,"2012.06");
    ConstantSP timeval=Util::parseConstant(DT_TIME,"13:30:10.008");
    ConstantSP minuteval=Util::parseConstant(DT_MINUTE,"13:30");
    ConstantSP secondval=Util::parseConstant(DT_SECOND,"13:30:10");
    ConstantSP datetimeval=Util::parseConstant(DT_DATETIME,"2012.06.13T13:30:10");
    ConstantSP timestampval=Util::parseConstant(DT_TIMESTAMP,"2012.06.13T13:30:10.008");
    ConstantSP nanotimeval=Util::parseConstant(DT_NANOTIME,"13:30:10.008007006");
    ConstantSP nanotimestampval=Util::parseConstant(DT_NANOTIMESTAMP,"2012.06.13T13:30:10.008007006");
    ConstantSP floatval=Util::parseConstant(DT_FLOAT,"2.1");
    ConstantSP doubleval=Util::parseConstant(DT_DOUBLE,"2.1");
    // ConstantSP symbolval=Util::parseConstant(DT_SYMBOL,"sym"); //not support
    ConstantSP stringval=Util::parseConstant(DT_STRING,"str");
    ConstantSP uuidval=Util::parseConstant(DT_UUID,"5d212a78-cc48-e3b1-4235-b4d91473ee87");
    // ConstantSP functiondefval=Util::parseConstant(DT_FUNCTIONDEF,"def f1(a,b) {return a+b;}"); //not support
    ConstantSP datehourval=Util::parseConstant(DT_DATEHOUR,"2012.06.13T13");
    ConstantSP ipaddrval=Util::parseConstant(DT_IP,"192.168.1.13");
    ConstantSP int128val=Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec32");
    // ConstantSP blobval=Util::parseConstant(DT_BLOB,"blob1"); //not support
    vector<string> nameVec = {"voidval","boolval","charval","shortval","intval","longval","dateval","monthval","timeval","minuteval","secondval",\
                                "datetimeval","timestampval","nanotimeval","nanotimestampval","floatval","doubleval","stringval","uuidval",\
                                "datehourval","ipaddrval","int128val"};
    vector<ConstantSP> valVec = {voidval,boolval,charval,shortval,intval,longval,dateval,monthval,timeval,minuteval,secondval,datetimeval,timestampval,\
                                    nanotimeval,nanotimestampval,floatval,doubleval,stringval,uuidval,datehourval,ipaddrval,int128val};
    conn.upload(nameVec,valVec);

    EXPECT_TRUE(conn.run("eqObj(voidval,NULL)")->getBool());
    EXPECT_TRUE(conn.run("eqObj(boolval,true)")->getBool());
    EXPECT_TRUE(conn.run("eqObj(charval,char(1))")->getBool());
    EXPECT_TRUE(conn.run("eqObj(shortval,short(1))")->getBool());
    EXPECT_TRUE(conn.run("eqObj(intval,int(1))")->getBool());
    EXPECT_TRUE(conn.run("eqObj(longval,long(1))")->getBool());
    EXPECT_TRUE(conn.run("eqObj(dateval,2013.06.13)")->getBool());
    EXPECT_TRUE(conn.run("eqObj(monthval,2012.06M)")->getBool());
    EXPECT_TRUE(conn.run("eqObj(timeval,13:30:10.008)")->getBool());
    EXPECT_TRUE(conn.run("eqObj(minuteval,13:30m)")->getBool());
    EXPECT_TRUE(conn.run("eqObj(secondval,13:30:10)")->getBool());
    EXPECT_TRUE(conn.run("eqObj(datetimeval,2012.06.13T13:30:10)")->getBool());
    EXPECT_TRUE(conn.run("eqObj(timestampval,2012.06.13T13:30:10.008)")->getBool());
    EXPECT_TRUE(conn.run("eqObj(nanotimeval,13:30:10.008007006)")->getBool());
    EXPECT_TRUE(conn.run("eqObj(nanotimestampval,2012.06.13T13:30:10.008007006)")->getBool());
    EXPECT_TRUE(conn.run("eqObj(floatval,float(2.1))")->getBool());
    EXPECT_TRUE(conn.run("eqObj(doubleval,double(2.1))")->getBool());
    EXPECT_TRUE(conn.run("eqObj(stringval,\"str\")")->getBool());
    EXPECT_TRUE(conn.run("eqObj(uuidval,uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87'))")->getBool());
    EXPECT_TRUE(conn.run("eqObj(datehourval,datehour('2012.06.13T13'))")->getBool());
    EXPECT_TRUE(conn.run("eqObj(ipaddrval,ipaddr('192.168.1.13'))")->getBool());
    EXPECT_TRUE(conn.run("eqObj(int128val,int128('e1671797c52e15f763380b45e841ec32'))")->getBool());
    cout<<"--------------All cases passed----------------"<<endl;
}
#ifndef WINDOWS
TEST_F(FunctionTest,test_FastVector_get){
    // todo: when will the variable `index` not IndexArray
    auto base = new SymbolBase(0);
    base->findAndInsert("test1");
    base->findAndInsert("test2");
    int *test1 = new int[2];
    test1[0] = 1;
    test1[1] = 2;
    double *test2 = new double[2];
    test2[0] = 1.1;
    test2[1] = 1.2;
    auto fastSymbolVec = new FastSymbolVector(base, 2, 6, test1, true);
    auto indexVec1 = Util::createIndexVector(0, 2);
    auto indexVec2 = new FastDoubleVector(2, 2, test2, true);
    EXPECT_EQ(indexVec1->isIndexArray(), true);
    EXPECT_EQ(indexVec2->isIndexArray(), false);
    EXPECT_EQ(indexVec1->size(), 2);
    EXPECT_EQ(indexVec2->size(), 2);

    std::cout << fastSymbolVec->get(indexVec1)->getString() << std::endl;
    std::cout << fastSymbolVec->get(indexVec2)->getString() << std::endl;
}

TEST_F(FunctionTest,test_FastVector_fill){
    auto base = new SymbolBase(0);
    int *test1 = new int[2];
    test1[0] = 1;
    test1[1] = 2;
    auto fastSymbolVec = new FastSymbolVector(base, 2, 6, test1, true);
    auto val1 = ConstantSP(new String("test"));
    fastSymbolVec->fill(0, 1, val1);

    vector<string> test2 = {"test1", "test2"};
    auto val2 = ConstantSP(new StringVector(test2, test2.size(), true));
    fastSymbolVec->fill(0, 3, val2);
    fastSymbolVec->fill(0, 2, val2);

    // todo: getCategory() != LITERAL
    double *test3 = new double[2];
    test3[0] = 1.1;
    test3[1] = 1.2;
    auto val3 = ConstantSP(new FastDoubleVector(2, 2, test3, true));
    EXPECT_ANY_THROW(fastSymbolVec->fill(0, 2, val3));
    // EXPECT_NE(val3->getCategory(), LITERAL);
    // EXPECT_EQ(val3->getCategory(), DT_DOUBLE);

}


TEST_F(FunctionTest,Guid){
    Guid uuid(1, 0);
    EXPECT_FALSE(uuid.isZero());
    GuidHash hash;
    std::cout << hash(uuid) << std::endl;
    VectorSP con = Util::createVector(DT_INT, 0, 1);
    int num = 0, partial = 0;
    EXPECT_ANY_THROW(con->Constant::serialize(nullptr, 0, 0, 0, num, partial));
    EXPECT_ANY_THROW(con->Constant::serialize(nullptr, 0, 0, 0, 0, num, partial));
    EXPECT_ANY_THROW(con->Constant::deserialize(nullptr, 0, 0, num));
    EXPECT_EQ("", con->Constant::getRowLabel()->getString());
    EXPECT_EQ("", con->Constant::getColumnLabel()->getString());
    EXPECT_EQ("", con->Vector::getColumnLabel()->getString());

    VectorSP matrix = Util::createMatrix(DT_INT, 3, 3, 9);
    matrix->setName("123");
    EXPECT_EQ("123", matrix->getScript());
    int* data = new int[3]{1, 2, 3};
    VectorSP vec = Util::createVector(DT_INT, 3, 3, true, 0, data);
    EXPECT_EQ("[1,2,3]", vec->getScript());
    VectorSP vec2 = Util::createIndexVector(0,200);
    EXPECT_EQ("array()", vec2->getScript());
    vec2->setName("234");
    EXPECT_EQ("234", vec2->getScript());
    VectorSP vec3 = Util::createVector(DT_INT, 0, 3);
    EXPECT_EQ("[]", vec3->getScript());
}

TEST_F(FunctionTest,Matrix_reshape){
    int* data = new int[16]{1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8};
    VectorSP matrix = Util::createMatrix(DT_INT, 4, 4, 16, 0, data);
    std::vector<std::string> rowLabels{"row1", "row2", "row3", "row4"};
    std::vector<std::string> columnLabels{"col1", "col2", "col3", "col4"};
    VectorSP rowVec = Util::createVector(DT_STRING, 0, 4);
    rowVec->appendString(rowLabels.data(), rowLabels.size());
    rowVec->setTemporary(false);
    VectorSP colVec = Util::createVector(DT_STRING, 0, 4);
    colVec->appendString(columnLabels.data(), columnLabels.size());
    colVec->setTemporary(false);
    matrix->setRowLabel(rowVec);
    matrix->setColumnLabel(colVec);
    matrix->getString();
    EXPECT_TRUE(matrix->reshape(4, 4));
    EXPECT_TRUE(matrix->reshape(3, 4));
    EXPECT_TRUE(matrix->reshape(2, 6));
    EXPECT_FALSE(matrix->reshape(2, 5));
}

TEST_F(FunctionTest,Matrix_getString){
    int* data = new int[48]{1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8};
    VectorSP matrix = Util::createMatrix(DT_INT, 1, 48, 48, 0, data);
    matrix->getString();
    matrix->getString(0);
    EXPECT_TRUE(matrix->reshape(48, 1));
    matrix->getString();
}


TEST_F(FunctionTest,Matrix_get){
    int* data = new int[16]{1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8};
    VectorSP matrix = Util::createMatrix(DT_INT, 4, 4, 16, 0, data);
    ConstantSP index = Util::createInt(1);
    EXPECT_EQ("[5,6,7,8]", dynamic_cast<FastIntMatrix*>(matrix.get())->get(index)->getString());
    ConstantSP index1 = Util::createInt(-1);
    ConstantSP index2 = Util::createInt(10);
    EXPECT_ANY_THROW(dynamic_cast<FastIntMatrix*>(matrix.get())->get(index1));
    EXPECT_ANY_THROW(dynamic_cast<FastIntMatrix*>(matrix.get())->get(index2));

    ConstantSP index3 = Util::createPair(DT_INT);
    index3->setInt(0, 0);
    index3->setInt(1, 1);
    dynamic_cast<FastIntMatrix*>(matrix.get())->get(index3)->getString();
    index3->setInt(0, INT_MIN);
    index3->setInt(1, INT_MIN);
    dynamic_cast<FastIntMatrix*>(matrix.get())->get(index3)->getString();
    index3->setInt(0, 1);
    index3->setInt(1, 0);
    dynamic_cast<FastIntMatrix*>(matrix.get())->get(index3)->getString();

    ConstantSP index4 = Util::createVector(DT_INT, 2, 2);
    index4->setInt(0, -1);
    index4->setInt(1, 0);
    EXPECT_ANY_THROW(dynamic_cast<FastIntMatrix*>(matrix.get())->get(index4));
    index4->setInt(0, 6);
    index4->setInt(1, 0);
    EXPECT_ANY_THROW(dynamic_cast<FastIntMatrix*>(matrix.get())->get(index4));
}


TEST_F(FunctionTest,Matrix_set){
    int* data = new int[16]{1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8};
    VectorSP matrix = Util::createMatrix(DT_INT, 4, 4, 16, 0, data);
    
    ConstantSP index = Util::createIndexVector(0, 3);
    ConstantSP value = Util::createInt(9);
    EXPECT_TRUE(matrix->set(index, value));
    ConstantSP index1 = Util::createIndexVector(0, 6);
    EXPECT_ANY_THROW(matrix->set(index1, value));
    ConstantSP index2 = Util::createIndexVector(-1, 6);
    EXPECT_ANY_THROW(matrix->set(index2, value));
}

TEST_F(FunctionTest, DFSChunkMeta_Constructor){
    IO_ERR ret;
    short flag = 0, size = 0;
    vector<string> sites1 = {"192.168.0.16:9002:datanode1", "192.168.0.16:9003:datanode2", "192.168.0.16:9004:datanode3", "192.168.0.16:9005:datanode4"};
    vector<string> sites2;
    Guid id("314");
    DFSChunkMetaSP chunk = new DFSChunkMeta("/home/appadmin/data", id, 3, 1, SPLIT_TABLET_CHUNK, sites2, 315);
    chunk->getString();
    chunk = new DFSChunkMeta("/home/appadmin/data", id, 3, 1, TABLET_CHUNK, sites1, 315);
    chunk->getString();
    std::cout << chunk->getAllocatedMemory() << std::endl;
    chunk = new DFSChunkMeta("/home/appadmin/data", id, 3, 1, FILE_CHUNK, sites1.data(), 0, 315);

    DataOutputStreamSP outStream1 = new DataOutputStream(1024);
    ConstantMarshallSP marshall1 = ConstantMarshallFactory::getInstance(DF_CHUNK, outStream1);
    EXPECT_TRUE(marshall1->start(chunk, false, false, ret));
    DataInputStreamSP inStream1 = new DataInputStream(outStream1->getBuffer(), outStream1->size());
    inStream1->readShort(flag);
    inStream1->readShort(size);
    DFSChunkMetaSP chunk1 = new DFSChunkMeta(inStream1);
    EXPECT_ANY_THROW(new DFSChunkMeta(inStream1));

    chunk = new DFSChunkMeta("/home/appadmin/data", id, 3, 1, FILE_CHUNK, sites1.data(), 4, 315);
    DataOutputStreamSP outStream2 = new DataOutputStream(1024);
    ConstantMarshallSP marshall2 = ConstantMarshallFactory::getInstance(DF_CHUNK, outStream2);
    EXPECT_TRUE(marshall2->start(chunk, false, false, ret));
    DataInputStreamSP inStream2 = new DataInputStream(outStream2->getBuffer(), outStream2->size());
    inStream2->readShort(flag);
    inStream2->readShort(size);
    DFSChunkMetaSP chunk2 = new DFSChunkMeta(inStream2);
}

TEST_F(FunctionTest, DFSChunkMeta_getMember){
    vector<string> sites1 = {"192.168.0.16:9002:datanode1", "192.168.0.16:9003:datanode2", "192.168.0.16:9004:datanode3", "192.168.0.16:9005:datanode4"};
    Guid id("314");
    DFSChunkMetaSP chunk = new DFSChunkMeta("/home/appadmin/data", id, 3, 1, FILE_CHUNK, sites1, 315);
    
    ConstantSP key1 = Util::createInt(1);
    EXPECT_ANY_THROW(chunk->getMember(key1));
    ConstantSP key2 = Util::createString("path");
    EXPECT_EQ("/home/appadmin/data", chunk->getMember(key2)->getString());
    ConstantSP key3 = Util::createPair(DT_STRING);
    EXPECT_ANY_THROW(chunk->getMember(key3));
    VectorSP key4 = Util::createVector(DT_STRING, 0, 8);
    std::vector<std::string> vec{"id", "cid", "version", "sites", "size", "isTablet", "splittable", "none"};
    key4->appendString(vec.data(), vec.size());
    chunk->getMember(key4);
    chunk->keys();
    chunk->values();
}

TEST_F(FunctionTest, DBConnection_upload){
    ConstantSP value = Util::createInt(1);
    EXPECT_ANY_THROW(conn.upload("0123", value));
    std::vector<ConstantSP> obj{value};
    std::vector<std::string> names{"123"};
    EXPECT_ANY_THROW(conn.upload(names, obj));
    names.push_back("234");
    EXPECT_ANY_THROW(conn.upload(names, obj));
    obj.clear();
    names.clear();
    conn.upload(names, obj);
    EXPECT_ANY_THROW(conn.run("1+1", 4, 2, 100));
    DBConnection dc;
    DBConnection dd;
    EXPECT_ANY_THROW(dc.run("1+1"));
    dc = std::move(dc);
    dd = std::move(dc);
}


TEST_F(FunctionTest, DLogger){
    {
        RecordTime t("name");
        Util::sleep(200);
    }
    char workDir[256]{};
    getcwd(workDir, sizeof(workDir));
    std::string file = std::string(workDir).append("/tempFile123");
    DLogger::SetLogFilePath(file);
    auto level = DLogger::GetMinLevel();
    DLogger::Info("123", "345");
    DLogger::SetLogFilePath("");
    DLogger::SetMinLevel(DLogger::Level::LevelWarn);
    DLogger::Info("123", "345");
    DLogger::Error("123", "345");
    remove(file.c_str());

    {
        RecordTime t("name");
        Util::sleep(100);
    }
    {
        RecordTime t("name2");
        Util::sleep(100);
    }
    {
        RecordTime t("name2");
        Util::sleep(200);
    }
    RecordTime::printAllTime();
}


BasicTableSP createTable(){
    int* data1 = new int[2]{1, 2};
    int* data2 = new int[2]{3, 4};
    std::vector<ConstantSP> cols ={Util::createVector(DT_INT, 2, 2, true, 0, data1), Util::createVector(DT_INT, 2, 2, true, 0, data2)};
    vector<string> colNames = { "col1", "col2"};
    return new BasicTable(cols, colNames);
}

TEST_F(FunctionTest, Table_Constuctor){
    BasicTableSP t = createTable();
    ConstantSP value = new Int(1);
    EXPECT_ANY_THROW(t->AbstractTable::set(1, value));
    EXPECT_ANY_THROW(t->AbstractTable::setColumnName(1, "123"));
    ConstantSP emptyFilter;
    EXPECT_EQ("[3,4]", t->AbstractTable::getColumn(std::string("col2"), emptyFilter)->getString());
    EXPECT_EQ("4", t->AbstractTable::get(1, 1)->getString());
    t->setName("Harry");
    EXPECT_FALSE(t->contain("Tom", "col1"));
    EXPECT_FALSE(t->contain("Tom", "col3"));

    vector<string> colNames2 = { "col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col0", "col1", "col2", "col3"};
    std::vector<ConstantSP> cols2;
    for(int i = 0; i < 13; ++i){
        VectorSP v = Util::createVector(DT_STRING, 0, 1);
        std::string content = "12345678901234567890";
        v->appendString(&content, 1);
        cols2.push_back(v);
    }
    BasicTableSP t1 = new BasicTable(cols2, colNames2);
    t1->getString(0);
    t1->getString();
    EXPECT_EQ(COMPRESS_NONE, t1->getColumnCompressMethod(2));
}

TEST_F(FunctionTest, Table_setColumnCompressMethods){
    BasicTableSP t = createTable();
    std::vector<COMPRESS_METHOD> methods{COMPRESS_DELTA, COMPRESS_NONE};
    EXPECT_ANY_THROW(t->setColumnCompressMethods(methods));
}

TEST_F(FunctionTest, Table_getInternal){
    int* data1 = new int[2]{1, 2};
    int* data2 = new int[2]{3, 4};
    std::vector<ConstantSP> cols ={Util::createVector(DT_INT, 2, 2, true, 0, data1), Util::createVector(DT_INT, 2, 2, true, 0, data2)};
    vector<string> colNames = { "col1", "col2"};
    BasicTableSP t = new BasicTable(cols, colNames);

    ConstantSP index = Util::createPair(DT_INT);
    index->setInt(0, INT_MIN);
    index->setInt(1, INT_MIN);
    t->get(index);
    index->setInt(0, 1);
    index->setInt(1, 0);
    t->get(index);
    t->getWindow(1, 1, 1, 1);
    EXPECT_ANY_THROW(t->AbstractTable::getInstance(1));
    EXPECT_ANY_THROW(t->AbstractTable::getValue());
    EXPECT_ANY_THROW(t->AbstractTable::getValue(1));
    std::string errMsg;
    INDEX rows = 0;
    std::vector<ConstantSP> values;
    EXPECT_FALSE(t->AbstractTable::append(values, rows, errMsg));
    EXPECT_FALSE(t->AbstractTable::update(values, index, colNames, errMsg));
    EXPECT_FALSE(t->AbstractTable::remove(index, errMsg));
}

TEST_F(FunctionTest, BasicTable_init){
    int* data1 = new int[2]{1, 2};
    int* data2 = new int[2]{3, 4};
    std::vector<ConstantSP> cols(2);
    vector<string> colNames = { "col1", "col2"};
    EXPECT_ANY_THROW(new BasicTable(cols, colNames));
    cols[0] = Util::createVector(DT_INT, 2, 2, true, 0, data1);
    cols[1] = Util::createVector(DT_INT, 2, 2, true, 0, data2);
    BasicTableSP t = new BasicTable(cols, colNames);
    ConstantSP value = Util::createDictionary(DT_INT, DT_INT);
    EXPECT_FALSE(t->set(0, value));
}

TEST_F(FunctionTest, BasicTable_appendTable){
    int* data1 = new int[2]{1, 2};
    int* data2 = new int[2]{3, 4};
    std::vector<ConstantSP> cols ={Util::createVector(DT_INT, 2, 2, true, 0, data1), Util::createVector(DT_INT, 2, 2, true, 0, data2)};
    vector<string> colNames = { "col1", "col2"};
    BasicTableSP t = new BasicTable(cols, colNames);

    std::vector<std::string> data3{"123", "456"};
    int* data4 = new int[2]{3, 4};
    std::vector<ConstantSP> cols2(2);
    VectorSP vec = Util::createVector(DT_STRING, 0, 2);
    vec->appendString(data3.data(), data3.size());
    cols2[1] = vec;
    cols2[0] = Util::createVector(DT_INT, 2, 2, true, 0, data4);
    vector<string> colNames2 = { "col1", "col2"};
    BasicTableSP t2 = new BasicTable(cols2, colNames2);

    std::vector<ConstantSP> values{t2};
    std::string errMsg;
    INDEX rows = 0;
    EXPECT_FALSE(t->append(values, rows, errMsg));
    
    t2 = t->getValue();
    values[0] = t2;
    EXPECT_TRUE(t->append(values, rows, errMsg));
}

TEST_F(FunctionTest, BasicTable_appendTuple){
    std::string errMsg;
    INDEX rows = 0;
    BasicTableSP t = createTable();
    
    VectorSP vec = Util::createVector(DT_ANY, 0, 2);
    vec->append(Util::createVector(DT_INT, 2, 2));
    vec->append(Util::createString("123"));
    std::vector<ConstantSP> values{vec};
    EXPECT_FALSE(t->append(values, rows, errMsg));
    
    VectorSP vec1 = Util::createVector(DT_ANY, 0, 2);
    vec1->append(Util::createInt(2));
    vec1->append(Util::createString("123"));
    values[0] = vec1;
    EXPECT_FALSE(t->append(values, rows, errMsg));

    VectorSP vec2 = Util::createVector(DT_ANY, 0, 2);
    vec2->append(Util::createInt(2));
    vec2->append(Util::createInt(2));
    values[0] = vec2;
    EXPECT_TRUE(t->append(values, rows, errMsg));

    VectorSP vec3 = Util::createVector(DT_ANY, 0, 2);
    vec3->append(Util::createVector(DT_INT, 2, 2));
    vec3->append(Util::createVector(DT_STRING, 2, 2));
    values[0] = vec3;
    EXPECT_FALSE(t->append(values, rows, errMsg));
}

TEST_F(FunctionTest, BasicTable_appendNormal){
    std::string errMsg;
    INDEX rows = 0;
    BasicTableSP t = createTable();
    
    std::vector<ConstantSP> values;
    values.push_back(Util::createVector(DT_INT, 2, 2));
    values.push_back(Util::createString("123"));
    EXPECT_FALSE(t->append(values, rows, errMsg));
    
    values[0] = Util::createInt(2);
    values[1] = Util::createString("123");
    EXPECT_FALSE(t->append(values, rows, errMsg));

    values[0] = Util::createInt(2);
    values[1] = Util::createInt(2);
    EXPECT_TRUE(t->append(values, rows, errMsg));

    values[0] = Util::createVector(DT_INT, 2, 2);
    std::vector<std::string> columnLabels{"col1", "col2"};
    VectorSP rowVec = Util::createVector(DT_STRING, 0, 4);
    rowVec->appendString(columnLabels.data(), columnLabels.size());
    values[1] = rowVec;
    EXPECT_FALSE(t->append(values, rows, errMsg));
}

TEST_F(FunctionTest, BasicTable_update){
    BasicTableSP t = createTable();

    ConstantSP index = Util::createIndexVector(0, 1);
    index->setNothing(true);
    std::vector<ConstantSP> values{Util::createVector(DT_INT, 3, 3)};
    std::vector<std::string> colName{"col3"};
    std::string errMsg;
    EXPECT_FALSE(t->update(values, index, colName, errMsg));
    values[0] = Util::createInt(1);
    EXPECT_TRUE(t->update(values, index, colName, errMsg));
    colName[0] = "col4";
    values[0] = Util::createIndexVector(0, 2);
    EXPECT_TRUE(t->update(values, index, colName, errMsg));
    colName[0] = "col5";
    values[0] = Util::createIndexVector(0, 2);
    values[0]->setTemporary(false);
    EXPECT_TRUE(t->update(values, index, colName, errMsg));

    colName[0] = "col1";
    values[0] = Util::createIndexVector(0, 2);
    EXPECT_TRUE(t->update(values, index, colName, errMsg));

    values[0] = Util::createPair(DT_STRING);
    EXPECT_FALSE(t->update(values, index, colName, errMsg));
}

TEST_F(FunctionTest, BasicTable_updateVector){
    int* data1 = new int[2]{1, 2};
    std::vector<std::string> columnLabels{"col1", "col2"};
    VectorSP rowVec = Util::createVector(DT_STRING, 0, 2);
    rowVec->appendString(columnLabels.data(), columnLabels.size());
    std::vector<ConstantSP> cols ={Util::createVector(DT_INT, 2, 2, true, 0, data1), rowVec};
    vector<string> colNames = { "col1", "col2"};
    BasicTableSP t = new BasicTable(cols, colNames);
    
    std::vector<ConstantSP> values;
    values.push_back(Util::createIndexVector(0, 2));
    ConstantSP index = Util::createIndexVector(0, 1);
    std::string errMsg;
    std::vector<std::string> colName{"col2"};
    EXPECT_FALSE(t->update(values, index, colName, errMsg));

    colName[0] = "col1";
    values[0] = new Void();
    EXPECT_TRUE(t->update(values, index, colName, errMsg));
 
    values[0] = Util::createVector(DT_FLOAT, 2, 2);
    EXPECT_FALSE(t->update(values, index, colName, errMsg));
    values[0] = Util::createInt(1);
    index->setNothing(true);
    EXPECT_TRUE(t->update(values, index, colName, errMsg));
}

TEST_F(FunctionTest, BasicTable_remove){
    BasicTableSP t = createTable();

    std::string errMsg;
    ConstantSP index = Util::createIndexVector(0, 1);
    index->setNothing(true);
    EXPECT_TRUE(t->remove(index, errMsg));
}

TEST_F(FunctionTest, BasicTable_join){
    BasicTableSP t = createTable();
    t->setReadOnly(true);

    std::vector<ConstantSP> columns;
    EXPECT_FALSE(t->join(columns));
    t->updateSize();
    EXPECT_FALSE(t->clear());
    columns.push_back(Util::createPair(DT_INT));
    t->setReadOnly(false);
    EXPECT_FALSE(t->join(columns));
    columns[0] = Util::createIndexVector(0, 3);
    EXPECT_FALSE(t->join(columns));
    columns[0] = Util::createIndexVector(0, 2);
    EXPECT_FALSE(t->join(columns));
    dynamic_cast<Vector*>(columns[0].get())->setName("col1");
    EXPECT_FALSE(t->join(columns));
    dynamic_cast<Vector*>(columns[0].get())->setName("col3");
    EXPECT_TRUE(t->join(columns));
    ((Vector*)t->getColumn(0).get())->append(Util::createIndexVector(0, 2));
    EXPECT_ANY_THROW(t->updateSize());
}

TEST_F(FunctionTest,test_FastFixedLengthVector_getDataArray){
    unsigned char data[5] = "abcd";
    auto fastIntVec = new FastInt128Vector(DT_INT, 2, 4, data, false);
    auto fastIntVec1 = new FastInt128Vector(DT_INT, 2, 4, data, true);
    bool hasNull = false;
    auto idx1 = Util::createIndexVector(1, 2);
    fastIntVec->get(idx1);
    double *vec1 = new double[2];
    vec1[0] = 0.1;
    vec1[1] = 1.2;
    double *vec2 = new double[2];
    vec2[0] = 3.1;
    vec2[1] = 3.2;
    auto idx2 = new FastDoubleVector(2, 2, vec1, true);
    auto idx3 = new FastDoubleVector(2, 2, vec2, true);
    fastIntVec1->get(idx2);
    fastIntVec1->get(idx3);
}

#endif