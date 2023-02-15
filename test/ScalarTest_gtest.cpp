#include <sstream>

class ScalarTest:public testing::Test
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

template <typename T>
static T round(T src, int bits)
{
    stringstream ss;
    ss << fixed << setprecision(bits) << src;
    ss >> src;
    return src;
}

TEST_F(ScalarTest,testScalar){
    srand(time(NULL));

    bool s1 = rand() % 2;
    char s2 = rand() % CHAR_MAX;
    int s3 = rand() % INT_MAX;
    short s4 = rand() % SHRT_MAX;
    long long s5 = rand() % LLONG_MAX;
    float s6 = rand() / float(RAND_MAX);
    double s7 = rand() / double(RAND_MAX);
    string str = "string"+to_string(rand()%100);
    string sym = "symbol"+to_string(rand()%100);
    string blob = "blob"+to_string(rand()%100);
    unsigned char int128[16];
    unsigned char ip[16];
    unsigned char uuid[16];
    for(auto i=0;i<16;i++){
        int128[i] = rand() % CHAR_MAX;
        ip[i] = rand() % CHAR_MAX;
        uuid[i] = rand() % CHAR_MAX;
    }

    ConstantSP boolval = Util::createBool(s1);
    ConstantSP charval = Util::createChar(s2);
    ConstantSP intval = Util::createInt(s3);
    ConstantSP shortval = Util::createShort(s4);
    ConstantSP longval = Util::createLong(s5);
    ConstantSP floatval = Util::createFloat(s6);
    ConstantSP doubleval = Util::createDouble(s7);
    ConstantSP stringval = Util::createString(str);
    ConstantSP symbolval = Util::createString(sym);
    ConstantSP blobval = Util::createString(blob);
    ConstantSP int128val = Util::createConstant(DT_INT128);
    int128val->setBinary(int128, sizeof(Guid));
    ConstantSP ipval = Util::createConstant(DT_IP);
    ipval->setBinary(ip, sizeof(Guid));
    ConstantSP uuidval = Util::createConstant(DT_UUID);
    uuidval->setBinary(uuid, sizeof(Guid));
    ConstantSP dateval = Util::createDate(s3);
    ConstantSP monthval = Util::createMonth(2022,2);
    ConstantSP timeval = Util::createTime(s3);
    ConstantSP minuteval = Util::createMinute(rand()%1440);
    ConstantSP secondval = Util::createSecond(rand()%86400);
    ConstantSP datetimeval = Util::createDateTime(s3);
    ConstantSP timestampval = Util::createTimestamp(s5);
    ConstantSP nanotimeval = Util::createNanoTime(s5);
    ConstantSP nanotimestampval = Util::createNanoTimestamp(s5);

    vector<ConstantSP> vals = {boolval,charval,intval,shortval,longval,floatval,doubleval,stringval,symbolval,blobval,int128val,\
                                ipval,uuidval,dateval,monthval,timeval,minuteval,secondval,datetimeval,timestampval,nanotimeval,nanotimestampval};
    vector<string> names = {"boolval","charval","intval","shortval","longval","floatval","doubleval","stringval","symbolval","blobval","int128val",\
                                "ipval","uuidval","dateval","monthval","timeval","minuteval","secondval","datetimeval","timestampval","nanotimeval","nanotimestampval"};
    conn.upload(names,vals);
    for(auto i=0; i<vals.size();i++){
        EXPECT_TRUE(vals[i]->isScalar());
        EXPECT_FALSE(vals[i]->isMatrix());
        EXPECT_FALSE(vals[i]->isVector());
        EXPECT_FALSE(vals[i]->isArray());
        EXPECT_FALSE(vals[i]->isChunk());
        EXPECT_FALSE(vals[i]->isDatabase());
        EXPECT_FALSE(vals[i]->isDictionary());
        EXPECT_FALSE(vals[i]->isHugeIndexArray());
        EXPECT_FALSE(vals[i]->isIndexArray());
        EXPECT_FALSE(vals[i]->isPair());
        EXPECT_FALSE(vals[i]->isSet());
        EXPECT_FALSE(vals[i]->isTable());
        EXPECT_FALSE(vals[i]->isTuple());
        EXPECT_EQ(vals[i]->getString(), conn.run(names[i])->getString());

        cout<<Util::getDataTypeString(vals[i]->getType())<<endl;

        if(i <= 6){
            if(i > 0){
                vals[i]->setBool(2);
                EXPECT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                EXPECT_EQ(vals[i]->getString(), "2");
                vals[i]->setBool(CHAR_MIN);
                EXPECT_TRUE(vals[i]->isNull());
                EXPECT_FALSE(vals[i]->add(0,1,(long long)3));

                vals[i]->setChar(2);
                // cout<<Util::getDataTypeString(vals[i]->getType())<<endl;
                // ConstantSP temp = vals[i]->getInstance();
                // temp->setChar(2);
                // EXPECT_EQ(temp->getString(), vals[i]->getString());
                EXPECT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                EXPECT_EQ(vals[i]->getString(), "2");
                vals[i]->setChar(CHAR_MIN);
                EXPECT_TRUE(vals[i]->isNull());

                vals[i]->setShort(2);
                EXPECT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                EXPECT_EQ(vals[i]->getString(), "2");
                vals[i]->setShort(SHRT_MIN);
                EXPECT_TRUE(vals[i]->isNull());

                vals[i]->setInt(2);
                vals[i]->nullFill(Util::createChar(3));
                EXPECT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                EXPECT_EQ(vals[i]->getString(), "2");
                vals[i]->setInt(INT_MIN);
                EXPECT_TRUE(vals[i]->isNull());
                vals[i]->nullFill(Util::createChar(3));
                EXPECT_EQ(vals[i]->getString(), "3");
                EXPECT_TRUE(vals[i]->add(0,1,(long long)1));
                EXPECT_EQ(vals[i]->getString(), "4");
                EXPECT_EQ(vals[i]->compare(0, Util::createChar(2)), 1);
                EXPECT_EQ(vals[i]->compare(0, Util::createChar(4)), 0);
                EXPECT_EQ(vals[i]->compare(0, Util::createChar(6)), -1);

                vals[i]->setLong(2);
                EXPECT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                EXPECT_EQ(vals[i]->getString(), "2");
                vals[i]->setLong(LLONG_MIN);
                EXPECT_TRUE(vals[i]->isNull());

                vals[i]->setFloat(2);
                vals[i]->nullFill(Util::createFloat(3));
                EXPECT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                EXPECT_EQ(vals[i]->getString(), "2");
                vals[i]->setFloat(FLT_NMIN);
                EXPECT_TRUE(vals[i]->isNull());
                vals[i]->nullFill(Util::createFloat(3));
                EXPECT_EQ(vals[i]->getString(), "3");
                EXPECT_TRUE(vals[i]->add(0,1,(float)1));
                EXPECT_EQ(vals[i]->getString(), "4");
                EXPECT_EQ(vals[i]->compare(0, Util::createFloat(2)), 1);
                EXPECT_EQ(vals[i]->compare(0, Util::createFloat(4)), 0);
                EXPECT_EQ(vals[i]->compare(0, Util::createFloat(6)), -1);

                vals[i]->setDouble(2);
                EXPECT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                EXPECT_EQ(vals[i]->getString(), "2");
                vals[i]->setDouble(DBL_NMIN);
                EXPECT_TRUE(vals[i]->isNull());

                vals[i]->setIndex(2);
                EXPECT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                EXPECT_EQ(vals[i]->getString(), "2");
                vals[i]->setIndex(INDEX_MIN);
                EXPECT_TRUE(vals[i]->isNull());
            }
            else{
                vals[i]->setBool(2);
                EXPECT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                EXPECT_EQ(vals[i]->getString(), "1");
                vals[i]->setBool(CHAR_MIN);
                EXPECT_TRUE(vals[i]->isNull());

                vals[i]->setChar(2);
                EXPECT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                EXPECT_EQ(vals[i]->getString(), "1");
                vals[i]->setChar(CHAR_MIN);
                EXPECT_TRUE(vals[i]->isNull());

                vals[i]->setShort(2);
                EXPECT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                EXPECT_EQ(vals[i]->getString(), "1");
                vals[i]->setShort(SHRT_MIN);
                EXPECT_TRUE(vals[i]->isNull());

                vals[i]->setInt(2);
                EXPECT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                EXPECT_EQ(vals[i]->getString(), "1");
                vals[i]->setInt(INT_MIN);
                EXPECT_TRUE(vals[i]->isNull());

                vals[i]->setLong(2);
                EXPECT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                EXPECT_EQ(vals[i]->getString(), "1");
                vals[i]->setLong(LLONG_MIN);
                EXPECT_TRUE(vals[i]->isNull());

                vals[i]->setFloat(2);
                EXPECT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                EXPECT_EQ(vals[i]->getString(), "1");
                vals[i]->setFloat(FLT_NMIN);
                EXPECT_TRUE(vals[i]->isNull());

                vals[i]->setDouble(2);
                EXPECT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                EXPECT_EQ(vals[i]->getString(), "1");
                vals[i]->setDouble(DBL_NMIN);
                EXPECT_TRUE(vals[i]->isNull());

                vals[i]->setIndex(2);
                EXPECT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                EXPECT_EQ(vals[i]->getString(), "1");
                vals[i]->setIndex(INDEX_MIN);
                EXPECT_TRUE(vals[i]->isNull());
            }

        }
        else if (i > 12)
        {
            vals[i]->setChar(2);
            ConstantSP temp = vals[i]->getInstance();
            temp->setChar(2);
            EXPECT_EQ(temp->getString(), vals[i]->getString());
        }
        

    }

    EXPECT_EQ(conn.run("a=NULL;a")->getString(),"");
    EXPECT_ANY_THROW(dateval->getInt128());

    #ifndef WINDOWS
    Time t1(86400000);
    Time t2(100001);
    Time t3(-1);
    Time t4(0);
    t1.validate();t2.validate();t4.validate();t3.validate();

    EXPECT_TRUE(t1.isNull() && t3.isNull());
    EXPECT_EQ(t2.getString(), "00:01:40.001");
    EXPECT_EQ(t4.getString(), "00:00:00.000");

    Minute m1(1440);
    Minute m2(1001);
    Minute m3(-1);
    Minute m4(0);
    m1.validate();m2.validate();m4.validate();m3.validate();

    EXPECT_TRUE(m1.isNull() && m3.isNull());
    EXPECT_EQ(m2.getString(), "16:41m");
    EXPECT_EQ(m4.getString(), "00:00m");

    Second sec1(86400);
    Second sec2(10001);
    Second sec3(-1);
    Second sec4(0);
    sec1.validate();sec2.validate();sec3.validate();sec4.validate();

    EXPECT_TRUE(sec1.isNull() && sec3.isNull());
    EXPECT_EQ(sec2.getString(), "02:46:41");
    EXPECT_EQ(sec4.getString(), "00:00:00");


    NanoTime ss1(86400000000000ll);
    NanoTime ss2(100000000001);
    NanoTime ss3(-1);
    NanoTime ss4(0);
    ss1.validate();ss2.validate();ss3.validate();ss4.validate();

    EXPECT_TRUE(ss1.isNull() && ss3.isNull());
    EXPECT_EQ(ss2.getString(), "00:01:40.000000001");
    EXPECT_EQ(ss4.getString(), "00:00:00.000000000");
    #endif

}

TEST_F(ScalarTest,testGuid){
    unsigned char uuid[16];
    for (auto i = 0; i < 16; i++) {
        uuid[i] = rand() % 256;
    }

    Guid uuidval(uuid);
    const string erruuid = "5d212a78=cc48-e3b1-4235-b4d91473ee87";
    const string erruuid1 ="erruuid";
    EXPECT_ANY_THROW(Guid err=new Guid(erruuid));
    EXPECT_ANY_THROW(Guid err=new Guid(erruuid1));
    EXPECT_FALSE(uuidval.isZero());
    EXPECT_TRUE(Guid(Util::createNullConstant(DT_UUID)->getString()).isZero());

    Guid uuidval0 = Guid((const string)"0a0a0a0a-0a0a-0a0a-0a0a-0a0a0a0a0a0a");
    Guid uuidval2 = Guid((const string)"00000000-0000-0000-0000-000000000000");
    Guid uuidval3 = Guid((const string)"c8c8c8c8-c8c8-c8c8-c8c8-c8c8c8c8c8c8");
    EXPECT_EQ(Guid::getString(uuid),Util::parseConstant(DT_UUID,uuidval.getString())->getString());
    EXPECT_TRUE(uuidval.operator==(Guid::getString(uuid)));
    EXPECT_FALSE(uuidval.operator!=(Guid::getString(uuid)));
    EXPECT_TRUE(uuidval0.operator>=(uuidval2));
    EXPECT_TRUE(uuidval0.operator<=(uuidval3));
    EXPECT_TRUE(uuidval0.operator>(uuidval2));
    EXPECT_TRUE(uuidval0.operator<(uuidval3));
    EXPECT_EQ(uuidval0.compare(uuidval0),0);
    EXPECT_EQ(uuidval0.compare(uuidval2),1);
    EXPECT_EQ(uuidval0.compare(uuidval3),-1);

    GuidHash(ghash);
}

TEST_F(ScalarTest,testDecimal){
    ConstantSP ddbval1 = conn.run("a=3.1626$DECIMAL32(8);a");
    EXPECT_ANY_THROW(ConstantSP decimalval1 = Util::createDecimal32(-1, 3.1626));
    EXPECT_ANY_THROW(ConstantSP decimalval1 = Util::createDecimal32(10, 3.1626)); //scale + value's places must less than 10
    EXPECT_ANY_THROW(ConstantSP decimalval1 = Util::createDecimal32(9, 3.1626));
    ConstantSP decimalval1 = Util::createDecimal32(8, 3.1626);
    EXPECT_EQ(decimalval1->getString(), ddbval1->getString());
    EXPECT_EQ(decimalval1->getType(), ddbval1->getType());
    EXPECT_EQ(decimalval1->getRawType(), ddbval1->getRawType());
    EXPECT_EQ(Util::getCategoryString(decimalval1->getCategory()), "DENARY");
    conn.upload("decimalval1",decimalval1);
    EXPECT_TRUE(conn.run("eqObj(a,decimalval1)")->getBool());

    ConstantSP ddbval2 = conn.run("b=3.1626$DECIMAL64(17);b");
    EXPECT_ANY_THROW(ConstantSP decimalval2 = Util::createDecimal64(-1, 13.1626));
    EXPECT_ANY_THROW(ConstantSP decimalval2 = Util::createDecimal64(19, 13.1626)); //scale + value's places must less than 19
    EXPECT_ANY_THROW(ConstantSP decimalval2 = Util::createDecimal64(18, 13.1626));
    ConstantSP decimalval2 = Util::createDecimal64(17, 3.1626);
    EXPECT_EQ(decimalval2->getString(), ddbval2->getString());
    EXPECT_EQ(decimalval2->getType(), ddbval2->getType());
    EXPECT_EQ(decimalval2->getRawType(), ddbval2->getRawType());
    EXPECT_EQ(Util::getCategoryString(decimalval2->getCategory()), "DENARY");
    conn.upload("decimalval2",decimalval2);
    EXPECT_TRUE(conn.run("eqObj(b,decimalval2)")->getBool());

	decimalval1->setDouble(1.2);
    EXPECT_EQ(decimalval1->getDouble(), (double)1.2);
	decimalval2->setFloat(1.223f);
    EXPECT_EQ(decimalval2->getFloat(), (float)1.223);
    decimalval2->setString("3.2561889425648");
    EXPECT_EQ(decimalval2->getString(), "3.25618894256480000");
    decimalval2->setNull();
    EXPECT_TRUE(decimalval2->isNull());

    decimalval1->assign(Util::createDecimal32(2,1.34567));
    decimalval2->assign(Util::createDecimal32(2,1.34567));
    EXPECT_EQ(decimalval1->getString(),"1.34000000");
    EXPECT_EQ(decimalval2->getString(),"1.34000000000000000");
    EXPECT_ANY_THROW(decimalval1->assign(Util::createDecimal64(0,-1000000000000000000)));
    EXPECT_ANY_THROW(decimalval1->assign(Util::createDecimal64(0,1000000000000000000)));
    decimalval1->assign(Util::createDecimal32(8,2.3641553));
    decimalval2->assign(Util::createDecimal64(17,2.3641553));
    EXPECT_EQ(decimalval1->getString(),"2.36415530");
    EXPECT_FLOAT_EQ(decimalval2->getFloat(), float(2.36415530000000000)); // result is 2.36415530000000032
    EXPECT_ANY_THROW(decimalval1->assign(Util::createString("11.111111111111111111")));
    EXPECT_ANY_THROW(decimalval2->assign(Util::createBlob("11.111111111111111111")));
    decimalval1->assign(Util::createString("1.11111"));
    decimalval2->assign(Util::createBlob("1.11111"));
    EXPECT_EQ(decimalval1->getString(),"1.11111000");
    EXPECT_EQ(decimalval2->getString(),"1.11111000000000000");
    EXPECT_FALSE(decimalval1->assign(Util::createDate(10000)));
    decimalval1->assign(Util::createShort(3));
    decimalval2->assign(Util::createShort(3));
    EXPECT_EQ(decimalval1->getString(),"3.00000000");
    EXPECT_EQ(decimalval2->getString(),"3.00000000000000000");
    decimalval1->assign(Util::createInt(1));
    decimalval2->assign(Util::createInt(1));
    EXPECT_EQ(decimalval1->getString(),"1.00000000");
    EXPECT_EQ(decimalval2->getString(),"1.00000000000000000");
    decimalval1->assign(Util::createLong(2));
    decimalval2->assign(Util::createLong(2));
    EXPECT_EQ(decimalval1->getString(),"2.00000000");
    EXPECT_EQ(decimalval2->getString(),"2.00000000000000000");
    decimalval1->assign(Util::createFloat((float)3.1315));
    decimalval2->assign(Util::createFloat((float)3.1315));
    EXPECT_NEAR(decimalval1->getFloat(),(float)3.1315, 4);
    EXPECT_NEAR(decimalval2->getFloat(),(float)3.1315, 4);
    decimalval1->assign(Util::createDouble(-3.131544));
    decimalval2->assign(Util::createDouble(-3.131544));
    EXPECT_EQ(decimalval1->getString(),"-3.13154400");
    EXPECT_EQ(decimalval2->getString(),"-3.13154400000000000");

    decimalval1->assign(Util::createInt(1));
	EXPECT_EQ(decimalval1->compare(0,Util::createInt(0)),1);
	EXPECT_EQ(decimalval1->compare(0,Util::createInt(2)),-1);
	EXPECT_EQ(decimalval1->compare(0,Util::createInt(1)),0);
	EXPECT_EQ(decimalval1->compare(0,Util::createDecimal32(3,0.99)),1);
	EXPECT_EQ(decimalval1->compare(0,Util::createDecimal32(2,2.01)),-1);
	EXPECT_EQ(decimalval1->compare(0,Util::createDecimal32(1,1)),0);
	EXPECT_EQ(decimalval1->compare(0,Util::createDecimal64(8,0.999999999)),1);
	EXPECT_EQ(decimalval1->compare(0,Util::createDecimal64(10,2.00001)),-1);
	EXPECT_EQ(decimalval1->compare(0,Util::createDecimal64(1,1)),0);
	EXPECT_ANY_THROW(decimalval1->compare(0,Util::createBlob("blob1")));

    ConstantSP decimalval3 = Util::createDecimal32(0, 100);
    ConstantSP decimalval4 = Util::createDecimal64(0, 100);
    decimalval3->setDouble(-1000000000);
    decimalval4->setDouble(-1000000000000000000);
    EXPECT_ANY_THROW(decimalval3->setString("1000000000"));
    EXPECT_ANY_THROW(decimalval4->setString("1000000000000000000"));
    EXPECT_EQ(decimalval3->getString(), "-1000000000");
    EXPECT_EQ(decimalval4->getString(), "-1000000000000000000");
    EXPECT_EQ(decimalval3->getInstance()->getString(),decimalval3->getString());
    EXPECT_EQ(decimalval4->getValue()->getString(),decimalval4->getString());

    EXPECT_EQ(decimalval3->getExtraParamForType(),0);
    EXPECT_FALSE(decimalval3->isNull());
    EXPECT_FALSE(decimalval4->isNull());
    decimalval3->setNull();
    decimalval4->setNull();
    EXPECT_EQ(decimalval3->getString(),"");
    EXPECT_EQ(decimalval4->getString(),"");
    EXPECT_TRUE(decimalval3->isNull());
    EXPECT_TRUE(decimalval3->isNull());

}

TEST_F(ScalarTest,testScalarFunction){

    srand(time(NULL));
    int buckets=2;
    char *buf0 = new char[2];
    char *buf00 = new char[2];
    char *buf = new char[2]; 
    short *buf1 = new short[2];
    int *buf2 = new int[2];
    INDEX *buf4 = new INDEX[2];
    long long *buf3 = new long long[2];
    float *buf5 = new float[2];
    double *buf6 = new double[2];
    string **buf8 = new string*[2];
    char **buf9 = new char*[24];
    unsigned char *buf10 = new unsigned char[2];
    SymbolBase *symbase= new SymbolBase(1);
    string exstr = "";

    cout<<"-----------------void-------------------"<<endl;
    ConstantSP voidval=Util::createConstant(DT_VOID);
    voidval->setBool(1);
    voidval->setChar(1);
    voidval->setShort(1);
    voidval->setInt(1);
    voidval->setLong(1);
    voidval->setIndex(1);
    voidval->setFloat(1);
    EXPECT_TRUE(voidval->isNull());
    voidval->setNull();

    voidval->nullFill(Util::createInt(1));
    EXPECT_TRUE(voidval->isNull());

    voidval->setBool(1,1);
    voidval->setChar(1,1);
    voidval->setShort(1,1);
    voidval->setInt(1,1);
    voidval->setLong(1,1);
    voidval->setIndex(1,1);
    voidval->setFloat(1,1);
    EXPECT_TRUE(voidval->isNull());
    voidval->setNull(1);
    voidval->validate(); // nothing to do.
    EXPECT_FALSE(voidval->reshape(2,2));
    EXPECT_FALSE(voidval->add(0, 1, double(1.3123)));
    EXPECT_FALSE(voidval->add(0, 1,(long long)1));
    EXPECT_EQ(voidval->compare(0, Util::createNullConstant(DT_VOID)),0);

    EXPECT_TRUE(voidval->hasNull());
    EXPECT_TRUE(voidval->hasNull(0,1));
    EXPECT_FALSE(voidval->sizeable());
    EXPECT_TRUE(voidval->copyable());
    EXPECT_EQ(voidval->itemCount(),1);
    EXPECT_EQ(voidval->uncompressedRows(),1);
    bool flag = false;
    EXPECT_FALSE(voidval->releaseMemory((long long)1, flag));
    // EXPECT_EQ(voidval->getAllocatedMemory(), 0);


    EXPECT_TRUE(voidval->isNull(0,1,buf0));
    EXPECT_TRUE(voidval->isValid(0,1,buf0));
    EXPECT_TRUE(voidval->getBool(0,1,buf00));
    EXPECT_TRUE(voidval->getChar(0,1,buf));
    EXPECT_TRUE(voidval->getShort(0,1,buf1)); 
    EXPECT_TRUE(voidval->getInt(0,1,buf2));
    EXPECT_TRUE(voidval->getLong(0,1,buf3));
    EXPECT_TRUE(voidval->getIndex(0,1,buf4));
    EXPECT_TRUE(voidval->getFloat(0,1,buf5));
    EXPECT_TRUE(voidval->getDouble(0,1,buf6));   
    EXPECT_FALSE(voidval->getSymbol(0,1,buf2,symbase,false)); 
    EXPECT_TRUE(voidval->getString(0,1,buf8));
    EXPECT_FALSE(voidval->getString(0,1,buf9));
    EXPECT_FALSE(voidval->getBinary(0,1,1,buf10));
    EXPECT_FALSE(voidval->getHash(0,1,buckets,buf2));
    cout<<voidval->getAllocatedMemory();
    EXPECT_EQ(voidval->getBoolConst(0,1,buf)[0], CHAR_MIN); 
    EXPECT_EQ(voidval->getCharConst(0,1,buf)[0], CHAR_MIN); 
    EXPECT_EQ(voidval->getShortConst(0,1,buf1)[0], SHRT_MIN); 
    EXPECT_EQ(voidval->getIntConst(0,1,buf2)[0], INT_MIN);
    EXPECT_EQ(voidval->getLongConst(0,1,buf3)[0], LLONG_MIN);
    EXPECT_EQ(voidval->getIndexConst(0,1,buf4)[0], INT_MIN);
    EXPECT_EQ(voidval->getFloatConst(0,1,buf5)[0], FLT_NMIN);
    EXPECT_EQ(voidval->getDoubleConst(0,1,buf6)[0], DBL_NMIN);   
    EXPECT_ANY_THROW(voidval->getSymbolConst(0,1,buf2,symbase,false)); 
    EXPECT_EQ(*(voidval->getStringConst(0,1,buf8)[0]),"");
    EXPECT_ANY_THROW(voidval->getStringConst(0,1,buf9));
    EXPECT_ANY_THROW(voidval->getBinaryConst(0,1,1,buf10));

    EXPECT_EQ(voidval->getBoolBuffer(0,1,buf)[0], CHAR_MIN); 
    EXPECT_EQ(voidval->getCharBuffer(0,1,buf)[0], CHAR_MIN); 
    EXPECT_EQ(voidval->getShortBuffer(0,1,buf1)[0], SHRT_MIN); 
    cout<<voidval->getIntBuffer(0,1,buf2)<<endl;
    EXPECT_EQ(buf2[0], INT_MIN);
    EXPECT_EQ(voidval->getLongBuffer(0,1,buf3)[0], LLONG_MIN);
    EXPECT_EQ(voidval->getIndexBuffer(0,1,buf4)[0], INT_MIN);
    EXPECT_EQ(voidval->getFloatBuffer(0,1,buf5)[0], FLT_NMIN);
    EXPECT_EQ(voidval->getDoubleBuffer(0,1,buf6)[0], DBL_NMIN);

    EXPECT_EQ(voidval->getStringRef(), "");

    EXPECT_EQ(voidval->getInstance()->getString(), "");
    EXPECT_EQ(voidval->getValue()->getString(), "");
    EXPECT_EQ(voidval->getRawType(), DT_VOID);

    cout<<"-----------------bool-------------------"<<endl;
    ConstantSP boolval = Util::createBool(1);
    EXPECT_EQ(boolval->getRawType(), DT_BOOL);
    ConstantSP boolval1 = boolval->getInstance();
    boolval1->setBool(1);
    EXPECT_EQ(boolval1->getBool(), boolval1->getValue()->getBool());
    EXPECT_FALSE(boolval->add(0, 1, (long long)2));
    EXPECT_FALSE(boolval->add(0, 1, (float)2));


    cout<<"-----------------string-------------------"<<endl;
    ConstantSP stringval=Util::createString("this is a string scalar");
    EXPECT_TRUE(stringval->isNull(0,1,buf0));
    EXPECT_TRUE(stringval->isValid(0,1,buf0));
    EXPECT_FALSE(stringval->getBool(0,1,buf00));
    EXPECT_FALSE(stringval->getChar(0,1,buf));
    EXPECT_FALSE(stringval->getShort(0,1,buf1)); 
    EXPECT_FALSE(stringval->getInt(0,1,buf2));
    EXPECT_FALSE(stringval->getLong(0,1,buf3));
    EXPECT_FALSE(stringval->getIndex(0,1,buf4));
    EXPECT_FALSE(stringval->getFloat(0,1,buf5));
    EXPECT_FALSE(stringval->getDouble(0,1,buf6));   
    EXPECT_FALSE(stringval->getSymbol(0,1,buf2,symbase,false)); 
    EXPECT_TRUE(stringval->getString(0,1,buf8));
    EXPECT_FALSE(stringval->getString(0,1,buf9));
    EXPECT_FALSE(stringval->getBinary(0,1,1,buf10));
    EXPECT_FALSE(stringval->getHash(0,1,buckets,buf2));
    cout<<stringval->getAllocatedMemory()<<endl;
    EXPECT_ANY_THROW(stringval->getBoolConst(0,1,buf)[0]);
    EXPECT_ANY_THROW(stringval->getCharConst(0,1,buf)[0]);
    EXPECT_ANY_THROW(stringval->getShortConst(0,1,buf1)[0]); 
    EXPECT_ANY_THROW(stringval->getIntConst(0,1,buf2)[0]);
    EXPECT_ANY_THROW(stringval->getLongConst(0,1,buf3)[0]);
    EXPECT_ANY_THROW(stringval->getIndexConst(0,1,buf4)[0]);
    EXPECT_ANY_THROW(stringval->getFloatConst(0,1,buf5)[0]);
    EXPECT_ANY_THROW(stringval->getDoubleConst(0,1,buf6)[0]);   
    EXPECT_ANY_THROW(stringval->getSymbolConst(0,1,buf2,symbase,false)); 
    EXPECT_EQ(*(stringval->getStringConst(0,1,buf8)[0]), "this is a string scalar");
    EXPECT_STREQ(*(stringval->getStringConst(0,24,buf9)), "this is a string scalar");
    EXPECT_ANY_THROW(stringval->getBinaryConst(0,1,1,buf10));
    
    stringval->getDataBuffer(0,1,buf8);
    EXPECT_EQ(*(buf8[0]), "this is a string scalar");

    EXPECT_ANY_THROW(stringval->getIndex());
    EXPECT_EQ(stringval->getStringRef(0), "this is a string scalar");
    ConstantSP strval2 = stringval->getInstance();
    strval2->setNull();
    strval2->nullFill(stringval);
    EXPECT_EQ(strval2->getString(), stringval->getValue()->getString());
    EXPECT_EQ(strval2->getRawType(), DT_STRING);
    EXPECT_EQ(strval2->compare(0, stringval), 0);

    cout<<"-----------------int-------------------"<<endl;
    ConstantSP intval=Util::createInt(100000);
    ConstantSP intNullval=Util::createNullConstant(DT_INT);
    vector<ConstantSP> intV = {intNullval, intval};
    for(auto &val:intV){
        if(val == intval){
            EXPECT_TRUE(val->isNull(0,1,buf0));
            EXPECT_FALSE(buf0[0]);
            EXPECT_TRUE(val->isValid(0,1,buf0));
            EXPECT_TRUE(val->getBool(0,1,buf00));
            EXPECT_TRUE(val->getChar(0,1,buf));
            EXPECT_TRUE(val->getShort(0,1,buf1)); 
            EXPECT_TRUE(val->getInt(0,1,buf2));
            EXPECT_TRUE(val->getLong(0,1,buf3));
            EXPECT_TRUE(val->getIndex(0,1,buf4));
            EXPECT_TRUE(val->getFloat(0,1,buf5));
            EXPECT_TRUE(val->getDouble(0,1,buf6));   
            EXPECT_FALSE(val->getSymbol(0,1,buf2,symbase,false)); 
            EXPECT_FALSE(val->getString(0,1,buf8));
            EXPECT_FALSE(val->getString(0,1,buf9));
            EXPECT_FALSE(val->getBinary(0,1,1,buf10));
            EXPECT_FALSE(val->getHash(0,1,buckets,buf2));
            cout<<val->getAllocatedMemory();
            EXPECT_EQ(val->getBoolConst(0,1,buf)[0], 1);
            EXPECT_EQ(val->getCharConst(0,1,buf)[0], '\xA0');
            // EXPECT_EQ(val->getShortConst(0,1,buf1)[0], SHRT_MIN); 
            EXPECT_EQ(val->getIntConst(0,1,buf2)[0], (int)100000); 
            EXPECT_EQ(val->getLongConst(0,1,buf3)[0], (long long)100000); 
            EXPECT_EQ(val->getIndexConst(0,1,buf4)[0], (INDEX)100000); 
            EXPECT_EQ(val->getFloatConst(0,1,buf5)[0], (float)100000); 
            EXPECT_EQ(val->getDoubleConst(0,1,buf6)[0], (double)100000); 
            EXPECT_ANY_THROW(val->getSymbolConst(0,1,buf2,symbase,false)); 
            EXPECT_ANY_THROW(val->getStringConst(0,1,buf8)[0]);
            EXPECT_ANY_THROW(val->getStringConst(0,24,buf9));
            EXPECT_ANY_THROW(val->getBinaryConst(0,1,1,buf10));
        }
        else{
            EXPECT_TRUE(val->isNull(0,1,buf0));
            EXPECT_TRUE(buf0[0]);
            EXPECT_TRUE(val->isValid(0,1,buf0));
            EXPECT_TRUE(val->getBool(0,1,buf00));
            EXPECT_TRUE(val->getChar(0,1,buf));
            EXPECT_TRUE(val->getShort(0,1,buf1)); 
            EXPECT_TRUE(val->getInt(0,1,buf2));
            EXPECT_TRUE(val->getLong(0,1,buf3));
            EXPECT_TRUE(val->getIndex(0,1,buf4));
            EXPECT_TRUE(val->getFloat(0,1,buf5));
            EXPECT_TRUE(val->getDouble(0,1,buf6));   
            EXPECT_FALSE(val->getSymbol(0,1,buf2,symbase,false)); 
            EXPECT_FALSE(val->getString(0,1,buf8));
            EXPECT_FALSE(val->getString(0,1,buf9));
            EXPECT_FALSE(val->getBinary(0,1,1,buf10));
            EXPECT_FALSE(val->getHash(0,1,buckets,buf2));
            cout<<val->getAllocatedMemory();
            EXPECT_EQ(val->getBoolConst(0,1,buf)[0], '\x80');
            EXPECT_EQ(val->getCharConst(0,1,buf)[0], '\x80');
            // EXPECT_EQ(val->getShortConst(0,1,buf1)[0], SHRT_MIN); 
            EXPECT_EQ(val->getIntConst(0,1,buf2)[0], INT_MIN); 
            EXPECT_EQ(val->getLongConst(0,1,buf3)[0], LLONG_MIN); 
            EXPECT_EQ(val->getIndexConst(0,1,buf4)[0], INDEX_MIN); 
            EXPECT_EQ(val->getFloatConst(0,1,buf5)[0], FLT_NMIN); 
            EXPECT_EQ(val->getDoubleConst(0,1,buf6)[0], DBL_NMIN); 
            EXPECT_ANY_THROW(val->getSymbolConst(0,1,buf2,symbase,false)); 
            EXPECT_ANY_THROW(val->getStringConst(0,1,buf8)[0]);
            EXPECT_ANY_THROW(val->getStringConst(0,24,buf9));
            EXPECT_ANY_THROW(val->getBinaryConst(0,1,1,buf10));
        }
    }

    EXPECT_EQ(intval->getBoolBuffer(0,1,buf)[0], '\xA0');
    EXPECT_EQ(intval->getCharBuffer(0,1,buf)[0], '\xA0');
    // EXPECT_EQ(intval->getShortBuffer(0,1,buf1)[0], SHRT_MIN); 
    cout<<intval->getIntBuffer(0,1,buf2)<<endl;
    EXPECT_EQ(buf2[0], 100000);
    EXPECT_EQ(intval->getLongBuffer(0,1,buf3)[0], 100000);
    EXPECT_EQ(intval->getIndexBuffer(0,1,buf4)[0], 100000);
    EXPECT_EQ(intval->getFloatBuffer(0,1,buf5)[0], 100000);
    EXPECT_EQ(intval->getDoubleBuffer(0,1,buf6)[0], 100000);
    intval->getDataBuffer(0,1,buf2);
    EXPECT_EQ(buf2[0], 100000);

    intval->getBinaryBuffer(0,1,sizeof(int),buf10);
    ConstantSP intval2 = Util::createConstant(DT_INT);
    intval2->setBinary(buf10,sizeof(int));
    cout<<intval2->getString()<<endl;

    EXPECT_EQ(intval->getRawType(), DT_INT);
    ConstantSP intval1 = intval->getInstance();
    intval1->setInt(100000);
    EXPECT_EQ(intval1->getInt(), intval->getValue()->getInt());

    cout<<"-----------------char-------------------"<<endl;
    char rand_val = rand()%CHAR_MAX+1;
    ConstantSP charval=Util::createChar(rand_val);
    ConstantSP charNullval=Util::createNullConstant(DT_CHAR);
    vector<ConstantSP> charV = {charNullval, charval};
    for(auto &val:charV){
        if(val == charval){
            EXPECT_TRUE(val->isNull(0,1,buf0));
            EXPECT_TRUE(val->isValid(0,1,buf0));
            EXPECT_TRUE(val->getBool(0,1,buf00));
            EXPECT_TRUE(val->getChar(0,1,buf));
            EXPECT_TRUE(val->getShort(0,1,buf1)); 
            EXPECT_TRUE(val->getInt(0,1,buf2));
            EXPECT_TRUE(val->getLong(0,1,buf3));
            EXPECT_TRUE(val->getIndex(0,1,buf4));
            EXPECT_TRUE(val->getFloat(0,1,buf5));
            EXPECT_TRUE(val->getDouble(0,1,buf6));   
            EXPECT_FALSE(val->getSymbol(0,1,buf2,symbase,false)); 
            EXPECT_FALSE(val->getString(0,1,buf8));
            EXPECT_FALSE(val->getString(0,1,buf9));
            EXPECT_FALSE(val->getBinary(0,1,1,buf10));
            EXPECT_FALSE(val->getHash(0,1,buckets,buf2));
            cout<<val->getAllocatedMemory();
            EXPECT_EQ(val->getBoolConst(0,1,buf)[0], 1);
            EXPECT_EQ(val->getCharConst(0,1,buf)[0], rand_val);
            EXPECT_EQ(val->getShortConst(0,1,buf1)[0], (short)rand_val); 
            EXPECT_EQ(val->getIntConst(0,1,buf2)[0], (int)rand_val); 
            EXPECT_EQ(val->getLongConst(0,1,buf3)[0], (long long)rand_val); 
            EXPECT_EQ(val->getIndexConst(0,1,buf4)[0], (INDEX)rand_val); 
            EXPECT_EQ(val->getFloatConst(0,1,buf5)[0], (float)rand_val); 
            EXPECT_EQ(val->getDoubleConst(0,1,buf6)[0], (double)rand_val); 
            EXPECT_ANY_THROW(val->getSymbolConst(0,1,buf2,symbase,false)); 
            EXPECT_ANY_THROW(val->getStringConst(0,1,buf8)[0]);
            EXPECT_ANY_THROW(val->getStringConst(0,24,buf9));
            EXPECT_ANY_THROW(val->getBinaryConst(0,1,1,buf10));
        }
        else{
            EXPECT_TRUE(val->isNull(0,1,buf0));
            EXPECT_TRUE(val->isValid(0,1,buf0));
            EXPECT_TRUE(val->getBool(0,1,buf00));
            EXPECT_TRUE(val->getChar(0,1,buf));
            EXPECT_TRUE(val->getShort(0,1,buf1)); 
            EXPECT_TRUE(val->getInt(0,1,buf2));
            EXPECT_TRUE(val->getLong(0,1,buf3));
            EXPECT_TRUE(val->getIndex(0,1,buf4));
            EXPECT_TRUE(val->getFloat(0,1,buf5));
            EXPECT_TRUE(val->getDouble(0,1,buf6));   
            EXPECT_FALSE(val->getSymbol(0,1,buf2,symbase,false)); 
            EXPECT_FALSE(val->getString(0,1,buf8));
            EXPECT_FALSE(val->getString(0,1,buf9));
            EXPECT_FALSE(val->getBinary(0,1,1,buf10));
            EXPECT_FALSE(val->getHash(0,1,buckets,buf2));
            cout<<val->getAllocatedMemory();
            EXPECT_EQ(val->getBoolConst(0,1,buf)[0], CHAR_MIN);
            EXPECT_EQ(val->getCharConst(0,1,buf)[0], CHAR_MIN);
            EXPECT_EQ(val->getShortConst(0,1,buf1)[0], SHRT_MIN); 
            EXPECT_EQ(val->getIntConst(0,1,buf2)[0], INT_MIN); 
            EXPECT_EQ(val->getLongConst(0,1,buf3)[0], LLONG_MIN); 
            EXPECT_EQ(val->getIndexConst(0,1,buf4)[0], INDEX_MIN); 
            EXPECT_EQ(val->getFloatConst(0,1,buf5)[0], FLT_NMIN); 
            EXPECT_EQ(val->getDoubleConst(0,1,buf6)[0], DBL_NMIN); 
            EXPECT_ANY_THROW(val->getSymbolConst(0,1,buf2,symbase,false)); 
            EXPECT_ANY_THROW(val->getStringConst(0,1,buf8)[0]);
            EXPECT_ANY_THROW(val->getStringConst(0,24,buf9));
            EXPECT_ANY_THROW(val->getBinaryConst(0,1,1,buf10));
        }
    }

    EXPECT_EQ(charval->getBoolBuffer(0,1,buf)[0], rand_val); 
    EXPECT_EQ(charval->getCharBuffer(0,1,buf)[0], rand_val); 
    EXPECT_EQ(charval->getShortBuffer(0,1,buf1)[0], (short)rand_val); 
    cout<<charval->getIntBuffer(0,1,buf2)<<endl;
    EXPECT_EQ(buf2[0], (int)rand_val);
    EXPECT_EQ(charval->getLongBuffer(0,1,buf3)[0], (long)rand_val);
    EXPECT_EQ(charval->getIndexBuffer(0,1,buf4)[0], (INDEX)rand_val);
    EXPECT_EQ(charval->getFloatBuffer(0,1,buf5)[0], (float)rand_val);
    EXPECT_EQ(charval->getDoubleBuffer(0,1,buf6)[0], (double)rand_val);
    charval->getDataBuffer(0,1,buf);
    EXPECT_EQ(buf[0], rand_val);

    EXPECT_EQ(charval->getRawType(), DT_CHAR);
    ConstantSP charval1 = charval->getInstance();
    charval1->setChar(rand_val);
    EXPECT_EQ(charval1->getChar(), charval->getValue()->getChar());

    cout<<"-----------------short-------------------"<<endl;
    short rand_val2 = rand()%SHRT_MAX+1;
    ConstantSP shortval=Util::createShort(rand_val2);
    ConstantSP shortNullval=Util::createNullConstant(DT_SHORT);
    vector<ConstantSP> shortV = {shortNullval, shortval};
    for(auto &val:shortV){
        if(val == shortval){
            EXPECT_TRUE(val->isNull(0,1,buf0));
            EXPECT_TRUE(val->isValid(0,1,buf0));
            EXPECT_TRUE(val->getBool(0,1,buf00));
            EXPECT_TRUE(val->getChar(0,1,buf));
            EXPECT_TRUE(val->getShort(0,1,buf1)); 
            EXPECT_TRUE(val->getInt(0,1,buf2));
            EXPECT_TRUE(val->getLong(0,1,buf3));
            EXPECT_TRUE(val->getIndex(0,1,buf4));
            EXPECT_TRUE(val->getFloat(0,1,buf5));
            EXPECT_TRUE(val->getDouble(0,1,buf6));   
            EXPECT_FALSE(val->getSymbol(0,1,buf2,symbase,false)); 
            EXPECT_FALSE(val->getString(0,1,buf8));
            EXPECT_FALSE(val->getString(0,1,buf9));
            EXPECT_FALSE(val->getBinary(0,1,1,buf10));
            EXPECT_FALSE(val->getHash(0,1,buckets,buf2));
            cout<<val->getAllocatedMemory();
            // EXPECT_EQ(val->getBoolConst(0,1,buf)[0], 1);
            // EXPECT_EQ(val->getCharConst(0,1,buf)[0], rand_val2);
            EXPECT_EQ(val->getShortConst(0,1,buf1)[0], (short)rand_val2); 
            EXPECT_EQ(val->getIntConst(0,1,buf2)[0], (int)rand_val2); 
            EXPECT_EQ(val->getLongConst(0,1,buf3)[0], (long long)rand_val2); 
            EXPECT_EQ(val->getIndexConst(0,1,buf4)[0], (INDEX)rand_val2); 
            EXPECT_EQ(val->getFloatConst(0,1,buf5)[0], (float)rand_val2); 
            EXPECT_EQ(val->getDoubleConst(0,1,buf6)[0], (double)rand_val2); 
            EXPECT_ANY_THROW(val->getSymbolConst(0,1,buf2,symbase,false)); 
            EXPECT_ANY_THROW(val->getStringConst(0,1,buf8)[0]);
            EXPECT_ANY_THROW(val->getStringConst(0,24,buf9));
            EXPECT_ANY_THROW(val->getBinaryConst(0,1,1,buf10));
        }
        else{
            EXPECT_TRUE(val->isNull(0,1,buf0));
            EXPECT_TRUE(val->isValid(0,1,buf0));
            EXPECT_TRUE(val->getBool(0,1,buf00));
            EXPECT_TRUE(val->getChar(0,1,buf));
            EXPECT_TRUE(val->getShort(0,1,buf1)); 
            EXPECT_TRUE(val->getInt(0,1,buf2));
            EXPECT_TRUE(val->getLong(0,1,buf3));
            EXPECT_TRUE(val->getIndex(0,1,buf4));
            EXPECT_TRUE(val->getFloat(0,1,buf5));
            EXPECT_TRUE(val->getDouble(0,1,buf6));   
            EXPECT_FALSE(val->getSymbol(0,1,buf2,symbase,false)); 
            EXPECT_FALSE(val->getString(0,1,buf8));
            EXPECT_FALSE(val->getString(0,1,buf9));
            EXPECT_FALSE(val->getBinary(0,1,1,buf10));
            EXPECT_FALSE(val->getHash(0,1,buckets,buf2));
            cout<<val->getAllocatedMemory();
            // EXPECT_EQ(val->getBoolConst(0,1,buf)[0], 1);
            // EXPECT_EQ(val->getCharConst(0,1,buf)[0], rand_val2);
            EXPECT_EQ(val->getShortConst(0,1,buf1)[0], SHRT_MIN); 
            EXPECT_EQ(val->getIntConst(0,1,buf2)[0], INT_MIN); 
            EXPECT_EQ(val->getLongConst(0,1,buf3)[0], LLONG_MIN); 
            EXPECT_EQ(val->getIndexConst(0,1,buf4)[0], INDEX_MIN); 
            EXPECT_EQ(val->getFloatConst(0,1,buf5)[0], FLT_NMIN); 
            EXPECT_EQ(val->getDoubleConst(0,1,buf6)[0], DBL_NMIN); 
            EXPECT_ANY_THROW(val->getSymbolConst(0,1,buf2,symbase,false)); 
            EXPECT_ANY_THROW(val->getStringConst(0,1,buf8)[0]);
            EXPECT_ANY_THROW(val->getStringConst(0,24,buf9));
            EXPECT_ANY_THROW(val->getBinaryConst(0,1,1,buf10));
        }
    }

    // EXPECT_EQ(shortval->getBoolBuffer(0,1,buf)[0], rand_val2); 
    // EXPECT_EQ(shortval->getCharBuffer(0,1,buf)[0], rand_val2); 
    EXPECT_EQ(shortval->getShortBuffer(0,1,buf1)[0], (short)rand_val2); 
    cout<<shortval->getIntBuffer(0,1,buf2)<<endl;
    EXPECT_EQ(buf2[0], (int)rand_val2);
    EXPECT_EQ(shortval->getLongBuffer(0,1,buf3)[0], (long)rand_val2);
    EXPECT_EQ(shortval->getIndexBuffer(0,1,buf4)[0], (INDEX)rand_val2);
    EXPECT_EQ(shortval->getFloatBuffer(0,1,buf5)[0], (float)rand_val2);
    EXPECT_EQ(shortval->getDoubleBuffer(0,1,buf6)[0], (double)rand_val2);
    shortval->getDataBuffer(0,1,buf1);
    EXPECT_EQ(buf1[0], rand_val2);

    EXPECT_EQ(shortval->getRawType(), DT_SHORT);
    ConstantSP shortval1 = shortval->getInstance();
    shortval1->setShort(rand_val2);
    EXPECT_EQ(shortval1->getShort(), shortval->getValue()->getShort());

    cout<<"-----------------long-------------------"<<endl;
    long long rand_val3 = rand()%LLONG_MAX+1;
    ConstantSP longval=Util::createLong(rand_val3);
    ConstantSP longNullval=Util::createNullConstant(DT_SHORT);
    vector<ConstantSP> longV = {longNullval, longval};
    for(auto &val:longV){
        if(val == longval){
            EXPECT_TRUE(val->isNull(0,1,buf0));
            EXPECT_TRUE(val->isValid(0,1,buf0));
            EXPECT_TRUE(val->getBool(0,1,buf00));
            EXPECT_TRUE(val->getChar(0,1,buf));
            EXPECT_TRUE(val->getShort(0,1,buf1)); 
            EXPECT_TRUE(val->getInt(0,1,buf2));
            EXPECT_TRUE(val->getLong(0,1,buf3));
            EXPECT_TRUE(val->getIndex(0,1,buf4));
            EXPECT_TRUE(val->getFloat(0,1,buf5));
            EXPECT_TRUE(val->getDouble(0,1,buf6));   
            EXPECT_FALSE(val->getSymbol(0,1,buf2,symbase,false)); 
            EXPECT_FALSE(val->getString(0,1,buf8));
            EXPECT_FALSE(val->getString(0,1,buf9));
            EXPECT_FALSE(val->getBinary(0,1,1,buf10));
            EXPECT_FALSE(val->getHash(0,1,buckets,buf2));
            cout<<val->getAllocatedMemory();
            // EXPECT_EQ(val->getBoolConst(0,1,buf)[0], 1);
            // EXPECT_EQ(val->getCharConst(0,1,buf)[0], rand_val3);
            // EXPECT_EQ(val->getShortConst(0,1,buf1)[0], (short)rand_val3); 
            EXPECT_EQ(val->getIntConst(0,1,buf2)[0], (int)rand_val3); 
            EXPECT_EQ(val->getLongConst(0,1,buf3)[0], (long long)rand_val3); 
            EXPECT_EQ(val->getIndexConst(0,1,buf4)[0], (INDEX)rand_val3); 
            EXPECT_EQ(val->getFloatConst(0,1,buf5)[0], (float)rand_val3); 
            EXPECT_EQ(val->getDoubleConst(0,1,buf6)[0], (double)rand_val3); 
            EXPECT_ANY_THROW(val->getSymbolConst(0,1,buf2,symbase,false)); 
            EXPECT_ANY_THROW(val->getStringConst(0,1,buf8)[0]);
            EXPECT_ANY_THROW(val->getStringConst(0,24,buf9));
            EXPECT_ANY_THROW(val->getBinaryConst(0,1,1,buf10));
        }
        else{
            EXPECT_TRUE(val->isNull(0,1,buf0));
            EXPECT_TRUE(val->isValid(0,1,buf0));
            EXPECT_TRUE(val->getBool(0,1,buf00));
            EXPECT_TRUE(val->getChar(0,1,buf));
            EXPECT_TRUE(val->getShort(0,1,buf1)); 
            EXPECT_TRUE(val->getInt(0,1,buf2));
            EXPECT_TRUE(val->getLong(0,1,buf3));
            EXPECT_TRUE(val->getIndex(0,1,buf4));
            EXPECT_TRUE(val->getFloat(0,1,buf5));
            EXPECT_TRUE(val->getDouble(0,1,buf6));   
            EXPECT_FALSE(val->getSymbol(0,1,buf2,symbase,false)); 
            EXPECT_FALSE(val->getString(0,1,buf8));
            EXPECT_FALSE(val->getString(0,1,buf9));
            EXPECT_FALSE(val->getBinary(0,1,1,buf10));
            EXPECT_FALSE(val->getHash(0,1,buckets,buf2));
            cout<<val->getAllocatedMemory();
            // EXPECT_EQ(val->getBoolConst(0,1,buf)[0], 1);
            // EXPECT_EQ(val->getCharConst(0,1,buf)[0], rand_val3);
            // EXPECT_EQ(val->getShortConst(0,1,buf1)[0], (short)rand_val3); 
            EXPECT_EQ(val->getIntConst(0,1,buf2)[0], INT_MIN); 
            EXPECT_EQ(val->getLongConst(0,1,buf3)[0], LLONG_MIN); 
            EXPECT_EQ(val->getIndexConst(0,1,buf4)[0], INDEX_MIN); 
            EXPECT_EQ(val->getFloatConst(0,1,buf5)[0], FLT_NMIN); 
            EXPECT_EQ(val->getDoubleConst(0,1,buf6)[0], DBL_NMIN); 
            EXPECT_ANY_THROW(val->getSymbolConst(0,1,buf2,symbase,false)); 
            EXPECT_ANY_THROW(val->getStringConst(0,1,buf8)[0]);
            EXPECT_ANY_THROW(val->getStringConst(0,24,buf9));
            EXPECT_ANY_THROW(val->getBinaryConst(0,1,1,buf10));
        }
    }

    // EXPECT_EQ(longval->getBoolBuffer(0,1,buf)[0], rand_val3); 
    // EXPECT_EQ(longval->getCharBuffer(0,1,buf)[0], rand_val3); 
    // EXPECT_EQ(longval->getShortBuffer(0,1,buf1)[0], (short)rand_val3); 
    cout<<longval->getIntBuffer(0,1,buf2)<<endl;
    EXPECT_EQ(buf2[0], (int)rand_val3);
    EXPECT_EQ(longval->getLongBuffer(0,1,buf3)[0], (long long)rand_val3);
    EXPECT_EQ(longval->getIndexBuffer(0,1,buf4)[0], (INDEX)rand_val3);
    EXPECT_EQ(longval->getFloatBuffer(0,1,buf5)[0], (float)rand_val3);
    EXPECT_EQ(longval->getDoubleBuffer(0,1,buf6)[0], (double)rand_val3);
    longval->getDataBuffer(0,1,buf3);
    EXPECT_EQ(buf3[0], rand_val3);

    EXPECT_EQ(longval->getRawType(), DT_LONG);
    ConstantSP longval1 = longval->getInstance();
    longval1->setLong(rand_val3);
    EXPECT_EQ(longval1->getString(), longval->getValue()->getString());

    cout<<"-----------------float-------------------"<<endl;
    float rand_val4 = 1.5862/*rand()/float(RAND_MAX)+1*/;
    ConstantSP floatval=Util::createFloat(rand_val4);
    ConstantSP negfloatval=Util::createFloat(0 - rand_val4);
    ConstantSP fNullval=Util::createNullConstant(DT_FLOAT);
    vector<ConstantSP> f_vals = {fNullval, negfloatval, floatval};
    for(auto &val:f_vals){
        EXPECT_TRUE(val->getBool(0,1,buf00));
        EXPECT_TRUE(val->getChar(0,1,buf));
        EXPECT_TRUE(val->getShort(0,1,buf1)); 
        EXPECT_TRUE(val->getInt(0,1,buf2));
        EXPECT_TRUE(val->getLong(0,1,buf3));
        EXPECT_TRUE(val->getIndex(0,1,buf4));
        EXPECT_TRUE(val->getFloat(0,1,buf5));
        EXPECT_TRUE(val->getDouble(0,1,buf6));   
        EXPECT_FALSE(val->getSymbol(0,1,buf2,symbase,false)); 
        EXPECT_FALSE(val->getString(0,1,buf8));
        EXPECT_FALSE(val->getString(0,1,buf9));
        EXPECT_FALSE(val->getBinary(0,1,1,buf10));
        EXPECT_FALSE(val->getHash(0,1,buckets,buf2));
        EXPECT_TRUE(val->getBoolConst(0,1,buf));
        EXPECT_TRUE(val->getCharConst(0,1,buf));
        EXPECT_TRUE(val->getShortConst(0,1,buf1)); 
        EXPECT_TRUE(val->getIntConst(0,1,buf2));
        EXPECT_TRUE(val->getLongConst(0,1,buf3));
        EXPECT_TRUE(val->getIndexConst(0,1,buf4));
        EXPECT_TRUE(val->getFloatConst(0,1,buf5));
        EXPECT_TRUE(val->getDoubleConst(0,1,buf6));   
        EXPECT_ANY_THROW(val->getSymbolConst(0,1,buf2,symbase,false)[0]); 
        EXPECT_ANY_THROW(val->getStringConst(0,1,buf8)[0]);
        EXPECT_ANY_THROW(val->getStringConst(0,1,buf9)[0]);
        EXPECT_ANY_THROW(val->getBinaryConst(0,1,1,buf10)[0]);
    }

    // EXPECT_EQ(longval->getBoolBuffer(0,1,buf)[0], 1); 
    // EXPECT_EQ(longval->getCharBuffer(0,1,buf)[0], rand_val4); 
    EXPECT_EQ(floatval->getShortBuffer(0,1,buf1)[0], (short)2); 
    cout<<floatval->getIntBuffer(0,1,buf2)<<endl;
    EXPECT_EQ(buf2[0], (int)2);
    EXPECT_EQ(floatval->getLongBuffer(0,1,buf3)[0], (long long)2);
    EXPECT_EQ(floatval->getIndexBuffer(0,1,buf4)[0], (INDEX)1);
    EXPECT_EQ(floatval->getFloatBuffer(0,1,buf5)[0], (float)rand_val4);
    EXPECT_EQ(floatval->getDoubleBuffer(0,1,buf6)[0], (double)rand_val4);
    floatval->getDataBuffer(0,1,buf5);
    EXPECT_EQ(buf5[0], rand_val4);

    EXPECT_EQ(floatval->getRawType(), DT_FLOAT);
    ConstantSP floatval1 = floatval->getInstance();
    floatval1->setFloat(rand_val4);
    EXPECT_EQ(floatval1->getFloat(), floatval->getValue()->getFloat());

    cout<<"-----------------double-------------------"<<endl;
    double rand_val5 = 2.1345/*rand()/float(RAND_MAX)+1*/;
    ConstantSP doubleval=Util::createDouble(rand_val5);
    ConstantSP negdoubleval=Util::createDouble(0 - rand_val5);
    ConstantSP doubleNullval=Util::createNullConstant(DT_DOUBLE);
    vector<ConstantSP> double_vals = {doubleNullval, negdoubleval, doubleval};
    for(auto &val:double_vals){
        EXPECT_TRUE(val->getBool(0,1,buf00));
        EXPECT_TRUE(val->getChar(0,1,buf));
        EXPECT_TRUE(val->getShort(0,1,buf1)); 
        EXPECT_TRUE(val->getInt(0,1,buf2));
        EXPECT_TRUE(val->getLong(0,1,buf3));
        EXPECT_TRUE(val->getIndex(0,1,buf4));
        EXPECT_TRUE(val->getFloat(0,1,buf5));
        EXPECT_TRUE(val->getDouble(0,1,buf6));   
        EXPECT_FALSE(val->getSymbol(0,1,buf2,symbase,false)); 
        EXPECT_FALSE(val->getString(0,1,buf8));
        EXPECT_FALSE(val->getString(0,1,buf9));
        EXPECT_FALSE(val->getBinary(0,1,1,buf10));
        EXPECT_FALSE(val->getHash(0,1,buckets,buf2));
        EXPECT_TRUE(val->getBoolConst(0,1,buf));
        EXPECT_TRUE(val->getCharConst(0,1,buf));
        EXPECT_TRUE(val->getShortConst(0,1,buf1)); 
        EXPECT_TRUE(val->getIntConst(0,1,buf2));
        EXPECT_TRUE(val->getLongConst(0,1,buf3));
        EXPECT_TRUE(val->getIndexConst(0,1,buf4));
        EXPECT_TRUE(val->getFloatConst(0,1,buf5));
        EXPECT_TRUE(val->getDoubleConst(0,1,buf6));   
        EXPECT_ANY_THROW(val->getSymbolConst(0,1,buf2,symbase,false)); 
        EXPECT_ANY_THROW(val->getStringConst(0,1,buf8));
        EXPECT_ANY_THROW(val->getStringConst(0,1,buf9));
        EXPECT_ANY_THROW(val->getBinaryConst(0,1,1,buf10));
    }

    // EXPECT_EQ(longval->getBoolBuffer(0,1,buf)[0], 1); 
    // EXPECT_EQ(longval->getCharBuffer(0,1,buf)[0], rand_val4); 
    EXPECT_EQ(doubleval->getShortBuffer(0,1,buf1)[0], (short)2); 
    cout<<doubleval->getIntBuffer(0,1,buf2)<<endl;
    EXPECT_EQ(buf2[0], (int)2);
    EXPECT_EQ(doubleval->getLongBuffer(0,1,buf3)[0], (long long)2);
    EXPECT_EQ(doubleval->getIndexBuffer(0,1,buf4)[0], (INDEX)2);
    EXPECT_EQ(doubleval->getFloatBuffer(0,1,buf5)[0], (float)rand_val5);
    EXPECT_EQ(doubleval->getDoubleBuffer(0,1,buf6)[0], (double)rand_val5);
    doubleval->getDataBuffer(0,1,buf6);
    EXPECT_EQ(buf6[0], rand_val5);

    EXPECT_EQ(doubleval->getRawType(), DT_DOUBLE);
    ConstantSP doubleval1 = doubleval->getInstance();
    doubleval1->setDouble(rand_val5);
    EXPECT_EQ(doubleval1->getDouble(), doubleval->getValue()->getDouble());

    cout<<"-----------------int128-------------------"<<endl;
    unsigned char int128[16];
    for(auto i=0; i<16; i++)
        int128[i] = rand() % CHAR_MAX;
    ConstantSP int128val = Util::createConstant(DT_INT128);
    int128val->setBinary(int128, sizeof(Guid));
    ConstantSP int128val2 = int128val->getInstance();
    int128val2->setNull(0);
    EXPECT_TRUE(int128val2->isNull(0, 1, buf0));
    EXPECT_EQ(buf0[0], 1);

    EXPECT_TRUE(int128val2->isValid(0, 1, buf00));
    EXPECT_EQ(buf00[0], 0);
    int128val2->nullFill(int128val);
    EXPECT_EQ(int128val2->getString(), int128val->getValue()->getString());

    EXPECT_EQ(int128val2->compare(1, int128val), 0);

    cout<<"-----------------uuid-------------------"<<endl;
    unsigned char uuid[16];
    for(auto i=0; i<16; i++)
        uuid[i] = rand() % CHAR_MAX;
    ConstantSP uuidval = Util::createConstant(DT_UUID);
    uuidval->setBinary(uuid, sizeof(Guid));
    ConstantSP uuidval2 = uuidval->getInstance();
    uuidval2->setNull(0);
    EXPECT_TRUE(uuidval2->isNull(0, 1, buf0));
    EXPECT_EQ(buf0[0], 1);

    EXPECT_TRUE(uuidval2->isValid(0, 1, buf00));
    EXPECT_EQ(buf00[0], 0);
    uuidval2->nullFill(uuidval);
    EXPECT_EQ(uuidval2->getString(), uuidval->getValue()->getString());

    #ifndef WINDOWS
    Uuid uuidval3 = Uuid(false);
    Uuid uuidval4 = Uuid(true);
    Uuid uuidval5 = Uuid(uuid);
    const char* uuid_data2 = "225d0132-1c2c-0710-3734-563613716f07";
    Uuid uuidval6 = Uuid(uuid_data2, 0);
    EXPECT_ANY_THROW(Uuid uuidval7 = Uuid(uuid_data2, 50));
    Uuid uuidval7 = Uuid(uuid_data2, 36);
    Uuid uuidval8 = Uuid(uuidval7);

    EXPECT_EQ(uuidval3.getString(), "00000000-0000-0000-0000-000000000000");
    EXPECT_EQ(uuidval3.getString(), uuidval6.getString());
    EXPECT_EQ(uuidval->getString(), uuidval5.getString());
    EXPECT_EQ(uuidval7.getString(), uuidval8.getString());
    #endif
    cout<<"-----------------ipaddr-------------------"<<endl;
    unsigned char ip[16];
    for(auto i=0; i<16; i++)
        ip[i] = rand() % CHAR_MAX;
    ConstantSP ipval = Util::createConstant(DT_IP);
    ipval->setBinary(ip, sizeof(Guid));
    ConstantSP ipval2 = ipval->getInstance();
    ipval2->setNull(0);
    EXPECT_TRUE(ipval2->isNull(0, 1, buf0));
    EXPECT_EQ(buf0[0], 1);

    EXPECT_TRUE(ipval2->isValid(0, 1, buf00));
    EXPECT_EQ(buf00[0], 0);
    ipval2->nullFill(ipval);
    EXPECT_EQ(ipval2->getString(), ipval->getValue()->getString());

    #ifndef WINDOWS
    const char* ip_1 = "";
    const char* ip_2 = "1.1.1";
    EXPECT_EQ(IPAddr(ip_1, 0).getString(), "0.0.0.0");
    EXPECT_EQ(IPAddr(ip_2, 6).getString(), "0.0.0.0");
    #endif

	delete[] buf, buf1, buf2, buf3, buf4, buf5, buf6, buf9, buf10, buf00, buf0, buf8;
}

TEST_F(ScalarTest,testFunctionCastTemporal_month){
    ConstantSP monthval = Util::createMonth(1);

    EXPECT_ANY_THROW(monthval->castTemporal(DT_CHAR));
    EXPECT_ANY_THROW(monthval->castTemporal(DT_STRING));
    EXPECT_ANY_THROW(monthval->castTemporal(DT_DATE));
    EXPECT_EQ(monthval->castTemporal(DT_MONTH)->getString(),conn.run("month(month(1))")->getString());
    EXPECT_ANY_THROW(monthval->castTemporal(DT_TIME));
    EXPECT_ANY_THROW(monthval->castTemporal(DT_MINUTE));
    EXPECT_ANY_THROW(monthval->castTemporal(DT_SECOND));
    EXPECT_ANY_THROW(monthval->castTemporal(DT_DATETIME));
    EXPECT_ANY_THROW(monthval->castTemporal(DT_TIMESTAMP));
    EXPECT_ANY_THROW(monthval->castTemporal(DT_NANOTIME));
    EXPECT_ANY_THROW(monthval->castTemporal(DT_NANOTIMESTAMP));
    EXPECT_ANY_THROW(monthval->castTemporal(DT_DATEHOUR));

}

TEST_F(ScalarTest,testFunctionCastTemporal_datatime){
    ConstantSP datetimeval = Util::createDateTime(1);

    EXPECT_ANY_THROW(datetimeval->castTemporal(DT_CHAR));
    EXPECT_ANY_THROW(datetimeval->castTemporal(DT_STRING));
    EXPECT_EQ(datetimeval->castTemporal(DT_DATE)->getString(),conn.run("date(datetime(1))")->getString());
    EXPECT_EQ(datetimeval->castTemporal(DT_MONTH)->getString(),conn.run("month(datetime(1))")->getString());
    EXPECT_EQ(datetimeval->castTemporal(DT_TIME)->getString(),conn.run("time(datetime(1))")->getString());
    EXPECT_EQ(datetimeval->castTemporal(DT_MINUTE)->getString(),conn.run("minute(datetime(1))")->getString());
    EXPECT_EQ(datetimeval->castTemporal(DT_SECOND)->getString(),conn.run("second(datetime(1))")->getString());
    EXPECT_EQ(datetimeval->castTemporal(DT_DATETIME)->getString(),conn.run("datetime(datetime(1))")->getString());
    EXPECT_EQ(datetimeval->castTemporal(DT_TIMESTAMP)->getString(),conn.run("timestamp(datetime(1))")->getString());
    EXPECT_EQ(datetimeval->castTemporal(DT_NANOTIME)->getString(),conn.run("nanotime(datetime(1))")->getString());
    EXPECT_EQ(datetimeval->castTemporal(DT_NANOTIMESTAMP)->getString(),conn.run("nanotimestamp(datetime(1))")->getString());
    EXPECT_EQ(datetimeval->castTemporal(DT_DATEHOUR)->getString(),conn.run("datehour(datetime(1))")->getString()); 

    int negSec = 0 - rand() % INT_MAX;
    int posSec = rand() % INT_MAX;
    negSec = negSec % 3600 == 0 ? negSec+1 : negSec;
    posSec = posSec % 3600 == 0 ? posSec+1 : posSec;
    int negSec3600 = 0 - (rand() % 10) * 3600;
    int posSec3600 = (rand() % 10) * 3600;

    int negSec86400 = (0 - rand() % 10) * 86400;
    int posSec86400 = (rand() % 10) * 86400;

    // cout<<negSec<<endl<<posSec<<endl<<negSec3600<<endl<<posSec3600<<endl;
    EXPECT_EQ(Util::createDateTime(INT_MIN)->castTemporal(DT_NANOTIMESTAMP)->getString(), "");
    EXPECT_EQ(Util::createDateTime(INT_MIN)->castTemporal(DT_DATE)->getString(),conn.run("date(datetime("+to_string(INT_MIN)+"))")->getString());
    EXPECT_EQ(Util::createDateTime(INT_MIN)->castTemporal(DT_MONTH)->getString(),conn.run("month(datetime("+to_string(INT_MIN)+"))")->getString());
    EXPECT_EQ(Util::createDateTime(INT_MIN)->castTemporal(DT_TIME)->getString(),conn.run("time(datetime("+to_string(INT_MIN)+"))")->getString());
    EXPECT_EQ(Util::createDateTime(INT_MIN)->castTemporal(DT_MINUTE)->getString(),conn.run("minute(datetime("+to_string(INT_MIN)+"))")->getString());
    EXPECT_EQ(Util::createDateTime(INT_MIN)->castTemporal(DT_SECOND)->getString(),conn.run("second(datetime("+to_string(INT_MIN)+"))")->getString());
    EXPECT_EQ(Util::createDateTime(INT_MIN)->castTemporal(DT_DATETIME)->getString(),conn.run("datetime(datetime("+to_string(INT_MIN)+"))")->getString());
    EXPECT_EQ(Util::createDateTime(INT_MIN)->castTemporal(DT_TIMESTAMP)->getString(),"");
    EXPECT_EQ(Util::createDateTime(INT_MIN)->castTemporal(DT_NANOTIME)->getString(),"");
    EXPECT_EQ(Util::createDateTime(INT_MIN)->castTemporal(DT_DATEHOUR)->getString(),conn.run("datehour(datetime("+to_string(INT_MIN)+"))")->getString());


    EXPECT_EQ(Util::createDateTime(negSec)->castTemporal(DT_DATEHOUR)->getString(),conn.run("datehour(datetime("+to_string(negSec)+"))")->getString());
    EXPECT_EQ(Util::createDateTime(negSec3600)->castTemporal(DT_DATEHOUR)->getString(),conn.run("datehour(datetime("+to_string(negSec3600)+"))")->getString());
    EXPECT_EQ(Util::createDateTime(posSec)->castTemporal(DT_DATEHOUR)->getString(),conn.run("datehour(datetime("+to_string(posSec)+"))")->getString());
    EXPECT_EQ(Util::createDateTime(posSec3600)->castTemporal(DT_DATEHOUR)->getString(),conn.run("datehour(datetime("+to_string(posSec3600)+"))")->getString());

    EXPECT_EQ(Util::createDateTime(negSec)->castTemporal(DT_DATE)->getString(),conn.run("date(datetime("+to_string(negSec)+"))")->getString());
    EXPECT_EQ(Util::createDateTime(negSec86400)->castTemporal(DT_DATE)->getString(),conn.run("date(datetime("+to_string(negSec86400)+"))")->getString());
    EXPECT_EQ(Util::createDateTime(posSec)->castTemporal(DT_DATE)->getString(),conn.run("date(datetime("+to_string(posSec)+"))")->getString());
    EXPECT_EQ(Util::createDateTime(posSec86400)->castTemporal(DT_DATE)->getString(),conn.run("date(datetime("+to_string(posSec86400)+"))")->getString());

    EXPECT_EQ(Util::createDateTime(negSec)->castTemporal(DT_NANOTIME)->getString(),conn.run("nanotime(datetime("+to_string(negSec)+"))")->getString());
    EXPECT_EQ(Util::createDateTime(negSec86400)->castTemporal(DT_NANOTIME)->getString(),conn.run("nanotime(datetime("+to_string(negSec86400)+"))")->getString());
    EXPECT_EQ(Util::createDateTime(posSec)->castTemporal(DT_NANOTIME)->getString(),conn.run("nanotime(datetime("+to_string(posSec)+"))")->getString());
    EXPECT_EQ(Util::createDateTime(posSec86400)->castTemporal(DT_NANOTIME)->getString(),conn.run("nanotime(datetime("+to_string(posSec86400)+"))")->getString());

    EXPECT_EQ(Util::createDateTime(negSec)->castTemporal(DT_MINUTE)->getString(),conn.run("minute(datetime("+to_string(negSec)+"))")->getString());
    EXPECT_EQ(Util::createDateTime(negSec86400)->castTemporal(DT_MINUTE)->getString(),conn.run("minute(datetime("+to_string(negSec86400)+"))")->getString());
    EXPECT_EQ(Util::createDateTime(posSec)->castTemporal(DT_MINUTE)->getString(),conn.run("minute(datetime("+to_string(posSec)+"))")->getString());
    EXPECT_EQ(Util::createDateTime(posSec86400)->castTemporal(DT_MINUTE)->getString(),conn.run("minute(datetime("+to_string(posSec86400)+"))")->getString());

    EXPECT_EQ(Util::createDateTime(negSec)->castTemporal(DT_SECOND)->getString(),conn.run("second(datetime("+to_string(negSec)+"))")->getString());
    EXPECT_EQ(Util::createDateTime(negSec86400)->castTemporal(DT_SECOND)->getString(),conn.run("second(datetime("+to_string(negSec86400)+"))")->getString());
    EXPECT_EQ(Util::createDateTime(posSec)->castTemporal(DT_SECOND)->getString(),conn.run("second(datetime("+to_string(posSec)+"))")->getString());
    EXPECT_EQ(Util::createDateTime(posSec86400)->castTemporal(DT_SECOND)->getString(),conn.run("second(datetime("+to_string(posSec86400)+"))")->getString());

}

TEST_F(ScalarTest,testFunctionCastTemporal_NanoTimestamp){
    srand(time(NULL));
    ConstantSP nanotimestampval = Util::createNanoTimestamp(1000000);

    EXPECT_ANY_THROW(nanotimestampval->castTemporal(DT_CHAR));
    EXPECT_ANY_THROW(nanotimestampval->castTemporal(DT_STRING));
    EXPECT_EQ(nanotimestampval->castTemporal(DT_DATE)->getString(),conn.run("date(nanotimestamp(1000000))")->getString());
    EXPECT_EQ(nanotimestampval->castTemporal(DT_MONTH)->getString(),conn.run("month(nanotimestamp(1000000))")->getString());
    EXPECT_EQ(nanotimestampval->castTemporal(DT_TIME)->getString(),conn.run("time(nanotimestamp(1000000))")->getString());
    EXPECT_EQ(nanotimestampval->castTemporal(DT_MINUTE)->getString(),conn.run("minute(nanotimestamp(1000000))")->getString());
    EXPECT_EQ(nanotimestampval->castTemporal(DT_SECOND)->getString(),conn.run("second(nanotimestamp(1000000))")->getString());
    EXPECT_EQ(nanotimestampval->castTemporal(DT_DATETIME)->getString(),conn.run("datetime(nanotimestamp(1000000))")->getString());
    EXPECT_EQ(nanotimestampval->castTemporal(DT_TIMESTAMP)->getString(),conn.run("timestamp(nanotimestamp(1000000))")->getString());
    EXPECT_EQ(nanotimestampval->castTemporal(DT_NANOTIME)->getString(),conn.run("nanotime(nanotimestamp(1000000))")->getString());
    EXPECT_EQ(nanotimestampval->castTemporal(DT_NANOTIMESTAMP)->getString(),conn.run("nanotimestamp(nanotimestamp(1000000))")->getString());
    EXPECT_EQ(nanotimestampval->castTemporal(DT_DATEHOUR)->getString(),conn.run("datehour(nanotimestamp(1000000))")->getString());

    long long negSec = 0 - rand() % LLONG_MAX;
    long long posSec = rand() % LLONG_MAX;
    negSec = negSec % 3600000000000 == 0 ? negSec+1 : negSec;
    posSec = posSec % 3600000000000 == 0 ? posSec+1 : posSec;
    long long negSec3600 = 0 - (rand() % 10) * 3600000000000;

    long long posSec3600 = (rand() % 10) * 3600000000000;

    long long negSec86400 = (0 - rand() % 10) * 86400000000000;
    long long posSec86400 = (rand() % 10) * 86400000000000;

    // cout<<negSec<<endl<<posSec<<endl<<negSec3600<<endl<<posSec3600<<endl;
    EXPECT_EQ(Util::createNanoTimestamp(LLONG_MIN)->castTemporal(DT_NANOTIMESTAMP)->getString(),conn.run("nanotimestamp(nanotimestamp("+to_string(LLONG_MIN)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(LLONG_MIN)->castTemporal(DT_DATE)->getString(),conn.run("date(nanotimestamp("+to_string(LLONG_MIN)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(LLONG_MIN)->castTemporal(DT_MONTH)->getString(),conn.run("month(nanotimestamp("+to_string(LLONG_MIN)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(LLONG_MIN)->castTemporal(DT_TIME)->getString(),conn.run("time(nanotimestamp("+to_string(LLONG_MIN)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(LLONG_MIN)->castTemporal(DT_MINUTE)->getString(),conn.run("minute(nanotimestamp("+to_string(LLONG_MIN)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(LLONG_MIN)->castTemporal(DT_SECOND)->getString(),conn.run("second(nanotimestamp("+to_string(LLONG_MIN)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(LLONG_MIN)->castTemporal(DT_DATETIME)->getString(),conn.run("datetime(nanotimestamp("+to_string(LLONG_MIN)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(LLONG_MIN)->castTemporal(DT_TIMESTAMP)->getString(),"");
    EXPECT_EQ(Util::createNanoTimestamp(LLONG_MIN)->castTemporal(DT_NANOTIME)->getString(),"");
    EXPECT_EQ(Util::createNanoTimestamp(LLONG_MIN)->castTemporal(DT_DATEHOUR)->getString(),conn.run("datehour(nanotimestamp("+to_string(LLONG_MIN)+"))")->getString());

    EXPECT_EQ(Util::createNanoTimestamp(negSec)->castTemporal(DT_DATEHOUR)->getString(),conn.run("datehour(nanotimestamp("+to_string(negSec)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(negSec3600)->castTemporal(DT_DATEHOUR)->getString(),conn.run("datehour(nanotimestamp("+to_string(negSec3600)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(posSec)->castTemporal(DT_DATEHOUR)->getString(),conn.run("datehour(nanotimestamp("+to_string(posSec)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(posSec3600)->castTemporal(DT_DATEHOUR)->getString(),conn.run("datehour(nanotimestamp("+to_string(posSec3600)+"))")->getString());

    EXPECT_EQ(Util::createNanoTimestamp(negSec)->castTemporal(DT_DATE)->getString(),conn.run("date(nanotimestamp("+to_string(negSec)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(negSec86400)->castTemporal(DT_DATE)->getString(),conn.run("date(nanotimestamp("+to_string(negSec86400)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(posSec)->castTemporal(DT_DATE)->getString(),conn.run("date(nanotimestamp("+to_string(posSec)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(posSec86400)->castTemporal(DT_DATE)->getString(),conn.run("date(nanotimestamp("+to_string(posSec86400)+"))")->getString());

    EXPECT_EQ(Util::createNanoTimestamp(negSec)->castTemporal(DT_DATETIME)->getString(),conn.run("datetime(nanotimestamp("+to_string(negSec)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(negSec86400)->castTemporal(DT_DATETIME)->getString(),conn.run("datetime(nanotimestamp("+to_string(negSec86400)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(posSec)->castTemporal(DT_DATETIME)->getString(),conn.run("datetime(nanotimestamp("+to_string(posSec)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(posSec86400)->castTemporal(DT_DATETIME)->getString(),conn.run("datetime(nanotimestamp("+to_string(posSec86400)+"))")->getString());

    EXPECT_EQ(Util::createNanoTimestamp(negSec)->castTemporal(DT_NANOTIME)->getString(),conn.run("nanotime(nanotimestamp("+to_string(negSec)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(negSec86400)->castTemporal(DT_NANOTIME)->getString(),conn.run("nanotime(nanotimestamp("+to_string(negSec86400)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(posSec)->castTemporal(DT_NANOTIME)->getString(),conn.run("nanotime(nanotimestamp("+to_string(posSec)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(posSec86400)->castTemporal(DT_NANOTIME)->getString(),conn.run("nanotime(nanotimestamp("+to_string(posSec86400)+"))")->getString());

    EXPECT_EQ(Util::createNanoTimestamp(negSec)->castTemporal(DT_MINUTE)->getString(),conn.run("minute(nanotimestamp("+to_string(negSec)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(negSec86400)->castTemporal(DT_MINUTE)->getString(),conn.run("minute(nanotimestamp("+to_string(negSec86400)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(posSec)->castTemporal(DT_MINUTE)->getString(),conn.run("minute(nanotimestamp("+to_string(posSec)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(posSec86400)->castTemporal(DT_MINUTE)->getString(),conn.run("minute(nanotimestamp("+to_string(posSec86400)+"))")->getString());

    EXPECT_EQ(Util::createNanoTimestamp(negSec)->castTemporal(DT_SECOND)->getString(),conn.run("second(nanotimestamp("+to_string(negSec)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(negSec86400)->castTemporal(DT_SECOND)->getString(),conn.run("second(nanotimestamp("+to_string(negSec86400)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(posSec)->castTemporal(DT_SECOND)->getString(),conn.run("second(nanotimestamp("+to_string(posSec)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(posSec86400)->castTemporal(DT_SECOND)->getString(),conn.run("second(nanotimestamp("+to_string(posSec86400)+"))")->getString());

    EXPECT_EQ(Util::createNanoTimestamp(negSec)->castTemporal(DT_TIMESTAMP)->getString(),conn.run("timestamp(nanotimestamp("+to_string(negSec)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(negSec86400)->castTemporal(DT_TIMESTAMP)->getString(),conn.run("timestamp(nanotimestamp("+to_string(negSec86400)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(posSec)->castTemporal(DT_TIMESTAMP)->getString(),conn.run("timestamp(nanotimestamp("+to_string(posSec)+"))")->getString());
    EXPECT_EQ(Util::createNanoTimestamp(posSec86400)->castTemporal(DT_TIMESTAMP)->getString(),conn.run("timestamp(nanotimestamp("+to_string(posSec86400)+"))")->getString());

}

TEST_F(ScalarTest,testFunctionCastTemporal_Timestamp){
    srand(time(NULL));
    ConstantSP timestampval = Util::createTimestamp(1000000);

    EXPECT_ANY_THROW(timestampval->castTemporal(DT_CHAR));
    EXPECT_ANY_THROW(timestampval->castTemporal(DT_STRING));
    EXPECT_EQ(timestampval->castTemporal(DT_DATE)->getString(),conn.run("date(timestamp(1000000))")->getString());
    EXPECT_EQ(timestampval->castTemporal(DT_MONTH)->getString(),conn.run("month(timestamp(1000000))")->getString());
    EXPECT_EQ(timestampval->castTemporal(DT_TIME)->getString(),conn.run("time(timestamp(1000000))")->getString());
    EXPECT_EQ(timestampval->castTemporal(DT_MINUTE)->getString(),conn.run("minute(timestamp(1000000))")->getString());
    EXPECT_EQ(timestampval->castTemporal(DT_SECOND)->getString(),conn.run("second(timestamp(1000000))")->getString());
    EXPECT_EQ(timestampval->castTemporal(DT_DATETIME)->getString(),conn.run("datetime(timestamp(1000000))")->getString());
    EXPECT_EQ(timestampval->castTemporal(DT_TIMESTAMP)->getString(),conn.run("timestamp(timestamp(1000000))")->getString());
    EXPECT_EQ(timestampval->castTemporal(DT_NANOTIME)->getString(),conn.run("nanotime(timestamp(1000000))")->getString());
    EXPECT_EQ(timestampval->castTemporal(DT_NANOTIMESTAMP)->getString(),conn.run("nanotimestamp(timestamp(1000000))")->getString());
    EXPECT_EQ(timestampval->castTemporal(DT_DATEHOUR)->getString(),conn.run("datehour(timestamp(1000000))")->getString()); 

    long long negSec = 0 - rand() % LLONG_MAX;
    long long posSec = rand() % LLONG_MAX;
    negSec = negSec % 3600 == 0 ? negSec+1 : negSec;
    posSec = posSec % 3600 == 0 ? posSec+1 : posSec;
    long long negSec3600 = 0 - (rand() % 10) * 3600;

    long long posSec3600 = (rand() % 10) * 3600;

    long long negSec86400 = (0 - rand() % 10) * 86400000;
    long long posSec86400 = (rand() % 10) * 86400000;

    // cout<<negSec<<endl<<posSec<<endl<<negSec3600<<endl<<posSec3600<<endl;
    EXPECT_EQ(Util::createTimestamp(LLONG_MIN)->castTemporal(DT_NANOTIMESTAMP)->getString(),"");
    EXPECT_EQ(Util::createTimestamp(LLONG_MIN)->castTemporal(DT_DATE)->getString(),conn.run("date(timestamp("+to_string(LLONG_MIN)+"))")->getString());
    EXPECT_EQ(Util::createTimestamp(LLONG_MIN)->castTemporal(DT_MONTH)->getString(),conn.run("month(timestamp("+to_string(LLONG_MIN)+"))")->getString());
    EXPECT_EQ(Util::createTimestamp(LLONG_MIN)->castTemporal(DT_TIME)->getString(),conn.run("time(timestamp("+to_string(LLONG_MIN)+"))")->getString());
    EXPECT_EQ(Util::createTimestamp(LLONG_MIN)->castTemporal(DT_MINUTE)->getString(),conn.run("minute(timestamp("+to_string(LLONG_MIN)+"))")->getString());
    EXPECT_EQ(Util::createTimestamp(LLONG_MIN)->castTemporal(DT_SECOND)->getString(),conn.run("second(timestamp("+to_string(LLONG_MIN)+"))")->getString());
    EXPECT_EQ(Util::createTimestamp(LLONG_MIN)->castTemporal(DT_DATETIME)->getString(),conn.run("datetime(timestamp("+to_string(LLONG_MIN)+"))")->getString());
    EXPECT_EQ(Util::createTimestamp(LLONG_MIN)->castTemporal(DT_TIMESTAMP)->getString(),"");
    EXPECT_EQ(Util::createTimestamp(LLONG_MIN)->castTemporal(DT_NANOTIME)->getString(),"");
    EXPECT_EQ(Util::createTimestamp(LLONG_MIN)->castTemporal(DT_DATEHOUR)->getString(),conn.run("datehour(timestamp("+to_string(LLONG_MIN)+"))")->getString());

    EXPECT_EQ(Util::createTimestamp(negSec)->castTemporal(DT_DATEHOUR)->getString(),conn.run("datehour(timestamp("+to_string(negSec)+"))")->getString());
    EXPECT_EQ(Util::createTimestamp(negSec3600)->castTemporal(DT_DATEHOUR)->getString(),conn.run("datehour(timestamp("+to_string(negSec3600)+"))")->getString());
    EXPECT_EQ(Util::createTimestamp(posSec)->castTemporal(DT_DATEHOUR)->getString(),conn.run("datehour(timestamp("+to_string(posSec)+"))")->getString());
    EXPECT_EQ(Util::createTimestamp(posSec3600)->castTemporal(DT_DATEHOUR)->getString(),conn.run("datehour(timestamp("+to_string(posSec3600)+"))")->getString());

    EXPECT_EQ(Util::createTimestamp(negSec)->castTemporal(DT_DATE)->getString(),conn.run("date(timestamp("+to_string(negSec)+"))")->getString());
    EXPECT_EQ(Util::createTimestamp(negSec86400)->castTemporal(DT_DATE)->getString(),conn.run("date(timestamp("+to_string(negSec86400)+"))")->getString());
    EXPECT_EQ(Util::createTimestamp(posSec)->castTemporal(DT_DATE)->getString(),conn.run("date(timestamp("+to_string(posSec)+"))")->getString());
    EXPECT_EQ(Util::createTimestamp(posSec86400)->castTemporal(DT_DATE)->getString(),conn.run("date(timestamp("+to_string(posSec86400)+"))")->getString());

    EXPECT_EQ(Util::createTimestamp(negSec)->castTemporal(DT_DATETIME)->getString(),conn.run("datetime(timestamp("+to_string(negSec)+"))")->getString());
    EXPECT_EQ(Util::createTimestamp(negSec86400)->castTemporal(DT_DATETIME)->getString(),conn.run("datetime(timestamp("+to_string(negSec86400)+"))")->getString());
    EXPECT_EQ(Util::createTimestamp(posSec)->castTemporal(DT_DATETIME)->getString(),conn.run("datetime(timestamp("+to_string(posSec)+"))")->getString());
    EXPECT_EQ(Util::createTimestamp(posSec86400)->castTemporal(DT_DATETIME)->getString(),conn.run("datetime(timestamp("+to_string(posSec86400)+"))")->getString());

    EXPECT_EQ(Util::createTimestamp(negSec)->castTemporal(DT_NANOTIME)->getString(),conn.run("nanotime(timestamp("+to_string(negSec)+"))")->getString());
    EXPECT_EQ(Util::createTimestamp(negSec86400)->castTemporal(DT_NANOTIME)->getString(),conn.run("nanotime(timestamp("+to_string(negSec86400)+"))")->getString());
    EXPECT_EQ(Util::createTimestamp(posSec)->castTemporal(DT_NANOTIME)->getString(),conn.run("nanotime(timestamp("+to_string(posSec)+"))")->getString());
    EXPECT_EQ(Util::createTimestamp(posSec86400)->castTemporal(DT_NANOTIME)->getString(),conn.run("nanotime(timestamp("+to_string(posSec86400)+"))")->getString());

    EXPECT_EQ(Util::createTimestamp(negSec)->castTemporal(DT_MINUTE)->getString(),conn.run("minute(timestamp("+to_string(negSec)+"))")->getString());
    EXPECT_EQ(Util::createTimestamp(negSec86400)->castTemporal(DT_MINUTE)->getString(),conn.run("minute(timestamp("+to_string(negSec86400)+"))")->getString());
    EXPECT_EQ(Util::createTimestamp(posSec)->castTemporal(DT_MINUTE)->getString(),conn.run("minute(timestamp("+to_string(posSec)+"))")->getString());
    EXPECT_EQ(Util::createTimestamp(posSec86400)->castTemporal(DT_MINUTE)->getString(),conn.run("minute(timestamp("+to_string(posSec86400)+"))")->getString());

    EXPECT_EQ(Util::createTimestamp(negSec)->castTemporal(DT_SECOND)->getString(),conn.run("second(timestamp("+to_string(negSec)+"))")->getString());
    EXPECT_EQ(Util::createTimestamp(negSec86400)->castTemporal(DT_SECOND)->getString(),conn.run("second(timestamp("+to_string(negSec86400)+"))")->getString());
    EXPECT_EQ(Util::createTimestamp(posSec)->castTemporal(DT_SECOND)->getString(),conn.run("second(timestamp("+to_string(posSec)+"))")->getString());
    EXPECT_EQ(Util::createTimestamp(posSec86400)->castTemporal(DT_SECOND)->getString(),conn.run("second(timestamp("+to_string(posSec86400)+"))")->getString());

}

TEST_F(ScalarTest,testFunctionCastTemporal_datehour){
    srand(time(NULL));

    ConstantSP datehourval = Util::createDateHour(1);
    EXPECT_ANY_THROW(datehourval->castTemporal(DT_CHAR));
    EXPECT_ANY_THROW(datehourval->castTemporal(DT_STRING));
    EXPECT_EQ(datehourval->castTemporal(DT_DATE)->getString(),conn.run("date(datehour(1))")->getString());
    EXPECT_EQ(datehourval->castTemporal(DT_MONTH)->getString(),conn.run("month(datehour(1))")->getString());
    EXPECT_EQ(datehourval->castTemporal(DT_TIME)->getString(),conn.run("time(datehour(1))")->getString());
    EXPECT_EQ(datehourval->castTemporal(DT_MINUTE)->getString(),conn.run("minute(datehour(1))")->getString());
    EXPECT_EQ(datehourval->castTemporal(DT_SECOND)->getString(),conn.run("second(datehour(1))")->getString());
    EXPECT_EQ(datehourval->castTemporal(DT_DATETIME)->getString(),conn.run("datetime(datehour(1))")->getString());
    EXPECT_EQ(datehourval->castTemporal(DT_TIMESTAMP)->getString(),conn.run("timestamp(datehour(1))")->getString());
    EXPECT_EQ(datehourval->castTemporal(DT_NANOTIME)->getString(),conn.run("nanotime(datehour(1))")->getString());
    EXPECT_EQ(datehourval->castTemporal(DT_NANOTIMESTAMP)->getString(),conn.run("nanotimestamp(datehour(1))")->getString());
    EXPECT_EQ(datehourval->castTemporal(DT_DATEHOUR)->getString(),conn.run("datehour(datehour(1))")->getString());

    // cout<<negSec<<endl<<posSec<<endl<<negSec3600<<endl<<posSec3600<<endl;
    EXPECT_EQ(Util::createDateHour(INT_MIN)->castTemporal(DT_NANOTIMESTAMP)->getString(),"");
    EXPECT_EQ(Util::createDateHour(INT_MIN)->castTemporal(DT_DATE)->getString(),conn.run("date(datehour("+to_string(INT_MIN)+"))")->getString());
    EXPECT_EQ(Util::createDateHour(INT_MIN)->castTemporal(DT_MONTH)->getString(),conn.run("month(datehour("+to_string(INT_MIN)+"))")->getString());
    EXPECT_EQ(Util::createDateHour(INT_MIN)->castTemporal(DT_TIME)->getString(),conn.run("time(datehour("+to_string(INT_MIN)+"))")->getString());
    EXPECT_EQ(Util::createDateHour(INT_MIN)->castTemporal(DT_MINUTE)->getString(),conn.run("minute(datehour("+to_string(INT_MIN)+"))")->getString());
    EXPECT_EQ(Util::createDateHour(INT_MIN)->castTemporal(DT_SECOND)->getString(),conn.run("second(datehour("+to_string(INT_MIN)+"))")->getString());
    EXPECT_EQ(Util::createDateHour(INT_MIN)->castTemporal(DT_DATETIME)->getString(),conn.run("datetime(datehour("+to_string(INT_MIN)+"))")->getString());
    EXPECT_EQ(Util::createDateHour(INT_MIN)->castTemporal(DT_TIMESTAMP)->getString(),"");
    EXPECT_EQ(Util::createDateHour(INT_MIN)->castTemporal(DT_NANOTIME)->getString(),"");
    EXPECT_EQ(Util::createDateHour(INT_MIN)->castTemporal(DT_DATEHOUR)->getString(),conn.run("datehour(datehour("+to_string(INT_MIN)+"))")->getString());

}


TEST_F(ScalarTest,testFunctionCastTemporal_date){
    srand(time(NULL));

    ConstantSP dateval = Util::createDate(1);
    EXPECT_ANY_THROW(dateval->castTemporal(DT_CHAR));
    EXPECT_ANY_THROW(dateval->castTemporal(DT_STRING));
    EXPECT_EQ(dateval->castTemporal(DT_DATE)->getString(),conn.run("date(1)")->getString());
    EXPECT_EQ(dateval->castTemporal(DT_MONTH)->getString(),conn.run("month(date(1))")->getString());
    EXPECT_ANY_THROW(dateval->castTemporal(DT_TIME));
    EXPECT_ANY_THROW(dateval->castTemporal(DT_MINUTE));
    EXPECT_ANY_THROW(dateval->castTemporal(DT_SECOND));
    EXPECT_EQ(dateval->castTemporal(DT_DATETIME)->getString(),conn.run("datetime(date(1))")->getString());
    EXPECT_EQ(dateval->castTemporal(DT_TIMESTAMP)->getString(),conn.run("timestamp(date(1))")->getString());
    EXPECT_ANY_THROW(dateval->castTemporal(DT_NANOTIME));
    EXPECT_EQ(dateval->castTemporal(DT_NANOTIMESTAMP)->getString(),conn.run("nanotimestamp(date(1))")->getString());
    EXPECT_EQ(dateval->castTemporal(DT_DATEHOUR)->getString(),conn.run("datehour(date(1))")->getString());

    // cout<<negSec<<endl<<posSec<<endl<<negSec3600<<endl<<posSec3600<<endl;
    EXPECT_EQ(Util::createDate(INT_MIN)->castTemporal(DT_NANOTIMESTAMP)->getString(),"");
    EXPECT_EQ(Util::createDate(INT_MIN)->castTemporal(DT_DATE)->getString(),conn.run("date(date("+to_string(INT_MIN)+"))")->getString());
    EXPECT_EQ(Util::createDate(INT_MIN)->castTemporal(DT_MONTH)->getString(),conn.run("month(date("+to_string(INT_MIN)+"))")->getString());
    EXPECT_EQ(Util::createDate(INT_MIN)->castTemporal(DT_DATETIME)->getString(),conn.run("datetime(date("+to_string(INT_MIN)+"))")->getString());
    EXPECT_EQ(Util::createDate(INT_MIN)->castTemporal(DT_TIMESTAMP)->getString(),"");
    EXPECT_EQ(Util::createDate(INT_MIN)->castTemporal(DT_DATEHOUR)->getString(),conn.run("datehour(date("+to_string(INT_MIN)+"))")->getString());

}

TEST_F(ScalarTest,testFunctionCastTemporal_second){
    srand(time(NULL));

    ConstantSP secondval = Util::createSecond(1);
    EXPECT_ANY_THROW(secondval->castTemporal(DT_CHAR));
    EXPECT_ANY_THROW(secondval->castTemporal(DT_STRING));
    EXPECT_ANY_THROW(secondval->castTemporal(DT_DATE));
    EXPECT_ANY_THROW(secondval->castTemporal(DT_MONTH));
    EXPECT_EQ(secondval->castTemporal(DT_TIME)->getString(),conn.run("time(second(1))")->getString());
    EXPECT_EQ(secondval->castTemporal(DT_MINUTE)->getString(),conn.run("minute(second(1))")->getString());
    EXPECT_EQ(secondval->castTemporal(DT_SECOND)->getString(),conn.run("second(second(1))")->getString());
    EXPECT_ANY_THROW(secondval->castTemporal(DT_DATETIME));
    EXPECT_ANY_THROW(secondval->castTemporal(DT_TIMESTAMP));
    EXPECT_EQ(secondval->castTemporal(DT_NANOTIME)->getString(),conn.run("nanotime(second(1))")->getString());
    EXPECT_ANY_THROW(secondval->castTemporal(DT_NANOTIMESTAMP));
    EXPECT_ANY_THROW(secondval->castTemporal(DT_DATEHOUR)); 

    // cout<<negSec<<endl<<posSec<<endl<<negSec3600<<endl<<posSec3600<<endl;
    EXPECT_EQ(Util::createSecond(INT_MIN)->castTemporal(DT_TIME)->getString(),conn.run("time(second("+to_string(INT_MIN)+"))")->getString());
    EXPECT_EQ(Util::createSecond(INT_MIN)->castTemporal(DT_MINUTE)->getString(),conn.run("minute(second("+to_string(INT_MIN)+"))")->getString());
    EXPECT_EQ(Util::createSecond(INT_MIN)->castTemporal(DT_SECOND)->getString(),conn.run("second(second("+to_string(INT_MIN)+"))")->getString());
    EXPECT_EQ(Util::createSecond(INT_MIN)->castTemporal(DT_NANOTIME)->getString(),"");

}


TEST_F(ScalarTest,testFunctionCastTemporal_minute){
    srand(time(NULL));

    ConstantSP minuteval = Util::createMinute(1);
    EXPECT_ANY_THROW(minuteval->castTemporal(DT_CHAR));
    EXPECT_ANY_THROW(minuteval->castTemporal(DT_STRING));
    EXPECT_ANY_THROW(minuteval->castTemporal(DT_DATE));
    EXPECT_ANY_THROW(minuteval->castTemporal(DT_MONTH));
    EXPECT_EQ(minuteval->castTemporal(DT_TIME)->getString(),conn.run("time(minute(1))")->getString());
    EXPECT_EQ(minuteval->castTemporal(DT_MINUTE)->getString(),conn.run("minute(minute(1))")->getString());
    EXPECT_EQ(minuteval->castTemporal(DT_SECOND)->getString(),conn.run("second(minute(1))")->getString());
    EXPECT_ANY_THROW(minuteval->castTemporal(DT_DATETIME));
    EXPECT_ANY_THROW(minuteval->castTemporal(DT_TIMESTAMP));
    EXPECT_EQ(minuteval->castTemporal(DT_NANOTIME)->getString(),conn.run("nanotime(minute(1))")->getString());
    EXPECT_ANY_THROW(minuteval->castTemporal(DT_NANOTIMESTAMP));
    EXPECT_ANY_THROW(minuteval->castTemporal(DT_DATEHOUR));  

    // cout<<negSec<<endl<<posSec<<endl<<negSec3600<<endl<<posSec3600<<endl;
    EXPECT_EQ(Util::createMinute(INT_MIN)->castTemporal(DT_TIME)->getString(),conn.run("time(minute("+to_string(INT_MIN)+"))")->getString());
    EXPECT_EQ(Util::createMinute(INT_MIN)->castTemporal(DT_MINUTE)->getString(),conn.run("minute(minute("+to_string(INT_MIN)+"))")->getString());
    EXPECT_EQ(Util::createMinute(INT_MIN)->castTemporal(DT_SECOND)->getString(),conn.run("second(minute("+to_string(INT_MIN)+"))")->getString());
    EXPECT_EQ(Util::createMinute(INT_MIN)->castTemporal(DT_NANOTIME)->getString(),"");

}


TEST_F(ScalarTest,testFunctionCastTemporal_time){
    srand(time(NULL));

    ConstantSP timeval = Util::createTime(1);
    EXPECT_ANY_THROW(timeval->castTemporal(DT_CHAR));
    EXPECT_ANY_THROW(timeval->castTemporal(DT_STRING));
    EXPECT_ANY_THROW(timeval->castTemporal(DT_DATE));
    EXPECT_ANY_THROW(timeval->castTemporal(DT_MONTH));
    EXPECT_EQ(timeval->castTemporal(DT_TIME)->getString(),conn.run("time(time(1))")->getString());
    EXPECT_EQ(timeval->castTemporal(DT_MINUTE)->getString(),conn.run("minute(time(1))")->getString());
    EXPECT_EQ(timeval->castTemporal(DT_SECOND)->getString(),conn.run("second(time(1))")->getString());
    EXPECT_ANY_THROW(timeval->castTemporal(DT_DATETIME));
    EXPECT_ANY_THROW(timeval->castTemporal(DT_TIMESTAMP));
    EXPECT_EQ(timeval->castTemporal(DT_NANOTIME)->getString(),conn.run("nanotime(time(1))")->getString());
    EXPECT_ANY_THROW(timeval->castTemporal(DT_NANOTIMESTAMP));
    EXPECT_ANY_THROW(timeval->castTemporal(DT_DATEHOUR));

    // cout<<negSec<<endl<<posSec<<endl<<negSec3600<<endl<<posSec3600<<endl;
    EXPECT_EQ(Util::createTime(INT_MIN)->castTemporal(DT_TIME)->getString(),conn.run("time(time("+to_string(INT_MIN)+"))")->getString());
    EXPECT_EQ(Util::createTime(INT_MIN)->castTemporal(DT_MINUTE)->getString(),conn.run("minute(time("+to_string(INT_MIN)+"))")->getString());
    EXPECT_EQ(Util::createTime(INT_MIN)->castTemporal(DT_SECOND)->getString(),conn.run("second(time("+to_string(INT_MIN)+"))")->getString());
    EXPECT_EQ(Util::createTime(INT_MIN)->castTemporal(DT_NANOTIME)->getString(),"");

}

TEST_F(ScalarTest,testFunctionCastTemporal_nanotime){
    srand(time(NULL));

    ConstantSP nanotimeval = Util::createNanoTime(1000000);
    EXPECT_ANY_THROW(nanotimeval->castTemporal(DT_CHAR));
    EXPECT_ANY_THROW(nanotimeval->castTemporal(DT_STRING));
    EXPECT_ANY_THROW(nanotimeval->castTemporal(DT_DATE));
    EXPECT_ANY_THROW(nanotimeval->castTemporal(DT_MONTH));
    EXPECT_EQ(nanotimeval->castTemporal(DT_TIME)->getString(),conn.run("time(nanotime(1000000))")->getString());
    EXPECT_EQ(nanotimeval->castTemporal(DT_MINUTE)->getString(),conn.run("minute(nanotime(1000000))")->getString());
    EXPECT_EQ(nanotimeval->castTemporal(DT_SECOND)->getString(),conn.run("second(nanotime(1000000))")->getString());
    EXPECT_ANY_THROW(nanotimeval->castTemporal(DT_DATETIME));
    EXPECT_ANY_THROW(nanotimeval->castTemporal(DT_TIMESTAMP));
    EXPECT_EQ(nanotimeval->castTemporal(DT_NANOTIME)->getString(),conn.run("nanotime(nanotime(1000000))")->getString());
    EXPECT_ANY_THROW(nanotimeval->castTemporal(DT_NANOTIMESTAMP));
    EXPECT_ANY_THROW(nanotimeval->castTemporal(DT_DATEHOUR));


    // cout<<negSec<<endl<<posSec<<endl<<negSec3600<<endl<<posSec3600<<endl;
    EXPECT_EQ(Util::createNanoTime(LLONG_MIN)->castTemporal(DT_TIME)->getString(),conn.run("time(nanotime("+to_string(LLONG_MIN)+"))")->getString());
    EXPECT_EQ(Util::createNanoTime(LLONG_MIN)->castTemporal(DT_MINUTE)->getString(),conn.run("minute(nanotime("+to_string(LLONG_MIN)+"))")->getString());
    EXPECT_EQ(Util::createNanoTime(LLONG_MIN)->castTemporal(DT_SECOND)->getString(),conn.run("second(nanotime("+to_string(LLONG_MIN)+"))")->getString());
    EXPECT_EQ(Util::createNanoTime(LLONG_MIN)->castTemporal(DT_NANOTIME)->getString(),"");

}

TEST_F(ScalarTest,testFunction_parseChar){
    ConstantSP charVal;
    charVal = Util::parseConstant(DT_CHAR, "00");
    EXPECT_TRUE(charVal->isNull());

    charVal = Util::parseConstant(DT_CHAR, "");
    EXPECT_TRUE(charVal->isNull());

    charVal = Util::parseConstant(DT_CHAR, "'\\30'");
    EXPECT_TRUE(charVal->isNull());

    charVal = Util::parseConstant(DT_CHAR, "'/3'");
    EXPECT_TRUE(charVal->isNull());

    charVal = Util::parseConstant(DT_CHAR, "'\\3\\");
    EXPECT_TRUE(charVal->isNull());

    charVal = Util::parseConstant(DT_CHAR, "-255");
    EXPECT_TRUE(charVal.isNull()) << "need to change nullptr to isNull()";

    charVal = Util::parseConstant(DT_CHAR, "'5\"");
    EXPECT_TRUE(charVal->isNull());

    charVal = Util::parseConstant(DT_CHAR, "255");
    EXPECT_TRUE(charVal.isNull()) << "need to change nullptr to isNull()";

    vector<string> charactors = {"'\\0'", "'\\b'", "'\\5'", "'\\n'", "'\\r'", "'\\t'", "'\\v'", "'\\\\'", "'\\''", "'\\\"'", "'a'", "'0'", "'3'", "'\t'", "'\n'", "'\r'", "13", "10"};
    vector<char> ex_chars = {'0', 'b', '5', '\n', '\r', '\t', 'v', '\\', '\'', '\"', 'a', '0', '3', '\t', '\n', '\r', '\r', '\n'};
    for(auto i=0; i< charactors.size();i++)
        // cout<<Util::parseConstant(DT_CHAR, charactors[i])->getChar()<<endl;
        EXPECT_EQ(Util::parseConstant(DT_CHAR, charactors[i])->getChar(),ex_chars[i]);

}

TEST_F(ScalarTest,testFunction_parseBool){
    ConstantSP Val;
    Val = Util::parseConstant(DT_BOOL, "00");
    EXPECT_TRUE(Val->isNull());

    Val = Util::parseConstant(DT_BOOL, "1245");
    EXPECT_TRUE(Val->getBool());

    Val = Util::parseConstant(DT_BOOL, "-12055");
    EXPECT_TRUE(Val->getBool());

    ConstantSP Val1 = conn.run("bool(1)");
    ConstantSP Val2 = Util::parseConstant(DT_BOOL, Val1->getString());
    ConstantSP Val3 = Util::parseConstant(DT_BOOL, "true");
    EXPECT_EQ(Val1->getBool(), Val2->getBool());

    Val1 = conn.run("bool(0)");
    Val2 = Util::parseConstant(DT_BOOL, Val1->getString());
    Val3 = Util::parseConstant(DT_BOOL, "false");
    EXPECT_EQ(Val1->getBool(), Val2->getBool());

}

TEST_F(ScalarTest,testFunction_parseShort){
    ConstantSP shortVal;
    shortVal = Util::parseConstant(DT_SHORT, "00");
    EXPECT_TRUE(shortVal->isNull());

    ConstantSP shortVal1 = conn.run("short(51234)");
    ConstantSP shortVal2 = Util::parseConstant(DT_SHORT, shortVal1->getString());
    ConstantSP shortVal3 = Util::parseConstant(DT_SHORT, "51234");
    EXPECT_EQ(shortVal1->getShort(), shortVal2->getShort());

}

TEST_F(ScalarTest,testFunction_parseInt){
    ConstantSP val;
    val = Util::parseConstant(DT_INT, "00");
    EXPECT_TRUE(val->isNull());

    ConstantSP val1 = conn.run("int(51234)");
    ConstantSP val2 = Util::parseConstant(DT_INT, val1->getString());
    ConstantSP val3 = Util::parseConstant(DT_INT, "51234");
    EXPECT_EQ(val1->getInt(), val2->getInt());

}

TEST_F(ScalarTest,testFunction_parseLong){
    ConstantSP val;
    val = Util::parseConstant(DT_LONG, "00");
    EXPECT_TRUE(val->isNull());

    ConstantSP val1 = conn.run("long(51234)");
    ConstantSP val2 = Util::parseConstant(DT_LONG, val1->getString());
    ConstantSP val3 = Util::parseConstant(DT_LONG, "51234");
    EXPECT_EQ(val1->getLong(), val2->getLong());

}

TEST_F(ScalarTest,testFunction_parseFloat){
    ConstantSP val;
    val = Util::parseConstant(DT_FLOAT, "00");
    EXPECT_TRUE(val->isNull());

    ConstantSP ex_val = conn.run("float(3.2314)");
    val = Util::parseConstant(DT_FLOAT, "3.2314");
    EXPECT_EQ(ex_val->getFloat(), val->getFloat());

}

TEST_F(ScalarTest,testFunction_parseDouble){
    ConstantSP val;
    val = Util::parseConstant(DT_DOUBLE, "00");
    EXPECT_TRUE(val->isNull());

    ConstantSP ex_val = conn.run("double(3.2314)");
    val = Util::parseConstant(DT_DOUBLE, "3.2314");
    EXPECT_EQ(ex_val->getDouble(), val->getDouble());

}

TEST_F(ScalarTest,testFunction_parseNanoTime){
    ConstantSP nanots;
    nanots = Util::parseConstant(DT_NANOTIME, "00");
    EXPECT_TRUE(nanots->isNull());

    nanots = Util::parseConstant(DT_NANOTIME, "25:00:01.443197923");
    EXPECT_TRUE(nanots.isNull()) << "need to change to isNull()";

    nanots = Util::parseConstant(DT_NANOTIME, "13-00:01.443197923");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_NANOTIME, "00:88:01.443197923");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_NANOTIME, "00:00-01.443197923");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_NANOTIME, "00:00:88.443197923");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_NANOTIME, "00:00:01-443197923");
    EXPECT_EQ(nanots->getString(), "00:00:01.000000000");

    nanots = Util::parseConstant(DT_NANOTIME, "00:00:30.443197923212");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_NANOTIME, "00:00:33.443197");
    EXPECT_EQ(nanots->getString(), "00:00:33.443197000");

    nanots = Util::parseConstant(DT_NANOTIME, "00:00:33.443");
    EXPECT_EQ(nanots->getString(), "00:00:33.443000000");


    ConstantSP nanots1 = conn.run("nanotime(2738549)");
    ConstantSP nanots2 = Util::parseConstant(DT_NANOTIME, nanots1->getString());
    ConstantSP nanots3 = Util::parseConstant(DT_NANOTIME, "00:00:00.002738549");
    EXPECT_EQ(nanots1->getString(), nanots2->getString());
}


TEST_F(ScalarTest,testFunction_parseNanoTimestamp){
    ConstantSP nanots;
    nanots = Util::parseConstant(DT_NANOTIMESTAMP, "00");
    EXPECT_TRUE(nanots->isNull());

    nanots = Util::parseConstant(DT_NANOTIMESTAMP, "0000.01.01T00:00:01.443197923");
    EXPECT_TRUE(nanots.isNull()) << "need to change nullptr to isNull()";

    nanots = Util::parseConstant(DT_NANOTIMESTAMP, "2020-01.01T00:00:01.443197923");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_NANOTIMESTAMP, "2022.00.01T00:00:01.443197923");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_NANOTIMESTAMP, "2022.01-01T00:00:01.443197923");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_NANOTIMESTAMP, "2022.01.00T00:00:01.443197923");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_NANOTIMESTAMP, "2022.01.01-00:00:01.443197923");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_NANOTIMESTAMP, "2022.01.20 25:00:01.443197923");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_NANOTIMESTAMP, "2022.01.20 13-00:01.443197923");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_NANOTIMESTAMP, "2022.01.01-00:00:01.443197923");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_NANOTIMESTAMP, "2022.01.01 00:88:01.443197923");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_NANOTIMESTAMP, "2022.01.01 00:00-01.443197923");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_NANOTIMESTAMP, "2022.01.01 00:00:88.443197923");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_NANOTIMESTAMP, "2022.01.01 00:00:01-443197923");
    EXPECT_EQ(nanots->getString(), "2022.01.01T00:00:01.000000000");

    nanots = Util::parseConstant(DT_NANOTIMESTAMP, "2022.01.01 00:00:30.443197923212");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_NANOTIMESTAMP, "2022.01.01 00:00:33.443197");
    EXPECT_EQ(nanots->getString(), "2022.01.01T00:00:33.443197000");

    nanots = Util::parseConstant(DT_NANOTIMESTAMP, "2022.01.01 00:00:33.443");
    EXPECT_EQ(nanots->getString(), "2022.01.01T00:00:33.443000000");


    ConstantSP nanots1 = conn.run("nanotimestamp(2738549)");
    ConstantSP nanots2 = Util::parseConstant(DT_NANOTIMESTAMP, nanots1->getString());
    ConstantSP nanots3 = Util::parseConstant(DT_NANOTIMESTAMP, "1970.01.01 00:00:00.002738549");
    EXPECT_EQ(nanots1->getString(), nanots2->getString());
}


TEST_F(ScalarTest,testFunction_parseTimestamp){
    ConstantSP ts;
    ts = Util::parseConstant(DT_TIMESTAMP, "00");
    EXPECT_TRUE(ts->isNull());

    ts = Util::parseConstant(DT_TIMESTAMP, "0000.01.01T00:00:01.443");
    EXPECT_TRUE(ts.isNull()) << "need to change nullptr to isNull()";

    ts = Util::parseConstant(DT_TIMESTAMP, "2020-01.01T00:00:01.443");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_TIMESTAMP, "2022.00.01T00:00:01.443");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_TIMESTAMP, "2022.01-01T00:00:01.443");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_TIMESTAMP, "2022.01.00T00:00:01.443");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_TIMESTAMP, "2022.01.01-00:00:01.443");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_TIMESTAMP, "2022.01.20 25:00:01.443");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_TIMESTAMP, "2022.01.20 13-00:01.443");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_TIMESTAMP, "2022.01.01-00:00:01.443");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_TIMESTAMP, "2022.01.01 00:88:01.443");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_TIMESTAMP, "2022.01.01 00:00-01.443");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_TIMESTAMP, "2022.01.01 00:00:88.443");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_TIMESTAMP, "2022.01.01 00:00:01-443");
    EXPECT_EQ(ts->getString(), "2022.01.01T00:00:01.000");

    ts = Util::parseConstant(DT_TIMESTAMP, "2022.01.01 00:00:30.443212");
    EXPECT_EQ(ts->getString(), "2022.01.01T00:00:30.443");


    ConstantSP ts1 = conn.run("timestamp(2738549)");
    ConstantSP ts2 = Util::parseConstant(DT_TIMESTAMP, ts1->getString());
    ConstantSP ts3 = Util::parseConstant(DT_TIMESTAMP, "1970.01.01T00:45:38.549");
    EXPECT_EQ(ts1->getString(), ts2->getString());
}


TEST_F(ScalarTest,testFunction_parseTime){
    ConstantSP nanots;
    nanots = Util::parseConstant(DT_TIME, "00");
    EXPECT_TRUE(nanots->isNull());

    nanots = Util::parseConstant(DT_TIME, "25:00:01.443");
    EXPECT_TRUE(nanots.isNull()) << "need to change to isNull()";

    nanots = Util::parseConstant(DT_TIME, "13-00:01.443");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_TIME, "00:88:01.443");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_TIME, "00:00-01.443");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_TIME, "00:00:88.443");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_TIME, "00:00:01-443");
    EXPECT_EQ(nanots->getString(), "00:00:01.000");

    nanots = Util::parseConstant(DT_TIME, "00:00:30.443212");
    EXPECT_TRUE(nanots.isNull());

    ConstantSP ts1 = conn.run("time(2738549)");
    ConstantSP ts2 = Util::parseConstant(DT_TIME, ts1->getString());
    ConstantSP ts3 = Util::parseConstant(DT_TIME, "00:45:38.549");
    EXPECT_EQ(ts1->getString(), ts2->getString());
}

TEST_F(ScalarTest,testFunction_parseSecond){
    ConstantSP nanots;
    nanots = Util::parseConstant(DT_SECOND, "00");
    EXPECT_TRUE(nanots->isNull());

    nanots = Util::parseConstant(DT_SECOND, "25:00:01");
    EXPECT_TRUE(nanots.isNull()) << "need to change to isNull()";

    nanots = Util::parseConstant(DT_SECOND, "13-00:01");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_SECOND, "00:88:01");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_SECOND, "00:00-01");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_SECOND, "00:00:88");
    EXPECT_TRUE(nanots.isNull());

    ConstantSP ts1 = conn.run("second(3663)");
    ConstantSP ts2 = Util::parseConstant(DT_SECOND, ts1->getString());
    ConstantSP ts3 = Util::parseConstant(DT_SECOND, "01:01:03");
    EXPECT_EQ(ts1->getString(), ts2->getString());
}


TEST_F(ScalarTest,testFunction_parseMinute){
    ConstantSP nanots;
    nanots = Util::parseConstant(DT_MINUTE, "00");
    EXPECT_TRUE(nanots->isNull());

    nanots = Util::parseConstant(DT_MINUTE, "88:01");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_MINUTE, "00-01");
    EXPECT_TRUE(nanots.isNull());

    nanots = Util::parseConstant(DT_MINUTE, "00:88");
    EXPECT_TRUE(nanots.isNull());

    ConstantSP ts1 = conn.run("minute(534)");
    ConstantSP ts3 = Util::parseConstant(DT_MINUTE, "08:54");
    EXPECT_EQ(ts1->getString(), ts3->getString());
}


TEST_F(ScalarTest,testFunction_parseDate){
    ConstantSP ts;
    ts = Util::parseConstant(DT_DATE, "00");
    EXPECT_TRUE(ts->isNull());

    ts = Util::parseConstant(DT_DATE, "2022.01");
    EXPECT_TRUE(ts.isNull()) << "need to change nullptr to isNull()";

    ts = Util::parseConstant(DT_DATE, "0000.01.01");
    EXPECT_TRUE(ts.isNull()) << "need to change nullptr to isNull()";

    ts = Util::parseConstant(DT_DATE, "2020-01.01");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_DATE, "2022.00.01");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_DATE, "2022.01-01");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_DATE, "2022.01.00");
    EXPECT_TRUE(ts.isNull());

    ConstantSP ts1 = conn.run("date(2738549)");
    ConstantSP ts2 = Util::parseConstant(DT_DATE, ts1->getString());
    ConstantSP ts3 = Util::parseConstant(DT_DATE, "9467.11.23");
    EXPECT_EQ(ts1->getString(), ts2->getString());
}


TEST_F(ScalarTest,testFunction_parseMonth){
    ConstantSP ts;
    ts = Util::parseConstant(DT_MONTH, "00");
    EXPECT_TRUE(ts->isNull());

    ts = Util::parseConstant(DT_MONTH, "2022");
    EXPECT_TRUE(ts.isNull()) << "need to change nullptr to isNull()";

    ts = Util::parseConstant(DT_MONTH, "0000.01");
    EXPECT_TRUE(ts.isNull()) << "need to change nullptr to isNull()";

    ts = Util::parseConstant(DT_MONTH, "2020-01");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_MONTH, "2022.00");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_MONTH, "2022.15");
    EXPECT_TRUE(ts.isNull());

    ConstantSP ts1 = conn.run("month(2783)");
    ConstantSP ts3 = Util::parseConstant(DT_MONTH, "0231.12");
    EXPECT_EQ(ts1->getString(), ts3->getString());
}


TEST_F(ScalarTest,testFunction_parseDatetime){
    ConstantSP ts;
    ts = Util::parseConstant(DT_DATETIME, "00");
    EXPECT_TRUE(ts->isNull());

    ts = Util::parseConstant(DT_DATETIME, "0000.01.01T00:00:01");
    EXPECT_TRUE(ts.isNull()) << "need to change nullptr to isNull()";

    ts = Util::parseConstant(DT_DATETIME, "2020-01.01T00:00:01");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_DATETIME, "2022.00.01T00:00:01");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_DATETIME, "2022.01-01T00:00:01");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_DATETIME, "2022.01.00T00:00:01");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_DATETIME, "2022.01.01-00:00:01");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_DATETIME, "2022.01.20 25:00:01");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_DATETIME, "2022.01.20 13-00:01");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_DATETIME, "2022.01.01-00:00:01");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_DATETIME, "2022.01.01 00:88:01");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_DATETIME, "2022.01.01 00:00-01");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_DATETIME, "2022.01.01 00:00:88");
    EXPECT_TRUE(ts.isNull());

    ConstantSP ts1 = conn.run("datetime(2738549)");
    ConstantSP ts2 = Util::parseConstant(DT_DATETIME, ts1->getString());
    ConstantSP ts3 = Util::parseConstant(DT_DATETIME, "1970.02.01T16:42:29");
    EXPECT_EQ(ts1->getString(), ts2->getString());
}


TEST_F(ScalarTest,testFunction_parseDatehour){
    ConstantSP ts;
    ts = Util::parseConstant(DT_DATEHOUR, "00");
    EXPECT_TRUE(ts->isNull());

    ts = Util::parseConstant(DT_DATEHOUR, "2022.01");
    EXPECT_TRUE(ts.isNull()) << "need to change nullptr to isNull()";

    ts = Util::parseConstant(DT_DATEHOUR, "0000.01.01T00");
    EXPECT_TRUE(ts.isNull()) << "need to change nullptr to isNull()";

    ts = Util::parseConstant(DT_DATEHOUR, "2020-01.01T00");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_DATEHOUR, "2022.00.01T00");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_DATEHOUR, "2022.01-01T00");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_DATEHOUR, "2022.01.00T00");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_DATEHOUR, "2022.01.01-00");
    EXPECT_TRUE(ts.isNull());

    ts = Util::parseConstant(DT_DATEHOUR, "2022.01.20 25");
    EXPECT_TRUE(ts.isNull());

    ConstantSP ts1 = conn.run("datehour(2738549)");
    ConstantSP ts2 = Util::parseConstant(DT_DATEHOUR, ts1->getString());
    ConstantSP ts3 = Util::parseConstant(DT_DATEHOUR, "2282.05.31T05");
    EXPECT_EQ(ts1->getString(), ts2->getString());
}

TEST_F(ScalarTest,testFunction_parseIP4){
    ConstantSP ipv4Val;
    ipv4Val = Util::parseConstant(DT_IP, "192.168.12F.12");
    EXPECT_TRUE(ipv4Val.isNull());

    ipv4Val = Util::parseConstant(DT_IP, ".168.12.12");
    EXPECT_EQ(ipv4Val->getString(), "0.168.12.12");


    ipv4Val = Util::parseConstant(DT_IP, "192.168.111.12");
    ConstantSP ipv4Val1 = Util::parseConstant(DT_IP, "255.255..");
    ConstantSP ipv4Val2 = Util::parseConstant(DT_IP, "124.013.22.1");
    ConstantSP ipv4Val3 = Util::parseConstant(DT_IP, "0.0.000.55"); 
    ConstantSP ipv4Val4 = Util::parseConstant(DT_IP, "0.0.0.0");

    // cout<<ipv4Val->getString()<<endl<<ipv4Val1->getString()<<endl<<ipv4Val2->getString()<<endl<<ipv4Val3->getString()<<endl<<ipv4Val4->getString()<<endl;
    EXPECT_EQ(ipv4Val->getString(), "192.168.111.12");
    EXPECT_EQ(ipv4Val1->getString(), "255.255.0.0");
    EXPECT_EQ(ipv4Val2->getString(), "124.13.22.1");
    EXPECT_EQ(ipv4Val3->getString(), "0.0.0.55");
    EXPECT_EQ(ipv4Val4->getString(), "0.0.0.0");
}

TEST_F(ScalarTest,testFunction_parseIP6){
    ConstantSP ipv6Val;
    ipv6Val = Util::parseConstant(DT_IP, "2001:3CA1:010F:001A:121B:0000:2C3B:0010");
    ConstantSP ipv6Val1 = Util::parseConstant(DT_IP, "2001:3CA1:010F:001A:121B:0000:3100:0");
    ConstantSP ipv6Val2 = Util::parseConstant(DT_IP, ":3CA1:10F:001A:121B:::10");
    ConstantSP ipv6Val3 = Util::parseConstant(DT_IP, "2001:3CA1:010F:1A:121B:00::0010"); 
    ConstantSP ipv6Val4 = Util::parseConstant(DT_IP, "0:0:0:0:0:0:0:0");

    // cout<<ipv6Val->getString()<<endl<<ipv6Val1->getString()<<endl<<ipv6Val2->getString()<<endl<<ipv6Val3->getString()<<endl<<ipv6Val4->getString()<<endl;
    EXPECT_EQ(ipv6Val->getString(), "2001:3ca1:10f:1a:121b:0:2c3b:10");
    EXPECT_EQ(ipv6Val1->getString(), "2001:3ca1:10f:1a:121b:0:3100:0");
    EXPECT_EQ(ipv6Val2->getString(), "0:3ca1:10f:1a:121b::10");
    EXPECT_EQ(ipv6Val3->getString(), "2001:3ca1:10f:1a:121b::10");
    EXPECT_EQ(ipv6Val4->getString(), "0.0.0.0");
}

TEST_F(ScalarTest,testFunction_parseInt128){
    ConstantSP val;
    val = Util::parseConstant(DT_INT128, "");
    EXPECT_EQ(val->getString(), "00000000000000000000000000000000");

    val = Util::parseConstant(DT_INT128, "e1671797c52e15f763380b45e841ec32");
    EXPECT_EQ(val->getString(), "e1671797c52e15f763380b45e841ec32");

}
