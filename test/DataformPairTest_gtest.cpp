class DataformPairTest:public testing::Test
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


TEST_F(DataformPairTest,testAnyPair){
	VectorSP v1 = Util::createPair(DT_ANY);
	EXPECT_EQ(v1.isNull(),true);
}


TEST_F(DataformPairTest,testStringPair){
	VectorSP v1 = Util::createPair(DT_STRING);
    v1->set(0, Util::createString("123abc"));
    v1->set(1, Util::createString("中文*……%#￥#！a"));
	string script = "a=pair(`123abc, '中文*……%#￥#！a');a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testStringNullPair){
	VectorSP v1 = Util::createPair(DT_STRING);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=pair(string(NULL), string(NULL));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testBoolPair){
	VectorSP v1 = Util::createPair(DT_BOOL);
    v1->set(0, Util::createBool(1));
    v1->set(1, Util::createBool(0));
	string script = "a=pair(true, false);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testBoolNullPair){
	VectorSP v1 = Util::createPair(DT_BOOL);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=pair(bool(NULL), bool(NULL));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testCharPair){
	VectorSP v1 = Util::createPair(DT_CHAR);
    v1->set(0, Util::createChar(1));
    v1->set(1, Util::createChar(0));
	string script = "a=pair(char(1), char(0));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testCharNullPair){
	VectorSP v1 = Util::createPair(DT_CHAR);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=pair(char(NULL), char(NULL));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testIntPair){
	VectorSP v1 = Util::createPair(DT_INT);
    v1->set(0, Util::createInt(1));
    v1->set(1, Util::createInt(0));
	string script = "a=pair(int(1), int(0));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testIntNullPair){
	VectorSP v1 = Util::createPair(DT_INT);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=pair(int(NULL), int(NULL));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testLongPair){
	VectorSP v1 = Util::createPair(DT_LONG);
    v1->set(0, Util::createLong(1));
    v1->set(1, Util::createLong(0));
	string script = "a=pair(long(1), long(0));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testLongNullPair){
	VectorSP v1 = Util::createPair(DT_LONG);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=pair(long(NULL), long(NULL));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testShortNullPair){
	VectorSP v1 = Util::createPair(DT_SHORT);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=pair(short(NULL), short(NULL));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testShortPair){
	VectorSP v1 = Util::createPair(DT_SHORT);
    v1->set(0, Util::createShort(1));
    v1->set(1, Util::createShort(0));
	string script = "a=pair(short(1), short(0));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testFloatPair){
	VectorSP v1 = Util::createPair(DT_FLOAT);
    v1->set(0, Util::createFloat(1));
    v1->set(1, Util::createFloat(0));
	string script = "a=pair(float(1), float(0));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isPair(),true);
}


TEST_F(DataformPairTest,testFloatNullPair){
	VectorSP v1 = Util::createPair(DT_FLOAT);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=pair(float(NULL), float(NULL));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testDoublePair){
	VectorSP v1 = Util::createPair(DT_DOUBLE);
    v1->set(0, Util::createDouble(1));
    v1->set(1, Util::createDouble(0));
	string script = "a=pair(double(1), double(0));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isPair(),true);
}


TEST_F(DataformPairTest,testDoubleNullPair){
	VectorSP v1 = Util::createPair(DT_DOUBLE);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=pair(double(NULL), double(NULL));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testDatehourPair){
	VectorSP v1 = Util::createPair(DT_DATEHOUR);
    v1->set(0, Util::createDateHour(1));
    v1->set(1, Util::createDateHour(100000));
	string script = "a=pair(datehour(1), datehour(100000));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testDatehourNullPair){
	VectorSP v1 = Util::createPair(DT_DATEHOUR);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=pair(datehour(NULL), datehour(NULL));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testDatePair){
	VectorSP v1 = Util::createPair(DT_DATE);
    v1->set(0, Util::createDate(1));
    v1->set(1, Util::createDate(48750));
	string script = "a=pair(date(1), date(48750));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testDatenullPair){
	VectorSP v1 = Util::createPair(DT_DATE);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=pair(date(NULL), date(NULL));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testMinutePair){
	VectorSP v1 = Util::createPair(DT_MINUTE);
    v1->set(0, Util::createMinute(1));
    v1->set(1, Util::createMinute(1000));
	string script = "a=pair(minute(1), minute(1000));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testMinutenullPair){
	VectorSP v1 = Util::createPair(DT_MINUTE);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=pair(minute(NULL), minute(NULL));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());

    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testDatetimePair){
	VectorSP v1 = Util::createPair(DT_DATETIME);
    v1->set(0, Util::createDateTime(1));
    v1->set(1, Util::createDateTime(48750));
	string script = "a=pair(datetime(1), datetime(48750));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testDatetimenullPair){
	VectorSP v1 = Util::createPair(DT_DATETIME);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=pair(datetime(NULL), datetime(NULL));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testTimeStampPair){
	VectorSP v1 = Util::createPair(DT_TIMESTAMP);
    v1->set(0, Util::createTimestamp(1));
    v1->set(1, Util::createTimestamp((long long)1000000000000));
	string script = "a=pair(timestamp(1), timestamp(1000000000000));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testTimeStampnullPair){
	VectorSP v1 = Util::createPair(DT_TIMESTAMP);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=pair(timestamp(NULL), timestamp(NULL));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testnanotimePair){
	VectorSP v1 = Util::createPair(DT_NANOTIME);
    v1->set(0, Util::createNanoTime(1));
    v1->set(1, Util::createNanoTime((long long)1000000000000));
	string script = "a=pair(nanotime(1), nanotime(1000000000000));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testnanotimenullPair){
	VectorSP v1 = Util::createPair(DT_NANOTIME);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=pair(nanotime(NULL), nanotime(NULL));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testnanotimestampPair){
	VectorSP v1 = Util::createPair(DT_NANOTIMESTAMP);
    v1->set(0, Util::createNanoTimestamp(1));
    v1->set(1, Util::createNanoTimestamp((long long)1000000000000));
	string script = "a=pair(nanotimestamp(1), nanotimestamp(1000000000000));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testnanotimestampnullPair){
	VectorSP v1 = Util::createPair(DT_NANOTIMESTAMP);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=pair(nanotimestamp(NULL), nanotimestamp(NULL));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testmonthPair){
	VectorSP v1 = Util::createPair(DT_MONTH);
    v1->set(0, Util::createMonth(1));
    v1->set(1, Util::createMonth(1000));
	string script = "a=pair(month(1), month(1000));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testmonthnullPair){
	VectorSP v1 = Util::createPair(DT_MONTH);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=pair(month(NULL), month(NULL));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testtimePair){
	VectorSP v1 = Util::createPair(DT_TIME);
    v1->set(0, Util::createTime(1));
    v1->set(1, Util::createTime((long)10000000));
	string script = "a=pair(time(1), time(10000000));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testtimenullPair){
	VectorSP v1 = Util::createPair(DT_TIME);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=pair(time(NULL), time(NULL));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testDecimal32Pair){
	srand((int)time(NULL));
	int scale = rand()%9;
	VectorSP v1 = Util::createPair(DT_DECIMAL32, scale);
    v1->set(0, Util::createDecimal32(scale,2.33));
    v1->set(1, Util::createDecimal32(scale,3.502));
	string script = "a=pair(decimal32(2.33,"+to_string(scale)+"),decimal32(3.502,"+to_string(scale)+"));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testDecimal32NullPair){
	srand((int)time(NULL));
	int scale = rand()%9;
	VectorSP v1 = Util::createPair(DT_DECIMAL32, scale);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=pair(decimal32(NULL,"+to_string(scale)+"),decimal32(NULL,"+to_string(scale)+"));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testDecimal64Pair){
	srand((int)time(NULL));
	int scale = rand()%18;
	VectorSP v1 = Util::createPair(DT_DECIMAL64, scale);
    v1->set(0, Util::createDecimal64(scale,2.33));
    v1->set(1, Util::createDecimal64(scale,3.502));
	string script = "a=pair(decimal64(2.33,"+to_string(scale)+"),decimal64(3.502,"+to_string(scale)+"));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	cout<<conn.run("v1")->getString()<<endl;
	cout<<conn.run("a")->getString()<<endl;
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}

TEST_F(DataformPairTest,testDecimal64NullPair){
	srand((int)time(NULL));
	int scale = rand()%9;
	VectorSP v1 = Util::createPair(DT_DECIMAL64, scale);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=pair(decimal64(NULL,"+to_string(scale)+"),decimal64(NULL,"+to_string(scale)+"));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isPair(),true);
}