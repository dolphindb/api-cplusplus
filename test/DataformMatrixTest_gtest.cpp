class DataformMatrixTest:public testing::Test
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


TEST_F(DataformMatrixTest,testStringMatrix){
	EXPECT_ANY_THROW(VectorSP v1 = Util::createMatrix(DT_STRING,2,2,4));
}

TEST_F(DataformMatrixTest,testAnyMatrix){
	EXPECT_ANY_THROW(VectorSP v1 = Util::createMatrix(DT_ANY,2,2,4));
}

TEST_F(DataformMatrixTest,testBoolMatrix){
	VectorSP v1 = Util::createMatrix(DT_BOOL,2,2,4);
    v1->set(0,0, Util::createBool(1));
    v1->set(1,0, Util::createBool(1));
	v1->set(0,1, Util::createBool(0));
	v1->set(1,1, Util::createBool(0));
	string script = "a=matrix([[bool(1),bool(0)],[bool(1),bool(0)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	cout<< conn.run("v1")->getString()<<endl;
	cout<< conn.run("a")->getString();
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testBoolNullMatrix){
	VectorSP v1 = Util::createMatrix(DT_BOOL,2,2,4);
    v1->set(0,0, Util::createNullConstant(DT_BOOL));
    v1->set(1,0, Util::createNullConstant(DT_BOOL));
	v1->set(0,1, Util::createNullConstant(DT_BOOL));
	v1->set(1,1, Util::createNullConstant(DT_BOOL));
	string script = "a=matrix([[bool(NULL),bool(NULL)],[bool(NULL),bool(NULL)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testCharMatrix){
	VectorSP v1 = Util::createMatrix(DT_CHAR,2,2,4);
    v1->set(0,0, Util::createChar(1));
    v1->set(1,0, Util::createChar(1));
	v1->set(0,1, Util::createChar(0));
	v1->set(1,1, Util::createChar(0));
	string script = "a=matrix([[char(1),char(0)],[char(1),char(0)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testCharNullMatrix){
	VectorSP v1 = Util::createMatrix(DT_CHAR,2,2,4);
    v1->set(0,0, Util::createNullConstant(DT_CHAR));
    v1->set(1,0, Util::createNullConstant(DT_CHAR));
	v1->set(0,1, Util::createNullConstant(DT_CHAR));
	v1->set(1,1, Util::createNullConstant(DT_CHAR));
	string script = "a=matrix([[char(NULL),char(NULL)],[char(NULL),char(NULL)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testIntMatrix){
	VectorSP v1 = Util::createMatrix(DT_INT,2,2,4);
    v1->set(0,0, Util::createInt(1));
    v1->set(1,0, Util::createInt(1));
	v1->set(0,1, Util::createInt(0));
	v1->set(1,1, Util::createInt(0));
	string script = "a=matrix([[int(1),int(0)],[int(1),int(0)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testIntNullMatrix){
	VectorSP v1 = Util::createMatrix(DT_INT,2,2,4);
    v1->set(0,0, Util::createNullConstant(DT_INT));
    v1->set(1,0, Util::createNullConstant(DT_INT));
	v1->set(0,1, Util::createNullConstant(DT_INT));
	v1->set(1,1, Util::createNullConstant(DT_INT));
	string script = "a=matrix([[int(NULL),int(NULL)],[int(NULL),int(NULL)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}


TEST_F(DataformMatrixTest,testLongMatrix){
	VectorSP v1 = Util::createMatrix(DT_LONG,2,2,4);
    v1->set(0,0, Util::createLong(1));
    v1->set(1,0, Util::createLong(1));
	v1->set(0,1, Util::createLong(0));
	v1->set(1,1, Util::createLong(0));
	string script = "a=matrix([[long(1),long(0)],[long(1),long(0)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testLongNullMatrix){
	VectorSP v1 = Util::createMatrix(DT_LONG,2,2,4);
    v1->set(0,0, Util::createNullConstant(DT_LONG));
    v1->set(1,0, Util::createNullConstant(DT_LONG));
	v1->set(0,1, Util::createNullConstant(DT_LONG));
	v1->set(1,1, Util::createNullConstant(DT_LONG));
	string script = "a=matrix([[long(NULL),long(NULL)],[long(NULL),long(NULL)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testShortNullMatrix){
	VectorSP v1 = Util::createMatrix(DT_SHORT,2,2,4);
    v1->set(0,0, Util::createNullConstant(DT_SHORT));
    v1->set(1,0, Util::createNullConstant(DT_SHORT));
	v1->set(0,1, Util::createNullConstant(DT_SHORT));
	v1->set(1,1, Util::createNullConstant(DT_SHORT));
	string script = "a=matrix([[short(NULL),short(NULL)],[short(NULL),short(NULL)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testShortMatrix){
	VectorSP v1 = Util::createMatrix(DT_SHORT,2,2,4);
    v1->set(0,0, Util::createShort(1));
    v1->set(1,0, Util::createShort(1));
	v1->set(0,1, Util::createShort(0));
	v1->set(1,1, Util::createShort(0));
	string script = "a=matrix([[short(1),short(0)],[short(1),short(0)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}


TEST_F(DataformMatrixTest,testFloatMatrix){
	VectorSP v1 = Util::createMatrix(DT_FLOAT,2,2,4);
    v1->set(0,0, Util::createFloat(1));
    v1->set(1,0, Util::createFloat(1));
	v1->set(0,1, Util::createFloat(0));
	v1->set(1,1, Util::createFloat(0));
	string script = "a=matrix([[float(1),float(0)],[float(1),float(0)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}


TEST_F(DataformMatrixTest,testFloatNullMatrix){
	VectorSP v1 = Util::createMatrix(DT_FLOAT,2,2,4);
    v1->set(0,0, Util::createNullConstant(DT_FLOAT));
    v1->set(1,0, Util::createNullConstant(DT_FLOAT));
	v1->set(0,1, Util::createNullConstant(DT_FLOAT));
	v1->set(1,1, Util::createNullConstant(DT_FLOAT));
	string script = "a=matrix([[float(NULL),float(NULL)],[float(NULL),float(NULL)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testDoubleMatrix){
	VectorSP v1 = Util::createMatrix(DT_DOUBLE,2,2,4);
    v1->set(0,0, Util::createDouble(1));
    v1->set(1,0, Util::createDouble(1));
	v1->set(0,1, Util::createDouble(0));
	v1->set(1,1, Util::createDouble(0));
	string script = "a=matrix([[double(1),double(0)],[double(1),double(0)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}


TEST_F(DataformMatrixTest,testDoubleNullMatrix){
	VectorSP v1 = Util::createMatrix(DT_DOUBLE,2,2,4);
    v1->set(0,0, Util::createNullConstant(DT_DOUBLE));
    v1->set(1,0, Util::createNullConstant(DT_DOUBLE));
	v1->set(0,1, Util::createNullConstant(DT_DOUBLE));
	v1->set(1,1, Util::createNullConstant(DT_DOUBLE));
	string script = "a=matrix([[double(NULL),double(NULL)],[double(NULL),double(NULL)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testDatehourMatrix){
	VectorSP v1 = Util::createMatrix(DT_DATEHOUR,2,2,4);
    v1->set(0,0, Util::createDateHour(1));
    v1->set(1,0, Util::createDateHour(1));
	v1->set(0,1, Util::createDateHour(0));
	v1->set(1,1, Util::createDateHour(0));
	string script = "a=matrix([[datehour(1),datehour(0)],[datehour(1),datehour(0)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testDatehourNullMatrix){
	VectorSP v1 = Util::createMatrix(DT_DATEHOUR,2,2,4);
    v1->set(0,0, Util::createNullConstant(DT_DATEHOUR));
    v1->set(1,0, Util::createNullConstant(DT_DATEHOUR));
	v1->set(0,1, Util::createNullConstant(DT_DATEHOUR));
	v1->set(1,1, Util::createNullConstant(DT_DATEHOUR));
	string script = "a=matrix([[datehour(NULL),datehour(NULL)],[datehour(NULL),datehour(NULL)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testDateMatrix){
	VectorSP v1 = Util::createMatrix(DT_DATE,2,2,4);
    v1->set(0,0, Util::createDate(1));
    v1->set(1,0, Util::createDate(1));
	v1->set(0,1, Util::createDate(0));
	v1->set(1,1, Util::createDate(0));
	string script = "a=matrix([[date(1),date(0)],[date(1),date(0)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testDatenullMatrix){
	VectorSP v1 = Util::createMatrix(DT_DATE,2,2,4);
    v1->set(0,0, Util::createNullConstant(DT_DATE));
    v1->set(1,0, Util::createNullConstant(DT_DATE));
	v1->set(0,1, Util::createNullConstant(DT_DATE));
	v1->set(1,1, Util::createNullConstant(DT_DATE));
	string script = "a=matrix([[date(NULL),date(NULL)],[date(NULL),date(NULL)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);

}

TEST_F(DataformMatrixTest,testMinuteMatrix){
	VectorSP v1 = Util::createMatrix(DT_MINUTE,2,2,4);
    v1->set(0,0, Util::createMinute(1));
    v1->set(1,0, Util::createMinute(1));
	v1->set(0,1, Util::createMinute(0));
	v1->set(1,1, Util::createMinute(0));
	string script = "a=matrix([[minute(1),minute(0)],[minute(1),minute(0)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testMinutenullMatrix){
	VectorSP v1 = Util::createMatrix(DT_MINUTE,2,2,4);
    v1->set(0,0, Util::createNullConstant(DT_MINUTE));
    v1->set(1,0, Util::createNullConstant(DT_MINUTE));
	v1->set(0,1, Util::createNullConstant(DT_MINUTE));
	v1->set(1,1, Util::createNullConstant(DT_MINUTE));
	string script = "a=matrix([[minute(NULL),minute(NULL)],[minute(NULL),minute(NULL)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);

}

TEST_F(DataformMatrixTest,testDatetimeMatrix){
	VectorSP v1 = Util::createMatrix(DT_DATETIME,2,2,4);
    v1->set(0,0, Util::createDateTime(1));
    v1->set(1,0, Util::createDateTime(1));
	v1->set(0,1, Util::createDateTime(0));
	v1->set(1,1, Util::createDateTime(0));
	string script = "a=matrix([[datetime(1),datetime(0)],[datetime(1),datetime(0)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testDatetimenullMatrix){
	VectorSP v1 = Util::createMatrix(DT_DATETIME,2,2,4);
    v1->set(0,0, Util::createNullConstant(DT_DATETIME));
    v1->set(1,0, Util::createNullConstant(DT_DATETIME));
	v1->set(0,1, Util::createNullConstant(DT_DATETIME));
	v1->set(1,1, Util::createNullConstant(DT_DATETIME));
	string script = "a=matrix([[datetime(NULL),datetime(NULL)],[datetime(NULL),datetime(NULL)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testTimeStampMatrix){
	VectorSP v1 = Util::createMatrix(DT_TIMESTAMP,2,2,4);
    v1->set(0,0, Util::createTimestamp(1));
    v1->set(1,0, Util::createTimestamp(1));
	v1->set(0,1, Util::createTimestamp(0));
	v1->set(1,1, Util::createTimestamp(0));
	string script = "a=matrix([[timestamp(1),timestamp(0)],[timestamp(1),timestamp(0)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testTimeStampnullMatrix){
	VectorSP v1 = Util::createMatrix(DT_TIMESTAMP,2,2,4);
    v1->set(0,0, Util::createNullConstant(DT_TIMESTAMP));
    v1->set(1,0, Util::createNullConstant(DT_TIMESTAMP));
	v1->set(0,1, Util::createNullConstant(DT_TIMESTAMP));
	v1->set(1,1, Util::createNullConstant(DT_TIMESTAMP));
	string script = "a=matrix([[timestamp(NULL),timestamp(NULL)],[timestamp(NULL),timestamp(NULL)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testnanotimeMatrix){
	VectorSP v1 = Util::createMatrix(DT_NANOTIME,2,2,4);
    v1->set(0,0, Util::createNanoTime(1));
    v1->set(1,0, Util::createNanoTime(1));
	v1->set(0,1, Util::createNanoTime(0));
	v1->set(1,1, Util::createNanoTime(0));
	string script = "a=matrix([[nanotime(1),nanotime(0)],[nanotime(1),nanotime(0)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testnanotimenullMatrix){
	VectorSP v1 = Util::createMatrix(DT_NANOTIME,2,2,4);
    v1->set(0,0, Util::createNullConstant(DT_NANOTIME));
    v1->set(1,0, Util::createNullConstant(DT_NANOTIME));
	v1->set(0,1, Util::createNullConstant(DT_NANOTIME));
	v1->set(1,1, Util::createNullConstant(DT_NANOTIME));
	string script = "a=matrix([[nanotime(NULL),nanotime(NULL)],[nanotime(NULL),nanotime(NULL)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testnanotimestampMatrix){
	VectorSP v1 = Util::createMatrix(DT_NANOTIMESTAMP,2,2,4);
    v1->set(0,0, Util::createNanoTimestamp(1));
    v1->set(1,0, Util::createNanoTimestamp(1));
	v1->set(0,1, Util::createNanoTimestamp(0));
	v1->set(1,1, Util::createNanoTimestamp(0));
	string script = "a=matrix([[nanotimestamp(1),nanotimestamp(0)],[nanotimestamp(1),nanotimestamp(0)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testnanotimestampnullMatrix){
	VectorSP v1 = Util::createMatrix(DT_NANOTIMESTAMP,2,2,4);
    v1->set(0,0, Util::createNullConstant(DT_NANOTIMESTAMP));
    v1->set(1,0, Util::createNullConstant(DT_NANOTIMESTAMP));
	v1->set(0,1, Util::createNullConstant(DT_NANOTIMESTAMP));
	v1->set(1,1, Util::createNullConstant(DT_NANOTIMESTAMP));
	string script = "a=matrix([[nanotimestamp(NULL),nanotimestamp(NULL)],[nanotimestamp(NULL),nanotimestamp(NULL)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testmonthMatrix){
	VectorSP v1 = Util::createMatrix(DT_MONTH,2,2,4);
    v1->set(0,0, Util::createMonth(1));
    v1->set(1,0, Util::createMonth(1));
	v1->set(0,1, Util::createMonth(0));
	v1->set(1,1, Util::createMonth(0));
	string script = "a=matrix([[month(1),month(0)],[month(1),month(0)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testmonthnullMatrix){
	VectorSP v1 = Util::createMatrix(DT_MONTH,2,2,4);
    v1->set(0,0, Util::createNullConstant(DT_MONTH));
    v1->set(1,0, Util::createNullConstant(DT_MONTH));
	v1->set(0,1, Util::createNullConstant(DT_MONTH));
	v1->set(1,1, Util::createNullConstant(DT_MONTH));
	string script = "a=matrix([[month(NULL),month(NULL)],[month(NULL),month(NULL)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testtimeMatrix){
	VectorSP v1 = Util::createMatrix(DT_TIME,2,2,4);
    v1->set(0,0, Util::createTime(1));
    v1->set(1,0, Util::createTime(1));
	v1->set(0,1, Util::createTime(0));
	v1->set(1,1, Util::createTime(0));
	string script = "a=matrix([[time(1),time(0)],[time(1),time(0)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testtimenullMatrix){
	VectorSP v1 = Util::createMatrix(DT_TIME,2,2,4);
    v1->set(0,0, Util::createNullConstant(DT_TIME));
    v1->set(1,0, Util::createNullConstant(DT_TIME));
	v1->set(0,1, Util::createNullConstant(DT_TIME));
	v1->set(1,1, Util::createNullConstant(DT_TIME));
	string script = "a=matrix([[time(NULL),time(NULL)],[time(NULL),time(NULL)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testBoolMatrixMoreThan65535){
	VectorSP v1 = Util::createMatrix(DT_BOOL,300,300,90000);
	for(int i=0;i<300;i++){
		for(int j=0;j<300;j++){
    		v1->set(i,j, Util::createBool(1));
		}
	}
	string script = "a=matrix(BOOL,300,300,90000,bool(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testCharMatrixMoreThan65535){
	VectorSP v1 = Util::createMatrix(DT_CHAR,300,300,90000);
	for(int i=0;i<300;i++){
		for(int j=0;j<300;j++){
    		v1->set(i,j, Util::createChar(1));
		}
	}
	string script = "a=matrix(CHAR,300,300,90000,char(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testIntMatrixMoreThan65535){
	VectorSP v1 = Util::createMatrix(DT_INT,300,300,90000);
	for(int i=0;i<300;i++){
		for(int j=0;j<300;j++){
    		v1->set(i,j, Util::createInt(1));
		}
	}
	string script = "a=matrix(INT,300,300,90000,int(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testLongMatrixMoreThan65535){
	VectorSP v1 = Util::createMatrix(DT_LONG,300,300,90000);
	for(int i=0;i<300;i++){
		for(int j=0;j<300;j++){
    		v1->set(i,j, Util::createLong(1));
		}
	}
	string script = "a=matrix(LONG,300,300,90000,long(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testShortMatrixMoreThan65535){
	VectorSP v1 = Util::createMatrix(DT_SHORT,300,300,90000);
	for(int i=0;i<300;i++){
		for(int j=0;j<300;j++){
    		v1->set(i,j, Util::createShort(1));
		}
	}
	string script = "a=matrix(SHORT,300,300,90000,short(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}


TEST_F(DataformMatrixTest,testFloatMatrixMoreThan65535){
	VectorSP v1 = Util::createMatrix(DT_FLOAT,300,300,90000);
	for(int i=0;i<300;i++){
		for(int j=0;j<300;j++){
    		v1->set(i,j, Util::createFloat(1));
		}
	}
	string script = "a=matrix(FLOAT,300,300,90000,float(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}


TEST_F(DataformMatrixTest,testDoubleMatrixMoreThan65535){
	VectorSP v1 = Util::createMatrix(DT_DOUBLE,300,300,90000);
	for(int i=0;i<300;i++){
		for(int j=0;j<300;j++){
    		v1->set(i,j, Util::createDouble(1));
		}
	}
	string script = "a=matrix(DOUBLE,300,300,90000,double(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}


TEST_F(DataformMatrixTest,testDatehourMatrixMoreThan65535){
	VectorSP v1 = Util::createMatrix(DT_DATEHOUR,300,300,90000);
	for(int i=0;i<300;i++){
		for(int j=0;j<300;j++){
    		v1->set(i,j, Util::createDateHour(1));
		}
	}
	string script = "a=matrix(DATEHOUR,300,300,90000,datehour(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testDateMatrixMoreThan65535){
	VectorSP v1 = Util::createMatrix(DT_DATE,300,300,90000);
	for(int i=0;i<300;i++){
		for(int j=0;j<300;j++){
    		v1->set(i,j, Util::createDate(1));
		}
	}
	string script = "a=matrix(DATE,300,300,90000,date(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testMinuteMatrixMoreThan65535){
	VectorSP v1 = Util::createMatrix(DT_MINUTE,300,300,90000);
	for(int i=0;i<300;i++){
		for(int j=0;j<300;j++){
    		v1->set(i,j, Util::createMinute(1));
		}
	}
	string script = "a=matrix(MINUTE,300,300,90000,minute(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testDatetimeMatrixMoreThan65535){
	VectorSP v1 = Util::createMatrix(DT_DATETIME,300,300,90000);
	for(int i=0;i<300;i++){
		for(int j=0;j<300;j++){
    		v1->set(i,j, Util::createDateTime(1));
		}
	}
	string script = "a=matrix(DATETIME,300,300,90000,datetime(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testTimeStampMatrixMoreThan65535){
	VectorSP v1 = Util::createMatrix(DT_TIMESTAMP,300,300,90000);
	for(int i=0;i<300;i++){
		for(int j=0;j<300;j++){
    		v1->set(i,j, Util::createTimestamp(1));
		}
	}
	string script = "a=matrix(TIMESTAMP,300,300,90000,timestamp(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testnanotimeMatrixMoreThan65535){
	VectorSP v1 = Util::createMatrix(DT_NANOTIME,300,300,90000);
	for(int i=0;i<300;i++){
		for(int j=0;j<300;j++){
    		v1->set(i,j, Util::createNanoTime(1));
		}
	}
	string script = "a=matrix(NANOTIME,300,300,90000,nanotime(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}


TEST_F(DataformMatrixTest,testnanotimestampMatrixMoreThan65535){
	VectorSP v1 = Util::createMatrix(DT_NANOTIMESTAMP,300,300,90000);
	for(int i=0;i<300;i++){
		for(int j=0;j<300;j++){
    		v1->set(i,j, Util::createNanoTimestamp(1));
		}
	}
	string script = "a=matrix(NANOTIMESTAMP,300,300,90000,nanotimestamp(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testmonthMatrixMoreThan65535){
	VectorSP v1 = Util::createMatrix(DT_MONTH,300,300,90000);
	for(int i=0;i<300;i++){
		for(int j=0;j<300;j++){
    		v1->set(i,j, Util::createMonth(1));
		}
	}
	string script = "a=matrix(MONTH,300,300,90000,month(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testtimeMatrixMoreThan65535){
	VectorSP v1 = Util::createMatrix(DT_TIME,300,300,90000);
	for(int i=0;i<300;i++){
		for(int j=0;j<300;j++){
    		v1->set(i,j, Util::createTime(1));
		}
	}
	string script = "a=matrix(TIME,300,300,90000,time(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}


TEST_F(DataformMatrixTest,testBoolMatrixMoreThan1048576){
	VectorSP v1 = Util::createMatrix(DT_BOOL,1100,1100,1210000);
	for(int i=0;i<1100;i++){
		for(int j=0;j<1100;j++){
    		v1->set(i,j, Util::createBool(1));
		}
	}
	string script = "a=matrix(BOOL,1100,1100,1210000,bool(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testCharMatrixMoreThan1048576){
	VectorSP v1 = Util::createMatrix(DT_CHAR,1100,1100,1210000);
	for(int i=0;i<1100;i++){
		for(int j=0;j<1100;j++){
    		v1->set(i,j, Util::createChar(1));
		}
	}
	string script = "a=matrix(CHAR,1100,1100,1210000,char(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testIntMatrixMoreThan1048576){
	VectorSP v1 = Util::createMatrix(DT_INT,1100,1100,1210000);
	for(int i=0;i<1100;i++){
		for(int j=0;j<1100;j++){
    		v1->set(i,j, Util::createInt(1));
		}
	}
	string script = "a=matrix(INT,1100,1100,1210000,int(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testLongMatrixMoreThan1048576){
	VectorSP v1 = Util::createMatrix(DT_LONG,1100,1100,1210000);
	for(int i=0;i<1100;i++){
		for(int j=0;j<1100;j++){
    		v1->set(i,j, Util::createLong(1));
		}
	}
	string script = "a=matrix(LONG,1100,1100,1210000,long(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testShortMatrixMoreThan1048576){
	VectorSP v1 = Util::createMatrix(DT_SHORT,1100,1100,1210000);
	for(int i=0;i<1100;i++){
		for(int j=0;j<1100;j++){
    		v1->set(i,j, Util::createShort(1));
		}
	}
	string script = "a=matrix(SHORT,1100,1100,1210000,short(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}


TEST_F(DataformMatrixTest,testFloatMatrixMoreThan1048576){
	VectorSP v1 = Util::createMatrix(DT_FLOAT,1100,1100,1210000);
	for(int i=0;i<1100;i++){
		for(int j=0;j<1100;j++){
    		v1->set(i,j, Util::createFloat(1));
		}
	}
	string script = "a=matrix(FLOAT,1100,1100,1210000,float(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}


TEST_F(DataformMatrixTest,testDoubleMatrixMoreThan1048576){
	VectorSP v1 = Util::createMatrix(DT_DOUBLE,1100,1100,1210000);
	for(int i=0;i<1100;i++){
		for(int j=0;j<1100;j++){
    		v1->set(i,j, Util::createDouble(1));
		}
	}
	string script = "a=matrix(DOUBLE,1100,1100,1210000,double(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}


TEST_F(DataformMatrixTest,testDatehourMatrixMoreThan1048576){
	VectorSP v1 = Util::createMatrix(DT_DATEHOUR,1100,1100,1210000);
	for(int i=0;i<1100;i++){
		for(int j=0;j<1100;j++){
    		v1->set(i,j, Util::createDateHour(1));
		}
	}
	string script = "a=matrix(DATEHOUR,1100,1100,1210000,datehour(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testDateMatrixMoreThan1048576){
	VectorSP v1 = Util::createMatrix(DT_DATE,1100,1100,1210000);
	for(int i=0;i<1100;i++){
		for(int j=0;j<1100;j++){
    		v1->set(i,j, Util::createDate(1));
		}
	}
	string script = "a=matrix(DATE,1100,1100,1210000,date(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testMinuteMatrixMoreThan1048576){
	VectorSP v1 = Util::createMatrix(DT_MINUTE,1100,1100,1210000);
	for(int i=0;i<1100;i++){
		for(int j=0;j<1100;j++){
    		v1->set(i,j, Util::createMinute(1));
		}
	}
	string script = "a=matrix(MINUTE,1100,1100,1210000,minute(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testDatetimeMatrixMoreThan1048576){
	VectorSP v1 = Util::createMatrix(DT_DATETIME,1100,1100,1210000);
	for(int i=0;i<1100;i++){
		for(int j=0;j<1100;j++){
    		v1->set(i,j, Util::createDateTime(1));
		}
	}
	string script = "a=matrix(DATETIME,1100,1100,1210000,datetime(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testTimeStampMatrixMoreThan1048576){
	VectorSP v1 = Util::createMatrix(DT_TIMESTAMP,1100,1100,1210000);
	for(int i=0;i<1100;i++){
		for(int j=0;j<1100;j++){
    		v1->set(i,j, Util::createTimestamp(1));
		}
	}
	string script = "a=matrix(TIMESTAMP,1100,1100,1210000,timestamp(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testnanotimeMatrixMoreThan1048576){
	VectorSP v1 = Util::createMatrix(DT_NANOTIME,1100,1100,1210000);
	for(int i=0;i<1100;i++){
		for(int j=0;j<1100;j++){
    		v1->set(i,j, Util::createNanoTime(1));
		}
	}
	string script = "a=matrix(NANOTIME,1100,1100,1210000,nanotime(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}


TEST_F(DataformMatrixTest,testnanotimestampMatrixMoreThan1048576){
	VectorSP v1 = Util::createMatrix(DT_NANOTIMESTAMP,1100,1100,1210000);
	for(int i=0;i<1100;i++){
		for(int j=0;j<1100;j++){
    		v1->set(i,j, Util::createNanoTimestamp(1));
		}
	}
	string script = "a=matrix(NANOTIMESTAMP,1100,1100,1210000,nanotimestamp(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testmonthMatrixMoreThan1048576){
	VectorSP v1 = Util::createMatrix(DT_MONTH,1100,1100,1210000);
	for(int i=0;i<1100;i++){
		for(int j=0;j<1100;j++){
    		v1->set(i,j, Util::createMonth(1));
		}
	}
	string script = "a=matrix(MONTH,1100,1100,1210000,month(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}

TEST_F(DataformMatrixTest,testtimeMatrixMoreThan1048576){
	VectorSP v1 = Util::createMatrix(DT_TIME,1100,1100,1210000);
	for(int i=0;i<1100;i++){
		for(int j=0;j<1100;j++){
    		v1->set(i,j, Util::createTime(1));
		}
	}
	string script = "a=matrix(TIME,1100,1100,1210000,time(1));a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);
}