class DataformVectorTest:public testing::Test
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


TEST_F(DataformVectorTest,testStringVector){
	VectorSP v1 = Util::createVector(DT_STRING, 2, 2);
    v1->set(0, Util::createString("asd123!@#"));
    v1->set(1, Util::createString("中文！@￥#%……a"));
	string script = "a=[\"asd123!@#\",\"中文！@￥#%……a\"];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
}

TEST_F(DataformVectorTest,testAnyVector){
	VectorSP v1 = Util::createVector(DT_ANY, 5, 5);
    v1->set(0, Util::createTime(1));
    v1->set(1, Util::createInt(1));
	v1->set(2, Util::createDouble(1));
	v1->set(3, Util::createString("asd"));
	v1->set(4, Util::createBool(1));
	string script = "a=[time(1), int(1),double(1),`asd,true];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());

	VectorSP v2 = Util::createVector(DT_ANY,0,2000,false,0,(void *)0,true);
	EXPECT_FALSE(v2->getNullFlag()); //anyvector is absolutely not null
	EXPECT_EQ(v2->getCapacity(),0);
	EXPECT_FALSE(v2->isFastMode());
	EXPECT_EQ(v2->getUnitLength(),0);
	EXPECT_TRUE(v2->sizeable());
	EXPECT_EQ(v2->getRawType(),DT_ANY);
	EXPECT_EQ(v2->getCategory(),MIXED);
	EXPECT_ANY_THROW(v2->getStringRef(0));

	v2->append(Util::createInt(1));
	VectorSP v3 = v2->getInstance(1);
	EXPECT_EQ(v3->getString(),"()");
	EXPECT_FALSE(v3->isNull()); //anyvector is absolutely not null
	VectorSP v4 = v2->getValue(10);
	EXPECT_EQ(v4->getString(),"(1)");
	v4->setNull(); //nothing to do and anyvector is not changed.
	EXPECT_EQ(v4->getString(),"(1)");
	EXPECT_FALSE(v2->append(Util::createInt(2), 1));
    EXPECT_TRUE(v2->isLargeConstant());

	EXPECT_EQ(v2->getBool(0),true);
	EXPECT_EQ(v2->getChar(0),(char)1);
	EXPECT_EQ(v2->getShort(0),(short)1);
	EXPECT_EQ(v2->getInt(0),(int)1);
	EXPECT_EQ(v2->getLong(0),(long)1);
	EXPECT_EQ(v2->getIndex(0),(INDEX)1);
	EXPECT_EQ(v2->getFloat(0),(float)1);
	EXPECT_EQ(v2->getDouble(0),(double)1);

	char *buf = new char[1];
	string **buf1 = new string*[1];
	char **buf2 = new char*[1];
	int numElement,partial;
	EXPECT_ANY_THROW(v2->serialize(buf,1,0,1,numElement,partial));
	// EXPECT_ANY_THROW(v2->getString(0,1,buf1));
	// EXPECT_ANY_THROW(v2->getString(0,1,buf2));
	EXPECT_ANY_THROW(v2->getStringConst(0,1,buf1));
	EXPECT_ANY_THROW(v2->getStringConst(0,1,buf2));

	EXPECT_ANY_THROW(v2->neg());
	EXPECT_ANY_THROW(v2->replace(Util::createInt(1),Util::createInt(0)));
	EXPECT_ANY_THROW(v2->asof(Util::createInt(1)));
	EXPECT_ANY_THROW(v2->getSubVector(0,-1,0));

	v2->append(Util::createInt(0));
	v2->reverse();
	EXPECT_EQ(v2->getString(),"(0,1)");
	v2->append(Util::createInt(-10));
	v2->reverse(0,3);
	EXPECT_EQ(v2->getString(),"(-10,1,0)");
}

TEST_F(DataformVectorTest,testStringNullVector){
	VectorSP v1 = Util::createVector(DT_STRING, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=[string(NULL),string(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
}

TEST_F(DataformVectorTest,testBoolVector){
	VectorSP v1 = Util::createVector(DT_BOOL, 2, 2);
    v1->set(0, Util::createBool(1));
    v1->set(1, Util::createBool(0));
	string script = "a=[bool(1),bool(0)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());

	EXPECT_EQ(v1->getCategory(),LOGICAL);
	EXPECT_FALSE(v1->add(0,1,(long long)1));
	EXPECT_FALSE(v1->add(0,1,(double)1));
	EXPECT_ANY_THROW(v1->asof(Util::createBool(1)));

}

TEST_F(DataformVectorTest,testBoolNullVector){
	VectorSP v1 = Util::createVector(DT_BOOL, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=[bool(NULL),bool(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0;i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest,testCharVector){
	VectorSP v1 = Util::createVector(DT_CHAR, 2, 2);
    v1->set(0, Util::createChar(1));
    v1->set(1, Util::createChar(0));
	string script = "a=[char(1),char(0)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(v1->getCategory(),INTEGRAL);
}

TEST_F(DataformVectorTest,testCharNullVector){
	VectorSP v1 = Util::createVector(DT_CHAR, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=[char(NULL),char(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0;i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest,testIntVector){
	VectorSP v1 = Util::createVector(DT_INT, 2, 2);
    v1->set(0, Util::createInt(1));
    v1->set(1, Util::createInt(0));
	string script = "a=[int(1),int(0)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	v1->append(Util::createInt(2));
	EXPECT_FALSE(v1->isSorted(false));
	EXPECT_FALSE(v1->isSorted(false,true));
	EXPECT_FALSE(v1->hasNull(0,2));

	VectorSP v2 = Util::createVector(DT_INT, 1, 1);
	v2->set(0,Util::createNullConstant(DT_INT));
	EXPECT_EQ(v2->getBool(),CHAR_MIN);
	EXPECT_EQ(v2->getChar(),CHAR_MIN);
	EXPECT_EQ(v2->getShort(),SHRT_MIN);
	EXPECT_EQ(v2->getInt(),INT_MIN);
	EXPECT_EQ(v2->getIndex(),INDEX_MIN);
	EXPECT_EQ(v2->getLong(),LLONG_MIN);
	EXPECT_EQ(v2->getDouble(),DBL_NMIN);
	EXPECT_EQ(v2->getFloat(),FLT_NMIN);

	v2->set(0,Util::createInt(0));
	EXPECT_FALSE(v2->getBool());
	EXPECT_EQ(v2->getChar(),(char)0);
	EXPECT_EQ(v2->getShort(),(short)0);
	EXPECT_EQ(v2->getInt(),(int)0);
	EXPECT_EQ(v2->getIndex(),(INDEX)0);
	EXPECT_EQ(v2->getLong(),(long long)0);
	EXPECT_EQ(v2->getDouble(),(double)0);
	EXPECT_EQ(v2->getFloat(),(float)0);

	EXPECT_ANY_THROW(v1->getBool());
	EXPECT_ANY_THROW(v1->getChar());
	EXPECT_ANY_THROW(v1->getShort());
	EXPECT_ANY_THROW(v1->getInt());
	EXPECT_ANY_THROW(v1->getIndex());
	EXPECT_ANY_THROW(v1->getLong());
	EXPECT_ANY_THROW(v1->getDouble());
	EXPECT_ANY_THROW(v1->getFloat());

	v2->setBool(1);
	EXPECT_EQ(v2->getBool(0),(bool)1);
	v2->setChar(1);
	EXPECT_EQ(v2->get(0)->getChar(),(char)1);
	v2->setShort(1);
	EXPECT_EQ(v2->get(0)->getShort(),(short)1);
	v2->setInt(1);
	EXPECT_EQ(v2->get(0)->getInt(),(int)1);
	v2->setLong(1);
	EXPECT_EQ(v2->get(0)->getLong(),(long long)1);
	v2->setIndex(1);
	EXPECT_EQ(v2->get(0)->getIndex(),(INDEX)1);
	v2->setFloat(1);
	EXPECT_EQ(v2->get(0)->getFloat(),(float)1);
	v2->setDouble(1);
	EXPECT_EQ(v2->get(0)->getDouble(),(double)1);
	v2->setString("1");
	EXPECT_EQ(v2->get(0)->getString(),"1");
	v2->setNull();

}

TEST_F(DataformVectorTest,testIntNullVector){
	VectorSP v1 = Util::createVector(DT_INT, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=[int(NULL),int(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0;i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}


TEST_F(DataformVectorTest,testLongVector){
	VectorSP v1 = Util::createVector(DT_LONG, 2, 2);
    v1->set(0, Util::createLong(1));
    v1->set(1, Util::createLong(0));
	string script = "a=[long(1),long(0)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
}

TEST_F(DataformVectorTest,testLongNullVector){
	VectorSP v1 = Util::createVector(DT_LONG, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=[long(NULL),long(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0;i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest,testShortNullVector){
	VectorSP v1 = Util::createVector(DT_SHORT, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=[short(NULL),short(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0;i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest,testShortVector){
	VectorSP v1 = Util::createVector(DT_SHORT, 2, 2);
    v1->set(0, Util::createShort(1));
    v1->set(1, Util::createShort(0));
	string script = "a=[short(1),short(0)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(v1->getCategory(),INTEGRAL);
}

TEST_F(DataformVectorTest,testFloatVector){
	VectorSP v1 = Util::createVector(DT_FLOAT, 2, 2);
    v1->set(0, Util::createFloat(1));
    v1->set(1, Util::createFloat(2.3131));
	string script = "a=[float(1),float(2.3131)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());

	EXPECT_EQ(v1->getCategory(),FLOATING);
	EXPECT_EQ(v1->getChar(1),(char)2);
	EXPECT_EQ(v1->getShort(1),(short)2);
	EXPECT_EQ(v1->getInt(1),(int)2);
	EXPECT_EQ(v1->getLong(1),(long long)2);

    char *buf = new char[2]; 
    short *buf1 = new short[2];
    int *buf2 = new int[2];
    long long *buf3 = new long long[2];
	v1->getChar(0,2,buf);
	EXPECT_EQ(buf[0],(char)1);
	EXPECT_EQ(buf[1],(char)2);
	buf[0]='\0';
	buf[1]='\0';
	v1->getCharConst(0,2,buf);
	EXPECT_EQ(buf[0],(char)1);
	EXPECT_EQ(buf[1],(char)2);
	v1->getShort(0,2,buf1);
	EXPECT_EQ(buf1[0],(short)1);
	EXPECT_EQ(buf1[1],(short)2);
	buf1[0]='\0';
	buf1[1]='\0';
	v1->getShortConst(0,2,buf1);
	EXPECT_EQ(buf1[0],(short)1);
	EXPECT_EQ(buf1[1],(short)2);
	v1->getInt(0,2,buf2);
	EXPECT_EQ(buf2[0],(int)1);
	EXPECT_EQ(buf2[1],(int)2);
	buf2[0]='\0';
	buf2[1]='\0';
	v1->getIntConst(0,2,buf2);
	EXPECT_EQ(buf2[0],(int)1);
	EXPECT_EQ(buf2[1],(int)2);
	v1->getLong(0,2,buf3);
	EXPECT_EQ(buf3[0],(long long)1);
	EXPECT_EQ(buf3[1],(long long)2);
	buf3[0]='\0';
	buf3[1]='\0';
	v1->getLongConst(0,2,buf3);
	EXPECT_EQ(buf3[0],(long long)1);
	EXPECT_EQ(buf3[1],(long long)2);
}


TEST_F(DataformVectorTest,testFloatNullVector){
	VectorSP v1 = Util::createVector(DT_FLOAT, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=[float(NULL),float(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());

	VectorSP v2 = Util::createVector(DT_FLOAT, 2, 2,true,0,(void *)0, true);
    v2->setNull(0);
    v2->setNull(1);
    char *buf = new char[2]; 
    short *buf1 = new short[2];
    int *buf2 = new int[2];
    long long *buf3 = new long long[2];
	v2->getChar(0,2,buf);
	EXPECT_EQ(buf[0],CHAR_MIN);
	EXPECT_EQ(buf[1],CHAR_MIN);
	buf[0]='\0';
	buf[1]='\0';
	v2->getCharConst(0,2,buf);
	EXPECT_EQ(buf[0],CHAR_MIN);
	EXPECT_EQ(buf[1],CHAR_MIN);
	v2->getShort(0,2,buf1);
	EXPECT_EQ(buf1[0],SHRT_MIN);
	EXPECT_EQ(buf1[1],SHRT_MIN);
	buf1[0]='\0';
	buf1[1]='\0';
	v2->getShortConst(0,2,buf1);
	EXPECT_EQ(buf1[0],SHRT_MIN);
	EXPECT_EQ(buf1[1],SHRT_MIN);
	v2->getInt(0,2,buf2);
	EXPECT_EQ(buf2[0],INT_MIN);
	EXPECT_EQ(buf2[1],INT_MIN);
	buf2[0]='\0';
	buf2[1]='\0';
	v2->getIntConst(0,2,buf2);
	EXPECT_EQ(buf2[0],INT_MIN);
	EXPECT_EQ(buf2[1],INT_MIN);
	v2->getLong(0,2,buf3);
	EXPECT_EQ(buf3[0],LLONG_MIN);
	EXPECT_EQ(buf3[1],LLONG_MIN);
	buf3[0]='\0';
	buf3[1]='\0';
	v2->getLongConst(0,2,buf3);
	EXPECT_EQ(buf3[0],LLONG_MIN);
	EXPECT_EQ(buf3[1],LLONG_MIN);
}

TEST_F(DataformVectorTest,testDoubleVector){
	VectorSP v1 = Util::createVector(DT_DOUBLE, 2, 2);
    v1->set(0, Util::createDouble(1));
    v1->set(1, Util::createDouble(2.3131));
	string script = "a=[double(1),double(2.3131)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());

	EXPECT_EQ(v1->getCategory(),FLOATING);
	EXPECT_EQ(v1->getChar(1),(char)2);
	EXPECT_EQ(v1->getShort(1),(short)2);
	EXPECT_EQ(v1->getInt(1),(int)2);
	EXPECT_EQ(v1->getLong(1),(long long)2);

    char *buf = new char[2]; 
    short *buf1 = new short[2];
    int *buf2 = new int[2];
    long long *buf3 = new long long[2];
	v1->getChar(0,2,buf);
	EXPECT_EQ(buf[0],(char)1);
	EXPECT_EQ(buf[1],(char)2);
	buf[0]='\0';
	buf[1]='\0';
	v1->getCharConst(0,2,buf);
	EXPECT_EQ(buf[0],(char)1);
	EXPECT_EQ(buf[1],(char)2);
	v1->getShort(0,2,buf1);
	EXPECT_EQ(buf1[0],(short)1);
	EXPECT_EQ(buf1[1],(short)2);
	buf1[0]='\0';
	buf1[1]='\0';
	v1->getShortConst(0,2,buf1);
	EXPECT_EQ(buf1[0],(short)1);
	EXPECT_EQ(buf1[1],(short)2);
	v1->getInt(0,2,buf2);
	EXPECT_EQ(buf2[0],(int)1);
	EXPECT_EQ(buf2[1],(int)2);
	buf2[0]='\0';
	buf2[1]='\0';
	v1->getIntConst(0,2,buf2);
	EXPECT_EQ(buf2[0],(int)1);
	EXPECT_EQ(buf2[1],(int)2);
	v1->getLong(0,2,buf3);
	EXPECT_EQ(buf3[0],(long long)1);
	EXPECT_EQ(buf3[1],(long long)2);
	buf3[0]='\0';
	buf3[1]='\0';
	v1->getLongConst(0,2,buf3);
	EXPECT_EQ(buf3[0],(long long)1);
	EXPECT_EQ(buf3[1],(long long)2);
}


TEST_F(DataformVectorTest,testDoubleNullVector){
	VectorSP v1 = Util::createVector(DT_DOUBLE, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=[double(NULL),double(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());

	VectorSP v2 = Util::createVector(DT_DOUBLE, 2, 2,true,0,(void *)0, true);
    v2->setNull(0);
    v2->setNull(1);
    char *buf = new char[2]; 
    short *buf1 = new short[2];
    int *buf2 = new int[2];
    long long *buf3 = new long long[2];
	v2->getChar(0,2,buf);
	EXPECT_EQ(buf[0],CHAR_MIN);
	EXPECT_EQ(buf[1],CHAR_MIN);
	buf[0]='\0';
	buf[1]='\0';
	v2->getCharConst(0,2,buf);
	EXPECT_EQ(buf[0],CHAR_MIN);
	EXPECT_EQ(buf[1],CHAR_MIN);
	v2->getShort(0,2,buf1);
	EXPECT_EQ(buf1[0],SHRT_MIN);
	EXPECT_EQ(buf1[1],SHRT_MIN);
	buf1[0]='\0';
	buf1[1]='\0';
	v2->getShortConst(0,2,buf1);
	EXPECT_EQ(buf1[0],SHRT_MIN);
	EXPECT_EQ(buf1[1],SHRT_MIN);
	v2->getInt(0,2,buf2);
	EXPECT_EQ(buf2[0],INT_MIN);
	EXPECT_EQ(buf2[1],INT_MIN);
	buf2[0]='\0';
	buf2[1]='\0';
	v2->getIntConst(0,2,buf2);
	EXPECT_EQ(buf2[0],INT_MIN);
	EXPECT_EQ(buf2[1],INT_MIN);
	v2->getLong(0,2,buf3);
	EXPECT_EQ(buf3[0],LLONG_MIN);
	EXPECT_EQ(buf3[1],LLONG_MIN);
	buf3[0]='\0';
	buf3[1]='\0';
	v2->getLongConst(0,2,buf3);
	EXPECT_EQ(buf3[0],LLONG_MIN);
	EXPECT_EQ(buf3[1],LLONG_MIN);
}

TEST_F(DataformVectorTest,testDatehourVector){
	VectorSP v1 = Util::createVector(DT_DATEHOUR, 2, 2);
    v1->set(0, Util::createDateHour(1));
    v1->set(1, Util::createDateHour(1000));
	string script = "a=[datehour(1),datehour(1000)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
}

TEST_F(DataformVectorTest,testDatehourNullVector){
	VectorSP v1 = Util::createVector(DT_DATEHOUR, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=[datehour(NULL),datehour(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0;i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest,testDateVector){
	VectorSP v1 = Util::createVector(DT_DATE, 2, 2);
    v1->set(0, Util::createDate(1));
    v1->set(1, Util::createDate(1000));
	string script = "a=[date(1),date(1000)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
}

TEST_F(DataformVectorTest,testDatenullVector){
	VectorSP v1 = Util::createVector(DT_DATE, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=[date(NULL),date(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0;i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());

}

TEST_F(DataformVectorTest,testMinuteVector){
	VectorSP v1 = Util::createVector(DT_MINUTE, 2, 2);
    v1->set(0, Util::createMinute(1));
    v1->set(1, Util::createMinute(1000));
	string script = "a=[minute(1),minute(1000)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
}

TEST_F(DataformVectorTest,testMinutenullVector){
	VectorSP v1 = Util::createVector(DT_MINUTE, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=[minute(NULL),minute(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0;i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());

}

TEST_F(DataformVectorTest,testDatetimeVector){
	VectorSP v1 = Util::createVector(DT_DATETIME, 2, 2);
    v1->set(0, Util::createDateTime(1));
    v1->set(1, Util::createDateTime(1000));
	string script = "a=[datetime(1),datetime(1000)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
}

TEST_F(DataformVectorTest,testDatetimenullVector){
	VectorSP v1 = Util::createVector(DT_DATETIME, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=[datetime(NULL),datetime(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0;i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());

}

TEST_F(DataformVectorTest,testTimestampVector){
	VectorSP v1 = Util::createVector(DT_TIMESTAMP, 2, 2);
    v1->set(0, Util::createTimestamp(1));
    v1->set(1, Util::createTimestamp(1000000));
	string script = "a=[timestamp(1),timestamp(1000000)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
}

TEST_F(DataformVectorTest,testTimestampnullVector){
	VectorSP v1 = Util::createVector(DT_TIMESTAMP, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=[timestamp(NULL),timestamp(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0;i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());

}

TEST_F(DataformVectorTest,testNanotimeVector){
	VectorSP v1 = Util::createVector(DT_NANOTIME, 2, 2);
    v1->set(0, Util::createNanoTime(1));
    v1->set(1, Util::createNanoTime(1000000));
	string script = "a=[nanotime(1),nanotime(1000000)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
}

TEST_F(DataformVectorTest,testNanotimenullVector){
	VectorSP v1 = Util::createVector(DT_NANOTIME, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=[nanotime(NULL),nanotime(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0;i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());

}

TEST_F(DataformVectorTest,testNanotimestampVector){
	VectorSP v1 = Util::createVector(DT_NANOTIMESTAMP, 2, 2);
    v1->set(0, Util::createNanoTimestamp(1));
    v1->set(1, Util::createNanoTimestamp(100000000));
	string script = "a=[nanotimestamp(1),nanotimestamp(100000000)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
}

TEST_F(DataformVectorTest,testNanotimestampnullVector){
	VectorSP v1 = Util::createVector(DT_NANOTIMESTAMP, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=[nanotimestamp(NULL),nanotimestamp(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0;i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());

}

TEST_F(DataformVectorTest,testMonthVector){
	VectorSP v1 = Util::createVector(DT_MONTH, 2, 2);
    v1->set(0, Util::createMonth(1));
    v1->set(1, Util::createMonth(1000));
	string script = "a=[month(1),month(1000)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());

	EXPECT_FALSE(v1->isIndexArray());
	EXPECT_TRUE(v1->getIndexArray()== NULL);
    EXPECT_EQ(v1->getValue(2)->getString(),v1->castTemporal(DT_MONTH)->getString());
}

TEST_F(DataformVectorTest,testMonthnullVector){
	VectorSP v1 = Util::createVector(DT_MONTH, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=[month(NULL),month(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0;i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());

}

TEST_F(DataformVectorTest,testTimeVector){
	VectorSP v1 = Util::createVector(DT_TIME, 2, 2);
    v1->set(0, Util::createTime(1));
    v1->set(1, Util::createTime(1000));
	string script = "a=[time(1),time(1000)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
}

TEST_F(DataformVectorTest,testTimenullVector){
	VectorSP v1 = Util::createVector(DT_TIME, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
	string script = "a=[time(NULL),time(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0;i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());

}

TEST_F(DataformVectorTest,testStringVectorMoreThan65535){
	VectorSP v1 = Util::createVector(DT_STRING,70000,70000);
	for(int i=0;i<70000;i++){
		v1->set(i,Util::createString(to_string(i)));
	}
	string script = "z=array(STRING,0);for (i in 0..69999){z.append!(string(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testAnyVectorMoreThan65535){
	VectorSP v1 = Util::createVector(DT_ANY,70000,70000);
	for(int i=0;i<70000;i++){
		v1->set(i,Util::createString(to_string(i)));
	}
	string script = "z=array(ANY,0);for (i in 0..69999){z.append!(string(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testCharVectorEqule128){
	VectorSP v1 = Util::createVector(DT_CHAR,128,128);
	for(int i=0;i<128;i++){
		v1->set(i,Util::createChar(i));
	}
	string script = "z=array(CHAR,0);for (i in 0..127){z.append!(char(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testIntVectorMoreThan65535){
	VectorSP v1 = Util::createVector(DT_INT,70000,70000);
	for(int i=0;i<70000;i++){
		v1->set(i,Util::createInt(i));
	}
	string script = "z=array(INT,0);for (i in 0..69999){z.append!(int(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testLongVectorMoreThan65535){
	VectorSP v1 = Util::createVector(DT_LONG,70000,70000);
	for(int i=0;i<70000;i++){
		v1->set(i,Util::createLong(i));
	}
	string script = "z=array(LONG,0);for (i in 0..69999){z.append!(long(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testShortVectorEqual256){
	VectorSP v1 = Util::createVector(DT_SHORT,256,256);
	for(int i=0;i<256;i++){
		v1->set(i,Util::createShort(i));
	}
	string script = "z=array(SHORT,0);for (i in 0..255){z.append!(short(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testFloatVectorMoreThan65535){
	VectorSP v1 = Util::createVector(DT_FLOAT,70000,70000);
	for(int i=0;i<70000;i++){
		v1->set(i,Util::createFloat(i));
	}
	string script = "z=array(FLOAT,0);for (i in 0..69999){z.append!(float(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testDoubleVectorMoreThan65535){
	VectorSP v1 = Util::createVector(DT_DOUBLE,70000,70000);
	for(int i=0;i<70000;i++){
		v1->set(i,Util::createDouble(i));
	}
	string script = "z=array(DOUBLE,0);for (i in 0..69999){z.append!(double(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}


TEST_F(DataformVectorTest,testDatehourVectorMoreThan65535){
	VectorSP v1 = Util::createVector(DT_DATEHOUR,70000,70000);
	for(int i=0;i<70000;i++){
		v1->set(i,Util::createDateHour(i));
	}
	string script = "z=array(DATEHOUR,0);for (i in 0..69999){z.append!(datehour(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testDateVectorMoreThan65535){
	VectorSP v1 = Util::createVector(DT_DATE,70000,70000);
	for(int i=0;i<70000;i++){
		v1->set(i,Util::createDate(i));
	}
	string script = "z=array(DATE,0);for (i in 0..69999){z.append!(date(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testMinuteVectorMoreThan1024){
	VectorSP v1 = Util::createVector(DT_MINUTE,1440,1440);
	for(int i=0;i<1440;i++){
		v1->set(i,Util::createMinute(i));
	}
	string script = "z=array(MINUTE,0);for (i in 0..1439){z.append!(minute(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testDatetimeVectorMoreThan65535){
	VectorSP v1 = Util::createVector(DT_DATETIME,70000,70000);
	for(int i=0;i<70000;i++){
		v1->set(i,Util::createDateTime(i));
	}
	string script = "z=array(DATETIME,0);for (i in 0..69999){z.append!(datetime(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testTimeStampVectorMoreThan65535){
	VectorSP v1 = Util::createVector(DT_TIMESTAMP,70000,70000);
	for(int i=0;i<70000;i++){
		v1->set(i,Util::createTimestamp(i));
	}
	string script = "z=array(TIMESTAMP,0);for (i in 0..69999){z.append!(timestamp(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testnanotimeVectorMoreThan65535){
	VectorSP v1 = Util::createVector(DT_NANOTIME,70000,70000);
	for(int i=0;i<70000;i++){
		v1->set(i,Util::createNanoTime(i));
	}
	string script = "z=array(NANOTIME,0);for (i in 0..69999){z.append!(nanotime(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testnanotimestampVectorMoreThan65535){
	VectorSP v1 = Util::createVector(DT_NANOTIMESTAMP,70000,70000);
	for(int i=0;i<70000;i++){
		v1->set(i,Util::createNanoTimestamp(i));
	}
	string script = "z=array(NANOTIMESTAMP,0);for (i in 0..69999){z.append!(nanotimestamp(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testmonthVectorMoreThan65535){
	VectorSP v1 = Util::createVector(DT_MONTH,70000,70000);
	for(int i=0;i<70000;i++){
		v1->set(i,Util::createMonth(i));
	}
	string script = "z=array(MONTH,0);for (i in 0..69999){z.append!(month(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testtimeVectorMoreThan65535){
	VectorSP v1 = Util::createVector(DT_TIME,70000,70000);
	for(int i=0;i<70000;i++){
		v1->set(i,Util::createTime(i));
	}
	string script = "z=array(TIME,0);for (i in 0..69999){z.append!(time(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}


TEST_F(DataformVectorTest,testStringVectorMoreThan1048576){
	VectorSP v1 = Util::createVector(DT_STRING,1100000,1100000);
	for(int i=0;i<1100000;i++){
		v1->set(i,Util::createString(to_string(i)));
	}
	string script = "z=array(STRING,0);for (i in 0..1099999){z.append!(string(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testAnyVectorMoreThan1048576){
	VectorSP v1 = Util::createVector(DT_ANY,1100000,1100000);
	for(int i=0;i<1100000;i++){
		v1->set(i,Util::createString(to_string(i)));
	}
	string script = "z=array(ANY,0);for (i in 0..1099999){z.append!(string(i))};z";
	EXPECT_ANY_THROW(VectorSP res_d = conn.run(script));  //any vector only support size <1048576
}

TEST_F(DataformVectorTest,testIntVectorMoreThan1048576){
	VectorSP v1 = Util::createVector(DT_INT,1100000,1100000);
	for(int i=0;i<1100000;i++){
		v1->set(i,Util::createInt(i));
	}
	string script = "z=array(INT,0);for (i in 0..1099999){z.append!(int(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testLongVectorMoreThan1048576){
	VectorSP v1 = Util::createVector(DT_LONG,1100000,1100000);
	for(int i=0;i<1100000;i++){
		v1->set(i,Util::createLong(i));
	}
	string script = "z=array(LONG,0);for (i in 0..1099999){z.append!(long(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testFloatVectorMoreThan1048576){
	VectorSP v1 = Util::createVector(DT_FLOAT,1100000,1100000);
	for(int i=0;i<1100000;i++){
		v1->set(i,Util::createFloat(i));
	}
	string script = "z=array(FLOAT,0);for (i in 0..1099999){z.append!(float(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testDoubleVectorMoreThan1048576){
	VectorSP v1 = Util::createVector(DT_DOUBLE,1100000,1100000);
	for(int i=0;i<1100000;i++){
		v1->set(i,Util::createDouble(i));
	}
	string script = "z=array(DOUBLE,0);for (i in 0..1099999){z.append!(double(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}


TEST_F(DataformVectorTest,testDatehourVectorMoreThan1048576){
	VectorSP v1 = Util::createVector(DT_DATEHOUR,1100000,1100000);
	for(int i=0;i<1100000;i++){
		v1->set(i,Util::createDateHour(i));
	}
	string script = "z=array(DATEHOUR,0);for (i in 0..1099999){z.append!(datehour(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testDateVectorMoreThan1048576){
	VectorSP v1 = Util::createVector(DT_DATE,1100000,1100000);
	for(int i=0;i<1100000;i++){
		v1->set(i,Util::createDate(i));
	}
	string script = "z=array(DATE,0);for (i in 0..1099999){z.append!(date(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testDatetimeVectorMoreThan1048576){
	VectorSP v1 = Util::createVector(DT_DATETIME,1100000,1100000);
	for(int i=0;i<1100000;i++){
		v1->set(i,Util::createDateTime(i));
	}
	string script = "z=array(DATETIME,0);for (i in 0..1099999){z.append!(datetime(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testTimeStampVectorMoreThan1048576){
	VectorSP v1 = Util::createVector(DT_TIMESTAMP,1100000,1100000);
	for(int i=0;i<1100000;i++){
		v1->set(i,Util::createTimestamp(i));
	}
	string script = "z=array(TIMESTAMP,0);for (i in 0..1099999){z.append!(timestamp(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testnanotimeVectorMoreThan1048576){
	VectorSP v1 = Util::createVector(DT_NANOTIME,1100000,1100000);
	for(int i=0;i<1100000;i++){
		v1->set(i,Util::createNanoTime(i));
	}
	string script = "z=array(NANOTIME,0);for (i in 0..1099999){z.append!(nanotime(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testnanotimestampVectorMoreThan1048576){
	VectorSP v1 = Util::createVector(DT_NANOTIMESTAMP,1100000,1100000);
	for(int i=0;i<1100000;i++){
		v1->set(i,Util::createNanoTimestamp(i));
	}
	string script = "z=array(NANOTIMESTAMP,0);for (i in 0..1099999){z.append!(nanotimestamp(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testmonthVectorMoreThan1048576){
	VectorSP v1 = Util::createVector(DT_MONTH,1100000,1100000);
	for(int i=0;i<1100000;i++){
		v1->set(i,Util::createMonth(i));
	}
	string script = "z=array(MONTH,0);for (i in 0..1099999){z.append!(month(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformVectorTest,testtimeVectorMoreThan1048576){
	VectorSP v1 = Util::createVector(DT_TIME,1100000,1100000);
	for(int i=0;i<1100000;i++){
		v1->set(i,Util::createTime(i));
	}
	string script = "z=array(TIME,0);for (i in 0..1099999){z.append!(time(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_d->size());
	string judgestr= "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}