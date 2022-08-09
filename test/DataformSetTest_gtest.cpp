class DataformSetTest:public testing::Test
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


TEST_F(DataformSetTest,testAnySet){
	SetSP v1 = Util::createSet(DT_ANY,5);
	EXPECT_EQ(v1.isNull(),true);
}


TEST_F(DataformSetTest,testStringSet){
	SetSP v1 = Util::createSet(DT_STRING,5);
    v1->append(Util::createString("123abc"));
    v1->append(Util::createString("中文*……%#￥#！a"));
	cout<< v1->getString();
	string script = "a=set([`123abc, '中文*……%#￥#！a']);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(v1->getInstance()->getString(),"set()");
	EXPECT_EQ(v1->getValue()->getString(),"set(\"中文*……%#￥#！a\",\"123abc\")");

	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->get(0)->getString(),res_v->keys()->get(1)->getString());
	EXPECT_EQ(v1->keys()->get(1)->getString(),res_v->keys()->get(0)->getString());

	v1->remove(Util::createString("中文*……%#￥#！a"));
	EXPECT_EQ(v1->getValue()->getString(),"set(\"123abc\")");
	ConstantSP res=Util::createConstant(DT_BOOL);
	ConstantSP res1=Util::createConstant(DT_BOOL);
	v1->contain(Util::createString("123abc"),res);
	EXPECT_TRUE(res->getBool());
	v1->contain(v1,res1);
	EXPECT_FALSE(res1->getBool());

	EXPECT_FALSE(v1->inverse(Util::createString("abc")));
	v1->inverse(v1);
	EXPECT_EQ(v1->getString(),"set()");

	SetSP v2 = Util::createSet(DT_STRING,5);
    v2->append(Util::createString("123abc"));
    v2->append(Util::createString("中文*……%#￥#！a"));
	EXPECT_TRUE(v2->isSuperset(v1));
	EXPECT_FALSE(v1->isSuperset(v2));

	EXPECT_EQ(v2->interaction(Util::createString("123abc"))->getString(),"set(\"123abc\")");
	EXPECT_EQ(v2->interaction(Util::createString("中文*……%#￥#！a"))->getString(),"set(\"中文*……%#￥#！a\")");
	EXPECT_EQ(v2->interaction(Util::createString("444"))->getString(),"set()");
}

TEST_F(DataformSetTest,testStringNullSet){
	SetSP v1 = Util::createSet(DT_STRING,5);
    v1->append(Util::createNullConstant(DT_STRING));
    v1->append(Util::createNullConstant(DT_STRING));
	string script = "a=set([string(NULL), string(NULL)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->getString(),res_v->keys()->getString());
}

TEST_F(DataformSetTest,testBoolSet){
	SetSP v1 = Util::createSet(DT_BOOL,5);
    EXPECT_EQ(v1.isNull(),true);
}

TEST_F(DataformSetTest,testCharSet){
	SetSP v1 = Util::createSet(DT_CHAR,5);
    v1->append(Util::createChar(1));
    v1->append(Util::createChar(0));
	string script = "a=set([char(1), char(0)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getInstance()->getString(),"set()");
	EXPECT_EQ(v1->getValue()->getString(),"set(0,1)");

	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->get(0)->getString(),res_v->keys()->get(1)->getString());
	EXPECT_EQ(v1->keys()->get(1)->getString(),res_v->keys()->get(0)->getString());

	v1->remove(Util::createChar(0));
	EXPECT_EQ(v1->getValue()->getString(),"set(1)");
	ConstantSP res=Util::createConstant(DT_BOOL);
	ConstantSP res1=Util::createConstant(DT_BOOL);
	v1->contain(Util::createChar(1),res);
	EXPECT_TRUE(res->getBool());
	v1->contain(v1,res1);
	EXPECT_FALSE(res1->getBool());

	EXPECT_FALSE(v1->inverse(Util::createChar(2)));
	v1->inverse(v1);
	EXPECT_EQ(v1->getString(),"set()");

	SetSP v2 = Util::createSet(DT_CHAR,5);
    v2->append(Util::createChar(1));
    v2->append(Util::createChar(0));
	EXPECT_TRUE(v2->isSuperset(v1));
	EXPECT_FALSE(v1->isSuperset(v2));

	EXPECT_EQ(v2->interaction(Util::createChar(1))->getString(),"set(1)");
	EXPECT_EQ(v2->interaction(Util::createChar(0))->getString(),"set(0)");
	EXPECT_EQ(v2->interaction(Util::createChar(2))->getString(),"set()");

}

TEST_F(DataformSetTest,testCharNullSet){
	SetSP v1 = Util::createSet(DT_CHAR,5);
    v1->append(Util::createNullConstant(DT_CHAR));
    v1->append(Util::createNullConstant(DT_CHAR));
	string script = "a=set([char(NULL), char(NULL)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->getString(),res_v->keys()->getString());
}

TEST_F(DataformSetTest,testIntSet){
	SetSP v1 = Util::createSet(DT_INT,5);
    v1->append(Util::createInt(1));
    v1->append(Util::createInt(0));
	string script = "a=set([int(1), int(0)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getInstance()->getString(),"set()");
	EXPECT_EQ(v1->getValue()->getString(),"set(0,1)");

	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->get(0)->getString(),res_v->keys()->get(1)->getString());
	EXPECT_EQ(v1->keys()->get(1)->getString(),res_v->keys()->get(0)->getString());

	v1->remove(Util::createInt(0));
	EXPECT_EQ(v1->getValue()->getString(),"set(1)");
	ConstantSP res=Util::createConstant(DT_BOOL);
	ConstantSP res1=Util::createConstant(DT_BOOL);
	v1->contain(Util::createInt(1),res);
	EXPECT_TRUE(res->getBool());
	v1->contain(v1,res1);
	EXPECT_FALSE(res1->getBool());

	EXPECT_FALSE(v1->inverse(Util::createInt(2)));
	v1->inverse(v1);
	EXPECT_EQ(v1->getString(),"set()");

	SetSP v2 = Util::createSet(DT_INT,5);
    v2->append(Util::createInt(1));
    v2->append(Util::createInt(0));
	EXPECT_TRUE(v2->isSuperset(v1));
	EXPECT_FALSE(v1->isSuperset(v2));

	EXPECT_EQ(v2->interaction(Util::createInt(1))->getString(),"set(1)");
	EXPECT_EQ(v2->interaction(Util::createInt(0))->getString(),"set(0)");
	EXPECT_EQ(v2->interaction(Util::createInt(2))->getString(),"set()");
}

TEST_F(DataformSetTest,testIntNullSet){
	SetSP v1 = Util::createSet(DT_INT,5);
    v1->append(Util::createNullConstant(DT_INT));
    v1->append(Util::createNullConstant(DT_INT));
	string script = "a=set([int(NULL), int(NULL)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->getString(),res_v->keys()->getString());
}

TEST_F(DataformSetTest,testLongSet){
	SetSP v1 = Util::createSet(DT_LONG,5);
    v1->append(Util::createLong(1));
    v1->append(Util::createLong(0));
	string script = "a=set([long(1), long(0)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getInstance()->getString(),"set()");
	EXPECT_EQ(v1->getValue()->getString(),"set(0,1)");

	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->get(0)->getString(),res_v->keys()->get(1)->getString());
	EXPECT_EQ(v1->keys()->get(1)->getString(),res_v->keys()->get(0)->getString());

	v1->remove(Util::createLong(0));
	EXPECT_EQ(v1->getValue()->getString(),"set(1)");
	ConstantSP res=Util::createConstant(DT_BOOL);
	ConstantSP res1=Util::createConstant(DT_BOOL);
	v1->contain(Util::createLong(1),res);
	EXPECT_TRUE(res->getBool());
	v1->contain(v1,res1);
	EXPECT_FALSE(res1->getBool());

	EXPECT_FALSE(v1->inverse(Util::createLong(2)));
	v1->inverse(v1);
	EXPECT_EQ(v1->getString(),"set()");

	SetSP v2 = Util::createSet(DT_LONG,5);
    v2->append(Util::createLong(1));
    v2->append(Util::createLong(0));
	EXPECT_TRUE(v2->isSuperset(v1));
	EXPECT_FALSE(v1->isSuperset(v2));

	EXPECT_EQ(v2->interaction(Util::createLong(1))->getString(),"set(1)");
	EXPECT_EQ(v2->interaction(Util::createLong(0))->getString(),"set(0)");
	EXPECT_EQ(v2->interaction(Util::createLong(2))->getString(),"set()");
}

TEST_F(DataformSetTest,testLongNullSet){
	SetSP v1 = Util::createSet(DT_LONG,5);
    v1->append(Util::createNullConstant(DT_LONG));
    v1->append(Util::createNullConstant(DT_LONG));
	string script = "a=set([long(NULL), long(NULL)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->getString(),res_v->keys()->getString());
}

TEST_F(DataformSetTest,testShortNullSet){
	SetSP v1 = Util::createSet(DT_SHORT,5);
    v1->append(Util::createNullConstant(DT_SHORT));
    v1->append(Util::createNullConstant(DT_SHORT));
	string script = "a=set([short(NULL), short(NULL)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->getString(),res_v->keys()->getString());
}

TEST_F(DataformSetTest,testShortSet){
	SetSP v1 = Util::createSet(DT_SHORT,5);
    v1->append(Util::createShort(1));
    v1->append(Util::createShort(0));
	string script = "a=set([short(1), short(0)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getInstance()->getString(),"set()");
	EXPECT_EQ(v1->getValue()->getString(),"set(0,1)");

	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->get(0)->getString(),res_v->keys()->get(1)->getString());
	EXPECT_EQ(v1->keys()->get(1)->getString(),res_v->keys()->get(0)->getString());

	v1->remove(Util::createShort(0));
	EXPECT_EQ(v1->getValue()->getString(),"set(1)");
	ConstantSP res=Util::createConstant(DT_BOOL);
	ConstantSP res1=Util::createConstant(DT_BOOL);
	v1->contain(Util::createShort(1),res);
	EXPECT_TRUE(res->getBool());
	v1->contain(v1,res1);
	EXPECT_FALSE(res1->getBool());
	EXPECT_FALSE(v1->inverse(Util::createShort(2)));
	v1->inverse(v1);
	EXPECT_EQ(v1->getString(),"set()");

	SetSP v2 = Util::createSet(DT_SHORT,5);
    v2->append(Util::createShort(1));
    v2->append(Util::createShort(0));
	EXPECT_TRUE(v2->isSuperset(v1));
	EXPECT_FALSE(v1->isSuperset(v2));

	EXPECT_EQ(v2->interaction(Util::createShort(1))->getString(),"set(1)");
	EXPECT_EQ(v2->interaction(Util::createShort(0))->getString(),"set(0)");
	EXPECT_EQ(v2->interaction(Util::createShort(2))->getString(),"set()");
}


TEST_F(DataformSetTest,testFloatSet){
	SetSP v1 = Util::createSet(DT_FLOAT,5);
    v1->append(Util::createFloat(1));
    v1->append(Util::createFloat(0));
	string script = "a=set([float(1), float(0)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getInstance()->getString(),"set()");
	EXPECT_EQ(v1->getValue()->getString(),"set(0,1)");

	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->get(0)->getString(),res_v->keys()->get(1)->getString());
	EXPECT_EQ(v1->keys()->get(1)->getString(),res_v->keys()->get(0)->getString());

	v1->remove(Util::createFloat(0));
	EXPECT_EQ(v1->getValue()->getString(),"set(1)");
	ConstantSP res=Util::createConstant(DT_BOOL);
	ConstantSP res1=Util::createConstant(DT_BOOL);
	v1->contain(Util::createFloat(1),res);
	EXPECT_TRUE(res->getBool());
	v1->contain(v1,res1);
	EXPECT_FALSE(res1->getBool());
	EXPECT_FALSE(v1->inverse(Util::createFloat(2)));
	v1->inverse(v1);
	EXPECT_EQ(v1->getString(),"set()");

	SetSP v2 = Util::createSet(DT_FLOAT,5);
    v2->append(Util::createFloat(1));
    v2->append(Util::createFloat(0));
	EXPECT_TRUE(v2->isSuperset(v1));
	EXPECT_FALSE(v1->isSuperset(v2));

	EXPECT_EQ(v2->interaction(Util::createFloat(1))->getString(),"set(1)");
	EXPECT_EQ(v2->interaction(Util::createFloat(0))->getString(),"set(0)");
	EXPECT_EQ(v2->interaction(Util::createFloat(2))->getString(),"set()");
}


TEST_F(DataformSetTest,testFloatNullSet){
	SetSP v1 = Util::createSet(DT_FLOAT,5);
    v1->append(Util::createNullConstant(DT_FLOAT));
    v1->append(Util::createNullConstant(DT_FLOAT));
	string script = "a=set([float(NULL), float(NULL)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->getString(),res_v->keys()->getString());
}

TEST_F(DataformSetTest,testDoubleSet){
	SetSP v1 = Util::createSet(DT_DOUBLE,5);
    v1->append(Util::createDouble(1));
    v1->append(Util::createDouble(0));
	string script = "a=set([double(1), double(0)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getInstance()->getString(),"set()");
	EXPECT_EQ(v1->getValue()->getString(),"set(0,1)");

	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->get(0)->getString(),res_v->keys()->get(1)->getString());
	EXPECT_EQ(v1->keys()->get(1)->getString(),res_v->keys()->get(0)->getString());

	v1->remove(Util::createDouble(0));
	EXPECT_EQ(v1->getValue()->getString(),"set(1)");
	ConstantSP res=Util::createConstant(DT_BOOL);
	ConstantSP res1=Util::createConstant(DT_BOOL);
	v1->contain(Util::createDouble(1),res);
	EXPECT_TRUE(res->getBool());
	v1->contain(v1,res1);
	EXPECT_FALSE(res1->getBool());
	EXPECT_FALSE(v1->inverse(Util::createDouble(2)));
	v1->inverse(v1);
	EXPECT_EQ(v1->getString(),"set()");

	SetSP v2 = Util::createSet(DT_DOUBLE,5);
    v2->append(Util::createDouble(1));
    v2->append(Util::createDouble(0));
	EXPECT_TRUE(v2->isSuperset(v1));
	EXPECT_FALSE(v1->isSuperset(v2));

	EXPECT_EQ(v2->interaction(Util::createDouble(1))->getString(),"set(1)");
	EXPECT_EQ(v2->interaction(Util::createDouble(0))->getString(),"set(0)");
	EXPECT_EQ(v2->interaction(Util::createDouble(2))->getString(),"set()");
}


TEST_F(DataformSetTest,testDoubleNullSet){
	SetSP v1 = Util::createSet(DT_DOUBLE,5);
    v1->append(Util::createNullConstant(DT_DOUBLE));
    v1->append(Util::createNullConstant(DT_DOUBLE));
	string script = "a=set([double(NULL), double(NULL)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->getString(),res_v->keys()->getString());
}

TEST_F(DataformSetTest,testDatehourSet){
	SetSP v1 = Util::createSet(DT_DATEHOUR,5);
    v1->append(Util::createDateHour(1));
    v1->append(Util::createDateHour(100000));
	string script = "a=set([datehour(1), datehour(100000)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getInstance()->getString(),"set()");
	EXPECT_EQ(v1->getValue()->getString(),"set(1981.05.29T16,1970.01.01T01)");

	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->get(0)->getString(),res_v->keys()->get(1)->getString());
	EXPECT_EQ(v1->keys()->get(1)->getString(),res_v->keys()->get(0)->getString());

	v1->remove(Util::createDateHour(1));
	EXPECT_EQ(v1->getValue()->getString(),"set(1981.05.29T16)");
	ConstantSP res=Util::createConstant(DT_BOOL);
	ConstantSP res1=Util::createConstant(DT_BOOL);
	v1->contain(Util::createDateHour(100000),res);
	EXPECT_TRUE(res->getBool());
	v1->contain(v1,res1);
	EXPECT_FALSE(res1->getBool());
	EXPECT_FALSE(v1->inverse(Util::createDateHour(2)));
	v1->inverse(v1);
	EXPECT_EQ(v1->getString(),"set()");

	SetSP v2 = Util::createSet(DT_DATEHOUR,5);
    v2->append(Util::createDateHour(1));
    v2->append(Util::createDateHour(100000));
	EXPECT_TRUE(v2->isSuperset(v1));
	EXPECT_FALSE(v1->isSuperset(v2));

	EXPECT_EQ(v2->interaction(Util::createDateHour(100000))->getString(),"set(1981.05.29T16)");
	EXPECT_EQ(v2->interaction(Util::createDateHour(1))->getString(),"set(1970.01.01T01)");
	EXPECT_EQ(v2->interaction(Util::createDateHour(2))->getString(),"set()");
}

TEST_F(DataformSetTest,testDatehourNullSet){
	SetSP v1 = Util::createSet(DT_DATEHOUR,5);
    v1->append(Util::createNullConstant(DT_DATEHOUR));
    v1->append(Util::createNullConstant(DT_DATEHOUR));
	string script = "a=set([datehour(NULL), datehour(NULL)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->getString(),res_v->keys()->getString());
}

TEST_F(DataformSetTest,testDateSet){
	SetSP v1 = Util::createSet(DT_DATE,5);
    v1->append(Util::createDate(1));
    v1->append(Util::createDate(48750));
	string script = "a=set([date(1), date(48750)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getInstance()->getString(),"set()");
	EXPECT_EQ(v1->getValue()->getString(),"set(2103.06.23,1970.01.02)");

	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->get(0)->getString(),res_v->keys()->get(1)->getString());
	EXPECT_EQ(v1->keys()->get(1)->getString(),res_v->keys()->get(0)->getString());

	v1->remove(Util::createDate(1));
	EXPECT_EQ(v1->getValue()->getString(),"set(2103.06.23)");
	ConstantSP res=Util::createConstant(DT_BOOL);
	ConstantSP res1=Util::createConstant(DT_BOOL);
	v1->contain(Util::createDate(48750),res);
	EXPECT_TRUE(res->getBool());
	v1->contain(v1,res1);
	EXPECT_FALSE(res1->getBool());
	EXPECT_FALSE(v1->inverse(Util::createDate(2)));
	v1->inverse(v1);
	EXPECT_EQ(v1->getString(),"set()");

	SetSP v2 = Util::createSet(DT_DATE,5);
    v2->append(Util::createDate(1));
    v2->append(Util::createDate(48750));
	EXPECT_TRUE(v2->isSuperset(v1));
	EXPECT_FALSE(v1->isSuperset(v2));

	EXPECT_EQ(v2->interaction(Util::createDate(48750))->getString(),"set(2103.06.23)");
	EXPECT_EQ(v2->interaction(Util::createDate(1))->getString(),"set(1970.01.02)");
	EXPECT_EQ(v2->interaction(Util::createDate(2))->getString(),"set()");
}

TEST_F(DataformSetTest,testDatenullSet){
	SetSP v1 = Util::createSet(DT_DATE,5);
    v1->append(Util::createNullConstant(DT_DATE));
    v1->append(Util::createNullConstant(DT_DATE));
	string script = "a=set([date(NULL), date(NULL)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->getString(),res_v->keys()->getString());
}

TEST_F(DataformSetTest,testMinuteSet){
	SetSP v1 = Util::createSet(DT_MINUTE,5);
    v1->append(Util::createMinute(1));
    v1->append(Util::createMinute(1000));
	string script = "a=set([minute(1), minute(1000)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getInstance()->getString(),"set()");
	EXPECT_EQ(v1->getValue()->getString(),"set(16:40m,00:01m)");

	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->get(0)->getString(),res_v->keys()->get(1)->getString());
	EXPECT_EQ(v1->keys()->get(1)->getString(),res_v->keys()->get(0)->getString());

	v1->remove(Util::createMinute(1));
	EXPECT_EQ(v1->getValue()->getString(),"set(16:40m)");
	ConstantSP res=Util::createConstant(DT_BOOL);
	ConstantSP res1=Util::createConstant(DT_BOOL);
	v1->contain(Util::createMinute(1000),res);
	EXPECT_TRUE(res->getBool());
	v1->contain(v1,res1);
	EXPECT_FALSE(res1->getBool());
	EXPECT_FALSE(v1->inverse(Util::createMinute(2)));
	v1->inverse(v1);
	EXPECT_EQ(v1->getString(),"set()");

	SetSP v2 = Util::createSet(DT_MINUTE,5);
    v2->append(Util::createMinute(1));
    v2->append(Util::createMinute(1000));
	EXPECT_TRUE(v2->isSuperset(v1));
	EXPECT_FALSE(v1->isSuperset(v2));

	EXPECT_EQ(v2->interaction(Util::createMinute(1000))->getString(),"set(16:40m)");
	EXPECT_EQ(v2->interaction(Util::createMinute(1))->getString(),"set(00:01m)");
	EXPECT_EQ(v2->interaction(Util::createMinute(2))->getString(),"set()");
}

TEST_F(DataformSetTest,testMinutenullSet){
	SetSP v1 = Util::createSet(DT_MINUTE,5);
    v1->append(Util::createNullConstant(DT_MINUTE));
    v1->append(Util::createNullConstant(DT_MINUTE));
	string script = "a=set([minute(NULL), minute(NULL)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());

    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->getString(),res_v->keys()->getString());
}

TEST_F(DataformSetTest,testDatetimeSet){
	SetSP v1 = Util::createSet(DT_DATETIME,5);
    v1->append(Util::createDateTime(1));
    v1->append(Util::createDateTime(48750));
	string script = "a=set([datetime(1), datetime(48750)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getInstance()->getString(),"set()");
	EXPECT_EQ(v1->getValue()->getString(),"set(1970.01.01T13:32:30,1970.01.01T00:00:01)");

	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->get(0)->getString(),res_v->keys()->get(1)->getString());
	EXPECT_EQ(v1->keys()->get(1)->getString(),res_v->keys()->get(0)->getString());

	v1->remove(Util::createDateTime(1));
	EXPECT_EQ(v1->getValue()->getString(),"set(1970.01.01T13:32:30)");
	ConstantSP res=Util::createConstant(DT_BOOL);
	ConstantSP res1=Util::createConstant(DT_BOOL);
	v1->contain(Util::createDateTime(48750),res);
	EXPECT_TRUE(res->getBool());
	v1->contain(v1,res1);
	EXPECT_FALSE(res1->getBool());
	EXPECT_FALSE(v1->inverse(Util::createDateTime(2)));
	v1->inverse(v1);
	EXPECT_EQ(v1->getString(),"set()");

	SetSP v2 = Util::createSet(DT_DATETIME,5);
    v2->append(Util::createDateTime(1));
    v2->append(Util::createDateTime(48750));
	EXPECT_TRUE(v2->isSuperset(v1));
	EXPECT_FALSE(v1->isSuperset(v2));

	EXPECT_EQ(v2->interaction(Util::createDateTime(48750))->getString(),"set(1970.01.01T13:32:30)");
	EXPECT_EQ(v2->interaction(Util::createDateTime(1))->getString(),"set(1970.01.01T00:00:01)");
	EXPECT_EQ(v2->interaction(Util::createDateTime(2))->getString(),"set()");
}

TEST_F(DataformSetTest,testDatetimenullSet){
	SetSP v1 = Util::createSet(DT_DATETIME,5);
    v1->append(Util::createNullConstant(DT_DATETIME));
    v1->append(Util::createNullConstant(DT_DATETIME));
	string script = "a=set([datetime(NULL), datetime(NULL)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->getString(),res_v->keys()->getString());
}

TEST_F(DataformSetTest,testTimeStampSet){
	SetSP v1 = Util::createSet(DT_TIMESTAMP,5);
    v1->append(Util::createTimestamp(1));
    v1->append(Util::createTimestamp(1000000000000));
	string script = "a=set([timestamp(1), timestamp(1000000000000)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getInstance()->getString(),"set()");
	EXPECT_EQ(v1->getValue()->getString(),"set(2001.09.09T01:46:40.000,1970.01.01T00:00:00.001)");

	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->get(0)->getString(),res_v->keys()->get(1)->getString());
	EXPECT_EQ(v1->keys()->get(1)->getString(),res_v->keys()->get(0)->getString());

	v1->remove(Util::createTimestamp(1));
	EXPECT_EQ(v1->getValue()->getString(),"set(2001.09.09T01:46:40.000)");
	ConstantSP res=Util::createConstant(DT_BOOL);
	ConstantSP res1=Util::createConstant(DT_BOOL);
	cout<<v1->getString()<<endl;
	cout<<Util::createTimestamp(1000000000000)->getString()<<endl;
	v1->contain(Util::createTimestamp(1000000000000),res);
	EXPECT_TRUE(res->getBool());
	v1->contain(v1,res1);
	EXPECT_FALSE(res1->getBool());
	EXPECT_FALSE(v1->inverse(Util::createTimestamp(2)));
	v1->inverse(v1);
	EXPECT_EQ(v1->getString(),"set()");

	SetSP v2 = Util::createSet(DT_TIMESTAMP,5);
    v2->append(Util::createTimestamp(1));
    v2->append(Util::createTimestamp(1000000000000));
	EXPECT_TRUE(v2->isSuperset(v1));
	EXPECT_FALSE(v1->isSuperset(v2));

	EXPECT_EQ(v2->interaction(Util::createTimestamp(1))->getString(),"set(1970.01.01T00:00:00.001)");
	EXPECT_EQ(v2->interaction(Util::createTimestamp(1000000000000))->getString(),"set(2001.09.09T01:46:40.000)");
	EXPECT_EQ(v2->interaction(Util::createTimestamp(2))->getString(),"set()");
}

TEST_F(DataformSetTest,testTimeStampnullSet){
	SetSP v1 = Util::createSet(DT_TIMESTAMP,5);
    v1->append(Util::createNullConstant(DT_TIMESTAMP));
    v1->append(Util::createNullConstant(DT_TIMESTAMP));
	string script = "a=set([timestamp(NULL), timestamp(NULL)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->getString(),res_v->keys()->getString());
}

TEST_F(DataformSetTest,testnanotimeSet){
	SetSP v1 = Util::createSet(DT_NANOTIME,5);
    v1->append(Util::createNanoTime(1));
    v1->append(Util::createNanoTime(1000000000000));
	string script = "a=set([nanotime(1), nanotime(1000000000000)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getInstance()->getString(),"set()");
	EXPECT_EQ(v1->getValue()->getString(),"set(00:16:40.000000000,00:00:00.000000001)");

	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->get(0)->getString(),res_v->keys()->get(1)->getString());
	EXPECT_EQ(v1->keys()->get(1)->getString(),res_v->keys()->get(0)->getString());

	v1->remove(Util::createNanoTime(1));
	EXPECT_EQ(v1->getValue()->getString(),"set(00:16:40.000000000)");
	ConstantSP res=Util::createConstant(DT_BOOL);
	ConstantSP res1=Util::createConstant(DT_BOOL);
	v1->contain(Util::createNanoTime(1000000000000),res);
	EXPECT_TRUE(res->getBool());
	v1->contain(v1,res1);
	EXPECT_FALSE(res1->getBool());
	EXPECT_FALSE(v1->inverse(Util::createNanoTime(2)));
	v1->inverse(v1);
	EXPECT_EQ(v1->getString(),"set()");

	SetSP v2 = Util::createSet(DT_NANOTIME,5);
    v2->append(Util::createNanoTime(1));
    v2->append(Util::createNanoTime(1000000000000));
	EXPECT_TRUE(v2->isSuperset(v1));
	EXPECT_FALSE(v1->isSuperset(v2));

	EXPECT_EQ(v2->interaction(Util::createNanoTime(1000000000000))->getString(),"set(00:16:40.000000000)");
	EXPECT_EQ(v2->interaction(Util::createNanoTime(1))->getString(),"set(00:00:00.000000001)");
	EXPECT_EQ(v2->interaction(Util::createNanoTime(2))->getString(),"set()");
}

TEST_F(DataformSetTest,testnanotimenullSet){
	SetSP v1 = Util::createSet(DT_NANOTIME,5);
    v1->append(Util::createNullConstant(DT_NANOTIME));
    v1->append(Util::createNullConstant(DT_NANOTIME));
	string script = "a=set([nanotime(NULL), nanotime(NULL)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->getString(),res_v->keys()->getString());
}

TEST_F(DataformSetTest,testnanotimestampSet){
	SetSP v1 = Util::createSet(DT_NANOTIMESTAMP,5);
    v1->append(Util::createNanoTimestamp(1));
    v1->append(Util::createNanoTimestamp(1000000000000));
	string script = "a=set([nanotimestamp(1), nanotimestamp(1000000000000)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getInstance()->getString(),"set()");
	EXPECT_EQ(v1->getValue()->getString(),"set(1970.01.01T00:16:40.000000000,1970.01.01T00:00:00.000000001)");

	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->get(0)->getString(),res_v->keys()->get(1)->getString());
	EXPECT_EQ(v1->keys()->get(1)->getString(),res_v->keys()->get(0)->getString());

	v1->remove(Util::createNanoTimestamp(1));
	EXPECT_EQ(v1->getValue()->getString(),"set(1970.01.01T00:16:40.000000000)");
	ConstantSP res=Util::createConstant(DT_BOOL);
	ConstantSP res1=Util::createConstant(DT_BOOL);
	v1->contain(Util::createNanoTimestamp(1000000000000),res);
	EXPECT_TRUE(res->getBool());
	v1->contain(v1,res1);
	EXPECT_FALSE(res1->getBool());
	EXPECT_FALSE(v1->inverse(Util::createNanoTimestamp(2)));
	v1->inverse(v1);
	EXPECT_EQ(v1->getString(),"set()");

	SetSP v2 = Util::createSet(DT_NANOTIMESTAMP,5);
    v2->append(Util::createNanoTimestamp(1));
    v2->append(Util::createNanoTimestamp(1000000000000));
	EXPECT_TRUE(v2->isSuperset(v1));
	EXPECT_FALSE(v1->isSuperset(v2));

	EXPECT_EQ(v2->interaction(Util::createNanoTimestamp(1000000000000))->getString(),"set(1970.01.01T00:16:40.000000000)");
	EXPECT_EQ(v2->interaction(Util::createNanoTimestamp(1))->getString(),"set(1970.01.01T00:00:00.000000001)");
	EXPECT_EQ(v2->interaction(Util::createNanoTimestamp(2))->getString(),"set()");
}

TEST_F(DataformSetTest,testnanotimestampnullSet){
	SetSP v1 = Util::createSet(DT_NANOTIMESTAMP,5);
    v1->append(Util::createNullConstant(DT_NANOTIMESTAMP));
    v1->append(Util::createNullConstant(DT_NANOTIMESTAMP));
	string script = "a=set([nanotimestamp(NULL), nanotimestamp(NULL)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->getString(),res_v->keys()->getString());
}

TEST_F(DataformSetTest,testmonthSet){
	SetSP v1 = Util::createSet(DT_MONTH,5);
    v1->append(Util::createMonth(1));
    v1->append(Util::createMonth(1000));
	string script = "a=set([month(1), month(1000)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getInstance()->getString(),"set()");
	EXPECT_EQ(v1->getValue()->getString(),"set(0083.05M,0000.02M)");

	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->get(0)->getString(),res_v->keys()->get(1)->getString());
	EXPECT_EQ(v1->keys()->get(1)->getString(),res_v->keys()->get(0)->getString());

	v1->remove(Util::createMonth(1));
	EXPECT_EQ(v1->getValue()->getString(),"set(0083.05M)");
	ConstantSP res=Util::createConstant(DT_BOOL);
	ConstantSP res1=Util::createConstant(DT_BOOL);
	v1->contain(Util::createMonth(1000),res);
	EXPECT_TRUE(res->getBool());
	v1->contain(v1,res1);
	EXPECT_FALSE(res1->getBool());
	EXPECT_FALSE(v1->inverse(Util::createMonth(2)));
	v1->inverse(v1);
	EXPECT_EQ(v1->getString(),"set()");

	SetSP v2 = Util::createSet(DT_MONTH,5);
    v2->append(Util::createMonth(1));
    v2->append(Util::createMonth(1000));
	EXPECT_TRUE(v2->isSuperset(v1));
	EXPECT_FALSE(v1->isSuperset(v2));

	EXPECT_EQ(v2->interaction(Util::createMonth(1))->getString(),"set(0000.02M)");
	EXPECT_EQ(v2->interaction(Util::createMonth(1000))->getString(),"set(0083.05M)");
	EXPECT_EQ(v2->interaction(Util::createMonth(2))->getString(),"set()");
}

TEST_F(DataformSetTest,testmonthnullSet){
	SetSP v1 = Util::createSet(DT_MONTH,5);
    v1->append(Util::createNullConstant(DT_MONTH));
    v1->append(Util::createNullConstant(DT_MONTH));
	string script = "a=set([month(NULL), month(NULL)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->getString(),res_v->keys()->getString());
}

TEST_F(DataformSetTest,testtimeSet){
	SetSP v1 = Util::createSet(DT_TIME,5);
    v1->append(Util::createTime(1));
    v1->append(Util::createTime((long)10000000));
	string script = "a=set([time(1), time(10000000)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getInstance()->getString(),"set()");
	EXPECT_EQ(v1->getValue()->getString(),"set(02:46:40.000,00:00:00.001)");

	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->get(0)->getString(),res_v->keys()->get(1)->getString());
	EXPECT_EQ(v1->keys()->get(1)->getString(),res_v->keys()->get(0)->getString());

	v1->remove(Util::createTime(1));
	EXPECT_EQ(v1->getValue()->getString(),"set(02:46:40.000)");
	ConstantSP res=Util::createConstant(DT_BOOL);
	ConstantSP res1=Util::createConstant(DT_BOOL);
	v1->contain(Util::createTime(10000000),res);
	EXPECT_TRUE(res->getBool());
	v1->contain(v1,res1);
	EXPECT_FALSE(res1->getBool());
	EXPECT_FALSE(v1->inverse(Util::createTime(2)));
	v1->inverse(v1);
	EXPECT_EQ(v1->getString(),"set()");
	
	SetSP v2 = Util::createSet(DT_TIME,5);
    v2->append(Util::createTime(1));
    v2->append(Util::createTime(10000000));
	EXPECT_TRUE(v2->isSuperset(v1));
	EXPECT_FALSE(v1->isSuperset(v2));

	EXPECT_EQ(v2->interaction(Util::createTime(10000000))->getString(),"set(02:46:40.000)");
	EXPECT_EQ(v2->interaction(Util::createTime(1))->getString(),"set(00:00:00.001)");
	EXPECT_EQ(v2->interaction(Util::createTime(2))->getString(),"set()");
}

TEST_F(DataformSetTest,testtimenullSet){
	SetSP v1 = Util::createSet(DT_TIME,5);
    v1->append(Util::createNullConstant(DT_TIME));
    v1->append(Util::createNullConstant(DT_TIME));
	string script = "a=set([time(NULL), time(NULL)]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->getString(),res_v->keys()->getString());
}

TEST_F(DataformSetTest,testint128Set){
	SetSP v1 = Util::createSet(DT_INT128,5);
	ConstantSP int128val=Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec32");
    v1->append(int128val);
    v1->append(Util::createNullConstant(DT_INT128));
	string script = "a=set([int128(`e1671797c52e15f763380b45e841ec32), NULL]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getInstance()->getString(),"set()");
	EXPECT_EQ(v1->getValue()->getString(),"set(00000000000000000000000000000000,e1671797c52e15f763380b45e841ec32)");

	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->get(0)->getString(),res_v->keys()->get(1)->getString());
	EXPECT_EQ(v1->keys()->get(1)->getString(),res_v->keys()->get(0)->getString());

	v1->remove(Util::createNullConstant(DT_INT128));
	EXPECT_EQ(v1->getValue()->getString(),"set(e1671797c52e15f763380b45e841ec32)");
	ConstantSP res=Util::createConstant(DT_BOOL);
	ConstantSP res1=Util::createConstant(DT_BOOL);
	v1->contain(int128val,res);
	EXPECT_TRUE(res->getBool());
	v1->contain(v1,res1);
	EXPECT_FALSE(res1->getBool());
	EXPECT_FALSE(v1->inverse(int128val));
	v1->inverse(v1);
	EXPECT_EQ(v1->getString(),"set()");
	
	SetSP v2 = Util::createSet(DT_INT128,5);
    v2->append(Util::createNullConstant(DT_INT128));
    v2->append(int128val);
	EXPECT_TRUE(v2->isSuperset(v1));
	EXPECT_FALSE(v1->isSuperset(v2));

	EXPECT_EQ(v2->interaction(int128val)->getString(),"set(e1671797c52e15f763380b45e841ec32)");
	EXPECT_EQ(v2->interaction(Util::createNullConstant(DT_INT128))->getString(),"set(00000000000000000000000000000000)");
	EXPECT_EQ(v2->interaction(Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec33"))->getString(),"set()");
}

TEST_F(DataformSetTest,testipaddrSet){
	SetSP v1 = Util::createSet(DT_IP,5);
	ConstantSP ipaddrval=Util::parseConstant(DT_IP,"192.168.1.13");
    v1->append(ipaddrval);
    v1->append(Util::createNullConstant(DT_IP));
	string script = "a=set([ipaddr(`192.168.1.13), NULL]);a";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(v1->getInstance()->getString(),"set()");
	EXPECT_EQ(v1->getValue()->getString(),"set(0.0.0.0,192.168.1.13)");

	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isSet(),true);
	EXPECT_EQ(v1->sizeable(),true);
	EXPECT_EQ(v1->keys()->get(0)->getString(),res_v->keys()->get(1)->getString());
	EXPECT_EQ(v1->keys()->get(1)->getString(),res_v->keys()->get(0)->getString());

	v1->remove(Util::createNullConstant(DT_IP));
	EXPECT_EQ(v1->getValue()->getString(),"set(192.168.1.13)");
	ConstantSP res=Util::createConstant(DT_BOOL);
	ConstantSP res1=Util::createConstant(DT_BOOL);
	v1->contain(ipaddrval,res);
	EXPECT_TRUE(res->getBool());
	v1->contain(v1,res1);
	EXPECT_FALSE(res1->getBool());
	EXPECT_FALSE(v1->inverse(ipaddrval));
	v1->inverse(v1);
	EXPECT_EQ(v1->getString(),"set()");
	
	SetSP v2 = Util::createSet(DT_IP,5);
    v2->append(Util::createNullConstant(DT_IP));
    v2->append(ipaddrval);
	EXPECT_TRUE(v2->isSuperset(v1));
	EXPECT_FALSE(v1->isSuperset(v2));

	EXPECT_EQ(v2->interaction(ipaddrval)->getString(),"set(192.168.1.13)");
	EXPECT_EQ(v2->interaction(Util::createNullConstant(DT_IP))->getString(),"set(0.0.0.0)");
	EXPECT_EQ(v2->interaction(Util::parseConstant(DT_IP,"192.168.1.14"))->getString(),"set()");
}

TEST_F(DataformSetTest,testCharSetEqual128){
	SetSP v1 = Util::createSet(DT_CHAR,128);
	for(int i=0;i<128;i++)
    	v1->append(Util::createChar(i));
	string script = "z=set(char(0..127));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(v1->getScript(),res_v->getScript());
}

TEST_F(DataformSetTest,testShortSetEqual256){
	SetSP v1 = Util::createSet(DT_SHORT,256);
	for(int i=0;i<256;i++)
    	v1->append(Util::createShort(i));
	string script = "z=set(short(0..255));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testIntSetMoreThan65535){
	SetSP v1 = Util::createSet(DT_INT,70000);
	for(int i=0;i<70000;i++)
    	v1->append(Util::createInt(i));
	string script = "z=set(int(0..69999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testLongSetMoreThan65535){
	SetSP v1 = Util::createSet(DT_LONG,70000);
	for(int i=0;i<70000;i++)
    	v1->append(Util::createLong(i));
	string script = "z=set(long(0..69999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testDateSetMoreThan65535){
	SetSP v1 = Util::createSet(DT_DATE,70000);
	for(int i=0;i<70000;i++)
    	v1->append(Util::createDate(i));
	string script = "z=set(date(0..69999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testMonthSetMoreThan65535){
	SetSP v1 = Util::createSet(DT_MONTH,70000);
	for(int i=0;i<70000;i++)
    	v1->append(Util::createMonth(i));
	string script = "z=set(month(0..69999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}


TEST_F(DataformSetTest,testTimeSetMoreThan65535){
	SetSP v1 = Util::createSet(DT_TIME,70000);
	for(int i=0;i<70000;i++)
    	v1->append(Util::createTime(i));
	string script = "z=set(time(0..69999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testMinuteSetEqual1440){
	SetSP v1 = Util::createSet(DT_MINUTE,1440);
	for(int i=0;i<1440;i++)
    	v1->append(Util::createMinute(i));
	string script = "z=set(minute(0..1439));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testSecondSetMoreThan65535){
	SetSP v1 = Util::createSet(DT_SECOND,70000);
	for(int i=0;i<70000;i++)
    	v1->append(Util::createSecond(i));
	string script = "z=set(second(0..69999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testDatetimeSetMoreThan65535){
	SetSP v1 = Util::createSet(DT_DATETIME,70000);
	for(int i=0;i<70000;i++)
    	v1->append(Util::createDateTime(i));
	string script = "z=set(datetime(0..69999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testTimestampSetMoreThan65535){
	SetSP v1 = Util::createSet(DT_TIMESTAMP,70000);
	for(int i=0;i<70000;i++)
    	v1->append(Util::createTimestamp(i));
	string script = "z=set(timestamp(0..69999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testNanotimeSetMoreThan65535){
	SetSP v1 = Util::createSet(DT_NANOTIME,70000);
	for(int i=0;i<70000;i++)
    	v1->append(Util::createNanoTime(i));
	string script = "z=set(nanotime(0..69999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testNanotimestampSetMoreThan65535){
	SetSP v1 = Util::createSet(DT_NANOTIMESTAMP,70000);
	for(int i=0;i<70000;i++)
    	v1->append(Util::createNanoTimestamp(i));
	string script = "z=set(nanotimestamp(0..69999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testFloatSetMoreThan65535){
	SetSP v1 = Util::createSet(DT_FLOAT,70000);
	for(int i=0;i<70000;i++)
    	v1->append(Util::createFloat(i));
	string script = "z=set(float(0..69999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testDoubleSetMoreThan65535){
	SetSP v1 = Util::createSet(DT_DOUBLE,70000);
	for(int i=0;i<70000;i++)
    	v1->append(Util::createDouble(i));
	string script = "z=set(double(0..69999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testStringSetMoreThan65535){
	SetSP v1 = Util::createSet(DT_STRING,70000);
	for(int i=0;i<70000;i++)
    	v1->append(Util::createString(to_string(i)));
	string script = "z=set(string(0..69999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testDatehourSetMoreThan65535){
	SetSP v1 = Util::createSet(DT_DATEHOUR,70000);
	for(int i=0;i<70000;i++)
    	v1->append(Util::createDateHour(i));
	string script = "z=set(datehour(0..69999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testIntSetMoreThan1048576){
	SetSP v1 = Util::createSet(DT_INT,1100000);
	for(int i=0;i<1100000;i++)
    	v1->append(Util::createInt(i));
	string script = "z=set(int(0..1099999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testLongSetMoreThan1048576){
	SetSP v1 = Util::createSet(DT_LONG,1100000);
	for(int i=0;i<1100000;i++)
    	v1->append(Util::createLong(i));
	string script = "z=set(long(0..1099999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testDateSetMoreThan1048576){
	SetSP v1 = Util::createSet(DT_DATE,1100000);
	for(int i=0;i<1100000;i++)
    	v1->append(Util::createDate(i));
	string script = "z=set(date(0..1099999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testMonthSetMoreThan1048576){
	SetSP v1 = Util::createSet(DT_MONTH,1100000);
	for(int i=0;i<1100000;i++)
    	v1->append(Util::createMonth(i));
	string script = "z=set(month(0..1099999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}


TEST_F(DataformSetTest,testTimeSetMoreThan1048576){
	SetSP v1 = Util::createSet(DT_TIME,1100000);
	for(int i=0;i<1100000;i++)
    	v1->append(Util::createTime(i));
	string script = "z=set(time(0..1099999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testSecondSetEqual86400){
	SetSP v1 = Util::createSet(DT_SECOND,86400);
	for(int i=0;i<86400;i++)
    	v1->append(Util::createSecond(i));
	string script = "z=set(second(0..86399));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testDatetimeSetMoreThan1048576){
	SetSP v1 = Util::createSet(DT_DATETIME,1100000);
	for(int i=0;i<1100000;i++)
    	v1->append(Util::createDateTime(i));
	string script = "z=set(datetime(0..1099999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testTimestampSetMoreThan1048576){
	SetSP v1 = Util::createSet(DT_TIMESTAMP,1100000);
	for(int i=0;i<1100000;i++)
    	v1->append(Util::createTimestamp(i));
	string script = "z=set(timestamp(0..1099999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testNanotimeSetMoreThan1048576){
	SetSP v1 = Util::createSet(DT_NANOTIME,1100000);
	for(int i=0;i<1100000;i++)
    	v1->append(Util::createNanoTime(i));
	string script = "z=set(nanotime(0..1099999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testNanotimestampSetMoreThan1048576){
	SetSP v1 = Util::createSet(DT_NANOTIMESTAMP,1100000);
	for(int i=0;i<1100000;i++)
    	v1->append(Util::createNanoTimestamp(i));
	string script = "z=set(nanotimestamp(0..1099999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testFloatSetMoreThan1048576){
	SetSP v1 = Util::createSet(DT_FLOAT,1100000);
	for(int i=0;i<1100000;i++)
    	v1->append(Util::createFloat(i));
	string script = "z=set(float(0..1099999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testDoubleSetMoreThan1048576){
	SetSP v1 = Util::createSet(DT_DOUBLE,1100000);
	for(int i=0;i<1100000;i++)
    	v1->append(Util::createDouble(i));
	string script = "z=set(double(0..1099999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testStringSetMoreThan1048576){
	SetSP v1 = Util::createSet(DT_STRING,1100000);
	for(int i=0;i<1100000;i++)
    	v1->append(Util::createString(to_string(i)));
	string script = "z=set(string(0..1099999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}

TEST_F(DataformSetTest,testDatehourSetMoreThan1048576){
	SetSP v1 = Util::createSet(DT_DATEHOUR,1100000);
	for(int i=0;i<1100000;i++)
    	v1->append(Util::createDateHour(i));
	string script = "z=set(datehour(0..1099999));z";
	SetSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(),res_v->size());
	EXPECT_EQ(conn.run("v1")->getScript(),conn.run("z")->getScript());
}