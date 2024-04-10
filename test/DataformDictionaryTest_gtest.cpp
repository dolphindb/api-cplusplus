#include "config.h"

class DataformDictionaryTest:public testing::Test
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
		try
		{
			ConstantSP res = conn.run("1+1");
		}
		catch(const std::exception& e)
		{
			conn.connect(hostName, port, "admin", "123456");
		}
		
        cout<<"ok"<<endl;
    }
    virtual void TearDown()
    {
        conn.run("undef all;");
    }
};

TEST_F(DataformDictionaryTest,testBlobDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_BLOB, DT_BLOB);
	dict1->set(Util::createBlob("zzz123中文a"), Util::createBlob("*-/%**%#~！#“》（a"));
	string script = "a=blob(\"zzz123中文a\");b=blob(\"*-/%**%#~！#“》（a\");c=dict(BLOB,BLOB);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getScript(), res_d->getScript());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"zzz123中文a");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"*-/%**%#~！#“》（a");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_BLOB);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_BLOB);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_BLOB,0,2);
	valVec->append(Util::createBlob("*-/%**%#~！#“》（a"));
	dict1->contain(Util::createBlob("zzz123中文a"),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	dict1->set(Util::createBlob("demo1"), Util::createBlob("value1"));
	dict1->set(Util::createBlob("demo2"), Util::createBlob("value2"));
	cout<<dict1->getAllocatedMemory()<<endl;
	VectorSP valVec2=Util::createVector(DT_STRING,2,2);
	valVec2->set(0,Util::createBlob("demo2"));
	valVec2->setNull(1);

	EXPECT_EQ(dict1->getMember(Util::createBlob("zzz123中文a"))->getString(),"*-/%**%#~！#“》（a");
	EXPECT_EQ(dict1->getMember(valVec2)->get(0)->getString(),"value2");
	dict1->remove(Util::createBlob("demo1"));
	dict1->remove(valVec2);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"zzz123中文a->*-/%**%#~！#“》（a\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testStringDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_STRING, DT_STRING);
	dict1->set(Util::createString("zzz123中文a"), Util::createString("*-/%**%#~！#“》（a"));
	string script = "a=\"zzz123中文a\";b=\"*-/%**%#~！#“》（a\";c=dict(STRING,STRING);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getScript(), res_d->getScript());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"zzz123中文a");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"*-/%**%#~！#“》（a");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_STRING);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_STRING);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_STRING,0,2);
	valVec->append(Util::createString("*-/%**%#~！#“》（a"));
	dict1->contain(Util::createString("zzz123中文a"),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	dict1->set(Util::createString("demo1"), Util::createString("value1"));
	dict1->set(Util::createString("demo2"), Util::createString("value2"));
	cout<<dict1->getAllocatedMemory()<<endl;
	VectorSP valVec2=Util::createVector(DT_STRING,2,2);
	valVec2->set(0,Util::createString("demo2"));
	valVec2->setNull(1);

	EXPECT_EQ(dict1->getMember(Util::createString("zzz123中文a"))->getString(),"*-/%**%#~！#“》（a");
	EXPECT_EQ(dict1->getMember(valVec2)->get(0)->getString(),"value2");
	dict1->remove(Util::createString("demo1"));
	dict1->remove(valVec2);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"zzz123中文a->*-/%**%#~！#“》（a\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}


TEST_F(DataformDictionaryTest,testStringAnyDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_STRING, DT_ANY);
	EXPECT_ANY_THROW(dict1->set(Util::createInt(1), Util::createDateTime(10000)));
	dict1->set(Util::createString("key_str"), Util::createDateTime(10000));
	string script = "a=\"key_str\";b = datetime(10000);c=dict(STRING,ANY);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("typestr(dict1)")->getString(),"STRING->ANY DICTIONARY");
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"key_str");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"1970.01.01T02:46:40");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_STRING);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_DATETIME);

	ConstantSP res1=Util::createConstant(DT_DATETIME);
	VectorSP valVec=Util::createVector(DT_STRING,0,2);
	valVec->append(Util::createString("key_str"));

	EXPECT_ANY_THROW(dict1->contain(Util::createInt(1), res1));
	dict1->contain(Util::createString("key_str"), res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_DATE);
	dict1->contain(valVec, res2);
	EXPECT_FALSE(res2->getBool());

	dict1->set(Util::createString("key2"), Util::createInt(1));
	dict1->set(Util::createString("key3"), Util::createFloat(3.2155));
	cout<<dict1->getAllocatedMemory()<<endl;
	VectorSP valVec2=Util::createVector(DT_STRING,2,2);
	valVec2->set(0,Util::createString("key3"));
	valVec2->setNull(1);

	EXPECT_ANY_THROW(dict1->getMember(Util::createInt(1)));
	EXPECT_EQ(dict1->getMember(Util::createString("key2"))->getInt(), 1);
	EXPECT_NEAR(dict1->getMember(valVec2)->get(0)->getFloat(), 3.2155, 4);
	EXPECT_ANY_THROW(dict1->remove(Util::createInt(1)));
	dict1->remove(Util::createString("key2"));
	dict1->remove(valVec2);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"key_str->1970.01.01T02:46:40\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());

	VectorSP matrix_val = Util::createMatrix(DT_INT, 2, 1, 2);
	matrix_val->set(0, 0, Util::createInt(999));
	matrix_val->set(0, 1, Util::createInt(888));
	DictionarySP dict2 = Util::createDictionary(DT_STRING, DT_INT);
	dict2->set(Util::createString("sym"), Util::createInt(23456));
	dict1->set(Util::createString("matrix"), matrix_val);
	dict1->set(Util::createString("dict"), dict2);
	cout<<dict1->getString()<<endl;
}


TEST_F(DataformDictionaryTest,testIntAnyDictionary){
	EXPECT_ANY_THROW(Util::createDictionary(DT_ANY, DT_INT));
	DictionarySP dict1 = Util::createDictionary(DT_INT, DT_ANY);
	dict1->set(Util::createInt(123), Util::createBool(1));
	string script = "a=int(123);b = bool(1);c=dict(INT,ANY);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"123");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"1");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_INT);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_BOOL);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_BOOL,0,2);
	valVec->append(Util::createBool(1));
	dict1->contain(Util::createInt(123),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	dict1->set(Util::createInt(4), Util::createString("1"));
	dict1->set(Util::createInt(7), Util::createBlob("2"));
	cout<<dict1->getAllocatedMemory()<<endl;
	VectorSP valVec2=Util::createVector(DT_INT,2,2);
	valVec2->set(0,Util::createInt(7));
	valVec2->setNull(1);

	EXPECT_EQ(dict1->getMember(Util::createInt(4))->getString(),"1");
	EXPECT_EQ(dict1->getMember(valVec2)->get(0)->getString(),"2");
	dict1->remove(Util::createInt(4));
	dict1->remove(valVec2);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"123->1\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}


TEST_F(DataformDictionaryTest,testLongAnyDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_LONG, DT_ANY);
	EXPECT_ANY_THROW(dict1->set(Util::createString("1"), Util::createDateTime(10000)));
	dict1->set(Util::createLong(1300000), Util::createDateTime(10000));
	string script = "a=long(1300000);b = datetime(10000);c=dict(LONG,ANY);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("typestr(dict1)")->getString(),"LONG->ANY DICTIONARY");
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getLong(), (long long)1300000);
	EXPECT_EQ(dict1->values()->get(0)->getString(),"1970.01.01T02:46:40");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_LONG);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_DATETIME);

	ConstantSP res1=Util::createConstant(DT_DATETIME);
	VectorSP valVec=Util::createVector(DT_LONG,0,2);
	valVec->append(Util::createLong(1300000));

	EXPECT_ANY_THROW(dict1->contain(Util::createInt(1), res1));
	dict1->contain(Util::createLong(1300000), res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_DATE);
	dict1->contain(valVec, res2);
	EXPECT_FALSE(res2->getBool());

	dict1->set(Util::createLong(1000000000), Util::createInt(1));
	dict1->set(Util::createLong(10000000000000), Util::createFloat(3.2155));
	cout<<dict1->getAllocatedMemory()<<endl;
	VectorSP valVec2=Util::createVector(DT_LONG,2,2);
	valVec2->set(0,Util::createLong(10000000000000));
	valVec2->setNull(1);

	EXPECT_ANY_THROW(dict1->getMember(Util::createInt(1)));
	EXPECT_EQ(dict1->getMember(Util::createLong(1000000000))->getInt(), 1);
	EXPECT_NEAR(dict1->getMember(valVec2)->get(0)->getFloat(), 3.2155, 4);
	EXPECT_ANY_THROW(dict1->remove(Util::createString("1")));
	dict1->remove(Util::createLong(1000000000));
	dict1->remove(valVec2);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"1300000->1970.01.01T02:46:40\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
	EXPECT_FALSE(dict1->containNotMarshallableObject());

	VectorSP matrix_val = Util::createMatrix(DT_INT, 2, 1, 2);
	matrix_val->set(0, 0, Util::createInt(999));
	matrix_val->set(0, 1, Util::createInt(888));
	DictionarySP dict2 = Util::createDictionary(DT_STRING, DT_INT);
	dict2->set(Util::createString("sym"), Util::createInt(23456));
	dict1->set(Util::createLong(1), matrix_val);
	dict1->set(Util::createLong(2), dict2);
	cout<<dict1->getString()<<endl;
}


TEST_F(DataformDictionaryTest,testSymbolDictionary){
	EXPECT_ANY_THROW(Util::createDictionary(DT_SYMBOL, DT_STRING));
}

TEST_F(DataformDictionaryTest,testBoolDictionary){
	EXPECT_ANY_THROW(Util::createDictionary(DT_BOOL, DT_STRING));
}

TEST_F(DataformDictionaryTest,testCharDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_CHAR, DT_STRING);
	dict1->set(Util::createChar(1), Util::createString("0"));
	string script = "a=char(1);b = string(0);c=dict(CHAR,STRING);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"1");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"0");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_CHAR);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_STRING);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_CHAR,0,2);
	valVec->append(Util::createChar(0));
	dict1->contain(Util::createChar(1),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	dict1->set(Util::createChar(2), Util::createString("2"));
	dict1->set(Util::createChar(3), Util::createString("3"));
	cout<<dict1->getAllocatedMemory()<<endl;
	VectorSP valVec2=Util::createVector(DT_CHAR,2,2);
	valVec2->set(0,Util::createChar(3));
	valVec2->setNull(1);

	EXPECT_EQ(dict1->getMember(Util::createChar(2))->getString(),"2");
	EXPECT_EQ(dict1->getMember(valVec2)->get(0)->getString(),"3");
	dict1->remove(Util::createChar(2));
	dict1->remove(valVec2);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"1->0\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testCharNullDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_CHAR, DT_CHAR);
	dict1->set(Util::createNullConstant(DT_CHAR), Util::createNullConstant(DT_CHAR));
	string script = "a=char(NULL);b = char(NULL);c=dict(CHAR,CHAR);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_CHAR);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_CHAR);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_CHAR,0,2);
	valVec->append(Util::createChar(0));
	dict1->contain(Util::createNullConstant(DT_CHAR),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"->\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testIntDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_INT, DT_STRING);
	dict1->set(Util::createInt(1), Util::createString("0"));
	string script = "a=int(1);b = string(0);c=dict(INT,STRING);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"1");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"0");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_INT);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_STRING);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_INT,0,2);
	valVec->append(Util::createInt(0));
	dict1->contain(Util::createInt(1),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	dict1->set(Util::createInt(2), Util::createString("2"));
	dict1->set(Util::createInt(3), Util::createString("3"));
	cout<<dict1->getAllocatedMemory()<<endl;
	VectorSP valVec2=Util::createVector(DT_CHAR,2,2);
	valVec2->set(0,Util::createInt(3));
	valVec2->setNull(1);

	EXPECT_EQ(dict1->getMember(Util::createInt(2))->getString(),"2");
	EXPECT_EQ(dict1->getMember(valVec2)->get(0)->getString(),"3");
	dict1->remove(Util::createInt(2));
	dict1->remove(valVec2);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"1->0\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testIntNullDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_INT, DT_INT);
	dict1->set(Util::createNullConstant(DT_INT), Util::createNullConstant(DT_INT));
	string script = "a=int(NULL);b = int(NULL);c=dict(INT,INT);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_INT);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_INT);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_INT,0,2);
	valVec->append(Util::createInt(0));
	dict1->contain(Util::createNullConstant(DT_INT),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"->\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}


TEST_F(DataformDictionaryTest,testLongDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_LONG, DT_STRING);
	dict1->set(Util::createLong(100000000), Util::createString("0"));
	string script = "a=long(100000000);b = string(0);c=dict(LONG,STRING);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"100000000");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"0");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_LONG);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_STRING);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_LONG,0,2);
	valVec->append(Util::createLong(0));
	dict1->contain(Util::createLong(100000000),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	dict1->set(Util::createLong(2), Util::createString("2"));
	dict1->set(Util::createLong(3), Util::createString("3"));
	cout<<dict1->getAllocatedMemory()<<endl;
	VectorSP valVec2=Util::createVector(DT_LONG,2,2);
	valVec2->set(0,Util::createLong(3));
	valVec2->setNull(1);

	EXPECT_EQ(dict1->getMember(Util::createLong(2))->getString(),"2");
	EXPECT_EQ(dict1->getMember(valVec2)->get(0)->getString(),"3");
	dict1->remove(Util::createLong(2));
	dict1->remove(valVec2);
	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"100000000->0\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testLongNullDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_LONG, DT_LONG);
	dict1->set(Util::createNullConstant(DT_LONG), Util::createNullConstant(DT_LONG));
	string script = "a=long(NULL);b = long(NULL);c=dict(LONG,LONG);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_LONG);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_LONG);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_LONG,0,2);
	valVec->append(Util::createLong(0));
	dict1->contain(Util::createNullConstant(DT_LONG),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"->\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testShortDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_SHORT, DT_STRING);
	dict1->set(Util::createShort(100), Util::createString("0"));
	string script = "a=short(100);b = string(0);c=dict(SHORT,STRING);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"100");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"0");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_SHORT);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_STRING);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_SHORT,0,2);
	valVec->append(Util::createShort(0));
	dict1->contain(Util::createShort(100),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	dict1->set(Util::createShort(2), Util::createString("2"));
	dict1->set(Util::createShort(3), Util::createString("3"));
	cout<<dict1->getAllocatedMemory()<<endl;
	VectorSP valVec2=Util::createVector(DT_SHORT,2,2);
	valVec2->set(0,Util::createShort(3));
	valVec2->setNull(1);

	EXPECT_EQ(dict1->getMember(Util::createShort(2))->getString(),"2");
	EXPECT_EQ(dict1->getMember(valVec2)->get(0)->getString(),"3");
	dict1->remove(Util::createShort(2));
	dict1->remove(valVec2);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"100->0\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testShortNullDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_SHORT, DT_SHORT);
	dict1->set(Util::createNullConstant(DT_SHORT), Util::createNullConstant(DT_SHORT));
	string script = "a=short(NULL);b = short(NULL);c=dict(SHORT,SHORT);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_SHORT);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_SHORT);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_SHORT,0,2);
	valVec->append(Util::createShort(0));
	dict1->contain(Util::createNullConstant(DT_SHORT),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"->\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testFloatDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_FLOAT, DT_STRING);
	dict1->set(Util::createFloat(100.2333), Util::createString("0"));

	string script = "a=float(100.2333);b = string(0);c=dict(FLOAT,STRING);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"100.233299");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"0");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_FLOAT);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_STRING);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_FLOAT,0,2);
	valVec->append(Util::createFloat(0));
	dict1->contain(Util::createFloat(100.2333),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	dict1->set(Util::createFloat(2), Util::createString("2"));
	dict1->set(Util::createFloat(3), Util::createString("3"));
	cout<<dict1->getAllocatedMemory()<<endl;
	VectorSP valVec2=Util::createVector(DT_FLOAT,2,2);
	valVec2->set(0,Util::createFloat(3));
	valVec2->setNull(1);

	EXPECT_EQ(dict1->getMember(Util::createFloat(2))->getString(),"2");
	EXPECT_EQ(dict1->getMember(valVec2)->get(0)->getString(),"3");
	dict1->remove(Util::createFloat(2));
	dict1->remove(valVec2);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"100.233299->0\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}


TEST_F(DataformDictionaryTest,testFloatNullDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_FLOAT, DT_FLOAT);
	dict1->set(Util::createNullConstant(DT_FLOAT), Util::createNullConstant(DT_FLOAT));
	string script = "a=float(NULL);b = float(NULL);c=dict(FLOAT,FLOAT);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_FLOAT);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_FLOAT);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_FLOAT,0,2);
	valVec->append(Util::createFloat(0));
	dict1->contain(Util::createNullConstant(DT_FLOAT),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"->\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testDoubleDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_DOUBLE, DT_STRING);
	dict1->set(Util::createDouble(100.2333), Util::createString("0"));
	string script = "a=double(100.2333);b = string(0);c=dict(DOUBLE,STRING);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"100.2333");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"0");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_DOUBLE);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_STRING);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_DOUBLE,0,2);
	valVec->append(Util::createDouble(0));
	dict1->contain(Util::createDouble(100.2333),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	dict1->set(Util::createDouble(2), Util::createString("2"));
	dict1->set(Util::createDouble(3), Util::createString("3"));
	cout<<dict1->getAllocatedMemory()<<endl;
	VectorSP valVec2=Util::createVector(DT_DOUBLE,2,2);
	valVec2->set(0,Util::createDouble(3));
	valVec2->setNull(1);

	EXPECT_EQ(dict1->getMember(Util::createDouble(2))->getString(),"2");
	EXPECT_EQ(dict1->getMember(valVec2)->get(0)->getString(),"3");
	dict1->remove(Util::createDouble(2));
	dict1->remove(valVec2);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"100.2333->0\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}


TEST_F(DataformDictionaryTest,testDoubleNullDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_DOUBLE, DT_DOUBLE);
	dict1->set(Util::createNullConstant(DT_DOUBLE), Util::createNullConstant(DT_DOUBLE));
	string script = "a=double(NULL);b = double(NULL);c=dict(DOUBLE,DOUBLE);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_DOUBLE);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_DOUBLE);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_DOUBLE,0,2);
	valVec->append(Util::createDouble(0));
	dict1->contain(Util::createNullConstant(DT_DOUBLE),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"->\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testDatehourDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_DATEHOUR, DT_STRING);
	dict1->set(Util::createDateHour(100), Util::createString("0"));
	string script = "a=datehour(100);b = string(0);c=dict(DATEHOUR,STRING);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"1970.01.05T04");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"0");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_DATEHOUR);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_STRING);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_DATEHOUR,0,2);
	valVec->append(Util::createDateHour(0));
	dict1->contain(Util::createDateHour(100),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	dict1->set(Util::createDateHour(2), Util::createString("2"));
	dict1->set(Util::createDateHour(3), Util::createString("3"));
	cout<<dict1->getAllocatedMemory()<<endl;
	VectorSP valVec2=Util::createVector(DT_DATEHOUR,2,2);
	valVec2->set(0,Util::createDateHour(3));
	valVec2->setNull(1);

	EXPECT_EQ(dict1->getMember(Util::createDateHour(2))->getString(),"2");
	EXPECT_EQ(dict1->getMember(valVec2)->get(0)->getString(),"3");
	dict1->remove(Util::createDateHour(2));
	dict1->remove(valVec2);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"1970.01.05T04->0\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testDatehourNullDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_DATEHOUR, DT_DATEHOUR);
	dict1->set(Util::createNullConstant(DT_DATEHOUR), Util::createNullConstant(DT_DATEHOUR));
	string script = "a=datehour(NULL);b = datehour(NULL);c=dict(DATEHOUR,DATEHOUR);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_DATEHOUR);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_DATEHOUR);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_DATEHOUR,0,2);
	valVec->append(Util::createDateHour(0));
	dict1->contain(Util::createNullConstant(DT_DATEHOUR),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"->\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testDateDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_DATE, DT_STRING);
	dict1->set(Util::createDate(100), Util::createString("0"));
	string script = "a=date(100);b = string(0);c=dict(DATE,STRING);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"1970.04.11");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"0");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_DATE);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_STRING);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_DATE,0,2);
	valVec->append(Util::createDate(0));
	dict1->contain(Util::createDate(100),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	dict1->set(Util::createDate(2), Util::createString("2"));
	dict1->set(Util::createDate(3), Util::createString("3"));
	cout<<dict1->getAllocatedMemory()<<endl;
	VectorSP valVec2=Util::createVector(DT_DATE,2,2);
	valVec2->set(0,Util::createDate(3));
	valVec2->setNull(1);

	EXPECT_EQ(dict1->getMember(Util::createDate(2))->getString(),"2");
	EXPECT_EQ(dict1->getMember(valVec2)->get(0)->getString(),"3");
	dict1->remove(Util::createDate(2));
	dict1->remove(valVec2);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"1970.04.11->0\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testDatenullDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_DATE, DT_DATE);
	dict1->set(Util::createNullConstant(DT_DATE), Util::createNullConstant(DT_DATE));
	string script = "a=date(NULL);b = date(NULL);c=dict(DATE,DATE);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_DATE);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_DATE);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_DATE,0,2);
	valVec->append(Util::createDate(0));
	dict1->contain(Util::createNullConstant(DT_DATE),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"->\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testMinuteDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_MINUTE, DT_STRING);
	dict1->set(Util::createMinute(1000), Util::createString("0"));
	string script = "a=minute(1000);b = string(0);c=dict(MINUTE,STRING);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"16:40m");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"0");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_MINUTE);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_STRING);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_MINUTE,0,2);
	valVec->append(Util::createMinute(0));
	dict1->contain(Util::createMinute(1000),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	dict1->set(Util::createMinute(2), Util::createString("2"));
	dict1->set(Util::createMinute(3), Util::createString("3"));
	cout<<dict1->getAllocatedMemory()<<endl;
	VectorSP valVec2=Util::createVector(DT_MINUTE,2,2);
	valVec2->set(0,Util::createMinute(3));
	valVec2->setNull(1);

	EXPECT_EQ(dict1->getMember(Util::createMinute(2))->getString(),"2");
	EXPECT_EQ(dict1->getMember(valVec2)->get(0)->getString(),"3");
	dict1->remove(Util::createMinute(2));
	dict1->remove(valVec2);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"16:40m->0\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testMinutenullDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_MINUTE, DT_MINUTE);
	dict1->set(Util::createNullConstant(DT_MINUTE), Util::createNullConstant(DT_MINUTE));
	string script = "a=minute(NULL);b = minute(NULL);c=dict(MINUTE,MINUTE);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_MINUTE);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_MINUTE);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_MINUTE,0,2);
	valVec->append(Util::createMinute(0));
	dict1->contain(Util::createNullConstant(DT_MINUTE),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"->\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testDatetimeDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_DATETIME, DT_STRING);
	dict1->set(Util::createDateTime(1000000000), Util::createString("0"));
	string script = "a=datetime(1000000000);b = string(0);c=dict(DATETIME,STRING);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"2001.09.09T01:46:40");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"0");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_DATETIME);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_STRING);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_DATETIME,0,2);
	valVec->append(Util::createDateTime(0));
	dict1->contain(Util::createDateTime(1000000000),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	dict1->set(Util::createDateTime(2), Util::createString("2"));
	dict1->set(Util::createDateTime(3), Util::createString("3"));
	cout<<dict1->getAllocatedMemory()<<endl;
	VectorSP valVec2=Util::createVector(DT_DATETIME,2,2);
	valVec2->set(0,Util::createDateTime(3));
	valVec2->setNull(1);

	EXPECT_EQ(dict1->getMember(Util::createDateTime(2))->getString(),"2");
	EXPECT_EQ(dict1->getMember(valVec2)->get(0)->getString(),"3");
	dict1->remove(Util::createDateTime(2));
	dict1->remove(valVec2);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"2001.09.09T01:46:40->0\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testDatetimenullDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_DATETIME, DT_DATETIME);
	dict1->set(Util::createNullConstant(DT_DATETIME), Util::createNullConstant(DT_DATETIME));
	string script = "a=datetime(NULL);b = datetime(NULL);c=dict(DATETIME,DATETIME);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_DATETIME);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_DATETIME);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_DATETIME,0,2);
	valVec->append(Util::createDateTime(0));
	dict1->contain(Util::createNullConstant(DT_DATETIME),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"->\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testTimeStampDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_TIMESTAMP, DT_STRING);
	dict1->set(Util::createTimestamp((long long)10000000000000), Util::createString("0"));
	string script = "a=timestamp(10000000000000);b = string(0);c=dict(TIMESTAMP,STRING);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"2286.11.20T17:46:40.000");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"0");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_TIMESTAMP);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_STRING);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_TIMESTAMP,0,2);
	valVec->append(Util::createTimestamp(0));
	dict1->contain(Util::createTimestamp((long long)10000000000000),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	dict1->set(Util::createTimestamp(2), Util::createString("2"));
	dict1->set(Util::createTimestamp(3), Util::createString("3"));
	cout<<dict1->getAllocatedMemory()<<endl;
	VectorSP valVec2=Util::createVector(DT_TIMESTAMP,2,2);
	valVec2->set(0,Util::createTimestamp(3));
	valVec2->setNull(1);

	EXPECT_EQ(dict1->getMember(Util::createTimestamp(2))->getString(),"2");
	EXPECT_EQ(dict1->getMember(valVec2)->get(0)->getString(),"3");
	dict1->remove(Util::createTimestamp(2));
	dict1->remove(valVec2);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"2286.11.20T17:46:40.000->0\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testTimeStampnullDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_TIMESTAMP, DT_TIMESTAMP);
	dict1->set(Util::createNullConstant(DT_TIMESTAMP), Util::createNullConstant(DT_TIMESTAMP));
	string script = "a=timestamp(NULL);b = timestamp(NULL);c=dict(TIMESTAMP,TIMESTAMP);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_TIMESTAMP);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_TIMESTAMP);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_TIMESTAMP,0,2);
	valVec->append(Util::createTimestamp(0));
	dict1->contain(Util::createNullConstant(DT_TIMESTAMP),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"->\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testnanotimeDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_NANOTIME, DT_STRING);
	dict1->set(Util::createNanoTime((long long)10000000000000), Util::createString("0"));
	string script = "a=nanotime(10000000000000);b = string(0);c=dict(NANOTIME,STRING);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"02:46:40.000000000");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"0");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_NANOTIME);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_STRING);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_NANOTIME,0,2);
	valVec->append(Util::createNanoTime(0));
	dict1->contain(Util::createNanoTime((long long)10000000000000),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	dict1->set(Util::createNanoTime(2), Util::createString("2"));
	dict1->set(Util::createNanoTime(3), Util::createString("3"));
	cout<<dict1->getAllocatedMemory()<<endl;
	VectorSP valVec2=Util::createVector(DT_NANOTIME,2,2);
	valVec2->set(0,Util::createNanoTime(3));
	valVec2->setNull(1);

	EXPECT_EQ(dict1->getMember(Util::createNanoTime(2))->getString(),"2");
	EXPECT_EQ(dict1->getMember(valVec2)->get(0)->getString(),"3");
	dict1->remove(Util::createNanoTime(2));
	dict1->remove(valVec2);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"02:46:40.000000000->0\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testnanotimenullDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_NANOTIME, DT_NANOTIME);
	dict1->set(Util::createNullConstant(DT_NANOTIME), Util::createNullConstant(DT_NANOTIME));
	string script = "a=nanotime(NULL);b = nanotime(NULL);c=dict(NANOTIME,NANOTIME);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_NANOTIME);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_NANOTIME);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_NANOTIME,0,2);
	valVec->append(Util::createNanoTime(0));
	dict1->contain(Util::createNullConstant(DT_NANOTIME),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"->\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testnanotimestampDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_NANOTIMESTAMP, DT_STRING);
	dict1->set(Util::createNanoTimestamp((long long)10000000000000), Util::createString("0"));
	string script = "a=nanotimestamp(10000000000000);b = string(0);c=dict(NANOTIMESTAMP,STRING);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"1970.01.01T02:46:40.000000000");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"0");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_NANOTIMESTAMP);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_STRING);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_NANOTIMESTAMP,0,2);
	valVec->append(Util::createNanoTimestamp(0));
	dict1->contain(Util::createNanoTimestamp((long long)10000000000000),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	dict1->set(Util::createNanoTimestamp(2), Util::createString("2"));
	dict1->set(Util::createNanoTimestamp(3), Util::createString("3"));
	cout<<dict1->getAllocatedMemory()<<endl;
	VectorSP valVec2=Util::createVector(DT_NANOTIMESTAMP,2,2);
	valVec2->set(0,Util::createNanoTimestamp(3));
	valVec2->setNull(1);

	EXPECT_EQ(dict1->getMember(Util::createNanoTimestamp(2))->getString(),"2");
	EXPECT_EQ(dict1->getMember(valVec2)->get(0)->getString(),"3");
	dict1->remove(Util::createNanoTimestamp(2));
	dict1->remove(valVec2);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"1970.01.01T02:46:40.000000000->0\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testnanotimestampnullDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_NANOTIMESTAMP, DT_NANOTIMESTAMP);
	dict1->set(Util::createNullConstant(DT_NANOTIMESTAMP), Util::createNullConstant(DT_NANOTIMESTAMP));
	string script = "a=nanotimestamp(NULL);b = nanotimestamp(NULL);c=dict(NANOTIMESTAMP,NANOTIMESTAMP);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_NANOTIMESTAMP);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_NANOTIMESTAMP);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_NANOTIMESTAMP,0,2);
	valVec->append(Util::createNanoTimestamp(0));
	dict1->contain(Util::createNullConstant(DT_NANOTIMESTAMP),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"->\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testmonthDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_MONTH, DT_STRING);
	dict1->set(Util::createMonth(10000), Util::createString("0"));
	string script = "a=month(10000);b = string(0);c=dict(MONTH,STRING);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"0833.05M");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"0");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_MONTH);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_STRING);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_MONTH,0,2);
	valVec->append(Util::createMonth(0));
	dict1->contain(Util::createMonth(10000),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	dict1->set(Util::createMonth(2), Util::createString("2"));
	dict1->set(Util::createMonth(3), Util::createString("3"));
	cout<<dict1->getAllocatedMemory()<<endl;
	VectorSP valVec2=Util::createVector(DT_MONTH,2,2);
	valVec2->set(0,Util::createMonth(3));
	valVec2->setNull(1);

	EXPECT_EQ(dict1->getMember(Util::createMonth(2))->getString(),"2");
	EXPECT_EQ(dict1->getMember(valVec2)->get(0)->getString(),"3");
	dict1->remove(Util::createMonth(2));
	dict1->remove(valVec2);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"0833.05M->0\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testmonthnullDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_MONTH, DT_MONTH);
	dict1->set(Util::createNullConstant(DT_MONTH), Util::createNullConstant(DT_MONTH));
	string script = "a=month(NULL);b = month(NULL);c=dict(MONTH,MONTH);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_MONTH);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_MONTH);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_MONTH,0,2);
	valVec->append(Util::createMonth(0));
	dict1->contain(Util::createNullConstant(DT_MONTH),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"->\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testtimeDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_TIME, DT_STRING);
	dict1->set(Util::createTime(909000), Util::createString("0"));
	string script = "a=time(909000);b = string(0);c=dict(TIME,STRING);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"00:15:09.000");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"0");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_TIME);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_STRING);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_TIME,0,2);
	valVec->append(Util::createTime(0));
	dict1->contain(Util::createTime(909000),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	dict1->set(Util::createTime(2), Util::createString("2"));
	dict1->set(Util::createTime(3), Util::createString("3"));
	cout<<dict1->getAllocatedMemory()<<endl;
	VectorSP valVec2=Util::createVector(DT_TIME,2,2);
	valVec2->set(0,Util::createTime(3));
	valVec2->setNull(1);

	EXPECT_EQ(dict1->getMember(Util::createTime(2))->getString(),"2");
	EXPECT_EQ(dict1->getMember(valVec2)->get(0)->getString(),"3");
	dict1->remove(Util::createTime(2));
	dict1->remove(valVec2);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"00:15:09.000->0\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testtimenullDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_TIME, DT_TIME);
	dict1->set(Util::createNullConstant(DT_TIME), Util::createNullConstant(DT_TIME));
	string script = "a=time(NULL);b = time(NULL);c=dict(TIME,TIME);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_TIME);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_TIME);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_TIME,0,2);
	valVec->append(Util::createTime(0));
	dict1->contain(Util::createNullConstant(DT_TIME),res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"->\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testInt128Dictionary){
	DictionarySP dict0 = Util::createDictionary(DT_INT128, DT_INT128);
	DictionarySP dict1 = Util::createDictionary(DT_INT128, DT_STRING);
	ConstantSP int128val=Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec32");
	dict0->set(int128val, int128val);
	dict1->set(int128val, Util::createString("0"));

	string script = "a=int128(\"e1671797c52e15f763380b45e841ec32\");b = string(0);c=dict(INT128,STRING);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.run("c0 = dict(INT128,INT128);c0[a]=a");
	conn.upload("dict1",{dict1});
	conn.upload("dict0",{dict0});

	string judgestr= "res=array(BOOL)\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					res.append!(eqObj(c0.keys()[0],dict0.keys()[0]))\n\
					res.append!(eqObj(c0.values()[0],dict0.values()[0]))\n\
					eqObj(res, [true,true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"e1671797c52e15f763380b45e841ec32");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"0");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_INT128);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_STRING);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_INT128,0,2);
	valVec->append(Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec34"));
	dict1->contain(int128val, res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	ConstantSP int128val_2=Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec33");
	ConstantSP int128val_3=Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec34");
	dict1->set(int128val_2, Util::createString("value1"));
	dict1->set(int128val_3, Util::createString("value2"));
	cout<<dict1->getAllocatedMemory()<<endl;
	VectorSP valVec2=Util::createVector(DT_INT128,2,2);
	valVec2->set(0,int128val_3);
	valVec2->setNull(1);

	EXPECT_EQ(dict1->getMember(int128val_2)->getString(),"value1");
	EXPECT_EQ(dict1->getMember(valVec2)->get(0)->getString(),"value2");
	dict1->remove(int128val_2);
	dict1->remove(valVec2);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"e1671797-c52e-15f7-6338-0b45e841ec32->0\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}


TEST_F(DataformDictionaryTest,testInt128AnyDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_INT128, DT_ANY);
	ConstantSP int128val=Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec32");
	dict1->set(int128val, Util::createInt(1));
	string script = "a=int128(\"e1671797c52e15f763380b45e841ec32\");b = int(1);c=dict(INT128,ANY);c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	// cout<<dict1->getString();
	// cout<<conn.run("dict1")->getString();
	// cout<<conn.run("c")->getString();
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"e1671797c52e15f763380b45e841ec32");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"1");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_INT128);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_INT);

	ConstantSP res1=Util::createConstant(DT_BOOL);
	VectorSP valVec=Util::createVector(DT_INT128,0,2);
	valVec->append(Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec34"));
	dict1->contain(int128val, res1);
	EXPECT_TRUE(res1->getBool());

	ConstantSP res2=Util::createConstant(DT_BOOL);
	dict1->contain(valVec,res2);
	EXPECT_FALSE(res2->getBool());

	ConstantSP int128val_2=Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec33");
	ConstantSP int128val_3=Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec34");
	dict1->set(int128val_2, Util::createString("value1"));
	dict1->set(int128val_3, Util::createString("value2"));
	cout<<dict1->getAllocatedMemory()<<endl;
	VectorSP valVec2=Util::createVector(DT_INT128,2,2);
	valVec2->set(0,int128val_3);
	valVec2->setNull(1);

	EXPECT_EQ(dict1->getMember(int128val_2)->getString(),"value1");
	EXPECT_EQ(dict1->getMember(valVec2)->get(0)->getString(),"value2");
	dict1->remove(int128val_2);
	dict1->remove(valVec2);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"e1671797c52e15f763380b45e841ec32->1\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}


TEST_F(DataformDictionaryTest,testDecimal32Dictionary){
	DictionarySP dict1 = Util::createDictionary(DT_INT, DT_DECIMAL32);
	dict1->set(Util::createInt(1), Util::createDecimal32(6, 1.3));
	string script = "a=int(1);b = decimal32(1.3, 6);c=dict(INT,DECIMAL32(6));c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=array(BOOL)\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getInt(), 1);
	EXPECT_EQ(dict1->values()->get(0)->getString(),"1.300000");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_INT);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_DECIMAL32);

	dict1->set(Util::createInt(2), Util::createDecimal32(1, 1.34));
	dict1->set(Util::createInt(3), Util::createDecimal32(0, -2));
	cout<<dict1->getAllocatedMemory()<<endl;
	VectorSP valVec2=Util::createVector(DT_INT,2,2);
	valVec2->set(0,Util::createInt(3));
	valVec2->setNull(1);

	EXPECT_EQ(dict1->getMember(Util::createInt(2))->getString(),"1.3");
	EXPECT_EQ(dict1->getMember(valVec2)->get(0)->getString(),"-2");
	dict1->remove(Util::createInt(2));
	dict1->remove(valVec2);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"1->1.300000\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testDecimal32NullDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_INT, DT_DECIMAL32);
	dict1->set(Util::createNullConstant(DT_INT), Util::createNullConstant(DT_DECIMAL32, 6));
	string script = "a=int(NULL);b = decimal32(NULL, 6);c=dict(INT,DECIMAL32(6));c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_INT);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_DECIMAL32);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"->\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}


TEST_F(DataformDictionaryTest,testDecimal64Dictionary){
	DictionarySP dict1 = Util::createDictionary(DT_INT, DT_DECIMAL64);
	dict1->set(Util::createInt(1), Util::createDecimal64(16, 1.3456));
	string script = "a=int(1);b = decimal64(1.3456, 16);c=dict(INT,DECIMAL64(16));c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=array(BOOL)\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getInt(), 1);
	EXPECT_EQ(dict1->values()->get(0)->getString(),"1.3456000000000000");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_INT);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_DECIMAL64);

	dict1->set(Util::createInt(2), Util::createDecimal64(1, 1.3456));
	dict1->set(Util::createInt(3), Util::createDecimal64(0, -2));
	cout<<dict1->getAllocatedMemory()<<endl;
	VectorSP valVec2=Util::createVector(DT_INT,2,2);
	valVec2->set(0,Util::createInt(3));
	valVec2->setNull(1);

	EXPECT_EQ(dict1->getMember(Util::createInt(2))->getString(),"1.3");
	EXPECT_EQ(dict1->getMember(valVec2)->get(0)->getString(),"-2");
	dict1->remove(Util::createInt(2));
	dict1->remove(valVec2);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"1->1.3456000000000000\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testDecimal64NullDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_INT, DT_DECIMAL64);
	dict1->set(Util::createNullConstant(DT_INT), Util::createNullConstant(DT_DECIMAL64, 16));
	string script = "a=int(NULL);b = decimal64(NULL, 16);c=dict(INT,DECIMAL64(16));c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_INT);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_DECIMAL64);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"->\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}


TEST_F(DataformDictionaryTest,testDecimal128Dictionary){
	DictionarySP dict1 = Util::createDictionary(DT_INT, DT_DECIMAL128);
	ConstantSP dsm128val = Util::createNullConstant(DT_DECIMAL128, 26);
	dsm128val->setString("1.12345678901");
	dict1->set(Util::createInt(1), dsm128val);
	string script = "a=int(1);b = decimal128('1.12345678901', 26);c=dict(INT,DECIMAL128(26));c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=array(BOOL)\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getInt(), 1);
	EXPECT_EQ(dict1->values()->get(0)->getString(),"1.12345678901000000000000000");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_INT);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_DECIMAL128);

	dict1->set(Util::createInt(2), Util::createDecimal128(1, 1.3456));
	dict1->set(Util::createInt(3), Util::createDecimal128(0, -2));
	cout<<dict1->getAllocatedMemory()<<endl;
	VectorSP valVec2=Util::createVector(DT_INT,2,2);
	valVec2->set(0,Util::createInt(3));
	valVec2->setNull(1);

	EXPECT_EQ(dict1->getMember(Util::createInt(2))->getString(),"1.3");
	EXPECT_EQ(dict1->getMember(valVec2)->get(0)->getString(),"-2");
	dict1->remove(Util::createInt(2));
	dict1->remove(valVec2);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"1->1.12345678901000000000000000\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}

TEST_F(DataformDictionaryTest,testDecimal128NullDictionary){
	DictionarySP dict1 = Util::createDictionary(DT_INT, DT_DECIMAL128);
	dict1->set(Util::createNullConstant(DT_INT), Util::createNullConstant(DT_DECIMAL128, 26));
	string script = "a=int(NULL);b = decimal128(NULL, 26);c=dict(INT,DECIMAL128(26));c[a]=b;c";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	string judgestr= "res=[true]\n\
					res.append!(eqObj(c.keys()[0],dict1.keys()[0]))\n\
					res.append!(eqObj(c.values()[0],dict1.values()[0]))\n\
					eqObj(res, [true,true,true])";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
	EXPECT_EQ(dict1->getString(), res_d->getString());
	EXPECT_EQ(dict1->getType(), res_d->getType());
	EXPECT_EQ(conn.run("dict1")->isDictionary(),true);

	EXPECT_EQ(dict1->keys()->get(0)->getString(),"");
	EXPECT_EQ(dict1->values()->get(0)->getString(),"");
	EXPECT_EQ(dict1->keys()->get(0)->getType(),DT_INT);
	EXPECT_EQ(dict1->values()->get(0)->getType(),DT_DECIMAL128);

	EXPECT_EQ(dict1->count(),1);
	EXPECT_EQ(dict1->getValue()->getString(),"->\n");
	dict1->clear();
	EXPECT_EQ(dict1->getString(),dict1->getInstance()->getString());
}


TEST_F(DataformDictionaryTest,testDictionarywithValueAllTypes){
	vector<ConstantSP> items = {
		Util::createChar('\1'),Util::createBool(true),
		Util::createShort(1),Util::createInt(1),
		Util::createLong(1),Util::createDate(1),Util::createMonth(1),Util::createTime(1),
		Util::createMinute(1),Util::createDateTime(1),Util::createSecond(1),Util::createTimestamp(1),
		Util::createNanoTime(1),Util::createNanoTimestamp(1),Util::createFloat(1),
		Util::createDouble(1),Util::createString("1"),Util::parseConstant(DT_UUID,"5d212a78-cc48-e3b1-4235-b4d91473ee87"),
		Util::parseConstant(DT_IP,"1:1:1:1:1:1:1:1"),Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec32"),Util::createBlob("1"),
		Util::createDateHour(1),Util::createDecimal32(6,1.0),Util::createDecimal64(16,1.0),
		Util::createDecimal128(26,1.0)};

	VectorSP values = Util::createVector(DT_ANY,0,items.size());
	for(int i=0; i<items.size(); i++) values->append(items[i]);

	conn.run("items = [1c,true,1h,1,1l,date(1),month(1),time(1),minute(1),datetime(1),second(1),timestamp(1),nanotime(1),nanotimestamp(1),1f,1.0,`1,\
				uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87'),ipaddr('1:1:1:1:1:1:1:1'),int128('e1671797c52e15f763380b45e841ec32'),\
				blob(`1),datehour(1),decimal32(1,6),decimal64(1,16),decimal128(1,26)]");

	for (auto i=0; i< items.size();i++){
		ConstantSP key = items[i];
		if (key->getType() == DT_BOOL || key->getType() == DT_DECIMAL32 || key->getType() == DT_DECIMAL64 || key->getType() == DT_DECIMAL128) continue;

		for(auto j=0; j< items.size();j++){
			ConstantSP val = items[j];
			// cout << "keyType:" << Util::getDataTypeString(key->getType()) << ", valType:" << Util::getDataTypeString(val->getType()) << endl;
			DictionarySP dict1 = Util::createDictionary(key->getType(), val->getType());
			dict1->set(key, val);
			conn.upload("dict1",dict1);
			conn.upload("key", key);

			conn.run(
				"ex = dict([key], [items["+to_string(j)+"]]);"
				"assert eqObj(ex[key], dict1[key])");
			ConstantSP ex = conn.run("ex");
			EXPECT_EQ(dict1->get(key)->getType(), ex->get(key)->getType());
			EXPECT_EQ(dict1->get(key)->getString(), ex->get(key)->getString());
		}
	}

}

TEST_F(DataformDictionaryTest,testStringDictionaryMoreThan65535){
	DictionarySP dict1 = Util::createDictionary(DT_STRING, DT_STRING);
	for(int i=0;i<70000;i++){
		dict1->set(Util::createString(to_string(i)), Util::createString("val_"+to_string(i)));
	}
	string script = "a=array(STRING,0);b=array(STRING,0);for (i in 0..69999){a.append!(string(i));b.append!(\"val_\"+string(i));};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,70001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testAnyDictionaryMoreThan65535){
	DictionarySP dict1 = Util::createDictionary(DT_INT, DT_ANY);
	for(int i=0;i<70000;i++)
		dict1->set(Util::createInt(i), Util::createBool(1));
	string script = "a=array(INT,0);b=array(BOOL,0);for (i in 0..69999){a.append!(int(i));b.append!(bool(1))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,70001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testCharDictionaryEqule128){
	DictionarySP dict1 = Util::createDictionary(DT_CHAR, DT_STRING);
	for(int i=0;i<128;i++)
		dict1->set(Util::createChar(i), Util::createString("val_"+to_string(i)));
	string script = "a=array(CHAR,0);b=array(STRING,0);for (i in 0..127){a.append!(char(i));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	cout<<conn.run("dict1.keys()")->getString()<<endl;
	cout<<conn.run("z.keys()")->getString()<<endl;
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,129))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testIntDictionaryMoreThan65535){
	DictionarySP dict1 = Util::createDictionary(DT_INT, DT_STRING);
	for(int i=0;i<70000;i++)
		dict1->set(Util::createInt(i), Util::createString("val_"+to_string(i)));
	string script = "a=array(INT,0);b=array(STRING,0);for (i in 0..69999){a.append!(int(i));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,70001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testLongDictionaryMoreThan65535){
	DictionarySP dict1 = Util::createDictionary(DT_LONG, DT_STRING);
	for(int i=0;i<70000;i++)
		dict1->set(Util::createLong(i*1000), Util::createString("val_"+to_string(i)));
	string script = "a=array(LONG,0);b=array(STRING,0);for (i in 0..69999){a.append!(long(i*1000));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,70001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testShortDictionaryEqual256){
	DictionarySP dict1 = Util::createDictionary(DT_SHORT, DT_STRING);
	for(int i=0;i<256;i++)
		dict1->set(Util::createShort(i), Util::createString("val_"+to_string(i)));
	string script = "a=array(SHORT,0);b=array(STRING,0);for (i in 0..255){a.append!(short(i));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	cout<<conn.run("dict1.keys()")->getString()<<endl;
	cout<<conn.run("z.keys()")->getString()<<endl;
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,257))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testFloatDictionaryMoreThan65535){
	DictionarySP dict1 = Util::createDictionary(DT_FLOAT, DT_STRING);
	for(int i=0;i<70000;i++)
		dict1->set(Util::createFloat(i), Util::createString("val_"+to_string(i)));
	string script = "a=array(FLOAT,0);b=array(STRING,0);for (i in 0..69999){a.append!(float(i));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,70001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testDoubleDictionaryMoreThan65535){
	DictionarySP dict1 = Util::createDictionary(DT_DOUBLE, DT_STRING);
	for(int i=0;i<70000;i++)
		dict1->set(Util::createDouble(i), Util::createString("val_"+to_string(i)));
	string script = "a=array(DOUBLE,0);b=array(STRING,0);for (i in 0..69999){a.append!(double(i));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,70001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}


TEST_F(DataformDictionaryTest,testDatehourDictionaryMoreThan65535){
	DictionarySP dict1 = Util::createDictionary(DT_DATEHOUR, DT_STRING);
	for(int i=0;i<70000;i++)
		dict1->set(Util::createDateHour(i), Util::createString("val_"+to_string(i)));
	string script = "a=array(DATEHOUR,0);b=array(STRING,0);for (i in 0..69999){a.append!(datehour(i));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,70001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testDateDictionaryMoreThan65535){
	DictionarySP dict1 = Util::createDictionary(DT_DATE, DT_STRING);
	for(int i=0;i<70000;i++)
		dict1->set(Util::createDate(i), Util::createString("val_"+to_string(i)));
	string script = "a=array(DATE,0);b=array(STRING,0);for (i in 0..69999){a.append!(date(i));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,70001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testMinuteDictionaryMoreThan1024){
	DictionarySP dict1 = Util::createDictionary(DT_MINUTE, DT_STRING);
	for(int i=0;i<1440;i++)
		dict1->set(Util::createMinute(i), Util::createString("val_"+to_string(i)));
	string script = "a=array(MINUTE,0);b=array(STRING,0);for (i in 0..1439){a.append!(minute(i));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	cout<<conn.run("dict1.keys()")->getString()<<endl;
	cout<<conn.run("z.keys()")->getString()<<endl;
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,1441))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testDatetimeDictionaryMoreThan65535){
	DictionarySP dict1 = Util::createDictionary(DT_DATETIME, DT_STRING);
	for(int i=0;i<70000;i++)
		dict1->set(Util::createDateTime(i), Util::createString("val_"+to_string(i)));
	string script = "a=array(DATETIME,0);b=array(STRING,0);for (i in 0..69999){a.append!(datetime(i));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,70001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testTimeStampDictionaryMoreThan65535){
	DictionarySP dict1 = Util::createDictionary(DT_TIMESTAMP, DT_STRING);
	for(int i=0;i<70000;i++)
		dict1->set(Util::createTimestamp(i), Util::createString("val_"+to_string(i)));
	string script = "a=array(TIMESTAMP,0);b=array(STRING,0);for (i in 0..69999){a.append!(timestamp(i));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,70001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testnanotimeDictionaryMoreThan65535){
	DictionarySP dict1 = Util::createDictionary(DT_NANOTIME, DT_STRING);
	for(int i=0;i<70000;i++)
		dict1->set(Util::createNanoTime(i), Util::createString("val_"+to_string(i)));
	string script = "a=array(NANOTIME,0);b=array(STRING,0);for (i in 0..69999){a.append!(nanotime(i));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,70001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testnanotimestampDictionaryMoreThan65535){
	DictionarySP dict1 = Util::createDictionary(DT_NANOTIMESTAMP, DT_STRING);
	for(int i=0;i<70000;i++)
		dict1->set(Util::createNanoTimestamp(i), Util::createString("val_"+to_string(i)));
	string script = "a=array(NANOTIMESTAMP,0);b=array(STRING,0);for (i in 0..69999){a.append!(nanotimestamp(i));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,70001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testmonthDictionaryMoreThan65535){
	DictionarySP dict1 = Util::createDictionary(DT_MONTH, DT_STRING);
	for(int i=0;i<70000;i++)
		dict1->set(Util::createMonth(i), Util::createString("val_"+to_string(i)));
	string script = "a=array(MONTH,0);b=array(STRING,0);for (i in 0..69999){a.append!(month(i));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,70001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testtimeDictionaryMoreThan65535){
	DictionarySP dict1 = Util::createDictionary(DT_TIME, DT_STRING);
	for(int i=0;i<70000;i++)
		dict1->set(Util::createTime(i), Util::createString("val_"+to_string(i)));
	string script = "a=array(TIME,0);b=array(STRING,0);for (i in 0..69999){a.append!(time(i));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,70001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}


TEST_F(DataformDictionaryTest,testStringDictionaryMoreThan1048576){
	DictionarySP dict1 = Util::createDictionary(DT_STRING, DT_STRING);
	for(int i=0;i<1100000;i++){
		dict1->set(Util::createString(to_string(i)), Util::createString("val_"+to_string(i)));
	}
	string script = "a=array(STRING,0);b=array(STRING,0);for (i in 0..1099999){a.append!(string(i));b.append!(\"val_\"+string(i));};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,1100001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testAnyDictionaryMoreThan1048576){
	DictionarySP dict1 = Util::createDictionary(DT_INT, DT_ANY);
	for(int i=0;i<1100000;i++)
		dict1->set(Util::createInt(i), Util::createBool(1));
	string script = "a=array(INT,0);b=array(BOOL,0);for (i in 0..1099999){a.append!(int(i));b.append!(bool(1))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,1100001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testIntDictionaryMoreThan1048576){
	DictionarySP dict1 = Util::createDictionary(DT_INT, DT_STRING);
	for(int i=0;i<1100000;i++)
		dict1->set(Util::createInt(i), Util::createString("val_"+to_string(i)));
	string script = "a=array(INT,0);b=array(STRING,0);for (i in 0..1099999){a.append!(int(i));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,1100001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testLongDictionaryMoreThan1048576){
	DictionarySP dict1 = Util::createDictionary(DT_LONG, DT_STRING);
	for(int i=0;i<1100000;i++)
		dict1->set(Util::createLong(i*1000), Util::createString("val_"+to_string(i)));
	string script = "a=array(LONG,0);b=array(STRING,0);for (i in 0..1099999){a.append!(long(i*1000));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,1100001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testFloatDictionaryMoreThan1048576){
	DictionarySP dict1 = Util::createDictionary(DT_FLOAT, DT_STRING);
	for(int i=0;i<1100000;i++)
		dict1->set(Util::createFloat(i), Util::createString("val_"+to_string(i)));
	string script = "a=array(FLOAT,0);b=array(STRING,0);for (i in 0..1099999){a.append!(float(i));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,1100001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testDoubleDictionaryMoreThan1048576){
	DictionarySP dict1 = Util::createDictionary(DT_DOUBLE, DT_STRING);
	for(int i=0;i<1100000;i++)
		dict1->set(Util::createDouble(i), Util::createString("val_"+to_string(i)));
	string script = "a=array(DOUBLE,0);b=array(STRING,0);for (i in 0..1099999){a.append!(double(i));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,1100001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}


TEST_F(DataformDictionaryTest,testDatehourDictionaryMoreThan1048576){
	DictionarySP dict1 = Util::createDictionary(DT_DATEHOUR, DT_STRING);
	for(int i=0;i<1100000;i++)
		dict1->set(Util::createDateHour(i), Util::createString("val_"+to_string(i)));
	string script = "a=array(DATEHOUR,0);b=array(STRING,0);for (i in 0..1099999){a.append!(datehour(i));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,1100001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testDateDictionaryMoreThan1048576){
	DictionarySP dict1 = Util::createDictionary(DT_DATE, DT_STRING);
	for(int i=0;i<1100000;i++)
		dict1->set(Util::createDate(i), Util::createString("val_"+to_string(i)));
	string script = "a=array(DATE,0);b=array(STRING,0);for (i in 0..1099999){a.append!(date(i));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,1100001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testDatetimeDictionaryMoreThan1048576){
	DictionarySP dict1 = Util::createDictionary(DT_DATETIME, DT_STRING);
	for(int i=0;i<1100000;i++)
		dict1->set(Util::createDateTime(i), Util::createString("val_"+to_string(i)));
	string script = "a=array(DATETIME,0);b=array(STRING,0);for (i in 0..1099999){a.append!(datetime(i));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,1100001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testTimeStampDictionaryMoreThan1048576){
	DictionarySP dict1 = Util::createDictionary(DT_TIMESTAMP, DT_STRING);
	for(int i=0;i<1100000;i++)
		dict1->set(Util::createTimestamp(i), Util::createString("val_"+to_string(i)));
	string script = "a=array(TIMESTAMP,0);b=array(STRING,0);for (i in 0..1099999){a.append!(timestamp(i));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,1100001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testnanotimeDictionaryMoreThan1048576){
	DictionarySP dict1 = Util::createDictionary(DT_NANOTIME, DT_STRING);
	for(int i=0;i<1100000;i++)
		dict1->set(Util::createNanoTime(i), Util::createString("val_"+to_string(i)));
	string script = "a=array(NANOTIME,0);b=array(STRING,0);for (i in 0..1099999){a.append!(nanotime(i));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,1100001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testnanotimestampDictionaryMoreThan1048576){
	DictionarySP dict1 = Util::createDictionary(DT_NANOTIMESTAMP, DT_STRING);
	for(int i=0;i<1100000;i++)
		dict1->set(Util::createNanoTimestamp(i), Util::createString("val_"+to_string(i)));
	string script = "a=array(NANOTIMESTAMP,0);b=array(STRING,0);for (i in 0..1099999){a.append!(nanotimestamp(i));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,1100001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testmonthDictionaryMoreThan1048576){
	DictionarySP dict1 = Util::createDictionary(DT_MONTH, DT_STRING);
	for(int i=0;i<1100000;i++)
		dict1->set(Util::createMonth(i), Util::createString("val_"+to_string(i)));
	string script = "a=array(MONTH,0);b=array(STRING,0);for (i in 0..1099999){a.append!(month(i));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,1100001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}

TEST_F(DataformDictionaryTest,testtimeDictionaryMoreThan1048576){
	DictionarySP dict1 = Util::createDictionary(DT_TIME, DT_STRING);
	for(int i=0;i<1100000;i++)
		dict1->set(Util::createTime(i), Util::createString("val_"+to_string(i)));
	string script = "a=array(TIME,0);b=array(STRING,0);for (i in 0..1099999){a.append!(time(i));b.append!(\"val_\"+string(i))};z=dict(a,b);z";
	DictionarySP res_d = conn.run(script);
	conn.upload("dict1",{dict1});
	EXPECT_EQ(conn.run("dict1.size()")->getInt(),res_d->size());
	string judgestr= "res=[false];for(key in z.keys()){res.append!(dict1[key].isNull())};eqObj(res,take(false,1100001))";
	EXPECT_EQ(conn.run(judgestr)->getBool(),true);
}