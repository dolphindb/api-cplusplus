class ArrayVectorTest:public testing::Test
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

#ifndef WINDOWS
TEST_F(ArrayVectorTest,testArrayVector_append) {
    VectorSP vec = Util::createArrayVector(DT_BOOL_ARRAY, 0, 1);
    SmartPointer<FastArrayVector> av1 = vec;
    VectorSP v1 = Util::createVector(DT_BOOL, 0, 6);
    EXPECT_TRUE(av1->append(v1));
    EXPECT_EQ(av1->size(), 1);

    VectorSP v2 = Util::createVector(DT_BOOL, 6, 6);
    v2->setNull(0);
    EXPECT_TRUE(av1->append(v2));
    EXPECT_EQ(av1->size(), 2);

    VectorSP indexArray = Util::createVector(DT_INT, 2, 2);
    indexArray->set(0, Util::createInt(0));
    indexArray->set(1, Util::createInt(1));

    EXPECT_FALSE(av1->append(Util::createInt(1),indexArray));
    EXPECT_FALSE(av1->append(v2, indexArray));

    VectorSP anyVector = Util::createVector(DT_ANY, 2, 2);
    EXPECT_TRUE(av1->append(anyVector, indexArray));
    EXPECT_EQ(av1->size(), 4);

    EXPECT_TRUE(av1->append(av1, indexArray));
    EXPECT_EQ(av1->size(), 6);
}

TEST_F(ArrayVectorTest, testArrayVector_checkVectorSize) {
    VectorSP vec = Util::createArrayVector(DT_BOOL_ARRAY, 0, 2);
    SmartPointer<FastArrayVector> av = vec;
    EXPECT_EQ(av->checkVectorSize(), 0);

    VectorSP v1 = Util::createVector(DT_BOOL, 6, 6);
    EXPECT_TRUE(av->append(v1));
    EXPECT_EQ(av->checkVectorSize(), 6);

    v1 = Util::createVector(DT_BOOL, 5, 6);
    EXPECT_TRUE(av->append(v1));
    EXPECT_EQ(av->checkVectorSize(), -1);
}

TEST_F(ArrayVectorTest,testArrayVector_count) {
    VectorSP vec = Util::createArrayVector(DT_BOOL_ARRAY, 0, 1);
    SmartPointer<FastArrayVector> av1 = vec;
    VectorSP v1 = Util::createVector(DT_BOOL, 0, 6);
    EXPECT_TRUE(av1->append(v1));
    EXPECT_EQ(av1->count(0, 1), 0);

    VectorSP v2 = Util::createVector(DT_BOOL, 6, 6);
    v2->setNull(0);
    EXPECT_TRUE(av1->append(v2));
    EXPECT_EQ(av1->count(0, 2), 1);

    VectorSP indexArray = Util::createVector(DT_INT, 2, 2);
    indexArray->set(0, Util::createInt(0));
    indexArray->set(1, Util::createInt(1));

    EXPECT_FALSE(av1->append(Util::createInt(1),indexArray));
    EXPECT_EQ(av1->count(0, 3), 2);
    EXPECT_FALSE(av1->append(v2, indexArray));
    EXPECT_EQ(av1->count(0, 4), 3);

    VectorSP anyVector = Util::createVector(DT_ANY, 2, 2);
    EXPECT_TRUE(av1->append(anyVector, indexArray));
    EXPECT_EQ(av1->size(), 4);
    EXPECT_EQ(av1->count(0, 5), 2);

    EXPECT_TRUE(av1->append(av1, indexArray));
    EXPECT_EQ(av1->size(), 6);
    EXPECT_EQ(av1->count(0, 6), 2);
}

TEST_F(ArrayVectorTest,testArrayVector_fill) {
    VectorSP vec = Util::createArrayVector(DT_BOOL_ARRAY, 0, 1);
    SmartPointer<FastArrayVector> av1 = vec;
    
    av1->fill(0, 0, Util::createBool(1));
    EXPECT_EQ(av1->size(), 0);

    VectorSP v1 = Util::createVector(DT_BOOL, 0, 6);
    EXPECT_TRUE(av1->append(v1));
    EXPECT_EQ(av1->size(), 1);

    av1->fill(0, 1, Util::createBool(1));
    EXPECT_EQ(av1->size(), 1);

    VectorSP v2 = Util::createVector(DT_BOOL, 6, 6);
    v2->setNull(0);
    EXPECT_TRUE(av1->append(v2));
    EXPECT_EQ(av1->size(), 2);

    VectorSP indexArray = Util::createVector(DT_INT, 2, 2);
    indexArray->set(0, Util::createInt(0));
    indexArray->set(1, Util::createInt(1));
    
    av1->fill(0, 1, Util::createVector(DT_BOOL, 2, 2));

    EXPECT_FALSE(av1->append(Util::createInt(1),indexArray));
    EXPECT_FALSE(av1->append(v2, indexArray));

    VectorSP anyVector = Util::createVector(DT_ANY, 2, 2);
    EXPECT_TRUE(av1->append(anyVector, indexArray));
    EXPECT_EQ(av1->size(), 4);

    EXPECT_TRUE(av1->append(av1, indexArray));
    EXPECT_EQ(av1->size(), 6);
}

TEST_F(ArrayVectorTest,testArrayVector_get) {
    VectorSP vec = Util::createArrayVector(DT_BOOL_ARRAY, 0, 1);
    SmartPointer<FastArrayVector> av1 = vec;

    EXPECT_ANY_THROW(av1->get(Util::createInt(-1)));

    VectorSP v1 = Util::createVector(DT_BOOL, 2, 2);
    EXPECT_TRUE(av1->append(v1));
    EXPECT_EQ(av1->size(), 1);
    ConstantSP res = av1->get(Util::createInt(0));
    EXPECT_EQ(res->getString(), "[1]");

    VectorSP v2 = Util::createVector(DT_BOOL, 6, 6);
    v2->setNull(0);
    EXPECT_TRUE(av1->append(v2));
    EXPECT_EQ(av1->size(), 2);

    ConstantSP pair = Util::createPair(DT_INT);
    pair->set(0, Util::createInt(-1));
    pair->set(1, Util::createInt(1));
    EXPECT_ANY_THROW(res = av1->get(pair));

    pair->set(0, Util::createInt(1));
    pair->set(1, Util::createInt(1));
    EXPECT_ANY_THROW(res = av1->get(pair));

    pair->set(0, Util::createInt(0));
    pair->set(1, Util::createInt(1));
    res = av1->get(pair);
    EXPECT_EQ(res->getString(), "[[1],[]]");

    VectorSP indexArray = Util::createVector(DT_INT, 2, 2);
    indexArray->set(0, Util::createInt(0));
    indexArray->set(1, Util::createInt(1));
    res = av1->get(indexArray);

    EXPECT_FALSE(av1->append(Util::createInt(1),indexArray));
    EXPECT_FALSE(av1->append(v2, indexArray));

    VectorSP anyVector = Util::createVector(DT_ANY, 2, 2);
    EXPECT_TRUE(av1->append(anyVector, indexArray));
    EXPECT_EQ(av1->size(), 4);
    EXPECT_ANY_THROW(res = av1->get(anyVector));
}

TEST_F(ArrayVectorTest,testArrayVector_getSubVector) {
    VectorSP vec = Util::createArrayVector(DT_BOOL_ARRAY, 0, 1);
    SmartPointer<FastArrayVector> av1 = vec;

    VectorSP v1 = Util::createVector(DT_BOOL, 2, 2);
    EXPECT_TRUE(av1->append(v1));
    ConstantSP av2 = av1->getSubVector(0, -1, 1);
    EXPECT_EQ(av2->getString(), "[]");

    av2 = av1->getSubVector(1, -1, 2);
    EXPECT_EQ(av2->getString(), "[]");
}

TEST_F(ArrayVectorTest,testArrayVector_set) {
    VectorSP vec = Util::createArrayVector(DT_INT_ARRAY, 0, 2);
    SmartPointer<FastArrayVector> av1 = vec;

    VectorSP v1 = Util::createVector(DT_INT, 2, 2);
    v1->set(0, Util::createInt(0));
    v1->set(1, Util::createInt(1));
    EXPECT_TRUE(av1->append(v1));
    EXPECT_FALSE(av1->set(v1, Util::createInt(0)));

    VectorSP v2 = Util::createVector(DT_INT, 3, 3);
    v2->set(0, Util::createInt(0));
    v2->set(1, Util::createInt(1));
    v2->set(2, Util::createInt(2));
    EXPECT_FALSE(av1->set(v1, v2));

    VectorSP v3 = Util::createVector(DT_ANY, 1, 1);
    v3->set(0, Util::createInt(0));
    EXPECT_FALSE(av1->set(v1, v3));

    VectorSP v4 = Util::createVector(DT_INT, 2, 2);
    v4->set(0, Util::createInt(0));
    v4->set(1, Util::createInt(1));
    EXPECT_FALSE(av1->set(v1, v4));

    VectorSP v5 = Util::createVector(DT_INT_ARRAY, 0, 2);
    v5->append(v2);
    v5->append(v2);
    EXPECT_FALSE(av1->set(v1, v5));
}
#endif

TEST_F(ArrayVectorTest,test_BoolArrayVector){
	vector<char> testValues{ 1,-1,12,0,-12};
	VectorSP v1=Util::createVector(DT_BOOL,6,6);
	VectorSP anyv1 = Util::createVector(DT_ANY, 1);

	for (unsigned i=0;i<testValues.size();i++){
		v1->setBool(i,testValues[i]);
	}
	v1->setNull(5);

	VectorSP av1=Util::createArrayVector(DT_BOOL_ARRAY,0,1);
	VectorSP av2=Util::createArrayVector(DT_BOOL_ARRAY,1,1);
	VectorSP av3=Util::createArrayVector(DT_BOOL_ARRAY,1,1);
	VectorSP av4=Util::createArrayVector(DT_BOOL_ARRAY,0,1);
	av1->append(v1);
	av2->fill(0,1,v1);
	av3->set(0,v1);
	av4->append(Util::createVector(DT_BOOL,6,6));
	av4->set(Util::createConstant(DT_INT),v1);

	conn.upload("av1", { av1 });
	string script = "value = bool[1,-1,12,0,-12,NULL]\n\
					index = [6]\n\
					b=arrayVector(index, value);b";
	TableSP ex_av1 = conn.run(script);
	ConstantSP res = conn.run("eqObj(av1,b)");

	EXPECT_TRUE(res->getBool());
	EXPECT_EQ(av1->getString(),ex_av1->getString());
	EXPECT_EQ(av1->getType(),ex_av1->getType());

    EXPECT_EQ(av1->getRawType(),DT_BOOL);
    EXPECT_EQ(av1->getForm(),DF_VECTOR);
    EXPECT_FALSE(av1->validIndex(2));
    EXPECT_FALSE(av1->validIndex(0,1,2));
    EXPECT_EQ(av1->getUnitLength(),1);
    EXPECT_TRUE(av1->sizeable());
    // cout<<av1->count()<<endl;
    // cout<<av1->getSourceValue()->getString()<<endl;
    // cout<<av1->getSourceIndex()->getString()<<endl;
    EXPECT_TRUE(av1->isIndexArray());

    EXPECT_ANY_THROW(av1->compare(0,Util::createInt(1)));
    EXPECT_ANY_THROW(av1->neg());
    EXPECT_ANY_THROW(av1->prev(0));
    EXPECT_ANY_THROW(av1->next(0));
    int* buf = new int[1];
    EXPECT_ANY_THROW(av1->getHash(0,1,1,buf));
}

TEST_F(ArrayVectorTest,test_CharArrayVector){
	vector<char> testValues{ 1,-1,12,0,-12};
	VectorSP v1=Util::createVector(DT_CHAR,6,6);
	VectorSP anyv1 = Util::createVector(DT_ANY, 1);

	for (unsigned i=0;i<testValues.size();i++){
		v1->setChar(i,testValues[i]);
	}
	v1->setNull(5);
	anyv1->set(0, v1);

	VectorSP av1=Util::createArrayVector(DT_CHAR_ARRAY,0,1);
	av1->append(anyv1);
	conn.upload("av1", { av1 });
	string script = "value = char[1,-1,12,0,-12,NULL]\n\
					index = [6]\n\
					b=arrayVector(index, value);b";
	TableSP ex_av1 = conn.run(script);
	ConstantSP res = conn.run("eqObj(av1,b)");

	EXPECT_TRUE(res->getBool());
	EXPECT_EQ(av1->getString(),ex_av1->getString());
	EXPECT_EQ(av1->getType(),ex_av1->getType());

}


TEST_F(ArrayVectorTest,test_ShortArrayVector){
	vector<short> testValues{ 1,-1,12,0,-12};
	VectorSP v1=Util::createVector(DT_SHORT,6,6);
	VectorSP anyv1 = Util::createVector(DT_ANY, 1);

	for (unsigned i=0;i<testValues.size();i++){
		v1->setShort(i,testValues[i]);
	}
	v1->setNull(5);
	anyv1->set(0, v1);

	VectorSP av1=Util::createArrayVector(DT_SHORT_ARRAY,0,1);
	av1->append(anyv1);
	conn.upload("av1", { av1 });
	string script = "value = short[1,-1,12,0,-12,NULL]\n\
					index = [6]\n\
					b=arrayVector(index, value);b";
	TableSP ex_av1 = conn.run(script);
	ConstantSP res = conn.run("eqObj(av1,b)");

	EXPECT_TRUE(res->getBool());
	EXPECT_EQ(av1->getString(),ex_av1->getString());
	EXPECT_EQ(av1->getType(),ex_av1->getType());

}

TEST_F(ArrayVectorTest,test_IntArrayVector){
	vector<int> testValues{ 1,-1,12,0,-12};
	VectorSP v1=Util::createVector(DT_INT,6,6);
	VectorSP anyv1 = Util::createVector(DT_ANY, 1);

	for (unsigned i=0;i<testValues.size();i++){
		v1->setInt(i,testValues[i]);
	}
	v1->setNull(5);
	anyv1->set(0, v1);

	VectorSP av1=Util::createArrayVector(DT_INT_ARRAY,0,1);
	av1->append(anyv1);
	conn.upload("av1", { av1 });
	string script = "value = int[1,-1,12,0,-12,NULL]\n\
					index = [6]\n\
					b=arrayVector(index, value);b";
	TableSP ex_av1 = conn.run(script);
	ConstantSP res = conn.run("eqObj(av1,b)");


	EXPECT_TRUE(res->getBool());
	EXPECT_EQ(av1->getString(),ex_av1->getString());
	EXPECT_EQ(av1->getType(),ex_av1->getType());

    VectorSP inst_av1 =  av1->getInstance(0);
    EXPECT_EQ(inst_av1->getString(),"[]");
    VectorSP inst_av2 =  av1->getInstance(1);
    EXPECT_EQ(inst_av2->size(),1);

    VectorSP indexVec = Util::createIndexVector(0,1);
    ConstantSP index = Util::createInt(0);
    INDEX ind = 0;
    ConstantSP val_1 = Util::createInt(1000);
    VectorSP val_v1 = Util::createVector(DT_INT,1,1);
    val_v1->set(0,Util::createInt(2000));
    VectorSP val_v2 = Util::createVector(DT_INT,1,1);
    val_v2->set(0,Util::createInt(3000));
    VectorSP val_t3 = Util::createVector(DT_ANY,1,1);
    val_t3->set(0,Util::createInt(4000));
    VectorSP val_av1 = Util::createArrayVector(DT_INT_ARRAY,0,1);
    VectorSP val_av2 = Util::createArrayVector(DT_INT_ARRAY,0,2);
    val_av1->append(val_v2);

    EXPECT_FALSE(av1->set(ind, val_av2));
    av1->set(ind, val_1);
    EXPECT_EQ(av1->getString(0),"[1000]");
    av1->set(ind, val_v1);
    EXPECT_EQ(av1->getString(0),"[2000]");
    av1->set(ind, val_av1);
    EXPECT_EQ(av1->getString(0),"[3000]");
    av1->set(ind, val_t3);
    EXPECT_EQ(av1->getString(0),"[4000]");
    val_t3->append(Util::createInt(4000));
    EXPECT_FALSE(av1->set(ind, val_t3));

    av1->set(0,val_v2);
    val_t3->remove(1);
    av1->set(indexVec, val_1);
    EXPECT_EQ(av1->getString(0),"[1000]");
    av1->set(indexVec, val_v1);
    EXPECT_EQ(av1->getString(0),"[2000]");
    av1->set(indexVec, val_av1);
    EXPECT_EQ(av1->getString(0),"[3000]");
    av1->set(indexVec, val_t3);
    EXPECT_EQ(av1->getString(0),"[4000]");
    val_t3->append(Util::createInt(4000));
    EXPECT_FALSE(av1->set(indexVec, val_t3));

    val_t3->remove(1);
    TableSP tab1 = conn.run("table(1 2 3 as col1)");
    EXPECT_ANY_THROW(av1->fill(0,10,val_1));
    EXPECT_ANY_THROW(av1->fill(0,1,tab1));
    EXPECT_ANY_THROW(av1->fill(1,1,val_1));
    
    av1->fill(0,1, val_1);
    EXPECT_EQ(av1->getString(),"[[1000]]");
    av1->append(val_1);
    val_av1->append(val_v2);

    av1->fill(0,2, val_av1);
    EXPECT_EQ(av1->getString(),"[[3000],[3000]]");
    val_t3->append(Util::createInt(4000));
    av1->fill(0,2, val_t3);
    EXPECT_EQ(av1->getString(),"[[4000],[4000]]");

    av1->set(0,val_v1);

    av1->reverse(0,0);
    av1->reverse();
    EXPECT_EQ(av1->getString(),"[[4000],[2000]]");
    av1->reverse(0,2);
    EXPECT_EQ(av1->getString(),"[[2000],[4000]]");

    av1->append(val_1,1);
    EXPECT_EQ(av1->getString(),"[[2000],[4000],[1000]]");
    av1->append(val_av1,1);
    EXPECT_EQ(av1->getString(),"[[2000],[4000],[1000],[3000]]");
    av1->append(val_t3,1);
    EXPECT_EQ(av1->getString(),"[[2000],[4000],[1000],[3000],[4000],[4000]]");

    EXPECT_FALSE(av1->remove(10));
    av1->remove(2);
    EXPECT_EQ(av1->getString(),"[[2000],[4000],[1000],[3000]]");
    av1->remove(-1);
    EXPECT_EQ(av1->getString(),"[[4000],[1000],[3000]]");
    av1->remove(3);
    EXPECT_EQ(av1->getString(),"[]");
    av1->append(val_v1);
    av1->remove(-1);
    EXPECT_EQ(av1->getString(),"[]");

    av1->append(val_v1);
    av1->append(val_v2);
    av1->append(val_t3);
    VectorSP nulIndexVec = Util::createIndexVector(0,0);
    VectorSP IndexVec = Util::createIndexVector(0,1);
    VectorSP IndexVec1 = Util::createIndexVector(0,3);
    EXPECT_TRUE(av1->remove(nulIndexVec));
    EXPECT_FALSE(av1->remove(Util::createInt(1)));
    av1->remove(IndexVec);
    EXPECT_EQ(av1->getString(),"[[3000],[4000],[4000]]");
    av1->remove(IndexVec1);
    EXPECT_EQ(av1->getString(),"[]");

    av1->append(val_v1);
    av1->append(val_v2);
    av1->append(val_t3);
    EXPECT_EQ(av1->get(0,0,1)->getString(),av1->get(0)->getString());
    EXPECT_EQ(av1->get(0,1,2)->getString(),av1->get(1)->getString());
    EXPECT_EQ(av1->get(0,2,3)->getString(),av1->get(2)->getString());

    EXPECT_FALSE(av1->isNull());
    av1->append(Util::createNullConstant(DT_INT));
    EXPECT_FALSE(av1->isNull(3));
    EXPECT_TRUE(av1->isNull(4));

    char* buf = new char[5];
    char* buf1 = new char[5];
    av1->isNull(0,5,buf);
    av1->isValid(0,5,buf1);
    for(int i=0;i<4;i++){
        EXPECT_TRUE((int)buf1[i]);
        EXPECT_FALSE((int)buf[i]);
    }
    EXPECT_TRUE((int)buf[4]);
    EXPECT_FALSE((int)buf1[4]);

    delete[] buf,buf1;

    for(unsigned int i=0;i<121;i++)
        val_v1->append(Util::createInt(1));
    av1->set(0,val_v1);
    EXPECT_EQ(av1->getString(0),"[2000,1,1...]");
    EXPECT_EQ(av1->getString(),"[[2000,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1...],[3000],[4000],[4000],[]]");

}

TEST_F(ArrayVectorTest,test_LongArrayVector){
	vector<long> testValues{ 1,-1,12,0,-12};
	VectorSP v1=Util::createVector(DT_LONG,6,6);
	VectorSP anyv1 = Util::createVector(DT_ANY, 1);

	for (unsigned i=0;i<testValues.size();i++){
		v1->setLong(i,testValues[i]);
	}
	v1->setNull(5);
	anyv1->set(0, v1);

	VectorSP av1=Util::createArrayVector(DT_LONG_ARRAY,0,1);
	av1->append(anyv1);
	conn.upload("av1", { av1 });
	string script = "value = long[1,-1,12,0,-12,NULL]\n\
					index = [6]\n\
					b=arrayVector(index, value);b";
	TableSP ex_av1 = conn.run(script);
	ConstantSP res = conn.run("eqObj(av1,b)");


	EXPECT_TRUE(res->getBool());
	EXPECT_EQ(av1->getString(),ex_av1->getString());
	EXPECT_EQ(av1->getType(),ex_av1->getType());

}

TEST_F(ArrayVectorTest,test_DateArrayVector){
	vector<int> testValues{ 1,-1,12,0,-12};
	VectorSP v1=Util::createVector(DT_DATE,6,6);
	VectorSP anyv1 = Util::createVector(DT_ANY, 1);

	for (unsigned i=0;i<testValues.size();i++){
		v1->setInt(i,testValues[i]);
	}
	v1->setNull(5);
	anyv1->set(0, v1);

	VectorSP av1=Util::createArrayVector(DT_DATE_ARRAY,0,1);
	av1->append(anyv1);
	conn.upload("av1", { av1 });
	string script = "value = date[1,-1,12,0,-12,NULL];index = [6];b=arrayVector(index, value);b";
	TableSP ex_av1 = conn.run(script);
	ConstantSP res = conn.run("eqObj(av1,b)");


	EXPECT_TRUE(res->getBool());
	EXPECT_EQ(av1->getString(),ex_av1->getString());
	EXPECT_EQ(av1->getType(),ex_av1->getType());

}

TEST_F(ArrayVectorTest,test_MonthArrayVector){
	vector<int> testValues{ 1,-1,12,0,-12};
	VectorSP v1=Util::createVector(DT_MONTH,6,6);
	VectorSP anyv1 = Util::createVector(DT_ANY, 1);

	for (unsigned i=0;i<testValues.size();i++){
		v1->setInt(i,testValues[i]);
	}
	v1->setNull(5);
	anyv1->set(0, v1);

	VectorSP av1=Util::createArrayVector(DT_MONTH_ARRAY,0,1);
	av1->append(anyv1);
	conn.upload("av1", { av1 });
	string script = "value = month[1,-1,12,0,-12,NULL]\n\
					index = [6]\n\
					b=arrayVector(index, value);b";
	TableSP ex_av1 = conn.run(script);
	ConstantSP res = conn.run("eqObj(av1,b)");


	EXPECT_TRUE(res->getBool());
	EXPECT_EQ(av1->getString(),ex_av1->getString());
	EXPECT_EQ(av1->getType(),ex_av1->getType());

}

TEST_F(ArrayVectorTest,test_TimeArrayVector){
	vector<int> testValues{ 1,123123,12,0,111};
	VectorSP v1=Util::createVector(DT_TIME,6,6);
	VectorSP anyv1 = Util::createVector(DT_ANY, 1);

	for (unsigned i=0;i<testValues.size();i++){
		v1->setInt(i,testValues[i]);
	}
	v1->setNull(5);
	anyv1->set(0, v1);

	VectorSP av1=Util::createArrayVector(DT_TIME_ARRAY,0,1);
	av1->append(anyv1);
	conn.upload("av1", { av1 });
	string script = "value = time[1,123123,12,0,111,NULL]\n\
					index = [6]\n\
					b=arrayVector(index, value);b";
	TableSP ex_av1 = conn.run(script);
	ConstantSP res = conn.run("eqObj(av1,b)");


	EXPECT_TRUE(res->getBool());
	EXPECT_EQ(av1->getString(),ex_av1->getString());
	EXPECT_EQ(av1->getType(),ex_av1->getType());

}

TEST_F(ArrayVectorTest,test_MinuteArrayVector){
	vector<int> testValues{ 1,120,12,0,111};
	VectorSP v1=Util::createVector(DT_MINUTE,6,6);
	VectorSP anyv1 = Util::createVector(DT_ANY, 1);

	for (unsigned i=0;i<testValues.size();i++){
		v1->setInt(i,testValues[i]);
	}
	v1->setNull(5);
	anyv1->set(0, v1);

	VectorSP av1=Util::createArrayVector(DT_MINUTE_ARRAY,0,1);
	av1->append(anyv1);
	conn.upload("av1", { av1 });
	string script = "value = minute[1,120,12,0,111,NULL]\n\
					index = [6]\n\
					b=arrayVector(index, value);b";
	TableSP ex_av1 = conn.run(script);
	ConstantSP res = conn.run("eqObj(av1,b)");


	EXPECT_TRUE(res->getBool());
	EXPECT_EQ(av1->getString(),ex_av1->getString());
	EXPECT_EQ(av1->getType(),ex_av1->getType());

}

TEST_F(ArrayVectorTest,test_SecondArrayVector){
	vector<int> testValues{ 1,123,12,0,86399};
	VectorSP v1=Util::createVector(DT_SECOND,6,6);
	VectorSP anyv1 = Util::createVector(DT_ANY, 1);

	for (unsigned i=0;i<testValues.size();i++){
		v1->setInt(i,testValues[i]);
	}
	v1->setNull(5);
	anyv1->set(0, v1);

	VectorSP av1=Util::createArrayVector(DT_SECOND_ARRAY,0,1);
	av1->append(anyv1);
	conn.upload("av1", { av1 });
	string script = "value = second[1,123,12,0,86399,NULL]\n\
					index = [6]\n\
					b=arrayVector(index, value);b";
	TableSP ex_av1 = conn.run(script);
	ConstantSP res = conn.run("eqObj(av1,b)");


	EXPECT_TRUE(res->getBool());
	EXPECT_EQ(av1->getString(),ex_av1->getString());
	EXPECT_EQ(av1->getType(),ex_av1->getType());

}

TEST_F(ArrayVectorTest,test_DatetimeArrayVector){
	vector<int> testValues{ 1,-1,12,0,-12};
	VectorSP v1=Util::createVector(DT_DATETIME,6,6);
	VectorSP anyv1 = Util::createVector(DT_ANY, 1);

	for (unsigned i=0;i<testValues.size();i++){
		v1->setInt(i,testValues[i]);
	}
	v1->setNull(5);
	anyv1->set(0, v1);

	VectorSP av1=Util::createArrayVector(DT_DATETIME_ARRAY,0,1);
	av1->append(anyv1);
	conn.upload("av1", { av1 });
	string script = "value = datetime[1,-1,12,0,-12,NULL]\n\
					index = [6]\n\
					b=arrayVector(index, value);b";
	TableSP ex_av1 = conn.run(script);
	ConstantSP res = conn.run("eqObj(av1,b)");


	EXPECT_TRUE(res->getBool());
	EXPECT_EQ(av1->getString(),ex_av1->getString());
	EXPECT_EQ(av1->getType(),ex_av1->getType());

}


TEST_F(ArrayVectorTest,test_TimestampArrayVector){
	vector<int> testValues{ 1,-1,12,0,-12};
	VectorSP v1=Util::createVector(DT_TIMESTAMP,6,6);
	VectorSP anyv1 = Util::createVector(DT_ANY, 1);

	for (unsigned i=0;i<testValues.size();i++){
		v1->setInt(i,testValues[i]);
	}
	v1->setNull(5);
	anyv1->set(0, v1);

	VectorSP av1=Util::createArrayVector(DT_TIMESTAMP_ARRAY,0,1);
	av1->append(anyv1);
	conn.upload("av1", { av1 });
	string script = "value = timestamp[1,-1,12,0,-12,NULL]\n\
					index = [6]\n\
					b=arrayVector(index, value);b";
	TableSP ex_av1 = conn.run(script);
	ConstantSP res = conn.run("eqObj(av1,b)");


	EXPECT_TRUE(res->getBool());
	EXPECT_EQ(av1->getString(),ex_av1->getString());
	EXPECT_EQ(av1->getType(),ex_av1->getType());

}


TEST_F(ArrayVectorTest,test_NanotimeArrayVector){
	vector<long long> testValues{ 1,123,12,0,10000000000000};
	VectorSP v1=Util::createVector(DT_NANOTIME,6,6);
	VectorSP anyv1 = Util::createVector(DT_ANY, 1);

	for (unsigned i=0;i<testValues.size();i++){
		v1->setLong(i,testValues[i]);
	}
	v1->setNull(5);
	anyv1->set(0, v1);

	VectorSP av1=Util::createArrayVector(DT_NANOTIME_ARRAY,0,1);
	av1->append(anyv1);
	conn.upload("av1", { av1 });
	string script = "value = nanotime[1,123,12,0,10000000000000,NULL]\n\
					index = [6]\n\
					b=arrayVector(index, value);b";
	TableSP ex_av1 = conn.run(script);
	ConstantSP res = conn.run("eqObj(av1,b)");


	EXPECT_TRUE(res->getBool());
	EXPECT_EQ(av1->getString(),ex_av1->getString());
	EXPECT_EQ(av1->getType(),ex_av1->getType());

}

TEST_F(ArrayVectorTest,test_NanotimestampArrayVector){
	vector<long long> testValues{ 1,-1,-12,0,100000000000000000};
	VectorSP v1=Util::createVector(DT_NANOTIMESTAMP,6,6);
	VectorSP anyv1 = Util::createVector(DT_ANY, 1);

	for (unsigned i=0;i<testValues.size();i++){
		v1->setLong(i,testValues[i]);
	}
	v1->setNull(5);
	anyv1->set(0, v1);

	VectorSP av1=Util::createArrayVector(DT_NANOTIMESTAMP_ARRAY,0,1);
	av1->append(anyv1);
	conn.upload("av1", { av1 });
	string script = "value = nanotimestamp[1,-1,-12,0,100000000000000000,NULL]\n\
					index = [6]\n\
					b=arrayVector(index, value);b";
	TableSP ex_av1 = conn.run(script);
	ConstantSP res = conn.run("eqObj(av1,b)");


	EXPECT_TRUE(res->getBool());
	EXPECT_EQ(av1->getString(),ex_av1->getString());
	EXPECT_EQ(av1->getType(),ex_av1->getType());

}

TEST_F(ArrayVectorTest,test_FloatArrayVector){
	vector<float> testValues{ 1.522222f,-1.5f,-12.0f,0,100000000000000000.1f};
	VectorSP v1=Util::createVector(DT_FLOAT,6,6);
	VectorSP anyv1 = Util::createVector(DT_ANY, 1);

	for (unsigned i=0;i<testValues.size();i++){
		v1->setFloat(i,testValues[i]);
	}
	v1->setNull(5);
	anyv1->set(0, v1);

	VectorSP av1=Util::createArrayVector(DT_FLOAT_ARRAY,0,1);
	av1->append(anyv1);
	conn.upload("av1", { av1 });
	string script = "value = float[1.522222,-1.5,-12.0,0,100000000000000000.1,NULL]\n\
					index = [6]\n\
					b=arrayVector(index, value);b";
	TableSP ex_av1 = conn.run(script);
	ConstantSP res = conn.run("eqObj(av1,b)");


	EXPECT_TRUE(res->getBool());
	EXPECT_EQ(av1->getString(),ex_av1->getString());
	EXPECT_EQ(av1->getType(),ex_av1->getType());

}

TEST_F(ArrayVectorTest,test_DoubleArrayVector){
	vector<double> testValues{ 1.533333333333,-1.5,-12.0,0,100000000000000000.1};
	VectorSP v1=Util::createVector(DT_DOUBLE,6,6);
	VectorSP anyv1 = Util::createVector(DT_ANY, 1);

	for (unsigned i=0;i<testValues.size();i++){
		v1->setDouble(i,testValues[i]);
	}
	v1->setNull(5);
	anyv1->set(0, v1);

	VectorSP av1=Util::createArrayVector(DT_DOUBLE_ARRAY,0,1);
	av1->append(anyv1);
	conn.upload("av1", { av1 });
	string script = "value = double[1.533333333333,-1.5,-12.0,0,100000000000000000.1,NULL]\n\
					index = [6]\n\
					b=arrayVector(index, value);b";
	TableSP ex_av1 = conn.run(script);
	ConstantSP res = conn.run("eqObj(av1,b)");


	EXPECT_TRUE(res->getBool());
	EXPECT_EQ(av1->getString(),ex_av1->getString());
	EXPECT_EQ(av1->getType(),ex_av1->getType());

}

TEST_F(ArrayVectorTest,test_SymbolArrayVector){
	vector<string> testValues{ "a123","智臾科技a","你好！a","~`!@#$%^&*()/*-a","~·！@#￥%……&*（）+——a"};
	VectorSP v1=Util::createVector(DT_SYMBOL,6,6);
	VectorSP anyv1 = Util::createVector(DT_ANY, 1);

	for (unsigned i=0;i<testValues.size();i++){
		v1->setString(i,testValues[i]);
	}
	v1->setNull(5);
	anyv1->set(0, v1);

	EXPECT_ANY_THROW(VectorSP av1=Util::createArrayVector(DT_SYMBOL_ARRAY,0,1));

}

TEST_F(ArrayVectorTest,test_StringArrayVector){
	vector<string> testValues{ "a123","智臾科技a","你好！a","~`!@#$%^&*()/*-a","~·！@#￥%……&*（）+——a"};
	VectorSP v1=Util::createVector(DT_STRING,6,6);
	VectorSP anyv1 = Util::createVector(DT_ANY, 1);

	for (unsigned i=0;i<testValues.size();i++){
		v1->setString(i,testValues[i]);
	}
	v1->setNull(5);
	anyv1->set(0, v1);

	EXPECT_ANY_THROW(VectorSP av1=Util::createArrayVector(DT_STRING_ARRAY,0,1));

}

TEST_F(ArrayVectorTest,test_UuidArrayVector){
	vector<string> testValues{ "5d212a78-cc48-e3b1-4235-b4d91473ee87","5d212a78-cc48-e3b1-4235-b4d91473ee88",\
								"5d212a78-cc48-e3b1-4235-b4d91473ee89","5d212a78-cc48-e3b1-4235-b4d91473ee90","5d212a78-cc48-e3b1-4235-b4d91473ee91"};
	VectorSP v1=Util::createVector(DT_UUID,6,6);
	VectorSP anyv1 = Util::createVector(DT_ANY, 1);

	for (unsigned i=0;i<testValues.size();i++){
		v1->setString(i,testValues[i]);
	}
	v1->setNull(5);
	anyv1->set(0, v1);

	VectorSP av1=Util::createArrayVector(DT_UUID_ARRAY,0,1);
	av1->append(anyv1);
	conn.upload("av1", { av1 });
	string script = "value = uuid[\"5d212a78-cc48-e3b1-4235-b4d91473ee87\",\"5d212a78-cc48-e3b1-4235-b4d91473ee88\",\
								\"5d212a78-cc48-e3b1-4235-b4d91473ee89\",\"5d212a78-cc48-e3b1-4235-b4d91473ee90\",\"5d212a78-cc48-e3b1-4235-b4d91473ee91\",NULL]\n\
					index = [6]\n\
					b=arrayVector(index, value);b";
	TableSP ex_av1 = conn.run(script);
	ConstantSP res = conn.run("eqObj(av1,b)");


	EXPECT_TRUE(res->getBool());
	EXPECT_EQ(av1->getString(),ex_av1->getString());
	EXPECT_EQ(av1->getType(),ex_av1->getType());

}

TEST_F(ArrayVectorTest,test_AnyArrayVector){
	VectorSP v1=Util::createVector(DT_ANY,6,6);
	VectorSP anyv1 = Util::createVector(DT_ANY, 1);

	v1->setInt(0,5);
	v1->setDouble(1,1.326586);
	v1->setString(2,"abc");
	v1->setBool(3,1);
	v1->setShort(4,5);
	v1->setNull(5);
	anyv1->set(0, v1);

	EXPECT_ANY_THROW(VectorSP av1=Util::createArrayVector(DT_ANY_ARRAY,0,1));

}

TEST_F(ArrayVectorTest,test_BlobArrayVector){
	vector<string> testValues{ "a123","智臾科技a","你好！a","~`!@#$%^&*()/*-a","~·！@#￥%……&*（）+——a"};
	VectorSP v1=Util::createVector(DT_BLOB,6,6);
	VectorSP anyv1 = Util::createVector(DT_ANY, 1);

	for (unsigned i=0;i<testValues.size();i++){
		v1->setString(i,testValues[i]);
	}
	v1->setNull(5);
	anyv1->set(0, v1);

	EXPECT_ANY_THROW(VectorSP av1=Util::createArrayVector(DT_BLOB_ARRAY,0,1));

}

TEST_F(ArrayVectorTest,test_CompressArrayVector){
	vector<int> testValues{ 1,-1,12,0,-12};
	EXPECT_ANY_THROW(VectorSP v1=Util::createVector(DT_COMPRESS,6,6));
}


TEST_F(ArrayVectorTest,test_DatehourArrayVector){
	vector<int> testValues{ 1,-1,12,0,-12};
	VectorSP v1=Util::createVector(DT_DATEHOUR,6,6);
	VectorSP anyv1 = Util::createVector(DT_ANY, 1);

	for (unsigned i=0;i<testValues.size();i++){
		v1->setInt(i,testValues[i]);
	}
	v1->setNull(5);
	anyv1->set(0, v1);

	VectorSP av1=Util::createArrayVector(DT_DATEHOUR_ARRAY,0,1);
	av1->append(anyv1);
	conn.upload("av1", { av1 });
	string script = "value = datehour[1,-1,12,0,-12,NULL]\n\
					index = [6]\n\
					b=arrayVector(index, value);b";
	TableSP ex_av1 = conn.run(script);
	ConstantSP res = conn.run("eqObj(av1,b)");


	EXPECT_TRUE(res->getBool());
	EXPECT_EQ(av1->getString(),ex_av1->getString());
	EXPECT_EQ(av1->getType(),ex_av1->getType());

}

TEST_F(ArrayVectorTest,test_IpaddrArrayVector){
	vector<string> testValues{ "192.168.1.13","192.168.1.14",\
								"192.168.1.15","192.168.1.16","192.168.1.17"};
	VectorSP v1=Util::createVector(DT_IP,6,6);
	VectorSP anyv1 = Util::createVector(DT_ANY, 1);

	for (unsigned i=0;i<testValues.size();i++){
		v1->setString(i,testValues[i]);
	}
	v1->setNull(5);
	anyv1->set(0, v1);

	VectorSP av1=Util::createArrayVector(DT_IP_ARRAY,0,1);
	av1->append(anyv1);
	conn.upload("av1", { av1 });
	string script = "value = ipaddr[\"192.168.1.13\",\"192.168.1.14\",\
								\"192.168.1.15\",\"192.168.1.16\",\"192.168.1.17\",NULL]\n\
					index = [6]\n\
					b=arrayVector(index, value);b";
	TableSP ex_av1 = conn.run(script);
	ConstantSP res = conn.run("eqObj(av1,b)");


	EXPECT_TRUE(res->getBool());
	EXPECT_EQ(av1->getString(),ex_av1->getString());
	EXPECT_EQ(av1->getType(),ex_av1->getType());

}

TEST_F(ArrayVectorTest,test_Int128ArrayVector){
	vector<string> testValues{ "e1671797c52e15f763380b45e841ec32","e1671797c52e15f763380b45e841ec33",\
								"e1671797c52e15f763380b45e841ec34","e1671797c52e15f763380b45e841ec35","e1671797c52e15f763380b45e841ec36"};
	VectorSP v1=Util::createVector(DT_INT128,6,6);
	VectorSP anyv1 = Util::createVector(DT_ANY, 1);

	for (unsigned i=0;i<testValues.size();i++){
		v1->setString(i,testValues[i]);
	}
	v1->setNull(5);
	anyv1->set(0, v1);

	VectorSP av1=Util::createArrayVector(DT_INT128_ARRAY,0,1);
	av1->append(anyv1);
	conn.upload("av1", { av1 });
	string script = "value = int128[\"e1671797c52e15f763380b45e841ec32\",\"e1671797c52e15f763380b45e841ec33\",\
								\"e1671797c52e15f763380b45e841ec34\",\"e1671797c52e15f763380b45e841ec35\",\"e1671797c52e15f763380b45e841ec36\",NULL]\n\
					index = [6]\n\
					b=arrayVector(index, value);b";
	TableSP ex_av1 = conn.run(script);
	ConstantSP res = conn.run("eqObj(av1,b)");


	EXPECT_TRUE(res->getBool());
	EXPECT_EQ(av1->getString(),ex_av1->getString());
	EXPECT_EQ(av1->getType(),ex_av1->getType());

}

TEST_F(ArrayVectorTest,testUploadDatetimeArrayVector){
    //
    VectorSP value;
    VectorSP index;
    VectorSP data;
    VectorSP res;
    index = Util::createVector(DT_INT,4,10);
    value = Util::createVector(DT_DATETIME,10,20);
    for(int i=0;i<10;++i){
        value->set(i,Util::createDateTime(i+1));
    }
    index->set(0,Util::createInt(2));
    index->set(1,Util::createInt(5));
    index->set(2,Util::createInt(8));
    index->set(3,Util::createInt(10));
    data = Util::createArrayVector(index,value);
    vector<ConstantSP> datas = {data};
    vector<string> dataname = { "datas1" };
    conn.upload(dataname,datas);
    res= conn.run("datas1+1");
    VectorSP expection = conn.run("a = take(datetime(1..10),10);res = arrayVector([2,5,8,10],a);res+1");
    EXPECT_EQ(res->getString(),expection->getString());
}


TEST_F(ArrayVectorTest,testUploadMonthArrayVector){
    //
    VectorSP value;
    VectorSP index;
    VectorSP data;
    VectorSP res;
    index = Util::createVector(DT_INT,4,10);
    value = Util::createVector(DT_MONTH,10,20);
    for(int i=0;i<10;++i){
        value->set(i,Util::createMonth(i+1));
    }
    index->set(0,Util::createInt(2));
    index->set(1,Util::createInt(5));
    index->set(2,Util::createInt(8));
    index->set(3,Util::createInt(10));
    data = Util::createArrayVector(index,value);
    vector<ConstantSP> datas = {data};
    vector<string> dataname = { "datas" };
    conn.upload(dataname,datas);
    res= conn.run("re1=datas+1;re1;");
    VectorSP expection = conn.run("a = take(month(1..10),10);res = arrayVector([2,5,8,10],a);re2=res+1;re2");
    EXPECT_EQ(res->getString(),expection->getString());
}

TEST_F(ArrayVectorTest,testUploadDoubleArrayVector){
    //
    VectorSP value;
    VectorSP index;
    VectorSP data;
    VectorSP res;
    index = Util::createVector(DT_INT,4,10);
    value = Util::createVector(DT_DOUBLE,10,20);
    for(int i=0;i<10;++i){
        value->set(i,Util::createDouble(i+1.1));
    }
    index->set(0,Util::createInt(2));
    index->set(1,Util::createInt(5));
    index->set(2,Util::createInt(8));
    index->set(3,Util::createInt(10));
    data = Util::createArrayVector(index,value);
    vector<ConstantSP> datas = {data};
    vector<string> dataname = { "datas3" };
    conn.upload(dataname,datas);
    res= conn.run("datas3+1");
    VectorSP expection = conn.run("a = take(1..10,10)+0.1;res = arrayVector([2,5,8,10],a);res+1");
    EXPECT_EQ(res->getString(),expection->getString());
}

TEST_F(ArrayVectorTest,testUploadDatehourArrayVector){
    //
    VectorSP value;
    VectorSP index;
    VectorSP data;
    VectorSP res;
    index = Util::createVector(DT_INT,4,10);
    value = Util::createVector(DT_DATEHOUR,10,20);
    for(int i=0;i<10;++i){
        value->set(i,Util::createDateHour(i+1));
    }
    index->set(0,Util::createInt(2));
    index->set(1,Util::createInt(5));
    index->set(2,Util::createInt(8));
    index->set(3,Util::createInt(10));
    data = Util::createArrayVector(index,value);
    vector<ConstantSP> datas = {data};
    vector<string> dataname = { "datas" };
    conn.upload(dataname,datas);
    res= conn.run("datas+1");
    VectorSP expection = conn.run("a = take(datehour(1..10),10);res = arrayVector([2,5,8,10],a);res+1");
    EXPECT_EQ(res->getString(),expection->getString());
}


TEST_F(ArrayVectorTest,testUploadTimeArrayVector){
    //
    VectorSP value;
    VectorSP index;
    VectorSP data;
    index = Util::createVector(DT_INT,4,10);
    value = Util::createVector(DT_TIME,10,20);
    for(int i=0;i<10;++i){
        value->set(i,Util::createTime(i+1000));
    }
    index->set(0,Util::createInt(2));
    index->set(1,Util::createInt(5));
    index->set(2,Util::createInt(8));
    index->set(3,Util::createInt(10));
    data = Util::createArrayVector(index,value);
    vector<ConstantSP> datas = {data};
    vector<string> dataname = {"datas4"};
    conn.upload(dataname,datas);
    VectorSP expection = conn.run("a = take(time(0..9+1000),10);res = arrayVector([2,5,8,10],a);re2=res+1;re2;");
    VectorSP res = conn.run("datas4+1;");
    EXPECT_EQ(res->getString(),expection->getString());
}



TEST_F(ArrayVectorTest,testUploadNanotimeArrayVector){
    //
    VectorSP value;
    VectorSP index;
    VectorSP data;
    VectorSP res;
    index = Util::createVector(DT_INT,4,10);
    value = Util::createVector(DT_NANOTIME,10,20);
    for(int i=0;i<10;++i){
        value->set(i,Util::createNanoTime(i+1));
    }
    index->set(0,Util::createInt(2));
    index->set(1,Util::createInt(5));
    index->set(2,Util::createInt(8));
    index->set(3,Util::createInt(10));
    data = Util::createArrayVector(index,value);
    vector<ConstantSP> datas = {data};
    vector<string> dataname = { "datas" };
    conn.upload(dataname,datas);
    res= conn.run("datas+1");
    VectorSP expection = conn.run("a = take(nanotime(1..10),10);res = arrayVector([2,5,8,10],a);res+1");
    EXPECT_EQ(res->getString(),expection->getString());
}


TEST_F(ArrayVectorTest,testUploadTimeStampArrayVector){
    //
    VectorSP value;
    VectorSP index;
    VectorSP data;
    VectorSP res;
    index = Util::createVector(DT_INT,4,10);
    value = Util::createVector(DT_TIMESTAMP,10,20);
    for(int i=0;i<10;++i){
        value->set(i,Util::createTimestamp(i+1));
    }
    index->set(0,Util::createInt(2));
    index->set(1,Util::createInt(5));
    index->set(2,Util::createInt(8));
    index->set(3,Util::createInt(10));
    data = Util::createArrayVector(index,value);
    vector<ConstantSP> datas = {data};
    vector<string> dataname = { "datas" };
    conn.upload(dataname,datas);
    res= conn.run("datas+1");
    VectorSP expection = conn.run("a = take(timestamp(1..10),10);res = arrayVector([2,5,8,10],a);res+1");
    EXPECT_EQ(res->getString(),expection->getString());
}


TEST_F(ArrayVectorTest,testUploadIntArrayVector){
    //
    VectorSP value;
    VectorSP index;
    VectorSP data;
    VectorSP res;
    VectorSP nonFastindex = Util::createVector(DT_INT,4,10,false);
    VectorSP nullValue = Util::createVector(DT_INT,10,20);
    index = Util::createVector(DT_INT,4,10);
    value = Util::createVector(DT_INT,10,20);
    for(int i=0;i<10;++i){
        value->set(i,Util::createInt(i+1));
        nullValue->setNull(i);
    }
    index->set(0,Util::createInt(2));
    index->set(1,Util::createInt(5));
    index->set(2,Util::createInt(8));
    index->set(3,Util::createInt(10));

    VectorSP nullData = Util::createArrayVector(index,nullValue);
    data = Util::createArrayVector(index,value);
    vector<ConstantSP> datas = {data};
    vector<string> dataname = { "datas" };
    conn.upload(dataname,datas);
    VectorSP expection = conn.run("a = take(1..10,10);res = arrayVector([2,5,8,10],a);re2=res+1;re2;");
    res= conn.run("re1=datas+1;re1;");
    EXPECT_ANY_THROW(data=Util::createArrayVector(nonFastindex,value));
    EXPECT_EQ(nullData->getString(),"[[,],[,,],[,,],[,]]");
    EXPECT_EQ(res->getString(),expection->getString());
}



TEST_F(ArrayVectorTest,testUploadDateArrayVector){
    //
    VectorSP value;
    VectorSP index;
    VectorSP data;
    VectorSP res;
    index = Util::createVector(DT_INT,4,10);
    value = Util::createVector(DT_DATE,10,20);
    for(int i=0;i<10;++i){
        value->set(i,Util::createDate(2012,1,2));
    }
    index->set(0,Util::createInt(2));
    index->set(1,Util::createInt(5));
    index->set(2,Util::createInt(8));
    index->set(3,Util::createInt(10));
    data = Util::createArrayVector(index,value);
    vector<ConstantSP> datas = {data};
    vector<string> dataname = { "datas" };
    conn.upload(dataname,datas);
    res= conn.run("re1=datas+1;re1");
    VectorSP expection = conn.run("a = take(2012.01.02,10);res = arrayVector([2,5,8,10],a);re2=res+1;re2;");
    EXPECT_EQ(res->getString(),expection->getString());
}


TEST_F(ArrayVectorTest,testUploadCharArrayVector){
    //
    VectorSP value;
    VectorSP index;
    VectorSP data;
    VectorSP res;
    index = Util::createVector(DT_INT,4,10);
    value = Util::createVector(DT_CHAR,10,20);
    for(int i=0;i<10;++i){
        value->set(i,Util::createChar(i+1));
    }
    index->set(0,Util::createInt(2));
    index->set(1,Util::createInt(5));
    index->set(2,Util::createInt(8));
    index->set(3,Util::createInt(10));
    data = Util::createArrayVector(index,value);
    vector<ConstantSP> datas = {data};
    vector<string> dataname = { "datas1" };
    conn.upload(dataname,datas);
    res= conn.run("datas1+1");
    VectorSP expection = conn.run("a = take(char(1..10),10);res = arrayVector([2,5,8,10],a);res+1");
    EXPECT_EQ(res->getString(),expection->getString());
}



TEST_F(ArrayVectorTest,testUploadBoolArrayVector){
    //
    VectorSP value;
    VectorSP index;
    VectorSP data;
    VectorSP res;
    index = Util::createVector(DT_INT,4,10);
    value = Util::createVector(DT_BOOL,10,20);
    for(int i=0;i<10;++i){
        value->set(i,Util::createBool((i+1)%2));
    }
    index->set(0,Util::createInt(2));
    index->set(1,Util::createInt(5));
    index->set(2,Util::createInt(8));
    index->set(3,Util::createInt(10));
    data = Util::createArrayVector(index,value);
    vector<ConstantSP> datas = {data};
    vector<string> dataname = { "datas1" };
    conn.upload(dataname,datas);
    VectorSP expection = conn.run("a = take(bool((1..10)%2),10);res = arrayVector([2,5,8,10],a);res+1;");
    res= conn.run("datas1+1");
    EXPECT_EQ(res->getString(),expection->getString());
}


TEST_F(ArrayVectorTest,testUploadShortArrayVector){
    //
    VectorSP value;
    VectorSP index;
    VectorSP data;
    VectorSP res;
    index = Util::createVector(DT_INT,4,10);
    value = Util::createVector(DT_SHORT,10,20);
    for(int i=0;i<10;++i){
        value->set(i,Util::createShort((i+1)));
    }
    index->set(0,Util::createInt(2));
    index->set(1,Util::createInt(5));
    index->set(2,Util::createInt(8));
    index->set(3,Util::createInt(10));
    data = Util::createArrayVector(index,value);
    vector<ConstantSP> datas = {data};
    vector<string> dataname = { "datas1" };
    conn.upload(dataname,datas);
    VectorSP expection = conn.run("a = take(short(1..10),10);res = arrayVector([2,5,8,10],a);res+1;");
    res= conn.run("datas1+1");
    EXPECT_EQ(res->getString(),expection->getString());
}




TEST_F(ArrayVectorTest,testUploadLongArrayVector){
    //
    VectorSP value;
    VectorSP index;
    VectorSP data;
    VectorSP res;
    index = Util::createVector(DT_INT,4,10);
    value = Util::createVector(DT_LONG,10,20);
    for(int i=0;i<10;++i){
        value->set(i,Util::createLong(i+1));
    }
    index->set(0,Util::createInt(2));
    index->set(1,Util::createInt(5));
    index->set(2,Util::createInt(8));
    index->set(3,Util::createInt(10));
    data = Util::createArrayVector(index,value);
    vector<ConstantSP> datas = {data};
    vector<string> dataname = { "datas1" };
    conn.upload(dataname,datas);
    VectorSP expection = conn.run("a = take(long(1..10),10);res = arrayVector([2,5,8,10],a);res+1;");
    res= conn.run("re2=datas1+1;re2");
    EXPECT_EQ(res->getString(),expection->getString());
}




TEST_F(ArrayVectorTest,testUploadMinuteArrayVector){
    //
    VectorSP value;
    VectorSP index;
    VectorSP data;
    VectorSP res;
    index = Util::createVector(DT_INT,4,10);
    value = Util::createVector(DT_MINUTE,10,20);
    for(int i=0;i<10;++i){
        value->set(i,Util::createMinute(i+1));
    }
    index->set(0,Util::createInt(2));
    index->set(1,Util::createInt(5));
    index->set(2,Util::createInt(8));
    index->set(3,Util::createInt(10));
    data = Util::createArrayVector(index,value);
    vector<ConstantSP> datas = {data};
    vector<string> dataname = { "datas2" };
    conn.upload(dataname,datas);
    res= conn.run("re2=datas2+1;re2");
    VectorSP expection = conn.run("a=take(minute(1..10),10);res = arrayVector([2,5,8,10],a);res+1;");
    EXPECT_EQ(res->getString(),expection->getString());
}


//
//
//TEST_F(ArrayVectorTest,testUploadSecondArrayVector){
//    //
//    VectorSP value;
//    VectorSP index;
//    VectorSP data;
//    VectorSP res;
//    index = Util::createVector(DT_INT,4,10);
//    value = Util::createVector(DT_SECOND,10,20);
//    for(int i=0;i<10;++i){
//        value->set(i,Util::createSecond(i+1));
//    }
//    index->set(0,Util::createInt(2));
//    index->set(1,Util::createInt(5));
//    index->set(2,Util::createInt(8));
//    index->set(3,Util::createInt(10));
//    data = Util::createArrayVector(index,value);
//    vector<ConstantSP> datas = {data};
//    vector<string> dataname = { "datas1" };
//    conn.upload(dataname,datas);
//    VectorSP expection = conn.run("a=take(second(1..10),10);res = arrayVector([2,5,8,10],a);re1=res+1;re1;");
//    res= conn.run("re2=datas1+1;re2;");
//    EXPECT_EQ(res->getString(),expection->getString());
//}




TEST_F(ArrayVectorTest,testUploadFloatArrayVector){
    //
    VectorSP value;
    VectorSP index;
    VectorSP data;
    VectorSP res;
    index = Util::createVector(DT_INT,4,10);
    value = Util::createVector(DT_FLOAT,10,20);
    for(int i=0;i<10;++i){
        value->set(i,Util::createFloat(i+1.1));
    }
    index->set(0,Util::createInt(2));
    index->set(1,Util::createInt(5));
    index->set(2,Util::createInt(8));
    index->set(3,Util::createInt(10));
    data = Util::createArrayVector(index,value);
    vector<ConstantSP> datas = {data};
    vector<string> dataname = { "datas1" };
    conn.upload(dataname,datas);
    VectorSP expection = conn.run("a=take(float(1..10)+0.1,10);res = arrayVector([2,5,8,10],a);re1=res+1;re1;");
    res= conn.run("re2=datas1+1;re2;");
    EXPECT_EQ(res->getString(),expection->getString());
}



TEST_F(ArrayVectorTest,testIntArrayVector_1){
    //NULL
    VectorSP v1 = conn.run("a=[int()];"
                           "b = arrayVector([1],a);b");
    ConstantSP v2 = v1->get(0);
    ConstantSP re1 = conn.run("a = [int()];a");
    EXPECT_EQ(v2->getString(),re1->getString());

    //not NULL
    v1 = conn.run("a=[1];"
                  "b = arrayVector([1],a);b");
    v2 = v1->get(0);
    EXPECT_EQ(v2->getInt(0),1);
}

TEST_F(ArrayVectorTest,testIntArrayVectorSmaller256){
    //not NULL
    VectorSP v1 = conn.run("a=take(1..128,128);"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=take(1..128,128);\n"
                           "index = rand(1..127,10);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(int(),128);\n"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("0 join take(5 3 2 6, 32).cumsum().int()");
    int size1 = v1->size();
    EXPECT_EQ(size1,32);
    EXPECT_EQ(v1->getValueSize(),128);
    int idx = 0;
    for(int i=0;i < size1;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_EQ(temp1->getInt(j),data1->getInt(idx+j));
            EXPECT_EQ(temp2->getInt(j),data2->getInt(idx+j));
            EXPECT_EQ(temp3->getInt(j),data3->getInt(idx+j));
        }

    }

}


TEST_F(ArrayVectorTest,testIntArrayVectorSmaller65535){
    //not NULL
    VectorSP v1 = conn.run("a=take(1..32768,32768);"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=take(1..32768,32768);\n"
                           "index = rand(1..32767,100);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(int(),32768);\n"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("0 join take(256 200 312 256, 128).cumsum().int()");
    int size1 = v1->size();
    EXPECT_EQ(size1,128);
    EXPECT_EQ(v1->getValueSize(),32768);
    int idx = 0;
    for(int i=0;i <size1 ;++i){
        ConstantSP temp1 = v1->get(i);
        //cout << v1->getString();
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_EQ(temp1->getInt(j),data1->getInt(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }

    }

}


TEST_F(ArrayVectorTest,testIntArrayVectorBigger65535){
    //not NULL
    VectorSP v1 = conn.run("a=take(1..100,524288);"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=take(1..100,524288);\n"
                           "index = rand(1..524287,10000);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(int(),524288);\n"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("0 join take(65536 66072 60000 70536, 8).cumsum().int()");
    int size1 = v1->size();
    EXPECT_EQ(size1,8);
    EXPECT_EQ(v1->getValueSize(),524288);
    int idx = 0;
    for(int i=0;i <size1;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_EQ(temp1->getInt(j),data1->getInt(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }

    }

}


TEST_F(ArrayVectorTest,testDoubleArrayVector_1){
    //NULL
    VectorSP v1 = conn.run("a=[double()];"
                           "b = arrayVector([1],a);b");
    ConstantSP v2 = v1->get(0);
    ConstantSP re1 = conn.run("[double()]");
    EXPECT_EQ(v2->getString(),re1->getString());

    //not NULL
    v1 = conn.run("a=[1.5];"
                  "b = arrayVector([1],a);b");
    v2 = v1->get(0);
    EXPECT_EQ(v2->getDouble(0),1.5);
}


TEST_F(ArrayVectorTest,testDoubleArrayVectorSmaller256){
    //not NULL
    VectorSP v1 = conn.run("a=rand(128.0,128);"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=rand(128.0,128);\n"
                           "index = rand(1..127,10);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(double(),128);\n"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("0 join take(5 3 2 6, 32).cumsum().int()");
    int size1 = v1->size();
    EXPECT_EQ(size1,32);
    EXPECT_EQ(v1->getValueSize(),128);
    int idx = 0;
    for(int i=0;i < size1;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_DOUBLE_EQ(temp1->getDouble(j),data1->getDouble(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }

    }
}


TEST_F(ArrayVectorTest,testDoubleArrayVectorSmaller65535){
    //not NULL
    VectorSP v1 = conn.run("a=rand(32768.0,32768);"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=rand(32768.0,32768);\n"
                           "index = rand(1..32767,100);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(int(),32768);\n"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("0 join take(256 200 312 256, 128).cumsum().int()");
    int size1 = v1->size();
    EXPECT_EQ(size1,128);
    EXPECT_EQ(v1->getValueSize(),index->getInt(index->size() -1));
    int idx = 0;
    for(int i=0;i <size1 ;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_DOUBLE_EQ(temp1->getDouble(j),data1->getDouble(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }
    }
}


TEST_F(ArrayVectorTest,testDoubleArrayVectorBigger65535){
    //not NULL
    VectorSP v1 = conn.run("a=rand(524288.0,524288);"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=rand(524288.0,524288);\n"
                           "index = rand(1..524287,10000);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(double(),524288);\n"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("0 join take(65536 66072 60000 70536, 8).cumsum().int()");
    int size1 = v1->size();
    EXPECT_EQ(size1,8);
    EXPECT_EQ(v1->getValueSize(),index->getInt(index->size() -1));
    int idx = 0;
    for(int i=0;i < size1;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_DOUBLE_EQ(temp1->getDouble(j),data1->getDouble(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }

    }

}


TEST_F(ArrayVectorTest,testDatehourArrayVector_1){
    //NULL
    VectorSP v1 = conn.run("a=[datehour()];"
                           "b = arrayVector([1],a);b");
    ConstantSP v2 = v1->get(0);
    ConstantSP re1 = conn.run("[datehour()]");
    EXPECT_EQ(v2->getString(),re1->getString());

    //not NULL
    v1 = conn.run("a=[datehour(1)];"
                  "b = arrayVector([1],a);b");
    v2 = v1->get(0);
    EXPECT_EQ(v2->getString(0),"1970.01.01T01");
}


TEST_F(ArrayVectorTest,testDatehourArrayVectorSmaller256){
    //not NULL
    VectorSP v1 = conn.run("a=datehour(1..128);"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=datehour(1..128);\n"
                           "index = rand(1..127,10);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(datehour(),128);\n"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("0 join take(5 3 2 6, 32).cumsum().int()");
    int size1 = v1->size();
    EXPECT_EQ(size1,32);
    EXPECT_EQ(v1->getValueSize(),index->getInt(index->size() -1));
    int idx = 0;
    for(int i=0;i < size1;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_EQ(temp1->getString(j),data1->getString(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }

    }
}


TEST_F(ArrayVectorTest,testDatehourArrayVectorSmaller65535){
    //not NULL
    VectorSP v1 = conn.run("a=datehour(1..32768);"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=datehour(1..32768);\n"
                           "index = rand(1..32767,100);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(datehour(),32768);\n"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("0 join take(256 200 312 256, 128).cumsum().int()");
    int size1 = v1->size();
    EXPECT_EQ(size1,128);
    EXPECT_EQ(v1->getValueSize(),index->getInt(index->size() -1));
    int idx = 0;
    for(int i=0;i <size1 ;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_EQ(temp1->getString(j),data1->getString(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }
    }
}


TEST_F(ArrayVectorTest,testDatehourArrayVectorBigger65535){
    //not NULL
    VectorSP v1 = conn.run("a=datehour(1..524288);"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=datehour(1..524288);\n"
                           "index = rand(1..524287,10000);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(datehour(),524288);\n"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("0 join take(65536 66072 60000 70536, 8).cumsum().int()");
    int size1 = v1->size();
    EXPECT_EQ(size1,8);
    EXPECT_EQ(v1->getValueSize(),index->getInt(index->size() -1));
    int idx = 0;
    for(int i=0;i < size1;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_DOUBLE_EQ(temp1->getDouble(j),data1->getDouble(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }

    }

}



TEST_F(ArrayVectorTest,testDateArrayVector_1){
    //NULL
    VectorSP v1 = conn.run("a=[date()];"
                           "b = arrayVector([1],a);b");
    ConstantSP v2 = v1->get(0);
    ConstantSP re1 = conn.run("[date()]");
    EXPECT_EQ(v2->getString(),re1->getString());

    //not NULL
    v1 = conn.run("a=[date(1)];"
                  "b = arrayVector([1],a);b");
    v2 = v1->get(0);
    EXPECT_EQ(v2->getString(0),"1970.01.02");
}


TEST_F(ArrayVectorTest,testDateArrayVectorSmaller256){
    //not NULL
    VectorSP v1 = conn.run("a=date(1..128);"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=date(1..128);\n"
                           "index = rand(1..127,10);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(date(),128);\n"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("0 join take(5 3 2 6, 32).cumsum().int()");
    int size1 = v1->size();
    EXPECT_EQ(size1,32);
    EXPECT_EQ(v1->getValueSize(),index->getInt(index->size() -1));
    int idx = 0;
    for(int i=0;i < size1;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_EQ(temp1->getString(j),data1->getString(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }

    }
}


TEST_F(ArrayVectorTest,testDateArrayVectorSmaller65535){
    //not NULL
    VectorSP v1 = conn.run("a=date(1..32768);"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=date(1..32768);\n"
                           "index = rand(1..32767,100);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(date(),32768);\n"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("0 join take(256 200 312 256, 128).cumsum().int()");
    int size1 = v1->size();
    EXPECT_EQ(size1,128);
    EXPECT_EQ(v1->getValueSize(),index->getInt(index->size() -1));
    int idx = 0;
    for(int i=0;i <size1 ;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_EQ(temp1->getString(j),data1->getString(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }
    }
}


TEST_F(ArrayVectorTest,testDateArrayVectorBigger65535){
    //not NULL
    VectorSP v1 = conn.run("a=date(1..524288);"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=date(1..524288);\n"
                           "index = rand(1..524287,10000);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(date(),524288);\n"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("0 join take(65536 66072 60000 70536, 8).cumsum().int()");
    int size1 = v1->size();
    EXPECT_EQ(size1,8);
    EXPECT_EQ(v1->getValueSize(),index->getInt(index->size() -1));
    int idx = 0;
    for(int i=0;i < size1;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_DOUBLE_EQ(temp1->getDouble(j),data1->getDouble(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }

    }

}

TEST_F(ArrayVectorTest,testDatetimeArrayVector_1){
    //NULL
    VectorSP v1 = conn.run("a=[datetime()];"
                           "b = arrayVector([1],a);b");
    ConstantSP v2 = v1->get(0);
    ConstantSP re1 = conn.run("[datetime()]");
    EXPECT_EQ(v2->getString(),re1->getString());

    //not NULL
    v1 = conn.run("a=[datetime(1)];"
                  "b = arrayVector([1],a);b");
    v2 = v1->get(0);
    EXPECT_EQ(v2->getString(0),"1970.01.01T00:00:01");
}


TEST_F(ArrayVectorTest,testDatetimeArrayVectorSmaller256){
    //not NULL
    VectorSP v1 = conn.run("a=datetime(1..128);"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=datetime(1..128);\n"
                           "index = rand(1..127,10);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(datetime(),128);\n"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("0 join take(5 3 2 6, 32).cumsum().int()");
    int size1 = v1->size();
    EXPECT_EQ(size1,32);
    EXPECT_EQ(v1->getValueSize(),index->getInt(index->size() -1));
    int idx = 0;
    for(int i=0;i < size1;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_EQ(temp1->getString(j),data1->getString(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }

    }
}


TEST_F(ArrayVectorTest,testDatetimeArrayVectorSmaller65535){
    //not NULL
    VectorSP v1 = conn.run("a=datetime(1..32768);"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=datetime(1..32768);\n"
                           "index = rand(1..32767,100);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(datetime(),32768);\n"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("0 join take(256 200 312 256, 128).cumsum().int()");
    int size1 = v1->size();
    EXPECT_EQ(size1,128);
    EXPECT_EQ(v1->getValueSize(),index->getInt(index->size() -1));
    int idx = 0;
    for(int i=0;i <size1 ;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_EQ(temp1->getString(j),data1->getString(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }
    }
}


TEST_F(ArrayVectorTest,testDatetimeArrayVectorBigger65535){
    //not NULL
    VectorSP v1 = conn.run("a=datetime(1..524288);"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=datetime(1..524288);\n"
                           "index = rand(1..524287,10000);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(datetime(),524288);\n"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a;");
    ConstantSP index = conn.run("index=0 join take(65536 66072 60000 70536, 8).cumsum().int();index");
    int size1 = v1->size();
    EXPECT_EQ(size1,8);
    EXPECT_EQ(v1->getValueSize(),index->getInt(index->size() -1));
    int idx = 0;
    for(int i=0;i < size1;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_EQ(temp1->getString(j),data1->getString(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }

    }

}


TEST_F(ArrayVectorTest,testTimeStampArrayVector_1){
    //NULL
    VectorSP v1 = conn.run("a=[timestamp()];"
                           "b = arrayVector([1],a);b");
    ConstantSP v2 = v1->get(0);
    ConstantSP re1 = conn.run("[timestamp()]");
    EXPECT_EQ(v2->getString(),re1->getString());

    //not NULL
    v1 = conn.run("a=[timestamp(1)];"
                  "b = arrayVector([1],a);b");
    v2 = v1->get(0);
    EXPECT_EQ(v2->getString(0),"1970.01.01T00:00:00.001");
}


TEST_F(ArrayVectorTest,testTimeStampArrayVectorSmaller256){
    //not NULL
    VectorSP v1 = conn.run("a=timestamp(1..128);"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=timestamp(1..128);\n"
                           "index = rand(1..127,10);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(timestamp(),128);\n"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("0 join take(5 3 2 6, 32).cumsum().int()");
    int size1 = v1->size();
    EXPECT_EQ(size1,32);
    EXPECT_EQ(v1->getValueSize(),index->getInt(index->size() -1));
    int idx = 0;
    for(int i=0;i < size1;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_EQ(temp1->getString(j),data1->getString(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }

    }
}


TEST_F(ArrayVectorTest,testTimeStampArrayVectorSmaller65535){
    //not NULL
    VectorSP v1 = conn.run("a=timestamp(1..32768);"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=timestamp(1..32768);\n"
                           "index = rand(1..32767,100);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(timestamp(),32768);\n"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("0 join take(256 200 312 256, 128).cumsum().int()");
    int size1 = v1->size();
    EXPECT_EQ(size1,128);
    EXPECT_EQ(v1->getValueSize(),index->getInt(index->size() -1));
    int idx = 0;
    for(int i=0;i <size1 ;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_EQ(temp1->getString(j),data1->getString(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }
    }
}


TEST_F(ArrayVectorTest,testTimeStampArrayVectorBigger65535){
    //not NULL
    VectorSP v1 = conn.run("a=timestamp(1..524288);"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=timestamp(1..524288);\n"
                           "index = rand(1..524287,10000);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(timestamp(),524288);\n"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("index=0 join take(65536 66072 60000 70536, 8).cumsum().int();index");
    int size1 = v1->size();
    EXPECT_EQ(size1,8);
    EXPECT_EQ(v1->getValueSize(),index->getInt(index->size() -1));
    int idx = 0;
    for(int i=0;i < size1;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_DOUBLE_EQ(temp1->getDouble(j),data1->getDouble(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }

    }

}

TEST_F(ArrayVectorTest,testNanotimestampArrayVector_1){
    //NULL
    VectorSP v1 = conn.run("a=[nanotimestamp()];"
                           "b = arrayVector([1],a);b");
    ConstantSP v2 = v1->get(0);
    ConstantSP re1 = conn.run("[nanotimestamp()]");
    EXPECT_EQ(v2->getString(),re1->getString());

    //not NULL
    v1 = conn.run("a=[nanotimestamp(1)];"
                  "b = arrayVector([1],a);b");
    v2 = v1->get(0);
    EXPECT_EQ(v2->getString(0),"1970.01.01T00:00:00.000000001");
}


TEST_F(ArrayVectorTest,testNanotimestampArrayVectorSmaller256){
    //not NULL
    VectorSP v1 = conn.run("a=nanotimestamp(1..128);"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=nanotimestamp(1..128);\n"
                           "index = rand(1..127,10);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(nanotimestamp(),128);\n"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("0 join take(5 3 2 6, 32).cumsum().int()");
    int size1 = v1->size();
    EXPECT_EQ(size1,32);
    EXPECT_EQ(v1->getValueSize(),index->getInt(index->size() -1));
    int idx = 0;
    for(int i=0;i < size1;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_EQ(temp1->getString(j),data1->getString(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }

    }
}


TEST_F(ArrayVectorTest,testNanotimestampArrayVectorSmaller65535){
    //not NULL
    VectorSP v1 = conn.run("a=nanotimestamp(1..32768);"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=nanotimestamp(1..32768);\n"
                           "index = rand(1..16384,100);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(nanotimestamp(),32768);\n"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("0 join take(256 200 312 256, 128).cumsum().int()");
    int size1 = v1->size();
    EXPECT_EQ(size1,128);
    EXPECT_EQ(v1->getValueSize(),index->getInt(index->size() -1));
    int idx = 0;
    for(int i=0;i <size1 ;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_EQ(temp1->getString(j),data1->getString(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }
    }
}


TEST_F(ArrayVectorTest,testNanotimestampArrayVectorBigger65535){
    //not NULL
    VectorSP v1 = conn.run("a=nanotimestamp(1..524288);"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=nanotimestamp(1..524288);\n"
                           "index = rand(1..524287,10000);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(nanotimestamp(),524288);\n"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("index=0 join take(65536 66072 60000 70536, 8).cumsum().int();index");
    int size1 = v1->size();
    EXPECT_EQ(size1,8);
    EXPECT_EQ(v1->getValueSize(),index->getInt(index->size() -1));
    int idx = 0;
    for(int i=0;i < size1;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_DOUBLE_EQ(temp1->getDouble(j),data1->getDouble(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }

    }

}


TEST_F(ArrayVectorTest,testMonthArrayVector_1){
    //NULL
    VectorSP v1 = conn.run("a=[month()];"
                           "b = arrayVector([1],a);b");
    ConstantSP v2 = v1->get(0);
    ConstantSP re1 = conn.run("[month()]");
    EXPECT_EQ(v2->getString(),re1->getString());

    //not NULL
    v1 = conn.run("a=[month(1970.01.11)];"
                  "b = arrayVector([1],a);b");
    v2 = v1->get(0);
    //cout << v2->getString();
    EXPECT_EQ(v2->getString(0),"1970.01M");
}


TEST_F(ArrayVectorTest,testMonthArrayVectorSmaller256){
    //not NULL
    VectorSP v1 = conn.run("a=month(1970.01.12+1..128);"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=month(1970.01.12+1..128);\n"
                           "index = rand(1..127,10);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(month(),128);\n"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("0 join take(5 3 2 6, 32).cumsum().int()");
    int size1 = v1->size();
    EXPECT_EQ(size1,32);
    EXPECT_EQ(v1->getValueSize(),index->getInt(index->size() -1));
    int idx = 0;
    for(int i=0;i < size1;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_EQ(temp1->getString(j),data1->getString(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }

    }
}


TEST_F(ArrayVectorTest,testMonthArrayVectorSmaller65535){
    //not NULL
    VectorSP v1 = conn.run("a=month(1970.01.12+1..32768);"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=month(1970.01.12+1..32768);\n"
                           "index = rand(1..16384,100);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(month(),32768);\n"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("0 join take(256 200 312 256, 128).cumsum().int()");
    int size1 = v1->size();
    EXPECT_EQ(size1,128);
    EXPECT_EQ(v1->getValueSize(),index->getInt(index->size() -1));
    int idx = 0;
    for(int i=0;i <size1 ;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_EQ(temp1->getString(j),data1->getString(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }
    }
}


TEST_F(ArrayVectorTest,testMonthArrayVectorBigger65535){
    //not NULL
    VectorSP v1 = conn.run("a=month(1921.01.01+1..524288);"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=month(1921.01.01+1..524288);\n"
                           "index = rand(1..524287,10000);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(month(),524288);\n"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("index=0 join take(65536 66072 60000 70536, 8).cumsum().int();index");
    int size1 = v1->size();
    EXPECT_EQ(size1,8);
    EXPECT_EQ(v1->getValueSize(),index->getInt(index->size() -1));
    int idx = 0;
    for(int i=0;i < size1;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_EQ(temp1->getString(j),data1->getString(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }

    }

}


TEST_F(ArrayVectorTest,testTimeArrayVector_1){
    //NULL
    VectorSP v1 = conn.run("a=[time()];"
                           "b = arrayVector([1],a);b");
    ConstantSP v2 = v1->get(0);
    ConstantSP re1 = conn.run("[time()]");
    EXPECT_EQ(v2->getString(),re1->getString());

    //not NULL
    v1 = conn.run("a=[time(13:30:10.008)];"
                  "b = arrayVector([1],a);b");
    v2 = v1->get(0);
    //cout << v2->getString();
    EXPECT_EQ(v2->getString(0),"13:30:10.008");
}


TEST_F(ArrayVectorTest,testTimeArrayVectorSmaller256){
    //not NULL
    VectorSP v1 = conn.run("a=time(13:30:10.008+1..128);"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=time(13:30:10.008+1..128);\n"
                           "index = rand(1..127,10);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(time(),128);\n"
                           "b = arrayVector(take(5 3 2 6, 32).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("0 join take(5 3 2 6, 32).cumsum().int()");
    int size1 = v1->size();
    EXPECT_EQ(size1,32);
    EXPECT_EQ(v1->getValueSize(),index->getInt(index->size() -1));
    int idx = 0;
    for(int i=0;i < size1;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_EQ(temp1->getString(j),data1->getString(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }

    }
}


TEST_F(ArrayVectorTest,testTimeArrayVectorSmaller65535){
    //not NULL
    VectorSP v1 = conn.run("a=time(13:30:10.008+1..32768);"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=time(13:30:10.008+1..32768);\n"
                           "index = rand(1..16383,100);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(time(),32768);\n"
                           "b = arrayVector(take(256 200 312 256, 128).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("0 join take(256 200 312 256, 128).cumsum().int()");
    int size1 = v1->size();
    EXPECT_EQ(size1,128);
    EXPECT_EQ(v1->getValueSize(),index->getInt(index->size() -1));
    int idx = 0;
    for(int i=0;i <size1 ;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_EQ(temp1->getString(j),data1->getString(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }
    }
}


TEST_F(ArrayVectorTest,testTimeArrayVectorBigger65535){
    //not NULL
    VectorSP v1 = conn.run("a=time(13:30:10.008+1..524288);"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data1 = conn.run("a");

    VectorSP v2 = conn.run("a=time(13:30:10.008+1..524288);\n"
                           "index = rand(1..262143,10000);\n"
                           "for(idx in index){\n"
                           "\ta[idx] = NULL\t\n"
                           "};"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data2 = conn.run("a");
    VectorSP v3 = conn.run("a=take(time(),524288);\n"
                           "b = arrayVector(take(65536 66072 60000 70536, 8).cumsum().int(),a);b");
    ConstantSP data3 = conn.run("a");
    ConstantSP index = conn.run("index=0 join take(65536 66072 60000 70536, 8).cumsum().int();index");
    int size1 = v1->size();
    EXPECT_EQ(size1,8);
    EXPECT_EQ(v1->getValueSize(),index->getInt(index->size() -1));
    int idx = 0;
    for(int i=0;i < size1;++i){
        ConstantSP temp1 = v1->get(i);
        ConstantSP temp2 = v2->get(i);
        ConstantSP temp3 = v3->get(i);
        int size2 = temp1->size();
        for(int j=0;j<size2;++j){
            idx = index->getInt(i);
            EXPECT_EQ(temp1->getString(j),data1->getString(idx+j));
            EXPECT_EQ(temp2->getString(j),data2->getString(idx+j));
            EXPECT_EQ(temp3->getString(j),data3->getString(idx+j));
        }

    }

}


TEST_F(ArrayVectorTest,testDiffTypeArrayVector){
    VectorSP v1;
    ConstantSP re;
    v1 = conn.run("a=[int()];"
                  "b = arrayVector([1],a);b");

    re = conn.run("[1,2,3]");
    EXPECT_EQ(v1->get(0)->getType(),re->getType());
    EXPECT_EQ(v1->get(0)->getRawType(),re->getRawType());

    v1 = conn.run("a=[double(1.3)];"
                  "b = arrayVector([1],a);b");

    re = conn.run("[1.3,3.2]");
    EXPECT_EQ(v1->get(0)->getType(),re->getType());
    EXPECT_EQ(v1->get(0)->getRawType(),re->getRawType());

    v1 = conn.run("a=[datehour(1)];"
                  "b = arrayVector([1],a);b");

    re = conn.run("[datehour(1)]");
    EXPECT_EQ(v1->get(0)->getType(),re->getType());
    EXPECT_EQ(v1->get(0)->getRawType(),re->getRawType());

    v1 = conn.run("a=[date(1)];"
                  "b = arrayVector([1],a);b");

    re = conn.run("[date(1)]");
    EXPECT_EQ(v1->get(0)->getType(),re->getType());
    EXPECT_EQ(v1->get(0)->getRawType(),re->getRawType());

    v1 = conn.run("a=[datetime(1)];"
                  "b = arrayVector([1],a);b");

    re = conn.run("[datetime(1)]");
    EXPECT_EQ(v1->get(0)->getType(),re->getType());
    EXPECT_EQ(v1->get(0)->getRawType(),re->getRawType());

    v1 = conn.run("a=[timestamp(1)];"
                  "b = arrayVector([1],a);b");

    re = conn.run("[timestamp(1)]");
    EXPECT_EQ(v1->get(0)->getType(),re->getType());
    EXPECT_EQ(v1->get(0)->getRawType(),re->getRawType());

    v1 = conn.run("a=[nanotime(1)];"
                  "b = arrayVector([1],a);b");

    re = conn.run("[nanotime(1)]");
    EXPECT_EQ(v1->get(0)->getType(),re->getType());
    EXPECT_EQ(v1->get(0)->getRawType(),re->getRawType());

    v1 = conn.run("a=[month(1)];"
                  "b = arrayVector([1],a);b");

    re = conn.run("[month(1)]");
    EXPECT_EQ(v1->get(0)->getType(),re->getType());
    EXPECT_EQ(v1->get(0)->getRawType(),re->getRawType());

    v1 = conn.run("a=[time(1)];"
                  "b = arrayVector([1],a);b");

    re = conn.run("[time(1)]");
    EXPECT_EQ(v1->get(0)->getType(),re->getType());
    EXPECT_EQ(v1->get(0)->getRawType(),re->getRawType());

}



TEST_F(ArrayVectorTest,testGetIndexAndDataArrayVector){
    VectorSP v1;
    ConstantSP data_re;
    ConstantSP index_re;
    INDEX * index_array;
    //int
    v1 = conn.run("a=int(1..262144);"
                  "b = arrayVector(take(5 3 2 6, 65536).cumsum().int(),a);b");

    data_re = conn.run("a");
    index_re = conn.run("take(5 3 2 6, 65536).cumsum().int()");
    int* data_int_array = (int*) v1->getDataArray();
    index_array = v1->getIndexArray();
    for(int i=0;i<data_re->size();++i){
        EXPECT_EQ(*(data_int_array+i),data_re->getInt(i));
    }
    for(int i=0;i<index_re->size();++i){
        EXPECT_EQ(*(index_array+i),index_re->getInt(i));
    }


    //double
    v1 = conn.run("a=double(1..262144);"
                  "b = arrayVector(take(5 3 2 6, 65536).cumsum().int(),a);b");

    data_re = conn.run("a");
    index_re = conn.run("take(5 3 2 6, 65536).cumsum().int()");
    double* data_double_array = (double*) v1->getDataArray();
    index_array = v1->getIndexArray();
    for(int i=0;i<data_re->size();++i){
        EXPECT_EQ(*(data_double_array+i),data_re->getDouble(i));
    }
    for(int i=0;i<index_re->size();++i){
        EXPECT_EQ(*(index_array+i),index_re->getInt(i));
    }

    //datehour
    v1 = conn.run("a=datehour(1..262144);"
                  "b = arrayVector(take(5 3 2 6, 65536).cumsum().int(),a);b");

    data_re = conn.run("a");
    index_re = conn.run("take(5 3 2 6, 65536).cumsum().int()");
    int* data_datehour_array = (int*) v1->getDataArray();
    index_array = v1->getIndexArray();
    for(int i=0;i<data_re->size();++i){
        EXPECT_EQ(*(data_datehour_array+i),data_re->getInt(i));
    }
    for(int i=0;i<index_re->size();++i){
        EXPECT_EQ(*(index_array+i),index_re->getInt(i));
    }


    //data
    v1 = conn.run("a=date(1..262144);"
                  "b = arrayVector(take(5 3 2 6, 65536).cumsum().int(),a);b");

    data_re = conn.run("a");
    index_re = conn.run("take(5 3 2 6, 65536).cumsum().int()");
    int* data_date_array = (int*) v1->getDataArray();
    index_array = v1->getIndexArray();
    for(int i=0;i<data_re->size();++i){
        EXPECT_EQ(*(data_date_array+i),data_re->getInt(i));
    }
    for(int i=0;i<index_re->size();++i){
        EXPECT_EQ(*(index_array+i),index_re->getInt(i));
    }

    //datetime
    v1 = conn.run("a=datetime(1..262144);"
                  "b = arrayVector(take(5 3 2 6, 65536).cumsum().int(),a);b");

    data_re = conn.run("a");
    index_re = conn.run("take(5 3 2 6, 65536).cumsum().int()");
    int* data_datetime_array = (int*) v1->getDataArray();
    index_array = v1->getIndexArray();
    for(int i=0;i<data_re->size();++i){
        EXPECT_EQ(*(data_datetime_array+i),data_re->getInt(i));
    }
    for(int i=0;i<index_re->size();++i){
        EXPECT_EQ(*(index_array+i),index_re->getInt(i));
    }

    //TimeStamp
    v1 = conn.run("a=timestamp(2009.10.12T00:00:00.000+1..262144);"
                  "b = arrayVector(take(5 3 2 6, 65536).cumsum().int(),a);b");

    data_re = conn.run("a");
    index_re = conn.run("take(5 3 2 6, 65536).cumsum().int()");
    long long* data_timestamp_array = (long long*) v1->getDataArray();
    index_array = v1->getIndexArray();
    for(int i=0;i<data_re->size();++i){
        EXPECT_EQ(*(data_timestamp_array+i),data_re->getLong(i));
    }
    for(int i=0;i<index_re->size();++i){
        EXPECT_EQ(*(index_array+i),index_re->getInt(i));
    }

    //nanotime
    v1 = conn.run("a=nanotime(13:30:10.008007007+1..262144);"
                  "b = arrayVector(take(5 3 2 6, 65536).cumsum().int(),a);b");

    data_re = conn.run("a");
    index_re = conn.run("take(5 3 2 6, 65536).cumsum().int()");
    long long* data_nanotime_array = (long long*) v1->getDataArray();
    index_array = v1->getIndexArray();
    for(int i=0;i<data_re->size();++i){
        EXPECT_EQ(*(data_nanotime_array+i),data_re->getLong(i));
    }
    for(int i=0;i<index_re->size();++i){
        EXPECT_EQ(*(index_array+i),index_re->getInt(i));
    }

    //nanotimestamp
    v1 = conn.run("a=nanotimestamp(2012.06.13T13:30:10.008007006+1..262144);"
                  "b = arrayVector(take(5 3 2 6, 65536).cumsum().int(),a);b");

    data_re = conn.run("a");
    index_re = conn.run("take(5 3 2 6, 65536).cumsum().int()");
    long long* data_nanotimestamp_array = (long long*) v1->getDataArray();
    index_array = v1->getIndexArray();
    for(int i=0;i<data_re->size();++i){
        EXPECT_EQ(*(data_nanotimestamp_array+i),data_re->getLong(i));
    }
    for(int i=0;i<index_re->size();++i){
        EXPECT_EQ(*(index_array+i),index_re->getInt(i));
    }

    //month
    v1 = conn.run("a=month(2012.06.13T13:30:10.008007006+1..262144);"
                  "b = arrayVector(take(5 3 2 6, 65536).cumsum().int(),a);b");

    data_re = conn.run("a");
    index_re = conn.run("take(5 3 2 6, 65536).cumsum().int()");
    int* data_month_array = (int*) v1->getDataArray();
    index_array = v1->getIndexArray();
    for(int i=0;i<data_re->size();++i){
        EXPECT_EQ(*(data_month_array+i),data_re->getInt(i));
    }
    for(int i=0;i<index_re->size();++i){
        EXPECT_EQ(*(index_array+i),index_re->getInt(i));
    }

    //time
    v1 = conn.run("a=time(13:30:10.008007006+1..262144);"
                  "b = arrayVector(take(5 3 2 6, 65536).cumsum().int(),a);b");

    data_re = conn.run("a");
    index_re = conn.run("take(5 3 2 6, 65536).cumsum().int()");
    int* data_time_array = (int*) v1->getDataArray();
    index_array = v1->getIndexArray();
    for(int i=0;i<data_re->size();++i){
        EXPECT_EQ(*(data_time_array+i),data_re->getInt(i));
    }
    for(int i=0;i<index_re->size();++i){
        EXPECT_EQ(*(index_array+i),index_re->getInt(i));
    }

}

TEST_F(ArrayVectorTest,testAppendIntArrayVector){

    VectorSP v1 = conn.run("a = array(INT[], 0, 1000).append!(arrayVector((1..1000)*50, rand(-100..100 join NULL, 1000*50)));a");
    //constant
    ConstantSP data1 = Util::createInt(100);
    v1->append(data1);
    EXPECT_EQ(v1->get(v1->size()-1)->getString(),"[100]");

    //tuple
    ConstantSP data2 =conn.run("t=(1,2,3,4,5);t");
    v1->append(data2);
    EXPECT_EQ(v1->get(v1->size()-1)->getString(),"[5]");

    //array vector
    VectorSP data3 = conn.run("arrayVector(take(5 3,2).cumsum().int(), take(1..8,8))");
    v1->append(data3);
    EXPECT_EQ(v1->get(v1->size()-1)->getString(),"[6,7,8]");
}



TEST_F(ArrayVectorTest,testAppendDoubleArrayVector){
    //
    VectorSP v1 = conn.run("a = array(DOUBLE[], 0, 1000).append!(arrayVector((1..1000)*50, rand(double(-100..100 join NULL), 1000*50)));a");
    //constant
    ConstantSP data1 = Util::createDouble(100.0);
    v1->append(data1);
    EXPECT_EQ(v1->get(v1->size()-1)->getString(),"[100]");

    //tuple
    ConstantSP data2 = conn.run("(34.6 32.4 53.3 12.2 5 43 1.0,)");
    v1->append(data2);
    EXPECT_EQ(v1->get(v1->size()-2)->getString(),"[34.6,32.4,53.3,12.2,5,43,1]");

    //array vector
    VectorSP data3 = conn.run("arrayVector(take(5 3,2).cumsum().int(), double(1..8))");
    v1->append(data3);
    EXPECT_EQ(v1->get(v1->size()-1)->getString(),"[6,7,8]");
}




TEST_F(ArrayVectorTest,testAppendDatehourArrayVector){
    //
    VectorSP v1 = conn.run("a = array(DATEHOUR[], 0, 1000).append!(arrayVector((1..1000)*50, rand(datehour(-100..100 join NULL), 1000*50)));a");
    //constant
    ConstantSP data1 = Util::createDateHour(100.4);
    v1->append(data1);
    EXPECT_EQ(v1->get(v1->size()-1)->getString(),"[1970.01.05T04]");

    //tuple
    ConstantSP data2 = conn.run("(datehour(34 32 53 12 5 43 11),)");
    v1->append(data2);
    EXPECT_EQ(v1->get(v1->size()-2)->getString(),"[1970.01.02T10,1970.01.02T08,1970.01.03T05,1970.01.01T12,1970.01.01T05,1970.01.02T19,1970.01.01T11]");

    //array vector
    VectorSP data3 = conn.run("arrayVector(take(5 3,2).cumsum().int(), datehour(1..8))");
    v1->append(data3);
    EXPECT_EQ(v1->get(v1->size()-1)->getString(),"[1970.01.01T06,1970.01.01T07,1970.01.01T08]");
}



TEST_F(ArrayVectorTest,testAppendDateArrayVector){
    //
    VectorSP v1 = conn.run("a = array(DATE[], 0, 1000).append!(arrayVector((1..1000)*50, rand(date(-100..100 join NULL), 1000*50)));a");
    //constant
    ConstantSP data1 = Util::createDate(100);
    v1->append(data1);
    EXPECT_EQ(v1->get(v1->size()-1)->getString(),"[1970.04.11]");

    //tuple
    ConstantSP data2 = conn.run("(date(34 32 53 12 5 43 11),)");
    v1->append(data2);
    EXPECT_EQ(v1->get(v1->size()-2)->getString(),"[1970.02.04,1970.02.02,1970.02.23,1970.01.13,1970.01.06,1970.02.13,1970.01.12]");

    //array vector
    VectorSP data3 = conn.run("arrayVector(take(5 3,2).cumsum().int(), double(1..8))");
    v1->append(data3);
    EXPECT_EQ(v1->get(v1->size()-1)->getString(),"[1970.01.07,1970.01.08,1970.01.09]");
}


TEST_F(ArrayVectorTest,testAppendDatetimeArrayVector){
    //
    VectorSP v1 = conn.run("a = array(DATETIME[], 0, 1000).append!(arrayVector((1..1000)*50, rand(datetime(-100..100 join NULL), 1000*50)));a");
    //constant
    ConstantSP data1 = Util::createDateTime(100);
    v1->append(data1);
    EXPECT_EQ(v1->get(v1->size()-1)->getString(),"["+data1->getString()+"]");

    //tuple
    ConstantSP data2 = conn.run("(datetime(34 32 53 12 5 43 11),)");
    v1->append(data2);
    EXPECT_EQ(v1->get(v1->size()-2)->getString(),data2->getString(0));

    //array vector
    VectorSP data3 = conn.run("arrayVector(take(5 3,2).cumsum().int(), datetime(1..8))");
    v1->append(data3);
    EXPECT_EQ(v1->get(v1->size()-1)->getString(),data3->get(1)->getString());
}

TEST_F(ArrayVectorTest,testAppendTimestampArrayVector){
    //
    VectorSP v1 = conn.run("a = array(TIMESTAMP[], 0, 1000).append!(arrayVector((1..1000)*50, rand(timestamp(-100..100 join NULL), 1000*50)));a");
    //constant
    ConstantSP data1 = Util::createTimestamp(10000000);
    v1->append(data1);
    EXPECT_EQ(v1->get(v1->size()-1)->getString(),"["+data1->getString()+"]");

    //tuple
    ConstantSP data2 = conn.run("(timestamp(34 32 53 12 5 43 11),)");
    v1->append(data2);
    EXPECT_EQ(v1->get(v1->size()-2)->getString(),data2->getString(0));

    //array vector
    VectorSP data3 = conn.run("arrayVector(take(5 3,2).cumsum().int(), timestamp(1..8))");
    v1->append(data3);
    EXPECT_EQ(v1->get(v1->size()-1)->getString(),data3->get(1)->getString());
}


TEST_F(ArrayVectorTest,testAppendNanotimeArrayVector){
    //
    VectorSP v1 = conn.run("a = array(NANOTIME[], 0, 1000).append!(arrayVector((1..1000)*50, rand(nanotime(-100..100 join NULL), 1000*50)));a");
    //constant
    ConstantSP data1 = Util::createNanoTime(10000000);
    v1->append(data1);
    EXPECT_EQ(v1->get(v1->size()-1)->getString(),"["+data1->getString()+"]");

    //tuple
    ConstantSP data2 = conn.run("(nanotime(34 32 53 12 5 43 11),)");
    v1->append(data2);
    EXPECT_EQ(v1->get(v1->size()-2)->getString(),data2->getString(0));

    //array vector
    VectorSP data3 = conn.run("arrayVector(take(5 3,2).cumsum().int(), nanotime(1..8))");
    v1->append(data3);
    EXPECT_EQ(v1->get(v1->size()-1)->getString(),data3->get(1)->getString());
}


TEST_F(ArrayVectorTest,testAppendMonthArrayVector){
    //
    VectorSP v1 = conn.run("a = array(MONTH[], 0, 1000).append!(arrayVector((1..1000)*50, rand(month(-100..100 join NULL), 1000*50)));a");
    //constant
    ConstantSP data1 = Util::createMonth(10000);
    v1->append(data1);
    EXPECT_EQ(v1->get(v1->size()-1)->getString(),"["+data1->getString()+"]");

    //tuple
    ConstantSP data2 = conn.run("(month(34 32 53 12 5 43 11),)");
    v1->append(data2);
    EXPECT_EQ(v1->get(v1->size()-2)->getString(),data2->getString(0));

    //array vector
    VectorSP data3 = conn.run("arrayVector(take(5 3,2).cumsum().int(), month(1..8))");
    v1->append(data3);
    EXPECT_EQ(v1->get(v1->size()-1)->getString(),data3->get(1)->getString());
}

TEST_F(ArrayVectorTest,testAppendTimeArrayVector){
    //
    VectorSP v1 = conn.run("a = array(TIME[], 0, 1000).append!(arrayVector((1..1000)*50, rand(time(-100..100 join NULL), 1000*50)));a");
    //constant
    ConstantSP data1 = Util::createTime(10000);
    v1->append(data1);
    EXPECT_EQ(v1->get(v1->size()-1)->getString(),"["+data1->getString()+"]");

    //tuple
    ConstantSP data2 = conn.run("(time(34 32 53 12 5 43 11),)");
    v1->append(data2);
    EXPECT_EQ(v1->get(v1->size()-2)->getString(),data2->getString(0));

    //array vector
    VectorSP data3 = conn.run("arrayVector(take(5 3,2).cumsum().int(), time(1..8))");
    v1->append(data3);
    EXPECT_EQ(v1->get(v1->size()-1)->getString(),data3->get(1)->getString());
}



TEST_F(ArrayVectorTest,testgetSubVectorArrayVector){
    //
    VectorSP v1;
    ConstantSP data_re;
    //int
    v1 = conn.run("a=int(1..262144);"
                  "b = arrayVector(take(5 3 2 6, 65536).cumsum().int(),a);b;");

    data_re = v1->getSubVector(35,102);
    for(int i=0;i<67;++i){
        EXPECT_EQ(data_re->get(i)->getString(),v1->get(i+35)->getString());
    }

}

TEST_F(ArrayVectorTest,testErrorIndexArrayVector){
    //
    VectorSP value;
    VectorSP index;
    VectorSP data;
    VectorSP res;
    index = Util::createVector(DT_INT,4,10);
    value = Util::createVector(DT_DATETIME,10,20);
    for(int i=0;i<10;++i){
        value->set(i,Util::createDateTime(i+1));
    }
    //bigger
    index->set(0,Util::createInt(2));
    index->set(1,Util::createInt(5));
    index->set(2,Util::createInt(8));
    index->set(3,Util::createInt(11));
    EXPECT_ANY_THROW(Util::createArrayVector(index,value));

    //small
    index->set(3,Util::createInt(8));
    EXPECT_ANY_THROW(Util::createArrayVector(index,value));
}

TEST_F(ArrayVectorTest,testCastTemporalDatetimeToDatetime){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_DATETIME_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATETIME, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDateTime(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    EXPECT_ANY_THROW(av1 = av0->castTemporal(DT_INT));
    av1 = av0->castTemporal(DT_DATETIME_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[datetime(0), datetime(1),datetime(0), datetime(1),datetime(0), datetime(1),datetime(0), datetime(1),datetime(0), datetime(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_DATETIME);
}


TEST_F(ArrayVectorTest,testCastTemporalDatetimeToDatehour){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_DATETIME_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATETIME, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDateTime(j * 3600));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_DATEHOUR_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[datehour(0), datehour(1),datehour(0), datehour(1),datehour(0), datehour(1),datehour(0), datehour(1),datehour(0), datehour(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_DATEHOUR);
}

TEST_F(ArrayVectorTest,testCastTemporalDatetimeToNanotime){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_DATETIME_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATETIME, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDateTime(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_NANOTIME_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[nanotime(0), nanotime(1000000000),nanotime(0), nanotime(1000000000),nanotime(0), nanotime(1000000000),nanotime(0), nanotime(1000000000),nanotime(0), nanotime(1000000000)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_NANOTIME);
}

TEST_F(ArrayVectorTest,testCastTemporalDatetimeToNanotimestamp){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_DATETIME_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATETIME, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDateTime(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_NANOTIMESTAMP_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[nanotimestamp(0), nanotimestamp(1000000000),nanotimestamp(0), nanotimestamp(1000000000),nanotimestamp(0), nanotimestamp(1000000000),nanotimestamp(0), nanotimestamp(1000000000),nanotimestamp(0), nanotimestamp(1000000000)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_NANOTIMESTAMP);
}

TEST_F(ArrayVectorTest,testCastTemporalDatetimeToDate){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_DATETIME_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATETIME, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDateTime(j * 100000));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_DATE_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[date(0), date(1),date(0), date(1),date(0), date(1),date(0), date(1),date(0), date(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_DATE);
}

TEST_F(ArrayVectorTest,testCastTemporalDatetimeToMonth){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_DATETIME_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATETIME, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDateTime(j * 3000000));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_MONTH_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[month(23640), month(23641),month(23640), month(23641),month(23640), month(23641),month(23640), month(23641),month(23640), month(23641)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_MONTH);
}

TEST_F(ArrayVectorTest,testCastTemporalDatetimeToSecond){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_DATETIME_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATETIME, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDateTime(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_SECOND_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[second(0), second(1),second(0), second(1),second(0), second(1),second(0), second(1),second(0), second(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_SECOND);
}

TEST_F(ArrayVectorTest,testCastTemporalDatetimeToInt){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_DATETIME_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATETIME, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDateTime(j * 100000));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    EXPECT_ANY_THROW(av1 = av0->castTemporal(DT_INT_ARRAY));
}


TEST_F(ArrayVectorTest,testCastTemporalDatetimeToString){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_DATETIME_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATETIME, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDateTime(j * 100000));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    EXPECT_ANY_THROW(av1 = av0->castTemporal(DT_STRING_ARRAY));
}

TEST_F(ArrayVectorTest,testCastTemporalDatehourToDatehour){
    VectorSP av1;
    VectorSP datehour_av1 = Util::createArrayVector(DT_DATEHOUR_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATEHOUR, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDateHour(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        datehour_av1->append(anyv1);
    }
    av1 = datehour_av1->castTemporal(DT_DATEHOUR_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[datehour(0), datehour(1),datehour(0), datehour(1),datehour(0), datehour(1),datehour(0), datehour(1),datehour(0), datehour(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_DATEHOUR);
}

TEST_F(ArrayVectorTest,testCastTemporalDatehourToDate){
    VectorSP av1;
    VectorSP datehour_av1 = Util::createArrayVector(DT_DATEHOUR_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATEHOUR, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDateHour(j * 24));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        datehour_av1->append(anyv1);
    }
    av1 = datehour_av1->castTemporal(DT_DATE_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[date(0), date(1),date(0), date(1),date(0), date(1),date(0), date(1),date(0), date(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_DATE);
}

TEST_F(ArrayVectorTest,testCastTemporalDatehourToDatetime){
    VectorSP av1;
    VectorSP datehour_av1 = Util::createArrayVector(DT_DATEHOUR_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATEHOUR, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDateHour(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        datehour_av1->append(anyv1);
    }
    av1 = datehour_av1->castTemporal(DT_DATETIME_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[datetime(0), datetime(3600),datetime(0), datetime(3600),datetime(0), datetime(3600),datetime(0), datetime(3600),datetime(0), datetime(3600)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_DATETIME);
}


TEST_F(ArrayVectorTest,testCastTemporalDatehourToNanotime){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_DATEHOUR_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATEHOUR, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDateHour(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_NANOTIME_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[nanotime(0), nanotime(3600000000000),nanotime(0), nanotime(3600000000000),nanotime(0), \
                    nanotime(3600000000000),nanotime(0), nanotime(3600000000000),nanotime(0), nanotime(3600000000000)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_NANOTIME);
}

TEST_F(ArrayVectorTest,testCastTemporalDatehourToNanotimestamp){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_DATEHOUR_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATEHOUR, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDateHour(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_NANOTIMESTAMP_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[nanotimestamp(0), nanotimestamp(3600000000000),nanotimestamp(0), nanotimestamp(3600000000000),\
                    nanotimestamp(0), nanotimestamp(3600000000000),nanotimestamp(0), nanotimestamp(3600000000000),nanotimestamp(0), nanotimestamp(3600000000000)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_NANOTIMESTAMP);
}


TEST_F(ArrayVectorTest,testCastTemporalDatehourToMonth){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_DATEHOUR_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATEHOUR, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDateHour(j * 800));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_MONTH_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[month(23640), month(23641),month(23640), month(23641),month(23640), \
                    month(23641),month(23640), month(23641),month(23640), month(23641)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_MONTH);
}

TEST_F(ArrayVectorTest,testCastTemporalDatehourToSecond){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_DATEHOUR_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATEHOUR, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDateHour(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_SECOND_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[second(0), second(3600),second(0), second(3600),second(0), second(3600),second(0), second(3600),second(0), second(3600)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_SECOND);
}

TEST_F(ArrayVectorTest,testCastTemporalDatehourToInt){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_DATEHOUR_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATEHOUR, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDateHour(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    EXPECT_ANY_THROW(av1 = av0->castTemporal(DT_INT_ARRAY));
}


TEST_F(ArrayVectorTest,testCastTemporalDatehourToString){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_DATEHOUR_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATEHOUR, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDateHour(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    EXPECT_ANY_THROW(av1 = av0->castTemporal(DT_STRING_ARRAY));
}

TEST_F(ArrayVectorTest,testCastTemporalDateToDate){
    VectorSP av0;
    VectorSP av1 = Util::createArrayVector(DT_DATE_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATE, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDate(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av1->append(anyv1);
    }
    av0 = av1->castTemporal(DT_DATE_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[date(0), date(1),date(0), date(1),date(0), date(1),date(0), date(1),date(0), date(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av0->getString(), av1_ex->getString());
    EXPECT_EQ(av0->getType(), av1_ex->getType());
    EXPECT_EQ(av0->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av0->get(0)->get(0)->getType(), DT_DATE);
}


TEST_F(ArrayVectorTest,testCastTemporalDateToDatehour){
    VectorSP av1;
    VectorSP datehour_av1 = Util::createArrayVector(DT_DATE_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATE, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDate(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        datehour_av1->append(anyv1);
    }
    av1 = datehour_av1->castTemporal(DT_DATEHOUR_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[datehour(0), datehour(24),datehour(0), datehour(24),datehour(0), datehour(24),datehour(0), datehour(24),datehour(0), datehour(24)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_DATEHOUR);
}

TEST_F(ArrayVectorTest,testCastTemporalDateToDatetime){
    VectorSP av1;
    VectorSP datehour_av1 = Util::createArrayVector(DT_DATE_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATE, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDate(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        datehour_av1->append(anyv1);
    }
    av1 = datehour_av1->castTemporal(DT_DATETIME_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[datetime(0), datetime(86400),datetime(0), datetime(86400),datetime(0), datetime(86400),datetime(0), datetime(86400),datetime(0), datetime(86400)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_DATETIME);
}


TEST_F(ArrayVectorTest,testCastTemporalDateToNanotime){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_DATE_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATE, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDate(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    EXPECT_ANY_THROW(av1 = av0->castTemporal(DT_NANOTIME_ARRAY));
}

TEST_F(ArrayVectorTest,testCastTemporalDateToTimestamp){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_DATE_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATE, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDate(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_TIMESTAMP_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[timestamp(0), timestamp(86400000),timestamp(0), timestamp(86400000),\
                    timestamp(0), timestamp(86400000),timestamp(0), timestamp(86400000),timestamp(0), timestamp(86400000)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_TIMESTAMP);
}

TEST_F(ArrayVectorTest,testCastTemporalDateToNanotimestamp){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_DATE_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATE, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDate(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_NANOTIMESTAMP_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[nanotimestamp(0), nanotimestamp(86400000000000),nanotimestamp(0), nanotimestamp(86400000000000),\
                    nanotimestamp(0), nanotimestamp(86400000000000),nanotimestamp(0), nanotimestamp(86400000000000),nanotimestamp(0), nanotimestamp(86400000000000)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_NANOTIMESTAMP);
}

TEST_F(ArrayVectorTest,testCastTemporalDateToSecond){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_DATE_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATE, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDate(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    EXPECT_ANY_THROW(av1 = av0->castTemporal(DT_SECOND_ARRAY));
}

TEST_F(ArrayVectorTest,testCastTemporalDateToInt){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_DATE_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATE, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDate(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    EXPECT_ANY_THROW(av1 = av0->castTemporal(DT_INT_ARRAY));
}


TEST_F(ArrayVectorTest,testCastTemporalDateToString){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_DATE_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_DATE, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createDate(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    EXPECT_ANY_THROW(av1 = av0->castTemporal(DT_STRING_ARRAY));
}


TEST_F(ArrayVectorTest,testCastTemporalTimeToTime){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_TIME_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_TIME, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createTime(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_TIME_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[time(0), time(1),time(0), time(1),time(0), time(1),time(0), time(1),time(0), time(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_TIME);
}

TEST_F(ArrayVectorTest,testCastTemporalTimeToDatetime){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_TIME_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_TIME, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createTime(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    EXPECT_ANY_THROW(av1 = av0->castTemporal(DT_DATETIME_ARRAY));

}


TEST_F(ArrayVectorTest,testCastTemporalTimeToNanotime){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_TIME_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_TIME, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createTime(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_NANOTIME_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[nanotime(0), nanotime(1000000),nanotime(0), nanotime(1000000),nanotime(0), nanotime(1000000),nanotime(0), nanotime(1000000),nanotime(0), nanotime(1000000)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_NANOTIME);
}

TEST_F(ArrayVectorTest,testCastTemporalTimeToSecond){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_TIME_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_TIME, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createTime(j * 1000));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_SECOND_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[second(0), second(1),second(0), second(1),second(0), second(1),second(0), second(1),second(0), second(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_SECOND);
}

TEST_F(ArrayVectorTest,testCastTemporalTimeToMinute){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_TIME_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_TIME, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createTime(j * 60000));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_MINUTE_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[minute(0), minute(1),minute(0), minute(1),minute(0), minute(1),minute(0), minute(1),minute(0), minute(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_MINUTE);
}

TEST_F(ArrayVectorTest,testCastTemporalTimeToInt){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_TIME_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_TIME, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createTime(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    EXPECT_ANY_THROW(av1 = av0->castTemporal(DT_INT_ARRAY));
}


TEST_F(ArrayVectorTest,testCastTemporalTimeToString){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_TIME_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_TIME, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createTime(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    EXPECT_ANY_THROW(av1 = av0->castTemporal(DT_STRING_ARRAY));
}


TEST_F(ArrayVectorTest,testCastTemporalMinuteToTime){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_MINUTE_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_MINUTE, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createMinute(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_TIME_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[time(0), time(60000),time(0), time(60000),time(0), time(60000),time(0), time(60000),time(0), time(60000)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_TIME);
}

TEST_F(ArrayVectorTest,testCastTemporalMinuteToDatetime){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_MINUTE_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_MINUTE, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createMinute(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    EXPECT_ANY_THROW(av1 = av0->castTemporal(DT_DATETIME_ARRAY));

}


TEST_F(ArrayVectorTest,testCastTemporalMinuteToNanotime){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_MINUTE_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_MINUTE, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createMinute(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_NANOTIME_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[nanotime(0), nanotime(60000000000),nanotime(0), nanotime(60000000000),\
                    nanotime(0), nanotime(60000000000),nanotime(0), nanotime(60000000000),nanotime(0), nanotime(60000000000)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_NANOTIME);
}

TEST_F(ArrayVectorTest,testCastTemporalMinuteToSecond){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_MINUTE_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_MINUTE, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createMinute(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_SECOND_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[second(0), second(60),second(0), second(60),second(0), second(60),second(0), second(60),second(0), second(60)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_SECOND);
}

TEST_F(ArrayVectorTest,testCastTemporalMinuteToMinute){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_MINUTE_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_MINUTE, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createMinute(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_MINUTE_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[minute(0), minute(1),minute(0), minute(1),minute(0), minute(1),minute(0), minute(1),minute(0), minute(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_MINUTE);
}

TEST_F(ArrayVectorTest,testCastTemporalMinuteToInt){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_MINUTE_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_MINUTE, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createMinute(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    EXPECT_ANY_THROW(av1 = av0->castTemporal(DT_INT_ARRAY));
}


TEST_F(ArrayVectorTest,testCastTemporalMinuteToString){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_MINUTE_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_MINUTE, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createMinute(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    EXPECT_ANY_THROW(av1 = av0->castTemporal(DT_STRING_ARRAY));
}


TEST_F(ArrayVectorTest,testCastTemporalSecondToTime){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_SECOND_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_SECOND, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createSecond(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_TIME_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[time(0), time(1000),time(0), time(1000),time(0), time(1000),time(0), time(1000),time(0), time(1000)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_TIME);
}

TEST_F(ArrayVectorTest,testCastTemporalSecondToDatetime){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_SECOND_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_SECOND, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createSecond(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    EXPECT_ANY_THROW(av1 = av0->castTemporal(DT_DATETIME_ARRAY));

}


TEST_F(ArrayVectorTest,testCastTemporalSecondToNanotime){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_SECOND_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_SECOND, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createSecond(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_NANOTIME_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[nanotime(0), nanotime(1000000000),nanotime(0), nanotime(1000000000),\
                    nanotime(0), nanotime(1000000000),nanotime(0), nanotime(1000000000),nanotime(0), nanotime(1000000000)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_NANOTIME);
}

TEST_F(ArrayVectorTest,testCastTemporalSecondToSecond){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_SECOND_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_SECOND, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createSecond(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_SECOND_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[second(0), second(1),second(0), second(1),second(0), second(1),second(0), second(1),second(0), second(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_SECOND);
}

TEST_F(ArrayVectorTest,testCastTemporalSecondToMinute){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_SECOND_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_SECOND, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createSecond(j * 60));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_MINUTE_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[minute(0), minute(1),minute(0), minute(1),minute(0), minute(1),minute(0), minute(1),minute(0), minute(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_MINUTE);
}

TEST_F(ArrayVectorTest,testCastTemporalSecondToInt){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_SECOND_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_SECOND, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createSecond(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    EXPECT_ANY_THROW(av1 = av0->castTemporal(DT_INT_ARRAY));
}


TEST_F(ArrayVectorTest,testCastTemporalSecondToString){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_SECOND_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_SECOND, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createSecond(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    EXPECT_ANY_THROW(av1 = av0->castTemporal(DT_STRING_ARRAY));
}

TEST_F(ArrayVectorTest,testCastTemporalNanotimeToNanotime){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_NANOTIME_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_NANOTIME, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createSecond(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_NANOTIME_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[nanotime(0), nanotime(1),nanotime(0), nanotime(1),nanotime(0), nanotime(1),nanotime(0), nanotime(1),nanotime(0), nanotime(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_NANOTIME);
}


TEST_F(ArrayVectorTest,testCastTemporalNanotimeToTime){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_NANOTIME_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_NANOTIME, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createNanoTime(j * 1000000000));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_TIME_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[time(0), time(1000),time(0), time(1000),time(0), time(1000),time(0), time(1000),time(0), time(1000)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_TIME);
}

TEST_F(ArrayVectorTest,testCastTemporalNanotimeToDatetime){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_NANOTIME_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_NANOTIME, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createNanoTime(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    EXPECT_ANY_THROW(av1 = av0->castTemporal(DT_DATETIME_ARRAY));

}

TEST_F(ArrayVectorTest,testCastTemporalNanotimeToSecond){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_NANOTIME_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_NANOTIME, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createNanoTime(j * 1000000000));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_SECOND_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[second(0), second(1),second(0), second(1),second(0), second(1),second(0), second(1),second(0), second(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_SECOND);
}

TEST_F(ArrayVectorTest,testCastTemporalNanotimeToMinute){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_NANOTIME_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_NANOTIME, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createNanoTime(j * (long long)60000000000));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_MINUTE_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[minute(0), minute(1),minute(0), minute(1),minute(0), minute(1),minute(0), minute(1),minute(0), minute(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_MINUTE);
}

TEST_F(ArrayVectorTest,testCastTemporalNanotimeToInt){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_NANOTIME_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_NANOTIME, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createNanoTime(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    EXPECT_ANY_THROW(av1 = av0->castTemporal(DT_INT_ARRAY));
}


TEST_F(ArrayVectorTest,testCastTemporalNanotimeToString){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_NANOTIME_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_NANOTIME, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createNanoTime(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    EXPECT_ANY_THROW(av1 = av0->castTemporal(DT_STRING_ARRAY));
}

TEST_F(ArrayVectorTest,testCastTemporalTimestampToTime){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_TIMESTAMP_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_TIMESTAMP, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createTimestamp(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_TIME_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[time(0), time(1),time(0), time(1),time(0), time(1),time(0), time(1),time(0), time(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_TIME);
}

TEST_F(ArrayVectorTest,testCastTemporalTimestampToDatetime){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_TIMESTAMP_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_TIMESTAMP, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createTimestamp(j * 1000));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_DATETIME_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[datetime(0), datetime(1),datetime(0), datetime(1),datetime(0), datetime(1),datetime(0), datetime(1),datetime(0), datetime(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_DATETIME);
}


TEST_F(ArrayVectorTest,testCastTemporalTimestampToDatehour){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_TIMESTAMP_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_TIMESTAMP, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createTimestamp(j * 3600000));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_DATEHOUR_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[datehour(0), datehour(1),datehour(0), datehour(1),datehour(0), datehour(1),datehour(0), datehour(1),datehour(0), datehour(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_DATEHOUR);
}

TEST_F(ArrayVectorTest,testCastTemporalTimestampToNanotime){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_TIMESTAMP_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_TIMESTAMP, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createTimestamp(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_NANOTIME_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[nanotime(0), nanotime(1000000),nanotime(0), nanotime(1000000),nanotime(0), nanotime(1000000),nanotime(0), nanotime(1000000),nanotime(0), nanotime(1000000)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_NANOTIME);
}

TEST_F(ArrayVectorTest,testCastTemporalTimestampToNanotimestamp){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_TIMESTAMP_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_TIMESTAMP, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createTimestamp(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_NANOTIMESTAMP_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[nanotimestamp(0), nanotimestamp(1000000),nanotimestamp(0), nanotimestamp(1000000),\
                    nanotimestamp(0), nanotimestamp(1000000),nanotimestamp(0), nanotimestamp(1000000),nanotimestamp(0), nanotimestamp(1000000)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_NANOTIMESTAMP);
}

TEST_F(ArrayVectorTest,testCastTemporalTimestampToDate){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_TIMESTAMP_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_TIMESTAMP, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createTimestamp(j * 86400000));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_DATE_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[date(0), date(1),date(0), date(1),date(0), date(1),date(0), date(1),date(0), date(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_DATE);
}

TEST_F(ArrayVectorTest,testCastTemporalTimestampToMonth){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_TIMESTAMP_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_TIMESTAMP, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createTimestamp(j * 3000000000));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_MONTH_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[month(23640), month(23641),month(23640), month(23641),month(23640), month(23641),month(23640), month(23641),month(23640), month(23641)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_MONTH);
}

TEST_F(ArrayVectorTest,testCastTemporalTimestampToSecond){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_TIMESTAMP_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_TIMESTAMP, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createTimestamp(j * 1000));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_SECOND_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[second(0), second(1),second(0), second(1),second(0), second(1),second(0), second(1),second(0), second(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_SECOND);
}

TEST_F(ArrayVectorTest,testCastTemporalTimestampToInt){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_TIMESTAMP_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_TIMESTAMP, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createTimestamp(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    EXPECT_ANY_THROW(av1 = av0->castTemporal(DT_INT_ARRAY));
}


TEST_F(ArrayVectorTest,testCastTemporalTimestampToString){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_TIMESTAMP_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_TIMESTAMP, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createTimestamp(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    EXPECT_ANY_THROW(av1 = av0->castTemporal(DT_STRING_ARRAY));
}


TEST_F(ArrayVectorTest,testCastTemporalNanotimestampToDatetime){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_NANOTIMESTAMP_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_NANOTIMESTAMP, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createNanoTimestamp(j * 1000000000));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_DATETIME_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[datetime(0), datetime(1),datetime(0), datetime(1),datetime(0), datetime(1),datetime(0), datetime(1),datetime(0), datetime(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_DATETIME);
}


TEST_F(ArrayVectorTest,testCastTemporalNanotimestampToDatehour){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_NANOTIMESTAMP_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_NANOTIMESTAMP, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createNanoTimestamp(j * 3600000000000));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_DATEHOUR_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[datehour(0), datehour(1),datehour(0), datehour(1),datehour(0), datehour(1),datehour(0), datehour(1),datehour(0), datehour(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_DATEHOUR);
}

TEST_F(ArrayVectorTest,testCastTemporalNanotimestampToNanotime){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_NANOTIMESTAMP_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_NANOTIMESTAMP, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createNanoTimestamp(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_NANOTIME_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[nanotime(0), nanotime(1),nanotime(0), nanotime(1),nanotime(0), nanotime(1),nanotime(0), nanotime(1),nanotime(0), nanotime(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_NANOTIME);
}

TEST_F(ArrayVectorTest,testCastTemporalNanotimestampToNanotimestamp){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_NANOTIMESTAMP_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_NANOTIMESTAMP, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createNanoTimestamp(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_NANOTIMESTAMP_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[nanotimestamp(0), nanotimestamp(1),nanotimestamp(0), nanotimestamp(1),\
                    nanotimestamp(0), nanotimestamp(1),nanotimestamp(0), nanotimestamp(1),nanotimestamp(0), nanotimestamp(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_NANOTIMESTAMP);
}

TEST_F(ArrayVectorTest,testCastTemporalNanotimestampToDate){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_NANOTIMESTAMP_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_NANOTIMESTAMP, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createNanoTimestamp(j * (long long)86400000000000));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_DATE_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[date(0), date(1),date(0), date(1),date(0), date(1),date(0), date(1),date(0), date(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_DATE);
}

TEST_F(ArrayVectorTest,testCastTemporalNanotimestampToMonth){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_NANOTIMESTAMP_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_NANOTIMESTAMP, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createNanoTimestamp(j * (long long)3000000000000000));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_MONTH_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[month(23640), month(23641),month(23640), month(23641),month(23640), month(23641),month(23640), month(23641),month(23640), month(23641)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_MONTH);
}

TEST_F(ArrayVectorTest,testCastTemporalNanotimestampToSecond){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_NANOTIMESTAMP_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_NANOTIMESTAMP, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createNanoTimestamp(j * 1000000000));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    av1 = av0->castTemporal(DT_SECOND_ARRAY);

    string script1 = "index=2 4 6 8 10;\
                    v=[second(0), second(1),second(0), second(1),second(0), second(1),second(0), second(1),second(0), second(1)];\
                    arrayVector(index,v)";
    VectorSP av1_ex = conn.run(script1);
    EXPECT_EQ(av1->getString(), av1_ex->getString());
    EXPECT_EQ(av1->getType(), av1_ex->getType());
    EXPECT_EQ(av1->get(0)->getForm(), DF_VECTOR);
    EXPECT_EQ(av1->get(0)->get(0)->getType(), DT_SECOND);
}

TEST_F(ArrayVectorTest,testCastTemporalNanotimestampToInt){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_NANOTIMESTAMP_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_NANOTIMESTAMP, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createNanoTimestamp(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    EXPECT_ANY_THROW(av1 = av0->castTemporal(DT_INT_ARRAY));
}


TEST_F(ArrayVectorTest,testCastTemporalNanotimestampToString){
    VectorSP av1;
    VectorSP av0 = Util::createArrayVector(DT_NANOTIMESTAMP_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_NANOTIMESTAMP, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createNanoTimestamp(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av0->append(anyv1);
    }
    EXPECT_ANY_THROW(av1 = av0->castTemporal(DT_STRING_ARRAY));
}

TEST_F(ArrayVectorTest,testCastTemporalMonthToDate){
    VectorSP av0;
    VectorSP av1 = Util::createArrayVector(DT_MONTH_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_MONTH, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createMonth(j / 30));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av1->append(anyv1);
    }
    EXPECT_ANY_THROW(av0 = av1->castTemporal(DT_DATE_ARRAY));
}

TEST_F(ArrayVectorTest,testCastTemporalIntToDate){
    VectorSP av0;
    VectorSP av1 = Util::createArrayVector(DT_INT_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_INT, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createInt(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av1->append(anyv1);
    }
    EXPECT_ANY_THROW(av0 = av1->castTemporal(DT_DATE_ARRAY));
}

TEST_F(ArrayVectorTest,testCastTemporalSecondToDate){
    VectorSP av0;
    VectorSP av1 = Util::createArrayVector(DT_SECOND_ARRAY, 0, 10);
    for (int i = 0; i < 5; i++) {
        VectorSP time = Util::createVector(DT_SECOND, 2);
        for (int j = 0; j < 2; j++) {
            time->set(j, Util::createSecond(j * 1));
        }
        VectorSP anyv1 = Util::createVector(DT_ANY, 1);
        anyv1->set(0, time);
        av1->append(anyv1);
    }
    EXPECT_ANY_THROW(av0 = av1->castTemporal(DT_DATE_ARRAY));
}

TEST_F(ArrayVectorTest,testDecimal32ArrayVector){
    VectorSP av1 = Util::createArrayVector(DT_DECIMAL32_ARRAY, 0, 3, true, 2);
    VectorSP v1 = Util::createVector(DT_DECIMAL32, 2, 2, true, 2);
    VectorSP v2 = Util::createVector(DT_DECIMAL32, 2, 2, true, 2);
    ConstantSP val1 = Util::createDecimal32(2, 2.35123);
    ConstantSP val2 = Util::createDecimal32(2, 0.1);
    for(auto i =0;i<v1->size();i++){
        v1->set(i, val1);
        v2->set(i, val2);
    }
    av1->append(v1);
    av1->append(v2);
    conn.upload("av1",av1);
    ConstantSP res = conn.run("index = 2 4;val = [decimal32(2.35123,2),decimal32(2.35123,2),decimal32(0.1,2),decimal32(0.1,2)];\
                                ex=arrayVector(index,val);eqObj(ex,av1)");
    EXPECT_TRUE(res->getBool());
    ConstantSP ex = conn.run("ex");
    EXPECT_EQ(ex->getString(),av1->getString());

}

TEST_F(ArrayVectorTest,testDecimal64ArrayVector){
    VectorSP av1 = Util::createArrayVector(DT_DECIMAL64_ARRAY, 0, 3, true, 11);
    VectorSP v1 = Util::createVector(DT_DECIMAL64, 2, 2, true, 11);
    VectorSP v2 = Util::createVector(DT_DECIMAL64, 2, 2, true, 11);
    ConstantSP val1 = Util::createDecimal64(11, 2.35123);
    ConstantSP val2 = Util::createDecimal64(11, 0.1);
    for(auto i =0;i<v1->size();i++){
        v1->set(i, val1);
        v2->set(i, val2);
    }
    av1->append(v1);
    av1->append(v2);
    conn.upload("av1",av1);
    ConstantSP res = conn.run("index = 2 4;val = [decimal64(2.35123,11),decimal64(2.35123,11),decimal64(0.1,11),decimal64(0.1,11)];\
                                ex=arrayVector(index,val);eqObj(ex,av1)");
    EXPECT_TRUE(res->getBool());
    ConstantSP ex = conn.run("ex");
    EXPECT_EQ(ex->getString(),av1->getString());
}

TEST_F(ArrayVectorTest,testDecimal32ArrayVector_gt65535){
    VectorSP indV = conn.run("1..35000*2");
    VectorSP valV = Util::createVector(DT_DECIMAL32, 70000, 70000, true, 2);
    ConstantSP val1 = Util::createDecimal32(2, 2.35123);
    ConstantSP val2 = Util::createDecimal32(2, 0.1);
    for(auto i =0;i<valV->size() /2;i++){
        valV->set(2*i, val1);
        valV->set(2*i+1, val2);
    }

    VectorSP av1 = Util::createArrayVector(indV, valV);
    conn.upload("av1",av1);

    ConstantSP res = conn.run("index = 1..35000*2;val = take([decimal32(2.35123,2),decimal32(0.1,2),decimal32(2.35123,2),decimal32(0.1,2)],70000);\
                                ex=arrayVector(index,val);eqObj(ex,av1)");
    EXPECT_TRUE(res->getBool());

    ConstantSP ex = conn.run("ex");
    EXPECT_EQ(ex->getString(),av1->getString());

}

TEST_F(ArrayVectorTest,testDecimal64ArrayVector_gt65535){
    VectorSP indV = conn.run("1..35000*2");
    VectorSP valV = Util::createVector(DT_DECIMAL64, 70000, 70000, true, 11);
    ConstantSP val1 = Util::createDecimal64(11, 2.35123);
    ConstantSP val2 = Util::createDecimal64(11, 0.1);
    for(auto i =0;i<valV->size() /2;i++){
        valV->set(2*i, val1);
        valV->set(2*i+1, val2);
    }

    VectorSP av1 = Util::createArrayVector(indV, valV);
    conn.upload("av1",av1);

    ConstantSP res = conn.run("index = 1..35000*2;val = take([decimal64(2.35123,11),decimal64(0.1,11),decimal64(2.35123,11),decimal64(0.1,11)],70000);\
                                ex=arrayVector(index,val);eqObj(ex,av1)");
    EXPECT_TRUE(res->getBool());
    
    ConstantSP ex = conn.run("ex");
    EXPECT_EQ(ex->getString(),av1->getString());

}