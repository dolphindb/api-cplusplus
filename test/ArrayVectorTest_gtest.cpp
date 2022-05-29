class ArrayVectorTest:public testing::Test
{
protected:
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
    index = Util::createVector(DT_INT,4,10);
    value = Util::createVector(DT_INT,10,20);
    for(int i=0;i<10;++i){
        value->set(i,Util::createInt(i+1));
    }
    index->set(0,Util::createInt(2));
    index->set(1,Util::createInt(5));
    index->set(2,Util::createInt(8));
    index->set(3,Util::createInt(10));
    data = Util::createArrayVector(index,value);
    vector<ConstantSP> datas = {data};
    vector<string> dataname = { "datas" };
    conn.upload(dataname,datas);
    VectorSP expection = conn.run("a = take(1..10,10);res = arrayVector([2,5,8,10],a);re2=res+1;re2;");
    res= conn.run("re1=datas+1;re1;");
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
                           "index = rand(1..524288,10000);\n"
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