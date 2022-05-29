class CompressTest:public testing::Test
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


TEST_F(CompressTest,CompressLong){
    const int count = 60000;
    vector<string> colName={"time","value"};
    vector<int> time(count);
    vector<long long>value(count);

    int basetime = Util::countDays(2012,1,1);
    for(int i=0;i<count;i++){
        time[i] = basetime + (i%15);
        value[i] = i;
    }

    VectorSP timeVector = Util::createVector(DT_DATE,count, count);
    VectorSP valueVector = Util::createVector(DT_LONG, count,count);
    timeVector->setInt(0,count,time.data());
    valueVector->setLong(0,count,value.data());
    vector<ConstantSP> colVector {timeVector,valueVector};
    TableSP table  = Util::createTable(colName,colVector);
    vector<COMPRESS_METHOD> typeVec{COMPRESS_DELTA, COMPRESS_DELTA};
    table->setColumnCompressMethods(typeVec);

    vector<ConstantSP> args{table};
    conn_compress.run("share streamTable(1:0, `time`value,[DATE,LONG]) as table1");
    int success = conn_compress.run("tableInsert{table1}",args)->getInt();
    EXPECT_EQ(success, count);

    TableSP t1 = conn_compress.run("select * from table1");
    EXPECT_EQ(t1->getString(),table->getString());

}




TEST_F(CompressTest,CompressVectorLongerThanTable){
    const int count = 60000;
    vector<string> colName={"time","value"};
    vector<int> time(count);
    vector<long long>value(count);

    int basetime = Util::countDays(2012,1,1);
    for(int i=0;i<count;i++){
        time[i] = basetime + (i%15);
        value[i] = i;
    }

    VectorSP timeVector = Util::createVector(DT_DATE,count, count);
    VectorSP valueVector = Util::createVector(DT_LONG, count,count);
    timeVector->setInt(0,0,time.data());
    valueVector->setLong(0,0,value.data());
    vector<ConstantSP> colVector {timeVector,valueVector};
    TableSP table  = Util::createTable(colName,colVector);
    vector<COMPRESS_METHOD> typeVec{COMPRESS_DELTA, COMPRESS_DELTA,COMPRESS_LZ4};
    EXPECT_ANY_THROW(table->setColumnCompressMethods(typeVec));
}




TEST_F(CompressTest,CompressVectorLessThanTable){
    const int count = 600000;
    vector<string> colName={"time","value"};
    vector<int> time(count);
    vector<long long>value(count);

    int basetime = Util::countDays(2012,1,1);
    for(int i=0;i<count;i++){
        time[i] = basetime + (i%15);
        value[i] = i;
    }

    VectorSP timeVector = Util::createVector(DT_DATE,count, count);
    VectorSP valueVector = Util::createVector(DT_LONG, count,count);
    timeVector->setInt(0,count,time.data());
    valueVector->setLong(0,count,value.data());
    vector<ConstantSP> colVector {timeVector,valueVector};
    TableSP table  = Util::createTable(colName,colVector);
    vector<COMPRESS_METHOD> typeVec{COMPRESS_METHOD::COMPRESS_DELTA};
    //table->setColumnCompressTypes(typeVec);
    EXPECT_ANY_THROW(table->setColumnCompressMethods(typeVec));
}


TEST_F(CompressTest,CompressWithErrorDataType){
    const int count = 600000;
    vector<string> colName={"time","value","name"};
    vector<int> time(count);
    vector<long long>value(count);
    vector<string> name(count);

    int basetime = Util::countDays(2012,1,1);
    for(int i=0;i<count;i++){
        time[i] = basetime + (i%15);
        value[i] = i;
        name[i] = to_string(i);
    }

    VectorSP timeVector = Util::createVector(DT_DATE,count, count);
    VectorSP valueVector = Util::createVector(DT_LONG, count,count);
    VectorSP nameVector = Util::createVector(DT_STRING,count,count,count);
    timeVector->setInt(0,count,time.data());
    valueVector->setLong(0,count,value.data());
    nameVector->setString(0,count,name.data());
    vector<ConstantSP> colVector {timeVector,valueVector,nameVector};
    TableSP table  = Util::createTable(colName,colVector);
    vector<COMPRESS_METHOD> typeVec{COMPRESS_DELTA,COMPRESS_LZ4,COMPRESS_DELTA};
    EXPECT_ANY_THROW(table->setColumnCompressMethods(typeVec));
}

TEST_F(CompressTest,CompressIncludeNull){
    const int count = 600000;
    vector<string> colName={"time","value","name"};
    vector<int> time(count);
    vector<long long>value(count);
    vector<string> name(count);

    int basetime = Util::countDays(2012,1,1);
    for(int i=0;i<count;i++){
        time[i] = basetime + (i%15);
        value[i] = (i % 500 == 0) ? i : (long long) nullptr ;
        name[i] = to_string(i);
    }

    VectorSP timeVector = Util::createVector(DT_DATE,count, count);
    VectorSP valueVector = Util::createVector(DT_LONG, count,count);
    VectorSP nameVector = Util::createVector(DT_STRING,count,count,count);
    timeVector->setInt(0,count,time.data());
    valueVector->setLong(0,count,value.data());
    nameVector->setString(0,count,name.data());
    vector<ConstantSP> colVector {timeVector,valueVector,nameVector};
    TableSP table  = Util::createTable(colName,colVector);
    vector<COMPRESS_METHOD> typeVec{COMPRESS_METHOD::COMPRESS_DELTA,COMPRESS_METHOD::COMPRESS_LZ4,COMPRESS_METHOD::COMPRESS_LZ4};
    table->setColumnCompressMethods(typeVec);

    vector<ConstantSP> args{table};
    conn_compress.run("share streamTable(1:0, `time`value`name,[DATE,LONG,STRING]) as table1");
    int success = conn_compress.run("tableInsert{table1}",args)->getInt();
    EXPECT_EQ(success, count);

    TableSP t1 = conn_compress.run("select * from table1");
    EXPECT_EQ(t1->getString(),table->getString());
}


TEST_F(CompressTest,CompressWithAllType){
    const int count = 600000;
    vector<string> colName={"cbool","cchar","cshort","cint","cdate","cmonth","ctime","cminute", "csecond","cdatetime","ctimestamp","cnanotime",
                            "cnanotimestamp","cfloat","cdouble","csymbol","cstring","cipaddr","cblob"};
    vector<int> time(count);
    vector<long long>value(count);
    vector<float> cfloat(count);
    vector<string> name(count);
    vector<string> blob(count);
    vector<string> ipaddr(count);
    int basetime = Util::countDays(2012,1,1);
    for(int i=0;i<count;i++){
        time[i] = basetime + (i%15);
        value[i] = (i % 500 == 0) ? i : (long long) nullptr;
        cfloat[i] = float(i)+0.1;
        name[i] = to_string(i);
        blob[i] = "fdsafsdfasd"+ to_string(i%32);
        ipaddr[i] = "192.168.100." + to_string(i%255);
    }

    VectorSP boolVector = Util::createVector(DT_BOOL,count, count);
    VectorSP charVector = Util::createVector(DT_CHAR, count,count);
    VectorSP shortVector = Util::createVector(DT_SHORT,count,count);
    VectorSP intVector = Util::createVector(DT_INT,count,count);
    VectorSP dateVector = Util::createVector(DT_DATE,count,count);
    VectorSP monthVector = Util::createVector(DT_MONTH,count,count);
    VectorSP timeVector = Util::createVector(DT_TIME,count,count);
    VectorSP minuteVector = Util::createVector(DT_MINUTE,count,count);
    VectorSP secondVector = Util::createVector(DT_SECOND,count,count);
    VectorSP datetimeVector = Util::createVector(DT_DATETIME,count,count);
    VectorSP timestampVector = Util::createVector(DT_TIMESTAMP,count,count);
    VectorSP nanotimeVector = Util::createVector(DT_NANOTIME,count,count);
    VectorSP nanotimestampVector = Util::createVector(DT_NANOTIMESTAMP,count,count);
    VectorSP floatVector = Util::createVector(DT_FLOAT,count,count);
    VectorSP doubleVector = Util::createVector(DT_DOUBLE,count,count);
    VectorSP symbolVector = Util::createVector(DT_SYMBOL,count,count);
    VectorSP stringVector = Util::createVector(DT_STRING,count,count);
    VectorSP ipaddrVector = Util::createVector(DT_IP, count,count);
    VectorSP blobVector = Util::createVector(DT_BLOB, count,count);

    boolVector->setInt(0,count,time.data());
    charVector->setInt(0,count,time.data());
    shortVector->setInt(0,count,time.data());
    intVector->setInt(0,count,time.data());
    dateVector->setInt(0,count,time.data());
    monthVector->setInt(0,count,time.data());
    timeVector->setInt(0,count,time.data());
    minuteVector->setInt(0,count,time.data());
    secondVector->setInt(0,count,time.data());
    datetimeVector->setLong(0,count,value.data());
    timestampVector->setLong(0,count,value.data());
    nanotimeVector->setLong(0,count,value.data());
    nanotimestampVector->setLong(0,count,value.data());
    floatVector->setFloat(0,count,cfloat.data());
    doubleVector->setFloat(0,count,cfloat.data());
    symbolVector->setString(0,count,name.data());
    stringVector->setString(0,count,name.data());
    ipaddrVector->setString(0,count,ipaddr.data());
    blobVector->setString(0,count,blob.data());

    vector<ConstantSP> colVector {boolVector,charVector,shortVector,intVector,dateVector,monthVector,timeVector,minuteVector,secondVector,
                                  datetimeVector,timestampVector,nanotimeVector,nanotimestampVector,floatVector,doubleVector,symbolVector,stringVector,
                                  ipaddrVector,blobVector};
    TableSP table  = Util::createTable(colName,colVector);
    vector<COMPRESS_METHOD> typeVec{COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_DELTA,
                                    COMPRESS_DELTA,COMPRESS_DELTA,COMPRESS_DELTA,COMPRESS_DELTA,
                                    COMPRESS_DELTA,COMPRESS_DELTA,COMPRESS_DELTA,COMPRESS_DELTA,
                                    COMPRESS_DELTA,COMPRESS_LZ4,COMPRESS_LZ4,
                                    COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4};
    table->setColumnCompressMethods(typeVec);

    vector<ConstantSP> args{table};
    string script = "colName =  `cbool`cchar`cshort`cint`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring`cipaddr`cblob;\n"
                    "colType = [BOOL, CHAR, SHORT, INT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING,IPADDR,BLOB];\n"
                    "share streamTable(1:0,colName,colType) as table1;";
    conn_compress.run(script);
    int success = conn_compress.run("tableInsert{table1}",args)->getInt();
    EXPECT_EQ(success, count);

    TableSP t1 = conn_compress.run("select * from table1");
//    cout << t1->getString() << endl;
//    cout << table->getString() <<endl;
    EXPECT_EQ(t1->getString(),table->getString());
}


TEST_F(CompressTest,CompressLZ4WithAllType){
    const int count = 600000;
    vector<string> colName={"cbool","cchar","cshort","cint","cdate","cmonth","ctime","cminute", "csecond","cdatetime","ctimestamp","cnanotime",
                            "cnanotimestamp","cfloat","cdouble","csymbol","cstring","cipaddr","cblob"};
    vector<int> time(count);
    vector<long long>value(count);
    vector<float> cfloat(count);
    vector<string> name(count);
    vector<string> blob(count);
    vector<string> ipaddr(count);
    int basetime = Util::countDays(2012,1,1);
    for(int i=0;i<count;i++){
        time[i] = basetime + (i%15);
        value[i] = (i % 500 == 0) ? i : (long long) nullptr;
        cfloat[i] = float(i)+0.1;
        name[i] = to_string(i);
        blob[i] = "fdsafsdfasd"+ to_string(i%32);
        ipaddr[i] = "192.168.100." + to_string(i%255);
    }

    VectorSP boolVector = Util::createVector(DT_BOOL,count, count);
    VectorSP charVector = Util::createVector(DT_CHAR, count,count);
    VectorSP shortVector = Util::createVector(DT_SHORT,count,count);
    VectorSP intVector = Util::createVector(DT_INT,count,count);
    VectorSP dateVector = Util::createVector(DT_DATE,count,count);
    VectorSP monthVector = Util::createVector(DT_MONTH,count,count);
    VectorSP timeVector = Util::createVector(DT_TIME,count,count);
    VectorSP minuteVector = Util::createVector(DT_MINUTE,count,count);
    VectorSP secondVector = Util::createVector(DT_SECOND,count,count);
    VectorSP datetimeVector = Util::createVector(DT_DATETIME,count,count);
    VectorSP timestampVector = Util::createVector(DT_TIMESTAMP,count,count);
    VectorSP nanotimeVector = Util::createVector(DT_NANOTIME,count,count);
    VectorSP nanotimestampVector = Util::createVector(DT_NANOTIMESTAMP,count,count);
    VectorSP floatVector = Util::createVector(DT_FLOAT,count,count);
    VectorSP doubleVector = Util::createVector(DT_DOUBLE,count,count);
    VectorSP symbolVector = Util::createVector(DT_SYMBOL,count,count);
    VectorSP stringVector = Util::createVector(DT_STRING,count,count);
    VectorSP ipaddrVector = Util::createVector(DT_IP, count,count);
    VectorSP blobVector = Util::createVector(DT_BLOB, count,count);

    boolVector->setInt(0,count,time.data());
    charVector->setInt(0,count,time.data());
    shortVector->setInt(0,count,time.data());
    intVector->setInt(0,count,time.data());
    dateVector->setInt(0,count,time.data());
    monthVector->setInt(0,count,time.data());
    timeVector->setInt(0,count,time.data());
    minuteVector->setInt(0,count,time.data());
    secondVector->setInt(0,count,time.data());
    datetimeVector->setLong(0,count,value.data());
    timestampVector->setLong(0,count,value.data());
    nanotimeVector->setLong(0,count,value.data());
    nanotimestampVector->setLong(0,count,value.data());
    floatVector->setFloat(0,count,cfloat.data());
    doubleVector->setFloat(0,count,cfloat.data());
    symbolVector->setString(0,count,name.data());
    stringVector->setString(0,count,name.data());
    ipaddrVector->setString(0,count,ipaddr.data());
    blobVector->setString(0,count,blob.data());

    vector<ConstantSP> colVector {boolVector,charVector,shortVector,intVector,dateVector,monthVector,timeVector,minuteVector,secondVector,
                                  datetimeVector,timestampVector,nanotimeVector,nanotimestampVector,floatVector,doubleVector,symbolVector,stringVector,
                                  ipaddrVector,blobVector};
    TableSP table  = Util::createTable(colName,colVector);
    vector<COMPRESS_METHOD> typeVec{COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,
                                    COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,
                                    COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,
                                    COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,
                                    COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4};
    table->setColumnCompressMethods(typeVec);

    vector<ConstantSP> args{table};
    string script = "colName =  `cbool`cchar`cshort`cint`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring`cipaddr`cblob;\n"
                    "colType = [BOOL, CHAR, SHORT, INT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING,IPADDR,BLOB];\n"
                    "share streamTable(1:0,colName,colType) as table1;";
    conn_compress.run(script);
    int success = conn_compress.run("tableInsert{table1}",args)->getInt();
    EXPECT_EQ(success, count);

    TableSP t1 = conn_compress.run("select * from table1");
//    cout << t1->getString() << endl;
//    cout << table->getString() <<endl;
    EXPECT_EQ(t1->getString(),table->getString());
}




TEST_F(CompressTest,CompressVectorWithDiffType){
    const int count = 600000;
    vector<string> colName={"time","value"};
    vector<int> time(count);
    vector<long long>value(count);

    int basetime = Util::countDays(2012,1,1);
    for(int i=0;i<count;i++){
        time[i] = basetime + (i%15);
        value[i] = i;
    }

    VectorSP timeVector = Util::createVector(DT_DATE,count, count);
    VectorSP valueVector = Util::createVector(DT_LONG, count,count);
    timeVector->setInt(0,count,time.data());
    valueVector->setLong(0,count,value.data());
    vector<ConstantSP> colVector {timeVector,valueVector};
    TableSP table  = Util::createTable(colName,colVector);
    vector<COMPRESS_METHOD> typeVec{COMPRESS_DELTA,COMPRESS_LZ4};
    //table->setColumnCompressTypes(typeVec);
    vector<ConstantSP> args{table};
    conn_compress.run("share streamTable(1:0, `time`value,[DATE,LONG]) as table1");
    int success = conn_compress.run("tableInsert{table1}",args)->getInt();
    EXPECT_EQ(success, count);
    TableSP t1 = conn_compress.run("select * from table1");
    EXPECT_EQ(t1->getString(),table->getString());
}

TEST_F(CompressTest,CompressWithDiffIndfsTable){
    string stricpt = "dbName = \"dfs://test_compress\"\n"
                     "if(existsDatabase(dbName)){\n"
                     "\tdropDatabase(dbName)\t\n"
                     "}\n"
                     "db = database(dbName,VALUE,`A`B`C)\n"
                     "t1 = table(1:0,`id`name`value,[INT,SYMBOL,LONG])\n"
                     "pt = db.createPartitionedTable(t1,`pt,`name,{\"id\":\"lz4\",\"value\":\"delta\"});";
    conn_compress.run(stricpt);
    const int count = 600000;
    vector<string> colName={"id","name","value"};
    vector<int> id(count);
    vector<string> name(count);
    vector<long long>value(count);

    string names[] = {"A","B", "C"};
    for(int i=0;i<count;i++){
        id[i] = i%15;
        name[i] = names[i%3];
        value[i] = i+1000;
    }
    VectorSP idVector = Util::createVector(DT_INT,count,count);
    VectorSP nameVector = Util::createVector(dolphindb::DT_SYMBOL,count,count);
    VectorSP valueVector = Util::createVector(DT_LONG,count,count);
    idVector->setInt(0,count,id.data());
    nameVector->setString(0,count,name.data());
    valueVector->setLong(0,count,value.data());
    vector<ConstantSP> colVector{idVector,nameVector,valueVector};
    TableSP table = Util::createTable(colName,colVector);
    vector<ConstantSP> args{table};
    vector<COMPRESS_METHOD> compress = {COMPRESS_DELTA,COMPRESS_LZ4,COMPRESS_LZ4};
    table->setColumnCompressMethods(compress);
    int success = conn_compress.run("tableInsert{loadTable(dbName,`pt)}",args)->getInt();
    EXPECT_EQ(success,count);
    TableSP t1 = conn_compress.run("select * from loadTable(dbName,`pt) order by value");
    EXPECT_EQ(t1->getString(),table->getString());
    conn_compress.run("dropDatabase(dbName)");
}



TEST_F(CompressTest,CompressLongWithArrayVector){
    int count = 60000;
    vector<string> colName(19);
    colName[0] = "time";
    colName[1] = "values";
    for(int i=2;i<19;i++){
        colName[i] = "factor"+ to_string(i);
    }
    vector<int> time(count);
    vector<long long>value(count);

    int basetime = Util::countDays(2012,1,1);
    for(int i=0;i<count;i++){
        time[i] = basetime + (i%15);
        value[i] = i;
    }
    VectorSP timeVector = Util::createVector(DT_DATE,count, count);
    VectorSP valueVector = Util::createVector(DT_LONG, count,count);
    timeVector->setInt(0,count,time.data());
    valueVector->setLong(0,count,value.data());

    //array vector
    VectorSP Index = conn_compress.run("take(5 3 7 3,60000).cumsum().int()");
    count = 270000;

    VectorSP boolvector = Util::createVector(DT_BOOL,count,count);
    VectorSP charvector = Util::createVector(DT_CHAR,count,count);
    VectorSP shortvector = Util::createVector(dolphindb::DT_SHORT,count,count);
    VectorSP intvector = Util::createVector(dolphindb::DT_INT,count,count);
    VectorSP longvector = Util::createVector(DT_LONG,count,count);
    VectorSP floatvector = Util::createVector(DT_FLOAT,count,count);
    VectorSP doublevector = Util::createVector(DT_DOUBLE, count,count);
    VectorSP datevector = Util::createVector(DT_DATE,count,count);
    VectorSP timestampvector = Util::createVector(dolphindb::DT_TIMESTAMP,count,count);
    VectorSP datehourvector = Util::createVector(dolphindb::DT_DATEHOUR,count,count);
    VectorSP datetimevector = Util::createVector(DT_DATETIME,count,count);
    VectorSP timevector = Util::createVector(DT_TIME,count,count);
    VectorSP minutevector = Util::createVector(DT_MINUTE, count,count);
    VectorSP monthvector = Util::createVector(DT_MONTH,count,count);
    VectorSP secondvector = Util::createVector(DT_SECOND,count,count);
    VectorSP nanotimevector =  Util::createVector(dolphindb::DT_NANOTIME,count,count);
    VectorSP nanotimestampVector  =Util::createVector(dolphindb::DT_NANOTIMESTAMP,count,count);
    vector<int> arrayValues(count);
    for(int i=0;i<count;i++){
        arrayValues[i] = i;
    }
    boolvector->setInt(0,count, arrayValues.data());
    charvector->setInt(0,count,arrayValues.data());
    shortvector->setInt(0,count,arrayValues.data());
    intvector->setInt(0,count,arrayValues.data());
    longvector->setInt(0,count,arrayValues.data());
    floatvector->setInt(0,count,arrayValues.data());
    doublevector->setInt(0,count,arrayValues.data());
    datevector->setInt(0,count,arrayValues.data());
    timestampvector->setInt(0,count,arrayValues.data());
    datehourvector->setInt(0,count,arrayValues.data());
    datetimevector->setInt(0,count,arrayValues.data());
    timevector->setInt(0,count,arrayValues.data());
    minutevector->setInt(0,count,arrayValues.data());
    monthvector->setInt(0,count,arrayValues.data());
    secondvector->setInt(0,count,arrayValues.data());
    nanotimevector->setInt(0,count,arrayValues.data());
    nanotimestampVector->setInt(0,count,arrayValues.data());

    VectorSP boolArray = Util::createArrayVector(Index, boolvector);
    VectorSP charArray = Util::createArrayVector(Index, charvector);
    VectorSP shortArray  = Util::createArrayVector(Index, shortvector);
    VectorSP longArray  = Util::createArrayVector(Index, longvector);
    VectorSP intArray  = Util::createArrayVector(Index, intvector);
    VectorSP floatArray = Util::createArrayVector(Index, floatvector);
    VectorSP doubleArray = Util::createArrayVector(Index, doublevector);
    VectorSP dateArray = Util::createArrayVector(Index, datevector);
    VectorSP timestampArray = Util::createArrayVector(Index, timestampvector);
    VectorSP datetimeArray = Util::createArrayVector(Index, datetimevector);
    VectorSP datehourArray = Util::createArrayVector(Index, datehourvector);
    VectorSP timeArray = Util::createArrayVector(Index, timevector);
    VectorSP minuteArray = Util::createArrayVector(Index, minutevector);
    VectorSP monthArray = Util::createArrayVector(Index, monthvector);
    VectorSP secondeArray = Util::createArrayVector(Index, secondvector);
    VectorSP nanotimeArray = Util::createArrayVector(Index, nanotimevector);
    VectorSP nanotimestampArray = Util::createArrayVector(Index, nanotimestampVector);

    //table insert
    vector<ConstantSP> colVector {timeVector,valueVector,boolArray, charArray, shortArray,longArray,intArray, floatArray, doubleArray,
                                  dateArray, timestampArray,datetimeArray, datehourArray, timeArray, minuteArray, monthArray,
                                  secondeArray,nanotimeArray, nanotimeArray};
    TableSP table  = Util::createTable(colName,colVector);

    vector<COMPRESS_METHOD> typeVec(19);
    typeVec[0] = COMPRESS_DELTA;
    typeVec[1] = COMPRESS_DELTA;
    for(int i=0;i<17;i++){
        typeVec[i+2] = COMPRESS_LZ4;
    }
    table->setColumnCompressMethods(typeVec);

    vector<ConstantSP> args{table};
    conn_compress.run("colName = `time`values\n"
             "for(i in 2..18){\n"
             "\tcolName.append!(\"factor\"+string(i))\n"
             "}\n"
             "colType = [DATE,LONG,BOOL[],CHAR[],SHORT[],INT[],LONG[],FLOAT[],DOUBLE[],DATE[],TIMESTAMP[],DATEHOUR[],DATETIME[],\n"
             "              TIME[],MINUTE[],MONTH[],SECOND[],NANOTIME[],NANOTIMESTAMP[]]\n"
             "share streamTable(100:0,colName,colType) as table1");
    int success = conn_compress.run("tableInsert{table1}",args)->getInt();
    //cout << success;
    EXPECT_EQ(success, 60000);

    TableSP t1 = conn_compress.run("select * from table1");
    EXPECT_EQ(t1->getString(),table->getString());

}
