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

char* random_uuid(char buf[37]) {
    const char *c = "89ab";
    char *p = buf;
    int n;
    for (n = 0; n < 16; ++n) {
        int b = rand() % 255;
        switch (n) {
        case 6:
            sprintf(p, "4%x", b % 15);
            break;
        case 8:
            sprintf(p, "%c%x", c[rand() % strlen(c)], b % 15);
            break;
        default:
            sprintf(p, "%02x", b);
            break;
        }
        p += 2;
        switch (n) {
        case 3:
        case 5:
        case 7:
        case 9:
            *p++ = '-';
            break;
        }
    }
    *p = 0;
    return buf;
}

TEST_F(ScalarTest,testScalar){
    int i;

    int intVal[2] = { 0x7fffffff, -0x7fffffff };
    long long longVal[2] = { 0x7fffffffffffffff, -0x7fffffffffffffff }; //9223372036854775807
    for (i = 0; i < 2; i++) {
        ConstantSP spInt = Util::createInt(intVal[i]);
        EXPECT_EQ(spInt->getInt(), intVal[i]);
        spInt = Util::parseConstant(DT_INT, std::to_string(intVal[i]));
        EXPECT_EQ(spInt->getInt(), intVal[i]);
        EXPECT_EQ(spInt->isScalar() && spInt->getForm() == DF_SCALAR && spInt->getType() == DT_INT,true);

        ConstantSP spLong = Util::createLong(longVal[i]);
        EXPECT_EQ(spLong->getLong(), longVal[i]);
        spLong = Util::parseConstant(DT_LONG, std::to_string(longVal[i]));
        EXPECT_EQ(spLong->getLong(), longVal[i]);
        EXPECT_EQ(spLong->isScalar() && spLong->getForm() == DF_SCALAR && spLong->getType() == DT_LONG,true);

        ConstantSP spFloat = Util::createFloat(intVal[i]+0.12345);
        EXPECT_EQ(spFloat->getFloat(), (float)(intVal[i]+0.12345));
        spFloat = Util::parseConstant(DT_FLOAT, std::to_string(intVal[i]+0.12345));
        EXPECT_EQ(spFloat->getFloat(), (float)(intVal[i]+0.12345));
        EXPECT_EQ(spFloat->isScalar() && spFloat->getForm() == DF_SCALAR && spFloat->getType() == DT_FLOAT,true);

        ConstantSP spDouble = Util::createDouble(longVal[i]+0.12345);
        EXPECT_EQ( spDouble->getDouble(), (double)(longVal[i]+0.12345));
        spDouble = Util::parseConstant(DT_DOUBLE, std::to_string(longVal[i]+0.12345));
        EXPECT_EQ(spDouble->getDouble(), (double)(longVal[i]+0.12345));
        EXPECT_EQ(spDouble->isScalar() && spDouble->getForm() == DF_SCALAR && spDouble->getType() == DT_DOUBLE,true);

    }
    ConstantSP sp=Util::parseConstant(DT_FLOAT, "a.1");

    ConstantSP spIP = Util::createConstant(DT_IP);
    unsigned char ip[16] = { 0 };
    for (i = 0; i < 16; i++)
        ip[i] = i;
    spIP->setBinary(0, 16, ip);
    EXPECT_EQ(spIP->getString(), string("f0e:d0c:b0a:908:706:504:302:100"));
    EXPECT_EQ(spIP->isScalar() && spIP->getForm() == DF_SCALAR && spIP->getType() == DT_IP,true);

    spIP = Util::parseConstant(DT_IP, "f0e:d0c:b0a:908:706:504:302:100"); //"192.168.2.1");
    EXPECT_EQ(spIP->isScalar() && spIP->getForm() == DF_SCALAR && spIP->getType() == DT_IP,true);
    unsigned char *bi = (unsigned char*) spIP->getBinary();
    for (i = 0; i < 16; i++){
        EXPECT_EQ(bi[i], ip[i]);
        }
    spIP = Util::parseConstant(DT_IP, "192.168.2.1");
    EXPECT_EQ(spIP->isScalar() && spIP->getForm() == DF_SCALAR && spIP->getType() == DT_IP,true);

    memset(ip, 0, 16);
    ip[0] = 1, ip[1] = 2, ip[2] = 168, ip[3] = 192;
    bi = (unsigned char*) spIP->getBinary();
    for (i = 0; i < 16; i++){
        EXPECT_EQ(bi[i], ip[i]);
        }

    spIP = Util::parseConstant(DT_IP, "90:b0a:908:706:504:302:100");
    EXPECT_EQ(spIP.isNull(),true);
    spIP = Util::parseConstant(DT_IP, "0:hh:b0a:908:706:504:302:100");
    EXPECT_EQ(spIP.isNull(),true);

    //spIP=Util::parseConstant(DT_IP, "::b0a:908:706:504:302:100");
    //assert(spIP.isNull());

    spIP = Util::parseConstant(DT_IP, "h.168.2.1");
    EXPECT_EQ(spIP.isNull(),true);
    spIP = Util::parseConstant(DT_IP, "h.192.168.2.1");
    EXPECT_EQ(spIP.isNull(),true);

    ConstantSP spUuid = Util::createConstant(DT_UUID);
    for (i = 0; i < 16; i++)
        ip[i] = i;
    spUuid->setBinary(0, 16, ip);
    EXPECT_EQ(spUuid->isScalar() && spUuid->getForm() == DF_SCALAR && spUuid->getType() == DT_UUID,true);
    bi = (unsigned char*) spUuid->getBinary();
    for (i = 0; i < 16; i++){
        EXPECT_EQ(bi[i], ip[i]);
        }
    char guid[37];
    for (i = 0; i < 100; i++) {
        random_uuid(guid);
        spUuid = Util::parseConstant(DT_UUID, string(guid));
        EXPECT_EQ(spUuid->isScalar() && spUuid->getForm() == DF_SCALAR && spUuid->getType() == DT_UUID,true);
        EXPECT_EQ(spUuid->getString(), string(guid));
    }
    spUuid = Util::parseConstant(DT_UUID, "5b2de9b4-3471-4e66-a019-d8e3f1222a58");
    EXPECT_EQ(spUuid->getString(), string("5b2de9b4-3471-4e66-a019-d8e3f1222a58"));

    try {
        spUuid = Util::parseConstant(DT_UUID, "hi2de9b4-3471-4e66-a019-d8e3f1222a58");
        fail++;
        cout << "Invalid uuid string ,but passed :" << spUuid->getString() << endl;

        spUuid = Util::parseConstant(DT_UUID, "5a2de9b4-071-4e66-a019-d8e3f1222a58");
        fail++;
        cout << "Invalid UUID string,but passed." << spUuid->getString() << endl;
    } catch (exception &ex) {
        cout << "Failed to  parseConstrant: " << ex.what() << endl;
    }

    ConstantSP spInt128 = Util::createConstant(DT_INT128);
    for (i = 0; i < 16; i++)
        ip[i] = i;
    spInt128->setBinary(0, 16, ip);
    EXPECT_EQ(spInt128->isScalar() && spInt128->getForm() == DF_SCALAR && spInt128->getType() == DT_INT128,true);
    bi = (unsigned char*) spInt128->getBinary();
    for (i = 0; i < 16; i++){
        EXPECT_EQ(bi[i], ip[i]);
        }

    spInt128 = Util::parseConstant(DT_INT128, "34f0302cae07db8201d895e3acc0c703");
    EXPECT_EQ(spInt128->getString(), string("34f0302cae07db8201d895e3acc0c703"));

    try {
        spInt128 = Util::parseConstant(DT_INT128, "hi2de9b4-3471-4e66-a019-d8e3f1222a58");
        if (!spInt128.isNull()) {
            fail++;
            cout << "Invalid int128 string ,but passed :" << spInt128->getString() << endl;
        }

        spUuid = Util::parseConstant(DT_INT128, "5a2de9b4-071-4e66-a019-d8e3f1222a58");
        if (!spInt128.isNull()) {
            fail++;
            cout << "Invalid int128 string ,but passed :" << spInt128->getString() << endl;
        }
    } catch (exception &ex) {
        cout << "Failed to  parseConstrant: " << ex.what() << endl;
    }
}

TEST_F(ScalarTest,testScalarFunction){
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
    char **buf9 = new char*[2];
    unsigned char *buf10 = new unsigned char[2];
    SymbolBase *symbase= new SymbolBase(1);
    void *buf11=NULL;

    cout<<"-----------------void-------------------"<<endl;
    ConstantSP voidval=Util::createConstant(DT_VOID);
    cout<<voidval->isNull(0,1,buf0)<<endl;
    cout<<voidval->isValid(0,1,buf0)<<endl;
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
    cout<<voidval->getAllocatedMemory()<<endl;
    cout<<voidval->getBoolConst(0,1,buf)[0]<<endl;
    cout<<voidval->getCharConst(0,1,buf)[0]<<endl;
    cout<<voidval->getShortConst(0,1,buf1)[0]<<endl; 
    cout<<voidval->getIntConst(0,1,buf2)[0]<<endl;
    cout<<voidval->getLongConst(0,1,buf3)[0]<<endl;
    cout<<voidval->getIndexConst(0,1,buf4)[0]<<endl;
    cout<<voidval->getFloatConst(0,1,buf5)[0]<<endl;
    cout<<voidval->getDoubleConst(0,1,buf6)[0]<<endl;   
    EXPECT_ANY_THROW(voidval->getSymbolConst(0,1,buf2,symbase,false)); 
    cout<<voidval->getStringConst(0,1,buf8)[0]<<endl;
    EXPECT_ANY_THROW(voidval->getStringConst(0,1,buf9));
    EXPECT_ANY_THROW(voidval->getBinaryConst(0,1,1,buf10));
    cout<<"-----------------string-------------------"<<endl;
    ConstantSP stringval=Util::createString("this is a string scalar");
    char *buf_str0 = new char[2];
    int numElement;
    int partial;
    cout<<stringval->serialize(buf_str0,5,0,2,numElement,partial)<<endl;
    cout<<stringval->serialize(buf_str0,5,0,1,numElement,partial)<<endl;
    cout<<stringval->serialize(buf_str0,100,0,1,numElement,partial)<<endl;
    ConstantSP blobval=Util::createBlob("this is a string scalar");
    char *buf_str1 = new char[2];
    cout<<blobval->serialize(buf_str1,1,0,-2,numElement,partial)<<endl;
    cout<<blobval->serialize(buf_str1,5,0,0,numElement,partial)<<endl;
    cout<<blobval->serialize(buf_str1,5,0,1,numElement,partial)<<endl;
    cout<<"-----------------int128-------------------"<<endl;
    ConstantSP int128val=Util::createConstant(DT_INT128);
    char *buf_int1280 = new char[2];
    int numElement_1;
    int partial_1;
    cout<<int128val->serialize(buf_int1280,5,0,2,numElement_1,partial_1);
    cout<<int128val->serialize(buf_int1280,0,0,1,numElement_1,partial_1)<<endl;

    cout<<"-----------------float-------------------"<<endl;
    ConstantSP floatval=Util::createFloat(1);
    EXPECT_TRUE(floatval->getBool(0,1,buf00));
    EXPECT_TRUE(floatval->getChar(0,1,buf));
    EXPECT_TRUE(floatval->getShort(0,1,buf1)); 
    EXPECT_TRUE(floatval->getInt(0,1,buf2));
    EXPECT_TRUE(floatval->getLong(0,1,buf3));
    EXPECT_TRUE(floatval->getIndex(0,1,buf4));
    EXPECT_TRUE(floatval->getFloat(0,1,buf5));
    EXPECT_TRUE(floatval->getDouble(0,1,buf6));   
    EXPECT_FALSE(floatval->getSymbol(0,1,buf2,symbase,false)); 
    EXPECT_FALSE(floatval->getString(0,1,buf8));
    EXPECT_FALSE(floatval->getString(0,1,buf9));
    EXPECT_FALSE(floatval->getBinary(0,1,1,buf10));
    EXPECT_FALSE(floatval->getHash(0,1,buckets,buf2));
    EXPECT_TRUE(floatval->getBoolConst(0,1,buf));
    EXPECT_TRUE(floatval->getCharConst(0,1,buf));
    EXPECT_TRUE(floatval->getShortConst(0,1,buf1)); 
    EXPECT_TRUE(floatval->getIntConst(0,1,buf2));
    EXPECT_TRUE(floatval->getLongConst(0,1,buf3));
    EXPECT_TRUE(floatval->getIndexConst(0,1,buf4));
    EXPECT_TRUE(floatval->getFloatConst(0,1,buf5));
    EXPECT_TRUE(floatval->getDoubleConst(0,1,buf6));   
    EXPECT_ANY_THROW(floatval->getSymbolConst(0,1,buf2,symbase,false)[0]); 
    EXPECT_ANY_THROW(floatval->getStringConst(0,1,buf8)[0]);
    EXPECT_ANY_THROW(floatval->getStringConst(0,1,buf9)[0]);
    EXPECT_ANY_THROW(floatval->getBinaryConst(0,1,1,buf10)[0]);

    cout<<"-----------------double-------------------"<<endl;
    ConstantSP doubleval=Util::createDouble(1);
    EXPECT_TRUE(doubleval->getBool(0,1,buf00));
    EXPECT_TRUE(doubleval->getChar(0,1,buf));
    EXPECT_TRUE(doubleval->getShort(0,1,buf1)); 
    EXPECT_TRUE(doubleval->getInt(0,1,buf2));
    EXPECT_TRUE(doubleval->getLong(0,1,buf3));
    EXPECT_TRUE(doubleval->getIndex(0,1,buf4));
    EXPECT_TRUE(doubleval->getFloat(0,1,buf5));
    EXPECT_TRUE(doubleval->getDouble(0,1,buf6));   
    EXPECT_FALSE(doubleval->getSymbol(0,1,buf2,symbase,false)); 
    EXPECT_FALSE(doubleval->getString(0,1,buf8));
    EXPECT_FALSE(doubleval->getString(0,1,buf9));
    EXPECT_FALSE(doubleval->getBinary(0,1,1,buf10));
    EXPECT_FALSE(doubleval->getHash(0,1,buckets,buf2));
    EXPECT_TRUE(doubleval->getBoolConst(0,1,buf));
    EXPECT_TRUE(doubleval->getCharConst(0,1,buf));
    EXPECT_TRUE(doubleval->getShortConst(0,1,buf1)); 
    EXPECT_TRUE(doubleval->getIntConst(0,1,buf2));
    EXPECT_TRUE(doubleval->getLongConst(0,1,buf3));
    EXPECT_TRUE(doubleval->getIndexConst(0,1,buf4));
    EXPECT_TRUE(doubleval->getFloatConst(0,1,buf5));
    EXPECT_TRUE(doubleval->getDoubleConst(0,1,buf6));   
    EXPECT_ANY_THROW(doubleval->getSymbolConst(0,1,buf2,symbase,false)[0]); 
    EXPECT_ANY_THROW(doubleval->getStringConst(0,1,buf8)[0]);
    EXPECT_ANY_THROW(doubleval->getStringConst(0,1,buf9)[0]);
    EXPECT_ANY_THROW(doubleval->getBinaryConst(0,1,1,buf10)[0]);
}

TEST_F(ScalarTest,testFunctionCastTemporal){
    ConstantSP dateval = Util::createDate(1);
    ConstantSP monthval = Util::createMonth(1);
    ConstantSP timeval = Util::createTime(1);
    ConstantSP minuteval = Util::createMinute(1);
    ConstantSP secondval = Util::createSecond(1);
    ConstantSP datetimeval = Util::createDateTime(1);
    ConstantSP timestampval = Util::createTimestamp(1000000);
    ConstantSP nanotimeval = Util::createNanoTime(1000000);
    ConstantSP nanotimestampval = Util::createNanoTimestamp(1000000);
    ConstantSP datehourval = Util::createDateHour(1);

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
}