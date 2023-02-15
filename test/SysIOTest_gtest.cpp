class SysIOTest:public testing::Test
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

class SysIOTest_Parameterized: public::testing::TestWithParam<DATA_TYPE>{

};

TEST_F(SysIOTest, test_Socket_No_Parameter){
    Socket s1 = Socket();
    EXPECT_TRUE(s1.isValid());
    s1.connect(hostName, port, false, 7200);
    EXPECT_EQ(s1.getHost(),hostName);
    EXPECT_EQ(s1.getPort(),port);
    Socket s2 = Socket(s1.getHandle(), true, 7200);

    char *val = (char *)"ccc579";
    size_t actualLength = 1;

    for(auto i=0;i<10;i++){
        IO_ERR res = s1.write(val,10, actualLength);
        EXPECT_EQ(res, OK);
    }
    EXPECT_EQ(s1.read(val,10,actualLength), NODATA); // when send tcp-data to connected socket(host:port), it can read datas;
    EXPECT_EQ(s2.read(val,10,actualLength), NODATA);

    s1.enableTcpNoDelay(true);
    EXPECT_TRUE(s1.ENABLE_TCP_NODELAY);

    EXPECT_TRUE(s1.skipAll());
    EXPECT_TRUE(s2.skipAll());

    s1.close();
    s2.close();
    EXPECT_EQ(s1.write(val,7, actualLength),DISCONNECTED);
    EXPECT_FALSE(s1.isValid());
}

// TEST_F(SysIOTest, test_Socket_with_Parameter){
//     Socket s1 = Socket(hostName, port, true, 7200);
//     EXPECT_TRUE(s1.isValid());
//     EXPECT_EQ(s1.getHost(), hostName);
//     EXPECT_EQ(s1.getPort(), port);
//     cout<<"now get socket handle: "<<s1.getHandle()<<endl;

//     char *val = (char *)"ccc579";
//     size_t actualLength = 1;
//     for(auto i=0;i<10;i++){
//         IO_ERR res = s1.write(val,10, actualLength);
//         cout<<res<<endl;
//         // EXPECT_EQ(res, OK);
//         Sleep(100);
//     }
//     EXPECT_EQ(s1.read(val,10,actualLength), NODATA);

//     s1.enableTcpNoDelay(true);
//     EXPECT_TRUE(s1.ENABLE_TCP_NODELAY);

//     EXPECT_TRUE(s1.skipAll());

//     s1.close();
//     EXPECT_EQ(s1.write(val,7, actualLength),OTHERERR);
//     EXPECT_FALSE(s1.isValid());
// }


// TEST_F(SysIOTest, test_UdpSocket){
//     srand((int)time(NULL));
//     int port1 = 13900 + rand()%100;
//     int port2 = 13900 + rand()%100;

//     UdpSocket s1 = UdpSocket(port1);
//     UdpSocket s2 = UdpSocket(hostName, port2);
//     s1.bind();
//     s2.bind();

//     char *buf = (char *)"dolphindb123";
//     char *recvbuf = new char[10];
//     size_t actualLength = 1;
//     cout<<s1.send(buf, 1)<<endl;
//     cout<<s1.recv(recvbuf, 10, actualLength);
    

//     // EXPECT_EQ(s1.getPort(), port1);
//     // EXPECT_EQ(s2.getPort(), port2);

//     delete[] buf, recvbuf;

// }

TEST_F(SysIOTest, test_DataStream_scalar_int)
{
    srand(time(NULL));
    int test_val = (int)rand()%INT_MAX;
    { // DDB scalar
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createInt(test_val);
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_SCALAR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getInt(), test_val);

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

    { // c++ scalar
        char *pOutbuf;
        int outSize;
        DataOutputStreamSP outStream = new DataOutputStream();

        outStream->write(test_val);
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        int readValue;
        inputStream->readInt(readValue);
        EXPECT_EQ(readValue, test_val);
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_scalar_intNull)
{
    { // DDB scalar
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createNullConstant(DT_INT);
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_SCALAR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getInt(), object->getInt());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

    { // c++ scalar
        char *pOutbuf;
        int outSize;
        DataOutputStreamSP outStream = new DataOutputStream();

        outStream->write(int(NULL));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        int readValue;
        inputStream->readInt(readValue);
        EXPECT_EQ(readValue, int(NULL));
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}

TEST_F(SysIOTest, test_DataStream_scalar_bool)
{
    srand(time(NULL));
    char test_val = rand() % 2;
    { // DDB scalar
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createBool(test_val);
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_SCALAR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getInt(), test_val);

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

    { // c++ scalar
        char *pOutbuf;
        int outSize;
        DataOutputStreamSP outStream = new DataOutputStream();

        outStream->write(test_val);
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        bool readValue;
        inputStream->readBool(readValue);
        EXPECT_EQ(readValue, test_val);
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_scalar_boolNull)
{
    { // DDB scalar
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createNullConstant(DT_BOOL);
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_SCALAR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getBool(), object->getBool());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

    { // c++ scalar
        char *pOutbuf;
        int outSize;
        DataOutputStreamSP outStream = new DataOutputStream();

        outStream->write(bool(NULL));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        bool readValue;
        inputStream->readBool(readValue);
        EXPECT_EQ(readValue, bool(NULL));
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}

TEST_F(SysIOTest, test_DataStream_scalar_INDEX)
{
    srand(time(NULL));
    INDEX test_val = (INDEX)rand() % INDEX_MAX;
    { // DDB scalar
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createInt(test_val);
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_SCALAR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getIndex(), test_val);

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

    { // c++ scalar
        char *pOutbuf;
        int outSize;
        DataOutputStreamSP outStream = new DataOutputStream();

        outStream->write(test_val);
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        INDEX readValue;
        inputStream->readIndex(readValue);
        EXPECT_EQ(readValue, test_val);
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}

TEST_F(SysIOTest, test_DataStream_scalar_float)
{
    srand(time(NULL));
    float test_val = rand()/float(RAND_MAX);
    { // DDB scalar
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createFloat(test_val);
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_SCALAR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getFloat(), test_val);

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

    { // c++ scalar
        char *pOutbuf;
        int outSize;
        DataOutputStreamSP outStream = new DataOutputStream();

        outStream->write(test_val);
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        float readValue;
        inputStream->readFloat(readValue);
        EXPECT_EQ(readValue, test_val);

        DataStreamSP inputStream1 = new DataStream(pOutbuf, outSize);
        inputStream1->enableReverseIntegerByteOrder(); // reverseOrder == true
        float readValue1;
        inputStream1->readFloat(readValue1);
        cout<<"result: "<<readValue1<<endl;

        inputStream->close();
        inputStream1->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_scalar_floatNull)
{
    { // DDB scalar
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createNullConstant(DT_FLOAT);
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_SCALAR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getFloat(), object->getFloat());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

    { // c++ scalar
        char *pOutbuf;
        int outSize;
        DataOutputStreamSP outStream = new DataOutputStream();

        outStream->write(float(NULL));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        float readValue;
        inputStream->readFloat(readValue);
        EXPECT_EQ(readValue, float(NULL));

        DataStreamSP inputStream1 = new DataStream(pOutbuf, outSize);
        inputStream1->enableReverseIntegerByteOrder(); // reverseOrder == true
        float readValue1;
        inputStream1->readFloat(readValue1);
        cout<<"result: "<<readValue1<<endl;

        inputStream->close();
        inputStream1->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}

TEST_F(SysIOTest, test_DataStream_scalar_string)
{
    srand(time(NULL));
    vector<string> rand_str = {"sd","dag","xxx","智臾科技a","23!@#$%","^&#%……@","/,m[[`"};
    string test_val = rand_str[rand() % rand_str.size()];
    { // DDB scalar
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createString(test_val);
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_SCALAR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), test_val);

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

    { // c++ scalar
        char *pOutbuf;
        int outSize;
        DataOutputStreamSP outStream = new DataOutputStream();
        outStream->write(test_val);
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        string readValue;
        inputStream->readString(readValue);
        EXPECT_EQ(inputStream->readString(readValue, 1000), END_OF_STREAM);
        EXPECT_EQ(readValue, test_val);
        EXPECT_TRUE(inputStream->isReadable());
        inputStream->isReadable(0);
        EXPECT_FALSE(inputStream->isReadable());
        EXPECT_TRUE(inputStream->isArrayStream());
        EXPECT_FALSE(inputStream->isWritable());
        inputStream->isWritable(1);
        EXPECT_TRUE(inputStream->isWritable());
        EXPECT_FALSE(inputStream->isFileStream());
        EXPECT_FALSE(inputStream->isSocketStream());

        inputStream->clearReadBuffer();
        EXPECT_EQ(inputStream->getDataSizeInArray(),0);

        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

    { // c++ scalar
        char *pOutbuf;
        int outSize;
        char buf[] = "aadddccc";
        DataOutputStreamSP outStream = new DataOutputStream();
        outStream->write(buf, 9);
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        string readValue;
        inputStream->readString(readValue);
        EXPECT_EQ(inputStream->readString(readValue, 1000), END_OF_STREAM);
        EXPECT_EQ(readValue, buf);
        EXPECT_TRUE(inputStream->isReadable());
        inputStream->isReadable(0);
        EXPECT_FALSE(inputStream->isReadable());
        EXPECT_TRUE(inputStream->isArrayStream());
        EXPECT_FALSE(inputStream->isWritable());
        inputStream->isWritable(1);
        EXPECT_TRUE(inputStream->isWritable());
        EXPECT_FALSE(inputStream->isFileStream());
        EXPECT_FALSE(inputStream->isSocketStream());

        inputStream->clearReadBuffer();
        EXPECT_EQ(inputStream->getDataSizeInArray(),0);

        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

    { // c++ scalar
        char *pOutbuf;
        int outSize;
        char buf[] = "aadddccc";
        DataOutputStreamSP outStream = new DataOutputStream();
        size_t actwrite;
        outStream->write(buf, 9, actwrite);
        EXPECT_EQ(actwrite, 9);
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        string readValue;
        inputStream->readString(readValue);
        EXPECT_EQ(inputStream->readString(readValue, 1000), END_OF_STREAM);
        EXPECT_EQ(readValue, buf);
        EXPECT_TRUE(inputStream->isReadable());
        inputStream->isReadable(0);
        EXPECT_FALSE(inputStream->isReadable());
        EXPECT_TRUE(inputStream->isArrayStream());
        EXPECT_FALSE(inputStream->isWritable());
        inputStream->isWritable(1);
        EXPECT_TRUE(inputStream->isWritable());
        EXPECT_FALSE(inputStream->isFileStream());
        EXPECT_FALSE(inputStream->isSocketStream());

        inputStream->clearReadBuffer();
        EXPECT_EQ(inputStream->getDataSizeInArray(),0);

        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }
}


TEST_F(SysIOTest, test_DataStream_scalar_stringNull)
{
    { // DDB scalar
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createNullConstant(DT_STRING);
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_SCALAR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

    { // c++ scalar
        char *pOutbuf;
        int outSize;
        DataOutputStreamSP outStream = new DataOutputStream();
        outStream->write(string(""));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        string readValue;
        inputStream->readString(readValue);
        EXPECT_EQ(inputStream->readString(readValue, 1000), END_OF_STREAM);
        EXPECT_EQ(readValue, string(""));
        EXPECT_TRUE(inputStream->isReadable());
        inputStream->isReadable(0);
        EXPECT_FALSE(inputStream->isReadable());
        EXPECT_TRUE(inputStream->isArrayStream());
        EXPECT_FALSE(inputStream->isWritable());
        inputStream->isWritable(1);
        EXPECT_TRUE(inputStream->isWritable());
        EXPECT_FALSE(inputStream->isFileStream());
        EXPECT_FALSE(inputStream->isSocketStream());

        inputStream->clearReadBuffer();
        EXPECT_EQ(inputStream->getDataSizeInArray(),0);

        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

    { // c++ scalar
        char *pOutbuf;
        int outSize;
        char buf[] = "";
        DataOutputStreamSP outStream = new DataOutputStream();
        outStream->write(buf, 1);
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        string readValue;
        inputStream->readString(readValue);
        EXPECT_EQ(inputStream->readString(readValue, 1), END_OF_STREAM);
        EXPECT_EQ(readValue, buf);

        cout<<inputStream->clearReadBuffer()<<endl;
        EXPECT_EQ(inputStream->getDataSizeInArray(), 0);

        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

    { // c++ scalar
        char *pOutbuf;
        int outSize;
        char buf[] = "";
        DataOutputStreamSP outStream = new DataOutputStream();
        size_t actwrite;
        outStream->write(buf, 1, actwrite);
        EXPECT_EQ(actwrite, 1);
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        string readValue;
        inputStream->readString(readValue);
        EXPECT_EQ(inputStream->readString(readValue, 1), END_OF_STREAM);
        EXPECT_EQ(readValue, buf);
        EXPECT_TRUE(inputStream->isReadable());
        inputStream->isReadable(0);
        EXPECT_FALSE(inputStream->isReadable());
        EXPECT_TRUE(inputStream->isArrayStream());
        EXPECT_FALSE(inputStream->isWritable());
        inputStream->isWritable(1);
        EXPECT_TRUE(inputStream->isWritable());
        EXPECT_FALSE(inputStream->isFileStream());
        EXPECT_FALSE(inputStream->isSocketStream());

        inputStream->clearReadBuffer();
        EXPECT_EQ(inputStream->getDataSizeInArray(), 0);

        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }
}

TEST_F(SysIOTest, test_DataStream_scalar_date)
{
    srand(time(NULL));
    int test_val = rand()%INT_MAX;
    { // DDB scalar
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createDate(test_val);
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_SCALAR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}

TEST_F(SysIOTest, test_DataStream_scalar_month)
{
    srand(time(NULL));
    int test_val = rand()%INT_MAX;
    { // DDB scalar
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createMonth(test_val);
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_SCALAR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_scalar_time)
{
    srand(time(NULL));
    int test_val = rand()%INT_MAX;
    { // DDB scalar
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createTime(test_val);
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_SCALAR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_scalar_minute)
{
    srand(time(NULL));
    int test_val = rand()%INT_MAX;
    { // DDB scalar
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createMinute(test_val);
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_SCALAR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_scalar_second)
{
    srand(time(NULL));
    int test_val = rand()%INT_MAX;
    { // DDB scalar
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createSecond(test_val);
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_SCALAR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_scalar_datetime)
{
    srand(time(NULL));
    int test_val = rand()%INT_MAX;
    { // DDB scalar
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createDateTime(test_val);
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_SCALAR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_scalar_timestamp)
{
    srand(time(NULL));
    long long test_val = rand()%LLONG_MAX;
    { // DDB scalar
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createTimestamp(test_val);
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_SCALAR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_scalar_nanotime)
{
    srand(time(NULL));
    long long test_val = rand()%LLONG_MAX;
    { // DDB scalar
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createNanoTime(test_val);
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_SCALAR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_scalar_nanotimestamp)
{
    srand(time(NULL));
    long long test_val = rand()%LLONG_MAX;
    { // DDB scalar
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createNanoTimestamp(test_val);
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_SCALAR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}

TEST_F(SysIOTest, test_DataStream_scalar_int128)
{
    srand(time(NULL));
    unsigned char int128[16];
    for (auto i = 0; i < 16; i++)
        int128[i] = rand() % CHAR_MAX;

    { // DDB scalar
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createConstant(DT_INT128);
        object->setBinary(int128, sizeof(Guid));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_SCALAR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}

TEST_F(SysIOTest, test_DataStream_scalar_ip)
{
    srand(time(NULL));
    unsigned char ip[16];
    for (auto i = 0; i < 16; i++)
        ip[i] = rand() % CHAR_MAX;
    string test_val = Guid(ip).getString();

    { // DDB scalar
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createConstant(DT_IP);
        object->setBinary(ip, sizeof(Guid));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_SCALAR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}

TEST_F(SysIOTest, test_DataStream_scalar_uuid)
{
    srand(time(NULL));
    unsigned char uuid[16];
    for (auto i = 0; i < 16; i++)
        uuid[i] = rand() % CHAR_MAX;

    { // DDB scalar
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createConstant(DT_UUID);
        object->setBinary(uuid, sizeof(Guid));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_SCALAR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}

INSTANTIATE_TEST_CASE_P(test_DataStream_scalar_Temporal_Null, SysIOTest_Parameterized, testing::Values(DT_DATE,DT_MONTH,DT_TIME,DT_MINUTE,DT_SECOND,DT_DATETIME,DT_TIMESTAMP,DT_NANOTIME,DT_NANOTIMESTAMP,DT_DATEHOUR));
TEST_P(SysIOTest_Parameterized, test_DataStream_scalar_Temporal_Null){
    DATA_TYPE test_type =  GetParam();
    { // DDB scalar
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createNullConstant(test_type);
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_SCALAR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }
}


INSTANTIATE_TEST_CASE_P(test_DataStream_scalar_Integral_Null, SysIOTest_Parameterized, testing::Values(DT_UUID, DT_IP, DT_INT128));
TEST_P(SysIOTest_Parameterized, test_DataStream_scalar_Integral_Null){
    DATA_TYPE test_type =  GetParam();
    { // DDB scalar
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createNullConstant(test_type);
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_SCALAR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }
}


TEST_F(SysIOTest, test_DataStream_vector_int)
{
    srand(time(NULL));
    int test_val = (int)rand()%INT_MAX;
    { // DDB vector
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createVector(DT_INT,1,1);
        object->set(0,Util::createInt(test_val));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_VECTOR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}

TEST_F(SysIOTest, test_DataStream_vector_bool)
{
    srand(time(NULL));
    char test_val = rand() % 2;
    { // DDB vector
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createVector(DT_BOOL,1,1);
        object->set(0,Util::createBool(test_val));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_VECTOR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}

TEST_F(SysIOTest, test_DataStream_vector_INDEX)
{
    srand(time(NULL));
    INDEX test_val = (INDEX)rand() % INDEX_MAX;
    { // DDB vector
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createVector(DT_INDEX,1,1);
        object->set(0,Util::createInt(test_val));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_VECTOR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}

TEST_F(SysIOTest, test_DataStream_vector_float)
{
    srand(time(NULL));
    float test_val = rand()/float(RAND_MAX);
    { // DDB vector
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createVector(DT_FLOAT,1,1);
        object->set(0,Util::createFloat(test_val));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_VECTOR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}

TEST_F(SysIOTest, test_DataStream_vector_string)
{
    vector<string> rand_str = {"sd","dag","xxx","智臾科技a","23!@#$%","^&#%……@","/,m[[`"};
    string test_val = rand_str[rand() % rand_str.size()];
    { // DDB vector
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createVector(DT_STRING,1,1);
        object->set(0,Util::createString(test_val));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_VECTOR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}

TEST_F(SysIOTest, test_DataStream_vector_date)
{
    srand(time(NULL));
    int test_val = rand()%INT_MAX;
    { // DDB vector
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createVector(DT_DATE,1,1);
        object->set(0,Util::createDate(test_val));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_VECTOR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}

TEST_F(SysIOTest, test_DataStream_vector_month)
{
    srand(time(NULL));
    int test_val = rand()%INT_MAX;
    { // DDB vector
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createVector(DT_MONTH,1,1);
        object->set(0,Util::createMonth(test_val));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_VECTOR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_vector_time)
{
    srand(time(NULL));
    int test_val = rand()%INT_MAX;
    { // DDB vector
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createVector(DT_TIME,1,1);
        object->set(0,Util::createTime(test_val));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_VECTOR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_vector_minute)
{
    srand(time(NULL));
    int test_val = rand()%INT_MAX;
    { // DDB vector
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createVector(DT_MINUTE,1,1);
        object->set(0,Util::createMinute(test_val));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_VECTOR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_vector_second)
{
    srand(time(NULL));
    int test_val = rand()%INT_MAX;
    { // DDB vector
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createVector(DT_SECOND,1,1);
        object->set(0,Util::createSecond(test_val));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_VECTOR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_vector_datetime)
{
    srand(time(NULL));
    int test_val = rand()%INT_MAX;
    { // DDB vector
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createVector(DT_DATETIME,1,1);
        object->set(0,Util::createDateTime(test_val));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_VECTOR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_vector_timestamp)
{
    srand(time(NULL));
    long long test_val = rand()%LLONG_MAX;
    { // DDB vector
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createVector(DT_TIMESTAMP,1,1);
        object->set(0,Util::createTimestamp(test_val));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_VECTOR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_vector_nanotime)
{
    srand(time(NULL));
    long long test_val = rand()%LLONG_MAX;
    { // DDB vector
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createVector(DT_NANOTIME,1,1);
        object->set(0,Util::createNanoTime(test_val));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_VECTOR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_vector_nanotimestamp)
{
    srand(time(NULL));
    long long test_val = rand()%LLONG_MAX;
    { // DDB vector
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createVector(DT_NANOTIMESTAMP,1,1);
        object->set(0,Util::createNanoTimestamp(test_val));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_VECTOR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_vector_uuid)
{
    srand(time(NULL));
    unsigned char uuid[16];
    for (auto i = 0; i < 16; i++)
        uuid[i] = rand() % CHAR_MAX;
    DdbVector<Guid> test_val(0, 1);
    test_val.add(uuid);

    { // DDB vector
        char *pOutbuf;
        int outSize;
        ConstantSP object = test_val.createVector(DT_UUID);
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_VECTOR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_vector_symbol)
{
    vector<string> rand_str = {"sd","dag","xxx","智臾科技a","23!@#$%","^&#%……@","/,m[[`"};
    string test_val = rand_str[rand() % rand_str.size()];
    { // DDB vector
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createVector(DT_SYMBOL,1,1);
        object->set(0,Util::createString(test_val));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_VECTOR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}

TEST_F(SysIOTest, test_DataStream_vector_ip)
{
    srand(time(NULL));
    unsigned char ip[16];
    for (auto i = 0; i < 16; i++)
        ip[i] = rand() % CHAR_MAX;
    DdbVector<Guid> test_val(0, 1);
    test_val.add(ip);

    { // DDB vector
        char *pOutbuf;
        int outSize;
        ConstantSP object = test_val.createVector(DT_IP);
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_VECTOR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_vector_int128)
{
    srand(time(NULL));
    unsigned char int128[16];
    for (auto i = 0; i < 16; i++)
        int128[i] = rand() % CHAR_MAX;
    DdbVector<Guid> test_val(0, 1);
    test_val.add(int128);

    { // DDB vector
        char *pOutbuf;
        int outSize;
        ConstantSP object = test_val.createVector(DT_INT128);
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_VECTOR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


INSTANTIATE_TEST_CASE_P(test_DataStream_vector_Null, SysIOTest_Parameterized, testing::Values(DT_BOOL,DT_CHAR,DT_SHORT,DT_INT,DT_LONG,DT_DATE,DT_MONTH,DT_TIME,DT_MINUTE,DT_SECOND,DT_DATETIME,DT_TIMESTAMP,DT_NANOTIME,DT_NANOTIMESTAMP,
    DT_FLOAT,DT_DOUBLE,DT_SYMBOL,DT_STRING,DT_UUID,DT_DATEHOUR,
    DT_IP,DT_INT128,DT_BLOB, DT_DECIMAL32, DT_DECIMAL64));
TEST_P(SysIOTest_Parameterized, test_DataStream_vector_Null){
    DATA_TYPE test_type =  GetParam();
    { // DDB vector
        char *pOutbuf;
        int outSize;
        ConstantSP object = Util::createVector(test_type, 1);
        object->setNull(0);
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_VECTOR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }
}

TEST_F(SysIOTest, test_DataStream_Vector_withAllType)
{
    srand(time(NULL));
    unsigned char uuid[16];
    for (auto i = 0; i < 16; i++)
        uuid[i] = rand() % CHAR_MAX;
    string uuidval = Guid(uuid).getString();
    char boolval = rand() % 2;
    char charval = rand() % CHAR_MAX;
    short shortval = rand() % SHRT_MAX;
    int intval = rand() % INT_MAX;
    long long longval = rand() % LLONG_MAX;
    float floatval = rand()/float(RAND_MAX);
    double doubleval = rand()/double(RAND_MAX);
    string strval = "dolphindb";

    ConstantSP object = Util::createVector(DT_ANY,19,19);
    object->set(0,Util::createBool(boolval));
    object->set(1,Util::createChar(charval));
    object->set(2,Util::createShort(shortval));
    object->set(3,Util::createInt(intval));
    object->set(4,Util::createLong(longval));
    object->set(5,Util::createFloat(floatval));
    object->set(6,Util::createDouble(doubleval));
    object->set(7,Util::createDate(intval));
    object->set(8,Util::createMonth(intval));
    object->set(9,Util::createTime(intval));
    object->set(10,Util::createMinute(intval));
    object->set(11,Util::createSecond(intval));
    object->set(12,Util::createDateTime(intval));
    object->set(13,Util::createTimestamp(longval));
    object->set(14,Util::createNanoTime(longval));
    object->set(15,Util::createNanoTimestamp(longval));
    object->set(16,Util::createString(strval));
    object->set(17,Util::createBlob(strval));
    object->set(18,Util::parseConstant(DT_UUID, uuidval));

    { // DDB vector
        char *pOutbuf;
        int outSize;

        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_VECTOR);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_table_int)
{
    srand(time(NULL));
    int test_val = (int)rand()%INT_MAX;
    { // DDB table
        char *pOutbuf;
        int outSize;
        vector<string> cols = {"col1", "col2"};
        vector<DATA_TYPE> colTypes = {DT_INT, DT_INT};
        ConstantSP object = Util::createTable(cols, colTypes, 1, 1);
        object->set(0, 0, Util::createInt(test_val));
        object->set(1, 0, Util::createInt(test_val+1));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_TABLE);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}

TEST_F(SysIOTest, test_DataStream_table_bool)
{
    srand(time(NULL));
    char test_val = rand() % 2;
    { // DDB table
        char *pOutbuf;
        int outSize;
        vector<string> cols = {"col1", "col2"};
        vector<DATA_TYPE> colTypes = {DT_BOOL, DT_BOOL};
        ConstantSP object = Util::createTable(cols, colTypes, 1, 1);
        object->set(0, 0, Util::createBool(test_val));
        object->set(1, 0, Util::createBool(test_val+1));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_TABLE);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}

TEST_F(SysIOTest, test_DataStream_table_INDEX)
{
    srand(time(NULL));
    INDEX test_val = (INDEX)rand() % INDEX_MAX;
    { // DDB table
        char *pOutbuf;
        int outSize;
        vector<string> cols = {"col1", "col2"};
        vector<DATA_TYPE> colTypes = {DT_INDEX, DT_INDEX};
        ConstantSP object = Util::createTable(cols, colTypes, 1, 1);
        object->set(0, 0, Util::createInt(test_val));
        object->set(1, 0, Util::createInt(test_val+1));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_TABLE);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}

TEST_F(SysIOTest, test_DataStream_table_float)
{
    srand(time(NULL));
    float test_val = rand()/float(RAND_MAX);
    { // DDB table
        char *pOutbuf;
        int outSize;
        vector<string> cols = {"col1", "col2"};
        vector<DATA_TYPE> colTypes = {DT_FLOAT, DT_FLOAT};
        ConstantSP object = Util::createTable(cols, colTypes, 1, 1);
        object->set(0, 0, Util::createFloat(test_val+2.333));
        object->set(1, 0, Util::createFloat(test_val+1.231));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_TABLE);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}

TEST_F(SysIOTest, test_DataStream_table_string)
{
    vector<string> rand_str = {"sd","dag","xxx","智臾科技a","23!@#$%","^&#%……@","/,m[[`"};
    string test_val = rand_str[rand() % rand_str.size()];
    { // DDB table
        char *pOutbuf;
        int outSize;
        vector<string> cols = {"col1", "col2"};
        vector<DATA_TYPE> colTypes = {DT_STRING, DT_STRING};
        ConstantSP object = Util::createTable(cols, colTypes, 1, 1);
        object->set(0, 0, Util::createString(test_val));
        object->set(1, 0, Util::createString(""));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_TABLE);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}

TEST_F(SysIOTest, test_DataStream_table_date)
{
    srand(time(NULL));
    int test_val = rand()%INT_MAX;
    { // DDB table
        char *pOutbuf;
        int outSize;
        vector<string> cols = {"col1", "col2"};
        vector<DATA_TYPE> colTypes = {DT_DATE, DT_DATE};
        ConstantSP object = Util::createTable(cols, colTypes, 1, 1);
        object->set(0, 0, Util::createDate(test_val));
        object->set(1, 0, Util::createDate(0));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_TABLE);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}

TEST_F(SysIOTest, test_DataStream_table_month)
{
    srand(time(NULL));
    int test_val = rand()%INT_MAX;
    { // DDB table
        char *pOutbuf;
        int outSize;
        vector<string> cols = {"col1", "col2"};
        vector<DATA_TYPE> colTypes = {DT_MONTH, DT_MONTH};
        ConstantSP object = Util::createTable(cols, colTypes, 1, 1);
        object->set(0, 0, Util::createMonth(test_val));
        object->set(1, 0, Util::createMonth(0));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_TABLE);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_table_time)
{
    srand(time(NULL));
    int test_val = rand()%INT_MAX;
    { // DDB table
        char *pOutbuf;
        int outSize;
        vector<string> cols = {"col1", "col2"};
        vector<DATA_TYPE> colTypes = {DT_TIME, DT_TIME};
        ConstantSP object = Util::createTable(cols, colTypes, 1, 1);
        object->set(0, 0, Util::createTime(test_val));
        object->set(1, 0, Util::createTime(0));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_TABLE);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_table_minute)
{
    srand(time(NULL));
    int test_val = rand()%INT_MAX;
    { // DDB table
        char *pOutbuf;
        int outSize;
        vector<string> cols = {"col1", "col2"};
        vector<DATA_TYPE> colTypes = {DT_MINUTE, DT_MINUTE};
        ConstantSP object = Util::createTable(cols, colTypes, 1, 1);
        object->set(0, 0, Util::createMinute(test_val));
        object->set(1, 0, Util::createMinute(0));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_TABLE);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_table_second)
{
    srand(time(NULL));
    int test_val = rand()%INT_MAX;
    { // DDB table
        char *pOutbuf;
        int outSize;
        vector<string> cols = {"col1", "col2"};
        vector<DATA_TYPE> colTypes = {DT_SECOND, DT_SECOND};
        ConstantSP object = Util::createTable(cols, colTypes, 1, 1);
        object->set(0, 0, Util::createSecond(test_val));
        object->set(1, 0, Util::createSecond(0));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_TABLE);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_table_datetime)
{
    srand(time(NULL));
    int test_val = rand()%INT_MAX;
    { // DDB table
        char *pOutbuf;
        int outSize;
        vector<string> cols = {"col1", "col2"};
        vector<DATA_TYPE> colTypes = {DT_DATETIME, DT_DATETIME};
        ConstantSP object = Util::createTable(cols, colTypes, 1, 1);
        object->set(0, 0, Util::createDateTime(test_val));
        object->set(1, 0, Util::createDateTime(0));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_TABLE);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_table_timestamp)
{
    srand(time(NULL));
    long long test_val = rand()%LLONG_MAX;
    { // DDB table
        char *pOutbuf;
        int outSize;
        vector<string> cols = {"col1", "col2"};
        vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_TIMESTAMP};
        ConstantSP object = Util::createTable(cols, colTypes, 1, 1);
        object->set(0, 0, Util::createTimestamp(test_val));
        object->set(1, 0, Util::createTimestamp(0));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_TABLE);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_table_nanotime)
{
    srand(time(NULL));
    long long test_val = rand()%LLONG_MAX;
    { // DDB table
        char *pOutbuf;
        int outSize;
        vector<string> cols = {"col1", "col2"};
        vector<DATA_TYPE> colTypes = {DT_NANOTIME, DT_NANOTIME};
        ConstantSP object = Util::createTable(cols, colTypes, 1, 1);
        object->set(0, 0, Util::createNanoTime(test_val));
        object->set(1, 0, Util::createNanoTime(0));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_TABLE);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_table_nanotimestamp)
{
    srand(time(NULL));
    long long test_val = rand()%LLONG_MAX;
    { // DDB table
        char *pOutbuf;
        int outSize;
        vector<string> cols = {"col1", "col2"};
        vector<DATA_TYPE> colTypes = {DT_NANOTIMESTAMP, DT_NANOTIMESTAMP};
        ConstantSP object = Util::createTable(cols, colTypes, 1, 1);
        object->set(0, 0, Util::createNanoTimestamp(test_val));
        object->set(1, 0, Util::createNanoTimestamp(0));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_TABLE);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_table_uuid)
{
    srand(time(NULL));
    unsigned char uuid[16];
    for (auto i = 0; i < 16; i++)
        uuid[i] = rand() % CHAR_MAX;
    DdbVector<Guid> test_val(2, 2);
    test_val.set(0, uuid);
    test_val.setNull(1);

    { // DDB table
        char *pOutbuf;
        int outSize;
        ConstantSP colVals = test_val.createVector(DT_UUID);
        vector<string> cols = {"col1", "col2"};
        vector<DATA_TYPE> colTypes = {DT_UUID, DT_UUID};
        ConstantSP object = Util::createTable(cols, colTypes, 1, 1);
        object->set(0, 0, colVals->get(0));
        object->set(1, 0, colVals->get(1));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_TABLE);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_table_symbol)
{
    vector<string> rand_str = {"sd","dag","xxx","智臾科技a","23!@#$%","^&#%……@","/,m[[`"};
    string test_val = rand_str[rand() % rand_str.size()];
    { // DDB table
        char *pOutbuf;
        int outSize;
        vector<string> cols = {"col1", "col2"};
        vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_SYMBOL};
        ConstantSP object = Util::createTable(cols, colTypes, 1, 1);
        object->set(0, 0, Util::createString(test_val));
        object->set(1, 0, Util::createString(""));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_TABLE);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}

TEST_F(SysIOTest, test_DataStream_table_ip)
{
    srand(time(NULL));
    unsigned char ip[16];
    for (auto i = 0; i < 16; i++)
        ip[i] = rand() % CHAR_MAX;
    DdbVector<Guid> test_val(2, 2);
    test_val.set(0,ip);
    test_val.setNull(1);

    { // DDB table
        char *pOutbuf;
        int outSize;
        ConstantSP colVals = test_val.createVector(DT_IP);
        vector<string> cols = {"col1", "col2"};
        vector<DATA_TYPE> colTypes = {DT_IP, DT_IP};
        ConstantSP object = Util::createTable(cols, colTypes, 1, 1);
        object->set(0, 0, colVals->get(0));
        object->set(1, 0, colVals->get(1));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_TABLE);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


TEST_F(SysIOTest, test_DataStream_table_int128)
{
    srand(time(NULL));
    unsigned char int128[16];
    for (auto i = 0; i < 16; i++)
        int128[i] = rand() % CHAR_MAX;
    DdbVector<Guid> test_val(2, 2);
    test_val.set(0,int128);
    test_val.setNull(1);

    { // DDB table
        char *pOutbuf;
        int outSize;
        ConstantSP colVals = test_val.createVector(DT_INT128);
        vector<string> cols = {"col1", "col2"};
        vector<DATA_TYPE> colTypes = {DT_INT128, DT_INT128};
        ConstantSP object = Util::createTable(cols, colTypes, 1, 1);
        object->set(0, 0, colVals->get(0));
        object->set(1, 0, colVals->get(1));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_TABLE);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}


INSTANTIATE_TEST_CASE_P(test_DataStream_table_Null, SysIOTest_Parameterized, testing::Values(DT_BOOL,DT_CHAR,DT_SHORT,DT_INT,DT_LONG,DT_DATE,DT_MONTH,DT_TIME,DT_MINUTE,DT_SECOND,DT_DATETIME,DT_TIMESTAMP,DT_NANOTIME,DT_NANOTIMESTAMP,
    DT_FLOAT,DT_DOUBLE,DT_SYMBOL,DT_STRING,DT_UUID,DT_DATEHOUR,
    DT_IP,DT_INT128,DT_BLOB, DT_DECIMAL32, DT_DECIMAL64));
TEST_P(SysIOTest_Parameterized, test_DataStream_table_Null){
    DATA_TYPE test_type =  GetParam();
    { // DDB table
        char *pOutbuf;
        int outSize;
        vector<string> cols = {"col1", "col2"};
        vector<DATA_TYPE> colTypes = {test_type, test_type};
        ConstantSP object = Util::createTable(cols, colTypes, 1, 1);
        object->set(0, 0, Util::createNullConstant(test_type));
        object->set(1, 0, Util::createNullConstant(test_type));
        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_TABLE);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }
}

TEST_F(SysIOTest, test_DataStream_table_withAllType)
{
	int colNum = 25, rowNum = 1; 
	vector<string> colNamesVec1;
	for (int i = 0; i < colNum; i++){
		colNamesVec1.emplace_back("col"+to_string(i));
	}
	vector<DATA_TYPE> colTypesVec1;
	colTypesVec1.emplace_back(DT_CHAR);
	colTypesVec1.emplace_back(DT_BOOL);
	colTypesVec1.emplace_back(DT_SHORT);
	colTypesVec1.emplace_back(DT_INT);
	colTypesVec1.emplace_back(DT_LONG);
	colTypesVec1.emplace_back(DT_DATE);
	colTypesVec1.emplace_back(DT_MONTH);
	colTypesVec1.emplace_back(DT_TIME);
	colTypesVec1.emplace_back(DT_MINUTE);
	colTypesVec1.emplace_back(DT_DATETIME);
	colTypesVec1.emplace_back(DT_SECOND);
	colTypesVec1.emplace_back(DT_TIMESTAMP);
	colTypesVec1.emplace_back(DT_NANOTIME);
	colTypesVec1.emplace_back(DT_NANOTIMESTAMP);
	colTypesVec1.emplace_back(DT_FLOAT);
	colTypesVec1.emplace_back(DT_DOUBLE);
	colTypesVec1.emplace_back(DT_STRING);
	colTypesVec1.emplace_back(DT_UUID);
	colTypesVec1.emplace_back(DT_IP);
	colTypesVec1.emplace_back(DT_INT128);
	colTypesVec1.emplace_back(DT_BLOB);
	colTypesVec1.emplace_back(DT_DATEHOUR);
	colTypesVec1.emplace_back(DT_DECIMAL32);
	colTypesVec1.emplace_back(DT_DECIMAL64);
	colTypesVec1.emplace_back(DT_SYMBOL);

	srand((int)time(NULL));
	TableSP object = Util::createTable(colNamesVec1, colTypesVec1, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(object->getColumn(i));

	}
	for (int i = 0; i < rowNum-1; i++){
		columnVecs[0]->set(i, Util::createChar(rand()%CHAR_MAX));
		columnVecs[1]->set(i, Util::createBool(rand()%2));
		columnVecs[2]->set(i, Util::createShort(rand()%SHRT_MAX));
		columnVecs[3]->set(i, Util::createInt(rand()%INT_MAX));
		columnVecs[4]->set(i, Util::createLong(rand()%LLONG_MAX));
		columnVecs[5]->set(i, Util::createDate(rand()%INT_MAX));
		columnVecs[6]->set(i, Util::createMonth(rand()%INT_MAX));
		columnVecs[7]->set(i, Util::createTime(rand()%INT_MAX));
		columnVecs[8]->set(i, Util::createMinute(rand()%1440));
		columnVecs[9]->set(i, Util::createDateTime(rand()%INT_MAX));
		columnVecs[10]->set(i, Util::createSecond(rand()%86400));
		columnVecs[11]->set(i, Util::createTimestamp(rand()%LLONG_MAX));
		columnVecs[12]->set(i, Util::createNanoTime(rand()%LLONG_MAX));
		columnVecs[13]->set(i, Util::createNanoTimestamp(rand()%LLONG_MAX));
		columnVecs[14]->set(i, Util::createFloat(rand()/float(RAND_MAX)));
		columnVecs[15]->set(i, Util::createDouble(rand()/double(RAND_MAX)));
		columnVecs[16]->set(i, Util::createString("str"+to_string(i)));
		columnVecs[17]->set(i, Util::parseConstant(DT_UUID,"5d212a78-cc48-e3b1-4235-b4d91473ee87"));	
		columnVecs[18]->set(i, Util::parseConstant(DT_IP,"192.0.0."+to_string(rand()%255)));
		columnVecs[19]->set(i, Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec32"));
		columnVecs[20]->set(i, Util::createBlob("blob"+to_string(i)));
		columnVecs[21]->set(i, Util::createDateHour(rand()%INT_MAX));
		columnVecs[22]->set(i, Util::createDecimal32(rand()%10,rand()/float(RAND_MAX)));
		columnVecs[23]->set(i, Util::createDecimal64(rand()%19,rand()/double(RAND_MAX)));
		columnVecs[24]->set(i, Util::createString("sym"+to_string(i)));
	}
	for (int j = 0; j < colNum; j++)
		columnVecs[j]->setNull(rowNum-1);

    { // DDB table
        char *pOutbuf;
        int outSize;

        DataOutputStreamSP outStream = new DataOutputStream();
        ConstantMarshallFactory marshallFactory(outStream);
        ConstantMarshall* marshall = marshallFactory.getConstantMarshall(object->getForm());
        IO_ERR ret;
        ASSERT_TRUE(marshall->start(object, true, false, ret));
        outSize = outStream->size();
        pOutbuf = new char[outSize];
        memcpy(pOutbuf, outStream->getBuffer(), outSize);
        marshall->reset();
        outStream->close();

        DataStreamSP inputStream = new DataStream(pOutbuf, outSize);
        short flag;
        inputStream->readShort(flag);
        ConstantUnmarshallFactory unmarshallFactory(inputStream);
        ConstantUnmarshall* unmarshall = unmarshallFactory.getConstantUnmarshall(DF_TABLE);
        IO_ERR ret2;
        ASSERT_TRUE(unmarshall->start(flag, true, ret2)) ;
        ConstantSP unmarsh_object=unmarshall->getConstant();
        EXPECT_EQ(unmarsh_object->getString(), object->getString());

        unmarshall->reset();
        inputStream->close();
        // delete[] pOutbuf; // when copy == false, pOutbuf can not be delete[].
    }

}



TEST_F(SysIOTest, test_class_Buffer)
{
    char buf1[]="aaaccc";
    char buf2[]="bbbddd";

    Buffer* bf1 = new Buffer();
    Buffer* bf2 = new Buffer(256);
    Buffer* bf3 = new Buffer(buf1, 256);
    Buffer* bf4 = new Buffer(buf2, -1, 256);
    int actlen;
    bf1->write(buf1,10,actlen);
    EXPECT_EQ((string)bf1->getBuffer(), buf1);

    bf2->write(buf2,20,actlen);
    EXPECT_EQ((string)bf2->getBuffer(), buf2);

    EXPECT_EQ((string)bf3->getBuffer(), buf1);
    EXPECT_EQ((string)bf4->getBuffer(), buf2);

    EXPECT_EQ(bf1->capacity(),256);
    EXPECT_EQ(bf2->capacity(),256);
    EXPECT_EQ(bf1->size(),10);
    EXPECT_EQ(bf2->size(),20);

    bf1->clear();
    EXPECT_EQ(bf1->size(), 0);
    string str = "lll1235678";
    bf2->clear();
    bf2->writeData(str);
    string bf2str(bf2->getBuffer(), str.length());
    EXPECT_EQ(bf2str, str);

    bf3->write(buf2, 7);
    EXPECT_EQ((string)bf3->getBuffer(), buf2);
}

TEST_F(SysIOTest, test_UdpSocket_Normal){
    char *readBuf = new char[256];
    memset(readBuf, 0, 256);
    size_t readLength = 0;
    ThreadSP readThread = new Thread(new Executor([=, &readLength](){
        UdpSocket st(10087);
        st.bind();
        st.recv(readBuf, 256, readLength);
    }));
    readThread->start();
    Util::sleep(100);
    char sendBuf[] = {"Hello World!"};
    UdpSocket sender("127.0.0.1", 10087);
    sender.send(sendBuf, sizeof(sendBuf));

    readThread->join();
    EXPECT_EQ(0, strcmp(readBuf, sendBuf));

    delete[] readBuf;
}

TEST_F(SysIOTest, test_DataStream_File){
#ifdef WINDOWS
    cout<<"skip this case."<<endl;
    EXPECT_EQ(1,1);
#else
    char workDir[256]{};
    getcwd(workDir, sizeof(workDir));
    std::string file = std::string(workDir).append(1, '/').append("tempFile123");
    {
        FILE* f = fopen(file.c_str(), "w+");
        ASSERT_NE(f, nullptr);
        DataStreamSP stream = new DataStream(f, true, true);
        char buf1[] = "Hello!\r\n";
        int sent = 0;
        stream->write(buf1, strlen(buf1), sent);
        char buf2[] = "My Name is";
        stream->writeLine(buf2, "\r\n");
        char buf3[] = "010!";
        stream->write(buf3, strlen(buf3), sent);
        std::cout << stream->getDescription() << std::endl;
        long long position;
        stream->seek(0, 0, position);
        EXPECT_EQ(position, 0);
    }
    {
        FILE* f = fopen(file.c_str(), "r");
        ASSERT_NE(f, nullptr);
        DataInputStreamSP input = new DataInputStream(f);
        EXPECT_FALSE(input->reset(100));
        char buffer[256]{};
        input->peekBuffer(buffer, 6);
        EXPECT_EQ(0, strcmp(buffer, "Hello!"));
        EXPECT_EQ(input->getPosition(), 0);
        input->moveToPosition(8);
        std::string line;
        input->peekLine(line);
        EXPECT_EQ(line, "My Name is");
        input->moveToPosition(20);
        char c = 2;
        input->readBool(c);
        EXPECT_EQ(c, '0');
        input->moveToPosition(0);
        char buf[256]{};
        size_t actualLength = 0;
        input->readBytes(buf, 6, actualLength);
        EXPECT_EQ(0, strcmp(buf, "Hello!"));
        input->moveToPosition(25);
        EXPECT_EQ(END_OF_STREAM, input->readBytes(buf, 6, actualLength));
    }
    remove(file.c_str());
#endif
}
