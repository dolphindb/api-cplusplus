class ExceptionTest:public testing::Test
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

static string eMsg="test";

TEST_F(ExceptionTest,testSyntaxException){
    try
    {
        SyntaxException e(eMsg);
        throw e;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        EXPECT_STREQ(e.what(),eMsg.c_str());
    }
    
}

TEST_F(ExceptionTest,testIncompatibleTypeException){
    DATA_TYPE dt1=DT_INT;
    DATA_TYPE dt2=DT_STRING;
    try
    {
        IncompatibleTypeException e(dt1,dt2);
        throw e;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        string ex_str="Incompatible type. Expected: " + Util::getDataTypeString(dt1) + ", Actual: " + Util::getDataTypeString(dt2);
        EXPECT_STREQ(e.what(),ex_str.c_str());
    }
    
}

TEST_F(ExceptionTest,testIllegalArgumentException){
    try
    {
        string eFuncName="func";
        IllegalArgumentException e(eFuncName,eMsg);
        throw e;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        EXPECT_STREQ(e.what(),eMsg.c_str());
    }
    
}

TEST_F(ExceptionTest,testRuntimeException){
    try
    {
        RuntimeException e(eMsg);
        throw e;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        EXPECT_STREQ(e.what(),eMsg.c_str());
    }
    
}

TEST_F(ExceptionTest,testOperatorRuntimeException){
    try
    {
        string eOptr="func";
        OperatorRuntimeException e(eOptr,eMsg);
        throw e;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        EXPECT_STREQ(e.what(),eMsg.c_str());
    }
    
}

TEST_F(ExceptionTest,testTableRuntimeException){
    try
    {
        TableRuntimeException e(eMsg);
        throw e;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        EXPECT_STREQ(e.what(),eMsg.c_str());
    }
    
}

TEST_F(ExceptionTest,testMemoryException){
    try
    {
        MemoryException e;
        throw e;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        string ex_str= "Out of memory";
        EXPECT_STREQ(e.what(),ex_str.c_str());
    }
    
}

TEST_F(ExceptionTest,testIOException){
    vector<IO_ERR> ioErrVec={OK,DISCONNECTED,NODATA,NOSPACE,TOO_LARGE_DATA,INPROGRESS,INVALIDDATA,END_OF_STREAM,READONLY,WRITEONLY,NOTEXIST,CORRUPT,NOT_LEADER,OTHERERR};
    string getMsg;
    for(int i=0;i<ioErrVec.size();i++){
        try
        {
            IOException e(getMsg,ioErrVec[i]);
            throw e;
        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }
    }
    for(int i=0;i<ioErrVec.size();i++){
        try
        {
            IOException e(ioErrVec[i]);
            throw e;
        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }
    }
    try
    {
        IOException e(getMsg);
        throw e;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
    
}

TEST_F(ExceptionTest,testDataCorruptionException){
    try
    {
        DataCorruptionException e(eMsg);
        throw e;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        EXPECT_STREQ(e.what(),("<DataCorruption>"+eMsg).c_str());
    }
    
}

TEST_F(ExceptionTest,testNotLeaderException){
    string newLeader="datanode3";
    try
    {
        NotLeaderException e(eMsg);
        throw e;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        EXPECT_STREQ(e.what(),("<NotLeader>"+eMsg).c_str());
    }
    try
    {
        NotLeaderException e(newLeader);
        throw e;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        EXPECT_STREQ(e.what(),("<NotLeader>"+newLeader).c_str());
    }
    
}

TEST_F(ExceptionTest,testMathException){
    try
    {
        MathException e(eMsg);
        throw e;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        EXPECT_STREQ(e.what(),eMsg.c_str());
    }
    
}

TEST_F(ExceptionTest,testTestingException){
    string eCase="main case";
    string esubCase="sub testcase";
    string emptySubCase="";
    try
    {

        TestingException e(eCase, esubCase);
        throw e;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        EXPECT_STREQ(e.what(),("Testing case "+eCase+"_"+esubCase+" failed").c_str());
    }
    
    try
    {

        TestingException e(eCase, emptySubCase);
        throw e;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        EXPECT_STREQ(e.what(),("Testing case "+eCase+" failed").c_str());
    }
    
}

TEST_F(ExceptionTest,testUserException){
    try
    {
        string eType="UserException";
        UserException e(eType, eMsg);
        EXPECT_STREQ((e.getExceptionType()).c_str(), eType.c_str());
        throw e;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        EXPECT_STREQ(e.what(),eMsg.c_str());
    }
    
}