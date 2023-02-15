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
		
        cout<<"ok"<<endl;
    }
    virtual void TearDown()
    {
        conn.run("undef all;");
    }
};


TEST_F(DataformMatrixTest,testStringMatrix){
	EXPECT_ANY_THROW(VectorSP v1 = Util::createMatrix(DT_STRING,2,2,4));
}

TEST_F(DataformMatrixTest,testAnyMatrix){
	EXPECT_ANY_THROW(VectorSP v1 = Util::createMatrix(DT_ANY,2,2,4));
}

TEST_F(DataformMatrixTest,testDecimal32Matrix){
	EXPECT_ANY_THROW(VectorSP v1 = Util::createMatrix(DT_DECIMAL32,2,2,4));
}

TEST_F(DataformMatrixTest,testDecimal64Matrix){
	EXPECT_ANY_THROW(VectorSP v1 = Util::createMatrix(DT_DECIMAL64,2,2,4));
}

#ifndef WINDOWS
TEST_F(DataformMatrixTest, testBoolMatrix_getValue) {
    char val[4] = {1, 1, 0, 0};
    FastBoolMatrix* matrix1 = new FastBoolMatrix(2, 2, 4, val, false);
    ConstantSP matrix2 = matrix1->getValue();
    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testBoolMatrix_set) {
    char val1[4] = {1, 1, 0, 0};
    FastBoolMatrix* matrix1 = new FastBoolMatrix(2, 2, 4, val1, false);
    char val2[4] = {1, 1, 1, 1};
    FastBoolMatrix* matrix2 = new FastBoolMatrix(2, 2, 4, val2, false);
    
    ConstantSP insertVec = Util::createVector(DT_BOOL, 2, 2);
    insertVec->set(0, Util::createBool(1));
    insertVec->set(1, Util::createBool(1));
    matrix1->set(1, 0, insertVec);

    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testCharMatrix_getValue) {
    char val[4] = {1, 1, 0, 0};
    FastCharMatrix* matrix1 = new FastCharMatrix(2, 2, 4, val, false);
    ConstantSP matrix2 = matrix1->getValue();
    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testCharMatrix_set) {
    char val1[4] = {1, 1, 0, 0};
    FastCharMatrix* matrix1 = new FastCharMatrix(2, 2, 4, val1, false);
    char val2[4] = {1, 1, 1, 1};
    FastCharMatrix* matrix2 = new FastCharMatrix(2, 2, 4, val2, false);
    
    ConstantSP insertVec = Util::createVector(DT_CHAR, 2, 2);
    insertVec->set(0, Util::createChar(1));
    insertVec->set(1, Util::createChar(1));
    matrix1->set(1, 0, insertVec);

    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testShortMatrix_getValue) {
    short val[4] = {1, 1, 0, 0};
    FastShortMatrix* matrix1 = new FastShortMatrix(2, 2, 4, val, false);
    ConstantSP matrix2 = matrix1->getValue();
    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testShortMatrix_set) {
    short val1[4] = {1, 1, 0, 0};
    FastShortMatrix* matrix1 = new FastShortMatrix(2, 2, 4, val1, false);
    short val2[4] = {1, 1, 1, 1};
    FastShortMatrix* matrix2 = new FastShortMatrix(2, 2, 4, val2, false);
    
    ConstantSP insertVec = Util::createVector(DT_SHORT, 2, 2);
    insertVec->set(0, Util::createShort(1));
    insertVec->set(1, Util::createShort(1));
    matrix1->set(1, 0, insertVec);

    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testIntMatrix_getValue) {
    int val[4] = {1, 1, 0, 0};
    FastIntMatrix* matrix1 = new FastIntMatrix(2, 2, 4, val, false);
    ConstantSP matrix2 = matrix1->getValue();
    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testIntMatrix_set) {
    int val1[4] = {1, 1, 0, 0};
    FastIntMatrix* matrix1 = new FastIntMatrix(2, 2, 4, val1, false);
    int val2[4] = {1, 1, 1, 1};
    FastIntMatrix* matrix2 = new FastIntMatrix(2, 2, 4, val2, false);
    
    ConstantSP insertVec = Util::createVector(DT_INT, 2, 2);
    insertVec->set(0, Util::createInt(1));
    insertVec->set(1, Util::createInt(1));
    matrix1->set(1, 0, insertVec);

    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testLongMatrix_getValue) {
    long long val[4] = {1, 1, 0, 0};
    FastLongMatrix* matrix1 = new FastLongMatrix(2, 2, 4, val, false);
    ConstantSP matrix2 = matrix1->getValue();
    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testLongMatrix_set) {
    long long val1[4] = {1, 1, 0, 0};
    FastLongMatrix* matrix1 = new FastLongMatrix(2, 2, 4, val1, false);
    long long val2[4] = {1, 1, 1, 1};
    FastLongMatrix* matrix2 = new FastLongMatrix(2, 2, 4, val2, false);
    
    ConstantSP insertVec = Util::createVector(DT_LONG, 2, 2);
    insertVec->set(0, Util::createLong(1));
    insertVec->set(1, Util::createLong(1));
    matrix1->set(1, 0, insertVec);

    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testFloatMatrix_getValue) {
    float val[4] = {1, 1, 0, 0};
    FastFloatMatrix* matrix1 = new FastFloatMatrix(2, 2, 4, val, false);
    ConstantSP matrix2 = matrix1->getValue();
    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testFloatMatrix_set) {
    float val1[4] = {1, 1, 0, 0};
    FastFloatMatrix* matrix1 = new FastFloatMatrix(2, 2, 4, val1, false);
    float val2[4] = {1, 1, 1, 1};
    FastFloatMatrix* matrix2 = new FastFloatMatrix(2, 2, 4, val2, false);
    
    ConstantSP insertVec = Util::createVector(DT_FLOAT, 2, 2);
    insertVec->set(0, Util::createFloat(1));
    insertVec->set(1, Util::createFloat(1));
    matrix1->set(1, 0, insertVec);

    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testDoubleMatrix_getValue) {
    double val[4] = {1, 1, 0, 0};
    FastDoubleMatrix* matrix1 = new FastDoubleMatrix(2, 2, 4, val, false);
    ConstantSP matrix2 = matrix1->getValue();
    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testDoubleMatrix_set) {
    double val1[4] = {1, 1, 0, 0};
    FastDoubleMatrix* matrix1 = new FastDoubleMatrix(2, 2, 4, val1, false);
    double val2[4] = {1, 1, 1, 1};
    FastDoubleMatrix* matrix2 = new FastDoubleMatrix(2, 2, 4, val2, false);
    
    ConstantSP insertVec = Util::createVector(DT_DOUBLE, 2, 2);
    insertVec->set(0, Util::createDouble(1));
    insertVec->set(1, Util::createDouble(1));
    matrix1->set(1, 0, insertVec);

    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testDateMatrix_getValue) {
    int val[4] = {1, 1, 0, 0};
    FastDateMatrix* matrix1 = new FastDateMatrix(2, 2, 4, val, false);
    ConstantSP matrix2 = matrix1->getValue();
    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testDateMatrix_set) {
    int val1[4] = {1, 1, 0, 0};
    FastDateMatrix* matrix1 = new FastDateMatrix(2, 2, 4, val1, false);
    int val2[4] = {1, 1, 1, 1};
    FastDateMatrix* matrix2 = new FastDateMatrix(2, 2, 4, val2, false);
    
    ConstantSP insertVec = Util::createVector(DT_DATE, 2, 2);
    insertVec->set(0, Util::createDate(1));
    insertVec->set(1, Util::createDate(1));
    matrix1->set(1, 0, insertVec);

    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testDateTimeMatrix_getValue) {
    int val[4] = {1, 1, 0, 0};
    FastDateTimeMatrix* matrix1 = new FastDateTimeMatrix(2, 2, 4, val, false);
    ConstantSP matrix2 = matrix1->getValue();
    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testDateTimeMatrix_set) {
    int val1[4] = {1, 1, 0, 0};
    FastDateTimeMatrix* matrix1 = new FastDateTimeMatrix(2, 2, 4, val1, false);
    int val2[4] = {1, 1, 1, 1};
    FastDateTimeMatrix* matrix2 = new FastDateTimeMatrix(2, 2, 4, val2, false);
    
    ConstantSP insertVec = Util::createVector(DT_DATETIME, 2, 2);
    insertVec->set(0, Util::createDateTime(1));
    insertVec->set(1, Util::createDateTime(1));
    matrix1->set(1, 0, insertVec);

    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testDateHourMatrix_set) {
    int val1[4] = {1, 1, 0, 0};
    FastDateHourMatrix* matrix1 = new FastDateHourMatrix(2, 2, 4, val1, false);
    int val2[4] = {1, 1, 1, 1};
    FastDateHourMatrix* matrix2 = new FastDateHourMatrix(2, 2, 4, val2, false);
    
    ConstantSP insertVec = Util::createVector(DT_DATEHOUR, 2, 2);
    insertVec->set(0, Util::createDateHour(1));
    insertVec->set(1, Util::createDateHour(1));
    matrix1->set(1, 0, insertVec);

    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testMonthMatrix_getValue) {
    int val[4] = {1, 1, 0, 0};
    FastMonthMatrix* matrix1 = new FastMonthMatrix(2, 2, 4, val, false);
    ConstantSP matrix2 = matrix1->getValue();
    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testMonthMatrix_set) {
    int val1[4] = {1, 1, 0, 0};
    FastMonthMatrix* matrix1 = new FastMonthMatrix(2, 2, 4, val1, false);
    int val2[4] = {1, 1, 1, 1};
    FastMonthMatrix* matrix2 = new FastMonthMatrix(2, 2, 4, val2, false);
    
    ConstantSP insertVec = Util::createVector(DT_MONTH, 2, 2);
    insertVec->set(0, Util::createMonth(1));
    insertVec->set(1, Util::createMonth(1));
    matrix1->set(1, 0, insertVec);

    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testTimeMatrix_getValue) {
    int val[4] = {1, 1, 0, 0};
    FastTimeMatrix* matrix1 = new FastTimeMatrix(2, 2, 4, val, false);
    ConstantSP matrix2 = matrix1->getValue();
    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testTimeMatrix_set) {
    int val1[4] = {1, 1, 0, 0};
    FastTimeMatrix* matrix1 = new FastTimeMatrix(2, 2, 4, val1, false);
    int val2[4] = {1, 1, 1, 1};
    FastTimeMatrix* matrix2 = new FastTimeMatrix(2, 2, 4, val2, false);
    
    ConstantSP insertVec = Util::createVector(DT_TIME, 2, 2);
    insertVec->set(0, Util::createTime(1));
    insertVec->set(1, Util::createTime(1));
    matrix1->set(1, 0, insertVec);

    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testSecondMatrix_getValue) {
    int val[4] = {1, 1, 0, 0};
    FastSecondMatrix* matrix1 = new FastSecondMatrix(2, 2, 4, val, false);
    ConstantSP matrix2 = matrix1->getValue();
    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testSecondMatrix_set) {
    int val1[4] = {1, 1, 0, 0};
    FastSecondMatrix* matrix1 = new FastSecondMatrix(2, 2, 4, val1, false);
    int val2[4] = {1, 1, 1, 1};
    FastSecondMatrix* matrix2 = new FastSecondMatrix(2, 2, 4, val2, false);
    
    ConstantSP insertVec = Util::createVector(DT_SECOND, 2, 2);
    insertVec->set(0, Util::createSecond(1));
    insertVec->set(1, Util::createSecond(1));
    matrix1->set(1, 0, insertVec);

    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testMinuteMatrix_getValue) {
    int val[4] = {1, 1, 0, 0};
    FastMinuteMatrix* matrix1 = new FastMinuteMatrix(2, 2, 4, val, false);
    ConstantSP matrix2 = matrix1->getValue();
    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testMinuteMatrix_set) {
    int val1[4] = {1, 1, 0, 0};
    FastMinuteMatrix* matrix1 = new FastMinuteMatrix(2, 2, 4, val1, false);
    int val2[4] = {1, 1, 1, 1};
    FastMinuteMatrix* matrix2 = new FastMinuteMatrix(2, 2, 4, val2, false);
    
    ConstantSP insertVec = Util::createVector(DT_MINUTE, 2, 2);
    insertVec->set(0, Util::createMinute(1));
    insertVec->set(1, Util::createMinute(1));
    matrix1->set(1, 0, insertVec);

    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testNanoTimeMatrix_getValue) {
    long long val[4] = {1, 1, 0, 0};
    FastNanoTimeMatrix* matrix1 = new FastNanoTimeMatrix(2, 2, 4, val, false);
    ConstantSP matrix2 = matrix1->getValue();
    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testNanoTimeMatrix_set) {
    long long val1[4] = {1, 1, 0, 0};
    FastNanoTimeMatrix* matrix1 = new FastNanoTimeMatrix(2, 2, 4, val1, false);
    long long val2[4] = {1, 1, 1, 1};
    FastNanoTimeMatrix* matrix2 = new FastNanoTimeMatrix(2, 2, 4, val2, false);
    
    ConstantSP insertVec = Util::createVector(DT_NANOTIME, 2, 2);
    insertVec->set(0, Util::createNanoTime(1));
    insertVec->set(1, Util::createNanoTime(1));
    matrix1->set(1, 0, insertVec);

    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testTimestampMatrix_getValue) {
    long long val[4] = {1, 1, 0, 0};
    FastTimestampMatrix* matrix1 = new FastTimestampMatrix(2, 2, 4, val, false);
    ConstantSP matrix2 = matrix1->getValue();
    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testTimestampMatrix_set) {
    long long val1[4] = {1, 1, 0, 0};
    FastTimestampMatrix* matrix1 = new FastTimestampMatrix(2, 2, 4, val1, false);
    long long val2[4] = {1, 1, 1, 1};
    FastTimestampMatrix* matrix2 = new FastTimestampMatrix(2, 2, 4, val2, false);
    
    ConstantSP insertVec = Util::createVector(DT_TIMESTAMP, 2, 2);
    insertVec->set(0, Util::createTimestamp(1));
    insertVec->set(1, Util::createTimestamp(1));
    matrix1->set(1, 0, insertVec);

    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testNanoTimestampMatrix_getValue) {
    long long val[4] = {1, 1, 0, 0};
    FastNanoTimestampMatrix* matrix1 = new FastNanoTimestampMatrix(2, 2, 4, val, false);
    ConstantSP matrix2 = matrix1->getValue();
    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}

TEST_F(DataformMatrixTest, testNanoTimestampMatrix_set) {
    long long val1[4] = {1, 1, 0, 0};
    FastNanoTimestampMatrix* matrix1 = new FastNanoTimestampMatrix(2, 2, 4, val1, false);
    long long val2[4] = {1, 1, 1, 1};
    FastNanoTimestampMatrix* matrix2 = new FastNanoTimestampMatrix(2, 2, 4, val2, false);
    
    ConstantSP insertVec = Util::createVector(DT_TIMESTAMP, 2, 2);
    insertVec->set(0, Util::createNanoTimestamp(1));
    insertVec->set(1, Util::createNanoTimestamp(1));
    matrix1->set(1, 0, insertVec);

    EXPECT_EQ(matrix1->getString(), matrix2->getString());
}
#endif


TEST_F(DataformMatrixTest,testBoolMatrix){
	VectorSP v1 = Util::createMatrix(DT_BOOL,2,2,4);
    v1->set(0,0, Util::createBool(1));
    v1->set(1,0, Util::createBool(1));
	v1->set(0,1, Util::createBool(0));
	v1->set(1,1, Util::createBool(0));
	string script = "a=matrix([[bool(1),bool(0)],[bool(1),bool(0)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});

	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(conn.run("v1")->isMatrix(),true);

    EXPECT_FALSE(v1->reshape(2,1));
    
    v1->reshape(1,4);
    EXPECT_EQ(v1->rows(),4);
    EXPECT_EQ(v1->columns(),1);
    v1->reshape(4,1);
    EXPECT_EQ(v1->rows(),1);
    EXPECT_EQ(v1->columns(),4);
    v1->reshape(2,2);
    EXPECT_EQ(v1->rows(),2);
    EXPECT_EQ(v1->columns(),2);
    EXPECT_EQ(v1->getRowLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());
    EXPECT_EQ(v1->getColumnLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());

    EXPECT_EQ(v1->getString(1),"{"+v1->getColumn(1)->get(0)->getString()+","+v1->getColumn(1)->get(1)->getString()+"}");
    EXPECT_ANY_THROW(v1->set(Util::createInt(3),Util::createBool(1)));
    EXPECT_ANY_THROW(v1->set(Util::createInt(-1),Util::createBool(1)));
    v1->set(Util::createInt(1),Util::createBool(1));
    EXPECT_EQ(v1->getColumn(1)->get(1)->getBool(),1);
    VectorSP vals = Util::createVector(DT_BOOL,0,2);
    VectorSP cols = Util::createVector(DT_INT,0,2);
    cols->append(Util::createInt(0));
    cols->append(Util::createInt(1));
    vals->append(Util::createBool(0));
    vals->append(Util::createBool(0));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    vals->append(Util::createBool(0));
    vals->append(Util::createBool(0));
    v1->set(cols,vals);
    EXPECT_EQ(v1->getString(),"#0 #1\n-- --\n0  0 \n0  0 \n");
    cols->append(Util::createInt(-1));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    cols->set(2, Util::createInt(3));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    // ConstantSP constCol = Util::createInt(0);
    // EXPECT_EQ(v1->get(constCol)->getString(),"#0\n--\n0 \n0 \n");

    v1->setItem(0,Util::createBool(1));
    EXPECT_EQ(v1->getItem(0)->getString(),"["+v1->getColumn(0)->get(0)->getString()+","+v1->getColumn(0)->get(1)->getString()+"]");

    VectorSP v2=v1->getInstance(4);
    EXPECT_EQ(v2->rows(),v1->rows());
    EXPECT_EQ(v2->columns(),4);
    VectorSP v3 = v1->getInstance();
    EXPECT_EQ(v3->rows(),v1->rows());
    EXPECT_EQ(v3->columns(),v1->columns());

    cout<<v1->getString()<<endl;
    VectorSP rowlabelvec = Util::createVector(DT_STRING,0,2);
    rowlabelvec->append(Util::createString("lab0"));
    rowlabelvec->append(Util::createString("lab1"));
    EXPECT_ANY_THROW(v1->setRowLabel(Util::createString("lab")));
    v1->setRowLabel(rowlabelvec);
    VectorSP collabelvec = Util::createVector(DT_STRING,0,2);
    collabelvec->append(Util::createString("collab0"));
    collabelvec->append(Util::createString("collab1"));
    EXPECT_ANY_THROW(v1->setColumnLabel(Util::createString("lab")));
    v1->setColumnLabel(collabelvec);

    EXPECT_EQ(v1->getRow(0)->getString(),"["+v1->getRow(0)->get(0)->getString()+","+v1->getRow(0)->get(1)->getString()+"]");
    VectorSP winMatrix1 = v1->getWindow(0,2,0,1);
    winMatrix1->set(0,0,Util::createBool(0));
    winMatrix1->set(1,0,Util::createBool(1));
    EXPECT_EQ(winMatrix1->getValue(2)->getString(),"["+winMatrix1->getRow(0)->get(0)->getString()+","+winMatrix1->getRow(0)->get(1)->getString()+"]");
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

    EXPECT_FALSE(v1->reshape(2,1));
    
    v1->reshape(1,4);
    EXPECT_EQ(v1->rows(),4);
    EXPECT_EQ(v1->columns(),1);
    v1->reshape(4,1);
    EXPECT_EQ(v1->rows(),1);
    EXPECT_EQ(v1->columns(),4);
    v1->reshape(2,2);
    EXPECT_EQ(v1->rows(),2);
    EXPECT_EQ(v1->columns(),2);
    EXPECT_EQ(v1->getRowLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());
    EXPECT_EQ(v1->getColumnLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());

    EXPECT_EQ(v1->getString(1),"{"+v1->getColumn(1)->get(0)->getString()+","+v1->getColumn(1)->get(1)->getString()+"}");
    EXPECT_ANY_THROW(v1->set(Util::createInt(3),Util::createChar(1)));
    EXPECT_ANY_THROW(v1->set(Util::createInt(-1),Util::createChar(1)));
    v1->set(Util::createInt(1),Util::createChar(1));
    EXPECT_EQ(v1->getColumn(1)->get(1)->getBool(),1);
    VectorSP vals = Util::createVector(DT_CHAR,0,2);
    VectorSP cols = Util::createVector(DT_INT,0,2);
    cols->append(Util::createInt(0));
    cols->append(Util::createInt(1));
    vals->append(Util::createChar(0));
    vals->append(Util::createChar(0));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    vals->append(Util::createChar(0));
    vals->append(Util::createChar(0));
    v1->set(cols,vals);
    EXPECT_EQ(v1->getString(),"#0 #1\n-- --\n0  0 \n0  0 \n");
    cols->append(Util::createInt(-1));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    cols->set(2, Util::createInt(3));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    // ConstantSP constCol = Util::createInt(0);
    // EXPECT_EQ(v1->get(constCol)->getString(),"#0\n--\n0 \n0 \n");

    v1->setItem(0,Util::createChar(1));
    EXPECT_EQ(v1->getItem(0)->getString(),"["+v1->getColumn(0)->get(0)->getString()+","+v1->getColumn(0)->get(1)->getString()+"]");

    VectorSP v2=v1->getInstance(4);
    EXPECT_EQ(v2->rows(),v1->rows());
    EXPECT_EQ(v2->columns(),4);
    VectorSP v3 = v1->getInstance();
    EXPECT_EQ(v3->rows(),v1->rows());
    EXPECT_EQ(v3->columns(),v1->columns());

    cout<<v1->getString()<<endl;
    VectorSP rowlabelvec = Util::createVector(DT_STRING,0,2);
    rowlabelvec->append(Util::createString("lab0"));
    rowlabelvec->append(Util::createString("lab1"));
    EXPECT_ANY_THROW(v1->setRowLabel(Util::createString("lab")));
    v1->setRowLabel(rowlabelvec);
    VectorSP collabelvec = Util::createVector(DT_STRING,0,2);
    collabelvec->append(Util::createString("collab0"));
    collabelvec->append(Util::createString("collab1"));
    EXPECT_ANY_THROW(v1->setColumnLabel(Util::createString("lab")));
    v1->setColumnLabel(collabelvec);

    EXPECT_EQ(v1->getRow(0)->getString(),"["+v1->getRow(0)->get(0)->getString()+","+v1->getRow(0)->get(1)->getString()+"]");
    VectorSP winMatrix1 = v1->getWindow(0,2,0,1);
    winMatrix1->set(0,0,Util::createChar(0));
    winMatrix1->set(1,0,Util::createChar(1));
    EXPECT_EQ(winMatrix1->getValue(2)->getString(),"["+winMatrix1->getRow(0)->get(0)->getString()+","+winMatrix1->getRow(0)->get(1)->getString()+"]");
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

    EXPECT_FALSE(v1->reshape(2,1));
    
    v1->reshape(1,4);
    EXPECT_EQ(v1->rows(),4);
    EXPECT_EQ(v1->columns(),1);
    v1->reshape(4,1);
    EXPECT_EQ(v1->rows(),1);
    EXPECT_EQ(v1->columns(),4);
    v1->reshape(2,2);
    EXPECT_EQ(v1->rows(),2);
    EXPECT_EQ(v1->columns(),2);
    EXPECT_EQ(v1->getRowLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());
    EXPECT_EQ(v1->getColumnLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());

    EXPECT_EQ(v1->getString(1),"{"+v1->getColumn(1)->get(0)->getString()+","+v1->getColumn(1)->get(1)->getString()+"}");
    EXPECT_ANY_THROW(v1->set(Util::createInt(3),Util::createInt(1)));
    EXPECT_ANY_THROW(v1->set(Util::createInt(-1),Util::createInt(1)));
    v1->set(Util::createInt(1),Util::createInt(1));
    EXPECT_EQ(v1->getColumn(1)->get(1)->getBool(),1);
    VectorSP vals = Util::createVector(DT_INT,0,2);
    VectorSP cols = Util::createVector(DT_INT,0,2);
    cols->append(Util::createInt(0));
    cols->append(Util::createInt(1));
    vals->append(Util::createInt(0));
    vals->append(Util::createInt(0));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    vals->append(Util::createInt(0));
    vals->append(Util::createInt(0));
    v1->set(cols,vals);
    EXPECT_EQ(v1->getString(),"#0 #1\n-- --\n0  0 \n0  0 \n");
    cols->append(Util::createInt(-1));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    cols->set(2, Util::createInt(3));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    // ConstantSP constCol = Util::createInt(0);
    // EXPECT_EQ(v1->get(constCol)->getString(),"#0\n--\n0 \n0 \n");

    v1->setItem(0,Util::createBool(1));
    EXPECT_EQ(v1->getItem(0)->getString(),"["+v1->getColumn(0)->get(0)->getString()+","+v1->getColumn(0)->get(1)->getString()+"]");

    VectorSP v2=v1->getInstance(4);
    EXPECT_EQ(v2->rows(),v1->rows());
    EXPECT_EQ(v2->columns(),4);
    VectorSP v3 = v1->getInstance();
    EXPECT_EQ(v3->rows(),v1->rows());
    EXPECT_EQ(v3->columns(),v1->columns());

    cout<<v1->getString()<<endl;
    VectorSP rowlabelvec = Util::createVector(DT_STRING,0,2);
    rowlabelvec->append(Util::createString("lab0"));
    rowlabelvec->append(Util::createString("lab1"));
    EXPECT_ANY_THROW(v1->setRowLabel(Util::createString("lab")));
    v1->setRowLabel(rowlabelvec);
    VectorSP collabelvec = Util::createVector(DT_STRING,0,2);
    collabelvec->append(Util::createString("collab0"));
    collabelvec->append(Util::createString("collab1"));
    EXPECT_ANY_THROW(v1->setColumnLabel(Util::createString("lab")));
    v1->setColumnLabel(collabelvec);

    EXPECT_EQ(v1->getRow(0)->getString(),"["+v1->getRow(0)->get(0)->getString()+","+v1->getRow(0)->get(1)->getString()+"]");
    VectorSP winMatrix1 = v1->getWindow(0,2,0,1);
    winMatrix1->set(0,0,Util::createInt(0));
    winMatrix1->set(1,0,Util::createInt(1));
    EXPECT_EQ(winMatrix1->getValue(2)->getString(),"["+winMatrix1->getRow(0)->get(0)->getString()+","+winMatrix1->getRow(0)->get(1)->getString()+"]");
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

    EXPECT_FALSE(v1->reshape(2,1));
    
    v1->reshape(1,4);
    EXPECT_EQ(v1->rows(),4);
    EXPECT_EQ(v1->columns(),1);
    v1->reshape(4,1);
    EXPECT_EQ(v1->rows(),1);
    EXPECT_EQ(v1->columns(),4);
    v1->reshape(2,2);
    EXPECT_EQ(v1->rows(),2);
    EXPECT_EQ(v1->columns(),2);
    EXPECT_EQ(v1->getRowLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());
    EXPECT_EQ(v1->getColumnLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());

    EXPECT_EQ(v1->getString(1),"{"+v1->getColumn(1)->get(0)->getString()+","+v1->getColumn(1)->get(1)->getString()+"}");
    EXPECT_ANY_THROW(v1->set(Util::createInt(3),Util::createLong(1)));
    EXPECT_ANY_THROW(v1->set(Util::createInt(-1),Util::createLong(1)));
    v1->set(Util::createInt(1),Util::createLong(1));
    EXPECT_EQ(v1->getColumn(1)->get(1)->getBool(),1);
    VectorSP vals = Util::createVector(DT_LONG,0,2);
    VectorSP cols = Util::createVector(DT_INT,0,2);
    cols->append(Util::createInt(0));
    cols->append(Util::createInt(1));
    vals->append(Util::createLong(0));
    vals->append(Util::createLong(0));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    vals->append(Util::createLong(0));
    vals->append(Util::createLong(0));
    v1->set(cols,vals);
    EXPECT_EQ(v1->getString(),"#0 #1\n-- --\n0  0 \n0  0 \n");
    cols->append(Util::createInt(-1));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    cols->set(2, Util::createInt(3));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    // ConstantSP constCol = Util::createInt(0);
    // EXPECT_EQ(v1->get(constCol)->getString(),"#0\n--\n0 \n0 \n");

    v1->setItem(0,Util::createLong(1));
    EXPECT_EQ(v1->getItem(0)->getString(),"["+v1->getColumn(0)->get(0)->getString()+","+v1->getColumn(0)->get(1)->getString()+"]");

    VectorSP v2=v1->getInstance(4);
    EXPECT_EQ(v2->rows(),v1->rows());
    EXPECT_EQ(v2->columns(),4);
    VectorSP v3 = v1->getInstance();
    EXPECT_EQ(v3->rows(),v1->rows());
    EXPECT_EQ(v3->columns(),v1->columns());

    cout<<v1->getString()<<endl;
    VectorSP rowlabelvec = Util::createVector(DT_STRING,0,2);
    rowlabelvec->append(Util::createString("lab0"));
    rowlabelvec->append(Util::createString("lab1"));
    EXPECT_ANY_THROW(v1->setRowLabel(Util::createString("lab")));
    v1->setRowLabel(rowlabelvec);
    VectorSP collabelvec = Util::createVector(DT_STRING,0,2);
    collabelvec->append(Util::createString("collab0"));
    collabelvec->append(Util::createString("collab1"));
    EXPECT_ANY_THROW(v1->setColumnLabel(Util::createString("lab")));
    v1->setColumnLabel(collabelvec);

    EXPECT_EQ(v1->getRow(0)->getString(),"["+v1->getRow(0)->get(0)->getString()+","+v1->getRow(0)->get(1)->getString()+"]");
    VectorSP winMatrix1 = v1->getWindow(0,2,0,1);
    winMatrix1->set(0,0,Util::createLong(0));
    winMatrix1->set(1,0,Util::createLong(1));
    EXPECT_EQ(winMatrix1->getValue(2)->getString(),"["+winMatrix1->getRow(0)->get(0)->getString()+","+winMatrix1->getRow(0)->get(1)->getString()+"]");
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

    EXPECT_FALSE(v1->reshape(2,1));
    
    v1->reshape(1,4);
    EXPECT_EQ(v1->rows(),4);
    EXPECT_EQ(v1->columns(),1);
    v1->reshape(4,1);
    EXPECT_EQ(v1->rows(),1);
    EXPECT_EQ(v1->columns(),4);
    v1->reshape(2,2);
    EXPECT_EQ(v1->rows(),2);
    EXPECT_EQ(v1->columns(),2);
    EXPECT_EQ(v1->getRowLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());
    EXPECT_EQ(v1->getColumnLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());

    EXPECT_EQ(v1->getString(1),"{"+v1->getColumn(1)->get(0)->getString()+","+v1->getColumn(1)->get(1)->getString()+"}");
    EXPECT_ANY_THROW(v1->set(Util::createInt(3),Util::createShort(1)));
    EXPECT_ANY_THROW(v1->set(Util::createInt(-1),Util::createShort(1)));
    v1->set(Util::createInt(1),Util::createShort(1));
    EXPECT_EQ(v1->getColumn(1)->get(1)->getBool(),1);
    VectorSP vals = Util::createVector(DT_SHORT,0,2);
    VectorSP cols = Util::createVector(DT_INT,0,2);
    cols->append(Util::createInt(0));
    cols->append(Util::createInt(1));
    vals->append(Util::createShort(0));
    vals->append(Util::createShort(0));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    vals->append(Util::createShort(0));
    vals->append(Util::createShort(0));
    v1->set(cols,vals);
    EXPECT_EQ(v1->getString(),"#0 #1\n-- --\n0  0 \n0  0 \n");
    cols->append(Util::createInt(-1));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    cols->set(2, Util::createInt(3));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    // ConstantSP constCol = Util::createInt(0);
    // EXPECT_EQ(v1->get(constCol)->getString(),"#0\n--\n0 \n0 \n");

    v1->setItem(0,Util::createShort(1));
    EXPECT_EQ(v1->getItem(0)->getString(),"["+v1->getColumn(0)->get(0)->getString()+","+v1->getColumn(0)->get(1)->getString()+"]");

    VectorSP v2=v1->getInstance(4);
    EXPECT_EQ(v2->rows(),v1->rows());
    EXPECT_EQ(v2->columns(),4);
    VectorSP v3 = v1->getInstance();
    EXPECT_EQ(v3->rows(),v1->rows());
    EXPECT_EQ(v3->columns(),v1->columns());

    cout<<v1->getString()<<endl;
    VectorSP rowlabelvec = Util::createVector(DT_STRING,0,2);
    rowlabelvec->append(Util::createString("lab0"));
    rowlabelvec->append(Util::createString("lab1"));
    EXPECT_ANY_THROW(v1->setRowLabel(Util::createString("lab")));
    v1->setRowLabel(rowlabelvec);
    VectorSP collabelvec = Util::createVector(DT_STRING,0,2);
    collabelvec->append(Util::createString("collab0"));
    collabelvec->append(Util::createString("collab1"));
    EXPECT_ANY_THROW(v1->setColumnLabel(Util::createString("lab")));
    v1->setColumnLabel(collabelvec);

    EXPECT_EQ(v1->getRow(0)->getString(),"["+v1->getRow(0)->get(0)->getString()+","+v1->getRow(0)->get(1)->getString()+"]");
    VectorSP winMatrix1 = v1->getWindow(0,2,0,1);
    winMatrix1->set(0,0,Util::createShort(0));
    winMatrix1->set(1,0,Util::createShort(1));
    EXPECT_EQ(winMatrix1->getValue(2)->getString(),"["+winMatrix1->getRow(0)->get(0)->getString()+","+winMatrix1->getRow(0)->get(1)->getString()+"]");
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

    EXPECT_FALSE(v1->reshape(2,1));
    
    v1->reshape(1,4);
    EXPECT_EQ(v1->rows(),4);
    EXPECT_EQ(v1->columns(),1);
    v1->reshape(4,1);
    EXPECT_EQ(v1->rows(),1);
    EXPECT_EQ(v1->columns(),4);
    v1->reshape(2,2);
    EXPECT_EQ(v1->rows(),2);
    EXPECT_EQ(v1->columns(),2);
    EXPECT_EQ(v1->getRowLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());
    EXPECT_EQ(v1->getColumnLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());

    EXPECT_EQ(v1->getString(1),"{"+v1->getColumn(1)->get(0)->getString()+","+v1->getColumn(1)->get(1)->getString()+"}");
    EXPECT_ANY_THROW(v1->set(Util::createInt(3),Util::createFloat(1)));
    EXPECT_ANY_THROW(v1->set(Util::createInt(-1),Util::createFloat(1)));
    v1->set(Util::createInt(1),Util::createFloat(1));
    EXPECT_EQ(v1->getColumn(1)->get(1)->getBool(),1);
    VectorSP vals = Util::createVector(DT_FLOAT,0,2);
    VectorSP cols = Util::createVector(DT_INT,0,2);
    cols->append(Util::createInt(0));
    cols->append(Util::createInt(1));
    vals->append(Util::createFloat(0));
    vals->append(Util::createFloat(0));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    vals->append(Util::createFloat(0));
    vals->append(Util::createFloat(0));
    v1->set(cols,vals);
    EXPECT_EQ(v1->getString(),"#0 #1\n-- --\n0  0 \n0  0 \n");
    cols->append(Util::createInt(-1));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    cols->set(2, Util::createInt(3));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    // ConstantSP constCol = Util::createInt(0);
    // EXPECT_EQ(v1->get(constCol)->getString(),"#0\n--\n0 \n0 \n");

    v1->setItem(0,Util::createFloat(1));
    EXPECT_EQ(v1->getItem(0)->getString(),"["+v1->getColumn(0)->get(0)->getString()+","+v1->getColumn(0)->get(1)->getString()+"]");

    VectorSP v2=v1->getInstance(4);
    EXPECT_EQ(v2->rows(),v1->rows());
    EXPECT_EQ(v2->columns(),4);
    VectorSP v3 = v1->getInstance();
    EXPECT_EQ(v3->rows(),v1->rows());
    EXPECT_EQ(v3->columns(),v1->columns());

    cout<<v1->getString()<<endl;
    VectorSP rowlabelvec = Util::createVector(DT_STRING,0,2);
    rowlabelvec->append(Util::createString("lab0"));
    rowlabelvec->append(Util::createString("lab1"));
    EXPECT_ANY_THROW(v1->setRowLabel(Util::createString("lab")));
    v1->setRowLabel(rowlabelvec);
    VectorSP collabelvec = Util::createVector(DT_STRING,0,2);
    collabelvec->append(Util::createString("collab0"));
    collabelvec->append(Util::createString("collab1"));
    EXPECT_ANY_THROW(v1->setColumnLabel(Util::createString("lab")));
    v1->setColumnLabel(collabelvec);
    cout<<v1->getString()<<endl;

    EXPECT_EQ(v1->getRow(0)->getString(),"["+v1->getRow(0)->get(0)->getString()+","+v1->getRow(0)->get(1)->getString()+"]");
    VectorSP winMatrix1 = v1->getWindow(0,2,0,1);
    winMatrix1->set(0,0,Util::createFloat(0));
    winMatrix1->set(1,0,Util::createFloat(1));
    EXPECT_EQ(winMatrix1->getValue(2)->getString(),"["+winMatrix1->getRow(0)->get(0)->getString()+","+winMatrix1->getRow(0)->get(1)->getString()+"]");
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

    EXPECT_FALSE(v1->reshape(2,1));
    
    v1->reshape(1,4);
    EXPECT_EQ(v1->rows(),4);
    EXPECT_EQ(v1->columns(),1);
    v1->reshape(4,1);
    EXPECT_EQ(v1->rows(),1);
    EXPECT_EQ(v1->columns(),4);
    v1->reshape(2,2);
    EXPECT_EQ(v1->rows(),2);
    EXPECT_EQ(v1->columns(),2);
    EXPECT_EQ(v1->getRowLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());
    EXPECT_EQ(v1->getColumnLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());

    EXPECT_EQ(v1->getString(1),"{"+v1->getColumn(1)->get(0)->getString()+","+v1->getColumn(1)->get(1)->getString()+"}");
    EXPECT_ANY_THROW(v1->set(Util::createInt(3),Util::createDouble(1)));
    EXPECT_ANY_THROW(v1->set(Util::createInt(-1),Util::createDouble(1)));
    v1->set(Util::createInt(1),Util::createDouble(1));
    EXPECT_EQ(v1->getColumn(1)->get(1)->getBool(),1);
    VectorSP vals = Util::createVector(DT_DOUBLE,0,2);
    VectorSP cols = Util::createVector(DT_INT,0,2);
    cols->append(Util::createInt(0));
    cols->append(Util::createInt(1));
    vals->append(Util::createDouble(0));
    vals->append(Util::createDouble(0));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    vals->append(Util::createDouble(0));
    vals->append(Util::createDouble(0));
    v1->set(cols,vals);
    EXPECT_EQ(v1->getString(),"#0 #1\n-- --\n0  0 \n0  0 \n");
    cols->append(Util::createInt(-1));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    cols->set(2, Util::createInt(3));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    // ConstantSP constCol = Util::createInt(0);
    // EXPECT_EQ(v1->get(constCol)->getString(),"#0\n--\n0 \n0 \n");

    v1->setItem(0,Util::createDouble(1));
    EXPECT_EQ(v1->getItem(0)->getString(),"["+v1->getColumn(0)->get(0)->getString()+","+v1->getColumn(0)->get(1)->getString()+"]");

    VectorSP v2=v1->getInstance(4);
    EXPECT_EQ(v2->rows(),v1->rows());
    EXPECT_EQ(v2->columns(),4);
    VectorSP v3 = v1->getInstance();
    EXPECT_EQ(v3->rows(),v1->rows());
    EXPECT_EQ(v3->columns(),v1->columns());

    cout<<v1->getString()<<endl;
    VectorSP rowlabelvec = Util::createVector(DT_STRING,0,2);
    rowlabelvec->append(Util::createString("lab0"));
    rowlabelvec->append(Util::createString("lab1"));
    EXPECT_ANY_THROW(v1->setRowLabel(Util::createString("lab")));
    v1->setRowLabel(rowlabelvec);
    VectorSP collabelvec = Util::createVector(DT_STRING,0,2);
    collabelvec->append(Util::createString("collab0"));
    collabelvec->append(Util::createString("collab1"));
    EXPECT_ANY_THROW(v1->setColumnLabel(Util::createString("lab")));
    v1->setColumnLabel(collabelvec);

    EXPECT_EQ(v1->getRow(0)->getString(),"["+v1->getRow(0)->get(0)->getString()+","+v1->getRow(0)->get(1)->getString()+"]");
    VectorSP winMatrix1 = v1->getWindow(0,2,0,1);
    winMatrix1->set(0,0,Util::createDouble(0));
    winMatrix1->set(1,0,Util::createDouble(1));
    EXPECT_EQ(winMatrix1->getValue(2)->getString(),"["+winMatrix1->getRow(0)->get(0)->getString()+","+winMatrix1->getRow(0)->get(1)->getString()+"]");
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

    EXPECT_FALSE(v1->reshape(2,1));
    
    v1->reshape(1,4);
    EXPECT_EQ(v1->rows(),4);
    EXPECT_EQ(v1->columns(),1);
    v1->reshape(4,1);
    EXPECT_EQ(v1->rows(),1);
    EXPECT_EQ(v1->columns(),4);
    v1->reshape(2,2);
    EXPECT_EQ(v1->rows(),2);
    EXPECT_EQ(v1->columns(),2);
    EXPECT_EQ(v1->getRowLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());
    EXPECT_EQ(v1->getColumnLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());

    EXPECT_EQ(v1->getString(1),"{"+v1->getColumn(1)->get(0)->getString()+","+v1->getColumn(1)->get(1)->getString()+"}");
    EXPECT_ANY_THROW(v1->set(Util::createInt(3),Util::createDateHour(1)));
    EXPECT_ANY_THROW(v1->set(Util::createInt(-1),Util::createDateHour(1)));
    v1->set(Util::createInt(1),Util::createDateHour(1));
    EXPECT_EQ(v1->getColumn(1)->get(1)->getBool(),1);
    VectorSP vals = Util::createVector(DT_DATEHOUR,0,2);
    VectorSP cols = Util::createVector(DT_INT,0,2);
    cols->append(Util::createInt(0));
    cols->append(Util::createInt(1));
    vals->append(Util::createDateHour(0));
    vals->append(Util::createDateHour(0));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    vals->append(Util::createDateHour(0));
    vals->append(Util::createDateHour(0));
    v1->set(cols,vals);
    EXPECT_EQ(v1->getString(),"#0            #1           \n------------- -------------\n"+v1->getColumn(0)->get(0)->getString()+" "+v1->getColumn(1)->get(0)->getString()+"\n"+v1->getColumn(0)->get(1)->getString()+" "+v1->getColumn(1)->get(1)->getString()+"\n");
    cols->append(Util::createInt(-1));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    cols->set(2, Util::createInt(3));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    // ConstantSP constCol = Util::createInt(0);
    // EXPECT_EQ(v1->get(constCol)->getString(),"#0\n--\n0 \n0 \n");

    v1->setItem(0,Util::createDateHour(1));
    EXPECT_EQ(v1->getItem(0)->getString(),"["+v1->getColumn(0)->get(0)->getString()+","+v1->getColumn(0)->get(1)->getString()+"]");

    VectorSP v2=v1->getInstance(4);
    EXPECT_EQ(v2->rows(),v1->rows());
    EXPECT_EQ(v2->columns(),4);
    VectorSP v3 = v1->getInstance();
    EXPECT_EQ(v3->rows(),v1->rows());
    EXPECT_EQ(v3->columns(),v1->columns());

    cout<<v1->getString()<<endl;
    VectorSP rowlabelvec = Util::createVector(DT_STRING,0,2);
    rowlabelvec->append(Util::createString("lab0"));
    rowlabelvec->append(Util::createString("lab1"));
    EXPECT_ANY_THROW(v1->setRowLabel(Util::createString("lab")));
    v1->setRowLabel(rowlabelvec);
    VectorSP collabelvec = Util::createVector(DT_STRING,0,2);
    collabelvec->append(Util::createString("collab0"));
    collabelvec->append(Util::createString("collab1"));
    EXPECT_ANY_THROW(v1->setColumnLabel(Util::createString("lab")));
    v1->setColumnLabel(collabelvec);

    EXPECT_EQ(v1->getRow(0)->getString(),"["+v1->getRow(0)->get(0)->getString()+","+v1->getRow(0)->get(1)->getString()+"]");
    VectorSP winMatrix1 = v1->getWindow(0,2,0,1);
    winMatrix1->set(0,0,Util::createDateHour(0));
    winMatrix1->set(1,0,Util::createDateHour(1));
    EXPECT_EQ(winMatrix1->getValue(2)->getString(),"["+winMatrix1->getRow(0)->get(0)->getString()+","+winMatrix1->getRow(0)->get(1)->getString()+"]");
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

    EXPECT_FALSE(v1->reshape(2,1));
    
    v1->reshape(1,4);
    EXPECT_EQ(v1->rows(),4);
    EXPECT_EQ(v1->columns(),1);
    v1->reshape(4,1);
    EXPECT_EQ(v1->rows(),1);
    EXPECT_EQ(v1->columns(),4);
    v1->reshape(2,2);
    EXPECT_EQ(v1->rows(),2);
    EXPECT_EQ(v1->columns(),2);
    EXPECT_EQ(v1->getRowLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());
    EXPECT_EQ(v1->getColumnLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());

    EXPECT_EQ(v1->getString(1),"{"+v1->getColumn(1)->get(0)->getString()+","+v1->getColumn(1)->get(1)->getString()+"}");
    EXPECT_ANY_THROW(v1->set(Util::createInt(3),Util::createDate(1)));
    EXPECT_ANY_THROW(v1->set(Util::createInt(-1),Util::createDate(1)));
    v1->set(Util::createInt(1),Util::createDate(1));
    EXPECT_EQ(v1->getColumn(1)->get(1)->getBool(),1);
    VectorSP vals = Util::createVector(DT_DATE,0,2);
    VectorSP cols = Util::createVector(DT_INT,0,2);
    cols->append(Util::createInt(0));
    cols->append(Util::createInt(1));
    vals->append(Util::createDate(0));
    vals->append(Util::createDate(0));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    vals->append(Util::createDate(0));
    vals->append(Util::createDate(0));
    v1->set(cols,vals);
    EXPECT_EQ(v1->getString(),"#0         #1        \n---------- ----------\n1970.01.01 1970.01.01\n1970.01.01 1970.01.01\n");
    cols->append(Util::createInt(-1));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    cols->set(2, Util::createInt(3));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    // ConstantSP constCol = Util::createInt(0);
    // EXPECT_EQ(v1->get(constCol)->getString(),"#0\n--\n0 \n0 \n");

    v1->setItem(0,Util::createDate(1));
    EXPECT_EQ(v1->getItem(0)->getString(),"["+v1->getColumn(0)->get(0)->getString()+","+v1->getColumn(0)->get(1)->getString()+"]");

    VectorSP v2=v1->getInstance(4);
    EXPECT_EQ(v2->rows(),v1->rows());
    EXPECT_EQ(v2->columns(),4);
    VectorSP v3 = v1->getInstance();
    EXPECT_EQ(v3->rows(),v1->rows());
    EXPECT_EQ(v3->columns(),v1->columns());

    cout<<v1->getString()<<endl;
    VectorSP rowlabelvec = Util::createVector(DT_STRING,0,2);
    rowlabelvec->append(Util::createString("lab0"));
    rowlabelvec->append(Util::createString("lab1"));
    EXPECT_ANY_THROW(v1->setRowLabel(Util::createString("lab")));
    v1->setRowLabel(rowlabelvec);
    VectorSP collabelvec = Util::createVector(DT_STRING,0,2);
    collabelvec->append(Util::createString("collab0"));
    collabelvec->append(Util::createString("collab1"));
    EXPECT_ANY_THROW(v1->setColumnLabel(Util::createString("lab")));
    v1->setColumnLabel(collabelvec);

    EXPECT_EQ(v1->getRow(0)->getString(),"["+v1->getRow(0)->get(0)->getString()+","+v1->getRow(0)->get(1)->getString()+"]");
    VectorSP winMatrix1 = v1->getWindow(0,2,0,1);
    winMatrix1->set(0,0,Util::createDate(0));
    winMatrix1->set(1,0,Util::createDate(1));
    EXPECT_EQ(winMatrix1->getValue(2)->getString(),"["+winMatrix1->getRow(0)->get(0)->getString()+","+winMatrix1->getRow(0)->get(1)->getString()+"]");
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

    EXPECT_FALSE(v1->reshape(2,1));
    
    v1->reshape(1,4);
    EXPECT_EQ(v1->rows(),4);
    EXPECT_EQ(v1->columns(),1);
    v1->reshape(4,1);
    EXPECT_EQ(v1->rows(),1);
    EXPECT_EQ(v1->columns(),4);
    v1->reshape(2,2);
    EXPECT_EQ(v1->rows(),2);
    EXPECT_EQ(v1->columns(),2);
    EXPECT_EQ(v1->getRowLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());
    EXPECT_EQ(v1->getColumnLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());

    EXPECT_EQ(v1->getString(1),"{"+v1->getColumn(1)->get(0)->getString()+","+v1->getColumn(1)->get(1)->getString()+"}");
    EXPECT_ANY_THROW(v1->set(Util::createInt(3),Util::createMinute(1)));
    EXPECT_ANY_THROW(v1->set(Util::createInt(-1),Util::createMinute(1)));
    v1->set(Util::createInt(1),Util::createMinute(1));
    EXPECT_EQ(v1->getColumn(1)->get(1)->getBool(),1);
    VectorSP vals = Util::createVector(DT_MINUTE,0,2);
    VectorSP cols = Util::createVector(DT_INT,0,2);
    cols->append(Util::createInt(0));
    cols->append(Util::createInt(1));
    vals->append(Util::createMinute(0));
    vals->append(Util::createMinute(0));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    vals->append(Util::createMinute(0));
    vals->append(Util::createMinute(0));
    v1->set(cols,vals);
    EXPECT_EQ(v1->getString(),"#0     #1    \n------ ------\n00:00m 00:00m\n00:00m 00:00m\n");
    cols->append(Util::createInt(-1));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    cols->set(2, Util::createInt(3));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    // ConstantSP constCol = Util::createInt(0);
    // EXPECT_EQ(v1->get(constCol)->getString(),"#0\n--\n0 \n0 \n");

    v1->setItem(0,Util::createMinute(1));
    EXPECT_EQ(v1->getItem(0)->getString(),"["+v1->getColumn(0)->get(0)->getString()+","+v1->getColumn(0)->get(1)->getString()+"]");

    VectorSP v2=v1->getInstance(4);
    EXPECT_EQ(v2->rows(),v1->rows());
    EXPECT_EQ(v2->columns(),4);
    VectorSP v3 = v1->getInstance();
    EXPECT_EQ(v3->rows(),v1->rows());
    EXPECT_EQ(v3->columns(),v1->columns());

    cout<<v1->getString()<<endl;
    VectorSP rowlabelvec = Util::createVector(DT_STRING,0,2);
    rowlabelvec->append(Util::createString("lab0"));
    rowlabelvec->append(Util::createString("lab1"));
    EXPECT_ANY_THROW(v1->setRowLabel(Util::createString("lab")));
    v1->setRowLabel(rowlabelvec);
    VectorSP collabelvec = Util::createVector(DT_STRING,0,2);
    collabelvec->append(Util::createString("collab0"));
    collabelvec->append(Util::createString("collab1"));
    EXPECT_ANY_THROW(v1->setColumnLabel(Util::createString("lab")));
    v1->setColumnLabel(collabelvec);

    EXPECT_EQ(v1->getRow(0)->getString(),"["+v1->getRow(0)->get(0)->getString()+","+v1->getRow(0)->get(1)->getString()+"]");
    VectorSP winMatrix1 = v1->getWindow(0,2,0,1);
    winMatrix1->set(0,0,Util::createMinute(0));
    winMatrix1->set(1,0,Util::createMinute(1));
    EXPECT_EQ(winMatrix1->getValue(2)->getString(),"["+winMatrix1->getRow(0)->get(0)->getString()+","+winMatrix1->getRow(0)->get(1)->getString()+"]");
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


TEST_F(DataformMatrixTest,testSecondMatrix){
	VectorSP v1 = Util::createMatrix(DT_SECOND,2,2,4);
    v1->set(0,0, Util::createSecond(1));
    v1->set(1,0, Util::createSecond(1));
	v1->set(0,1, Util::createSecond(0));
	v1->set(1,1, Util::createSecond(0));
	string script = "a=matrix([[second(1),second(0)],[second(1),second(0)]]);a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1",{v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(),true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
    EXPECT_EQ(conn.run("v1")->isMatrix(),true);

    EXPECT_FALSE(v1->reshape(2,1));
    
    v1->reshape(1,4);
    EXPECT_EQ(v1->rows(),4);
    EXPECT_EQ(v1->columns(),1);
    v1->reshape(4,1);
    EXPECT_EQ(v1->rows(),1);
    EXPECT_EQ(v1->columns(),4);
    v1->reshape(2,2);
    EXPECT_EQ(v1->rows(),2);
    EXPECT_EQ(v1->columns(),2);
    EXPECT_EQ(v1->getRowLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());
    EXPECT_EQ(v1->getColumnLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());

    EXPECT_EQ(v1->getString(1),"{"+v1->getColumn(1)->get(0)->getString()+","+v1->getColumn(1)->get(1)->getString()+"}");
    EXPECT_ANY_THROW(v1->set(Util::createInt(3),Util::createSecond(1)));
    EXPECT_ANY_THROW(v1->set(Util::createInt(-1),Util::createSecond(1)));
    v1->set(Util::createInt(1),Util::createSecond(1));
    EXPECT_EQ(v1->getColumn(1)->get(1)->getBool(),1);
    VectorSP vals = Util::createVector(DT_SECOND,0,2);
    VectorSP cols = Util::createVector(DT_INT,0,2);
    cols->append(Util::createInt(0));
    cols->append(Util::createInt(1));
    vals->append(Util::createSecond(0));
    vals->append(Util::createSecond(0));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    vals->append(Util::createSecond(0));
    vals->append(Util::createSecond(0));
    v1->set(cols,vals);
    EXPECT_EQ(v1->getString(),"#0       #1      \n-------- --------\n00:00:00 00:00:00\n00:00:00 00:00:00\n");
    cols->append(Util::createInt(-1));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    cols->set(2, Util::createInt(3));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    // ConstantSP constCol = Util::createInt(0);
    // EXPECT_EQ(v1->get(constCol)->getString(),"#0\n--\n0 \n0 \n");

    v1->setItem(0,Util::createSecond(1));
    EXPECT_EQ(v1->getItem(0)->getString(),"["+v1->getColumn(0)->get(0)->getString()+","+v1->getColumn(0)->get(1)->getString()+"]");

    VectorSP v2=v1->getInstance(4);
    EXPECT_EQ(v2->rows(),v1->rows());
    EXPECT_EQ(v2->columns(),4);
    VectorSP v3 = v1->getInstance();
    EXPECT_EQ(v3->rows(),v1->rows());
    EXPECT_EQ(v3->columns(),v1->columns());

    cout<<v1->getString()<<endl;
    VectorSP rowlabelvec = Util::createVector(DT_STRING,0,2);
    rowlabelvec->append(Util::createString("lab0"));
    rowlabelvec->append(Util::createString("lab1"));
    EXPECT_ANY_THROW(v1->setRowLabel(Util::createString("lab")));
    v1->setRowLabel(rowlabelvec);
    VectorSP collabelvec = Util::createVector(DT_STRING,0,2);
    collabelvec->append(Util::createString("collab0"));
    collabelvec->append(Util::createString("collab1"));
    EXPECT_ANY_THROW(v1->setColumnLabel(Util::createString("lab")));
    v1->setColumnLabel(collabelvec);

    EXPECT_EQ(v1->getRow(0)->getString(),"["+v1->getRow(0)->get(0)->getString()+","+v1->getRow(0)->get(1)->getString()+"]");
    VectorSP winMatrix1 = v1->getWindow(0,2,0,1);
    winMatrix1->set(0,0,Util::createSecond(0));
    winMatrix1->set(1,0,Util::createSecond(1));
    EXPECT_EQ(winMatrix1->getValue(2)->getString(),"["+winMatrix1->getRow(0)->get(0)->getString()+","+winMatrix1->getRow(0)->get(1)->getString()+"]");
}

TEST_F(DataformMatrixTest,testSecondnullMatrix){
	VectorSP v1 = Util::createMatrix(DT_SECOND,2,2,4);
    v1->set(0,0, Util::createNullConstant(DT_SECOND));
    v1->set(1,0, Util::createNullConstant(DT_SECOND));
	v1->set(0,1, Util::createNullConstant(DT_SECOND));
	v1->set(1,1, Util::createNullConstant(DT_SECOND));
	string script = "a=matrix([[second(NULL),second(NULL)],[second(NULL),second(NULL)]]);a";
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

    EXPECT_FALSE(v1->reshape(2,1));
    
    v1->reshape(1,4);
    EXPECT_EQ(v1->rows(),4);
    EXPECT_EQ(v1->columns(),1);
    v1->reshape(4,1);
    EXPECT_EQ(v1->rows(),1);
    EXPECT_EQ(v1->columns(),4);
    v1->reshape(2,2);
    EXPECT_EQ(v1->rows(),2);
    EXPECT_EQ(v1->columns(),2);
    EXPECT_EQ(v1->getRowLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());
    EXPECT_EQ(v1->getColumnLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());

    EXPECT_EQ(v1->getString(1),"{"+v1->getColumn(1)->get(0)->getString()+","+v1->getColumn(1)->get(1)->getString()+"}");
    EXPECT_ANY_THROW(v1->set(Util::createInt(3),Util::createDateTime(1)));
    EXPECT_ANY_THROW(v1->set(Util::createInt(-1),Util::createDateTime(1)));
    v1->set(Util::createInt(1),Util::createDateTime(1));
    EXPECT_EQ(v1->getColumn(1)->get(1)->getBool(),1);
    VectorSP vals = Util::createVector(DT_DATETIME,0,2);
    VectorSP cols = Util::createVector(DT_INT,0,2);
    cols->append(Util::createInt(0));
    cols->append(Util::createInt(1));
    vals->append(Util::createDateTime(0));
    vals->append(Util::createDateTime(0));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    vals->append(Util::createDateTime(0));
    vals->append(Util::createDateTime(0));
    v1->set(cols,vals);
    EXPECT_EQ(v1->getString(),"#0                  #1                 \n------------------- -------------------\n1970.01.01T00:00:00 1970.01.01T00:00:00\n1970.01.01T00:00:00 1970.01.01T00:00:00\n");
    cols->append(Util::createInt(-1));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    cols->set(2, Util::createInt(3));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    // ConstantSP constCol = Util::createInt(0);
    // EXPECT_EQ(v1->get(constCol)->getString(),"#0\n--\n0 \n0 \n");

    v1->setItem(0,Util::createDateTime(1));
    EXPECT_EQ(v1->getItem(0)->getString(),"["+v1->getColumn(0)->get(0)->getString()+","+v1->getColumn(0)->get(1)->getString()+"]");

    VectorSP v2=v1->getInstance(4);
    EXPECT_EQ(v2->rows(),v1->rows());
    EXPECT_EQ(v2->columns(),4);
    VectorSP v3 = v1->getInstance();
    EXPECT_EQ(v3->rows(),v1->rows());
    EXPECT_EQ(v3->columns(),v1->columns());

    cout<<v1->getString()<<endl;
    VectorSP rowlabelvec = Util::createVector(DT_STRING,0,2);
    rowlabelvec->append(Util::createString("lab0"));
    rowlabelvec->append(Util::createString("lab1"));
    EXPECT_ANY_THROW(v1->setRowLabel(Util::createString("lab")));
    v1->setRowLabel(rowlabelvec);
    VectorSP collabelvec = Util::createVector(DT_STRING,0,2);
    collabelvec->append(Util::createString("collab0"));
    collabelvec->append(Util::createString("collab1"));
    EXPECT_ANY_THROW(v1->setColumnLabel(Util::createString("lab")));
    v1->setColumnLabel(collabelvec);

    EXPECT_EQ(v1->getRow(0)->getString(),"["+v1->getRow(0)->get(0)->getString()+","+v1->getRow(0)->get(1)->getString()+"]");
    VectorSP winMatrix1 = v1->getWindow(0,2,0,1);
    winMatrix1->set(0,0,Util::createDateTime(0));
    winMatrix1->set(1,0,Util::createDateTime(1));
    EXPECT_EQ(winMatrix1->getValue(2)->getString(),"["+winMatrix1->getRow(0)->get(0)->getString()+","+winMatrix1->getRow(0)->get(1)->getString()+"]");
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

    EXPECT_FALSE(v1->reshape(2,1));
    
    v1->reshape(1,4);
    EXPECT_EQ(v1->rows(),4);
    EXPECT_EQ(v1->columns(),1);
    v1->reshape(4,1);
    EXPECT_EQ(v1->rows(),1);
    EXPECT_EQ(v1->columns(),4);
    v1->reshape(2,2);
    EXPECT_EQ(v1->rows(),2);
    EXPECT_EQ(v1->columns(),2);
    EXPECT_EQ(v1->getRowLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());
    EXPECT_EQ(v1->getColumnLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());

    EXPECT_EQ(v1->getString(1),"{"+v1->getColumn(1)->get(0)->getString()+","+v1->getColumn(1)->get(1)->getString()+"}");
    EXPECT_ANY_THROW(v1->set(Util::createInt(3),Util::createTimestamp(1)));
    EXPECT_ANY_THROW(v1->set(Util::createInt(-1),Util::createTimestamp(1)));
    v1->set(Util::createInt(1),Util::createTimestamp(1));
    EXPECT_EQ(v1->getColumn(1)->get(1)->getBool(),1);
    VectorSP vals = Util::createVector(DT_TIMESTAMP,0,2);
    VectorSP cols = Util::createVector(DT_INT,0,2);
    cols->append(Util::createInt(0));
    cols->append(Util::createInt(1));
    vals->append(Util::createTimestamp(0));
    vals->append(Util::createTimestamp(0));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    vals->append(Util::createTimestamp(0));
    vals->append(Util::createTimestamp(0));
    v1->set(cols,vals);
    EXPECT_EQ(v1->getString(),"#0                      #1                     \n----------------------- -----------------------\n1970.01.01T00:00:00.000 1970.01.01T00:00:00.000\n1970.01.01T00:00:00.000 1970.01.01T00:00:00.000\n");
    cols->append(Util::createInt(-1));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    cols->set(2, Util::createInt(3));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    // ConstantSP constCol = Util::createInt(0);
    // EXPECT_EQ(v1->get(constCol)->getString(),"#0\n--\n0 \n0 \n");

    v1->setItem(0,Util::createTimestamp(1));
    EXPECT_EQ(v1->getItem(0)->getString(),"["+v1->getColumn(0)->get(0)->getString()+","+v1->getColumn(0)->get(1)->getString()+"]");

    VectorSP v2=v1->getInstance(4);
    EXPECT_EQ(v2->rows(),v1->rows());
    EXPECT_EQ(v2->columns(),4);
    VectorSP v3 = v1->getInstance();
    EXPECT_EQ(v3->rows(),v1->rows());
    EXPECT_EQ(v3->columns(),v1->columns());

    cout<<v1->getString()<<endl;
    VectorSP rowlabelvec = Util::createVector(DT_STRING,0,2);
    rowlabelvec->append(Util::createString("lab0"));
    rowlabelvec->append(Util::createString("lab1"));
    EXPECT_ANY_THROW(v1->setRowLabel(Util::createString("lab")));
    v1->setRowLabel(rowlabelvec);
    VectorSP collabelvec = Util::createVector(DT_STRING,0,2);
    collabelvec->append(Util::createString("collab0"));
    collabelvec->append(Util::createString("collab1"));
    EXPECT_ANY_THROW(v1->setColumnLabel(Util::createString("lab")));
    v1->setColumnLabel(collabelvec);

    EXPECT_EQ(v1->getRow(0)->getString(),"["+v1->getRow(0)->get(0)->getString()+","+v1->getRow(0)->get(1)->getString()+"]");
    VectorSP winMatrix1 = v1->getWindow(0,2,0,1);
    winMatrix1->set(0,0,Util::createTimestamp(0));
    winMatrix1->set(1,0,Util::createTimestamp(1));
    EXPECT_EQ(winMatrix1->getValue(2)->getString(),"["+winMatrix1->getRow(0)->get(0)->getString()+","+winMatrix1->getRow(0)->get(1)->getString()+"]");
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

    EXPECT_FALSE(v1->reshape(2,1));
    
    v1->reshape(1,4);
    EXPECT_EQ(v1->rows(),4);
    EXPECT_EQ(v1->columns(),1);
    v1->reshape(4,1);
    EXPECT_EQ(v1->rows(),1);
    EXPECT_EQ(v1->columns(),4);
    v1->reshape(2,2);
    EXPECT_EQ(v1->rows(),2);
    EXPECT_EQ(v1->columns(),2);
    EXPECT_EQ(v1->getRowLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());
    EXPECT_EQ(v1->getColumnLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());

    EXPECT_EQ(v1->getString(1),"{"+v1->getColumn(1)->get(0)->getString()+","+v1->getColumn(1)->get(1)->getString()+"}");
    EXPECT_ANY_THROW(v1->set(Util::createInt(3),Util::createNanoTime(1)));
    EXPECT_ANY_THROW(v1->set(Util::createInt(-1),Util::createNanoTime(1)));
    v1->set(Util::createInt(1),Util::createNanoTime(1));
    EXPECT_EQ(v1->getColumn(1)->get(1)->getBool(),1);
    VectorSP vals = Util::createVector(DT_NANOTIME,0,2);
    VectorSP cols = Util::createVector(DT_INT,0,2);
    cols->append(Util::createInt(0));
    cols->append(Util::createInt(1));
    vals->append(Util::createNanoTime(0));
    vals->append(Util::createNanoTime(0));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    vals->append(Util::createNanoTime(0));
    vals->append(Util::createNanoTime(0));
    v1->set(cols,vals);
    EXPECT_EQ(v1->getString(),"#0                 #1                \n------------------ ------------------\n00:00:00.000000000 00:00:00.000000000\n00:00:00.000000000 00:00:00.000000000\n");
    cols->append(Util::createInt(-1));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    cols->set(2, Util::createInt(3));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    // ConstantSP constCol = Util::createInt(0);
    // EXPECT_EQ(v1->get(constCol)->getString(),"#0\n--\n0 \n0 \n");

    v1->setItem(0,Util::createNanoTime(1));
    EXPECT_EQ(v1->getItem(0)->getString(),"["+v1->getColumn(0)->get(0)->getString()+","+v1->getColumn(0)->get(1)->getString()+"]");

    VectorSP v2=v1->getInstance(4);
    EXPECT_EQ(v2->rows(),v1->rows());
    EXPECT_EQ(v2->columns(),4);
    VectorSP v3 = v1->getInstance();
    EXPECT_EQ(v3->rows(),v1->rows());
    EXPECT_EQ(v3->columns(),v1->columns());

    cout<<v1->getString()<<endl;
    VectorSP rowlabelvec = Util::createVector(DT_STRING,0,2);
    rowlabelvec->append(Util::createString("lab0"));
    rowlabelvec->append(Util::createString("lab1"));
    EXPECT_ANY_THROW(v1->setRowLabel(Util::createString("lab")));
    v1->setRowLabel(rowlabelvec);
    VectorSP collabelvec = Util::createVector(DT_STRING,0,2);
    collabelvec->append(Util::createString("collab0"));
    collabelvec->append(Util::createString("collab1"));
    EXPECT_ANY_THROW(v1->setColumnLabel(Util::createString("lab")));
    v1->setColumnLabel(collabelvec);

    EXPECT_EQ(v1->getRow(0)->getString(),"["+v1->getRow(0)->get(0)->getString()+","+v1->getRow(0)->get(1)->getString()+"]");
    VectorSP winMatrix1 = v1->getWindow(0,2,0,1);
    winMatrix1->set(0,0,Util::createNanoTime(0));
    winMatrix1->set(1,0,Util::createNanoTime(1));
    EXPECT_EQ(winMatrix1->getValue(2)->getString(),"["+winMatrix1->getRow(0)->get(0)->getString()+","+winMatrix1->getRow(0)->get(1)->getString()+"]");
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

    EXPECT_FALSE(v1->reshape(2,1));
    
    v1->reshape(1,4);
    EXPECT_EQ(v1->rows(),4);
    EXPECT_EQ(v1->columns(),1);
    v1->reshape(4,1);
    EXPECT_EQ(v1->rows(),1);
    EXPECT_EQ(v1->columns(),4);
    v1->reshape(2,2);
    EXPECT_EQ(v1->rows(),2);
    EXPECT_EQ(v1->columns(),2);
    EXPECT_EQ(v1->getRowLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());
    EXPECT_EQ(v1->getColumnLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());

    EXPECT_EQ(v1->getString(1),"{"+v1->getColumn(1)->get(0)->getString()+","+v1->getColumn(1)->get(1)->getString()+"}");
    EXPECT_ANY_THROW(v1->set(Util::createInt(3),Util::createNanoTimestamp(1)));
    EXPECT_ANY_THROW(v1->set(Util::createInt(-1),Util::createNanoTimestamp(1)));
    v1->set(Util::createInt(1),Util::createNanoTimestamp(1));
    EXPECT_EQ(v1->getColumn(1)->get(1)->getBool(),1);
    VectorSP vals = Util::createVector(DT_NANOTIMESTAMP,0,2);
    VectorSP cols = Util::createVector(DT_INT,0,2);
    cols->append(Util::createInt(0));
    cols->append(Util::createInt(1));
    vals->append(Util::createNanoTimestamp(0));
    vals->append(Util::createNanoTimestamp(0));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    vals->append(Util::createNanoTimestamp(0));
    vals->append(Util::createNanoTimestamp(0));
    v1->set(cols,vals);
    EXPECT_EQ(v1->getString(),"#0                        #1                       \n------------------------- -------------------------\n1970.01.01T00:00:00.000...1970.01.01T00:00:00.00...\n1970.01.01T00:00:00.000...1970.01.01T00:00:00.00...\n");
    cols->append(Util::createInt(-1));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    cols->set(2, Util::createInt(3));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    // ConstantSP constCol = Util::createInt(0);
    // EXPECT_EQ(v1->get(constCol)->getString(),"#0\n--\n0 \n0 \n");

    v1->setItem(0,Util::createNanoTimestamp(1));
    EXPECT_EQ(v1->getItem(0)->getString(),"["+v1->getColumn(0)->get(0)->getString()+","+v1->getColumn(0)->get(1)->getString()+"]");

    VectorSP v2=v1->getInstance(4);
    EXPECT_EQ(v2->rows(),v1->rows());
    EXPECT_EQ(v2->columns(),4);
    VectorSP v3 = v1->getInstance();
    EXPECT_EQ(v3->rows(),v1->rows());
    EXPECT_EQ(v3->columns(),v1->columns());

    cout<<v1->getString()<<endl;
    VectorSP rowlabelvec = Util::createVector(DT_STRING,0,2);
    rowlabelvec->append(Util::createString("lab0"));
    rowlabelvec->append(Util::createString("lab1"));
    EXPECT_ANY_THROW(v1->setRowLabel(Util::createString("lab")));
    v1->setRowLabel(rowlabelvec);
    VectorSP collabelvec = Util::createVector(DT_STRING,0,2);
    collabelvec->append(Util::createString("collab0"));
    collabelvec->append(Util::createString("collab1"));
    EXPECT_ANY_THROW(v1->setColumnLabel(Util::createString("lab")));
    v1->setColumnLabel(collabelvec);

    EXPECT_EQ(v1->getRow(0)->getString(),"["+v1->getRow(0)->get(0)->getString()+","+v1->getRow(0)->get(1)->getString()+"]");
    VectorSP winMatrix1 = v1->getWindow(0,2,0,1);
    winMatrix1->set(0,0,Util::createNanoTimestamp(0));
    winMatrix1->set(1,0,Util::createNanoTimestamp(1));
    EXPECT_EQ(winMatrix1->getValue(2)->getString(),"["+winMatrix1->getRow(0)->get(0)->getString()+","+winMatrix1->getRow(0)->get(1)->getString()+"]");
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

    EXPECT_FALSE(v1->reshape(2,1));
    
    v1->reshape(1,4);
    EXPECT_EQ(v1->rows(),4);
    EXPECT_EQ(v1->columns(),1);
    v1->reshape(4,1);
    EXPECT_EQ(v1->rows(),1);
    EXPECT_EQ(v1->columns(),4);
    v1->reshape(2,2);
    EXPECT_EQ(v1->rows(),2);
    EXPECT_EQ(v1->columns(),2);
    EXPECT_EQ(v1->getRowLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());
    EXPECT_EQ(v1->getColumnLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());

    EXPECT_EQ(v1->getString(1),"{"+v1->getColumn(1)->get(0)->getString()+","+v1->getColumn(1)->get(1)->getString()+"}");
    EXPECT_ANY_THROW(v1->set(Util::createInt(3),Util::createMonth(1)));
    EXPECT_ANY_THROW(v1->set(Util::createInt(-1),Util::createMonth(1)));
    v1->set(Util::createInt(1),Util::createMonth(1));
    EXPECT_EQ(v1->getColumn(1)->get(1)->getBool(),1);
    VectorSP vals = Util::createVector(DT_MONTH,0,2);
    VectorSP cols = Util::createVector(DT_INT,0,2);
    cols->append(Util::createInt(0));
    cols->append(Util::createInt(1));
    vals->append(Util::createMonth(0));
    vals->append(Util::createMonth(0));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    vals->append(Util::createMonth(0));
    vals->append(Util::createMonth(0));
    v1->set(cols,vals);
    EXPECT_EQ(v1->getString(),"#0       #1      \n-------- --------\n0000.01M 0000.01M\n0000.01M 0000.01M\n");
    cols->append(Util::createInt(-1));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    cols->set(2, Util::createInt(3));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    // ConstantSP constCol = Util::createInt(0);
    // EXPECT_EQ(v1->get(constCol)->getString(),"#0\n--\n0 \n0 \n");

    v1->setItem(0,Util::createMonth(1));
    EXPECT_EQ(v1->getItem(0)->getString(),"["+v1->getColumn(0)->get(0)->getString()+","+v1->getColumn(0)->get(1)->getString()+"]");

    VectorSP v2=v1->getInstance(4);
    EXPECT_EQ(v2->rows(),v1->rows());
    EXPECT_EQ(v2->columns(),4);
    VectorSP v3 = v1->getInstance();
    EXPECT_EQ(v3->rows(),v1->rows());
    EXPECT_EQ(v3->columns(),v1->columns());

    cout<<v1->getString()<<endl;
    VectorSP rowlabelvec = Util::createVector(DT_STRING,0,2);
    rowlabelvec->append(Util::createString("lab0"));
    rowlabelvec->append(Util::createString("lab1"));
    EXPECT_ANY_THROW(v1->setRowLabel(Util::createString("lab")));
    v1->setRowLabel(rowlabelvec);
    VectorSP collabelvec = Util::createVector(DT_STRING,0,2);
    collabelvec->append(Util::createString("collab0"));
    collabelvec->append(Util::createString("collab1"));
    EXPECT_ANY_THROW(v1->setColumnLabel(Util::createString("lab")));
    v1->setColumnLabel(collabelvec);

    EXPECT_EQ(v1->getRow(0)->getString(),"["+v1->getRow(0)->get(0)->getString()+","+v1->getRow(0)->get(1)->getString()+"]");
    VectorSP winMatrix1 = v1->getWindow(0,2,0,1);
    winMatrix1->set(0,0,Util::createMonth(0));
    winMatrix1->set(1,0,Util::createMonth(1));
    EXPECT_EQ(winMatrix1->getValue(2)->getString(),"["+winMatrix1->getRow(0)->get(0)->getString()+","+winMatrix1->getRow(0)->get(1)->getString()+"]");
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

    EXPECT_FALSE(v1->reshape(2,1));
    
    v1->reshape(1,4);
    EXPECT_EQ(v1->rows(),4);
    EXPECT_EQ(v1->columns(),1);
    v1->reshape(4,1);
    EXPECT_EQ(v1->rows(),1);
    EXPECT_EQ(v1->columns(),4);
    v1->reshape(2,2);
    EXPECT_EQ(v1->rows(),2);
    EXPECT_EQ(v1->columns(),2);
    EXPECT_EQ(v1->getRowLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());
    EXPECT_EQ(v1->getColumnLabel()->getString(),Util::createNullConstant(DT_STRING)->getString());

    EXPECT_EQ(v1->getString(1),"{"+v1->getColumn(1)->get(0)->getString()+","+v1->getColumn(1)->get(1)->getString()+"}");
    EXPECT_ANY_THROW(v1->set(Util::createInt(3),Util::createTime(1)));
    EXPECT_ANY_THROW(v1->set(Util::createInt(-1),Util::createTime(1)));
    v1->set(Util::createInt(1),Util::createTime(1));
    EXPECT_EQ(v1->getColumn(1)->get(1)->getBool(),1);
    VectorSP vals = Util::createVector(DT_TIME,0,2);
    VectorSP cols = Util::createVector(DT_INT,0,2);
    cols->append(Util::createInt(0));
    cols->append(Util::createInt(1));
    vals->append(Util::createTime(0));
    vals->append(Util::createTime(0));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    vals->append(Util::createTime(0));
    vals->append(Util::createTime(0));
    v1->set(cols,vals);
    EXPECT_EQ(v1->getString(), "#0           #1          \n------------ ------------\n00:00:00.000 00:00:00.000\n00:00:00.000 00:00:00.000\n");
    cols->append(Util::createInt(-1));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    cols->set(2, Util::createInt(3));
    EXPECT_ANY_THROW(v1->set(cols,vals));
    // ConstantSP constCol = Util::createInt(0);
    // EXPECT_EQ(v1->get(constCol)->getString(),"#0\n--\n0 \n0 \n");

    v1->setItem(0,Util::createTime(1));
    EXPECT_EQ(v1->getItem(0)->getString(),"["+v1->getColumn(0)->get(0)->getString()+","+v1->getColumn(0)->get(1)->getString()+"]");

    VectorSP v2=v1->getInstance(4);
    EXPECT_EQ(v2->rows(),v1->rows());
    EXPECT_EQ(v2->columns(),4);
    VectorSP v3 = v1->getInstance();
    EXPECT_EQ(v3->rows(),v1->rows());
    EXPECT_EQ(v3->columns(),v1->columns());

    cout<<v1->getString()<<endl;
    VectorSP rowlabelvec = Util::createVector(DT_STRING,0,2);
    rowlabelvec->append(Util::createString("lab0"));
    rowlabelvec->append(Util::createString("lab1"));
    EXPECT_ANY_THROW(v1->setRowLabel(Util::createString("lab")));
    v1->setRowLabel(rowlabelvec);
    VectorSP collabelvec = Util::createVector(DT_STRING,0,2);
    collabelvec->append(Util::createString("collab0"));
    collabelvec->append(Util::createString("collab1"));
    EXPECT_ANY_THROW(v1->setColumnLabel(Util::createString("lab")));
    v1->setColumnLabel(collabelvec);

    EXPECT_EQ(v1->getRow(0)->getString(),"["+v1->getRow(0)->get(0)->getString()+","+v1->getRow(0)->get(1)->getString()+"]");
    VectorSP winMatrix1 = v1->getWindow(0,2,0,1);
    winMatrix1->set(0,0,Util::createTime(0));
    winMatrix1->set(1,0,Util::createTime(1));
    EXPECT_EQ(winMatrix1->getValue(2)->getString(),"["+winMatrix1->getRow(0)->get(0)->getString()+","+winMatrix1->getRow(0)->get(1)->getString()+"]");
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