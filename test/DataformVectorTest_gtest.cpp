
class DataformVectorTest : public testing::Test
{
protected:
	// Suite
	static void SetUpTestCase()
	{
		// DBConnection conn;
		conn.initialize();
		bool ret = conn.connect(hostName, port, "admin", "123456");
		if (!ret)
		{
			cout << "Failed to connect to the server" << endl;
		}
		else
		{
			cout << "connect to " + hostName + ":" + std::to_string(port) << endl;
		}
	}
	static void TearDownTestCase()
	{
		conn.close();
	}

	// Case
	virtual void SetUp()
	{
		cout << "check connect...";
		ConstantSP res = conn.run("1+1");

		cout << "ok" << endl;
	}
	virtual void TearDown()
	{
		conn.run("undef all;");
	}
};

#ifndef WINDOWS
TEST_F(DataformVectorTest, testSymbolVector_get)
{
	ConstantSP v1 = Util::createVector(DT_SYMBOL, 0, 2);
	((VectorSP)v1)->append(Util::createString("0"));
	((VectorSP)v1)->append(Util::createString("1"));
	EXPECT_TRUE(v1->size() == 2);

	ConstantSP value1 = v1->get(Util::createInt(0));
	EXPECT_EQ(value1->getString(), "0");

	ConstantSP value2 = v1->get(Util::createInt(1));
	EXPECT_EQ(value2->getString(), "1");

	ConstantSP value3 = v1->get(Util::createInt(2));
	EXPECT_TRUE(value3.isNull());
}

TEST_F(DataformVectorTest, testSymbolVector_has)
{
	ConstantSP v1 = Util::createVector(DT_SYMBOL, 0, 2);
	((VectorSP)v1)->append(Util::createString("0"));
	((VectorSP)v1)->append(Util::createString("1"));
	EXPECT_TRUE(v1->size() == 2);
	SmartPointer<FastSymbolVector> symbolVector = v1;

	EXPECT_TRUE(symbolVector->has("0"));
	EXPECT_FALSE(symbolVector->has("2"));
}

TEST_F(DataformVectorTest, testSymbolVector_search)
{
	ConstantSP v1 = Util::createVector(DT_SYMBOL, 0, 2);
	((VectorSP)v1)->append(Util::createString("0"));
	((VectorSP)v1)->append(Util::createString("1"));
	EXPECT_TRUE(v1->size() == 2);
	SmartPointer<FastSymbolVector> symbolVector = v1;

	EXPECT_EQ(symbolVector->search("0"), 0);
	EXPECT_EQ(symbolVector->search("1"), 1);
	EXPECT_EQ(symbolVector->search("2"), -1);
}

TEST_F(DataformVectorTest, testIntVector_has)
{
	ConstantSP v1 = Util::createVector(DT_INT, 0, 2);
	((VectorSP)v1)->append(Util::createInt(0));
	((VectorSP)v1)->append(Util::createInt(1));
	EXPECT_TRUE(v1->size() == 2);
	SmartPointer<FastIntVector> intVector = v1;

	EXPECT_TRUE(intVector->has(0));
	EXPECT_FALSE(intVector->has(2));
}

TEST_F(DataformVectorTest, testIntVector_search)
{
	ConstantSP v1 = Util::createVector(DT_INT, 0, 2);
	((VectorSP)v1)->append(Util::createInt(0));
	((VectorSP)v1)->append(Util::createInt(1));
	EXPECT_TRUE(v1->size() == 2);
	SmartPointer<FastIntVector> intVector = v1;

	EXPECT_EQ(intVector->search(0), 0);
	EXPECT_EQ(intVector->search(1), 1);
	EXPECT_EQ(intVector->search(2), -1);
}

TEST_F(DataformVectorTest, testFixedLengthVector_resize)
{
	VectorSP vec = Util::createVector(DT_UUID, 4, 4);

	vec->resize(-1);
	EXPECT_EQ(vec->size(), 4);

	vec->resize(5);
	EXPECT_EQ(vec->size(), 5);
}

TEST_F(DataformVectorTest, testFixedLengthVector_reserve)
{
	VectorSP vec = Util::createVector(DT_UUID, 4, 4);
	EXPECT_EQ(vec->reserve(3), 4);
}

// assign不同长度的vector会fail
TEST_F(DataformVectorTest, testAnyVector_assign_fail)
{
	VectorSP v1 = Util::createVector(DT_ANY, 0, 2);
	v1->append(Util::createInt(1));
	v1->append(Util::createInt(2));
	VectorSP assignVector = Util::createVector(DT_INT, 3, 3);
	EXPECT_FALSE(v1->assign(assignVector));
}

// AnyVector::getValue()
TEST_F(DataformVectorTest, testAnyVector_getValue)
{
	ConstantSP v1 = Util::createVector(DT_ANY, 0, 2);
	((VectorSP)v1)->append(Util::createInt(1));
	((VectorSP)v1)->append(Util::createInt(2));
	ConstantSP value = v1->getValue();

	EXPECT_EQ(v1->getForm(), value->getForm());
	EXPECT_EQ(value->getInt(0), 1);
	EXPECT_EQ(v1->get(0), value->get(0));
	EXPECT_EQ(value->getInt(1), 2);
	EXPECT_EQ(v1->get(1), value->get(1));
}

// // getSubVector函数有bug
TEST_F(DataformVectorTest, testAnyVector_getSubVector)
{
	ConstantSP v1 = Util::createVector(DT_ANY, 0, 2);
	((VectorSP)v1)->append(Util::createInt(1));
	((VectorSP)v1)->append(Util::createInt(2));
	EXPECT_TRUE(v1->size() == 2);

	ConstantSP v2 = ((VectorSP)v1)->getSubVector(-1, 2);
	EXPECT_TRUE(v2->size() == 0);

	ConstantSP v3 = ((VectorSP)v1)->getSubVector(2, 2);
	EXPECT_TRUE(v3->size() == 0);

	ConstantSP v4 = ((VectorSP)v1)->getSubVector(0, 3);
	EXPECT_TRUE(v4->size() == 0);

	ConstantSP v5 = ((VectorSP)v1)->getSubVector(0, 2);
	EXPECT_EQ(v5->size(), 2);
	EXPECT_TRUE(v5->getInt(0) == 1);
	EXPECT_TRUE(v5->getInt(1) == 2);

	ConstantSP v6 = ((VectorSP)v1)->getSubVector(1, -1);
	EXPECT_EQ(v6->size(), 1);
	EXPECT_EQ(v6->getInt(0), 2);
}

TEST_F(DataformVectorTest, testAnyVector_get)
{
	ConstantSP v1 = Util::createVector(DT_ANY, 0, 2);
	((VectorSP)v1)->append(Util::createInt(0));
	((VectorSP)v1)->append(Util::createInt(1));
	EXPECT_TRUE(v1->size() == 2);

	ConstantSP value1 = v1->get(Util::createInt(0));
	EXPECT_EQ(value1->getInt(), 0);

	ConstantSP value2 = v1->get(Util::createInt(1));
	EXPECT_EQ(value2->getInt(), 1);

	ConstantSP value3 = v1->get(Util::createInt(2));
	EXPECT_TRUE(value3.isNull());

	ConstantSP v2 = v1->getValue();
	ConstantSP value4 = v1->get(v2);
	EXPECT_EQ(value4->getInt(0), 0);
	EXPECT_EQ(value4->getInt(1), 1);
}

TEST_F(DataformVectorTest, testAnyVector_append_fail)
{
	ConstantSP v1 = Util::createVector(DT_ANY, Util::MAX_LENGTH_FOR_ANY_VECTOR, Util::MAX_LENGTH_FOR_ANY_VECTOR);
	EXPECT_FALSE(((VectorSP)v1)->append(Util::createInt(0)));
}

TEST_F(DataformVectorTest, testAnyVector_isHomogeneousScalar)
{
	ConstantSP v1 = Util::createVector(DT_ANY, 0, 3);
	SmartPointer<AnyVector> anyVector = v1;
	DATA_TYPE type;
	EXPECT_FALSE(anyVector->isHomogeneousScalar(type));

	anyVector->append(Util::createVector(DT_INT, 0, 3));
	EXPECT_FALSE(anyVector->isHomogeneousScalar(type));

	v1 = Util::createVector(DT_ANY, 0, 3);
	anyVector = v1;
	anyVector->append(Util::createInt(0));
	anyVector->append(Util::createDouble(0));
	EXPECT_FALSE(anyVector->isHomogeneousScalar(type));
	EXPECT_EQ(type, DT_INT);

	v1 = Util::createVector(DT_ANY, 0, 3);
	anyVector = v1;
	anyVector->append(Util::createInt(0));
	anyVector->append(Util::createVector(DT_INT, 0, 3));
	EXPECT_FALSE(anyVector->isHomogeneousScalar(type));

	v1 = Util::createVector(DT_ANY, 0, 3);
	anyVector = v1;
	anyVector->append(Util::createInt(0));
	anyVector->append(Util::createInt(1));
	EXPECT_TRUE(anyVector->isHomogeneousScalar(type));
}

TEST_F(DataformVectorTest, testAnyVector_isTabular)
{
	ConstantSP v1 = Util::createVector(DT_ANY, 0, 3);
	SmartPointer<AnyVector> anyVector = v1;
	EXPECT_FALSE(anyVector->isTabular());

	anyVector->append(Util::createVector(DT_INT, 0, 3));
	anyVector->append(Util::createVector(DT_INT, 0, 3));
	EXPECT_TRUE(anyVector->isTabular());

	anyVector->append(Util::createInt(0));
	EXPECT_FALSE(anyVector->isTabular());

	v1 = Util::createVector(DT_ANY, 0, 3);
	SmartPointer<AnyVector> anyVector2 = v1;
	anyVector2->append(anyVector);
	EXPECT_FALSE(anyVector2->isTabular());
}

TEST_F(DataformVectorTest, testAnyVector_convertToRegularVector)
{
	ConstantSP v1 = Util::createVector(DT_ANY, 0, 3);
	SmartPointer<AnyVector> anyVector = v1;
	ConstantSP regularVector = anyVector->convertToRegularVector();
	EXPECT_TRUE(regularVector->isNull());

	anyVector->append(Util::createInt(0));
	regularVector = anyVector->convertToRegularVector();
	EXPECT_EQ(regularVector->size(), 1);
	EXPECT_EQ(regularVector->getInt(0), 0);

	anyVector->append(Util::createInt(1));
	regularVector = anyVector->convertToRegularVector();
	EXPECT_EQ(regularVector->size(), 2);
	EXPECT_EQ(regularVector->getInt(1), 1);
}

TEST_F(DataformVectorTest, testStringVector_search)
{
	VectorSP v1 = Util::createVector(DT_STRING, 2, 2);
	v1->set(0, Util::createString("1"));
	v1->set(1, Util::createString("2"));
	SmartPointer<StringVector> stringVec = v1;
	EXPECT_EQ(stringVec->search("1"), 0);
	EXPECT_EQ(stringVec->search("2"), 1);
	EXPECT_EQ(stringVec->search("3"), -1);
}

TEST_F(DataformVectorTest, testStringVector_getSubVector)
{
	VectorSP v1 = Util::createVector(DT_STRING, 2, 2);
	v1->set(0, Util::createString("1"));
	v1->set(1, Util::createString("2"));
	SmartPointer<StringVector> stringVec = v1;
	ConstantSP subVector = stringVec->getSubVector(-1, 2, 2);
	EXPECT_EQ(subVector->size(), 0);

	subVector = stringVec->getSubVector(2, 2, 2);
	EXPECT_EQ(subVector->size(), 0);

	subVector = stringVec->getSubVector(0, 3, 2);
	EXPECT_EQ(subVector->size(), 0);
}

TEST_F(DataformVectorTest, testStringVector_get)
{
	VectorSP v1 = Util::createVector(DT_STRING, 2, 2);
	v1->set(0, Util::createString("1"));
	v1->set(1, Util::createString("2"));
	SmartPointer<StringVector> stringVec = v1;
	ConstantSP res = stringVec->get(Util::createInt(0));
	EXPECT_EQ(res->getString(), "1");

	res = stringVec->get(Util::createInt(2));
	EXPECT_EQ(res->getString(), "");

	ConstantSP indexArray = Util::createVector(DT_INT, 2, 2);
	indexArray->set(0, Util::createInt(0));
	indexArray->set(1, Util::createInt(1));
	res = stringVec->get(indexArray);
	EXPECT_EQ(res->getString(0), "1");
	EXPECT_EQ(res->getString(1), "2");
}

TEST_F(DataformVectorTest, testStringVector_remove)
{
	VectorSP v1 = Util::createVector(DT_STRING, 3, 3);
	v1->set(0, Util::createString("1"));
	v1->set(1, Util::createString("2"));
	v1->set(2, Util::createString("3"));
	SmartPointer<StringVector> stringVec = v1;
	stringVec->remove(1);
	EXPECT_EQ(stringVec->size(), 2);
	EXPECT_EQ(stringVec->getString(0), "1");
	EXPECT_EQ(stringVec->getString(1), "2");

	stringVec->remove(-1);
	EXPECT_EQ(stringVec->size(), 1);
	EXPECT_EQ(stringVec->getString(0), "2");
}

TEST_F(DataformVectorTest, testBoolVector_get)
{
	VectorSP v1 = Util::createVector(DT_BOOL, 2, 2);
	v1->set(0, Util::createBool(0));
	v1->set(1, Util::createBool(1));
	SmartPointer<FastBoolVector> boolVector = v1;
	ConstantSP res = boolVector->get(Util::createInt(1));
	EXPECT_EQ(res->getBool(), true);

	res = boolVector->get(Util::createInt(10));
	EXPECT_TRUE(res.isNull());

	ConstantSP indexArray = Util::createVector(DT_INT, 2, 2);
	indexArray->set(0, Util::createInt(0));
	indexArray->set(1, Util::createInt(1));
	res = boolVector->get(indexArray);
	EXPECT_EQ(res->getBool(0), false);
	EXPECT_EQ(res->getBool(1), true);
}

TEST_F(DataformVectorTest, testBoolVector_compare)
{
	VectorSP v1 = Util::createVector(DT_BOOL, 2, 2);
	ConstantSP bool0 = Util::createBool(0);
	ConstantSP bool1 = Util::createBool(1);
	v1->set(0, bool0);
	v1->set(1, bool1);
	SmartPointer<FastBoolVector> boolVector = v1;
	EXPECT_EQ(boolVector->compare(0, bool1), -1);
	EXPECT_EQ(boolVector->compare(1, bool0), 1);
}

TEST_F(DataformVectorTest, testCharVector_get)
{
	VectorSP v1 = Util::createVector(DT_CHAR, 2, 2);
	v1->set(0, Util::createChar('0'));
	v1->set(1, Util::createChar('1'));
	SmartPointer<FastCharVector> charVector = v1;
	ConstantSP res = charVector->get(Util::createInt(1));
	EXPECT_EQ(res->getChar(), '1');

	res = charVector->get(Util::createInt(10));
	EXPECT_TRUE(res.isNull());

	ConstantSP indexArray = Util::createVector(DT_INT, 2, 2);
	indexArray->set(0, Util::createInt(0));
	indexArray->set(1, Util::createInt(1));
	res = charVector->get(indexArray);
	EXPECT_EQ(res->getChar(0), '0');
	EXPECT_EQ(res->getChar(1), '1');
}

TEST_F(DataformVectorTest, testCharVector_compare)
{
	VectorSP v1 = Util::createVector(DT_CHAR, 2, 2);
	ConstantSP char0 = Util::createChar('0');
	ConstantSP char1 = Util::createChar('1');
	v1->set(0, char0);
	v1->set(1, char1);
	SmartPointer<FastCharVector> charVector = v1;
	EXPECT_EQ(charVector->compare(0, char0), 0);
	EXPECT_EQ(charVector->compare(0, char1), -1);
	EXPECT_EQ(charVector->compare(1, char0), 1);
}

TEST_F(DataformVectorTest, testCharVector_upper)
{
	VectorSP v1 = Util::createVector(DT_CHAR, 4, 4);
	ConstantSP char0 = Util::createChar('0');
	ConstantSP char1 = Util::createChar('a');
	ConstantSP char2 = Util::createChar('B');
	ConstantSP char3 = Util::createChar('|');
	v1->set(0, char0);
	v1->set(1, char1);
	v1->set(2, char2);
	v1->set(3, char3);
	SmartPointer<FastCharVector> charVector = v1;
	charVector->upper();
	EXPECT_EQ(charVector->getChar(1), 'A');
}

TEST_F(DataformVectorTest, testCharVector_lower)
{
	VectorSP v1 = Util::createVector(DT_CHAR, 4, 4);
	ConstantSP char0 = Util::createChar('0');
	ConstantSP char1 = Util::createChar('a');
	ConstantSP char2 = Util::createChar('B');
	ConstantSP char3 = Util::createChar('|');
	v1->set(0, char0);
	v1->set(1, char1);
	v1->set(2, char2);
	v1->set(3, char3);
	SmartPointer<FastCharVector> charVector = v1;
	charVector->lower();
	EXPECT_EQ(charVector->getChar(2), 'b');
}

TEST_F(DataformVectorTest, testIntVector_get)
{
	VectorSP v1 = Util::createVector(DT_INT, 2, 2);
	v1->set(0, Util::createInt(0));
	v1->set(1, Util::createInt(1));
	SmartPointer<FastIntVector> intVector = v1;
	ConstantSP res = intVector->get(Util::createInt(1));
	EXPECT_EQ(res->getInt(), 1);

	res = intVector->get(Util::createInt(10));
	EXPECT_TRUE(res.isNull());

	ConstantSP indexArray = Util::createVector(DT_INT, 2, 2);
	indexArray->set(0, Util::createInt(0));
	indexArray->set(1, Util::createInt(1));
	res = intVector->get(indexArray);
	EXPECT_EQ(res->getInt(0), 0);
	EXPECT_EQ(res->getInt(1), 1);
}

TEST_F(DataformVectorTest, testIntVector_compare)
{
	VectorSP v1 = Util::createVector(DT_INT, 2, 2);
	ConstantSP int0 = Util::createInt(0);
	ConstantSP int1 = Util::createInt(1);
	v1->set(0, int0);
	v1->set(1, int1);
	SmartPointer<FastIntVector> intVector = v1;
	EXPECT_EQ(intVector->compare(0, int0), 0);
	EXPECT_EQ(intVector->compare(0, int1), -1);
	EXPECT_EQ(intVector->compare(1, int0), 1);
}

TEST_F(DataformVectorTest, testLongVector_get)
{
	VectorSP v1 = Util::createVector(DT_LONG, 2, 2);
	v1->set(0, Util::createLong(0));
	v1->set(1, Util::createLong(1));
	SmartPointer<FastLongVector> longVector = v1;
	ConstantSP res = longVector->get(Util::createLong(1));
	EXPECT_EQ(res->getLong(), 1);

	res = longVector->get(Util::createLong(10));
	EXPECT_TRUE(res.isNull());

	ConstantSP indexArray = Util::createVector(DT_LONG, 2, 2);
	indexArray->set(0, Util::createLong(0));
	indexArray->set(1, Util::createLong(1));
	res = longVector->get(indexArray);
	EXPECT_EQ(res->getLong(0), 0);
	EXPECT_EQ(res->getLong(1), 1);
}

TEST_F(DataformVectorTest, testLongVector_compare)
{
	VectorSP v1 = Util::createVector(DT_LONG, 2, 2);
	ConstantSP long0 = Util::createLong(0);
	ConstantSP long1 = Util::createLong(1);
	v1->set(0, long0);
	v1->set(1, long1);
	SmartPointer<FastLongVector> longVector = v1;
	EXPECT_EQ(longVector->compare(0, long0), 0);
	EXPECT_EQ(longVector->compare(0, long1), -1);
	EXPECT_EQ(longVector->compare(1, long0), 1);
}

TEST_F(DataformVectorTest, testShortVector_get)
{
	VectorSP v1 = Util::createVector(DT_SHORT, 2, 2);
	v1->set(0, Util::createShort(0));
	v1->set(1, Util::createShort(1));
	SmartPointer<FastShortVector> shortVector = v1;
	ConstantSP res = shortVector->get(Util::createInt(1));
	EXPECT_EQ(res->getShort(), 1);

	res = shortVector->get(Util::createInt(10));
	EXPECT_TRUE(res.isNull());

	ConstantSP indexArray = Util::createVector(DT_INT, 2, 2);
	indexArray->set(0, Util::createInt(0));
	indexArray->set(1, Util::createInt(1));
	res = shortVector->get(indexArray);
	EXPECT_EQ(res->getShort(0), 0);
	EXPECT_EQ(res->getShort(1), 1);
}

TEST_F(DataformVectorTest, testShortVector_compare)
{
	VectorSP v1 = Util::createVector(DT_SHORT, 2, 2);
	ConstantSP short0 = Util::createShort(0);
	ConstantSP short1 = Util::createShort(1);
	v1->set(0, short0);
	v1->set(1, short1);
	SmartPointer<FastShortVector> shortVector = v1;
	EXPECT_EQ(shortVector->compare(0, short0), 0);
	EXPECT_EQ(shortVector->compare(0, short1), -1);
	EXPECT_EQ(shortVector->compare(1, short0), 1);
}

TEST_F(DataformVectorTest, testFloatVector_get)
{
	VectorSP v1 = Util::createVector(DT_FLOAT, 2, 2);
	v1->set(0, Util::createFloat(0));
	v1->set(1, Util::createFloat(1));
	SmartPointer<FastFloatVector> floatVector = v1;
	ConstantSP res = floatVector->get(Util::createFloat(1));
	EXPECT_EQ(res->getFloat(), 1);

	res = floatVector->get(Util::createFloat(10));
	EXPECT_TRUE(res.isNull());

	ConstantSP indexArray = Util::createVector(DT_FLOAT, 2, 2);
	indexArray->set(0, Util::createFloat(0));
	indexArray->set(1, Util::createFloat(1));
	res = floatVector->get(indexArray);
	EXPECT_EQ(res->getFloat(0), 0);
	EXPECT_EQ(res->getFloat(1), 1);
}

TEST_F(DataformVectorTest, testDoubleVector_get)
{
	VectorSP v1 = Util::createVector(DT_DOUBLE, 2, 2);
	v1->set(0, Util::createDouble(0));
	v1->set(1, Util::createDouble(1));
	SmartPointer<FastDoubleVector> doubleVector = v1;
	ConstantSP res = doubleVector->get(Util::createDouble(1));
	EXPECT_EQ(res->getDouble(), 1);

	res = doubleVector->get(Util::createDouble(10));
	EXPECT_TRUE(res.isNull());

	ConstantSP indexArray = Util::createVector(DT_DOUBLE, 2, 2);
	indexArray->set(0, Util::createDouble(0));
	indexArray->set(1, Util::createDouble(1));
	res = doubleVector->get(indexArray);
	EXPECT_EQ(res->getDouble(0), 0);
	EXPECT_EQ(res->getDouble(1), 1);
}

TEST_F(DataformVectorTest, testDateVector_get)
{
	VectorSP v1 = Util::createVector(DT_DATE, 2, 2);
	v1->set(0, Util::createDate(0));
	v1->set(1, Util::createDate(1));
	SmartPointer<FastDateVector> dateVector = v1;
	ConstantSP res = dateVector->get(Util::createInt(1));
	EXPECT_EQ(res->getString(), "1970.01.02");

	res = dateVector->get(Util::createInt(10));
	EXPECT_TRUE(res.isNull());

	ConstantSP indexArray = Util::createVector(DT_DATE, 2, 2);
	indexArray->set(0, Util::createInt(0));
	indexArray->set(1, Util::createInt(1));
	res = dateVector->get(indexArray);
	EXPECT_EQ(res->getString(0), "1970.01.01");
	EXPECT_EQ(res->getString(1), "1970.01.02");
}

TEST_F(DataformVectorTest, testDateTimeVector_get)
{
	VectorSP v1 = Util::createVector(DT_DATETIME, 2, 2);
	v1->set(0, Util::createDateTime(0));
	v1->set(1, Util::createDateTime(1));
	SmartPointer<FastDateTimeVector> dateTimeVector = v1;
	ConstantSP res = dateTimeVector->get(Util::createInt(1));
	EXPECT_EQ(res->getString(), "1970.01.01T00:00:01");

	res = dateTimeVector->get(Util::createInt(10));
	EXPECT_TRUE(res.isNull());

	ConstantSP indexArray = Util::createVector(DT_DATETIME, 2, 2);
	indexArray->set(0, Util::createInt(0));
	indexArray->set(1, Util::createInt(1));
	res = dateTimeVector->get(indexArray);
	EXPECT_EQ(res->getString(0), "1970.01.01T00:00:00");
	EXPECT_EQ(res->getString(1), "1970.01.01T00:00:01");
}

TEST_F(DataformVectorTest, testDateHourVector_get)
{
	VectorSP v1 = Util::createVector(DT_DATEHOUR, 2, 2);
	v1->set(0, Util::createDateHour(0));
	v1->set(1, Util::createDateHour(1));
	SmartPointer<FastDateHourVector> dateHourVector = v1;
	ConstantSP res = dateHourVector->get(Util::createInt(1));
	EXPECT_EQ(res->getString(), "1970.01.01T01");

	res = dateHourVector->get(Util::createInt(10));
	EXPECT_TRUE(res.isNull());

	ConstantSP indexArray = Util::createVector(DT_DATEHOUR, 2, 2);
	indexArray->set(0, Util::createInt(0));
	indexArray->set(1, Util::createInt(1));
	res = dateHourVector->get(indexArray);
	EXPECT_EQ(res->getString(0), "1970.01.01T00");
	EXPECT_EQ(res->getString(1), "1970.01.01T01");
}

TEST_F(DataformVectorTest, testMonthVector_get)
{
	VectorSP v1 = Util::createVector(DT_MONTH, 2, 2);
	v1->set(0, Util::createMonth(0));
	v1->set(1, Util::createMonth(1));
	SmartPointer<FastMonthVector> monthVector = v1;
	ConstantSP res = monthVector->get(Util::createInt(1));
	EXPECT_EQ(res->getString(), "0000.02M");

	res = monthVector->get(Util::createInt(10));
	EXPECT_TRUE(res.isNull());

	ConstantSP indexArray = Util::createVector(DT_MONTH, 2, 2);
	indexArray->set(0, Util::createInt(0));
	indexArray->set(1, Util::createInt(1));
	res = monthVector->get(indexArray);
	EXPECT_EQ(res->getString(0), "0000.01M");
	EXPECT_EQ(res->getString(1), "0000.02M");
}

TEST_F(DataformVectorTest, testTimeVector_get)
{
	VectorSP v1 = Util::createVector(DT_TIME, 2, 2);
	v1->set(0, Util::createTime(0));
	v1->set(1, Util::createTime(1));
	SmartPointer<FastTimeVector> timeVector = v1;
	ConstantSP res = timeVector->get(Util::createInt(1));
	EXPECT_EQ(res->getString(), "00:00:00.001");

	res = timeVector->get(Util::createInt(10));
	EXPECT_TRUE(res.isNull());

	ConstantSP indexArray = Util::createVector(DT_TIME, 2, 2);
	indexArray->set(0, Util::createInt(0));
	indexArray->set(1, Util::createInt(1));
	res = timeVector->get(indexArray);
	EXPECT_EQ(res->getString(0), "00:00:00.000");
	EXPECT_EQ(res->getString(1), "00:00:00.001");
}

TEST_F(DataformVectorTest, testMinuteVector_get)
{
	VectorSP v1 = Util::createVector(DT_MINUTE, 2, 2);
	v1->set(0, Util::createMinute(0));
	v1->set(1, Util::createMinute(1));
	SmartPointer<FastMinuteVector> minuteVector = v1;
	ConstantSP res = minuteVector->get(Util::createInt(1));
	EXPECT_EQ(res->getString(), "00:01m");

	res = minuteVector->get(Util::createInt(10));
	EXPECT_TRUE(res.isNull());

	ConstantSP indexArray = Util::createVector(DT_MINUTE, 2, 2);
	indexArray->set(0, Util::createInt(0));
	indexArray->set(1, Util::createInt(1));
	res = minuteVector->get(indexArray);
	EXPECT_EQ(res->getString(0), "00:00m");
	EXPECT_EQ(res->getString(1), "00:01m");
}

TEST_F(DataformVectorTest, testSecondVector_get)
{
	VectorSP v1 = Util::createVector(DT_SECOND, 2, 2);
	v1->set(0, Util::createSecond(0));
	v1->set(1, Util::createSecond(1));
	SmartPointer<FastSecondVector> secondVector = v1;
	ConstantSP res = secondVector->get(Util::createInt(1));
	EXPECT_EQ(res->getString(), "00:00:01");

	res = secondVector->get(Util::createInt(10));
	EXPECT_TRUE(res.isNull());

	ConstantSP indexArray = Util::createVector(DT_SECOND, 2, 2);
	indexArray->set(0, Util::createInt(0));
	indexArray->set(1, Util::createInt(1));
	res = secondVector->get(indexArray);
	EXPECT_EQ(res->getString(0), "00:00:00");
	EXPECT_EQ(res->getString(1), "00:00:01");
}

TEST_F(DataformVectorTest, testNanoTimeVector_get)
{
	VectorSP v1 = Util::createVector(DT_NANOTIME, 2, 2);
	v1->set(0, Util::createNanoTime(0));
	v1->set(1, Util::createNanoTime(1));
	SmartPointer<FastNanoTimeVector> nanoTimeVector = v1;
	ConstantSP res = nanoTimeVector->get(Util::createInt(1));
	EXPECT_EQ(res->getString(), "00:00:00.000000001");

	res = nanoTimeVector->get(Util::createInt(10));
	EXPECT_TRUE(res.isNull());

	ConstantSP indexArray = Util::createVector(DT_NANOTIME, 2, 2);
	indexArray->set(0, Util::createInt(0));
	indexArray->set(1, Util::createInt(1));
	res = nanoTimeVector->get(indexArray);
	EXPECT_EQ(res->getString(0), "00:00:00.000000000");
	EXPECT_EQ(res->getString(1), "00:00:00.000000001");
}

TEST_F(DataformVectorTest, testTimestampVector_get)
{
	VectorSP v1 = Util::createVector(DT_TIMESTAMP, 2, 2);
	v1->set(0, Util::createTimestamp(0));
	v1->set(1, Util::createTimestamp(1));
	SmartPointer<FastTimestampVector> timestampVector = v1;
	ConstantSP res = timestampVector->get(Util::createInt(1));
	EXPECT_EQ(res->getString(), "1970.01.01T00:00:00.001");

	res = timestampVector->get(Util::createInt(10));
	EXPECT_TRUE(res.isNull());

	ConstantSP indexArray = Util::createVector(DT_TIMESTAMP, 2, 2);
	indexArray->set(0, Util::createInt(0));
	indexArray->set(1, Util::createInt(1));
	res = timestampVector->get(indexArray);
	EXPECT_EQ(res->getString(0), "1970.01.01T00:00:00.000");
	EXPECT_EQ(res->getString(1), "1970.01.01T00:00:00.001");
}

TEST_F(DataformVectorTest, testNanoTimestampVector_get)
{
	VectorSP v1 = Util::createVector(DT_NANOTIMESTAMP, 2, 2);
	v1->set(0, Util::createNanoTimestamp(0));
	v1->set(1, Util::createNanoTimestamp(1));
	SmartPointer<FastNanoTimestampVector> nanoNanoTimestampVector = v1;
	ConstantSP res = nanoNanoTimestampVector->get(Util::createInt(1));
	EXPECT_EQ(res->getString(), "1970.01.01T00:00:00.000000001");

	res = nanoNanoTimestampVector->get(Util::createInt(10));
	EXPECT_TRUE(res.isNull());

	ConstantSP indexArray = Util::createVector(DT_NANOTIMESTAMP, 2, 2);
	indexArray->set(0, Util::createInt(0));
	indexArray->set(1, Util::createInt(1));
	res = nanoNanoTimestampVector->get(indexArray);
	EXPECT_EQ(res->getString(0), "1970.01.01T00:00:00.000000000");
	EXPECT_EQ(res->getString(1), "1970.01.01T00:00:00.000000001");
}

#endif

TEST_F(DataformVectorTest, testAnyVector)
{
	VectorSP v1 = Util::createVector(DT_ANY, 5, 5);
	v1->set(0, Util::createTime(1));
	v1->set(1, Util::createInt(1));
	v1->set(2, Util::createDouble(1));
	v1->set(3, Util::createString("asd"));
	v1->set(4, Util::createBool(1));
	string script = "a=[time(1), int(1),double(1),`asd,true];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());

	VectorSP v2 = Util::createVector(DT_ANY, 0, 2000, false, 0, (void *)0, true);
	EXPECT_FALSE(v2->getNullFlag()); // anyvector is absolutely not null
	EXPECT_EQ(v2->getCapacity(), 0);
	EXPECT_FALSE(v2->isFastMode());
	EXPECT_EQ(v2->getUnitLength(), 0);
	EXPECT_TRUE(v2->sizeable());
	EXPECT_EQ(v2->getRawType(), DT_ANY);
	EXPECT_EQ(v2->getCategory(), MIXED);
	EXPECT_ANY_THROW(v2->getStringRef(0));

	v2->append(Util::createInt(1));
	VectorSP v3 = v2->getInstance(1);
	EXPECT_EQ(v3->getString(), "()");
	EXPECT_FALSE(v3->isNull()); // anyvector is absolutely not null
	VectorSP v4 = v2->getValue(10);
	EXPECT_EQ(v4->getString(), "(1)");
	v4->setNull(); // nothing to do and anyvector is not changed.
	EXPECT_EQ(v4->getString(), "(1)");
	EXPECT_FALSE(v2->append(Util::createInt(2), 1));
	EXPECT_TRUE(v2->isLargeConstant());

	EXPECT_EQ(v2->getBool(0), true);
	EXPECT_EQ(v2->getChar(0), (char)1);
	EXPECT_EQ(v2->getShort(0), (short)1);
	EXPECT_EQ(v2->getInt(0), (int)1);
	EXPECT_EQ(v2->getLong(0), (long)1);
	EXPECT_EQ(v2->getIndex(0), (INDEX)1);
	EXPECT_EQ(v2->getFloat(0), (float)1);
	EXPECT_EQ(v2->getDouble(0), (double)1);

	char *buf = new char[1];
	string **buf1 = new string *[1];
	char **buf2 = new char *[1];
	int numElement, partial;
	EXPECT_ANY_THROW(v2->serialize(buf, 1, 0, 1, numElement, partial));
	// EXPECT_ANY_THROW(v2->getString(0,1,buf1));
	// EXPECT_ANY_THROW(v2->getString(0,1,buf2));
	EXPECT_ANY_THROW(v2->getStringConst(0, 1, buf1));
	EXPECT_ANY_THROW(v2->getStringConst(0, 1, buf2));

	EXPECT_ANY_THROW(v2->neg());
	EXPECT_ANY_THROW(v2->replace(Util::createInt(1), Util::createInt(0)));
	EXPECT_ANY_THROW(v2->asof(Util::createInt(1)));
	// EXPECT_ANY_THROW(v2->getSubVector(0,2,0));

	v2->append(Util::createInt(0));
	v2->reverse();
	EXPECT_EQ(v2->getString(), "(0,1)");
	v2->append(Util::createInt(-10));
	v2->reverse(0, 3);
	EXPECT_EQ(v2->getString(), "(-10,1,0)");

	v2->clear();
	EXPECT_EQ(v2->getString(), "()");

	VectorSP v5 = Util::createVector(DT_ANY, 2, 2);
	v5->set(0, Util::createDouble(3.21));
	v5->set(1, Util::createInt(1));
	EXPECT_FALSE(v5->assign(Util::createVector(DT_INT, 5, 5)));
	v5->assign(Util::createString("5"));
	EXPECT_EQ(v5->getString(), "(\"5\",\"5\")");
	v5->assign(Util::createNullConstant(DT_INT));
	EXPECT_EQ(v5->getString(), "(,)");

	v5->set(0, Util::createDouble(3.21));
	v5->set(1, Util::createInt(1));
	v5->append(Util::createString("str"));
	v5->append(Util::createNullConstant(DT_BOOL));

	v5->nullFill(Util::createDateTime(30000));
	EXPECT_EQ(v5->getString(), "(3.21,1,\"str\",1970.01.01T08:20:00)");
	v5->fill(0, 3, Util::createDate(700));
	EXPECT_EQ(v5->getString(), "(1971.12.02,1971.12.02,1971.12.02,1970.01.01T08:20:00)");
	VectorSP valVec = Util::createVector(DT_IP, 3, 3);
	valVec->set(0, Util::createNullConstant(DT_UUID));
	valVec->set(1, Util::parseConstant(DT_IP, "192.0.0.5"));
	valVec->set(2, Util::parseConstant(DT_IP, "10.20.30.40"));

	v5->fill(0, 3, valVec);
	EXPECT_EQ(v5->getString(), "(,192.0.0.5,10.20.30.40,1970.01.01T08:20:00)");

	char *buf3 = new char[2];
	v5->isNull(0, 2, buf3);
	EXPECT_TRUE((int)buf3[0]);
	EXPECT_FALSE((int)buf3[1]);

	buf3[0] = '\0';
	buf3[1] = '\0';
	v5->isValid(0, 2, buf3);
	EXPECT_FALSE((int)buf3[0]);
	EXPECT_TRUE((int)buf3[1]);
	delete[] buf, buf1, buf2, buf3;

	EXPECT_EQ(v5->getValue(0)->getString(), v5->getString());
	VectorSP v6 = Util::createVector(DT_ANY, 1, 1);
	v6->set(0, Util::createFloat(3.12));

	EXPECT_ANY_THROW(v5->getBool());
	EXPECT_ANY_THROW(v5->getChar());
	EXPECT_ANY_THROW(v5->getShort());
	EXPECT_ANY_THROW(v5->getLong());
	EXPECT_ANY_THROW(v5->getIndex());
	EXPECT_ANY_THROW(v5->getInt());
	EXPECT_ANY_THROW(v5->getFloat());
	EXPECT_ANY_THROW(v5->getDouble());
	EXPECT_EQ(v6->getBool(), (bool)3);
	EXPECT_EQ(v6->getChar(), (char)3);
	EXPECT_EQ(v6->getShort(), (short)3);
	EXPECT_EQ(v6->getLong(), (long)3);
	EXPECT_EQ(v6->getIndex(), (INDEX)3);
	EXPECT_EQ(v6->getInt(), (int)3);
	EXPECT_EQ(v6->getFloat(), (float)3.12);
	EXPECT_EQ(v6->getDouble(), (float)3.12);

	cout << v5->getAllocatedMemory(1) << endl; // AC-122: not supported

	VectorSP v7 = Util::createVector(DT_ANY, 3, 10);
	VectorSP v8 = Util::createVector(DT_INT, 1, 1);
	v8->setInt(1);
	v7->setNull(0);
	v7->set(1, Util::createInt(1));
	v7->set(2, v8);
	char *buf4 = new char[2];
	char *buf5 = new char[2];
	short *buf6 = new short[2];
	int *buf7 = new int[2];
	INDEX *buf8 = new INDEX[2];
	long long *buf9 = new long long[2];
	float *buf10 = new float[2];
	double *buf11 = new double[2];
	EXPECT_FALSE(v7->getBool(2, 1, buf4));
	EXPECT_FALSE(v7->getChar(2, 1, buf5));
	EXPECT_FALSE(v7->getShort(2, 1, buf6));
	EXPECT_FALSE(v7->getInt(2, 1, buf7));
	EXPECT_FALSE(v7->getIndex(2, 1, buf8));
	EXPECT_FALSE(v7->getLong(2, 1, buf9));
	EXPECT_FALSE(v7->getFloat(2, 1, buf10));
	EXPECT_FALSE(v7->getDouble(2, 1, buf11));

	EXPECT_EQ(v7->getBoolConst(1, 1, buf4)[0], (bool)1);
	EXPECT_EQ(v7->getCharConst(1, 1, buf5)[0], (char)1);
	EXPECT_EQ(v7->getShortConst(1, 1, buf6)[0], (short)1);
	EXPECT_EQ(v7->getIntConst(1, 1, buf7)[0], (int)1);
	EXPECT_EQ(v7->getIndexConst(1, 1, buf8)[0], (INDEX)1);
	EXPECT_EQ(v7->getLongConst(1, 1, buf9)[0], (long)1);
	EXPECT_EQ(v7->getFloatConst(1, 1, buf10)[0], (float)1);
	EXPECT_EQ(v7->getDoubleConst(1, 1, buf11)[0], (float)1);
	delete[] buf4, buf5, buf6, buf7, buf8, buf9, buf10, buf11;

	VectorSP indexVec = Util::createIndexVector(0, 3);
	INDEX index = 2;
	v7->set(index, Util::createString("setstr"));
	EXPECT_EQ(v7->get(2)->getString(), "setstr");
	v7->set(index, Util::createNullConstant(DT_STRING));
	EXPECT_TRUE(v7->get(2)->isNull());
	v7->set(indexVec, Util::createNullConstant(DT_INT));
	EXPECT_EQ(v7->getString(), "(,,)");

	v7->append(Util::createNullConstant(DT_INT));
	EXPECT_TRUE(v7->get(2)->isNull());

	v7->append(Util::createString("str"));
	v7->remove(1);
	EXPECT_EQ(v7->getString(), "(,,,)");
	v7->remove(-1);
	EXPECT_EQ(v7->getString(), "(,,)");

	v7->append(Util::createString("str"));
	v7->append(Util::createInt(3));
	v7->append(v8);

	v7->next(1);
	EXPECT_EQ(v7->size(), 6);
	EXPECT_TRUE(v7->get(5)->isNull());

	v7->prev(2);
	EXPECT_EQ(v7->size(), 6);
	EXPECT_TRUE(v7->get(3)->isNull());

	v7->append(Util::createDecimal32(2, 3.1234));
	v7->append(Util::createDecimal64(5, 3.1234));
	conn.upload("v7", v7);
	EXPECT_EQ(conn.run("typestr(v7[6])")->getString(), "DECIMAL32");
	EXPECT_EQ(conn.run("typestr(v7[7])")->getString(), "DECIMAL64");
}

TEST_F(DataformVectorTest, testStringVector_upper)
{
	VectorSP v1 = Util::createVector(DT_STRING, 2, 2);
	v1->set(0, Util::createString("|DolphinDB"));
	v1->upper();
	EXPECT_EQ(v1->getString(0), "|DOLPHINDB");
}

TEST_F(DataformVectorTest, testStringVector_lower)
{
	VectorSP v1 = Util::createVector(DT_STRING, 2, 2);
	v1->set(0, Util::createString("|DolphinDB"));
	v1->lower();
	EXPECT_EQ(v1->getString(0), "|dolphindb");
}

TEST_F(DataformVectorTest, testStringVector_assign)
{
	VectorSP v1 = Util::createVector(DT_STRING, 2, 2);
	v1->set(0, Util::createString("1"));
	v1->set(1, Util::createString("2"));
	VectorSP v2 = Util::createVector(DT_STRING, 2, 2);
	EXPECT_TRUE(v2->assign(v1));
	EXPECT_EQ(v2->getString(0), "1");
	EXPECT_EQ(v2->getString(1), "2");
}

TEST_F(DataformVectorTest, testStringVector)
{
	VectorSP v1 = Util::createVector(DT_STRING, 2, 2);
	v1->set(0, Util::createString("asd123!@#"));
	v1->set(1, Util::createString("中文！@￥#%……a"));
	string script = "a=[\"asd123!@#\",\"中文！@￥#%……a\"];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());

	EXPECT_ANY_THROW(v1->getBool());
	EXPECT_ANY_THROW(v1->getChar());
	EXPECT_ANY_THROW(v1->getShort());
	EXPECT_ANY_THROW(v1->getLong());
	EXPECT_ANY_THROW(v1->getFloat());
	EXPECT_ANY_THROW(v1->getDouble());
	EXPECT_ANY_THROW(v1->getIndex());
	EXPECT_ANY_THROW(v1->neg());

	EXPECT_EQ(v1->getUnitLength(), 0);
	EXPECT_TRUE(v1->sizeable());
	EXPECT_EQ(v1->compare(1, Util::createString("中文！@￥#%……a")), 0);
	EXPECT_EQ(v1->getStringRef(), v1->get(0)->getString());
	v1->setNull(); // nothing to do
	EXPECT_FALSE(v1->hasNull());

	v1->reverse();
	EXPECT_EQ(v1->get(0)->getString(), Util::createString("中文！@￥#%……a")->getString());
	EXPECT_EQ(v1->get(1)->getString(), Util::createString("asd123!@#")->getString());
	v1->reverse(0, 1);
	EXPECT_EQ(v1->get(1)->getString(), Util::createString("中文！@￥#%……a")->getString());
	EXPECT_EQ(v1->get(0)->getString(), Util::createString("asd123!@#")->getString());

	cout << v1->getDataArray() << endl;
	cout << v1->getAllocatedMemory(1) << endl;

	v1->upper();
	EXPECT_EQ(v1->get(0)->getString(), "ASD123!@#");
	v1->lower();
	EXPECT_EQ(v1->get(0)->getString(), "asd123!@#");

	v1->set(0, Util::createString(" 1 2 3      "));
	v1->set(1, Util::createString(" \t\r\n 1 2 3 \t\n\r"));
	v1->trim();
	EXPECT_EQ(v1->get(0)->getString(), "1 2 3");
	EXPECT_EQ(v1->get(1)->getString(), "\t\r\n 1 2 3 \t\n\r");
	v1->strip();
	EXPECT_EQ(v1->get(0)->getString(), "1 2 3");
	EXPECT_EQ(v1->get(1)->getString(), "1 2 3");

	v1->set(Util::createInt(1), Util::createInt(1));
	EXPECT_EQ(v1->get(1)->getString(), "1");
	v1->set(Util::createInt(0), Util::createNullConstant(DT_INT));
	EXPECT_TRUE(v1->get(0)->isNull());
	EXPECT_ANY_THROW(v1->set(Util::createInt(1), Util::createVector(DT_STRING, 1, 1)));

	EXPECT_FALSE(v1->assign(Util::createVector(DT_STRING, 5, 5)));
	v1->assign(Util::createString("5"));
	EXPECT_EQ(v1->getString(), "[\"5\",\"5\"]");
	v1->assign(Util::createNullConstant(DT_STRING));
	EXPECT_EQ(v1->getString(), "[,]");

	v1->set(0, Util::createString("bcd"));
	EXPECT_EQ(v1->getSubVector(0, -1, 2)->getString(), "[\"bcd\"]");

	v1->append(Util::createString("efg"), 10);
	EXPECT_EQ(v1->size(), 3);
	VectorSP strVec = Util::createVector(DT_STRING, 1, 1);
	strVec->set(0, Util::createString("hij"));
	strVec->append(Util::createString("klm"));
	v1->append(strVec, 2);
	EXPECT_EQ(v1->size(), 5);

	VectorSP v2 = Util::createVector(DT_STRING, 0, 0);
	char *strbuf = (char *)"charstr";
	char **str1 = &strbuf;
	v2->appendString(str1, 1);
	v2->appendString(str1, 1);
	EXPECT_EQ(v2->getString(), "[\"charstr\",\"charstr\"]");
	v2->append(Util::createNullConstant(DT_STRING));
	v2->append(Util::createString("str"));

	// EXPECT_ANY_THROW(v2->next(-2));
	// EXPECT_ANY_THROW(v2->prev(-2));
	v2->next(1);
	EXPECT_EQ(v2->size(), 4);
	EXPECT_TRUE(v2->get(3)->isNull());

	v2->prev(2);
	EXPECT_EQ(v2->size(), 4);
	EXPECT_TRUE(v2->get(0)->isNull());
	EXPECT_TRUE(v2->get(1)->isNull());

	v2->nullFill(Util::createString("filled"));
	EXPECT_EQ(v2->getString(), "[\"filled\",\"filled\",\"charstr\",\"filled\"]");

	v2->fill(0, 1, Util::createString("twicefilled"));
	v2->fill(1, 1, Util::createNullConstant(DT_STRING));
	v2->fill(3, 1, Util::createInt(3));
	EXPECT_EQ(v2->getString(), "[\"twicefilled\",,\"charstr\",\"3\"]");

	char *buf = new char[2];
	v2->isNull(1, 1, buf);
	EXPECT_EQ((int)buf[0], 1);
	buf[0] = '\0';
	v2->nullFill(Util::createString("filled"));
	v2->isNull(1, 1, buf);
	EXPECT_EQ((int)buf[0], 0);
	delete[] buf;

	string **buf1 = new string *[2];
	EXPECT_EQ(*v2->getStringConst(0, 1, buf1)[0], v2->get(0)->getString());
	delete[] buf1;

	v2->replace(Util::createString("3"), Util::createString("replaced"));
	// cout<<v2->getString()<<endl;
	EXPECT_EQ(v2->getString(), "[\"twicefilled\",\"filled\",\"charstr\",\"replaced\"]");

	VectorSP indexVec = Util::createIndexVector(0, 10);
	v2->remove(indexVec);
	EXPECT_EQ(v2->getString(), "[]");

	char **buf2 = new char *[v2->size()];
	v2->getStringConst(0, v2->size(), buf2);
	for (auto i = 0; i < v2->size(); i++)
		EXPECT_EQ(buf2[i], v2->get(i)->getString());

	delete[] buf2;
}

TEST_F(DataformVectorTest, testStringNullVector)
{
	VectorSP v1 = Util::createVector(DT_STRING, 2, 2);
	v1->setNull(0);
	v1->setNull(1);
	string script = "a=[string(NULL),string(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
}

TEST_F(DataformVectorTest, testBoolVector)
{
	VectorSP v1 = Util::createVector(DT_BOOL, 2, 2);
	v1->set(0, Util::createBool(1));
	v1->set(1, Util::createBool(0));
	string script = "a=[bool(1),bool(0)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());

	EXPECT_EQ(v1->getCategory(), LOGICAL);
	EXPECT_FALSE(v1->add(0, 1, (long long)1));
	EXPECT_FALSE(v1->add(0, 1, (double)1));
	EXPECT_ANY_THROW(v1->asof(Util::createBool(1)));

	char *buf = new char[2];
	buf[0] = 1;
	buf[1] = 2;
	v1->getBoolBuffer(0, 2, buf);
	// cout<<v1->getString();
	EXPECT_TRUE(v1->setBool(0, 2, buf));
	EXPECT_EQ(v1->getString(), "[1,1]");
	EXPECT_TRUE(v1->appendBool(buf, 1));
	EXPECT_EQ(v1->getString(), "[1,1,1]");

	EXPECT_ANY_THROW(v1->fill(2, 1, Util::createString("1")));
	v1->fill(2, 1, Util::createNullConstant(DT_BOOL));
	EXPECT_TRUE(v1->hasNull());
	EXPECT_FALSE(v1->compare(0, Util::createBool(1)));

	VectorSP indexVec = Util::createIndexVector(2, 1);
	ConstantSP index = Util::createInt(2);
	v1->set(indexVec, Util::createBool(1));
	EXPECT_EQ(v1->get(2)->getBool(), 1);
	v1->set(indexVec, Util::createNullConstant(DT_BOOL));
	EXPECT_TRUE(v1->get(2)->isNull());
	v1->set(index, Util::createBool(1));
	EXPECT_EQ(v1->get(2)->getBool(), 1);

	VectorSP v2 = Util::createVector(DT_BOOL, 2, 10);
	v2->set(0, Util::createBool(1));
	v2->set(1, Util::createBool(0));
	char *buf1 = new char[2];
	buf1[0] = 1;
	short *buf2 = new short[2];
	buf2[0] = 1;
	int *buf3 = new int[2];
	buf3[0] = 1;
	INDEX *buf5 = new INDEX[2];
	buf5[0] = 1;
	long long *buf4 = new long long[2];
	buf4[0] = 1;
	float *buf6 = new float[2];
	buf6[0] = 1;
	double *buf7 = new double[2];
	buf7[0] = 1;

	// cout<<v2->getString()<<endl;
	v2->appendBool(buf1, 1);
	EXPECT_EQ(v2->getString(), "[1,0,1]");
	v2->appendChar(buf1, 1);
	EXPECT_EQ(v2->getString(), "[1,0,1,1]");
	v2->appendShort(buf2, 1);
	EXPECT_EQ(v2->getString(), "[1,0,1,1,1]");
	v2->appendInt(buf3, 1);
	EXPECT_EQ(v2->getString(), "[1,0,1,1,1,1]");
	v2->appendLong(buf4, 1);
	EXPECT_EQ(v2->getString(), "[1,0,1,1,1,1,1]");
	v2->appendIndex(buf5, 1);
	EXPECT_EQ(v2->getString(), "[1,0,1,1,1,1,1,1]");
	v2->appendFloat(buf6, 1);
	EXPECT_EQ(v2->getString(), "[1,0,1,1,1,1,1,1,1]");
	v2->appendDouble(buf7, 1);
	EXPECT_EQ(v2->getString(), "[1,0,1,1,1,1,1,1,1,1]");

	VectorSP v3 = Util::createVector(DT_BOOL, 2, 10);
	v3->set(0, Util::createBool(1));
	v3->set(1, Util::createBool(0));
	char *buf9 = new char[2];
	short *buf10 = new short[2];
	int *buf11 = new int[2];
	INDEX *buf12 = new INDEX[2];
	long long *buf13 = new long long[2];
	float *buf14 = new float[2];
	double *buf15 = new double[2];

	v3->getBool(0, 2, buf9);
	EXPECT_EQ((bool)buf9[0], v3->get(0)->getBool());
	EXPECT_EQ((bool)buf9[1], v3->get(1)->getBool());
	v3->getChar(0, 2, buf9);
	EXPECT_EQ((char)buf9[0], v3->get(0)->getChar());
	EXPECT_EQ((char)buf9[1], v3->get(1)->getChar());
	v3->getShort(0, 2, buf10);
	EXPECT_EQ((short)buf10[0], v3->get(0)->getShort());
	EXPECT_EQ((short)buf10[1], v3->get(1)->getShort());
	v3->getInt(0, 2, buf11);
	EXPECT_EQ((int)buf11[0], v3->get(0)->getInt());
	EXPECT_EQ((int)buf11[1], v3->get(1)->getInt());
	v3->getLong(0, 2, buf13);
	EXPECT_EQ((long long)buf13[0], v3->get(0)->getLong());
	EXPECT_EQ((long long)buf13[1], v3->get(1)->getLong());
	v3->getIndex(0, 2, buf12);
	EXPECT_EQ((INDEX)buf12[0], v3->get(0)->getIndex());
	EXPECT_EQ((INDEX)buf12[1], v3->get(1)->getIndex());
	v3->getFloat(0, 2, buf14);
	EXPECT_EQ((float)buf14[0], v3->get(0)->getFloat());
	EXPECT_EQ((float)buf14[1], v3->get(1)->getFloat());
	v3->getDouble(0, 2, buf15);
	EXPECT_EQ((double)buf15[0], v3->get(0)->getDouble());
	EXPECT_EQ((double)buf15[1], v3->get(1)->getDouble());

	memset(buf9, '\0', 2);
	memset(buf10, 0, 2);
	memset(buf11, 0, 2);
	memset(buf12, 0, 2);
	memset(buf13, 0, 2);
	memset(buf14, 0, 2);
	memset(buf15, 0, 2);

	// cout<<v3->getString()<<endl;

	const char *resbuf = v3->getBoolConst(0, 2, buf9);
	EXPECT_EQ((bool)resbuf[0], v3->get(0)->getBool());
	EXPECT_EQ((bool)resbuf[1], v3->get(1)->getBool());

	const char *resbuf1 = v3->getCharConst(0, 2, buf9);
	;
	EXPECT_EQ((char)resbuf1[0], v3->get(0)->getChar());
	EXPECT_EQ((char)resbuf1[1], v3->get(1)->getChar());

	const short *resbuf2 = v3->getShortConst(0, 2, buf10);
	EXPECT_EQ((short)resbuf2[0], v3->get(0)->getShort());
	EXPECT_EQ((short)resbuf2[1], v3->get(1)->getShort());

	const int *resbuf3 = v3->getIntConst(0, 2, buf11);
	EXPECT_EQ((int)resbuf3[0], v3->get(0)->getInt());
	EXPECT_EQ((int)resbuf3[1], v3->get(1)->getInt());

	const long long *resbuf4 = v3->getLongConst(0, 2, buf13);
	EXPECT_EQ((long long)resbuf4[0], v3->get(0)->getLong());
	EXPECT_EQ((long long)resbuf4[1], v3->get(1)->getLong());

	const INDEX *resbuf5 = v3->getIndexConst(0, 2, buf12);
	EXPECT_EQ((INDEX)resbuf5[0], v3->get(0)->getIndex());
	EXPECT_EQ((INDEX)resbuf5[1], v3->get(1)->getIndex());

	const float *resbuf6 = v3->getFloatConst(0, 2, buf14);
	EXPECT_EQ((float)resbuf6[0], v3->get(0)->getFloat());
	EXPECT_EQ((float)resbuf6[1], v3->get(1)->getFloat());

	const double *resbuf7 = v3->getDoubleConst(0, 2, buf15);
	EXPECT_EQ((double)resbuf7[0], v3->get(0)->getDouble());
	EXPECT_EQ((double)resbuf7[1], v3->get(1)->getDouble());

	delete[] buf, buf1, buf2, buf3, buf4, buf5, buf6, buf7, buf9, buf10, buf11, buf12, buf13, buf14, buf15;
}

TEST_F(DataformVectorTest, testBoolNullVector)
{
	VectorSP v1 = Util::createVector(DT_BOOL, 2, 2);
	v1->setNull(0);
	v1->set(1, Util::createNullConstant(DT_BOOL));
	string script = "a=[bool(NULL),bool(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0; i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());

	VectorSP v3 = Util::createVector(DT_BOOL, 2, 10, true, 0, (void *)0, true);
	v3->setNull(0);
	v3->setNull(1);
	char *buf9 = new char[2];
	short *buf10 = new short[2];
	int *buf11 = new int[2];
	INDEX *buf12 = new INDEX[2];
	long long *buf13 = new long long[2];
	float *buf14 = new float[2];
	double *buf15 = new double[2];

	v3->getBool(0, 2, buf9);
	EXPECT_EQ((bool)buf9[0], true);
	EXPECT_EQ((bool)buf9[1], true);
	v3->getChar(0, 2, buf9);
	EXPECT_EQ((char)buf9[0], v3->get(0)->getChar());
	EXPECT_EQ((char)buf9[1], v3->get(1)->getChar());
	v3->getShort(0, 2, buf10);
	EXPECT_EQ((short)buf10[0], v3->get(0)->getShort());
	EXPECT_EQ((short)buf10[1], v3->get(1)->getShort());
	v3->getInt(0, 2, buf11);
	EXPECT_EQ((int)buf11[0], v3->get(0)->getInt());
	EXPECT_EQ((int)buf11[1], v3->get(1)->getInt());
	v3->getLong(0, 2, buf13);
	EXPECT_EQ((long long)buf13[0], v3->get(0)->getLong());
	EXPECT_EQ((long long)buf13[1], v3->get(1)->getLong());
	v3->getIndex(0, 2, buf12);
	EXPECT_EQ((INDEX)buf12[0], v3->get(0)->getIndex());
	EXPECT_EQ((INDEX)buf12[1], v3->get(1)->getIndex());
	v3->getFloat(0, 2, buf14);
	EXPECT_EQ((float)buf14[0], v3->get(0)->getFloat());
	EXPECT_EQ((float)buf14[1], v3->get(1)->getFloat());
	v3->getDouble(0, 2, buf15);
	EXPECT_EQ((double)buf15[0], v3->get(0)->getDouble());
	EXPECT_EQ((double)buf15[1], v3->get(1)->getDouble());

	memset(buf9, '\0', 2);
	memset(buf10, 0, 2);
	memset(buf11, 0, 2);
	memset(buf12, 0, 2);
	memset(buf13, 0, 2);
	memset(buf14, 0, 2);
	memset(buf15, 0, 2);

	// cout<<v3->getString()<<endl;

	const char *resbuf = v3->getBoolConst(0, 2, buf9);
	EXPECT_EQ((bool)resbuf[0], true);
	EXPECT_EQ((bool)resbuf[1], true);

	const char *resbuf1 = v3->getCharConst(0, 2, buf9);
	;
	EXPECT_EQ((char)resbuf1[0], v3->get(0)->getChar());
	EXPECT_EQ((char)resbuf1[1], v3->get(1)->getChar());

	const short *resbuf2 = v3->getShortConst(0, 2, buf10);
	EXPECT_EQ((short)resbuf2[0], v3->get(0)->getShort());
	EXPECT_EQ((short)resbuf2[1], v3->get(1)->getShort());

	const int *resbuf3 = v3->getIntConst(0, 2, buf11);
	EXPECT_EQ((int)resbuf3[0], v3->get(0)->getInt());
	EXPECT_EQ((int)resbuf3[1], v3->get(1)->getInt());

	const long long *resbuf4 = v3->getLongConst(0, 2, buf13);
	EXPECT_EQ((long long)resbuf4[0], v3->get(0)->getLong());
	EXPECT_EQ((long long)resbuf4[1], v3->get(1)->getLong());

	const INDEX *resbuf5 = v3->getIndexConst(0, 2, buf12);
	EXPECT_EQ((INDEX)resbuf5[0], v3->get(0)->getIndex());
	EXPECT_EQ((INDEX)resbuf5[1], v3->get(1)->getIndex());

	const float *resbuf6 = v3->getFloatConst(0, 2, buf14);
	EXPECT_EQ((float)resbuf6[0], v3->get(0)->getFloat());
	EXPECT_EQ((float)resbuf6[1], v3->get(1)->getFloat());

	const double *resbuf7 = v3->getDoubleConst(0, 2, buf15);
	EXPECT_EQ((double)resbuf7[0], v3->get(0)->getDouble());
	EXPECT_EQ((double)resbuf7[1], v3->get(1)->getDouble());

	delete[] buf9;
	delete[] buf10;
	delete[] buf11;
	delete[] buf12;
	delete[] buf13;
	delete[] buf14;
	delete[] buf15;
}

TEST_F(DataformVectorTest, testCharVector)
{
	VectorSP v1 = Util::createVector(DT_CHAR, 2, 2);
	v1->set(0, Util::createChar(1));
	v1->set(1, Util::createChar(0));
	string script = "a=[char(1),char(0)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(v1->getCategory(), INTEGRAL);

	EXPECT_ANY_THROW(v1->fill(1, 1, Util::createString("1")));
	EXPECT_TRUE(v1->validIndex(1));
	EXPECT_TRUE(v1->validIndex(0, 1, 1));
	EXPECT_FALSE(v1->validIndex(0, 1, '\0'));

	VectorSP subv1 = Util::createVector(DT_CHAR, 0, 2);
	subv1->append(Util::createChar('a'));
	subv1->append(Util::createChar('c'));
	subv1->upper();
	EXPECT_EQ(subv1->get(0)->getChar(), 'A');
	EXPECT_EQ(subv1->get(1)->getChar(), 'C');
	subv1->lower();
	EXPECT_EQ(subv1->get(0)->getChar(), 'a');
	EXPECT_EQ(subv1->get(1)->getChar(), 'c');

	VectorSP indexVec = Util::createIndexVector(0, 1);
	ConstantSP index = Util::createInt(0);
	subv1->set(indexVec, Util::createChar(3));
	EXPECT_EQ(subv1->get(0)->getChar(), (char)3);
	subv1->set(index, Util::createChar(5));
	EXPECT_EQ(subv1->get(0)->getChar(), (char)5);
	subv1->set(index, Util::createNullConstant(DT_CHAR));
	EXPECT_TRUE(subv1->get(0)->isNull());

	char *buf = new char[10];
	char *buf_1 = new char[10];
	INDEX *buf_2 = new INDEX[2];
	buf_2[0] = 7;
	buf_2[1] = 9;
	// string *buf3 = new string[10];
	v1->nullFill(Util::createChar(3));
	EXPECT_EQ(v1->getString(), "[1,0]");
	v1->isNull(0, 2, buf);
	v1->isValid(0, 2, buf_1);
	EXPECT_FALSE((int)buf[0]);
	EXPECT_TRUE((int)buf_1[0]);

	v1->append(Util::createNullConstant(DT_CHAR));
	v1->nullFill(Util::createChar(9));
	v1->isNull(0, 3, buf);
	v1->isValid(0, 3, buf_1);
	EXPECT_FALSE((int)buf[0]);
	EXPECT_TRUE((int)buf_1[0]);
	EXPECT_EQ(v1->getString(), "[1,0,9]");
	v1->append(Util::createNullConstant(DT_CHAR));
	v1->nullFill(Util::createFloat(2.1));
	EXPECT_EQ(v1->getString(), "[1,0,9,2]");

	v1->getIndex(0, 2, buf_2);
	EXPECT_EQ(buf_2[0], 1);
	EXPECT_EQ(v1->getIndexConst(0, 2, buf_2)[0], 1);
	EXPECT_EQ(v1->getIndexBuffer(0, 2, buf_2)[0], 1);
	// cout<<v1->getString(0,2,buf3)<<endl;
	v1->setIndex(0, 2, buf_2);
	EXPECT_EQ(buf_2[0], 1);

	delete[] buf_1, buf_2, buf;

	VectorSP v2 = Util::createVector(DT_CHAR, 2, 10);
	v2->set(0, Util::createChar(1));
	v2->set(1, Util::createChar(0));
	char *buf1 = new char[2];
	buf1[0] = 1;
	short *buf2 = new short[2];
	buf2[0] = 1;
	int *buf3 = new int[2];
	buf3[0] = 1;
	INDEX *buf4 = new INDEX[2];
	buf4[0] = 1;
	long long *buf5 = new long long[2];
	buf5[0] = 1;
	float *buf6 = new float[2];
	buf6[0] = 1;
	double *buf7 = new double[2];
	buf7[0] = 1;

	// cout<<v2->getString()<<endl;
	v2->appendBool(buf1, 1);
	EXPECT_EQ(v2->getString(), "[1,0,1]");
	v2->appendChar(buf1, 1);
	EXPECT_EQ(v2->getString(), "[1,0,1,1]");
	v2->appendShort(buf2, 1);
	EXPECT_EQ(v2->getString(), "[1,0,1,1,1]");
	v2->appendInt(buf3, 1);
	EXPECT_EQ(v2->getString(), "[1,0,1,1,1,1]");
	v2->appendLong(buf5, 1);
	EXPECT_EQ(v2->getString(), "[1,0,1,1,1,1,1]");
	v2->appendIndex(buf4, 1);
	EXPECT_EQ(v2->getString(), "[1,0,1,1,1,1,1,1]");
	v2->appendFloat(buf6, 1);
	EXPECT_EQ(v2->getString(), "[1,0,1,1,1,1,1,1,1]");
	v2->appendDouble(buf7, 1);
	EXPECT_EQ(v2->getString(), "[1,0,1,1,1,1,1,1,1,1]");

	VectorSP v3 = Util::createVector(DT_CHAR, 2, 10);
	v3->set(0, Util::createChar(1));
	v3->set(1, Util::createChar(0));
	char *buf9 = new char[2];
	short *buf10 = new short[2];
	int *buf11 = new int[2];
	INDEX *buf12 = new INDEX[2];
	long long *buf13 = new long long[2];
	float *buf14 = new float[2];
	double *buf15 = new double[2];

	v3->getBool(0, 2, buf9);
	EXPECT_EQ((bool)buf9[0], v3->get(0)->getBool());
	EXPECT_EQ((bool)buf9[1], v3->get(1)->getBool());
	v3->getChar(0, 2, buf9);
	EXPECT_EQ((char)buf9[0], v3->get(0)->getChar());
	EXPECT_EQ((char)buf9[1], v3->get(1)->getChar());
	v3->getShort(0, 2, buf10);
	EXPECT_EQ((short)buf10[0], v3->get(0)->getShort());
	EXPECT_EQ((short)buf10[1], v3->get(1)->getShort());
	v3->getInt(0, 2, buf11);
	EXPECT_EQ((int)buf11[0], v3->get(0)->getInt());
	EXPECT_EQ((int)buf11[1], v3->get(1)->getInt());
	v3->getLong(0, 2, buf13);
	EXPECT_EQ((long long)buf13[0], v3->get(0)->getLong());
	EXPECT_EQ((long long)buf13[1], v3->get(1)->getLong());
	v3->getIndex(0, 2, buf12);
	EXPECT_EQ((INDEX)buf12[0], v3->get(0)->getIndex());
	EXPECT_EQ((INDEX)buf12[1], v3->get(1)->getIndex());
	v3->getFloat(0, 2, buf14);
	EXPECT_EQ((float)buf14[0], v3->get(0)->getFloat());
	EXPECT_EQ((float)buf14[1], v3->get(1)->getFloat());
	v3->getDouble(0, 2, buf15);
	EXPECT_EQ((double)buf15[0], v3->get(0)->getDouble());
	EXPECT_EQ((double)buf15[1], v3->get(1)->getDouble());

	memset(buf9, '\0', 2);
	memset(buf10, 0, 2);
	memset(buf11, 0, 2);
	memset(buf12, 0, 2);
	memset(buf13, 0, 2);
	memset(buf14, 0, 2);
	memset(buf15, 0, 2);

	// cout<<v3->getString()<<endl;

	const char *resbuf = v3->getBoolConst(0, 2, buf9);
	EXPECT_EQ((bool)resbuf[0], v3->get(0)->getBool());
	EXPECT_EQ((bool)resbuf[1], v3->get(1)->getBool());

	const char *resbuf1 = v3->getCharConst(0, 2, buf9);
	;
	EXPECT_EQ((char)resbuf1[0], v3->get(0)->getChar());
	EXPECT_EQ((char)resbuf1[1], v3->get(1)->getChar());

	const short *resbuf2 = v3->getShortConst(0, 2, buf10);
	EXPECT_EQ((short)resbuf2[0], v3->get(0)->getShort());
	EXPECT_EQ((short)resbuf2[1], v3->get(1)->getShort());

	const int *resbuf3 = v3->getIntConst(0, 2, buf11);
	EXPECT_EQ((int)resbuf3[0], v3->get(0)->getInt());
	EXPECT_EQ((int)resbuf3[1], v3->get(1)->getInt());

	const long long *resbuf4 = v3->getLongConst(0, 2, buf13);
	EXPECT_EQ((long long)resbuf4[0], v3->get(0)->getLong());
	EXPECT_EQ((long long)resbuf4[1], v3->get(1)->getLong());

	const INDEX *resbuf5 = v3->getIndexConst(0, 2, buf12);
	EXPECT_EQ((INDEX)resbuf5[0], v3->get(0)->getIndex());
	EXPECT_EQ((INDEX)resbuf5[1], v3->get(1)->getIndex());

	const float *resbuf6 = v3->getFloatConst(0, 2, buf14);
	EXPECT_EQ((float)resbuf6[0], v3->get(0)->getFloat());
	EXPECT_EQ((float)resbuf6[1], v3->get(1)->getFloat());

	const double *resbuf7 = v3->getDoubleConst(0, 2, buf15);
	EXPECT_EQ((double)resbuf7[0], v3->get(0)->getDouble());
	EXPECT_EQ((double)resbuf7[1], v3->get(1)->getDouble());

	delete[] buf1;
	delete[] buf2;
	delete[] buf3;
	delete[] buf4;
	delete[] buf5;
	delete[] buf6;
	delete[] buf7;
	delete[] buf15;
	delete[] buf9;
	delete[] buf10;
	delete[] buf11;
	delete[] buf12;
	delete[] buf13;
	delete[] buf14;
}

TEST_F(DataformVectorTest, testCharNullVector)
{
	VectorSP v1 = Util::createVector(DT_CHAR, 2, 2);
	v1->setNull(0);
	v1->setNull(1);
	string script = "a=[char(NULL),char(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0; i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());

	VectorSP v3 = Util::createVector(DT_CHAR, 2, 10, true, 0, (void *)0, true);
	v3->setNull(0);
	v3->setNull(1);
	char *buf9 = new char[2];
	short *buf10 = new short[2];
	int *buf11 = new int[2];
	INDEX *buf12 = new INDEX[2];
	long long *buf13 = new long long[2];
	float *buf14 = new float[2];
	double *buf15 = new double[2];

	v3->getBool(0, 2, buf9);
	EXPECT_EQ((bool)buf9[0], true);
	EXPECT_EQ((bool)buf9[1], true);
	v3->getChar(0, 2, buf9);
	EXPECT_EQ((char)buf9[0], v3->get(0)->getChar());
	EXPECT_EQ((char)buf9[1], v3->get(1)->getChar());
	v3->getShort(0, 2, buf10);
	EXPECT_EQ((short)buf10[0], v3->get(0)->getShort());
	EXPECT_EQ((short)buf10[1], v3->get(1)->getShort());
	v3->getInt(0, 2, buf11);
	EXPECT_EQ((int)buf11[0], v3->get(0)->getInt());
	EXPECT_EQ((int)buf11[1], v3->get(1)->getInt());
	v3->getLong(0, 2, buf13);
	EXPECT_EQ((long long)buf13[0], v3->get(0)->getLong());
	EXPECT_EQ((long long)buf13[1], v3->get(1)->getLong());
	v3->getIndex(0, 2, buf12);
	EXPECT_EQ((INDEX)buf12[0], v3->get(0)->getIndex());
	EXPECT_EQ((INDEX)buf12[1], v3->get(1)->getIndex());
	v3->getFloat(0, 2, buf14);
	EXPECT_EQ((float)buf14[0], v3->get(0)->getFloat());
	EXPECT_EQ((float)buf14[1], v3->get(1)->getFloat());
	v3->getDouble(0, 2, buf15);
	EXPECT_EQ((double)buf15[0], v3->get(0)->getDouble());
	EXPECT_EQ((double)buf15[1], v3->get(1)->getDouble());

	memset(buf9, '\0', 2);
	memset(buf10, 0, 2);
	memset(buf11, 0, 2);
	memset(buf12, 0, 2);
	memset(buf13, 0, 2);
	memset(buf14, 0, 2);
	memset(buf15, 0, 2);

	// cout<<v3->getString()<<endl;

	const char *resbuf = v3->getBoolConst(0, 2, buf9);
	EXPECT_EQ((bool)resbuf[0], true);
	EXPECT_EQ((bool)resbuf[1], true);

	const char *resbuf1 = v3->getCharConst(0, 2, buf9);
	;
	EXPECT_EQ((char)resbuf1[0], v3->get(0)->getChar());
	EXPECT_EQ((char)resbuf1[1], v3->get(1)->getChar());

	const short *resbuf2 = v3->getShortConst(0, 2, buf10);
	EXPECT_EQ((short)resbuf2[0], v3->get(0)->getShort());
	EXPECT_EQ((short)resbuf2[1], v3->get(1)->getShort());

	const int *resbuf3 = v3->getIntConst(0, 2, buf11);
	EXPECT_EQ((int)resbuf3[0], v3->get(0)->getInt());
	EXPECT_EQ((int)resbuf3[1], v3->get(1)->getInt());

	const long long *resbuf4 = v3->getLongConst(0, 2, buf13);
	EXPECT_EQ((long long)resbuf4[0], v3->get(0)->getLong());
	EXPECT_EQ((long long)resbuf4[1], v3->get(1)->getLong());

	const INDEX *resbuf5 = v3->getIndexConst(0, 2, buf12);
	EXPECT_EQ((INDEX)resbuf5[0], v3->get(0)->getIndex());
	EXPECT_EQ((INDEX)resbuf5[1], v3->get(1)->getIndex());

	const float *resbuf6 = v3->getFloatConst(0, 2, buf14);
	EXPECT_EQ((float)resbuf6[0], v3->get(0)->getFloat());
	EXPECT_EQ((float)resbuf6[1], v3->get(1)->getFloat());

	const double *resbuf7 = v3->getDoubleConst(0, 2, buf15);
	EXPECT_EQ((double)resbuf7[0], v3->get(0)->getDouble());
	EXPECT_EQ((double)resbuf7[1], v3->get(1)->getDouble());

	delete[] buf9;
	delete[] buf10;
	delete[] buf11;
	delete[] buf12;
	delete[] buf13;
	delete[] buf14;
	delete[] buf15;
}

TEST_F(DataformVectorTest, testIntVector)
{
	VectorSP v1 = Util::createVector(DT_INT, 2, 2);
	v1->set(0, Util::createInt(1));
	v1->set(1, Util::createInt(0));
	string script = "a=[int(1),int(0)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	v1->append(Util::createInt(2));
	EXPECT_FALSE(v1->isSorted(false));
	EXPECT_FALSE(v1->isSorted(false, true));
	EXPECT_FALSE(v1->hasNull(0, 2));

	EXPECT_ANY_THROW(v1->fill(1, 1, Util::createString("1")));
	EXPECT_TRUE(v1->validIndex(2));
	EXPECT_TRUE(v1->validIndex(0, 1, 1));
	EXPECT_FALSE(v1->validIndex(0, 1, (int)0));

	VectorSP subv1 = Util::createVector(DT_INT, 0, 2);
	subv1->append(Util::createInt(1));
	subv1->append(Util::createInt(2));
	EXPECT_FALSE(v1->compare(0, Util::createInt(1)));

	VectorSP indexVec = Util::createIndexVector(0, 1);
	ConstantSP index = Util::createInt(0);
	subv1->set(indexVec, Util::createInt(3));
	EXPECT_EQ(subv1->get(0)->getInt(), (int)3);
	subv1->set(index, Util::createInt(5));
	EXPECT_EQ(subv1->get(0)->getInt(), (int)5);
	subv1->set(index, Util::createNullConstant(DT_INT));
	EXPECT_TRUE(subv1->get(0)->isNull());
	subv1->set(1, Util::createNullConstant(DT_INT));
	EXPECT_TRUE(subv1->get(1)->isNull());

	VectorSP v2 = Util::createVector(DT_INT, 1, 1);
	v2->set(0, Util::createNullConstant(DT_INT));
	EXPECT_EQ(v2->getBool(), CHAR_MIN);
	EXPECT_EQ(v2->getChar(), CHAR_MIN);
	EXPECT_EQ(v2->getShort(), SHRT_MIN);
	EXPECT_EQ(v2->getInt(), INT_MIN);
	EXPECT_EQ(v2->getIndex(), INDEX_MIN);
	EXPECT_EQ(v2->getLong(), LLONG_MIN);
	EXPECT_EQ(v2->getDouble(), DBL_NMIN);
	EXPECT_EQ(v2->getFloat(), FLT_NMIN);

	v2->set(0, Util::createInt(0));
	EXPECT_FALSE(v2->getBool());
	EXPECT_EQ(v2->getChar(), (char)0);
	EXPECT_EQ(v2->getShort(), (short)0);
	EXPECT_EQ(v2->getInt(), (int)0);
	EXPECT_EQ(v2->getIndex(), (INDEX)0);
	EXPECT_EQ(v2->getLong(), (long long)0);
	EXPECT_EQ(v2->getDouble(), (double)0);
	EXPECT_EQ(v2->getFloat(), (float)0);

	EXPECT_ANY_THROW(v1->getBool());
	EXPECT_ANY_THROW(v1->getChar());
	EXPECT_ANY_THROW(v1->getShort());
	EXPECT_ANY_THROW(v1->getInt());
	EXPECT_ANY_THROW(v1->getIndex());
	EXPECT_ANY_THROW(v1->getLong());
	EXPECT_ANY_THROW(v1->getDouble());
	EXPECT_ANY_THROW(v1->getFloat());

	v2->setBool(1);
	EXPECT_EQ(v2->getBool(0), (bool)1);
	v2->setChar(1);
	EXPECT_EQ(v2->get(0)->getChar(), (char)1);
	v2->setShort(1);
	EXPECT_EQ(v2->get(0)->getShort(), (short)1);
	v2->setInt(1);
	EXPECT_EQ(v2->get(0)->getInt(), (int)1);
	v2->setLong(1);
	EXPECT_EQ(v2->get(0)->getLong(), (long long)1);
	v2->setIndex(1);
	EXPECT_EQ(v2->get(0)->getIndex(), (INDEX)1);
	v2->setFloat(1);
	EXPECT_EQ(v2->get(0)->getFloat(), (float)1);
	v2->setDouble(1);
	EXPECT_EQ(v2->get(0)->getDouble(), (double)1);
	v2->setString("1");
	EXPECT_EQ(v2->get(0)->getString(), "1");

	VectorSP v3 = Util::createVector(DT_INT, 2, 2);
	v3->setNull(); // code nothing to do
	v3->set(0, Util::createInt(0));
	v3->set(1, Util::createInt(1));
	v3->remove(Util::createInt(1000));

	v3->next(-1);
	v3->next(1000);
	v3->next(1);
	EXPECT_EQ(v3->getString(), "[1,]");
	v3->prev(-1);
	v3->prev(1000);
	v3->prev(1);
	EXPECT_EQ(v3->getString(), "[,1]");

	VectorSP v4 = Util::createVector(DT_INT, 2, 10);
	v4->set(0, Util::createInt(1));
	v4->set(1, Util::createInt(0));
	char *buf_1 = new char[2];
	buf_1[0] = 1;
	short *buf_2 = new short[2];
	buf_2[0] = 1;
	int *buf_3 = new int[2];
	buf_3[0] = 1;
	INDEX *buf_4 = new INDEX[2];
	buf_4[0] = 1;
	long long *buf_5 = new long long[2];
	buf_5[0] = 1;
	float *buf_6 = new float[2];
	buf_6[0] = 1;
	double *buf_7 = new double[2];
	buf_7[0] = 1;

	// cout<<v4->getString()<<endl;
	v4->appendBool(buf_1, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1]");
	v4->appendChar(buf_1, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1]");
	v4->appendShort(buf_2, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1]");
	v4->appendInt(buf_3, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1,1]");
	v4->appendLong(buf_5, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1,1,1]");
	v4->appendIndex(buf_4, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1]");
	v4->appendFloat(buf_6, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1,1]");
	v4->appendDouble(buf_7, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1,1,1]");

	VectorSP v5 = Util::createVector(DT_INT, 2, 10);
	v5->set(0, Util::createInt(1));
	v5->set(1, Util::createInt(0));
	char *buf9 = new char[2];
	short *buf10 = new short[2];
	int *buf11 = new int[2];
	INDEX *buf12 = new INDEX[2];
	long long *buf13 = new long long[2];
	float *buf14 = new float[2];
	double *buf15 = new double[2];

	v5->getBool(0, 2, buf9);
	EXPECT_EQ((bool)buf9[0], v5->get(0)->getBool());
	EXPECT_EQ((bool)buf9[1], v5->get(1)->getBool());
	v5->getChar(0, 2, buf9);
	EXPECT_EQ((char)buf9[0], v5->get(0)->getChar());
	EXPECT_EQ((char)buf9[1], v5->get(1)->getChar());
	v5->getShort(0, 2, buf10);
	EXPECT_EQ((short)buf10[0], v5->get(0)->getShort());
	EXPECT_EQ((short)buf10[1], v5->get(1)->getShort());
	v5->getInt(0, 2, buf11);
	EXPECT_EQ((int)buf11[0], v5->get(0)->getInt());
	EXPECT_EQ((int)buf11[1], v5->get(1)->getInt());
	v5->getLong(0, 2, buf13);
	EXPECT_EQ((long long)buf13[0], v5->get(0)->getLong());
	EXPECT_EQ((long long)buf13[1], v5->get(1)->getLong());
	v5->getIndex(0, 2, buf12);
	EXPECT_EQ((INDEX)buf12[0], v5->get(0)->getIndex());
	EXPECT_EQ((INDEX)buf12[1], v5->get(1)->getIndex());
	v5->getFloat(0, 2, buf14);
	EXPECT_EQ((float)buf14[0], v5->get(0)->getFloat());
	EXPECT_EQ((float)buf14[1], v5->get(1)->getFloat());
	v5->getDouble(0, 2, buf15);
	EXPECT_EQ((double)buf15[0], v5->get(0)->getDouble());
	EXPECT_EQ((double)buf15[1], v5->get(1)->getDouble());

	memset(buf9, '\0', 2);
	memset(buf10, 0, 2);
	memset(buf11, 0, 2);
	memset(buf12, 0, 2);
	memset(buf13, 0, 2);
	memset(buf14, 0, 2);
	memset(buf15, 0, 2);

	// cout<<v5->getString()<<endl;

	const char *resbuf = v5->getBoolConst(0, 2, buf9);
	EXPECT_EQ((bool)resbuf[0], v5->get(0)->getBool());
	EXPECT_EQ((bool)resbuf[1], v5->get(1)->getBool());

	const char *resbuf1 = v5->getCharConst(0, 2, buf9);
	;
	EXPECT_EQ((char)resbuf1[0], v5->get(0)->getChar());
	EXPECT_EQ((char)resbuf1[1], v5->get(1)->getChar());

	const short *resbuf2 = v5->getShortConst(0, 2, buf10);
	EXPECT_EQ((short)resbuf2[0], v5->get(0)->getShort());
	EXPECT_EQ((short)resbuf2[1], v5->get(1)->getShort());

	const int *resbuf3 = v5->getIntConst(0, 2, buf11);
	EXPECT_EQ((int)resbuf3[0], v5->get(0)->getInt());
	EXPECT_EQ((int)resbuf3[1], v5->get(1)->getInt());

	const long long *resbuf4 = v5->getLongConst(0, 2, buf13);
	EXPECT_EQ((long long)resbuf4[0], v5->get(0)->getLong());
	EXPECT_EQ((long long)resbuf4[1], v5->get(1)->getLong());

	const INDEX *resbuf5 = v5->getIndexConst(0, 2, buf12);
	EXPECT_EQ((INDEX)resbuf5[0], v5->get(0)->getIndex());
	EXPECT_EQ((INDEX)resbuf5[1], v5->get(1)->getIndex());

	const float *resbuf6 = v5->getFloatConst(0, 2, buf14);
	EXPECT_EQ((float)resbuf6[0], v5->get(0)->getFloat());
	EXPECT_EQ((float)resbuf6[1], v5->get(1)->getFloat());

	const double *resbuf7 = v5->getDoubleConst(0, 2, buf15);
	EXPECT_EQ((double)resbuf7[0], v5->get(0)->getDouble());
	EXPECT_EQ((double)resbuf7[1], v5->get(1)->getDouble());

	delete[] buf_1;
	delete[] buf_2;
	delete[] buf_3;
	delete[] buf_4;
	delete[] buf_5;
	delete[] buf_6;
	delete[] buf_7;
	delete[] buf15;
	delete[] buf9;
	delete[] buf10;
	delete[] buf11;
	delete[] buf12;
	delete[] buf13;
	delete[] buf14;
}

TEST_F(DataformVectorTest, testIntNullVector)
{
	VectorSP v1 = Util::createVector(DT_INT, 2, 2);
	v1->setNull(0);
	v1->setNull(1);
	string script = "a=[int(NULL),int(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0; i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());

	VectorSP v3 = Util::createVector(DT_INT, 2, 10, true, 0, (void *)0, true);
	v3->setNull(0);
	v3->setNull(1);
	char *buf9 = new char[2];
	short *buf10 = new short[2];
	int *buf11 = new int[2];
	INDEX *buf12 = new INDEX[2];
	long long *buf13 = new long long[2];
	float *buf14 = new float[2];
	double *buf15 = new double[2];

	v3->getBool(0, 2, buf9);
	EXPECT_EQ((bool)buf9[0], true);
	EXPECT_EQ((bool)buf9[1], true);
	v3->getChar(0, 2, buf9);
	EXPECT_EQ((char)buf9[0], v3->get(0)->getChar());
	EXPECT_EQ((char)buf9[1], v3->get(1)->getChar());
	v3->getShort(0, 2, buf10);
	EXPECT_EQ((short)buf10[0], v3->get(0)->getShort());
	EXPECT_EQ((short)buf10[1], v3->get(1)->getShort());
	v3->getInt(0, 2, buf11);
	EXPECT_EQ((int)buf11[0], v3->get(0)->getInt());
	EXPECT_EQ((int)buf11[1], v3->get(1)->getInt());
	v3->getLong(0, 2, buf13);
	EXPECT_EQ((long long)buf13[0], v3->get(0)->getLong());
	EXPECT_EQ((long long)buf13[1], v3->get(1)->getLong());
	v3->getIndex(0, 2, buf12);
	EXPECT_EQ((INDEX)buf12[0], v3->get(0)->getIndex());
	EXPECT_EQ((INDEX)buf12[1], v3->get(1)->getIndex());
	v3->getFloat(0, 2, buf14);
	EXPECT_EQ((float)buf14[0], v3->get(0)->getFloat());
	EXPECT_EQ((float)buf14[1], v3->get(1)->getFloat());
	v3->getDouble(0, 2, buf15);
	EXPECT_EQ((double)buf15[0], v3->get(0)->getDouble());
	EXPECT_EQ((double)buf15[1], v3->get(1)->getDouble());

	memset(buf9, '\0', 2);
	memset(buf10, 0, 2);
	memset(buf11, 0, 2);
	memset(buf12, 0, 2);
	memset(buf13, 0, 2);
	memset(buf14, 0, 2);
	memset(buf15, 0, 2);

	// cout<<v3->getString()<<endl;

	const char *resbuf = v3->getBoolConst(0, 2, buf9);
	EXPECT_EQ((bool)resbuf[0], true);
	EXPECT_EQ((bool)resbuf[1], true);

	const char *resbuf1 = v3->getCharConst(0, 2, buf9);
	;
	EXPECT_EQ((char)resbuf1[0], v3->get(0)->getChar());
	EXPECT_EQ((char)resbuf1[1], v3->get(1)->getChar());

	const short *resbuf2 = v3->getShortConst(0, 2, buf10);
	EXPECT_EQ((short)resbuf2[0], v3->get(0)->getShort());
	EXPECT_EQ((short)resbuf2[1], v3->get(1)->getShort());

	const int *resbuf3 = v3->getIntConst(0, 2, buf11);
	EXPECT_EQ((int)resbuf3[0], v3->get(0)->getInt());
	EXPECT_EQ((int)resbuf3[1], v3->get(1)->getInt());

	const long long *resbuf4 = v3->getLongConst(0, 2, buf13);
	EXPECT_EQ((long long)resbuf4[0], v3->get(0)->getLong());
	EXPECT_EQ((long long)resbuf4[1], v3->get(1)->getLong());

	const INDEX *resbuf5 = v3->getIndexConst(0, 2, buf12);
	EXPECT_EQ((INDEX)resbuf5[0], v3->get(0)->getIndex());
	EXPECT_EQ((INDEX)resbuf5[1], v3->get(1)->getIndex());

	const float *resbuf6 = v3->getFloatConst(0, 2, buf14);
	EXPECT_EQ((float)resbuf6[0], v3->get(0)->getFloat());
	EXPECT_EQ((float)resbuf6[1], v3->get(1)->getFloat());

	const double *resbuf7 = v3->getDoubleConst(0, 2, buf15);
	EXPECT_EQ((double)resbuf7[0], v3->get(0)->getDouble());
	EXPECT_EQ((double)resbuf7[1], v3->get(1)->getDouble());

	delete[] buf9;
	delete[] buf10;
	delete[] buf11;
	delete[] buf12;
	delete[] buf13;
	delete[] buf14;
	delete[] buf15;
}

TEST_F(DataformVectorTest, testLongVector)
{
	VectorSP v1 = Util::createVector(DT_LONG, 2, 2);
	v1->set(0, Util::createLong(1));
	v1->set(1, Util::createLong(0));
	string script = "a=[long(1),long(0)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());

	EXPECT_ANY_THROW(v1->fill(1, 1, Util::createString("1")));
	EXPECT_TRUE(v1->validIndex(2));
	EXPECT_TRUE(v1->validIndex(0, 1, 1));
	EXPECT_FALSE(v1->validIndex(0, 1, (long long)0));

	VectorSP subv1 = Util::createVector(DT_LONG, 0, 2);
	subv1->append(Util::createLong(1));
	subv1->append(Util::createLong(2));
	EXPECT_EQ(v1->compare(0, Util::createLong(1)), 0);
	EXPECT_EQ(v1->compare(0, Util::createLong(0)), 1);
	EXPECT_EQ(v1->compare(0, Util::createLong(3)), -1);

	VectorSP indexVec = Util::createIndexVector(0, 1);
	ConstantSP index = Util::createLong(0);
	subv1->set(indexVec, Util::createLong(3));
	EXPECT_EQ(subv1->get(0)->getLong(), (long long)3);
	subv1->set(index, Util::createLong(5));
	EXPECT_EQ(subv1->get(0)->getLong(), (long long)5);
	subv1->set(index, Util::createNullConstant(DT_LONG));
	EXPECT_TRUE(subv1->get(0)->isNull());
	subv1->set(1, Util::createNullConstant(DT_LONG));
	EXPECT_TRUE(subv1->get(1)->isNull());

	v1->add(0, 2, (long long)1);
	EXPECT_EQ(v1->getString(), "[2,1]");
	v1->neg();
	EXPECT_EQ(v1->getString(), "[-2,-1]");
	v1->append(Util::createNullConstant(DT_LONG));
	v1->replace(Util::createNullConstant(DT_LONG), Util::createLong(4));
	v1->replace(Util::createLong(-2), Util::createNullConstant(DT_LONG));
	v1->replace(Util::createLong(-1), Util::createLong(3));
	EXPECT_EQ(v1->getString(), "[,3,4]");

	VectorSP v4 = Util::createVector(DT_LONG, 2, 10);
	v4->set(0, Util::createLong(1));
	v4->set(1, Util::createLong(0));
	char *buf_1 = new char[2];
	buf_1[0] = 1;
	short *buf_2 = new short[2];
	buf_2[0] = 1;
	int *buf_3 = new int[2];
	buf_3[0] = 1;
	INDEX *buf_4 = new INDEX[2];
	buf_4[0] = 1;
	long long *buf_5 = new long long[2];
	buf_5[0] = 1;
	float *buf_6 = new float[2];
	buf_6[0] = 1;
	double *buf_7 = new double[2];
	buf_7[0] = 1;

	// cout<<v4->getString()<<endl;
	v4->appendBool(buf_1, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1]");
	v4->appendChar(buf_1, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1]");
	v4->appendShort(buf_2, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1]");
	v4->appendInt(buf_3, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1,1]");
	v4->appendLong(buf_5, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1,1,1]");
	v4->appendIndex(buf_4, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1]");
	v4->appendFloat(buf_6, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1,1]");
	v4->appendDouble(buf_7, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1,1,1]");

	VectorSP v5 = Util::createVector(DT_LONG, 2, 10);
	v5->set(0, Util::createLong(1));
	v5->set(1, Util::createLong(0));
	char *buf9 = new char[2];
	short *buf10 = new short[2];
	int *buf11 = new int[2];
	INDEX *buf12 = new INDEX[2];
	long long *buf13 = new long long[2];
	float *buf14 = new float[2];
	double *buf15 = new double[2];

	v5->getBool(0, 2, buf9);
	EXPECT_EQ((bool)buf9[0], v5->get(0)->getBool());
	EXPECT_EQ((bool)buf9[1], v5->get(1)->getBool());
	v5->getChar(0, 2, buf9);
	EXPECT_EQ((char)buf9[0], v5->get(0)->getChar());
	EXPECT_EQ((char)buf9[1], v5->get(1)->getChar());
	v5->getShort(0, 2, buf10);
	EXPECT_EQ((short)buf10[0], v5->get(0)->getShort());
	EXPECT_EQ((short)buf10[1], v5->get(1)->getShort());
	v5->getInt(0, 2, buf11);
	EXPECT_EQ((int)buf11[0], v5->get(0)->getInt());
	EXPECT_EQ((int)buf11[1], v5->get(1)->getInt());
	v5->getLong(0, 2, buf13);
	EXPECT_EQ((long long)buf13[0], v5->get(0)->getLong());
	EXPECT_EQ((long long)buf13[1], v5->get(1)->getLong());
	v5->getIndex(0, 2, buf12);
	EXPECT_EQ((INDEX)buf12[0], v5->get(0)->getIndex());
	EXPECT_EQ((INDEX)buf12[1], v5->get(1)->getIndex());
	v5->getFloat(0, 2, buf14);
	EXPECT_EQ((float)buf14[0], v5->get(0)->getFloat());
	EXPECT_EQ((float)buf14[1], v5->get(1)->getFloat());
	v5->getDouble(0, 2, buf15);
	EXPECT_EQ((double)buf15[0], v5->get(0)->getDouble());
	EXPECT_EQ((double)buf15[1], v5->get(1)->getDouble());

	memset(buf9, '\0', 2);
	memset(buf10, 0, 2);
	memset(buf11, 0, 2);
	memset(buf12, 0, 2);
	memset(buf13, 0, 2);
	memset(buf14, 0, 2);
	memset(buf15, 0, 2);

	// cout<<v5->getString()<<endl;

	const char *resbuf = v5->getBoolConst(0, 2, buf9);
	EXPECT_EQ((bool)resbuf[0], v5->get(0)->getBool());
	EXPECT_EQ((bool)resbuf[1], v5->get(1)->getBool());

	const char *resbuf1 = v5->getCharConst(0, 2, buf9);
	;
	EXPECT_EQ((char)resbuf1[0], v5->get(0)->getChar());
	EXPECT_EQ((char)resbuf1[1], v5->get(1)->getChar());

	const short *resbuf2 = v5->getShortConst(0, 2, buf10);
	EXPECT_EQ((short)resbuf2[0], v5->get(0)->getShort());
	EXPECT_EQ((short)resbuf2[1], v5->get(1)->getShort());

	const int *resbuf3 = v5->getIntConst(0, 2, buf11);
	EXPECT_EQ((int)resbuf3[0], v5->get(0)->getInt());
	EXPECT_EQ((int)resbuf3[1], v5->get(1)->getInt());

	const long long *resbuf4 = v5->getLongConst(0, 2, buf13);
	EXPECT_EQ((long long)resbuf4[0], v5->get(0)->getLong());
	EXPECT_EQ((long long)resbuf4[1], v5->get(1)->getLong());

	const INDEX *resbuf5 = v5->getIndexConst(0, 2, buf12);
	EXPECT_EQ((INDEX)resbuf5[0], v5->get(0)->getIndex());
	EXPECT_EQ((INDEX)resbuf5[1], v5->get(1)->getIndex());

	const float *resbuf6 = v5->getFloatConst(0, 2, buf14);
	EXPECT_EQ((float)resbuf6[0], v5->get(0)->getFloat());
	EXPECT_EQ((float)resbuf6[1], v5->get(1)->getFloat());

	const double *resbuf7 = v5->getDoubleConst(0, 2, buf15);
	EXPECT_EQ((double)resbuf7[0], v5->get(0)->getDouble());
	EXPECT_EQ((double)resbuf7[1], v5->get(1)->getDouble());

	delete[] buf_1;
	delete[] buf_2;
	delete[] buf_3;
	delete[] buf_4;
	delete[] buf_5;
	delete[] buf_6;
	delete[] buf_7;
	delete[] buf15;
	delete[] buf9;
	delete[] buf10;
	delete[] buf11;
	delete[] buf12;
	delete[] buf13;
	delete[] buf14;

	VectorSP v6 = Util::createVector(DT_LONG, 2, 2);
	v6->set(0, Util::createLong(1));
	v6->set(1, Util::createLong(0));
}

TEST_F(DataformVectorTest, testLongNullVector)
{
	VectorSP v1 = Util::createVector(DT_LONG, 2, 2);
	v1->setNull(0);
	v1->setNull(1);
	string script = "a=[long(NULL),long(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0; i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
	v1->add(0, 2, (long long)1);
	EXPECT_EQ(v1->getString(), "[" + to_string(LLONG_MIN + 1) + "," + to_string(LLONG_MIN + 1) + "]");
	v1->set(0, Util::createLong(1));
	v1->neg();
	EXPECT_EQ(v1->getString(), "[-1," + to_string(LLONG_MAX) + "]");

	VectorSP v3 = Util::createVector(DT_LONG, 2, 10, true, 0, (void *)0, true);
	v3->setNull(0);
	v3->setNull(1);
	char *buf9 = new char[2];
	short *buf10 = new short[2];
	int *buf11 = new int[2];
	INDEX *buf12 = new INDEX[2];
	long long *buf13 = new long long[2];
	float *buf14 = new float[2];
	double *buf15 = new double[2];

	v3->getBool(0, 2, buf9);
	EXPECT_EQ((bool)buf9[0], true);
	EXPECT_EQ((bool)buf9[1], true);
	v3->getChar(0, 2, buf9);
	EXPECT_EQ((char)buf9[0], v3->get(0)->getChar());
	EXPECT_EQ((char)buf9[1], v3->get(1)->getChar());
	v3->getShort(0, 2, buf10);
	EXPECT_EQ((short)buf10[0], v3->get(0)->getShort());
	EXPECT_EQ((short)buf10[1], v3->get(1)->getShort());
	v3->getInt(0, 2, buf11);
	EXPECT_EQ((int)buf11[0], v3->get(0)->getInt());
	EXPECT_EQ((int)buf11[1], v3->get(1)->getInt());
	v3->getLong(0, 2, buf13);
	EXPECT_EQ((long long)buf13[0], v3->get(0)->getLong());
	EXPECT_EQ((long long)buf13[1], v3->get(1)->getLong());
	v3->getIndex(0, 2, buf12);
	EXPECT_EQ((INDEX)buf12[0], v3->get(0)->getIndex());
	EXPECT_EQ((INDEX)buf12[1], v3->get(1)->getIndex());
	v3->getFloat(0, 2, buf14);
	EXPECT_EQ((float)buf14[0], v3->get(0)->getFloat());
	EXPECT_EQ((float)buf14[1], v3->get(1)->getFloat());
	v3->getDouble(0, 2, buf15);
	EXPECT_EQ((double)buf15[0], v3->get(0)->getDouble());
	EXPECT_EQ((double)buf15[1], v3->get(1)->getDouble());

	memset(buf9, '\0', 2);
	memset(buf10, 0, 2);
	memset(buf11, 0, 2);
	memset(buf12, 0, 2);
	memset(buf13, 0, 2);
	memset(buf14, 0, 2);
	memset(buf15, 0, 2);

	// cout<<v3->getString()<<endl;

	const char *resbuf = v3->getBoolConst(0, 2, buf9);
	EXPECT_EQ((bool)resbuf[0], true);
	EXPECT_EQ((bool)resbuf[1], true);

	const char *resbuf1 = v3->getCharConst(0, 2, buf9);
	;
	EXPECT_EQ((char)resbuf1[0], v3->get(0)->getChar());
	EXPECT_EQ((char)resbuf1[1], v3->get(1)->getChar());

	const short *resbuf2 = v3->getShortConst(0, 2, buf10);
	EXPECT_EQ((short)resbuf2[0], v3->get(0)->getShort());
	EXPECT_EQ((short)resbuf2[1], v3->get(1)->getShort());

	const int *resbuf3 = v3->getIntConst(0, 2, buf11);
	EXPECT_EQ((int)resbuf3[0], v3->get(0)->getInt());
	EXPECT_EQ((int)resbuf3[1], v3->get(1)->getInt());

	const long long *resbuf4 = v3->getLongConst(0, 2, buf13);
	EXPECT_EQ((long long)resbuf4[0], v3->get(0)->getLong());
	EXPECT_EQ((long long)resbuf4[1], v3->get(1)->getLong());

	const INDEX *resbuf5 = v3->getIndexConst(0, 2, buf12);
	EXPECT_EQ((INDEX)resbuf5[0], v3->get(0)->getIndex());
	EXPECT_EQ((INDEX)resbuf5[1], v3->get(1)->getIndex());

	const float *resbuf6 = v3->getFloatConst(0, 2, buf14);
	EXPECT_EQ((float)resbuf6[0], v3->get(0)->getFloat());
	EXPECT_EQ((float)resbuf6[1], v3->get(1)->getFloat());

	const double *resbuf7 = v3->getDoubleConst(0, 2, buf15);
	EXPECT_EQ((double)resbuf7[0], v3->get(0)->getDouble());
	EXPECT_EQ((double)resbuf7[1], v3->get(1)->getDouble());

	delete[] buf9;
	delete[] buf10;
	delete[] buf11;
	delete[] buf12;
	delete[] buf13;
	delete[] buf14;
	delete[] buf15;
}

TEST_F(DataformVectorTest, testShortNullVector)
{
	VectorSP v1 = Util::createVector(DT_SHORT, 2, 2);
	v1->setNull(0);
	v1->setNull(1);
	string script = "a=[short(NULL),short(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0; i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());

	VectorSP v3 = Util::createVector(DT_SHORT, 2, 10, true, 0, (void *)0, true);
	v3->setNull(0);
	v3->setNull(1);
	char *buf9 = new char[2];
	short *buf10 = new short[2];
	int *buf11 = new int[2];
	INDEX *buf12 = new INDEX[2];
	long long *buf13 = new long long[2];
	float *buf14 = new float[2];
	double *buf15 = new double[2];

	v3->getBool(0, 2, buf9);
	EXPECT_EQ((bool)buf9[0], true);
	EXPECT_EQ((bool)buf9[1], true);
	v3->getChar(0, 2, buf9);
	EXPECT_EQ((char)buf9[0], v3->get(0)->getChar());
	EXPECT_EQ((char)buf9[1], v3->get(1)->getChar());
	v3->getShort(0, 2, buf10);
	EXPECT_EQ((short)buf10[0], v3->get(0)->getShort());
	EXPECT_EQ((short)buf10[1], v3->get(1)->getShort());
	v3->getInt(0, 2, buf11);
	EXPECT_EQ((int)buf11[0], v3->get(0)->getInt());
	EXPECT_EQ((int)buf11[1], v3->get(1)->getInt());
	v3->getLong(0, 2, buf13);
	EXPECT_EQ((long long)buf13[0], v3->get(0)->getLong());
	EXPECT_EQ((long long)buf13[1], v3->get(1)->getLong());
	v3->getIndex(0, 2, buf12);
	EXPECT_EQ((INDEX)buf12[0], v3->get(0)->getIndex());
	EXPECT_EQ((INDEX)buf12[1], v3->get(1)->getIndex());
	v3->getFloat(0, 2, buf14);
	EXPECT_EQ((float)buf14[0], v3->get(0)->getFloat());
	EXPECT_EQ((float)buf14[1], v3->get(1)->getFloat());
	v3->getDouble(0, 2, buf15);
	EXPECT_EQ((double)buf15[0], v3->get(0)->getDouble());
	EXPECT_EQ((double)buf15[1], v3->get(1)->getDouble());

	memset(buf9, '\0', 2);
	memset(buf10, 0, 2);
	memset(buf11, 0, 2);
	memset(buf12, 0, 2);
	memset(buf13, 0, 2);
	memset(buf14, 0, 2);
	memset(buf15, 0, 2);

	// cout<<v3->getString()<<endl;

	const char *resbuf = v3->getBoolConst(0, 2, buf9);
	EXPECT_EQ((bool)resbuf[0], true);
	EXPECT_EQ((bool)resbuf[1], true);

	const char *resbuf1 = v3->getCharConst(0, 2, buf9);
	;
	EXPECT_EQ((char)resbuf1[0], v3->get(0)->getChar());
	EXPECT_EQ((char)resbuf1[1], v3->get(1)->getChar());

	const short *resbuf2 = v3->getShortConst(0, 2, buf10);
	EXPECT_EQ((short)resbuf2[0], v3->get(0)->getShort());
	EXPECT_EQ((short)resbuf2[1], v3->get(1)->getShort());

	const int *resbuf3 = v3->getIntConst(0, 2, buf11);
	EXPECT_EQ((int)resbuf3[0], v3->get(0)->getInt());
	EXPECT_EQ((int)resbuf3[1], v3->get(1)->getInt());

	const long long *resbuf4 = v3->getLongConst(0, 2, buf13);
	EXPECT_EQ((long long)resbuf4[0], v3->get(0)->getLong());
	EXPECT_EQ((long long)resbuf4[1], v3->get(1)->getLong());

	const INDEX *resbuf5 = v3->getIndexConst(0, 2, buf12);
	EXPECT_EQ((INDEX)resbuf5[0], v3->get(0)->getIndex());
	EXPECT_EQ((INDEX)resbuf5[1], v3->get(1)->getIndex());

	const float *resbuf6 = v3->getFloatConst(0, 2, buf14);
	EXPECT_EQ((float)resbuf6[0], v3->get(0)->getFloat());
	EXPECT_EQ((float)resbuf6[1], v3->get(1)->getFloat());

	const double *resbuf7 = v3->getDoubleConst(0, 2, buf15);
	EXPECT_EQ((double)resbuf7[0], v3->get(0)->getDouble());
	EXPECT_EQ((double)resbuf7[1], v3->get(1)->getDouble());

	delete[] buf9;
	delete[] buf10;
	delete[] buf11;
	delete[] buf12;
	delete[] buf13;
	delete[] buf14;
	delete[] buf15;
}

TEST_F(DataformVectorTest, testShortVector)
{
	VectorSP v1 = Util::createVector(DT_SHORT, 2, 2);
	v1->set(0, Util::createShort(1));
	v1->set(1, Util::createShort(0));
	string script = "a=[short(1),short(0)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	EXPECT_EQ(v1->getCategory(), INTEGRAL);

	EXPECT_ANY_THROW(v1->fill(1, 1, Util::createString("1")));
	EXPECT_TRUE(v1->validIndex(1));
	EXPECT_TRUE(v1->validIndex(0, 1, 1));
	EXPECT_FALSE(v1->validIndex(0, 1, (short)0));

	VectorSP subv1 = Util::createVector(DT_SHORT, 0, 2);
	subv1->append(Util::createShort(1));
	subv1->append(Util::createShort(2));
	EXPECT_FALSE(v1->compare(0, Util::createShort(1)));

	VectorSP indexVec = Util::createIndexVector(0, 1);
	ConstantSP index = Util::createInt(0);
	subv1->set(indexVec, Util::createShort(3));
	EXPECT_EQ(subv1->get(0)->getShort(), (short)3);
	subv1->set(index, Util::createShort(5));
	EXPECT_EQ(subv1->get(0)->getShort(), (short)5);
	subv1->set(index, Util::createNullConstant(DT_SHORT));
	EXPECT_TRUE(subv1->get(0)->isNull());

	VectorSP v4 = Util::createVector(DT_SHORT, 2, 10);
	v4->set(0, Util::createShort(1));
	v4->set(1, Util::createShort(0));
	char *buf_1 = new char[2];
	buf_1[0] = 1;
	short *buf_2 = new short[2];
	buf_2[0] = 1;
	int *buf_3 = new int[2];
	buf_3[0] = 1;
	INDEX *buf_4 = new INDEX[2];
	buf_4[0] = 1;
	long long *buf_5 = new long long[2];
	buf_5[0] = 1;
	float *buf_6 = new float[2];
	buf_6[0] = 1;
	double *buf_7 = new double[2];
	buf_7[0] = 1;

	// cout<<v4->getString()<<endl;
	v4->appendBool(buf_1, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1]");
	v4->appendChar(buf_1, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1]");
	v4->appendShort(buf_2, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1]");
	v4->appendInt(buf_3, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1,1]");
	v4->appendLong(buf_5, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1,1,1]");
	v4->appendIndex(buf_4, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1]");
	v4->appendFloat(buf_6, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1,1]");
	v4->appendDouble(buf_7, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1,1,1]");

	VectorSP v5 = Util::createVector(DT_SHORT, 2, 10);
	v5->set(0, Util::createShort(1));
	v5->set(1, Util::createShort(0));
	char *buf9 = new char[2];
	short *buf10 = new short[2];
	int *buf11 = new int[2];
	INDEX *buf12 = new INDEX[2];
	long long *buf13 = new long long[2];
	float *buf14 = new float[2];
	double *buf15 = new double[2];

	v5->getBool(0, 2, buf9);
	EXPECT_EQ((bool)buf9[0], v5->get(0)->getBool());
	EXPECT_EQ((bool)buf9[1], v5->get(1)->getBool());
	v5->getChar(0, 2, buf9);
	EXPECT_EQ((char)buf9[0], v5->get(0)->getChar());
	EXPECT_EQ((char)buf9[1], v5->get(1)->getChar());
	v5->getShort(0, 2, buf10);
	EXPECT_EQ((short)buf10[0], v5->get(0)->getShort());
	EXPECT_EQ((short)buf10[1], v5->get(1)->getShort());
	v5->getInt(0, 2, buf11);
	EXPECT_EQ((int)buf11[0], v5->get(0)->getInt());
	EXPECT_EQ((int)buf11[1], v5->get(1)->getInt());
	v5->getLong(0, 2, buf13);
	EXPECT_EQ((long long)buf13[0], v5->get(0)->getLong());
	EXPECT_EQ((long long)buf13[1], v5->get(1)->getLong());
	v5->getIndex(0, 2, buf12);
	EXPECT_EQ((INDEX)buf12[0], v5->get(0)->getIndex());
	EXPECT_EQ((INDEX)buf12[1], v5->get(1)->getIndex());
	v5->getFloat(0, 2, buf14);
	EXPECT_EQ((float)buf14[0], v5->get(0)->getFloat());
	EXPECT_EQ((float)buf14[1], v5->get(1)->getFloat());
	v5->getDouble(0, 2, buf15);
	EXPECT_EQ((double)buf15[0], v5->get(0)->getDouble());
	EXPECT_EQ((double)buf15[1], v5->get(1)->getDouble());

	memset(buf9, '\0', 2);
	memset(buf10, 0, 2);
	memset(buf11, 0, 2);
	memset(buf12, 0, 2);
	memset(buf13, 0, 2);
	memset(buf14, 0, 2);
	memset(buf15, 0, 2);

	// cout<<v5->getString()<<endl;

	const char *resbuf = v5->getBoolConst(0, 2, buf9);
	EXPECT_EQ((bool)resbuf[0], v5->get(0)->getBool());
	EXPECT_EQ((bool)resbuf[1], v5->get(1)->getBool());

	const char *resbuf1 = v5->getCharConst(0, 2, buf9);
	;
	EXPECT_EQ((char)resbuf1[0], v5->get(0)->getChar());
	EXPECT_EQ((char)resbuf1[1], v5->get(1)->getChar());

	const short *resbuf2 = v5->getShortConst(0, 2, buf10);
	EXPECT_EQ((short)resbuf2[0], v5->get(0)->getShort());
	EXPECT_EQ((short)resbuf2[1], v5->get(1)->getShort());

	const int *resbuf3 = v5->getIntConst(0, 2, buf11);
	EXPECT_EQ((int)resbuf3[0], v5->get(0)->getInt());
	EXPECT_EQ((int)resbuf3[1], v5->get(1)->getInt());

	const long long *resbuf4 = v5->getLongConst(0, 2, buf13);
	EXPECT_EQ((long long)resbuf4[0], v5->get(0)->getLong());
	EXPECT_EQ((long long)resbuf4[1], v5->get(1)->getLong());

	const INDEX *resbuf5 = v5->getIndexConst(0, 2, buf12);
	EXPECT_EQ((INDEX)resbuf5[0], v5->get(0)->getIndex());
	EXPECT_EQ((INDEX)resbuf5[1], v5->get(1)->getIndex());

	const float *resbuf6 = v5->getFloatConst(0, 2, buf14);
	EXPECT_EQ((float)resbuf6[0], v5->get(0)->getFloat());
	EXPECT_EQ((float)resbuf6[1], v5->get(1)->getFloat());

	const double *resbuf7 = v5->getDoubleConst(0, 2, buf15);
	EXPECT_EQ((double)resbuf7[0], v5->get(0)->getDouble());
	EXPECT_EQ((double)resbuf7[1], v5->get(1)->getDouble());

	delete[] buf_1;
	delete[] buf_2;
	delete[] buf_3;
	delete[] buf_4;
	delete[] buf_5;
	delete[] buf_6;
	delete[] buf_7;
	delete[] buf15;
	delete[] buf9;
	delete[] buf10;
	delete[] buf11;
	delete[] buf12;
	delete[] buf13;
	delete[] buf14;
}

TEST_F(DataformVectorTest, testFloatVector)
{
	VectorSP v1 = Util::createVector(DT_FLOAT, 2, 2);
	v1->set(0, Util::createFloat(1));
	v1->set(1, Util::createFloat(2.3131));
	string script = "a=[float(1),float(2.3131)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());

	EXPECT_EQ(v1->getCategory(), FLOATING);
	EXPECT_EQ(v1->getChar(1), (char)2);
	EXPECT_EQ(v1->getShort(1), (short)2);
	EXPECT_EQ(v1->getInt(1), (int)2);
	EXPECT_EQ(v1->getLong(1), (long long)2);

	EXPECT_ANY_THROW(v1->fill(1, 1, Util::createString("1")));
	EXPECT_FALSE(v1->validIndex(0, 1, (float)0));

	VectorSP subv1 = Util::createVector(DT_FLOAT, 0, 2);
	subv1->append(Util::createFloat(1));
	subv1->append(Util::createFloat(2));
	EXPECT_EQ(v1->compare(0, Util::createFloat(1)), 0);
	EXPECT_EQ(v1->compare(0, Util::createFloat(0)), 1);
	EXPECT_EQ(v1->compare(0, Util::createFloat(3)), -1);

	VectorSP indexVec = Util::createIndexVector(0, 1);
	ConstantSP index = Util::createFloat(0);
	subv1->set(indexVec, Util::createFloat(3));
	EXPECT_EQ(subv1->get(0)->getFloat(), (float)3);
	subv1->set(index, Util::createFloat(5));
	EXPECT_EQ(subv1->get(0)->getFloat(), (float)5);
	subv1->set(index, Util::createNullConstant(DT_FLOAT));
	EXPECT_TRUE(subv1->get(0)->isNull());
	subv1->set(1, Util::createNullConstant(DT_FLOAT));
	EXPECT_TRUE(subv1->get(1)->isNull());

	char *buf = new char[2];
	short *buf1 = new short[2];
	int *buf2 = new int[2];
	long long *buf3 = new long long[2];
	v1->getChar(0, 2, buf);
	EXPECT_EQ(buf[0], (char)1);
	EXPECT_EQ(buf[1], (char)2);
	buf[0] = '\0';
	buf[1] = '\0';
	v1->getCharConst(0, 2, buf);
	EXPECT_EQ(buf[0], (char)1);
	EXPECT_EQ(buf[1], (char)2);
	v1->getShort(0, 2, buf1);
	EXPECT_EQ(buf1[0], (short)1);
	EXPECT_EQ(buf1[1], (short)2);
	buf1[0] = '\0';
	buf1[1] = '\0';
	v1->getShortConst(0, 2, buf1);
	EXPECT_EQ(buf1[0], (short)1);
	EXPECT_EQ(buf1[1], (short)2);
	v1->getInt(0, 2, buf2);
	EXPECT_EQ(buf2[0], (int)1);
	EXPECT_EQ(buf2[1], (int)2);
	buf2[0] = '\0';
	buf2[1] = '\0';
	v1->getIntConst(0, 2, buf2);
	EXPECT_EQ(buf2[0], (int)1);
	EXPECT_EQ(buf2[1], (int)2);
	v1->getLong(0, 2, buf3);
	EXPECT_EQ(buf3[0], (long long)1);
	EXPECT_EQ(buf3[1], (long long)2);
	buf3[0] = '\0';
	buf3[1] = '\0';
	v1->getLongConst(0, 2, buf3);
	EXPECT_EQ(buf3[0], (long long)1);
	EXPECT_EQ(buf3[1], (long long)2);

	float *buf4 = new float[1];
	buf4[0] = 3.3114;
	v1->appendFloat(buf4, 1);
	EXPECT_EQ(v1->get(2)->getFloat(), (float)3.3114);

	VectorSP v2 = Util::createVector(DT_FLOAT, 2, 2);
	v2->set(0, Util::createFloat(1));
	v2->set(1, Util::createFloat((float)3));
	v2->append(Util::createNullConstant(DT_FLOAT));

	v2->replace(Util::createNullConstant(DT_FLOAT), Util::createFloat(4));
	v2->replace(Util::createFloat((float)3), Util::createNullConstant(DT_FLOAT));
	v2->replace(Util::createFloat(1), Util::createLong(3));
	EXPECT_EQ(v2->getString(), "[3,,4]");
	v2->reverse();
	EXPECT_EQ(v2->getString(), "[4,,3]");
	v2->reverse();
	v2->set(1, Util::createFloat(3.5));
	EXPECT_ANY_THROW(v2->asof(Util::createVector(DT_FLOAT, 0, 1)));
	EXPECT_EQ(v2->asof(Util::createFloat(2)), -1);
	EXPECT_EQ(v2->asof(Util::createFloat(3.1)), 0);

	VectorSP v4 = Util::createVector(DT_FLOAT, 2, 10);
	v4->set(0, Util::createFloat(1));
	v4->set(1, Util::createFloat(0));
	char *buf_1 = new char[2];
	buf_1[0] = 1;
	short *buf_2 = new short[2];
	buf_2[0] = 1;
	int *buf_3 = new int[2];
	buf_3[0] = 1;
	INDEX *buf_4 = new INDEX[2];
	buf_4[0] = 1;
	long long *buf_5 = new long long[2];
	buf_5[0] = 1;
	float *buf_6 = new float[2];
	buf_6[0] = 1;
	double *buf_7 = new double[2];
	buf_7[0] = 1;

	// cout<<v4->getString()<<endl;
	v4->appendBool(buf_1, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1]");
	v4->appendChar(buf_1, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1]");
	v4->appendShort(buf_2, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1]");
	v4->appendInt(buf_3, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1,1]");
	v4->appendLong(buf_5, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1,1,1]");
	v4->appendIndex(buf_4, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1]");
	v4->appendFloat(buf_6, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1,1]");
	v4->appendDouble(buf_7, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1,1,1]");

	VectorSP v5 = Util::createVector(DT_FLOAT, 2, 10);
	v5->set(0, Util::createFloat(1));
	v5->set(1, Util::createFloat(0));
	char *buf9 = new char[2];
	short *buf10 = new short[2];
	int *buf11 = new int[2];
	INDEX *buf12 = new INDEX[2];
	long long *buf13 = new long long[2];
	float *buf14 = new float[2];
	double *buf15 = new double[2];

	v5->getBool(0, 2, buf9);
	EXPECT_EQ((bool)buf9[0], v5->get(0)->getBool());
	EXPECT_EQ((bool)buf9[1], v5->get(1)->getBool());
	v5->getChar(0, 2, buf9);
	EXPECT_EQ((char)buf9[0], v5->get(0)->getChar());
	EXPECT_EQ((char)buf9[1], v5->get(1)->getChar());
	v5->getShort(0, 2, buf10);
	EXPECT_EQ((short)buf10[0], v5->get(0)->getShort());
	EXPECT_EQ((short)buf10[1], v5->get(1)->getShort());
	v5->getInt(0, 2, buf11);
	EXPECT_EQ((int)buf11[0], v5->get(0)->getInt());
	EXPECT_EQ((int)buf11[1], v5->get(1)->getInt());
	v5->getLong(0, 2, buf13);
	EXPECT_EQ((long long)buf13[0], v5->get(0)->getLong());
	EXPECT_EQ((long long)buf13[1], v5->get(1)->getLong());
	v5->getIndex(0, 2, buf12);
	EXPECT_EQ((INDEX)buf12[0], v5->get(0)->getIndex());
	EXPECT_EQ((INDEX)buf12[1], v5->get(1)->getIndex());
	v5->getFloat(0, 2, buf14);
	EXPECT_EQ((float)buf14[0], v5->get(0)->getFloat());
	EXPECT_EQ((float)buf14[1], v5->get(1)->getFloat());
	v5->getDouble(0, 2, buf15);
	EXPECT_EQ((double)buf15[0], v5->get(0)->getDouble());
	EXPECT_EQ((double)buf15[1], v5->get(1)->getDouble());

	memset(buf9, '\0', 2);
	memset(buf10, 0, 2);
	memset(buf11, 0, 2);
	memset(buf12, 0, 2);
	memset(buf13, 0, 2);
	memset(buf14, 0, 2);
	memset(buf15, 0, 2);

	// cout<<v5->getString()<<endl;

	const char *resbuf = v5->getBoolConst(0, 2, buf9);
	EXPECT_EQ((bool)resbuf[0], v5->get(0)->getBool());
	EXPECT_EQ((bool)resbuf[1], v5->get(1)->getBool());

	const char *resbuf1 = v5->getCharConst(0, 2, buf9);
	;
	EXPECT_EQ((char)resbuf1[0], v5->get(0)->getChar());
	EXPECT_EQ((char)resbuf1[1], v5->get(1)->getChar());

	const short *resbuf2 = v5->getShortConst(0, 2, buf10);
	EXPECT_EQ((short)resbuf2[0], v5->get(0)->getShort());
	EXPECT_EQ((short)resbuf2[1], v5->get(1)->getShort());

	const int *resbuf3 = v5->getIntConst(0, 2, buf11);
	EXPECT_EQ((int)resbuf3[0], v5->get(0)->getInt());
	EXPECT_EQ((int)resbuf3[1], v5->get(1)->getInt());

	const long long *resbuf4 = v5->getLongConst(0, 2, buf13);
	EXPECT_EQ((long long)resbuf4[0], v5->get(0)->getLong());
	EXPECT_EQ((long long)resbuf4[1], v5->get(1)->getLong());

	const INDEX *resbuf5 = v5->getIndexConst(0, 2, buf12);
	EXPECT_EQ((INDEX)resbuf5[0], v5->get(0)->getIndex());
	EXPECT_EQ((INDEX)resbuf5[1], v5->get(1)->getIndex());

	const float *resbuf6 = v5->getFloatConst(0, 2, buf14);
	EXPECT_EQ((float)resbuf6[0], v5->get(0)->getFloat());
	EXPECT_EQ((float)resbuf6[1], v5->get(1)->getFloat());

	const double *resbuf7 = v5->getDoubleConst(0, 2, buf15);
	EXPECT_EQ((double)resbuf7[0], v5->get(0)->getDouble());
	EXPECT_EQ((double)resbuf7[1], v5->get(1)->getDouble());

	delete[] buf_1;
	delete[] buf_2;
	delete[] buf_3;
	delete[] buf_4;
	delete[] buf_5;
	delete[] buf_6;
	delete[] buf_7;
	delete[] buf15;
	delete[] buf9;
	delete[] buf10;
	delete[] buf11;
	delete[] buf12;
	delete[] buf13;
	delete[] buf14;
}

TEST_F(DataformVectorTest, testFloatNullVector)
{
	VectorSP v1 = Util::createVector(DT_FLOAT, 2, 2);
	v1->setNull(0);
	v1->setNull(1);
	string script = "a=[float(NULL),float(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());

	VectorSP v2 = Util::createVector(DT_FLOAT, 2, 2, true, 0, (void *)0, true);
	v2->setNull(0);
	v2->setNull(1);
	char *buf = new char[2];
	short *buf1 = new short[2];
	int *buf2 = new int[2];
	long long *buf3 = new long long[2];
	v2->getChar(0, 2, buf);
	EXPECT_EQ(buf[0], CHAR_MIN);
	EXPECT_EQ(buf[1], CHAR_MIN);
	buf[0] = '\0';
	buf[1] = '\0';
	v2->getCharConst(0, 2, buf);
	EXPECT_EQ(buf[0], CHAR_MIN);
	EXPECT_EQ(buf[1], CHAR_MIN);
	v2->getShort(0, 2, buf1);
	EXPECT_EQ(buf1[0], SHRT_MIN);
	EXPECT_EQ(buf1[1], SHRT_MIN);
	buf1[0] = '\0';
	buf1[1] = '\0';
	v2->getShortConst(0, 2, buf1);
	EXPECT_EQ(buf1[0], SHRT_MIN);
	EXPECT_EQ(buf1[1], SHRT_MIN);
	v2->getInt(0, 2, buf2);
	EXPECT_EQ(buf2[0], INT_MIN);
	EXPECT_EQ(buf2[1], INT_MIN);
	buf2[0] = '\0';
	buf2[1] = '\0';
	v2->getIntConst(0, 2, buf2);
	EXPECT_EQ(buf2[0], INT_MIN);
	EXPECT_EQ(buf2[1], INT_MIN);
	v2->getLong(0, 2, buf3);
	EXPECT_EQ(buf3[0], LLONG_MIN);
	EXPECT_EQ(buf3[1], LLONG_MIN);
	buf3[0] = '\0';
	buf3[1] = '\0';
	v2->getLongConst(0, 2, buf3);
	EXPECT_EQ(buf3[0], LLONG_MIN);
	EXPECT_EQ(buf3[1], LLONG_MIN);

	VectorSP v3 = Util::createVector(DT_FLOAT, 2, 10, true, 0, (void *)0, true);
	v3->setNull(0);
	v3->setNull(1);
	char *buf9 = new char[2];
	short *buf10 = new short[2];
	int *buf11 = new int[2];
	INDEX *buf12 = new INDEX[2];
	long long *buf13 = new long long[2];
	float *buf14 = new float[2];
	double *buf15 = new double[2];

	v3->getBool(0, 2, buf9);
	EXPECT_EQ((bool)buf9[0], true);
	EXPECT_EQ((bool)buf9[1], true);
	v3->getChar(0, 2, buf9);
	EXPECT_EQ((char)buf9[0], v3->get(0)->getChar());
	EXPECT_EQ((char)buf9[1], v3->get(1)->getChar());
	v3->getShort(0, 2, buf10);
	EXPECT_EQ((short)buf10[0], v3->get(0)->getShort());
	EXPECT_EQ((short)buf10[1], v3->get(1)->getShort());
	v3->getInt(0, 2, buf11);
	EXPECT_EQ((int)buf11[0], v3->get(0)->getInt());
	EXPECT_EQ((int)buf11[1], v3->get(1)->getInt());
	v3->getLong(0, 2, buf13);
	EXPECT_EQ((long long)buf13[0], v3->get(0)->getLong());
	EXPECT_EQ((long long)buf13[1], v3->get(1)->getLong());
	v3->getIndex(0, 2, buf12);
	EXPECT_EQ((INDEX)buf12[0], v3->get(0)->getIndex());
	EXPECT_EQ((INDEX)buf12[1], v3->get(1)->getIndex());
	v3->getFloat(0, 2, buf14);
	EXPECT_EQ((float)buf14[0], v3->get(0)->getFloat());
	EXPECT_EQ((float)buf14[1], v3->get(1)->getFloat());
	v3->getDouble(0, 2, buf15);
	EXPECT_EQ((double)buf15[0], v3->get(0)->getDouble());
	EXPECT_EQ((double)buf15[1], v3->get(1)->getDouble());

	memset(buf9, '\0', 2);
	memset(buf10, 0, 2);
	memset(buf11, 0, 2);
	memset(buf12, 0, 2);
	memset(buf13, 0, 2);
	memset(buf14, 0, 2);
	memset(buf15, 0, 2);

	// cout<<v3->getString()<<endl;

	const char *resbuf = v3->getBoolConst(0, 2, buf9);
	EXPECT_EQ((bool)resbuf[0], true);
	EXPECT_EQ((bool)resbuf[1], true);

	const char *resbuf1 = v3->getCharConst(0, 2, buf9);
	;
	EXPECT_EQ((char)resbuf1[0], v3->get(0)->getChar());
	EXPECT_EQ((char)resbuf1[1], v3->get(1)->getChar());

	const short *resbuf2 = v3->getShortConst(0, 2, buf10);
	EXPECT_EQ((short)resbuf2[0], v3->get(0)->getShort());
	EXPECT_EQ((short)resbuf2[1], v3->get(1)->getShort());

	const int *resbuf3 = v3->getIntConst(0, 2, buf11);
	EXPECT_EQ((int)resbuf3[0], v3->get(0)->getInt());
	EXPECT_EQ((int)resbuf3[1], v3->get(1)->getInt());

	const long long *resbuf4 = v3->getLongConst(0, 2, buf13);
	EXPECT_EQ((long long)resbuf4[0], v3->get(0)->getLong());
	EXPECT_EQ((long long)resbuf4[1], v3->get(1)->getLong());

	const INDEX *resbuf5 = v3->getIndexConst(0, 2, buf12);
	EXPECT_EQ((INDEX)resbuf5[0], v3->get(0)->getIndex());
	EXPECT_EQ((INDEX)resbuf5[1], v3->get(1)->getIndex());

	const float *resbuf6 = v3->getFloatConst(0, 2, buf14);
	EXPECT_EQ((float)resbuf6[0], v3->get(0)->getFloat());
	EXPECT_EQ((float)resbuf6[1], v3->get(1)->getFloat());

	const double *resbuf7 = v3->getDoubleConst(0, 2, buf15);
	EXPECT_EQ((double)resbuf7[0], v3->get(0)->getDouble());
	EXPECT_EQ((double)resbuf7[1], v3->get(1)->getDouble());

	delete[] buf9;
	delete[] buf10;
	delete[] buf11;
	delete[] buf12;
	delete[] buf13;
	delete[] buf14;
	delete[] buf15;
}

TEST_F(DataformVectorTest, testDoubleVector)
{
	VectorSP v1 = Util::createVector(DT_DOUBLE, 2, 2);
	v1->set(0, Util::createDouble(1));
	v1->set(1, Util::createDouble(2.3131));
	string script = "a=[double(1),double(2.3131)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());

	EXPECT_EQ(v1->getCategory(), FLOATING);
	EXPECT_EQ(v1->getChar(1), (char)2);
	EXPECT_EQ(v1->getShort(1), (short)2);
	EXPECT_EQ(v1->getInt(1), (int)2);
	EXPECT_EQ(v1->getLong(1), (long long)2);

	EXPECT_ANY_THROW(v1->fill(1, 1, Util::createString("1")));
	EXPECT_FALSE(v1->validIndex(0, 1, (float)0));

	VectorSP subv1 = Util::createVector(DT_DOUBLE, 0, 2);
	subv1->append(Util::createDouble(1));
	subv1->append(Util::createDouble(2));
	EXPECT_EQ(v1->compare(0, Util::createDouble(1)), 0);
	EXPECT_EQ(v1->compare(0, Util::createDouble(0)), 1);
	EXPECT_EQ(v1->compare(0, Util::createDouble(3)), -1);

	VectorSP indexVec = Util::createIndexVector(0, 1);
	ConstantSP index = Util::createDouble(0);
	subv1->set(indexVec, Util::createDouble(3));
	EXPECT_EQ(subv1->get(0)->getDouble(), (float)3);
	subv1->set(index, Util::createDouble(5));
	EXPECT_EQ(subv1->get(0)->getDouble(), (float)5);
	subv1->set(index, Util::createNullConstant(DT_DOUBLE));
	EXPECT_TRUE(subv1->get(0)->isNull());
	subv1->set(1, Util::createNullConstant(DT_DOUBLE));
	EXPECT_TRUE(subv1->get(1)->isNull());

	char *buf = new char[2];
	short *buf1 = new short[2];
	int *buf2 = new int[2];
	long long *buf3 = new long long[2];
	v1->getChar(0, 2, buf);
	EXPECT_EQ(buf[0], (char)1);
	EXPECT_EQ(buf[1], (char)2);
	buf[0] = '\0';
	buf[1] = '\0';
	v1->getCharConst(0, 2, buf);
	EXPECT_EQ(buf[0], (char)1);
	EXPECT_EQ(buf[1], (char)2);
	v1->getShort(0, 2, buf1);
	EXPECT_EQ(buf1[0], (short)1);
	EXPECT_EQ(buf1[1], (short)2);
	buf1[0] = '\0';
	buf1[1] = '\0';
	v1->getShortConst(0, 2, buf1);
	EXPECT_EQ(buf1[0], (short)1);
	EXPECT_EQ(buf1[1], (short)2);
	v1->getInt(0, 2, buf2);
	EXPECT_EQ(buf2[0], (int)1);
	EXPECT_EQ(buf2[1], (int)2);
	buf2[0] = '\0';
	buf2[1] = '\0';
	v1->getIntConst(0, 2, buf2);
	EXPECT_EQ(buf2[0], (int)1);
	EXPECT_EQ(buf2[1], (int)2);
	v1->getLong(0, 2, buf3);
	EXPECT_EQ(buf3[0], (long long)1);
	EXPECT_EQ(buf3[1], (long long)2);
	buf3[0] = '\0';
	buf3[1] = '\0';
	v1->getLongConst(0, 2, buf3);
	EXPECT_EQ(buf3[0], (long long)1);
	EXPECT_EQ(buf3[1], (long long)2);

	double *buf4 = new double[1];
	buf4[0] = 3.3114;
	v1->appendDouble(buf4, 1);
	EXPECT_EQ(v1->get(2)->getDouble(), (double)3.3114);
	v1->add(0, 2, (double)1);
	EXPECT_EQ(v1->getString(), "[2,3.3131,3.3114]");
	v1->append(Util::createNullConstant(DT_DOUBLE));
	v1->add(0, 3, (double)1);
	EXPECT_EQ(v1->getString(), "[3,4.3131,4.3114,]");

	VectorSP v4 = Util::createVector(DT_DOUBLE, 2, 10);
	v4->set(0, Util::createDouble(1));
	v4->set(1, Util::createDouble(0));
	char *buf_1 = new char[2];
	buf_1[0] = 1;
	short *buf_2 = new short[2];
	buf_2[0] = 1;
	int *buf_3 = new int[2];
	buf_3[0] = 1;
	INDEX *buf_4 = new INDEX[2];
	buf_4[0] = 1;
	long long *buf_5 = new long long[2];
	buf_5[0] = 1;
	float *buf_6 = new float[2];
	buf_6[0] = 1;
	double *buf_7 = new double[2];
	buf_7[0] = 1;

	// cout<<v4->getString()<<endl;
	v4->appendBool(buf_1, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1]");
	v4->appendChar(buf_1, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1]");
	v4->appendShort(buf_2, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1]");
	v4->appendInt(buf_3, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1,1]");
	v4->appendLong(buf_5, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1,1,1]");
	v4->appendIndex(buf_4, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1]");
	v4->appendFloat(buf_6, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1,1]");
	v4->appendDouble(buf_7, 1);
	EXPECT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1,1,1]");

	VectorSP v5 = Util::createVector(DT_DOUBLE, 2, 10);
	v5->set(0, Util::createDouble(1));
	v5->set(1, Util::createDouble(0));
	char *buf9 = new char[2];
	short *buf10 = new short[2];
	int *buf11 = new int[2];
	INDEX *buf12 = new INDEX[2];
	long long *buf13 = new long long[2];
	float *buf14 = new float[2];
	double *buf15 = new double[2];

	v5->getBool(0, 2, buf9);
	EXPECT_EQ((bool)buf9[0], v5->get(0)->getBool());
	EXPECT_EQ((bool)buf9[1], v5->get(1)->getBool());
	v5->getChar(0, 2, buf9);
	EXPECT_EQ((char)buf9[0], v5->get(0)->getChar());
	EXPECT_EQ((char)buf9[1], v5->get(1)->getChar());
	v5->getShort(0, 2, buf10);
	EXPECT_EQ((short)buf10[0], v5->get(0)->getShort());
	EXPECT_EQ((short)buf10[1], v5->get(1)->getShort());
	v5->getInt(0, 2, buf11);
	EXPECT_EQ((int)buf11[0], v5->get(0)->getInt());
	EXPECT_EQ((int)buf11[1], v5->get(1)->getInt());
	v5->getLong(0, 2, buf13);
	EXPECT_EQ((long long)buf13[0], v5->get(0)->getLong());
	EXPECT_EQ((long long)buf13[1], v5->get(1)->getLong());
	v5->getIndex(0, 2, buf12);
	EXPECT_EQ((INDEX)buf12[0], v5->get(0)->getIndex());
	EXPECT_EQ((INDEX)buf12[1], v5->get(1)->getIndex());
	v5->getFloat(0, 2, buf14);
	EXPECT_EQ((float)buf14[0], v5->get(0)->getFloat());
	EXPECT_EQ((float)buf14[1], v5->get(1)->getFloat());
	v5->getDouble(0, 2, buf15);
	EXPECT_EQ((double)buf15[0], v5->get(0)->getDouble());
	EXPECT_EQ((double)buf15[1], v5->get(1)->getDouble());

	memset(buf9, '\0', 2);
	memset(buf10, 0, 2);
	memset(buf11, 0, 2);
	memset(buf12, 0, 2);
	memset(buf13, 0, 2);
	memset(buf14, 0, 2);
	memset(buf15, 0, 2);

	// cout<<v5->getString()<<endl;
	const char *resbuf = v5->getBoolConst(0, 2, buf9);
	EXPECT_EQ((bool)resbuf[0], v5->get(0)->getBool());
	EXPECT_EQ((bool)resbuf[1], v5->get(1)->getBool());

	const char *resbuf1 = v5->getCharConst(0, 2, buf9);
	;
	EXPECT_EQ((char)resbuf1[0], v5->get(0)->getChar());
	EXPECT_EQ((char)resbuf1[1], v5->get(1)->getChar());

	const short *resbuf2 = v5->getShortConst(0, 2, buf10);
	EXPECT_EQ((short)resbuf2[0], v5->get(0)->getShort());
	EXPECT_EQ((short)resbuf2[1], v5->get(1)->getShort());

	const int *resbuf3 = v5->getIntConst(0, 2, buf11);
	EXPECT_EQ((int)resbuf3[0], v5->get(0)->getInt());
	EXPECT_EQ((int)resbuf3[1], v5->get(1)->getInt());

	const long long *resbuf4 = v5->getLongConst(0, 2, buf13);
	EXPECT_EQ((long long)resbuf4[0], v5->get(0)->getLong());
	EXPECT_EQ((long long)resbuf4[1], v5->get(1)->getLong());

	const INDEX *resbuf5 = v5->getIndexConst(0, 2, buf12);
	EXPECT_EQ((INDEX)resbuf5[0], v5->get(0)->getIndex());
	EXPECT_EQ((INDEX)resbuf5[1], v5->get(1)->getIndex());

	const float *resbuf6 = v5->getFloatConst(0, 2, buf14);
	EXPECT_EQ((float)resbuf6[0], v5->get(0)->getFloat());
	EXPECT_EQ((float)resbuf6[1], v5->get(1)->getFloat());

	const double *resbuf7 = v5->getDoubleConst(0, 2, buf15);
	EXPECT_EQ((double)resbuf7[0], v5->get(0)->getDouble());
	EXPECT_EQ((double)resbuf7[1], v5->get(1)->getDouble());

	delete[] buf_1;
	delete[] buf_2;
	delete[] buf_3;
	delete[] buf_4;
	delete[] buf_5;
	delete[] buf_6;
	delete[] buf_7;
	delete[] buf15;
	delete[] buf9;
	delete[] buf10;
	delete[] buf11;
	delete[] buf12;
	delete[] buf13;
	delete[] buf14;
}

TEST_F(DataformVectorTest, testDoubleNullVector)
{
	VectorSP v1 = Util::createVector(DT_DOUBLE, 2, 2);
	v1->setNull(0);
	v1->setNull(1);
	string script = "a=[double(NULL),double(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());

	VectorSP v2 = Util::createVector(DT_DOUBLE, 2, 2, true, 0, (void *)0, true);
	v2->setNull(0);
	v2->setNull(1);
	char *buf = new char[2];
	short *buf1 = new short[2];
	int *buf2 = new int[2];
	long long *buf3 = new long long[2];
	v2->getChar(0, 2, buf);
	EXPECT_EQ(buf[0], CHAR_MIN);
	EXPECT_EQ(buf[1], CHAR_MIN);
	buf[0] = '\0';
	buf[1] = '\0';
	v2->getCharConst(0, 2, buf);
	EXPECT_EQ(buf[0], CHAR_MIN);
	EXPECT_EQ(buf[1], CHAR_MIN);
	v2->getShort(0, 2, buf1);
	EXPECT_EQ(buf1[0], SHRT_MIN);
	EXPECT_EQ(buf1[1], SHRT_MIN);
	buf1[0] = '\0';
	buf1[1] = '\0';
	v2->getShortConst(0, 2, buf1);
	EXPECT_EQ(buf1[0], SHRT_MIN);
	EXPECT_EQ(buf1[1], SHRT_MIN);
	v2->getInt(0, 2, buf2);
	EXPECT_EQ(buf2[0], INT_MIN);
	EXPECT_EQ(buf2[1], INT_MIN);
	buf2[0] = '\0';
	buf2[1] = '\0';
	v2->getIntConst(0, 2, buf2);
	EXPECT_EQ(buf2[0], INT_MIN);
	EXPECT_EQ(buf2[1], INT_MIN);
	v2->getLong(0, 2, buf3);
	EXPECT_EQ(buf3[0], LLONG_MIN);
	EXPECT_EQ(buf3[1], LLONG_MIN);
	buf3[0] = '\0';
	buf3[1] = '\0';
	v2->getLongConst(0, 2, buf3);
	EXPECT_EQ(buf3[0], LLONG_MIN);
	EXPECT_EQ(buf3[1], LLONG_MIN);

	VectorSP v3 = Util::createVector(DT_DOUBLE, 2, 10, true, 0, (void *)0, true);
	v3->setNull(0);
	v3->setNull(1);
	char *buf9 = new char[2];
	short *buf10 = new short[2];
	int *buf11 = new int[2];
	INDEX *buf12 = new INDEX[2];
	long long *buf13 = new long long[2];
	float *buf14 = new float[2];
	double *buf15 = new double[2];

	v3->getBool(0, 2, buf9);
	EXPECT_EQ((bool)buf9[0], true);
	EXPECT_EQ((bool)buf9[1], true);
	v3->getChar(0, 2, buf9);
	EXPECT_EQ((char)buf9[0], v3->get(0)->getChar());
	EXPECT_EQ((char)buf9[1], v3->get(1)->getChar());
	v3->getShort(0, 2, buf10);
	EXPECT_EQ((short)buf10[0], v3->get(0)->getShort());
	EXPECT_EQ((short)buf10[1], v3->get(1)->getShort());
	v3->getInt(0, 2, buf11);
	EXPECT_EQ((int)buf11[0], v3->get(0)->getInt());
	EXPECT_EQ((int)buf11[1], v3->get(1)->getInt());
	v3->getLong(0, 2, buf13);
	EXPECT_EQ((long long)buf13[0], v3->get(0)->getLong());
	EXPECT_EQ((long long)buf13[1], v3->get(1)->getLong());
	v3->getIndex(0, 2, buf12);
	EXPECT_EQ((INDEX)buf12[0], v3->get(0)->getIndex());
	EXPECT_EQ((INDEX)buf12[1], v3->get(1)->getIndex());
	v3->getFloat(0, 2, buf14);
	EXPECT_EQ((float)buf14[0], v3->get(0)->getFloat());
	EXPECT_EQ((float)buf14[1], v3->get(1)->getFloat());
	v3->getDouble(0, 2, buf15);
	EXPECT_EQ((double)buf15[0], v3->get(0)->getDouble());
	EXPECT_EQ((double)buf15[1], v3->get(1)->getDouble());

	memset(buf9, '\0', 2);
	memset(buf10, 0, 2);
	memset(buf11, 0, 2);
	memset(buf12, 0, 2);
	memset(buf13, 0, 2);
	memset(buf14, 0, 2);
	memset(buf15, 0, 2);

	// cout<<v3->getString()<<endl;

	const char *resbuf = v3->getBoolConst(0, 2, buf9);
	EXPECT_EQ((bool)resbuf[0], true);
	EXPECT_EQ((bool)resbuf[1], true);

	const char *resbuf1 = v3->getCharConst(0, 2, buf9);
	;
	EXPECT_EQ((char)resbuf1[0], v3->get(0)->getChar());
	EXPECT_EQ((char)resbuf1[1], v3->get(1)->getChar());

	const short *resbuf2 = v3->getShortConst(0, 2, buf10);
	EXPECT_EQ((short)resbuf2[0], v3->get(0)->getShort());
	EXPECT_EQ((short)resbuf2[1], v3->get(1)->getShort());

	const int *resbuf3 = v3->getIntConst(0, 2, buf11);
	EXPECT_EQ((int)resbuf3[0], v3->get(0)->getInt());
	EXPECT_EQ((int)resbuf3[1], v3->get(1)->getInt());

	const long long *resbuf4 = v3->getLongConst(0, 2, buf13);
	EXPECT_EQ((long long)resbuf4[0], v3->get(0)->getLong());
	EXPECT_EQ((long long)resbuf4[1], v3->get(1)->getLong());

	const INDEX *resbuf5 = v3->getIndexConst(0, 2, buf12);
	EXPECT_EQ((INDEX)resbuf5[0], v3->get(0)->getIndex());
	EXPECT_EQ((INDEX)resbuf5[1], v3->get(1)->getIndex());

	const float *resbuf6 = v3->getFloatConst(0, 2, buf14);
	EXPECT_EQ((float)resbuf6[0], v3->get(0)->getFloat());
	EXPECT_EQ((float)resbuf6[1], v3->get(1)->getFloat());

	const double *resbuf7 = v3->getDoubleConst(0, 2, buf15);
	EXPECT_EQ((double)resbuf7[0], v3->get(0)->getDouble());
	EXPECT_EQ((double)resbuf7[1], v3->get(1)->getDouble());

	delete[] buf9;
	delete[] buf10;
	delete[] buf11;
	delete[] buf12;
	delete[] buf13;
	delete[] buf14;
	delete[] buf15;
}

TEST_F(DataformVectorTest, testDatehourVector)
{
	VectorSP v1 = Util::createVector(DT_DATEHOUR, 2, 2);
	v1->set(0, Util::createDateHour(1));
	v1->set(1, Util::createDateHour(1000));
	string script = "a=[datehour(1),datehour(1000)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
}

TEST_F(DataformVectorTest, testDatehourNullVector)
{
	VectorSP v1 = Util::createVector(DT_DATEHOUR, 2, 2);
	v1->setNull(0);
	v1->setNull(1);
	string script = "a=[datehour(NULL),datehour(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0; i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testDateVector)
{
	VectorSP v1 = Util::createVector(DT_DATE, 2, 2);
	v1->set(0, Util::createDate(1));
	v1->set(1, Util::createDate(1000));
	string script = "a=[date(1),date(1000)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
}

TEST_F(DataformVectorTest, testDatenullVector)
{
	VectorSP v1 = Util::createVector(DT_DATE, 2, 2);
	v1->setNull(0);
	v1->setNull(1);
	string script = "a=[date(NULL),date(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0; i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testMinuteVector)
{
	VectorSP v1 = Util::createVector(DT_MINUTE, 2, 2);
	v1->set(0, Util::createMinute(1));
	v1->set(1, Util::createMinute(1000));
	string script = "a=[minute(1),minute(1000)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());

	v1->append(Util::createMinute(2000));
	v1->validate();
	EXPECT_EQ(v1->get(v1->size() - 1)->getInt(), INT_MIN);
	EXPECT_TRUE(v1->hasNull());
}

TEST_F(DataformVectorTest, testMinutenullVector)
{
	VectorSP v1 = Util::createVector(DT_MINUTE, 2, 2);
	v1->setNull(0);
	v1->setNull(1);
	string script = "a=[minute(NULL),minute(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0; i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testDatetimeVector)
{
	VectorSP v1 = Util::createVector(DT_DATETIME, 2, 2);
	v1->set(0, Util::createDateTime(1));
	v1->set(1, Util::createDateTime(1000));
	string script = "a=[datetime(1),datetime(1000)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
}

TEST_F(DataformVectorTest, testDatetimenullVector)
{
	VectorSP v1 = Util::createVector(DT_DATETIME, 2, 2);
	v1->setNull(0);
	v1->setNull(1);
	string script = "a=[datetime(NULL),datetime(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0; i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testTimestampVector)
{
	VectorSP v1 = Util::createVector(DT_TIMESTAMP, 2, 2);
	v1->set(0, Util::createTimestamp(1));
	v1->set(1, Util::createTimestamp(1000000));
	string script = "a=[timestamp(1),timestamp(1000000)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
}

TEST_F(DataformVectorTest, testTimestampnullVector)
{
	VectorSP v1 = Util::createVector(DT_TIMESTAMP, 2, 2);
	v1->setNull(0);
	v1->setNull(1);
	string script = "a=[timestamp(NULL),timestamp(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0; i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testNanotimeVector)
{
	VectorSP v1 = Util::createVector(DT_NANOTIME, 2, 2);
	v1->set(0, Util::createNanoTime(1));
	v1->set(1, Util::createNanoTime(1000000));
	string script = "a=[nanotime(1),nanotime(1000000)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());

	v1->append(Util::createNanoTime(86400000000000ll));
	v1->validate();
	EXPECT_EQ(v1->get(v1->size() - 1)->getLong(), LLONG_MIN);
	EXPECT_TRUE(v1->hasNull());
}

TEST_F(DataformVectorTest, testNanotimenullVector)
{
	VectorSP v1 = Util::createVector(DT_NANOTIME, 2, 2);
	v1->setNull(0);
	v1->setNull(1);
	string script = "a=[nanotime(NULL),nanotime(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0; i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testNanotimestampVector)
{
	VectorSP v1 = Util::createVector(DT_NANOTIMESTAMP, 2, 2);
	v1->set(0, Util::createNanoTimestamp(1));
	v1->set(1, Util::createNanoTimestamp(100000000));
	string script = "a=[nanotimestamp(1),nanotimestamp(100000000)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
}

TEST_F(DataformVectorTest, testNanotimestampnullVector)
{
	VectorSP v1 = Util::createVector(DT_NANOTIMESTAMP, 2, 2);
	v1->setNull(0);
	v1->setNull(1);
	string script = "a=[nanotimestamp(NULL),nanotimestamp(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0; i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testMonthVector)
{
	VectorSP v1 = Util::createVector(DT_MONTH, 2, 2);
	v1->set(0, Util::createMonth(1));
	v1->set(1, Util::createMonth(1000));
	string script = "a=[month(1),month(1000)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());

	EXPECT_FALSE(v1->isIndexArray());
	EXPECT_TRUE(v1->getIndexArray() == NULL);
	EXPECT_EQ(v1->getValue(2)->getString(), v1->castTemporal(DT_MONTH)->getString());
}

TEST_F(DataformVectorTest, testMonthnullVector)
{
	VectorSP v1 = Util::createVector(DT_MONTH, 2, 2);
	v1->setNull(0);
	v1->setNull(1);
	string script = "a=[month(NULL),month(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0; i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testTimeVector)
{
	VectorSP v1 = Util::createVector(DT_TIME, 2, 2);
	v1->set(0, Util::createTime(1));
	v1->set(1, Util::createTime(1000));
	string script = "a=[time(1),time(1000)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());

	v1->append(Util::createTime(86401));
	v1->validate();
	EXPECT_EQ(v1->get(v1->size() - 1)->getInt(), INT_MIN);
	EXPECT_TRUE(v1->hasNull());
}

TEST_F(DataformVectorTest, testTimenullVector)
{
	VectorSP v1 = Util::createVector(DT_TIME, 2, 2);
	v1->setNull(0);
	v1->setNull(1);
	string script = "a=[time(NULL),time(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0; i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testSecondVector)
{
	VectorSP v1 = Util::createVector(DT_SECOND, 2, 2);
	v1->set(0, Util::createSecond(1));
	v1->set(1, Util::createSecond(1000));
	string script = "a=[second(1),second(1000)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());

	v1->append(Util::createSecond(86400));
	v1->validate();
	EXPECT_EQ(v1->get(v1->size() - 1)->getInt(), INT_MIN);
	EXPECT_TRUE(v1->hasNull());
}

TEST_F(DataformVectorTest, testSecondnullVector)
{
	VectorSP v1 = Util::createVector(DT_SECOND, 2, 2);
	v1->setNull(0);
	v1->setNull(1);
	string script = "a=[second(NULL),second(NULL)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0; i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testInt128Vector)
{

	VectorSP v1 = Util::createVector(DT_INT128, 2, 2);
	v1->set(0, Util::parseConstant(DT_INT128, "e1671797c52e15f763380b45e841ec32"));
	v1->set(1, Util::parseConstant(DT_INT128, "e1671797c52e15f763380b45e841ec33"));
	string script = "a=[int128(`e1671797c52e15f763380b45e841ec32),int128(`e1671797c52e15f763380b45e841ec33)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());

	EXPECT_ANY_THROW(v1->setString(1, "123"));
	EXPECT_TRUE(v1->isFastMode());
	EXPECT_TRUE(v1->sizeable());
	v1->setNull();
	EXPECT_FALSE(v1->hasNull(0, 1));
	v1->reverse();
	EXPECT_EQ(v1->get(0)->getString(), "e1671797c52e15f763380b45e841ec33");
	EXPECT_EQ(v1->get(1)->getString(), "e1671797c52e15f763380b45e841ec32");

	EXPECT_ANY_THROW(v1->getBool());
	EXPECT_ANY_THROW(v1->getChar());
	EXPECT_ANY_THROW(v1->getShort());
	EXPECT_ANY_THROW(v1->getLong());
	EXPECT_ANY_THROW(v1->getFloat());
	EXPECT_ANY_THROW(v1->getDouble());
	EXPECT_ANY_THROW(v1->getIndex());
	EXPECT_ANY_THROW(v1->neg());
	EXPECT_ANY_THROW(v1->getBinary());

	// cout<<v1->get(Util::createInt(1))<<endl;
	EXPECT_EQ(v1->compare(0, Util::parseConstant(DT_INT128, "e1671797c52e15f763380b45e841ec33")), 0);
	v1->replace(Util::parseConstant(DT_INT128, "e1671797c52e15f763380b45e841ec33"), Util::parseConstant(DT_INT128, "e1671797c52e15f763380b45e841ec55"));
	EXPECT_EQ(v1->get(0)->getString(), "e1671797c52e15f763380b45e841ec55");

	v1->append(Util::createNullConstant(DT_INT128));
	EXPECT_ANY_THROW(v1->nullFill(Util::createInt(1)));
	v1->nullFill(Util::parseConstant(DT_INT128, "e1671797c52e15f763380b45e841ec99"));
	EXPECT_EQ(v1->get(2)->getString(), "e1671797c52e15f763380b45e841ec99");

	char *buf1 = new char[3];
	char *buf2 = new char[3];
	v1->isNull(0, 3, buf1);
	EXPECT_FALSE((int)buf1[0]);
	EXPECT_FALSE((int)buf1[1]);
	EXPECT_FALSE((int)buf1[2]);
	v1->isValid(0, 3, buf2);
	EXPECT_TRUE((int)buf2[0]);
	EXPECT_TRUE((int)buf2[1]);
	EXPECT_TRUE((int)buf2[2]);

	string strEmpty = "";
	string strlt32 = "192.168.1.1";
	string int128str = "e1671797c52e15f763380b45e841ec00";
	string *pstrEmpty = &strEmpty;
	string *pstrlt32 = &strlt32;
	string *pint128str = &int128str;
	// EXPECT_FALSE(v1->appendString(pstrEmpty,10));
	v1->appendString(pstrEmpty, 1);
	EXPECT_EQ(v1->get(3)->getString(), Util::createNullConstant(DT_INT128)->getString());

	EXPECT_FALSE(v1->appendString(pstrlt32, 1));
	v1->appendString(pint128str, 1);
	EXPECT_EQ(v1->get(4)->getString(), int128str);
	EXPECT_TRUE(v1->hasNull());

	char *c1 = (char *)strEmpty.data();
	char *c2 = (char *)strlt32.data();
	char *c3 = (char *)int128str.data();
	char **pc1 = &c1;
	char **pc2 = &c2;
	char **pc3 = &c3;
	// EXPECT_FALSE(v1->appendString(pc1,10));
	v1->appendString(pc1, 1);

	EXPECT_EQ(v1->get(5)->getString(), Util::createNullConstant(DT_INT128)->getString());
	EXPECT_FALSE(v1->appendString(pc2, 1));
	v1->appendString(pc3, 1);

	EXPECT_EQ(v1->get(6)->getString(), c3);
	EXPECT_TRUE(v1->hasNull());
	cout << v1->getString() << endl;

	v1->getDataArray();
	VectorSP instanceVec = v1->getInstance(1);
	instanceVec->set(0, Util::parseConstant(DT_INT128, "e1671797c52e15f763380b45e841ec00"));
	EXPECT_EQ(instanceVec->size(), 1);
	EXPECT_EQ(instanceVec->getString(), "[e1671797c52e15f763380b45e841ec00]");
	instanceVec = v1->getInstance();
	EXPECT_EQ(instanceVec->size(), 7);
	EXPECT_EQ(v1->getValue(1)->getString(), v1->getString());

	ConstantSP index = Util::createInt(3);
	VectorSP indexVec = Util::createIndexVector(1, 2);
	VectorSP valVec = Util::createVector(DT_INT128, 2, 2);
	valVec->set(0, Util::parseConstant(DT_INT128, "e1671797c52e15f763380b45e841ec66"));
	valVec->set(1, Util::parseConstant(DT_INT128, "e1671797c52e15f763380b45e841ec77"));
	EXPECT_FALSE(v1->set(index, Util::createInt(4)));
	v1->set(index, Util::parseConstant(DT_INT128, "e1671797c52e15f763380b45e841ec88"));
	EXPECT_EQ(v1->get(3)->getString(), "e1671797c52e15f763380b45e841ec88");
	v1->set(index, Util::createNullConstant(DT_INT128));
	EXPECT_TRUE(v1->get(3)->isNull());
	v1->set(indexVec, valVec);
	EXPECT_EQ(v1->getString(), "[e1671797c52e15f763380b45e841ec55,e1671797c52e15f763380b45e841ec66,e1671797c52e15f763380b45e841ec77,,e1671797c52e15f763380b45e841ec00,,e1671797c52e15f763380b45e841ec00]");

	instanceVec->fill(0, 7, v1);
	EXPECT_EQ(instanceVec->getString(), v1->getString());

	EXPECT_FALSE(instanceVec->remove(index));
	instanceVec->remove(indexVec);
	EXPECT_EQ(instanceVec->getString(), "[e1671797c52e15f763380b45e841ec55,,e1671797c52e15f763380b45e841ec00,,e1671797c52e15f763380b45e841ec00]");
	EXPECT_FALSE(instanceVec->remove(10));
	instanceVec->remove(1);
	EXPECT_EQ(instanceVec->getString(), "[e1671797c52e15f763380b45e841ec55,,e1671797c52e15f763380b45e841ec00,]");
	instanceVec->remove(-1);
	EXPECT_EQ(instanceVec->getString(), "[,e1671797c52e15f763380b45e841ec00,]");

	v1->next(4);
	EXPECT_EQ(v1->getString(), "[e1671797c52e15f763380b45e841ec00,,e1671797c52e15f763380b45e841ec00,,,,]");
	v1->prev(4);
	EXPECT_EQ(v1->getString(), "[,,,,e1671797c52e15f763380b45e841ec00,,e1671797c52e15f763380b45e841ec00]");

	VectorSP valVec2 = Util::createVector(DT_INT128, 7, 7);
	for (unsigned int i = 0; i < 7; i++)
		valVec2->set(i, Util::parseConstant(DT_INT128, "e1671797c52e15f763380b45e841ec" + to_string(4 * 10 + i)));
	VectorSP valVec3 = valVec2->getSubVector(0, 3);

	EXPECT_FALSE(v1->assign(valVec3));
	v1->assign(valVec2);
	EXPECT_EQ(v1->getValue(7)->getString(), valVec2->getString());

	int *buf3 = new int[1];
	buf3[0] = 1;
	EXPECT_TRUE(instanceVec->setData(1, 1, v1->getDataBuffer(0, 1, buf3)));
	EXPECT_EQ(instanceVec->getString(), "[,e1671797c52e15f763380b45e841ec40,]");
	instanceVec->setData(0, 1, buf3);
	cout << instanceVec->getString() << endl;

	EXPECT_ANY_THROW(instanceVec->getInt128());
	VectorSP valVec4 = instanceVec->getSubVector(0, 1);
	cout << valVec4->getInt128().getString() << endl;

	cout << v1->getAllocatedMemory(1) << endl;
	// v1->initialize();
	// v1->clear();
	cout << v1->getString() << endl;
	delete[] buf1, buf2, buf3;
}

TEST_F(DataformVectorTest, testInt128nullVector)
{
	VectorSP v1 = Util::createVector(DT_INT128, 2, 2);
	v1->setNull(0);
	v1->setNull(1);
	string script = "a=[int128(''),int128('')];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0; i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testUuidVector)
{
	VectorSP v1 = Util::createVector(DT_UUID, 2, 2);
	v1->set(0, Util::parseConstant(DT_UUID, "5d212a78-cc48-e3b1-4235-b4d91473ee87"));
	v1->set(1, Util::parseConstant(DT_UUID, "5d212a78-cc48-e3b1-4235-b4d91473ee88"));
	string script = "a=[uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87'),uuid('5d212a78-cc48-e3b1-4235-b4d91473ee88')];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());

	EXPECT_ANY_THROW(v1->setString(1, "123"));
	EXPECT_TRUE(v1->isFastMode());
	EXPECT_TRUE(v1->sizeable());
	v1->setNull();
	EXPECT_FALSE(v1->hasNull(0, 1));
	v1->reverse();
	EXPECT_EQ(v1->get(0)->getString(), "5d212a78-cc48-e3b1-4235-b4d91473ee88");
	EXPECT_EQ(v1->get(1)->getString(), "5d212a78-cc48-e3b1-4235-b4d91473ee87");

	EXPECT_ANY_THROW(v1->getBool());
	EXPECT_ANY_THROW(v1->getChar());
	EXPECT_ANY_THROW(v1->getShort());
	EXPECT_ANY_THROW(v1->getLong());
	EXPECT_ANY_THROW(v1->getFloat());
	EXPECT_ANY_THROW(v1->getDouble());
	EXPECT_ANY_THROW(v1->getIndex());
	EXPECT_ANY_THROW(v1->neg());
	EXPECT_ANY_THROW(v1->getBinary());

	// cout<<v1->get(Util::createInt(1))<<endl;
	EXPECT_EQ(v1->compare(0, Util::parseConstant(DT_UUID, "5d212a78-cc48-e3b1-4235-b4d91473ee87")), 1);
	v1->replace(Util::parseConstant(DT_UUID, "5d212a78-cc48-e3b1-4235-b4d91473ee87"), Util::parseConstant(DT_UUID, "5d212a78-cc48-e3b1-4235-b4d91473ee99"));
	EXPECT_EQ(v1->get(1)->getString(), "5d212a78-cc48-e3b1-4235-b4d91473ee99");

	v1->append(Util::createNullConstant(DT_UUID));
	EXPECT_ANY_THROW(v1->nullFill(Util::createInt(1)));
	v1->nullFill(Util::parseConstant(DT_UUID, "5d212a78-cc48-e3b1-4235-b4d91473ee99"));
	EXPECT_EQ(v1->get(2)->getString(), "5d212a78-cc48-e3b1-4235-b4d91473ee99");

	char *buf1 = new char[3];
	char *buf2 = new char[3];
	v1->isNull(0, 3, buf1);
	EXPECT_FALSE((int)buf1[0]);
	EXPECT_FALSE((int)buf1[1]);
	EXPECT_FALSE((int)buf1[2]);
	v1->isValid(0, 3, buf2);
	EXPECT_TRUE((int)buf2[0]);
	EXPECT_TRUE((int)buf2[1]);
	EXPECT_TRUE((int)buf2[2]);

	string strEmpty = "";
	string strlt36 = "192.168.1.1";
	string uuidstr = "5d212a78-cc48-e3b1-4235-b4d91473ee00";
	string *pstrEmpty = &strEmpty;
	string *pstrlt36 = &strlt36;
	string *puuidstr = &uuidstr;
	// EXPECT_FALSE(v1->appendString(pstrEmpty,10));
	v1->appendString(pstrEmpty, 1);
	EXPECT_EQ(v1->get(3)->getString(), Util::createNullConstant(DT_UUID)->getString());
	EXPECT_FALSE(v1->appendString(pstrlt36, 1));
	v1->appendString(puuidstr, 1);
	EXPECT_EQ(v1->get(4)->getString(), uuidstr);
	EXPECT_TRUE(v1->hasNull());

	char *c1 = (char *)strEmpty.data();
	char *c2 = (char *)strlt36.data();
	char *c3 = (char *)uuidstr.data();
	char **pc1 = &c1;
	char **pc2 = &c2;
	char **pc3 = &c3;
	// EXPECT_FALSE(v1->appendString(pc1,10));
	v1->appendString(pc1, 1);
	EXPECT_EQ(v1->get(5)->getString(), Util::createNullConstant(DT_UUID)->getString());
	EXPECT_FALSE(v1->appendString(pc2, 1));
	v1->appendString(pc3, 1);
	EXPECT_EQ(v1->get(6)->getString(), c3);
	EXPECT_TRUE(v1->hasNull());
	cout << v1->getString() << endl;
}

TEST_F(DataformVectorTest, testUuidnullVector)
{
	VectorSP v1 = Util::createVector(DT_UUID, 2, 2);
	v1->setNull(0);
	v1->setNull(1);
	string script = "a=[uuid(''),uuid('')];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0; i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testIpaddrVector)
{
	VectorSP v1 = Util::createVector(DT_IP, 2, 2);
	v1->set(0, Util::parseConstant(DT_IP, "192.168.1.13"));
	v1->set(1, Util::parseConstant(DT_IP, "192.168.1.15"));
	string script = "a=[ipaddr(`192.168.1.13),ipaddr(`192.168.1.15)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());

	EXPECT_ANY_THROW(v1->setString(1, "123"));
	EXPECT_TRUE(v1->isFastMode());
	EXPECT_TRUE(v1->sizeable());
	v1->setNull();
	EXPECT_FALSE(v1->hasNull(0, 1));
	v1->reverse();
	EXPECT_EQ(v1->get(0)->getString(), "192.168.1.15");
	EXPECT_EQ(v1->get(1)->getString(), "192.168.1.13");

	EXPECT_ANY_THROW(v1->getBool());
	EXPECT_ANY_THROW(v1->getChar());
	EXPECT_ANY_THROW(v1->getShort());
	EXPECT_ANY_THROW(v1->getLong());
	EXPECT_ANY_THROW(v1->getFloat());
	EXPECT_ANY_THROW(v1->getDouble());
	EXPECT_ANY_THROW(v1->getIndex());
	EXPECT_ANY_THROW(v1->neg());
	EXPECT_ANY_THROW(v1->getBinary());

	// cout<<v1->get(Util::createInt(1))<<endl;
	EXPECT_EQ(v1->compare(0, Util::parseConstant(DT_IP, "192.168.1.13")), 1);
	v1->replace(Util::parseConstant(DT_IP, "192.168.1.13"), Util::parseConstant(DT_IP, "192.168.1.255"));
	EXPECT_EQ(v1->get(1)->getString(), "192.168.1.255");

	v1->append(Util::createNullConstant(DT_IP));
	EXPECT_ANY_THROW(v1->nullFill(Util::createInt(1)));
	v1->nullFill(Util::parseConstant(DT_IP, "192.168.1.255"));
	EXPECT_EQ(v1->get(2)->getString(), "192.168.1.255");

	char *buf1 = new char[3];
	char *buf2 = new char[3];
	v1->isNull(0, 3, buf1);
	EXPECT_FALSE((int)buf1[0]);
	EXPECT_FALSE((int)buf1[1]);
	EXPECT_FALSE((int)buf1[2]);
	v1->isValid(0, 3, buf2);
	EXPECT_TRUE((int)buf2[0]);
	EXPECT_TRUE((int)buf2[1]);
	EXPECT_TRUE((int)buf2[2]);

	string strEmpty = "";
	string errstr = "256.256.0.0";
	string ipstr = "192.168.1.1";
	string *pstrEmpty = &strEmpty;
	string *perrstr = &errstr;
	string *pipstr = &ipstr;
	// EXPECT_FALSE(v1->appendString(pstrEmpty,10));
	v1->appendString(pstrEmpty, 1);
	EXPECT_EQ(v1->get(3)->getString(), Util::createNullConstant(DT_IP)->getString());
	EXPECT_FALSE(v1->appendString(perrstr, 1));
	v1->appendString(pipstr, 1);
	EXPECT_EQ(v1->get(4)->getString(), ipstr);
	EXPECT_TRUE(v1->hasNull());

	char *c1 = (char *)strEmpty.data();
	char *c2 = (char *)errstr.data();
	char *c3 = (char *)ipstr.data();
	char **pc1 = &c1;
	char **pc2 = &c2;
	char **pc3 = &c3;
	// EXPECT_FALSE(v1->appendString(pc1,10));
	v1->appendString(pc1, 1);
	EXPECT_EQ(v1->get(5)->getString(), Util::createNullConstant(DT_IP)->getString());
	EXPECT_FALSE(v1->appendString(pc2, 1));
	v1->appendString(pc3, 1);
	EXPECT_EQ(v1->get(6)->getString(), c3);
	EXPECT_TRUE(v1->hasNull());
	cout << v1->getString() << endl;
}

TEST_F(DataformVectorTest, testIpaddrnullVector)
{
	VectorSP v1 = Util::createVector(DT_IP, 2, 2);
	v1->setNull(0);
	v1->setNull(1);
	string script = "a=[ipaddr(''),ipaddr('')];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(v1->getType(), res_v->getType());
	for (int i = 0; i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testSymbolVector)
{
	VectorSP v1 = Util::createVector(DT_SYMBOL, 2, 2);
	v1->set(0, Util::createString("sym1"));
	v1->set(1, Util::createString("sym2"));
	string script = "a=symbol[`sym1,`sym2];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});

	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(conn.run("v1")->getType(), res_v->getType());

	EXPECT_ANY_THROW(v1->getBool());
	EXPECT_ANY_THROW(v1->getChar());
	EXPECT_ANY_THROW(v1->getShort());
	EXPECT_ANY_THROW(v1->getLong());
	EXPECT_ANY_THROW(v1->getFloat());
	EXPECT_ANY_THROW(v1->getDouble());
	EXPECT_ANY_THROW(v1->getIndex());
	EXPECT_ANY_THROW(v1->neg());

	EXPECT_FALSE(v1->isIndexArray());
	EXPECT_EQ(v1->getIndexArray(), (INDEX *)NULL);
	EXPECT_EQ(v1->asof(Util::createString("sym1")), 0);
	EXPECT_EQ(v1->asof(Util::createString("sym2")), 1);

	EXPECT_EQ(v1->getUnitLength(), 4);
	EXPECT_TRUE(v1->sizeable());
	EXPECT_EQ(v1->compare(1, Util::createString("sym2")), 0);
	EXPECT_EQ(v1->getStringRef(), "sym1");
	v1->setNull(); // nothing to do
	EXPECT_FALSE(v1->hasNull());

	v1->reverse();
	EXPECT_EQ(v1->get(0)->getString(), Util::createString("sym2")->getString());
	EXPECT_EQ(v1->get(1)->getString(), Util::createString("sym1")->getString());
	v1->reverse(0, 1);
	EXPECT_EQ(v1->get(1)->getString(), Util::createString("sym2")->getString());
	EXPECT_EQ(v1->get(0)->getString(), Util::createString("sym1")->getString());

	cout << v1->getDataArray() << endl;
	v1->append(Util::createNullConstant(DT_STRING));
	v1->nullFill(Util::createString("sym3"));
	EXPECT_EQ(v1->get(2)->getString(), "sym3");
	EXPECT_EQ(v1->getSubVector(1, 2)->getString(), "[\"sym2\",\"sym3\"]");
	EXPECT_EQ(v1->getSubVector(1, 2, 2)->getString(), "[\"sym2\",\"sym3\"]");
	EXPECT_ANY_THROW(v1->getSubVector(0, 2, -1)->getString());

	VectorSP instanceVec = v1->getInstance(2);
	instanceVec->set(0, Util::createString("1"));
	instanceVec->set(1, Util::createString("2"));
	EXPECT_EQ(instanceVec->getString(), "[\"1\",\"2\"]");

	EXPECT_FALSE(v1->append(Util::createString("1"), 2));
	v1->append(Util::createString("sym4"), 1);
	string res = v1->get(3)->getString();
	EXPECT_EQ(res, "sym4");

	string strEmpty = "";
	string appendstr = "sym5";
	string *pstrEmpty = &strEmpty;
	string *pappendstr = &appendstr;
	v1->appendString(pstrEmpty, 1);
	EXPECT_EQ(v1->get(4)->getString(), "");
	v1->appendString(pappendstr, 1);
	EXPECT_EQ(v1->get(5)->getString(), appendstr);
	EXPECT_TRUE(v1->hasNull());

	char *c1 = (char *)strEmpty.data();
	char *c2 = (char *)appendstr.data();
	char **pc1 = &c1;
	char **pc2 = &c2;
	v1->appendString(pc1, 1);
	// cout<<v1->getString()<<endl;
	EXPECT_EQ(v1->get(6)->getString(), "");

	v1->setString("");
	EXPECT_EQ(v1->get(0)->getString(), "");

	string bufstring = "sym1";
	char *pbuf = (char *)bufstring.data();
	char **strbuf = &pbuf;
	EXPECT_FALSE(v1->setString(0, 10, strbuf));
	v1->setString(0, 1, strbuf);
	EXPECT_EQ(v1->get(0)->getString(), "sym1");

	v1->replace(Util::createString(""), Util::createString("replaceVal"));

	ConstantSP indexVal = Util::createInt(0);
	VectorSP indexVec = Util::createIndexVector(1, 1);
	VectorSP intIndexVec = Util::createVector(DT_INT, 0, 1);
	intIndexVec->append(Util::createInt(2));

	v1->set(indexVal, Util::createNullConstant(DT_STRING));
	v1->set(indexVec, Util::createString("indexVec"));
	v1->set(intIndexVec, Util::createString("intIndexVec"));
	EXPECT_TRUE(v1->hasNull());
	EXPECT_EQ(v1->getString(), "[,\"indexVec\",\"intIndexVec\",\"sym4\",\"replaceVal\",\"sym5\",\"replaceVal\"]");

	VectorSP valVec1 = Util::createVector(DT_STRING, 2, 2);
	VectorSP valVec2 = Util::createVector(DT_STRING, 1, 1);
	ConstantSP val1 = Util::createString("value1");
	valVec1->set(0, Util::createString("vec1"));
	valVec1->setNull(1);
	valVec2->set(0, Util::createString("vec2"));
	v1->fill(0, 1, val1);
	v1->fill(1, 1, valVec1);
	v1->fill(2, 2, valVec2);
	v1->fill(4, 2, valVec1);
	EXPECT_EQ(v1->getString(), "[\"value1\",\"[\\\"vec1\\\",]\",\"[\\\"vec2\\\"]\",\"[\\\"vec2\\\"]\",\"vec1\",,\"replaceVal\"]");

	EXPECT_TRUE(v1->validIndex(-1));
	EXPECT_FALSE(v1->validIndex(5));

	char **buf = new char *[v1->size()];
	v1->getStringConst(0, v1->size(), buf);
	for (auto i = 0; i < v1->size(); i++)
		EXPECT_EQ(buf[i], v1->get(i)->getString());

	delete[] buf;
}

TEST_F(DataformVectorTest, testSymbolnullVector)
{
	VectorSP v1 = Util::createVector(DT_SYMBOL, 2, 2);
	v1->setNull(0);
	v1->setNull(1);
	string script = "a=symbol['',''];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getScript(), res_v->getScript());
	EXPECT_EQ(conn.run("v1")->getType(), res_v->getType());
	for (int i = 0; i < 2; i++)
		EXPECT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testDecimal32Vector)
{
	EXPECT_ANY_THROW(VectorSP v0 = Util::createVector(DT_DECIMAL32, 2, 2, true, 10));
	VectorSP v1 = Util::createVector(DT_DECIMAL32, 2, 2, true, 2);
	v1->set(0, Util::createDecimal32(2, 0.315));
	v1->set(1, Util::createDecimal32(2, 3.1));

	string script = "a=[decimal32(0.31,2),decimal32(3.10,2)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getRawType());
	EXPECT_EQ(Util::getCategoryString(v1->getCategory()), "DENARY");
	EXPECT_EQ(v1->getExtraParamForType(), 2);
	EXPECT_EQ(v1->getString(1), res_v->get(1)->getString());

	float buf1[1];
	EXPECT_EQ(v1->getFloat(1), res_v->get(1)->getFloat());
	v1->getFloat(0, 1, buf1);
	EXPECT_EQ((float)buf1[0], res_v->get(0)->getFloat());
	double buf2[1];
	EXPECT_EQ(v1->getDouble(1), res_v->get(1)->getDouble());
	v1->getDouble(0, 1, buf2);
	EXPECT_EQ((double)buf2[0], res_v->get(0)->getDouble());

	v1->setFloat(0, 0.999);
	v1->setDouble(1, 2.5);
	EXPECT_EQ(v1->getString(), "[0.99,2.50]");
	const float buf3[1] = {4.1};
	const double buf4[1] = {5.6320001};
	v1->setFloat(0, 1, buf3);
	v1->setDouble(1, 1, buf4);
	EXPECT_EQ(v1->getString(), "[4.10,5.63]");

	v1->set(0, Util::createDecimal32(1, 1.2322));
	v1->set(1, Util::createDecimal32(5, 3.15));
	EXPECT_EQ(v1->getType(), DT_DECIMAL32);
	EXPECT_EQ(v1->getString(), "[1.20,3.15]");

	VectorSP indexV = Util::createIndexVector(0, 2);
	VectorSP valV = Util::createVector(DT_DECIMAL32, 2, 2);
	VectorSP errTypevalV = Util::createVector(DT_NANOTIME, 1, 1);
	VectorSP diffScalevalV = Util::createVector(DT_DECIMAL32, 2, 2); // default scale=0
	valV->set(0, Util::createDecimal32(2, 13.0582));
	valV->setNull(1);
	errTypevalV->set(0, Util::createNanoTime(1000000000000));
	diffScalevalV->set(0, Util::createDecimal32(5, 1.5));
	diffScalevalV->set(1, Util::createNullConstant(DT_DECIMAL32));
	string strbuf[2] = {"0", "-1356.14655"};
	string strerrbuf[1] = {"abcdefg"};
	char *charbuf[3] = {(char *)"9.4856", (char *)"84912.39123", (char *)"31945.1"};
	char *charerrbuf[1] = {(char *)"abcdefg"};

	v1->append(Util::createDecimal32(2, 1.5));
	v1->set(2, Util::createNullConstant(DT_DECIMAL32));
	EXPECT_FALSE(v1->append(Util::createDecimal32(2, 1.5), 3));
	v1->append(Util::createDecimal32(2, 3.12123), 1);
	EXPECT_EQ(v1->getString(), "[1.20,3.15,,3.12]");
	v1->append(Util::createNullConstant(DT_DECIMAL32), 1);
	EXPECT_EQ(v1->getString(), "[1.20,3.15,,3.12,]");
	EXPECT_ANY_THROW(v1->append(errTypevalV, 1));
	v1->append(diffScalevalV, 2);
	EXPECT_EQ(v1->getString(), "[1.20,3.15,,3.12,,1.00,]");
	v1->append(valV, 2);
	EXPECT_EQ(v1->getString(), "[1.20,3.15,,3.12,,1.00,,13.00,]");
	EXPECT_ANY_THROW(v1->appendString(charerrbuf, 1));
	v1->appendString(charbuf, 3);
	EXPECT_EQ(v1->getString(), "[1.20,3.15,,3.12,,1.00,,13.00,,9.48,84912.39,31945.10]");
	EXPECT_ANY_THROW(v1->appendString(strerrbuf, 1));
	v1->appendString(strbuf, 2);
	EXPECT_EQ(v1->getString(), "[1.20,3.15,,3.12,,1.00,,13.00,,9.48,84912.39,31945.10,0.00,-1356.14]");

	conn.upload("v1", v1);
	EXPECT_TRUE(conn.run("eqObj(v1,decimal32([1.20,3.15,NULL,3.12,NULL,1.00,NULL,13.00,NULL,9.48,84912.39,31945.10,0.00,-1356.14],2))")->getBool());

	v1->nullFill(Util::createFloat(4.3335));
	EXPECT_EQ(v1->getString(), "[1.20,3.15,4.33,3.12,4.33,1.00,4.33,13.00,4.33,9.48,84912.39,31945.10,0.00,-1356.14]");

	VectorSP v2 = v1->getSubVector(0, 2, 10);
	cout << v2->getString() << endl;
	EXPECT_TRUE(v2->set(Util::createInt(0), Util::createDecimal32(2, 2.7999)));
	EXPECT_ANY_THROW(v2->set(Util::createIndexVector(1, 1), errTypevalV));
	v2->set(Util::createIndexVector(1, 1), diffScalevalV);
	EXPECT_EQ(v2->getString(), "[2.79,1.00]");
	v2->set(indexV, valV);
	EXPECT_EQ(v2->getString(), "[13.00,]");

	EXPECT_TRUE(v2->get(1)->isNull());
	// EXPECT_TRUE(v2->get(Util::createInt(1))->isNull());
	// EXPECT_TRUE(v2->get(Util::createIndexVector(1,1))->isNull());

	EXPECT_ANY_THROW(v2->fill(1, 5, valV));
	v2->fill(1, 1, Util::createDecimal32(5, 0));
	EXPECT_EQ(v2->getString(), "[13.00,0.00]");
	v2->fill(0, 2, Util::createNullConstant(DT_DECIMAL32));
	EXPECT_EQ(v2->getString(), "[,]");
	EXPECT_ANY_THROW(v2->fill(1, 1, errTypevalV));
	v2->fill(0, 2, diffScalevalV);
	EXPECT_EQ(v2->getString(), "[1.00,]");
	v2->fill(0, 2, valV);
	EXPECT_EQ(v2->getString(), "[13.00,]");

	EXPECT_FALSE(v2->validIndex(1));
	v2->fill(1, 1, Util::createDecimal32(5, 0));
	EXPECT_TRUE(v2->validIndex(-1));

	EXPECT_EQ(v2->compare(0, Util::createInt(12)), 1);
	EXPECT_EQ(v2->compare(1, Util::createInt(1)), -1);
	EXPECT_EQ(v2->compare(1, Util::createInt(0)), 0);
	EXPECT_EQ(v2->compare(0, Util::createDecimal32(3, 12.99)), 1);
	EXPECT_EQ(v2->compare(0, Util::createDecimal32(2, 13.01)), -1);
	EXPECT_EQ(v2->compare(1, Util::createDecimal32(1, 0)), 0);
	EXPECT_EQ(v2->compare(0, Util::createDecimal64(8, 12.999999999)), 1);
	EXPECT_EQ(v2->compare(0, Util::createDecimal64(10, 13.00001)), -1);
	EXPECT_EQ(v2->compare(1, Util::createDecimal64(1, 0)), 0);
	EXPECT_ANY_THROW(v2->compare(0, Util::createBlob("blob1")));
	v2->nullFill(Util::createInt(1));
}

TEST_F(DataformVectorTest, testDecimal32NullVector)
{
	for (int i = 0; i < 10; i++)
	{
		VectorSP v1 = Util::createVector(DT_DECIMAL32, 2, 2, true, i);
		v1->set(0, Util::createNullConstant(DT_DECIMAL32));
		v1->set(1, Util::createNullConstant(DT_DECIMAL32));

		string script = "a=[decimal32(NULL," + to_string(i) + "),decimal32(NULL," + to_string(i) + ")];a";
		VectorSP res_v = conn.run(script);
		conn.upload("v1", {v1});
		EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
		EXPECT_EQ(v1->getString(), res_v->getString());
		EXPECT_EQ(v1->getType(), res_v->getRawType());
		EXPECT_EQ(Util::getCategoryString(v1->getCategory()), "DENARY");
		EXPECT_EQ(v1->getExtraParamForType(), i);
	}
}

TEST_F(DataformVectorTest, testDecimal64Vector)
{
	EXPECT_ANY_THROW(VectorSP v0 = Util::createVector(DT_DECIMAL64, 2, 2, true, 19));
	VectorSP v1 = Util::createVector(DT_DECIMAL64, 2, 2, true, 2);
	v1->set(0, Util::createDecimal64(2, 0.315));
	v1->set(1, Util::createDecimal64(2, 3.1));

	string script = "a=[decimal64(0.31,2),decimal64(3.10,2)];a";
	VectorSP res_v = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
	EXPECT_EQ(v1->getString(), res_v->getString());
	EXPECT_EQ(v1->getType(), res_v->getRawType());
	EXPECT_EQ(Util::getCategoryString(v1->getCategory()), "DENARY");
	EXPECT_EQ(v1->getExtraParamForType(), 2);
	EXPECT_EQ(v1->getString(1), res_v->get(1)->getString());

	float buf1[1];
	EXPECT_EQ(v1->getFloat(1), res_v->get(1)->getFloat());
	v1->getFloat(0, 1, buf1);
	EXPECT_EQ((float)buf1[0], res_v->get(0)->getFloat());
	double buf2[1];
	EXPECT_EQ(v1->getDouble(1), res_v->get(1)->getDouble());
	v1->getDouble(0, 1, buf2);
	EXPECT_EQ((double)buf2[0], res_v->get(0)->getDouble());

	v1->setFloat(0, 0.999);
	v1->setDouble(1, 2.5);
	EXPECT_EQ(v1->getString(), "[0.99,2.50]");
	const float buf3[1] = {4.1};
	const double buf4[1] = {5.6320001};
	v1->setFloat(0, 1, buf3);
	v1->setDouble(1, 1, buf4);
	EXPECT_EQ(v1->getString(), "[4.10,5.63]");

	v1->set(0, Util::createDecimal64(1, 1.2322));
	v1->set(1, Util::createDecimal64(5, 3.15));
	EXPECT_EQ(v1->getType(), DT_DECIMAL64);
	EXPECT_EQ(v1->getString(), "[1.20,3.15]");

	VectorSP indexV = Util::createIndexVector(0, 2);
	VectorSP valV = Util::createVector(DT_DECIMAL64, 2, 2);
	VectorSP errTypevalV = Util::createVector(DT_NANOTIME, 1, 1);
	VectorSP diffScalevalV = Util::createVector(DT_DECIMAL64, 2, 2); // default scale=0
	valV->set(0, Util::createDecimal64(2, 13.0582));
	valV->setNull(1);
	errTypevalV->set(0, Util::createNanoTime(1000000000000));
	diffScalevalV->set(0, Util::createDecimal64(5, 1.5));
	diffScalevalV->set(1, Util::createNullConstant(DT_DECIMAL64));
	string strbuf[2] = {"0", "-1356.14655"};
	string strerrbuf[1] = {"abcdefg"};
	char *charbuf[3] = {(char *)"9.4856", (char *)"84912.39123", (char *)"31945.1"};
	char *charerrbuf[1] = {(char *)"abcdefg"};

	v1->append(Util::createDecimal64(2, 1.5));
	v1->set(2, Util::createNullConstant(DT_DECIMAL64));
	EXPECT_FALSE(v1->append(Util::createDecimal64(2, 1.5), 3));
	v1->append(Util::createDecimal64(2, 3.12123), 1);
	EXPECT_EQ(v1->getString(), "[1.20,3.15,,3.12]");
	v1->append(Util::createNullConstant(DT_DECIMAL64), 1);
	EXPECT_EQ(v1->getString(), "[1.20,3.15,,3.12,]");
	EXPECT_ANY_THROW(v1->append(errTypevalV, 1));
	v1->append(diffScalevalV, 2);
	EXPECT_EQ(v1->getString(), "[1.20,3.15,,3.12,,1.00,]");
	v1->append(valV, 2);
	EXPECT_EQ(v1->getString(), "[1.20,3.15,,3.12,,1.00,,13.00,]");
	EXPECT_ANY_THROW(v1->appendString(charerrbuf, 1));
	v1->appendString(charbuf, 3);
	EXPECT_EQ(v1->getString(), "[1.20,3.15,,3.12,,1.00,,13.00,,9.48,84912.39,31945.10]");
	EXPECT_ANY_THROW(v1->appendString(strerrbuf, 1));
	v1->appendString(strbuf, 2);
	EXPECT_EQ(v1->getString(), "[1.20,3.15,,3.12,,1.00,,13.00,,9.48,84912.39,31945.10,0.00,-1356.14]");

	conn.upload("v1", v1);
	EXPECT_TRUE(conn.run("eqObj(v1,decimal64([1.20,3.15,NULL,3.12,NULL,1.00,NULL,13.00,NULL,9.48,84912.39,31945.10,0.00,-1356.14],2))")->getBool());

	v1->nullFill(Util::createFloat(4.3335));
	EXPECT_EQ(v1->getString(), "[1.20,3.15,4.33,3.12,4.33,1.00,4.33,13.00,4.33,9.48,84912.39,31945.10,0.00,-1356.14]");

	VectorSP v2 = v1->getSubVector(0, 2, 10);
	cout << v2->getString() << endl;
	EXPECT_TRUE(v2->set(Util::createInt(0), Util::createDecimal64(2, 2.7999)));
	EXPECT_ANY_THROW(v2->set(Util::createIndexVector(1, 1), errTypevalV));
	v2->set(Util::createIndexVector(1, 1), diffScalevalV);
	EXPECT_EQ(v2->getString(), "[2.79,1.00]");
	v2->set(indexV, valV);
	EXPECT_EQ(v2->getString(), "[13.00,]");

	EXPECT_TRUE(v2->get(1)->isNull());
	// EXPECT_TRUE(v2->get(Util::createInt(1))->isNull());
	// EXPECT_TRUE(v2->get(Util::createIndexVector(1,1))->isNull());

	EXPECT_ANY_THROW(v2->fill(1, 5, valV));
	v2->fill(1, 1, Util::createDecimal64(5, 0));
	EXPECT_EQ(v2->getString(), "[13.00,0.00]");
	v2->fill(0, 2, Util::createNullConstant(DT_DECIMAL64));
	EXPECT_EQ(v2->getString(), "[,]");
	EXPECT_ANY_THROW(v2->fill(1, 1, errTypevalV));
	v2->fill(0, 2, diffScalevalV);
	EXPECT_EQ(v2->getString(), "[1.00,]");
	v2->fill(0, 2, valV);
	EXPECT_EQ(v2->getString(), "[13.00,]");

	EXPECT_FALSE(v2->validIndex(1));
	v2->fill(1, 1, Util::createDecimal64(5, 0));
	EXPECT_TRUE(v2->validIndex(-1));

	EXPECT_EQ(v2->compare(0, Util::createInt(12)), 1);
	EXPECT_EQ(v2->compare(1, Util::createInt(1)), -1);
	EXPECT_EQ(v2->compare(1, Util::createInt(0)), 0);
	EXPECT_EQ(v2->compare(0, Util::createDecimal32(3, 12.99)), 1);
	EXPECT_EQ(v2->compare(0, Util::createDecimal32(2, 13.01)), -1);
	EXPECT_EQ(v2->compare(1, Util::createDecimal32(1, 0)), 0);
	EXPECT_EQ(v2->compare(0, Util::createDecimal64(8, 12.999999999)), 1);
	EXPECT_EQ(v2->compare(0, Util::createDecimal64(10, 13.00001)), -1);
	EXPECT_EQ(v2->compare(1, Util::createDecimal64(1, 0)), 0);
	EXPECT_ANY_THROW(v2->compare(0, Util::createBlob("blob1")));
	v2->nullFill(Util::createInt(1));
}

TEST_F(DataformVectorTest, testDecimal64NullVector)
{
	for (int i = 0; i < 19; i++)
	{
		VectorSP v1 = Util::createVector(DT_DECIMAL64, 2, 2, true, i);
		v1->set(0, Util::createNullConstant(DT_DECIMAL64));
		v1->set(1, Util::createNullConstant(DT_DECIMAL64));

		string script = "a=[decimal64(NULL," + to_string(i) + "),decimal64(NULL," + to_string(i) + ")];a";
		VectorSP res_v = conn.run(script);
		conn.upload("v1", {v1});
		EXPECT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
		EXPECT_EQ(v1->getString(), res_v->getString());
		EXPECT_EQ(v1->getType(), res_v->getRawType());
		EXPECT_EQ(Util::getCategoryString(v1->getCategory()), "DENARY");
		EXPECT_EQ(v1->getExtraParamForType(), i);
	}
}

TEST_F(DataformVectorTest, testCreateStringVectorByDdbVector)
{
	vector<string> vals = {"str1", "str2", "str3", "str4", "str5", "str6"};
	int size = vals.size();
	string *val = new string[size * 2 + 2];
	for (int i = 0; i < size; i++)
	{
		val[i] = vals[i];
	}
	DdbVector<string> v1(val, size, size * 2 + 2);
	DdbVector<string> v2(0, size + 10);
	for (auto i = 0; i < size; i++)
	{
		v2.add(vals[i]);
	}
	v1.addNull();
	v2.addNull();
	v1.setNull(0);
	v2.setNull(0);

	const string str1 = "str1";
	v1.add(str1);
	v2.add(str1);

	VectorSP ddbv1 = v1.createVector(DT_STRING);
	VectorSP ddbv2 = v2.createVector(DT_STRING);
	vector<string> names = {"ddbv1", "ddbv2"};
	vector<ConstantSP> objs = {ddbv1, ddbv2};
	conn.upload(names, objs);
	conn.run("a=[NULL,`str2,`str3,`str4,`str5,`str6,NULL,`str1];");
	EXPECT_TRUE(conn.run("eqObj(ddbv1,a)")->getBool());
	EXPECT_TRUE(conn.run("eqObj(ddbv2,a)")->getBool());

	EXPECT_EQ(v1.size(), 8);
	EXPECT_EQ(v2.size(), 8);

	string *val_new = new string[size * 2 + 2];
	for (int i = 0; i < size; i++)
	{
		val_new[i] = vals[i];
	}
	DdbVector<string> v3(val_new, vals.size(), size * 2 + 2);
	DdbVector<string> v4(0, size * 2 + 2);
	string *appendbuf1 = new string[size];
	string *appendbuf2 = new string[size];
	string setbuf1[1];
	string setbuf2[1];
	setbuf1[0] = "set";
	setbuf2[0] = "set";
	const string appendconstbuf1[1] = {"append1str"};
	const string appendconstbuf2[1] = {"append1str"};
	const string setconstbuf1[1] = {"setstr"};
	const string setconstbuf2[1] = {"setstr"};
	for (int i = 0; i < size; i++)
	{
		appendbuf1[i] = vals[i];
	}

	// v3.append(buf,size); //append() is not supported for string DdbVector.
	// v3.set(buf,size); //set() is not supported for string DdbVector.
	v3.appendString(appendbuf1, size);
	v3.appendString(appendconstbuf1, 1);
	v3.setString(0, 1, setbuf1);
	v3.setString(1, 1, setconstbuf1);
	for (int i = 0; i < size; i++)
	{
		appendbuf2[i] = vals[i];
	}
	// v4.append(buf,size); //append() is not supported for string DdbVector.
	// v4.set(buf,size); //set() is not supported for string DdbVector.
	v4.appendString(appendbuf2, size);
	v4.appendString(appendconstbuf2, 1);
	v4.setString(0, 1, setbuf2);
	v4.setString(1, 1, setconstbuf2);

	VectorSP ddbv3 = v3.createVector(DT_STRING);
	VectorSP ddbv4 = v4.createVector(DT_STRING);
	// cout<<ddbv3->getString()<<endl;
	// cout<<ddbv4->getString()<<endl;
	conn.upload("ddbv3", ddbv3);
	conn.upload("ddbv4", ddbv4);
	conn.run("a=[`set,`setstr,`str3,`str4,`str5,`str6,`append1str];b=[`set,`setstr,`str3,`str4,`str5,`str6,`str1,`str2,`str3,`str4,`str5,`str6,`append1str]");
	EXPECT_TRUE(conn.run("eqObj(ddbv3,b)")->getBool());
	EXPECT_TRUE(conn.run("eqObj(ddbv4,a)")->getBool());

	for (int i = 0; i < v3.size(); i++)
	{
		EXPECT_EQ(v3.data()[i], ddbv3->get(i)->getString());
	}
	for (int i = 0; i < v4.size(); i++)
	{
		EXPECT_EQ(v4.data()[i], ddbv4->get(i)->getString());
	}

	delete[] appendbuf1, appendbuf2;
}

TEST_F(DataformVectorTest, testCreateCharVectorByDdbVector)
{
	vector<char> vals = {1, 2, 3, 4, 5, 6};
	int size = vals.size();
	char *val = new char[size * 2 + 2];
	for (int i = 0; i < size; i++)
	{
		val[i] = vals[i];
	}
	DdbVector<char> v1(val, size, size * 2 + 2);
	DdbVector<char> v2(0, size + 10);
	for (auto i = 0; i < size; i++)
	{
		v2.add(vals[i]);
	}
	v1.addNull();
	v2.addNull();
	v1.setNull(0);
	v2.setNull(0);

	const char str1 = 7;
	v1.add(str1);
	v2.add(str1);

	VectorSP ddbv1 = v1.createVector(DT_CHAR);
	VectorSP ddbv2 = v2.createVector(DT_CHAR);
	vector<string> names = {"ddbv1", "ddbv2"};
	vector<ConstantSP> objs = {ddbv1, ddbv2};
	conn.upload(names, objs);
	conn.run("a=[NULL,2,3,4,5,6,NULL,7];");
	EXPECT_TRUE(conn.run("eqObj(ddbv1,a)")->getBool());
	EXPECT_TRUE(conn.run("eqObj(ddbv2,a)")->getBool());

	EXPECT_EQ(v1.size(), 8);
	EXPECT_EQ(v2.size(), 8);

	char *val_new = new char[size * 2 + 2];
	for (int i = 0; i < size; i++)
	{
		val_new[i] = vals[i];
	}
	DdbVector<char> v3(val_new, size, size * 2 + 2);
	DdbVector<char> v4(0, size * 2 + 2);
	for (int i = 0; i < size; i++)
	{
		v4.add(vals[i]);
	}
	char setval1 = 8;
	char setval2 = 8;
	const char appendconstbuf1[1] = {9};
	const char appendconstbuf2[1] = {9};
	const char setconstval1 = 0;
	const char setconstval2 = 0;
	const char setconstval3[1] = {9};
	const char setconstval4[1] = {9};

	v3.append(appendconstbuf1, 1);
	v3.set(0, setval1);
	v3.set(1, setconstval1);
	v3.set(2, 1, setconstval3);

	v4.append(appendconstbuf2, 1);
	v4.set(0, setval2);
	v4.set(1, setconstval2);
	v4.set(2, 1, setconstval4);

	VectorSP ddbv3 = v3.createVector(DT_CHAR);
	VectorSP ddbv4 = v4.createVector(DT_CHAR);
	// cout<<ddbv3->getString()<<endl;
	// cout<<ddbv4->getString()<<endl;
	conn.upload("ddbv3", ddbv3);
	conn.upload("ddbv4", ddbv4);
	conn.run("a=char[8,0,9,4,5,6,9]");
	EXPECT_TRUE(conn.run("eqObj(ddbv3,a)")->getBool());
	EXPECT_TRUE(conn.run("eqObj(ddbv4,a)")->getBool());
}

TEST_F(DataformVectorTest, testCreateIntVectorByDdbVector)
{
	vector<int> vals = {1, 2, 3, 4, 5, 6};
	int size = vals.size();
	int *val = new int[size * 2 + 2];
	for (int i = 0; i < size; i++)
	{
		val[i] = vals[i];
	}
	DdbVector<int> v1(val, size, size * 2 + 2);
	DdbVector<int> v2(0, size + 10);
	for (auto i = 0; i < size; i++)
	{
		v2.add(vals[i]);
	}
	v1.addNull();
	v2.addNull();
	v1.setNull(0);
	v2.setNull(0);

	const int str1 = 7;
	v1.add(str1);
	v2.add(str1);

	VectorSP ddbv1 = v1.createVector(DT_INT);
	VectorSP ddbv2 = v2.createVector(DT_INT);
	vector<string> names = {"ddbv1", "ddbv2"};
	vector<ConstantSP> objs = {ddbv1, ddbv2};
	conn.upload(names, objs);
	conn.run("a=[NULL,2,3,4,5,6,NULL,7];");
	EXPECT_TRUE(conn.run("eqObj(ddbv1,a)")->getBool());
	EXPECT_TRUE(conn.run("eqObj(ddbv2,a)")->getBool());

	EXPECT_EQ(v1.size(), 8);
	EXPECT_EQ(v2.size(), 8);

	int *val_new = new int[size * 2 + 2];
	for (int i = 0; i < size; i++)
	{
		val_new[i] = vals[i];
	}
	DdbVector<int> v3(val_new, size, size * 2 + 2);
	DdbVector<int> v4(0, size * 2 + 2);
	for (int i = 0; i < size; i++)
	{
		v4.add(vals[i]);
	}
	int setval1 = 8;
	int setval2 = 8;
	const int appendconstbuf1[1] = {9};
	const int appendconstbuf2[1] = {9};
	const int setconstval1 = 0;
	const int setconstval2 = 0;
	const int setconstval3[1] = {9};
	const int setconstval4[1] = {9};

	v3.append(appendconstbuf1, 1);
	v3.set(0, setval1);
	v3.set(1, setconstval1);
	v3.set(2, 1, setconstval3);

	v4.append(appendconstbuf2, 1);
	v4.set(0, setval2);
	v4.set(1, setconstval2);
	v4.set(2, 1, setconstval4);

	VectorSP ddbv3 = v3.createVector(DT_INT);
	VectorSP ddbv4 = v4.createVector(DT_INT);
	// cout<<ddbv3->getString()<<endl;
	// cout<<ddbv4->getString()<<endl;
	conn.upload("ddbv3", ddbv3);
	conn.upload("ddbv4", ddbv4);
	conn.run("a=int[8,0,9,4,5,6,9]");
	EXPECT_TRUE(conn.run("eqObj(ddbv3,a)")->getBool());
	EXPECT_TRUE(conn.run("eqObj(ddbv4,a)")->getBool());
}

TEST_F(DataformVectorTest, testCreateShortVectorByDdbVector)
{
	vector<short> vals = {1, 2, 3, 4, 5, 6};
	int size = vals.size();
	short *val = new short[size * 2 + 2];
	for (int i = 0; i < size; i++)
	{
		val[i] = vals[i];
	}
	DdbVector<short> v1(val, size, size * 2 + 2);
	DdbVector<short> v2(0, size + 10);
	for (auto i = 0; i < size; i++)
	{
		v2.add(vals[i]);
	}
	v1.addNull();
	v2.addNull();
	v1.setNull(0);
	v2.setNull(0);

	const short str1 = 7;
	v1.add(str1);
	v2.add(str1);

	VectorSP ddbv1 = v1.createVector(DT_SHORT);
	VectorSP ddbv2 = v2.createVector(DT_SHORT);
	vector<string> names = {"ddbv1", "ddbv2"};
	vector<ConstantSP> objs = {ddbv1, ddbv2};
	conn.upload(names, objs);
	conn.run("a=[NULL,2,3,4,5,6,NULL,7];");
	EXPECT_TRUE(conn.run("eqObj(ddbv1,a)")->getBool());
	EXPECT_TRUE(conn.run("eqObj(ddbv2,a)")->getBool());

	EXPECT_EQ(v1.size(), 8);
	EXPECT_EQ(v2.size(), 8);

	short *val_new = new short[size * 2 + 2];
	for (int i = 0; i < size; i++)
	{
		val_new[i] = vals[i];
	}
	DdbVector<short> v3(val_new, size, size * 2 + 2);
	DdbVector<short> v4(0, size * 2 + 2);
	for (int i = 0; i < size; i++)
	{
		v4.add(vals[i]);
	}
	short setval1 = 8;
	short setval2 = 8;
	const short appendconstbuf1[1] = {9};
	const short appendconstbuf2[1] = {9};
	const short setconstval1 = 0;
	const short setconstval2 = 0;
	const short setconstval3[1] = {9};
	const short setconstval4[1] = {9};

	v3.append(appendconstbuf1, 1);
	v3.set(0, setval1);
	v3.set(1, setconstval1);
	v3.set(2, 1, setconstval3);

	v4.append(appendconstbuf2, 1);
	v4.set(0, setval2);
	v4.set(1, setconstval2);
	v4.set(2, 1, setconstval4);

	VectorSP ddbv3 = v3.createVector(DT_SHORT);
	VectorSP ddbv4 = v4.createVector(DT_SHORT);
	// cout<<ddbv3->getString()<<endl;
	// cout<<ddbv4->getString()<<endl;
	conn.upload("ddbv3", ddbv3);
	conn.upload("ddbv4", ddbv4);
	conn.run("a=short[8,0,9,4,5,6,9]");
	EXPECT_TRUE(conn.run("eqObj(ddbv3,a)")->getBool());
	EXPECT_TRUE(conn.run("eqObj(ddbv4,a)")->getBool());
}

TEST_F(DataformVectorTest, testCreateLongVectorByDdbVector)
{
	vector<long long> vals = {1, 2, 3, 4, 5, 6};
	int size = vals.size();
	long long *val = new long long[size * 2 + 2];
	for (int i = 0; i < size; i++)
	{
		val[i] = vals[i];
	}
	DdbVector<long long> v1(val, size, size * 2 + 2);
	DdbVector<long long> v2(0, size + 10);
	for (auto i = 0; i < size; i++)
	{
		v2.add(vals[i]);
	}
	v1.addNull();
	v2.addNull();
	v1.setNull(0);
	v2.setNull(0);

	const long long str1 = 7;
	v1.add(str1);
	v2.add(str1);

	VectorSP ddbv1 = v1.createVector(DT_LONG);
	VectorSP ddbv2 = v2.createVector(DT_LONG);
	vector<string> names = {"ddbv1", "ddbv2"};
	vector<ConstantSP> objs = {ddbv1, ddbv2};
	conn.upload(names, objs);
	conn.run("a=[NULL,2,3,4,5,6,NULL,7];");
	EXPECT_TRUE(conn.run("eqObj(ddbv1,a)")->getBool());
	EXPECT_TRUE(conn.run("eqObj(ddbv2,a)")->getBool());

	EXPECT_EQ(v1.size(), 8);
	EXPECT_EQ(v2.size(), 8);

	long long *val_new = new long long[size * 2 + 2];
	for (int i = 0; i < size; i++)
	{
		val_new[i] = vals[i];
	}
	DdbVector<long long> v3(val_new, size, size * 2 + 2);
	DdbVector<long long> v4(0, size * 2 + 2);
	for (int i = 0; i < size; i++)
	{
		v4.add(vals[i]);
	}
	long long setval1 = 8;
	long long setval2 = 8;
	const long long appendconstbuf1[1] = {9};
	const long long appendconstbuf2[1] = {9};
	const long long setconstval1 = 0;
	const long long setconstval2 = 0;
	const long long setconstval3[1] = {9};
	const long long setconstval4[1] = {9};

	v3.append(appendconstbuf1, 1);
	v3.set(0, setval1);
	v3.set(1, setconstval1);
	v3.set(2, 1, setconstval3);

	v4.append(appendconstbuf2, 1);
	v4.set(0, setval2);
	v4.set(1, setconstval2);
	v4.set(2, 1, setconstval4);

	VectorSP ddbv3 = v3.createVector(DT_LONG);
	VectorSP ddbv4 = v4.createVector(DT_LONG);
	// cout<<ddbv3->getString()<<endl;
	// cout<<ddbv4->getString()<<endl;
	conn.upload("ddbv3", ddbv3);
	conn.upload("ddbv4", ddbv4);
	conn.run("a=long[8,0,9,4,5,6,9]");
	EXPECT_TRUE(conn.run("eqObj(ddbv3,a)")->getBool());
	EXPECT_TRUE(conn.run("eqObj(ddbv4,a)")->getBool());
}

TEST_F(DataformVectorTest, testCreateFloatVectorByDdbVector)
{
	vector<float> vals = {1, 2, 3, 4, 5, 6};
	int size = vals.size();
	float *val = new float[size * 2 + 2];
	for (int i = 0; i < size; i++)
	{
		val[i] = vals[i];
	}
	DdbVector<float> v1(val, size, size * 2 + 2);
	DdbVector<float> v2(0, size + 10);
	for (auto i = 0; i < size; i++)
	{
		v2.add(vals[i]);
	}
	v1.addNull();
	v2.addNull();
	v1.setNull(0);
	v2.setNull(0);

	const float str1 = 7;
	v1.add(str1);
	v2.add(str1);

	VectorSP ddbv1 = v1.createVector(DT_FLOAT);
	VectorSP ddbv2 = v2.createVector(DT_FLOAT);
	vector<string> names = {"ddbv1", "ddbv2"};
	vector<ConstantSP> objs = {ddbv1, ddbv2};
	conn.upload(names, objs);
	conn.run("a=float[NULL,2,3,4,5,6,NULL,7];");
	EXPECT_TRUE(conn.run("eqObj(ddbv1,a)")->getBool());
	EXPECT_TRUE(conn.run("eqObj(ddbv2,a)")->getBool());

	EXPECT_EQ(v1.size(), 8);
	EXPECT_EQ(v2.size(), 8);

	float *val_new = new float[size * 2 + 2];
	for (int i = 0; i < size; i++)
	{
		val_new[i] = vals[i];
	}
	DdbVector<float> v3(val_new, size, size * 2 + 2);
	DdbVector<float> v4(0, size * 2 + 2);
	for (int i = 0; i < size; i++)
	{
		v4.add(vals[i]);
	}
	float setval1 = 8;
	float setval2 = 8;
	const float appendconstbuf1[1] = {9};
	const float appendconstbuf2[1] = {9};
	const float setconstval1 = 0;
	const float setconstval2 = 0;
	const float setconstval3[1] = {9};
	const float setconstval4[1] = {9};

	v3.append(appendconstbuf1, 1);
	v3.set(0, setval1);
	v3.set(1, setconstval1);
	v3.set(2, 1, setconstval3);

	v4.append(appendconstbuf2, 1);
	v4.set(0, setval2);
	v4.set(1, setconstval2);
	v4.set(2, 1, setconstval4);

	VectorSP ddbv3 = v3.createVector(DT_FLOAT);
	VectorSP ddbv4 = v4.createVector(DT_FLOAT);
	// cout<<ddbv3->getString()<<endl;
	// cout<<ddbv4->getString()<<endl;
	conn.upload("ddbv3", ddbv3);
	conn.upload("ddbv4", ddbv4);
	conn.run("a=float[8,0,9,4,5,6,9]");
	EXPECT_TRUE(conn.run("eqObj(ddbv3,a)")->getBool());
	EXPECT_TRUE(conn.run("eqObj(ddbv4,a)")->getBool());
}

TEST_F(DataformVectorTest, testCreateDoubleVectorByDdbVector)
{
	vector<double> vals = {1, 2, 3, 4, 5, 6};
	int size = vals.size();
	double *val = new double[size * 2 + 2];
	for (int i = 0; i < size; i++)
	{
		val[i] = vals[i];
	}
	DdbVector<double> v1(val, size, size * 2 + 2);
	DdbVector<double> v2(0, size + 10);
	for (auto i = 0; i < size; i++)
	{
		v2.add(vals[i]);
	}
	v1.addNull();
	v2.addNull();
	v1.setNull(0);
	v2.setNull(0);

	const double str1 = 7;
	v1.add(str1);
	v2.add(str1);

	VectorSP ddbv1 = v1.createVector(DT_DOUBLE);
	VectorSP ddbv2 = v2.createVector(DT_DOUBLE);
	vector<string> names = {"ddbv1", "ddbv2"};
	vector<ConstantSP> objs = {ddbv1, ddbv2};
	conn.upload(names, objs);
	conn.run("a=double[NULL,2,3,4,5,6,NULL,7];");
	EXPECT_TRUE(conn.run("eqObj(ddbv1,a)")->getBool());
	EXPECT_TRUE(conn.run("eqObj(ddbv2,a)")->getBool());

	EXPECT_EQ(v1.size(), 8);
	EXPECT_EQ(v2.size(), 8);

	double *val_new = new double[size * 2 + 2];
	for (int i = 0; i < size; i++)
	{
		val_new[i] = vals[i];
	}
	DdbVector<double> v3(val_new, size, size * 2 + 2);
	DdbVector<double> v4(0, size * 2 + 2);
	for (int i = 0; i < size; i++)
	{
		v4.add(vals[i]);
	}
	double setval1 = 8;
	double setval2 = 8;
	const double appendconstbuf1[1] = {9};
	const double appendconstbuf2[1] = {9};
	const double setconstval1 = 0;
	const double setconstval2 = 0;
	const double setconstval3[1] = {9};
	const double setconstval4[1] = {9};

	v3.append(appendconstbuf1, 1);
	v3.set(0, setval1);
	v3.set(1, setconstval1);
	v3.set(2, 1, setconstval3);

	v4.append(appendconstbuf2, 1);
	v4.set(0, setval2);
	v4.set(1, setconstval2);
	v4.set(2, 1, setconstval4);

	VectorSP ddbv3 = v3.createVector(DT_DOUBLE);
	VectorSP ddbv4 = v4.createVector(DT_DOUBLE);
	// cout<<ddbv3->getString()<<endl;
	// cout<<ddbv4->getString()<<endl;
	conn.upload("ddbv3", ddbv3);
	conn.upload("ddbv4", ddbv4);
	conn.run("a=double[8,0,9,4,5,6,9]");
	EXPECT_TRUE(conn.run("eqObj(ddbv3,a)")->getBool());
	EXPECT_TRUE(conn.run("eqObj(ddbv4,a)")->getBool());
}

TEST_F(DataformVectorTest, testCreateInt128VectorByDdbVector)
{

	vector<Guid> vals;
	for (auto i = 0; i < 6; i++)
	{
		unsigned char int128[16];
		for (auto i = 0; i < 16; i++)
		{
			int128[i] = 100;
		}
		vals.emplace_back(int128);
	}
	int size = vals.size();
	Guid *val = new Guid[size * 2 + 2];
	for (int i = 0; i < size; i++)
	{
		val[i] = vals[i];
	}
	DdbVector<Guid> v1(val, size, size * 2 + 2);
	DdbVector<Guid> v2(0, size + 10);
	for (auto i = 0; i < size; i++)
	{
		v2.add(vals[i]);
	}
	v1.addNull();
	v2.addNull();
	v1.setNull(0);
	v2.setNull(0);

	unsigned char int128_1[16];
	for (auto i = 0; i < 16; i++)
	{
		int128_1[i] = 200;
	}
	const Guid str1 = int128_1;
	v1.add(str1);
	v2.add(str1);

	VectorSP ddbv1 = v1.createVector(DT_INT128);
	VectorSP ddbv2 = v2.createVector(DT_INT128);
	// cout<<ddbv1->getString()<<endl;
	// cout<<ddbv2->getString()<<endl;

	vector<string> names = {"ddbv1", "ddbv2"};
	vector<ConstantSP> objs = {ddbv1, ddbv2};
	conn.upload(names, objs);
	conn.run("a=int128[,`64646464646464646464646464646464,`64646464646464646464646464646464,\
						`64646464646464646464646464646464,`64646464646464646464646464646464,\
						`64646464646464646464646464646464,,`c8c8c8c8c8c8c8c8c8c8c8c8c8c8c8c8];");
	EXPECT_TRUE(conn.run("eqObj(ddbv1,a)")->getBool());
	EXPECT_TRUE(conn.run("eqObj(ddbv2,a)")->getBool());

	EXPECT_EQ(v1.size(), 8);
	EXPECT_EQ(v2.size(), 8);

	Guid *val_new = new Guid[size * 2 + 2];
	for (int i = 0; i < size; i++)
	{
		val_new[i] = vals[i];
	}
	DdbVector<Guid> v3(val_new, size, size * 2 + 2);
	DdbVector<Guid> v4(0, size * 2 + 2);
	for (int i = 0; i < size; i++)
	{
		v4.add(vals[i]);
	}
	Guid setval1 = vals[4];
	Guid setval2 = vals[4];
	const Guid appendconstbuf1[1] = {vals[3]};
	const Guid appendconstbuf2[1] = {vals[3]};
	const Guid setconstval1 = vals[2];
	const Guid setconstval2 = vals[2];
	const Guid setconstval3[1] = {vals[1]};
	const Guid setconstval4[1] = {vals[1]};

	v3.append(appendconstbuf1, 1);
	v3.set(0, setval1);
	v3.set(1, setconstval1);
	v3.set(2, 1, setconstval3);

	v4.append(appendconstbuf2, 1);
	v4.set(0, setval2);
	v4.set(1, setconstval2);
	v4.set(2, 1, setconstval4);

	VectorSP ddbv3 = v3.createVector(DT_INT128);
	VectorSP ddbv4 = v4.createVector(DT_INT128);
	// cout<<ddbv3->getString()<<endl;
	// cout<<ddbv4->getString()<<endl;
	conn.upload("ddbv3", ddbv3);
	conn.upload("ddbv4", ddbv4);
	conn.run("a=int128[`64646464646464646464646464646464,`64646464646464646464646464646464,`64646464646464646464646464646464,\
	`64646464646464646464646464646464,`64646464646464646464646464646464,`64646464646464646464646464646464,`64646464646464646464646464646464]");
	EXPECT_TRUE(conn.run("eqObj(ddbv3,a)")->getBool());
	EXPECT_TRUE(conn.run("eqObj(ddbv4,a)")->getBool());
}

TEST_F(DataformVectorTest, testCreateUuidVectorByDdbVector)
{

	vector<Guid> vals;
	for (auto i = 0; i < 6; i++)
	{
		unsigned char int128[16];
		for (auto i = 0; i < 16; i++)
		{
			int128[i] = 100;
		}
		vals.emplace_back(int128);
	}
	int size = vals.size();
	Guid *val = new Guid[size * 2 + 2];
	for (int i = 0; i < size; i++)
	{
		val[i] = vals[i];
	}
	DdbVector<Guid> v1(val, size, size * 2 + 2);
	DdbVector<Guid> v2(0, size + 10);
	for (auto i = 0; i < size; i++)
	{
		v2.add(vals[i]);
	}
	v1.addNull();
	v2.addNull();
	v1.setNull(0);
	v2.setNull(0);

	unsigned char int128_1[16];
	for (auto i = 0; i < 16; i++)
	{
		int128_1[i] = 200;
	}
	const Guid str1 = int128_1;
	v1.add(str1);
	v2.add(str1);

	VectorSP ddbv1 = v1.createVector(DT_UUID);
	VectorSP ddbv2 = v2.createVector(DT_UUID);
	// cout<<ddbv1->getString()<<endl;
	// cout<<ddbv2->getString()<<endl;

	vector<string> names = {"ddbv1", "ddbv2"};
	vector<ConstantSP> objs = {ddbv1, ddbv2};
	conn.upload(names, objs);
	conn.run("a=uuid[,\"64646464-6464-6464-6464-646464646464\",\"64646464-6464-6464-6464-646464646464\",\
	\"64646464-6464-6464-6464-646464646464\",\"64646464-6464-6464-6464-646464646464\",\
	\"64646464-6464-6464-6464-646464646464\",,\"c8c8c8c8-c8c8-c8c8-c8c8-c8c8c8c8c8c8\"]");
	EXPECT_TRUE(conn.run("eqObj(ddbv1,a)")->getBool());
	EXPECT_TRUE(conn.run("eqObj(ddbv2,a)")->getBool());

	EXPECT_EQ(v1.size(), 8);
	EXPECT_EQ(v2.size(), 8);

	Guid *val_new = new Guid[size * 2 + 2];
	for (int i = 0; i < size; i++)
	{
		val_new[i] = vals[i];
	}
	DdbVector<Guid> v3(val_new, size, size * 2 + 2);
	DdbVector<Guid> v4(0, size * 2 + 2);
	for (int i = 0; i < size; i++)
	{
		v4.add(vals[i]);
	}
	Guid setval1 = vals[4];
	Guid setval2 = vals[4];
	const Guid appendconstbuf1[1] = {vals[3]};
	const Guid appendconstbuf2[1] = {vals[3]};
	const Guid setconstval1 = vals[2];
	const Guid setconstval2 = vals[2];
	const Guid setconstval3[1] = {vals[1]};
	const Guid setconstval4[1] = {vals[1]};

	v3.append(appendconstbuf1, 1);
	v3.set(0, setval1);
	v3.set(1, setconstval1);
	v3.set(2, 1, setconstval3);

	v4.append(appendconstbuf2, 1);
	v4.set(0, setval2);
	v4.set(1, setconstval2);
	v4.set(2, 1, setconstval4);

	VectorSP ddbv3 = v3.createVector(DT_UUID);
	VectorSP ddbv4 = v4.createVector(DT_UUID);
	// cout<<ddbv3->getString()<<endl;
	// cout<<ddbv4->getString()<<endl;
	conn.upload("ddbv3", ddbv3);
	conn.upload("ddbv4", ddbv4);
	conn.run("a=uuid[\"64646464-6464-6464-6464-646464646464\",\"64646464-6464-6464-6464-646464646464\",\
	\"64646464-6464-6464-6464-646464646464\",\"64646464-6464-6464-6464-646464646464\",\"64646464-6464-6464-6464-646464646464\",\
	\"64646464-6464-6464-6464-646464646464\",\"64646464-6464-6464-6464-646464646464\"]");
	EXPECT_TRUE(conn.run("eqObj(ddbv3,a)")->getBool());
	EXPECT_TRUE(conn.run("eqObj(ddbv4,a)")->getBool());
}

TEST_F(DataformVectorTest, testCreateIpaddrVectorByDdbVector)
{
	vector<Guid> vals;
	for (auto i = 0; i < 6; i++)
	{
		unsigned char int128[16];
		for (auto i = 0; i < 16; i++)
		{
			int128[i] = 100;
		}
		vals.emplace_back(int128);
	}
	int size = vals.size();
	Guid *val = new Guid[size * 2 + 2];
	for (int i = 0; i < size; i++)
	{
		val[i] = vals[i];
	}
	DdbVector<Guid> v1(val, size, size * 2 + 2);
	DdbVector<Guid> v2(0, size + 10);
	for (auto i = 0; i < size; i++)
	{
		v2.add(vals[i]);
	}
	v1.addNull();
	v2.addNull();
	v1.setNull(0);
	v2.setNull(0);

	unsigned char int128_1[16];
	for (auto i = 0; i < 16; i++)
	{
		int128_1[i] = 200;
	}
	const Guid str1 = int128_1;
	v1.add(str1);
	v2.add(str1);

	VectorSP ddbv1 = v1.createVector(DT_IP);
	VectorSP ddbv2 = v2.createVector(DT_IP);
	// cout<<ddbv1->getString()<<endl;
	// cout<<ddbv2->getString()<<endl;

	vector<string> names = {"ddbv1", "ddbv2"};
	vector<ConstantSP> objs = {ddbv1, ddbv2};
	conn.upload(names, objs);
	conn.run("a=ipaddr[,\"6464:6464:6464:6464:6464:6464:6464:6464\",\"6464:6464:6464:6464:6464:6464:6464:6464\",\"6464:6464:6464:6464:6464:6464:6464:6464\",\
	\"6464:6464:6464:6464:6464:6464:6464:6464\",\"6464:6464:6464:6464:6464:6464:6464:6464\",,\"c8c8:c8c8:c8c8:c8c8:c8c8:c8c8:c8c8:c8c8\"]");
	EXPECT_TRUE(conn.run("eqObj(ddbv1,a)")->getBool());
	EXPECT_TRUE(conn.run("eqObj(ddbv2,a)")->getBool());

	EXPECT_EQ(v1.size(), 8);
	EXPECT_EQ(v2.size(), 8);

	Guid *val_new = new Guid[size * 2 + 2];
	for (int i = 0; i < size; i++)
	{
		val_new[i] = vals[i];
	}
	DdbVector<Guid> v3(val_new, size, size * 2 + 2);
	DdbVector<Guid> v4(0, size * 2 + 2);
	for (int i = 0; i < size; i++)
	{
		v4.add(vals[i]);
	}
	Guid setval1 = vals[4];
	Guid setval2 = vals[4];
	const Guid appendconstbuf1[1] = {vals[3]};
	const Guid appendconstbuf2[1] = {vals[3]};
	const Guid setconstval1 = vals[2];
	const Guid setconstval2 = vals[2];
	const Guid setconstval3[1] = {vals[1]};
	const Guid setconstval4[1] = {vals[1]};

	v3.append(appendconstbuf1, 1);
	v3.set(0, setval1);
	v3.set(1, setconstval1);
	v3.set(2, 1, setconstval3);

	v4.append(appendconstbuf2, 1);
	v4.set(0, setval2);
	v4.set(1, setconstval2);
	v4.set(2, 1, setconstval4);

	VectorSP ddbv3 = v3.createVector(DT_IP);
	VectorSP ddbv4 = v4.createVector(DT_IP);
	// cout<<ddbv3->getString()<<endl;
	// cout<<ddbv4->getString()<<endl;
	conn.upload("ddbv3", ddbv3);
	conn.upload("ddbv4", ddbv4);
	conn.run("a=ipaddr[\"6464:6464:6464:6464:6464:6464:6464:6464\",\"6464:6464:6464:6464:6464:6464:6464:6464\",\"6464:6464:6464:6464:6464:6464:6464:6464\",\
	\"6464:6464:6464:6464:6464:6464:6464:6464\",\"6464:6464:6464:6464:6464:6464:6464:6464\",\"6464:6464:6464:6464:6464:6464:6464:6464\",\"6464:6464:6464:6464:6464:6464:6464:6464\"]");
	EXPECT_TRUE(conn.run("eqObj(ddbv3,a)")->getBool());
	EXPECT_TRUE(conn.run("eqObj(ddbv4,a)")->getBool());
}

TEST_F(DataformVectorTest, testStringVectorMoreThan65535)
{
	VectorSP v1 = Util::createVector(DT_STRING, 70000, 70000);
	for (int i = 0; i < 70000; i++)
	{
		v1->set(i, Util::createString(to_string(i)));
	}
	string script = "z=array(STRING,0);for (i in 0..69999){z.append!(string(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testAnyVectorMoreThan65535)
{
	VectorSP v1 = Util::createVector(DT_ANY, 70000, 70000);
	for (int i = 0; i < 70000; i++)
	{
		v1->set(i, Util::createString(to_string(i)));
	}
	string script = "z=array(ANY,0);for (i in 0..69999){z.append!(string(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testCharVectorEqule128)
{
	VectorSP v1 = Util::createVector(DT_CHAR, 128, 128);
	for (int i = 0; i < 128; i++)
	{
		v1->set(i, Util::createChar(i));
	}
	string script = "z=array(CHAR,0);for (i in 0..127){z.append!(char(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testIntVectorMoreThan65535)
{
	VectorSP v1 = Util::createVector(DT_INT, 70000, 70000);
	for (int i = 0; i < 70000; i++)
	{
		v1->set(i, Util::createInt(i));
	}
	string script = "z=array(INT,0);for (i in 0..69999){z.append!(int(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testLongVectorMoreThan65535)
{
	VectorSP v1 = Util::createVector(DT_LONG, 70000, 70000);
	for (int i = 0; i < 70000; i++)
	{
		v1->set(i, Util::createLong(i));
	}
	string script = "z=array(LONG,0);for (i in 0..69999){z.append!(long(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testShortVectorEqual256)
{
	VectorSP v1 = Util::createVector(DT_SHORT, 256, 256);
	for (int i = 0; i < 256; i++)
	{
		v1->set(i, Util::createShort(i));
	}
	string script = "z=array(SHORT,0);for (i in 0..255){z.append!(short(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testFloatVectorMoreThan65535)
{
	VectorSP v1 = Util::createVector(DT_FLOAT, 70000, 70000);
	for (int i = 0; i < 70000; i++)
	{
		v1->set(i, Util::createFloat(i));
	}
	string script = "z=array(FLOAT,0);for (i in 0..69999){z.append!(float(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testDoubleVectorMoreThan65535)
{
	VectorSP v1 = Util::createVector(DT_DOUBLE, 70000, 70000);
	for (int i = 0; i < 70000; i++)
	{
		v1->set(i, Util::createDouble(i));
	}
	string script = "z=array(DOUBLE,0);for (i in 0..69999){z.append!(double(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testDatehourVectorMoreThan65535)
{
	VectorSP v1 = Util::createVector(DT_DATEHOUR, 70000, 70000);
	for (int i = 0; i < 70000; i++)
	{
		v1->set(i, Util::createDateHour(i));
	}
	string script = "z=array(DATEHOUR,0);for (i in 0..69999){z.append!(datehour(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testDateVectorMoreThan65535)
{
	VectorSP v1 = Util::createVector(DT_DATE, 70000, 70000);
	for (int i = 0; i < 70000; i++)
	{
		v1->set(i, Util::createDate(i));
	}
	string script = "z=array(DATE,0);for (i in 0..69999){z.append!(date(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testMinuteVectorMoreThan1024)
{
	VectorSP v1 = Util::createVector(DT_MINUTE, 1440, 1440);
	for (int i = 0; i < 1440; i++)
	{
		v1->set(i, Util::createMinute(i));
	}
	string script = "z=array(MINUTE,0);for (i in 0..1439){z.append!(minute(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testDatetimeVectorMoreThan65535)
{
	VectorSP v1 = Util::createVector(DT_DATETIME, 70000, 70000);
	for (int i = 0; i < 70000; i++)
	{
		v1->set(i, Util::createDateTime(i));
	}
	string script = "z=array(DATETIME,0);for (i in 0..69999){z.append!(datetime(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testTimeStampVectorMoreThan65535)
{
	VectorSP v1 = Util::createVector(DT_TIMESTAMP, 70000, 70000);
	for (int i = 0; i < 70000; i++)
	{
		v1->set(i, Util::createTimestamp(i));
	}
	string script = "z=array(TIMESTAMP,0);for (i in 0..69999){z.append!(timestamp(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testnanotimeVectorMoreThan65535)
{
	VectorSP v1 = Util::createVector(DT_NANOTIME, 70000, 70000);
	for (int i = 0; i < 70000; i++)
	{
		v1->set(i, Util::createNanoTime(i));
	}
	string script = "z=array(NANOTIME,0);for (i in 0..69999){z.append!(nanotime(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testnanotimestampVectorMoreThan65535)
{
	VectorSP v1 = Util::createVector(DT_NANOTIMESTAMP, 70000, 70000);
	for (int i = 0; i < 70000; i++)
	{
		v1->set(i, Util::createNanoTimestamp(i));
	}
	string script = "z=array(NANOTIMESTAMP,0);for (i in 0..69999){z.append!(nanotimestamp(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testmonthVectorMoreThan65535)
{
	VectorSP v1 = Util::createVector(DT_MONTH, 70000, 70000);
	for (int i = 0; i < 70000; i++)
	{
		v1->set(i, Util::createMonth(i));
	}
	string script = "z=array(MONTH,0);for (i in 0..69999){z.append!(month(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testtimeVectorMoreThan65535)
{
	VectorSP v1 = Util::createVector(DT_TIME, 70000, 70000);
	for (int i = 0; i < 70000; i++)
	{
		v1->set(i, Util::createTime(i));
	}
	string script = "z=array(TIME,0);for (i in 0..69999){z.append!(time(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testStringVectorMoreThan1048576)
{
	VectorSP v1 = Util::createVector(DT_STRING, 1100000, 1100000);
	for (int i = 0; i < 1100000; i++)
	{
		v1->set(i, Util::createString(to_string(i)));
	}
	string script = "z=array(STRING,0);for (i in 0..1099999){z.append!(string(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testAnyVectorMoreThan1048576)
{
	VectorSP v1 = Util::createVector(DT_ANY, 1100000, 1100000);
	for (int i = 0; i < 1100000; i++)
	{
		v1->set(i, Util::createString(to_string(i)));
	}
	string script = "z=array(ANY,0);for (i in 0..1099999){z.append!(string(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(res_d->getType(), DT_ANY);
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testIntVectorMoreThan1048576)
{
	VectorSP v1 = Util::createVector(DT_INT, 1100000, 1100000);
	for (int i = 0; i < 1100000; i++)
	{
		v1->set(i, Util::createInt(i));
	}
	string script = "z=array(INT,0);for (i in 0..1099999){z.append!(int(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testLongVectorMoreThan1048576)
{
	VectorSP v1 = Util::createVector(DT_LONG, 1100000, 1100000);
	for (int i = 0; i < 1100000; i++)
	{
		v1->set(i, Util::createLong(i));
	}
	string script = "z=array(LONG,0);for (i in 0..1099999){z.append!(long(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testFloatVectorMoreThan1048576)
{
	VectorSP v1 = Util::createVector(DT_FLOAT, 1100000, 1100000);
	for (int i = 0; i < 1100000; i++)
	{
		v1->set(i, Util::createFloat(i));
	}
	string script = "z=array(FLOAT,0);for (i in 0..1099999){z.append!(float(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testDoubleVectorMoreThan1048576)
{
	VectorSP v1 = Util::createVector(DT_DOUBLE, 1100000, 1100000);
	for (int i = 0; i < 1100000; i++)
	{
		v1->set(i, Util::createDouble(i));
	}
	string script = "z=array(DOUBLE,0);for (i in 0..1099999){z.append!(double(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testDatehourVectorMoreThan1048576)
{
	VectorSP v1 = Util::createVector(DT_DATEHOUR, 1100000, 1100000);
	for (int i = 0; i < 1100000; i++)
	{
		v1->set(i, Util::createDateHour(i));
	}
	string script = "z=array(DATEHOUR,0);for (i in 0..1099999){z.append!(datehour(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testDateVectorMoreThan1048576)
{
	VectorSP v1 = Util::createVector(DT_DATE, 1100000, 1100000);
	for (int i = 0; i < 1100000; i++)
	{
		v1->set(i, Util::createDate(i));
	}
	string script = "z=array(DATE,0);for (i in 0..1099999){z.append!(date(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testDatetimeVectorMoreThan1048576)
{
	VectorSP v1 = Util::createVector(DT_DATETIME, 1100000, 1100000);
	for (int i = 0; i < 1100000; i++)
	{
		v1->set(i, Util::createDateTime(i));
	}
	string script = "z=array(DATETIME,0);for (i in 0..1099999){z.append!(datetime(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testTimeStampVectorMoreThan1048576)
{
	VectorSP v1 = Util::createVector(DT_TIMESTAMP, 1100000, 1100000);
	for (int i = 0; i < 1100000; i++)
	{
		v1->set(i, Util::createTimestamp(i));
	}
	string script = "z=array(TIMESTAMP,0);for (i in 0..1099999){z.append!(timestamp(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testnanotimeVectorMoreThan1048576)
{
	VectorSP v1 = Util::createVector(DT_NANOTIME, 1100000, 1100000);
	for (int i = 0; i < 1100000; i++)
	{
		v1->set(i, Util::createNanoTime(i));
	}
	string script = "z=array(NANOTIME,0);for (i in 0..1099999){z.append!(nanotime(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testnanotimestampVectorMoreThan1048576)
{
	VectorSP v1 = Util::createVector(DT_NANOTIMESTAMP, 1100000, 1100000);
	for (int i = 0; i < 1100000; i++)
	{
		v1->set(i, Util::createNanoTimestamp(i));
	}
	string script = "z=array(NANOTIMESTAMP,0);for (i in 0..1099999){z.append!(nanotimestamp(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testmonthVectorMoreThan1048576)
{
	VectorSP v1 = Util::createVector(DT_MONTH, 1100000, 1100000);
	for (int i = 0; i < 1100000; i++)
	{
		v1->set(i, Util::createMonth(i));
	}
	string script = "z=array(MONTH,0);for (i in 0..1099999){z.append!(month(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testtimeVectorMoreThan1048576)
{
	VectorSP v1 = Util::createVector(DT_TIME, 1100000, 1100000);
	for (int i = 0; i < 1100000; i++)
	{
		v1->set(i, Util::createTime(i));
	}
	string script = "z=array(TIME,0);for (i in 0..1099999){z.append!(time(i))};z";
	VectorSP res_d = conn.run(script);
	conn.upload("v1", {v1});
	EXPECT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
	string judgestr = "eqObj(z,v1)";
	EXPECT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testenumVector)
{
	TableSP table;
	{ // create a table with 70000 rows.
		int size = 70000;
		DdbVector<double> doubleV(0, size);
		DdbVector<float> floatV(0, size);
		DdbVector<int> intV(0, size);
		DdbVector<short> shortV(0, size);
		DdbVector<long long> longV(0, size);
		DdbVector<char> boolV(0, size);
		DdbVector<char> charV(0, size);
		DdbVector<string> strV(0, size);
		unsigned char int128[16];
		DdbVector<Guid> int128V(0, size);
		unsigned char uuid[16];
		DdbVector<Guid> uuidV(0, size);
		unsigned char ip[16];
		DdbVector<Guid> ipV(0, size);
		DdbVector<string> blobV(0, size);
		DdbVector<int> dateV(0, size);
		DdbVector<int> minuteV(0, size);
		DdbVector<int> datetimeV(0, size);
		DdbVector<long long> nanotimeV(0, size);
		DdbVector<int> datehourV(0, size);
		DdbVector<int32_t> decimal32V(0, size);
		DdbVector<int64_t> decimal64V(0, size);
		DdbVector<string> symV(0, size);
		for (auto i = 0; i < size - 1; i++)
		{
			doubleV.add(i);
			floatV.add(i);
			intV.add(i);
			shortV.add(i);
			longV.add(i);
			boolV.add(i % 1);
			charV.add(i % CHAR_MAX);
			strV.add("str" + to_string(i));
			int128[i % 16] = i % CHAR_MAX;
			int128V.add(int128);
			uuid[i % 16] = i % CHAR_MAX;
			uuidV.add(uuid);
			ip[i % 16] = i % CHAR_MAX;
			ipV.add(ip);
			blobV.add("blob" + to_string(i));
			dateV.add(i);
			minuteV.add(i);
			datetimeV.add(i);
			nanotimeV.add((long long)i);
			datehourV.add(i);
			decimal32V.add(i / 100.00);
			decimal64V.add(i / 100.0000);
			symV.add("sym" + to_string(i));
		}
		doubleV.addNull();
		floatV.addNull();
		intV.addNull();
		shortV.addNull();
		longV.addNull();
		boolV.addNull();
		charV.addNull();
		strV.addNull();
		int128V.addNull();
		uuidV.addNull();
		ipV.addNull();
		blobV.addNull();
		dateV.addNull();
		minuteV.addNull();
		datetimeV.addNull();
		nanotimeV.addNull();
		datehourV.addNull();
		decimal32V.addNull();
		decimal64V.addNull();
		symV.addNull();
		table = Util::createTable(
			{"double", "float", "int", "short", "long", "bool", "char", "str", "int128", "uuid", "ip", "blob", "date", "minute", "datetime", "nanotime", "datehour", "decimal32", "decimal64", "symbol"},
			{doubleV.createVector(DT_DOUBLE), floatV.createVector(DT_FLOAT), intV.createVector(DT_INT), shortV.createVector(DT_SHORT),
			 longV.createVector(DT_LONG), boolV.createVector(DT_BOOL), charV.createVector(DT_CHAR), strV.createVector(DT_STRING),
			 int128V.createVector(DT_INT128), uuidV.createVector(DT_UUID), ipV.createVector(DT_IP), blobV.createVector(DT_BLOB),
			 dateV.createVector(DT_DATE), minuteV.createVector(DT_MINUTE), datetimeV.createVector(DT_DATETIME), nanotimeV.createVector(DT_NANOTIME),
			 datehourV.createVector(DT_DATEHOUR), decimal32V.createVector(DT_DECIMAL32), decimal64V.createVector(DT_DECIMAL64), symV.createVector(DT_SYMBOL)});
		// cout << "table created:" << endl << table->getString() << endl;
		conn.upload("test_tab", {table});
	}
	TableSP res_tab = conn.run("test_tab");
	{ // create threads for all columns and save datas in vector
		vector<ThreadSP> threads(table->columns());
		VectorSP *cols = new VectorSP[table->columns()];
		int colIndex = 0;
		cols[colIndex++] = Util::createVector(DT_DOUBLE, 0, table->rows());
		cols[colIndex++] = Util::createVector(DT_FLOAT, 0, table->rows());
		cols[colIndex++] = Util::createVector(DT_INT, 0, table->rows());
		cols[colIndex++] = Util::createVector(DT_SHORT, 0, table->rows());
		cols[colIndex++] = Util::createVector(DT_LONG, 0, table->rows());
		cols[colIndex++] = Util::createVector(DT_BOOL, 0, table->rows());
		cols[colIndex++] = Util::createVector(DT_CHAR, 0, table->rows());
		cols[colIndex++] = Util::createVector(DT_STRING, 0, table->rows());
		cols[colIndex++] = Util::createVector(DT_INT128, 0, table->rows());
		cols[colIndex++] = Util::createVector(DT_UUID, 0, table->rows());
		cols[colIndex++] = Util::createVector(DT_IP, 0, table->rows());
		cols[colIndex++] = Util::createVector(DT_BLOB, 0, table->rows());
		cols[colIndex++] = Util::createVector(DT_DATE, 0, table->rows());
		cols[colIndex++] = Util::createVector(DT_MINUTE, 0, table->rows());
		cols[colIndex++] = Util::createVector(DT_DATETIME, 0, table->rows());
		cols[colIndex++] = Util::createVector(DT_NANOTIME, 0, table->rows());
		cols[colIndex++] = Util::createVector(DT_DATEHOUR, 0, table->rows());
		cols[colIndex++] = Util::createVector(DT_DECIMAL32, 0, table->rows());
		cols[colIndex++] = Util::createVector(DT_DECIMAL64, 0, table->rows());
		cols[colIndex++] = Util::createVector(DT_SYMBOL, 0, table->rows());
		for (auto colIndex = 0; colIndex < table->columns(); colIndex++)
		{
			VectorSP pvector = table->getColumn(colIndex);
			threads[colIndex] = new Thread(
				new Executor([=]
							 {
				switch (pvector->getType()) {
					case DT_DOUBLE:
						Util::enumDoubleVector(pvector, [&](const double *pbuf, INDEX startIndex, int length) {
							for (auto index = startIndex, i = 0; i < length; i++, index++) {
								cols[colIndex]->append(Util::createDouble(pbuf[i]));
							}
							return true;
						});
					break;
					case DT_FLOAT:
						Util::enumFloatVector(pvector, [&](const float *pbuf, INDEX startIndex, int length) {
							for (auto index = startIndex, i = 0; i < length; i++, index++) {
								cols[colIndex]->append(Util::createFloat(pbuf[i]));
							}
							return true;
						});
					break;
					case DT_INT:
						Util::enumIntVector(pvector, [&](const int *pbuf, INDEX startIndex, int length) {
							for (auto index = startIndex, i = 0; i < length; i++, index++) {
								cols[colIndex]->append(Util::createInt(pbuf[i]));
							}
							return true;
						});
					break;
					case DT_SHORT:
						Util::enumShortVector(pvector, [&](const short *pbuf, INDEX startIndex, int length) {
							for (auto index = startIndex, i = 0; i < length; i++, index++) {
								cols[colIndex]->append(Util::createShort(pbuf[i]));
							}
							return true;
						});
					break;
					case DT_LONG:
						Util::enumLongVector(pvector, [&](const long long *pbuf, INDEX startIndex, int length) {
							for (auto index = startIndex, i = 0; i < length; i++, index++) {
								cols[colIndex]->append(Util::createLong(pbuf[i]));
							}
							return true;
						});
					break;
					case DT_BOOL:
						Util::enumBoolVector(pvector, [&](const char *pbuf, INDEX startIndex, int length) {
							for (auto index = startIndex, i = 0; i < length; i++, index++) {
								cols[colIndex]->append(Util::createBool(pbuf[i]));
							}
							return true;
						});
					break;
					case DT_CHAR:
						Util::enumCharVector(pvector, [&](const char *pbuf, INDEX startIndex, int length) {
							for (auto index = startIndex, i = 0; i < length; i++, index++) {
								cols[colIndex]->append(Util::createChar(pbuf[i]));
							}
							return true;
						});
					break;
					case DT_STRING:
						Util::enumStringVector(pvector, [&](string **pbuf, INDEX startIndex, int length) {
							for (auto index = startIndex, i = 0; i < length; i++, index++) {
								cols[colIndex]->append(Util::createString(*pbuf[i]));
							}
							return true;
						});
					break;
					case DT_INT128:
					{
						Util::enumInt128Vector(pvector, [&](const Guid *pbuf, INDEX startIndex, int length) {
							for (auto index = startIndex, i = 0; i < length; i++, index++) {
								ConstantSP pconst = Util::createObject(DT_INT128, (const unsigned char*)(pbuf + i));
								cols[colIndex]->append(pconst);
							}
							return true;
						});
					break;
					}
					case DT_UUID:
					{
						Util::enumInt128Vector(pvector, [&](const Guid *pbuf, INDEX startIndex, int length) {
							for (auto index = startIndex, i = 0; i < length; i++, index++) {
								ConstantSP pconst = Util::createObject(DT_UUID, (const unsigned char*)(pbuf + i));
								cols[colIndex]->append(pconst);
							}
							return true;
						});
					break;
					}
					case DT_IP:
					{
						Util::enumInt128Vector(pvector, [&](const Guid *pbuf, INDEX startIndex, int length) {
							for (auto index = startIndex, i = 0; i < length; i++, index++) {
								ConstantSP pconst = Util::createObject(DT_IP, (const unsigned char*)(pbuf + i));
								cols[colIndex]->append(pconst);
							}
							return true;
						});
					break;
					}
					case DT_BLOB:
						Util::enumStringVector(pvector, [&](string **pbuf, INDEX startIndex, int length) {
							for (auto index = startIndex, i = 0; i < length; i++, index++) {
								cols[colIndex]->append(Util::createBlob(*pbuf[i]));
							}
							return true;
						});
					break;
					case DT_DATE:
						Util::enumIntVector(pvector, [&](const int *pbuf, INDEX startIndex, int length) {
							for (auto index = startIndex, i = 0; i < length; i++, index++) {
								cols[colIndex]->append(Util::createDate(pbuf[i]));
							}
							return true;
						});
					break;
					case DT_MINUTE:
						Util::enumIntVector(pvector, [&](const int *pbuf, INDEX startIndex, int length) {
							for (auto index = startIndex, i = 0; i < length; i++, index++) {
								cols[colIndex]->append(Util::createMinute(pbuf[i]));
							}
							return true;
						});
					break;
					case DT_DATETIME:
						Util::enumIntVector(pvector, [&](const int *pbuf, INDEX startIndex, int length) {
							for (auto index = startIndex, i = 0; i < length; i++, index++) {
								cols[colIndex]->append(Util::createDateTime(pbuf[i]));
							}
							return true;
						});
					break;
					case DT_NANOTIME:
						Util::enumLongVector(pvector, [&](const long long *pbuf, INDEX startIndex, int length) {
							for (auto index = startIndex, i = 0; i < length; i++, index++) {
								cols[colIndex]->append(Util::createNanoTime(pbuf[i]));
							}
							return true;
						});
					break;
					case DT_DATEHOUR:
						Util::enumIntVector(pvector, [&](const int *pbuf, INDEX startIndex, int length) {
							for (auto index = startIndex, i = 0; i < length; i++, index++) {
								cols[colIndex]->append(Util::createDateHour(pbuf[i]));
							}
							return true;
						});
					break;
					case DT_DECIMAL32:
						Util::enumDecimal32Vector(pvector, [&](const int32_t *pbuf, INDEX startIndex, int length) {
							for (auto index = startIndex, i = 0; i < length; i++, index++) {
								ConstantSP pconst = Util::createDecimal32(pvector->getExtraParamForType(),0);
								pconst->setBinary((const unsigned char*)&pbuf[i], sizeof(int32_t));
								cols[colIndex]->append(pconst);
							}
							return true;
						});
					break;
					case DT_DECIMAL64:
						Util::enumDecimal64Vector(pvector, [&](const int64_t *pbuf, INDEX startIndex, int length) {
							for (auto index = startIndex, i = 0; i < length; i++, index++) {
								ConstantSP pconst = Util::createDecimal64(pvector->getExtraParamForType(),0);
								pconst->setBinary((const unsigned char*)&pbuf[i], sizeof(int64_t));
								cols[colIndex]->append(pconst);
							}
							return true;
						});
					break;
					case DT_SYMBOL:
						Util::enumStringVector(pvector, [&](string **pbuf, INDEX startIndex, int length) {
							for (auto index = startIndex, i = 0; i < length; i++, index++) {
								cols[colIndex]->append(Util::createString(*pbuf[i]));
							}
							return true;
						});
					break;
				} }));
		}
		for (auto &one : threads)
		{
			one->start();
		}
		for (auto &one : threads)
		{
			one->join();
		}
		for (int i = 0; i < table->rows(); i++)
		{
			int colIndex = 0;
			ASSERT_EQ(cols[colIndex++]->get(i)->getDouble(), res_tab->getColumn(0)->get(i)->getDouble());
			ASSERT_EQ(cols[colIndex++]->get(i)->getFloat(), res_tab->getColumn(1)->get(i)->getFloat());
			ASSERT_EQ(cols[colIndex++]->get(i)->getInt(), res_tab->getColumn(2)->get(i)->getInt());
			ASSERT_EQ(cols[colIndex++]->get(i)->getShort(), res_tab->getColumn(3)->get(i)->getShort());
			ASSERT_EQ(cols[colIndex++]->get(i)->getLong(), res_tab->getColumn(4)->get(i)->getLong());
			ASSERT_EQ(cols[colIndex++]->get(i)->getBool(), res_tab->getColumn(5)->get(i)->getBool());
			ASSERT_EQ(cols[colIndex++]->get(i)->getChar(), res_tab->getColumn(6)->get(i)->getChar());
			ASSERT_EQ(cols[colIndex++]->get(i)->getString(), res_tab->getColumn(7)->get(i)->getString());
			ASSERT_EQ(cols[colIndex++]->get(i)->getInt128(), res_tab->getColumn(8)->get(i)->getInt128());
			ASSERT_EQ(cols[colIndex++]->get(i)->getUuid(), res_tab->getColumn(9)->get(i)->getUuid());
			ASSERT_EQ(cols[colIndex++]->get(i)->getString(), res_tab->getColumn(10)->get(i)->getString());
			ASSERT_EQ(cols[colIndex++]->get(i)->getString(), res_tab->getColumn(11)->get(i)->getString());
			ASSERT_EQ(cols[colIndex++]->get(i)->getString(), res_tab->getColumn(12)->get(i)->getString());
			ASSERT_EQ(cols[colIndex++]->get(i)->getString(), res_tab->getColumn(13)->get(i)->getString());
			ASSERT_EQ(cols[colIndex++]->get(i)->getString(), res_tab->getColumn(14)->get(i)->getString());
			ASSERT_EQ(cols[colIndex++]->get(i)->getString(), res_tab->getColumn(15)->get(i)->getString());
			ASSERT_EQ(cols[colIndex++]->get(i)->getString(), res_tab->getColumn(16)->get(i)->getString());
			ASSERT_EQ(cols[colIndex++]->get(i)->getString(), res_tab->getColumn(17)->get(i)->getString());
			ASSERT_EQ(cols[colIndex++]->get(i)->getString(), res_tab->getColumn(18)->get(i)->getString());
		}
		delete[] cols;
	}
}

TEST_F(DataformVectorTest, testVectorFunction)
{

	srand(time(NULL));
	int buckets = 2;
	char *buf0 = new char[Util::BUF_SIZE];
	char *buf00 = new char[Util::BUF_SIZE];
	char *buf = new char[Util::BUF_SIZE];
	short *buf1 = new short[Util::BUF_SIZE];
	int *buf2 = new int[Util::BUF_SIZE];
	INDEX *buf4 = new INDEX[Util::BUF_SIZE];
	long long *buf3 = new long long[Util::BUF_SIZE];
	float *buf5 = new float[Util::BUF_SIZE];
	double *buf6 = new double[Util::BUF_SIZE];
	string **buf8 = new string *[Util::BUF_SIZE];
	char **buf9 = new char *[Util::BUF_SIZE];
	unsigned char *buf10 = new unsigned char[Util::BUF_SIZE];
	SymbolBase *symbase = new SymbolBase(1);
	string exstr = "";
	int size = 3;

	cout << "-----------------string-------------------" << endl;
	ConstantSP stringval = Util::createString("this is a string scalar");
	VectorSP stringV = Util::createVector(DT_STRING, size);
	stringV->initialize();
	for (auto i = 0; i < size; i++)
		stringV->set(i, stringval);
	EXPECT_TRUE(stringV->isNull(0, size, buf0));
	EXPECT_FALSE(stringV->isValid(0, size, buf0));
	EXPECT_FALSE(stringV->getBool(0, size, buf00));
	EXPECT_FALSE(stringV->getChar(0, size, buf));
	EXPECT_FALSE(stringV->getShort(0, size, buf1));
	EXPECT_FALSE(stringV->getInt(0, size, buf2));
	EXPECT_FALSE(stringV->getLong(0, size, buf3));
	EXPECT_FALSE(stringV->getIndex(0, size, buf4));
	EXPECT_FALSE(stringV->getFloat(0, size, buf5));
	EXPECT_FALSE(stringV->getDouble(0, size, buf6));
	EXPECT_FALSE(stringV->getSymbol(0, size, buf2, symbase, false));
	EXPECT_FALSE(stringV->getBinary(0, size, size, buf10));
	EXPECT_TRUE(stringV->getHash(0, size, buckets, buf2));
	cout << stringV->getAllocatedMemory(size) << endl;
	EXPECT_ANY_THROW(stringV->getBoolConst(0, size, buf)[0]);
	EXPECT_ANY_THROW(stringV->getCharConst(0, size, buf)[0]);
	EXPECT_ANY_THROW(stringV->getShortConst(0, size, buf1)[0]);
	EXPECT_ANY_THROW(stringV->getIntConst(0, size, buf2)[0]);
	EXPECT_ANY_THROW(stringV->getLongConst(0, size, buf3)[0]);
	EXPECT_ANY_THROW(stringV->getIndexConst(0, size, buf4)[0]);
	EXPECT_ANY_THROW(stringV->getFloatConst(0, size, buf5)[0]);
	EXPECT_ANY_THROW(stringV->getDoubleConst(0, size, buf6)[0]);
	EXPECT_ANY_THROW(stringV->getSymbolConst(0, size, buf2, symbase, false));
	stringV->getStringConst(0, size, buf8);
	for (auto i = 0; i < size; i++)
		EXPECT_EQ(*(buf8[i]), stringval->getString());

	stringV->getStringConst(0, size, buf9);
	for (auto i = 0; i < size; i++)
		EXPECT_EQ(buf9[i], stringval->getString());

	EXPECT_ANY_THROW(stringV->getBinaryConst(0, size, sizeof(string), buf10));

	cout << "-----------------symbol-------------------" << endl;
	ConstantSP symval = Util::createString("symbol scalar");
	VectorSP symV = Util::createVector(DT_SYMBOL, size);
	symV->set(0, symval);
	symV->set(1, Util::createString("123"));
	EXPECT_TRUE(symV->isNull(0, size, buf0));
	EXPECT_TRUE(symV->isValid(0, size, buf0));
	EXPECT_TRUE(symV->getBool(0, size, buf00));
	symV->getChar(0, size, buf);
	symV->getShort(0, size, buf1);
	symV->getInt(0, size, buf2);
	symV->getLong(0, size, buf3);
	symV->getIndex(0, size, buf4);
	symV->getFloat(0, size, buf5);
	symV->getDouble(0, size, buf6);
	EXPECT_EQ(buf[0], 1);
	EXPECT_EQ(buf[1], 2);
	EXPECT_EQ(buf1[0], 1);
	EXPECT_EQ(buf1[1], 2);
	EXPECT_EQ(buf2[0], 1);
	EXPECT_EQ(buf2[1], 2);
	EXPECT_EQ(buf3[0], 1);
	EXPECT_EQ(buf3[1], 2);
	EXPECT_EQ(buf4[0], 1);
	EXPECT_EQ(buf4[1], 2);
	EXPECT_EQ(buf5[0], 1);
	EXPECT_EQ(buf5[1], 2);
	EXPECT_EQ(buf6[0], 1);
	EXPECT_EQ(buf6[1], 2);

	EXPECT_FALSE(symV->getSymbol(0, size, buf2, symbase, false));
	EXPECT_FALSE(symV->getBinary(0, size, size, buf10));
	EXPECT_TRUE(symV->getHash(0, size, buckets, buf2));
	EXPECT_EQ(symV->getAllocatedMemory(size), 0);
	EXPECT_EQ(symV->getBoolConst(0, size, buf)[0], 1);
	EXPECT_EQ(symV->getCharConst(0, size, buf)[0], 1);
	EXPECT_EQ(symV->getShortConst(0, size, buf1)[0], 1);
	EXPECT_EQ(symV->getIntConst(0, size, buf2)[0], 1);
	EXPECT_EQ(symV->getLongConst(0, size, buf3)[0], 1);
	EXPECT_EQ(symV->getIndexConst(0, size, buf4)[0], 1);
	EXPECT_EQ(symV->getFloatConst(0, size, buf5)[0], 1);
	EXPECT_EQ(symV->getDoubleConst(0, size, buf6)[0], 1);
	EXPECT_EQ(symV->getCharConst(0, size, buf)[1], 2);
	EXPECT_EQ(symV->getShortConst(0, size, buf1)[1], 2);
	EXPECT_EQ(symV->getIntConst(0, size, buf2)[1], 2);
	EXPECT_EQ(symV->getLongConst(0, size, buf3)[1], 2);
	EXPECT_EQ(symV->getIndexConst(0, size, buf4)[1], 2);
	EXPECT_EQ(symV->getFloatConst(0, size, buf5)[1], 2);
	EXPECT_EQ(symV->getDoubleConst(0, size, buf6)[1], 2);
	EXPECT_ANY_THROW(symV->getSymbolConst(0, size, buf2, symbase, false));
	symV->getStringConst(0, size, buf8);
	for (auto i = 0; i < size; i++)
		EXPECT_EQ(*(buf8[i]), symV->get(i)->getString());

	symV->getStringConst(0, size, buf9);
	for (auto i = 0; i < size; i++)
		EXPECT_EQ(buf9[i], symV->get(i)->getString());

	EXPECT_ANY_THROW(symV->getBinaryConst(0, size, sizeof(string), buf10));

	cout << "-----------------int-------------------" << endl;
	ConstantSP intval = Util::createInt(100000);
	VectorSP intV = Util::createVector(DT_INT, size);
	for (auto i = 0; i < size; i++)
		intV->set(i, intval);
	EXPECT_TRUE(intV->isNull(0, size, buf0));
	for (auto i = 0; i < size; i++)
		EXPECT_FALSE(buf0[i]);

	EXPECT_TRUE(intV->isValid(0, size, buf0));
	for (auto i = 0; i < size; i++)
		EXPECT_TRUE(buf0[i]);

	EXPECT_TRUE(intV->getBool(0, size, buf00));
	EXPECT_TRUE(intV->getChar(0, size, buf));
	EXPECT_TRUE(intV->getShort(0, size, buf1));
	EXPECT_TRUE(intV->getInt(0, size, buf2));
	EXPECT_TRUE(intV->getLong(0, size, buf3));
	EXPECT_TRUE(intV->getIndex(0, size, buf4));
	EXPECT_TRUE(intV->getFloat(0, size, buf5));
	EXPECT_TRUE(intV->getDouble(0, size, buf6));
	for (auto i = 0; i < size; i++)
	{
		EXPECT_EQ(buf00[i], true);
		EXPECT_EQ(buf[i], '\xA0');
		EXPECT_EQ(buf1[i], intV->get(i)->getShort());
		EXPECT_EQ(buf2[i], intV->get(i)->getInt());
		EXPECT_EQ(buf3[i], intV->get(i)->getLong());
		EXPECT_EQ(buf4[i], intV->get(i)->getIndex());
		EXPECT_EQ(buf5[i], intV->get(i)->getFloat());
		EXPECT_EQ(buf6[i], intV->get(i)->getDouble());
	}

	EXPECT_FALSE(intV->getSymbol(0, size, buf2, symbase, false));
	EXPECT_FALSE(intV->getBinary(0, size, sizeof(int), buf10));
	EXPECT_TRUE(intV->getHash(0, size, buckets, buf2));
	EXPECT_EQ(intV->getAllocatedMemory(size), 0);

	for (auto i = 0; i < size; i++)
	{
		EXPECT_EQ(intV->getBoolConst(0, size, buf)[i], 1);
		EXPECT_EQ(intV->getCharConst(0, size, buf)[i], '\xA0');
		// EXPECT_EQ(intV->getShortConst(0,size,buf1)[i], SHRT_MIN);
		EXPECT_EQ(intV->getIntConst(0, size, buf2)[i], 100000);
		EXPECT_EQ(intV->getLongConst(0, size, buf3)[i], 100000);
		EXPECT_EQ(intV->getIndexConst(0, size, buf4)[i], 100000);
		EXPECT_EQ(intV->getFloatConst(0, size, buf5)[i], 100000);
		EXPECT_EQ(intV->getDoubleConst(0, size, buf6)[i], 100000);
	}
	EXPECT_ANY_THROW(intV->getSymbolConst(0, size, buf2, symbase, false));
	EXPECT_ANY_THROW(intV->getStringConst(0, size, buf8));
	EXPECT_ANY_THROW(intV->getStringConst(0, 24, buf9));
	EXPECT_ANY_THROW(intV->getBinaryConst(0, size, sizeof(int), buf10));

	for (auto i = 0; i < size; i++)
	{
		EXPECT_EQ(intV->getBoolBuffer(0, size, buf)[i], '\xA0');
		EXPECT_EQ(intV->getCharBuffer(0, size, buf)[i], '\xA0');
		// EXPECT_EQ(intV->getShortBuffer(0,size,buf1)[0], SHRT_MIN);
		EXPECT_EQ(intV->getIntBuffer(0, size, buf2)[i], 100000);
		EXPECT_EQ(intV->getLongBuffer(0, size, buf3)[i], 100000);
		EXPECT_EQ(intV->getIndexBuffer(0, size, buf4)[i], 100000);
		EXPECT_EQ(intV->getFloatBuffer(0, size, buf5)[i], 100000);
		EXPECT_EQ(intV->getDoubleBuffer(0, size, buf6)[i], 100000);
	}

	cout << "-----------------char-------------------" << endl;
	char rand_val = rand() % CHAR_MAX + 1;
	ConstantSP charval = Util::createChar(rand_val);
	VectorSP charV = Util::createVector(DT_CHAR, size);
	for (auto i = 0; i < size; i++)
		charV->setChar(i, rand_val);

	EXPECT_TRUE(charV->isNull(0, size, buf0));
	for (auto i = 0; i < size; i++)
		EXPECT_FALSE(buf0[i]);

	EXPECT_TRUE(charV->isValid(0, size, buf0));
	for (auto i = 0; i < size; i++)
		EXPECT_TRUE(buf0[i]);

	EXPECT_TRUE(charV->getBool(0, size, buf00));
	EXPECT_TRUE(charV->getChar(0, size, buf));
	EXPECT_TRUE(charV->getShort(0, size, buf1));
	EXPECT_TRUE(charV->getInt(0, size, buf2));
	EXPECT_TRUE(charV->getLong(0, size, buf3));
	EXPECT_TRUE(charV->getIndex(0, size, buf4));
	EXPECT_TRUE(charV->getFloat(0, size, buf5));
	EXPECT_TRUE(charV->getDouble(0, size, buf6));
	for (auto i = 0; i < size; i++)
	{
		EXPECT_EQ(buf00[i], true);
		EXPECT_EQ(buf[i], rand_val);
		EXPECT_EQ(buf1[i], charV->get(i)->getShort());
		EXPECT_EQ(buf2[i], charV->get(i)->getInt());
		EXPECT_EQ(buf3[i], charV->get(i)->getLong());
		EXPECT_EQ(buf4[i], charV->get(i)->getIndex());
		EXPECT_EQ(buf5[i], charV->get(i)->getFloat());
		EXPECT_EQ(buf6[i], charV->get(i)->getDouble());
	}

	EXPECT_FALSE(charV->getSymbol(0, size, buf2, symbase, false));
	EXPECT_FALSE(charV->getBinary(0, size, size, buf10));
	EXPECT_TRUE(charV->getHash(0, size, buckets, buf2));
	EXPECT_EQ(charV->getAllocatedMemory(size), 0);

	for (auto i = 0; i < size; i++)
	{
		EXPECT_EQ(charV->getBoolConst(0, size, buf)[i], 1);
		EXPECT_EQ(charV->getCharConst(0, size, buf)[i], rand_val);
		EXPECT_EQ(charV->getShortConst(0, size, buf1)[i], (short)rand_val);
		EXPECT_EQ(charV->getIntConst(0, size, buf2)[i], (int)rand_val);
		EXPECT_EQ(charV->getLongConst(0, size, buf3)[i], (long long)rand_val);
		EXPECT_EQ(charV->getIndexConst(0, size, buf4)[i], (INDEX)rand_val);
		EXPECT_EQ(charV->getFloatConst(0, size, buf5)[i], (float)rand_val);
		EXPECT_EQ(charV->getDoubleConst(0, size, buf6)[i], (double)rand_val);
	}

	EXPECT_ANY_THROW(charV->getSymbolConst(0, size, buf2, symbase, false));
	EXPECT_ANY_THROW(charV->getStringConst(0, size, buf8)[0]);
	EXPECT_ANY_THROW(charV->getStringConst(0, 24, buf9));
	EXPECT_ANY_THROW(charV->getBinaryConst(0, size, sizeof(char), buf10));

	for (auto i = 0; i < size; i++)
	{
		EXPECT_EQ(charV->getBoolBuffer(0, size, buf)[i], (bool)rand_val);
		EXPECT_EQ(charV->getCharBuffer(0, size, buf)[i], rand_val);
		EXPECT_EQ(charV->getShortBuffer(0, size, buf1)[i], (short)rand_val);
		EXPECT_EQ(charV->getIntBuffer(0, size, buf2)[i], (int)rand_val);
		EXPECT_EQ(charV->getLongBuffer(0, size, buf3)[i], (long)rand_val);
		EXPECT_EQ(charV->getIndexBuffer(0, size, buf4)[i], (INDEX)rand_val);
		EXPECT_EQ(charV->getFloatBuffer(0, size, buf5)[i], (float)rand_val);
		EXPECT_EQ(charV->getDoubleBuffer(0, size, buf6)[i], (double)rand_val);
	}

	ConstantSP indexV = Util::createIndexVector(0, 5);
	EXPECT_TRUE(charV->remove(indexV));
	EXPECT_EQ(charV->size(), 0);

	cout << "-----------------short-------------------" << endl;
	short rand_val2 = rand() % SHRT_MAX + 1;
	ConstantSP shortval = Util::createShort(rand_val2);
	VectorSP shortV = Util::createVector(DT_SHORT, size);
	for (auto i = 0; i < size; i++)
		shortV->setShort(i, rand_val2);

	EXPECT_TRUE(shortV->isNull(0, size, buf0));
	for (auto i = 0; i < size; i++)
		EXPECT_FALSE(buf0[i]);

	EXPECT_TRUE(shortV->isValid(0, size, buf0));
	for (auto i = 0; i < size; i++)
		EXPECT_TRUE(buf0[i]);

	// EXPECT_TRUE(shortV->getBool(0,size,buf00));
	// EXPECT_TRUE(shortV->getChar(0,size,buf));
	EXPECT_TRUE(shortV->getShort(0, size, buf1));
	EXPECT_TRUE(shortV->getInt(0, size, buf2));
	EXPECT_TRUE(shortV->getLong(0, size, buf3));
	EXPECT_TRUE(shortV->getIndex(0, size, buf4));
	EXPECT_TRUE(shortV->getFloat(0, size, buf5));
	EXPECT_TRUE(shortV->getDouble(0, size, buf6));
	for (auto i = 0; i < size; i++)
	{
		// EXPECT_EQ(buf00[i], true);
		// EXPECT_EQ(buf[i], rand_val2);
		EXPECT_EQ(buf1[i], shortV->get(i)->getShort());
		EXPECT_EQ(buf2[i], shortV->get(i)->getInt());
		EXPECT_EQ(buf3[i], shortV->get(i)->getLong());
		EXPECT_EQ(buf4[i], shortV->get(i)->getIndex());
		EXPECT_EQ(buf5[i], shortV->get(i)->getFloat());
		EXPECT_EQ(buf6[i], shortV->get(i)->getDouble());
	}

	EXPECT_FALSE(shortV->getSymbol(0, size, buf2, symbase, false));
	EXPECT_FALSE(shortV->getBinary(0, size, size, buf10));
	EXPECT_TRUE(shortV->getHash(0, size, buckets, buf2));
	EXPECT_EQ(shortV->getAllocatedMemory(size), 0);

	for (auto i = 0; i < size; i++)
	{
		// EXPECT_EQ(shortV->getBoolConst(0,size,buf)[i], 1);
		// EXPECT_EQ(shortV->getCharConst(0,size,buf)[i], rand_val2);
		EXPECT_EQ(shortV->getShortConst(0, size, buf1)[i], (short)rand_val2);
		EXPECT_EQ(shortV->getIntConst(0, size, buf2)[i], (int)rand_val2);
		EXPECT_EQ(shortV->getLongConst(0, size, buf3)[i], (long long)rand_val2);
		EXPECT_EQ(shortV->getIndexConst(0, size, buf4)[i], (INDEX)rand_val2);
		EXPECT_EQ(shortV->getFloatConst(0, size, buf5)[i], (float)rand_val2);
		EXPECT_EQ(shortV->getDoubleConst(0, size, buf6)[i], (double)rand_val2);
	}

	EXPECT_ANY_THROW(shortV->getSymbolConst(0, size, buf2, symbase, false));
	EXPECT_ANY_THROW(shortV->getStringConst(0, size, buf8)[0]);
	EXPECT_ANY_THROW(shortV->getStringConst(0, 24, buf9));
	EXPECT_ANY_THROW(shortV->getBinaryConst(0, size, sizeof(char), buf10));

	for (auto i = 0; i < size; i++)
	{
		// EXPECT_EQ(shortV->getBoolBuffer(0,size,buf)[i], (bool)rand_val2);
		// EXPECT_EQ(shortV->getCharBuffer(0,size,buf)[i], rand_val2);
		EXPECT_EQ(shortV->getShortBuffer(0, size, buf1)[i], (short)rand_val2);
		EXPECT_EQ(shortV->getIntBuffer(0, size, buf2)[i], (int)rand_val2);
		EXPECT_EQ(shortV->getLongBuffer(0, size, buf3)[i], (long)rand_val2);
		EXPECT_EQ(shortV->getIndexBuffer(0, size, buf4)[i], (INDEX)rand_val2);
		EXPECT_EQ(shortV->getFloatBuffer(0, size, buf5)[i], (float)rand_val2);
		EXPECT_EQ(shortV->getDoubleBuffer(0, size, buf6)[i], (double)rand_val2);
	}

	cout << "-----------------long-------------------" << endl;
	long long rand_val3 = rand() % LLONG_MAX + 1;
	ConstantSP longval = Util::createLong(rand_val3);
	VectorSP longV = Util::createVector(DT_LONG, size);
	for (auto i = 0; i < size; i++)
		longV->setLong(i, rand_val3);

	EXPECT_TRUE(longV->isNull(0, size, buf0));
	for (auto i = 0; i < size; i++)
		EXPECT_FALSE(buf0[i]);

	EXPECT_TRUE(longV->isValid(0, size, buf0));
	for (auto i = 0; i < size; i++)
		EXPECT_TRUE(buf0[i]);

	// EXPECT_TRUE(longV->getBool(0,size,buf00));
	// EXPECT_TRUE(longV->getChar(0,size,buf));
	// EXPECT_TRUE(longV->getShort(0,size,buf1));
	EXPECT_TRUE(longV->getInt(0, size, buf2));
	EXPECT_TRUE(longV->getLong(0, size, buf3));
	EXPECT_TRUE(longV->getIndex(0, size, buf4));
	EXPECT_TRUE(longV->getFloat(0, size, buf5));
	EXPECT_TRUE(longV->getDouble(0, size, buf6));
	for (auto i = 0; i < size; i++)
	{
		// EXPECT_EQ(buf00[i], true);
		// EXPECT_EQ(buf[i], rand_val3);
		// EXPECT_EQ(buf1[i], longV->get(i)->getShort());
		EXPECT_EQ(buf2[i], longV->get(i)->getInt());
		EXPECT_EQ(buf3[i], longV->get(i)->getLong());
		EXPECT_EQ(buf4[i], longV->get(i)->getIndex());
		EXPECT_EQ(buf5[i], longV->get(i)->getFloat());
		EXPECT_EQ(buf6[i], longV->get(i)->getDouble());
	}

	EXPECT_FALSE(longV->getSymbol(0, size, buf2, symbase, false));
	EXPECT_FALSE(longV->getBinary(0, size, size, buf10));
	EXPECT_TRUE(longV->getHash(0, size, buckets, buf2));
	EXPECT_EQ(longV->getAllocatedMemory(size), 0);

	for (auto i = 0; i < size; i++)
	{
		// EXPECT_EQ(longV->getBoolConst(0,size,buf)[i], 1);
		// EXPECT_EQ(longV->getCharConst(0,size,buf)[i], rand_val3);
		// EXPECT_EQ(longV->getShortConst(0,size,buf1)[i], (short)rand_val3);
		EXPECT_EQ(longV->getIntConst(0, size, buf2)[i], (int)rand_val3);
		EXPECT_EQ(longV->getLongConst(0, size, buf3)[i], (long long)rand_val3);
		EXPECT_EQ(longV->getIndexConst(0, size, buf4)[i], (INDEX)rand_val3);
		EXPECT_EQ(longV->getFloatConst(0, size, buf5)[i], (float)rand_val3);
		EXPECT_EQ(longV->getDoubleConst(0, size, buf6)[i], (double)rand_val3);
	}

	EXPECT_ANY_THROW(longV->getSymbolConst(0, size, buf2, symbase, false));
	EXPECT_ANY_THROW(longV->getStringConst(0, size, buf8)[0]);
	EXPECT_ANY_THROW(longV->getStringConst(0, 24, buf9));
	EXPECT_ANY_THROW(longV->getBinaryConst(0, size, sizeof(char), buf10));

	for (auto i = 0; i < size; i++)
	{
		// EXPECT_EQ(longV->getBoolBuffer(0,size,buf)[i], (bool)rand_val3);
		// EXPECT_EQ(longV->getCharBuffer(0,size,buf)[i], rand_val3);
		// EXPECT_EQ(longV->getShortBuffer(0,size,buf1)[i], (short)rand_val3);
		EXPECT_EQ(longV->getIntBuffer(0, size, buf2)[i], (int)rand_val3);
		EXPECT_EQ(longV->getLongBuffer(0, size, buf3)[i], (long)rand_val3);
		EXPECT_EQ(longV->getIndexBuffer(0, size, buf4)[i], (INDEX)rand_val3);
		EXPECT_EQ(longV->getFloatBuffer(0, size, buf5)[i], (float)rand_val3);
		EXPECT_EQ(longV->getDoubleBuffer(0, size, buf6)[i], (double)rand_val3);
	}

	//     cout<<"-----------------float-------------------"<<endl;
	float rand_val4 = 1.5862 /*rand()/float(RAND_MAX)+1*/;
	VectorSP floatV = Util::createVector(DT_FLOAT, size);
	for (auto i = 0; i < size; i++)
		floatV->setFloat(i, rand_val4);
	EXPECT_TRUE(floatV->isNull(0, size, buf0));
	for (auto i = 0; i < size; i++)
		EXPECT_FALSE(buf0[i]);

	EXPECT_TRUE(floatV->isValid(0, size, buf0));
	for (auto i = 0; i < size; i++)
		EXPECT_TRUE(buf0[i]);

	// EXPECT_TRUE(floatV->getBool(0,size,buf00));
	// EXPECT_TRUE(floatV->getChar(0,size,buf));
	EXPECT_TRUE(floatV->getShort(0, size, buf1));
	EXPECT_TRUE(floatV->getInt(0, size, buf2));
	EXPECT_TRUE(floatV->getLong(0, size, buf3));
	EXPECT_TRUE(floatV->getIndex(0, size, buf4));
	EXPECT_TRUE(floatV->getFloat(0, size, buf5));
	EXPECT_TRUE(floatV->getDouble(0, size, buf6));
	for (auto i = 0; i < size; i++)
	{
		// EXPECT_EQ(buf00[i], true);
		// EXPECT_EQ(buf[i], rand_val4);
		EXPECT_EQ(buf1[i], floatV->get(i)->getShort());
		EXPECT_EQ(buf2[i], floatV->get(i)->getInt());
		EXPECT_EQ(buf3[i], floatV->get(i)->getLong());
		EXPECT_EQ(buf4[i], floatV->get(i)->getIndex());
		EXPECT_EQ(buf5[i], floatV->get(i)->getFloat());
		EXPECT_EQ(buf6[i], floatV->get(i)->getDouble());
	}

	EXPECT_FALSE(floatV->getSymbol(0, size, buf2, symbase, false));
	EXPECT_FALSE(floatV->getBinary(0, size, size, buf10));
	EXPECT_FALSE(floatV->getHash(0, size, buckets, buf2));
	EXPECT_EQ(floatV->getAllocatedMemory(size), 0);

	for (auto i = 0; i < size; i++)
	{
		// EXPECT_EQ(floatV->getBoolConst(0,size,buf)[i], 1);
		// EXPECT_EQ(floatV->getCharConst(0,size,buf)[i], rand_val4);
		EXPECT_EQ(floatV->getShortConst(0, size, buf1)[i], floatV->get(i)->getShort());
		EXPECT_EQ(floatV->getIntConst(0, size, buf2)[i], floatV->get(i)->getInt());
		EXPECT_EQ(floatV->getLongConst(0, size, buf3)[i], floatV->get(i)->getLong());
		EXPECT_EQ(floatV->getIndexConst(0, size, buf4)[i], floatV->get(i)->getIndex());
		EXPECT_EQ(floatV->getFloatConst(0, size, buf5)[i], floatV->get(i)->getFloat());
		EXPECT_EQ(floatV->getDoubleConst(0, size, buf6)[i], floatV->get(i)->getDouble());
	}

	EXPECT_ANY_THROW(floatV->getSymbolConst(0, size, buf2, symbase, false));
	EXPECT_ANY_THROW(floatV->getStringConst(0, size, buf8)[0]);
	EXPECT_ANY_THROW(floatV->getStringConst(0, 24, buf9));
	EXPECT_ANY_THROW(floatV->getBinaryConst(0, size, sizeof(char), buf10));

	for (auto i = 0; i < size; i++)
	{
		// EXPECT_EQ(floatV->getBoolBuffer(0,size,buf)[i], (bool)2);
		// EXPECT_EQ(floatV->getCharBuffer(0,size,buf)[i], 2);
		EXPECT_EQ(floatV->getShortBuffer(0, size, buf1)[i], floatV->get(i)->getShort());
		EXPECT_EQ(floatV->getIntBuffer(0, size, buf2)[i], floatV->get(i)->getInt());
		EXPECT_EQ(floatV->getLongBuffer(0, size, buf3)[i], floatV->get(i)->getLong());
		EXPECT_EQ(floatV->getIndexBuffer(0, size, buf4)[i], floatV->get(i)->getIndex());
		EXPECT_EQ(floatV->getFloatBuffer(0, size, buf5)[i], floatV->get(i)->getFloat());
		EXPECT_EQ(floatV->getDoubleBuffer(0, size, buf6)[i], floatV->get(i)->getDouble());
	}

	//    cout<<"-----------------double-------------------"<<endl;
	double rand_val5 = 2.1345 /*rand()/float(RAND_MAX)+1*/;
	VectorSP doubleV = Util::createVector(DT_DOUBLE, size);
	for (auto i = 0; i < size; i++)
		doubleV->setDouble(i, rand_val4);
	EXPECT_TRUE(doubleV->isNull(0, size, buf0));
	for (auto i = 0; i < size; i++)
		EXPECT_FALSE(buf0[i]);

	EXPECT_TRUE(doubleV->isValid(0, size, buf0));
	for (auto i = 0; i < size; i++)
		EXPECT_TRUE(buf0[i]);

	// EXPECT_TRUE(doubleV->getBool(0,size,buf00));
	// EXPECT_TRUE(doubleV->getChar(0,size,buf));
	EXPECT_TRUE(doubleV->getShort(0, size, buf1));
	EXPECT_TRUE(doubleV->getInt(0, size, buf2));
	EXPECT_TRUE(doubleV->getLong(0, size, buf3));
	EXPECT_TRUE(doubleV->getIndex(0, size, buf4));
	EXPECT_TRUE(doubleV->getFloat(0, size, buf5));
	EXPECT_TRUE(doubleV->getDouble(0, size, buf6));
	for (auto i = 0; i < size; i++)
	{
		// EXPECT_EQ(buf00[i], true);
		// EXPECT_EQ(buf[i], rand_val4);
		EXPECT_EQ(buf1[i], doubleV->get(i)->getShort());
		EXPECT_EQ(buf2[i], doubleV->get(i)->getInt());
		EXPECT_EQ(buf3[i], doubleV->get(i)->getLong());
		EXPECT_EQ(buf4[i], doubleV->get(i)->getIndex());
		EXPECT_EQ(buf5[i], doubleV->get(i)->getFloat());
		EXPECT_EQ(buf6[i], doubleV->get(i)->getDouble());
	}

	EXPECT_FALSE(doubleV->getSymbol(0, size, buf2, symbase, false));
	EXPECT_FALSE(doubleV->getBinary(0, size, size, buf10));
	EXPECT_FALSE(doubleV->getHash(0, size, buckets, buf2));
	EXPECT_EQ(doubleV->getAllocatedMemory(size), 0);

	for (auto i = 0; i < size; i++)
	{
		// EXPECT_EQ(doubleV->getBoolConst(0,size,buf)[i], 1);
		// EXPECT_EQ(doubleV->getCharConst(0,size,buf)[i], rand_val4);
		EXPECT_EQ(doubleV->getShortConst(0, size, buf1)[i], doubleV->get(i)->getShort());
		EXPECT_EQ(doubleV->getIntConst(0, size, buf2)[i], doubleV->get(i)->getInt());
		EXPECT_EQ(doubleV->getLongConst(0, size, buf3)[i], doubleV->get(i)->getLong());
		EXPECT_EQ(doubleV->getIndexConst(0, size, buf4)[i], doubleV->get(i)->getIndex());
		EXPECT_EQ(doubleV->getFloatConst(0, size, buf5)[i], doubleV->get(i)->getFloat());
		EXPECT_EQ(doubleV->getDoubleConst(0, size, buf6)[i], doubleV->get(i)->getDouble());
	}

	EXPECT_ANY_THROW(doubleV->getSymbolConst(0, size, buf2, symbase, false));
	EXPECT_ANY_THROW(doubleV->getStringConst(0, size, buf8)[0]);
	EXPECT_ANY_THROW(doubleV->getStringConst(0, 24, buf9));
	EXPECT_ANY_THROW(doubleV->getBinaryConst(0, size, sizeof(char), buf10));

	for (auto i = 0; i < size; i++)
	{
		// EXPECT_EQ(doubleV->getBoolBuffer(0,size,buf)[i], (bool)2);
		// EXPECT_EQ(doubleV->getCharBuffer(0,size,buf)[i], 2);
		EXPECT_EQ(doubleV->getShortBuffer(0, size, buf1)[i], doubleV->get(i)->getShort());
		EXPECT_EQ(doubleV->getIntBuffer(0, size, buf2)[i], doubleV->get(i)->getInt());
		EXPECT_EQ(doubleV->getLongBuffer(0, size, buf3)[i], doubleV->get(i)->getLong());
		EXPECT_EQ(doubleV->getIndexBuffer(0, size, buf4)[i], doubleV->get(i)->getIndex());
		EXPECT_EQ(doubleV->getFloatBuffer(0, size, buf5)[i], doubleV->get(i)->getFloat());
		EXPECT_EQ(doubleV->getDoubleBuffer(0, size, buf6)[i], doubleV->get(i)->getDouble());
	}

	delete[] buf, buf1, buf2, buf3, buf4, buf5, buf6, buf9, buf10, buf00, buf0, buf8;
}