#include <gtest/gtest.h>
#include "config.h"
#include "ConstantImp.h"

class DataformVectorTest : public testing::Test
{
    public:
        static dolphindb::DBConnection conn;
        // Suite
        static void SetUpTestSuite()
        {
            bool ret = conn.connect(HOST, PORT, USER, PASSWD);
            if (!ret)
            {
                std::cout << "Failed to connect to the server" << std::endl;
            }
            else
            {
                std::cout << "connect to " + HOST + ":" + std::to_string(PORT) << std::endl;
            }
        }
        static void TearDownTestSuite()
        {
            conn.close();
        }

    protected:
        // Case
        virtual void SetUp()
        {

        }
        virtual void TearDown()
        {
            
        }
};

dolphindb::DBConnection DataformVectorTest::conn(false, false);

#ifndef _MSC_VER
TEST_F(DataformVectorTest, testSymbolVector_get)
{
    dolphindb::ConstantSP v1 = dolphindb::Util::createVector(dolphindb::DT_SYMBOL, 0, 2);
    ((dolphindb::VectorSP)v1)->append(dolphindb::Util::createString("0"));
    ((dolphindb::VectorSP)v1)->append(dolphindb::Util::createString("1"));
    ASSERT_TRUE(v1->size() == 2);

    dolphindb::ConstantSP value1 = v1->get(dolphindb::Util::createInt(0));
    ASSERT_EQ(value1->getString(), "0");

    dolphindb::ConstantSP value2 = v1->get(dolphindb::Util::createInt(1));
    ASSERT_EQ(value2->getString(), "1");

    dolphindb::ConstantSP value3 = v1->get(dolphindb::Util::createInt(2));
    ASSERT_TRUE(value3.isNull());
}

TEST_F(DataformVectorTest, testSymbolVector_has)
{
    dolphindb::ConstantSP v1 = dolphindb::Util::createVector(dolphindb::DT_SYMBOL, 0, 2);
    ((dolphindb::VectorSP)v1)->append(dolphindb::Util::createString("0"));
    ((dolphindb::VectorSP)v1)->append(dolphindb::Util::createString("1"));
    ASSERT_TRUE(v1->size() == 2);
    dolphindb::SmartPointer<dolphindb::FastSymbolVector> symbolVector = v1;

    ASSERT_TRUE(symbolVector->has("0"));
    ASSERT_FALSE(symbolVector->has("2"));
}

TEST_F(DataformVectorTest, testSymbolVector_search)
{
    dolphindb::ConstantSP v1 = dolphindb::Util::createVector(dolphindb::DT_SYMBOL, 0, 2);
    ((dolphindb::VectorSP)v1)->append(dolphindb::Util::createString("0"));
    ((dolphindb::VectorSP)v1)->append(dolphindb::Util::createString("1"));
    ASSERT_TRUE(v1->size() == 2);
    dolphindb::SmartPointer<dolphindb::FastSymbolVector> symbolVector = v1;

    ASSERT_EQ(symbolVector->search("0"), 0);
    ASSERT_EQ(symbolVector->search("1"), 1);
    ASSERT_EQ(symbolVector->search("2"), -1);
}

TEST_F(DataformVectorTest, testIntVector_has)
{
    dolphindb::ConstantSP v1 = dolphindb::Util::createVector(dolphindb::DT_INT, 0, 2);
    ((dolphindb::VectorSP)v1)->append(dolphindb::Util::createInt(0));
    ((dolphindb::VectorSP)v1)->append(dolphindb::Util::createInt(1));
    ASSERT_TRUE(v1->size() == 2);
    dolphindb::SmartPointer<dolphindb::FastIntVector> intVector = v1;

    ASSERT_TRUE(intVector->has(0));
    ASSERT_FALSE(intVector->has(2));
}

TEST_F(DataformVectorTest, testIntVector_search)
{
    dolphindb::ConstantSP v1 = dolphindb::Util::createVector(dolphindb::DT_INT, 0, 2);
    ((dolphindb::VectorSP)v1)->append(dolphindb::Util::createInt(0));
    ((dolphindb::VectorSP)v1)->append(dolphindb::Util::createInt(1));
    ASSERT_TRUE(v1->size() == 2);
    dolphindb::SmartPointer<dolphindb::FastIntVector> intVector = v1;

    ASSERT_EQ(intVector->search(0), 0);
    ASSERT_EQ(intVector->search(1), 1);
    ASSERT_EQ(intVector->search(2), -1);
}

TEST_F(DataformVectorTest, testFixedLengthVector_resize)
{
    dolphindb::VectorSP vec = dolphindb::Util::createVector(dolphindb::DT_UUID, 4, 4);

    vec->resize(-1);
    ASSERT_EQ(vec->size(), 4);

    vec->resize(5);
    ASSERT_EQ(vec->size(), 5);
}

TEST_F(DataformVectorTest, testFixedLengthVector_reserve)
{
    dolphindb::VectorSP vec = dolphindb::Util::createVector(dolphindb::DT_UUID, 4, 4);
    ASSERT_EQ(vec->reserve(3), 4);
}

// assign不同长度的vector会fail
TEST_F(DataformVectorTest, testAnyVector_assign_fail)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_ANY, 0, 2);
    v1->append(dolphindb::Util::createInt(1));
    v1->append(dolphindb::Util::createInt(2));
    dolphindb::VectorSP assignVector = dolphindb::Util::createVector(dolphindb::DT_INT, 3, 3);
    ASSERT_FALSE(v1->assign(assignVector));
}

// dolphindb::AnyVector::getValue()
TEST_F(DataformVectorTest, testAnyVector_getValue)
{
    dolphindb::ConstantSP v1 = dolphindb::Util::createVector(dolphindb::DT_ANY, 0, 2);
    ((dolphindb::VectorSP)v1)->append(dolphindb::Util::createInt(1));
    ((dolphindb::VectorSP)v1)->append(dolphindb::Util::createInt(2));
    dolphindb::ConstantSP value = v1->getValue();

    ASSERT_EQ(v1->getForm(), value->getForm());
    ASSERT_EQ(value->getInt(0), 1);
    ASSERT_EQ(v1->get(0), value->get(0));
    ASSERT_EQ(value->getInt(1), 2);
    ASSERT_EQ(v1->get(1), value->get(1));
}

// // getSubVector函数有bug
TEST_F(DataformVectorTest, testAnyVector_getSubVector)
{
    dolphindb::ConstantSP v1 = dolphindb::Util::createVector(dolphindb::DT_ANY, 0, 2);
    ((dolphindb::VectorSP)v1)->append(dolphindb::Util::createInt(1));
    ((dolphindb::VectorSP)v1)->append(dolphindb::Util::createInt(2));
    ASSERT_TRUE(v1->size() == 2);

    dolphindb::ConstantSP v2 = ((dolphindb::VectorSP)v1)->getSubVector(-1, 2);
    ASSERT_TRUE(v2->size() == 0);

    dolphindb::ConstantSP v3 = ((dolphindb::VectorSP)v1)->getSubVector(2, 2);
    ASSERT_TRUE(v3->size() == 0);

    dolphindb::ConstantSP v4 = ((dolphindb::VectorSP)v1)->getSubVector(0, 3);
    ASSERT_TRUE(v4->size() == 0);

    dolphindb::ConstantSP v5 = ((dolphindb::VectorSP)v1)->getSubVector(0, 2);
    ASSERT_EQ(v5->size(), 2);
    ASSERT_TRUE(v5->getInt(0) == 1);
    ASSERT_TRUE(v5->getInt(1) == 2);

    dolphindb::ConstantSP v6 = ((dolphindb::VectorSP)v1)->getSubVector(1, -1);
    ASSERT_EQ(v6->size(), 1);
    ASSERT_EQ(v6->getInt(0), 2);
}

TEST_F(DataformVectorTest, testAnyVector_get)
{
    dolphindb::ConstantSP v1 = dolphindb::Util::createVector(dolphindb::DT_ANY, 0, 2);
    ((dolphindb::VectorSP)v1)->append(dolphindb::Util::createInt(0));
    ((dolphindb::VectorSP)v1)->append(dolphindb::Util::createInt(1));
    ASSERT_TRUE(v1->size() == 2);

    dolphindb::ConstantSP value1 = v1->get(dolphindb::Util::createInt(0));
    ASSERT_EQ(value1->getInt(), 0);

    dolphindb::ConstantSP value2 = v1->get(dolphindb::Util::createInt(1));
    ASSERT_EQ(value2->getInt(), 1);

    dolphindb::ConstantSP value3 = v1->get(dolphindb::Util::createInt(2));
    ASSERT_TRUE(value3.isNull());

    dolphindb::ConstantSP v2 = v1->getValue();
    dolphindb::ConstantSP value4 = v1->get(v2);
    ASSERT_EQ(value4->getInt(0), 0);
    ASSERT_EQ(value4->getInt(1), 1);
}

TEST_F(DataformVectorTest, testAnyVector_append_fail)
{
    dolphindb::ConstantSP v1 = dolphindb::Util::createVector(dolphindb::DT_ANY, dolphindb::Util::MAX_LENGTH_FOR_ANY_VECTOR, dolphindb::Util::MAX_LENGTH_FOR_ANY_VECTOR);
    ASSERT_FALSE(((dolphindb::VectorSP)v1)->append(dolphindb::Util::createInt(0)));
}

TEST_F(DataformVectorTest, testAnyVector_isHomogeneousScalar)
{
    dolphindb::ConstantSP v1 = dolphindb::Util::createVector(dolphindb::DT_ANY, 0, 3);
    dolphindb::SmartPointer<dolphindb::AnyVector> anyVector = v1;
    dolphindb::DATA_TYPE type;
    ASSERT_FALSE(anyVector->isHomogeneousScalar(type));

    anyVector->append(dolphindb::Util::createVector(dolphindb::DT_INT, 0, 3));
    ASSERT_FALSE(anyVector->isHomogeneousScalar(type));

    v1 = dolphindb::Util::createVector(dolphindb::DT_ANY, 0, 3);
    anyVector = v1;
    anyVector->append(dolphindb::Util::createInt(0));
    anyVector->append(dolphindb::Util::createDouble(0));
    ASSERT_FALSE(anyVector->isHomogeneousScalar(type));
    ASSERT_EQ(type, dolphindb::DT_INT);

    v1 = dolphindb::Util::createVector(dolphindb::DT_ANY, 0, 3);
    anyVector = v1;
    anyVector->append(dolphindb::Util::createInt(0));
    anyVector->append(dolphindb::Util::createVector(dolphindb::DT_INT, 0, 3));
    ASSERT_FALSE(anyVector->isHomogeneousScalar(type));

    v1 = dolphindb::Util::createVector(dolphindb::DT_ANY, 0, 3);
    anyVector = v1;
    anyVector->append(dolphindb::Util::createInt(0));
    anyVector->append(dolphindb::Util::createInt(1));
    ASSERT_TRUE(anyVector->isHomogeneousScalar(type));
}

TEST_F(DataformVectorTest, testAnyVector_isTabular)
{
    dolphindb::ConstantSP v1 = dolphindb::Util::createVector(dolphindb::DT_ANY, 0, 3);
    dolphindb::SmartPointer<dolphindb::AnyVector> anyVector = v1;
    ASSERT_FALSE(anyVector->isTabular());

    anyVector->append(dolphindb::Util::createVector(dolphindb::DT_INT, 0, 3));
    anyVector->append(dolphindb::Util::createVector(dolphindb::DT_INT, 0, 3));
    ASSERT_TRUE(anyVector->isTabular());

    anyVector->append(dolphindb::Util::createInt(0));
    ASSERT_FALSE(anyVector->isTabular());

    v1 = dolphindb::Util::createVector(dolphindb::DT_ANY, 0, 3);
    dolphindb::SmartPointer<dolphindb::AnyVector> anyVector2 = v1;
    anyVector2->append(anyVector);
    ASSERT_FALSE(anyVector2->isTabular());
}

TEST_F(DataformVectorTest, testAnyVector_convertToRegularVector)
{
    dolphindb::ConstantSP v1 = dolphindb::Util::createVector(dolphindb::DT_ANY, 0, 3);
    dolphindb::SmartPointer<dolphindb::AnyVector> anyVector = v1;
    dolphindb::ConstantSP regularVector = anyVector->convertToRegularVector();
    ASSERT_TRUE(regularVector->isNull());

    anyVector->append(dolphindb::Util::createInt(0));
    regularVector = anyVector->convertToRegularVector();
    ASSERT_EQ(regularVector->size(), 1);
    ASSERT_EQ(regularVector->getInt(0), 0);

    anyVector->append(dolphindb::Util::createInt(1));
    regularVector = anyVector->convertToRegularVector();
    ASSERT_EQ(regularVector->size(), 2);
    ASSERT_EQ(regularVector->getInt(1), 1);
}

TEST_F(DataformVectorTest, testStringVector_search)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_STRING, 2, 2);
    v1->set(0, dolphindb::Util::createString("1"));
    v1->set(1, dolphindb::Util::createString("2"));
    dolphindb::SmartPointer<dolphindb::StringVector> stringVec = v1;
    ASSERT_EQ(stringVec->search("1"), 0);
    ASSERT_EQ(stringVec->search("2"), 1);
    ASSERT_EQ(stringVec->search("3"), -1);
}

TEST_F(DataformVectorTest, testStringVector_getSubVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_STRING, 2, 2);
    v1->set(0, dolphindb::Util::createString("1"));
    v1->set(1, dolphindb::Util::createString("2"));
    dolphindb::SmartPointer<dolphindb::StringVector> stringVec = v1;
    dolphindb::ConstantSP subVector = stringVec->getSubVector(-1, 2, 2);
    ASSERT_EQ(subVector->size(), 0);

    subVector = stringVec->getSubVector(2, 2, 2);
    ASSERT_EQ(subVector->size(), 0);

    subVector = stringVec->getSubVector(0, 3, 2);
    ASSERT_EQ(subVector->size(), 0);
}

TEST_F(DataformVectorTest, testStringVector_get)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_STRING, 2, 2);
    v1->set(0, dolphindb::Util::createString("1"));
    v1->set(1, dolphindb::Util::createString("2"));
    dolphindb::SmartPointer<dolphindb::StringVector> stringVec = v1;
    dolphindb::ConstantSP res = stringVec->get(dolphindb::Util::createInt(0));
    ASSERT_EQ(res->getString(), "1");

    res = stringVec->get(dolphindb::Util::createInt(2));
    ASSERT_EQ(res->getString(), "");

    dolphindb::ConstantSP indexArray = dolphindb::Util::createVector(dolphindb::DT_INT, 2, 2);
    indexArray->set(0, dolphindb::Util::createInt(0));
    indexArray->set(1, dolphindb::Util::createInt(1));
    res = stringVec->get(indexArray);
    ASSERT_EQ(res->getString(0), "1");
    ASSERT_EQ(res->getString(1), "2");
}

TEST_F(DataformVectorTest, testStringVector_remove)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_STRING, 3, 3);
    v1->set(0, dolphindb::Util::createString("1"));
    v1->set(1, dolphindb::Util::createString("2"));
    v1->set(2, dolphindb::Util::createString("3"));
    dolphindb::SmartPointer<dolphindb::StringVector> stringVec = v1;
    stringVec->remove(1);
    ASSERT_EQ(stringVec->size(), 2);
    ASSERT_EQ(stringVec->getString(0), "1");
    ASSERT_EQ(stringVec->getString(1), "2");

    stringVec->remove(-1);
    ASSERT_EQ(stringVec->size(), 1);
    ASSERT_EQ(stringVec->getString(0), "2");
}

TEST_F(DataformVectorTest, testBoolVector_get)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_BOOL, 2, 2);
    v1->set(0, dolphindb::Util::createBool(false));
    v1->set(1, dolphindb::Util::createBool(true));
    dolphindb::SmartPointer<dolphindb::FastBoolVector> boolVector = v1;
    dolphindb::ConstantSP res = boolVector->get(dolphindb::Util::createInt(1));
    ASSERT_EQ(res->getBool(), true);

    res = boolVector->get(dolphindb::Util::createInt(10));
    ASSERT_TRUE(res.isNull());

    dolphindb::ConstantSP indexArray = dolphindb::Util::createVector(dolphindb::DT_INT, 2, 2);
    indexArray->set(0, dolphindb::Util::createInt(0));
    indexArray->set(1, dolphindb::Util::createInt(1));
    res = boolVector->get(indexArray);
    ASSERT_EQ(res->getBool(0), false);
    ASSERT_EQ(res->getBool(1), true);
}

TEST_F(DataformVectorTest, testBoolVector_compare)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_BOOL, 2, 2);
    dolphindb::ConstantSP bool0 = dolphindb::Util::createBool(false);
    dolphindb::ConstantSP bool1 = dolphindb::Util::createBool(true);
    v1->set(0, bool0);
    v1->set(1, bool1);
    dolphindb::SmartPointer<dolphindb::FastBoolVector> boolVector = v1;
    ASSERT_EQ(boolVector->compare(0, bool1), -1);
    ASSERT_EQ(boolVector->compare(1, bool0), 1);
}

TEST_F(DataformVectorTest, testCharVector_get)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_CHAR, 2, 2);
    v1->set(0, dolphindb::Util::createChar('0'));
    v1->set(1, dolphindb::Util::createChar('1'));
    dolphindb::SmartPointer<dolphindb::FastCharVector> charVector = v1;
    dolphindb::ConstantSP res = charVector->get(dolphindb::Util::createInt(1));
    ASSERT_EQ(res->getChar(), '1');

    res = charVector->get(dolphindb::Util::createInt(10));
    ASSERT_TRUE(res.isNull());

    dolphindb::ConstantSP indexArray = dolphindb::Util::createVector(dolphindb::DT_INT, 2, 2);
    indexArray->set(0, dolphindb::Util::createInt(0));
    indexArray->set(1, dolphindb::Util::createInt(1));
    res = charVector->get(indexArray);
    ASSERT_EQ(res->getChar(0), '0');
    ASSERT_EQ(res->getChar(1), '1');
}

TEST_F(DataformVectorTest, testCharVector_compare)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_CHAR, 2, 2);
    dolphindb::ConstantSP char0 = dolphindb::Util::createChar('0');
    dolphindb::ConstantSP char1 = dolphindb::Util::createChar('1');
    v1->set(0, char0);
    v1->set(1, char1);
    dolphindb::SmartPointer<dolphindb::FastCharVector> charVector = v1;
    ASSERT_EQ(charVector->compare(0, char0), 0);
    ASSERT_EQ(charVector->compare(0, char1), -1);
    ASSERT_EQ(charVector->compare(1, char0), 1);
}

TEST_F(DataformVectorTest, testCharVector_upper)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_CHAR, 4, 4);
    dolphindb::ConstantSP char0 = dolphindb::Util::createChar('0');
    dolphindb::ConstantSP char1 = dolphindb::Util::createChar('a');
    dolphindb::ConstantSP char2 = dolphindb::Util::createChar('B');
    dolphindb::ConstantSP char3 = dolphindb::Util::createChar('|');
    v1->set(0, char0);
    v1->set(1, char1);
    v1->set(2, char2);
    v1->set(3, char3);
    dolphindb::SmartPointer<dolphindb::FastCharVector> charVector = v1;
    charVector->upper();
    ASSERT_EQ(charVector->getChar(1), 'A');
}

TEST_F(DataformVectorTest, testCharVector_lower)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_CHAR, 4, 4);
    dolphindb::ConstantSP char0 = dolphindb::Util::createChar('0');
    dolphindb::ConstantSP char1 = dolphindb::Util::createChar('a');
    dolphindb::ConstantSP char2 = dolphindb::Util::createChar('B');
    dolphindb::ConstantSP char3 = dolphindb::Util::createChar('|');
    v1->set(0, char0);
    v1->set(1, char1);
    v1->set(2, char2);
    v1->set(3, char3);
    dolphindb::SmartPointer<dolphindb::FastCharVector> charVector = v1;
    charVector->lower();
    ASSERT_EQ(charVector->getChar(2), 'b');
}

TEST_F(DataformVectorTest, testIntVector_get)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_INT, 2, 2);
    v1->set(0, dolphindb::Util::createInt(0));
    v1->set(1, dolphindb::Util::createInt(1));
    dolphindb::SmartPointer<dolphindb::FastIntVector> intVector = v1;
    dolphindb::ConstantSP res = intVector->get(dolphindb::Util::createInt(1));
    ASSERT_EQ(res->getInt(), 1);

    res = intVector->get(dolphindb::Util::createInt(10));
    ASSERT_TRUE(res.isNull());

    dolphindb::ConstantSP indexArray = dolphindb::Util::createVector(dolphindb::DT_INT, 2, 2);
    indexArray->set(0, dolphindb::Util::createInt(0));
    indexArray->set(1, dolphindb::Util::createInt(1));
    res = intVector->get(indexArray);
    ASSERT_EQ(res->getInt(0), 0);
    ASSERT_EQ(res->getInt(1), 1);
}

TEST_F(DataformVectorTest, testIntVector_compare)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_INT, 2, 2);
    dolphindb::ConstantSP int0 = dolphindb::Util::createInt(0);
    dolphindb::ConstantSP int1 = dolphindb::Util::createInt(1);
    v1->set(0, int0);
    v1->set(1, int1);
    dolphindb::SmartPointer<dolphindb::FastIntVector> intVector = v1;
    ASSERT_EQ(intVector->compare(0, int0), 0);
    ASSERT_EQ(intVector->compare(0, int1), -1);
    ASSERT_EQ(intVector->compare(1, int0), 1);
}

TEST_F(DataformVectorTest, testLongVector_get)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_LONG, 2, 2);
    v1->set(0, dolphindb::Util::createLong(0));
    v1->set(1, dolphindb::Util::createLong(1));
    dolphindb::SmartPointer<dolphindb::FastLongVector> longVector = v1;
    dolphindb::ConstantSP res = longVector->get(dolphindb::Util::createLong(1));
    ASSERT_EQ(res->getLong(), 1);

    res = longVector->get(dolphindb::Util::createLong(10));
    ASSERT_TRUE(res.isNull());

    dolphindb::ConstantSP indexArray = dolphindb::Util::createVector(dolphindb::DT_LONG, 2, 2);
    indexArray->set(0, dolphindb::Util::createLong(0));
    indexArray->set(1, dolphindb::Util::createLong(1));
    res = longVector->get(indexArray);
    ASSERT_EQ(res->getLong(0), 0);
    ASSERT_EQ(res->getLong(1), 1);
}

TEST_F(DataformVectorTest, testLongVector_compare)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_LONG, 2, 2);
    dolphindb::ConstantSP long0 = dolphindb::Util::createLong(0);
    dolphindb::ConstantSP long1 = dolphindb::Util::createLong(1);
    v1->set(0, long0);
    v1->set(1, long1);
    dolphindb::SmartPointer<dolphindb::FastLongVector> longVector = v1;
    ASSERT_EQ(longVector->compare(0, long0), 0);
    ASSERT_EQ(longVector->compare(0, long1), -1);
    ASSERT_EQ(longVector->compare(1, long0), 1);
}

TEST_F(DataformVectorTest, testShortVector_get)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_SHORT, 2, 2);
    v1->set(0, dolphindb::Util::createShort(0));
    v1->set(1, dolphindb::Util::createShort(1));
    dolphindb::SmartPointer<dolphindb::FastShortVector> shortVector = v1;
    dolphindb::ConstantSP res = shortVector->get(dolphindb::Util::createInt(1));
    ASSERT_EQ(res->getShort(), 1);

    res = shortVector->get(dolphindb::Util::createInt(10));
    ASSERT_TRUE(res.isNull());

    dolphindb::ConstantSP indexArray = dolphindb::Util::createVector(dolphindb::DT_INT, 2, 2);
    indexArray->set(0, dolphindb::Util::createInt(0));
    indexArray->set(1, dolphindb::Util::createInt(1));
    res = shortVector->get(indexArray);
    ASSERT_EQ(res->getShort(0), 0);
    ASSERT_EQ(res->getShort(1), 1);
}

TEST_F(DataformVectorTest, testShortVector_compare)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_SHORT, 2, 2);
    dolphindb::ConstantSP short0 = dolphindb::Util::createShort(0);
    dolphindb::ConstantSP short1 = dolphindb::Util::createShort(1);
    v1->set(0, short0);
    v1->set(1, short1);
    dolphindb::SmartPointer<dolphindb::FastShortVector> shortVector = v1;
    ASSERT_EQ(shortVector->compare(0, short0), 0);
    ASSERT_EQ(shortVector->compare(0, short1), -1);
    ASSERT_EQ(shortVector->compare(1, short0), 1);
}

TEST_F(DataformVectorTest, testFloatVector_get)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_FLOAT, 2, 2);
    v1->set(0, dolphindb::Util::createFloat(0));
    v1->set(1, dolphindb::Util::createFloat(1));
    dolphindb::SmartPointer<dolphindb::FastFloatVector> floatVector = v1;
    dolphindb::ConstantSP res = floatVector->get(dolphindb::Util::createFloat(1));
    ASSERT_EQ(res->getFloat(), 1);

    res = floatVector->get(dolphindb::Util::createFloat(10));
    ASSERT_TRUE(res.isNull());

    dolphindb::ConstantSP indexArray = dolphindb::Util::createVector(dolphindb::DT_FLOAT, 2, 2);
    indexArray->set(0, dolphindb::Util::createFloat(0));
    indexArray->set(1, dolphindb::Util::createFloat(1));
    res = floatVector->get(indexArray);
    ASSERT_EQ(res->getFloat(0), 0);
    ASSERT_EQ(res->getFloat(1), 1);
}

TEST_F(DataformVectorTest, testDoubleVector_get)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DOUBLE, 2, 2);
    v1->set(0, dolphindb::Util::createDouble(0));
    v1->set(1, dolphindb::Util::createDouble(1));
    dolphindb::SmartPointer<dolphindb::FastDoubleVector> doubleVector = v1;
    dolphindb::ConstantSP res = doubleVector->get(dolphindb::Util::createDouble(1));
    ASSERT_EQ(res->getDouble(), 1);

    res = doubleVector->get(dolphindb::Util::createDouble(10));
    ASSERT_TRUE(res.isNull());

    dolphindb::ConstantSP indexArray = dolphindb::Util::createVector(dolphindb::DT_DOUBLE, 2, 2);
    indexArray->set(0, dolphindb::Util::createDouble(0));
    indexArray->set(1, dolphindb::Util::createDouble(1));
    res = doubleVector->get(indexArray);
    ASSERT_EQ(res->getDouble(0), 0);
    ASSERT_EQ(res->getDouble(1), 1);
}

TEST_F(DataformVectorTest, testDateVector_get)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DATE, 2, 2);
    v1->set(0, dolphindb::Util::createDate(0));
    v1->set(1, dolphindb::Util::createDate(1));
    dolphindb::SmartPointer<dolphindb::FastDateVector> dateVector = v1;
    dolphindb::ConstantSP res = dateVector->get(dolphindb::Util::createInt(1));
    ASSERT_EQ(res->getString(), "1970.01.02");

    res = dateVector->get(dolphindb::Util::createInt(10));
    ASSERT_TRUE(res.isNull());

    dolphindb::ConstantSP indexArray = dolphindb::Util::createVector(dolphindb::DT_DATE, 2, 2);
    indexArray->set(0, dolphindb::Util::createInt(0));
    indexArray->set(1, dolphindb::Util::createInt(1));
    res = dateVector->get(indexArray);
    ASSERT_EQ(res->getString(0), "1970.01.01");
    ASSERT_EQ(res->getString(1), "1970.01.02");
}

TEST_F(DataformVectorTest, testDateTimeVector_get)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DATETIME, 2, 2);
    v1->set(0, dolphindb::Util::createDateTime(0));
    v1->set(1, dolphindb::Util::createDateTime(1));
    dolphindb::SmartPointer<dolphindb::FastDateTimeVector> dateTimeVector = v1;
    dolphindb::ConstantSP res = dateTimeVector->get(dolphindb::Util::createInt(1));
    ASSERT_EQ(res->getString(), "1970.01.01T00:00:01");

    res = dateTimeVector->get(dolphindb::Util::createInt(10));
    ASSERT_TRUE(res.isNull());

    dolphindb::ConstantSP indexArray = dolphindb::Util::createVector(dolphindb::DT_DATETIME, 2, 2);
    indexArray->set(0, dolphindb::Util::createInt(0));
    indexArray->set(1, dolphindb::Util::createInt(1));
    res = dateTimeVector->get(indexArray);
    ASSERT_EQ(res->getString(0), "1970.01.01T00:00:00");
    ASSERT_EQ(res->getString(1), "1970.01.01T00:00:01");
}

TEST_F(DataformVectorTest, testDateHourVector_get)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DATEHOUR, 2, 2);
    v1->set(0, dolphindb::Util::createDateHour(0));
    v1->set(1, dolphindb::Util::createDateHour(1));
    dolphindb::SmartPointer<dolphindb::FastDateHourVector> dateHourVector = v1;
    dolphindb::ConstantSP res = dateHourVector->get(dolphindb::Util::createInt(1));
    ASSERT_EQ(res->getString(), "1970.01.01T01");

    res = dateHourVector->get(dolphindb::Util::createInt(10));
    ASSERT_TRUE(res.isNull());

    dolphindb::ConstantSP indexArray = dolphindb::Util::createVector(dolphindb::DT_DATEHOUR, 2, 2);
    indexArray->set(0, dolphindb::Util::createInt(0));
    indexArray->set(1, dolphindb::Util::createInt(1));
    res = dateHourVector->get(indexArray);
    ASSERT_EQ(res->getString(0), "1970.01.01T00");
    ASSERT_EQ(res->getString(1), "1970.01.01T01");
}

TEST_F(DataformVectorTest, testMonthVector_get)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_MONTH, 2, 2);
    v1->set(0, dolphindb::Util::createMonth(0));
    v1->set(1, dolphindb::Util::createMonth(1));
    dolphindb::SmartPointer<dolphindb::FastMonthVector> monthVector = v1;
    dolphindb::ConstantSP res = monthVector->get(dolphindb::Util::createInt(1));
    ASSERT_EQ(res->getString(), "0000.02M");

    res = monthVector->get(dolphindb::Util::createInt(10));
    ASSERT_TRUE(res.isNull());

    dolphindb::ConstantSP indexArray = dolphindb::Util::createVector(dolphindb::DT_MONTH, 2, 2);
    indexArray->set(0, dolphindb::Util::createInt(0));
    indexArray->set(1, dolphindb::Util::createInt(1));
    res = monthVector->get(indexArray);
    ASSERT_EQ(res->getString(0), "0000.01M");
    ASSERT_EQ(res->getString(1), "0000.02M");
}

TEST_F(DataformVectorTest, testTimeVector_get)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_TIME, 2, 2);
    v1->set(0, dolphindb::Util::createTime(0));
    v1->set(1, dolphindb::Util::createTime(1));
    dolphindb::SmartPointer<dolphindb::FastTimeVector> timeVector = v1;
    dolphindb::ConstantSP res = timeVector->get(dolphindb::Util::createInt(1));
    ASSERT_EQ(res->getString(), "00:00:00.001");

    res = timeVector->get(dolphindb::Util::createInt(10));
    ASSERT_TRUE(res.isNull());

    dolphindb::ConstantSP indexArray = dolphindb::Util::createVector(dolphindb::DT_TIME, 2, 2);
    indexArray->set(0, dolphindb::Util::createInt(0));
    indexArray->set(1, dolphindb::Util::createInt(1));
    res = timeVector->get(indexArray);
    ASSERT_EQ(res->getString(0), "00:00:00.000");
    ASSERT_EQ(res->getString(1), "00:00:00.001");
}

TEST_F(DataformVectorTest, testMinuteVector_get)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_MINUTE, 2, 2);
    v1->set(0, dolphindb::Util::createMinute(0));
    v1->set(1, dolphindb::Util::createMinute(1));
    dolphindb::SmartPointer<dolphindb::FastMinuteVector> minuteVector = v1;
    dolphindb::ConstantSP res = minuteVector->get(dolphindb::Util::createInt(1));
    ASSERT_EQ(res->getString(), "00:01m");

    res = minuteVector->get(dolphindb::Util::createInt(10));
    ASSERT_TRUE(res.isNull());

    dolphindb::ConstantSP indexArray = dolphindb::Util::createVector(dolphindb::DT_MINUTE, 2, 2);
    indexArray->set(0, dolphindb::Util::createInt(0));
    indexArray->set(1, dolphindb::Util::createInt(1));
    res = minuteVector->get(indexArray);
    ASSERT_EQ(res->getString(0), "00:00m");
    ASSERT_EQ(res->getString(1), "00:01m");
}

TEST_F(DataformVectorTest, testSecondVector_get)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_SECOND, 2, 2);
    v1->set(0, dolphindb::Util::createSecond(0));
    v1->set(1, dolphindb::Util::createSecond(1));
    dolphindb::SmartPointer<dolphindb::FastSecondVector> secondVector = v1;
    dolphindb::ConstantSP res = secondVector->get(dolphindb::Util::createInt(1));
    ASSERT_EQ(res->getString(), "00:00:01");

    res = secondVector->get(dolphindb::Util::createInt(10));
    ASSERT_TRUE(res.isNull());

    dolphindb::ConstantSP indexArray = dolphindb::Util::createVector(dolphindb::DT_SECOND, 2, 2);
    indexArray->set(0, dolphindb::Util::createInt(0));
    indexArray->set(1, dolphindb::Util::createInt(1));
    res = secondVector->get(indexArray);
    ASSERT_EQ(res->getString(0), "00:00:00");
    ASSERT_EQ(res->getString(1), "00:00:01");
}

TEST_F(DataformVectorTest, testNanoTimeVector_get)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_NANOTIME, 2, 2);
    v1->set(0, dolphindb::Util::createNanoTime(0));
    v1->set(1, dolphindb::Util::createNanoTime(1));
    dolphindb::SmartPointer<dolphindb::FastNanoTimeVector> nanoTimeVector = v1;
    dolphindb::ConstantSP res = nanoTimeVector->get(dolphindb::Util::createInt(1));
    ASSERT_EQ(res->getString(), "00:00:00.000000001");

    res = nanoTimeVector->get(dolphindb::Util::createInt(10));
    ASSERT_TRUE(res.isNull());

    dolphindb::ConstantSP indexArray = dolphindb::Util::createVector(dolphindb::DT_NANOTIME, 2, 2);
    indexArray->set(0, dolphindb::Util::createInt(0));
    indexArray->set(1, dolphindb::Util::createInt(1));
    res = nanoTimeVector->get(indexArray);
    ASSERT_EQ(res->getString(0), "00:00:00.000000000");
    ASSERT_EQ(res->getString(1), "00:00:00.000000001");
}

TEST_F(DataformVectorTest, testTimestampVector_get)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_TIMESTAMP, 2, 2);
    v1->set(0, dolphindb::Util::createTimestamp(0));
    v1->set(1, dolphindb::Util::createTimestamp(1));
    dolphindb::SmartPointer<dolphindb::FastTimestampVector> timestampVector = v1;
    dolphindb::ConstantSP res = timestampVector->get(dolphindb::Util::createInt(1));
    ASSERT_EQ(res->getString(), "1970.01.01T00:00:00.001");

    res = timestampVector->get(dolphindb::Util::createInt(10));
    ASSERT_TRUE(res.isNull());

    dolphindb::ConstantSP indexArray = dolphindb::Util::createVector(dolphindb::DT_TIMESTAMP, 2, 2);
    indexArray->set(0, dolphindb::Util::createInt(0));
    indexArray->set(1, dolphindb::Util::createInt(1));
    res = timestampVector->get(indexArray);
    ASSERT_EQ(res->getString(0), "1970.01.01T00:00:00.000");
    ASSERT_EQ(res->getString(1), "1970.01.01T00:00:00.001");
}

TEST_F(DataformVectorTest, testNanoTimestampVector_get)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_NANOTIMESTAMP, 2, 2);
    v1->set(0, dolphindb::Util::createNanoTimestamp(0));
    v1->set(1, dolphindb::Util::createNanoTimestamp(1));
    dolphindb::SmartPointer<dolphindb::FastNanoTimestampVector> nanoNanoTimestampVector = v1;
    dolphindb::ConstantSP res = nanoNanoTimestampVector->get(dolphindb::Util::createInt(1));
    ASSERT_EQ(res->getString(), "1970.01.01T00:00:00.000000001");

    res = nanoNanoTimestampVector->get(dolphindb::Util::createInt(10));
    ASSERT_TRUE(res.isNull());

    dolphindb::ConstantSP indexArray = dolphindb::Util::createVector(dolphindb::DT_NANOTIMESTAMP, 2, 2);
    indexArray->set(0, dolphindb::Util::createInt(0));
    indexArray->set(1, dolphindb::Util::createInt(1));
    res = nanoNanoTimestampVector->get(indexArray);
    ASSERT_EQ(res->getString(0), "1970.01.01T00:00:00.000000000");
    ASSERT_EQ(res->getString(1), "1970.01.01T00:00:00.000000001");
}

#endif // _MSC_VER

TEST_F(DataformVectorTest, testVoidVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_VOID, 100);
    ASSERT_EQ(v1->getCategory(), dolphindb::NOTHING);
    for (dolphindb::INDEX i = 0; i < 100; i++)
        ASSERT_EQ(v1->getString(i), "");
    ASSERT_ANY_THROW(v1->set(0, dolphindb::Util::createInt(1)));
    ASSERT_ANY_THROW(v1->set(dolphindb::Util::createInt(0), dolphindb::Util::createInt(1)));
    ASSERT_ANY_THROW(v1->fill(0, 1, dolphindb::Util::createInt(1)));
    ASSERT_ANY_THROW(v1->append(dolphindb::Util::createInt(1), 1));
    ASSERT_ANY_THROW(v1->append(dolphindb::Util::createInt(1), 0, 1));
    ASSERT_FALSE(v1->add(0, 1, 1ll));
    ASSERT_FALSE(v1->add(0, 1, 1.));
    ASSERT_ANY_THROW(v1->compare(0, dolphindb::Util::createInt(1)));
    ASSERT_ANY_THROW(v1->asof(dolphindb::Util::createInt(1)));
}

TEST_F(DataformVectorTest, testAnyVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_ANY, 5, 5);
    v1->set(0, dolphindb::Util::createTime(1));
    v1->set(1, dolphindb::Util::createInt(1));
    v1->set(2, dolphindb::Util::createDouble(1));
    v1->set(3, dolphindb::Util::createString("asd"));
    v1->set(4, dolphindb::Util::createBool(true));
    std::string script = "a=[time(1), int(1),double(1),`asd,true];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());

    dolphindb::VectorSP v2 = dolphindb::Util::createVector(dolphindb::DT_ANY, 0, 2000, false, 0, (void *)0, true);
    ASSERT_FALSE(v2->getNullFlag()); // anystd::vector is absolutely not null
    ASSERT_EQ(v2->getCapacity(), 0);
    ASSERT_FALSE(v2->isFastMode());
    ASSERT_EQ(v2->getUnitLength(), 0);
    ASSERT_TRUE(v2->sizeable());
    ASSERT_EQ(v2->getRawType(), dolphindb::DT_ANY);
    ASSERT_EQ(v2->getCategory(), dolphindb::MIXED);
    ASSERT_ANY_THROW(v2->getStringRef(0));

    v2->append(dolphindb::Util::createInt(1));
    dolphindb::VectorSP v3 = v2->getInstance(1);
    ASSERT_EQ(v3->getString(), "()");
    ASSERT_FALSE(v3->isNull()); // anystd::vector is absolutely not null
    dolphindb::VectorSP v4 = v2->getValue(10);
    ASSERT_EQ(v4->getString(), "(1)");
    v4->setNull(); // nothing to do and anystd::vector is not changed.
    ASSERT_EQ(v4->getString(), "(1)");
    ASSERT_FALSE(v2->append(dolphindb::Util::createInt(2), 1));
    ASSERT_TRUE(v2->isLargeConstant());

    ASSERT_EQ(v2->getBool(0), true);
    ASSERT_EQ(v2->getChar(0), (char)1);
    ASSERT_EQ(v2->getShort(0), (short)1);
    ASSERT_EQ(v2->getInt(0), (int)1);
    ASSERT_EQ(v2->getLong(0), (long)1);
    ASSERT_EQ(v2->getIndex(0), (dolphindb::INDEX)1);
    ASSERT_EQ(v2->getFloat(0), (float)1);
    ASSERT_EQ(v2->getDouble(0), (double)1);

    char *buf = new char[1];
    std::string **buf1 = new std::string *[1];
    char **buf2 = new char *[1];
    int numElement, partial;
    ASSERT_ANY_THROW(v2->serialize(buf, 1, 0, 1, numElement, partial));
    // ASSERT_ANY_THROW(v2->getString(0,1,buf1));
    // ASSERT_ANY_THROW(v2->getString(0,1,buf2));
    ASSERT_ANY_THROW(v2->getStringConst(0, 1, buf1));
    ASSERT_ANY_THROW(v2->getStringConst(0, 1, buf2));

    ASSERT_ANY_THROW(v2->neg());
    ASSERT_ANY_THROW(v2->replace(dolphindb::Util::createInt(1), dolphindb::Util::createInt(0)));
    ASSERT_ANY_THROW(v2->asof(dolphindb::Util::createInt(1)));
    // ASSERT_ANY_THROW(v2->getSubVector(0,2,0));

    v2->append(dolphindb::Util::createInt(0));
    v2->reverse();
    ASSERT_EQ(v2->getString(), "(0,1)");
    v2->append(dolphindb::Util::createInt(-10));
    v2->reverse(0, 3);
    ASSERT_EQ(v2->getString(), "(-10,1,0)");

    v2->clear();
    ASSERT_EQ(v2->getString(), "()");

    dolphindb::VectorSP v5 = dolphindb::Util::createVector(dolphindb::DT_ANY, 2, 2);
    v5->set(0, dolphindb::Util::createDouble(3.21));
    v5->set(1, dolphindb::Util::createInt(1));
    ASSERT_FALSE(v5->assign(dolphindb::Util::createVector(dolphindb::DT_INT, 5, 5)));
    v5->assign(dolphindb::Util::createString("5"));
    ASSERT_EQ(v5->getString(), "(\"5\",\"5\")");
    v5->assign(dolphindb::Util::createNullConstant(dolphindb::DT_INT));
    ASSERT_EQ(v5->getString(), "(,)");

    v5->set(0, dolphindb::Util::createDouble(3.21));
    v5->set(1, dolphindb::Util::createInt(1));
    v5->append(dolphindb::Util::createString("str"));
    v5->append(dolphindb::Util::createNullConstant(dolphindb::DT_BOOL));

    v5->nullFill(dolphindb::Util::createDateTime(30000));
    ASSERT_EQ(v5->getString(), "(3.21,1,\"str\",1970.01.01T08:20:00)");
    v5->fill(0, 3, dolphindb::Util::createDate(700));
    ASSERT_EQ(v5->getString(), "(1971.12.02,1971.12.02,1971.12.02,1970.01.01T08:20:00)");
    dolphindb::VectorSP valVec = dolphindb::Util::createVector(dolphindb::DT_IP, 3, 3);
    valVec->set(0, dolphindb::Util::createNullConstant(dolphindb::DT_UUID));
    valVec->set(1, dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.0.0.5"));
    valVec->set(2, dolphindb::Util::parseConstant(dolphindb::DT_IP, "10.20.30.40"));

    v5->fill(0, 3, valVec);
    ASSERT_EQ(v5->getString(), "(,192.0.0.5,10.20.30.40,1970.01.01T08:20:00)");

    char *buf3 = new char[2];
    v5->isNull(0, 2, buf3);
    ASSERT_TRUE((int)buf3[0]);
    ASSERT_FALSE((int)buf3[1]);

    buf3[0] = '\0';
    buf3[1] = '\0';
    v5->isValid(0, 2, buf3);
    ASSERT_FALSE((int)buf3[0]);
    ASSERT_TRUE((int)buf3[1]);
    delete[] buf, buf1, buf2, buf3;

    ASSERT_EQ(v5->getValue(0)->getString(), v5->getString());
    dolphindb::VectorSP v6 = dolphindb::Util::createVector(dolphindb::DT_ANY, 1, 1);
    v6->set(0, dolphindb::Util::createFloat(3.12));

    ASSERT_ANY_THROW(v5->getBool());
    ASSERT_ANY_THROW(v5->getChar());
    ASSERT_ANY_THROW(v5->getShort());
    ASSERT_ANY_THROW(v5->getLong());
    ASSERT_ANY_THROW(v5->getIndex());
    ASSERT_ANY_THROW(v5->getInt());
    ASSERT_ANY_THROW(v5->getFloat());
    ASSERT_ANY_THROW(v5->getDouble());
    ASSERT_EQ(v6->getBool(), (bool)3);
    ASSERT_EQ(v6->getChar(), (char)3);
    ASSERT_EQ(v6->getShort(), (short)3);
    ASSERT_EQ(v6->getLong(), (long)3);
    ASSERT_EQ(v6->getIndex(), (dolphindb::INDEX)3);
    ASSERT_EQ(v6->getInt(), (int)3);
    ASSERT_EQ(v6->getFloat(), (float)3.12);
    ASSERT_EQ(v6->getDouble(), (float)3.12);

    std::cout << v5->getAllocatedMemory(1) << std::endl; // AC-122: not supported

    dolphindb::VectorSP v7 = dolphindb::Util::createVector(dolphindb::DT_ANY, 3, 10);
    dolphindb::VectorSP v8 = dolphindb::Util::createVector(dolphindb::DT_INT, 1, 1);
    v8->setInt(1);
    v7->setNull(0);
    v7->set(1, dolphindb::Util::createInt(1));
    v7->set(2, v8);
    char *buf4 = new char[2];
    char *buf5 = new char[2];
    short *buf6 = new short[2];
    int *buf7 = new int[2];
    dolphindb::INDEX *buf8 = new dolphindb::INDEX[2];
    long long *buf9 = new long long[2];
    float *buf10 = new float[2];
    double *buf11 = new double[2];
    ASSERT_FALSE(v7->getBool(2, 1, buf4));
    ASSERT_FALSE(v7->getChar(2, 1, buf5));
    ASSERT_FALSE(v7->getShort(2, 1, buf6));
    ASSERT_FALSE(v7->getInt(2, 1, buf7));
    ASSERT_FALSE(v7->getIndex(2, 1, buf8));
    ASSERT_FALSE(v7->getLong(2, 1, buf9));
    ASSERT_FALSE(v7->getFloat(2, 1, buf10));
    ASSERT_FALSE(v7->getDouble(2, 1, buf11));

    ASSERT_EQ(v7->getBoolConst(1, 1, buf4)[0], (bool)1);
    ASSERT_EQ(v7->getCharConst(1, 1, buf5)[0], (char)1);
    ASSERT_EQ(v7->getShortConst(1, 1, buf6)[0], (short)1);
    ASSERT_EQ(v7->getIntConst(1, 1, buf7)[0], (int)1);
    ASSERT_EQ(v7->getIndexConst(1, 1, buf8)[0], (dolphindb::INDEX)1);
    ASSERT_EQ(v7->getLongConst(1, 1, buf9)[0], (long)1);
    ASSERT_EQ(v7->getFloatConst(1, 1, buf10)[0], (float)1);
    ASSERT_EQ(v7->getDoubleConst(1, 1, buf11)[0], (float)1);
    delete[] buf4, buf5, buf6, buf7, buf8, buf9, buf10, buf11;

    dolphindb::VectorSP indexVec = dolphindb::Util::createIndexVector(0, 3);
    dolphindb::INDEX index = 2;
    v7->set(index, dolphindb::Util::createString("setstr"));
    ASSERT_EQ(v7->get(2)->getString(), "setstr");
    v7->set(index, dolphindb::Util::createNullConstant(dolphindb::DT_STRING));
    ASSERT_TRUE(v7->get(2)->isNull());
    v7->set(indexVec, dolphindb::Util::createNullConstant(dolphindb::DT_INT));
    ASSERT_EQ(v7->getString(), "(,,)");

    v7->append(dolphindb::Util::createNullConstant(dolphindb::DT_INT));
    ASSERT_TRUE(v7->get(2)->isNull());

    v7->append(dolphindb::Util::createString("str"));
    v7->remove(1);
    ASSERT_EQ(v7->getString(), "(,,,)");
    v7->remove(-1);
    ASSERT_EQ(v7->getString(), "(,,)");

    v7->append(dolphindb::Util::createString("str"));
    v7->append(dolphindb::Util::createInt(3));
    v7->append(v8);

    v7->next(1);
    ASSERT_EQ(v7->size(), 6);
    ASSERT_TRUE(v7->get(5)->isNull());

    v7->prev(2);
    ASSERT_EQ(v7->size(), 6);
    ASSERT_TRUE(v7->get(3)->isNull());

    v7->append(dolphindb::Util::createDecimal32(2, 3.1234));
    v7->append(dolphindb::Util::createDecimal64(5, 3.1234));
    conn.upload("v7", v7);
    ASSERT_EQ(conn.run("typestr(v7[6])")->getString(), "DECIMAL32");
    ASSERT_EQ(conn.run("typestr(v7[7])")->getString(), "DECIMAL64");
}

TEST_F(DataformVectorTest, testStringVector_upper)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_STRING, 2, 2);
    v1->set(0, dolphindb::Util::createString("|DolphinDB"));
    v1->upper();
    ASSERT_EQ(v1->getString(0), "|DOLPHINDB");
}

TEST_F(DataformVectorTest, testStringVector_lower)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_STRING, 2, 2);
    v1->set(0, dolphindb::Util::createString("|DolphinDB"));
    v1->lower();
    ASSERT_EQ(v1->getString(0), "|dolphindb");
}

TEST_F(DataformVectorTest, testStringVector_assign)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_STRING, 2, 2);
    v1->set(0, dolphindb::Util::createString("1"));
    v1->set(1, dolphindb::Util::createString("2"));
    dolphindb::VectorSP v2 = dolphindb::Util::createVector(dolphindb::DT_STRING, 2, 2);
    ASSERT_TRUE(v2->assign(v1));
    ASSERT_EQ(v2->getString(0), "1");
    ASSERT_EQ(v2->getString(1), "2");
}

TEST_F(DataformVectorTest, testBlobVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_BLOB, 2, 2);
    v1->set(0, dolphindb::Util::createBlob("asd123!@#"));
    v1->set(1, dolphindb::Util::createBlob("中文！@￥#%……a"));
    std::string script = "a=blob([\"asd123!@#\",\"中文！@￥#%……a\"]);a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());

    ASSERT_ANY_THROW(v1->getBool());
    ASSERT_ANY_THROW(v1->getChar());
    ASSERT_ANY_THROW(v1->getShort());
    ASSERT_ANY_THROW(v1->getLong());
    ASSERT_ANY_THROW(v1->getFloat());
    ASSERT_ANY_THROW(v1->getDouble());
    ASSERT_ANY_THROW(v1->getIndex());
    ASSERT_ANY_THROW(v1->neg());

    ASSERT_EQ(v1->getUnitLength(), 0);
    ASSERT_TRUE(v1->sizeable());
    ASSERT_EQ(v1->compare(1, dolphindb::Util::createBlob("中文！@￥#%……a")), 0);
    ASSERT_EQ(v1->getStringRef(), v1->get(0)->getString());
    v1->setNull(); // nothing to do
    ASSERT_FALSE(v1->hasNull());

    v1->reverse();
    ASSERT_EQ(v1->get(0)->getString(), dolphindb::Util::createBlob("中文！@￥#%……a")->getString());
    ASSERT_EQ(v1->get(1)->getString(), dolphindb::Util::createBlob("asd123!@#")->getString());
    v1->reverse(0, 1);
    ASSERT_EQ(v1->get(1)->getString(), dolphindb::Util::createBlob("中文！@￥#%……a")->getString());
    ASSERT_EQ(v1->get(0)->getString(), dolphindb::Util::createBlob("asd123!@#")->getString());

    std::cout << v1->getDataArray() << std::endl;
    std::cout << v1->getAllocatedMemory(1) << std::endl;

    v1->upper();
    ASSERT_EQ(v1->get(0)->getString(), "ASD123!@#");
    v1->lower();
    ASSERT_EQ(v1->get(0)->getString(), "asd123!@#");

    v1->set(0, dolphindb::Util::createBlob(" 1 2 3      "));
    v1->set(1, dolphindb::Util::createBlob(" \t\r\n 1 2 3 \t\n\r"));
    v1->trim();
    ASSERT_EQ(v1->get(0)->getString(), "1 2 3");
    ASSERT_EQ(v1->get(1)->getString(), "\t\r\n 1 2 3 \t\n\r");
    v1->strip();
    ASSERT_EQ(v1->get(0)->getString(), "1 2 3");
    ASSERT_EQ(v1->get(1)->getString(), "1 2 3");

    v1->set(dolphindb::Util::createInt(1), dolphindb::Util::createInt(1));
    ASSERT_EQ(v1->get(1)->getString(), "1");
    v1->set(dolphindb::Util::createInt(0), dolphindb::Util::createNullConstant(dolphindb::DT_INT));
    ASSERT_TRUE(v1->get(0)->isNull());
    ASSERT_ANY_THROW(v1->set(dolphindb::Util::createInt(1), dolphindb::Util::createVector(dolphindb::DT_BLOB, 1, 1)));

    ASSERT_FALSE(v1->assign(dolphindb::Util::createVector(dolphindb::DT_BLOB, 5, 5)));
    v1->assign(dolphindb::Util::createString("5"));
    ASSERT_EQ(v1->getString(), "[\"5\",\"5\"]");
    v1->assign(dolphindb::Util::createNullConstant(dolphindb::DT_BLOB));
    ASSERT_EQ(v1->getString(), "[,]");

    v1->set(0, dolphindb::Util::createString("bcd"));
    ASSERT_EQ(v1->getSubVector(0, -1, 2)->getString(), "[\"bcd\"]");

    v1->append(dolphindb::Util::createString("efg"), 10);
    ASSERT_EQ(v1->size(), 3);
    dolphindb::VectorSP strVec = dolphindb::Util::createVector(dolphindb::DT_BLOB, 1, 1);
    strVec->set(0, dolphindb::Util::createString("hij"));
    strVec->append(dolphindb::Util::createString("klm"));
    v1->append(strVec, 2);
    ASSERT_EQ(v1->size(), 5);

    dolphindb::VectorSP v2 = dolphindb::Util::createVector(dolphindb::DT_BLOB, 0, 0);
    char *strbuf = (char *)"charstr";
    char **str1 = &strbuf;
    v2->appendString(str1, 1);
    v2->appendString(str1, 1);
    ASSERT_EQ(v2->getString(), "[\"charstr\",\"charstr\"]");
    v2->append(dolphindb::Util::createNullConstant(dolphindb::DT_BLOB));
    v2->append(dolphindb::Util::createString("str"));

    // ASSERT_ANY_THROW(v2->next(-2));
    // ASSERT_ANY_THROW(v2->prev(-2));
    v2->next(1);
    ASSERT_EQ(v2->size(), 4);
    ASSERT_TRUE(v2->get(3)->isNull());

    v2->prev(2);
    ASSERT_EQ(v2->size(), 4);
    ASSERT_TRUE(v2->get(0)->isNull());
    ASSERT_TRUE(v2->get(1)->isNull());

    v2->nullFill(dolphindb::Util::createString("filled"));
    ASSERT_EQ(v2->getString(), "[\"filled\",\"filled\",\"charstr\",\"filled\"]");

    v2->fill(0, 1, dolphindb::Util::createString("twicefilled"));
    v2->fill(1, 1, dolphindb::Util::createNullConstant(dolphindb::DT_BLOB));
    v2->fill(3, 1, dolphindb::Util::createInt(3));
    ASSERT_EQ(v2->getString(), "[\"twicefilled\",,\"charstr\",\"3\"]");

    char *buf = new char[2];
    v2->isNull(1, 1, buf);
    ASSERT_EQ((int)buf[0], 1);
    buf[0] = '\0';
    v2->nullFill(dolphindb::Util::createString("filled"));
    v2->isNull(1, 1, buf);
    ASSERT_EQ((int)buf[0], 0);
    delete[] buf;

    std::string **buf1 = new std::string *[2];
    ASSERT_EQ(*v2->getStringConst(0, 1, buf1)[0], v2->get(0)->getString());
    delete[] buf1;

    v2->replace(dolphindb::Util::createString("3"), dolphindb::Util::createString("replaced"));
    // std::cout<<v2->getString()<<std::endl;
    ASSERT_EQ(v2->getString(), "[\"twicefilled\",\"filled\",\"charstr\",\"replaced\"]");

    dolphindb::VectorSP indexVec = dolphindb::Util::createIndexVector(0, 10);
    v2->remove(indexVec);
    ASSERT_EQ(v2->getString(), "[]");

    char **buf2 = new char *[v2->size()];
    v2->getStringConst(0, v2->size(), buf2);
    for (auto i = 0; i < v2->size(); i++)
        ASSERT_EQ(buf2[i], v2->get(i)->getString());

    delete[] buf2;
}

TEST_F(DataformVectorTest, testBlobNullVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_BLOB, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
    std::string script = "a=[blob(string(NULL)),blob(string(NULL))];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
}

TEST_F(DataformVectorTest, testStringVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_STRING, 2, 2);
    v1->set(0, dolphindb::Util::createString("asd123!@#"));
    v1->set(1, dolphindb::Util::createString("中文！@￥#%……a"));
    std::string script = "a=[\"asd123!@#\",\"中文！@￥#%……a\"];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());

    ASSERT_ANY_THROW(v1->getBool());
    ASSERT_ANY_THROW(v1->getChar());
    ASSERT_ANY_THROW(v1->getShort());
    ASSERT_ANY_THROW(v1->getLong());
    ASSERT_ANY_THROW(v1->getFloat());
    ASSERT_ANY_THROW(v1->getDouble());
    ASSERT_ANY_THROW(v1->getIndex());
    ASSERT_ANY_THROW(v1->neg());

    ASSERT_EQ(v1->getUnitLength(), 0);
    ASSERT_TRUE(v1->sizeable());
    ASSERT_EQ(v1->compare(1, dolphindb::Util::createString("中文！@￥#%……a")), 0);
    ASSERT_EQ(v1->getStringRef(), v1->get(0)->getString());
    v1->setNull(); // nothing to do
    ASSERT_FALSE(v1->hasNull());

    v1->reverse();
    ASSERT_EQ(v1->get(0)->getString(), dolphindb::Util::createString("中文！@￥#%……a")->getString());
    ASSERT_EQ(v1->get(1)->getString(), dolphindb::Util::createString("asd123!@#")->getString());
    v1->reverse(0, 1);
    ASSERT_EQ(v1->get(1)->getString(), dolphindb::Util::createString("中文！@￥#%……a")->getString());
    ASSERT_EQ(v1->get(0)->getString(), dolphindb::Util::createString("asd123!@#")->getString());

    std::cout << v1->getDataArray() << std::endl;
    std::cout << v1->getAllocatedMemory(1) << std::endl;

    v1->upper();
    ASSERT_EQ(v1->get(0)->getString(), "ASD123!@#");
    v1->lower();
    ASSERT_EQ(v1->get(0)->getString(), "asd123!@#");

    v1->set(0, dolphindb::Util::createString(" 1 2 3      "));
    v1->set(1, dolphindb::Util::createString(" \t\r\n 1 2 3 \t\n\r"));
    v1->trim();
    ASSERT_EQ(v1->get(0)->getString(), "1 2 3");
    ASSERT_EQ(v1->get(1)->getString(), "\t\r\n 1 2 3 \t\n\r");
    v1->strip();
    ASSERT_EQ(v1->get(0)->getString(), "1 2 3");
    ASSERT_EQ(v1->get(1)->getString(), "1 2 3");

    v1->set(dolphindb::Util::createInt(1), dolphindb::Util::createInt(1));
    ASSERT_EQ(v1->get(1)->getString(), "1");
    v1->set(dolphindb::Util::createInt(0), dolphindb::Util::createNullConstant(dolphindb::DT_INT));
    ASSERT_TRUE(v1->get(0)->isNull());
    ASSERT_ANY_THROW(v1->set(dolphindb::Util::createInt(1), dolphindb::Util::createVector(dolphindb::DT_STRING, 1, 1)));

    ASSERT_FALSE(v1->assign(dolphindb::Util::createVector(dolphindb::DT_STRING, 5, 5)));
    v1->assign(dolphindb::Util::createString("5"));
    ASSERT_EQ(v1->getString(), "[\"5\",\"5\"]");
    v1->assign(dolphindb::Util::createNullConstant(dolphindb::DT_STRING));
    ASSERT_EQ(v1->getString(), "[,]");

    v1->set(0, dolphindb::Util::createString("bcd"));
    ASSERT_EQ(v1->getSubVector(0, -1, 2)->getString(), "[\"bcd\"]");

    v1->append(dolphindb::Util::createString("efg"), 10);
    ASSERT_EQ(v1->size(), 3);
    dolphindb::VectorSP strVec = dolphindb::Util::createVector(dolphindb::DT_STRING, 1, 1);
    strVec->set(0, dolphindb::Util::createString("hij"));
    strVec->append(dolphindb::Util::createString("klm"));
    v1->append(strVec, 2);
    ASSERT_EQ(v1->size(), 5);

    dolphindb::VectorSP v2 = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, 0);
    char *strbuf = (char *)"charstr";
    char **str1 = &strbuf;
    v2->appendString(str1, 1);
    v2->appendString(str1, 1);
    ASSERT_EQ(v2->getString(), "[\"charstr\",\"charstr\"]");
    v2->append(dolphindb::Util::createNullConstant(dolphindb::DT_STRING));
    v2->append(dolphindb::Util::createString("str"));

    // ASSERT_ANY_THROW(v2->next(-2));
    // ASSERT_ANY_THROW(v2->prev(-2));
    v2->next(1);
    ASSERT_EQ(v2->size(), 4);
    ASSERT_TRUE(v2->get(3)->isNull());

    v2->prev(2);
    ASSERT_EQ(v2->size(), 4);
    ASSERT_TRUE(v2->get(0)->isNull());
    ASSERT_TRUE(v2->get(1)->isNull());

    v2->nullFill(dolphindb::Util::createString("filled"));
    ASSERT_EQ(v2->getString(), "[\"filled\",\"filled\",\"charstr\",\"filled\"]");

    v2->fill(0, 1, dolphindb::Util::createString("twicefilled"));
    v2->fill(1, 1, dolphindb::Util::createNullConstant(dolphindb::DT_STRING));
    v2->fill(3, 1, dolphindb::Util::createInt(3));
    ASSERT_EQ(v2->getString(), "[\"twicefilled\",,\"charstr\",\"3\"]");

    char *buf = new char[2];
    v2->isNull(1, 1, buf);
    ASSERT_EQ((int)buf[0], 1);
    buf[0] = '\0';
    v2->nullFill(dolphindb::Util::createString("filled"));
    v2->isNull(1, 1, buf);
    ASSERT_EQ((int)buf[0], 0);
    delete[] buf;

    std::string **buf1 = new std::string *[2];
    ASSERT_EQ(*v2->getStringConst(0, 1, buf1)[0], v2->get(0)->getString());
    delete[] buf1;

    v2->replace(dolphindb::Util::createString("3"), dolphindb::Util::createString("replaced"));
    // std::cout<<v2->getString()<<std::endl;
    ASSERT_EQ(v2->getString(), "[\"twicefilled\",\"filled\",\"charstr\",\"replaced\"]");

    dolphindb::VectorSP indexVec = dolphindb::Util::createIndexVector(0, 10);
    v2->remove(indexVec);
    ASSERT_EQ(v2->getString(), "[]");

    char **buf2 = new char *[v2->size()];
    v2->getStringConst(0, v2->size(), buf2);
    for (auto i = 0; i < v2->size(); i++)
        ASSERT_EQ(buf2[i], v2->get(i)->getString());

    delete[] buf2;
}

TEST_F(DataformVectorTest, testStringNullVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_STRING, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
    std::string script = "a=[string(NULL),string(NULL)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
}

TEST_F(DataformVectorTest, testBoolVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_BOOL, 2, 2);
    v1->set(0, dolphindb::Util::createBool(true));
    v1->set(1, dolphindb::Util::createBool(false));
    std::string script = "a=[bool(1),bool(0)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());

    ASSERT_EQ(v1->getCategory(), dolphindb::LOGICAL);
    ASSERT_FALSE(v1->add(0, 1, (long long)1));
    ASSERT_FALSE(v1->add(0, 1, (double)1));
    ASSERT_ANY_THROW(v1->asof(dolphindb::Util::createBool(true)));

    char *buf = new char[2];
    buf[0] = 1;
    buf[1] = 2;
    v1->getBoolBuffer(0, 2, buf);
    // std::cout<<v1->getString();
    ASSERT_TRUE(v1->setBool(0, 2, buf));
    ASSERT_EQ(v1->getString(), "[1,1]");
    ASSERT_TRUE(v1->appendBool(buf, 1));
    ASSERT_EQ(v1->getString(), "[1,1,1]");

    ASSERT_ANY_THROW(v1->fill(2, 1, dolphindb::Util::createString("1")));
    v1->fill(2, 1, dolphindb::Util::createNullConstant(dolphindb::DT_BOOL));
    ASSERT_TRUE(v1->hasNull());
    ASSERT_FALSE(v1->compare(0, dolphindb::Util::createBool(true)));

    dolphindb::VectorSP indexVec = dolphindb::Util::createIndexVector(2, 1);
    dolphindb::ConstantSP index = dolphindb::Util::createInt(2);
    v1->set(indexVec, dolphindb::Util::createBool(true));
    ASSERT_EQ(v1->get(2)->getBool(), 1);
    v1->set(indexVec, dolphindb::Util::createNullConstant(dolphindb::DT_BOOL));
    ASSERT_TRUE(v1->get(2)->isNull());
    v1->set(index, dolphindb::Util::createBool(true));
    ASSERT_EQ(v1->get(2)->getBool(), 1);

    dolphindb::VectorSP v2 = dolphindb::Util::createVector(dolphindb::DT_BOOL, 2, 10);
    v2->set(0, dolphindb::Util::createBool(true));
    v2->set(1, dolphindb::Util::createBool(false));
    char *buf1 = new char[2];
    buf1[0] = 1;
    short *buf2 = new short[2];
    buf2[0] = 1;
    int *buf3 = new int[2];
    buf3[0] = 1;
    dolphindb::INDEX *buf5 = new dolphindb::INDEX[2];
    buf5[0] = 1;
    long long *buf4 = new long long[2];
    buf4[0] = 1;
    float *buf6 = new float[2];
    buf6[0] = 1;
    double *buf7 = new double[2];
    buf7[0] = 1;

    // std::cout<<v2->getString()<<std::endl;
    v2->appendBool(buf1, 1);
    ASSERT_EQ(v2->getString(), "[1,0,1]");
    v2->appendChar(buf1, 1);
    ASSERT_EQ(v2->getString(), "[1,0,1,1]");
    v2->appendShort(buf2, 1);
    ASSERT_EQ(v2->getString(), "[1,0,1,1,1]");
    v2->appendInt(buf3, 1);
    ASSERT_EQ(v2->getString(), "[1,0,1,1,1,1]");
    v2->appendLong(buf4, 1);
    ASSERT_EQ(v2->getString(), "[1,0,1,1,1,1,1]");
    v2->appendIndex(buf5, 1);
    ASSERT_EQ(v2->getString(), "[1,0,1,1,1,1,1,1]");
    v2->appendFloat(buf6, 1);
    ASSERT_EQ(v2->getString(), "[1,0,1,1,1,1,1,1,1]");
    v2->appendDouble(buf7, 1);
    ASSERT_EQ(v2->getString(), "[1,0,1,1,1,1,1,1,1,1]");

    dolphindb::VectorSP v3 = dolphindb::Util::createVector(dolphindb::DT_BOOL, 2, 10);
    v3->set(0, dolphindb::Util::createBool(true));
    v3->set(1, dolphindb::Util::createBool(false));
    char *buf9 = new char[2];
    short *buf10 = new short[2];
    int *buf11 = new int[2];
    dolphindb::INDEX *buf12 = new dolphindb::INDEX[2];
    long long *buf13 = new long long[2];
    float *buf14 = new float[2];
    double *buf15 = new double[2];

    v3->getBool(0, 2, buf9);
    ASSERT_EQ((bool)buf9[0], v3->get(0)->getBool());
    ASSERT_EQ((bool)buf9[1], v3->get(1)->getBool());
    v3->getChar(0, 2, buf9);
    ASSERT_EQ((char)buf9[0], v3->get(0)->getChar());
    ASSERT_EQ((char)buf9[1], v3->get(1)->getChar());
    v3->getShort(0, 2, buf10);
    ASSERT_EQ((short)buf10[0], v3->get(0)->getShort());
    ASSERT_EQ((short)buf10[1], v3->get(1)->getShort());
    v3->getInt(0, 2, buf11);
    ASSERT_EQ((int)buf11[0], v3->get(0)->getInt());
    ASSERT_EQ((int)buf11[1], v3->get(1)->getInt());
    v3->getLong(0, 2, buf13);
    ASSERT_EQ((long long)buf13[0], v3->get(0)->getLong());
    ASSERT_EQ((long long)buf13[1], v3->get(1)->getLong());
    v3->getIndex(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)buf12[0], v3->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)buf12[1], v3->get(1)->getIndex());
    v3->getFloat(0, 2, buf14);
    ASSERT_EQ((float)buf14[0], v3->get(0)->getFloat());
    ASSERT_EQ((float)buf14[1], v3->get(1)->getFloat());
    v3->getDouble(0, 2, buf15);
    ASSERT_EQ((double)buf15[0], v3->get(0)->getDouble());
    ASSERT_EQ((double)buf15[1], v3->get(1)->getDouble());

    memset(buf9, '\0', 2);
    memset(buf10, 0, 2);
    memset(buf11, 0, 2);
    memset(buf12, 0, 2);
    memset(buf13, 0, 2);
    memset(buf14, 0, 2);
    memset(buf15, 0, 2);

    // std::cout<<v3->getString()<<std::endl;

    const char *resbuf = v3->getBoolConst(0, 2, buf9);
    ASSERT_EQ((bool)resbuf[0], v3->get(0)->getBool());
    ASSERT_EQ((bool)resbuf[1], v3->get(1)->getBool());

    const char *resbuf1 = v3->getCharConst(0, 2, buf9);
    ;
    ASSERT_EQ((char)resbuf1[0], v3->get(0)->getChar());
    ASSERT_EQ((char)resbuf1[1], v3->get(1)->getChar());

    const short *resbuf2 = v3->getShortConst(0, 2, buf10);
    ASSERT_EQ((short)resbuf2[0], v3->get(0)->getShort());
    ASSERT_EQ((short)resbuf2[1], v3->get(1)->getShort());

    const int *resbuf3 = v3->getIntConst(0, 2, buf11);
    ASSERT_EQ((int)resbuf3[0], v3->get(0)->getInt());
    ASSERT_EQ((int)resbuf3[1], v3->get(1)->getInt());

    const long long *resbuf4 = v3->getLongConst(0, 2, buf13);
    ASSERT_EQ((long long)resbuf4[0], v3->get(0)->getLong());
    ASSERT_EQ((long long)resbuf4[1], v3->get(1)->getLong());

    const dolphindb::INDEX *resbuf5 = v3->getIndexConst(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)resbuf5[0], v3->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)resbuf5[1], v3->get(1)->getIndex());

    const float *resbuf6 = v3->getFloatConst(0, 2, buf14);
    ASSERT_EQ((float)resbuf6[0], v3->get(0)->getFloat());
    ASSERT_EQ((float)resbuf6[1], v3->get(1)->getFloat());

    const double *resbuf7 = v3->getDoubleConst(0, 2, buf15);
    ASSERT_EQ((double)resbuf7[0], v3->get(0)->getDouble());
    ASSERT_EQ((double)resbuf7[1], v3->get(1)->getDouble());

    dolphindb::VectorSP errfillVals = conn.run("`str1`str2");
    ASSERT_ANY_THROW(v3->fill(0, 2, errfillVals));
    v3->fill(0, 2, dolphindb::Util::createNullConstant(dolphindb::DT_BOOL));
    ASSERT_EQ(v3->getString(), "[,]");
    ASSERT_FALSE(v3->append(dolphindb::Util::createString("aaa"), 0, 2));

    delete[] buf, buf1, buf2, buf3, buf4, buf5, buf6, buf7, buf9, buf10, buf11, buf12, buf13, buf14, buf15;
}

TEST_F(DataformVectorTest, testBoolNullVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_BOOL, 2, 2);
    v1->setNull(0);
    v1->set(1, dolphindb::Util::createNullConstant(dolphindb::DT_BOOL));
    std::string script = "a=[bool(NULL),bool(NULL)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
    for (int i = 0; i < 2; i++)
        ASSERT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());

    dolphindb::VectorSP v3 = dolphindb::Util::createVector(dolphindb::DT_BOOL, 2, 10, true, 0, (void *)0, true);
    v3->setNull(0);
    v3->setNull(1);
    char *buf9 = new char[2];
    short *buf10 = new short[2];
    int *buf11 = new int[2];
    dolphindb::INDEX *buf12 = new dolphindb::INDEX[2];
    long long *buf13 = new long long[2];
    float *buf14 = new float[2];
    double *buf15 = new double[2];

    v3->getBool(0, 2, buf9);
    ASSERT_EQ((bool)buf9[0], true);
    ASSERT_EQ((bool)buf9[1], true);
    v3->getChar(0, 2, buf9);
    ASSERT_EQ((char)buf9[0], v3->get(0)->getChar());
    ASSERT_EQ((char)buf9[1], v3->get(1)->getChar());
    v3->getShort(0, 2, buf10);
    ASSERT_EQ((short)buf10[0], v3->get(0)->getShort());
    ASSERT_EQ((short)buf10[1], v3->get(1)->getShort());
    v3->getInt(0, 2, buf11);
    ASSERT_EQ((int)buf11[0], v3->get(0)->getInt());
    ASSERT_EQ((int)buf11[1], v3->get(1)->getInt());
    v3->getLong(0, 2, buf13);
    ASSERT_EQ((long long)buf13[0], v3->get(0)->getLong());
    ASSERT_EQ((long long)buf13[1], v3->get(1)->getLong());
    v3->getIndex(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)buf12[0], v3->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)buf12[1], v3->get(1)->getIndex());
    v3->getFloat(0, 2, buf14);
    ASSERT_EQ((float)buf14[0], v3->get(0)->getFloat());
    ASSERT_EQ((float)buf14[1], v3->get(1)->getFloat());
    v3->getDouble(0, 2, buf15);
    ASSERT_EQ((double)buf15[0], v3->get(0)->getDouble());
    ASSERT_EQ((double)buf15[1], v3->get(1)->getDouble());

    memset(buf9, '\0', 2);
    memset(buf10, 0, 2);
    memset(buf11, 0, 2);
    memset(buf12, 0, 2);
    memset(buf13, 0, 2);
    memset(buf14, 0, 2);
    memset(buf15, 0, 2);

    // std::cout<<v3->getString()<<std::endl;

    const char *resbuf = v3->getBoolConst(0, 2, buf9);
    ASSERT_EQ((bool)resbuf[0], true);
    ASSERT_EQ((bool)resbuf[1], true);

    const char *resbuf1 = v3->getCharConst(0, 2, buf9);
    ;
    ASSERT_EQ((char)resbuf1[0], v3->get(0)->getChar());
    ASSERT_EQ((char)resbuf1[1], v3->get(1)->getChar());

    const short *resbuf2 = v3->getShortConst(0, 2, buf10);
    ASSERT_EQ((short)resbuf2[0], v3->get(0)->getShort());
    ASSERT_EQ((short)resbuf2[1], v3->get(1)->getShort());

    const int *resbuf3 = v3->getIntConst(0, 2, buf11);
    ASSERT_EQ((int)resbuf3[0], v3->get(0)->getInt());
    ASSERT_EQ((int)resbuf3[1], v3->get(1)->getInt());

    const long long *resbuf4 = v3->getLongConst(0, 2, buf13);
    ASSERT_EQ((long long)resbuf4[0], v3->get(0)->getLong());
    ASSERT_EQ((long long)resbuf4[1], v3->get(1)->getLong());

    const dolphindb::INDEX *resbuf5 = v3->getIndexConst(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)resbuf5[0], v3->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)resbuf5[1], v3->get(1)->getIndex());

    const float *resbuf6 = v3->getFloatConst(0, 2, buf14);
    ASSERT_EQ((float)resbuf6[0], v3->get(0)->getFloat());
    ASSERT_EQ((float)resbuf6[1], v3->get(1)->getFloat());

    const double *resbuf7 = v3->getDoubleConst(0, 2, buf15);
    ASSERT_EQ((double)resbuf7[0], v3->get(0)->getDouble());
    ASSERT_EQ((double)resbuf7[1], v3->get(1)->getDouble());

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
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_CHAR, 2, 2);
    v1->set(0, dolphindb::Util::createChar(1));
    v1->set(1, dolphindb::Util::createChar(0));
    std::string script = "a=[char(1),char(0)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
    ASSERT_EQ(v1->getCategory(), dolphindb::INTEGRAL);

    ASSERT_ANY_THROW(v1->fill(1, 1, dolphindb::Util::createString("1")));
    ASSERT_TRUE(v1->validIndex(1));
    ASSERT_TRUE(v1->validIndex(0, 1, 1));
    ASSERT_FALSE(v1->validIndex(0, 1, '\0'));

    dolphindb::VectorSP subv1 = dolphindb::Util::createVector(dolphindb::DT_CHAR, 0, 2);
    subv1->append(dolphindb::Util::createChar('a'));
    subv1->append(dolphindb::Util::createChar('c'));
    subv1->upper();
    ASSERT_EQ(subv1->get(0)->getChar(), 'A');
    ASSERT_EQ(subv1->get(1)->getChar(), 'C');
    subv1->lower();
    ASSERT_EQ(subv1->get(0)->getChar(), 'a');
    ASSERT_EQ(subv1->get(1)->getChar(), 'c');

    dolphindb::VectorSP indexVec = dolphindb::Util::createIndexVector(0, 1);
    dolphindb::ConstantSP index = dolphindb::Util::createInt(0);
    subv1->set(indexVec, dolphindb::Util::createChar(3));
    ASSERT_EQ(subv1->get(0)->getChar(), (char)3);
    subv1->set(index, dolphindb::Util::createChar(5));
    ASSERT_EQ(subv1->get(0)->getChar(), (char)5);
    subv1->set(index, dolphindb::Util::createNullConstant(dolphindb::DT_CHAR));
    ASSERT_TRUE(subv1->get(0)->isNull());

    char *buf = new char[10];
    char *buf_1 = new char[10];
    dolphindb::INDEX *buf_2 = new dolphindb::INDEX[2];
    buf_2[0] = 7;
    buf_2[1] = 9;
    // std::string *buf3 = new std::string[10];
    v1->nullFill(dolphindb::Util::createChar(3));
    ASSERT_EQ(v1->getString(), "[1,0]");
    v1->isNull(0, 2, buf);
    v1->isValid(0, 2, buf_1);
    ASSERT_FALSE((int)buf[0]);
    ASSERT_TRUE((int)buf_1[0]);

    v1->append(dolphindb::Util::createNullConstant(dolphindb::DT_CHAR));
    v1->nullFill(dolphindb::Util::createChar(9));
    v1->isNull(0, 3, buf);
    v1->isValid(0, 3, buf_1);
    ASSERT_FALSE((int)buf[0]);
    ASSERT_TRUE((int)buf_1[0]);
    ASSERT_EQ(v1->getString(), "[1,0,9]");
    v1->append(dolphindb::Util::createNullConstant(dolphindb::DT_CHAR));
    v1->nullFill(dolphindb::Util::createFloat(2.1));
    ASSERT_EQ(v1->getString(), "[1,0,9,2]");

    v1->getIndex(0, 2, buf_2);
    ASSERT_EQ(buf_2[0], 1);
    ASSERT_EQ(v1->getIndexConst(0, 2, buf_2)[0], 1);
    ASSERT_EQ(v1->getIndexBuffer(0, 2, buf_2)[0], 1);
    // std::cout<<v1->getString(0,2,buf3)<<std::endl;
    v1->setIndex(0, 2, buf_2);
    ASSERT_EQ(buf_2[0], 1);

    delete[] buf_1, buf_2, buf;

    dolphindb::VectorSP v2 = dolphindb::Util::createVector(dolphindb::DT_CHAR, 2, 10);
    v2->set(0, dolphindb::Util::createChar(1));
    v2->set(1, dolphindb::Util::createChar(0));
    char *buf1 = new char[2];
    buf1[0] = 1;
    short *buf2 = new short[2];
    buf2[0] = 1;
    int *buf3 = new int[2];
    buf3[0] = 1;
    dolphindb::INDEX *buf4 = new dolphindb::INDEX[2];
    buf4[0] = 1;
    long long *buf5 = new long long[2];
    buf5[0] = 1;
    float *buf6 = new float[2];
    buf6[0] = 1;
    double *buf7 = new double[2];
    buf7[0] = 1;

    // std::cout<<v2->getString()<<std::endl;
    v2->appendBool(buf1, 1);
    ASSERT_EQ(v2->getString(), "[1,0,1]");
    v2->appendChar(buf1, 1);
    ASSERT_EQ(v2->getString(), "[1,0,1,1]");
    v2->appendShort(buf2, 1);
    ASSERT_EQ(v2->getString(), "[1,0,1,1,1]");
    v2->appendInt(buf3, 1);
    ASSERT_EQ(v2->getString(), "[1,0,1,1,1,1]");
    v2->appendLong(buf5, 1);
    ASSERT_EQ(v2->getString(), "[1,0,1,1,1,1,1]");
    v2->appendIndex(buf4, 1);
    ASSERT_EQ(v2->getString(), "[1,0,1,1,1,1,1,1]");
    v2->appendFloat(buf6, 1);
    ASSERT_EQ(v2->getString(), "[1,0,1,1,1,1,1,1,1]");
    v2->appendDouble(buf7, 1);
    ASSERT_EQ(v2->getString(), "[1,0,1,1,1,1,1,1,1,1]");

    dolphindb::VectorSP v3 = dolphindb::Util::createVector(dolphindb::DT_CHAR, 2, 10);
    v3->set(0, dolphindb::Util::createChar(1));
    v3->set(1, dolphindb::Util::createChar(0));
    char *buf9 = new char[2];
    short *buf10 = new short[2];
    int *buf11 = new int[2];
    dolphindb::INDEX *buf12 = new dolphindb::INDEX[2];
    long long *buf13 = new long long[2];
    float *buf14 = new float[2];
    double *buf15 = new double[2];

    v3->getBool(0, 2, buf9);
    ASSERT_EQ((bool)buf9[0], v3->get(0)->getBool());
    ASSERT_EQ((bool)buf9[1], v3->get(1)->getBool());
    v3->getChar(0, 2, buf9);
    ASSERT_EQ((char)buf9[0], v3->get(0)->getChar());
    ASSERT_EQ((char)buf9[1], v3->get(1)->getChar());
    v3->getShort(0, 2, buf10);
    ASSERT_EQ((short)buf10[0], v3->get(0)->getShort());
    ASSERT_EQ((short)buf10[1], v3->get(1)->getShort());
    v3->getInt(0, 2, buf11);
    ASSERT_EQ((int)buf11[0], v3->get(0)->getInt());
    ASSERT_EQ((int)buf11[1], v3->get(1)->getInt());
    v3->getLong(0, 2, buf13);
    ASSERT_EQ((long long)buf13[0], v3->get(0)->getLong());
    ASSERT_EQ((long long)buf13[1], v3->get(1)->getLong());
    v3->getIndex(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)buf12[0], v3->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)buf12[1], v3->get(1)->getIndex());
    v3->getFloat(0, 2, buf14);
    ASSERT_EQ((float)buf14[0], v3->get(0)->getFloat());
    ASSERT_EQ((float)buf14[1], v3->get(1)->getFloat());
    v3->getDouble(0, 2, buf15);
    ASSERT_EQ((double)buf15[0], v3->get(0)->getDouble());
    ASSERT_EQ((double)buf15[1], v3->get(1)->getDouble());

    memset(buf9, '\0', 2);
    memset(buf10, 0, 2);
    memset(buf11, 0, 2);
    memset(buf12, 0, 2);
    memset(buf13, 0, 2);
    memset(buf14, 0, 2);
    memset(buf15, 0, 2);

    // std::cout<<v3->getString()<<std::endl;

    const char *resbuf = v3->getBoolConst(0, 2, buf9);
    ASSERT_EQ((bool)resbuf[0], v3->get(0)->getBool());
    ASSERT_EQ((bool)resbuf[1], v3->get(1)->getBool());

    const char *resbuf1 = v3->getCharConst(0, 2, buf9);
    ;
    ASSERT_EQ((char)resbuf1[0], v3->get(0)->getChar());
    ASSERT_EQ((char)resbuf1[1], v3->get(1)->getChar());

    const short *resbuf2 = v3->getShortConst(0, 2, buf10);
    ASSERT_EQ((short)resbuf2[0], v3->get(0)->getShort());
    ASSERT_EQ((short)resbuf2[1], v3->get(1)->getShort());

    const int *resbuf3 = v3->getIntConst(0, 2, buf11);
    ASSERT_EQ((int)resbuf3[0], v3->get(0)->getInt());
    ASSERT_EQ((int)resbuf3[1], v3->get(1)->getInt());

    const long long *resbuf4 = v3->getLongConst(0, 2, buf13);
    ASSERT_EQ((long long)resbuf4[0], v3->get(0)->getLong());
    ASSERT_EQ((long long)resbuf4[1], v3->get(1)->getLong());

    const dolphindb::INDEX *resbuf5 = v3->getIndexConst(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)resbuf5[0], v3->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)resbuf5[1], v3->get(1)->getIndex());

    const float *resbuf6 = v3->getFloatConst(0, 2, buf14);
    ASSERT_EQ((float)resbuf6[0], v3->get(0)->getFloat());
    ASSERT_EQ((float)resbuf6[1], v3->get(1)->getFloat());

    const double *resbuf7 = v3->getDoubleConst(0, 2, buf15);
    ASSERT_EQ((double)resbuf7[0], v3->get(0)->getDouble());
    ASSERT_EQ((double)resbuf7[1], v3->get(1)->getDouble());

    delete[] buf, buf1, buf2, buf3, buf4, buf5, buf6, buf7, buf9, buf10, buf11, buf12, buf13, buf14, buf15;

    dolphindb::VectorSP errfillVals = conn.run("`str1`str2");
    ASSERT_ANY_THROW(v3->fill(0, 2, errfillVals));
    v3->fill(0, 2, dolphindb::Util::createNullConstant(dolphindb::DT_CHAR));
    ASSERT_EQ(v3->getString(), "[,]");
    ASSERT_FALSE(v3->append(dolphindb::Util::createString("aaa"), 0, 2));
}

TEST_F(DataformVectorTest, testCharNullVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_CHAR, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
    std::string script = "a=[char(NULL),char(NULL)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
    for (int i = 0; i < 2; i++)
        ASSERT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());

    dolphindb::VectorSP v3 = dolphindb::Util::createVector(dolphindb::DT_CHAR, 2, 10, true, 0, (void *)0, true);
    v3->setNull(0);
    v3->setNull(1);
    char *buf9 = new char[2];
    short *buf10 = new short[2];
    int *buf11 = new int[2];
    dolphindb::INDEX *buf12 = new dolphindb::INDEX[2];
    long long *buf13 = new long long[2];
    float *buf14 = new float[2];
    double *buf15 = new double[2];

    v3->getBool(0, 2, buf9);
    ASSERT_EQ((bool)buf9[0], true);
    ASSERT_EQ((bool)buf9[1], true);
    v3->getChar(0, 2, buf9);
    ASSERT_EQ((char)buf9[0], v3->get(0)->getChar());
    ASSERT_EQ((char)buf9[1], v3->get(1)->getChar());
    v3->getShort(0, 2, buf10);
    ASSERT_EQ((short)buf10[0], v3->get(0)->getShort());
    ASSERT_EQ((short)buf10[1], v3->get(1)->getShort());
    v3->getInt(0, 2, buf11);
    ASSERT_EQ((int)buf11[0], v3->get(0)->getInt());
    ASSERT_EQ((int)buf11[1], v3->get(1)->getInt());
    v3->getLong(0, 2, buf13);
    ASSERT_EQ((long long)buf13[0], v3->get(0)->getLong());
    ASSERT_EQ((long long)buf13[1], v3->get(1)->getLong());
    v3->getIndex(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)buf12[0], v3->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)buf12[1], v3->get(1)->getIndex());
    v3->getFloat(0, 2, buf14);
    ASSERT_EQ((float)buf14[0], v3->get(0)->getFloat());
    ASSERT_EQ((float)buf14[1], v3->get(1)->getFloat());
    v3->getDouble(0, 2, buf15);
    ASSERT_EQ((double)buf15[0], v3->get(0)->getDouble());
    ASSERT_EQ((double)buf15[1], v3->get(1)->getDouble());

    memset(buf9, '\0', 2);
    memset(buf10, 0, 2);
    memset(buf11, 0, 2);
    memset(buf12, 0, 2);
    memset(buf13, 0, 2);
    memset(buf14, 0, 2);
    memset(buf15, 0, 2);

    // std::cout<<v3->getString()<<std::endl;

    const char *resbuf = v3->getBoolConst(0, 2, buf9);
    ASSERT_EQ((bool)resbuf[0], true);
    ASSERT_EQ((bool)resbuf[1], true);

    const char *resbuf1 = v3->getCharConst(0, 2, buf9);
    ;
    ASSERT_EQ((char)resbuf1[0], v3->get(0)->getChar());
    ASSERT_EQ((char)resbuf1[1], v3->get(1)->getChar());

    const short *resbuf2 = v3->getShortConst(0, 2, buf10);
    ASSERT_EQ((short)resbuf2[0], v3->get(0)->getShort());
    ASSERT_EQ((short)resbuf2[1], v3->get(1)->getShort());

    const int *resbuf3 = v3->getIntConst(0, 2, buf11);
    ASSERT_EQ((int)resbuf3[0], v3->get(0)->getInt());
    ASSERT_EQ((int)resbuf3[1], v3->get(1)->getInt());

    const long long *resbuf4 = v3->getLongConst(0, 2, buf13);
    ASSERT_EQ((long long)resbuf4[0], v3->get(0)->getLong());
    ASSERT_EQ((long long)resbuf4[1], v3->get(1)->getLong());

    const dolphindb::INDEX *resbuf5 = v3->getIndexConst(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)resbuf5[0], v3->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)resbuf5[1], v3->get(1)->getIndex());

    const float *resbuf6 = v3->getFloatConst(0, 2, buf14);
    ASSERT_EQ((float)resbuf6[0], v3->get(0)->getFloat());
    ASSERT_EQ((float)resbuf6[1], v3->get(1)->getFloat());

    const double *resbuf7 = v3->getDoubleConst(0, 2, buf15);
    ASSERT_EQ((double)resbuf7[0], v3->get(0)->getDouble());
    ASSERT_EQ((double)resbuf7[1], v3->get(1)->getDouble());

    delete[] buf9, buf10, buf11, buf12, buf13, buf14, buf15;
}

TEST_F(DataformVectorTest, testIntVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_INT, 2, 2);
    v1->set(0, dolphindb::Util::createInt(1));
    v1->set(1, dolphindb::Util::createInt(0));
    std::string script = "a=[int(1),int(0)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
    v1->append(dolphindb::Util::createInt(2));
    ASSERT_FALSE(v1->isSorted(false));
    ASSERT_FALSE(v1->isSorted(false, true));
    ASSERT_FALSE(v1->hasNull(0, 2));

    ASSERT_ANY_THROW(v1->fill(1, 1, dolphindb::Util::createString("1")));
    ASSERT_TRUE(v1->validIndex(2));
    ASSERT_TRUE(v1->validIndex(0, 1, 1));
    ASSERT_FALSE(v1->validIndex(0, 1, (int)0));

    dolphindb::VectorSP subv1 = dolphindb::Util::createVector(dolphindb::DT_INT, 0, 2);
    subv1->append(dolphindb::Util::createInt(1));
    subv1->append(dolphindb::Util::createInt(2));
    ASSERT_FALSE(v1->compare(0, dolphindb::Util::createInt(1)));

    dolphindb::VectorSP indexVec = dolphindb::Util::createIndexVector(0, 1);
    dolphindb::ConstantSP index = dolphindb::Util::createInt(0);
    subv1->set(indexVec, dolphindb::Util::createInt(3));
    ASSERT_EQ(subv1->get(0)->getInt(), (int)3);
    subv1->set(index, dolphindb::Util::createInt(5));
    ASSERT_EQ(subv1->get(0)->getInt(), (int)5);
    subv1->set(index, dolphindb::Util::createNullConstant(dolphindb::DT_INT));
    ASSERT_TRUE(subv1->get(0)->isNull());
    subv1->set(1, dolphindb::Util::createNullConstant(dolphindb::DT_INT));
    ASSERT_TRUE(subv1->get(1)->isNull());

    dolphindb::VectorSP v2 = dolphindb::Util::createVector(dolphindb::DT_INT, 1, 1);
    v2->set(0, dolphindb::Util::createNullConstant(dolphindb::DT_INT));
    ASSERT_EQ(v2->getBool(), CHAR_MIN);
    ASSERT_EQ(v2->getChar(), CHAR_MIN);
    ASSERT_EQ(v2->getShort(), SHRT_MIN);
    ASSERT_EQ(v2->getInt(), INT_MIN);
    ASSERT_EQ(v2->getIndex(), dolphindb::INDEX_MIN);
    ASSERT_EQ(v2->getLong(), LLONG_MIN);
    ASSERT_EQ(v2->getDouble(), dolphindb::DBL_NMIN);
    ASSERT_EQ(v2->getFloat(), dolphindb::FLT_NMIN);

    v2->set(0, dolphindb::Util::createInt(0));
    ASSERT_FALSE(v2->getBool());
    ASSERT_EQ(v2->getChar(), (char)0);
    ASSERT_EQ(v2->getShort(), (short)0);
    ASSERT_EQ(v2->getInt(), (int)0);
    ASSERT_EQ(v2->getIndex(), (dolphindb::INDEX)0);
    ASSERT_EQ(v2->getLong(), (long long)0);
    ASSERT_EQ(v2->getDouble(), (double)0);
    ASSERT_EQ(v2->getFloat(), (float)0);

    ASSERT_ANY_THROW(v1->getBool());
    ASSERT_ANY_THROW(v1->getChar());
    ASSERT_ANY_THROW(v1->getShort());
    ASSERT_ANY_THROW(v1->getInt());
    ASSERT_ANY_THROW(v1->getIndex());
    ASSERT_ANY_THROW(v1->getLong());
    ASSERT_ANY_THROW(v1->getDouble());
    ASSERT_ANY_THROW(v1->getFloat());

    v2->setBool(1);
    ASSERT_EQ(v2->getBool(0), (bool)1);
    v2->setChar('\2');
    ASSERT_EQ(v2->get(0)->getChar(), (char)2);
    v2->setShort(3);
    ASSERT_EQ(v2->get(0)->getShort(), (short)3);
    v2->setInt(4);
    ASSERT_EQ(v2->get(0)->getInt(), (int)4);
    v2->setLong(5);
    ASSERT_EQ(v2->get(0)->getLong(), (long long)5);
    v2->setIndex(6);
    ASSERT_EQ(v2->get(0)->getIndex(), (dolphindb::INDEX)6);
    v2->setFloat(7.11);
    ASSERT_EQ(v2->get(0)->getFloat(), (float)7);
    v2->setDouble(-8.1);
    ASSERT_EQ(v2->get(0)->getDouble(), (double)-8);
    v2->setString("9");
    ASSERT_EQ(v2->get(0)->getString(), "-8"); // setString not effected

    v2->setBool(0, 11);
    ASSERT_EQ(v2->getBool(0), (bool)1);
    v2->setChar(0, '\0');
    ASSERT_EQ(v2->get(0)->getChar(), (char)0);
    v2->setShort(0, 33);
    ASSERT_EQ(v2->get(0)->getShort(), (short)33);
    v2->setInt(0, 44);
    ASSERT_EQ(v2->get(0)->getInt(), (int)44);
    v2->setLong(0, 55);
    ASSERT_EQ(v2->get(0)->getLong(), (long long)55);
    v2->setIndex(0, 66);
    ASSERT_EQ(v2->get(0)->getIndex(), (dolphindb::INDEX)66);
    v2->setFloat(0, -7.11);
    ASSERT_EQ(v2->get(0)->getFloat(), (float)-7);
    v2->setDouble(0, 8.1);
    ASSERT_EQ(v2->get(0)->getDouble(), (double)8);
    v2->setString(0, "99");
    ASSERT_EQ(v2->get(0)->getString(), "8"); // setString not effected


    dolphindb::VectorSP v3 = dolphindb::Util::createVector(dolphindb::DT_INT, 2, 2);
    v3->setNull(); // code nothing to do
    v3->set(0, dolphindb::Util::createInt(0));
    v3->set(1, dolphindb::Util::createInt(1));
    v3->remove(dolphindb::Util::createInt(1000));

    v3->next(-1);
    v3->next(1000);
    v3->next(1);
    ASSERT_EQ(v3->getString(), "[1,]");
    v3->prev(-1);
    v3->prev(1000);
    v3->prev(1);
    ASSERT_EQ(v3->getString(), "[,1]");

    dolphindb::VectorSP v4 = dolphindb::Util::createVector(dolphindb::DT_INT, 2, 10);
    v4->set(0, dolphindb::Util::createInt(1));
    v4->set(1, dolphindb::Util::createInt(0));
    char *buf_1 = new char[2];
    buf_1[0] = 1;
    short *buf_2 = new short[2];
    buf_2[0] = 1;
    int *buf_3 = new int[2];
    buf_3[0] = 1;
    dolphindb::INDEX *buf_4 = new dolphindb::INDEX[2];
    buf_4[0] = 1;
    long long *buf_5 = new long long[2];
    buf_5[0] = 1;
    float *buf_6 = new float[2];
    buf_6[0] = 1;
    double *buf_7 = new double[2];
    buf_7[0] = 1;

    // std::cout<<v4->getString()<<std::endl;
    v4->appendBool(buf_1, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1]");
    v4->appendChar(buf_1, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1]");
    v4->appendShort(buf_2, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1]");
    v4->appendInt(buf_3, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1,1]");
    v4->appendLong(buf_5, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1,1,1]");
    v4->appendIndex(buf_4, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1]");
    v4->appendFloat(buf_6, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1,1]");
    v4->appendDouble(buf_7, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1,1,1]");

    dolphindb::VectorSP v5 = dolphindb::Util::createVector(dolphindb::DT_INT, 2, 10);
    v5->set(0, dolphindb::Util::createInt(1));
    v5->set(1, dolphindb::Util::createInt(0));
    char *buf9 = new char[2];
    short *buf10 = new short[2];
    int *buf11 = new int[2];
    dolphindb::INDEX *buf12 = new dolphindb::INDEX[2];
    long long *buf13 = new long long[2];
    float *buf14 = new float[2];
    double *buf15 = new double[2];

    v5->getBool(0, 2, buf9);
    ASSERT_EQ((bool)buf9[0], v5->get(0)->getBool());
    ASSERT_EQ((bool)buf9[1], v5->get(1)->getBool());
    v5->getChar(0, 2, buf9);
    ASSERT_EQ((char)buf9[0], v5->get(0)->getChar());
    ASSERT_EQ((char)buf9[1], v5->get(1)->getChar());
    v5->getShort(0, 2, buf10);
    ASSERT_EQ((short)buf10[0], v5->get(0)->getShort());
    ASSERT_EQ((short)buf10[1], v5->get(1)->getShort());
    v5->getInt(0, 2, buf11);
    ASSERT_EQ((int)buf11[0], v5->get(0)->getInt());
    ASSERT_EQ((int)buf11[1], v5->get(1)->getInt());
    v5->getLong(0, 2, buf13);
    ASSERT_EQ((long long)buf13[0], v5->get(0)->getLong());
    ASSERT_EQ((long long)buf13[1], v5->get(1)->getLong());
    v5->getIndex(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)buf12[0], v5->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)buf12[1], v5->get(1)->getIndex());
    v5->getFloat(0, 2, buf14);
    ASSERT_EQ((float)buf14[0], v5->get(0)->getFloat());
    ASSERT_EQ((float)buf14[1], v5->get(1)->getFloat());
    v5->getDouble(0, 2, buf15);
    ASSERT_EQ((double)buf15[0], v5->get(0)->getDouble());
    ASSERT_EQ((double)buf15[1], v5->get(1)->getDouble());

    memset(buf9, '\0', 2);
    memset(buf10, 0, 2);
    memset(buf11, 0, 2);
    memset(buf12, 0, 2);
    memset(buf13, 0, 2);
    memset(buf14, 0, 2);
    memset(buf15, 0, 2);

    // std::cout<<v5->getString()<<std::endl;

    const char *resbuf = v5->getBoolConst(0, 2, buf9);
    ASSERT_EQ((bool)resbuf[0], v5->get(0)->getBool());
    ASSERT_EQ((bool)resbuf[1], v5->get(1)->getBool());

    const char *resbuf1 = v5->getCharConst(0, 2, buf9);
    ;
    ASSERT_EQ((char)resbuf1[0], v5->get(0)->getChar());
    ASSERT_EQ((char)resbuf1[1], v5->get(1)->getChar());

    const short *resbuf2 = v5->getShortConst(0, 2, buf10);
    ASSERT_EQ((short)resbuf2[0], v5->get(0)->getShort());
    ASSERT_EQ((short)resbuf2[1], v5->get(1)->getShort());

    const int *resbuf3 = v5->getIntConst(0, 2, buf11);
    ASSERT_EQ((int)resbuf3[0], v5->get(0)->getInt());
    ASSERT_EQ((int)resbuf3[1], v5->get(1)->getInt());

    const long long *resbuf4 = v5->getLongConst(0, 2, buf13);
    ASSERT_EQ((long long)resbuf4[0], v5->get(0)->getLong());
    ASSERT_EQ((long long)resbuf4[1], v5->get(1)->getLong());

    const dolphindb::INDEX *resbuf5 = v5->getIndexConst(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)resbuf5[0], v5->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)resbuf5[1], v5->get(1)->getIndex());

    const float *resbuf6 = v5->getFloatConst(0, 2, buf14);
    ASSERT_EQ((float)resbuf6[0], v5->get(0)->getFloat());
    ASSERT_EQ((float)resbuf6[1], v5->get(1)->getFloat());

    const double *resbuf7 = v5->getDoubleConst(0, 2, buf15);
    ASSERT_EQ((double)resbuf7[0], v5->get(0)->getDouble());
    ASSERT_EQ((double)resbuf7[1], v5->get(1)->getDouble());

    delete[] buf_1, buf_2, buf_3, buf_4, buf_5, buf_6, buf_7, buf9, buf10, buf11, buf12, buf13, buf14, buf15;

    dolphindb::VectorSP errfillVals = conn.run("`str1`str2");
    ASSERT_ANY_THROW(v3->fill(0, 2, errfillVals));
    v3->fill(0, 2, dolphindb::Util::createNullConstant(dolphindb::DT_INT));
    ASSERT_EQ(v3->getString(), "[,]");
    ASSERT_FALSE(v3->append(dolphindb::Util::createString("aaa"), 0, 2));
}

TEST_F(DataformVectorTest, testIntNullVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_INT, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
    std::string script = "a=[int(NULL),int(NULL)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
    for (int i = 0; i < 2; i++)
        ASSERT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());

    dolphindb::VectorSP v3 = dolphindb::Util::createVector(dolphindb::DT_INT, 2, 10, true, 0, (void *)0, true);
    v3->setNull(0);
    v3->setNull(1);
    char *buf9 = new char[2];
    short *buf10 = new short[2];
    int *buf11 = new int[2];
    dolphindb::INDEX *buf12 = new dolphindb::INDEX[2];
    long long *buf13 = new long long[2];
    float *buf14 = new float[2];
    double *buf15 = new double[2];

    v3->getBool(0, 2, buf9);
    ASSERT_EQ((bool)buf9[0], true);
    ASSERT_EQ((bool)buf9[1], true);
    v3->getChar(0, 2, buf9);
    ASSERT_EQ((char)buf9[0], v3->get(0)->getChar());
    ASSERT_EQ((char)buf9[1], v3->get(1)->getChar());
    v3->getShort(0, 2, buf10);
    ASSERT_EQ((short)buf10[0], v3->get(0)->getShort());
    ASSERT_EQ((short)buf10[1], v3->get(1)->getShort());
    v3->getInt(0, 2, buf11);
    ASSERT_EQ((int)buf11[0], v3->get(0)->getInt());
    ASSERT_EQ((int)buf11[1], v3->get(1)->getInt());
    v3->getLong(0, 2, buf13);
    ASSERT_EQ((long long)buf13[0], v3->get(0)->getLong());
    ASSERT_EQ((long long)buf13[1], v3->get(1)->getLong());
    v3->getIndex(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)buf12[0], v3->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)buf12[1], v3->get(1)->getIndex());
    v3->getFloat(0, 2, buf14);
    ASSERT_EQ((float)buf14[0], v3->get(0)->getFloat());
    ASSERT_EQ((float)buf14[1], v3->get(1)->getFloat());
    v3->getDouble(0, 2, buf15);
    ASSERT_EQ((double)buf15[0], v3->get(0)->getDouble());
    ASSERT_EQ((double)buf15[1], v3->get(1)->getDouble());

    memset(buf9, '\0', 2);
    memset(buf10, 0, 2);
    memset(buf11, 0, 2);
    memset(buf12, 0, 2);
    memset(buf13, 0, 2);
    memset(buf14, 0, 2);
    memset(buf15, 0, 2);

    // std::cout<<v3->getString()<<std::endl;

    const char *resbuf = v3->getBoolConst(0, 2, buf9);
    ASSERT_EQ((bool)resbuf[0], true);
    ASSERT_EQ((bool)resbuf[1], true);

    const char *resbuf1 = v3->getCharConst(0, 2, buf9);
    ;
    ASSERT_EQ((char)resbuf1[0], v3->get(0)->getChar());
    ASSERT_EQ((char)resbuf1[1], v3->get(1)->getChar());

    const short *resbuf2 = v3->getShortConst(0, 2, buf10);
    ASSERT_EQ((short)resbuf2[0], v3->get(0)->getShort());
    ASSERT_EQ((short)resbuf2[1], v3->get(1)->getShort());

    const int *resbuf3 = v3->getIntConst(0, 2, buf11);
    ASSERT_EQ((int)resbuf3[0], v3->get(0)->getInt());
    ASSERT_EQ((int)resbuf3[1], v3->get(1)->getInt());

    const long long *resbuf4 = v3->getLongConst(0, 2, buf13);
    ASSERT_EQ((long long)resbuf4[0], v3->get(0)->getLong());
    ASSERT_EQ((long long)resbuf4[1], v3->get(1)->getLong());

    const dolphindb::INDEX *resbuf5 = v3->getIndexConst(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)resbuf5[0], v3->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)resbuf5[1], v3->get(1)->getIndex());

    const float *resbuf6 = v3->getFloatConst(0, 2, buf14);
    ASSERT_EQ((float)resbuf6[0], v3->get(0)->getFloat());
    ASSERT_EQ((float)resbuf6[1], v3->get(1)->getFloat());

    const double *resbuf7 = v3->getDoubleConst(0, 2, buf15);
    ASSERT_EQ((double)resbuf7[0], v3->get(0)->getDouble());
    ASSERT_EQ((double)resbuf7[1], v3->get(1)->getDouble());

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
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_LONG, 2, 2);
    v1->set(0, dolphindb::Util::createLong(1));
    v1->set(1, dolphindb::Util::createLong(0));
    std::string script = "a=[long(1),long(0)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());

    ASSERT_ANY_THROW(v1->fill(1, 1, dolphindb::Util::createString("1")));
    ASSERT_TRUE(v1->validIndex(2));
    ASSERT_TRUE(v1->validIndex(0, 1, 1));
    ASSERT_FALSE(v1->validIndex(0, 1, (long long)0));

    dolphindb::VectorSP subv1 = dolphindb::Util::createVector(dolphindb::DT_LONG, 0, 2);
    subv1->append(dolphindb::Util::createLong(1));
    subv1->append(dolphindb::Util::createLong(2));
    ASSERT_EQ(v1->compare(0, dolphindb::Util::createLong(1)), 0);
    ASSERT_EQ(v1->compare(0, dolphindb::Util::createLong(0)), 1);
    ASSERT_EQ(v1->compare(0, dolphindb::Util::createLong(3)), -1);

    dolphindb::VectorSP indexVec = dolphindb::Util::createIndexVector(0, 1);
    dolphindb::ConstantSP index = dolphindb::Util::createLong(0);
    subv1->set(indexVec, dolphindb::Util::createLong(3));
    ASSERT_EQ(subv1->get(0)->getLong(), (long long)3);
    subv1->set(index, dolphindb::Util::createLong(5));
    ASSERT_EQ(subv1->get(0)->getLong(), (long long)5);
    subv1->set(index, dolphindb::Util::createNullConstant(dolphindb::DT_LONG));
    ASSERT_TRUE(subv1->get(0)->isNull());
    subv1->set(1, dolphindb::Util::createNullConstant(dolphindb::DT_LONG));
    ASSERT_TRUE(subv1->get(1)->isNull());

    v1->add(0, 2, (long long)1);
    ASSERT_EQ(v1->getString(), "[2,1]");
    v1->neg();
    ASSERT_EQ(v1->getString(), "[-2,-1]");
    v1->append(dolphindb::Util::createNullConstant(dolphindb::DT_LONG));
    v1->replace(dolphindb::Util::createNullConstant(dolphindb::DT_LONG), dolphindb::Util::createLong(4));
    v1->replace(dolphindb::Util::createLong(-2), dolphindb::Util::createNullConstant(dolphindb::DT_LONG));
    v1->replace(dolphindb::Util::createLong(-1), dolphindb::Util::createLong(3));
    ASSERT_EQ(v1->getString(), "[,3,4]");

    dolphindb::VectorSP v4 = dolphindb::Util::createVector(dolphindb::DT_LONG, 2, 10);
    v4->set(0, dolphindb::Util::createLong(1));
    v4->set(1, dolphindb::Util::createLong(0));
    char *buf_1 = new char[2];
    buf_1[0] = 1;
    short *buf_2 = new short[2];
    buf_2[0] = 1;
    int *buf_3 = new int[2];
    buf_3[0] = 1;
    dolphindb::INDEX *buf_4 = new dolphindb::INDEX[2];
    buf_4[0] = 1;
    long long *buf_5 = new long long[2];
    buf_5[0] = 1;
    float *buf_6 = new float[2];
    buf_6[0] = 1;
    double *buf_7 = new double[2];
    buf_7[0] = 1;

    // std::cout<<v4->getString()<<std::endl;
    v4->appendBool(buf_1, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1]");
    v4->appendChar(buf_1, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1]");
    v4->appendShort(buf_2, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1]");
    v4->appendInt(buf_3, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1,1]");
    v4->appendLong(buf_5, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1,1,1]");
    v4->appendIndex(buf_4, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1]");
    v4->appendFloat(buf_6, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1,1]");
    v4->appendDouble(buf_7, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1,1,1]");

    dolphindb::VectorSP v5 = dolphindb::Util::createVector(dolphindb::DT_LONG, 2, 10);
    v5->set(0, dolphindb::Util::createLong(1));
    v5->set(1, dolphindb::Util::createLong(0));
    char *buf9 = new char[2];
    short *buf10 = new short[2];
    int *buf11 = new int[2];
    dolphindb::INDEX *buf12 = new dolphindb::INDEX[2];
    long long *buf13 = new long long[2];
    float *buf14 = new float[2];
    double *buf15 = new double[2];

    v5->getBool(0, 2, buf9);
    ASSERT_EQ((bool)buf9[0], v5->get(0)->getBool());
    ASSERT_EQ((bool)buf9[1], v5->get(1)->getBool());
    v5->getChar(0, 2, buf9);
    ASSERT_EQ((char)buf9[0], v5->get(0)->getChar());
    ASSERT_EQ((char)buf9[1], v5->get(1)->getChar());
    v5->getShort(0, 2, buf10);
    ASSERT_EQ((short)buf10[0], v5->get(0)->getShort());
    ASSERT_EQ((short)buf10[1], v5->get(1)->getShort());
    v5->getInt(0, 2, buf11);
    ASSERT_EQ((int)buf11[0], v5->get(0)->getInt());
    ASSERT_EQ((int)buf11[1], v5->get(1)->getInt());
    v5->getLong(0, 2, buf13);
    ASSERT_EQ((long long)buf13[0], v5->get(0)->getLong());
    ASSERT_EQ((long long)buf13[1], v5->get(1)->getLong());
    v5->getIndex(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)buf12[0], v5->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)buf12[1], v5->get(1)->getIndex());
    v5->getFloat(0, 2, buf14);
    ASSERT_EQ((float)buf14[0], v5->get(0)->getFloat());
    ASSERT_EQ((float)buf14[1], v5->get(1)->getFloat());
    v5->getDouble(0, 2, buf15);
    ASSERT_EQ((double)buf15[0], v5->get(0)->getDouble());
    ASSERT_EQ((double)buf15[1], v5->get(1)->getDouble());

    memset(buf9, '\0', 2);
    memset(buf10, 0, 2);
    memset(buf11, 0, 2);
    memset(buf12, 0, 2);
    memset(buf13, 0, 2);
    memset(buf14, 0, 2);
    memset(buf15, 0, 2);

    // std::cout<<v5->getString()<<std::endl;

    const char *resbuf = v5->getBoolConst(0, 2, buf9);
    ASSERT_EQ((bool)resbuf[0], v5->get(0)->getBool());
    ASSERT_EQ((bool)resbuf[1], v5->get(1)->getBool());

    const char *resbuf1 = v5->getCharConst(0, 2, buf9);
    ;
    ASSERT_EQ((char)resbuf1[0], v5->get(0)->getChar());
    ASSERT_EQ((char)resbuf1[1], v5->get(1)->getChar());

    const short *resbuf2 = v5->getShortConst(0, 2, buf10);
    ASSERT_EQ((short)resbuf2[0], v5->get(0)->getShort());
    ASSERT_EQ((short)resbuf2[1], v5->get(1)->getShort());

    const int *resbuf3 = v5->getIntConst(0, 2, buf11);
    ASSERT_EQ((int)resbuf3[0], v5->get(0)->getInt());
    ASSERT_EQ((int)resbuf3[1], v5->get(1)->getInt());

    const long long *resbuf4 = v5->getLongConst(0, 2, buf13);
    ASSERT_EQ((long long)resbuf4[0], v5->get(0)->getLong());
    ASSERT_EQ((long long)resbuf4[1], v5->get(1)->getLong());

    const dolphindb::INDEX *resbuf5 = v5->getIndexConst(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)resbuf5[0], v5->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)resbuf5[1], v5->get(1)->getIndex());

    const float *resbuf6 = v5->getFloatConst(0, 2, buf14);
    ASSERT_EQ((float)resbuf6[0], v5->get(0)->getFloat());
    ASSERT_EQ((float)resbuf6[1], v5->get(1)->getFloat());

    const double *resbuf7 = v5->getDoubleConst(0, 2, buf15);
    ASSERT_EQ((double)resbuf7[0], v5->get(0)->getDouble());
    ASSERT_EQ((double)resbuf7[1], v5->get(1)->getDouble());

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

    dolphindb::VectorSP v6 = dolphindb::Util::createVector(dolphindb::DT_LONG, 2, 2);
    v6->set(0, dolphindb::Util::createLong(1));
    v6->set(1, dolphindb::Util::createLong(0));

    dolphindb::VectorSP errfillVals = conn.run("`str1`str2");
    ASSERT_ANY_THROW(v5->fill(0, 2, errfillVals));
    v5->fill(0, 2, dolphindb::Util::createNullConstant(dolphindb::DT_LONG));
    ASSERT_EQ(v5->getString(), "[,]");
    ASSERT_FALSE(v5->append(dolphindb::Util::createString("aaa"), 0, 2));
}

TEST_F(DataformVectorTest, testLongNullVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_LONG, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
    std::string script = "a=[long(NULL),long(NULL)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
    for (int i = 0; i < 2; i++)
        ASSERT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
    v1->add(0, 2, (long long)1);
    ASSERT_EQ(v1->getString(), "[" + std::to_string(LLONG_MIN + 1) + "," + std::to_string(LLONG_MIN + 1) + "]");
    v1->set(0, dolphindb::Util::createLong(1));
    v1->neg();
    ASSERT_EQ(v1->getString(), "[-1," + std::to_string(LLONG_MAX) + "]");

    dolphindb::VectorSP v3 = dolphindb::Util::createVector(dolphindb::DT_LONG, 2, 10, true, 0, (void *)0, true);
    v3->setNull(0);
    v3->setNull(1);
    char *buf9 = new char[2];
    short *buf10 = new short[2];
    int *buf11 = new int[2];
    dolphindb::INDEX *buf12 = new dolphindb::INDEX[2];
    long long *buf13 = new long long[2];
    float *buf14 = new float[2];
    double *buf15 = new double[2];

    v3->getBool(0, 2, buf9);
    ASSERT_EQ((bool)buf9[0], true);
    ASSERT_EQ((bool)buf9[1], true);
    v3->getChar(0, 2, buf9);
    ASSERT_EQ((char)buf9[0], v3->get(0)->getChar());
    ASSERT_EQ((char)buf9[1], v3->get(1)->getChar());
    v3->getShort(0, 2, buf10);
    ASSERT_EQ((short)buf10[0], v3->get(0)->getShort());
    ASSERT_EQ((short)buf10[1], v3->get(1)->getShort());
    v3->getInt(0, 2, buf11);
    ASSERT_EQ((int)buf11[0], v3->get(0)->getInt());
    ASSERT_EQ((int)buf11[1], v3->get(1)->getInt());
    v3->getLong(0, 2, buf13);
    ASSERT_EQ((long long)buf13[0], v3->get(0)->getLong());
    ASSERT_EQ((long long)buf13[1], v3->get(1)->getLong());
    v3->getIndex(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)buf12[0], v3->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)buf12[1], v3->get(1)->getIndex());
    v3->getFloat(0, 2, buf14);
    ASSERT_EQ((float)buf14[0], v3->get(0)->getFloat());
    ASSERT_EQ((float)buf14[1], v3->get(1)->getFloat());
    v3->getDouble(0, 2, buf15);
    ASSERT_EQ((double)buf15[0], v3->get(0)->getDouble());
    ASSERT_EQ((double)buf15[1], v3->get(1)->getDouble());

    memset(buf9, '\0', 2);
    memset(buf10, 0, 2);
    memset(buf11, 0, 2);
    memset(buf12, 0, 2);
    memset(buf13, 0, 2);
    memset(buf14, 0, 2);
    memset(buf15, 0, 2);

    // std::cout<<v3->getString()<<std::endl;

    const char *resbuf = v3->getBoolConst(0, 2, buf9);
    ASSERT_EQ((bool)resbuf[0], true);
    ASSERT_EQ((bool)resbuf[1], true);

    const char *resbuf1 = v3->getCharConst(0, 2, buf9);
    ;
    ASSERT_EQ((char)resbuf1[0], v3->get(0)->getChar());
    ASSERT_EQ((char)resbuf1[1], v3->get(1)->getChar());

    const short *resbuf2 = v3->getShortConst(0, 2, buf10);
    ASSERT_EQ((short)resbuf2[0], v3->get(0)->getShort());
    ASSERT_EQ((short)resbuf2[1], v3->get(1)->getShort());

    const int *resbuf3 = v3->getIntConst(0, 2, buf11);
    ASSERT_EQ((int)resbuf3[0], v3->get(0)->getInt());
    ASSERT_EQ((int)resbuf3[1], v3->get(1)->getInt());

    const long long *resbuf4 = v3->getLongConst(0, 2, buf13);
    ASSERT_EQ((long long)resbuf4[0], v3->get(0)->getLong());
    ASSERT_EQ((long long)resbuf4[1], v3->get(1)->getLong());

    const dolphindb::INDEX *resbuf5 = v3->getIndexConst(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)resbuf5[0], v3->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)resbuf5[1], v3->get(1)->getIndex());

    const float *resbuf6 = v3->getFloatConst(0, 2, buf14);
    ASSERT_EQ((float)resbuf6[0], v3->get(0)->getFloat());
    ASSERT_EQ((float)resbuf6[1], v3->get(1)->getFloat());

    const double *resbuf7 = v3->getDoubleConst(0, 2, buf15);
    ASSERT_EQ((double)resbuf7[0], v3->get(0)->getDouble());
    ASSERT_EQ((double)resbuf7[1], v3->get(1)->getDouble());

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
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_SHORT, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
    std::string script = "a=[short(NULL),short(NULL)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
    for (int i = 0; i < 2; i++)
        ASSERT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());

    dolphindb::VectorSP v3 = dolphindb::Util::createVector(dolphindb::DT_SHORT, 2, 10, true, 0, (void *)0, true);
    v3->setNull(0);
    v3->setNull(1);
    char *buf9 = new char[2];
    short *buf10 = new short[2];
    int *buf11 = new int[2];
    dolphindb::INDEX *buf12 = new dolphindb::INDEX[2];
    long long *buf13 = new long long[2];
    float *buf14 = new float[2];
    double *buf15 = new double[2];

    v3->getBool(0, 2, buf9);
    ASSERT_EQ((bool)buf9[0], true);
    ASSERT_EQ((bool)buf9[1], true);
    v3->getChar(0, 2, buf9);
    ASSERT_EQ((char)buf9[0], v3->get(0)->getChar());
    ASSERT_EQ((char)buf9[1], v3->get(1)->getChar());
    v3->getShort(0, 2, buf10);
    ASSERT_EQ((short)buf10[0], v3->get(0)->getShort());
    ASSERT_EQ((short)buf10[1], v3->get(1)->getShort());
    v3->getInt(0, 2, buf11);
    ASSERT_EQ((int)buf11[0], v3->get(0)->getInt());
    ASSERT_EQ((int)buf11[1], v3->get(1)->getInt());
    v3->getLong(0, 2, buf13);
    ASSERT_EQ((long long)buf13[0], v3->get(0)->getLong());
    ASSERT_EQ((long long)buf13[1], v3->get(1)->getLong());
    v3->getIndex(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)buf12[0], v3->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)buf12[1], v3->get(1)->getIndex());
    v3->getFloat(0, 2, buf14);
    ASSERT_EQ((float)buf14[0], v3->get(0)->getFloat());
    ASSERT_EQ((float)buf14[1], v3->get(1)->getFloat());
    v3->getDouble(0, 2, buf15);
    ASSERT_EQ((double)buf15[0], v3->get(0)->getDouble());
    ASSERT_EQ((double)buf15[1], v3->get(1)->getDouble());

    memset(buf9, '\0', 2);
    memset(buf10, 0, 2);
    memset(buf11, 0, 2);
    memset(buf12, 0, 2);
    memset(buf13, 0, 2);
    memset(buf14, 0, 2);
    memset(buf15, 0, 2);

    // std::cout<<v3->getString()<<std::endl;

    const char *resbuf = v3->getBoolConst(0, 2, buf9);
    ASSERT_EQ((bool)resbuf[0], true);
    ASSERT_EQ((bool)resbuf[1], true);

    const char *resbuf1 = v3->getCharConst(0, 2, buf9);
    ;
    ASSERT_EQ((char)resbuf1[0], v3->get(0)->getChar());
    ASSERT_EQ((char)resbuf1[1], v3->get(1)->getChar());

    const short *resbuf2 = v3->getShortConst(0, 2, buf10);
    ASSERT_EQ((short)resbuf2[0], v3->get(0)->getShort());
    ASSERT_EQ((short)resbuf2[1], v3->get(1)->getShort());

    const int *resbuf3 = v3->getIntConst(0, 2, buf11);
    ASSERT_EQ((int)resbuf3[0], v3->get(0)->getInt());
    ASSERT_EQ((int)resbuf3[1], v3->get(1)->getInt());

    const long long *resbuf4 = v3->getLongConst(0, 2, buf13);
    ASSERT_EQ((long long)resbuf4[0], v3->get(0)->getLong());
    ASSERT_EQ((long long)resbuf4[1], v3->get(1)->getLong());

    const dolphindb::INDEX *resbuf5 = v3->getIndexConst(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)resbuf5[0], v3->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)resbuf5[1], v3->get(1)->getIndex());

    const float *resbuf6 = v3->getFloatConst(0, 2, buf14);
    ASSERT_EQ((float)resbuf6[0], v3->get(0)->getFloat());
    ASSERT_EQ((float)resbuf6[1], v3->get(1)->getFloat());

    const double *resbuf7 = v3->getDoubleConst(0, 2, buf15);
    ASSERT_EQ((double)resbuf7[0], v3->get(0)->getDouble());
    ASSERT_EQ((double)resbuf7[1], v3->get(1)->getDouble());

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
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_SHORT, 2, 2);
    v1->set(0, dolphindb::Util::createShort(1));
    v1->set(1, dolphindb::Util::createShort(0));
    std::string script = "a=[short(1),short(0)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
    ASSERT_EQ(v1->getCategory(), dolphindb::INTEGRAL);

    ASSERT_ANY_THROW(v1->fill(1, 1, dolphindb::Util::createString("1")));
    ASSERT_TRUE(v1->validIndex(1));
    ASSERT_TRUE(v1->validIndex(0, 1, 1));
    ASSERT_FALSE(v1->validIndex(0, 1, (short)0));

    dolphindb::VectorSP subv1 = dolphindb::Util::createVector(dolphindb::DT_SHORT, 0, 2);
    subv1->append(dolphindb::Util::createShort(1));
    subv1->append(dolphindb::Util::createShort(2));
    ASSERT_FALSE(v1->compare(0, dolphindb::Util::createShort(1)));

    dolphindb::VectorSP indexVec = dolphindb::Util::createIndexVector(0, 1);
    dolphindb::ConstantSP index = dolphindb::Util::createInt(0);
    subv1->set(indexVec, dolphindb::Util::createShort(3));
    ASSERT_EQ(subv1->get(0)->getShort(), (short)3);
    subv1->set(index, dolphindb::Util::createShort(5));
    ASSERT_EQ(subv1->get(0)->getShort(), (short)5);
    subv1->set(index, dolphindb::Util::createNullConstant(dolphindb::DT_SHORT));
    ASSERT_TRUE(subv1->get(0)->isNull());

    dolphindb::VectorSP v4 = dolphindb::Util::createVector(dolphindb::DT_SHORT, 2, 10);
    v4->set(0, dolphindb::Util::createShort(1));
    v4->set(1, dolphindb::Util::createShort(0));
    char *buf_1 = new char[2];
    buf_1[0] = 1;
    short *buf_2 = new short[2];
    buf_2[0] = 1;
    int *buf_3 = new int[2];
    buf_3[0] = 1;
    dolphindb::INDEX *buf_4 = new dolphindb::INDEX[2];
    buf_4[0] = 1;
    long long *buf_5 = new long long[2];
    buf_5[0] = 1;
    float *buf_6 = new float[2];
    buf_6[0] = 1;
    double *buf_7 = new double[2];
    buf_7[0] = 1;

    // std::cout<<v4->getString()<<std::endl;
    v4->appendBool(buf_1, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1]");
    v4->appendChar(buf_1, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1]");
    v4->appendShort(buf_2, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1]");
    v4->appendInt(buf_3, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1,1]");
    v4->appendLong(buf_5, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1,1,1]");
    v4->appendIndex(buf_4, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1]");
    v4->appendFloat(buf_6, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1,1]");
    v4->appendDouble(buf_7, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1,1,1]");

    dolphindb::VectorSP v5 = dolphindb::Util::createVector(dolphindb::DT_SHORT, 2, 10);
    v5->set(0, dolphindb::Util::createShort(1));
    v5->set(1, dolphindb::Util::createShort(0));
    char *buf9 = new char[2];
    short *buf10 = new short[2];
    int *buf11 = new int[2];
    dolphindb::INDEX *buf12 = new dolphindb::INDEX[2];
    long long *buf13 = new long long[2];
    float *buf14 = new float[2];
    double *buf15 = new double[2];

    v5->getBool(0, 2, buf9);
    ASSERT_EQ((bool)buf9[0], v5->get(0)->getBool());
    ASSERT_EQ((bool)buf9[1], v5->get(1)->getBool());
    v5->getChar(0, 2, buf9);
    ASSERT_EQ((char)buf9[0], v5->get(0)->getChar());
    ASSERT_EQ((char)buf9[1], v5->get(1)->getChar());
    v5->getShort(0, 2, buf10);
    ASSERT_EQ((short)buf10[0], v5->get(0)->getShort());
    ASSERT_EQ((short)buf10[1], v5->get(1)->getShort());
    v5->getInt(0, 2, buf11);
    ASSERT_EQ((int)buf11[0], v5->get(0)->getInt());
    ASSERT_EQ((int)buf11[1], v5->get(1)->getInt());
    v5->getLong(0, 2, buf13);
    ASSERT_EQ((long long)buf13[0], v5->get(0)->getLong());
    ASSERT_EQ((long long)buf13[1], v5->get(1)->getLong());
    v5->getIndex(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)buf12[0], v5->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)buf12[1], v5->get(1)->getIndex());
    v5->getFloat(0, 2, buf14);
    ASSERT_EQ((float)buf14[0], v5->get(0)->getFloat());
    ASSERT_EQ((float)buf14[1], v5->get(1)->getFloat());
    v5->getDouble(0, 2, buf15);
    ASSERT_EQ((double)buf15[0], v5->get(0)->getDouble());
    ASSERT_EQ((double)buf15[1], v5->get(1)->getDouble());

    memset(buf9, '\0', 2);
    memset(buf10, 0, 2);
    memset(buf11, 0, 2);
    memset(buf12, 0, 2);
    memset(buf13, 0, 2);
    memset(buf14, 0, 2);
    memset(buf15, 0, 2);

    // std::cout<<v5->getString()<<std::endl;

    const char *resbuf = v5->getBoolConst(0, 2, buf9);
    ASSERT_EQ((bool)resbuf[0], v5->get(0)->getBool());
    ASSERT_EQ((bool)resbuf[1], v5->get(1)->getBool());

    const char *resbuf1 = v5->getCharConst(0, 2, buf9);
    ;
    ASSERT_EQ((char)resbuf1[0], v5->get(0)->getChar());
    ASSERT_EQ((char)resbuf1[1], v5->get(1)->getChar());

    const short *resbuf2 = v5->getShortConst(0, 2, buf10);
    ASSERT_EQ((short)resbuf2[0], v5->get(0)->getShort());
    ASSERT_EQ((short)resbuf2[1], v5->get(1)->getShort());

    const int *resbuf3 = v5->getIntConst(0, 2, buf11);
    ASSERT_EQ((int)resbuf3[0], v5->get(0)->getInt());
    ASSERT_EQ((int)resbuf3[1], v5->get(1)->getInt());

    const long long *resbuf4 = v5->getLongConst(0, 2, buf13);
    ASSERT_EQ((long long)resbuf4[0], v5->get(0)->getLong());
    ASSERT_EQ((long long)resbuf4[1], v5->get(1)->getLong());

    const dolphindb::INDEX *resbuf5 = v5->getIndexConst(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)resbuf5[0], v5->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)resbuf5[1], v5->get(1)->getIndex());

    const float *resbuf6 = v5->getFloatConst(0, 2, buf14);
    ASSERT_EQ((float)resbuf6[0], v5->get(0)->getFloat());
    ASSERT_EQ((float)resbuf6[1], v5->get(1)->getFloat());

    const double *resbuf7 = v5->getDoubleConst(0, 2, buf15);
    ASSERT_EQ((double)resbuf7[0], v5->get(0)->getDouble());
    ASSERT_EQ((double)resbuf7[1], v5->get(1)->getDouble());

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

    dolphindb::VectorSP errfillVals = conn.run("`str1`str2");
    ASSERT_ANY_THROW(v5->fill(0, 2, errfillVals));
    v5->fill(0, 2, dolphindb::Util::createNullConstant(dolphindb::DT_SHORT));
    ASSERT_EQ(v5->getString(), "[,]");
    ASSERT_FALSE(v5->append(dolphindb::Util::createString("aaa"), 0, 2));
}

TEST_F(DataformVectorTest, testFloatVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_FLOAT, 2, 2);
    v1->set(0, dolphindb::Util::createFloat(1));
    v1->set(1, dolphindb::Util::createFloat(2.3131));
    std::string script = "a=[float(1),float(2.3131)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());

    ASSERT_EQ(v1->getCategory(), dolphindb::FLOATING);
    ASSERT_EQ(v1->getChar(1), (char)2);
    ASSERT_EQ(v1->getShort(1), (short)2);
    ASSERT_EQ(v1->getInt(1), (int)2);
    ASSERT_EQ(v1->getLong(1), (long long)2);

    ASSERT_ANY_THROW(v1->fill(1, 1, dolphindb::Util::createString("1")));
    ASSERT_FALSE(v1->validIndex(0, 1, (float)0));

    dolphindb::VectorSP subv1 = dolphindb::Util::createVector(dolphindb::DT_FLOAT, 0, 2);
    subv1->append(dolphindb::Util::createFloat(1));
    subv1->append(dolphindb::Util::createFloat(2));
    ASSERT_EQ(v1->compare(0, dolphindb::Util::createFloat(1)), 0);
    ASSERT_EQ(v1->compare(0, dolphindb::Util::createFloat(0)), 1);
    ASSERT_EQ(v1->compare(0, dolphindb::Util::createFloat(3)), -1);

    dolphindb::VectorSP indexVec = dolphindb::Util::createIndexVector(0, 1);
    dolphindb::ConstantSP index = dolphindb::Util::createFloat(0);
    subv1->set(indexVec, dolphindb::Util::createFloat(3));
    ASSERT_EQ(subv1->get(0)->getFloat(), (float)3);
    subv1->set(index, dolphindb::Util::createFloat(5));
    ASSERT_EQ(subv1->get(0)->getFloat(), (float)5);
    subv1->set(index, dolphindb::Util::createNullConstant(dolphindb::DT_FLOAT));
    ASSERT_TRUE(subv1->get(0)->isNull());
    subv1->set(1, dolphindb::Util::createNullConstant(dolphindb::DT_FLOAT));
    ASSERT_TRUE(subv1->get(1)->isNull());

    char *buf = new char[2];
    short *buf1 = new short[2];
    int *buf2 = new int[2];
    long long *buf3 = new long long[2];
    v1->getChar(0, 2, buf);
    ASSERT_EQ(buf[0], (char)1);
    ASSERT_EQ(buf[1], (char)2);
    buf[0] = '\0';
    buf[1] = '\0';
    v1->getCharConst(0, 2, buf);
    ASSERT_EQ(buf[0], (char)1);
    ASSERT_EQ(buf[1], (char)2);
    v1->getShort(0, 2, buf1);
    ASSERT_EQ(buf1[0], (short)1);
    ASSERT_EQ(buf1[1], (short)2);
    buf1[0] = '\0';
    buf1[1] = '\0';
    v1->getShortConst(0, 2, buf1);
    ASSERT_EQ(buf1[0], (short)1);
    ASSERT_EQ(buf1[1], (short)2);
    v1->getInt(0, 2, buf2);
    ASSERT_EQ(buf2[0], (int)1);
    ASSERT_EQ(buf2[1], (int)2);
    buf2[0] = '\0';
    buf2[1] = '\0';
    v1->getIntConst(0, 2, buf2);
    ASSERT_EQ(buf2[0], (int)1);
    ASSERT_EQ(buf2[1], (int)2);
    v1->getLong(0, 2, buf3);
    ASSERT_EQ(buf3[0], (long long)1);
    ASSERT_EQ(buf3[1], (long long)2);
    buf3[0] = '\0';
    buf3[1] = '\0';
    v1->getLongConst(0, 2, buf3);
    ASSERT_EQ(buf3[0], (long long)1);
    ASSERT_EQ(buf3[1], (long long)2);

    float *buf4 = new float[1];
    buf4[0] = 3.3114;
    v1->appendFloat(buf4, 1);
    ASSERT_EQ(v1->get(2)->getFloat(), (float)3.3114);

    dolphindb::VectorSP v2 = dolphindb::Util::createVector(dolphindb::DT_FLOAT, 2, 2);
    v2->set(0, dolphindb::Util::createFloat(1));
    v2->set(1, dolphindb::Util::createFloat((float)3));
    v2->append(dolphindb::Util::createNullConstant(dolphindb::DT_FLOAT));

    v2->replace(dolphindb::Util::createNullConstant(dolphindb::DT_FLOAT), dolphindb::Util::createFloat(4));
    v2->replace(dolphindb::Util::createFloat((float)3), dolphindb::Util::createNullConstant(dolphindb::DT_FLOAT));
    v2->replace(dolphindb::Util::createFloat(1), dolphindb::Util::createLong(3));
    ASSERT_EQ(v2->getString(), "[3,,4]");
    v2->reverse();
    ASSERT_EQ(v2->getString(), "[4,,3]");
    v2->reverse();
    v2->set(1, dolphindb::Util::createFloat(3.5));
    ASSERT_ANY_THROW(v2->asof(dolphindb::Util::createVector(dolphindb::DT_FLOAT, 0, 1)));
    ASSERT_EQ(v2->asof(dolphindb::Util::createFloat(2)), -1);
    ASSERT_EQ(v2->asof(dolphindb::Util::createFloat(3.1)), 0);

    dolphindb::VectorSP v4 = dolphindb::Util::createVector(dolphindb::DT_FLOAT, 2, 10);
    v4->set(0, dolphindb::Util::createFloat(1));
    v4->set(1, dolphindb::Util::createFloat(0));
    char *buf_1 = new char[2];
    buf_1[0] = 1;
    short *buf_2 = new short[2];
    buf_2[0] = 1;
    int *buf_3 = new int[2];
    buf_3[0] = 1;
    dolphindb::INDEX *buf_4 = new dolphindb::INDEX[2];
    buf_4[0] = 1;
    long long *buf_5 = new long long[2];
    buf_5[0] = 1;
    float *buf_6 = new float[2];
    buf_6[0] = 1;
    double *buf_7 = new double[2];
    buf_7[0] = 1;

    // std::cout<<v4->getString()<<std::endl;
    v4->appendBool(buf_1, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1]");
    v4->appendChar(buf_1, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1]");
    v4->appendShort(buf_2, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1]");
    v4->appendInt(buf_3, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1,1]");
    v4->appendLong(buf_5, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1,1,1]");
    v4->appendIndex(buf_4, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1]");
    v4->appendFloat(buf_6, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1,1]");
    v4->appendDouble(buf_7, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1,1,1]");

    dolphindb::VectorSP v5 = dolphindb::Util::createVector(dolphindb::DT_FLOAT, 2, 10);
    v5->set(0, dolphindb::Util::createFloat(1));
    v5->set(1, dolphindb::Util::createFloat(0));
    char *buf9 = new char[2];
    short *buf10 = new short[2];
    int *buf11 = new int[2];
    dolphindb::INDEX *buf12 = new dolphindb::INDEX[2];
    long long *buf13 = new long long[2];
    float *buf14 = new float[2];
    double *buf15 = new double[2];

    v5->getBool(0, 2, buf9);
    ASSERT_EQ((bool)buf9[0], v5->get(0)->getBool());
    ASSERT_EQ((bool)buf9[1], v5->get(1)->getBool());
    v5->getChar(0, 2, buf9);
    ASSERT_EQ((char)buf9[0], v5->get(0)->getChar());
    ASSERT_EQ((char)buf9[1], v5->get(1)->getChar());
    v5->getShort(0, 2, buf10);
    ASSERT_EQ((short)buf10[0], v5->get(0)->getShort());
    ASSERT_EQ((short)buf10[1], v5->get(1)->getShort());
    v5->getInt(0, 2, buf11);
    ASSERT_EQ((int)buf11[0], v5->get(0)->getInt());
    ASSERT_EQ((int)buf11[1], v5->get(1)->getInt());
    v5->getLong(0, 2, buf13);
    ASSERT_EQ((long long)buf13[0], v5->get(0)->getLong());
    ASSERT_EQ((long long)buf13[1], v5->get(1)->getLong());
    v5->getIndex(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)buf12[0], v5->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)buf12[1], v5->get(1)->getIndex());
    v5->getFloat(0, 2, buf14);
    ASSERT_EQ((float)buf14[0], v5->get(0)->getFloat());
    ASSERT_EQ((float)buf14[1], v5->get(1)->getFloat());
    v5->getDouble(0, 2, buf15);
    ASSERT_EQ((double)buf15[0], v5->get(0)->getDouble());
    ASSERT_EQ((double)buf15[1], v5->get(1)->getDouble());

    memset(buf9, '\0', 2);
    memset(buf10, 0, 2);
    memset(buf11, 0, 2);
    memset(buf12, 0, 2);
    memset(buf13, 0, 2);
    memset(buf14, 0, 2);
    memset(buf15, 0, 2);

    // std::cout<<v5->getString()<<std::endl;

    const char *resbuf = v5->getBoolConst(0, 2, buf9);
    ASSERT_EQ((bool)resbuf[0], v5->get(0)->getBool());
    ASSERT_EQ((bool)resbuf[1], v5->get(1)->getBool());

    const char *resbuf1 = v5->getCharConst(0, 2, buf9);
    ;
    ASSERT_EQ((char)resbuf1[0], v5->get(0)->getChar());
    ASSERT_EQ((char)resbuf1[1], v5->get(1)->getChar());

    const short *resbuf2 = v5->getShortConst(0, 2, buf10);
    ASSERT_EQ((short)resbuf2[0], v5->get(0)->getShort());
    ASSERT_EQ((short)resbuf2[1], v5->get(1)->getShort());

    const int *resbuf3 = v5->getIntConst(0, 2, buf11);
    ASSERT_EQ((int)resbuf3[0], v5->get(0)->getInt());
    ASSERT_EQ((int)resbuf3[1], v5->get(1)->getInt());

    const long long *resbuf4 = v5->getLongConst(0, 2, buf13);
    ASSERT_EQ((long long)resbuf4[0], v5->get(0)->getLong());
    ASSERT_EQ((long long)resbuf4[1], v5->get(1)->getLong());

    const dolphindb::INDEX *resbuf5 = v5->getIndexConst(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)resbuf5[0], v5->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)resbuf5[1], v5->get(1)->getIndex());

    const float *resbuf6 = v5->getFloatConst(0, 2, buf14);
    ASSERT_EQ((float)resbuf6[0], v5->get(0)->getFloat());
    ASSERT_EQ((float)resbuf6[1], v5->get(1)->getFloat());

    const double *resbuf7 = v5->getDoubleConst(0, 2, buf15);
    ASSERT_EQ((double)resbuf7[0], v5->get(0)->getDouble());
    ASSERT_EQ((double)resbuf7[1], v5->get(1)->getDouble());

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

    dolphindb::VectorSP errfillVals = conn.run("`str1`str2");
    ASSERT_ANY_THROW(v5->fill(0, 2, errfillVals));
    v5->fill(0, 2, dolphindb::Util::createNullConstant(dolphindb::DT_FLOAT));
    ASSERT_EQ(v5->getString(), "[,]");
    ASSERT_FALSE(v5->append(dolphindb::Util::createString("aaa"), 0, 2));
}

TEST_F(DataformVectorTest, testFloatNullVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_FLOAT, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
    std::string script = "a=[float(NULL),float(NULL)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());

    dolphindb::VectorSP v2 = dolphindb::Util::createVector(dolphindb::DT_FLOAT, 2, 2, true, 0, (void *)0, true);
    v2->setNull(0);
    v2->setNull(1);
    char *buf = new char[2];
    short *buf1 = new short[2];
    int *buf2 = new int[2];
    long long *buf3 = new long long[2];
    v2->getChar(0, 2, buf);
    ASSERT_EQ(buf[0], CHAR_MIN);
    ASSERT_EQ(buf[1], CHAR_MIN);
    buf[0] = '\0';
    buf[1] = '\0';
    v2->getCharConst(0, 2, buf);
    ASSERT_EQ(buf[0], CHAR_MIN);
    ASSERT_EQ(buf[1], CHAR_MIN);
    v2->getShort(0, 2, buf1);
    ASSERT_EQ(buf1[0], SHRT_MIN);
    ASSERT_EQ(buf1[1], SHRT_MIN);
    buf1[0] = '\0';
    buf1[1] = '\0';
    v2->getShortConst(0, 2, buf1);
    ASSERT_EQ(buf1[0], SHRT_MIN);
    ASSERT_EQ(buf1[1], SHRT_MIN);
    v2->getInt(0, 2, buf2);
    ASSERT_EQ(buf2[0], INT_MIN);
    ASSERT_EQ(buf2[1], INT_MIN);
    buf2[0] = '\0';
    buf2[1] = '\0';
    v2->getIntConst(0, 2, buf2);
    ASSERT_EQ(buf2[0], INT_MIN);
    ASSERT_EQ(buf2[1], INT_MIN);
    v2->getLong(0, 2, buf3);
    ASSERT_EQ(buf3[0], LLONG_MIN);
    ASSERT_EQ(buf3[1], LLONG_MIN);
    buf3[0] = '\0';
    buf3[1] = '\0';
    v2->getLongConst(0, 2, buf3);
    ASSERT_EQ(buf3[0], LLONG_MIN);
    ASSERT_EQ(buf3[1], LLONG_MIN);

    dolphindb::VectorSP v3 = dolphindb::Util::createVector(dolphindb::DT_FLOAT, 2, 10, true, 0, (void *)0, true);
    v3->setNull(0);
    v3->setNull(1);
    char *buf9 = new char[2];
    short *buf10 = new short[2];
    int *buf11 = new int[2];
    dolphindb::INDEX *buf12 = new dolphindb::INDEX[2];
    long long *buf13 = new long long[2];
    float *buf14 = new float[2];
    double *buf15 = new double[2];

    v3->getBool(0, 2, buf9);
    ASSERT_EQ((bool)buf9[0], true);
    ASSERT_EQ((bool)buf9[1], true);
    v3->getChar(0, 2, buf9);
    ASSERT_EQ((char)buf9[0], v3->get(0)->getChar());
    ASSERT_EQ((char)buf9[1], v3->get(1)->getChar());
    v3->getShort(0, 2, buf10);
    ASSERT_EQ((short)buf10[0], v3->get(0)->getShort());
    ASSERT_EQ((short)buf10[1], v3->get(1)->getShort());
    v3->getInt(0, 2, buf11);
    ASSERT_EQ((int)buf11[0], v3->get(0)->getInt());
    ASSERT_EQ((int)buf11[1], v3->get(1)->getInt());
    v3->getLong(0, 2, buf13);
    ASSERT_EQ((long long)buf13[0], v3->get(0)->getLong());
    ASSERT_EQ((long long)buf13[1], v3->get(1)->getLong());
    v3->getIndex(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)buf12[0], v3->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)buf12[1], v3->get(1)->getIndex());
    v3->getFloat(0, 2, buf14);
    ASSERT_EQ((float)buf14[0], v3->get(0)->getFloat());
    ASSERT_EQ((float)buf14[1], v3->get(1)->getFloat());
    v3->getDouble(0, 2, buf15);
    ASSERT_EQ((double)buf15[0], v3->get(0)->getDouble());
    ASSERT_EQ((double)buf15[1], v3->get(1)->getDouble());

    memset(buf9, '\0', 2);
    memset(buf10, 0, 2);
    memset(buf11, 0, 2);
    memset(buf12, 0, 2);
    memset(buf13, 0, 2);
    memset(buf14, 0, 2);
    memset(buf15, 0, 2);

    // std::cout<<v3->getString()<<std::endl;

    const char *resbuf = v3->getBoolConst(0, 2, buf9);
    ASSERT_EQ((bool)resbuf[0], true);
    ASSERT_EQ((bool)resbuf[1], true);

    const char *resbuf1 = v3->getCharConst(0, 2, buf9);
    ;
    ASSERT_EQ((char)resbuf1[0], v3->get(0)->getChar());
    ASSERT_EQ((char)resbuf1[1], v3->get(1)->getChar());

    const short *resbuf2 = v3->getShortConst(0, 2, buf10);
    ASSERT_EQ((short)resbuf2[0], v3->get(0)->getShort());
    ASSERT_EQ((short)resbuf2[1], v3->get(1)->getShort());

    const int *resbuf3 = v3->getIntConst(0, 2, buf11);
    ASSERT_EQ((int)resbuf3[0], v3->get(0)->getInt());
    ASSERT_EQ((int)resbuf3[1], v3->get(1)->getInt());

    const long long *resbuf4 = v3->getLongConst(0, 2, buf13);
    ASSERT_EQ((long long)resbuf4[0], v3->get(0)->getLong());
    ASSERT_EQ((long long)resbuf4[1], v3->get(1)->getLong());

    const dolphindb::INDEX *resbuf5 = v3->getIndexConst(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)resbuf5[0], v3->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)resbuf5[1], v3->get(1)->getIndex());

    const float *resbuf6 = v3->getFloatConst(0, 2, buf14);
    ASSERT_EQ((float)resbuf6[0], v3->get(0)->getFloat());
    ASSERT_EQ((float)resbuf6[1], v3->get(1)->getFloat());

    const double *resbuf7 = v3->getDoubleConst(0, 2, buf15);
    ASSERT_EQ((double)resbuf7[0], v3->get(0)->getDouble());
    ASSERT_EQ((double)resbuf7[1], v3->get(1)->getDouble());

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
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DOUBLE, 2, 2);
    v1->set(0, dolphindb::Util::createDouble(1));
    v1->set(1, dolphindb::Util::createDouble(2.3131));
    std::string script = "a=[double(1),double(2.3131)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());

    ASSERT_EQ(v1->getCategory(), dolphindb::FLOATING);
    ASSERT_EQ(v1->getChar(1), (char)2);
    ASSERT_EQ(v1->getShort(1), (short)2);
    ASSERT_EQ(v1->getInt(1), (int)2);
    ASSERT_EQ(v1->getLong(1), (long long)2);

    ASSERT_ANY_THROW(v1->fill(1, 1, dolphindb::Util::createString("1")));
    ASSERT_FALSE(v1->validIndex(0, 1, (float)0));

    dolphindb::VectorSP subv1 = dolphindb::Util::createVector(dolphindb::DT_DOUBLE, 0, 2);
    subv1->append(dolphindb::Util::createDouble(1));
    subv1->append(dolphindb::Util::createDouble(2));
    ASSERT_EQ(v1->compare(0, dolphindb::Util::createDouble(1)), 0);
    ASSERT_EQ(v1->compare(0, dolphindb::Util::createDouble(0)), 1);
    ASSERT_EQ(v1->compare(0, dolphindb::Util::createDouble(3)), -1);

    dolphindb::VectorSP indexVec = dolphindb::Util::createIndexVector(0, 1);
    dolphindb::ConstantSP index = dolphindb::Util::createDouble(0);
    subv1->set(indexVec, dolphindb::Util::createDouble(3));
    ASSERT_EQ(subv1->get(0)->getDouble(), (float)3);
    subv1->set(index, dolphindb::Util::createDouble(5));
    ASSERT_EQ(subv1->get(0)->getDouble(), (float)5);
    subv1->set(index, dolphindb::Util::createNullConstant(dolphindb::DT_DOUBLE));
    ASSERT_TRUE(subv1->get(0)->isNull());
    subv1->set(1, dolphindb::Util::createNullConstant(dolphindb::DT_DOUBLE));
    ASSERT_TRUE(subv1->get(1)->isNull());

    char *buf = new char[2];
    short *buf1 = new short[2];
    int *buf2 = new int[2];
    long long *buf3 = new long long[2];
    v1->getChar(0, 2, buf);
    ASSERT_EQ(buf[0], (char)1);
    ASSERT_EQ(buf[1], (char)2);
    buf[0] = '\0';
    buf[1] = '\0';
    v1->getCharConst(0, 2, buf);
    ASSERT_EQ(buf[0], (char)1);
    ASSERT_EQ(buf[1], (char)2);
    v1->getShort(0, 2, buf1);
    ASSERT_EQ(buf1[0], (short)1);
    ASSERT_EQ(buf1[1], (short)2);
    buf1[0] = '\0';
    buf1[1] = '\0';
    v1->getShortConst(0, 2, buf1);
    ASSERT_EQ(buf1[0], (short)1);
    ASSERT_EQ(buf1[1], (short)2);
    v1->getInt(0, 2, buf2);
    ASSERT_EQ(buf2[0], (int)1);
    ASSERT_EQ(buf2[1], (int)2);
    buf2[0] = '\0';
    buf2[1] = '\0';
    v1->getIntConst(0, 2, buf2);
    ASSERT_EQ(buf2[0], (int)1);
    ASSERT_EQ(buf2[1], (int)2);
    v1->getLong(0, 2, buf3);
    ASSERT_EQ(buf3[0], (long long)1);
    ASSERT_EQ(buf3[1], (long long)2);
    buf3[0] = '\0';
    buf3[1] = '\0';
    v1->getLongConst(0, 2, buf3);
    ASSERT_EQ(buf3[0], (long long)1);
    ASSERT_EQ(buf3[1], (long long)2);

    double *buf4 = new double[1];
    buf4[0] = 3.3114;
    v1->appendDouble(buf4, 1);
    ASSERT_EQ(v1->get(2)->getDouble(), (double)3.3114);
    v1->add(0, 2, (double)1);
    ASSERT_EQ(v1->getString(), "[2,3.3131,3.3114]");
    v1->append(dolphindb::Util::createNullConstant(dolphindb::DT_DOUBLE));
    v1->add(0, 3, (double)1);
    ASSERT_EQ(v1->getString(), "[3,4.3131,4.3114,]");

    dolphindb::VectorSP v4 = dolphindb::Util::createVector(dolphindb::DT_DOUBLE, 2, 10);
    v4->set(0, dolphindb::Util::createDouble(1));
    v4->set(1, dolphindb::Util::createDouble(0));
    char *buf_1 = new char[2];
    buf_1[0] = 1;
    short *buf_2 = new short[2];
    buf_2[0] = 1;
    int *buf_3 = new int[2];
    buf_3[0] = 1;
    dolphindb::INDEX *buf_4 = new dolphindb::INDEX[2];
    buf_4[0] = 1;
    long long *buf_5 = new long long[2];
    buf_5[0] = 1;
    float *buf_6 = new float[2];
    buf_6[0] = 1;
    double *buf_7 = new double[2];
    buf_7[0] = 1;

    // std::cout<<v4->getString()<<std::endl;
    v4->appendBool(buf_1, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1]");
    v4->appendChar(buf_1, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1]");
    v4->appendShort(buf_2, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1]");
    v4->appendInt(buf_3, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1,1]");
    v4->appendLong(buf_5, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1,1,1]");
    v4->appendIndex(buf_4, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1]");
    v4->appendFloat(buf_6, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1,1]");
    v4->appendDouble(buf_7, 1);
    ASSERT_EQ(v4->getString(), "[1,0,1,1,1,1,1,1,1,1]");

    dolphindb::VectorSP v5 = dolphindb::Util::createVector(dolphindb::DT_DOUBLE, 2, 10);
    v5->set(0, dolphindb::Util::createDouble(1));
    v5->set(1, dolphindb::Util::createDouble(0));
    char *buf9 = new char[2];
    short *buf10 = new short[2];
    int *buf11 = new int[2];
    dolphindb::INDEX *buf12 = new dolphindb::INDEX[2];
    long long *buf13 = new long long[2];
    float *buf14 = new float[2];
    double *buf15 = new double[2];

    v5->getBool(0, 2, buf9);
    ASSERT_EQ((bool)buf9[0], v5->get(0)->getBool());
    ASSERT_EQ((bool)buf9[1], v5->get(1)->getBool());
    v5->getChar(0, 2, buf9);
    ASSERT_EQ((char)buf9[0], v5->get(0)->getChar());
    ASSERT_EQ((char)buf9[1], v5->get(1)->getChar());
    v5->getShort(0, 2, buf10);
    ASSERT_EQ((short)buf10[0], v5->get(0)->getShort());
    ASSERT_EQ((short)buf10[1], v5->get(1)->getShort());
    v5->getInt(0, 2, buf11);
    ASSERT_EQ((int)buf11[0], v5->get(0)->getInt());
    ASSERT_EQ((int)buf11[1], v5->get(1)->getInt());
    v5->getLong(0, 2, buf13);
    ASSERT_EQ((long long)buf13[0], v5->get(0)->getLong());
    ASSERT_EQ((long long)buf13[1], v5->get(1)->getLong());
    v5->getIndex(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)buf12[0], v5->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)buf12[1], v5->get(1)->getIndex());
    v5->getFloat(0, 2, buf14);
    ASSERT_EQ((float)buf14[0], v5->get(0)->getFloat());
    ASSERT_EQ((float)buf14[1], v5->get(1)->getFloat());
    v5->getDouble(0, 2, buf15);
    ASSERT_EQ((double)buf15[0], v5->get(0)->getDouble());
    ASSERT_EQ((double)buf15[1], v5->get(1)->getDouble());

    memset(buf9, '\0', 2);
    memset(buf10, 0, 2);
    memset(buf11, 0, 2);
    memset(buf12, 0, 2);
    memset(buf13, 0, 2);
    memset(buf14, 0, 2);
    memset(buf15, 0, 2);

    // std::cout<<v5->getString()<<std::endl;
    const char *resbuf = v5->getBoolConst(0, 2, buf9);
    ASSERT_EQ((bool)resbuf[0], v5->get(0)->getBool());
    ASSERT_EQ((bool)resbuf[1], v5->get(1)->getBool());

    const char *resbuf1 = v5->getCharConst(0, 2, buf9);
    ;
    ASSERT_EQ((char)resbuf1[0], v5->get(0)->getChar());
    ASSERT_EQ((char)resbuf1[1], v5->get(1)->getChar());

    const short *resbuf2 = v5->getShortConst(0, 2, buf10);
    ASSERT_EQ((short)resbuf2[0], v5->get(0)->getShort());
    ASSERT_EQ((short)resbuf2[1], v5->get(1)->getShort());

    const int *resbuf3 = v5->getIntConst(0, 2, buf11);
    ASSERT_EQ((int)resbuf3[0], v5->get(0)->getInt());
    ASSERT_EQ((int)resbuf3[1], v5->get(1)->getInt());

    const long long *resbuf4 = v5->getLongConst(0, 2, buf13);
    ASSERT_EQ((long long)resbuf4[0], v5->get(0)->getLong());
    ASSERT_EQ((long long)resbuf4[1], v5->get(1)->getLong());

    const dolphindb::INDEX *resbuf5 = v5->getIndexConst(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)resbuf5[0], v5->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)resbuf5[1], v5->get(1)->getIndex());

    const float *resbuf6 = v5->getFloatConst(0, 2, buf14);
    ASSERT_EQ((float)resbuf6[0], v5->get(0)->getFloat());
    ASSERT_EQ((float)resbuf6[1], v5->get(1)->getFloat());

    const double *resbuf7 = v5->getDoubleConst(0, 2, buf15);
    ASSERT_EQ((double)resbuf7[0], v5->get(0)->getDouble());
    ASSERT_EQ((double)resbuf7[1], v5->get(1)->getDouble());

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

    dolphindb::VectorSP errfillVals = conn.run("`str1`str2");
    ASSERT_ANY_THROW(v5->fill(0, 2, errfillVals));
    v5->fill(0, 2, dolphindb::Util::createNullConstant(dolphindb::DT_FLOAT));
    ASSERT_EQ(v5->getString(), "[,]");
    ASSERT_FALSE(v5->append(dolphindb::Util::createString("aaa"), 0, 2));
}

TEST_F(DataformVectorTest, testDoubleNullVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DOUBLE, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
    std::string script = "a=[double(NULL),double(NULL)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());

    dolphindb::VectorSP v2 = dolphindb::Util::createVector(dolphindb::DT_DOUBLE, 2, 2, true, 0, (void *)0, true);
    v2->setNull(0);
    v2->setNull(1);
    char *buf = new char[2];
    short *buf1 = new short[2];
    int *buf2 = new int[2];
    long long *buf3 = new long long[2];
    v2->getChar(0, 2, buf);
    ASSERT_EQ(buf[0], CHAR_MIN);
    ASSERT_EQ(buf[1], CHAR_MIN);
    buf[0] = '\0';
    buf[1] = '\0';
    v2->getCharConst(0, 2, buf);
    ASSERT_EQ(buf[0], CHAR_MIN);
    ASSERT_EQ(buf[1], CHAR_MIN);
    v2->getShort(0, 2, buf1);
    ASSERT_EQ(buf1[0], SHRT_MIN);
    ASSERT_EQ(buf1[1], SHRT_MIN);
    buf1[0] = '\0';
    buf1[1] = '\0';
    v2->getShortConst(0, 2, buf1);
    ASSERT_EQ(buf1[0], SHRT_MIN);
    ASSERT_EQ(buf1[1], SHRT_MIN);
    v2->getInt(0, 2, buf2);
    ASSERT_EQ(buf2[0], INT_MIN);
    ASSERT_EQ(buf2[1], INT_MIN);
    buf2[0] = '\0';
    buf2[1] = '\0';
    v2->getIntConst(0, 2, buf2);
    ASSERT_EQ(buf2[0], INT_MIN);
    ASSERT_EQ(buf2[1], INT_MIN);
    v2->getLong(0, 2, buf3);
    ASSERT_EQ(buf3[0], LLONG_MIN);
    ASSERT_EQ(buf3[1], LLONG_MIN);
    buf3[0] = '\0';
    buf3[1] = '\0';
    v2->getLongConst(0, 2, buf3);
    ASSERT_EQ(buf3[0], LLONG_MIN);
    ASSERT_EQ(buf3[1], LLONG_MIN);

    dolphindb::VectorSP v3 = dolphindb::Util::createVector(dolphindb::DT_DOUBLE, 2, 10, true, 0, (void *)0, true);
    v3->setNull(0);
    v3->setNull(1);
    char *buf9 = new char[2];
    short *buf10 = new short[2];
    int *buf11 = new int[2];
    dolphindb::INDEX *buf12 = new dolphindb::INDEX[2];
    long long *buf13 = new long long[2];
    float *buf14 = new float[2];
    double *buf15 = new double[2];

    v3->getBool(0, 2, buf9);
    ASSERT_EQ((bool)buf9[0], true);
    ASSERT_EQ((bool)buf9[1], true);
    v3->getChar(0, 2, buf9);
    ASSERT_EQ((char)buf9[0], v3->get(0)->getChar());
    ASSERT_EQ((char)buf9[1], v3->get(1)->getChar());
    v3->getShort(0, 2, buf10);
    ASSERT_EQ((short)buf10[0], v3->get(0)->getShort());
    ASSERT_EQ((short)buf10[1], v3->get(1)->getShort());
    v3->getInt(0, 2, buf11);
    ASSERT_EQ((int)buf11[0], v3->get(0)->getInt());
    ASSERT_EQ((int)buf11[1], v3->get(1)->getInt());
    v3->getLong(0, 2, buf13);
    ASSERT_EQ((long long)buf13[0], v3->get(0)->getLong());
    ASSERT_EQ((long long)buf13[1], v3->get(1)->getLong());
    v3->getIndex(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)buf12[0], v3->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)buf12[1], v3->get(1)->getIndex());
    v3->getFloat(0, 2, buf14);
    ASSERT_EQ((float)buf14[0], v3->get(0)->getFloat());
    ASSERT_EQ((float)buf14[1], v3->get(1)->getFloat());
    v3->getDouble(0, 2, buf15);
    ASSERT_EQ((double)buf15[0], v3->get(0)->getDouble());
    ASSERT_EQ((double)buf15[1], v3->get(1)->getDouble());

    memset(buf9, '\0', 2);
    memset(buf10, 0, 2);
    memset(buf11, 0, 2);
    memset(buf12, 0, 2);
    memset(buf13, 0, 2);
    memset(buf14, 0, 2);
    memset(buf15, 0, 2);

    // std::cout<<v3->getString()<<std::endl;

    const char *resbuf = v3->getBoolConst(0, 2, buf9);
    ASSERT_EQ((bool)resbuf[0], true);
    ASSERT_EQ((bool)resbuf[1], true);

    const char *resbuf1 = v3->getCharConst(0, 2, buf9);
    ;
    ASSERT_EQ((char)resbuf1[0], v3->get(0)->getChar());
    ASSERT_EQ((char)resbuf1[1], v3->get(1)->getChar());

    const short *resbuf2 = v3->getShortConst(0, 2, buf10);
    ASSERT_EQ((short)resbuf2[0], v3->get(0)->getShort());
    ASSERT_EQ((short)resbuf2[1], v3->get(1)->getShort());

    const int *resbuf3 = v3->getIntConst(0, 2, buf11);
    ASSERT_EQ((int)resbuf3[0], v3->get(0)->getInt());
    ASSERT_EQ((int)resbuf3[1], v3->get(1)->getInt());

    const long long *resbuf4 = v3->getLongConst(0, 2, buf13);
    ASSERT_EQ((long long)resbuf4[0], v3->get(0)->getLong());
    ASSERT_EQ((long long)resbuf4[1], v3->get(1)->getLong());

    const dolphindb::INDEX *resbuf5 = v3->getIndexConst(0, 2, buf12);
    ASSERT_EQ((dolphindb::INDEX)resbuf5[0], v3->get(0)->getIndex());
    ASSERT_EQ((dolphindb::INDEX)resbuf5[1], v3->get(1)->getIndex());

    const float *resbuf6 = v3->getFloatConst(0, 2, buf14);
    ASSERT_EQ((float)resbuf6[0], v3->get(0)->getFloat());
    ASSERT_EQ((float)resbuf6[1], v3->get(1)->getFloat());

    const double *resbuf7 = v3->getDoubleConst(0, 2, buf15);
    ASSERT_EQ((double)resbuf7[0], v3->get(0)->getDouble());
    ASSERT_EQ((double)resbuf7[1], v3->get(1)->getDouble());

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
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DATEHOUR, 2, 2);
    v1->set(0, dolphindb::Util::createDateHour(1));
    v1->set(1, dolphindb::Util::createDateHour(1000));
    std::string script = "a=[datehour(1),datehour(1000)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
}

TEST_F(DataformVectorTest, testDatehourNullVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DATEHOUR, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
    std::string script = "a=[datehour(NULL),datehour(NULL)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
    for (int i = 0; i < 2; i++)
        ASSERT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testDateVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DATE, 2, 2);
    v1->set(0, dolphindb::Util::createDate(1));
    v1->set(1, dolphindb::Util::createDate(1000));
    std::string script = "a=[date(1),date(1000)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
}

TEST_F(DataformVectorTest, testDatenullVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DATE, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
    std::string script = "a=[date(NULL),date(NULL)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
    for (int i = 0; i < 2; i++)
        ASSERT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testMinuteVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_MINUTE, 2, 2);
    v1->set(0, dolphindb::Util::createMinute(1));
    v1->set(1, dolphindb::Util::createMinute(1000));
    std::string script = "a=[minute(1),minute(1000)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());

    v1->append(dolphindb::Util::createMinute(2000));
    v1->validate();
    ASSERT_EQ(v1->get(v1->size() - 1)->getInt(), INT_MIN);
    ASSERT_TRUE(v1->hasNull());
}

TEST_F(DataformVectorTest, testMinutenullVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_MINUTE, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
    std::string script = "a=[minute(NULL),minute(NULL)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
    for (int i = 0; i < 2; i++)
        ASSERT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testDatetimeVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DATETIME, 2, 2);
    v1->set(0, dolphindb::Util::createDateTime(1));
    v1->set(1, dolphindb::Util::createDateTime(1000));
    std::string script = "a=[datetime(1),datetime(1000)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
}

TEST_F(DataformVectorTest, testDatetimenullVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DATETIME, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
    std::string script = "a=[datetime(NULL),datetime(NULL)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
    for (int i = 0; i < 2; i++)
        ASSERT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testTimestampVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_TIMESTAMP, 2, 2);
    v1->set(0, dolphindb::Util::createTimestamp(1));
    v1->set(1, dolphindb::Util::createTimestamp(1000000));
    std::string script = "a=[timestamp(1),timestamp(1000000)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
}

TEST_F(DataformVectorTest, testTimestampnullVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_TIMESTAMP, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
    std::string script = "a=[timestamp(NULL),timestamp(NULL)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
    for (int i = 0; i < 2; i++)
        ASSERT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testNanotimeVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_NANOTIME, 2, 2);
    v1->set(0, dolphindb::Util::createNanoTime(1));
    v1->set(1, dolphindb::Util::createNanoTime(1000000));
    std::string script = "a=[nanotime(1),nanotime(1000000)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());

    v1->append(dolphindb::Util::createNanoTime(86400000000000ll));
    v1->validate();
    ASSERT_EQ(v1->get(v1->size() - 1)->getLong(), LLONG_MIN);
    ASSERT_TRUE(v1->hasNull());
}

TEST_F(DataformVectorTest, testNanotimenullVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_NANOTIME, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
    std::string script = "a=[nanotime(NULL),nanotime(NULL)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
    for (int i = 0; i < 2; i++)
        ASSERT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testNanotimestampVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_NANOTIMESTAMP, 2, 2);
    v1->set(0, dolphindb::Util::createNanoTimestamp(1));
    v1->set(1, dolphindb::Util::createNanoTimestamp(100000000));
    std::string script = "a=[nanotimestamp(1),nanotimestamp(100000000)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
}

TEST_F(DataformVectorTest, testNanotimestampnullVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_NANOTIMESTAMP, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
    std::string script = "a=[nanotimestamp(NULL),nanotimestamp(NULL)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
    for (int i = 0; i < 2; i++)
        ASSERT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testMonthVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_MONTH, 2, 2);
    v1->set(0, dolphindb::Util::createMonth(1));
    v1->set(1, dolphindb::Util::createMonth(1000));
    std::string script = "a=[month(1),month(1000)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());

    ASSERT_FALSE(v1->isIndexArray());
    ASSERT_TRUE(v1->getIndexArray() == NULL);
    ASSERT_EQ(v1->getValue(2)->getString(), v1->castTemporal(dolphindb::DT_MONTH)->getString());
}

TEST_F(DataformVectorTest, testMonthnullVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_MONTH, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
    std::string script = "a=[month(NULL),month(NULL)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
    for (int i = 0; i < 2; i++)
        ASSERT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testTimeVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_TIME, 2, 2);
    v1->set(0, dolphindb::Util::createTime(1));
    v1->set(1, dolphindb::Util::createTime(1000));
    std::string script = "a=[time(1),time(1000)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());

    v1->append(dolphindb::Util::createTime(86401));
    v1->validate();
    ASSERT_EQ(v1->get(v1->size() - 1)->getInt(), INT_MIN);
    ASSERT_TRUE(v1->hasNull());
}

TEST_F(DataformVectorTest, testTimenullVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_TIME, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
    std::string script = "a=[time(NULL),time(NULL)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
    for (int i = 0; i < 2; i++)
        ASSERT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testSecondVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_SECOND, 2, 2);
    v1->set(0, dolphindb::Util::createSecond(1));
    v1->set(1, dolphindb::Util::createSecond(1000));
    std::string script = "a=[second(1),second(1000)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());

    v1->append(dolphindb::Util::createSecond(86400));
    v1->validate();
    ASSERT_EQ(v1->get(v1->size() - 1)->getInt(), INT_MIN);
    ASSERT_TRUE(v1->hasNull());
}

TEST_F(DataformVectorTest, testSecondnullVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_SECOND, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
    std::string script = "a=[second(NULL),second(NULL)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
    for (int i = 0; i < 2; i++)
        ASSERT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testInt128Vector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_INT128, 2, 2);
    v1->set(0, dolphindb::Util::parseConstant(dolphindb::DT_INT128, "e1671797c52e15f763380b45e841ec32"));
    v1->set(1, dolphindb::Util::parseConstant(dolphindb::DT_INT128, "e1671797c52e15f763380b45e841ec33"));
    std::string script = "a=[int128(`e1671797c52e15f763380b45e841ec32),int128(`e1671797c52e15f763380b45e841ec33)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());

    ASSERT_ANY_THROW(v1->setString(1, "123"));
    ASSERT_TRUE(v1->isFastMode());
    ASSERT_TRUE(v1->sizeable());
    v1->setNull();
    ASSERT_FALSE(v1->hasNull(0, 1));
    v1->reverse();
    ASSERT_EQ(v1->get(0)->getString(), "e1671797c52e15f763380b45e841ec33");
    ASSERT_EQ(v1->get(1)->getString(), "e1671797c52e15f763380b45e841ec32");

    ASSERT_ANY_THROW(v1->getBool());
    ASSERT_ANY_THROW(v1->getChar());
    ASSERT_ANY_THROW(v1->getShort());
    ASSERT_ANY_THROW(v1->getLong());
    ASSERT_ANY_THROW(v1->getFloat());
    ASSERT_ANY_THROW(v1->getDouble());
    ASSERT_ANY_THROW(v1->getIndex());
    ASSERT_ANY_THROW(v1->neg());
    ASSERT_ANY_THROW(v1->getBinary());

    // std::cout<<v1->get(dolphindb::Util::createInt(1))<<std::endl;
    ASSERT_EQ(v1->compare(0, dolphindb::Util::parseConstant(dolphindb::DT_INT128, "e1671797c52e15f763380b45e841ec33")), 0);
    v1->replace(dolphindb::Util::parseConstant(dolphindb::DT_INT128, "e1671797c52e15f763380b45e841ec33"), dolphindb::Util::parseConstant(dolphindb::DT_INT128, "e1671797c52e15f763380b45e841ec55"));
    ASSERT_EQ(v1->get(0)->getString(), "e1671797c52e15f763380b45e841ec55");

    v1->append(dolphindb::Util::createNullConstant(dolphindb::DT_INT128));
    ASSERT_ANY_THROW(v1->nullFill(dolphindb::Util::createInt(1)));
    v1->nullFill(dolphindb::Util::parseConstant(dolphindb::DT_INT128, "e1671797c52e15f763380b45e841ec99"));
    ASSERT_EQ(v1->get(2)->getString(), "e1671797c52e15f763380b45e841ec99");

    char *buf1 = new char[3];
    char *buf2 = new char[3];
    v1->isNull(0, 3, buf1);
    ASSERT_FALSE((int)buf1[0]);
    ASSERT_FALSE((int)buf1[1]);
    ASSERT_FALSE((int)buf1[2]);
    v1->isValid(0, 3, buf2);
    ASSERT_TRUE((int)buf2[0]);
    ASSERT_TRUE((int)buf2[1]);
    ASSERT_TRUE((int)buf2[2]);

    std::string strEmpty = "";
    std::string strlt32 = "192.168.1.1";
    std::string int128str = "e1671797c52e15f763380b45e841ec00";
    std::string *pstrEmpty = &strEmpty;
    std::string *pstrlt32 = &strlt32;
    std::string *pint128str = &int128str;
    // ASSERT_FALSE(v1->appendString(pstrEmpty,10));
    v1->appendString(pstrEmpty, 1);
    ASSERT_EQ(v1->get(3)->getString(), dolphindb::Util::createNullConstant(dolphindb::DT_INT128)->getString());

    ASSERT_FALSE(v1->appendString(pstrlt32, 1));
    v1->appendString(pint128str, 1);
    ASSERT_EQ(v1->get(4)->getString(), int128str);
    ASSERT_TRUE(v1->hasNull());

    char *c1 = (char *)strEmpty.data();
    char *c2 = (char *)strlt32.data();
    char *c3 = (char *)int128str.data();
    char **pc1 = &c1;
    char **pc2 = &c2;
    char **pc3 = &c3;
    // ASSERT_FALSE(v1->appendString(pc1,10));
    v1->appendString(pc1, 1);

    ASSERT_EQ(v1->get(5)->getString(), dolphindb::Util::createNullConstant(dolphindb::DT_INT128)->getString());
    ASSERT_FALSE(v1->appendString(pc2, 1));
    v1->appendString(pc3, 1);

    ASSERT_EQ(v1->get(6)->getString(), c3);
    ASSERT_TRUE(v1->hasNull());
    std::cout << v1->getString() << std::endl;

    v1->getDataArray();
    dolphindb::VectorSP instanceVec = v1->getInstance(1);
    instanceVec->set(0, dolphindb::Util::parseConstant(dolphindb::DT_INT128, "e1671797c52e15f763380b45e841ec00"));
    ASSERT_EQ(instanceVec->size(), 1);
    ASSERT_EQ(instanceVec->getString(), "[e1671797c52e15f763380b45e841ec00]");
    instanceVec = v1->getInstance();
    ASSERT_EQ(instanceVec->size(), 7);
    ASSERT_EQ(v1->getValue(1)->getString(), v1->getString());

    dolphindb::ConstantSP index = dolphindb::Util::createInt(3);
    dolphindb::VectorSP indexVec = dolphindb::Util::createIndexVector(1, 2);
    dolphindb::VectorSP valVec = dolphindb::Util::createVector(dolphindb::DT_INT128, 2, 2);
    valVec->set(0, dolphindb::Util::parseConstant(dolphindb::DT_INT128, "e1671797c52e15f763380b45e841ec66"));
    valVec->set(1, dolphindb::Util::parseConstant(dolphindb::DT_INT128, "e1671797c52e15f763380b45e841ec77"));
    ASSERT_FALSE(v1->set(index, dolphindb::Util::createInt(4)));
    v1->set(index, dolphindb::Util::parseConstant(dolphindb::DT_INT128, "e1671797c52e15f763380b45e841ec88"));
    ASSERT_EQ(v1->get(3)->getString(), "e1671797c52e15f763380b45e841ec88");
    v1->set(index, dolphindb::Util::createNullConstant(dolphindb::DT_INT128));
    ASSERT_TRUE(v1->get(3)->isNull());
    v1->set(indexVec, valVec);
    ASSERT_EQ(v1->getString(), "[e1671797c52e15f763380b45e841ec55,e1671797c52e15f763380b45e841ec66,e1671797c52e15f763380b45e841ec77,,e1671797c52e15f763380b45e841ec00,,e1671797c52e15f763380b45e841ec00]");

    instanceVec->fill(0, 7, v1);
    ASSERT_EQ(instanceVec->getString(), v1->getString());

    ASSERT_FALSE(instanceVec->remove(index));
    instanceVec->remove(indexVec);
    ASSERT_EQ(instanceVec->getString(), "[e1671797c52e15f763380b45e841ec55,,e1671797c52e15f763380b45e841ec00,,e1671797c52e15f763380b45e841ec00]");
    ASSERT_FALSE(instanceVec->remove(10));
    instanceVec->remove(1);
    ASSERT_EQ(instanceVec->getString(), "[e1671797c52e15f763380b45e841ec55,,e1671797c52e15f763380b45e841ec00,]");
    instanceVec->remove(-1);
    ASSERT_EQ(instanceVec->getString(), "[,e1671797c52e15f763380b45e841ec00,]");

    v1->next(4);
    ASSERT_EQ(v1->getString(), "[e1671797c52e15f763380b45e841ec00,,e1671797c52e15f763380b45e841ec00,,,,]");
    v1->prev(4);
    ASSERT_EQ(v1->getString(), "[,,,,e1671797c52e15f763380b45e841ec00,,e1671797c52e15f763380b45e841ec00]");

    dolphindb::VectorSP valVec2 = dolphindb::Util::createVector(dolphindb::DT_INT128, 7, 7);
    for (unsigned int i = 0; i < 7; i++)
        valVec2->set(i, dolphindb::Util::parseConstant(dolphindb::DT_INT128, "e1671797c52e15f763380b45e841ec" + std::to_string(4 * 10 + i)));
    dolphindb::VectorSP valVec3 = valVec2->getSubVector(0, 3);

    ASSERT_FALSE(v1->assign(valVec3));
    v1->assign(valVec2);
    ASSERT_EQ(v1->getValue(7)->getString(), valVec2->getString());
    delete [] buf1;
    delete [] buf2;
}

TEST_F(DataformVectorTest, testInt128nullVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_INT128, 3, 3);
    v1->setNull(0);
    v1->setNull(1);
    v1->set(2, dolphindb::Util::createNullConstant(dolphindb::DT_INT128));
    v1->append(dolphindb::Util::createNullConstant(dolphindb::DT_INT128));
    std::string script = "a=[int128(),int128(),int128(),int128()];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
    for (int i = 0; i < v1->size(); i++)
        ASSERT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testUuidVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_UUID, 2, 2);
    v1->set(0, dolphindb::Util::parseConstant(dolphindb::DT_UUID, "5d212a78-cc48-e3b1-4235-b4d91473ee87"));
    v1->set(1, dolphindb::Util::parseConstant(dolphindb::DT_UUID, "5d212a78-cc48-e3b1-4235-b4d91473ee88"));
    std::string script = "a=[uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87'),uuid('5d212a78-cc48-e3b1-4235-b4d91473ee88')];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());

    ASSERT_ANY_THROW(v1->setString(1, "123"));
    ASSERT_TRUE(v1->isFastMode());
    ASSERT_TRUE(v1->sizeable());
    v1->setNull();
    ASSERT_FALSE(v1->hasNull(0, 1));
    v1->reverse();
    ASSERT_EQ(v1->get(0)->getString(), "5d212a78-cc48-e3b1-4235-b4d91473ee88");
    ASSERT_EQ(v1->get(1)->getString(), "5d212a78-cc48-e3b1-4235-b4d91473ee87");

    ASSERT_ANY_THROW(v1->getBool());
    ASSERT_ANY_THROW(v1->getChar());
    ASSERT_ANY_THROW(v1->getShort());
    ASSERT_ANY_THROW(v1->getLong());
    ASSERT_ANY_THROW(v1->getFloat());
    ASSERT_ANY_THROW(v1->getDouble());
    ASSERT_ANY_THROW(v1->getIndex());
    ASSERT_ANY_THROW(v1->neg());
    ASSERT_ANY_THROW(v1->getBinary());

    // std::cout<<v1->get(dolphindb::Util::createInt(1))<<std::endl;
    ASSERT_EQ(v1->compare(0, dolphindb::Util::parseConstant(dolphindb::DT_UUID, "5d212a78-cc48-e3b1-4235-b4d91473ee87")), 1);
    v1->replace(dolphindb::Util::parseConstant(dolphindb::DT_UUID, "5d212a78-cc48-e3b1-4235-b4d91473ee87"), dolphindb::Util::parseConstant(dolphindb::DT_UUID, "5d212a78-cc48-e3b1-4235-b4d91473ee99"));
    ASSERT_EQ(v1->get(1)->getString(), "5d212a78-cc48-e3b1-4235-b4d91473ee99");

    v1->append(dolphindb::Util::createNullConstant(dolphindb::DT_UUID));
    ASSERT_ANY_THROW(v1->nullFill(dolphindb::Util::createInt(1)));
    v1->nullFill(dolphindb::Util::parseConstant(dolphindb::DT_UUID, "5d212a78-cc48-e3b1-4235-b4d91473ee99"));
    ASSERT_EQ(v1->get(2)->getString(), "5d212a78-cc48-e3b1-4235-b4d91473ee99");

    char *buf1 = new char[3];
    char *buf2 = new char[3];
    v1->isNull(0, 3, buf1);
    ASSERT_FALSE((int)buf1[0]);
    ASSERT_FALSE((int)buf1[1]);
    ASSERT_FALSE((int)buf1[2]);
    v1->isValid(0, 3, buf2);
    ASSERT_TRUE((int)buf2[0]);
    ASSERT_TRUE((int)buf2[1]);
    ASSERT_TRUE((int)buf2[2]);

    std::string strEmpty = "";
    std::string strlt36 = "192.168.1.1";
    std::string uuidstr = "5d212a78-cc48-e3b1-4235-b4d91473ee00";
    std::string *pstrEmpty = &strEmpty;
    std::string *pstrlt36 = &strlt36;
    std::string *puuidstr = &uuidstr;
    // ASSERT_FALSE(v1->appendString(pstrEmpty,10));
    v1->appendString(pstrEmpty, 1);
    ASSERT_EQ(v1->get(3)->getString(), dolphindb::Util::createNullConstant(dolphindb::DT_UUID)->getString());
    ASSERT_FALSE(v1->appendString(pstrlt36, 1));
    v1->appendString(puuidstr, 1);
    ASSERT_EQ(v1->get(4)->getString(), uuidstr);
    ASSERT_TRUE(v1->hasNull());

    char *c1 = (char *)strEmpty.data();
    char *c2 = (char *)strlt36.data();
    char *c3 = (char *)uuidstr.data();
    char **pc1 = &c1;
    char **pc2 = &c2;
    char **pc3 = &c3;
    // ASSERT_FALSE(v1->appendString(pc1,10));
    v1->appendString(pc1, 1);
    ASSERT_EQ(v1->get(5)->getString(), dolphindb::Util::createNullConstant(dolphindb::DT_UUID)->getString());
    ASSERT_FALSE(v1->appendString(pc2, 1));
    v1->appendString(pc3, 1);
    ASSERT_EQ(v1->get(6)->getString(), c3);
    ASSERT_TRUE(v1->hasNull());
    std::cout << v1->getString() << std::endl;
}

TEST_F(DataformVectorTest, testUuidnullVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_UUID, 3, 3);
    v1->setNull(0);
    v1->setNull(1);
    v1->set(2, dolphindb::Util::createNullConstant(dolphindb::DT_UUID));
    v1->append(dolphindb::Util::createNullConstant(dolphindb::DT_UUID));
    std::string script = "a=[uuid(),uuid(),uuid(),uuid()];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
    for (int i = 0; i < v1->size(); i++)
        ASSERT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testIpaddrVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_IP, 2, 2);
    v1->set(0, dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.1.13"));
    v1->set(1, dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.1.15"));
    std::string script = "a=[ipaddr(`192.168.1.13),ipaddr(`192.168.1.15)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());

    ASSERT_ANY_THROW(v1->setString(1, "123"));
    ASSERT_TRUE(v1->isFastMode());
    ASSERT_TRUE(v1->sizeable());
    v1->setNull();
    ASSERT_FALSE(v1->hasNull(0, 1));
    v1->reverse();
    ASSERT_EQ(v1->get(0)->getString(), "192.168.1.15");
    ASSERT_EQ(v1->get(1)->getString(), "192.168.1.13");

    ASSERT_ANY_THROW(v1->getBool());
    ASSERT_ANY_THROW(v1->getChar());
    ASSERT_ANY_THROW(v1->getShort());
    ASSERT_ANY_THROW(v1->getLong());
    ASSERT_ANY_THROW(v1->getFloat());
    ASSERT_ANY_THROW(v1->getDouble());
    ASSERT_ANY_THROW(v1->getIndex());
    ASSERT_ANY_THROW(v1->neg());
    ASSERT_ANY_THROW(v1->getBinary());

    // std::cout<<v1->get(dolphindb::Util::createInt(1))<<std::endl;
    ASSERT_EQ(v1->compare(0, dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.1.13")), 1);
    v1->replace(dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.1.13"), dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.1.255"));
    ASSERT_EQ(v1->get(1)->getString(), "192.168.1.255");

    v1->append(dolphindb::Util::createNullConstant(dolphindb::DT_IP));
    ASSERT_ANY_THROW(v1->nullFill(dolphindb::Util::createInt(1)));
    v1->nullFill(dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.1.255"));
    ASSERT_EQ(v1->get(2)->getString(), "192.168.1.255");

    char *buf1 = new char[3];
    char *buf2 = new char[3];
    v1->isNull(0, 3, buf1);
    ASSERT_FALSE((int)buf1[0]);
    ASSERT_FALSE((int)buf1[1]);
    ASSERT_FALSE((int)buf1[2]);
    v1->isValid(0, 3, buf2);
    ASSERT_TRUE((int)buf2[0]);
    ASSERT_TRUE((int)buf2[1]);
    ASSERT_TRUE((int)buf2[2]);

    std::string strEmpty = "";
    std::string errstr = "256.256.0.0";
    std::string ipstr = "192.168.1.1";
    std::string *pstrEmpty = &strEmpty;
    std::string *perrstr = &errstr;
    std::string *pipstr = &ipstr;
    // ASSERT_FALSE(v1->appendString(pstrEmpty,10));
    v1->appendString(pstrEmpty, 1);
    ASSERT_EQ(v1->get(3)->getString(), dolphindb::Util::createNullConstant(dolphindb::DT_IP)->getString());
    ASSERT_FALSE(v1->appendString(perrstr, 1));
    v1->appendString(pipstr, 1);
    ASSERT_EQ(v1->get(4)->getString(), ipstr);
    ASSERT_TRUE(v1->hasNull());

    char *c1 = (char *)strEmpty.data();
    char *c2 = (char *)errstr.data();
    char *c3 = (char *)ipstr.data();
    char **pc1 = &c1;
    char **pc2 = &c2;
    char **pc3 = &c3;
    // ASSERT_FALSE(v1->appendString(pc1,10));
    v1->appendString(pc1, 1);
    ASSERT_EQ(v1->get(5)->getString(), dolphindb::Util::createNullConstant(dolphindb::DT_IP)->getString());
    ASSERT_FALSE(v1->appendString(pc2, 1));
    v1->appendString(pc3, 1);
    ASSERT_EQ(v1->get(6)->getString(), c3);
    ASSERT_TRUE(v1->hasNull());
    std::cout << v1->getString() << std::endl;
}

TEST_F(DataformVectorTest, testIpaddrnullVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_IP, 3, 3);
    v1->setNull(0);
    v1->setNull(1);
    v1->set(2, dolphindb::Util::createNullConstant(dolphindb::DT_IP));
    v1->append(dolphindb::Util::createNullConstant(dolphindb::DT_IP));
    std::string script = "a=[ipaddr(),ipaddr(),ipaddr(),ipaddr()];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(v1->getType(), res_v->getType());
    for (int i = 0; i < v1->size(); i++)
        ASSERT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testSymbolVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_SYMBOL, 2, 2);
    v1->set(0, dolphindb::Util::createString("sym1"));
    v1->set(1, dolphindb::Util::createString("sym2"));
    std::string script = "a=symbol[`sym1,`sym2];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});

    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(conn.run("v1")->getType(), res_v->getType());

    ASSERT_ANY_THROW(v1->getBool());
    ASSERT_ANY_THROW(v1->getChar());
    ASSERT_ANY_THROW(v1->getShort());
    ASSERT_ANY_THROW(v1->getLong());
    ASSERT_ANY_THROW(v1->getFloat());
    ASSERT_ANY_THROW(v1->getDouble());
    ASSERT_ANY_THROW(v1->getIndex());
    ASSERT_ANY_THROW(v1->neg());

    ASSERT_FALSE(v1->isIndexArray());
    ASSERT_EQ(v1->getIndexArray(), (dolphindb::INDEX *)NULL);
    ASSERT_EQ(v1->asof(dolphindb::Util::createString("sym1")), 0);
    ASSERT_EQ(v1->asof(dolphindb::Util::createString("sym2")), 1);

    ASSERT_EQ(v1->getUnitLength(), 4);
    ASSERT_TRUE(v1->sizeable());
    ASSERT_EQ(v1->compare(1, dolphindb::Util::createString("sym2")), 0);
    ASSERT_EQ(v1->getStringRef(), "sym1");
    v1->setNull(); // nothing to do
    ASSERT_FALSE(v1->hasNull());

    v1->reverse();
    ASSERT_EQ(v1->get(0)->getString(), dolphindb::Util::createString("sym2")->getString());
    ASSERT_EQ(v1->get(1)->getString(), dolphindb::Util::createString("sym1")->getString());
    v1->reverse(0, 1);
    ASSERT_EQ(v1->get(1)->getString(), dolphindb::Util::createString("sym2")->getString());
    ASSERT_EQ(v1->get(0)->getString(), dolphindb::Util::createString("sym1")->getString());

    std::cout << v1->getDataArray() << std::endl;
    v1->append(dolphindb::Util::createNullConstant(dolphindb::DT_STRING));
    v1->nullFill(dolphindb::Util::createString("sym3"));
    ASSERT_EQ(v1->get(2)->getString(), "sym3");
    ASSERT_EQ(v1->getSubVector(1, 2)->getString(), "[\"sym2\",\"sym3\"]");
    ASSERT_EQ(v1->getSubVector(1, 2, 2)->getString(), "[\"sym2\",\"sym3\"]");
    ASSERT_ANY_THROW(v1->getSubVector(0, 2, -1)->getString());

    dolphindb::VectorSP instanceVec = v1->getInstance(2);
    instanceVec->set(0, dolphindb::Util::createString("1"));
    instanceVec->set(1, dolphindb::Util::createString("2"));
    ASSERT_EQ(instanceVec->getString(), "[\"1\",\"2\"]");

    ASSERT_FALSE(v1->append(dolphindb::Util::createString("1"), 2));
    v1->append(dolphindb::Util::createString("sym4"), 1);
    std::string res = v1->get(3)->getString();
    ASSERT_EQ(res, "sym4");

    std::string strEmpty = "";
    std::string appendstr = "sym5";
    std::string *pstrEmpty = &strEmpty;
    std::string *pappendstr = &appendstr;
    v1->appendString(pstrEmpty, 1);
    ASSERT_EQ(v1->get(4)->getString(), "");
    v1->appendString(pappendstr, 1);
    ASSERT_EQ(v1->get(5)->getString(), appendstr);
    ASSERT_TRUE(v1->hasNull());

    char *c1 = (char *)strEmpty.data();
    char *c2 = (char *)appendstr.data();
    char **pc1 = &c1;
    char **pc2 = &c2;
    v1->appendString(pc1, 1);
    ASSERT_EQ(v1->get(6)->getString(), "");

    v1->setString("");
    ASSERT_EQ(v1->get(0)->getString(), "");

    std::string bufstring = "sym1";
    char *pbuf = (char *)bufstring.data();
    char **strbuf = &pbuf;
    ASSERT_FALSE(v1->setString(0, 10, strbuf));
    v1->setString(0, 1, strbuf);
    ASSERT_EQ(v1->get(0)->getString(), "sym1");

    v1->replace(dolphindb::Util::createString(""), dolphindb::Util::createString("replaceVal"));

    dolphindb::ConstantSP indexVal = dolphindb::Util::createInt(0);
    dolphindb::VectorSP indexVec = dolphindb::Util::createIndexVector(1, 1);
    dolphindb::VectorSP intIndexVec = dolphindb::Util::createVector(dolphindb::DT_INT, 0, 1);
    intIndexVec->append(dolphindb::Util::createInt(2));

    v1->set(indexVal, dolphindb::Util::createNullConstant(dolphindb::DT_STRING));
    v1->set(indexVec, dolphindb::Util::createString("indexVec"));
    v1->set(intIndexVec, dolphindb::Util::createString("intIndexVec"));
    ASSERT_TRUE(v1->hasNull());
    ASSERT_EQ(v1->getString(), "[,\"indexVec\",\"intIndexVec\",\"sym4\",\"replaceVal\",\"sym5\",\"replaceVal\"]");

    dolphindb::VectorSP valVec1 = dolphindb::Util::createVector(dolphindb::DT_STRING, 2, 2);
    dolphindb::VectorSP valVec2 = dolphindb::Util::createVector(dolphindb::DT_STRING, 1, 1);
    dolphindb::ConstantSP val1 = dolphindb::Util::createString("value1");
    valVec1->set(0, dolphindb::Util::createString("vec1"));
    valVec1->setNull(1);
    valVec2->set(0, dolphindb::Util::createString("vec2"));
    v1->fill(0, 1, val1);
    v1->fill(1, 1, valVec1);
    v1->fill(2, 2, valVec2);
    v1->fill(4, 2, valVec1);
    ASSERT_EQ(v1->getString(), "[\"value1\",\"[\\\"vec1\\\",]\",\"[\\\"vec2\\\"]\",\"[\\\"vec2\\\"]\",\"vec1\",,\"replaceVal\"]");

    ASSERT_TRUE(v1->validIndex(-1));
    ASSERT_FALSE(v1->validIndex(5));

    char **buf = new char *[v1->size()];
    v1->getStringConst(0, v1->size(), buf);
    for (auto i = 0; i < v1->size(); i++)
        ASSERT_EQ(buf[i], v1->get(i)->getString());

    delete[] buf;
}

TEST_F(DataformVectorTest, testSymbolnullVector)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_SYMBOL, 2, 2);
    v1->setNull(0);
    v1->setNull(1);
    std::string script = "a=symbol['',''];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getScript(), res_v->getScript());
    ASSERT_EQ(conn.run("v1")->getType(), res_v->getType());
    for (int i = 0; i < 2; i++)
        ASSERT_EQ(v1->getItem(i)->getString(), res_v->getItem(i)->getString());
}

TEST_F(DataformVectorTest, testDecimal32Vector)
{
    ASSERT_ANY_THROW(dolphindb::VectorSP v0 = dolphindb::Util::createVector(dolphindb::DT_DECIMAL32, 2, 2, true, 10));
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DECIMAL32, 2, 2, true, 2);
    v1->set(0, dolphindb::Util::createDecimal32(2, 0.315));
    v1->set(1, dolphindb::Util::createDecimal32(2, 3.1));

    std::string script = "a=[decimal32(0.31,2),decimal32(3.10,2)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getString(), res_v->getString());
    ASSERT_EQ(v1->getType(), res_v->getRawType());
    ASSERT_EQ(dolphindb::Util::getCategoryString(v1->getCategory()), "DENARY");
    ASSERT_EQ(v1->getExtraParamForType(), 2);
    ASSERT_EQ(v1->getString(1), res_v->get(1)->getString());

    float buf1[1];
    ASSERT_EQ(v1->getFloat(1), res_v->get(1)->getFloat());
    v1->getFloat(0, 1, buf1);
    ASSERT_EQ((float)buf1[0], res_v->get(0)->getFloat());
    double buf2[1];
    ASSERT_EQ(v1->getDouble(1), res_v->get(1)->getDouble());
    v1->getDouble(0, 1, buf2);
    ASSERT_EQ((double)buf2[0], res_v->get(0)->getDouble());

    v1->setFloat(0, 0.999);
    v1->setDouble(1, 2.5);
    ASSERT_EQ(v1->getString(), "[0.99,2.50]");
    const float buf3[1] = {4.1};
    const double buf4[1] = {5.6320001};
    v1->setFloat(0, 1, buf3);
    v1->setDouble(1, 1, buf4);
    ASSERT_EQ(v1->getString(), "[4.09,5.63]");

    v1->set(0, dolphindb::Util::createDecimal32(1, 1.2322));
    v1->set(1, dolphindb::Util::createDecimal32(5, 3.15));
    ASSERT_EQ(v1->getType(), dolphindb::DT_DECIMAL32);
    ASSERT_EQ(v1->getString(), "[1.20,3.15]");

    dolphindb::VectorSP indexV = dolphindb::Util::createIndexVector(0, 2);
    dolphindb::VectorSP valV = dolphindb::Util::createVector(dolphindb::DT_DECIMAL32, 2, 2);
    dolphindb::VectorSP errTypevalV = dolphindb::Util::createVector(dolphindb::DT_NANOTIME, 1, 1);
    dolphindb::VectorSP diffScalevalV = dolphindb::Util::createVector(dolphindb::DT_DECIMAL32, 2, 2); // default scale=0
    valV->set(0, dolphindb::Util::createDecimal32(2, 13.0582));
    valV->setNull(1);
    errTypevalV->set(0, dolphindb::Util::createNanoTime(1000000000000));
    diffScalevalV->set(0, dolphindb::Util::createDecimal32(5, 1.5));
    diffScalevalV->set(1, dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL32));
    std::string strbuf[2] = {"0", "-1356.14655"};
    std::string strerrbuf[1] = {"abcdefg"};
    char *charbuf[3] = {(char *)"9.4856", (char *)"84912.39123", (char *)"31945.1"};
    char *charerrbuf[1] = {(char *)"abcdefg"};

    v1->append(dolphindb::Util::createDecimal32(2, 1.5));
    v1->set(2, dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL32));
    v1->append(dolphindb::Util::createDecimal32(2, 1.5326), 3);
    v1->append(dolphindb::Util::createDecimal32(2, 3.12123), 1);
    ASSERT_EQ(v1->getString(), "[1.20,3.15,,1.53,1.53,1.53,3.12]");
    v1->append(dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL32), 1);
    ASSERT_EQ(v1->getString(), "[1.20,3.15,,1.53,1.53,1.53,3.12,]");
    ASSERT_ANY_THROW(v1->append(errTypevalV, 1));
    v1->append(diffScalevalV, 2);
    ASSERT_EQ(v1->getString(), "[1.20,3.15,,1.53,1.53,1.53,3.12,,1.00,]");
    v1->append(valV, 2);
    ASSERT_EQ(v1->getString(), "[1.20,3.15,,1.53,1.53,1.53,3.12,,1.00,,13.00,]");
    v1->appendString(charerrbuf, 1);
    v1->appendString(charbuf, 3);
    ASSERT_EQ(v1->getString(), "[1.20,3.15,,1.53,1.53,1.53,3.12,,1.00,,13.00,,,9.49,84912.39,31945.10]");
    v1->appendString(strerrbuf, 1);
    v1->appendString(strbuf, 2);
    ASSERT_EQ(v1->getString(), "[1.20,3.15,,1.53,1.53,1.53,3.12,,1.00,,13.00,,,9.49,84912.39,31945.10,,0.00,-1356.15]");

    conn.upload("v1", v1);
    ASSERT_TRUE(conn.run("eqObj(v1,decimal32([1.20,3.15,NULL,1.53,1.53,1.53,3.12,NULL,1.00,NULL,13.00,NULL,NULL,9.49,84912.39,31945.10,NULL,0.00,-1356.15],2))")->getBool());

    v1->nullFill(dolphindb::Util::createFloat(4.3335));
    ASSERT_EQ(v1->getString(), "[1.20,3.15,4.33,1.53,1.53,1.53,3.12,4.33,1.00,4.33,13.00,4.33,4.33,9.49,84912.39,31945.10,4.33,0.00,-1356.15]");

    dolphindb::VectorSP v2 = v1->getSubVector(0, 2, 10);
    std::cout << v2->getString() << std::endl;
    ASSERT_TRUE(v2->set(dolphindb::Util::createInt(0), dolphindb::Util::createDecimal32(2, 2.7999)));
    ASSERT_ANY_THROW(v2->set(dolphindb::Util::createIndexVector(1, 1), errTypevalV));
    v2->set(dolphindb::Util::createIndexVector(1, 1), diffScalevalV);
    ASSERT_EQ(v2->getString(), "[2.79,1.00]");
    v2->set(indexV, valV);
    ASSERT_EQ(v2->getString(), "[13.00,]");

    ASSERT_TRUE(v2->get(1)->isNull());
    // ASSERT_TRUE(v2->get(dolphindb::Util::createInt(1))->isNull());
    // ASSERT_TRUE(v2->get(dolphindb::Util::createIndexVector(1,1))->isNull());

    ASSERT_ANY_THROW(v2->fill(1, 5, valV));
    v2->fill(1, 1, dolphindb::Util::createDecimal32(5, 0));
    ASSERT_EQ(v2->getString(), "[13.00,0.00]");
    v2->fill(0, 2, dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL32));
    ASSERT_EQ(v2->getString(), "[,]");
    ASSERT_ANY_THROW(v2->fill(1, 1, errTypevalV));
    v2->fill(0, 2, diffScalevalV);
    ASSERT_EQ(v2->getString(), "[1.00,]");
    v2->fill(0, 2, valV);
    ASSERT_EQ(v2->getString(), "[13.00,]");

    ASSERT_FALSE(v2->validIndex(1));
    v2->fill(1, 1, dolphindb::Util::createDecimal32(5, 0));
    ASSERT_TRUE(v2->validIndex(-1));

    ASSERT_EQ(v2->compare(0, dolphindb::Util::createInt(12)), 1);
    ASSERT_EQ(v2->compare(1, dolphindb::Util::createInt(1)), -1);
    ASSERT_EQ(v2->compare(1, dolphindb::Util::createInt(0)), 0);
    ASSERT_EQ(v2->compare(0, dolphindb::Util::createDecimal32(3, 12.99)), 1);
    ASSERT_EQ(v2->compare(0, dolphindb::Util::createDecimal32(2, 13.01)), -1);
    ASSERT_EQ(v2->compare(1, dolphindb::Util::createDecimal32(1, 0)), 0);
    ASSERT_EQ(v2->compare(0, dolphindb::Util::createDecimal64(8, 12.999999999)), 1);
    ASSERT_EQ(v2->compare(0, dolphindb::Util::createDecimal64(10, 13.00001)), -1);
    ASSERT_EQ(v2->compare(1, dolphindb::Util::createDecimal64(1, 0)), 0);
    ASSERT_ANY_THROW(v2->compare(0, dolphindb::Util::createBlob("blob1")));
    v2->nullFill(dolphindb::Util::createInt(1));
}

TEST_F(DataformVectorTest, testDecimal32NullVector)
{
    for (int i = 0; i < 10; i++)
    {
        dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DECIMAL32, 2, 2, true, i);
        v1->set(0, dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL32));
        v1->set(1, dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL32));

        std::string script = "a=[decimal32(NULL," + std::to_string(i) + "),decimal32(NULL," + std::to_string(i) + ")];a";
        dolphindb::VectorSP res_v = conn.run(script);
        conn.upload("v1", {v1});
        ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
        ASSERT_EQ(v1->getString(), res_v->getString());
        ASSERT_EQ(v1->getType(), res_v->getRawType());
        ASSERT_EQ(dolphindb::Util::getCategoryString(v1->getCategory()), "DENARY");
        ASSERT_EQ(v1->getExtraParamForType(), i);
    }
}

TEST_F(DataformVectorTest, testDecimal64Vector)
{
    ASSERT_ANY_THROW(dolphindb::VectorSP v0 = dolphindb::Util::createVector(dolphindb::DT_DECIMAL64, 2, 2, true, 19));
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DECIMAL64, 2, 2, true, 16);
    v1->set(0, dolphindb::Util::createDecimal64(16, 0.315));
    v1->set(1, dolphindb::Util::createDecimal64(16, 3.1));

    std::string script = "a=[decimal64(0.315,16),decimal64(3.1,16)];a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getString(), res_v->getString());
    ASSERT_EQ(v1->getType(), res_v->getRawType());
    ASSERT_EQ(dolphindb::Util::getCategoryString(v1->getCategory()), "DENARY");
    ASSERT_EQ(v1->getExtraParamForType(), 16);
    ASSERT_EQ(v1->getString(1), res_v->get(1)->getString());

    float buf1[1];
    ASSERT_EQ(v1->getFloat(1), res_v->get(1)->getFloat());
    v1->getFloat(0, 1, buf1);
    ASSERT_EQ((float)buf1[0], res_v->get(0)->getFloat());
    double buf2[1];
    ASSERT_EQ(v1->getDouble(1), res_v->get(1)->getDouble());
    v1->getDouble(0, 1, buf2);
    ASSERT_EQ((double)buf2[0], res_v->get(0)->getDouble());

    v1->setFloat(0, 0.999);
    v1->setDouble(1, 2.5);
    std::string re0 = v1->get(0)->getString();
    std::string re1 = v1->get(1)->getString();
    size_t re0_scale = re0.length() - re0.find(".") - 1;
    size_t re1_scale = re1.length() - re1.find(".") - 1;
    ASSERT_EQ(re0_scale, 16);
    ASSERT_EQ(re1_scale, 16);
    double re0_val = stod(re0);
    double re1_val = stod(re1);
    ASSERT_NEAR(re0_val, 0.999, 0.001f);
    ASSERT_NEAR(re1_val, 2.5, 0.1f);


    const float buf3[1] = {4.1};
    const double buf4[1] = {5.6320001};
    v1->setFloat(0, 1, buf3);
    v1->setDouble(1, 1, buf4);
    re0 = v1->get(0)->getString();
    re1 = v1->get(1)->getString();
    re0_scale = re0.length() - re0.find(".") - 1;
    re1_scale = re1.length() - re1.find(".") - 1;
    ASSERT_EQ(re0_scale, 16);
    ASSERT_EQ(re1_scale, 16);
    re0_val = stod(re0);
    re1_val = stod(re1);
    ASSERT_NEAR(re0_val, 4.1, 0.1);
    ASSERT_NEAR(re1_val, 5.6320001, 0.001);

    v1->set(0, dolphindb::Util::createDecimal64(1, 1.2322));
    v1->set(1, dolphindb::Util::createDecimal64(5, 3.15));
    ASSERT_EQ(v1->getType(), dolphindb::DT_DECIMAL64);
    ASSERT_EQ(v1->getString(), "[1.2000000000000000,3.1500000000000000]");

    dolphindb::VectorSP indexV = dolphindb::Util::createIndexVector(0, 2);
    dolphindb::VectorSP valV = dolphindb::Util::createVector(dolphindb::DT_DECIMAL64, 2, 2, true, 16);
    dolphindb::VectorSP errTypevalV = dolphindb::Util::createVector(dolphindb::DT_NANOTIME, 1, 1);
    dolphindb::VectorSP diffScalevalV = dolphindb::Util::createVector(dolphindb::DT_DECIMAL64, 2, 2); // default scale=0
    dolphindb::ConstantSP strval = dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL64, 16);
    strval->setString("1.146556453894532645");
    valV->set(0, strval);
    valV->setNull(1);
    errTypevalV->set(0, dolphindb::Util::createNanoTime(1000000000000));
    diffScalevalV->set(0, dolphindb::Util::createDecimal64(16, 1.5));
    diffScalevalV->set(1, dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL64));
    std::string strbuf[2] = {"0", "-1.146556453894532645"};
    std::string strerrbuf[1] = {"abcdefg"};
    char *charbuf[3] = {(char *)"9.4856", (char *)"-0.135468384653648488", (char *)"99.1"};
    char *charerrbuf[1] = {(char *)"abcdefg"};

    v1->append(dolphindb::Util::createDecimal64(2, 1.5));
    v1->set(2, dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL64));
    v1->append(dolphindb::Util::createDecimal64(2, 1.5), 3);
    v1->append(dolphindb::Util::createDecimal64(2, 3.12123), 1);
    ASSERT_EQ(v1->getString(), "[1.2000000000000000,3.1500000000000000,,1.5000000000000000,1.5000000000000000,1.5000000000000000,3.1200000000000000]");
    v1->append(dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL64), 1);
    ASSERT_EQ(v1->getString(), "[1.2000000000000000,3.1500000000000000,,1.5000000000000000,1.5000000000000000,1.5000000000000000,3.1200000000000000,]");
    ASSERT_ANY_THROW(v1->append(errTypevalV, 1));
    v1->append(diffScalevalV, 2);
    ASSERT_EQ(v1->getString(), "[1.2000000000000000,3.1500000000000000,,1.5000000000000000,1.5000000000000000,1.5000000000000000,3.1200000000000000,,1.0000000000000000,]");
    v1->append(valV, 2);
    ASSERT_EQ(v1->getString(), "[1.2000000000000000,3.1500000000000000,,1.5000000000000000,1.5000000000000000,1.5000000000000000,3.1200000000000000,,1.0000000000000000,,1.1465564538945326,]");
    v1->appendString(charerrbuf, 1);
    v1->appendString(charbuf, 3);

    ASSERT_EQ(v1->getString(), "[1.2000000000000000,3.1500000000000000,,1.5000000000000000,1.5000000000000000,1.5000000000000000,3.1200000000000000,,1.0000000000000000,,1.1465564538945326,,,9.4856000000000000,-0.1354683846536485,99.1000000000000000]");
    v1->appendString(strerrbuf, 1);
    v1->appendString(strbuf, 2);

    ASSERT_EQ(v1->getString(), "[1.2000000000000000,3.1500000000000000,,1.5000000000000000,1.5000000000000000,1.5000000000000000,3.1200000000000000,,1.0000000000000000,,1.1465564538945326,,,9.4856000000000000,-0.1354683846536485,99.1000000000000000,,0.0000000000000000,-1.1465564538945326]");

    conn.upload("v1", v1);
    ASSERT_TRUE(conn.run("eqObj(v1,decimal64(['1.2','3.15',string(NULL),'1.5','1.5','1.5','3.12',string(NULL),string(1),string(NULL),`1.146556453894532645,string(NULL),string(NULL),'9.4856','-0.135468384653648488',`99.1,string(NULL),string(0),'-1.146556453894532645'],16))")->getBool());

    v1->nullFill(dolphindb::Util::createFloat(0));
    ASSERT_EQ(v1->getString(), "[1.2000000000000000,3.1500000000000000,0.0000000000000000,1.5000000000000000,1.5000000000000000,1.5000000000000000,3.1200000000000000,0.0000000000000000,1.0000000000000000,0.0000000000000000,1.1465564538945326,0.0000000000000000,0.0000000000000000,9.4856000000000000,-0.1354683846536485,99.1000000000000000,0.0000000000000000,0.0000000000000000,-1.1465564538945326]");

    dolphindb::VectorSP v2 = v1->getSubVector(0, 2, 10);
    ASSERT_TRUE(v2->set(dolphindb::Util::createInt(0), dolphindb::Util::createDecimal64(2, 2.7999)));
    ASSERT_ANY_THROW(v2->set(dolphindb::Util::createIndexVector(1, 1), errTypevalV));
    v2->set(dolphindb::Util::createIndexVector(1, 1), diffScalevalV);
    ASSERT_EQ(v2->getString(), "[2.7900000000000000,1.0000000000000000]");
    v2->set(indexV, valV);
    ASSERT_EQ(v2->getString(), "[1.1465564538945326,]");

    ASSERT_TRUE(v2->get(1)->isNull());
    // ASSERT_TRUE(v2->get(dolphindb::Util::createInt(1))->isNull());
    // ASSERT_TRUE(v2->get(dolphindb::Util::createIndexVector(1,1))->isNull());

    ASSERT_ANY_THROW(v2->fill(1, 5, valV));
    v2->fill(1, 1, dolphindb::Util::createDecimal64(5, 0));
    ASSERT_EQ(v2->getString(), "[1.1465564538945326,0.0000000000000000]");
    v2->fill(0, 2, dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL64));
    ASSERT_EQ(v2->getString(), "[,]");
    ASSERT_ANY_THROW(v2->fill(1, 1, errTypevalV));
    v2->fill(0, 2, diffScalevalV);
    ASSERT_EQ(v2->getString(), "[1.0000000000000000,]");
    v2->fill(0, 2, valV);
    ASSERT_EQ(v2->getString(), "[1.1465564538945326,]");

    ASSERT_FALSE(v2->validIndex(1));
    v2->fill(1, 1, dolphindb::Util::createDecimal64(5, 0));
    ASSERT_TRUE(v2->validIndex(-1));

    ASSERT_EQ(v2->compare(0, dolphindb::Util::createInt(1.09)), 1);
    ASSERT_EQ(v2->compare(1, dolphindb::Util::createInt(1)), -1);
    ASSERT_EQ(v2->compare(1, dolphindb::Util::createInt(0)), 0);
    ASSERT_EQ(v2->compare(0, dolphindb::Util::createDecimal32(3, 1.099)), 1);
    ASSERT_EQ(v2->compare(0, dolphindb::Util::createDecimal32(2, 1.2)), -1);
    ASSERT_EQ(v2->compare(1, dolphindb::Util::createDecimal32(1, 0)), 0);
    ASSERT_EQ(v2->compare(0, dolphindb::Util::createDecimal64(8, 1.09999999)), 1);
    ASSERT_EQ(v2->compare(0, dolphindb::Util::createDecimal64(10, 1.2000000001)), -1);
    ASSERT_EQ(v2->compare(1, dolphindb::Util::createDecimal64(1, 0)), 0);
    ASSERT_ANY_THROW(v2->compare(0, dolphindb::Util::createBlob("blob1")));
    v2->nullFill(dolphindb::Util::createInt(1));
}

TEST_F(DataformVectorTest, testDecimal64NullVector)
{
    for (int i = 0; i < 19; i++)
    {
        dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DECIMAL64, 2, 2, true, i);
        v1->set(0, dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL64));
        v1->set(1, dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL64));

        std::string script = "a=[decimal64(NULL," + std::to_string(i) + "),decimal64(NULL," + std::to_string(i) + ")];a";
        dolphindb::VectorSP res_v = conn.run(script);
        conn.upload("v1", {v1});
        ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
        ASSERT_EQ(v1->getString(), res_v->getString());
        ASSERT_EQ(v1->getType(), res_v->getRawType());
        ASSERT_EQ(dolphindb::Util::getCategoryString(v1->getCategory()), "DENARY");
        ASSERT_EQ(v1->getExtraParamForType(), i);
    }
}

TEST_F(DataformVectorTest, testDecimal128Vector)
{
    ASSERT_ANY_THROW(dolphindb::VectorSP v0 = dolphindb::Util::createVector(dolphindb::DT_DECIMAL128, 2, 2, true, 39));
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DECIMAL128, 2, 2, true, 26);
    dolphindb::ConstantSP val1 = dolphindb::Util::createConstant(dolphindb::DT_DECIMAL128, 26);
    dolphindb::ConstantSP val2 = dolphindb::Util::createConstant(dolphindb::DT_DECIMAL128, 26);
    val1->setString("0.195662");
    val2->setString("-1.4566253625221653233545652333333");
    v1->set(0, val1);
    v1->set(1, val2);

    std::string script = "a=decimal128('0.195662' '-1.4566253625221653233545652333333', 26);a";
    dolphindb::VectorSP res_v = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
    ASSERT_EQ(v1->getString(), res_v->getString());
    ASSERT_EQ(v1->getType(), res_v->getRawType());
    ASSERT_EQ(dolphindb::Util::getCategoryString(v1->getCategory()), "DENARY");
    ASSERT_EQ(v1->getExtraParamForType(), 26);
    ASSERT_EQ(v1->getString(1), res_v->get(1)->getString());

    float buf1[1];
    ASSERT_EQ(v1->getFloat(1), res_v->get(1)->getFloat());
    v1->getFloat(0, 1, buf1);
    ASSERT_EQ((float)buf1[0], res_v->get(0)->getFloat());
    double buf2[1];
    ASSERT_EQ(v1->getDouble(1), res_v->get(1)->getDouble());
    v1->getDouble(0, 1, buf2);
    ASSERT_EQ((double)buf2[0], res_v->get(0)->getDouble());

    v1->setFloat(0, 0.999);
    v1->setDouble(1, 2.5);
    std::string re0 = v1->get(0)->getString();
    std::string re1 = v1->get(1)->getString();
    size_t re0_scale = re0.length() - re0.find(".") - 1;
    size_t re1_scale = re1.length() - re1.find(".") - 1;
    ASSERT_EQ(re0_scale, 26);
    ASSERT_EQ(re1_scale, 26);
    double re0_val = stod(re0);
    double re1_val = stod(re1);
    ASSERT_NEAR(re0_val, 0.999, 0.001);
    ASSERT_NEAR(re1_val, 2.5, 0.1);

    const float buf3[1] = {4.1};
    const double buf4[1] = {5.6320001};
    v1->setFloat(0, 1, buf3);
    v1->setDouble(1, 1, buf4);
    re0 = v1->get(0)->getString();
    re1 = v1->get(1)->getString();
    re0_scale = re0.length() - re0.find(".") - 1;
    re1_scale = re1.length() - re1.find(".") - 1;
    ASSERT_EQ(re0_scale, 26);
    ASSERT_EQ(re1_scale, 26);
    re0_val = stod(re0);
    re1_val = stod(re1);
    ASSERT_NEAR(re0_val, 4.1, 0.1);
    ASSERT_NEAR(re1_val, 5.6320001, 0.001);

    v1->set(0, dolphindb::Util::createDecimal128(1, 1.2322));
    v1->set(1, dolphindb::Util::createDecimal128(5, 3.15f));
    ASSERT_EQ(v1->getType(), dolphindb::DT_DECIMAL128);
    ASSERT_EQ(v1->getString(), "[1.20000000000000000000000000,3.15000000000000000000000000]");

    dolphindb::VectorSP indexV = dolphindb::Util::createIndexVector(0, 2);
    dolphindb::VectorSP valV = dolphindb::Util::createVector(dolphindb::DT_DECIMAL128, 2, 2, true, 26);
    dolphindb::VectorSP errTypevalV = dolphindb::Util::createVector(dolphindb::DT_NANOTIME, 1, 1);
    dolphindb::VectorSP diffScalevalV = dolphindb::Util::createVector(dolphindb::DT_DECIMAL128, 2, 2); // default scale=0
    dolphindb::ConstantSP strval = dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL128, 26);

    strval->setString("100000000.31624525345261230126453256");
    valV->set(0, strval);
    valV->setNull(1);

    errTypevalV->set(0, dolphindb::Util::createNanoTime(1000000000000));
    diffScalevalV->set(0, dolphindb::Util::createDecimal128(5, 1.5));
    diffScalevalV->set(1, dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL128));
    std::string strbuf[3] = {"0", "-1.6524349686135264535463213236", "0.000000000000000000000000000000001"};
    std::string strerrbuf[1] = {"abcdefg"};
    char *charbuf[3] = {(char *)"9.4856645683462645345624983524568", (char *)"0", (char *)"9999999999.123"};
    char *charerrbuf[1] = {(char *)"abcdefg"};

    v1->append(dolphindb::Util::createDecimal128(2, 1.5));
    v1->set(2, dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL128));
    std::cout << v1->getString() << std::endl;
    v1->append(dolphindb::Util::createDecimal128(2, 1.5), 3);
    v1->append(dolphindb::Util::createDecimal128(2, 3.12123), 1);
    ASSERT_EQ(v1->getString(), "[1.20000000000000000000000000,3.15000000000000000000000000,,1.50000000000000000000000000,1.50000000000000000000000000,1.50000000000000000000000000,3.12000000000000000000000000]");

    v1->append(dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL128), 1);
    ASSERT_EQ(v1->getString(), "[1.20000000000000000000000000,3.15000000000000000000000000,,1.50000000000000000000000000,1.50000000000000000000000000,1.50000000000000000000000000,3.12000000000000000000000000,]");
    v1->append(errTypevalV, 1);
    v1->append(diffScalevalV, 2);

    ASSERT_EQ(v1->getString(), "[1.20000000000000000000000000,3.15000000000000000000000000,,1.50000000000000000000000000,1.50000000000000000000000000,1.50000000000000000000000000,3.12000000000000000000000000,,1000000000000.00000000000000000000000000,1.00000000000000000000000000,]");
    v1->append(valV, 2);

    ASSERT_EQ(v1->getString(), "[1.20000000000000000000000000,3.15000000000000000000000000,,1.50000000000000000000000000,1.50000000000000000000000000,1.50000000000000000000000000,3.12000000000000000000000000,,1000000000000.00000000000000000000000000,1.00000000000000000000000000,,100000000.31624525345261230126453256,]");
    v1->appendString(charerrbuf, 1);
    v1->appendString(charbuf, 3);

    ASSERT_EQ(v1->getString(), "[1.20000000000000000000000000,3.15000000000000000000000000,,1.50000000000000000000000000,1.50000000000000000000000000,1.50000000000000000000000000,3.12000000000000000000000000,,1000000000000.00000000000000000000000000,1.00000000000000000000000000,,100000000.31624525345261230126453256,,,9.48566456834626453456249835,0.00000000000000000000000000,9999999999.12300000000000000000000000]");
    v1->appendString(strerrbuf, 1);
    v1->appendString(strbuf, 3);

    ASSERT_EQ(v1->getString(), "[1.20000000000000000000000000,3.15000000000000000000000000,,1.50000000000000000000000000,1.50000000000000000000000000,1.50000000000000000000000000,3.12000000000000000000000000,,1000000000000.00000000000000000000000000,1.00000000000000000000000000,,100000000.31624525345261230126453256,,,9.48566456834626453456249835,0.00000000000000000000000000,9999999999.12300000000000000000000000,,0.00000000000000000000000000,-1.65243496861352645354632132,0.00000000000000000000000000]");

    conn.upload("v1", v1);
    ASSERT_TRUE(conn.run("eqObj(v1,array(DECIMAL128(26)).append!(decimal128([`1.20000000000000000000000000,`3.15000000000000000000000000,NULL,`1.50000000000000000000000000,`1.50000000000000000000000000,`1.50000000000000000000000000,`3.12000000000000000000000000,NULL,1000000000000,`1.00000000000000000000000000,NULL,`100000000.31624525345261230126453256,NULL,NULL,`9.48566456834626453456249835,`0.00000000000000000000000000,`9999999999.12300000000000000000000000,NULL,`0.00000000000000000000000000,'-1.65243496861352645354632132',`0.00000000000000000000000000],26)))")->getBool());

    v1->nullFill(dolphindb::Util::createString("0"));
    ASSERT_EQ(v1->getString(), "[1.20000000000000000000000000,3.15000000000000000000000000,0.00000000000000000000000000,1.50000000000000000000000000,1.50000000000000000000000000,1.50000000000000000000000000,3.12000000000000000000000000,0.00000000000000000000000000,1000000000000.00000000000000000000000000,1.00000000000000000000000000,0.00000000000000000000000000,100000000.31624525345261230126453256,0.00000000000000000000000000,0.00000000000000000000000000,9.48566456834626453456249835,0.00000000000000000000000000,9999999999.12300000000000000000000000,0.00000000000000000000000000,0.00000000000000000000000000,-1.65243496861352645354632132,0.00000000000000000000000000]");

    dolphindb::VectorSP v2 = v1->getSubVector(0, 2, 10);
    ASSERT_TRUE(v2->set(dolphindb::Util::createInt(0), dolphindb::Util::createDecimal128(2, 2.7999)));
    v2->set(dolphindb::Util::createIndexVector(1, 1), errTypevalV);
    v2->set(dolphindb::Util::createIndexVector(1, 1), diffScalevalV);
    ASSERT_EQ(v2->getString(), "[2.79000000000000000000000000,1.00000000000000000000000000]");
    v2->set(indexV, valV);
    ASSERT_EQ(v2->getString(), "[100000000.31624525345261230126453256,]");

    ASSERT_TRUE(v2->get(1)->isNull());
    // ASSERT_TRUE(v2->get(dolphindb::Util::createInt(1))->is8Null());
    // ASSERT_TRUE(v2->get(dolphindb::Util::createIndexVector(1,1))->isNull());

    ASSERT_ANY_THROW(v2->fill(1, 5, valV));
    v2->fill(1, 1, dolphindb::Util::createDecimal128(5, 0));
    ASSERT_EQ(v2->getString(), "[100000000.31624525345261230126453256,0.00000000000000000000000000]");
    v2->fill(0, 2, dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL128));
    ASSERT_EQ(v2->getString(), "[,]");
    v2->fill(1, 1, errTypevalV);
    v2->fill(0, 2, diffScalevalV);
    ASSERT_EQ(v2->getString(), "[1.00000000000000000000000000,]");
    v2->fill(0, 2, valV);
    ASSERT_EQ(v2->getString(), "[100000000.31624525345261230126453256,]");

    ASSERT_FALSE(v2->validIndex(1));
    v2->fill(1, 1, dolphindb::Util::createDecimal128(5, 0));
    ASSERT_TRUE(v2->validIndex(-1));

    ASSERT_EQ(v2->compare(0, dolphindb::Util::createInt(99999999.0)), 1);
    ASSERT_EQ(v2->compare(1, dolphindb::Util::createInt(1)), -1);
    ASSERT_EQ(v2->compare(1, dolphindb::Util::createInt(0)), 0);
    ASSERT_EQ(v2->compare(0, dolphindb::Util::createDecimal32(1, 99999999.0)), 1);
    ASSERT_EQ(v2->compare(0, dolphindb::Util::createDecimal32(0, 100000001.0)), -1);
    ASSERT_EQ(v2->compare(1, dolphindb::Util::createDecimal32(1, 0)), 0);
    ASSERT_EQ(v2->compare(0, dolphindb::Util::createDecimal64(8, 99999999.0)), 1);
    ASSERT_EQ(v2->compare(0, dolphindb::Util::createDecimal64(10, 100000001.0)), -1);
    ASSERT_EQ(v2->compare(1, dolphindb::Util::createDecimal64(1, 0)), 0);
    ASSERT_EQ(v2->compare(0, dolphindb::Util::createDecimal128(26, 99999999.0)), 1);
    ASSERT_EQ(v2->compare(0, dolphindb::Util::createDecimal128(26, 100000001.0)), -1);
    ASSERT_EQ(v2->compare(1, dolphindb::Util::createDecimal128(1, 0)), 0);
    ASSERT_ANY_THROW(v2->compare(0, dolphindb::Util::createBlob("blob1")));
    v2->nullFill(dolphindb::Util::createInt(1));
    ASSERT_EQ(v2->getString(), "[100000000.31624525345261230126453256,0.00000000000000000000000000]");
}

TEST_F(DataformVectorTest, testDecimal128NullVector)
{
    for (int i = 0; i < 39; i++)
    {
        dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DECIMAL128, 2, 2, true, i);
        v1->set(0, dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL128));
        v1->set(1, dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL128));

        std::string script = "a=[decimal128(NULL," + std::to_string(i) + "),decimal128(NULL," + std::to_string(i) + ")];a";
        dolphindb::VectorSP res_v = conn.run(script);
        conn.upload("v1", {v1});
        ASSERT_EQ(conn.run("eqObj(v1,a)")->getBool(), true);
        ASSERT_EQ(v1->getString(), res_v->getString());
        ASSERT_EQ(v1->getType(), res_v->getRawType());
        ASSERT_EQ(dolphindb::Util::getCategoryString(v1->getCategory()), "DENARY");
        ASSERT_EQ(v1->getExtraParamForType(), i);
    }
}

TEST_F(DataformVectorTest, testDecimal32Vector_typeConvert){
    dolphindb::VectorSP dsm32V = dolphindb::Util::createVector(dolphindb::DT_DECIMAL32, 13, 20, true, 6);
    dolphindb::VectorSP v1 = conn.run("v1=array(DECIMAL32(6)).append!(decimal32(NULL -0.2345671 '999.1',6));v1");
    ASSERT_EQ(v1->getString(), "[,-0.234567,999.100000]");

    dsm32V->set(0, dolphindb::Util::createDecimal32(2, 0.234567));
    dolphindb::VectorSP indexV = dolphindb::Util::createIndexVector(1, 3);
    dsm32V->set(indexV, v1);
    dsm32V->setBool(4, true);
    dsm32V->setChar(5, '\2');
    dsm32V->setDouble(6, -0.23559913);
    dsm32V->setFloat(7, 1.1);
    dsm32V->setInt(8, 100);
    dsm32V->setLong(9, 6l);
    dsm32V->setShort(10, 200);
    dolphindb::ConstantSP dsm32strval = dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL32, 6);
    dsm32strval->setString("3.4562987561");
    dsm32V->set(11, dsm32strval);
    dsm32V->setNull(12);
    ASSERT_EQ(dsm32V->getString(), "[0.230000,,-0.234567,999.100000,1.000000,2.000000,-0.235599,1.100000,100.000000,6.000000,200.000000,3.456299,]");

    dolphindb::VectorSP v2 = conn.run("take(decimal32(NULL, 9), 3)");
    dsm32V->append(v2, 2);
    char *boolbuf = new char(true);
    char *charbuf = new char('\0');
    double *doublebuf = new double(-1.2345);
    float *floatbuf = new float(1.1);
    int *intbuf = new int(100);
    long long *longbuf = new long long(200);
    short *shortbuf = new short(300);
    std::string *stringbuf = new std::string("3.4562987561");
    dsm32V->appendBool(boolbuf, 1);
    dsm32V->appendChar(charbuf, 1);
    dsm32V->appendDouble(doublebuf, 1);
    dsm32V->appendFloat(floatbuf, 1);
    dsm32V->appendInt(intbuf, 1);
    dsm32V->appendLong(longbuf, 1);
    dsm32V->appendShort(shortbuf, 1);
    ASSERT_TRUE(dsm32V->appendString(stringbuf, 1));
    dsm32V->append(dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL32, 6));
    std::cout<< dsm32V->getString()<<std::endl;
    ASSERT_EQ(dsm32V->getString(), "[0.230000,,-0.234567,999.100000,1.000000,2.000000,-0.235599,1.100000,100.000000,6.000000,200.000000,3.456299,,"
                                    ",,1.000000,0.000000,-1.234500,1.100000,100.000000,200.000000,300.000000,3.456299,]");
    delete boolbuf, charbuf, doublebuf, floatbuf, intbuf, longbuf, shortbuf, stringbuf;
}

TEST_F(DataformVectorTest, testDecimal64Vector_typeConvert){
    dolphindb::VectorSP dsm64V = dolphindb::Util::createVector(dolphindb::DT_DECIMAL64, 13, 20, true, 16);
    dolphindb::VectorSP v1 = conn.run("v1=array(DECIMAL64(16)).append!(decimal64(NULL '-0.23456786523468213' '1.1',16));v1");
    ASSERT_EQ(v1->getString(), "[,-0.2345678652346821,1.1000000000000000]");

    dsm64V->set(0, dolphindb::Util::createDecimal64(6, 0.23456789));
    dolphindb::VectorSP indexV = dolphindb::Util::createIndexVector(1, 3);
    dsm64V->set(indexV, v1);
    dsm64V->setBool(4, true);
    dsm64V->setChar(5, '\2');
    dsm64V->setDouble(6, -0.23559913);
    dsm64V->setFloat(7, 1.1f);
    dsm64V->setInt(8, 100);
    dsm64V->setLong(9, 6l);
    dsm64V->setShort(10, 200);
    dolphindb::ConstantSP dsm64strval = dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL64, 16);
    dsm64strval->setString("3.4562987561654895642");
    dsm64V->set(11, dsm64strval);
    dsm64V->setNull(12);

    std::vector<double> vals = {0.23456789,DBL_MIN,-0.2345678652346821,1.1,1.0,2.0,-0.23559913,1.1,100.0,6.0,200.0,3.4562987561654895642,DBL_MIN};
    for (auto i=0;i<dsm64V->size();i++){
        std::string re = dsm64V->get(i)->getString();
        size_t re_scale = re.length() - re.find(".") - 1;

        if (vals[i] == DBL_MIN){
            ASSERT_EQ(re, "");
            ASSERT_EQ(re_scale, 0);
            continue;
        }
        ASSERT_EQ(re_scale, 16);
        double re_val = stod(re);
        ASSERT_NEAR(re_val, vals[i], 0.000001);
    }

    dolphindb::VectorSP v2 = conn.run("take(decimal64(NULL, 9), 3)");
    dsm64V->append(v2, 2);
    char *boolbuf = new char(true);
    char *charbuf = new char('\0');
    double *doublebuf = new double(-1.2345);
    float *floatbuf = new float(1.1);
    int *intbuf = new int(100);
    long long *longbuf = new long long(200);
    short *shortbuf = new short(300);
    std::string *stringbuf = new std::string("3.4562987561654895642");
    dsm64V->appendBool(boolbuf, 1);
    dsm64V->appendChar(charbuf, 1);
    dsm64V->appendDouble(doublebuf, 1);
    dsm64V->appendFloat(floatbuf, 1);
    dsm64V->appendInt(intbuf, 1);
    dsm64V->appendLong(longbuf, 1);
    dsm64V->appendShort(shortbuf, 1);
    ASSERT_TRUE(dsm64V->appendString(stringbuf, 1));
    dsm64V->append(dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL64, 17));
    std::cout<< dsm64V->getString()<<std::endl;

    vals = {0.23456789,DBL_MIN,-0.2345678652346821,1.1,1.0,2.0,-0.23559913,1.1,100.0,6.0,200.0,3.4562987561654895642,DBL_MIN,DBL_MIN,DBL_MIN,1.0,0.0,-1.2345,1.1,100.0,200.0,300.0,3.4562987561654896,DBL_MIN};
    for (auto i=0;i<dsm64V->size();i++){
        std::string re = dsm64V->get(i)->getString();
        size_t re_scale = re.length() - re.find(".") - 1;

        if (vals[i] == DBL_MIN){
            ASSERT_EQ(re, "");
            ASSERT_EQ(re_scale, 0);
            continue;
        }
        ASSERT_EQ(re_scale, 16);
        double re_val = stod(re);
        ASSERT_NEAR(re_val, vals[i], 0.000001);
    }
    delete boolbuf, charbuf, doublebuf, floatbuf, intbuf, longbuf, shortbuf, stringbuf;
}

TEST_F(DataformVectorTest, testDecimal128Vector_typeConvert){
    dolphindb::VectorSP dsm128V = dolphindb::Util::createVector(dolphindb::DT_DECIMAL128, 13, 20, true, 26);
    dolphindb::VectorSP v1 = conn.run("v1=array(DECIMAL128(26)).append!(decimal128(NULL '-0.23456779999999999854247936123456' '999.1',26));v1");
    ASSERT_EQ(v1->getString(), "[,-0.23456779999999999854247936,999.10000000000000000000000000]");

    dsm128V->set(0, dolphindb::Util::createDecimal128(20, 0.234567));
    dolphindb::VectorSP indexV = dolphindb::Util::createIndexVector(1, 3);
    dsm128V->set(indexV, v1);
    dsm128V->setBool(4, true);
    dsm128V->setChar(5, '\2');
    dsm128V->setDouble(6, -0.23559913);
    dsm128V->setFloat(7, 1.1f);
    dsm128V->setInt(8, 100);
    dsm128V->setLong(9, 6l);
    dsm128V->setShort(10, 200);
    dolphindb::ConstantSP dsm128strval = dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL128, 26);
    dsm128strval->setString("3.1643352698353646483264836492356");
    dsm128V->set(11, dsm128strval);
    dsm128V->setNull(12);

    std::vector<double> vals = {0.23456789,DBL_MIN,-0.2345678652346821,999.1,1.0,2.0,-0.23559913,1.1,100.0,6.0,200.0,3.1643352698353646483264836492356,DBL_MIN};
    for (auto i=0;i<dsm128V->size();i++){
        std::string re = dsm128V->get(i)->getString();
        size_t re_scale = re.length() - re.find(".") - 1;

        if (vals[i] == DBL_MIN){
            ASSERT_EQ(re, "");
            ASSERT_EQ(re_scale, 0);
            continue;
        }
        ASSERT_EQ(re_scale, 26);
        double re_val = stod(re);
        ASSERT_NEAR(re_val, vals[i], 0.000001);
    }

    dolphindb::VectorSP v2 = conn.run("take(decimal128(NULL, 0), 3)");
    dsm128V->append(v2, 2);
    char *boolbuf = new char(true);
    char *charbuf = new char('\0');
    double *doublebuf = new double(-1.2345);
    float *floatbuf = new float(1.1);
    int *intbuf = new int(100);
    long long *longbuf = new long long(200);
    short *shortbuf = new short(300);
    std::string *stringbuf = new std::string("3.1643352698353646483264836492356");
    dsm128V->appendBool(boolbuf, 1);
    dsm128V->appendChar(charbuf, 1);
    dsm128V->appendDouble(doublebuf, 1);
    dsm128V->appendFloat(floatbuf, 1);
    dsm128V->appendInt(intbuf, 1);
    dsm128V->appendLong(longbuf, 1);
    dsm128V->appendShort(shortbuf, 1);
    ASSERT_TRUE(dsm128V->appendString(stringbuf, 1));

    dsm128V->append(dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL128, 30));
    std::cout<< dsm128V->getString()<<std::endl;
    vals = {0.23456789,DBL_MIN,-0.2345678652346821,999.1,1.0,2.0,-0.23559913,1.1,100.0,6.0,200.0,3.1643352698353646483264836492356,DBL_MIN,DBL_MIN,DBL_MIN,1.0,0.0,-1.2345,1.1,100.0,200.0,300.0,3.1643352698353646483264836492356,DBL_MIN};
    for (auto i=0;i<dsm128V->size();i++){
        std::string re = dsm128V->get(i)->getString();
        size_t re_scale = re.length() - re.find(".") - 1;

        if (vals[i] == DBL_MIN){
            ASSERT_EQ(re, "");
            ASSERT_EQ(re_scale, 0);
            continue;
        }
        ASSERT_EQ(re_scale, 26);
        double re_val = stod(re);
        ASSERT_NEAR(re_val, vals[i], 0.000001);
    }
    delete boolbuf, charbuf, doublebuf, floatbuf, intbuf, longbuf, shortbuf, stringbuf;
}

TEST_F(DataformVectorTest, testCreateStringVectorByDdbVector)
{
    std::vector<std::string> vals = {"str1", "str2", "str3", "str4", "str5", "str6"};
    int size = vals.size();
    std::string *val = new std::string[size * 2 + 2];
    for (int i = 0; i < size; i++)
    {
        val[i] = vals[i];
    }
    dolphindb::DdbVector<std::string> v1(val, size, size * 2 + 2);
    dolphindb::DdbVector<std::string> v2(0, size + 10);
    for (auto i = 0; i < size; i++)
    {
        v2.add(vals[i]);
    }
    v1.addNull();
    v2.addNull();
    v1.setNull(0);
    v2.setNull(0);

    const std::string str1 = "str1";
    v1.add(str1);
    v2.add(str1);

    dolphindb::VectorSP ddbv1 = v1.createVector(dolphindb::DT_STRING);
    dolphindb::VectorSP ddbv2 = v2.createVector(dolphindb::DT_STRING);
    std::vector<std::string> names = {"ddbv1", "ddbv2"};
    std::vector<dolphindb::ConstantSP> objs = {ddbv1, ddbv2};
    conn.upload(names, objs);
    conn.run("a=[NULL,`str2,`str3,`str4,`str5,`str6,NULL,`str1];");
    ASSERT_TRUE(conn.run("eqObj(ddbv1,a)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(ddbv2,a)")->getBool());

    ASSERT_EQ(v1.size(), 8);
    ASSERT_EQ(v2.size(), 8);

    std::string *val_new = new std::string[size * 2 + 2];
    for (int i = 0; i < size; i++)
    {
        val_new[i] = vals[i];
    }
    dolphindb::DdbVector<std::string> v3(val_new, vals.size(), size * 2 + 2);
    dolphindb::DdbVector<std::string> v4(0, size * 2 + 2);
    std::string *appendbuf1 = new std::string[size];
    std::string *appendbuf2 = new std::string[size];
    std::string setbuf1[1];
    std::string setbuf2[1];
    setbuf1[0] = "set";
    setbuf2[0] = "set";
    const std::string appendconstbuf1[1] = {"append1str"};
    const std::string appendconstbuf2[1] = {"append1str"};
    const std::string setconstbuf1[1] = {"setstr"};
    const std::string setconstbuf2[1] = {"setstr"};
    for (int i = 0; i < size; i++)
    {
        appendbuf1[i] = vals[i];
    }

    // v3.append(buf,size); //append() is not supported for std::string dolphindb::DdbVector.
    // v3.set(buf,size); //set() is not supported for std::string dolphindb::DdbVector.
    v3.appendString(appendbuf1, size);
    v3.appendString(appendconstbuf1, 1);
    v3.setString(0, 1, setbuf1);
    v3.setString(1, 1, setconstbuf1);
    for (int i = 0; i < size; i++)
    {
        appendbuf2[i] = vals[i];
    }
    // v4.append(buf,size); //append() is not supported for std::string dolphindb::DdbVector.
    // v4.set(buf,size); //set() is not supported for std::string dolphindb::DdbVector.
    v4.appendString(appendbuf2, size);
    v4.appendString(appendconstbuf2, 1);
    v4.setString(0, 1, setbuf2);
    v4.setString(1, 1, setconstbuf2);

    dolphindb::VectorSP ddbv3 = v3.createVector(dolphindb::DT_STRING);
    dolphindb::VectorSP ddbv4 = v4.createVector(dolphindb::DT_STRING);
    // std::cout<<ddbv3->getString()<<std::endl;
    // std::cout<<ddbv4->getString()<<std::endl;
    conn.upload("ddbv3", ddbv3);
    conn.upload("ddbv4", ddbv4);
    conn.run("a=[`set,`setstr,`str3,`str4,`str5,`str6,`append1str];b=[`set,`setstr,`str3,`str4,`str5,`str6,`str1,`str2,`str3,`str4,`str5,`str6,`append1str]");
    ASSERT_TRUE(conn.run("eqObj(ddbv3,b)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(ddbv4,a)")->getBool());

    for (int i = 0; i < v3.size(); i++)
    {
        ASSERT_EQ(v3.data()[i], ddbv3->get(i)->getString());
    }
    for (int i = 0; i < v4.size(); i++)
    {
        ASSERT_EQ(v4.data()[i], ddbv4->get(i)->getString());
    }

    delete[] appendbuf1, appendbuf2;
}

TEST_F(DataformVectorTest, testCreateCharVectorByDdbVector)
{
    std::vector<char> vals = {1, 2, 3, 4, 5, 6};
    int size = vals.size();
    char *val = new char[size * 2 + 2];
    for (int i = 0; i < size; i++)
    {
        val[i] = vals[i];
    }
    dolphindb::DdbVector<char> v1(val, size, size * 2 + 2);
    dolphindb::DdbVector<char> v2(0, size + 10);
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

    dolphindb::VectorSP ddbv1 = v1.createVector(dolphindb::DT_CHAR);
    dolphindb::VectorSP ddbv2 = v2.createVector(dolphindb::DT_CHAR);
    std::vector<std::string> names = {"ddbv1", "ddbv2"};
    std::vector<dolphindb::ConstantSP> objs = {ddbv1, ddbv2};
    conn.upload(names, objs);
    conn.run("a=[NULL,2,3,4,5,6,NULL,7];");
    ASSERT_TRUE(conn.run("eqObj(ddbv1,a)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(ddbv2,a)")->getBool());

    ASSERT_EQ(v1.size(), 8);
    ASSERT_EQ(v2.size(), 8);

    char *val_new = new char[size * 2 + 2];
    for (int i = 0; i < size; i++)
    {
        val_new[i] = vals[i];
    }
    dolphindb::DdbVector<char> v3(val_new, size, size * 2 + 2);
    dolphindb::DdbVector<char> v4(0, size * 2 + 2);
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

    dolphindb::VectorSP ddbv3 = v3.createVector(dolphindb::DT_CHAR);
    dolphindb::VectorSP ddbv4 = v4.createVector(dolphindb::DT_CHAR);
    // std::cout<<ddbv3->getString()<<std::endl;
    // std::cout<<ddbv4->getString()<<std::endl;
    conn.upload("ddbv3", ddbv3);
    conn.upload("ddbv4", ddbv4);
    conn.run("a=char[8,0,9,4,5,6,9]");
    ASSERT_TRUE(conn.run("eqObj(ddbv3,a)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(ddbv4,a)")->getBool());
}

TEST_F(DataformVectorTest, testCreateIntVectorByDdbVector)
{
    std::vector<int> vals = {1, 2, 3, 4, 5, 6};
    int size = vals.size();
    int *val = new int[size * 2 + 2];
    for (int i = 0; i < size; i++)
    {
        val[i] = vals[i];
    }
    dolphindb::DdbVector<int> v1(val, size, size * 2 + 2);
    dolphindb::DdbVector<int> v2(0, size + 10);
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

    dolphindb::VectorSP ddbv1 = v1.createVector(dolphindb::DT_INT);
    dolphindb::VectorSP ddbv2 = v2.createVector(dolphindb::DT_INT);
    std::vector<std::string> names = {"ddbv1", "ddbv2"};
    std::vector<dolphindb::ConstantSP> objs = {ddbv1, ddbv2};
    conn.upload(names, objs);
    conn.run("a=[NULL,2,3,4,5,6,NULL,7];");
    ASSERT_TRUE(conn.run("eqObj(ddbv1,a)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(ddbv2,a)")->getBool());

    ASSERT_EQ(v1.size(), 8);
    ASSERT_EQ(v2.size(), 8);

    int *val_new = new int[size * 2 + 2];
    for (int i = 0; i < size; i++)
    {
        val_new[i] = vals[i];
    }
    dolphindb::DdbVector<int> v3(val_new, size, size * 2 + 2);
    dolphindb::DdbVector<int> v4(0, size * 2 + 2);
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

    dolphindb::VectorSP ddbv3 = v3.createVector(dolphindb::DT_INT);
    dolphindb::VectorSP ddbv4 = v4.createVector(dolphindb::DT_INT);
    // std::cout<<ddbv3->getString()<<std::endl;
    // std::cout<<ddbv4->getString()<<std::endl;
    conn.upload("ddbv3", ddbv3);
    conn.upload("ddbv4", ddbv4);
    conn.run("a=int[8,0,9,4,5,6,9]");
    ASSERT_TRUE(conn.run("eqObj(ddbv3,a)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(ddbv4,a)")->getBool());
}

TEST_F(DataformVectorTest, testCreateShortVectorByDdbVector)
{
    std::vector<short> vals = {1, 2, 3, 4, 5, 6};
    int size = vals.size();
    short *val = new short[size * 2 + 2];
    for (int i = 0; i < size; i++)
    {
        val[i] = vals[i];
    }
    dolphindb::DdbVector<short> v1(val, size, size * 2 + 2);
    dolphindb::DdbVector<short> v2(0, size + 10);
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

    dolphindb::VectorSP ddbv1 = v1.createVector(dolphindb::DT_SHORT);
    dolphindb::VectorSP ddbv2 = v2.createVector(dolphindb::DT_SHORT);
    std::vector<std::string> names = {"ddbv1", "ddbv2"};
    std::vector<dolphindb::ConstantSP> objs = {ddbv1, ddbv2};
    conn.upload(names, objs);
    conn.run("a=[NULL,2,3,4,5,6,NULL,7];");
    ASSERT_TRUE(conn.run("eqObj(ddbv1,a)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(ddbv2,a)")->getBool());

    ASSERT_EQ(v1.size(), 8);
    ASSERT_EQ(v2.size(), 8);

    short *val_new = new short[size * 2 + 2];
    for (int i = 0; i < size; i++)
    {
        val_new[i] = vals[i];
    }
    dolphindb::DdbVector<short> v3(val_new, size, size * 2 + 2);
    dolphindb::DdbVector<short> v4(0, size * 2 + 2);
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

    dolphindb::VectorSP ddbv3 = v3.createVector(dolphindb::DT_SHORT);
    dolphindb::VectorSP ddbv4 = v4.createVector(dolphindb::DT_SHORT);
    // std::cout<<ddbv3->getString()<<std::endl;
    // std::cout<<ddbv4->getString()<<std::endl;
    conn.upload("ddbv3", ddbv3);
    conn.upload("ddbv4", ddbv4);
    conn.run("a=short[8,0,9,4,5,6,9]");
    ASSERT_TRUE(conn.run("eqObj(ddbv3,a)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(ddbv4,a)")->getBool());
}

TEST_F(DataformVectorTest, testCreateLongVectorByDdbVector)
{
    std::vector<long long> vals = {1, 2, 3, 4, 5, 6};
    int size = vals.size();
    long long *val = new long long[size * 2 + 2];
    for (int i = 0; i < size; i++)
    {
        val[i] = vals[i];
    }
    dolphindb::DdbVector<long long> v1(val, size, size * 2 + 2);
    dolphindb::DdbVector<long long> v2(0, size + 10);
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

    dolphindb::VectorSP ddbv1 = v1.createVector(dolphindb::DT_LONG);
    dolphindb::VectorSP ddbv2 = v2.createVector(dolphindb::DT_LONG);
    std::vector<std::string> names = {"ddbv1", "ddbv2"};
    std::vector<dolphindb::ConstantSP> objs = {ddbv1, ddbv2};
    conn.upload(names, objs);
    conn.run("a=[NULL,2,3,4,5,6,NULL,7];");
    ASSERT_TRUE(conn.run("eqObj(ddbv1,a)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(ddbv2,a)")->getBool());

    ASSERT_EQ(v1.size(), 8);
    ASSERT_EQ(v2.size(), 8);

    long long *val_new = new long long[size * 2 + 2];
    for (int i = 0; i < size; i++)
    {
        val_new[i] = vals[i];
    }
    dolphindb::DdbVector<long long> v3(val_new, size, size * 2 + 2);
    dolphindb::DdbVector<long long> v4(0, size * 2 + 2);
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

    dolphindb::VectorSP ddbv3 = v3.createVector(dolphindb::DT_LONG);
    dolphindb::VectorSP ddbv4 = v4.createVector(dolphindb::DT_LONG);
    // std::cout<<ddbv3->getString()<<std::endl;
    // std::cout<<ddbv4->getString()<<std::endl;
    conn.upload("ddbv3", ddbv3);
    conn.upload("ddbv4", ddbv4);
    conn.run("a=long[8,0,9,4,5,6,9]");
    ASSERT_TRUE(conn.run("eqObj(ddbv3,a)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(ddbv4,a)")->getBool());
}

TEST_F(DataformVectorTest, testCreateFloatVectorByDdbVector)
{
    std::vector<float> vals = {1, 2, 3, 4, 5, 6};
    int size = vals.size();
    float *val = new float[size * 2 + 2];
    for (int i = 0; i < size; i++)
    {
        val[i] = vals[i];
    }
    dolphindb::DdbVector<float> v1(val, size, size * 2 + 2);
    dolphindb::DdbVector<float> v2(0, size + 10);
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

    dolphindb::VectorSP ddbv1 = v1.createVector(dolphindb::DT_FLOAT);
    dolphindb::VectorSP ddbv2 = v2.createVector(dolphindb::DT_FLOAT);
    std::vector<std::string> names = {"ddbv1", "ddbv2"};
    std::vector<dolphindb::ConstantSP> objs = {ddbv1, ddbv2};
    conn.upload(names, objs);
    conn.run("a=float[NULL,2,3,4,5,6,NULL,7];");
    ASSERT_TRUE(conn.run("eqObj(ddbv1,a)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(ddbv2,a)")->getBool());

    ASSERT_EQ(v1.size(), 8);
    ASSERT_EQ(v2.size(), 8);

    float *val_new = new float[size * 2 + 2];
    for (int i = 0; i < size; i++)
    {
        val_new[i] = vals[i];
    }
    dolphindb::DdbVector<float> v3(val_new, size, size * 2 + 2);
    dolphindb::DdbVector<float> v4(0, size * 2 + 2);
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

    dolphindb::VectorSP ddbv3 = v3.createVector(dolphindb::DT_FLOAT);
    dolphindb::VectorSP ddbv4 = v4.createVector(dolphindb::DT_FLOAT);
    // std::cout<<ddbv3->getString()<<std::endl;
    // std::cout<<ddbv4->getString()<<std::endl;
    conn.upload("ddbv3", ddbv3);
    conn.upload("ddbv4", ddbv4);
    conn.run("a=float[8,0,9,4,5,6,9]");
    ASSERT_TRUE(conn.run("eqObj(ddbv3,a)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(ddbv4,a)")->getBool());
}

TEST_F(DataformVectorTest, testCreateDoubleVectorByDdbVector)
{
    std::vector<double> vals = {1, 2, 3, 4, 5, 6};
    int size = vals.size();
    double *val = new double[size * 2 + 2];
    for (int i = 0; i < size; i++)
    {
        val[i] = vals[i];
    }
    dolphindb::DdbVector<double> v1(val, size, size * 2 + 2);
    dolphindb::DdbVector<double> v2(0, size + 10);
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

    dolphindb::VectorSP ddbv1 = v1.createVector(dolphindb::DT_DOUBLE);
    dolphindb::VectorSP ddbv2 = v2.createVector(dolphindb::DT_DOUBLE);
    std::vector<std::string> names = {"ddbv1", "ddbv2"};
    std::vector<dolphindb::ConstantSP> objs = {ddbv1, ddbv2};
    conn.upload(names, objs);
    conn.run("a=double[NULL,2,3,4,5,6,NULL,7];");
    ASSERT_TRUE(conn.run("eqObj(ddbv1,a)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(ddbv2,a)")->getBool());

    ASSERT_EQ(v1.size(), 8);
    ASSERT_EQ(v2.size(), 8);

    double *val_new = new double[size * 2 + 2];
    for (int i = 0; i < size; i++)
    {
        val_new[i] = vals[i];
    }
    dolphindb::DdbVector<double> v3(val_new, size, size * 2 + 2);
    dolphindb::DdbVector<double> v4(0, size * 2 + 2);
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

    dolphindb::VectorSP ddbv3 = v3.createVector(dolphindb::DT_DOUBLE);
    dolphindb::VectorSP ddbv4 = v4.createVector(dolphindb::DT_DOUBLE);
    // std::cout<<ddbv3->getString()<<std::endl;
    // std::cout<<ddbv4->getString()<<std::endl;
    conn.upload("ddbv3", ddbv3);
    conn.upload("ddbv4", ddbv4);
    conn.run("a=double[8,0,9,4,5,6,9]");
    ASSERT_TRUE(conn.run("eqObj(ddbv3,a)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(ddbv4,a)")->getBool());
}

TEST_F(DataformVectorTest, testCreateInt128VectorByDdbVector)
{
    std::vector<dolphindb::Guid> vals;
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
    dolphindb::Guid *val = new dolphindb::Guid[size * 2 + 2];
    for (int i = 0; i < size; i++)
    {
        val[i] = vals[i];
    }
    dolphindb::DdbVector<dolphindb::Guid> v1(val, size, size * 2 + 2);
    dolphindb::DdbVector<dolphindb::Guid> v2(0, size + 10);
    for (auto i = 0; i < size; i++)
    {
        v2.add(vals[i]);
    }
    v1.addNull();
    v2.addNull();
    v1.setNull(0);
    v2.setNull(0);

    dolphindb::VectorSP ddbv1 = v1.createVector(dolphindb::DT_INT128);
    dolphindb::VectorSP ddbv2 = v2.createVector(dolphindb::DT_INT128);

    std::vector<std::string> names = {"ddbv1", "ddbv2"};
    std::vector<dolphindb::ConstantSP> objs = {ddbv1, ddbv2};
    conn.upload(names, objs);
    conn.run("a=int128[NULL,`64646464646464646464646464646464,`64646464646464646464646464646464,\
                        `64646464646464646464646464646464,`64646464646464646464646464646464,\
                        `64646464646464646464646464646464,NULL];");
    ASSERT_TRUE(conn.run("eqObj(ddbv1,a)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(ddbv2,a)")->getBool());

    ASSERT_EQ(v1.size(), 7);
    ASSERT_EQ(v2.size(), 7);

    dolphindb::Guid *val_new = new dolphindb::Guid[size * 2 + 2];
    for (int i = 0; i < size; i++)
    {
        val_new[i] = vals[i];
    }
    dolphindb::DdbVector<dolphindb::Guid> v3(val_new, size, size * 2 + 2);
    dolphindb::DdbVector<dolphindb::Guid> v4(0, size * 2 + 2);
    for (int i = 0; i < size; i++)
    {
        v4.add(vals[i]);
    }
    dolphindb::Guid setval1 = vals[4];
    dolphindb::Guid setval2 = vals[4];
    const dolphindb::Guid appendconstbuf1[1] = {vals[3]};
    const dolphindb::Guid appendconstbuf2[1] = {vals[3]};
    const dolphindb::Guid setconstval1 = vals[2];
    const dolphindb::Guid setconstval2 = vals[2];
    const dolphindb::Guid setconstval3[1] = {vals[1]};
    const dolphindb::Guid setconstval4[1] = {vals[1]};

    v3.append(appendconstbuf1, 1);
    v3.set(0, setval1);
    v3.set(1, setconstval1);
    v3.set(2, 1, setconstval3);

    v4.append(appendconstbuf2, 1);
    v4.set(0, setval2);
    v4.set(1, setconstval2);
    v4.set(2, 1, setconstval4);

    dolphindb::VectorSP ddbv3 = v3.createVector(dolphindb::DT_INT128);
    dolphindb::VectorSP ddbv4 = v4.createVector(dolphindb::DT_INT128);
    conn.upload("ddbv3", ddbv3);
    conn.upload("ddbv4", ddbv4);
    conn.run("a=int128[`64646464646464646464646464646464,`64646464646464646464646464646464,`64646464646464646464646464646464,\
    `64646464646464646464646464646464,`64646464646464646464646464646464,`64646464646464646464646464646464,`64646464646464646464646464646464]");
    ASSERT_TRUE(conn.run("eqObj(ddbv3,a)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(ddbv4,a)")->getBool());
}

TEST_F(DataformVectorTest, testCreateUuidVectorByDdbVector)
{
    std::vector<dolphindb::Guid> vals;
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
    dolphindb::Guid *val = new dolphindb::Guid[size * 2 + 2];
    for (int i = 0; i < size; i++)
    {
        val[i] = vals[i];
    }
    dolphindb::DdbVector<dolphindb::Guid> v1(val, size, size * 2 + 2);
    dolphindb::DdbVector<dolphindb::Guid> v2(0, size + 10);
    for (auto i = 0; i < size; i++)
    {
        v2.add(vals[i]);
    }
    v1.addNull();
    v2.addNull();
    v1.setNull(0);
    v2.setNull(0);

    dolphindb::VectorSP ddbv1 = v1.createVector(dolphindb::DT_UUID);
    dolphindb::VectorSP ddbv2 = v2.createVector(dolphindb::DT_UUID);
    // std::cout<<ddbv1->getString()<<std::endl;
    // std::cout<<ddbv2->getString()<<std::endl;

    std::vector<std::string> names = {"ddbv1", "ddbv2"};
    std::vector<dolphindb::ConstantSP> objs = {ddbv1, ddbv2};
    conn.upload(names, objs);
    conn.run("a=uuid[NULL,\"64646464-6464-6464-6464-646464646464\",\"64646464-6464-6464-6464-646464646464\",\
    \"64646464-6464-6464-6464-646464646464\",\"64646464-6464-6464-6464-646464646464\",\
    \"64646464-6464-6464-6464-646464646464\",NULL]");
    ASSERT_TRUE(conn.run("eqObj(ddbv1,a)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(ddbv2,a)")->getBool());

    ASSERT_EQ(v1.size(), 7);
    ASSERT_EQ(v2.size(), 7);

    dolphindb::Guid *val_new = new dolphindb::Guid[size * 2 + 2];
    for (int i = 0; i < size; i++)
    {
        val_new[i] = vals[i];
    }
    dolphindb::DdbVector<dolphindb::Guid> v3(val_new, size, size * 2 + 2);
    dolphindb::DdbVector<dolphindb::Guid> v4(0, size * 2 + 2);
    for (int i = 0; i < size; i++)
    {
        v4.add(vals[i]);
    }
    dolphindb::Guid setval1 = vals[4];
    dolphindb::Guid setval2 = vals[4];
    const dolphindb::Guid appendconstbuf1[1] = {vals[3]};
    const dolphindb::Guid appendconstbuf2[1] = {vals[3]};
    const dolphindb::Guid setconstval1 = vals[2];
    const dolphindb::Guid setconstval2 = vals[2];
    const dolphindb::Guid setconstval3[1] = {vals[1]};
    const dolphindb::Guid setconstval4[1] = {vals[1]};

    v3.append(appendconstbuf1, 1);
    v3.set(0, setval1);
    v3.set(1, setconstval1);
    v3.set(2, 1, setconstval3);

    v4.append(appendconstbuf2, 1);
    v4.set(0, setval2);
    v4.set(1, setconstval2);
    v4.set(2, 1, setconstval4);

    dolphindb::VectorSP ddbv3 = v3.createVector(dolphindb::DT_UUID);
    dolphindb::VectorSP ddbv4 = v4.createVector(dolphindb::DT_UUID);
    // std::cout<<ddbv3->getString()<<std::endl;
    // std::cout<<ddbv4->getString()<<std::endl;
    conn.upload("ddbv3", ddbv3);
    conn.upload("ddbv4", ddbv4);
    conn.run("a=uuid[\"64646464-6464-6464-6464-646464646464\",\"64646464-6464-6464-6464-646464646464\",\
    \"64646464-6464-6464-6464-646464646464\",\"64646464-6464-6464-6464-646464646464\",\"64646464-6464-6464-6464-646464646464\",\
    \"64646464-6464-6464-6464-646464646464\",\"64646464-6464-6464-6464-646464646464\"]");
    ASSERT_TRUE(conn.run("eqObj(ddbv3,a)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(ddbv4,a)")->getBool());
}

TEST_F(DataformVectorTest, testCreateIpaddrVectorByDdbVector)
{
    std::vector<dolphindb::Guid> vals;
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
    dolphindb::Guid *val = new dolphindb::Guid[size * 2 + 2];
    for (int i = 0; i < size; i++)
    {
        val[i] = vals[i];
    }
    dolphindb::DdbVector<dolphindb::Guid> v1(val, size, size * 2 + 2);
    dolphindb::DdbVector<dolphindb::Guid> v2(0, size + 10);
    for (auto i = 0; i < size; i++)
    {
        v2.add(vals[i]);
    }
    v1.addNull();
    v2.addNull();
    v1.setNull(0);
    v2.setNull(0);

    dolphindb::VectorSP ddbv1 = v1.createVector(dolphindb::DT_IP);
    dolphindb::VectorSP ddbv2 = v2.createVector(dolphindb::DT_IP);
    // std::cout<<ddbv1->getString()<<std::endl;
    // std::cout<<ddbv2->getString()<<std::endl;

    std::vector<std::string> names = {"ddbv1", "ddbv2"};
    std::vector<dolphindb::ConstantSP> objs = {ddbv1, ddbv2};
    conn.upload(names, objs);
    conn.run("a=ipaddr[NULL,\"6464:6464:6464:6464:6464:6464:6464:6464\",\"6464:6464:6464:6464:6464:6464:6464:6464\",\"6464:6464:6464:6464:6464:6464:6464:6464\",\
    \"6464:6464:6464:6464:6464:6464:6464:6464\",\"6464:6464:6464:6464:6464:6464:6464:6464\",NULL]");
    ASSERT_TRUE(conn.run("eqObj(ddbv1,a)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(ddbv2,a)")->getBool());

    ASSERT_EQ(v1.size(), 7);
    ASSERT_EQ(v2.size(), 7);

    dolphindb::Guid *val_new = new dolphindb::Guid[size * 2 + 2];
    for (int i = 0; i < size; i++)
    {
        val_new[i] = vals[i];
    }
    dolphindb::DdbVector<dolphindb::Guid> v3(val_new, size, size * 2 + 2);
    dolphindb::DdbVector<dolphindb::Guid> v4(0, size * 2 + 2);
    for (int i = 0; i < size; i++)
    {
        v4.add(vals[i]);
    }
    dolphindb::Guid setval1 = vals[4];
    dolphindb::Guid setval2 = vals[4];
    const dolphindb::Guid appendconstbuf1[1] = {vals[3]};
    const dolphindb::Guid appendconstbuf2[1] = {vals[3]};
    const dolphindb::Guid setconstval1 = vals[2];
    const dolphindb::Guid setconstval2 = vals[2];
    const dolphindb::Guid setconstval3[1] = {vals[1]};
    const dolphindb::Guid setconstval4[1] = {vals[1]};

    v3.append(appendconstbuf1, 1);
    v3.set(0, setval1);
    v3.set(1, setconstval1);
    v3.set(2, 1, setconstval3);

    v4.append(appendconstbuf2, 1);
    v4.set(0, setval2);
    v4.set(1, setconstval2);
    v4.set(2, 1, setconstval4);

    dolphindb::VectorSP ddbv3 = v3.createVector(dolphindb::DT_IP);
    dolphindb::VectorSP ddbv4 = v4.createVector(dolphindb::DT_IP);
    // std::cout<<ddbv3->getString()<<std::endl;
    // std::cout<<ddbv4->getString()<<std::endl;
    conn.upload("ddbv3", ddbv3);
    conn.upload("ddbv4", ddbv4);
    conn.run("a=ipaddr[\"6464:6464:6464:6464:6464:6464:6464:6464\",\"6464:6464:6464:6464:6464:6464:6464:6464\",\"6464:6464:6464:6464:6464:6464:6464:6464\",\
    \"6464:6464:6464:6464:6464:6464:6464:6464\",\"6464:6464:6464:6464:6464:6464:6464:6464\",\"6464:6464:6464:6464:6464:6464:6464:6464\",\"6464:6464:6464:6464:6464:6464:6464:6464\"]");
    ASSERT_TRUE(conn.run("eqObj(ddbv3,a)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(ddbv4,a)")->getBool());
}

TEST_F(DataformVectorTest, testStringVectorMoreThan65535)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_STRING, 70000, 70000);
    for (int i = 0; i < 70000; i++)
    {
        v1->set(i, dolphindb::Util::createString(std::to_string(i)));
    }
    std::string script = "z=array(STRING,0);for (i in 0..69999){z.append!(string(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testAnyVectorMoreThan65535)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_ANY, 70000, 70000);
    for (int i = 0; i < 70000; i++)
    {
        v1->set(i, dolphindb::Util::createString(std::to_string(i)));
    }
    std::string script = "z=array(ANY,0);for (i in 0..69999){z.append!(string(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testCharVectorEqule128)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_CHAR, 128, 128);
    for (int i = 0; i < 128; i++)
    {
        v1->set(i, dolphindb::Util::createChar(i));
    }
    std::string script = "z=array(CHAR,0);for (i in 0..127){z.append!(char(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testIntVectorMoreThan65535)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_INT, 70000, 70000);
    for (int i = 0; i < 70000; i++)
    {
        v1->set(i, dolphindb::Util::createInt(i));
    }
    std::string script = "z=array(INT,0);for (i in 0..69999){z.append!(int(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testLongVectorMoreThan65535)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_LONG, 70000, 70000);
    for (int i = 0; i < 70000; i++)
    {
        v1->set(i, dolphindb::Util::createLong(i));
    }
    std::string script = "z=array(LONG,0);for (i in 0..69999){z.append!(long(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testShortVectorEqual256)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_SHORT, 256, 256);
    for (int i = 0; i < 256; i++)
    {
        v1->set(i, dolphindb::Util::createShort(i));
    }
    std::string script = "z=array(SHORT,0);for (i in 0..255){z.append!(short(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testFloatVectorMoreThan65535)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_FLOAT, 70000, 70000);
    for (int i = 0; i < 70000; i++)
    {
        v1->set(i, dolphindb::Util::createFloat(i));
    }
    std::string script = "z=array(FLOAT,0);for (i in 0..69999){z.append!(float(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testDoubleVectorMoreThan65535)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DOUBLE, 70000, 70000);
    for (int i = 0; i < 70000; i++)
    {
        v1->set(i, dolphindb::Util::createDouble(i));
    }
    std::string script = "z=array(DOUBLE,0);for (i in 0..69999){z.append!(double(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testDatehourVectorMoreThan65535)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DATEHOUR, 70000, 70000);
    for (int i = 0; i < 70000; i++)
    {
        v1->set(i, dolphindb::Util::createDateHour(i));
    }
    std::string script = "z=array(DATEHOUR,0);for (i in 0..69999){z.append!(datehour(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testDateVectorMoreThan65535)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DATE, 70000, 70000);
    for (int i = 0; i < 70000; i++)
    {
        v1->set(i, dolphindb::Util::createDate(i));
    }
    std::string script = "z=array(DATE,0);for (i in 0..69999){z.append!(date(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testMinuteVectorMoreThan1024)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_MINUTE, 1440, 1440);
    for (int i = 0; i < 1440; i++)
    {
        v1->set(i, dolphindb::Util::createMinute(i));
    }
    std::string script = "z=array(MINUTE,0);for (i in 0..1439){z.append!(minute(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testDatetimeVectorMoreThan65535)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DATETIME, 70000, 70000);
    for (int i = 0; i < 70000; i++)
    {
        v1->set(i, dolphindb::Util::createDateTime(i));
    }
    std::string script = "z=array(DATETIME,0);for (i in 0..69999){z.append!(datetime(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testTimeStampVectorMoreThan65535)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_TIMESTAMP, 70000, 70000);
    for (int i = 0; i < 70000; i++)
    {
        v1->set(i, dolphindb::Util::createTimestamp(i));
    }
    std::string script = "z=array(TIMESTAMP,0);for (i in 0..69999){z.append!(timestamp(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testnanotimeVectorMoreThan65535)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_NANOTIME, 70000, 70000);
    for (int i = 0; i < 70000; i++)
    {
        v1->set(i, dolphindb::Util::createNanoTime(i));
    }
    std::string script = "z=array(NANOTIME,0);for (i in 0..69999){z.append!(nanotime(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testnanotimestampVectorMoreThan65535)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_NANOTIMESTAMP, 70000, 70000);
    for (int i = 0; i < 70000; i++)
    {
        v1->set(i, dolphindb::Util::createNanoTimestamp(i));
    }
    std::string script = "z=array(NANOTIMESTAMP,0);for (i in 0..69999){z.append!(nanotimestamp(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testmonthVectorMoreThan65535)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_MONTH, 70000, 70000);
    for (int i = 0; i < 70000; i++)
    {
        v1->set(i, dolphindb::Util::createMonth(i));
    }
    std::string script = "z=array(MONTH,0);for (i in 0..69999){z.append!(month(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testtimeVectorMoreThan65535)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_TIME, 70000, 70000);
    for (int i = 0; i < 70000; i++)
    {
        v1->set(i, dolphindb::Util::createTime(i));
    }
    std::string script = "z=array(TIME,0);for (i in 0..69999){z.append!(time(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testStringVectorMoreThan1048576)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_STRING, 1100000, 1100000);
    for (int i = 0; i < 1100000; i++)
    {
        v1->set(i, dolphindb::Util::createString(std::to_string(i)));
    }
    std::string script = "z=array(STRING,0);for (i in 0..1099999){z.append!(string(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testAnyVectorMoreThan1048576)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_ANY, 1100000, 1100000);
    for (int i = 0; i < 1100000; i++)
    {
        v1->set(i, dolphindb::Util::createString(std::to_string(i)));
    }
    std::string script = "z=array(ANY,0);for (i in 0..1099999){z.append!(string(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(res_d->getType(), dolphindb::DT_ANY);
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testIntVectorMoreThan1048576)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_INT, 1100000, 1100000);
    for (int i = 0; i < 1100000; i++)
    {
        v1->set(i, dolphindb::Util::createInt(i));
    }
    std::string script = "z=array(INT,0);for (i in 0..1099999){z.append!(int(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testLongVectorMoreThan1048576)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_LONG, 1100000, 1100000);
    for (int i = 0; i < 1100000; i++)
    {
        v1->set(i, dolphindb::Util::createLong(i));
    }
    std::string script = "z=array(LONG,0);for (i in 0..1099999){z.append!(long(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testFloatVectorMoreThan1048576)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_FLOAT, 1100000, 1100000);
    for (int i = 0; i < 1100000; i++)
    {
        v1->set(i, dolphindb::Util::createFloat(i));
    }
    std::string script = "z=array(FLOAT,0);for (i in 0..1099999){z.append!(float(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testDoubleVectorMoreThan1048576)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DOUBLE, 1100000, 1100000);
    for (int i = 0; i < 1100000; i++)
    {
        v1->set(i, dolphindb::Util::createDouble(i));
    }
    std::string script = "z=array(DOUBLE,0);for (i in 0..1099999){z.append!(double(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testDatehourVectorMoreThan1048576)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DATEHOUR, 1100000, 1100000);
    for (int i = 0; i < 1100000; i++)
    {
        v1->set(i, dolphindb::Util::createDateHour(i));
    }
    std::string script = "z=array(DATEHOUR,0);for (i in 0..1099999){z.append!(datehour(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testDateVectorMoreThan1048576)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DATE, 1100000, 1100000);
    for (int i = 0; i < 1100000; i++)
    {
        v1->set(i, dolphindb::Util::createDate(i));
    }
    std::string script = "z=array(DATE,0);for (i in 0..1099999){z.append!(date(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testDatetimeVectorMoreThan1048576)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_DATETIME, 1100000, 1100000);
    for (int i = 0; i < 1100000; i++)
    {
        v1->set(i, dolphindb::Util::createDateTime(i));
    }
    std::string script = "z=array(DATETIME,0);for (i in 0..1099999){z.append!(datetime(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testTimeStampVectorMoreThan1048576)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_TIMESTAMP, 1100000, 1100000);
    for (int i = 0; i < 1100000; i++)
    {
        v1->set(i, dolphindb::Util::createTimestamp(i));
    }
    std::string script = "z=array(TIMESTAMP,0);for (i in 0..1099999){z.append!(timestamp(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testnanotimeVectorMoreThan1048576)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_NANOTIME, 1100000, 1100000);
    for (int i = 0; i < 1100000; i++)
    {
        v1->set(i, dolphindb::Util::createNanoTime(i));
    }
    std::string script = "z=array(NANOTIME,0);for (i in 0..1099999){z.append!(nanotime(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testnanotimestampVectorMoreThan1048576)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_NANOTIMESTAMP, 1100000, 1100000);
    for (int i = 0; i < 1100000; i++)
    {
        v1->set(i, dolphindb::Util::createNanoTimestamp(i));
    }
    std::string script = "z=array(NANOTIMESTAMP,0);for (i in 0..1099999){z.append!(nanotimestamp(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testmonthVectorMoreThan1048576)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_MONTH, 1100000, 1100000);
    for (int i = 0; i < 1100000; i++)
    {
        v1->set(i, dolphindb::Util::createMonth(i));
    }
    std::string script = "z=array(MONTH,0);for (i in 0..1099999){z.append!(month(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testtimeVectorMoreThan1048576)
{
    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_TIME, 1100000, 1100000);
    for (int i = 0; i < 1100000; i++)
    {
        v1->set(i, dolphindb::Util::createTime(i));
    }
    std::string script = "z=array(TIME,0);for (i in 0..1099999){z.append!(time(i))};z";
    dolphindb::VectorSP res_d = conn.run(script);
    conn.upload("v1", {v1});
    ASSERT_EQ(conn.run("v1.size()")->getInt(), res_d->size());
    std::string judgestr = "eqObj(z,v1)";
    ASSERT_EQ(conn.run(judgestr)->getBool(), true);
}

TEST_F(DataformVectorTest, testenumVector)
{
    dolphindb::TableSP table;
    { // create a table with 70000 rows.
        int size = 70000;
        dolphindb::DdbVector<double> doubleV(0, size);
        dolphindb::DdbVector<float> floatV(0, size);
        dolphindb::DdbVector<int> intV(0, size);
        dolphindb::DdbVector<short> shortV(0, size);
        dolphindb::DdbVector<long long> longV(0, size);
        dolphindb::DdbVector<char> boolV(0, size);
        dolphindb::DdbVector<char> charV(0, size);
        dolphindb::DdbVector<std::string> strV(0, size);
        dolphindb::DdbVector<dolphindb::Guid> int128V(0, size);
        dolphindb::DdbVector<dolphindb::Guid> uuidV(0, size);
        dolphindb::DdbVector<dolphindb::Guid> ipV(0, size);
        dolphindb::DdbVector<std::string> blobV(0, size);
        dolphindb::DdbVector<int> dateV(0, size);
        dolphindb::DdbVector<int> minuteV(0, size);
        dolphindb::DdbVector<int> datetimeV(0, size);
        dolphindb::DdbVector<long long> nanotimeV(0, size);
        dolphindb::DdbVector<int> datehourV(0, size);
        dolphindb::DdbVector<int32_t> decimal32V(0, size);
        dolphindb::DdbVector<int64_t> decimal64V(0, size);
        dolphindb::DdbVector<wide_integer::int128> decimal128V(0, size);
        dolphindb::DdbVector<std::string> symV(0, size);
        for (auto i = 0; i < size - 1; i++)
        {
            doubleV.add(i);
            floatV.add(i);
            intV.add(i);
            shortV.add(i);
            longV.add(i);
            boolV.add(i % 1);
            charV.add(i % CHAR_MAX);
            strV.add("str" + std::to_string(i));
            int128V.add(dolphindb::Guid(true));
            uuidV.add(dolphindb::Guid(true));
            ipV.add(dolphindb::Guid(true));
            blobV.add("blob" + std::to_string(i));
            dateV.add(i);
            minuteV.add(i);
            datetimeV.add(i);
            nanotimeV.add((long long)i);
            datehourV.add(i);
            decimal32V.add(i / 100.00);
            decimal64V.add(i / 100.0000);
            decimal128V.add(i);
            symV.add("sym" + std::to_string(i));
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
        decimal128V.addNull();
        symV.addNull();
        table = dolphindb::Util::createTable(
            {"double", "float", "int", "short", "long", "bool", "char", "str", "int128", "uuid", "ip", "blob", "date", "minute", "datetime", "nanotime", "datehour", "decimal32", "decimal64", "decimal128", "symbol"},
            {doubleV.createVector(dolphindb::DT_DOUBLE), floatV.createVector(dolphindb::DT_FLOAT), intV.createVector(dolphindb::DT_INT), shortV.createVector(dolphindb::DT_SHORT),
             longV.createVector(dolphindb::DT_LONG), boolV.createVector(dolphindb::DT_BOOL), charV.createVector(dolphindb::DT_CHAR), strV.createVector(dolphindb::DT_STRING),
             int128V.createVector(dolphindb::DT_INT128), uuidV.createVector(dolphindb::DT_UUID), ipV.createVector(dolphindb::DT_IP), blobV.createVector(dolphindb::DT_BLOB),
             dateV.createVector(dolphindb::DT_DATE), minuteV.createVector(dolphindb::DT_MINUTE), datetimeV.createVector(dolphindb::DT_DATETIME), nanotimeV.createVector(dolphindb::DT_NANOTIME),
             datehourV.createVector(dolphindb::DT_DATEHOUR), decimal32V.createVector(dolphindb::DT_DECIMAL32), decimal64V.createVector(dolphindb::DT_DECIMAL64), decimal128V.createVector(dolphindb::DT_DECIMAL128), symV.createVector(dolphindb::DT_SYMBOL)});
        ASSERT_ANY_THROW(datehourV.createVector(dolphindb::DT_DATEHOUR)); // createVector can only be called once.
        conn.upload("test_tab", {table});
    }
    dolphindb::TableSP res_tab = conn.run("test_tab");
    { // create threads for all columns and save datas in std::vector
        std::vector<dolphindb::ThreadSP> threads(table->columns());
        dolphindb::VectorSP *cols = new dolphindb::VectorSP[table->columns()];
        int colIndex = 0;
        cols[colIndex++] = dolphindb::Util::createVector(dolphindb::DT_DOUBLE, 0, table->rows());
        cols[colIndex++] = dolphindb::Util::createVector(dolphindb::DT_FLOAT, 0, table->rows());
        cols[colIndex++] = dolphindb::Util::createVector(dolphindb::DT_INT, 0, table->rows());
        cols[colIndex++] = dolphindb::Util::createVector(dolphindb::DT_SHORT, 0, table->rows());
        cols[colIndex++] = dolphindb::Util::createVector(dolphindb::DT_LONG, 0, table->rows());
        cols[colIndex++] = dolphindb::Util::createVector(dolphindb::DT_BOOL, 0, table->rows());
        cols[colIndex++] = dolphindb::Util::createVector(dolphindb::DT_CHAR, 0, table->rows());
        cols[colIndex++] = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, table->rows());
        cols[colIndex++] = dolphindb::Util::createVector(dolphindb::DT_INT128, 0, table->rows());
        cols[colIndex++] = dolphindb::Util::createVector(dolphindb::DT_UUID, 0, table->rows());
        cols[colIndex++] = dolphindb::Util::createVector(dolphindb::DT_IP, 0, table->rows());
        cols[colIndex++] = dolphindb::Util::createVector(dolphindb::DT_BLOB, 0, table->rows());
        cols[colIndex++] = dolphindb::Util::createVector(dolphindb::DT_DATE, 0, table->rows());
        cols[colIndex++] = dolphindb::Util::createVector(dolphindb::DT_MINUTE, 0, table->rows());
        cols[colIndex++] = dolphindb::Util::createVector(dolphindb::DT_DATETIME, 0, table->rows());
        cols[colIndex++] = dolphindb::Util::createVector(dolphindb::DT_NANOTIME, 0, table->rows());
        cols[colIndex++] = dolphindb::Util::createVector(dolphindb::DT_DATEHOUR, 0, table->rows());
        cols[colIndex++] = dolphindb::Util::createVector(dolphindb::DT_DECIMAL32, 0, table->rows());
        cols[colIndex++] = dolphindb::Util::createVector(dolphindb::DT_DECIMAL64, 0, table->rows());
        cols[colIndex++] = dolphindb::Util::createVector(dolphindb::DT_DECIMAL128, 0, table->rows());
        cols[colIndex++] = dolphindb::Util::createVector(dolphindb::DT_SYMBOL, 0, table->rows());
        for (auto colIndex = 0; colIndex < table->columns(); colIndex++)
        {
            dolphindb::VectorSP pvector = table->getColumn(colIndex);
            threads[colIndex] = new dolphindb::Thread(
                new dolphindb::Executor([=]
                             {
                switch (pvector->getType()) {
                    case dolphindb::DT_DOUBLE:
                        dolphindb::Util::enumDoubleVector(pvector, [&](const double *pbuf, dolphindb::INDEX startIndex, int length) {
                            for (auto index = startIndex, i = 0; i < length; i++, index++) {
                                cols[colIndex]->append(dolphindb::Util::createDouble(pbuf[i]));
                            }
                            return true;
                        });
                    break;
                    case dolphindb::DT_FLOAT:
                        dolphindb::Util::enumFloatVector(pvector, [&](const float *pbuf, dolphindb::INDEX startIndex, int length) {
                            for (auto index = startIndex, i = 0; i < length; i++, index++) {
                                cols[colIndex]->append(dolphindb::Util::createFloat(pbuf[i]));
                            }
                            return true;
                        });
                    break;
                    case dolphindb::DT_INT:
                        dolphindb::Util::enumIntVector(pvector, [&](const int *pbuf, dolphindb::INDEX startIndex, int length) {
                            for (auto index = startIndex, i = 0; i < length; i++, index++) {
                                cols[colIndex]->append(dolphindb::Util::createInt(pbuf[i]));
                            }
                            return true;
                        });
                    break;
                    case dolphindb::DT_SHORT:
                        dolphindb::Util::enumShortVector(pvector, [&](const short *pbuf, dolphindb::INDEX startIndex, int length) {
                            for (auto index = startIndex, i = 0; i < length; i++, index++) {
                                cols[colIndex]->append(dolphindb::Util::createShort(pbuf[i]));
                            }
                            return true;
                        });
                    break;
                    case dolphindb::DT_LONG:
                        dolphindb::Util::enumLongVector(pvector, [&](const long long *pbuf, dolphindb::INDEX startIndex, int length) {
                            for (auto index = startIndex, i = 0; i < length; i++, index++) {
                                cols[colIndex]->append(dolphindb::Util::createLong(pbuf[i]));
                            }
                            return true;
                        });
                    break;
                    case dolphindb::DT_BOOL:
                        dolphindb::Util::enumBoolVector(pvector, [&](const char *pbuf, dolphindb::INDEX startIndex, int length) {
                            for (auto index = startIndex, i = 0; i < length; i++, index++) {
                                cols[colIndex]->append(dolphindb::Util::createBool(pbuf[i]));
                            }
                            return true;
                        });
                    break;
                    case dolphindb::DT_CHAR:
                        dolphindb::Util::enumCharVector(pvector, [&](const char *pbuf, dolphindb::INDEX startIndex, int length) {
                            for (auto index = startIndex, i = 0; i < length; i++, index++) {
                                cols[colIndex]->append(dolphindb::Util::createChar(pbuf[i]));
                            }
                            return true;
                        });
                    break;
                    case dolphindb::DT_STRING:
                        dolphindb::Util::enumStringVector(pvector, [&](std::string **pbuf, dolphindb::INDEX startIndex, int length) {
                            for (auto index = startIndex, i = 0; i < length; i++, index++) {
                                cols[colIndex]->append(dolphindb::Util::createString(*pbuf[i]));
                            }
                            return true;
                        });
                    break;
                    case dolphindb::DT_INT128:
                    {
                        dolphindb::Util::enumInt128Vector(pvector, [&](const dolphindb::Guid *pbuf, dolphindb::INDEX startIndex, int length) {
                            for (auto index = startIndex, i = 0; i < length; i++, index++) {
                                dolphindb::ConstantSP pconst = dolphindb::Util::createObject(dolphindb::DT_INT128, (const unsigned char*)(pbuf + i));
                                cols[colIndex]->append(pconst);
                            }
                            return true;
                        });
                    break;
                    }
                    case dolphindb::DT_UUID:
                    {
                        dolphindb::Util::enumInt128Vector(pvector, [&](const dolphindb::Guid *pbuf, dolphindb::INDEX startIndex, int length) {
                            for (auto index = startIndex, i = 0; i < length; i++, index++) {
                                dolphindb::ConstantSP pconst = dolphindb::Util::createObject(dolphindb::DT_UUID, (const unsigned char*)(pbuf + i));
                                cols[colIndex]->append(pconst);
                            }
                            return true;
                        });
                    break;
                    }
                    case dolphindb::DT_IP:
                    {
                        dolphindb::Util::enumInt128Vector(pvector, [&](const dolphindb::Guid *pbuf, dolphindb::INDEX startIndex, int length) {
                            for (auto index = startIndex, i = 0; i < length; i++, index++) {
                                dolphindb::ConstantSP pconst = dolphindb::Util::createObject(dolphindb::DT_IP, (const unsigned char*)(pbuf + i));
                                cols[colIndex]->append(pconst);
                            }
                            return true;
                        });
                    break;
                    }
                    case dolphindb::DT_BLOB:
                        dolphindb::Util::enumStringVector(pvector, [&](std::string **pbuf, dolphindb::INDEX startIndex, int length) {
                            for (auto index = startIndex, i = 0; i < length; i++, index++) {
                                cols[colIndex]->append(dolphindb::Util::createBlob(*pbuf[i]));
                            }
                            return true;
                        });
                    break;
                    case dolphindb::DT_DATE:
                        dolphindb::Util::enumIntVector(pvector, [&](const int *pbuf, dolphindb::INDEX startIndex, int length) {
                            for (auto index = startIndex, i = 0; i < length; i++, index++) {
                                cols[colIndex]->append(dolphindb::Util::createDate(pbuf[i]));
                            }
                            return true;
                        });
                    break;
                    case dolphindb::DT_MINUTE:
                        dolphindb::Util::enumIntVector(pvector, [&](const int *pbuf, dolphindb::INDEX startIndex, int length) {
                            for (auto index = startIndex, i = 0; i < length; i++, index++) {
                                cols[colIndex]->append(dolphindb::Util::createMinute(pbuf[i]));
                            }
                            return true;
                        });
                    break;
                    case dolphindb::DT_DATETIME:
                        dolphindb::Util::enumIntVector(pvector, [&](const int *pbuf, dolphindb::INDEX startIndex, int length) {
                            for (auto index = startIndex, i = 0; i < length; i++, index++) {
                                cols[colIndex]->append(dolphindb::Util::createDateTime(pbuf[i]));
                            }
                            return true;
                        });
                    break;
                    case dolphindb::DT_NANOTIME:
                        dolphindb::Util::enumLongVector(pvector, [&](const long long *pbuf, dolphindb::INDEX startIndex, int length) {
                            for (auto index = startIndex, i = 0; i < length; i++, index++) {
                                cols[colIndex]->append(dolphindb::Util::createNanoTime(pbuf[i]));
                            }
                            return true;
                        });
                    break;
                    case dolphindb::DT_DATEHOUR:
                        dolphindb::Util::enumIntVector(pvector, [&](const int *pbuf, dolphindb::INDEX startIndex, int length) {
                            for (auto index = startIndex, i = 0; i < length; i++, index++) {
                                cols[colIndex]->append(dolphindb::Util::createDateHour(pbuf[i]));
                            }
                            return true;
                        });
                    break;
                    case dolphindb::DT_DECIMAL32:
                        dolphindb::Util::enumDecimal32Vector(pvector, [&](const int32_t *pbuf, dolphindb::INDEX startIndex, int length) {
                            for (auto index = startIndex, i = 0; i < length; i++, index++) {
                                dolphindb::ConstantSP pconst = dolphindb::Util::createDecimal32(pvector->getExtraParamForType(),0);
                                pconst->setBinary((const unsigned char*)&pbuf[i], sizeof(int32_t));
                                cols[colIndex]->append(pconst);
                            }
                            return true;
                        });
                    break;
                    case dolphindb::DT_DECIMAL64:
                        dolphindb::Util::enumDecimal64Vector(pvector, [&](const int64_t *pbuf, dolphindb::INDEX startIndex, int length) {
                            for (auto index = startIndex, i = 0; i < length; i++, index++) {
                                dolphindb::ConstantSP pconst = dolphindb::Util::createDecimal64(pvector->getExtraParamForType(),0);
                                pconst->setBinary((const unsigned char*)&pbuf[i], sizeof(int64_t));
                                cols[colIndex]->append(pconst);
                            }
                            return true;
                        });
                    break;
                    case dolphindb::DT_DECIMAL128:
                        dolphindb::Util::enumDecimal128Vector(pvector, [&](const wide_integer::int128 *pbuf, dolphindb::INDEX startIndex, int length) {
                            for (auto index = startIndex, i = 0; i < length; i++, index++) {
                                dolphindb::ConstantSP pconst = dolphindb::Util::createDecimal128(pvector->getExtraParamForType(),0);
                                pconst->setBinary((const unsigned char*)&pbuf[i], sizeof(wide_integer::int128));
                                cols[colIndex]->append(pconst);
                            }
                            return true;
                        });
                    break;
                    case dolphindb::DT_SYMBOL:
                        dolphindb::Util::enumStringVector(pvector, [&](std::string **pbuf, dolphindb::INDEX startIndex, int length) {
                            for (auto index = startIndex, i = 0; i < length; i++, index++) {
                                cols[colIndex]->append(dolphindb::Util::createString(*pbuf[i]));
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
            ASSERT_EQ(cols[colIndex++]->get(i)->getString(), res_tab->getColumn(19)->get(i)->getString());
        }
        delete[] cols;
    }
}

TEST_F(DataformVectorTest, testVectorFunction)
{

    srand(time(NULL));
    int buckets = 2;
    char *buf0 = new char[dolphindb::Util::BUF_SIZE];
    char *buf00 = new char[dolphindb::Util::BUF_SIZE];
    char *buf = new char[dolphindb::Util::BUF_SIZE];
    short *buf1 = new short[dolphindb::Util::BUF_SIZE];
    int *buf2 = new int[dolphindb::Util::BUF_SIZE];
    dolphindb::INDEX *buf4 = new dolphindb::INDEX[dolphindb::Util::BUF_SIZE];
    long long *buf3 = new long long[dolphindb::Util::BUF_SIZE];
    float *buf5 = new float[dolphindb::Util::BUF_SIZE];
    double *buf6 = new double[dolphindb::Util::BUF_SIZE];
    std::string **buf8 = new std::string *[dolphindb::Util::BUF_SIZE];
    char **buf9 = new char *[dolphindb::Util::BUF_SIZE];
    unsigned char *buf10 = new unsigned char[dolphindb::Util::BUF_SIZE];
    dolphindb::SymbolBase *symbase = new dolphindb::SymbolBase(1);
    std::string exstr = "";
    int size = 3;

    std::cout << "-----------------std::string-------------------" << std::endl;
    dolphindb::ConstantSP stringval = dolphindb::Util::createString("this is a std::string scalar");
    dolphindb::VectorSP stringV = dolphindb::Util::createVector(dolphindb::DT_STRING, size);
    stringV->initialize();
    for (auto i = 0; i < size; i++)
        stringV->set(i, stringval);
    ASSERT_TRUE(stringV->isNull(0, size, buf0));
    ASSERT_FALSE(stringV->isValid(0, size, buf0));
    ASSERT_FALSE(stringV->getBool(0, size, buf00));
    ASSERT_FALSE(stringV->getChar(0, size, buf));
    ASSERT_FALSE(stringV->getShort(0, size, buf1));
    ASSERT_FALSE(stringV->getInt(0, size, buf2));
    ASSERT_FALSE(stringV->getLong(0, size, buf3));
    ASSERT_FALSE(stringV->getIndex(0, size, buf4));
    ASSERT_FALSE(stringV->getFloat(0, size, buf5));
    ASSERT_FALSE(stringV->getDouble(0, size, buf6));
    ASSERT_FALSE(stringV->getSymbol(0, size, buf2, symbase, false));
    ASSERT_FALSE(stringV->getBinary(0, size, size, buf10));
    ASSERT_TRUE(stringV->getHash(0, size, buckets, buf2));
    ASSERT_ANY_THROW(stringV->getBoolConst(0, size, buf)[0]);
    ASSERT_ANY_THROW(stringV->getCharConst(0, size, buf)[0]);
    ASSERT_ANY_THROW(stringV->getShortConst(0, size, buf1)[0]);
    ASSERT_ANY_THROW(stringV->getIntConst(0, size, buf2)[0]);
    ASSERT_ANY_THROW(stringV->getLongConst(0, size, buf3)[0]);
    ASSERT_ANY_THROW(stringV->getIndexConst(0, size, buf4)[0]);
    ASSERT_ANY_THROW(stringV->getFloatConst(0, size, buf5)[0]);
    ASSERT_ANY_THROW(stringV->getDoubleConst(0, size, buf6)[0]);
    ASSERT_ANY_THROW(stringV->getSymbolConst(0, size, buf2, symbase, false));
    stringV->getStringConst(0, size, buf8);
    for (auto i = 0; i < size; i++)
        ASSERT_EQ(*(buf8[i]), stringval->getString());

    stringV->getStringConst(0, size, buf9);
    for (auto i = 0; i < size; i++)
        ASSERT_EQ(buf9[i], stringval->getString());

    ASSERT_ANY_THROW(stringV->getBinaryConst(0, size, sizeof(std::string), buf10));

    std::cout << "-----------------symbol-------------------" << std::endl;
    dolphindb::ConstantSP symval = dolphindb::Util::createString("symbol scalar");
    dolphindb::VectorSP symV = dolphindb::Util::createVector(dolphindb::DT_SYMBOL, size);
    symV->set(0, symval);
    symV->set(1, dolphindb::Util::createString("123"));
    ASSERT_TRUE(symV->isNull(0, size, buf0));
    ASSERT_TRUE(symV->isValid(0, size, buf0));
    ASSERT_TRUE(symV->getBool(0, size, buf00));
    symV->getChar(0, size, buf);
    symV->getShort(0, size, buf1);
    symV->getInt(0, size, buf2);
    symV->getLong(0, size, buf3);
    symV->getIndex(0, size, buf4);
    symV->getFloat(0, size, buf5);
    symV->getDouble(0, size, buf6);
    ASSERT_EQ(buf[0], 1);
    ASSERT_EQ(buf[1], 2);
    ASSERT_EQ(buf1[0], 1);
    ASSERT_EQ(buf1[1], 2);
    ASSERT_EQ(buf2[0], 1);
    ASSERT_EQ(buf2[1], 2);
    ASSERT_EQ(buf3[0], 1);
    ASSERT_EQ(buf3[1], 2);
    ASSERT_EQ(buf4[0], 1);
    ASSERT_EQ(buf4[1], 2);
    ASSERT_EQ(buf5[0], 1);
    ASSERT_EQ(buf5[1], 2);
    ASSERT_EQ(buf6[0], 1);
    ASSERT_EQ(buf6[1], 2);

    ASSERT_FALSE(symV->getSymbol(0, size, buf2, symbase, false));
    ASSERT_FALSE(symV->getBinary(0, size, size, buf10));
    ASSERT_TRUE(symV->getHash(0, size, buckets, buf2));
    ASSERT_EQ(symV->getAllocatedMemory(size), 0);
    ASSERT_EQ(symV->getBoolConst(0, size, buf)[0], 1);
    ASSERT_EQ(symV->getCharConst(0, size, buf)[0], 1);
    ASSERT_EQ(symV->getShortConst(0, size, buf1)[0], 1);
    ASSERT_EQ(symV->getIntConst(0, size, buf2)[0], 1);
    ASSERT_EQ(symV->getLongConst(0, size, buf3)[0], 1);
    ASSERT_EQ(symV->getIndexConst(0, size, buf4)[0], 1);
    ASSERT_EQ(symV->getFloatConst(0, size, buf5)[0], 1);
    ASSERT_EQ(symV->getDoubleConst(0, size, buf6)[0], 1);
    ASSERT_EQ(symV->getCharConst(0, size, buf)[1], 2);
    ASSERT_EQ(symV->getShortConst(0, size, buf1)[1], 2);
    ASSERT_EQ(symV->getIntConst(0, size, buf2)[1], 2);
    ASSERT_EQ(symV->getLongConst(0, size, buf3)[1], 2);
    ASSERT_EQ(symV->getIndexConst(0, size, buf4)[1], 2);
    ASSERT_EQ(symV->getFloatConst(0, size, buf5)[1], 2);
    ASSERT_EQ(symV->getDoubleConst(0, size, buf6)[1], 2);
    ASSERT_ANY_THROW(symV->getSymbolConst(0, size, buf2, symbase, false));
    symV->getStringConst(0, size, buf8);
    for (auto i = 0; i < size; i++)
        ASSERT_EQ(*(buf8[i]), symV->get(i)->getString());

    symV->getStringConst(0, size, buf9);
    for (auto i = 0; i < size; i++)
        ASSERT_EQ(buf9[i], symV->get(i)->getString());

    ASSERT_ANY_THROW(symV->getBinaryConst(0, size, sizeof(std::string), buf10));

    std::cout << "-----------------int-------------------" << std::endl;
    dolphindb::ConstantSP intval = dolphindb::Util::createInt(100000);
    dolphindb::VectorSP intV = dolphindb::Util::createVector(dolphindb::DT_INT, size);
    for (auto i = 0; i < size; i++)
        intV->set(i, intval);
    ASSERT_TRUE(intV->isNull(0, size, buf0));
    for (auto i = 0; i < size; i++)
        ASSERT_FALSE(buf0[i]);

    ASSERT_TRUE(intV->isValid(0, size, buf0));
    for (auto i = 0; i < size; i++)
        ASSERT_TRUE(buf0[i]);

    ASSERT_TRUE(intV->getBool(0, size, buf00));
    ASSERT_TRUE(intV->getChar(0, size, buf));
    ASSERT_TRUE(intV->getShort(0, size, buf1));
    ASSERT_TRUE(intV->getInt(0, size, buf2));
    ASSERT_TRUE(intV->getLong(0, size, buf3));
    ASSERT_TRUE(intV->getIndex(0, size, buf4));
    ASSERT_TRUE(intV->getFloat(0, size, buf5));
    ASSERT_TRUE(intV->getDouble(0, size, buf6));
    for (auto i = 0; i < size; i++)
    {
        ASSERT_EQ(buf00[i], true);
        ASSERT_EQ(buf[i], '\xA0');
        ASSERT_EQ(buf1[i], intV->get(i)->getShort());
        ASSERT_EQ(buf2[i], intV->get(i)->getInt());
        ASSERT_EQ(buf3[i], intV->get(i)->getLong());
        ASSERT_EQ(buf4[i], intV->get(i)->getIndex());
        ASSERT_EQ(buf5[i], intV->get(i)->getFloat());
        ASSERT_EQ(buf6[i], intV->get(i)->getDouble());
    }

    ASSERT_FALSE(intV->getSymbol(0, size, buf2, symbase, false));
    ASSERT_FALSE(intV->getBinary(0, size, sizeof(int), buf10));
    ASSERT_TRUE(intV->getHash(0, size, buckets, buf2));
    ASSERT_EQ(intV->getAllocatedMemory(size), 0);

    for (auto i = 0; i < size; i++)
    {
        ASSERT_EQ(intV->getBoolConst(0, size, buf)[i], 1);
        ASSERT_EQ(intV->getCharConst(0, size, buf)[i], '\xA0');
        // ASSERT_EQ(intV->getShortConst(0,size,buf1)[i], SHRT_MIN);
        ASSERT_EQ(intV->getIntConst(0, size, buf2)[i], 100000);
        ASSERT_EQ(intV->getLongConst(0, size, buf3)[i], 100000);
        ASSERT_EQ(intV->getIndexConst(0, size, buf4)[i], 100000);
        ASSERT_EQ(intV->getFloatConst(0, size, buf5)[i], 100000);
        ASSERT_EQ(intV->getDoubleConst(0, size, buf6)[i], 100000);
    }
    ASSERT_ANY_THROW(intV->getSymbolConst(0, size, buf2, symbase, false));
    ASSERT_ANY_THROW(intV->getStringConst(0, size, buf8));
    ASSERT_ANY_THROW(intV->getStringConst(0, 24, buf9));
    ASSERT_ANY_THROW(intV->getBinaryConst(0, size, sizeof(int), buf10));

    for (auto i = 0; i < size; i++)
    {
        ASSERT_EQ(intV->getBoolBuffer(0, size, buf)[i], '\xA0');
        ASSERT_EQ(intV->getCharBuffer(0, size, buf)[i], '\xA0');
        // ASSERT_EQ(intV->getShortBuffer(0,size,buf1)[0], SHRT_MIN);
        ASSERT_EQ(intV->getIntBuffer(0, size, buf2)[i], 100000);
        ASSERT_EQ(intV->getLongBuffer(0, size, buf3)[i], 100000);
        ASSERT_EQ(intV->getIndexBuffer(0, size, buf4)[i], 100000);
        ASSERT_EQ(intV->getFloatBuffer(0, size, buf5)[i], 100000);
        ASSERT_EQ(intV->getDoubleBuffer(0, size, buf6)[i], 100000);
    }

    std::cout << "-----------------char-------------------" << std::endl;
    char rand_val = rand() % CHAR_MAX + 1;
    dolphindb::ConstantSP charval = dolphindb::Util::createChar(rand_val);
    dolphindb::VectorSP charV = dolphindb::Util::createVector(dolphindb::DT_CHAR, size);
    for (auto i = 0; i < size; i++)
        charV->setChar(i, rand_val);

    ASSERT_TRUE(charV->isNull(0, size, buf0));
    for (auto i = 0; i < size; i++)
        ASSERT_FALSE(buf0[i]);

    ASSERT_TRUE(charV->isValid(0, size, buf0));
    for (auto i = 0; i < size; i++)
        ASSERT_TRUE(buf0[i]);

    ASSERT_TRUE(charV->getBool(0, size, buf00));
    ASSERT_TRUE(charV->getChar(0, size, buf));
    ASSERT_TRUE(charV->getShort(0, size, buf1));
    ASSERT_TRUE(charV->getInt(0, size, buf2));
    ASSERT_TRUE(charV->getLong(0, size, buf3));
    ASSERT_TRUE(charV->getIndex(0, size, buf4));
    ASSERT_TRUE(charV->getFloat(0, size, buf5));
    ASSERT_TRUE(charV->getDouble(0, size, buf6));
    for (auto i = 0; i < size; i++)
    {
        ASSERT_EQ(buf00[i], true);
        ASSERT_EQ(buf[i], rand_val);
        ASSERT_EQ(buf1[i], charV->get(i)->getShort());
        ASSERT_EQ(buf2[i], charV->get(i)->getInt());
        ASSERT_EQ(buf3[i], charV->get(i)->getLong());
        ASSERT_EQ(buf4[i], charV->get(i)->getIndex());
        ASSERT_EQ(buf5[i], charV->get(i)->getFloat());
        ASSERT_EQ(buf6[i], charV->get(i)->getDouble());
    }

    ASSERT_FALSE(charV->getSymbol(0, size, buf2, symbase, false));
    ASSERT_FALSE(charV->getBinary(0, size, size, buf10));
    ASSERT_TRUE(charV->getHash(0, size, buckets, buf2));
    ASSERT_EQ(charV->getAllocatedMemory(size), 0);

    for (auto i = 0; i < size; i++)
    {
        ASSERT_EQ(charV->getBoolConst(0, size, buf)[i], 1);
        ASSERT_EQ(charV->getCharConst(0, size, buf)[i], rand_val);
        ASSERT_EQ(charV->getShortConst(0, size, buf1)[i], (short)rand_val);
        ASSERT_EQ(charV->getIntConst(0, size, buf2)[i], (int)rand_val);
        ASSERT_EQ(charV->getLongConst(0, size, buf3)[i], (long long)rand_val);
        ASSERT_EQ(charV->getIndexConst(0, size, buf4)[i], (dolphindb::INDEX)rand_val);
        ASSERT_EQ(charV->getFloatConst(0, size, buf5)[i], (float)rand_val);
        ASSERT_EQ(charV->getDoubleConst(0, size, buf6)[i], (double)rand_val);
    }

    ASSERT_ANY_THROW(charV->getSymbolConst(0, size, buf2, symbase, false));
    ASSERT_ANY_THROW(charV->getStringConst(0, size, buf8)[0]);
    ASSERT_ANY_THROW(charV->getStringConst(0, 24, buf9));
    ASSERT_ANY_THROW(charV->getBinaryConst(0, size, sizeof(char), buf10));

    for (auto i = 0; i < size; i++)
    {
        ASSERT_EQ(charV->getBoolBuffer(0, size, buf)[i], (bool)rand_val);
        ASSERT_EQ(charV->getCharBuffer(0, size, buf)[i], rand_val);
        ASSERT_EQ(charV->getShortBuffer(0, size, buf1)[i], (short)rand_val);
        ASSERT_EQ(charV->getIntBuffer(0, size, buf2)[i], (int)rand_val);
        ASSERT_EQ(charV->getLongBuffer(0, size, buf3)[i], (long)rand_val);
        ASSERT_EQ(charV->getIndexBuffer(0, size, buf4)[i], (dolphindb::INDEX)rand_val);
        ASSERT_EQ(charV->getFloatBuffer(0, size, buf5)[i], (float)rand_val);
        ASSERT_EQ(charV->getDoubleBuffer(0, size, buf6)[i], (double)rand_val);
    }

    dolphindb::ConstantSP indexV = dolphindb::Util::createIndexVector(0, 5);
    ASSERT_TRUE(charV->remove(indexV));
    ASSERT_EQ(charV->size(), 0);

    std::cout << "-----------------short-------------------" << std::endl;
    short rand_val2 = rand() % SHRT_MAX + 1;
    dolphindb::ConstantSP shortval = dolphindb::Util::createShort(rand_val2);
    dolphindb::VectorSP shortV = dolphindb::Util::createVector(dolphindb::DT_SHORT, size);
    for (auto i = 0; i < size; i++)
        shortV->setShort(i, rand_val2);

    ASSERT_TRUE(shortV->isNull(0, size, buf0));
    for (auto i = 0; i < size; i++)
        ASSERT_FALSE(buf0[i]);

    ASSERT_TRUE(shortV->isValid(0, size, buf0));
    for (auto i = 0; i < size; i++)
        ASSERT_TRUE(buf0[i]);

    // ASSERT_TRUE(shortV->getBool(0,size,buf00));
    // ASSERT_TRUE(shortV->getChar(0,size,buf));
    ASSERT_TRUE(shortV->getShort(0, size, buf1));
    ASSERT_TRUE(shortV->getInt(0, size, buf2));
    ASSERT_TRUE(shortV->getLong(0, size, buf3));
    ASSERT_TRUE(shortV->getIndex(0, size, buf4));
    ASSERT_TRUE(shortV->getFloat(0, size, buf5));
    ASSERT_TRUE(shortV->getDouble(0, size, buf6));
    for (auto i = 0; i < size; i++)
    {
        // ASSERT_EQ(buf00[i], true);
        // ASSERT_EQ(buf[i], rand_val2);
        ASSERT_EQ(buf1[i], shortV->get(i)->getShort());
        ASSERT_EQ(buf2[i], shortV->get(i)->getInt());
        ASSERT_EQ(buf3[i], shortV->get(i)->getLong());
        ASSERT_EQ(buf4[i], shortV->get(i)->getIndex());
        ASSERT_EQ(buf5[i], shortV->get(i)->getFloat());
        ASSERT_EQ(buf6[i], shortV->get(i)->getDouble());
    }

    ASSERT_FALSE(shortV->getSymbol(0, size, buf2, symbase, false));
    ASSERT_FALSE(shortV->getBinary(0, size, size, buf10));
    ASSERT_TRUE(shortV->getHash(0, size, buckets, buf2));
    ASSERT_EQ(shortV->getAllocatedMemory(size), 0);

    for (auto i = 0; i < size; i++)
    {
        // ASSERT_EQ(shortV->getBoolConst(0,size,buf)[i], 1);
        // ASSERT_EQ(shortV->getCharConst(0,size,buf)[i], rand_val2);
        ASSERT_EQ(shortV->getShortConst(0, size, buf1)[i], (short)rand_val2);
        ASSERT_EQ(shortV->getIntConst(0, size, buf2)[i], (int)rand_val2);
        ASSERT_EQ(shortV->getLongConst(0, size, buf3)[i], (long long)rand_val2);
        ASSERT_EQ(shortV->getIndexConst(0, size, buf4)[i], (dolphindb::INDEX)rand_val2);
        ASSERT_EQ(shortV->getFloatConst(0, size, buf5)[i], (float)rand_val2);
        ASSERT_EQ(shortV->getDoubleConst(0, size, buf6)[i], (double)rand_val2);
    }

    ASSERT_ANY_THROW(shortV->getSymbolConst(0, size, buf2, symbase, false));
    ASSERT_ANY_THROW(shortV->getStringConst(0, size, buf8)[0]);
    ASSERT_ANY_THROW(shortV->getStringConst(0, 24, buf9));
    ASSERT_ANY_THROW(shortV->getBinaryConst(0, size, sizeof(char), buf10));

    for (auto i = 0; i < size; i++)
    {
        // ASSERT_EQ(shortV->getBoolBuffer(0,size,buf)[i], (bool)rand_val2);
        // ASSERT_EQ(shortV->getCharBuffer(0,size,buf)[i], rand_val2);
        ASSERT_EQ(shortV->getShortBuffer(0, size, buf1)[i], (short)rand_val2);
        ASSERT_EQ(shortV->getIntBuffer(0, size, buf2)[i], (int)rand_val2);
        ASSERT_EQ(shortV->getLongBuffer(0, size, buf3)[i], (long)rand_val2);
        ASSERT_EQ(shortV->getIndexBuffer(0, size, buf4)[i], (dolphindb::INDEX)rand_val2);
        ASSERT_EQ(shortV->getFloatBuffer(0, size, buf5)[i], (float)rand_val2);
        ASSERT_EQ(shortV->getDoubleBuffer(0, size, buf6)[i], (double)rand_val2);
    }

    std::cout << "-----------------long-------------------" << std::endl;
    long long rand_val3 = rand() % LLONG_MAX + 1;
    dolphindb::ConstantSP longval = dolphindb::Util::createLong(rand_val3);
    dolphindb::VectorSP longV = dolphindb::Util::createVector(dolphindb::DT_LONG, size);
    for (auto i = 0; i < size; i++)
        longV->setLong(i, rand_val3);

    ASSERT_TRUE(longV->isNull(0, size, buf0));
    for (auto i = 0; i < size; i++)
        ASSERT_FALSE(buf0[i]);

    ASSERT_TRUE(longV->isValid(0, size, buf0));
    for (auto i = 0; i < size; i++)
        ASSERT_TRUE(buf0[i]);

    // ASSERT_TRUE(longV->getBool(0,size,buf00));
    // ASSERT_TRUE(longV->getChar(0,size,buf));
    // ASSERT_TRUE(longV->getShort(0,size,buf1));
    ASSERT_TRUE(longV->getInt(0, size, buf2));
    ASSERT_TRUE(longV->getLong(0, size, buf3));
    ASSERT_TRUE(longV->getIndex(0, size, buf4));
    ASSERT_TRUE(longV->getFloat(0, size, buf5));
    ASSERT_TRUE(longV->getDouble(0, size, buf6));
    for (auto i = 0; i < size; i++)
    {
        // ASSERT_EQ(buf00[i], true);
        // ASSERT_EQ(buf[i], rand_val3);
        // ASSERT_EQ(buf1[i], longV->get(i)->getShort());
        ASSERT_EQ(buf2[i], longV->get(i)->getInt());
        ASSERT_EQ(buf3[i], longV->get(i)->getLong());
        ASSERT_EQ(buf4[i], longV->get(i)->getIndex());
        ASSERT_EQ(buf5[i], longV->get(i)->getFloat());
        ASSERT_EQ(buf6[i], longV->get(i)->getDouble());
    }

    ASSERT_FALSE(longV->getSymbol(0, size, buf2, symbase, false));
    ASSERT_FALSE(longV->getBinary(0, size, size, buf10));
    ASSERT_TRUE(longV->getHash(0, size, buckets, buf2));
    ASSERT_EQ(longV->getAllocatedMemory(size), 0);

    for (auto i = 0; i < size; i++)
    {
        // ASSERT_EQ(longV->getBoolConst(0,size,buf)[i], 1);
        // ASSERT_EQ(longV->getCharConst(0,size,buf)[i], rand_val3);
        // ASSERT_EQ(longV->getShortConst(0,size,buf1)[i], (short)rand_val3);
        ASSERT_EQ(longV->getIntConst(0, size, buf2)[i], (int)rand_val3);
        ASSERT_EQ(longV->getLongConst(0, size, buf3)[i], (long long)rand_val3);
        ASSERT_EQ(longV->getIndexConst(0, size, buf4)[i], (dolphindb::INDEX)rand_val3);
        ASSERT_EQ(longV->getFloatConst(0, size, buf5)[i], (float)rand_val3);
        ASSERT_EQ(longV->getDoubleConst(0, size, buf6)[i], (double)rand_val3);
    }

    ASSERT_ANY_THROW(longV->getSymbolConst(0, size, buf2, symbase, false));
    ASSERT_ANY_THROW(longV->getStringConst(0, size, buf8)[0]);
    ASSERT_ANY_THROW(longV->getStringConst(0, 24, buf9));
    ASSERT_ANY_THROW(longV->getBinaryConst(0, size, sizeof(char), buf10));

    for (auto i = 0; i < size; i++)
    {
        // ASSERT_EQ(longV->getBoolBuffer(0,size,buf)[i], (bool)rand_val3);
        // ASSERT_EQ(longV->getCharBuffer(0,size,buf)[i], rand_val3);
        // ASSERT_EQ(longV->getShortBuffer(0,size,buf1)[i], (short)rand_val3);
        ASSERT_EQ(longV->getIntBuffer(0, size, buf2)[i], (int)rand_val3);
        ASSERT_EQ(longV->getLongBuffer(0, size, buf3)[i], (long)rand_val3);
        ASSERT_EQ(longV->getIndexBuffer(0, size, buf4)[i], (dolphindb::INDEX)rand_val3);
        ASSERT_EQ(longV->getFloatBuffer(0, size, buf5)[i], (float)rand_val3);
        ASSERT_EQ(longV->getDoubleBuffer(0, size, buf6)[i], (double)rand_val3);
    }

    //     std::cout<<"-----------------float-------------------"<<std::endl;
    float rand_val4 = 1.5862 /*rand()/float(RAND_MAX)+1*/;
    dolphindb::VectorSP floatV = dolphindb::Util::createVector(dolphindb::DT_FLOAT, size);
    for (auto i = 0; i < size; i++)
        floatV->setFloat(i, rand_val4);
    ASSERT_TRUE(floatV->isNull(0, size, buf0));
    for (auto i = 0; i < size; i++)
        ASSERT_FALSE(buf0[i]);

    ASSERT_TRUE(floatV->isValid(0, size, buf0));
    for (auto i = 0; i < size; i++)
        ASSERT_TRUE(buf0[i]);

    // ASSERT_TRUE(floatV->getBool(0,size,buf00));
    // ASSERT_TRUE(floatV->getChar(0,size,buf));
    ASSERT_TRUE(floatV->getShort(0, size, buf1));
    ASSERT_TRUE(floatV->getInt(0, size, buf2));
    ASSERT_TRUE(floatV->getLong(0, size, buf3));
    ASSERT_TRUE(floatV->getIndex(0, size, buf4));
    ASSERT_TRUE(floatV->getFloat(0, size, buf5));
    ASSERT_TRUE(floatV->getDouble(0, size, buf6));
    for (auto i = 0; i < size; i++)
    {
        // ASSERT_EQ(buf00[i], true);
        // ASSERT_EQ(buf[i], rand_val4);
        ASSERT_EQ(buf1[i], floatV->get(i)->getShort());
        ASSERT_EQ(buf2[i], floatV->get(i)->getInt());
        ASSERT_EQ(buf3[i], floatV->get(i)->getLong());
        ASSERT_EQ(buf4[i], floatV->get(i)->getIndex());
        ASSERT_EQ(buf5[i], floatV->get(i)->getFloat());
        ASSERT_EQ(buf6[i], floatV->get(i)->getDouble());
    }

    ASSERT_FALSE(floatV->getSymbol(0, size, buf2, symbase, false));
    ASSERT_FALSE(floatV->getBinary(0, size, size, buf10));
    ASSERT_FALSE(floatV->getHash(0, size, buckets, buf2));
    ASSERT_EQ(floatV->getAllocatedMemory(size), 0);

    for (auto i = 0; i < size; i++)
    {
        // ASSERT_EQ(floatV->getBoolConst(0,size,buf)[i], 1);
        // ASSERT_EQ(floatV->getCharConst(0,size,buf)[i], rand_val4);
        ASSERT_EQ(floatV->getShortConst(0, size, buf1)[i], floatV->get(i)->getShort());
        ASSERT_EQ(floatV->getIntConst(0, size, buf2)[i], floatV->get(i)->getInt());
        ASSERT_EQ(floatV->getLongConst(0, size, buf3)[i], floatV->get(i)->getLong());
        ASSERT_EQ(floatV->getIndexConst(0, size, buf4)[i], floatV->get(i)->getIndex());
        ASSERT_EQ(floatV->getFloatConst(0, size, buf5)[i], floatV->get(i)->getFloat());
        ASSERT_EQ(floatV->getDoubleConst(0, size, buf6)[i], floatV->get(i)->getDouble());
    }

    ASSERT_ANY_THROW(floatV->getSymbolConst(0, size, buf2, symbase, false));
    ASSERT_ANY_THROW(floatV->getStringConst(0, size, buf8)[0]);
    ASSERT_ANY_THROW(floatV->getStringConst(0, 24, buf9));
    ASSERT_ANY_THROW(floatV->getBinaryConst(0, size, sizeof(char), buf10));

    for (auto i = 0; i < size; i++)
    {
        // ASSERT_EQ(floatV->getBoolBuffer(0,size,buf)[i], (bool)2);
        // ASSERT_EQ(floatV->getCharBuffer(0,size,buf)[i], 2);
        ASSERT_EQ(floatV->getShortBuffer(0, size, buf1)[i], floatV->get(i)->getShort());
        ASSERT_EQ(floatV->getIntBuffer(0, size, buf2)[i], floatV->get(i)->getInt());
        ASSERT_EQ(floatV->getLongBuffer(0, size, buf3)[i], floatV->get(i)->getLong());
        ASSERT_EQ(floatV->getIndexBuffer(0, size, buf4)[i], floatV->get(i)->getIndex());
        ASSERT_EQ(floatV->getFloatBuffer(0, size, buf5)[i], floatV->get(i)->getFloat());
        ASSERT_EQ(floatV->getDoubleBuffer(0, size, buf6)[i], floatV->get(i)->getDouble());
    }

    //    std::cout<<"-----------------double-------------------"<<std::endl;
    double rand_val5 = 2.1345 /*rand()/float(RAND_MAX)+1*/;
    dolphindb::VectorSP doubleV = dolphindb::Util::createVector(dolphindb::DT_DOUBLE, size);
    for (auto i = 0; i < size; i++)
        doubleV->setDouble(i, rand_val4);
    ASSERT_TRUE(doubleV->isNull(0, size, buf0));
    for (auto i = 0; i < size; i++)
        ASSERT_FALSE(buf0[i]);

    ASSERT_TRUE(doubleV->isValid(0, size, buf0));
    for (auto i = 0; i < size; i++)
        ASSERT_TRUE(buf0[i]);

    // ASSERT_TRUE(doubleV->getBool(0,size,buf00));
    // ASSERT_TRUE(doubleV->getChar(0,size,buf));
    ASSERT_TRUE(doubleV->getShort(0, size, buf1));
    ASSERT_TRUE(doubleV->getInt(0, size, buf2));
    ASSERT_TRUE(doubleV->getLong(0, size, buf3));
    ASSERT_TRUE(doubleV->getIndex(0, size, buf4));
    ASSERT_TRUE(doubleV->getFloat(0, size, buf5));
    ASSERT_TRUE(doubleV->getDouble(0, size, buf6));
    for (auto i = 0; i < size; i++)
    {
        // ASSERT_EQ(buf00[i], true);
        // ASSERT_EQ(buf[i], rand_val4);
        ASSERT_EQ(buf1[i], doubleV->get(i)->getShort());
        ASSERT_EQ(buf2[i], doubleV->get(i)->getInt());
        ASSERT_EQ(buf3[i], doubleV->get(i)->getLong());
        ASSERT_EQ(buf4[i], doubleV->get(i)->getIndex());
        ASSERT_EQ(buf5[i], doubleV->get(i)->getFloat());
        ASSERT_EQ(buf6[i], doubleV->get(i)->getDouble());
    }

    ASSERT_FALSE(doubleV->getSymbol(0, size, buf2, symbase, false));
    ASSERT_FALSE(doubleV->getBinary(0, size, size, buf10));
    ASSERT_FALSE(doubleV->getHash(0, size, buckets, buf2));
    ASSERT_EQ(doubleV->getAllocatedMemory(size), 0);

    for (auto i = 0; i < size; i++)
    {
        // ASSERT_EQ(doubleV->getBoolConst(0,size,buf)[i], 1);
        // ASSERT_EQ(doubleV->getCharConst(0,size,buf)[i], rand_val4);
        ASSERT_EQ(doubleV->getShortConst(0, size, buf1)[i], doubleV->get(i)->getShort());
        ASSERT_EQ(doubleV->getIntConst(0, size, buf2)[i], doubleV->get(i)->getInt());
        ASSERT_EQ(doubleV->getLongConst(0, size, buf3)[i], doubleV->get(i)->getLong());
        ASSERT_EQ(doubleV->getIndexConst(0, size, buf4)[i], doubleV->get(i)->getIndex());
        ASSERT_EQ(doubleV->getFloatConst(0, size, buf5)[i], doubleV->get(i)->getFloat());
        ASSERT_EQ(doubleV->getDoubleConst(0, size, buf6)[i], doubleV->get(i)->getDouble());
    }

    ASSERT_ANY_THROW(doubleV->getSymbolConst(0, size, buf2, symbase, false));
    ASSERT_ANY_THROW(doubleV->getStringConst(0, size, buf8)[0]);
    ASSERT_ANY_THROW(doubleV->getStringConst(0, 24, buf9));
    ASSERT_ANY_THROW(doubleV->getBinaryConst(0, size, sizeof(char), buf10));

    for (auto i = 0; i < size; i++)
    {
        // ASSERT_EQ(doubleV->getBoolBuffer(0,size,buf)[i], (bool)2);
        // ASSERT_EQ(doubleV->getCharBuffer(0,size,buf)[i], 2);
        ASSERT_EQ(doubleV->getShortBuffer(0, size, buf1)[i], doubleV->get(i)->getShort());
        ASSERT_EQ(doubleV->getIntBuffer(0, size, buf2)[i], doubleV->get(i)->getInt());
        ASSERT_EQ(doubleV->getLongBuffer(0, size, buf3)[i], doubleV->get(i)->getLong());
        ASSERT_EQ(doubleV->getIndexBuffer(0, size, buf4)[i], doubleV->get(i)->getIndex());
        ASSERT_EQ(doubleV->getFloatBuffer(0, size, buf5)[i], doubleV->get(i)->getFloat());
        ASSERT_EQ(doubleV->getDoubleBuffer(0, size, buf6)[i], doubleV->get(i)->getDouble());
    }

    delete[] buf, buf1, buf2, buf3, buf4, buf5, buf6, buf9, buf10, buf00, buf0, buf8;
}

class initParaTest : public testing::Test, public ::testing::WithParamInterface<std::tuple<std::string, std::string, std::string>>
{
    public:
        static dolphindb::DBConnection conn;
        // Suite
        static void SetUpTestSuite()
        {
            // dolphindb::DBConnection conn;
            conn.initialize();
            bool ret = conn.connect(HOST, PORT, USER, PASSWD);
            if (!ret)
            {
                std::cout << "Failed to connect to the server" << std::endl;
            }
            else
            {
                std::cout << "connect to " + HOST + ":" + std::to_string(PORT) << std::endl;
                conn.run("CHAR_MIN = char(-(exp2(7)-1));"
                        "CHAR_MAX = char(exp2(7)-1);"
                        "INT_MIN = int(-(exp2(31)-1));"
                        "INT_MAX = int(exp2(31)-1);"
                        "SHRT_MIN = short(-(exp2(15)-1));"
                        "SHRT_MAX = short(exp2(15)-1);"
                        "LLONG_MIN = -9223372036854775807l;"
                        "LLONG_MAX = 9223372036854775807l;");
            }
        }
        static void TearDownTestSuite()
        {
            conn.close();
        }

    protected:
        // Case
        virtual void SetUp()
        {

        }
        virtual void TearDown()
        {

        }
};

dolphindb::DBConnection initParaTest::conn(false, false);

class BigArray_SomeNull : public initParaTest
{
};

namespace BigArray1
{
    std::vector<std::tuple<std::string, std::string, std::string>> get_prepare()
    {
        std::vector<std::string> name = {"BOOL", "CHAR", "SHORT", "INT", "LONG", "DATE", "MONTH",
                               "TIME", "MINUTE", "SECOND", "DATETIME", "TIMESTAMP", "NANOTIME",
                               "NANOTIMESTAMP", "FLOAT", "DOUBLE", "SYMBOL", "UUID",
                               "IPADDR", "INT128", "DATEHOUR", "DECIMAL32", "DECIMAL64", "BLOB"};
        std::vector<std::string> script = {"x=bigarray(BOOL,0,10000000).append!(0..2).append!(CHAR_MAX).append!(CHAR_MIN).append!(NULL);x",
                                 "x=bigarray(CHAR,0,10000000).append!(0..2).append!(CHAR_MAX).append!(CHAR_MIN).append!(NULL);x",
                                 "x=bigarray(SHORT,0,10000000).append!(0..2).append!(SHRT_MAX).append!(SHRT_MIN).append!(NULL);x",
                                 "x=bigarray(INT,0,10000000).append!(0..2).append!(INT_MAX).append!(INT_MIN).append!(NULL);x",
                                 "x=bigarray(LONG,0,10000000).append!(0..2).append!(LLONG_MAX).append!(LLONG_MIN).append!(NULL);x",
                                 "x=bigarray(DATE,0,10000000).append!(1..5).append!(NULL);x",
                                 "x=bigarray(MONTH,0,10000000).append!(1..5).append!(NULL);x",
                                 "x=bigarray(TIME,0,10000000).append!(1..5).append!(NULL);x",
                                 "x=bigarray(MINUTE,0,10000000).append!(1..5).append!(NULL);x",
                                 "x=bigarray(SECOND,0,10000000).append!(1..5).append!(NULL);x",
                                 "x=bigarray(DATETIME,0,10000000).append!(1..5).append!(NULL);x",
                                 "x=bigarray(TIMESTAMP,0,10000000).append!(1..5).append!(NULL);x",
                                 "x=bigarray(NANOTIME,0,10000000).append!(1..5).append!(NULL);x",
                                 "x=bigarray(NANOTIMESTAMP,0,10000000).append!(1..5).append!(NULL);x",
                                 "x=bigarray(FLOAT,0,10000000).append!(1..5).append!(NULL);x",
                                 "x=bigarray(DOUBLE,0,10000000).append!(1..5).append!(NULL);x",
                                 "x=bigarray(SYMBOL,0,10000000).append!(string(1..5)).append!('');x",
                                 "x=bigarray(UUID,0,10000000).append!(take(uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87'),5)).append!(uuid(''));x",
                                 "x=bigarray(IPADDR,0,10000000).append!(take(ipaddr('1.1.1.1'),5)).append!(ipaddr(''));x",
                                 "x=bigarray(INT128,0,10000000).append!(take(int128('e1671797c52e15f763380b45e841ec32'),5)).append!(int128(''));x",
                                 "x=bigarray(DATEHOUR,0,10000000).append!(1..5).append!(NULL);x",
                                 "x=bigarray(DECIMAL32(2),0,10000000).append!(1..5).append!(NULL);x",
                                 "x=bigarray(DECIMAL64(10),0,10000000).append!(1..5).append!(NULL);x",
                                 "x=bigarray(BLOB,0,10000000).append!(string(1..5)).append!('');x"};
        std::vector<std::string> ex_val = {"[0,1,1,1,1,]", "[0,1,2,127,-127,]", "[0,1,2,32767,-32767,]",
                                 "[0,1,2,2147483647,-2147483647,]", "[0,1,2,9223372036854775807,-9223372036854775807,]",
                                 "[1970.01.02,1970.01.03,1970.01.04,1970.01.05,1970.01.06,]", "[0000.02M,0000.03M,0000.04M,0000.05M,0000.06M,]",
                                 "[00:00:00.001,00:00:00.002,00:00:00.003,00:00:00.004,00:00:00.005,]", "[00:01m,00:02m,00:03m,00:04m,00:05m,]",
                                 "[00:00:01,00:00:02,00:00:03,00:00:04,00:00:05,]",
                                 "[1970.01.01T00:00:01,1970.01.01T00:00:02,1970.01.01T00:00:03,1970.01.01T00:00:04,1970.01.01T00:00:05,]",
                                 "[1970.01.01T00:00:00.001,1970.01.01T00:00:00.002,1970.01.01T00:00:00.003,1970.01.01T00:00:00.004,1970.01.01T00:00:00.005,]",
                                 "[00:00:00.000000001,00:00:00.000000002,00:00:00.000000003,00:00:00.000000004,00:00:00.000000005,]",
                                 "[1970.01.01T00:00:00.000000001,1970.01.01T00:00:00.000000002,1970.01.01T00:00:00.000000003,1970.01.01T00:00:00.000000004,1970.01.01T00:00:00.000000005,]",
                                 "[1,2,3,4,5,]", "[1,2,3,4,5,]", "[\"1\",\"2\",\"3\",\"4\",\"5\",]",
                                 "[5d212a78-cc48-e3b1-4235-b4d91473ee87,5d212a78-cc48-e3b1-4235-b4d91473ee87,5d212a78-cc48-e3b1-4235-b4d91473ee87,5d212a78-cc48-e3b1-4235-b4d91473ee87,5d212a78-cc48-e3b1-4235-b4d91473ee87,]",
                                 "[1.1.1.1,1.1.1.1,1.1.1.1,1.1.1.1,1.1.1.1,]",
                                 "[e1671797c52e15f763380b45e841ec32,e1671797c52e15f763380b45e841ec32,e1671797c52e15f763380b45e841ec32,e1671797c52e15f763380b45e841ec32,e1671797c52e15f763380b45e841ec32,]",
                                 "[1970.01.01T01,1970.01.01T02,1970.01.01T03,1970.01.01T04,1970.01.01T05,]",
                                 "[1.00,2.00,3.00,4.00,5.00,]", "[1.0000000000,2.0000000000,3.0000000000,4.0000000000,5.0000000000,]", "[\"1\",\"2\",\"3\",\"4\",\"5\",]"};
        std::vector<std::tuple<std::string, std::string, std::string>> prepare;
        for (auto i = 0; i < int(name.size()); i++)
        {
            prepare.push_back(std::make_tuple(name[i], script[i], ex_val[i]));
        }
        return prepare;
    }
}

INSTANTIATE_TEST_SUITE_P(someNull_BigArray, BigArray_SomeNull, testing::ValuesIn(BigArray1::get_prepare()));
TEST_P(BigArray_SomeNull, test_bigArray)
{
    std::string cur_type = std::get<0>(GetParam());
    std::cout << "download bigarray type " << cur_type << std::endl;
    dolphindb::VectorSP bigV = conn.run(std::get<1>(GetParam()));
    int size = bigV->size();
    ASSERT_EQ(size, 6);
    ASSERT_EQ(bigV->getForm(), dolphindb::DF_VECTOR);
    if (cur_type == "SYMBOL")
    {
        ASSERT_EQ(dolphindb::Util::getDataTypeString(bigV->getType()), "STRING");
    }
    else
    {
        ASSERT_EQ(dolphindb::Util::getDataTypeString(bigV->getType()), std::get<0>(GetParam()));
    }
    ASSERT_EQ(bigV->getString(), std::get<2>(GetParam()));
    ASSERT_TRUE(bigV->get(5)->isNull());
    conn.upload("res", {bigV});
    ASSERT_TRUE(conn.run("eqObj(res,x)")->getBool());
}

class BigArray_AllNull : public initParaTest
{
};

namespace BigArray2
{
    std::vector<std::tuple<std::string, std::string, std::string>> get_prepare()
    {
        std::vector<std::string> name = {"BOOL", "CHAR", "SHORT", "INT", "LONG", "DATE", "MONTH",
                               "TIME", "MINUTE", "SECOND", "DATETIME", "TIMESTAMP", "NANOTIME",
                               "NANOTIMESTAMP", "FLOAT", "DOUBLE", "SYMBOL", "UUID",
                               "IPADDR", "INT128", "DATEHOUR", "DECIMAL32", "DECIMAL64", "BLOB"};
        std::vector<std::string> script = {"x=bigarray(BOOL,0,10000000).append!(take(bool(NULL),6));x",
                                 "x=bigarray(CHAR,0,10000000).append!(take(char(NULL),6));x",
                                 "x=bigarray(SHORT,0,10000000).append!(take(short(NULL),6));x",
                                 "x=bigarray(INT,0,10000000).append!(take(int(NULL),6));x",
                                 "x=bigarray(LONG,0,10000000).append!(take(long(NULL),6));x",
                                 "x=bigarray(DATE,0,10000000).append!(take(date(NULL),6));x",
                                 "x=bigarray(MONTH,0,10000000).append!(take(month(NULL),6));x",
                                 "x=bigarray(TIME,0,10000000).append!(take(time(NULL),6));x",
                                 "x=bigarray(MINUTE,0,10000000).append!(take(minute(NULL),6));x",
                                 "x=bigarray(SECOND,0,10000000).append!(take(second(NULL),6));x",
                                 "x=bigarray(DATETIME,0,10000000).append!(take(datetime(NULL),6));x",
                                 "x=bigarray(TIMESTAMP,0,10000000).append!(take(timestamp(NULL),6));x",
                                 "x=bigarray(NANOTIME,0,10000000).append!(take(nanotime(NULL),6));x",
                                 "x=bigarray(NANOTIMESTAMP,0,10000000).append!(take(nanotimestamp(NULL),6));x",
                                 "x=bigarray(FLOAT,0,10000000).append!(take(float(NULL),6));x",
                                 "x=bigarray(DOUBLE,0,10000000).append!(take(double(NULL),6));x",
                                 "x=bigarray(SYMBOL,0,10000000).append!(symbol(take('',6)));x",
                                 "x=bigarray(UUID,0,10000000).append!(take(uuid(''),6));x",
                                 "x=bigarray(IPADDR,0,10000000).append!(take(ipaddr(''),6));x",
                                 "x=bigarray(INT128,0,10000000).append!(take(int128(''),6));x",
                                 "x=bigarray(DATEHOUR,0,10000000).append!(take(datehour(NULL),6));x",
                                 "x=bigarray(DECIMAL32(2),0,10000000).append!(take(decimal32(NULL,2),6));x",
                                 "x=bigarray(DECIMAL64(2),0,10000000).append!(take(decimal64(NULL,10),6));x",
                                 "x=bigarray(BLOB,0,10000000).append!(take(blob(''),6));x"};
        std::vector<std::string> ex_val = {
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
        };
        std::vector<std::tuple<std::string, std::string, std::string>> prepare;
        for (auto i = 0; i < int(name.size()); i++)
        {
            prepare.push_back(std::make_tuple(name[i], script[i], ex_val[i]));
        }
        return prepare;
    }
}

INSTANTIATE_TEST_SUITE_P(allNull_BigArray, BigArray_AllNull, testing::ValuesIn(BigArray2::get_prepare()));
TEST_P(BigArray_AllNull, test_bigArray)
{
    std::string cur_type = std::get<0>(GetParam());
    std::cout << "download bigarray type " << cur_type << std::endl;
    dolphindb::VectorSP bigV = conn.run(std::get<1>(GetParam()));
    int size = bigV->size();
    ASSERT_EQ(size, 6);
    ASSERT_EQ(bigV->getForm(), dolphindb::DF_VECTOR);
    ASSERT_EQ(dolphindb::Util::getDataTypeString(bigV->getType()), std::get<0>(GetParam()));
    for (auto i = 0; i < size; i++)
    {
        ASSERT_TRUE(bigV->get(i)->isNull());
    }

    ASSERT_EQ(bigV->getString(), std::get<2>(GetParam()));
    ASSERT_TRUE(bigV->get(5)->isNull());
    conn.upload("res", {bigV});
    ASSERT_TRUE(conn.run("eqObj(res,x)")->getBool());
}

class BigArray_gt1048576 : public initParaTest
{
};

namespace BigArray3
{
    std::vector<std::tuple<std::string, std::string, std::string>> get_prepare()
    {
        std::vector<std::string> name = {"BOOL", "CHAR", "SHORT", "INT", "LONG", "DATE", "MONTH",
                               "TIME", "MINUTE", "SECOND", "DATETIME", "TIMESTAMP", "NANOTIME",
                               "NANOTIMESTAMP", "FLOAT", "DOUBLE", "SYMBOL", "UUID",
                               "IPADDR", "INT128", "DATEHOUR", "DECIMAL32", "DECIMAL64", "BLOB"};
        std::vector<std::string> script = {"x=bigarray(BOOL,0,10000000).append!(take(CHAR_MIN,200000)).append!(take(CHAR_MAX,200000)).append!(take(0,600000)).append!(take(bool(NULL),100000));x",
                                 "x=bigarray(CHAR,0,10000000).append!(take(CHAR_MIN,200000)).append!(take(CHAR_MAX,200000)).append!(take(0,600000)).append!(take(char(NULL),100000));x",
                                 "x=bigarray(SHORT,0,10000000).append!(take(SHRT_MIN,200000)).append!(take(SHRT_MAX,200000)).append!(take(0,600000)).append!(take(short(NULL),100000));x",
                                 "x=bigarray(INT,0,10000000).append!(take(INT_MIN,200000)).append!(take(INT_MAX,200000)).append!(take(0,600000)).append!(take(int(NULL),100000));x",
                                 "x=bigarray(LONG,0,10000000).append!(take(LLONG_MIN,200000)).append!(take(LLONG_MAX,200000)).append!(take(0,600000)).append!(take(long(NULL),100000));x",
                                 "x=bigarray(DATE,0,10000000).append!(take(INT_MIN,200000)).append!(take(INT_MAX,200000)).append!(take(0,600000)).append!(take(date(NULL),100000));x",
                                 "x=bigarray(MONTH,0,10000000).append!(take(INT_MIN,200000)).append!(take(INT_MAX,200000)).append!(take(0,600000)).append!(take(month(NULL),100000));x",
                                 "x=bigarray(TIME,0,10000000).append!(take(INT_MIN,200000)).append!(take(INT_MAX,200000)).append!(take(0,600000)).append!(take(time(NULL),100000));x",
                                 "x=bigarray(MINUTE,0,10000000).append!(take(INT_MIN,200000)).append!(take(INT_MAX,200000)).append!(take(0,600000)).append!(take(minute(NULL),100000));x",
                                 "x=bigarray(SECOND,0,10000000).append!(take(INT_MIN,200000)).append!(take(INT_MAX,200000)).append!(take(0,600000)).append!(take(second(NULL),100000));x",
                                 "x=bigarray(DATETIME,0,10000000).append!(take(INT_MIN,200000)).append!(take(INT_MAX,200000)).append!(take(0,600000)).append!(take(datetime(NULL),100000));x",
                                 "x=bigarray(TIMESTAMP,0,10000000).append!(take(LLONG_MIN,200000)).append!(take(LLONG_MAX,200000)).append!(take(0,600000)).append!(take(timestamp(NULL),100000));x",
                                 "x=bigarray(NANOTIME,0,10000000).append!(take(LLONG_MIN,200000)).append!(take(LLONG_MAX,200000)).append!(take(0,600000)).append!(take(nanotime(NULL),100000));x",
                                 "x=bigarray(NANOTIMESTAMP,0,10000000).append!(take(LLONG_MIN,200000)).append!(take(LLONG_MAX,200000)).append!(take(0,600000)).append!(take(nanotimestamp(NULL),100000));x",
                                 "x=bigarray(FLOAT,0,10000000).append!(take(-1.123456,1000000)).append!(take(float(NULL),100000));x",
                                 "x=bigarray(DOUBLE,0,10000000).append!(take(-1.123456,1000000)).append!(take(double(NULL),100000));x",
                                 "x=bigarray(SYMBOL,0,10000000).append!(symbol(take(`str,1000000))).append!(symbol(take('',100000)));x",
                                 "x=bigarray(UUID,0,10000000).append!(take(uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87'),1000000)).append!(take(uuid(''),100000));x",
                                 "x=bigarray(IPADDR,0,10000000).append!(take(ipaddr('1.1.1.1'),1000000)).append!(take(ipaddr(''),100000));x",
                                 "x=bigarray(INT128,0,10000000).append!(take(int128('e1671797c52e15f763380b45e841ec32'),1000000)).append!(take(int128(''),100000));x",
                                 "x=bigarray(DATEHOUR,0,10000000).append!(take(INT_MIN,200000)).append!(take(INT_MAX,200000)).append!(take(0,600000)).append!(take(datehour(NULL),100000));x",
                                 "x=bigarray(DECIMAL32(2),0,10000000).append!(take(decimal32(0.456789,2),1000000)).append!(take(decimal32(NULL,2),100000));x",
                                 "x=bigarray(DECIMAL64(10),0,10000000).append!(take(decimal64(-0.13,10),1000000)).append!(take(decimal64(NULL,10),100000));x",
                                 "x=bigarray(BLOB,0,10000000).append!(string(take(`blob,1000000))).append!(take(blob(''),100000));x"};
        std::vector<std::string> ex_val = {
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
            "[,,,,,]",
        };
        std::vector<std::tuple<std::string, std::string, std::string>> prepare;
        for (auto i = 0; i < int(name.size()); i++)
        {
            prepare.push_back(std::make_tuple(name[i], script[i], ex_val[i]));
        }
        return prepare;
    }
}

INSTANTIATE_TEST_SUITE_P(gt1048576_BigArray, BigArray_gt1048576, testing::ValuesIn(BigArray3::get_prepare()));
TEST_P(BigArray_gt1048576, test_bigArray)
{
    std::string cur_type = std::get<0>(GetParam());
    std::cout << "download bigarray type " << cur_type << std::endl;
    dolphindb::VectorSP bigV = conn.run(std::get<1>(GetParam()));
    int size = bigV->size();
    ASSERT_EQ(size, 1100000);
    ASSERT_EQ(bigV->getForm(), dolphindb::DF_VECTOR);
    std::string ex_type = std::get<0>(GetParam());
    ASSERT_EQ(dolphindb::Util::getDataTypeString(bigV->getType()), ex_type);
    char char_min = conn.run("CHAR_MIN")->getChar();
    char char_max = conn.run("CHAR_MAX")->getChar();
    short short_min = conn.run("SHRT_MIN")->getShort();
    short short_max = conn.run("SHRT_MAX")->getShort();
    int int_min = conn.run("INT_MIN")->getInt();
    int int_max = conn.run("INT_MAX")->getInt();
    long long long_min = conn.run("LLONG_MIN")->getLong();
    long long long_max = conn.run("LLONG_MAX")->getLong();

    if (ex_type == "BOOL")
    {
        for (auto i = 0; i < 400000; i++)
        {
            ASSERT_EQ(bigV->get(i)->getBool(), 1);
        }
        for (auto i = 0; i < 600000; i++)
        {
            ASSERT_EQ(bigV->get(i + 400000)->getBool(), 0);
        }
        for (auto i = 0; i < 100000; i++)
        {
            ASSERT_TRUE(bigV->get(i + 1000000)->isNull());
        }
    }
    else if (ex_type == "CHAR")
    {
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i)->getChar(), char_min);
        }
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i + 200000)->getChar(), char_max);
        }
        for (auto i = 0; i < 600000; i++)
        {
            ASSERT_EQ(bigV->get(i + 400000)->getChar(), (char)0);
        }
        for (auto i = 0; i < 100000; i++)
        {
            ASSERT_TRUE(bigV->get(i + 1000000)->isNull());
        }
    }
    else if (ex_type == "SHORT")
    {
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i)->getShort(), short_min);
        }
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i + 200000)->getShort(), short_max);
        }
        for (auto i = 0; i < 600000; i++)
        {
            ASSERT_EQ(bigV->get(i + 400000)->getShort(), (short)0);
        }
        for (auto i = 0; i < 100000; i++)
        {
            ASSERT_TRUE(bigV->get(i + 1000000)->isNull());
        }
    }
    else if (ex_type == "INT")
    {
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i)->getInt(), int_min);
        }
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i + 200000)->getInt(), int_max);
        }
        for (auto i = 0; i < 600000; i++)
        {
            ASSERT_EQ(bigV->get(i + 400000)->getInt(), (int)0);
        }
        for (auto i = 0; i < 100000; i++)
        {
            ASSERT_TRUE(bigV->get(i + 1000000)->isNull());
        }
    }
    else if (ex_type == "LONG")
    {
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i)->getLong(), long_min);
        }
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i + 200000)->getLong(), long_max);
        }
        for (auto i = 0; i < 600000; i++)
        {
            ASSERT_EQ(bigV->get(i + 400000)->getLong(), (long long)0);
        }
        for (auto i = 0; i < 100000; i++)
        {
            ASSERT_TRUE(bigV->get(i + 1000000)->isNull());
        }
    }
    else if (ex_type == "DATE")
    {
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i)->getString(), dolphindb::Util::createDate(int_min)->getString());
        }
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i + 200000)->getString(), dolphindb::Util::createDate(int_max)->getString());
        }
        for (auto i = 0; i < 600000; i++)
        {
            ASSERT_EQ(bigV->get(i + 400000)->getString(), dolphindb::Util::createDate(0)->getString());
        }
        for (auto i = 0; i < 100000; i++)
        {
            ASSERT_TRUE(bigV->get(i + 1000000)->isNull());
        }
    }
    else if (ex_type == "MONTH")
    {
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i)->getString(), dolphindb::Util::createMonth(int_min)->getString());
        }
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i + 200000)->getString(), dolphindb::Util::createMonth(int_max)->getString());
        }
        for (auto i = 0; i < 600000; i++)
        {
            ASSERT_EQ(bigV->get(i + 400000)->getString(), dolphindb::Util::createMonth(0)->getString());
        }
        for (auto i = 0; i < 100000; i++)
        {
            ASSERT_TRUE(bigV->get(i + 1000000)->isNull());
        }
    }
    else if (ex_type == "TIME")
    {
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i)->getString(), dolphindb::Util::createTime(int_min)->getString());
        }
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i + 200000)->getString(), dolphindb::Util::createTime(int_max)->getString());
        }
        for (auto i = 0; i < 600000; i++)
        {
            ASSERT_EQ(bigV->get(i + 400000)->getString(), dolphindb::Util::createTime(0)->getString());
        }
        for (auto i = 0; i < 100000; i++)
        {
            ASSERT_TRUE(bigV->get(i + 1000000)->isNull());
        }
    }
    else if (ex_type == "MINUTE")
    {
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i)->getString(), dolphindb::Util::createMinute(int_min)->getString());
        }
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i + 200000)->getString(), dolphindb::Util::createMinute(int_max)->getString());
        }
        for (auto i = 0; i < 600000; i++)
        {
            ASSERT_EQ(bigV->get(i + 400000)->getString(), dolphindb::Util::createMinute(0)->getString());
        }
        for (auto i = 0; i < 100000; i++)
        {
            ASSERT_TRUE(bigV->get(i + 1000000)->isNull());
        }
    }
    else if (ex_type == "SECOND")
    {
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i)->getString(), dolphindb::Util::createSecond(int_min)->getString());
        }
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i + 200000)->getString(), dolphindb::Util::createSecond(int_max)->getString());
        }
        for (auto i = 0; i < 600000; i++)
        {
            ASSERT_EQ(bigV->get(i + 400000)->getString(), dolphindb::Util::createSecond(0)->getString());
        }
        for (auto i = 0; i < 100000; i++)
        {
            ASSERT_TRUE(bigV->get(i + 1000000)->isNull());
        }
    }
    else if (ex_type == "DATETIME")
    {
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i)->getString(), dolphindb::Util::createDateTime(int_min)->getString());
        }
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i + 200000)->getString(), dolphindb::Util::createDateTime(int_max)->getString());
        }
        for (auto i = 0; i < 600000; i++)
        {
            ASSERT_EQ(bigV->get(i + 400000)->getString(), dolphindb::Util::createDateTime(0)->getString());
        }
        for (auto i = 0; i < 100000; i++)
        {
            ASSERT_TRUE(bigV->get(i + 1000000)->isNull());
        }
    }
    else if (ex_type == "TIMESTAMP")
    {
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i)->getString(), dolphindb::Util::createTimestamp(long_min)->getString());
        }
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i + 200000)->getString(), dolphindb::Util::createTimestamp(long_max)->getString());
        }
        for (auto i = 0; i < 600000; i++)
        {
            ASSERT_EQ(bigV->get(i + 400000)->getString(), dolphindb::Util::createTimestamp(0)->getString());
        }
        for (auto i = 0; i < 100000; i++)
        {
            ASSERT_TRUE(bigV->get(i + 1000000)->isNull());
        }
    }
    else if (ex_type == "NANOTIME")
    {
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i)->getString(), dolphindb::Util::createNanoTime(long_min)->getString());
        }
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i + 200000)->getString(), dolphindb::Util::createNanoTime(long_max)->getString());
        }
        for (auto i = 0; i < 600000; i++)
        {
            ASSERT_EQ(bigV->get(i + 400000)->getString(), dolphindb::Util::createNanoTime(0)->getString());
        }
        for (auto i = 0; i < 100000; i++)
        {
            ASSERT_TRUE(bigV->get(i + 1000000)->isNull());
        }
    }
    else if (ex_type == "NANOTIMESTAMP")
    {
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i)->getString(), dolphindb::Util::createNanoTimestamp(long_min)->getString());
        }
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i + 200000)->getString(), dolphindb::Util::createNanoTimestamp(long_max)->getString());
        }
        for (auto i = 0; i < 600000; i++)
        {
            ASSERT_EQ(bigV->get(i + 400000)->getString(), dolphindb::Util::createNanoTimestamp(0)->getString());
        }
        for (auto i = 0; i < 100000; i++)
        {
            ASSERT_TRUE(bigV->get(i + 1000000)->isNull());
        }
    }
    else if (ex_type == "DATEHOUR")
    {
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i)->getString(), dolphindb::Util::createDateHour(int_min)->getString());
        }
        for (auto i = 0; i < 200000; i++)
        {
            ASSERT_EQ(bigV->get(i + 200000)->getString(), dolphindb::Util::createDateHour(int_max)->getString());
        }
        for (auto i = 0; i < 600000; i++)
        {
            ASSERT_EQ(bigV->get(i + 400000)->getString(), dolphindb::Util::createDateHour(0)->getString());
        }
        for (auto i = 0; i < 100000; i++)
        {
            ASSERT_TRUE(bigV->get(i + 1000000)->isNull());
        }
    }
    else if (ex_type == "FLOAT")
    {
        for (auto i = 0; i < 1000000; i++)
        {
            ASSERT_FLOAT_EQ(bigV->get(i)->getFloat(), (float)-1.123456);
        }
        for (auto i = 0; i < 100000; i++)
        {
            ASSERT_TRUE(bigV->get(i + 1000000)->isNull());
        }
    }
    else if (ex_type == "DOUBLE")
    {
        for (auto i = 0; i < 1000000; i++)
        {
            ASSERT_FLOAT_EQ(bigV->get(i)->getDouble(), (double)-1.123456);
        }
        for (auto i = 0; i < 100000; i++)
        {
            ASSERT_TRUE(bigV->get(i + 1000000)->isNull());
        }
    }
    else if (ex_type == "SYMBOL")
    {
        for (auto i = 0; i < 1000000; i++)
        {
            ASSERT_EQ(bigV->get(i)->getString(), "str");
        }
        for (auto i = 0; i < 100000; i++)
        {
            ASSERT_TRUE(bigV->get(i + 1000000)->isNull());
        }
    }
    else if (ex_type == "UUID")
    {
        for (auto i = 0; i < 1000000; i++)
        {
            ASSERT_EQ(bigV->get(i)->getString(), "5d212a78-cc48-e3b1-4235-b4d91473ee87");
        }
        for (auto i = 0; i < 100000; i++)
        {
            ASSERT_TRUE(bigV->get(i + 1000000)->isNull());
        }
    }
    else if (ex_type == "IPADDR")
    {
        for (auto i = 0; i < 1000000; i++)
        {
            ASSERT_EQ(bigV->get(i)->getString(), "1.1.1.1");
        }
        for (auto i = 0; i < 100000; i++)
        {
            ASSERT_TRUE(bigV->get(i + 1000000)->isNull());
        }
    }
    else if (ex_type == "INT128")
    {
        for (auto i = 0; i < 1000000; i++)
        {
            ASSERT_EQ(bigV->get(i)->getString(), "e1671797c52e15f763380b45e841ec32");
        }
        for (auto i = 0; i < 100000; i++)
        {
            ASSERT_TRUE(bigV->get(i + 1000000)->isNull());
        }
    }
    else if (ex_type == "DECIMAL32")
    {
        for (auto i = 0; i < 1000000; i++)
        {
            ASSERT_EQ(bigV->get(i)->getString(), "0.46");
        }
        for (auto i = 0; i < 100000; i++)
        {
            ASSERT_TRUE(bigV->get(i + 1000000)->isNull());
        }
    }
    else if (ex_type == "DECIMAL64")
    {
        for (auto i = 0; i < 1000000; i++)
        {
            ASSERT_EQ(bigV->get(i)->getString(), "-0.1300000000");
        }
        for (auto i = 0; i < 100000; i++)
        {
            ASSERT_TRUE(bigV->get(i + 1000000)->isNull());
        }
    }
    else if (ex_type == "BLOB")
    {
        for (auto i = 0; i < 1000000; i++)
        {
            ASSERT_EQ(bigV->get(i)->getString(), "blob");
        }
        for (auto i = 0; i < 100000; i++)
        {
            ASSERT_TRUE(bigV->get(i + 1000000)->isNull());
        }
    }

    conn.upload("res", {bigV});
    ASSERT_TRUE(conn.run("eqObj(res,x)")->getBool());
}

TEST_F(DataformVectorTest, test_nocapacity_symbolvector_appendString)
{
    int batchSize = 1000;
    std::string str = "a";
    dolphindb::VectorSP vec = dolphindb::Util::createVector(dolphindb::DT_SYMBOL, 0, batchSize);
    for (int i = 0; i < batchSize * 2; ++i)
    {
        vec->appendString(&str, 1);
    }
    int size = vec->size();
    ASSERT_EQ(vec->getType(), dolphindb::DT_SYMBOL);
    ASSERT_EQ(size, batchSize * 2);
    for(auto i =0; i<size; i++){
        ASSERT_EQ(vec->get(i)->getString(), str);
    }
}

TEST_F(DataformVectorTest, test_nocapacity_stringVector_appendString)
{
    int batchSize = 1000;
    std::string str = "a";
    dolphindb::VectorSP vec = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, batchSize);
    for (int i = 0; i < batchSize * 2; ++i)
    {
        vec->appendString(&str, 1);
    }
    int size = vec->size();
    ASSERT_EQ(vec->getType(), dolphindb::DT_STRING);
    ASSERT_EQ(size, batchSize * 2);
    for(auto i =0; i<size; i++){
        ASSERT_EQ(vec->get(i)->getString(), str);
    }
}

TEST_F(DataformVectorTest, test_nocapacity_blobvector_appendString)
{
    int batchSize = 1000;
    std::string str = "a";
    dolphindb::VectorSP vec = dolphindb::Util::createVector(dolphindb::DT_BLOB, 0, batchSize);
    for (int i = 0; i < batchSize * 2; ++i)
    {
        vec->appendString(&str, 1);
    }
    int size = vec->size();
    ASSERT_EQ(vec->getType(), dolphindb::DT_BLOB);
    ASSERT_EQ(size, batchSize * 2);
    for(auto i =0; i<size; i++){
        ASSERT_EQ(vec->get(i)->getString(), str);
    }
}

TEST_F(DataformVectorTest, test_upload_download_vector_with_huge_value_string)
{
    dolphindb::VectorSP v0 = conn.run("a = array(STRING).append!(string(concat(take(\"123&#@!^%;d《》中文\",100000))));a");
    std::string ex0 = "[\"";
    for (auto i = 0; i < 100000; i++){
        ex0 += "123&#@!^%;d《》中文";
    }
    ex0 += "\"]";
    ASSERT_EQ(v0->getString(), ex0);


    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, 1);
    std::string init = "1";
    std::string val;
    for (auto i = 0; i < 256 * 1024; i++){
        val += init;
    }
    v1->append(dolphindb::Util::createString(val));
    ASSERT_ANY_THROW(conn.upload("b", {v1}));

}

TEST_F(DataformVectorTest, test_upload_download_vector_with_huge_value_blob)
{
    dolphindb::VectorSP v0 = conn.run("a = array(BLOB).append!(string(concat(take(\"123&#@!^%;d《》中文\",100000))));a");
    std::string ex0 = "[\"";
    for (auto i = 0; i < 100000; i++){
        ex0 += "123&#@!^%;d《》中文";
    }
    ex0 += "\"]";
    ASSERT_EQ(v0->getString(), ex0);


    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_BLOB, 0, 1);
    std::string init = "1";
    std::string val;
    for (auto i = 0; i < 256 * 1024; i++){
        val += init;
    }
    v1->append(dolphindb::Util::createBlob(val));
    conn.upload("b", {v1});
    ASSERT_TRUE(conn.run("eqObj(b, blob([concat(take(`1, 256 * 1024))]))")->getBool());
}

TEST_F(DataformVectorTest, test_upload_download_vector_with_huge_value_symbol)
{
    dolphindb::VectorSP v0 = conn.run("a = array(SYMBOL).append!(string(concat(take(\"123&#@!^%;d《》中文\",100000))));a");
    std::string ex0 = "[\"";
    for (auto i = 0; i < 100000; i++){
        ex0 += "123&#@!^%;d《》中文";
    }
    ex0 += "\"]";
    ASSERT_EQ(v0->getString(), ex0);


    dolphindb::VectorSP v1 = dolphindb::Util::createVector(dolphindb::DT_SYMBOL, 0, 1);
    std::string init = "1";
    std::string val;
    for (auto i = 0; i < 256 * 1024; i++){
        val += init;
    }
    v1->append(dolphindb::Util::createString(val));

    ASSERT_ANY_THROW(conn.upload("b", {v1}));
}

class DataformVectorTest_Any : public DataformVectorTest, public testing::WithParamInterface<std::tuple<dolphindb::DATA_FORM, std::string>>{
    public:
        static std::vector<std::tuple<dolphindb::DATA_FORM, std::string>> getData(){
            return {
                std::make_tuple(dolphindb::DF_PAIR, "ex=take([1:2], 2000);ex"),
                std::make_tuple(dolphindb::DF_VECTOR, "ex=take([1..2], 2000);ex"),
                std::make_tuple(dolphindb::DF_SCALAR, "ex=take([1,`str], 2000);ex"),
                std::make_tuple(dolphindb::DF_DICTIONARY, "ex=take([dict(`a`b`c, 1 2 3)], 2000);ex"),
                std::make_tuple(dolphindb::DF_TABLE, "ex=take([table(1 2 3 as c1)], 2000);ex"),
                std::make_tuple(dolphindb::DF_MATRIX, "ex=take([matrix(1 2, 3 4)], 2000);ex"),
                std::make_tuple(dolphindb::DF_SET, "ex=take([set(1 2 3)], 2000);ex"),
            };
        }
};

INSTANTIATE_TEST_SUITE_P(, DataformVectorTest_Any, testing::ValuesIn(DataformVectorTest_Any::getData()),
        [](const ::testing::TestParamInfo<DataformVectorTest_Any::ParamType>& info){
            std::string _f = dolphindb::Util::getDataFormString(std::get<0>(info.param));
            return _f;
        });
TEST_P(DataformVectorTest_Any, testAnyVector_download_and_upload){
    dolphindb::DATA_FORM ex_df = std::get<0>(GetParam());
    std::string script = std::get<1>(GetParam());
    dolphindb::VectorSP anyV = conn.run(script);

    ASSERT_EQ(anyV->size(), 2000);
    for (auto i = 0; i < 2000; i++){
        ASSERT_EQ(anyV->get(i)->getForm(), ex_df);
    }

    conn.upload("res", {anyV});
    if (ex_df != dolphindb::DF_SET && ex_df != dolphindb::DF_TABLE && ex_df != dolphindb::DF_DICTIONARY){
        ASSERT_TRUE(conn.run("eqObj(res, ex)")->getBool());
    }else if (ex_df == dolphindb::DF_SET){
        conn.run("\
            for (i in 0:(ex.size()-1)){\
                assert (1 in res[i] && 2 in res[i] && 3 in res[i]);\
            }");
    }else if (ex_df == dolphindb::DF_TABLE){
        conn.run("\
            for (i in 0:(ex.size()-1)){\
                assert each(eqObj, ex[i].values(), res[i].values());\
            }");
    }else if (ex_df == dolphindb::DF_DICTIONARY){
        conn.run("\
            for (i in 0:(ex.size()-1)){\
                assert 1,each(eqObj, ex[i].values().sort(), res[i].values().sort());\
                assert 2,each(eqObj, ex[i].keys().sort(), res[i].keys().sort());\
            }");
    }
}

class DataformVectorTest_castTemporal : public DataformVectorTest, public testing::WithParamInterface<std::tuple<std::pair<dolphindb::DATA_TYPE, std::string>, dolphindb::DATA_TYPE>>{
public:
    static std::vector<std::pair<dolphindb::DATA_TYPE, std::string>> getData(){
        return {
            std::make_pair(dolphindb::DT_DATEHOUR, "x=array(DATEHOUR).append!(datehour(1..700)).append!(NULL);x"),
            std::make_pair(dolphindb::DT_DATE, "x=array(DATE).append!(date(1..700)).append!(NULL);x"),
            std::make_pair(dolphindb::DT_MONTH, "x=array(MONTH).append!(month(1..700)).append!(NULL);x"),
            std::make_pair(dolphindb::DT_TIME, "x=array(TIME).append!(time(1..700)).append!(NULL);x"),
            std::make_pair(dolphindb::DT_MINUTE, "x=array(MINUTE).append!(minute(1..700)).append!(NULL);x"),
            std::make_pair(dolphindb::DT_SECOND, "x=array(SECOND).append!(second(1..700)).append!(NULL);x"),
            std::make_pair(dolphindb::DT_DATETIME, "x=array(DATETIME).append!(datetime(1..700)).append!(NULL);x"),
            std::make_pair(dolphindb::DT_TIMESTAMP, "x=array(TIMESTAMP).append!(timestamp(1..700)).append!(NULL);x"),
            std::make_pair(dolphindb::DT_NANOTIME, "x=array(NANOTIME).append!(nanotime(1..700)).append!(NULL);x"),
            std::make_pair(dolphindb::DT_NANOTIMESTAMP, "x=array(NANOTIMESTAMP).append!(nanotimestamp(1..700)).append!(NULL);x"),
        };
    }
};

INSTANTIATE_TEST_SUITE_P(, DataformVectorTest_castTemporal, testing::Combine(
                            testing::ValuesIn(DataformVectorTest_castTemporal::getData()),
                            testing::Values(dolphindb::DT_DATE, dolphindb::DT_MONTH, dolphindb::DT_TIME, dolphindb::DT_MINUTE, dolphindb::DT_SECOND, dolphindb::DT_DATETIME, dolphindb::DT_TIMESTAMP, dolphindb::DT_NANOTIME, dolphindb::DT_NANOTIMESTAMP, dolphindb::DT_DATEHOUR, dolphindb::DT_BOOL, dolphindb::DT_STRING)),
                            [](const ::testing::TestParamInfo<DataformVectorTest_castTemporal::ParamType>& info){
                                std::string _f = dolphindb::Util::getDataTypeString(std::get<0>(info.param).first) + "_to_" + dolphindb::Util::getDataTypeString(std::get<1>(info.param));
                                return _f;
                            }
                        );
TEST_P(DataformVectorTest_castTemporal, test_vector_castTemporal){
    dolphindb::DATA_TYPE from_type = std::get<0>(GetParam()).first;
    std::string script = std::get<0>(GetParam()).second;
    dolphindb::DATA_TYPE to_type = std::get<1>(GetParam());
    try{
        dolphindb::VectorSP vec = conn.run(script);
        ASSERT_EQ(vec->getType(), from_type);
        dolphindb::VectorSP res_vec = vec->castTemporal(to_type);
        ASSERT_EQ(res_vec->getType(), to_type);
        conn.upload("res", {res_vec});
        ASSERT_TRUE(conn.run("ex = cast(x, "+dolphindb::Util::getDataTypeString(to_type)+");eqObj(res, ex)")->getBool());
    }catch(std::exception& e){
        ASSERT_EQ(e.what(), "castTemporal from "+ dolphindb::Util::getDataTypeString(from_type)+" to "+ dolphindb::Util::getDataTypeString(to_type)+" not supported ");
    }
}

TEST_F(DataformVectorTest, test_decimalvector_append_null){
    dolphindb::VectorSP dsmV = conn.run("decimal64(`0'1.123''-99999', 10)");
    #define reset_dsmV \
        dsmV = conn.run("decimal64(`0'1.123''-99999', 10)")

    std::string overflowStr = "111111111111111";
    char* overflowChar = new char[overflowStr.size()+1];
    strcpy(overflowChar, overflowStr.c_str());
    ASSERT_ANY_THROW(dsmV->appendString(&overflowStr, overflowStr.size()));
    ASSERT_ANY_THROW(dsmV->appendString(&overflowChar, strlen(overflowChar)));

    std::string nullStr = "";
    char* nullChar = new char[1];
    strcpy(nullChar, nullStr.c_str());
    char nullChar2 = CHAR_MIN;
    double nullDouble = dolphindb::DBL_NMIN;
    float nullFloat = dolphindb::FLT_NMIN;
    int nullInt = INT_MIN;
    long long nullLong = LLONG_MIN;
    short nullShort = SHRT_MIN;

    dsmV->appendString(&nullStr, 1);
    ASSERT_EQ(dsmV->getString(), "[0.0000000000,1.1230000000,-99999.0000000000,]");

    reset_dsmV;
    dsmV->appendString(&nullChar, 1);
    ASSERT_EQ(dsmV->getString(), "[0.0000000000,1.1230000000,-99999.0000000000,]");

    reset_dsmV;
    dsmV->appendBool(&nullChar2, 1);
    ASSERT_EQ(dsmV->getString(), "[0.0000000000,1.1230000000,-99999.0000000000,]");

    reset_dsmV;
    dsmV->appendChar(&nullChar2, 1);
    ASSERT_EQ(dsmV->getString(), "[0.0000000000,1.1230000000,-99999.0000000000,]");

    reset_dsmV;
    dsmV->appendDouble(&nullDouble, 1);
    ASSERT_EQ(dsmV->getString(), "[0.0000000000,1.1230000000,-99999.0000000000,]");

    reset_dsmV;
    dsmV->appendFloat(&nullFloat, 1);
    ASSERT_EQ(dsmV->getString(), "[0.0000000000,1.1230000000,-99999.0000000000,]");

    reset_dsmV;
    dsmV->appendInt(&nullInt, 1);
    ASSERT_EQ(dsmV->getString(), "[0.0000000000,1.1230000000,-99999.0000000000,]");

    reset_dsmV;
    dsmV->appendLong(&nullLong, 1);
    ASSERT_EQ(dsmV->getString(), "[0.0000000000,1.1230000000,-99999.0000000000,]");

    reset_dsmV;
    dsmV->appendShort(&nullShort, 1);
    ASSERT_EQ(dsmV->getString(), "[0.0000000000,1.1230000000,-99999.0000000000,]");

    delete[] overflowChar, nullChar;
}

TEST_F(DataformVectorTest, test_vector_func_set_null)
{
    dolphindb::VectorSP vec1 = conn.run("1 2 3");
    dolphindb::VectorSP vec2 = conn.run("4 5 6");
    ASSERT_FALSE(vec1->hasNull());
    ASSERT_FALSE(vec2->hasNull());

    dolphindb::VectorSP indV = dolphindb::Util::createIndexVector(1, 2);
    vec1->set(indV, dolphindb::Util::createNullConstant(dolphindb::DT_INT));
    ASSERT_TRUE(vec1->hasNull());
    ASSERT_EQ(vec1->getString(), "[1,,]");

    vec2->set(dolphindb::Util::createInt(0), dolphindb::Util::createNullConstant(dolphindb::DT_INT));
    ASSERT_TRUE(vec2->hasNull());
    ASSERT_EQ(vec2->getString(), "[,5,6]");
}

TEST_F(DataformVectorTest, test_stringVector_exception){
    std::string ss("123\0 123", 8);
    {
        try{
            dolphindb::VectorSP v = dolphindb::Util::createVector(dolphindb::DT_STRING, 0);
            v->appendString(&ss, 1);
        }catch(std::exception& e){
            ASSERT_EQ(std::string(e.what()), "A String cannot contain the character '\\0'");
        }
    }
    {
        try{
            std::vector<std::string> vv{ss};
            dolphindb::ConstantSP v = dolphindb::Util::createVector(dolphindb::DT_STRING, 1, 1, true, 0, &vv);
        }catch(std::exception& e){
            ASSERT_EQ(std::string(e.what()), "A String cannot contain the character '\\0'");
        }
    }
    {
        dolphindb::VectorSP v = dolphindb::Util::createVector(dolphindb::DT_STRING, 0);
        v->append(dolphindb::Util::createString("aaa"));
        try{
            v->setString(ss);
        }catch(std::exception& e){
            ASSERT_EQ(std::string(e.what()), "A String cannot contain the character '\\0'");
        }
        try{
            v->setString(0, ss);
        }catch(std::exception& e){
            ASSERT_EQ(std::string(e.what()), "A String cannot contain the character '\\0'");
        }
        try{
            v->setString(0, 1, &ss);
        }catch(std::exception& e){
            ASSERT_EQ(std::string(e.what()), "A String cannot contain the character '\\0'");
        }
        try{
            v->appendString(&ss, 1);
        }catch(std::exception& e){
            ASSERT_EQ(std::string(e.what()), "A String cannot contain the character '\\0'");
        }
    }
    {
        dolphindb::VectorSP v = dolphindb::Util::createVector(dolphindb::DT_SYMBOL, 0);
        v->append(dolphindb::Util::createString("aaa"));
        try{
            v->setString(ss);
        }catch(std::exception& e){
            ASSERT_EQ(std::string(e.what()), "A String cannot contain the character '\\0'");
        }
        try{
            v->setString(0, ss);
        }catch(std::exception& e){
            ASSERT_EQ(std::string(e.what()), "A String cannot contain the character '\\0'");
        }
        try{
            v->setString(0, 1, &ss);
        }catch(std::exception& e){
            ASSERT_EQ(std::string(e.what()), "A String cannot contain the character '\\0'");
        }
        try{
            v->appendString(&ss, 1);
        }catch(std::exception& e){
            ASSERT_EQ(std::string(e.what()), "A String cannot contain the character '\\0'");
        }
    }
}


TEST_F(DataformVectorTest, testCreateVector){
    bool s1 = false;
    char s2 = char(-128);
    int s3 = INT_MIN;
    short s4 = SHRT_MIN;
    long long s5 = LLONG_MIN;
    float s6 = FLT_MIN;
    double s7 = DBL_MIN;
    std::string str = "";
    std::string sym = "";
    std::string blob = "";
    unsigned char int128[16];
    unsigned char ip[16];
    unsigned char uuid[16];
    for(auto i=0;i<16;i++){
        int128[i] = CHAR_MIN;
        ip[i] = CHAR_MIN;
        uuid[i] = CHAR_MIN;
    }

    dolphindb::ConstantSP boolval = dolphindb::Util::createBool(s1);
    dolphindb::ConstantSP charval = dolphindb::Util::createChar(s2);
    dolphindb::ConstantSP intval = dolphindb::Util::createInt(s3);
    dolphindb::ConstantSP shortval = dolphindb::Util::createShort(s4);
    dolphindb::ConstantSP longval = dolphindb::Util::createLong(s5);
    dolphindb::ConstantSP floatval = dolphindb::Util::createFloat(s6);
    dolphindb::ConstantSP doubleval = dolphindb::Util::createDouble(s7);
    dolphindb::ConstantSP stringval = dolphindb::Util::createString(str);
    dolphindb::ConstantSP symbolval = dolphindb::Util::createString(sym);
    dolphindb::ConstantSP blobval = dolphindb::Util::createString(blob);
    dolphindb::ConstantSP int128val = dolphindb::Util::createConstant(dolphindb::DT_INT128);
    int128val->setBinary(int128, sizeof(dolphindb::Guid));
    dolphindb::ConstantSP ipval = dolphindb::Util::createConstant(dolphindb::DT_IP);
    ipval->setBinary(ip, sizeof(dolphindb::Guid));
    dolphindb::ConstantSP uuidval = dolphindb::Util::createConstant(dolphindb::DT_UUID);
    uuidval->setBinary(uuid, sizeof(dolphindb::Guid));
    dolphindb::ConstantSP dateval = dolphindb::Util::createDate(s3);
    dolphindb::ConstantSP monthval = dolphindb::Util::createMonth(s3);
    dolphindb::ConstantSP timeval = dolphindb::Util::createTime(s3);
    dolphindb::ConstantSP minuteval = dolphindb::Util::createMinute(s3);
    dolphindb::ConstantSP secondval = dolphindb::Util::createSecond(s3);
    dolphindb::ConstantSP datetimeval = dolphindb::Util::createDateTime(s3);
    dolphindb::ConstantSP timestampval = dolphindb::Util::createTimestamp(s5);
    dolphindb::ConstantSP nanotimeval = dolphindb::Util::createNanoTime(s5);
    dolphindb::ConstantSP nanotimestampval = dolphindb::Util::createNanoTimestamp(s5);
    dolphindb::VectorSP boolVec = dolphindb::Util::createVector(dolphindb::DT_BOOL, 0);
    boolVec->append(boolval);
    dolphindb::VectorSP charVec = dolphindb::Util::createVector(dolphindb::DT_CHAR, 0);
    charVec->append(charval);
    dolphindb::VectorSP intVec = dolphindb::Util::createVector(dolphindb::DT_INT, 0);
    intVec->append(intval);
    dolphindb::VectorSP shortVec = dolphindb::Util::createVector(dolphindb::DT_SHORT, 0);
    shortVec->append(shortval);
    dolphindb::VectorSP longVec = dolphindb::Util::createVector(dolphindb::DT_LONG, 0);
    longVec->append(longval);
    dolphindb::VectorSP floatVec = dolphindb::Util::createVector(dolphindb::DT_FLOAT, 0);
    floatVec->append(floatval);
    dolphindb::VectorSP doubleVec = dolphindb::Util::createVector(dolphindb::DT_DOUBLE, 0);
    doubleVec->append(doubleval);
    dolphindb::VectorSP stringVec = dolphindb::Util::createVector(dolphindb::DT_STRING, 0);
    stringVec->append(stringval);
    dolphindb::VectorSP symbolVec = dolphindb::Util::createVector(dolphindb::DT_SYMBOL, 0);
    symbolVec->append(symbolval);
    dolphindb::VectorSP blobVec = dolphindb::Util::createVector(dolphindb::DT_BLOB, 0);
    blobVec->append(blobval);
    dolphindb::VectorSP int128Vec = dolphindb::Util::createVector(dolphindb::DT_INT128, 0);
    int128Vec->append(int128val);
    dolphindb::VectorSP ipVec = dolphindb::Util::createVector(dolphindb::DT_IP, 0);
    ipVec->append(ipval);
    dolphindb::VectorSP uuidVec = dolphindb::Util::createVector(dolphindb::DT_UUID, 0);
    uuidVec->append(uuidval);
    dolphindb::VectorSP dateVec = dolphindb::Util::createVector(dolphindb::DT_DATE, 0);
    dateVec->append(dateval);
    dolphindb::VectorSP monthVec = dolphindb::Util::createVector(dolphindb::DT_MONTH, 0);
    monthVec->append(monthval);
    dolphindb::VectorSP timeVec = dolphindb::Util::createVector(dolphindb::DT_TIME, 0);
    timeVec->append(timeval);
    dolphindb::VectorSP minuteVec = dolphindb::Util::createVector(dolphindb::DT_MINUTE, 0);
    minuteVec->append(minuteval);
    dolphindb::VectorSP secondVec = dolphindb::Util::createVector(dolphindb::DT_SECOND, 0);
    secondVec->append(secondval);
    dolphindb::VectorSP datetimeVec = dolphindb::Util::createVector(dolphindb::DT_DATETIME, 0);
    datetimeVec->append(datetimeval);
    dolphindb::VectorSP timestampVec = dolphindb::Util::createVector(dolphindb::DT_TIMESTAMP, 0);
    timestampVec->append(timestampval);
    dolphindb::VectorSP nanotimeVec = dolphindb::Util::createVector(dolphindb::DT_NANOTIME, 0);
    nanotimeVec->append(nanotimeval);
    dolphindb::VectorSP nanotimestampVec = dolphindb::Util::createVector(dolphindb::DT_NANOTIMESTAMP, 0);
    nanotimestampVec->append(nanotimestampval);

    std::vector<dolphindb::ConstantSP> vals = {boolVec,charVec,intVec,shortVec,longVec,floatVec,doubleVec,stringVec,symbolVec,blobVec,int128Vec,ipVec,uuidVec,dateVec,monthVec,timeVec,minuteVec,secondVec,datetimeVec,timestampVec,nanotimeVec,nanotimestampVec};
    std::vector<std::string> names = {"boolVec","charVec","intVec","shortVec","longVec","floatVec","doubleVec","stringVec","symbolVec","blobVec","int128Vec","ipVec","uuidVec","dateVec","monthVec","timeVec","minuteVec","secondVec","datetimeVec","timestampVec","nanotimeVec","nanotimestampVec"};
    conn.upload(names,vals);

    ASSERT_TRUE(conn.run("eqObj(boolVec, bool("+boolVec->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(charVec, char("+charVec->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(intVec, int("+intVec->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(shortVec, short("+shortVec->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(longVec, long("+longVec->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(floatVec, float("+floatVec->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(doubleVec, double("+doubleVec->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(stringVec, string("+stringVec->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(symbolVec, symbol("+symbolVec->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(blobVec, blob("+blobVec->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(int128Vec, int128([`80808080808080808080808080808080]))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(ipVec, ipaddr(['8080:8080:8080:8080:8080:8080:8080:8080']))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(uuidVec, uuid(['80808080-8080-8080-8080-808080808080']))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(dateVec, date("+dateVec->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(monthVec, month("+monthVec->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(timeVec, time("+timeVec->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(minuteVec, minute("+minuteVec->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(secondVec, second("+secondVec->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(datetimeVec, datetime("+datetimeVec->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(timestampVec, timestamp("+timestampVec->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(nanotimeVec, nanotime("+nanotimeVec->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(nanotimestampVec, nanotimestamp("+nanotimestampVec->getString()+"))")->getBool());
}



class DataformVectorTest_download : public DataformVectorTest, public testing::WithParamInterface<std::tuple<std::string, std::string, dolphindb::DATA_TYPE>> 
{
public:
    static std::vector<std::tuple<std::string, std::string, dolphindb::DATA_TYPE>> getData(){
        return {
            std::make_tuple(std::string("x=true false NULL;x"), std::string("[1,0,]"), dolphindb::DT_BOOL),
            std::make_tuple(std::string("x=127c -128c NULL;x"), std::string("[127,,]"), dolphindb::DT_CHAR),
            std::make_tuple(std::string("x=2147483647 -2147483648 NULL;x"), std::string("[2147483647,,]"), dolphindb::DT_INT),
            std::make_tuple(std::string("x=32768h 32767h NULL;x"), std::string("[,32767,]"), dolphindb::DT_SHORT),
            std::make_tuple(std::string("x=9223372036854775807l NULL;x"), std::string("[9223372036854775807,]"), dolphindb::DT_LONG),
            std::make_tuple(std::string("x=3.14f NULL;x"), std::string("[3.14,]"), dolphindb::DT_FLOAT),
            std::make_tuple(std::string("x=3.1415926 NULL;x"), std::string("[3.141593,]"), dolphindb::DT_DOUBLE),
            std::make_tuple(std::string("x=`aaa`bbb`;x"), std::string("[\"aaa\",\"bbb\",]"), dolphindb::DT_STRING),
            std::make_tuple(std::string("x=symbol(`aaa`bbb`);x"), std::string("[\"aaa\",\"bbb\",]"), dolphindb::DT_SYMBOL),
            std::make_tuple(std::string("x=blob(`aaa`bbb`);x"), std::string("[\"aaa\",\"bbb\",]"), dolphindb::DT_BLOB),
            std::make_tuple(std::string("x=int128(`0123456789abcdef0123456789abcdef`);x"), std::string("[0123456789abcdef0123456789abcdef,]"), dolphindb::DT_INT128),
            std::make_tuple(std::string("x=ipaddr('192.168.1.1''');x"), std::string("[192.168.1.1,]"), dolphindb::DT_IP),
            std::make_tuple(std::string("x=uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87''');x"), std::string("[5d212a78-cc48-e3b1-4235-b4d91473ee87,]"), dolphindb::DT_UUID),
            std::make_tuple(std::string("x=2022.01.01 NULL;x"), std::string("[2022.01.01,]"), dolphindb::DT_DATE),
            std::make_tuple(std::string("x=2022.01M NULL;x"), std::string("[2022.01M,]"), dolphindb::DT_MONTH),
            std::make_tuple(std::string("x=13:30:00.000 NULL;x"), std::string("[13:30:00.000,]"), dolphindb::DT_TIME),
            std::make_tuple(std::string("x=23:59m NULL;x"), std::string("[23:59m,]"), dolphindb::DT_MINUTE),
            std::make_tuple(std::string("x=01:08:54 NULL;x"), std::string("[01:08:54,]"), dolphindb::DT_SECOND),
            std::make_tuple(std::string("x=2022.01.01T13:30:00 NULL;x"), std::string("[2022.01.01T13:30:00,]"), dolphindb::DT_DATETIME),
            std::make_tuple(std::string("x=2022.01.01T13:30:00.123 NULL;x"), std::string("[2022.01.01T13:30:00.123,]"), dolphindb::DT_TIMESTAMP),
            std::make_tuple(std::string("x=13:30:00.123456789 NULL;x"), std::string("[13:30:00.123456789,]"), dolphindb::DT_NANOTIME),
            std::make_tuple(std::string("x=2022.01.01T13:30:00.123456789 NULL;x"), std::string("[2022.01.01T13:30:00.123456789,]"), dolphindb::DT_NANOTIMESTAMP)
        };
    }
};
INSTANTIATE_TEST_SUITE_P(, DataformVectorTest_download, testing::ValuesIn(DataformVectorTest_download::getData()));

TEST_P(DataformVectorTest_download, test_download_vector) {
    std::string script = std::get<0>(GetParam());
    std::string ex = std::get<1>(GetParam());
    dolphindb::DATA_TYPE type = std::get<2>(GetParam());
    dolphindb::ConstantSP res = conn.run(script);
    ASSERT_EQ(res->getString(), ex);
    ASSERT_TRUE(res->isVector());
    ASSERT_EQ(res->getType(), type == dolphindb::DT_SYMBOL? dolphindb::DT_STRING : type);
    ASSERT_TRUE(res->hasNull());
    ASSERT_TRUE(res->isNull(res->size()-1));
}
