#ifdef __linux__

#include <gtest/gtest.h>
#include "config.h"
#include "ConstantMarshall.h"
#include "DFSChunkMeta.h"
#include "ConstantImp.h"
#include "Set.h"
#include "Format.h"
#include "ConstantFactory.h"

class MarshallTest:public testing::Test
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

dolphindb::DBConnection MarshallTest::conn(false, false);

TEST_F(MarshallTest, ScalarUnmarshallStart){
    conn.connect(HOST, PORT, USER, PASSWD);
    dolphindb::ConstantSP t = conn.run("def cacusum(mutable t){return t}; cacusum");
    ASSERT_EQ(t->getString(), "cacusum");
    conn.close();

    dolphindb::ConstantSP scalar = dolphindb::Util::createDecimal32(3, 100);
    dolphindb::DataOutputStreamSP outStream = new dolphindb::DataOutputStream(1024);
    dolphindb::ConstantMarshallSP marshall = dolphindb::ConstantMarshallFactory::getInstance(scalar->getForm(), outStream);
    dolphindb::IO_ERR ret;
    marshall->start(scalar, false, false, ret);

    std::string binary(outStream->getBuffer(), outStream->size());
    dolphindb::DataInputStreamSP inStream = new dolphindb::DataInputStream(binary.data(), 3);
    short flag;
    inStream->readShort(flag);
    dolphindb::DATA_FORM form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    auto unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));

    std::string copy = binary;
    int invalid = -1;
    memcpy(&copy[2], &invalid, 4);
    inStream = new dolphindb::DataInputStream(copy.data(), 6);
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
}

TEST_F(MarshallTest, ChunkUnmarshallStart){
    dolphindb::IO_ERR ret;
    std::vector<std::string> sites = {"192.168.0.16:9002:datanode1", "192.168.0.16:9003:datanode2", "192.168.0.16:9004:datanode3", "192.168.0.16:9005:datanode4"};
    dolphindb::Guid id("314");
    dolphindb::DFSChunkMetaSP chunk = new dolphindb::DFSChunkMeta("/home/appadmin/data", id, 3, 1, dolphindb::FILE_CHUNK, sites, 315);
    dolphindb::DataOutputStreamSP outStream = new dolphindb::DataOutputStream(1024);
    dolphindb::ConstantMarshallSP marshall = dolphindb::ConstantMarshallFactory::getInstance(chunk->getForm(), outStream);
    marshall->start(chunk, false, false, ret);
    marshall->reset();

    std::string binary(outStream->getBuffer(), outStream->size());
    dolphindb::DataInputStreamSP inStream = new dolphindb::DataInputStream(binary.data(), 3);
    short flag;
    inStream->readShort(flag);
    dolphindb::DATA_FORM form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    dolphindb::ConstantUnmarshallFactory unmarshallFactory(inStream);
    auto unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));

    std::string copy = binary;
    short invalid = -1;
    memcpy(&copy[2], &invalid, 2);
    inStream = new dolphindb::DataInputStream(copy.data(), 4);
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));

    copy = binary;
    invalid = 5000;
    memcpy(&copy[2], &invalid, 2);
    inStream = new dolphindb::DataInputStream(copy.data(), 4);
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));

    inStream = new dolphindb::DataInputStream(binary.data(), 6);
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));

    copy = binary;
    invalid = 1;
    memcpy(&copy[2], &invalid, 2);
    inStream = new dolphindb::DataInputStream(copy.data(), copy.size());
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));

    copy = binary;
    invalid = 45;
    memcpy(&copy[2], &invalid, 2);
    inStream = new dolphindb::DataInputStream(copy.data(), copy.size());
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));

    copy = binary;
    invalid = 46;
    memcpy(&copy[2], &invalid, 2);
    inStream = new dolphindb::DataInputStream(copy.data(), copy.size());
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));

    copy = binary;
    invalid = 165;
    memcpy(&copy[2], &invalid, 2);
    inStream = new dolphindb::DataInputStream(copy.data(), copy.size());
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
}
TEST_F(MarshallTest, DictionaryUnmarshallStart){
    dolphindb::IO_ERR ret;
    dolphindb::DictionarySP dict = dolphindb::Util::createDictionary(dolphindb::DT_INT, dolphindb::DT_INT);
    dict->set(dolphindb::Util::createInt(3), dolphindb::Util::createInt(14));
    dolphindb::DataOutputStreamSP outStream = new dolphindb::DataOutputStream(1024);
    dolphindb::ConstantMarshallSP marshall = dolphindb::ConstantMarshallFactory::getInstance(dolphindb::DF_CHART, outStream);
    marshall->start(dict, false, false, ret);

    std::string binary(outStream->getBuffer(), outStream->size());
    dolphindb::DataInputStreamSP inStream = new dolphindb::DataInputStream(binary.data(), 3);
    short flag;
    inStream->readShort(flag);
    dolphindb::DATA_FORM form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    auto unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));

    inStream = new dolphindb::DataInputStream(binary.data(), 5);
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(dolphindb::DF_CHART, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));

    inStream = new dolphindb::DataInputStream(binary.data(), 17);
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));

    inStream = new dolphindb::DataInputStream(binary.data(), 20);
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
}

TEST_F(MarshallTest, SetUnmarshallStart){
    dolphindb::IO_ERR ret;
    dolphindb::SetSP set = dolphindb::Util::createSet(dolphindb::DT_INT, 9);
    set->append(dolphindb::Util::createInt(9));
    dolphindb::DataOutputStreamSP outStream = new dolphindb::DataOutputStream(1024);
    dolphindb::ConstantMarshallSP marshall = dolphindb::ConstantMarshallFactory::getInstance(set->getForm(), outStream);
    marshall->start(set, false, false, ret);

    std::string binary(outStream->getBuffer(), outStream->size());
    dolphindb::DataInputStreamSP inStream = new dolphindb::DataInputStream(binary.data(), 3);
    short flag;
    inStream->readShort(flag);
    dolphindb::DATA_FORM form = static_cast<dolphindb::DATA_FORM>(flag >> 8);

    auto unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    //key std::vector read fail
    ASSERT_FALSE(unmarshall->start(flag, false, ret));

    inStream = new dolphindb::DataInputStream(binary.data(), 6);
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    //std::vector unmarshal fail
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
}

TEST_F(MarshallTest, TableUnmarshallStart){
    dolphindb::IO_ERR ret;
    std::vector<std::string> colNames{"id"};
    std::string specialName(5, 'c');
    colNames.push_back(specialName);
    colNames.push_back("name");
    std::vector<std::string> names{"Tom", "Bob", "Lucy"};
    long long* ids = new long long[3]{1, 2, 3};
    int* ages = new int[3]{22, 23, 24};
    dolphindb::VectorSP idVec = dolphindb::Util::createVector(dolphindb::DT_LONG, 3, 3, true, 0, ids);
    dolphindb::VectorSP nameVec = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, 3);
    nameVec->appendString(names.data(), names.size());
    dolphindb::VectorSP ageVec = dolphindb::Util::createVector(dolphindb::DT_INT, 3, 3, true, 0, ages);
    std::vector<dolphindb::ConstantSP> cols{idVec, nameVec, ageVec};
    dolphindb::TableSP table = dolphindb::Util::createTable(colNames, cols);
    dolphindb::DataOutputStreamSP outStream = new dolphindb::DataOutputStream(1024);
    dolphindb::ConstantMarshallSP marshall = dolphindb::ConstantMarshallFactory::getInstance(table->getForm(), outStream);
    marshall->start(table, false, false, ret);

    std::string binary(outStream->getBuffer(), outStream->size());
    dolphindb::DataInputStreamSP inStream = new dolphindb::DataInputStream(binary.data(), 3);
    short flag;
    inStream->readShort(flag);
    dolphindb::DATA_FORM form = static_cast<dolphindb::DATA_FORM>(flag >> 8);

    dolphindb::ConstantUnmarshallFactory unmarshallFactory(inStream);
    auto unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    //TableUnmarshall::start in_->readInt(rows_) return false
    ASSERT_FALSE(unmarshall->start(flag, false, ret));

    std::string copy = binary;
    int valid = -1;
    memcpy(&copy[2], &valid, 4);
    inStream = new dolphindb::DataInputStream(copy.data(), 10);
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    //TableUnmarshall::start rows_ < 0
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();
    
    copy = binary;
    inStream = new dolphindb::DataInputStream(copy.data(), 7);
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    //TableUnmarshall::start in_->readInt(columns_) return false
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();

    copy = binary;
    memcpy(&copy[6], &valid, 4);
    inStream = new dolphindb::DataInputStream(copy.data(), 10);
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    //TableUnmarshall::start columns_ < 0
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();

    copy = binary;
    inStream = new dolphindb::DataInputStream(copy.data(), 10);
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    //TableUnmarshall::start read table name fail
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();

    copy = binary;
    inStream = new dolphindb::DataInputStream(copy.data(), 11);
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    //TableUnmarshall::start read column name fail
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();

    copy = binary;
    inStream = new dolphindb::DataInputStream(copy.data(), 25);
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    //TableUnmarshall::start read column flag fail
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();

    copy = binary;
    inStream = new dolphindb::DataInputStream(copy.data(), 27);
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    //TableUnmarshall::start unmarshal column fail
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();
}

TEST_F(MarshallTest, MatrixUnmarshallStart){
    dolphindb::IO_ERR ret;
    int* pData = new int[9]{1, 2, 3, 4, 5, 6, 7, 8, 9};
    dolphindb::VectorSP matrix = dolphindb::Util::createMatrix(dolphindb::DT_INT, 3, 3, 9, 0, pData);
    dolphindb::VectorSP rowLabelVec = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, 3);
    dolphindb::VectorSP colLabelVec = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, 3);
    std::vector<std::string> rowData{"row1", "row2", "row3"};
    std::vector<std::string> colData{"col1", "col2", "col3"};
    rowLabelVec->appendString(rowData.data(), rowData.size());
    colLabelVec->appendString(colData.data(), colData.size());
    matrix->setRowLabel(rowLabelVec);
    matrix->setColumnLabel(colLabelVec);

    dolphindb::DataOutputStreamSP outStream = new dolphindb::DataOutputStream(1024);
    dolphindb::ConstantMarshallSP marshall = dolphindb::ConstantMarshallFactory::getInstance(matrix->getForm(), outStream);

    marshall->start(matrix, false, false, ret);
    marshall->reset();

    std::string binary(outStream->getBuffer(), outStream->size());
    dolphindb::DataInputStreamSP inStream = new dolphindb::DataInputStream(binary.data(), 2);
    
    short flag;
    inStream->readShort(flag);
    dolphindb::DATA_FORM form = static_cast<dolphindb::DATA_FORM>(flag >> 8);

    dolphindb::ConstantUnmarshallFactory unmarshallFactory(inStream);
    auto unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    //MatrixUnmarshall::start in_->readChar(labelFlag_) return false
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();

    std::string copy(binary);
    copy[2] = -1;
    inStream = new dolphindb::DataInputStream(copy.data(), 3);

    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    //MatrixUnmarshall::start labelFlag_ < 0
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();

    copy = binary;
    inStream = new dolphindb::DataInputStream(copy.data(), 4);
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    //MatrixUnmarshall::start labelFlag&1 in_->readShort(flag) return false
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();

    copy = binary;
    inStream = new dolphindb::DataInputStream(copy.data(), 5);
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    //MatrixUnmarshall::start labelFlag&1 vectorUnmarshall_.start return false
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();

    copy = binary;
    inStream = new dolphindb::DataInputStream(copy.data(), 29);
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    //MatrixUnmarshall::start labelFlag&2 in_->readShort(flag) return false
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();

    copy = binary;
    inStream = new dolphindb::DataInputStream(copy.data(), 30);
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    //MatrixUnmarshall::start labelFlag&2 vectorUnmarshall_.start return false
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();

    copy = binary;
    inStream = new dolphindb::DataInputStream(copy.data(), 54);
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    //MatrixUnmarshall::start in_->readShort(flag) return false
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();

    copy = binary;
    inStream = new dolphindb::DataInputStream(copy.data(), 57);
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    //MatrixUnmarshall::start vectorUnmarshall_.start return false
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();
}

TEST_F(MarshallTest, VectorUnmarshallStartSymbolbaseUnmarshallStart){
    //VectorUnmarshall::start symbaseUnmarshall_->start(blocking, ret) return false
    dolphindb::IO_ERR ret;
    dolphindb::VectorSP bv = dolphindb::Util::createVector(dolphindb::DT_SYMBOL, 0, 10);
    std::string con = "12345";
    bv->appendString(&con, 1);
    bv->appendString(&con, 1);
    bv->appendString(&con, 1);
    bv->appendString(&con, 1);
    bv->appendString(&con, 1);
    bv->appendString(&con, 1);
    bv->appendString(&con, 1);
    bv->appendString(&con, 1);
    std::cout << bv->getString() << std::endl;

    dolphindb::DataOutputStreamSP outStream = new dolphindb::DataOutputStream(1024);
    dolphindb::ConstantMarshallSP marshall = dolphindb::ConstantMarshallFactory::getInstance(bv->getForm(), outStream);
    marshall->start(bv, true, false, ret);

    std::string binary(outStream->getBuffer(), outStream->size());
    
    dolphindb::DataInputStreamSP inStream = new dolphindb::DataInputStream(binary.data(), 12);
    short flag;
    inStream->readShort(flag);
    dolphindb::DATA_FORM form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    auto unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();

    std::string copy = binary;
    int invalid = -1;
    mempcpy(&copy[10], &invalid, 4);
    inStream = new dolphindb::DataInputStream(copy.data(), 14);
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();

    inStream = new dolphindb::DataInputStream(binary.data(), 16);
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();

    inStream = new dolphindb::DataInputStream(binary.data(), 19);
    inStream->readShort(flag);
    form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();
}

TEST_F(MarshallTest, VectorUnmarshallObjDeserializeFail){
    dolphindb::IO_ERR ret;
    dolphindb::VectorSP bv = dolphindb::Util::createVector(dolphindb::DT_INT, 0, 1);
    bv->append(dolphindb::Util::createInt(1));
    std::cout << bv->getString() << std::endl;

    dolphindb::DataOutputStreamSP outStream = new dolphindb::DataOutputStream(1024);
    dolphindb::ConstantMarshallSP marshall = dolphindb::ConstantMarshallFactory::getInstance(bv->getForm(), outStream);
    marshall->start(bv, true, false, ret);
    std::string binary(outStream->getBuffer(), outStream->size());

    dolphindb::DataInputStreamSP inStream = new dolphindb::DataInputStream(binary.data(), 12);
    
    short flag;
    inStream->readShort(flag);
    dolphindb::DATA_FORM form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    auto unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();
}

TEST_F(MarshallTest, VectorUnmarshallStartReadScaleFail){
    //VectorUnmarshall::start symbaseUnmarshall_->start(blocking, ret) return false
    dolphindb::IO_ERR ret;
    dolphindb::VectorSP bv = dolphindb::Util::createVector(dolphindb::DT_DECIMAL32, 0, 10, true, 3);
    int con = 100;
    bv->appendInt(&con, 1);
    bv->appendInt(&con, 1);
    bv->appendInt(&con, 1);
    std::cout << bv->getString() << std::endl;

    dolphindb::DataOutputStreamSP outStream = new dolphindb::DataOutputStream(1024);
    dolphindb::ConstantMarshallSP marshall = dolphindb::ConstantMarshallFactory::getInstance(bv->getForm(), outStream);
    marshall->start(bv, true, false, ret);
    std::string binary(outStream->getBuffer(), outStream->size());
    
    dolphindb::DataInputStreamSP inStream = new dolphindb::DataInputStream(binary.data(), 12);
    short flag;
    inStream->readShort(flag);
    dolphindb::DATA_FORM form = static_cast<dolphindb::DATA_FORM>(flag >> 8);

    auto unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();

    //VectorUnmarshall::start scale_ < 0 return true
    std::string copy = binary;
    int invalid = -1;
    memcpy(&copy[10], &invalid, 4);
    inStream = new dolphindb::DataInputStream(copy.data(), 14);
    inStream->readShort(flag);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();

    //VectorUnmarshall::start form is invalid
    copy = binary;
    inStream = new dolphindb::DataInputStream(copy.data(), copy.size());
    inStream->readShort(flag);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    flag = flag & 0x00ff;
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();
}

TEST_F(MarshallTest, VectorUnmarshallStartObjIsNull){
    dolphindb::IO_ERR ret;
    dolphindb::VectorSP bv = dolphindb::Util::createPair(dolphindb::DT_INT);
    bv->setInt(0, 1);
    bv->setInt(1, 2);
    std::cout << bv->getString() << std::endl;

    dolphindb::DataOutputStreamSP outStream = new dolphindb::DataOutputStream(1024);
    dolphindb::ConstantMarshallSP marshall = dolphindb::ConstantMarshallFactory::getInstance(bv->getForm(), outStream);
    marshall->start(bv, true, false, ret);

    std::string binary(outStream->getBuffer(), outStream->size());
    dolphindb::DataInputStreamSP inStream = new dolphindb::DataInputStream(binary.data(), binary.size());
    short flag;
    inStream->readShort(flag);
    dolphindb::DATA_FORM form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    //SET TYPE TO dolphindb::DT_ANY
    flag = flag & 0xff00;
    flag = flag | 0x0019;

    auto unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();
}

TEST_F(MarshallTest, VectorUnmarshallAnyVectorSpecialCase){
dolphindb::IO_ERR ret;
    dolphindb::VectorSP bv = dolphindb::Util::createVector(dolphindb::DT_ANY, 0, 1);
    bv->append(dolphindb::Util::createInt(1));
    std::cout << bv->getString() << std::endl;

    dolphindb::DataOutputStreamSP outStream = new dolphindb::DataOutputStream(1024);
    dolphindb::ConstantMarshallSP marshall = dolphindb::ConstantMarshallFactory::getInstance(bv->getForm(), outStream);
    marshall->start(bv, true, false, ret);
    std::string binary(outStream->getBuffer(), outStream->size());

    std::string copy = binary;
    short invalidFlag = 0x0904;
    memcpy(&copy[10], &invalidFlag, 2);
    dolphindb::DataInputStreamSP inStream = new dolphindb::DataInputStream(copy.data(), copy.size());
    short flag;
    inStream->readShort(flag);
    dolphindb::DATA_FORM form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    auto unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();

    copy = binary;
    inStream = new dolphindb::DataInputStream(copy.data(), 14, false);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    inStream->reset(14);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();

    copy = binary;
    inStream = new dolphindb::DataInputStream(copy.data(), 11);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();
}

TEST_F(MarshallTest, VectorMarshalStart){
    dolphindb::IO_ERR ret;
    dolphindb::VectorSP bv = dolphindb::Util::createVector(dolphindb::DT_BLOB, 0, 10);
    std::string con = "12345";
    bv->appendString(&con, 1);
    bv->appendString(&con, 1);
    bv->appendString(&con, 1);
    std::cout << bv->getString() << std::endl;

    dolphindb::DataOutputStreamSP outStream = new dolphindb::DataOutputStream(1024);
    dolphindb::ConstantMarshallSP marshall = dolphindb::ConstantMarshallFactory::getInstance(bv->getForm(), outStream);
    marshall->start(bv, true, false, ret);

    std::string binary(outStream->getBuffer(), outStream->size());
    //VectorUnmarshall::start input->readInt(rows_) return false
    dolphindb::DataInputStreamSP inStream = new dolphindb::DataInputStream(binary.data(), 4);
    short flag;
    inStream->readShort(flag);
    dolphindb::DATA_FORM form = static_cast<dolphindb::DATA_FORM>(flag >> 8);

    auto unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();

    //VectorUnmarshall::start rows_ < 0 return true
    std::string copy = binary;
    int invalid = -1;
    memcpy(&copy[2], &invalid, 4);
    inStream = new dolphindb::DataInputStream(copy.data(), 6);
    inStream->readShort(flag);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();

    //VectorUnmarshall::start input->readInt(columns_) return false
    copy = binary;
    inStream = new dolphindb::DataInputStream(copy.data(), 8);
    inStream->readShort(flag);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();

    //VectorUnmarshall::start columns_ < 0 return true
    copy = binary;
    memcpy(&copy[6], &invalid, 4);
    inStream = new dolphindb::DataInputStream(copy.data(), 10);
    inStream->readShort(flag);
    unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_FALSE(unmarshall->start(flag, false, ret));
    unmarshall->reset();
}

TEST_F(MarshallTest, SymbolBaseMarshallStart){
    dolphindb::IO_ERR ret;
    dolphindb::SymbolBaseSP base = new dolphindb::SymbolBase(3);
    base->findAndInsert("314");
    base->findAndInsert("315");
    dolphindb::DataOutputStreamSP outStream = new dolphindb::DataOutputStream(1024);
    dolphindb::SymbolBaseMarshallSP marshall = new dolphindb::SymbolBaseMarshall(outStream);
    ASSERT_TRUE(marshall->start(base, false, ret));

    dolphindb::DataInputStreamSP inStream = new dolphindb::DataInputStream(outStream->getBuffer(), outStream->size());
    dolphindb::SymbolBaseUnmarshallSP unmarshall = new dolphindb::SymbolBaseUnmarshall(inStream);
    ASSERT_TRUE(unmarshall->start(false, ret));
    auto result = unmarshall->getSymbolBase();
    ASSERT_EQ(result->find("314"), 1);
    ASSERT_EQ(result->find("315"), 2);
}

TEST_F(MarshallTest, ChunkMarshallStart){
    dolphindb::IO_ERR ret;
    std::vector<std::string> sites = {"192.168.0.16:9002:datanode1", "192.168.0.16:9003:datanode2", "192.168.0.16:9004:datanode3", "192.168.0.16:9005:datanode4"};
    dolphindb::Guid id("314");
    dolphindb::DFSChunkMetaSP chunk = new dolphindb::DFSChunkMeta("/home/appadmin/data", id, 3, 1, dolphindb::FILE_CHUNK, sites, 315);
    std::string sendHeader("header\n");

    dolphindb::DataOutputStreamSP outStream = new dolphindb::DataOutputStream(1024);
    dolphindb::ConstantMarshallSP marshall = dolphindb::ConstantMarshallFactory::getInstance(chunk->getForm(), outStream);

    ASSERT_FALSE(marshall->start(nullptr, 1025, chunk, true, false, ret));
    ASSERT_TRUE(marshall->start(sendHeader.c_str(), sendHeader.size(), chunk, false, false, ret));
    marshall->reset();

    dolphindb::DataInputStreamSP inStream = new dolphindb::DataInputStream(outStream->getBuffer(), outStream->size());
    std::string readHeader;
    inStream->readLine(readHeader);
    ASSERT_EQ(readHeader, "header");

    short flag;
    inStream->readShort(flag);
    dolphindb::DATA_FORM form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    dolphindb::DATA_TYPE type = static_cast<dolphindb::DATA_TYPE >(flag & 0xff);

    dolphindb::ConstantUnmarshallFactory unmarshallFactory(inStream);
    auto unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    ASSERT_TRUE(unmarshall->start(flag, false, ret));
    dolphindb::ConstantSP result = unmarshall->getConstant();
    ASSERT_EQ(chunk->getString(), result->getString());
    unmarshall->reset();
}

TEST_F(MarshallTest, DictionaryMarshallStart){
    dolphindb::IO_ERR ret;
    dolphindb::DictionarySP dict = dolphindb::Util::createDictionary(dolphindb::DT_INT, dolphindb::DT_INT);
    dict->set(dolphindb::Util::createInt(3), dolphindb::Util::createInt(14));

    dolphindb::DataOutputStreamSP outStream = new dolphindb::DataOutputStream(1024);
    dolphindb::ConstantMarshallSP marshall = dolphindb::ConstantMarshallFactory::getInstance(dict->getForm(), outStream);

    ASSERT_FALSE(marshall->start(nullptr, 1025, dict, true, false, ret));
    ASSERT_TRUE(marshall->start(dict, false, false, ret));

    dolphindb::DataInputStreamSP inStream = new dolphindb::DataInputStream(outStream->getBuffer(), outStream->size());
    short flag;
    inStream->readShort(flag);
    dolphindb::DATA_FORM form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    dolphindb::DATA_TYPE type = static_cast<dolphindb::DATA_TYPE >(flag & 0xff);

    auto unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    unmarshall->start(flag, false, ret);
    dolphindb::ConstantSP result = unmarshall->getConstant();
    ASSERT_EQ(dict->getString(), result->getString());
}

TEST_F(MarshallTest, SetMarshallStart){
    dolphindb::IO_ERR ret;
    dolphindb::SetSP set = dolphindb::Util::createSet(dolphindb::DT_INT, 9);
    set->append(dolphindb::Util::createInt(9));

    dolphindb::DataOutputStreamSP outStream = new dolphindb::DataOutputStream(1024);
    dolphindb::ConstantMarshallSP marshall = dolphindb::ConstantMarshallFactory::getInstance(set->getForm(), outStream);

    ASSERT_FALSE(marshall->start(nullptr, 1025, set, true, false, ret));
    ASSERT_TRUE(marshall->start(set, false, false, ret));

    dolphindb::DataInputStreamSP inStream = new dolphindb::DataInputStream(outStream->getBuffer(), outStream->size());
    short flag;
    inStream->readShort(flag);
    dolphindb::DATA_FORM form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    dolphindb::DATA_TYPE type = static_cast<dolphindb::DATA_TYPE >(flag & 0xff);

    auto unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    unmarshall->start(flag, false, ret);
    dolphindb::ConstantSP result = unmarshall->getConstant();
    ASSERT_EQ(set->getString(), result->getString());
}

TEST_F(MarshallTest, TableMarshallStart){
    dolphindb::IO_ERR ret;
    std::vector<std::string> colNames{"id"};
    std::string specialName(5000, 'c');
    colNames.push_back(specialName);
    colNames.push_back("name");
    std::vector<std::string> names{"Tom", "Bob", "Lucy"};
    long long* ids = new long long[3]{1, 2, 3};
    int* ages = new int[3]{22, 23, 24};
    dolphindb::VectorSP idVec = dolphindb::Util::createVector(dolphindb::DT_LONG, 3, 3, true, 0, ids);
    dolphindb::VectorSP nameVec = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, 3);
    nameVec->appendString(names.data(), names.size());
    dolphindb::VectorSP ageVec = dolphindb::Util::createVector(dolphindb::DT_INT, 3, 3, true, 0, ages);
    std::vector<dolphindb::ConstantSP> cols{idVec, nameVec, ageVec};
    dolphindb::TableSP table = dolphindb::Util::createTable(colNames, cols);

    dolphindb::DataOutputStreamSP outStream = new dolphindb::DataOutputStream(1024);
    dolphindb::ConstantMarshallSP marshall = dolphindb::ConstantMarshallFactory::getInstance(table->getForm(), outStream);

    ASSERT_FALSE(marshall->start(nullptr, 1025, table, true, false, ret));
    ASSERT_TRUE(marshall->start(table, false, false, ret));

    dolphindb::DataInputStreamSP inStream = new dolphindb::DataInputStream(outStream->getBuffer(), outStream->size());
    short flag;
    inStream->readShort(flag);
    dolphindb::DATA_FORM form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    dolphindb::DATA_TYPE type = static_cast<dolphindb::DATA_TYPE >(flag & 0xff);

    dolphindb::ConstantUnmarshallFactory unmarshallFactory(inStream);
    auto unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    unmarshall->start(flag, false, ret);
    dolphindb::ConstantSP result = unmarshall->getConstant();
    ASSERT_EQ(table->getString(), result->getString());
}


TEST_F(MarshallTest, MatrixMarshallStart){
    dolphindb::IO_ERR ret;
    int* pData = new int[9]{1, 2, 3, 4, 5, 6, 7, 8, 9};
    dolphindb::VectorSP matrix = dolphindb::Util::createMatrix(dolphindb::DT_INT, 3, 3, 9, 0, pData);
    dolphindb::VectorSP rowLabelVec = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, 3);
    dolphindb::VectorSP colLabelVec = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, 3);
    std::vector<std::string> rowData{"row1", "row2", "row3"};
    std::vector<std::string> colData{"col1", "col2", "col3"};
    rowLabelVec->appendString(rowData.data(), rowData.size());
    colLabelVec->appendString(colData.data(), colData.size());
    matrix->setRowLabel(rowLabelVec);
    matrix->setColumnLabel(colLabelVec);

    dolphindb::DataOutputStreamSP outStream = new dolphindb::DataOutputStream(1024);
    dolphindb::ConstantMarshallFactory factory(outStream);
    ASSERT_EQ(factory.getConstantMarshall((dolphindb::DATA_FORM)-1), nullptr);
    ASSERT_EQ(factory.getConstantMarshall((dolphindb::DATA_FORM)9), nullptr);
    dolphindb::ConstantMarshallSP marshall = dolphindb::ConstantMarshallFactory::getInstance(matrix->getForm(), outStream);

    ASSERT_FALSE(marshall->start(nullptr, 1025, matrix, true, false, ret));
    ASSERT_TRUE(marshall->start(matrix, false, false, ret));
    marshall->reset();

    dolphindb::DataInputStreamSP inStream = new dolphindb::DataInputStream(outStream->getBuffer(), outStream->size());
    short flag;
    inStream->readShort(flag);
    dolphindb::DATA_FORM form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    dolphindb::DATA_TYPE type = static_cast<dolphindb::DATA_TYPE >(flag & 0xff);

    dolphindb::ConstantUnmarshallFactory unmarshallFactory(inStream);
    ASSERT_EQ(unmarshallFactory.getConstantUnmarshall((dolphindb::DATA_FORM)-1), nullptr);
    ASSERT_EQ(unmarshallFactory.getConstantUnmarshall((dolphindb::DATA_FORM)9), nullptr);
    auto unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    unmarshall->start(flag, false, ret);
    dolphindb::ConstantSP result = unmarshall->getConstant();
    ASSERT_EQ(matrix->getString(), result->getString());
}

TEST_F(MarshallTest, PairMarshallStart){
    dolphindb::IO_ERR ret;
    dolphindb::VectorSP vec = dolphindb::Util::createVector(dolphindb::DT_INT, 0, 100);
    std::vector<int> data{314, 315, 316};
    vec->appendInt(data.data(), data.size());

    dolphindb::DataOutputStreamSP outStream = new dolphindb::DataOutputStream(1024);
    dolphindb::ConstantMarshallSP vectorMarshall = dolphindb::ConstantMarshallFactory::getInstance(dolphindb::DF_PAIR, outStream);

    dynamic_cast<dolphindb::VectorMarshall*>(vectorMarshall.get())->setCompressMethod(dolphindb::COMPRESS_LZ4);

    ASSERT_TRUE(vectorMarshall->start(vec, false, true, ret));

    dolphindb::DataInputStreamSP inStream = new dolphindb::DataInputStream(outStream->getBuffer(), outStream->size());

    short flag;
    inStream->readShort(flag);
    dolphindb::DATA_FORM form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    dolphindb::DATA_TYPE type = static_cast<dolphindb::DATA_TYPE >(flag & 0xff);

    dolphindb::ConstantUnmarshallSP unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(dolphindb::DF_PAIR, inStream);
    unmarshall->start(flag, false, ret);
    dolphindb::ConstantSP result = unmarshall->getConstant();
    ASSERT_EQ(result->getString(), vec->getString());
}

TEST_F(MarshallTest, SymbolVectorMarshallStart){
    dolphindb::IO_ERR ret;
    dolphindb::VectorSP symbolVec = dolphindb::Util::createVector(dolphindb::DT_SYMBOL, 0, 100);
    std::vector<std::string> data{"314", "315", "316"};
    symbolVec->appendString(data.data(), data.size());

    dolphindb::DataOutputStreamSP outStream = new dolphindb::DataOutputStream(1024);
    dolphindb::ConstantMarshallSP vectorMarshall = dolphindb::ConstantMarshallFactory::getInstance(dolphindb::DF_VECTOR, outStream);

    dynamic_cast<dolphindb::VectorMarshall*>(vectorMarshall.get())->setCompressMethod(dolphindb::COMPRESS_LZ4);
    std::string sendHeader("header\n");

    ASSERT_FALSE(vectorMarshall->start(nullptr, 1025, symbolVec, true, false, ret));
    ASSERT_TRUE(vectorMarshall->start(sendHeader.c_str(), sendHeader.size(), symbolVec, false, true, ret));

    dolphindb::DataInputStreamSP inStream = new dolphindb::DataInputStream(outStream->getBuffer(), outStream->size());
    std::string readHeader;
    inStream->readLine(readHeader);
    ASSERT_EQ(readHeader, "header");

    short flag;
    inStream->readShort(flag);
    dolphindb::DATA_FORM form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    dolphindb::DATA_TYPE type = static_cast<dolphindb::DATA_TYPE >(flag & 0xff);

    dolphindb::ConstantUnmarshallSP unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    unmarshall->start(flag, false, ret);
    dolphindb::ConstantSP result = unmarshall->getConstant();
    ASSERT_EQ(result->getString(), symbolVec->getString());
}

TEST_F(MarshallTest,MarshalFlush){
    dolphindb::ConstantSP scalar = dolphindb::Util::createInt(314);
    dolphindb::DataOutputStreamSP outStream = new dolphindb::DataOutputStream(1024);
    dolphindb::ConstantMarshallSP emptyMarshall = dolphindb::ConstantMarshallFactory::getInstance((dolphindb::DATA_FORM)-1, outStream);
    ASSERT_EQ(emptyMarshall.isNull(), true);
    dolphindb::ConstantMarshallSP marshall = dolphindb::ConstantMarshallFactory::getInstance(scalar->getForm(), outStream);
    
    std::string sendHeader("header\n");

    dolphindb::IO_ERR ret;
    ASSERT_FALSE(marshall->start(nullptr, 1025, scalar, true, false, ret));
    ASSERT_TRUE(marshall->start(sendHeader.c_str(), sendHeader.size(), scalar, false, false, ret));
    marshall->flush();

    dolphindb::DataInputStreamSP inStream = new dolphindb::DataInputStream(outStream->getBuffer(), outStream->size());
    std::string readHeader;
    inStream->readLine(readHeader);
    ASSERT_EQ(readHeader, "header");

    short flag;
    inStream->readShort(flag);
    dolphindb::DATA_FORM form = static_cast<dolphindb::DATA_FORM>(flag >> 8);
    dolphindb::DATA_TYPE type = static_cast<dolphindb::DATA_TYPE >(flag & 0xff);

    auto emptyUnmarshall = dolphindb::ConstantUnmarshallFactory::getInstance((dolphindb::DATA_FORM)-1, inStream);
    ASSERT_EQ(emptyUnmarshall.isNull(), true);
    auto unmarshall = dolphindb::ConstantUnmarshallFactory::getInstance(form, inStream);
    unmarshall->start(flag, false, ret);
    dolphindb::ConstantSP result = unmarshall->getConstant();
    ASSERT_EQ(result->getInt(), 314);
}

TEST_F(MarshallTest, StringVectorDeserialize_OriginSizeSmaller){
    char buf[MARSHALL_BUFFER_SIZE];
    
    dolphindb::VectorSP vec = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, 9);
    std::vector<std::string> names{"Tom", "Bob", "Lucy"};
    vec->appendString(names.data(), names.size());
    int num = 0;
    int partial = 0;

    ASSERT_EQ(-1, vec->serialize(buf, MARSHALL_BUFFER_SIZE, 3, 0, num, partial));
    int len = vec->serialize(buf, MARSHALL_BUFFER_SIZE, 0, 0, num, partial);
    dolphindb::DataInputStreamSP inStream = new dolphindb::DataInputStream(buf, len);
    dolphindb::VectorSP result = dolphindb::Util::createVector(dolphindb::DT_STRING, 1, 9);
    result->deserialize(inStream.get(), 0, 3, num);
    ASSERT_EQ(result->getString(), vec->getString());
}

TEST_F(MarshallTest, StringVectorTrim_ContainNull){
    dolphindb::VectorSP vec = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, 3);
    std::vector<std::string> names{"   ", "Bol", "Lucy"};
    vec->appendString(names.data(), names.size());

    dolphindb::ConstantSP emptyStr = dolphindb::Util::createString("");
    vec->set(1, emptyStr);
    vec->trim();
    ASSERT_EQ(vec->get(0)->getString(), "");
}

TEST_F(MarshallTest, StringVectorStrip_ContainNull){
    dolphindb::VectorSP vec = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, 3);
    std::vector<std::string> names{"Lily \r\n", "Bol", "Lucy"};
    vec->appendString(names.data(), names.size());

    dolphindb::ConstantSP emptyStr = dolphindb::Util::createString("");
    vec->set(1, emptyStr);
    vec->strip();
    ASSERT_EQ(vec->get(0)->getString(), "Lily");
}

TEST_F(MarshallTest, StringVectorSet_NOT_LITERAL){
    dolphindb::VectorSP vec = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, 3);
    std::vector<std::string> names{"Lily", "Bob", "Lucy"};
    vec->appendString(names.data(), names.size());

    dolphindb::VectorSP vec2 = dolphindb::Util::createVector(dolphindb::DT_INT, 0, 2);
    std::vector<int> names2{314, 315};
    vec2->appendInt(names2.data(), names2.size());

    dolphindb::VectorSP indexVec = dolphindb::Util::createIndexVector(0, 2);
    vec->set(indexVec, vec2);
    ASSERT_EQ(vec->get(0)->getString(), "314");
    ASSERT_EQ(vec->get(1)->getString(), "315");
}

TEST_F(MarshallTest, StringVectorGet_INDEX_LARGERTHAN_SIZE){
    dolphindb::VectorSP vec = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, 3);
    std::vector<std::string> names{"Lily", "Bob", "Lucy"};
    vec->appendString(names.data(), names.size());

    dolphindb::VectorSP indexVec = dolphindb::Util::createIndexVector(0, 4);
    auto result = dynamic_cast<dolphindb::StringVector*>(vec.get())->get(indexVec);
    ASSERT_EQ(result->get(3)->getString(), "");
}

TEST_F(MarshallTest, StringVectorGet_NOT_INDEXARRAY){
    dolphindb::VectorSP vec = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, 3);
    std::vector<std::string> names{"Lily", "Bob", "Lucy"};
    vec->appendString(names.data(), names.size());
    dolphindb::ConstantSP emptyStr = dolphindb::Util::createString("");
    vec->set(1, emptyStr);

    dolphindb::VectorSP indexVec = dolphindb::Util::createVector(dolphindb::DT_DOUBLE, 0, 4);
    std::vector<double> index{0, 1, 2, 3};
    indexVec->appendDouble(index.data(), index.size());
 
    auto result = dynamic_cast<dolphindb::StringVector*>(vec.get())->get(indexVec);
    ASSERT_EQ(result->get(3)->getString(), "");
}

TEST_F(MarshallTest, StringVectorAppend_NOT_LITERAL){
    dolphindb::VectorSP vec = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, 3);
    std::vector<std::string> names{"Lily", "Bob", "Lucy"};
    vec->appendString(names.data(), names.size());

    dolphindb::VectorSP vec2 = dolphindb::Util::createVector(dolphindb::DT_INT, 0, 2);
    std::vector<int> names2{314, 315};
    vec2->appendInt(names2.data(), names2.size());

    vec->append(vec2, 2);
    ASSERT_EQ(vec->get(3)->getString(), "314");
    ASSERT_EQ(vec->get(4)->getString(), "315");
}

TEST_F(MarshallTest, StringVectorAppendString_LESSTHAN_CAPACITY){
    dolphindb::VectorSP vec = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, 9);
    std::vector<std::string> names{"Lily", "Bob", "Lucy"};
    vec->appendString(names.data(), names.size());

    char str1[] = "hello";
    char str2[] = "hi";
    char* str[] = {str1, str2};

    vec->appendString(str, 2);
    ASSERT_EQ(vec->get(3)->getString(), "hello");
    ASSERT_EQ(vec->get(4)->getString(), "hi");
}

TEST_F(MarshallTest, StringVectorRemove_NOINDEXARRAY){
    dolphindb::VectorSP vec = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, 9);
    std::vector<std::string> names{"Lily", "Bob", "Lucy"};
    vec->appendString(names.data(), names.size());
    dolphindb::ConstantSP emptyStr = dolphindb::Util::createString("");
    vec->set(1, emptyStr);

    dolphindb::VectorSP indexVec = dolphindb::Util::createVector(dolphindb::DT_DOUBLE, 0, 2);
    std::vector<double> index{0, 1};
    indexVec->appendDouble(index.data(), index.size());
    ASSERT_FALSE(vec->remove(indexVec));
    
    indexVec = dolphindb::Util::createVector(dolphindb::DT_INT, 0, 2);
    std::vector<int> ind{0, 2};
    indexVec->appendInt(ind.data(), ind.size());

    ASSERT_TRUE(vec->remove(indexVec));
    ASSERT_EQ(vec->size(), 1);
    ASSERT_EQ(vec->get(0)->getString(), "");
}

TEST_F(MarshallTest, StringVectorFill_LENGTH_NOTEQUAL_SIZE){
    dolphindb::VectorSP vec = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, 9);
    std::vector<std::string> names{"Lily", "Bob", "Lucy", "Tom", "Riddle"};
    vec->appendString(names.data(), names.size());

    dolphindb::VectorSP vec2 = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, 9);
    std::vector<std::string> names2{"Harry", "Potter", "Ron"};
    vec2->appendString(names2.data(), names2.size());

    vec->fill(0, 2, vec2);
    std::cout << vec->getString() << std::endl;
    ASSERT_EQ(vec->get(0)->getString(), "Harry");

    dolphindb::VectorSP vec3 = dolphindb::Util::createVector(dolphindb::DT_DOUBLE, 0, 9);
    std::vector<double> names3{314, 315};
    vec3->appendDouble(names3.data(), names3.size());

    vec->fill(2, 2, vec3);
    std::cout << vec->getString() << std::endl;
    ASSERT_EQ(vec->getString(2), "314");
}

TEST_F(MarshallTest, StringVectorGetString_String){
    dolphindb::VectorSP vec = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, 9);
    std::vector<std::string> names{"Lily", "Bob", "Lucy", "Tom", "Riddle"};
    vec->appendString(names.data(), names.size());

    std::string* strs[5]{nullptr};
    dynamic_cast<dolphindb::StringVector*>(vec.get())->getString(0, 5, strs);
    ASSERT_EQ(*strs[0], "Lily");
    ASSERT_EQ(*strs[4], "Riddle");
}

TEST_F(MarshallTest, StringVectorGetString_Char){
    dolphindb::VectorSP vec = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, 9);
    std::vector<std::string> names{"Lily", "Bob", "Lucy", "Tom", "Riddle"};
    vec->appendString(names.data(), names.size());

    char* strs[5]{nullptr};
    dynamic_cast<dolphindb::StringVector*>(vec.get())->getString(0, 5, strs);
    ASSERT_EQ(std::string(strs[0]), "Lily");
    ASSERT_EQ(std::string(strs[4]), "Riddle");
}

TEST_F(MarshallTest, StringVectorGetGetAllocatedMemory){
    dolphindb::ConstantSP vec = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, 9);
    ASSERT_GE(vec->getAllocatedMemory(), 64);
    ASSERT_GE(dynamic_cast<dolphindb::StringVector*>(vec.get())->getAllocatedMemory(0), 64);
    std::vector<std::string> names{"Lily", "Bob", "Lucy", "Tom", "Riddle"};
    dynamic_cast<dolphindb::StringVector*>(vec.get())->appendString(names.data(), names.size());
    ASSERT_GE(vec->getAllocatedMemory(), 129);
}

TEST_F(MarshallTest, AnyVectorAssign_Fail){
    dolphindb::VectorSP vec = dolphindb::Util::createVector(dolphindb::DT_ANY, 0, 9);
    dolphindb::ConstantSP element1 = dolphindb::Util::createInt(0);
    dolphindb::ConstantSP element2 = dolphindb::Util::createDouble(2);
    vec->append(element1);
    vec->append(element2);

    dolphindb::VectorSP vec2 = dolphindb::Util::createVector(dolphindb::DT_ANY, 0, 9);
    vec2->append(element2);
    vec2->append(element1);
    ASSERT_TRUE(vec->assign(vec2));
    ASSERT_EQ(vec->get(0)->getDouble(), 2);
    ASSERT_EQ(vec->get(1)->getInt(), 0);
}

TEST_F(MarshallTest, AnyVectorFill_SizeNotEqual){
    dolphindb::VectorSP vec = dolphindb::Util::createVector(dolphindb::DT_ANY, 0, 9);
    dolphindb::ConstantSP element1 = dolphindb::Util::createInt(0);
    dolphindb::ConstantSP element2 = dolphindb::Util::createDouble(2);
    vec->append(element1);
    vec->append(element2);

    dolphindb::VectorSP vec2 = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, 9);
    std::vector<std::string> names{"Lily", "Bob", "Lucy", "Tom", "Riddle"};
    vec2->appendString(names.data(), names.size());

    vec->fill(0, 1, vec2);
    ASSERT_GE(dynamic_cast<dolphindb::AnyVector*>(vec.get())->getAllocatedMemory(), 281);
    ASSERT_EQ(vec->get(0)->getString(), vec2->getString());
}

TEST_F(MarshallTest, AnyVectorSet_Vector_NotContainNull){
    dolphindb::VectorSP vec = dolphindb::Util::createVector(dolphindb::DT_ANY, 0, 9);
    dolphindb::ConstantSP element1 = dolphindb::Util::createInt(0);
    dolphindb::ConstantSP element2 = dolphindb::Util::createDouble(2);
    vec->append(element1);
    vec->append(element2);
    vec->append(element2);

    dolphindb::VectorSP vec2 = dolphindb::Util::createVector(dolphindb::DT_STRING, 0, 9);
    std::vector<std::string> names{"Lily", "Bob", "Lucy", "Tom", "Riddle"};
    vec2->appendString(names.data(), names.size());

    dolphindb::VectorSP indexVec = dolphindb::Util::createIndexVector(0, 2);
    vec->set(indexVec, vec2);
    ASSERT_EQ(vec->getString(0), "Lily");
    ASSERT_EQ(vec->getString(1), "Bob");
}

TEST_F(MarshallTest, AnyVectorSet_Scalar){
    dolphindb::VectorSP vec = dolphindb::Util::createVector(dolphindb::DT_ANY, 0, 9);
    dolphindb::ConstantSP element1 = dolphindb::Util::createInt(0);
    dolphindb::ConstantSP element2 = dolphindb::Util::createDouble(2);
    vec->append(element1);
    vec->append(element2);
    dolphindb::ConstantSP str = dolphindb::Util::createString("Tom");
    dolphindb::ConstantSP index1 = dolphindb::Util::createInt(0);
    dolphindb::ConstantSP emptyStr = dolphindb::Util::createString("");
    dolphindb::ConstantSP index2 = dolphindb::Util::createInt(1);

    vec->set(index1, str);
    vec->set(index2, emptyStr);
    ASSERT_EQ(vec->getString(0), "Tom");
    ASSERT_EQ(vec->getString(1), "");
}

TEST_F(MarshallTest, AnyVectorGet){
    dolphindb::ConstantSP vec = dolphindb::Util::createVector(dolphindb::DT_ANY, 0, 9);

    dolphindb::ConstantSP element1 = dolphindb::Util::createInt(0);
    dolphindb::ConstantSP element2 = dolphindb::Util::createString("");
    
    dynamic_cast<dolphindb::AnyVector*>(vec.get())->append(element1);
    dynamic_cast<dolphindb::AnyVector*>(vec.get())->append(element2);
    dolphindb::VectorSP doubleIndexVec = dolphindb::Util::createVector(dolphindb::DT_DOUBLE, 0, 9);
    std::vector<double> value1{0, 1, 2};
    doubleIndexVec->appendDouble(value1.data(), value1.size());
    dolphindb::VectorSP indexVec = dolphindb::Util::createIndexVector(0, 3);

    auto result1 = vec->get(doubleIndexVec);
    auto result2 = vec->get(indexVec);
    ASSERT_EQ(result1->getString(), result2->getString());
}

TEST_F(MarshallTest, FastBoolVectorFill_SizeNotEqual){
    bool* values = new bool[3]{true, true, false};
    dolphindb::VectorSP vec = dolphindb::Util::createVector(dolphindb::DT_BOOL, 3, 9, true, 0, values);

    bool* value2 = new bool[2]{false, false};
    dolphindb::VectorSP vec2 = dolphindb::Util::createVector(dolphindb::DT_BOOL, 2, 9, true, 0, value2);

    ASSERT_ANY_THROW(vec->fill(0, 1, vec2));
}

TEST_F(MarshallTest, CountDays){
    ASSERT_EQ(dolphindb::Util::countDays(2022, 11, 15), 19311);
    ASSERT_EQ(INT_MIN, dolphindb::Util::countDays(2022, 0, 1));
    ASSERT_EQ(INT_MIN, dolphindb::Util::countDays(2022, 13, 1));
    ASSERT_EQ(INT_MIN, dolphindb::Util::countDays(2022, 11, -1));
    ASSERT_EQ(INT_MIN, dolphindb::Util::countDays(2022, 11, 32));
    ASSERT_EQ(INT_MIN, dolphindb::Util::countDays(2000, 11, 32));
}

TEST_F(MarshallTest, parseYear){
    ASSERT_EQ(2370, dolphindb::Util::parseYear(146100));
    ASSERT_EQ(1970, dolphindb::Util::parseYear(274));
    ASSERT_EQ(1999, dolphindb::Util::parseYear(10956));
}

TEST_F(MarshallTest, getMonthEndAndParseConstant){
    ASSERT_EQ(11291, dolphindb::Util::getMonthEnd(11276));
    ASSERT_EQ(19326, dolphindb::Util::getMonthEnd(19311));
    ASSERT_EQ(17135, dolphindb::Util::getMonthEnd(17120));
    ASSERT_EQ(nullptr, dolphindb::Util::parseConstant(-1, ""));
}

TEST_F(MarshallTest, isFlatDictionary_SizeLargerThan1024){
    auto dict = dolphindb::Util::createDictionary(dolphindb::DT_STRING, dolphindb::DT_INT);
    for(int i = 0; i < 1025; ++i){
        dolphindb::ConstantSP key = dolphindb::Util::createString(std::to_string(i));
        dolphindb::ConstantSP value = dolphindb::Util::createInt(i);
        dict->set(key, value);
    }
    ASSERT_FALSE(dolphindb::Util::isFlatDictionary(dict));
}

TEST_F(MarshallTest, isFlatDictionary_ValueNotScalar){
    auto dict = dolphindb::Util::createDictionary(dolphindb::DT_STRING, dolphindb::DT_INT_ARRAY);
    dolphindb::ConstantSP key = dolphindb::Util::createString(std::to_string(1));
    dolphindb::VectorSP value = dolphindb::Util::createVector(dolphindb::DT_INT, 0, 3);
    std::vector<int> data{314, 315, 316};
    value->appendInt(data.data(), data.size());
    dict->set(key, value);
    ASSERT_FALSE(dolphindb::Util::isFlatDictionary(dict));
}

TEST_F(MarshallTest, isFlatDictionary_ValueType){
    auto dict = dolphindb::Util::createDictionary(dolphindb::DT_STRING, dolphindb::DT_DATEHOUR);
    dolphindb::ConstantSP key = dolphindb::Util::createString("1");
    dolphindb::ConstantSP value = new dolphindb::DateHour(60);
    dict->set(key, value);
    ASSERT_FALSE(dolphindb::Util::isFlatDictionary(dict));
}

TEST_F(MarshallTest, createTable_2Argument){
    {
        dolphindb::DictionarySP dict1 = dolphindb::Util::createDictionary(dolphindb::DT_INT, dolphindb::DT_INT);
        ASSERT_EQ(dolphindb::Util::createTable(dict1.get(), 1), nullptr);
    }
    {
        dolphindb::DictionarySP dict2 = dolphindb::Util::createDictionary(dolphindb::DT_STRING, dolphindb::DT_INT);
        for(int i = 0; i < 1025; ++i){
            dolphindb::ConstantSP key = dolphindb::Util::createString(std::to_string(i));
            dolphindb::ConstantSP value = dolphindb::Util::createInt(i);
            dict2->set(key, value);
        }
        ASSERT_EQ(dolphindb::Util::createTable(dict2.get(), 1), nullptr);
    }
    {
        dolphindb::DictionarySP dict3 = dolphindb::Util::createDictionary(dolphindb::DT_STRING, dolphindb::DT_STRING);
        dolphindb::ConstantSP key = dolphindb::Util::createString("1");
        dolphindb::ConstantSP value = dolphindb::Util::createString("");
        dict3->set(key, value);
        ASSERT_ANY_THROW(dolphindb::Util::createTable(dict3.get(), 1));
    }
    {
        dolphindb::DictionarySP dict4 = dolphindb::Util::createDictionary(dolphindb::DT_STRING, dolphindb::DT_INT_ARRAY);
        dolphindb::ConstantSP key = dolphindb::Util::createString(std::to_string(1));
        dolphindb::VectorSP value = dolphindb::Util::createVector(dolphindb::DT_INT, 0, 3);
        std::vector<int> data{314, 315, 316};
        value->appendInt(data.data(), data.size());
        dict4->set(key, value);
        ASSERT_ANY_THROW(dolphindb::Util::createTable(dict4.get(), 1));
    }
}

TEST_F(MarshallTest, createTable_5Argument){
    {
        std::vector<std::string> colNames{"Name", "Age"};
        std::vector<dolphindb::DATA_TYPE> colTypes{dolphindb::DT_VOID, dolphindb::DT_OBJECT};
        ASSERT_ANY_THROW(dolphindb::Util::createTable(colNames, colTypes, 0, 1));
    }
    {
        std::vector<std::string> colNames{"Name", "Age"};
        std::vector<dolphindb::DATA_TYPE> colTypes{dolphindb::DT_OBJECT, dolphindb::DT_VOID};
        ASSERT_ANY_THROW(dolphindb::Util::createTable(colNames, colTypes, 0, 1));
    }
    {
        std::vector<std::string> colNames{"Name", "Age"};
        std::vector<dolphindb::DATA_TYPE> colTypes{dolphindb::DT_ANY, dolphindb::DT_ANY};
        ASSERT_ANY_THROW(dolphindb::Util::createTable(colNames, colTypes, 0, 1));
    }
}

TEST_F(MarshallTest, createTable_toHex){
    char buf1[256]{};
    char buf2[256]{};
    unsigned char data[]{'1', 'Z', 'z', 160};

    dolphindb::Util::toHex(data, sizeof(data), true, buf1);
    ASSERT_EQ(strcmp(buf1, "a07a5a31"), 0);
    dolphindb::Util::toHex(data, sizeof(data), false, buf2);
    ASSERT_EQ(strcmp(buf2, "315a7aa0"), 0);
}

TEST_F(MarshallTest, createTable_fromHex){
    unsigned char buf1[256]{0};
    unsigned char buf2[256]{0};
    unsigned char result1[]{102, 170, 170};
    unsigned char result2[]{170, 170, 102};

    std::string data1("=0");
    ASSERT_FALSE(dolphindb::Util::fromHex(data1.c_str(), data1.size(), true, buf1));
    std::string data2("0=");
    ASSERT_FALSE(dolphindb::Util::fromHex(data2.c_str(), data2.size(), true, buf1));
    std::string data3("z0");
    ASSERT_FALSE(dolphindb::Util::fromHex(data3.c_str(), data3.size(), true, buf1));
    std::string data4("0z");
    ASSERT_FALSE(dolphindb::Util::fromHex(data4.c_str(), data4.size(), true, buf1));
    std::string data5("aaAA66");
    dolphindb::Util::fromHex(data5.c_str(), data5.size(), true, buf1); 
    ASSERT_EQ(memcmp(buf1, result1, sizeof(result1)), 0);
    dolphindb::Util::fromHex(data5.c_str(), data5.size(), false, buf2);
    ASSERT_EQ(memcmp(buf2, result2, sizeof(result2)), 0);
}

TEST_F(MarshallTest, fromGuid){
    unsigned char buf1[256]{0};
    std::string guid1("01234567-901214567-9012-456789012345");
    ASSERT_FALSE(dolphindb::Util::fromGuid(guid1.c_str(), buf1));
    guid1 = "01234567-9012-456719012-456789012345";
    ASSERT_FALSE(dolphindb::Util::fromGuid(guid1.c_str(), buf1));
    guid1 = "01234567-9012-4567-90121456789012345";
    ASSERT_FALSE(dolphindb::Util::fromGuid(guid1.c_str(), buf1));
    guid1 = "01234567-AAaa-4567-9012-456789012345";
    ASSERT_TRUE(dolphindb::Util::fromGuid(guid1.c_str(), buf1));
    unsigned char result1[]{69, 35, 1, 137, 103, 69, 18, 144, 103, 69, 170, 170, 103, 69, 35, 1};
    ASSERT_EQ(memcmp(buf1, result1, sizeof(result1)), 0);
}

TEST_F(MarshallTest, getDurationUnit){
    ASSERT_EQ(dolphindb::DU_YEAR, dolphindb::Util::getDurationUnit("y"));
    ASSERT_EQ(-1, dolphindb::Util::getDurationUnit("yY"));
    ASSERT_EQ(3600, dolphindb::Util::getTemporalDurationConversionRatio(dolphindb::DT_SECOND, dolphindb::DU_HOUR));
    ASSERT_EQ(86400, dolphindb::Util::getTemporalUplimit(dolphindb::DT_SECOND));
    ASSERT_EQ(dolphindb::Util::convertToIntegralDataType(dolphindb::DT_DATEMINUTE), dolphindb::DT_INT);
    ASSERT_EQ(dolphindb::Util::convertToIntegralDataType(dolphindb::DT_UUID), dolphindb::DT_INT128);
    ASSERT_EQ(dolphindb::Util::getCategory(dolphindb::DT_DATEMINUTE), dolphindb::TEMPORAL);
    ASSERT_EQ(dolphindb::Util::getCategory(dolphindb::DT_DECIMAL128), dolphindb::DENARY);
    ASSERT_EQ(5, dolphindb::Util::wc("123 ;ABC ab{}c b"));
    ASSERT_EQ(dolphindb::Util::toUpper('A'), 'A');
    ASSERT_EQ(dolphindb::Util::toUpper('}'), '}');
    ASSERT_EQ(dolphindb::Util::toLower('}'), '}');
    ASSERT_EQ(dolphindb::Util::toLower('a'), 'a');
    ASSERT_EQ(dolphindb::Util::toLower('A'), 'a');
    ASSERT_EQ("0.0002", dolphindb::Util::doubleToString(0.0002));
    ASSERT_FALSE(dolphindb::Util::endWith("123", "1234"));
    ASSERT_FALSE(dolphindb::Util::startWith("123", "1234"));
}

TEST_F(MarshallTest, isVariableCandidate){
    ASSERT_TRUE(dolphindb::Util::isVariableCandidate("a123"));
    ASSERT_FALSE(dolphindb::Util::isVariableCandidate("?"));
    ASSERT_FALSE(dolphindb::Util::isVariableCandidate("}"));
    ASSERT_FALSE(dolphindb::Util::isVariableCandidate("]"));
    ASSERT_FALSE(dolphindb::Util::isVariableCandidate("A}"));
    ASSERT_FALSE(dolphindb::Util::isVariableCandidate("A]"));
    ASSERT_FALSE(dolphindb::Util::isVariableCandidate("A."));
    ASSERT_EQ("\"abc\\\"abc\\\"\"", dolphindb::Util::literalConstant("abc\"abc\""));
    int epochTimes = INT_MIN;
    ASSERT_EQ(&epochTimes, dolphindb::Util::toLocalDateTime(&epochTimes, 1));
    long long epoch = LLONG_MIN;
    ASSERT_EQ(epoch, *dolphindb::Util::toLocalTimestamp(&epoch, 1));
    ASSERT_EQ(28800000001000, dolphindb::Util::toLocalNanoTimestamp(1000));
    long long times[]{1000, LLONG_MIN};
    dolphindb::Util::toLocalNanoTimestamp(times, 2);
    ASSERT_EQ(times[0], 28800000001000);
    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    std::cout << dolphindb::Util::toMicroTimestampStr(now, false) << std::endl;
}

TEST_F(MarshallTest, getDataTypeSize){
    ASSERT_EQ(sizeof(char), dolphindb::Util::getDataTypeSize(dolphindb::DT_COMPRESS));
    ASSERT_EQ(sizeof(int), dolphindb::Util::getDataTypeSize(dolphindb::DT_SYMBOL));
    std::vector<std::string> elems;
    dolphindb::Util::split("abc.", '.', elems);
    ASSERT_EQ(elems.size(), 1);
    ASSERT_EQ(elems[0], "abc");

    ASSERT_TRUE(dolphindb::Util::strWildCmp("123", "?23"));
    ASSERT_TRUE(dolphindb::Util::strWildCmp("123", "12%"));
    ASSERT_TRUE(dolphindb::Util::strWildCmp("123", "12%3"));
    ASSERT_TRUE(dolphindb::Util::strWildCmp("123", "123%"));
    ASSERT_FALSE(dolphindb::Util::strWildCmp("123", "32%3"));
    ASSERT_TRUE(dolphindb::Util::strWildCmp("123", "1%3"));

    ASSERT_TRUE(dolphindb::Util::strCaseInsensitiveWildCmp("123", "?23"));
    ASSERT_TRUE(dolphindb::Util::strCaseInsensitiveWildCmp("123", "12%"));
    ASSERT_TRUE(dolphindb::Util::strCaseInsensitiveWildCmp("123", "12%3"));
    ASSERT_TRUE(dolphindb::Util::strCaseInsensitiveWildCmp("123", "123%"));
    ASSERT_FALSE(dolphindb::Util::strCaseInsensitiveWildCmp("123", "32%3"));
    ASSERT_TRUE(dolphindb::Util::strCaseInsensitiveWildCmp("123", "1%3"));

    std::string dest;
    dolphindb::Util::writeDoubleQuotedString(dest, "123\"");
    ASSERT_EQ(dest, "\"123\"\"\"");
}

TEST_F(MarshallTest, sleep){
    dolphindb::Util::sleep(0);
    dolphindb::ConstantSP schema;
    ASSERT_ANY_THROW(dolphindb::Util::createDomain(dolphindb::HIER, dolphindb::DT_STRING, schema));

    dolphindb::VectorSP value = dolphindb::Util::createVector(dolphindb::DT_INT, 0, 3);
    std::vector<int> data{314, 315, 316};
    value->appendInt(data.data(), data.size());
    std::vector<int> indices1{-1};
    std::vector<int> indices2{4};
    ASSERT_ANY_THROW(dolphindb::Util::createSubVector(value, indices1));
    ASSERT_ANY_THROW(dolphindb::Util::createSubVector(value, indices2));

    dolphindb::SymbolBaseSP base = new dolphindb::SymbolBase(1);
    dolphindb::Util::createSymbolVector(base, 0, 3);
    int* data1 = new int[3]{1, 2, 3};
    dolphindb::VectorSP vec = dolphindb::Util::createSymbolVector(base, 0, 3, true, data1);
    ASSERT_EQ("[]", vec->getString());
    ASSERT_EQ(nullptr, dolphindb::Util::createSymbolVector(base, 0, 3, true, nullptr, (void**)&base));
}

TEST_F(MarshallTest, createValue){
    ASSERT_DOUBLE_EQ(100, dolphindb::Util::createValue(dolphindb::DT_DECIMAL64, 100, "decimal64")->getDouble());
    ASSERT_DOUBLE_EQ(100, dolphindb::Util::createValue(dolphindb::DT_DECIMAL32, 100, "decimal64")->getDouble());
    ASSERT_DOUBLE_EQ(100, dolphindb::Util::createValue(dolphindb::DT_DECIMAL32, 100, "decimal32", nullptr, 3)->getDouble());
    ASSERT_ANY_THROW(dolphindb::Util::createValue(dolphindb::DT_INT, LLONG_MIN, "int"));
    ASSERT_ANY_THROW(dolphindb::Util::createValue(dolphindb::DT_INT, LLONG_MAX, "int"));
    ASSERT_ANY_THROW(dolphindb::Util::createValue(dolphindb::DT_SHORT, LLONG_MIN, "short"));
    ASSERT_ANY_THROW(dolphindb::Util::createValue(dolphindb::DT_SHORT, LLONG_MAX, "short"));
    ASSERT_ANY_THROW(dolphindb::Util::createValue(dolphindb::DT_CHAR, LLONG_MIN, "char"));
    ASSERT_ANY_THROW(dolphindb::Util::createValue(dolphindb::DT_CHAR, LLONG_MAX, "char"));

    dolphindb::ConstantSP object = dolphindb::Util::createObject(dolphindb::DT_INT, (const char*)0);
    ASSERT_EQ("", object->getString());
    dolphindb::ConstantSP object1 = dolphindb::Util::createObject(dolphindb::DT_INT, (const void*)0);
    ASSERT_EQ("", object1->getString());
    int32_t val1 = 200;
    dolphindb::ConstantSP object2 = dolphindb::Util::createObject(dolphindb::DT_DECIMAL32, (const void*)&val1, nullptr, 0);
    ASSERT_DOUBLE_EQ(200, object2->getDouble());
    int64_t val2 = 200;
    dolphindb::ConstantSP object3 = dolphindb::Util::createObject(dolphindb::DT_DECIMAL64, (const void*)&val2, nullptr, 0);
    ASSERT_DOUBLE_EQ(200, object3->getDouble());
    std::string str1("123");
    dolphindb::ConstantSP object4 = dolphindb::Util::createObject(dolphindb::DT_SYMBOL, (const void*)str1.c_str(), nullptr, 0);
    ASSERT_EQ("123", object4->getString());
    dolphindb::ConstantSP object5 = dolphindb::Util::createObject(dolphindb::DT_STRING, (const void*)str1.c_str(), nullptr, 0);
    ASSERT_EQ("123", object5->getString());
    dolphindb::ConstantSP object6 = dolphindb::Util::createObject(dolphindb::DT_BLOB, (const void*)str1.c_str(), nullptr, 0);
    ASSERT_EQ("123", object6->getString());
    ASSERT_ANY_THROW(dolphindb::Util::createObject(dolphindb::DT_SECOND, (const void*)str1.c_str(), nullptr, 0));
}

TEST_F(MarshallTest, createObject){
    dolphindb::ConstantSP object1 = dolphindb::Util::createObject(dolphindb::DT_DECIMAL32, 300.0f, nullptr, 0);
    ASSERT_DOUBLE_EQ(300, object1->getDouble());
    dolphindb::ConstantSP object2 = dolphindb::Util::createObject(dolphindb::DT_DECIMAL32, 300.0f, nullptr, 1);
    ASSERT_DOUBLE_EQ(300, object2->getDouble());
    dolphindb::ConstantSP object3 = dolphindb::Util::createObject(dolphindb::DT_DECIMAL64, 300.0f, nullptr, 0);
    ASSERT_DOUBLE_EQ(300, object3->getDouble());
    dolphindb::ConstantSP object4 = dolphindb::Util::createObject(dolphindb::DT_DECIMAL64, 300.0f, nullptr, 1);
    ASSERT_DOUBLE_EQ(300, object4->getDouble());
    ASSERT_ANY_THROW(dolphindb::Util::createObject(dolphindb::DT_FLOAT, DBL_MIN, nullptr, 1));
    ASSERT_ANY_THROW(dolphindb::Util::createObject(dolphindb::DT_FLOAT, DBL_MAX, nullptr, 1));
    dolphindb::ConstantSP object5 = dolphindb::Util::createObject(dolphindb::DT_DECIMAL32, 400.0, nullptr, 0);
    ASSERT_DOUBLE_EQ(400, object5->getDouble());
    dolphindb::ConstantSP object6 = dolphindb::Util::createObject(dolphindb::DT_DECIMAL64, 400.0, nullptr, 0);
    ASSERT_DOUBLE_EQ(400, object6->getDouble());
    dolphindb::ConstantSP object7 = dolphindb::Util::createObject(dolphindb::DT_DECIMAL64, 400.0, nullptr, 1);
    ASSERT_DOUBLE_EQ(400, object7->getDouble());
}

TEST_F(MarshallTest, createVectorObject){
    std::vector<double> vals{1, 2};
    dolphindb::ErrorCodeInfo info;
    ASSERT_TRUE(dolphindb::Util::createObject(dolphindb::DT_SECOND, vals, &info, 0).isNull());

    std::vector<dolphindb::Constant*> vals1{dolphindb::Util::createInt(1)};
    ASSERT_EQ("([1])", dolphindb::Util::createObject(dolphindb::DT_INT, vals1)->getString());
    dolphindb::ConstantSP val1 = dolphindb::Util::createInt(2);
    std::vector<dolphindb::ConstantSP> vec{val1};
    ASSERT_EQ("([2])", dolphindb::Util::createObject(dolphindb::DT_INT, vec)->getString());
    unsigned char c[] = "aa";
    std::vector<const unsigned char*> vec1{c};
    ASSERT_EQ("([\"aa\"])", dolphindb::Util::createObject(dolphindb::DT_STRING, vec1)->getString());
    std::vector<const void*> vec2{c};
    ASSERT_EQ("([\"aa\"])", dolphindb::Util::createObject(dolphindb::DT_STRING, vec2)->getString());
}

TEST_F(MarshallTest, checkColDataType){
    dolphindb::ConstantSP cons = dolphindb::Util::createVector(dolphindb::DT_INT_ARRAY, 0, 1);
    ASSERT_TRUE(dolphindb::Util::checkColDataType(dolphindb::DT_INT_ARRAY, false, cons));
    ASSERT_FALSE(dolphindb::Util::checkColDataType(dolphindb::DT_INT, false, cons));
    dolphindb::ConstantSP cons1 = dolphindb::Util::createInt(1);
    ASSERT_TRUE(dolphindb::Util::checkColDataType(dolphindb::DT_INT, false, cons1));
    ASSERT_FALSE(dolphindb::Util::checkColDataType(dolphindb::DT_LONG, false, cons1));
    dolphindb::ConstantSP cons2 = dolphindb::Util::createString("123");
    ASSERT_TRUE(dolphindb::Util::checkColDataType(dolphindb::DT_SYMBOL, false, cons2));
    ASSERT_FALSE(dolphindb::Util::checkColDataType(dolphindb::DT_SYMBOL, false, cons1));
    ASSERT_TRUE(dolphindb::Util::checkColDataType(dolphindb::DT_SYMBOL, true, cons1));
    cons1->setTemporary(false);
    ASSERT_FALSE(dolphindb::Util::checkColDataType(dolphindb::DT_SYMBOL, true, cons1));

    char workDir[256]{};
    char buf[] = "123345456";
    getcwd(workDir, sizeof(workDir));
    std::string file = std::string(workDir).append("/tempFile123");
    dolphindb::Util::writeFile(file.c_str(), buf, 0);
    dolphindb::Util::writeFile(file.c_str(), buf, sizeof(buf));
    std::string file2 = std::string(workDir).append("/123/tempFile123");
    dolphindb::Util::writeFile(file2.c_str(), buf, sizeof(buf));
}

TEST_F(MarshallTest, isDigitOrLetter){
    ASSERT_TRUE(dolphindb::Util::isDigit('1'));
    ASSERT_FALSE(dolphindb::Util::isDigit('+'));
    ASSERT_FALSE(dolphindb::Util::isDigit('A'));
    ASSERT_TRUE(dolphindb::Util::isDateDelimitor('.'));
    ASSERT_TRUE(dolphindb::Util::isDateDelimitor('/'));
    ASSERT_TRUE(dolphindb::Util::isDateDelimitor('-'));
    ASSERT_FALSE(dolphindb::Util::isDateDelimitor('A'));
    ASSERT_TRUE(dolphindb::Util::isLetter('a'));
    ASSERT_TRUE(dolphindb::Util::isLetter('A'));
    ASSERT_FALSE(dolphindb::Util::isLetter('1'));
    ASSERT_FALSE(dolphindb::Util::isLetter(']'));
    ASSERT_FALSE(dolphindb::Util::isLetter('}'));
    ASSERT_TRUE(dolphindb::Util::is64BIT());
}

TEST_F(MarshallTest, NumberFormat_initialize){
    ASSERT_ANY_THROW(dolphindb::NumberFormat f1(""));
    ASSERT_ANY_THROW(dolphindb::NumberFormat f2("0.##A####"));
    ASSERT_ANY_THROW(dolphindb::NumberFormat f4("0.######EE0"));
    ASSERT_ANY_THROW(dolphindb::NumberFormat f5("0.######0."));
    ASSERT_ANY_THROW(dolphindb::NumberFormat f6("0.######0."));
    ASSERT_ANY_THROW(dolphindb::NumberFormat f7("0.###,###0,"));
    ASSERT_ANY_THROW(dolphindb::NumberFormat f8("0%.######0%"));
    ASSERT_ANY_THROW(dolphindb::NumberFormat f9("0%.######0"));
    ASSERT_ANY_THROW(dolphindb::NumberFormat f10(".%"));
    ASSERT_ANY_THROW(dolphindb::NumberFormat f11("E0.######"));
    ASSERT_ANY_THROW(dolphindb::NumberFormat f12("0.######E"));
    ASSERT_ANY_THROW(dolphindb::NumberFormat f13("0.######E#"));
    ASSERT_ANY_THROW(dolphindb::NumberFormat f14(",0.######"));
    ASSERT_ANY_THROW(dolphindb::NumberFormat f15("0.######,"));
    ASSERT_ANY_THROW(dolphindb::NumberFormat f16("0.###,###"));
    dolphindb::NumberFormat f17("0,000.0%");
    ASSERT_EQ("4,252.0%", f17.format(42.52));
    dolphindb::NumberFormat f3("0,000");
    ASSERT_EQ("100,000", f3.format(100000));
    dolphindb::NumberFormat f18("#0,00.0");
    ASSERT_EQ("10,00,00.0", f18.format(100000));
    dolphindb::NumberFormat f19("#.E0");
    ASSERT_EQ("1E5", f19.format(100020));
    dolphindb::NumberFormat f20("#.00##00E0");
    ASSERT_EQ("1.0002E5", f20.format(100020));
    dolphindb::NumberFormat f21("#.0000000#######");
    ASSERT_EQ(".0000010", f21.format(0.000001));
}

TEST_F(MarshallTest, NumberFormat_format){
    dolphindb::NumberFormat f0("000123");
    ASSERT_EQ("010123", f0.format(10));
    dolphindb::NumberFormat f1("123#.0000000#######");
    ASSERT_EQ("123-.0000010", f1.format(-0.000001));
    ASSERT_EQ("123.0100000E16", f1.format(1.1e+15));
    dolphindb::NumberFormat f2("000.0000E00");
    ASSERT_EQ("-100.0000E-08", f2.format(-0.000001));
    ASSERT_EQ("100.0000E08", f2.format(1e+10));
    ASSERT_EQ("900.0000E-02", f2.format(9));
    dolphindb::NumberFormat f3("000.0000############");
    ASSERT_EQ("010.0000", f3.format(10));
}

TEST_F(MarshallTest, NumberFormat_decimalFormat){
    dolphindb::DecimalFormat f1("000###");
    ASSERT_EQ("100", f1.format(100));
    dolphindb::DecimalFormat f2("000###;000###");
    ASSERT_EQ("100", f2.format(-100));
    ASSERT_EQ("100", f2.format(100));
    dolphindb::DecimalFormat f3(";000###");
    dolphindb::DecimalFormat f4("000###;");
}

TEST_F(MarshallTest, NumberFormat_TemporalFormat){
    ASSERT_ANY_THROW(dolphindb::TemporalFormat f1(""));
    ASSERT_ANY_THROW(dolphindb::TemporalFormat f2("12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"));
    ASSERT_ANY_THROW(dolphindb::TemporalFormat f3("yyyy\\"));
    char format[]{-5, 'y', 'y', 'y', 'y', '\0'};
    dolphindb::TemporalFormat f4(format);
    dolphindb::TemporalFormat f5("yy");
    ASSERT_EQ("70", f5.format(200, dolphindb::DT_MINUTE));
    ASSERT_ANY_THROW(dolphindb::TemporalFormat f6("ysysysysysysysys"));
    dolphindb::TemporalFormat f7("yyyyMMMddaaaHHmmss");
    ASSERT_EQ("1970JAN05PM 150640", f7.format(400000000, dolphindb::DT_TIMESTAMP));
    ASSERT_EQ("1970JAN04AM 112000", f7.format(300000000, dolphindb::DT_TIMESTAMP));
    dolphindb::TemporalFormat f8("yyyyMMddaaaHHmmss");
    ASSERT_EQ("19700104AM 112000", f8.format(300000000, dolphindb::DT_TIMESTAMP));
    dolphindb::TemporalFormat f9("yyMMddaaaHHmmss");
    ASSERT_EQ("700105PM 150640", f9.format(400000000, dolphindb::DT_TIMESTAMP));
    ASSERT_EQ("700104AM 112000", f9.format(300000000, dolphindb::DT_TIMESTAMP));
    dolphindb::TemporalFormat f10("yyMMMddaaaHHmmss");
    ASSERT_EQ("70JAN04AM 112000", f10.format(300000000, dolphindb::DT_TIMESTAMP));
    dolphindb::TemporalFormat f11("yyyMMMddaaaHHHHHmmss");
    ASSERT_EQ("1970JAN04AM 000112000", f11.format(300000000, dolphindb::DT_TIMESTAMP));
}

TEST_F(MarshallTest, ConstantFactory_parseConstant){
    dolphindb::ConstantFactory f;
    ASSERT_ANY_THROW(f.parseConstant(-1, ""));
    ASSERT_ANY_THROW(f.parseConstant(44, ""));
    ASSERT_EQ(nullptr, f.parseConstant(40, ""));
    ASSERT_EQ(nullptr, f.parseConstant(dolphindb::DT_SHORT, "66666"));
    ASSERT_EQ(nullptr, f.parseConstant(dolphindb::DT_SHORT, "-66666"));
    ASSERT_EQ(nullptr, f.parseConstant(dolphindb::DT_INT128, "0z123456789012345678901234567890"));

    dolphindb::ConstantSP int1 = f.parseConstant(dolphindb::DT_INT, "123");
    dolphindb::ConstantSP double1 = f.parseConstant(CONSTANT_DOUBLE_ENUM, "p");
    dolphindb::ConstantSP double2 = f.parseConstant(CONSTANT_DOUBLE_ENUM, "e");
    
    ASSERT_ANY_THROW(f.createConstant((dolphindb::DATA_TYPE)-1, 0));
    ASSERT_ANY_THROW(f.createConstant((dolphindb::DATA_TYPE)100, 0));
    ASSERT_ANY_THROW(f.createConstant(dolphindb::DT_DICTIONARY, 0));
    dolphindb::ConstantSP int2 = f.createConstant(dolphindb::DT_INT, 123);

    ASSERT_ANY_THROW(f.createConstantVector((dolphindb::DATA_TYPE)-1, 0, 1, true, 0, nullptr, nullptr, 0, false));
    ASSERT_ANY_THROW(f.createConstantVector((dolphindb::DATA_TYPE)100, 0, 1, true, 0, nullptr, nullptr, 0, false));
    ASSERT_ANY_THROW(f.createConstantArrayVector((dolphindb::DATA_TYPE)-1, 0, 1, true, 0, nullptr, nullptr, nullptr, 0, false));
    ASSERT_ANY_THROW(f.createConstantArrayVector((dolphindb::DATA_TYPE)200, 0, 1, true, 0, nullptr, nullptr, nullptr, 0, false));
    ASSERT_ANY_THROW(f.createConstantMatrix((dolphindb::DATA_TYPE)-1, 1, 1, 1, 1, nullptr, nullptr, 0, false));
    ASSERT_ANY_THROW(f.createConstantMatrix((dolphindb::DATA_TYPE)100, 1, 1, 1, 1, nullptr, nullptr, 0, false));
    ASSERT_EQ(dolphindb::DT_VOID, f.getDataType("FOR"));
    ASSERT_EQ(-1, f.getDataForm("FOR"));
    ASSERT_EQ("Uknown data type -1", f.getDataTypeString((dolphindb::DATA_TYPE)-1));
    ASSERT_EQ("Uknown data form -1", f.getDataFormString((dolphindb::DATA_FORM)-1));
    ASSERT_EQ("Uknown data form 10", f.getDataFormString((dolphindb::DATA_FORM)10));
    ASSERT_EQ("Uknown table type -1", f.getTableTypeString((dolphindb::TABLE_TYPE)-1));
    ASSERT_EQ("Uknown table type 10", f.getTableTypeString((dolphindb::TABLE_TYPE)10));
}

TEST_F(MarshallTest, ConstantFactory_createDictionary){
    dolphindb::ConstantFactory f;
    dolphindb::DictionarySP d1 = f.createDictionary(dolphindb::DT_INT, dolphindb::DT_INT, dolphindb::DT_UUID);
    dolphindb::DictionarySP d2 = f.createDictionary(dolphindb::DT_INT, dolphindb::DT_INT, dolphindb::DT_IP);
    dolphindb::DictionarySP d3 = f.createDictionary(dolphindb::DT_INT, dolphindb::DT_INT, dolphindb::DT_BLOB);
    dolphindb::DictionarySP d4 = f.createDictionary(dolphindb::DT_INT, dolphindb::DT_INT, dolphindb::DT_DATEMINUTE);
    dolphindb::DictionarySP d5 = f.createDictionary(dolphindb::DT_CHAR, dolphindb::DT_CHAR, dolphindb::DT_ANY);
    dolphindb::DictionarySP d6 = f.createDictionary(dolphindb::DT_SHORT, dolphindb::DT_SHORT, dolphindb::DT_ANY);
    ASSERT_ANY_THROW(f.createDictionary(dolphindb::DT_SECOND, dolphindb::DT_CHAR, dolphindb::DT_ANY));

    ASSERT_EQ("UnknowCategory-1", f.getCategoryString((dolphindb::DATA_CATEGORY)-1));
    ASSERT_EQ("UnknowCategory12", f.getCategoryString((dolphindb::DATA_CATEGORY)12));
}

TEST_F(MarshallTest, StringSerialize){
    dolphindb::ConstantSP str = dolphindb::Util::createString("1234567890");
    char buf1[20]{};
    char buf2[20]{};
    int numElement = 0, partial = 0;
    ASSERT_EQ(-1, str->serialize(buf1, 6, 0, 100, numElement, partial));
    ASSERT_EQ(6, str->serialize(buf1, 6, 0, 0, numElement, partial));
    ASSERT_EQ(0, numElement);
    ASSERT_EQ(6, partial);
    dolphindb::ConstantSP blob = dolphindb::Util::createBlob("1234567890");
    ASSERT_EQ(-1, blob->serialize(buf2, 6, 0, 1, numElement, partial));
    ASSERT_EQ(0, blob->serialize(buf2, 3, 0, 0, numElement, partial));
    ASSERT_EQ(6, blob->serialize(buf2, 6, 0, 0, numElement, partial));
    ASSERT_EQ(6, blob->serialize(buf2, 6, 0, 4, numElement, partial));

    char buf3[50]{};
    dolphindb::ConstantSP int128 = dolphindb::Util::createObject(dolphindb::DT_INT128, "12345678901234567890123456789012");
    ASSERT_EQ(-1, int128->serialize(buf3, 6, 0, 20, numElement, partial));
    ASSERT_EQ(6, int128->serialize(buf3, 6, 0, 0, numElement, partial));
    ASSERT_FALSE(int128->getBinary(0, 0, 20, nullptr));
    unsigned char* data = new unsigned char[32]{};
    dolphindb::ConstantSP int128Vec = dolphindb::Util::createVector(dolphindb::DT_INT128, 1, 1, true, 0, data);
    int128Vec->setString(0, "");
    ASSERT_ANY_THROW(int128Vec->setString(0, "0z123456789012345678901234567890"));

    dolphindb::ConstantSP dou1 = dolphindb::Util::createDouble(0.0/0.0);
    std::cout << dou1->getString() << std::endl;
    dolphindb::ConstantSP dou2 = dolphindb::Util::createDouble(INFINITY);
    std::cout << dou2->getString() << std::endl;
    dolphindb::ConstantSP flo1 = dolphindb::Util::createFloat(0.0/0.0);
    std::cout << flo1->getString() << std::endl;
    dolphindb::ConstantSP flo2 = dolphindb::Util::createFloat(INFINITY);
    std::cout << flo2->getString() << std::endl;
}

TEST_F(MarshallTest, IPParser){
    dolphindb::ConstantFactory f;
    ASSERT_EQ(nullptr, f.parseConstant(dolphindb::DT_IP, "12.1.1.1.1"));
    ASSERT_EQ(nullptr, f.parseConstant(dolphindb::DT_IP, "-12.1.1.1.1"));
    ASSERT_EQ(nullptr, f.parseConstant(dolphindb::DT_IP, "12.11.11"));

    dolphindb::Mutex m;
    dolphindb::LockGuard<dolphindb::Mutex> lock(&m, false);
    dolphindb::LockGuard<dolphindb::Mutex> lock1(&m);
    lock1.unlock();
    dolphindb::TryLockGuard<dolphindb::Mutex> lock2(&m, false);
    ASSERT_FALSE(lock2.isLocked());
    dolphindb::TryLockGuard<dolphindb::Mutex> lock3(&m);
    ASSERT_TRUE(lock3.isLocked());
    dolphindb::RWLock l;
    {
        dolphindb::RWLockGuard<dolphindb::RWLock> lock4(&l, true, false);
        lock4.upgrade();
    }
    {
        dolphindb::RWLockGuard<dolphindb::RWLock> lock4(nullptr, true, false);
        lock4.upgrade();
    }
    {
        dolphindb::RWLockGuard<dolphindb::RWLock> lock4(&l, false, false);
        lock4.upgrade();
    }
    {
        dolphindb::RWLockGuard<dolphindb::RWLock> lock4(&l, true);
        lock4.upgrade();
    }
    {
        dolphindb::RWLockGuard<dolphindb::RWLock> lock4(&l, false);
        lock4.upgrade();
    }
    {
        dolphindb::TryRWLockGuard<dolphindb::RWLock> lock4(&l, false);
    }
    {
        dolphindb::TryRWLockGuard<dolphindb::RWLock> lock4(&l, true);
    }
    {
        dolphindb::TryRWLockGuard<dolphindb::RWLock> lock4(&l, false, false);
    }
    {
        dolphindb::TryRWLockGuard<dolphindb::RWLock> lock4(&l, true, false);
    } 
}

#endif // __linux__