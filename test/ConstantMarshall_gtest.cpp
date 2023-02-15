class MarshallTest:public testing::Test
{
protected:
    //Suite
    static void SetUpTestCase() {}
    static void TearDownTestCase(){}

    //Case
    virtual void SetUp(){}
    virtual void TearDown(){}
};

#ifdef LINUX
TEST_F(MarshallTest, SymbolBaseMarshallStart){
    IO_ERR ret;
    SymbolBaseSP base = new SymbolBase(3);
    base->findAndInsert("314");
    base->findAndInsert("315");
    DataOutputStreamSP outStream = new DataOutputStream(1024);
    SymbolBaseMarshallSP marshall = new SymbolBaseMarshall(outStream);
    EXPECT_TRUE(marshall->start(base, false, ret));

    DataInputStreamSP inStream = new DataInputStream(outStream->getBuffer(), outStream->size());
    SymbolBaseUnmarshallSP unmarshall = new SymbolBaseUnmarshall(inStream);
    EXPECT_TRUE(unmarshall->start(false, ret));
    auto result = unmarshall->getSymbolBase();
    EXPECT_EQ(result->find("314"), 1);
    EXPECT_EQ(result->find("315"), 2);
}

TEST_F(MarshallTest, ChunkMarshallStart){
    IO_ERR ret;
    vector<string> sites = {"192.168.0.16:9002:datanode1", "192.168.0.16:9003:datanode2", "192.168.0.16:9004:datanode3", "192.168.0.16:9005:datanode4"};
    Guid id("314");
    DFSChunkMetaSP chunk = new DFSChunkMeta("/home/appadmin/data", id, 3, 1, FILE_CHUNK, sites, 315);
    std::string sendHeader("header\n");

    DataOutputStreamSP outStream = new DataOutputStream(1024);
    ConstantMarshallSP marshall = ConstantMarshallFactory::getInstance(chunk->getForm(), outStream);

    EXPECT_FALSE(marshall->start(nullptr, 1025, chunk, true, false, ret));
    EXPECT_TRUE(marshall->start(sendHeader.c_str(), sendHeader.size(), chunk, false, false, ret));
    marshall->reset();

    DataInputStreamSP inStream = new DataInputStream(outStream->getBuffer(), outStream->size());
    std::string readHeader;
    inStream->readLine(readHeader);
    EXPECT_EQ(readHeader, "header");

    short flag;
    inStream->readShort(flag);
    DATA_FORM form = static_cast<DATA_FORM>(flag >> 8);
    DATA_TYPE type = static_cast<DATA_TYPE >(flag & 0xff);

    ConstantUnmarshallFactory unmarshallFactory(inStream);
    auto unmarshall = ConstantUnmarshallFactory::getInstance(form, inStream);
    EXPECT_TRUE(unmarshall->start(flag, false, ret));
    ConstantSP result = unmarshall->getConstant();
    EXPECT_EQ(chunk->getString(), result->getString());
    unmarshall->reset();
}

TEST_F(MarshallTest, DictionaryMarshallStart){
    IO_ERR ret;
    DictionarySP dict = Util::createDictionary(DT_INT, DT_INT);
    dict->set(Util::createInt(3), Util::createInt(14));

    DataOutputStreamSP outStream = new DataOutputStream(1024);
    ConstantMarshallSP marshall = ConstantMarshallFactory::getInstance(dict->getForm(), outStream);

    EXPECT_FALSE(marshall->start(nullptr, 1025, dict, true, false, ret));
    EXPECT_TRUE(marshall->start(dict, false, false, ret));

    DataInputStreamSP inStream = new DataInputStream(outStream->getBuffer(), outStream->size());
    short flag;
    inStream->readShort(flag);
    DATA_FORM form = static_cast<DATA_FORM>(flag >> 8);
    DATA_TYPE type = static_cast<DATA_TYPE >(flag & 0xff);

    auto unmarshall = ConstantUnmarshallFactory::getInstance(form, inStream);
    unmarshall->start(flag, false, ret);
    ConstantSP result = unmarshall->getConstant();
    EXPECT_EQ(dict->getString(), result->getString());
}

TEST_F(MarshallTest, SetMarshallStart){
    IO_ERR ret;
    SetSP set = Util::createSet(DT_INT, 9);
    set->append(Util::createInt(9));

    DataOutputStreamSP outStream = new DataOutputStream(1024);
    ConstantMarshallSP marshall = ConstantMarshallFactory::getInstance(set->getForm(), outStream);

    EXPECT_FALSE(marshall->start(nullptr, 1025, set, true, false, ret));
    EXPECT_TRUE(marshall->start(set, false, false, ret));

    DataInputStreamSP inStream = new DataInputStream(outStream->getBuffer(), outStream->size());
    short flag;
    inStream->readShort(flag);
    DATA_FORM form = static_cast<DATA_FORM>(flag >> 8);
    DATA_TYPE type = static_cast<DATA_TYPE >(flag & 0xff);

    auto unmarshall = ConstantUnmarshallFactory::getInstance(form, inStream);
    unmarshall->start(flag, false, ret);
    ConstantSP result = unmarshall->getConstant();
    EXPECT_EQ(set->getString(), result->getString());
}

TEST_F(MarshallTest, TableMarshallStart){
    IO_ERR ret;
    std::vector<std::string> colNames{"id"};
    std::string specialName(5000, 'c');
    colNames.push_back(specialName);
    colNames.push_back("name");
    std::vector<std::string> names{"Tom", "Bob", "Lucy"};
    long long* ids = new long long[3]{1, 2, 3};
    int* ages = new int[3]{22, 23, 24};
    VectorSP idVec = Util::createVector(DT_LONG, 3, 3, true, 0, ids);
    VectorSP nameVec = Util::createVector(DT_STRING, 0, 3);
    nameVec->appendString(names.data(), names.size());
    VectorSP ageVec = Util::createVector(DT_INT, 3, 3, true, 0, ages);
    std::vector<ConstantSP> cols{idVec, nameVec, ageVec};
    TableSP table = Util::createTable(colNames, cols);

    DataOutputStreamSP outStream = new DataOutputStream(1024);
    ConstantMarshallSP marshall = ConstantMarshallFactory::getInstance(table->getForm(), outStream);

    EXPECT_FALSE(marshall->start(nullptr, 1025, table, true, false, ret));
    EXPECT_TRUE(marshall->start(table, false, false, ret));

    DataInputStreamSP inStream = new DataInputStream(outStream->getBuffer(), outStream->size());
    short flag;
    inStream->readShort(flag);
    DATA_FORM form = static_cast<DATA_FORM>(flag >> 8);
    DATA_TYPE type = static_cast<DATA_TYPE >(flag & 0xff);

    ConstantUnmarshallFactory unmarshallFactory(inStream);
    auto unmarshall = ConstantUnmarshallFactory::getInstance(form, inStream);
    unmarshall->start(flag, false, ret);
    ConstantSP result = unmarshall->getConstant();
    EXPECT_EQ(table->getString(), result->getString());
}


TEST_F(MarshallTest, MatrixMarshallStart){
    IO_ERR ret;
    int* pData = new int[9]{1, 2, 3, 4, 5, 6, 7, 8, 9};
    VectorSP matrix = Util::createMatrix(DT_INT, 3, 3, 9, 0, pData);
    VectorSP rowLabelVec = Util::createVector(DT_STRING, 0, 3);
    VectorSP colLabelVec = Util::createVector(DT_STRING, 0, 3);
    std::vector<std::string> rowData{"row1", "row2", "row3"};
    std::vector<std::string> colData{"col1", "col2", "col3"};
    rowLabelVec->appendString(rowData.data(), rowData.size());
    colLabelVec->appendString(colData.data(), colData.size());
    matrix->setRowLabel(rowLabelVec);
    matrix->setColumnLabel(colLabelVec);

    DataOutputStreamSP outStream = new DataOutputStream(1024);
    ConstantMarshallFactory factory(outStream);
    EXPECT_EQ(factory.getConstantMarshall((DATA_FORM)-1), nullptr);
    EXPECT_EQ(factory.getConstantMarshall((DATA_FORM)9), nullptr);
    ConstantMarshallSP marshall = ConstantMarshallFactory::getInstance(matrix->getForm(), outStream);

    EXPECT_FALSE(marshall->start(nullptr, 1025, matrix, true, false, ret));
    EXPECT_TRUE(marshall->start(matrix, false, false, ret));
    marshall->reset();

    DataInputStreamSP inStream = new DataInputStream(outStream->getBuffer(), outStream->size());
    short flag;
    inStream->readShort(flag);
    DATA_FORM form = static_cast<DATA_FORM>(flag >> 8);
    DATA_TYPE type = static_cast<DATA_TYPE >(flag & 0xff);

    ConstantUnmarshallFactory unmarshallFactory(inStream);
    EXPECT_EQ(unmarshallFactory.getConstantUnmarshall((DATA_FORM)-1), nullptr);
    EXPECT_EQ(unmarshallFactory.getConstantUnmarshall((DATA_FORM)9), nullptr);
    auto unmarshall = ConstantUnmarshallFactory::getInstance(form, inStream);
    unmarshall->start(flag, false, ret);
    ConstantSP result = unmarshall->getConstant();
    EXPECT_EQ(matrix->getString(), result->getString());
}

TEST_F(MarshallTest, PairMarshallStart){
    IO_ERR ret;
    VectorSP vec = Util::createVector(DT_INT, 0, 100);
    std::vector<int> data{314, 315, 316};
    vec->appendInt(data.data(), data.size());

    DataOutputStreamSP outStream = new DataOutputStream(1024);
    ConstantMarshallSP vectorMarshall = ConstantMarshallFactory::getInstance(DF_PAIR, outStream);

    dynamic_cast<VectorMarshall*>(vectorMarshall.get())->setCompressMethod(COMPRESS_LZ4);

    EXPECT_TRUE(vectorMarshall->start(vec, false, true, ret));

    DataInputStreamSP inStream = new DataInputStream(outStream->getBuffer(), outStream->size());

    short flag;
    inStream->readShort(flag);
    DATA_FORM form = static_cast<DATA_FORM>(flag >> 8);
    DATA_TYPE type = static_cast<DATA_TYPE >(flag & 0xff);

    ConstantUnmarshallSP unmarshall = ConstantUnmarshallFactory::getInstance(DF_PAIR, inStream);
    unmarshall->start(flag, false, ret);
    ConstantSP result = unmarshall->getConstant();
    EXPECT_EQ(result->getString(), vec->getString());
}

TEST_F(MarshallTest, SymbolVectorMarshallStart){
    IO_ERR ret;
    VectorSP symbolVec = Util::createVector(DT_SYMBOL, 0, 100);
    std::vector<std::string> data{"314", "315", "316"};
    symbolVec->appendString(data.data(), data.size());

    DataOutputStreamSP outStream = new DataOutputStream(1024);
    ConstantMarshallSP vectorMarshall = ConstantMarshallFactory::getInstance(DF_VECTOR, outStream);

    dynamic_cast<VectorMarshall*>(vectorMarshall.get())->setCompressMethod(COMPRESS_LZ4);
    std::string sendHeader("header\n");

    EXPECT_FALSE(vectorMarshall->start(nullptr, 1025, symbolVec, true, false, ret));
    EXPECT_TRUE(vectorMarshall->start(sendHeader.c_str(), sendHeader.size(), symbolVec, false, true, ret));

    DataInputStreamSP inStream = new DataInputStream(outStream->getBuffer(), outStream->size());
    std::string readHeader;
    inStream->readLine(readHeader);
    EXPECT_EQ(readHeader, "header");

    short flag;
    inStream->readShort(flag);
    DATA_FORM form = static_cast<DATA_FORM>(flag >> 8);
    DATA_TYPE type = static_cast<DATA_TYPE >(flag & 0xff);

    ConstantUnmarshallSP unmarshall = ConstantUnmarshallFactory::getInstance(form, inStream);
    unmarshall->start(flag, false, ret);
    ConstantSP result = unmarshall->getConstant();
    EXPECT_EQ(result->getString(), symbolVec->getString());
}

TEST_F(MarshallTest,MarshalFlush){
    ConstantSP scalar = Util::createInt(314);
    DataOutputStreamSP outStream = new DataOutputStream(1024);
    ConstantMarshallSP emptyMarshall = ConstantMarshallFactory::getInstance((DATA_FORM)-1, outStream);
    EXPECT_EQ(emptyMarshall.isNull(), true);
    ConstantMarshallSP marshall = ConstantMarshallFactory::getInstance(scalar->getForm(), outStream);
    
    std::string sendHeader("header\n");

    IO_ERR ret;
    EXPECT_FALSE(marshall->start(nullptr, 1025, scalar, true, false, ret));
    EXPECT_TRUE(marshall->start(sendHeader.c_str(), sendHeader.size(), scalar, false, false, ret));
    marshall->flush();

    DataInputStreamSP inStream = new DataInputStream(outStream->getBuffer(), outStream->size());
    std::string readHeader;
    inStream->readLine(readHeader);
    EXPECT_EQ(readHeader, "header");

    short flag;
    inStream->readShort(flag);
    DATA_FORM form = static_cast<DATA_FORM>(flag >> 8);
    DATA_TYPE type = static_cast<DATA_TYPE >(flag & 0xff);

    auto emptyUnmarshall = ConstantUnmarshallFactory::getInstance((DATA_FORM)-1, inStream);
    EXPECT_EQ(emptyUnmarshall.isNull(), true);
    auto unmarshall = ConstantUnmarshallFactory::getInstance(form, inStream);
    unmarshall->start(flag, false, ret);
    ConstantSP result = unmarshall->getConstant();
    EXPECT_EQ(result->getInt(), 314);
}

TEST_F(MarshallTest, StringVectorDeserialize_OriginSizeSmaller){
    char buf[MARSHALL_BUFFER_SIZE];
    
    VectorSP vec = Util::createVector(DT_STRING, 0, 9);
    std::vector<std::string> names{"Tom", "Bob", "Lucy"};
    vec->appendString(names.data(), names.size());
    int num = 0;
    int partial = 0;

    EXPECT_EQ(-1, vec->serialize(buf, MARSHALL_BUFFER_SIZE, 3, 0, num, partial));
    int len = vec->serialize(buf, MARSHALL_BUFFER_SIZE, 0, 0, num, partial);
    DataInputStreamSP inStream = new DataInputStream(buf, len);
    VectorSP result = Util::createVector(DT_STRING, 1, 9);
    result->deserialize(inStream.get(), 0, 3, num);
    EXPECT_EQ(result->getString(), vec->getString());
}

TEST_F(MarshallTest, StringVectorTrim_ContainNull){
    VectorSP vec = Util::createVector(DT_STRING, 0, 3);
    std::vector<std::string> names{"   ", "Bol", "Lucy"};
    vec->appendString(names.data(), names.size());

    ConstantSP emptyStr = Util::createString("");
    vec->set(1, emptyStr);
    vec->trim();
    EXPECT_EQ(vec->get(0)->getString(), "");
}

TEST_F(MarshallTest, StringVectorStrip_ContainNull){
    VectorSP vec = Util::createVector(DT_STRING, 0, 3);
    std::vector<std::string> names{"Lily \r\n", "Bol", "Lucy"};
    vec->appendString(names.data(), names.size());

    ConstantSP emptyStr = Util::createString("");
    vec->set(1, emptyStr);
    vec->strip();
    EXPECT_EQ(vec->get(0)->getString(), "Lily");
}

TEST_F(MarshallTest, StringVectorSet_NOT_LITERAL){
    VectorSP vec = Util::createVector(DT_STRING, 0, 3);
    std::vector<std::string> names{"Lily", "Bob", "Lucy"};
    vec->appendString(names.data(), names.size());

    VectorSP vec2 = Util::createVector(DT_INT, 0, 2);
    std::vector<int> names2{314, 315};
    vec2->appendInt(names2.data(), names2.size());

    VectorSP indexVec = Util::createIndexVector(0, 2);
    vec->set(indexVec, vec2);
    EXPECT_EQ(vec->get(0)->getString(), "314");
    EXPECT_EQ(vec->get(1)->getString(), "315");
}

TEST_F(MarshallTest, StringVectorGet_INDEX_LARGERTHAN_SIZE){
    VectorSP vec = Util::createVector(DT_STRING, 0, 3);
    std::vector<std::string> names{"Lily", "Bob", "Lucy"};
    vec->appendString(names.data(), names.size());

    VectorSP indexVec = Util::createIndexVector(0, 4);
    auto result = dynamic_cast<StringVector*>(vec.get())->get(indexVec);
    EXPECT_EQ(result->get(3)->getString(), "");
}

TEST_F(MarshallTest, StringVectorGet_NOT_INDEXARRAY){
    VectorSP vec = Util::createVector(DT_STRING, 0, 3);
    std::vector<std::string> names{"Lily", "Bob", "Lucy"};
    vec->appendString(names.data(), names.size());
    ConstantSP emptyStr = Util::createString("");
    vec->set(1, emptyStr);

    VectorSP indexVec = Util::createVector(DT_DOUBLE, 0, 4);
    std::vector<double> index{0, 1, 2, 3};
    indexVec->appendDouble(index.data(), index.size());
 
    auto result = dynamic_cast<StringVector*>(vec.get())->get(indexVec);
    EXPECT_EQ(result->get(3)->getString(), "");
}

TEST_F(MarshallTest, StringVectorAppend_NOT_LITERAL){
    VectorSP vec = Util::createVector(DT_STRING, 0, 3);
    std::vector<std::string> names{"Lily", "Bob", "Lucy"};
    vec->appendString(names.data(), names.size());

    VectorSP vec2 = Util::createVector(DT_INT, 0, 2);
    std::vector<int> names2{314, 315};
    vec2->appendInt(names2.data(), names2.size());

    vec->append(vec2, 2);
    EXPECT_EQ(vec->get(3)->getString(), "314");
    EXPECT_EQ(vec->get(4)->getString(), "315");
}

TEST_F(MarshallTest, StringVectorAppendString_LESSTHAN_CAPACITY){
    VectorSP vec = Util::createVector(DT_STRING, 0, 9);
    std::vector<std::string> names{"Lily", "Bob", "Lucy"};
    vec->appendString(names.data(), names.size());

    char str1[] = "hello";
    char str2[] = "hi";
    char* str[] = {str1, str2};

    vec->appendString(str, 2);
    EXPECT_EQ(vec->get(3)->getString(), "hello");
    EXPECT_EQ(vec->get(4)->getString(), "hi");
}

TEST_F(MarshallTest, StringVectorRemove_NOINDEXARRAY){
    VectorSP vec = Util::createVector(DT_STRING, 0, 9);
    std::vector<std::string> names{"Lily", "Bob", "Lucy"};
    vec->appendString(names.data(), names.size());
    ConstantSP emptyStr = Util::createString("");
    vec->set(1, emptyStr);

    VectorSP indexVec = Util::createVector(DT_DOUBLE, 0, 2);
    std::vector<double> index{0, 1};
    indexVec->appendDouble(index.data(), index.size());
    EXPECT_FALSE(vec->remove(indexVec));
    
    indexVec = Util::createVector(DT_INT, 0, 2);
    std::vector<int> ind{0, 2};
    indexVec->appendInt(ind.data(), ind.size());

    EXPECT_TRUE(vec->remove(indexVec));
    EXPECT_EQ(vec->size(), 1);
    EXPECT_EQ(vec->get(0)->getString(), "");
}

TEST_F(MarshallTest, StringVectorFill_LENGTH_NOTEQUAL_SIZE){
    VectorSP vec = Util::createVector(DT_STRING, 0, 9);
    std::vector<std::string> names{"Lily", "Bob", "Lucy", "Tom", "Riddle"};
    vec->appendString(names.data(), names.size());

    VectorSP vec2 = Util::createVector(DT_STRING, 0, 9);
    std::vector<std::string> names2{"Harry", "Potter", "Ron"};
    vec2->appendString(names2.data(), names2.size());

    vec->fill(0, 2, vec2);
    std::cout << vec->getString() << std::endl;
    EXPECT_EQ(vec->get(0)->getString(), "Harry");

    VectorSP vec3 = Util::createVector(DT_DOUBLE, 0, 9);
    std::vector<double> names3{314, 315};
    vec3->appendDouble(names3.data(), names3.size());

    vec->fill(2, 2, vec3);
    std::cout << vec->getString() << std::endl;
    EXPECT_EQ(vec->getString(2), "314");
}

TEST_F(MarshallTest, StringVectorGetString_String){
    VectorSP vec = Util::createVector(DT_STRING, 0, 9);
    std::vector<std::string> names{"Lily", "Bob", "Lucy", "Tom", "Riddle"};
    vec->appendString(names.data(), names.size());

    std::string* strs[5]{nullptr};
    dynamic_cast<StringVector*>(vec.get())->getString(0, 5, strs);
    EXPECT_EQ(*strs[0], "Lily");
    EXPECT_EQ(*strs[4], "Riddle");
}

TEST_F(MarshallTest, StringVectorGetString_Char){
    VectorSP vec = Util::createVector(DT_STRING, 0, 9);
    std::vector<std::string> names{"Lily", "Bob", "Lucy", "Tom", "Riddle"};
    vec->appendString(names.data(), names.size());

    char* strs[5]{nullptr};
    dynamic_cast<StringVector*>(vec.get())->getString(0, 5, strs);
    EXPECT_EQ(std::string(strs[0]), "Lily");
    EXPECT_EQ(std::string(strs[4]), "Riddle");
}

TEST_F(MarshallTest, StringVectorGetGetAllocatedMemory){
    ConstantSP vec = Util::createVector(DT_STRING, 0, 9);
    EXPECT_EQ(vec->getAllocatedMemory(), 64);
    EXPECT_EQ(dynamic_cast<StringVector*>(vec.get())->getAllocatedMemory(0), 64);
    std::vector<std::string> names{"Lily", "Bob", "Lucy", "Tom", "Riddle"};
    dynamic_cast<StringVector*>(vec.get())->appendString(names.data(), names.size());
    EXPECT_EQ(vec->getAllocatedMemory(), 129);
}

TEST_F(MarshallTest, AnyVectorAssign_Fail){
    VectorSP vec = Util::createVector(DT_ANY, 0, 9);
    ConstantSP element1 = Util::createInt(0);
    ConstantSP element2 = Util::createDouble(2);
    vec->append(element1);
    vec->append(element2);

    VectorSP vec2 = Util::createVector(DT_ANY, 0, 9);
    vec2->append(element2);
    vec2->append(element1);
    EXPECT_TRUE(vec->assign(vec2));
    EXPECT_EQ(vec->get(0)->getDouble(), 2);
    EXPECT_EQ(vec->get(1)->getInt(), 0);
}

TEST_F(MarshallTest, AnyVectorFill_SizeNotEqual){
    VectorSP vec = Util::createVector(DT_ANY, 0, 9);
    ConstantSP element1 = Util::createInt(0);
    ConstantSP element2 = Util::createDouble(2);
    vec->append(element1);
    vec->append(element2);

    VectorSP vec2 = Util::createVector(DT_STRING, 0, 9);
    std::vector<std::string> names{"Lily", "Bob", "Lucy", "Tom", "Riddle"};
    vec2->appendString(names.data(), names.size());

    vec->fill(0, 1, vec2);
    EXPECT_EQ(dynamic_cast<AnyVector*>(vec.get())->getAllocatedMemory(), 281);
    EXPECT_EQ(vec->get(0)->getString(), vec2->getString());
}

TEST_F(MarshallTest, AnyVectorSet_Vector_NotContainNull){
    VectorSP vec = Util::createVector(DT_ANY, 0, 9);
    ConstantSP element1 = Util::createInt(0);
    ConstantSP element2 = Util::createDouble(2);
    vec->append(element1);
    vec->append(element2);
    vec->append(element2);

    VectorSP vec2 = Util::createVector(DT_STRING, 0, 9);
    std::vector<std::string> names{"Lily", "Bob", "Lucy", "Tom", "Riddle"};
    vec2->appendString(names.data(), names.size());

    VectorSP indexVec = Util::createIndexVector(0, 2);
    vec->set(indexVec, vec2);
    EXPECT_EQ(vec->getString(0), "Lily");
    EXPECT_EQ(vec->getString(1), "Bob");
}

TEST_F(MarshallTest, AnyVectorSet_Scalar){
    VectorSP vec = Util::createVector(DT_ANY, 0, 9);
    ConstantSP element1 = Util::createInt(0);
    ConstantSP element2 = Util::createDouble(2);
    vec->append(element1);
    vec->append(element2);
    ConstantSP str = Util::createString("Tom");
    ConstantSP index1 = Util::createInt(0);
    ConstantSP emptyStr = Util::createString("");
    ConstantSP index2 = Util::createInt(1);

    vec->set(index1, str);
    vec->set(index2, emptyStr);
    EXPECT_EQ(vec->getString(0), "Tom");
    EXPECT_EQ(vec->getString(1), "");
}

TEST_F(MarshallTest, AnyVectorGet){
    ConstantSP vec = Util::createVector(DT_ANY, 0, 9);

    ConstantSP element1 = Util::createInt(0);
    ConstantSP element2 = Util::createString("");
    
    dynamic_cast<AnyVector*>(vec.get())->append(element1);
    dynamic_cast<AnyVector*>(vec.get())->append(element2);
    VectorSP doubleIndexVec = Util::createVector(DT_DOUBLE, 0, 9);
    std::vector<double> value1{0, 1, 2};
    doubleIndexVec->appendDouble(value1.data(), value1.size());
    VectorSP indexVec = Util::createIndexVector(0, 3);

    auto result1 = vec->get(doubleIndexVec);
    auto result2 = vec->get(indexVec);
    EXPECT_EQ(result1->getString(), result2->getString());
}

TEST_F(MarshallTest, FastBoolVectorFill_SizeNotEqual){
    bool* values = new bool[3]{true, true, false};
    VectorSP vec = Util::createVector(DT_BOOL, 3, 9, true, 0, values);

    bool* value2 = new bool[2]{false, false};
    VectorSP vec2 = Util::createVector(DT_BOOL, 2, 9, true, 0, value2);

    EXPECT_ANY_THROW(vec->fill(0, 1, vec2));
}

TEST_F(MarshallTest, CountDays){
    EXPECT_EQ(Util::countDays(2022, 11, 15), 19311);
    EXPECT_EQ(INT_MIN, Util::countDays(2022, 0, 1));
    EXPECT_EQ(INT_MIN, Util::countDays(2022, 13, 1));
    EXPECT_EQ(INT_MIN, Util::countDays(2022, 11, -1));
    EXPECT_EQ(INT_MIN, Util::countDays(2022, 11, 32));
    EXPECT_EQ(INT_MIN, Util::countDays(2000, 11, 32));
}

TEST_F(MarshallTest, parseYear){
    EXPECT_EQ(2370, Util::parseYear(146100));
    EXPECT_EQ(1970, Util::parseYear(274));
    EXPECT_EQ(1999, Util::parseYear(10956));
}

TEST_F(MarshallTest, getMonthEndAndParseConstant){
    EXPECT_EQ(11291, Util::getMonthEnd(11276));
    EXPECT_EQ(19326, Util::getMonthEnd(19311));
    EXPECT_EQ(17135, Util::getMonthEnd(17120));
    EXPECT_EQ(nullptr, Util::parseConstant(-1, ""));
}

TEST_F(MarshallTest, isFlatDictionary_SizeLargerThan1024){
    auto dict = Util::createDictionary(DT_STRING, DT_INT);
    for(int i = 0; i < 1025; ++i){
        ConstantSP key = Util::createString(std::to_string(i));
        ConstantSP value = Util::createInt(i);
        dict->set(key, value);
    }
    EXPECT_FALSE(Util::isFlatDictionary(dict));
}

TEST_F(MarshallTest, isFlatDictionary_ValueNotScalar){
    auto dict = Util::createDictionary(DT_STRING, DT_INT_ARRAY);
    ConstantSP key = Util::createString(std::to_string(1));
    VectorSP value = Util::createVector(DT_INT, 0, 3);
    std::vector<int> data{314, 315, 316};
    value->appendInt(data.data(), data.size());
    dict->set(key, value);
    EXPECT_FALSE(Util::isFlatDictionary(dict));
}

TEST_F(MarshallTest, isFlatDictionary_ValueType){
    auto dict = Util::createDictionary(DT_STRING, DT_DATEHOUR);
    ConstantSP key = Util::createString("1");
    ConstantSP value = new DateHour(60);
    dict->set(key, value);
    EXPECT_FALSE(Util::isFlatDictionary(dict));
}

TEST_F(MarshallTest, createTable_2Argument){
    {
        DictionarySP dict1 = Util::createDictionary(DT_INT, DT_INT);
        EXPECT_EQ(Util::createTable(dict1.get(), 1), nullptr);
    }
    {
        DictionarySP dict2 = Util::createDictionary(DT_STRING, DT_INT);
        for(int i = 0; i < 1025; ++i){
            ConstantSP key = Util::createString(std::to_string(i));
            ConstantSP value = Util::createInt(i);
            dict2->set(key, value);
        }
        EXPECT_EQ(Util::createTable(dict2.get(), 1), nullptr);
    }
    {
        DictionarySP dict3 = Util::createDictionary(DT_STRING, DT_STRING);
        ConstantSP key = Util::createString("1");
        ConstantSP value = Util::createString("");
        dict3->set(key, value);
        EXPECT_ANY_THROW(Util::createTable(dict3.get(), 1));
    }
    {
        DictionarySP dict4 = Util::createDictionary(DT_STRING, DT_INT_ARRAY);
        ConstantSP key = Util::createString(std::to_string(1));
        VectorSP value = Util::createVector(DT_INT, 0, 3);
        std::vector<int> data{314, 315, 316};
        value->appendInt(data.data(), data.size());
        dict4->set(key, value);
        EXPECT_ANY_THROW(Util::createTable(dict4.get(), 1));
    }
}

TEST_F(MarshallTest, createTable_5Argument){
    {
        std::vector<std::string> colNames{"Name", "Age"};
        std::vector<DATA_TYPE> colTypes{DT_VOID, DT_OBJECT};
        EXPECT_ANY_THROW(Util::createTable(colNames, colTypes, 0, 1));
    }
    {
        std::vector<std::string> colNames{"Name", "Age"};
        std::vector<DATA_TYPE> colTypes{DT_OBJECT, DT_VOID};
        EXPECT_ANY_THROW(Util::createTable(colNames, colTypes, 0, 1));
    }
    {
        std::vector<std::string> colNames{"Name", "Age"};
        std::vector<DATA_TYPE> colTypes{DT_ANY, DT_ANY};
        EXPECT_ANY_THROW(Util::createTable(colNames, colTypes, 0, 1));
    }
}

TEST_F(MarshallTest, createTable_toHex){
    char buf1[256]{};
    char buf2[256]{};
    unsigned char data[]{'1', 'Z', 'z', 160};

    Util::toHex(data, sizeof(data), true, buf1);
    EXPECT_EQ(strcmp(buf1, "a07a5a31"), 0);
    Util::toHex(data, sizeof(data), false, buf2);
    EXPECT_EQ(strcmp(buf2, "315a7aa0"), 0);
}

TEST_F(MarshallTest, createTable_fromHex){
    unsigned char buf1[256]{0};
    unsigned char buf2[256]{0};
    unsigned char result1[]{102, 170, 170};
    unsigned char result2[]{170, 170, 102};

    std::string data1("=0");
    EXPECT_FALSE(Util::fromHex(data1.c_str(), data1.size(), true, buf1));
    std::string data2("0=");
    EXPECT_FALSE(Util::fromHex(data2.c_str(), data2.size(), true, buf1));
    std::string data3("z0");
    EXPECT_FALSE(Util::fromHex(data3.c_str(), data3.size(), true, buf1));
    std::string data4("0z");
    EXPECT_FALSE(Util::fromHex(data4.c_str(), data4.size(), true, buf1));
    std::string data5("aaAA66");
    Util::fromHex(data5.c_str(), data5.size(), true, buf1); 
    EXPECT_EQ(memcmp(buf1, result1, sizeof(result1)), 0);
    Util::fromHex(data5.c_str(), data5.size(), false, buf2);
    EXPECT_EQ(memcmp(buf2, result2, sizeof(result2)), 0);
}

TEST_F(MarshallTest, fromGuid){
    unsigned char buf1[256]{0};
    std::string guid1("01234567-901214567-9012-456789012345");
    EXPECT_FALSE(Util::fromGuid(guid1.c_str(), buf1));
    guid1 = "01234567-9012-456719012-456789012345";
    EXPECT_FALSE(Util::fromGuid(guid1.c_str(), buf1));
    guid1 = "01234567-9012-4567-90121456789012345";
    EXPECT_FALSE(Util::fromGuid(guid1.c_str(), buf1));
    guid1 = "01234567-AAaa-4567-9012-456789012345";
    EXPECT_TRUE(Util::fromGuid(guid1.c_str(), buf1));
    unsigned char result1[]{69, 35, 1, 137, 103, 69, 18, 144, 103, 69, 170, 170, 103, 69, 35, 1};
    EXPECT_EQ(memcmp(buf1, result1, sizeof(result1)), 0);
}

TEST_F(MarshallTest, getDurationUnit){
    EXPECT_EQ(DU_YEAR, Util::getDurationUnit("y"));
    EXPECT_EQ(-1, Util::getDurationUnit("yY"));
    EXPECT_EQ(3600, Util::getTemporalDurationConversionRatio(DT_SECOND, DU_HOUR));
    EXPECT_EQ(86400, Util::getTemporalUplimit(DT_SECOND));
    EXPECT_EQ(Util::convertToIntegralDataType(DT_DATEMINUTE), DT_INT);
    EXPECT_EQ(Util::convertToIntegralDataType(DT_UUID), DT_INT128);
    EXPECT_EQ(Util::getCategory(DT_DATEMINUTE), TEMPORAL);
    EXPECT_EQ(Util::getCategory(DT_DECIMAL128), DENARY);
    EXPECT_EQ(5, Util::wc("123 ;ABC ab{}c b"));
    EXPECT_EQ(Util::toUpper('A'), 'A');
    EXPECT_EQ(Util::toUpper('}'), '}');
    EXPECT_EQ(Util::toLower('}'), '}');
    EXPECT_EQ(Util::toLower('a'), 'a');
    EXPECT_EQ(Util::toLower('A'), 'a');
    EXPECT_EQ("0.0002", Util::doubleToString(0.0002));
    EXPECT_FALSE(Util::endWith("123", "1234"));
    EXPECT_FALSE(Util::startWith("123", "1234"));
}

TEST_F(MarshallTest, isVariableCandidate){
    EXPECT_TRUE(Util::isVariableCandidate("a123"));
    EXPECT_FALSE(Util::isVariableCandidate("?"));
    EXPECT_FALSE(Util::isVariableCandidate("}"));
    EXPECT_FALSE(Util::isVariableCandidate("]"));
    EXPECT_FALSE(Util::isVariableCandidate("A}"));
    EXPECT_FALSE(Util::isVariableCandidate("A]"));
    EXPECT_FALSE(Util::isVariableCandidate("A."));
    EXPECT_EQ("\"abc\\\"abc\\\"\"", Util::literalConstant("abc\"abc\""));
    int epochTimes = INT_MIN;
    EXPECT_EQ(&epochTimes, Util::toLocalDateTime(&epochTimes, 1));
    long long epoch = LLONG_MIN;
    EXPECT_EQ(&epoch, Util::toLocalTimestamp(&epoch, 1));
    EXPECT_EQ(1000, Util::toLocalNanoTimestamp(1000));
    long long times[]{1000, LLONG_MIN};
    Util::toLocalNanoTimestamp(times, 2);
    EXPECT_EQ(times[0], 1000);
    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    std::cout << Util::toMicroTimestampStr(now, false) << std::endl;
}

TEST_F(MarshallTest, getDataTypeSize){
    EXPECT_EQ(sizeof(char), Util::getDataTypeSize(DT_COMPRESS));
    EXPECT_EQ(sizeof(int), Util::getDataTypeSize(DT_SYMBOL));
    std::vector<std::string> elems;
    Util::split("abc.", '.', elems);
    ASSERT_EQ(elems.size(), 1);
    EXPECT_EQ(elems[0], "abc");

    EXPECT_TRUE(Util::strWildCmp("123", "?23"));
    EXPECT_TRUE(Util::strWildCmp("123", "12%"));
    EXPECT_TRUE(Util::strWildCmp("123", "12%3"));
    EXPECT_TRUE(Util::strWildCmp("123", "123%"));
    EXPECT_FALSE(Util::strWildCmp("123", "32%3"));
    EXPECT_TRUE(Util::strWildCmp("123", "1%3"));

    EXPECT_TRUE(Util::strCaseInsensitiveWildCmp("123", "?23"));
    EXPECT_TRUE(Util::strCaseInsensitiveWildCmp("123", "12%"));
    EXPECT_TRUE(Util::strCaseInsensitiveWildCmp("123", "12%3"));
    EXPECT_TRUE(Util::strCaseInsensitiveWildCmp("123", "123%"));
    EXPECT_FALSE(Util::strCaseInsensitiveWildCmp("123", "32%3"));
    EXPECT_TRUE(Util::strCaseInsensitiveWildCmp("123", "1%3"));

    std::string dest;
    Util::writeDoubleQuotedString(dest, "123\"");
    EXPECT_EQ(dest, "\"123\"\"\"");
}

TEST_F(MarshallTest, sleep){
    Util::sleep(0);
    ConstantSP schema;
    EXPECT_ANY_THROW(Util::createDomain(HIER, DT_STRING, schema));

    VectorSP value = Util::createVector(DT_INT, 0, 3);
    std::vector<int> data{314, 315, 316};
    value->appendInt(data.data(), data.size());
    std::vector<int> indices1{-1};
    std::vector<int> indices2{4};
    EXPECT_ANY_THROW(Util::createSubVector(value, indices1));
    EXPECT_ANY_THROW(Util::createSubVector(value, indices2));

    SymbolBaseSP base = new SymbolBase(1);
    Util::createSymbolVector(base, 0, 3);
    int* data1 = new int[3]{1, 2, 3};
    VectorSP vec = Util::createSymbolVector(base, 0, 3, true, data1);
    EXPECT_EQ("[]", vec->getString());
    EXPECT_EQ(nullptr, Util::createSymbolVector(base, 0, 3, true, nullptr, (void**)&base));
}

TEST_F(MarshallTest, createValue){
    EXPECT_DOUBLE_EQ(100, Util::createValue(DT_DECIMAL64, 100, "decimal64")->getDouble());
    EXPECT_DOUBLE_EQ(100, Util::createValue(DT_DECIMAL32, 100, "decimal64")->getDouble());
    EXPECT_DOUBLE_EQ(100, Util::createValue(DT_DECIMAL32, 100, "decimal32", nullptr, 3)->getDouble());
    EXPECT_ANY_THROW(Util::createValue(DT_INT, LLONG_MIN, "int"));
    EXPECT_ANY_THROW(Util::createValue(DT_INT, LLONG_MAX, "int"));
    EXPECT_ANY_THROW(Util::createValue(DT_SHORT, LLONG_MIN, "short"));
    EXPECT_ANY_THROW(Util::createValue(DT_SHORT, LLONG_MAX, "short"));
    EXPECT_ANY_THROW(Util::createValue(DT_CHAR, LLONG_MIN, "char"));
    EXPECT_ANY_THROW(Util::createValue(DT_CHAR, LLONG_MAX, "char"));

    ConstantSP object = Util::createObject(DT_INT, (const char*)0);
    EXPECT_EQ("", object->getString());
    ConstantSP object1 = Util::createObject(DT_INT, (const void*)0);
    EXPECT_EQ("", object1->getString());
    int32_t val1 = 200;
    ConstantSP object2 = Util::createObject(DT_DECIMAL32, (const void*)&val1, nullptr, 0);
    EXPECT_DOUBLE_EQ(200, object2->getDouble());
    int64_t val2 = 200;
    ConstantSP object3 = Util::createObject(DT_DECIMAL64, (const void*)&val2, nullptr, 0);
    EXPECT_DOUBLE_EQ(200, object3->getDouble());
    std::string str1("123");
    ConstantSP object4 = Util::createObject(DT_SYMBOL, (const void*)str1.c_str(), nullptr, 0);
    EXPECT_EQ("123", object4->getString());
    ConstantSP object5 = Util::createObject(DT_STRING, (const void*)str1.c_str(), nullptr, 0);
    EXPECT_EQ("123", object5->getString());
    ConstantSP object6 = Util::createObject(DT_BLOB, (const void*)str1.c_str(), nullptr, 0);
    EXPECT_EQ("123", object6->getString());
    EXPECT_ANY_THROW(Util::createObject(DT_SECOND, (const void*)str1.c_str(), nullptr, 0));
}

TEST_F(MarshallTest, createObject){
    ConstantSP object1 = Util::createObject(DT_DECIMAL32, 300.0f, nullptr, 0);
    EXPECT_DOUBLE_EQ(300, object1->getDouble());
    ConstantSP object2 = Util::createObject(DT_DECIMAL32, 300.0f, nullptr, 1);
    EXPECT_DOUBLE_EQ(300, object2->getDouble());
    ConstantSP object3 = Util::createObject(DT_DECIMAL64, 300.0f, nullptr, 0);
    EXPECT_DOUBLE_EQ(300, object3->getDouble());
    ConstantSP object4 = Util::createObject(DT_DECIMAL64, 300.0f, nullptr, 1);
    EXPECT_DOUBLE_EQ(300, object4->getDouble());
    EXPECT_ANY_THROW(Util::createObject(DT_FLOAT, DBL_MIN, nullptr, 1));
    EXPECT_ANY_THROW(Util::createObject(DT_FLOAT, DBL_MAX, nullptr, 1));
    ConstantSP object5 = Util::createObject(DT_DECIMAL32, 400.0, nullptr, 0);
    EXPECT_DOUBLE_EQ(400, object5->getDouble());
    ConstantSP object6 = Util::createObject(DT_DECIMAL64, 400.0, nullptr, 0);
    EXPECT_DOUBLE_EQ(400, object6->getDouble());
    ConstantSP object7 = Util::createObject(DT_DECIMAL64, 400.0, nullptr, 1);
    EXPECT_DOUBLE_EQ(400, object7->getDouble());
}

TEST_F(MarshallTest, createVectorObject){
    std::vector<double> vals{1, 2};
    ErrorCodeInfo info;
    EXPECT_TRUE(Util::createObject(DT_SECOND, vals, &info, 0).isNull());

    std::vector<Constant*> vals1{Util::createInt(1)};
    EXPECT_EQ("([1])", Util::createObject(DT_INT, vals1)->getString());
    ConstantSP val1 = Util::createInt(2);
    std::vector<ConstantSP> vec{val1};
    EXPECT_EQ("([2])", Util::createObject(DT_INT, vec)->getString());
    unsigned char c[] = "aa";
    std::vector<const unsigned char*> vec1{c};
    EXPECT_EQ("([\"aa\"])", Util::createObject(DT_STRING, vec1)->getString());
    std::vector<const void*> vec2{c};
    EXPECT_EQ("([\"aa\"])", Util::createObject(DT_STRING, vec2)->getString());
}

TEST_F(MarshallTest, checkColDataType){
    ConstantSP cons = Util::createVector(DT_INT_ARRAY, 0, 1);
    EXPECT_TRUE(Util::checkColDataType(DT_INT_ARRAY, false, cons));
    EXPECT_FALSE(Util::checkColDataType(DT_INT, false, cons));
    ConstantSP cons1 = Util::createInt(1);
    EXPECT_TRUE(Util::checkColDataType(DT_INT, false, cons1));
    EXPECT_FALSE(Util::checkColDataType(DT_LONG, false, cons1));
    ConstantSP cons2 = Util::createString("123");
    EXPECT_TRUE(Util::checkColDataType(DT_SYMBOL, false, cons2));
    EXPECT_FALSE(Util::checkColDataType(DT_SYMBOL, false, cons1));
    EXPECT_TRUE(Util::checkColDataType(DT_SYMBOL, true, cons1));
    cons1->setTemporary(false);
    EXPECT_FALSE(Util::checkColDataType(DT_SYMBOL, true, cons1));

    char workDir[256]{};
    char buf[] = "123345456";
    getcwd(workDir, sizeof(workDir));
    std::string file = std::string(workDir).append("/tempFile123");
    Util::writeFile(file.c_str(), buf, 0);
    Util::writeFile(file.c_str(), buf, sizeof(buf));
    std::string file2 = std::string(workDir).append("/123/tempFile123");
    Util::writeFile(file2.c_str(), buf, sizeof(buf));
}

TEST_F(MarshallTest, isDigitOrLetter){
    EXPECT_TRUE(Util::isDigit('1'));
    EXPECT_FALSE(Util::isDigit('+'));
    EXPECT_FALSE(Util::isDigit('A'));
    EXPECT_TRUE(Util::isDateDelimitor('.'));
    EXPECT_TRUE(Util::isDateDelimitor('/'));
    EXPECT_TRUE(Util::isDateDelimitor('-'));
    EXPECT_FALSE(Util::isDateDelimitor('A'));
    EXPECT_TRUE(Util::isLetter('a'));
    EXPECT_TRUE(Util::isLetter('A'));
    EXPECT_FALSE(Util::isLetter('1'));
    EXPECT_FALSE(Util::isLetter(']'));
    EXPECT_FALSE(Util::isLetter('}'));
    EXPECT_TRUE(Util::is64BIT());
}

TEST_F(MarshallTest, NumberFormat_initialize){
    EXPECT_ANY_THROW(NumberFormat f1(""));
    EXPECT_ANY_THROW(NumberFormat f2("0.##A####"));
    EXPECT_ANY_THROW(NumberFormat f4("0.######EE0"));
    EXPECT_ANY_THROW(NumberFormat f5("0.######0."));
    EXPECT_ANY_THROW(NumberFormat f6("0.######0."));
    EXPECT_ANY_THROW(NumberFormat f7("0.###,###0,"));
    EXPECT_ANY_THROW(NumberFormat f8("0%.######0%"));
    EXPECT_ANY_THROW(NumberFormat f9("0%.######0"));
    EXPECT_ANY_THROW(NumberFormat f10(".%"));
    EXPECT_ANY_THROW(NumberFormat f11("E0.######"));
    EXPECT_ANY_THROW(NumberFormat f12("0.######E"));
    EXPECT_ANY_THROW(NumberFormat f13("0.######E#"));
    EXPECT_ANY_THROW(NumberFormat f14(",0.######"));
    EXPECT_ANY_THROW(NumberFormat f15("0.######,"));
    EXPECT_ANY_THROW(NumberFormat f16("0.###,###"));
    NumberFormat f17("0,000.0%");
    EXPECT_EQ("4,252.0%", f17.format(42.52));
    NumberFormat f3("0,000");
    EXPECT_EQ("100,000", f3.format(100000));
    NumberFormat f18("#0,00.0");
    EXPECT_EQ("10,00,00.0", f18.format(100000));
    NumberFormat f19("#.E0");
    EXPECT_EQ("1E5", f19.format(100020));
    NumberFormat f20("#.00##00E0");
    EXPECT_EQ("1.0002E5", f20.format(100020));
    NumberFormat f21("#.0000000#######");
    EXPECT_EQ(".0000010", f21.format(0.000001));
}

TEST_F(MarshallTest, NumberFormat_format){
    NumberFormat f0("000123");
    EXPECT_EQ("010123", f0.format(10));
    NumberFormat f1("123#.0000000#######");
    EXPECT_EQ("123-.0000010", f1.format(-0.000001));
    EXPECT_EQ("123.0100000E16", f1.format(1.1e+15));
    NumberFormat f2("000.0000E00");
    EXPECT_EQ("-100.0000E-08", f2.format(-0.000001));
    EXPECT_EQ("100.0000E08", f2.format(1e+10));
    EXPECT_EQ("900.0000E-02", f2.format(9));
    NumberFormat f3("000.0000############");
    EXPECT_EQ("010.0000", f3.format(10));
}

TEST_F(MarshallTest, NumberFormat_decimalFormat){
    DecimalFormat f1("000###");
    EXPECT_EQ("100", f1.format(100));
    DecimalFormat f2("000###;000###");
    EXPECT_EQ("100", f2.format(-100));
    EXPECT_EQ("100", f2.format(100));
    DecimalFormat f3(";000###");
    DecimalFormat f4("000###;");
}

TEST_F(MarshallTest, NumberFormat_TemporalFormat){
    EXPECT_ANY_THROW(TemporalFormat f1(""));
    EXPECT_ANY_THROW(TemporalFormat f2("12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"));
    EXPECT_ANY_THROW(TemporalFormat f3("yyyy\\"));
    char format[]{-5, 'y', 'y', 'y', 'y'};
    TemporalFormat f4(format);
    TemporalFormat f5("yy");
    EXPECT_EQ("70", f5.format(200, DT_MINUTE));
    EXPECT_ANY_THROW(TemporalFormat f6("ysysysysysysysys"));
    TemporalFormat f7("yyyyMMMddaaaHHmmss");
    EXPECT_EQ("1970JAN05PM 150640", f7.format(400000000, DT_TIMESTAMP));
    EXPECT_EQ("1970JAN04AM 112000", f7.format(300000000, DT_TIMESTAMP));
    TemporalFormat f8("yyyyMMddaaaHHmmss");
    EXPECT_EQ("19700104AM 112000", f8.format(300000000, DT_TIMESTAMP));
    TemporalFormat f9("yyMMddaaaHHmmss");
    EXPECT_EQ("700105PM 150640", f9.format(400000000, DT_TIMESTAMP));
    EXPECT_EQ("700104AM 112000", f9.format(300000000, DT_TIMESTAMP));
    TemporalFormat f10("yyMMMddaaaHHmmss");
    EXPECT_EQ("70JAN04AM 112000", f10.format(300000000, DT_TIMESTAMP));
    TemporalFormat f11("yyyMMMddaaaHHHHHmmss");
    EXPECT_EQ("1970JAN04AM 000112000", f11.format(300000000, DT_TIMESTAMP));
}

TEST_F(MarshallTest, ConstantFactory_parseConstant){
    ConstantFactory f;
    EXPECT_ANY_THROW(f.parseConstant(-1, ""));
    EXPECT_ANY_THROW(f.parseConstant(44, ""));
    EXPECT_EQ(nullptr, f.parseConstant(40, ""));
    EXPECT_EQ(nullptr, f.parseConstant(DT_SHORT, "66666"));
    EXPECT_EQ(nullptr, f.parseConstant(DT_SHORT, "-66666"));
    EXPECT_EQ(nullptr, f.parseConstant(DT_INT128, "0z123456789012345678901234567890"));

    ConstantSP int1 = f.parseConstant(DT_INT, "123");
    ConstantSP double1 = f.parseConstant(CONSTANT_DOUBLE_ENUM, "p");
    ConstantSP double2 = f.parseConstant(CONSTANT_DOUBLE_ENUM, "e");
    
    EXPECT_ANY_THROW(f.createConstant((DATA_TYPE)-1, 0));
    EXPECT_ANY_THROW(f.createConstant((DATA_TYPE)100, 0));
    EXPECT_ANY_THROW(f.createConstant(DT_DICTIONARY, 0));
    ConstantSP int2 = f.createConstant(DT_INT, 123);

    EXPECT_ANY_THROW(f.createConstantVector((DATA_TYPE)-1, 0, 1, true, 0, nullptr, nullptr, 0, false));
    EXPECT_ANY_THROW(f.createConstantVector((DATA_TYPE)100, 0, 1, true, 0, nullptr, nullptr, 0, false));
    EXPECT_ANY_THROW(f.createConstantArrayVector((DATA_TYPE)-1, 0, 1, true, 0, nullptr, nullptr, nullptr, 0, false));
    EXPECT_ANY_THROW(f.createConstantArrayVector((DATA_TYPE)200, 0, 1, true, 0, nullptr, nullptr, nullptr, 0, false));
    EXPECT_ANY_THROW(f.createConstantMatrix((DATA_TYPE)-1, 1, 1, 1, 1, nullptr, nullptr, 0, false));
    EXPECT_ANY_THROW(f.createConstantMatrix((DATA_TYPE)100, 1, 1, 1, 1, nullptr, nullptr, 0, false));
    EXPECT_EQ(DT_VOID, f.getDataType("FOR"));
    EXPECT_EQ(-1, f.getDataForm("FOR"));
    EXPECT_EQ("UknownType-1", f.getDataTypeString((DATA_TYPE)-1));
    EXPECT_EQ("UknownForm-1", f.getDataFormString((DATA_FORM)-1));
    EXPECT_EQ("UknownForm10", f.getDataFormString((DATA_FORM)10));
    EXPECT_EQ("UknownTable-1", f.getTableTypeString((TABLE_TYPE)-1));
    EXPECT_EQ("UknownTable10", f.getTableTypeString((TABLE_TYPE)10));
}

TEST_F(MarshallTest, ConstantFactory_createDictionary){
    ConstantFactory f;
    DictionarySP d1 = f.createDictionary(DT_INT, DT_INT, DT_UUID);
    DictionarySP d2 = f.createDictionary(DT_INT, DT_INT, DT_IP);
    DictionarySP d3 = f.createDictionary(DT_INT, DT_INT, DT_BLOB);
    DictionarySP d4 = f.createDictionary(DT_INT, DT_INT, DT_DATEMINUTE);
    DictionarySP d5 = f.createDictionary(DT_CHAR, DT_CHAR, DT_ANY);
    DictionarySP d6 = f.createDictionary(DT_SHORT, DT_SHORT, DT_ANY);
    DictionarySP d7 = f.createDictionary(DT_SECOND, DT_CHAR, DT_ANY);

    EXPECT_EQ("UnknowCategory-1", f.getCategoryString((DATA_CATEGORY)-1));
    EXPECT_EQ("UnknowCategory12", f.getCategoryString((DATA_CATEGORY)12));
}

TEST_F(MarshallTest, StringSerialize){
    ConstantSP str = Util::createString("1234567890");
    char buf1[20]{};
    char buf2[20]{};
    int numElement = 0, partial = 0;
    EXPECT_EQ(-1, str->serialize(buf1, 6, 0, 100, numElement, partial));
    EXPECT_EQ(6, str->serialize(buf1, 6, 0, 0, numElement, partial));
    EXPECT_EQ(0, numElement);
    EXPECT_EQ(6, partial);
    ConstantSP blob = Util::createBlob("1234567890");
    EXPECT_EQ(-1, blob->serialize(buf2, 6, 0, 1, numElement, partial));
    EXPECT_EQ(0, blob->serialize(buf2, 3, 0, 0, numElement, partial));
    EXPECT_EQ(6, blob->serialize(buf2, 6, 0, 0, numElement, partial));
    EXPECT_EQ(6, blob->serialize(buf2, 6, 0, 4, numElement, partial));

    char buf3[50]{};
    ConstantSP int128 = Util::createObject(DT_INT128, "12345678901234567890123456789012");
    EXPECT_EQ(-1, int128->serialize(buf3, 6, 0, 20, numElement, partial));
    EXPECT_EQ(6, int128->serialize(buf3, 6, 0, 0, numElement, partial));
    EXPECT_FALSE(int128->getBinary(0, 0, 20, nullptr));
    unsigned char* data = new unsigned char[32]{};
    ConstantSP int128Vec = Util::createVector(DT_INT128, 1, 1, true, 0, data);
    int128Vec->setString(0, "");
    EXPECT_ANY_THROW(int128Vec->setString(0, "0z123456789012345678901234567890"));

    ConstantSP dou1 = Util::createDouble(0.0/0.0);
    std::cout << dou1->getString() << std::endl;
    ConstantSP dou2 = Util::createDouble(INFINITY);
    std::cout << dou2->getString() << std::endl;
    ConstantSP flo1 = Util::createFloat(0.0/0.0);
    std::cout << flo1->getString() << std::endl;
    ConstantSP flo2 = Util::createFloat(INFINITY);
    std::cout << flo2->getString() << std::endl;
}

TEST_F(MarshallTest, IPParser){
    ConstantFactory f;
    EXPECT_EQ(nullptr, f.parseConstant(DT_IP, "12.1.1.1.1"));
    EXPECT_EQ(nullptr, f.parseConstant(DT_IP, "-12.1.1.1.1"));
    EXPECT_EQ(nullptr, f.parseConstant(DT_IP, "12.11.11"));

    Mutex m;
    LockGuard<Mutex> lock(&m, false);
    LockGuard<Mutex> lock1(&m);
    lock1.unlock();
    TryLockGuard<Mutex> lock2(&m, false);
    EXPECT_FALSE(lock2.isLocked());
    TryLockGuard<Mutex> lock3(&m);
    EXPECT_TRUE(lock3.isLocked());
    RWLock l;
    {
        RWLockGuard<RWLock> lock4(&l, true, false);
        lock4.upgrade();
    }
    {
        RWLockGuard<RWLock> lock4(nullptr, true, false);
        lock4.upgrade();
    }
    {
        RWLockGuard<RWLock> lock4(&l, false, false);
        lock4.upgrade();
    }
    {
        RWLockGuard<RWLock> lock4(&l, true);
        lock4.upgrade();
    }
    {
        RWLockGuard<RWLock> lock4(&l, false);
        lock4.upgrade();
    }
    {
        TryRWLockGuard<RWLock> lock4(&l, false);
    }
    {
        TryRWLockGuard<RWLock> lock4(&l, true);
    }
    {
        TryRWLockGuard<RWLock> lock4(&l, false, false);
    }
    {
        TryRWLockGuard<RWLock> lock4(&l, true, false);
    } 
}

TEST_F(MarshallTest, BoundedBlockingQueue_test){
    BoundedBlockingQueue<int> queue(100);
    queue.push(1314);
    int result = 0;
    queue.pop(result);
    EXPECT_EQ(result, 1314);
}

#endif