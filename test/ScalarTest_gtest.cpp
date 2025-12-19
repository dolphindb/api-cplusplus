#include <gtest/gtest.h>
#include "config.h"
#include <sstream>
#include <ScalarImp.h>

class ScalarTest:public testing::Test
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
        //Case
        virtual void SetUp()
        {

        }
        virtual void TearDown()
        {

        }
};

dolphindb::DBConnection ScalarTest::conn(false, false);

template <typename T>
static T round(T src, int bits)
{
    std::stringstream ss;
    ss << std::fixed << std::setprecision(bits) << src;
    ss >> src;
    return src;
}

TEST_F(ScalarTest,testCreateObjectDecimal128){
    dolphindb::ConstantSP d1 = dolphindb::Util::createObject(dolphindb::DT_DECIMAL128, (float)1, nullptr, 2);
    ASSERT_EQ(d1->getString(), "1.00");
    dolphindb::ConstantSP d2 = dolphindb::Util::createObject(dolphindb::DT_DECIMAL128, (float)1, nullptr, -2);
    ASSERT_EQ(d2->getString(), "1");
    dolphindb::ConstantSP d3 = dolphindb::Util::createObject(dolphindb::DT_DECIMAL128, (double)1, nullptr, 2);
    ASSERT_EQ(d3->getString(), "1.00");
    dolphindb::ConstantSP d4 = dolphindb::Util::createObject(dolphindb::DT_DECIMAL128, (double)1, nullptr, -2);
    ASSERT_EQ(d4->getString(), "1");
}

TEST_F(ScalarTest,testScalar1){
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

    std::vector<dolphindb::ConstantSP> vals = {boolval,charval,intval,shortval,longval,floatval,doubleval,stringval,symbolval,blobval,int128val,\
                                ipval,uuidval,dateval,monthval,timeval,minuteval,secondval,datetimeval,timestampval,nanotimeval,nanotimestampval};
    std::vector<std::string> names = {"boolval","charval","intval","shortval","longval","floatval","doubleval","stringval","symbolval","blobval","int128val",\
                                "ipval","uuidval","dateval","monthval","timeval","minuteval","secondval","datetimeval","timestampval","nanotimeval","nanotimestampval"};
    conn.upload(names,vals);

    ASSERT_TRUE(conn.run("eqObj(boolval,bool("+boolval->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(charval,'"+charval->getString()+"')")->getBool());
    ASSERT_TRUE(conn.run("eqObj(intval,"+intval->getString()+")")->getBool());
    ASSERT_TRUE(conn.run("eqObj(shortval,"+shortval->getString()+")")->getBool());
    ASSERT_TRUE(conn.run("eqObj(longval,"+longval->getString()+")")->getBool());
    ASSERT_TRUE(conn.run("eqObj(floatval,"+floatval->getString()+", 4)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(doubleval,"+doubleval->getString()+", 5)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(stringval,\""+stringval->getString()+"\")")->getBool());
    ASSERT_TRUE(conn.run("eqObj(symbolval,\""+symbolval->getString()+"\")")->getBool());
    ASSERT_TRUE(conn.run("eqObj(blobval,\""+blobval->getString()+"\")")->getBool());
    ASSERT_TRUE(conn.run("eqObj(int128val,int128('"+int128val->getString()+"'))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(ipval,ipaddr('"+ipval->getString()+"'))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(uuidval,uuid('"+uuidval->getString()+"'))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(dateval,date("+dateval->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(monthval,month("+monthval->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(timeval,time("+timeval->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(minuteval,minute("+minuteval->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(secondval,second("+secondval->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(datetimeval,datetime("+datetimeval->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(timestampval,timestamp("+timestampval->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(nanotimeval,nanotime("+nanotimeval->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(nanotimestampval,nanotimestamp("+nanotimestampval->getString()+"))")->getBool());

    for(auto i=0; i<vals.size();i++){
        ASSERT_TRUE(vals[i]->isScalar());
        ASSERT_FALSE(vals[i]->isMatrix());
        ASSERT_FALSE(vals[i]->isVector());
        ASSERT_FALSE(vals[i]->isArray());
        ASSERT_FALSE(vals[i]->isChunk());
        ASSERT_FALSE(vals[i]->isDatabase());
        ASSERT_FALSE(vals[i]->isDictionary());
        ASSERT_FALSE(vals[i]->isHugeIndexArray());
        ASSERT_FALSE(vals[i]->isIndexArray());
        ASSERT_FALSE(vals[i]->isPair());
        ASSERT_FALSE(vals[i]->isSet());
        ASSERT_FALSE(vals[i]->isTable());
        ASSERT_FALSE(vals[i]->isTuple());
        ASSERT_EQ(vals[i]->getString(), conn.run(names[i])->getString());

        std::cout<<dolphindb::Util::getDataTypeString(vals[i]->getType())<<std::endl;

        if(i <= 6){
            if(i > 0){
                vals[i]->setBool(2);
                ASSERT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                ASSERT_EQ(vals[i]->getString(), "2");
                vals[i]->setBool(CHAR_MIN);
                ASSERT_TRUE(vals[i]->isNull());
                ASSERT_FALSE(vals[i]->add(0,1,(long long)3));

                vals[i]->setChar(2);
                // std::cout<<dolphindb::Util::getDataTypeString(vals[i]->getType())<<std::endl;
                // dolphindb::ConstantSP temp = vals[i]->getInstance();
                // temp->setChar(2);
                // ASSERT_EQ(temp->getString(), vals[i]->getString());
                ASSERT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                ASSERT_EQ(vals[i]->getString(), "2");
                vals[i]->setChar(CHAR_MIN);
                ASSERT_TRUE(vals[i]->isNull());

                vals[i]->setShort(2);
                ASSERT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                ASSERT_EQ(vals[i]->getString(), "2");
                vals[i]->setShort(SHRT_MIN);
                ASSERT_TRUE(vals[i]->isNull());

                vals[i]->setInt(2);
                vals[i]->nullFill(dolphindb::Util::createChar(3));
                ASSERT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                ASSERT_EQ(vals[i]->getString(), "2");
                vals[i]->setInt(INT_MIN);
                ASSERT_TRUE(vals[i]->isNull());
                vals[i]->nullFill(dolphindb::Util::createChar(3));
                ASSERT_EQ(vals[i]->getString(), "3");
                ASSERT_TRUE(vals[i]->add(0,1,(long long)1));
                ASSERT_EQ(vals[i]->getString(), "4");
                ASSERT_EQ(vals[i]->compare(0, dolphindb::Util::createChar(2)), 1);
                ASSERT_EQ(vals[i]->compare(0, dolphindb::Util::createChar(4)), 0);
                ASSERT_EQ(vals[i]->compare(0, dolphindb::Util::createChar(6)), -1);

                vals[i]->setLong(2);
                ASSERT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                ASSERT_EQ(vals[i]->getString(), "2");
                vals[i]->setLong(LLONG_MIN);
                ASSERT_TRUE(vals[i]->isNull());

                vals[i]->setFloat(2);
                vals[i]->nullFill(dolphindb::Util::createFloat(3));
                ASSERT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                ASSERT_EQ(vals[i]->getString(), "2");
                vals[i]->setFloat(dolphindb::FLT_NMIN);
                ASSERT_TRUE(vals[i]->isNull());
                vals[i]->nullFill(dolphindb::Util::createFloat(3));
                ASSERT_EQ(vals[i]->getString(), "3");
                ASSERT_TRUE(vals[i]->add(0,1,(float)1));
                ASSERT_EQ(vals[i]->getString(), "4");
                ASSERT_EQ(vals[i]->compare(0, dolphindb::Util::createFloat(2)), 1);
                ASSERT_EQ(vals[i]->compare(0, dolphindb::Util::createFloat(4)), 0);
                ASSERT_EQ(vals[i]->compare(0, dolphindb::Util::createFloat(6)), -1);

                vals[i]->setDouble(2);
                ASSERT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                ASSERT_EQ(vals[i]->getString(), "2");
                vals[i]->setDouble(dolphindb::DBL_NMIN);
                ASSERT_TRUE(vals[i]->isNull());

                vals[i]->setIndex(2);
                ASSERT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                ASSERT_EQ(vals[i]->getString(), "2");
                vals[i]->setIndex(dolphindb::INDEX_MIN);
                ASSERT_TRUE(vals[i]->isNull());
            }
            else{
                vals[i]->setBool(2);
                ASSERT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                ASSERT_EQ(vals[i]->getString(), "1");
                vals[i]->setBool(CHAR_MIN);
                ASSERT_TRUE(vals[i]->isNull());

                vals[i]->setChar(2);
                ASSERT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                ASSERT_EQ(vals[i]->getString(), "1");
                vals[i]->setChar(CHAR_MIN);
                ASSERT_TRUE(vals[i]->isNull());

                vals[i]->setShort(2);
                ASSERT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                ASSERT_EQ(vals[i]->getString(), "1");
                vals[i]->setShort(SHRT_MIN);
                ASSERT_TRUE(vals[i]->isNull());

                vals[i]->setInt(2);
                ASSERT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                ASSERT_EQ(vals[i]->getString(), "1");
                vals[i]->setInt(INT_MIN);
                ASSERT_TRUE(vals[i]->isNull());

                vals[i]->setLong(2);
                ASSERT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                ASSERT_EQ(vals[i]->getString(), "1");
                vals[i]->setLong(LLONG_MIN);
                ASSERT_TRUE(vals[i]->isNull());

                vals[i]->setFloat(2);
                ASSERT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                ASSERT_EQ(vals[i]->getString(), "1");
                vals[i]->setFloat(dolphindb::FLT_NMIN);
                ASSERT_TRUE(vals[i]->isNull());

                vals[i]->setDouble(2);
                ASSERT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                ASSERT_EQ(vals[i]->getString(), "1");
                vals[i]->setDouble(dolphindb::DBL_NMIN);
                ASSERT_TRUE(vals[i]->isNull());

                vals[i]->setIndex(2);
                ASSERT_EQ(vals[i]->getType(), conn.run(names[i])->getType());
                ASSERT_EQ(vals[i]->getString(), "1");
                vals[i]->setIndex(dolphindb::INDEX_MIN);
                ASSERT_TRUE(vals[i]->isNull());
            }

        }
        else if (i > 12)
        {
            vals[i]->setChar(2);
            dolphindb::ConstantSP temp = vals[i]->getInstance();
            temp->setChar(2);
            ASSERT_EQ(temp->getString(), vals[i]->getString());
        }


    }

    ASSERT_EQ(conn.run("a=NULL;a")->getString(),"");
    ASSERT_ANY_THROW(dateval->getInt128());

    #ifndef _WIN32
    dolphindb::Time t1(86400000);
    dolphindb::Time t2(100001);
    dolphindb::Time t3(-1);
    dolphindb::Time t4(0);
    t1.validate();t2.validate();t4.validate();t3.validate();

    ASSERT_TRUE(t1.isNull() && t3.isNull());
    ASSERT_EQ(t2.getString(), "00:01:40.001");
    ASSERT_EQ(t4.getString(), "00:00:00.000");

    dolphindb::Minute m1(1440);
    dolphindb::Minute m2(1001);
    dolphindb::Minute m3(-1);
    dolphindb::Minute m4(0);
    m1.validate();m2.validate();m4.validate();m3.validate();

    ASSERT_TRUE(m1.isNull() && m3.isNull());
    ASSERT_EQ(m2.getString(), "16:41m");
    ASSERT_EQ(m4.getString(), "00:00m");

    dolphindb::Second sec1(86400);
    dolphindb::Second sec2(10001);
    dolphindb::Second sec3(-1);
    dolphindb::Second sec4(0);
    sec1.validate();sec2.validate();sec3.validate();sec4.validate();

    ASSERT_TRUE(sec1.isNull() && sec3.isNull());
    ASSERT_EQ(sec2.getString(), "02:46:41");
    ASSERT_EQ(sec4.getString(), "00:00:00");


    dolphindb::NanoTime ss1(86400000000000ll);
    dolphindb::NanoTime ss2(100000000001);
    dolphindb::NanoTime ss3(-1);
    dolphindb::NanoTime ss4(0);
    ss1.validate();ss2.validate();ss3.validate();ss4.validate();

    ASSERT_TRUE(ss1.isNull() && ss3.isNull());
    ASSERT_EQ(ss2.getString(), "00:01:40.000000001");
    ASSERT_EQ(ss4.getString(), "00:00:00.000000000");
    #endif

}


TEST_F(ScalarTest,testScalar2){
    bool s1 = true;
    char s2 = char(127);
    int s3 = INT_MAX;
    short s4 = SHRT_MAX;
    long long s5 = LLONG_MAX;
    float s6 = -31649.1234f;
    double s7 = -126553.123456789;
    std::string str = "中文《；‘【】*&……%￥%#’@“”：“”？》";
    std::string sym = "中文《；‘【】*&……%￥%#’@“”：“”？》";
    std::string blob = "中文《；‘【】*&……%￥%#’@“”：“”？》";
    unsigned char int128[16];
    unsigned char ip[16];
    unsigned char uuid[16];
    for(auto i=0;i<16;i++){
        int128[i] = CHAR_MAX;
        ip[i] = CHAR_MAX;
        uuid[i] = CHAR_MAX;
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
    dolphindb::ConstantSP dateval = dolphindb::Util::createDate(2025,12,31);
    dolphindb::ConstantSP monthval = dolphindb::Util::createMonth(2025,12);
    dolphindb::ConstantSP timeval = dolphindb::Util::createTime(23,59,59,999);
    dolphindb::ConstantSP minuteval = dolphindb::Util::createMinute(23,59);
    dolphindb::ConstantSP secondval = dolphindb::Util::createSecond(23,59,59);
    dolphindb::ConstantSP datetimeval = dolphindb::Util::createDateTime(2025,12,31,23,59,59);
    dolphindb::ConstantSP timestampval = dolphindb::Util::createTimestamp(2025,12,31,23,59,59,999999);
    dolphindb::ConstantSP nanotimeval = dolphindb::Util::createNanoTime(23,59,59,999999999);
    dolphindb::ConstantSP nanotimestampval = dolphindb::Util::createNanoTimestamp(2025,12,31,23,59,59,999999999);

    std::vector<dolphindb::ConstantSP> vals = {boolval,charval,intval,shortval,longval,floatval,doubleval,stringval,symbolval,blobval,int128val,\
                                ipval,uuidval,dateval,monthval,timeval,minuteval,secondval,datetimeval,timestampval,nanotimeval,nanotimestampval};
    std::vector<std::string> names = {"boolval","charval","intval","shortval","longval","floatval","doubleval","stringval","symbolval","blobval","int128val",\
                                "ipval","uuidval","dateval","monthval","timeval","minuteval","secondval","datetimeval","timestampval","nanotimeval","nanotimestampval"};
    conn.upload(names,vals);

    ASSERT_TRUE(conn.run("eqObj(boolval,bool("+boolval->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(charval,"+charval->getString()+"c)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(intval,"+intval->getString()+")")->getBool());
    ASSERT_TRUE(conn.run("eqObj(shortval,"+shortval->getString()+")")->getBool());
    ASSERT_TRUE(conn.run("eqObj(longval,"+longval->getString()+")")->getBool());
    ASSERT_TRUE(conn.run("eqObj(floatval,float("+floatval->getString()+"), 4)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(doubleval,double("+doubleval->getString()+"), 5)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(stringval,\""+stringval->getString()+"\")")->getBool());
    ASSERT_TRUE(conn.run("eqObj(symbolval,\""+symbolval->getString()+"\")")->getBool());
    ASSERT_TRUE(conn.run("eqObj(blobval,\""+blobval->getString()+"\")")->getBool());
    ASSERT_TRUE(conn.run("eqObj(int128val,int128('"+int128val->getString()+"'))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(ipval,ipaddr('"+ipval->getString()+"'))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(uuidval,uuid('"+uuidval->getString()+"'))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(dateval,date("+dateval->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(monthval,month("+monthval->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(timeval,time("+timeval->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(minuteval,minute("+minuteval->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(secondval,second("+secondval->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(datetimeval,datetime("+datetimeval->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(timestampval,timestamp("+timestampval->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(nanotimeval,nanotime("+nanotimeval->getString()+"))")->getBool());
    ASSERT_TRUE(conn.run("eqObj(nanotimestampval,nanotimestamp("+nanotimestampval->getString()+"))")->getBool());
}

TEST_F(ScalarTest,testGuid){
    unsigned char uuid[16];
    for (auto i = 0; i < 16; i++) {
        uuid[i] = rand() % 256;
    }

    dolphindb::Guid uuidval(uuid);
    const std::string erruuid = "5d212a78=cc48-e3b1-4235-b4d91473ee87";
    const std::string erruuid1 ="erruuid";
    ASSERT_ANY_THROW(dolphindb::Guid err=dolphindb::Guid(erruuid));
    ASSERT_ANY_THROW(dolphindb::Guid err=dolphindb::Guid(erruuid1));
    ASSERT_FALSE(uuidval.isZero());
    ASSERT_TRUE(dolphindb::Guid(dolphindb::Util::createNullConstant(dolphindb::DT_UUID)->getString()).isZero());

    dolphindb::Guid uuidval0 = dolphindb::Guid((const std::string)"0a0a0a0a-0a0a-0a0a-0a0a-0a0a0a0a0a0a");
    dolphindb::Guid uuidval2 = dolphindb::Guid((const std::string)"00000000-0000-0000-0000-000000000000");
    dolphindb::Guid uuidval3 = dolphindb::Guid((const std::string)"c8c8c8c8-c8c8-c8c8-c8c8-c8c8c8c8c8c8");
    ASSERT_EQ(dolphindb::Guid::getString(uuid),dolphindb::Util::parseConstant(dolphindb::DT_UUID,uuidval.getString())->getString());
    ASSERT_TRUE(uuidval.operator==(uuidval));
    ASSERT_FALSE(uuidval.operator!=(uuidval));
    ASSERT_TRUE(uuidval0.operator>=(uuidval2));
    ASSERT_TRUE(uuidval0.operator<=(uuidval3));
    ASSERT_TRUE(uuidval0.operator>(uuidval2));
    ASSERT_TRUE(uuidval0.operator<(uuidval3));
    ASSERT_EQ(uuidval0.compare(uuidval0),0);
    ASSERT_EQ(uuidval0.compare(uuidval2),1);
    ASSERT_EQ(uuidval0.compare(uuidval3),-1);

    dolphindb::GuidHash(ghash);
}

TEST_F(ScalarTest,test_decimal32){
    dolphindb::ConstantSP decimal32_normal = conn.run("a=3.1626$DECIMAL32(8);a");
    dolphindb::ConstantSP decimal32_null = conn.run("b=NULL$DECIMAL32(8);b");
    ASSERT_ANY_THROW(dolphindb::ConstantSP decimalval1 = dolphindb::Util::createDecimal32(-1, 3.1626));
    ASSERT_ANY_THROW(dolphindb::ConstantSP decimalval1 = dolphindb::Util::createDecimal32(0, 10000000000)); //scale + value's places must less than 11
    dolphindb::ConstantSP d0 = dolphindb::Util::createConstant(dolphindb::DT_DECIMAL32, 0);
    ASSERT_ANY_THROW(d0->setString("1000000000.0")); // scale + value's places must less than 11
    ASSERT_ANY_THROW(d0->setString("-1000000000.0")); // scale + value's places must less than 11
    ASSERT_ANY_THROW(dolphindb::ConstantSP decimalval1 = dolphindb::Util::createDecimal32(10, 3.1626)); //scale + value's places must less than 11
    ASSERT_ANY_THROW(dolphindb::ConstantSP decimalval1 = dolphindb::Util::createDecimal32(9, 3.1626));
    dolphindb::ConstantSP decimalval1 = dolphindb::Util::createDecimal32(8, 3.1626);
    dolphindb::ConstantSP decimalval2 = dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL32, 8);

    ASSERT_EQ(decimalval1->getString(), "3.16260000");
    ASSERT_EQ(decimalval1->getString(), decimal32_normal->getString());
    ASSERT_EQ(decimalval1->getType(), dolphindb::DT_DECIMAL32);
    ASSERT_EQ(decimalval1->getType(), decimal32_normal->getType());
    ASSERT_EQ(decimalval1->getRawType(), dolphindb::DT_DECIMAL32);
    ASSERT_EQ(decimalval1->getRawType(), decimal32_normal->getRawType());

    ASSERT_EQ(decimalval2->getString(), "");
    ASSERT_EQ(decimalval2->getString(), decimal32_null->getString());
    ASSERT_EQ(decimalval2->getType(), dolphindb::DT_DECIMAL32);
    ASSERT_EQ(decimalval2->getType(), decimal32_null->getType());
    ASSERT_EQ(decimalval2->getRawType(), dolphindb::DT_DECIMAL32);
    ASSERT_EQ(decimalval2->getRawType(), decimal32_null->getRawType());
    ASSERT_TRUE(decimalval2->isNull());

    ASSERT_EQ(dolphindb::Util::getCategoryString(decimalval1->getCategory()), "DENARY");
    std::vector<std::string> names = {"decimalval1", "decimalval2"};
    std::vector<dolphindb::ConstantSP> vals = {decimalval1, decimalval2};
    conn.upload(names, vals);
    ASSERT_TRUE(conn.run("eqObj(a,decimalval1)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(b,decimalval2)")->getBool());

    srand(time(NULL));
    // 随机10000个数据和scale进行断言
    std::random_device rd;  // 随机设备，用于获取种子
    std::mt19937 gen(rd()); // Mersenne Twister 19937 生成器，种子初始化

    // 创建分布对象，指定浮点数范围
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    for(auto i=0;i<1000;i++){
        std::string data = std::to_string(dist(gen));
        int scale = rand() % 10;
        dolphindb::ConstantSP ex = conn.run("data=`"+ data +";"
                                "scale="+std::to_string(scale)+";go;"
                                "d=data$DECIMAL32(scale);d");
        dolphindb::ConstantSP res = dolphindb::Util::createConstant(dolphindb::DT_DECIMAL32, scale);
        // std::cout<< "data: "<< data<< ", scale: "<<scale<<std::endl;
        res->setString(data);
        ASSERT_DOUBLE_EQ(ex->getDouble(), res->getDouble());
        conn.upload("res", res);
        ASSERT_EQ(conn.run("eqFloat(res, d, "+std::to_string(scale)+")")->getBool(), true);
    }

    decimalval1->setDouble(1.2);
    ASSERT_EQ(decimalval1->getDouble(), (double)1.2);
    decimalval2->setFloat(1.223f);
    ASSERT_EQ(decimalval2->getFloat(), (float)1.223);
    decimalval2->setString("3.2561");
    ASSERT_EQ(decimalval2->getString(), "3.25610000");
    decimalval1->setNull();
    ASSERT_TRUE(decimalval1->isNull());

    decimalval1->assign(dolphindb::Util::createDecimal32(2,1.34567));
    ASSERT_EQ(decimalval1->getString(),"1.34000000");
    ASSERT_FALSE(decimalval1->assign(dolphindb::Util::createDecimal32(0,-1000000)));
    ASSERT_FALSE(decimalval1->assign(dolphindb::Util::createDecimal32(0,1000000)));
    decimalval1->assign(dolphindb::Util::createDecimal32(8,2.3641553));
    ASSERT_EQ(decimalval1->getString(),"2.36415530");
    ASSERT_FALSE(decimalval1->assign(dolphindb::Util::createString("11.111111111111111111")));
    decimalval1->assign(dolphindb::Util::createString("1.11111"));
    ASSERT_EQ(decimalval1->getString(),"1.11111000");
    ASSERT_FALSE(decimalval1->assign(dolphindb::Util::createDate(10000)));
    decimalval1->assign(dolphindb::Util::createShort(3));
    ASSERT_EQ(decimalval1->getString(),"3.00000000");
    decimalval1->assign(dolphindb::Util::createInt(1));
    ASSERT_EQ(decimalval1->getString(),"1.00000000");
    decimalval1->assign(dolphindb::Util::createLong(2));
    ASSERT_EQ(decimalval1->getString(),"2.00000000");
    decimalval1->assign(dolphindb::Util::createFloat((float)3.1315));
    ASSERT_NEAR(decimalval1->getFloat(),(float)3.1315, 4);
    decimalval1->assign(dolphindb::Util::createDouble(-3.131544));
    ASSERT_EQ(decimalval1->getString(),"-3.13154400");

    decimalval1->assign(dolphindb::Util::createInt(1));
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createInt(0)),1);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createInt(2)),-1);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createInt(1)),0);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createDecimal32(3,0.99)),1);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createDecimal32(2,2.01)),-1);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createDecimal32(1,1)),0);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createDecimal64(8,0.999999999)),1);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createDecimal64(10,2.00001)),-1);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createDecimal64(1,1)),0);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL64)), 1);
    ASSERT_EQ(decimalval1->compare(0, dolphindb::Util::createFloat(1.001)), -1);
    ASSERT_EQ(decimalval1->compare(0, dolphindb::Util::createDouble(0.99)), 1);
    ASSERT_EQ(decimalval1->compare(0, dolphindb::Util::createDouble(1)), 0);
    ASSERT_ANY_THROW(decimalval1->compare(0,dolphindb::Util::createBlob("blob1")));

    dolphindb::ConstantSP decimalval3 = dolphindb::Util::createDecimal32(0, 100);
    decimalval3->setDouble(-1000000000);
    ASSERT_ANY_THROW(decimalval3->setString("1000000000"));
    ASSERT_EQ(decimalval3->getString(), "-1000000000");
    ASSERT_EQ(decimalval3->getInstance()->getString(),decimalval3->getString());

    ASSERT_EQ(decimalval3->getExtraParamForType(),0);
    ASSERT_FALSE(decimalval3->isNull());
    decimalval3->setNull();
    ASSERT_EQ(decimalval3->getString(),"");
    ASSERT_TRUE(decimalval3->isNull());

    ASSERT_EQ(decimalval3->compare(0, dolphindb::Util::createNullConstant(dolphindb::DT_INT)), 0);
    ASSERT_EQ(decimalval3->compare(0, dolphindb::Util::createFloat(1.234)), -1);
}

TEST_F(ScalarTest,test_decimal64){
    dolphindb::ConstantSP decimal64_normal = conn.run("a=13.1626$DECIMAL64(16);a");
    dolphindb::ConstantSP decimal64_null = conn.run("b=NULL$DECIMAL64(16);b");
    ASSERT_ANY_THROW(dolphindb::ConstantSP decimalval1 = dolphindb::Util::createDecimal64(-1, 13.1626));
    ASSERT_ANY_THROW(dolphindb::ConstantSP decimalval1 = dolphindb::Util::createDecimal64(0, 10000000000000000000.0)); //scale + value's places must less than 20
    dolphindb::ConstantSP d0 = dolphindb::Util::createConstant(dolphindb::DT_DECIMAL64, 0);
    ASSERT_ANY_THROW(d0->setString("1000000000000000000.0")); // scale + value's places must less than 20
    ASSERT_ANY_THROW(d0->setString("-1000000000000000000.0")); // scale + value's places must less than 20
    ASSERT_ANY_THROW(dolphindb::ConstantSP decimalval1 = dolphindb::Util::createDecimal64(19, 13.1626)); //scale + value's places must less than 20
    ASSERT_ANY_THROW(dolphindb::ConstantSP decimalval1 = dolphindb::Util::createDecimal64(18, 13.1626));
    dolphindb::ConstantSP decimalval1 = dolphindb::Util::createDecimal64(16, 13.1626);
    dolphindb::ConstantSP decimalval2 = dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL64, 16);

    ASSERT_EQ(decimalval1->getString(), "13.1626000000000000");
    ASSERT_EQ(decimalval1->getString(), decimal64_normal->getString());
    ASSERT_EQ(decimalval1->getType(), dolphindb::DT_DECIMAL64);
    ASSERT_EQ(decimalval1->getType(), decimal64_normal->getType());
    ASSERT_EQ(decimalval1->getRawType(), dolphindb::DT_DECIMAL64);
    ASSERT_EQ(decimalval1->getRawType(), decimal64_normal->getRawType());

    ASSERT_EQ(decimalval2->getString(), "");
    ASSERT_EQ(decimalval2->getString(), decimal64_null->getString());
    ASSERT_EQ(decimalval2->getType(), dolphindb::DT_DECIMAL64);
    ASSERT_EQ(decimalval2->getType(), decimal64_null->getType());
    ASSERT_EQ(decimalval2->getRawType(), dolphindb::DT_DECIMAL64);
    ASSERT_EQ(decimalval2->getRawType(), decimal64_null->getRawType());
    ASSERT_TRUE(decimalval2->isNull());

    ASSERT_EQ(dolphindb::Util::getCategoryString(decimalval1->getCategory()), "DENARY");
    std::vector<std::string> names = {"decimalval1", "decimalval2"};
    std::vector<dolphindb::ConstantSP> vals = {decimalval1, decimalval2};
    conn.upload(names, vals);
    ASSERT_TRUE(conn.run("eqObj(a,decimalval1)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(b,decimalval2)")->getBool());

    srand(time(NULL));
    // 随机10000个数据和scale进行断言
    std::random_device rd;  // 随机设备，用于获取种子
    std::mt19937 gen(rd()); // Mersenne Twister 19937 生成器，种子初始化

    // 创建分布对象，指定浮点数范围
    std::uniform_real_distribution<double> dist(0.0, 1000.0);
    for(auto i=0;i<1000;i++){
        std::string data = std::to_string(dist(gen));
        int scale = rand() % 16;
        // std::cout<< "data: "<< data<< ", scale: "<<scale<<std::endl;
        dolphindb::ConstantSP ex = conn.run("data=`"+ data +";"
                                "scale="+std::to_string(scale)+";go;"
                                "d=data$DECIMAL64(scale);d");
        dolphindb::ConstantSP res = dolphindb::Util::createConstant(dolphindb::DT_DECIMAL64, scale);
        res->setString(data);
        ASSERT_DOUBLE_EQ(ex->getDouble(), res->getDouble());
        conn.upload("res", res);
        ASSERT_EQ(conn.run("eqFloat(res, d, "+std::to_string(scale)+")")->getBool(), true);
    }

    decimalval1->setDouble(1.2);
    ASSERT_EQ(decimalval1->getDouble(), (double)1.2);
    decimalval2->setFloat(1.223f);
    ASSERT_EQ(decimalval2->getFloat(), (float)1.223);
    decimalval2->setString("3.256164354978934568721652635");
    ASSERT_EQ(decimalval2->getString(), "3.2561643549789346");
    decimalval1->setNull();
    ASSERT_TRUE(decimalval1->isNull());

    decimalval2->assign(dolphindb::Util::createDecimal64(2,1.34567));
    ASSERT_EQ(decimalval2->getString(),"1.3400000000000000");
    ASSERT_FALSE(decimalval1->assign(dolphindb::Util::createDecimal64(0,-10000000000000000)));
    ASSERT_FALSE(decimalval1->assign(dolphindb::Util::createDecimal64(0,10000000000000000)));
    decimalval2->assign(dolphindb::Util::createDecimal64(17,2.3641553));
    ASSERT_FLOAT_EQ(decimalval2->getFloat(), float(2.36415530000000000)); // result is 2.36415530000000064
    ASSERT_FALSE(decimalval1->assign(dolphindb::Util::createString("111.1")));
    ASSERT_FALSE(decimalval2->assign(dolphindb::Util::createBlob("111.1")));
    decimalval1->assign(dolphindb::Util::createString("1.11111"));
    decimalval2->assign(dolphindb::Util::createBlob("1.11111"));
    ASSERT_EQ(decimalval1->getString(),"1.1111100000000000");
    ASSERT_EQ(decimalval2->getString(),"1.1111100000000000");
    ASSERT_FALSE(decimalval1->assign(dolphindb::Util::createDate(10000)));
    decimalval1->assign(dolphindb::Util::createShort(3));
    ASSERT_EQ(decimalval1->getString(),"3.0000000000000000");
    decimalval2->assign(dolphindb::Util::createInt(1));
    ASSERT_EQ(decimalval2->getString(),"1.0000000000000000");
    decimalval2->assign(dolphindb::Util::createLong(2));
    ASSERT_EQ(decimalval2->getString(),"2.0000000000000000");
    decimalval1->assign(dolphindb::Util::createFloat((float)3.1315));
    decimalval2->assign(dolphindb::Util::createFloat((float)3.1315));
    ASSERT_NEAR(decimalval1->getFloat(),(float)3.1315, 4);
    ASSERT_NEAR(decimalval2->getFloat(),(float)3.1315, 4);
    decimalval2->assign(dolphindb::Util::createDouble(-3.131544));
    ASSERT_EQ(decimalval2->getString(),"-3.1315440000000000");

    decimalval1->assign(dolphindb::Util::createInt(1));
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createInt(0)),1);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createInt(2)),-1);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createInt(1)),0);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createDecimal64(13,0.99)),1);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createDecimal64(12,2.01)),-1);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createDecimal64(11,1)),0);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createDecimal64(18,0.999999999)),1);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createDecimal64(10,2.00001)),-1);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createDecimal64(11,1)),0);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL64)), 1);
    ASSERT_EQ(decimalval1->compare(0, dolphindb::Util::createFloat(1.001)), -1);
    ASSERT_EQ(decimalval1->compare(0, dolphindb::Util::createDouble(0.99)), 1);
    ASSERT_EQ(decimalval1->compare(0, dolphindb::Util::createDouble(1)), 0);
    ASSERT_ANY_THROW(decimalval1->compare(0,dolphindb::Util::createBlob("blob1")));

    dolphindb::ConstantSP decimalval4 = dolphindb::Util::createDecimal64(0, 100);
    decimalval4->setDouble(-1000000000000000000);
    ASSERT_ANY_THROW(decimalval4->setString("1000000000000000000"));
    ASSERT_EQ(decimalval4->getString(), "-1000000000000000000");
    ASSERT_EQ(decimalval4->getValue()->getString(),decimalval4->getString());

    ASSERT_FALSE(decimalval4->isNull());
    decimalval4->setNull();
    ASSERT_EQ(decimalval4->getString(),"");
    ASSERT_TRUE(decimalval4->isNull());
    ASSERT_EQ(decimalval4->compare(0, dolphindb::Util::createNullConstant(dolphindb::DT_INT)), 0);
    ASSERT_EQ(decimalval4->compare(0, dolphindb::Util::createFloat(1.234)), -1);
}

TEST_F(ScalarTest,test_decimal128){
    dolphindb::ConstantSP decimal128_normal = conn.run("a=13.1626$DECIMAL128(26);a");
    dolphindb::ConstantSP decimal128_null = conn.run("b=NULL$DECIMAL128(26);b");
    ASSERT_ANY_THROW(dolphindb::ConstantSP decimalval1 = dolphindb::Util::createDecimal128(-1, 13.1626));
    dolphindb::ConstantSP d0 = dolphindb::Util::createConstant(dolphindb::DT_DECIMAL128, 0);
    ASSERT_ANY_THROW(d0->setString("100000000000000000000000000000000000000.0")); // scale + value's places must less than 40
    ASSERT_ANY_THROW(d0->setString("-100000000000000000000000000000000000000.0")); // scale + value's places must less than 40
    ASSERT_ANY_THROW(dolphindb::ConstantSP decimalval1 = dolphindb::Util::createDecimal128(38, 13.1626)); // scale + value's places must less than 40
    ASSERT_ANY_THROW(dolphindb::ConstantSP decimalval1 = dolphindb::Util::createDecimal128(37, 113.1626));
    dolphindb::ConstantSP decimalval1 = dolphindb::Util::createDecimal128(26, 13.1626);
    dolphindb::ConstantSP decimalval2 = dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL128, 26);

    ASSERT_DOUBLE_EQ(decimalval1->getDouble(), 13.1626);
    ASSERT_NEAR(decimalval1->getDouble(), decimal128_normal->getDouble(), 0.0001);
    ASSERT_EQ(decimalval1->getType(), dolphindb::DT_DECIMAL128);
    ASSERT_EQ(decimalval1->getType(), decimal128_normal->getType());
    ASSERT_EQ(decimalval1->getRawType(), dolphindb::DT_DECIMAL128);
    ASSERT_EQ(decimalval1->getRawType(), decimal128_normal->getRawType());

    ASSERT_EQ(decimalval2->getString(), "");
    ASSERT_TRUE(decimalval2->isNull());
    ASSERT_TRUE(decimal128_null->isNull());
    ASSERT_EQ(decimalval2->getString(), decimal128_null->getString());
    ASSERT_EQ(decimalval2->getType(), dolphindb::DT_DECIMAL128);
    ASSERT_EQ(decimalval2->getType(), decimal128_null->getType());
    ASSERT_EQ(decimalval2->getRawType(), dolphindb::DT_DECIMAL128);
    ASSERT_EQ(decimalval2->getRawType(), decimal128_null->getRawType());
    ASSERT_TRUE(decimalval2->isNull());

    ASSERT_EQ(dolphindb::Util::getCategoryString(decimalval1->getCategory()), "DENARY");
    std::vector<std::string> names = {"decimalval1", "decimalval2"};
    std::vector<dolphindb::ConstantSP> vals = {decimalval1, decimalval2};
    conn.upload(names, vals);
    ASSERT_TRUE(conn.run("eqObj(a,decimalval1)")->getBool());
    ASSERT_TRUE(conn.run("eqObj(b,decimalval2)")->getBool());

    srand(time(NULL));
    // 随机10000个数据和scale进行断言
    std::random_device rd;  // 随机设备，用于获取种子
    std::mt19937 gen(rd()); // Mersenne Twister 19937 生成器，种子初始化

    // 创建分布对象，指定浮点数范围
    std::uniform_real_distribution<double> dist(0.0, 1000000.0);
    for(auto i=0;i<1000;i++){
        std::string data = std::to_string(dist(gen));
        int scale = rand() % 33;
        // std::cout<< "data: "<< data<< ", scale: "<<scale<<std::endl;
        dolphindb::ConstantSP ex = conn.run("data=`"+ data +";"
                                "scale="+std::to_string(scale)+";go;"
                                "d=data$DECIMAL128(scale);d");
        dolphindb::ConstantSP res = dolphindb::Util::createConstant(dolphindb::DT_DECIMAL128, scale);
        res->setString(data);
        ASSERT_DOUBLE_EQ(ex->getDouble(), res->getDouble());
        conn.upload("res", res);
        ASSERT_EQ(conn.run("eqObj(res, d)")->getBool(), true);
    }

    decimalval1->setDouble(1.2);
    ASSERT_EQ(decimalval1->getDouble(), (double)1.2);
    decimalval2->setFloat(1.223f);
    ASSERT_EQ(decimalval2->getFloat(), (float)1.223);
    decimalval2->setString("3.256164354978934568721652635");
    ASSERT_EQ(decimalval2->getString(), "3.25616435497893456872165264");
    decimalval1->setNull();
    ASSERT_TRUE(decimalval1->isNull());

    decimalval2->assign(dolphindb::Util::createDecimal128(2,1.1));
    ASSERT_DOUBLE_EQ(decimalval2->getDouble(), 1.1);
    ASSERT_FALSE(decimalval1->assign(dolphindb::Util::createDecimal128(0,-1000000000000000000)));
    ASSERT_FALSE(decimalval1->assign(dolphindb::Util::createDecimal128(0,1000000000000000000)));
    decimalval2->assign(dolphindb::Util::createDecimal128(17,2.3641553));
    ASSERT_FLOAT_EQ(decimalval2->getFloat(), float(2.36415530000000000)); // result is 2.36415530000000064
    ASSERT_FALSE(decimalval1->assign(dolphindb::Util::createString("1111111111111.1")));
    ASSERT_FALSE(decimalval2->assign(dolphindb::Util::createBlob("1111111111111.1")));
    decimalval1->assign(dolphindb::Util::createString("2.11111"));
    decimalval2->assign(dolphindb::Util::createBlob("2.11111"));
    ASSERT_EQ(decimalval1->getString(),"2.11111000000000000000000000");
    ASSERT_EQ(decimalval2->getString(),"2.11111000000000000000000000");
    ASSERT_FALSE(decimalval1->assign(dolphindb::Util::createTimestamp(10000000000000000)));
    decimalval1->assign(dolphindb::Util::createShort(3));
    ASSERT_EQ(decimalval1->getString(),"3.00000000000000000000000000");
    decimalval2->assign(dolphindb::Util::createInt(1));
    ASSERT_EQ(decimalval2->getString(),"1.00000000000000000000000000");
    decimalval2->assign(dolphindb::Util::createLong(2));
    ASSERT_EQ(decimalval2->getString(),"2.00000000000000000000000000");
    decimalval1->assign(dolphindb::Util::createFloat((float)3.1315));
    decimalval2->assign(dolphindb::Util::createFloat((float)3.1315));
    ASSERT_NEAR(decimalval1->getFloat(),(float)3.1315, 4);
    ASSERT_NEAR(decimalval2->getFloat(),(float)3.1315, 4);
    decimalval2->assign(dolphindb::Util::createDouble(-3.131544));
    std::string re0 = decimalval2->getString();
    size_t re0_scale = re0.length() - re0.find(".") - 1;
    ASSERT_EQ(re0_scale, 26);
    double re0_val = stod(re0);
    ASSERT_NEAR(re0_val, -3.131544, 0.000001);

    decimalval1->assign(dolphindb::Util::createInt(1));
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createInt(0)),1);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createInt(2)),-1);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createInt(1)),0);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createDecimal128(13,0.99)),1);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createDecimal128(12,2.01)),-1);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createDecimal128(11,1)),0);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createDecimal128(18,0.999999999)),1);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createDecimal128(10,2.00001)),-1);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createDecimal128(11,1)),0);
    ASSERT_EQ(decimalval1->compare(0,dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL128)), 1);
    ASSERT_EQ(decimalval1->compare(0, dolphindb::Util::createFloat(1.001)), -1);
    ASSERT_EQ(decimalval1->compare(0, dolphindb::Util::createDouble(0.99)), 1);
    ASSERT_EQ(decimalval1->compare(0, dolphindb::Util::createDouble(1)), 0);
    ASSERT_ANY_THROW(decimalval1->compare(0,dolphindb::Util::createBlob("blob1")));

    dolphindb::ConstantSP decimalval4 = dolphindb::Util::createDecimal128(0, 100);
    decimalval4->setString("-10000000000000000000000000000");
    ASSERT_ANY_THROW(decimalval4->setString("10000000000000000000000000000111111111111"));
    ASSERT_EQ(decimalval4->getString(), "-10000000000000000000000000000");
    ASSERT_EQ(decimalval4->getValue()->getString(),decimalval4->getString());

    ASSERT_FALSE(decimalval4->isNull());
    decimalval4->setNull();
    ASSERT_EQ(decimalval4->getString(),"");
    ASSERT_TRUE(decimalval4->isNull());

    ASSERT_EQ(decimalval4->compare(0, dolphindb::Util::createNullConstant(dolphindb::DT_INT)), 0);
    ASSERT_EQ(decimalval4->compare(0, dolphindb::Util::createFloat(1.234)), -1);
}

TEST_F(ScalarTest, test_convertType_decimal32){
    dolphindb::ConstantSP dcmVal = dolphindb::Util::createDecimal32(5, 1.6612348486);
    ASSERT_EQ(dcmVal->getString(), "1.66123");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setDouble(-0.3456);
    ASSERT_DOUBLE_EQ(dcmVal->getDouble(), -0.3456);
    ASSERT_EQ(dcmVal->getString(), "-0.34560");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("987.213");
    ASSERT_EQ(dcmVal->getString(), "987.21300");
    std::cout<< dcmVal->getString()<<std::endl;

    const char* str = "+0.9998495";
    dcmVal->setString(str);
    ASSERT_EQ(dcmVal->getString(), "0.99985");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("-*0.9998495");
    ASSERT_EQ(dcmVal->getString(), "0.00000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("*123456");
    ASSERT_EQ(dcmVal->getString(), "");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("abc");
    ASSERT_EQ(dcmVal->getString(), "");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("-abc");
    ASSERT_EQ(dcmVal->getString(), "0.00000");
    std::cout<< dcmVal->getString()<<std::endl;  

    dcmVal->setString(".3");
    ASSERT_EQ(dcmVal->getString(), "0.30000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("-.3");
    ASSERT_EQ(dcmVal->getString(), "-0.30000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("\n9876.321");
    ASSERT_EQ(dcmVal->getString(), "");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString(" 9876.321");
    ASSERT_EQ(dcmVal->getString(), "9876.32100");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("\t9876.321");
    ASSERT_EQ(dcmVal->getString(), "9876.32100");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("\r9876.321");
    ASSERT_EQ(dcmVal->getString(), "");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("9876.321  \n\t");
    ASSERT_EQ(dcmVal->getString(), "9876.32100");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("9876.321\t\t");
    ASSERT_EQ(dcmVal->getString(), "9876.32100");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("-\n9876.321");
    ASSERT_EQ(dcmVal->getString(), "0.00000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("- 9876.321");
    ASSERT_EQ(dcmVal->getString(), "-9876.32100");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("-\t9876.321");
    ASSERT_EQ(dcmVal->getString(), "-9876.32100");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("-\r9876.321");
    ASSERT_EQ(dcmVal->getString(), "0.00000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("98 76.321");
    ASSERT_EQ(dcmVal->getString(), "9876.32100");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("98\t76.321");
    ASSERT_EQ(dcmVal->getString(), "9876.32100");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("98\n76.321");
    ASSERT_EQ(dcmVal->getString(), "98.00000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("-9876.321  \n\t");
    ASSERT_EQ(dcmVal->getString(), "-9876.32100");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("-9876.321\t\t");
    ASSERT_EQ(dcmVal->getString(), "-9876.32100");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("0000.321\t\t");
    ASSERT_EQ(dcmVal->getString(), "0.32100");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("1111\t");
    ASSERT_EQ(dcmVal->getString(), "1111.00000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("1234\n");
    ASSERT_EQ(dcmVal->getString(), "1234.00000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("1111 ");
    ASSERT_EQ(dcmVal->getString(), "1111.00000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("3.*");
    ASSERT_EQ(dcmVal->getString(), "3.00000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("5.abc");
    ASSERT_EQ(dcmVal->getString(), "5.00000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("4.\n");
    ASSERT_EQ(dcmVal->getString(), "4.00000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("123.456\t");
    ASSERT_EQ(dcmVal->getString(), "123.45600");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("-123.456  ");
    ASSERT_EQ(dcmVal->getString(), "-123.45600");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setBool(false);
    ASSERT_EQ(dcmVal->getString(), "0.00000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setChar('\1');
    ASSERT_EQ(dcmVal->getString(), "1.00000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setFloat(-1.1);
    ASSERT_EQ(dcmVal->getString(), "-1.10000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setInt(1234);
    ASSERT_EQ(dcmVal->getString(), "1234.00000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setLong(9999l);
    ASSERT_EQ(dcmVal->getString(), "9999.00000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setShort(10000);
    ASSERT_EQ(dcmVal->getString(), "10000.00000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setNull();
    ASSERT_EQ(dcmVal->getString(), "");
    ASSERT_TRUE(dcmVal->isNull());
    std::cout<< dcmVal->getString()<<std::endl;

}

TEST_F(ScalarTest, test_convertType_decimal64){
    dolphindb::ConstantSP dcmVal = dolphindb::Util::createDecimal64(16, 1.661);
    ASSERT_EQ(dcmVal->getString(), "1.6610000000000000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setDouble(-0.3456);
    ASSERT_DOUBLE_EQ(dcmVal->getDouble(), -0.3456);
    ASSERT_EQ(dcmVal->getString(), "-0.3456000000000000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("0.21364538294533664856788");
    ASSERT_EQ(dcmVal->getString(), "0.2136453829453366");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setBool(false);
    ASSERT_EQ(dcmVal->getString(), "0.0000000000000000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setChar('\1');
    ASSERT_EQ(dcmVal->getString(), "1.0000000000000000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setFloat(-1.1);
    std::string re0 = dcmVal->getString();
    size_t re0_scale = re0.length() - re0.find(".") - 1;
    ASSERT_EQ(re0_scale, 16);
    double re0_val = stod(re0);
    ASSERT_NEAR(re0_val, -1.1, 0.1f);
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setInt(1);
    ASSERT_EQ(dcmVal->getString(), "1.0000000000000000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setLong(-1ll);
    ASSERT_EQ(dcmVal->getString(), "-1.0000000000000000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setShort(1);
    ASSERT_EQ(dcmVal->getString(), "1.0000000000000000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setNull();
    ASSERT_EQ(dcmVal->getString(), "");
    ASSERT_TRUE(dcmVal->isNull());
    std::cout<< dcmVal->getString()<<std::endl;

}

TEST_F(ScalarTest, test_convertType_decimal128){
    dolphindb::ConstantSP dcmVal = dolphindb::Util::createDecimal128(26, -0.53);
    std::string re0 = dcmVal->getString();
    size_t re0_scale = re0.length() - re0.find(".") - 1;
    ASSERT_EQ(re0_scale, 26);
    double re0_val = stod(re0);
    ASSERT_NEAR(re0_val, -0.53, 0.01f);
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setDouble(8.3456);
    ASSERT_DOUBLE_EQ(dcmVal->getDouble(), 8.3456);
    re0 = dcmVal->getString();
    re0_scale = re0.length() - re0.find(".") - 1;
    ASSERT_EQ(re0_scale, 26);
    re0_val = stod(re0);
    ASSERT_NEAR(re0_val, 8.3456, 0.0001);
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setString("987.213649237905538612254387602358");
    ASSERT_EQ(dcmVal->getString(), "987.21364923790553861225438760");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setBool(false);
    ASSERT_EQ(dcmVal->getString(), "0.00000000000000000000000000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setChar('\1');
    ASSERT_EQ(dcmVal->getString(), "1.00000000000000000000000000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setFloat(-1.1);
    re0 = dcmVal->getString();
    re0_scale = re0.length() - re0.find(".") - 1;
    ASSERT_EQ(re0_scale, 26);
    re0_val = stod(re0);
    ASSERT_NEAR(re0_val, -1.1, 0.1f);
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setInt(1);
    ASSERT_EQ(dcmVal->getString(), "1.00000000000000000000000000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setLong(-1ll);
    ASSERT_EQ(dcmVal->getString(), "-1.00000000000000000000000000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setShort(1);
    ASSERT_EQ(dcmVal->getString(), "1.00000000000000000000000000");
    std::cout<< dcmVal->getString()<<std::endl;

    dcmVal->setNull();
    ASSERT_EQ(dcmVal->getString(), "");
    ASSERT_TRUE(dcmVal->isNull());
    std::cout<< dcmVal->getString()<<std::endl;

}


TEST_F(ScalarTest,testScalarFunction){

    srand(time(NULL));
    int buckets=2;
    char *buf0 = new char[2];
    char *buf00 = new char[2];
    char *buf = new char[2];
    short *buf1 = new short[2];
    int *buf2 = new int[2];
    dolphindb::INDEX *buf4 = new dolphindb::INDEX[2];
    long long *buf3 = new long long[2];
    float *buf5 = new float[2];
    double *buf6 = new double[2];
    std::string **buf8 = new std::string*[2];
    char **buf9 = new char*[24];
    unsigned char *buf10 = new unsigned char[2];
    dolphindb::SymbolBase *symbase= new dolphindb::SymbolBase(1);
    std::string exstr = "";

    std::cout<<"-----------------void-------------------"<<std::endl;
    dolphindb::ConstantSP voidval=dolphindb::Util::createConstant(dolphindb::DT_VOID);
    voidval->setBool(1);
    voidval->setChar(1);
    voidval->setShort(1);
    voidval->setInt(1);
    voidval->setLong(1);
    voidval->setIndex(1);
    voidval->setFloat(1);
    ASSERT_TRUE(voidval->isNull());
    voidval->setNull();

    voidval->nullFill(dolphindb::Util::createInt(1));
    ASSERT_TRUE(voidval->isNull());

    voidval->setBool(1,1);
    voidval->setChar(1,1);
    voidval->setShort(1,1);
    voidval->setInt(1,1);
    voidval->setLong(1,1);
    voidval->setIndex(1,1);
    voidval->setFloat(1,1);
    ASSERT_TRUE(voidval->isNull());
    voidval->setNull(1);
    voidval->validate(); // nothing to do.
    ASSERT_FALSE(voidval->reshape(2,2));
    ASSERT_FALSE(voidval->add(0, 1, double(1.3123)));
    ASSERT_FALSE(voidval->add(0, 1,(long long)1));
    ASSERT_EQ(voidval->compare(0, dolphindb::Util::createNullConstant(dolphindb::DT_VOID)),0);

    ASSERT_TRUE(voidval->hasNull());
    ASSERT_TRUE(voidval->hasNull(0,1));
    ASSERT_FALSE(voidval->sizeable());
    ASSERT_TRUE(voidval->copyable());
    ASSERT_EQ(voidval->itemCount(),1);
    ASSERT_EQ(voidval->uncompressedRows(),1);
    bool flag = false;
    ASSERT_FALSE(voidval->releaseMemory((long long)1, flag));
    // ASSERT_EQ(voidval->getAllocatedMemory(), 0);


    ASSERT_TRUE(voidval->isNull(0,1,buf0));
    ASSERT_TRUE(voidval->isValid(0,1,buf0));
    ASSERT_TRUE(voidval->getBool(0,1,buf00));
    ASSERT_TRUE(voidval->getChar(0,1,buf));
    ASSERT_TRUE(voidval->getShort(0,1,buf1));
    ASSERT_TRUE(voidval->getInt(0,1,buf2));
    ASSERT_TRUE(voidval->getLong(0,1,buf3));
    ASSERT_TRUE(voidval->getIndex(0,1,buf4));
    ASSERT_TRUE(voidval->getFloat(0,1,buf5));
    ASSERT_TRUE(voidval->getDouble(0,1,buf6));
    ASSERT_FALSE(voidval->getSymbol(0,1,buf2,symbase,false));
    ASSERT_TRUE(voidval->getString(0,1,buf8));
    ASSERT_FALSE(voidval->getString(0,1,buf9));
    ASSERT_TRUE(voidval->getBinary(0,1,1,buf10));
    ASSERT_FALSE(voidval->getHash(0,1,buckets,buf2));
    std::cout<<voidval->getAllocatedMemory();
    ASSERT_EQ(voidval->getBoolConst(0,1,buf)[0], CHAR_MIN);
    ASSERT_EQ(voidval->getCharConst(0,1,buf)[0], CHAR_MIN);
    ASSERT_EQ(voidval->getShortConst(0,1,buf1)[0], SHRT_MIN);
    ASSERT_EQ(voidval->getIntConst(0,1,buf2)[0], INT_MIN);
    ASSERT_EQ(voidval->getLongConst(0,1,buf3)[0], LLONG_MIN);
    ASSERT_EQ(voidval->getIndexConst(0,1,buf4)[0], INT_MIN);
    ASSERT_EQ(voidval->getFloatConst(0,1,buf5)[0], dolphindb::FLT_NMIN);
    ASSERT_EQ(voidval->getDoubleConst(0,1,buf6)[0], dolphindb::DBL_NMIN);
    ASSERT_ANY_THROW(voidval->getSymbolConst(0,1,buf2,symbase,false));
    ASSERT_EQ(*(voidval->getStringConst(0,1,buf8)[0]),"");
    ASSERT_ANY_THROW(voidval->getStringConst(0,1,buf9));
    ASSERT_ANY_THROW(voidval->getBinaryConst(0,1,1,buf10));

    ASSERT_EQ(voidval->getBoolBuffer(0,1,buf)[0], CHAR_MIN);
    ASSERT_EQ(voidval->getCharBuffer(0,1,buf)[0], CHAR_MIN);
    ASSERT_EQ(voidval->getShortBuffer(0,1,buf1)[0], SHRT_MIN);
    std::cout<<voidval->getIntBuffer(0,1,buf2)<<std::endl;
    ASSERT_EQ(buf2[0], INT_MIN);
    ASSERT_EQ(voidval->getLongBuffer(0,1,buf3)[0], LLONG_MIN);
    ASSERT_EQ(voidval->getIndexBuffer(0,1,buf4)[0], INT_MIN);
    ASSERT_EQ(voidval->getFloatBuffer(0,1,buf5)[0], dolphindb::FLT_NMIN);
    ASSERT_EQ(voidval->getDoubleBuffer(0,1,buf6)[0], dolphindb::DBL_NMIN);

    ASSERT_EQ(voidval->getStringRef(), "");

    ASSERT_EQ(voidval->getInstance()->getString(), "");
    ASSERT_EQ(voidval->getValue()->getString(), "");
    ASSERT_EQ(voidval->getRawType(), dolphindb::DT_VOID);

    std::cout<<"-----------------bool-------------------"<<std::endl;
    dolphindb::ConstantSP boolval = dolphindb::Util::createBool(true);
    ASSERT_EQ(boolval->getRawType(), dolphindb::DT_BOOL);
    dolphindb::ConstantSP boolval1 = boolval->getInstance();
    boolval1->setBool(1);
    ASSERT_EQ(boolval1->getBool(), boolval1->getValue()->getBool());
    ASSERT_FALSE(boolval->add(0, 1, (long long)2));
    ASSERT_FALSE(boolval->add(0, 1, (float)2));


    std::cout<<"-----------------string-------------------"<<std::endl;
    dolphindb::ConstantSP stringval=dolphindb::Util::createString("this is a std::string scalar");
    ASSERT_TRUE(stringval->isNull(0,1,buf0));
    ASSERT_TRUE(stringval->isValid(0,1,buf0));
    ASSERT_FALSE(stringval->getBool(0,1,buf00));
    ASSERT_FALSE(stringval->getChar(0,1,buf));
    ASSERT_FALSE(stringval->getShort(0,1,buf1));
    ASSERT_FALSE(stringval->getInt(0,1,buf2));
    ASSERT_FALSE(stringval->getLong(0,1,buf3));
    ASSERT_FALSE(stringval->getIndex(0,1,buf4));
    ASSERT_FALSE(stringval->getFloat(0,1,buf5));
    ASSERT_FALSE(stringval->getDouble(0,1,buf6));
    ASSERT_FALSE(stringval->getSymbol(0,1,buf2,symbase,false));
    ASSERT_TRUE(stringval->getString(0,1,buf8));
    ASSERT_FALSE(stringval->getString(0,1,buf9));
    ASSERT_FALSE(stringval->getBinary(0,1,1,buf10));
    ASSERT_FALSE(stringval->getHash(0,1,buckets,buf2));
    std::cout<<stringval->getAllocatedMemory()<<std::endl;
    ASSERT_ANY_THROW(stringval->getBoolConst(0,1,buf)[0]);
    ASSERT_ANY_THROW(stringval->getCharConst(0,1,buf)[0]);
    ASSERT_ANY_THROW(stringval->getShortConst(0,1,buf1)[0]);
    ASSERT_ANY_THROW(stringval->getIntConst(0,1,buf2)[0]);
    ASSERT_ANY_THROW(stringval->getLongConst(0,1,buf3)[0]);
    ASSERT_ANY_THROW(stringval->getIndexConst(0,1,buf4)[0]);
    ASSERT_ANY_THROW(stringval->getFloatConst(0,1,buf5)[0]);
    ASSERT_ANY_THROW(stringval->getDoubleConst(0,1,buf6)[0]);
    ASSERT_ANY_THROW(stringval->getSymbolConst(0,1,buf2,symbase,false));
    ASSERT_EQ(*(stringval->getStringConst(0,1,buf8)[0]), "this is a std::string scalar");
    ASSERT_STREQ(*(stringval->getStringConst(0,24,buf9)), "this is a std::string scalar");
    ASSERT_ANY_THROW(stringval->getBinaryConst(0,1,1,buf10));

    stringval->getDataBuffer(0,1,buf8);
    ASSERT_EQ(*(buf8[0]), "this is a std::string scalar");

    ASSERT_ANY_THROW(stringval->getIndex());
    ASSERT_EQ(stringval->getStringRef(0), "this is a std::string scalar");
    dolphindb::ConstantSP strval2 = stringval->getInstance();
    strval2->setNull();
    strval2->nullFill(stringval);
    ASSERT_EQ(strval2->getString(), stringval->getValue()->getString());
    ASSERT_EQ(strval2->getRawType(), dolphindb::DT_STRING);
    ASSERT_EQ(strval2->compare(0, stringval), 0);

    std::cout<<"-----------------int-------------------"<<std::endl;
    dolphindb::ConstantSP intval=dolphindb::Util::createInt(100000);
    dolphindb::ConstantSP intNullval=dolphindb::Util::createNullConstant(dolphindb::DT_INT);
    std::vector<dolphindb::ConstantSP> intV = {intNullval, intval};
    for(auto &val:intV){
        if(val == intval){
            ASSERT_TRUE(val->isNull(0,1,buf0));
            ASSERT_FALSE(buf0[0]);
            ASSERT_TRUE(val->isValid(0,1,buf0));
            ASSERT_TRUE(val->getBool(0,1,buf00));
            ASSERT_TRUE(val->getChar(0,1,buf));
            ASSERT_TRUE(val->getShort(0,1,buf1));
            ASSERT_TRUE(val->getInt(0,1,buf2));
            ASSERT_TRUE(val->getLong(0,1,buf3));
            ASSERT_TRUE(val->getIndex(0,1,buf4));
            ASSERT_TRUE(val->getFloat(0,1,buf5));
            ASSERT_TRUE(val->getDouble(0,1,buf6));
            ASSERT_FALSE(val->getSymbol(0,1,buf2,symbase,false));
            ASSERT_FALSE(val->getString(0,1,buf8));
            ASSERT_FALSE(val->getString(0,1,buf9));
            ASSERT_FALSE(val->getBinary(0,1,1,buf10));
            ASSERT_FALSE(val->getHash(0,1,buckets,buf2));
            std::cout<<val->getAllocatedMemory();
            ASSERT_EQ(val->getBoolConst(0,1,buf)[0], 1);
            ASSERT_EQ(val->getCharConst(0,1,buf)[0], '\xA0');
            // ASSERT_EQ(val->getShortConst(0,1,buf1)[0], SHRT_MIN);
            ASSERT_EQ(val->getIntConst(0,1,buf2)[0], (int)100000);
            ASSERT_EQ(val->getLongConst(0,1,buf3)[0], (long long)100000);
            ASSERT_EQ(val->getIndexConst(0,1,buf4)[0], (dolphindb::INDEX)100000);
            ASSERT_EQ(val->getFloatConst(0,1,buf5)[0], (float)100000);
            ASSERT_EQ(val->getDoubleConst(0,1,buf6)[0], (double)100000);
            ASSERT_ANY_THROW(val->getSymbolConst(0,1,buf2,symbase,false));
            ASSERT_ANY_THROW(val->getStringConst(0,1,buf8)[0]);
            ASSERT_ANY_THROW(val->getStringConst(0,24,buf9));
            ASSERT_ANY_THROW(val->getBinaryConst(0,1,1,buf10));
        }
        else{
            ASSERT_TRUE(val->isNull(0,1,buf0));
            ASSERT_TRUE(buf0[0]);
            ASSERT_TRUE(val->isValid(0,1,buf0));
            ASSERT_TRUE(val->getBool(0,1,buf00));
            ASSERT_TRUE(val->getChar(0,1,buf));
            ASSERT_TRUE(val->getShort(0,1,buf1));
            ASSERT_TRUE(val->getInt(0,1,buf2));
            ASSERT_TRUE(val->getLong(0,1,buf3));
            ASSERT_TRUE(val->getIndex(0,1,buf4));
            ASSERT_TRUE(val->getFloat(0,1,buf5));
            ASSERT_TRUE(val->getDouble(0,1,buf6));
            ASSERT_FALSE(val->getSymbol(0,1,buf2,symbase,false));
            ASSERT_FALSE(val->getString(0,1,buf8));
            ASSERT_FALSE(val->getString(0,1,buf9));
            ASSERT_FALSE(val->getBinary(0,1,1,buf10));
            ASSERT_FALSE(val->getHash(0,1,buckets,buf2));
            std::cout<<val->getAllocatedMemory();
            ASSERT_EQ(val->getBoolConst(0,1,buf)[0], '\x80');
            ASSERT_EQ(val->getCharConst(0,1,buf)[0], '\x80');
            // ASSERT_EQ(val->getShortConst(0,1,buf1)[0], SHRT_MIN);
            ASSERT_EQ(val->getIntConst(0,1,buf2)[0], INT_MIN);
            ASSERT_EQ(val->getLongConst(0,1,buf3)[0], LLONG_MIN);
            ASSERT_EQ(val->getIndexConst(0,1,buf4)[0], dolphindb::INDEX_MIN);
            ASSERT_EQ(val->getFloatConst(0,1,buf5)[0], dolphindb::FLT_NMIN);
            ASSERT_EQ(val->getDoubleConst(0,1,buf6)[0], dolphindb::DBL_NMIN);
            ASSERT_ANY_THROW(val->getSymbolConst(0,1,buf2,symbase,false));
            ASSERT_ANY_THROW(val->getStringConst(0,1,buf8)[0]);
            ASSERT_ANY_THROW(val->getStringConst(0,24,buf9));
            ASSERT_ANY_THROW(val->getBinaryConst(0,1,1,buf10));
        }
    }

    ASSERT_EQ(intval->getBoolBuffer(0,1,buf)[0], '\xA0');
    ASSERT_EQ(intval->getCharBuffer(0,1,buf)[0], '\xA0');
    // ASSERT_EQ(intval->getShortBuffer(0,1,buf1)[0], SHRT_MIN);
    std::cout<<intval->getIntBuffer(0,1,buf2)<<std::endl;
    ASSERT_EQ(buf2[0], 100000);
    ASSERT_EQ(intval->getLongBuffer(0,1,buf3)[0], 100000);
    ASSERT_EQ(intval->getIndexBuffer(0,1,buf4)[0], 100000);
    ASSERT_EQ(intval->getFloatBuffer(0,1,buf5)[0], 100000);
    ASSERT_EQ(intval->getDoubleBuffer(0,1,buf6)[0], 100000);
    intval->getDataBuffer(0,1,buf2);
    ASSERT_EQ(buf2[0], 100000);

    intval->getBinaryBuffer(0,1,sizeof(int),buf10);
    dolphindb::ConstantSP intval2 = dolphindb::Util::createConstant(dolphindb::DT_INT);
    intval2->setBinary(buf10,sizeof(int));
    std::cout<<intval2->getString()<<std::endl;

    ASSERT_EQ(intval->getRawType(), dolphindb::DT_INT);
    dolphindb::ConstantSP intval1 = intval->getInstance();
    intval1->setInt(100000);
    ASSERT_EQ(intval1->getInt(), intval->getValue()->getInt());

    std::cout<<"-----------------char-------------------"<<std::endl;
    char rand_val = rand()%CHAR_MAX+1;
    dolphindb::ConstantSP charval=dolphindb::Util::createChar(rand_val);
    dolphindb::ConstantSP charNullval=dolphindb::Util::createNullConstant(dolphindb::DT_CHAR);
    std::vector<dolphindb::ConstantSP> charV = {charNullval, charval};
    for(auto &val:charV){
        if(val == charval){
            ASSERT_TRUE(val->isNull(0,1,buf0));
            ASSERT_TRUE(val->isValid(0,1,buf0));
            ASSERT_TRUE(val->getBool(0,1,buf00));
            ASSERT_TRUE(val->getChar(0,1,buf));
            ASSERT_TRUE(val->getShort(0,1,buf1));
            ASSERT_TRUE(val->getInt(0,1,buf2));
            ASSERT_TRUE(val->getLong(0,1,buf3));
            ASSERT_TRUE(val->getIndex(0,1,buf4));
            ASSERT_TRUE(val->getFloat(0,1,buf5));
            ASSERT_TRUE(val->getDouble(0,1,buf6));
            ASSERT_FALSE(val->getSymbol(0,1,buf2,symbase,false));
            ASSERT_FALSE(val->getString(0,1,buf8));
            ASSERT_FALSE(val->getString(0,1,buf9));
            ASSERT_FALSE(val->getBinary(0,1,1,buf10));
            ASSERT_FALSE(val->getHash(0,1,buckets,buf2));
            std::cout<<val->getAllocatedMemory();
            ASSERT_EQ(val->getBoolConst(0,1,buf)[0], 1);
            ASSERT_EQ(val->getCharConst(0,1,buf)[0], rand_val);
            ASSERT_EQ(val->getShortConst(0,1,buf1)[0], (short)rand_val);
            ASSERT_EQ(val->getIntConst(0,1,buf2)[0], (int)rand_val);
            ASSERT_EQ(val->getLongConst(0,1,buf3)[0], (long long)rand_val);
            ASSERT_EQ(val->getIndexConst(0,1,buf4)[0], (dolphindb::INDEX)rand_val);
            ASSERT_EQ(val->getFloatConst(0,1,buf5)[0], (float)rand_val);
            ASSERT_EQ(val->getDoubleConst(0,1,buf6)[0], (double)rand_val);
            ASSERT_ANY_THROW(val->getSymbolConst(0,1,buf2,symbase,false));
            ASSERT_ANY_THROW(val->getStringConst(0,1,buf8)[0]);
            ASSERT_ANY_THROW(val->getStringConst(0,24,buf9));
            ASSERT_ANY_THROW(val->getBinaryConst(0,1,1,buf10));
        }
        else{
            ASSERT_TRUE(val->isNull(0,1,buf0));
            ASSERT_TRUE(val->isValid(0,1,buf0));
            ASSERT_TRUE(val->getBool(0,1,buf00));
            ASSERT_TRUE(val->getChar(0,1,buf));
            ASSERT_TRUE(val->getShort(0,1,buf1));
            ASSERT_TRUE(val->getInt(0,1,buf2));
            ASSERT_TRUE(val->getLong(0,1,buf3));
            ASSERT_TRUE(val->getIndex(0,1,buf4));
            ASSERT_TRUE(val->getFloat(0,1,buf5));
            ASSERT_TRUE(val->getDouble(0,1,buf6));
            ASSERT_FALSE(val->getSymbol(0,1,buf2,symbase,false));
            ASSERT_FALSE(val->getString(0,1,buf8));
            ASSERT_FALSE(val->getString(0,1,buf9));
            ASSERT_FALSE(val->getBinary(0,1,1,buf10));
            ASSERT_FALSE(val->getHash(0,1,buckets,buf2));
            std::cout<<val->getAllocatedMemory();
            ASSERT_EQ(val->getBoolConst(0,1,buf)[0], CHAR_MIN);
            ASSERT_EQ(val->getCharConst(0,1,buf)[0], CHAR_MIN);
            ASSERT_EQ(val->getShortConst(0,1,buf1)[0], SHRT_MIN);
            ASSERT_EQ(val->getIntConst(0,1,buf2)[0], INT_MIN);
            ASSERT_EQ(val->getLongConst(0,1,buf3)[0], LLONG_MIN);
            ASSERT_EQ(val->getIndexConst(0,1,buf4)[0], dolphindb::INDEX_MIN);
            ASSERT_EQ(val->getFloatConst(0,1,buf5)[0], dolphindb::FLT_NMIN);
            ASSERT_EQ(val->getDoubleConst(0,1,buf6)[0], dolphindb::DBL_NMIN);
            ASSERT_ANY_THROW(val->getSymbolConst(0,1,buf2,symbase,false));
            ASSERT_ANY_THROW(val->getStringConst(0,1,buf8)[0]);
            ASSERT_ANY_THROW(val->getStringConst(0,24,buf9));
            ASSERT_ANY_THROW(val->getBinaryConst(0,1,1,buf10));
        }
    }

    ASSERT_EQ(charval->getBoolBuffer(0,1,buf)[0], rand_val);
    ASSERT_EQ(charval->getCharBuffer(0,1,buf)[0], rand_val);
    ASSERT_EQ(charval->getShortBuffer(0,1,buf1)[0], (short)rand_val);
    std::cout<<charval->getIntBuffer(0,1,buf2)<<std::endl;
    ASSERT_EQ(buf2[0], (int)rand_val);
    ASSERT_EQ(charval->getLongBuffer(0,1,buf3)[0], (long)rand_val);
    ASSERT_EQ(charval->getIndexBuffer(0,1,buf4)[0], (dolphindb::INDEX)rand_val);
    ASSERT_EQ(charval->getFloatBuffer(0,1,buf5)[0], (float)rand_val);
    ASSERT_EQ(charval->getDoubleBuffer(0,1,buf6)[0], (double)rand_val);
    charval->getDataBuffer(0,1,buf);
    ASSERT_EQ(buf[0], rand_val);

    ASSERT_EQ(charval->getRawType(), dolphindb::DT_CHAR);
    dolphindb::ConstantSP charval1 = charval->getInstance();
    charval1->setChar(rand_val);
    ASSERT_EQ(charval1->getChar(), charval->getValue()->getChar());

    std::cout<<"-----------------short-------------------"<<std::endl;
    short rand_val2 = rand()%SHRT_MAX+1;
    dolphindb::ConstantSP shortval=dolphindb::Util::createShort(rand_val2);
    dolphindb::ConstantSP shortNullval=dolphindb::Util::createNullConstant(dolphindb::DT_SHORT);
    std::vector<dolphindb::ConstantSP> shortV = {shortNullval, shortval};
    for(auto &val:shortV){
        if(val == shortval){
            ASSERT_TRUE(val->isNull(0,1,buf0));
            ASSERT_TRUE(val->isValid(0,1,buf0));
            ASSERT_TRUE(val->getBool(0,1,buf00));
            ASSERT_TRUE(val->getChar(0,1,buf));
            ASSERT_TRUE(val->getShort(0,1,buf1));
            ASSERT_TRUE(val->getInt(0,1,buf2));
            ASSERT_TRUE(val->getLong(0,1,buf3));
            ASSERT_TRUE(val->getIndex(0,1,buf4));
            ASSERT_TRUE(val->getFloat(0,1,buf5));
            ASSERT_TRUE(val->getDouble(0,1,buf6));
            ASSERT_FALSE(val->getSymbol(0,1,buf2,symbase,false));
            ASSERT_FALSE(val->getString(0,1,buf8));
            ASSERT_FALSE(val->getString(0,1,buf9));
            ASSERT_FALSE(val->getBinary(0,1,1,buf10));
            ASSERT_FALSE(val->getHash(0,1,buckets,buf2));
            std::cout<<val->getAllocatedMemory();
            // ASSERT_EQ(val->getBoolConst(0,1,buf)[0], 1);
            // ASSERT_EQ(val->getCharConst(0,1,buf)[0], rand_val2);
            ASSERT_EQ(val->getShortConst(0,1,buf1)[0], (short)rand_val2);
            ASSERT_EQ(val->getIntConst(0,1,buf2)[0], (int)rand_val2);
            ASSERT_EQ(val->getLongConst(0,1,buf3)[0], (long long)rand_val2);
            ASSERT_EQ(val->getIndexConst(0,1,buf4)[0], (dolphindb::INDEX)rand_val2);
            ASSERT_EQ(val->getFloatConst(0,1,buf5)[0], (float)rand_val2);
            ASSERT_EQ(val->getDoubleConst(0,1,buf6)[0], (double)rand_val2);
            ASSERT_ANY_THROW(val->getSymbolConst(0,1,buf2,symbase,false));
            ASSERT_ANY_THROW(val->getStringConst(0,1,buf8)[0]);
            ASSERT_ANY_THROW(val->getStringConst(0,24,buf9));
            ASSERT_ANY_THROW(val->getBinaryConst(0,1,1,buf10));
        }
        else{
            ASSERT_TRUE(val->isNull(0,1,buf0));
            ASSERT_TRUE(val->isValid(0,1,buf0));
            ASSERT_TRUE(val->getBool(0,1,buf00));
            ASSERT_TRUE(val->getChar(0,1,buf));
            ASSERT_TRUE(val->getShort(0,1,buf1));
            ASSERT_TRUE(val->getInt(0,1,buf2));
            ASSERT_TRUE(val->getLong(0,1,buf3));
            ASSERT_TRUE(val->getIndex(0,1,buf4));
            ASSERT_TRUE(val->getFloat(0,1,buf5));
            ASSERT_TRUE(val->getDouble(0,1,buf6));
            ASSERT_FALSE(val->getSymbol(0,1,buf2,symbase,false));
            ASSERT_FALSE(val->getString(0,1,buf8));
            ASSERT_FALSE(val->getString(0,1,buf9));
            ASSERT_FALSE(val->getBinary(0,1,1,buf10));
            ASSERT_FALSE(val->getHash(0,1,buckets,buf2));
            std::cout<<val->getAllocatedMemory();
            // ASSERT_EQ(val->getBoolConst(0,1,buf)[0], 1);
            // ASSERT_EQ(val->getCharConst(0,1,buf)[0], rand_val2);
            ASSERT_EQ(val->getShortConst(0,1,buf1)[0], SHRT_MIN);
            ASSERT_EQ(val->getIntConst(0,1,buf2)[0], INT_MIN);
            ASSERT_EQ(val->getLongConst(0,1,buf3)[0], LLONG_MIN);
            ASSERT_EQ(val->getIndexConst(0,1,buf4)[0], dolphindb::INDEX_MIN);
            ASSERT_EQ(val->getFloatConst(0,1,buf5)[0], dolphindb::FLT_NMIN);
            ASSERT_EQ(val->getDoubleConst(0,1,buf6)[0], dolphindb::DBL_NMIN);
            ASSERT_ANY_THROW(val->getSymbolConst(0,1,buf2,symbase,false));
            ASSERT_ANY_THROW(val->getStringConst(0,1,buf8)[0]);
            ASSERT_ANY_THROW(val->getStringConst(0,24,buf9));
            ASSERT_ANY_THROW(val->getBinaryConst(0,1,1,buf10));
        }
    }

    // ASSERT_EQ(shortval->getBoolBuffer(0,1,buf)[0], rand_val2);
    // ASSERT_EQ(shortval->getCharBuffer(0,1,buf)[0], rand_val2);
    ASSERT_EQ(shortval->getShortBuffer(0,1,buf1)[0], (short)rand_val2);
    std::cout<<shortval->getIntBuffer(0,1,buf2)<<std::endl;
    ASSERT_EQ(buf2[0], (int)rand_val2);
    ASSERT_EQ(shortval->getLongBuffer(0,1,buf3)[0], (long)rand_val2);
    ASSERT_EQ(shortval->getIndexBuffer(0,1,buf4)[0], (dolphindb::INDEX)rand_val2);
    ASSERT_EQ(shortval->getFloatBuffer(0,1,buf5)[0], (float)rand_val2);
    ASSERT_EQ(shortval->getDoubleBuffer(0,1,buf6)[0], (double)rand_val2);
    shortval->getDataBuffer(0,1,buf1);
    ASSERT_EQ(buf1[0], rand_val2);

    ASSERT_EQ(shortval->getRawType(), dolphindb::DT_SHORT);
    dolphindb::ConstantSP shortval1 = shortval->getInstance();
    shortval1->setShort(rand_val2);
    ASSERT_EQ(shortval1->getShort(), shortval->getValue()->getShort());

    std::cout<<"-----------------long-------------------"<<std::endl;
    long long rand_val3 = rand()%LLONG_MAX+1;
    dolphindb::ConstantSP longval=dolphindb::Util::createLong(rand_val3);
    dolphindb::ConstantSP longNullval=dolphindb::Util::createNullConstant(dolphindb::DT_SHORT);
    std::vector<dolphindb::ConstantSP> longV = {longNullval, longval};
    for(auto &val:longV){
        if(val == longval){
            ASSERT_TRUE(val->isNull(0,1,buf0));
            ASSERT_TRUE(val->isValid(0,1,buf0));
            ASSERT_TRUE(val->getBool(0,1,buf00));
            ASSERT_TRUE(val->getChar(0,1,buf));
            ASSERT_TRUE(val->getShort(0,1,buf1));
            ASSERT_TRUE(val->getInt(0,1,buf2));
            ASSERT_TRUE(val->getLong(0,1,buf3));
            ASSERT_TRUE(val->getIndex(0,1,buf4));
            ASSERT_TRUE(val->getFloat(0,1,buf5));
            ASSERT_TRUE(val->getDouble(0,1,buf6));
            ASSERT_FALSE(val->getSymbol(0,1,buf2,symbase,false));
            ASSERT_FALSE(val->getString(0,1,buf8));
            ASSERT_FALSE(val->getString(0,1,buf9));
            ASSERT_FALSE(val->getBinary(0,1,1,buf10));
            ASSERT_FALSE(val->getHash(0,1,buckets,buf2));
            std::cout<<val->getAllocatedMemory();
            // ASSERT_EQ(val->getBoolConst(0,1,buf)[0], 1);
            // ASSERT_EQ(val->getCharConst(0,1,buf)[0], rand_val3);
            // ASSERT_EQ(val->getShortConst(0,1,buf1)[0], (short)rand_val3);
            ASSERT_EQ(val->getIntConst(0,1,buf2)[0], (int)rand_val3);
            ASSERT_EQ(val->getLongConst(0,1,buf3)[0], (long long)rand_val3);
            ASSERT_EQ(val->getIndexConst(0,1,buf4)[0], (dolphindb::INDEX)rand_val3);
            ASSERT_EQ(val->getFloatConst(0,1,buf5)[0], (float)rand_val3);
            ASSERT_EQ(val->getDoubleConst(0,1,buf6)[0], (double)rand_val3);
            ASSERT_ANY_THROW(val->getSymbolConst(0,1,buf2,symbase,false));
            ASSERT_ANY_THROW(val->getStringConst(0,1,buf8)[0]);
            ASSERT_ANY_THROW(val->getStringConst(0,24,buf9));
            ASSERT_ANY_THROW(val->getBinaryConst(0,1,1,buf10));
        }
        else{
            ASSERT_TRUE(val->isNull(0,1,buf0));
            ASSERT_TRUE(val->isValid(0,1,buf0));
            ASSERT_TRUE(val->getBool(0,1,buf00));
            ASSERT_TRUE(val->getChar(0,1,buf));
            ASSERT_TRUE(val->getShort(0,1,buf1));
            ASSERT_TRUE(val->getInt(0,1,buf2));
            ASSERT_TRUE(val->getLong(0,1,buf3));
            ASSERT_TRUE(val->getIndex(0,1,buf4));
            ASSERT_TRUE(val->getFloat(0,1,buf5));
            ASSERT_TRUE(val->getDouble(0,1,buf6));
            ASSERT_FALSE(val->getSymbol(0,1,buf2,symbase,false));
            ASSERT_FALSE(val->getString(0,1,buf8));
            ASSERT_FALSE(val->getString(0,1,buf9));
            ASSERT_FALSE(val->getBinary(0,1,1,buf10));
            ASSERT_FALSE(val->getHash(0,1,buckets,buf2));
            std::cout<<val->getAllocatedMemory();
            // ASSERT_EQ(val->getBoolConst(0,1,buf)[0], 1);
            // ASSERT_EQ(val->getCharConst(0,1,buf)[0], rand_val3);
            // ASSERT_EQ(val->getShortConst(0,1,buf1)[0], (short)rand_val3);
            ASSERT_EQ(val->getIntConst(0,1,buf2)[0], INT_MIN);
            ASSERT_EQ(val->getLongConst(0,1,buf3)[0], LLONG_MIN);
            ASSERT_EQ(val->getIndexConst(0,1,buf4)[0], dolphindb::INDEX_MIN);
            ASSERT_EQ(val->getFloatConst(0,1,buf5)[0], dolphindb::FLT_NMIN);
            ASSERT_EQ(val->getDoubleConst(0,1,buf6)[0], dolphindb::DBL_NMIN);
            ASSERT_ANY_THROW(val->getSymbolConst(0,1,buf2,symbase,false));
            ASSERT_ANY_THROW(val->getStringConst(0,1,buf8)[0]);
            ASSERT_ANY_THROW(val->getStringConst(0,24,buf9));
            ASSERT_ANY_THROW(val->getBinaryConst(0,1,1,buf10));
        }
    }

    // ASSERT_EQ(longval->getBoolBuffer(0,1,buf)[0], rand_val3);
    // ASSERT_EQ(longval->getCharBuffer(0,1,buf)[0], rand_val3);
    // ASSERT_EQ(longval->getShortBuffer(0,1,buf1)[0], (short)rand_val3);
    std::cout<<longval->getIntBuffer(0,1,buf2)<<std::endl;
    ASSERT_EQ(buf2[0], (int)rand_val3);
    ASSERT_EQ(longval->getLongBuffer(0,1,buf3)[0], (long long)rand_val3);
    ASSERT_EQ(longval->getIndexBuffer(0,1,buf4)[0], (dolphindb::INDEX)rand_val3);
    ASSERT_EQ(longval->getFloatBuffer(0,1,buf5)[0], (float)rand_val3);
    ASSERT_EQ(longval->getDoubleBuffer(0,1,buf6)[0], (double)rand_val3);
    longval->getDataBuffer(0,1,buf3);
    ASSERT_EQ(buf3[0], rand_val3);

    ASSERT_EQ(longval->getRawType(), dolphindb::DT_LONG);
    dolphindb::ConstantSP longval1 = longval->getInstance();
    longval1->setLong(rand_val3);
    ASSERT_EQ(longval1->getString(), longval->getValue()->getString());

    std::cout<<"-----------------float-------------------"<<std::endl;
    float rand_val4 = 1.5862/*rand()/float(RAND_MAX)+1*/;
    dolphindb::ConstantSP floatval=dolphindb::Util::createFloat(rand_val4);
    dolphindb::ConstantSP negfloatval=dolphindb::Util::createFloat(0 - rand_val4);
    dolphindb::ConstantSP fNullval=dolphindb::Util::createNullConstant(dolphindb::DT_FLOAT);
    std::vector<dolphindb::ConstantSP> f_vals = {fNullval, negfloatval, floatval};
    for(auto &val:f_vals){
        ASSERT_TRUE(val->getBool(0,1,buf00));
        ASSERT_TRUE(val->getChar(0,1,buf));
        ASSERT_TRUE(val->getShort(0,1,buf1));
        ASSERT_TRUE(val->getInt(0,1,buf2));
        ASSERT_TRUE(val->getLong(0,1,buf3));
        ASSERT_TRUE(val->getIndex(0,1,buf4));
        ASSERT_TRUE(val->getFloat(0,1,buf5));
        ASSERT_TRUE(val->getDouble(0,1,buf6));
        ASSERT_FALSE(val->getSymbol(0,1,buf2,symbase,false));
        ASSERT_FALSE(val->getString(0,1,buf8));
        ASSERT_FALSE(val->getString(0,1,buf9));
        ASSERT_FALSE(val->getBinary(0,1,1,buf10));
        ASSERT_FALSE(val->getHash(0,1,buckets,buf2));
        ASSERT_TRUE(val->getBoolConst(0,1,buf));
        ASSERT_TRUE(val->getCharConst(0,1,buf));
        ASSERT_TRUE(val->getShortConst(0,1,buf1));
        ASSERT_TRUE(val->getIntConst(0,1,buf2));
        ASSERT_TRUE(val->getLongConst(0,1,buf3));
        ASSERT_TRUE(val->getIndexConst(0,1,buf4));
        ASSERT_TRUE(val->getFloatConst(0,1,buf5));
        ASSERT_TRUE(val->getDoubleConst(0,1,buf6));
        ASSERT_ANY_THROW(val->getSymbolConst(0,1,buf2,symbase,false)[0]);
        ASSERT_ANY_THROW(val->getStringConst(0,1,buf8)[0]);
        ASSERT_ANY_THROW(val->getStringConst(0,1,buf9)[0]);
        ASSERT_ANY_THROW(val->getBinaryConst(0,1,1,buf10)[0]);
    }

    // ASSERT_EQ(longval->getBoolBuffer(0,1,buf)[0], 1);
    // ASSERT_EQ(longval->getCharBuffer(0,1,buf)[0], rand_val4);
    ASSERT_EQ(floatval->getShortBuffer(0,1,buf1)[0], (short)2);
    std::cout<<floatval->getIntBuffer(0,1,buf2)<<std::endl;
    ASSERT_EQ(buf2[0], (int)2);
    ASSERT_EQ(floatval->getLongBuffer(0,1,buf3)[0], (long long)2);
    ASSERT_EQ(floatval->getIndexBuffer(0,1,buf4)[0], (dolphindb::INDEX)1);
    ASSERT_EQ(floatval->getFloatBuffer(0,1,buf5)[0], (float)rand_val4);
    ASSERT_EQ(floatval->getDoubleBuffer(0,1,buf6)[0], (double)rand_val4);
    floatval->getDataBuffer(0,1,buf5);
    ASSERT_EQ(buf5[0], rand_val4);

    ASSERT_EQ(floatval->getRawType(), dolphindb::DT_FLOAT);
    dolphindb::ConstantSP floatval1 = floatval->getInstance();
    floatval1->setFloat(rand_val4);
    ASSERT_EQ(floatval1->getFloat(), floatval->getValue()->getFloat());

    std::cout<<"-----------------double-------------------"<<std::endl;
    double rand_val5 = 2.1345/*rand()/float(RAND_MAX)+1*/;
    dolphindb::ConstantSP doubleval=dolphindb::Util::createDouble(rand_val5);
    dolphindb::ConstantSP negdoubleval=dolphindb::Util::createDouble(0 - rand_val5);
    dolphindb::ConstantSP doubleNullval=dolphindb::Util::createNullConstant(dolphindb::DT_DOUBLE);
    std::vector<dolphindb::ConstantSP> double_vals = {doubleNullval, negdoubleval, doubleval};
    for(auto &val:double_vals){
        ASSERT_TRUE(val->getBool(0,1,buf00));
        ASSERT_TRUE(val->getChar(0,1,buf));
        ASSERT_TRUE(val->getShort(0,1,buf1));
        ASSERT_TRUE(val->getInt(0,1,buf2));
        ASSERT_TRUE(val->getLong(0,1,buf3));
        ASSERT_TRUE(val->getIndex(0,1,buf4));
        ASSERT_TRUE(val->getFloat(0,1,buf5));
        ASSERT_TRUE(val->getDouble(0,1,buf6));
        ASSERT_FALSE(val->getSymbol(0,1,buf2,symbase,false));
        ASSERT_FALSE(val->getString(0,1,buf8));
        ASSERT_FALSE(val->getString(0,1,buf9));
        ASSERT_FALSE(val->getBinary(0,1,1,buf10));
        ASSERT_FALSE(val->getHash(0,1,buckets,buf2));
        ASSERT_TRUE(val->getBoolConst(0,1,buf));
        ASSERT_TRUE(val->getCharConst(0,1,buf));
        ASSERT_TRUE(val->getShortConst(0,1,buf1));
        ASSERT_TRUE(val->getIntConst(0,1,buf2));
        ASSERT_TRUE(val->getLongConst(0,1,buf3));
        ASSERT_TRUE(val->getIndexConst(0,1,buf4));
        ASSERT_TRUE(val->getFloatConst(0,1,buf5));
        ASSERT_TRUE(val->getDoubleConst(0,1,buf6));
        ASSERT_ANY_THROW(val->getSymbolConst(0,1,buf2,symbase,false));
        ASSERT_ANY_THROW(val->getStringConst(0,1,buf8));
        ASSERT_ANY_THROW(val->getStringConst(0,1,buf9));
        ASSERT_ANY_THROW(val->getBinaryConst(0,1,1,buf10));
    }

    // ASSERT_EQ(longval->getBoolBuffer(0,1,buf)[0], 1);
    // ASSERT_EQ(longval->getCharBuffer(0,1,buf)[0], rand_val4);
    ASSERT_EQ(doubleval->getShortBuffer(0,1,buf1)[0], (short)2);
    std::cout<<doubleval->getIntBuffer(0,1,buf2)<<std::endl;
    ASSERT_EQ(buf2[0], (int)2);
    ASSERT_EQ(doubleval->getLongBuffer(0,1,buf3)[0], (long long)2);
    ASSERT_EQ(doubleval->getIndexBuffer(0,1,buf4)[0], (dolphindb::INDEX)2);
    ASSERT_EQ(doubleval->getFloatBuffer(0,1,buf5)[0], (float)rand_val5);
    ASSERT_EQ(doubleval->getDoubleBuffer(0,1,buf6)[0], (double)rand_val5);
    doubleval->getDataBuffer(0,1,buf6);
    ASSERT_EQ(buf6[0], rand_val5);

    ASSERT_EQ(doubleval->getRawType(), dolphindb::DT_DOUBLE);
    dolphindb::ConstantSP doubleval1 = doubleval->getInstance();
    doubleval1->setDouble(rand_val5);
    ASSERT_EQ(doubleval1->getDouble(), doubleval->getValue()->getDouble());

    std::cout<<"-----------------int128-------------------"<<std::endl;
    unsigned char int128[16];
    for(auto i=0; i<16; i++)
        int128[i] = rand() % CHAR_MAX;
    dolphindb::ConstantSP int128val = dolphindb::Util::createConstant(dolphindb::DT_INT128);
    int128val->setBinary(int128, sizeof(dolphindb::Guid));
    dolphindb::ConstantSP int128val2 = int128val->getInstance();
    int128val2->setNull(0);
    ASSERT_TRUE(int128val2->isNull(0, 1, buf0));
    ASSERT_EQ(buf0[0], 1);

    ASSERT_TRUE(int128val2->isValid(0, 1, buf00));
    ASSERT_EQ(buf00[0], 0);
    int128val2->nullFill(int128val);
    ASSERT_EQ(int128val2->getString(), int128val->getValue()->getString());

    ASSERT_EQ(int128val2->compare(1, int128val), 0);

    std::cout<<"-----------------uuid-------------------"<<std::endl;
    unsigned char uuid[16];
    for(auto i=0; i<16; i++)
        uuid[i] = rand() % CHAR_MAX;
    dolphindb::ConstantSP uuidval = dolphindb::Util::createConstant(dolphindb::DT_UUID);
    uuidval->setBinary(uuid, sizeof(dolphindb::Guid));
    dolphindb::ConstantSP uuidval2 = uuidval->getInstance();
    uuidval2->setNull(0);
    ASSERT_TRUE(uuidval2->isNull(0, 1, buf0));
    ASSERT_EQ(buf0[0], 1);

    ASSERT_TRUE(uuidval2->isValid(0, 1, buf00));
    ASSERT_EQ(buf00[0], 0);
    uuidval2->nullFill(uuidval);
    ASSERT_EQ(uuidval2->getString(), uuidval->getValue()->getString());

    #ifndef _WIN32
    dolphindb::Uuid uuidval3 = dolphindb::Uuid(false);
    dolphindb::Uuid uuidval4 = dolphindb::Uuid(true);
    dolphindb::Uuid uuidval5 = dolphindb::Uuid(uuid);
    const char* uuid_data2 = "225d0132-1c2c-0710-3734-563613716f07";
    dolphindb::Uuid uuidval6 = dolphindb::Uuid(uuid_data2, 0);
    ASSERT_ANY_THROW(dolphindb::Uuid uuidval7 = dolphindb::Uuid(uuid_data2, 50));
    dolphindb::Uuid uuidval7 = dolphindb::Uuid(uuid_data2, 36);
    dolphindb::Uuid uuidval8 = dolphindb::Uuid(uuidval7);

    ASSERT_EQ(uuidval3.getString(), "00000000-0000-0000-0000-000000000000");
    ASSERT_EQ(uuidval3.getString(), uuidval6.getString());
    ASSERT_EQ(uuidval->getString(), uuidval5.getString());
    ASSERT_EQ(uuidval7.getString(), uuidval8.getString());
    #endif
    std::cout<<"-----------------ipaddr-------------------"<<std::endl;
    unsigned char ip[16];
    for(auto i=0; i<16; i++)
        ip[i] = rand() % CHAR_MAX;
    dolphindb::ConstantSP ipval = dolphindb::Util::createConstant(dolphindb::DT_IP);
    ipval->setBinary(ip, sizeof(dolphindb::Guid));
    dolphindb::ConstantSP ipval2 = ipval->getInstance();
    ipval2->setNull(0);
    ASSERT_TRUE(ipval2->isNull(0, 1, buf0));
    ASSERT_EQ(buf0[0], 1);

    ASSERT_TRUE(ipval2->isValid(0, 1, buf00));
    ASSERT_EQ(buf00[0], 0);
    ipval2->nullFill(ipval);
    ASSERT_EQ(ipval2->getString(), ipval->getValue()->getString());

    #ifndef _WIN32
    const char* ip_1 = "";
    const char* ip_2 = "1.1.1";
    ASSERT_EQ(dolphindb::IPAddr(ip_1, 0).getString(), "0.0.0.0");
    ASSERT_EQ(dolphindb::IPAddr(ip_2, 6).getString(), "0.0.0.0");
    #endif

    delete[] buf, buf1, buf2, buf3, buf4, buf5, buf6, buf9, buf10, buf00, buf0, buf8;
}

TEST_F(ScalarTest,testFunctionCastTemporal_month){
    dolphindb::ConstantSP monthval = dolphindb::Util::createMonth(1);

    ASSERT_ANY_THROW(monthval->castTemporal(dolphindb::DT_CHAR));
    ASSERT_ANY_THROW(monthval->castTemporal(dolphindb::DT_STRING));
    ASSERT_ANY_THROW(monthval->castTemporal(dolphindb::DT_DATE));
    ASSERT_EQ(monthval->castTemporal(dolphindb::DT_MONTH)->getString(),conn.run("month(month(1))")->getString());
    ASSERT_ANY_THROW(monthval->castTemporal(dolphindb::DT_TIME));
    ASSERT_ANY_THROW(monthval->castTemporal(dolphindb::DT_MINUTE));
    ASSERT_ANY_THROW(monthval->castTemporal(dolphindb::DT_SECOND));
    ASSERT_ANY_THROW(monthval->castTemporal(dolphindb::DT_DATETIME));
    ASSERT_ANY_THROW(monthval->castTemporal(dolphindb::DT_TIMESTAMP));
    ASSERT_ANY_THROW(monthval->castTemporal(dolphindb::DT_NANOTIME));
    ASSERT_ANY_THROW(monthval->castTemporal(dolphindb::DT_NANOTIMESTAMP));
    ASSERT_ANY_THROW(monthval->castTemporal(dolphindb::DT_DATEHOUR));
}

TEST_F(ScalarTest,testFunctionCastTemporal_datatime){
    dolphindb::ConstantSP datetimeval = dolphindb::Util::createDateTime(1);

    ASSERT_ANY_THROW(datetimeval->castTemporal(dolphindb::DT_CHAR));
    ASSERT_ANY_THROW(datetimeval->castTemporal(dolphindb::DT_STRING));
    ASSERT_EQ(datetimeval->castTemporal(dolphindb::DT_DATE)->getString(),conn.run("date(datetime(1))")->getString());
    ASSERT_EQ(datetimeval->castTemporal(dolphindb::DT_MONTH)->getString(),conn.run("month(datetime(1))")->getString());
    ASSERT_EQ(datetimeval->castTemporal(dolphindb::DT_TIME)->getString(),conn.run("time(datetime(1))")->getString());
    ASSERT_EQ(datetimeval->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(datetime(1))")->getString());
    ASSERT_EQ(datetimeval->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(datetime(1))")->getString());
    ASSERT_EQ(datetimeval->castTemporal(dolphindb::DT_DATETIME)->getString(),conn.run("datetime(datetime(1))")->getString());
    ASSERT_EQ(datetimeval->castTemporal(dolphindb::DT_TIMESTAMP)->getString(),conn.run("timestamp(datetime(1))")->getString());
    ASSERT_EQ(datetimeval->castTemporal(dolphindb::DT_NANOTIME)->getString(),conn.run("nanotime(datetime(1))")->getString());
    ASSERT_EQ(datetimeval->castTemporal(dolphindb::DT_NANOTIMESTAMP)->getString(),conn.run("nanotimestamp(datetime(1))")->getString());
    ASSERT_EQ(datetimeval->castTemporal(dolphindb::DT_DATEHOUR)->getString(),conn.run("datehour(datetime(1))")->getString());

    int negSec = 0 - rand() % INT_MAX;
    int posSec = rand() % INT_MAX;
    negSec = negSec % 3600 == 0 ? negSec+1 : negSec;
    posSec = posSec % 3600 == 0 ? posSec+1 : posSec;
    int negSec3600 = 0 - (rand() % 10) * 3600;
    int posSec3600 = (rand() % 10) * 3600;

    int negSec86400 = (0 - rand() % 10) * 86400;
    int posSec86400 = (rand() % 10) * 86400;

    // std::cout<<negSec<<std::endl<<posSec<<std::endl<<negSec3600<<std::endl<<posSec3600<<std::endl;
    ASSERT_EQ(dolphindb::Util::createDateTime(INT_MIN)->castTemporal(dolphindb::DT_NANOTIMESTAMP)->getString(), "");
    ASSERT_EQ(dolphindb::Util::createDateTime(INT_MIN)->castTemporal(dolphindb::DT_DATE)->getString(),conn.run("date(datetime("+std::to_string(INT_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateTime(INT_MIN)->castTemporal(dolphindb::DT_MONTH)->getString(),conn.run("month(datetime("+std::to_string(INT_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateTime(INT_MIN)->castTemporal(dolphindb::DT_TIME)->getString(),conn.run("time(datetime("+std::to_string(INT_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateTime(INT_MIN)->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(datetime("+std::to_string(INT_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateTime(INT_MIN)->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(datetime("+std::to_string(INT_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateTime(INT_MIN)->castTemporal(dolphindb::DT_DATETIME)->getString(),conn.run("datetime(datetime("+std::to_string(INT_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateTime(INT_MIN)->castTemporal(dolphindb::DT_TIMESTAMP)->getString(),"");
    ASSERT_EQ(dolphindb::Util::createDateTime(INT_MIN)->castTemporal(dolphindb::DT_NANOTIME)->getString(),"");
    ASSERT_EQ(dolphindb::Util::createDateTime(INT_MIN)->castTemporal(dolphindb::DT_DATEHOUR)->getString(),conn.run("datehour(datetime("+std::to_string(INT_MIN)+"))")->getString());


    ASSERT_EQ(dolphindb::Util::createDateTime(negSec)->castTemporal(dolphindb::DT_DATEHOUR)->getString(),conn.run("datehour(datetime("+std::to_string(negSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateTime(negSec3600)->castTemporal(dolphindb::DT_DATEHOUR)->getString(),conn.run("datehour(datetime("+std::to_string(negSec3600)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateTime(posSec)->castTemporal(dolphindb::DT_DATEHOUR)->getString(),conn.run("datehour(datetime("+std::to_string(posSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateTime(posSec3600)->castTemporal(dolphindb::DT_DATEHOUR)->getString(),conn.run("datehour(datetime("+std::to_string(posSec3600)+"))")->getString());

    ASSERT_EQ(dolphindb::Util::createDateTime(negSec)->castTemporal(dolphindb::DT_DATE)->getString(),conn.run("date(datetime("+std::to_string(negSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateTime(negSec86400)->castTemporal(dolphindb::DT_DATE)->getString(),conn.run("date(datetime("+std::to_string(negSec86400)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateTime(posSec)->castTemporal(dolphindb::DT_DATE)->getString(),conn.run("date(datetime("+std::to_string(posSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateTime(posSec86400)->castTemporal(dolphindb::DT_DATE)->getString(),conn.run("date(datetime("+std::to_string(posSec86400)+"))")->getString());

    ASSERT_EQ(dolphindb::Util::createDateTime(negSec)->castTemporal(dolphindb::DT_NANOTIME)->getString(),conn.run("nanotime(datetime("+std::to_string(negSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateTime(negSec86400)->castTemporal(dolphindb::DT_NANOTIME)->getString(),conn.run("nanotime(datetime("+std::to_string(negSec86400)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateTime(posSec)->castTemporal(dolphindb::DT_NANOTIME)->getString(),conn.run("nanotime(datetime("+std::to_string(posSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateTime(posSec86400)->castTemporal(dolphindb::DT_NANOTIME)->getString(),conn.run("nanotime(datetime("+std::to_string(posSec86400)+"))")->getString());

    ASSERT_EQ(dolphindb::Util::createDateTime(negSec)->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(datetime("+std::to_string(negSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateTime(negSec86400)->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(datetime("+std::to_string(negSec86400)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateTime(posSec)->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(datetime("+std::to_string(posSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateTime(posSec86400)->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(datetime("+std::to_string(posSec86400)+"))")->getString());

    ASSERT_EQ(dolphindb::Util::createDateTime(negSec)->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(datetime("+std::to_string(negSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateTime(negSec86400)->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(datetime("+std::to_string(negSec86400)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateTime(posSec)->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(datetime("+std::to_string(posSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateTime(posSec86400)->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(datetime("+std::to_string(posSec86400)+"))")->getString());

}

TEST_F(ScalarTest,testFunctionCastTemporal_NanoTimestamp){
    srand(time(NULL));
    dolphindb::ConstantSP nanotimestampval = dolphindb::Util::createNanoTimestamp(1000000);

    ASSERT_ANY_THROW(nanotimestampval->castTemporal(dolphindb::DT_CHAR));
    ASSERT_ANY_THROW(nanotimestampval->castTemporal(dolphindb::DT_STRING));
    ASSERT_EQ(nanotimestampval->castTemporal(dolphindb::DT_DATE)->getString(),conn.run("date(nanotimestamp(1000000))")->getString());
    ASSERT_EQ(nanotimestampval->castTemporal(dolphindb::DT_MONTH)->getString(),conn.run("month(nanotimestamp(1000000))")->getString());
    ASSERT_EQ(nanotimestampval->castTemporal(dolphindb::DT_TIME)->getString(),conn.run("time(nanotimestamp(1000000))")->getString());
    ASSERT_EQ(nanotimestampval->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(nanotimestamp(1000000))")->getString());
    ASSERT_EQ(nanotimestampval->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(nanotimestamp(1000000))")->getString());
    ASSERT_EQ(nanotimestampval->castTemporal(dolphindb::DT_DATETIME)->getString(),conn.run("datetime(nanotimestamp(1000000))")->getString());
    ASSERT_EQ(nanotimestampval->castTemporal(dolphindb::DT_TIMESTAMP)->getString(),conn.run("timestamp(nanotimestamp(1000000))")->getString());
    ASSERT_EQ(nanotimestampval->castTemporal(dolphindb::DT_NANOTIME)->getString(),conn.run("nanotime(nanotimestamp(1000000))")->getString());
    ASSERT_EQ(nanotimestampval->castTemporal(dolphindb::DT_NANOTIMESTAMP)->getString(),conn.run("nanotimestamp(nanotimestamp(1000000))")->getString());
    ASSERT_EQ(nanotimestampval->castTemporal(dolphindb::DT_DATEHOUR)->getString(),conn.run("datehour(nanotimestamp(1000000))")->getString());

    long long negSec = 0 - rand() % LLONG_MAX;
    long long posSec = rand() % LLONG_MAX;
    negSec = negSec % 3600000000000 == 0 ? negSec+1 : negSec;
    posSec = posSec % 3600000000000 == 0 ? posSec+1 : posSec;
    long long negSec3600 = 0 - (rand() % 10) * 3600000000000;

    long long posSec3600 = (rand() % 10) * 3600000000000;

    long long negSec86400 = (0 - rand() % 10) * 86400000000000;
    long long posSec86400 = (rand() % 10) * 86400000000000;

    // std::cout<<negSec<<std::endl<<posSec<<std::endl<<negSec3600<<std::endl<<posSec3600<<std::endl;
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(LLONG_MIN)->castTemporal(dolphindb::DT_NANOTIMESTAMP)->getString(),conn.run("nanotimestamp(nanotimestamp("+std::to_string(LLONG_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(LLONG_MIN)->castTemporal(dolphindb::DT_DATE)->getString(),conn.run("date(nanotimestamp("+std::to_string(LLONG_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(LLONG_MIN)->castTemporal(dolphindb::DT_MONTH)->getString(),conn.run("month(nanotimestamp("+std::to_string(LLONG_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(LLONG_MIN)->castTemporal(dolphindb::DT_TIME)->getString(),conn.run("time(nanotimestamp("+std::to_string(LLONG_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(LLONG_MIN)->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(nanotimestamp("+std::to_string(LLONG_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(LLONG_MIN)->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(nanotimestamp("+std::to_string(LLONG_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(LLONG_MIN)->castTemporal(dolphindb::DT_DATETIME)->getString(),conn.run("datetime(nanotimestamp("+std::to_string(LLONG_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(LLONG_MIN)->castTemporal(dolphindb::DT_TIMESTAMP)->getString(),"");
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(LLONG_MIN)->castTemporal(dolphindb::DT_NANOTIME)->getString(),"");
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(LLONG_MIN)->castTemporal(dolphindb::DT_DATEHOUR)->getString(),conn.run("datehour(nanotimestamp("+std::to_string(LLONG_MIN)+"))")->getString());

    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(negSec)->castTemporal(dolphindb::DT_DATEHOUR)->getString(),conn.run("datehour(nanotimestamp("+std::to_string(negSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(negSec3600)->castTemporal(dolphindb::DT_DATEHOUR)->getString(),conn.run("datehour(nanotimestamp("+std::to_string(negSec3600)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(posSec)->castTemporal(dolphindb::DT_DATEHOUR)->getString(),conn.run("datehour(nanotimestamp("+std::to_string(posSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(posSec3600)->castTemporal(dolphindb::DT_DATEHOUR)->getString(),conn.run("datehour(nanotimestamp("+std::to_string(posSec3600)+"))")->getString());

    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(negSec)->castTemporal(dolphindb::DT_DATE)->getString(),conn.run("date(nanotimestamp("+std::to_string(negSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(negSec86400)->castTemporal(dolphindb::DT_DATE)->getString(),conn.run("date(nanotimestamp("+std::to_string(negSec86400)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(posSec)->castTemporal(dolphindb::DT_DATE)->getString(),conn.run("date(nanotimestamp("+std::to_string(posSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(posSec86400)->castTemporal(dolphindb::DT_DATE)->getString(),conn.run("date(nanotimestamp("+std::to_string(posSec86400)+"))")->getString());

    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(negSec)->castTemporal(dolphindb::DT_DATETIME)->getString(),conn.run("datetime(nanotimestamp("+std::to_string(negSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(negSec86400)->castTemporal(dolphindb::DT_DATETIME)->getString(),conn.run("datetime(nanotimestamp("+std::to_string(negSec86400)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(posSec)->castTemporal(dolphindb::DT_DATETIME)->getString(),conn.run("datetime(nanotimestamp("+std::to_string(posSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(posSec86400)->castTemporal(dolphindb::DT_DATETIME)->getString(),conn.run("datetime(nanotimestamp("+std::to_string(posSec86400)+"))")->getString());

    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(negSec)->castTemporal(dolphindb::DT_NANOTIME)->getString(),conn.run("nanotime(nanotimestamp("+std::to_string(negSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(negSec86400)->castTemporal(dolphindb::DT_NANOTIME)->getString(),conn.run("nanotime(nanotimestamp("+std::to_string(negSec86400)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(posSec)->castTemporal(dolphindb::DT_NANOTIME)->getString(),conn.run("nanotime(nanotimestamp("+std::to_string(posSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(posSec86400)->castTemporal(dolphindb::DT_NANOTIME)->getString(),conn.run("nanotime(nanotimestamp("+std::to_string(posSec86400)+"))")->getString());

    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(negSec)->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(nanotimestamp("+std::to_string(negSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(negSec86400)->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(nanotimestamp("+std::to_string(negSec86400)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(posSec)->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(nanotimestamp("+std::to_string(posSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(posSec86400)->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(nanotimestamp("+std::to_string(posSec86400)+"))")->getString());

    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(negSec)->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(nanotimestamp("+std::to_string(negSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(negSec86400)->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(nanotimestamp("+std::to_string(negSec86400)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(posSec)->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(nanotimestamp("+std::to_string(posSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(posSec86400)->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(nanotimestamp("+std::to_string(posSec86400)+"))")->getString());

    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(negSec)->castTemporal(dolphindb::DT_TIMESTAMP)->getString(),conn.run("timestamp(nanotimestamp("+std::to_string(negSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(negSec86400)->castTemporal(dolphindb::DT_TIMESTAMP)->getString(),conn.run("timestamp(nanotimestamp("+std::to_string(negSec86400)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(posSec)->castTemporal(dolphindb::DT_TIMESTAMP)->getString(),conn.run("timestamp(nanotimestamp("+std::to_string(posSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTimestamp(posSec86400)->castTemporal(dolphindb::DT_TIMESTAMP)->getString(),conn.run("timestamp(nanotimestamp("+std::to_string(posSec86400)+"))")->getString());

}

TEST_F(ScalarTest,testFunctionCastTemporal_Timestamp){
    srand(time(NULL));
    dolphindb::ConstantSP timestampval = dolphindb::Util::createTimestamp(1000000);

    ASSERT_ANY_THROW(timestampval->castTemporal(dolphindb::DT_CHAR));
    ASSERT_ANY_THROW(timestampval->castTemporal(dolphindb::DT_STRING));
    ASSERT_EQ(timestampval->castTemporal(dolphindb::DT_DATE)->getString(),conn.run("date(timestamp(1000000))")->getString());
    ASSERT_EQ(timestampval->castTemporal(dolphindb::DT_MONTH)->getString(),conn.run("month(timestamp(1000000))")->getString());
    ASSERT_EQ(timestampval->castTemporal(dolphindb::DT_TIME)->getString(),conn.run("time(timestamp(1000000))")->getString());
    ASSERT_EQ(timestampval->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(timestamp(1000000))")->getString());
    ASSERT_EQ(timestampval->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(timestamp(1000000))")->getString());
    ASSERT_EQ(timestampval->castTemporal(dolphindb::DT_DATETIME)->getString(),conn.run("datetime(timestamp(1000000))")->getString());
    ASSERT_EQ(timestampval->castTemporal(dolphindb::DT_TIMESTAMP)->getString(),conn.run("timestamp(timestamp(1000000))")->getString());
    ASSERT_EQ(timestampval->castTemporal(dolphindb::DT_NANOTIME)->getString(),conn.run("nanotime(timestamp(1000000))")->getString());
    ASSERT_EQ(timestampval->castTemporal(dolphindb::DT_NANOTIMESTAMP)->getString(),conn.run("nanotimestamp(timestamp(1000000))")->getString());
    ASSERT_EQ(timestampval->castTemporal(dolphindb::DT_DATEHOUR)->getString(),conn.run("datehour(timestamp(1000000))")->getString());

    long long negSec = 0 - rand() % LLONG_MAX;
    long long posSec = rand() % LLONG_MAX;
    negSec = negSec % 3600 == 0 ? negSec+1 : negSec;
    posSec = posSec % 3600 == 0 ? posSec+1 : posSec;
    long long negSec3600 = 0 - (rand() % 10) * 3600;

    long long posSec3600 = (rand() % 10) * 3600;

    long long negSec86400 = (0 - rand() % 10) * 86400000;
    long long posSec86400 = (rand() % 10) * 86400000;

    // std::cout<<negSec<<std::endl<<posSec<<std::endl<<negSec3600<<std::endl<<posSec3600<<std::endl;
    ASSERT_EQ(dolphindb::Util::createTimestamp(LLONG_MIN)->castTemporal(dolphindb::DT_NANOTIMESTAMP)->getString(),"");
    ASSERT_EQ(dolphindb::Util::createTimestamp(LLONG_MIN)->castTemporal(dolphindb::DT_DATE)->getString(),conn.run("date(timestamp("+std::to_string(LLONG_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTimestamp(LLONG_MIN)->castTemporal(dolphindb::DT_MONTH)->getString(),conn.run("month(timestamp("+std::to_string(LLONG_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTimestamp(LLONG_MIN)->castTemporal(dolphindb::DT_TIME)->getString(),conn.run("time(timestamp("+std::to_string(LLONG_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTimestamp(LLONG_MIN)->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(timestamp("+std::to_string(LLONG_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTimestamp(LLONG_MIN)->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(timestamp("+std::to_string(LLONG_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTimestamp(LLONG_MIN)->castTemporal(dolphindb::DT_DATETIME)->getString(),conn.run("datetime(timestamp("+std::to_string(LLONG_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTimestamp(LLONG_MIN)->castTemporal(dolphindb::DT_TIMESTAMP)->getString(),"");
    ASSERT_EQ(dolphindb::Util::createTimestamp(LLONG_MIN)->castTemporal(dolphindb::DT_NANOTIME)->getString(),"");
    ASSERT_EQ(dolphindb::Util::createTimestamp(LLONG_MIN)->castTemporal(dolphindb::DT_DATEHOUR)->getString(),conn.run("datehour(timestamp("+std::to_string(LLONG_MIN)+"))")->getString());

    ASSERT_EQ(dolphindb::Util::createTimestamp(negSec)->castTemporal(dolphindb::DT_DATEHOUR)->getString(),conn.run("datehour(timestamp("+std::to_string(negSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTimestamp(negSec3600)->castTemporal(dolphindb::DT_DATEHOUR)->getString(),conn.run("datehour(timestamp("+std::to_string(negSec3600)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTimestamp(posSec)->castTemporal(dolphindb::DT_DATEHOUR)->getString(),conn.run("datehour(timestamp("+std::to_string(posSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTimestamp(posSec3600)->castTemporal(dolphindb::DT_DATEHOUR)->getString(),conn.run("datehour(timestamp("+std::to_string(posSec3600)+"))")->getString());

    ASSERT_EQ(dolphindb::Util::createTimestamp(negSec)->castTemporal(dolphindb::DT_DATE)->getString(),conn.run("date(timestamp("+std::to_string(negSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTimestamp(negSec86400)->castTemporal(dolphindb::DT_DATE)->getString(),conn.run("date(timestamp("+std::to_string(negSec86400)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTimestamp(posSec)->castTemporal(dolphindb::DT_DATE)->getString(),conn.run("date(timestamp("+std::to_string(posSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTimestamp(posSec86400)->castTemporal(dolphindb::DT_DATE)->getString(),conn.run("date(timestamp("+std::to_string(posSec86400)+"))")->getString());

    ASSERT_EQ(dolphindb::Util::createTimestamp(negSec)->castTemporal(dolphindb::DT_DATETIME)->getString(),conn.run("datetime(timestamp("+std::to_string(negSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTimestamp(negSec86400)->castTemporal(dolphindb::DT_DATETIME)->getString(),conn.run("datetime(timestamp("+std::to_string(negSec86400)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTimestamp(posSec)->castTemporal(dolphindb::DT_DATETIME)->getString(),conn.run("datetime(timestamp("+std::to_string(posSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTimestamp(posSec86400)->castTemporal(dolphindb::DT_DATETIME)->getString(),conn.run("datetime(timestamp("+std::to_string(posSec86400)+"))")->getString());

    ASSERT_EQ(dolphindb::Util::createTimestamp(negSec)->castTemporal(dolphindb::DT_NANOTIME)->getString(),conn.run("nanotime(timestamp("+std::to_string(negSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTimestamp(negSec86400)->castTemporal(dolphindb::DT_NANOTIME)->getString(),conn.run("nanotime(timestamp("+std::to_string(negSec86400)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTimestamp(posSec)->castTemporal(dolphindb::DT_NANOTIME)->getString(),conn.run("nanotime(timestamp("+std::to_string(posSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTimestamp(posSec86400)->castTemporal(dolphindb::DT_NANOTIME)->getString(),conn.run("nanotime(timestamp("+std::to_string(posSec86400)+"))")->getString());

    ASSERT_EQ(dolphindb::Util::createTimestamp(negSec)->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(timestamp("+std::to_string(negSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTimestamp(negSec86400)->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(timestamp("+std::to_string(negSec86400)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTimestamp(posSec)->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(timestamp("+std::to_string(posSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTimestamp(posSec86400)->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(timestamp("+std::to_string(posSec86400)+"))")->getString());

    ASSERT_EQ(dolphindb::Util::createTimestamp(negSec)->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(timestamp("+std::to_string(negSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTimestamp(negSec86400)->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(timestamp("+std::to_string(negSec86400)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTimestamp(posSec)->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(timestamp("+std::to_string(posSec)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTimestamp(posSec86400)->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(timestamp("+std::to_string(posSec86400)+"))")->getString());

}

TEST_F(ScalarTest,testFunctionCastTemporal_datehour){
    srand(time(NULL));

    dolphindb::ConstantSP datehourval = dolphindb::Util::createDateHour(1);
    ASSERT_ANY_THROW(datehourval->castTemporal(dolphindb::DT_CHAR));
    ASSERT_ANY_THROW(datehourval->castTemporal(dolphindb::DT_STRING));
    ASSERT_EQ(datehourval->castTemporal(dolphindb::DT_DATE)->getString(),conn.run("date(datehour(1))")->getString());
    ASSERT_EQ(datehourval->castTemporal(dolphindb::DT_MONTH)->getString(),conn.run("month(datehour(1))")->getString());
    ASSERT_EQ(datehourval->castTemporal(dolphindb::DT_TIME)->getString(),conn.run("time(datehour(1))")->getString());
    ASSERT_EQ(datehourval->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(datehour(1))")->getString());
    ASSERT_EQ(datehourval->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(datehour(1))")->getString());
    ASSERT_EQ(datehourval->castTemporal(dolphindb::DT_DATETIME)->getString(),conn.run("datetime(datehour(1))")->getString());
    ASSERT_EQ(datehourval->castTemporal(dolphindb::DT_TIMESTAMP)->getString(),conn.run("timestamp(datehour(1))")->getString());
    ASSERT_EQ(datehourval->castTemporal(dolphindb::DT_NANOTIME)->getString(),conn.run("nanotime(datehour(1))")->getString());
    ASSERT_EQ(datehourval->castTemporal(dolphindb::DT_NANOTIMESTAMP)->getString(),conn.run("nanotimestamp(datehour(1))")->getString());
    ASSERT_EQ(datehourval->castTemporal(dolphindb::DT_DATEHOUR)->getString(),conn.run("datehour(datehour(1))")->getString());

    // std::cout<<negSec<<std::endl<<posSec<<std::endl<<negSec3600<<std::endl<<posSec3600<<std::endl;
    ASSERT_EQ(dolphindb::Util::createDateHour(INT_MIN)->castTemporal(dolphindb::DT_NANOTIMESTAMP)->getString(),"");
    ASSERT_EQ(dolphindb::Util::createDateHour(INT_MIN)->castTemporal(dolphindb::DT_DATE)->getString(),conn.run("date(datehour("+std::to_string(INT_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateHour(INT_MIN)->castTemporal(dolphindb::DT_MONTH)->getString(),conn.run("month(datehour("+std::to_string(INT_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateHour(INT_MIN)->castTemporal(dolphindb::DT_TIME)->getString(),conn.run("time(datehour("+std::to_string(INT_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateHour(INT_MIN)->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(datehour("+std::to_string(INT_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateHour(INT_MIN)->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(datehour("+std::to_string(INT_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateHour(INT_MIN)->castTemporal(dolphindb::DT_DATETIME)->getString(),conn.run("datetime(datehour("+std::to_string(INT_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDateHour(INT_MIN)->castTemporal(dolphindb::DT_TIMESTAMP)->getString(),"");
    ASSERT_EQ(dolphindb::Util::createDateHour(INT_MIN)->castTemporal(dolphindb::DT_NANOTIME)->getString(),"");
    ASSERT_EQ(dolphindb::Util::createDateHour(INT_MIN)->castTemporal(dolphindb::DT_DATEHOUR)->getString(),conn.run("datehour(datehour("+std::to_string(INT_MIN)+"))")->getString());

}


TEST_F(ScalarTest,testFunctionCastTemporal_date){
    srand(time(NULL));

    dolphindb::ConstantSP dateval = dolphindb::Util::createDate(1);
    ASSERT_ANY_THROW(dateval->castTemporal(dolphindb::DT_CHAR));
    ASSERT_ANY_THROW(dateval->castTemporal(dolphindb::DT_STRING));
    ASSERT_EQ(dateval->castTemporal(dolphindb::DT_DATE)->getString(),conn.run("date(1)")->getString());
    ASSERT_EQ(dateval->castTemporal(dolphindb::DT_MONTH)->getString(),conn.run("month(date(1))")->getString());
    ASSERT_ANY_THROW(dateval->castTemporal(dolphindb::DT_TIME));
    ASSERT_ANY_THROW(dateval->castTemporal(dolphindb::DT_MINUTE));
    ASSERT_ANY_THROW(dateval->castTemporal(dolphindb::DT_SECOND));
    ASSERT_EQ(dateval->castTemporal(dolphindb::DT_DATETIME)->getString(),conn.run("datetime(date(1))")->getString());
    ASSERT_EQ(dateval->castTemporal(dolphindb::DT_TIMESTAMP)->getString(),conn.run("timestamp(date(1))")->getString());
    ASSERT_ANY_THROW(dateval->castTemporal(dolphindb::DT_NANOTIME));
    ASSERT_EQ(dateval->castTemporal(dolphindb::DT_NANOTIMESTAMP)->getString(),conn.run("nanotimestamp(date(1))")->getString());
    ASSERT_EQ(dateval->castTemporal(dolphindb::DT_DATEHOUR)->getString(),conn.run("datehour(date(1))")->getString());

    // std::cout<<negSec<<std::endl<<posSec<<std::endl<<negSec3600<<std::endl<<posSec3600<<std::endl;
    ASSERT_EQ(dolphindb::Util::createDate(INT_MIN)->castTemporal(dolphindb::DT_NANOTIMESTAMP)->getString(),"");
    ASSERT_EQ(dolphindb::Util::createDate(INT_MIN)->castTemporal(dolphindb::DT_DATE)->getString(),conn.run("date(date("+std::to_string(INT_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDate(INT_MIN)->castTemporal(dolphindb::DT_MONTH)->getString(),conn.run("month(date("+std::to_string(INT_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDate(INT_MIN)->castTemporal(dolphindb::DT_DATETIME)->getString(),conn.run("datetime(date("+std::to_string(INT_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createDate(INT_MIN)->castTemporal(dolphindb::DT_TIMESTAMP)->getString(),"");
    ASSERT_EQ(dolphindb::Util::createDate(INT_MIN)->castTemporal(dolphindb::DT_DATEHOUR)->getString(),conn.run("datehour(date("+std::to_string(INT_MIN)+"))")->getString());

}

TEST_F(ScalarTest,testFunctionCastTemporal_second){
    srand(time(NULL));

    dolphindb::ConstantSP secondval = dolphindb::Util::createSecond(1);
    ASSERT_ANY_THROW(secondval->castTemporal(dolphindb::DT_CHAR));
    ASSERT_ANY_THROW(secondval->castTemporal(dolphindb::DT_STRING));
    ASSERT_ANY_THROW(secondval->castTemporal(dolphindb::DT_DATE));
    ASSERT_ANY_THROW(secondval->castTemporal(dolphindb::DT_MONTH));
    ASSERT_EQ(secondval->castTemporal(dolphindb::DT_TIME)->getString(),conn.run("time(second(1))")->getString());
    ASSERT_EQ(secondval->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(second(1))")->getString());
    ASSERT_EQ(secondval->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(second(1))")->getString());
    ASSERT_ANY_THROW(secondval->castTemporal(dolphindb::DT_DATETIME));
    ASSERT_ANY_THROW(secondval->castTemporal(dolphindb::DT_TIMESTAMP));
    ASSERT_EQ(secondval->castTemporal(dolphindb::DT_NANOTIME)->getString(),conn.run("nanotime(second(1))")->getString());
    ASSERT_ANY_THROW(secondval->castTemporal(dolphindb::DT_NANOTIMESTAMP));
    ASSERT_ANY_THROW(secondval->castTemporal(dolphindb::DT_DATEHOUR));

    // std::cout<<negSec<<std::endl<<posSec<<std::endl<<negSec3600<<std::endl<<posSec3600<<std::endl;
    ASSERT_EQ(dolphindb::Util::createSecond(INT_MIN)->castTemporal(dolphindb::DT_TIME)->getString(),conn.run("time(second("+std::to_string(INT_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createSecond(INT_MIN)->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(second("+std::to_string(INT_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createSecond(INT_MIN)->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(second("+std::to_string(INT_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createSecond(INT_MIN)->castTemporal(dolphindb::DT_NANOTIME)->getString(),"");

}


TEST_F(ScalarTest,testFunctionCastTemporal_minute){
    srand(time(NULL));

    dolphindb::ConstantSP minuteval = dolphindb::Util::createMinute(1);
    ASSERT_ANY_THROW(minuteval->castTemporal(dolphindb::DT_CHAR));
    ASSERT_ANY_THROW(minuteval->castTemporal(dolphindb::DT_STRING));
    ASSERT_ANY_THROW(minuteval->castTemporal(dolphindb::DT_DATE));
    ASSERT_ANY_THROW(minuteval->castTemporal(dolphindb::DT_MONTH));
    ASSERT_EQ(minuteval->castTemporal(dolphindb::DT_TIME)->getString(),conn.run("time(minute(1))")->getString());
    ASSERT_EQ(minuteval->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(minute(1))")->getString());
    ASSERT_EQ(minuteval->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(minute(1))")->getString());
    ASSERT_ANY_THROW(minuteval->castTemporal(dolphindb::DT_DATETIME));
    ASSERT_ANY_THROW(minuteval->castTemporal(dolphindb::DT_TIMESTAMP));
    ASSERT_EQ(minuteval->castTemporal(dolphindb::DT_NANOTIME)->getString(),conn.run("nanotime(minute(1))")->getString());
    ASSERT_ANY_THROW(minuteval->castTemporal(dolphindb::DT_NANOTIMESTAMP));
    ASSERT_ANY_THROW(minuteval->castTemporal(dolphindb::DT_DATEHOUR));

    // std::cout<<negSec<<std::endl<<posSec<<std::endl<<negSec3600<<std::endl<<posSec3600<<std::endl;
    ASSERT_EQ(dolphindb::Util::createMinute(INT_MIN)->castTemporal(dolphindb::DT_TIME)->getString(),conn.run("time(minute("+std::to_string(INT_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createMinute(INT_MIN)->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(minute("+std::to_string(INT_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createMinute(INT_MIN)->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(minute("+std::to_string(INT_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createMinute(INT_MIN)->castTemporal(dolphindb::DT_NANOTIME)->getString(),"");

}


TEST_F(ScalarTest,testFunctionCastTemporal_time){
    srand(time(NULL));

    dolphindb::ConstantSP timeval = dolphindb::Util::createTime(1);
    ASSERT_ANY_THROW(timeval->castTemporal(dolphindb::DT_CHAR));
    ASSERT_ANY_THROW(timeval->castTemporal(dolphindb::DT_STRING));
    ASSERT_ANY_THROW(timeval->castTemporal(dolphindb::DT_DATE));
    ASSERT_ANY_THROW(timeval->castTemporal(dolphindb::DT_MONTH));
    ASSERT_EQ(timeval->castTemporal(dolphindb::DT_TIME)->getString(),conn.run("time(time(1))")->getString());
    ASSERT_EQ(timeval->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(time(1))")->getString());
    ASSERT_EQ(timeval->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(time(1))")->getString());
    ASSERT_ANY_THROW(timeval->castTemporal(dolphindb::DT_DATETIME));
    ASSERT_ANY_THROW(timeval->castTemporal(dolphindb::DT_TIMESTAMP));
    ASSERT_EQ(timeval->castTemporal(dolphindb::DT_NANOTIME)->getString(),conn.run("nanotime(time(1))")->getString());
    ASSERT_ANY_THROW(timeval->castTemporal(dolphindb::DT_NANOTIMESTAMP));
    ASSERT_ANY_THROW(timeval->castTemporal(dolphindb::DT_DATEHOUR));

    // std::cout<<negSec<<std::endl<<posSec<<std::endl<<negSec3600<<std::endl<<posSec3600<<std::endl;
    ASSERT_EQ(dolphindb::Util::createTime(INT_MIN)->castTemporal(dolphindb::DT_TIME)->getString(),conn.run("time(time("+std::to_string(INT_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTime(INT_MIN)->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(time("+std::to_string(INT_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTime(INT_MIN)->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(time("+std::to_string(INT_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createTime(INT_MIN)->castTemporal(dolphindb::DT_NANOTIME)->getString(),"");

}

TEST_F(ScalarTest,testFunctionCastTemporal_nanotime){
    srand(time(NULL));

    dolphindb::ConstantSP nanotimeval = dolphindb::Util::createNanoTime(1000000);
    ASSERT_ANY_THROW(nanotimeval->castTemporal(dolphindb::DT_CHAR));
    ASSERT_ANY_THROW(nanotimeval->castTemporal(dolphindb::DT_STRING));
    ASSERT_ANY_THROW(nanotimeval->castTemporal(dolphindb::DT_DATE));
    ASSERT_ANY_THROW(nanotimeval->castTemporal(dolphindb::DT_MONTH));
    ASSERT_EQ(nanotimeval->castTemporal(dolphindb::DT_TIME)->getString(),conn.run("time(nanotime(1000000))")->getString());
    ASSERT_EQ(nanotimeval->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(nanotime(1000000))")->getString());
    ASSERT_EQ(nanotimeval->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(nanotime(1000000))")->getString());
    ASSERT_ANY_THROW(nanotimeval->castTemporal(dolphindb::DT_DATETIME));
    ASSERT_ANY_THROW(nanotimeval->castTemporal(dolphindb::DT_TIMESTAMP));
    ASSERT_EQ(nanotimeval->castTemporal(dolphindb::DT_NANOTIME)->getString(),conn.run("nanotime(nanotime(1000000))")->getString());
    ASSERT_ANY_THROW(nanotimeval->castTemporal(dolphindb::DT_NANOTIMESTAMP));
    ASSERT_ANY_THROW(nanotimeval->castTemporal(dolphindb::DT_DATEHOUR));


    // std::cout<<negSec<<std::endl<<posSec<<std::endl<<negSec3600<<std::endl<<posSec3600<<std::endl;
    ASSERT_EQ(dolphindb::Util::createNanoTime(LLONG_MIN)->castTemporal(dolphindb::DT_TIME)->getString(),conn.run("time(nanotime("+std::to_string(LLONG_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTime(LLONG_MIN)->castTemporal(dolphindb::DT_MINUTE)->getString(),conn.run("minute(nanotime("+std::to_string(LLONG_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTime(LLONG_MIN)->castTemporal(dolphindb::DT_SECOND)->getString(),conn.run("second(nanotime("+std::to_string(LLONG_MIN)+"))")->getString());
    ASSERT_EQ(dolphindb::Util::createNanoTime(LLONG_MIN)->castTemporal(dolphindb::DT_NANOTIME)->getString(),"");

}

TEST_F(ScalarTest,testFunction_parseChar){
    dolphindb::ConstantSP charVal;
    charVal = dolphindb::Util::parseConstant(dolphindb::DT_CHAR, "00");
    ASSERT_TRUE(charVal->isNull());

    charVal = dolphindb::Util::parseConstant(dolphindb::DT_CHAR, "");
    ASSERT_TRUE(charVal->isNull());

    charVal = dolphindb::Util::parseConstant(dolphindb::DT_CHAR, "'\\30'");
    ASSERT_TRUE(charVal->isNull());

    charVal = dolphindb::Util::parseConstant(dolphindb::DT_CHAR, "'/3'");
    ASSERT_TRUE(charVal->isNull());

    charVal = dolphindb::Util::parseConstant(dolphindb::DT_CHAR, "'\\3\\");
    ASSERT_TRUE(charVal->isNull());

    charVal = dolphindb::Util::parseConstant(dolphindb::DT_CHAR, "-255");
    ASSERT_TRUE(charVal.isNull()) << "need to change nullptr to isNull()";

    charVal = dolphindb::Util::parseConstant(dolphindb::DT_CHAR, "'5\"");
    ASSERT_TRUE(charVal->isNull());

    charVal = dolphindb::Util::parseConstant(dolphindb::DT_CHAR, "255");
    ASSERT_TRUE(charVal.isNull()) << "need to change nullptr to isNull()";

    std::vector<std::string> charactors = {"'\\0'", "'\\b'", "'\\5'", "'\\n'", "'\\r'", "'\\t'", "'\\v'", "'\\\\'", "'\\''", "'\\\"'", "'a'", "'0'", "'3'", "'\t'", "'\n'", "'\r'", "13", "10"};
    std::vector<char> ex_chars = {'0', 'b', '5', '\n', '\r', '\t', 'v', '\\', '\'', '\"', 'a', '0', '3', '\t', '\n', '\r', '\r', '\n'};
    for(auto i=0; i< charactors.size();i++)
        // std::cout<<dolphindb::Util::parseConstant(dolphindb::DT_CHAR, charactors[i])->getChar()<<std::endl;
        ASSERT_EQ(dolphindb::Util::parseConstant(dolphindb::DT_CHAR, charactors[i])->getChar(),ex_chars[i]);

}

TEST_F(ScalarTest,testFunction_parseBool){
    dolphindb::ConstantSP Val;
    Val = dolphindb::Util::parseConstant(dolphindb::DT_BOOL, "00");
    ASSERT_TRUE(Val->isNull());

    Val = dolphindb::Util::parseConstant(dolphindb::DT_BOOL, "1245");
    ASSERT_TRUE(Val->getBool());

    Val = dolphindb::Util::parseConstant(dolphindb::DT_BOOL, "-12055");
    ASSERT_TRUE(Val->getBool());

    dolphindb::ConstantSP Val1 = conn.run("bool(1)");
    dolphindb::ConstantSP Val2 = dolphindb::Util::parseConstant(dolphindb::DT_BOOL, Val1->getString());
    dolphindb::ConstantSP Val3 = dolphindb::Util::parseConstant(dolphindb::DT_BOOL, "true");
    ASSERT_EQ(Val1->getBool(), Val2->getBool());

    Val1 = conn.run("bool(0)");
    Val2 = dolphindb::Util::parseConstant(dolphindb::DT_BOOL, Val1->getString());
    Val3 = dolphindb::Util::parseConstant(dolphindb::DT_BOOL, "false");
    ASSERT_EQ(Val1->getBool(), Val2->getBool());

}

TEST_F(ScalarTest,testFunction_parseShort){
    dolphindb::ConstantSP shortVal;
    shortVal = dolphindb::Util::parseConstant(dolphindb::DT_SHORT, "00");
    ASSERT_TRUE(shortVal->isNull());

    dolphindb::ConstantSP shortVal1 = conn.run("short(51234)");
    dolphindb::ConstantSP shortVal2 = dolphindb::Util::parseConstant(dolphindb::DT_SHORT, shortVal1->getString());
    dolphindb::ConstantSP shortVal3 = dolphindb::Util::parseConstant(dolphindb::DT_SHORT, "51234");
    ASSERT_EQ(shortVal1->getShort(), shortVal2->getShort());

}

TEST_F(ScalarTest,testFunction_parseInt){
    dolphindb::ConstantSP val;
    val = dolphindb::Util::parseConstant(dolphindb::DT_INT, "00");
    ASSERT_TRUE(val->isNull());

    dolphindb::ConstantSP val1 = conn.run("int(51234)");
    dolphindb::ConstantSP val2 = dolphindb::Util::parseConstant(dolphindb::DT_INT, val1->getString());
    dolphindb::ConstantSP val3 = dolphindb::Util::parseConstant(dolphindb::DT_INT, "51234");
    ASSERT_EQ(val1->getInt(), val2->getInt());

}

TEST_F(ScalarTest,testFunction_parseLong){
    dolphindb::ConstantSP val;
    val = dolphindb::Util::parseConstant(dolphindb::DT_LONG, "00");
    ASSERT_TRUE(val->isNull());

    dolphindb::ConstantSP val1 = conn.run("long(51234)");
    dolphindb::ConstantSP val2 = dolphindb::Util::parseConstant(dolphindb::DT_LONG, val1->getString());
    dolphindb::ConstantSP val3 = dolphindb::Util::parseConstant(dolphindb::DT_LONG, "51234");
    ASSERT_EQ(val1->getLong(), val2->getLong());

}

TEST_F(ScalarTest,testFunction_parseFloat){
    dolphindb::ConstantSP val;
    val = dolphindb::Util::parseConstant(dolphindb::DT_FLOAT, "00");
    ASSERT_TRUE(val->isNull());

    dolphindb::ConstantSP ex_val = conn.run("float(3.2314)");
    val = dolphindb::Util::parseConstant(dolphindb::DT_FLOAT, "3.2314");
    ASSERT_EQ(ex_val->getFloat(), val->getFloat());

}

TEST_F(ScalarTest,testFunction_parseDouble){
    dolphindb::ConstantSP val;
    val = dolphindb::Util::parseConstant(dolphindb::DT_DOUBLE, "00");
    ASSERT_TRUE(val->isNull());

    dolphindb::ConstantSP ex_val = conn.run("double(3.2314)");
    val = dolphindb::Util::parseConstant(dolphindb::DT_DOUBLE, "3.2314");
    ASSERT_EQ(ex_val->getDouble(), val->getDouble());

}

TEST_F(ScalarTest,testFunction_parseNanoTime){
    dolphindb::ConstantSP nanots;
    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIME, "00");
    ASSERT_TRUE(nanots->isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIME, "25:00:01.443197923");
    ASSERT_TRUE(nanots.isNull()) << "need to change to isNull()";

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIME, "13-00:01.443197923");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIME, "00:88:01.443197923");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIME, "00:00-01.443197923");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIME, "00:00:88.443197923");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIME, "00:00:01-443197923");
    ASSERT_EQ(nanots->getString(), "00:00:01.000000000");

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIME, "00:00:30.443197923212");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIME, "00:00:33.443197");
    ASSERT_EQ(nanots->getString(), "00:00:33.443197000");

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIME, "00:00:33.443");
    ASSERT_EQ(nanots->getString(), "00:00:33.443000000");


    dolphindb::ConstantSP nanots1 = conn.run("nanotime(2738549)");
    dolphindb::ConstantSP nanots2 = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIME, nanots1->getString());
    dolphindb::ConstantSP nanots3 = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIME, "00:00:00.002738549");
    ASSERT_EQ(nanots1->getString(), nanots2->getString());
}


TEST_F(ScalarTest,testFunction_parseNanoTimestamp){
    dolphindb::ConstantSP nanots;
    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIMESTAMP, "00");
    ASSERT_TRUE(nanots->isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIMESTAMP, "0000.01.01T00:00:01.443197923");
    ASSERT_TRUE(nanots.isNull()) << "need to change nullptr to isNull()";

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIMESTAMP, "2020-01.01T00:00:01.443197923");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIMESTAMP, "2022.00.01T00:00:01.443197923");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIMESTAMP, "2022.01-01T00:00:01.443197923");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIMESTAMP, "2022.01.00T00:00:01.443197923");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIMESTAMP, "2022.01.01-00:00:01.443197923");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIMESTAMP, "2022.01.20 25:00:01.443197923");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIMESTAMP, "2022.01.20 13-00:01.443197923");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIMESTAMP, "2022.01.01-00:00:01.443197923");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIMESTAMP, "2022.01.01 00:88:01.443197923");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIMESTAMP, "2022.01.01 00:00-01.443197923");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIMESTAMP, "2022.01.01 00:00:88.443197923");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIMESTAMP, "2022.01.01 00:00:01-443197923");
    ASSERT_EQ(nanots->getString(), "2022.01.01T00:00:01.000000000");

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIMESTAMP, "2022.01.01 00:00:30.443197923212");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIMESTAMP, "2022.01.01 00:00:33.443197");
    ASSERT_EQ(nanots->getString(), "2022.01.01T00:00:33.443197000");

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIMESTAMP, "2022.01.01 00:00:33.443");
    ASSERT_EQ(nanots->getString(), "2022.01.01T00:00:33.443000000");


    dolphindb::ConstantSP nanots1 = conn.run("nanotimestamp(2738549)");
    dolphindb::ConstantSP nanots2 = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIMESTAMP, nanots1->getString());
    dolphindb::ConstantSP nanots3 = dolphindb::Util::parseConstant(dolphindb::DT_NANOTIMESTAMP, "1970.01.01 00:00:00.002738549");
    ASSERT_EQ(nanots1->getString(), nanots2->getString());
}


TEST_F(ScalarTest,testFunction_parseTimestamp){
    dolphindb::ConstantSP ts;
    ts = dolphindb::Util::parseConstant(dolphindb::DT_TIMESTAMP, "00");
    ASSERT_TRUE(ts->isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_TIMESTAMP, "0000.01.01T00:00:01.443");
    ASSERT_TRUE(ts.isNull()) << "need to change nullptr to isNull()";

    ts = dolphindb::Util::parseConstant(dolphindb::DT_TIMESTAMP, "2020-01.01T00:00:01.443");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_TIMESTAMP, "2022.00.01T00:00:01.443");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_TIMESTAMP, "2022.01-01T00:00:01.443");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_TIMESTAMP, "2022.01.00T00:00:01.443");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_TIMESTAMP, "2022.01.01-00:00:01.443");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_TIMESTAMP, "2022.01.20 25:00:01.443");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_TIMESTAMP, "2022.01.20 13-00:01.443");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_TIMESTAMP, "2022.01.01-00:00:01.443");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_TIMESTAMP, "2022.01.01 00:88:01.443");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_TIMESTAMP, "2022.01.01 00:00-01.443");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_TIMESTAMP, "2022.01.01 00:00:88.443");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_TIMESTAMP, "2022.01.01 00:00:01-443");
    ASSERT_EQ(ts->getString(), "2022.01.01T00:00:01.000");

    ts = dolphindb::Util::parseConstant(dolphindb::DT_TIMESTAMP, "2022.01.01 00:00:30.443212");
    ASSERT_EQ(ts->getString(), "2022.01.01T00:00:30.443");


    dolphindb::ConstantSP ts1 = conn.run("timestamp(2738549)");
    dolphindb::ConstantSP ts2 = dolphindb::Util::parseConstant(dolphindb::DT_TIMESTAMP, ts1->getString());
    dolphindb::ConstantSP ts3 = dolphindb::Util::parseConstant(dolphindb::DT_TIMESTAMP, "1970.01.01T00:45:38.549");
    ASSERT_EQ(ts1->getString(), ts2->getString());
}


TEST_F(ScalarTest,testFunction_parseTime){
    dolphindb::ConstantSP nanots;
    nanots = dolphindb::Util::parseConstant(dolphindb::DT_TIME, "00");
    ASSERT_TRUE(nanots->isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_TIME, "25:00:01.443");
    ASSERT_TRUE(nanots.isNull()) << "need to change to isNull()";

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_TIME, "13-00:01.443");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_TIME, "00:88:01.443");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_TIME, "00:00-01.443");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_TIME, "00:00:88.443");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_TIME, "00:00:01-443");
    ASSERT_EQ(nanots->getString(), "00:00:01.000");

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_TIME, "00:00:30.443212");
    ASSERT_TRUE(nanots.isNull());

    dolphindb::ConstantSP ts1 = conn.run("time(2738549)");
    dolphindb::ConstantSP ts2 = dolphindb::Util::parseConstant(dolphindb::DT_TIME, ts1->getString());
    dolphindb::ConstantSP ts3 = dolphindb::Util::parseConstant(dolphindb::DT_TIME, "00:45:38.549");
    ASSERT_EQ(ts1->getString(), ts2->getString());
}

TEST_F(ScalarTest,testFunction_parseSecond){
    dolphindb::ConstantSP nanots;
    nanots = dolphindb::Util::parseConstant(dolphindb::DT_SECOND, "00");
    ASSERT_TRUE(nanots->isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_SECOND, "25:00:01");
    ASSERT_TRUE(nanots.isNull()) << "need to change to isNull()";

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_SECOND, "13-00:01");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_SECOND, "00:88:01");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_SECOND, "00:00-01");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_SECOND, "00:00:88");
    ASSERT_TRUE(nanots.isNull());

    dolphindb::ConstantSP ts1 = conn.run("second(3663)");
    dolphindb::ConstantSP ts2 = dolphindb::Util::parseConstant(dolphindb::DT_SECOND, ts1->getString());
    dolphindb::ConstantSP ts3 = dolphindb::Util::parseConstant(dolphindb::DT_SECOND, "01:01:03");
    ASSERT_EQ(ts1->getString(), ts2->getString());
}


TEST_F(ScalarTest,testFunction_parseMinute){
    dolphindb::ConstantSP nanots;
    nanots = dolphindb::Util::parseConstant(dolphindb::DT_MINUTE, "00");
    ASSERT_TRUE(nanots->isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_MINUTE, "88:01");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_MINUTE, "00-01");
    ASSERT_TRUE(nanots.isNull());

    nanots = dolphindb::Util::parseConstant(dolphindb::DT_MINUTE, "00:88");
    ASSERT_TRUE(nanots.isNull());

    dolphindb::ConstantSP ts1 = conn.run("minute(534)");
    dolphindb::ConstantSP ts3 = dolphindb::Util::parseConstant(dolphindb::DT_MINUTE, "08:54");
    ASSERT_EQ(ts1->getString(), ts3->getString());
}


TEST_F(ScalarTest,testFunction_parseDate){
    dolphindb::ConstantSP ts;
    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATE, "00");
    ASSERT_TRUE(ts->isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATE, "2022.01");
    ASSERT_TRUE(ts.isNull()) << "need to change nullptr to isNull()";

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATE, "0000.01.01");
    ASSERT_TRUE(ts.isNull()) << "need to change nullptr to isNull()";

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATE, "2020-01.01");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATE, "2022.00.01");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATE, "2022.01-01");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATE, "2022.01.00");
    ASSERT_TRUE(ts.isNull());

    dolphindb::ConstantSP ts1 = conn.run("date(2738549)");
    dolphindb::ConstantSP ts2 = dolphindb::Util::parseConstant(dolphindb::DT_DATE, ts1->getString());
    dolphindb::ConstantSP ts3 = dolphindb::Util::parseConstant(dolphindb::DT_DATE, "9467.11.23");
    ASSERT_EQ(ts1->getString(), ts2->getString());
}


TEST_F(ScalarTest,testFunction_parseMonth){
    dolphindb::ConstantSP ts;
    ts = dolphindb::Util::parseConstant(dolphindb::DT_MONTH, "00");
    ASSERT_TRUE(ts->isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_MONTH, "2022");
    ASSERT_TRUE(ts.isNull()) << "need to change nullptr to isNull()";

    ts = dolphindb::Util::parseConstant(dolphindb::DT_MONTH, "0000.01");
    ASSERT_TRUE(ts.isNull()) << "need to change nullptr to isNull()";

    ts = dolphindb::Util::parseConstant(dolphindb::DT_MONTH, "2020-01");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_MONTH, "2022.00");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_MONTH, "2022.15");
    ASSERT_TRUE(ts.isNull());

    dolphindb::ConstantSP ts1 = conn.run("month(2783)");
    dolphindb::ConstantSP ts3 = dolphindb::Util::parseConstant(dolphindb::DT_MONTH, "0231.12");
    ASSERT_EQ(ts1->getString(), ts3->getString());
}


TEST_F(ScalarTest,testFunction_parseDatetime){
    dolphindb::ConstantSP ts;
    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATETIME, "00");
    ASSERT_TRUE(ts->isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATETIME, "0000.01.01T00:00:01");
    ASSERT_TRUE(ts.isNull()) << "need to change nullptr to isNull()";

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATETIME, "2020-01.01T00:00:01");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATETIME, "2022.00.01T00:00:01");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATETIME, "2022.01-01T00:00:01");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATETIME, "2022.01.00T00:00:01");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATETIME, "2022.01.01-00:00:01");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATETIME, "2022.01.20 25:00:01");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATETIME, "2022.01.20 13-00:01");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATETIME, "2022.01.01-00:00:01");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATETIME, "2022.01.01 00:88:01");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATETIME, "2022.01.01 00:00-01");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATETIME, "2022.01.01 00:00:88");
    ASSERT_TRUE(ts.isNull());

    dolphindb::ConstantSP ts1 = conn.run("datetime(2738549)");
    dolphindb::ConstantSP ts2 = dolphindb::Util::parseConstant(dolphindb::DT_DATETIME, ts1->getString());
    dolphindb::ConstantSP ts3 = dolphindb::Util::parseConstant(dolphindb::DT_DATETIME, "1970.02.01T16:42:29");
    ASSERT_EQ(ts1->getString(), ts2->getString());
}


TEST_F(ScalarTest,testFunction_parseDatehour){
    dolphindb::ConstantSP ts;
    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATEHOUR, "00");
    ASSERT_TRUE(ts->isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATEHOUR, "2022.01");
    ASSERT_TRUE(ts.isNull()) << "need to change nullptr to isNull()";

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATEHOUR, "0000.01.01T00");
    ASSERT_TRUE(ts.isNull()) << "need to change nullptr to isNull()";

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATEHOUR, "2020-01.01T00");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATEHOUR, "2022.00.01T00");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATEHOUR, "2022.01-01T00");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATEHOUR, "2022.01.00T00");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATEHOUR, "2022.01.01-00");
    ASSERT_TRUE(ts.isNull());

    ts = dolphindb::Util::parseConstant(dolphindb::DT_DATEHOUR, "2022.01.20 25");
    ASSERT_TRUE(ts.isNull());

    dolphindb::ConstantSP ts1 = conn.run("datehour(2738549)");
    dolphindb::ConstantSP ts2 = dolphindb::Util::parseConstant(dolphindb::DT_DATEHOUR, ts1->getString());
    dolphindb::ConstantSP ts3 = dolphindb::Util::parseConstant(dolphindb::DT_DATEHOUR, "2282.05.31T05");
    ASSERT_EQ(ts1->getString(), ts2->getString());
}

TEST_F(ScalarTest,testFunction_parseIP4){
    dolphindb::ConstantSP ipv4Val;
    ipv4Val = dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.12F.12");
    ASSERT_TRUE(ipv4Val.isNull());

    ipv4Val = dolphindb::Util::parseConstant(dolphindb::DT_IP, ".168.12.12");
    ASSERT_EQ(ipv4Val->getString(), "0.168.12.12");


    ipv4Val = dolphindb::Util::parseConstant(dolphindb::DT_IP, "192.168.111.12");
    dolphindb::ConstantSP ipv4Val1 = dolphindb::Util::parseConstant(dolphindb::DT_IP, "255.255..");
    dolphindb::ConstantSP ipv4Val2 = dolphindb::Util::parseConstant(dolphindb::DT_IP, "124.013.22.1");
    dolphindb::ConstantSP ipv4Val3 = dolphindb::Util::parseConstant(dolphindb::DT_IP, "0.0.000.55");
    dolphindb::ConstantSP ipv4Val4 = dolphindb::Util::parseConstant(dolphindb::DT_IP, "0.0.0.0");

    // std::cout<<ipv4Val->getString()<<std::endl<<ipv4Val1->getString()<<std::endl<<ipv4Val2->getString()<<std::endl<<ipv4Val3->getString()<<std::endl<<ipv4Val4->getString()<<std::endl;
    ASSERT_EQ(ipv4Val->getString(), "192.168.111.12");
    ASSERT_EQ(ipv4Val1->getString(), "255.255.0.0");
    ASSERT_EQ(ipv4Val2->getString(), "124.13.22.1");
    ASSERT_EQ(ipv4Val3->getString(), "0.0.0.55");
    ASSERT_EQ(ipv4Val4->getString(), "0.0.0.0");
}

TEST_F(ScalarTest,testFunction_parseIP6){
    dolphindb::ConstantSP ipv6Val;
    ipv6Val = dolphindb::Util::parseConstant(dolphindb::DT_IP, "2001:3CA1:010F:001A:121B:0000:2C3B:0010");
    dolphindb::ConstantSP ipv6Val1 = dolphindb::Util::parseConstant(dolphindb::DT_IP, "2001:3CA1:010F:001A:121B:0000:3100:0");
    dolphindb::ConstantSP ipv6Val2 = dolphindb::Util::parseConstant(dolphindb::DT_IP, ":3CA1:10F:001A:121B:::10");
    dolphindb::ConstantSP ipv6Val3 = dolphindb::Util::parseConstant(dolphindb::DT_IP, "2001:3CA1:010F:1A:121B:00::0010");
    dolphindb::ConstantSP ipv6Val4 = dolphindb::Util::parseConstant(dolphindb::DT_IP, "0:0:0:0:0:0:0:0");

    // std::cout<<ipv6Val->getString()<<std::endl<<ipv6Val1->getString()<<std::endl<<ipv6Val2->getString()<<std::endl<<ipv6Val3->getString()<<std::endl<<ipv6Val4->getString()<<std::endl;
    ASSERT_EQ(ipv6Val->getString(), "2001:3ca1:10f:1a:121b:0:2c3b:10");
    ASSERT_EQ(ipv6Val1->getString(), "2001:3ca1:10f:1a:121b:0:3100:0");
    ASSERT_EQ(ipv6Val2->getString(), "0:3ca1:10f:1a:121b::10");
    ASSERT_EQ(ipv6Val3->getString(), "2001:3ca1:10f:1a:121b::10");
    ASSERT_EQ(ipv6Val4->getString(), "0.0.0.0");
}

TEST_F(ScalarTest,testFunction_parseInt128){
    dolphindb::ConstantSP val;
    val = dolphindb::Util::parseConstant(dolphindb::DT_INT128, "");
    ASSERT_EQ(val->getString(), "00000000000000000000000000000000");

    val = dolphindb::Util::parseConstant(dolphindb::DT_INT128, "e1671797c52e15f763380b45e841ec32");
    ASSERT_EQ(val->getString(), "e1671797c52e15f763380b45e841ec32");

}

TEST_F(ScalarTest,testFunction_parseUUID){
    dolphindb::ConstantSP val;
    val = dolphindb::Util::parseConstant(dolphindb::DT_UUID, "");
    ASSERT_EQ(val->getString(), "00000000-0000-0000-0000-000000000000");

    val = dolphindb::Util::parseConstant(dolphindb::DT_UUID, "5d212a78-cc48-e3b1-4235-b4d91473ee87");
    ASSERT_EQ(val->getString(), "5d212a78-cc48-e3b1-4235-b4d91473ee87");
}

TEST_F(ScalarTest,test_string_exception){
    std::string ss("123\0 123", 8);
    {
        try{
            dolphindb::Util::createString(ss);
            ASSERT_TRUE(false);
        }catch(const std::exception& e){
            ASSERT_EQ(std::string(e.what()), "A String cannot contain the character '\\0'");
        }
    }
    {
        try{
            dolphindb::ConstantSP s = dolphindb::Util::createString("aaa");
            s->setString(ss);
            ASSERT_TRUE(false);
        }catch(const std::exception& e){
            ASSERT_EQ(std::string(e.what()), "A String cannot contain the character '\\0'");
        }
    }
}


class ScalarTest_download : public ScalarTest, public testing::WithParamInterface<std::tuple<std::string, std::string, dolphindb::DATA_TYPE>> 
{
public:
    static std::vector<std::tuple<std::string, std::string, dolphindb::DATA_TYPE>> getData(){
        return {
            std::make_tuple(std::string("x=false;x"), std::string("0"), dolphindb::DT_BOOL),
            std::make_tuple(std::string("x=127c;x"), std::string("127"), dolphindb::DT_CHAR),
            std::make_tuple(std::string("x=2147483647;x"), std::string("2147483647"), dolphindb::DT_INT),
            std::make_tuple(std::string("x=-32768h;x"), std::string(""), dolphindb::DT_SHORT),
            std::make_tuple(std::string("x=9223372036854775807l;x"), std::string("9223372036854775807"), dolphindb::DT_LONG),
            std::make_tuple(std::string("x=3.14f;x"), std::string("3.14"), dolphindb::DT_FLOAT),
            std::make_tuple(std::string("x=3.1415926;x"), std::string("3.141593"), dolphindb::DT_DOUBLE),
            std::make_tuple(std::string("x=\"hello,world!\";x"), std::string("hello,world!"), dolphindb::DT_STRING),
            std::make_tuple(std::string("x=symbol(`a`b`c)[0];x"), std::string("a"), dolphindb::DT_SYMBOL),
            std::make_tuple(std::string("x=blob(`a`b`c)[0];x"), std::string("a"), dolphindb::DT_BLOB),
            std::make_tuple(std::string("x=int128(`0123456789abcdef0123456789abcdef);x"), std::string("0123456789abcdef0123456789abcdef"), dolphindb::DT_INT128),
            std::make_tuple(std::string("x=ipaddr('192.168.1.1');x"), std::string("192.168.1.1"), dolphindb::DT_IP),
            std::make_tuple(std::string("x=uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87');x"), std::string("5d212a78-cc48-e3b1-4235-b4d91473ee87"), dolphindb::DT_UUID),
            std::make_tuple(std::string("x=date(2022.01.01);x"), std::string("2022.01.01"), dolphindb::DT_DATE),
            std::make_tuple(std::string("x=month('2022.01');x"), std::string("2022.01M"), dolphindb::DT_MONTH),
            std::make_tuple(std::string("x=13:30:00.000;x"), std::string("13:30:00.000"), dolphindb::DT_TIME),
            std::make_tuple(std::string("x=minute(534);x"), std::string("08:54m"), dolphindb::DT_MINUTE),
            std::make_tuple(std::string("x=second(534);x"), std::string("00:08:54"), dolphindb::DT_SECOND),
            std::make_tuple(std::string("x=datetime(2022.01.01T13:30:00);x"), std::string("2022.01.01T13:30:00"), dolphindb::DT_DATETIME),
            std::make_tuple(std::string("x=timestamp(2022.01.01T13:30:00.123);x"), std::string("2022.01.01T13:30:00.123"), dolphindb::DT_TIMESTAMP),
            std::make_tuple(std::string("x=nanotime(13:30:00.123456789);x"), std::string("13:30:00.123456789"), dolphindb::DT_NANOTIME),
            std::make_tuple(std::string("x=nanotimestamp(2022.01.01T13:30:00.123456789);x"), std::string("2022.01.01T13:30:00.123456789"), dolphindb::DT_NANOTIMESTAMP)
        };
    }
};
INSTANTIATE_TEST_SUITE_P(, ScalarTest_download, testing::ValuesIn(ScalarTest_download::getData()));

TEST_P(ScalarTest_download, test_download_scalar) {
    std::string script = std::get<0>(GetParam());
    std::string ex = std::get<1>(GetParam());
    dolphindb::DATA_TYPE type = std::get<2>(GetParam());
    dolphindb::ConstantSP res = conn.run(script);
    ASSERT_EQ(res->getString(), ex);
    ASSERT_TRUE(res->isScalar());
    ASSERT_EQ(res->getType(), type == dolphindb::DT_SYMBOL? dolphindb::DT_STRING : type);
}

TEST_F(ScalarTest, UDL_success){
    using dolphindb::operator ""_d;
    using dolphindb::operator ""_M;
    using dolphindb::operator ""_t;
    using dolphindb::operator ""_m;
    using dolphindb::operator ""_s;
    using dolphindb::operator ""_D;
    using dolphindb::operator ""_T;
    using dolphindb::operator ""_n;
    using dolphindb::operator ""_N;
    dolphindb::ConstantSP date = "2025.12.19"_d;
    dolphindb::ConstantSP month = "2025.12"_M;
    dolphindb::ConstantSP time = "09:35:24.123"_t;
    dolphindb::ConstantSP minute = "09:35"_m;
    dolphindb::ConstantSP second = "09:35:24"_s;
    dolphindb::ConstantSP datetime = "2025.12.19T09:35:24"_D;
    dolphindb::ConstantSP timestamp = "2025.12.19T09:35:24.123"_T;
    dolphindb::ConstantSP nanotime = "09:35:24.123456789"_n;
    dolphindb::ConstantSP nanotimestamp = "2025.12.19T09:35:24.123456789"_N;
    ASSERT_EQ(date->getString(), "2025.12.19");
    ASSERT_EQ(date->getType(), dolphindb::DT_DATE);
    ASSERT_EQ(month->getString(), "2025.12M");
    ASSERT_EQ(month->getType(), dolphindb::DT_MONTH);
    ASSERT_EQ(time->getString(), "09:35:24.123");
    ASSERT_EQ(time->getType(), dolphindb::DT_TIME);
    ASSERT_EQ(minute->getString(), "09:35m");
    ASSERT_EQ(minute->getType(), dolphindb::DT_MINUTE);
    ASSERT_EQ(second->getString(), "09:35:24");
    ASSERT_EQ(second->getType(), dolphindb::DT_SECOND);
    ASSERT_EQ(datetime->getString(), "2025.12.19T09:35:24");
    ASSERT_EQ(datetime->getType(), dolphindb::DT_DATETIME);
    ASSERT_EQ(timestamp->getString(), "2025.12.19T09:35:24.123");
    ASSERT_EQ(timestamp->getType(), dolphindb::DT_TIMESTAMP);
    ASSERT_EQ(nanotime->getString(), "09:35:24.123456789");
    ASSERT_EQ(nanotime->getType(), dolphindb::DT_NANOTIME);
    ASSERT_EQ(nanotimestamp->getString(), "2025.12.19T09:35:24.123456789");
    ASSERT_EQ(nanotimestamp->getType(), dolphindb::DT_NANOTIMESTAMP);
}

TEST_F(ScalarTest, UDL_error){
    using dolphindb::operator ""_d;
    using dolphindb::operator ""_M;
    using dolphindb::operator ""_t;
    using dolphindb::operator ""_m;
    using dolphindb::operator ""_s;
    using dolphindb::operator ""_D;
    using dolphindb::operator ""_T;
    using dolphindb::operator ""_n;
    using dolphindb::operator ""_N;
    dolphindb::ConstantSP date = ""_d;
    dolphindb::ConstantSP month = ""_M;
    dolphindb::ConstantSP time = ""_t;
    dolphindb::ConstantSP minute = ""_m;
    dolphindb::ConstantSP second = ""_s;
    dolphindb::ConstantSP datetime = ""_D;
    dolphindb::ConstantSP timestamp = ""_T;
    dolphindb::ConstantSP nanotime = ""_n;
    dolphindb::ConstantSP nanotimestamp = ""_N;
    ASSERT_TRUE(date.isNull());
    ASSERT_TRUE(month.isNull());
    ASSERT_TRUE(time.isNull());
    ASSERT_TRUE(minute.isNull());
    ASSERT_TRUE(second.isNull());
    ASSERT_TRUE(datetime.isNull());
    ASSERT_TRUE(timestamp.isNull());
    ASSERT_TRUE(nanotime.isNull());
    ASSERT_TRUE(nanotimestamp.isNull());
}