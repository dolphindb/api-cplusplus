#include <gtest/gtest.h>
#include "config.h"
#include "StreamReplicator.h"

class ReplicatorConfigTest : public testing::Test
{
    public:
        static void SetUpTestSuite()
        {
        }
        static void TearDownTestSuite()
        {
        }

    protected:
        virtual void SetUp()
        {

        }
        virtual void TearDown()
        {

        }
};

TEST_F(ReplicatorConfigTest, set_batching_batchSize_less_than_1){
    dolphindb::ReplicatorConfig config;
    ASSERT_ANY_THROW(config.setBatching(0, std::chrono::seconds{5}));
}

TEST_F(ReplicatorConfigTest, set_batching_batchInterval_less_than_0){
    dolphindb::ReplicatorConfig config;
    ASSERT_ANY_THROW(config.setBatching(1, std::chrono::seconds{-1}));
}

TEST_F(ReplicatorConfigTest, set_batching){
    dolphindb::ReplicatorConfig config;
    config.setBatching(3, std::chrono::seconds{5});
}

TEST_F(ReplicatorConfigTest, set_retry_maxRetry_less_than_minus_one){
    dolphindb::ReplicatorConfig config;
    ASSERT_ANY_THROW(config.setRetry(-2, std::chrono::seconds{5}));
}

TEST_F(ReplicatorConfigTest, set_retry_retryInterval_less_than_0){
    dolphindb::ReplicatorConfig config;
    ASSERT_ANY_THROW(config.setRetry(0, std::chrono::seconds{-1}));
}

TEST_F(ReplicatorConfigTest, set_retry){
    dolphindb::ReplicatorConfig config;
    config.setRetry(3, std::chrono::seconds{5});
}

TEST_F(ReplicatorConfigTest, setCompression){
    dolphindb::ReplicatorConfig config;
    config.setCompression({dolphindb::COMPRESS_NONE, dolphindb::COMPRESS_LZ4, dolphindb::COMPRESS_DELTA});
}

TEST_F(ReplicatorConfigTest, onDataDump){
    dolphindb::ReplicatorConfig config;
    config.onDataDump([](const std::string& hostLabel, dolphindb::ConstantSP table) -> bool {
        return true;
    });
}

TEST_F(ReplicatorConfigTest, onConnectionStateChange){
    dolphindb::ReplicatorConfig config;
    config.onConnectionStateChange([](const std::string &hostLabel, dolphindb::ConnectionState state) -> bool {
        return true;
    });
}

class StreamReplicatorTest : public testing::Test, public testing::WithParamInterface<std::tuple<std::string, std::vector<dolphindb::ConstantSP>, std::vector<std::string>>>
{
    public:
        static dolphindb::DBConnection conn;
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
        static std::vector<std::tuple<std::string, std::vector<dolphindb::ConstantSP>, std::vector<std::string>>> getBasicData(){
            // BOOL[]
            dolphindb::VectorSP bool_1 = dolphindb::Util::createVector(dolphindb::DT_BOOL,0,3);
            dolphindb::VectorSP bool_2 = dolphindb::Util::createVector(dolphindb::DT_BOOL,0,1);
            dolphindb::ConstantSP bool_true = dolphindb::Util::createBool(true);
            dolphindb::ConstantSP bool_false = dolphindb::Util::createBool(false);
            dolphindb::ConstantSP bool_null = dolphindb::Util::createNullConstant(dolphindb::DT_BOOL);
            bool_1->append(bool_true);
            bool_1->append(bool_false);
            bool_1->append(bool_null);
            bool_2->append(bool_null);
            // CHAR[]
            dolphindb::VectorSP char_1 = dolphindb::Util::createVector(dolphindb::DT_CHAR,0,3);
            dolphindb::VectorSP char_2 = dolphindb::Util::createVector(dolphindb::DT_BOOL,0,1);
            dolphindb::ConstantSP char_max = dolphindb::Util::createChar(127);
            dolphindb::ConstantSP char_min = dolphindb::Util::createChar(-127);
            dolphindb::ConstantSP char_0 = dolphindb::Util::createChar(0);
            dolphindb::ConstantSP char_null = dolphindb::Util::createNullConstant(dolphindb::DT_CHAR);
            char_1->append(char_max);
            char_1->append(char_min);
            char_1->append(char_0);
            char_1->append(char_null);
            char_2->append(char_null);
            // SHORT[]
            dolphindb::VectorSP short_1 = dolphindb::Util::createVector(dolphindb::DT_SHORT,0,3);
            dolphindb::VectorSP short_2 = dolphindb::Util::createVector(dolphindb::DT_SHORT,0,1);
            dolphindb::ConstantSP short_max = dolphindb::Util::createShort(32767);
            dolphindb::ConstantSP short_min = dolphindb::Util::createShort(-32767);
            dolphindb::ConstantSP short_0 = dolphindb::Util::createShort(0);
            dolphindb::ConstantSP short_null = dolphindb::Util::createNullConstant(dolphindb::DT_SHORT);
            short_1->append(short_max);
            short_1->append(short_min);
            short_1->append(short_0);
            short_1->append(short_null);
            short_2->append(short_null);
            // INT[]
            dolphindb::VectorSP int_1 = dolphindb::Util::createVector(dolphindb::DT_INT,0,3);
            dolphindb::VectorSP int_2 = dolphindb::Util::createVector(dolphindb::DT_INT,0,1);
            dolphindb::ConstantSP int_max = dolphindb::Util::createInt(2147483647);
            dolphindb::ConstantSP int_min = dolphindb::Util::createInt(-2147483647);
            dolphindb::ConstantSP int_0 = dolphindb::Util::createInt(0);
            dolphindb::ConstantSP int_null = dolphindb::Util::createNullConstant(dolphindb::DT_INT);
            int_1->append(int_max);
            int_1->append(int_min);
            int_1->append(int_0);
            int_1->append(int_null);
            int_2->append(int_null);
            // LONG[]
            dolphindb::VectorSP long_1 = dolphindb::Util::createVector(dolphindb::DT_LONG,0,3);
            dolphindb::VectorSP long_2 = dolphindb::Util::createVector(dolphindb::DT_LONG,0,1);
            dolphindb::ConstantSP long_max = dolphindb::Util::createLong(9223372036854775807);
            dolphindb::ConstantSP long_min = dolphindb::Util::createLong(-9223372036854775807);
            dolphindb::ConstantSP long_0 = dolphindb::Util::createLong(0);
            dolphindb::ConstantSP long_null = dolphindb::Util::createNullConstant(dolphindb::DT_LONG);
            long_1->append(long_max);
            long_1->append(long_min);
            long_1->append(long_0);
            long_1->append(long_null);
            long_2->append(long_null);
            // DATE[]
            dolphindb::VectorSP date_1 = dolphindb::Util::createVector(dolphindb::DT_DATE,0,2);
            dolphindb::VectorSP date_2 = dolphindb::Util::createVector(dolphindb::DT_DATE,0,1);
            dolphindb::ConstantSP date_0 = dolphindb::Util::createDate(0);
            dolphindb::ConstantSP date_ = dolphindb::Util::createDate(19132);
            dolphindb::ConstantSP date_null = dolphindb::Util::createNullConstant(dolphindb::DT_DATE);
            date_1->append(date_0);
            date_1->append(date_);
            date_1->append(date_null);
            date_2->append(date_null);
            // MONTH[]
            dolphindb::VectorSP month_1 = dolphindb::Util::createVector(dolphindb::DT_MONTH,0,2);
            dolphindb::VectorSP month_2 = dolphindb::Util::createVector(dolphindb::DT_MONTH,0,1);
            dolphindb::ConstantSP month_0 = dolphindb::Util::createMonth(0);
            dolphindb::ConstantSP month_ = dolphindb::Util::createMonth(23640);
            dolphindb::ConstantSP month_null = dolphindb::Util::createNullConstant(dolphindb::DT_MONTH);
            month_1->append(month_0);
            month_1->append(month_);
            month_1->append(month_null);
            month_2->append(month_null);
            // DATEHOUR[]
            dolphindb::VectorSP datehour_1 = dolphindb::Util::createVector(dolphindb::DT_DATEHOUR,0,2);
            dolphindb::VectorSP datehour_2 = dolphindb::Util::createVector(dolphindb::DT_DATEHOUR,0,1);
            dolphindb::ConstantSP datehour_0 = dolphindb::Util::createDateHour(0);
            dolphindb::ConstantSP datehour_ = dolphindb::Util::createDateHour(1);
            dolphindb::ConstantSP datehour_null = dolphindb::Util::createNullConstant(dolphindb::DT_DATEHOUR);
            datehour_1->append(datehour_0);
            datehour_1->append(datehour_);
            datehour_1->append(datehour_null);
            datehour_2->append(datehour_null);
            // MINUTE[]
            dolphindb::VectorSP minute_1 = dolphindb::Util::createVector(dolphindb::DT_MINUTE,0,2);
            dolphindb::VectorSP minute_2 = dolphindb::Util::createVector(dolphindb::DT_MINUTE,0,1);
            dolphindb::ConstantSP minute_0 = dolphindb::Util::createMinute(0);
            dolphindb::ConstantSP minute_ = dolphindb::Util::createMinute(1);
            dolphindb::ConstantSP minute_null = dolphindb::Util::createNullConstant(dolphindb::DT_MINUTE);
            minute_1->append(minute_0);
            minute_1->append(minute_);
            minute_1->append(minute_null);
            minute_2->append(minute_null);
            // SECOND[]
            dolphindb::VectorSP second_1 = dolphindb::Util::createVector(dolphindb::DT_SECOND,0,2);
            dolphindb::VectorSP second_2 = dolphindb::Util::createVector(dolphindb::DT_SECOND,0,1);
            dolphindb::ConstantSP second_0 = dolphindb::Util::createSecond(0);
            dolphindb::ConstantSP second_ = dolphindb::Util::createSecond(1);
            dolphindb::ConstantSP second_null = dolphindb::Util::createNullConstant(dolphindb::DT_SECOND);
            second_1->append(second_0);
            second_1->append(second_);
            second_1->append(second_null);
            second_2->append(second_null);
            // TIME[]
            dolphindb::VectorSP time_1 = dolphindb::Util::createVector(dolphindb::DT_TIME,0,2);
            dolphindb::VectorSP time_2 = dolphindb::Util::createVector(dolphindb::DT_TIME,0,1);
            dolphindb::ConstantSP time_0 = dolphindb::Util::createTime(0);
            dolphindb::ConstantSP time_ = dolphindb::Util::createTime(1);
            dolphindb::ConstantSP time_null = dolphindb::Util::createNullConstant(dolphindb::DT_TIME);
            time_1->append(time_0);
            time_1->append(time_);
            time_1->append(time_null);
            time_2->append(time_null);
            // DATETIME[]
            dolphindb::VectorSP datetime_1 = dolphindb::Util::createVector(dolphindb::DT_DATETIME,0,2);
            dolphindb::VectorSP datetime_2 = dolphindb::Util::createVector(dolphindb::DT_DATETIME,0,1);
            dolphindb::ConstantSP datetime_0 = dolphindb::Util::createDateTime(0);
            dolphindb::ConstantSP datetime_ = dolphindb::Util::createDateTime(1);
            dolphindb::ConstantSP datetime_null = dolphindb::Util::createNullConstant(dolphindb::DT_DATETIME);
            datetime_1->append(datetime_0);
            datetime_1->append(datetime_);
            datetime_1->append(datetime_null);
            datetime_2->append(datetime_null);
            // TIMESTAMP[]
            dolphindb::VectorSP timestamp_1 = dolphindb::Util::createVector(dolphindb::DT_TIMESTAMP,0,2);
            dolphindb::VectorSP timestamp_2 = dolphindb::Util::createVector(dolphindb::DT_TIMESTAMP,0,1);
            dolphindb::ConstantSP timestamp_0 = dolphindb::Util::createTimestamp(0);
            dolphindb::ConstantSP timestamp_ = dolphindb::Util::createTimestamp(1);
            dolphindb::ConstantSP timestamp_null = dolphindb::Util::createNullConstant(dolphindb::DT_TIMESTAMP);
            timestamp_1->append(timestamp_0);
            timestamp_1->append(timestamp_);
            timestamp_1->append(timestamp_null);
            timestamp_2->append(timestamp_null);
            // NANOTIME[]
            dolphindb::VectorSP nanotime_1 = dolphindb::Util::createVector(dolphindb::DT_NANOTIME,0,2);
            dolphindb::VectorSP nanotime_2 = dolphindb::Util::createVector(dolphindb::DT_NANOTIME,0,1);
            dolphindb::ConstantSP nanotime_0 = dolphindb::Util::createNanoTime(0);
            dolphindb::ConstantSP nanotime_ = dolphindb::Util::createNanoTime(1);
            dolphindb::ConstantSP nanotime_null = dolphindb::Util::createNullConstant(dolphindb::DT_NANOTIME);
            nanotime_1->append(nanotime_0);
            nanotime_1->append(nanotime_);
            nanotime_1->append(nanotime_null);
            nanotime_2->append(nanotime_null);
            // NANOTIMESTAMP[]
            dolphindb::VectorSP nanotimestamp_1 = dolphindb::Util::createVector(dolphindb::DT_NANOTIMESTAMP,0,2);
            dolphindb::VectorSP nanotimestamp_2 = dolphindb::Util::createVector(dolphindb::DT_NANOTIMESTAMP,0,1);
            dolphindb::ConstantSP nanotimestamp_0 = dolphindb::Util::createNanoTimestamp(0);
            dolphindb::ConstantSP nanotimestamp_ = dolphindb::Util::createNanoTimestamp(1);
            dolphindb::ConstantSP nanotimestamp_null = dolphindb::Util::createNullConstant(dolphindb::DT_NANOTIMESTAMP);
            nanotimestamp_1->append(nanotimestamp_0);
            nanotimestamp_1->append(nanotimestamp_);
            nanotimestamp_1->append(nanotimestamp_null);
            nanotimestamp_2->append(nanotimestamp_null);
            // FLOAT[]
            dolphindb::VectorSP float_1 = dolphindb::Util::createVector(dolphindb::DT_FLOAT,0,2);
            dolphindb::VectorSP float_2 = dolphindb::Util::createVector(dolphindb::DT_FLOAT,0,1);
            dolphindb::ConstantSP float_ = dolphindb::Util::createFloat(3.14);
            dolphindb::ConstantSP float_nan = dolphindb::Util::createFloat(NAN);
            dolphindb::ConstantSP float_inf = dolphindb::Util::createFloat(INFINITY);
            dolphindb::ConstantSP float_null = dolphindb::Util::createNullConstant(dolphindb::DT_FLOAT);
            float_1->append(float_);
            float_1->append(float_nan);
            float_1->append(float_inf);
            float_1->append(float_null);
            float_2->append(float_null);
            // DOUBLE[]
            dolphindb::VectorSP double_1 = dolphindb::Util::createVector(dolphindb::DT_DOUBLE,0,2);
            dolphindb::VectorSP double_2 = dolphindb::Util::createVector(dolphindb::DT_DOUBLE,0,1);
            dolphindb::ConstantSP double_ = dolphindb::Util::createDouble(3.14);
            dolphindb::ConstantSP double_nan = dolphindb::Util::createDouble(NAN);
            dolphindb::ConstantSP double_inf = dolphindb::Util::createDouble(INFINITY);
            dolphindb::ConstantSP double_null = dolphindb::Util::createNullConstant(dolphindb::DT_DOUBLE);
            double_1->append(double_);
            double_1->append(double_nan);
            double_1->append(double_inf);
            double_1->append(double_null);
            double_2->append(double_null);
            // INT128[]
            dolphindb::VectorSP int128_1 = dolphindb::Util::createVector(dolphindb::DT_INT128,0,2);
            dolphindb::VectorSP int128_2 = dolphindb::Util::createVector(dolphindb::DT_INT128,0,1);
            dolphindb::ConstantSP int128_normal = dolphindb::Util::parseConstant(dolphindb::DT_INT128,"e1671797c52e15f763380b45e841ec32");
            dolphindb::ConstantSP int128_null = dolphindb::Util::createNullConstant(dolphindb::DT_INT128);
            int128_1->append(int128_normal);
            int128_1->append(int128_normal);
            int128_1->append(int128_normal);
            int128_2->append(int128_null);
            // UUID[]
            dolphindb::VectorSP uuid_1 = dolphindb::Util::createVector(dolphindb::DT_UUID,0,2);
            dolphindb::VectorSP uuid_2 = dolphindb::Util::createVector(dolphindb::DT_UUID,0,1);
            dolphindb::ConstantSP uuid_normal = dolphindb::Util::parseConstant(dolphindb::DT_UUID,"5d212a78-cc48-e3b1-4235-b4d91473ee87");
            dolphindb::ConstantSP uuid_null = dolphindb::Util::createNullConstant(dolphindb::DT_UUID);
            uuid_1->append(uuid_normal);
            uuid_1->append(uuid_normal);
            uuid_1->append(uuid_normal);
            uuid_2->append(uuid_null);
            // IPADDR[]
            dolphindb::VectorSP ip_1 = dolphindb::Util::createVector(dolphindb::DT_IP,0,2);
            dolphindb::VectorSP ip_2 = dolphindb::Util::createVector(dolphindb::DT_IP,0,1);
            dolphindb::ConstantSP ip_normal = dolphindb::Util::parseConstant(dolphindb::DT_IP,"127.0.0.1");
            dolphindb::ConstantSP ip_null = dolphindb::Util::createNullConstant(dolphindb::DT_IP);
            ip_1->append(ip_normal);
            ip_1->append(ip_normal);
            ip_1->append(ip_normal);
            ip_2->append(ip_null);
            // DECIMAL32(2)[]
            dolphindb::VectorSP decimal32_1 = dolphindb::Util::createVector(dolphindb::DT_DECIMAL32,0,2,true,2);
            dolphindb::VectorSP decimal32_2 = dolphindb::Util::createVector(dolphindb::DT_DECIMAL32,0,1,true,2);
            dolphindb::ConstantSP decimal32_314 = dolphindb::Util::createDecimal32(2, 3.14);
            dolphindb::ConstantSP decimal32_315 = dolphindb::Util::createDecimal32(2, 3.15);
            dolphindb::ConstantSP decimal32_null = dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL32, 2);
            decimal32_1->append(decimal32_314);
            decimal32_1->append(decimal32_315);
            decimal32_1->append(decimal32_null);
            decimal32_2->append(decimal32_null);
            // DECIMAL64(6)[]
            dolphindb::VectorSP decimal64_1 = dolphindb::Util::createVector(dolphindb::DT_DECIMAL64,0,2,true,6);
            dolphindb::VectorSP decimal64_2 = dolphindb::Util::createVector(dolphindb::DT_DECIMAL64,0,1,true,6);
            dolphindb::ConstantSP decimal64_3141592 = dolphindb::Util::createDecimal64(6, 3.141592);
            dolphindb::ConstantSP decimal64_3141593 = dolphindb::Util::createDecimal64(6, 3.141593);
            dolphindb::ConstantSP decimal64_null = dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL64, 6);
            decimal64_1->append(decimal64_3141592);
            decimal64_1->append(decimal64_3141593);
            decimal64_1->append(decimal64_null);
            decimal64_2->append(decimal64_null);
            // DECIMAL128(8)[]
            dolphindb::VectorSP decimal128_1 = dolphindb::Util::createVector(dolphindb::DT_DECIMAL128,0,2,true,8);
            dolphindb::VectorSP decimal128_2 = dolphindb::Util::createVector(dolphindb::DT_DECIMAL128,0,1,true,8);
            dolphindb::ConstantSP decimal128_314159265 = dolphindb::Util::createDecimal32(8, 3.14159265);
            dolphindb::ConstantSP decimal128_314159266 = dolphindb::Util::createDecimal32(8, 3.14159266);
            dolphindb::ConstantSP decimal128_null = dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL128, 8);
            decimal128_1->append(decimal128_314159265);
            decimal128_1->append(decimal128_314159266);
            decimal128_1->append(decimal128_null);
            decimal128_2->append(decimal128_null);
            return {
                {"BOOL", {dolphindb::Util::createBool(true), dolphindb::Util::createBool(false), dolphindb::Util::createBool((char)-128)}, {"1", "0", ""}},
                {"CHAR", {dolphindb::Util::createChar(127), dolphindb::Util::createChar(-127), dolphindb::Util::createChar(0), dolphindb::Util::createChar(-128)}, {"127", "-127", "0", ""}},
                {"SHORT", {dolphindb::Util::createShort(32767), dolphindb::Util::createShort(-32767), dolphindb::Util::createShort(0), dolphindb::Util::createShort(-32768)}, {"32767", "-32767", "0", ""}},
                {"INT", {dolphindb::Util::createInt(2147483647), dolphindb::Util::createInt(-2147483647), dolphindb::Util::createInt(0), dolphindb::Util::createInt(-2147483648)}, {"2147483647", "-2147483647", "0", ""}},
                {"LONG", {dolphindb::Util::createLong(9223372036854775807), dolphindb::Util::createLong(-9223372036854775807), dolphindb::Util::createLong(0), dolphindb::Util::createLong(-9223372036854775808)}, {"9223372036854775807", "-9223372036854775807", "0", ""}},
                {"DATE", {dolphindb::Util::createDate(0), dolphindb::Util::createDate(19132), dolphindb::Util::createDate(-2147483648)}, {"1970.01.01", "2022.05.20", ""}},
                {"MONTH", {dolphindb::Util::createMonth(23640), dolphindb::Util::createMonth(0), dolphindb::Util::createMonth(-2147483648)}, {"1970.01M", "0000.01M", ""}},
                {"DATEHOUR", {dolphindb::Util::createDateHour(0), dolphindb::Util::createDateHour(1), dolphindb::Util::createDateHour(-2147483648)}, {"1970.01.01T00", "1970.01.01T01", ""}},
                {"MINUTE", {dolphindb::Util::createMinute(0), dolphindb::Util::createMinute(1), dolphindb::Util::createMinute(-2147483648)}, {"00:00m", "00:01m", ""}},
                {"SECOND", {dolphindb::Util::createSecond(0), dolphindb::Util::createSecond(1), dolphindb::Util::createSecond(-2147483648)}, {"00:00:00", "00:00:01", ""}},
                {"TIME", {dolphindb::Util::createTime(0), dolphindb::Util::createTime(1), dolphindb::Util::createTime(-2147483648)}, {"00:00:00.000", "00:00:00.001", ""}},
                {"DATETIME", {dolphindb::Util::createDateTime(0), dolphindb::Util::createDateTime(1), dolphindb::Util::createDateTime(-2147483648)}, {"1970.01.01T00:00:00", "1970.01.01T00:00:01", ""}},
                {"TIMESTAMP", {dolphindb::Util::createTimestamp(0), dolphindb::Util::createTimestamp(1), dolphindb::Util::createTimestamp(-9223372036854775808)}, {"1970.01.01T00:00:00.000", "1970.01.01T00:00:00.001", ""}},
                {"NANOTIME", {dolphindb::Util::createNanoTime(0), dolphindb::Util::createNanoTime(1), dolphindb::Util::createNanoTime(-9223372036854775808)}, {"00:00:00.000000000", "00:00:00.000000001", ""}},
                {"NANOTIMESTAMP", {dolphindb::Util::createNanoTimestamp(0), dolphindb::Util::createNanoTimestamp(1), dolphindb::Util::createNanoTimestamp(-9223372036854775808)}, {"1970.01.01T00:00:00.000000000", "1970.01.01T00:00:00.000000001", ""}},
                {"FLOAT", {dolphindb::Util::createFloat(3.14), dolphindb::Util::createFloat(NAN), dolphindb::Util::createFloat(INFINITY)}, {"3.14", "NaN", "inf"}},
                {"DOUBLE", {dolphindb::Util::createDouble(3.14), dolphindb::Util::createDouble(NAN), dolphindb::Util::createDouble(INFINITY)}, {"3.14", "NaN", "inf"}},
                {"SYMBOL", {dolphindb::Util::createString("abc!@#中文 123"), dolphindb::Util::createString("")}, {"abc!@#中文 123", ""}},
                {"STRING", {dolphindb::Util::createString("abc!@#中文 123"), dolphindb::Util::createString("")}, {"abc!@#中文 123", ""}},
                {"BLOB", {dolphindb::Util::createBlob("abc!@#中文 123"), dolphindb::Util::createBlob("")}, {"abc!@#中文 123", ""}},
                {"INT128", {dolphindb::Util::parseConstant(dolphindb::DT_INT128,"e1671797c52e15f763380b45e841ec32"),dolphindb::Util::parseConstant(dolphindb::DT_INT128,"00000000000000000000000000000000")},{"e1671797c52e15f763380b45e841ec32","00000000000000000000000000000000"}},
                {"UUID", {dolphindb::Util::parseConstant(dolphindb::DT_UUID,"5d212a78-cc48-e3b1-4235-b4d91473ee87"),dolphindb::Util::parseConstant(dolphindb::DT_UUID,"00000000-0000-0000-0000-000000000000")},{"5d212a78-cc48-e3b1-4235-b4d91473ee87","00000000-0000-0000-0000-000000000000"}},
                {"IPADDR", {dolphindb::Util::parseConstant(dolphindb::DT_IP,"127.0.0.1"),dolphindb::Util::parseConstant(dolphindb::DT_IP,"0.0.0.0")},{"127.0.0.1","0.0.0.0"}},
                {"DECIMAL32(2)", {dolphindb::Util::createDecimal32(2, 3.14), dolphindb::Util::createDecimal32(2, 3.15)}, {"3.14", "3.15"}},
                {"DECIMAL64(6)", {dolphindb::Util::createDecimal64(6, 3.141592), dolphindb::Util::createDecimal64(6, 3.141593)}, {"3.141592", "3.141593"}},
                {"DECIMAL128(8)", {dolphindb::Util::createDecimal128(8, 3.14159265), dolphindb::Util::createDecimal32(8, 3.14159266)}, {"3.14159265", "3.14159266"}},
                {"BOOL[]", {bool_1, bool_2}, {"[1,0,]", "[00b]"}},
                {"CHAR[]", {char_1, char_2}, {"[127,-127,0,]", "[00c]"}},
                {"SHORT[]", {short_1, short_2}, {"[32767,-32767,0,]", "[00h]"}},
                {"INT[]", {int_1, int_2}, {"[2147483647,-2147483647,0,]", "[00i]"}},
                {"LONG[]", {long_1, long_2}, {"[9223372036854775807,-9223372036854775807,0,]", "[00l]"}},
                {"DATE[]", {date_1, date_2}, {"[1970.01.01,2022.05.20,]", "[00d]"}},
                {"MONTH[]", {month_1, month_2}, {"[0000.01M,1970.01M,]", "[00M]"}},
                {"DATEHOUR[]", {datehour_1, datehour_2}, {"[1970.01.01T00,1970.01.01T01,]", "[00 ]"}},
                {"MINUTE[]", {minute_1, minute_2}, {"[00:00m,00:01m,]", "[00m]"}},
                {"SECOND[]", {second_1, second_2}, {"[00:00:00,00:00:01,]", "[00s]"}},
                {"TIME[]", {time_1, time_2}, {"[00:00:00.000,00:00:00.001,]", "[00t]"}},
                {"DATETIME[]", {datetime_1, datetime_2}, {"[1970.01.01T00:00:00,1970.01.01T00:00:01,]", "[00D]"}},
                {"TIMESTAMP[]", {timestamp_1, timestamp_2}, {"[1970.01.01T00:00:00.000,1970.01.01T00:00:00.001,]", "[00T]"}},
                {"NANOTIME[]", {nanotime_1, nanotime_2}, {"[00:00:00.000000000,00:00:00.000000001,]", "[00n]"}},
                {"NANOTIMESTAMP[]", {nanotimestamp_1, nanotimestamp_2}, {"[1970.01.01T00:00:00.000000000,1970.01.01T00:00:00.000000001,]", "[00N]"}},
                {"FLOAT[]", {float_1, float_2}, {"[3.14,NaN,inf,]", "[00f]"}},
                {"DOUBLE[]", {double_1, double_2}, {"[3.14,NaN,inf,]", "[00F]"}},
                {"INT128[]", {int128_1, int128_2}, {"[e1671797c52e15f763380b45e841ec32,e1671797c52e15f763380b45e841ec32,e1671797c52e15f763380b45e841ec32]", "[00000000000000000000000000000000]"}},
                {"UUID[]", {uuid_1, uuid_2}, {"[5d212a78-cc48-e3b1-4235-b4d91473ee87,5d212a78-cc48-e3b1-4235-b4d91473ee87,5d212a78-cc48-e3b1-4235-b4d91473ee87]", "[00000000-0000-0000-0000-000000000000]"}},
                {"IPADDR[]", {ip_1, ip_2}, {"[127.0.0.1,127.0.0.1,127.0.0.1]", "[0.0.0.0]"}},
                {"DECIMAL32(2)[]", {decimal32_1, decimal32_2}, {"[3.14,3.15,]", "[]"}},
                {"DECIMAL64(6)[]", {decimal64_1, decimal64_2}, {"[3.141592,3.141593,]", "[]"}},
                {"DECIMAL128(8)[]", {decimal128_1, decimal128_2}, {"[3.14159265,3.14159266,]", "[]"}},
            };
        }

    protected:
        virtual void SetUp()
        {

        }
        virtual void TearDown()
        {

        }
};

dolphindb::DBConnection StreamReplicatorTest::conn;

TEST_F(StreamReplicatorTest, constructor_hosts_size_less_than_2){
    ASSERT_ANY_THROW(dolphindb::StreamReplicator streamReplocator({{HOST, PORT, USER, PASSWD}}, "test"));
}

TEST_F(StreamReplicatorTest, constructor_hosts_empty){
    ASSERT_ANY_THROW(dolphindb::StreamReplicator streamReplocator(std::vector<dolphindb::HostInfo>{}, "test"));
}

TEST_F(StreamReplicatorTest, constructor_hosts_error){
    ASSERT_ANY_THROW(dolphindb::StreamReplicator streamReplocator({{HOST, 123, USER, PASSWD},{HOST, 123, USER, PASSWD}}, "test"));
}

TEST_F(StreamReplicatorTest, constructor_table_schema_mismatch_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share table(1..3 as id) as "+table_name);
    conn_2.run("share table(1..3 as data) as "+table_name);
    ASSERT_ANY_THROW(dolphindb::StreamReplicator streamReplocator({{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER}}, table_name));
}

TEST_F(StreamReplicatorTest, constructor){
    std::string table_name = getCaseName();
    conn.run("share table(1..3 as id) as " + table_name);
    dolphindb::StreamReplicator streamReplocator({{HOST, PORT, USER, PASSWD}, {HOST, PORT, USER, PASSWD}}, table_name);
}

TEST_F(StreamReplicatorTest, insert_data_never_sucess_all_fail_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share table(array(INT) as data) as "+table_name);
    conn_2.run("share table(array(INT) as data) as "+table_name);
    dolphindb::StreamReplicator streamReplocator{{{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "conn_1"}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, "conn_2"}}, table_name};
    ASSERT_ANY_THROW(streamReplocator.insert(dolphindb::Util::createString("abc")));
}

INSTANTIATE_TEST_SUITE_P(basic_types, StreamReplicatorTest, testing::ValuesIn(StreamReplicatorTest::getBasicData()));

TEST_P(StreamReplicatorTest, insert_data_all_type_should_serial){
    std::string data_type = std::get<0>(GetParam());
    std::vector<dolphindb::ConstantSP> data = std::get<1>(GetParam());
    std::vector<std::string> expect = std::get<2>(GetParam());
    auto len = data.size();
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share table(array("+data_type+") as data) as "+table_name);
    conn_2.run("share table(array("+data_type+") as data) as "+table_name);
    dolphindb::StreamReplicator streamReplocator{{{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "conn_1"}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, "conn_2"}}, table_name};
    for (auto& i : data)
        streamReplocator.insert(i);
    unsigned times = 0;
    while (streamReplocator.getStatus().streams["conn_1"].insertedRows < len || streamReplocator.getStatus().streams["conn_2"].insertedRows < len){
        if (times >= 10)
            ASSERT_TRUE(false);
        std::cout << streamReplocator.getStatus().streams["conn_1"].errorMsg << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        ++times;
    }
    dolphindb::TableSP res_1 = conn_1.run(table_name);
    dolphindb::TableSP res_2 = conn_2.run(table_name);
    ASSERT_EQ(res_1->size(),len);
    ASSERT_EQ(res_2->size(),len);
    ASSERT_EQ(res_1->getString(), res_2->getString());
    for (size_t i=0; i < len; ++i)
        ASSERT_EQ(res_1->getColumn(0)->getRow(i)->getString(), expect[i]);
}

TEST_P(StreamReplicatorTest, insert_data_all_type_stream_table_should_serial){
    std::string data_type = std::get<0>(GetParam());
    std::vector<dolphindb::ConstantSP> data = std::get<1>(GetParam());
    std::vector<std::string> expect = std::get<2>(GetParam());
    auto len = data.size();
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share streamTable(array("+data_type+") as data) as "+table_name);
    conn_2.run("share streamTable(array("+data_type+") as data) as "+table_name);
    dolphindb::StreamReplicator streamReplocator{{{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "conn_1"}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, "conn_2"}}, table_name};
    for (auto& i : data)
        streamReplocator.insert(i);
    unsigned times = 0;
    while (streamReplocator.getStatus().streams["conn_1"].insertedRows < len || streamReplocator.getStatus().streams["conn_2"].insertedRows < len){
        if (times >= 10)
            ASSERT_TRUE(false);
        std::cout << streamReplocator.getStatus().streams["conn_1"].errorMsg << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        ++times;
    }
    dolphindb::TableSP res_1 = conn_1.run(table_name);
    dolphindb::TableSP res_2 = conn_2.run(table_name);
    ASSERT_EQ(res_1->size(),len);
    ASSERT_EQ(res_2->size(),len);
    ASSERT_EQ(res_1->getString(), res_2->getString());
    for (size_t i=0; i < len; ++i)
        ASSERT_EQ(res_1->getColumn(0)->getRow(i)->getString(), expect[i]);
}

TEST_F(StreamReplicatorTest, insert_data_all_type_from_cpp_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share table(100:0, "
        "`c_bool`c_char`c_short`c_int`c_long`c_date`c_month`c_datehour`c_minute`c_second`c_time`c_datetime`c_timestamp`c_nanotime`c_nanotimestamp`c_float`c_double`c_symbol`c_string`c_blob`c_int128`c_uuid`c_ip`c_decimal32`c_decimal64`c_decimal128"
        "`c_bool_av`c_char_av`c_short_av`c_int_av`c_long_av`c_date_av`c_month_av`c_datehour_av`c_minute_av`c_second_av`c_time_av`c_datetime_av`c_timestamp_av`c_nanotime_av`c_nanotimestamp_av`c_float_av`c_double_av`c_int128_av`c_uuid_av`c_ip_av`c_decimal32_av`c_decimal64_av`c_decimal128_av,"
        "[BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, DATEHOUR, MINUTE, SECOND, TIME, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, INT128, UUID, IPADDR, DECIMAL32(2), DECIMAL64(6), DECIMAL128(8), "
        "BOOL[], CHAR[], SHORT[], INT[], LONG[], DATE[], MONTH[], DATEHOUR[], MINUTE[], SECOND[], TIME[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], FLOAT[], DOUBLE[], INT128[], UUID[], IPADDR[], DECIMAL32(2)[], DECIMAL64(6)[], DECIMAL128(8)[]]) as "+table_name);
    conn_2.run("share table(100:0, "
        "`c_bool`c_char`c_short`c_int`c_long`c_date`c_month`c_datehour`c_minute`c_second`c_time`c_datetime`c_timestamp`c_nanotime`c_nanotimestamp`c_float`c_double`c_symbol`c_string`c_blob`c_int128`c_uuid`c_ip`c_decimal32`c_decimal64`c_decimal128"
        "`c_bool_av`c_char_av`c_short_av`c_int_av`c_long_av`c_date_av`c_month_av`c_datehour_av`c_minute_av`c_second_av`c_time_av`c_datetime_av`c_timestamp_av`c_nanotime_av`c_nanotimestamp_av`c_float_av`c_double_av`c_int128_av`c_uuid_av`c_ip_av`c_decimal32_av`c_decimal64_av`c_decimal128_av,"
        "[BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, DATEHOUR, MINUTE, SECOND, TIME, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, INT128, UUID, IPADDR, DECIMAL32(2), DECIMAL64(6), DECIMAL128(8), "
        "BOOL[], CHAR[], SHORT[], INT[], LONG[], DATE[], MONTH[], DATEHOUR[], MINUTE[], SECOND[], TIME[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], FLOAT[], DOUBLE[], INT128[], UUID[], IPADDR[], DECIMAL32(2)[], DECIMAL64(6)[], DECIMAL128(8)[]]) as "+table_name);
    {   
        dolphindb::StreamReplicator streamReplocator({{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "conn_1"}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, "conn_2"}}, table_name);
        streamReplocator.insert(
            true, 127, 32767, 2147483647, 9223372036854775807, 0, 23640, 0, 0, 0, 0, 0, 0L, 0L, 0L, 3.14, 3.14, "abc!@#中文 123", "abc!@#中文 123", "abc!@#中文 123", "e1671797c52e15f763380b45e841ec32", "5d212a78-cc48-e3b1-4235-b4d91473ee87", "127.0.0.1", 3.14, 3.141592, 3.14159265, 
            std::vector<bool>{true, false}, std::vector<char>{127, -127, 0, -128}, std::vector<short>{32767, -32767, 0, -32768}, std::vector<int>{2147483647,-2147483647,0,-INT32_MIN}, std::vector<long long>{9223372036854775807,-9223372036854775807,0,INT64_MIN},
            std::vector<int>{0,19132}, std::vector<int>{0,23640}, std::vector<int>{0,1}, std::vector<int>{0,1}, std::vector<int>{0,1}, std::vector<int>{0,1}, std::vector<int>{0,1}, std::vector<long long>{0,1}, std::vector<long long>{0,1}, std::vector<long long>{0,1}, std::vector<float>{3.14, NAN, INFINITY}, std::vector<double>{3.14, NAN, INFINITY}, 
            std::vector<std::string>{3,"e1671797c52e15f763380b45e841ec32"}, std::vector<std::string>{3,"5d212a78-cc48-e3b1-4235-b4d91473ee87"}, std::vector<std::string>{3,"127.0.0.1"}, std::vector<double>{3.14,3.15}, std::vector<double>{3.141592,3.141593}, std::vector<double>{3.14159265,3.14159266}
        );
    }
    dolphindb::TableSP res_1 = conn_1.run(table_name);
    dolphindb::TableSP res_2 = conn_2.run(table_name);
    ASSERT_EQ(res_1->size(),1);
    ASSERT_EQ(res_2->size(),1);
    ASSERT_EQ(res_1->getString(), res_2->getString());
    std::vector<std::string> expect={
        "1", "127", "32767", "2147483647", "9223372036854775807", "1970.01.01", "1970.01M", "1970.01.01T00", "00:00m", "00:00:00", "00:00:00.000", "1970.01.01T00:00:00", "1970.01.01T00:00:00.000", "00:00:00.000000000", "1970.01.01T00:00:00.000000000", 
        "3.14", "3.14", "abc!@#中文 123", "abc!@#中文 123", "abc!@#中文 123", "e1671797c52e15f763380b45e841ec32", "5d212a78-cc48-e3b1-4235-b4d91473ee87", "127.0.0.1", "3.14", "3.141592", "3.14159265", "[1,0]", "[127,-127,0,]", "[32767,-32767,0,]", "[2147483647,-2147483647,0,]", "[9223372036854775807,-9223372036854775807,0,]", "[1970.01.01,2022.05.20]", 
        "[0000.01M,1970.01M]", "[1970.01.01T00,1970.01.01T01]", "[00:00m,00:01m]", "[00:00:00,00:00:01]", "[00:00:00.000,00:00:00.001]", "[1970.01.01T00:00:00,1970.01.01T00:00:01]", "[1970.01.01T00:00:00.000,1970.01.01T00:00:00.001]", "[00:00:00.000000000,00:00:00.000000001]", 
        "[1970.01.01T00:00:00.000000000,1970.01.01T00:00:00.000000001]", "[3.14,NaN,inf]", "[3.14,NaN,inf]", "[e1671797c52e15f763380b45e841ec32,e1671797c52e15f763380b45e841ec32,e1671797c52e15f763380b45e841ec32]", "[5d212a78-cc48-e3b1-4235-b4d91473ee87,5d212a78-cc48-e3b1-4235-b4d91473ee87,5d212a78-cc48-e3b1-4235-b4d91473ee87]", "[127.0.0.1,127.0.0.1,127.0.0.1]", "[3.14,3.15]", "[3.141592,3.141593]", "[3.14159265,3.14159266]"
    };
    for (int i=0;i<49;++i)
        ASSERT_EQ(res_1->getColumn(i)->getRow(0)->getString(), expect[i]);
}

TEST_F(StreamReplicatorTest, insert_data_any_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share streamTable(array(ANY) as data) as "+table_name);
    conn_2.run("share streamTable(array(ANY) as data) as "+table_name);
    ASSERT_ANY_THROW(dolphindb::StreamReplicator streamReplocator({{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "conn_1"}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, "conn_2"}}, table_name));
    // dolphindb::ConstantSP scalar_ = dolphindb::Util::createInt(1);
    // dolphindb::VectorSP pair_ = dolphindb::Util::createPair(dolphindb::DT_INT);
    // pair_->append(scalar_);
    // pair_->append(scalar_);
    // dolphindb::VectorSP vector_ = dolphindb::Util::createVector(dolphindb::DT_INT,0,2);
    // vector_->append(scalar_);
    // vector_->append(scalar_);
    // vector_->append(scalar_);
    // dolphindb::VectorSP array_vector_ = dolphindb::Util::createArrayVector(dolphindb::DT_INT_ARRAY,0,2);
    // array_vector_->append(vector_);
    // array_vector_->append(vector_);
    // dolphindb::VectorSP matrix_ = dolphindb::Util::createMatrix(dolphindb::DT_INT, 2, 2, 2);
    // dolphindb::SetSP set_ = dolphindb::Util::createSet(dolphindb::DT_INT,0);
    // set_->append(scalar_);
    // dolphindb::DictionarySP dict_ = dolphindb::Util::createDictionary(dolphindb::DT_INT,dolphindb::DT_INT);
    // dict_->set(scalar_,scalar_);
    // dolphindb::TableSP table_ = dolphindb::Util::createTable({"a"},{vector_});
    // std::vector<dolphindb::ConstantSP> data{scalar_, pair_, vector_, array_vector_, matrix_, set_, dict_, table_};
    // for (auto& i : data)
    //     streamReplocator.insert(i);
}

TEST_F(StreamReplicatorTest, insert_conn_3_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_3{HOST_CLUSTER, PORT_DNODE3, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share streamTable(array(BOOL) as data) as "+table_name);
    conn_2.run("share streamTable(array(BOOL) as data) as "+table_name);
    conn_3.run("share streamTable(array(BOOL) as data) as "+table_name);
    {
        dolphindb::StreamReplicator streamReplocator{{{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "conn_1"}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, "conn_2"}, {HOST_CLUSTER, PORT_DNODE3, USER_CLUSTER, PASSWD_CLUSTER, "conn_3"}}, table_name};
        dolphindb::ConstantSP data = dolphindb::Util::createBool(true);
        for (size_t i = 0;i<100;++i)
            streamReplocator.insert(data);
    }
    dolphindb::TableSP out_1 = conn_1.run(table_name);
    dolphindb::TableSP out_2 = conn_2.run(table_name);
    dolphindb::TableSP out_3 = conn_3.run(table_name);
    ASSERT_EQ(out_1->size(), 100);
    ASSERT_EQ(out_2->size(), 100);
    ASSERT_EQ(out_3->size(), 100);
}

TEST_F(StreamReplicatorTest, insert_concurrency_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_3{HOST_CLUSTER, PORT_DNODE3, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share streamTable(array(INT) as data) as "+table_name+"_1");
    conn_2.run("share streamTable(array(INT) as data) as "+table_name+"_1");
    conn_3.run("share streamTable(array(INT) as data) as "+table_name+"_1");
    conn_1.run("share streamTable(array(INT) as data) as "+table_name+"_2");
    conn_2.run("share streamTable(array(INT) as data) as "+table_name+"_2");
    conn_3.run("share streamTable(array(INT) as data) as "+table_name+"_2");
    {
        dolphindb::StreamReplicator streamReplocator_1{{{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "conn_1"}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, "conn_2"}, {HOST_CLUSTER, PORT_DNODE3, USER_CLUSTER, PASSWD_CLUSTER, "conn_3"}}, table_name+"_1"};
        dolphindb::StreamReplicator streamReplocator_2{{{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "conn_1"}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, "conn_2"}, {HOST_CLUSTER, PORT_DNODE3, USER_CLUSTER, PASSWD_CLUSTER, "conn_3"}}, table_name+"_2"};
        dolphindb::ConstantSP data_1 = dolphindb::Util::createInt(0);
        dolphindb::ConstantSP data_2 = dolphindb::Util::createInt(1);
        for (size_t i = 0;i<100;++i){
            streamReplocator_1.insert(data_1);
            streamReplocator_2.insert(data_2);
        }
    }
    dolphindb::TableSP out_1_1 = conn_1.run(table_name+"_1");
    dolphindb::TableSP out_1_2 = conn_2.run(table_name+"_1");
    dolphindb::TableSP out_1_3 = conn_3.run(table_name+"_1");
    dolphindb::TableSP out_2_1 = conn_1.run(table_name+"_2");
    dolphindb::TableSP out_2_2 = conn_2.run(table_name+"_2");
    dolphindb::TableSP out_2_3 = conn_3.run(table_name+"_2");
    ASSERT_EQ(out_1_1->size(), 100);
    ASSERT_EQ(out_1_2->size(), 100);
    ASSERT_EQ(out_1_3->size(), 100);
    ASSERT_EQ(out_2_1->size(), 100);
    ASSERT_EQ(out_2_2->size(), 100);
    ASSERT_EQ(out_2_3->size(), 100);
    for (size_t i=0;i<100;++i){
        ASSERT_EQ(out_1_1->getColumn(0)->getRow(i)->getString(),"0");
        ASSERT_EQ(out_1_2->getColumn(0)->getRow(i)->getString(),"0");
        ASSERT_EQ(out_1_2->getColumn(0)->getRow(i)->getString(),"0");
        ASSERT_EQ(out_2_1->getColumn(0)->getRow(i)->getString(),"1");
        ASSERT_EQ(out_2_2->getColumn(0)->getRow(i)->getString(),"1");
        ASSERT_EQ(out_2_2->getColumn(0)->getRow(i)->getString(),"1");
    }
}

TEST_F(StreamReplicatorTest, insert_data_500000_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share streamTable(array(BOOL) as data) as "+table_name);
    conn_2.run("share streamTable(array(BOOL) as data) as "+table_name);
    {
        dolphindb::StreamReplicator streamReplocator{{{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "conn_1"}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, "conn_2"}}, table_name};
        dolphindb::ConstantSP data = dolphindb::Util::createBool(true);
        for (size_t i = 0;i<500000;++i)
            streamReplocator.insert(data);
    }
    dolphindb::TableSP out_1 = conn_1.run(table_name);
    dolphindb::TableSP out_2 = conn_2.run(table_name);
    ASSERT_EQ(out_1->size(), 500000);
    ASSERT_EQ(out_2->size(), 500000);
}

TEST_F(StreamReplicatorTest, insert_data_batch_size_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share table(array(INT) as data) as "+table_name);
    conn_2.run("share table(array(INT) as data) as "+table_name);
    dolphindb::ReplicatorConfig config;
    config.setBatching(5, std::chrono::seconds{100});
    dolphindb::StreamReplicator streamReplocator{{{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "conn_1"}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, "conn_2"}}, table_name, config};
    for (auto i : std::vector<int>{0, 1, 2, 3})
        streamReplocator.insert(dolphindb::Util::createInt(i));
    {
        dolphindb::TableSP res_1 = conn_1.run(table_name);
        dolphindb::TableSP res_2 = conn_2.run(table_name);
        ASSERT_EQ(res_1->size(),0);
        ASSERT_EQ(res_2->size(),0);
        ASSERT_EQ(res_1->getString(), res_2->getString());
    }
    streamReplocator.insert(dolphindb::Util::createInt(4));
    unsigned times = 0;
    while (streamReplocator.getStatus().streams["conn_1"].insertedRows < 5 || streamReplocator.getStatus().streams["conn_2"].insertedRows < 5){
        if (times >= 10)
            ASSERT_TRUE(false);
        std::cout << streamReplocator.getStatus().streams["conn_1"].errorMsg << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        ++times;
    }
    {
        dolphindb::TableSP res_1 = conn_1.run(table_name);
        dolphindb::TableSP res_2 = conn_2.run(table_name);
        ASSERT_EQ(res_1->size(),5);
        ASSERT_EQ(res_2->size(),5);
        ASSERT_EQ(res_1->getString(), res_2->getString());
    }
}

TEST_F(StreamReplicatorTest, insert_data_batch_interval_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share table(array(INT) as data) as "+table_name);
    conn_2.run("share table(array(INT) as data) as "+table_name);
    dolphindb::ReplicatorConfig config;
    config.setBatching(1000, std::chrono::seconds{5});
    dolphindb::StreamReplicator streamReplocator{{{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "conn_1"}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, "conn_2"}}, table_name, config};
    for (auto i : std::vector<int>{0, 1, 2, 3, 4})
        streamReplocator.insert(dolphindb::Util::createInt(i));
    {
        dolphindb::TableSP res_1 = conn_1.run(table_name);
        dolphindb::TableSP res_2 = conn_2.run(table_name);
        ASSERT_EQ(res_1->size(),0);
        ASSERT_EQ(res_2->size(),0);
        ASSERT_EQ(res_1->getString(), res_2->getString());
    }
    std::this_thread::sleep_for(std::chrono::seconds{8});
    {
        dolphindb::TableSP res_1 = conn_1.run(table_name);
        dolphindb::TableSP res_2 = conn_2.run(table_name);
        ASSERT_EQ(res_1->size(),5);
        ASSERT_EQ(res_2->size(),5);
        ASSERT_EQ(res_1->getString(), res_2->getString());
    }
}

TEST_F(StreamReplicatorTest, insert_data_compress_less_error_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share table(100:0, "
        "`c_bool`c_char`c_short`c_int`c_long`c_date`c_month`c_datehour`c_minute`c_second`c_time`c_datetime`c_timestamp`c_nanotime`c_nanotimestamp`c_float`c_double`c_symbol`c_string`c_blob`c_decimal32`c_decimal64`c_decimal128"
        "`c_bool_av`c_char_av`c_short_av`c_int_av`c_long_av`c_date_av`c_month_av`c_datehour_av`c_minute_av`c_second_av`c_time_av`c_datetime_av`c_timestamp_av`c_nanotime_av`c_nanotimestamp_av`c_float_av`c_double_av`c_decimal32_av`c_decimal64_av`c_decimal128_av,"
        "[BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, DATEHOUR, MINUTE, SECOND, TIME, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, DECIMAL32(2), DECIMAL64(6), DECIMAL128(8), "
        "BOOL[], CHAR[], SHORT[], INT[], LONG[], DATE[], MONTH[], DATEHOUR[], MINUTE[], SECOND[], TIME[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], FLOAT[], DOUBLE[], DECIMAL32(2)[], DECIMAL64(6)[], DECIMAL128(8)[]]) as "+table_name);
    conn_2.run("share table(100:0, "
        "`c_bool`c_char`c_short`c_int`c_long`c_date`c_month`c_datehour`c_minute`c_second`c_time`c_datetime`c_timestamp`c_nanotime`c_nanotimestamp`c_float`c_double`c_symbol`c_string`c_blob`c_decimal32`c_decimal64`c_decimal128"
        "`c_bool_av`c_char_av`c_short_av`c_int_av`c_long_av`c_date_av`c_month_av`c_datehour_av`c_minute_av`c_second_av`c_time_av`c_datetime_av`c_timestamp_av`c_nanotime_av`c_nanotimestamp_av`c_float_av`c_double_av`c_decimal32_av`c_decimal64_av`c_decimal128_av,"
        "[BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, DATEHOUR, MINUTE, SECOND, TIME, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, DECIMAL32(2), DECIMAL64(6), DECIMAL128(8), "
        "BOOL[], CHAR[], SHORT[], INT[], LONG[], DATE[], MONTH[], DATEHOUR[], MINUTE[], SECOND[], TIME[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], FLOAT[], DOUBLE[], DECIMAL32(2)[], DECIMAL64(6)[], DECIMAL128(8)[]]) as "+table_name);
    dolphindb::ReplicatorConfig config;
    config.setCompression({dolphindb::COMPRESS_LZ4});
    ASSERT_ANY_THROW(dolphindb::StreamReplicator streamReplocator({{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "conn_1"}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, "conn_2"}}, table_name, config));
}

TEST_F(StreamReplicatorTest, insert_data_compress_greater_error_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share table(100:0, "
        "`c_bool`c_char`c_short`c_int`c_long`c_date`c_month`c_datehour`c_minute`c_second`c_time`c_datetime`c_timestamp`c_nanotime`c_nanotimestamp`c_float`c_double`c_symbol`c_string`c_blob`c_decimal32`c_decimal64`c_decimal128"
        "`c_bool_av`c_char_av`c_short_av`c_int_av`c_long_av`c_date_av`c_month_av`c_datehour_av`c_minute_av`c_second_av`c_time_av`c_datetime_av`c_timestamp_av`c_nanotime_av`c_nanotimestamp_av`c_float_av`c_double_av`c_decimal32_av`c_decimal64_av`c_decimal128_av,"
        "[BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, DATEHOUR, MINUTE, SECOND, TIME, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, DECIMAL32(2), DECIMAL64(6), DECIMAL128(8), "
        "BOOL[], CHAR[], SHORT[], INT[], LONG[], DATE[], MONTH[], DATEHOUR[], MINUTE[], SECOND[], TIME[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], FLOAT[], DOUBLE[], DECIMAL32(2)[], DECIMAL64(6)[], DECIMAL128(8)[]]) as "+table_name);
    conn_2.run("share table(100:0, "
        "`c_bool`c_char`c_short`c_int`c_long`c_date`c_month`c_datehour`c_minute`c_second`c_time`c_datetime`c_timestamp`c_nanotime`c_nanotimestamp`c_float`c_double`c_symbol`c_string`c_blob`c_decimal32`c_decimal64`c_decimal128"
        "`c_bool_av`c_char_av`c_short_av`c_int_av`c_long_av`c_date_av`c_month_av`c_datehour_av`c_minute_av`c_second_av`c_time_av`c_datetime_av`c_timestamp_av`c_nanotime_av`c_nanotimestamp_av`c_float_av`c_double_av`c_decimal32_av`c_decimal64_av`c_decimal128_av,"
        "[BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, DATEHOUR, MINUTE, SECOND, TIME, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, DECIMAL32(2), DECIMAL64(6), DECIMAL128(8), "
        "BOOL[], CHAR[], SHORT[], INT[], LONG[], DATE[], MONTH[], DATEHOUR[], MINUTE[], SECOND[], TIME[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], FLOAT[], DOUBLE[], DECIMAL32(2)[], DECIMAL64(6)[], DECIMAL128(8)[]]) as "+table_name);
    dolphindb::ReplicatorConfig config;
    config.setCompression({44,dolphindb::COMPRESS_LZ4});
    ASSERT_ANY_THROW(dolphindb::StreamReplicator streamReplocator({{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "conn_1"}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, "conn_2"}}, table_name, config));
}

TEST_F(StreamReplicatorTest, insert_data_compress_not_support_delta_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share table(100:0, [`c_bool], [BOOL]) as "+table_name);
    conn_2.run("share table(100:0, [`c_bool], [BOOL]) as "+table_name);
    dolphindb::ReplicatorConfig config;
    config.setCompression({dolphindb::COMPRESS_DELTA});
    ASSERT_ANY_THROW(dolphindb::StreamReplicator streamReplocator({{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "conn_1"}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, "conn_2"}}, table_name, config));
}

// AC-669
TEST_F(StreamReplicatorTest, insert_data_compress_none_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share table(100:0, "
        "`c_bool`c_char`c_short`c_int`c_long`c_date`c_month`c_datehour`c_minute`c_second`c_time`c_datetime`c_timestamp`c_nanotime`c_nanotimestamp`c_float`c_double`c_symbol`c_string`c_blob`c_decimal32`c_decimal64`c_decimal128"
        "`c_bool_av`c_char_av`c_short_av`c_int_av`c_long_av`c_date_av`c_month_av`c_datehour_av`c_minute_av`c_second_av`c_time_av`c_datetime_av`c_timestamp_av`c_nanotime_av`c_nanotimestamp_av`c_float_av`c_double_av`c_decimal32_av`c_decimal64_av`c_decimal128_av,"
        "[BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, DATEHOUR, MINUTE, SECOND, TIME, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, DECIMAL32(2), DECIMAL64(6), DECIMAL128(8), "
        "BOOL[], CHAR[], SHORT[], INT[], LONG[], DATE[], MONTH[], DATEHOUR[], MINUTE[], SECOND[], TIME[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], FLOAT[], DOUBLE[], DECIMAL32(2)[], DECIMAL64(6)[], DECIMAL128(8)[]]) as "+table_name);
    conn_2.run("share table(100:0, "
        "`c_bool`c_char`c_short`c_int`c_long`c_date`c_month`c_datehour`c_minute`c_second`c_time`c_datetime`c_timestamp`c_nanotime`c_nanotimestamp`c_float`c_double`c_symbol`c_string`c_blob`c_decimal32`c_decimal64`c_decimal128"
        "`c_bool_av`c_char_av`c_short_av`c_int_av`c_long_av`c_date_av`c_month_av`c_datehour_av`c_minute_av`c_second_av`c_time_av`c_datetime_av`c_timestamp_av`c_nanotime_av`c_nanotimestamp_av`c_float_av`c_double_av`c_decimal32_av`c_decimal64_av`c_decimal128_av,"
        "[BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, DATEHOUR, MINUTE, SECOND, TIME, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, DECIMAL32(2), DECIMAL64(6), DECIMAL128(8), "
        "BOOL[], CHAR[], SHORT[], INT[], LONG[], DATE[], MONTH[], DATEHOUR[], MINUTE[], SECOND[], TIME[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], FLOAT[], DOUBLE[], DECIMAL32(2)[], DECIMAL64(6)[], DECIMAL128(8)[]]) as "+table_name);
    dolphindb::ReplicatorConfig config;
    config.setCompression({43, dolphindb::COMPRESS_NONE});
    ASSERT_ANY_THROW(dolphindb::StreamReplicator streamReplocator({{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "conn_1"}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, "conn_2"}}, table_name, config));
}

TEST_F(StreamReplicatorTest, insert_data_compress_lz4_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share table(100:0, "
        "`c_bool`c_char`c_short`c_int`c_long`c_date`c_month`c_datehour`c_minute`c_second`c_time`c_datetime`c_timestamp`c_nanotime`c_nanotimestamp`c_float`c_double`c_symbol`c_string`c_blob`c_int128`c_uuid`c_ip`c_decimal32`c_decimal64`c_decimal128"
        "`c_bool_av`c_char_av`c_short_av`c_int_av`c_long_av`c_date_av`c_month_av`c_datehour_av`c_minute_av`c_second_av`c_time_av`c_datetime_av`c_timestamp_av`c_nanotime_av`c_nanotimestamp_av`c_float_av`c_double_av`c_int128_av`c_uuid_av`c_ip_av`c_decimal32_av`c_decimal64_av`c_decimal128_av,"
        "[BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, DATEHOUR, MINUTE, SECOND, TIME, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, INT128, UUID, IPADDR, DECIMAL32(2), DECIMAL64(6), DECIMAL128(8), "
        "BOOL[], CHAR[], SHORT[], INT[], LONG[], DATE[], MONTH[], DATEHOUR[], MINUTE[], SECOND[], TIME[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], FLOAT[], DOUBLE[], INT128[], UUID[], IPADDR[], DECIMAL32(2)[], DECIMAL64(6)[], DECIMAL128(8)[]]) as "+table_name);
    conn_2.run("share table(100:0, "
        "`c_bool`c_char`c_short`c_int`c_long`c_date`c_month`c_datehour`c_minute`c_second`c_time`c_datetime`c_timestamp`c_nanotime`c_nanotimestamp`c_float`c_double`c_symbol`c_string`c_blob`c_int128`c_uuid`c_ip`c_decimal32`c_decimal64`c_decimal128"
        "`c_bool_av`c_char_av`c_short_av`c_int_av`c_long_av`c_date_av`c_month_av`c_datehour_av`c_minute_av`c_second_av`c_time_av`c_datetime_av`c_timestamp_av`c_nanotime_av`c_nanotimestamp_av`c_float_av`c_double_av`c_int128_av`c_uuid_av`c_ip_av`c_decimal32_av`c_decimal64_av`c_decimal128_av,"
        "[BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, DATEHOUR, MINUTE, SECOND, TIME, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, INT128, UUID, IPADDR, DECIMAL32(2), DECIMAL64(6), DECIMAL128(8), "
        "BOOL[], CHAR[], SHORT[], INT[], LONG[], DATE[], MONTH[], DATEHOUR[], MINUTE[], SECOND[], TIME[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], FLOAT[], DOUBLE[], INT128[], UUID[], IPADDR[], DECIMAL32(2)[], DECIMAL64(6)[], DECIMAL128(8)[]]) as "+table_name);
    {   
        dolphindb::ReplicatorConfig config;
        config.setCompression({49, dolphindb::COMPRESS_LZ4});
        dolphindb::StreamReplicator streamReplocator({{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "conn_1"}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, "conn_2"}}, table_name, config);
        // BOOL[]
        dolphindb::VectorSP bool_1 = dolphindb::Util::createVector(dolphindb::DT_BOOL,0,3);
        dolphindb::ConstantSP bool_true = dolphindb::Util::createBool(true);
        dolphindb::ConstantSP bool_false = dolphindb::Util::createBool(false);
        dolphindb::ConstantSP bool_null = dolphindb::Util::createNullConstant(dolphindb::DT_BOOL);
        bool_1->append(bool_true);
        bool_1->append(bool_false);
        bool_1->append(bool_null);
        // CHAR[]
        dolphindb::VectorSP char_1 = dolphindb::Util::createVector(dolphindb::DT_CHAR,0,3);
        dolphindb::ConstantSP char_max = dolphindb::Util::createChar(127);
        dolphindb::ConstantSP char_min = dolphindb::Util::createChar(-127);
        dolphindb::ConstantSP char_0 = dolphindb::Util::createChar(0);
        dolphindb::ConstantSP char_null = dolphindb::Util::createNullConstant(dolphindb::DT_CHAR);
        char_1->append(char_max);
        char_1->append(char_min);
        char_1->append(char_0);
        char_1->append(char_null);
        // SHORT[]
        dolphindb::VectorSP short_1 = dolphindb::Util::createVector(dolphindb::DT_SHORT,0,3);
        dolphindb::ConstantSP short_max = dolphindb::Util::createShort(32767);
        dolphindb::ConstantSP short_min = dolphindb::Util::createShort(-32767);
        dolphindb::ConstantSP short_0 = dolphindb::Util::createShort(0);
        dolphindb::ConstantSP short_null = dolphindb::Util::createNullConstant(dolphindb::DT_SHORT);
        short_1->append(short_max);
        short_1->append(short_min);
        short_1->append(short_0);
        short_1->append(short_null);
        // INT[]
        dolphindb::VectorSP int_1 = dolphindb::Util::createVector(dolphindb::DT_INT,0,3);
        dolphindb::ConstantSP int_max = dolphindb::Util::createInt(2147483647);
        dolphindb::ConstantSP int_min = dolphindb::Util::createInt(-2147483647);
        dolphindb::ConstantSP int_0 = dolphindb::Util::createInt(0);
        dolphindb::ConstantSP int_null = dolphindb::Util::createNullConstant(dolphindb::DT_INT);
        int_1->append(int_max);
        int_1->append(int_min);
        int_1->append(int_0);
        int_1->append(int_null);
        // LONG[]
        dolphindb::VectorSP long_1 = dolphindb::Util::createVector(dolphindb::DT_LONG,0,3);
        dolphindb::ConstantSP long_max = dolphindb::Util::createLong(9223372036854775807);
        dolphindb::ConstantSP long_min = dolphindb::Util::createLong(-9223372036854775807);
        dolphindb::ConstantSP long_0 = dolphindb::Util::createLong(0);
        dolphindb::ConstantSP long_null = dolphindb::Util::createNullConstant(dolphindb::DT_LONG);
        long_1->append(long_max);
        long_1->append(long_min);
        long_1->append(long_0);
        long_1->append(long_null);
        // DATE[]
        dolphindb::VectorSP date_1 = dolphindb::Util::createVector(dolphindb::DT_DATE,0,2);
        dolphindb::ConstantSP date_0 = dolphindb::Util::createDate(0);
        dolphindb::ConstantSP date_ = dolphindb::Util::createDate(19132);
        dolphindb::ConstantSP date_null = dolphindb::Util::createNullConstant(dolphindb::DT_DATE);
        date_1->append(date_0);
        date_1->append(date_);
        date_1->append(date_null);
        // MONTH[]
        dolphindb::VectorSP month_1 = dolphindb::Util::createVector(dolphindb::DT_MONTH,0,2);
        dolphindb::ConstantSP month_0 = dolphindb::Util::createMonth(0);
        dolphindb::ConstantSP month_ = dolphindb::Util::createMonth(23640);
        dolphindb::ConstantSP month_null = dolphindb::Util::createNullConstant(dolphindb::DT_MONTH);
        month_1->append(month_0);
        month_1->append(month_);
        month_1->append(month_null);
        // DATEHOUR[]
        dolphindb::VectorSP datehour_1 = dolphindb::Util::createVector(dolphindb::DT_DATEHOUR,0,2);
        dolphindb::ConstantSP datehour_0 = dolphindb::Util::createDateHour(0);
        dolphindb::ConstantSP datehour_ = dolphindb::Util::createDateHour(1);
        dolphindb::ConstantSP datehour_null = dolphindb::Util::createNullConstant(dolphindb::DT_DATEHOUR);
        datehour_1->append(datehour_0);
        datehour_1->append(datehour_);
        datehour_1->append(datehour_null);
        // MINUTE[]
        dolphindb::VectorSP minute_1 = dolphindb::Util::createVector(dolphindb::DT_MINUTE,0,2);
        dolphindb::ConstantSP minute_0 = dolphindb::Util::createMinute(0);
        dolphindb::ConstantSP minute_ = dolphindb::Util::createMinute(1);
        dolphindb::ConstantSP minute_null = dolphindb::Util::createNullConstant(dolphindb::DT_MINUTE);
        minute_1->append(minute_0);
        minute_1->append(minute_);
        minute_1->append(minute_null);
        // SECOND[]
        dolphindb::VectorSP second_1 = dolphindb::Util::createVector(dolphindb::DT_SECOND,0,2);
        dolphindb::ConstantSP second_0 = dolphindb::Util::createSecond(0);
        dolphindb::ConstantSP second_ = dolphindb::Util::createSecond(1);
        dolphindb::ConstantSP second_null = dolphindb::Util::createNullConstant(dolphindb::DT_SECOND);
        second_1->append(second_0);
        second_1->append(second_);
        second_1->append(second_null);
        // TIME[]
        dolphindb::VectorSP time_1 = dolphindb::Util::createVector(dolphindb::DT_TIME,0,2);
        dolphindb::VectorSP time_2 = dolphindb::Util::createVector(dolphindb::DT_TIME,0,1);
        dolphindb::ConstantSP time_0 = dolphindb::Util::createTime(0);
        dolphindb::ConstantSP time_ = dolphindb::Util::createTime(1);
        dolphindb::ConstantSP time_null = dolphindb::Util::createNullConstant(dolphindb::DT_TIME);
        time_1->append(time_0);
        time_1->append(time_);
        time_1->append(time_null);
        time_2->append(time_null);
        // DATETIME[]
        dolphindb::VectorSP datetime_1 = dolphindb::Util::createVector(dolphindb::DT_DATETIME,0,2);
        dolphindb::ConstantSP datetime_0 = dolphindb::Util::createDateTime(0);
        dolphindb::ConstantSP datetime_ = dolphindb::Util::createDateTime(1);
        dolphindb::ConstantSP datetime_null = dolphindb::Util::createNullConstant(dolphindb::DT_DATETIME);
        datetime_1->append(datetime_0);
        datetime_1->append(datetime_);
        datetime_1->append(datetime_null);
        // TIMESTAMP[]
        dolphindb::VectorSP timestamp_1 = dolphindb::Util::createVector(dolphindb::DT_TIMESTAMP,0,2);
        dolphindb::ConstantSP timestamp_0 = dolphindb::Util::createTimestamp(0);
        dolphindb::ConstantSP timestamp_ = dolphindb::Util::createTimestamp(1);
        dolphindb::ConstantSP timestamp_null = dolphindb::Util::createNullConstant(dolphindb::DT_TIMESTAMP);
        timestamp_1->append(timestamp_0);
        timestamp_1->append(timestamp_);
        timestamp_1->append(timestamp_null);
        // NANOTIME[]
        dolphindb::VectorSP nanotime_1 = dolphindb::Util::createVector(dolphindb::DT_NANOTIME,0,2);
        dolphindb::ConstantSP nanotime_0 = dolphindb::Util::createNanoTime(0);
        dolphindb::ConstantSP nanotime_ = dolphindb::Util::createNanoTime(1);
        dolphindb::ConstantSP nanotime_null = dolphindb::Util::createNullConstant(dolphindb::DT_NANOTIME);
        nanotime_1->append(nanotime_0);
        nanotime_1->append(nanotime_);
        nanotime_1->append(nanotime_null);
        // NANOTIMESTAMP[]
        dolphindb::VectorSP nanotimestamp_1 = dolphindb::Util::createVector(dolphindb::DT_NANOTIMESTAMP,0,2);
        dolphindb::ConstantSP nanotimestamp_0 = dolphindb::Util::createNanoTimestamp(0);
        dolphindb::ConstantSP nanotimestamp_ = dolphindb::Util::createNanoTimestamp(1);
        dolphindb::ConstantSP nanotimestamp_null = dolphindb::Util::createNullConstant(dolphindb::DT_NANOTIMESTAMP);
        nanotimestamp_1->append(nanotimestamp_0);
        nanotimestamp_1->append(nanotimestamp_);
        nanotimestamp_1->append(nanotimestamp_null);
        // FLOAT[]
        dolphindb::VectorSP float_1 = dolphindb::Util::createVector(dolphindb::DT_FLOAT,0,2);
        dolphindb::ConstantSP float_ = dolphindb::Util::createFloat(3.14);
        dolphindb::ConstantSP float_nan = dolphindb::Util::createFloat(NAN);
        dolphindb::ConstantSP float_inf = dolphindb::Util::createFloat(INFINITY);
        dolphindb::ConstantSP float_null = dolphindb::Util::createNullConstant(dolphindb::DT_FLOAT);
        float_1->append(float_);
        float_1->append(float_nan);
        float_1->append(float_inf);
        float_1->append(float_null);
        // DOUBLE[]
        dolphindb::VectorSP double_1 = dolphindb::Util::createVector(dolphindb::DT_DOUBLE,0,2);
        dolphindb::ConstantSP double_ = dolphindb::Util::createDouble(3.14);
        dolphindb::ConstantSP double_nan = dolphindb::Util::createDouble(NAN);
        dolphindb::ConstantSP double_inf = dolphindb::Util::createDouble(INFINITY);
        dolphindb::ConstantSP double_null = dolphindb::Util::createNullConstant(dolphindb::DT_DOUBLE);
        double_1->append(double_);
        double_1->append(double_nan);
        double_1->append(double_inf);
        double_1->append(double_null);
        // INT128[]
        dolphindb::VectorSP int128_1 = dolphindb::Util::createVector(dolphindb::DT_INT128,0,2);
        dolphindb::ConstantSP int128_normal = dolphindb::Util::parseConstant(dolphindb::DT_INT128,"e1671797c52e15f763380b45e841ec32");
        int128_1->append(int128_normal);
        int128_1->append(int128_normal);
        int128_1->append(int128_normal);
        // UUID[]
        dolphindb::VectorSP uuid_1 = dolphindb::Util::createVector(dolphindb::DT_UUID,0,2);
        dolphindb::ConstantSP uuid_normal = dolphindb::Util::parseConstant(dolphindb::DT_UUID,"5d212a78-cc48-e3b1-4235-b4d91473ee87");
        uuid_1->append(uuid_normal);
        uuid_1->append(uuid_normal);
        uuid_1->append(uuid_normal);
        // IPADDR[]
        dolphindb::VectorSP ip_1 = dolphindb::Util::createVector(dolphindb::DT_IP,0,2);
        dolphindb::ConstantSP ip_normal = dolphindb::Util::parseConstant(dolphindb::DT_IP,"127.0.0.1");
        ip_1->append(ip_normal);
        ip_1->append(ip_normal);
        ip_1->append(ip_normal);
        // DECIMAL32(2)[]
        dolphindb::VectorSP decimal32_1 = dolphindb::Util::createVector(dolphindb::DT_DECIMAL32,0,2,true,2);
        dolphindb::ConstantSP decimal32_314 = dolphindb::Util::createDecimal32(2, 3.14);
        dolphindb::ConstantSP decimal32_315 = dolphindb::Util::createDecimal32(2, 3.15);
        dolphindb::ConstantSP decimal32_null = dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL32, 2);
        decimal32_1->append(decimal32_314);
        decimal32_1->append(decimal32_315);
        decimal32_1->append(decimal32_null);
        // DECIMAL64(6)[]
        dolphindb::VectorSP decimal64_1 = dolphindb::Util::createVector(dolphindb::DT_DECIMAL64,0,2,true,6);
        dolphindb::ConstantSP decimal64_3141592 = dolphindb::Util::createDecimal64(6, 3.141592);
        dolphindb::ConstantSP decimal64_3141593 = dolphindb::Util::createDecimal64(6, 3.141593);
        dolphindb::ConstantSP decimal64_null = dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL64, 6);
        decimal64_1->append(decimal64_3141592);
        decimal64_1->append(decimal64_3141593);
        decimal64_1->append(decimal64_null);
        // DECIMAL128(8)[]
        dolphindb::VectorSP decimal128_1 = dolphindb::Util::createVector(dolphindb::DT_DECIMAL128,0,2,true,8);
        dolphindb::ConstantSP decimal128_314159265 = dolphindb::Util::createDecimal32(8, 3.14159265);
        dolphindb::ConstantSP decimal128_314159266 = dolphindb::Util::createDecimal32(8, 3.14159266);
        dolphindb::ConstantSP decimal128_null = dolphindb::Util::createNullConstant(dolphindb::DT_DECIMAL128, 8);
        decimal128_1->append(decimal128_314159265);
        decimal128_1->append(decimal128_314159266);
        decimal128_1->append(decimal128_null);
        streamReplocator.insert(
            dolphindb::Util::createBool(true), dolphindb::Util::createChar(127), dolphindb::Util::createShort(32767), dolphindb::Util::createInt(2147483647), dolphindb::Util::createLong(9223372036854775807), dolphindb::Util::createDate(0), dolphindb::Util::createMonth(23640), dolphindb::Util::createDateHour(0), 
            dolphindb::Util::createMinute(0), dolphindb::Util::createSecond(0), dolphindb::Util::createTime(0), dolphindb::Util::createDateTime(0), dolphindb::Util::createTimestamp(0), dolphindb::Util::createNanoTime(0), dolphindb::Util::createNanoTimestamp(0), dolphindb::Util::createFloat(3.14), dolphindb::Util::createDouble(3.14), 
            dolphindb::Util::createString("abc!@#中文 123"), dolphindb::Util::createString("abc!@#中文 123"), dolphindb::Util::createBlob("abc!@#中文 123"), int128_normal, uuid_normal, ip_normal, dolphindb::Util::createDecimal32(2, 3.14), dolphindb::Util::createDecimal64(6, 3.141592), dolphindb::Util::createDecimal128(8, 3.14159265), bool_1, 
            char_1, short_1, int_1, long_1, date_1, month_1, datehour_1, minute_1, second_1, time_1, datetime_1, timestamp_1, nanotime_1, nanotimestamp_1, float_1, double_1, int128_1, uuid_1, ip_1, decimal32_1, decimal64_1, decimal128_1
        );
    }
    dolphindb::TableSP res_1 = conn_1.run(table_name);
    dolphindb::TableSP res_2 = conn_2.run(table_name);
    ASSERT_EQ(res_1->size(),1);
    ASSERT_EQ(res_2->size(),1);
    ASSERT_EQ(res_1->getString(), res_2->getString());
    std::vector<std::string> expect={
        "1", "127", "32767", "2147483647", "9223372036854775807", "1970.01.01", "1970.01M", "1970.01.01T00", "00:00m", "00:00:00", "00:00:00.000", "1970.01.01T00:00:00", "1970.01.01T00:00:00.000", "00:00:00.000000000", "1970.01.01T00:00:00.000000000", 
        "3.14", "3.14", "abc!@#中文 123", "abc!@#中文 123", "abc!@#中文 123", "e1671797c52e15f763380b45e841ec32", "5d212a78-cc48-e3b1-4235-b4d91473ee87", "127.0.0.1", "3.14", "3.141592", "3.14159265", "[1,0,]", "[127,-127,0,]", "[32767,-32767,0,]", "[2147483647,-2147483647,0,]", "[9223372036854775807,-9223372036854775807,0,]", "[1970.01.01,2022.05.20,]", 
        "[0000.01M,1970.01M,]", "[1970.01.01T00,1970.01.01T01,]", "[00:00m,00:01m,]", "[00:00:00,00:00:01,]", "[00:00:00.000,00:00:00.001,]", "[1970.01.01T00:00:00,1970.01.01T00:00:01,]", "[1970.01.01T00:00:00.000,1970.01.01T00:00:00.001,]", "[00:00:00.000000000,00:00:00.000000001,]", 
        "[1970.01.01T00:00:00.000000000,1970.01.01T00:00:00.000000001,]", "[3.14,NaN,inf,]", "[3.14,NaN,inf,]", "[e1671797c52e15f763380b45e841ec32,e1671797c52e15f763380b45e841ec32,e1671797c52e15f763380b45e841ec32]", "[5d212a78-cc48-e3b1-4235-b4d91473ee87,5d212a78-cc48-e3b1-4235-b4d91473ee87,5d212a78-cc48-e3b1-4235-b4d91473ee87]", "[127.0.0.1,127.0.0.1,127.0.0.1]", "[3.14,3.15,]", "[3.141592,3.141593,]", "[3.14159265,3.14159266,]"
    };
    for (int i=0;i<49;++i)
        ASSERT_EQ(res_1->getColumn(i)->getRow(0)->getString(), expect[i]);
}

TEST_F(StreamReplicatorTest, insert_data_compress_delta_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share table(100:0, "
        "`c_short`c_int`c_long`c_date`c_month`c_datehour`c_minute`c_second`c_time`c_datetime`c_timestamp`c_nanotime`c_nanotimestamp`c_decimal32`c_decimal64,"
        "[SHORT, INT, LONG, DATE, MONTH, DATEHOUR, MINUTE, SECOND, TIME, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, DECIMAL32(2), DECIMAL64(6)])"
        " as "+table_name);
    conn_2.run("share table(100:0, "
        "`c_short`c_int`c_long`c_date`c_month`c_datehour`c_minute`c_second`c_time`c_datetime`c_timestamp`c_nanotime`c_nanotimestamp`c_decimal32`c_decimal64,"
        "[SHORT, INT, LONG, DATE, MONTH, DATEHOUR, MINUTE, SECOND, TIME, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, DECIMAL32(2), DECIMAL64(6)])"
        " as "+table_name);
    {   
        dolphindb::ReplicatorConfig config;
        config.setCompression({15, dolphindb::COMPRESS_DELTA});
        dolphindb::StreamReplicator streamReplocator({{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "conn_1"}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, "conn_2"}}, table_name, config);
        streamReplocator.insert(
            dolphindb::Util::createShort(32767), dolphindb::Util::createInt(2147483647), dolphindb::Util::createLong(9223372036854775807), dolphindb::Util::createDate(0), dolphindb::Util::createMonth(23640), dolphindb::Util::createDateHour(0), 
            dolphindb::Util::createMinute(0), dolphindb::Util::createSecond(0), dolphindb::Util::createTime(0), dolphindb::Util::createDateTime(0), dolphindb::Util::createTimestamp(0), dolphindb::Util::createNanoTime(0), dolphindb::Util::createNanoTimestamp(0),
            dolphindb::Util::createDecimal32(2, 3.14), dolphindb::Util::createDecimal64(6, 3.141592)
        );
    }
    dolphindb::TableSP res_1 = conn_1.run(table_name);
    dolphindb::TableSP res_2 = conn_2.run(table_name);
    ASSERT_EQ(res_1->size(),1);
    ASSERT_EQ(res_2->size(),1);
    ASSERT_EQ(res_1->getString(), res_2->getString());
    std::vector<std::string> expect={
        "32767", "2147483647", "9223372036854775807", "1970.01.01", "1970.01M", "1970.01.01T00", "00:00m", "00:00:00", "00:00:00.000", "1970.01.01T00:00:00", "1970.01.01T00:00:00.000", "00:00:00.000000000", "1970.01.01T00:00:00.000000000", "3.14", "3.141592"
    };
    for (int i=0;i<15;++i)
        ASSERT_EQ(res_1->getColumn(i)->getRow(0)->getString(), expect[i]);
}

TEST_F(StreamReplicatorTest, insert_data_fail_1_retry_with_label_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share table(array(INT) as data) as "+table_name);
    conn_2.run("share table(array(INT) as data) as "+table_name);
    dolphindb::ReplicatorConfig config;
    config.setRetry(3, std::chrono::seconds{1});
    dolphindb::StreamReplicator streamReplocator{{{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "conn_1"}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, "conn_2"}}, table_name, config};
    conn_1.run("undef `"+table_name+",SHARED");
    streamReplocator.insert(dolphindb::Util::createInt(0));
    std::this_thread::sleep_for(std::chrono::seconds{1});
    {
        dolphindb::TableSP res_2 = conn_2.run(table_name);
        ASSERT_EQ(res_2->size(),1);
        ASSERT_EQ(res_2->getColumn(0)->getRow(0)->getString(), "0");
    }
    std::this_thread::sleep_for(std::chrono::seconds{5});
    {
        dolphindb::ReplicatorStatus status = streamReplocator.getStatus();
        ASSERT_EQ(status.totalRows, 1);
        dolphindb::ReplicatorStreamStatus status_1 = status.streams["conn_1"];
        dolphindb::ReplicatorStreamStatus status_2 = status.streams["conn_2"];
        ASSERT_EQ(status_1.insertedRows, 0);
        ASSERT_EQ(status_2.insertedRows, 1);
        ASSERT_EQ(status_1.dumpedRows, 1);
        ASSERT_EQ(status_2.dumpedRows, 0);
        ASSERT_EQ(status_1.maxRetry, 0);
        ASSERT_EQ(status_2.maxRetry, 3);
        ASSERT_EQ(status_1.connectionState_, dolphindb::ConnectionState::Reconnecting);
        ASSERT_EQ(status_2.connectionState_, dolphindb::ConnectionState::Connected);
        ASSERT_EQ(status_1.errorMsg, HOST_CLUSTER + ":" + std::to_string(PORT_DNODE1) + " Server response: 'Syntax Error: [line #1] Cannot recognize the token " + table_name + "' script: 'tableInsert{" + table_name + "}'");
        ASSERT_EQ(status_2.errorMsg, "");
    }
    conn_1.run("share table(array(INT) as data) as "+table_name);
    streamReplocator.insert(dolphindb::Util::createInt(1));
    std::this_thread::sleep_for(std::chrono::seconds{1});
    {
        dolphindb::TableSP res_1 = conn_1.run(table_name);
        ASSERT_EQ(res_1->size(),0);
        dolphindb::TableSP res_2 = conn_2.run(table_name);
        ASSERT_EQ(res_2->size(),2);
        ASSERT_EQ(res_2->getColumn(0)->getRow(0)->getString(), "0");
        ASSERT_EQ(res_2->getColumn(0)->getRow(1)->getString(), "1");
        dolphindb::ReplicatorStatus status = streamReplocator.getStatus();
        ASSERT_EQ(status.totalRows, 2);
        dolphindb::ReplicatorStreamStatus status_1 = status.streams["conn_1"];
        dolphindb::ReplicatorStreamStatus status_2 = status.streams["conn_2"];
        ASSERT_EQ(status_1.insertedRows, 0);
        ASSERT_EQ(status_2.insertedRows, 2);
        ASSERT_EQ(status_1.dumpedRows, 2);
        ASSERT_EQ(status_2.dumpedRows, 0);
        ASSERT_EQ(status_1.maxRetry, 0);
        ASSERT_EQ(status_2.maxRetry, 3);
        ASSERT_EQ(status_1.connectionState_, dolphindb::ConnectionState::Reconnecting);
        ASSERT_EQ(status_2.connectionState_, dolphindb::ConnectionState::Connected);
        ASSERT_NE(status_1.errorMsg, "");
        ASSERT_EQ(status_2.errorMsg, "");
    }
}

TEST_F(StreamReplicatorTest, insert_data_fail_1_retry_without_label_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share table(array(INT) as data) as "+table_name);
    conn_2.run("share table(array(INT) as data) as "+table_name);
    dolphindb::ReplicatorConfig config;
    config.setRetry(3, std::chrono::seconds{1});
    dolphindb::StreamReplicator streamReplocator{{{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER}}, table_name, config};
    conn_1.run("undef `"+table_name+",SHARED");
    streamReplocator.insert(dolphindb::Util::createInt(0));
    std::this_thread::sleep_for(std::chrono::seconds{1});
    {
        dolphindb::TableSP res_2 = conn_2.run(table_name);
        ASSERT_EQ(res_2->size(),1);
        ASSERT_EQ(res_2->getColumn(0)->getRow(0)->getString(), "0");
    }
    std::this_thread::sleep_for(std::chrono::seconds{5});
    {
        dolphindb::ReplicatorStatus status = streamReplocator.getStatus();
        ASSERT_EQ(status.totalRows, 1);
        dolphindb::ReplicatorStreamStatus status_1 = status.streams[HOST_CLUSTER+":"+std::to_string(PORT_DNODE1)];
        dolphindb::ReplicatorStreamStatus status_2 = status.streams[HOST_CLUSTER+":"+std::to_string(PORT_DNODE2)];
        ASSERT_EQ(status_1.insertedRows, 0);
        ASSERT_EQ(status_2.insertedRows, 1);
        ASSERT_EQ(status_1.dumpedRows, 1);
        ASSERT_EQ(status_2.dumpedRows, 0);
        ASSERT_EQ(status_1.maxRetry, 0);
        ASSERT_EQ(status_2.maxRetry, 3);
        ASSERT_EQ(status_1.connectionState_, dolphindb::ConnectionState::Reconnecting);
        ASSERT_EQ(status_2.connectionState_, dolphindb::ConnectionState::Connected);
        ASSERT_EQ(status_1.errorMsg, HOST_CLUSTER + ":" + std::to_string(PORT_DNODE1) + " Server response: 'Syntax Error: [line #1] Cannot recognize the token " + table_name + "' script: 'tableInsert{" + table_name + "}'");
        ASSERT_EQ(status_2.errorMsg, "");
    }
    conn_1.run("share table(array(INT) as data) as "+table_name);
    streamReplocator.insert(dolphindb::Util::createInt(1));
    std::this_thread::sleep_for(std::chrono::seconds{1});
    {
        dolphindb::TableSP res_1 = conn_1.run(table_name);
        ASSERT_EQ(res_1->size(),0);
        dolphindb::TableSP res_2 = conn_2.run(table_name);
        ASSERT_EQ(res_2->size(),2);
        ASSERT_EQ(res_2->getColumn(0)->getRow(0)->getString(), "0");
        ASSERT_EQ(res_2->getColumn(0)->getRow(1)->getString(), "1");
        dolphindb::ReplicatorStatus status = streamReplocator.getStatus();
        ASSERT_EQ(status.totalRows, 2);
        dolphindb::ReplicatorStreamStatus status_1 = status.streams[HOST_CLUSTER+":"+std::to_string(PORT_DNODE1)];
        dolphindb::ReplicatorStreamStatus status_2 = status.streams[HOST_CLUSTER+":"+std::to_string(PORT_DNODE2)];
        ASSERT_EQ(status_1.insertedRows, 0);
        ASSERT_EQ(status_2.insertedRows, 2);
        ASSERT_EQ(status_1.dumpedRows, 2);
        ASSERT_EQ(status_2.dumpedRows, 0);
        ASSERT_EQ(status_1.maxRetry, 0);
        ASSERT_EQ(status_2.maxRetry, 3);
        ASSERT_EQ(status_1.connectionState_, dolphindb::ConnectionState::Reconnecting);
        ASSERT_EQ(status_2.connectionState_, dolphindb::ConnectionState::Connected);
        ASSERT_NE(status_1.errorMsg, "");
        ASSERT_EQ(status_2.errorMsg, "");
    }
}

TEST_F(StreamReplicatorTest, insert_data_fail_1_retry_with_1_label_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share table(array(INT) as data) as "+table_name);
    conn_2.run("share table(array(INT) as data) as "+table_name);
    dolphindb::ReplicatorConfig config;
    config.setRetry(3, std::chrono::seconds{1});
    dolphindb::StreamReplicator streamReplocator{{{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "conn_1"}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER}}, table_name, config};
    conn_1.run("undef `"+table_name+",SHARED");
    streamReplocator.insert(dolphindb::Util::createInt(0));
    std::this_thread::sleep_for(std::chrono::seconds{1});
    {
        dolphindb::TableSP res_2 = conn_2.run(table_name);
        ASSERT_EQ(res_2->size(),1);
        ASSERT_EQ(res_2->getColumn(0)->getRow(0)->getString(), "0");
    }
    std::this_thread::sleep_for(std::chrono::seconds{5});
    {
        dolphindb::ReplicatorStatus status = streamReplocator.getStatus();
        ASSERT_EQ(status.totalRows, 1);
        dolphindb::ReplicatorStreamStatus status_1 = status.streams["conn_1"];
        dolphindb::ReplicatorStreamStatus status_2 = status.streams[HOST_CLUSTER+":"+std::to_string(PORT_DNODE2)];
        ASSERT_EQ(status_1.insertedRows, 0);
        ASSERT_EQ(status_2.insertedRows, 1);
        ASSERT_EQ(status_1.dumpedRows, 1);
        ASSERT_EQ(status_2.dumpedRows, 0);
        ASSERT_EQ(status_1.maxRetry, 0);
        ASSERT_EQ(status_2.maxRetry, 3);
        ASSERT_EQ(status_1.connectionState_, dolphindb::ConnectionState::Reconnecting);
        ASSERT_EQ(status_2.connectionState_, dolphindb::ConnectionState::Connected);
        ASSERT_EQ(status_1.errorMsg, HOST_CLUSTER + ":" + std::to_string(PORT_DNODE1) + " Server response: 'Syntax Error: [line #1] Cannot recognize the token " + table_name + "' script: 'tableInsert{" + table_name + "}'");
        ASSERT_EQ(status_2.errorMsg, "");
    }
    conn_1.run("share table(array(INT) as data) as "+table_name);
    streamReplocator.insert(dolphindb::Util::createInt(1));
    std::this_thread::sleep_for(std::chrono::seconds{1});
    {
        dolphindb::TableSP res_1 = conn_1.run(table_name);
        ASSERT_EQ(res_1->size(),0);
        dolphindb::TableSP res_2 = conn_2.run(table_name);
        ASSERT_EQ(res_2->size(),2);
        ASSERT_EQ(res_2->getColumn(0)->getRow(0)->getString(), "0");
        ASSERT_EQ(res_2->getColumn(0)->getRow(1)->getString(), "1");
        dolphindb::ReplicatorStatus status = streamReplocator.getStatus();
        ASSERT_EQ(status.totalRows, 2);
        dolphindb::ReplicatorStreamStatus status_1 = status.streams["conn_1"];
        dolphindb::ReplicatorStreamStatus status_2 = status.streams[HOST_CLUSTER+":"+std::to_string(PORT_DNODE2)];
        ASSERT_EQ(status_1.insertedRows, 0);
        ASSERT_EQ(status_2.insertedRows, 2);
        ASSERT_EQ(status_1.dumpedRows, 2);
        ASSERT_EQ(status_2.dumpedRows, 0);
        ASSERT_EQ(status_1.maxRetry, 0);
        ASSERT_EQ(status_2.maxRetry, 3);
        ASSERT_EQ(status_1.connectionState_, dolphindb::ConnectionState::Reconnecting);
        ASSERT_EQ(status_2.connectionState_, dolphindb::ConnectionState::Connected);
        ASSERT_NE(status_1.errorMsg, "");
        ASSERT_EQ(status_2.errorMsg, "");
    }
}

TEST_F(StreamReplicatorTest, insert_data_fail_all_retry_with_label_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share table(array(INT) as data) as "+table_name);
    conn_2.run("share table(array(INT) as data) as "+table_name);
    dolphindb::ReplicatorConfig config;
    config.setRetry(3, std::chrono::seconds{1});
    dolphindb::StreamReplicator streamReplocator{{{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "conn_1"}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, "conn_2"}}, table_name, config};
    conn_1.run("undef `"+table_name+",SHARED");
    conn_2.run("undef `"+table_name+",SHARED");
    streamReplocator.insert(dolphindb::Util::createInt(0));
    std::this_thread::sleep_for(std::chrono::seconds{10});
    {
        dolphindb::ReplicatorStatus status = streamReplocator.getStatus();
        ASSERT_EQ(status.totalRows, 1);
        dolphindb::ReplicatorStreamStatus status_1 = status.streams["conn_1"];
        dolphindb::ReplicatorStreamStatus status_2 = status.streams["conn_2"];
        ASSERT_EQ(status_1.insertedRows, 0);
        ASSERT_EQ(status_2.insertedRows, 0);
        ASSERT_EQ(status_1.dumpedRows, 1);
        ASSERT_EQ(status_2.dumpedRows, 1);
        ASSERT_EQ(status_1.maxRetry, 0);
        ASSERT_EQ(status_2.maxRetry, 0);
        ASSERT_EQ(status_1.connectionState_, dolphindb::ConnectionState::Reconnecting);
        ASSERT_EQ(status_2.connectionState_, dolphindb::ConnectionState::Reconnecting);
        ASSERT_EQ(status_1.errorMsg, HOST_CLUSTER + ":" + std::to_string(PORT_DNODE1) + " Server response: 'Syntax Error: [line #1] Cannot recognize the token " + table_name + "' script: 'tableInsert{" + table_name + "}'");
        ASSERT_EQ(status_2.errorMsg, HOST_CLUSTER + ":" + std::to_string(PORT_DNODE2) + " Server response: 'Syntax Error: [line #1] Cannot recognize the token " + table_name + "' script: 'tableInsert{" + table_name + "}'");
    }
    conn_1.run("share table(array(INT) as data) as "+table_name);
    conn_2.run("share table(array(INT) as data) as "+table_name);
    streamReplocator.insert(dolphindb::Util::createInt(1));
    std::this_thread::sleep_for(std::chrono::seconds{1});
    {
        dolphindb::TableSP res_1 = conn_1.run(table_name);
        ASSERT_EQ(res_1->size(),0);
        dolphindb::TableSP res_2 = conn_2.run(table_name);
        ASSERT_EQ(res_2->size(),0);
        dolphindb::ReplicatorStatus status = streamReplocator.getStatus();
        ASSERT_EQ(status.totalRows, 2);
        dolphindb::ReplicatorStreamStatus status_1 = status.streams["conn_1"];
        dolphindb::ReplicatorStreamStatus status_2 = status.streams["conn_2"];
        ASSERT_EQ(status_1.insertedRows, 0);
        ASSERT_EQ(status_2.insertedRows, 0);
        ASSERT_EQ(status_1.dumpedRows, 2);
        ASSERT_EQ(status_2.dumpedRows, 2);
        ASSERT_EQ(status_1.maxRetry, 0);
        ASSERT_EQ(status_2.maxRetry, 0);
        ASSERT_EQ(status_1.connectionState_, dolphindb::ConnectionState::Reconnecting);
        ASSERT_EQ(status_2.connectionState_, dolphindb::ConnectionState::Reconnecting);
        ASSERT_NE(status_1.errorMsg, "");
        ASSERT_NE(status_2.errorMsg, "");
    }
}

TEST_F(StreamReplicatorTest, insert_data_fail_all_retry_with_1_label_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share table(array(INT) as data) as "+table_name);
    conn_2.run("share table(array(INT) as data) as "+table_name);
    dolphindb::ReplicatorConfig config;
    config.setRetry(3, std::chrono::seconds{1});
    dolphindb::StreamReplicator streamReplocator{{{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "conn_1"}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER}}, table_name, config};
    conn_1.run("undef `"+table_name+",SHARED");
    conn_2.run("undef `"+table_name+",SHARED");
    streamReplocator.insert(dolphindb::Util::createInt(0));
    std::this_thread::sleep_for(std::chrono::seconds{10});
    {
        dolphindb::ReplicatorStatus status = streamReplocator.getStatus();
        ASSERT_EQ(status.totalRows, 1);
        dolphindb::ReplicatorStreamStatus status_1 = status.streams["conn_1"];
        dolphindb::ReplicatorStreamStatus status_2 = status.streams[HOST_CLUSTER+":"+std::to_string(PORT_DNODE2)];
        ASSERT_EQ(status_1.insertedRows, 0);
        ASSERT_EQ(status_2.insertedRows, 0);
        ASSERT_EQ(status_1.dumpedRows, 1);
        ASSERT_EQ(status_2.dumpedRows, 1);
        ASSERT_EQ(status_1.maxRetry, 0);
        ASSERT_EQ(status_2.maxRetry, 0);
        ASSERT_EQ(status_1.connectionState_, dolphindb::ConnectionState::Reconnecting);
        ASSERT_EQ(status_2.connectionState_, dolphindb::ConnectionState::Reconnecting);
        ASSERT_EQ(status_1.errorMsg, HOST_CLUSTER + ":" + std::to_string(PORT_DNODE1) + " Server response: 'Syntax Error: [line #1] Cannot recognize the token " + table_name + "' script: 'tableInsert{" + table_name + "}'");
        ASSERT_EQ(status_2.errorMsg, HOST_CLUSTER + ":" + std::to_string(PORT_DNODE2) + " Server response: 'Syntax Error: [line #1] Cannot recognize the token " + table_name + "' script: 'tableInsert{" + table_name + "}'");
    }
    conn_1.run("share table(array(INT) as data) as "+table_name);
    conn_2.run("share table(array(INT) as data) as "+table_name);
    streamReplocator.insert(dolphindb::Util::createInt(1));
    std::this_thread::sleep_for(std::chrono::seconds{1});
    {
        dolphindb::TableSP res_1 = conn_1.run(table_name);
        ASSERT_EQ(res_1->size(),0);
        dolphindb::TableSP res_2 = conn_2.run(table_name);
        ASSERT_EQ(res_2->size(),0);
        dolphindb::ReplicatorStatus status = streamReplocator.getStatus();
        ASSERT_EQ(status.totalRows, 2);
        dolphindb::ReplicatorStreamStatus status_1 = status.streams["conn_1"];
        dolphindb::ReplicatorStreamStatus status_2 = status.streams[HOST_CLUSTER+":"+std::to_string(PORT_DNODE2)];
        ASSERT_EQ(status_1.insertedRows, 0);
        ASSERT_EQ(status_2.insertedRows, 0);
        ASSERT_EQ(status_1.dumpedRows, 2);
        ASSERT_EQ(status_2.dumpedRows, 2);
        ASSERT_EQ(status_1.maxRetry, 0);
        ASSERT_EQ(status_2.maxRetry, 0);
        ASSERT_EQ(status_1.connectionState_, dolphindb::ConnectionState::Reconnecting);
        ASSERT_EQ(status_2.connectionState_, dolphindb::ConnectionState::Reconnecting);
        ASSERT_NE(status_1.errorMsg, "");
        ASSERT_NE(status_2.errorMsg, "");
    }
}

TEST_F(StreamReplicatorTest, insert_data_fail_all_retry_without_label_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share table(array(INT) as data) as "+table_name);
    conn_2.run("share table(array(INT) as data) as "+table_name);
    dolphindb::ReplicatorConfig config;
    config.setRetry(3, std::chrono::seconds{1});
    dolphindb::StreamReplicator streamReplocator{{{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER}}, table_name, config};
    conn_1.run("undef `"+table_name+",SHARED");
    conn_2.run("undef `"+table_name+",SHARED");
    streamReplocator.insert(dolphindb::Util::createInt(0));
    std::this_thread::sleep_for(std::chrono::seconds{10});
    {
        dolphindb::ReplicatorStatus status = streamReplocator.getStatus();
        ASSERT_EQ(status.totalRows, 1);
        dolphindb::ReplicatorStreamStatus status_1 = status.streams[HOST_CLUSTER+":"+std::to_string(PORT_DNODE1)];
        dolphindb::ReplicatorStreamStatus status_2 = status.streams[HOST_CLUSTER+":"+std::to_string(PORT_DNODE2)];
        ASSERT_EQ(status_1.insertedRows, 0);
        ASSERT_EQ(status_2.insertedRows, 0);
        ASSERT_EQ(status_1.dumpedRows, 1);
        ASSERT_EQ(status_2.dumpedRows, 1);
        ASSERT_EQ(status_1.maxRetry, 0);
        ASSERT_EQ(status_2.maxRetry, 0);
        ASSERT_EQ(status_1.connectionState_, dolphindb::ConnectionState::Reconnecting);
        ASSERT_EQ(status_2.connectionState_, dolphindb::ConnectionState::Reconnecting);
        ASSERT_EQ(status_1.errorMsg, HOST_CLUSTER + ":" + std::to_string(PORT_DNODE1) + " Server response: 'Syntax Error: [line #1] Cannot recognize the token " + table_name + "' script: 'tableInsert{" + table_name + "}'");
        ASSERT_EQ(status_2.errorMsg, HOST_CLUSTER + ":" + std::to_string(PORT_DNODE2) + " Server response: 'Syntax Error: [line #1] Cannot recognize the token " + table_name + "' script: 'tableInsert{" + table_name + "}'");
    }
    conn_1.run("share table(array(INT) as data) as "+table_name);
    conn_2.run("share table(array(INT) as data) as "+table_name);
    streamReplocator.insert(dolphindb::Util::createInt(1));
    std::this_thread::sleep_for(std::chrono::seconds{1});
    {
        dolphindb::TableSP res_1 = conn_1.run(table_name);
        ASSERT_EQ(res_1->size(),0);
        dolphindb::TableSP res_2 = conn_2.run(table_name);
        ASSERT_EQ(res_2->size(),0);
        dolphindb::ReplicatorStatus status = streamReplocator.getStatus();
        ASSERT_EQ(status.totalRows, 2);
        dolphindb::ReplicatorStreamStatus status_1 = status.streams[HOST_CLUSTER+":"+std::to_string(PORT_DNODE1)];
        dolphindb::ReplicatorStreamStatus status_2 = status.streams[HOST_CLUSTER+":"+std::to_string(PORT_DNODE2)];
        ASSERT_EQ(status_1.insertedRows, 0);
        ASSERT_EQ(status_2.insertedRows, 0);
        ASSERT_EQ(status_1.dumpedRows, 2);
        ASSERT_EQ(status_2.dumpedRows, 2);
        ASSERT_EQ(status_1.maxRetry, 0);
        ASSERT_EQ(status_2.maxRetry, 0);
        ASSERT_EQ(status_1.connectionState_, dolphindb::ConnectionState::Reconnecting);
        ASSERT_EQ(status_2.connectionState_, dolphindb::ConnectionState::Reconnecting);
        ASSERT_NE(status_1.errorMsg, "");
        ASSERT_NE(status_2.errorMsg, "");
    }
}

TEST_F(StreamReplicatorTest, insert_data_fail_1_on_data_dump_with_label_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share table(array(INT) as data) as "+table_name);
    conn_2.run("share table(array(INT) as data) as "+table_name);
    dolphindb::ReplicatorConfig config;
    config.setRetry(3, std::chrono::seconds{1});
    std::vector<std::string> host_label_list;
    std::vector<dolphindb::ConstantSP> table_list;
    config.onDataDump([&](const std::string &hostLabel, dolphindb::ConstantSP table) -> bool {
        host_label_list.emplace_back(hostLabel);
        table_list.emplace_back(table);
        return true;
    });
    dolphindb::StreamReplicator streamReplocator{{{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "conn_1"}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, "conn_2"}}, table_name, config};
    conn_1.run("undef `"+table_name+",SHARED");
    streamReplocator.insert(dolphindb::Util::createInt(0));
    std::this_thread::sleep_for(std::chrono::seconds{1});
    {
        dolphindb::TableSP res_2 = conn_2.run(table_name);
        ASSERT_EQ(res_2->size(),1);
        ASSERT_EQ(res_2->getColumn(0)->getRow(0)->getString(), "0");
    }
    std::this_thread::sleep_for(std::chrono::seconds{5});
    {
        ASSERT_EQ(host_label_list.size(), 1);
        ASSERT_EQ(table_list.size(), 1);
        ASSERT_EQ(host_label_list[0], "conn_1");
        ASSERT_EQ(table_list[0]->getColumn(0)->getRow(0)->getString(), "0");
    }
}

TEST_F(StreamReplicatorTest, insert_data_fail_1_on_data_dump_without_label_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share table(array(INT) as data) as "+table_name);
    conn_2.run("share table(array(INT) as data) as "+table_name);
    dolphindb::ReplicatorConfig config;
    config.setRetry(3, std::chrono::seconds{1});
    std::vector<std::string> host_label_list;
    std::vector<dolphindb::ConstantSP> table_list;
    config.onDataDump([&](const std::string &hostLabel, dolphindb::ConstantSP table) -> bool {
        host_label_list.emplace_back(hostLabel);
        table_list.emplace_back(table);
        return true;
    });
    dolphindb::StreamReplicator streamReplocator{{{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, "conn_2"}}, table_name, config};
    conn_1.run("undef `"+table_name+",SHARED");
    streamReplocator.insert(dolphindb::Util::createInt(0));
    std::this_thread::sleep_for(std::chrono::seconds{1});
    {
        dolphindb::TableSP res_2 = conn_2.run(table_name);
        ASSERT_EQ(res_2->size(),1);
        ASSERT_EQ(res_2->getColumn(0)->getRow(0)->getString(), "0");
    }
    std::this_thread::sleep_for(std::chrono::seconds{5});
    {
        ASSERT_EQ(host_label_list.size(), 1);
        ASSERT_EQ(table_list.size(), 1);
        ASSERT_EQ(host_label_list[0], HOST_CLUSTER+":"+std::to_string(PORT_DNODE1));
        ASSERT_EQ(table_list[0]->getColumn(0)->getRow(0)->getString(), "0");
    }
}

TEST_F(StreamReplicatorTest, insert_data_fail_all_on_data_dump_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share table(array(INT) as data) as "+table_name);
    conn_2.run("share table(array(INT) as data) as "+table_name);
    dolphindb::ReplicatorConfig config;
    config.setRetry(3, std::chrono::seconds{1});
    std::vector<std::string> host_label_list;
    std::vector<dolphindb::ConstantSP> table_list;
    config.onDataDump([&](const std::string &hostLabel, dolphindb::ConstantSP table) -> bool {
        host_label_list.emplace_back(hostLabel);
        table_list.emplace_back(table);
        return true;
    });
    dolphindb::StreamReplicator streamReplocator{{{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "conn_1"}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, "conn_2"}}, table_name, config};
    conn_1.run("undef `"+table_name+",SHARED");
    conn_2.run("undef `"+table_name+",SHARED");
    streamReplocator.insert(dolphindb::Util::createInt(0));
    std::this_thread::sleep_for(std::chrono::seconds{10});
    {
        ASSERT_EQ(host_label_list.size(), 2);
        ASSERT_EQ(table_list.size(), 2);
        ASSERT_EQ(table_list[0]->getColumn(0)->getRow(0)->getString(), "0");
        ASSERT_EQ(table_list[0]->getString(), table_list[1]->getString());
    }
}

TEST_F(StreamReplicatorTest, insert_data_conn_1_on_connection_state_change_with_label_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share table(array(INT) as data) as "+table_name);
    conn_2.run("share table(array(INT) as data) as "+table_name);
    dolphindb::ReplicatorConfig config;
    config.setRetry(1, std::chrono::seconds{1});
    std::vector<std::string> host_label_list;
    std::vector<dolphindb::ConnectionState> state_list;
    config.onConnectionStateChange([&](const std::string &hostLabel, dolphindb::ConnectionState state) -> bool {
        host_label_list.emplace_back(hostLabel);
        state_list.emplace_back(state);
        return true;
    });
    dolphindb::StreamReplicator streamReplocator{{{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "conn_1"}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, "conn_2"}}, table_name, config};
    conn_1.run("undef `"+table_name+",SHARED");
    streamReplocator.insert(dolphindb::Util::createInt(0));
    std::this_thread::sleep_for(std::chrono::seconds{1});
    {
        dolphindb::TableSP res_2 = conn_2.run(table_name);
        ASSERT_EQ(res_2->size(),1);
        ASSERT_EQ(res_2->getColumn(0)->getRow(0)->getString(), "0");
    }
    std::this_thread::sleep_for(std::chrono::seconds{5});
    {
        ASSERT_EQ(host_label_list.size(), 1);
        ASSERT_EQ(state_list.size(), 1);
        ASSERT_EQ(host_label_list[0], "conn_1");
        ASSERT_EQ(state_list[0], dolphindb::ConnectionState::Reconnecting);
    }
}

TEST_F(StreamReplicatorTest, insert_data_conn_1_on_connection_state_change_without_label_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share table(array(INT) as data) as "+table_name);
    conn_2.run("share table(array(INT) as data) as "+table_name);
    dolphindb::ReplicatorConfig config;
    config.setRetry(1, std::chrono::seconds{1});
    std::vector<std::string> host_label_list;
    std::vector<dolphindb::ConnectionState> state_list;
    config.onConnectionStateChange([&](const std::string &hostLabel, dolphindb::ConnectionState state) -> bool {
        host_label_list.emplace_back(hostLabel);
        state_list.emplace_back(state);
        return true;
    });
    dolphindb::StreamReplicator streamReplocator{{{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER}}, table_name, config};
    conn_1.run("undef `"+table_name+",SHARED");
    streamReplocator.insert(dolphindb::Util::createInt(0));
    std::this_thread::sleep_for(std::chrono::seconds{1});
    {
        dolphindb::TableSP res_2 = conn_2.run(table_name);
        ASSERT_EQ(res_2->size(),1);
        ASSERT_EQ(res_2->getColumn(0)->getRow(0)->getString(), "0");
    }
    std::this_thread::sleep_for(std::chrono::seconds{5});
    {
        ASSERT_EQ(host_label_list.size(), 1);
        ASSERT_EQ(state_list.size(), 1);
        ASSERT_EQ(host_label_list[0], HOST_CLUSTER+":"+std::to_string(PORT_DNODE1));
        ASSERT_EQ(state_list[0], dolphindb::ConnectionState::Reconnecting);
    }
}

TEST_F(StreamReplicatorTest, insert_data_conn_all_on_connection_state_change_with_label_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share table(array(INT) as data) as "+table_name);
    conn_2.run("share table(array(INT) as data) as "+table_name);
    dolphindb::ReplicatorConfig config;
    config.setRetry(1, std::chrono::seconds{1});
    std::vector<dolphindb::ConnectionState> state_list_1;
    std::vector<dolphindb::ConnectionState> state_list_2;
    config.onConnectionStateChange([&](const std::string &hostLabel, dolphindb::ConnectionState state) -> bool {
        if (hostLabel == "conn_1")
            state_list_1.emplace_back(state);
        else
            state_list_2.emplace_back(state);
        return true;
    });
    dolphindb::StreamReplicator streamReplocator{{{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "conn_1"}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, "conn_2"}}, table_name, config};
    conn_1.run("undef `"+table_name+",SHARED");
    conn_2.run("undef `"+table_name+",SHARED");
    streamReplocator.insert(dolphindb::Util::createInt(0));
    std::this_thread::sleep_for(std::chrono::seconds{5});
    {
        ASSERT_EQ(state_list_1.size(), 1);
        ASSERT_EQ(state_list_1[0], dolphindb::ConnectionState::Reconnecting);
        ASSERT_EQ(state_list_2.size(), 1);
        ASSERT_EQ(state_list_2[0], dolphindb::ConnectionState::Reconnecting);
    }
}

TEST_F(StreamReplicatorTest, insert_data_conn_all_on_connection_state_change_without_label_should_serial){
    std::string table_name = getCaseName();
    dolphindb::DBConnection conn_1{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER};
    dolphindb::DBConnection conn_2{HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER};
    conn_1.run("share table(array(INT) as data) as "+table_name);
    conn_2.run("share table(array(INT) as data) as "+table_name);
    dolphindb::ReplicatorConfig config;
    config.setRetry(1, std::chrono::seconds{1});
    std::vector<dolphindb::ConnectionState> state_list_1;
    std::vector<dolphindb::ConnectionState> state_list_2;
    config.onConnectionStateChange([&](const std::string &hostLabel, dolphindb::ConnectionState state) -> bool {
        if (hostLabel == HOST_CLUSTER+":"+std::to_string(PORT_DNODE1))
            state_list_1.emplace_back(state);
        else
            state_list_2.emplace_back(state);
        return true;
    });
    dolphindb::StreamReplicator streamReplocator{{{HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER}, {HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER}}, table_name, config};
    conn_1.run("undef `"+table_name+",SHARED");
    conn_2.run("undef `"+table_name+",SHARED");
    streamReplocator.insert(dolphindb::Util::createInt(0));
    std::this_thread::sleep_for(std::chrono::seconds{5});
    {
        ASSERT_EQ(state_list_1.size(), 1);
        ASSERT_EQ(state_list_1[0], dolphindb::ConnectionState::Reconnecting);
        ASSERT_EQ(state_list_2.size(), 1);
        ASSERT_EQ(state_list_2[0], dolphindb::ConnectionState::Reconnecting);
    }
}