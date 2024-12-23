#include "config.h"

class DLoggerTest:public testing::Test
{
};


TEST_F(DLoggerTest, test_logTextType_with_default_level_debug)
{
    std::tuple<
        const char*,        // 字符串常量
        const void*,        // 指针常量
        std::string,        // std::string
        int,                // int
        char,               // char
        unsigned,           // unsigned int
        long,               // long
        unsigned long,      // unsigned long
        long long,          // long long
        unsigned long long, // unsigned long long
        float,              // float
        double,             // double
        long double         // long double
    > args = std::make_tuple(
        "Hello, DolphinDB!",    // const char*
        nullptr,            // const void*
        std::string("DDB"), // std::string
        42,                 // int
        'A',                // char
        123u,               // unsigned
        -12345L,            // long
        12345UL,            // unsigned long
        -9876543210LL,      // long long
        9876543210ULL,      // unsigned long long
        3.14f,              // float
        2.718,              // double
        1.61803398875L      // long double
    );
    constexpr size_t tuple_size = std::tuple_size<decltype(args)>::value;
    bool res = false;
    for (size_t i = 0; i < tuple_size; ++i) {
        switch (i) {
            case 0:
                res = DLogger::Info("now is :",std::get<0>(args));
                EXPECT_TRUE(res);
                res = DLogger::Debug("now is :",std::get<0>(args));
                EXPECT_TRUE(res);
                res = DLogger::Warn("now is :",std::get<0>(args));
                EXPECT_TRUE(res);
                res = DLogger::Error("now is :",std::get<0>(args));
                EXPECT_TRUE(res);
                break;
            case 1:
                res = DLogger::Info("now is :",std::get<1>(args));
                EXPECT_TRUE(res);
                res = DLogger::Debug("now is :",std::get<1>(args));
                EXPECT_TRUE(res);
                res = DLogger::Warn("now is :",std::get<1>(args));
                EXPECT_TRUE(res);
                res = DLogger::Error("now is :",std::get<1>(args));
                EXPECT_TRUE(res);
                break;
            case 2:
                res = DLogger::Info("now is :",std::get<2>(args));
                EXPECT_TRUE(res);
                res = DLogger::Debug("now is :",std::get<2>(args));
                EXPECT_TRUE(res);
                res = DLogger::Warn("now is :",std::get<2>(args));
                EXPECT_TRUE(res);
                res = DLogger::Error("now is :",std::get<2>(args));
                EXPECT_TRUE(res);
                break;
            case 3:
                res = DLogger::Info("now is :",std::get<3>(args));
                EXPECT_TRUE(res);
                res = DLogger::Debug("now is :",std::get<3>(args));
                EXPECT_TRUE(res);
                res = DLogger::Warn("now is :",std::get<3>(args));
                EXPECT_TRUE(res);
                res = DLogger::Error("now is :",std::get<3>(args));
                EXPECT_TRUE(res);
                break;
            case 4:
                res = DLogger::Info("now is :",std::get<4>(args));
                EXPECT_TRUE(res);
                res = DLogger::Debug("now is :",std::get<4>(args));
                EXPECT_TRUE(res);
                res = DLogger::Warn("now is :",std::get<4>(args));
                EXPECT_TRUE(res);
                res = DLogger::Error("now is :",std::get<4>(args));
                EXPECT_TRUE(res);
                break;
            case 5:
                res = DLogger::Info("now is :",std::get<5>(args));
                EXPECT_TRUE(res);
                res = DLogger::Debug("now is :",std::get<5>(args));
                EXPECT_TRUE(res);
                res = DLogger::Warn("now is :",std::get<5>(args));
                EXPECT_TRUE(res);
                res = DLogger::Error("now is :",std::get<5>(args));
                EXPECT_TRUE(res);
                break;
            case 6:
                res = DLogger::Info("now is :",std::get<6>(args));
                EXPECT_TRUE(res);
                res = DLogger::Debug("now is :",std::get<6>(args));
                EXPECT_TRUE(res);
                res = DLogger::Warn("now is :",std::get<6>(args));
                EXPECT_TRUE(res);
                res = DLogger::Error("now is :",std::get<6>(args));
                EXPECT_TRUE(res);
                break;
            case 7:
                res = DLogger::Info("now is :",std::get<7>(args));
                EXPECT_TRUE(res);
                res = DLogger::Debug("now is :",std::get<7>(args));
                EXPECT_TRUE(res);
                res = DLogger::Warn("now is :",std::get<7>(args));
                EXPECT_TRUE(res);
                res = DLogger::Error("now is :",std::get<7>(args));
                EXPECT_TRUE(res);
                break;
            case 8:
                res = DLogger::Info("now is :",std::get<8>(args));
                EXPECT_TRUE(res);
                res = DLogger::Debug("now is :",std::get<8>(args));
                EXPECT_TRUE(res);
                res = DLogger::Warn("now is :",std::get<8>(args));
                EXPECT_TRUE(res);
                res = DLogger::Error("now is :",std::get<8>(args));
                EXPECT_TRUE(res);
                break;
            case 9:
                res = DLogger::Info("now is :",std::get<9>(args));
                EXPECT_TRUE(res);
                res = DLogger::Debug("now is :",std::get<9>(args));
                EXPECT_TRUE(res);
                res = DLogger::Warn("now is :",std::get<9>(args));
                EXPECT_TRUE(res);
                res = DLogger::Error("now is :",std::get<9>(args));
                EXPECT_TRUE(res);
                break;
            case 10:
                res = DLogger::Info("now is :",std::get<10>(args));
                EXPECT_TRUE(res);
                res = DLogger::Debug("now is :",std::get<10>(args));
                EXPECT_TRUE(res);
                res = DLogger::Warn("now is :",std::get<10>(args));
                EXPECT_TRUE(res);
                res = DLogger::Error("now is :",std::get<10>(args));
                EXPECT_TRUE(res);
                break;
            case 11:
                res = DLogger::Info("now is :",std::get<11>(args));
                EXPECT_TRUE(res);
                res = DLogger::Debug("now is :",std::get<11>(args));
                EXPECT_TRUE(res);
                res = DLogger::Warn("now is :",std::get<11>(args));
                EXPECT_TRUE(res);
                res = DLogger::Error("now is :",std::get<11>(args));
                EXPECT_TRUE(res);
                break;
            case 12:
                res = DLogger::Info("now is :",std::get<12>(args));
                EXPECT_TRUE(res);
                res = DLogger::Debug("now is :",std::get<12>(args));
                EXPECT_TRUE(res);
                res = DLogger::Warn("now is :",std::get<12>(args));
                EXPECT_TRUE(res);
                res = DLogger::Error("now is :",std::get<12>(args));
                EXPECT_TRUE(res);
                break;
            default:
                break;
        }
    }
}


#ifndef _WIN32
TEST_F(DLoggerTest, DLogger_funcs){
    {
        RecordTime t("name");
        Util::sleep(200);
    }
    char workDir[256]{};
    getcwd(workDir, sizeof(workDir));
    std::string file = std::string(workDir).append("/tempFile123");
    DLogger::SetLogFilePath(file);
    auto level = DLogger::GetMinLevel();
    DLogger::Info("123", "345");
    DLogger::SetLogFilePath("");
    DLogger::SetMinLevel(DLogger::Level::LevelWarn);
    DLogger::Info("123", "345");
    DLogger::Error("123", "345");
    remove(file.c_str());

    {
        RecordTime t("name");
        Util::sleep(100);
    }
    {
        RecordTime t("name2");
        Util::sleep(100);
    }
    {
        RecordTime t("name2");
        Util::sleep(200);
    }
    RecordTime::printAllTime();
}

TEST_F(DLoggerTest, logfile){
    {
        RecordTime t("name");
        Util::sleep(200);
    }
    char workDir[256]{};
    const char* path = getcwd(workDir, sizeof(workDir));
    printf("%s\n", path);
    std::string file = std::string(workDir).append("/tempFile123");
    DLogger::SetLogFilePath(file);
    auto level = DLogger::GetMinLevel();
    DLogger::Info("123", "345");
    DLogger::SetLogFilePath("");
    DLogger::SetMinLevel(DLogger::Level::LevelWarn);
    DLogger::Info("123", "345");
    DLogger::Error("123", "345");
    remove(file.c_str());

    {
        RecordTime t("name");
        Util::sleep(100);
    }
    {
        RecordTime t("name2");
        Util::sleep(100);
    }
    {
        RecordTime t("name2");
        Util::sleep(200);
    }
    RecordTime::printAllTime();
}
#endif

TEST_F(DLoggerTest, test_logLevel)
{
    {
        DLogger::SetMinLevel(DLogger::Level::LevelDebug);
        bool res = DLogger::Debug("debug");
        EXPECT_TRUE(res);
        res = DLogger::Info("info");
        EXPECT_TRUE(res);
        res = DLogger::Warn("warn");
        EXPECT_TRUE(res);
        res = DLogger::Error("error");
        EXPECT_TRUE(res);
    }
    {
        DLogger::SetMinLevel(DLogger::Level::LevelInfo);
        bool res = DLogger::Debug("debug");
        EXPECT_FALSE(res);
        res = DLogger::Info("info");
        EXPECT_TRUE(res);
        res = DLogger::Warn("warn");
        EXPECT_TRUE(res);
        res = DLogger::Error("error");
        EXPECT_TRUE(res);
    }
    {
        DLogger::SetMinLevel(DLogger::Level::LevelWarn);
        bool res = DLogger::Debug("debug");
        EXPECT_FALSE(res);
        res = DLogger::Info("info");
        EXPECT_FALSE(res);
        res = DLogger::Warn("warn");
        EXPECT_TRUE(res);
        res = DLogger::Error("error");
        EXPECT_TRUE(res);
    }
    {
        DLogger::SetMinLevel(DLogger::Level::LevelError);
        bool res = DLogger::Debug("debug");
        EXPECT_FALSE(res);
        res = DLogger::Info("info");
        EXPECT_FALSE(res);
        res = DLogger::Warn("warn");
        EXPECT_FALSE(res);
        res = DLogger::Error("error");
        EXPECT_TRUE(res);
    }
}