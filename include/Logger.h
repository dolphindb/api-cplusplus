#pragma once

#include <string>
#include "Exports.h"
namespace dolphindb {

class EXPORT_DECL DLogger {
public:
    enum Level {
        LevelDebug,
        LevelInfo,
        LevelWarn,
        LevelError,
        LevelCount,
    };
    template<typename... TArgs>
    static bool Info(TArgs... args) {
        std::string text;
        return Write(text, LevelInfo, 0, args...);
    }
    template<typename... TArgs>
    static bool Debug(TArgs... args) {
        std::string text;
        return Write(text, LevelDebug, 0, args...);
    }
    template<typename... TArgs>
    static bool Warn(TArgs... args) {
        std::string text;
        return Write(text, LevelWarn, 0, args...);
    }
    template<typename... TArgs>
    static bool Error(TArgs... args) {
        std::string text;
        return Write(text, LevelError, 0, args...);
    }
    static void SetLogFilePath(const std::string &filepath){ logFilePath_=filepath; }
    static void SetMinLevel(Level level);
    static Level GetMinLevel(){ return minLevel_; }
private:
    static Level minLevel_;
    static std::string logFilePath_;
    static std::string levelText_[LevelCount];
    static bool FormatFirst(std::string &text, Level level);
    static bool WriteLog(std::string &text);
    template<typename TA, typename... TArgs>
    static bool Write(std::string &text, Level level, int deepth, TA first, TArgs... args) {
        if (deepth == 0) {
            if (FormatFirst(text, level) == false)
                return false;
        }
        text += " " + Create(first);
        return Write(text, level, deepth + 1, args...);
    }
    template<typename TA>
    static bool Write(std::string &text, Level level, int deepth, TA first) {
        if (deepth == 0) {
            if (FormatFirst(text, level) == false)
                return false;
        }
        text += " " + Create(first);
        return WriteLog(text);
    }
    static std::string Create(const char *value) {
        std::string str(value);
        return str;
    }
    static std::string Create(const void *value) {
        return Create((unsigned long long)value);
    }
    static std::string Create(std::string str) {
        return str;
    }
    static std::string Create(int value) {
        return std::to_string(value);
    }
    static std::string Create(char value) {
        return std::string(&value, 1);
    }
    static std::string Create(unsigned value) {
        return std::to_string(value);
    }
    static std::string Create(long value) {
        return std::to_string(value);
    }
    static std::string Create(unsigned long value) {
        return std::to_string(value);
    }
    static std::string Create(long long value) {
        return std::to_string(value);
    }
    static std::string Create(unsigned long long value) {
        return std::to_string(value);
    }
    static std::string Create(float value) {
        return std::to_string(value);
    }
    static std::string Create(double value) {
        return std::to_string(value);
    }
    static std::string Create(long double value) {
        return std::to_string(value);
    }
};

#define DLOG true ? dolphindb::DLogger::GetMinLevel() : dolphindb::DLogger::Info

}