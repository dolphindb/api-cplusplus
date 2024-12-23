#include "Logger.h"
#include "Util.h"
#include "spdlog/sinks/stdout_color_sinks.h"

namespace {
std::shared_ptr<spdlog::logger> initLogger()
{
    auto logger = spdlog::stdout_color_mt("DolphinDBAPI");
    logger->set_level(spdlog::level::debug);
    return logger;
}
}

namespace dolphindb {

std::shared_ptr<spdlog::logger> DdbLogger = initLogger();

DLogger::Level DLogger::minLevel_ = DLogger::LevelDebug;
std::string DLogger::logFilePath_;

void DLogger::SetMinLevel(Level level)
{
    minLevel_ = level;
}

bool DLogger::WriteLog(std::string &text, spdlog::level::level_enum level)
{
    DdbLogger->log(level, text.c_str());
    if(logFilePath_.empty()==false){
        text+="\n";
        Util::writeFile(logFilePath_.data(),text.data(),text.length());
    }
    return true;
}

bool DLogger::FormatFirst(std::string &text, Level level)
{
    if (level < minLevel_) {
        return false;
    }
    text = text + "[" + std::to_string(Util::getCurThreadId()) + "] " + ":";
    return true;
}

}
