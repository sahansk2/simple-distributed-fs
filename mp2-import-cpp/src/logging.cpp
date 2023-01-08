#include <string>
#include <iostream>
#include <stdlib.h>  
#include "logging.hpp"

unsigned int GLOBAL_LOGLEVEL;
std::mutex LOGGING_MUTEX_;
const std::string LOG_COLORS[6] = {"", RED_COLOR, MAGENTA_COLOR, YELLOW_COLOR, BLUE_COLOR, ""}; // No need for TRACE, who uses that anyway??
const std::string LOG_NAMES[6] = {"UNUSED", "ERRO", "WARN", "INFO", "DEBU", "TRAC"}; // No need for TRACE, who uses that anyway??

void SET_LOGGER(char * loglevel_c) {
    if (loglevel_c != nullptr) {
        auto loglevel = std::string(loglevel_c);
        if (loglevel == std::string("ERROR")) {
            GLOBAL_LOGLEVEL = 1;
        } else if (loglevel == std::string("WARN")) {
            GLOBAL_LOGLEVEL = 2;
        } else if (loglevel == std::string("INFO")) {
            GLOBAL_LOGLEVEL = 3;
        } else if (loglevel == std::string("DEBUG")) {
            GLOBAL_LOGLEVEL = 4;
        } else if (loglevel == std::string("TRACE")) {
            GLOBAL_LOGLEVEL = 5;
        } else {
                GLOBAL_LOGLEVEL = 5; // for demo only.
                std::cerr << "Unrecognized loglevel. Please specify one of <ERROR, WARN, INFO, DEBUG, TRACE>, case sensitive!" << std::endl;
                STDERR_LOG(LOG_ERROR, (
                    std::cerr << "This is error" << '\n'
                ));
                STDERR_LOG(LOG_WARN, (
                    std::cerr << "This is warn" << '\n'
                ));
                STDERR_LOG(LOG_INFO, (
                    std::cerr << "This is info" << '\n'
                ));
                STDERR_LOG(LOG_DEBUG, (
                    std::cerr << "This is debug" << '\n'
                ));
                STDERR_LOG(LOG_TRACE, (
                    std::cerr << "This is trace" << '\n'
                ));
                exit(3);
        }
    } else {
        std::cerr << "Loglevel not found. Defaulting to ERROR." << std::endl;
        GLOBAL_LOGLEVEL = 0;
    }
}