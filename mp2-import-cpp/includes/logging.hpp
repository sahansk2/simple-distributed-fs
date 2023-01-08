#ifndef LOGGING_HPP
#define LOGGING_HPP

#include <string>
#include <mutex>

#define LOG_ERROR 1
#define LOG_WARN 2
#define LOG_INFO 3
#define LOG_DEBUG 4
#define LOG_TRACE 5

#define RED_COLOR "\033[0;31m"
#define YELLOW_COLOR "\033[1;33m"
#define BLUE_COLOR "\033[0;36m"
#define MAGENTA_COLOR "\033[0;35m"
#define END_COLOR "\033[0m"

// Macro because I want to use the __PRETTY_FUNC__ / __LINE__ / __FILE__ variable, and actually calling a function will mess that up.
// Sorry that this line is super long
// Look at logging.cpp to see proper usage; this macro exploits the fact that (std::cerr << {} << {} << ....) actually evaluates to an expression.
// so NO SEMICOLON!!!

extern std::mutex LOGGING_MUTEX_;
extern unsigned int GLOBAL_LOGLEVEL; // Only set once, don't worry about race conditions
extern const std::string LOG_COLORS[6];
extern const std::string LOG_NAMES[6];
#define STDERR_LOG(level, arg) { if (level <= GLOBAL_LOGLEVEL) {LOGGING_MUTEX_.lock(); std::cerr << LOG_COLORS[level] << "MP2[" << LOG_NAMES[level]  << "]" << END_COLOR << " " << __FILE__ << ":" << __LINE__ << ":" << __PRETTY_FUNCTION__ << " -- "; arg; LOGGING_MUTEX_.unlock();} }


void SET_LOGGER(char* loglevel_c);


#endif
