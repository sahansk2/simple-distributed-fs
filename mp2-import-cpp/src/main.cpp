#include "node.hpp"

#include <iostream>
#include <string>
#include <cstdlib>
#include "httplib.h"
#include "json.hpp"
#include "constants.hpp"
#include "logging.hpp"

using json = nlohmann::json;

/**
  entrypoint for MP2. Takes in two arguments:

  @param machine_id unique id for this machine
  @param is_introducer whether this machine is the interoducer
*/
int main(int argc, char* argv[]) {
    if (argc != 4+1 && argc != 4 && argc != 7 ) {
        std::cout << "Usage: ./bin/exec <machine_id> <is_introducer> <port> <loglevel=ERROR,WARN,INFO,DEBUG,TRACE (optional)>" << std::endl;
        std::cout << "(Alt usage): ./bin/exec <machine_id> <is_introducer> <port> <ping_rate> <drop_threshold> <loss_rate>" << std::endl;
        return 1;
    }
    int machine_id = std::stoi(argv[1]);
    bool is_introducer = std::string(argv[2]) == "true";
    unsigned int port = static_cast<unsigned int>(std::stoi(argv[3]));
    if (argc == 5) {
      SET_LOGGER(argv[4]);
    } else {
      SET_LOGGER(nullptr);
    }

    int ping_rate = 1;
    int drop_threshold = 3;
    double loss_rate = 0.0;
    if (argc == 6) {
      ping_rate = std::stoi(argv[4]);
      drop_threshold = std::stoi(argv[5]);
      loss_rate = std::stod(argv[6]);
    }

    std::mutex cmd_mtx;
    
    Node machine(machine_id, is_introducer, ping_rate, drop_threshold, loss_rate, port);

    httplib::Server svr;

    svr.Get("/mp2/listmem", [&machine, &cmd_mtx](const httplib::Request &, httplib::Response &res){
      STDERR_LOG(LOG_DEBUG, (std::cerr << "Entered /mp2/listmem" << '\n'));
      STDERR_LOG(LOG_TRACE, (std::cerr << "/mp2/listmem is about to acquire cmd_mtx..." << '\n'));
      cmd_mtx.lock();
      STDERR_LOG(LOG_TRACE, (std::cerr << "/mp2/listmem acquired cmd_mtx" << '\n'));
      auto return_str = machine.HandleUserCommands("LIST_MEM");
      STDERR_LOG(LOG_TRACE, (std::cerr << "/mp2/listmem releasing cmd_mtx..." << '\n'));
      cmd_mtx.unlock();
      STDERR_LOG(LOG_DEBUG, (std::cerr << "/mp2/listmem is about to return " << return_str << '\n'));
      res.set_content(return_str, "application/json");
    });

    svr.Get("/mp2/listself", [&machine, &cmd_mtx](const httplib::Request &, httplib::Response &res){
      STDERR_LOG(LOG_DEBUG, (std::cerr << "Entered /mp2/listself" << '\n'));
      STDERR_LOG(LOG_TRACE, (std::cerr << "/mp2/listself is about to acquire cmd_mtx..." << '\n'));
      cmd_mtx.lock();
      STDERR_LOG(LOG_DEBUG, (std::cerr << "/mp2/listself acquired cmd_mtx" << '\n'));
      auto return_str = machine.HandleUserCommands("LIST_SELF");
      STDERR_LOG(LOG_TRACE, (std::cerr << "/mp2/listself releasing cmd_mtx..." << '\n'));
      cmd_mtx.unlock(); 
      res.set_content(return_str, "application/json");
    });

    svr.Get("/mp2/join", [&machine, &cmd_mtx](const httplib::Request &, httplib::Response &res){
      json j;
      j["join"] = "true";
      STDERR_LOG(LOG_DEBUG, (std::cerr << "Entered /mp2/join" << '\n'));
      STDERR_LOG(LOG_TRACE, (std::cerr << "/mp2/join is about to acquire cmd_mtx..." << '\n'));
      cmd_mtx.lock();
      STDERR_LOG(LOG_TRACE, (std::cerr << "/mp2/join acquired cmd_mtx" << '\n'));      
      auto return_str = machine.HandleUserCommands("JOIN");
      j["data"] = return_str;
      STDERR_LOG(LOG_TRACE, (std::cerr << "/mp2/join releasing cmd_mtx..." << '\n'));
      cmd_mtx.unlock();
      res.set_content(j.dump(), "application/json");
    });

    svr.Get("/mp2/leave", [&machine, &cmd_mtx](const httplib::Request &, httplib::Response &res){
      json j;
      j["leave"] = "true";
      STDERR_LOG(LOG_DEBUG, (std::cerr << "Entered /mp2/leave" << '\n'));
      STDERR_LOG(LOG_TRACE, (std::cerr << "/mp2/leave is about to acquire cmd_mtx..." << '\n'));
      cmd_mtx.lock();
      STDERR_LOG(LOG_TRACE, (std::cerr << "/mp2/leave acquired cmd_mtx" << '\n'));     
      auto return_str = machine.HandleUserCommands("LEAVE");  
      j["data"] = return_str;
      STDERR_LOG(LOG_TRACE, (std::cerr << "/mp2/leave releasing cmd_mtx..." << '\n'));
      cmd_mtx.unlock();
      res.set_content(j.dump(), "application/json");
    });
    
    svr.Get("/mp2/quit", [&machine, &cmd_mtx, &svr](const httplib::Request &, httplib::Response &res){
      json j;
      j["quit"] = "true";
      STDERR_LOG(LOG_DEBUG, (std::cerr << "Entered /mp2/quit" << '\n'));
      STDERR_LOG(LOG_TRACE, (std::cerr << "/mp2/quit is about to acquire cmd_mtx..." << '\n'));      
      cmd_mtx.lock();
      STDERR_LOG(LOG_TRACE, (std::cerr << "/mp2/quit acquired cmd_mtx" << '\n'));      
      auto return_str = machine.HandleUserCommands("STOP");
      j["data"] = return_str;
      STDERR_LOG(LOG_TRACE, (std::cerr << "/mp2/quit releasing cmd_mtx..." << '\n'));
      cmd_mtx.unlock();
      res.set_content(j.dump(), "application/json");
      svr.stop();
    });

    machine.Start();
    svr.listen("0.0.0.0", cs425::SERVE_PORT);
    STDERR_LOG(LOG_DEBUG, (std::cerr << "MP2 server going down, have a nice _______!"));

    return 0;
}