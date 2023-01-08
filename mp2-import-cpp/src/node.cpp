#include "node.hpp"

#include "constants.hpp"
#include "message.hpp"
#include "socket.hpp"
#include "json.hpp"
#include "httplib.h"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <fstream>
#include <sstream>

#include <optional>
#include <thread>
#include <vector>
#include <regex>

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include <memory>
#include "logging.hpp"

using json = nlohmann::json;

#define nil 0


int execRemoteCommand(std::string cmd, std::string outfname, std::string filter); // it go up

Node::Node(int id, bool is_introducer, int ping_rate, int drop_threshold, double loss_rate, unsigned int port):
    id_{id}, is_introducer_{is_introducer}, ping_rate_{ping_rate}, drop_threshold_{drop_threshold}, loss_rate_{loss_rate}, port_{port} {}

static std::string hostname(void) {
    std::string ip;
    {
        std::unique_ptr<char[]> addr(new char[cs425::HOSTNAME_LEN]);
        int rc = gethostname(addr.get(), cs425::HOSTNAME_LEN);
        if (rc != 0) {
            STDERR_LOG(LOG_ERROR, (std::cerr << "Couldn't get hostname! Returning IP equiv. of -1." << '\n'));
            return "255.255.255.255";
        }
        ip = std::string(addr.get());
        STDERR_LOG(LOG_DEBUG, (std::cerr << "Extracted hostname: " << ip << '\n'));
    }
    return ip;
}

void Node::Start() {
    start_time_ = std::chrono::steady_clock::now();
    if (is_introducer_) {
        STDERR_LOG(LOG_DEBUG, (std::cerr << "DNS initializer flag passed. Overwriting DNS with our hostname." << '\n'));
         
        // Construct the DNS. Echo our hostname and the fixed SERVE_PORT into the global file.
        std::stringstream ss;
        ss << "echo " << hostname() << ":" << this->port_ << " > " << cs425::DNS_PATH;
        int rc = execRemoteCommand(ss.str(), "/dev/null", "");
        if (rc != 0) {
            STDERR_LOG(LOG_ERROR, (std::cerr << "Couldn't exec remote command to set DNS!\n"));
        }
    }

    std::thread ping_thread(&Node::PingSuccessors, this);
    ping_thread.detach();
    std::thread process_thread(&Node::MessageHandler, this);
    process_thread.detach();

    // Node::HandleUserCommands("");
}

std::string Node::HandleUserCommandLISTMEM() {
    STDERR_LOG(LOG_TRACE, (std::cerr << "About to acquire mutex_..." << '\n'));
    mutex_.lock();
    STDERR_LOG(LOG_TRACE, (std::cerr << "Acquired mutex_." << '\n'));
    json vec(members_);
    STDERR_LOG(LOG_TRACE, (std::cerr << "Releasing mutex_...." << '\n'));
    mutex_.unlock();
    STDERR_LOG(LOG_DEBUG, (std::cerr << "HandleUserCommandLISTMEM returns: \n"))
    STDERR_LOG(LOG_DEBUG, (std::cerr << "\t"  << vec.dump() << '\n'));
    return vec.dump();
}

std::string Node::HandleUserCommandLISTSELF() {
    STDERR_LOG(LOG_TRACE, (std::cerr << "About to acquire mutex_..." << '\n'));
    mutex_.lock();
    STDERR_LOG(LOG_TRACE, (std::cerr << "Acquired mutex_." << '\n'));
    json j_self(self_);
    STDERR_LOG(LOG_TRACE, (std::cerr << "Releasing mutex_...." << '\n'));
    mutex_.unlock();
    STDERR_LOG(LOG_DEBUG, (std::cerr << "HandleUserCommandLISTSELF returns: \n"));
    STDERR_LOG(LOG_DEBUG, (std::cerr << "\t" << j_self.dump()<< '\n'));
    return j_self.dump();
}

std::string Node::HandleUserCommandJOIN() {
    return "Finished JOIN";
}

std::string Node::HandleUserCommandLEAVE() {
    return "{}";
}

std::string Node::HandleUserCommandQUIT() {
    return "{}";
}

std::string Node::HandleUserCommands(std::string input) {
    // while (true) {
    LogMisc("INFO", "Type any of the following: LIST_MEM/LIST_SELF/JOIN/LEAVE/WAIT/STOP");

    if (input == "LIST_MEM") {
        PrintMembers();
        return HandleUserCommandLISTMEM();
    } else if (input == "LIST_SELF") {
        PrintSelf();
        return HandleUserCommandLISTSELF();
    } else if (input == "JOIN") {
        if (is_member_) {
            LogMisc("ERROR", "Node is already in group, ignoring JOIN");
            return "Node is already in group, ignoring JOIN";
        }

        // Don't know IP address until JoinAck received, so start w/ ID only
        time_t time_since_epoch = time(0);
        std::string compound_id = std::to_string(id_) + "-" + std::to_string(time_since_epoch);
        std::string ip = hostname();
        Member self_partial{compound_id, ip, 0};

        Message outbound_message{MessageType::Join, {self_partial}};
        std::string serialized_outbound_message = Message::Serialize(outbound_message);
        auto introducer_str = QueryDNS(); 
        STDERR_LOG(LOG_DEBUG, (std::cerr << "About to send a join to: " << introducer_str.first << ":"  << introducer_str.second << '\n'));
        Socket::TrySendMessage(
            serialized_outbound_message, 
            std::atoi(introducer_str.second.c_str()), 
            introducer_str.first, // Used to be a DNS hostname 
            loss_rate_
        );
        return HandleUserCommandJOIN();
    } else if (input == "LEAVE") {
        if (!is_member_) {
            LogMisc("ERROR", "Node is not in group, ignoring LEAVE");
            return "Node is not in group, ignoring LEAVE";
        }

        Message outbound_message{MessageType::Leave, {self_}};
        std::string serialized_outbound_message = Message::Serialize(outbound_message);
        
        for (const Member& successor : GetSuccessors()) {
            Socket::TrySendMessage(serialized_outbound_message, port_, successor.address, loss_rate_);
        }

        STDERR_LOG(LOG_TRACE, (std::cerr << "About to acquire mutex_..." << '\n'));
        mutex_.lock();
        STDERR_LOG(LOG_TRACE, (std::cerr << "Acquired mutex_." << '\n'));

        members_.clear();
        STDERR_LOG(LOG_DEBUG, (std::cerr << "About to notify MP3..." << '\n'));
        NotifyMP3();
        STDERR_LOG(LOG_TRACE, (std::cerr << "Releasing mutex_...." << '\n'));
        mutex_.unlock();

        is_member_ = false;

        LogMemberLeave(self_);
        return HandleUserCommandLEAVE();
    } else if (input == "WAIT") {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        return "Waited";
    } else if (std::cin.eof() || input == "STOP"){
        return "no you can't";
    } else {
        LogMisc("ERROR", "Command not recognized, try again");
        return "Command not recognized, try again";
    }
    return "you can't do that guy";
}

void Node::PingSuccessors() {
    while (true) {
        if (is_member_) {
            for (const Member& member : GetSuccessors()) {
                STDERR_LOG(LOG_TRACE, (std::cerr << "About to acquire mutex_..." << '\n'));
                mutex_.lock();
                STDERR_LOG(LOG_TRACE, (std::cerr << "Acquired mutex_." << '\n'));

                std::vector<Member>::iterator it = std::find(members_.begin(), members_.end(), member);

                int num_dropped = (++(it->pings_dropped));

                STDERR_LOG(LOG_TRACE, (std::cerr << "Releasing mutex_...." << '\n'));
                mutex_.unlock();

                if (num_dropped > drop_threshold_) {
                    HandleLeave(member);
                    LogMemberFailure(member);
                } else {
                    Message outbound_message{MessageType::Ping, {}};
                    std::string serialized_outbound_message = Message::Serialize(outbound_message);
                    Socket::TrySendMessage(serialized_outbound_message, port_, member.address, loss_rate_);
                }
            }
        }

        std::this_thread::sleep_for(std::chrono::seconds(ping_rate_));
    }
}

void Node::MessageHandler() {
    std::optional<Socket> opt_socket = Socket::ConstructReceiver(port_);
    Socket socket = std::move(*opt_socket);
    while (true) {
        std::pair<std::string, std::string> result = socket.ReceiveMessage();
        std::string serialized_received_message = result.first;
        std::string sender = result.second;
         
        // No sender address found - skip
        if (sender == "") {
            continue;
        }

        Message received_message = Message::Deserialize(serialized_received_message);

        switch (received_message.GetType()) {
            case MessageType::Join: {
                STDERR_LOG(LOG_DEBUG, (std::cerr << "Got a JOIN\n"));
                // Only Introducer can process Join messages
                if (is_introducer_ && !received_message.GetMembers().empty()) {              
                    Member new_member = received_message.GetMembers().at(0);
                    STDERR_LOG(LOG_DEBUG, (std::cerr << "The JOIN representing the new member is " << new_member << "\n"));
                    // Attach address to IP
                    new_member.id += "-" + sender;
                    new_member.address = sender;

                    // If this is Introducer Node or Introducer is present, can introduce to everyone
                    if (std::stoi(new_member.id) == id_ || is_member_) {
                        HandleJoin(new_member);
                    } else {
                        STDERR_LOG(LOG_INFO, (std::cerr << "We cannot process the JOIN. \n"));
                        LogMisc("ERROR", "JOIN rejected, introducer not in group yet");
                    }                    
                }

                break;
            }
            case MessageType::JoinAck: {
                STDERR_LOG(LOG_DEBUG, (std::cerr << "Got a JoinACK\n"));
                HandleJoinAck(received_message.GetMembers());

                break;
            }
            case MessageType::Introduce: {
                STDERR_LOG(LOG_TRACE, (std::cerr << "Received a message of type: introduce \n"));
                if (is_member_ && !received_message.GetMembers().empty()) {
                    Member new_member = received_message.GetMembers().at(0);
                    HandleIntroduce(new_member);
                }
                
                break;
            }
            case MessageType::Ping: {
                if (is_member_) {
                    HandlePing(sender);
                }

                break;
            }
            case MessageType::Ack: {
                if (is_member_ && !received_message.GetMembers().empty()) {
                    Member acking_member = received_message.GetMembers().at(0);
                    HandleAck(acking_member);
                }

                break;
            }
            case MessageType::Leave: {
                STDERR_LOG(LOG_TRACE, (std::cerr << "Received a message of type: leave \n"));
                if (is_member_ && !received_message.GetMembers().empty()) {
                    Member leaving_member = received_message.GetMembers().at(0);
                    HandleLeave(leaving_member);
                }

                break;
            }
            default: {
                STDERR_LOG(LOG_WARN, (std::cerr << "Unknown MessageType '" <<
                     std::to_string(static_cast<unsigned int>(received_message.GetType())) 
                     <<"' found. Original code called exit(1), we won't do that.\n"));
            }
        }
    }
}

int execRemoteCommand(std::string cmd, std::string outfname, std::string filter) {
    STDERR_LOG(LOG_DEBUG, (
        std::cerr << "Got function call to execute with querier: " 
            << '`' << cmd << '`' 
            << " to dump to " << outfname 
            << " with output filtered by `" << filter << "`." << '\n'));

    if (filter.length() == 0) {
        filter = "tee";
    }
    std::stringstream ss;
    char* dns_program = std::getenv("MP1_BIN");
    if (dns_program == nullptr) {
        STDERR_LOG(LOG_ERROR, (std::cerr << "couldn't find env variable MP1_BIN! \n"));
        return -1;
    }
    std::string dns_program_str(dns_program);
    ss  << dns_program_str
        << " -f "
        << cs425::DNS_ADDR
        << " -c \""
        << cmd
        << "\""
        << " 2> /dev/null "
        << "| " << filter 
        << "> " << outfname;
    STDERR_LOG(LOG_DEBUG, (
       std::cerr << "Final command about to be called with system(): " << "\n\t" << ss.str() << '\n'
    ));
    auto rc = system(ss.str().c_str());
    STDERR_LOG(LOG_DEBUG, (std::cerr << "executing: " << ss.str() << '\n'));
    if (rc != 0) {
        STDERR_LOG(LOG_ERROR, (std::cerr << "The command failed with return code: " << rc << ".\n"));
        STDERR_LOG(LOG_ERROR, (std::cerr << "exec failed with retcode " << rc << "!\n"));
        return rc;
    }
    return 0;
}



std::pair<std::string, std::string> Node::QueryDNS(void) {
    std::stringstream ss;
    // im sorry
    ss  << "sed 's/" << cs425::DNS_ADDR << ":6969: //'";
    auto rc = execRemoteCommand("cat " + cs425::DNS_PATH, "fetchdns.txt", ss.str());
    if (rc != 0) {
        STDERR_LOG(LOG_ERROR, (std::cerr << "Couldn't exec remote command!\n"));
        return {"INVAL","INVAL"};
    }
    std::fstream fetchdns;
    fetchdns.open("fetchdns.txt", std::ios_base::in);
    {
        std::string line;
        fetchdns >> line;
        std::regex tokre("^(.*?):(.*?)$");
        std::smatch matches;
        if (!std::regex_match(line, matches, tokre)) {
            STDERR_LOG(LOG_ERROR, (std::cerr << "Regex to parse the DNS file failed! Returning empty...\n"));
            return {{},{}}; // yes!
        }
        return {matches[1].str(), matches[2].str()};
    }
}


void Node::NotifyMP3(void) {
    STDERR_LOG(LOG_TRACE, (std::cerr << "Notifying mp3 of membership list change...\n")); 
    httplib::Client cli("localhost", cs425::MP3_PORT);
    cli.Get("/mp3/mp2notify");
}


/*
- Called _after_ membership list mutations have occurred
- Assume mutex acquired for membership list
*/
void Node::HandleDNS(void) {
    static Member prior = Member();
    ([](){([](){([](){([](){})();})();})();([](){})();})();
    auto min_member = std::min_element(members_.begin(), members_.end());
    if ((prior != self_) && (*min_member == self_)) {
        // do dns stuff
        STDERR_LOG(LOG_DEBUG, (std::cerr << "About to handle DNS." << '\n'));
        std::stringstream ss;
        ss << "echo " << self_.address << ":" << self_.port << " > " << cs425::DNS_PATH;
        int rc = execRemoteCommand(ss.str(), "/dev/null", "");
        if (rc != 0) {
            STDERR_LOG(LOG_ERROR, (std::cerr << "Couldn't exec remote command!\n"));
        }
        STDERR_LOG(LOG_TRACE, (std::cerr << "About to acquire introducer_mutex_..." << '\n'));
        introducer_mutex_.lock();
        STDERR_LOG(LOG_TRACE, (std::cerr << "Acquired introducer_mutex_. Setting is_introducer_ to true." << '\n'));
        is_introducer_ = true;
        STDERR_LOG(LOG_TRACE, (std::cerr << "Unlocking introducer_mutex_..." << '\n'));
        introducer_mutex_.unlock();
    } else {
        STDERR_LOG(LOG_INFO, (std::cerr << "We are not the new introducer; therefore DNS is not our responsibility." << '\n'));
    }

    // Make sure we set the prior introducer
    prior = *min_member; 
}


void Node::HandleJoin(const Member& new_member) {
    STDERR_LOG(LOG_DEBUG, (std::cerr << "Join recieved for member: " << new_member << '\n' ));
    Message outbound_message{MessageType::Introduce, {new_member}};
    std::string serialized_outbound_message = Message::Serialize(outbound_message);

    for (const Member& successor : GetSuccessors()) {
        Socket::TrySendMessage(serialized_outbound_message, port_, successor.address, loss_rate_);
    }
    STDERR_LOG(LOG_TRACE, (std::cerr << "About to acquire mutex_... " << '\n' ));
    mutex_.lock();
    STDERR_LOG(LOG_TRACE, (std::cerr << "Acquired mutex_. " << '\n' ));

    // Because GetSuccessors won't ping the Introducer, introducer manually adds the new
    // node to its own list
    members_.push_back(new_member);
    STDERR_LOG(LOG_DEBUG, (std::cerr << "About to notify MP3..." << '\n'));
    NotifyMP3();
    // std::sort(members_.begin(), members_.end());
    outbound_message = {MessageType::JoinAck, members_};
    STDERR_LOG(LOG_TRACE, (std::cerr << "Releasing mutex_..." << '\n' ));
    mutex_.unlock();

    serialized_outbound_message = Message::Serialize(outbound_message);
    Socket::TrySendMessage(serialized_outbound_message, port_, new_member.address, loss_rate_);

    LogMemberJoinIntroducer(new_member);
}

void Node::HandleJoinAck(const std::vector<Member>& existing_members) {
    STDERR_LOG(LOG_TRACE, (std::cerr << "About to acquire mutex_..." << '\n'));
    mutex_.lock();
    STDERR_LOG(LOG_TRACE, (std::cerr << "Acquired mutex_." << '\n'));
    // New node's membership list is set to introducer's. All new nodes are at back of list
    members_ = existing_members;
    // auto bruh = Member();
    // bruh.id = "bruh";
    // bruh.address = "bruh";
    // bruh.pings_dropped = -1;

    ring_position_ = members_.size() - 1;
    // This self member variable is a bit sloppy, but used in a few spots for a node to reference itself
    self_ = members_.at(ring_position_);
    STDERR_LOG(LOG_DEBUG, (std::cerr << "Received JOINACK. Current ID will be: " << self_ << '\n'));
    NotifyMP3();
    STDERR_LOG(LOG_TRACE, (std::cerr << "Releasing mutex_...." << '\n'));
    mutex_.unlock();

    // Only say you are a member after getting a JoinAck
    is_member_ = true;

    // Do DNS check
    this->HandleDNS();
    LogMemberJoinAck(self_);
}




void Node::HandleIntroduce(const Member& new_member) {
    STDERR_LOG(LOG_TRACE, (std::cerr << "About to acquire mutex_..." << '\n'));
    mutex_.lock();
    STDERR_LOG(LOG_TRACE, (std::cerr << "Acquired mutex_." << '\n'));

    
    auto it = std::find(members_.begin(), members_.end(), new_member);

    // If you have already received the new node, drop the message
    if (it != members_.end()) {
        STDERR_LOG(LOG_TRACE, (std::cerr << "Releasing mutex_...." << '\n'));
        mutex_.unlock();
        return;
    }

    // Otherwise, add the new node to the back of membership list
    members_.push_back(new_member);
    STDERR_LOG(LOG_DEBUG, (std::cerr << "About to notify MP3..." << '\n'));
    NotifyMP3();
    STDERR_LOG(LOG_TRACE, (std::cerr << "Releasing mutex_...." << '\n'));
    mutex_.unlock();

    // Forward to successors
    Message outbound_message{MessageType::Introduce, {new_member}};
    std::string serialized_outbound_message = Message::Serialize(outbound_message);
    for (const Member& successor : GetSuccessors()) {
        Socket::TrySendMessage(serialized_outbound_message, port_, successor.address, loss_rate_);
    }

    LogMemberJoin(new_member);
}

void Node::HandlePing(const std::string& source) {
    Message outbound_message{MessageType::Ack, {self_}};
    std::string serialized_outbound_message = Message::Serialize(outbound_message);
    Socket::TrySendMessage(serialized_outbound_message, port_, source, loss_rate_);
}

void Node::HandleAck(const Member& acking_member) {
    STDERR_LOG(LOG_TRACE, (std::cerr << "About to acquire mutex_..." << '\n'));
    mutex_.lock();
    STDERR_LOG(LOG_TRACE, (std::cerr << "Acquired mutex_." << '\n'));

    auto it = std::find(members_.begin(), members_.end(), acking_member);

    if (it == members_.end()) {
        mutex_.unlock();
        return;
    }

    it->pings_dropped = 0;

    mutex_.unlock();
}

void Node::HandleLeave(const Member& leaving_member) {
    STDERR_LOG(LOG_TRACE, (std::cerr << "About to acquire mutex_..." << '\n'));
    mutex_.lock();
    STDERR_LOG(LOG_TRACE, (std::cerr << "Acquired mutex_." << '\n'));

    // std::sort(members_.begin(), members_.end());

    // Find index of where leaving node is
    int found_index = -1;
    for (size_t index = 0; index < members_.size(); ++index) {
        if (members_[index] == leaving_member) {
            found_index = index;
            break;
        }
    }

    // If you have already removed the leaving node, drop the message
    if (found_index == -1) {
        STDERR_LOG(LOG_TRACE, (std::cerr << "Releasing mutex_...." << '\n'));
        mutex_.unlock();
        return;
    }

    // Otherwise, remove said node
    members_.erase(members_.begin() + found_index);
    STDERR_LOG(LOG_DEBUG, (std::cerr << "About to notify MP3..." << '\n'));
    NotifyMP3();

    // make introducer check here
    this->HandleDNS();

    // Check if erased node was behind you, if so decrement your ring position
    if (ring_position_ > static_cast<unsigned int>(found_index)) {
        --ring_position_;
    }
    STDERR_LOG(LOG_TRACE, (std::cerr << "Releasing mutex_...." << '\n'));
    mutex_.unlock();

    // Forward to successors
    Message outbound_message{MessageType::Leave, {leaving_member}};
    std::string serialized_outbound_message = Message::Serialize(outbound_message);
    for (const Member& successor : GetSuccessors()) {
        Socket::TrySendMessage(serialized_outbound_message, port_, successor.address, loss_rate_);
    }

    LogMemberLeaveForwarded(leaving_member);
}

void Node::LogMemberJoinIntroducer(const Member& member) {
    std::cout << "---------------------------------------------------------------" << std::endl;
    std::cout << "[JOIN] - Time = " << GetElapsedSeconds() << "s" << std::endl;
    std::cout << "Successfully introduced new member to group" << std::endl;
    std::cout << "Added node " << member.id << " to local membership list" << std::endl;
    std::cout << "Forwarding join to successors via INTRODUCE messages" << std::endl;
    PrintMembers();
}

void Node::LogMemberJoinAck(const Member& member) {
    std::cout << "---------------------------------------------------------------" << std::endl;
    std::cout << "[JOIN] - Time = " << GetElapsedSeconds() << "s" << std::endl;
    std::cout << "Successfully added self to group" << std::endl;
    std::cout << "Added node " << member.id << " to local membership list" << std::endl;
    PrintMembers();
}

void Node::LogMemberJoin(const Member& member) {
    std::cout << "---------------------------------------------------------------" << std::endl;
    std::cout << "[JOIN] - Time = " << GetElapsedSeconds() << "s" << std::endl;
    std::cout << "Added node " << member.id << " to local membership list" << std::endl;
    std::cout << "Forwarding join to successors via INTRODUCE messages" << std::endl;
    PrintMembers();
}

void Node::LogMemberLeave(const Member& member) {
    std::cout << "---------------------------------------------------------------" << std::endl;
    std::cout << "[LEAVE] - Time = " << GetElapsedSeconds() << "s" << std::endl;
    std::cout << "Node " << member.id << " requested to leave, removing self from group" << std::endl;
    std::cout << "Forwarding leave to successors via LEAVE messages" << std::endl;
    PrintMembers();
}

void Node::LogMemberLeaveForwarded(const Member& member) {
    std::cout << "---------------------------------------------------------------" << std::endl;
    std::cout << "[LEAVE_FORWARDED] - Time = " << GetElapsedSeconds() << "s" << std::endl;
    std::cout << "Informed node " << member.id << " either left or failed, removing from local membership list" << std::endl;
    std::cout << "Forwarding leave to successors via LEAVE messages" << std::endl;
    PrintMembers();
}

void Node::LogMemberFailure(const Member& member) {
    std::cout << "---------------------------------------------------------------" << std::endl;
    std::cout << "[FAILURE] - Time = " << GetElapsedSeconds() << "s" << std::endl;
    std::cout << "Detected node " << member.id << " as failed, removing from local membership list" << std::endl;
    std::cout << "Forwarding failure to successors via LEAVE messages" << std::endl;
    PrintMembers();
}

void Node::LogMisc(const std::string& tag, const std::string& message) {
    std::cout << "---------------------------------------------------------------" << std::endl;
    std::cout << "[" << tag << "] - Time = " << GetElapsedSeconds() << "s" << ": ";
    std::cout << message << std::endl;
}

void Node::PrintMembers() {
    std::cout << "---------------------------------------------------------------" << std::endl;
    std::cout << "[MEMBERSHIP_LIST] - Time = " << GetElapsedSeconds() << "s" << std::endl;

    if (!is_member_) {
        std::cout << "Not a member of a group" << std::endl;
        return;
    }

    STDERR_LOG(LOG_TRACE, (std::cerr << "About to acquire mutex_..." << '\n'));
    mutex_.lock();
    STDERR_LOG(LOG_TRACE, (std::cerr << "Acquired mutex_." << '\n'));

    std::cout << "Membership list:" << std::endl;
    printf("%30s | %15s | %5s\n", "Id", "Address", "Drops");
    for (const Member& member : members_) {
        printf("%30s | %15s | %5d\n", member.id.c_str(), member.address.c_str(), member.pings_dropped);
    }

    std::cout << std::endl;
    STDERR_LOG(LOG_TRACE, (std::cerr << "Releasing mutex_...." << '\n'));
    mutex_.unlock();
}

void Node::PrintSelf() {
    std::cout << "---------------------------------------------------------------" << std::endl;
    std::cout << "[SELF] - Time = " << GetElapsedSeconds() << "s" << std::endl;
    std::cout << "Self's ID: " << self_.id << std::endl;
}

double Node::GetElapsedSeconds() {
    std::chrono::time_point<std::chrono::steady_clock> current_time = std::chrono::steady_clock::now();
    double milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(current_time - start_time_).count();

    return milliseconds / 1000.0;
}

void Node::DebugPrintMembers() {
    if (!is_member_) {
        std::cout << "Not a member of a group" << std::endl;
        return;
    }

    STDERR_LOG(LOG_TRACE, (std::cerr << "About to acquire mutex_..." << '\n'));
    mutex_.lock();
    STDERR_LOG(LOG_TRACE, (std::cerr << "Acquired mutex_." << '\n'));

    std::vector<Member>::iterator it = std::find(members_.begin(), members_.end(), self_);

    std::cout << "---------------------------------------------------------------" << std::endl;
    std::cout << "Full list:" << std::endl;
    printf("%30s | %15s | %5s\n", "Id", "Address", "Drops");
    unsigned int index = 0;
    for (const Member& member : members_) {
        printf("%30s | %15s | %5d", member.id.c_str(), member.address.c_str(), member.pings_dropped);

        if (index == ring_position_ && it != members_.end()) {
            printf(" <-- rp");
        }

        printf("\n");
        ++index;
    }

    std::cout << "Self + successors:" << std::endl;
    
    // unlikely to trigger
    if (it == members_.end()) {
        std::cout << "(Self not in group)" << std::endl;
    }

    printf("%30s | %15s | %10s\n", "Id", "Address", "Drops");
    if (it != members_.end()) {
        printf("%30s | %15s | %5d (self)\n", it->id.c_str(), it->address.c_str(), it->pings_dropped);
    }
    STDERR_LOG(LOG_TRACE, (std::cerr << "Releasing mutex_...." << '\n'));
    mutex_.unlock();

    for (const Member& member : GetSuccessors()) {
        printf("%30s | %15s | %5d (successor)\n", member.id.c_str(), member.address.c_str(), member.pings_dropped);
    }

    std::cout << std::endl;
}

std::vector<Member> Node::GetSuccessors() {
    std::vector<Member> successors;
    // STDERR_LOG(LOG_TRACE, (std::cerr << "About to acquire mutex_..." << '\n'));
    mutex_.lock();
    // STDERR_LOG(LOG_TRACE, (std::cerr << "Acquired mutex_." << '\n'));

    if (members_.empty()) {
        // STDERR_LOG(LOG_TRACE, (std::cerr << "Releasing mutex_...." << '\n'));
        mutex_.unlock();
        return {};
    }

    if (std::find(members_.begin(), members_.end(), self_) == members_.end()) {
        unsigned int index = 0;
        while (index < cs425::NUM_MONITORS && index < members_.size()) {
            successors.push_back(members_.at(index));
            ++index;
        }
    } else {
        unsigned int offset = 1;
        // Loop until we hit the number of monitors or we arrive at self
        while (offset <= cs425::NUM_MONITORS && (ring_position_ + offset) % members_.size() != ring_position_) {
            unsigned int index = (ring_position_ + offset) % members_.size();
            successors.push_back(members_.at(index));

            ++offset;
        }
    }
    // STDERR_LOG(LOG_TRACE, (std::cerr << "Releasing mutex_...." << '\n'));
    mutex_.unlock();

    return successors;
}

void to_json(json& j, const Member& m) {
    j = json{{"member_id", m.id}, {"address", m.address}, {"pings_dropped", m.pings_dropped}, {"port", m.port}};
}

void from_json(const json& j, Member& m) {
    j.at("member_id").get_to(m.id);
    j.at("address").get_to(m.address);
    j.at("pings_dropped").get_to(m.pings_dropped);
    j.at("port").get_to(m.port);
}
