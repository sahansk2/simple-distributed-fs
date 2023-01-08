#ifndef NODE_HPP
#define NODE_HPP

#include "member.hpp"

#include <atomic>
#include <chrono>
#include <mutex>
#include <vector>
#include <string>

class Node {
public:
    Node(int id, bool is_introducer, int ping_rate, int drop_threshold, double loss_rate, unsigned int port);

    void Start();

    std::string HandleUserCommands(std::string input);
    void PingSuccessors();
    void MessageHandler();

    void HandleJoin(const Member& new_member);
    void HandleJoinAck(const std::vector<Member>& existing_members);
    void HandleIntroduce(const Member& new_member);
    void HandlePing(const std::string& source);
    void HandleAck(const Member& acking_member);
    void HandleLeave(const Member& leaving_member);
    void HandleDNS(); 
    void NotifyMP3();
    std::pair<std::string, std::string> QueryDNS();
private:
    void LogMemberJoinIntroducer(const Member& member);
    void LogMemberJoinAck(const Member& member);
    void LogMemberJoin(const Member& member);
    void LogMemberLeave(const Member& member);
    void LogMemberLeaveForwarded(const Member& member);
    void LogMemberFailure(const Member& member);
    void LogMisc(const std::string& tag, const std::string& message);
    void PrintMembers();
    void PrintSelf();
    double GetElapsedSeconds();
    void DebugPrintMembers();
    std::vector<Member> GetSuccessors();
    std::string HandleUserCommandLISTMEM();
    std::string HandleUserCommandLISTSELF();
    std::string HandleUserCommandJOIN();
    std::string HandleUserCommandLEAVE();
    std::string HandleUserCommandQUIT();

    int id_;
    bool is_introducer_;
    const int ping_rate_;
    const int drop_threshold_;
    const double loss_rate_;
    const unsigned int port_;
    std::vector<Member> members_;
    std::atomic_uint ring_position_ = 0;
    std::mutex mutex_;
    std::mutex introducer_mutex_;
    Member self_;
    std::atomic_bool is_member_ = false;

    std::chrono::time_point<std::chrono::steady_clock> start_time_;
};

#endif