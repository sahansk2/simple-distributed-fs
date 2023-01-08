#ifndef MESSAGE_HPP
#define MESSAGE_HPP

#include "member.hpp"

#include <string>
#include <vector>

enum class MessageType {
    Join = 1,
    JoinAck = 2,
    Introduce = 3,
    Ping = 4,
    Ack = 5,
    Leave = 6
};

class Message {
public:
    Message(MessageType type, std::vector<Member> members);

    static std::string Serialize(const Message& message);
    static Message Deserialize(const std::string& serialized_message);

    const MessageType& GetType() const;
    const std::vector<Member>& GetMembers() const;

private:
    static std::vector<std::string> Split(const std::string& str, char delimiter);

    MessageType type_;
    std::vector<Member> members_;
};

#endif