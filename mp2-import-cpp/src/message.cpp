#include "message.hpp"

#include "member.hpp"

#include <string>
#include <vector>

Message::Message(MessageType type, std::vector<Member> members): type_{type}, members_{members} {}

std::string Message::Serialize(const Message& message) {
    std::string out = std::to_string(static_cast<unsigned int>(message.type_)) + "\n";
    for (const Member& member : message.members_) {
        out += member.id + " " + member.address + " " + std::to_string(member.pings_dropped) + "\n";
    }
    return out;
}

Message Message::Deserialize(const std::string& serialized) {
    std::vector<std::string> lines = Message::Split(serialized, '\n');

    int type = std::stoi(lines[0]);
    lines.erase(lines.begin());

    std::vector<Member> members;
    for (const std::string& s : lines) {
        std::vector<std::string> memberParts = Message::Split(s, ' ');

        Member m{memberParts.at(0), memberParts.at(1), std::stoi(memberParts.at(2))};
        members.push_back(m);
    }

    return {static_cast<MessageType>(type), members};
}

std::vector<std::string> Message::Split(const std::string& str, char delimiter) {
    unsigned int last = -1;
    unsigned int curr = -1;
    std::vector<std::string> v;
    do {
        last = curr;
        curr = str.find(delimiter, last + 1);
        if (curr < str.size()) {
            v.push_back(str.substr(last + 1, curr - last - 1));
        }
    } while (curr < str.size());
    if (str[str.size() - 1] != delimiter) {
        v.push_back(str.substr(last + 1, str.size() - last - 1));
    }
    return v;
}

const MessageType& Message::GetType() const { return type_; }

const std::vector<Member>& Message::GetMembers() const { return members_; }