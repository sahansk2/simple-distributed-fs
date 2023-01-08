#ifndef MEMBER_HPP
#define MEMBER_HPP

#include <string>
#include <iostream>
#include "json.hpp"

using json = nlohmann::json;

struct Member {
    std::string id = "0"; // id + timestamp + address
    std::string address = "0.0.0.0";
    int pings_dropped = 0;
    unsigned int port = 4321;

    bool operator==(const Member& rhs) { return id == rhs.id && address == rhs.address; }
    bool operator!=(const Member& rhs) { return id != rhs.id || address != rhs.address; }
    bool operator<(const Member& rhs) { return id < rhs.id && address < rhs.address; }

    friend std::ostream& operator<<(std::ostream& os, const Member& m) { os << "Member{ id=" << m.id << ", address=" << m.address << ", port=" << m.port << ", pings_dropped=" << m.pings_dropped << "}"; return os;}
};

void to_json(json& j, const Member& m);
void from_json(const json& j, Member& m);

#endif