#include "socket.hpp"
#include "logging.hpp"
#include <iostream>
#include <optional>
#include <random>
#include <string>

#include <arpa/inet.h>
#include <netdb.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

bool Socket::TrySendMessage(const std::string& serialized_message, unsigned int port, const std::string& address, double loss_rate) {
    double random_val = distribution_(generator_);
    if (random_val < loss_rate) {
        std::cout << "send dropped" << std::endl;
        return false;
    }
    
    std::optional<Socket> opt_socket = Socket::ConstructSender(port, address);
    if (opt_socket.has_value()) {
        return (*opt_socket).SendMessage(serialized_message);
    }

    return false;
}

std::optional<Socket> Socket::ConstructReceiver(unsigned int port) {
    const char* port_str = std::to_string(port).c_str();

    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET; // IPv4
    hints.ai_socktype = SOCK_DGRAM; // UDP socket
    hints.ai_flags = AI_PASSIVE; // use own IP address

    struct addrinfo* info;
    if (getaddrinfo(NULL, port_str, &hints, &info) != 0) {
        return std::nullopt;
    }

    int file_descriptor = socket(info->ai_family, info->ai_socktype, 0);
    if (file_descriptor == -1) {
        return std::nullopt;
    }

    // allow for reuse of address
    int yes = 1;
    if (setsockopt(file_descriptor, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) {
        return std::nullopt;
    }

    if (bind(file_descriptor, info->ai_addr, info->ai_addrlen) == -1) {
        close(file_descriptor);
        return std::nullopt;
    }

    freeaddrinfo(info);

    return Socket(port, "", file_descriptor);
}

std::optional<Socket> Socket::ConstructSender(unsigned int port, const std::string& host) {
    const char* port_str = std::to_string(port).c_str();
    const char* host_str = host.c_str();

    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET; // IPv4
    hints.ai_socktype = SOCK_DGRAM; // stream socket

    struct addrinfo* info;
    if (getaddrinfo(host_str, port_str, &hints, &info) != 0) {
        std::cerr << "ERROR: Couldn't getaddrinfo!\n";
        return std::nullopt;
    }

    int file_descriptor = socket(info->ai_family, info->ai_socktype, 0);
    if (file_descriptor == -1) {
        std::cerr << "ERROR: Failed to create socket fd\n";
        return std::nullopt;
    }

    freeaddrinfo(info);

    return Socket(port, host, file_descriptor);
}

Socket::~Socket() { _destroy(); }

Socket::Socket(Socket&& rhs): port_{rhs.port_}, host_{rhs.host_}, file_descriptor_{rhs.file_descriptor_} {
    rhs.port_ = 0;
    rhs.host_ = "";
    rhs.file_descriptor_ = 0;
    rhs.valid_file_descriptor_ = false;
}

Socket& Socket::operator=(Socket&& rhs) {
    // check for self-assignment
    if (this != &rhs) {
        _destroy();
        *this = Socket(std::move(rhs));
    }

    return *this;
}

bool Socket::SendMessage(const std::string& message) {
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET; // IPv4
    hints.ai_socktype = SOCK_STREAM; // stream socket

    struct addrinfo* info;
    if (getaddrinfo(host_.c_str(), std::to_string(port_).c_str(), &hints, &info) != 0) {
        perror("talker: getaddrinfo");
        return false;
    }

    if (sendto(file_descriptor_, message.c_str(), message.size(), 0, info->ai_addr, info->ai_addrlen) == -1) {
        perror("talker: sendto");
        return false;
    }

    freeaddrinfo(info);

    return true;
}

std::pair<std::string, std::string> Socket::ReceiveMessage() {
    struct sockaddr_storage sender_addr;
    socklen_t addr_len = sizeof(struct sockaddr_storage);
    char buf[1024];
    int received = recvfrom(file_descriptor_, buf, 1023, 0, (struct sockaddr*) &sender_addr, &addr_len);
    if (received == -1) {
        perror("recvfrom");
        return {"", ""};
    }

    buf[received] = '\0';

    // TODO: cleanup
    struct sockaddr_in* test = (struct sockaddr_in*) &sender_addr;
    char s[INET_ADDRSTRLEN];
    inet_ntop(sender_addr.ss_family, &(test->sin_addr), s, sizeof s);

    return {std::string(buf), s};
}

Socket::Socket(unsigned int port, const std::string& host, unsigned int file_descriptor):
    port_{port}, host_{host}, file_descriptor_{file_descriptor} {}

void Socket::_destroy() {
    // ensure that we do not double close() a socket
    if (valid_file_descriptor_) {
        close(file_descriptor_);
    }
}

std::default_random_engine Socket::generator_{static_cast<long unsigned int>(time(0))};

std::uniform_real_distribution<double> Socket::distribution_{0.0, 1.0};