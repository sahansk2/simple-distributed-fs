#ifndef SOCKET_HPP
#define SOCKET_HPP

#include <optional>
#include <random>
#include <string>

class Socket {
public:
    static bool TrySendMessage(const std::string& serialized_message, unsigned int port, const std::string& address, double loss_rate);
    static std::optional<Socket> ConstructReceiver(unsigned int port);
    static std::optional<Socket> ConstructSender(unsigned int port, const std::string& host);

    // only allowing moves, not copying
    ~Socket();
    Socket(const Socket& rhs) = delete;
    Socket& operator=(const Socket& rhs) = delete;
    Socket(Socket&& rhs);
    Socket& operator=(Socket&& rhs);

    bool SendMessage(const std::string& message);
    std::pair<std::string, std::string> ReceiveMessage();

private:
    Socket(unsigned int port, const std::string& host, unsigned int file_descriptor);

    void _destroy();

    unsigned int port_;
    std::string host_;
    volatile unsigned int file_descriptor_ = 0;
    // This flag is used to prevent closing a socket twice during a destructor called after a move
    bool valid_file_descriptor_ = true;

    static std::default_random_engine generator_;
    static std::uniform_real_distribution<double> distribution_;
};

#endif