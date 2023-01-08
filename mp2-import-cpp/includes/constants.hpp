#include <string>

/**
  For constants that should be used throughout the MP
*/
namespace cs425 {
  /**
    agreed port between client and server
  */
  const unsigned int NUM_MONITORS = 3;
  const std::string DNS_ADDR = "fa22-cs425-5101.cs.illinois.edu";
  const std::string DNS_PATH = "./realdns.txt";
  const size_t SERVE_PORT = 7777;
  const size_t HOSTNAME_LEN = 255;
  const size_t MP3_PORT = 7778;
}
