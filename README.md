# MP3: Simple Distributed File System
## Sahan Kumarasinghe & Tejus Kurdukar

This project implements MP3, which is an implementation of a simple distributed file system tolerating up to three simultaneous failures within 20 seconds. This distributed file system also stores the five most recent versions of files. Our implementation was chosen as the [recommended Golang implementation](https://courses.engr.illinois.edu/cs425/fa2022/assignments.html) out of 25 other Golang implementations in ECE 428's Fall 2022 semester. Our full project utilizes three different binaries, each communicating to each other via TCP, HTTP, and gRPC, and our design features load balancing via hashing, implicit leader elections, compression over TCP ports, and active replication for failure tolerance and bandwidth. For more details, please read our [report](./cs425-tkurdu2-sahansk2-mp3-report.pdf).

## Dependencies

In order to run this, you will need to have installed:

* `go` version 1.17.3 or greater.
* `g++` version 8.5.0 or greater.

## Installation

To build,, run in this directory (mp3)

```
make -j
```


## Quick Start Running

For those on short on time, here is a working command that will correctly run the application. Recommended flags are already in the example command.

1. Modify the variable `DNS_ADDR` in `mp2-import-cpp/includes/constants.hpp` to the hostname of your desired DNS VM, and rebuild (via `make -j`)
2. Run the DNS process on that VM by running:

```
./run_dns.sh --env ./binary-paths.env
```

3. Follow the below instructions for starting the MP3 project.

For the first replica to initialize the DNS with ID=4 (note the `--mp2` argument), run the below command. (ID=4 is an arbitrary ID, it could be any numeric value ID.)

```
./run_sdfs.sh --mp2 "4 true 4321 ERROR" --mp3 "-loglevel ERROR" --cli "-loglevel ERROR" --env ./binary-paths.env
```

For an additional replica to join the *existing* existing cluster with ID=5 (note again the `--mp2` argment), run the below command.
```
./run_sdfs.sh --mp2 "5 false 4321 ERROR" --mp3 "-loglevel INFO" --cli "-loglevel ERROR" --env ./binary-paths.env
```

Repeat the above step for every additional replica. 

See below for detailed instructions on invocation.

## Flag Information


As can be seen, it is necessary to pass the arguments for each binary as a string to `run_sdfs.sh`.

### `--mp2 "<machine_id> <inits_dns> <UDP port> <loglevel>"`

* `<machine_id>`: The ID of this machine, e.g. `1`,`2`,`3`.
* `<inits_dns>`: Whether this process should initialize the DNS entry to themselves or not.
* `<UDP Port>`: Port for UDP communication.
* `<loglevel>`: Logging level for MP2. Set to one of `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE`.

### `--mp3 "-loglevel <loglevel>"`

* `<loglevel>`: Logging level for MP3. Set to one of `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE`. **IMPORTANT**: User feedback is not visible if `-loglevel ERROR` or `-loglevel WARN` is set.


## Credits/Libraries Imported into Codebase

* Recommended C++ MP2 Solution:
    - Christopher Kull (chkull2) 
    - Kyle McNamara (kylegm3)
* MP2 Modifications (for same-process-group IPC)
    - HTTP Library: https://github.com/yhirose/cpp-httplib. 
    - JSON Library: https://github.com/nlohmann/json/
