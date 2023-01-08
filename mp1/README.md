# WARNING: This README may be outdated. MP1 no longer 


# MP1: Mass Server Grep and Command

## What

This repository currently contains MP1 code for this class. This repository consists of several applications:
- `logger`: Acts like a server; it will listen to incoming commands.
- `querier`: Acts like a client.

### Instructions for Logger

#### Build

1. Make sure you have your `go` version at least at `go 1.7`.

2. `make`


#### Deploy
First, transfer the built binary `logger` into any directory on the VM containing logs. Then, execute the binary from a working directory containing the logs.

`logger` takes two flags as arguments:
- `-port`: The default port to listen on. Default is `6969`. You may consider changing this.
- `-loglevel`: The logging level of the logger. Default is `error`.

For example, to host the logger on port `1234`, then run:

```sh
./logger -port 1234
```

### Instructions for Querier

#### Build

See the toplevel directory for instructions.

#### Deploy

First, transfer the built binary `querier` into any directory on a VM. Then, execute the binary in no particular directory.


### Querier Flags

Querier only now takes a smaller set of flags.

- `-c`: Command to execute. That command may contain pipes and output redirects, as long as the command can be parsed like an argument to `bash -c`.
- `-f`: Filter the queried VMs by a regex expression.