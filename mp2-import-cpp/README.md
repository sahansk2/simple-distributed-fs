# WARNING: This README may be outdated. Namely, MP2 no longer spawns a CLI, and instead runs on a microservice architecture.

# CS 425 Fall 2022 MP2 - Distributed Group Membership

Authors: Group 20 - Christopher Kull (chkull2) and Kyle McNamara (kylegm3)

This repository contains 1) our implementation of the distributed group membership, 2) our report, see `mp2_report.pdf`, and 3) this README which serves as an explanation for how our codebase is set up and how to run our code.

## Running a Node

In order to start up a node, run these commands from the mp2 directory:
```
make
./bin/exec <machine_id> <is_introducer>
OR: ./bin/exec <machine_id> <is_introducer> <ping_rate> <drop_threshold> <loss_rate>
```
Once a node starts up, you can input the following commands:
- LIST_MEM = print this node's member list
- LIST_SELF = print this node's id
- JOIN = join the group
- LEAVE = leave the group
- WAIT = wait 1 second, used for manual tests
- STOP = kill this node

## Using MP1 for logging

In order to start up a node on machine i, run these commands from the mp2 directory:
```
make
./bin/exec <machine_id> <is_introducer> > /home/shared/logs/mp2/machine.i.log
OR: ./bin/exec <machine_id> <is_introducer> <ping_rate> <drop_threshold> <loss_rate> > /home/shared/logs/mp2/machine.i.log
```

Once again, you can input the same commands LIST_MEM/LIST_SELF/JOIN/LEAVE/WAIT/STOP,
However now the output will be sent to a log file located at /home/shared/logs/mp2/machine.i.log for machine i.
Follow the MP1 instructions here https://gitlab.engr.illinois.edu/cs425fa22-chkull2/cs425fa22-mp1/-/blob/main/README.md to start up the MP1 servers/client.
Once you do that, you will be able to execute greps in the log files located at /home/shared/logs/mp2/machine.i.log for machine i.
(If there is any confusion on grepping for logs, use the MP1 readme in the link)

Tags to grep for:
- [JOIN] - used when a node joins the group
- [LEAVE] - used when a node leaves the group
- [LEAVE_FORWARDED] - used when a node tells another node about a failure or leave that occured
- [FAILURE] - used when we detect that a node has failed
- [MEMBERSHIP_LIST] - used when a machine prints its membership list
- [SELF] - used when a machine prints its own id
- [INFO] - used for miscellaneous informational logs
- [ERROR] - used for errors such as trying to join when the node is already joined

All logs have a time stamp attached so you can track how long operations take.

## Running the Manual Tests

In each inputs/input_i.txt file, the user can input a list of commands to run on machine i, using WAIT to time commands across machines.
(See the example currently in the inputs/ folder in this repo)
Next run
```
sh scripts/run_machine_on_all.sh
```
Once it finishes running through each command in each inputs/input_i.txt file, run
```
sh scripts/get_output_files.sh
```
to fill each outputs/output_i.txt with the logging information for that machine during the run.
Now you can examine or grep each outputs/output_i.txt and confirm that the logs are correct