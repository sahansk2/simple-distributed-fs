#!/bin/bash

# first make introducer
for i in 1
do
  ssh -i /home/shared/.ssh/id_ed25519 -t chkull2@fa22-cs425-20$(printf %02d $i).cs.illinois.edu "
    cd /home/shared/mp2
    ./bin/exec 1000$i true < inputs/input_$i.txt > outputs/output.txt
  "  &
done

# then do rest NOTE make sure you're first instruction is WAIT so introducer can get up and running
for i in 2 3 4 5 6 7 8 9 10
do
  ssh -i /home/shared/.ssh/id_ed25519 -t chkull2@fa22-cs425-20$(printf %02d $i).cs.illinois.edu "
    cd /home/shared/mp2
    ./bin/exec 1000$i false < inputs/input_$i.txt > outputs/output.txt
  " &
done