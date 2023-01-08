#!/bin/bash

for i in 1 2 3 4 5 6 7 8 9 10
do
  ssh -i /home/shared/.ssh/id_ed25519 -t chkull2@fa22-cs425-20$(printf %02d $i).cs.illinois.edu "
    cat /home/shared/mp2/outputs/output.txt 
  " > outputs/output_$i.txt
done