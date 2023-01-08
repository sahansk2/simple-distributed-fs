#!/bin/bash

# Pulls the latest version of the provided remote located at /home/shared/$LocalName, clones if needed
# Change out the below variables as needed
Remote="git@gitlab.engr.illinois.edu:cs425fa22-chkull2/mp2-group-membership.git"
LocalName="mp2"

for i in 1 2 3 4 5 6 7 8 9 10
do
  ssh -i /home/shared/.ssh/id_ed25519 -t chkull2@fa22-cs425-20$(printf %02d $i).cs.illinois.edu "
    cd /home/shared
    git clone $Remote $LocalName 2> /dev/null || (cd $LocalName; git pull --ff-only)
    cd mp2
    make
  "
done