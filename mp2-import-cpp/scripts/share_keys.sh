#!/bin/bash
# Note: this script won't run out of the box but is here for future reference

# Requires an ed25519 key generated beforehand. Change out usernames as needed.
# Script that shares public/private keys with all others in the cluster
# Adds the private key to the ssh-agent for GitLab push/pulling (key added to repo)
# Copies the private key to /home/shared/.ssh so that other scripts can use the key for ssh access as username

for i in 1 2 3 4 5 6 7 8 9 10
do
  ssh-copy-id chkull2@fa22-cs425-20$(printf %02d $i).cs.illinois.edu
  scp /home/chkull2/.ssh/id_ed25519.pub chkull2@fa22-cs425-20$(printf %02d $i).cs.illinois.edu:/home/chkull2/.ssh/id_ed25519.pub
  scp /home/chkull2/.ssh/id_ed25519 chkull2@fa22-cs425-20$(printf %02d $i).cs.illinois.edu:/home/chkull2/.ssh/id_ed25519
  ssh -i /home/chkull2/.ssh/id_ed25519 -t chkull2@fa22-cs425-20$(printf %02d $i).cs.illinois.edu "
    eval "$(ssh-agent -s)"
    ssh-add /home/chkull2/.ssh/id_ed25519
    mkdir /home/shared/.ssh
    cp /home/chkull2/.ssh/id_ed25519 /home/shared/.ssh
  "
done