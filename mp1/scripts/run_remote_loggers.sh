#!/usr/bin/env bash

cmd=$1
netid=$2

LOGS_DIR=$(pwd)/../logs

function killma()
{
  for i in {1..9}
  do
    ssh "$netid"@fa22-cs425-510"$i".cs.illinois.edu "killall logger" &
    sleep 1
    scp -i ~/.ssh/id_rsa "$netid"@fa22-cs425-510"$i".cs.illinois.edu:/home/"$netid"/logger_${i}_out.log $LOGS_DIR
  done

  ssh "$netid"@fa22-cs425-5110.cs.illinois.edu "killall logger" &
  sleep 1
  scp -i ~/.ssh/id_rsa "$netid"@fa22-cs425-5110.cs.illinois.edu:/home/"$netid"/logger_10_out.log $LOGS_DIR

  exit
}

trap killma INT

if [[ -z "$cmd" || -z "$netid" ]]; then
  echo "Usage: ./run_remote_loggers.sh <cmd> <netid>"
  exit 0
fi

for i in {1..9}
do
  ssh "$netid"@fa22-cs425-510"$i".cs.illinois.edu "${cmd} &> logger_${i}_out.log" &
done

ssh "$netid"@fa22-cs425-5110.cs.illinois.edu "${cmd} &> logger_10_out.log" &

while true;
do
  sleep 1
done
