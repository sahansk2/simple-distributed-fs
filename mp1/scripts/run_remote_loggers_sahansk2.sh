#!/usr/bin/env bash

cmd=$1
netid=$2

LOGS_DIR=$(pwd)/../logs

function killma()
{
  echo "Attempting to kill loggers"
  for i in {1..9}
  do
    ssh "$netid"@dist0"$i" "killall logger" &
    
  done

  ssh "$netid"@dist10 "killall logger" &

  exit
}

trap killma INT

if [[ -z "$cmd" || -z "$netid" ]]; then
  echo "Usage: ./run_remote_loggers.sh <cmd> <netid>"
  exit 0
fi

for i in {1..9}
do
  echo "Making logger $i!"
  ssh "$netid"@dist0"$i" "${cmd} 2>&1 | tee logger_${i}_out.log" &
done

echo "Making logger 10!"
ssh "$netid"@dist10 "${cmd} 2>&1 | tee logger_10_out.log" &

while true;
do
  sleep 1
done
