#!/usr/bin/env bash

file=$1
netid=$2

if [[ -z "$file" || -z "$netid" ]]; then
  echo "Usage: ./deploy_to_vms.sh <file> <netid>"
  exit 0
fi

for i in {1..9}
do
  scp -i ~/.ssh/id_rsa "$file" "$netid"@fa22-cs425-510"$i".cs.illinois.edu:/home/"$netid"
done

scp -i ~/.ssh/id_rsa "$file" "$netid"@fa22-cs425-5110.cs.illinois.edu:/home/"$netid"
