#!/usr/bin/env bash

trap 'echo "Killing local loggers..."; exit' INT

MAIN=$(pwd)/../main
$MAIN/logger -port 6968 -loglevel debug 2>&1 | tee $MAIN/../logs/logger_1.log &
$MAIN/logger -port 6969 -loglevel debug 2>&1 | tee $MAIN/../logs/logger_2.log &
$MAIN/logger -port 6970 -loglevel debug 2>&1 | tee $MAIN/../logs/logger_3.log &
$MAIN/logger -port 6971 -loglevel debug 2>&1 | tee $MAIN/../logs/logger_4.log &
$MAIN/logger -port 6972 -loglevel debug 2>&1 | tee $MAIN/../logs/logger_5.log &


while true;
do
  sleep 1
done
