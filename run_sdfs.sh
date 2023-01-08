#!/usr/bin/env bash


function printHelp() {
   printf "\tUsage: ./run_sdfs.sh --mp2 <mp2 path + args> --mp3 <mp3 path + args> --cli <cli path + args> --env <env path>\n"
   printf "\tIf MP2_BIN, MP3_BIN_MAIN, MP3_BIN_CLI are set, then each set argument will be prepended with the respective variable.\n"
   exit 0
}

function stopBinaries()
{

   echo "Killing processes..." 
   kill "$MP2PID"
   kill "$MP3PID"
   # kill "$CLIPID"

   exit 0
}


# trap stopBinaries INT


# Parse args
# REFERENCE: https://stackoverflow.com/questions/12022592/how-can-i-use-long-options-with-the-bash-getopts-builtin

mp2cmd="";
mp3cmd="";
clicmd="";
for arg in "$@"; do
   shift
   case "$arg" in
      '--mp2')    set -- "$@" '-2'   ;;
      '--mp3')    set -- "$@" '-3'   ;;
      '--cli')    set -- "$@" '-c'   ;;
      '--env')    set -- "$@" '-e'   ;; 
      *)          set -- "$@" "$arg" ;; 
   esac
done


OPTIND=1
while getopts "2:3:c:e:" opt 
do
   case "$opt" in
      '2')  mp2cmd=$OPTARG                               ;;
      '3')  mp3cmd=$OPTARG                               ;;
      'c')  clicmd=$OPTARG                               ;;
      'e')  envfile=$OPTARG                              ;;
      *)  echo "invalid flags passed."
          printHelp                                      ;;
   esac
done

if [[ -n "${envfile}" ]]; then
   printf "Received envfile: ${envfile}. Attempting to source...\n"
   if [[ -f "${envfile}" ]]; then
	   . ${envfile}
   else
	   printf "\tCould not stat envfile!\n"
	   printHelp
   fi
fi

if [[ -z "$mp2cmd" || -z "$mp3cmd" || -z "$clicmd" ]]; then
   printf "args: $@ \n"
   printf "\n"
   printf "Received mp2cmd: ${mp2cmd}\n"
   printf "Received mp3cmd: ${mp3cmd}\n"
   printf "Received clicmd: ${clicmd}\n"
   printHelp 
fi


if [[ -n "$MP2_BIN" ]]; then 
	echo "MP2_BIN env. variable found!: $MP2_BIN"
	mp2cmd="${MP2_BIN} $mp2cmd"
	export MP2_BIN=$MP2_BIN
fi
if [[ -n "$MP3_BIN_MAIN" ]]; then
	echo "MP3_BIN_MAIN env. variable found!: $MP3_BIN_MAIN"
	mp3cmd="${MP3_BIN_MAIN} $mp3cmd"
	export MP3_BIN_MAIN=$MP3_BIN_MAIN
fi
if [[ -n "$MP3_BIN_CLI" ]]; then
	echo "MP3_BIN_CLI env. variable found!: $MP3_BIN_CLI"
	clicmd="${MP3_BIN_CLI} $clicmd"
	export MP3_BIN_CLI=$MP3_BIN_CLI
fi
if [[ -n "$MP1_BIN" ]]; then
	echo "$MP1_BIN env. variable found!: $MP3_BIN_MAIN"
	export MP1_BIN=$MP1_BIN
else
  echo "Didn't find env var MP1_BIN, necessary for DNS-like functionality!"
  exit 1
fi

# Start binaries
# https://unix.stackexchange.com/q/179130
set -m # Enable process control
(
  printf "Starting MP2 binary: ${mp2cmd}\n"
  echo "" > mp2.log
  (
    ${mp2cmd} 1> mp2.log || kill 0
  ) &
  sleep 1
  printf "Starting MP3 binary: ${mp3cmd}\n"
  (
    ${mp3cmd} || kill 0
  ) &
  printf "Starting command line interface: ${clicmd}\n"
  (
    ${clicmd} || kill 0
  )
  kill 0
)

#stopBinaries
