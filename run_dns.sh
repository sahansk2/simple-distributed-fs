#!/usr/bin/env bash


function printHelp() {
   printf "\tUsage: $0  --env <env path>\n"
   printf "\t MP1_SERV_BIN must be specified in the environment file. binary-paths.env specifies this for you.\n"
   exit 0
}

# Parse args
# REFERENCE: https://stackoverflow.com/questions/12022592/how-can-i-use-long-options-with-the-bash-getopts-builtin
mp1serv="";
for arg in "$@"; do
   shift
   case "$arg" in
      '--env')        set -- "$@" '-e'   ;;
      *)              set -- "$@" "$arg" ;;
   esac
done


OPTIND=1
while getopts "e:" opt
do
   case "$opt" in
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

if [[ -n "$MP1_SERV_BIN" ]]; then
  echo "MP1_SERV_BIN env. variable found!: $MP1_SERV_BIN"
  mp1servcmd="${MP1_SERV_BIN} $mp1servcmd"
  export MP1_SERV_BIN=$MP1_SERV_BIN
else
  echo "Didn't find MP1_SERV_BIN, necessary for DNS-like functionality (server)!"
  exit 1
fi

exec ${mp1servcmd}
