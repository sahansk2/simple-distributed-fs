package main

import (
	"flag"
	"fmt"
	"github.com/sirupsen/logrus"
	"net"
	"os"
	"regexp"
	"strconv"
	"sync"
	"waltuh/main/config"
	"waltuh/main/qutil"
)

var onlyCountsFlag = false
var stdoutLockedLineCountVar int // Implicitly locked when stdout lock is obtained

/* query
 *	@brief Goroutine for handling a connection with one logger.
 *		   Sends remote command to logger, then fetches output in chunks.
 *		   Writes to stdout.
 * 	@param hostname: hostname of logger
 *  @param port: port of logger
 *  @cmdToSend: remote command to send to logger
 *  @stdoutLock: mutex for writing to stdout
 *  @queryGroup: WaitGroup for this goroutine
 *  @return: Void
 */
func query(hostname string, port string, cmdToSend string, stdoutLock *sync.Mutex, queryGroup *sync.WaitGroup) {
	// For signaling that this query is done
	defer queryGroup.Done()

	config.QLogger.Debug("Initiating connection with: " + hostname + ":" + port)
	conn, err := net.Dial("tcp", hostname+":"+port)
	if err != nil {
		config.QLogger.Error("Failed to connect to: " + hostname + ":" + port)
		config.QLogger.Error("Error: " + err.Error())
		return
	}

	// Send remote command
	_, err = conn.Write([]byte(cmdToSend))

	chunk := make([]byte, 4096)
	countOutputChunk := make([]byte, 4096)
	countOutputChunkEnd := 0

	stdoutLock.Lock()
	defer stdoutLock.Unlock()

	linecount := 0
	for {
		// Read a chunk of output from socket
		bytesRead, err := conn.Read(chunk)
		config.QLogger.Debug(fmt.Sprintf("Recieved %v bytes from %v:%v", strconv.Itoa(bytesRead), hostname, port))

		if err != nil {
			if tcperr, ok := err.(*net.OpError); ok {
				config.QLogger.Debug("TCP Error: ", tcperr.Error())
			}
			config.QLogger.Debug("Error: ", err.Error())
			config.QLogger.Debug("Closing connection with: " + hostname + ":" + port)
			break
		}
		// Outputting depends on whether we want to output counts or not
		// Intended (print out results and manually calculate counts)
		if !onlyCountsFlag {
			// Print received output
			os.Stdout.Write(chunk[:bytesRead])
			for i := 0; i < bytesRead; i++ {
				if chunk[i] == byte('\n') {
					linecount += 1
				}
			}
		} else {
			config.QLogger.Debug("hello!")
			// Copy new data into countOutputChunk
			copy(countOutputChunk[countOutputChunkEnd:], chunk[:bytesRead])

			// Grep will always output a newline for the end of its result
			// Look for the newline to find out whether we should stop trying to read from the TCP connection
			fullOutput := false
			for i := 0; i < bytesRead; i++ {
				if countOutputChunk[countOutputChunkEnd+i] == byte('\n') {
					fullOutput = true
					break
				}
			}
			// Move byte pointer up
			countOutputChunkEnd += bytesRead
			if !fullOutput {
				config.QLogger.Debug("Read ", bytesRead, " bytes, but couldn't find newline for counts. Continuing byteread loop")
				config.QLogger.Debug("countOutputChunk end: ", countOutputChunkEnd)
				continue
			} else {
				config.QLogger.Debug("found newline!")
				// Read a newline; we're done.
				break
			}
		}
	}
	// Do this parsing only if we specified the count flag
	if onlyCountsFlag {
		// Print to stdout what we got from the logger
		os.Stdout.Write(countOutputChunk[:countOutputChunkEnd])
		stringOut := qutil.TrimLoggerPrefixAndWhitespace(countOutputChunk[:countOutputChunkEnd])
		// Parse the byte stream to a string to an int
		// This ought to work?
		linecount, err = strconv.Atoi(stringOut)
		if err != nil {
			config.QLogger.Error("atoi failed >:(")
			config.QLogger.Error(err)
			return
		}
		config.QLogger.Debug("linecount is: ", linecount)

	}

	stdoutLockedLineCountVar += linecount
}

func main() {
	stdoutLockedLineCountVar = 0

	// Flag for qutil to issue a log command for querying
	var machineILogFlag bool
	flag.BoolVar(&machineILogFlag, "D", false, "[BROKEN] DEMO FLAG: By default, query same name file. Otherwise, query machine.i.log")

	var numberOfLocalInstances int
	flag.IntVar(&numberOfLocalInstances, "U", 0, "Number of localhost loggers to connect to. Ports count up from 6969")

	var commandToExec string
	// Set log level
	var logLevelFlag string
	var portFlag string
	flag.StringVar(&portFlag, "port", "6969", fmt.Sprintf("Port of the running loggers."))
	flag.StringVar(&logLevelFlag, "loglevel", "error", fmt.Sprintf("%s", logrus.AllLevels))
	//flag.BoolVar(&onlyCountsFlag, "c", false, "DEMO FLAG: Flag to set when parsing only line counts.")
	flag.StringVar(&commandToExec, "c", "", "Command to execute.")
	var filterRegexp string
	flag.StringVar(&filterRegexp, "f", ".*", "Regex to match the machines that you wish to query. Runs on all \"HOSTNAME:PORT\" values.")
	flag.Parse()

	if len(commandToExec) == 0 {
		fmt.Fprintln(os.Stderr, "Missing command.")
		fmt.Fprintln(os.Stderr, "Here's an example command that will work:\n")
		fmt.Fprintln(os.Stderr, "\tgo run querier.go -U 1 -c \"echo 'percents%are%gone' | sed 's/%/ /g'\"\n")
		flag.PrintDefaults()
		os.Exit(2)
	}

	config.ConfigureQLogger(logLevelFlag)
	config.QLogger.Debug("Hello!")
	config.QLogger.Debug(fmt.Sprintf("machineILogFlag: %v, numberOfLocalInstances: %v, logLevelFlag: %v", machineILogFlag, numberOfLocalInstances, logLevelFlag))
	ipsToConnectTo := qutil.GetIpsToConnectTo(numberOfLocalInstances)

	// For protecting stdout
	var pLock sync.Mutex
	var queryGroup sync.WaitGroup

	r := regexp.MustCompile(filterRegexp)
	for i, host := range ipsToConnectTo {
		hostname := host.Addr
		port := portFlag

		if r.FindStringIndex(fmt.Sprintf("%s:%s", hostname, port)) != nil {
			cmdToSend := qutil.GenerateTransferrableCommand(machineILogFlag, i, commandToExec)

			// Launch goroutine to query a logger
			queryGroup.Add(1)
			go query(hostname, port, cmdToSend, &pLock, &queryGroup)
		}
	}
	// Wait for all query goroutines to join
	queryGroup.Wait()

	fmt.Fprintf(os.Stderr, "Count of lines: %v\n", stdoutLockedLineCountVar)
}
