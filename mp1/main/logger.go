package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/sirupsen/logrus"
	"net"
	"os"
	"os/exec"
	"waltuh/main/config"
	"waltuh/main/lutil"
)

var loggers *logrus.Entry

/* handlema
 *	@brief Goroutine for handling a connection to the qutil.
 *		   Executes grep on qutil's behalf, and sends output back
 *		   with logging information.
 * 	@param connection: socket abstraction for handling connection to qutil
 *  @return: Void
 */
func handlema(connection net.Conn, port string) {
	loggers.Debug("Entered connection handler")

	// Subroutine for closing connection -- deferred until end of handler
	defer (func(conn net.Conn) {
		loggers.Debug("Closing connection...")
		err := connection.Close()
		if err != nil {
			loggers.Error("conn close fail: ", err)
		}
	})(connection)

	// Create tmp file for storing grep output
	// REFERENCE: https://pkg.go.dev/io/ioutil#example-TempFile
	tmpfile, err := os.CreateTemp("", "example")
	if err != nil {
		loggers.Error("tempma: ", err.Error())
		return
	}
	if config.DeleteMaFile {
		// Delete tmp file at end of handler
		defer os.Remove(tmpfile.Name())
	}

	// Receive grep command from qutil
	grepmaCmdBuf := make([]byte, config.ArgMax)
	cmdLen, err := connection.Read(grepmaCmdBuf)
	loggers.Debug("cmdlen:", cmdLen)
	loggers.Debug("grepmaCmdBuf[:cmdLen]:", string(grepmaCmdBuf[:cmdLen]))
	if err != nil {
		loggers.Error("readma:", err.Error())
		return
	}

	// Invoke grep
	err = callGrep(grepmaCmdBuf, cmdLen, port, tmpfile)
	if err != nil {
		loggers.Error("callGrep failed: ", err.Error())
	}

	// Load grep output from disk and stream to output
	tmpfile, _ = os.Open(tmpfile.Name())
	tmpfileReader := bufio.NewReader(tmpfile)
	nbytes, err := tmpfileReader.WriteTo(connection)
	if err != nil {
		loggers.Warn("Error writing tmpfile to connection: ", err.Error())
		loggers.Warn("nbytes written: ", nbytes)
		return
	}
	loggers.Debug("Finished writing tmpfile")
}

/* callGrep
 *	@brief Parses grep command from qutil and invokes grep. Grep output is written to
 *  	   disk in /tmp. Additionally, 'sed' is used to attach information to grep output,
 *		   i.e hostname, port, and path to the log file.
 * 	@param grepmaCmdBuf: received buffer from qutil, containing full command
 *	@param cmdLen: length of received buffer from qutil
 *	@param tmpfile: tmp file used to store grep output.
 *  @return: Void
 */
func callGrep(grepmaCmdBuf []byte, cmdLen int, port string, tmpfile *os.File) error {
	// Extract grep executable, args from received buffer
	grepmaCmd := string(grepmaCmdBuf[:cmdLen])
	//grepmaCmdArray, _ := shlex.Split(grepmaCmd)
	//grepmaExecutable := grepmaCmdArray[0]
	//grepmaArgs := grepmaCmdArray[1:]
	//
	//loggers.Debug("exec: ", grepmaExecutable)
	//loggers.Debug("args: ", grepmaArgs)
	loggers.Debug("tmpfilema:", tmpfile.Name())

	//logfilename := "<None>"
	//if len(grepmaArgs) > 0 {
	//	We do this because when Grep reads from STDIN, it does not know the filename :(
	//logfilename = grepmaArgs[len(grepmaArgs)-1]
	//}
	// Set up grep command, sed command
	// We use sed to add further information to grep output; i.e hostname, port, path to file

	grepcmd := exec.Command("bash", "-c", grepmaCmd)
	loggers.Debug("final grep command:", grepcmd)
	hostname, _ := os.Hostname()
	sedcmd := exec.Command("sed",
		"-i",
		fmt.Sprintf("s,^,%s,g",
			lutil.GetVMPrefix(hostname, port)),
		tmpfile.Name())

	grepcmd.Stdout = tmpfile
	grepcmd.Stderr = tmpfile
	tmpfile_reader := bufio.NewReader(tmpfile)

	err := grepcmd.Run()
	if err != nil {
		// If grep doesn't find anything, its exit code is 1
		loggers.Error("cmd error")
		tmpfile_reader.WriteTo(loggers.Writer())
		// return err // This is dog, don't use this. Otherwise, sed will never run.
	}
	err = sedcmd.Run()
	if err != nil {
		loggers.Error("sed error")
		return err
	}
	return nil
}

func main() {

	// Parse flags
	portFlag := flag.String("port", "6969", "Port used for listening for connections from qutil.")
	// PanicLevel,     FatalLevel,     ErrorLevel,     WarnLevel,     InfoLevel,     DebugLevel,     TraceLevel
	logLevelFlag := flag.String("loglevel", "error", fmt.Sprintf("Logger flags: %s", logrus.AllLevels))
	flag.Parse()
	port := *portFlag
	logLevel, err := logrus.ParseLevel(*logLevelFlag)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}

	hostname, _ := os.Hostname()
	// Configure global logger
	loggers = logrus.WithFields(logrus.Fields{
		"port":     port,
		"hostname": hostname,
	})
	logrus.SetLevel(logLevel)
	// Start listening on specified port
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		loggers.Fatal("Error tcp listen:", err.Error())
	}

	loggers.Debug("listening...")
	// Accept() loop
	for {
		conn, err := ln.Accept()
		if err != nil {
			loggers.Error("Logger accept() error: ", err.Error())
		}
		go handlema(conn, port)
	}
}
