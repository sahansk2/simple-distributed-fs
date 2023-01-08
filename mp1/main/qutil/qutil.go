package qutil

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"waltuh/main/config"
)

func GetIpsToConnectTo(numberOfLocalInstances int) []config.Host {
	var ipsToConnectTo []config.Host
	// Figure out whether we're connecting to localhost or not
	if numberOfLocalInstances > 0 {
		ipsToConnectTo = make([]config.Host, numberOfLocalInstances)
		for i, _ := range ipsToConnectTo {
			ipsToConnectTo[i].Port = strconv.Itoa(6969 + i)
			ipsToConnectTo[i].Addr = "localhost"
		}
	} else {
		ipsToConnectTo = config.VMIps
	}
	return ipsToConnectTo
}

func GenerateTransferrableCommand(machineILogFlag bool, nthMachine int, cmd string) string {
	// Contains the remote command
	if machineILogFlag {
		logName := fmt.Sprintf("machine.%v.log", nthMachine+1)
		// There's no way we're ACTUALLY using this machineILogFlag.
		cmd = fmt.Sprintf("%s %s", cmd, logName)
	}

	cmdToSend := cmd
	config.QLogger.Debug("cmdToSend: ", cmdToSend)
	return cmdToSend
}

func TrimLoggerPrefixAndWhitespace(inputBytes []byte) string {
	inputString := string(inputBytes)
	regex := regexp.MustCompile("^.+:")
	return strings.TrimSpace(regex.ReplaceAllString(inputString, ""))
}
