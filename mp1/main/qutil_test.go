package main

import (
	"fmt"
	"testing"
	"waltuh/main/config"
	"waltuh/main/lutil"
	"waltuh/main/qutil"
)

func TestGetIpsToConnectToZeroInstances(t *testing.T) {
	hosts := qutil.GetIpsToConnectTo(0)

	// Make sure it's equal
	fail := false
	for i, host := range hosts {
		if config.VMIps[i] != host {
			fail = true
		}
	}

	if fail {
		t.Error("Hosts should be equal to VM IPs")
	}
}

func TestGetIpsToConnectToZero(t *testing.T) {
	hosts := qutil.GetIpsToConnectTo(10)

	for _, host := range hosts {
		if host.Addr != "localhost" {
			t.Errorf("All hosts should be localhost!")
		}
	}
}

func TestTrimLoggerPrefixWithPersonalComputerHostname1(t *testing.T) {
	hostname := "LAPTOP-HBEMDKAB"
	port := "6969"

	prefix := lutil.GetVMPrefix(hostname, port)
	line := "24"
	loggedLine := []byte(fmt.Sprintf("%s%s\n", prefix, line))

	actual := qutil.TrimLoggerPrefixAndWhitespace(loggedLine)
	if line != actual {
		t.Errorf("Expected <%v>, got <%v>", line, actual)
	}
}

func TestTrimLoggerPrefixWithPersonalComputerHostname2(t *testing.T) {
	hostname := "DESKTOP-FLCDK0O"
	port := "6969"
	logfilename := "machine.1.log"
	prefix := lutil.GetVMPrefix(hostname, port, logfilename)
	line := "24"
	loggedLine := []byte(fmt.Sprintf("%s%s\n", prefix, line))

	actual := qutil.TrimLoggerPrefixAndWhitespace(loggedLine)
	if line != actual {
		t.Errorf("Expected <%v>, got <%v>", line, actual)
	}
}

func TestTrimLoggerPrefixWithVMHostname(t *testing.T) {
	hostname := "fa22-cs425-5101.cs.illinois.edu"
	port := "6969"
	logfilename := "machine.1.log"
	prefix := lutil.GetVMPrefix(hostname, port, logfilename)
	line := "24"
	loggedLine := []byte(fmt.Sprintf("%s%s\n", prefix, line))

	actual := qutil.TrimLoggerPrefixAndWhitespace(loggedLine)
	if line != actual {
		t.Errorf("Expected <%v>, got <%v>", line, actual)
	}
}
