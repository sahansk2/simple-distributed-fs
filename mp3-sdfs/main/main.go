package main

import (
	"amogus"
	"amogus/api"
	"amogus/mp3util"
	"flag"
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"time"
)

/**
 * sendJoin
 *	Issue JOIN to MP2
 */
func sendJoin() error {

	/* Give some time for MP2 to bootup */
	time.Sleep(1 * time.Second)

	/* Issue JOIN to MP2 */
	_, err := api.IssueMP2Command("join")
	if err != nil {
		return err
	}

	return nil
}

/* main
 *  Creates master, replica objects. Each of these objects are responsible
 *  for different functionalities in MP3:
 * 		Master selects quorums and ensures total ordering, e.g ordered file writes.
 *			If this node is the current master, the master GRPC server will run. Otherwise, it will
 *			be inactive.
 *  	Replica handles file replication and receiving writes/reads from client
 * 		Client issues the central commands: put, get, etc to the current master.
 *			Clients are created on a per-request basis. See api.go.
 *  Main also starts the http server for communicating between cli, mp2 and mp3 binaries,
 *	and issues a JOIN to mp2.
 *  	Note: Upon receipt of a join ack, mp2 will notify mp3 - then the membership list can be
 *  	initialized via the membershipUpdateLoop in api.go
 */
func main() {
	fmt.Fprintf(os.Stderr, "MP3 Main PID: %v\n", os.Getpid())

	logLevelFlag := flag.String("loglevel", "error", fmt.Sprintf("Logger flags: %s", logrus.AllLevels))
	dumpToFileFlag := flag.Bool("d", false, "Specify whether you would like to dump to a file or not.")
	flag.Parse()

	hostname, _ := os.Hostname()
	mp3util.ConfigureLogger(hostname, *logLevelFlag, *dumpToFileFlag)

	master := amogus.NewMasterGRPCService()
	replica := amogus.NewReplicaGRPCService()
	//replica := amogus.Replica() 		// Replica not real rn

	done := make(chan bool)
	go api.RunAPI(master, replica, done)
	replica.Run()
	err := sendJoin()
	if err != nil {
		mp3util.NodeLogger.Fatal("Failed to send join to mp2: ", err)
	}

	for {
		select {
		case <-done:
			return
		}
	}
}
