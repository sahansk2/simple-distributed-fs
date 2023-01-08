package main

import (
	"amogus/api"
	"amogus/mp3util"
	"amogus/schema"
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"strconv"
	"strings"
	"time"
)

/**
 * main
 *	Stdin loop. Reads commands from user and queries mp2/mp3 modules accordingly.
 *	List of commands:
 *		listmem => GET mp2/listmem
 *		listself => GET mp2/listself
 *		join => GET mp2/join
 *		leave => GET mp2/leave
 *		quit => GET mp2/quit
 *		putfile <localfilename> <sdfsfilename>
 *		getfile <sdfsfilename> <localfilename> => POST mp3/get {sdfsfilename: <sdfsfilename, localfilename: <localfilename}
 *		deletefile <sdfsfilename>
 *		getversions <sdfsfilename> <num-versions> <localfilename>
 * 		ls <sdfsfilename>
 *		store
 */
func main() {
	fmt.Fprintf(os.Stderr, "MP3 CLI PID: %v\n", os.Getpid())

	logLevelFlag := flag.String("loglevel", "error", fmt.Sprintf("Logger flags: %s", logrus.AllLevels))
	dumpToFileFlag := flag.Bool("d", false, "Specify whether you would like to dump to a file or not.")

	mp3util.ConfigureLogger("CLI", *logLevelFlag, *dumpToFileFlag)

	var help = func() {
		fmt.Println("Command options:\n",
			"listmem\n",
			"listself\n",
			"join\n",
			"leave\n",
			"quit\n",
			"putfile <localfilename> <sdfsfilename>\n",
			"getfile <sdfsfilename> <localfilename>\n",
			"deletefile <sdfsfilename>\n",
			"getversions <sdfsfilename> <num-versions> <localfilename>\n",
			"ls <sdfsfilename>\n",
			"store\n",
			"help")
	}
	help()

	/*
	 * Infinite loop to read and execute
	 * user commands from stdin.
	 */
	reader := bufio.NewScanner(os.Stdin)
	fmt.Print("Booting up sdfs")
	// "There, now it looks like it's actually *doing* things." - Tejus Kurdukar, 11/5/22 @ 10:48 pm
	for i := 0; i < 6; i++ {
		time.Sleep(time.Second / 2)
		fmt.Print(".")
	}

	fmt.Println("Done!")
	for {
		fmt.Print("> ")
		hastok := reader.Scan()
		if !hastok {
			os.Exit(0)
		}

		cmd := strings.Fields(reader.Text())
		if len(cmd) == 0 {
			continue
		}

		opcode := cmd[0]
		switch opcode {

		case "join", "leave":
			api.IssueMP2Command(opcode)

		case "listself":
			var self schema.Member
			resp, err := api.IssueMP2Command(opcode)
			if err != nil {
				continue
			}

			err = json.NewDecoder(resp.Body).Decode(&self)
			if err != nil {
				mp3util.NodeLogger.Error("Could not decode json. err= ", err)
				continue
			}

			fmt.Println("Self id: ", self)

		case "listmem":
			var membershipList []schema.Member
			resp, err := api.IssueMP2Command(opcode)
			if err != nil {
				continue
			}

			err = json.NewDecoder(resp.Body).Decode(&membershipList)
			if err != nil {
				mp3util.NodeLogger.Error("Could not decode json. err= ", err)
				continue
			}

			fmt.Println("Received member(s): ", membershipList)

		case "getversions":
			if len(cmd) != 4 {
				fmt.Println("Usage: getversions <sdfsfilename> <num-versions> <localfilename>")
				continue
			}
			val, err := strconv.ParseInt(cmd[2], 10, 64)
			if err != nil {
				fmt.Println("Couldn't parse <num-versions>!")
				continue
			}
			args := schema.CliArgs{
				SdfsFileName:  cmd[1],
				NumVersions:   int(val),
				LocalFileName: cmd[3],
				Bruhflag:      false,
			}

			_, err = api.IssueMP3Command(opcode, args)
			if err != nil {
				fmt.Printf("MP3 failed command %v with error: %v\n", opcode, err)
				continue
			}
			fmt.Printf("Command %v executed.\n", opcode)

		case "getversionsbruh":
			if len(cmd) != 4 {
				fmt.Println("Usage: getversionsbruh <sdfsfilename> <num-versions> <localfilename>")
				continue
			}
			val, err := strconv.ParseInt(cmd[2], 10, 64)
			if err != nil {
				fmt.Println("Couldn't parse <num-versions>!")
				continue
			}
			args := schema.CliArgs{
				SdfsFileName:  cmd[1],
				NumVersions:   int(val),
				LocalFileName: cmd[3],
				Bruhflag:      true,
			}

			_, err = api.IssueMP3Command("getversions", args)
			if err != nil {
				fmt.Printf("MP3 failed command %v with error: %v\n", opcode, err)
				continue
			}
			fmt.Printf("Command %v executed.\n", opcode)

		case "getfile":
			if len(cmd) != 3 {
				fmt.Println("Usage: getfile <sdfsfilename> <localfilename>")
				continue
			}

			args := schema.CliArgs{
				SdfsFileName:  cmd[1],
				LocalFileName: cmd[2],
			}

			_, err := api.IssueMP3Command(opcode, args)
			if err != nil {
				fmt.Printf("MP3 failed command %v with error: %v\n", opcode, err)
				continue
			}
			fmt.Printf("Command %v executed.\n", opcode)

		case "putfile":
			if len(cmd) != 3 {
				fmt.Println("Usage: putfile <localfilename> <sdfsfilename>")
				continue
			}

			args := schema.CliArgs{
				LocalFileName: cmd[1],
				SdfsFileName:  cmd[2],
			}
			_, err := api.IssueMP3Command(opcode, args)
			if err != nil {
				fmt.Printf("MP3 failed command %v with error: %v\n", opcode, err)
				continue
			}
			fmt.Printf("Command %v executed.\n", opcode)

		case "deletefile":
			if len(cmd) != 2 {
				fmt.Println("Usage: deletefile <sdfsfilename>")
				continue
			}

			args := schema.CliArgs{
				SdfsFileName: cmd[1],
			}
			_, err := api.IssueMP3Command(opcode, args)
			if err != nil {
				fmt.Printf("MP3 failed command %v with error: %v\n", opcode, err)
				continue
			}
			fmt.Printf("Command %v executed.\n", opcode)

		case "ls":
			if len(cmd) != 2 {
				fmt.Println("Usage: ls <sdfsfilename>")
				continue
			}
			args := schema.CliArgs{
				SdfsFileName: cmd[1],
			}
			_, err := api.IssueMP3Command(opcode, args)
			if err != nil {
				fmt.Printf("MP3 failed command %v with error: %v\n", opcode, err)
				continue
			}
			fmt.Printf("Command %v executed.\n", opcode)

		case "store":
			if len(cmd) != 1 {
				fmt.Println("Usage: store")
				continue
			}
			_, err := api.IssueMP3Command(opcode, schema.CliArgs{})
			if err != nil {
				fmt.Printf("MP3 failed command %v with error: %v\n", opcode, err)
				continue
			}
			fmt.Printf("Command %v executed.\n", opcode)

		case "quit":
			fmt.Println("ok bye")
			os.Exit(0)

		case "help":
			help()

		case "shell":
			// TODO: Show Tejus dakooters
		default:
			fmt.Println("Not yet implemented/invalid cmd")
		}
	}
}
