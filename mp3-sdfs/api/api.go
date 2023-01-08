package api

import (
	"amogus"
	"amogus/config"
	"amogus/mp3util"
	"amogus/schema"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

/**
 * IssueMP2Command
 *	Issue GET request to mp2 module, for given command.
 *	@param cmd - one of "join", "leave", "listmem", "listself", "quit"
 *	@return resp - http response from mp2 module
 */
func IssueMP2Command(cmd string) (*http.Response, error) {
	mp3util.NodeLogger.Debug("Issuing command to mp2: ", cmd)
	resp, err := http.Get(fmt.Sprintf("http://localhost:%s/mp2/%s", config.MP2_PORT, cmd))
	if err != nil {
		mp3util.NodeLogger.Error("Could not communicate with mp2: ", err)
		return nil, err
	}
	mp3util.NodeLogger.Debug("Got response code: ", resp.StatusCode, " for cmd: ", cmd)
	return resp, nil
}

/**
 * IssueMP3Command
 *	Issue POST request to mp3 module, for given command.
 *	@param opcode - one of "getlist", "putfile", "deletefile", "ls", "store"
 *	@return resp - http response from mp3 module
 */
func IssueMP3Command(opcode string, args schema.CliArgs) (*http.Response, error) {
	jsonArgs, err := json.Marshal(args)
	if err != nil {
		fmt.Println("ERROR: Could not parse file args into JSON: err=", err)
		return nil, err
	}

	resp, err := http.Post(fmt.Sprintf("http://localhost:%s/mp3/%s", config.MP3_PORT, opcode),
		"MP3_JSON", bytes.NewBuffer(jsonArgs))

	if err != nil {
		mp3util.NodeLogger.Error("Could not communicate with mp3: err=", err)
		return nil, err
	}

	mp3util.NodeLogger.Debug("Got response: ", resp.StatusCode)
	if resp.StatusCode != 200 {
		return nil, errors.New("Failed to execute command")
	}

	return resp, nil
}

/**
 * parseJSON
 *	Decode JSON input file arguments, received from command line interface,
 *	into a CliArgs struct. The CliArgs struct contains:
 *		LocalFileName, SDFSFileName
 *	One or more fields may be left empty, depending on the command (e.g 'store'
 *	doesn't need these to be filled).
 *
 *	@param request - JSON to decode
 *	@return args - CliArgs struct
 */
func parseJSON(request io.Reader) (schema.CliArgs, error) {
	var args schema.CliArgs
	err := json.NewDecoder(request).Decode(&args)
	if err != nil {
		return args, errors.New(fmt.Sprintf("Failed to parse JSON: %v", request))
	}

	return args, nil
}

type clientCallback func(args schema.CliArgs) error

/**
 * clientHandler
 *	A boilerplate client handler for issuing a command.
 *	Accepted commands: getfile, putfile, ls, deletefile, getversions, store.
 *	Yea I like higher order functions
 *	@param w - writer for writing http response back to caller (command line interface)
 *	@param r - request from caller (command line interface)
 *	@param cb - client callback
 */
func clientHandler(w http.ResponseWriter, r *http.Request, cb clientCallback) error {
	/* Parse JSON */
	args, err := parseJSON(r.Body)
	if err != nil {
		w.WriteHeader(500)
		return err
	}

	/* Issue command to client */
	err = cb(args)
	if err != nil {
		w.WriteHeader(404)
		return err
	}

	return nil
}

/**
 * GetMembershipChanges
 *	Issue LISTMEM to MP2, LISTSELF to MP2.
 *	Update the global membership list/self id known to mp3.
 */
func GetMembershipChanges() error {
	memList := &schema.MemList
	memList.Mtx.Lock()
	defer memList.Mtx.Unlock()

	/* Get membership list from MP2 using listmem command */
	resp, err := IssueMP2Command("listmem")
	if err != nil {
		return err
	}

	err = json.NewDecoder(resp.Body).Decode(&memList.List)
	if err != nil {
		return err
	}
	mp3util.NodeLogger.Info("Received member(s): ", memList.List)

	/* Get self ID from MP2 using listself command */
	resp, err = IssueMP2Command("listself")
	if err != nil {
		return err
	}

	err = json.NewDecoder(resp.Body).Decode(&memList.SelfNode)
	if err != nil {
		return err
	}
	mp3util.NodeLogger.Info("Received self ID: ", memList.SelfNode.Member_Id)

	return nil
}

/**
 * membershipUpdateLoop
 *	Listens to channel from mp2 for incoming notifications. MP2 pings mp3
 * 	on joins, leaves, and failures. The HTTP handler pushes a notification on the mp2chan
 *	read by this loop.
 * 	This function deals with churn by waiting CHURN_TIMOUT_MS before
 *	actually issuing a membership list changes.
 *
 *	@param master - for invoking master.MembershipListChanged(), to update its state
 *	@param mp2chan - channel for membership list change notifications
 *	@param done - done channel
 */
func membershipUpdateLoop(master *amogus.MasterGRPCService, replica *amogus.ReplicaService, mp2chan chan bool, done chan bool) {
	dur := time.Duration(config.CHURN_TIMEOUT_MS) * time.Millisecond
	t := time.NewTimer(dur)
	doneflag := false
	for !doneflag {
		select {
		case <-t.C:
			err := GetMembershipChanges()
			t.Stop() // Avoid weird edge cases
			if err != nil {
				mp3util.NodeLogger.Error("Failed to update membership list: ", err)
				continue
			}

			err = master.MembershipListChanged()
			if err != nil {
				mp3util.NodeLogger.Warn("Master failed to update state upon membership list change: ", err)
				continue
			}

			var startTime time.Time
			var endTime time.Time
			if config.COLLECT_STATS {
				startTime = time.Now()
			}
			err = replica.Replicate()
			if config.COLLECT_STATS {
				endTime = time.Now()
				delta := endTime.Sub(startTime)
				mp3util.NodeLogger.Infof("Replicate took %v usec to complete\n", delta.Microseconds())
			}

			if err != nil {
				mp3util.NodeLogger.Warn("Active replication failed: ", err)
				continue
			}

		case <-mp2chan:
			t = time.NewTimer(dur) // Tick again
		case <-done:
			doneflag = true
		}
	}
}

/**
 * RunAPI
 *	Establishes the HTTP API interface for interacting with MP2 and command line interface
 *	binaries. These are other binaries local on the machine - don't confuse these with
 * 	other nodes. Note that this function blocks, listening for incoming requests.
 *
 *  Below are a series of HTTP listener/callbacks - action is taken upon receiving
 *	commands to modify state within MP3. Usually, these involve creating a new
 *	client object, for issuing a command such as "getfile", "putfile", etc to master.
 * 	Notably, MP2 will ping MP3 via the /mp3/mp2notify handler for membership list changes.
 *
 *  We are almost sorry for this architecture. We suffer from a rare addiction: microservices.
 *	@param master - pointer to master object
 *	@param replica - pointer to replica object
 */
func RunAPI(master *amogus.MasterGRPCService, replica *amogus.ReplicaService, done chan bool) {

	/* Start MP2 notification channel. See membershipUpdateLoop */
	mp2chan := make(chan bool)
	go membershipUpdateLoop(master, replica, mp2chan, done)

	/* MP2 invokes this handler upon any change in the membership list */
	http.HandleFunc("/mp3/mp2notify", func(w http.ResponseWriter, r *http.Request) {
		mp3util.NodeLogger.Debug("Entered /mp2notify handler")
		mp2chan <- true
	})

	http.HandleFunc("/mp3/getfile", func(w http.ResponseWriter, r *http.Request) {
		mp3util.NodeLogger.Debug("Entered /getfile handler")
		if config.COLLECT_STATS {
			startTime := time.Now()
			defer func(startTime time.Time) {
				endTime := time.Now()
				delta := endTime.Sub(startTime)
				mp3util.NodeLogger.Infof("Getfile took %v usec to complete\n", delta.Microseconds())
			}(startTime)
		}
		client, err := amogus.NewClient()
		if err != nil {
			mp3util.NodeLogger.Debug("Could not start client: ", err)
			w.WriteHeader(500)
			return
		}
		defer client.Close()

		err = clientHandler(w, r, client.GetFile)
		if err != nil {
			mp3util.NodeLogger.Error("getfile error: ", err)
			fmt.Fprintf(w, "getfile error: %v", err.Error())
			w.WriteHeader(500)
		}
	})

	http.HandleFunc("/mp3/putfile", func(w http.ResponseWriter, r *http.Request) {
		mp3util.NodeLogger.Debug("Entered /putfile handler")
		if config.COLLECT_STATS {
			startTime := time.Now()
			defer func(startTime time.Time) {
				endTime := time.Now()
				delta := endTime.Sub(startTime)
				mp3util.NodeLogger.Infof("Putfile took %v usec to complete\n", delta.Microseconds())
			}(startTime)
		}
		client, err := amogus.NewClient()
		if err != nil {
			mp3util.NodeLogger.Debug("Could not start client: ", err)
			w.WriteHeader(500)
			return
		}
		defer client.Close()

		err = clientHandler(w, r, client.PutFile)
		if err != nil {
			mp3util.NodeLogger.Error("putfile error: ", err)
			fmt.Fprintf(w, "getfile error: %v", err.Error())
			w.WriteHeader(500)
		}
	})

	http.HandleFunc("/mp3/ls", func(w http.ResponseWriter, r *http.Request) {
		mp3util.NodeLogger.Debug("Entered /ls handler")
		client, err := amogus.NewClient()
		if err != nil {
			mp3util.NodeLogger.Debug("Could not start client: ", err)
			w.WriteHeader(500)
			return
		}
		defer client.Close()

		err = clientHandler(w, r, client.Ls)
		if err != nil {
			mp3util.NodeLogger.Error("ls error: ", err)
			fmt.Fprintf(w, "getfile error: %v", err.Error())
			w.WriteHeader(500)
		}
	})

	http.HandleFunc("/mp3/store", func(w http.ResponseWriter, r *http.Request) {
		mp3util.NodeLogger.Debug("Entered /store handler")
		client, err := amogus.NewClient()
		if err != nil {
			mp3util.NodeLogger.Debug("Could not start client: ", err)
			w.WriteHeader(500)
			return
		}
		defer client.Close()

		err = clientHandler(w, r, client.Store)
		if err != nil {
			mp3util.NodeLogger.Error("store error: ", err)
			w.WriteHeader(500)
			fmt.Fprintf(w, "getfile error: %v", err.Error())
		}
	})

	http.HandleFunc("/mp3/deletefile", func(w http.ResponseWriter, r *http.Request) {
		mp3util.NodeLogger.Debug("Entered /mp3/deletefile handler")
		client, err := amogus.NewClient()
		if err != nil {
			mp3util.NodeLogger.Debug("Could not start client: ", err)
			w.WriteHeader(500)
			return
		}
		defer client.Close()

		err = clientHandler(w, r, client.DeleteFile)
		if err != nil {
			mp3util.NodeLogger.Error("deletefile error: ", err)
			fmt.Fprintf(w, "getfile error: %v", err.Error())
			w.WriteHeader(500)
		}
	})

	http.HandleFunc("/mp3/getversions", func(w http.ResponseWriter, r *http.Request) {
		mp3util.NodeLogger.Debug("Entered /getversions handler")
		if config.COLLECT_STATS {
			startTime := time.Now()
			defer func(startTime time.Time) {
				endTime := time.Now()
				delta := endTime.Sub(startTime)
				mp3util.NodeLogger.Infof("Getversions took %v usec to complete\n", delta.Microseconds())
			}(startTime)
		}
		client, err := amogus.NewClient()
		if err != nil {
			mp3util.NodeLogger.Debug("Could not start client: ", err)
			w.WriteHeader(500)
			return
		}
		defer client.Close()

		err = clientHandler(w, r, client.GetVersions)
		if err != nil {
			mp3util.NodeLogger.Error("getversions error: ", err)
			w.WriteHeader(500)
			fmt.Fprintf(w, "getversions error: %v", err.Error())
		}
	})

	mp3util.NodeLogger.Debug("Launching HTTP server...")
	err := http.ListenAndServe(":"+config.MP3_PORT, nil)
	if err != nil {
		mp3util.NodeLogger.Error("Failed to start http server: err")
	}
	mp3util.NodeLogger.Debug("HTTP server terminated.")
	done <- true
}
