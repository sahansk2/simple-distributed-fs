package amogus

import (
	"amogus/config"
	"amogus/fsys"
	"amogus/mp3util"
	"amogus/proto"
	"amogus/schema"
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"
)

type Client struct {
	masterStub      proto.MasterClient
	CleanupCallback func()
}

/**
 * Close
 *	Used to teardown client conn with master.
 */
func (c *Client) Close() {
	c.CleanupCallback()
}

/**
 * NewClient
 *	Creates a client, connecting to the current master node.
 *	@return c - new client object
 */
func NewClient() (*Client, error) {
	memList := &schema.MemList
	memList.Mtx.Lock()
	defer memList.Mtx.Unlock()
	mater, err := memList.CurrMasterNode()
	if err != nil {
		mp3util.NodeLogger.Error("Couldn't establish master node to connect to")
		return nil, err
	}

	c := &Client{}

	/* Set up connection with the master node */
	mp3util.NodeLogger.Debugf("Dialing GRPC for master %v at address %v:%v\n", mater.Member_Id, mater.Address, mater.Port)
	conn, err := grpc.Dial(fmt.Sprintf("%v:%v", mater.Address, config.MP3_MASTER_PORT), grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Fatalf("Failed to dial: %v", err)
	}
	c.CleanupCallback = func() {
		err := conn.Close()
		if err != nil {
			mp3util.NodeLogger.Error("Could not close client connection")
		}
	}
	c.masterStub = proto.NewMasterClient(conn)
	err = c.createLocalStorage()

	return c, nil
}

func (c *Client) createLocalStorage() error {
	localFileDir := filepath.Join(".", fsys.LOCALFILE_DIR)
	err := os.MkdirAll(localFileDir, 0777)
	if err != nil && !os.IsExist(err) {
		mp3util.NodeLogger.Errorf("Error creating directory %v: %v\n", localFileDir, err)
		return err
	}

	return nil
}

func (c *Client) openFile(filePath string, mode int) (*os.File, error) {
	fd, err := os.OpenFile(filePath, mode, os.ModePerm)
	if err != nil {
		mp3util.NodeLogger.Error("Client failed to open file: ", filePath, " with err: ", err)
		return nil, err
	}
	return fd, nil
}

/**
 * GetReplicas
 *	Client side request over GRPC to master. Fetches quorum of replicas from
 *	the master, and returns metadata (e.g hostname, port info) back to the
 *	caller.
 *	@param args - file args, containing target file on sdfs
 *	@return replicaList - metadata for quorum of replicas
 */
func (c *Client) GetReplicas(args schema.CliArgs) ([]ReplicaMetadata, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := c.masterStub.GetReplicas(ctx, &proto.FileInfo{
		Sdfsname:    args.SdfsFileName,
		ContentHash: "IGNORED_FIELD", // doesn't make sense we just want partitioning function :(
	})

	if err != nil {
		mp3util.NodeLogger.Fatal("Failed GetReplicas", err)
		return nil, err
	}

	/* Receive quorum of replicas */
	var replicaList []ReplicaMetadata
	for {
		replica, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			mp3util.NodeLogger.Error("Failed to recv entire stream of replicas from master", err)
			return nil, err
		}
		replicaList = append(replicaList, NewReplicaMetadata(replica))
		mp3util.NodeLogger.Debug("Returned: ", replica.GetMemberid(), " ", replica.GetName(), " ", replica.GetPort())
	}
	return replicaList, nil
}

func (c *Client) GetReplicasNonQuorum(args schema.CliArgs) ([]ReplicaMetadata, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := c.masterStub.GetReplicasNonQuorum(ctx, &proto.FileInfo{
		Sdfsname:    args.SdfsFileName,
		ContentHash: "IGNORED_FIELD", // doesn't make sense we just want partitioning function :(
	})

	if err != nil {
		mp3util.NodeLogger.Fatal("Failed GetReplicas", err)
		return nil, err
	}

	/* Receive quorum of replicas */
	var replicaList []ReplicaMetadata
	for {
		replica, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			mp3util.NodeLogger.Error("Failed to recv entire stream of replicas from master", err)
			return nil, err
		}
		replicaList = append(replicaList, NewReplicaMetadata(replica))
		mp3util.NodeLogger.Debug("Returned: ", replica.GetMemberid(), " ", replica.GetName(), " ", replica.GetPort())
	}
	return replicaList, nil
}

func (c *Client) FinalizeWrite(contentHash string, args schema.CliArgs, replicas []ReplicaMetadata) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	/* Convert structs */
	var quorum []*proto.ReplicaInfo
	for _, r := range replicas {
		quorum = append(quorum, &proto.ReplicaInfo{
			Name:     r.Address,
			Port:     r.Port,
			Memberid: r.MemberId,
		})
	}

	status, err := c.masterStub.FinalizeWrite(ctx, &proto.FileAndQuorumInfo{
		Quorum: quorum,
		Args:   &proto.FileInfo{Sdfsname: args.SdfsFileName, ContentHash: contentHash},
	})

	if err != nil {
		mp3util.NodeLogger.Error("Error finalizing write on master: ", err)
	}
	mp3util.NodeLogger.Debug("Status received from FinalizeWrite", status)
	return err
}

/////// woo yea
func (c *Client) QueryReplicaForLatestVersion(args schema.CliArgs, r ReplicaMetadata) (time.Time, error) {
	// Defer resource leak info: https://stackoverflow.com/a/45620423/6184823
	mp3util.NodeLogger.Debugf("Initiating GetFile transaction with replica with ID=%v at addr=%v\n", r.MemberId, r.Address)
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%v:%v", r.Address, config.MP3_REPLICA_TCP_PORT), config.DEFAULT_TCP_TIMEOUT)
	if err != nil {
		mp3util.NodeLogger.Errorf("Couldn't connect to replica with ID=%v at addr=%v: %v !\n", r.MemberId, r.Address, err)
		return time.Unix(0, 0), err
	}
	defer conn.Close()
	/* Issue request to replica to fetch file version */
	req := fsys.TCPChannelRequest{RequestType: fsys.CLIENT_REQ_FILE_METADATA, SDFSFileName: args.SdfsFileName}
	err = req.Send(conn)
	if err != nil {
		mp3util.NodeLogger.Errorf("Couldn't send request to replica with ID=%v at addr=%v: %v !\n", r.MemberId, r.Address, err)
		return time.Time{}, err
	}

	resp, err := fsys.RecvTCPChannelResponse(conn)
	if err != nil {
		mp3util.NodeLogger.Warnf("Error response from replica with ID=%v at addr=%v: %v !\n", r.MemberId, r.Address, err)
		return time.Unix(0, 0), err
	}

	mp3util.NodeLogger.Debugf("Got response: %v\n", *resp)

	if resp.ResponseCode == fsys.OK {
		return time.Unix(0, resp.SDFSFileVersion), nil
	} else {
		mp3util.NodeLogger.Errorf("Response was not OK from replica %v - response was %v", r, resp)
		return time.Unix(0, 0), nil
	}
}

/*
NON GRPC Function
*/
func (c *Client) SendFileToReplica(args schema.CliArgs, fd *os.File, compressedFileSize int64, r ReplicaMetadata) (*fsys.TCPChannelResponse, error) {

	// Defer resource leak info: https://stackoverflow.com/a/45620423/6184823
	mp3util.NodeLogger.Debugf("Initiating PutFile transaction with replica with ID=%v at addr=%v\n", r.MemberId, r.Address)
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%v:%v", r.Address, config.MP3_REPLICA_TCP_PORT), config.DEFAULT_TCP_TIMEOUT)
	defer fd.Seek(0, 0)
	if err != nil {
		mp3util.NodeLogger.Errorf("Couldn't connect to replica with ID=%v at addr=%v: %v !\n", r.MemberId, r.Address, err)
		return nil, err
	}

	defer conn.Close()
	mp3util.NodeLogger.Debug("Constructing request to send tot he file")
	/* Issue request to replica to put file */
	err = (&fsys.TCPChannelRequest{
		RequestType:  fsys.CLIENT_SEND_FILE_DATA,
		SDFSFileName: args.SdfsFileName,
		FileSize:     compressedFileSize,
	}).Send(conn)

	if err != nil {
		mp3util.NodeLogger.Errorf("Couldn't send request to replica with ID=%v at addr=%v: %v !\n", r.MemberId, r.Address, err)
		return nil, err
	}

	resp, err := fsys.RecvTCPChannelResponse(conn)
	if err != nil {
		mp3util.NodeLogger.Errorf("Did not get OK from replica with ID=%v at addr=%v: %v !\n", r.MemberId, r.Address, err)
		return nil, err
	}

	/* Now send the entire compressed file over to the replica */
	if err = fsys.SendFileAsGzip(fd, conn); err != nil {
		mp3util.NodeLogger.Errorf("Couldn't send file to replica with ID=%v at addr=%v: %v !\n", r.MemberId, r.Address, err)
	}

	resp, err = fsys.RecvTCPChannelResponse(conn)
	if err != nil {
		mp3util.NodeLogger.Errorf("Did not get OK from replica with ID=%v at addr=%v: %v !\n", r.MemberId, r.Address, err)
		return nil, err
	}

	return resp, nil
}

/**
 * GetFile
 *	Gets a file from SDFS and copies locally onto disk.
 */
func (c *Client) GetFile(args schema.CliArgs) error {
	mp3util.NodeLogger.Debug("Entered client.GetFile")
	replicas, err := c.GetReplicas(args)
	mp3util.NodeLogger.Debug("Getfile received replicas: ", replicas)

	if err != nil || len(replicas) == 0 {
		mp3util.NodeLogger.Error("Client can't get replicas!")
		if err != nil {
			return err
		} else {
			return errors.New("Length of GetReplicas was zero.")
		}
	}

	/* Contact each replica with a CLIENT_REQ_FILE_METADATA request.
	 * The response will contain the latest file version a replica has for the given sdfsfile.
	 * Determine which replica has the latest file version.
	 */

	// Cap the replicas to query for reads here.
	if len(replicas) > config.READ_CONSISTENCY {
		replicas = replicas[:config.READ_CONSISTENCY]
	}
	var latestVersionReplica ReplicaMetadata
	latestVersion := time.Unix(0, 0)
	replicaWithFileExists := false
	for _, r := range replicas {
		replicaVersion, err := c.QueryReplicaForLatestVersion(args, r)
		if err == nil {
			replicaWithFileExists = true
			/* Determine latest timestamp replica */
			if replicaVersion.After(latestVersion) {
				latestVersion = replicaVersion
				latestVersionReplica = r
			}
		}
	}
	if !replicaWithFileExists {
		mp3util.NodeLogger.Errorf("Replica with SDFSFile=%v not found!", args.SdfsFileName)
		return os.ErrNotExist
	}

	latestVersionReplicaFileInfo := ReplicaFileInfo{
		ReplicaID: latestVersionReplica,
		Version:   time.Now(),
	}
	err = c.ReceiveFileFromReplica(args.SdfsFileName, args.LocalFileName, latestVersionReplicaFileInfo)
	if err != nil {
		mp3util.NodeLogger.Errorf("Failed to receive file from replica with ID=%v at addr=%v: %v !\n", latestVersionReplica.MemberId, latestVersionReplica.Address, err)
		return err
	}
	return err
}

func (c *Client) ReceiveFileFromReplica(sdfsFileName string, localFileName string, repInfo ReplicaFileInfo) error {

	r := repInfo.ReplicaID
	version := repInfo.Version.UnixNano()

	/* Now, receive the file from the replica with the latest version */
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%v:%v", r.Address, config.MP3_REPLICA_TCP_PORT), config.DEFAULT_TCP_TIMEOUT)
	if err != nil {
		mp3util.NodeLogger.Errorf("Couldn't connect to replica with ID=%v at addr=%v: %v !\n", r.MemberId, r.Address, err)
		return err
	}
	defer conn.Close()

	err = (&fsys.TCPChannelRequest{
		RequestType:       fsys.CLIENT_REQ_FILE_DATA,
		SDFSFileName:      sdfsFileName,
		UpperVersionBound: version,
	}).Send(conn)
	if err != nil {
		mp3util.NodeLogger.Error("Couldn't send request to download file from replica! Error: %v", err)
		return err
	}
	resp, err := fsys.RecvTCPChannelResponse(conn)
	if err != nil {
		mp3util.NodeLogger.Errorf("Couldn't get response back from replica! Error: %v", err)
		return err
	}

	localFilePath := filepath.Join(fsys.LOCALFILE_DIR, localFileName)
	fd, err := c.openFile(localFilePath, os.O_WRONLY|os.O_CREATE)
	defer fd.Close()
	if err != nil {
		return err
	}
	mp3util.NodeLogger.Debug("About to recv file over TCP")
	nbytes, err := fsys.RecvFileFromGzip(&io.LimitedReader{
		R: conn,
		N: resp.ReturningSDFSFileSize,
	}, fd)
	mp3util.NodeLogger.Debug("Received ", nbytes, " bytes from replica")
	return err
}

func (c *Client) PutFile(args schema.CliArgs) error {
	mp3util.NodeLogger.Debug("Entered client.PutFile")
	replicas, err := c.GetReplicas(args)
	mp3util.NodeLogger.Debug("Received replicas: ", replicas)

	if err != nil || len(replicas) == 0 {
		mp3util.NodeLogger.Error("Client can't get replicas!")
		if err != nil {
			return err
		} else {
			return errors.New("Length of GetReplicas was zero.")
		}
	}

	/* Open file locally and calculate compressed file size */
	localFilePath := filepath.Join(".", args.LocalFileName)
	fd, err := c.openFile(localFilePath, os.O_RDONLY)
	if err != nil {
		return err
	}
	defer fd.Close()

	compressedFileSize, err := fsys.GetGzipFileSize(fd)
	mp3util.NodeLogger.Debugf("Calculated compressed file size: %v", compressedFileSize)
	fd.Seek(0, 0)
	if err != nil {
		return err
	}

	/* Contact each replica with a CLIENT_SEND_FILE_DATA request.
	 * The response will contain an ACK.
	 * Then, send the gzip'd file to each replica
	 */
	contentHash := ""
	for _, r := range replicas {
		resp, err := c.SendFileToReplica(args, fd, compressedFileSize, r)
		if err == nil {
			contentHash = resp.FileContentHash
		}
	}

	if contentHash == "" {
		return errors.New("No response from any replicas")
	}

	/* Issue a write request to master to finalize file send */
	return c.FinalizeWrite(contentHash, args, replicas)
}

func (c *Client) Ls(args schema.CliArgs) error {
	/*
		LS: need to find all machines that could have the file.
		Step 1: Get the replicas from the master.
		Step 2: Initialize connection with the replicas and ask do you have file?
		Step 3: Replica respond
		Step 4: We print to user.
	*/
	mp3util.NodeLogger.Debug("Entered client.GetFile")
	replicas, err := c.GetReplicasNonQuorum(args)
	mp3util.NodeLogger.Debug("Received replicas: ", replicas)

	if err != nil || len(replicas) == 0 {
		mp3util.NodeLogger.Error("Client can't get replicas!")
		if err != nil {
			return err
		} else {
			return errors.New("Length of GetReplicas was zero.")
		}
	}

	/* Contact each replica with a CLIENT_REQ_FILE_METADATA request.
	 * The response will contain the latest file version a replica has for the given sdfsfile.
	 * Determine which replica has the latest file version.
	 */

	var replicasWithFile []ReplicaFileInfo
	for _, r := range replicas {
		v, err := c.QueryReplicaForLatestVersion(args, r)
		if err == nil {
			replicasWithFile = append(replicasWithFile, ReplicaFileInfo{ReplicaID: r, Version: v})
		}
	}

	w := tabwriter.NewWriter(os.Stdout, 1, 4, 4, ' ', 0)
	fmt.Fprintln(w, "Replica\tFile\tLatest Version\tReadable Version\t")
	fmt.Fprintln(w, "===========\t===========\t===========\t===========\t")
	for _, replicaAndVer := range replicasWithFile {
		fmt.Fprintf(w, "%v\t%v\t%v\t%v\t\n", replicaAndVer.ReplicaID.MemberId, args.SdfsFileName,
			replicaAndVer.Version.UnixNano(), replicaAndVer.Version.Format(time.RFC822))
	}
	w.Flush()
	return nil
}

/*
Store doesn't take any arguments. TESTED WORKING.
*/
func (c *Client) Store(args schema.CliArgs) error {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%v:%v", "localhost", config.MP3_REPLICA_TCP_PORT), config.DEFAULT_TCP_TIMEOUT)
	if err != nil {
		mp3util.NodeLogger.Errorf("Couldn't connect to replica on self! Error: %v", err)
		return err
	}
	err = (&fsys.TCPChannelRequest{
		RequestType:  fsys.CLIENT_LIST_FILES,
		SDFSFileName: args.SdfsFileName,
	}).Send(conn)

	if err != nil {
		mp3util.NodeLogger.Error("Couldn't send request to self replica! Error: ", err)
		return err
	}

	resp, err := fsys.RecvTCPChannelResponse(conn)
	if err != nil {
		mp3util.NodeLogger.Error("Couldn't get response back from self replica! Error: ", err)
		return err
	}

	// im sooo hungry manh
	//prettyTable := tabwriter.NewWriter(os.Stderr, 0, 4, 0, '\t', 0)
	w := tabwriter.NewWriter(os.Stdout, 1, 2, 3, ' ', 0)
	fmt.Fprintln(w, "File\tVersion\t(Readable Version)\t") // goofy aah whjitspace
	fmt.Fprintln(w, "===========\t===========\t===========\t")
	for _, locallyStoredSDFSFile := range resp.FileList {
		t := time.Unix(0, locallyStoredSDFSFile.Version)
		fmt.Fprintf(w, "%v\t%v\t%v\t\n", locallyStoredSDFSFile.SDFSFileName, t.UnixNano(), t.Format(time.RFC822))
	}
	w.Flush()
	return nil
}

func (c *Client) DeleteFile(args schema.CliArgs) error {
	// TODO: Potbelly milkshake for five dollars
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.masterStub.FinalizeDelete(
		ctx,
		&proto.FileInfo{
			Sdfsname: args.SdfsFileName,
		},
	)
	mp3util.NodeLogger.Debugf("Response gotten from master: %v", resp)
	if err != nil {
		mp3util.NodeLogger.Error("Error trying to delete the file: ", err)
		return err
	}
	return nil
}

func (c *Client) GetVersions(args schema.CliArgs) error {

	/* Procedure:
	 *	Get 6 replicas from master's partitioning function, for file
	 *	Contact each of 6 replicas asking for k latest versions
	 *	Get lists back from each, compile and sort lists to determine
	 *	 which nodes have the latest version.
	 *	Get files with version numbers from correct nodes
	 */

	mp3util.NodeLogger.Debug("Entered client.GetVersions")
	replicas, err := c.GetReplicasNonQuorum(args)
	mp3util.NodeLogger.Debug("Client GETVERSIONS: Received replicas: ", replicas)

	if err != nil || len(replicas) == 0 {
		mp3util.NodeLogger.Error("Client can't get replicas!")
		if err != nil {
			return err
		} else {
			return errors.New("Length of GetReplicas was zero.")
		}
	}

	/* Get replica-version pairs */
	var replicaVersionPairs []ReplicaFileInfo
	for _, r := range replicas {
		req := &fsys.TCPChannelRequest{
			RequestType:     fsys.CLIENT_REQ_KVERSIONS,
			SDFSFileVersion: time.Now().UnixNano(),
			SDFSFileName:    args.SdfsFileName,
			KVersions:       args.NumVersions,
		}

		mp3util.NodeLogger.Debug("Client GETVERSIONS: Received replicas: ", replicas)
		resp, err := UnicastToReplica(req, r)
		if err != nil {
			mp3util.NodeLogger.Errorf("Couldn't unicast KGetVersions on replica %v! Error: %v", r.MemberId, err)
			continue
		}

		kFiles := resp.FileList
		for _, file := range kFiles {
			replicaVersionPairs = append(replicaVersionPairs, ReplicaFileInfo{
				ReplicaID: r,
				Version:   time.Unix(0, file.Version),
			})
		}
	}

	/* Sort list by descending version and choose latest k unique versions */
	sort.Slice(replicaVersionPairs, func(i, j int) bool {
		a := replicaVersionPairs[i].Version
		b := replicaVersionPairs[j].Version
		return a.After(b)
	})

	numVersions := args.NumVersions
	if numVersions > len(replicaVersionPairs) {
		numVersions = len(replicaVersionPairs)
	}

	/* 0th entry in the table has the earliest version, so ver. number counts up */
	ver := 0

	fetchedVersionMap := make(map[int64]bool)

	mp3util.NodeLogger.Debug("GETVERSIONS replicaVersionPairs: ", replicaVersionPairs)
	for k := 0; k < len(replicaVersionPairs); k++ {
		fileName := fmt.Sprintf("%s-version-%d", args.LocalFileName, ver)
		repVersionPair := replicaVersionPairs[k]
		mp3util.NodeLogger.Debugf("GETVERSIONS ver/replica/time = %v/%v/%v", ver, repVersionPair.ReplicaID, repVersionPair.Version.UnixNano())
		if fetchedVersionMap[repVersionPair.Version.UnixNano()] {
			mp3util.NodeLogger.Warnf("Already fetched file/version = %v/%v. Skipping replica: %v", args.SdfsFileName, repVersionPair.Version, repVersionPair.ReplicaID)
			continue
		}

		err = c.ReceiveFileFromReplica(args.SdfsFileName, fileName, repVersionPair)
		if err != nil {
			mp3util.NodeLogger.Warnf("Failed to contact replica %v for file, version =  %v, %v",
				repVersionPair.ReplicaID, repVersionPair.Version)
			continue
		}

		fetchedVersionMap[repVersionPair.Version.UnixNano()] = true

		ver++
		if ver == args.NumVersions {
			break
		}
	}

	mp3util.NodeLogger.Debugf("Successfully ontacted %v replicas", ver)

	if ver < args.NumVersions {
		mp3util.NodeLogger.Warnf("Could not fetch all %v versions for file %v", args.NumVersions, args.SdfsFileName)
	}
	if args.Bruhflag {
		go c.GetVersionsBruh(args.LocalFileName, ver)
	}
	return nil
}

func (c *Client) GetVersionsBruh(localFileName string, latestVersion int) {
	mp3util.NodeLogger.Warn("This was never meant to beeeeeeeeeeeeeeeeeee")
	fmt.Println("You are bad for calling this command.\n")
	fmt.Printf("OOOOOOOOOOOOOOOOO\n")

	var cmd []string
	pathma := path.Join(".", fsys.LOCALFILE_DIR)
	cmd = append(cmd, "cat")
	latestVersion--
	for latestVersion >= 0 {
		cmd = append(cmd, fmt.Sprintf("<(printf '\\n=======%v======\\n')", latestVersion))
		cmd = append(cmd, fmt.Sprintf("%v/%v-version-%v", pathma, localFileName, latestVersion))
		latestVersion--
	}

	cmd = append(cmd, fmt.Sprintf("> %v/%v", pathma, localFileName))

	catCmd := exec.Command("bash", "-c", fmt.Sprintf("\"%v\"", strings.Join(cmd, " ")))
	mp3util.NodeLogger.Debugf("Goofy aah catCmd: %v", catCmd)
	err := catCmd.Run()
	if err != nil {
		mp3util.NodeLogger.Error("Oops get versions bruh messed up maybe you should use the real one the error is ", err)
	}
}
