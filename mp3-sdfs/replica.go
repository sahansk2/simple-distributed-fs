package amogus

import (
	"amogus/config"
	"amogus/fsys"
	"amogus/mp3util"
	"amogus/proto"
	"amogus/schema"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type ReplicaMetadata struct {
	Address  string
	MemberId string
	Port     uint32
}

type ReplicaFileInfo struct {
	ReplicaID ReplicaMetadata
	Version   time.Time
}

type ReplicaService struct {
	dataConn net.Conn
	sdfs     *fsys.LocalSDFSStorage
}

type ReplicationJobs struct {
	inProgressReplications fsys.SDFSFileVersionSet
	transferQueue          []fsys.SDFSFile
	mtx                    sync.Mutex
}

var inProgressReplicationJobs ReplicationJobs

func NewReplicaGRPCService() *ReplicaService {
	r := &ReplicaService{}
	sdfs, err := fsys.NewSDFSStorage()
	if err != nil {
		mp3util.NodeLogger.Fatal("Failed to create filesystem module")
		return nil
	}
	inProgressReplicationJobs.inProgressReplications = make(map[string]map[int64]bool)
	r.sdfs = sdfs
	return r
}

/*

 */
func (r *ReplicaService) DataConnHandleQUERYREPLICATIONOFFER(conn net.Conn, req fsys.TCPChannelRequest) error {
	defer conn.Close()
	var replicationTransactions fsys.SDFSFileVersionSet
	err := func() error {
		inProgressReplicationJobs.mtx.Lock()
		defer inProgressReplicationJobs.mtx.Unlock()
		todoReplicationTransactions, err := r.IdentifyDesiredFiles(req)

		if err != nil {
			mp3util.NodeLogger.Error("Couldn't update inProgressReplicationJobs via replication offer!")
			fsys.TrySendTCPChannelResponseError(conn, fsys.MISC_ERROR)
			return err
		}
		err = (&fsys.TCPChannelResponse{
			ResponseCode:            fsys.OK,
			RequestedFileVersionSet: todoReplicationTransactions,
		}).Send(conn)
		if err != nil {
			mp3util.NodeLogger.Error("Couldn't send back requested replication transactions!")
			return err
		}
		// Mark each in-progress.
		for file, _ := range todoReplicationTransactions { // For each file. This is a map.
			_, fileExistsInIPJobs := inProgressReplicationJobs.inProgressReplications[file]
			if !fileExistsInIPJobs {
				inProgressReplicationJobs.inProgressReplications[file] = make(map[int64]bool)
			}
			for version, _ := range todoReplicationTransactions[file] { // For each version. This is a map now.
				inProgressReplicationJobs.inProgressReplications[file][version] = true // You could even put false, IDC
			}
		}
		replicationTransactions = todoReplicationTransactions
		return nil
	}()

	if err != nil {
		mp3util.NodeLogger.Error("Couldn't set up replication workflow! Error: %v ", err)
		return err
	}
	mp3util.NodeLogger.Debugf("replicationTransactions to initialize: %v", replicationTransactions)
	// For every single file
	nthTransaction := 1
	err = func() error {
		for len(replicationTransactions) > 0 { // While we still have transactions to complete on this thread.
			/* Opcode: fsys.REPLICA_SEND_FILE */
			fileReq, err := fsys.RecvTCPChannelRequest(conn)
			if err != nil {
				mp3util.NodeLogger.Errorf("Couldn't get TCPChannelRequest for replication on the %v'th transaction "+
					"out of %v total pending transactions!!", nthTransaction, len(replicationTransactions))
				return err
			}
			err = (&fsys.TCPChannelResponse{ResponseCode: fsys.OK}).Send(conn)
			if err != nil {
				mp3util.NodeLogger.Error("Couldn't send ACK for replication on the %v'th transaction out of %v total pending transactions!",
					nthTransaction, len(replicationTransactions))
			}
			mp3util.NodeLogger.Infof("Now downloading %v @ %v...", fileReq.SDFSFileName, fileReq.SDFSFileVersion)
			contentHash, err := r.sdfs.DumpBytesToTmpfile(&io.LimitedReader{R: conn, N: fileReq.FileSize})
			if err != nil {
				mp3util.NodeLogger.Errorf("Couldn't finish downloading file: %v @ %v! Error: %v", fileReq.SDFSFileName, fileReq.SDFSFileVersion, err)
				return err
			}
			mp3util.NodeLogger.Infof("Now registering replica-sent file to fs...")
			err = r.sdfs.RegisterTmpfileToSDFS(contentHash, time.Unix(0, fileReq.SDFSFileVersion), fileReq.SDFSFileName)
			if err != nil {
				mp3util.NodeLogger.Errorf("Couldn't register tmpfile for %v @ %v! Error: %v", fileReq.SDFSFileName, fileReq.SDFSFileVersion, err)
			}

			inProgressReplicationJobs.mtx.Lock()
			// Delete the version
			delete(inProgressReplicationJobs.inProgressReplications[fileReq.SDFSFileName], fileReq.SDFSFileVersion)
			// IF the file no longer has any versions associated with it, no reason to keep the file in the map either.
			if len(inProgressReplicationJobs.inProgressReplications[fileReq.SDFSFileName]) == 0 {
				delete(inProgressReplicationJobs.inProgressReplications, fileReq.SDFSFileName)
			}
			inProgressReplicationJobs.mtx.Unlock()

			delete(replicationTransactions[fileReq.SDFSFileName], fileReq.SDFSFileVersion)
			if len(replicationTransactions[fileReq.SDFSFileName]) == 0 {
				delete(replicationTransactions, fileReq.SDFSFileName)
			}

			nthTransaction += 1
		}
		return nil
	}()

	if err != nil {
		mp3util.NodeLogger.Errorf("Error downloading all file transactions! Error: %v. Acquiring lock to unreserve the incomplete transactions...")
		inProgressReplicationJobs.mtx.Lock()
		defer inProgressReplicationJobs.mtx.Unlock()
		// TODO: Mark the outstanding transfers somewhere so that we may restart them via passive replication or something like that.
		mp3util.NodeLogger.Debugf("Number of elements to unreserve: %v", len(replicationTransactions))
		numDeleted := 0
		for fi := range replicationTransactions {
			// Remove all versions
			for ver := range replicationTransactions[fi] {
				delete(inProgressReplicationJobs.inProgressReplications[fi], ver)
				numDeleted += 1
			}
			// If the file no longer has pending versions associated with it, delete it from the map.
			if len(inProgressReplicationJobs.inProgressReplications[fi]) == 0 {
				delete(inProgressReplicationJobs.inProgressReplications, fi)
			}
		}
		mp3util.NodeLogger.Debugf("Number of deleted inProgress replication jobs: %v", numDeleted)
		return err
	}

	mp3util.NodeLogger.Debug("Successfully replicated %v files!", nthTransaction-1)
	return nil
}

/*
ASSUME CALLER GRABS LOCK.
*/
func (r *ReplicaService) IdentifyDesiredFiles(req fsys.TCPChannelRequest) (fsys.SDFSFileVersionSet, error) {
	mp3util.NodeLogger.Debug("Got a replication offer from another replica.")
	localSet, err := r.sdfs.ListStoredSDFSFilesAllVersions() // lock and THEN get the stored DFS versions to avoid race conditions.
	if err != nil {
		mp3util.NodeLogger.Error("Couldn't list stored SDFS files!")
		return nil, err
	}
	mp3util.NodeLogger.Debugf("All versions stored are: %v", localSet)
	// TODO: This algorithm is subject to change. Right now we're getting as many files as possible from a single replica.
	// Isn't it the case that the upload bandwidth is far lower than the download bandwidth? If we have time, we should make it so that
	// we contact as many different replicas as possible to replicate.
	unregisteredSDFSFileVersionPairs := make(fsys.SDFSFileVersionSet)
	versionsToDelete := make(fsys.SDFSFileVersionSet)

	for assignedFile := range req.FileVersionSet {
		_, existsLocally := localSet[assignedFile]
		if !existsLocally {
			localSet[assignedFile] = map[int64]bool{} // Make MergedKLatestVersions work with this map, otherwise something weird might happen.
		}
		unregisteredSDFSFileVersionPairs[assignedFile], versionsToDelete[assignedFile] =
			fsys.MergedKLatestVersions(localSet[assignedFile], req.FileVersionSet[assignedFile], config.NUM_VERSIONS)

		mp3util.NodeLogger.Debugf("unregistered=%v, versionsToDelete=%v", unregisteredSDFSFileVersionPairs[assignedFile], versionsToDelete[assignedFile])
		// Prevent malformed outputs, our business logic can't handle an file -> empty map.
		if len(unregisteredSDFSFileVersionPairs[assignedFile]) == 0 {
			delete(unregisteredSDFSFileVersionPairs, assignedFile)
		}
		if len(versionsToDelete[assignedFile]) == 0 {
			delete(versionsToDelete, assignedFile)
		}
	}

	mp3util.NodeLogger.Debugf("We currently do NOT have the following files/versions registered on our filesystem: %v."+
		"We now will check the global to see whether we already have inprogress transfers for these unregistered files/versions.", unregisteredSDFSFileVersionPairs)

	pendingTransactions := fsys.DiffForDeletes(unregisteredSDFSFileVersionPairs, inProgressReplicationJobs.inProgressReplications)

	mp3util.NodeLogger.Debugf("Diffing for deletes from unknownPairs->inProgress, because this "+
		"represents files for which transfers do not exist. (e.g. X in unknownPairs but X is not inProgress). inProgress=%v, missing=%v",
		inProgressReplicationJobs.inProgressReplications, unregisteredSDFSFileVersionPairs)
	// Attempt to send a response to the TCPChannel the specific sequence of files and versions, UNORDERED (because it's a map)
	// Therefore we'll need to first read a "request" from the other replica to transfer a specific file with specific size, and then
	// we ACK so we know how many bytes to read from the TCP connection.

	mp3util.NodeLogger.Debugf("The *new* transactions we will initialize are: %v", pendingTransactions)
	return pendingTransactions, nil
}

func (r *ReplicaService) DataConnHandleCLIENTREQKVERSIONS(conn net.Conn, req fsys.TCPChannelRequest) error {
	defer conn.Close()
	mp3util.NodeLogger.Debugf("About to acquire filehandles for SDFSFileName=%v, KVersions=%v", req.SDFSFileName, req.KVersions)
	handles, err := r.sdfs.AcquireFileHandles(req.KVersions, req.SDFSFileName, time.Now())
	if err != nil || len(handles) == 0 {
		if os.IsNotExist(err) || len(handles) == 0 {
			mp3util.NodeLogger.Warn("No file found on this replica.")
			fsys.TrySendTCPChannelResponseError(conn, fsys.FILE_NOT_FOUND)
			return err
		} else {
			mp3util.NodeLogger.Warn("Replica could not access file for some strange reason. Error: ", err)
			fsys.TrySendTCPChannelResponseError(conn, fsys.MISC_ERROR)
			return err
		}
	}
	mp3util.NodeLogger.Debugf("Successfully obtained %v file handles.", len(handles))
	defer fsys.CloseHandles(handles)

	var allVersions []fsys.SDFSFile
	for _, r := range handles {
		allVersions = append(allVersions, fsys.SDFSFile{
			SDFSFileName: req.SDFSFileName,
			Version:      r.Version.UnixNano(),
		})
	}
	mp3util.NodeLogger.Debugf("About to send the response back to the client.")
	err = (&fsys.TCPChannelResponse{
		ResponseCode:          "OK",
		ReturningSDFSFileSize: handles[0].FileSize,
		FileList:              allVersions,
	}).Send(conn)
	if err != nil {
		mp3util.NodeLogger.Errorf("Couldn't send back OK response to client!")
		return err
	}
	mp3util.NodeLogger.Debug("Successfully responded to client req K versions")
	return nil
}

func (r *ReplicaService) DataConnHandleCLIENTREQFILEDATA(conn net.Conn, req fsys.TCPChannelRequest) error {
	defer conn.Close()

	/* Find the latest version of a file per the client's request */
	handles, err := r.sdfs.AcquireFileHandles(1, req.SDFSFileName, time.Unix(0, req.UpperVersionBound))
	if err != nil || len(handles) == 0 {
		if os.IsNotExist(err) || len(handles) == 0 {
			fsys.TrySendTCPChannelResponseError(conn, fsys.FILE_NOT_FOUND)
			return err
		} else {
			fsys.TrySendTCPChannelResponseError(conn, fsys.MISC_ERROR)
			return err
		}
		return err
	}
	defer fsys.CloseHandles(handles)

	err = (&fsys.TCPChannelResponse{
		ResponseCode:          "OK",
		ReturningSDFSFileSize: handles[0].FileSize,
		SDFSFileVersion:       handles[0].Version.UnixNano(),
	}).Send(conn)
	if err != nil {
		mp3util.NodeLogger.Errorf("Couldn't send back OK response to client!")
	}

	/* Send entire file back to client; it's ALREADY gunzipped. */
	nbytes, err := io.Copy(conn, handles[0].Handle)
	if err != nil {
		mp3util.NodeLogger.Errorf("Only sent %v bytes before erroring out sending the (already gzipped) file from disk to conn! Error: %v", nbytes, err)
		return err
	}
	mp3util.NodeLogger.Debugf("Wrote %v bytes to the connection.", nbytes)

	return nil
}

func (r *ReplicaService) DataConnHandleCLIENTREQFILEMETADATA(conn net.Conn, req fsys.TCPChannelRequest) error {
	defer conn.Close()
	/* Find the latest version'ed file for the client's request */
	handles, err := r.sdfs.AcquireFileHandles(1, req.SDFSFileName, time.Now())
	defer fsys.CloseHandles(handles)
	if err != nil {
		mp3util.NodeLogger.Warn("File not found: ", req.SDFSFileName)
		err = (&fsys.TCPChannelResponse{
			ResponseCode: fsys.FILE_NOT_FOUND,
		}).Send(conn)
		return err
	}

	/* Determine file size */
	path := filepath.Join(r.sdfs.RootDir, fsys.STOREDFILE_DIR, handles[0].SDFSFileName)
	stat, err := os.Stat(path)
	if err != nil {
		return err
	}

	/* Construct response for latest file */
	err = (&fsys.TCPChannelResponse{
		ResponseCode:          fsys.OK,
		ReturningSDFSFileSize: stat.Size(),
		SDFSFileVersion:       handles[0].Version.UnixNano(),
	}).Send(conn)

	if err != nil {
		mp3util.NodeLogger.Error("Could not send response back to the client! Error: ", err)
		return err
	}

	return nil
}

/*
Put request from a client.
*/
func (r *ReplicaService) DataConnHandleCLIENTSENDFILEDATA(conn net.Conn, req fsys.TCPChannelRequest) error {
	defer conn.Close()
	/* ACK the client's request to Putfile */
	err := (&fsys.TCPChannelResponse{
		ResponseCode: fsys.OK,
	}).Send(conn)
	if err != nil {
		mp3util.NodeLogger.Error("Could not send ACK to client: ", err)
		return err
	}

	/* Now receive the entire file from the client */
	mp3util.NodeLogger.Debugf("Now receiving the entire file from the client. Given filesize: %v", req.FileSize)
	hashName, err := r.sdfs.DumpBytesToTmpfile(&io.LimitedReader{R: conn, N: req.FileSize})
	if err != nil {
		err = (&fsys.TCPChannelResponse{
			ResponseCode: fsys.MISC_ERROR,
		}).Send(conn)
		if err != nil {
			mp3util.NodeLogger.Error("Could not communicate with client: ", err)
		}
	}

	/* Send hashname back to client */
	response := fsys.TCPChannelResponse{
		ResponseCode:    fsys.OK,
		FileContentHash: hashName,
	}
	mp3util.NodeLogger.Debugf("We are sending back response: %v")
	err = (&response).Send(conn)
	if err != nil {
		mp3util.NodeLogger.Error("Could not communicate with client: ", err)
	}
	return nil
}

func (r *ReplicaService) DataConnHandleCLIENTLISTFILES(conn net.Conn, _ fsys.TCPChannelRequest) error {
	files, err := r.sdfs.ListDirectory()
	if err != nil {
		mp3util.NodeLogger.Error("Unable to list directory! Error: ", err)
		return err
	}
	err = (&fsys.TCPChannelResponse{
		ResponseCode: fsys.OK,
		FileList:     files,
	}).Send(conn)
	if err != nil {
		mp3util.NodeLogger.Error("Unable to send response! Error: ", err)
		return err
	}
	return nil
}

func (r *ReplicaService) DataConnHandleMASTERFINALIZEWRITE(conn net.Conn, req fsys.TCPChannelRequest) error {
	defer conn.Close()

	version := time.Unix(0, req.SDFSFileVersion)
	err := r.sdfs.RegisterTmpfileToSDFS(req.FileContentHash, version, req.SDFSFileName)
	resp := &fsys.TCPChannelResponse{ResponseCode: fsys.OK}
	if err != nil {
		mp3util.NodeLogger.Error("Replica registerToSDFS error: ", err)
		resp.ResponseCode = fsys.BAD_REQUEST
	}
	return handleTCPChannelRequestErr(resp.Send(conn))
}

func handleTCPChannelRequestErr(err error) error {
	if err != nil {
		mp3util.NodeLogger.Error("Replica TCPChannelRequest command failed! Error: ", err)
		return err
	}
	return nil
}

func (r *ReplicaService) DataConnHandleMASTERFINALIZEDELETE(conn net.Conn, req fsys.TCPChannelRequest) error {
	defer conn.Close()
	timestamp := time.Unix(0, req.SDFSFileVersion)
	resp := &fsys.TCPChannelResponse{ResponseCode: fsys.OK}

	fileExists, err := r.sdfs.RemoveSDFSFile(req.SDFSFileName, timestamp)
	if err != nil {
		mp3util.NodeLogger.Error("RemoveSDFSFile error: ", err)
		resp.ResponseCode = fsys.BAD_REQUEST
	}

	if fileExists {
		mp3util.NodeLogger.Warnf("There existed a write after the deletion request for %v", req.SDFSFileName)
	}

	if os.IsNotExist(err) {
		mp3util.NodeLogger.Warnf("Delete failed: DFS file %v not found on this replica", req.SDFSFileName)
		resp.ResponseCode = fsys.FILE_NOT_FOUND
	}

	return handleTCPChannelRequestErr(resp.Send(conn))
}

func (r *ReplicaService) DataConnAccept(conn *net.Conn) {
	req, err := fsys.RecvTCPChannelRequest(*conn)
	if err != nil {
		mp3util.NodeLogger.Error("Replica couldn't receive TCPChannelRequest! Error: ", err)
		return
	}
	switch req.RequestType {
	case fsys.CLIENT_REQ_FILE_METADATA:
		err = r.DataConnHandleCLIENTREQFILEMETADATA(*conn, *req)
		if err != nil {
			mp3util.NodeLogger.Error("DataConnHandleCLIENTREQFILEMETADATA failed. Error: ", err)
			return
		}
	case fsys.CLIENT_REQ_FILE_DATA: // GetFile: send the file to client
		err = r.DataConnHandleCLIENTREQFILEDATA(*conn, *req)
		if err != nil {
			mp3util.NodeLogger.Error("DataConnHandleCLIENTREQFILEDATA failed. Error: ", err)
			return
		}
	case fsys.CLIENT_SEND_FILE_DATA: // PutFile: get the data from the client
		err = r.DataConnHandleCLIENTSENDFILEDATA(*conn, *req)
		if err != nil {
			mp3util.NodeLogger.Error("DataConnHandleCLIENTSENDFILEDATA failed. Error: ", err)
			return
		}
	case fsys.CLIENT_LIST_FILES:
		err := r.DataConnHandleCLIENTLISTFILES(*conn, *req)
		if err != nil {
			mp3util.NodeLogger.Error("DataConnHandleCLIENTLISTFILES. Error: ", err)
			return
		}
	case fsys.MASTER_FINALIZE_WRITE:
		err := r.DataConnHandleMASTERFINALIZEWRITE(*conn, *req)
		if err != nil {
			mp3util.NodeLogger.Error("DataConnHandleMASTERFINALIZEWRITE. Error: ", err)
			return
		}
	case fsys.MASTER_FINALIZE_DELETE:
		err := r.DataConnHandleMASTERFINALIZEDELETE(*conn, *req)
		if err != nil {
			mp3util.NodeLogger.Error("DataConnHandleMASTERFINALIZEDELETE. Error: ", err)
			return
		}
	case fsys.CLIENT_REQ_KVERSIONS:
		err := r.DataConnHandleCLIENTREQKVERSIONS(*conn, *req)
		if err != nil {
			mp3util.NodeLogger.Error("DataConnHandleCLIENTREQKVERSIONS. Error: ", err)
			return
		}
	case fsys.REPLICA_QUERY_FILES:
		err := r.DataConnHandleQUERYREPLICATIONOFFER(*conn, *req)
		if err != nil {
			mp3util.NodeLogger.Error("DataConnHandleQUERYREPLICATIONOFFER. Error: ", err)
			return
		}
	default:
		mp3util.NodeLogger.Error("Unsupported RequestType: ", req.RequestType)
		return
	}
}

func NewReplicaMetadata(replica *proto.ReplicaInfo) ReplicaMetadata {
	return ReplicaMetadata{
		Address:  replica.Name,
		MemberId: replica.Memberid,
		Port:     replica.Port,
	}
}

func (r *ReplicaService) RunDataconnTCP() {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%v", config.MP3_REPLICA_TCP_PORT))
	if err != nil {
		mp3util.NodeLogger.Error("Couldn't TCP listen on Replica port! Error: ", err)
		return
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			mp3util.NodeLogger.Error("Couldn't TCP accept on replica port! Error: ", err)
			continue
		}
		go r.DataConnAccept(&conn)
	}
}

func (r *ReplicaService) Run() {
	go r.RunDataconnTCP()
	mp3util.NodeLogger.Info("Started Dataconn TCP for replica")
	go r.ReplicaDaemon()
	mp3util.NodeLogger.Info("Started Replica Daemon")

	// go r.RunGRPC()
	// mp3util.NodeLogger.Info("Started GRPC server for replica")
}

func UnicastToReplica(req *fsys.TCPChannelRequest, r ReplicaMetadata) (*fsys.TCPChannelResponse, error) {
	mp3util.NodeLogger.Debugf("Unicast to replica with ID=%v at addr=%v\n", r.MemberId, r.Address)
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%v:%v", r.Address, config.MP3_REPLICA_TCP_PORT), config.DEFAULT_TCP_TIMEOUT)
	if err != nil {
		mp3util.NodeLogger.Errorf("Couldn't connect to replica with ID=%v at addr=%v: %v !\n", r.MemberId, r.Address, err)
		return nil, err
	}
	defer conn.Close()

	/* Issue request to replica to write file */
	err = req.Send(conn)
	if err != nil {
		mp3util.NodeLogger.Errorf("Couldn't send request to replica with ID=%v at addr=%v: %v !\n", r.MemberId, r.Address, err)
		return nil, err
	}

	resp, err := fsys.RecvTCPChannelResponse(conn)
	if err != nil {
		mp3util.NodeLogger.Errorf("Did not get OK from replica with ID=%v at addr=%v: %v !\n", r.MemberId, r.Address, err)
		return nil, err
	}

	return resp, nil
}

/*
TODO: Replication for cases:
(
- 1. For each file in our directory, run partitioner.
- 2. For each machine, ask the machine if they have the file & what versions
- 3. If the machines respond with "yes", do nothing.
- 4. If the machine says "i don't have it", then start transferring the file + versions to them.
- 5. Make sure the target is OK with it, they should say "Oh I don't have this, can you give", or they should say "no I don't want"
- 6. Once all files are transferred to all the targets that need the file, wipe it from disk if we're not responsible for it.
)

// Actual code flow
(initiator)
- Identify all filever-pairs we have
- Send it to each replica in partition, and ask each replica to respond with the files & versions that *THEY* want.
- Do the transfer
- Maybe we can do something clever to transfer the files to all replicas?IDK bruh bruh nananana

(responder)
- Gets a request from initiator that they detected some failures and want to send files or some bruh 
- Responder is like "OK i want these ones"
	- To make sure the responder doesn't get the SAME file-version multiple times (connections from multiple initiators)
		, we might have a lock to a shared variable that indicates whether we're already downloading
		(Replicate,ReplicateRequest,PassiveReplicate,GarbageCollect all share mutex)
*/

func (r *ReplicaService) SendFileSetToReplica(conn net.Conn, requested fsys.SDFSFileVersionSet, replica ReplicaMetadata) error {

	// Pls utilize an existing connection
	mp3util.NodeLogger.Debug("Constructing request to send desired file set ", requested, " to replica ", replica)

	for filename, versions := range requested {
		for version, _ := range versions {

			fileHandles, err := r.sdfs.AcquireFileHandles(1, filename, time.Unix(0, version))
			if err != nil {
				mp3util.NodeLogger.Warn("Failed to acquire file handle for file %v", filename)
				fsys.CloseHandles(fileHandles)
				continue
			}

			fileSize := fileHandles[0].FileSize
			fd := fileHandles[0].Handle
			defer fd.Close()

			mp3util.NodeLogger.Debugf("Sending request to send file %v, version %v, file size %v to replica %v",
				filename, version, fileSize, replica)

			/* Issue request to replica to put file */
			err = (&fsys.TCPChannelRequest{
				RequestType:     fsys.REPLICA_SEND_FILE,
				SDFSFileName:    filename,
				SDFSFileVersion: version,
				FileSize:        fileSize,
			}).Send(conn)

			if err != nil {
				mp3util.NodeLogger.Errorf("Failed to send request to send file %v, version %v, file size %v to replica %v",
					filename, version, fileSize, replica)
				return err
			}

			_, err = fsys.RecvTCPChannelResponse(conn)
			if err != nil {
				mp3util.NodeLogger.Errorf("Did not get OK from replica with ID=%v at addr=%v: %v !\n", replica.MemberId, replica.Address, err)
				return err
			}

			/* Now send the entire compressed file over to the replica */
			mp3util.NodeLogger.Debugf("Sending entire file %v, version %v, file size %v to replica %v",
				filename, version, fileSize, replica)
			nbytes, err := io.Copy(conn, fd)
			if err != nil {
				mp3util.NodeLogger.Errorf("Unable to write all bytes to the connection, only wrote %v bytes! Error: %v", nbytes, err)
				return err
			}

			mp3util.NodeLogger.Debugf("Copied %v bytes to replica %v", nbytes, replica)
		}
	}
	return nil
}

func (r *ReplicaService) Replicate() error {
	mp3util.NodeLogger.Debug("Starting active replication")
	myVersionSet, err := r.sdfs.ListStoredSDFSFilesAllVersions()
	mp3util.NodeLogger.Debug("SDFS Version set: ", myVersionSet)
	if err != nil {
		mp3util.NodeLogger.Warn("Failed to get version set for file")
	}

	visitedReplicas := make(map[ReplicaMetadata]bool)

	for fileName := range myVersionSet {
		partition, err := schema.RunPartitioner(&proto.FileInfo{
			Sdfsname: fileName,
		})
		if err != nil {
			mp3util.NodeLogger.Errorf("Partitioner failed for filename %v", fileName)
		}

		/* Query each replica in partition for files and versions they want */
		for _, repInfo := range partition {

			replica := NewReplicaMetadata(repInfo)
			if visitedReplicas[replica] {
				continue
			}

			req := &fsys.TCPChannelRequest{
				RequestType:    fsys.REPLICA_QUERY_FILES,
				FileVersionSet: myVersionSet,
			}

			mp3util.NodeLogger.Debugf("Replicate: Unicast REPLICA_QUERY_FILES to replica with ID=%v at addr=%v\n", replica.MemberId, replica.Address)
			conn, err := net.DialTimeout("tcp", fmt.Sprintf("%v:%v", replica.Address, config.MP3_REPLICA_TCP_PORT), config.DEFAULT_TCP_TIMEOUT)

			if err != nil {
				mp3util.NodeLogger.Errorf("Replicate couldn't connect to replica with ID=%v at addr=%v: %v !\n", replica.MemberId, replica.Address, err)
				if err != nil {
					mp3util.NodeLogger.Error("Failed to close conn", err)
				}
				continue
			}

			/* Ask replica to return set of files that the replica desires */
			err = req.Send(conn)
			if err != nil {
				mp3util.NodeLogger.Errorf("Couldn't send request to replica with ID=%v at addr=%v: %v !\n", replica.MemberId, replica.Address, err)
				err := conn.Close()
				if err != nil {
					mp3util.NodeLogger.Error("Failed to close conn", err)
				}
				continue
			}

			resp, err := fsys.RecvTCPChannelResponse(conn)
			if err != nil {
				mp3util.NodeLogger.Errorf("Did not get OK from replica with ID=%v at addr=%v: %v !\n", replica.MemberId, replica.Address, err)
				err := conn.Close()
				if err != nil {
					mp3util.NodeLogger.Error("Failed to close conn", err)
				}
				continue
			}

			requestedFiles := resp.RequestedFileVersionSet
			err = r.SendFileSetToReplica(conn, requestedFiles, replica)
			if err != nil {
				mp3util.NodeLogger.Warnf("Could not send requested file version set to replica %v", replica)
				continue
			}

			visitedReplicas[replica] = true
			if err := conn.Close(); err != nil {
				mp3util.NodeLogger.Error("Failed to close conn", err)
			}
		}
	}

	return nil
}

func (r *ReplicaService) ReplicaDaemon() {
	t1 := time.NewTimer(config.GARBAGE_COLLECTION_PERIOD)
	//t2 := time.NewTimer(config.PASSIVE_REPLICATION_PERIOD)
	for {
		select {
		case <-t1.C:
			err := r.GarbageCollect()
			t1.Stop() // Avoid weird edge cases
			if err != nil {
				mp3util.NodeLogger.Warn("Failed to garbage collect: ", err)
			}
			t1 = time.NewTimer(config.GARBAGE_COLLECTION_PERIOD) // Tick again

			//case <-t2.C:
			//	err := r.Replicate()
			//	t2.Stop() // Avoid weird edge cases
			//	if err != nil {
			//		mp3util.NodeLogger.Warn("Failed passive replication: ", err)
			//		continue
			//	}
			//	t2 = time.NewTimer(config.PASSIVE_REPLICATION_PERIOD) // Tick again
		}
	}
}

func (r *ReplicaService) GarbageCollect() error {
	schema.MemList.Mtx.Lock()
	self := ReplicaMetadata{
		Address:  schema.MemList.SelfNode.Address,
		MemberId: schema.MemList.SelfNode.Member_Id,
		Port:     schema.MemList.SelfNode.Port,
	}
	schema.MemList.Mtx.Unlock()

	inProgressReplicationJobs.mtx.Lock()
	defer inProgressReplicationJobs.mtx.Unlock()

	files, err := r.sdfs.ListDirectory()
	if err != nil {
		mp3util.NodeLogger.Warn("Garbage collection failed!")
		return err
	}
	if len(files) > 0 {
		mp3util.NodeLogger.Debugf("Periodic garbage collection has files.")
	}
	for _, f := range files {
		mp3util.NodeLogger.Debugf("Checking ownership of file: %v", f.SDFSFileName)
		reps, err := schema.RunPartitioner(&proto.FileInfo{
			Sdfsname:    f.SDFSFileName,
			ContentHash: "",
		})
		if err != nil {
			mp3util.NodeLogger.Errorf("Couldn't run partiioner on the local file! Error: %v", err)
			return err
		}
		ownerOfFile := false
		for _, r := range reps {
			if NewReplicaMetadata(r) == self {
				ownerOfFile = true
				break
			}
		}

		if !ownerOfFile {
			mp3util.NodeLogger.Debugf("No longer the owner of file %v. Deleting now...", f.SDFSFileName)
			fileExists, err := r.sdfs.RemoveSDFSFile(f.SDFSFileName, time.Now())
			if err != nil {
				mp3util.NodeLogger.Errorf("Failed to clean up sdfs file %v: err = %v", f.SDFSFileName, err)
				return err
			}
			if fileExists {
				mp3util.NodeLogger.Warnf("Partial delete for %v due to a more recent write issued by master.: ", f.SDFSFileName)
				return err
			}
		}
	}

	return nil
}
