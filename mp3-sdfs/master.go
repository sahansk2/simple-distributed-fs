package amogus

import (
	"amogus/config"
	"amogus/fsys"
	"amogus/mp3util"
	"amogus/proto"
	"amogus/schema"
	"context"
	"google.golang.org/grpc"
	"math/rand"
	"net"
	"sync"
	"time"
)

type MasterGRPCService struct {
	proto.UnimplementedMasterServer
	server   *grpc.Server
	isActive bool
	mtx      sync.Mutex
}

/**
 * NewMasterGRPCService
 *	Creates a new master object. Checks if this node is the current
 *	master - if so, initializes master object with the "isActive" flags
 *	set to true.
 *	@return m - master object
 */
func NewMasterGRPCService() *MasterGRPCService {
	m := &MasterGRPCService{}
	return m
}

/**
 * MembershipListChanged
 *	Called when membership list changes. Checks if this node is the new
 *	master node. If so, runs the GRPC server. If not, stops the GRPC server.
 * 	Note: Needs to grab two locks; one for the master, one for the membership list
 */
func (m *MasterGRPCService) MembershipListChanged() error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	memList := &schema.MemList
	memList.Mtx.Lock()
	defer memList.Mtx.Unlock()
	currMaster, err := memList.CurrMasterNode()
	if err != nil {
		m.stop()
		m.isActive = false
		return err
	}

	/* If this node is a new master, run the GRPC server.
	 * If this node is no longer a master, stop the GRPC server.
	 * In all other cases, no action taken
	 */
	selfNode := &memList.SelfNode
	if (!m.isActive) && (currMaster.Member_Id == selfNode.Member_Id) {
		mp3util.NodeLogger.Info("Node elected as master. Running GRPC server.")
		m.run()
		m.isActive = true
		return nil
	}
	if (m.isActive) && (currMaster.Member_Id != selfNode.Member_Id) {
		mp3util.NodeLogger.Info("Node no longer master. Stopping GRPC server.")
		m.stop()
		m.isActive = false
		return nil
	}

	return nil
}

/**
 * selectQuorum
 *	Returns quorum from given set of replicas.
 *	W, R = 4. Number of replicas = 6.
 *	@param replicaList - list of replicas
 *	@return quorum - quorum of replicas
 */
func selectQuorum(replicaList []*proto.ReplicaInfo) []*proto.ReplicaInfo {
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(replicaList), func(i, j int) {
		replicaList[i], replicaList[j] = replicaList[j], replicaList[i]
	})
	end := config.QUORUM_SIZE
	if len(replicaList) < end {
		end = len(replicaList)
	}
	quorum := replicaList[:end]
	mp3util.NodeLogger.Debug("Selected quorum: ", quorum)
	return quorum
}

/**
 * partitioner
 *	Determines which replicas contain a given sdfsfile. First, hashes the sdfsfilename
 *	to determine its file id in the ring. Then, hashes the IDs of each member in
 *	the membership list, and determines which six replicas contain the file.
 *	This partitioner works exactly the same as Chord's. The first ID that overshoots the
 *	file id is the first node that contains the sdfsfile. The next succeeding 5, in sorted
 *	order of hashed id, are replicas. Returns a quorum of the six replicas (W, R = 4, for 6 replicas).
 *
 *	@param f - file info struct, containing sdfsfilename
 *	@return replicaList - quorum of replicas that are responsible for a given sdfsfile.
 */
func (m *MasterGRPCService) partitioner(f *proto.FileInfo) ([]*proto.ReplicaInfo, error) {
	return schema.RunPartitioner(f)
}

/**
 * GetReplicas
 *	Response to client request for replicas, whether for getfile or putfile.
 * 	Runs partitioning function for the requested file to get the correct set of
 * 	replicas in the membership list (six total replicas). Then, chooses a quorum
 *	(four replicas) and streams replica contact information back to the client.
 *
 *	@param f - args containing requested file
 *	@param stream - grpc stream back to client
 *	@output Quorum of replicas
 */
func (m *MasterGRPCService) GetReplicas(f *proto.FileInfo, stream proto.Master_GetReplicasServer) error {
	mp3util.NodeLogger.Debug("Entered master/GetReplicas")

	if config.NO_PARTITIONING_DEBUG {
		for _, m := range schema.MemList.List {
			retm := &proto.ReplicaInfo{Name: m.Address, Port: m.Port, Memberid: m.Member_Id}
			err := stream.Send(retm)
			if err != nil {
				mp3util.NodeLogger.Error("Couldn't send back replicas to stream. Error: ", err)
				return err
			}
			return nil
		}
		return nil
	}

	replicas, err := m.partitioner(f)
	if err != nil {
		mp3util.NodeLogger.Debug("Partitioner failed: ", err)
		return err
	}
	replicas = selectQuorum(replicas)
	mp3util.NodeLogger.Debug("About to return quorum of replicas")
	/* Stream back replicas in quorum, one by one */
	for _, r := range replicas {
		err := stream.Send(r)
		if err != nil {
			mp3util.NodeLogger.Error("Failed to send replica: ", err)
		}
	}

	return nil
}

/**
 * GetReplicaNonQuorum
poger
*/
func (m *MasterGRPCService) GetReplicasNonQuorum(f *proto.FileInfo, stream proto.Master_GetReplicasNonQuorumServer) error {
	mp3util.NodeLogger.Debug("Entered master/GetReplicasNonQuorum")
	replicas, err := m.partitioner(f)
	if err != nil {
		mp3util.NodeLogger.Debug("Partitioner failed: ", err)
		return err
	}
	mp3util.NodeLogger.Debug("About to return quorum of replicas")
	/* Stream back replicas in quorum, one by one */
	for _, r := range replicas {
		err := stream.Send(r)
		if err != nil {
			mp3util.NodeLogger.Error("Failed to send replica: ", err)
		}
	}
	return nil
}

// Input: FileInfo
// Output: Status
func (m *MasterGRPCService) FinalizeWrite(ctx context.Context, fq *proto.FileAndQuorumInfo) (*proto.Status, error) {
	mp3util.NodeLogger.Debug("Entered master/FinalizeWrite")
	m.mtx.Lock()
	defer m.mtx.Unlock()

	timestamp := time.Now().UnixNano()
	/* Contact each replica in quorum and issue a FinalizeWrite request */
	for _, repInfo := range fq.Quorum {
		r := NewReplicaMetadata(repInfo)
		req := &fsys.TCPChannelRequest{
			RequestType:     fsys.MASTER_FINALIZE_WRITE,
			SDFSFileVersion: timestamp,
			FileContentHash: fq.Args.ContentHash,
			SDFSFileName:    fq.Args.Sdfsname,
		}

		_, err := UnicastToReplica(req, r)
		if err != nil {
			mp3util.NodeLogger.Errorf("Couldn't finalize write on replica %v! Error: %v", r.MemberId, err)
		}
	}
	return &proto.Status{Rc: "FinishedWriteFinished"}, nil
}

// Input: FileInfo
// Output: Status
func (m *MasterGRPCService) FinalizeDelete(ctx context.Context, f *proto.FileInfo) (*proto.Status, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	mp3util.NodeLogger.Debug("Entered master/finalizedelete")

	timestamp := time.Now().UnixNano()
	replicas, err := m.partitioner(f)
	if err != nil {
		mp3util.NodeLogger.Error("Couldn't delete file on node!")
		return nil, err
	}

	for _, r := range replicas {
		_, err := UnicastToReplica(&fsys.TCPChannelRequest{
			RequestType:     fsys.MASTER_FINALIZE_DELETE,
			SDFSFileName:    f.Sdfsname,
			SDFSFileVersion: timestamp,
		}, NewReplicaMetadata(r))
		if err != nil {
			// TODO: Drink Potbelly Milkshake
			mp3util.NodeLogger.Warnf("Couldn't delete file on replica %v! Error: %v", r.Memberid, err)
			continue
		}
	}

	return &proto.Status{Rc: "FinalizeDeleteFinished"}, nil
}

/**
 * run
 *	Runs the master GRPC server on this node. Called
 *	once on init and every time this node becomes the master, which
 *	is provoked from a membership list change.
 *	NOTE: Assumes caller grabs lock
 */
func (m *MasterGRPCService) run() {
	grpcServer := grpc.NewServer()
	proto.RegisterMasterServer(grpcServer, m)

	conn, err := net.Listen("tcp", ":"+config.MP3_MASTER_PORT)
	if err != nil {
		mp3util.NodeLogger.Fatal("Master tcp: ", err)
	}

	/* Spawn goroutine for GRPC server */
	go func() {
		err := grpcServer.Serve(conn)
		if err != nil {
			mp3util.NodeLogger.Fatal("Master grpc server: ", err)
		}
	}()

	mp3util.NodeLogger.Info("Started GRPC server for master")
	m.server = grpcServer
}

/**
 * Stop
 *	Stops the master GRPC server on this node. Called whenever
 *	this node is no longer the master, provoked by a membership list
 *	change.
 *	NOTE: Assumes caller grabs lock
 */
func (m *MasterGRPCService) stop() {
	mp3util.NodeLogger.Info("Stopping GRPC server for master.")
	if m.server != nil {
		m.server.Stop()
	}
}
