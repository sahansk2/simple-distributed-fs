package schema

import (
	"amogus/config"
	"amogus/mp3util"
	"amogus/proto"
	"container/ring"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
)

type Member struct {
	Member_Id    string
	Address      string
	PingsDropped int
	Port         uint32
}

type MembershipList struct {
	List     []Member
	SelfNode Member
	Mtx      sync.Mutex
}

var MemList MembershipList

type CliArgs struct {
	LocalFileName string
	SdfsFileName  string
	NumVersions   int
	Bruhflag      bool
}

/**
 * CurrMasterNode
 *	The membership list from MP2 is NOT assumed to be
 *	sorted. But, the designated master node is the node in the
 *	membership with the min ID. Linear scan to find the smallest ID
 *
 *	NOTE: ASSUMES CALLER GRABS LOCK
 */
func (ml MembershipList) CurrMasterNode() (Member, error) {
	if len(ml.List) == 0 {
		/* Empty membership list... */
		mp3util.NodeLogger.Warn("Membership list is empty. Returning nil master...")
		return Member{}, errors.New("Empty membership list, no master.")
	}

	masterNode := ml.List[0]
	for _, m := range ml.List {
		currMasterId := masterNode.Member_Id
		membId := m.Member_Id
		if membId == "" {
			return Member{}, errors.New(fmt.Sprintf("Member ID corrupted: %v", m))
		}

		if membId < currMasterId {
			masterNode = m
		}
	}

	return masterNode, nil
}

/*
Note: does not need f.ContentHash.
*/
func RunPartitioner(f *proto.FileInfo) ([]*proto.ReplicaInfo, error) {
	mp3util.NodeLogger.Debug("Running partitioner for sdfsfilename: ", f.Sdfsname)
	var replicaList []*proto.ReplicaInfo

	/* Hash the sdfs name and truncate to RING_SIZE bits */
	fileHashId := GetRingId(f.Sdfsname)
	mp3util.NodeLogger.Debugf("SDFSname %v hashes to id: %x", f.Sdfsname, fileHashId)

	memList := &MemList
	memList.Mtx.Lock()
	defer memList.Mtx.Unlock()

	/* Sort the membership list by hash-ID in the ring */
	memListSortedByHash := memList.List // TODO: Verify this actually makes a copy
	sort.Slice(memListSortedByHash, func(i, j int) bool {
		a := GetRingId(memListSortedByHash[i].Member_Id)
		b := GetRingId(memListSortedByHash[j].Member_Id)
		return a < b
	})

	/* Create ring buffer representing topology of nodes */
	ringBuf := ring.New(len(memListSortedByHash))
	currRing := ringBuf
	for _, memb := range memListSortedByHash {
		currRing.Value = memb
		currRing = currRing.Next()
	}

	/* Debug loop */
	currRing = ringBuf
	for i := 0; i < ringBuf.Len(); i++ {
		memb := currRing.Value.(Member)
		mp3util.NodeLogger.Tracef("Node with id: %v hashes to %x", memb.Member_Id, GetRingId(memb.Member_Id))
		currRing = currRing.Next()
	}

	/* Now iterate through each member in the sorted chord ring, and find the group of six
	 * replicas that contain this file id */
	currRing = ringBuf
	for i := 0; i < ringBuf.Len(); i++ {
		memb := currRing.Value.(Member)
		if GetRingId(memb.Member_Id) >= fileHashId {
			break
		}
		currRing = currRing.Next()
	}

	start := currRing
	for i := 0; i < config.NUM_REPLICAS; i++ {
		memb := currRing.Value.(Member)
		mp3util.NodeLogger.Tracef("Target node containing file has id: %x", GetRingId(memb.Member_Id))
		replicaList = append(replicaList, &proto.ReplicaInfo{Name: memb.Address, Port: memb.Port, Memberid: memb.Member_Id})
		/* Fewer than 6 nodes in the system */
		if currRing.Next() == start {
			break
		}
		currRing = currRing.Next()
	}

	/* Return quorum */
	return replicaList, nil
}

/**
 * GetRingId
 *	Computes the SHA256 hash of a given string, and truncates to
 *	RING_SIZE bits. This represents an ID in the chord-style ring.
 * 	@param s - string to hash
 *	@return id - id in ring
 */
func GetRingId(s string) uint64 {
	h := sha256.New()
	h.Write([]byte(s))

	/* Truncate to RING_SIZE bits */
	truncate := 32 - (config.RING_SIZE / 8)

	nameHash := hex.EncodeToString(h.Sum(nil)[truncate:])
	id, err := strconv.ParseUint(nameHash, 16, config.RING_SIZE)
	if err != nil {
		mp3util.NodeLogger.Fatal("Failed to compute hash for string: ", s, " with err: ", err)
	}
	return id
}
