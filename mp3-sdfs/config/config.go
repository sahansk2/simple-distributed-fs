package config

import "time"

var MP2_PORT = "7777"
var MP3_PORT = "7778"
var MP3_MASTER_PORT = "7779" // GRPC
var LISTENER_PORT = "25565"
var CHURN_TIMEOUT_MS = 1500
var GARBAGE_COLLECTION_PERIOD = time.Second * 5
var PASSIVE_REPLICATION_PERIOD = time.Second * 10 // Currently unused
var MP3_REPLICA_TCP_PORT = "7780"                 // TCP
var MP3_REPLICA_GRPC_PORT = "7781"                // GRPC
var RING_SIZE = 32                                // For chord-style file partitioning
var NUM_REPLICAS = 5
var QUORUM_SIZE = 4
var READ_CONSISTENCY = 2
var NO_PARTITIONING_DEBUG = false
var DEFAULT_TCP_TIMEOUT = time.Duration(5 * time.Second)
var NUM_VERSIONS = 5
var COLLECT_STATS = true
