package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"raft-redis-cluster/raft"
	"raft-redis-cluster/store"
	"raft-redis-cluster/transport"
	"strings"
	"time"

	raftboltdb "github.com/hashicorp/raft-boltdb"

	hraft "github.com/hashicorp/raft"
)

type initialPeersList []Peer

type Peer struct {
	NodeID    string
	RaftAddr  string
	RedisAddr string
}

func (i *initialPeersList) Set(value string) error {
	node := strings.Split(value, ",")
	for _, n := range node {
		nodes := strings.Split(n, "=")
		if len(nodes) != 2 {
			return errors.New("invalid node format")
		}
		address := strings.Split(nodes[1], "|")
		if len(address) != 2 {
			return errors.New("invalid address format")
		}
		*i = append(*i, Peer{
			NodeID:    nodes[0],
			RaftAddr:  address[0],
			RedisAddr: address[1],
		})
	}

	return nil
}

func (i *initialPeersList) String() string {
	return fmt.Sprintf("%v", *i)
}

var (
	raftAddr  = flag.String("address", "localhost:50051", "TCP h› ›ost+port for this raft node")
	redisAddr = flag.String("redis_address", "localhost:6379", "› ›TCP host+port for redis")
	serverID  = flag.String("server_id", "", "Node id used by Ra› ›ft")
	dataDir   = flag.String("data_dir", "", "Raft data dir")
	initial   initialPeersList
)

func init() {
	flag.Var(&initial, "initial_peers", "Initial peers in the format node1=raftAddr|redisAddr,node2=raftAddr|redisAddr")
	flag.Parse()
}

func validateFlags() {
	if *serverID == "" {
		log.Fatal("server_id is required")
	}
	if *dataDir == "" {
		log.Fatal("data_dir is required")
	}
	if *raftAddr == "" {
		log.Fatal("address is required")
	}
	if *redisAddr == "" {
		log.Fatal("redis_address is required")
	}
}

func main() {
	dataStore := store.NewMemoryStore()
	st := raft.NewStateMachine(dataStore)
	r, sdb, err := NewRaft(*dataDir, *serverID, *raftAddr, st, initial)
	if err != nil {
		log.Fatal(err)
	}
	redis := transport.NewRedis(hraft.ServerID(*serverID), r, dataStore, sdb)
	err = redis.Serve(*redisAddr)
	if err != nil {
		log.Fatal(err)
	}
}

const snapShotRetainCount = 2

func NewRaft(baseDir string, id string, address string, fsm hraft.FSM, nodes initialPeersList) (*hraft.Raft, hraft.StableStore, error) {
	c := hraft.DefaultConfig()
	c.LocalID = hraft.ServerID(id)

	ldb, err := raftboltdb.NewBoltStore(filepath.Join(baseDir, "logs.dat"))
	if err != nil {
		return nil, nil, err
	}

	sdb, err := raftboltdb.NewBoltStore(filepath.Join(baseDir, "stable.dat"))
	if err != nil {
		return nil, nil, err
	}

	fss, err := hraft.NewFileSnapshotStore(baseDir, snapShotRetainCount, os.Stderr)
	if err != nil {
		return nil, nil, err
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, nil, err
	}

	tm, err := hraft.NewTCPTransport(address, tcpAddr, 10, time.Second*10, os.Stderr)
	if err != nil {
		return nil, nil, err
	}

	r, err := hraft.NewRaft(c, fsm, ldb, sdb, fss, tm)
	if err != nil {
		return nil, nil, err
	}

	cfg := hraft.Configuration{
		Servers: []hraft.Server{
			{
				Suffrage: hraft.Voter,
				ID:       hraft.ServerID(id),
				Address:  hraft.ServerAddress(address),
			},
		},
	}

	// 自分以外の initialPeers を追加
	for _, peer := range nodes {
		sid := hraft.ServerID(peer.NodeID)
		cfg.Servers = append(cfg.Servers, hraft.Server{
			Suffrage: hraft.Voter,
			ID:       sid,
			Address:  hraft.ServerAddress(peer.RaftAddr),
		})

		err := store.SetRedisAddrByNodeID(sdb, sid, peer.RedisAddr)
		if err != nil {
			return nil, nil, err
		}
	}

	f := r.BootstrapCluster(cfg)
	if err := f.Error(); err != nil {
		return nil, nil, err
	}

	return r, sdb, nil
}
