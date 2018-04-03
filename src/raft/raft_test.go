package raft

import (
	"strconv"
	"testing"
	"time"
)

func TestRaft(t *testing.T) {
	N := 5
	ip := "localhost"
	port := 8080
	peers := make([]*Peer, N)
	servers := make([]*RaftServer, N)

	for i := 0; i < N; i++ {
		peers[i] = NewPeer(ip, strconv.Itoa(port+i))
	}

	for i := 0; i < N; i++ {
		servers[i] = NewRaftServer(peers, i)
		servers[i].Init()
	}
	time.Sleep(1000 * time.Second)
}
