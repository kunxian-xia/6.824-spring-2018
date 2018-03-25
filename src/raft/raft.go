package consensus

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"reflect"
	"sync"
	"time"
)

type Peer struct {
	ip   string
	port string
}

func NewPeer(ip string, port string) *Peer {
	peer := new(Peer)
	peer.ip = ip
	peer.port = port
	return peer
}

type State int

const (
	FOLLOWER  = iota
	CANDIDATE = iota
	LEADER    = iota
)

type TxType int

const ()

type Transaction struct {
	txtype TxType
}
type Log struct {
	tx   Transaction
	term int
}
type AppendEntryArgs struct {
	term     int
	leaderId int
	entries  []Log
}
type AppendEntryReply struct {
	term int
}
type RequestVoteArgs struct {
	term        int // term in which this election happens
	candidateId int // who is asking for vote
}
type RequestVoteReply struct {
	term        int
	voteGranted bool
}

type RaftServer struct {
	mux   sync.Mutex
	peers []*Peer
	me    int

	state       State
	votedFor    int
	leaderId    int
	currentTerm int

	status int
	//add channels
	timeout chan int
	newterm chan int
	leader  chan int
}

//handler for RequestVote RPC
func (server *RaftServer) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	if args.term > server.currentTerm {
		server.state = FOLLOWER
		server.currentTerm = args.term
		server.votedFor = args.candidateId
		reply.term = server.currentTerm
		reply.voteGranted = true
	} else {
		switch server.state {
		case FOLLOWER:
			//receives a vote request
			if args.term < server.currentTerm {
				reply.voteGranted = false
			} else {
				if server.votedFor == 0 || server.votedFor == args.candidateId {
					server.votedFor = args.candidateId
					reply.voteGranted = true
				} else {
					reply.voteGranted = false
				}
				//server.currentTerm = args.term
			}
			reply.term = server.currentTerm
			break

		case CANDIDATE:
		case LEADER:
			//candidate's votedFor must be itself
			reply.term = server.currentTerm
			reply.voteGranted = false
			//assert(server.votedFor == server.me)
			if server.votedFor != server.me {
			} else {
			}
			break

		default:
			//panic()
			break
		}
	}
	return nil
}

//handler for AppendEntries RPC
func (server *RaftServer) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) error {
	if args.term < server.currentTerm {
		reply.term = server.currentTerm
	} else {
		server.currentTerm = args.term
		server.leaderId = args.leaderId
		server.state = FOLLOWER
		reply.term = server.currentTerm
	}
	return nil
}

func NewRaftServer(peers []*Peer, me int) *RaftServer {
	server := new(RaftServer)
	server.peers = peers
	server.me = me
	server.state = FOLLOWER
	server.votedFor = -1
	server.currentTerm = 0
	server.leaderId = -1
	return server
}
func (server *RaftServer) Init() {
	n := len(server.peers)
	server.timeout = make(chan int)
	server.leader = make(chan int)
	server.newterm = make(chan int, n)

	rpc.Register(server)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":"+server.peers[server.me].port)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
func (server *RaftServer) Run() {
	for {
		select {
		case <-server.timeout:
			server.mux.Lock()
			switch server.state {
			case FOLLOWER:
				//start an election
				server.state = CANDIDATE
				go server.candidate()
				break
			case CANDIDATE:
				//start a new term
				break

			case LEADER:
				//ignore
				break
			}
			server.mux.Unlock()
		case <-server.leader:

			go server.follower()
			//switch to follower
		case <-server.newterm:
			//switch to follower
		}
	}
}

func (server *RaftServer) leaderMode() {
	n := len(server.peers)
	cmds := make([]chan int, n)
	for i := 0; i < n; i++ {
		if i == server.me {
			continue
		}
		addr := server.peers[i].ip + ":" + server.peers[i].port
		go func(cmd chan int) {
			for {
				var reply AppendEntryReply
				var appendCall *rpc.Call
				var ch chan int
				client, err := rpc.DialHTTP("tcp", addr)
				if err == nil {
					server.mux.Lock()
					args := &AppendEntryArgs{term: server.currentTerm,
						leaderId: server.me}
					server.mux.Unlock()
					appendCall = client.Go("RaftServer.AppendEntries", args, &reply, nil)
				} else {
					//wait some time
					continue
				}
				select {
				case <-cmd:
					return
				case heartbeat := <-appendCall.Done:
					if heartbeat.Error == nil {
						term := heartbeat.Reply.(AppendEntryReply).term
						if term > server.currentTerm {
							//signal new term
						}
					}
					//time.Sleep(rand.Int63())
				}
			}
		}(cmds[i])
	}
	cases := make([])
}
func (server *RaftServer) followerMode() {
	//do nothing
}
func (server *RaftServer) candidateMode() {
	//request for vote
	n := len(server.peers)
	replys := make([]chan *RequestVoteReply, n)
	channels := map[int]int{}
	for i, j := 0, 0; i < n; i++ {
		if i == server.me {
			continue
		}
		channels[j], j = i, j+1
		addr := server.peers[i].ip + ":" + server.peers[i].port
		go func(addr string, ch chan *RequestVoteReply) {
			for {
				var cli *rpc.Client
				var reply RequestVoteReply
				for {
					client, err := rpc.DialHTTP("tcp", addr)
					if err == nil {
						cli = client
						break
					}
					time.Sleep(30 * time.Millisecond)
				}
				server.mux.Lock()
				args := &RequestVoteArgs{candidateId: server.me,
					term: server.currentTerm}
				server.mux.Unlock()
				voteCall := cli.Go("RaftServer.RequestVote", args, &reply, nil)
				replyCall := <-voteCall.Done
				if replyCall.Error == nil {
					r := replyCall.Reply.(RequestVoteReply)
					ch <- (&r)
					break
				}
			}
		}(addr, replys[i])
	}

	cases := make([]reflect.SelectCase, n)
	for i, ch := range replys {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
	}
	//cases[n-1] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf()}
	remaining := len(cases)
	votes := 0
	for remaining > 0 {
		chosen, value, ok := reflect.Select(cases)
		if !ok {
			// The chosen channel has been closed, so zero out the channel to disable the case
			cases[chosen].Chan = reflect.ValueOf(nil)
			remaining -= 1
			continue
		}
		term, voteGranted := int(value.FieldByName("term").Int()),
			value.FieldByName("voteGranted").Bool()
		fmt.Printf("Vote from #%d: (%d, %t)\n", channels[chosen], term, voteGranted)

		if term > server.currentTerm {
			//signal newterm to main routine
		} else if voteGranted {
			votes += 1
		}
		if votes > n/2 {
			//signal leader
		}
	}
}

func (server *RaftServer) timer() {
	for {
		time.Sleep(250 * time.Millisecond)
		server.timeout <- 1
	}
}

/*
type Raft struct {
}

func Make(peers []*Peer, me int) *Raft {

}

func (raft *Raft) Start(command interface{}) {

}

func (raft *Raft) GetState() (term int, isLeader bool) {

}
*/
