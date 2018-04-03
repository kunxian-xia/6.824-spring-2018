package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"time"

	"github.com/kunxian-xia/6.824-spring-2018/src/raft"
)

type EchoArg struct {
	Msg string
}
type EchoReply struct {
	Answer string
}
type Echo struct {
	Counter int
}

func (s *Echo) EchoRPC(args *EchoArg, reply *EchoReply) error {
	question := args.Msg
	s.Counter += 1
	reply.Answer = "the " + strconv.Itoa(s.Counter) + " question: " + question
	return nil
}
func client() {
	client, err := rpc.DialHTTP("tcp", "localhost:8081")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("echo > ")
		text, _ := reader.ReadString('\n')
		if text == "quit" {
			fmt.Println("see you next time!")
			return
		} else {
			args := &EchoArg{text}
			var reply EchoReply
			err = client.Call("Echo.EchoRPC", args, &reply)
			if err != nil {
				log.Fatal("echo error:", err)
			}
			fmt.Println(reply.Answer)
		}
	}
}

func server() {
	echo := new(Echo)
	echo.Counter = 0
	rpc.Register(echo)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":8080")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func timer(ch chan int) {
	for {
		time.Sleep(1000 * time.Millisecond)
		ch <- 1
	}
}

func closure() {
	for i := 0; i < 10; i++ {
		go func() {
			time.Sleep(50 * time.Millisecond)
			fmt.Printf("# %d \n", i)
		}()
	}
}

func call(i int, ch chan int) {
	if i == 0 {
		time.Sleep(3 * time.Second)
		fmt.Println("data is ready")
		ch <- 15
	} else {
		ch2 := make(chan int)
		go call(i-1, ch2)
		msg := <-ch2
		fmt.Printf("read %d from %d\n", msg, i-1)
		ch <- msg + 1
	}
}

type Point struct {
	x int
	y int
}

func client1(ch chan *Point) {
	p := &Point{x: 0, y: 0}
	ch <- p
	time.Sleep(2 * time.Second)
	fmt.Printf("x=%d, y=%d\n", p.x, p.y)
}

func client2() {
	ch := make(chan *Point)

	go client1(ch)
	point := <-ch
	point.x += 3
	point.y += 5
}

func spinner(d time.Duration) {
	for {
		for _, r := range `-\|/` {
			fmt.Printf("\r%c", r)
			time.Sleep(d)
		}
	}
}

func fib(n int) int {
	if n < 2 {
		return n
	} else {
		return fib(n-1) + fib(n-2)
	}
}

func raft_test() {
	var i int
	N := 11
	ip := "localhost"
	port := 8080
	peers := make([]*raft.Peer, N)
	servers := make([]*raft.RaftServer, N)

	for i = 0; i < N; i++ {
		peers[i] = raft.NewPeer(ip, strconv.Itoa(port+i))
	}

	for i = 0; i < N; i++ {
		servers[i] = raft.NewRaftServer(peers, i)
		servers[i].Init()
	}
	for j := 0; j < 4; j++ {
		time.Sleep(10 * time.Second)
		for i = 0; i < N; i++ {
			if servers[i].GetState() == raft.LEADER {
				servers[i].Kill()
			}
		}
	}
	//time.Sleep(100 * time.Second)
}

func main() {
	/*
		go client()
		go server()
	*/
	/*
		ch := make(chan int)
		//state := make(chan int)
		go timer(ch)

		for {
			select {
			case <-ch:
				fmt.Println("times out event happens")
				//		case <-state:

			}
		}
	*/
	//closure()
	/*
			ch := make(chan int)
			go call(3, ch)
			fmt.Printf("read %d from %d\n", <-ch, 3)
		//client2()
		go spinner(100 * time.Millisecond)
		N := fib(45)
		fmt.Printf("\r%d\n", N)
		time.Sleep(8 * time.Second)
	*/
	raft_test()
}
