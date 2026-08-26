// Package simnet is an in-process, fake "network" for testing distributed
// algorithms (Raft, in particular) the way MIT's 6.824/6.5840 labrpc does it.
//
// The trick is that a peer never talks to another peer through a real socket.
// Instead it holds a *ClientEnd and calls end.Call("Raft.RequestVote", args, reply).
// The Network intercepts that call, gob-encodes args (so a bug that mutates a
// struct through a shared pointer gets caught, exactly like a real network
// would force a copy), decides whether the link is currently connected,
// whether to drop or delay the request, and — if it goes through — uses
// reflection to invoke the matching method on the receiving service.
//
// None of this is Raft-specific. You register your Raft peer's RPC handlers
// as a *Service and everything else (partitions, drops, reordering, crash
// simulation via the companion persister package) is free.
package simnet

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------
// Wire messages
// ---------------------------------------------------------------------

type reqMsg struct {
	endname string
	svcMeth string
	args    []byte
	replyCh chan replyMsg
}

type replyMsg struct {
	ok    bool
	reply []byte
}

// ---------------------------------------------------------------------
// ClientEnd: what a Raft peer holds to call one other peer.
// ---------------------------------------------------------------------

type ClientEnd struct {
	endname string
	ch      chan reqMsg
	done    chan struct{}
}

// Call sends an RPC-shaped request through the simulated network and blocks
// for a reply. It returns false if the call was dropped, timed out, or the
// caller's end is currently disconnected — exactly the signal a real Go
// net/rpc client gives you on a broken connection, so your Raft code should
// already know how to handle it.
func (e *ClientEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
	req := reqMsg{endname: e.endname, svcMeth: svcMeth, replyCh: make(chan replyMsg)}

	qb := new(bytes.Buffer)
	if err := gob.NewEncoder(qb).Encode(args); err != nil {
		panic(fmt.Sprintf("simnet: encode args for %s: %v", svcMeth, err))
	}
	req.args = qb.Bytes()

	select {
	case e.ch <- req:
	case <-e.done:
		return false
	}

	rep, ok := <-req.replyCh
	if !ok || !rep.ok {
		return false
	}

	rb := bytes.NewBuffer(rep.reply)
	if err := gob.NewDecoder(rb).Decode(reply); err != nil {
		log.Fatalf("simnet: decode reply for %s: %v", svcMeth, err)
	}
	return true
}

// ---------------------------------------------------------------------
// Service / Server: the receiving side, dispatched to by reflection.
// ---------------------------------------------------------------------

// Service wraps one Go object (e.g. your *Raft) so its exported RPC-shaped
// methods — func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) —
// can be invoked by name.
type Service struct {
	name    string
	rcvr    reflect.Value
	methods map[string]reflect.Method
}

func MakeService(rcvr interface{}) *Service {
	svc := &Service{
		rcvr:    reflect.ValueOf(rcvr),
		methods: map[string]reflect.Method{},
	}
	typ := reflect.TypeOf(rcvr)
	svc.name = reflect.Indirect(svc.rcvr).Type().Name()

	for m := 0; m < typ.NumMethod(); m++ {
		method := typ.Method(m)
		mtype := method.Type
		// expect: func (rf *Raft) Method(args ArgsT, reply *ReplyT)
		if method.PkgPath != "" || mtype.NumIn() != 3 || mtype.In(2).Kind() != reflect.Ptr || mtype.NumOut() != 0 {
			continue
		}
		svc.methods[method.Name] = method
	}
	return svc
}

// Server hosts one or more Services under one simulated network address.
type Server struct {
	mu       sync.Mutex
	services map[string]*Service
	rpcCount int
}

func MakeServer() *Server {
	return &Server{services: map[string]*Service{}}
}

func (rs *Server) AddService(svc *Service) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.services[svc.name] = svc
}

func (rs *Server) dispatch(req reqMsg) replyMsg {
	dot := -1
	for i := len(req.svcMeth) - 1; i >= 0; i-- {
		if req.svcMeth[i] == '.' {
			dot = i
			break
		}
	}
	if dot < 0 {
		return replyMsg{false, nil}
	}
	serviceName, methodName := req.svcMeth[:dot], req.svcMeth[dot+1:]

	rs.mu.Lock()
	rs.rpcCount++
	svc, ok := rs.services[serviceName]
	rs.mu.Unlock()
	if !ok {
		log.Fatalf("simnet: unknown service %q in %s", serviceName, req.svcMeth)
	}

	method, ok := svc.methods[methodName]
	if !ok {
		log.Fatalf("simnet: unknown method %q on service %q", methodName, serviceName)
	}

	argsType := method.Type.In(1)
	argsIsPtr := argsType.Kind() == reflect.Ptr
	var argsVal reflect.Value
	if argsIsPtr {
		argsVal = reflect.New(argsType.Elem())
	} else {
		argsVal = reflect.New(argsType)
	}
	if err := gob.NewDecoder(bytes.NewBuffer(req.args)).Decode(argsVal.Interface()); err != nil {
		log.Fatalf("simnet: decode args for %s: %v", req.svcMeth, err)
	}
	if !argsIsPtr {
		argsVal = argsVal.Elem()
	}

	replyType := method.Type.In(2).Elem()
	replyVal := reflect.New(replyType)

	svc.rcvr.MethodByName(methodName).Call([]reflect.Value{argsVal, replyVal})

	rb := new(bytes.Buffer)
	if err := gob.NewEncoder(rb).EncodeValue(replyVal); err != nil {
		log.Fatalf("simnet: encode reply for %s: %v", req.svcMeth, err)
	}
	return replyMsg{true, rb.Bytes()}
}

// ---------------------------------------------------------------------
// Network: owns connectivity, reliability, and the dispatch loop.
// ---------------------------------------------------------------------

type Network struct {
	mu             sync.Mutex
	reliable       bool
	longDelays     bool // pause a long time before reporting a dead/disconnected peer
	longReordering bool // sometimes delay a reply a long time
	ends           map[string]*ClientEnd
	enabled        map[string]bool    // by end name
	servers        map[string]*Server // by server name
	connections    map[string]string  // end name -> server name it currently reaches
	endCh          chan reqMsg
	done           chan struct{}
	rpcTotal       int32
}

func MakeNetwork() *Network {
	rn := &Network{
		reliable:    true,
		ends:        map[string]*ClientEnd{},
		enabled:     map[string]bool{},
		servers:     map[string]*Server{},
		connections: map[string]string{},
		endCh:       make(chan reqMsg),
		done:        make(chan struct{}),
	}
	go func() {
		for {
			select {
			case req := <-rn.endCh:
				atomic.AddInt32(&rn.rpcTotal, 1)
				go rn.processReq(req)
			case <-rn.done:
				return
			}
		}
	}()
	return rn
}

func (rn *Network) Cleanup() { close(rn.done) }

func (rn *Network) Reliable(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.reliable = yes
}

func (rn *Network) LongReordering(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.longReordering = yes
}

func (rn *Network) LongDelays(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.longDelays = yes
}

func (rn *Network) RPCCount() int { return int(atomic.LoadInt32(&rn.rpcTotal)) }

// MakeEnd creates the handle a peer uses to call out. endname should be
// unique, e.g. fmt.Sprintf("%d->%d", from, to).
func (rn *Network) MakeEnd(endname string) *ClientEnd {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if _, ok := rn.ends[endname]; ok {
		log.Fatalf("simnet: end %q already exists", endname)
	}
	e := &ClientEnd{endname: endname, ch: rn.endCh, done: rn.done}
	rn.ends[endname] = e
	rn.enabled[endname] = true
	rn.connections[endname] = ""
	return e
}

func (rn *Network) AddServer(servername string, rs *Server) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.servers[servername] = rs
}

func (rn *Network) DeleteServer(servername string) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.servers[servername] = nil
}

// Connect wires an end to the server it should currently reach.
func (rn *Network) Connect(endname string, servername string) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.connections[endname] = servername
}

// Enable/disable one end. This is how you simulate a partition: disable
// every end belonging to the isolated peer (its outgoing ends AND every
// other peer's end that targets it).
func (rn *Network) Enable(endname string, enabled bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.enabled[endname] = enabled
}

func (rn *Network) processReq(req reqMsg) {
	rn.mu.Lock()
	enabled := rn.enabled[req.endname]
	servername := rn.connections[req.endname]
	server := rn.servers[servername]
	reliable := rn.reliable
	longReordering := rn.longReordering
	longDelays := rn.longDelays
	rn.mu.Unlock()

	if !enabled || servername == "" || server == nil {
		// Simulate an eventual, silent failure — a real client would time out.
		ms := rand.Intn(100)
		if longDelays {
			ms = rand.Intn(7000)
		}
		time.AfterFunc(time.Duration(ms)*time.Millisecond, func() {
			req.replyCh <- replyMsg{false, nil}
		})
		return
	}

	if !reliable {
		time.Sleep(time.Duration(rand.Intn(27)) * time.Millisecond)
	}
	if !reliable && rand.Intn(1000) < 100 {
		// drop the request itself
		req.replyCh <- replyMsg{false, nil}
		return
	}

	ech := make(chan replyMsg)
	go func() { ech <- server.dispatch(req) }()

	var reply replyMsg
	replyOK := false
	select {
	case reply = <-ech:
		replyOK = true
	case <-time.After(2 * time.Second):
		// handler hung (e.g. the receiver was disconnected mid-call) — bail
	}

	rn.mu.Lock()
	stillGood := rn.enabled[req.endname] && rn.connections[req.endname] == servername
	rn.mu.Unlock()

	switch {
	case !replyOK || !stillGood:
		req.replyCh <- replyMsg{false, nil}
	case !reliable && rand.Intn(1000) < 100:
		// drop the reply on the way back
		req.replyCh <- replyMsg{false, nil}
	case longReordering && rand.Intn(900) < 600:
		delay := 200 + rand.Intn(2000)
		time.AfterFunc(time.Duration(delay)*time.Millisecond, func() {
			req.replyCh <- reply
		})
	default:
		req.replyCh <- reply
	}
}
