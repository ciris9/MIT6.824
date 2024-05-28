package kvraft

import (
	"bytes"
	"log"
	"mit6.824/labgob"
	"mit6.824/labrpc"
	"mit6.824/raft"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	serverLen    int

	kv          map[string]string
	dedup       map[int]interface{}
	get         map[int]chan string
	done        map[int]chan struct{}
	lastApplied int
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	var dedup map[int]interface{}
	var kvmap map[string]string
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&dedup); err != nil {
		dedup = make(map[int]interface{})
	}
	if err := d.Decode(&kvmap); err != nil {
		kvmap = make(map[string]string)
	}
	kv.dedup = dedup
	kv.kv = kvmap
}

func (kv *KVServer) apply(v raft.ApplyMsg) {
	if v.CommandIndex <= kv.lastApplied {
		return
	}
	var key string
	switch args := v.Command.(type) {
	case GetArgs:
		key = args.Key
		kv.lastApplied = v.CommandIndex
	case PutAppendArgs:
		key = args.Key
		if dup, ok := kv.dedup[int(args.ClientId)]; ok {
			if putDup, ok := dup.(PutAppendArgs); ok && putDup.RequestId == args.RequestId {
				//solve Idempotency problem
				DPrintf("Method:apply {Node %v} the same requestId:%d! reject", kv.rf.GetNode(), putDup.RequestId)
				break
			}
		}
		if args.Op == OpPut {
			kv.kv[args.Key] = args.Value
		} else {
			builder := strings.Builder{}
			builder.WriteString(kv.kv[args.Key])
			builder.WriteString(args.Value)
			kv.kv[args.Key] = builder.String()
		}
		kv.dedup[int(args.ClientId)] = v.Command
		kv.lastApplied = v.CommandIndex
	}
	DPrintf("Method:apply {Node %v} appled commandIndex:%d command:%v key:%s value: %s", kv.rf.GetNode(), v.CommandIndex, v.Command, key, kv.kv[key])
	//rf node's state size is bigger than maxRaftState , start the snapshot.
	if kv.rf.GetStateSize() >= kv.maxraftstate && kv.maxraftstate != -1 {
		w := new(bytes.Buffer)
		encoder := labgob.NewEncoder(w)
		if err := encoder.Encode(kv.dedup); err != nil {
			panic(err)
		}
		if err := encoder.Encode(kv.kv); err != nil {
			panic(err)
		}
		kv.rf.Snapshot(v.CommandIndex, w.Bytes())
	}
}

func (kv *KVServer) DoApply() {
	for v := range kv.applyCh {
		if kv.killed() {
			return
		}

		if v.CommandValid {
			kv.apply(v)
			if _, isLeader := kv.rf.GetState(); !isLeader {
				continue
			}
			kv.mu.Lock()
			switch args := v.Command.(type) {
			case GetArgs:
				getCh := kv.get[v.CommandIndex]
				var value string
				if s, ok := kv.kv[args.Key]; ok {
					value = s
				}
				kv.mu.Unlock()
				go func() {
					getCh <- value
				}()
			case PutAppendArgs:
				putCh := kv.done[v.CommandIndex]
				kv.mu.Unlock()
				go func() {
					putCh <- struct{}{}
				}()
			}
		} else {
			if ok := kv.rf.CondInstallSnapshot(v.SnapshotTerm, v.SnapshotIndex, v.Snapshot); ok {
				kv.lastApplied = v.SnapshotIndex
				kv.readSnapshot(v.Snapshot)
			}
		}
	}
}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := *args
	i, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan string, 1)
	kv.mu.Lock()
	kv.get[i] = ch
	kv.mu.Unlock()
	select {
	case v := <-ch:
		reply.Value = v
		reply.Err = OK
		return
	case <-time.After(DefaultTimeOut):
		reply.Err = ErrTimeOut
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := *args
	i, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan struct{}, 1)
	kv.mu.Lock()
	kv.done[i] = ch
	kv.mu.Unlock()
	select {
	case <-ch:
		reply.Err = OK
		return
	case <-time.After(DefaultTimeOut):
		reply.Err = ErrTimeOut
		return
	}
}

// Kill the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.serverLen = len(servers)
	kv.get = make(map[int]chan string)
	kv.done = make(map[int]chan struct{})

	kv.readSnapshot(persister.ReadSnapshot())
	go kv.DoApply()

	return kv
}
