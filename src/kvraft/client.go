package kvraft

import (
	"mit6.824/labrpc"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int64
	clientId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	return ck
}

// Get fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	var args = GetArgs{
		Key: key,
	}
	leaderId := atomic.LoadInt64(&ck.leaderId)
	for {
		var reply GetReply
		if ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply); ok {
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrTimeOut {
				continue
			}
		}
		nextLeaderId := (leaderId + 1) % int64(len(ck.servers))
		atomic.StoreInt64(&leaderId, nextLeaderId)
		time.Sleep(DefaultRetryTime)
	}
}

// PutAppend shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	//with rand command id to solve Idempotency problem
	args := PutAppendArgs{
		Args: Args{
			ClientId:  ck.clientId,
			RequestId: nrand(),
		},
		Key:   key,
		Value: value,
		Op:    op,
	}
	leaderId := atomic.LoadInt64(&ck.leaderId)
	for {
		var reply PutAppendReply
		if ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply); ok {
			if reply.Err == OK {
				return
			} else if reply.Err == ErrTimeOut {
				continue
			}
		}
		nextLeaderId := (leaderId + 1) % int64(len(ck.servers))
		atomic.StoreInt64(&leaderId, nextLeaderId)
		time.Sleep(DefaultRetryTime)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}
