package kvraft

import (
	"time"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
)

const (
	OpPut    = "Put"
	OpAppend = "Append"
)

const (
	DefaultRetryTime = time.Millisecond * 200
	DefaultTimeOut   = time.Millisecond * 300
)

type Err string

type Args struct {
	ClientId  int64
	RequestId int64
}

// Put or Append
type PutAppendArgs struct {
	Args
	Key   string
	Value string
	Op    string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Args
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}
