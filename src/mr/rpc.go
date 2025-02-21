package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

const (
	_map = iota
	_reduce
	_wait
	_end
)

// rpc task state
type TaskState struct {
	/*
		Declared in consts above
			0  map
			1  reduce
			2  wait
			3  end
	*/
	State       int
	MapIndex    int
	ReduceIndex int
	FileName    string
	R           int
	M           int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
