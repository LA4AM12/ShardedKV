package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	Timeout        = 500
	OpGet          = "Get"
	OpPut          = "Put"
	OpAppend       = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	ClientId    int64
	OperationId int64
	Op          string // "Put" or "Append"
	Key         string
	Value       string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ClientId    int64
	OperationId int64
	Key         string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
