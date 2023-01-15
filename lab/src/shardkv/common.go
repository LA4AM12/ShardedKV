package shardkv

import "6.824/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK               = "OK"
	ErrNoKey         = "ErrNoKey"
	ErrWrongGroup    = "ErrWrongGroup"
	ErrWrongLeader   = "ErrWrongLeader"
	ErrTimeout       = "ErrTimeout"
	ErrShardNotExist = "ErrShardNotExist"
	Timeout          = 500
	Interval         = 80
	OpPut            = "Put"
	OpAppend         = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	ClientId    int64
	OperationId int64
	Type        string // "Put" or "Append"
	Key         string
	Value       string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ClientId    int64
	OperationId int64
	Key         string
}

type GetReply struct {
	Err   Err
	Value string
}

type FetchShardsArgs struct {
	ConfigNum int
	ShardNs   []int
}

type FetchShardsReply struct {
	Err            Err
	ConfigNum      int
	KVs            map[int]map[string]string
	LastApplyMemos map[int]map[int64]OpContext
}

type HelpGCArgs struct {
	ConfigNum int
	ShardNs   []int
}

type HelpGCReply struct {
	Err       Err
	ConfigNum int
}

type CommandType uint8

const (
	GetOp CommandType = iota
	PutAppendOp
	Configuration
	FetchShard
	GC
)

type Command struct {
	Type CommandType
	Args interface{}
}

type CommandResponse struct {
	Err    Err
	Result interface{}
}

func NewGetOpCommand(args *GetArgs) *Command {
	return &Command{GetOp, *args}
}

func NewPutAppendOpCommand(args *PutAppendArgs) *Command {
	return &Command{PutAppendOp, *args}
}

func NewConfigurationCommand(config *shardctrler.Config) *Command {
	return &Command{Configuration, *config}
}

func NewFetchShardCommand(data *FetchShardsReply) *Command {
	return &Command{FetchShard, *data}
}

func NewGCCommand(args *HelpGCArgs) *Command {
	return &Command{GC, *args}
}

type OpContext struct {
	OperationId int64
	Err         Err
	Value       string
}

type ShardStatus uint8

const (
	Serving ShardStatus = iota
	Fetching
	Migrating
	HelpGC
)

type Shard struct {
	Status        ShardStatus
	Data          map[string]string
	LastApplyMemo map[int64]OpContext
}

func (shard *Shard) isDupOp(clientId, opId int64) bool {
	if context, ok := shard.LastApplyMemo[clientId]; ok && context.OperationId == opId {
		return true
	}
	return false
}

func (shard *Shard) Get(key string) (string, Err) {
	if value, ok := shard.Data[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (shard *Shard) Put(key, value string) Err {
	shard.Data[key] = value
	return OK
}

func (shard *Shard) Append(key, value string) Err {
	shard.Data[key] += value
	return OK
}

func (shard *Shard) deepCopyDataAndMemo() (map[string]string, map[int64]OpContext) {
	data := make(map[string]string)
	memo := make(map[int64]OpContext)
	for k, v := range shard.Data {
		data[k] = v
	}
	for k, v := range shard.LastApplyMemo {
		memo[k] = v
	}
	return data, memo
}
