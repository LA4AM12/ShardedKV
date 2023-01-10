package shardctrler

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func copyMap(raw map[int][]string) map[int][]string {
	ret := make(map[int][]string)
	for k, v := range raw {
		ret[k] = v
	}
	return ret
}

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	// Your data here.

	configs []Config // indexed by config num

	lastApplyMemo map[int64]OpContext
	notifyChs     map[int]chan OpContext
}

type OpContext struct {
	ClientId    int64
	OperationId int64
	Err         Err
	Config      Config // for query
}

type Op struct {
	// Your data here.
	ClientId    int64
	OperationId int64
	Type        string
	Servers     map[int][]string // for join
	GIDs        []int            // for leave
	Shard       int              // for move
	GID         int              // for move
	Num         int              // for query
}

func (sc *ShardCtrler) balance(config *Config) {
	// generate gid -> shards map
	g2s := make(map[int][]int)
	for gId, _ := range config.Groups {
		g2s[gId] = []int{}
	}
	for sId, gId := range config.Shards {
		g2s[gId] = append(g2s[gId], sId)
	}

	// deterministic
	keys := make([]int, 0)
	for k := range g2s {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	for {
		var gIdWithMaxShards, gIdWithMinShards int
		max, min := 0, NShards

		for _, gId := range keys {
			shards := g2s[gId]
			if gId != 0 && len(shards) > max {
				max = len(shards)
				gIdWithMaxShards = gId
			}
			if gId != 0 && len(shards) < min {
				min = len(shards)
				gIdWithMinShards = gId
			}
		}

		// has unassigned shards
		if len(g2s[0]) > 0 {
			gIdWithMaxShards = 0
		}

		if gIdWithMaxShards != 0 && max-min <= 1 {
			break
		}

		sId := g2s[gIdWithMaxShards][0]
		g2s[gIdWithMaxShards] = g2s[gIdWithMaxShards][1:]
		g2s[gIdWithMinShards] = append(g2s[gIdWithMinShards], sId)
		config.Shards[sId] = gIdWithMinShards
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.RLock()
	if sc.isDupCmd(args.ClientId, args.OperationId) {
		reply.Err = sc.lastApplyMemo[args.ClientId].Err
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()

	op := Op{
		Type:        OpJoin,
		ClientId:    args.ClientId,
		OperationId: args.OperationId,
		Servers:     args.Servers,
	}

	idx, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	notifyCh := make(chan OpContext, 1)
	sc.mu.Lock()
	sc.notifyChs[idx] = notifyCh
	sc.mu.Unlock()

	select {
	case opContext := <-notifyCh:
		if opContext.ClientId != args.ClientId || opContext.OperationId != args.OperationId {
			reply.WrongLeader = true
			return
		}
		reply.Err = opContext.Err
	case <-time.After(time.Duration(Timeout) * time.Millisecond):
		reply.Err = TimeoutErr
		DPrintf("Join timeout : server %v clientId %v operationId %v", sc.me, args.ClientId, args.OperationId)
	}

	go func() {
		sc.mu.Lock()
		delete(sc.notifyChs, idx)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) join(servers map[int][]string) Err {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{len(sc.configs), lastConfig.Shards, copyMap(lastConfig.Groups)}
	for k, v := range servers {
		newConfig.Groups[k] = v
	}
	sc.balance(&newConfig)
	sc.configs = append(sc.configs, newConfig)
	return OK
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.RLock()
	if sc.isDupCmd(args.ClientId, args.OperationId) {
		reply.Err = sc.lastApplyMemo[args.ClientId].Err
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()

	op := Op{
		Type:        OpLeave,
		ClientId:    args.ClientId,
		OperationId: args.OperationId,
		GIDs:        args.GIDs,
	}

	idx, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	notifyCh := make(chan OpContext, 1)
	sc.mu.Lock()
	sc.notifyChs[idx] = notifyCh
	sc.mu.Unlock()

	select {
	case opContext := <-notifyCh:
		if opContext.ClientId != args.ClientId || opContext.OperationId != args.OperationId {
			reply.WrongLeader = true
			return
		}
		reply.Err = opContext.Err
	case <-time.After(time.Duration(Timeout) * time.Millisecond):
		reply.Err = TimeoutErr
		DPrintf("Leave timeout : server %v clientId %v operationId %v", sc.me, args.ClientId, args.OperationId)
	}

	go func() {
		sc.mu.Lock()
		delete(sc.notifyChs, idx)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) leave(gIds []int) Err {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{len(sc.configs), lastConfig.Shards, copyMap(lastConfig.Groups)}
	for _, gId := range gIds {
		delete(newConfig.Groups, gId)
	}
	for sId, gId := range newConfig.Shards {
		if _, ok := newConfig.Groups[gId]; !ok {
			newConfig.Shards[sId] = 0
		}
	}
	if len(newConfig.Groups) > 0 {
		sc.balance(&newConfig)
	}
	sc.configs = append(sc.configs, newConfig)
	return OK
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.RLock()
	if sc.isDupCmd(args.ClientId, args.OperationId) {
		reply.Err = sc.lastApplyMemo[args.ClientId].Err
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()

	op := Op{
		Type:        OpMove,
		ClientId:    args.ClientId,
		OperationId: args.OperationId,
		GID:         args.GID,
		Shard:       args.Shard,
	}

	idx, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	notifyCh := make(chan OpContext, 1)
	sc.mu.Lock()
	sc.notifyChs[idx] = notifyCh
	sc.mu.Unlock()

	select {
	case opContext := <-notifyCh:
		if opContext.ClientId != args.ClientId || opContext.OperationId != args.OperationId {
			reply.WrongLeader = true
			return
		}
		reply.Err = opContext.Err
	case <-time.After(time.Duration(Timeout) * time.Millisecond):
		reply.Err = TimeoutErr
		DPrintf("Move timeout : server %v clientId %v operationId %v", sc.me, args.ClientId, args.OperationId)
	}

	go func() {
		sc.mu.Lock()
		delete(sc.notifyChs, idx)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) move(shard, gId int) Err {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{len(sc.configs), lastConfig.Shards, copyMap(lastConfig.Groups)}
	newConfig.Shards[shard] = gId
	sc.configs = append(sc.configs, newConfig)
	return OK
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.RLock()
	if sc.isDupCmd(args.ClientId, args.OperationId) {
		reply.Err = sc.lastApplyMemo[args.ClientId].Err
		reply.Config = sc.lastApplyMemo[args.ClientId].Config
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()

	op := Op{
		Type:        OpQuery,
		ClientId:    args.ClientId,
		OperationId: args.OperationId,
		Num:         args.Num,
	}

	idx, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	notifyCh := make(chan OpContext, 1)
	sc.mu.Lock()
	sc.notifyChs[idx] = notifyCh
	sc.mu.Unlock()

	select {
	case opContext := <-notifyCh:
		if opContext.ClientId != args.ClientId || opContext.OperationId != args.OperationId {
			reply.WrongLeader = true
			return
		}
		reply.Err, reply.Config = opContext.Err, opContext.Config
	case <-time.After(time.Duration(Timeout) * time.Millisecond):
		reply.Err = TimeoutErr
		DPrintf("Query timeout : server %v clientId %v operationId %v", sc.me, args.ClientId, args.OperationId)
	}

	go func() {
		sc.mu.Lock()
		delete(sc.notifyChs, idx)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) query(num int) (Err, Config) {
	if num == -1 || num >= len(sc.configs) {
		return OK, sc.configs[len(sc.configs)-1]
	}
	return OK, sc.configs[num]
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) isDupCmd(clientId, opId int64) bool {
	if context, ok := sc.lastApplyMemo[clientId]; ok && context.OperationId == opId {
		return true
	}
	return false
}

func (sc *ShardCtrler) applyCmd(op Op) OpContext {
	if sc.isDupCmd(op.ClientId, op.OperationId) {
		return sc.lastApplyMemo[op.ClientId]
	}
	opContext := OpContext{
		ClientId:    op.ClientId,
		OperationId: op.OperationId,
	}
	switch op.Type {
	case OpJoin:
		opContext.Err = sc.join(op.Servers)
	case OpLeave:
		opContext.Err = sc.leave(op.GIDs)
	case OpMove:
		opContext.Err = sc.move(op.Shard, op.GID)
	case OpQuery:
		opContext.Err, opContext.Config = sc.query(op.Num)
	}
	sc.lastApplyMemo[op.ClientId] = opContext
	return opContext
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		applyMsg := <-sc.applyCh
		sc.mu.Lock()
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)
			opContext := sc.applyCmd(op)
			if notifyCh, ok := sc.notifyChs[applyMsg.CommandIndex]; ok {
				notifyCh <- opContext
			}
		}
		sc.mu.Unlock()
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastApplyMemo = make(map[int64]OpContext)
	sc.notifyChs = make(map[int]chan OpContext)

	go sc.applier()
	return sc
}
