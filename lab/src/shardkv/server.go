package shardkv

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	makeEnd      func(string) *labrpc.ClientEnd
	gid          int
	sc           *shardctrler.Clerk
	maxRaftState int   // snapshot if log grows this big
	dead         int32 // set by Kill()

	// Your definitions here.
	lastApplied  int
	stateMachine map[int]*Shard
	preConfig    shardctrler.Config
	curConfig    shardctrler.Config
	notifyChs    map[int]chan interface{}
}

func (kv *ShardKV) execCommand(cmd *Command) *CommandResponse {
	var resp CommandResponse
	resp.Err = OK
	index, _, isLeader := kv.rf.Start(*cmd)
	if !isLeader {
		resp.Err = ErrWrongLeader
		return &resp
	}

	notifyCh := make(chan interface{}, 1)
	kv.mu.Lock()
	kv.notifyChs[index] = notifyCh
	kv.mu.Unlock()

	select {
	case resp.Result = <-notifyCh:
	case <-time.After(Timeout * time.Millisecond):
		resp.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		delete(kv.notifyChs, index)
		kv.mu.Unlock()
	}()
	return &resp
}

func (kv *ShardKV) isServing(shardN int) bool {
	if shard, ok := kv.stateMachine[shardN]; ok {
		if shard.Status == Serving || shard.Status == HelpGC {
			return true
		}
		DPrintf("ErrWrongGroup: Group %v  shard %v status %v", kv.gid, shardN, shard.Status)
	} else {
		DPrintf("ErrWrongGroup: Group %v dont have shard %v", kv.gid, shardN)
	}
	return false
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	shardN := key2shard(args.Key)
	kv.mu.RLock()
	if !kv.isServing(shardN) {
		reply.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}
	shard := kv.stateMachine[shardN]
	if shard.isDupOp(args.ClientId, args.OperationId) {
		opContext := shard.LastApplyMemo[args.ClientId]
		reply.Err, reply.Value = opContext.Err, opContext.Value
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	resp := kv.execCommand(NewGetOpCommand(args))
	if resp.Err != OK {
		reply.Err = resp.Err
		return
	}

	r := resp.Result.(*GetReply)
	reply.Err, reply.Value = r.Err, r.Value
	DPrintf("[Get] Group %v Server %v args %v reply %v", kv.gid, kv.me, *args, *reply)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	shardN := key2shard(args.Key)
	kv.mu.RLock()
	if !kv.isServing(shardN) {
		reply.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}
	shard := kv.stateMachine[shardN]
	if shard.isDupOp(args.ClientId, args.OperationId) {
		reply.Err = shard.LastApplyMemo[args.ClientId].Err
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	resp := kv.execCommand(NewPutAppendOpCommand(args))
	if resp.Err != OK {
		reply.Err = resp.Err
		return
	}

	r := resp.Result.(*PutAppendReply)
	reply.Err = r.Err

	DPrintf("[PutAppend] Group %v Server %v args %v reply %v", kv.gid, kv.me, *args, *reply)
}

func (kv *ShardKV) FetchShard(args *FetchShardsArgs, reply *FetchShardsReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	reply.Err, reply.ConfigNum = OK, kv.curConfig.Num
	if kv.curConfig.Num != args.ConfigNum {
		reply.Err = ErrWrongGroup
		return
	}

	reply.KVs = make(map[int]map[string]string)
	reply.LastApplyMemos = make(map[int]map[int64]OpContext)
	for _, sn := range args.ShardNs {
		reply.KVs[sn], reply.LastApplyMemos[sn] = kv.stateMachine[sn].deepCopyDataAndMemo()
	}
}

func (kv *ShardKV) HelpGC(args *HelpGCArgs, reply *HelpGCReply) {
	kv.mu.RLock()
	curConfigNum := kv.curConfig.Num
	kv.mu.RUnlock()
	if curConfigNum != args.ConfigNum {
		reply.Err = ErrWrongGroup
		return
	}

	resp := kv.execCommand(NewGCCommand(args))
	if resp.Err != OK {
		reply.Err = resp.Err
		return
	}
	r := resp.Result.(*HelpGCReply)
	reply.Err, reply.ConfigNum = r.Err, r.ConfigNum
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		kv.mu.Lock()
		if applyMsg.CommandValid {
			if applyMsg.CommandIndex < kv.lastApplied {
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = applyMsg.CommandIndex
			cmd := applyMsg.Command.(Command)
			result := kv.applyCmd(&cmd)
			ch, ok := kv.notifyChs[applyMsg.CommandIndex]
			if _, isLeader := kv.rf.GetState(); isLeader && ok {
				ch <- result
			}

			if kv.needSnapshot() {
				kv.takeSnapshot(applyMsg.CommandIndex)
			}
		} else if applyMsg.SnapshotValid {
			if applyMsg.SnapshotIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}
			kv.applySnapshots(applyMsg.Snapshot)
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) applyCmd(command *Command) interface{} {
	var result interface{}
	switch command.Type {
	case GetOp:
		args := command.Args.(GetArgs)
		result = kv.applyGet(&args)
	case PutAppendOp:
		args := command.Args.(PutAppendArgs)
		result = kv.applyPutAppend(&args)
	case Configuration:
		conf := command.Args.(shardctrler.Config)
		kv.applyConfiguration(&conf)
	case FetchShard:
		data := command.Args.(FetchShardsReply)
		kv.applyFetchShards(&data)
	case GC:
		args := command.Args.(HelpGCArgs)
		result = kv.applyGC(&args)
	}
	return result
}

func (kv *ShardKV) applyGet(args *GetArgs) *GetReply {
	var reply GetReply
	shardN := key2shard(args.Key)
	if kv.isServing(shardN) {
		shard := kv.stateMachine[shardN]
		if shard.isDupOp(args.ClientId, args.OperationId) {
			opContext := shard.LastApplyMemo[args.ClientId]
			reply.Err, reply.Value = opContext.Err, opContext.Value
		} else {
			value, err := shard.Get(args.Key)
			opContext := OpContext{
				OperationId: args.OperationId,
				Value:       value,
				Err:         err,
			}
			shard.LastApplyMemo[args.ClientId] = opContext
			reply.Err, reply.Value = err, value
		}
	} else {
		reply.Err = ErrWrongGroup
	}
	return &reply
}

func (kv *ShardKV) applyPutAppend(args *PutAppendArgs) *PutAppendReply {
	var reply PutAppendReply
	shardN := key2shard(args.Key)
	if kv.isServing(shardN) {
		shard := kv.stateMachine[shardN]
		if shard.isDupOp(args.ClientId, args.OperationId) {
			reply.Err = shard.LastApplyMemo[args.ClientId].Err
		} else {
			var err Err
			switch args.Type {
			case OpPut:
				err = shard.Put(args.Key, args.Value)
			case OpAppend:
				err = shard.Append(args.Key, args.Value)
			}
			opContext := OpContext{
				OperationId: args.OperationId,
				Err:         err,
			}
			shard.LastApplyMemo[args.ClientId] = opContext
			reply.Err = err
		}
	} else {
		reply.Err = ErrWrongGroup
	}
	return &reply
}

func (kv *ShardKV) applyConfiguration(conf *shardctrler.Config) {
	if conf.Num != kv.curConfig.Num+1 {
		return
	}
	DPrintf("Group %v server %v apply config %v", kv.gid, kv.me, *conf)
	for _, shard := range kv.stateMachine {
		shard.Status = Migrating
	}
	for sn, gId := range conf.Shards {
		if gId == kv.gid {
			if shard, ok := kv.stateMachine[sn]; ok {
				shard.Status = Serving
			} else {
				shard = &Shard{Fetching, make(map[string]string), make(map[int64]OpContext)}
				if kv.curConfig.Num == 0 {
					shard.Status = Serving
				}
				kv.stateMachine[sn] = shard
			}
		}
	}
	kv.preConfig = kv.curConfig
	kv.curConfig = *conf
}

func (kv *ShardKV) applyFetchShards(data *FetchShardsReply) {
	if data.ConfigNum != kv.curConfig.Num {
		return
	}
	for sn, memo := range data.LastApplyMemos {
		shard := kv.stateMachine[sn]
		if shard.Status == Fetching {
			for k, v := range data.KVs[sn] {
				shard.Data[k] = v
			}
			for k, v := range memo {
				shard.LastApplyMemo[k] = v
			}
			shard.Status = HelpGC
		}
	}
	DPrintf("Group %v Server %v fetching new shards %v", kv.me, kv.gid, *data)
}

func (kv *ShardKV) applyGC(args *HelpGCArgs) *HelpGCReply {
	var reply HelpGCReply

	for _, sn := range args.ShardNs {
		if shard, ok := kv.stateMachine[sn]; ok {
			if shard.Status == HelpGC {
				shard.Status = Serving
			} else if shard.Status == Migrating {
				delete(kv.stateMachine, sn)
				DPrintf("Group %v Server %v Shard %v has been deleted.", kv.gid, kv.me, sn)
			}
		}
	}
	reply.Err, reply.ConfigNum = OK, kv.curConfig.Num
	return &reply
}

func (kv *ShardKV) leaderCronJob(job func(), timeout time.Duration) {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			job()
		}
		time.Sleep(timeout * time.Millisecond)
	}
}

func (kv *ShardKV) fetchConfigJob() {
	shardsUpToDate := true
	kv.mu.RLock()
	for _, shard := range kv.stateMachine {
		if shard.Status != Serving {
			shardsUpToDate = false
			break
		}
	}
	curCfNum := kv.curConfig.Num
	kv.mu.RUnlock()

	if shardsUpToDate {
		conf := kv.sc.Query(curCfNum + 1)
		if conf.Num == curCfNum+1 {
			DPrintf("Group %v server %v find a new config with Num %v", kv.gid, kv.me, conf.Num)
			kv.execCommand(NewConfigurationCommand(&conf))
		}
	}
}

func (kv *ShardKV) fetchShardsJob() {
	kv.mu.RLock()
	gId2FetchingShards := kv.getGId2ShardsByStatus(Fetching)

	var wg sync.WaitGroup
	for gId, shardNs := range gId2FetchingShards {
		wg.Add(1)
		go func(servers []string, configNum int, shardNs []int) {
			defer wg.Done()
			args := FetchShardsArgs{configNum, shardNs}
			for _, server := range servers {
				var reply FetchShardsReply
				srv := kv.makeEnd(server)
				ok := srv.Call("ShardKV.FetchShard", &args, &reply)
				if ok && reply.Err == OK {
					kv.execCommand(NewFetchShardCommand(&reply))
					break
				}
			}
		}(kv.preConfig.Groups[gId], kv.curConfig.Num, shardNs)
	}
	kv.mu.RUnlock()
	wg.Wait()
}

func (kv *ShardKV) helpGCShardsJob() {
	kv.mu.RLock()
	gId2HelpGCShards := kv.getGId2ShardsByStatus(HelpGC)

	var wg sync.WaitGroup
	for gId, shardNs := range gId2HelpGCShards {
		wg.Add(1)
		go func(servers []string, configNum int, shardNs []int) {
			defer wg.Done()
			args := HelpGCArgs{configNum, shardNs}
			for _, server := range servers {
				var reply HelpGCReply
				srv := kv.makeEnd(server)
				ok := srv.Call("ShardKV.HelpGC", &args, &reply)
				if ok && reply.Err == OK {
					kv.execCommand(NewGCCommand(&args))
					break
				}
			}
		}(kv.preConfig.Groups[gId], kv.curConfig.Num, shardNs)
	}
	kv.mu.RUnlock()
	wg.Wait()
}

func (kv *ShardKV) getGId2ShardsByStatus(status ShardStatus) map[int][]int {
	res := make(map[int][]int)
	for sn, shard := range kv.stateMachine {
		if shard.Status == status {
			gId := kv.preConfig.Shards[sn]
			res[gId] = append(res[gId], sn)
		}
	}
	return res
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftState, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetArgs{})
	labgob.Register(FetchShardsReply{})
	labgob.Register(HelpGCArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxRaftState = maxRaftState
	kv.makeEnd = make_end
	kv.gid = gid
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.sc = shardctrler.MakeClerk(ctrlers)
	kv.stateMachine = make(map[int]*Shard)
	kv.notifyChs = make(map[int]chan interface{})

	kv.applySnapshots(persister.ReadSnapshot())
	go kv.leaderCronJob(kv.fetchConfigJob, Interval)
	go kv.leaderCronJob(kv.fetchShardsJob, Interval)
	go kv.leaderCronJob(kv.helpGCShardsJob, Interval)

	go kv.applier()

	return kv
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) needSnapshot() bool {
	if kv.maxRaftState != -1 && kv.rf.RaftStateSize() >= kv.maxRaftState {
		return true
	}
	return false
}

func (kv *ShardKV) takeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.preConfig)
	e.Encode(kv.curConfig)
	e.Encode(kv.stateMachine)
	e.Encode(kv.lastApplied)
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *ShardKV) applySnapshots(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var preConfig, curConfig shardctrler.Config
	var stateMachine map[int]*Shard
	var lastApplied int
	if d.Decode(&preConfig) != nil ||
		d.Decode(&curConfig) != nil ||
		d.Decode(&stateMachine) != nil ||
		d.Decode(&lastApplied) != nil {
		DPrintf("server %d decode error", kv.me)
	} else {
		kv.preConfig = preConfig
		kv.curConfig = curConfig
		kv.stateMachine = stateMachine
		kv.lastApplied = lastApplied
	}
}
