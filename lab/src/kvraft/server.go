package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
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

type Op struct {
	ClientId    int64
	OperationId int64
	Type        string
	Key         string
	Value       string
}

type OpContext struct {
	ClientId    int64
	OperationId int64
	Reply       Reply
}

type Reply struct {
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied int
	notifyChs   map[int]chan OpContext
	kvStore     map[string]string
	lastOpMemo  map[int64]OpContext
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		ClientId:    args.ClientId,
		OperationId: args.OperationId,
		Type:        OpGet,
		Key:         args.Key,
	}

	kv.mu.RLock()
	if lastOp, ok := kv.isDuplicated(op); ok {
		DPrintf("server %v received duplicated Get request for clientId %v OperationId %v", kv.me, args.ClientId, args.OperationId)
		reply.Err, reply.Value = lastOp.Reply.Err, lastOp.Reply.Value
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	notifyCh := make(chan OpContext, 1)
	kv.mu.Lock()
	kv.notifyChs[index] = notifyCh
	kv.mu.Unlock()

	select {
	case opContext := <-notifyCh:
		if opContext.ClientId != args.ClientId || opContext.OperationId != args.OperationId {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err, reply.Value = opContext.Reply.Err, opContext.Reply.Value
	case <-time.After(time.Duration(Timeout) * time.Millisecond):
		reply.Err = ErrTimeout
		DPrintf("Get timeout : server %v clientId %v operationId %v", kv.me, args.ClientId, args.OperationId)
	}

	go func() {
		kv.mu.Lock()
		delete(kv.notifyChs, index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		ClientId:    args.ClientId,
		OperationId: args.OperationId,
		Type:        args.Op,
		Key:         args.Key,
		Value:       args.Value,
	}

	kv.mu.RLock()
	if lastOp, ok := kv.isDuplicated(op); ok {
		DPrintf("server %v received duplicated %v request for clientId %v OperationId %v", kv.me, args.Op, args.ClientId, args.OperationId)
		reply.Err = lastOp.Reply.Err
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	notifyCh := make(chan OpContext, 1)
	kv.mu.Lock()
	kv.notifyChs[index] = notifyCh
	kv.mu.Unlock()

	select {
	case opContext := <-notifyCh:
		if opContext.ClientId != args.ClientId || opContext.OperationId != args.OperationId {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = opContext.Reply.Err
	case <-time.After(time.Duration(Timeout) * time.Millisecond):
		reply.Err = ErrTimeout
		DPrintf("PutAppend timeout : server %v clientId %v operationId %v", kv.me, args.ClientId, args.OperationId)
	}

	go func() {
		kv.mu.Lock()
		delete(kv.notifyChs, index)
		kv.mu.Unlock()
	}()
}

// the tester calls Kill() when a KVServer instance won't
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

func (kv *KVServer) applier() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		kv.mu.Lock()
		if applyMsg.CommandValid {
			if applyMsg.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}
			op := applyMsg.Command.(Op)
			var opContext OpContext
			if lastOp, ok := kv.isDuplicated(op); ok {
				opContext = lastOp
			} else {
				opContext = kv.applyOp(op)
				kv.lastOpMemo[op.ClientId] = opContext
				kv.lastApplied = applyMsg.CommandIndex
			}
			if notifyCh, ok := kv.notifyChs[applyMsg.CommandIndex]; ok {
				notifyCh <- opContext
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

func (kv *KVServer) applyOp(op Op) OpContext {
	opContext := OpContext{
		ClientId:    op.ClientId,
		OperationId: op.OperationId,
	}
	switch op.Type {
	case OpPut:
		kv.kvStore[op.Key] = op.Value
		opContext.Reply = Reply{Err: OK}
	case OpAppend:
		kv.kvStore[op.Key] += op.Value
		opContext.Reply = Reply{Err: OK}
	case OpGet:
		reply := Reply{}
		if _, ok := kv.kvStore[op.Key]; !ok {
			reply.Err = ErrNoKey
		} else {
			reply.Value = kv.kvStore[op.Key]
			reply.Err = OK
		}
		opContext.Reply = reply
	default:
		panic("unexpected type")
	}
	return opContext
}

func (kv *KVServer) isDuplicated(op Op) (OpContext, bool) {
	if lastOp, ok := kv.lastOpMemo[op.ClientId]; ok && op.OperationId == lastOp.OperationId {
		return lastOp, true
	}
	return OpContext{}, false
}

func (kv *KVServer) needSnapshot() bool {
	DPrintf("server %v RaftStateSize %v", kv.me, kv.rf.RaftStateSize())
	if kv.maxraftstate != -1 && kv.rf.RaftStateSize() >= kv.maxraftstate {
		return true
	}
	return false
}

func (kv *KVServer) takeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvStore)
	e.Encode(kv.lastOpMemo)
	e.Encode(kv.lastApplied)

	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *KVServer) applySnapshots(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvStore map[string]string
	var lastOpMemo map[int64]OpContext
	var lastApplied int
	if d.Decode(&kvStore) != nil || d.Decode(&lastOpMemo) != nil || d.Decode(&lastApplied) != nil {
		DPrintf("server %d decode error", kv.me)
	} else {
		kv.kvStore = kvStore
		kv.lastOpMemo = lastOpMemo
		kv.lastApplied = lastApplied
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log.
// if maxraftstate is -1, you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvStore = make(map[string]string)
	kv.notifyChs = make(map[int]chan OpContext)
	kv.lastOpMemo = make(map[int64]OpContext)
	kv.lastApplied = 0
	kv.applySnapshots(persister.ReadSnapshot())

	go kv.applier()
	return kv
}
