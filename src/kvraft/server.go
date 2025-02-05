package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/util"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType   string
	Key      string
	Value    string
	SeqId    int64
	ClientID int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap         map[string]string           // kv数据库，用 map 简单表示
	lastSeqMap    map[int64]int64             // 请求幂等性, key是 clientID , value 是最后一个请求的Id
	lastResultMap map[int64]string            // 请求幂等性, key是 clientID , value 是最后一个请求对应的结果
	chanMap       map[int]chan CommonResponse // 使用管道，在异步 applyCh 线程 和 处理客户端请求线程 之间传输结果
	//persister *raft.Persister   // 持久化功能，因为 rf 里面的那个是小写的，导致包外取不到
}

func (kv *KVServer) CommonOp(req *CommonRequest, resp *CommonResponse) {
	kv.mu.Lock()
	if kv.lastSeqMap[req.ClientID] == req.SeqId {
		util.Warning("出现了幂等")
		resp.Value = kv.lastResultMap[req.ClientID]
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		SeqId:    req.SeqId,
		OpType:   req.Op,
		Key:      req.Key,
		Value:    req.Value,
		ClientID: req.ClientID,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		// 不是leader，直接返回
		resp.Err = ErrWrongLeader
		return
	}
	ch := make(chan CommonResponse, 1)
	kv.mu.Lock()
	kv.chanMap[index] = ch
	kv.mu.Unlock()
	// 等待 applyCh 结果
	select {
	case result := <-ch:
		resp.Value = result.Value
		//resp.Err = result.Err
	case <-time.After(1 * time.Second): // 超时，防止死锁
		util.Error("%d号server操作超时！！！  %+v", kv.me, *req)
		resp.Err = ErrFailed
	}
	// 清理 channel
	kv.mu.Lock()
	delete(kv.chanMap, index)
	kv.mu.Unlock()
	// 要保证返回的时候是 leader
	if _, isLeader = kv.rf.GetState(); !isLeader {
		resp.Err = ErrFailed
		return
	}
	util.Success("%d号server返回  index: %v req:%+v resp:%v", kv.me, index, *req, *resp)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
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
//
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
	kv.kvMap = make(map[string]string)
	kv.chanMap = make(map[int]chan CommonResponse)
	kv.lastSeqMap = make(map[int64]int64)
	kv.lastResultMap = make(map[int64]string)
	// 恢复数据
	if persister.SnapshotSize() > 0 {
		r := bytes.NewBuffer(persister.ReadSnapshot())
		d := labgob.NewDecoder(r)
		if err := d.Decode(&kv.kvMap); err != nil {
			panic("Decode失败 kvMap" + err.Error())
		}
		if err := d.Decode(&kv.lastSeqMap); err != nil {
			panic("Decode失败 lastSeqMap" + err.Error())
		}
		if err := d.Decode(&kv.lastResultMap); err != nil {
			panic("Decode失败 lastResultMap" + err.Error())
		}
	}
	go kv.BackGround()
	return kv
}

func (kv *KVServer) BackGround() {
	// 当raft层成功时，通知应用层可以返回了
	for applyMsg := range kv.applyCh {
		// 是快照，还是普通的日志，区分处理
		if applyMsg.SnapshotValid && len(applyMsg.Snapshot) != 0 {
			kv.mu.Lock()
			r := bytes.NewBuffer(applyMsg.Snapshot)
			d := labgob.NewDecoder(r)
			if err := d.Decode(&kv.kvMap); err != nil {
				panic("Decode失败 kvMap" + err.Error())
			}
			if err := d.Decode(&kv.lastSeqMap); err != nil {
				panic("Decode失败 lastSeqMap" + err.Error())
			}
			if err := d.Decode(&kv.lastResultMap); err != nil {
				panic("Decode失败 lastResultMap" + err.Error())
			}
			kv.mu.Unlock()
			return
		}
		kv.mu.Lock()
		op := applyMsg.Command.(Op)
		// 幂等
		if kv.lastSeqMap[op.ClientID] == op.SeqId {
			util.Warning("%v号机器发生幂等 req:%+v", kv.me, applyMsg)
			kv.mu.Unlock()
			continue
		}
		// 执行命令
		var reply CommonResponse
		if op.OpType == "Put" {
			kv.kvMap[op.Key] = op.Value
		} else if op.OpType == "Append" {
			kv.kvMap[op.Key] += op.Value
		} else if op.OpType == "Get" {
			reply.Value = kv.kvMap[op.Key]
		} else {
			util.Error("未知的操作类型")
		}

		// 记录结果，防止重复执行
		kv.lastSeqMap[op.ClientID] = op.SeqId
		kv.lastResultMap[op.ClientID] = kv.kvMap[op.Key]
		//util.Trace("%v号server落库 index:%v %+v", kv.me, applyMsg.CommandIndex, applyMsg)
		tempLd := false
		// 如果有等待的 channel，则通知
		if ch, exists := kv.chanMap[applyMsg.CommandIndex]; exists {
			if currentTerm, isLeader := kv.rf.GetState(); isLeader && int(applyMsg.CommandTerm) == currentTerm {
				tempLd = true
				ch <- reply
				delete(kv.chanMap, applyMsg.CommandIndex) // 清理 channel
			} else {
				util.Info("不是leader了，不要返回给客户端了 %+v", applyMsg)
			}
		}
		kv.mu.Unlock()

		// 如果日志太多，超过了参数, 需要做一次快照，保存到持久化里面
		if kv.maxraftstate != -1 && kv.rf.GetCurrentLogSize() > kv.maxraftstate {
			go func() {
				kv.mu.Lock()
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				_ = e.Encode(kv.kvMap)
				_ = e.Encode(kv.lastSeqMap)
				_ = e.Encode(kv.lastResultMap)
				data := w.Bytes()
				kv.rf.Snapshot(applyMsg.CommandIndex, data)
				util.Warning("%d号机器[是否ld:%v]做了快照 index:%v", kv.me, tempLd, applyMsg.CommandIndex)
				kv.mu.Unlock()
			}()
		}
	}
}
