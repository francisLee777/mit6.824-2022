package kvraft

import (
	"6.824/labrpc"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientID      int64
	lastSeqID     int64
	currentLeader int64 // 保存一下当前leader，减少每次的轮询。
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
	// You'll have to add code here.
	ck.clientID = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	return ck.CommonOp(key, "", "Get")
}

func (ck *Clerk) CommonOp(key string, value string, op string) string {
	seqId := atomic.AddInt64(&ck.lastSeqID, 1)
	for {
		req := &CommonRequest{
			Key:      key,
			Value:    value,
			Op:       op,
			SeqId:    seqId,
			ClientID: ck.clientID,
		}
		resp := &CommonResponse{}
		if b := ck.servers[ck.currentLeader].Call("KVServer.CommonOp", req, resp); !b || resp.Err == ErrWrongLeader || resp.Err == ErrFailed {
			ck.currentLeader = (ck.currentLeader + 1) % int64(len(ck.servers))
			//time.Sleep(30 * time.Millisecond)
			//atomic.AddInt64(&ck.lastSeqID, 1)
			continue
		}
		return resp.Value
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
}

func (ck *Clerk) Put(key string, value string) {
	ck.CommonOp(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.CommonOp(key, value, "Append")
}
