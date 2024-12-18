@@ -0,0 +1,469 @@
package shardctrler

import (
	"6.824/labgob"
	"6.824/raft"
	"sort"
	"time"
)
import "6.824/labrpc"
import "sync"

const WaitCmdTimeOut = time.Millisecond * 500 
const MaxLockTime = time.Millisecond * 10     // debug

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	stopCh          chan struct{}
	commandNotifyCh map[int64]chan CommandResult
	lastApplies     map[int64]int64 //k-v：ClientId-CommandId

	configs []Config // indexed by config num

	//用于互斥锁
	lockStartTime time.Time
	lockEndTime   time.Time
	lockMsg       string
}

type CommandResult struct {
	Err    Err
	Config Config
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ReqId     int64 
	CommandId int64
	ClientId  int64
	Args      interface{}
	Method    string
}

func (sc *ShardCtrler) lock(msg string) {
	sc.mu.Lock()
	sc.lockStartTime = time.Now()
	sc.lockMsg = msg
}

func (sc *ShardCtrler) unlock(msg string) {
	sc.lockEndTime = time.Now()
	duration := sc.lockEndTime.Sub(sc.lockStartTime)
	sc.lockMsg = ""
	sc.mu.Unlock()
	if duration > MaxLockTime {
		DPrintf("lock too long:%s:%s\n", msg, duration)
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	close(sc.stopCh)
	// Your code here, if desired.
}

func (sc *ShardCtrler) removeCh(reqId int64) {
	sc.lock("removeCh")
	defer sc.unlock("removeCh")
	delete(sc.commandNotifyCh, reqId)
}

func (sc *ShardCtrler) getConfigByIndex(idx int) Config {
	if idx < 0 || idx >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1].Copy()
	}
	return sc.configs[idx].Copy()
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

/*
rpc
*/

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	res := sc.waitCommand(args.ClientId, args.CommandId, "Join", *args)
	if res.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	reply.Err = res.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	res := sc.waitCommand(args.ClientId, args.CommandId, "Leave", *args)
	if res.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	reply.Err = res.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	res := sc.waitCommand(args.ClientId, args.CommandId, "Move", *args)
	if res.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	reply.Err = res.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	DPrintf("server %v query:args %+v", sc.me, args)

	sc.lock("query")
	if args.Num >= 0 && args.Num < len(sc.configs) {
		reply.Err = OK
		reply.WrongLeader = false
		reply.Config = sc.getConfigByIndex(args.Num)
		sc.unlock("query")
		return
	}
	sc.unlock("query")
	res := sc.waitCommand(args.ClientId, args.CommandId, "Query", *args)
	if res.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	reply.Err = res.Err
	reply.Config = res.Config
}

func (sc *ShardCtrler) waitCommand(clientId int64, commandId int64, method string, args interface{}) (res CommandResult) {
	DPrintf("server %v wait cmd start,clientId：%v,commandId: %v,method: %s,args: %+v", sc.me, clientId, commandId, method, args)
	op := Op{
		ReqId:     nrand(),
		ClientId:  clientId,
		CommandId: commandId,
		Method:    method,
		Args:      args,
	}
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		DPrintf("server %v wait cmd NOT LEADER.", sc.me)
		return
	}
	sc.lock("waitCommand")
	ch := make(chan CommandResult, 1)
	sc.commandNotifyCh[op.ReqId] = ch
	sc.unlock("waitCommand")
	DPrintf("server %v wait cmd notify,index: %v,term: %v,op: %+v", sc.me, index, term, op)

	t := time.NewTimer(WaitCmdTimeOut)
	defer t.Stop()

	select {
	case <-t.C:
		res.Err = ErrTimeout
	case res = <-ch:
	case <-sc.stopCh:
		res.Err = ErrServer
	}

	sc.removeCh(op.ReqId)
	DPrintf("server %v wait cmd end,Op: %+v.", sc.me, op)
	return

}

func (sc *ShardCtrler) adjustConfig(conf *Config) {
	if len(conf.Groups) == 0 {
		conf.Shards = [NShards]int{}
	} else if len(conf.Groups) == 1 {
		for gid, _ := range conf.Groups {
			for i, _ := range conf.Shards {
				conf.Shards[i] = gid
			}
		}
	} else if len(conf.Groups) <= NShards {
		avgShardsCount := NShards / len(conf.Groups)
		otherShardsCount := NShards - avgShardsCount*len(conf.Groups)
		isTryAgain := true

		for isTryAgain {
			isTryAgain = false
			DPrintf("adjust config,%+v", conf)
			var gids []int
			for gid, _ := range conf.Groups {
				gids = append(gids, gid)
			}
			sort.Ints(gids)
			for _, gid := range gids {
				count := 0
				for _, val := range conf.Shards {
					if val == gid {
						count++
					}
				}

				if count == avgShardsCount {
					continue
				} else if count > avgShardsCount && otherShardsCount == 0 {
					temp := 0
					for k, v := range conf.Shards {
						if gid == v {
							if temp < avgShardsCount {
								temp += 1
							} else {
								conf.Shards[k] = 0
							}
						}
					}
				} else if count > avgShardsCount && otherShardsCount > 0 {
					temp := 0
					for k, v := range conf.Shards {
						if gid == v {
							if temp < avgShardsCount {
								temp += 1
							} else if temp == avgShardsCount && otherShardsCount != 0 {
								otherShardsCount -= 1
							} else {
								conf.Shards[k] = 0
							}
						}
					}

				} else {
					//count < arg
					for k, v := range conf.Shards {
						if v == 0 && count < avgShardsCount {
							conf.Shards[k] = gid
							count += 1
						}
						if count == avgShardsCount {
							break
						}
					}
					if count < avgShardsCount {
						DPrintf("adjust config try again.")
						isTryAgain = true
						continue
					}
				}
			}

			cur := 0
			for k, v := range conf.Shards {
				if v == 0 {
					conf.Shards[k] = gids[cur]
					cur += 1
					cur %= len(conf.Groups)
				}
			}

		}
	} else {

		gidsFlag := make(map[int]int)
		emptyShards := make([]int, 0, NShards)
		for k, gid := range conf.Shards {
			if gid == 0 {
				emptyShards = append(emptyShards, k)
				continue
			}
			if _, ok := gidsFlag[gid]; ok {
				conf.Shards[k] = 0
				emptyShards = append(emptyShards, k)
			} else {
				gidsFlag[gid] = 1
			}
		}
		if len(emptyShards) > 0 {
			var gids []int
			for k, _ := range conf.Groups {
				gids = append(gids, k)
			}
			sort.Ints(gids)
			temp := 0
			for _, gid := range gids {
				if _, ok := gidsFlag[gid]; !ok {
					conf.Shards[emptyShards[temp]] = gid
					temp += 1
				}
				if temp >= len(emptyShards) {
					break
				}
			}

		}
	}
}

func (sc *ShardCtrler) handleJoinCommand(args JoinArgs) {
	conf := sc.getConfigByIndex(-1)
	conf.Num += 1

	for k, v := range args.Servers {
		conf.Groups[k] = v
	}

	sc.adjustConfig(&conf)
	sc.configs = append(sc.configs, conf)
}

func (sc *ShardCtrler) handleLeaveCommand(args LeaveArgs) {
	conf := sc.getConfigByIndex(-1)
	conf.Num += 1

	for _, gid := range args.GIDs {
		delete(conf.Groups, gid)
		for i, v := range conf.Shards {
			if v == gid {
				conf.Shards[i] = 0
			}
		}
	}

	sc.adjustConfig(&conf)
	sc.configs = append(sc.configs, conf)
}

func (sc *ShardCtrler) handleMoveCommand(args MoveArgs) {
	conf := sc.getConfigByIndex(-1)
	conf.Num += 1
	conf.Shards[args.Shard] = args.GID
	sc.configs = append(sc.configs, conf)
}

func (sc *ShardCtrler) notifyWaitCommand(reqId int64, err Err, conf Config) {
	if ch, ok := sc.commandNotifyCh[reqId]; ok {
		ch <- CommandResult{
			Err:    err,
			Config: conf,
		}
	}
}

func (sc *ShardCtrler) handleApplyCh() {
	for {
		select {
		case <-sc.stopCh:
			DPrintf("get from stopCh,server-%v stop!", sc.me)
			return
		case cmd := <-sc.applyCh:
			if cmd.SnapshotValid {
				continue
			}
			if !cmd.CommandValid {
				continue
			}
			cmdIdx := cmd.CommandIndex
			DPrintf("server %v start apply command %v：%+v", sc.me, cmdIdx, cmd.Command)
			op := cmd.Command.(Op)
			sc.lock("handleApplyCh")

			if op.Method == "Query" {
				conf := sc.getConfigByIndex(op.Args.(QueryArgs).Num)
				sc.notifyWaitCommand(op.ReqId, OK, conf)
			} else {
				isRepeated := false
				if v, ok := sc.lastApplies[op.ClientId]; ok {
					if v == op.CommandId {
						isRepeated = true
					}
				}
				if !isRepeated {
					switch op.Method {
					case "Join":
						sc.handleJoinCommand(op.Args.(JoinArgs))
					case "Leave":
						sc.handleLeaveCommand(op.Args.(LeaveArgs))
					case "Move":
						sc.handleMoveCommand(op.Args.(MoveArgs))
					default:
						panic("unknown method")
					}
				}
				sc.lastApplies[op.ClientId] = op.CommandId
				sc.notifyWaitCommand(op.ReqId, OK, Config{})
			}

			DPrintf("apply op: cmdId:%d, op: %+v", cmdIdx, op)
			sc.unlock("handleApplyCh")
		}
	}
}

/*
初始化代码
*/

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Op{})

	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.stopCh = make(chan struct{})
	sc.commandNotifyCh = make(map[int64]chan CommandResult)
	sc.lastApplies = make(map[int64]int64)

	go sc.handleApplyCh()

	return sc
}