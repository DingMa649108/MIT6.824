@@ -0,0 +1,235 @@
package shardkv

import (
	"6.824/shardctrler"
	"time"
)

func (kv *ShardKV) OutputDataExist(configNum int, shardId int) bool {
	if _, ok := kv.outputShards[configNum]; ok {
		if _, ok = kv.outputShards[configNum][shardId]; ok {
			return true
		}
	}
	return false
}

func (kv *ShardKV) FetchShardData(args *FetchShardDataArgs, reply *FetchShardDataReply) {
	kv.log("get req fetchsharddata:args:%+v, reply:%+v", args, reply)
	defer kv.log("resp fetchsharddata:args:%+v, reply:%+v", args, reply)
	kv.lock("fetchShardData")
	defer kv.unlock("fetchShardData")

	if args.ConfigNum >= kv.config.Num {
		return
	}

	reply.Success = false
	if configData, ok := kv.outputShards[args.ConfigNum]; ok {
		if shardData, ok := configData[args.ShardNum]; ok {
			reply.Success = true
			reply.Data = make(map[string]string)
			reply.CommandIndexes = make(map[int64]int64)
			for k, v := range shardData.Data {
				reply.Data[k] = v
			}
			for k, v := range shardData.CommandIndexes {
				reply.CommandIndexes[k] = v
			}
		}
	}
	return

}

func (kv *ShardKV) CleanShardData(args *CleanShardDataArgs, reply *CleanShardDataReply) {
	kv.log("get req CleanShardData:args:%+v, reply:%+v", args, reply)
	defer kv.log("resp CleanShardData:args:%+v, reply:%+v", args, reply)
	kv.lock("cleanShardData")

	if args.ConfigNum >= kv.config.Num {
		kv.unlock("cleanShardData")
		return
	}
	kv.unlock("cleanShardData")
	_, _, isLeader := kv.rf.Start(*args)
	if !isLeader {
		return
	}

	// 简单处理下。。。
	for i := 0; i < 10; i++ {
		kv.lock("cleanShardData")
		exist := kv.OutputDataExist(args.ConfigNum, args.ShardNum)
		kv.unlock("cleanShardData")
		if !exist {
			reply.Success = true
			return
		}
		time.Sleep(time.Millisecond * 20)
	}

	//采用下面这种方式获取start结果，其实会慢一些，还会出现锁的问题
	//kv.lock("CleanShardData")
	//ch := make(chan struct{}, 1)
	//kv.cleanOutputDataNotifyCh[fmt.Sprintf("%d%d", args.ConfigNum, args.ShardNum)] = ch
	//kv.unlock("CleanShardData")
	//t := time.NewTimer(WaitCmdTimeOut)
	//defer t.Stop()
	//
	//select {
	//case <-t.C:
	//case <-ch:
	//case <-kv.stopCh:
	//}
	//
	//kv.lock("removeCh")
	////删除ch
	//if _, ok := kv.cleanOutputDataNotifyCh[fmt.Sprintf("%d%d", args.ConfigNum, args.ShardNum)]; ok {
	//	delete(kv.cleanOutputDataNotifyCh, fmt.Sprintf("%d%d", args.ConfigNum, args.ShardNum))
	//}
	////判断是否还存在
	//exist := kv.OutputDataExist(args.ConfigNum, args.ShardNum)
	//kv.unlock("removeCh")
	//if !exist {
	//	reply.Success = true
	//}
	return

}

func (kv *ShardKV) fetchShards() {
	for {
		select {
		case <-kv.stopCh:
			return
		case <-kv.pullShardsTimer.C:
			_, isLeader := kv.rf.GetState()
			if isLeader {
				kv.lock("pullshards")
				for shardId, _ := range kv.inputShards {
					go kv.fetchShard(shardId, kv.oldConfig)
				}
				kv.unlock("pullshards")
			}
			kv.pullShardsTimer.Reset(PullShardsInterval)

		}
	}
}

func (kv *ShardKV) fetchShard(shardId int, config shardctrler.Config) {
	args := FetchShardDataArgs{
		ConfigNum: config.Num,
		ShardNum:  shardId,
	}

	t := time.NewTimer(CallPeerFetchShardDataTimeOut)
	defer t.Stop()

	for {
		for _, s := range config.Groups[config.Shards[shardId]] {
			reply := FetchShardDataReply{}
			srv := kv.make_end(s)
			done := make(chan bool, 1)
			go func(args *FetchShardDataArgs, reply *FetchShardDataReply) {
				done <- srv.Call("ShardKV.FetchShardData", args, reply)
			}(&args, &reply)

			t.Reset(CallPeerFetchShardDataTimeOut)

			select {
			case <-kv.stopCh:
				return
			case <-t.C:
			case isDone := <-done:
				if isDone && reply.Success == true {
					kv.lock("pullShard")
					if _, ok := kv.inputShards[shardId]; ok && kv.config.Num == config.Num+1 {
						replyCopy := reply.Copy()
						mergeShardData := MergeShardData{
							ConfigNum:      args.ConfigNum,
							ShardNum:       args.ShardNum,
							Data:           replyCopy.Data,
							CommandIndexes: replyCopy.CommandIndexes,
						}
						kv.log("pullShard get data:%+v", mergeShardData)
						kv.unlock("pullShard")
						kv.rf.Start(mergeShardData)
						return
					} else {
						kv.unlock("pullshard")
					}
				}
			}

		}
	}

}


func (kv *ShardKV) callPeerCleanShardData(config shardctrler.Config, shardId int) {
	args := CleanShardDataArgs{
		ConfigNum: config.Num,
		ShardNum:  shardId,
	}

	t := time.NewTimer(CallPeerCleanShardDataTimeOut)
	defer t.Stop()

	for {
		for _, group := range config.Groups[config.Shards[shardId]] {
			reply := CleanShardDataReply{}
			srv := kv.make_end(group)
			done := make(chan bool, 1)

			go func(args *CleanShardDataArgs, reply *CleanShardDataReply) {
				done <- srv.Call("ShardKV.CleanShardData", args, reply)
			}(&args, &reply)

			t.Reset(CallPeerCleanShardDataTimeOut)

			select {
			case <-kv.stopCh:
				return
			case <-t.C:
			case isDone := <-done:
				if isDone && reply.Success == true {
					return
				}
			}

		}
		kv.lock("callPeerCleanShardData")
		if kv.config.Num != config.Num+1 || len(kv.inputShards) == 0 {
			kv.unlock("callPeerCleanShardData")
			break
		}
		kv.unlock("callPeerCleanShardData")
	}
}