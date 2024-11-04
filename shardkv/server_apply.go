@@ -0,0 +1,259 @@
package shardkv

import (
	"6.824/shardctrler"
)

func (kv *ShardKV) notifyWaitCommand(reqId int64, err Err, value string) {
	if ch, ok := kv.commandNotifyCh[reqId]; ok {
		ch <- CommandResult{
			Err:   err,
			Value: value,
		}
	}
}

func (kv *ShardKV) getValueByKey(key string) (err Err, value string) {
	if v, ok := kv.data[key2shard(key)][key]; ok {
		err = OK
		value = v
	} else {
		err = ErrNoKey
		value = ""
	}
	return
}

func (kv *ShardKV) ProcessKeyReady(configNum int, key string) Err {
	if configNum == 0 || configNum != kv.config.Num {
		kv.log("process key ready err config.")
		return ErrWrongGroup
	}
	shardId := key2shard(key)
	if _, ok := kv.meShards[shardId]; !ok {
		kv.log("process key ready err shard.")
		return ErrWrongGroup
	}
	if _, ok := kv.inputShards[shardId]; ok {
		kv.log("process key ready err waitShard.")
		return ErrWrongGroup
	}
	return OK
}

func (kv *ShardKV) handleApplyCh() {
	for {
		select {
		case <-kv.stopCh:
			kv.log("get from stopCh,server-%v stop!", kv.me)
			return
		case cmd := <-kv.applyCh:
			if cmd.SnapshotValid {
				kv.log("%v get install sn,%v %v", kv.me, cmd.SnapshotIndex, cmd.SnapshotTerm)
				kv.lock("waitApplyCh_sn")
				kv.readPersist(false, cmd.SnapshotTerm, cmd.SnapshotIndex, cmd.Snapshot)
				kv.unlock("waitApplyCh_sn")
				continue
			}
			if !cmd.CommandValid {
				continue
			}
			cmdIdx := cmd.CommandIndex
			if op, ok := cmd.Command.(Op); ok {
				kv.handleOpCommand(cmdIdx, op)
			} else if config, ok := cmd.Command.(shardctrler.Config); ok {
				kv.handleConfigCommand(cmdIdx, config)
			} else if mergeData, ok := cmd.Command.(MergeShardData); ok {
				kv.handleMergeShardDataCommand(cmdIdx, mergeData)
			} else if cleanData, ok := cmd.Command.(CleanShardDataArgs); ok {
				kv.handleCleanShardDataCommand(cmdIdx, cleanData)
			} else {
				panic("apply command,NOT FOUND COMMDN！")
			}

		}

	}

}

func (kv *ShardKV) handleOpCommand(cmdIdx int, op Op) {
	kv.log("start apply command %v：%+v", cmdIdx, op)
	kv.lock("handleApplyCh")
	defer kv.unlock("handleApplyCh")
	shardId := key2shard(op.Key)
	if err := kv.ProcessKeyReady(op.ConfigNum, op.Key); err != OK {
		kv.notifyWaitCommand(op.ReqId, err, "")
		return
	}
	if op.Method == "Get" {
		e, v := kv.getValueByKey(op.Key)
		kv.notifyWaitCommand(op.ReqId, e, v)
	} else if op.Method == "Put" || op.Method == "Append" {
		isRepeated := false
		if v, ok := kv.lastApplies[shardId][op.ClientId]; ok {
			if v == op.CommandId {
				isRepeated = true
			}
		}

		if !isRepeated {
			switch op.Method {
			case "Put":
				kv.data[shardId][op.Key] = op.Value
				kv.lastApplies[shardId][op.ClientId] = op.CommandId
			case "Append":
				e, v := kv.getValueByKey(op.Key)
				if e == ErrNoKey {
					kv.data[shardId][op.Key] = op.Value
					kv.lastApplies[shardId][op.ClientId] = op.CommandId
				} else {
					kv.data[shardId][op.Key] = v + op.Value
					kv.lastApplies[shardId][op.ClientId] = op.CommandId
				}
			default:
				panic("unknown method " + op.Method)
			}

		}
		kv.notifyWaitCommand(op.ReqId, OK, "")
	} else {
		panic("unknown method " + op.Method)
	}

	kv.log("apply op: cmdId:%d, op: %+v, data:%v", cmdIdx, op, kv.data[shardId][op.Key])
	kv.saveSnapshot(cmdIdx)
}

func (kv *ShardKV) handleConfigCommand(cmdIdx int, config shardctrler.Config) {
	kv.log("start handle config %v：%+v", cmdIdx, config)
	kv.lock("handleApplyCh")
	defer kv.unlock("handleApplyCh")
	if config.Num <= kv.config.Num {
		kv.saveSnapshot(cmdIdx)
		return
	}

	if config.Num != kv.config.Num+1 {
		panic("applyConfig err")
	}

	oldConfig := kv.config.Copy()
	outputShards := make([]int, 0, shardctrler.NShards)
	inputShards := make([]int, 0, shardctrler.NShards)
	meShards := make([]int, 0, shardctrler.NShards)

	for i := 0; i < shardctrler.NShards; i++ {
		if config.Shards[i] == kv.gid {
			meShards = append(meShards, i)
			if oldConfig.Shards[i] != kv.gid {
				inputShards = append(inputShards, i)
			}
		} else {
			if oldConfig.Shards[i] == kv.gid {
				outputShards = append(outputShards, i)
			}
		}
	}

	kv.meShards = make(map[int]bool)
	for _, shardId := range meShards {
		kv.meShards[shardId] = true
	}

	d := make(map[int]MergeShardData)
	for _, shardId := range outputShards {
		mergeShardData := MergeShardData{
			ConfigNum:      oldConfig.Num,
			ShardNum:       shardId,
			Data:           kv.data[shardId],
			CommandIndexes: kv.lastApplies[shardId],
		}
		d[shardId] = mergeShardData
		kv.data[shardId] = make(map[string]string)
		kv.lastApplies[shardId] = make(map[int64]int64)
	}
	kv.outputShards[oldConfig.Num] = d

	kv.inputShards = make(map[int]bool)
	if oldConfig.Num != 0 {
		for _, shardId := range inputShards {
			kv.inputShards[shardId] = true
		}
	}

	kv.config = config
	kv.oldConfig = oldConfig
	kv.log("apply op: cmdId:%d, config:%+v", cmdIdx, config)
	kv.saveSnapshot(cmdIdx)
}

func (kv *ShardKV) handleMergeShardDataCommand(cmdIdx int, data MergeShardData) {
	kv.log("start merge Shard Data %v：%+v", cmdIdx, data)
	kv.lock("handleApplyCh")
	defer kv.unlock("handleApplyCh")
	if kv.config.Num != data.ConfigNum+1 {
		return
	}

	if _, ok := kv.inputShards[data.ShardNum]; !ok {
		return
	}

	kv.data[data.ShardNum] = make(map[string]string)
	kv.lastApplies[data.ShardNum] = make(map[int64]int64)

	for k, v := range data.Data {
		kv.data[data.ShardNum][k] = v
	}
	for k, v := range data.CommandIndexes {
		kv.lastApplies[data.ShardNum][k] = v
	}
	delete(kv.inputShards, data.ShardNum)

	kv.log("apply op: cmdId:%d, mergeShardData:%+v", cmdIdx, data)
	kv.saveSnapshot(cmdIdx)
	go kv.callPeerCleanShardData(kv.oldConfig, data.ShardNum)
}

func (kv *ShardKV) handleCleanShardDataCommand(cmdIdx int, data CleanShardDataArgs) {
	kv.log("start clean shard data %v：%+v", cmdIdx, data)
	kv.lock("handleApplyCh")
	defer kv.unlock("handleApplyCh")
	if kv.OutputDataExist(data.ConfigNum, data.ShardNum) {
		delete(kv.outputShards[data.ConfigNum], data.ShardNum)
	}

	//if ch, ok := kv.cleanOutputDataNotifyCh[fmt.Sprintf("%d%d", data.ConfigNum, data.ShardNum)]; ok {
	//	ch <- struct{}{}
	//}

	kv.saveSnapshot(cmdIdx)
}