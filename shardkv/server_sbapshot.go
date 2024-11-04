@@ -0,0 +1,76 @@
package shardkv

import (
	"6.824/labgob"
	"6.824/shardctrler"
	"bytes"
	"log"
)

func (kv *ShardKV) saveSnapshot(logIndex int) {
	if kv.maxraftstate == -1 || kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.data) != nil ||
		e.Encode(kv.lastApplies) != nil ||
		e.Encode(kv.inputShards) != nil ||
		e.Encode(kv.outputShards) != nil ||
		e.Encode(kv.config) != nil ||
		e.Encode(kv.oldConfig) != nil ||
		e.Encode(kv.meShards) != nil {
		panic("gen snapshot data encode err")
	}
	data := w.Bytes()
	kv.rf.Snapshot(logIndex, data)
}

func (kv *ShardKV) readPersist(isInit bool, snapshotTerm, snapshotIndex int, data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	//if !isInit {
	//	res := kv.rf.CondInstallSnapshot(snapshotTerm, snapshotIndex, data)
	//	if !res {
	//		log.Panicln("kv read persist err in CondInstallSnapshot!")
	//		return
	//	}
	//}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvData [shardctrler.NShards]map[string]string
	var lastApplies [shardctrler.NShards]map[int64]int64
	var inputShards map[int]bool
	var outputShards map[int]map[int]MergeShardData
	var config shardctrler.Config
	var oldConfig shardctrler.Config
	var meShards map[int]bool

	if d.Decode(&kvData) != nil ||
		d.Decode(&lastApplies) != nil ||
		d.Decode(&inputShards) != nil ||
		d.Decode(&outputShards) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&oldConfig) != nil ||
		d.Decode(&meShards) != nil {
		log.Fatal("kv read persist err")
	} else {
		kv.data = kvData
		kv.lastApplies = lastApplies
		kv.inputShards = inputShards
		kv.outputShards = outputShards
		kv.config = config
		kv.oldConfig = oldConfig
		kv.meShards = meShards
	}
}