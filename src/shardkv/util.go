package shardkv

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type logTopic string

const (
	dClerk     logTopic = "CLERK"
	dSKServer  logTopic = "SERVER"
	dRaft      logTopic = "RAFT"
	dConfig    logTopic = "CONFIG"
	dSnap      logTopic = "SNAPSHOT"
	dKVStorage logTopic = "KVSTORAGE"
	DTest      logTopic = "TEST"
	DMoveShard logTopic = "MOVESHARD"
	DBug       logTopic = "BUG"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	// debugVerbosity = getVerbosity()
	debugVerbosity = 1
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		// if topic != dConfig {
		// 	return
		// }
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

// 在SCServer中，只有Leader才打印信息
func (kv *ShardKV) DebugLeader(topic logTopic, format string, a ...interface{}) {
	_, isLeader := kv.rf.GetState()
	if debugVerbosity >= 1 && isLeader {
		// if debugVerbosity >= 1 {
		timeInterval := time.Since(debugStart).Microseconds()
		timeInterval /= 100
		prefix := fmt.Sprintf("%06d %v G%v ", timeInterval, string(topic), kv.gid)
		// timestamp := time.Now().Format("2006-01-02 15:04:05.000")
		// prefix := fmt.Sprintf("%s %06d %v ", timestamp, timeInterval, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
