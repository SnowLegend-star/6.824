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
	dClerk    logTopic = "CLERK"
	dSKServer logTopic = "SERVER"
	dRaft     logTopic = "RAFT"
	dConfig   logTopic = "CONFIG"
	dSnap     logTopic = "SNAPSHOT"
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
		if topic != dConfig {
			return
		}
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
		if topic != dConfig {
			return
		}
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
