package shardctrler

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
	dSCServer logTopic = "SERVER"
	dRaft     logTopic = "RAFT"
	dConfig   logTopic = "CONFIG"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	// debugVerbosity = getVerbosity()
	debugVerbosity = 0
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		if topic == dClerk || topic == dSCServer {
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
func (sc *ShardCtrler) DebugLeader(topic logTopic, format string, a ...interface{}) {
	_, isLeader := sc.rf.GetState()
	if debugVerbosity >= 1 && isLeader {
		if topic == dClerk || topic == dSCServer {
			return
		}
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
