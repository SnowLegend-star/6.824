package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskArgs struct {
	// Filename_WANT string
}

type TaskReply struct {
	Filename_TODO    string //待处理的任务文件
	TaskNumberMap    int    //任务文件的编号
	NReduce          int    //每次都要传递这个参数感觉有点浪费带宽了
	TaskNumnerReduce int    //返回的Reduce任务编号
	MMap             int
	TaskType         string //任务文件的类型
	// StartTime        time.Time //这个任务开始的时间
}

type TaskCompleteArgs struct {
	TaskCompleteNumber int    //完成的任务文件的编号
	TaskCompleteType   string //完成任务文件的类型
	// isComplete int //默认是0
}

type TaskCompleteReply struct {
	Copy_that string
	//master不需要返回任何信息
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
