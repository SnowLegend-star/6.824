package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu         sync.Mutex
	file       []string        //编号和filename都可以作为一个文件的唯一区分
	visitedM   map[string]bool //用一个映射来记录map任务所有文件map[string] bool的访问情况
	visitedR   []bool          //记录Reduce任务所有文件的访问情况
	tasknumber map[string]int  //用另一个映射来记录文件的任务编号
	NReduce    int
	// tasknumberR         map[string]int //加一个任务类型
	MMap                int   //有M个map文件
	TaskCompleteStatusM []int //记录对应的每个任务文件是否完成
	TaskCompleteStatusR []int
	TaskTickM           map[string]time.Time //记录任务的发送时间，超过10s没回复按worker进程挂了处理
	TaskTickR           map[int]time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) TaskAssign(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	if c.isTaskComplete_Map() == false { //map任务未全部完成就只能分配map任务
		for key, value := range c.visitedM { //visit[filename] bool

			//10s还没返回的任务重新置为unvisited状态
			if value == true {
				number := c.tasknumber[key]
				if c.TaskCompleteStatusM[number] == 0 { //已经分配的map任务文件还未完成
					timeEnd := time.Now()
					duration := timeEnd.Sub(c.TaskTickM[key])
					if duration > 10*time.Second {
						c.visitedM[key] = false
					}
				}
			}

			if value == false {
				reply.Filename_TODO = key
				break
			}

		}
		c.visitedM[reply.Filename_TODO] = true
		reply.TaskNumberMap = c.tasknumber[reply.Filename_TODO]
		reply.NReduce = c.NReduce
		reply.MMap = c.MMap
		reply.TaskType = "map"
		c.TaskTickM[reply.Filename_TODO] = time.Now() //master记录这个文件的发送时间

		if reply.Filename_TODO == "" { //如果待处理的文件为空——即所有文件都分配出去但是还没返回任务完成的signal
			reply.TaskType = "wait"
		}
	} else {
		for index := 0; index < c.NReduce; index++ { //Reduce任务从宏观着手
			//10s还没返回的任务重新置为unvisited状态
			if c.visitedR[index] == true {
				if c.TaskCompleteStatusR[index] == 0 {
					timeEnd := time.Now()
					duration := timeEnd.Sub(c.TaskTickR[index])
					if duration > 10*time.Second {
						c.visitedR[index] = false
					}
				}
			}

			if c.visitedR[index] == false { //寻找第一个未被访问的Reduce任务编号
				reply.TaskNumnerReduce = index
				break
			}
		}
		c.visitedR[reply.TaskNumnerReduce] = true
		reply.NReduce = c.NReduce
		reply.MMap = c.MMap
		reply.TaskType = "Reduce"
	}
	c.mu.Unlock()
	return nil
}

// 处理接收来自worker工作完成的消息
func (c *Coordinator) TaskComplete_Handeler(args *TaskCompleteArgs, reply *TaskCompleteReply) error { //这里少了个返回值error
	c.mu.Lock()
	defer c.mu.Unlock()
	// fmt.Printf("收到任务完成的请求: 类型=%v, 编号=%v\n", args.TaskCompleteType, args.TaskCompleteNumber)
	if args.TaskCompleteType == "map" {
		c.TaskCompleteStatusM[args.TaskCompleteNumber] = 1
		// fmt.Printf("map阶段的%v号任务文件: %v已经完成\n", args.TaskCompleteNumber, c.file[args.TaskCompleteNumber])
		// fmt.Printf("当前TaskCompleteStatusM内容如下:%v\n", c.TaskCompleteStatusM)
	} else {
		c.TaskCompleteStatusR[args.TaskCompleteNumber] = 1
	}
	// reply.Copy_that = "Copy that!"
	return nil
}

// 判断所有的map任务完成了没
func (c *Coordinator) isTaskComplete_Map() bool {
	ret := false

	//当有一个map任务未完成就直接退出
	flag := 1
	for index := 0; index < c.MMap; index++ {
		if c.TaskCompleteStatusM[index] == 0 {
			flag = 0
			break
		}
	}
	if flag == 1 {
		ret = true
	}
	return ret
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	//当所有Reduce任务完成后就说明整个job完成了
	flag := 1
	for index := 0; index < c.NReduce; index++ {
		if c.TaskCompleteStatusR[index] == 0 { //有一个任务未完成直接退出
			flag = 0
			break
		}
	}
	if flag == 1 {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.		Reduce要用到10个哈希buckets
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		file:       files,
		visitedM:   make(map[string]bool),
		visitedR:   make([]bool, nReduce),
		tasknumber: make(map[string]int),
		NReduce:    nReduce,
		MMap:       len(files),

		TaskCompleteStatusM: make([]int, len(files)),
		TaskCompleteStatusR: make([]int, nReduce),
		TaskTickM:           make(map[string]time.Time),
		TaskTickR:           make(map[int]time.Time),
	}

	filesTxt := files[0:]
	//初始化c的结构体
	for key, value := range filesTxt { //怎么会给coordinator传入一个.so文件呢？
		// if key == 0 {
		// 	continue
		// }
		c.tasknumber[value] = key
		c.visitedM[value] = false
		c.TaskCompleteStatusM[key] = 0
	}
	for index := 0; index < nReduce; index++ {
		c.TaskCompleteStatusR[index] = 0
	}
	// Your code here.
	// AllFiles := files

	c.server()
	return &c
}
