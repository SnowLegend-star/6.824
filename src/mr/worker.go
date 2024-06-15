package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"

	// "strconv"
	"time"
)

// Map functions return a slice of KeyValue.
// 应该是KeyValue[words]="1"
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		reply := TaskRequset()
		if reply.TaskType == "map" {
			TaskMap(reply, mapf)
			TaskComplete(reply.TaskNumberMap, "map")
		}
		if reply.TaskType == "Reduce" {
			TaskReduce(reply, reducef)
			TaskComplete(reply.TaskNumnerReduce, "Reduce")
		}
		if reply.TaskType == "wait" {
			time.Sleep(time.Second)
		}
		time.Sleep(time.Second) //每个1s循环向master请求任务
	}
}

func TaskMap(reply TaskReply, mapf func(string, string) []KeyValue) { //此时worker进行map任务
	//map阶段的中间键要存储在文件中

	// intermediate := []KeyValue{}

	file_TODO := reply.Filename_TODO //获得任务文件名称
	// fmt.Printf("成功获得分配任务:%v\n", file_TODO)

	// intermediate_FileName := "mr" + strconv.Itoa(reply.TaskNumber)

	file, err := os.Open(file_TODO)

	if err != nil {
		log.Fatalf("没能成功打开file_TODO: %v", file_TODO)
	}
	content, err := ioutil.ReadAll(file) //读取文件的所有内容
	// ff := func(r rune) bool { return !unicode.IsLetter(r) }
	// words := strings.FieldsFunc(contents, ff) //将文件内容分割为一个一个的单词
	if err != nil {
		log.Fatalf("没能成功读取file_TODO: %v", file_TODO)
	}
	file.Close()

	kva := mapf(file_TODO, string(content)) //下标和KeyValue类型的元素
	// intermediate = append(intermediate, kva...)
	intermediate := make([][]KeyValue, reply.NReduce)

	for _, value := range kva {
		index := ihash(value.Key) % reply.NReduce
		intermediate[index] = append(intermediate[index], value)
	}

	// sort.Sort(ByKey(intermediate))

	//创建mr-m-n的中间文件
	for index := 0; index < reply.NReduce; index++ {
		// intermediate_FileName := "mr" + strconv.Itoa(reply.TaskNumberMap) + strconv.Itoa(index) //得到中间文件的名称  //直接生成mrxx了，西巴
		intermediate_FileName := fmt.Sprintf("mr-%d-%d", reply.TaskNumberMap, index)
		intermediate_File, _ := os.Create(intermediate_FileName)
		enc := json.NewEncoder(intermediate_File)
		//没必要每次都遍历整个intermediate，不然time cost太高了
		// for key := range intermediate { //intermediate中的key是数组下标
		// 	if ihash(strconv.Itoa(key))%reply.NReduce == index {
		// 		enc.Encode(intermediate[key])
		// 	}
		// }
		for _, kv := range intermediate[index] { //intermediate中的key是数组下标
			enc.Encode(&kv)
		}
		intermediate_File.Close()
	}
}

func TaskReduce(reply TaskReply, reducef func(string, []string) string) { //此时worker进行Reduce任务
	kva := []KeyValue{} //存储Reduce生成的中间键值对
	// reply := TaskRequset()
	taskNumberReduce := reply.TaskNumnerReduce

	//先读取所有mr-m*-r文件
	for i := 0; i < reply.MMap; i++ {
		intermediate_FileName := fmt.Sprintf("mr-%d-%d", i, taskNumberReduce)
		intermediate_File, err := os.Open(intermediate_FileName)
		if err != nil {
			log.Fatalf("没能成功读取file_TODO: %v", intermediate_FileName)
		}
		// _, err = ioutil.ReadAll(intermediate_File)
		// if err != nil {
		// 	log.Fatalf("没能成功读取file_TODO: %v", intermediate_FileName)
		// }
		dec := json.NewDecoder(intermediate_File)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		intermediate_File.Close()
	}

	outputFileName := fmt.Sprintf("mr-out-%d", reply.TaskNumnerReduce)
	ofile, err := os.Create(outputFileName)
	if err != nil {
		fmt.Printf("创建输出文件: %v 失败！\n", outputFileName)
	}

	//给kva所有的键值对进行分组操作
	reduceInput := make(map[string][]string)
	for _, kv := range kva {
		reduceInput[kv.Key] = append(reduceInput[kv.Key], kv.Value)
	}

	//调用Reducee来处理每个key
	for key, values := range reduceInput {
		result := reducef(key, values)
		//用Fprintf函数写入文件
		fmt.Fprintf(ofile, "%v %v\n", key, result)
	}
	ofile.Close() //记得及时关闭文件
}

func TaskComplete(tasknumber int, tasktype string) { //完成了master分配的任务
	//这次通信告诉master任务完成，同时告知master不用分配任务
	args := TaskCompleteArgs{
		TaskCompleteNumber: tasknumber,
		TaskCompleteType:   tasktype}
	reply := TaskCompleteReply{}

	ok := call("Coordinator.TaskComplete_Handeler", &args, &reply) //这里应该是Coordinator.xxx
	if ok {
		// fmt.Println("成功向master发送任务完成的消息!")
		// fmt.Printf("成功向master发送任务完成的消息! master返回:%v\n", reply.Copy_that)
	} else {
		fmt.Println("未能向master发送任务完成的消息!")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func TaskRequset() TaskReply { //向master请求任务
	args := TaskArgs{}
	reply := TaskReply{}

	ok := call("Coordinator.TaskAssign", &args, &reply)

	if ok {
		// fmt.Printf("The file to be deal with is %v\n", reply.Filename_TODO)
		// return reply.filename_TODO
	} else {
		// fmt.Printf("Task request failed!\n")
		// os.Exit(1)
		fmt.Println("No more file can be assigned!")
	}

	return reply
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
