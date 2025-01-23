package mr

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// JobFetchReq 获取任务的请求体
type JobFetchReq struct {
	// 不需要有东西。如果服务端需要记录客户端信息，可以传入客户端id等信息，微服务mesh层应该做的事情。
}

// JobFetchResp 获取任务的返回体
type JobFetchResp struct {
	NeedWait bool // 是否需要等待下次轮询任务， 因为服务端可能已经分发完map任务，但Map阶段还没结束[map任务正在被执行]
	Job           // 继承写法,相当于把Job里面的所有属性写到这里
}

// JobDoneReq 任务完成提交的请求体
type JobDoneReq struct {
	Job // 继承写法,相当于把Job里面的所有属性写到这里
}

// JobDoneResp 任务完成提交的返回体
type JobDoneResp struct {
	// 不需要有额外信息
}

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	startTime := int64(0)
	for true {
		startTime = time.Now().UnixNano()
		// 索要任务，得到的可能是 Map 或者 Reduce 的任务
		job := CallFetchJob()
		// 需要等待流转到 reduce
		if job.NeedWait {
			time.Sleep(5 * BackgroundInterval)
			continue
		}
		// 任务都做完了，停止循环
		if job.FetchCount == 0 {
			fmt.Println(logTime() + WorkerLogPrefix + "任务都做完了，worker退出")
			break
		}
		// 做任务
		job.DoJob(mapf, reducef)
		// 做完了，提交
		CallCommitJob(&JobDoneReq{job.Job})
		//fmt.Println(WorkerLogPrefix+"一次worker循环耗时[毫秒]:", (time.Now().UnixNano()-startTime)/1e6)
		startTime++
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

// DoJob 开始工作
func (job *Job) DoJob(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	var err error
	switch job.JobType {
	case MapJob:
		if err = job.DoMapJob(mapf); err != nil {
			fmt.Println("DoMapJob_error ", err)
		}
	case ReduceJob:
		if err = job.DoReduceJob(reducef); err != nil {
			fmt.Println("DoReduceJob_error ", err)
		}
	}
	// 成功做完修改状态
	if err == nil {
		job.JobFinished = true
	}
	fmt.Printf(WorkerLogPrefix+"DoMapJob_finished %v\n ", toJsonString(job))
}

func (job *Job) DoMapJob(mapf func(string, string) []KeyValue) error {
	// 读取文件，返回的str是文件内所有字符串
	str, err := job.ReadFile(job.FileName)
	if err != nil {
		return err
	}
	keyValueList := mapf(job.FileName, str)
	// 将Map产生的kv对进行排序，也可以自己实现
	sort.Sort(ByKey(keyValueList))
	// 开ReduceNumber个临时文件， 复写
	fileList := make([]*os.File, job.ReduceNumber)
	// 每个文件都 open 前清除一下写模式
	for i := range fileList {
		// 原始的 fileName 为 ../pg.txt, 获取其中的 pg.txt 文件名
		temp := strings.Split(job.FileName, "/")
		fileList[i], err = os.OpenFile(temp[1]+"_"+fmt.Sprint(i), os.O_WRONLY|os.O_TRUNC|os.O_CREATE, os.ModePerm)
		if err != nil {
			return err
		}
		defer fileList[i].Close()
	}
	// 遍历每个 kv 对，根据 hash 值写到对应的分区文件里面去
	for _, kv := range keyValueList {
		_, err = fileList[ihash(kv.Key)%job.ReduceNumber].WriteString(fmt.Sprintf("%v %v\n", kv.Key, kv.Value))
		if err != nil {
			return err
		}
	}
	return nil
}

func (job *Job) DoReduceJob(reducef func(string, []string) string) error {
	resFile, err := os.OpenFile("mr-out-"+fmt.Sprint(job.ReduceID), os.O_WRONLY|os.O_TRUNC|os.O_CREATE, os.ModePerm)
	if err != nil {
		return err
	}
	defer resFile.Close()
	keyValueList2 := make([][]*KeyValue, 0)
	// 先遍历一下目录内的文件， 找出 _reduceID 结尾的
	dir, err := ioutil.ReadDir(".")
	if err != nil {
		return err
	}
	kvCount := 0
	for _, file := range dir {
		// 找出 .txt_reduceID 结尾的, 读取文件并解析，加入结果集
		if strings.HasSuffix(file.Name(), fmt.Sprint(".txt_", job.ReduceID)) {
			content, err := job.ReadAndParseFile(file.Name())
			if err != nil {
				return err
			}
			keyValueList2 = append(keyValueList2, content)
			kvCount += len(content)
		}
	}
	sortedList := make([]*KeyValue, 0, 1000)
	// 对keyValueList2进行reduceNumber路归并排序, 内维已经排好
	indexList := make([]int, len(keyValueList2))
	for minI := findMinIndex(keyValueList2, indexList); minI != -1; minI = findMinIndex(keyValueList2, indexList) {
		sortedList = append(sortedList, keyValueList2[minI][indexList[minI]])
		indexList[minI]++
	}
	// 一维聚合, cv 样例中的代码
	i := 0
	for i < len(sortedList) {
		j := i + 1
		for j < len(sortedList) && sortedList[j].Key == sortedList[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, sortedList[k].Value)
		}
		output := reducef(sortedList[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(resFile, "%v %v\n", sortedList[i].Key, output)
		i = j
	}
	fmt.Printf(WorkerLogPrefix+"DoReduceJob_finished %v\n", toJsonString(job))
	return nil
}

func (job *Job) ReadFile(filename string) (string, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return "", err
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return "", err
	}
	return string(content), nil
}

// 多路归并， 找最小值
func findMinIndex(keyValueList2 [][]*KeyValue, indexList []int) int {
	// z 比 Z 的 ascii 码大
	tempStr := "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
	minI := -1
	for i, j := range indexList {
		if j < len(keyValueList2[i]) && keyValueList2[i][j].Key < tempStr {
			minI = i
			tempStr = keyValueList2[i][j].Key
		}
	}
	return minI
}

func (job *Job) ReadAndParseFile(filename string) ([]*KeyValue, error) {
	content, err := job.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	str := content
	lineList := strings.Split(strings.TrimSpace(str), "\n")
	res := make([]*KeyValue, 0, len(lineList))
	// line 格式    apple 2 3
	for _, line := range lineList {
		separatorIndex := strings.Index(line, " ")
		if separatorIndex == -1 {
			// 为什么会出现空行呢
			continue
		}
		res = append(res, &KeyValue{Key: line[0:separatorIndex], Value: line[separatorIndex+1:]})
	}
	return res, nil
}

func logTime() string {
	return fmt.Sprint((time.Now().UnixNano()/1e6)%10000, " ")
}

func CallFetchJob() JobFetchResp {
	req := JobFetchReq{}
	resp := JobFetchResp{}
	call("Coordinator.JobFetch", &req, &resp)
	//fmt.Printf(WorkerLogPrefix+"CallFetchJob job resp %+v\n", resp)
	return resp
}

func CallCommitJob(job *JobDoneReq) {
	//fmt.Printf(WorkerLogPrefix+"CallCommitJob job req %+v\n", *job)
	resp := JobDoneResp{}
	call("Coordinator.JobDone", job, &resp)
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
