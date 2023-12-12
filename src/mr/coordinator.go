package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	// InReduce 当前系统所处的mr阶段
	InReduce    = 1
	InMap       = 2
	AllFinished = 3
	// JobTimeoutSecond 任务的超时时间，超过 xx 当做客户端crash了，重新分配任务。 如果设置的太小，则 map count 检测程序不通过。
	JobTimeoutSecond = 5
	// MapJob 任务类型
	MapJob    = 1
	ReduceJob = 2
	// BackgroundInterval 后台线程的运行间隔
	BackgroundInterval = 500 * time.Millisecond
	// MasterLogPrefix 日志前缀
	MasterLogPrefix = "master_log: "
	WorkerLogPrefix = "worker_log: "
)

// Coordinator 协调者，也称服务端，Master
type Coordinator struct {
	// Your definitions here.
	Files         []string   // 原始输入的文件列表
	ReduceNumber  int        //  Reduce的数量，需要显示指定。而Map任务的数量就是 files 的长度
	MapJobList    []*Job     // 分发给 worker 的 map 任务
	ReduceJobList []*Job     // 分发给 worker 的 reduce 任务
	JobListMutex  sync.Mutex // 访问or修改任务状态时要加锁 可以进一步减少锁的粒度   服务端的全局锁
	CurrentStates int32      // 所处的阶段枚举， 1-map阶段、2-reduce阶段、3-都完成[程序可退出]
}

// Job 任务信息
type Job struct {
	// 静态信息
	FileName     string // map任务输入文件名
	ListIndex    int    // 在任务列表中的 index 下标
	ReduceID     int    // reduce 任务号  从0-(N-1)  和上面一个属性重复了
	ReduceNumber int    // reduce 数量个数，分区用到
	JobType      int    // 任务类型  map 还是 reduce
	// 动态信息
	JobFinished    bool  // 任务是否正确完成
	StartTime      int64 // 任务的分配时间[初始为0]，如果下次检查是否可分配时超过2s，就当失败处理，重新分配
	FirstStartTime int64 // 第一次被分发的时间，不管有没有完成或者crash
	FinishedTime   int64 // 最终完成的时间
	FetchCount     int   // 任务分配次数，用于统计信息
	// 规定： map产生的中间文件的名字格式是  输入文件名_分区号    reduce 产生的最终文件是 mr-out-reduce任务号
}

// InitCoordinator 初始化函数
func (m *Coordinator) InitCoordinator(files []string, nReduce int) {
	m.Files = files
	m.ReduceNumber = nReduce
	m.CurrentStates = InMap
	m.initMapJob()
}

// 初始化 Map 任务
func (m *Coordinator) initMapJob() {
	m.MapJobList = make([]*Job, 0, len(m.Files))
	for i, file := range m.Files {
		m.MapJobList = append(m.MapJobList, &Job{
			JobType:      MapJob,
			FileName:     file,
			ListIndex:    i,
			ReduceNumber: m.ReduceNumber,
		})
	}
	fmt.Printf(MasterLogPrefix+"master init finished %+v\n", toJsonString(m))
}

// JobFetch 客户端索要任务的处理函数
func (m *Coordinator) JobFetch(req *JobFetchReq, resp *JobFetchResp) error {
	m.JobListMutex.Lock()
	defer m.JobListMutex.Unlock()
	currentTime := time.Now().UnixMilli()
	// 看看有哪些没完成的任务，分配出去
	jobList := m.MapJobList
	switch m.CurrentStates {
	case AllFinished:
		return nil
	case InMap:
		jobList = m.MapJobList
	case InReduce:
		jobList = m.ReduceJobList
	}
	// 遍历所有Map/Reduce任务列表
	for _, job := range jobList {
		// 任务没完成，且是第一次运行或者之前超时了[防止重复分发]
		if !job.JobFinished && (job.StartTime == 0 || (currentTime-job.StartTime)/1000 > int64(JobTimeoutSecond)) {
			job.FetchCount++
			job.StartTime = currentTime
			// 记录第一次运行时的时间
			if job.FirstStartTime == 0 {
				job.FirstStartTime = currentTime
			}
			// 赋值给resp，即分发给请求源:客户端
			fmt.Printf(logTime()+MasterLogPrefix+"—————————————— 分发出任务:%v \n", job.ListIndex)
			resp.Job = *job
			return nil
		}
	}
	// 只要不是所有任务都是已完成状态，就让work继续等，否则无法通过early_exit测试程序
	if m.CurrentStates != AllFinished {
		resp.NeedWait = true
	}
	return nil
}

// JobDone 客户端提交任务的处理函数
func (m *Coordinator) JobDone(req *JobDoneReq, resp *JobDoneResp) error {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered:", r)
		}
	}()
	m.JobListMutex.Lock()
	defer m.JobListMutex.Unlock()
	finished := req.JobFinished
	jobType := "Map任务"
	if req.JobType == ReduceJob {
		jobType = "Reduce任务"
	}
	fmt.Printf(logTime()+MasterLogPrefix+"—————————————— 完成%s任务耗时:%v 毫秒[从第一次被分发], %v毫秒[最近一次]，分配次数%v —————————————— \n",
		jobType, time.Now().UnixMilli()-req.FirstStartTime, time.Now().UnixMilli()-req.StartTime, req.FetchCount)
	switch m.CurrentStates {
	// 将任务的状态改为status，后台定时线程会扫描此状态
	case InMap:
		m.MapJobList[req.ListIndex].JobFinished = finished
	case InReduce:
		m.ReduceJobList[req.ListIndex].JobFinished = finished
	}
	return nil
}

// Background 后台扫描，任务是否都完成，是否有卡死的任务，是否进入下一个阶段，每隔100毫秒扫描一次
func (m *Coordinator) Background() {
	for atomic.LoadInt32(&m.CurrentStates) != AllFinished {
		// 循环遍历任务
		m.JobListMutex.Lock()
		isAllJobDone := true
		leftCount := 0
		switch m.CurrentStates {
		case InMap:
			for _, job := range m.MapJobList {
				if !job.JobFinished {
					isAllJobDone = false
					leftCount++
				}
			}
			fmt.Printf(logTime()+MasterLogPrefix+"—————————————— 还剩 %v 个 map 任务\n", leftCount)
			// map 任务都做完了，流转到 reduce 状态, 而且要生成 reduce 任务
			if isAllJobDone {
				leftCount = 0
				atomic.StoreInt32(&m.CurrentStates, InReduce)
				m.generateReduceMap()
				fmt.Printf(MasterLogPrefix+"background: CurrentStates change from %v to %v\n", InMap, InReduce)
			}
		case InReduce:
			for _, job := range m.ReduceJobList {
				if !job.JobFinished {
					isAllJobDone = false
					leftCount++
				}
			}
			fmt.Printf(logTime()+MasterLogPrefix+"—————————————— 还剩 %v 个 reduce 任务\n", leftCount)
			// reduce 任务都做完了，流转到 结束 状态
			if isAllJobDone {
				atomic.StoreInt32(&m.CurrentStates, AllFinished)
				fmt.Printf(MasterLogPrefix+"background: CurrentStates change from %v to %v\n", InReduce, AllFinished)
			}
		}
		m.JobListMutex.Unlock()
		time.Sleep(5 * BackgroundInterval)
	}
}

// 生成reduce任务，调用前需要持有锁
func (m *Coordinator) generateReduceMap() {
	reduceJobList := make([]*Job, 0, m.ReduceNumber)
	for i := 0; i < m.ReduceNumber; i++ {
		reduceJobList = append(reduceJobList, &Job{
			ListIndex:    i,
			ReduceID:     i,
			ReduceNumber: m.ReduceNumber,
			JobType:      ReduceJob,
		})
	}
	m.ReduceJobList = reduceJobList
	fmt.Printf(MasterLogPrefix+" generateReduceMap finished,m.ReduceJobList: %+v\n", toJsonString(reduceJobList))
}

// 序列化一个结构体对象
func toJsonString(inter interface{}) string {
	bytes, _ := json.Marshal(inter)
	return string(bytes)
}

// Your code here -- RPC handlers for the worker to call.

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
	for atomic.LoadInt32(&c.CurrentStates) != AllFinished {
		time.Sleep(300 * time.Millisecond)
	}
	ret = true
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.InitCoordinator(files, nReduce)
	go c.Background()
	c.server()
	return &c
}
