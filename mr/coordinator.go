@@ -0,0 +1,263 @@
package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskPhase int  
type TaskStatus int 

const (
	TaskPhase_Map    TaskPhase = 0
	TaskPhase_Reduce TaskPhase = 1
)

//任务状态
const (
	TaskStatus_New        TaskStatus = 0 
	TaskStatus_Ready      TaskStatus = 1 
	TaskStatus_Running    TaskStatus = 2 
	TaskStatus_Terminated TaskStatus = 3 
	TaskStatus_Error      TaskStatus = 4 
)

const (
	ScheduleInterval   = time.Millisecond * 500 
	MaxTaskRunningTime = time.Second * 5        
)

//任务
type Task struct {
	FileName string    
	Phase    TaskPhase 
	Seq      int      
	NMap     int      
	NReduce  int      
	Alive    bool      
}

//任务状态
type TaskState struct {
	Status    TaskStatus 
	WorkerId  int        
	StartTime time.Time  
}

type Coordinator struct {
	files      []string    
	nReduce    int         
	taskPhase  TaskPhase   
	taskStates []TaskState 
	taskChan   chan Task   
	workerSeq  int         
	done       bool        
	muLock     sync.Mutex  

}

//creat task
func (c *Coordinator) NewOneTask(seq int) Task {
	task := Task{
		FileName: "",
		Phase:    c.taskPhase,
		NMap:     len(c.files),
		NReduce:  c.nReduce,
		Seq:      seq,
		Alive:    true,
	}

	DPrintf("m:%+v, taskseq:%d, lenfiles:%d, lents:%d", c, seq, len(c.files), len(c.taskStates))

	if task.Phase == TaskPhase_Map {
		task.FileName = c.files[seq]
	}
	return task
}

//scan status and update
func (c *Coordinator) scanTaskState() {
	DPrintf("scanTaskState...")
	c.muLock.Lock()
	defer c.muLock.Unlock()

	if c.done {
		return
	}

	allDone := true
	//loop task status
	for k, v := range c.taskStates {
		switch v.Status {
		case TaskStatus_New:
			allDone = false
			c.taskStates[k].Status = TaskStatus_Ready
			c.taskChan <- c.NewOneTask(k)
		case TaskStatus_Ready:
			allDone = false
		case TaskStatus_Running:
			allDone = false
			//Overtime reallocate task
			if time.Now().Sub(v.StartTime) > MaxTaskRunningTime {
				c.taskStates[k].Status = TaskStatus_Ready
				c.taskChan <- c.NewOneTask(k)
			}
		case TaskStatus_Terminated:
		case TaskStatus_Error:
			allDone = false
			c.taskStates[k].Status = TaskStatus_Ready
			c.taskChan <- c.NewOneTask(k)
		default:
			panic("t. status err in schedule")
		}
	}

	if allDone {
		if c.taskPhase == TaskPhase_Map {
			//Reduce
			DPrintf("init ReduceTask")
			c.taskPhase = TaskPhase_Reduce
			c.taskStates = make([]TaskState, c.nReduce)
		} else {
			log.Println("finish all tasks!!!😊")
			c.done = true
		}
	}
}

//scan and update on a fixed time frame
func (c *Coordinator) schedule() {
	for !c.Done() {
		c.scanTaskState()
		time.Sleep(ScheduleInterval)
	}
}

// Your code here -- RPC handlers for the worker to call.

//Handle RPC request. Get task.
func (c *Coordinator) GetOneTask(args *TaskArgs, reply *TaskReply) error {
	task := <-c.taskChan
	reply.Task = &task

	if task.Alive {
		//change status
		c.muLock.Lock()
		if task.Phase != c.taskPhase {
			return errors.New("GetOneTask Task phase neq")
		}
		c.taskStates[task.Seq].WorkerId = args.WorkerId
		c.taskStates[task.Seq].Status = TaskStatus_Running
		c.taskStates[task.Seq].StartTime = time.Now()
		c.muLock.Unlock()
	}

	DPrintf("in get one Task, args:%+v, reply:%+v", args, reply)
	return nil
}

//Handle RPC request. Register worker.
func (c *Coordinator) RegWorker(args *RegArgs, reply *RegReply) error {
	DPrintf("worker reg!")
	c.muLock.Lock()
	defer c.muLock.Unlock()
	c.workerSeq++
	reply.WorkerId = c.workerSeq
	return nil
}

//Handle RPC request. Worker return task status.
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.muLock.Lock()
	defer c.muLock.Unlock()

	DPrintf("get report task: %+v, taskPhase: %+v", args, c.taskPhase)

	//If the stage is different or current work is assigned to other workers, don't change
	if c.taskPhase != args.Phase || c.taskStates[args.Seq].WorkerId != args.WorkerId {
		DPrintf("in report task,workerId=%v report a useless task=%v", args.WorkerId, args.Seq)
		return nil
	}

	if args.Done {
		c.taskStates[args.Seq].Status = TaskStatus_Terminated
	} else {
		c.taskStates[args.Seq].Status = TaskStatus_Error
	}

	go c.scanTaskState()
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)  // register RPC
	rpc.HandleHTTP() // send RPC service to HTTP service
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
// if all works are done, return true
//
func (c *Coordinator) Done() bool {
	c.muLock.Lock()
	defer c.muLock.Unlock()

	return c.done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:      files,
		nReduce:    nReduce,
		taskPhase:  TaskPhase_Map,
		taskStates: make([]TaskState, len(files)),
		workerSeq:  0,
		done:       false,
	}
	if len(files) > nReduce {
		c.taskChan = make(chan Task, len(files))
	} else {
		c.taskChan = make(chan Task, nReduce)
	}

	go c.schedule()
	c.server()
	DPrintf("master init")

	return &c
}