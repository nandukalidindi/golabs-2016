package mapreduce

import "container/list"
import "fmt"
import "sync"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// https://github.com/golang/go/blob/f78a4c84ac8ed44aaf331989aa32e40081fd8f13/misc/cgo/test/cthread.go
// COUNT ATOMICALLY. MUTEX STRUCT CODE IS DERIVED FROM THE LINK ABOVE
type atomicCount struct {
    sync.Mutex
    count int
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) runSpecifiedJob(args *DoJobArgs, mutexCount *atomicCount, maxJobs *int, jobTypeChannel chan int) {
    // READ FROM THE REGISTERED CHANNEL
    worker := <-mr.registerChannel

    var reply DoJobReply
    ok := call(worker, "Worker.DoJob", args, &reply)
    if ok == true {
        // ************** ATOMIC COUNT **************
        mutexCount.Lock()    	
        mutexCount.count += 1        
        if mutexCount.count == *maxJobs {
            // CLOSE THE CHANNEL ONCE ALL JOBS ARE PROCESSED TO RESUME WITH OTHER JOB TYPES
        	close(jobTypeChannel)
        } 
        mutexCount.Unlock()
        // ************** ATOMIC COUNT **************

        mr.registerChannel <- worker              
    } else {
        delete(mr.Workers, worker)
        fmt.Printf("WORKER %s DIED!\n", worker)        
        // Retry job with a different worker
        mr.runSpecifiedJob(args, mutexCount, maxJobs, jobTypeChannel)
    }
}

func (mr *MapReduce) RunMaster() *list.List {
    // Your code here
    // ************** MAP JOBS **************
    var mapAtomicCount atomicCount
    mapChannel := make(chan int)

    for i := 0; i < mr.nMap; i++ {
        args := &DoJobArgs{mr.file, Map, i, mr.nReduce}
        // FIRE MAP JOBS IN THEIR OWN GO ROUTINES
        go mr.runSpecifiedJob(args, &mapAtomicCount, &mr.nMap, mapChannel)    
    }
    <-mapChannel
    // ************** MAP JOBS **************

    // ************** REDUCE JOBS **************
    var reduceAtomicCount atomicCount
    reduceChannel := make(chan int)

    for i := 0; i < mr.nReduce; i++ {
        args := &DoJobArgs{mr.file, Reduce, i, mr.nMap}
        // FIRE REDUCE JOBS IN THEIR OWN GO ROUTINES
        go mr.runSpecifiedJob(args, &reduceAtomicCount, &mr.nReduce, reduceChannel)
    }
    <-reduceChannel
    // ************** REDUCE JOBS **************

    return mr.KillWorkers()
}