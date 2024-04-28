package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type KeyValue struct {
	Key   string
	Value string
}

func WriteFile(filename string, r io.Reader) (err error) {
	f, err := ioutil.TempFile(".", filename)
	if err != nil {
		return fmt.Errorf("cannot create temp file: %v", err)
	}
	defer func() {
		if err != nil {
			os.Remove(f.Name())
		}
	}()
	defer f.Close()
	name := f.Name()
	if _, err := io.Copy(f, r); err != nil {
		return fmt.Errorf("cannot write data to tempfile %q: %v", name, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("can't close tempfile %q: %v", name, err)
	}
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
	} else if err != nil {
		return err
	} else {
		if err := os.Chmod(name, info.Mode()); err != nil {
			return fmt.Errorf("can't set filemode on tempfile %q: %v", name, err)
		}
	}
	if err := os.Rename(name, filename); err != nil {
		return fmt.Errorf("cannot replace %q with tempfile %q: %v", filename, name, err)
	}
	return nil
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		response := doHeartbeat()
		switch response.TaskType {
		case MapTask:
			doMapTask(mapf, response)
		case ReduceTask:
			doReduceTask(reducef, response)
		case WaitTask:
			time.Sleep(1 * time.Second)
		case CompleteTask:
			return
		default:
			panic(fmt.Sprintf("error:TaskType %v", response.TaskType))
		}
	}
}

func doMapTask(mapf func(string, string) []KeyValue, response *HeartbeatResponse) {
	fileName := response.FileName
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	intermediates := make([][]KeyValue, response.NReduce)
	for _, kv := range kva {
		index := ihash(kv.Key) % response.NReduce
		intermediates[index] = append(intermediates[index], kv)
	}
	var wg sync.WaitGroup
	for index, intermediate := range intermediates {
		wg.Add(1)
		go func(index int, intermediate []KeyValue) {
			defer wg.Done()
			intermediateFilePath := fmt.Sprintf("mr-%d-%d", response.Id, index)
			var buf bytes.Buffer
			enc := json.NewEncoder(&buf)
			for _, kv := range intermediate {
				err := enc.Encode(kv)
				if err != nil {
					log.Fatalf("encode error: %v", err)
				}
			}
			WriteFile(intermediateFilePath, &buf)
		}(index, intermediate)
	}
	wg.Wait()
	doReport(response.Id, MapPhase)
}

func doReduceTask(reducef func(string, []string) string, response *HeartbeatResponse) {
	var kva []KeyValue
	for i := 0; i < response.NMap; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", i, response.Id)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	results := make(map[string][]string)
	for _, kv := range kva {
		results[kv.Key] = append(results[kv.Key], kv.Value)
	}
	var buf bytes.Buffer
	for key, value := range results {
		output := reducef(key, value)
		fmt.Fprintf(&buf, "%v %v\n", key, output)
	}
	outputFileName := fmt.Sprintf("mr-out-%d", response.Id)
	WriteFile(outputFileName, &buf)
	doReport(response.Id, ReducePhase)
}

func doReport(id int, phase SchedulePhase) {
	call("Coordinator.Report", &ReportRequest{id, phase}, &ReportResponse{})
}

func doHeartbeat() *HeartbeatResponse {
	response := HeartbeatResponse{}
	call("Coordinator.Heartbeat", &HeartbeatRequest{}, &response)
	return &response
}

func call(rpcname string, args interface{}, reply interface{}) bool {
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
