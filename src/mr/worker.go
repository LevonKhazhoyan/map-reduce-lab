package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type KeyValue struct {
	Key   string
	Value string
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for true {
		reply := CallForTask(MsgForTask, "")
		if reply.TaskType == "" {
			break
		}
		switch reply.TaskType {
		case "map":
			mapInWorker(&reply, mapf)
		case "reduce":
			reduceInWorker(&reply, reducef)
		}
	}
}

func mapInWorker(reply *MyReply, mapf func(string, string) []KeyValue) {
	file, err := os.Open(reply.Filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}

	kva := mapf(reply.Filename, string(content))
	kvas := Partition(kva, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		filename := WriteToJSONFile(kvas[i], reply.MapNumMapped, i)
		_ = SendInterFiles(MsgForInterFileLoc, filename, i)
	}
	_ = CallForTask(MsgForFinishMap, reply.Filename)
}

func reduceInWorker(reply *MyReply, reducef func(string, []string) string) {
	var intermediate []KeyValue
	for _, v := range reply.ReduceFileList {
		file, err := os.Open(v)
		defer func(file *os.File) {
			err := file.Close()
			if err != nil {

			}
		}(file)
		if err != nil {
			log.Fatalf("cannot open %v", v)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if dec.Decode(&kv) != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(reply.ReduceNumMapped)
	openedFile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		_, _ = fmt.Fprintf(openedFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	_ = CallForTask(MsgForFinishReduce, strconv.Itoa(reply.ReduceNumMapped))
}

func CallForTask(msgType int, msgCnt string) MyReply {
	args := MyArgs{}
	args.MessageType = msgType
	args.MessageCnt = msgCnt

	reply := MyReply{}

	res := call("Coordinator.MyCallHandler", &args, &reply)
	if !res {
		return MyReply{TaskType: ""}
	}
	return reply
}

func SendInterFiles(msgType int, msgCnt string, nReduceType int) MyReply {
	args := MyIntermediateFile{}
	args.MessageType = msgType
	args.MessageCnt = msgCnt
	args.NReduceType = nReduceType

	reply := MyReply{}

	res := call("Coordinator.MyInnerFileHandler", &args, &reply)
	if !res {
		fmt.Println("error sending intermediate files' location")
	}
	return reply
}

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

func WriteToJSONFile(intermediate []KeyValue, mapTaskNum, reduceTaskNUm int) string {
	filename := "mr-" + strconv.Itoa(mapTaskNum) + "-" + strconv.Itoa(reduceTaskNUm)
	jsonFile, _ := os.Create(filename)

	enc := json.NewEncoder(jsonFile)
	for _, kv := range intermediate {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal("error: ", err)
		}
	}
	return filename
}

func WriteToReduceOutput(key, values string, nReduce int) {
	filename := "mr-out-" + strconv.Itoa(nReduce)
	openedFile, err := os.Open(filename)
	if err != nil {
		fmt.Println("no such file")
		openedFile, _ = os.Create(filename)
	}

	fmt.Fprintf(openedFile, "%v %v\n", key, values)
}

func Partition(kva []KeyValue, reduceCount int) [][]KeyValue {
	partitionedKva := make([][]KeyValue, reduceCount)
	for _, v := range kva {
		partitionKey := ihash(v.Key) % reduceCount
		partitionedKva[partitionKey] = append(partitionedKva[partitionKey], v)
	}
	return partitionedKva
}
