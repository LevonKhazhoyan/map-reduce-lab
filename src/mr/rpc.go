package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//
import "os"
import "strconv"

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
const (
	MsgForTask = iota
	MsgForInterFileLoc
	MsgForFinishMap
	MsgForFinishReduce
)

type MyArgs struct {
	MessageType int
	MessageCnt  string
}

// send intermediate files' filename to master
type MyIntermediateFile struct {
	MessageType int
	MessageCnt  string
	NReduceType int
}

type MapJob struct {
	InputFile    string
	MapJobNumber int
	ReducerCount int
}

type ReduceJob struct {
	IntermediateFiles []string
	ReduceNumber      int
}

type RequestTaskReply struct {
	MapJob    *MapJob
	ReduceJob *ReduceJob
	Done      bool
}

type ReportMapTaskArgs struct {
	InputFile        string
	IntermediateFile []string
}

type MyReply struct {
	Filename        string // get a filename
	MapNumMapped    int
	ReduceNumMapped int
	NReduce         int
	TaskType        string
	ReduceFileList  []string // File list about
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
