package internal

// HTTPBatch interface help to freeze
// the input and outout function definations
type HTTPBatch interface {
	GetFlow() []Flow
	ProduceOutput(<-chan FlowResult, chan<- struct{})
}

// CmdPipe helps to define the Command line Agrs
type CmdPipe struct {
	Version bool
	IFile   string
	ODir    string
	Workers int
	Timeout int
}
