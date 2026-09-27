package wal

type OpType string

const (
	OpTypeSet    OpType = "set"
	OpTypeDelete OpType = "delete"
	OpTypeClear  OpType = "clear"
)

type Record struct {
	OpType OpType
	Key    string
	Value  string
}

func (r Record) Encode() ([]byte, error) {

}

func (r Record) Decode(data []byte) error {

}
