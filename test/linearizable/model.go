package linearizable

import (
	"fmt"

	"github.com/anishathalye/porcupine"
)

type kvInput struct {
	Op    string
	Key   string
	Value string
}

// kvState is the value of one key. Present distinguishes a missing key from
// a key whose value is empty.
type kvState struct {
	Value   string
	Present bool
}

func kvModel() porcupine.Model {
	return porcupine.Model{
		Init: func() any {
			return kvState{}
		},
		Step: func(state, input, output any) (bool, any) {
			st := state.(kvState)
			in := input.(kvInput)
			switch in.Op {
			case "put":
				return true, kvState{Value: in.Value, Present: true}
			case "get":
				return output.(kvState) == st, st
			default:
				return false, st
			}
		},
		Equal: func(a, b any) bool {
			return a.(kvState) == b.(kvState)
		},
		DescribeOperation: func(input, output any) string {
			in := input.(kvInput)
			if in.Op == "put" {
				return fmt.Sprintf("put(%q, %q)", in.Key, in.Value)
			}
			out := output.(kvState)
			if !out.Present {
				return fmt.Sprintf("get(%q) -> missing", in.Key)
			}
			return fmt.Sprintf("get(%q) -> %q", in.Key, out.Value)
		},
		DescribeState: func(state any) string {
			st := state.(kvState)
			if !st.Present {
				return "missing"
			}
			return fmt.Sprintf("%q", st.Value)
		},
		Partition: partitionByKey,
	}
}

func partitionByKey(history []porcupine.Operation) [][]porcupine.Operation {
	grouped := map[string][]porcupine.Operation{}
	var keys []string
	for _, op := range history {
		key := op.Input.(kvInput).Key
		if _, ok := grouped[key]; !ok {
			keys = append(keys, key)
		}
		grouped[key] = append(grouped[key], op)
	}
	parts := make([][]porcupine.Operation, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, grouped[key])
	}
	return parts
}
