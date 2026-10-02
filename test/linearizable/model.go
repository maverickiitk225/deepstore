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

// kvOutput is what a call returned. Unknown means a put was in flight when
// the process was killed, so the write may or may not have been synced.
type kvOutput struct {
	Value   string
	Present bool
	Unknown bool
}

func kvModel() porcupine.Model {
	nm := porcupine.NondeterministicModel{
		Init: func() []any {
			return []any{kvState{}}
		},
		Step: func(state, input, output any) []any {
			st := state.(kvState)
			in := input.(kvInput)
			out := output.(kvOutput)
			switch in.Op {
			case "put":
				written := kvState{Value: in.Value, Present: true}
				if out.Unknown {
					return []any{st, written}
				}
				return []any{written}
			case "get":
				if out.Value == st.Value && out.Present == st.Present {
					return []any{st}
				}
				return nil
			default:
				return nil
			}
		},
		Equal: func(a, b any) bool {
			return a.(kvState) == b.(kvState)
		},
		DescribeOperation: func(input, output any) string {
			in := input.(kvInput)
			out := output.(kvOutput)
			if in.Op == "put" {
				if out.Unknown {
					return fmt.Sprintf("put(%q, %q) -> unknown", in.Key, in.Value)
				}
				return fmt.Sprintf("put(%q, %q)", in.Key, in.Value)
			}
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
	return nm.ToModel()
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
