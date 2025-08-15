package utils

import (
	"encoding/json"
	"fmt"
)

func MustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func AsJSON(v any) *string {
	if v == nil {
		return nil
	}

	switch v := v.(type) {
	case string:
		if len(v) > 0 && (v[0] == '{' || v[0] == '[') {
			return &v
		}
	case []byte:
		if len(v) > 0 && (v[0] == '{' || v[0] == '[') {
			s := string(v)
			return &s
		}
	}

	data, err := json.Marshal(v)
	if err != nil {
		if str, ok := v.(string); ok {
			return &str
		}
		s := fmt.Sprintf("%v", v)
		return &s
	}
	s := string(data)
	return &s
}
