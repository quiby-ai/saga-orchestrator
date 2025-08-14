package utils

import "encoding/json"

func MustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// AsJSON converts a value to its JSON representation
func AsJSON(raw any) any {
	if raw == nil {
		return nil
	}
	return raw
}
