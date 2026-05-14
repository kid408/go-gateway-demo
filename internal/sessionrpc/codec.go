package sessionrpc

import (
	"encoding/json"

	"google.golang.org/grpc/encoding"
)

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (jsonCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func (jsonCodec) Name() string {
	return "session-json"
}

func init() {
	encoding.RegisterCodec(jsonCodec{})
}

func JSONCodec() encoding.Codec {
	return jsonCodec{}
}
