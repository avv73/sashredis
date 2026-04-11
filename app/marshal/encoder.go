package marshal

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type Encoder struct {
}

func NewEncoder() *Encoder {
	return &Encoder{}
}

func (e *Encoder) Encode(input *types.RedisData) ([]byte, error) {
	switch input.Type {
	case types.SString:
		return e.encodeSString(input.Data), nil
	}

	return nil, fmt.Errorf("unsupported redis data type for encoding: %d", input.Type)
}

func (e *Encoder) encodeSString(input string) []byte {
	return fmt.Appendf(nil, "+%s\r\n", input)
}
