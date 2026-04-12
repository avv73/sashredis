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
	case types.Error:
		return e.encodeError(input.Data), nil
	case types.BString:
		return e.encodeBString(input.Data), nil
	}

	return nil, fmt.Errorf("unsupported redis data type for encoding: %d", input.Type)
}

func (e *Encoder) encodeSString(input string) []byte {
	return fmt.Appendf(nil, "+%s\r\n", input)
}

func (e *Encoder) encodeError(input string) []byte {
	return fmt.Appendf(nil, "-%s\r\n", input)
}

func (e *Encoder) encodeBString(input string) []byte {
	return fmt.Appendf(nil, "$%d\r\n%s\r\n", len(input), input)
}
