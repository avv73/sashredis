package marshal

import (
	"fmt"
	"strings"

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
	case types.Integer:
		return e.encodeInteger(input.Data), nil
	case types.Array:
		return e.encodeArray(input)
	case types.Null:
		return e.encodeNullBulkString(), nil
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

func (e *Encoder) encodeNullBulkString() []byte {
	return fmt.Appendf(nil, "$-1\r\n")
}

func (e *Encoder) encodeInteger(input string) []byte {
	return fmt.Appendf(nil, ":%s\r\n", input)
}

func (e *Encoder) encodeArray(input *types.RedisData) ([]byte, error) {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(input.Holds)))

	for _, element := range input.Holds {
		result, err := e.Encode(element)
		if err != nil {
			return nil, err
		}
		sb.Write(result)
	}
	return []byte(sb.String()), nil
}
