package types

import (
	"fmt"
	"strings"
)

type CommandName string

const (
	Ping   CommandName = "PING"
	Echo   CommandName = "ECHO"
	Set    CommandName = "SET"
	Get    CommandName = "GET"
	Rpush  CommandName = "RPUSH"
	Lrange CommandName = "LRANGE"
	Lpush  CommandName = "LPUSH"
	Llen   CommandName = "LLEN"
	Lpop   CommandName = "LPOP"
	Blpop  CommandName = "BLPOP"
	Type   CommandName = "TYPE"
	Xadd   CommandName = "XADD"
	Xrange CommandName = "XRANGE"
	Xread  CommandName = "XREAD"
	Incr   CommandName = "INCR"
)

type DataType int

const (
	Null DataType = iota
	NullArray
	Integer
	SString
	BString
	Array
	Error
)

func (d DataType) String() string {
	switch d {
	case Null:
		return "Null"
	case Integer:
		return "Integer"
	case SString:
		return "SString"
	case BString:
		return "BString"
	case Array:
		return "Array"
	case Error:
		return "Error"
	}

	return fmt.Sprintf("unknown:%d", d)
}

type Command struct {
	Command CommandName
	Args    []*RedisData
}

type RedisData struct {
	Type  DataType
	Data  string
	Holds []*RedisData
}

func (r *RedisData) IsNil() bool {
	return r == nil || r.Type == Null
}

func (r *RedisData) String() string {
	var nested strings.Builder
	for _, child := range r.Holds {
		nested.WriteString(child.String())
	}

	return fmt.Sprintf("%s:<data: %s> holds: [%s]", r.Type.String(), r.Data, nested.String())
}

func (r *RedisData) Clone() *RedisData {
	new := &RedisData{
		Data: r.Data,
		Type: r.Type,
	}

	for _, data := range r.Holds {
		new.Holds = append(new.Holds, data.Clone())
	}
	return new
}
