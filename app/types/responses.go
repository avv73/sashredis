package types

var OkResponse *CommandResponse = &CommandResponse{Data: &RedisData{
	Type: SString,
	Data: "OK",
}}

var QueuedResponse *CommandResponse = &CommandResponse{Data: &RedisData{
	Type: SString,
	Data: "QUEUED",
}}

// Serializes to $-1\r\n (null bulk string)
var NullResponse *CommandResponse = &CommandResponse{Data: &RedisData{
	Type: Null,
}}

// Serializes to *-1\r\n
var NullArrayResponse *RedisData = &RedisData{
	Type: NullArray,
}
