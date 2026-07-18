package types

var OkResponse *RedisData = &RedisData{
	Type: SString,
	Data: "OK",
}

var QueuedResponse *RedisData = &RedisData{
	Type: SString,
	Data: "QUEUED",
}

// Serializes to $-1\r\n (null bulk string)
var NullResponse *RedisData = &RedisData{
	Type: Null,
}

// Serializes to *-1\r\n
var NullArrayResponse *RedisData = &RedisData{
	Type: NullArray,
}
