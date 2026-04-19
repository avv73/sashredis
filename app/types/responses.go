package types

var OkResponse *RedisData = &RedisData{
	Type: SString,
	Data: "OK",
}

var NullResponse *RedisData = &RedisData{
	Type: Null,
}

var NullArrayResponse *RedisData = &RedisData{
	Type: NullArray,
}
