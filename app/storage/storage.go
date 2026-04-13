package storage

import "github.com/codecrafters-io/redis-starter-go/app/types"

type Storage struct {
	kvpStore map[string]*types.RedisData
}

func NewStorage() *Storage {
	return &Storage{
		kvpStore: make(map[string]*types.RedisData),
	}
}

func (s *Storage) SetKvp(key string, data *types.RedisData) {
	s.kvpStore[key] = data
}

func (s *Storage) GetKvp(key string) (*types.RedisData, bool) {
	result, ok := s.kvpStore[key]
	return result, ok
}
