package storage

import (
	"context"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type Storage struct {
	kvpStore map[string]*StorageBucket
	systemMu sync.Mutex
}

type StorageBucket struct {
	Data        *types.RedisData
	MsExp       int
	LastWritten time.Time
}

type SetKvpOpts func(*SetKvpOption)

type SetKvpOption struct {
	MsExpiration *int
}

func WithMillisecondsExp(milliseconds int) SetKvpOpts {
	return func(sko *SetKvpOption) {
		sko.MsExpiration = &milliseconds
	}
}

func NewStorage() *Storage {
	return &Storage{
		kvpStore: make(map[string]*StorageBucket),
		systemMu: sync.Mutex{},
	}
}

func (s *Storage) SetKvp(ctx context.Context, key string, data *types.RedisData, opts ...SetKvpOpts) {
	option := SetKvpOption{}
	for _, opt := range opts {
		opt(&option)
	}

	s.systemMu.Lock()
	defer s.systemMu.Unlock()
	storageBucket := StorageBucket{
		LastWritten: time.Now(),
	}

	if option.MsExpiration != nil {
		storageBucket.MsExp = *option.MsExpiration
		go s.scheduleDeletion(ctx, key, storageBucket)
	}

	storageBucket.Data = data
	s.kvpStore[key] = &storageBucket
}

func (s *Storage) GetKvp(key string) (*types.RedisData, bool) {
	s.systemMu.Lock()
	defer s.systemMu.Unlock()
	result, ok := s.kvpStore[key]
	if !ok {
		return nil, false
	}
	if result.LastWritten.Add(time.Millisecond * time.Duration(result.MsExp)).Before(time.Now()) {
		delete(s.kvpStore, key)
		return nil, false
	}

	return result.Data, true
}

func (s *Storage) scheduleDeletion(ctx context.Context, key string, bucket StorageBucket) {
	timer := time.NewTimer(time.Millisecond * time.Duration(bucket.MsExp))
	log.Infof("logged for deletion - %s", key)
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}

	s.systemMu.Lock()
	defer s.systemMu.Unlock()
	log.Infof("start deletion - %s", key)
	data, ok := s.kvpStore[key]
	if !ok || !data.LastWritten.Equal(bucket.LastWritten) {
		// Exit scheduler if data is no longer available or the data has been changed in the meantime.
		return
	}

	delete(s.kvpStore, key)
}
