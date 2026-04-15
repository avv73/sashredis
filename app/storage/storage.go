package storage

import (
	"context"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type Storage struct {
	store    map[string]*StorageBucket
	systemMu sync.Mutex
}

type BucketType int

const (
	Value = iota + 1
	List
)

type StorageBucket struct {
	Data     *types.RedisData
	Type     BucketType
	Holds    []*types.RedisData
	Metadata *StorageMetadata
}

type StorageMetadata struct {
	MsExp       *int
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
		store:    make(map[string]*StorageBucket),
		systemMu: sync.Mutex{},
	}
}

func (s *Storage) SetKvp(ctx context.Context, key string, data *types.RedisData, opts ...SetKvpOpts) error {
	option := SetKvpOption{}
	for _, opt := range opts {
		opt(&option)
	}

	s.systemMu.Lock()
	defer s.systemMu.Unlock()

	storageBucket := StorageBucket{
		Type: Value,
		Data: data,
		Metadata: &StorageMetadata{
			LastWritten: time.Now(),
		},
	}

	if option.MsExpiration != nil {
		storageBucket.Metadata.MsExp = option.MsExpiration
		go s.scheduleDeletion(ctx, key, *storageBucket.Metadata)
	}

	s.store[key] = &storageBucket
	return nil
}

func (s *Storage) GetKvp(key string) (*types.RedisData, bool, error) {
	s.systemMu.Lock()
	defer s.systemMu.Unlock()
	if !s.doesExistingDataMatchType(key, Value) {
		return nil, false, types.ErrWrongType
	}

	result, ok := s.store[key]
	if !ok {
		return nil, false, nil
	}
	if result.Metadata != nil &&
		result.Metadata.MsExp != nil &&
		result.Metadata.LastWritten.Add(time.Millisecond*time.Duration(*result.Metadata.MsExp)).Before(time.Now()) {
		delete(s.store, key)
		return nil, false, nil
	}

	return result.Data, true, nil
}

func (s *Storage) AppendToList(key string, data *types.RedisData) (int, error) {
	if !s.doesExistingDataMatchType(key, List) {
		return 0, types.ErrWrongType
	}

	_, ok := s.store[key]
	if !ok {
		s.store[key] = &StorageBucket{
			Type:  List,
			Holds: make([]*types.RedisData, 0, 1),
		}
	}

	s.store[key].Holds = append(s.store[key].Holds, data)
	return len(s.store[key].Holds), nil
}

func (s *Storage) scheduleDeletion(ctx context.Context, key string, bucket StorageMetadata) {
	timer := time.NewTimer(time.Millisecond * time.Duration(*bucket.MsExp))
	log.Infof("logged for deletion - %s", key)
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}

	s.systemMu.Lock()
	defer s.systemMu.Unlock()
	log.Infof("start deletion - %s", key)
	data, ok := s.store[key]
	metadata := data.Metadata
	if !ok || metadata == nil || !metadata.LastWritten.Equal(bucket.LastWritten) {
		// Exit scheduler if data is no longer available or the data has been changed in the meantime.
		return
	}

	delete(s.store, key)
}

// Returns true if we have stored with the same key with the same bucket type, returns true if key is not found.
func (s *Storage) doesExistingDataMatchType(key string, targetType BucketType) bool {
	val, ok := s.store[key]
	if !ok {
		return true
	}
	return val.Type == targetType
}
