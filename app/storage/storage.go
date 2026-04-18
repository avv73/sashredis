package storage

import (
	"container/list"
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
	List     *list.List
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
			Type: List,
			List: list.New(),
		}
	}

	s.store[key].List.PushBack(data)
	return s.store[key].List.Len(), nil
}

func (s *Storage) PrependToList(key string, data *types.RedisData) (int, error) {
	if !s.doesExistingDataMatchType(key, List) {
		return 0, types.ErrWrongType
	}

	_, ok := s.store[key]
	if !ok {
		s.store[key] = &StorageBucket{
			Type: List,
			List: list.New(),
		}
	}

	s.store[key].List.PushFront(data)
	return s.store[key].List.Len(), nil
}

func (s *Storage) FetchFromList(key string, startIdx int, endIdx int) ([]*types.RedisData, error) {
	if !s.doesExistingDataMatchType(key, List) {
		return nil, types.ErrWrongType
	}

	bucket, ok := s.store[key]
	if !ok {
		return []*types.RedisData{}, nil
	}

	if startIdx < 0 {
		startIdx = max(bucket.List.Len()+startIdx, 0)
	}
	if endIdx < 0 {
		endIdx = max(bucket.List.Len()+endIdx, 0)
	}

	if startIdx >= bucket.List.Len() || startIdx > endIdx {
		return []*types.RedisData{}, nil
	}

	endIdx++ // including
	if endIdx > bucket.List.Len() {
		endIdx = bucket.List.Len()
	}

	var counter int
	var result []*types.RedisData
	for e := bucket.List.Front(); e != nil && counter < endIdx; e = e.Next() {
		if counter >= startIdx {
			result = append(result, e.Value.(*types.RedisData))
		}

		counter++
	}

	return result, nil
}

func (s *Storage) ListLength(key string) (int, error) {
	if !s.doesExistingDataMatchType(key, List) {
		return 0, types.ErrWrongType
	}

	bucket, ok := s.store[key]
	if !ok {
		return 0, nil
	}
	return bucket.List.Len(), nil
}

func (s *Storage) PopList(key string) (*types.RedisData, bool, error) {
	if !s.doesExistingDataMatchType(key, List) {
		return nil, false, types.ErrWrongType
	}

	bucket, ok := s.store[key]
	if !ok {
		return nil, false, nil
	}

	result := bucket.List.Remove(bucket.List.Front()).(*types.RedisData)
	return result, true, nil
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
