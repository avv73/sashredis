package storage

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type StoreNotifyCallback func(*types.RedisData)

type Storage struct {
	store       map[string]*StorageBucket
	storeNotify map[string][]StoreNotifyCallback
}

type BucketType int

const (
	Value BucketType = iota + 1
	List
	Stream
)

type StreamKvp struct {
	Key   string
	Value string
}

type StreamEntry struct {
	EntryId StreamEntryKey
	Kvps    []*StreamKvp
}

type StreamEntryKey struct {
	Time           int
	SequenceNumber int
}

func (s *StreamEntryKey) String() string {
	return fmt.Sprintf("%d-%d", s.Time, s.SequenceNumber)
}

type StorageBucket struct {
	Data     *types.RedisData
	Type     BucketType
	List     *list.List
	Stream   []StreamEntry
	Metadata *StorageMetadata
}

type StorageMetadata struct {
	MsExp       *int
	LastWritten time.Time
}

func (s *StorageMetadata) isExpired() bool {
	return s.MsExp != nil && s.LastWritten.Add(time.Millisecond*time.Duration(*s.MsExp)).Before(time.Now())
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
		store:       make(map[string]*StorageBucket),
		storeNotify: make(map[string][]StoreNotifyCallback),
	}
}

func (s *Storage) SetKvp(ctx context.Context, key string, data *types.RedisData, opts ...SetKvpOpts) error {
	s.probeExpiredValues()
	option := SetKvpOption{}
	for _, opt := range opts {
		opt(&option)
	}

	storageBucket := StorageBucket{
		Type: Value,
		Data: data,
		Metadata: &StorageMetadata{
			LastWritten: time.Now(),
		},
	}

	if option.MsExpiration != nil {
		storageBucket.Metadata.MsExp = option.MsExpiration
	}

	s.store[key] = &storageBucket
	return nil
}

func (s *Storage) GetKvp(key string) (*types.RedisData, bool, error) {
	s.probeExpiredValues()
	if !s.doesExistingDataMatchType(key, Value) {
		return nil, false, types.ErrWrongType
	}

	result, ok := s.store[key]
	if !ok {
		return nil, false, nil
	}
	if result.Metadata != nil && result.Metadata.isExpired() {
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

	defer s.notify(key, data)
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
	s.notify(key, data)

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

func (s *Storage) PopList(key string, times int) (*types.RedisData, bool, error) {
	if !s.doesExistingDataMatchType(key, List) {
		return nil, false, types.ErrWrongType
	}

	if times < 0 {
		return nil, false, errors.New("value is out of range, must be positive")
	}

	bucket, ok := s.store[key]
	if !ok || bucket.List.Len() == 0 {
		return nil, false, nil
	}

	if times == 1 {
		result := bucket.List.Remove(bucket.List.Front()).(*types.RedisData)
		return result, true, nil
	}

	times = min(times, bucket.List.Len())

	result := &types.RedisData{
		Type:  types.Array,
		Holds: make([]*types.RedisData, 0, times),
	}

	for range times {
		result.Holds = append(result.Holds, bucket.List.Remove(bucket.List.Front()).(*types.RedisData))
	}

	return result, true, nil
}

func (s *Storage) SchedulePopList(ctx context.Context, key string, timeout float64, callback func(*types.RedisData, bool)) error {
	if !s.doesExistingDataMatchType(key, List) {
		return types.ErrWrongType
	}

	if timeout < 0 {
		return errors.New("expected positive timeout")
	}

	if _, ok := s.storeNotify[key]; !ok {
		s.storeNotify[key] = make([]StoreNotifyCallback, 0, 1)
	}

	doneCtx, cancel := context.WithCancel(ctx)
	var once sync.Once
	notifyCallback := func(data *types.RedisData) {
		cancel()
		once.Do(func() {
			bucket, ok := s.store[key]
			if !ok || bucket.List.Len() == 0 {
				log.Error("unexpected: possible race condition, empty after notification")
				callback(nil, false)
				return
			}
			result := bucket.List.Remove(bucket.List.Front()).(*types.RedisData)
			callback(result, true)
		})
	}

	s.storeNotify[key] = append(s.storeNotify[key], notifyCallback)

	if timeout != 0 {
		go func() {
			timeoutTimer := time.NewTimer(time.Duration(timeout * float64(time.Second)))
			select {
			case <-timeoutTimer.C:
				once.Do(func() {
					callback(nil, false)
				})
			case <-doneCtx.Done():
				return
			}
		}()
	}
	return nil
}

func (s *Storage) Type(ctx context.Context, key string) string {
	bucket, ok := s.store[key]
	if !ok {
		return "none"
	}

	switch bucket.Type {
	case Value:
		return "string"
	case List:
		return "list"
	case Stream:
		return "stream"
	}

	log.Errorf("unexpected type command key: %s for bucket type: %d", key, bucket.Type)
	return "?"
}

func (s *Storage) AddToStreamWithCustomEntryKey(ctx context.Context, streamKey string, entryKey string, data []*StreamKvp) (string, error) {
	if !s.doesExistingDataMatchType(streamKey, Stream) {
		return "", types.ErrWrongType
	}

	_, ok := s.store[streamKey]
	if !ok {
		s.store[streamKey] = &StorageBucket{
			Type:   Stream,
			Stream: make([]StreamEntry, 0),
		}
	}

	parsedEntryKey, err := s.validateCustomEntryKey(streamKey, entryKey)
	if err != nil {
		return "", err
	}

	s.store[streamKey].Stream = append(s.store[streamKey].Stream, StreamEntry{
		EntryId: parsedEntryKey,
		Kvps:    data,
	})

	return parsedEntryKey.String(), nil
}

var errInvalidXaddId = types.NewRedisError(types.GeneralError, "The ID specified in XADD is equal or smaller than the target stream top item")

func (s *Storage) validateCustomEntryKey(streamKey string, entryKey string) (StreamEntryKey, error) {
	millisecondsTime, sequenceNum, err := parseEntryKey(entryKey)
	if err != nil {
		return StreamEntryKey{}, err
	}

	stream := s.store[streamKey].Stream
	if millisecondsTime != nil && *millisecondsTime == 0 && sequenceNum != nil && *sequenceNum == 0 {
		return StreamEntryKey{}, types.NewRedisError(types.GeneralError, "The ID specified in XADD must be greater than 0-0")
	}

	if len(stream) == 0 {
		if sequenceNum != nil {
			return StreamEntryKey{
				Time:           *millisecondsTime,
				SequenceNumber: *sequenceNum,
			}, nil
		}

		if millisecondsTime != nil && *millisecondsTime == 0 {
			return StreamEntryKey{
				Time:           *millisecondsTime,
				SequenceNumber: 1,
			}, nil
		}
		return StreamEntryKey{
			Time:           *millisecondsTime,
			SequenceNumber: 0,
		}, nil
	}

	lastEntryId := stream[len(stream)-1].EntryId

	if *millisecondsTime > lastEntryId.Time {
		return StreamEntryKey{
			Time:           *millisecondsTime,
			SequenceNumber: 0,
		}, nil
	}

	if *millisecondsTime < lastEntryId.Time {
		return StreamEntryKey{}, errInvalidXaddId
	}

	if sequenceNum != nil && *sequenceNum <= lastEntryId.SequenceNumber {
		return StreamEntryKey{}, errInvalidXaddId
	}

	return StreamEntryKey{
		Time:           *millisecondsTime,
		SequenceNumber: lastEntryId.SequenceNumber + 1,
	}, nil
}

// func generateEntryKey(millisecondsTime *int, sequenceNum *int, stream []StreamEntry) StreamEntryKey {
// 	if sequenceNum == nil {
// 		sameLatestTimeIdx := sort.Search(len(stream), func(idx int) bool {
// 			if stream[idx].EntryId.Time == *millisecondsTime {
// 				return true
// 			}
// 			return false
// 		})

// 		if sameLatestTimeIdx == len(stream) {
// 			return StreamEntryKey{
// 				Time:           *millisecondsTime,
// 				SequenceNumber: 0,
// 			}
// 		}

// 		// probe for latest
// 		for i := sameLatestTimeIdx + 1; i < len(stream); i++ {
// 			if stream[i].EntryId.Time == *millisecondsTime {
// 				sameLatestTimeIdx = i
// 				continue
// 			}
// 			break
// 		}

// 		return StreamEntryKey{
// 			Time:           *millisecondsTime,
// 			SequenceNumber: stream[sameLatestTimeIdx].EntryId.SequenceNumber + 1,
// 		}

// 	}

// 	return StreamEntryKey{
// 		Time:           *millisecondsTime,
// 		SequenceNumber: *sequenceNum,
// 	}
// }

// (millisecondsTime, sequenceNum)
func parseEntryKey(entryKey string) (*int, *int, error) {
	tokens := strings.Split(entryKey, "-")
	if len(tokens) != 2 {
		return nil, nil, errors.New("expected entry key to be in format {milliseconds}-{sequenceNum}")
	}

	millisecondsTime, err := strconv.Atoi(tokens[0])
	if err != nil {
		return nil, nil, errors.New("expected milliseconds to be valid int")
	}

	if millisecondsTime < 0 {
		return nil, nil, errors.New("expected milliseconds to be non-negative")
	}

	seqNumber, err := strconv.Atoi(tokens[1])
	if err != nil {
		if tokens[1] == "*" {
			return &millisecondsTime, nil, nil
		}
		return nil, nil, errors.New("expected sequenceNum to be valid int")
	}

	if seqNumber < 0 {
		return nil, nil, errors.New("expected sequenceNum to be non-negative")
	}

	return &millisecondsTime, &seqNumber, nil
}

func (s *Storage) probeExpiredValues() {
	attempts := 3
	for key, val := range s.store {
		if attempts <= 0 {
			return
		}

		attempts--
		if val.Type != Value {
			continue
		}

		if val.Metadata != nil && val.Metadata.isExpired() {
			delete(s.store, key)
		}
	}
}

// Returns true if we have stored with the same key with the same bucket type, returns true if key is not found.
func (s *Storage) doesExistingDataMatchType(key string, targetType BucketType) bool {
	val, ok := s.store[key]
	if !ok {
		return true
	}
	return val.Type == targetType
}

func (s *Storage) notify(key string, data *types.RedisData) {
	if subs, ok := s.storeNotify[key]; ok {
		subs[0](data)
		if len(subs) == 1 {
			delete(s.storeNotify, key)
		} else {
			s.storeNotify[key] = subs[1:]
		}
	}
}
