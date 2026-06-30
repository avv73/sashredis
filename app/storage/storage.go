package storage

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/codecrafters-io/redis-starter-go/app/types"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

type StoreNotifyCallback func(key string, data *types.RedisData)

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

func (s *StreamEntry) ToRedisData() *types.RedisData {
	kvps := &types.RedisData{
		Type:  types.Array,
		Holds: make([]*types.RedisData, 0, len(s.Kvps)),
	}
	for _, kvp := range s.Kvps {
		kvps.Holds = append(kvps.Holds, &types.RedisData{
			Type: types.BString,
			Data: kvp.Key,
		})
		kvps.Holds = append(kvps.Holds, &types.RedisData{
			Type: types.BString,
			Data: kvp.Value,
		})
	}

	result := &types.RedisData{
		Type: types.Array,
		Holds: []*types.RedisData{
			&types.RedisData{
				Type: types.BString,
				Data: s.EntryId.String(),
			},
			kvps,
		},
	}
	return result
}

type StreamEntryKey struct {
	Time           int64
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

	defer s.notifyFirst(key, data)
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
	s.notifyFirst(key, data)

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
	notifyCallback := func(key string, data *types.RedisData) {
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

type StorageType string

const (
	StorageTypeString  StorageType = "string"
	StorageTypeList    StorageType = "list"
	StorageTypeStream  StorageType = "stream"
	StorageTypeUnknown StorageType = "?"
)

func (s *Storage) Type(ctx context.Context, key string) StorageType {
	bucket, ok := s.store[key]
	if !ok {
		return "none"
	}

	switch bucket.Type {
	case Value:
		return StorageTypeString
	case List:
		return StorageTypeList
	case Stream:
		return StorageTypeStream
	}

	log.Errorf("unexpected type command key: %s for bucket type: %d", key, bucket.Type)
	return StorageTypeUnknown
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

	entry := StreamEntry{
		EntryId: parsedEntryKey,
		Kvps:    data,
	}

	s.store[streamKey].Stream = append(s.store[streamKey].Stream, entry)
	s.notifyAll(streamKey, entry.ToRedisData())

	return parsedEntryKey.String(), nil
}

var filterFunc = func(stream *StorageBucket, filterTime int64, filterSeqNum int) func(i int) int {
	return func(i int) int {
		if stream.Stream[i].EntryId.Time < filterTime {
			return 1
		}
		if stream.Stream[i].EntryId.Time > filterTime {
			return -1
		}
		if stream.Stream[i].EntryId.SequenceNumber < filterSeqNum {
			return 1
		}
		if stream.Stream[i].EntryId.SequenceNumber > filterSeqNum {
			return -1
		}
		return 0
	}
}

func (s *Storage) QueryStream(ctx context.Context, streamKey string, startId string, endId string) ([]*types.RedisData, error) {
	if !s.doesExistingDataMatchType(streamKey, Stream) {
		return nil, types.ErrWrongType
	}

	if _, ok := s.store[streamKey]; !ok {
		return nil, errors.New("no such stream exists")
	}

	var startTime *int64
	var startSeqNum *int
	if startId == "-" {
		startTime = utils.ToPtr(int64(0))
		startSeqNum = utils.ToPtr(1)
	} else {
		var err error
		startTime, startSeqNum, err = types.ParseStreamEntryKey(startId, false)
		if err != nil {
			return nil, err
		}
	}

	if startTime == nil {
		return nil, errors.New("start time expected")
	}

	if startSeqNum == nil {
		startSeqNum = utils.ToPtr(0)
	}

	var endTime *int64
	var endSeqNum *int
	if endId == "+" {
		lastEl := s.store[streamKey].Stream[len(s.store[streamKey].Stream)-1]
		endTime = utils.ToPtr(lastEl.EntryId.Time)
		endSeqNum = utils.ToPtr(lastEl.EntryId.SequenceNumber)
	} else {
		var err error
		endTime, endSeqNum, err = types.ParseStreamEntryKey(endId, false)
		if err != nil {
			return nil, err
		}
	}

	if endTime == nil {
		return nil, errors.New("end time expected")
	}

	if endSeqNum == nil {
		endSeqNum = utils.ToPtr(math.MaxInt)
	}

	stream := s.store[streamKey]

	startIdx, _ := sort.Find(len(stream.Stream), filterFunc(stream, *startTime, *startSeqNum))

	if startIdx == len(stream.Stream) {
		return []*types.RedisData{}, nil
	}

	endIdx, hasEnd := sort.Find(len(stream.Stream), filterFunc(stream, *endTime, *endSeqNum))

	if !hasEnd && *endSeqNum == math.MaxInt {
		endIdx -= 1
	}

	if endIdx == len(stream.Stream) {
		return []*types.RedisData{}, nil
	}

	elements := stream.Stream[startIdx : endIdx+1]
	results := make([]*types.RedisData, 0, endIdx-startIdx+1)
	for _, element := range elements {
		results = append(results, element.ToRedisData())
	}

	return results, nil
}

func (s *Storage) ReadStream(ctx context.Context, streamKeys []string, ids []string) ([]*types.RedisData, error) {
	if len(streamKeys) != len(ids) {
		return nil, errors.New("expected equal number of keys and ids")
	}

	results := make([]*types.RedisData, 0, len(streamKeys))
	for i, streamKey := range streamKeys {
		if !s.doesExistingDataMatchType(streamKey, Stream) {
			return nil, types.ErrWrongType
		}

		if _, ok := s.store[streamKey]; !ok {
			continue
		}

		stream := s.store[streamKey]
		startTime, startSeqNum, err := types.ParseStreamEntryKey(ids[i], true)
		if err != nil {
			return nil, fmt.Errorf("expected a valid key: %w", err)
		}
		if startTime == nil || startSeqNum == nil {
			return nil, errors.New("expected time or sequence num")
		}
		startIdx, found := sort.Find(len(stream.Stream), filterFunc(stream, *startTime, *startSeqNum))

		if found {
			startIdx++ // offset by one if we find the match, it is exclusive of the exact element
		}

		elementsResults := &types.RedisData{Type: types.Array}
		streamResults := &types.RedisData{
			Type: types.Array,
			Holds: []*types.RedisData{
				&types.RedisData{
					Type: types.BString,
					Data: streamKey,
				},
				elementsResults,
			},
		}
		results = append(results, streamResults)

		if startIdx >= len(stream.Stream) {
			elementsResults.Type = types.NullArray
			continue
		}

		elements := stream.Stream[startIdx:]
		elementsResults.Holds = make([]*types.RedisData, 0, len(elements))

		for _, element := range elements {
			elementsResults.Holds = append(elementsResults.Holds, element.ToRedisData())
		}

	}

	return results, nil
}

func (s *Storage) ScheduleReadStream(ctx context.Context, streamKeys []string, ids []string, timeout int64, callback func([]*types.RedisData, bool)) error {
	if timeout < 0 {
		return errors.New("expected positive timeout")
	}

	for _, key := range streamKeys {
		if !s.doesExistingDataMatchType(key, Stream) {
			return types.ErrWrongType
		}

		if _, ok := s.storeNotify[key]; !ok {
			s.storeNotify[key] = make([]StoreNotifyCallback, 0, 1)
		}
	}

	doneCtx, cancel := context.WithCancel(ctx)
	var once sync.Once
	notifyCallback := func(key string, data *types.RedisData) {
		cancel()
		once.Do(func() {
			result := []*types.RedisData{&types.RedisData{
				Type: types.Array,
				Holds: []*types.RedisData{
					&types.RedisData{
						Type: types.BString,
						Data: key,
					},
					&types.RedisData{
						Type: types.Array,
						Holds: []*types.RedisData{
							data,
						},
					},
				},
			}}

			for _, lkey := range streamKeys {
				s.unsubscribe(lkey)
			}

			callback(result, true)
		})
	}
	for _, key := range streamKeys {
		s.storeNotify[key] = append(s.storeNotify[key], notifyCallback)
	}

	if timeout != 0 {
		go func() {
			timeoutTimer := time.NewTimer(time.Duration(timeout) * time.Millisecond)
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

func (s *Storage) Increment(ctx context.Context, key string) (*types.RedisData, error) {
	value, ok, err := s.GetKvp(key)
	if err != nil {
		return nil, err
	}

	if !ok {
		value = &types.RedisData{
			Type: types.BString,
			Data: "0",
		}
	}
	valueInt, err := strconv.ParseUint(value.Data, 10, 64)
	if err != nil {
		return nil, types.ErrValueNotInteger
	}

	valueInt++
	value.Data = strconv.FormatUint(valueInt, 10)

	s.SetKvp(ctx, key, value)

	valueToReturn := value.Clone()
	valueToReturn.Type = types.Integer // Do not persist integer type to the underlying store, it is used only for formatting.
	return valueToReturn, nil
}

var errInvalidXaddId = types.NewRedisError(types.GeneralError, "The ID specified in XADD is equal or smaller than the target stream top item")

func (s *Storage) validateCustomEntryKey(streamKey string, entryKey string) (StreamEntryKey, error) {
	millisecondsTime, sequenceNum, err := types.ParseStreamEntryKey(entryKey, true)
	if err != nil {
		return StreamEntryKey{}, err
	}

	stream := s.store[streamKey].Stream
	if millisecondsTime != nil && *millisecondsTime == 0 && sequenceNum != nil && *sequenceNum == 0 {
		return StreamEntryKey{}, types.NewRedisError(types.GeneralError, "The ID specified in XADD must be greater than 0-0")
	}

	if millisecondsTime == nil {
		millisecondsTime = utils.ToPtr(time.Now().UnixMilli())
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

func (s *Storage) notifyFirst(key string, data *types.RedisData) {
	if subs, ok := s.storeNotify[key]; ok {
		subs[0](key, data)
		s.unsubscribe(key)
	}
}

func (s *Storage) notifyAll(key string, data *types.RedisData) {
	if subs, ok := s.storeNotify[key]; ok {
		for _, sub := range subs {
			sub(key, data)
			s.unsubscribe(key)
		}
	}
}

func (s *Storage) unsubscribe(key string) {
	if subs, ok := s.storeNotify[key]; ok {
		if len(subs) == 1 {
			delete(s.storeNotify, key)
		} else {
			s.storeNotify[key] = subs[1:]
		}
	}
}
