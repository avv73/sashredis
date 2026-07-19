package handler

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/types"
)

type InfoStorage interface {
	GetReplicationInfo() map[string]string
}

type InfoHandler struct {
	storage InfoStorage
}

func NewInfoStorage(storage InfoStorage) *InfoHandler {
	return &InfoHandler{
		storage: storage,
	}
}

func (s *InfoHandler) HandleCommand(ctx context.Context, command *types.Command) (*types.RedisData, error) {
	if len(command.Args) != 1 && command.Args[0].Data != "replication" {
		return nil, errors.New("expected INFO replication")
	}

	data := s.storage.GetReplicationInfo()

	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var pairs []string
	for _, key := range keys {
		pairs = append(pairs, fmt.Sprintf("%s:%s", key, data[key]))
	}

	resultStr := strings.Join(pairs, "\r\n")
	return &types.RedisData{
		Type: types.BString,
		Data: fmt.Sprintf("%s\r\n", resultStr),
	}, nil
}
