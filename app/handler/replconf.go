package handler

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/types"
	log "github.com/sirupsen/logrus"
)

type ReplConfHandler struct {
}

func NewReplConfHandler() *ReplConfHandler {
	return &ReplConfHandler{}
}

func (*ReplConfHandler) HandleCommand(ctx context.Context, command *types.Command) (*types.RedisData, error) {
	if len(command.Args) == 0 {
		return nil, errors.New("unexpected number of arguments")
	}

	var sb strings.Builder
	for _, arg := range command.Args {
		sb.WriteString(fmt.Sprintf("%s;", arg.Data))
	}

	log.Infof("Received REPLCONF from replica: %s\n", sb.String())

	return types.OkResponse, nil
}
