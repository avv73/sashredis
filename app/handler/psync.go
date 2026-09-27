package handler

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/types"
	log "github.com/sirupsen/logrus"
)

type PSyncHandler struct {
	psyncInfoStorage InfoStorage
}

func NewPSyncHandler(storage InfoStorage) *PSyncHandler {
	return &PSyncHandler{
		psyncInfoStorage: storage,
	}
}

const emptyRdbFile = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

func (p *PSyncHandler) HandleCommand(ctx context.Context, command *types.Command) (*types.CommandResponse, error) {
	if len(command.Args) != 2 {
		return nil, errors.New("unexpected number of arguments")
	}

	replicationId := command.Args[0].Data
	offset := command.Args[1].Data

	log.Infof("Received PSYNC from replica: replId: %s offset: %s\n", replicationId, offset)

	masterReplicationId, ok := p.psyncInfoStorage.GetReplicationInfo().GetMasterReplId()
	if !ok {
		return nil, errors.New("master has no replication id set")
	}

	decoded, err := base64.StdEncoding.DecodeString(emptyRdbFile)
	if err != nil {
		return nil, err
	}

	length := len(decoded)
	strDataHeader := fmt.Sprintf("$%d\r\n", length)
	headerBinary := []byte(strDataHeader)

	extraResponse := append(headerBinary, decoded...)
	return &types.CommandResponse{
		Data: &types.RedisData{
			Type: types.SString,
			Data: fmt.Sprintf("FULLRESYNC %s 0", masterReplicationId),
		},
		ExtraData: extraResponse,
	}, nil
}
