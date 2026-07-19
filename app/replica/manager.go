package replica

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/config"
)

type ServerInfoHandler interface {
	SetReplicationInfo(isMaster bool)
}

type MasterClient interface {
	Ping(ctx context.Context) error
	Connect(ctx context.Context, host string, port int) error
}

type Manager struct {
	infoStore    ServerInfoHandler
	masterClient MasterClient
}

func NewManager(storage ServerInfoHandler, masterClient MasterClient) *Manager {
	return &Manager{
		infoStore:    storage,
		masterClient: masterClient,
	}
}

func (m *Manager) Initialize(ctx context.Context) error {
	hasReplicaOf := false
	replicaOf := config.GetConfig().ReplicaOf
	if replicaOf != "" {
		hasReplicaOf = true
		err := m.performHandshake(ctx, replicaOf)
		if err != nil {
			return err
		}
	}

	m.infoStore.SetReplicationInfo(!hasReplicaOf)
	return nil
}

func (m *Manager) performHandshake(ctx context.Context, replicaOf string) error {
	hostname, portStr, found := strings.Cut(replicaOf, " ")
	if !found || hostname == "" || portStr == "" {
		return errors.New("malformed replicaof argument")
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return errors.New("malformed port")
	}

	err = m.masterClient.Connect(ctx, hostname, port)
	if err != nil {
		return fmt.Errorf("handshake failed connect: %w", err)
	}

	err = m.masterClient.Ping(ctx)
	if err != nil {
		return fmt.Errorf("handshake failed ping: %w", err)
	}

	return nil
}
