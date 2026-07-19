package replica

import "github.com/codecrafters-io/redis-starter-go/app/config"

type ServerInfoHandler interface {
	SetReplicationInfo(isMaster bool)
}

type Manager struct {
	infoStore ServerInfoHandler
}

func NewManager(storage ServerInfoHandler) *Manager {
	return &Manager{
		infoStore: storage,
	}
}

func (m *Manager) Initialize() {
	// TODO: Determine whether we have replication here and do some special stuff.
	hasReplicaOf := false
	conf := config.GetConfig()
	if conf.ReplicaOf != "" {
		hasReplicaOf = true
	}

	m.infoStore.SetReplicationInfo(!hasReplicaOf)
}
