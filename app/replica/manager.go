package replica

type ServerInfoHandler interface {
	SetDefaultReplicationInfo()
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
	m.infoStore.SetDefaultReplicationInfo()
}
