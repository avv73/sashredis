package storage

type ServerInfo struct {
	data map[string]string
}

func NewServerInfoStore() *ServerInfo {
	return &ServerInfo{
		data: make(map[string]string),
	}
}

func (s *ServerInfo) SetReplicationInfo(isMaster bool) {
	role := "slave"
	if isMaster {
		role = "master"
	}

	s.data["role"] = role
}

var replicationInfoKeys map[string]any = map[string]any{
	"role": struct{}{},
}

func (s *ServerInfo) GetReplicationInfo() map[string]string {
	result := make(map[string]string)
	for key, val := range s.data {
		if _, ok := replicationInfoKeys[key]; !ok {
			continue
		}
		result[key] = val
	}
	return result
}
