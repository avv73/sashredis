package storage

import "github.com/codecrafters-io/redis-starter-go/app/utils"

type ServerInfo struct {
	data map[string]string
}

func NewServerInfoStore() *ServerInfo {
	return &ServerInfo{
		data: make(map[string]string),
	}
}

const roleKey string = "role"
const masterReplicaIdKey string = "master_replid"
const masterReplicaOffsetKey string = "master_repl_offset"

func (s *ServerInfo) SetReplicationInfo(isMaster bool) {
	role := "slave"
	if isMaster {
		role = "master"
	}

	s.data[roleKey] = role

	if isMaster {
		s.data[masterReplicaIdKey] = utils.RandString(40)
		s.data[masterReplicaOffsetKey] = "0"
	}
}

var replicationInfoKeys map[string]any = map[string]any{
	roleKey:                struct{}{},
	masterReplicaIdKey:     struct{}{},
	masterReplicaOffsetKey: struct{}{},
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
