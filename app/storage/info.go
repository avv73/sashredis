package storage

import (
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

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

type ReplicationInfo map[string]string

func (s ReplicationInfo) GetRole() (string, bool) {
	val, ok := s[roleKey]
	return val, ok
}

func (s ReplicationInfo) GetMasterReplId() (string, bool) {
	val, ok := s[masterReplicaIdKey]
	return val, ok
}

func (s ReplicationInfo) GetMasterReplOffset() (int64, bool) {
	val, ok := s[masterReplicaOffsetKey]
	if !ok {
		return 0, false
	}
	valInt, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, false
	}
	return valInt, true
}

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

func (s *ServerInfo) GetReplicationInfo() ReplicationInfo {
	result := make(map[string]string)
	for key, val := range s.data {
		if _, ok := replicationInfoKeys[key]; !ok {
			continue
		}
		result[key] = val
	}
	return result
}
