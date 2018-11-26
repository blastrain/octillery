package algorithm

import (
	"database/sql"

	"go.knocknote.io/octillery/debug"
)

type ModuloShardingAlgorithm struct {
}

func (m *ModuloShardingAlgorithm) Init(conns []*sql.DB) bool {
	return true
}

func (m *ModuloShardingAlgorithm) Shard(conns []*sql.DB, shardId int64) (*sql.DB, error) {
	shardIndex := shardId % int64(len(conns))
	debug.Printf("shardIndex = %d. (shardId = %d, len(conns) = %d)", shardIndex, shardId, len(conns))
	return conns[int(shardIndex)], nil
}

func init() {
	Register("", func() ShardingAlgorithm {
		return &ModuloShardingAlgorithm{}
	})
	Register("modulo", func() ShardingAlgorithm {
		return &ModuloShardingAlgorithm{}
	})
}
