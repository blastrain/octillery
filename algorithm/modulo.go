package algorithm

import (
	"database/sql"

	"github.com/aokabi/octillery/debug"
)

type moduloShardingAlgorithm struct {
}

func (m *moduloShardingAlgorithm) Init(conns []*sql.DB) bool {
	return true
}

func (m *moduloShardingAlgorithm) Shard(conns []*sql.DB, shardID int64) (*sql.DB, error) {
	shardIndex := shardID % int64(len(conns))
	debug.Printf("shardIndex = %d. (shardId = %d, len(conns) = %d)", shardIndex, shardID, len(conns))
	return conns[int(shardIndex)], nil
}

func init() {
	Register("", func() ShardingAlgorithm {
		return &moduloShardingAlgorithm{}
	})
	Register("modulo", func() ShardingAlgorithm {
		return &moduloShardingAlgorithm{}
	})
}
