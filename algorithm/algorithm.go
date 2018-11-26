package algorithm

import (
	"database/sql"
	"sync"

	"github.com/pkg/errors"
)

var (
	algorithmsMu sync.RWMutex
	algorithms   = make(map[string]func() ShardingAlgorithm)
)

// ShardingAlgorithm is a algorithm for assign sharding target.
//
// octillery currently supports modulo and hashmap.
// If use the other new algorithm, implement the following interface as plugin ( new_algorithm.go )
// and call algorithm.Register("algorithm_name", &NewAlgorithmStructure{}).
// Also, new_algorithm.go file should put inside go.knocknote.io/octillery/algorithm directory.
type ShardingAlgorithm interface {
	// initialize structure by connection list. if returns true, no more call this.
	Init(conns []*sql.DB) bool

	// assign sharding target by connection list and shard_key
	Shard(conns []*sql.DB, lastInsertID int64) (*sql.DB, error)
}

// Register register sharding algorithm with name
func Register(name string, algorithmFactory func() ShardingAlgorithm) {
	algorithmsMu.Lock()
	defer algorithmsMu.Unlock()
	if algorithmFactory == nil {
		panic("register sharding algorithm factory is nil")
	}
	if _, dup := algorithms[name]; dup {
		panic("register called twice for sharding algorithm " + name)
	}
	algorithms[name] = algorithmFactory
}

// LoadShardingAlgorithm load algorithm by name
func LoadShardingAlgorithm(name string) (ShardingAlgorithm, error) {
	algorithmFactory := algorithms[name]
	if algorithmFactory == nil {
		return nil, errors.Errorf("cannnot load sharding algorithm from %s", name)
	}
	return algorithmFactory(), nil
}
