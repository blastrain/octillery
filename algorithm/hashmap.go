package algorithm

import (
	"database/sql"
	"fmt"
	"hash/crc32"

	"github.com/pkg/errors"
	"go.knocknote.io/octillery/debug"
)

const (
	hashSlotMaxSize = 1023
)

type hashMapCluster struct {
	startSlot uint32
	endSlot   uint32
	conn      *sql.DB
}

type hashMapShardingAlgorithm struct {
	hashSlotSize uint32
	clusters     []*hashMapCluster
}

func (h *hashMapShardingAlgorithm) addCluster(startSlot uint32, endSlot uint32, conn *sql.DB) {
	if h.clusters == nil {
		h.clusters = make([]*hashMapCluster, 0)
	}
	h.clusters = append(h.clusters, &hashMapCluster{
		startSlot: startSlot,
		endSlot:   endSlot,
		conn:      conn,
	})
}

func (h *hashMapShardingAlgorithm) hashSlotToClusterIndex(hashSlot uint32) (int, error) {
	for idx, cluster := range h.clusters {
		if cluster.startSlot <= hashSlot && hashSlot <= cluster.endSlot {
			return idx, nil
		}
	}
	return -1, errors.Errorf("unknown hashSlot %d", hashSlot)
}

func (h *hashMapShardingAlgorithm) Init(conns []*sql.DB) bool {
	if len(conns) < 2 {
		return false
	}
	eachClusterSlotNum := uint32(hashSlotMaxSize / len(conns))
	startSlotNum := uint32(0)
	endSlotNum := eachClusterSlotNum
	lastIndex := len(conns) - 1
	for idx, conn := range conns {
		if idx == lastIndex {
			endSlotNum = hashSlotMaxSize
		}
		h.addCluster(startSlotNum, endSlotNum, conn)
		startSlotNum += eachClusterSlotNum + 1
		endSlotNum += eachClusterSlotNum + 1
	}
	h.hashSlotSize = hashSlotMaxSize
	return true
}

func (h *hashMapShardingAlgorithm) Shard(conns []*sql.DB, shardID int64) (*sql.DB, error) {
	hash := crc32.ChecksumIEEE([]byte(fmt.Sprintf("%d", shardID)))
	hashSlot := hash % h.hashSlotSize
	clusterIndex, err := h.hashSlotToClusterIndex(hashSlot)
	debug.Printf("shardId = %d hash = %d hashSlot = %d clusterIndex = %d", shardID, hash, hashSlot, clusterIndex)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get clusterIndex from hashSlot %d. shardId = %d, hash = %d", hashSlot, shardID, hash)
	}
	return h.clusters[clusterIndex].conn, nil
}

func init() {
	Register("hashmap", func() ShardingAlgorithm {
		return &hashMapShardingAlgorithm{}
	})
}
