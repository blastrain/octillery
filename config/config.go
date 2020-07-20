package config

import (
	"io/ioutil"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

// DatabaseConfig type for database definition
type DatabaseConfig struct {
	// database name of MySQL or database file path of SQLite
	NameOrPath string `yaml:"database"`

	// adapter name ( 'mysql' or 'sqlite3' )
	Adapter string `yaml:"adapter"`

	// database encoding like utf8mb4
	Encoding string `yaml:"encoding"`

	// login user name to database server
	Username string `yaml:"username"`

	// login password to database server
	Password string `yaml:"password"`

	// main server's dsn list ( currently support single main only )
	Mains []string `yaml:"main"`

	// subordinate server's dsn list ( currently not support )
	Subordinates []string `yaml:"subordinate"`

	// backup server's dsn list ( currently not support )
	Backups []string `yaml:"backup"`
}

// TableConfig type for table definition
type TableConfig struct {
	DatabaseConfig `yaml:",inline"`

	// enable sharding in this table
	IsShard bool `yaml:"shard"`

	// unique id's column for all shards. id is published by sequencer
	ShardColumnName string `yaml:"shard_column"`

	// column name for deciding sharding target
	// this column's value is passed to sharding algorithm
	// if not specified, shard_column value is used as shard_key
	ShardKeyColumnName string `yaml:"shard_key"`

	// sharding algorithm ( default: modulo )
	Algorithm string `yaml:"algorithm"`

	// support unique id in between all shards
	Sequencer *DatabaseConfig `yaml:"sequencer"`

	// shard configurations
	Shards []map[string]*DatabaseConfig `yaml:"shards"`
}

// IsUsedSequencer returns whether 'sequencer' parameter is defined or not in table configuration.
func (c *TableConfig) IsUsedSequencer() bool {
	return c.IsShard && c.ShardColumnName != "" && c.Sequencer != nil
}

// ShardConfigByName returns DatabaseConfig instance by name of shards
func (c *TableConfig) ShardConfigByName(shardName string) *DatabaseConfig {
	for _, shard := range c.Shards {
		if cfg, exists := shard[shardName]; exists {
			return cfg
		}
	}
	return nil
}

// Error returns error of this table configuration.
func (c *TableConfig) Error() error {
	if !c.IsShard {
		return nil
	}
	if c.ShardColumnName != "" && c.Sequencer == nil {
		return errors.New("cannot find sequencer's definition in config file")
	}
	if c.ShardColumnName == "" && c.Sequencer != nil {
		return errors.New("cannot find shard_column in config file")
	}
	if c.ShardKeyColumnName == "" && c.ShardColumnName == "" && c.Sequencer == nil {
		return errors.New("cannot find shard_key in config file")
	}
	return nil
}

// A Config is a database configuration includes database sharding definition.
type Config struct {
	// distributed transaction support
	DistributedTransaction bool `yaml:"distributed_transaction"`
	// map table name and configuration
	Tables map[string]*TableConfig `yaml:"tables"`
}

// ShardColumnName column name of unique id for all shards
func (c *Config) ShardColumnName(tableName string) string {
	cfg, exists := c.Tables[tableName]
	if !exists {
		return ""
	}
	return cfg.ShardColumnName
}

// ShardKeyColumnName column name for deciding sharding target
func (c *Config) ShardKeyColumnName(tableName string) string {
	cfg, exists := c.Tables[tableName]
	if !exists {
		return ""
	}
	if cfg.ShardKeyColumnName == "" {
		return cfg.ShardColumnName
	}
	return cfg.ShardKeyColumnName
}

// IsShardTable returns whether 'is_shard' parameter is defined or not in table configuration.
func (c *Config) IsShardTable(tableName string) bool {
	cfg, exists := c.Tables[tableName]
	if !exists {
		return false
	}
	return cfg.IsShard
}

var globalConfig *Config

// Get get database configuration.
//
// If use this method, must call after Load().
// If call this method before Load(), it returns error
func Get() (*Config, error) {
	if globalConfig == nil {
		return nil, errors.New("must call config.Load() before config.Get()")
	}
	return globalConfig, nil
}

// Load load database configuration by file path.
func Load(configPath string) (*Config, error) {
	yamlFile, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	config := &Config{DistributedTransaction: true}
	if err := yaml.Unmarshal(yamlFile, &config); err != nil {
		return nil, errors.WithStack(err)
	}
	globalConfig = config
	return config, nil
}
