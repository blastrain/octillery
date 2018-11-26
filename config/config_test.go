package config

import (
	"path/filepath"
	"testing"

	"go.knocknote.io/octillery/path"
)

func TestError(t *testing.T) {
	t.Run("get before load", func(t *testing.T) {
		cfg, err := Get()
		if cfg != nil {
			t.Fatal("invalid config instance")
		}
		if err == nil {
			t.Fatal("cannot handle error")
		}
	})
	// invalid file path
	if _, err := Load(""); err == nil {
		t.Fatal("cannot handle error")
	}
	// invalid yaml syntax
	if _, err := Load(filepath.Join(path.ThisDirPath(), "config.go")); err == nil {
		t.Fatal("cannot handle error")
	}
	// load invalid config file
	confPath := filepath.Join(path.ThisDirPath(), "invalid_config.yml")
	cfg, err := Load(confPath)
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	if cfg.ShardColumnName("invalid_table_name") != "" {
		t.Fatal("cannot handle error")
	}
	if cfg.ShardKeyColumnName("invalid_table_name") != "" {
		t.Fatal("cannot handle error")
	}
	if cfg.IsShardTable("invalid_table_name") {
		t.Fatal("cannot handle error")
	}
	if err := cfg.Tables["exists_shard_column_but_not_sequencer"].Error(); err == nil {
		t.Fatal("cannot handle error")
	}
	if err := cfg.Tables["not_shard_column_but_exists_sequencer"].Error(); err == nil {
		t.Fatal("cannot handle error")
	}
	if err := cfg.Tables["not_shard_key"].Error(); err == nil {
		t.Fatal("cannot handle error")
	}
}

func TestConfig(t *testing.T) {
	confPath := filepath.Join(path.ThisDirPath(), "..", "test_databases.yml")
	t.Run("load", func(t *testing.T) {
		cfg, err := Load(confPath)
		if err != nil {
			t.Fatalf("%+v\n", err)
		}
		if cfg == nil {
			t.Fatal("cannot load config")
		}
	})
	t.Run("get after load", func(t *testing.T) {
		cfg, err := Get()
		if err != nil {
			t.Fatalf("%+v\n", err)
		}
		if cfg == nil {
			t.Fatal("cannot get config instance")
		}
	})
	t.Run("shard column name", func(t *testing.T) {
		cfg, _ := Get()
		if cfg.ShardColumnName("users") != "id" {
			t.Fatal("cannot get shard column name from config")
		}
		if cfg.ShardColumnName("user_items") != "" {
			t.Fatal("cannot get shard column name from config")
		}
		if cfg.ShardColumnName("user_decks") != "id" {
			t.Fatal("cannot get shard column name from config")
		}
		if cfg.ShardColumnName("user_stages") != "" {
			t.Fatal("cannot get shard column name from config")
		}
	})
	t.Run("shard key column name", func(t *testing.T) {
		cfg, _ := Get()
		if cfg.ShardKeyColumnName("users") != "id" {
			t.Fatal("cannot get shard column name from config")
		}
		if cfg.ShardKeyColumnName("user_items") != "user_id" {
			t.Fatal("cannot get shard column name from config")
		}
		if cfg.ShardKeyColumnName("user_decks") != "user_id" {
			t.Fatal("cannot get shard column name from config")
		}
		if cfg.ShardKeyColumnName("user_stages") != "" {
			t.Fatal("cannot get shard column name from config")
		}
	})
	t.Run("is shard table", func(t *testing.T) {
		cfg, _ := Get()
		if !cfg.IsShardTable("users") {
			t.Fatal("not work")
		}
		if !cfg.IsShardTable("user_items") {
			t.Fatal("not work")
		}
		if !cfg.IsShardTable("user_decks") {
			t.Fatal("not work")
		}
		if cfg.IsShardTable("user_stages") {
			t.Fatal("not work")
		}
	})
	t.Run("table config error", func(t *testing.T) {
		cfg, _ := Get()
		for _, tableConfig := range cfg.Tables {
			if err := tableConfig.Error(); err != nil {
				t.Fatalf("%+v\n", err)
			}
		}
	})
	t.Run("get shard config by name", func(t *testing.T) {
		cfg, _ := Get()
		if shard := cfg.Tables["users"].ShardConfigByName("user_shard_1"); shard == nil {
			t.Fatal("not work")
		}
		if shard := cfg.Tables["users"].ShardConfigByName("hoge"); shard != nil {
			t.Fatal("not work")
		}
	})
	t.Run("is used sequencer", func(t *testing.T) {
		cfg, _ := Get()
		if !cfg.Tables["users"].IsUsedSequencer() {
			t.Fatal("not work")
		}
		if cfg.Tables["user_items"].IsUsedSequencer() {
			t.Fatal("not work")
		}
		if !cfg.Tables["user_decks"].IsUsedSequencer() {
			t.Fatal("not work")
		}
		if cfg.Tables["user_stages"].IsUsedSequencer() {
			t.Fatal("not work")
		}
	})
}
