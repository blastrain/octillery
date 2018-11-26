package connection

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.knocknote.io/octillery/algorithm"
	"go.knocknote.io/octillery/config"
	adap "go.knocknote.io/octillery/connection/adapter"
)

type DBShardConnection struct {
	ShardName  string
	Connection *sql.DB
	Masters    []*sql.DB
	Slaves     []*sql.DB
}

type DBShardConnections struct {
	connMap  map[string]*DBShardConnection
	connList []*DBShardConnection
}

func (c *DBShardConnections) addConnection(conn *DBShardConnection) {
	if c.connMap == nil {
		c.connMap = make(map[string]*DBShardConnection)
	}
	if c.connList == nil {
		c.connList = make([]*DBShardConnection, 0)
	}
	c.connMap[conn.ShardName] = conn
	c.connList = append(c.connList, conn)
}

func (c *DBShardConnections) ShardConnectionByName(shardName string) *DBShardConnection {
	return c.connMap[shardName]
}

func (c *DBShardConnections) ShardConnectionByIndex(shardIndex int) *DBShardConnection {
	if shardIndex < len(c.connList) {
		return c.connList[shardIndex]
	}
	return nil
}

func (c *DBShardConnections) Close() error {
	var errs []string
	for _, conn := range c.connList {
		if err := closeConn(conn.Connection); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ":"))
	}
	return nil
}

func (c *DBShardConnections) ShardNum() int {
	return len(c.connList)
}

func (c *DBShardConnections) AllShard() []*DBShardConnection {
	return c.connList
}

type DBConnection struct {
	Config             *config.TableConfig
	Algorithm          algorithm.ShardingAlgorithm
	Adapter            adap.DBAdapter
	IsShard            bool
	IsUsedSequencer    bool
	Connection         *sql.DB
	Sequencer          *sql.DB
	ShardKeyColumnName string
	ShardColumnName    string
	ShardConnections   *DBShardConnections
}

type TxConnection struct {
	dbConn *DBConnection
	tx     *sql.Tx
	conn   *sql.DB
	ctx    context.Context
	opts   *sql.TxOptions
}

func (c *TxConnection) ValidateConnection(conn *DBConnection) error {
	if c.dbConn == nil {
		return nil
	}
	if !c.dbConn.EqualDSN(conn) {
		return errors.New("transaction error. cannot access other database by same Tx instance")
	}
	return nil
}

func (c *TxConnection) beginIfNotInitialized(conn *sql.DB) error {
	if c.tx != nil {
		return nil
	}
	if c.ctx != nil {
		tx, err := conn.BeginTx(c.ctx, c.opts)
		if err != nil {
			return errors.WithStack(err)
		}
		c.tx = tx
	} else {
		tx, err := conn.Begin()
		if err != nil {
			return errors.WithStack(err)
		}
		c.tx = tx
	}
	c.conn = conn
	return nil
}

func (c *TxConnection) Prepare(ctx context.Context, conn *sql.DB, query string) (*sql.Stmt, error) {
	if err := c.beginIfNotInitialized(conn); err != nil {
		return nil, errors.WithStack(err)
	}
	if ctx == nil {
		stmt, err := c.tx.Prepare(query)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return stmt, nil
	}
	stmt, err := c.tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return stmt, nil
}

func (c *TxConnection) Stmt(ctx context.Context, conn *sql.DB, stmt *sql.Stmt) (*sql.Stmt, error) {
	if err := c.beginIfNotInitialized(conn); err != nil {
		return nil, errors.WithStack(err)
	}
	if ctx == nil {
		return c.tx.Stmt(stmt), nil
	}
	return c.tx.StmtContext(ctx, stmt), nil
}

func (c *TxConnection) QueryRow(ctx context.Context, conn *sql.DB, query string, args ...interface{}) (*sql.Row, error) {
	if err := c.beginIfNotInitialized(conn); err != nil {
		return nil, errors.WithStack(err)
	}
	if ctx == nil {
		return c.tx.QueryRow(query, args...), nil
	}
	return c.tx.QueryRowContext(ctx, query, args...), nil
}

func (c *TxConnection) Query(ctx context.Context, conn *sql.DB, query string, args ...interface{}) (*sql.Rows, error) {
	if err := c.beginIfNotInitialized(conn); err != nil {
		return nil, errors.WithStack(err)
	}
	if ctx == nil {
		rows, err := c.tx.Query(query, args...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return rows, nil
	}
	rows, err := c.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return rows, nil
}

func (c *TxConnection) Exec(ctx context.Context, conn *sql.DB, query string, args ...interface{}) (sql.Result, error) {
	if err := c.beginIfNotInitialized(conn); err != nil {
		return nil, errors.WithStack(err)
	}
	if ctx == nil {
		result, err := c.tx.Exec(query, args...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return result, nil
	}
	result, err := c.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return result, nil
}

func (c *TxConnection) Commit() error {
	if c == nil {
		return errors.New("cannot commit. TxConnection is nil")
	}
	if c.tx == nil {
		return errors.New("cannot commit. Tx is nil")
	}
	return errors.WithStack(c.tx.Commit())
}

func (c *TxConnection) Rollback() error {
	if c == nil {
		return nil
	}
	if c.tx == nil {
		return nil
	}
	return errors.WithStack(c.tx.Rollback())
}

func (c *DBConnection) Begin(ctx context.Context, opts *sql.TxOptions) *TxConnection {
	return &TxConnection{dbConn: c, ctx: ctx, opts: opts}
}

func (c *DBConnection) NextSequenceID(tableName string) (int64, error) {
	if c.Sequencer == nil {
		return 0, errors.New("cannot get next sequence id")
	}
	return c.Adapter.NextSequenceID(c.Sequencer, sequencerTableName(tableName))
}

func (c *DBConnection) IsEqualShardColumnToShardKeyColumn() bool {
	if c.ShardKeyColumnName == "" {
		return true
	}
	return c.ShardColumnName == c.ShardKeyColumnName
}

func (c *DBConnection) ShardConnectionByID(id int64) (*DBShardConnection, error) {
	conns := []*sql.DB{}
	connMap := map[*sql.DB]*DBShardConnection{}
	for _, shardConn := range c.ShardConnections.AllShard() {
		connMap[shardConn.Connection] = shardConn
		conns = append(conns, shardConn.Connection)
	}
	dbConn, err := c.Algorithm.Shard(conns, id)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return connMap[dbConn], nil
}

func (c *DBConnection) EqualDSN(conn *DBConnection) bool {
	if c == conn {
		return true
	}
	if c.Config.NameOrPath != conn.Config.NameOrPath {
		return false
	}
	if len(c.Config.Masters) != len(conn.Config.Masters) {
		return false
	}
	if c.Config.IsShard != conn.Config.IsShard {
		return false
	}
	if c.Config.IsShard {
		for idx, cfg := range c.Config.Shards {
			for name, shard := range cfg {
				shardConn := conn.Config.Shards[idx][name]
				if shard.NameOrPath != shardConn.NameOrPath {
					return false
				}
				if len(shard.Masters) != len(shardConn.Masters) {
					return false
				}
				for idx, master := range shard.Masters {
					if master != shardConn.Masters[idx] {
						return false
					}
				}
			}
		}
	} else {
		for idx, master := range c.Config.Masters {
			if master != conn.Config.Masters[idx] {
				return false
			}
		}
	}
	return true
}

func (c *DBConnection) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if ctx == nil {
		rows, err := c.Connection.Query(query, args...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return rows, nil
	}

	rows, err := c.Connection.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return rows, nil
}

func (c *DBConnection) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	if ctx == nil {
		return c.Connection.QueryRow(query, args...)
	}
	return c.Connection.QueryRowContext(ctx, query, args...)
}

func (c *DBConnection) Prepare(ctx context.Context, query string) (*sql.Stmt, error) {
	if ctx == nil {
		stmt, err := c.Connection.Prepare(query)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return stmt, nil
	}
	stmt, err := c.Connection.PrepareContext(ctx, query)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return stmt, nil
}

func (c *DBConnection) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if ctx == nil {
		result, err := c.Connection.Exec(query, args...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return result, nil
	}
	result, err := c.Connection.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return result, nil
}

type DBConnectionMap struct {
	*sync.Map
}

func (m DBConnectionMap) Get(tableName string) *DBConnection {
	if conn, exists := m.Load(tableName); exists {
		return conn.(*DBConnection)
	}
	return nil
}

func (m DBConnectionMap) Set(tableName string, conn *DBConnection) {
	m.Store(tableName, conn)
}

func (m DBConnectionMap) Each(f func(string, *DBConnection) bool) {
	m.Range(func(k, v interface{}) bool {
		return f(k.(string), v.(*DBConnection))
	})
}

type DBConnectionManager struct {
	connMap         DBConnectionMap
	maxIdleConns    int
	maxOpenConns    int
	connMaxLifetime time.Duration
	queryString     string
}

func (cm *DBConnectionManager) SetQueryString(s string) error {
	idx := strings.Index(s, "?")
	if idx < 0 {
		return nil
	}
	u, err := url.Parse(s[idx:])
	if err != nil {
		return errors.WithStack(err)
	}
	cm.queryString = u.Query().Encode()
	return nil
}

func (cm *DBConnectionManager) SetMaxIdleConns(n int) {
	cm.maxIdleConns = n
}

func (cm *DBConnectionManager) SetMaxOpenConns(n int) {
	cm.maxOpenConns = n
}

func (cm *DBConnectionManager) SetConnMaxLifetime(d time.Duration) {
	cm.connMaxLifetime = d
}

func closeConn(conn *sql.DB) error {
	if conn == nil {
		return nil
	}
	return conn.Close()
}

func (cm *DBConnectionManager) Close() error {
	errs := []string{}
	cm.connMap.Each(func(tableName string, conn *DBConnection) bool {
		if conn.IsShard {
			if conn.IsUsedSequencer {
				if err := closeConn(conn.Sequencer); err != nil {
					errs = append(errs, err.Error())
				}
			}
			if err := conn.ShardConnections.Close(); err != nil {
				errs = append(errs, err.Error())
			}
		} else {
			if err := closeConn(conn.Connection); err != nil {
				errs = append(errs, err.Error())
			}
		}
		return true
	})
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ":"))
	}
	return nil
}

func (cm *DBConnectionManager) ConnectionByTableName(tableName string) (*DBConnection, error) {
	conn := cm.connMap.Get(tableName)
	if conn == nil {
		if err := cm.open(tableName); err != nil {
			return nil, errors.WithStack(err)
		}
		conn = cm.connMap.Get(tableName)
	}
	if conn == nil {
		return nil, errors.Errorf("cannot find database connection from table name %s", tableName)
	}
	return conn, nil
}

func (cm *DBConnectionManager) SequencerConnectionByTableName(tableName string) (*sql.DB, error) {
	conn, err := cm.ConnectionByTableName(tableName)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if conn.Sequencer == nil {
		return nil, errors.WithStack(err)
	}
	return conn.Sequencer, nil
}

func (cm *DBConnectionManager) CurrentSequenceID(tableName string) (int64, error) {
	conn, err := cm.ConnectionByTableName(tableName)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if conn.Sequencer == nil {
		return 0, errors.WithStack(err)
	}
	return conn.Adapter.CurrentSequenceID(conn.Sequencer, sequencerTableName(tableName))
}

func (cm *DBConnectionManager) NextSequenceID(tableName string) (int64, error) {
	conn, err := cm.ConnectionByTableName(tableName)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if conn.Sequencer == nil {
		return 0, errors.WithStack(err)
	}
	return conn.Adapter.NextSequenceID(conn.Sequencer, sequencerTableName(tableName))
}

func (cm *DBConnectionManager) IsShardTable(tableName string) bool {
	conn, err := cm.ConnectionByTableName(tableName)
	if err != nil {
		return false
	}
	return conn.IsShard
}

func (cm *DBConnectionManager) IsEqualShardColumnToShardKeyColumn(tableName string) bool {
	return cm.ShardColumnName(tableName) == cm.ShardKeyColumnName(tableName)
}

func (cm *DBConnectionManager) ShardColumnName(tableName string) string {
	conn, _ := cm.ConnectionByTableName(tableName)
	return conn.ShardColumnName
}

func (cm *DBConnectionManager) ShardKeyColumnName(tableName string) string {
	conn, _ := cm.ConnectionByTableName(tableName)
	if conn.ShardKeyColumnName == "" {
		return conn.ShardColumnName
	}
	return conn.ShardKeyColumnName
}

func (cm *DBConnectionManager) open(tableName string) error {
	for tblName, tableConfig := range globalConfig.Tables {
		if tableName != tblName {
			continue
		}
		if tableConfig.IsShard {
			return errors.WithStack(cm.openShardConnection(tableName, tableConfig))
		} else {
			return errors.WithStack(cm.openConnection(tableName, tableConfig))
		}
	}
	return errors.New("not found tableName in database config")
}

func (cm *DBConnectionManager) setConnectionSettings(conn *sql.DB) {
	if conn == nil {
		return
	}
	conn.SetMaxIdleConns(cm.maxIdleConns)
	conn.SetMaxOpenConns(cm.maxOpenConns)
	conn.SetConnMaxLifetime(cm.connMaxLifetime)
}

func (cm *DBConnectionManager) openShardConnection(tableName string, table *config.TableConfig) error {
	var seqConn *sql.DB
	if table.IsUsedSequencer() {
		adapter, err := adap.Adapter(table.Sequencer.Adapter)
		if err != nil {
			return errors.WithStack(err)
		}
		if seqConn, err = adapter.OpenConnection(table.Sequencer, cm.queryString); err != nil {
			return errors.WithStack(err)
		}
	}
	var adapter adap.DBAdapter
	shardConns := &DBShardConnections{}
	conns := make([]*sql.DB, 0)
	for _, shard := range table.Shards {
		for shardName, shardValue := range shard {
			var err error
			adapter, err = adap.Adapter(shardValue.Adapter)
			if err != nil {
				return errors.WithStack(err)
			}
			shardConn, err := adapter.OpenConnection(shardValue, cm.queryString)
			if err != nil {
				return errors.WithStack(err)
			}
			cm.setConnectionSettings(shardConn)
			conns = append(conns, shardConn)
			shardConns.addConnection(&DBShardConnection{
				ShardName:  shardName,
				Connection: shardConn,
			})
		}
	}
	logic, err := algorithm.LoadShardingAlgorithm(table.Algorithm)
	if err != nil {
		return errors.WithStack(err)
	}
	if !logic.Init(conns) {
		return errors.New("cannot initialize sharding algorithm")
	}
	cm.connMap.Set(tableName, &DBConnection{
		Config:             table,
		IsShard:            table.IsShard,
		Algorithm:          logic,
		Adapter:            adapter,
		IsUsedSequencer:    table.IsUsedSequencer(),
		Sequencer:          seqConn,
		ShardColumnName:    table.ShardColumnName,
		ShardKeyColumnName: table.ShardKeyColumnName,
		ShardConnections:   shardConns,
	})
	return nil
}

func (cm *DBConnectionManager) openConnection(tableName string, table *config.TableConfig) error {
	adapter, err := adap.Adapter(table.DatabaseConfig.Adapter)
	if err != nil {
		return errors.WithStack(err)
	}
	conn, err := adapter.OpenConnection(&table.DatabaseConfig, cm.queryString)
	if err != nil {
		return errors.WithStack(err)
	}
	cm.setConnectionSettings(conn)
	cm.connMap.Set(tableName, &DBConnection{
		Config:     table,
		Adapter:    adapter,
		Connection: conn,
	})
	return nil
}

var globalConfig *config.Config

func NewConnectionManager() (*DBConnectionManager, error) {
	if globalConfig == nil {
		return nil, errors.New("cannot setup from sharding config.")
	}
	connMgr := &DBConnectionManager{
		connMap:     DBConnectionMap{&sync.Map{}},
		queryString: "",
	}
	return connMgr, nil
}

func SetConfig(cfg *config.Config) error {
	globalConfig = cfg
	return errors.WithStack(setupDBFromConfig(cfg))
}

func setupDBFromConfig(config *config.Config) error {
	if config == nil {
		return errors.New("cannot setup database connection. config is nil")
	}
	for tableName, table := range config.Tables {
		var err error
		if table.IsShard {
			err = setupShardDB(tableName, table)
		} else {
			err = setupDB(tableName, table)
		}
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func insertRowToSequencerIfNotExists(conn *sql.DB, tableName string, adapter adap.DBAdapter) error {
	seqId, err := adapter.CurrentSequenceID(conn, sequencerTableName(tableName))
	if err != nil {
		return errors.WithStack(err)
	}
	if seqId == 0 {
		return adapter.InsertRowToSequencerIfNotExists(conn, sequencerTableName(tableName))
	}
	return nil
}

func sequencerTableName(tableName string) string {
	return fmt.Sprintf("%s_ids", tableName)
}

func setupShardDB(tableName string, table *config.TableConfig) error {
	if err := table.Error(); err != nil {
		return errors.WithStack(err)
	}
	if table.IsUsedSequencer() {
		adapter, err := adap.Adapter(table.Sequencer.Adapter)
		if err != nil {
			return errors.WithStack(err)
		}
		if err := adapter.ExecDDL(table.Sequencer); err != nil {
			return errors.WithStack(err)
		}
		seqConn, err := adapter.OpenConnection(table.Sequencer, "")
		defer closeConn(seqConn)
		if err != nil {
			return errors.WithStack(err)
		}
		if err := adapter.CreateSequencerTableIfNotExists(seqConn, sequencerTableName(tableName)); err != nil {
			return errors.WithStack(err)
		}
		if err := insertRowToSequencerIfNotExists(seqConn, tableName, adapter); err != nil {
			return errors.WithStack(err)
		}
	}
	for _, shard := range table.Shards {
		for _, shardValue := range shard {
			adapter, err := adap.Adapter(shardValue.Adapter)
			if err != nil {
				return errors.WithStack(err)
			}
			if err := adapter.ExecDDL(shardValue); err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return nil
}

func setupDB(tableName string, table *config.TableConfig) error {
	adapter, err := adap.Adapter(table.DatabaseConfig.Adapter)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(adapter.ExecDDL(&table.DatabaseConfig))
}
