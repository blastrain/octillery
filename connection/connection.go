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

var (
	globalConfig *config.Config
)

// QueryLog type for storing information of executed query
type QueryLog struct {
	Query        string        `json:"query"`
	Args         []interface{} `json:"args"`
	LastInsertID int64         `json:"lastInsertId"`
}

// Connection common interface for DBConnection and DBShardConnection
type Connection interface {
	DSN() string
	Conn() *sql.DB
}

// DBShardConnection has connection to sharded database.
type DBShardConnection struct {
	ShardName  string
	Connection *sql.DB
	Masters    []*sql.DB
	Slaves     []*sql.DB
	dsn        string
}

// DSN returns DSN for shard
func (c *DBShardConnection) DSN() string {
	return c.dsn
}

// Conn returns *sql.DB instance for shard
func (c *DBShardConnection) Conn() *sql.DB {
	return c.Connection
}

// DBShardConnections has all DBShardConnection instances.
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

// ShardConnectionByName returns DBShardConnection structure by database name
func (c *DBShardConnections) ShardConnectionByName(shardName string) *DBShardConnection {
	return c.connMap[shardName]
}

// ShardConnectionByIndex returns DBShardConnection structure by index of shards
func (c *DBShardConnections) ShardConnectionByIndex(shardIndex int) *DBShardConnection {
	if shardIndex < len(c.connList) {
		return c.connList[shardIndex]
	}
	return nil
}

// Close close all database connections for shards
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

// ShardNum returns number of shards
func (c *DBShardConnections) ShardNum() int {
	return len(c.connList)
}

// AllShard returns slice of DBShardConnection structure
func (c *DBShardConnections) AllShard() []*DBShardConnection {
	return c.connList
}

// DBConnection has connection to sequencer or master server or all shards
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

// TxConnection manage transaction
type TxConnection struct {
	dsnList                    []string
	dsnToTx                    map[string]*sql.Tx
	txToWriteQueries           map[*sql.Tx][]*QueryLog
	ctx                        context.Context
	opts                       *sql.TxOptions
	WriteQueries               []*QueryLog
	ReadQueries                []*QueryLog
	BeforeCommitCallback       func() error
	AfterCommitSuccessCallback func() error
	AfterCommitFailureCallback func(bool, []*QueryLog) error
}

func (c *TxConnection) beginIfNotInitialized(conn Connection) error {
	dsn := conn.DSN()
	tx := c.dsnToTx[dsn]
	if !globalConfig.DistributedTransaction {
		entries := len(c.dsnToTx)
		if entries > 0 && tx == nil {
			return errors.New("transaction error. cannot access other database by same Tx instance")
		}
	}
	if tx != nil {
		return nil
	}
	newTx, err := func() (*sql.Tx, error) {
		if c.ctx != nil {
			return conn.Conn().BeginTx(c.ctx, c.opts)
		}
		return conn.Conn().Begin()
	}()
	if err != nil {
		return errors.WithStack(err)
	}
	c.dsnList = append(c.dsnList, dsn)
	c.dsnToTx[dsn] = newTx
	return nil
}

// Prepare executes `Prepare` with transaction.
func (c *TxConnection) Prepare(ctx context.Context, conn Connection, query string) (*sql.Stmt, error) {
	if err := c.beginIfNotInitialized(conn); err != nil {
		return nil, errors.WithStack(err)
	}
	tx := c.dsnToTx[conn.DSN()]
	stmt, err := func() (*sql.Stmt, error) {
		if ctx == nil {
			return tx.Prepare(query)
		}
		return tx.PrepareContext(ctx, query)
	}()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return stmt, nil
}

func (c *TxConnection) AddWriteQuery(conn Connection, result sql.Result, query string, args ...interface{}) error {
	id, err := result.LastInsertId()
	if err != nil {
		return errors.WithStack(err)
	}
	queryLog := &QueryLog{
		Query:        query,
		Args:         args,
		LastInsertID: id,
	}
	tx := c.dsnToTx[conn.DSN()]
	c.txToWriteQueries[tx] = append(c.txToWriteQueries[tx], queryLog)
	c.WriteQueries = append(c.WriteQueries, queryLog)
	return nil
}

func (c *TxConnection) AddReadQuery(query string, args ...interface{}) {
	c.ReadQueries = append(c.ReadQueries, &QueryLog{
		Query: query,
		Args:  args,
	})
}

// Stmt executes `Stmt` with transaction.
func (c *TxConnection) Stmt(ctx context.Context, conn Connection, stmt *sql.Stmt) (*sql.Stmt, error) {
	if err := c.beginIfNotInitialized(conn); err != nil {
		return nil, errors.WithStack(err)
	}
	tx := c.dsnToTx[conn.DSN()]
	if ctx == nil {
		return tx.Stmt(stmt), nil
	}
	return tx.StmtContext(ctx, stmt), nil
}

// QueryRow executes `QueryRow` with transaction.
func (c *TxConnection) QueryRow(ctx context.Context, conn Connection, query string, args ...interface{}) (*sql.Row, error) {
	if err := c.beginIfNotInitialized(conn); err != nil {
		return nil, errors.WithStack(err)
	}
	tx := c.dsnToTx[conn.DSN()]
	row := func() *sql.Row {
		if ctx == nil {
			return tx.QueryRow(query, args...)
		}
		return tx.QueryRowContext(ctx, query, args...)
	}()
	c.ReadQueries = append(c.ReadQueries, &QueryLog{
		Query: query,
		Args:  args,
	})
	return row, nil
}

// Query executes `Query` with transaction.
func (c *TxConnection) Query(ctx context.Context, conn Connection, query string, args ...interface{}) (*sql.Rows, error) {
	if err := c.beginIfNotInitialized(conn); err != nil {
		return nil, errors.WithStack(err)
	}
	tx := c.dsnToTx[conn.DSN()]
	rows, err := func() (*sql.Rows, error) {
		if ctx == nil {
			return tx.Query(query, args...)
		}
		return tx.QueryContext(ctx, query, args...)
	}()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	c.ReadQueries = append(c.ReadQueries, &QueryLog{
		Query: query,
		Args:  args,
	})
	return rows, nil
}

// Exec executes `Exec` with transaction.
func (c *TxConnection) Exec(ctx context.Context, conn Connection, query string, args ...interface{}) (sql.Result, error) {
	if err := c.beginIfNotInitialized(conn); err != nil {
		return nil, errors.WithStack(err)
	}
	tx := c.dsnToTx[conn.DSN()]
	result, err := func() (sql.Result, error) {
		if ctx == nil {
			return tx.Exec(query, args...)
		}
		return tx.ExecContext(ctx, query, args...)
	}()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	queryLog := &QueryLog{
		Query:        query,
		Args:         args,
		LastInsertID: id,
	}
	c.txToWriteQueries[tx] = append(c.txToWriteQueries[tx], queryLog)
	c.WriteQueries = append(c.WriteQueries, queryLog)
	return result, nil
}

// Commit executes `Commit` with transaction.
func (c *TxConnection) Commit() (e error) {
	if c == nil {
		return nil
	}
	if len(c.dsnToTx) == 0 {
		return nil
	}
	if err := c.BeforeCommitCallback(); err != nil {
		return errors.WithStack(err)
	}
	committedWriteQueryNum := 0
	failedWriteQueries := []*QueryLog{}
	isCriticalError := false

	defer func() {
		if len(failedWriteQueries) == 0 {
			if err := c.AfterCommitSuccessCallback(); err != nil {
				e = errors.WithStack(err)
			}
		} else if len(failedWriteQueries) > 0 {
			if err := c.AfterCommitFailureCallback(isCriticalError, failedWriteQueries); err != nil {
				e = errors.WithStack(err)
			}
		}
	}()

	errs := []string{}
	for _, dsn := range c.dsnList {
		tx := c.dsnToTx[dsn]
		if err := tx.Commit(); err != nil {
			failedWriteQueries = append(failedWriteQueries, c.txToWriteQueries[tx]...)
			if committedWriteQueryNum > 0 {
				// distributed transaction error
				isCriticalError = true
				errs = append(errs, errors.Wrapf(err, "cannot commit to %s", dsn).Error())
			} else {
				return errors.Wrapf(err, "cannot commit to %s", dsn)
			}
		} else {
			committedWriteQueryNum += len(c.txToWriteQueries[tx])
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ":"))
	}
	return nil
}

// Rollback executes `Rollback` with transaction.
func (c *TxConnection) Rollback() error {
	if c == nil {
		return nil
	}
	if len(c.dsnToTx) == 0 {
		return nil
	}
	errs := []string{}
	for _, tx := range c.dsnToTx {
		if err := tx.Rollback(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ":"))
	}
	return nil
}

// DSN returns DSN for not sharded database
func (c *DBConnection) DSN() string {
	cfg := c.Config
	if len(cfg.Masters) > 0 {
		return fmt.Sprintf("%s/%s", cfg.Masters[0], cfg.NameOrPath)
	}
	return fmt.Sprintf("%s", cfg.NameOrPath)
}

// Conn returns *sql.DB for not sharded database
func (c *DBConnection) Conn() *sql.DB {
	return c.Connection
}

// Begin creates TxConnection instance for transaction.
func (c *DBConnection) Begin(ctx context.Context, opts *sql.TxOptions) *TxConnection {
	return &TxConnection{
		dsnList:                    []string{},
		dsnToTx:                    map[string]*sql.Tx{},
		txToWriteQueries:           map[*sql.Tx][]*QueryLog{},
		ctx:                        ctx,
		opts:                       opts,
		BeforeCommitCallback:       func() error { return nil },
		AfterCommitSuccessCallback: func() error { return nil },
		AfterCommitFailureCallback: func(bool, []*QueryLog) error { return nil },
	}
}

// NextSequenceID returns next unique id by sequencer table name.
func (c *DBConnection) NextSequenceID(tableName string) (int64, error) {
	if c.Sequencer == nil {
		return 0, errors.New("cannot get next sequence id")
	}
	return c.Adapter.NextSequenceID(c.Sequencer, sequencerTableName(tableName))
}

// IsEqualShardColumnToShardKeyColumn returns whether shard_column value equals to shard_key value or not.
func (c *DBConnection) IsEqualShardColumnToShardKeyColumn() bool {
	if c.ShardKeyColumnName == "" {
		return true
	}
	return c.ShardColumnName == c.ShardKeyColumnName
}

// ShardConnectionByID returns connection to shard by unique id.
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

// EqualDSN returns whether connection is same DSN connection that executed SQL previously or not.
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

// Query executes `Query` (not shards).
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

// QueryRow executes `QueryRow` (not shards).
func (c *DBConnection) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	if ctx == nil {
		return c.Connection.QueryRow(query, args...)
	}
	return c.Connection.QueryRowContext(ctx, query, args...)
}

// Prepare executes `Prepare` (not shards).
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

// Exec executes `Exec` (not shards).
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

// DBConnectionMap has all DBConnection.
type DBConnectionMap struct {
	*sync.Map
}

// Get get DBConnection instance by table name.
func (m DBConnectionMap) Get(tableName string) *DBConnection {
	if conn, exists := m.Load(tableName); exists {
		return conn.(*DBConnection)
	}
	return nil
}

// Set set DBConnection instance with table name.
func (m DBConnectionMap) Set(tableName string, conn *DBConnection) {
	m.Store(tableName, conn)
}

// Each iterate all DBConnections.
func (m DBConnectionMap) Each(f func(string, *DBConnection) bool) {
	m.Range(func(k, v interface{}) bool {
		return f(k.(string), v.(*DBConnection))
	})
}

// DBConnectionManager has DBConnectionMap and settings to connection of database
type DBConnectionManager struct {
	connMap         DBConnectionMap
	maxIdleConns    int
	maxOpenConns    int
	connMaxLifetime time.Duration
	queryString     string
}

// SetQueryString set up query string like `?parseTime=true`
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

// SetMaxIdleConns compatible interface of SetMaxIdleConns in 'database/sql' package
func (cm *DBConnectionManager) SetMaxIdleConns(n int) {
	cm.maxIdleConns = n
}

// SetMaxOpenConns compatible interface of SetMaxOpenConns in 'database/sql' package
func (cm *DBConnectionManager) SetMaxOpenConns(n int) {
	cm.maxOpenConns = n
}

// SetConnMaxLifetime compatible interface of SetConnMaxLifetime in 'database/sql' package
func (cm *DBConnectionManager) SetConnMaxLifetime(d time.Duration) {
	cm.connMaxLifetime = d
}

func closeConn(conn *sql.DB) error {
	if conn == nil {
		return nil
	}
	return conn.Close()
}

// Close close all connections
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

// ConnectionByTableName returns DBConnection instance by table name
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

// SequencerConnectionByTableName returns `*sql.DB` instance by table name
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

// CurrentSequenceID returns current unique id by table name of sequencer
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

// NextSequenceID returns next unique id by table name of sequencer
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

// IsShardTable whether sharding table or not.
func (cm *DBConnectionManager) IsShardTable(tableName string) bool {
	conn, err := cm.ConnectionByTableName(tableName)
	if err != nil {
		return false
	}
	return conn.IsShard
}

// IsEqualShardColumnToShardKeyColumn returns whether shard_column value equals to shard_key value or not.
func (cm *DBConnectionManager) IsEqualShardColumnToShardKeyColumn(tableName string) bool {
	return cm.ShardColumnName(tableName) == cm.ShardKeyColumnName(tableName)
}

// ShardColumnName returns shard_column value by table name
func (cm *DBConnectionManager) ShardColumnName(tableName string) string {
	conn, _ := cm.ConnectionByTableName(tableName)
	return conn.ShardColumnName
}

// ShardKeyColumnName returns shard_key value by table name
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
		}
		return errors.WithStack(cm.openConnection(tableName, tableConfig))
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
			var dsn string
			if len(shardValue.Masters) > 0 {
				dsn = fmt.Sprintf("%s/%s", shardValue.Masters[0], shardValue.NameOrPath)
			} else {
				dsn = shardValue.NameOrPath
			}
			shardConns.addConnection(&DBShardConnection{
				ShardName:  shardName,
				Connection: shardConn,
				dsn:        dsn,
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

// NewConnectionManager creates instance of DBConnectionManager,
// If call this before loads configuration file, it returns error.
func NewConnectionManager() (*DBConnectionManager, error) {
	if globalConfig == nil {
		return nil, errors.New("cannot setup from sharding config")
	}
	connMgr := &DBConnectionManager{
		connMap:     DBConnectionMap{&sync.Map{}},
		queryString: "",
	}
	return connMgr, nil
}

// SetConfig set config.Config instance to internal global variable
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
	seqID, err := adapter.CurrentSequenceID(conn, sequencerTableName(tableName))
	if err != nil {
		return errors.WithStack(err)
	}
	if seqID == 0 {
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
