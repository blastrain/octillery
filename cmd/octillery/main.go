package main

import (
	"bufio"
	coresql "database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/logic"
	flags "github.com/jessevdk/go-flags"
	vtparser "github.com/knocknote/vitess-sqlparser/sqlparser"
	"github.com/pkg/errors"
	"go.knocknote.io/octillery"
	"go.knocknote.io/octillery/algorithm"
	"go.knocknote.io/octillery/config"
	"go.knocknote.io/octillery/connection"
	_ "go.knocknote.io/octillery/connection/adapter/plugin"
	"go.knocknote.io/octillery/database/sql"
	"go.knocknote.io/octillery/migrator"
	"go.knocknote.io/octillery/printer"
	"go.knocknote.io/octillery/sqlparser"
	"go.knocknote.io/octillery/transposer"
)

// Option type for command line options
type Option struct {
	Version   VersionCommand   `description:"print the version of octillery" command:"version"`
	Transpose TransposeCommand `description:"replace 'database/sql' to 'go.knocknote.io/octillery/database/sql'" command:"transpose"`
	Migrate   MigrateCommand   `description:"migrate database schema ( powered by schemalex )" command:"migrate"`
	Import    ImportCommand    `description:"import seeds" command:"import"`
	Console   ConsoleCommand   `description:"database console" command:"console"`
	Install   InstallCommand   `description:"install database adapter" command:"install"`
	Shard     ShardCommand     `description:"get sharded database information by sharding key" command:"shard"`
}

// VersionCommand type for version command
type VersionCommand struct {
}

// TransposeCommand type for transpose command
type TransposeCommand struct {
	DryRun bool     `long:"dry-run" description:"show diff only"`
	Ignore []string `long:"ignore"  description:"ignore directory or file"`
}

// MigrateCommand type for migrate command
type MigrateCommand struct {
	DryRun bool                 `long:"dry-run"           description:"show diff only"`
	Quiet  bool                 `long:"quiet"   short:"q" description:"not print logs during migration"`
	Config string               `long:"config"  short:"c" description:"database configuration file path" required:"config path"`
	Online OnlineMigrateCommand `description:"online migration ( powered by gh-ost )" command:"online"`
}

type OnlineMigrateCommand struct {
	Host                 string `long:"host"               default:"127.0.0.1" description:"MySQL hostname (preferably a replica, not the master)"`
	AssumeMasterHostname string `long:"assume-master-host" default:""          description:"(optional) explicitly tell gh-ost the identity of the master. Format: some.host.com[:port] This is useful in master-master setups where you wish to pick an explicit master, or in a tungsten-replicator where gh-ost is unable to determine the master"`
	Port                 int    `long:"port"               default:"3306"      description:"MySQL port (preferably a replica, not the master)"`
	User                 string `long:"user"               default:""          description:"MySQL user"`
	Password             string `long:"password"           default:""          description:"MySQL password"`
	MasterUser           string `long:"master-user"        default:""          description:"MySQL user on master, if different from that on replica. Requires --assume-master-host"`
	MasterPassword       string `long:"master-password"    default:""          description:"MySQL password on master, if different from that on replica. Requires --assume-master-host"`
	ConfigFile           string `long:"conf"               default:""          description:"Config file"`
	AskPass              string `long:"ask-pass"           default:"false"     description:"prompt for MySQL password"`

	DatabaseName             string `long:"database"           default:""          description:"database name (mandatory)"`
	OriginalTableName        string `long:"table"              default:""          description:"table name (mandatory)"`
	AlterStatement           string `long:"alter"              default:""          description:"alter statement (mandatory)"`
	CountTableRows           string `long:"exact-rowcount"     default:"false"     description:"actually count table rows as opposed to estimate them (results in more accurate progress estimation)"`
	ConcurrentCountTableRows string `long:"concurrent-rowcount" default:"true"     description:"(with --exact-rowcount), when true (default): count rows after row-copy begins, concurrently, and adjust row estimate later on; when false: first count rows, then start row copy"`
	AllowedRunningOnMaster   string `long:"allow-on-master"     default:"false"    description:"allow this migration to run directly on master. Preferably it would run on a replica"`
	AllowedMasterMaster      string `long:"allow-master-master" default:"false"    description:"explicitly allow running in a master-master setup"`
	NullableUniqueKeyAllowed string `long:"allow-nullable-unique-key" default:"false" description:"allow gh-ost to migrate based on a unique key with nullable columns. As long as no NULL values exist, this should be OK. If NULL values exist in chosen key, data may be corrupted. Use at your own risk!"`
	ApproveRenamedColumns    string `long:"approve-renamed-columns" default:"false" description:"in case your ALTER statement renames columns, gh-ost will note that and offer its interpretation of the rename. By default gh-ost does not proceed to execute. This flag approves that gh-ost's interpretation is correct"`
	SkipRenamedColumns       string `long:"skip-renamed-columns" default:"false" description:"in case your ALTER statement renames columns, gh-ost will note that and offer its interpretation of the rename. By default gh-ost does not proceed to execute. This flag tells gh-ost to skip the renamed columns, i.e. to treat what gh-ost thinks are renamed columns as unrelated columns. NOTE: you may lose column data"`
	IsTungsten               string `long:"tungsten" default:"false" description:"explicitly let gh-ost know that you are running on a tungsten-replication based topology (you are likely to also provide --assume-master-host)"`
	DiscardForeignKeys       string `long:"discard-foreign-keys" default:"false" description:"DANGER! This flag will migrate a table that has foreign keys and will NOT create foreign keys on the ghost table, thus your altered table will have NO foreign keys. This is useful for intentional dropping of foreign keys"`
	SkipForeignKeyChecks     string `long:"skip-foreign-key-checks" default:"false" description:"set to 'true' when you know for certain there are no foreign keys on your table, and wish to skip the time it takes for gh-ost to verify that"`
	AliyunRDS                string `long:"aliyun-rds" default:"false" description:"set to 'true' when you execute on Aliyun RDS."`
	GoogleCloudPlatform      string `long:"gcp" default:"false" description:"set to 'true' when you execute on a 1st generation Google Cloud Platform (GCP)."`

	ExecuteFlag                  string `long:"execute" default:"false" description:"actually execute the alter & migrate the table. Default is noop: do some tests and exit"`
	TestOnReplica                string `long:"test-on-replica" default:"false" description:"Have the migration run on a replica, not on the master. At the end of migration replication is stopped, and tables are swapped and immediately swap-revert. Replication remains stopped and you can compare the two tables for building trust"`
	TestOnReplicaSkipReplicaStop string `long:"test-on-replica-skip-replica-stop" default:"false" description:"When --test-on-replica is enabled, do not issue commands stop replication (requires --test-on-replica)"`
	MigrateOnReplica             string `long:"migrate-on-replica" default:"false" description:"Have the migration run on a replica, not on the master. This will do the full migration on the replica including cut-over (as opposed to --test-on-replica)"`

	OkToDropTable            string `long:"ok-to-drop-table" default:"false" description:"Shall the tool drop the old table at end of operation. DROPping tables can be a long locking operation, which is why I'm not doing it by default. I'm an online tool, yes?"`
	InitiallyDropOldTable    string `long:"initially-drop-old-table" default:"false" description:"Drop a possibly existing OLD table (remains from a previous run?) before beginning operation. Default is to panic and abort if such table exists"`
	InitiallyDropGhostTable  string `long:"initially-drop-ghost-table" default:"false" description:"Drop a possibly existing Ghost table (remains from a previous run?) before beginning operation. Default is to panic and abort if such table exists"`
	TimestampOldTable        string `long:"timestamp-old-table" default:"false" description:"Use a timestamp in old table name. This makes old table names unique and non conflicting cross migrations"`
	CutOver                  string `long:"cut-over" default:"atomic" description:"choose cut-over type (default|atomic, two-step)"`
	ForceNamedCutOverCommand string `long:"force-named-cut-over" default:"false" description:"When true, the 'unpostpone|cut-over' interactive command must name the migrated table"`

	SwitchToRowBinlogFormat   string `long:"switch-to-rbr" default:"false" description:"let this tool automatically switch binary log format to 'ROW' on the replica, if needed. The format will NOT be switched back. I'm too scared to do that, and wish to protect you if you happen to execute another migration while this one is running"`
	AssumeRBR                 string `long:"assume-rbr" default:"false" description:"set to 'true' when you know for certain your server uses 'ROW' binlog_format. gh-ost is unable to tell, event after reading binlog_format, whether the replication process does indeed use 'ROW', and restarts replication to be certain RBR setting is applied. Such operation requires SUPER privileges which you might not have. Setting this flag avoids restarting replication and you can proceed to use gh-ost without SUPER privileges"`
	CutOverExponentialBackoff string `long:"cut-over-exponential-backoff" default:"false" description:"Wait exponentially longer intervals between failed cut-over attempts. Wait intervals obey a maximum configurable with 'exponential-backoff-max-interval')."`

	ExponentialBackoffMaxInterval int64   `long:"exponential-backoff-max-interval" default:"64" description:"Maximum number of seconds to wait between attempts when performing various operations with exponential backoff."`
	ChunkSize                     int64   `long:"chunk-size" default:"1000" description:"amount of rows to handle in each iteration (allowed range: 100-100,000)"`
	DmlBatchSize                  int64   `long:"dml-batch-size" default:"10" description:"batch size for DML events to apply in a single transaction (range 1-100)"`
	DefaultRetries                int64   `long:"default-retries" default:"60" description:"Default number of retries for various operations before panicking"`
	CutOverLockTimeoutSeconds     int64   `long:"cut-over-lock-timeout-seconds" default:"3" description:"Max number of seconds to hold locks on tables while attempting to cut-over (retry attempted when lock exceeds timeout)"`
	NiceRatio                     float64 `long:"nice-ratio" default:"0" description:"force being 'nice', imply sleep time per chunk time; range: [0.0..100.0]. Example values: 0 is aggressive. 1: for every 1ms spent copying rows, sleep additional 1ms (effectively doubling runtime); 0.7: for every 10ms spend in a rowcopy chunk, spend 7ms sleeping immediately after"`

	MaxLagMillis               int64  `long:"max-lag-millis" default:"1500" description:"replication lag at which to throttle operation"`
	ReplicationLagQuery        string `long:"replication-lag-query" default:"" description:"Deprecated. gh-ost uses an internal, subsecond resolution query"`
	ThrottleControlReplicas    string `long:"throttle-control-replicas" default:"" description:"List of replicas on which to check for lag; comma delimited. Example: myhost1.com:3306,myhost2.com,myhost3.com:3307"`
	ThrottleQuery              string `long:"throttle-query" default:"" description:"when given, issued (every second) to check if operation should throttle. Expecting to return zero for no-throttle, >0 for throttle. Query is issued on the migrated server. Make sure this query is lightweight"`
	ThrottleHTTP               string `long:"throttle-http" default:"" description:"when given, gh-ost checks given URL via HEAD request; any response code other than 200 (OK) causes throttling; make sure it has low latency response"`
	HeartbeatIntervalMillis    int64  `long:"heartbeat-interval-millis" default:"100" description:"how frequently would gh-ost inject a heartbeat value"`
	ThrottleFlagFile           string `long:"throttle-flag-file" default:"" description:"operation pauses when this file exists; hint: use a file that is specific to the table being altered"`
	ThrottleAdditionalFlagFile string `long:"throttle-additional-flag-file" default:"/tmp/gh-ost.throttle" description:"operation pauses when this file exists; hint: keep default, use for throttling multiple gh-ost operations"`
	PostponeCutOverFlagFile    string `long:"postpone-cut-over-flag-file" default:"" description:"while this file exists, migration will postpone the final stage of swapping tables, and will keep on syncing the ghost table. Cut-over/swapping would be ready to perform the moment the file is deleted."`
	PanicFlagFile              string `long:"panic-flag-file" default:"" description:"when this file is created, gh-ost will immediately terminate, without cleanup"`

	DropServeSocket string `long:"initially-drop-socket-file" default:"false" description:"Should gh-ost forcibly delete an existing socket file. Be careful: this might drop the socket file of a running migration!"`
	ServeSocketFile string `long:"serve-socket-file" default:"" description:"Unix socket file to serve on. Default: auto-determined and advertised upon startup"`
	ServeTCPPort    int64  `long:"serve-tcp-port" default:"0" description:"TCP port to serve on. Default: disabled"`

	HooksPath        string `long:"hooks-path" default:"" description:"directory where hook files are found (default: empty, ie. hooks disabled). Hook files found on this path, and conforming to hook naming conventions will be executed"`
	HooksHintMessage string `long:"hooks-hint" default:"" description:"arbitrary message to be injected to hooks via GH_OST_HOOKS_HINT, for your convenience"`

	ReplicaServerId uint `long:"replica-server-id" default:"99999" description:"server id used by gh-ost process. Default: 99999"`

	MaxLoad                          string `long:"max-load" default:"" description:"Comma delimited status-name=threshold. e.g: 'Threads_running=100,Threads_connected=500'. When status exceeds threshold, app throttles writes"`
	CriticalLoad                     string `long:"critical-load" default:"" description:"Comma delimited status-name=threshold, same format as --max-load. When status exceeds threshold, app panics and quits"`
	CriticalLoadIntervalMilliseconds int64  `long:"critical-load-interval-millis" default:"0" description:"When 0, migration immediately bails out upon meeting critical-load. When non-zero, a second check is done after given interval, and migration only bails out if 2nd check still meets critical load"`
	CriticalLoadHibernateSeconds     int64  `long:"critical-load-hibernate-seconds" default:"0" description:"When nonzero, critical-load does not panic and bail out; instead, gh-ost goes into hibernate for the specified duration. It will not read/write anything to from/to any server"`
	Quiet                            string `long:"quiet" default:"false" description:"quiet"`
	Verbose                          string `long:"verbose" description:"false" description:"verbose"`
	Debug                            string `long:"debug" default:"false" description:"debug mode (very verbose)"`
	Stack                            string `long:"stack" default:"false" description:"add stack trace upon error"`
	Help                             string `long:"help" default:"false" description:"Display usage"`
	Version                          string `long:"version" default:"false" description:"Print version & exit"`
	CheckFlag                        string `long:"check-flag" default:"false" description:"Check if another flag exists/supported. This allows for cross-version scripting. Exits with 0 when all additional provided flags exist, nonzero otherwise. You must provide (dummy) values for flags that require a value. Example: gh-ost --check-flag --cut-over-lock-timeout-seconds --nice-ratio 0"`
	ForceTmpTableName                string `long:"force-table-names" default:"" description:"table name prefix to be used on the temporary tables"`
}

// ImportCommand type for import command
type ImportCommand struct {
	Config string `long:"config" short:"c" description:"database configuration file path" required:"config path"`
}

// ConsoleCommand type for console command
type ConsoleCommand struct {
	Config string `long:"config" short:"c" description:"database configuration file path" required:"config path"`
}

// InstallCommand type for install command
type InstallCommand struct {
	MySQLAdapter  bool `long:"mysql"  description:"install mysql adapter"`
	SQLiteAdapter bool `long:"sqlite" description:"install sqlite3 adapter"`
}

// ShardCommand type for shard command
type ShardCommand struct {
	ShardID int64  `long:"id"     short:"i" description:"id of sharding key column" required:"id"`
	Config  string `long:"config" short:"c" description:"database configuration file path" required:"config path"`
}

var opts Option

// Execute executes version command
func (cmd *VersionCommand) Execute(args []string) error {
	fmt.Printf(
		"octillery version %s, built with go %s for %s/%s\n",
		octillery.Version,
		runtime.Version(),
		runtime.GOOS,
		runtime.GOARCH,
	)
	return nil
}

// Execute executes tranpose command
func (cmd *TransposeCommand) Execute(args []string) error {
	searchPath := "."
	if len(args) > 0 {
		searchPath = args[0]
	}
	pattern := regexp.MustCompile("^database/sql")
	packagePrefix := "go.knocknote.io/octillery"
	transposeClosure := func(packageName string) string {
		return fmt.Sprintf("%s/%s", packagePrefix, packageName)
	}

	if cmd.DryRun {
		return errors.WithStack(transposer.New().TransposeDryRun(pattern, searchPath, cmd.Ignore, transposeClosure))
	}
	return errors.WithStack(transposer.New().Transpose(pattern, searchPath, cmd.Ignore, transposeClosure))
}

// Execute executes migrate command
func (cmd *MigrateCommand) Execute(args []string) error {
	if len(args) == 0 {
		return errors.New("argument is required. it is path to directory includes schema file or direct path to schema file")
	}
	if err := octillery.LoadConfig(cmd.Config); err != nil {
		return errors.WithStack(err)
	}

	schemaPath := args[0]
	migrator, err := migrator.NewMigrator("mysql", cmd.DryRun, cmd.Quiet)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(migrator.Migrate(schemaPath))
}

func acceptSignals(migrationContext *base.MigrationContext) {
	c := make(chan os.Signal, 1)

	signal.Notify(c, syscall.SIGHUP)
	go func() {
		for sig := range c {
			switch sig {
			case syscall.SIGHUP:
				log.Println("Received SIGHUP. Reloading configuration")
				if err := migrationContext.ReadConfigFile(); err != nil {
					log.Println(err)
				} else {
					migrationContext.MarkPointOfInterest()
				}
			}
		}
	}()
}

func toBool(s string) bool {
	b, _ := strconv.ParseBool(s)
	return b
}

// Execute executes online migrate command
func (cmd *OnlineMigrateCommand) Execute(args []string) error {
	if toBool(cmd.CheckFlag) {
		return nil
	}

	if cmd.DatabaseName == "" {
		return errors.New("--database must be provided and database name must not be empty")
	}
	if cmd.OriginalTableName == "" {
		return errors.New("--table must be provided and table name must not be empty")
	}
	if cmd.AlterStatement == "" {
		return errors.New("--alter must be provided and statement must not be empty")
	}

	if toBool(cmd.AllowedRunningOnMaster) && toBool(cmd.TestOnReplica) {
		return errors.New("--allow-on-master and --test-on-replica are mutually exclusive")
	}
	if toBool(cmd.AllowedRunningOnMaster) && toBool(cmd.MigrateOnReplica) {
		return errors.New("--allow-on-master and --migrate-on-replica are mutually exclusive")
	}
	if toBool(cmd.MigrateOnReplica) && toBool(cmd.TestOnReplica) {
		return errors.New("--migrate-on-replica and --test-on-replica are mutually exclusive")
	}
	if toBool(cmd.SwitchToRowBinlogFormat) && toBool(cmd.AssumeRBR) {
		return errors.New("--switch-to-rbr and --assume-rbr are mutually exclusive")
	}
	if toBool(cmd.TestOnReplicaSkipReplicaStop) {
		if !toBool(cmd.TestOnReplica) {
			return errors.New("--test-on-replica-skip-replica-stop requires --test-on-replica to be enabled")
		}
		log.Println("--test-on-replica-skip-replica-stop enabled. We will not stop replication before cut-over. Ensure you have a plugin that does this.")
	}
	if cmd.MasterUser != "" && cmd.AssumeMasterHostname == "" {
		return errors.New("--master-user requires --assume-master-host")
	}
	if cmd.MasterPassword != "" && cmd.AssumeMasterHostname == "" {
		return errors.New("--master-password requires --assume-master-host")
	}
	if cmd.ReplicationLagQuery != "" {
		log.Println("--replication-lag-query is deprecated")
	}

	ctx := base.NewMigrationContext()
	ctx.Noop = !toBool(cmd.ExecuteFlag)
	ctx.InspectorConnectionConfig.Key.Hostname = cmd.Host
	ctx.AssumeMasterHostname = cmd.AssumeMasterHostname
	ctx.InspectorConnectionConfig.Key.Port = cmd.Port
	ctx.CliUser = cmd.User
	ctx.CliPassword = cmd.Password
	ctx.CliMasterUser = cmd.MasterUser
	ctx.CliMasterPassword = cmd.MasterPassword
	ctx.ConfigFile = cmd.ConfigFile
	ctx.DatabaseName = cmd.DatabaseName
	ctx.OriginalTableName = cmd.OriginalTableName
	ctx.AlterStatement = cmd.AlterStatement
	ctx.CountTableRows = toBool(cmd.CountTableRows)
	ctx.ConcurrentCountTableRows = toBool(cmd.ConcurrentCountTableRows)
	ctx.AllowedRunningOnMaster = toBool(cmd.AllowedRunningOnMaster)
	ctx.AllowedMasterMaster = toBool(cmd.AllowedMasterMaster)
	ctx.NullableUniqueKeyAllowed = toBool(cmd.NullableUniqueKeyAllowed)
	ctx.ApproveRenamedColumns = toBool(cmd.ApproveRenamedColumns)
	ctx.SkipRenamedColumns = toBool(cmd.SkipRenamedColumns)
	ctx.IsTungsten = toBool(cmd.IsTungsten)
	ctx.DiscardForeignKeys = toBool(cmd.DiscardForeignKeys)
	ctx.SkipForeignKeyChecks = toBool(cmd.SkipForeignKeyChecks)
	ctx.AliyunRDS = toBool(cmd.AliyunRDS)
	ctx.GoogleCloudPlatform = toBool(cmd.GoogleCloudPlatform)
	ctx.TestOnReplica = toBool(cmd.TestOnReplica)
	ctx.TestOnReplicaSkipReplicaStop = toBool(cmd.TestOnReplicaSkipReplicaStop)
	ctx.MigrateOnReplica = toBool(cmd.MigrateOnReplica)
	ctx.OkToDropTable = toBool(cmd.OkToDropTable)
	ctx.InitiallyDropOldTable = toBool(cmd.InitiallyDropOldTable)
	ctx.InitiallyDropGhostTable = toBool(cmd.InitiallyDropGhostTable)
	ctx.TimestampOldTable = toBool(cmd.TimestampOldTable)
	ctx.ForceNamedCutOverCommand = toBool(cmd.ForceNamedCutOverCommand)
	ctx.SwitchToRowBinlogFormat = toBool(cmd.SwitchToRowBinlogFormat)
	ctx.AssumeRBR = toBool(cmd.AssumeRBR)
	ctx.CutOverExponentialBackoff = toBool(cmd.CutOverExponentialBackoff)
	ctx.ThrottleFlagFile = cmd.ThrottleFlagFile
	ctx.ThrottleAdditionalFlagFile = cmd.ThrottleAdditionalFlagFile
	ctx.PostponeCutOverFlagFile = cmd.PostponeCutOverFlagFile
	ctx.PanicFlagFile = cmd.PanicFlagFile
	ctx.DropServeSocket = toBool(cmd.DropServeSocket)
	ctx.ServeSocketFile = cmd.ServeSocketFile
	ctx.ServeTCPPort = cmd.ServeTCPPort
	ctx.HooksPath = cmd.HooksPath
	ctx.HooksHintMessage = cmd.HooksHintMessage
	ctx.ReplicaServerId = cmd.ReplicaServerId
	ctx.CriticalLoadIntervalMilliseconds = cmd.CriticalLoadIntervalMilliseconds
	ctx.CriticalLoadHibernateSeconds = cmd.CriticalLoadHibernateSeconds
	ctx.ForceTmpTableName = cmd.ForceTmpTableName

	switch cmd.CutOver {
	case "atomic", "default", "":
		ctx.CutOverType = base.CutOverAtomic
	case "two-step":
		ctx.CutOverType = base.CutOverTwoStep
	default:
		return errors.Errorf("Unknown cut-over: %s", cmd.CutOver)
	}
	if err := ctx.ReadConfigFile(); err != nil {
		return errors.WithStack(err)
	}
	if err := ctx.ReadThrottleControlReplicaKeys(cmd.ThrottleControlReplicas); err != nil {
		return errors.WithStack(err)
	}
	if err := ctx.ReadMaxLoad(cmd.MaxLoad); err != nil {
		return errors.WithStack(err)
	}
	if err := ctx.ReadCriticalLoad(cmd.CriticalLoad); err != nil {
		return errors.WithStack(err)
	}
	if ctx.ServeSocketFile == "" {
		ctx.ServeSocketFile = fmt.Sprintf("/tmp/gh-ost.%s.%s.sock", ctx.DatabaseName, ctx.OriginalTableName)
	}
	/*
		if cmd.AskPass {
			fmt.Println("Password:")
			bytePassword, err := terminal.ReadPassword(int(syscall.Stdin))
			if err != nil {
				return errors.WithStack(err)
			}
			ctx.CliPassword = string(bytePassword)
		}
	*/
	ctx.SetHeartbeatIntervalMilliseconds(cmd.HeartbeatIntervalMillis)
	ctx.SetNiceRatio(cmd.NiceRatio)
	ctx.SetChunkSize(cmd.ChunkSize)
	ctx.SetDMLBatchSize(cmd.DmlBatchSize)
	ctx.SetMaxLagMillisecondsThrottleThreshold(cmd.MaxLagMillis)
	ctx.SetThrottleQuery(cmd.ThrottleQuery)
	ctx.SetThrottleHTTP(cmd.ThrottleHTTP)
	ctx.SetDefaultNumRetries(cmd.DefaultRetries)
	ctx.ApplyCredentials()
	if err := ctx.SetCutOverLockTimeoutSeconds(cmd.CutOverLockTimeoutSeconds); err != nil {
		return errors.WithStack(err)
	}
	if err := ctx.SetExponentialBackoffMaxInterval(cmd.ExponentialBackoffMaxInterval); err != nil {
		return errors.WithStack(err)
	}

	log.Println("starting gh-ost")
	acceptSignals(ctx)

	migrator := logic.NewMigrator(ctx)
	if err := migrator.Migrate(); err != nil {
		migrator.ExecOnFailureHook()
		return errors.WithStack(err)
	}
	fmt.Fprintf(os.Stdout, "# Done\n")
	return nil
}

func (cmd *ImportCommand) schemaFromTableName(tableName string) (vtparser.Statement, error) {
	mgr, err := connection.NewConnectionManager()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer mgr.Close()
	conn, err := mgr.ConnectionByTableName(tableName)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var db *coresql.DB
	if conn.IsShard {
		for _, shard := range conn.ShardConnections.AllShard() {
			db = shard.Connection
			break
		}
	} else {
		db = conn.Connection
	}
	if db == nil {
		return nil, errors.New("cannot get database connection")
	}
	var table string
	var tableSchema string
	if err = db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", tableName)).Scan(&table, &tableSchema); err != nil {
		return nil, errors.Wrapf(err, `failed to execute 'SHOW CREATE TABLE "%s"'`, tableName)
	}
	parser, err := sqlparser.New()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	query, err := parser.Parse(tableSchema)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return query.(*sqlparser.QueryBase).Stmt, nil
}

var (
	unsignedPattern  = regexp.MustCompile(`(?i)unsigned`)
	charPattern      = regexp.MustCompile(`(?i)char`)
	blobPattern      = regexp.MustCompile(`(?i)blob`)
	datePattern      = regexp.MustCompile(`(?i)date`)
	dateTimePattern  = regexp.MustCompile(`(?i)datetime`)
	timePattern      = regexp.MustCompile(`(?i)time`)
	timeStampPattern = regexp.MustCompile(`(?i)timestamp`)
	yearPattern      = regexp.MustCompile(`(?i)year`)
	intPattern       = regexp.MustCompile(`(?i)int`)
	floatPattern     = regexp.MustCompile(`(?i)float`)
	doublePattern    = regexp.MustCompile(`(?i)double`)
	decimalPattern   = regexp.MustCompile(`(?i)decimal`)
	enumPattern      = regexp.MustCompile(`(?i)enum`)
	setPattern       = regexp.MustCompile(`(?i)set`)
	textPattern      = regexp.MustCompile(`(?i)text`)
)

// GoType type of Go for mapping from MySQL type
type GoType int

const (
	// UnknownType the undefined type
	UnknownType GoType = iota
	// GoString type of string
	GoString
	// GoBytes type of bytes
	GoBytes
	// GoUint type of uint
	GoUint
	// GoInt type of int
	GoInt
	// GoFloat type of float
	GoFloat
	// GoDateFormat type of time.Time
	GoDateFormat
	// GoTimeFormat type of time.Time
	GoTimeFormat
	// GoDateTimeFormat type of time.Time
	GoDateTimeFormat
	// GoTimeStampFormat type of time.Time
	GoTimeStampFormat
	// GoYearFormat type of time.Time
	GoYearFormat
)

// nolint: gocyclo
func (cmd *ImportCommand) convertMySQLTypeToGOType(typ string) GoType {
	if charPattern.MatchString(typ) ||
		enumPattern.MatchString(typ) ||
		setPattern.MatchString(typ) ||
		textPattern.MatchString(typ) {
		return GoString
	}
	if blobPattern.MatchString(typ) {
		return GoBytes
	}
	if floatPattern.MatchString(typ) || doublePattern.MatchString(typ) {
		return GoFloat
	}
	if unsignedPattern.MatchString(typ) {
		return GoUint
	}
	if intPattern.MatchString(typ) || decimalPattern.MatchString(typ) {
		return GoInt
	}
	if dateTimePattern.MatchString(typ) {
		return GoDateTimeFormat
	}
	if datePattern.MatchString(typ) {
		return GoDateFormat
	}
	if timeStampPattern.MatchString(typ) {
		return GoTimeStampFormat
	}
	if timePattern.MatchString(typ) {
		return GoTimeFormat
	}
	if yearPattern.MatchString(typ) {
		return GoYearFormat
	}
	return UnknownType
}

func (cmd *ImportCommand) columnTypes(schema vtparser.Statement) (map[string]GoType, error) {
	columnToTypeMap := map[string]GoType{}
	for _, column := range schema.(*vtparser.CreateTable).Columns {
		typ := cmd.convertMySQLTypeToGOType(column.Type)
		if typ == UnknownType {
			return columnToTypeMap, errors.Errorf("cannot map %s to Go type", column.Type)
		}
		columnToTypeMap[column.Name] = typ
	}
	return columnToTypeMap, nil
}

func (cmd *ImportCommand) timeValueWithFormat(format string, v string) (*time.Time, error) {
	if v == "null" {
		return nil, nil
	}
	value, err := time.Parse(format, v)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &value, nil
}

// nolint: gocyclo
func (cmd *ImportCommand) values(record []string, types []GoType, columns []string, tableName string) ([]interface{}, error) {
	values := []interface{}{}
	for idx, v := range record {
		typ := types[idx]
		switch typ {
		case GoInt:
			value, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot convert %v to int64. table:[%s] column:[%s]", v, tableName, columns[idx])
			}
			values = append(values, value)
		case GoUint:
			value, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot convert %v to uint64. table:[%s] column:[%s]", v, tableName, columns[idx])
			}
			values = append(values, value)
		case GoFloat:
			value, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot convert %v to float64. table:[%s] column:[%s]", v, tableName, columns[idx])
			}
			values = append(values, value)
		case GoString:
			if unquotedString, err := strconv.Unquote(fmt.Sprintf("\"%s\"", v)); err == nil {
				values = append(values, unquotedString)
			} else {
				values = append(values, v)
			}
		case GoBytes:
			values = append(values, []byte(v))
		case GoDateFormat:
			format := "2006-01-02"
			value, err := cmd.timeValueWithFormat(format, v)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot convert %v to time.Time. table:[%s] column:[%s]", v, tableName, columns[idx])
			}
			values = append(values, value)
		case GoTimeFormat:
			format := "15:04:05"
			value, err := cmd.timeValueWithFormat(format, v)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot convert %v to time.Time. table:[%s] column:[%s]", v, tableName, columns[idx])
			}
			values = append(values, value)
		case GoDateTimeFormat, GoTimeStampFormat:
			format := "2006-01-02 15:04:05"
			value, err := cmd.timeValueWithFormat(format, v)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot convert %v to time.Time. table:[%s] column:[%s]", v, tableName, columns[idx])
			}
			values = append(values, value)
		case GoYearFormat:
			format := "2006"
			value, err := cmd.timeValueWithFormat(format, v)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot convert %v to time.Time. table:[%s] column:[%s]", v, tableName, columns[idx])
			}
			values = append(values, value)
		default:
		}
	}
	return values, nil
}

// Execute executes import command
// nolint: gocyclo
func (cmd *ImportCommand) Execute(args []string) error {
	if len(args) == 0 {
		return errors.New("argument is required. it is path to directory includes schema file or direct path to schema file")
	}
	if err := octillery.LoadConfig(cmd.Config); err != nil {
		return errors.WithStack(err)
	}
	cfg, err := config.Get()
	if err != nil {
		return errors.WithStack(err)
	}

	seedsPath := args[0]

	importTables := map[string][][]string{}

	if err := filepath.Walk(seedsPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return errors.WithStack(err)
		}
		if info.IsDir() {
			return nil
		}
		ext := filepath.Ext(path)
		if ext != ".csv" {
			return nil
		}
		baseName := filepath.Base(path)
		tableName := baseName[:len(baseName)-len(ext)]
		if _, exists := cfg.Tables[tableName]; !exists {
			return errors.Errorf("invalid table name %s", tableName)
		}
		seeds, err := os.Open(path)
		if err != nil {
			return errors.Wrapf(err, "failed to open file %s", path)
		}
		defer seeds.Close()
		reader := csv.NewReader(seeds)
		reader.LazyQuotes = true
		records, err := reader.ReadAll()
		if err != nil {
			return errors.Wrapf(err, "failed to read file %s", path)
		}
		importTables[tableName] = records
		return nil
	}); err != nil {
		return errors.WithStack(err)
	}

	conn, err := sql.Open("", "?parseTime=true")
	if err != nil {
		return errors.WithStack(err)
	}
	defer conn.Close()

	for tableName, records := range importTables {
		if len(records) < 2 {
			continue
		}
		schema, err := cmd.schemaFromTableName(tableName)
		if err != nil {
			return errors.Wrapf(err, "cannot get schema. table is %s", tableName)
		}
		columnNameToTypeMap, err := cmd.columnTypes(schema)
		if err != nil {
			return errors.Wrapf(err, "cannot get column types. table is %s", tableName)
		}
		columns := records[0]
		types := []GoType{}
		for _, column := range columns {
			typ, exists := columnNameToTypeMap[column]
			if !exists {
				return errors.Errorf("cannot get Go type from column name %s. table is %s", column, tableName)
			}
			types = append(types, typ)
		}

		placeholders := []string{}
		for i := 0; i < len(columns); i++ {
			placeholders = append(placeholders, "?")
		}
		escapedColumns := []string{}
		for _, column := range columns {
			escapedColumns = append(escapedColumns, fmt.Sprintf("`%s`", column))
		}
		if !cfg.Tables[tableName].IsShard {
			// try to bulk insert if not sharding table
			placeholderTmpl := fmt.Sprintf("(%s)", strings.Join(placeholders, ","))
			recordsWithoutHeader := records[1:]
			maxPlaceholderNum := 1000
			if len(recordsWithoutHeader) < maxPlaceholderNum {
				maxPlaceholderNum = len(recordsWithoutHeader)
			}
			allBulkRequestNum := len(recordsWithoutHeader) / maxPlaceholderNum
			remainRecordNum := len(recordsWithoutHeader) - maxPlaceholderNum*allBulkRequestNum
			if _, err := conn.Exec(fmt.Sprintf("TRUNCATE TABLE `%s`", tableName)); err != nil {
				return errors.Wrapf(err, "cannot truncate table %s", tableName)
			}
			for i := 0; i < allBulkRequestNum; i++ {
				start := i * maxPlaceholderNum
				end := start + maxPlaceholderNum
				if (i + 1) == allBulkRequestNum {
					end += remainRecordNum
				}
				filteredRecords := recordsWithoutHeader[start:end]
				allPlaceholders := []string{}
				values := []interface{}{}
				for _, record := range filteredRecords {
					vals, err := cmd.values(record, types, columns, tableName)
					if err != nil {
						return errors.WithStack(err)
					}
					allPlaceholders = append(allPlaceholders, placeholderTmpl)
					values = append(values, vals...)
				}
				prepareText := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tableName, strings.Join(escapedColumns, ","), strings.Join(allPlaceholders, ","))
				if _, err := conn.Exec(prepareText, values...); err != nil {
					return errors.Wrapf(err, "cannot insert [%s]:%v", prepareText, values)
				}
			}
		} else {
			prepareText := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(escapedColumns, ","), strings.Join(placeholders, ","))
			stmt, err := conn.Prepare(prepareText)
			if err != nil {
				return errors.Wrapf(err, "cannot prepare [%s]", prepareText)
			}
			if _, err := conn.Exec(fmt.Sprintf("TRUNCATE TABLE `%s`", tableName)); err != nil {
				return errors.Wrapf(err, "cannot truncate table %s", tableName)
			}
			for _, record := range records[1:] {
				values, err := cmd.values(record, types, columns, tableName)
				if err != nil {
					return errors.WithStack(err)
				}
				if _, err := stmt.Exec(values...); err != nil {
					return errors.Wrapf(err, "cannot insert [%s]:%v", prepareText, values)
				}
			}
		}
	}
	return nil
}

// Execute executes console command
func (cmd *ConsoleCommand) Execute(args []string) error {
	if err := octillery.LoadConfig(cmd.Config); err != nil {
		return errors.WithStack(err)
	}
	db, err := sql.Open("", "")
	if err != nil {
		return errors.WithStack(err)
	}
	fmt.Print("octillery> ")
	s := bufio.NewScanner(os.Stdin)
	for s.Scan() {
		query := s.Text()
		if query == "quit" || query == "exit" {
			return nil
		}
		multiRows, result, err := octillery.Exec(db, query)
		if err != nil {
			fmt.Printf("%+v\n", err)
		} else if multiRows != nil {
			printer, err := printer.NewPrinter(multiRows)
			if err != nil {
				fmt.Printf("%+v\n", err)
				return nil
			}
			printer.Print()
		} else if result != nil {

		}
		fmt.Print("octillery> ")
	}
	return nil
}

func (cmd *InstallCommand) lookupOctillery() ([]string, error) {
	libraryPath := filepath.Join("go.knocknote.io", "octillery")
	installPaths := []string{}
	cwd, err := os.Getwd()
	if err != nil {
		return installPaths, errors.WithStack(err)
	}
	// First, lookup vendor/go.knocknote.io/octillery
	vendorPath := filepath.Join(cwd, "vendor", libraryPath)
	if _, err := os.Stat(vendorPath); !os.IsNotExist(err) {
		installPaths = append(installPaths, vendorPath)
	}
	goPath := os.Getenv("GOPATH")
	if goPath == "" {
		goPath = filepath.Join(os.Getenv("HOME"), "go")
	}
	// Second, lookup $GOPATH/src/go.knocknote.io/octillery
	underGoPath := filepath.Join(goPath, "src", libraryPath)
	if _, err := os.Stat(underGoPath); !os.IsNotExist(err) {
		installPaths = append(installPaths, underGoPath)
	}
	if os.Getenv("GO111MODULE") == "on" {
		// lookup $GOPATH/pkg/mod/go.knocknote.io/octillery@*
		modPathPrefix := filepath.Join(goPath, "pkg", "mod", libraryPath)
		modPaths, err := filepath.Glob(modPathPrefix + "@*")
		if err == nil {
			installPaths = append(installPaths, modPaths...)
		}
	}
	if len(installPaths) == 0 {
		return installPaths, errors.New("cannot find 'go.knocknote.io/octillery' library")
	}
	return installPaths, nil
}

func (cmd *InstallCommand) installToPath(sourcePath string) error {
	adapterBasePath := filepath.Join(sourcePath, "connection", "adapter", "plugin")
	var adapterPath string
	if cmd.MySQLAdapter {
		adapterPath = filepath.Join(adapterBasePath, "mysql.go")
	} else if cmd.SQLiteAdapter {
		adapterPath = filepath.Join(adapterBasePath, "sqlite3.go")
	} else {
		return errors.New("unknown adapter name. currently supports '--mysql' or '--sqlite' only")
	}
	adapterData, err := ioutil.ReadFile(adapterPath)
	if err != nil {
		return errors.WithStack(err)
	}
	pluginDir := filepath.Join(sourcePath, "plugin")
	if err := os.Chmod(pluginDir, 0755); err != nil {
		return errors.WithStack(err)
	}
	baseName := filepath.Base(adapterPath)
	pluginPath := filepath.Join(pluginDir, baseName)
	log.Printf("install to %s\n", pluginPath)
	return errors.WithStack(ioutil.WriteFile(pluginPath, adapterData, 0644))
}

// Execute executes install command
func (cmd *InstallCommand) Execute(args []string) error {
	if len(args) > 0 {
		path, err := filepath.Abs(args[0])
		if err != nil {
			return errors.WithStack(err)
		}
		if err := cmd.installToPath(path); err != nil {
			return errors.WithStack(err)
		}
		return nil
	}
	paths, err := cmd.lookupOctillery()
	if err != nil {
		return errors.WithStack(err)
	}
	for _, path := range paths {
		if err := cmd.installToPath(path); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// Execute executes shard command
func (cmd *ShardCommand) Execute(args []string) error {
	if len(args) == 0 {
		return errors.New("required table name included configuration file")
	}
	cfg, err := config.Load(cmd.Config)
	if err != nil {
		return errors.WithStack(err)
	}
	tableName := args[0]
	tableConfig, exists := cfg.Tables[tableName]
	if !exists {
		return errors.Errorf("cannot find table name %s in configuration file", tableName)
	}
	if !tableConfig.IsShard {
		return errors.Errorf("%s table is not sharded", tableName)
	}
	logic, err := algorithm.LoadShardingAlgorithm(tableConfig.Algorithm)
	if err != nil {
		return errors.WithStack(err)
	}
	conns := []*coresql.DB{}
	connMap := map[*coresql.DB]*config.DatabaseConfig{}
	for _, shardMap := range tableConfig.Shards {
		// append dummy connection
		conn := &coresql.DB{}
		for _, shard := range shardMap {
			connMap[conn] = shard
		}
		conns = append(conns, conn)
	}
	if !logic.Init(conns) {
		return errors.New("cannot initialize sharding algorithm")
	}
	conn, err := logic.Shard(conns, cmd.ShardID)
	if err != nil {
		return errors.WithStack(err)
	}
	if shardConfig, exists := connMap[conn]; exists {
		dsn := ""
		if len(shardConfig.Masters) > 0 {
			dsn = shardConfig.Masters[0]
		}
		info := struct {
			Database string `json:"database"`
			DSN      string `json:"dsn"`
		}{
			Database: shardConfig.NameOrPath,
			DSN:      dsn,
		}
		bytes, err := json.Marshal(info)
		if err != nil {
			return errors.WithStack(err)
		}
		fmt.Println(string(bytes))
		return nil
	}
	return errors.New("cannot find target database")
}

func main() {
	parser := flags.NewParser(&opts, flags.Default)
	parser.Parse()
}
