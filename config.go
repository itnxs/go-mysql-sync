package canal

import (
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	driver "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Config config
type Config struct {
	MysqlDNS     string             `json:"mysqlDns" toml:"mysqlDns"`
	ServerID     int                `json:"serverId" toml:"serverId"`
	Flavor       Flavor             `json:"flavor" toml:"flavor"`
	Mode         Mode               `json:"mode" toml:"mode"`
	NewLatest    bool               `json:"newLatest" toml:"newLatest"`
	Tables       []string           `json:"tables"  toml:"tables"`
	Logger       logrus.FieldLogger `json:"-" json:"-"`
	driverConfig *driver.Config
}

// Mode mysql replication mode
type Mode string

const (
	// MODE_GTID GTID Mode
	MODE_GTID Mode = "gtid"
	// MODE_POSITION common Mode
	MODE_POSITION Mode = "position"
)

// Must must mode
func (m Mode) Must() Mode {
	if m == MODE_GTID {
		return MODE_GTID
	}
	return MODE_POSITION
}

// Flavor mysql or mariaDB
type Flavor string

const (
	// FLAVOR_MYSQL MySQL DB
	FLAVOR_MYSQL Flavor = "mysql"
	// FLAVOR_MARIADB MariaDB
	FLAVOR_MARIADB Flavor = "mariadb"
)

// YaString convert binlogo flavor string
func (f Flavor) YaString() string {
	if f == FLAVOR_MARIADB {
		return mysql.MariaDBFlavor
	}
	return mysql.MySQLFlavor
}

// init 初始化配置
func (c *Config) init() (err error) {
	if c.Logger == nil {
		c.Logger = logrus.New()
	}

	c.Flavor = Flavor(c.Flavor.YaString())
	c.Mode = c.Mode.Must()

	c.driverConfig, err = driver.ParseDSN(c.MysqlDNS)
	return errors.WithStack(err)
}

// newCanalConfig 新建Canal配置文件
func (c *Config) newCanalConfig() *canal.Config {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = c.driverConfig.Addr
	cfg.User = c.driverConfig.User
	cfg.Password = c.driverConfig.Passwd
	cfg.Logger = c.Logger
	cfg.Flavor = c.Flavor.YaString()
	cfg.IncludeTableRegex = c.Tables
	cfg.ServerID = uint32(c.ServerID)
	cfg.Dump.Databases = []string{c.driverConfig.DBName}
	cfg.Dump.TableDB = c.driverConfig.DBName
	cfg.Dump.Tables = c.Tables
	cfg.Dump.SkipMasterData = true
	return cfg
}
