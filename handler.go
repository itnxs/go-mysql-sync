package canal

import (
	"fmt"
	"strings"
	"sync"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Message 消息内容
type Message struct {
	Action string                   `json:"action"`
	Table  string                   `json:"table"`
	Data   []map[string]interface{} `json:"data"`
}

// OnMessage 消息处理
type OnMessage func(Message) error

// eventHandler 事件处理
type eventHandler struct {
	canal.DummyEventHandler
	logger    logrus.FieldLogger
	db        *sqlx.DB
	config    *Config
	store     BinLogStore
	tables    map[string]interface{}
	mutex     sync.RWMutex
	onMessage OnMessage
}

// newEventHandler 新建事件处理
func newEventHandler(store BinLogStore, c *Config) (*eventHandler, error) {
	db, err := sqlx.Open("mysql", c.MysqlDNS)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	tables := make(map[string]interface{})
	for _, table := range c.Tables {
		table = strings.ToLower(fmt.Sprintf("%s.%s", c.driverConfig.DBName, table))
		tables[table] = table
	}

	return &eventHandler{
		logger: c.Logger,
		db:     db,
		config: c,
		store:  store,
		tables: tables,
		onMessage: func(m Message) error {
			c.Logger.WithField("data", m).Info("onMessage")
			return nil
		},
	}, nil
}

// checkTable 验证表名称
func (h *eventHandler) checkTable(table string) bool {
	if len(h.tables) <= 0 {
		return true
	}
	_, ok := h.tables[table]
	return ok
}

// isAction 验证数据更新操作类型
func (h *eventHandler) checkAction(action string) bool {
	switch action {
	case canal.InsertAction, canal.UpdateAction, canal.DeleteAction:
		return true
	}
	return false
}

// OnRow 数据更新处理
func (h *eventHandler) OnRow(e *canal.RowsEvent) error {
	table := strings.ToLower(e.Table.String())
	if !h.checkAction(e.Action) {
		h.logger.WithField("action", e.Action).Debug("action skip")
		return nil
	} else if !h.checkTable(table) {
		h.logger.WithField("table", e.Table).Debug("table skip")
		return nil
	}

	rows := make([]map[string]interface{}, 0)
	for _, v := range e.Rows {
		row := make(map[string]interface{})
		for i, c := range e.Table.Columns {
			if vv, ok := v[i].([]byte); ok {
				row[c.Name] = string(vv)
			} else {
				row[c.Name] = v[i]
			}
		}
		rows = append(rows, row)
	}

	return h.onMessage(Message{
		Action: e.Action,
		Table:  table,
		Data:   rows,
	})
}

// OnPosSynced 位置更新处理
func (h *eventHandler) OnPosSynced(e *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, ok bool) error {
	h.logger.WithField("position", pos).WithField("set", set).Debug("OnPosSynced")
	if err := h.store.SetPosition(pos); err != nil {
		return errors.WithStack(err)
	}
	if set != nil {
		if err := h.store.SetGtIDSet(h.config.Flavor, set); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}
