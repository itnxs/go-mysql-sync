package gomysqlsync

import (
    "fmt"
    "strings"
    "sync"

    "github.com/go-mysql-org/go-mysql/canal"
    "github.com/go-mysql-org/go-mysql/mysql"
    "github.com/go-mysql-org/go-mysql/replication"
    "github.com/pkg/errors"
    "github.com/sirupsen/logrus"
)

// Message 消息内容
type Message struct {
    Action string                 `json:"action"`
    Table  string                 `json:"table"`
    Data   map[string]interface{} `json:"data"`
}

// OnMessage 消息处理
type OnMessage func(Message) error

// eventHandler 事件处理
type eventHandler struct {
    canal.DummyEventHandler
    logger    logrus.FieldLogger
    config    *Config
    store     BinLogStore
    dbName    string
    tables    map[string]interface{}
    mutex     sync.RWMutex
    onMessage OnMessage
}

// newEventHandler 新建事件处理
func newEventHandler(store BinLogStore, c *Config) (*eventHandler, error) {
    tables := make(map[string]interface{})
    for _, table := range c.Tables {
        table = strings.ToLower(fmt.Sprintf("%s.%s", c.driverConfig.DBName, table))
        tables[table] = table
    }

    return &eventHandler{
        logger: c.Logger,
        config: c,
        store:  store,
        dbName: c.driverConfig.DBName,
        tables: tables,
        onMessage: func(m Message) error {
            c.Logger.WithField("data", m).Info("onMessage")
            return nil
        },
    }, nil
}

// checkTable 验证表名称
func (h *eventHandler) checkTable(table string) bool {
    if len(h.dbName) <= 0 || !strings.HasPrefix(table, h.dbName) {
        return false
    }
    if len(h.tables) <= 0 {
        return true
    }
    _, ok := h.tables[table]
    return ok
}

// checkAction 验证数据更新操作类型
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
    } else if len(e.Rows) <= 0 {
        h.logger.WithField("table", e.Table).Debug("rows empty")
        return nil
    }

    row := e.Rows[len(e.Rows)-1]
    data := make(map[string]interface{})
    for i, c := range e.Table.Columns {
        if i >= len(row) {
            continue
        }
        if vv, ok := row[i].([]byte); ok {
            data[c.Name] = string(vv)
        } else {
            data[c.Name] = row[i]
        }
    }

    return h.onMessage(Message{Action: e.Action, Table: table, Data: data})
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
