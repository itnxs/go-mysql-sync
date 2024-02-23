package gomysqlsync

import (
    "github.com/go-mysql-org/go-mysql/canal"
    "github.com/go-mysql-org/go-mysql/mysql"
    "github.com/pkg/errors"
    "github.com/sirupsen/logrus"
)

// CanalServer 订阅服务
type CanalServer struct {
    canal   *canal.Canal
    config  *Config
    handler *eventHandler
    logger  logrus.FieldLogger
}

// NewCanalServer 新建订阅服务
func NewCanalServer(c *Config) (*CanalServer, error) {
    if err := c.init(); err != nil {
        return nil, err
    }

    cc, err := canal.NewCanal(c.newCanalConfig())
    if err != nil {
        return nil, errors.Wrap(err, "new canal")
    }

    s := NewBinLogMemoryStore()
    handler, err := newEventHandler(s, c)
    if err != nil {
        return nil, errors.Wrap(err, "new event handler")
    }

    server := &CanalServer{
        config:  c,
        canal:   cc,
        handler: handler,
        logger:  c.Logger,
    }

    return server, err
}

// SetStore 设置存储
func (s *CanalServer) SetStore(store BinLogStore) *CanalServer {
    s.handler.store = store
    return s
}

// OnMessage 接受消息
func (s *CanalServer) OnMessage(callback OnMessage) *CanalServer {
    s.handler.onMessage = callback
    return s
}

// Close 关闭服务
func (s *CanalServer) Close() {
    s.logger.Info("close canal server")
    s.canal.Close()
}

// Start 开始运行
func (s *CanalServer) Start() error {
    s.logger.Info("start canal server")

    if err := s.binlogInit(); err != nil {
        return err
    }

    s.canal.SetEventHandler(s.handler)
    if s.config.Mode == MODE_GTID {
        set, err := s.handler.store.GetGtIDSet()
        if err != nil {
            return err
        }

        err = s.canal.StartFromGTID(set)
        return errors.WithStack(err)
    } else if s.config.Mode == MODE_POSITION {
        position, err := s.handler.store.GetPosition()
        if err != nil {
            return err
        }

        err = s.canal.RunFrom(mysql.Position{
            Name: position.Name,
            Pos:  position.Pos,
        })

        return errors.WithStack(err)
    }

    return errors.WithStack(errors.New("mysql mode error"))
}

// binlogInit binlog位置初始化
func (s *CanalServer) binlogInit() error {
    if s.handler.store.Exists() {
        s.logger.Info("binlog store exists")
        return nil
    }

    if s.config.Dump {
        s.logger.WithField("tables", s.config.Tables).Info("execute command dump")
        defer s.logger.WithField("tables", s.config.Tables).Info("execute command dump done")
        return s.canal.Dump()
    }

    pos, err := s.canal.GetMasterPos()
    if err != nil {
        return errors.WithStack(err)
    }

    set, err := s.canal.GetMasterGTIDSet()
    if err != nil {
        return errors.WithStack(err)
    }

    if err = s.handler.store.SetPosition(pos); err != nil {
        return errors.Wrap(err, "binlogStore.SetPosition")
    }

    if err = s.handler.store.SetGtIDSet(s.config.Flavor, set); err != nil {
        return errors.Wrap(err, "binlogStore.SetGtIDSet")
    }

    s.logger.WithFields(logrus.Fields{
        "position": pos,
        "flavor":   s.config.Flavor,
    }).Info("init binlog store done")
    return nil
}
