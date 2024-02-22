package canal

import (
	"strconv"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-redis/redis"
	"github.com/pkg/errors"
)

// BinLogStore binlog 存储
type BinLogStore interface {
	Reset() error
	Exists() bool
	GetPosition() (mysql.Position, error)
	SetPosition(position mysql.Position) error
	GetGtIDSet() (mysql.GTIDSet, error)
	SetGtIDSet(flavor Flavor, set mysql.GTIDSet) error
}

// BinLogMemoryStore 内存存储
type BinLogMemoryStore struct {
	sync.RWMutex
	set      string
	flavor   Flavor
	position mysql.Position
}

// NewBinLogMemoryStore 新建内存存储
func NewBinLogMemoryStore() BinLogStore {
	return &BinLogMemoryStore{}
}

// Reset interface
func (s *BinLogMemoryStore) Reset() error {
	s.Lock()
	s.set = ""
	s.flavor = ""
	s.position.Name = ""
	s.position.Pos = 0
	s.Unlock()
	return nil
}

// Exists interface
func (s *BinLogMemoryStore) Exists() bool {
	return !(s.set == "" && s.position.Name == "")
}

// GetPosition interface
func (s *BinLogMemoryStore) GetPosition() (mysql.Position, error) {
	s.RWMutex.RLock()
	defer s.RUnlock()
	return s.position, nil
}

// SetPosition interface
func (s *BinLogMemoryStore) SetPosition(position mysql.Position) error {
	s.Lock()
	s.position.Name = position.Name
	s.position.Pos = position.Pos
	s.Unlock()
	return nil
}

// GetGtIDSet interface
func (s *BinLogMemoryStore) GetGtIDSet() (mysql.GTIDSet, error) {
	s.RWMutex.RLock()
	defer s.RUnlock()
	return mysql.ParseGTIDSet(s.flavor.YaString(), s.set)
}

// SetGtIDSet interface
func (s *BinLogMemoryStore) SetGtIDSet(flavor Flavor, set mysql.GTIDSet) error {
	s.Lock()
	s.flavor = flavor
	s.set = set.String()
	s.Unlock()
	return nil
}

// BinLogRedisStore Redis存储
type BinLogRedisStore struct {
	name string
	conn redis.Cmdable
}

// NewBinLogRedisStore 新建Redis存储
func NewBinLogRedisStore(conn redis.Cmdable, name string) BinLogStore {
	return &BinLogRedisStore{
		name: name,
		conn: conn,
	}
}

// Reset interface
func (s *BinLogRedisStore) Reset() error {
	err := s.conn.Del(s.name).Err()
	return errors.WithStack(err)
}

// Exists interface
func (s *BinLogRedisStore) Exists() bool {
	v, _ := s.conn.Exists(s.name).Result()
	return v > 0
}

// GetPosition interface
func (s *BinLogRedisStore) GetPosition() (mysql.Position, error) {
	pos := mysql.Position{}

	data, err := s.conn.HGetAll(s.name).Result()
	if errors.Is(err, redis.Nil) {
		return pos, nil
	} else if err != nil {
		return pos, errors.WithStack(err)
	}

	if v, ok := data["name"]; ok {
		pos.Name = v
	}

	if v, ok := data["pos"]; ok {
		p, _ := strconv.ParseInt(v, 10, 64)
		pos.Pos = uint32(p)
	}

	return pos, nil
}

// SetPosition interface
func (s *BinLogRedisStore) SetPosition(position mysql.Position) error {
	err := s.conn.HMSet(s.name, map[string]interface{}{
		"name": position.Name,
		"pos":  position.Pos,
	}).Err()

	return errors.WithStack(err)
}

// GetGtIDSet interface
func (s *BinLogRedisStore) GetGtIDSet() (mysql.GTIDSet, error) {
	data, err := s.conn.HGetAll(s.name).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	} else if err != nil {
		return nil, errors.WithStack(err)
	}

	if v, ok := data["set"]; ok {
		flavor, _ := data["flavor"]
		set, err := mysql.ParseGTIDSet(flavor, v)
		return set, errors.WithStack(err)
	}

	return nil, nil
}

// SetGtIDSet interface
func (s *BinLogRedisStore) SetGtIDSet(flavor Flavor, set mysql.GTIDSet) error {
	err := s.conn.HMSet(s.name, map[string]interface{}{
		"flavor": flavor.YaString(),
		"set":    set.String(),
	}).Err()
	return errors.WithStack(err)
}
