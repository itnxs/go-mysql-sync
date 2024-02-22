package canal

import (
	"testing"

	"github.com/sirupsen/logrus"
)

func TestServer_Start(t *testing.T) {
	c := &Config{
		ServerID: 2000,
		MysqlDNS: "root:123456@tcp(127.0.0.1:3306)/mysql?charset=utf8mb4&autocommit=true",
		Logger:   logrus.New(),
		Tables:   []string{"beats"},
	}

	s, err := NewServer(c)
	if err != nil {
		panic(err)
	}

	err = s.Start()
	if err != nil {
		panic(err)
	}
}
