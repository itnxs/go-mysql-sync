package main

import (
    "encoding/json"
    "fmt"

    gomysqlsync "github.com/itnxs/go-mysql-sync"
    "github.com/sirupsen/logrus"
)

func main() {
    c := &gomysqlsync.Config{
        ServerID:  2000,
        MysqlDNS:  "root:123456@tcp(127.0.0.1:3306)/test?charset=utf8mb4",
        Logger:    logrus.New(),
        NewLatest: true,       // 是否使用最新位置
        Tables:    []string{}, // 过滤表
    }

    s, err := gomysqlsync.NewServer(c)
    if err != nil {
        panic(err)
    }

    // 订阅位置存储（默认就是内存存储）
    store := gomysqlsync.NewBinLogMemoryStore()
    // gomysqlsync.NewBinLogRedisStore()
    s.SetStore(store)

    // 接受数据变化消息
    s.OnMessage(func(m gomysqlsync.Message) error {
        data, _ := json.Marshal(m)
        fmt.Println(string(data))
        return nil
    })

    err = s.Start()
    if err != nil {
        panic(err)
    }
}
