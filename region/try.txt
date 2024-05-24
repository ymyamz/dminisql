package main

import (
	"context"
	"fmt"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)
func main() {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:20079"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalln(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// PUT = add + edit
	key := "name" // key 的位置在 /ns/ 目录下的 名为 service 文件 中
	value := "jack"
	_, err = client.Put(ctx, key, value)
	if err != nil {
		log.Printf("etcd put error,%v\n", err)
		return
	}

	// GET
	getResponse, err := client.Get(ctx, key)
	if err != nil {
		log.Printf("etcd GET error,%v\n", err)
		return
	}

	for _, kv := range getResponse.Kvs {
		fmt.Printf("Key:%s ===> Val:%s \n", kv.Key, kv.Value)
	}

	
}