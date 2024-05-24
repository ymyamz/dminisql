package master

import (
	"distribute-sql/util"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Master struct {
	regionCount int

	etcdClient    *clientv3.Client
	regionClients map[string]*rpc.Client

}
func (master *Master) Init(){
	master.regionCount=0
}

func (master *Master) Run(){
	fmt.Println("master init and listening ")
	//初始化etcd集群
	var err error 
	master.etcdClient, err= clientv3.New(clientv3.Config{
		Endpoints:   []string{util.ETCD_ENDPOINT},
		DialTimeout: 1 * time.Second,
	})
	if err != nil {  
		fmt.Printf("master error >>> etcd connect error: %v", err)  
	}  
	defer master.etcdClient.Close()


	// 注册rpc函数
	rpc.Register(master)
	rpc.HandleHTTP()
	// 启动server
	l, err := net.Listen("tcp",  util.MASTER_PORT)
	if err != nil {
		fmt.Println("Accept error:", err)
	}
	go http.Serve(l, nil) // 进入的链接让rpc来执行
	for {
		time.Sleep(10 * time.Second)
	}

}

