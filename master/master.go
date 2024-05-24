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
	//ip->client
	regionClients map[string]*rpc.Client
	//ip含有的table列表
	owntablelist map[string]*[]string 
	//该table所在region的ip
	tableIP      map[string]string

}
func (master *Master) Init(){
	master.regionCount=len(util.Region_IPs)

	//code阶段，先对region进行初始化，后续再进行优化
	//遍历每一个region_ips，建立rpc连接
	master.regionClients=make(map[string]*rpc.Client)
	for _,region_ip := range util.Region_IPs{
		client, err := rpc.DialHTTP("tcp", region_ip+util.REGION_PORT)
		if err!= nil {
			fmt.Println("master error >>> region rpc dial error:", err)
			return
		}
		fmt.Println("master init >>> region rpc dial success:", region_ip)
		master.regionClients[region_ip] = client
	}

	//初始化ip含有的table列表
	master.owntablelist=make(map[string]*[]string)
	for _,region_ip := range util.Region_IPs{
		master.owntablelist[region_ip]=&[]string{}
	}

	//初始化该table所在region的ip
	//TODO

	
}

func (master *Master) Run(){
	fmt.Println("master init and listening ")
	//初始化etcd集群
	// var err error 
	// master.etcdClient, err= clientv3.New(clientv3.Config{
	// 	Endpoints:   []string{util.ETCD_ENDPOINT},
	// 	DialTimeout: 1 * time.Second,
	// })
	// if err != nil {  
	// 	fmt.Printf("master error >>> etcd connect error: %v", err)  
	// }  
	// defer master.etcdClient.Close()



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

