package region

import (
	"context"
	"database/sql"
	"distribute-sql/util"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"

	_ "github.com/mattn/go-sqlite3"
	clientv3 "go.etcd.io/etcd/client/v3"
)
type Region struct {
	db *sql.DB
	etcdClient   *clientv3.Client
	hostIP string
	backupIP string
	backupClient *rpc.Client

}
func (region *Region) Init() {

	region.hostIP = region.foundhostIP()
	var err error
	region.etcdClient, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{util.ETCD_ENDPOINT},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {  
		fmt.Printf("master error >>> etcd connect error: %v", err)  
	}  
	defer region.etcdClient.Close()
	go region.keepalive()


	//连接数据库文件

	region.db, err = sql.Open("sqlite3", util.DB_FILEPATH)
	if err != nil {
		fmt.Printf("Database creation failed: %v\n", err)
		return
	}
	defer region.db.Close()
	fmt.Printf("Database connection successful\n")

	//注册RPC服务
	// 注册rpc函数
	rpc.Register(region)
	rpc.HandleHTTP()
	// 启动server
	l, err := net.Listen("tcp",  util.REGION_PORT)
	if err != nil {
		fmt.Println("Accept error:", err)
	}
	go http.Serve(l, nil) // 进入的链接让rpc来执行
	for {
		time.Sleep(10 * time.Second)
	}


}

//在etcd中通过租约来保持心跳
func (region *Region) keepalive() {

	for {
		lease, err := region.etcdClient.Grant(context.Background(), 5)
		if err != nil {
			log.Printf("etcd grant error")
			continue
		}

		_, err = region.etcdClient.Put(context.Background(), region.hostIP, "", clientv3.WithLease(lease.ID))
		if err != nil {
			log.Printf("etcd put error")
			continue
		}

		ch, err := region.etcdClient.KeepAlive(context.Background(),lease.ID)
		if err != nil {
			log.Printf("etcd keepalive error")
			continue
		}

		for _ = range ch {
		}
	}

}

func (region *Region)foundhostIP()string{
	// 获取本机的主要IP地址  
	conn, err := net.Dial("udp", "8.8.8.8:80")  
	if err != nil {  
		fmt.Println(err)  
	}  
	defer conn.Close()  
  
	localAddr := conn.LocalAddr().(*net.UDPAddr)  
	localIP:=localAddr.IP.String()  
	fmt.Println("Local IP address:", localIP)  

	
	// 检查localIP是否在Region_IPs中  
	found := false  
	for _, ip := range util.Region_IPs {  
		if ip == localIP {  
			found = true  
			break  
		}  
	}  
	
	if found {  
		fmt.Println("Local IP found in Region_IPs")  
		return localIP  
	} else {  
		fmt.Println("Local IP not found in Region_IPs")  
		return "localhost"
	}  
	

}


