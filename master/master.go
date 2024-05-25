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
	owntablelist map[string]*[]string  // ip->tables
	//该table所在region的ip
	tableIP      map[string]string // table->ip

	backup	map[string]string // region server ip -> backup server ip
	regionip_list []string

	busy_operation_num map[string]int // operations for each region in 1 minute, > BUSY_THRESHOLD deemed as busy

}
func (master *Master) Init(mode string){
	//便于本地测试
	if mode=="d"{
		master.regionip_list=util.Region_IPs
	}else{
		master.regionip_list=util.Region_IPs_LOCAL
	}
	master.regionCount=len(master.regionip_list)
	// etcd client init
	// wait for update

	//code阶段，先对region进行初始化，后续再进行优化
	//遍历每一个region_ips，建立rpc连接
	master.regionClients=make(map[string]*rpc.Client)
	for _,region_ip := range master.regionip_list{
		client, err := rpc.DialHTTP("tcp", region_ip+util.REGION_PORT)
		if err!= nil {
			fmt.Println("master error >>> region rpc dial error:", err)
			return
		}
		fmt.Println("master init >>> region rpc dial success:", region_ip)
		master.regionClients[region_ip] = client
		master.busy_operation_num[region_ip] = 0
	}

	//初始化ip含有的table列表
	master.owntablelist=make(map[string]*[]string)
	for _,region_ip := range master.regionip_list{
		master.owntablelist[region_ip]=&[]string{}
	}

	//初始化该table所在region的ip
	//TODO
	master.tableIP=make(map[string]string)
	master.InitTableIP()

	master.backup = make(map[string]string)
	
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

//把本地的db文件中的table信息同步
func (master *Master)InitTableIP(){
	for _,region_ip := range master.regionip_list{
		client:=master.regionClients[region_ip]

		var res []string
		call, err := util.TimeoutRPC(client.Go("Region.TableName", "no use", &res, nil), util.TIMEOUT_M)
		if err != nil {
			fmt.Println("SYSTEM HINT>>> timeout, region down!")
		}
		if call.Error != nil {
			fmt.Println("RESULT>>> failed ",call.Error)
		} else {
			fmt.Println("RESULT>>> res: \n",res)
		}
		//打印返回的table列表
		fmt.Println("region_ip:",region_ip,"table list:",res)
		//更新本地的tableIP和owntablelist
		for _,table := range res{
			master.tableIP[table]=region_ip
		}
		master.owntablelist[region_ip]=&res
	}

}


//询问master某个table在哪个ip的region中
func (master *Master)GetTableIP(table string,reply *string) error{
	if _,ok:=master.tableIP[table];!ok{
		*reply=""
	}
	*reply= master.tableIP[table]
	return nil
}

