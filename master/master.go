package master

import (
	"distribute-sql/util"
	"encoding/gob"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Master struct {
	RegionCount      int
	EtcdClient       *clientv3.Client
	RegionClients    map[string]*rpc.Client
	Owntablelist     map[string]*[]string // ip -> tables
	TableIP          map[string]string    // table -> ip
	Backup           map[string]string    // region server ip -> Backup server ip
	Available        string               // available regions
	RegionIPList     []string
	BusyOperationNum map[string]int       // operations for each region in 1 minute, > BUSY_THRESHOLD deemed as busy
	IndexInfo        map[string]string    // index->table
	TableIndex       map[string]*[]string // table->indexs
}

// SerializableMaster is used for selective serialization
type SerializableMaster struct {
	RegionCount      int
	Owntablelist     map[string]*[]string // ip -> tables
	TableIP          map[string]string    // table -> ip
	Backup           map[string]string    // region server ip -> backup server ip
	Available        string               // available regions
	RegionIPList     []string
	BusyOperationNum map[string]int       // operations for each region in 1 minute, > BUSY_THRESHOLD deemed as busy
	IndexInfo        map[string]string    // index->table
	TableIndex       map[string]*[]string // table->indexs
}

func (master *Master) toSerializable() *SerializableMaster {
	return &SerializableMaster{
		RegionCount:      master.RegionCount,
		Owntablelist:     master.Owntablelist,
		TableIP:          master.TableIP,
		Backup:           master.Backup,
		Available:        master.Available,
		RegionIPList:     master.RegionIPList,
		BusyOperationNum: master.BusyOperationNum,
		IndexInfo:        master.IndexInfo,
		TableIndex:       master.TableIndex,
	}
}

func (master *Master) fromSerializable(serializableMaster *SerializableMaster) {
	master.RegionCount = serializableMaster.RegionCount
	master.Owntablelist = serializableMaster.Owntablelist
	master.TableIP = serializableMaster.TableIP
	master.Backup = serializableMaster.Backup
	master.Available = serializableMaster.Available
	master.RegionIPList = serializableMaster.RegionIPList
	master.BusyOperationNum = serializableMaster.BusyOperationNum
	master.IndexInfo = serializableMaster.IndexInfo
	master.TableIndex = serializableMaster.TableIndex
}

func (master *Master) SaveToFile(filename string, reply *string) error {
	fmt.Println("Saving Master struct to file...")
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(master.toSerializable())
	if err != nil {
		return err
	}

	return nil
}

func LoadFromFile(filename string) (*SerializableMaster, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	var serializableMaster SerializableMaster
	err = decoder.Decode(&serializableMaster)
	if err != nil {
		return nil, err
	}

	return &serializableMaster, nil
}

// func (master *Master) Init(mode string) {
// 	//便于本地测试
// 	if mode == "d" {
// 		master.RegionIPList = util.Region_IPs
// 	} else {
// 		master.RegionIPList = util.Region_IPs_LOCAL
// 	}
// 	master.RegionCount = len(master.RegionIPList)
// 	master.BusyOperationNum = make(map[string]int)
// 	// etcd client init
// 	// wait for update

// 	//code阶段，先对region进行初始化，后续再进行优化
// 	//遍历每一个region_ips，建立rpc连接
// 	master.RegionClients = make(map[string]*rpc.Client)
// 	for _, region_ip := range master.RegionIPList {
// 		client, err := rpc.DialHTTP("tcp", region_ip+util.REGION_PORT)
// 		if err != nil {
// 			fmt.Println("master error >>> region rpc dial error:", err)
// 			return
// 		}
// 		fmt.Println("master init >>> region rpc dial success:", region_ip)
// 		master.RegionClients[region_ip] = client
// 		master.BusyOperationNum[region_ip] = 0
// 	}

// 	//初始化ip含有的table列表
// 	master.Owntablelist = make(map[string]*[]string)
// 	for _, region_ip := range master.RegionIPList {
// 		master.Owntablelist[region_ip] = &[]string{}
// 	}

//	//初始化索引
//	master.IndexInfo = make(map[string]string)
//	master.TableIndex = make(map[string]*[]string)
// 	//初始化该table所在region的ip

// 	master.TableIP = make(map[string]string)
// 	master.InitTableIP()

// 	master.Backup = make(map[string]string)
// 	master.BusyOperationNum = make(map[string]int)
// }

func (master *Master) Init(mode string) {
	// Attempt to load from file
	serializableMaster, err := LoadFromFile("master.gob")
	master.RegionClients = make(map[string]*rpc.Client)
	master.Backup = make(map[string]string)
	master.Available = ""
	//if err == nil {
	//test init
	if false  {
		// Successfully loaded from file
		master.fromSerializable(serializableMaster)
		fmt.Println("Master struct loaded from file")
		for _, region_ip := range master.RegionIPList {
			client, err := rpc.DialHTTP("tcp", "localhost:"+region_ip)
			if err != nil {
				fmt.Println("master error >>> region rpc dial error:", err)
				return
			}
			fmt.Println("master init >>> region rpc dial success:", region_ip)
			master.RegionClients[region_ip] = client
			master.BusyOperationNum[region_ip] = 0
		}
	} else {
		// Proceed with initialization if loading fails
		fmt.Println("Initializing Master struct...")
		//load from etcd

		// if mode == "d" {
		// 	master.RegionIPList = util.Region_IPs
		// } else {
		// 	master.RegionIPList = util.Region_IPs_LOCAL
		// }
		available_list := master.getAvailableRegions()
		fmt.Println("etcd find regions:", available_list)
		master.assignment(available_list)
		//打印分配后的结果（Available，RegionIPList）

		fmt.Println("Available regions:", master.Available)
		for i, region_ip := range master.RegionIPList {
			fmt.Println("Region", i, ":", region_ip, " backup:", master.Backup[region_ip])
		}

		master.RegionCount = len(master.RegionIPList)
		master.BusyOperationNum = make(map[string]int)
		master.RegionClients = make(map[string]*rpc.Client)

		// etcd client init
		// TODO: Initialize EtcdClient properly
		master.EtcdClient = nil // Placeholder, replace with actual initialization
		for _, region_ip := range master.RegionIPList {
			client, err := rpc.DialHTTP("tcp", "localhost:"+region_ip)
			if err != nil {
				fmt.Println("master error >>> region rpc dial error:", err)
				return
			}
			fmt.Println("master init >>> region rpc dial success:", region_ip)
			master.RegionClients[region_ip] = client
			master.BusyOperationNum[region_ip] = 0
			
			//通知server服务器它的backup服务器是谁,注意需要等待返回后才能下一步（不可是异步的，会冲突报错）
			var res string
			err = client.Call("Region.AssignBackup", master.Backup[region_ip], &res)  
			if err != nil {
				fmt.Println("SYSTEM HINT>>> timeout, region down!")
			}

			//初始化backup服务器
			bkclient, err := rpc.DialHTTP("tcp", "localhost:"+master.Backup[region_ip])
			if err != nil {
				fmt.Println("master error >>> bkup region rpc dial error:", err)
				return
			}
			fmt.Println("master init >>> bkup region rpc dial success:", master.Backup[region_ip])
			master.RegionClients[master.Backup[region_ip]] = bkclient
		}

		//初始化索引
		master.IndexInfo = make(map[string]string)
		master.TableIndex = make(map[string]*[]string)

		// Initialize owntablelist
		master.Owntablelist = make(map[string]*[]string)
		for _, region_ip := range master.RegionIPList {
			master.Owntablelist[region_ip] = &[]string{}
		}

		// Initialize tableIP
		master.TableIP = make(map[string]string)
		master.InitTableIP()

	}

	// Save to file after initialization
	var reply string
	err = master.SaveToFile("master.gob", &reply)
	if err != nil {
		fmt.Println("Error saving to file:", err)
	}
}

func (master *Master) Run() {
	fmt.Println("master init and listening ")
	//初始化etcd集群
	var err error
	master.EtcdClient, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{util.ETCD_ENDPOINT},
		DialTimeout: 1 * time.Second,
	})
	if err != nil {
		fmt.Printf("master error >>> etcd connect error: %v", err)
	}
	defer master.EtcdClient.Close()
	go master.watch() // 启动etcd监听

	// 注册rpc函数
	rpc.Register(master)
	rpc.HandleHTTP()
	// 启动server
	l, err := net.Listen("tcp", util.MASTER_IP_LOCAL)
	if err != nil {
		fmt.Println("Accept error:", err)
	}
	go http.Serve(l, nil) // 进入的链接让rpc来执行
	for {
		time.Sleep(10 * time.Second)
	}

}

// 把本地的db文件中的table信息同步
func (master *Master) InitTableIP() {
	for _, region_ip := range master.RegionIPList {
		client := master.RegionClients[region_ip]

		var res []string
		call, err := util.TimeoutRPC(client.Go("Region.TableName", "no use", &res, nil), util.TIMEOUT_M)
		if err != nil {
			fmt.Println("SYSTEM HINT>>> timeout, region down!")
		}
		if call.Error != nil {
			fmt.Println("RESULT>>> failed ", call.Error)
		} else {
			fmt.Println("RESULT>>> res: \n", res)
		}
		if len(res) != 0 && res[0] == "failedinquery" {
			continue
		}
		//打印返回的table列表
		fmt.Println("region_ip:", region_ip, "table list:", res)
		//更新本地的tableIP和owntablelist
		for _, table := range res {
			master.TableIP[table] = region_ip
			master.InitIndex(table) //根据table初始化索引
		}
		master.Owntablelist[region_ip] = &res
	}

}

// 询问master某个table在哪个ip的region中
func (master *Master) GetTableIP(table string, reply *string) error {
	if _, ok := master.TableIP[table]; !ok {
		*reply = ""
	}
	*reply = master.TableIP[table]
	return nil
}

// 把本地的db文件中的index信息同步
// SELECT * FROM sqlite_master
// WHERE type='index' AND tbl_name='your_table_name';

func (master *Master) InitIndex(table string) {
	//for _, table := range master.tableIP {
	ip := master.TableIP[table]
	client := master.RegionClients[ip]

	//fmt.Println("table=", table, "ip=", ip, "client=", client)

	var res []string
	call, err := util.TimeoutRPC(client.Go("Region.Index", table, &res, nil), util.TIMEOUT_M)

	if err != nil {
		fmt.Println("SYSTEM HINT>>> timeout, region down!")
	}
	if call.Error != nil {
		fmt.Println("RESULT>>> failed ", call.Error)
	} else {
		fmt.Println("RESULT>>> res: \n", res)
	}
	//打印返回的index列表
	fmt.Println("table:", table, "index list:", res)

	//更新
	for _, index := range res {

		if index != "failedinquery" && index != "failedinscan" {

			master.IndexInfo[index] = table
		}
		master.TableIndex[table] = &res
	}

	//}

}

func (master *Master) AllTableIp(placeholder string, reply *map[string]string) error {
	*reply = master.TableIP
	return nil
}
