package master

import (
	"context"
	"distribute-sql/util"
	"fmt"
	"log"
	"net/rpc"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

//记录了etcd的相关代码，监视在util中的region_ip的etcd变化

//如果list是单数，返回相对应的主从server和backup字符串数组，和一个落单的avaiable;
//如果list是双数，返回相对应的主从server和backup字符串数组，avaiable=""。
func (master *Master) assignment(available_list []string) {

	if len(available_list) % 2 == 1 {
		num:=(len(available_list)+1)/2
		master.Available= available_list[0]
		master.RegionIPList= available_list[1:num]
		back_list:=available_list[num:]
		//对于regioniplist中的每一个，建立映射server到backup的映射关系在master.Backup中
		for i := 0; i < num-1; i++ { 
			master.Backup[master.RegionIPList[i]]=back_list[i]
		}

	} else {
		num:=len(available_list)/2
		master.Available= ""
		master.RegionIPList= available_list[:num]
		back_list:=available_list[num:]
		for i := 0; i < num; i++ { 
			master.Backup[master.RegionIPList[i]]=back_list[i]
		}
	}
}

func (master *Master) getAvailableRegions()[]string {
	var err error
	master.EtcdClient, err= clientv3.New(clientv3.Config{
		Endpoints:   []string{util.ETCD_ENDPOINT},
		DialTimeout: 1 * time.Second,
	})
	if err != nil {
		fmt.Printf("master error >>> etcd connect error: %v", err)
	}
	defer master.EtcdClient.Close()

	//提取返回当前所有key值的string[]
	//声明一个空的available_list，用于存储当前所有region的ip地址
	available_list := make([]string, 0)

	regions, err := master.EtcdClient.Get(context.Background(), "", clientv3.WithPrefix())
	if err!= nil {
		fmt.Printf("master error >>> etcd get regions error: %v", err)
	}
	for _, region := range regions.Kvs {
		IP := string(region.Key)
		available_list = append(available_list, IP)
	}
	
	return available_list

}

// 用于初始化和后续加入region的连接
func (master *Master) addRegion (region_ip string) {
	//根据是否有available来决定是否添加region
	if master.Available == "" {
		master.Available = region_ip
	}else{
		//把当前的设为主server，available设为backup
		back_ip := master.Available
		client, err := rpc.DialHTTP("tcp", region_ip+util.REGION_PORT)
		if err != nil {
			fmt.Println("master error >>> region rpc " + region_ip + " dial error:", err)
			return
		}
		master.RegionCount += 1
		master.RegionClients[region_ip] = client
		master.BusyOperationNum[region_ip] = 0
		
		master.Owntablelist[region_ip] = &[]string{}
		master.Backup[region_ip] = back_ip
		//拨号通知server backup，删除server和backup内的data.db数据

	}

}

func (master *Master) watch() {
	watcher := master.EtcdClient.Watch(context.Background(), "", clientv3.WithPrefix())
	for wresp := range watcher {
		for _, ev := range wresp.Events {
			fmt.Printf("Type:%s Key:%s Value:%s\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			IP := string(ev.Kv.Value)
			switch ev.Type {

			case clientv3.EventTypePut:
				//新增region,清除新增region的所有表格（可能落后于版本）
				
				master.addRegion(IP)

			case clientv3.EventTypeDelete:
				//如果是主server挂了
				//判断IP在RegionIPList中，如果是，则把backup中内容都转存
				ok := util.FindElement(&master.RegionIPList, IP)
				if ok!=-1 {
					master.deleteserver(IP)
				}else{
					master.deletebackup(IP)
				}
				
				
			}

		}
	}
}



func (master *Master) transferOwnTables(src, dst string) {
	pTables := master.Owntablelist[src]
	for _, table := range *pTables {
		master.TableIP[table] = dst
	}
	master.Owntablelist[dst] = pTables
	delete(master.Owntablelist, src)
}

func (master *Master) removeOwnTables(ip string) {
	pTables := master.Owntablelist[ip]
	for _, table := range *pTables {
		master.deleteTableIndices(table)
		delete(master.TableIP, table)
	}
	delete(master.Owntablelist, ip)
}

func (master *Master) deleteserver(IP string) {
	//如果有available，启动backup为server;否则把backup中内容都转存到某个ip中。
	if master.Available != "" {
		//从master.RegionIPList中删除
		util.DeleteFromSlice(&master.RegionIPList, IP)

		//删除client
		client, ok := master.RegionClients[IP]
		if ok {
			client.Close()
			delete(master.RegionClients, IP)
		}
		//拨号添加backup
		new_server := master.Backup[IP]
		new_client, err := rpc.DialHTTP("tcp", new_server+util.REGION_PORT)
		if err != nil {
			fmt.Println("master error >>> region rpc " + new_server + " dial error:", err)
			return
		}
		master.RegionClients[new_server] = new_client//添加到server列表
		//转移owntablelist
		_, ok = master.Owntablelist[IP]
		if ok {
			backupIP, ok := master.Backup[IP]
			if ok {
				master.transferOwnTables(IP, backupIP)
			} else {
				log.Printf("%v has no backup", IP)
				master.removeOwnTables(IP)
			}
		}
		delete(master.Backup, IP)
		master.Backup[new_server]=master.Available
		master.Available=""
		//拨号通知server他的backup
		var suc bool
		_, err = util.TimeoutRPC(client.Go("Region.AssignBackup", master.Backup[new_server], &suc, nil), util.TIMEOUT_M)
		if err != nil {
			fmt.Println("region return err ", err)
		}
		fmt.Println("server "+IP+" down, "+new_server+"change to server with backup is "+master.Backup[new_server])

	}else{
		//把backup中内容都转存到某个ip中
		//TODO
	}
}

func (master *Master) deletebackup(IP string) {
	if master.Available != "" {
		//把avaiable设为backup
		//查询backup中是哪个server的值是IP
		server:=""
		for k, v := range master.Backup {
			if v == IP {
				server = k
				break
			}
		}
		if server == "" {
			fmt.Printf("master error >>> backup %v not found", IP)
			return
		}
		new_backup:=master.Available
		master.Backup[server] = new_backup
		//拨号通知server他的backup
		client:= master.RegionClients[server]

		var suc bool
		_, err := util.TimeoutRPC(client.Go("Region.AssignBackup", master.Backup[server], &suc, nil), util.TIMEOUT_M)
		if err != nil {
			fmt.Println("AssignBackup return err ", err)
		}
		fmt.Println("server "+server+" 's backup "+IP+" change to "+master.Backup[server])
		//从master.Backup中删除
		master.Available=""
	}else{
		//把backup中内容都转存到某个server pair中,backup转为available
		//TODO
	}

}