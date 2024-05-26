package master

import (
	"context"
	"distribute-sql/util"
	"fmt"
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

func (master *Master) watch() {
	watcher := master.EtcdClient.Watch(context.Background(), "", clientv3.WithPrefix())
	for wresp := range watcher {
		for _, ev := range wresp.Events {
			fmt.Printf("Type:%s Key:%s Value:%s\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			IP := string(ev.Kv.Value)
			switch ev.Type {

			case clientv3.EventTypePut:
				//新增region,清除新增region的所有表格（可能落后于版本）
				master.RegionCount += 1
				master.addRegion(IP)

			case clientv3.EventTypeDelete:
				//删除region,清除region缓存在本地的所有表格，启动backup
				master.RegionCount -= 1

			}

		}
	}
}
