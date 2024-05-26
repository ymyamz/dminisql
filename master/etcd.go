package master

import (
	"context"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
)

//记录了etcd的相关代码，监视在util中的region_ip的etcd变化


func (master *Master) watch() {
	watcher :=master.etcdClient.Watch(context.Background(), "", clientv3.WithPrefix()) 
	for wresp := range watcher {
		for _, ev := range wresp.Events {
			fmt.Printf("Type:%s Key:%s Value:%s\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			IP:=string(ev.Kv.Value)
			switch ev.Type {

			case clientv3.EventTypePut:
				//新增region,清除新增region的所有表格（可能落后于版本）
				master.regionCount+=1
				master.addRegion(IP)
				
			case clientv3.EventTypeDelete:
				//删除region,清除region缓存在本地的所有表格，启动backup
				master.regionCount-=1
				
			}	

		}
	}
}