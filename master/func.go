package master

import (
	"distribute-sql/util"
	"fmt"
	"math"
	"strings"
)

func (m *Master) CallTest(arg string, reply *string) error {  
	fmt.Println("CALL master SUCCESS")  
	*reply = "hello " + arg  
	return nil  
}  

func (master *Master)TableCreate(input string, reply *string)  error {  
	fmt.Println("master tablecreate.called")
	items:=strings.Split(input, " ")
	table_name:=items[2]  
	_, found := master.tableIP[table_name]
	if found {
		*reply = "table already exists"
	}else {
		//寻找table数最少的节点
		min, best := math.MaxInt, ""
		for ip, pTables := range master.owntablelist {
			if len(*pTables) < min {
				min, best = len(*pTables), ip
			}
		}

		rpcRegion:=master.regionClients[best]
		fmt.Println("best_ip:",best)

		var res string
		//创建表
		err := rpcRegion.Go("Region.Execute", input, &res, nil)
		if err!= nil {
			fmt.Println("region return err ",err)
		}
		master.tableIP[table_name] = best
		*reply = res
	}
	fmt.Println("region return ",*reply)
	return nil
}
//test
func (master *Master)QueryReigon(input string, reply *string)  error {  
	fmt.Println("master.query called")
	rpcRegion:=master.regionClients["localhost"]
	var res string

	call, err := util.TimeoutRPC(rpcRegion.Go("Region.Query", input, &res, nil), util.TIMEOUT_M)
	if err != nil {
		fmt.Println("SYSTEM HINT>>> timeout, region down!")
	}
	if call.Error != nil {
		fmt.Println("RESULT>>> failed ",call.Error)
	} else {
		fmt.Println("RESULT>>> res: \n",res)
	}
	*reply = res

	return nil
}

// TableDrop 方法用于在分布式系统中删除表
func (master *Master) TableDrop(input string, reply *string) error {
    fmt.Println("master tabledrop.called")
    
    // 解析输入命令，获取要删除的表名
    items := strings.Split(input, " ")
    table_name := items[2]
    
    // 检查要删除的表是否存在
    _, found := master.tableIP[table_name]
    if !found {
        *reply = "table doesn't exist"
    } else {
        // 获取要删除表的服务器 IP 地址
        ip := master.tableIP[table_name]
        rpcRegion := master.regionClients[ip]
        var res string
        
        // 调用远程过程执行 SQL 命令
        call, err := util.TimeoutRPC(rpcRegion.Go("Region.Execute", input, &res, nil), util.TIMEOUT_M)
        if err != nil {
            fmt.Println("region return err ", err)
            return err
        }
        
        // 检查远程过程调用是否成功
        if call.Error != nil {
            fmt.Println("%v region process table drop failed", ip)
            return call.Error
        }
        
        // 删除表，并更新数据结构
        master.deleteTable(table_name, ip)
        return nil
    }
    
    fmt.Println("region return ", *reply)
    return nil
}

func (master *Master) deleteTable(table, ip string) {
	// master.deleteTableIndices(table)
	delete(master.tableIP, table)
	util.DeleteFromSlice(master.owntablelist[ip], table)
}