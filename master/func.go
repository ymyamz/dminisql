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

//直接查看owntablelsit查询所有region的table，以表格形式返回所有table以及其所属regionip
func (m *Master) TableShow(arg string, reply *string) error {  
	fmt.Println("master tableshow.called")
	m.check_and_reset_Regions()
	var res string
	res="|"+ fmt.Sprintf(" %-15s |", "name")+ fmt.Sprintf(" %-15s |", "region_ip")+"\n"
	res+="|-----------------|-----------------|\n"

	for _,region_ip := range m.regionip_list{
		tables:=*m.owntablelist[region_ip]
		m.busy_operation_num[region_ip] += 1
		for _,table := range tables{
			res+="|"+ fmt.Sprintf(" %-15s |", table)+ fmt.Sprintf(" %-15s |", region_ip)+"\n"
		}
	}
	*reply=res
	return nil

}

func (master *Master)TableCreate(input string, reply *string)  error {  
	fmt.Println("master tablecreate.called")
	master.check_and_reset_Regions()
	items:=strings.Split(input, " ")
	table_name:=items[2]  
	_, found := master.tableIP[table_name]
	if found {
		*reply = "table already exists"
	}else {
		//寻找table数最少的节点
		min, best := math.MaxInt, ""
		for ip, pTables := range master.owntablelist {
			if len(*pTables) < min && master.busy_operation_num[ip]<util.BUSY_THRESHOLD{
				min, best = len(*pTables), ip
			}
		}

		rpcRegion:=master.regionClients[best]
		fmt.Println("best_ip:",best)
		master.busy_operation_num[best] += 1

		var res string
		//创建表
		err := rpcRegion.Go("Region.Execute", input, &res, nil)
		if err!= nil {
			fmt.Println("region return err ",err)
		}
		master.tableIP[table_name] = best
		util.AddToSlice(master.owntablelist[best], table_name)
		*reply = "table created in region " + best
	}
	fmt.Println("region return ",*reply)
	return nil
}
//test
func (master *Master)QueryReigon(input string, reply *string)  error {  
	fmt.Println("master.query called")
	// TODO Change the ip
	rpcRegion:=master.regionClients["localhost"]
	master.busy_operation_num["localhost"] += 1
	master.check_and_reset_Regions()
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

func (master *Master)check_and_reset_Regions()  error {
	all_busy := true
	for _,region_ip := range m.regionip_list{
		if m.busy_operation_num[region_ip] < util.BUSY_THRESHOLD{
			all_busy = false
		}
	}
	if all_busy{
		for _,region_ip := range m.regionip_list{
			m.busy_operation_num[region_ip] = 0
		}
	}
	return nil
}
