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

// 直接查看owntablelsit查询所有region的table，以表格形式返回所有table以及其所属regionip
func (m *Master) TableShow(arg string, reply *string) error {
	fmt.Println("master tableshow.called")
	m.check_and_reset_Regions()
	var res string
	res = "|" + fmt.Sprintf(" %-15s |", "name") + fmt.Sprintf(" %-15s |", "region_ip") + "\n"
	res += "|-----------------|-----------------|\n"

	for _, region_ip := range m.RegionIPList {
		tables := *m.Owntablelist[region_ip]
		m.BusyOperationNum[region_ip] += 1
		for _, table := range tables {
			res += "|" + fmt.Sprintf(" %-15s |", table) + fmt.Sprintf(" %-15s |", region_ip) + "\n"
		}
	}
	*reply = res
	return nil

}

func (master *Master) TableCreate(input string, reply *string) error {
	fmt.Println("master tablecreate.called")
	master.check_and_reset_Regions()
	items := strings.Split(input, " ")
	//table_name := items[2]
	table_name := extractTable(items[2])
	_, found := master.TableIP[table_name]
	if found {
		*reply = "table already exists"
	} else {
		//寻找table数最少的节点
		min, best := math.MaxInt, ""
		for ip, pTables := range master.Owntablelist {
			if len(*pTables) < min && master.BusyOperationNum[ip] < util.BUSY_THRESHOLD {
				min, best = len(*pTables), ip
			}
		}

		rpcRegion := master.RegionClients[best]
		fmt.Println("best_ip:", best)
		master.BusyOperationNum[best] += 1

		var res string
		//创建表
		err := rpcRegion.Go("Region.Execute", input, &res, nil)
		if err != nil {
			fmt.Println("region return err ", err)
		}
		master.TableIP[table_name] = best
		util.AddToSlice(master.Owntablelist[best], table_name)
		*reply = "table created in region " + best
	}
	fmt.Println("region return ", *reply)
	return nil
}

// test
func (master *Master) QueryReigon(input string, reply *string) error {
	fmt.Println("master.query called")
	// TODO Change the ip
	rpcRegion := master.RegionClients["localhost"]
	master.BusyOperationNum["localhost"] += 1
	master.check_and_reset_Regions()
	var res string

	call, err := util.TimeoutRPC(rpcRegion.Go("Region.Query", input, &res, nil), util.TIMEOUT_M)
	if err != nil {
		fmt.Println("SYSTEM HINT>>> timeout, region down!")
	}
	if call.Error != nil {
		fmt.Println("RESULT>>> failed ", call.Error)
	} else {
		fmt.Println("RESULT>>> res: \n", res)
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
	//table_name := extractTable(items[2])

	// 检查要删除的表是否存在
	_, found := master.TableIP[table_name]
	if !found {
		*reply = "table doesn't exist"
	} else {
		// 获取要删除表的服务器 IP 地址
		ip := master.TableIP[table_name]
		rpcRegion := master.RegionClients[ip]
		var res string

		// 调用远程过程执行 SQL 命令
		call, err := util.TimeoutRPC(rpcRegion.Go("Region.Execute", input, &res, nil), util.TIMEOUT_M)
		if err != nil {
			fmt.Println("region return err ", err)
			return err
		}

		// 检查远程过程调用是否成功
		if call.Error != nil {
			fmt.Printf("%v region process table drop failed", ip)
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
	master.deleteTableIndices(table)
	delete(master.TableIP, table)
	util.DeleteFromSlice(master.Owntablelist[ip], table)
}

func (master *Master) check_and_reset_Regions() error {
	all_busy := true
	for _, region_ip := range master.RegionIPList {
		if master.BusyOperationNum[region_ip] < util.BUSY_THRESHOLD {
			all_busy = false
		}
	}
	if all_busy {
		for _, region_ip := range master.RegionIPList {
			master.BusyOperationNum[region_ip] = 0
		}
	}
	return nil
}

// 提取table名
// 规则:若有(等特殊字符，table名需要用[]框起来
func extractTable(s string) string {
	if len(s) == 0 {
		return ""
	}

	if s[0] == '[' {
		// 查找']'的位置
		for i, char := range s {
			if char == ']' {
				// 返回']'和'['之间的字符串
				return s[1:i]
			}
		}
	} else {
		// 查找'（'的位置
		for i, char := range s {
			if char == '(' {
				// 返回找到的'('前的字符串
				return s[:i]
			}
		}
	}

	return ""
}

// 创建索引
func (master *Master) IndexCreate(input string, reply *string) error {
	fmt.Println("master indexcreate.called")
	items := strings.Split(input, " ")
	index_name := items[2]
	_, found := master.IndexInfo[index_name]
	if found {
		*reply = "Index already exists"
		return nil
	}
	table_name := extractTable(items[4])

	_, found1 := master.TableIP[table_name]
	if !found1 {
		*reply = "table doesn't exists"
		return nil
	}

	ip := master.TableIP[table_name]

	rpcRegion := master.RegionClients[ip]
	fmt.Println("table_ip:", ip)

	var res string
	//创建索引
	err := rpcRegion.Go("Region.Execute", input, &res, nil)
	if err != nil {
		fmt.Println("region return err ", err)
	}
	//fmt.Println(res)
	if res != "Execute failed" {
		//todo 如何检测构造失败 当构造失败时res输出为空
		//fmt.Println("!!1")
		master.IndexInfo[index_name] = table_name
		//fmt.Println("!!2")
		//master.tableIndex[table_name] = index_name
		util.AddToSliceIndex(master.TableIndex[table_name], index_name)
		//fmt.Println("!!3")
		*reply = "index created in region " + ip
		fmt.Println("region return ", *reply)
	} else {
		*reply = "failed"
		fmt.Println("Execute failed")
	}
	return nil
}
func (master *Master) deleteTableIndices(table string) {
	//_, found := master.tableIndex[table]
	//if !found {
	//	return
	//}
	indexes := master.TableIndex[table]
	if indexes != nil {
		// 遍历索引切片
		for _, index := range *indexes {
			master.deleteIndex(index, table)
		}
	} else {
		//不存在索引
		return
	}
	//master.deleteIndex(master.tableIndex[table])
	//delete(master.tableIndex, table)
}
func (master *Master) deleteIndex(index string, table string) {
	delete(master.IndexInfo, index)
	util.DeleteFromSlice(master.TableIndex[table], index)
}

// 删除索引
func (master *Master) IndexDrop(input string, reply *string) error {
	fmt.Println("master indexdrop.called")

	// 解析输入命令，获取要删除的索引名
	items := strings.Split(input, " ")
	index_name := items[2]

	// 检查要删除的索引是否存在
	_, found := master.IndexInfo[index_name]
	if !found {
		*reply = "index doesn't exist"
		return nil
	}

	//要删除索引的ip地址
	table_name := master.IndexInfo[index_name]
	ip := master.TableIP[table_name]
	rpcRegion := master.RegionClients[ip]
	var res string

	// 调用远程过程执行 SQL 命令
	call, err := util.TimeoutRPC(rpcRegion.Go("Region.Execute", input, &res, nil), util.TIMEOUT_M)
	if err != nil {
		fmt.Println("region return err ", err)
		return err
	}

	// 检查远程过程调用是否成功
	if call.Error != nil {
		fmt.Println("%v region process index drop failed", ip)
		return call.Error
	}

	master.deleteIndex(index_name, table_name)
	return nil

	fmt.Println("region return ", *reply)
	return nil
}

// 查询index
func (master *Master) IndexShow(arg string, reply *string) error {
	fmt.Println("master indexshow.called")
	var res string
	res = "|" + fmt.Sprintf(" %-15s |", "index_name") + fmt.Sprintf(" %-15s |", "table") + "\n"
	res += "|-----------------|-----------------|\n"
	for index, table := range master.IndexInfo {
		res += fmt.Sprintf("| %-15s | %-15s |\n", index, table)
	}
	*reply = res
	return nil

}


