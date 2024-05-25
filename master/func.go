package master

import (
	"distribute-sql/util"
	"fmt"
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
		var best_server string
		master.FindBest(best_server)
		rpcRegion:=master.regionClients[best_server]
		var res string
		//创建表
		err := rpcRegion.Go("Region.Execute", input, &res, nil)
		if err!= nil {
			fmt.Println("region return err ",err)
		}
		
		*reply = res
	}
	fmt.Println("region return ",*reply)
	return nil
}

func (master *Master)TableDrop(input string, reply* string) error {
	// DROP TABLE table_name;

	fmt.Println("master tabledrop.called")
	items:=strings.Split(input, " ")
	table_name:=items[2]  
	_, found := master.tableIP[table_name]
	if !found {
		*reply = "table doesn't eist"
	}else {
		ip := master.tableIP[table_name]
		rpcRegion := master.regionClients[ip]
		var res string
		// drop table
		call, err := TimeoutRPC(rpcRegion.Go("Region.Execute", input, &res, nil), 10000)
		if err!= nil {
			fmt.Println("region return err ",err)
			return err
		}
		if call.Error != nil{
			fmt.Println("%v region process table drop failed", ip)
			return call.Error
		}
		
		master.deleteTable(args.Table, ip)
		return nil
	}
	fmt.Println("region return ",*reply)
	return ni
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


func (master *Master)FindBest(best string*) error {
	min, *best := math.MaxInt, ""
	for ip, pTables := range master.serverTables {
		if len(*pTables) < min {
			min, *best = len(*pTables), ip
		}
	}
	return nil
}