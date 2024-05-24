package master

import (
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
		rpcRegion:=master.regionClients["localhost"]
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
//test
func (master *Master)QueryReigon(input string, reply *string)  error {  
	fmt.Println("master.query called")
	rpcRegion:=master.regionClients["localhost"]
	var res string
	//创建表
	err := rpcRegion.Go("Region.Query", input, &res, nil)
	if err!= nil {
		fmt.Println("region return err ",err)
	}
	fmt.Println("region return ",res)
	*reply = res
	return nil
}
