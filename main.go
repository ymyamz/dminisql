package main

import (
	. "distribute-sql/client" // 相对路径导入client包
	. "distribute-sql/master"
	. "distribute-sql/region"
	"fmt"
	"os"
) 
  
func main() {  
	args := os.Args  
	// 获取类型client/master/region  
	if len(args) > 2 {  
		mode := args[1]
		//便于服务器测试  
		//env ==d or l
		var env string
		env=args[2]
		

		switch mode {  
		case "client":  
			fmt.Println("Running in client mode")  
			var client Client
			if env=="d" {
				client.Init("d")
			}else{
				client.Init("local")
			}
			client.Run()

		case "master":  
			fmt.Println("Running in master mode")  
			var master Master
			if env=="d" {
				master.Init("d")
			}else{
				master.Init("local")
			}
			master.Run()

		case "region":  
			fmt.Println("Running in region mode")  
			var region Region
			if env!="d"{
				host:=args[3]
				region.Init(host)
			}

		default:  
			fmt.Println("Unknown mode:", mode)  
		}  
	} else {  
		fmt.Println("No mode specified")  
	}  
}  