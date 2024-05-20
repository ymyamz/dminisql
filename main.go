package main

import (
	. "distribute-sql/client" // 相对路径导入client包
	"fmt"
	"os"
) 
  
func main() {  
	args := os.Args  
  
	// 获取类型client/master/region  
	if len(args) > 1 {  
		mode := args[1]  
		switch mode {  
		case "client":  
			fmt.Println("Running in client mode")  
			var client Client
			client.Init()
			client.Run()

		case "master":  
			fmt.Println("Running in master mode")  


		case "region":  
			fmt.Println("Running in region mode")  


		default:  
			fmt.Println("Unknown mode:", mode)  
		}  
	} else {  
		fmt.Println("No mode specified")  
	}  
}  