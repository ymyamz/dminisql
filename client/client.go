package client

import (
	"bufio"
	"distribute-sql/util"
	"fmt"
	"net/rpc"
	"os"
	"strings"
)

type Client struct {
	rpcMaster    *rpc.Client
	rpcRegionMap map[string]*rpc.Client // [ip]rpc
}

func (client *Client) Init(){
	
	//test local,you can change util.MASTER_IP_LOCAL
	rpcMas, err := rpc.DialHTTP("tcp", util.MASTER_IP_LOCAL+util.MASTER_PORT)
	if err != nil {
		fmt.Printf("CLIENT ERROR >>> connect error: %v", err)
	}
	client.rpcMaster = rpcMas	  
	fmt.Println("client init and link to master ",util.MASTER_IP)

}
func (client *Client) Test(){


	var res string
	

	call, err := util.TimeoutRPC(client.rpcMaster.Go("Master.CallTest", "test", &res, nil), util.TIMEOUT_M)
	if err != nil {
		fmt.Println("SYSTEM HINT>>> timeout, master down!")
	}
	if call.Error != nil {
		fmt.Println("RESULT>>> failed",call.Error)
	} else {
		fmt.Println("RESULT>>> res: ",res)
	}


}
func (client *Client) Run(){


	//模拟一个数据库，反复从命令行读取一行sql语句交给sqlite执行，区分是执行语句还是查询语句如果是查询语句，则打印出查询结果；如果执行语句，则打印出执行结果；如果输入exit，则退出循环。
	// 循环读取命令行输入，直到输入exit退出循环
	for {
		fmt.Print("Enter SQL statement or 'exit': ")
		
		input := client.accept_sql_statement()
		input = strings.ToLower(input)
		input = strings.TrimSpace(input)
		input = strings.ReplaceAll(input, "\\s+", " ")
		fmt.Println("Received query:",input)

		if input == "exit" {
			break
		}
		//TODO sparse
		client.parse_sql_statement(input)
		
	}


}

// accept_sql_statement() 函数从命令行读取一行sql语句，直到输入分号结束，然后返回sql语句字符串。

func (client *Client)accept_sql_statement()(string) {  
	var query strings.Builder  
	scanner := bufio.NewScanner(os.Stdin)  
	
  
	for {  
		scanner.Scan()  
		text := scanner.Text()  
  
		// 检查是否遇到分号  
		if strings.Contains(text, ";") {  
			parts := strings.Split(text, ";")  
			query.WriteString(parts[0]) // 将分号之前的部分添加到查询中  
			break                       // 退出循环  
		}  
  
		query.WriteString(text) // 将读取的文本添加到查询中  
	}  

	//fmt.Println("Received query:", query.String())
	return query.String()
}  

//解析输入的语句
func (client *Client)parse_sql_statement(input string){
	//解析sql语句
	items:=strings.Split(input, " ")
	//if sql is create
	switch items[0] {

		case "create":
			if items[1]=="table"{
				var res string
				//具体table名称解析等在master中进行
				call, err := util.TimeoutRPC(client.rpcMaster.Go("Master.TableCreate", input, &res, nil), util.TIMEOUT_M)
				if err != nil {
					fmt.Println("SYSTEM HINT>>> timeout, master down!")
				}
				if call.Error != nil {
					fmt.Println("RESULT>>> failed ",call.Error)
				} else {
					fmt.Println("RESULT>>> res: ",res)
				}
			}
		case "show":
			//如果是show tables，则input要修改
			if items[1]=="tables"{
				var res string
				input="SELECT name FROM sqlite_master WHERE type='table'"
				call, err := util.TimeoutRPC(client.rpcMaster.Go("Master.QueryReigon", input, &res, nil), util.TIMEOUT_M)
				if err != nil {
					fmt.Println("SYSTEM HINT>>> timeout, master down!")
				}
				if call.Error != nil {
					fmt.Println("RESULT>>> failed ",call.Error)
				} else {
					fmt.Println(res)
				}
			}
		case "select":

			var res string

			call, err := util.TimeoutRPC(client.rpcMaster.Go("Master.QueryReigon", input, &res, nil), util.TIMEOUT_M)
			if err != nil {
				fmt.Println("SYSTEM HINT>>> timeout, master down!")
			}
			if call.Error != nil {
				fmt.Println("RESULT>>> failed ",call.Error)
			} else {
				fmt.Println(res)
			}
			
		//其他默认执行
		default:
			var res string
			//具体table名称解析等在master中进行
			call, err := util.TimeoutRPC(client.rpcMaster.Go("Master.TableCreate", input, &res, nil), util.TIMEOUT_M)
			if err != nil {
				fmt.Println("SYSTEM HINT>>> timeout, master down!")
			}
			if call.Error != nil {
				fmt.Println("RESULT>>> failed ",call.Error)
			} else {
				fmt.Println("RESULT>>> res: ",res)
			}
	}
	

}