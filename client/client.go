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
				call_func:="Master.TableCreate"
				client.connect_to_master(call_func,input)
				
			}
		case "show":
			//如果是show tables，则input要修改.//有待修改成返回所有region的table
			if items[1]=="tables"{
				call_func:="Master.QueryReigon"
				input_showtables:="SELECT name FROM sqlite_master WHERE type='table'"
				client.connect_to_master(call_func,input_showtables)

			}

		//其他默认执行
		default:
			//先解析出具体的table，询问master table的ip地址，然后连接到对应的region，执行sql语句
			table_name:=client.prepocess_sql(input)
			if table_name!=""{
				//询问master table的ip地址
				region_ip:=client.connect_to_master("Master.GetTableIP",table_name)
				//连接到对应的region，执行sql语句
				if region_ip!=""{
					client.connect_to_region(region_ip,"Region.Query",input)
				}

			}

	}
	

}

func (client *Client)prepocess_sql(input string)string{
	var table string
	words := strings.Split(input, " ")
	if words[0] == "select" {
		//select语句的表名放在from后面
		for i := 0; i < len(words); i++ {
			if words[i] == "from" && i != (len(words)-1) {
				table = words[i+1]
				break
			}
		}
	} else if words[0] == "insert" || words[0] == "delete" {
		if len(words) >= 3 {
			table = words[2]
		}
	} else {
		table=""
	}
	return table
}