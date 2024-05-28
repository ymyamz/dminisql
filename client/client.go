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
	rpcMaster *rpc.Client
	//rpcRegionMap map[string]*rpc.Client // [ip]rpc
}

func (client *Client) Init(mode string) {

	//test local,you can change util.MASTER_IP_LOCAL

	rpcMas, err := rpc.DialHTTP("tcp", "localhost"+util.MASTER_IP_LOCAL)
	if err != nil {
		fmt.Printf("CLIENT ERROR >>> connect error: %v", err)
	}
	client.rpcMaster = rpcMas
	fmt.Println("client init and link to master ", util.MASTER_IP_LOCAL)

}
func (client *Client) Test() {

	var res string

	call, err := util.TimeoutRPC(client.rpcMaster.Go("Master.CallTest", "test", &res, nil), util.TIMEOUT_M)
	if err != nil {
		fmt.Println("SYSTEM HINT>>> timeout, master down!")
	}
	if call.Error != nil {
		fmt.Println("RESULT>>> failed", call.Error)
	} else {
		fmt.Println("RESULT>>>\n", res)
	}

}
func (client *Client) Run() {

	//模拟一个数据库，反复从命令行读取一行sql语句交给sqlite执行，区分是执行语句还是查询语句如果是查询语句，则打印出查询结果；如果执行语句，则打印出执行结果；如果输入exit，则退出循环。
	// 循环读取命令行输入，直到输入exit退出循环
	for {
		fmt.Print("Enter SQL statement or 'exit': ")

		input := client.accept_sql_statement()
		input = strings.ToLower(input)
		input = strings.TrimSpace(input)
		input = strings.ReplaceAll(input, "\\s+", " ")

		if input == "exit" {
			call_func := "Master.SaveToFile"
			client.connect_to_master(call_func, "master.gob")
			call_func = "Master.LoadBalance"
			client.connect_to_master(call_func, "")
			break
		}
		//如果是文件读入 例如".read ./sql/test.txt"
		if input[0] == '.' {
			if input[1:5] == "read" {
				//读取文件名（空格后面的内容）
				file_name := input[6:]
				file, err := os.Open(file_name)
				if err != nil {
					fmt.Println("CLIENT ERROR>>> open file error:", err)
					continue
				}
				defer file.Close()
				//读取文件内容
				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					line := scanner.Text()
					if line == "" || len(line) == 0 {
						continue
					}
					//去掉line两边的空格和末尾的分号
					line = strings.TrimSpace(line)
					if line[len(line)-1] == ';' {
						line = line[:len(line)-1]
					}
					line = strings.ToLower(line)
					line = strings.ReplaceAll(line, "\\s+", " ")
					fmt.Println(line)
					client.parse_sql_statement(line)
				}
				if err := scanner.Err(); err != nil {
					fmt.Println("CLIENT ERROR>>> read file error:", err)
					continue
				}
			}
		} else {
			//如果是正常执行语句
			client.parse_sql_statement(input)
		}
		call_func := "Master.SaveToFile"
		client.connect_to_master(call_func, "master.gob")
		call_func = "Master.LoadBalance"
		client.connect_to_master(call_func, "")
	}

}

// accept_sql_statement() 函数从命令行读取一行sql语句，直到输入分号结束，然后返回sql语句字符串。

func (client *Client) accept_sql_statement() string {
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

// 解析输入的语句
func (client *Client) parse_sql_statement(input string) {
	//解析sql语句
	items := strings.Split(input, " ")
	//if sql is create
	switch items[0] {

	case "create":
		if items[1] == "table" {
			call_func := "Master.TableCreate"
			client.connect_to_master(call_func, input)

		}

		if items[1] == "index" { //创建索引

			call_func := "Master.IndexCreate"
			client.connect_to_master(call_func, input)
		}
	case "show":
		//返回所有region的table
		if items[1] == "tables" {
			call_func := "Master.TableShow"
			//input_showtables:="SELECT name FROM sqlite_master WHERE type='table'"
			client.connect_to_master(call_func, "no use")

		}
		if items[1] == "indexes" { //查询索引

			call_func := "Master.IndexShow"
			client.connect_to_master(call_func, "no use")
		}
		//用于查询master当前属性
		if items[1] == "info" {
			call_func := "Master.ShowNowInfo"
			client.connect_to_master(call_func, "no use")
		}
	case "drop":
		if items[1] == "table" {
			call_func := "Master.TableDrop"
			client.connect_to_master(call_func, input)
		}
		if items[1] == "index" { //删除索引

			call_func := "Master.IndexDrop"
			client.connect_to_master(call_func, input)
		}

	case "select":
		var tables []string
		var size int
		size=0
		var ip string
		for i := 0; i < len(items); i++ {
			table_name := items[i]
			found := client.connect_to_master("Master.GetTableIP", table_name)
			if found != "" { //存在该table
				tables = append(tables, table_name)
				ip = found
				size+=1
			}
		}
		if size == 1 {
			client.connect_to_region(ip, "Region.Query", input)

			//client.connect_to_region_test(ip, "Region.Get", input)

		} else if size == 0 {
			fmt.Println("table doesn't exist")
		} else {
			client.connect_to_master("Master.Complex_query_master", input)
		}
	//case "test":
	//	input = "SELECT sql FROM sqlite_master WHERE tbl_name='user';"
	//	table_name := "user"
	//	region_ip := client.connect_to_master("Master.GetTableIP", table_name)
	//	client.connect_to_region_test(region_ip, "Region.Get", input)

	//其他默认执行
	default:
		//先解析出具体的table，询问master table的ip地址，然后连接到对应的region，执行sql语句
		table_name, call_func := client.prepocess_sql(input)
		if table_name != "" {
			//询问master table的ip地址
			region_ip := client.connect_to_master("Master.GetTableIP", table_name)
			//连接到对应的region，执行sql语句
			if region_ip != "" {
				client.connect_to_region(region_ip, call_func, input)
			}

		} else {
			fmt.Println("CLIENT ERROR>>> unknow sql statement:", input)
		}

	}

}

func (client *Client) prepocess_sql(input string) (string, string) {
	var table string
	var call_func string
	words := strings.Split(input, " ")
	if words[0] == "select" {
		call_func = "Region.Query"
		//select语句的表名放在from后面
		for i := 0; i < len(words); i++ {
			if words[i] == "from" && i != (len(words)-1) {
				table = words[i+1]
				break
			}
		}
	} else if words[0] == "insert" || words[0] == "delete" {
		call_func = "Region.Execute"
		if len(words) >= 3 {
			table = words[2]
		}
	} else {
		table = ""

	}
	return table, call_func
}
