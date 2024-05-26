package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strings"

	// 导入包，导入前缀为下划线，则init函数被执行，然后注册驱动。
	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB
var err error

func main() {
	// Open() 函数指定驱动名称和数据源名称
	db, err = sql.Open("sqlite3", "data.db")
	if err != nil {
		fmt.Printf("Database creation failed: %v\n", err)
		return
	}
	// 调用db.Close() 函数，确保关闭数据库并阻止启动新的查询
	defer db.Close()
	create_init()

	//模拟一个数据库，反复从命令行读取一行sql语句交给sqlite执行，区分是执行语句还是查询语句如果是查询语句，则打印出查询结果；如果执行语句，则打印出执行结果；如果输入exit，则退出循环。
	// 循环读取命令行输入，直到输入exit退出循环
	for {
		fmt.Print("Enter SQL statement or 'exit': ")
		var input string
		
		input = accept_sql_statement()
		//把input转化为全小写
		input = strings.ToLower(input)
		if input == "exit" {
			break
		}
		//执行show类型语句
		if input[0:4] == "show"  {
			show_sql(input)
			continue
		}
		// 执行查询语句
		if input[0:6] == "select" {
			select_sql(input)
		} else {
		// 执行非查询语句
			_, err := db.Exec(input)
			if err != nil {
				fmt.Printf("Execute failed: %v\n", err)
				continue
			}
			fmt.Println("Execute successful")
		}
	}

}
func create_init() {
	// 创建表
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS user (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER)")
	if err != nil {
		fmt.Printf("Table creation failed: %v\n", err)
		return
	}
	// 插入数据
	_, err = db.Exec("INSERT INTO user (name, age) VALUES (?, ?)", "Tom", 25)
	if err != nil {
		fmt.Printf("Insert failed: %v\n", err)
		
		return
	}
	fmt.Println("Insert successful")
}

// accept_sql_statement() 函数从命令行读取一行sql语句，直到输入分号结束，然后返回sql语句字符串。

func accept_sql_statement()(string) {  
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

//处理select查询语句
func select_sql(input string){
	rows, err := db.Query(input)
	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
		return
	}
	cols, _ := rows.Columns()  
	colVals := make([]interface{}, len(cols))  
	colPtrs := make([]interface{}, len(cols))  
	for i := range colPtrs {  
		colPtrs[i] = &colVals[i]  
	}  

	// Print column headers  
	header := "|"  
	separator := "|"  
	for _, colName := range cols {  
		header += fmt.Sprintf(" %-15s |", colName)  // Assuming a maximum width of 15 for each column  
		separator += "-----------------|"  
	}  
	fmt.Println(header)  
	fmt.Println(separator)  
	// Iterate over rows  
	for rows.Next() {  
		err = rows.Scan(colPtrs...)  
		if err != nil {  
			fmt.Println(err)  
			return  
		}  
		rowOutput := "|"  
		for _, col := range colVals {  
			if col == nil {  
				rowOutput += fmt.Sprintf(" %-15s |", "NULL")  
			} else {  
				switch v := col.(type) {  
				case []byte:  
					rowOutput += fmt.Sprintf(" %-15s |", string(v))  
				case int64:  
					rowOutput += fmt.Sprintf(" %-15d |", v)  
				case string:  
					rowOutput += fmt.Sprintf(" %-15s |", v)  
				default:  
					rowOutput += fmt.Sprintf(" %-15s |", "Unknown type")  
				}  
			}  
		}  
		fmt.Println(rowOutput)  
	}  
}
func show_sql(input string) {

	if(input=="show tables"){
		select_sql("SELECT name FROM sqlite_master WHERE type='table'")
	} else if(input=="show databases"){
		//查询sqlite返回当前数据库名称

	} else{
		fmt.Println("Invalid show command")
	}	

}