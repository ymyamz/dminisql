package master

import (
	"fmt"
	"log"

	_ "github.com/mattn/go-sqlite3"
)

// 批量插入
func (master *Master) Insert(data []string) error {
	table := data[0]
	fmt.Println("Insert into table:", table)
	if len(data) > 0 {
		for _, line := range data {
			fmt.Printf(line + " ")
		}
		fmt.Println()
	} else {
		fmt.Println(" no data")
	}

	sql := "INSERT INTO " + table + "("

	// 获取表的列数和列名
	GetInput := "PRAGMA table_info(" + table + ")"
	var res []string
	err := master.Get(GetInput, &res)
	if err != nil {
		log.Fatal(err)
	}

	columnCount := 0
	flag := 1
	i := 0

	// 遍历结果集以计算列数和获取列名
	for _, name := range res {
		i++
		if i == 6 {
			i = 1
		}
		if i == 2 {
			columnCount++
			if flag == 1 {
				flag = 0
			} else {
				sql += ","
			}
			sql += name
		}
	}
	fmt.Println("表的列数", columnCount)
	fmt.Println("表的列名", res)

	sql += ")VALUES ("
	for i := 0; i < columnCount; i++ {
		if i != 0 {
			sql += ","
		}
		sql += "?"
	}
	sql += ")"
	fmt.Println("sql:", sql)

	// 开始一个事务
	tx, err := master.db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	// 一次性插入多行数据
	stmt, err := tx.Prepare(sql)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	i = 0
	input := make([]interface{}, columnCount)
	flag = 1
	for _, line := range data {
		if flag == 1 {
			flag = 0
			continue
		}
		input[i] = line
		i++
		if i == columnCount {
			i = 0
			fmt.Println("input:", input)
			_, err = stmt.Exec(input...)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	fmt.Println("input success")
	// 提交事务
	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}

	return nil
}


// 获取所有数据
func (master *Master) Get(input string, reply *[]string) error {

	fmt.Println("Get called")
	rows, err := master.db.Query(input)
	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
		//*reply = "failed query"
		*reply = append(*reply, "failedinquery")
		return nil
	}
	cols, _ := rows.Columns()
	colVals := make([]interface{}, len(cols))
	colPtrs := make([]interface{}, len(cols))
	for i := range colPtrs {
		colPtrs[i] = &colVals[i]
	}

	var response []string

	// Iterate over rows
	for rows.Next() {
		err = rows.Scan(colPtrs...)
		if err != nil {
			fmt.Printf("Query failed: %v\n", err)
			//*reply = "failedscan"
			*reply = append(*reply, "failedinscan")
			return nil
		}
		var rowOutput []string
		for _, col := range colVals {

			if col == nil {
			} else {
				switch v := col.(type) {
				case []byte:
					rowOutput = append(rowOutput, fmt.Sprintf(" %s", string(v)))
				case int64:
					rowOutput = append(rowOutput, fmt.Sprintf(" %d", v))
				case string:
					rowOutput = append(rowOutput, fmt.Sprintf(" %s", v))
				default: // unknown type
					rowOutput = append(rowOutput, fmt.Sprintf(" %s", fmt.Sprint(v)))
				}
			}
		}
		response = append(response, rowOutput...)
	}
	*reply = response
	return nil
}


// 查询类
func (master *Master) Query(input string, reply *string) error {

	fmt.Println("Query called")
	rows, err := master.db.Query(input)
	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
		*reply = "failed query"
		return nil
	}
	cols, _ := rows.Columns()
	colVals := make([]interface{}, len(cols))
	colPtrs := make([]interface{}, len(cols))
	for i := range colPtrs {
		colPtrs[i] = &colVals[i]
	}

	response := ""

	// Print column headers
	header := "|"
	separator := "|"
	for _, colName := range cols {
		header += fmt.Sprintf(" %-15s |", colName) // Assuming a maximum width of 15 for each column
		separator += "-----------------|"
	}
	response += header + "\n"
	response += separator + "\n"

	// Iterate over rows
	for rows.Next() {
		err = rows.Scan(colPtrs...)
		if err != nil {
			fmt.Printf("Query failed: %v\n", err)
			*reply = "failedscan"
			return nil
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
		response += rowOutput + "\n"
	}
	*reply = response
	return nil
}