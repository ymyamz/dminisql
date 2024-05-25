package region

import (
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

//master初始化使用
//返回当前有什么table
func (region *Region) TableName(input string, reply *[]string) error  {  
    //TODO  
    fmt.Println("Return TABLENAME in region")  
    rows, err := region.db.Query("SELECT name FROM sqlite_master WHERE type='table'")  
    if err != nil {  
        fmt.Printf("Query failed: %v\n", err)  
        *reply = append(*reply, "failedinquery")  
        return nil  
    }  
    defer rows.Close()  
  
    var tables []string  
    for rows.Next() {  
        var tableName string  
        err = rows.Scan(&tableName)  
        if err != nil {  
            fmt.Printf("Scan failed: %v\n", err)  
            *reply = append(*reply, "failedinscan")  
            return nil  
        }  
        tables = append(tables, tableName)  
    }  
    *reply = tables  
    return nil  
}  

//非查询类
func (region *Region)Execute(input string, reply *string) error {
	fmt.Println("Execute input:", input)
	_,err:=region.db.Exec(input)
	if err != nil {
		fmt.Printf("Execute failed: %v\n", err)
		*reply = "Execute failed"
		return nil
	}
	*reply = "Execute success"
	return nil
}

//查询类
func (region *Region) Query(input string, reply *string) error{
	//TODO
	fmt.Println("Query called")
	rows, err := region.db.Query(input)
	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
		*reply="failedinquery"
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
	*reply=response
	return nil
}