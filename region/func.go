package region

import (
	"distribute-sql/util"
	"fmt"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"

	"github.com/jlaffaye/ftp"
	_ "github.com/mattn/go-sqlite3"
)

// master初始化使用
// 返回当前有什么table
func (region *Region) TableName(input string, reply *[]string) error {
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

// 返回当前有什么index
// SELECT * FROM sqlite_master
// WHERE type='index' AND tbl_name='your_table_name';
func (region *Region) Index(input string, reply *[]string) error {
	fmt.Println("Return Index in region,table:", input)
	rows, err := region.db.Query("SELECT name FROM sqlite_master WHERE type='index'AND tbl_name= ? ", input)
	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
		*reply = append(*reply, "failedinquery")
		//*reply = "failedinquery"
		return nil
	}
	defer rows.Close()

	var indexes []string
	var index_name string
	for rows.Next() {
		err = rows.Scan(&index_name)
		if err != nil {
			fmt.Printf("Scan failed: %v\n", err)
			*reply = append(*reply, "failedinscan")
			//*reply = "failedinscan"
			return nil
		}
		indexes = append(indexes, index_name)
	}
	*reply = indexes
	return nil
}

// 批量插入
func (region *Region) Insert(data []string, reply *string) error {
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
	err := region.Get(GetInput, &res)

	//rows, err := region.db.Query("PRAGMA table_info(" + table + ")")
	//if err != nil {
	//	fmt.Println("查询失败：", err)
	//	return nil
	//}
	//defer rows.Close()

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
	//for rows.Next() {
	//	var name string
	//	if err := rows.Scan(&name); err != nil {
	//		fmt.Println("扫描失败：", err)
	//		return nil
	//	}
	//	columnCount++
	//	if flag == 1 {
	//		flag = 0
	//	} else {
	//		sql += ","
	//	}
	//	sql += name
	//}

	sql += ")VALUES ("
	for i := 0; i < columnCount; i++ {
		if i != 0 {
			sql += ","
		}
		sql += "?"
	}
	sql += ")"
	fmt.Println("sql:", sql)

	// 检查是否有错误
	//if err := rows.Err(); err != nil {
	//	fmt.Println("rows 扫描失败：", err)
	//	return nil
	//}

	// 开始一个事务
	tx, err := region.db.Begin()
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

	// 提交事务
	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}

	return nil
}

// 非查询类
func (region *Region) Execute(input string, reply *string) error {
	fmt.Println("Execute input:", input)
	_, err := region.db.Exec(input)
	if err != nil {
		fmt.Printf("Execute failed: %v\n", err)
		*reply = "Execute failed"
		return nil
	}
	*reply = "Execute success"
	fmt.Println("Execute success")

	if region.backupIP != "" {
		rpcBackupRegion, err := rpc.DialHTTP("tcp", "localhost:"+region.backupIP)
		if err != nil {
			log.Printf("fail to connect to backup %v", region.backupIP)
			return nil
		}
		// backup's Region.Process must return nil error
		_, err = util.TimeoutRPC(rpcBackupRegion.Go("Region.Execute", &input, &reply, nil), util.TIMEOUT_S)
		if err != nil {
			log.Printf("%v's Region.Process timeout", region.backupIP)
			return nil
		}
	}
	return nil
}

// 查询类
func (region *Region) Query(input string, reply *string) error {

	fmt.Println("Query called")
	rows, err := region.db.Query(input)
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

// //获取建表sql
// func (region *Region) Create(input string, reply *string) error {
//
//		fmt.Println("Create called")
//		rows, err := region.db.Query(input)
//		if err != nil {
//			fmt.Printf("Query failed: %v\n", err)
//			*reply = "failed query"
//			return nil
//		}
//		*reply=rows;
//		return nil
//	}
//
// 获取所有数据
func (region *Region) Get(input string, reply *[]string) error {

	fmt.Println("Get called")
	rows, err := region.db.Query(input)
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

// 给server region分配backup，由server给backup下载data.db
func (region *Region) AssignBackup(ip string, dummyReply *bool) error {
	fmt.Printf("Region.AssignBackup called,backup ip: %v", ip)
	client, err := rpc.DialHTTP("tcp", "localhost:"+ip)
	if err != nil {
		fmt.Printf(ip, " rpc.DialHTTP err: %v")
	} else {

		region.backupClient = client
		region.backupIP = ip
		// 通知backup下载data.db,直接覆盖了本地data.db
		//在本地测试中，IP都改成127.0.0.1
		//util.TransferFile(region.hostIP+util.FILE_PORT, ip+util.FILE_PORT, "./data/"+region.hostIP+".db")
		util.TransferFile("127.0.0.1", "127.0.0.1"+util.FILE_PORT, "./data/"+region.hostIP+".db")

		backupClient, err := rpc.DialHTTP("tcp", "localhost:"+region.backupIP)
		if err != nil {
			fmt.Println("Error:", err)
		}

		var res []string
		args := SaveFileArgs{
			FileName:     region.hostIP + ".db",
			SaveFileName: "",
		}
		// 通知backup从ftp获取data.db
		backupClient.Go("Region.SaveFileFromFTP", args, &res, nil)

	}
	return err
}

// 写一个转存函数，将region的data.db中的数据转存到best ip pair中？？
// 在reply中写转存到哪个ip中了
func (region *Region) TransferToBestPair(tableip map[string]string, reply *string) error {
	var masterIp string
	if util.Local {
		masterIp = util.MASTER_IP_LOCAL
	} else {
		masterIp = util.MASTER_IP
	}
	MasterClient, err := rpc.DialHTTP("tcp", "localhost"+masterIp)
	if err != nil {
		fmt.Println("Error:", err)
		return err
	}
	var bestIp string
	err = MasterClient.Call("Master.FindBest", "", &bestIp)
	if err != nil {
		fmt.Println("Error:", err)
		return err
	}
	fmt.Println("Best server is: " + bestIp)
	*reply = bestIp
	// var tableip map[string]string
	// tableip = make(map[string]string)
	// _, err = util.TimeoutRPC(MasterClient.Go("Master.AllTableIp", "", &tableip, nil), util.TIMEOUT_S)
	// if err != nil {
	// 	fmt.Println("Error:", err)
	// 	return err
	// }

	// Find all the tables in this region server
	var tables []string
	var targetIP string
	if region.serverIP != "" {
		// 说明是backup
		targetIP = region.serverIP
		region.serverIP = ""
	} else {
		targetIP = region.hostIP
	}
	fmt.Println(targetIP)
	for table, ip := range tableip {
		if ip == targetIP {
			tables = append(tables, table)
		}
	}
	fmt.Println(tables)
	fmt.Println(tableip)

	for i := 0; i < len(tables); i++ {
		args := util.MoveStruct{
			Table:  tables[i],
			Region: bestIp,
		}
		fmt.Println(args)
		var tmp string
		_, err = util.TimeoutRPC(MasterClient.Go("Master.Move", args, &tmp, nil), util.TIMEOUT_S)
		if err != nil {
			fmt.Println("Error:", err)
			return err
		}
	}
	return nil
}

// Region 从ftp服务器上下载文件到本地
// 要么直接指定savefileName 要么就是regionIP+尾缀
func (region *Region) SaveFileFromFTP(args SaveFileArgs, reply *string) error {

	fileName := args.FileName
	savefileName := args.SaveFileName
	fmt.Println("SaveFileFromFTP called, save from ", args.FileName)
	// connect FTP Server
	conn, err := ftp.Dial("localhost" + util.FILE_PORT)
	if err != nil {
		return fmt.Errorf("error connecting to FTP server: %v", err)
	}
	defer conn.Quit()

	// 使用匿名登录
	err = conn.Login("anonymous", "anonymous")
	if err != nil {
		return fmt.Errorf("error logging in to FTP server: %v", err)
	}

	// 获取FTP根目录的文件列表
	files, err := conn.List("/")
	if err != nil {
		return fmt.Errorf("error listing files on FTP server: %v", err)
	}

	// 找到需要下载的文件
	var fileToDownload *ftp.Entry
	for _, file := range files {
		if file.Name == fileName {
			fileToDownload = file
			break
		}
	}

	if fileToDownload == nil {
		return fmt.Errorf("file %s not found on FTP server", fileName)
	}

	// 创建本地文件
	var localFilePath string
	if savefileName == "" {
		localFilePath = filepath.Join("./data/", region.hostIP, util.GetPostfix(fileName))
	} else {
		localFilePath = filepath.Join("./", savefileName)
	}
	localFile, err := os.Create(localFilePath)
	if err != nil {
		return fmt.Errorf("error creating local file: %v", err)
	}
	defer localFile.Close()

	// 从FTP服务器下载文件
	r, err := conn.Retr(fileToDownload.Name)
	if err != nil {
		return fmt.Errorf("error downloading file from FTP server: %v", err)
	}
	defer r.Close()

	// 将文件内容复制到本地文件
	_, err = io.Copy(localFile, r)
	if err != nil {
		return fmt.Errorf("error copying file contents: %v", err)
	}

	fmt.Println("File downloaded successfully")
	return nil
}

// 把region本地的data.db删了
func (region *Region) ClearAllData(input string, reply *string) error {
	os.Remove("./data/" + region.hostIP + ".db")
	*reply = "success"
	return nil
}
