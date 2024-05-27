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

// 给server region分配backup，由server给backup下载data.db
func (region *Region) AssignBackup(ip string, dummyReply *bool) error {
	fmt.Printf("Region.AssignBackup called: backup ip: %v", ip)
	client, err := rpc.DialHTTP("tcp", "localhost:"+ip)
	if err != nil {
		log.Printf(ip, " rpc.DialHTTP err: %v")
	} else {
		region.backupClient = client
		region.backupIP = ip
		//TODO 通知backup下载data.db,注意先删除backup本地可能存在的data.db 应该直接覆盖了就相当于删了

		util.TransferFile(region.serverIP, ip+util.FILE_PORT, "./data/"+region.hostIP+".db")
		backupClient, err := rpc.DialHTTP("tcp", region.backupIP+util.REGION_PORT)
		var res []string
		args := SaveFileArgs{
			FileName:     "./data/" + region.hostIP + ".db",
			SaveFileName: "",
		}
		backupClient.Go("Region.SaveFileFromFTP", args, &res, nil)
		if err != nil {
			fmt.Println("Error:", err)
		}
	}
	return err
}

//写一个转存函数，将region的data.db中的数据转存到best ip pair中？？
//TODO

// Region 从ftp服务器上下载文件到本地
// 要么直接指定savefileName 要么就是regionIP+尾缀
func (region *Region) SaveFileFromFTP(args SaveFileArgs, reply *string) error {
	fileName := args.FileName
	savefileName := args.SaveFileName
	// connect FTP Server
	conn, err := ftp.Dial(region.serverIP + util.FILE_PORT)
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
	fmt.Println(localFilePath)
	fmt.Println("hhhhhh")
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
