package util

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/rpc"
	"os"
	"time"

	"strings"

	"github.com/jlaffaye/ftp"
)

const (
	Local     = true
	TIMEOUT_S = 1000
	TIMEOUT_M = 2000
	TIMEOUT_L = 10000

	MASTER_IP       = "172.20.0.10"
	MASTER_IP_LOCAL = ":8000"
	MASTER_PORT     = ":4095"
	REGION_PORT     = ":5095"
	ETCD_ENDPOINT   = "127.0.0.1:2379"

	//DB_FILEPATH   = "/data/app/etcd/data.db"
	//本地使用：
	DB_FILEPATH        = "data.db"
	BUSY_THRESHOLD     = 100
	REMOTE_WORKING_DIR = "/data/gopath/dminisql/data"
	LOCAL_WORKING_DIR  = "data"
	FILE_PORT          = ":21"
)

type MoveStruct struct {
	Table  string
	Region string
}

var Region_IPs []string
var Region_IPs_LOCAL []string

func init() {
	Region_IPs_LOCAL = []string{"localhost"}
	Region_IPs = []string{"172.20.0.11", "172.20.0.12"}
}

// rpc util
func TimeoutRPC(call *rpc.Call, ms int) (*rpc.Call, error) {
	select {
	case res := <-call.Done:
		return res, nil
	case <-time.After(time.Duration(ms) * time.Millisecond):
		return nil, fmt.Errorf("%v timeout", call.ServiceMethod)
	}
}

func FindElement(pSlice *[]string, str string) int {
	for i, v := range *pSlice {
		if v == str {
			return i
		}
	}
	return -1
}

func DeleteFromSlice(pSlice *[]string, str string) bool {
	// Dont use append, for the sake of efficiency
	index := FindElement(pSlice, str)
	if index == -1 {
		return false
	}
	(*pSlice)[index] = (*pSlice)[len(*pSlice)-1]
	*pSlice = (*pSlice)[:len(*pSlice)-1]
	return true
}

func DeleteValueFromMap(mp *map[string]string, value string) error {
	var keysToDelete []string
	for key, v := range *mp {
		if v == value {
			keysToDelete = append(keysToDelete, key)
		}
	}
	if len(keysToDelete) == 0 {
		return errors.New("value not found in map")
	}
	for _, table := range keysToDelete {
		delete(*mp, table)
	}
	return nil
}

func AddToSlice(ptr *[]string, newString string) {
	// 如果指针为nil，创建一个新的切片
	if *ptr == nil {
		*ptr = make([]string, 0)
	}

	// 添加新的字符串到切片中
	*ptr = append(*ptr, newString)
}

func CleanDir(localDir string) {
	dir, err := ioutil.ReadDir(localDir)
	if err != nil {
		fmt.Println("Can't obtain files in dir")
	}
	for _, d := range dir {
		os.RemoveAll(localDir + d.Name())
	}
}

func AddToSliceIndex(ptr *[]string, newString string) {
	// 如果指针为nil，创建一个新的切片
	if ptr == nil {
		ptr = new([]string) // 分配内存给切片
	}

	// 添加新的字符串到切片中
	*ptr = append(*ptr, newString)
}

func GetPostfix(filename string) string {
	index := strings.LastIndex(filename, ".")
	var extension string
	if index != -1 {
		// 提取尾部
		extension = filename[index:]
	}
	return extension
}

func TransferFile(sourceIP string, targetIP string, fileName string) error {
	// Connect to the FTP server on the target machine
	conn, err := ftp.Dial(targetIP)
	if err != nil {
		return fmt.Errorf("error connecting to FTP server: %v", err)
	}
	defer conn.Quit()

	// Log in to the FTP server (assuming anonymous login for simplicity)
	err = conn.Login("anonymous", "anonymous")
	if err != nil {
		return fmt.Errorf("error logging in to FTP server: %v", err)
	}

	// Open the source file
	file, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("error opening source file: %v", err)
	}
	defer file.Close()

	// Transfer the file
	err = conn.Stor(fileName, file)
	if err != nil {
		return fmt.Errorf("error transferring file: %v", err)
	}

	fmt.Println("File sent successfully")
	return nil
}
