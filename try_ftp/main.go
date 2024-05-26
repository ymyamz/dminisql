package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func downloadFileFromRemote(remoteIP string, remoteFilePath string, localFilePath string) error {  
	conn, err := net.Dial("tcp", remoteIP+":80")  
	if err != nil {  
		return err  
	}  
	defer conn.Close()  
  
	// 发送请求  
	request := "GET " + remoteFilePath + " HTTP/1.0\r\n\r\n"  
	_, err = conn.Write([]byte(request))  
	if err != nil {  
		return err  
	}  
  
	// 创建本地文件  
	out, err := os.Create(localFilePath)  
	if err != nil {  
		return err  
	}  
	defer out.Close()  
  
	// 将远程文件内容拷贝到本地文件  
	_, err = io.Copy(out, conn)  
	if err != nil {  
		return err  
	}  
  
	fmt.Printf("File downloaded: %s\n", localFilePath)  
	return nil  
}  
  
func main() {  
	remoteIP := "172.20.0.12"
	remoteFilePath := "/data/gopath/dminisql/data.db"  
	localFilePath := "/data/gopath/dminisql/data.db"  
	err := downloadFileFromRemote(remoteIP, remoteFilePath, localFilePath)  
	if err != nil {  
		fmt.Println(err)  
	}  
}  