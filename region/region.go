package region

import (
	"database/sql"
	"distribute-sql/util"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"time"

	_ "github.com/mattn/go-sqlite3"
)
type Region struct {
	db *sql.DB
}
func (region *Region) Init() {
	//连接数据库文件
	var err error
	region.db, err = sql.Open("sqlite3", util.DB_FILEPATH)
	if err != nil {
		fmt.Printf("Database creation failed: %v\n", err)
		return
	}
	defer region.db.Close()
	fmt.Printf("Database connection successful\n")

	//注册RPC服务
	// 注册rpc函数
	rpc.Register(region)
	rpc.HandleHTTP()
	// 启动server
	l, err := net.Listen("tcp",  util.REGION_PORT)
	if err != nil {
		fmt.Println("Accept error:", err)
	}
	go http.Serve(l, nil) // 进入的链接让rpc来执行
	for {
		time.Sleep(10 * time.Second)
	}


}

