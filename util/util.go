package util

import (
	"fmt"
	"net/rpc"
	"time"
)

const (
	TIMEOUT_S = 1000
	TIMEOUT_M = 2000
	TIMEOUT_L = 10000

	MASTER_IP     = "172.20.0.10"
	MASTER_IP_LOCAL       = "localhost"
	MASTER_PORT   = ":4095"
	REGION_PORT   = ":5095"
	ETCD_ENDPOINT = "127.0.0.1:20079"
	//DB_FILEPATH   = "/data/app/etcd/data.db"
	//本地使用：
	DB_FILEPATH   = "data.db"
)
var Region_IPs []string
func init() {
	Region_IPs=[]string{"localhost"}
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
