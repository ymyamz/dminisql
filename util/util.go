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

	MASTER_IP     = "172.20.0.11"
	MASTER_PORT   = ":4095"
	REGION_PORT   = ":5095"
	ETCD_ENDPOINT = "127.0.0.1:20079"
)

// rpc util
func TimeoutRPC(call *rpc.Call, ms int) (*rpc.Call, error) {
	select {
	case res := <-call.Done:
		return res, nil
	case <-time.After(time.Duration(ms) * time.Millisecond):
		return nil, fmt.Errorf("%v timeout", call.ServiceMethod)
	}
}
