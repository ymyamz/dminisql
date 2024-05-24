package client

import (
	"distribute-sql/util"
	"fmt"
	"net/rpc"
)

type Client struct {
	rpcMaster    *rpc.Client
	rpcRegionMap map[string]*rpc.Client // [ip]rpc
}

func (client *Client) Init(){
	fmt.Println("client init and link to master ",util.MASTER_IP)
	rpcMas, err := rpc.DialHTTP("tcp", "localhost"+util.MASTER_PORT)
	if err != nil {
		fmt.Printf("CLIENT ERROR >>> connect error: %v", err)
	}
	client.rpcMaster = rpcMas	  


}
func (client *Client) Run(){


	var res string
	

	call, err := util.TimeoutRPC(client.rpcMaster.Go("Master.CallTest", "test", &res, nil), util.TIMEOUT_M)
	if err != nil {
		fmt.Println("SYSTEM HINT>>> timeout, master down!")
	}
	if call.Error != nil {
		fmt.Println("RESULT>>> failed",call.Error)
	} else {
		fmt.Println("RESULT>>> res: ",res)
	}


}