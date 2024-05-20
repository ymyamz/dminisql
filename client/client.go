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
	rpcMas, err := rpc.DialHTTP("tcp", util.MASTER_IP+":"+util.MASTER_PORT)
	if err != nil {
		fmt.Printf("CLIENT ERROR >>> connect error: %v", err)
	}
	client.rpcMaster = rpcMas	  


}
func (client *Client) Run(){


	var res string
	

	err :=client.rpcMaster.Call("Master.call_test", &res)
	if err != nil {
		fmt.Printf("CLIENT ERROR >>> master rpc call: %v",err)
	}
	fmt.Printf("RES %v",res)
	

}