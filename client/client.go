package client

import (
	"fmt"
	"net/rpc"
)

type Client struct {
	ipCache      map[string]string
	rpcMaster    *rpc.Client
	rpcRegionMap map[string]*rpc.Client // [ip]rpc
}

func (client *Client) Init(masterip string){
	fmt.Println("client init\n"+masterip)  

}