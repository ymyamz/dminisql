package client

import (
	"distribute-sql/util"
	"fmt"
	"net/rpc"
)

func (client *Client) connect_to_master(call_func string, input string) string {
	var res string
	//具体table名称解析等在master中进行
	call, err := util.TimeoutRPC(client.rpcMaster.Go(call_func, input, &res, nil), util.TIMEOUT_M)
	if err != nil {
		fmt.Println("SYSTEM HINT>>> timeout, master down!")
	}
	//不输出
	if call_func=="Master.SaveToFile"{
		return ""
	}
	if call.Error != nil {
		fmt.Println("RESULT>>> failed ", call.Error)
	} else {
		if call_func != "Master.GetTableIP" {
			fmt.Println("RESULT>>>\n" + res)
		}
	}
	
	return res
}
func (client *Client) connect_to_region(region_ip string, call_func string, input string) string {
	rpcRegion, err := rpc.DialHTTP("tcp", "localhost:"+region_ip)
	if err != nil {
		fmt.Printf("CLIENT ERROR >>> connect error: %v", err)
	}
	var res string
	//具体table名称解析等在master中进行
	call, err := util.TimeoutRPC(rpcRegion.Go(call_func, input, &res, nil), util.TIMEOUT_M)
	if err != nil {
		fmt.Println("SYSTEM HINT>>> timeout, master down!")
	}
	if call.Error != nil {
		fmt.Println("RESULT>>> failed ", call.Error)
	} else {
		fmt.Println("RESULT>>>\n" + res)
	}
	return res
}
func (client *Client) connect_to_region_test(region_ip string, call_func string, input string) {
	rpcRegion, err := rpc.DialHTTP("tcp", "localhost:"+region_ip)
	if err != nil {
		fmt.Printf("CLIENT ERROR >>> connect error: %v", err)
	}
	var res []string
	//具体table名称解析等在master中进行
	call, err := util.TimeoutRPC(rpcRegion.Go(call_func, input, &res, nil), util.TIMEOUT_M)
	if err != nil {
		fmt.Println("SYSTEM HINT>>> timeout, master down!")
	}
	if call.Error != nil {
		fmt.Println("RESULT>>> failed ", call.Error)
	} else {
		if len(res) > 0 {
			for _, line := range res {
				fmt.Println("RESULT>>>", line)
			}
		} else {
			fmt.Println("RESULT>>> no data returned")
		}
	}
}
