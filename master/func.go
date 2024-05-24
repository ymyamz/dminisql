package master

import (
	"fmt"
)

func (m *Master) CallTest(arg string, reply *string) error {  
	fmt.Println("CALL master SUCCESS")  
	*reply = "hello " + arg  
	return nil  
}  