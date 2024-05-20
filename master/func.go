package master

import (
	"fmt"
)

func (master *Master)call_test(res *string){
	fmt.Println("CALL master SUCCESS\n")
	*res="abcdefg"
}