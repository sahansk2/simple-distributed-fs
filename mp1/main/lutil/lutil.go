package lutil

import "fmt"

func GetVMPrefix(hostname, port string) string {
	return fmt.Sprintf("%s:%s: ", hostname, port)
}
