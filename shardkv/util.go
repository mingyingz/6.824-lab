package shardkv

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"time"
)

// Debugging
const Debug = false

func monitor() {
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()
	ticker := time.NewTicker(1 * time.Second) // 每秒钟调用一次
	defer ticker.Stop()

	for range ticker.C {
		num := runtime.NumGoroutine()
		fmt.Printf("当前协程数量：%d\n", num)
	}
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Delete(slice []int, si int) []int {
	for j := 0; j < len(slice); j++ {
		if slice[j] == si {
			slice = append(slice[:j], slice[j+1:]...)
			break
		}
	}
	return slice
}

func Contains(slice []int, si int) bool {
	for j := 0; j < len(slice); j++ {
		if slice[j] == si {
			return true
		}
	}
	return false
}

func CopyIntMap(m map[int]int) map[int]int {
	n := make(map[int]int)
	for k, v := range m {
		n[k] = v
	}
	return n
}

func CopyStringMap(m map[string]string) map[string]string {
	n := make(map[string]string)
	for k, v := range m {
		n[k] = v
	}
	return n
}

func CopySlice(slice []int) []int {
	n := make([]int, len(slice))
	copy(n, slice)
	return n
}
