package raft

import "log"

// Debugging
const Debug = false

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

func CopyMap(m map[int]int) map[int]int {
	n := make(map[int]int)
	for k, v := range m {
		n[k] = v
	}
	return n
}

func CopySlice(slice []Entry) []Entry {
	n := make([]Entry, len(slice))
	copy(n, slice)
	return n
}
