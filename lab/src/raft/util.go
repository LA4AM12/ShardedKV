package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		//t := time.Now()
		//a = append([]interface{}{t.UnixMilli()}, a...)
		//log.Printf("[%6d] "+format, a...)
		log.Printf(format, a...)
	}
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a < b {
		return b
	}
	return a
}
