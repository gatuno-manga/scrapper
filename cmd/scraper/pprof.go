//go:build pprof

package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
)

func init() {
	go func() {
		log.Println("Starting pprof server on :6060 (DEBUG MODE)")
		if err := http.ListenAndServe(":6060", nil); err != nil {
			log.Printf("pprof server failed: %v", err)
		}
	}()
}
