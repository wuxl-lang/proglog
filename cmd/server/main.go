package main

import (
	"log"

	"github.com/wuxl-lang/proglog/internal/server"
)

func main() {
	srv := server.NewHttpServer(":11111")
	log.Fatal(srv.ListenAndServe())
}
