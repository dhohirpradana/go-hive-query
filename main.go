package main

import (
	"log"

	"github.com/gin-gonic/gin"
)

func main() {
	router := gin.Default()

	router.GET("/test-hive", testHiveConnection)

	router.POST("/hive-query", handleHiveQuery)

	addr := "localhost:8080"
	log.Printf("Starting server on %s...\n", addr)
	if err := router.Run(addr); err != nil {
		log.Fatal("Error starting server:", err)
	}
}
