package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/beltran/gohive"
	"github.com/gin-gonic/gin"
)

func getQuery(c *gin.Context, param string) string {
	if c.Query(param) == "" {
		return ""
	}
	return c.Query(param)
}

func handleHiveQuery(c *gin.Context) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to read request body"})
		return
	}

	host := c.Query("host")
	if host == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Host parameter is required"})
		return
	}

	port := c.Query("port")
	if port == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Port parameter is required"})
		return
	}

	portInt, _ := strconv.Atoi(port)

	usn := c.Query("username")
	if usn == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Username parameter is required"})
		return
	}

	pass := c.Query("password")
	if pass == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Password parameter is required"})
		return
	}

	query := string(body)
	if query == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Query parameter is required"})
		return
	}

	ctx := context.Background()

	configuration := gohive.NewConnectConfiguration()
	configuration.Username = usn
	configuration.Password = pass
	connection, errConn := gohive.Connect(host, portInt, "NONE", configuration)
	if errConn != nil {
		log.Fatal(errConn)
	}
	cursor := connection.Cursor()

	cursor.Exec(ctx, query)
	if cursor.Err != nil {
		log.Fatal(cursor.Err)
		c.JSON(http.StatusBadRequest, gin.H{"error": cursor.Err.Error()})
		return
	}

	cursor.Close()
	connection.Close()

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func testHiveConnection(c *gin.Context) {
	host := getQuery(c, "host")
	if host == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Host parameter is required"})
		return
	}

	port := getQuery(c, "port")
	if port == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Port parameter is required"})
		return
	}

	usn := getQuery(c, "username")
	if usn == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Username parameter is required"})
		return
	}

	pass := getQuery(c, "password")
	if pass == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Password parameter is required"})
		return
	}

	portInt, _ := strconv.Atoi(port)

	timeout := 5 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	configuration := gohive.NewConnectConfiguration()
	configuration.Username = usn
	configuration.Password = pass
	connection, errConn := gohive.Connect(host, portInt, "NONE", configuration)
	if errConn != nil {
		log.Println("Error connecting to Hive:", errConn)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to connect to Hive", "details": errConn.Error()})
		return
	}

	cursor := connection.Cursor()
	// cursor.Exec(ctx, "SELECT * FROM persons LIMIT 10")
	cursor.Exec(ctx, "SHOW DATABASES")

	if cursor.Err != nil {
		log.Println("Error executing Hive query:", cursor.Err)
		c.JSON(http.StatusBadRequest, gin.H{"error": cursor.Err.Error()})
		return
	}

	log.Println(cursor.FetchLogs())

	// fieldSchemas:[FieldSchema(name:persons.id, type:int, comment:null), FieldSchema(name:persons.last_name, type:varchar(255), comment:null), FieldSchema(name:persons.first_name, type:varchar(255), comment:null), FieldSchema(name:persons.address, type:varchar(255), comment:null), FieldSchema(name:persons.city, type:varchar(255), comment:null)]
	// get the field schemas

	s := ""
	for cursor.HasMore(ctx) {
		cursor.FetchOne(ctx, &s)
		if cursor.Err != nil {
			log.Fatal(cursor.Err)
		}
		log.Println(s)
	}

	// for cursor.HasMore(ctx) {
	// 	cursor.FetchOne(ctx, func(row []string) {
	// 		if cursor.Err != nil {
	// 			log.Fatal(cursor.Err)
	// 		}

	// 		log.Println(row)
	// 	})
	// }

	cursor.Close()
	connection.Close()

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
