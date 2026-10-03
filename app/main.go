package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	ln, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092:", err)
		os.Exit(1)
	}
	defer ln.Close()

	conn, err := ln.Accept()
	if err != nil {
		fmt.Println("Error accepting connection:", err)
		os.Exit(1)
	}

	if err := handleConn(conn); err != nil {
		fmt.Println("Error handling connection:", err)
	}
}

func handleConn(conn net.Conn) error {
	defer conn.Close()

	msg, err := readFrame(conn)
	if err != nil {
		return fmt.Errorf("read frame: %w", err)
	}

	req, err := parseRequest(msg)
	if err != nil {
		return fmt.Errorf("parse request: %w", err)
	}

	header := ResponseHeader{correlationID: req.correlationID}
	if req.apiVersion < 0 || req.apiVersion > 4 {
		header.errorCode = 35
	}

	if err := writeResponse(conn, header); err != nil {
		return fmt.Errorf("write response: %w", err)
	}
	return nil
}
