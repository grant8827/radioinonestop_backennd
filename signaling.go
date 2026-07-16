package main

import (
	"log"
	"net/http"

	"github.com/gorilla/websocket"
)

var studioUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return isAllowedOrigin(r.Header.Get("Origin")) },
}

func HandleStudioWebsocket(w http.ResponseWriter, r *http.Request) {
	conn, err := studioUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}
	defer conn.Close()

	log.Println("Studio WebSocket client connected")

	for {
		// Read incoming studio metadata (Updated marquee text, armed channels list, active logos)
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Println("Connection close or error:", err)
			break
		}

		// Parse message JSON
		// Broadcast configuration updates to downstream transcode threads dynamically
		log.Printf("Received parameter change request: %s", string(message))
	}
}
