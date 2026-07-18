package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type encoderConfig struct {
	Action  string `json:"action"`
	Bitrate string `json:"bitrate"`
}

type encoderStatus struct {
	Status string `json:"status"`
	Msg    string `json:"msg,omitempty"`
}

type authorization struct {
	UserID         string `json:"user_id"`
	StreamKey      string `json:"stream_key"`
	SourcePassword string `json:"source_password"`
	Username       string `json:"username"`
	Bitrate        string `json:"bitrate"`
	Plan           string `json:"plan"`
}

var (
	apiBase = strings.TrimRight(envOr("API_BASE", "https://radioinonestop.com"), "/")
	httpAPI = &http.Client{Timeout: 15 * time.Second}
	owners  = struct {
		sync.Mutex
		active map[string]bool
	}{active: make(map[string]bool)}
	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			origin := strings.TrimSpace(r.Header.Get("Origin"))
			if origin == "" {
				return false
			}
			u, err := url.Parse(origin)
			if err != nil {
				return false
			}
			host := strings.ToLower(u.Hostname())
			return host == "radioinonestop.com" || strings.HasSuffix(host, ".radioinonestop.com") || host == "localhost" || host == "127.0.0.1"
		},
	}
)

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func acquireOwner(userID string) bool {
	owners.Lock()
	defer owners.Unlock()
	if owners.active[userID] {
		return false
	}
	owners.active[userID] = true
	return true
}

func releaseOwner(userID string) {
	owners.Lock()
	delete(owners.active, userID)
	owners.Unlock()
}

func authorize(token, bitrate string) (authorization, error) {
	body, _ := json.Marshal(map[string]string{"bitrate": bitrate})
	req, err := http.NewRequest(http.MethodPost, apiBase+"/api/user/encoder-authorize", bytes.NewReader(body))
	if err != nil {
		return authorization{}, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpAPI.Do(req)
	if err != nil {
		return authorization{}, fmt.Errorf("authorization service unavailable: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return authorization{}, fmt.Errorf("authorization failed: %s", strings.TrimSpace(string(message)))
	}
	var auth authorization
	if err := json.NewDecoder(resp.Body).Decode(&auth); err != nil {
		return authorization{}, fmt.Errorf("invalid authorization response: %w", err)
	}
	if auth.UserID == "" || auth.StreamKey == "" || auth.SourcePassword == "" || auth.Bitrate == "" {
		return authorization{}, fmt.Errorf("authorization response is incomplete")
	}
	if auth.Username == "" {
		auth.Username = "source"
	}
	return auth, nil
}

func updateSession(token string, live bool) error {
	body, _ := json.Marshal(map[string]bool{"live": live})
	req, err := http.NewRequest(http.MethodPost, apiBase+"/api/user/encoder-session", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpAPI.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("session update failed (%d): %s", resp.StatusCode, strings.TrimSpace(string(message)))
	}
	return nil
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"status":"ok","service":"encoder-worker"}`)
}

func handleEncode(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimSpace(r.URL.Query().Get("token"))
	if token == "" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	var writeMu sync.Mutex
	writeJSON := func(status, message string) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return conn.WriteJSON(encoderStatus{Status: status, Msg: message})
	}
	writeControl := func(messageType int, data []byte, deadline time.Time) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return conn.WriteControl(messageType, data, deadline)
	}

	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	messageType, raw, err := conn.ReadMessage()
	conn.SetReadDeadline(time.Time{})
	if err != nil || messageType != websocket.TextMessage {
		_ = writeJSON("error", "expected encoder configuration")
		return
	}
	var cfg encoderConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		_ = writeJSON("error", "invalid encoder configuration")
		return
	}
	if cfg.Action == "broadcast" {
		_ = writeJSON("error", "Station Hub remains on the main server; select Icecast mode")
		return
	}
	if cfg.Action != "" && cfg.Action != "start" {
		_ = writeJSON("error", "unsupported encoder action")
		return
	}
	if cfg.Bitrate == "" {
		cfg.Bitrate = "96k"
	}

	auth, err := authorize(token, cfg.Bitrate)
	if err != nil {
		_ = writeJSON("error", err.Error())
		return
	}
	if !acquireOwner(auth.UserID) {
		_ = writeJSON("error", "An Icecast encoder is already active in another tab or device")
		return
	}
	defer releaseOwner(auth.UserID)

	icecastHost := envOr("ICECAST_HOST", "icecast")
	icecastPort := envOr("ICECAST_PORT", "8000")
	icecastURL := &url.URL{
		Scheme: "icecast",
		User:   url.UserPassword(auth.Username, auth.SourcePassword),
		Host:   icecastHost + ":" + icecastPort,
		Path:   "/" + strings.TrimPrefix(auth.StreamKey, "/"),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-loglevel", "error",
		"-fflags", "nobuffer",
		"-f", "webm",
		"-i", "pipe:0",
		"-vn",
		"-c:a", "libmp3lame",
		"-b:a", auth.Bitrate,
		"-ar", "44100",
		"-ac", "2",
		"-content_type", "audio/mpeg",
		"-flush_packets", "1",
		"-f", "mp3",
		icecastURL.String(),
	)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		_ = writeJSON("error", "cannot open FFmpeg input")
		return
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		_ = writeJSON("error", "cannot start FFmpeg")
		return
	}
	ffmpegDone := make(chan error, 1)
	go func() { ffmpegDone <- cmd.Wait() }()

	mount := "/" + strings.TrimPrefix(auth.StreamKey, "/")
	log.Printf("[encoder/%s] started mount=%s bitrate=%s plan=%s", auth.UserID, mount, auth.Bitrate, auth.Plan)
	_ = writeJSON("live", fmt.Sprintf("Encoder connected → stream.radioinonestop.com:8000%s", mount))

	liveMarked := false
	startedAt := time.Now()
	audioBytes := int64(0)
	defer func() {
		cancel()
		_ = stdin.Close()
		if liveMarked {
			if err := updateSession(token, false); err != nil {
				log.Printf("[encoder/%s] offline update: %v", auth.UserID, err)
			}
		}
		log.Printf("[encoder/%s] stopped after=%s bytes=%d", auth.UserID, time.Since(startedAt).Round(time.Millisecond), audioBytes)
	}()

	pingDone := make(chan struct{})
	defer close(pingDone)
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := writeControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second)); err != nil {
					return
				}
			case <-pingDone:
				return
			}
		}
	}()

	for {
		select {
		case ffErr := <-ffmpegDone:
			message := strings.TrimSpace(stderr.String())
			if message == "" && ffErr != nil {
				message = ffErr.Error()
			}
			if message == "" {
				message = "FFmpeg stopped"
			}
			_ = writeJSON("error", message)
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if messageType == websocket.TextMessage {
			var control encoderConfig
			if json.Unmarshal(data, &control) == nil && control.Action == "stop" {
				_ = writeJSON("stopped", "Broadcast stopped")
				return
			}
			continue
		}
		if messageType != websocket.BinaryMessage || len(data) == 0 {
			continue
		}
		if _, err := stdin.Write(data); err != nil {
			_ = writeJSON("error", "FFmpeg stopped accepting audio")
			return
		}
		audioBytes += int64(len(data))
		if !liveMarked {
			if err := updateSession(token, true); err != nil {
				_ = writeJSON("error", "could not publish station live state")
				return
			}
			liveMarked = true
		}
	}
}

func main() {
	port := envOr("PORT", "8080")
	mux := http.NewServeMux()
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/ws/encode", handleEncode)
	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       90 * time.Second,
	}
	log.Printf("[encoder-worker] listening on :%s api=%s", port, apiBase)
	log.Fatal(server.ListenAndServe())
}
