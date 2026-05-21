// Radio In One Stop — Media Server
//
// Architecture:
//   OBS/vMix  ──RTMP──►  rtmpServer (port 1935)
//                              │
//                         streamManager
//                         spawns FFmpeg per stream
//                              │
//                         /tmp/hls/<streamKey>/
//                         index.m3u8 + *.ts/.mp4
//                              │
//   Browser  ◄──HLS────  /hls/<streamKey>/index.m3u8
//
// Concurrency target: 500+ concurrent listeners.
// HLS segments are served as static files by Go's
// net/http ofile server — each request is handled in
// its own goroutine, zero bottleneck at the Go layer.
// FFmpeg does all the heavy lifting.

package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/mail"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	"github.com/lib/pq"
	"github.com/livekit/protocol/auth"
	"golang.org/x/crypto/bcrypt"
)

// ─── Configuration ────────────────────────────────────────────────────────────

// StationConfig holds runtime-editable station metadata.
type StationConfig struct {
	StationName string `json:"stationName"`
	// HLS base URL returned to the frontend so it always knows
	// where to point hls.js. Defaults to http://<host>/hls.
	HLSBaseURL string `json:"hlsBaseURL"`
}

// HLSDir is the root directory where FFmpeg writes HLS segments.
const HLSDir = "/tmp/hls"

// RTMPPort is the TCP port that accepts RTMP connections.
const RTMPPort = "1935"

// ─── Stream state ─────────────────────────────────────────────────────────────

// streamState tracks a single live RTMP→HLS transcode job.
type streamState struct {
	key          string // stream key, e.g. "radio" or "video"
	cancel       context.CancelFunc
	startedAt    time.Time
	live         atomic.Bool
	destinations []string // RTMP forwarding destinations
}

// streamManager owns all active transcoding sessions.
type streamManager struct {
	mu      sync.RWMutex
	streams map[string]*streamState
}

func newStreamManager() *streamManager {
	return &streamManager{streams: make(map[string]*streamState)}
}

// start launches an FFmpeg transcode for the given stream key.
// If a session already exists for that key it is stopped first.
func (sm *streamManager) start(key string, rtmpConn net.Conn, destinations []string) {
	sm.mu.Lock()
	if old, ok := sm.streams[key]; ok {
		old.cancel()
	}

	ctx, cancel := context.WithCancel(context.Background())
	ss := &streamState{key: key, cancel: cancel, startedAt: time.Now(), destinations: destinations}
	ss.live.Store(true)
	sm.streams[key] = ss
	sm.mu.Unlock()

	outDir := filepath.Join(HLSDir, key)
	if err := os.MkdirAll(outDir, 0755); err != nil {
		log.Printf("[stream/%s] mkdir error: %v", key, err)
		cancel()
		return
	}

	go sm.transcode(ctx, key, outDir, rtmpConn, destinations)
}

// transcode runs FFmpeg with LL-HLS settings.
//
// FFmpeg reads from the RTMP connection via stdin (piped) rather than
// opening a second TCP connection — this avoids the 2-hop latency of a
// full RTMP re-ingest and keeps the pipeline inside one process tree.
//
// LL-HLS settings:
//
//	-hls_time 1          → 1-second target segment duration
//	-hls_list_size 6     → keep 6 segments in the manifest (≈6 s window)
//	-hls_flags delete_segments+independent_segments+split_by_time+program_date_time
//	-hls_segment_type fmp4  → fragmented MP4 partial segments
//	-hls_fmp4_init_filename init.mp4
//	-hls_flags +append_list → running playlist for low-latency
//
// Audio-only path (radio): no video track, aac 192 k.
// Video path: h264 baseline, aac 192 k.
func (sm *streamManager) transcode(ctx context.Context, key, outDir string, rtmpConn net.Conn, destinations []string) {
	defer func() {
		rtmpConn.Close()
		sm.mu.Lock()
		if ss, ok := sm.streams[key]; ok && ss.key == key {
			ss.live.Store(false)
		}
		sm.mu.Unlock()
		log.Printf("[stream/%s] transcode exited", key)
	}()

	playlist := filepath.Join(outDir, "index.m3u8")
	segPattern := filepath.Join(outDir, "seg%05d.mp4")

	// Decide whether this is audio-only (stream key contains "radio")
	isRadio := strings.Contains(strings.ToLower(key), "radio")

	var args []string
	if isRadio {
		args = []string{
			"-loglevel", "warning",
			"-re",          // read at native frame rate (live simulation)
			"-i", "pipe:0", // read RTMP data from stdin
			"-vn", // drop video track
			"-c:a", "aac",
			"-b:a", "192k",
			"-ar", "44100",
			"-ac", "2",
			// LL-HLS output
			"-f", "hls",
			"-hls_time", "1",
			"-hls_list_size", "6",
			"-hls_flags", "delete_segments+independent_segments+program_date_time+append_list",
			"-hls_segment_type", "fmp4",
			"-hls_fmp4_init_filename", "init.mp4",
			"-hls_segment_filename", segPattern,
			playlist,
		}
	} else {
		args = []string{
			"-loglevel", "warning",
			"-re",
			"-i", "pipe:0",
			// Video: re-encode to H.264 baseline for maximum compatibility
			"-c:v", "libx264",
			"-profile:v", "baseline",
			"-level:v", "3.1",
			"-preset", "veryfast", // CPU efficiency on a VPS
			"-tune", "zerolatency", // minimize encoder latency
			"-b:v", "2500k",
			"-maxrate", "2500k",
			"-bufsize", "5000k",
			"-g", "60", // keyframe every 2 s at 30 fps — required for HLS
			"-sc_threshold", "0",
			// Audio
			"-c:a", "aac",
			"-b:a", "192k",
			"-ar", "44100",
			"-ac", "2",
			// LL-HLS output
			"-f", "hls",
			"-hls_time", "1",
			"-hls_list_size", "6",
			"-hls_flags", "delete_segments+independent_segments+program_date_time+append_list",
			"-hls_segment_type", "fmp4",
			"-hls_fmp4_init_filename", "init.mp4",
			"-hls_segment_filename", segPattern,
			playlist,
		}
	}

	// Append RTMP forwarding destinations as additional FFmpeg outputs.
	if isRadio {
		for _, dest := range destinations {
			args = append(args, "-vn", "-c:a", "copy", "-f", "flv", dest)
		}
	} else {
		for _, dest := range destinations {
			args = append(args, "-c:v", "copy", "-c:a", "copy", "-f", "flv", dest)
		}
	}

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)

	// Pipe the raw RTMP bytes from the accepted TCP connection into FFmpeg stdin.
	// FFmpeg's RTMP demuxer operates directly on the bytestream.
	cmd.Stdin = rtmpConn
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	log.Printf("[stream/%s] FFmpeg starting (radio=%v)", key, isRadio)
	if err := cmd.Run(); err != nil {
		if !errors.Is(ctx.Err(), context.Canceled) {
			log.Printf("[stream/%s] FFmpeg error: %v", key, err)
		}
	}
}

// stop terminates a transcoding session by key.
func (sm *streamManager) stop(key string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if ss, ok := sm.streams[key]; ok {
		ss.cancel()
		delete(sm.streams, key)
	}
}

// status returns live=true/false and start time for a key.
func (sm *streamManager) status(key string) (live bool, startedAt time.Time) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if ss, ok := sm.streams[key]; ok {
		return ss.live.Load(), ss.startedAt
	}
	return false, time.Time{}
}

// listStreams returns all known stream keys.
func (sm *streamManager) listStreams() []string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	keys := make([]string, 0, len(sm.streams))
	for k := range sm.streams {
		keys = append(keys, k)
	}
	return keys
}

// ─── RTMP ingestion ───────────────────────────────────────────────────────────

// rtmpServer listens on TCP 1935 for RTMP connections.
// It reads the stream key from the RTMP connect/publish handshake,
// then hands the raw connection to streamManager.start().
//
// Full RTMP parsing is complex; we delegate it entirely to FFmpeg by
// piping stdin. The stream key is extracted from the URL path that the
// publisher sends in the RTMP "connect" AMF command.
//
// Simplified key extraction: we peek at the first 4 KB looking for
// the connect URL string (e.g. "rtmp://server:1935/live/radio").
// This is reliable with OBS, vMix, Liquidsoap, and FFmpeg publishers.
func startRTMPServer(sm *streamManager) {
	ln, err := net.Listen("tcp", ":"+RTMPPort)
	if err != nil {
		log.Printf("[rtmp] WARNING: listen error (RTMP disabled): %v", err)
		return
	}
	log.Printf("[rtmp] Listening on :%s", RTMPPort)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("[rtmp] accept error: %v", err)
			continue
		}
		go handleRTMPConn(conn, sm)
	}
}

// handleRTMPConn peeks at the incoming RTMP bytes to extract the stream
// key, then pipes the full connection (including already-read bytes) to FFmpeg.
func handleRTMPConn(conn net.Conn, sm *streamManager) {
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	// Peek at up to 4 KB to find the stream key in the RTMP URL.
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		conn.Close()
		return
	}
	conn.SetReadDeadline(time.Time{}) // reset deadline after handshake peek

	key := extractStreamKey(buf[:n])
	if key == "" {
		key = "live" // fallback key
	}
	log.Printf("[rtmp] New publisher → stream key: %q", key)

	// Look up multistream destinations for this key.
	destinations := getDestinationsForKey(key)
	if len(destinations) > 0 {
		log.Printf("[rtmp/%s] Multistreaming to %d destination(s)", key, len(destinations))
	}

	// Reconstruct a reader that includes the peeked bytes + remaining conn.
	combined := io.MultiReader(strings.NewReader(string(buf[:n])), conn)

	// Wrap back into a net.Conn-like reader the transcode pipeline can use.
	sm.start(key, &peekedConn{Reader: combined, Conn: conn}, destinations)
}

// peekedConn wraps the re-joined reader with the original net.Conn
// so FFmpeg still gets a proper io.Reader and we can Close() it.
type peekedConn struct {
	io.Reader
	net.Conn
}

func (p *peekedConn) Read(b []byte) (int, error) { return p.Reader.Read(b) }

// extractStreamKey looks for a path component in the raw RTMP bytes.
// RTMP clients encode the app path as a UTF-8 string in AMF0 format
// after the C0/C1/C2 handshake. We search for a "/" prefix which marks
// the app URL (e.g. "/live/radio"). We take the last path segment.
func extractStreamKey(data []byte) string {
	s := string(data)
	// Common patterns: "/live/radio", "live/radio", "radio"
	for _, prefix := range []string{"/live/", "live/"} {
		if idx := strings.Index(s, prefix); idx != -1 {
			rest := s[idx+len(prefix):]
			// Key ends at the next non-printable or space character
			end := strings.IndexAny(rest, "\x00\x01\x02\x03 \t\n\r")
			if end == -1 {
				end = len(rest)
			}
			if end > 0 && end < 64 {
				return sanitizeKey(rest[:end])
			}
		}
	}
	return ""
}

// sanitizeKey strips anything that is not alphanumeric, dash, or underscore.
func sanitizeKey(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// ─── Chat hub ─────────────────────────────────────────────────────────────────

// ChatMessage is the payload exchanged over WebSocket.
type ChatMessage struct {
	Type    string `json:"type"`
	User    string `json:"user"`
	Message string `json:"message"`
	Time    string `json:"time"`
}

type chatHub struct {
	clients   map[*websocket.Conn]bool
	mu        sync.Mutex
	broadcast chan ChatMessage
}

var hub = &chatHub{
	clients:   make(map[*websocket.Conn]bool),
	broadcast: make(chan ChatMessage, 256),
}

func (h *chatHub) run() {
	for msg := range h.broadcast {
		h.mu.Lock()
		for conn := range h.clients {
			if err := conn.WriteJSON(msg); err != nil {
				conn.Close()
				delete(h.clients, conn)
			}
		}
		h.mu.Unlock()
	}
}

func (h *chatHub) register(c *websocket.Conn)   { h.mu.Lock(); h.clients[c] = true; h.mu.Unlock() }
func (h *chatHub) unregister(c *websocket.Conn) { h.mu.Lock(); delete(h.clients, c); h.mu.Unlock() }

// ─── Global state ─────────────────────────────────────────────────────────────

var (
	stationCfg = StationConfig{
		StationName: "Radio In One Stop",
		HLSBaseURL:  "", // populated at startup from PORT env
	}
	stationMu   sync.RWMutex
	viewerCount int64

	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     func(r *http.Request) bool { return true },
	}

	streams *streamManager
)

// ─── Authentication & Database ──────────────────────────────────────────────

type contextKey string

const (
	contextKeyUserID contextKey = "user_id"
	contextKeyEmail  contextKey = "email"
)

// Claims holds the JWT payload.
type Claims struct {
	UserID      string `json:"user_id"`
	Email       string `json:"email"`
	StationName string `json:"station_name"`
	jwt.RegisteredClaims
}

var (
	db             *sql.DB
	jwtSecret      []byte
	rtmpIngestBase = "rtmp://localhost:1935/live"
)

// knownPlatforms maps platform ID → RTMP server base URL.
var knownPlatforms = map[string]string{
	"youtube":   "rtmp://a.rtmp.youtube.com/live2",
	"facebook":  "rtmps://live-api-s.facebook.com:443/rtmp",
	"tiktok":    "rtmp://push.tiktok.live/live",
	"instagram": "rtmps://edgetee-upload.facebook.com:443/rtmp",
}

// initDB opens the PostgreSQL database and runs schema migrations.
func initDB(dsn string) error {
	var err error
	db, err = sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	if err = db.Ping(); err != nil {
		return err
	}
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id            TEXT PRIMARY KEY,
			email         TEXT UNIQUE NOT NULL,
			password_hash TEXT NOT NULL,
			stream_key    TEXT UNIQUE NOT NULL,
			created_at    TEXT NOT NULL
		)
	`)
	if err != nil {
		return err
	}
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS destinations (
			id         TEXT PRIMARY KEY,
			user_id    TEXT NOT NULL,
			platform   TEXT NOT NULL,
			rtmp_url   TEXT NOT NULL,
			stream_key TEXT NOT NULL DEFAULT '',
			enabled    INTEGER NOT NULL DEFAULT 0,
			updated_at TEXT NOT NULL,
			FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
			UNIQUE(user_id, platform)
		)
	`)
	if err != nil {
		return err
	}
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS stations (
			id                      TEXT PRIMARY KEY,
			user_id                 TEXT NOT NULL UNIQUE,
			station_slug            TEXT NOT NULL UNIQUE,
			station_name            TEXT NOT NULL DEFAULT '',
			logo_url                TEXT NOT NULL DEFAULT '',
			is_live                 BOOLEAN NOT NULL DEFAULT false,
			current_listeners_count INTEGER NOT NULL DEFAULT 0,
			last_connected_at       TEXT,
			FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
		)
	`)
	if err != nil {
		return err
	}
	// Idempotent migrations for existing databases.
	for _, migration := range []string{
		`ALTER TABLE stations ADD COLUMN IF NOT EXISTS station_name TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE stations ADD COLUMN IF NOT EXISTS logo_url TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE stations ADD COLUMN IF NOT EXISTS genre TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE stations ADD COLUMN IF NOT EXISTS description TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE users ADD COLUMN IF NOT EXISTS last_name TEXT NOT NULL DEFAULT ''`,
	} {
		if _, err = db.Exec(migration); err != nil {
			return err
		}
	}
	return nil
}

// generateKey returns a cryptographically random 32-character hex string.
func generateKey() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// ─── Station hub (in-memory audio fan-out) ────────────────────────────────────

// stationHub fans out raw WebM audio chunks from one broadcaster to N listeners.
// The first received chunk (WebM initialization segment) is buffered so that
// listeners who join mid-stream still get a valid stream header.
type stationHub struct {
	mu     sync.Mutex
	subs   map[chan []byte]struct{}
	header []byte      // first chunk (WebM EBML + segment header)
	done   chan struct{} // closed when broadcaster disconnects
}

func newStationHub() *stationHub {
	return &stationHub{
		subs: make(map[chan []byte]struct{}),
		done: make(chan struct{}),
	}
}

func (h *stationHub) subscribe() chan []byte {
	ch := make(chan []byte, 32) // ~8 s buffer at 250 ms chunks
	h.mu.Lock()
	if len(h.header) > 0 {
		ch <- append([]byte(nil), h.header...)
	}
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *stationHub) unsubscribe(ch chan []byte) {
	h.mu.Lock()
	delete(h.subs, ch)
	h.mu.Unlock()
}

func (h *stationHub) broadcast(data []byte) {
	h.mu.Lock()
	if len(h.header) == 0 {
		h.header = append([]byte(nil), data...)
	}
	cp := append([]byte(nil), data...)
	for ch := range h.subs {
		select {
		case ch <- cp:
		default: // slow listener — drop chunk rather than blocking the broadcaster
		}
	}
	h.mu.Unlock()
}

var (
	hubsMu sync.RWMutex
	hubs   = make(map[string]*stationHub)
)

func getOrCreateHub(slug string) *stationHub {
	hubsMu.Lock()
	defer hubsMu.Unlock()
	if h, ok := hubs[slug]; ok {
		return h
	}
	h := newStationHub()
	hubs[slug] = h
	return h
}

// closeHub removes the hub from the registry and signals all listeners to exit.
func closeHub(slug string, h *stationHub) {
	hubsMu.Lock()
	if hubs[slug] == h {
		delete(hubs, slug)
	}
	hubsMu.Unlock()
	close(h.done)
}

// ─── Station helpers ──────────────────────────────────────────────────────────

// generateStationSlug derives a URL-safe slug from the station name
// and appends a 4-character random hex suffix for uniqueness.
// Falls back to the email prefix if stationName is empty.
func generateStationSlug(stationName, email string) string {
	source := stationName
	if strings.TrimSpace(source) == "" {
		at := strings.Index(email, "@")
		source = email
		if at > 0 {
			source = email[:at]
		}
	}
	var b strings.Builder
	prevHyphen := false
	for _, r := range strings.ToLower(source) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			prevHyphen = false
		} else if !prevHyphen && b.Len() > 0 {
			b.WriteRune('-')
			prevHyphen = true
		}
	}
	slug := strings.TrimSuffix(b.String(), "-")
	if len(slug) > 40 {
		slug = slug[:40]
	}
	if slug == "" {
		slug = "station"
	}
	rb := make([]byte, 2)
	rand.Read(rb) //nolint:errcheck
	return slug + "-" + hex.EncodeToString(rb)
}

// ensureStation creates a station row for the user if one does not already exist.
// It is idempotent and safe to call on every login / credential fetch.
// stationName and logoURL are stored only on initial creation; pass "" on login paths.
func ensureStation(userID, email, stationName, logoURL string) (string, error) {
	var slug string
	err := db.QueryRow(`SELECT station_slug FROM stations WHERE user_id = $1`, userID).Scan(&slug)
	if err == nil {
		return slug, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	slug = generateStationSlug(stationName, email)
	stationID, err := generateKey()
	if err != nil {
		return "", err
	}
	_, err = db.Exec(
		`INSERT INTO stations (id, user_id, station_slug, station_name, logo_url, is_live, current_listeners_count)
		 VALUES ($1, $2, $3, $4, $5, false, 0)
		 ON CONFLICT DO NOTHING`,
		stationID, userID, slug, stationName, logoURL,
	)
	if err != nil {
		return "", err
	}
	return slug, nil
}

// jwtSign creates a signed JWT for the given user (30-day expiry).
func jwtSign(userID, email, stationName string) (string, error) {
	claims := Claims{
		UserID:      userID,
		Email:       email,
		StationName: stationName,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(30 * 24 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(jwtSecret)
}

// jwtVerify parses and validates a JWT, returning its claims.
func jwtVerify(tokenStr string) (*Claims, error) {
	t, err := jwt.ParseWithClaims(tokenStr, &Claims{}, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return jwtSecret, nil
	})
	if err != nil {
		return nil, err
	}
	if claims, ok := t.Claims.(*Claims); ok && t.Valid {
		return claims, nil
	}
	return nil, fmt.Errorf("invalid token")
}

// requireAuth is HTTP middleware that validates a Bearer JWT and injects
// user context values for downstream handlers.
func requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		claims, err := jwtVerify(strings.TrimPrefix(auth, "Bearer "))
		if err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		ctx := context.WithValue(r.Context(), contextKeyUserID, claims.UserID)
		ctx = context.WithValue(ctx, contextKeyEmail, claims.Email)
		next.ServeHTTP(w, r.WithContext(ctx))
	}
}

// handleRegister creates a new user with an auto-generated stream key.
// POST /api/auth/register  {"email":"...","password":"...","first_name":"...","last_name":"...","station_name":"...","logo_url":"...","genre":"...","description":"..."}
func handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Email       string `json:"email"`
		Password    string `json:"password"`
		FirstName   string `json:"first_name"`
		LastName    string `json:"last_name"`
		StationName string `json:"station_name"`
		LogoURL     string `json:"logo_url"`
		Genre       string `json:"genre"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	body.Email = strings.ToLower(strings.TrimSpace(body.Email))
	body.FirstName = strings.TrimSpace(body.FirstName)
	body.LastName = strings.TrimSpace(body.LastName)
	body.StationName = strings.TrimSpace(body.StationName)
	body.Genre = strings.TrimSpace(body.Genre)
	body.Description = strings.TrimSpace(body.Description)
	if _, err := mail.ParseAddress(body.Email); err != nil {
		http.Error(w, "invalid email address", http.StatusBadRequest)
		return
	}
	if body.FirstName == "" {
		http.Error(w, "first name is required", http.StatusBadRequest)
		return
	}
	if body.LastName == "" {
		http.Error(w, "last name is required", http.StatusBadRequest)
		return
	}
	if body.StationName == "" {
		http.Error(w, "station name is required", http.StatusBadRequest)
		return
	}
	if len(body.Password) < 8 {
		http.Error(w, "password must be at least 8 characters", http.StatusBadRequest)
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(body.Password), bcrypt.DefaultCost)
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	userID, err := generateKey()
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	streamKey, err := generateKey()
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	_, err = db.Exec(
		`INSERT INTO users (id, email, password_hash, stream_key, first_name, last_name, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		userID, body.Email, string(hash), streamKey, body.FirstName, body.LastName, time.Now().UTC().Format(time.RFC3339),
	)
	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
			http.Error(w, "email already registered", http.StatusConflict)
			return
		}
		log.Printf("[auth] register error: %v", err)
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	token, err := jwtSign(userID, body.Email, body.StationName)
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	stationSlug, _ := ensureStation(userID, body.Email, body.StationName, body.LogoURL)
	// Store genre and description if station was created
	if stationSlug != "" && (body.Genre != "" || body.Description != "") {
		_, _ = db.Exec(
			`UPDATE stations SET genre = $1, description = $2 WHERE user_id = $3`,
			body.Genre, body.Description, userID,
		)
	}
	resp := map[string]string{
		"token":      token,
		"stream_key": streamKey,
		"rtmp_url":   rtmpIngestBase + "/" + streamKey,
	}
	if stationSlug != "" {
		resp["station_slug"] = stationSlug
		resp["listen_url"] = "/listen/" + stationSlug
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleLogin authenticates an existing user and returns a fresh JWT.
// POST /api/auth/login  {"email": "...", "password": "..."}
func handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	body.Email = strings.ToLower(strings.TrimSpace(body.Email))
	var userID, passwordHash, streamKey string
	err := db.QueryRow(
		`SELECT id, password_hash, stream_key FROM users WHERE email = $1`, body.Email,
	).Scan(&userID, &passwordHash, &streamKey)
	if errors.Is(err, sql.ErrNoRows) {
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
		return
	}
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	if err := bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(body.Password)); err != nil {
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
		return
	}
	var stationName string
	_ = db.QueryRow(`SELECT station_name FROM stations WHERE user_id = $1`, userID).Scan(&stationName)
	token, err := jwtSign(userID, body.Email, stationName)
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	stationSlug, _ := ensureStation(userID, body.Email, "", "")
	loginResp := map[string]string{
		"token":      token,
		"stream_key": streamKey,
		"rtmp_url":   rtmpIngestBase + "/" + streamKey,
	}
	if stationSlug != "" {
		loginResp["station_slug"] = stationSlug
		loginResp["listen_url"] = "/listen/" + stationSlug
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(loginResp)
}

// handleUserProfile dispatches GET/PUT on /api/user/profile.
func handleUserProfile(w http.ResponseWriter, r *http.Request) {
	userID := r.Context().Value(contextKeyUserID).(string)
	email, _ := r.Context().Value(contextKeyEmail).(string)

	switch r.Method {
	case http.MethodGet:
		var firstName, lastName string
		_ = db.QueryRow(`SELECT first_name, last_name FROM users WHERE id = $1`, userID).Scan(&firstName, &lastName)
		var stationName, genre, description, logoURL string
		_ = db.QueryRow(`SELECT station_name, genre, description, logo_url FROM stations WHERE user_id = $1`, userID).
			Scan(&stationName, &genre, &description, &logoURL)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"email":        email,
			"first_name":   firstName,
			"last_name":    lastName,
			"station_name": stationName,
			"genre":        genre,
			"description":  description,
			"logo_url":     logoURL,
		})

	case http.MethodPut:
		var body struct {
			Email       string `json:"email"`
			FirstName   string `json:"first_name"`
			LastName    string `json:"last_name"`
			StationName string `json:"station_name"`
			Genre       string `json:"genre"`
			Description string `json:"description"`
			LogoURL     string `json:"logo_url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		body.Email = strings.ToLower(strings.TrimSpace(body.Email))

		if body.Email != "" && body.Email != email {
			_, err := db.Exec(`UPDATE users SET email = $1 WHERE id = $2`, body.Email, userID)
			if err != nil {
				if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
					http.Error(w, "email already in use", http.StatusConflict)
					return
				}
				http.Error(w, "server error", http.StatusInternalServerError)
				return
			}
			email = body.Email
		}
		if _, err := db.Exec(
			`UPDATE users SET first_name = $1, last_name = $2 WHERE id = $3`,
			strings.TrimSpace(body.FirstName), strings.TrimSpace(body.LastName), userID,
		); err != nil {
			http.Error(w, "server error", http.StatusInternalServerError)
			return
		}
		if _, err := db.Exec(
			`UPDATE stations SET station_name = $1, genre = $2, description = $3, logo_url = $4 WHERE user_id = $5`,
			strings.TrimSpace(body.StationName), body.Genre, strings.TrimSpace(body.Description), body.LogoURL, userID,
		); err != nil {
			http.Error(w, "server error", http.StatusInternalServerError)
			return
		}
		// Issue a fresh token so the frontend picks up any name/email change immediately
		token, err := jwtSign(userID, email, strings.TrimSpace(body.StationName))
		if err != nil {
			http.Error(w, "server error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"token": token})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleChangePassword changes the authenticated user's password.
// PUT /api/user/password  {"current_password":"...","new_password":"..."}
func handleChangePassword(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID := r.Context().Value(contextKeyUserID).(string)
	var body struct {
		CurrentPassword string `json:"current_password"`
		NewPassword     string `json:"new_password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if len(body.NewPassword) < 8 {
		http.Error(w, "new password must be at least 8 characters", http.StatusBadRequest)
		return
	}
	var hash string
	if err := db.QueryRow(`SELECT password_hash FROM users WHERE id = $1`, userID).Scan(&hash); err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(body.CurrentPassword)); err != nil {
		http.Error(w, "current password is incorrect", http.StatusUnauthorized)
		return
	}
	newHash, err := bcrypt.GenerateFromPassword([]byte(body.NewPassword), bcrypt.DefaultCost)
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	if _, err := db.Exec(`UPDATE users SET password_hash = $1 WHERE id = $2`, string(newHash), userID); err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleDeleteAccount permanently deletes the authenticated user's account.
// DELETE /api/user/account  {"password":"..."}
func handleDeleteAccount(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID := r.Context().Value(contextKeyUserID).(string)
	var body struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	var hash string
	if err := db.QueryRow(`SELECT password_hash FROM users WHERE id = $1`, userID).Scan(&hash); err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(body.Password)); err != nil {
		http.Error(w, "incorrect password", http.StatusUnauthorized)
		return
	}
	if _, err := db.Exec(`DELETE FROM users WHERE id = $1`, userID); err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleStreamCredentials dispatches GET/PUT on /api/user/stream-credentials.
func handleStreamCredentials(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		handleGetCredentials(w, r)
	case http.MethodPut:
		handleSaveCredentials(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleGetCredentials returns the current user's stream key, ingest URL,
// and saved external platform destinations.
func handleGetCredentials(w http.ResponseWriter, r *http.Request) {
	userID := r.Context().Value(contextKeyUserID).(string)
	email, _ := r.Context().Value(contextKeyEmail).(string)
	var streamKey string
	if err := db.QueryRow(`SELECT stream_key FROM users WHERE id = $1`, userID).Scan(&streamKey); err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	// Ensure a station row exists (idempotent — safe for existing users)
	stationSlug, _ := ensureStation(userID, email, "", "")

	rows, err := db.Query(
		`SELECT platform, rtmp_url, stream_key, enabled FROM destinations WHERE user_id = $1 ORDER BY platform`,
		userID,
	)
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	type destResp struct {
		Platform  string `json:"platform"`
		RTMPUrl   string `json:"rtmp_url"`
		StreamKey string `json:"stream_key"`
		Enabled   bool   `json:"enabled"`
	}
	var dests []destResp
	for rows.Next() {
		var d destResp
		var enabled int
		if err := rows.Scan(&d.Platform, &d.RTMPUrl, &d.StreamKey, &enabled); err != nil {
			continue
		}
		d.Enabled = enabled == 1
		dests = append(dests, d)
	}
	if dests == nil {
		dests = []destResp{}
	}
	resp := map[string]interface{}{
		"stream_key":       streamKey,
		"rtmp_url":         rtmpIngestBase + "/" + streamKey,
		"rtmp_ingest_base": rtmpIngestBase,
		"destinations":     dests,
	}
	if stationSlug != "" {
		resp["station_slug"] = stationSlug
		resp["listen_url"] = "/listen/" + stationSlug
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleSaveCredentials upserts external platform stream keys for the current user.
// PUT /api/user/stream-credentials
// Body: {"destinations": [{"platform": "youtube", "stream_key": "...", "enabled": true}]}
func handleSaveCredentials(w http.ResponseWriter, r *http.Request) {
	userID := r.Context().Value(contextKeyUserID).(string)
	var body struct {
		Destinations []struct {
			Platform  string `json:"platform"`
			StreamKey string `json:"stream_key"`
			Enabled   bool   `json:"enabled"`
		} `json:"destinations"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()
	for _, d := range body.Destinations {
		rtmpURL, ok := knownPlatforms[d.Platform]
		if !ok {
			continue
		}
		id, _ := generateKey()
		enabled := 0
		if d.Enabled {
			enabled = 1
		}
		_, err := tx.Exec(`
			INSERT INTO destinations (id, user_id, platform, rtmp_url, stream_key, enabled, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (user_id, platform) DO UPDATE SET
				stream_key = EXCLUDED.stream_key,
				enabled    = EXCLUDED.enabled,
				updated_at = EXCLUDED.updated_at
		`, id, userID, d.Platform, rtmpURL, strings.TrimSpace(d.StreamKey), enabled, time.Now().UTC().Format(time.RFC3339))
		if err != nil {
			log.Printf("[auth] save destinations error: %v", err)
			http.Error(w, "server error", http.StatusInternalServerError)
			return
		}
	}
	if err := tx.Commit(); err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// getDestinationsForKey returns the enabled RTMP forwarding target URLs
// for the user whose stream_key matches the given key.
func getDestinationsForKey(streamKey string) []string {
	if db == nil {
		return nil
	}
	rows, err := db.Query(`
		SELECT d.rtmp_url, d.stream_key
		FROM destinations d
		JOIN users u ON u.id = d.user_id
		WHERE u.stream_key = $1 AND d.enabled = 1 AND d.stream_key != ''
	`, streamKey)
	if err != nil {
		log.Printf("[db] destinations lookup error: %v", err)
		return nil
	}
	defer rows.Close()
	var dests []string
	for rows.Next() {
		var rtmpURL, key string
		if err := rows.Scan(&rtmpURL, &key); err != nil {
			continue
		}
		dests = append(dests, rtmpURL+"/"+key)
	}
	return dests
}

// ─── HTTP handlers ────────────────────────────────────────────────────────────

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleConferenceToken issues a short-lived LiveKit JWT so the browser
// can join an audio conference room. For authenticated users the display
// name is always read from the stations table (authoritative). Guests
// (no valid JWT) fall back to the ?username= query param.
func handleConferenceToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	room := strings.TrimSpace(r.URL.Query().Get("room"))
	username := strings.TrimSpace(r.URL.Query().Get("username"))

	if room == "" {
		http.Error(w, "room is required", http.StatusBadRequest)
		return
	}
	if len(room) > 64 {
		http.Error(w, "room must be \u2264 64 characters", http.StatusBadRequest)
		return
	}
	// Restrict room to safe characters (UUID-like: hex + hyphens)
	for _, c := range room {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-') {
			http.Error(w, "invalid room id", http.StatusBadRequest)
			return
		}
	}

	// If the request carries a valid auth token, look up the station name
	// from the DB so it is always current, regardless of JWT age.
	if authHeader := r.Header.Get("Authorization"); strings.HasPrefix(authHeader, "Bearer ") {
		if claims, err := jwtVerify(strings.TrimPrefix(authHeader, "Bearer ")); err == nil && claims.UserID != "" {
			var stationName string
			if dbErr := db.QueryRow(`SELECT station_name FROM stations WHERE user_id = $1`, claims.UserID).Scan(&stationName); dbErr == nil && strings.TrimSpace(stationName) != "" {
				username = strings.TrimSpace(stationName)
			}
		}
	}

	if username == "" {
		http.Error(w, "username is required", http.StatusBadRequest)
		return
	}
	if len(username) > 64 {
		username = username[:64]
	}

	apiKey := os.Getenv("LIVEKIT_API_KEY")
	apiSecret := os.Getenv("LIVEKIT_API_SECRET")
	livekitURL := os.Getenv("LIVEKIT_URL")

	if apiKey == "" || apiSecret == "" || livekitURL == "" {
		http.Error(w, "conference not configured on server", http.StatusServiceUnavailable)
		return
	}

	canPublish := true
	canSubscribe := true

	at := auth.NewAccessToken(apiKey, apiSecret)
	grant := &auth.VideoGrant{
		RoomJoin:     true,
		Room:         room,
		CanPublish:   &canPublish,
		CanSubscribe: &canSubscribe,
	}
	at.AddGrant(grant).
		SetIdentity(username).
		SetName(username).
		SetValidFor(2 * time.Hour)

	token, err := at.ToJWT()
	if err != nil {
		log.Printf("[conference] token generation failed: %v", err)
		http.Error(w, "failed to generate token", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"token": token,
		"url":   livekitURL,
	})
}

// handleConfig returns/updates station metadata.
// It also computes the HLS URLs for radio and video streams dynamically
// so the frontend always gets real, working playlist URLs.
func handleConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	case http.MethodGet:
		stationMu.RLock()
		base := stationCfg.HLSBaseURL
		name := stationCfg.StationName
		stationMu.RUnlock()

		// Derive HLS URLs from the base — backend serves them under /hls/
		json.NewEncoder(w).Encode(map[string]string{
			"stationName": name,
			"hlsBaseURL":  base,
			"radioUrl":    base + "/hls/radio/index.m3u8",
			"videoUrl":    base + "/hls/video/index.m3u8",
		})

	case http.MethodPost:
		var body struct {
			StationName string `json:"stationName"`
			HLSBaseURL  string `json:"hlsBaseURL"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		stationMu.Lock()
		if body.StationName != "" {
			stationCfg.StationName = body.StationName
		}
		if body.HLSBaseURL != "" {
			stationCfg.HLSBaseURL = body.HLSBaseURL
		}
		stationMu.Unlock()
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleStreams lists active streams and their live status.
func handleStreams(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	type streamInfo struct {
		Key       string `json:"key"`
		Live      bool   `json:"live"`
		StartedAt string `json:"startedAt,omitempty"`
	}
	var infos []streamInfo
	for _, k := range streams.listStreams() {
		live, startedAt := streams.status(k)
		inf := streamInfo{Key: k, Live: live}
		if !startedAt.IsZero() {
			inf.StartedAt = startedAt.UTC().Format(time.RFC3339)
		}
		infos = append(infos, inf)
	}
	if infos == nil {
		infos = []streamInfo{}
	}
	json.NewEncoder(w).Encode(infos)
}

// handleStreamStatus returns live/offline status for a single stream key.
// GET /api/streams/status?key=radio
func handleStreamStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	live, startedAt := streams.status(sanitizeKey(key))
	resp := map[string]interface{}{"live": live}
	if !startedAt.IsZero() {
		resp["startedAt"] = startedAt.UTC().Format(time.RFC3339)
	}
	json.NewEncoder(w).Encode(resp)
}

func handleViewers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int64{"viewers": atomic.LoadInt64(&viewerCount)})
}

func handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	atomic.AddInt64(&viewerCount, 1)
	go func() {
		time.Sleep(15 * time.Second)
		atomic.AddInt64(&viewerCount, -1)
	}()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int64{"viewers": atomic.LoadInt64(&viewerCount)})
}

func handleChat(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("[ws] upgrade:", err)
		return
	}
	hub.register(conn)
	defer func() {
		hub.unregister(conn)
		conn.Close()
	}()

	conn.SetReadLimit(512)
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		var msg ChatMessage
		if err := conn.ReadJSON(&msg); err != nil {
			break
		}
		msg.Time = time.Now().Format("15:04")
		msg.Type = "message"
		if len(msg.Message) > 256 {
			msg.Message = msg.Message[:256]
		}
		if len(msg.User) > 32 {
			msg.User = msg.User[:32]
		}
		hub.broadcast <- msg
	}
}

// ─── Browser audio encoder WebSocket ─────────────────────────────────────────
//
// Protocol (all frames are WebSocket messages):
//   Browser → Server (text)  : JSON config  {"action":"start","host":"...","port":"8000","mount":"/radio","username":"source","password":"...","codec":"mp3","bitrate":"192k"}
//   Browser → Server (binary): raw WebM/Opus chunks from MediaRecorder (250 ms timeslices)
//   Browser → Server (text)  : {"action":"stop"}  — graceful stop
//   Server  → Browser (text) : {"status":"live","msg":"..."}   — FFmpeg started OK
//   Server  → Browser (text) : {"status":"stopped"}           — clean stop
//   Server  → Browser (text) : {"status":"error","msg":"..."}  — fatal error
//
// Auth: JWT is passed as the `token` query parameter because browsers cannot
// set custom headers on WebSocket upgrade requests.

type encoderConfig struct {
	Action   string `json:"action"` // "start" (default) | "stop"
	Host     string `json:"host"`
	Port     string `json:"port"`
	Mount    string `json:"mount"`
	Username string `json:"username"`
	Password string `json:"password"`
	Codec    string `json:"codec"`   // "mp3" | "aac"
	Bitrate  string `json:"bitrate"` // e.g. "192k"
}

type encoderStatus struct {
	Status string `json:"status"`
	Msg    string `json:"msg,omitempty"`
}

// handleBroadcast is the hub-mode branch of the encoder WebSocket.
// The browser sends raw WebM audio chunks; the server fans them out to
// all active /listen/{slug} HTTP clients for this station.
func handleBroadcast(conn *websocket.Conn, sendStatus func(string, string), userID string) {
	var stationSlug string
	err := db.QueryRow(`SELECT station_slug FROM stations WHERE user_id = $1`, userID).Scan(&stationSlug)
	if err != nil {
		sendStatus("error", "no station found for your account — log out and back in")
		return
	}

	db.Exec(`UPDATE stations SET is_live = true, last_connected_at = $1 WHERE user_id = $2`, //nolint:errcheck
		time.Now().UTC().Format(time.RFC3339), userID)

	h := getOrCreateHub(stationSlug)
	sendStatus("live", "Broadcasting — listeners at /listen/"+stationSlug)
	log.Printf("[hub/%s] broadcaster connected (user=%s)", stationSlug, userID)

	defer func() {
		closeHub(stationSlug, h)
		db.Exec(`UPDATE stations SET is_live = false WHERE user_id = $1`, userID) //nolint:errcheck
		log.Printf("[hub/%s] broadcaster disconnected", stationSlug)
	}()

	for {
		conn.SetReadDeadline(time.Now().Add(15 * time.Second))
		mt, data, err := conn.ReadMessage()
		conn.SetReadDeadline(time.Time{})
		if err != nil {
			return
		}
		if mt == websocket.TextMessage {
			var ctrl encoderConfig
			if json.Unmarshal(data, &ctrl) == nil && ctrl.Action == "stop" {
				sendStatus("stopped", "")
				return
			}
			continue
		}
		h.broadcast(data)
	}
}

// handleListen streams live WebM audio from the station hub to an HTTP client.
// GET /listen/{station_slug}

// handleGetStations returns all registered stations (public).
// GET /api/stations
func handleGetStations(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(`
		SELECT station_slug, station_name, logo_url, is_live, current_listeners_count, genre, description
		FROM stations
		ORDER BY is_live DESC, station_name ASC
	`)
	if err != nil {
		http.Error(w, `{"error":"db error"}`, http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	type Station struct {
		Slug      string `json:"slug"`
		Name      string `json:"name"`
		LogoURL   string `json:"logo_url"`
		IsLive    bool   `json:"is_live"`
		Listeners int    `json:"listeners"`
		Genre     string `json:"genre"`
		Desc      string `json:"description"`
	}
	stations := []Station{}
	for rows.Next() {
		var s Station
		if err := rows.Scan(&s.Slug, &s.Name, &s.LogoURL, &s.IsLive, &s.Listeners, &s.Genre, &s.Desc); err != nil {
			continue
		}
		stations = append(stations, s)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stations)
}

// handleGetStation returns a single station by slug (public).
// GET /api/stations/{slug}
func handleGetStation(w http.ResponseWriter, r *http.Request) {
	slug := strings.TrimPrefix(r.URL.Path, "/api/stations/")
	slug = strings.Trim(slug, "/")
	if slug == "" {
		handleGetStations(w, r)
		return
	}
	type Station struct {
		Slug      string `json:"slug"`
		Name      string `json:"name"`
		LogoURL   string `json:"logo_url"`
		IsLive    bool   `json:"is_live"`
		Listeners int    `json:"listeners"`
		Genre     string `json:"genre"`
		Desc      string `json:"description"`
	}
	var s Station
	err := db.QueryRow(`
		SELECT station_slug, station_name, logo_url, is_live, current_listeners_count, genre, description
		FROM stations WHERE station_slug = $1
	`, slug).Scan(&s.Slug, &s.Name, &s.LogoURL, &s.IsLive, &s.Listeners, &s.Genre, &s.Desc)
	if err != nil {
		http.Error(w, `{"error":"station not found"}`, http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s)
}


func handleListen(w http.ResponseWriter, r *http.Request) {
	slug := strings.TrimPrefix(r.URL.Path, "/listen/")
	slug = strings.Trim(slug, "/")
	if slug == "" || strings.ContainsAny(slug, " \t\n\r/") {
		http.Error(w, "invalid station id", http.StatusBadRequest)
		return
	}

	// Verify station exists in DB
	var isLive bool
	err := db.QueryRow(`SELECT is_live FROM stations WHERE station_slug = $1`, slug).Scan(&isLive)
	if errors.Is(err, sql.ErrNoRows) {
		http.Error(w, "station not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}

	// Get hub
	hubsMu.RLock()
	h, ok := hubs[slug]
	hubsMu.RUnlock()
	if !ok {
		http.Error(w, "station is offline", http.StatusServiceUnavailable)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "audio/webm; codecs=opus")
	w.Header().Set("Cache-Control", "no-cache, no-store")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ch := h.subscribe()
	defer h.unsubscribe(ch)

	db.Exec(`UPDATE stations SET current_listeners_count = current_listeners_count + 1 WHERE station_slug = $1`, slug) //nolint:errcheck
	defer db.Exec(`UPDATE stations SET current_listeners_count = GREATEST(0, current_listeners_count - 1) WHERE station_slug = $1`, slug) //nolint:errcheck

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.done:
			return
		case data, ok := <-ch:
			if !ok {
				return
			}
			if _, err := w.Write(data); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func handleEncoderWS(w http.ResponseWriter, r *http.Request) {
	// ── Authenticate via JWT query param (browsers can't set WS headers) ──
	tokenStr := r.URL.Query().Get("token")
	if tokenStr == "" {
		if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
			tokenStr = strings.TrimPrefix(auth, "Bearer ")
		}
	}
	if tokenStr == "" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	claims := &Claims{}
	if _, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return jwtSecret, nil
	}); err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[encoder] ws upgrade: %v", err)
		return
	}
	defer conn.Close()

	sendStatus := func(status, msg string) {
		data, _ := json.Marshal(encoderStatus{Status: status, Msg: msg})
		conn.WriteMessage(websocket.TextMessage, data) //nolint:errcheck
	}

	// ── Read JSON config frame (first text message) ────────────────────────
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	mt, raw, err := conn.ReadMessage()
	conn.SetReadDeadline(time.Time{})
	if err != nil {
		return
	}
	if mt != websocket.TextMessage {
		sendStatus("error", "expected JSON config frame first")
		return
	}
	var cfg encoderConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		sendStatus("error", "invalid config: "+err.Error())
		return
	}
	if cfg.Action == "stop" {
		return
	}

	// ── Hub broadcast mode (no Icecast / FFmpeg required) ─────────────────
	if cfg.Action == "broadcast" {
		handleBroadcast(conn, sendStatus, claims.UserID)
		return
	}

	// ── Validate and sanitize inputs ───────────────────────────────────────
	cfg.Host = strings.TrimSpace(cfg.Host)
	if cfg.Host == "" || strings.ContainsAny(cfg.Host, " \t\n\r@?#") {
		sendStatus("error", "invalid host")
		return
	}
	cfg.Port = strings.TrimSpace(cfg.Port)
	if cfg.Port == "" {
		cfg.Port = "8000"
	}
	var portNum int
	if _, err := fmt.Sscanf(cfg.Port, "%d", &portNum); err != nil || portNum < 1 || portNum > 65535 {
		sendStatus("error", "invalid port (must be 1–65535)")
		return
	}
	cfg.Mount = strings.TrimSpace(cfg.Mount)
	if cfg.Mount == "" {
		cfg.Mount = "/radio"
	}
	if !strings.HasPrefix(cfg.Mount, "/") {
		cfg.Mount = "/" + cfg.Mount
	}
	if cfg.Username == "" {
		cfg.Username = "source"
	}
	if cfg.Bitrate == "" {
		cfg.Bitrate = "192k"
	}
	codec := cfg.Codec
	if codec != "aac" {
		codec = "mp3"
	}

	// ── Server-side Icecast target override ───────────────────────────────
	// ICECAST_HOST / ICECAST_PORT env vars let the server admin pin the
	// Icecast endpoint (e.g. Railway private networking) regardless of what
	// the browser sends.  When unset the client-supplied values are used.
	if h := os.Getenv("ICECAST_HOST"); h != "" {
		cfg.Host = h
	}
	if p := os.Getenv("ICECAST_PORT"); p != "" {
		cfg.Port = p
	}

	// ── Build icecast:// URL using net/url (safe, no shell injection) ──────
	icecastURL := &url.URL{
		Scheme: "icecast",
		User:   url.UserPassword(cfg.Username, cfg.Password),
		Host:   cfg.Host + ":" + cfg.Port,
		Path:   cfg.Mount,
	}

	var audioCodec, outFmt string
	if codec == "aac" {
		audioCodec, outFmt = "aac", "adts"
	} else {
		audioCodec, outFmt = "libmp3lame", "mp3"
	}

	// FFmpeg args — exec.Command never passes these through a shell
	args := []string{
		"-loglevel", "warning",
		"-f", "webm",
		"-i", "pipe:0",
		"-vn",
		"-c:a", audioCodec,
		"-b:a", cfg.Bitrate,
		"-ar", "44100",
		"-ac", "2",
		"-f", outFmt,
		icecastURL.String(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		sendStatus("error", "ffmpeg stdin pipe: "+err.Error())
		return
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		sendStatus("error", "ffmpeg start: "+err.Error())
		return
	}
	log.Printf("[encoder/%s] started → %s:%s%s (codec=%s bitrate=%s)",
		claims.UserID, cfg.Host, cfg.Port, cfg.Mount, codec, cfg.Bitrate)

	sendStatus("live", fmt.Sprintf("Streaming → %s:%s%s", cfg.Host, cfg.Port, cfg.Mount))

	ffmpegDone := make(chan error, 1)
	go func() { ffmpegDone <- cmd.Wait() }()

	// ── Pump WebSocket binary frames → FFmpeg stdin ────────────────────────
	for {
		select {
		case <-ffmpegDone:
			sendStatus("stopped", "FFmpeg process exited")
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(15 * time.Second))
		mt, data, err := conn.ReadMessage()
		conn.SetReadDeadline(time.Time{})
		if err != nil {
			cancel()
			stdin.Close()
			<-ffmpegDone
			return
		}

		if mt == websocket.TextMessage {
			var ctrl encoderConfig
			if json.Unmarshal(data, &ctrl) == nil && ctrl.Action == "stop" {
				cancel()
				stdin.Close()
				<-ffmpegDone
				sendStatus("stopped", "")
				return
			}
			continue
		}

		// Binary audio chunk
		if _, err := stdin.Write(data); err != nil {
			cancel()
			sendStatus("error", "write to ffmpeg: "+err.Error())
			<-ffmpegDone
			return
		}
	}
}

// ─── HLS static file server ───────────────────────────────────────────────────

// hlsHandler serves HLS segments and manifests from HLSDir.
// Headers are set for maximum CDN / browser cacheability:
//   - .m3u8 playlists: no-cache (they update every ~1s)
//   - .mp4 / .ts segments: immutable (content-addressed by name)
func hlsHandler(w http.ResponseWriter, r *http.Request) {
	// Strip /hls/ prefix and sanitize the path
	rel := strings.TrimPrefix(r.URL.Path, "/hls/")
	abs := filepath.Join(HLSDir, filepath.Clean("/"+rel))

	// Prevent path traversal outside HLSDir
	if !strings.HasPrefix(abs, HLSDir) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	switch {
	case strings.HasSuffix(abs, ".m3u8"):
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	case strings.HasSuffix(abs, ".mp4"):
		w.Header().Set("Cache-Control", "public, max-age=86400, immutable")
		w.Header().Set("Content-Type", "video/mp4")
	case strings.HasSuffix(abs, ".ts"):
		w.Header().Set("Cache-Control", "public, max-age=86400, immutable")
		w.Header().Set("Content-Type", "video/MP2T")
	}

	http.ServeFile(w, r, abs)
}

// ─── Entry point ──────────────────────────────────────────────────────────────

func main() {
	// Load .env file if present (ignored in production where env vars are set externally)
	_ = godotenv.Load()

	// Ensure HLS output directory exists
	if err := os.MkdirAll(HLSDir, 0755); err != nil {
		log.Fatalf("Cannot create HLS dir: %v", err)
	}

	streams = newStreamManager()

	// Initialize PostgreSQL database.
	dbDSN := os.Getenv("DATABASE_URL")
	if dbDSN == "" {
		log.Fatalf("[db] DATABASE_URL environment variable is required")
	}
	if err := initDB(dbDSN); err != nil {
		log.Fatalf("[db] init error: %v", err)
	}
	log.Printf("[db] Connected to PostgreSQL")

	// JWT secret — use JWT_SECRET env var or generate ephemeral secret.
	if secret := os.Getenv("JWT_SECRET"); secret != "" {
		jwtSecret = []byte(secret)
	} else {
		log.Println("[auth] WARNING: JWT_SECRET not set — tokens invalidated on restart")
		b := make([]byte, 32)
		if _, err := rand.Read(b); err != nil {
			log.Fatalf("[auth] cannot generate JWT secret: %v", err)
		}
		jwtSecret = b
	}

	// RTMP ingest base URL returned in credential responses.
	if v := os.Getenv("RTMP_INGEST_BASE"); v != "" {
		rtmpIngestBase = v
	}

	// Start RTMP ingest server on :1935
	go startRTMPServer(streams)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// HLSBaseURL is intentionally left empty so the frontend receives
	// relative URLs (/hls/...) that work in dev (Vite proxy) and
	// production (Nginx) without hard-coding a host/port.
	// It can be overridden via the Admin Settings UI if needed.

	// HTTP routes
	mux := http.NewServeMux()
	mux.HandleFunc("/api/conference/token", handleConferenceToken)
	mux.HandleFunc("/api/health", handleHealth)
	mux.HandleFunc("/api/config", handleConfig)
	mux.HandleFunc("/api/streams", handleStreams)
	mux.HandleFunc("/api/streams/status", handleStreamStatus)
	mux.HandleFunc("/api/viewers", handleViewers)
	mux.HandleFunc("/api/viewers/heartbeat", handleHeartbeat)
	mux.HandleFunc("/ws/chat", handleChat)
	mux.HandleFunc("/api/auth/register", handleRegister)
	mux.HandleFunc("/api/auth/login", handleLogin)
	mux.HandleFunc("/api/user/stream-credentials", requireAuth(handleStreamCredentials))
	mux.HandleFunc("/api/user/profile", requireAuth(handleUserProfile))
	mux.HandleFunc("/api/user/password", requireAuth(handleChangePassword))
	mux.HandleFunc("/api/user/account", requireAuth(handleDeleteAccount))
	mux.HandleFunc("/api/stations/", handleGetStation)
	mux.HandleFunc("/api/stations", handleGetStations)
	mux.HandleFunc("/ws/encode", handleEncoderWS)
	mux.HandleFunc("/listen/", handleListen)

	// HLS static file handler (serves /hls/<streamKey>/index.m3u8 etc.)
	mux.HandleFunc("/hls/", hlsHandler)

	go hub.run()

	log.Printf("[http] Listening on :%s  (HLS dir: %s)", port, HLSDir)
	log.Printf("[http] RTMP ingest: rtmp://localhost:%s/live/<streamKey>", RTMPPort)
	log.Fatal(http.ListenAndServe(":"+port, corsMiddleware(mux)))
}
