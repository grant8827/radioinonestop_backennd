// Radio In One Stop — Media Server v2.1
// Super Admin API with role-based access control
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
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"html"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/mail"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	"github.com/lib/pq"
	"github.com/oschwald/geoip2-golang"
	"golang.org/x/crypto/bcrypt"
)

// ─── Configuration ────────────────────────────────────────────────────────────

// allowedOrigins is the set of frontend origins permitted to make
// cross-origin API calls and open the chat/encoder/conference WebSockets.
// Configure via the comma-separated ALLOWED_ORIGINS env var; falls back to
// the production frontend domain if unset.
var allowedOrigins = parseAllowedOrigins(os.Getenv("ALLOWED_ORIGINS"))

func parseAllowedOrigins(raw string) map[string]bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = "https://radioinonestop.com"
	}
	origins := map[string]bool{}
	for _, o := range strings.Split(raw, ",") {
		o = strings.TrimSpace(o)
		if o != "" {
			origins[o] = true
		}
	}
	return origins
}

func isAllowedOrigin(origin string) bool {
	return origin != "" && allowedOrigins[origin]
}

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
// If a session already exists for that key we reject the new publisher so the
// active broadcaster is never replaced implicitly.
func (sm *streamManager) start(key string, rtmpConn net.Conn, destinations []string) error {
	sm.mu.Lock()
	if existing, ok := sm.streams[key]; ok && existing.live.Load() {
		sm.mu.Unlock()
		return fmt.Errorf("stream %q is already live", key)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ss := &streamState{key: key, cancel: cancel, startedAt: time.Now(), destinations: destinations}
	ss.live.Store(true)
	sm.streams[key] = ss
	sm.mu.Unlock()

	outDir := filepath.Join(HLSDir, key)
	if err := os.MkdirAll(outDir, 0755); err != nil {
		log.Printf("[stream/%s] mkdir error: %v", key, err)
		sm.mu.Lock()
		delete(sm.streams, key)
		sm.mu.Unlock()
		cancel()
		return err
	}

	go sm.transcode(ctx, key, outDir, rtmpConn, destinations)
	return nil
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

	// VIDEO DISABLED: all accepted RTMP input is audio-only. This preserves
	// personal and shared radio keys while dropping any incoming video track.
	isRadio := true

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

	// Check if streaming is disabled for this user due to listener limit
	userID := getUserIDFromStreamKey(key)
	if userID != "" && isStreamingDisabled(userID) {
		log.Printf("[rtmp/%s] Rejected: streaming disabled for user (listener limit exceeded)", key)
		conn.Close()
		return
	}

	// Look up multistream destinations for this key.
	destinations := getDestinationsForKey(key)
	if len(destinations) > 0 {
		log.Printf("[rtmp/%s] Multistreaming to %d destination(s)", key, len(destinations))
	}

	// Reconstruct a reader that includes the peeked bytes + remaining conn.
	combined := io.MultiReader(strings.NewReader(string(buf[:n])), conn)

	// Wrap back into a net.Conn-like reader the transcode pipeline can use.
	if err := sm.start(key, &peekedConn{Reader: combined, Conn: conn}, destinations); err != nil {
		log.Printf("[rtmp/%s] Rejected duplicate publisher: %v", key, err)
		conn.Close()
		return
	}
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
		CheckOrigin:     func(r *http.Request) bool { return isAllowedOrigin(r.Header.Get("Origin")) },
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
	Role        string `json:"role"`
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
			role          TEXT NOT NULL DEFAULT 'user',
			created_at    TEXT NOT NULL
		)
	`)
	if err != nil {
		return err
	}
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS pending_registrations (
			email          TEXT PRIMARY KEY,
			password_hash  TEXT NOT NULL,
			first_name     TEXT NOT NULL DEFAULT '',
			last_name      TEXT NOT NULL DEFAULT '',
			station_name   TEXT NOT NULL DEFAULT '',
			logo_url       TEXT NOT NULL DEFAULT '',
			genre          TEXT NOT NULL DEFAULT '',
			description    TEXT NOT NULL DEFAULT '',
			otp_code       TEXT NOT NULL,
			otp_expires_at TEXT NOT NULL,
			created_at     TEXT NOT NULL
		)
	`)
	if err != nil {
		return err
	}
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS support_messages (
			id TEXT PRIMARY KEY,
			email TEXT NOT NULL,
			reason TEXT NOT NULL,
			message TEXT NOT NULL,
			station TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL DEFAULT 'open',
			admin_reply TEXT NOT NULL DEFAULT '',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			replied_at TIMESTAMPTZ
		)
	`)
	if err != nil {
		return err
	}
	_, _ = db.Exec(`ALTER TABLE support_messages ADD COLUMN IF NOT EXISTS station TEXT NOT NULL DEFAULT ''`)
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
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS schedules (
			id           TEXT PRIMARY KEY,
			user_id      TEXT NOT NULL,
			song_id      TEXT NOT NULL,
			title        TEXT NOT NULL,
			artist       TEXT NOT NULL DEFAULT '',
			source_type  TEXT NOT NULL DEFAULT 'library',
			source_url   TEXT NOT NULL DEFAULT '',
			playlist     JSONB NOT NULL DEFAULT '[]'::jsonb,
			trigger_time TIMESTAMPTZ NOT NULL,
			enabled      BOOLEAN NOT NULL DEFAULT true,
			recurring    TEXT NOT NULL DEFAULT 'none',
			triggered    BOOLEAN NOT NULL DEFAULT false,
			created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
			CHECK (recurring IN ('none', 'daily', 'weekly', 'monthly', 'yearly')),
			CHECK (source_type IN ('library', 'url', 'playlist'))
		)
	`)
	if err != nil {
		return err
	}
	_, _ = db.Exec(`ALTER TABLE schedules ADD COLUMN IF NOT EXISTS source_type TEXT NOT NULL DEFAULT 'library'`)
	_, _ = db.Exec(`ALTER TABLE schedules ADD COLUMN IF NOT EXISTS source_url TEXT NOT NULL DEFAULT ''`)
	_, _ = db.Exec(`ALTER TABLE schedules ADD COLUMN IF NOT EXISTS playlist JSONB NOT NULL DEFAULT '[]'::jsonb`)
	_, _ = db.Exec(`ALTER TABLE schedules ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT ''`)
	_, _ = db.Exec(`ALTER TABLE schedules DROP CONSTRAINT IF EXISTS schedules_recurring_check`)
	_, _ = db.Exec(`ALTER TABLE schedules ADD CONSTRAINT schedules_recurring_check CHECK (recurring IN ('none', 'daily', 'weekly', 'monthly', 'yearly'))`)
	_, _ = db.Exec(`ALTER TABLE schedules DROP CONSTRAINT IF EXISTS schedules_source_type_check`)
	_, _ = db.Exec(`ALTER TABLE schedules ADD CONSTRAINT schedules_source_type_check CHECK (source_type IN ('library', 'url', 'playlist'))`)
	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_schedules_due ON schedules(enabled, triggered, trigger_time)`)
	if err != nil {
		return err
	}
	// Listener analytics tables.
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS listener_sessions (
			id               TEXT PRIMARY KEY,
			user_id          TEXT NOT NULL,
			mount            TEXT NOT NULL,
			ip_hash          TEXT NOT NULL,
			country_code     TEXT NOT NULL DEFAULT 'XX',
			country_name     TEXT NOT NULL DEFAULT 'Unknown',
			started_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			ended_at         TIMESTAMPTZ,
			connected_secs   INTEGER NOT NULL DEFAULT 0,
			FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
		)
	`)
	if err != nil {
		return err
	}
	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_ls_user_started ON listener_sessions(user_id, started_at)`)
	if err != nil {
		return err
	}
	// Migrate: add columns that may be missing if the table was created by an older build.
	for _, col := range []struct{ name, def string }{
		{"country_code", "TEXT NOT NULL DEFAULT 'XX'"},
		{"country_name", "TEXT NOT NULL DEFAULT 'Unknown'"},
		{"ended_at", "TIMESTAMPTZ"},
		{"connected_secs", "INTEGER NOT NULL DEFAULT 0"},
		{"last_seen_at", "TIMESTAMPTZ NOT NULL DEFAULT NOW()"},
	} {
		_, _ = db.Exec(`ALTER TABLE listener_sessions ADD COLUMN IF NOT EXISTS ` + col.name + ` ` + col.def)
	}
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS listener_hourly (
			user_id        TEXT NOT NULL,
			hour_bucket    TIMESTAMPTZ NOT NULL,
			max_concurrent INTEGER NOT NULL DEFAULT 0,
			unique_ips     INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (user_id, hour_bucket)
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
		`ALTER TABLE users ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'user'`,
		`ALTER TABLE stations ADD COLUMN IF NOT EXISTS source_password TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE stations ADD COLUMN IF NOT EXISTS icecast_listen_url TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE stations ADD COLUMN IF NOT EXISTS plan TEXT NOT NULL DEFAULT 'starter'`,
		`ALTER TABLE stations ADD COLUMN IF NOT EXISTS billing_cycle TEXT NOT NULL DEFAULT 'monthly'`,
		`ALTER TABLE stations ADD COLUMN IF NOT EXISTS is_suspended BOOLEAN NOT NULL DEFAULT false`,
		`ALTER TABLE stations ADD COLUMN IF NOT EXISTS paypal_subscription_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE stations ADD COLUMN IF NOT EXISTS stripe_subscription_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE stations ADD COLUMN IF NOT EXISTS trial_used BOOLEAN NOT NULL DEFAULT false`,
		`ALTER TABLE stations ADD COLUMN IF NOT EXISTS trial_started_at TIMESTAMPTZ`,
		`ALTER TABLE stations ADD COLUMN IF NOT EXISTS trial_ends_at TIMESTAMPTZ`,
		`ALTER TABLE users ADD COLUMN IF NOT EXISTS otp_code TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE users ADD COLUMN IF NOT EXISTS otp_expires_at TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE users ADD COLUMN IF NOT EXISTS is_email_verified BOOLEAN NOT NULL DEFAULT false`,
		// Mark pre-existing accounts (no OTP code = registered before email verification was added) as verified
		`UPDATE users SET is_email_verified = true WHERE otp_code = '' AND is_email_verified = false`,
		`ALTER TABLE users ADD COLUMN IF NOT EXISTS reset_token TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE users ADD COLUMN IF NOT EXISTS reset_expires_at TEXT NOT NULL DEFAULT ''`,
	} {
		if _, err = db.Exec(migration); err != nil {
			return err
		}
	}
	for _, migration := range []string{
		`ALTER TABLE pending_registrations ADD COLUMN IF NOT EXISTS first_name TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE pending_registrations ADD COLUMN IF NOT EXISTS last_name TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE pending_registrations ADD COLUMN IF NOT EXISTS station_name TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE pending_registrations ADD COLUMN IF NOT EXISTS logo_url TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE pending_registrations ADD COLUMN IF NOT EXISTS genre TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE pending_registrations ADD COLUMN IF NOT EXISTS description TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE pending_registrations ADD COLUMN IF NOT EXISTS otp_code TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE pending_registrations ADD COLUMN IF NOT EXISTS otp_expires_at TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE pending_registrations ADD COLUMN IF NOT EXISTS created_at TEXT NOT NULL DEFAULT ''`,
	} {
		if _, err = db.Exec(migration); err != nil {
			return err
		}
	}
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS package_plans (
			id                 TEXT PRIMARY KEY,
			display_name       TEXT NOT NULL,
			monthly_price_cents INTEGER NOT NULL DEFAULT 0,
			yearly_price_cents  INTEGER NOT NULL DEFAULT 0,
			listener_limit      INTEGER NOT NULL DEFAULT 0,
			channel_limit       INTEGER NOT NULL DEFAULT 0,
			created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return err
	}
	for _, migration := range []string{
		`ALTER TABLE package_plans ADD COLUMN IF NOT EXISTS display_name TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE package_plans ADD COLUMN IF NOT EXISTS monthly_price_cents INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE package_plans ADD COLUMN IF NOT EXISTS yearly_price_cents INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE package_plans ADD COLUMN IF NOT EXISTS listener_limit INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE package_plans ADD COLUMN IF NOT EXISTS channel_limit INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE package_plans ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
	} {
		if _, err = db.Exec(migration); err != nil {
			return err
		}
	}
	_, err = db.Exec(`
		INSERT INTO package_plans (id, display_name, monthly_price_cents, yearly_price_cents, listener_limit, channel_limit)
		VALUES
			('starter', 'Starter', 2900, 29000, 500, 0),
			('professional', 'Professional', 3900, 39000, 1000, 0),
			('enterprise', 'Enterprise', 5900, 59000, 2000, 3),
			('ultimate', 'Ultimate', 9900, 99000, 999999, 6)
		ON CONFLICT (id) DO UPDATE SET
			display_name = EXCLUDED.display_name,
			monthly_price_cents = EXCLUDED.monthly_price_cents,
			yearly_price_cents = EXCLUDED.yearly_price_cents,
			listener_limit = EXCLUDED.listener_limit,
			channel_limit = EXCLUDED.channel_limit
	`)
	if err != nil {
		return err
	}

	// Add new columns for admin management (safe to run multiple times)
	_, _ = db.Exec(`ALTER TABLE package_plans ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT ''`)
	_, _ = db.Exec(`ALTER TABLE package_plans ADD COLUMN IF NOT EXISTS monthly_price NUMERIC(10,2) NOT NULL DEFAULT 0`)
	_, _ = db.Exec(`ALTER TABLE package_plans ADD COLUMN IF NOT EXISTS yearly_price NUMERIC(10,2) NOT NULL DEFAULT 0`)
	_, _ = db.Exec(`ALTER TABLE package_plans ADD COLUMN IF NOT EXISTS features JSONB NOT NULL DEFAULT '[]'::jsonb`)
	_, _ = db.Exec(`ALTER TABLE package_plans ADD COLUMN IF NOT EXISTS sale_percent INTEGER NOT NULL DEFAULT 0`)
	_, _ = db.Exec(`ALTER TABLE package_plans ADD COLUMN IF NOT EXISTS monthly_sale_percent INTEGER NOT NULL DEFAULT 0`)
	_, _ = db.Exec(`ALTER TABLE package_plans ADD COLUMN IF NOT EXISTS yearly_sale_percent INTEGER NOT NULL DEFAULT 0`)
	_, _ = db.Exec(`ALTER TABLE package_plans ADD COLUMN IF NOT EXISTS is_featured BOOLEAN NOT NULL DEFAULT false`)
	_, _ = db.Exec(`ALTER TABLE package_plans ADD COLUMN IF NOT EXISTS paypal_plan_id_monthly TEXT NOT NULL DEFAULT ''`)
	_, _ = db.Exec(`ALTER TABLE package_plans ADD COLUMN IF NOT EXISTS paypal_plan_id_yearly TEXT NOT NULL DEFAULT ''`)
	_, _ = db.Exec(`ALTER TABLE package_plans ADD COLUMN IF NOT EXISTS trial_enabled BOOLEAN NOT NULL DEFAULT false`)

	// Sync pricing data to new columns
	_, _ = db.Exec(`
		UPDATE package_plans 
		SET name = id, 
		    monthly_price = monthly_price_cents / 100.0, 
		    yearly_price = yearly_price_cents / 100.0
		WHERE monthly_price = 0 OR yearly_price = 0
	`)
	_, err = db.Exec(`
		WITH defaults(id, display_name, features) AS (
			VALUES
				('starter', 'Starter', jsonb_build_array(
					'Radio DJ & Mixer',
					'96 kbps audio streaming',
					'Custom stream URL',
					'Embeddable player widget',
					'Listeners analytics',
					'Up to 500 concurrent listeners',
					'Record sessions'
				)),
				('professional', 'Professional', jsonb_build_array(
					'Everything in Starter',
					'96 or 128 kbps audio streaming',
					'Conference live-chat rooms',
					'Up to 10 participants',
					'Priority audio processing',
					'Up to 1000 concurrent listeners'
				)),
				('enterprise', 'Enterprise', jsonb_build_array(
					'Everything in Professional',
					'96, 128 or 192 kbps audio streaming',
					'Advanced listener reporting',
					'Priority station support',
					'Up to 2000 concurrent listeners'
				)),
				('ultimate', 'Ultimate', jsonb_build_array(
					'Everything in Enterprise',
					'96, 128, 192 or 320 kbps audio streaming',
					'Premium radio automation',
					'Advanced analytics dashboard',
					'Custom branding options',
					'Unlimited concurrent listeners'
				))
		)
		UPDATE package_plans p
		SET
			name = CASE WHEN p.name = '' OR p.name = p.id THEN d.display_name ELSE p.name END,
			display_name = CASE WHEN p.display_name = '' OR p.display_name = p.id THEN d.display_name ELSE p.display_name END,
			features = CASE WHEN p.features = '[]'::jsonb THEN d.features ELSE p.features END
		FROM defaults d
		WHERE p.id = d.id
	`)
	if err != nil {
		return err
	}

	// Force-update plan features and is_featured to current authoritative values.
	_, _ = db.Exec(`
		UPDATE package_plans SET
			features = CASE id
				WHEN 'starter' THEN jsonb_build_array(
					'Radio DJ & Mixer',
					'96 kbps audio streaming',
					'Custom stream URL',
					'Embeddable player widget',
					'Listeners analytics',
					'Up to 500 concurrent listeners',
					'Record sessions'
				)
				WHEN 'professional' THEN jsonb_build_array(
					'Everything in Starter',
					'96 or 128 kbps audio streaming',
					'Track Scheduler',
					'Conference rooms (up to 2 guests)',
					'Priority audio processing',
					'Up to 1,000 concurrent listeners'
				)
				WHEN 'enterprise' THEN jsonb_build_array(
					'Everything in Professional',
					'Conference rooms (up to 5 guests)',
					'96, 128 or 192 kbps audio streaming',
					'Advanced listener reporting',
					'Priority station support',
					'Up to 2,000 concurrent listeners'
				)
				WHEN 'ultimate' THEN jsonb_build_array(
					'Everything in Enterprise',
					'96, 128, 192 or 320 kbps audio streaming',
					'Conference rooms (up to 20 guests)',
					'Premium radio automation',
					'Advanced analytics dashboard',
					'Custom branding options',
					'Unlimited concurrent listeners'
				)
				ELSE features
			END,
			is_featured = (id = 'professional')
		WHERE id IN ('starter','professional','enterprise','ultimate')
	`)

	// Create marketing content table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS marketing_content (
			id          TEXT PRIMARY KEY,
			page        TEXT NOT NULL,
			section     TEXT NOT NULL,
			content_type TEXT NOT NULL,
			content     TEXT NOT NULL,
			is_active   BOOLEAN NOT NULL DEFAULT true,
			created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS package_upgrade_history (
			id                TEXT PRIMARY KEY,
			user_id           TEXT NOT NULL,
			old_plan          TEXT NOT NULL DEFAULT '',
			new_plan          TEXT NOT NULL,
			old_billing_cycle TEXT NOT NULL DEFAULT '',
			new_billing_cycle TEXT NOT NULL,
			status            TEXT NOT NULL DEFAULT 'active',
			payment_reference TEXT NOT NULL DEFAULT '',
			created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
		)
	`)
	if err != nil {
		return err
	}
	// OAuth platform connections table.
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS oauth_connections (
			id            TEXT PRIMARY KEY,
			user_id       TEXT NOT NULL,
			platform      TEXT NOT NULL,
			access_token  TEXT NOT NULL,
			refresh_token TEXT NOT NULL DEFAULT '',
			expires_at    TIMESTAMPTZ,
			scope         TEXT NOT NULL DEFAULT '',
			connected_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
			UNIQUE(user_id, platform)
		)
	`)
	if err != nil {
		return err
	}

	// ── Advertising Platform Tables ───────────────────────────────────────
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS ad_placements (
			id          TEXT PRIMARY KEY,
			name        TEXT NOT NULL,
			description TEXT NOT NULL DEFAULT '',
			placement   TEXT NOT NULL,
			width       INT NOT NULL DEFAULT 0,
			height      INT NOT NULL DEFAULT 0,
			base_price  DECIMAL(10,2) NOT NULL DEFAULT 0,
			active      BOOLEAN NOT NULL DEFAULT true,
			created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS ad_campaigns (
			id              TEXT PRIMARY KEY,
			placement_id    TEXT NOT NULL,
			advertiser_name TEXT NOT NULL,
			target_url      TEXT NOT NULL DEFAULT '',
			asset_type      TEXT NOT NULL,
			asset_url       TEXT NOT NULL DEFAULT '',
			asset_name      TEXT NOT NULL DEFAULT '',
			price           DECIMAL(10,2) NOT NULL DEFAULT 0,
			original_price  DECIMAL(10,2) NOT NULL DEFAULT 0,
			discount_percent INT NOT NULL DEFAULT 0,
			status          TEXT NOT NULL DEFAULT 'draft',
			impressions     BIGINT NOT NULL DEFAULT 0,
			clicks          BIGINT NOT NULL DEFAULT 0,
			started_at      TIMESTAMPTZ,
			ended_at        TIMESTAMPTZ,
			created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			FOREIGN KEY (placement_id) REFERENCES ad_placements(id) ON DELETE CASCADE
		)
	`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS ad_analytics (
			id          BIGSERIAL PRIMARY KEY,
			campaign_id TEXT NOT NULL,
			event_type  TEXT NOT NULL,
			ip_address  TEXT NOT NULL DEFAULT '',
			user_agent  TEXT NOT NULL DEFAULT '',
			country     TEXT NOT NULL DEFAULT '',
			created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			FOREIGN KEY (campaign_id) REFERENCES ad_campaigns(id) ON DELETE CASCADE
		)
	`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_ad_analytics_campaign ON ad_analytics(campaign_id, created_at DESC)
	`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_ad_campaigns_placement ON ad_campaigns(placement_id, status)
	`)
	if err != nil {
		return err
	}

	// ── Schema migrations (idempotent) ─────────────────────────────────────
	// Add label column to destinations if not present (Stage 5).
	_, _ = db.Exec(`ALTER TABLE destinations ADD COLUMN IF NOT EXISTS label TEXT NOT NULL DEFAULT ''`)
	// Add serverUrl alias column so we can store RTMP base URL directly.
	_, _ = db.Exec(`ALTER TABLE destinations ADD COLUMN IF NOT EXISTS server_url TEXT NOT NULL DEFAULT ''`)
	// Drop old UNIQUE(user_id, platform) to allow multiple destinations per platform.
	_, _ = db.Exec(`ALTER TABLE destinations DROP CONSTRAINT IF EXISTS destinations_user_id_platform_key`)
	// Enforce unique station names (case-insensitive), ignoring blank names.
	_, _ = db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_stations_lower_name ON stations (lower(station_name)) WHERE station_name != ''`)

	// ── Seed Default Ad Placements ────────────────────────────────────────
	defaultPlacements := []struct {
		ID          string
		Name        string
		Description string
		Placement   string
		Width       int
		Height      int
		BasePrice   float64
	}{
		{"player-overlay", "Radio Player Video Overlay", "Video overlay displayed on the player during streaming", "player-overlay", 640, 360, 150.00},
		{"header-banner", "Global Webpage Header Banner", "Horizontal banner displayed at the top of all pages", "header-banner", 728, 90, 110.00},
		{"sidebar", "Listener Directory Sidebar", "Square banner in the sidebar of the listener pages", "sidebar", 300, 250, 100.00},
		{"audio-pre", "Audio Stream Pre-roll", "Audio advertisement played before stream starts", "audio-pre", 0, 0, 80.00},
	}

	for _, p := range defaultPlacements {
		_, _ = db.Exec(`
			INSERT INTO ad_placements (id, name, description, placement, width, height, base_price)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (id) DO NOTHING
		`, p.ID, p.Name, p.Description, p.Placement, p.Width, p.Height, p.BasePrice)
	}

	return nil
}

// ─── Analytics — GeoIP + Icecast Poller ──────────────────────────────────────

var geoipDB *geoip2.Reader

func initGeoIP() {
	path := os.Getenv("GEOIP_DB_PATH")
	if path == "" {
		log.Printf("[geoip] GEOIP_DB_PATH not set — country resolution disabled")
		log.Printf("[geoip] Download GeoLite2-Country.mmdb from https://dev.maxmind.com/geoip/geolite2-free-geolocation-data")
		return
	}
	var err error
	geoipDB, err = geoip2.Open(path)
	if err != nil {
		log.Printf("[geoip] Failed to open %s: %v — country resolution disabled", path, err)
		return
	}
	log.Printf("[geoip] Loaded GeoIP database from %s", path)
}

func resolveCountry(ipStr string) (code, name string) {
	code, name = "XX", "Unknown"
	if geoipDB == nil {
		return
	}
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return
	}
	// Private / loopback addresses → mark as Local.
	for _, cidr := range []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "127.0.0.0/8", "::1/128", "fc00::/7"} {
		_, block, _ := net.ParseCIDR(cidr)
		if block != nil && block.Contains(ip) {
			return "LC", "Local"
		}
	}
	record, err := geoipDB.Country(ip)
	if err != nil || record.Country.IsoCode == "" {
		return
	}
	code = record.Country.IsoCode
	if n, ok := record.Country.Names["en"]; ok {
		name = n
	}
	return
}

func hashIP(ip string) string {
	h := sha256.Sum256([]byte(ip))
	return hex.EncodeToString(h[:])
}

func clientIP(r *http.Request) string {
	if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
		if first := strings.TrimSpace(strings.Split(forwarded, ",")[0]); first != "" {
			return first
		}
	}
	if realIP := strings.TrimSpace(r.Header.Get("X-Real-IP")); realIP != "" {
		return realIP
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil && host != "" {
		return host
	}
	return r.RemoteAddr
}

func icecastListenerCountForUser(userID string) int {
	activeSessionsMu.Lock()
	defer activeSessionsMu.Unlock()
	return len(activeSessions[userID])
}

func webListenerCountForUser(userID string) int {
	webListenerSessionsMu.Lock()
	defer webListenerSessionsMu.Unlock()
	n := 0
	for _, sess := range webListenerSessions {
		if sess.userID == userID {
			n++
		}
	}
	return n
}

func totalLiveListenerCount(userID string) int {
	return icecastListenerCountForUser(userID) + webListenerCountForUser(userID)
}

// Listener limits per plan
func getListenerLimit(plan string) int {
	switch plan {
	case "starter":
		return 500
	case "professional":
		return 1000
	case "enterprise":
		return 2000
	case "ultimate":
		return 999999 // effectively unlimited
	default:
		return 500 // default to starter
	}
}

// Get user's plan from database
func getUserPlan(userID string) string {
	var plan string
	err := db.QueryRow(`SELECT plan FROM stations WHERE user_id = $1`, userID).Scan(&plan)
	if err != nil || plan == "" {
		return "starter" // default
	}
	return plan
}

// Check if user is at or over their listener limit
func isAtListenerLimit(userID string) bool {
	plan := getUserPlan(userID)
	limit := getListenerLimit(plan)
	current := totalLiveListenerCount(userID)
	return current >= limit
}

// Check if user exceeded limit by more than 5 (streaming disabled)
func isStreamingDisabled(userID string) bool {
	plan := getUserPlan(userID)
	limit := getListenerLimit(plan)
	current := totalLiveListenerCount(userID)
	return current > limit+5
}

// Get user ID from stream key
func getUserIDFromStreamKey(streamKey string) string {
	if db == nil {
		return ""
	}
	var userID string
	err := db.QueryRow(`SELECT id FROM users WHERE stream_key = $1`, streamKey).Scan(&userID)
	if err != nil {
		return ""
	}
	return userID
}

func syncLiveListenerCount(userID string) {
	if userID == "" {
		return
	}
	count := totalLiveListenerCount(userID)
	_, _ = db.Exec(`UPDATE stations SET current_listeners_count = $1 WHERE user_id = $2`, count, userID)
}

func registerWebListener(userID, slug, ip string, ttl time.Duration) (string, error) {
	// Check listener limit before allowing new connection
	if isAtListenerLimit(userID) {
		return "", errors.New("listener capacity reached")
	}

	sessionID, err := generateKey()
	if err != nil {
		return "", err
	}
	ipHash := hashIP(ip)
	countryCode, countryName := resolveCountry(ip)
	now := time.Now()
	sess := &webListenerSession{
		sessionID:   sessionID,
		userID:      userID,
		slug:        slug,
		ipHash:      ipHash,
		countryCode: countryCode,
		countryName: countryName,
		startedAt:   now,
	}
	if ttl > 0 {
		sess.expiresAt = now.Add(ttl)
	}
	webListenerSessionsMu.Lock()
	webListenerSessions[sessionID] = sess
	webListenerSessionsMu.Unlock()
	if _, dbErr := db.Exec(`
		INSERT INTO listener_sessions (id, user_id, mount, ip_hash, country_code, country_name, started_at, last_seen_at, connected_secs)
		VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW(), 0)
		ON CONFLICT (id) DO NOTHING
	`, sessionID, userID, "/web/"+slug, ipHash, countryCode, countryName); dbErr != nil {
		log.Printf("[db] listener_sessions INSERT failed (web): %v", dbErr)
	}
	syncLiveListenerCount(userID)
	return sessionID, nil
}

func touchWebListener(sessionID string, ttl time.Duration) bool {
	webListenerSessionsMu.Lock()
	sess, ok := webListenerSessions[sessionID]
	if ok && ttl > 0 {
		sess.expiresAt = time.Now().Add(ttl)
	}
	connSecs := 0
	if ok {
		connSecs = int(time.Since(sess.startedAt).Seconds())
	}
	webListenerSessionsMu.Unlock()
	if ok {
		go db.Exec(`UPDATE listener_sessions SET last_seen_at = NOW(), connected_secs = $1 WHERE id = $2`, connSecs, sessionID) //nolint:errcheck
	}
	return ok
}

func webListenerMatches(sessionID, slug string) bool {
	webListenerSessionsMu.Lock()
	defer webListenerSessionsMu.Unlock()
	sess, ok := webListenerSessions[sessionID]
	return ok && sess.slug == slug
}

func unregisterWebListener(sessionID string) {
	webListenerSessionsMu.Lock()
	sess, ok := webListenerSessions[sessionID]
	if ok {
		delete(webListenerSessions, sessionID)
	}
	webListenerSessionsMu.Unlock()
	if ok {
		connSecs := int(time.Since(sess.startedAt).Seconds())
		_, _ = db.Exec(`UPDATE listener_sessions SET ended_at = NOW(), last_seen_at = NOW(), connected_secs = $1 WHERE id = $2`, connSecs, sessionID)
		syncLiveListenerCount(sess.userID)
	}
}

func startWebListenerCleanup() {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			now := time.Now()
			affected := map[string]bool{}
			expired := map[string]int{}
			webListenerSessionsMu.Lock()
			for id, sess := range webListenerSessions {
				if !sess.expiresAt.IsZero() && now.After(sess.expiresAt) {
					affected[sess.userID] = true
					expired[id] = int(now.Sub(sess.startedAt).Seconds())
					delete(webListenerSessions, id)
				}
			}
			webListenerSessionsMu.Unlock()
			for id, connSecs := range expired {
				_, _ = db.Exec(`UPDATE listener_sessions SET ended_at = NOW(), last_seen_at = NOW(), connected_secs = $1 WHERE id = $2`, connSecs, id)
			}
			for userID := range affected {
				syncLiveListenerCount(userID)
			}
		}
	}()
}

// icecastXML models the /admin/listclients XML response.
type icecastXML struct {
	Sources []icecastSource `xml:"source"`
}
type icecastSource struct {
	Mount     string            `xml:"mount,attr"`
	Listeners []icecastListener `xml:"listener"`
}
type icecastListener struct {
	IP        string `xml:"IP"`
	Connected int    `xml:"Connected"` // seconds connected
}

// activeSession tracks an open listener session in memory.
type activeSession struct {
	sessionID   string
	ipHash      string
	countryCode string
	countryName string
	startedAt   time.Time
}

// webListenerSession tracks listeners served through the app player/HLS paths.
type webListenerSession struct {
	sessionID   string
	userID      string
	slug        string
	ipHash      string
	countryCode string
	countryName string
	startedAt   time.Time
	expiresAt   time.Time // zero means tied to an open HTTP stream
}

var (
	// activeSessions: userID → ipHash → session
	activeSessions   = map[string]map[string]*activeSession{}
	activeSessionsMu sync.Mutex

	webListenerSessions        = map[string]*webListenerSession{}
	webListenerSessionsMu      sync.Mutex
	icecastAnalyticsAuthFailed atomic.Bool
	icecastAnalyticsPollCount  atomic.Uint64
)

func startAnalyticsWorker() {
	icecastBase := os.Getenv("ICECAST_URL")
	if icecastBase == "" {
		// Fall back to ICECAST_HOST + ICECAST_PORT (set in docker-compose/Railway).
		host := os.Getenv("ICECAST_HOST")
		if host == "" {
			host = "localhost"
		}
		port := os.Getenv("ICECAST_PORT")
		if port == "" {
			port = "8000"
		}
		icecastBase = "http://" + host + ":" + port
	}
	icecastUser := os.Getenv("ICECAST_ADMIN_USER")
	if icecastUser == "" {
		icecastUser = "admin"
	}
	icecastPass := os.Getenv("ICECAST_ADMIN_PASSWORD")
	if icecastPass == "" {
		log.Printf("[analytics] Icecast poller disabled: ICECAST_ADMIN_PASSWORD is not set")
		return
	}

	client := &http.Client{Timeout: 5 * time.Second}

	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if icecastAnalyticsAuthFailed.Load() {
				return
			}
			if !pollAllMounts(client, icecastBase, icecastUser, icecastPass) {
				icecastAnalyticsAuthFailed.Store(true)
				return
			}
		}
	}()
	log.Printf("[analytics] Icecast poller started → %s (interval=15s)", icecastBase)
}

// pollAllMounts fetches live mounts from the DB and polls each one.
// We poll all stations (not just is_live=true) so a stale flag doesn't
// cause listener counts to silently stay at 0.
func pollAllMounts(client *http.Client, base, user, pass string) bool {
	rows, err := db.Query(`SELECT u.id, u.stream_key FROM users u
		JOIN stations s ON s.user_id = u.id WHERE u.stream_key IS NOT NULL AND u.stream_key <> ''`)
	if err != nil {
		log.Printf("[analytics] pollAllMounts DB error: %v", err)
		return true
	}
	defer rows.Close()

	type mountUser struct{ userID, streamKey string }
	var live []mountUser
	for rows.Next() {
		var mu mountUser
		if err := rows.Scan(&mu.userID, &mu.streamKey); err == nil {
			live = append(live, mu)
		}
	}
	if n := icecastAnalyticsPollCount.Add(1); n == 1 || n%20 == 0 {
		log.Printf("[analytics] polling %d mount(s) via %s", len(live), base)
	}

	// For users no longer live, close their open sessions.
	activeSessionsMu.Lock()
	liveSet := map[string]bool{}
	for _, mu := range live {
		liveSet[mu.userID] = true
	}
	for userID := range activeSessions {
		if !liveSet[userID] {
			closeAllSessions(userID)
		}
	}
	activeSessionsMu.Unlock()

	for _, mu := range live {
		if !pollMount(client, base, user, pass, mu.userID, mu.streamKey) {
			return false
		}
	}
	return true
}

func pollMount(client *http.Client, base, user, pass, userID, streamKey string) bool {
	mount := "/" + streamKey
	reqURL := base + "/admin/listclients?mount=" + url.QueryEscape(mount)
	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		log.Printf("[analytics] pollMount build request error: %v", err)
		return true
	}
	req.SetBasicAuth(user, pass)

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[analytics] pollMount GET %s error: %v", reqURL, err)
		return true
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		log.Printf("[analytics] Icecast poller disabled: admin credentials rejected with status %d", resp.StatusCode)
		return false
	}
	if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusNotFound {
		// Icecast returns 400/404 when a mount has no active source.
		// Treat this as offline (zero listeners), not as an error.
		activeSessionsMu.Lock()
		closeAllSessions(userID)
		activeSessionsMu.Unlock()
		syncLiveListenerCount(userID)
		return true
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("[analytics] pollMount GET %s status %d", reqURL, resp.StatusCode)
		activeSessionsMu.Lock()
		closeAllSessions(userID)
		activeSessionsMu.Unlock()
		syncLiveListenerCount(userID)
		return true
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("[analytics] pollMount read body error: %v", err)
		return true
	}

	var stats icecastXML
	if err := xml.Unmarshal(body, &stats); err != nil {
		log.Printf("[analytics] pollMount XML parse error: %v — body: %s", err, string(body))
		return true
	}
	log.Printf("[analytics] mount=%s sources=%d", mount, len(stats.Sources))

	// Collect current IPs from this poll.
	currentIPs := map[string]int{} // ipHash → connected_secs
	for _, src := range stats.Sources {
		for _, l := range src.Listeners {
			h := hashIP(l.IP)
			currentIPs[h] = l.Connected
			code, cname := resolveCountry(l.IP)
			upsertSession(userID, mount, h, code, cname, l.Connected)
		}
	}

	// Close sessions for IPs that vanished.
	activeSessionsMu.Lock()
	if sessions, ok := activeSessions[userID]; ok {
		for ipHash := range sessions {
			if _, still := currentIPs[ipHash]; !still {
				sessionID := sessions[ipHash].sessionID
				delete(sessions, ipHash)
				go db.Exec(`UPDATE listener_sessions SET ended_at = NOW() WHERE id = $1`, sessionID) //nolint:errcheck
			}
		}
	}
	activeSessionsMu.Unlock()

	// Update hourly stats + sync live count to stations table.
	concurrent := len(currentIPs)
	// Always sync so the count drops to 0 when all listeners disconnect,
	// while preserving web/HLS listeners counted outside Icecast.
	go syncLiveListenerCount(userID)
	if concurrent > 0 {
		hourBucket := time.Now().UTC().Truncate(time.Hour)
		db.Exec(` //nolint:errcheck
			INSERT INTO listener_hourly (user_id, hour_bucket, max_concurrent, unique_ips)
			VALUES ($1, $2, $3, $3)
			ON CONFLICT (user_id, hour_bucket) DO UPDATE
				SET max_concurrent = GREATEST(listener_hourly.max_concurrent, EXCLUDED.max_concurrent),
				    unique_ips     = GREATEST(listener_hourly.unique_ips, EXCLUDED.unique_ips)
		`, userID, hourBucket, concurrent)
	}
	return true
}

func upsertSession(userID, mount, ipHash, countryCode, countryName string, connSecs int) {
	activeSessionsMu.Lock()
	defer activeSessionsMu.Unlock()

	if activeSessions[userID] == nil {
		activeSessions[userID] = map[string]*activeSession{}
	}

	if sess, exists := activeSessions[userID][ipHash]; exists {
		// Update last_seen and connected_secs.
		go db.Exec(` //nolint:errcheck
			UPDATE listener_sessions SET last_seen_at = NOW(), connected_secs = $1 WHERE id = $2
		`, connSecs, sess.sessionID)
		return
	}

	// New session.
	sessionID, _ := generateKey()
	sess := &activeSession{
		sessionID:   sessionID,
		ipHash:      ipHash,
		countryCode: countryCode,
		countryName: countryName,
		startedAt:   time.Now(),
	}
	activeSessions[userID][ipHash] = sess
	go func() {
		if _, dbErr := db.Exec(`
			INSERT INTO listener_sessions (id, user_id, mount, ip_hash, country_code, country_name, started_at, last_seen_at, connected_secs)
			VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW(), $7)
			ON CONFLICT (id) DO NOTHING
		`, sessionID, userID, mount, ipHash, countryCode, countryName, 0); dbErr != nil {
			log.Printf("[db] listener_sessions INSERT failed (icecast): %v", dbErr)
		}
	}()
}

func closeAllSessions(userID string) {
	sessions := activeSessions[userID]
	for _, sess := range sessions {
		sid := sess.sessionID
		go db.Exec(`UPDATE listener_sessions SET ended_at = NOW() WHERE id = $1`, sid) //nolint:errcheck
	}
	delete(activeSessions, userID)
}

// ─── Analytics REST endpoint ──────────────────────────────────────────────────

type analyticsCountry struct {
	Code  string  `json:"code"`
	Name  string  `json:"name"`
	Count int     `json:"count"`
	Pct   float64 `json:"pct"`
}

type analyticsResponse struct {
	LiveCount       int                `json:"live_count"`
	DailySessions   int                `json:"daily_sessions"`
	MonthlySessions int                `json:"monthly_sessions"`
	TotalListeners  int                `json:"total_listeners"`
	AvgDurationSecs float64            `json:"avg_duration_secs"`
	Countries       []analyticsCountry `json:"countries"`
	ChartLabels     []string           `json:"chart_labels"`
	ChartData       []int              `json:"chart_data"`
	RawSample       json.RawMessage    `json:"raw_sample"`
}

func handleAnalytics(w http.ResponseWriter, r *http.Request) {
	userID, ok := r.Context().Value(contextKeyUserID).(string)
	if !ok || userID == "" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	// ── Live count from in-memory sessions ──────────────────────────────────
	activeSessionsMu.Lock()
	// Snapshot country counts from active sessions.
	countryCounts := map[string]struct {
		code, name string
		n          int
	}{}
	for _, sess := range activeSessions[userID] {
		e := countryCounts[sess.countryCode]
		e.code = sess.countryCode
		e.name = sess.countryName
		e.n++
		countryCounts[sess.countryCode] = e
	}
	activeSessionsMu.Unlock()
	webListenerSessionsMu.Lock()
	for _, sess := range webListenerSessions {
		if sess.userID != userID {
			continue
		}
		e := countryCounts[sess.countryCode]
		e.code = sess.countryCode
		e.name = sess.countryName
		e.n++
		countryCounts[sess.countryCode] = e
	}
	webListenerSessionsMu.Unlock()
	liveCount := totalLiveListenerCount(userID)

	now := time.Now().UTC()
	dayStart := now.Truncate(24 * time.Hour)
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	weeksAgo6 := now.AddDate(0, 0, -42)

	// ── Daily sessions ──────────────────────────────────────────────────────
	var dailySessions int
	db.QueryRow(`SELECT COUNT(DISTINCT ip_hash) FROM listener_sessions
		WHERE user_id = $1 AND started_at >= $2`, userID, dayStart).Scan(&dailySessions) //nolint:errcheck

	// ── Monthly sessions ────────────────────────────────────────────────────
	var monthlySessions int
	db.QueryRow(`SELECT COUNT(DISTINCT ip_hash) FROM listener_sessions
		WHERE user_id = $1 AND started_at >= $2`, userID, monthStart).Scan(&monthlySessions) //nolint:errcheck

	// ── Total unique listeners ──────────────────────────────────────────────
	var totalListeners int
	db.QueryRow(`SELECT COUNT(DISTINCT ip_hash) FROM listener_sessions
		WHERE user_id = $1`, userID).Scan(&totalListeners) //nolint:errcheck

	// ── Avg duration ────────────────────────────────────────────────────────
	var avgDuration float64
	db.QueryRow(`SELECT COALESCE(AVG(connected_secs), 0) FROM listener_sessions
		WHERE user_id = $1 AND connected_secs > 0`, userID).Scan(&avgDuration) //nolint:errcheck

	// ── Country breakdown (active sessions) ─────────────────────────────────
	// If no active sessions, fall back to today's DB rows.
	if len(countryCounts) == 0 {
		rows, err := db.Query(`SELECT country_code, country_name, COUNT(DISTINCT ip_hash)
			FROM listener_sessions WHERE user_id = $1 AND started_at >= $2
			GROUP BY country_code, country_name ORDER BY 3 DESC LIMIT 20`, userID, dayStart)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var code, name string
				var n int
				if rows.Scan(&code, &name, &n) == nil {
					countryCounts[code] = struct {
						code, name string
						n          int
					}{code, name, n}
				}
			}
		}
	}

	countries := make([]analyticsCountry, 0, len(countryCounts))
	total := 0
	for _, e := range countryCounts {
		total += e.n
	}
	for _, e := range countryCounts {
		pct := 0.0
		if total > 0 {
			pct = float64(e.n) / float64(total) * 100
		}
		countries = append(countries, analyticsCountry{Code: e.code, Name: e.name, Count: e.n, Pct: pct})
	}
	// Sort by count desc.
	for i := 0; i < len(countries); i++ {
		for j := i + 1; j < len(countries); j++ {
			if countries[j].Count > countries[i].Count {
				countries[i], countries[j] = countries[j], countries[i]
			}
		}
	}

	// ── 6-week chart (weekly unique listeners) ───────────────────────────────
	chartLabels := make([]string, 6)
	chartData := make([]int, 6)
	for i := 0; i < 6; i++ {
		wStart := weeksAgo6.AddDate(0, 0, i*7)
		wEnd := wStart.AddDate(0, 0, 7)
		label := wStart.Format("Jan 2")
		chartLabels[i] = label
		var n int
		db.QueryRow(`SELECT COUNT(DISTINCT ip_hash) FROM listener_sessions
			WHERE user_id = $1 AND started_at >= $2 AND started_at < $3`,
			userID, wStart, wEnd).Scan(&n) //nolint:errcheck
		chartData[i] = n
	}

	// ── Raw sample (latest 5 open sessions) ──────────────────────────────────
	type rawRow struct {
		CountryCode string    `json:"country_code"`
		CountryName string    `json:"country_name"`
		ConnSecs    int       `json:"connected_secs"`
		StartedAt   time.Time `json:"started_at"`
	}
	var rawRows []rawRow
	if rows, err := db.Query(`SELECT country_code, country_name, connected_secs, started_at
		FROM listener_sessions WHERE user_id = $1
		ORDER BY last_seen_at DESC LIMIT 5`, userID); err == nil {
		defer rows.Close()
		for rows.Next() {
			var rr rawRow
			if rows.Scan(&rr.CountryCode, &rr.CountryName, &rr.ConnSecs, &rr.StartedAt) == nil {
				rawRows = append(rawRows, rr)
			}
		}
	}
	rawJSON, _ := json.Marshal(rawRows)

	resp := analyticsResponse{
		LiveCount:       liveCount,
		DailySessions:   dailySessions,
		MonthlySessions: monthlySessions,
		TotalListeners:  totalListeners,
		AvgDurationSecs: avgDuration,
		Countries:       countries,
		ChartLabels:     chartLabels,
		ChartData:       chartData,
		RawSample:       json.RawMessage(rawJSON),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
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
	header []byte        // first chunk (WebM EBML + segment header)
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

// slugifyName converts a free-form name into a URL-safe slug component.
// Allowed characters: a-z, 0-9, dot (.). Everything else collapses to a
// single hyphen. Leading/trailing hyphens are stripped.
func slugifyName(source string) string {
	var b strings.Builder
	prevHyphen := false
	for _, r := range strings.ToLower(source) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '.' {
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
	return slug
}

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
	rb := make([]byte, 2)
	rand.Read(rb) //nolint:errcheck
	return slugifyName(source) + "-" + hex.EncodeToString(rb)
}

// ensureStation creates a station row for the user if one does not already exist.
// It is idempotent and safe to call on every login / credential fetch.
// stationName and logoURL are stored only on initial creation; pass "" on login paths.
type sqlQueryExec interface {
	QueryRow(query string, args ...any) *sql.Row
	Exec(query string, args ...any) (sql.Result, error)
}

func ensureStationWithDB(q sqlQueryExec, userID, email, stationName, logoURL string) (string, error) {
	var slug string
	err := q.QueryRow(`SELECT station_slug FROM stations WHERE user_id = $1`, userID).Scan(&slug)
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
	sourcePassword, err := generateKey()
	if err != nil {
		return "", err
	}
	_, err = q.Exec(
		`INSERT INTO stations (id, user_id, station_slug, station_name, logo_url, is_live, current_listeners_count, source_password)
		 VALUES ($1, $2, $3, $4, $5, false, 0, $6)
		 ON CONFLICT DO NOTHING`,
		stationID, userID, slug, stationName, logoURL, sourcePassword,
	)
	if err != nil {
		return "", err
	}
	return slug, nil
}

func ensureStation(userID, email, stationName, logoURL string) (string, error) {
	return ensureStationWithDB(db, userID, email, stationName, logoURL)
}

// jwtSign creates a signed JWT for the given user (30-day expiry).
func jwtSign(userID, email, stationName, role string) (string, error) {
	claims := Claims{
		UserID:      userID,
		Email:       email,
		StationName: stationName,
		Role:        role,
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

// requireAdmin middleware checks if the authenticated user has admin role
func requireAdmin(next http.HandlerFunc) http.HandlerFunc {
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
		if claims.Role != "admin" {
			http.Error(w, "forbidden: admin access required", http.StatusForbidden)
			return
		}
		ctx := context.WithValue(r.Context(), contextKeyUserID, claims.UserID)
		ctx = context.WithValue(ctx, contextKeyEmail, claims.Email)
		next.ServeHTTP(w, r.WithContext(ctx))
	}
}

// ── Auth rate limiting (in-memory, per-process) ─────────────────────────────
// Bounds OTP brute-forcing, OTP-resend spam, and password-guessing against
// login. Keyed by email — every one of these endpoints already targets a
// specific account, so per-account throttling is what actually matters here.
// State resets on process restart and isn't shared across instances; that's
// an accepted tradeoff for a single-process deployment rather than adding a
// Redis/DB dependency for this.

type authAttemptEntry struct {
	count   int
	firstAt time.Time
}

var (
	otpVerifyAttemptsMu sync.Mutex
	otpVerifyAttempts   = map[string]*authAttemptEntry{}

	otpResendAtMu sync.Mutex
	otpResendAt   = map[string]time.Time{}

	loginAttemptsMu  sync.Mutex
	loginAttemptsMap = map[string]*authAttemptEntry{}
)

const (
	otpVerifyMaxAttempts = 5
	otpVerifyWindow      = 15 * time.Minute
	otpResendCooldown    = 60 * time.Second
	loginMaxAttempts     = 8
	loginLockoutWindow   = 15 * time.Minute
)

// otpVerifyRateLimited records this attempt and reports whether email has
// exceeded its guess budget for the current OTP.
func otpVerifyRateLimited(email string) bool {
	otpVerifyAttemptsMu.Lock()
	defer otpVerifyAttemptsMu.Unlock()
	now := time.Now()
	entry := otpVerifyAttempts[email]
	if entry == nil || now.Sub(entry.firstAt) > otpVerifyWindow {
		entry = &authAttemptEntry{firstAt: now}
		otpVerifyAttempts[email] = entry
	}
	entry.count++
	return entry.count > otpVerifyMaxAttempts
}

func otpVerifyResetAttempts(email string) {
	otpVerifyAttemptsMu.Lock()
	delete(otpVerifyAttempts, email)
	otpVerifyAttemptsMu.Unlock()
}

// otpResendRateLimited reports whether email requested a resend too recently,
// and records this request as the new "last sent" time if not.
func otpResendRateLimited(email string) bool {
	otpResendAtMu.Lock()
	defer otpResendAtMu.Unlock()
	if last, ok := otpResendAt[email]; ok && time.Since(last) < otpResendCooldown {
		return true
	}
	otpResendAt[email] = time.Now()
	return false
}

// loginRateLimited reports whether email has too many recent failed logins.
func loginRateLimited(email string) bool {
	loginAttemptsMu.Lock()
	defer loginAttemptsMu.Unlock()
	entry := loginAttemptsMap[email]
	if entry == nil {
		return false
	}
	if time.Since(entry.firstAt) > loginLockoutWindow {
		delete(loginAttemptsMap, email)
		return false
	}
	return entry.count >= loginMaxAttempts
}

func loginRecordFailure(email string) {
	loginAttemptsMu.Lock()
	defer loginAttemptsMu.Unlock()
	now := time.Now()
	entry := loginAttemptsMap[email]
	if entry == nil || now.Sub(entry.firstAt) > loginLockoutWindow {
		entry = &authAttemptEntry{firstAt: now}
		loginAttemptsMap[email] = entry
	}
	entry.count++
}

func loginRecordSuccess(email string) {
	loginAttemptsMu.Lock()
	delete(loginAttemptsMap, email)
	loginAttemptsMu.Unlock()
}

// handleRegister creates a new user with an auto-generated stream key.
func generateOTP() (string, error) {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	n := (uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])) % 1000000
	return fmt.Sprintf("%06d", n), nil
}

type pendingRegistration struct {
	Email        string
	PasswordHash string
	FirstName    string
	LastName     string
	StationName  string
	LogoURL      string
	Genre        string
	Description  string
	OTPCode      string
	OTPExpiresAt string
}

func upsertPendingRegistration(p pendingRegistration) error {
	_, err := db.Exec(
		`INSERT INTO pending_registrations (email, password_hash, first_name, last_name, station_name, logo_url, genre, description, otp_code, otp_expires_at, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		 ON CONFLICT (email) DO UPDATE SET
		     password_hash = EXCLUDED.password_hash,
		     first_name = EXCLUDED.first_name,
		     last_name = EXCLUDED.last_name,
		     station_name = EXCLUDED.station_name,
		     logo_url = EXCLUDED.logo_url,
		     genre = EXCLUDED.genre,
		     description = EXCLUDED.description,
		     otp_code = EXCLUDED.otp_code,
		     otp_expires_at = EXCLUDED.otp_expires_at`,
		p.Email, p.PasswordHash, p.FirstName, p.LastName, p.StationName, p.LogoURL, p.Genre, p.Description, p.OTPCode, p.OTPExpiresAt, time.Now().UTC().Format(time.RFC3339),
	)
	return err
}

func createVerifiedUserFromPending(ctx context.Context, p pendingRegistration) (string, string, string, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return "", "", "", err
	}
	defer tx.Rollback()

	var userID, streamKey, role string
	var alreadyVerified bool
	err = tx.QueryRow(`SELECT id, stream_key, role, is_email_verified FROM users WHERE email = $1 FOR UPDATE`, p.Email).
		Scan(&userID, &streamKey, &role, &alreadyVerified)
	switch {
	case err == nil:
		if alreadyVerified {
			return "", "", "", fmt.Errorf("email already registered")
		}
		if streamKey == "" {
			streamKey, err = generateKey()
			if err != nil {
				return "", "", "", err
			}
		}
		if role == "" {
			role = "user"
		}
		_, err = tx.Exec(
			`UPDATE users
			    SET password_hash = $1,
			        stream_key = $2,
			        first_name = $3,
			        last_name = $4,
			        is_email_verified = true,
			        otp_code = '',
			        otp_expires_at = ''
			  WHERE id = $5`,
			p.PasswordHash, streamKey, p.FirstName, p.LastName, userID,
		)
		if err != nil {
			return "", "", "", err
		}
	case errors.Is(err, sql.ErrNoRows):
		userID, err = generateKey()
		if err != nil {
			return "", "", "", err
		}
		streamKey, err = generateKey()
		if err != nil {
			return "", "", "", err
		}
		_, err = tx.Exec(
			`INSERT INTO users (id, email, password_hash, stream_key, first_name, last_name, role, created_at, otp_code, otp_expires_at, is_email_verified)
			 VALUES ($1, $2, $3, $4, $5, $6, 'user', $7, '', '', true)`,
			userID, p.Email, p.PasswordHash, streamKey, p.FirstName, p.LastName, time.Now().UTC().Format(time.RFC3339),
		)
		if err != nil {
			return "", "", "", err
		}
	default:
		return "", "", "", err
	}

	stationSlug, err := ensureStationWithDB(tx, userID, p.Email, p.StationName, p.LogoURL)
	if err != nil {
		return "", "", "", err
	}
	_, err = tx.Exec(
		`UPDATE stations
		    SET is_suspended = true,
		        genre = $1,
		        description = $2,
		        station_name = CASE WHEN $3 <> '' THEN $3 ELSE station_name END,
		        logo_url = CASE WHEN $4 <> '' THEN $4 ELSE logo_url END
		  WHERE user_id = $5`,
		p.Genre, p.Description, p.StationName, p.LogoURL, userID,
	)
	if err != nil {
		return "", "", "", err
	}
	if _, err = tx.Exec(`DELETE FROM pending_registrations WHERE email = $1`, p.Email); err != nil {
		return "", "", "", err
	}
	if err = tx.Commit(); err != nil {
		return "", "", "", err
	}
	return userID, streamKey, stationSlug, nil
}

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
	if len(body.Password) < 8 {
		http.Error(w, "password must be at least 8 characters", http.StatusBadRequest)
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(body.Password), bcrypt.DefaultCost)
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	// Check if email already exists
	var existingID string
	var existingVerified bool
	_ = db.QueryRow(`SELECT id, is_email_verified FROM users WHERE email = $1`, body.Email).
		Scan(&existingID, &existingVerified)
	if existingID != "" {
		if existingVerified {
			http.Error(w, "email already registered", http.StatusConflict)
			return
		}
		// Unverified account — regenerate OTP and resend
		otp, err := generateOTP()
		if err != nil {
			http.Error(w, "server error", http.StatusInternalServerError)
			return
		}
		exp := time.Now().UTC().Add(15 * time.Minute).Format(time.RFC3339)
		_, _ = db.Exec(`UPDATE users SET password_hash = $1, first_name = $2, last_name = $3, otp_code = $4, otp_expires_at = $5 WHERE id = $6`, string(hash), body.FirstName, body.LastName, otp, exp, existingID)
		if body.StationName != "" || body.LogoURL != "" || body.Genre != "" || body.Description != "" {
			_, _ = ensureStation(existingID, body.Email, body.StationName, body.LogoURL)
			_, _ = db.Exec(
				`UPDATE stations
				    SET genre = $1,
				        description = $2,
				        station_name = CASE WHEN $3 <> '' THEN $3 ELSE station_name END,
				        logo_url = CASE WHEN $4 <> '' THEN $4 ELSE logo_url END
				  WHERE user_id = $5`,
				body.Genre, body.Description, body.StationName, body.LogoURL, existingID,
			)
		}
		otpVerifyResetAttempts(body.Email)
		if err := sendOTPEmail(body.Email, body.FirstName, otp); err != nil {
			log.Printf("[email] resend OTP to %s: %v", body.Email, err)
			http.Error(w, "failed to send verification email — please try again", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "verify_email", "email": body.Email})
		return
	}

	otp, err := generateOTP()
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	otpExp := time.Now().UTC().Add(15 * time.Minute).Format(time.RFC3339)
	err = upsertPendingRegistration(pendingRegistration{
		Email:        body.Email,
		PasswordHash: string(hash),
		FirstName:    body.FirstName,
		LastName:     body.LastName,
		StationName:  body.StationName,
		LogoURL:      body.LogoURL,
		Genre:        body.Genre,
		Description:  body.Description,
		OTPCode:      otp,
		OTPExpiresAt: otpExp,
	})
	if err != nil {
		log.Printf("[auth] register error: %v", err)
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	if err := sendOTPEmail(body.Email, body.FirstName, otp); err != nil {
		log.Printf("[email] send OTP to %s: %v", body.Email, err)
		http.Error(w, "failed to send verification email — please try again", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "verify_email", "email": body.Email})
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

	if loginRateLimited(body.Email) {
		http.Error(w, "too many failed attempts — please try again later", http.StatusTooManyRequests)
		return
	}

	var userID, passwordHash, streamKey, role string
	var isVerified bool
	err := db.QueryRow(
		`SELECT id, password_hash, stream_key, role, is_email_verified FROM users WHERE email = $1`, body.Email,
	).Scan(&userID, &passwordHash, &streamKey, &role, &isVerified)
	if errors.Is(err, sql.ErrNoRows) {
		loginRecordFailure(body.Email)
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
		return
	}
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	if err := bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(body.Password)); err != nil {
		loginRecordFailure(body.Email)
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
		return
	}
	loginRecordSuccess(body.Email)
	if !isVerified {
		http.Error(w, "email_not_verified", http.StatusForbidden)
		return
	}
	var stationName string
	_ = db.QueryRow(`SELECT station_name FROM stations WHERE user_id = $1`, userID).Scan(&stationName)
	token, err := jwtSign(userID, body.Email, stationName, role)
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

// POST /api/auth/verify-otp  {"email":"...","otp":"..."}
func handleVerifyOTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Email string `json:"email"`
		OTP   string `json:"otp"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	body.Email = strings.ToLower(strings.TrimSpace(body.Email))
	body.OTP = strings.TrimSpace(body.OTP)

	if otpVerifyRateLimited(body.Email) {
		http.Error(w, "too many attempts — request a new code", http.StatusTooManyRequests)
		return
	}

	var pending pendingRegistration
	err := db.QueryRow(
		`SELECT email, password_hash, first_name, last_name, station_name, logo_url, genre, description, otp_code, otp_expires_at
		   FROM pending_registrations
		  WHERE email = $1`,
		body.Email,
	).Scan(&pending.Email, &pending.PasswordHash, &pending.FirstName, &pending.LastName, &pending.StationName, &pending.LogoURL, &pending.Genre, &pending.Description, &pending.OTPCode, &pending.OTPExpiresAt)
	switch {
	case err == nil:
		if pending.OTPCode == "" || pending.OTPCode != body.OTP {
			http.Error(w, "invalid code", http.StatusUnauthorized)
			return
		}
		exp, _ := time.Parse(time.RFC3339, pending.OTPExpiresAt)
		if time.Now().UTC().After(exp) {
			http.Error(w, "code expired", http.StatusUnauthorized)
			return
		}
		otpVerifyResetAttempts(body.Email)
		userID, streamKey, stationSlug, err := createVerifiedUserFromPending(r.Context(), pending)
		if err != nil {
			if err.Error() == "email already registered" {
				http.Error(w, "email already registered", http.StatusConflict)
				return
			}
			http.Error(w, "server error", http.StatusInternalServerError)
			return
		}
		token, err := jwtSign(userID, body.Email, pending.StationName, "user")
		if err != nil {
			http.Error(w, "server error", http.StatusInternalServerError)
			return
		}
		go func() {
			if err := sendWelcomeEmail(body.Email, pending.FirstName); err != nil {
				log.Printf("[email] welcome to %s: %v", body.Email, err)
			}
		}()
		resp := map[string]string{
			"token":      token,
			"stream_key": streamKey,
			"rtmp_url":   rtmpIngestBase + "/" + streamKey,
		}
		if stationSlug != "" {
			resp["station_slug"] = stationSlug
			resp["listen_url"] = "/listen/" + stationSlug
			resp["hub_listen_url"] = "/listen/" + stationSlug
		}
		resp["icecast_listen_url"] = publicIcecastListenURL(streamKey)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
		return
	case err != nil && !errors.Is(err, sql.ErrNoRows):
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}

	var userID, storedOTP, otpExp, streamKey, firstName, stationName string
	err = db.QueryRow(
		`SELECT u.id, u.otp_code, u.otp_expires_at, u.stream_key, u.first_name,
		        COALESCE(s.station_name, '') FROM users u
		 LEFT JOIN stations s ON s.user_id = u.id
		 WHERE u.email = $1`, body.Email,
	).Scan(&userID, &storedOTP, &otpExp, &streamKey, &firstName, &stationName)
	if errors.Is(err, sql.ErrNoRows) {
		http.Error(w, "invalid code", http.StatusUnauthorized)
		return
	}
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	if storedOTP == "" || storedOTP != body.OTP {
		http.Error(w, "invalid code", http.StatusUnauthorized)
		return
	}
	exp, _ := time.Parse(time.RFC3339, otpExp)
	if time.Now().UTC().After(exp) {
		http.Error(w, "code expired", http.StatusUnauthorized)
		return
	}
	otpVerifyResetAttempts(body.Email)
	_, _ = db.Exec(`UPDATE users SET is_email_verified = true, otp_code = '', otp_expires_at = '' WHERE id = $1`, userID)
	token, err := jwtSign(userID, body.Email, stationName, "user")
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	stationSlug, _ := ensureStation(userID, body.Email, stationName, "")
	go func() {
		if err := sendWelcomeEmail(body.Email, firstName); err != nil {
			log.Printf("[email] welcome to %s: %v", body.Email, err)
		}
	}()
	resp := map[string]string{
		"token":      token,
		"stream_key": streamKey,
		"rtmp_url":   rtmpIngestBase + "/" + streamKey,
	}
	if stationSlug != "" {
		resp["station_slug"] = stationSlug
		resp["listen_url"] = "/listen/" + stationSlug
		resp["hub_listen_url"] = "/listen/" + stationSlug
	}
	resp["icecast_listen_url"] = publicIcecastListenURL(streamKey)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// POST /api/auth/resend-otp  {"email":"..."}
func handleResendOTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Email string `json:"email"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	body.Email = strings.ToLower(strings.TrimSpace(body.Email))
	var pendingEmail, firstName string
	err := db.QueryRow(`SELECT email, first_name FROM pending_registrations WHERE email = $1`, body.Email).
		Scan(&pendingEmail, &firstName)
	switch {
	case err == nil:
		if otpResendRateLimited(body.Email) {
			w.WriteHeader(http.StatusOK)
			return
		}
		otp, otpErr := generateOTP()
		if otpErr != nil {
			http.Error(w, "server error", http.StatusInternalServerError)
			return
		}
		exp := time.Now().UTC().Add(15 * time.Minute).Format(time.RFC3339)
		_, _ = db.Exec(`UPDATE pending_registrations SET otp_code = $1, otp_expires_at = $2 WHERE email = $3`, otp, exp, pendingEmail)
		otpVerifyResetAttempts(body.Email)
		if err := sendOTPEmail(body.Email, firstName, otp); err != nil {
			log.Printf("[email] resend OTP to %s: %v", body.Email, err)
			http.Error(w, "failed to send verification email — please try again", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	case err != nil && !errors.Is(err, sql.ErrNoRows):
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}

	var userID string
	var verified bool
	err = db.QueryRow(`SELECT id, first_name, is_email_verified FROM users WHERE email = $1`, body.Email).
		Scan(&userID, &firstName, &verified)
	if errors.Is(err, sql.ErrNoRows) || verified {
		// Always return 200 — don't leak account existence
		w.WriteHeader(http.StatusOK)
		return
	}
	if otpResendRateLimited(body.Email) {
		// Still return 200 (don't leak account existence via a differing
		// status code) — just skip sending another email so soon.
		w.WriteHeader(http.StatusOK)
		return
	}
	otp, err := generateOTP()
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	exp := time.Now().UTC().Add(15 * time.Minute).Format(time.RFC3339)
	_, _ = db.Exec(`UPDATE users SET otp_code = $1, otp_expires_at = $2 WHERE id = $3`, otp, exp, userID)
	otpVerifyResetAttempts(body.Email)
	if err := sendOTPEmail(body.Email, firstName, otp); err != nil {
		log.Printf("[email] resend OTP to %s: %v", body.Email, err)
		http.Error(w, "failed to send verification email — please try again", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// POST /api/auth/forgot-password  {"email":"..."}
func handleForgotPassword(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Email string `json:"email"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	body.Email = strings.ToLower(strings.TrimSpace(body.Email))
	log.Printf("[forgot-password] request for %s", body.Email)
	// Always return 200 — don't leak whether the email exists
	w.WriteHeader(http.StatusOK)

	var userID, firstName string
	err := db.QueryRow(`SELECT id, first_name FROM users WHERE email = $1`, body.Email).
		Scan(&userID, &firstName)
	if err != nil {
		log.Printf("[forgot-password] user not found for %s: %v", body.Email, err)
		return
	}
	log.Printf("[forgot-password] found user %s (%s), generating token", userID, body.Email)
	tokenBytes := make([]byte, 32)
	if _, err = rand.Read(tokenBytes); err != nil {
		log.Printf("[forgot-password] rand error: %v", err)
		return
	}
	resetToken := hex.EncodeToString(tokenBytes)
	exp := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	_, _ = db.Exec(`UPDATE users SET reset_token = $1, reset_expires_at = $2 WHERE id = $3`, resetToken, exp, userID)
	baseURL := os.Getenv("APP_BASE_URL")
	if baseURL == "" {
		baseURL = "https://radioinonestop.com"
	}
	resetLink := baseURL + "/reset-password?token=" + resetToken
	log.Printf("[forgot-password] sending reset email to %s link=%s", body.Email, resetLink)
	go func() {
		if err := sendPasswordResetEmail(body.Email, firstName, resetLink); err != nil {
			log.Printf("[forgot-password] SMTP error for %s: %v", body.Email, err)
		} else {
			log.Printf("[forgot-password] email sent OK to %s", body.Email)
		}
	}()
}

// POST /api/auth/reset-password  {"token":"...","password":"..."}
func handleResetPassword(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Token    string `json:"token"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	body.Token = strings.TrimSpace(body.Token)
	if len(body.Password) < 8 {
		http.Error(w, "password must be at least 8 characters", http.StatusBadRequest)
		return
	}
	var userID, resetExp string
	err := db.QueryRow(`SELECT id, reset_expires_at FROM users WHERE reset_token = $1 AND reset_token != ''`, body.Token).
		Scan(&userID, &resetExp)
	if errors.Is(err, sql.ErrNoRows) {
		http.Error(w, "invalid or expired reset link", http.StatusUnauthorized)
		return
	}
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	exp, _ := time.Parse(time.RFC3339, resetExp)
	if time.Now().UTC().After(exp) {
		http.Error(w, "invalid or expired reset link", http.StatusUnauthorized)
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(body.Password), bcrypt.DefaultCost)
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	_, _ = db.Exec(`UPDATE users SET password_hash = $1, reset_token = '', reset_expires_at = '' WHERE id = $2`, string(hash), userID)
	w.WriteHeader(http.StatusOK)
}

// expireTrialIfNeeded makes an expired unpaid trial use the same account gate
// as other suspended stations. Paid subscriptions are never expired by this.
func expireTrialIfNeeded(userID string) {
	_, _ = db.Exec(`
		UPDATE stations
		SET is_suspended = true
		WHERE user_id = $1
		  AND trial_ends_at IS NOT NULL
		  AND trial_ends_at <= NOW()
		  AND COALESCE(paypal_subscription_id, '') = ''
		  AND COALESCE(stripe_subscription_id, '') = ''
	`, userID)
}

func ensureStationProfileRow(userID, email, stationName, logoURL string) error {
	if userID == "" || email == "" {
		return nil
	}
	_, err := ensureStationWithDB(db, userID, email, stationName, logoURL)
	return err
}

// handleUserProfile dispatches GET/PUT on /api/user/profile.
func handleUserProfile(w http.ResponseWriter, r *http.Request) {
	userID := r.Context().Value(contextKeyUserID).(string)
	email, _ := r.Context().Value(contextKeyEmail).(string)

	switch r.Method {
	case http.MethodGet:
		if err := ensureStationProfileRow(userID, email, "", ""); err != nil {
			log.Printf("[profile] ensure station row for user %s failed: %v", userID, err)
		}
		expireTrialIfNeeded(userID)
		var firstName, lastName string
		_ = db.QueryRow(`SELECT first_name, last_name FROM users WHERE id = $1`, userID).Scan(&firstName, &lastName)
		var stationName, genre, description, logoURL, stationSlug, plan, billingCycle string
		var paypalSubscriptionID, stripeSubscriptionID string
		var isSuspended bool
		var trialUsed bool
		var trialStartedAt, trialEndsAt sql.NullTime
		_ = db.QueryRow(`
			SELECT station_name, genre, description, logo_url, station_slug, plan,
			       billing_cycle, is_suspended, trial_used, trial_started_at, trial_ends_at,
			       paypal_subscription_id, stripe_subscription_id
			FROM stations WHERE user_id = $1
		`, userID).Scan(&stationName, &genre, &description, &logoURL, &stationSlug,
			&plan, &billingCycle, &isSuspended, &trialUsed, &trialStartedAt, &trialEndsAt,
			&paypalSubscriptionID, &stripeSubscriptionID)
		trialActive := trialEndsAt.Valid && trialEndsAt.Time.After(time.Now())
		paymentRequired := trialEndsAt.Valid && !trialActive &&
			paypalSubscriptionID == "" && stripeSubscriptionID == ""
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"email":         email,
			"first_name":    firstName,
			"last_name":     lastName,
			"station_name":  stationName,
			"genre":         genre,
			"description":   description,
			"logo_url":      logoURL,
			"listen_url":    "/listen/" + stationSlug,
			"plan":          plan,
			"billing_cycle": billingCycle,
			"is_suspended":  isSuspended,
			"trial_used":    trialUsed,
			"trial_active":  trialActive,
			"trial_started_at": func() interface{} {
				if trialStartedAt.Valid {
					return trialStartedAt.Time
				}
				return nil
			}(),
			"trial_ends_at": func() interface{} {
				if trialEndsAt.Valid {
					return trialEndsAt.Time
				}
				return nil
			}(),
			"payment_required": paymentRequired,
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
		newStationName := strings.TrimSpace(body.StationName)
		if newStationName != "" {
			var takenBy string
			_ = db.QueryRow(`SELECT user_id FROM stations WHERE lower(station_name) = lower($1) AND user_id != $2`, newStationName, userID).Scan(&takenBy)
			if takenBy != "" {
				http.Error(w, "station name already taken", http.StatusConflict)
				return
			}
		}
		if err := ensureStationProfileRow(userID, email, newStationName, body.LogoURL); err != nil {
			log.Printf("[profile] ensure station row for user %s failed: %v", userID, err)
			http.Error(w, "server error", http.StatusInternalServerError)
			return
		}
		if _, err := db.Exec(
			`UPDATE stations SET station_name = $1, genre = $2, description = $3, logo_url = $4 WHERE user_id = $5`,
			newStationName, body.Genre, strings.TrimSpace(body.Description), body.LogoURL, userID,
		); err != nil {
			if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
				http.Error(w, "station name already taken", http.StatusConflict)
				return
			}
			http.Error(w, "server error", http.StatusInternalServerError)
			return
		}

		// Regenerate the station slug name-part from the new station name,
		// preserving the existing random suffix so the URL structure stays consistent.
		var listenURL string
		if newStationName != "" {
			var currentSlug string
			if dbErr := db.QueryRow(`SELECT station_slug FROM stations WHERE user_id = $1`, userID).Scan(&currentSlug); dbErr == nil {
				// Extract the existing 4-char hex suffix after the last hyphen
				suffix := ""
				if idx := strings.LastIndex(currentSlug, "-"); idx >= 0 {
					suffix = currentSlug[idx+1:]
				}
				if suffix == "" {
					rb := make([]byte, 2)
					rand.Read(rb) //nolint:errcheck
					suffix = hex.EncodeToString(rb)
				}
				newSlug := slugifyName(newStationName) + "-" + suffix
				// Best-effort update; ignore collisions (slug stays the same on conflict)
				_, _ = db.Exec(`UPDATE stations SET station_slug = $1 WHERE user_id = $2`, newSlug, userID)
				listenURL = "/listen/" + newSlug
			}
		}
		if listenURL == "" {
			var slug string
			_ = db.QueryRow(`SELECT station_slug FROM stations WHERE user_id = $1`, userID).Scan(&slug)
			listenURL = "/listen/" + slug
		}

		// Issue a fresh token so the frontend picks up any name/email change immediately
		var role string
		_ = db.QueryRow(`SELECT role FROM users WHERE id = $1`, userID).Scan(&role)
		if role == "" {
			role = "user"
		}
		token, err := jwtSign(userID, email, newStationName, role)
		if err != nil {
			http.Error(w, "server error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"token": token, "listen_url": listenURL})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleListenerStatus returns the current listener count, limit, and status
// GET /api/user/listener-status
func handleListenerStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID := r.Context().Value(contextKeyUserID).(string)
	plan := getUserPlan(userID)
	limit := getListenerLimit(plan)
	current := totalLiveListenerCount(userID)
	overLimit := current - limit

	var status string
	if current >= limit+6 {
		status = "suspended" // 6+ over limit - streaming disabled
	} else if current > limit {
		status = "warning" // 1-5 over limit - warning only
	} else if current >= int(float64(limit)*0.9) {
		status = "approaching" // 90%+ of limit
	} else {
		status = "ok"
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"current":    current,
		"limit":      limit,
		"plan":       plan,
		"status":     status,
		"over_limit": overLimit,
		"percentage": int(float64(current) / float64(limit) * 100),
	})
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

// handleEncoderAuthorize returns the server-controlled Icecast settings needed
// by the dedicated encoder worker. Authentication is the user's normal bearer
// token; the worker never needs the Railway JWT secret or database credentials.
func handleEncoderAuthorize(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID := r.Context().Value(contextKeyUserID).(string)
	expireTrialIfNeeded(userID)
	var body struct {
		Bitrate string `json:"bitrate"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	body.Bitrate = strings.TrimSpace(body.Bitrate)
	if body.Bitrate == "" {
		body.Bitrate = "96k"
	}

	var streamKey, sourcePassword, plan string
	var suspended bool
	err := db.QueryRow(`
		SELECT u.stream_key, st.source_password, COALESCE(st.plan, 'starter'), COALESCE(st.is_suspended, false)
		FROM users u
		JOIN stations st ON st.user_id = u.id
		WHERE u.id = $1
	`, userID).Scan(&streamKey, &sourcePassword, &plan, &suspended)
	if err != nil {
		http.Error(w, "station not found", http.StatusNotFound)
		return
	}
	if suspended {
		http.Error(w, "station is suspended", http.StatusForbidden)
		return
	}
	if !bitrateAllowedForPlan(plan, body.Bitrate) {
		http.Error(w, fmt.Sprintf("%s bitrate is not available on the %s package", body.Bitrate, plan), http.StatusForbidden)
		return
	}
	if shared := strings.TrimSpace(os.Getenv("ICECAST_SOURCE_PASSWORD")); shared != "" {
		sourcePassword = shared
	}
	if streamKey == "" || sourcePassword == "" {
		http.Error(w, "station encoder credentials are incomplete", http.StatusConflict)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"user_id":         userID,
		"stream_key":      streamKey,
		"source_password": sourcePassword,
		"username":        "source",
		"bitrate":         body.Bitrate,
		"plan":            plan,
	})
}

// handleEncoderSession lets the authenticated encoder worker publish the
// current user's live state without giving the worker direct database access.
// Public station reads still verify the Icecast mount before reporting it live.
func handleEncoderSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID := r.Context().Value(contextKeyUserID).(string)
	var body struct {
		Live bool `json:"live"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if body.Live {
		var streamKey string
		if err := db.QueryRow(`SELECT stream_key FROM users WHERE id = $1`, userID).Scan(&streamKey); err != nil || streamKey == "" {
			http.Error(w, "station not found", http.StatusNotFound)
			return
		}
		_, err := db.Exec(`
			UPDATE stations
			SET is_live = true, last_connected_at = $1, icecast_listen_url = $2
			WHERE user_id = $3
		`, time.Now().UTC().Format(time.RFC3339), publicIcecastListenURL(streamKey), userID)
		if err != nil {
			http.Error(w, "server error", http.StatusInternalServerError)
			return
		}
	} else {
		if _, err := db.Exec(`UPDATE stations SET is_live = false, icecast_listen_url = '' WHERE user_id = $1`, userID); err != nil {
			http.Error(w, "server error", http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
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

	// Fetch source_password; generate one if this is a pre-migration station row
	var sourcePassword string
	_ = db.QueryRow(`SELECT source_password FROM stations WHERE user_id = $1`, userID).Scan(&sourcePassword)
	if sourcePassword == "" {
		if p, err := generateKey(); err == nil {
			sourcePassword = p
			_, _ = db.Exec(`UPDATE stations SET source_password = $1 WHERE user_id = $2`, sourcePassword, userID)
		}
	}
	// If a server-side global Icecast password is configured, expose that instead.
	// External clients (BUTT, Mixxx) should use this password with any mount.
	if envPass := os.Getenv("ICECAST_SOURCE_PASSWORD"); envPass != "" {
		sourcePassword = envPass
	}

	rows, err := db.Query(
		`SELECT id, platform, label, rtmp_url, stream_key, enabled FROM destinations WHERE user_id = $1 ORDER BY updated_at`,
		userID,
	)
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	// Return new camelCase format so migrateChannel() passes through unchanged.
	type destResp struct {
		ID        string `json:"id"`
		Platform  string `json:"platform"`
		Label     string `json:"label"`
		ServerURL string `json:"serverUrl"`
		StreamKey string `json:"streamKey"`
		Active    bool   `json:"active"`
	}
	var dests []destResp
	for rows.Next() {
		var d destResp
		var enabled int
		if err := rows.Scan(&d.ID, &d.Platform, &d.Label, &d.ServerURL, &d.StreamKey, &enabled); err != nil {
			continue
		}
		d.Active = enabled == 1
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
		"source_password":  sourcePassword,
		"icecast_host":     strings.TrimSpace(os.Getenv("ICECAST_HOST")),
		"icecast_port":     strings.TrimSpace(os.Getenv("ICECAST_PORT")),
		"icecast_username": "source",
	}
	if stationSlug != "" {
		resp["station_slug"] = stationSlug
		resp["listen_url"] = "/listen/" + stationSlug
		resp["hub_listen_url"] = "/listen/" + stationSlug
	}
	resp["icecast_listen_url"] = publicIcecastListenURL(streamKey)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleSaveCredentials upserts external platform stream keys for the current user.
// PUT /api/user/stream-credentials
// Body: {"destinations": [{id, platform, label, serverUrl, streamKey, active}]}
// Replaces all destinations for the user atomically (DELETE + INSERT in a transaction).
func handleSaveCredentials(w http.ResponseWriter, r *http.Request) {
	userID := r.Context().Value(contextKeyUserID).(string)
	var body struct {
		Destinations []struct {
			ID        string `json:"id"`
			Platform  string `json:"platform"`
			Label     string `json:"label"`
			ServerURL string `json:"serverUrl"`
			StreamKey string `json:"streamKey"`
			Active    bool   `json:"active"`
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

	// Delete all existing destinations for this user and re-insert fresh.
	if _, err := tx.Exec(`DELETE FROM destinations WHERE user_id = $1`, userID); err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	for _, d := range body.Destinations {
		serverURL := strings.TrimRight(strings.TrimSpace(d.ServerURL), "/")
		streamKey := strings.TrimSpace(d.StreamKey)
		if serverURL == "" || streamKey == "" {
			continue // skip incomplete entries
		}
		id := d.ID
		if id == "" {
			id, _ = generateKey()
		}
		platform := d.Platform
		if platform == "" {
			platform = "custom"
		}
		label := d.Label
		if label == "" {
			label = platform
		}
		enabled := 0
		if d.Active {
			enabled = 1
		}
		// rtmp_url stores the server base URL; server_url is the same value
		// (both columns kept for backward-compat with getDestinationsForKey).
		if _, err := tx.Exec(`
			INSERT INTO destinations (id, user_id, platform, label, rtmp_url, server_url, stream_key, enabled, updated_at)
			VALUES ($1, $2, $3, $4, $5, $5, $6, $7, $8)
		`, id, userID, platform, label, serverURL, streamKey, enabled, time.Now().UTC().Format(time.RFC3339)); err != nil {
			log.Printf("[stream-creds] insert error: %v", err)
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

// handleDestinations provides REST CRUD for stream destinations.
// GET /api/destinations
// POST /api/destinations
// PUT /api/destinations/{id}
// DELETE /api/destinations/{id}
func handleDestinations(w http.ResponseWriter, r *http.Request) {
	userID := r.Context().Value(contextKeyUserID).(string)
	id := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/destinations"), "/")

	type destinationPayload struct {
		Name      string `json:"name"`
		ServerURL string `json:"serverUrl"`
		StreamKey string `json:"streamKey"`
		Enabled   bool   `json:"enabled"`
		Platform  string `json:"platform"`
	}

	switch r.Method {
	case http.MethodGet:
		rows, err := db.Query(`
			SELECT id, platform, label, rtmp_url, stream_key, enabled
			FROM destinations
			WHERE user_id = $1
			ORDER BY updated_at DESC
		`, userID)
		if err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		type destinationResp struct {
			ID        string `json:"id"`
			Name      string `json:"name"`
			ServerURL string `json:"serverUrl"`
			StreamKey string `json:"streamKey"`
			Enabled   bool   `json:"enabled"`
			Platform  string `json:"platform"`
		}

		resp := make([]destinationResp, 0)
		for rows.Next() {
			var d destinationResp
			var enabled int
			if err := rows.Scan(&d.ID, &d.Platform, &d.Name, &d.ServerURL, &d.StreamKey, &enabled); err != nil {
				continue
			}
			d.Enabled = enabled == 1
			resp = append(resp, d)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"destinations": resp})
		return

	case http.MethodPost:
		var body destinationPayload
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}

		name := strings.TrimSpace(body.Name)
		serverURL := strings.TrimRight(strings.TrimSpace(body.ServerURL), "/")
		streamKey := strings.TrimSpace(body.StreamKey)
		platform := strings.TrimSpace(body.Platform)
		if platform == "" {
			platform = "custom"
		}
		if name == "" || serverURL == "" || streamKey == "" {
			http.Error(w, "name, serverUrl, and streamKey are required", http.StatusBadRequest)
			return
		}
		if !strings.HasPrefix(strings.ToLower(serverURL), "rtmp://") && !strings.HasPrefix(strings.ToLower(serverURL), "rtmps://") {
			http.Error(w, "serverUrl must start with rtmp:// or rtmps://", http.StatusBadRequest)
			return
		}

		destinationID, err := generateKey()
		if err != nil {
			http.Error(w, "could not create destination", http.StatusInternalServerError)
			return
		}

		enabled := 0
		if body.Enabled {
			enabled = 1
		}

		if _, err := db.Exec(`
			INSERT INTO destinations (id, user_id, platform, label, rtmp_url, server_url, stream_key, enabled, updated_at)
			VALUES ($1, $2, $3, $4, $5, $5, $6, $7, $8)
		`, destinationID, userID, platform, name, serverURL, streamKey, enabled, time.Now().UTC().Format(time.RFC3339)); err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok", "id": destinationID})
		return

	case http.MethodPut:
		if id == "" {
			http.Error(w, "destination id required", http.StatusBadRequest)
			return
		}
		var body destinationPayload
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}

		name := strings.TrimSpace(body.Name)
		serverURL := strings.TrimRight(strings.TrimSpace(body.ServerURL), "/")
		streamKey := strings.TrimSpace(body.StreamKey)
		platform := strings.TrimSpace(body.Platform)
		if platform == "" {
			platform = "custom"
		}
		if name == "" || serverURL == "" || streamKey == "" {
			http.Error(w, "name, serverUrl, and streamKey are required", http.StatusBadRequest)
			return
		}
		if !strings.HasPrefix(strings.ToLower(serverURL), "rtmp://") && !strings.HasPrefix(strings.ToLower(serverURL), "rtmps://") {
			http.Error(w, "serverUrl must start with rtmp:// or rtmps://", http.StatusBadRequest)
			return
		}

		enabled := 0
		if body.Enabled {
			enabled = 1
		}

		res, err := db.Exec(`
			UPDATE destinations
			SET platform = $1, label = $2, rtmp_url = $3, server_url = $3, stream_key = $4, enabled = $5, updated_at = $6
			WHERE id = $7 AND user_id = $8
		`, platform, name, serverURL, streamKey, enabled, time.Now().UTC().Format(time.RFC3339), id, userID)
		if err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
		affected, _ := res.RowsAffected()
		if affected == 0 {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		return

	case http.MethodDelete:
		if id == "" {
			http.Error(w, "destination id required", http.StatusBadRequest)
			return
		}
		res, err := db.Exec(`DELETE FROM destinations WHERE id = $1 AND user_id = $2`, id, userID)
		if err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
		affected, _ := res.RowsAffected()
		if affected == 0 {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
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
		// Ensure exactly one slash between server URL and stream key.
		dest := strings.TrimRight(rtmpURL, "/") + "/" + strings.TrimLeft(key, "/")
		dests = append(dests, dest)
	}
	return dests
}

// ─── HTTP handlers ────────────────────────────────────────────────────────────

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if isAllowedOrigin(origin) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("[panic] %s %s: %v", r.Method, r.URL.Path, rec)
				http.Error(w, "server error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// conferenceGuestLimits maps plan name \u2192 max number of guests (excluding host).
var conferenceGuestLimits = map[string]int{
	"starter":      2,
	"professional": 2,
	"enterprise":   5,
	"ultimate":     20,
}

// \u2500\u2500\u2500 Conference WebRTC Signaling \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

type confPeer struct {
	id       string
	username string
	isHost   bool
	mu       sync.Mutex
	conn     *websocket.Conn
}

func (p *confPeer) writeJSON(v any) {
	p.mu.Lock()
	_ = p.conn.WriteJSON(v)
	p.mu.Unlock()
}

type confRoomState struct {
	mu    sync.RWMutex
	peers map[string]*confPeer
}

var (
	confRooms   = map[string]*confRoomState{}
	confRoomsMu sync.Mutex
)

type confMsg struct {
	Type      string          `json:"type"`
	PeerID    string          `json:"peerId,omitempty"`
	From      string          `json:"from,omitempty"`
	To        string          `json:"to,omitempty"`
	Username  string          `json:"username,omitempty"`
	IsHost    bool            `json:"isHost,omitempty"`
	Peers     []confPeerInfo  `json:"peers,omitempty"`
	SDP       string          `json:"sdp,omitempty"`
	Candidate json.RawMessage `json:"candidate,omitempty"`
	Message   string          `json:"message,omitempty"`
}

type confPeerInfo struct {
	PeerID   string `json:"peerId"`
	Username string `json:"username"`
	IsHost   bool   `json:"isHost"`
}

func handleConferenceSignal(w http.ResponseWriter, r *http.Request) {
	roomID := strings.TrimSpace(r.URL.Query().Get("room"))
	username := strings.TrimSpace(r.URL.Query().Get("username"))
	token := r.URL.Query().Get("token")

	if roomID == "" || username == "" {
		http.Error(w, "room and username required", http.StatusBadRequest)
		return
	}
	for _, c := range roomID {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-') {
			http.Error(w, "invalid room id", http.StatusBadRequest)
			return
		}
	}

	isRoomOwner := false
	if token != "" {
		if claims, err := jwtVerify(token); err == nil && claims.UserID == roomID {
			isRoomOwner = true
			var stationName string
			if dbErr := db.QueryRow(`SELECT station_name FROM stations WHERE user_id = $1`, claims.UserID).Scan(&stationName); dbErr == nil && strings.TrimSpace(stationName) != "" {
				username = strings.TrimSpace(stationName)
			}
		}
	}

	if !isRoomOwner {
		var plan string
		if dbErr := db.QueryRow(`SELECT plan FROM stations WHERE user_id = $1`, roomID).Scan(&plan); dbErr != nil {
			plan = "professional"
		}
		limit, ok := conferenceGuestLimits[plan]
		if !ok {
			limit = 5
		}
		confRoomsMu.Lock()
		existing := confRooms[roomID]
		var count int
		if existing != nil {
			existing.mu.RLock()
			count = len(existing.peers)
			existing.mu.RUnlock()
		}
		confRoomsMu.Unlock()
		if count >= limit+1 {
			http.Error(w, fmt.Sprintf("Room is full (%d/%d guests)", limit, limit), http.StatusForbidden)
			return
		}
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	idBytes := make([]byte, 6)
	rand.Read(idBytes)
	peerID := hex.EncodeToString(idBytes)

	peer := &confPeer{id: peerID, username: username, isHost: isRoomOwner, conn: conn}

	confRoomsMu.Lock()
	room, exists := confRooms[roomID]
	if !exists {
		room = &confRoomState{peers: make(map[string]*confPeer)}
		confRooms[roomID] = room
	}
	confRoomsMu.Unlock()

	room.mu.Lock()
	snapshot := make([]confPeerInfo, 0, len(room.peers))
	for _, p := range room.peers {
		snapshot = append(snapshot, confPeerInfo{PeerID: p.id, Username: p.username, IsHost: p.isHost})
	}
	room.peers[peerID] = peer
	room.mu.Unlock()

	peer.writeJSON(confMsg{Type: "joined", PeerID: peerID, IsHost: isRoomOwner, Peers: snapshot})

	joined := confMsg{Type: "peer_joined", PeerID: peerID, Username: username, IsHost: isRoomOwner}
	room.mu.RLock()
	for id, p := range room.peers {
		if id != peerID {
			p.writeJSON(joined)
		}
	}
	room.mu.RUnlock()

	done := make(chan struct{})
	go func() {
		t := time.NewTicker(25 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				peer.mu.Lock()
				_ = conn.WriteMessage(websocket.PingMessage, nil)
				peer.mu.Unlock()
			case <-done:
				return
			}
		}
	}()

	conn.SetReadLimit(32 * 1024)
	conn.SetReadDeadline(time.Now().Add(70 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(70 * time.Second))
		return nil
	})

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			break
		}
		conn.SetReadDeadline(time.Now().Add(70 * time.Second))
		var msg confMsg
		if json.Unmarshal(data, &msg) != nil {
			continue
		}
		switch msg.Type {
		case "offer", "answer", "candidate":
			if msg.To == "" {
				continue
			}
			room.mu.RLock()
			target, ok := room.peers[msg.To]
			room.mu.RUnlock()
			if !ok {
				continue
			}
			target.writeJSON(confMsg{Type: msg.Type, From: peerID, SDP: msg.SDP, Candidate: msg.Candidate})
		}
	}

	close(done)
	conn.Close()

	room.mu.Lock()
	delete(room.peers, peerID)
	empty := len(room.peers) == 0
	room.mu.Unlock()

	if empty {
		confRoomsMu.Lock()
		delete(confRooms, roomID)
		confRoomsMu.Unlock()
	} else {
		left := confMsg{Type: "peer_left", PeerID: peerID}
		room.mu.RLock()
		for _, p := range room.peers {
			p.writeJSON(left)
		}
		room.mu.RUnlock()
	}
}

// handleConfig returns/updates station metadata.
// It computes the radio HLS URL dynamically.
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
			// VIDEO DISABLED: videoUrl intentionally omitted.
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

var planAudioBitrates = map[string]map[string]bool{
	"starter":      {"96k": true},
	"professional": {"96k": true, "128k": true},
	"enterprise":   {"96k": true, "128k": true, "192k": true},
	"ultimate":     {"96k": true, "128k": true, "192k": true, "320k": true},
}

func bitrateAllowedForPlan(plan, bitrate string) bool {
	allowed, ok := planAudioBitrates[plan]
	if !ok {
		allowed = planAudioBitrates["starter"]
	}
	return allowed[bitrate]
}

type encoderSessionStore struct {
	mu   sync.RWMutex
	live map[string]bool
}

var liveEncoderSessions = &encoderSessionStore{live: make(map[string]bool)}

type encoderOwnerStore struct {
	mu     sync.Mutex
	active map[string]bool
}

var activeBroadcastOwners = &encoderOwnerStore{active: make(map[string]bool)}

func (s *encoderOwnerStore) acquire(userID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active[userID] {
		return false
	}
	s.active[userID] = true
	return true
}

func (s *encoderOwnerStore) release(userID string) {
	s.mu.Lock()
	delete(s.active, userID)
	s.mu.Unlock()
}

func (s *encoderSessionStore) markLive(userID string) {
	if userID == "" {
		return
	}
	s.mu.Lock()
	s.live[userID] = true
	s.mu.Unlock()
}

func (s *encoderSessionStore) clear(userID string) {
	if userID == "" {
		return
	}
	s.mu.Lock()
	delete(s.live, userID)
	s.mu.Unlock()
}

func (s *encoderSessionStore) isLive(userID string) bool {
	if userID == "" {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.live[userID]
}

type tailBuffer struct {
	mu   sync.Mutex
	data []byte
	max  int
}

func newTailBuffer(max int) *tailBuffer {
	return &tailBuffer{max: max}
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.data = append(b.data, p...)
	if b.max > 0 && len(b.data) > b.max {
		b.data = append([]byte(nil), b.data[len(b.data)-b.max:]...)
	}
	return len(p), nil
}

func (b *tailBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.TrimSpace(string(b.data))
}

func ffmpegStatusMessage(prefix string, err error, stderr *tailBuffer) string {
	msg := prefix
	if err != nil {
		msg += ": " + err.Error()
	}
	if stderr != nil {
		if detail := stderr.String(); detail != "" {
			msg += ": " + detail
		}
	}
	return msg
}

func sanitizePublicStationLiveState(_ string, isLive bool, icecastListenURL string) (bool, string) {
	if !isLive {
		return false, ""
	}
	if strings.HasPrefix(icecastListenURL, "/icecast/") {
		icecastListenURL = publicIcecastListenURL(strings.TrimPrefix(icecastListenURL, "/icecast/"))
	}
	return isLive, icecastListenURL
}

// publicIcecastListenURL returns a direct listener URL when Icecast is hosted
// outside the web application. Keeping the mount key as the only variable
// makes this work for existing and newly registered stations alike.
func publicIcecastListenURL(streamKey string) string {
	base := strings.TrimRight(strings.TrimSpace(os.Getenv("ICECAST_PUBLIC_URL")), "/")
	if base == "" {
		// Production Icecast runs on its public streaming host. The website's
		// /icecast proxy depends on a private cross-service address and returns
		// 502 when the frontend and Icecast are deployed on different providers.
		base = "https://stream.radioinonestop.com"
	}
	return base + "/" + streamKey
}

// handleBroadcast is the hub-mode branch of the encoder WebSocket.
// The browser sends raw WebM/Opus audio chunks which are:
//  1. Fanned out via the stationHub to /listen/{slug} HTTP clients (WebM, desktop-only)
//  2. Also piped into an FFmpeg process that transcodes to HLS (AAC/MPEG-TS)
//     served at /hls/{slug}/index.m3u8 — works on iOS, Android, and all browsers.
func handleBroadcast(conn *websocket.Conn, sendStatus func(string, string), userID string) {
	var stationSlug string
	err := db.QueryRow(`SELECT station_slug FROM stations WHERE user_id = $1`, userID).Scan(&stationSlug)
	if err != nil {
		sendStatus("error", "no station found for your account — log out and back in")
		return
	}

	liveMarked := false
	markLive := func() {
		if liveMarked {
			return
		}
		liveMarked = true
		liveEncoderSessions.markLive(userID)
		db.Exec(`UPDATE stations SET is_live = true, last_connected_at = $1, icecast_listen_url = '' WHERE user_id = $2`, //nolint:errcheck
			time.Now().UTC().Format(time.RFC3339), userID)
		log.Printf("[hub/%s] marked live after first audio chunk", stationSlug)
	}

	// ── Start FFmpeg: WebM/Opus → HLS (AAC + MPEG-TS segments) ─────────────
	hlsDir := filepath.Join(HLSDir, stationSlug)
	if mkErr := os.MkdirAll(hlsDir, 0755); mkErr != nil {
		log.Printf("[hub/%s] mkdir error: %v", stationSlug, mkErr)
	}
	playlist := filepath.Join(hlsDir, "index.m3u8")
	segPattern := filepath.Join(hlsDir, "seg%05d.ts")

	ffCtx, ffCancel := context.WithCancel(context.Background())
	ffCmd := exec.CommandContext(ffCtx, "ffmpeg",
		"-loglevel", "error",
		"-f", "webm", // tell FFmpeg the input format — skip probing, required for live pipe
		"-i", "pipe:0", // read WebM from stdin
		"-vn", // audio only
		"-c:a", "aac",
		"-b:a", "128k",
		"-ar", "44100",
		"-ac", "2",
		"-f", "hls",
		"-hls_time", "2",
		"-hls_list_size", "5",
		"-hls_flags", "delete_segments+independent_segments",
		"-hls_segment_type", "mpegts",
		"-hls_segment_filename", segPattern,
		playlist,
	)
	ffCmd.Stdout = os.Stdout
	ffCmd.Stderr = os.Stderr

	var ffStdin io.WriteCloser
	if ffStdin, err = ffCmd.StdinPipe(); err != nil {
		log.Printf("[hub/%s] FFmpeg stdin pipe error: %v", stationSlug, err)
		ffCancel()
		ffStdin = nil
	} else if err = ffCmd.Start(); err != nil {
		log.Printf("[hub/%s] FFmpeg start error (HLS disabled): %v", stationSlug, err)
		ffStdin.Close()
		ffStdin = nil
	} else {
		log.Printf("[hub/%s] FFmpeg HLS transcoder started → %s", stationSlug, playlist)
		go func() {
			if werr := ffCmd.Wait(); werr != nil {
				log.Printf("[hub/%s] FFmpeg exited unexpectedly: %v", stationSlug, werr)
			}
		}()
	}

	h := getOrCreateHub(stationSlug)
	sendStatus("live", "Broadcasting — listeners at /listen/"+stationSlug)
	log.Printf("[hub/%s] broadcaster connected (user=%s)", stationSlug, userID)

	defer func() {
		// Stop FFmpeg and clean up HLS segments.
		ffCancel()
		if ffStdin != nil {
			ffStdin.Close()
		}
		ffCmd.Wait() //nolint:errcheck
		os.RemoveAll(hlsDir)
		log.Printf("[hub/%s] HLS segments cleaned up", stationSlug)

		closeHub(stationSlug, h)
		if liveMarked {
			liveEncoderSessions.clear(userID)
			db.Exec(`UPDATE stations SET is_live = false, icecast_listen_url = '' WHERE user_id = $1`, userID) //nolint:errcheck
		}
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
		// Fan out raw WebM to /listen/ clients.
		h.broadcast(data)
		markLive()
		// Also feed FFmpeg for HLS transcoding.
		if ffStdin != nil {
			if _, werr := ffStdin.Write(data); werr != nil {
				log.Printf("[hub/%s] FFmpeg write error: %v", stationSlug, werr)
				ffStdin = nil
			}
		}
	}
}

// handleListen streams live WebM audio from the station hub to an HTTP client.
// GET /listen/{station_slug}

// handleIcecastAuth is called by Icecast's URL authentication module.
// Icecast POSTs form data: action, mount, user, pass, ip, agent.
// We check whether the provided password matches the station's source_password
// for the stream_key embedded in the mount path, then respond:
//
//	"awk=allow\r\n" → Icecast lets the source connect
//	"awk=deny\r\n"  → Icecast rejects the source
//
// POST /api/icecast/auth
func handleIcecastAuth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")

	allow := func() {
		// Icecast URL authentication requires this response header. Keep the
		// legacy response body for compatibility with older deployments.
		w.Header().Set("Icecast-Auth-User", "1")
		w.Write([]byte("awk=allow\r\n")) //nolint:errcheck
	}
	deny := func() {
		w.Header().Set("Icecast-Auth-Message", "Invalid source credentials")
		w.Write([]byte("awk=deny\r\n")) //nolint:errcheck
	}

	if err := r.ParseForm(); err != nil {
		deny()
		return
	}

	// Log every incoming auth request so we can confirm Icecast is reaching us.
	log.Printf("[icecast-auth] request from=%s action=%q mount=%q user=%q",
		r.RemoteAddr, r.FormValue("action"), r.FormValue("mount"), r.FormValue("user"))

	// Non-source actions (e.g. listener auth) — allow by default.
	action := r.FormValue("action")
	if action != "source_auth" && action != "stream_auth" {
		allow()
		return
	}

	mount := strings.TrimPrefix(r.FormValue("mount"), "/")
	pass := r.FormValue("pass")
	if mount == "" || pass == "" {
		deny()
		return
	}

	// The mount path is the user's stream_key (e.g. /081924935dc8175a4b7d464b72fe652d).
	// Look up the station whose user has this stream_key and verify source_password.
	var storedPassword string
	err := db.QueryRow(`
		SELECT st.source_password
		FROM stations st
		JOIN users u ON u.id = st.user_id
		WHERE u.stream_key = $1
	`, mount).Scan(&storedPassword)
	sharedPass := strings.TrimSpace(os.Getenv("ICECAST_SOURCE_PASSWORD"))
	if sharedPass != "" && pass == sharedPass {
		log.Printf("[icecast-auth] allowed mount=/%s (shared secret)", mount)
		allow()
		return
	}

	if err != nil || storedPassword == "" || storedPassword != pass {
		log.Printf("[icecast-auth] denied mount=/%s", mount)
		deny()
		return
	}

	log.Printf("[icecast-auth] allowed mount=/%s", mount)
	allow()
}

// handleListenerSession starts, refreshes, or stops a web/HLS listener session.
// POST /api/listeners/start     {"slug":"station-slug"}
// POST /api/listeners/heartbeat {"session_id":"..."}
// POST /api/listeners/stop      {"session_id":"..."}
func handleListenerSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	switch r.URL.Path {
	case "/api/listeners/start":
		var body struct {
			Slug string `json:"slug"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
			return
		}
		slug := strings.TrimSpace(body.Slug)
		if slug == "" || strings.ContainsAny(slug, " \t\n\r/") {
			http.Error(w, `{"error":"invalid station id"}`, http.StatusBadRequest)
			return
		}

		var userID string
		var isLive bool
		err := db.QueryRow(`SELECT user_id, is_live FROM stations WHERE station_slug = $1`, slug).Scan(&userID, &isLive)
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, `{"error":"station not found"}`, http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, `{"error":"server error"}`, http.StatusInternalServerError)
			return
		}
		if !isLive {
			http.Error(w, `{"error":"station is offline"}`, http.StatusConflict)
			return
		}

		sessionID, err := registerWebListener(userID, slug, clientIP(r), 30*time.Second)
		if err != nil {
			http.Error(w, `{"error":"server error"}`, http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"session_id": sessionID})

	case "/api/listeners/heartbeat":
		var body struct {
			SessionID string `json:"session_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.SessionID == "" {
			http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
			return
		}
		if !touchWebListener(body.SessionID, 30*time.Second) {
			http.Error(w, `{"error":"listener session not found"}`, http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})

	case "/api/listeners/stop":
		var body struct {
			SessionID string `json:"session_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.SessionID == "" {
			http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
			return
		}
		unregisterWebListener(body.SessionID)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})

	default:
		http.NotFound(w, r)
	}
}

// handleGetStations returns all registered stations (public).
// GET /api/stations
func handleGetStations(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(`
		SELECT user_id, station_slug, station_name, logo_url, is_live, current_listeners_count, genre, description, icecast_listen_url
		FROM stations
		INNER JOIN users ON users.id = stations.user_id
		WHERE users.is_email_verified = true
		ORDER BY is_live DESC, station_name ASC
	`)
	if err != nil {
		http.Error(w, `{"error":"db error"}`, http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	type Station struct {
		UserID           string `json:"-"`
		Slug             string `json:"slug"`
		Name             string `json:"name"`
		LogoURL          string `json:"logo_url"`
		IsLive           bool   `json:"is_live"`
		Listeners        int    `json:"listeners"`
		Genre            string `json:"genre"`
		Desc             string `json:"description"`
		IcecastListenURL string `json:"icecast_listen_url"`
	}
	stations := []Station{}
	for rows.Next() {
		var s Station
		if err := rows.Scan(&s.UserID, &s.Slug, &s.Name, &s.LogoURL, &s.IsLive, &s.Listeners, &s.Genre, &s.Desc, &s.IcecastListenURL); err != nil {
			continue
		}
		s.IsLive, s.IcecastListenURL = sanitizePublicStationLiveState(s.UserID, s.IsLive, s.IcecastListenURL)
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
		UserID           string `json:"-"`
		Slug             string `json:"slug"`
		Name             string `json:"name"`
		LogoURL          string `json:"logo_url"`
		IsLive           bool   `json:"is_live"`
		Listeners        int    `json:"listeners"`
		Genre            string `json:"genre"`
		Desc             string `json:"description"`
		IcecastListenURL string `json:"icecast_listen_url"`
	}
	var s Station
	err := db.QueryRow(`
		SELECT user_id, station_slug, station_name, logo_url, is_live, current_listeners_count, genre, description, icecast_listen_url
		FROM stations
		INNER JOIN users ON users.id = stations.user_id
		WHERE station_slug = $1 AND users.is_email_verified = true
	`, slug).Scan(&s.UserID, &s.Slug, &s.Name, &s.LogoURL, &s.IsLive, &s.Listeners, &s.Genre, &s.Desc, &s.IcecastListenURL)
	if err != nil {
		http.Error(w, `{"error":"station not found"}`, http.StatusNotFound)
		return
	}
	s.IsLive, s.IcecastListenURL = sanitizePublicStationLiveState(s.UserID, s.IsLive, s.IcecastListenURL)
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
	var userID string
	var isLive bool
	err := db.QueryRow(`
		SELECT stations.user_id, stations.is_live
		FROM stations
		INNER JOIN users ON users.id = stations.user_id
		WHERE stations.station_slug = $1 AND users.is_email_verified = true
	`, slug).Scan(&userID, &isLive)
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

	existingSessionID := r.URL.Query().Get("listener_session")
	if existingSessionID != "" && webListenerMatches(existingSessionID, slug) {
		touchWebListener(existingSessionID, 30*time.Second)
	} else {
		sessionID, err := registerWebListener(userID, slug, clientIP(r), 0)
		if err == nil {
			defer unregisterWebListener(sessionID)
		}
	}

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
	if !activeBroadcastOwners.acquire(claims.UserID) {
		log.Printf("[encoder/%s] rejected duplicate broadcast connection", claims.UserID)
		sendStatus("error", "A stream is already live on another tab or PC. Stop it there before going live here.")
		return
	}
	defer activeBroadcastOwners.release(claims.UserID)

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
		cfg.Bitrate = "96k"
	}
	plan := getUserPlan(claims.UserID)
	if !bitrateAllowedForPlan(plan, cfg.Bitrate) {
		sendStatus("error", fmt.Sprintf("%s bitrate is not available on the %s package", cfg.Bitrate, plan))
		return
	}
	// The shared standby source is MP3. Icecast can preserve a listener
	// connection across fallback/override only when both mounts use the same
	// content type, so keep every protected station mount on MP3 as well.
	codec := "mp3"

	// ── Server-side Icecast target override ───────────────────────────────
	// ICECAST_HOST / ICECAST_PORT env vars let the server admin pin the
	// Icecast endpoint (e.g. Railway private networking) regardless of what
	// the browser sends.  When unset the client-supplied values are used.
	icecastHostOverride := strings.TrimSpace(os.Getenv("ICECAST_HOST"))
	if icecastHostOverride != "" {
		cfg.Host = icecastHostOverride
	}
	if p := strings.TrimSpace(os.Getenv("ICECAST_PORT")); p != "" {
		cfg.Port = p
	}
	if _, err := net.LookupHost(cfg.Host); err != nil {
		sendStatus("error", fmt.Sprintf("cannot resolve Icecast host %q: %v", cfg.Host, err))
		return
	}

	// ── Resolve Icecast source password ───────────────────────────────────
	// The server-side env var takes precedence over the client-supplied password.
	// This lets us use a shared secret (set identically on the backend and Icecast
	// services) without the browser needing to know it, and without requiring
	// Icecast to make HTTP callbacks to the backend for URL auth.
	icecastPass := cfg.Password
	if envPass := strings.TrimSpace(os.Getenv("ICECAST_SOURCE_PASSWORD")); envPass != "" {
		icecastPass = envPass
	}
	if strings.TrimSpace(icecastPass) == "" {
		sendStatus("error", "Icecast source password is missing; provide encoder password or set ICECAST_SOURCE_PASSWORD")
		return
	}

	// ── Build icecast:// URL using net/url (safe, no shell injection) ──────
	icecastURL := &url.URL{
		Scheme: "icecast",
		User:   url.UserPassword(cfg.Username, icecastPass),
		Host:   cfg.Host + ":" + cfg.Port,
		Path:   cfg.Mount,
	}

	var audioCodec, outFmt, contentType string
	if codec == "aac" {
		audioCodec, outFmt, contentType = "aac", "adts", "audio/aac"
	} else {
		audioCodec, outFmt, contentType = "libmp3lame", "mp3", "audio/mpeg"
	}

	// FFmpeg args — exec.Command never passes these through a shell
	args := []string{
		"-loglevel", "error",
		"-fflags", "nobuffer",
		"-f", "webm",
		"-i", "pipe:0",
		"-vn",
		"-c:a", audioCodec,
		"-b:a", cfg.Bitrate,
		"-ar", "44100",
		"-ac", "2",
		"-content_type", contentType,
		"-flush_packets", "1",
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
	ffmpegStderr := newTailBuffer(16 * 1024)
	cmd.Stderr = io.MultiWriter(os.Stderr, ffmpegStderr)

	if err := cmd.Start(); err != nil {
		sendStatus("error", "ffmpeg start: "+err.Error())
		return
	}
	log.Printf("[encoder/%s] started → %s:%s%s (codec=%s bitrate=%s)",
		claims.UserID, cfg.Host, cfg.Port, cfg.Mount, codec, cfg.Bitrate)
	// Signal readiness immediately so the browser starts pushing audio chunks.
	// The DB live flag is still set only after we receive the first real bytes.
	sendStatus("live", fmt.Sprintf("Encoder connected → %s:%s%s", cfg.Host, cfg.Port, cfg.Mount))

	ffmpegDone := make(chan error, 1)
	go func() { ffmpegDone <- cmd.Wait() }()

	liveMarked := false
	exitReason := "handler completed"
	connectedAt := time.Now()
	lastAudioAt := time.Time{}
	audioBytes := int64(0)
	markLive := func() {
		if liveMarked {
			return
		}
		liveMarked = true
		liveEncoderSessions.markLive(claims.UserID)
		icecastListenURL := "/icecast" + cfg.Mount
		db.Exec(`UPDATE stations SET is_live = true, last_connected_at = $1, icecast_listen_url = $2 WHERE user_id = $3`, //nolint:errcheck
			time.Now().UTC().Format(time.RFC3339), icecastListenURL, claims.UserID)
		log.Printf("[encoder/%s] first audio received; station marked live", claims.UserID)
	}
	defer func() {
		lastAudio := "never"
		if !lastAudioAt.IsZero() {
			lastAudio = time.Since(lastAudioAt).Round(time.Millisecond).String() + " ago"
		}
		log.Printf("[encoder/%s] stopped after %s (audio_bytes=%d last_audio=%s): %s",
			claims.UserID, time.Since(connectedAt).Round(time.Millisecond), audioBytes, lastAudio, exitReason)
		if liveMarked {
			liveEncoderSessions.clear(claims.UserID)
			db.Exec(`UPDATE stations SET is_live = false, icecast_listen_url = '' WHERE user_id = $1`, claims.UserID) //nolint:errcheck
		}
	}()

	// ── Keepalive pings ────────────────────────────────────────────────────
	// Railway (and most reverse proxies) will drop WebSocket connections that
	// show no *control* frames for ~5 minutes, even if binary audio data is
	// flowing.  Send a WS Ping every 30 s so the proxy resets its idle timer.
	pingTicker := time.NewTicker(30 * time.Second)
	defer pingTicker.Stop()
	go func() {
		for {
			select {
			case <-pingTicker.C:
				if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second)); err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	conn.SetPongHandler(func(string) error { return nil })

	// ── Pump WebSocket binary frames → FFmpeg stdin ────────────────────────
	for {
		select {
		case err := <-ffmpegDone:
			if err != nil {
				exitReason = ffmpegStatusMessage("FFmpeg exited", err, ffmpegStderr)
				sendStatus("error", ffmpegStatusMessage("FFmpeg exited", err, ffmpegStderr))
			} else {
				exitReason = "FFmpeg process exited normally"
				sendStatus("stopped", "FFmpeg process exited")
			}
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		mt, data, err := conn.ReadMessage()
		conn.SetReadDeadline(time.Time{})
		if err != nil {
			if closeErr, ok := err.(*websocket.CloseError); ok {
				exitReason = fmt.Sprintf("websocket closed code=%d text=%q", closeErr.Code, closeErr.Text)
			} else {
				exitReason = "websocket read error: " + err.Error()
			}
			cancel()
			stdin.Close()
			if ffErr := <-ffmpegDone; ffErr != nil {
				exitReason += "; " + ffmpegStatusMessage("FFmpeg shutdown", ffErr, ffmpegStderr)
			}
			return
		}

		if mt == websocket.TextMessage {
			var ctrl encoderConfig
			if json.Unmarshal(data, &ctrl) == nil && ctrl.Action == "stop" {
				exitReason = "client requested stop"
				cancel()
				stdin.Close()
				<-ffmpegDone
				sendStatus("stopped", "")
				return
			}
			continue
		}

		// Binary audio chunk
		lastAudioAt = time.Now()
		audioBytes += int64(len(data))
		if _, err := stdin.Write(data); err != nil {
			exitReason = "FFmpeg stdin write failed: " + err.Error()
			cancel()
			stdin.Close() //nolint:errcheck
			ffErr := <-ffmpegDone
			if ffErr != nil || ffmpegStderr.String() != "" {
				exitReason = ffmpegStatusMessage(exitReason, ffErr, ffmpegStderr)
				sendStatus("error", ffmpegStatusMessage("FFmpeg stopped before accepting audio", ffErr, ffmpegStderr))
			} else {
				sendStatus("error", "write to ffmpeg: "+err.Error())
			}
			return
		}
		markLive()
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

// ─── OAuth 2.0 — Platform Connection (Login-to-Stream) ───────────────────────

// oauthStateEntry holds data associated with a short-lived CSRF state token.
type oauthStateEntry struct {
	UserID    string
	Platform  string
	ExpiresAt time.Time
}

// oauthStates stores pending state tokens keyed by the random state string.
// Entries expire after 10 minutes; cleanup is lazy (checked on access).
var oauthStates sync.Map // string → oauthStateEntry

// oauthPlatformConfig defines per-platform OAuth2 settings.
type oauthPlatformConfig struct {
	AuthURL         string
	TokenURL        string
	Scopes          string
	ClientID        func() string
	ClientSecret    func() string
	ExtraAuthParams url.Values // appended to the authorization redirect URL
}

// supportedOAuthPlatforms is the registry of platforms that support OAuth
// Login-to-Stream. Credentials are read from environment variables at
// runtime so the map can be initialised at package level.
var supportedOAuthPlatforms = map[string]oauthPlatformConfig{
	"twitch": {
		AuthURL:      "https://id.twitch.tv/oauth2/authorize",
		TokenURL:     "https://id.twitch.tv/oauth2/token",
		Scopes:       "channel:manage:broadcast",
		ClientID:     func() string { return os.Getenv("TWITCH_CLIENT_ID") },
		ClientSecret: func() string { return os.Getenv("TWITCH_CLIENT_SECRET") },
	},
	"youtube": {
		AuthURL:  "https://accounts.google.com/o/oauth2/v2/auth",
		TokenURL: "https://oauth2.googleapis.com/token",
		Scopes:   "https://www.googleapis.com/auth/youtube.force-ssl",
		ExtraAuthParams: url.Values{
			"access_type": {"offline"},
			"prompt":      {"consent"},
		},
		ClientID:     func() string { return os.Getenv("YOUTUBE_CLIENT_ID") },
		ClientSecret: func() string { return os.Getenv("YOUTUBE_CLIENT_SECRET") },
	},
	"facebook": {
		AuthURL:      "https://www.facebook.com/v20.0/dialog/oauth",
		TokenURL:     "https://graph.facebook.com/v20.0/oauth/access_token",
		Scopes:       "publish_video,pages_manage_posts",
		ClientID:     func() string { return os.Getenv("FACEBOOK_APP_ID") },
		ClientSecret: func() string { return os.Getenv("FACEBOOK_APP_SECRET") },
	},
	"tiktok": {
		AuthURL:      "https://www.tiktok.com/v2/auth/authorize/",
		TokenURL:     "https://open.tiktokapis.com/v2/oauth/token/",
		Scopes:       "user.info.basic,video.publish",
		ClientID:     func() string { return os.Getenv("TIKTOK_CLIENT_KEY") },
		ClientSecret: func() string { return os.Getenv("TIKTOK_CLIENT_SECRET") },
	},
	"instagram": {
		AuthURL:      "https://api.instagram.com/oauth/authorize",
		TokenURL:     "https://api.instagram.com/oauth/access_token",
		Scopes:       "user_profile,user_media",
		ClientID:     func() string { return os.Getenv("INSTAGRAM_APP_ID") },
		ClientSecret: func() string { return os.Getenv("INSTAGRAM_APP_SECRET") },
	},
	"x": {
		AuthURL:      "https://twitter.com/i/oauth2/authorize",
		TokenURL:     "https://api.twitter.com/2/oauth2/token",
		Scopes:       "tweet.write media.write offline.access",
		ClientID:     func() string { return os.Getenv("X_CLIENT_ID") },
		ClientSecret: func() string { return os.Getenv("X_CLIENT_SECRET") },
	},
	"linkedin": {
		AuthURL:      "https://www.linkedin.com/oauth/v2/authorization",
		TokenURL:     "https://www.linkedin.com/oauth/v2/accessToken",
		Scopes:       "w_member_social rw_organizationAdmin",
		ClientID:     func() string { return os.Getenv("LINKEDIN_CLIENT_ID") },
		ClientSecret: func() string { return os.Getenv("LINKEDIN_CLIENT_SECRET") },
	},
}

// appBaseURL returns the backend's public base URL used to construct OAuth
// redirect URIs. Defaults to localhost:8080 for local development.
func appBaseURL() string {
	if v := os.Getenv("APP_BASE_URL"); v != "" {
		return strings.TrimRight(v, "/")
	}
	return "http://localhost:8080"
}

// frontendURL returns the frontend's public base URL used to redirect the
// browser after a successful or failed OAuth callback.
func frontendURL() string {
	if v := os.Getenv("FRONTEND_URL"); v != "" {
		return strings.TrimRight(v, "/")
	}
	return "http://localhost:5173"
}

// handleOAuthRoute is registered under /api/auth/ and dispatches to either
// handleOAuthConnect (POST …/{platform}/connect) or
// handleOAuthCallback (GET …/{platform}/callback).
func handleOAuthRoute(w http.ResponseWriter, r *http.Request) {
	// Strip prefix → e.g. "twitch/connect" or "twitch/callback"
	tail := strings.TrimPrefix(r.URL.Path, "/api/auth/")
	tail = strings.Trim(tail, "/")
	parts := strings.SplitN(tail, "/", 2)
	if len(parts) != 2 {
		http.NotFound(w, r)
		return
	}
	platform, action := parts[0], parts[1]

	// Ensure platform is known before touching auth.
	if _, ok := supportedOAuthPlatforms[platform]; !ok {
		http.NotFound(w, r)
		return
	}

	switch action {
	case "connect":
		// Requires a valid JWT Bearer token (user must be logged in).
		authHdr := r.Header.Get("Authorization")
		if !strings.HasPrefix(authHdr, "Bearer ") {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		claims, err := jwtVerify(strings.TrimPrefix(authHdr, "Bearer "))
		if err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		ctx := context.WithValue(r.Context(), contextKeyUserID, claims.UserID)
		handleOAuthConnect(w, r.WithContext(ctx), platform)
	case "callback":
		// No JWT — browser redirect. User is identified via state token.
		handleOAuthCallback(w, r, platform)
	default:
		http.NotFound(w, r)
	}
}

// handleOAuthConnect initiates the OAuth2 Authorization Code flow.
// POST /api/auth/{platform}/connect
// Returns {"redirect_url": "https://..."} for the client to navigate to.
func handleOAuthConnect(w http.ResponseWriter, r *http.Request, platform string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	cfg := supportedOAuthPlatforms[platform]
	if cfg.ClientID() == "" {
		http.Error(w, "platform not configured", http.StatusServiceUnavailable)
		return
	}

	userID := r.Context().Value(contextKeyUserID).(string)

	stateBytes := make([]byte, 16)
	if _, err := rand.Read(stateBytes); err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	state := hex.EncodeToString(stateBytes)
	oauthStates.Store(state, oauthStateEntry{
		UserID:    userID,
		Platform:  platform,
		ExpiresAt: time.Now().Add(10 * time.Minute),
	})

	redirectURI := appBaseURL() + "/api/auth/" + platform + "/callback"
	params := url.Values{}
	params.Set("client_id", cfg.ClientID())
	params.Set("redirect_uri", redirectURI)
	params.Set("response_type", "code")
	params.Set("scope", cfg.Scopes)
	params.Set("state", state)
	for k, vs := range cfg.ExtraAuthParams {
		if len(vs) > 0 {
			params.Set(k, vs[0])
		}
	}

	authURL := cfg.AuthURL + "?" + params.Encode()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"redirect_url": authURL})
}

// handleOAuthCallback receives the provider redirect and exchanges the
// authorization code for access/refresh tokens.
// GET /api/auth/{platform}/callback?code=...&state=...
func handleOAuthCallback(w http.ResponseWriter, r *http.Request, platform string) {
	errRedirect := func(reason string) {
		http.Redirect(w, r,
			frontendURL()+"?oauth_error="+url.QueryEscape(reason)+"&platform="+platform,
			http.StatusFound)
	}

	state := r.URL.Query().Get("state")
	code := r.URL.Query().Get("code")
	if state == "" || code == "" {
		errRedirect("missing_params")
		return
	}

	entryRaw, ok := oauthStates.LoadAndDelete(state)
	if !ok {
		errRedirect("invalid_state")
		return
	}
	entry := entryRaw.(oauthStateEntry)
	if time.Now().After(entry.ExpiresAt) || entry.Platform != platform {
		errRedirect("expired_state")
		return
	}

	redirectURI := appBaseURL() + "/api/auth/" + platform + "/callback"
	tok, err := oauthExchangeCode(supportedOAuthPlatforms[platform], code, redirectURI)
	if err != nil {
		log.Printf("[oauth] token exchange failed platform=%s user=%s: %v", platform, entry.UserID, err)
		errRedirect("token_exchange_failed")
		return
	}

	connID, err := generateKey()
	if err != nil {
		errRedirect("server_error")
		return
	}
	var expiresAt sql.NullTime
	if tok.ExpiresIn > 0 {
		expiresAt = sql.NullTime{
			Time:  time.Now().Add(time.Duration(tok.ExpiresIn) * time.Second),
			Valid: true,
		}
	}
	_, err = db.Exec(`
		INSERT INTO oauth_connections (id, user_id, platform, access_token, refresh_token, expires_at, scope, connected_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
		ON CONFLICT (user_id, platform) DO UPDATE SET
			access_token  = EXCLUDED.access_token,
			refresh_token = EXCLUDED.refresh_token,
			expires_at    = EXCLUDED.expires_at,
			scope         = EXCLUDED.scope,
			connected_at  = NOW()
	`, connID, entry.UserID, platform, tok.AccessToken, tok.RefreshToken, expiresAt, tok.Scope)
	if err != nil {
		log.Printf("[oauth] db upsert failed platform=%s user=%s: %v", platform, entry.UserID, err)
		errRedirect("db_error")
		return
	}

	log.Printf("[oauth] connected platform=%s user=%s", platform, entry.UserID)
	http.Redirect(w, r, frontendURL()+"?oauth_success="+platform, http.StatusFound)
}

// oauthTokenResponse is the standard OAuth2 token endpoint response.
type oauthTokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	Scope        string `json:"scope"`
	TokenType    string `json:"token_type"`
}

// oauthExchangeCode exchanges an authorization code for tokens via
// application/x-www-form-urlencoded POST (standard for most providers).
func oauthExchangeCode(cfg oauthPlatformConfig, code, redirectURI string) (*oauthTokenResponse, error) {
	params := url.Values{}
	params.Set("grant_type", "authorization_code")
	params.Set("code", code)
	params.Set("redirect_uri", redirectURI)
	params.Set("client_id", cfg.ClientID())
	params.Set("client_secret", cfg.ClientSecret())

	resp, err := http.PostForm(cfg.TokenURL, params)
	if err != nil {
		return nil, fmt.Errorf("POST %s: %w", cfg.TokenURL, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("token endpoint %s returned %d: %s", cfg.TokenURL, resp.StatusCode, string(body))
	}
	var tok oauthTokenResponse
	if err := json.Unmarshal(body, &tok); err != nil {
		return nil, fmt.Errorf("parse token response: %w", err)
	}
	return &tok, nil
}

// handleOAuthConnections dispatches GET (list all connections) and
// DELETE (remove by platform) for /api/user/oauth-connections[/{platform}].
func handleOAuthConnections(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		handleGetOAuthConnections(w, r)
	case http.MethodDelete:
		handleDeleteOAuthConnection(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// GET /api/user/oauth-connections
func handleGetOAuthConnections(w http.ResponseWriter, r *http.Request) {
	userID := r.Context().Value(contextKeyUserID).(string)
	rows, err := db.Query(`
		SELECT platform, connected_at, expires_at, scope
		FROM oauth_connections
		WHERE user_id = $1
		ORDER BY platform
	`, userID)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type connEntry struct {
		Platform    string     `json:"platform"`
		ConnectedAt time.Time  `json:"connected_at"`
		ExpiresAt   *time.Time `json:"expires_at,omitempty"`
		Scope       string     `json:"scope"`
	}
	result := []connEntry{} // never nil — serialises as [] not null
	for rows.Next() {
		var e connEntry
		var expiresAt sql.NullTime
		if err := rows.Scan(&e.Platform, &e.ConnectedAt, &expiresAt, &e.Scope); err != nil {
			continue
		}
		if expiresAt.Valid {
			e.ExpiresAt = &expiresAt.Time
		}
		result = append(result, e)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

// DELETE /api/user/oauth-connections/{platform}
func handleDeleteOAuthConnection(w http.ResponseWriter, r *http.Request) {
	userID := r.Context().Value(contextKeyUserID).(string)
	platform := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/user/oauth-connections"), "/")

	if _, ok := supportedOAuthPlatforms[platform]; !ok {
		http.Error(w, "unsupported platform", http.StatusBadRequest)
		return
	}
	if _, err := db.Exec(`DELETE FROM oauth_connections WHERE user_id = $1 AND platform = $2`, userID, platform); err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	log.Printf("[oauth] disconnected platform=%s user=%s", platform, userID)
	w.WriteHeader(http.StatusNoContent)
}

// ─── Stage 5 — WebRTC relay (browser Go Live → platform RTMP) ────────────────
//
// When the user streams via browser (WebRTC WHIP → MediaMTX), the traditional
// RTMP relay never fires. These handlers let the frontend trigger an FFmpeg
// relay that pulls the stream from MediaMTX via RTSP and forwards it to the
// user's active destinations, just like the OBS/RTMP path.

// mediamtxRTSPBase returns the base RTSP URL for MediaMTX.
// Defaults to rtsp://mediamtx:8554 (Docker Compose internal hostname).
func mediamtxRTSPBase() string {
	base := os.Getenv("MEDIAMTX_RTSP_URL")
	if base == "" {
		base = "rtsp://mediamtx:8554"
	}
	return strings.TrimRight(base, "/")
}

// webRelayManager maps userID → running relay cancel function.
type webRelayManager struct {
	mu       sync.Mutex
	sessions map[string]*ActiveSession
}

// ActiveSession tracks a single running relay process.
type ActiveSession struct {
	mu           sync.Mutex
	Cmd          *exec.Cmd
	Cancel       context.CancelFunc
	UserID       string
	Path         string
	StartedAt    time.Time
	Destinations []string
}

type relayMetricsState struct {
	mu        sync.RWMutex
	fps       float64
	bandwidth float64
	cpu       float64
	updatedAt time.Time
}

var webRelay = &webRelayManager{sessions: make(map[string]*ActiveSession)}
var relayMetrics = &relayMetricsState{}

var (
	ffmpegFPSRe     = regexp.MustCompile(`fps=\s*([0-9]+(?:\.[0-9]+)?)`)
	ffmpegBitrateRe = regexp.MustCompile(`bitrate=\s*([0-9]+(?:\.[0-9]+)?)kbits/s`)
)

func (m *webRelayManager) replace(userID string, session *ActiveSession) {
	m.mu.Lock()
	old := m.sessions[userID]
	m.sessions[userID] = session
	m.mu.Unlock()
	if old != nil {
		stopActiveSession(old)
	}
}

func (m *webRelayManager) stop(userID string) bool {
	m.mu.Lock()
	s := m.sessions[userID]
	if s != nil {
		delete(m.sessions, userID)
	}
	m.mu.Unlock()
	if s != nil {
		stopActiveSession(s)
		return true
	}
	return false
}

func (m *webRelayManager) deleteIfMatch(userID string, session *ActiveSession) {
	m.mu.Lock()
	if m.sessions[userID] == session {
		delete(m.sessions, userID)
	}
	m.mu.Unlock()
}

func (m *webRelayManager) activeCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sessions)
}

func stopActiveSession(s *ActiveSession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Cancel != nil {
		s.Cancel()
	}
	if s.Cmd != nil && s.Cmd.Process != nil {
		_ = s.Cmd.Process.Kill()
	}
}

func setRelayMetrics(fps, bandwidth float64) {
	relayMetrics.mu.Lock()
	if fps >= 0 {
		relayMetrics.fps = fps
	}
	if bandwidth >= 0 {
		relayMetrics.bandwidth = bandwidth
	}
	relayMetrics.updatedAt = time.Now()
	relayMetrics.mu.Unlock()
}

func setRelayCPU(cpu float64) {
	relayMetrics.mu.Lock()
	relayMetrics.cpu = cpu
	relayMetrics.updatedAt = time.Now()
	relayMetrics.mu.Unlock()
}

func getRelayMetrics() (cpu, bandwidth, fps float64) {
	relayMetrics.mu.RLock()
	defer relayMetrics.mu.RUnlock()
	return relayMetrics.cpu, relayMetrics.bandwidth, relayMetrics.fps
}

func processFFmpegProgressLine(line string) {
	fps := -1.0
	bw := -1.0
	if m := ffmpegFPSRe.FindStringSubmatch(line); len(m) == 2 {
		if v, err := strconv.ParseFloat(m[1], 64); err == nil {
			fps = v
		}
	}
	if m := ffmpegBitrateRe.FindStringSubmatch(line); len(m) == 2 {
		if kbps, err := strconv.ParseFloat(m[1], 64); err == nil {
			bw = kbps / 1000.0
		}
	}
	if fps >= 0 || bw >= 0 {
		setRelayMetrics(fps, bw)
	}
}

func sampleSystemCPU() float64 {
	cmd := exec.Command("sh", "-c", "ps -A -o %cpu | awk 'NR>1 {s+=$1} END {print s+0}'")
	out, err := cmd.Output()
	if err != nil {
		return 0
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil {
		return 0
	}
	cores := runtime.NumCPU()
	if cores > 0 {
		v = v / float64(cores)
	}
	if v < 0 {
		return 0
	}
	return v
}

func startMetricsWorker() {
	t := time.NewTicker(1 * time.Second)
	go func() {
		defer t.Stop()
		for range t.C {
			setRelayCPU(sampleSystemCPU())
		}
	}()
}

func getActiveDestinationURLs(userID string) ([]string, error) {
	rows, err := db.Query(`
		SELECT rtmp_url, stream_key FROM destinations
		WHERE user_id = $1 AND enabled = 1 AND stream_key != '' AND rtmp_url != ''
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dests []string
	for rows.Next() {
		var rtmpURL, key string
		if err := rows.Scan(&rtmpURL, &key); err == nil {
			dests = append(dests, strings.TrimRight(rtmpURL, "/")+"/"+strings.TrimLeft(key, "/"))
		}
	}
	return dests, nil
}

func buildRelayFFmpegArgs(rtspSrc string, destinations []string) []string {
	args := []string{
		"-rtsp_transport", "tcp",
		"-i", rtspSrc,
		// Transcode WebRTC (VP8/Opus) to RTMP/FLV (H.264/AAC)
		"-c:v", "libx264",
		"-preset", "veryfast",
		"-b:v", "3000k",
		"-maxrate", "3000k",
		"-bufsize", "6000k",
		"-pix_fmt", "yuv420p",
		"-g", "60", // Keyframe interval (2s at 30fps)
		"-c:a", "aac",
		"-b:a", "160k",
		"-ar", "44100",
	}

	if len(destinations) == 1 {
		args = append(args, "-f", "flv", destinations[0])
	} else if len(destinations) > 1 {
		var teeOuts []string
		for _, dest := range destinations {
			// Use the tee muxer to push to multiple RTMP destinations efficiently
			escaped := dest
			escaped = strings.ReplaceAll(escaped, "\\", "\\\\")
			escaped = strings.ReplaceAll(escaped, "|", "\\|")
			escaped = strings.ReplaceAll(escaped, "[", "\\[")
			escaped = strings.ReplaceAll(escaped, "]", "\\]")
			teeOuts = append(teeOuts, fmt.Sprintf("[f=flv]%s", escaped))
		}
		args = append(args, "-f", "tee", strings.Join(teeOuts, "|"))
	}

	return args
}

func startRelayForUser(userID, path string) (int, error) {
	if isStreamingDisabled(userID) {
		return 0, errors.New("streaming disabled - listener limit exceeded")
	}

	dests, err := getActiveDestinationURLs(userID)
	if err != nil {
		return 0, err
	}
	if len(dests) == 0 {
		return 0, nil
	}

	cleanPath := strings.Trim(path, "/")
	if cleanPath == "" {
		return 0, errors.New("path required")
	}

	rtspSrc := mediamtxRTSPBase() + "/" + cleanPath
	args := buildRelayFFmpegArgs(rtspSrc, dests)

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	cmd.Stdout = os.Stdout
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return 0, err
	}

	if err := cmd.Start(); err != nil {
		log.Printf("[relay] FFmpeg failed to start for user=%s: %v", userID, err)
		cancel()
		return 0, err
	}

	session := &ActiveSession{
		Cmd:          cmd,
		Cancel:       cancel,
		UserID:       userID,
		Path:         cleanPath,
		StartedAt:    time.Now(),
		Destinations: dests,
	}
	webRelay.replace(userID, session)

	go func() {
		scanner := bufio.NewScanner(stderrPipe)
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			processFFmpegProgressLine(line)
			log.Printf("[relay/%s] %s", userID, line)
		}
	}()

	go func(s *ActiveSession, c context.Context) {
		defer cancel()
		if err := cmd.Wait(); err != nil && c.Err() == nil {
			log.Printf("[relay] FFmpeg exited user=%s path=%s: %v", userID, cleanPath, err)
		}
		webRelay.deleteIfMatch(userID, s)
	}(session, ctx)

	log.Printf("[relay] started user=%s path=%s destinations=%d", userID, cleanPath, len(dests))
	return len(dests), nil
}

// POST /api/stream/relay/start
// Body: {"path": "<mediamtx-path>"} — the path the browser published to via WHIP.
// Spawns an FFmpeg process that pulls from RTSP and forwards to all active
// destinations for the authenticated user.
func handleRelayStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID := r.Context().Value(contextKeyUserID).(string)

	var body struct {
		Path string `json:"path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Path == "" {
		http.Error(w, "path required", http.StatusBadRequest)
		return
	}

	destinationCount, err := startRelayForUser(userID, body.Path)
	if err != nil {
		if strings.Contains(err.Error(), "disabled") {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		http.Error(w, "relay start failed", http.StatusInternalServerError)
		return
	}

	if destinationCount == 0 {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":   "ok",
			"relaying": false,
			"reason":   "no active destinations",
		})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":       "ok",
		"relaying":     true,
		"destinations": destinationCount,
	})
}

// POST /api/stream/relay/stop
// Kills the running FFmpeg relay for the authenticated user.
func handleRelayStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID := r.Context().Value(contextKeyUserID).(string)
	ok := webRelay.stop(userID)

	log.Printf("[relay] stopped user=%s (was_running=%v)", userID, ok)
	w.WriteHeader(http.StatusNoContent)
}

// POST /api/stream/status
// Body: {"action":"publish|done","user":"<stream_key>","path":"live/<stream_key>"}
// This endpoint is designed for MediaMTX runOnPublish/runOnUnpublish callbacks.
func handleStreamLifecycleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if expected := strings.TrimSpace(os.Getenv("STREAM_STATUS_TOKEN")); expected != "" {
		if r.Header.Get("X-Stream-Status-Token") != expected {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
	}

	var body struct {
		Action string `json:"action"`
		User   string `json:"user"`
		Path   string `json:"path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}

	action := strings.ToLower(strings.TrimSpace(body.Action))
	pathValue := strings.Trim(strings.TrimSpace(body.Path), "/")
	userValue := strings.Trim(strings.TrimSpace(body.User), "/")
	if pathValue == "" {
		pathValue = userValue
	}

	streamKey := userValue
	if streamKey == "" {
		streamKey = pathValue
	}
	// Fix for MediaMTX WHIP paths which end in /whip
	if strings.HasSuffix(streamKey, "/whip") {
		streamKey = strings.TrimSuffix(streamKey, "/whip")
	}
	if strings.Contains(streamKey, "/") {
		parts := strings.Split(streamKey, "/")
		streamKey = parts[len(parts)-1]
	}

	if streamKey == "" {
		http.Error(w, "stream user/key required", http.StatusBadRequest)
		return
	}
	userID := getUserIDFromStreamKey(streamKey)
	if userID == "" {
		http.Error(w, "unknown stream key", http.StatusNotFound)
		return
	}

	switch action {
	case "publish":
		count, err := startRelayForUser(userID, pathValue)
		if err != nil {
			if strings.Contains(err.Error(), "disabled") {
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}
			http.Error(w, "relay start failed", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":       "ok",
			"action":       "publish",
			"destinations": count,
		})
		return
	case "done", "unpublish":
		stopped := webRelay.stop(userID)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "ok",
			"action":  "done",
			"stopped": stopped,
		})
		return
	default:
		http.Error(w, "unsupported action", http.StatusBadRequest)
		return
	}
}

// GET /api/metrics
func handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	cpu, bandwidth, fps := getRelayMetrics()
	if cpu <= 0 {
		cpu = sampleSystemCPU()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"cpu":            cpu,
		"bandwidth":      bandwidth,
		"fps":            fps,
		"active_streams": webRelay.activeCount(),
	})
}

// POST /api/stream/auth
// Body: {"user":"<stream_key>"} or {"streamKey":"<stream_key>"}
// Returns 200 when publish is allowed, 403 when blocked.
// handleStreamAuth is called by MediaMTX's externalAuthenticationURL for every
// publish/read attempt.  MediaMTX sends a POST with JSON:
//
//	{"ip":"…","user":"…","password":"…","path":"live/<key>","protocol":"rtmp",
//	 "id":"…","action":"publish","query":"…"}
//
// The stream key is the last segment of "path" (after the app name).
// We also accept our own internal format: {"user":"<key>"} or {"streamKey":"<key>"}.
//
// Returns 200 to allow, 403 to reject so MediaMTX closes the socket immediately.
func handleStreamAuth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Optional shared-secret check so external callers can't spoof the callback.
	if expected := strings.TrimSpace(os.Getenv("STREAM_AUTH_TOKEN")); expected != "" {
		if r.Header.Get("X-Stream-Auth-Token") != expected {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
	}

	// MediaMTX external-auth payload (superset of our own internal format).
	var body struct {
		// MediaMTX fields
		Action   string `json:"action"`   // "publish" | "read"
		Path     string `json:"path"`     // e.g. "live/abc123def" or "abc123def"
		User     string `json:"user"`     // RTMP user / stream-key when set
		Password string `json:"password"` // RTMP password field (unused here)
		Protocol string `json:"protocol"` // "rtmp" | "rtsp" | "webrtc" …
		IP       string `json:"ip"`
		Query    string `json:"query"` // URL query string — some OBS versions put the key here
		// Internal / legacy format
		StreamKey string `json:"streamKey"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}

	// Readers (HLS / RTSP playback) are always allowed — we only gate publishers.
	action := strings.ToLower(strings.TrimSpace(body.Action))
	if action == "read" || action == "" {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Extract the stream key from the path last segment, user field, or streamKey field.
	key := strings.TrimSpace(body.StreamKey)
	if key == "" {
		key = strings.TrimSpace(body.User)
	}
	if key == "" {
		// Take the last path segment: "live/abc123" → "abc123"
		p := strings.Trim(strings.TrimSpace(body.Path), "/")
		if idx := strings.LastIndex(p, "/"); idx >= 0 {
			key = p[idx+1:]
		} else {
			key = p
		}
	}
	if key == "" && body.Query != "" {
		// e.g. ?streamkey=abc123 or ?key=abc123
		if q, err := url.ParseQuery(body.Query); err == nil {
			for _, qk := range []string{"streamkey", "key", "stream_key", "token"} {
				if v := strings.TrimSpace(q.Get(qk)); v != "" {
					key = v
					break
				}
			}
		}
		if key == "" {
			// Last resort: the query itself might just be the key with no parameter name.
			key = strings.Trim(body.Query, "?/ ")
		}
	}

	if key == "" {
		// No key at all — deny.
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	userID := getUserIDFromStreamKey(key)
	if userID == "" {
		log.Printf("[stream/auth] rejected unknown key=%q ip=%s", key, body.IP)
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if isStreamingDisabled(userID) {
		log.Printf("[stream/auth] rejected disabled user=%s key=%q", userID, key)
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	log.Printf("[stream/auth] allowed user=%s key=%q protocol=%s ip=%s", userID, key, body.Protocol, body.IP)
	w.WriteHeader(http.StatusOK)
}

// ─── OAuth Stage 4 — Stream Key Provisioning ─────────────────────────────────

// oauthStreamDest is a stream destination auto-provisioned via OAuth.
type oauthStreamDest struct {
	Platform  string `json:"platform"`
	Label     string `json:"label"`
	ServerURL string `json:"server_url"`
	StreamKey string `json:"stream_key"`
}

// refreshOAuthTokenIfNeeded checks whether the stored access token is expired
// or close to expiry (within 5 minutes) and refreshes it if needed.
// Returns the current valid access token.
func refreshOAuthTokenIfNeeded(userID, platform string) (string, error) {
	var accessToken, refreshToken string
	var expiresAt sql.NullTime
	err := db.QueryRow(`
		SELECT access_token, refresh_token, expires_at
		FROM oauth_connections
		WHERE user_id = $1 AND platform = $2
	`, userID, platform).Scan(&accessToken, &refreshToken, &expiresAt)
	if err != nil {
		return "", fmt.Errorf("no connection for platform %s: %w", platform, err)
	}

	// If no expiry stored, or token is still fresh (>5 min remaining), return as-is.
	if !expiresAt.Valid || time.Until(expiresAt.Time) > 5*time.Minute {
		return accessToken, nil
	}

	// Token is expired or about to expire — refresh it.
	cfg, ok := supportedOAuthPlatforms[platform]
	if !ok || refreshToken == "" {
		return accessToken, nil // Can't refresh; try with the current token.
	}

	params := url.Values{}
	params.Set("grant_type", "refresh_token")
	params.Set("refresh_token", refreshToken)
	params.Set("client_id", cfg.ClientID())
	params.Set("client_secret", cfg.ClientSecret())

	resp, err := http.PostForm(cfg.TokenURL, params)
	if err != nil {
		return accessToken, fmt.Errorf("refresh POST failed: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return accessToken, fmt.Errorf("refresh endpoint returned %d: %s", resp.StatusCode, string(body))
	}

	var tok oauthTokenResponse
	if err := json.Unmarshal(body, &tok); err != nil {
		return accessToken, fmt.Errorf("parse refresh response: %w", err)
	}

	// Persist updated tokens.
	var newExpiry sql.NullTime
	if tok.ExpiresIn > 0 {
		newExpiry = sql.NullTime{Time: time.Now().Add(time.Duration(tok.ExpiresIn) * time.Second), Valid: true}
	}
	newRefresh := tok.RefreshToken
	if newRefresh == "" {
		newRefresh = refreshToken // some providers don't rotate the refresh token
	}
	_, _ = db.Exec(`
		UPDATE oauth_connections
		SET access_token = $1, refresh_token = $2, expires_at = $3
		WHERE user_id = $4 AND platform = $5
	`, tok.AccessToken, newRefresh, newExpiry, userID, platform)

	log.Printf("[oauth] refreshed token platform=%s user=%s", platform, userID)
	return tok.AccessToken, nil
}

// fetchTwitchStreamKey fetches the RTMP stream key for the authenticated Twitch user.
func fetchTwitchStreamKey(accessToken, clientID string) (*oauthStreamDest, error) {
	// Step 1: get the broadcaster's user ID and display name.
	req, _ := http.NewRequest("GET", "https://api.twitch.tv/helix/users", nil)
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Client-Id", clientID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("twitch users: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("twitch users %d: %s", resp.StatusCode, string(body))
	}
	var usersResp struct {
		Data []struct {
			ID          string `json:"id"`
			DisplayName string `json:"display_name"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &usersResp); err != nil || len(usersResp.Data) == 0 {
		return nil, fmt.Errorf("twitch users parse: %w", err)
	}
	broadcasterID := usersResp.Data[0].ID
	displayName := usersResp.Data[0].DisplayName

	// Step 2: get the stream key.
	req2, _ := http.NewRequest("GET", "https://api.twitch.tv/helix/streams/key?broadcaster_id="+broadcasterID, nil)
	req2.Header.Set("Authorization", "Bearer "+accessToken)
	req2.Header.Set("Client-Id", clientID)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		return nil, fmt.Errorf("twitch stream key: %w", err)
	}
	defer resp2.Body.Close()
	body2, _ := io.ReadAll(resp2.Body)
	if resp2.StatusCode >= 400 {
		return nil, fmt.Errorf("twitch stream key %d: %s", resp2.StatusCode, string(body2))
	}
	var keyResp struct {
		Data []struct {
			StreamKey string `json:"stream_key"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body2, &keyResp); err != nil || len(keyResp.Data) == 0 {
		return nil, fmt.Errorf("twitch stream key parse: %w", err)
	}

	return &oauthStreamDest{
		Platform:  "twitch",
		Label:     "Twitch — " + displayName,
		ServerURL: "rtmp://live.twitch.tv/app/",
		StreamKey: keyResp.Data[0].StreamKey,
	}, nil
}

// fetchYouTubeStreamKey fetches (or creates) a YouTube live stream and returns its RTMP credentials.
func fetchYouTubeStreamKey(accessToken string) (*oauthStreamDest, error) {
	// Step 1: try to get an existing persistent live stream.
	req, _ := http.NewRequest("GET",
		"https://www.googleapis.com/youtube/v3/liveStreams?mine=true&part=cdn,snippet&maxResults=1",
		nil)
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("youtube liveStreams list: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("youtube liveStreams %d: %s", resp.StatusCode, string(body))
	}

	type ytIngestion struct {
		IngestionAddress string `json:"ingestionAddress"`
		StreamName       string `json:"streamName"`
	}
	type ytCDN struct {
		IngestionInfo ytIngestion `json:"ingestionInfo"`
	}
	type ytItem struct {
		Snippet struct {
			Title string `json:"title"`
		} `json:"snippet"`
		CDN ytCDN `json:"cdn"`
	}
	var listResp struct {
		Items []ytItem `json:"items"`
	}
	if err := json.Unmarshal(body, &listResp); err != nil {
		return nil, fmt.Errorf("youtube liveStreams parse: %w", err)
	}

	// Step 2: if none exist, create a new persistent stream.
	var item ytItem
	if len(listResp.Items) > 0 {
		item = listResp.Items[0]
	} else {
		createBody := `{"snippet":{"title":"Radio In One Stop"},"cdn":{"format":"1080p","frameRate":"variable","ingestionType":"rtmp"}}`
		req2, _ := http.NewRequest("POST",
			"https://www.googleapis.com/youtube/v3/liveStreams?part=cdn,snippet",
			strings.NewReader(createBody))
		req2.Header.Set("Authorization", "Bearer "+accessToken)
		req2.Header.Set("Content-Type", "application/json")
		resp2, err := http.DefaultClient.Do(req2)
		if err != nil {
			return nil, fmt.Errorf("youtube liveStreams create: %w", err)
		}
		defer resp2.Body.Close()
		body2, _ := io.ReadAll(resp2.Body)
		if resp2.StatusCode >= 400 {
			return nil, fmt.Errorf("youtube liveStreams create %d: %s", resp2.StatusCode, string(body2))
		}
		if err := json.Unmarshal(body2, &item); err != nil {
			return nil, fmt.Errorf("youtube liveStreams create parse: %w", err)
		}
	}

	if item.CDN.IngestionInfo.StreamName == "" {
		return nil, fmt.Errorf("youtube: no stream key in response")
	}

	return &oauthStreamDest{
		Platform:  "youtube",
		Label:     "YouTube Live",
		ServerURL: item.CDN.IngestionInfo.IngestionAddress,
		StreamKey: item.CDN.IngestionInfo.StreamName,
	}, nil
}

// POST /api/user/oauth-stream-keys/sync
// For each connected platform with a valid/refreshable token, fetches the
// RTMP credentials from the platform API and returns them so the frontend can
// inject them into the multistream destinations list.
func handleSyncOAuthStreamKeys(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID := r.Context().Value(contextKeyUserID).(string)

	// Load all connected platforms for this user.
	rows, err := db.Query(`SELECT platform FROM oauth_connections WHERE user_id = $1`, userID)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var platforms []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err == nil {
			platforms = append(platforms, p)
		}
	}

	type syncResult struct {
		Platform string           `json:"platform"`
		Dest     *oauthStreamDest `json:"dest,omitempty"`
		Error    string           `json:"error,omitempty"`
	}

	results := make([]syncResult, 0, len(platforms))

	for _, platform := range platforms {
		accessToken, err := refreshOAuthTokenIfNeeded(userID, platform)
		if err != nil {
			results = append(results, syncResult{Platform: platform, Error: err.Error()})
			continue
		}

		var dest *oauthStreamDest
		switch platform {
		case "twitch":
			clientID := supportedOAuthPlatforms["twitch"].ClientID()
			dest, err = fetchTwitchStreamKey(accessToken, clientID)
		case "youtube":
			dest, err = fetchYouTubeStreamKey(accessToken)
		default:
			err = fmt.Errorf("stream key provisioning for %s not yet implemented", platform)
		}

		if err != nil {
			log.Printf("[oauth] sync failed platform=%s user=%s: %v", platform, userID, err)
			results = append(results, syncResult{Platform: platform, Error: err.Error()})
		} else {
			log.Printf("[oauth] synced stream key platform=%s user=%s", platform, userID)
			results = append(results, syncResult{Platform: platform, Dest: dest})
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(results)
}

// ─── Advertising Platform Handlers ────────────────────────────────────────────

// handleAdPlacements - GET all available ad placements
func handleAdPlacements(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rows, err := db.Query(`
		SELECT id, name, description, placement, width, height, base_price, active
		FROM ad_placements
		WHERE active = true
		ORDER BY created_at
	`)
	if err != nil {
		log.Printf("[ads] Error fetching placements: %v", err)
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type Placement struct {
		ID          string  `json:"id"`
		Name        string  `json:"name"`
		Description string  `json:"description"`
		Placement   string  `json:"placement"`
		Width       int     `json:"width"`
		Height      int     `json:"height"`
		BasePrice   float64 `json:"basePrice"`
		Active      bool    `json:"active"`
	}

	var placements []Placement
	for rows.Next() {
		var p Placement
		if err := rows.Scan(&p.ID, &p.Name, &p.Description, &p.Placement, &p.Width, &p.Height, &p.BasePrice, &p.Active); err != nil {
			continue
		}
		placements = append(placements, p)
	}

	if placements == nil {
		placements = []Placement{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(placements)
}

// handleAdCampaigns - GET all campaigns or POST to create new campaign
func handleAdCampaigns(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		getAdCampaigns(w, r)
	case http.MethodPost:
		createAdCampaign(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func getAdCampaigns(w http.ResponseWriter, r *http.Request) {
	// Support filtering by placementId and status via query params
	placementID := r.URL.Query().Get("placementId")
	status := r.URL.Query().Get("status")

	query := `
		SELECT 
			c.id, c.placement_id, c.advertiser_name, c.target_url,
			c.asset_type, c.asset_url, c.asset_name,
			c.price, c.original_price, c.discount_percent,
			c.status, c.impressions, c.clicks, c.created_at,
			p.name as placement_name, p.placement
		FROM ad_campaigns c
		JOIN ad_placements p ON c.placement_id = p.id
	`

	var conditions []string
	var args []interface{}
	argIdx := 1

	if placementID != "" {
		conditions = append(conditions, fmt.Sprintf("p.placement = $%d", argIdx))
		args = append(args, placementID)
		argIdx++
	}

	if status != "" {
		conditions = append(conditions, fmt.Sprintf("c.status = $%d", argIdx))
		args = append(args, status)
		argIdx++
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	query += " ORDER BY c.created_at DESC"

	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("[ads] Error fetching campaigns: %v", err)
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type Campaign struct {
		ID              string  `json:"id"`
		PlacementID     string  `json:"placementId"`
		PlacementName   string  `json:"placementName"`
		Placement       string  `json:"placement"`
		AdvertiserName  string  `json:"advertiserName"`
		TargetURL       string  `json:"targetUrl"`
		AssetType       string  `json:"assetType"`
		AssetURL        string  `json:"assetUrl"`
		AssetName       string  `json:"assetName"`
		Price           float64 `json:"price"`
		OriginalPrice   float64 `json:"originalPrice"`
		DiscountPercent int     `json:"discountPercent"`
		Status          string  `json:"status"`
		Impressions     int64   `json:"impressions"`
		Clicks          int64   `json:"clicks"`
		CreatedAt       string  `json:"createdAt"`
	}

	var campaigns []Campaign
	for rows.Next() {
		var c Campaign
		var createdAt time.Time
		if err := rows.Scan(
			&c.ID, &c.PlacementID, &c.AdvertiserName, &c.TargetURL,
			&c.AssetType, &c.AssetURL, &c.AssetName,
			&c.Price, &c.OriginalPrice, &c.DiscountPercent,
			&c.Status, &c.Impressions, &c.Clicks, &createdAt,
			&c.PlacementName, &c.Placement,
		); err != nil {
			continue
		}
		c.CreatedAt = createdAt.Format(time.RFC3339)
		campaigns = append(campaigns, c)
	}

	if campaigns == nil {
		campaigns = []Campaign{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(campaigns)
}

func createAdCampaign(w http.ResponseWriter, r *http.Request) {
	contentType := r.Header.Get("Content-Type")

	// Handle both JSON and multipart form data
	var placementID, advertiserName, targetURL, assetType, assetURL, assetName string
	var price float64
	var discountPercent int

	if strings.HasPrefix(contentType, "multipart/form-data") {
		// Parse multipart form
		if err := r.ParseMultipartForm(32 << 20); err != nil { // 32 MB max
			http.Error(w, "failed to parse form", http.StatusBadRequest)
			return
		}

		placementID = r.FormValue("placementId")
		advertiserName = r.FormValue("advertiserName")
		targetURL = r.FormValue("targetUrl")
		assetType = r.FormValue("assetType")
		assetName = r.FormValue("assetName")
		fmt.Sscanf(r.FormValue("price"), "%f", &price)
		fmt.Sscanf(r.FormValue("discountPercent"), "%d", &discountPercent)

		// Handle file upload
		file, header, err := r.FormFile("assetFile")
		if err == nil {
			defer file.Close()

			// Create uploads directory if it doesn't exist
			uploadsDir := "./uploads/ads"
			if err := os.MkdirAll(uploadsDir, 0755); err != nil {
				log.Printf("[ads] Error creating uploads dir: %v", err)
				http.Error(w, "server error", http.StatusInternalServerError)
				return
			}

			// Generate unique filename
			ext := filepath.Ext(header.Filename)
			fileID, _ := generateKey()
			filename := fileID + ext
			filePath := filepath.Join(uploadsDir, filename)

			// Save file
			dst, err := os.Create(filePath)
			if err != nil {
				log.Printf("[ads] Error creating file: %v", err)
				http.Error(w, "failed to save file", http.StatusInternalServerError)
				return
			}
			defer dst.Close()

			if _, err := io.Copy(dst, file); err != nil {
				log.Printf("[ads] Error saving file: %v", err)
				http.Error(w, "failed to save file", http.StatusInternalServerError)
				return
			}

			assetURL = "/uploads/ads/" + filename
			if assetName == "" {
				assetName = header.Filename
			}
		} else {
			// No file uploaded, use URL if provided
			assetURL = r.FormValue("assetUrl")
		}
	} else {
		// JSON request
		var body struct {
			PlacementID     string  `json:"placementId"`
			AdvertiserName  string  `json:"advertiserName"`
			TargetURL       string  `json:"targetUrl"`
			AssetType       string  `json:"assetType"`
			AssetURL        string  `json:"assetUrl"`
			AssetName       string  `json:"assetName"`
			Price           float64 `json:"price"`
			DiscountPercent int     `json:"discountPercent"`
		}

		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}

		placementID = body.PlacementID
		advertiserName = body.AdvertiserName
		targetURL = body.TargetURL
		assetType = body.AssetType
		assetURL = body.AssetURL
		assetName = body.AssetName
		price = body.Price
		discountPercent = body.DiscountPercent
	}

	if placementID == "" || advertiserName == "" || assetType == "" {
		http.Error(w, "missing required fields", http.StatusBadRequest)
		return
	}

	campaignID, err := generateKey()
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}

	originalPrice := price
	if discountPercent > 0 {
		originalPrice = price / (1.0 - float64(discountPercent)/100.0)
	}

	_, err = db.Exec(`
		INSERT INTO ad_campaigns (
			id, placement_id, advertiser_name, target_url,
			asset_type, asset_url, asset_name,
			price, original_price, discount_percent, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'draft')
	`, campaignID, placementID, advertiserName, targetURL,
		assetType, assetURL, assetName,
		price, originalPrice, discountPercent)

	if err != nil {
		log.Printf("[ads] Error creating campaign: %v", err)
		http.Error(w, "failed to create campaign", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":    true,
		"campaignId": campaignID,
		"assetUrl":   assetURL,
	})
}

// handleAdCampaign - GET/PUT/DELETE individual campaign
func handleAdCampaign(w http.ResponseWriter, r *http.Request) {
	// Extract ID from /api/ads/campaigns/{id}
	path := strings.TrimPrefix(r.URL.Path, "/api/ads/campaigns/")
	campaignID := strings.Split(path, "/")[0]

	if campaignID == "" {
		http.Error(w, "campaign ID required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		getAdCampaign(w, r, campaignID)
	case http.MethodPut:
		updateAdCampaign(w, r, campaignID)
	case http.MethodDelete:
		deleteAdCampaign(w, r, campaignID)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func getAdCampaign(w http.ResponseWriter, r *http.Request, campaignID string) {
	var c struct {
		ID              string  `json:"id"`
		PlacementID     string  `json:"placementId"`
		AdvertiserName  string  `json:"advertiserName"`
		TargetURL       string  `json:"targetUrl"`
		AssetType       string  `json:"assetType"`
		AssetURL        string  `json:"assetUrl"`
		AssetName       string  `json:"assetName"`
		Price           float64 `json:"price"`
		OriginalPrice   float64 `json:"originalPrice"`
		DiscountPercent int     `json:"discountPercent"`
		Status          string  `json:"status"`
		Impressions     int64   `json:"impressions"`
		Clicks          int64   `json:"clicks"`
	}

	err := db.QueryRow(`
		SELECT id, placement_id, advertiser_name, target_url,
			asset_type, asset_url, asset_name,
			price, original_price, discount_percent,
			status, impressions, clicks
		FROM ad_campaigns
		WHERE id = $1
	`, campaignID).Scan(
		&c.ID, &c.PlacementID, &c.AdvertiserName, &c.TargetURL,
		&c.AssetType, &c.AssetURL, &c.AssetName,
		&c.Price, &c.OriginalPrice, &c.DiscountPercent,
		&c.Status, &c.Impressions, &c.Clicks,
	)

	if errors.Is(err, sql.ErrNoRows) {
		http.Error(w, "campaign not found", http.StatusNotFound)
		return
	}
	if err != nil {
		log.Printf("[ads] Error fetching campaign %s: %v", campaignID, err)
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(c)
}

func updateAdCampaign(w http.ResponseWriter, r *http.Request, campaignID string) {
	var body struct {
		AdvertiserName  *string  `json:"advertiserName"`
		TargetURL       *string  `json:"targetUrl"`
		AssetURL        *string  `json:"assetUrl"`
		Price           *float64 `json:"price"`
		DiscountPercent *int     `json:"discountPercent"`
		Status          *string  `json:"status"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// Build dynamic UPDATE query
	updates := []string{}
	args := []interface{}{}
	argPos := 1

	if body.AdvertiserName != nil {
		updates = append(updates, fmt.Sprintf("advertiser_name = $%d", argPos))
		args = append(args, *body.AdvertiserName)
		argPos++
	}
	if body.TargetURL != nil {
		updates = append(updates, fmt.Sprintf("target_url = $%d", argPos))
		args = append(args, *body.TargetURL)
		argPos++
	}
	if body.AssetURL != nil {
		updates = append(updates, fmt.Sprintf("asset_url = $%d", argPos))
		args = append(args, *body.AssetURL)
		argPos++
	}
	if body.Price != nil {
		updates = append(updates, fmt.Sprintf("price = $%d", argPos))
		args = append(args, *body.Price)
		argPos++
	}
	if body.DiscountPercent != nil {
		updates = append(updates, fmt.Sprintf("discount_percent = $%d", argPos))
		args = append(args, *body.DiscountPercent)
		argPos++
	}
	if body.Status != nil {
		updates = append(updates, fmt.Sprintf("status = $%d", argPos))
		args = append(args, *body.Status)
		argPos++
	}

	if len(updates) == 0 {
		http.Error(w, "no fields to update", http.StatusBadRequest)
		return
	}

	updates = append(updates, fmt.Sprintf("updated_at = NOW()"))
	args = append(args, campaignID)

	query := fmt.Sprintf("UPDATE ad_campaigns SET %s WHERE id = $%d", strings.Join(updates, ", "), argPos)

	_, err := db.Exec(query, args...)
	if err != nil {
		log.Printf("[ads] Error updating campaign %s: %v", campaignID, err)
		http.Error(w, "failed to update campaign", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
	})
}

func deleteAdCampaign(w http.ResponseWriter, r *http.Request, campaignID string) {
	_, err := db.Exec(`DELETE FROM ad_campaigns WHERE id = $1`, campaignID)
	if err != nil {
		log.Printf("[ads] Error deleting campaign %s: %v", campaignID, err)
		http.Error(w, "failed to delete campaign", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleAdTrack - Track impressions and clicks (public endpoint, no auth)
func handleAdTrack(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var body struct {
		CampaignID string `json:"campaignId"`
		EventType  string `json:"eventType"` // "impression" or "click"
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if body.CampaignID == "" || (body.EventType != "impression" && body.EventType != "click") {
		http.Error(w, "invalid parameters", http.StatusBadRequest)
		return
	}

	// Extract client info
	ip := r.RemoteAddr
	if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
		ip = strings.Split(forwarded, ",")[0]
	}
	userAgent := r.Header.Get("User-Agent")
	countryCode, _ := resolveCountry(ip)

	// Update campaign counters
	if body.EventType == "impression" {
		_, _ = db.Exec(`UPDATE ad_campaigns SET impressions = impressions + 1 WHERE id = $1`, body.CampaignID)
	} else {
		_, _ = db.Exec(`UPDATE ad_campaigns SET clicks = clicks + 1 WHERE id = $1`, body.CampaignID)
	}

	// Log event in analytics table (async in production)
	go func() {
		_, _ = db.Exec(`
			INSERT INTO ad_analytics (campaign_id, event_type, ip_address, user_agent, country)
			VALUES ($1, $2, $3, $4, $5)
		`, body.CampaignID, body.EventType, ip, userAgent, countryCode)
	}()

	w.WriteHeader(http.StatusNoContent)
}

// handleAdStats - Get aggregated stats for dashboard
func handleAdStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var stats struct {
		ActiveCampaigns  int     `json:"activeCampaigns"`
		TotalImpressions int64   `json:"totalImpressions"`
		TotalClicks      int64   `json:"totalClicks"`
		EstRevenue       float64 `json:"estRevenue"`
		AvgCTR           float64 `json:"avgCTR"`
	}

	// Get active campaigns count
	_ = db.QueryRow(`SELECT COUNT(*) FROM ad_campaigns WHERE status = 'active'`).Scan(&stats.ActiveCampaigns)

	// Get total impressions and clicks
	_ = db.QueryRow(`
		SELECT COALESCE(SUM(impressions), 0), COALESCE(SUM(clicks), 0)
		FROM ad_campaigns
		WHERE status = 'active'
	`).Scan(&stats.TotalImpressions, &stats.TotalClicks)

	// Calculate estimated monthly revenue
	_ = db.QueryRow(`
		SELECT COALESCE(SUM(price), 0)
		FROM ad_campaigns
		WHERE status = 'active'
	`).Scan(&stats.EstRevenue)

	// Calculate average CTR
	if stats.TotalImpressions > 0 {
		stats.AvgCTR = float64(stats.TotalClicks) / float64(stats.TotalImpressions) * 100
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// ── Super Admin API ───────────────────────────────────────────────────────────

// handleAdminUsers - GET all users, PUT to update user
func handleAdminUsers(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		getAllUsers(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func getAllUsers(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(`
		SELECT 
			u.id,
			u.email,
			COALESCE(u.role, 'user'),
			COALESCE(u.created_at::text, ''),
			COALESCE(s.station_name, ''),
			COALESCE(s.plan, 'starter'),
			COALESCE(s.billing_cycle, 'monthly'),
			COALESCE(s.is_suspended, false),
			COALESCE(s.trial_used, false),
			s.trial_started_at,
			s.trial_ends_at
		FROM users u
		LEFT JOIN stations s ON u.id = s.user_id
		ORDER BY u.created_at DESC
	`)
	if err != nil {
		log.Printf("[admin] Error fetching users: %v", err)
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type User struct {
		ID             string     `json:"id"`
		Email          string     `json:"email"`
		Role           string     `json:"role"`
		CreatedAt      string     `json:"createdAt"`
		StationName    string     `json:"stationName"`
		Plan           string     `json:"plan"`
		BillingCycle   string     `json:"billingCycle"`
		IsSuspended    bool       `json:"isSuspended"`
		TrialUsed      bool       `json:"trialUsed"`
		TrialStartedAt *time.Time `json:"trialStartedAt"`
		TrialEndsAt    *time.Time `json:"trialEndsAt"`
	}

	var users []User
	for rows.Next() {
		var u User

		if err := rows.Scan(&u.ID, &u.Email, &u.Role, &u.CreatedAt,
			&u.StationName, &u.Plan, &u.BillingCycle, &u.IsSuspended,
			&u.TrialUsed, &u.TrialStartedAt, &u.TrialEndsAt); err != nil {
			log.Printf("[admin] Error scanning user row: %v", err)
			continue
		}

		if u.Plan == "" {
			u.Plan = "starter"
		}
		if u.BillingCycle == "" {
			u.BillingCycle = "monthly"
		}

		users = append(users, u)
	}
	if err := rows.Err(); err != nil {
		log.Printf("[admin] Error reading user rows: %v", err)
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}

	if users == nil {
		users = []User{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(users)
}

// handleAdminUserUpdate - PUT /api/admin/users/:id
func handleAdminUserUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract user ID from path
	path := strings.TrimPrefix(r.URL.Path, "/api/admin/users/")
	userID := strings.Split(path, "/")[0]

	if userID == "" {
		http.Error(w, "user ID required", http.StatusBadRequest)
		return
	}

	var body struct {
		Plan         string `json:"plan"`
		BillingCycle string `json:"billingCycle"`
		IsSuspended  *bool  `json:"isSuspended"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	body.Plan = strings.ToLower(strings.TrimSpace(body.Plan))
	body.BillingCycle = strings.ToLower(strings.TrimSpace(body.BillingCycle))
	if body.Plan == "" {
		body.Plan = "starter"
	}
	if body.BillingCycle == "" {
		body.BillingCycle = "monthly"
	}
	if body.BillingCycle != "monthly" && body.BillingCycle != "yearly" {
		http.Error(w, "invalid billing cycle", http.StatusBadRequest)
		return
	}
	var planExists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM package_plans WHERE id = $1)`, body.Plan).Scan(&planExists); err != nil {
		log.Printf("[admin] Error validating plan %q: %v", body.Plan, err)
		http.Error(w, "failed to validate plan", http.StatusInternalServerError)
		return
	}
	if !planExists {
		http.Error(w, "invalid plan", http.StatusBadRequest)
		return
	}
	isSuspended := false
	if body.IsSuspended != nil {
		isSuspended = *body.IsSuspended
	} else {
		_ = db.QueryRow(`SELECT is_suspended FROM stations WHERE user_id = $1`, userID).Scan(&isSuspended)
	}

	var email string
	_ = db.QueryRow(`SELECT email FROM users WHERE id = $1`, userID).Scan(&email)
	if _, err := ensureStation(userID, email, "", ""); err != nil {
		log.Printf("[admin] Error ensuring station for user %s: %v", userID, err)
		http.Error(w, "failed to prepare station", http.StatusInternalServerError)
		return
	}

	// Update station plan and suspension status
	_, err := db.Exec(`
		UPDATE stations 
		SET plan = $1, billing_cycle = $2, is_suspended = $3
		WHERE user_id = $4
	`, body.Plan, body.BillingCycle, isSuspended, userID)

	if err != nil {
		log.Printf("[admin] Error updating user: %v", err)
		http.Error(w, "failed to update user", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "User updated successfully",
	})
}

// handleAdminPricing - GET/PUT pricing configuration
func handleAdminPricing(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		getAdminPricing(w, r)
	case http.MethodPut:
		updateAdminPricing(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleStartPackageTrial activates a one-time 30-day trial only when the
// selected package has been enabled for trials by Super Admin.
func handleStartPackageTrial(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID := r.Context().Value(contextKeyUserID).(string)
	var body struct {
		Plan         string `json:"plan"`
		BillingCycle string `json:"billing_cycle"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	body.Plan = strings.ToLower(strings.TrimSpace(body.Plan))
	body.BillingCycle = strings.ToLower(strings.TrimSpace(body.BillingCycle))
	if body.BillingCycle != "monthly" && body.BillingCycle != "yearly" {
		body.BillingCycle = "monthly"
	}

	result, err := db.Exec(`
		UPDATE stations
		SET plan = $1,
		    billing_cycle = $2,
		    is_suspended = false,
		    trial_used = true,
		    trial_started_at = NOW(),
		    trial_ends_at = NOW() + INTERVAL '30 days'
		WHERE user_id = $3
		  AND trial_used = false
		  AND COALESCE(paypal_subscription_id, '') = ''
		  AND COALESCE(stripe_subscription_id, '') = ''
		  AND EXISTS (
			  SELECT 1 FROM package_plans
			  WHERE id = $1 AND trial_enabled = true
		  )
	`, body.Plan, body.BillingCycle, userID)
	if err != nil {
		log.Printf("[trial] Error starting package trial for user %s: %v", userID, err)
		http.Error(w, "failed to start trial", http.StatusInternalServerError)
		return
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		http.Error(w, "free trial is unavailable for this package or has already been used", http.StatusConflict)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":       true,
		"trial_ends_at": time.Now().Add(30 * 24 * time.Hour),
	})
}

func getAdminPricing(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(`
		SELECT id, name, monthly_price, yearly_price, features, monthly_sale_percent, yearly_sale_percent, is_featured,
		       paypal_plan_id_monthly, paypal_plan_id_yearly, trial_enabled
		FROM package_plans
		ORDER BY monthly_price ASC
	`)
	if err != nil {
		log.Printf("[admin] Error fetching pricing: %v", err)
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type PackagePlan struct {
		ID                  string   `json:"id"`
		Name                string   `json:"name"`
		MonthlyPrice        float64  `json:"monthlyPrice"`
		YearlyPrice         float64  `json:"yearlyPrice"`
		Features            []string `json:"features"`
		MonthlySalePercent  int      `json:"monthlySalePercent"`
		YearlySalePercent   int      `json:"yearlySalePercent"`
		IsFeatured          bool     `json:"isFeatured"`
		PayPalPlanIDMonthly string   `json:"paypalPlanIdMonthly"`
		PayPalPlanIDYearly  string   `json:"paypalPlanIdYearly"`
		TrialEnabled        bool     `json:"trialEnabled"`
	}

	var plans []PackagePlan
	for rows.Next() {
		var p PackagePlan
		var featuresJSON []byte
		if err := rows.Scan(&p.ID, &p.Name, &p.MonthlyPrice, &p.YearlyPrice,
			&featuresJSON, &p.MonthlySalePercent, &p.YearlySalePercent, &p.IsFeatured,
			&p.PayPalPlanIDMonthly, &p.PayPalPlanIDYearly, &p.TrialEnabled); err != nil {
			continue
		}
		json.Unmarshal(featuresJSON, &p.Features)
		plans = append(plans, p)
	}

	if plans == nil {
		plans = []PackagePlan{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(plans)
}

func updateAdminPricing(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID                  string   `json:"id"`
		MonthlyPrice        float64  `json:"monthlyPrice"`
		YearlyPrice         float64  `json:"yearlyPrice"`
		Features            []string `json:"features"`
		MonthlySalePercent  int      `json:"monthlySalePercent"`
		YearlySalePercent   int      `json:"yearlySalePercent"`
		IsFeatured          bool     `json:"isFeatured"`
		PayPalPlanIDMonthly string   `json:"paypalPlanIdMonthly"`
		PayPalPlanIDYearly  string   `json:"paypalPlanIdYearly"`
		TrialEnabled        bool     `json:"trialEnabled"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	featuresJSON, _ := json.Marshal(body.Features)

	_, err := db.Exec(`
		UPDATE package_plans
		SET monthly_price = $1, yearly_price = $2, features = $3, 
		    monthly_sale_percent = $4, yearly_sale_percent = $5, is_featured = $6,
		    paypal_plan_id_monthly = $7, paypal_plan_id_yearly = $8, trial_enabled = $9
		WHERE id = $10
	`, body.MonthlyPrice, body.YearlyPrice, featuresJSON,
		body.MonthlySalePercent, body.YearlySalePercent, body.IsFeatured,
		body.PayPalPlanIDMonthly, body.PayPalPlanIDYearly, body.TrialEnabled, body.ID)

	if err != nil {
		log.Printf("[admin] Error updating pricing: %v", err)
		http.Error(w, "failed to update pricing", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "Pricing updated successfully",
	})
}

// handlePublicPricing - GET pricing for public pages (no auth required)
func handlePublicPricing(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rows, err := db.Query(`
		SELECT id, name, monthly_price, yearly_price, features, monthly_sale_percent, yearly_sale_percent, is_featured, trial_enabled
		FROM package_plans
		ORDER BY monthly_price ASC
	`)
	if err != nil {
		log.Printf("[public] Error fetching pricing: %v", err)
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type PackagePlan struct {
		ID                 string   `json:"id"`
		Name               string   `json:"name"`
		MonthlyPrice       float64  `json:"monthlyPrice"`
		YearlyPrice        float64  `json:"yearlyPrice"`
		Features           []string `json:"features"`
		MonthlySalePercent int      `json:"monthlySalePercent"`
		YearlySalePercent  int      `json:"yearlySalePercent"`
		IsFeatured         bool     `json:"isFeatured"`
		TrialEnabled       bool     `json:"trialEnabled"`
	}

	var plans []PackagePlan
	for rows.Next() {
		var p PackagePlan
		var featuresJSON []byte
		if err := rows.Scan(&p.ID, &p.Name, &p.MonthlyPrice, &p.YearlyPrice,
			&featuresJSON, &p.MonthlySalePercent, &p.YearlySalePercent, &p.IsFeatured,
			&p.TrialEnabled); err != nil {
			continue
		}
		json.Unmarshal(featuresJSON, &p.Features)
		plans = append(plans, p)
	}

	if plans == nil {
		plans = []PackagePlan{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(plans)
}

// ── PayPal Subscription Integration ──────────────────────────────────────────

var (
	paypalClientID      = os.Getenv("PAYPAL_CLIENT_ID")
	paypalSecret        = os.Getenv("PAYPAL_SECRET")
	paypalMode          = os.Getenv("PAYPAL_MODE") // "sandbox" or "live"
	paypalWebhookID     = os.Getenv("PAYPAL_WEBHOOK_ID")
	stripeSecretKey     = os.Getenv("STRIPE_SECRET_KEY")
	stripeWebhookSecret = os.Getenv("STRIPE_WEBHOOK_SECRET")
)

func getPayPalBaseURL() string {
	if paypalMode == "live" {
		return "https://api-m.paypal.com"
	}
	return "https://api-m.sandbox.paypal.com"
}

func getPayPalAccessToken() (string, error) {
	url := getPayPalBaseURL() + "/v1/oauth2/token"

	req, err := http.NewRequest("POST", url, strings.NewReader("grant_type=client_credentials"))
	if err != nil {
		return "", err
	}

	req.SetBasicAuth(paypalClientID, paypalSecret)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var result struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	return result.AccessToken, nil
}

// verifyPayPalWebhookSignature confirms an incoming webhook request was actually
// sent by PayPal (not forged) by asking PayPal's own verification API to check the
// transmission headers against the raw request body.
func verifyPayPalWebhookSignature(r *http.Request, body []byte) bool {
	if strings.TrimSpace(paypalWebhookID) == "" {
		log.Printf("[paypal webhook] PAYPAL_WEBHOOK_ID not configured; rejecting webhook")
		return false
	}

	transmissionID := r.Header.Get("Paypal-Transmission-Id")
	transmissionTime := r.Header.Get("Paypal-Transmission-Time")
	certURL := r.Header.Get("Paypal-Cert-Url")
	authAlgo := r.Header.Get("Paypal-Auth-Algo")
	transmissionSig := r.Header.Get("Paypal-Transmission-Sig")
	if transmissionID == "" || transmissionTime == "" || certURL == "" || authAlgo == "" || transmissionSig == "" {
		log.Printf("[paypal webhook] missing transmission headers")
		return false
	}

	accessToken, err := getPayPalAccessToken()
	if err != nil || strings.TrimSpace(accessToken) == "" {
		log.Printf("[paypal webhook] failed to get access token for verification: %v", err)
		return false
	}

	verifyReq := struct {
		AuthAlgo         string          `json:"auth_algo"`
		CertURL          string          `json:"cert_url"`
		TransmissionID   string          `json:"transmission_id"`
		TransmissionSig  string          `json:"transmission_sig"`
		TransmissionTime string          `json:"transmission_time"`
		WebhookID        string          `json:"webhook_id"`
		WebhookEvent     json.RawMessage `json:"webhook_event"`
	}{
		AuthAlgo:         authAlgo,
		CertURL:          certURL,
		TransmissionID:   transmissionID,
		TransmissionSig:  transmissionSig,
		TransmissionTime: transmissionTime,
		WebhookID:        paypalWebhookID,
		WebhookEvent:     json.RawMessage(body),
	}
	payload, err := json.Marshal(verifyReq)
	if err != nil {
		log.Printf("[paypal webhook] failed to marshal verification request: %v", err)
		return false
	}

	req, err := http.NewRequest("POST", getPayPalBaseURL()+"/v1/notifications/verify-webhook-signature", bytes.NewReader(payload))
	if err != nil {
		log.Printf("[paypal webhook] failed to build verification request: %v", err)
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		log.Printf("[paypal webhook] verification request failed: %v", err)
		return false
	}
	defer resp.Body.Close()

	var result struct {
		VerificationStatus string `json:"verification_status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Printf("[paypal webhook] failed to decode verification response: %v", err)
		return false
	}

	return resp.StatusCode == http.StatusOK && result.VerificationStatus == "SUCCESS"
}

// getPayPalSubscription fetches a subscription's live status and plan ID directly
// from PayPal so callers never have to trust client-submitted subscription details.
func getPayPalSubscription(subscriptionID string) (status string, planID string, err error) {
	accessToken, err := getPayPalAccessToken()
	if err != nil || strings.TrimSpace(accessToken) == "" {
		return "", "", fmt.Errorf("failed to get PayPal access token: %w", err)
	}

	req, err := http.NewRequest("GET", getPayPalBaseURL()+"/v1/billing/subscriptions/"+url.PathEscape(subscriptionID), nil)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return "", "", fmt.Errorf("PayPal subscription lookup failed (%d): %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Status string `json:"status"`
		PlanID string `json:"plan_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", "", err
	}
	return result.Status, result.PlanID, nil
}

// handlePayPalCreateSubscription returns the PayPal plan ID for a subscription
func handlePayPalCreateSubscription(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get plan and billing from query parameters
	planID := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("plan")))
	billingCycle := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("billing")))

	if planID == "" || billingCycle == "" {
		http.Error(w, "missing plan or billing parameter", http.StatusBadRequest)
		return
	}
	if billingCycle != "monthly" && billingCycle != "yearly" {
		http.Error(w, "invalid billing cycle", http.StatusBadRequest)
		return
	}

	// Get PayPal plan ID from database
	var paypalPlanID string
	column := "paypal_plan_id_monthly"
	if billingCycle == "yearly" {
		column = "paypal_plan_id_yearly"
	}

	err := db.QueryRow(`SELECT `+column+` FROM package_plans WHERE id = $1`, planID).Scan(&paypalPlanID)
	if errors.Is(err, sql.ErrNoRows) {
		http.Error(w, "invalid plan", http.StatusBadRequest)
		return
	}
	if err != nil {
		log.Printf("[paypal] Error loading PayPal plan ID for %s/%s: %v", planID, billingCycle, err)
		http.Error(w, "failed to load PayPal plan", http.StatusInternalServerError)
		return
	}
	if strings.TrimSpace(paypalPlanID) == "" {
		log.Printf("[paypal] Plan ID not found for %s/%s", planID, billingCycle)
		http.Error(w, fmt.Sprintf("PayPal plan not configured for %s %s", planID, billingCycle), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"plan_id": paypalPlanID,
	})
}

// handlePayPalWebhook processes PayPal webhook events
func handlePayPalWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "error reading body", http.StatusBadRequest)
		return
	}

	if !verifyPayPalWebhookSignature(r, body) {
		log.Printf("[paypal webhook] signature verification failed, rejecting request")
		http.Error(w, "invalid webhook signature", http.StatusUnauthorized)
		return
	}

	var event struct {
		EventType string `json:"event_type"`
		Resource  struct {
			ID         string `json:"id"`
			Status     string `json:"status"`
			Subscriber struct {
				EmailAddress string `json:"email_address"`
			} `json:"subscriber"`
			PlanID string `json:"plan_id"`
		} `json:"resource"`
	}

	if err := json.Unmarshal(body, &event); err != nil {
		log.Printf("[paypal webhook] Error parsing: %v", err)
		http.Error(w, "error parsing webhook", http.StatusBadRequest)
		return
	}

	log.Printf("[paypal webhook] Event: %s, Subscription: %s, Status: %s",
		event.EventType, event.Resource.ID, event.Resource.Status)

	subscriptionID := event.Resource.ID
	email := event.Resource.Subscriber.EmailAddress

	switch event.EventType {
	case "BILLING.SUBSCRIPTION.CREATED", "BILLING.SUBSCRIPTION.ACTIVATED":
		// Activate subscription
		_, err := db.Exec(`
			UPDATE stations
			SET paypal_subscription_id = $1, is_suspended = false,
			    trial_started_at = NULL, trial_ends_at = NULL
			WHERE user_id = (SELECT id FROM users WHERE email = $2)
		`, subscriptionID, email)
		if err != nil {
			log.Printf("[paypal webhook] Failed to activate: %v", err)
		} else {
			log.Printf("[paypal webhook] Activated subscription for %s", email)
			var firstName, plan string
			_ = db.QueryRow(`SELECT u.first_name, COALESCE(s.plan,'') FROM users u LEFT JOIN stations s ON s.user_id = u.id WHERE u.email = $1`, email).Scan(&firstName, &plan)
			go func() {
				if err := sendSubscriptionConfirmedEmail(email, firstName, plan); err != nil {
					log.Printf("[email] subscription confirmed to %s: %v", email, err)
				}
			}()
		}

	case "BILLING.SUBSCRIPTION.CANCELLED", "BILLING.SUBSCRIPTION.SUSPENDED":
		// Suspend subscription
		_, err := db.Exec(`
			UPDATE stations 
			SET is_suspended = true
			WHERE paypal_subscription_id = $1
		`, subscriptionID)
		if err != nil {
			log.Printf("[paypal webhook] Failed to suspend: %v", err)
		} else {
			log.Printf("[paypal webhook] Suspended subscription %s", subscriptionID)
		}

	case "PAYMENT.SALE.COMPLETED":
		// Payment successful - ensure station is active
		_, err := db.Exec(`
			UPDATE stations 
			SET is_suspended = false
			WHERE paypal_subscription_id = $1
		`, subscriptionID)
		if err != nil {
			log.Printf("[paypal webhook] Failed to reactivate: %v", err)
		}

	case "BILLING.SUBSCRIPTION.PAYMENT.FAILED":
		log.Printf("[paypal webhook] Payment failed for subscription %s", subscriptionID)
		var firstName string
		_ = db.QueryRow(`SELECT u.first_name FROM users u INNER JOIN stations s ON s.user_id = u.id WHERE s.paypal_subscription_id = $1`, subscriptionID).Scan(&firstName)
		go func() {
			if err := sendSubscriptionFailedEmail(email, firstName); err != nil {
				log.Printf("[email] payment failed to %s: %v", email, err)
			}
		}()
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// handlePayPalSuccess handles successful subscription creation
func handlePayPalSuccess(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var body struct {
		SubscriptionID string `json:"subscription_id"`
		Plan           string `json:"plan"`
		BillingCycle   string `json:"billing_cycle"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	subscriptionID := strings.TrimSpace(body.SubscriptionID)
	if subscriptionID == "" {
		subscriptionID = strings.TrimSpace(r.URL.Query().Get("subscription_id"))
	}
	if subscriptionID == "" {
		http.Error(w, "missing subscription_id", http.StatusBadRequest)
		return
	}
	body.Plan = strings.ToLower(strings.TrimSpace(body.Plan))
	body.BillingCycle = strings.ToLower(strings.TrimSpace(body.BillingCycle))
	if body.Plan == "" {
		body.Plan = "starter"
	}
	if body.BillingCycle == "" {
		body.BillingCycle = "monthly"
	}
	if body.BillingCycle != "monthly" && body.BillingCycle != "yearly" {
		http.Error(w, "invalid billing cycle", http.StatusBadRequest)
		return
	}

	userID := r.Context().Value(contextKeyUserID).(string)
	email, _ := r.Context().Value(contextKeyEmail).(string)

	var planExists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM package_plans WHERE id = $1)`, body.Plan).Scan(&planExists); err != nil {
		log.Printf("[paypal] Error validating plan %q for user %s: %v", body.Plan, userID, err)
		http.Error(w, "failed to validate plan", http.StatusInternalServerError)
		return
	}
	if !planExists {
		http.Error(w, "invalid plan", http.StatusBadRequest)
		return
	}

	// Look up the PayPal plan ID we expect for the claimed plan/billing cycle so
	// we can confirm below that the subscription the client is reporting actually
	// matches what they say they bought (not a cheaper plan's subscription ID).
	var expectedPayPalPlanID string
	planColumn := "paypal_plan_id_monthly"
	if body.BillingCycle == "yearly" {
		planColumn = "paypal_plan_id_yearly"
	}
	if err := db.QueryRow(`SELECT `+planColumn+` FROM package_plans WHERE id = $1`, body.Plan).Scan(&expectedPayPalPlanID); err != nil {
		log.Printf("[paypal] Error loading expected PayPal plan ID for %s/%s user %s: %v", body.Plan, body.BillingCycle, userID, err)
		http.Error(w, "failed to validate plan", http.StatusInternalServerError)
		return
	}

	// Verify the subscription directly with PayPal rather than trusting the
	// client-submitted subscription_id/plan — this is what stops a user from
	// POSTing an arbitrary subscription ID and an expensive plan name to get
	// upgraded without ever paying.
	paypalStatus, paypalPlanID, err := getPayPalSubscription(subscriptionID)
	if err != nil {
		log.Printf("[paypal] Error verifying subscription %s for user %s: %v", subscriptionID, userID, err)
		http.Error(w, "failed to verify subscription with PayPal", http.StatusBadGateway)
		return
	}
	if paypalStatus != "ACTIVE" {
		log.Printf("[paypal] Subscription %s for user %s is not active (status=%s)", subscriptionID, userID, paypalStatus)
		http.Error(w, "subscription is not active", http.StatusPaymentRequired)
		return
	}
	if strings.TrimSpace(expectedPayPalPlanID) == "" || paypalPlanID != expectedPayPalPlanID {
		log.Printf("[paypal] Subscription %s plan mismatch for user %s: got %s, expected %s (%s/%s)",
			subscriptionID, userID, paypalPlanID, expectedPayPalPlanID, body.Plan, body.BillingCycle)
		http.Error(w, "subscription does not match requested plan", http.StatusBadRequest)
		return
	}

	if _, err := ensureStation(userID, email, "", ""); err != nil {
		log.Printf("[paypal] Error ensuring station for user %s: %v", userID, err)
		http.Error(w, "failed to prepare station", http.StatusInternalServerError)
		return
	}

	upgradeID, err := generateKey()
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback() //nolint:errcheck

	var oldPlan, oldBillingCycle string
	if err := tx.QueryRow(`SELECT plan, billing_cycle FROM stations WHERE user_id = $1 FOR UPDATE`, userID).
		Scan(&oldPlan, &oldBillingCycle); err != nil {
		log.Printf("[paypal] Error loading current plan for user %s: %v", userID, err)
		http.Error(w, "failed to load current plan", http.StatusInternalServerError)
		return
	}

	// Update station with PayPal subscription ID and active plan
	_, err = tx.Exec(`
		UPDATE stations 
		SET paypal_subscription_id = $1, is_suspended = false, plan = $2, billing_cycle = $3,
		    trial_started_at = NULL, trial_ends_at = NULL
		WHERE user_id = $4
	`, subscriptionID, body.Plan, body.BillingCycle, userID)

	if err != nil {
		log.Printf("[paypal] Error saving subscription: %v", err)
		http.Error(w, "error saving subscription", http.StatusInternalServerError)
		return
	}
	if _, err := tx.Exec(
		`INSERT INTO package_upgrade_history
			(id, user_id, old_plan, new_plan, old_billing_cycle, new_billing_cycle, status, payment_reference)
		 VALUES ($1, $2, $3, $4, $5, $6, 'active', $7)`,
		upgradeID, userID, oldPlan, body.Plan, oldBillingCycle, body.BillingCycle, subscriptionID,
	); err != nil {
		log.Printf("[paypal] Error recording subscription history for user %s: %v", userID, err)
		http.Error(w, "failed to record subscription", http.StatusInternalServerError)
		return
	}
	if err := tx.Commit(); err != nil {
		log.Printf("[paypal] Error committing subscription for user %s: %v", userID, err)
		http.Error(w, "error saving subscription", http.StatusInternalServerError)
		return
	}

	log.Printf("[paypal] Subscription %s linked to user %s", subscriptionID, userID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "Subscription activated",
	})
}

// ── Stripe Subscription Integration ──────────────────────────────────────────

func stripeAPIRequest(method, path string, form url.Values) ([]byte, int, error) {
	if strings.TrimSpace(stripeSecretKey) == "" {
		return nil, 0, fmt.Errorf("stripe credentials not configured")
	}

	var bodyReader io.Reader
	if form != nil {
		bodyReader = strings.NewReader(form.Encode())
	}

	req, err := http.NewRequest(method, "https://api.stripe.com"+path, bodyReader)
	if err != nil {
		return nil, 0, err
	}
	req.SetBasicAuth(stripeSecretKey, "")
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}

	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	return respBody, resp.StatusCode, nil
}

func verifyStripeWebhookSignature(signatureHeader string, body []byte) bool {
	if strings.TrimSpace(stripeWebhookSecret) == "" || strings.TrimSpace(signatureHeader) == "" {
		return false
	}

	parts := strings.Split(signatureHeader, ",")
	timestamp := ""
	v1Sigs := make([]string, 0)
	for _, part := range parts {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}
		if kv[0] == "t" {
			timestamp = kv[1]
		}
		if kv[0] == "v1" {
			v1Sigs = append(v1Sigs, kv[1])
		}
	}
	if timestamp == "" || len(v1Sigs) == 0 {
		return false
	}

	tsInt, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return false
	}
	now := time.Now().Unix()
	if tsInt < now-300 || tsInt > now+300 {
		return false
	}

	signedPayload := timestamp + "." + string(body)
	mac := hmac.New(sha256.New, []byte(stripeWebhookSecret))
	mac.Write([]byte(signedPayload))
	expected := hex.EncodeToString(mac.Sum(nil))

	for _, sig := range v1Sigs {
		if hmac.Equal([]byte(expected), []byte(sig)) {
			return true
		}
	}
	return false
}

func extractStringValue(raw json.RawMessage, key string) string {
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return ""
	}
	v, _ := m[key].(string)
	return strings.TrimSpace(v)
}

func handleStripeCreateCheckoutSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if strings.TrimSpace(stripeSecretKey) == "" {
		http.Error(w, "Stripe credentials not configured", http.StatusBadRequest)
		return
	}

	var body struct {
		Plan         string `json:"plan"`
		BillingCycle string `json:"billing_cycle"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	body.Plan = strings.ToLower(strings.TrimSpace(body.Plan))
	body.BillingCycle = strings.ToLower(strings.TrimSpace(body.BillingCycle))
	if body.Plan == "" {
		body.Plan = "starter"
	}
	if body.BillingCycle == "" {
		body.BillingCycle = "monthly"
	}
	if body.BillingCycle != "monthly" && body.BillingCycle != "yearly" {
		http.Error(w, "invalid billing cycle", http.StatusBadRequest)
		return
	}

	var (
		displayName  string
		monthlyPrice float64
		yearlyPrice  float64
		monthlySale  int
		yearlySale   int
	)
	err := db.QueryRow(`
		SELECT display_name, monthly_price, yearly_price,
		       COALESCE(monthly_sale_percent, 0), COALESCE(yearly_sale_percent, 0)
		FROM package_plans
		WHERE id = $1
	`, body.Plan).Scan(&displayName, &monthlyPrice, &yearlyPrice, &monthlySale, &yearlySale)
	if errors.Is(err, sql.ErrNoRows) {
		http.Error(w, "invalid plan", http.StatusBadRequest)
		return
	}
	if err != nil {
		log.Printf("[stripe] plan query error for %s: %v", body.Plan, err)
		http.Error(w, "failed to load plan", http.StatusInternalServerError)
		return
	}

	effectivePrice := monthlyPrice
	interval := "month"
	if body.BillingCycle == "yearly" {
		effectivePrice = yearlyPrice
		interval = "year"
		if yearlySale > 0 && yearlySale < 100 {
			effectivePrice = yearlyPrice * (1 - float64(yearlySale)/100.0)
		}
	} else if monthlySale > 0 && monthlySale < 100 {
		effectivePrice = monthlyPrice * (1 - float64(monthlySale)/100.0)
	}
	unitAmount := int64(math.Round(effectivePrice * 100))
	if unitAmount <= 0 {
		http.Error(w, "invalid plan price", http.StatusBadRequest)
		return
	}

	userID, _ := r.Context().Value(contextKeyUserID).(string)
	email, _ := r.Context().Value(contextKeyEmail).(string)

	frontendBase := strings.TrimSpace(r.Header.Get("Origin"))
	if frontendBase == "" {
		frontendBase = strings.TrimSpace(os.Getenv("FRONTEND_BASE_URL"))
	}
	if frontendBase == "" {
		frontendBase = "http://localhost:5173"
	}

	q := url.Values{}
	q.Set("plan", body.Plan)
	q.Set("billing", body.BillingCycle)
	q.Set("provider", "stripe")
	successURL := frontendBase + "/payment?" + q.Encode() + "&status=success&session_id={CHECKOUT_SESSION_ID}"
	cancelURL := frontendBase + "/payment?" + q.Encode() + "&status=cancel"

	form := url.Values{}
	form.Set("mode", "subscription")
	form.Set("success_url", successURL)
	form.Set("cancel_url", cancelURL)
	form.Set("client_reference_id", userID)
	form.Set("customer_email", email)
	form.Set("metadata[plan]", body.Plan)
	form.Set("metadata[billing_cycle]", body.BillingCycle)
	form.Set("metadata[user_id]", userID)
	form.Set("line_items[0][quantity]", "1")
	form.Set("line_items[0][price_data][currency]", "usd")
	form.Set("line_items[0][price_data][unit_amount]", strconv.FormatInt(unitAmount, 10))
	form.Set("line_items[0][price_data][recurring][interval]", interval)
	form.Set("line_items[0][price_data][product_data][name]", fmt.Sprintf("%s (%s)", displayName, strings.Title(body.BillingCycle)))

	respBody, status, err := stripeAPIRequest(http.MethodPost, "/v1/checkout/sessions", form)
	if err != nil {
		log.Printf("[stripe] checkout create error: %v", err)
		http.Error(w, "failed to create checkout session", http.StatusInternalServerError)
		return
	}
	if status >= 300 {
		log.Printf("[stripe] checkout create failed (%d): %s", status, string(respBody))
		http.Error(w, "failed to create checkout session", http.StatusBadRequest)
		return
	}

	var session struct {
		ID  string `json:"id"`
		URL string `json:"url"`
	}
	if err := json.Unmarshal(respBody, &session); err != nil || strings.TrimSpace(session.URL) == "" {
		log.Printf("[stripe] invalid checkout response: %s", string(respBody))
		http.Error(w, "invalid checkout response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"session_id": session.ID,
		"url":        session.URL,
	})
}

func handleStripeSuccess(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var body struct {
		SessionID string `json:"session_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	sessionID := strings.TrimSpace(body.SessionID)
	if sessionID == "" {
		sessionID = strings.TrimSpace(r.URL.Query().Get("session_id"))
	}
	if sessionID == "" {
		http.Error(w, "missing session_id", http.StatusBadRequest)
		return
	}

	respBody, status, err := stripeAPIRequest(http.MethodGet, "/v1/checkout/sessions/"+url.PathEscape(sessionID)+"?expand[]=subscription", nil)
	if err != nil {
		log.Printf("[stripe] session lookup error: %v", err)
		http.Error(w, "failed to verify session", http.StatusInternalServerError)
		return
	}
	if status >= 300 {
		log.Printf("[stripe] session lookup failed (%d): %s", status, string(respBody))
		http.Error(w, "invalid Stripe session", http.StatusBadRequest)
		return
	}

	var session struct {
		ID                string            `json:"id"`
		Status            string            `json:"status"`
		ClientReferenceID string            `json:"client_reference_id"`
		Metadata          map[string]string `json:"metadata"`
		Subscription      interface{}       `json:"subscription"`
	}
	if err := json.Unmarshal(respBody, &session); err != nil {
		http.Error(w, "invalid Stripe response", http.StatusBadRequest)
		return
	}
	if session.Status != "complete" {
		http.Error(w, "checkout not complete", http.StatusBadRequest)
		return
	}

	userID := r.Context().Value(contextKeyUserID).(string)
	email, _ := r.Context().Value(contextKeyEmail).(string)
	if session.ClientReferenceID != "" && session.ClientReferenceID != userID {
		http.Error(w, "session does not belong to user", http.StatusForbidden)
		return
	}

	plan := strings.ToLower(strings.TrimSpace(session.Metadata["plan"]))
	billingCycle := strings.ToLower(strings.TrimSpace(session.Metadata["billing_cycle"]))
	if plan == "" {
		plan = "starter"
	}
	if billingCycle == "" {
		billingCycle = "monthly"
	}
	if billingCycle != "monthly" && billingCycle != "yearly" {
		http.Error(w, "invalid billing cycle", http.StatusBadRequest)
		return
	}

	var planExists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM package_plans WHERE id = $1)`, plan).Scan(&planExists); err != nil {
		log.Printf("[stripe] Error validating plan %q for user %s: %v", plan, userID, err)
		http.Error(w, "failed to validate plan", http.StatusInternalServerError)
		return
	}
	if !planExists {
		http.Error(w, "invalid plan", http.StatusBadRequest)
		return
	}

	subscriptionID := ""
	switch v := session.Subscription.(type) {
	case string:
		subscriptionID = strings.TrimSpace(v)
	case map[string]interface{}:
		if idVal, ok := v["id"].(string); ok {
			subscriptionID = strings.TrimSpace(idVal)
		}
	}
	if subscriptionID == "" {
		subscriptionID = session.ID
	}

	if _, err := ensureStation(userID, email, "", ""); err != nil {
		log.Printf("[stripe] Error ensuring station for user %s: %v", userID, err)
		http.Error(w, "failed to prepare station", http.StatusInternalServerError)
		return
	}

	upgradeID, err := generateKey()
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback() //nolint:errcheck

	var oldPlan, oldBillingCycle string
	if err := tx.QueryRow(`SELECT plan, billing_cycle FROM stations WHERE user_id = $1 FOR UPDATE`, userID).
		Scan(&oldPlan, &oldBillingCycle); err != nil {
		log.Printf("[stripe] Error loading current plan for user %s: %v", userID, err)
		http.Error(w, "failed to load current plan", http.StatusInternalServerError)
		return
	}

	_, err = tx.Exec(`
		UPDATE stations
		SET stripe_subscription_id = $1, is_suspended = false, plan = $2, billing_cycle = $3,
		    trial_started_at = NULL, trial_ends_at = NULL
		WHERE user_id = $4
	`, subscriptionID, plan, billingCycle, userID)
	if err != nil {
		log.Printf("[stripe] Error saving subscription: %v", err)
		http.Error(w, "error saving subscription", http.StatusInternalServerError)
		return
	}
	if _, err := tx.Exec(
		`INSERT INTO package_upgrade_history
			(id, user_id, old_plan, new_plan, old_billing_cycle, new_billing_cycle, status, payment_reference)
		 VALUES ($1, $2, $3, $4, $5, $6, 'active', $7)`,
		upgradeID, userID, oldPlan, plan, oldBillingCycle, billingCycle, subscriptionID,
	); err != nil {
		log.Printf("[stripe] Error recording subscription history for user %s: %v", userID, err)
		http.Error(w, "failed to record subscription", http.StatusInternalServerError)
		return
	}
	if err := tx.Commit(); err != nil {
		log.Printf("[stripe] Error committing subscription for user %s: %v", userID, err)
		http.Error(w, "error saving subscription", http.StatusInternalServerError)
		return
	}

	log.Printf("[stripe] Subscription %s linked to user %s", subscriptionID, userID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "Subscription activated",
	})
}

func handleStripeWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "error reading body", http.StatusBadRequest)
		return
	}
	if !verifyStripeWebhookSignature(r.Header.Get("Stripe-Signature"), body) {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	var event struct {
		Type string `json:"type"`
		Data struct {
			Object json.RawMessage `json:"object"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &event); err != nil {
		http.Error(w, "invalid webhook payload", http.StatusBadRequest)
		return
	}

	switch event.Type {
	case "checkout.session.completed":
		var obj struct {
			ID                string            `json:"id"`
			ClientReferenceID string            `json:"client_reference_id"`
			Metadata          map[string]string `json:"metadata"`
			Subscription      interface{}       `json:"subscription"`
		}
		if err := json.Unmarshal(event.Data.Object, &obj); err == nil {
			subscriptionID := ""
			switch v := obj.Subscription.(type) {
			case string:
				subscriptionID = strings.TrimSpace(v)
			case map[string]interface{}:
				if idVal, ok := v["id"].(string); ok {
					subscriptionID = strings.TrimSpace(idVal)
				}
			}
			if subscriptionID == "" {
				subscriptionID = strings.TrimSpace(obj.ID)
			}
			plan := strings.ToLower(strings.TrimSpace(obj.Metadata["plan"]))
			billingCycle := strings.ToLower(strings.TrimSpace(obj.Metadata["billing_cycle"]))
			if billingCycle != "monthly" && billingCycle != "yearly" {
				billingCycle = ""
			}
			_, _ = db.Exec(`
				UPDATE stations
				SET stripe_subscription_id = $1,
				    is_suspended = false,
				    plan = CASE WHEN $2 <> '' THEN $2 ELSE plan END,
				    billing_cycle = CASE WHEN $3 <> '' THEN $3 ELSE billing_cycle END,
				    trial_started_at = NULL,
				    trial_ends_at = NULL
				WHERE user_id = $4
			`, subscriptionID, plan, billingCycle, obj.ClientReferenceID)
		}

	case "invoice.paid":
		subscriptionID := extractStringValue(event.Data.Object, "subscription")
		if subscriptionID != "" {
			_, _ = db.Exec(`UPDATE stations SET is_suspended = false WHERE stripe_subscription_id = $1`, subscriptionID)
		}

	case "invoice.payment_failed", "customer.subscription.deleted":
		subscriptionID := extractStringValue(event.Data.Object, "subscription")
		if subscriptionID == "" {
			subscriptionID = extractStringValue(event.Data.Object, "id")
		}
		if subscriptionID != "" {
			_, _ = db.Exec(`UPDATE stations SET is_suspended = true WHERE stripe_subscription_id = $1`, subscriptionID)
		}

	case "customer.subscription.updated":
		subscriptionID := extractStringValue(event.Data.Object, "id")
		status := strings.ToLower(extractStringValue(event.Data.Object, "status"))
		if subscriptionID != "" {
			if status == "active" || status == "trialing" {
				_, _ = db.Exec(`UPDATE stations SET is_suspended = false WHERE stripe_subscription_id = $1`, subscriptionID)
			} else if status == "past_due" || status == "unpaid" || status == "canceled" {
				_, _ = db.Exec(`UPDATE stations SET is_suspended = true WHERE stripe_subscription_id = $1`, subscriptionID)
			}
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// ── PayPal Plan Sync (Admin) ──────────────────────────────────────────────────

// paypalAPIRequest performs an authenticated JSON request to the PayPal REST API.
func paypalAPIRequest(method, path string, payload interface{}) ([]byte, int, error) {
	token, err := getPayPalAccessToken()
	if err != nil {
		return nil, 0, fmt.Errorf("auth: %w", err)
	}
	var bodyReader io.Reader
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, 0, err
		}
		bodyReader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, getPayPalBaseURL()+path, bodyReader)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Prefer", "return=representation")

	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	return respBody, resp.StatusCode, nil
}

// ensurePayPalProduct creates or returns a single product representing our subscriptions.
func ensurePayPalProduct() (string, error) {
	// Try to find by querying existing products (paginated). PayPal does not allow lookup by
	// arbitrary name, so we cache the product ID in a simple settings table.
	var productID string
	_ = db.QueryRow(`SELECT value FROM app_settings WHERE key = 'paypal_product_id'`).Scan(&productID)
	if strings.TrimSpace(productID) != "" {
		return productID, nil
	}

	payload := map[string]interface{}{
		"name":        "Radio In One Stop Subscription",
		"description": "Radio In One Stop streaming subscription plans",
		"type":        "SERVICE",
		"category":    "SOFTWARE",
	}
	body, status, err := paypalAPIRequest(http.MethodPost, "/v1/catalogs/products", payload)
	if err != nil {
		return "", err
	}
	if status >= 300 {
		return "", fmt.Errorf("create product failed (%d): %s", status, string(body))
	}
	var result struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", err
	}
	if result.ID == "" {
		return "", fmt.Errorf("paypal returned no product id: %s", string(body))
	}
	_, _ = db.Exec(`
		INSERT INTO app_settings (key, value) VALUES ('paypal_product_id', $1)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
	`, result.ID)
	return result.ID, nil
}

// createPayPalPlan creates a subscription plan in PayPal and returns the plan ID.
func createPayPalPlan(productID, name, description string, priceUSD float64, intervalUnit string) (string, error) {
	payload := map[string]interface{}{
		"product_id":  productID,
		"name":        name,
		"description": description,
		"status":      "ACTIVE",
		"billing_cycles": []map[string]interface{}{
			{
				"frequency": map[string]interface{}{
					"interval_unit":  intervalUnit, // "MONTH" or "YEAR"
					"interval_count": 1,
				},
				"tenure_type":  "REGULAR",
				"sequence":     1,
				"total_cycles": 0, // 0 = infinite
				"pricing_scheme": map[string]interface{}{
					"fixed_price": map[string]interface{}{
						"value":         fmt.Sprintf("%.2f", priceUSD),
						"currency_code": "USD",
					},
				},
			},
		},
		"payment_preferences": map[string]interface{}{
			"auto_bill_outstanding":     true,
			"setup_fee_failure_action":  "CONTINUE",
			"payment_failure_threshold": 3,
		},
	}
	body, status, err := paypalAPIRequest(http.MethodPost, "/v1/billing/plans", payload)
	if err != nil {
		return "", err
	}
	if status >= 300 {
		return "", fmt.Errorf("create plan failed (%d): %s", status, string(body))
	}
	var result struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", err
	}
	if result.ID == "" {
		return "", fmt.Errorf("paypal returned no plan id: %s", string(body))
	}
	return result.ID, nil
}

// handleAdminPayPalSyncPlans creates replacement PayPal subscription plans for
// every package_plan and saves the new plan IDs back to the database. PayPal plan
// prices are immutable, so replacing them is required after a price or sale change.
func handleAdminPayPalSyncPlans(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if strings.TrimSpace(paypalClientID) == "" || strings.TrimSpace(paypalSecret) == "" {
		http.Error(w, "PayPal credentials not configured", http.StatusBadRequest)
		return
	}

	// Ensure settings table exists for caching product ID.
	_, _ = db.Exec(`CREATE TABLE IF NOT EXISTS app_settings (key TEXT PRIMARY KEY, value TEXT NOT NULL DEFAULT '')`)

	productID, err := ensurePayPalProduct()
	if err != nil {
		log.Printf("[paypal sync] product error: %v", err)
		http.Error(w, "failed to create PayPal product: "+err.Error(), http.StatusInternalServerError)
		return
	}

	rows, err := db.Query(`
		SELECT id, display_name, monthly_price, yearly_price,
		       COALESCE(monthly_sale_percent, 0), COALESCE(yearly_sale_percent, 0),
		       paypal_plan_id_monthly, paypal_plan_id_yearly
		FROM package_plans
		ORDER BY monthly_price ASC
	`)
	if err != nil {
		http.Error(w, "failed to load plans: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type planResult struct {
		ID            string `json:"id"`
		Name          string `json:"name"`
		MonthlyPlanID string `json:"monthly_plan_id"`
		YearlyPlanID  string `json:"yearly_plan_id"`
		Status        string `json:"status"`
		Error         string `json:"error,omitempty"`
	}
	var results []planResult
	overallErr := ""

	for rows.Next() {
		var (
			id, name, existingMonthly, existingYearly string
			monthlyPrice, yearlyPrice                 float64
			monthlySale, yearlySale                   int
		)
		if err := rows.Scan(&id, &name, &monthlyPrice, &yearlyPrice, &monthlySale, &yearlySale, &existingMonthly, &existingYearly); err != nil {
			overallErr = err.Error()
			break
		}

		res := planResult{ID: id, Name: name, MonthlyPlanID: existingMonthly, YearlyPlanID: existingYearly, Status: "skipped"}

		// Apply sale discounts to PayPal price (matches what the customer sees)
		effectiveMonthly := monthlyPrice
		if monthlySale > 0 && monthlySale < 100 {
			effectiveMonthly = monthlyPrice * (1 - float64(monthlySale)/100.0)
		}
		effectiveYearly := yearlyPrice
		if yearlySale > 0 && yearlySale < 100 {
			effectiveYearly = yearlyPrice * (1 - float64(yearlySale)/100.0)
		}

		if effectiveMonthly > 0 {
			planID, err := createPayPalPlan(productID, name+" (Monthly)", name+" plan billed monthly", effectiveMonthly, "MONTH")
			if err != nil {
				res.Status = "error"
				res.Error = "monthly: " + err.Error()
				log.Printf("[paypal sync] %s monthly: %v", id, err)
			} else {
				res.MonthlyPlanID = planID
				res.Status = "created"
				_, _ = db.Exec(`UPDATE package_plans SET paypal_plan_id_monthly = $1 WHERE id = $2`, planID, id)
			}
		}
		if effectiveYearly > 0 {
			planID, err := createPayPalPlan(productID, name+" (Yearly)", name+" plan billed yearly", effectiveYearly, "YEAR")
			if err != nil {
				if res.Status != "error" {
					res.Status = "error"
				}
				if res.Error != "" {
					res.Error += "; "
				}
				res.Error += "yearly: " + err.Error()
				log.Printf("[paypal sync] %s yearly: %v", id, err)
			} else {
				res.YearlyPlanID = planID
				if res.Status != "error" {
					res.Status = "created"
				}
				_, _ = db.Exec(`UPDATE package_plans SET paypal_plan_id_yearly = $1 WHERE id = $2`, planID, id)
			}
		}

		results = append(results, res)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"product_id": productID,
		"mode":       paypalMode,
		"plans":      results,
		"error":      overallErr,
	})
}

var supportReasons = map[string]bool{
	"pricing": true, "station_not_streaming": true, "account": true, "technical": true, "other": true,
}

// handleSupportMessages accepts messages from visitors and signed-in users.
func handleSupportMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct{ Email, Reason, Station, Message string }
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	body.Email = strings.ToLower(strings.TrimSpace(body.Email))
	body.Reason = strings.TrimSpace(body.Reason)
	body.Message = strings.TrimSpace(body.Message)
	body.Station = strings.TrimSpace(body.Station)
	address, err := mail.ParseAddress(body.Email)
	if err != nil || address.Address != body.Email {
		http.Error(w, "enter a valid email address", http.StatusBadRequest)
		return
	}
	if !supportReasons[body.Reason] {
		http.Error(w, "select a valid reason", http.StatusBadRequest)
		return
	}
	if body.Station == "" || len(body.Station) > 300 {
		http.Error(w, "enter the station name", http.StatusBadRequest)
		return
	}
	if len(body.Message) < 10 || len(body.Message) > 4000 {
		http.Error(w, "message must be between 10 and 4000 characters", http.StatusBadRequest)
		return
	}
	id, err := generateKey()
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	if _, err = db.Exec(`INSERT INTO support_messages (id, email, reason, station, message) VALUES ($1,$2,$3,$4,$5)`, id, body.Email, body.Reason, body.Station, body.Message); err != nil {
		log.Printf("[support] create message: %v", err)
		http.Error(w, "could not send message", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"id": id, "status": "sent"})
}

func handleAdminSupport(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		rows, err := db.Query(`SELECT id,email,reason,station,message,status,admin_reply,created_at::text,COALESCE(replied_at::text,'') FROM support_messages ORDER BY created_at DESC`)
		if err != nil {
			http.Error(w, "server error", http.StatusInternalServerError)
			return
		}
		defer rows.Close()
		type item struct {
			ID         string `json:"id"`
			Email      string `json:"email"`
			Reason     string `json:"reason"`
			Station    string `json:"station"`
			Message    string `json:"message"`
			Status     string `json:"status"`
			AdminReply string `json:"adminReply"`
			CreatedAt  string `json:"createdAt"`
			RepliedAt  string `json:"repliedAt"`
		}
		items := []item{}
		for rows.Next() {
			var x item
			if rows.Scan(&x.ID, &x.Email, &x.Reason, &x.Station, &x.Message, &x.Status, &x.AdminReply, &x.CreatedAt, &x.RepliedAt) == nil {
				items = append(items, x)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(items)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		ID    string `json:"id"`
		Reply string `json:"reply"`
	}
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&body) != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	body.Reply = strings.TrimSpace(body.Reply)
	if body.ID == "" || len(body.Reply) < 1 || len(body.Reply) > 4000 {
		http.Error(w, "a reply is required", http.StatusBadRequest)
		return
	}
	var email, original string
	if err := db.QueryRow(`SELECT email,message FROM support_messages WHERE id=$1`, body.ID).Scan(&email, &original); err != nil {
		http.Error(w, "message not found", http.StatusNotFound)
		return
	}
	content := fmt.Sprintf(`<h2 style="margin:0 0 16px;color:white;font-size:20px;">Reply from Radio In One Stop</h2><p style="color:#d1d5db;line-height:1.7;white-space:pre-wrap;">%s</p><hr style="border:0;border-top:1px solid #374151;margin:24px 0;"><p style="color:#6b7280;font-size:13px;">Your original message:</p><p style="color:#9ca3af;font-size:13px;line-height:1.6;white-space:pre-wrap;">%s</p>`, html.EscapeString(body.Reply), html.EscapeString(original))
	if err := sendMail(email, "Reply to your Radio In One Stop message", emailBase("Support reply", "We replied to your message", content)); err != nil {
		log.Printf("[support] reply email to %s: %v", email, err)
		http.Error(w, "email could not be sent", http.StatusBadGateway)
		return
	}
	if _, err := db.Exec(`UPDATE support_messages SET admin_reply=$1,status='replied',replied_at=NOW() WHERE id=$2`, body.Reply, body.ID); err != nil {
		http.Error(w, "reply sent but status could not be saved", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"success": true})
}

// handleAdminMarketing - GET/PUT marketing content for public pages
func handleAdminMarketing(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		getAdminMarketing(w, r)
	case http.MethodPut:
		updateAdminMarketing(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func getAdminMarketing(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(`
		SELECT id, page, section, content_type, content, is_active
		FROM marketing_content
		ORDER BY page, section
	`)
	if err != nil {
		log.Printf("[admin] Error fetching marketing content: %v", err)
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type MarketingContent struct {
		ID          string `json:"id"`
		Page        string `json:"page"`
		Section     string `json:"section"`
		ContentType string `json:"contentType"`
		Content     string `json:"content"`
		IsActive    bool   `json:"isActive"`
	}

	var content []MarketingContent
	for rows.Next() {
		var c MarketingContent
		if err := rows.Scan(&c.ID, &c.Page, &c.Section, &c.ContentType, &c.Content, &c.IsActive); err != nil {
			continue
		}
		content = append(content, c)
	}

	if content == nil {
		content = []MarketingContent{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(content)
}

func updateAdminMarketing(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID          string `json:"id"`
		Page        string `json:"page"`
		Section     string `json:"section"`
		ContentType string `json:"contentType"`
		Content     string `json:"content"`
		IsActive    bool   `json:"isActive"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if body.ID == "" {
		// Insert new marketing content
		newID, _ := generateKey()
		_, err := db.Exec(`
			INSERT INTO marketing_content (id, page, section, content_type, content, is_active)
			VALUES ($1, $2, $3, $4, $5, $6)
		`, newID, body.Page, body.Section, body.ContentType, body.Content, body.IsActive)

		if err != nil {
			log.Printf("[admin] Error creating marketing content: %v", err)
			http.Error(w, "failed to create marketing content", http.StatusInternalServerError)
			return
		}
	} else {
		// Update existing marketing content
		_, err := db.Exec(`
			UPDATE marketing_content
			SET page = $1, section = $2, content_type = $3, content = $4, is_active = $5
			WHERE id = $6
		`, body.Page, body.Section, body.ContentType, body.Content, body.IsActive, body.ID)

		if err != nil {
			log.Printf("[admin] Error updating marketing content: %v", err)
			http.Error(w, "failed to update marketing content", http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "Marketing content updated successfully",
	})
}

// ─── Song scheduler ───────────────────────────────────────────────────────────

type Schedule struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	SongID      string          `json:"song_id"`
	Title       string          `json:"title"`
	Artist      string          `json:"artist"`
	SourceType  string          `json:"source_type"`
	SourceURL   string          `json:"source_url"`
	Playlist    []ScheduleTrack `json:"playlist"`
	TriggerTime time.Time       `json:"trigger_time"`
	Enabled     bool            `json:"enabled"`
	Recurring   string          `json:"recurring"`
	Triggered   bool            `json:"triggered"`
}

type ScheduleTrack struct {
	SongID string `json:"song_id"`
	Title  string `json:"title"`
	Artist string `json:"artist"`
}

type scheduleEvent struct {
	Type     string   `json:"type"`
	Schedule Schedule `json:"schedule"`
}

type scheduleSSEBroker struct {
	mu      sync.RWMutex
	clients map[string]map[chan []byte]struct{}
}

var schedulerEvents = &scheduleSSEBroker{clients: make(map[string]map[chan []byte]struct{})}

func (b *scheduleSSEBroker) register(userID string) chan []byte {
	ch := make(chan []byte, 8)
	b.mu.Lock()
	if b.clients[userID] == nil {
		b.clients[userID] = make(map[chan []byte]struct{})
	}
	b.clients[userID][ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

func (b *scheduleSSEBroker) unregister(userID string, ch chan []byte) {
	b.mu.Lock()
	if clients := b.clients[userID]; clients != nil {
		delete(clients, ch)
		if len(clients) == 0 {
			delete(b.clients, userID)
		}
	}
	b.mu.Unlock()
}

func (b *scheduleSSEBroker) broadcast(userID string, event scheduleEvent) {
	payload, err := json.Marshal(event)
	if err != nil {
		return
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	for ch := range b.clients[userID] {
		select {
		case ch <- payload:
		default:
			log.Printf("[scheduler] dropping event for slow SSE client user=%s", userID)
		}
	}
}

func scanSchedule(scanner interface{ Scan(...interface{}) error }) (Schedule, error) {
	var s Schedule
	var playlistJSON []byte
	err := scanner.Scan(&s.ID, &s.Name, &s.SongID, &s.Title, &s.Artist, &s.SourceType, &s.SourceURL, &playlistJSON, &s.TriggerTime, &s.Enabled, &s.Recurring, &s.Triggered)
	if err == nil && len(playlistJSON) > 0 {
		_ = json.Unmarshal(playlistJSON, &s.Playlist)
	}
	return s, err
}

const scheduleColumns = `id, name, song_id, title, artist, source_type, source_url, playlist, trigger_time, enabled, recurring, triggered`

func validRecurrence(value string) bool {
	return value == "none" || value == "daily" || value == "weekly" || value == "monthly" || value == "yearly"
}

func validScheduleSource(value string) bool {
	return value == "library" || value == "url" || value == "playlist"
}

func nextScheduleTime(trigger time.Time, recurring string) time.Time {
	switch recurring {
	case "daily":
		return trigger.Add(24 * time.Hour)
	case "weekly":
		return trigger.Add(7 * 24 * time.Hour)
	case "monthly":
		return trigger.AddDate(0, 1, 0)
	case "yearly":
		return trigger.AddDate(1, 0, 0)
	default:
		return trigger
	}
}

func triggerSchedule(userID, scheduleID string, manual bool) (Schedule, error) {
	tx, err := db.Begin()
	if err != nil {
		return Schedule{}, err
	}
	defer tx.Rollback()

	query := `SELECT ` + scheduleColumns + ` FROM schedules WHERE id = $1 AND user_id = $2 FOR UPDATE`
	s, err := scanSchedule(tx.QueryRow(query, scheduleID, userID))
	if err != nil {
		return Schedule{}, err
	}
	if !manual && (!s.Enabled || s.Triggered) {
		return Schedule{}, sql.ErrNoRows
	}

	if manual {
		// Manual trigger plays now without changing the programmed recurrence.
	} else if s.Recurring == "none" {
		s.Triggered = true
		_, err = tx.Exec(`UPDATE schedules SET triggered = true, updated_at = NOW() WHERE id = $1`, s.ID)
	} else {
		s.TriggerTime = nextScheduleTime(s.TriggerTime, s.Recurring)
		s.Triggered = false
		_, err = tx.Exec(`UPDATE schedules SET trigger_time = $1, triggered = false, updated_at = NOW() WHERE id = $2`, s.TriggerTime, s.ID)
	}
	if err != nil {
		return Schedule{}, err
	}
	if err = tx.Commit(); err != nil {
		return Schedule{}, err
	}
	schedulerEvents.broadcast(userID, scheduleEvent{Type: "trigger", Schedule: s})
	log.Printf("[scheduler] triggered user=%s schedule=%s song=%q manual=%v", userID, s.ID, s.Title, manual)
	return s, nil
}

func runSchedulerLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for now := range ticker.C {
		rows, err := db.Query(`
			SELECT id, user_id
			FROM schedules
			WHERE enabled = true
			  AND triggered = false
			  AND trigger_time <= $1
			  AND trigger_time >= $2
			ORDER BY trigger_time
		`, now.UTC(), now.UTC().Add(-time.Minute))
		if err != nil {
			log.Printf("[scheduler] due query error: %v", err)
			continue
		}
		var due [][2]string
		for rows.Next() {
			var id, userID string
			if rows.Scan(&id, &userID) == nil {
				due = append(due, [2]string{id, userID})
			}
		}
		rows.Close()
		for _, item := range due {
			if _, err := triggerSchedule(item[1], item[0], false); err != nil && err != sql.ErrNoRows {
				log.Printf("[scheduler] trigger error schedule=%s: %v", item[0], err)
			}
		}
	}
}

func handleSchedules(w http.ResponseWriter, r *http.Request) {
	userID, _ := r.Context().Value(contextKeyUserID).(string)
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		rows, err := db.Query(`SELECT `+scheduleColumns+` FROM schedules WHERE user_id = $1 ORDER BY trigger_time`, userID)
		if err != nil {
			http.Error(w, "failed to load schedules", http.StatusInternalServerError)
			return
		}
		defer rows.Close()
		schedules := make([]Schedule, 0)
		for rows.Next() {
			s, scanErr := scanSchedule(rows)
			if scanErr == nil {
				schedules = append(schedules, s)
			}
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"schedules": schedules})

	case http.MethodPost:
		var body Schedule
		if json.NewDecoder(r.Body).Decode(&body) != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		body.Name = strings.TrimSpace(body.Name)
		body.SongID = strings.TrimSpace(body.SongID)
		body.Title = strings.TrimSpace(body.Title)
		body.Artist = strings.TrimSpace(body.Artist)
		body.SourceType = strings.ToLower(strings.TrimSpace(body.SourceType))
		body.SourceURL = strings.TrimSpace(body.SourceURL)
		body.Recurring = strings.ToLower(strings.TrimSpace(body.Recurring))
		if body.SourceType == "" {
			body.SourceType = "library"
		}
		if body.SongID == "" || body.Title == "" || body.TriggerTime.IsZero() || !validRecurrence(body.Recurring) || !validScheduleSource(body.SourceType) {
			http.Error(w, "song_id, title, trigger_time, recurring, and source_type are required", http.StatusBadRequest)
			return
		}
		if body.SourceType == "url" && body.SourceURL == "" {
			http.Error(w, "source_url is required for URL schedules", http.StatusBadRequest)
			return
		}
		if body.SourceType == "playlist" && len(body.Playlist) == 0 {
			http.Error(w, "playlist tracks are required", http.StatusBadRequest)
			return
		}
		playlistJSON, _ := json.Marshal(body.Playlist)
		id, _ := generateKey()
		body.ID = id
		body.Enabled = true
		body.Triggered = false
		_, err := db.Exec(`
			INSERT INTO schedules (id, user_id, name, song_id, title, artist, source_type, source_url, playlist, trigger_time, enabled, recurring, triggered)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,false)
		`, body.ID, userID, body.Name, body.SongID, body.Title, body.Artist, body.SourceType, body.SourceURL, playlistJSON, body.TriggerTime.UTC(), body.Enabled, body.Recurring)
		if err != nil {
			http.Error(w, "failed to create schedule", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(body)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func handleScheduleByID(w http.ResponseWriter, r *http.Request) {
	userID, _ := r.Context().Value(contextKeyUserID).(string)
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/schedules/"), "/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "schedule id is required", http.StatusBadRequest)
		return
	}
	id := parts[0]
	w.Header().Set("Content-Type", "application/json")

	if len(parts) == 2 && parts[1] == "trigger" {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s, err := triggerSchedule(userID, id, true)
		if err == sql.ErrNoRows {
			http.Error(w, "schedule not found", http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, "failed to trigger schedule", http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(s)
		return
	}

	switch r.Method {
	case http.MethodPut:
		var body Schedule
		if json.NewDecoder(r.Body).Decode(&body) != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		body.Name = strings.TrimSpace(body.Name)
		body.SongID = strings.TrimSpace(body.SongID)
		body.Title = strings.TrimSpace(body.Title)
		body.Artist = strings.TrimSpace(body.Artist)
		body.SourceType = strings.ToLower(strings.TrimSpace(body.SourceType))
		body.SourceURL = strings.TrimSpace(body.SourceURL)
		body.Recurring = strings.ToLower(strings.TrimSpace(body.Recurring))
		if body.SourceType == "" {
			body.SourceType = "library"
		}
		if body.SongID == "" || body.Title == "" || body.TriggerTime.IsZero() || !validRecurrence(body.Recurring) || !validScheduleSource(body.SourceType) || (body.SourceType == "url" && body.SourceURL == "") {
			http.Error(w, "invalid schedule", http.StatusBadRequest)
			return
		}
		if body.SourceType == "playlist" && len(body.Playlist) == 0 {
			http.Error(w, "playlist tracks are required", http.StatusBadRequest)
			return
		}
		playlistJSON, _ := json.Marshal(body.Playlist)
		res, err := db.Exec(`
			UPDATE schedules
			SET name=$1, song_id=$2, title=$3, artist=$4, source_type=$5, source_url=$6,
			    playlist=$7, trigger_time=$8, enabled=$9, recurring=$10, triggered=false, updated_at=NOW()
			WHERE id=$11 AND user_id=$12
		`, body.Name, body.SongID, body.Title, body.Artist, body.SourceType, body.SourceURL, playlistJSON, body.TriggerTime.UTC(), body.Enabled, body.Recurring, id, userID)
		if err != nil {
			http.Error(w, "failed to update schedule", http.StatusInternalServerError)
			return
		}
		if n, _ := res.RowsAffected(); n == 0 {
			http.Error(w, "schedule not found", http.StatusNotFound)
			return
		}
		body.ID = id
		body.Triggered = false
		json.NewEncoder(w).Encode(body)
	case http.MethodDelete:
		res, err := db.Exec(`DELETE FROM schedules WHERE id=$1 AND user_id=$2`, id, userID)
		if err != nil {
			http.Error(w, "failed to delete schedule", http.StatusInternalServerError)
			return
		}
		if n, _ := res.RowsAffected(); n == 0 {
			http.Error(w, "schedule not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func handleScheduleEvents(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimSpace(r.URL.Query().Get("token"))
	claims, err := jwtVerify(token)
	if err != nil || claims.UserID == "" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	ch := schedulerEvents.register(claims.UserID)
	defer schedulerEvents.unregister(claims.UserID, ch)
	fmt.Fprint(w, ": connected\n\n")
	flusher.Flush()

	keepalive := time.NewTicker(20 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case payload := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", payload)
			flusher.Flush()
		case <-keepalive.C:
			fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

// isDisallowedProxyIP reports whether ip must not be reached by the scheduler
// URL-stream proxy: loopback, private/internal ranges, and link-local
// addresses (which includes the 169.254.169.254 cloud metadata endpoint).
func isDisallowedProxyIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	return ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified() ||
		ip.IsMulticast()
}

// schedulerURLStreamClient fetches broadcaster-supplied external stream URLs
// while refusing to connect to internal/private addresses. The check runs in
// the dialer's Control hook, which fires with the literal IP the OS is about
// to connect to (after DNS resolution) — this closes the DNS-rebinding gap a
// simple pre-check of the hostname would leave open, and it re-runs for every
// redirect the client follows since each one opens a new connection.
var schedulerURLStreamClient = &http.Client{
	Transport: &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
			Control: func(_, address string, _ syscall.RawConn) error {
				host, _, err := net.SplitHostPort(address)
				if err != nil {
					return err
				}
				ip := net.ParseIP(host)
				if ip == nil || isDisallowedProxyIP(ip) {
					return fmt.Errorf("refusing to dial disallowed address %q", host)
				}
				return nil
			},
		}).DialContext,
		ResponseHeaderTimeout: 15 * time.Second,
	},
}

func handleSchedulerURLStream(w http.ResponseWriter, r *http.Request) {
	// The <audio> element src cannot send Authorization headers, so accept the
	// JWT as a ?token= query param (same pattern as /api/events).
	tokenStr := strings.TrimSpace(r.URL.Query().Get("token"))
	if tokenStr == "" {
		if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
			tokenStr = strings.TrimPrefix(auth, "Bearer ")
		}
	}
	if _, err := jwtVerify(tokenStr); err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rawURL := strings.TrimSpace(r.URL.Query().Get("url"))
	if rawURL == "" {
		http.Error(w, "url is required", http.StatusBadRequest)
		return
	}

	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		http.Error(w, "invalid url", http.StatusBadRequest)
		return
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		http.Error(w, "url must use http or https", http.StatusBadRequest)
		return
	}

	req, err := http.NewRequestWithContext(r.Context(), r.Method, parsed.String(), nil)
	if err != nil {
		http.Error(w, "failed to create upstream request", http.StatusBadRequest)
		return
	}
	if rangeHeader := strings.TrimSpace(r.Header.Get("Range")); rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	req.Header.Set("Accept", "audio/*,*/*;q=0.8")
	if ua := strings.TrimSpace(r.Header.Get("User-Agent")); ua != "" {
		req.Header.Set("User-Agent", ua)
	}

	resp, err := schedulerURLStreamClient.Do(req)
	if err != nil {
		http.Error(w, "failed to fetch source audio", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for _, header := range []string{"Content-Type", "Content-Length", "Accept-Ranges", "Content-Range", "Cache-Control", "ETag", "Last-Modified"} {
		if value := resp.Header.Get(header); value != "" {
			w.Header().Set(header, value)
		}
	}
	// URL sources can be live radio streams with no natural end. Tell reverse
	// proxies to forward audio chunks immediately instead of buffering them.
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(resp.StatusCode)
	if r.Method == http.MethodHead {
		return
	}
	if _, err := io.Copy(w, resp.Body); err != nil {
		log.Printf("[scheduler] url stream proxy copy error: %v", err)
	}
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

	// Do not clear station live state here. Browser encoders can run on the
	// dedicated DigitalOcean worker and must survive Railway API redeploys.
	// Public station reads already verify the real Icecast mount and clear stale
	// state when a source is no longer present.

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
	// DISABLE_GO_RTMP=1 → MediaMTX owns port 1935 exclusively;
	// lifecycle is handled via /api/stream/status webhook callbacks.
	if os.Getenv("DISABLE_GO_RTMP") != "1" {
		go startRTMPServer(streams)
	} else {
		log.Printf("[rtmp] Go RTMP server disabled — MediaMTX is the ingest endpoint on :1935")
	}

	// GeoIP + Icecast analytics worker.
	initGeoIP()
	startAnalyticsWorker()
	startWebListenerCleanup()
	startMetricsWorker()
	go runSchedulerLoop()

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
	mux.HandleFunc("/api/analytics", requireAuth(handleAnalytics))
	mux.HandleFunc("/ws/conference", handleConferenceSignal)
	mux.HandleFunc("/api/health", handleHealth)
	mux.HandleFunc("/api/config", handleConfig)
	mux.HandleFunc("/api/streams", handleStreams)
	mux.HandleFunc("/api/streams/status", handleStreamStatus)
	mux.HandleFunc("/api/viewers", handleViewers)
	mux.HandleFunc("/api/viewers/heartbeat", handleHeartbeat)
	mux.HandleFunc("/ws/chat", handleChat)
	mux.HandleFunc("/api/auth/register", handleRegister)
	mux.HandleFunc("/api/auth/login", handleLogin)
	mux.HandleFunc("/api/auth/verify-otp", handleVerifyOTP)
	mux.HandleFunc("/api/auth/resend-otp", handleResendOTP)
	mux.HandleFunc("/api/auth/forgot-password", handleForgotPassword)
	mux.HandleFunc("/api/auth/reset-password", handleResetPassword)
	mux.HandleFunc("/api/auth/", handleOAuthRoute) // platform OAuth connect + callback
	mux.HandleFunc("/api/user/stream-credentials", requireAuth(handleStreamCredentials))
	mux.HandleFunc("/api/user/encoder-authorize", requireAuth(handleEncoderAuthorize))
	mux.HandleFunc("/api/user/encoder-session", requireAuth(handleEncoderSession))
	mux.HandleFunc("/api/schedules", requireAuth(handleSchedules))
	mux.HandleFunc("/api/schedules/", requireAuth(handleScheduleByID))
	mux.HandleFunc("/api/scheduler/url-stream", handleSchedulerURLStream)
	mux.HandleFunc("/api/events", handleScheduleEvents)
	// VIDEO DISABLED: RTMP multistream destination routes are not registered.
	mux.HandleFunc("/api/user/profile", requireAuth(handleUserProfile))
	mux.HandleFunc("/api/user/listener-status", requireAuth(handleListenerStatus))
	mux.HandleFunc("/api/user/password", requireAuth(handleChangePassword))
	mux.HandleFunc("/api/user/account", requireAuth(handleDeleteAccount))
	// VIDEO DISABLED: social-video OAuth routes are not registered.
	mux.HandleFunc("/api/stream/status", handleStreamLifecycleWebhook)
	mux.HandleFunc("/api/stream/auth", handleStreamAuth)
	// VIDEO DISABLED: WebRTC-to-RTMP relay routes are not registered.
	mux.HandleFunc("/api/metrics", requireAuth(handleMetrics))
	mux.HandleFunc("/api/stations/", handleGetStation)
	mux.HandleFunc("/api/icecast/auth", handleIcecastAuth)
	mux.HandleFunc("/api/listeners/start", handleListenerSession)
	mux.HandleFunc("/api/listeners/heartbeat", handleListenerSession)
	mux.HandleFunc("/api/listeners/stop", handleListenerSession)
	mux.HandleFunc("/api/stations", handleGetStations)
	mux.HandleFunc("/ws/encode", handleEncoderWS)
	mux.HandleFunc("/listen/", handleListen)

	// ── Advertising Platform API (Admin Only) ────────────────────────────────
	mux.HandleFunc("/api/ads/placements", requireAdmin(handleAdPlacements))
	mux.HandleFunc("/api/ads/campaigns", requireAdmin(handleAdCampaigns))
	mux.HandleFunc("/api/ads/campaigns/", requireAdmin(handleAdCampaign))
	mux.HandleFunc("/api/ads/track", handleAdTrack)
	mux.HandleFunc("/api/ads/stats", requireAdmin(handleAdStats))

	// ── Public API (no auth required) ────────────────────────────────────────
	mux.HandleFunc("/api/public/pricing", handlePublicPricing)
	mux.HandleFunc("/api/support/messages", handleSupportMessages)
	mux.HandleFunc("/api/trial/start", requireAuth(handleStartPackageTrial))

	// ── PayPal Subscription API ───────────────────────────────────────────────
	mux.HandleFunc("/api/paypal/create-subscription", requireAuth(handlePayPalCreateSubscription))
	mux.HandleFunc("/api/paypal/webhook", handlePayPalWebhook)
	mux.HandleFunc("/api/paypal/success", requireAuth(handlePayPalSuccess))

	// ── Stripe Subscription API ───────────────────────────────────────────────
	mux.HandleFunc("/api/stripe/create-checkout-session", requireAuth(handleStripeCreateCheckoutSession))
	mux.HandleFunc("/api/stripe/webhook", handleStripeWebhook)
	mux.HandleFunc("/api/stripe/success", requireAuth(handleStripeSuccess))

	// ── Super Admin API ───────────────────────────────────────────────────────
	mux.HandleFunc("/api/admin/users", requireAdmin(handleAdminUsers))
	mux.HandleFunc("/api/admin/users/", requireAdmin(handleAdminUserUpdate))
	mux.HandleFunc("/api/admin/pricing", requireAdmin(handleAdminPricing))
	mux.HandleFunc("/api/admin/paypal/sync-plans", requireAdmin(handleAdminPayPalSyncPlans))
	mux.HandleFunc("/api/admin/marketing", requireAdmin(handleAdminMarketing))
	mux.HandleFunc("/api/admin/support", requireAdmin(handleAdminSupport))

	// HLS static file handler (serves /hls/<streamKey>/index.m3u8 etc.)
	mux.HandleFunc("/hls/", hlsHandler)

	// Uploads static file handler (serves /uploads/ads/<filename>)
	uploadsFS := http.FileServer(http.Dir("./uploads"))
	mux.Handle("/uploads/", http.StripPrefix("/uploads/", uploadsFS))

	go hub.run()

	log.Printf("[http] Listening on :%s  (HLS dir: %s)", port, HLSDir)
	log.Printf("[http] RTMP ingest: rtmp://localhost:%s/live/<streamKey>", RTMPPort)
	log.Fatal(http.ListenAndServe(":"+port, recoverMiddleware(corsMiddleware(mux))))
}
