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
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
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
	"github.com/oschwald/geoip2-golang"
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
		`ALTER TABLE stations ADD COLUMN IF NOT EXISTS source_password TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE stations ADD COLUMN IF NOT EXISTS icecast_listen_url TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE stations ADD COLUMN IF NOT EXISTS plan TEXT NOT NULL DEFAULT 'starter'`,
		`ALTER TABLE stations ADD COLUMN IF NOT EXISTS billing_cycle TEXT NOT NULL DEFAULT 'monthly'`,
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

	webListenerSessions   = map[string]*webListenerSession{}
	webListenerSessionsMu sync.Mutex
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
		icecastPass = "changeme123"
	}

	client := &http.Client{Timeout: 5 * time.Second}

	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			pollAllMounts(client, icecastBase, icecastUser, icecastPass)
		}
	}()
	log.Printf("[analytics] Icecast poller started → %s", icecastBase)
}

// pollAllMounts fetches live mounts from the DB and polls each one.
// We poll all stations (not just is_live=true) so a stale flag doesn't
// cause listener counts to silently stay at 0.
func pollAllMounts(client *http.Client, base, user, pass string) {
	rows, err := db.Query(`SELECT u.id, u.stream_key FROM users u
		JOIN stations s ON s.user_id = u.id WHERE u.stream_key IS NOT NULL AND u.stream_key <> ''`)
	if err != nil {
		log.Printf("[analytics] pollAllMounts DB error: %v", err)
		return
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
	log.Printf("[analytics] polling %d mount(s) via %s", len(live), base)

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
		pollMount(client, base, user, pass, mu.userID, mu.streamKey)
	}
}

func pollMount(client *http.Client, base, user, pass, userID, streamKey string) {
	mount := "/" + streamKey
	reqURL := base + "/admin/listclients?mount=" + url.QueryEscape(mount)
	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		log.Printf("[analytics] pollMount build request error: %v", err)
		return
	}
	req.SetBasicAuth(user, pass)

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[analytics] pollMount GET %s error: %v", reqURL, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("[analytics] pollMount GET %s status %d", reqURL, resp.StatusCode)
		activeSessionsMu.Lock()
		closeAllSessions(userID)
		activeSessionsMu.Unlock()
		syncLiveListenerCount(userID)
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("[analytics] pollMount read body error: %v", err)
		return
	}

	var stats icecastXML
	if err := xml.Unmarshal(body, &stats); err != nil {
		log.Printf("[analytics] pollMount XML parse error: %v — body: %s", err, string(body))
		return
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
	sourcePassword, err := generateKey()
	if err != nil {
		return "", err
	}
	_, err = db.Exec(
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
		var stationName, genre, description, logoURL, stationSlug, plan, billingCycle string
		_ = db.QueryRow(`SELECT station_name, genre, description, logo_url, station_slug, plan, billing_cycle FROM stations WHERE user_id = $1`, userID).
			Scan(&stationName, &genre, &description, &logoURL, &stationSlug, &plan, &billingCycle)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
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
		if _, err := db.Exec(
			`UPDATE stations SET station_name = $1, genre = $2, description = $3, logo_url = $4 WHERE user_id = $5`,
			newStationName, body.Genre, strings.TrimSpace(body.Description), body.LogoURL, userID,
		); err != nil {
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
		token, err := jwtSign(userID, email, newStationName)
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

// handleUpgradePlan updates the user's subscription plan and billing cycle
// POST /api/user/upgrade {"plan": "professional", "billing_cycle": "monthly"}
func handleUpgradePlan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID := r.Context().Value(contextKeyUserID).(string)
	email, _ := r.Context().Value(contextKeyEmail).(string)

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

	var planExists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM package_plans WHERE id = $1)`, body.Plan).Scan(&planExists); err != nil {
		log.Printf("[upgrade] Error validating plan %q for user %s: %v", body.Plan, userID, err)
		http.Error(w, "failed to validate plan", http.StatusInternalServerError)
		return
	}
	if !planExists {
		http.Error(w, "invalid plan", http.StatusBadRequest)
		return
	}

	// Validate billing cycle
	if body.BillingCycle != "monthly" && body.BillingCycle != "yearly" {
		http.Error(w, "invalid billing cycle", http.StatusBadRequest)
		return
	}

	if _, err := ensureStation(userID, email, "", ""); err != nil {
		log.Printf("[upgrade] Error ensuring station for user %s: %v", userID, err)
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
	err = tx.QueryRow(`SELECT plan, billing_cycle FROM stations WHERE user_id = $1 FOR UPDATE`, userID).
		Scan(&oldPlan, &oldBillingCycle)
	if errors.Is(err, sql.ErrNoRows) {
		http.Error(w, "station not found", http.StatusInternalServerError)
		return
	}
	if err != nil {
		log.Printf("[upgrade] Error loading current plan for user %s: %v", userID, err)
		http.Error(w, "failed to load current plan", http.StatusInternalServerError)
		return
	}

	result, err := tx.Exec(`UPDATE stations SET plan = $1, billing_cycle = $2 WHERE user_id = $3`,
		body.Plan, body.BillingCycle, userID)
	if err != nil {
		log.Printf("[upgrade] Error updating plan for user %s: %v", userID, err)
		http.Error(w, "failed to update plan", http.StatusInternalServerError)
		return
	}
	rowsAffected, err := result.RowsAffected()
	if err == nil && rowsAffected == 0 {
		http.Error(w, "no station updated", http.StatusInternalServerError)
		return
	}
	if _, err := tx.Exec(
		`INSERT INTO package_upgrade_history
			(id, user_id, old_plan, new_plan, old_billing_cycle, new_billing_cycle, status)
		 VALUES ($1, $2, $3, $4, $5, $6, 'active')`,
		upgradeID, userID, oldPlan, body.Plan, oldBillingCycle, body.BillingCycle,
	); err != nil {
		log.Printf("[upgrade] Error recording plan history for user %s: %v", userID, err)
		http.Error(w, "failed to record upgrade", http.StatusInternalServerError)
		return
	}
	if err := tx.Commit(); err != nil {
		log.Printf("[upgrade] Error committing plan upgrade for user %s: %v", userID, err)
		http.Error(w, "failed to update plan", http.StatusInternalServerError)
		return
	}

	log.Printf("[upgrade] User %s changed plan from %s to %s (%s)", userID, oldPlan, body.Plan, body.BillingCycle)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":       true,
		"upgrade_id":    upgradeID,
		"old_plan":      oldPlan,
		"plan":          body.Plan,
		"billing_cycle": body.BillingCycle,
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

	db.Exec(`UPDATE stations SET is_live = true, last_connected_at = $1 WHERE user_id = $2`, //nolint:errcheck
		time.Now().UTC().Format(time.RFC3339), userID)

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
		// Fan out raw WebM to /listen/ clients.
		h.broadcast(data)
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

	allow := func() { w.Write([]byte("awk=allow\r\n")) } //nolint:errcheck
	deny := func() { w.Write([]byte("awk=deny\r\n")) }   //nolint:errcheck

	if err := r.ParseForm(); err != nil {
		deny()
		return
	}

	// Log every incoming auth request so we can confirm Icecast is reaching us.
	log.Printf("[icecast-auth] request from=%s action=%q mount=%q user=%q",
		r.RemoteAddr, r.FormValue("action"), r.FormValue("mount"), r.FormValue("user"))

	// Non-source actions (e.g. listener auth) — allow by default.
	action := r.FormValue("action")
	if action != "source_auth" {
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
		SELECT station_slug, station_name, logo_url, is_live, current_listeners_count, genre, description, icecast_listen_url
		FROM stations
		ORDER BY is_live DESC, station_name ASC
	`)
	if err != nil {
		http.Error(w, `{"error":"db error"}`, http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	type Station struct {
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
		if err := rows.Scan(&s.Slug, &s.Name, &s.LogoURL, &s.IsLive, &s.Listeners, &s.Genre, &s.Desc, &s.IcecastListenURL); err != nil {
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
		SELECT station_slug, station_name, logo_url, is_live, current_listeners_count, genre, description, icecast_listen_url
		FROM stations WHERE station_slug = $1
	`, slug).Scan(&s.Slug, &s.Name, &s.LogoURL, &s.IsLive, &s.Listeners, &s.Genre, &s.Desc, &s.IcecastListenURL)
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
	var userID string
	var isLive bool
	err := db.QueryRow(`SELECT user_id, is_live FROM stations WHERE station_slug = $1`, slug).Scan(&userID, &isLive)
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
	if h := strings.TrimSpace(os.Getenv("ICECAST_HOST")); h != "" {
		cfg.Host = h
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
	if envPass := os.Getenv("ICECAST_SOURCE_PASSWORD"); envPass != "" {
		icecastPass = envPass
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
		"-f", "webm",
		"-i", "pipe:0",
		"-vn",
		"-c:a", audioCodec,
		"-b:a", cfg.Bitrate,
		"-ar", "44100",
		"-ac", "2",
		"-content_type", contentType,
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

	ffmpegDone := make(chan error, 1)
	go func() { ffmpegDone <- cmd.Wait() }()

	// ── Wait up to 3 s for early FFmpeg failure before declaring live ─────
	// FFmpeg exits almost instantly on bad hostname/password; only after
	// surviving this window do we tell the client the stream is live.
	startupTimer := time.NewTimer(3 * time.Second)
	select {
	case err := <-ffmpegDone:
		startupTimer.Stop()
		sendStatus("error", ffmpegStatusMessage("FFmpeg failed to connect to Icecast", err, ffmpegStderr))
		return
	case <-startupTimer.C:
		// still running — Icecast accepted the connection
	}

	// Mark station live in DB so the home page card updates.
	icecastListenURL := "/icecast" + cfg.Mount
	db.Exec(`UPDATE stations SET is_live = true, last_connected_at = $1, icecast_listen_url = $2 WHERE user_id = $3`, //nolint:errcheck
		time.Now().UTC().Format(time.RFC3339), icecastListenURL, claims.UserID)
	defer db.Exec(`UPDATE stations SET is_live = false, icecast_listen_url = '' WHERE user_id = $1`, claims.UserID) //nolint:errcheck

	sendStatus("live", fmt.Sprintf("Streaming → %s:%s%s", cfg.Host, cfg.Port, cfg.Mount))

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
				sendStatus("error", ffmpegStatusMessage("FFmpeg exited", err, ffmpegStderr))
			} else {
				sendStatus("stopped", "FFmpeg process exited")
			}
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
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
			stdin.Close() //nolint:errcheck
			ffErr := <-ffmpegDone
			if ffErr != nil || ffmpegStderr.String() != "" {
				sendStatus("error", ffmpegStatusMessage("FFmpeg stopped before accepting audio", ffErr, ffmpegStderr))
			} else {
				sendStatus("error", "write to ffmpeg: "+err.Error())
			}
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
	mu     sync.Mutex
	relays map[string]context.CancelFunc
}

var webRelay = &webRelayManager{relays: make(map[string]context.CancelFunc)}

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

	// Check if streaming is disabled due to exceeding listener limit
	if isStreamingDisabled(userID) {
		http.Error(w, "streaming disabled - listener limit exceeded", http.StatusForbidden)
		return
	}

	var body struct {
		Path string `json:"path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Path == "" {
		http.Error(w, "path required", http.StatusBadRequest)
		return
	}

	// Load active destinations for this user.
	rows, err := db.Query(`
		SELECT rtmp_url, stream_key FROM destinations
		WHERE user_id = $1 AND enabled = 1 AND stream_key != '' AND rtmp_url != ''
	`, userID)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var dests []string
	for rows.Next() {
		var rtmpURL, key string
		if err := rows.Scan(&rtmpURL, &key); err == nil {
			dests = append(dests, strings.TrimRight(rtmpURL, "/")+"/"+strings.TrimLeft(key, "/"))
		}
	}

	if len(dests) == 0 {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":   "ok",
			"relaying": false,
			"reason":   "no active destinations",
		})
		return
	}

	// Build FFmpeg args: pull via RTSP, stream-copy to each destination.
	rtspSrc := mediamtxRTSPBase() + "/" + body.Path
	args := []string{
		"-rtsp_transport", "tcp",
		"-i", rtspSrc,
	}
	for _, dest := range dests {
		args = append(args, "-c:v", "copy", "-c:a", "copy", "-f", "flv", dest)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		cancel()
		log.Printf("[relay] start error user=%s: %v", userID, err)
		http.Error(w, "relay start failed", http.StatusInternalServerError)
		return
	}

	// Replace any previous relay for this user.
	webRelay.mu.Lock()
	if old, ok := webRelay.relays[userID]; ok {
		old()
	}
	webRelay.relays[userID] = cancel
	webRelay.mu.Unlock()

	// Watch the process; clean up when it exits.
	go func() {
		defer cancel()
		if err := cmd.Wait(); err != nil && ctx.Err() == nil {
			log.Printf("[relay] FFmpeg exited user=%s path=%s: %v", userID, body.Path, err)
		}
		webRelay.mu.Lock()
		if webRelay.relays[userID] != nil {
			delete(webRelay.relays, userID)
		}
		webRelay.mu.Unlock()
	}()

	log.Printf("[relay] started user=%s path=%s destinations=%d", userID, body.Path, len(dests))
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":       "ok",
		"relaying":     true,
		"destinations": len(dests),
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

	webRelay.mu.Lock()
	cancel, ok := webRelay.relays[userID]
	if ok {
		cancel()
		delete(webRelay.relays, userID)
	}
	webRelay.mu.Unlock()

	log.Printf("[relay] stopped user=%s (was_running=%v)", userID, ok)
	w.WriteHeader(http.StatusNoContent)
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
	rows, err := db.Query(`
		SELECT 
			c.id, c.placement_id, c.advertiser_name, c.target_url,
			c.asset_type, c.asset_url, c.asset_name,
			c.price, c.original_price, c.discount_percent,
			c.status, c.impressions, c.clicks, c.created_at,
			p.name as placement_name, p.placement
		FROM ad_campaigns c
		JOIN ad_placements p ON c.placement_id = p.id
		ORDER BY c.created_at DESC
	`)
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

	if body.PlacementID == "" || body.AdvertiserName == "" || body.AssetType == "" {
		http.Error(w, "missing required fields", http.StatusBadRequest)
		return
	}

	campaignID, err := generateKey()
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}

	originalPrice := body.Price
	if body.DiscountPercent > 0 {
		originalPrice = body.Price / (1.0 - float64(body.DiscountPercent)/100.0)
	}

	_, err = db.Exec(`
		INSERT INTO ad_campaigns (
			id, placement_id, advertiser_name, target_url,
			asset_type, asset_url, asset_name,
			price, original_price, discount_percent, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'draft')
	`, campaignID, body.PlacementID, body.AdvertiserName, body.TargetURL,
		body.AssetType, body.AssetURL, body.AssetName,
		body.Price, originalPrice, body.DiscountPercent)

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
		ActiveCampaigns int     `json:"activeCampaigns"`
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

	// GeoIP + Icecast analytics worker.
	initGeoIP()
	startAnalyticsWorker()
	startWebListenerCleanup()

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
	mux.HandleFunc("/api/auth/", handleOAuthRoute) // platform OAuth connect + callback
	mux.HandleFunc("/api/user/stream-credentials", requireAuth(handleStreamCredentials))
	mux.HandleFunc("/api/user/profile", requireAuth(handleUserProfile))
	mux.HandleFunc("/api/user/listener-status", requireAuth(handleListenerStatus))
	mux.HandleFunc("/api/user/upgrade", requireAuth(handleUpgradePlan))
	mux.HandleFunc("/api/user/password", requireAuth(handleChangePassword))
	mux.HandleFunc("/api/user/account", requireAuth(handleDeleteAccount))
	mux.HandleFunc("/api/user/oauth-connections", requireAuth(handleOAuthConnections))
	mux.HandleFunc("/api/user/oauth-connections/", requireAuth(handleOAuthConnections))
	mux.HandleFunc("/api/user/oauth-stream-keys/sync", requireAuth(handleSyncOAuthStreamKeys))
	mux.HandleFunc("/api/stream/relay/start", requireAuth(handleRelayStart))
	mux.HandleFunc("/api/stream/relay/stop", requireAuth(handleRelayStop))
	mux.HandleFunc("/api/stations/", handleGetStation)
	mux.HandleFunc("/api/icecast/auth", handleIcecastAuth)
	mux.HandleFunc("/api/listeners/start", handleListenerSession)
	mux.HandleFunc("/api/listeners/heartbeat", handleListenerSession)
	mux.HandleFunc("/api/listeners/stop", handleListenerSession)
	mux.HandleFunc("/api/stations", handleGetStations)
	mux.HandleFunc("/ws/encode", handleEncoderWS)
	mux.HandleFunc("/listen/", handleListen)

	// ── Advertising Platform API ──────────────────────────────────────────
	mux.HandleFunc("/api/ads/placements", requireAuth(handleAdPlacements))
	mux.HandleFunc("/api/ads/campaigns", requireAuth(handleAdCampaigns))
	mux.HandleFunc("/api/ads/campaigns/", requireAuth(handleAdCampaign))
	mux.HandleFunc("/api/ads/track", handleAdTrack)
	mux.HandleFunc("/api/ads/stats", requireAuth(handleAdStats))

	// HLS static file handler (serves /hls/<streamKey>/index.m3u8 etc.)
	mux.HandleFunc("/hls/", hlsHandler)

	go hub.run()

	log.Printf("[http] Listening on :%s  (HLS dir: %s)", port, HLSDir)
	log.Printf("[http] RTMP ingest: rtmp://localhost:%s/live/<streamKey>", RTMPPort)
	log.Fatal(http.ListenAndServe(":"+port, corsMiddleware(mux)))
}
