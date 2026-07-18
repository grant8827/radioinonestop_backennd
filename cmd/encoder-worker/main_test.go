package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuthorizeAndSessionUpdate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Fatalf("missing bearer token")
		}
		switch r.URL.Path {
		case "/api/user/encoder-authorize":
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body["bitrate"] != "128k" {
				t.Fatalf("unexpected bitrate %q", body["bitrate"])
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"user_id":"u1","stream_key":"mount1","source_password":"secret","username":"source","bitrate":"128k","plan":"professional"}`))
		case "/api/user/encoder-session":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	originalAPIBase := apiBase
	apiBase = server.URL
	defer func() { apiBase = originalAPIBase }()

	auth, err := authorize("test-token", "128k")
	if err != nil {
		t.Fatal(err)
	}
	if auth.StreamKey != "mount1" || auth.Bitrate != "128k" {
		t.Fatalf("unexpected authorization: %+v", auth)
	}
	if err := updateSession("test-token", true); err != nil {
		t.Fatal(err)
	}
}

func TestOwnerExclusion(t *testing.T) {
	const userID = "owner-test"
	if !acquireOwner(userID) {
		t.Fatal("first owner acquisition failed")
	}
	if acquireOwner(userID) {
		t.Fatal("duplicate owner acquisition succeeded")
	}
	releaseOwner(userID)
	if !acquireOwner(userID) {
		t.Fatal("owner was not released")
	}
	releaseOwner(userID)
}
